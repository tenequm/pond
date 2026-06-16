# Copy append fast-path: make store-to-store copy bandwidth-bound

## Problem

`pond copy` store-to-store is commit-latency-bound on remote object stores. A real local -> Hetzner `nbg1` copy of 11,119 sessions (2,021,772 messages, 1,395,610 parts) made almost no progress in minutes: ~28 sessions landed durably in 207s.

Two root causes, both in the write path:

1. **Wrong primitive.** `merge_scanner` (`src/sessions.rs`) calls `merge_insert` once per source scan batch. `merge_insert` (even with `WhenMatched::DoNothing`) joins each batch against the destination to find matches. For a fresh/absent session there are no matches - the probe is pure waste, and it grows with the destination because `messages`/`parts` have no scalar index covering the full composite merge key (`(session_id, id)` / `(session_id, message_id, id)`); only the leading columns are indexed, so the probe falls back to a full key-column scan (~250-300 MB and rising).
2. **One commit per batch.** Each `merge_insert` is one Lance commit = one conditional-PUT round-trip + manifest write. On a high-latency store, thousands of serialized commits dominate wall time. This is the same overhead `pond sync` already fixed by buffering 100 sessions per flush (`IngestValidator`, `src/sessions.rs`); copy regressed it.

The fix mirrors what sync already does (batch commits) plus picks the right primitive per session: **append the sessions that cannot collide, merge only the rare ones that can.** Append never joins, never reads the target, and writes large fragments under one commit.

## Priority

**Efficiency and speed win over progress granularity where they conflict.** The append path commits **once per table** (not chunked to manufacture per-commit progress updates). Live progress is driven **read-side** - counting rows as they stream out of the source - which is free and does not compromise the single-commit append. Written/committed byte and fragment totals finalize when each append returns; that coarser granularity is the accepted trade.

## Confirmed against lance 7.0.0 (implemented)

The append API exists in the pinned crates.io 7.0.0 (verified in registry source, not just the v8 tree):

- `InsertBuilder::new(Arc<Dataset>).with_params(&WriteParams{ mode: Append, max_bytes_per_file, .. }).progress(cb).execute_stream(stream) -> Result<Dataset>`. `execute_stream` takes `impl StreamingWriteSource`; pond feeds `scanner.try_into_stream().await?.into()` (a `SendableRecordBatchStream`, which impls the trait). `WriteMode::Append`, `WriteParams.max_bytes_per_file`, and `WriteParams.write_progress` all present.
- **Live write-side stats via `WriteProgressFn`** (the draft's "coarse write totals" trade is unnecessary): `.progress(|WriteStats{ bytes_written: u64, rows_written: u64, files_written: u32 }| ..)`, fired per batch + a final cumulative tick, *cumulative per stream*. This drives the live S3-throughput number, not just an end-of-call total.
- `execute_stream` returns only `Dataset` (no write summary). So `AppendStats{ rows, bytes_written, files_written, attempts }` is **captured**: `rows`/`bytes`/`files` come from the cumulative `WriteStats` high-water, `attempts` from pond's own `retry_lance` counter (Lance exposes `num_attempts` only on `MergeStats`, never for inserts).
- Fragment sizing: append sets `max_bytes_per_file = TARGET_FRAGMENT_BYTES` (256 MiB), reusing `write_params_for_create()` so the appended fragments match the table's storage version / stable-row-id mode.

### Progress integration (write-side, `WriteProgressFn`)

`CopyState::record_write_progress(table, &WriteCumulative, &WriteStats)` folds each cumulative tick into the shared counters: `rows_written` -> per-table done bucket, `bytes_written`/`files_written` -> wire totals. `WriteCumulative` (`progress.rs`) is a per-stream high-water (`fetch_max` + `saturating_sub` for the delta).

**Correction to the draft's gotcha #4.** The draft said "fresh `WriteCumulative` per OCC attempt." That is wrong - it would double-count: attempt 1 contributes its partial high-water X to the shared counter, then a fresh per-attempt cumulative makes attempt 2 re-add its full total F, summing to X+F. The implemented design uses **one persistent `WriteCumulative` per `append_stream` call, shared across attempts**. Because `WriteStats` resets to zero each new stream and the high-water is monotonic (`fetch_max`), a retry's early ticks fall below the mark (delta 0, the live bar briefly stalls) until they pass it, after which the shared total lands at exactly F. `record_append(attempts)` then folds only `commit_attempts_max` (the final cumulative tick already carried bytes/rows/files, so folding them again would double-count - draft gotcha #5, honored).

`merge_attempts_max` was renamed `commit_attempts_max` (JSON `merge_retries_max` -> `commit_retries_max`): both append and merge feed it.

## What is already in the working tree (visibility layer)

A live-progress layer was added ahead of this refactor. It is built around `merge_scanner` and per-batch `MergeStats`, so this refactor must integrate with it, not ignore it.

- `src/progress.rs` (new): `CopyState` (lock-free atomic counter bag), `CopySnapshot`, `Reporter` (indicatif `MultiProgress` for TTY, NDJSON for `--json`, OSC 9;4 taskbar via `termpulse-core`), `Phase` (Plan/Stream/Indexes/Verify), `RateWindow` (30s sliding window -> read/write B/s + sessions/s + ETA), `Mode` (Human/Json/Silent). A background ticker reads `CopyState::snapshot()` every 100 ms.
  - `CopyState::record_merge(table, &MergeStats)` advances per-table done counters + `bytes_written` + `fragments_written` + `skipped_duplicates` + `merge_attempts_max`.
  - `CopyState::record_scan_summary(bytes_read)` is fed from Lance's end-of-scan `ExecutionSummaryCounts` via `scanner.scan_stats_callback`.
- `src/substrate.rs`: `merge()` now returns Lance `MergeStats` verbatim; `merge_insert_stats()` exposes it; `merge_insert()` is the thin `u64` wrapper. `MergeStats` re-exported.
- `src/sessions.rs`: `merge_scanner`, `copy_delta_from`, `copy_table_delta`, `copy_table_scan` all thread `state: Option<&Arc<CopyState>>`. Archive import passes `None`.
- `src/main.rs`: `--json` flag on `copy`; `init_tracing` switched to `IndicatifLayer` + registry; `run_store_to_store_copy` builds `CopyState`/`Reporter`, sets phases, emits per-phase receipts, calls `reporter.finish`.

### Constraint this imposes

The progress buckets (`sessions_done`/`messages_done`/`parts_done`) are currently advanced only from `record_merge`. The append path produces no `MergeStats`, so without integration the append path would show **zero progress** and feed nothing to the live surface. The refactor therefore adds a read-side counting hook the append path drives, and keeps `record_merge` for the merge (grown) path.

## Design

### 1. Split the plan: absent vs grown (`src/sessions.rs`)

```rust
pub struct DeltaPlan {
    pub absent: Vec<String>,        // not on dest -> append (cannot collide)
    pub grown:  Vec<String>,        // on dest, source has more messages -> merge (must dedup)
    pub source_sessions: usize,
}
impl DeltaPlan {
    pub fn is_empty(&self) -> bool { self.absent.is_empty() && self.grown.is_empty() }
    pub fn total(&self) -> usize   { self.absent.len() + self.grown.len() }
}
```

`plan_incremental_from` keeps its existing `try_join!` over both id-sets + both count maps and routes each source id: `!dest_ids.contains(id)` -> `absent`; else `source_count > dest_count` -> `grown`. Drops the private `is_full()`. Append-only is what makes appending `absent` safe: a copied row is immutable, so a re-run re-plans from current dest state and an interrupted-then-resumed copy never double-appends (the landed sessions are no longer "absent").

### 2. Append primitive (`src/substrate.rs`)

Add an append write next to `merge`, reusing `retry_lance` + cached-dataset replace:

```rust
pub(crate) async fn append_stream<F, Fut>(
    &self,
    table: Table,
    make_source: F,           // rebuilds the source scan stream per retry attempt
) -> Result<AppendStats>
where F: Fn() -> Fut, Fut: Future<Output = Result<SendableRecordBatchStream>>;
```

- Uses `InsertBuilder::new(dataset).with_params(WriteParams { mode: Append, max_rows_per_file: <Lance default 1Mi>, max_rows_per_group: 8192, .. }).execute_stream(source)`. Verify the exact 7.0.0 signature (this tree's `lance` is crates.io 7.0.0; the v8 source shows `execute_stream` + `WriteParams.write_progress`, confirm both exist in 7.0.0 or fall back to `Dataset::append`).
- `make_source` is a factory, not a pre-built reader, because the scan stream is one-shot - on OCC conflict `retry_lance` rebuilds it. Source re-scan is local and cheap; copy is a single admin writer so conflicts are rare.
- Commits **once** at the end of the stream. Memory stays bounded - Lance flushes fragments to the store as `max_rows_per_file` fills.
- Returns `AppendStats { rows, bytes_written, files_written, attempts }` (rename/extend as the real Lance return allows) so the progress layer gets final write totals. No join, no `skipped_duplicates`.

### 3. Read-side progress hook (`src/progress.rs`, `src/sessions.rs`)

Efficiency-first: do **not** chunk the append. Drive progress from the stream as rows are pulled from the source, before they reach the single append commit.

- Add `CopyState::record_streamed(table, rows: u64)` - advances the same per-table done bucket `record_merge` uses. (Generalize the bucket selection already in `record_merge`.)
- Add `CopyState::record_append(&AppendStats)` - folds final `bytes_written`/`files_written`/`attempts` once per append call (3 per full copy).
- New `append_scanner(table, make_source_scanner, state)` analog of `merge_scanner`: wraps the source stream so each batch bumps `record_streamed(table, batch.num_rows())` and `record_scan_summary` (read bytes) as it passes through, then hands the wrapped stream to `Handle::append_stream`. The live bar advances on read throughput during the whole append; write totals land at commit.
- `Phase::Stream` covers both append (absent) and merge (grown) sub-steps.

Known minor limitation (pre-existing, document don't fix): a *grown* session's `sessions_done` does not advance from the Sessions-table merge because its session row already exists (merge inserts 0). Grown is rare; the from-empty common case is all-absent -> all-appended -> 100%. Acceptable ETA skew only on small incremental re-runs.

### 4. Rewrite the copy path (`src/sessions.rs`)

`copy_delta_from` keeps the 3-table `try_join!` (parallel, unchanged). Replace `copy_table_delta` with `copy_table(source, table, key_column, plan, state)` doing **append-then-merge**, sequential within a table (one write lock), parallel across tables:

1. Force dest table existence (`self.handle.dataset(table).await?`) - unchanged, keeps an empty `parts` table from going missing.
2. **Append `absent`** (the dominant cost):
   - `absent.len() == source_sessions` (true from-empty / resumed copy where ~everything is absent) -> unfiltered source scan -> **one** `append_scanner` -> one commit.
   - else (partial) -> chunk `absent` by `COPY_SESSION_IN_CHUNK = 512` into `in_predicate(key_column, chunk)` (btree-pushed) -> one `append_scanner` per chunk (~22 commits for 11k, vs thousands). Still efficiency-first; chunking here is forced by predicate size, not progress.
3. **Merge `grown`** via existing `merge_scanner` (chunked 512, `state` passed for `record_merge`). Tiny set.

Both append and merge source scanners keep `blob_handling(AllBinary)` so blob bytes (parts) materialize into the write - identical to today.

### 5. CLI wiring (`src/main.rs`)

- `run_store_to_store_copy` receipts that read `plan.sessions.len()` switch to `plan.total()`; `state.set_sessions_total(plan.total() as u64)`. The plan receipt can break out `{absent} new + {grown} grown`.
- No new flags. `--json` and the Reporter wiring already in the tree stay.

## File-by-file

| File | Change |
|---|---|
| `src/sessions.rs` | `DeltaPlan` -> `{absent, grown, source_sessions}`; `plan_incremental_from` routing; `copy_delta_from` -> `copy_table` (append-then-merge); new `append_scanner`; `merge_scanner` kept for grown + archive import |
| `src/substrate.rs` | new `Handle::append_stream` (+ `AppendStats`); reuse `retry_lance` |
| `src/progress.rs` | `CopyState::record_streamed(table, rows)`, `record_append(&AppendStats)`; generalize bucket selection |
| `src/main.rs` | `plan.sessions` -> `plan.total()`; plan receipt wording; `set_sessions_total` |
| `tests/integration/copy.rs` | new `DeltaPlan` shape; assertions below |
| `benches/copy_bench.rs` | new plan fields; append-vs-merge scenario + version-delta print |
| `docs/spec.md` | section 7.8 copy paragraph: note absent-session append fast-path alongside the incremental merge |

## Tests (`tests/integration/copy.rs`)

- Update for `{absent, grown}` (the in-tree `copy_delta_from(.., None)` signature already lands the `state` arg).
- **Commit collapsing**: read dest `messages` table Lance `version()` before/after a from-empty copy of N sessions; assert it advanced by a small constant (1 append), not O(scan batches). Directly tests the perf-correctness property, in-memory, no S3.
- **Resumed copy = no duplicates**: copy a subset, then copy all; assert dest row counts == source (append path does not dedup, correctness relies on re-planning).
- **Grown still dedups**: reuse the equal-timestamp growth case; assert it routes through `grown` and merge-skips already-present rows.
- Existing round-trip / rerun-noop / union / superset-verify assertions hold.

## Bench (`benches/copy_bench.rs`)

Update for the new plan fields; add an "append fast-path vs old per-batch merge" scenario printing dataset `version()` deltas alongside wall time. Re-run the real local -> `nbg1` copy to confirm wall-time class change (commit-bound minutes -> bandwidth-bound).

## Risks / verify during impl (do not assume)

1. **Lance 7.0.0 append API.** Confirm `InsertBuilder::execute_stream` (+ `WriteParams.write_progress` if used) exists in pinned crates.io 7.0.0; the v8 source tree shows it but line/shape can differ. Fall back to `Dataset::append` if needed.
2. **Append schema match for the blob `parts` column.** `merge` proves AllBinary-materialized batches are write-shaped, but append is stricter about exact schema equality. If the materialized blob column type mismatches the dataset's stored type on append, fall back to merge for `parts` only and flag it - do not ship a guess.
3. **Empty-table creation schema.** Confirm `handle.dataset(table)` on a fresh store materializes the canonical schema (append needs the table to exist with the right columns) - same precondition merge relies on today.
4. **OCC retry rebuilds the source scan** (factory closure), not a one-shot reader.

## Deferred (not this change)

- Scalar index on the full composite merge key (`messages`/`parts`) - after the split, the merge probe only runs on the tiny grown set; its real beneficiary is `sync`, not flagged slow. Adding it re-enters the v7 BTree-rebuild bug pond deliberately dodges.
- `LANCE_IO_THREADS` / AIMD tuning - document in copy help, do not mutate global env.
- Archive-restore unification - `pond copy --from x.pond` has the same slowness and could reuse `append_scanner`; the seam is already shared (`merge_scanner` stays), so it inherits the speedup when adopted. Separate fast-follow.
- Issue #44 ordering column (`options.source.line`) - correctness/conformance, orthogonal to copy speed; explicitly deferred by the user.

## Validation + commit

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, then the manual local -> S3 run. One combined commit, conventional `perf(copy)!:` (the `DeltaPlan` field rename is a breaking API change -> release-plz minor bump). The in-tree visibility-layer changes (`progress.rs`, `MergeStats`, `--json`) fold into the same commit since they share the path.
