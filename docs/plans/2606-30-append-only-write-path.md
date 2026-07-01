# Append-only write path + incremental scalar indexes - implementation plan

Status: ready to implement. Owner: TBD. Prereqs: none (pre-1.0, breaking changes are free). Storage target of record: `s3+https://nbg1.your-objectstorage.com/pondarium/pond` (Hetzner S3).

This plan converts pond's remote-storage write path to pure append-only and makes scalar-index maintenance incremental. It is written so a fresh-session agent can implement it end to end. Line numbers are approximate (verify by symbol name; the codebase drifts). Read the cited `docs/spec.md` sections before changing behavior.

## 0. Read first

- `docs/spec.md` sections 3 (substrate), 3.5 (concurrency), 3.7 (`lance-index-maintenance`), 5.4 (`session-movement-complete`), 5.5 (embeddings-derived), 7.8 (CLI verbs - `sync`/`copy`/`optimize`), 8 (search + producing embeddings).
- `CLAUDE.md` sections: "Sync change-detection oracle", "Copy write path", "Object-store request rate" (never tune AIMD), "Errors", "Process".
- Prior plans this supersedes/extends: `docs/plans/2606-21-embedding-writeback-and-derived-vector-storage.md` (this plan adopts its option **D**, embed-at-ingest, and rejects option **C**, a separate vector table - the multi-table join pain on Lance's planner-less crate set is not worth it for search).

## 1. Guiding principle: the S3 commit law

Measured this session on the real store: **the S3 commit floor is ~1.0 s and flat from 1 to 512 rows per commit** - appending 1 row costs the same as 512. One bulk commit moves 7,387 rows/s on S3 (61k/s local). So on an object store every write operation's cost is approximately `(number of commits) x ~1 s`, not bytes. Every change below is in service of one rule: **minimize commit count, and never issue a request you can avoid.** This is also why `merge_insert` over S3 (one commit per streamed batch) is pathological and `append` (one commit per table) is not.

Corollary already enforced in `CLAUDE.md`: never widen Lance's AIMD limiter to "go faster" - throttling is a symptom of too many requests, and on a round-trip-bound store the fix is always fewer requests.

## 2. Scope & sequencing

Three workstreams, each independently shippable and independently verifiable. Land in this order (cheapest/lowest-risk first):

- **A - incremental scalar indexes** (`#3`, the 520 s full BTree/ZoneMap rebuild). Smallest change, highest ROI, no redesign. Removes a stale bug-workaround.
- **B - copy grown-sessions: filter + append** (`#2`, the 463 s merge stream). Unifies copy onto the ingest write path.
- **C - embed at ingest + remove `--no-optimize`** (option D). Folds the vector into the ingest commit; deletes the deferred-ingest mode.

Out of scope here (tracked separately): read-path cold-start (index page-in 175-442 s), FTS warm posting-list cost, the `from_date`/`to_date` search defect (GitHub issue #75), and anything touching the AIMD limiter.

## 3. Workstream A - incremental scalar indexes (BTree + ZoneMap)

### Problem

`pond optimize`'s index stage FULL-REBUILDS the BTree and ZoneMap scalar indexes (`create_index replace=true`) on any non-empty unindexed-fragment set, with no tiny-delta guard. On S3 this is the ~520 s tail of every `pond copy` and every `pond sync` (once `--no-optimize` is gone, see C). The full rebuild re-scans the entire source column (e.g. all 2.1M `messages.session_id` values) from S3 data files - that scan is the cost.

### Root cause: a stale workaround

The full rebuild is a workaround for a Lance beta.16 bug that **is already fixed in pond's pinned `7.0.0`**. The spec note (`lance-index-maintenance`, 3.7) says: BTree uses `create_index(replace=true)` because "`optimize_indices` BTree path tripped `RowAddrTreeMap::from_sorted_iter` on column-update commits; rebuild from scratch avoids the bug. Switch to `optimize_indices(append)` once upstream is fixed." It is fixed: `~/.cargo/registry/src/index.crates.io-*/lance-index-7.0.0/src/scalar/btree/flat.rs:54-55` now sorts by row id unconditionally before the failing call:

```rust
// Sort by row id to make bitmap construction more efficient
let data = data.sort_by_column(IDS_COL_IDX, None)?;
```

so `RowAddrTreeMap::from_sorted_iter` always sees sorted addresses, for appends and column-updates alike. The workaround has outlived its bug; pond is paying the 520 s for nothing.

### Lance 7.0.0 facts (verified in the vendored source)

- BTree append: `Dataset::optimize_indices` (`lance-7.0.0/src/index.rs:~1221`) -> `merge_indices`/`merge_indices_with_unindexed_frags` (`lance-7.0.0/src/index/append.rs:~103,126`) -> scalar arm (`append.rs:~519`) -> `index.update` -> `combine_old_new` (`lance-index-7.0.0/src/scalar/btree.rs:~1516-1556`). It reads the existing (compact, sorted) index pages + only the new fragments' data, merges by value, writes a new merged index file. It does NOT re-scan already-indexed source. `OptimizeOptions::append()` sets `num_indices_to_merge = 0` (`lance-index-7.0.0/src/optimize.rs`).
- ZoneMap append: `ZoneMapIndex::update` -> `rebuild_zones` (`lance-index-7.0.0/src/scalar/zoned.rs:~271-284`) literally `combined = existing.to_vec(); combined.append(new_zones)`, training zones over only new data. Near-O(delta). Caveat: ZoneMap `can_remap == false` (`scalar/zonemap.rs:~567`) - it cannot survive a compaction row-address remap, so a compaction still forces its rebuild (see gotcha).
- Shape note: for BTree/ZoneMap the append path produces a single rewritten merged index file (new UUID, old removed), NOT a disjoint delta segment. Disjoint segments (query-time merge of indexed + unindexed) are the vector/FTS path. So BTree cost is `O(index size)` per fold (read+rewrite the whole index file), not `O(delta)` - much cheaper than the full source-column scan, but it still grows with the corpus.

### Target

In `src/substrate.rs` `optimize_table_indices` (~2789-2988): route the scalar **BTree** intents and the **ZoneMap** intent through `optimize_indices(append)` (the same path FTS/Bitmap/IVF already take at ~2891-2895/2957-2961), instead of the `replace=true` rebuild (~2856-2890) and the `Scalar(_)` full-rebuild arm (~2896-2927). The existing skip guard (`unindexed_fragments(intent).is_empty() -> continue`, ~2851-2854) stays.

Optional but recommended (avoids rewriting the whole BTree file on every 1-row sync, since BTree fold is `O(index size)`): add a **lag threshold** - only fold a scalar index when its unindexed tail exceeds N fragments or N rows; below that, leave it unindexed (Lance flat-scans the tail and merges results - correct, just slightly slower reads). This is a pond-level policy on top of the append fold, not a Lance change.

### Gotchas

- Behavioral verification is mandatory (the bug note existed for a reason): after switching, run a real-corpus `pond optimize --only index` against an S3 scratch store and assert no `from_sorted_iter` error and identical query results vs a full rebuild.
- Compaction interaction: ZoneMap (and BTree by row-address) cannot remap after a compaction `Rewrite`. pond uses stable row ids (`lance-table-creation-stable-row-ids`) - confirm the compaction phase (`run_optimize_compact_phase`, substrate.rs:~2208) either relies on stable row ids so the index survives, or rebuilds the scalar indexes post-compact. Do not let a compaction silently invalidate an index.
- Keep the "skip indexes whose columns no write touched" property (spec 3.7) - sound because Lance prunes coverage only for indexes whose fields overlap a write's modified fields.

### Verify

`cargo test`; a real-corpus `pond optimize --only index` on S3 scratch (no error, identical results, far below the old 520 s); confirm the BTree/ZoneMap segments cover the new fragments.

## 4. Workstream B - copy grown-sessions: filter + append

### Problem

`pond copy` routes "grown" sessions (present on dest, more rows on source) through `merge_insert`, which re-streams the session's ENTIRE content and commits once per RecordBatch. Measured: 4 grown sessions = 6,044 messages re-scanned to insert 10, ~463 s on S3, plus a "Too many concurrent writers" OCC conflict (merge is a high-conflict `Update`-family op; a concurrent sync/cron advancing the version makes it retry to exhaustion).

### Target

Make the grown-session path do exactly what ingest already does (`src/sessions.rs` `upsert_session_batch`, ~817-1069): a per-row pre-existence sweep (~903-964) then **append only the absent rows** (`append_batches`, ~1048-1053). Extract that "filter-absent -> append data tables, insert-only-merge the tiny sessions row" routine into one shared helper and call it from both `upsert_session_batch` (sync) and the copy grown-session path (`copy_table`/`merge_scanner`, ~448-474, ~648-694).

Concretely for copy: for grown sessions, fetch the dest's present message/part PKs (a `session_id IN (...)` key scan, index-accelerated), filter the source scan to the absent rows in memory, and `append_stream`/`append_batches` the remainder. The 10 new messages append; the 6,034 present ones are never streamed or joined.

### Why this is correct and conflict-free

- New rows under the deterministic PK are absent -> cannot collide -> safe to append (`lance-chokepoints-write`, `lance-deterministic-pk`). `merge_insert` gave no extra concurrency-correctness over filter-then-append: both dedup only against committed state; neither prevents a true concurrent-same-PK race (a pre-existing property of the unenforced PK + no-coordinator design).
- `Append` is OCC-compatible with `Append` (Lance auto-rebases; `Append` conflicts only with `Overwrite`/`Restore`/`UpdateMemWalState`). So the "Too many concurrent writers" error - which came from the merge op losing the OCC race to a concurrent writer - disappears. **Do NOT add a write lock or in-process queue** (spec 3.5 forbids it; OCC + append is the design).
- Measured guard: append leaves 62 objects where merge left 2,685; append is bandwidth-bound, merge is commit-latency-bound (`CLAUDE.md` "Copy write path").

### Gotchas

- Keep the absent-whole-session fast path (`append_sessions`/`append_scanner`, ~702-756) - it already appends; only the grown branch changes.
- Keep the in-batch dedup floor (`adapter-integrity-dedup`) and the closing id-set verify on every copy (`session-movement-complete`, 5.4 - exit 6 on a missing row).
- The only remaining `merge_insert` on the data path after this is the tiny insert-only sessions row; that is fine (one row per session).

### Verify

`cargo bench --bench write_bench -- --only append|merge` (the existing regression guard) plus the new `--append-sweep` mode; a copy of grown sessions to S3 scratch must move only the delta rows in one commit per table and pass the closing verify.

## 5. Workstream C - embed at ingest + remove `--no-optimize`

### Problem

Embeddings are written back via `merge_update` on the composite PK `(session_id, id)` (`src/sessions.rs` `write_embeddings` ~1894-1903, fed by `pending_embedding_messages` ~1907; stage in `src/main.rs` `run_embed_stage` ~3060-3176). Lance 7.0.0 index-accelerates only single-column merge keys, so the composite key forces a full key-column scan to LOCATE the rows: measured 8 vectors = 6.36 s / 143 MiB. The write itself is tiny (1 data file + 1 deletion vector) - the join is the cost - and it churns the resident rowmap and relocates rows. Separately, `--no-optimize` (deferred embed+index) is being removed: it loses its purpose once embed is inline and index folds are cheap, and the "210 pending" state it produces is a footgun.

### Target

Fold embedding into ingest so the vector rides the message row's birth append (option D):

- In the ingest write path (`upsert_session_batch`, ~817-1069), before the `append_batches(Messages, ...)` call, compute embeddings for the batch's embeddable messages and populate the `vector`/`embedding_model` columns on those rows, so they are written in the SAME append commit. `search_text` is already computed at the message boundary (`adapter-integrity-event-ordering`); embed from it there.
- Keep the model resident across the process (reuse the existing `LazyEmbedder`); do not load per batch.
- Inference batch size 8-32 (default 32 is fine). Measured: throughput is padding-bound on this corpus, so SMALLER batches are slightly faster (8 ~= 85 msg/s vs 256 ~= 45); keep the length-sorted window. Do not raise the batch for "GPU efficiency" - that is the wrong lever here.
- Gate on the embedding-enabled config (embedding stays opt-in, spec 8). With embedding off, ingest writes null vectors as today.
- Remove `--no-optimize` from `pond sync` and `pond copy` (CLI in `src/main.rs`: help ~360, sync gate ~1127-1133, copy branch ~2007-2012). `pond optimize` remains for explicit maintenance (compaction/version-cleanup) and the model-swap re-embed edge case.

### Binding condition (from the benchmark)

The vector MUST be written in the same append commit as the message rows. The S3 commit floor is ~1 s flat; a median active-coding sync (8 embeds) costs ~0.1 s of inference and the ~1 s commit it already pays - so inline embed adds ~0 marginal latency IF it rides the existing commit. A SEPARATE embed commit costs one extra ~1 s S3 round trip per sync (wasteful). Full backfill is embed-compute-dominated: ~57-65 min on metal for 292k embeddable messages (the same compute the embed stage pays today, just relocated), append adds < 1 min.

### What still uses the writeback `merge_update` (keep, do not delete)

- Model-swap re-embed (accepted rare edge case): re-derives vectors from stored `search_text` for rows whose `embedding_model` differs. This is the one legitimate keyed column-update. It stays in `pond optimize --force-embed`.
- Foreign/`pond copy` of already-embedded rows: vectors arrive as data columns and ride the append - no writeback needed.

### Gotchas

- Preserve `session-embed-from-canonical` (5.5): embeddings are still derived from stored `search_text`, never the source record. Only the TIMING moves (inline at ingest vs a later pass); the derivation source is unchanged. The model-swap path still re-derives from stored `search_text`.
- Do not couple ingest to a remote embedder; the seam stays local-model today (spec 8 "embedding seam").
- `pond sync` no longer has an embed-less fast mode; the 5-minute cron's `pond sync -q` now always embeds inline + folds indexes incrementally (cheap after A). Confirm the cron stays well under the S3 commit budget.

### Verify

`cargo bench --bench embed_bench -- --batch <N>` (throughput unchanged/expected); a sync of fresh sessions to S3 scratch writes vectors in the ingest commit (no separate embed commit), and `pond search` (vector arm) returns the new rows after the incremental IVF fold.

## 6. Spec amendments (required - land with the code, same PR)

`docs/spec.md` is the source of truth; these behaviors change, so the spec changes with them (pre-1.0, no migration notes):

1. **3.7 `lance-index-maintenance`** - update the fold table: BTree and ZoneMap now `optimize_indices(append)` (incremental), not `create_index(replace=true)`; remove the "switch once upstream is fixed" caveat (it is fixed in 7.0.0). Note the ZoneMap no-remap / compaction-rebuild caveat.
2. **7.8 `pond copy`** - replace "the rare grown sessions go through merge-insert" with "grown sessions append only their absent rows after a per-row pre-existence filter (the shared ingest write path)." Keep the append-for-absent and the closing verify language.
3. **7.8 `pond sync` / `pond optimize` + the `--no-optimize` mentions** - remove `--no-optimize`. `pond sync` always imports + embeds inline + folds indexes; `pond optimize` is explicit maintenance + model-swap re-embed.
4. **8 "Producing embeddings"** - change "produced after ingest, never during it" to: produced at ingest, inline, from the `search_text` computed at the message boundary; deferred re-embed (model swap) still re-derives from stored `search_text`. Preserve `session-embed-from-canonical` (5.5) verbatim - only timing changes.
5. **5.5** - note the `vector`/`embedding_model` columns are populated in the ingest commit (still nullable columns on `messages`; no schema migration; null when embedding is disabled).

Mark the breaking commits `<type>!:` so release-plz bumps the minor (per `CLAUDE.md` Process).

## 7. Benchmarks & regression guards

Harness edits for this work are ALREADY in the working tree (uncommitted as of this plan; keep them - validated `fmt`/`clippy -D warnings`/`cargo test --lib embed::` clean, production behavior unchanged):

- `src/embed.rs` - a sweepable `batch_size` field + `EmbedWorker::with_batch_size()` (default 32, mirrors `with_sort_window()`/`with_limit()`).
- `benches/embed_bench.rs` - `--batch <N>`.
- `benches/write_bench.rs` - `--append-sweep "<B,...,bulk>"` + `--sweep-commits-cap` (the S3 commit-floor sweep; first-class re-runnable mode).

Guards to run/keep green:
- `write_bench --only append|merge` and the new `--append-sweep` - prove append stays bandwidth-bound and grown-session copy moves only the delta.
- `embed_bench --batch` - embed throughput per batch size.
- `sync_oracle_bench` - the messages-based oracle stays fast (unaffected, but do not regress).

## 8. Validation checklist (every workstream, before commit/push)

- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` (CI runs `--locked`).
- Real-S3-scratch behavioral check (never write the production `pondarium/pond`; use an isolated scratch prefix and delete it after).
- Confirm no write lock / in-process queue was introduced (spec 3.5).
- Confirm the AIMD limiter was not touched (`CLAUDE.md` rule).

## 9. Out of scope / explicitly do NOT do

- Do NOT adopt option C (separate vector dataset) - rejected; Lance's planner-less crate set makes the search-time `messages x vectors` join painful and breaks prefilter pushdown.
- Do NOT add a write lock, in-process write queue, or external coordinator for contention - append + OCC handles it (spec 3.5).
- Do NOT tune Lance's AIMD rate limiter - leave defaults (`CLAUDE.md` "Object-store request rate").
- Do NOT fold tool/reasoning/system content into `search_text` (`CLAUDE.md` "Search scope is intentional").
- Read-path work (cold-start index page-in, FTS warm cost, the `from_date`/`to_date` defect issue #75) is a separate track - not this plan.

## 10. Measured evidence appendix

- S3 commit floor ~1.0 s, flat 1..512 rows/commit; bulk single-commit 7,387 rows/s (S3) / 61k/s (local).
- Embed (metal, e5-small): ~75-85 msg/s aggregate (batch 8 optimal, default 32 fine; padding-bound); full backfill 292,064 embeddable msgs ~57-65 min.
- Copy merge regression: 4 grown sessions, 6,044 msgs re-scanned for 10 new, ~463 s + OCC conflict; append vs merge = 62 vs 2,685 objects, 5.47x (full-corpus write_bench).
- Index finalize: ~520 s full BTree/ZoneMap rebuild on a tiny delta (no tiny-delta guard); the cost is the full source-column scan, removed by the append fold.
- Embedding writeback: 8 vectors = 6.36 s / 143 MiB composite-key locate scan (no single-column index acceleration on 7.0.0); write itself tiny.
- Corpus shape: ~13.6% of messages embed (user/assistant w/ search_text); per session avg 25 / median 8 / p90 51 / max 3,013 embedded; total store ~2.14M messages / 11,773 sessions / 292k embeddable.
