# Ingest overhaul plan - correctness + perf

Status: approved 2026-05-15. **Implemented and verified 2026-05-15.** Pending one large commit.

## Measured results

Bench harness: `cargo bench --bench ingest_bench --locked -- --source-dir <PATH> ...`

| Project | files | before (Step 0) | after (Step 8) | speedup |
|---|---|---|---|---|
| sati     | 394 |  12.90 s |  1.21 s | 10.7x |
| blackbox | 418 |  23.54 s |  3.39 s |  6.9x |
| surf     | 146 |   3.62 s |  0.47 s |  7.7x |
| agentbox | 937 |  87.60 s |  5.80 s | 15.1x |

Bottleneck shifted from merge_inserts (80% of wall) to decode (66-70%). Per-table merge_insert calls dropped from one-per-session-per-table (e.g. 418 * 3 = 1254 for blackbox) to N-batches * 3 tables (e.g. 5 * 3 = 15 for blackbox), running in parallel via `tokio::try_join!`.

## Correctness results

Reporting before/after on the same corpora:

| Project | before: errors | after: dropped_events + dropped_sessions + skipped_files |
|---|---|---|
| sati     | 12,297 | 0 + 93 + 0 (clean attribution: 93 subagent immutable-conflict rejections) |
| blackbox |      0 | 0 + 0 + 0 |
| surf     |  4,483 | 0 + 11 + 0 |
| agentbox | 39,836 | 256 + 92 + 0 (with 3 partial-session recoveries from skip-bad-line) |

`agentbox.inserted` went from 209,632 to 209,857 - the skip-bad-line in adapters recovered ~225 rows from previously-aborted files (F2 NUL-hole class and bad-surrogate class).

Background and decisions are captured in the conversation transcript at
`~/.claude/projects/-Users-tenequm-Projects-pond/<session-id>.jsonl` and
distill to this plan.

## Why

Real-world measurement against the user's Claude Code corpus surfaced two
gaps in pond's ingest:

1. **Correctness.** Against `~/.claude/projects/-Users-tenequm-Projects-sati/`
   the validator rejected 93 of 394 sessions (24%) over rules that abort the
   *entire session substream* on a single bad event. The "errors=12297" the
   summary printed was 93 sessions x ~130 events each, conflated into one
   integer. Reframed by three parallel senior-engineer reviews:
   - rules themselves are correctness-preserving (Engineer A),
   - abort-the-whole-session granularity is theatre (Engineer C),
   - reporting bundles three unrelated populations into one int.

2. **Perf.** Instrumented bench shows 77-84% of wall time is in
   `merge_insert` calls; pond does 3 commits per session (sessions /
   messages / parts), serially. Lance research confirms manifest cost is
   per-commit, not per-row; batching N sessions + cross-table `tokio::join!`
   is the documented lever.

3. **Self-heal.** User's binding constraint: source `.jsonl` files are
   ground truth; pond must never cause data loss; a fixed pond must always
   converge on re-run. `WhenMatched::DoNothing` (current) doesn't refresh
   stale field values from source - need Level 2 (`UpdateAll`) plus an
   adapter invariant.

## Decisions (locked)

| # | Decision | Choice |
|---|----------|--------|
| D1 | Validator policy | **A** - per-event drop. Bad event dropped + reported via `SyncEvent`, rest of session commits. |
| D2 | UUID fallback | **C** - hold. Decide after forensic data (D6). |
| D3 | `IngestSummary` shape | **A** - split into `inserted, matched, dropped_events, dropped_sessions, skipped_files, storage_errors`. |
| D4 | `design.md` 3.4 rewrite | **A** - "unit of abort is the offending event, not the substream." |
| D5 | "Never-fewer-rows" adapter invariant | **A** - add to `design.md` 2.3 invariants. |
| D6 | Forensic capture | **A + C** - `--show-rejections` flag in `benches/ingest_bench.rs` plus a `pond::sync` tracing target so real `pond sync` runs are visible when piped. |
| D7 | Scope | **A** - one big commit covering correctness + perf. |
| D8 | Skip-bad-line in adapter | **A** - on JSON parse error mid-file, skip the line and continue; do not abort the file. Same for both adapters. |
| Self-heal level | **Level 2** | `WhenMatched::UpdateAll` for sessions/messages/parts (embeddings stay `DoNothing`). Adapter "never-fewer-rows" invariant documented. |

## Implementation order (within the single commit)

1. **Forensic capture** (D6 = A + C).
   - Add `--show-rejections` flag to `benches/ingest_bench.rs`. Buckets
     reasons, prints top counts per corpus.
   - Add `tracing::info!(target: "pond::sync", ...)` per `SessionDone` in
     `sync_with_progress`. Default filter `warn` keeps it silent; users opt
     in with `POND_LOG=info` or `POND_LOG=pond::sync=info`.
   - Run the bench on sati + surf, capture top rejection reasons. **This
     evidence informs Step 7 (D2 decision).**

2. **Skip-bad-line in adapter** (D8 = A).
   - `src/adapter/claude_code.rs::events()`: on JSON parse fail for one
     line, `yield Err(adapter_error)` then `continue` (not `break`). Same
     in `codex_cli.rs`.
   - Each bad line surfaces as one `SyncEvent::SessionDone { status:
     Skipped }` rather than aborting the rest of the file.
   - Manual validation against the known-broken files (F2 nul hole, F3 bad
     surrogate, plus the two from the most recent sync).

3. **Validator per-event drop** (D1 = A, D4 = A).
   - `IngestValidator::push_message`: on mismatch / before-Session, emit
     one Error outcome for that event and continue. No `fail_substream`.
   - `IngestValidator::push_part`: same; if the part's parent Message was
     dropped, drop the part too (skip-until-next-valid-Message-anchor).
   - `flush_session`: commit whatever survived.
   - Remove the N-events-per-rejection cascade.
   - Update `tests/conformance.rs::ordering_contract_rejects_part_before_message`
     to match the new per-event semantic.

4. **Reporting struct** (D3 = A).
   - `IngestSummary { inserted, matched, dropped_events, dropped_sessions,
     skipped_files, storage_errors }`.
   - `add_outcomes` rewired to bucket by class.
   - Bar final-summary line:
     `sync claude-code: inserted=N matched=M dropped_events=K dropped_sessions=S skipped_files=F`
   - Update all callers and tests.

5. **Level 2 self-heal**.
   - `src/substrate.rs::merge_insert`: switch to
     `MergeInsertBuilder::try_new(...).when_matched(WhenMatched::UpdateAll)?`
     for sessions, messages, parts. Embeddings table keeps `DoNothing`
     (computed vectors should not be silently re-emitted by a re-sync).
   - Document the "adapter never produces a subset of what a prior version
     produced for the same source" invariant in `design.md` 2.3.

6. **Correctness gate: re-run bench**.
   - Same three projects (sati / blackbox / surf). Expect rejection rate
     to drop near zero.
   - If meaningful residual rejections remain, dig in before touching D2.

7. **UUID fallback** (D2 = C, decided 2026-05-15 by forensic data).
   - Step 1 forensic data showed 100% of validator rejections (104/104
     sessions across sati + surf) trace to the **"project is immutable"**
     rule, not uuid collisions. Engineer A's UUID-fallback hypothesis
     was wrong. **No change** to the fallback.
   - The root cause is unrelated: Claude Code's **subagent files** under
     `<project>/subagents/agent-<hash>.jsonl` carry the parent session's
     `sessionId` but a different `cwd`. Pond's adapter creates a `Session`
     event with `project = cwd` for each file, so the validator sees the
     same session id with conflicting projects and rejects.
   - **Tracked as a follow-up** (see "Follow-ups after this commit"). The
     per-event drop in Step 3 will report this cleanly as
     `dropped_events = 104` instead of `errors = 12297`; data behavior
     for subagent files is unchanged in this commit.

8. **Perf optimizations** (D7 = A).
   - New `Store::upsert_session_batch(&[(Session, Vec<MessageWrite>,
     Vec<Part>)])` that flushes the three tables in parallel via
     `tokio::try_join!`.
   - `ingest_adapter` accumulates K=100 complete sessions in memory; on K
     reached or end-of-stream, calls `upsert_session_batch`, clears the
     buffer, continues. `SyncEvent::SessionDone` still fires per-session.
   - Re-run bench. Expected: ≥3x speedup on the three projects.
   - If the speedup target isn't hit, drop this step and ship Steps 1-7 +
     9-10. Bench harness is the safety net.

9. **`design.md` amendments** (D4 = A, D5 = A).
   - 3.4: rewrite the "unit of abort" paragraph.
   - 2.3: add the "never-fewer-rows" invariant bullet.

10. **Final gate**.
    - `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`,
      `cargo test --locked`.
    - Bench numbers (before/after) in the commit message.

## Working-tree state at plan approval

Already present but uncommitted (per user instruction "keep everything"):
- `src/handlers.rs` perf probe + `SyncEvent` / `SyncStatus` types + new
  `ingest_adapter` signature.
- `src/substrate.rs` perf probe on `merge_insert` + `Handle::location()`
  and `Handle::storage_options()`.
- `src/sessions.rs` `corpus_stats` + new structs.
- `src/adapter/{mod,claude_code,codex_cli}.rs` discover() trait method.
- `src/main.rs` `sync_with_progress` (bar), `render_status` (tree),
  config_path fix.
- `benches/ingest_bench.rs` and `Cargo.toml` bench entry.
- Test call sites updated for new `ingest_adapter` signature.
- `docs/plan.md` Stage 4 wording change (cutover side-by-side).
- `src/transport.rs` module-doc fix.

## Follow-ups after this commit

- **Subagent file handling.** Claude Code's `<project>/subagents/agent-*.jsonl`
  files reference the parent `sessionId` with a different `cwd`. The pond
  adapter treats each as its own `Session` with `project = cwd`, which
  conflicts with the parent and gets validator-rejected. Forensic data
  showed this is the cause of 100% of current rejections. Needs an
  adapter design pass: either (a) ingest subagent rows as additional
  messages on the parent session with the subagent context in
  `options`, or (b) derive a unique session id per subagent file
  (e.g. `{parent_session_id}/{agent_hash}`) and link via the
  existing `parent_session_id` schema field. Out of this commit's scope.

## What this plan deliberately does NOT do

- Quarantine table (Engineer C's policy B). Deferred to v1.1 once the
  per-event drop policy is exercised in real use.
- Level 3 self-heal (full mirror with `when_not_matched_by_source: Delete`).
  Deferred unless the "never-fewer-rows" invariant proves insufficient.
- Parallel file parsing (claude-kb-style 4-worker pool). Lance research
  said the lever is batching + cross-table join, not file-level
  parallelism. If post-Step-8 perf is still short of target, revisit.
- Touching the wire-level `pond_ingest` handler. Per-event drop applies
  to the CLI sync path; the wire handler retains per-row error reporting
  as today (it's keyed for HTTP clients, not bar UX).

## Risk register

- Step 3 is the largest behavior change. The existing
  `ordering_contract_rejects_part_before_message` test will fail under
  the new semantic; it gets rewritten to match.
- Step 8 introduces concurrent commits across three Mutex-guarded
  `CachedDataset`s. Cross-table is safe (different mutexes) per Lance
  research, but bears watching.
- Combined commit is large. Step 6 (correctness gate before perf) lets
  us pause and ship if Step 8 misbehaves.
