# Sync/copy durability and remote performance overhaul

## How to use this doc

You are a fresh agent picking up an investigation that is already complete. This doc is self-contained: it has the problems, the reasoning, the measured evidence, and the exact change plan. Read `docs/spec.md` sections 3 (storage substrate), 5 (session datasets), 6 (adapters), 7 (protocol) before touching behavior - the rule mnemonics referenced below (e.g. `lance-append-only`, `no-cross-shard-atomic-write`, `session-durable-copy`, `adapter-integrity-no-silent-drops`) live there. The performance facts here are also recorded compactly in `AGENTS.md` ("Sync change-detection oracle" and "Copy write path" sections); this doc is the long form.

Do not re-run the investigations. Every number below was measured on the real corpus (local store `~/.local/share/pond`, 11,185 sessions / ~276k messages; remote store `s3+https://nbg1.your-objectstorage.com/pondarium/pond`; adapter source `~/.claude/projects`, 9,383 JSONL files / 5.7 GiB). The benches that produced them are in `benches/` and are the regression guards.

## Delivery constraint (hard requirement)

All changes from this plan MUST land as ONE chunk and ONE commit. Do not split into multiple commits. The single commit also subsumes the already-present uncommitted working-tree artifacts from the investigation (see "Current working-tree state" at the end): the `AGENTS.md` notes and the `benches/sync_oracle_bench.rs` + `benches/copy_bench.rs` extensions. Mark the commit breaking if it changes the oracle/skip wire or copy semantics (`<type>!:`), per the repo's release-plz convention.

## Background: how sync, copy, and adapters work today

pond is a Lance-backed store of agent session transcripts. Three logical tables, each a separate Lance dataset with its own manifest: `sessions`, `messages`, `parts`. Lance has NO cross-dataset transaction - this is load-bearing and the spec encodes it as `no-cross-shard-atomic-write` (spec.md:186): no write batch may span more than one primary-key family atomically.

`pond sync` reads adapter source files (JSONL on local disk, e.g. `~/.claude/projects/<proj>/<uuid>.jsonl`) and ingests them. To stay fast it skips files that have not changed since last ingest. The skip is in `src/adapter/jsonl.rs` `collect_heads` + the loop at lines ~148-163: for each file it reads `mtime`, looks up a per-session watermark via the `SkipOracle` (`src/adapter/mod.rs`, trait at ~154), and skips when `mtime <= ingested`. The watermark is produced by `Store::session_last_ingested_at` (`src/sessions.rs` ~1093).

`pond copy --from <store> --to <store>` is store-to-store. It plans an incremental delta in `plan_incremental_from` (`src/sessions.rs` ~559) using per-session MESSAGE COUNTS (`all_session_message_counts`), routes absent sessions to APPEND and grown sessions to MERGE, and streams via `copy_delta_from`. The closing id-set verify (`verify_stores`, `src/main.rs` ~2689) proves the destination is a superset.

All writes go through `merge_insert` with `WhenMatched::DoNothing` + `WhenNotMatched::InsertAll` (`src/substrate.rs` ~1901). Sync's write path is `upsert_session_batch` (`src/sessions.rs` ~763), which fires three independent `merge_insert_chunks` (sessions, messages, parts) concurrently via `tokio::try_join!` (~983).

## Part 1: The problems

### Data-loss audit (five real paths; storage substrate is sound)

A four-agent audit plus empirical reproductions found five silent-loss paths. The Lance storage substrate itself is safe: `merge_insert` never deletes (`WhenMatched::DoNothing`, `WhenNotMatchedBySource` never enabled), compaction veto only removes rewrite tasks, GC shields the latest manifest + 7-day-unverified files, OCC retries instead of last-writer-wins. The loss is all in the sync/copy orchestration and adapter decode.

1. Sync M1 - non-atomic flush freezes a watermark without its data (PRIMARY; exists on main too). `upsert_session_batch` commits sessions/messages/parts as three independent Lance transactions with no rollback. If the session row commits but the messages commit fails (S3 blip, timeout, token expiry mid-flush), the session row exists and produces a watermark, but the messages do not exist. `WhenMatched::DoNothing` then freezes that watermark forever. Every later sync sees `mtime <= watermark`, fires `Skipped{Fresh}`, and never re-decodes the file. The messages are permanently lost and sync reports "up to date." Reproduced: Probe A confirmed a message-less session DOES carry a watermark. This violates `session-durable-copy` (the source is still present but we refuse to re-read it).

2. Copy parts-only-growth under-copy (branch-new; reproduced). The plan keys on message COUNT, with a whole-table `parts_recovery` backstop (`source_totals.2 > dest_totals.2 && source_totals.1 == dest_totals.1`). A part added to an existing message (message count unchanged) is invisible to the per-session router, and the whole-table backstop is disarmed the instant ANY other session gains a message. Reproduced: Probe C - source had 4 parts, a default copy that printed "done" left the destination with 3.

3. Copy M2 - watermark poisoning (branch-new). Copy stamps new destination session rows with a fresh copy-time row version (Probe B: dest watermark > source watermark). A later sync on the destination then skips source files older than the copy. NOTE: this DISSOLVES once B6 lands - see below.

4. Copy default verify dropped to opt-in (branch-new; spec violation). `spec.md:573` mandates an unconditional closing id-set verify ("exit 6 if the destination is missing source rows"). This branch downgraded it to `--verify-only`. That downgrade is exactly what makes problem 2 SILENT.

5. Adapter silent drops (longstanding). `src/adapter/claude_code.rs:352-356` dedups on bare `uuid` (`HashSet<String>`) and drops same-uuid rows with no counter - the root of the known 1/405825 "0-part vanish". It is correct for byte-identical replays, lossy when a same-uuid pair differs. Also `src/adapter/codex_cli.rs` (~595, unknown `role`) and `src/adapter/claude_desktop_app.rs` (~623, null/non-array `content`) turn a decodable record into `Err` + a dropped-event counter, losing content. All violate `adapter-integrity-no-silent-drops` (spec.md:443).

### Performance findings (all measured)

B6 - the sync oracle is catastrophic on S3. `session_last_ingested_at` derives the watermark from the sessions-table `_row_last_updated_at_version` joined to `Dataset::versions()` commit timestamps. `versions()` is a microsecond local metadata read but a per-manifest-object fetch storm over S3. Measured on the real remote store: 79 s warm / 133 s cold. A messages-based key (`COUNT(*)` / `MAX(timestamp)` / last-id `GROUP BY session_id`) is 0.5-0.6 s warm / 3-5 s cold - 25-160x faster. This 79-133 s is a large slice of the observed 5-minute remote sync. CONTEXT: the commit `7a6601e` on this branch "restored" this oracle (it had been briefly broken into a `MAX(messages.timestamp)` form that never skipped and caused full rescans). The restore fixed the full-rescan but reinstated main's S3-catastrophic version. B6 supersedes `7a6601e` - it replaces the oracle with the fast messages-based key AND keeps the correct comparison.

C8 - the copy append fast-path is load-bearing on S3. Forcing absent sessions through merge-insert instead of append is 5.47x slower on the real corpus to S3: append 13.8 min / 1 commit per table / 62 objects; merge 75.7 min / 354 commits / 2,685 objects. Merge over S3 is commit-latency-bound (one commit per chunk = one round-trip); append is bandwidth-bound. Absent rows cannot collide, so they MUST append. This is why a write-path unification must keep append-for-absent as a first-class mode and never collapse everything into merge_insert.

## Part 2: Design principles (the reasoning)

Durability model is idempotent-replay, not transactions. The spec forbids cross-table atomic writes (`no-cross-shard-atomic-write`) and Lance cannot do them. Safety comes from `lance-append-only` + `lance-deterministic-pk`: any partial write heals by re-running, BECAUSE re-ingest is a no-op for rows already present. The bugs above are all cases where something SUPPRESSES that heal (a frozen watermark, a silent skip, a dropped verify). Fix the suppressors; do not reach for transactions.

Correctness must be stateless ground-truth; the skip is only an optimization. The freshness skip may only ever cause a redundant re-decode (safe), never a silent skip of changed-or-incomplete data. The authoritative correctness layer is stateless: idempotent merge on a deterministic PK (sync) and id-set verify (copy). A destination-stamped timestamp marker that is read back as authoritative is the anti-pattern - it can disagree with the data (M1) and is compared across clocks under multiple writers (clock skew). Prefer signals derived from actual content (per-session last message-id / count), which are clock-free and safe under multiple sources by construction (session ids are globally unique; the store is the union).

Commit the watermark-bearing row last. The session row is the thing that makes a session "seen". Commit messages+parts first, the session row only after both succeed. Then a partial first-ingest leaves no session row -> no watermark -> the session is invisible and re-ingested idempotently. This closes M1 with ordering, not transactions, at the cost of one tiny reordered commit.

Source change-detection is local and clock-free. Adapter files always live on the local filesystem regardless of where the store lives, so source-side detection (mtime vs last-record tail-peek) is backend-independent. mtime is clock-coupled and decoupled from data completeness; a JSONL last-record-id tail-peek compared against the store's per-session last-id is stateless and clock-free, and it actively heals M1's store-behind case. It is blind only to in-place mid-file edits of an already-complete session, which append-only JSONL sources do not do.

One controlled write seam, two modes. Custom write paths popping up in corners is what forced this investigation. The fix is a single shared write seam exposing both `append` (absent/new rows - validated fast path) and `merge` (grown/existing rows), with routing decided by the plan. Sync and copy both go through it. The append mode is mandatory and benchmark-guarded.

## Part 3: The change plan (sequenced; one commit)

### Phase 1 - stop the data loss

- A1 Commit the session row LAST. In `upsert_session_batch` (`src/sessions.rs` ~983), run the messages + parts `merge_insert_chunks` first (in parallel), then commit the sessions chunk only after both succeed. Orphan message/part rows from an aborted run are benign (deterministic PK, deduped on the healing run). This is the M1 fix.
- A2 Restore the unconditional copy id-set verify. In `run_store_to_store_copy` (`src/main.rs` ~2598), call `verify_stores` at the end of every copy (not just `--verify-only`), exit 6 if the destination is missing source ids. Measured cost on S3: ~3-4 s - trivial against a multi-minute copy. This is the spec-mandated backstop (spec.md:573) that makes problem 2 non-silent.
- A3 Per-session parts signal in the copy plan. In `plan_incremental_from` (`src/sessions.rs` ~559), add a per-session parts key (e.g. `COUNT(*) FROM parts GROUP BY session_id`) so a part added under a count-stable message is routed. Measured cost on S3: 1.0 s warm / 3.6 s cold - affordable. A2 is the catch-all backstop; A3 is the cheaper per-copy belt so the verify rarely has to fail.
- A4 Dedup on `(uuid, content-hash)`. In `src/adapter/claude_code.rs:352-356`, change the bare-uuid `HashSet` so a same-uuid record with DIFFERENT content is kept (or emits a visible typed drop), not silently discarded. Measured cost: hashing all 1.59M records of the corpus adds 0.010 s - effectively free.
- A5 Lossless carriers for codex/desktop. `src/adapter/codex_cli.rs` (unknown `role`) and `src/adapter/claude_desktop_app.rs` (null/non-array `content`) must fall back to the lossless carrier the unknown-block-type paths already use, instead of `Err` + drop.
- Tests: turn the reproductions into permanent regression tests (see Part 5).

### Phase 2 - perf and the stateless redesign

- B6 Replace the sync oracle. Replace `session_last_ingested_at`'s `versions()`-join with a messages-based per-session key (last message-id, or count). Measured: 79 s -> 0.5 s warm on S3. This SUPERSEDES `7a6601e` (replace the function body, do not add alongside) and closes M2 (the watermark now derives from actual messages copy carries, so there is no copy-time stamp to poison). Keep the correct comparison semantics; do NOT reintroduce the `MAX(messages.timestamp)`-vs-file-mtime bug that caused full rescans (file mtime is always newer than the newest message inside, so that comparison never skips).
- B7 Source skip: mtime -> tail-peek last-id. Replace the `mtime <= ingested` check (`src/adapter/jsonl.rs` ~148-163) with reading the JSONL file's last record id and comparing it to the store's per-session last-id. Stateless, clock-free, single path - NO `--recheck` mode. Measured marginal cost: +150 ms warm to tail-peek the whole 9,383-file corpus (sync already opens every file for the header, so the tail read is incremental). The current sync already does a header read per file; this adds a tail read.
- A3 also belongs here if you prefer to land all the oracle/plan key changes together; it is listed in Phase 1 because it is a data-loss catch.

### Phase 3 - structure (lock it in)

- C8 Unify the write seam with BOTH modes. One shared seam exposing `append` (absent/new - the load-bearing fast path) and `merge` (grown/existing). Refactor copy's bespoke append/merge_scanner path and sync's `upsert_session_batch` to route through it; the plan decides the mode. Do NOT drop append-for-absent (5.47x S3 regression). Note: sync's streaming per-flush ingest does not pre-plan absence the way copy does, so applying append-for-absent to sync-to-remote is more involved - treat that as a stretch, not a requirement; the minimum is one seam that exposes both primitives so no caller hand-rolls writes again.
- `pond verify` / reconcile command. An explicit deep stateless full count/id-set comparison between source files (or a source store) and the store - the only body-reading tier. A maintenance verb, NOT a sync mode. This is where the expensive "read everything and prove completeness" lives, for healing historical M1 damage or auditing.

### Cross-cutting (every phase)

Every step emits structured progress/tracing. The motivating anti-pattern is the C8 merge run: 75 minutes, 354 commits, zero output. Sync/copy/optimize must stream phase + per-commit progress through the existing `pond::output` / indicatif stack so a long remote op shows what it is doing live. Make "observable by default" a checklist item on each step you touch, not an afterthought. Validate each change with `cargo fmt` + `cargo clippy --all-targets -- -D warnings` + `cargo test` and present the diff before applying per the repo workflow.

## Part 4: Benchmark data (do not re-run)

Sync oracle, real S3 store, fresh cold process each (`cargo bench --bench sync_oracle_bench -- --url <store> --only <name> --cold-only`):

- current (`session_last_ingested_at`, versions-join): 133 s cold / 79 s warm on S3; 98 ms / 26 ms local.
- messages_group_count: 5.4 s cold / 0.6 s warm on S3; 52 ms / 17 ms local.
- messages_group_maxts: 3.0 s cold / 0.5 s warm on S3; 24 ms / 18 ms local.
- verify_collect_ids_all (A2): 3.2 s cold / 4.2 s warm on S3; 0.8 s local.
- parts_group_count (A3): 3.6 s cold / 1.0 s warm on S3; 34 ms / 9 ms local.

Source-side skip, real `~/.claude/projects` (9,383 files / 5.7 GiB), warm (`/tmp/tailpeek_probe.rs`, std-only):

- stat-only (mtime): 0.024 s (2.6 us/file).
- header peek (current): 0.165 s (17.6 us/file).
- header + tail (B7): 0.319 s (34 us/file). Marginal tail cost +0.154 s. Cold is unmeasured but bounded: 89% of files are < 1 MB so OS read-ahead from the head read likely covers the tail; estimated < 2 s, once, post-reboot. This was accepted as negligible.

Copy append vs merge (C8), full real corpus local-source -> S3 scratch, clean cold each (`cargo bench --bench copy_bench -- --source-url <store> --dest-url <s3-base> --only append|merge`):

- append: 13.8 min, 1 commit per table, 62 objects.
- merge: 75.7 min, 354 commits, 2,685 objects. 5.47x slower.
- Local (synthetic 2000 sessions): append 757 ms vs merge 2770 ms = 3.66x. The penalty is per-row CPU join locally and commit-latency over S3.

Adapter dedup hash (A4), full corpus, 1.59M records (`/tmp/dedup_hash_probe.rs`): marginal hash cost 0.010 s. Negligible.

## Part 5: Regression tests to add (in the single commit)

- M1: a test that a session whose messages commit was skipped/failed does NOT get permanently skipped - i.e. after a partial first-ingest, a re-sync ingests the messages. Probe A pattern: assert the watermark/visibility is tied to message presence under the commit-row-last ordering.
- Copy parts-only-growth: Probe C as a real test - session A gains a part on an existing message while session B gains a message; assert the destination ends with all parts (the verify catches it / the parts signal routes it). Put it in `tests/integration/copy.rs`.
- Oracle/skip: a test that the messages-based oracle yields a correct per-session key and that the tail-peek skip never skips a grown file.
- C8 guard: `benches/copy_bench.rs --only append|merge` is the standing guard; reference it in the seam code comment so a future change that drops append-for-absent is caught.

## Part 6: What NOT to do

- Do NOT make the three tables transactional / merge them into one dataset. Forbidden by `no-cross-shard-atomic-write` and impossible in Lance 7.0. Use commit-row-last ordering instead.
- Do NOT derive a per-sync watermark from `Dataset::versions()` on a remote store (79-133 s).
- Do NOT compare file mtime against `MAX(messages.timestamp)` (file mtime is always newer -> never skips -> full rescan; this was the briefly-shipped bug `7a6601e` reverted).
- Do NOT route absent sessions through merge-insert on a remote store (5.47x; commit-latency-bound).
- Do NOT add a `--recheck` two-mode skip; the single stateless tail-peek path replaces mtime outright.
- Do NOT split the work into multiple commits.

## Part 7: Open items

- The user observed "failed events" on a real sync that they could not locate in shell history. When those logs surface, classify them: if they are storage/write errors, M1 is firing LIVE (real loss in progress, raises Phase 1 urgency); if they are adapter decode drops, they are the longstanding adapter drops (problem 5) that the broken-watermark full-rescan re-surfaced on every run. Not blocking, but verify.

## Current working-tree state (fold into the single commit)

Uncommitted artifacts already produced by the investigation, to be included in the one commit:

- `AGENTS.md`: added "Sync change-detection oracle (S3 perf, measured)" and "Copy write path: the append fast-path is load-bearing" sections. Also a `session_last_ingested_at` doc-comment correction.
- `benches/sync_oracle_bench.rs`: added `parts_group_count` (A3) and `verify_collect_ids_all` (A2) candidates.
- `benches/copy_bench.rs`: added `merge_copy`, `--dest-url` (S3 dest via config creds), `--source-url` (use an existing store as source, no seed), and `--only append|merge` (cold-isolated runs).
- This doc.
- Note: `7a6601e` is already committed on the branch; B6 supersedes its oracle. The throwaway `/tmp/tailpeek_probe.rs` and `/tmp/dedup_hash_probe.rs` are outside the repo and need not be kept.
