# openclaw: ingest every rotated-out session generation - plan (2026-09-10)

Goal: fix [#224](https://github.com/tenequm/pond/issues/224). On file-era OpenClaw hosts (every
stable release through 2026.7.1) the openclaw adapter ingests only the transcript each routing
key currently points at, because it resolves a transcript's session key solely from
`sessions.json` `sessionId`, and `sessions.json` maps each key to its CURRENT session only. Every
rotated-out generation - isolated cron runs, hook runs, isolated heartbeats, compaction
predecessors, reset archives - is dropped without a count. The reporter's host ingested 13
sessions out of ~1.8k real transcripts.

Status (2026-09-10): BOTH ERAS IMPLEMENTED, committed and pushed on
`fix/openclaw-rotated-sessions`. Draft PR #228, CI fully green on `5bc4f8b`
(build-and-test, flake-check, windows-verify, release-note-lint); the three
commits after it are one origin/main merge and two documentation commits.

- `3b70b7b` file-era fixtures, `63495c4` file-era fix (OpenClaw <= 2026.7.1)
- `087c185` merge of origin/main, which brought in #225 (agy adapter)
- `6f0757f` DB-era reader + the silent-skip fix + three spec corrections
- `1e5ece4` DB-era tests, cron exact keys, first polish round
- `5bc4f8b` `discover()` propagation, dry-run/status reporting, `has_table` shared
- `136fb1d` `docs(benchmarks): record the contaminated gate row and the openclaw
  ingest arms`
- `92c32c4` merge of origin/main, which brought in `df9bb1a` = #232 (a rebuilt
  store no longer inherits the old store's rowmap), the prerequisite section 8
  was waiting on
- `88ea4ad` `docs(benchmarks): the quiet gate re-run, and what it refutes` - the
  clean row section 8 deferred, now taken

Proven end to end on the captured 2026.9.3 root: 8 sessions / 86 messages, up
from 0. 564 tests pass; clippy `--all-targets -D warnings` and fmt clean.

`docs/benchmarks/{results.md,bench-gate-baseline.jsonl}` and this plan are all
committed on the branch.

Every delta in section 4 and every adjudicated decision in section 6 is approved
and built (user OK 2026-09-10).

Two deviations from the plan as written, both decided during implementation:

- Step 4's "counted fallback in the sync summary" is a `tracing::info!` line plus the queryable
  `options.openclaw.session_key_source` tag, NOT a new `AdapterYield` variant. A fallback-key
  session is ingested, not skipped, so no existing `SkipReason` fits, and adding a variant would
  touch every adapter and the orchestrator for a diagnostic.
- Step 5 removed `error_outcomes_for_substream` and the `DROP_REASON_IMMUTABLE_*` constants:
  with both conflict sites keeping the stored labels, nothing emits them any more, and leaving
  public reason keys no code path can produce would be a lie in the histogram's documented
  vocabulary. `dropped_sessions` still counts genuinely invalid Session rows (empty
  `source_agent`). `BatchCounts`/`IngestSummary` gained `relabeled_sessions`.

## 0. Read first

- `packages/pond/src/adapter/openclaw.rs`: `collect_file_sessions` (~L1586) and
  `load_legacy_key_map` (~L1657) - the gate; `session_kind` (~L1031); `resolve_lineage` (~L959);
  `build_session` (~L851); `parse_archive_name` (~L1566); `reconcile_deletions` (~L1782).
- `docs/spec.md`: `model-project-non-empty` (4.5), `model-no-synthesis`,
  `adapter-integrity-no-silent-drops`, 5.2 composite keys, 7.6 immutable `source_agent`/`project`.
- `packages/pond/src/sessions.rs:1203-1222`: the immutable-field rejection path.
- Capture evidence, in-repo: `packages/pond/tests/fixtures/adapter/openclaw-captures/` holds one
  state root per pass (`rotate-reply`, `cron`, `compaction`, `hooks-heartbeat`, plus `db-era`),
  each with its own `capture.sh`. `packages/pond/tests/fixtures/README.md` documents provenance,
  census and sanitization per pass under its `openclaw-captures/*` sections - read those, not the
  capture kit. The kit that produced them (an external, machine-local checkout that drives a
  pinned OpenClaw under a throwaway `$HOME` against a stub model, writing a `REPORT.md` per pass)
  is not part of this repo and is not needed to work on #224; the committed fixtures and their
  README carry everything the plan relies on. Upstream OpenClaw source for the file era is tag
  `v2026.7.1-2`.

## 1. Verified facts (OpenClaw 2026.7.1-2, real captures)

- Transcript header: `{type:"session", version, id, timestamp, cwd, parentSession?}`. No session
  key. `cwd` is `$HOME/.openclaw/workspace` for every session kind.
- Stored keys are always `agent:<agentId>:`-prefixed (`routing/session-key.ts:115`):
  `agent:main:cron:<jobId>`, `agent:main:cron:<jobId>:run:<startedAtMs>` (main-target cron),
  `agent:main:hook:<uuid>`, `agent:main:hook:ingress`, `agent:main:main:heartbeat`.
- Orphan producers (plain `<id>.jsonl` left with no `sessions.json` entry): isolated cron runs,
  every hook run (`forceNew`), isolated heartbeat beats, compaction predecessors
  (`truncateAfterCompaction`), `oc agent` stale rotation. Reply-path resets rename to
  `<id>.jsonl.reset.<ts>`; the main-target cron reaper renames to `<id>.jsonl.deleted.<ts>`.
- Compaction successors and checkpoint branches are named `<ts>_<id>.jsonl` (header `id` = `<id>`)
  and carry `parentSession` = the previous file's absolute path.
- Key sources available on disk (all agree when present):
  1. `sessions.json` entry `sessionId` (base key for cron).
  2. `sessions.json` entry `usageFamilySessionIds` (reply-path rollovers and automatic compaction
     only; never cron/hook/heartbeat/manual compaction).
  3. `sessions.json` entry `sessionFile` basename (covers `<ts>_<id>.jsonl`).
  4. `sessions.json` entry `systemPromptReport.{sessionKey,sessionId}` (run key for cron).
  5. `<transcript-basename>.trajectory.jsonl`: every line's top-level `sessionKey` (run key for
     cron); exists only for transcripts that ran a turn; head-trimmed at 10 MB.
  6. `<root>/state/openclaw.sqlite` `cron_run_logs` (`session_id`, `session_key` = run key;
     `session_id` NULL for main-target runs).
  7. `<root>/state/openclaw.sqlite` `audit_events` (`session_id`, `session_key`; gateway runs).
  8. First user message prefix `[cron:<jobId> <jobName>]` (isolated cron only, `run.ts:865`).
- pond already keys messages on `(session_id, id)`, so entry ids replayed across generations are
  fine.

## 2. Decisions

- Resolve keys through the sources above in order, then normalize a cron `:run:<segment>` suffix
  to the job key `agent:<id>:cron:<jobId>`; keep the exact run key in `options.openclaw`. Hook
  uuids are never stripped.
- Transcripts no source resolves are still ingested with `project = agent:<agentId>` (the owning
  directory); `options.openclaw.session_key_source` records which source resolved each key
  (`agent_dir_fallback` for these), and `options.openclaw.session_key` is omitted on fallback.
- `session_kind` classifies on the key remainder after `agent:<id>:`.
- Session identity comes from the header `id`, not the filename; header `parentSession` paths are
  reduced to an id (basename, strip `.jsonl`/archive suffix, strip `<ts>_`).
- `reconcile_deletions` always preserves fallback-key sessions.
- Core ingest keeps the stored `source_agent`/`project` when a re-submitted session differs,
  writing new messages under the stored values and counting the conflict (spec 7.6 amendment,
  all adapters). Previously stored cron/hook rows therefore stay `openclaw`; new ones are labeled
  correctly. No durable key cache.
- Fixtures and code ship in one PR on `fix/openclaw-rotated-sessions`, fixtures first.

## 3. Plan

1. Fixtures: `packages/pond/tests/fixtures/adapter/openclaw-captures/<pass>/` (one state root per
   pass + `capture.sh`), README section with provenance and census.
2. `session_kind` prefix fix, plus `heartbeat` and `dashboard` rules (+ unit cases for prefixed
   keys).
3. Key-resolution ladder + cron normalization + source tag + in-process memo; header-id identity;
   `parentSession` path -> id.
4. Fallback project, `reconcile_deletions` gate, counted fallback in the sync summary, cron
   `:run:` `.deleted.` ingestion, checkpoint-branch `fork` + cut-point.
5. Core ingest keep-stored-label change + spec 7.6 amendment.
6. Tests: integration tests over each capture pass (every transcript ingested, expected project /
   source_agent / parent per census), re-sync all fresh, unit tests for each helper.
7. Docs: adapter module doc (the `sessions.json` "`sessionId` -> `sessionKey` map" wording is
   wrong), CHANGELOG note on previously stored cron/hook rows.

## 4. Deltas (found during capture) - ALL APPROVED by the user 2026-09-10

1. `openclaw/heartbeat` kind for `...:heartbeat` keys; skip key `heartbeat`.
2. Ingest reaper `.deleted.` archives whose resolved key is a cron `:run:` key (routine
   retention, not user deletion). Other `.deleted.` stay excluded; `reconcile_deletions` keeps
   preserving them.
3. Add sources 4, 7 and 8 to the ladder, each with its own `session_key_source` tag.
4. Checkpoint branches (`agent:<id>:dashboard:<uuid>`, `sessions.json` `parentSessionKey` +
   `label: "Checkpoint branch"`) take relation `fork`, not `spawn`. `resolve_lineage`
   (~L959) currently routes a bare `parentSessionKey` to `spawn`; the label/`dashboard:`
   segment is the only discriminator (compaction successors and branches share the
   `<ts>_<uuid>.jsonl` shape and a path `parentSession`).

   Fork-with-cut-point (spec 4, `parent_message_id`): the branch's `parentSession` header
   resolves to the parent session id; a `compactionCheckpoints[]` record on the PARENT's
   `sessions.json` entry whose `postCompaction.sessionId` equals that id supplies
   `postCompaction.entryId` as the cut-point. Set `parent_message_id` ONLY when exactly one
   checkpoint record matches - in-place compaction (no `truncateAfterCompaction`) writes a
   degenerate record whose `preCompaction.sessionId` == `postCompaction.sessionId`, so a
   session can hold several records naming itself. Otherwise leave `parent_message_id` unset
   (`model-no-synthesis`); never emit it without `parent_session_id` (spec 4, ~L319).
   Verified in `out/compaction/fixture/agents/main/sessions/sessions.json`: branch
   `agent:main:dashboard:0ffe2a4d-...` -> parent `95256a97-...`, checkpoint
   `974d1e87-...` `postCompaction.entryId` = `f72ac723`. The `dashboard:` uuid is NOT a
   checkpointId, so the branch cannot be matched to its checkpoint through its key.

   Also add a `session_kind` rule for `dashboard:` (currently unclassified, so a branch lands
   as `openclaw` main).

## 5. DB era (OpenClaw >= 2026.8.1) - IN SCOPE, same PR

Previously written here as out of scope. That was a misreading: the user's "Approve without
step 8" declined FILING AN ISSUE for it, not fixing it. On being asked again (2026-09-10) they
chose to fold the fix into PR #228 rather than defer it. Do not re-defer.

CORRECTED 2026-09-10 by the real 2026.9.3 capture (`out/db-era/REPORT.md`). This section
previously read "transcripts on 2026.9.3 are STILL FILES ... so pond falls back to the file
path and the section-3 ladder already helps those hosts". **Both halves are refuted.** H1/H2:
`agents/<id>/sessions/` DOES NOT EXIST until something is deleted - no `sessions.json`, no
`<id>.jsonl`, no `.trajectory.jsonl`. There is no writer for any of them
(`session-sqlite-target.ts:224-250`, `session-manager-persistence.ts:198-203`). The file-era
ladder has nothing to read, and pond ingests **0 of the 8** session generations in the capture
fixture - 8 `session_windows` rows across 6 routing keys (H9). Severity is HIGHER than first
stated, not lower.

2026.8.1 dropped `sessions` / `session_entries` / `session_routes`. The replacements
(`v2026.9.3:src/state/openclaw-agent-schema.sql`):

- `session_nodes` - PK `session_key`; `current_session_id`, `entry_json`, `created_via`,
  `parent_session_key`, `spawned_by`, `fork_source_session_key`, `fork_source_session_id`,
  `fork_source_entry_id`, `label`, `archived_at`.
- `session_windows` - PK `session_id`; `session_key`, `previous_session_id`, `reason` in
  (`initial`, `reset`, `rollover`, `fork`, `rewind`, `switch`, `recovery`, `compaction`).
  **One row per generation** - natively what section 3's ladder reconstructs from sidecars.
- `transcript_events` (FK to `session_windows`), and `session_transcript_archives`, the
  "canonical cold-tier owner for reclaimed transcript generations" holding reset/deleted
  generations as blobs (`archive_blob`, `encoding` `identity`|`zstd`, `archive_sha256`); the
  `.deleted`/`.reset` FILE is documented as derived and recreatable from the row.

Capture: DONE (`out/db-era/{REPORT.md,NOTES.md,capture.sh,fixture/}`, 2026-09-10).

Table mapping holds: `sessions` -> `session_windows` (one row per generation - the DB-era
answer to #224), `session_entries` -> `session_nodes.entry_json`, `session_routes` ->
`session_nodes.current_session_id`, fork cut-point straight from `fork_source_entry_id`
(H5 CONFIRMED, no checkpoint inference needed).

**"lineage from `previous_session_id` + `reason`" is REFUTED and must not be implemented.**
H4b: `reason` is NULL on all 8 fixture rows - `bindSessionRoot` hardcodes `reason: null`
(`session-accessor.sqlite-session-row.ts:52`) and the CHECK enum is vestigial; the only
non-NULL writer anywhere is doctor repair, which writes `'recovery'`. `previous_session_id`
is set on only 1 of 8 rows (the idle/daily rollover path alone; cron's two generations are
not chained). See section 8 for the adjudicated lineage rules.

## 6. Adjudicated decisions (Fable 5, 2026-09-10) - ALL approved for THIS PR

User calls: keep the 7.6 behaviour and fix only the false clause (no Section 9 item); fix the
silent-skip in this PR; fix all ten extras in this PR.

### Root-generation ambiguity: DECLINE, do not infer (user, 2026-09-10)

`is_root_generation` finds the generation a key was created as by looking for the single row
with no `previous_session_id`. When a key has SEVERAL such rows the layout cannot say which
came first, and every generation of that key declines its lineage edge - including the one
where the edge is real.

This is not rare. In the db-era capture `previous_session_id` is NULL on 7 of 8 rows, and
BOTH generations of `agent:main:cron:556dc024` are NULL; only the `dashboard:rollover` pair
records it. So a forked key rotated by anything other than the idle/daily rollover path
loses its fork edge entirely, where the unguarded code gave one right edge and one wrong one.

Alternative considered and REJECTED: order the key's generations by `created_at` and give the
edge to the earliest. It recovers the real edge, but it infers a root the source never
states, and this adapter's whole failure mode in #224 was trusting a plausible-looking
derivation. `model-no-synthesis` prefers an absent edge to a real-looking wrong one, and
`resolve_window_key` already resolves its own ambiguity the same way. Consistency with the
sibling rule decided it.

Revisit only with evidence about which paths write `previous_session_id`, not on the strength
of the edges it would recover.

### Spec changes - DONE
- **7.6 (L680)** false repair clause replaced. It claimed a stale label is "repaired by
  `pond erase` and a re-sync"; 5.4's denylist makes that re-sync impossible and
  `pond erase` does not exist (`main.rs:4362`). Now: stale attribution (not cosmetic -
  `project` is the filter scope, and a `source_agent` subpath governs default search
  exclusion), repair is a deliberate migration this spec does not yet define, and erase is
  explicitly NOT that path. Worded WITHOUT a Section 9 pointer, deliberately: the user
  declined the new item, and a dangling pointer is the exact defect found in 551.
- **551** "no v1 source emits it, so every stored graph is depth-one today" deleted - the
  committed compaction fixture refutes it (`e5844304 <- 95256a97 <- {596721bc, 0c202da3}`).
  Now says ingest records every edge at any depth and the cap is a RESTORE limit, with an
  explicit ban on adapters flattening edges to stay inside it (that would make an edge
  depend on ingest order, which `adapter-integrity-additive-sync` forbids).
  **No code change**: `handlers.rs:759-796` already returns `Lineage::TooDeep{child_id}` and
  `main.rs:1988-1998` maps it to exit 2. Only the descriptive sentence was false.
- **315** broadened to "spawned from, forked from, or continued from", matching the lineage
  operations 5.2 (L488) already enumerates (sub-agent spawn, `/compact`, resume, fork). 4
  was narrower than 5.2 AND than the shipped adapter. Added: a rotation starting a fresh
  context is NOT lineage (`model-no-synthesis`) - this is what keeps rollover out.

### Adapter work - DONE

All six landed in `6f0757f`, `1e5ece4` and `5bc4f8b` and are covered by the
DB-era integration tests. The items below are kept as written, as the record of
what was decided and built, not as work outstanding.

1. **Silent skip was a live `session-movement-complete` (L502) violation.** Probing one table
   name, finding no `sessions`, and reporting "up to date" against a DB with 8 windows / 94
   events is "a skip that outruns durability". Era detect from the TABLE SET:
   (1) `session_windows`+`session_nodes`+`transcript_events` -> DB v2;
   (2) `sessions`+`session_entries` -> DB v1 (today's path);
   (3) neither, no `transcript_events` -> file era (quiet fallback is correct);
   (4) anything else, incl. `schema_meta.schema_version` above the known max ->
   `AdapterError::schema` naming path + tables found + the `schema_meta` row; still ingest
   files; `discover()` returns Err, NOT `Ok(0)`.
   Unreadable DB on an agent with no live transcripts: WARN + a sync-summary line, not the
   DEBUG at `handlers.rs:390`. Configured `path` with no `agents/` dir: Config error
   (`openclaw.rs:338`); auto-discovery stays quiet.
2. **Archives**: row is canonical, verify `archive_sha256` over the RAW blob before
   decompress, same-`(session_id,generation)` file counts as `SkipReason::Superseded` (never
   silent). File only when no row, or row fails and file passes. Both `deleted` and `reset`
   reasons in scope. One deletion policy across eras; `reconcile_deletions` switches its
   live-entry probe from `session_entries` to `session_nodes`.
3. **`sha2 = "0.10"`** declared directly - already in `Cargo.lock` twice (0.10.9 via
   aws-sigv4/rsa, 0.11.0 via datafusion-functions/opendal), so zero new crates.
4. **Reset generations**: ingest whole, each `type:"reset"` event as a rule-3 system carrier
   (6.5 L583). No split (would mint ids the source never issued), no Session-level boundary
   index (the sessions merge is insert-only, so it would freeze at first ingest).
5. **Lineage**: fork -> `parent_session_id = fork_source_session_id` +
   `parent_message_id = fork_source_entry_id`; key-only fork (`compaction.branch`) -> parent
   only. `parent_session_key`/`spawned_by` -> resolve ONLY when unambiguous (one window for
   the key, or exactly one with `created_at <=` the child's), else options only. NEVER
   resolve a key through `current_session_id` (it moves on rollover -> a real-looking wrong
   id). Rollover `previous_session_id` -> NOT lineage, options only. `reason` -> mirror to
   options, dispatch on nothing.
6. `.deleted.` non-ingest line added to the module-doc contract (L60-67), naming both tiers,
   the `ingest_deleted` knob, the cron exception, and what `reconcile_deletions=false` costs.

### The ten extras - all in scope
1. Truncating compaction destroys events between syncs, unarchived. Not a pond violation
   (we keep a superset) but document it + recommend `serve --with-sync`/short schedule.
   Confirm `db_session_watermark` (`openclaw.rs:930-940`) is safe post-truncation.
2. State in the module doc that the adapter never dedups by event id ACROSS sessions - the
   composite PK (L488) handles it and a cross-session dedup would be silent loss (L611).
3. Ladder rung 3 (`sessionFile` basename) must REQUIRE a `.jsonl` value: 2026.9.3 puts a
   session KEY there.
4. DB-era cron: `session_windows.session_key` is the base job key; the `:run:<id>` exact key
   lives only in `audit_events` and `task_runs.child_session_key` - add both as DB-era rungs
   so "exact key survives in options" holds across eras.
5. Define the DB-era freshness proof (L502): per window, newest event timestamp + window
   count. `seq` is ordering only, never identity (already at L55-58).
6. Mixed-era hosts: populate `db_ids` from `session_windows` or every doctor-migrated file
   re-ingests as a duplicate.
7. Module doc header (L3-6) names the 2026.7.2-7.x table set as if current - rewrite.
8. `pond erase` cascade depth (recursive vs refuse) - decide before erase ships.
9. Heartbeat unverified for the DB era - keep the file-era rule, flag it.
10. Settle `parse_archive_name` against the real
    `<id>.jsonl.deleted.<ts>.<gen>.zst` filename with a unit test, not by reading.

## 7. Known gaps - what "we ingest everything" does and does NOT cover

Proven: for the captured 2026.9.3 root, 9 distinct session ids exist anywhere in
the agent DB and 8 are ingested. The 9th is the deliberately-deleted session,
excluded as erasure intent and REPORTED ("1 deleted-archive(s) preserved"). Zero
orphans - no `transcript_events` rows whose `session_id` has no window, no
`session_nodes.current_session_id` pointing at a missing window. For the four
file-era captures, a test asserts every transcript file lands.

NOT proven, and the honest way to close each is another capture, not more
reasoning:

1. **DB era v1 (2026.7.2-2026.7.x) has no real capture.** The `sessions` +
   `session_entries` branch is covered only by synthetic fixtures - the same
   "what we believed the format was" that hid #224 in the first place.
2. **Mixed-era (doctor-migrated) hosts have no fixture.** The `db_ids`
   supersession that stops migrated files re-ingesting as duplicates is reasoned,
   not demonstrated. Getting it wrong produces DUPLICATE message rows, not a
   no-op, because the file reader synthesizes its own ordering key.
3. **`reason='reset'` archive rows are untested against real data.** 2026.9.3
   never writes one (reset does not rotate), so that path rests on schema
   evidence alone.
4. **Heartbeat is unverified for the DB era** - the capture did not run one.
5. The capture ran the scenarios the brief asked for. A real host may hold shapes
   never produced here: channel sessions, plugin-owned sessions, subagent spawns.

Knowingly not ingested, by design:

- `deleted` archives unless `ingest_deleted` (reported, not silent).
- A DB-era deleted archive whose derived file retention already removed:
  excluded AND invisible to `reconcile_deletions`, which enumerates FILENAMES.
  Documented as a blind spot in the openclaw module doc rather than papered over.
  Closing it means teaching that pass to enumerate `session_transcript_archives`.
- Events created and destroyed between two syncs by truncating compaction -
  unrecoverable by anyone, not just pond.

Deferred refactors from the first polish pass (all real, none behavioural):
`SESSION_WINDOW_COLUMNS` duplicating `SESSION_COLUMNS`; the decode-tail
duplication between `fetch_archive_lines` and `read_entry_lines`; four functions
past ~100 lines; wildcard `_ =>` arms in the era dispatch.

## 8. The clean bench-gate re-run - DONE

The 2026-09-10 gate row (`5bc4f8b-dirty`, in `bench-gate-baseline.jsonl`) is
CONTAMINATED - builds, tests and another bench ran on the same VM throughout its
30-minute window. `docs/benchmarks/results.md` documents it as such so nobody
reads it as a regression.

The quiet re-run this section deferred has now happened, after #232 merged so the
row carries `rowmap_cold_ms` / `rowmap_warm_ms` instead of the dead
`oracle_warm_ms`. It is recorded in `docs/benchmarks/results.md` under
"bench-gate: quiet Linux re-run (openclaw DB era, #228)": `92c32c4`, clean tree,
`pond schedule stop` for the window, nothing else on the VM, `EQUIVALENCE OK`.

It confirmed contention for half the call and refuted the other half:

- **Confirmed for the `write_copy` family.** `write_copy_ms` 7353 -> 3777 and
  `write_copy_noop_ms` 1086 -> 481 once the machine was quiet.
- **Refuted for `write_index_build_ms`.** It did not recover: 14480 loaded vs
  14284 quiet, a 1% difference. It had already stepped up (9063 -> 12982) between
  two earlier rows that predate this branch's code, and stayed at 13-14.5s for
  every row after. Calling it CPU-bound evidence of contention was wrong twice: it
  did not move with load, and `write_bench` builds its index against an S3 scratch
  prefix, so the time is round trips, not CPU.

The durable conclusion, and the reason this section is worth keeping: **on this
store a single gate row brackets a change only to within roughly a factor of
two**, and running quiet does not fix that. Read a printed delta against the
immediately preceding row as "nothing moved by more than the noise floor", or not
at all. Same conclusion #226 reached from the other direction.

Prerequisites, for the next such run: `pond schedule stop` first (see results.md
2026-08-25 incident), nothing else running on the VM, and a fresh store path if
any ingest timing is involved (#226).

## 9. Benchmark run notes (user request, 2026-09-10)

Run the full bench gate and append a jsonl record. NOT before the DB-era reader, its tests
and the section-4/5 fixes are all done.

- Command: `bash ops/scripts/bench-gate.sh` (or `moon run bench-gate`). Appends one row to
  `docs/benchmarks/bench-gate-baseline.jsonl` and prints the delta vs the previous row.
- Store: the operator config `~/.config/pond/config.toml` points at the REAL production
  store `s3+https://nbg1.your-objectstorage.com/pondarium/pond`. Read probes are read-only.
  The write benches never write that store: `bench-gate.sh:136` sets
  `WRITE_BASE="${STORE_URL%/*}/benchw"`, a SIBLING prefix in the same bucket
  (`pondarium/benchw-*`), swept before the run and s5cmd-deleted after. The script refuses a
  bucket-root scratch glob and warns rather than guessing if creds or s5cmd are missing.
- `s5cmd` and `moon` are present on this box. `hyperfine` is being installed but the gate
  does not call it - it is only used by the ad-hoc release A/B runs recorded in `results.md`.
- Probe ids `PROBE_SID=8b7b9e47-...` / `PROBE_MID=419caaa5-...` are real rows in that store,
  so they resolve from here.
- **Before running:** stop any local pond schedule (`pond schedule stop`). `results.md`
  (~L270) documents a scheduled sync compacting the store during a gate window, orphaning
  index fragments and hard-erroring dated search until `pond optimize --rebuild`. Other
  hosts still push hourly, so the window is never fully quiet - record that in the entry.
- **This will be the first Linux row.** `results.md` §Environments (~L17) states no Linux
  entry exists. Add one for this box before appending, and do not compare the row's absolute
  figures against the mac-m1max/win-5700x3d rows - only ratios travel between environments.
- **The gate does not measure this PR.** It measures store read/write; #224 changes adapter
  decode. The matching measurement is an `ingest_bench` decode A/B, as was done for
  codex-cli in #216 (`results.md` §ingest_bench decode).
- **The openclaw arm is NOT ours to add - PR #225 supersedes it.** The user approved adding
  one (2026-09-10) and it was written and reverted the same day: the `feat/agy-adapter`
  session is replacing `--adapter`'s hand-written two-variant enum with a lookup through
  `pond::adapter::registry()`, which makes every adapter benchable with no bench-side code.
  Verified their seam works for openclaw: `OpenClawConfig` (`openclaw.rs:151-159`) defaults
  every field but `path`, so `factory.open(json!({"path": corpus}))` deserializes. Our
  `ingest_bench.rs` edits are reverted; the file is untouched in this PR.
  **After #225 lands, rebase and re-add exactly ONE predicate** -
  `!name.contains(".trajectory.")` - and nothing more. The archive half is already fixed
  upstream: #225's `count_sources` now counts a file when ANY dot-delimited segment of its
  name is a source type, not by `Path::extension()`, so `<id>.jsonl.reset.<ts>`,
  `<id>.jsonl.zst` and `<id>.jsonl.1` all count for every adapter. Do NOT re-add a
  `name.contains(".jsonl")` test - it would be redundant with theirs.
  What remains ours is adapter-shaped knowledge: openclaw writes `<id>.trajectory.jsonl`
  sidecars that share the transcript extension but are NOT transcripts (pond reads them only
  for the `sessionKey` they carry), so they must not be counted as sources. #225's doc
  comment names this exact case as a known limitation.
- **Trap that would fake the A/B (their issue #226):** `~/.cache/pond/rowmetamap-<hash>` is
  keyed by store PATH and outlives a deleted store, so a rebuilt store at the same path
  reports "up to date" and ingests nothing. It faked two of their measurements. Use a fresh
  store path per bench run, never a recreated one.
- Corpus for the A/B: the committed capture fixtures, NOT the live `~/.openclaw`. Frozen
  input means both arms see byte-identical bytes; #216's codex-cli entry had to caveat that
  its live corpus grew mid-run.
- Reading #225's new `peak rss` line: it is a PROCESS high-water mark, so with `--passes 2+`
  it reports the largest pass (usually pass 1, the cold insert), not the last one.
- When the openclaw decode row exists, paste it into issue #229. The agy work found cost
  dominated by the store's whole-session buffer and an all-rows-at-once Arrow
  materialization at flush, not by the walk or the decode. An openclaw row showing the same
  shape on a completely different source layout (DB era = a rusqlite read path, no file
  walk at all) is a second independent data point that the cost sits downstream of the
  adapter seam. Requested by the `feat/agy-adapter` session.
