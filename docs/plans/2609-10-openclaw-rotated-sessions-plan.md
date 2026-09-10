# openclaw: ingest every rotated-out session generation - plan (2026-09-10)

Goal: fix [#224](https://github.com/tenequm/pond/issues/224). On file-era OpenClaw hosts (every
stable release through 2026.7.1) the openclaw adapter ingests only the transcript each routing
key currently points at, because it resolves a transcript's session key solely from
`sessions.json` `sessionId`, and `sessions.json` maps each key to its CURRENT session only. Every
rotated-out generation - isolated cron runs, hook runs, isolated heartbeats, compaction
predecessors, reset archives - is dropped without a count. The reporter's host ingested 13
sessions out of ~1.8k real transcripts.

Status: IMPLEMENTED on `fix/openclaw-rotated-sessions`. Fixtures captured from real OpenClaw
2026.7.1-2 and committed; every delta in section 4 approved and built (user OK 2026-09-10).

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
- Capture evidence (durable, outside the repo): `/home/tenequm/pj/openclaw-capture/out/<pass>/REPORT.md`
  for passes `rotate-reply`, `cron`, `compaction`, `hooks-heartbeat`; upstream source at
  `~/pjv/openclaw/openclaw` tag `v2026.7.1-2`.

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

## 5. Out of scope

- Released OpenClaw 2026.8.1+ stores sessions in `session_nodes` / `session_windows` /
  `transcript_events` (no `sessions` table), so pond's DB path silently falls back to files there.
  Separate work; not filed yet by the user's choice.
