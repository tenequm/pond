# opencode SQLite adapter rewrite

Status: planned, grilled, decisions locked 2026-07-14. Branch: `fix/opencode-sqlite-adapter` (worktree). This doc is the full implementation contract; it must be sufficient to execute after context compaction.

## Root cause (the bug)

opencode stopped writing the JSON file tree in v1.2.0 (2026-02-14). Sessions live in a SQLite database `~/.local/share/opencode/opencode.db` (WAL, Drizzle-managed). pond's adapter (`src/adapter/opencode.rs`) reads only the legacy `storage/{session,message,part}/` fan-out tree, so it ingests only stale pre-migration leftovers. User report (Fabian/xeroc): `pond sync opencode` says "up to date, 1 sessions, 8 messages" against a 3.0 GB `opencode.db`. Reproduced locally: DB holds 59 sessions / 1225 messages / 4617 parts; the stale tree beside it has 25 session JSONs.

Upstream timeline (repo `~/pjv/anomalyco/opencode`):

- 2026-01-27 `a48a5a346` "core: migrate from custom JSON storage to standard Drizzle"; relanded 2026-02-13 ("sqlite again #10597"); first release with SQLite storage: v1.2.0 (2026-02-14). The one-time startup migration copied JSON into the DB and left the JSON files on disk.
- 2026-06-02 `ca2acc4f8` removed the JSON-to-SQLite startup migration: users jumping from pre-v1.2 straight to current opencode have tree-only sessions that will never reach the DB. Both sources must be read for completeness (`session-movement-complete`).
- Our fixture snapshot (2026-05-13) and the adapter (built 2026-06-03) both postdate the format's death; the capture machine's stale tree parsed fine so nothing forced the "does the current release still write here?" question.

## Upstream format facts (verified against source + a live DB)

- DB path: `<data-dir>/opencode.db` on channels latest/beta/prod, `<data-dir>/opencode-<channel>.db` otherwise (`packages/core/src/database/database.ts:43-55`). Data dir: `$XDG_DATA_HOME/opencode` = `~/.local/share/opencode`. WAL mode with `-wal`/`-shm` sidecars.
- Schema (`packages/core/src/session/sql.ts`):
  - `session`: typed columns, no JSON blob - `id`, `project_id`, `workspace_id`, `parent_id`, `slug`, `directory`, `path`, `title`, `version`, `share_url`, `summary_additions/deletions/files/diffs`, `metadata`, `cost`, `tokens_input/output/reasoning/cache_read/cache_write`, `revert`, `permission`, `agent`, `model` (JSON), `time_created`, `time_updated`, `time_compacting`, `time_archived`.
  - `message`: `id`, `session_id`, `time_created`, `time_updated`, `data` = the old per-file message JSON minus `id`/`sessionID`.
  - `part`: `id`, `message_id`, `session_id`, `time_created`, `time_updated`, `data` = old part JSON minus `id`/`sessionID`/`messageID`.
- opencode's own hydration (`packages/opencode/src/session/message-v2.ts:80-93`) rebuilds the JSON as `{...row.data, id, sessionID(, messageID)}`; parts ordered `ORDER BY message_id, id`. Session row-to-JSON mapping is `fromRow` (`packages/opencode/src/session/session.ts:59`) producing the `SessionInfo` shape (camelCase, nested `time.{created,updated}`, null columns omitted).
- Timestamp trap: on rows copied by the Feb migration, the `time_created` COLUMN is the migration time, not the message time (observed: row 2026-06-24 vs `data.time.created` 2025-10-07). The truthful timestamp is `data.time.created`; the column is only a fallback. `opencode import` also writes `time_created` from `data.time.created` when present.
- Part `type` union today (`packages/schema/src/v1/session.ts`): `text`, `subtask`, `reasoning`, `file`, `tool` (state union pending/running/completed/error, fused call+result), `step-start`, `step-finish`, `snapshot`, `patch`, `agent`, `retry`, `compaction`. New since our fixtures: `subtask`, `agent`, `retry`, `compaction`, `snapshot`. User messages now carry `agent`, `model`, `system`, `tools` fields. All shapes flow to `options` via `raw_record` unchanged; the existing `map_part` core (fused-tool split, `synthetic` flag -> provenance) transfers as-is.
- `opencode import <file>` (`packages/opencode/src/cli/cmd/import.ts`) accepts `{ info: Session, messages: [{ info: Message, parts: Part[] }] }` and inserts into the DB (re-homing projectID/directory to the current instance). This is the sanctioned external ingest surface.

## Locked decisions (grilled 2026-07-14)

1. Scope of this unit: adapter rewrite + generated fixture + tests. Deferred to later units: drift canary (source-has-newer-unreadable-artifacts warning), fixtures-README process rules. No GitHub issue; Misha pings Fabian when the release lands.
2. SQLite via `rusqlite` with the `bundled` feature, read-only (`mode=ro` URI, `busy_timeout`). pond's first SQLite dependency; static-binary friendly.
3. Live-DB safety: short per-session read bursts, never one long read transaction (a minutes-long snapshot stalls WAL checkpointing). Cross-session consistency is not needed - ingest is additive; the next sync picks up whatever moved mid-run.
4. Config root: `[adapters.opencode].path` now means the opencode DATA DIR (`~/.local/share/opencode`). One-line normalization: a configured path whose basename is `storage` resolves to its parent (existing configs keep working). `probe_default` returns the data dir.
5. Sources: every `opencode*.db` in the data dir (skip `-wal`/`-shm` sidecars; covers channel variants) PLUS the legacy `storage/` tree. Dedup by session id in-adapter: DB wins, tree fills gaps (`adapter-integrity-dedup`); skipped tree duplicates are counted, not silent.
6. Session `raw_record`: mirror opencode's `fromRow` - reconstruct the `SessionInfo` JSON shape (camelCase, nested `time`, null columns omitted). Lossless per `model-lossless-projection` (every non-null column recoverable), and it is the same object `opencode import` accepts.
7. Restore (native AND foreign): emit one `{info, messages:[{info, parts}]}` JSON file per session (children included per `adapter-lineage-complete-restore`), the `opencode import` shape. The file-tree serializer is dead; do not keep it. Conformance: parse the .db fixture, serialize, assert value-equality against the rows' reconstructed JSON.
8. Fixture: GENERATED by running the real pinned opencode CLI (`opencode run`) in sandboxed XDG dirs with staged prompts reaching all part types; committed as plain git (no LFS while it stays small, ~0.5-2 MB after VACUUM). One doctored row (documented UPDATE) replicates the migration-stamp quirk since no current CLI can produce it. The legacy `storage/` tree fixture STAYS to cover the stranded-JSON path.
9. Freshness watermark: per session, the newest message by ULID id order (matching the file gate's filename-order semantics), watermark = its `data.time.created` plus its tool parts' `state.time.end` maxima - NEVER the `time_created` column (migration-stamped rows would re-read forever). A handful of row reads per session; no full-table json scan.
10. Subagent labeling: sessions with `parent_id` set get `source_agent` = `opencode/<agent>` (the session's `agent` column; bare `opencode/subagent` when the column is null - a label, not synthesized source data). Matches the claude-code convention so child sessions leave default search. Already-stored plain-"opencode" children keep their stamp (source_agent immutable; pre-release, no migration).

Engineering defaults (flagged, not re-grilled): message timestamp chain `data.time.created` -> row `time_created` -> session anchor; parts ordered by id; rusqlite work inside `spawn_blocking`; `retry` part staged best-effort (needs a provider failure; if unreachable, record as a known fixture gap in the fixtures README); commit type `feat:` (non-breaking); fmt/clippy/test green + /polish before commit; no commit without explicit user consent.

## Implementation steps

### Step 1 - fixture generation harness (first; adapter tests depend on it)

Produce `tests/fixtures/adapter/opencode/opencode.db` by driving the real opencode CLI:

- Sandbox: temp `HOME`, `XDG_DATA_HOME`, `XDG_CONFIG_HOME`, `XDG_STATE_HOME`; a scratch project dir as cwd. Pin and record the opencode version (`opencode --version`) in the fixtures README.
- Drive `opencode run "<prompt>"` (non-interactive) with a real provider key and a cheap model. Stage sessions covering: plain text turns; reasoning; tool use with completed AND error states; a file attachment (`file` part); a subtask/Task spawn producing a child session with `parent_id` (parent gets `subtask` part); compaction (`compaction` part - drive via a long session or opencode's compact command); `patch` part if reachable (an edit-producing prompt); an archived session (`time_archived`); multiple projects if cheap. `retry` best-effort.
- Post-process: sqlite3 `UPDATE` one message row's `time_created` to months after its `data.time.created` (the doctored migration-stamp row; document the exact row id in the fixtures README); `PRAGMA wal_checkpoint(TRUNCATE)`; `VACUUM`; delete `-wal`/`-shm`. Verify: dump to text (`sqlite3 .dump`) and run trufflehog/gitleaks + JSON parse validation per the fixtures README verification section; confirm no absolute paths leak beyond the sandbox placeholder (`/Users/user/...` style rewrites if any).
- The generation procedure is documented in the fixtures README (per-platform opencode section rewrite); no committed generation script (code-is-documentation, and the capture is a snapshot artifact).
- Update `tests/fixtures/README.md` opencode section: new source-of-truth layout (db + stale tree), capture version, doctored row, closed/open gaps (retry if missed).

### Step 2 - adapter rewrite (`src/adapter/opencode.rs`)

- Add `rusqlite = { version = "<latest>", features = ["bundled"] }` to `Cargo.toml`.
- Root handling: `OpencodeAdapter::new(<data-dir>)`; normalize basename `storage` -> parent. `probe_default`: `~/.local/share/opencode` exists AND (any `opencode*.db` OR `storage/` present).
- Discovery: enumerate `opencode*.db` files (extension `.db` only), open each read-only (`SQLITE_OPEN_READ_ONLY | URI`, `busy_timeout=5000`). Collect session heads: `SELECT id, project_id, workspace_id, parent_id, slug, directory, path, title, version, ... FROM session` (all columns). Then legacy tree sessions via the existing `collect_session_files`, minus ids already seen in a DB (dedup, DB wins; count skipped duplicates).
- Per-session read (short bursts, no long tx): messages `SELECT id, time_created, data FROM message WHERE session_id = ? ORDER BY id`; parts per message `SELECT id, time_created, data FROM part WHERE message_id = ? ORDER BY id` (or one query per session keyed by message_id - either way bounded to the session).
- Record reconstruction: message value = `{...data, id, sessionID}`; part value = `{...data, id, sessionID, messageID}`; feed the EXISTING `build_message_events`/`map_part` pipeline unchanged (fused-tool split, provenance, options/raw_record). Apply `bound_value` to every reconstructed record (the seam cap currently applied in `read_json`); enforce `RECORD_CAP` on the raw `data` text length per record with the same schema-error surface.
- Session mapping: canonical `Session` from columns - `id`, `parent_session_id` = `parent_id`, `project` = `directory` (schema error when absent per `model-project-non-empty`), `created_at` = row `time_created`, `source_agent` = `opencode` or `opencode/<agent>` when `parent_id` is set, `options` = raw_record of the fromRow-shaped `SessionInfo` reconstruction.
- Message timestamps: `data.time.created` -> row `time_created` -> session anchor.
- Freshness/`plan()`: for each DB session, newest message by id (`ORDER BY id DESC LIMIT 1`), watermark from its `data.time.created` + its tool parts' `state.time.end`; legacy-tree sessions keep the existing subtree walk. Same watermark logic in the `events_with` gate (skip Fresh sessions before reading their full subtree).
- Keep the legacy-tree read path (`collect_session_files`, `walk_session_subtree`, `read_one_session`) as the gap-filler; it must keep working when no `.db` exists at the root (adapter pointed at a bare tree, e.g. old fixtures and the normalize rule's parent having only `storage/`).
- Errors: rusqlite failures surface as `AdapterError::io`/`schema` with adapter + db path + session/message id location (adapter-integrity-no-silent-drops); a malformed `data` JSON drops only that record, like today's malformed part file.

### Step 3 - restore rewrite (same file)

- `serialize_native` and `serialize_foreign` both emit `RestoredFile` per session: path `<session_id>.json`, body `{ "info": <session raw_record or reconstructed info>, "messages": [ { "info": {...msg}, "parts": [...] } ] }`.
- Native: `info` from the session raw_record; message/part bodies from their `raw_record`s (which are already the `{...data, id, sessionID(, messageID)}` shape in both eras); synthetic split records (Tool message + ToolResult) skipped - the fused source `tool` part already carries the call+result.
- Foreign: build idiomatic minimal `info`/message/part JSONs from canonical (same field mapping as today's `foreign_part`, re-targeted at the import shape; ToolCall re-fuses into a `tool` part with `state.status = completed` and `input`; canonical Tool/System carrier messages skipped as today).
- Downgrade rule stays: session without raw_record -> foreign.
- Update `write_restored_files` callers/tests expecting the old tree layout.

### Step 4 - tests

Unit tests in `src/adapter/opencode.rs` (placement rule: adapter behavior = unit tests in the source file):

- Fixture-corpus ingest: sessions/messages/parts counts > 0, canonical shape assertions (mirroring the current tests) against the .db fixture; fused-tool split; injected synthetic text; new part types land as injected carriers with raw_record (subtask/compaction/patch/agent/snapshot); child session has `parent_session_id` and `source_agent = opencode/<agent>`.
- Doctored-row test: the migration-stamped message's canonical timestamp equals `data.time.created`, and the freshness watermark for its session ignores the column.
- Dual-source dedup: fixture db + legacy tree containing one overlapping session id and one tree-only session -> overlapping emitted once (DB copy), tree-only ingested.
- Config normalization: path ending in `storage` reads the parent's db.
- `plan` agrees with the `events_with` gate (port of the existing test to the db source).
- Freshness re-read/skip pair (port: append a message row to a copy of the fixture db vs watermark).
- Malformed `data` JSON in one part row drops only that part.
- Native restore conformance: parse fixture db -> serialize native -> value-equality of every message/part against the rows' reconstructed JSON, session info against fromRow reconstruction; synthetic split records absent.
- Foreign restore: serialize a pi-fixture session to the import shape; assert valid JSON with `info`/`messages` structure (there is no re-parse path for import JSON; golden-file review per spec 6.8).
- probe_default finds the data dir.
- Legacy-tree-only root still ingests (the old fixture tree without a db).

### Step 5 - validation and delivery

- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` green locally.
- Manual end-to-end: `pond sync opencode --dry-run` then real sync against the live local `~/.local/share/opencode` (59 sessions expected, up from 25); verify `pond search`/`pond get` return DB-era sessions; verify a second sync is a freshness no-op.
- /polish the diff, then commit (with explicit user consent) as `feat(adapter): read opencode sqlite storage` (plus fixture commit if split), PR to main, release via release-plz flow.

## Execution strategy (subagents)

Implementation is delegated to Opus subagents (per memory: explicit `model: "opus"` on every Agent call; sonnet only for mechanical low-risk chores); the main session stays review-focused and context-lean, verifying each delivered piece against this plan and the spec rules it names. Suggested split:

- Agent A (opus): fixture generation harness run + post-processing + fixtures README update (needs Bash + provider key; interactive checkpoints with Misha for the API key/model choice).
- Agent B (opus): adapter read-path rewrite (steps 2 + freshness) with unit tests against the fixture from A.
- Agent C (opus): restore rewrite + conformance tests (step 3-4 restore parts), after B lands.
- Main session: reviews each diff, runs validation, owns spec-rule compliance (`model-no-synthesis`, `adapter-bounded-values`, `adapter-integrity-*`), sequencing, and the final polish/commit.

Deferred follow-up units (not this branch): drift canary in sync/status; fixtures README process rules (record client version + verify the current release still writes the captured location); optional upstream release watch.
