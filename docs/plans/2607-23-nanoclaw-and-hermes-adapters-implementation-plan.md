# nanoclaw + hermes adapters - implementation plan (2026-07-23)

Goal: add two new source adapters to pond in one PR on this branch (`feat/hermes-and-nanoclaw-support`): `nanoclaw` (NanoClaw, github.com/nanocoai/nanoclaw) and `hermes` (Hermes Agent, github.com/NousResearch/hermes-agent). Both source codebases are cloned for reference at `~/pjv/nanocoai/nanoclaw` and `~/pjv/nousresearch/hermes-agent` (refresh with `git pull` if needed; never clone into this repo).

This document is self-contained: it carries the source-format research (verified against the clones on 2026-07-23) plus all settled design decisions. Where it cites source files of nanoclaw/hermes, line numbers are as of the 2026-07-23 clones - re-verify against the clone before relying on them.

## 0. Read first (pond side)

- `docs/spec.md` section 6 (adapters) and section 4.8 (canonical model rules). The seam is load-bearing: no synthesis (`Extracted<T>`), every Part gets explicit provenance, three placement rules (typed Parts / options / rule-3 System carrier), no silent drops, additive sync.
- `packages/pond/src/adapter/mod.rs` - `AdapterFactory` + `Adapter` traits, registry, shared helpers (`part_id`, `raw_record`, `config_path`, `write_restored_files`), `SkipReason`, `SourceWatermark`, `test_support` (`assert_probe_default`, `assert_native_restore`).
- `packages/pond/src/adapter/jsonl.rs` - the `JsonlTree` engine (tree walk, freshness gate, bounded reads, torn-tail tolerance). nanoclaw rides this.
- `packages/pond/src/adapter/sqlite.rs` - read-only DB plumbing (both adapters use it; hermes is the "known second caller" this seam was built for, per `docs/researches/2607-17-openclaw-integration-research.md` section 6).
- `packages/pond/src/adapter/claude_code.rs` - nanoclaw transcripts are Claude-Code-format JSONL; the record mapping here is what nanoclaw reuses.
- `packages/pond/src/adapter/opencode.rs` - reused for nanoclaw's opencode-provider sessions.
- `packages/pond/src/adapter/openclaw.rs` header comment + `packages/pond/tests/integration/adapter/openclaw.rs` - the closest precedent for session-kind taxonomy, lineage mapping, synthetic-DDL test fixtures, and skip taxonomy.
- `packages/pond/tests/fixtures/README.md` - the nanoclaw fixture section documents the real captured layout (this is measured ground truth; trust it over assumptions).
- Prior decisions that bind this work: capture is "tail the harness's native transcripts" (never a push API); openclaw settled `project` = conversation/shared-state scope and `source_agent` subpath taxonomy (`<name>/<kind>`); the 2607-17 research doc section 6 requires hermes-facing surfaces to stay thin over the same pond primitives.

## 1. nanoclaw - source format reference

NanoClaw is a TypeScript host that runs agents in containers. It has NO transcript format of its own for the Claude provider: the container runs the Claude Agent SDK (`@anthropic-ai/claude-agent-sdk`, driving the `claude-code` binary), which writes standard Claude-Code JSONL. NanoClaw's own SQLite stores are routing/IPC metadata, not transcripts.

### 1.1 On-disk layout

Everything lives under the nanoclaw install root (the checkout directory; `PROJECT_ROOT = process.cwd()`, see `src/config.ts:56-58`). There is no canonical home path.

```
<root>/data/v2.db                                   central metadata DB (better-sqlite3)
<root>/data/v2-sessions/<agentGroupId>/
  .claude-shared/                                   per-GROUP Claude state, mounted as container ~/.claude
    projects/<mangled-cwd>/<sdkSessionUuid>.jsonl   the transcripts (Claude-Code format)
    projects/<mangled-cwd>/<uuid>.jsonl.rotated-<epochMs>   rotated-out complete transcripts
  <sessionId>/                                      per-nanoclaw-session IPC folder
    inbound.db                                      host-written queue (messages_in, session_routing)
    outbound.db                                     container-written (messages_out, session_state)
    opencode-xdg/                                   ONLY for opencode-provider groups: OpenCode's XDG data dir
```

- Container cwd is always `/workspace/agent`, so the mangled project dir is `-workspace-agent` in practice - but glob `projects/*/*.jsonl` instead of hardcoding it (nanoclaw itself scans rather than reproducing the mangling, `container/agent-runner/src/providers/claude.ts:398-416`).
- `.claude-shared` is per agent GROUP; many nanoclaw sessions' transcripts coexist in one projects dir, one file per SDK session UUID.
- Rotation (`claude.ts:490-523`): when a transcript exceeds 12 MiB or 14 days, it is renamed to `<path>.jsonl.rotated-<epochMs>` and a new UUID session starts. Rotated files are complete, ordinary transcripts - ingest them.
- `/clear` starts a new SDK UUID; old file stays. So: multiple files per group is normal.
- Pre-compaction hook writes lossy Markdown summaries to `groups/<folder>/conversations/*.md` - NOT a transcript source, ignore.
- `nanoclaw sessionId` format: `sess-<epochMs>-<rand>` (`src/session-manager.ts:69-71`).

### 1.2 Transcript record shape

Standard Claude-Code JSONL (same family the `claude_code` adapter parses): per-line `{type: "user"|"assistant"|"system"|..., message: {role, content: string | [blocks]}, timestamp: ISO-8601, session_id: <uuid>, uuid, parentUuid, ...}` with `tool_use`/`tool_result`/`thinking` content blocks, per-assistant `model` and `usage`, and SDK subagent nesting via `parent_tool_use_id`.

Two nanoclaw-specific deviations, measured in the committed fixtures (`tests/fixtures/adapter/nanoclaw/`, documented in `tests/fixtures/README.md`):

1. `queue-operation` records interleaved in the JSONL - nanoclaw's own queue-tracking events. They have NO `uuid`/`parentUuid`. Ingest via placement rule 3 (System carrier, whole record in options) - never drop.
2. Sidecar dirs per session: `<sessionUuid>/subagents/agent-<id>.jsonl` (one per subagent transcript, plus a minimal `.meta.json`) and `<sessionUuid>/tool-results/`. The claude_code adapter already handles this subagent layout - mirror its approach.

The fixture set is the schema anchor: real capture `agentgroup-anon-001/` + synthetic `agentgroup-synthetic-001/` (2 structural-replay sessions, one with 3-subagent fan-out). Study these before writing the mapper.

### 1.3 Metadata joins (SQLite, read-only)

The transcript filename UUID is joined back to nanoclaw identity in two hops:

- Per-session `outbound.db`, table `session_state` (key/value): key `continuation:claude` (legacy key `sdk_session_id`) holds the SDK session UUID == transcript filename stem (`container/agent-runner/src/db/session-state.ts:14-16,69-75`; schema `src/db/schema.ts:257-261`). This is the ONLY link from a transcript to a nanoclaw session.
- Central `data/v2.db`: `sessions` (id, agent_group_id, messaging_group_id, thread_id, agent_provider, created_at, last_active, status) -> `messaging_groups` (channel_type, platform_id, name, is_group) -> `agent_groups` (name, folder) -> `container_configs` (provider, model, assistant_name). Task sessions have NULL messaging_group_id and `thread_id = "system:tasks:<seriesId>"`; per-session `inbound.db :: session_routing` (single row) is the fallback routing source.

SQLite quirks (documented invariants, `src/session-manager.ts:1-12`): journal_mode=DELETE (never WAL) because of the host/container bind mount; one writer per file; nanoclaw's own host side opens read-only with busy_timeout=5000 and open-read-close per operation. Docker-Desktop-on-macOS page-cache races can produce transient `database disk image is malformed` - open read-only, set busy timeout, retry-once on malformed, and degrade gracefully (metadata absent, transcript still ingested) if a DB is unreadable. `session_state` columns `series_id`/`trigger`/`source_session_id`/`on_wake` on `messages_in` may be absent on old installs - do not assume them.

Orphans are normal in both directions: a `sessions` row can exist with its folder rm-rf'd (operator reset), and a transcript can outlive its session row. Metadata absence must never block ingestion and must never be synthesized - absent stays absent.

### 1.4 Providers

The base runtime ships ONLY the Claude provider. Codex and opencode are opt-in per-group skills:

- Codex provider: `codex app-server` with SERVER-SIDE history; the continuation is a thread id, explicitly no on-disk transcript. Nothing to ingest. Sessions with `agent_provider = 'codex'` in v2.db should surface as `AdapterYield::Skipped { reason: Unsupported("codex provider keeps history server-side") }` so the count is visible, not silent.
- OpenCode provider: OpenCode's own native storage lands in `data/v2-sessions/<group>/<sessionId>/opencode-xdg/` (its XDG data dir). Pond's existing `opencode` adapter parses exactly this layout. See section 3.4.

## 2. hermes - source format reference

Hermes Agent (Python) persists everything in ONE SQLite database per profile. No JSONL, no per-session files. Central module: `hermes_state.py` (all line refs below are into `~/pjv/nousresearch/hermes-agent/hermes_state.py`).

### 2.1 Location and discovery

- Default home: `~/.hermes` (POSIX; env override `HERMES_HOME`; Windows `%LOCALAPPDATA%\hermes` - out of scope, pond is not doing Windows-specific probing).
- DB: `<home>/state.db` (`DEFAULT_DB_PATH`, line ~154). Profiles: `<home>/profiles/<name>/state.db` - each profile is an independent DB; scan both.
- WAL mode by default (sidecars `state.db-wal`/`state.db-shm` present while live), falls back to journal DELETE on network mounts. Hermes itself sanctions cross-process reads via `sqlite3.connect("file:<path>?mode=ro", uri=True)` (~1582-1588) - which is what `sqlite.rs::open_db` already does.
- NOT transcripts, ignore: `kanban.db`, `sessions/sessions.json` (legacy routing index, no message content), `cron/`, `checkpoints/`, `logs/`, and the memory plugins (mem0/hindsight postgres/redis - a separate subsystem).
- `schema_version` table holds a single row; `SCHEMA_VERSION = 23` at time of writing. Migrations auto-upgrade on open by hermes itself, so a live install reads at the current version. Treat drift like the openclaw adapter treats `schema_meta.schema_version`: best-effort with a warning, rule-3 carriers keep unknown shapes lossless.

### 2.2 `sessions` table (the session row)

Key columns (full DDL at ~999-1047): `id TEXT PK`, `source TEXT` (surface: `local`, `telegram`, `discord`, `whatsapp`, `slack`, `signal`, `matrix`, `email`, ... plus non-gateway values `cli`, `tui`, `cron`, `acp`, `unknown`), `user_id`, `session_key` (platform+chat+thread routing key), `chat_id`, `chat_type` (`dm|group|channel|thread`), `thread_id`, `display_name`, `origin_json` (full serialized SessionSource: user_name, chat_name, scope_id, ...), `model`, `model_config` (JSON), `system_prompt`, `parent_session_id` (FK -> sessions.id), `started_at REAL` (unix epoch seconds, float, UTC), `ended_at`, `end_reason`, token counters, `cwd`, `git_branch`, `git_repo_root`, billing columns, `title`, `profile_name`, `rewind_count`, `archived`.

`parent_session_id` covers three lineage kinds (documented ~3328-3341): compression forks (a session ending `end_reason='compression'` forks a child continuing the same conversation), delegate/subagent spawns, and branch continuations. Compression-fork children inherit the parent's gateway routing columns; delegate children deliberately do not - that inheritance difference plus `end_reason` on the parent is how the adapter distinguishes fork kinds. `async_delegations` table (~1115-1134) carries delegation bookkeeping if needed for disambiguation.

### 2.3 `messages` table (the transcript)

Columns (~1049-1071): `id INTEGER PRIMARY KEY AUTOINCREMENT`, `session_id`, `role` (`user|assistant|tool|system`), `content`, `tool_call_id`, `tool_calls` (JSON array, OpenAI shape, on assistant rows), `tool_name`, `timestamp REAL` (epoch seconds float, UTC, from `time.time()`), `token_count`, `finish_reason`, `reasoning`, `reasoning_content`, `reasoning_details` (JSON), `codex_reasoning_items` (JSON), `codex_message_items` (JSON), `platform_message_id`, `observed`, `active INTEGER` (0 = soft-archived), `compacted INTEGER` (1 = summarized-away by compaction), `api_content` (byte-fidelity copy of the exact string sent to the API).

- Content encoding: `content` is a plain string, OR a JSON payload prefixed with the sentinel `"\x00json:"` (NUL byte + `json:`; `_CONTENT_JSON_PREFIX`, ~5530). Strip the prefix and parse to recover multimodal part lists (`[{"type":"text",...},{"type":"image_url",...}]`); otherwise treat as plain text (`_decode_content`, ~5567-5579).
- Tool calls: `tool_calls` JSON on the assistant row -> `PartKind::ToolCall` per element (call_id/name/params from the OpenAI shape). Tool results: separate `role='tool'` rows -> `Message::Tool` + `PartKind::ToolResult` (call_id = `tool_call_id`, name = `tool_name`, result = decoded content). `tool_calls` may be double-encoded JSON strings in old rows (fixed upstream in #68856) - parse-if-string.
- Reasoning: `reasoning`/`reasoning_content` -> `PartKind::Reasoning` text; `reasoning_details`/`codex_*` -> options (rule 2).
- Ordering: ALWAYS `ORDER BY id`. `timestamp` is non-monotonic by design (hermes docs this; every hermes read path orders by id). Pond re-sorts by (timestamp, id) canonically - preserve source order through ids and keep the source `id` in options so order survives.

### 2.4 Mutation model (the one hard part, resolved)

Hermes rewrites history in place:

- Compaction (`archive_and_compact`, ~5867-5917): one transaction flips prior turns to `active=0, compacted=1` and INSERTs summary rows as fresh `active=1`. Old rows stay on disk.
- Rewind/undo (`rewind_to_message`, ~6610): flips `active=0, compacted=0`; `restore_rewound` flips back.
- `/retry`, `/undo`, `/compress` (`replace_messages`, ~5805-5851): `DELETE FROM messages WHERE session_id=?` then re-INSERT.

Resolution - this is safe for pond because `messages.id` is `AUTOINCREMENT`: SQLite guarantees AUTOINCREMENT rowids are NEVER reused, so a deleted id never comes back carrying different content. Therefore:

- Message identity: `message_id = <session_id>:<messages.id>` (zero-padded via the existing `part_id`-style convention if useful). Stable and collision-free forever.
- Rewrites appear to pond as NEW rows (higher ids). Pond's additive sync keeps the superseded rows as history - the same superset philosophy as openclaw's A3 decision (branch/rewind never invalidates synced rows).
- `active`/`compacted`/`observed` are snapshots at ingest time, recorded in per-message options; additive sync means pond does not track later flag flips. Document this in the module header as a known, accepted divergence (pond is an archive, not a mirror).
- All rows are ingested, `active=0` included - they are real history (pre-compaction turns, rewound turns).

Watermark: `MAX(timestamp)` over ALL rows of the session (micros, like openclaw). Rewrite-inserted rows are stamped with current `time.time()` at rewrite, so they land above the previous watermark; a full re-check remains available via `pond sync --verify` (NoopOracle). Read the newest row cheaply (`ORDER BY id DESC LIMIT 1` is NOT sufficient since timestamp is non-monotonic - use `MAX(timestamp)` directly; it is indexed via `idx_messages_session`).

## 3. Design decisions (settled - do not relitigate)

### 3.1 Identity and attribution

| | nanoclaw | hermes |
|---|---|---|
| adapter `name()` | `nanoclaw` | `hermes` |
| pond session_id | SDK session UUID (transcript filename stem); subagent sidecars = their own sessions per claude_code convention | `sessions.id` verbatim |
| message ids | claude_code convention (record `uuid`; rule-3 carriers get derived ids per existing claude_code/openclaw precedent) | `<session_id>:<messages.id>` |
| `project` | `<agentGroupId>` (the directory name under `data/v2-sessions/` - always present, real source data, and the group IS nanoclaw's shared-state scope: one workspace, one `.claude-shared` per group). Channel/chat identity goes to options. | `session_key` when present; else `<source>:<chat_id>` composite; else `cwd`; else `source`. All components are verbatim source fields routed through the seam - never invent a value. Precedent: openclaw's project = session_key decision (composite conversation identity as the shared-state scope). |
| `source_agent` | `nanoclaw` main; `nanoclaw/subagent` for sidecar subagent transcripts; opencode-provider sessions also `nanoclaw` (see 3.4) | `hermes` main; `hermes/subagent` for delegate/spawn children; `hermes/cron` for `source='cron'` sessions. Compression-fork children stay plain `hermes` (they ARE the conversation continuing). Mirror openclaw's `session_kind` -> `source_agent()` shape. |
| lineage | SDK `parent_tool_use_id` nesting per claude_code handling; nanoclaw-level `source_session_id` (agent-to-agent) goes to options only - it is peer messaging, not parentage | `parent_session_id` verbatim; a `relation` tag (`compaction_successor` / `spawn` / `branch`) in options, derived from parent `end_reason` + routing-inheritance signals; `parent_message_id` stays None (no cut-point tracking, same as openclaw) |

### 3.2 Placement summary

Both adapters follow spec 6.5 strictly. Highlights: hermes `api_content`, `finish_reason`, `token_count`, `platform_message_id`, `active`/`compacted` flags, `reasoning_details`, `codex_*` -> per-message options; hermes session columns mirrored verbatim into `options.hermes` the way openclaw mirrors `SESSION_COLUMNS` into `options.openclaw` (drive SELECT and decode from ONE column list so schema tracking is a one-line change); nanoclaw v2.db/session_state join results -> `options.nanoclaw` (channel_type, platform_id, chat name, agent group name, provider, model, nanoclaw sessionId, thread_id); `options.source = {adapter, raw_record}` on every message for native restore (bounded via the seam). Unknown/unmappable records (nanoclaw `queue-operation`, any unknown hermes role) -> rule-3 System carriers.

### 3.3 Restore (`serialize`)

- nanoclaw: native = replay stored `raw_record`s as JSONL at `data/v2-sessions/<agentGroupId>/.claude-shared/projects/<projectDir>/<uuid>.jsonl` (store the observed `projectDir` and `agentGroupId` in `options.source` at ingest so the path round-trips; `validate_path_id` all segments). Foreign = whatever claude_code's foreign shape produces (the formats are the same family; reuse, do not re-implement).
- hermes: native restore has no file-era format to target. v1 decision: `serialize` emits a single-session SQLite DB (`state.db` bytes containing `schema_version`, the `sessions` row, and its `messages` rows rebuilt from raw_records) when raw records are present - this is loadable/attachable by hermes tooling and value-complete. If that proves disproportionate during implementation, the sanctioned fallback is: always return `actual_fidelity: Foreign` (the seam supports the downgrade and the CLI warns) with an idiomatic NDJSON of the reconstructed rows, and note the limitation in the module header. Do NOT silently skip implementing `serialize` - the trait requires it and the foreign path must work for the cross-adapter matrix.

### 3.4 nanoclaw opencode-provider support (in scope)

Composition, not a new parser: for each nanoclaw session folder containing `opencode-xdg/`, run the opencode adapter's reader against that root and re-attribute the yielded events: `source_agent` -> `nanoclaw` (+`nanoclaw/subagent` where opencode's own taxonomy says subagent), `project` -> the nanoclaw `<agentGroupId>`, provider + nanoclaw session metadata -> `options.nanoclaw`. OpenCode's own session/message ids stay canonical (they are the source ids).

This needs a small seam inside `opencode.rs`: expose the configured reader (root + event stream) `pub(crate)` with an attribution-override hook, keeping all opencode format knowledge in `opencode.rs` and all nanoclaw layout knowledge in `nanoclaw.rs`. Two real callers justify the seam (repo rule). Keep the override surface minimal: `{source_agent_root, project, extra_options}` - do not build a generic re-attribution framework.

Codex-provider sessions: emit `Skipped { reason: Unsupported(...) }` as in 1.4. Providers are detected per session from `v2.db sessions.agent_provider` (may be NULL -> group's `container_configs.provider`, default claude).

### 3.5 Discovery / config

- nanoclaw: config `{ "path": "<install root>" }` (the dir containing `data/v2.db`). `probe_default`: check, in order, `~/nanoclaw`, `~/pj/nanoclaw`, `~/Projects/nanoclaw` for an existing `data/v2-sessions/` dir; return the first hit, else None. An empty candidate must not masquerade as a source (openclaw's `resolve_root` precedent). Config-first is the normal path since installs live anywhere.
- hermes: config `{ "path": "<hermes home>" }`, default probe `~/.hermes` honoring `$HERMES_HOME`, requiring `state.db` to exist. The adapter enumerates `<home>/state.db` plus `<home>/profiles/*/state.db`.

### 3.6 Freshness

- nanoclaw: `JsonlTree` peek path - `peek_session_id` from filename, `peek_watermark` via the existing bounded tail peek (`jsonl.rs::peek_last_mapped`), torn trailing lines tolerated (live containers append mid-write). Rotated files are immutable once renamed.
- hermes: per-session `MAX(timestamp)` micros as in 2.4; read via one grouped query per DB (`SELECT session_id, MAX(timestamp) ... GROUP BY session_id`), then gate with `is_session_fresh` and stream survivors, mirroring `openclaw.rs::events_with`'s enumerate/gate/read-survivors architecture (spawn_blocking + mpsc + `emit!`).

## 4. Work plan

Suggested implementation order (each step leaves the tree green):

1. `packages/pond/src/adapter/nanoclaw.rs` - claude-provider path: JsonlTree impl over `data/v2-sessions/*/.claude-shared/projects/*/*.jsonl` (+ `.rotated-*`), reusing claude_code's record mapping. If the mapping in `claude_code.rs` is not cleanly callable, factor the shared claude-JSONL record mapping into a `pub(crate)` seam (either in `claude_code.rs` or a new `adapter/claude_jsonl.rs`) - two real callers justify it; keep claude_code-install-specific policy (paths, probe) out of the shared part. Handle `queue-operation` rule-3 carriers and subagent sidecars.
2. nanoclaw metadata join: read-only `session_state`/`v2.db` enrichment with the 1.3 quirks (retry-once on malformed, absent-tolerant). Unit tests with synthetic DBs built from DDL (openclaw integration suite pattern).
3. nanoclaw opencode-provider composition (3.4) + codex Unsupported skips.
4. nanoclaw `serialize` (native + foreign) + `test_support::assert_native_restore` + `assert_probe_default`.
5. `packages/pond/src/adapter/hermes.rs` - factory/discovery/enumeration, session mapping (2.2, 3.1), message mapping (2.3), content sentinel decoding, lineage/relation, options mirroring via a single HERMES column list, streaming with the 3.6 oracle gate.
6. hermes `serialize` (3.3) + conformance tests.
7. Registry: two `mod`/`pub use` lines + two `&Factory` entries in `registry()` (`adapter/mod.rs`). Check `discovery.rs` and any user-facing adapter listings (README, `pond init` wizard source list) pick the new names up automatically; update `tests/fixtures/README.md` (nanoclaw section: note it is now exercised by the adapter; add a hermes section) and the README adapter/fixtures table.
8. Integration suites: `packages/pond/tests/integration/adapter/nanoclaw.rs` and `.../hermes.rs`, each registered with a `#[path]` line in `tests/integration/adapter/mod.rs`. nanoclaw drives the committed fixtures (real + synthetic) plus synthetic metadata DBs; hermes builds a synthetic `state.db` from the real DDL (copy the CREATE TABLE shapes from `hermes_state.py` `SCHEMA_SQL`, subset to sessions/messages/schema_version) covering: multimodal `\x00json:` content, tool call/result linking, reasoning columns, compaction flag flips (`active=0,compacted=1` rows ingested), a delete+reinsert rewrite (new ids appear, old pond rows survive - additive re-sync test mirroring openclaw's), all three lineage kinds, profile enumeration, and skip taxonomy. Optionally add either adapter to the foreign-restore matrix in `adapter/mod.rs` (tests) - nice-to-have, not required for this PR.
9. Docs pass: module headers carry the format matrix + documented non-ingest lists (openclaw.rs header is the template); each adapter documents WHAT it deliberately does not ingest (nanoclaw: conversations/*.md summaries, messages_in/out IPC bodies, codex sessions; hermes: kanban.db, sessions.json, memory plugins, FTS shadow tables).

## 5. Test data caveats

- Fixture corpora must stay synthetic or anonymized (repo rule; the nanoclaw real capture is already anonymized as `agentgroup-anon-001`). Never copy from a live `~/.hermes` or a real nanoclaw install into fixtures.
- The committed nanoclaw fixtures may predate some current nanoclaw behaviors (rotation suffixes, `continuation:claude` key). Fixtures are ground truth for record SHAPES; the clone at `~/pjv/nanocoai/nanoclaw` is ground truth for layout/keys - extend the synthetic fixture set where the two disagree, do not mutate the real capture.
- Real-corpus validation (optional, recommended before merge): the operator has live nanoclaw data on a remote host and can run `pond sync nanoclaw --dry-run` there; ask rather than assuming access.

## 6. Process requirements

- One PR from this branch into main. Commits use conventional style; the feature commits should be `feat(adapter): ...` so release-plz derives the bump (do NOT open a release PR or pick a version - release-plz does both).
- `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test` green locally before every push. Test placement rule: unit tests in `#[cfg(test)] mod tests` at the bottom of the source file; only genuine cross-module suites in `tests/integration/adapter/`.
- Keep `adapter/mod.rs` seam-pure: no nanoclaw/hermes specifics leak into it beyond the registry lines. The bounded-values, no-synthesis, and provenance rules are enforced by the type system - if something will not compile without a synthesized value, the design is wrong, not the seam.
- MCP surfaces remain read-only; nothing in this work touches transport/handlers beyond what adapter registration already wires.

## 7. Non-goals (this PR)

- No nanoclaw or hermes RECALL integrations (plugins/providers projecting pond tools into those harnesses) - capture only. The hermes MemoryProvider frontend is a separate later phase (2607-17 research, section 6).
- No push/ingest API; capture stays tail-the-source-of-truth.
- No Windows discovery for hermes.
- No live-watch daemon changes; `pond sync` cadence is unchanged.
- No erase/deletion reconciliation for either source (openclaw's is bespoke to its `.deleted.` archives; neither new source has an equivalent signal worth building against yet).
