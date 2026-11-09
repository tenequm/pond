# Goose adapter

Last verified: 2026-08-27, aaif-goose/goose main @ 2026-08-27.

## Upstream pointers

Authoritative writer sources (github.com/aaif-goose/goose, `main` branch):

- `crates/goose/src/session/session_manager.rs` - the `sessions`/`messages` SQLite schema and write path (current generation).
- `crates/goose-provider-types/src/conversation/message.rs` - the `MessageContentBlock` type taxonomy (`text`, `image`, `thinking`, `toolRequest`, `toolResponse`, `toolConfirmationRequest`, `systemNotification`, `error`, ...).
- `crates/goose-provider-types/src/conversation/tool_result_serde.rs` - the `ToolResult` serde shape (`status`, `error`, `value`, per-tool result codecs).
- `crates/goose/src/session/legacy.rs` - the pre-1.10 JSONL writer (line 1 session metadata, lines 2+ messages).
- `crates/goose/src/config/paths.rs` - where the data directory lives (default and env overrides).
- `crates/goose-cli/src/commands/session.rs` - CLI-facing session identity and subagent/scheduled classification.
- Field history (git, `--follow` on the writer files): the `sessions`/`messages` schema (`id`, `session_type`, `parent_session_id`, `working_dir`, `project_id`, `created_timestamp`) and the `content_json` block array date from the 1.10 SQLite migration in `crates/goose/src/session/session_manager.rs`; `created_timestamp` is epoch seconds, normalized to seconds when above `10_000_000_000` (millis); the legacy JSONL `content` field holds a JSON array of typed blocks per `crates/goose/src/session/legacy.rs`; the Windows data dir `{APPDATA}\Block\goose\data` is derived in `crates/goose/src/config/paths.rs`.

Third-party reference (NON-authoritative, used only to cross-check shape archaeology; the on-disk code in `packages/pond/src/adapter/goose.rs` is authoritative for extraction behavior):

- `github.com/vshulcz/deja-vu/blob/main/internal/sources/goose.go`

## Decision table

| # | Decision | Answer | Evidence |
|---|----------|--------|----------|
| 1 | source_agent brand | `goose`; kind-subpath taxonomy keyed on `session_type`: user/missing -> `goose`, `sub_agent` -> `goose/sub-agent`, `scheduled` -> `goose/scheduled`, `hidden` -> `goose/hidden`, `terminal` -> `goose/terminal`, plus `goose/gateway`, `goose/acp`. | `session_type` column and subagent/scheduled classification in `crates/goose-cli/src/commands/session.rs`; `sessions.session_type` in `crates/goose/src/session/session_manager.rs` |
| 2 | Session identity | DB session `id` verbatim. Message id = `<session_id>:<messages.id>`. Legacy message id = own `id` field, else `<session>:line<N>` (1-based file line; the metadata line is line 1). | `messages.id` in `crates/goose/src/session/session_manager.rs`; line-1 metadata + line-N messages in `crates/goose/src/session/legacy.rs` |
| 3 | Project resolution | DB: `working_dir` -> `project_id` -> compact-repr(session_id). Legacy: `working_dir` (non-empty) -> compact-repr(session_id). Project is always non-empty and derived from real source data (model-project-non-empty, model-no-synthesis). | `working_dir` / `project_id` columns in `crates/goose/src/session/session_manager.rs` |
| 4 | Ordering key | Per-message `(created_timestamp normalized, id)`: `ORDER BY CASE WHEN created_timestamp > 10000000000 THEN created_timestamp/1000 ELSE created_timestamp END, id\`. Legacy messages order by file line, timestamp normalized the same way. | \`ORDER BY\` on \`messages(created_timestamp, id)\` in \`crates/goose/src/session/session_manager.rs\`; \`created\` on legacy lines in \`crates/goose/src/session/legacy.rs\` |
| 5 | Tool-call correlation | Per-session `tool_call_id -> name` map built from successful `toolRequest` blocks. `ToolResult` resolves its name from that map; a `toolRequest` on error status carries `call_id` only. No guessed links. | \`toolRequest\`/\`toolResponse\` block shapes in \`crates/goose-provider-types/src/conversation/message.rs\` |
| 6 | Provenance | Conversational: `text`, `image`, `thinking` (with `redactedThinking` injected). Injected: `toolRequest`/`toolResponse`, `toolConfirmationRequest`/`actionRequired`, `frontendToolRequest` (legacy — removed upstream 2026-08-28, PR #11645), `systemNotification`, `error`, `redactedThinking`, and all unknown types carried as compact-repr text. | Block type taxonomy in \`crates/goose-provider-types/src/conversation/message.rs\`; \`ToolResult\` serde in \`crates/goose-provider-types/src/conversation/tool_result_serde.rs\` |
| 7 | Lineage | `parent_session_id` from the DB `sessions` row; mapped to \`options.goose.relation\` (\`child\`/\`root\`). | \`parent_session_id\` column in \`crates/goose/src/session/session_manager.rs\` |
| 8 | Deliberate non-capture | Provider/extension inventory and per-session `usage_ledger` token/cost rows stored in non-session tables are not captured; only \`sessions\` and \`messages\` are ingressed. | \`sessions\`/\`messages\` are the only ingressed tables in \`crates/goose/src/session/session_manager.rs\`; \`usage_ledger\` lives outside them |
| 9 | Restore face | Ingest-only: `sessions.db` / legacy JSONL are read but not restored to disk. Follow-up sessions re-imported via the \`pond` session import envelope. | \`restore_unsupported\` on the adapter; no write path exposed (MCP read-only surface) |
| 10 | Freshness | DB: `MAX(normalized created_timestamp)` microseconds grouped over `messages`. Legacy: tail-peek of the last line's `created` timestamp; empty file / source with no usable watermark -> `SourceWatermark::Empty`. Where both generations hold the same session id, the DB wins (legacy counted as superseded). | \`created_timestamp\` in \`session_manager.rs\`; legacy \`created\` in \`legacy.rs\`; tail-peek bounded by \`PEEK_TAIL_CAP\` in \`packages/pond/src/adapter/jsonl.rs\` |
| 11 | Windows | Data dir under `%APPDATA%\Block\goose\data` probe arm; path components joined with `.join()`; JSONL lines trim `\r`; timestamps parsed as UTC. | \`data_dir = {APPDATA}\{author}\{app_name}\data\` (\`author="Block"\`, \`app_name="goose"\`) in \`crates/goose/src/config/paths.rs\` |

## Two storage generations

Goose stores sessions in two generations:

- **SQLite DB** (current, >= 1.10): `sessions/sessions.db` with `sessions` and `messages` tables. The schema is copied verbatim from upstream `session_manager.rs` (see `SESSION_COLUMNS` / `MESSAGE_COLUMNS` in the adapter). `content_json` holds a JSON array of content blocks; `created_timestamp` is epoch seconds (or milliseconds above 10_000_000_000).
- **Legacy JSONL** (pre-1.10): `sessions/<id>.jsonl`. Line 1 is session metadata; lines 2+ are messages as JSON objects with a `content` field that may be a JSON array, a JSON-encoded string, or a plain string.

Both generations are read. When the same session id exists in both, the DB copy supersedes the legacy copy (the legacy file is counted, never silently dropped). Legacy messages keep positional order; the DB uses the normalized timestamp + `id` ordering.

## Content block inventory

`MessageContentBlock` types map to canonical pond parts as follows:

- `text` -> `Text`.
- `image` -> `File` (mimeType + `data`), or a compact-repr injected `Text` carrier when `data` is absent (absence is corruption; never a synthesized empty payload).
- `thinking` -> `Reasoning` (signature carried in `options.goose.signature`).
- `toolRequest` / `toolResponse` -> `ToolCall` / `ToolResult` (injected provenance; name resolved via the per-session tool map).
- `toolConfirmationRequest` / `actionRequired` (toolConfirmation) -> `ToolApprovalRequest`, with the block `id` (or `data.id`) as both `approval_id` and `tool_call_id`; a block with no usable id becomes a compact-repr injected `Text` carrier rather than fabricating an id.
- `actionRequired` (toolConfirmationResponse) -> `ToolApprovalResponse`, approved when `permission` is in {`allow_once`, `always_allow`}.
- `systemNotification` / `error` -> injected `Text` kind, with type carried in `options.goose`.
- Unknown types -> compact-repr injected `Text` carrier (forward-compat; nothing dropped, nothing invented).
