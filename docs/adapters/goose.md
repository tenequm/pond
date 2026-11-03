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

Third-party reference (NON-authoritative, used only to cross-check shape archaeology; the on-disk code in `packages/pond/src/adapter/goose.rs` is authoritative for extraction behavior):

- `github.com/vshulcz/deja-vu/blob/main/internal/sources/goose.go`

## Decision table

| # | Decision | Resolution |
|---|----------|------------|
| 1 | source_agent brand | `goose`; kind-subpath taxonomy keyed on `session_type`: user/missing -> `goose`, `sub_agent` -> `goose/sub-agent`, `scheduled` -> `goose/scheduled`, `hidden` -> `goose/hidden`, `terminal` -> `goose/terminal`, plus `goose/gateway`, `goose/acp`. |
| 2 | Session identity | DB session `id` verbatim. Message id = `<session_id>:<messages.id>`. Legacy message id = own `id` field, else `<session>:line<N>` from `options.source.message_id` parity. |
| 3 | Project resolution | DB: `working_dir` -> `project_id` -> compact-repr(session_id). Legacy: `working_dir` (non-empty) -> compact-repr(session_id). Project is always non-empty and derived from real source data (model-project-non-empty, model-no-synthesis). |
| 4 | Ordering key | Per-message `(created_timestamp normalized, id)`: `ORDER BY CASE WHEN created_timestamp > 10000000000 THEN created_timestamp/1000 ELSE created_timestamp END, id`. Legacy messages order by file line, timestamp normalized the same way. |
| 5 | Tool-call correlation | Per-session `tool_call_id -> name` map built from successful `toolRequest` blocks. `ToolResult` resolves its name from that map; a `toolRequest` on error status carries `call_id` only. No guessed links. |
| 6 | Provenance | Conversational: `text`, `image`, `thinking` (with `redactedThinking` injected). Injected: `toolRequest`/`toolResponse`, `toolConfirmationRequest`/`actionRequired`, `frontendToolRequest`, `systemNotification`, `error`, `redactedThinking`, and all unknown types carried as compact-repr text. |
| 7 | Lineage | `parent_session_id` from the DB `sessions` row; mapped to `options.goose.relation` (`child`/`root`). |
| 8 | Deliberate non-capture | Provider/extension inventory stored in non-session tables is not captured; only `sessions` and `messages` are ingressed. |
| 9 | Restore face | Ingest-only: `sessions.db` / legacy JSONL are read but not restored to disk. Follow-up sessions re-imported via the `pond` session import envelope. |
| 10 | Freshness | DB: `MAX(normalized created_timestamp)` microseconds grouped over `messages`. Legacy: tail-peek of the last line's `created` timestamp; empty file / source with no usable watermark -> `SourceWatermark::Empty`. Where both generations hold the same session id, the DB wins (legacy counted as superseded). |
| 11 | Windows | Data dir under `%APPDATA%\Block\goose\data` probe arm; path components joined with `.join()`; JSONL lines trim `\r`; timestamps parsed as UTC. |

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
