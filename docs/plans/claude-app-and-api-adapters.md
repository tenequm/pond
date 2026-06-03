# Plan: Claude app and API session-log adapters

Status: plan only (no code yet). Branch: `feat/add-claude-app-and-api-adapters`.

## Goal

Ingest Claude session logs - and only session logs - from every Claude origin into pond, through the existing adapter seam. Scope was decided with the owner: split adapters by product/origin (where the user thinks the sessions came from), not by file format.

## Decision summary (locked)

| Adapter | `source_agent` | Reads | Status |
|---|---|---|---|
| `claude-code` | `claude-code` | all of `~/.claude/projects/*.jsonl` - CLI sessions AND Desktop "Code" tab sessions (the Code tab writes to the same `~/.claude/projects` store in identical CLI JSONL format) | exists, no change |
| `claude-desktop-app` | `claude-desktop-app` | `~/Library/Application Support/Claude/local-agent-mode-sessions/<acct>/<workspace>/local_<uuid>/audit.jsonl` (+ sibling `local_<uuid>.json` metadata) - Cowork/agent-mode only | new (Phase 2) |
| `claude-ai-export` | `claude-ai-export` | the official export `.zip` -> `conversations.json` only (web chat session logs) | new (Phase 1) |
| `claude-ai` | `claude-ai` | live claude.ai web API (the "api" in the branch name) - same conversations as the export, but auto/recurring and with per-message model/usage | optional (Phase 3) |

Naming follows the existing kebab convention (`claude-code`, `codex-cli`); no sub-variants, no invented suffixes.

Consequence of the product split, stated plainly: Desktop "Code" tab sessions are ingested by `claude-code` and stamped `claude-code`, because they physically live in `~/.claude/projects` in CLI format. `claude-desktop-app` stays Cowork-only and never touches `~/.claude/projects` or the `claude-code-sessions/` wrappers. Net new work now = 2 adapters; `claude-code` needs no changes.

## Empirical grounding (verified on the owner's machine + docs/GitHub/community research, 2026-06-03)

Everything the Claude desktop app stores locally lives under `~/Library/Application Support/Claude/` (~13 GB on this machine). The bulk (12 GB) is `vm_bundles/` - the Cowork sandbox VM disk images (`rootfs.img`, encrypted `sessiondata.img`), which hold no chat data. The relevant transcript stores:

- Web chats (claude.ai): server-side only. The local `IndexedDB/https_claude.ai_0.indexeddb.leveldb` `conversations_v2` store is a binary Chromium LevelDB cache/drafts/index (purged on logout), not the source of truth. The authoritative copy is reachable only via the official export or the web API.
- Cowork / agent-mode: `local-agent-mode-sessions/<acct>/<workspace>/local_<uuid>/audit.jsonl` plus sibling `local_<uuid>.json` metadata. Plain JSON + JSONL, fully readable. On this machine: 11 sessions spanning 2026-01-25 to today, all retained. Anthropic documents Cowork local data as "not subject to Anthropic's standard data retention policies"; no documented local auto-prune. A schema reference already exists in-repo at `tests/fixtures/adapter/claude_app/schema-notes.md`.
- Desktop "Code" tab: writes transcripts to `~/.claude/projects/<encoded-cwd>/<cliSessionId>.jsonl` - the same store and format as the CLI. The `claude-code-sessions/<acct>/<workspace>/local_<uuid>.json` files are GUI metadata wrappers (title, model, `effort`, git `branch`/`worktreeName`/`worktreePath`, `enabledMcpTools`) that point to the real transcript via `cliSessionId`. Verified: the two wrappers on this machine resolve to `.jsonl` files under `~/.claude/projects/...worktrees...`. The CLI `cleanupPeriodDays` (default 30 days) prunes `~/.claude/projects`; this machine retains back to 2026-01-12 (raised or disabled), so the April Code-tab transcripts survive.
- Official export `.zip`: contains `conversations.json` (1488 conversations, ~214 MB), `projects/`, `memories.json`, `design_chats/`, `users.json`. We ingest conversations only.

Retention summary (do not conflate local vs server): CLI/`~/.claude/projects` local cache 30-day default (`cleanupPeriodDays`, `0` disables persistence); Cowork local store no documented prune (retained until deleted; logout/app-update can be destructive); server-side deleted-conversation purge within 30 days; model-training opt-in retains de-identified data up to 5 years. The export is a manual point-in-time snapshot (emailed link, 24h expiry), so the web-chat path is not auto-recurring without the Phase 3 API adapter.

## The adapter seam (recap, with anchors)

A new adapter implements two traits and adds one registry line - no central dispatch enum.

- `AdapterFactory` (src/adapter/mod.rs ~47-73): `name()` (stable kebab name -> config key, CLI arg, `source_agent`), `open(config) -> Box<dyn Adapter>`, `probe_default(env) -> Option<Value>` (auto-discovery under `env.home`), `serialize(session, fidelity) -> Vec<RestoredFile>` (native = lossless replay, foreign = best-effort).
- `Adapter` (src/adapter/mod.rs ~90-116): `discover()` (session count for the progress bar), `events()` / `events_with(oracle)` -> a stream of `IngestEvent` in append-only order per session.
- Registration: add `mod <name>;` + `pub use <name>::<Name>Factory;` and one entry in `registry()` (src/adapter/mod.rs ~321). CLI wiring (`pond sync`, picker, `by_name`) is then automatic.
- Two parsing strategies: `JsonlTree` (src/adapter/jsonl.rs) for one-JSONL-per-session file layouts (handles discovery, bounded parse, freshness skip, replay dedup); or implement `events_with()` directly for anything else (single JSON array, paired metadata files, HTTP stream).
- No-synthesis seam (src/adapter/extract.rs): canonical leaf values are `Extracted<T>`, constructible only via `extract_str`/`extract_bool`/`extract_value`/`extract_compact_repr` reading a `Source`. `serde_json::Value` already implements `Source`. Synthesized literals and fallback defaults do not compile. Values are bounded to `LEAF_CAP` (10 MiB).
- Ordering contract (spec.md#adapter-integrity-event-ordering): per session emit `Session`, then for each message `Message` immediately followed by its `Part`s in ordinal order, before the next message.
- Idempotency: deterministic primary keys + Lance `merge_insert` insert-only (matched PK = skip). `source_agent` and `project` are immutable after first ingest - re-ingest with a different value is a hard `Conflict`, not a silent update.

Canonical model (src/wire.rs):

- `Session { id, parent_session_id, parent_message_id, source_agent, created_at, project: Extracted<String>, options }`. Note: there is no `name`/`title` field on `Session` - conversation titles and summaries go into `options.source`.
- `Message` enum: `System { content: Option<Extracted<String>> }`, `User`, `Assistant`, `Tool` - each `{ id, session_id, timestamp, options }`.
- `Part { session_id, id = "{message_id}:{ordinal:04}", message_id, ordinal, provenance, options, kind }`. `provenance` is mandatory (`Conversational` vs `Injected`; injected is excluded from search, included in restore). `PartKind`: `Text { text }`, `Reasoning { text }`, `File { media_type, file_name, data }`, `ToolCall { call_id, name, params, provider_executed }`, `ToolResult { call_id, name, is_failure, result }`, plus approval variants.

## The `project` field - load-bearing constraint and resolution

`spec.md#model-project-non-empty` plus the seam: `project` is `Extracted<String>`, must be non-empty, and a hardcoded literal (e.g. `"default"`) cannot be produced (it would not compile). So a synthesized default is not an option. It is also not needed - in both new adapters `project` is always derivable from real source data, which satisfies the "sensible default grouping" intent:

- `claude-ai-export`: `project` = `conversation.account.uuid`. Verified present on every conversation (the conversation object has no folder/project key at all - union of keys across 1488 is `account, chat_messages, created_at, name, summary, updated_at, uuid`). Effect: all of an account's loose web chats group under one project (the account uuid) - the honest equivalent of a single default bucket. 225/1488 conversations have an empty `name`, so the title cannot be used for `project`.
- `claude-desktop-app`: `project` = `userSelectedFolders[0]` when present, else `cwd` (the sandbox path `/sessions/<slug>`, always present). Both are real, extracted folder values.

Rule for any future gap: extract some other always-present source field (an id), never synthesize a constant. Synthesizing would require weakening the seam and violates `spec.md#model-no-synthesis`.

## Adapter 1: `claude-ai-export` (Phase 1, highest value, zero risk)

New file `src/adapter/claude_ai_export.rs`; register in `src/adapter/mod.rs`.

- `name()` / `source_agent` = `claude-ai-export`.
- Config: `{ "path": <export.zip | extracted dir | conversations.json> }`. `open()` resolves the path; read `conversations.json` out of the `.zip` using the existing `zip` dependency (no extract-to-disk needed) or read a path directly. `probe_default` returns `None` (manual - the user supplies the export path; the export is not auto-discoverable under `$HOME`).
- Parsing: custom `events_with()` stream (not `JsonlTree` - it is one JSON array, not per-session files). Iterate the array; for each conversation with >= 1 message, emit `Session`, then each message as `Message` + `Part`s in `created_at` order. Conversations with 0 messages are skipped with a count.
- Mapping:
  - `Session.id` = `conversation.uuid`; `created_at` = `conversation.created_at`; `project` = `account.uuid`; `options.source` = `{ name, summary, updated_at, raw_record (bounded) }`.
  - `Message`: `sender` `human` -> `User`, `assistant` -> `Assistant`; `id` = `message.uuid`; `timestamp` = `message.created_at`.
  - `Part`s from `content[]` (block types observed: `text`, `thinking`, `tool_use`, `tool_result`): `text` -> `Text`, `thinking` -> `Reasoning`, `tool_use` -> `ToolCall { call_id = block.id, name, params = input }`, `tool_result` -> `ToolResult { call_id = tool_use_id, name (resolve from the matching prior tool_use in the same conversation; else None), is_failure, result }`. Provenance per spec (conversational for authored text/thinking/tool_use; tool results injected). `ordinal` follows block order; part id via `part_id(message_id, ordinal)`.
  - `attachments[]` / `files[]`: references only (`file_uuid`, `file_name`) - the export carries no bytes. Recommendation: stash these in `options.source.files` rather than emit empty `File` parts. (Decision to confirm.)
  - `parent_message_uuid`: messages form a tree (edits/regenerations). Canonical ordering is by `timestamp` within the session; persist `parent_message_uuid` in message/part `options.source` for losslessness and keep session ordering linear by `created_at`.
- Idempotency: `conversation.uuid` and `message.uuid` are stable, so re-ingesting a later export is insert-only (existing rows skipped, new conversations added).
- `serialize()` native restore: reconstruct a `conversations.json` element from `options.source.raw_record`.
- Tests: do NOT commit the owner's real ~214 MB export. Add a small synthetic, anonymized fixture `tests/fixtures/adapter/claude_ai_export/conversations.json` (2-3 tiny conversations covering text, thinking, a tool_use/tool_result pair, an empty-`name` conversation, and a 0-message conversation). Unit tests in `#[cfg(test)] mod tests` at the bottom of the source file; an integration suite `tests/integration/claude_ai_export.rs` registered via `#[path]` in `tests/integration.rs`.

## Adapter 2: `claude-desktop-app` (Phase 2, fixtures already exist)

New file `src/adapter/claude_desktop_app.rs`; register in `src/adapter/mod.rs`.

- `name()` / `source_agent` = `claude-desktop-app`. Cowork/agent-mode only. Does NOT read `~/.claude/projects` or the `claude-code-sessions/` wrappers (those belong to `claude-code` per the locked decision).
- Config: `{ "path": <local-agent-mode-sessions root> }`. `probe_default` returns `Some({ path })` when `~/Library/Application Support/Claude/local-agent-mode-sessions` exists under `env.home`.
- Parsing: discover `local_<uuid>/` session dirs; for each, read the sibling `local_<uuid>.json` metadata plus `local_<uuid>/audit.jsonl`. This pairs a metadata file with a JSONL transcript, so either drive it with a custom `events_with()` or adapt `JsonlTree` so `session()` reads the sibling metadata. (`audit.jsonl` is one JSON object per line.)
- Mapping:
  - `Session.id` = `sessionId` (the `local_<uuid>` app-session id). `created_at` = `createdAt` (unix ms -> `DateTime`). `project` = `userSelectedFolders[0]` else `cwd`. `options.source` = `{ model, title, systemPrompt, initialMessage, enabledMcpTools, cliSessionId, vmProcessName, raw metadata (bounded) }`.
  - `audit.jsonl` records (`type`): `user` -> `User` (`message.content` may be a string or typed blocks), `assistant` -> `Assistant` (Anthropic Messages shape: `content[]` text/tool_use/thinking, plus `usage`), `system` -> `System` (`subtype: init` is a capability snapshot -> `options.source` or a `System` message), `tool_result` -> `Tool` (`ToolResult` parts), `result` -> end-of-turn summary (`duration_ms`, token totals) -> `options.source` or skip. Timestamps from `_audit_timestamp` (ISO-8601).
  - Tool result names: join via `parent_tool_use_id` to the matching `tool_use` (per-session `tool_use_id -> name` map, same pattern as `claude_code.rs`); unresolved name -> `None`, never a sentinel.
  - Two id layers: `sessionId` (app session) vs `cliSessionId` / `session_id` in audit records (inner agent loop) - keep `sessionId` as the canonical session id and record `cliSessionId` in options.
  - Skip the nested `.claude/` env (it is the inner loop, already represented by `audit.jsonl`; ingesting it would double-count) and the `.audit-key`. `uploads/` holds real attached file bytes on disk: recommendation is to record filenames in `options.source` for now, optionally promote to `File` parts later. (Decision to confirm.)
- Idempotency: `sessionId` stable; audit records carry `uuid` for dedup; `merge_insert` insert-only.
- `serialize()` native restore: reconstruct `audit.jsonl` (+ metadata) from `options.source.raw_record`.
- Tests: reuse the existing fixtures at `tests/fixtures/adapter/claude_app/` (4 sessions, two schema variants, plus `schema-notes.md`). Note the fixture directory is named `claude_app` while the adapter is `claude-desktop-app` - either point the tests at the existing dir or rename the fixtures to match. (Decision to confirm.) Unit tests in-file; integration suite `tests/integration/claude_desktop_app.rs`.

## Adapter 3: `claude-ai` (Phase 3, optional - the "api" in the branch name)

Adds what the export cannot: automatic/recurring sync and per-message `model`/`usage`/`stop_reason` on web chats. Not required for coverage (the export already captures the web chats).

New file `src/adapter/claude_ai.rs`; `name()` / `source_agent` = `claude-ai`.

- Reuse: the `conversation -> Session/Message/Part` mapper from `claude-ai-export`. When this second caller arrives, factor that mapper into a shared function (the seam's "second caller" rule). The only difference is the source of conversations: an authenticated HTTP stream instead of the zip.
- Endpoints: `GET /api/account` (org discovery) -> `GET /api/organizations/{org}/chat_conversations?limit&offset` (+ `/count_all`) -> `GET /api/organizations/{org}/chat_conversations/{uuid}?tree=True&rendering_mode=messages`.
- Auth: `sessionKey` cookie. Source it from the macOS Cookies DB (`~/Library/Application Support/Claude/Cookies`, SQLite, decryptable on darwin) or from an env/config value; on 401/403, error with a clear re-auth recovery message.
- Cloudflare: claude.ai TLS-fingerprints clients and blocks plain Rust HTTP (reqwest/ureq + rustls). A Chrome-impersonating client (e.g. `rquest`) is likely required - a new dependency class for pond (network + auth). Prototype reachability from the target network before committing the dependency.
- Incremental: the list is sorted by `updated_at`; use the `SkipOracle` to compare against pond's last-written timestamp and re-pull only changed conversations.
- Risks: undocumented and ToS-risky (possible account action), and can break without notice. Mark optional.

## Coverage matrix

| Data | Covered by |
|---|---|
| CLI sessions | `claude-code` (existing) |
| Desktop "Code" tab sessions | `claude-code` (existing; same `~/.claude/projects` store) |
| Cowork / agent-mode sessions | `claude-desktop-app` (Phase 2) |
| Web chats (1488), full content blocks + tree | `claude-ai-export` (Phase 1) |
| Web chats with auto-sync + per-message model/usage | `claude-ai` API (Phase 3, optional) |
| Not covered by design | live web sync without the API; the binary IndexedDB cache (redundant with the export); `vm_bundles` (no chat data); Projects/Memory/design_chats (out of scope - session logs only) |

## Phasing / order of work

1. `claude-ai-export` - file-backed, zip, conversations only. Highest value (1488 chats), zero auth/ToS risk.
2. `claude-desktop-app` - Cowork `audit.jsonl`. Fixtures already in-repo.
3. `claude-ai` API - only if auto-sync / model-usage is wanted; reuses the Phase 1 mapper; accepts the `rquest` dependency and ToS risk.

`claude-code`: no change.

## Open decisions to confirm before/while implementing

- `claude-ai-export` `project` = `account.uuid` (recommended; a literal `"default"` cannot compile through the no-synthesis seam).
- `claude-desktop-app` session id = `sessionId` (recommended) vs the inner `session_id`.
- Attachments/uploads: record references in `options.source` (recommended) vs emit `File` parts with bytes.
- Fixture naming: reuse `tests/fixtures/adapter/claude_app/` vs rename to `claude_desktop_app`.
- Whether to build Phase 3 (API) at all, and accept the `rquest` dependency + ToS risk.

## References

- Spec: `docs/spec.md` sections on adapters, the canonical model, session datasets, protocol; anchors `#adapters`, `#model-no-synthesis`, `#model-project-non-empty`, `#adapter-integrity-event-ordering`, `#adapter-native-restore-lossless`.
- Seam: `src/adapter/mod.rs` (factory/adapter traits ~47-116, `registry()` ~321), `src/adapter/extract.rs` (`Source`/`Extracted<T>`, `extract_*`), `src/adapter/jsonl.rs` (`JsonlTree`), `src/wire.rs` (canonical types).
- Template adapter: `src/adapter/claude_code.rs` (factory ~68-91, adapter/events ~298-355, session build ~381-503, message/part mapping ~591-748, tool-name resolution ~888-891, tests ~1074-1350); `src/adapter/codex_cli.rs` for a second example.
- Ingest/storage: `src/handlers.rs` (`ingest_adapter`), `src/sessions.rs` (validator, schemas, `merge_insert`), `src/substrate.rs` (merge-insert behavior).
- Fixtures: `tests/fixtures/adapter/claude_app/` and `tests/fixtures/adapter/claude_app/schema-notes.md` (Cowork); a new synthetic fixture is needed for `claude-ai-export`.
