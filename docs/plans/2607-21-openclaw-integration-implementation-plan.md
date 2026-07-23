# OpenClaw integration - implementation plan (2026-07-21)

Status: design finalized and confirmed by Misha 2026-07-21 after a full decision-by-decision review. This document is self-contained: a fresh agent implements from this file plus the referenced sources, without needing the design conversation.

Read first: `docs/spec.md` in full (the pond contract - especially sections 4, 5.4, 6, 6.5, 8.2-8.3), then `docs/researches/2607-17-openclaw-integration-research.md` (background research; this plan supersedes its open decisions - everything below is DECIDED).

## 0. What is being built

Three deliverables, in this order:

1. **An `openclaw` adapter in pond** (Rust, `src/adapter/openclaw.rs` + one registry line) that ingests OpenClaw's per-agent SQLite transcript databases and archive files into pond's canonical model. This is the capture half and ships first - OpenClaw prunes history under a disk budget, so every week without the adapter loses data permanently.
2. **`pond serve --with-sync`** - a flag adding an in-process periodic sync scheduler to the existing serve command, so one process serves MCP and keeps the store fresh with one shared embedding model.
3. **An OpenClaw plugin** (TypeScript npm package, thin) that projects pond's read tools into OpenClaw agents with community-aligned access scoping, and manages the pond process so that installing the plugin is the complete installation.

Positioning context (one paragraph, so the code comments make sense): OpenClaw's own memory system handles curated facts and recent in-gateway recall; pond is the durable lossless tier beneath it - permanence past OpenClaw's disk budget, cross-harness corpus, off-gateway indexing, restore. The plugin deliberately does NOT compete with OpenClaw's memory plugins: no memory slot claim, no auto-recall injection, no `before_prompt_build` hook. Tools only.

## 1. Ground facts: OpenClaw's storage (verified against main, 2026-07-21)

Local clone: `~/pjv/openclaw/openclaw`. The schema is young and moving (~300 commits/day); this adapter tracks latest main. All paths below are per-agent.

### 1.1 Locations

- Agent DB: `~/.openclaw/agents/<agentId>/agent/openclaw-agent.sqlite` (WAL mode; one long-lived Gateway process is the sole writer).
- Archives + legacy artifacts: `~/.openclaw/agents/<agentId>/sessions/` - `<sessionId>.jsonl.<reason>.<timestamp>[.zst]` where reason is `deleted` or `reset`; zstd level 3; plain JSONL when the runtime lacks zstd. Legacy pre-SQLite installs may hold plain `<id>.jsonl` transcripts and `sessions.json` here.
- Root override: `OPENCLAW_STATE_DIR` env (`src/config/paths.ts:61-97`). The adapter's `path` config points at the root (default `~/.openclaw`); discovery enumerates `agents/*/`.

### 1.2 Tables (DDL: `src/state/openclaw-agent-schema.sql`)

The session artifact (ingest these):

- `sessions` - `session_id` PK, `session_key`, `session_scope`, `parent_session_key`, `spawned_by`, `chat_type`, `channel`, `account_id`, `model_provider`/`model`, `agent_harness_id`, `status`, ms-epoch timestamps.
- `transcript_events` - `(session_id, seq)` PK, `event_json`, `created_at`. The entry stream. **`seq` is NOT stable**: `replaceSqliteTranscriptEventsInTransaction` (in `src/config/sessions/session-accessor.sqlite-transcript-store.ts`) deletes and rewrites rows with new seqs during repairs/rewinds/message-cuts. Never derive identity or sync watermarks from seq; entry `id` (inside event_json) is the stable identity.
- `session_entries` - SessionEntry JSON per routing key (label, toggles, token counters, and - see 1.5 - the lineage/provenance fields).
- `conversations`, `session_conversations`, `session_routes` - channel routing metadata.
- `session_transcript_generations` - `session_id -> generation` watermark, **rotated in the same transaction as any destructive transcript replacement, preserved by pure appends** (`src/config/sessions/session-accessor.sqlite-transcript-state.ts`: `ensureTranscriptGenerationInTransaction` / `rotateTranscriptGenerationInTransaction`). This is the source-rewrite detector.
- `schema_meta.schema_version` - read for drift detection only.

Derived projections (read if convenient, never a data source of record): `transcript_event_identities` ((session_id, event_id) -> seq/event_type/parent_id - extracted from event_json), `session_transcript_active_events` (active branch materialization), `session_transcript_fts` (their FTS index).

Foreign artifacts (documented non-ingest, spec 6.9): `trajectory_runtime_events` (separate runtime telemetry stream; revisit if users ask for run-level forensics), `board_tabs`/`board_widgets` (UI state), `heartbeat_outcomes` (ops telemetry), `acp_parent_stream_events` (protocol mirror of transcript content).

### 1.3 Entry JSON (pi-coding-agent `FileEntry`, session version 3)

Types: `@mariozechner/pi-coding-agent/dist/core/session-manager.d.ts` (in openclaw's node_modules). pond already has `src/adapter/pi_coding_agent.rs` for the standalone pi harness - same entry family, so its parsing patterns are the in-repo precedent (NOT shared code; OpenClaw's stream is richer and lives in SQLite).

- Header entry: `{type:"session", version, id, timestamp(ISO), cwd, parentSession?}` - first entry of every transcript. `parentSession` marks a compaction-successor link.
- Entry types (9 today, union is OPEN via module augmentation - unknown types WILL appear): `message`, `thinking_level_change`, `model_change`, `compaction` (summary, firstKeptEntryId, tokensBefore), `branch_summary` (fromId, summary), `custom` (extension state, NOT in LLM context), `custom_message` (extension-injected, IS in LLM context), `label`, `session_info`.
- Every entry has `id`, `parentId`, `timestamp` - the transcript is an append-only TREE (branches from rewinds/forks), not a linear log.
- Archives hold the SAME JSONL entry lines - one entry parser serves SQLite rows, archive files, and legacy transcripts.

### 1.4 Message shape (pi-ai; `@mariozechner/pi-ai/dist/types.d.ts`) -> canonical mapping

| OpenClaw | pond canonical |
|---|---|
| `TextContent{text,textSignature}` | TextPart (+signature to part options) |
| `ThinkingContent{thinking,thinkingSignature,redacted}` | ReasoningPart (+options) |
| `ToolCall{id,name,arguments,thoughtSignature}` | ToolCallPart |
| `ToolResultMessage{toolCallId,toolName,isError,content,details}` | ToolMessage + ToolResultPart (`is_failure` = isError) |
| `ImageContent{data,mimeType}` | FilePart (blob via parts `data` column) |
| `usage`/`stopReason`/`api`/`provider`/`model`/`errorMessage`/`responseId` | `options.openclaw.*` |

Provenance (spec `model-part-provenance` - the load-bearing classification):

- User messages carry `provenance.kind in {external_user, inter_session, internal_system}` plus `originSessionId`/`sourceSessionKey`/`sourceChannel`/`sourceTool` (`src/sessions/input-provenance.ts`). `external_user` content -> `conversational`; the `[Inter-session message]` prefix + explanation line is a documented envelope (`INTER_SESSION_PROMPT_PREFIX_BASE`, same file) - split at the exact byte boundary into an `injected` envelope Part and the payload Part (placement rule 1).
- `custom_message` entries -> `injected` Parts. Heartbeat prompts and any runtime-inserted scaffolding -> `injected`.
- `custom`/`label`/`session_info`/`thinking_level_change`/`model_change`/`compaction`/`branch_summary` and ANY unknown entry type -> placement rule 3 carriers (system-role Message, empty content, whole record in options) or session/message options where they are clearly metadata on a mapped object. Nothing in the entry stream is ever skipped.
- Secrets are redacted by OpenClaw BEFORE storage (`redactTranscriptMessageForStorage`), so pond stores what OpenClaw kept; restore round-trips redaction placeholders. This is fine and documented.

### 1.5 Session keys, lineage, retention (all changed July 2026 - do not trust older notes)

- `session_key` shapes: `agent:<agentId>:main`, `agent:<agentId>:<channel>:group:<id>`, `agent:<agentId>:subagent:<uuid>`, `agent:<agentId>:explicit:model-run-<uuid>` (gateway probes), `cron:<jobId>`, `hook:<uuid>`. The key encodes agent + surface + conversation and exists for every session.
- Automatic resets are OFF by default since #111140 (2026-07-18): one key usually maps to one long-lived sessionId; explicit `/new`/`/reset` still rotates.
- Reset RETAINS history since #111194 (2026-07-19): reset advances the key -> sessionId mapping but keeps prior generations' rows in SQLite. Explicit deletion writes a verified `.jsonl.deleted.<ts>[.zst]` archive then removes rows. Disk budget (default 10 GiB, physical bytes: db + WAL + session files) evicts oldest UNREFERENCED history: archive-verify-then-delete, and may later prune the archives themselves. Pond is the layer that survives this.
- PR #111861 (steipete, open as of 2026-07-21, expected to land) adds write-once lineage/provenance fields to the SessionEntry JSON (no DDL): `createdVia` (operator|spawn|channel|cron|talk|run|plugin|internal), `createdActor` ({type: human|agent|system, id?}), `createdAt`, `forkSource {sessionKey, sessionId, entryId?}` (exact fork cut-point), `previousSessionId` (chains same-key generation rotations). Map them when present; their absence (older rows, pre-merge builds) is fine - all optional. A follow-up may later promote these to `sessions` columns; keep lineage extraction in ONE function so that is a one-spot change.

## 2. Decisions (all confirmed 2026-07-21; do not re-litigate)

1. **Tree-to-linear = A3.** One pond session per OpenClaw session. ALL entries flattened in source append order (read via current `(created_at, seq)` snapshot ordering); canonical ordering is `(timestamp, id)` per spec 4.6. `parentId` preserved in each message's options. No session-per-branch, no active-branch-only. Rationale: ingest is a pure function of the append log - branch switching/rewinds never invalidate synced rows; search sees each branch once; native restore rebuilds the tree from options. Accepted costs: near-duplicate hits after rewinds; pond becomes a superset of the source after destructive rewrites (that is the product).
2. **`project` = `session_key` verbatim.** It is the conversation identity (OpenClaw itself keeps reset history "searchable under the same session key"). Substring filters give both scopes: prefix `agent:main:` = all of agent main's chats; full key = one chat across generation rotations. Total (every session has one, satisfying `model-project-non-empty`), immutable. Header `cwd` -> session options (project semantics are source-defined: directory for claude-code, chat for openclaw).
3. **Lineage taxonomy** (mirrors upstream's #111861 un-conflation; never collapse edge kinds):
   - fork (`forkSource`) -> `parent_session_id` + `parent_message_id` (= forkSource.entryId), options `relation: "fork"`.
   - subagent spawn (`spawned_by` / sessionKey shape) -> `parent_session_id`, options `relation: "spawn"`.
   - compaction successor (header `parentSession`) -> `parent_session_id` (+ cut message when recoverable), options `relation: "compaction_successor"`.
   - rotation (`previousSessionId`) -> session options ONLY, never `parent_session_id` (no history is copied; project already stitches the conversation).
   - `controlOwnerSessionKey`, `createdVia`, `createdActor` -> options verbatim.
4. **`source_agent` taxonomy:** `openclaw` for main/channel conversations; `openclaw/subagent` (incl. swarm children), `openclaw/cron`, `openclaw/hook`, `openclaw/probe` (model-run keys). Subpath kinds inherit pond's default search exclusion (spec 8.3) while staying fully stored and reachable.
5. **Ingest scope:** the session artifact entirely (1.2 "ingest these" + archives + legacy JSONL); derived projections are not data sources; foreign artifacts are documented non-ingest (1.2). Within `transcript_events` nothing is ever skipped - unknown entry types land via placement rule 3.
6. **Noise:** ingest everything by default (probes included - they are tiny). `[adapters.openclaw] skip_kinds = []` config knob (values: `subagent`, `cron`, `hook`, `probe`) as documented deliberate non-ingest for operators who want it. Default empty.
7. **Deletion policy:**
   - `.jsonl.deleted.*` archives are SKIPPED at ingest by default (`ingest_deleted = false` knob if someone wants them).
   - `reconcile_deletions = true` (default ON): at sync time, a deleted-reason archive whose session key has NO live `session_entries` row = explicit user deletion -> run the equivalent of `pond erase` for that session (cascades to children, denylists the key so resync cannot resurrect). Same archive with a live entry still present = budget eviction of an old generation -> PRESERVE. Ambiguity always resolves to preserve + a named line in the sync summary. VERIFIED ground for this rule: both deletion and eviction write `reason: "deleted"` archives (`session-accessor.sqlite-lifecycle.ts:284,341`, `session-history-eviction.ts:352`) - the filename cannot discriminate; but eviction structurally only targets sessions NOT referenced by a live entry/route/run (`readHistoricalSessionIds` + protected-set recheck inside the delete transaction), while explicit deletion removes the entry. Re-verify this at build time (1.5 moves fast).
   - Erase is NEVER exposed over MCP (hard pond constraint, spec 7.7).
8. **Discovery:** one `[adapters.openclaw]` entry, `path` = root (default `~/.openclaw`, honoring `OPENCLAW_STATE_DIR` in probe_default), auto-enumerating `agents/*/`. A dir without the expected DB is skipped with a note. No per-agent config in v1.
9. **Sync freshness:** interval sync (default 5 minutes via --with-sync); O6 live push stays deferred. Per-session staleness oracle: messages-based (per repo CLAUDE.md sync-oracle rule - never a versions() walk), plus the generation token: cache `(session_id -> generation, last_ingested_entry_count/max_created_at)` in the local freshness state; a changed generation with previously-synced content marks source-side rewrite -> pond keeps its superset (additive sync never deletes), logs the divergence in the sync summary.
10. **Plugin = tools only.** Projects `pond_search`, `pond_get`, `pond_sql_query` as native OpenClaw tools. NO memory slot, NO auto-recall, NO before_prompt_build, NO CLI namespace in v1.
11. **Plugin scoping (community-aligned; this exact model was pressure-tested against upstream precedent):** reuse `openclaw/plugin-sdk/session-visibility` (a published package export - see `extensions/clickclack/src/discussions/register.ts:3` for the import pattern) so the plugin honors the operator's existing `tools.sessions.visibility` (`self|tree|agent|all`, default `tree`) and `tools.agentToAgent` allowlist exactly as core `sessions_search` does (#105057: "Agents only ever see sessions they could already read via sessions_history"). Plugin-owned config adds only: `sources` (default `["openclaw"]`; `["*"]` opts into the cross-harness corpus - claude-code etc.; note foreign-harness content has no OpenClaw redaction pass), and a group-context clamp (group/channel sessions clamp to `tree` unless explicitly overridden - the #109009/#100140 private-vs-shared asymmetry). Enforcement: resolve the caller's visible scope via the SDK helpers, translate to pond filters (project = full session_key for self/tree + spawned keys; project prefix `agent:<id>:` for agent; source_agent for sources), clamp params before forwarding. Fail closed on any scope-resolution error; filter before limiting; copy #105057's bounds/redaction/typed-error patterns (`src/agents/tools/sessions-search-tool.ts`: limit default 10 max 25, query <=4096 chars, response <=32KB, snippet <=300 chars, output union with `{status: "forbidden"|"error"}`). This is policy against confused/prompt-injected agents, not a security boundary against the operator - document that plainly.
12. **Runtime:** plugin-managed `pond serve --transport stdio --with-sync` child process (no port, no token, no auth surface - keeps pond's no-auth non-goal intact), restarted on crash via `api.registerService`. Config `url` mode instead attaches to an external `pond serve` (streamable HTTP; auth is the integrator's shim, as in the existing NanoClaw setup). One MCP client implementation, two dial modes. Install story: `openclaw plugins install <pkg>` = capture + recall live with zero external machinery.

## 3. Deliverable 1: the `openclaw` adapter (build first)

Follow the adapter seam contract exactly (spec section 6; `src/adapter/mod.rs` is seam+registry ONLY - all OpenClaw specifics live in `src/adapter/openclaw.rs`). Precedents to read before writing code: `src/adapter/opencode.rs` (SQLite source: read-only open with `OpenFlags::SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_URI` + busy_timeout, connection cache, oracle interplay - `open_db` at ~line 798), `src/adapter/pi_coding_agent.rs` (FileEntry-family parsing, rule-3 carriers, parentId-in-options precedent), `src/adapter/claude_desktop_app.rs` (multi-root enumeration). `rusqlite` (0.40 bundled) and `zstd` (0.13) are already workspace deps.

### 3.1 Structure

- `OpenClawFactory` (`name = "openclaw"`): `probe_default` returns `{path: <root>}` when `$OPENCLAW_STATE_DIR` or `~/.openclaw` contains `agents/`; `open(config)` builds the adapter from `path` + policy knobs; `serialize` = the restore face (native restore target: the archive JSONL entry-line format - value-complete per spec 6.3; writing into a live agent DB is NOT the restore target in v1).
- `OpenClawAdapter::discover()` enumerates `agents/*/`, yielding per-agent sources: the agent DB (if present) and the `sessions/` artifact dir (archives + legacy).
- `events_with(oracle)`: for each agent, list sessions (`sessions` table + archive files), consult the skip oracle per session (see 3.4), and for stale sessions emit the canonical event stream in spec order (Session, then each Message followed by its Parts).

### 3.2 Per-session ingest (A3)

1. Read the session row + its `session_entries` JSON + conversation/route rows -> build the canonical Session: `id` = session_id; `project` = session_key; `source_agent` per kind taxonomy (decision 4; derive kind from session_key shape, plus `createdVia` when present); `created_at` = source timestamp; lineage per decision 3; everything else (scope, chat_type, channel, account_id, model ids, status, cwd from header, previousSessionId, createdVia/Actor, control/route metadata) -> session options under `options.openclaw.*`.
2. Read `transcript_events` for the session ordered by current snapshot `(created_at, seq)`; parse each `event_json` as a FileEntry; emit per 1.3/1.4. MessageID = entry `id` (source-supplied); PartID = entry id + ordinal; message timestamp = entry timestamp (ISO; the session header anchor is the permitted fallback). Store each entry's `parentId` in message options (A3 tree preservation); when the active-branch fact is not derivable from the entry stream, capture the active leaf pointer into session options (verification item V2).
3. Tool results: pi-ai `toolResult` messages become canonical ToolMessages with ToolResultPart; correlate via `toolCallId`.
4. Every value passes the seam extractors (no synthesis, bounded values); unknown entry types -> rule-3 carriers with the whole record in options. Nothing skipped, nothing invented.

### 3.3 Archives + legacy

Same entry parser over: zstd-decompressed `.jsonl.<reason>.<ts>.zst`, plain `.jsonl.<reason>.<ts>`, legacy plain `<id>.jsonl`. Reason `reset` archives are ordinary history (pre-#111194 installs). Reason `deleted` follows decision 7. An archived session that also exists in SQLite (post-#111194 retained generations can coexist with older artifacts): rows are identical entries under the same deterministic PKs - additive sync makes re-ingest a no-op, so ingest both sides without special-casing.

### 3.4 Staleness oracle + rewrite detection

Per session: compare stored freshness facts (entry count / max created_at, from the local freshness cache like other adapters) against the live table; unchanged -> skip. Additionally read `session_transcript_generations`: unchanged generation + unchanged counts = skip; changed generation = source rewrote history -> re-scan the session (new entry ids land additively; pond keeps superseded rows), and report `rewritten: N sessions` in the sync summary. Never use `seq` as a watermark.

### 3.5 Deletion reconciliation (decision 7)

At the end of each sync, per agent: list `.jsonl.deleted.*` artifacts; for each, resolve the session key (from the archived header/entries or the artifact's sessionId matched against ingested rows); if the key has no live `session_entries` row AND the session exists in pond -> erase + denylist (reuse the `pond erase` machinery; cascades to children). Otherwise preserve. Every action and every ambiguous preserve is named in the sync output (pond error-style: name the fix/fact, not just the symptom).

### 3.6 Config

```toml
[adapters.openclaw]
path = "~/.openclaw"          # root; agents auto-discovered
skip_kinds = []                # subset of ["subagent","cron","hook","probe"]
ingest_deleted = false         # ingest .deleted. archives
reconcile_deletions = true     # erase on unambiguous user deletion
```

`[adapters.<name>]` entries are free-form JSON blobs (`config.rs` ~line 308) - no config-system changes needed.

### 3.7 Tests

Unit tests in `#[cfg(test)] mod tests` at the bottom of `openclaw.rs`; cross-module suites under `tests/integration/adapter/openclaw.rs` with a `#[path]` line in `tests/integration/adapter/mod.rs` (repo placement rules are strict - see CLAUDE.md). Committed fixture corpus (sanitized, generated from a real minimal install): a fixture agent DB + artifacts covering (a) a branched session (rewind + re-fork - proves A3 + near-duplicate tolerance), (b) an inter-session provenance message (byte-exact envelope split), (c) a `custom_message` (injected), (d) an unknown entry type (rule-3 catch-all), (e) a zstd `.reset.` archive, (f) a `.deleted.` archive with no live entry (reconciliation fires) and one WITH a live entry (eviction lookalike - must preserve), (g) a subagent pair (spawn lineage), (h) a compaction entry + successor header. Round-trip conformance test per spec 6.8 (parse -> serialize native -> value-equal). Sync summary counters (unknown entry types, schema_version, rewrites) asserted.

## 4. Deliverable 2: `pond serve --with-sync`

- New flag on `Command::Serve` (`src/main.rs` ~line 596/1297; serve dispatch -> `transport::serve` / `serve_stdio` at `src/transport.rs:80/1118`). With the flag, spawn a tokio task looping: run the sync pipeline (`run_sync` internals, `src/main.rs` ~line 3181 - factor the reusable core out of the CLI verb as needed), sleep `interval` (config `[serve] sync_interval_minutes = 5` or a `--sync-every` flag; pick one, document it).
- Both transports support it (stdio is the plugin's managed mode; http serves the `url` mode).
- Errors in the sync loop are caught, logged, and never crash the serving half. The per-host sync flock (existing single-flight machinery) applies - a concurrently scheduled `pond schedule` run and the in-serve loop must not fight; in-serve sync passes the same `--no-wait` semantics (skip when the lock is held).
- Spec touch: this is the sanctioned consolidation - "no daemon beyond `pond serve`" stays literally true. It does NOT activate live-write (spec 9.4); only process topology changes. Update spec section 7.8's serve entry accordingly in the same PR.
- One embedding model instance serves both query-time and ingest-time embedding - this is the point of the fusion (previously: every 5-minute sync child cold-loaded ~500MB).

## 5. Deliverable 3: the OpenClaw plugin

TypeScript npm package (name suggestion: `@pond/openclaw` or `openclaw-pond`; decide at publish). Template for mechanics: `extensions/memory-lancedb/` (manifest + `definePluginEntry` + `registerTool` + `registerService`), but the plugin claims NO `kind: "memory"` slot.

### 5.1 Manifest (`openclaw.plugin.json`)

`id: "pond"`, `contracts.tools: ["pond_search", "pond_get", "pond_sql_query"]` (names must not collide with core tools - they don't), `configSchema` for the settings in 5.3, `uiHints` marking nothing sensitive (no keys in v1 managed mode).

### 5.2 Tools

Three `api.registerTool` registrations forwarding to pond over MCP. Follow #105057's contract patterns (`src/agents/tools/sessions-search-tool.ts`): TypeBox schemas with `additionalProperties: false`; named bound constants (limit default 10 / max 25, query <= 4096 chars, response <= 32KB, snippet <= 300 chars for search; equivalent budget caps for get/sql); output unions where forbidden/error is a typed result `{status: "forbidden"|"error", error}`; every text snippet through `redactToolPayloadText` (import from the same SDK surface clickclack uses, or reimplement the call pattern if not exported - verification item V5); hidden/out-of-scope hits dropped BEFORE limits apply. Hit shape mirrors `SessionsSearchHit` (sessionKey/timestamp/role/snippet/score/sessionId/messageId) so agents transfer their `sessions_search -> sessions_history` habits to `pond_search -> pond_get` unchanged. Subagent leaf sessions: respect the same deny posture as core (`SUBAGENT_TOOL_DENY_LEAF` includes sessions_search; the tool factory receives ctx and can return null - hide pond tools from leaf subagents identically).

### 5.3 Scoping (decision 11)

Per tool call: resolve the calling session key + agentId from tool ctx (verification item V3); compute visible scope via `openclaw/plugin-sdk/session-visibility` (`resolveEffectiveSessionToolsVisibility`, `createAgentToAgentPolicy`, `listSpawnedSessionKeys`); translate to pond filters; clamp/override any caller-supplied filter that exceeds scope. Config:

```json5
{
  // managed mode (default): plugin spawns and supervises the pond child
  pond: { mode: "managed", syncIntervalMinutes: 5 },
  // or: attach to an external pond serve; the operator owns auth (shim)
  // pond: { mode: "url", url: "https://host/mcp", headers: {...} },
  sources: ["openclaw"],        // ["*"] opts into cross-harness corpus
  groupSessions: "clamp",       // group/channel callers clamped to tree; "inherit" to disable
}
```

`tools.sessions.visibility` and `tools.agentToAgent` are read from the operator's existing OpenClaw config via the SDK - the plugin adds no parallel vocabulary for them.

### 5.4 Service (managed mode)

`api.registerService({id: "pond", start, stop})`: locate the pond binary (config `binaryPath` override; else PATH), spawn `pond serve --transport stdio --with-sync`, speak MCP over the child's stdio, restart with backoff on exit, kill on stop. First-run UX: if pond is not installed or `pond init` has never run, fail with a message naming the exact commands (pond's name-the-fix error convention). The plugin never writes pond config; `[adapters.openclaw]` enablement is `pond init`/`pond adapters` territory (sync never auto-enables adapters - pond contract).

### 5.5 Plugin tests

Vitest against a fake MCP endpoint (golden request/response fixtures for the three tools); scope-clamp unit tests per visibility level (self/tree/agent/all x agentToAgent allow/deny x group clamp); a schema-conformance check that tool schemas compile under OpenClaw's grammar constraints (see #108580 for why: exotic schema features break llama.cpp GBNF).

## 6. Sequencing

1. Adapter + fixtures + conformance (deliverable 1) - capture starts, MCP-attach (`openclaw mcp add pond`) already usable as an interim read surface with zero plugin code.
2. `pond serve --with-sync` (deliverable 2) - small, independent.
3. Plugin (deliverable 3).
4. Docs: adapter contract section (documented non-ingest list, deletion policy, scoping story), plugin README (privacy model stated plainly: what agents can see by default, what each widening step exposes, policy-not-security-boundary caveat).

## 7. Implementation-time verification checklist

Facts to re-verify against live code before/while building (the ground moves ~300 commits/day):

- V1. WAL read-only mechanics: confirm `SQLITE_OPEN_READ_ONLY` + busy_timeout suffices against a live gateway writer (WAL readers need `-shm` access; opencode.rs precedent works on quiescent DBs - test against a RUNNING gateway; fallbacks: retry, or snapshot-copy the db/-wal/-shm trio before reading).
- V2. Active-branch fact: is the active leaf recoverable from the entry stream / session_entries JSON, or only from `session_transcript_active_events`? If the latter, capture the active leaf pointer into session options at ingest (needed for value-complete native restore of "which branch is live").
- V3. Tool ctx: does an OpenClaw tool invocation see the calling session key (needed for `self`/`tree` clamping)? Tool factories receive ctx with agentId; check the per-call context shape. If absent, degrade: enforce `agent` scope, let callers narrow via explicit project filter.
- V4. #111861 merge state: if merged, map the new lineage fields (all additive JSON); if evolved, adjust decision-3 field names.
- V5. Redaction + hit-shape SDK exports: exact import paths for `redactToolPayloadText` (or equivalent) and `plugin-sdk/session-transcript-hit`.
- V6. Deletion-vs-eviction discriminator (decision 7): re-read `session-history-eviction.ts` + `session-accessor.sqlite-lifecycle.ts` on current main; also decide handling for whole-agent removal (entries purged - arguably deletion intent; current call: treat as ambiguous -> preserve + report).
- V7. Active-memory `toolsAllow`: whether plugin-registered pond tools are eligible for the recall sub-agent (nice-to-have interop, not v1 scope).
- V8. Legacy layout probing: confirm the pre-SQLite `sessions/` file shapes on an old install before writing the legacy branch (or gate it behind "artifacts present" detection).

## 8. References

- pond: `docs/spec.md` (the contract), `src/adapter/{mod,opencode,pi_coding_agent,claude_desktop_app}.rs` (seam + precedents), `src/main.rs` (serve/sync wiring), `docs/researches/2607-17-openclaw-integration-research.md` (background; decisions superseded by this plan).
- OpenClaw (`~/pjv/openclaw/openclaw`): `src/state/openclaw-agent-schema.sql`; `src/config/sessions/session-accessor.sqlite-transcript-store.ts`, `...-transcript-state.ts`, `...-lifecycle.ts`, `session-history-eviction.ts`; `src/sessions/input-provenance.ts`; `src/config/sessions/archive-compression.ts`, `artifacts.ts`; `src/plugin-sdk/session-visibility.ts`; `src/agents/tools/sessions-search-tool.ts`; `extensions/memory-lancedb/` (plugin mechanics template); `docs/plugins/building-plugins.md`.
- Upstream threads that shaped decisions: #105057 (sessions_search - the accepted scoping model), #111194 (reset retains history), #111140 (resets off by default), #110374 (delta reads + generation token), #111861 (canonical lineage, open), #100140 (rememberAcrossConversations - privacy posture), #109009/#65374 (private-vs-shared and cross-agent leak precedents), #108580 (tool-schema GBNF constraint).
