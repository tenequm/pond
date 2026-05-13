# Session samples

Curated, anonymized session captures from 8 agentic-client platforms. These files
ground pond's canonical-type design (see `docs/design.md` section 3.1) and will
serve as test fixtures for the v1 SourceAdapter implementations (see
`docs/design.md` section 3.4).

Snapshot date: 2026-05-13.

## Why

pond ingests sessions from many runtimes. Each runtime writes its own on-disk
format. Designing canonical types without ground truth produces hallucinated
schemas. This directory is that ground truth: one or more real sessions per
platform, captured from local storage or platform exports, anonymized.

Constraints baked in:

- **Native on-disk layout preserved.** A SourceAdapter that walks a real user
  install (`~/.claude/projects/...`, `~/.codex/sessions/...`, etc.) sees the
  same directory shape, the same filename conventions, the same sidecar files
  when pointed at these samples. Discovery code is testable end to end.
- **Schema-critical fields preserved verbatim.** All event IDs, message IDs,
  session UUIDs, tool-call IDs, timestamps, schema discriminators, role
  enums, token and cost counters, model names, MIME types, HMACs (where
  present), and field names are unchanged from the source data.
- **Privacy-sensitive content scrubbed.** Personal names (English and
  non-English forms), real project / product / company names, Slack / Discord
  / Telegram IDs, internal service hostnames, real wallet addresses,
  balances and transaction listings, bug-report substance describing real
  production incidents, real IP addresses, credentials, and email addresses
  are replaced with consistent placeholders. Long blocks of personal or
  proprietary text content are replaced with `<redacted: short description>`
  markers preserving the JSON envelope.

## Layout

```
session-samples/
  README.md                  this file
  claude-app/                Claude Desktop (macOS), Cowork / local-agent-mode
  claude-code/               Claude Code CLI
  claude-managed-agents/     Anthropic API Managed Agents (playground export)
  codex/                     OpenAI Codex CLI
  nanoclaw/                  nanoclaw runtime (Claude Code Agent SDK in containers)
  openclaw/                  openclaw runtime
  opencode/                  opencode CLI
  pi/                        pi-mono CLI
```

Each platform subdir mirrors that platform's native on-disk path layout so a
SourceAdapter can be tested by pointing its discovery code directly at the
sample tree.

## Per-platform notes

### claude-app (Claude Desktop, Cowork)

- Source path: `~/Library/Application Support/Claude/local-agent-mode-sessions/<account-uuid>/<workspace-uuid>/`
- Layout: pair per session. `local_<session-uuid>.json` is the metadata file
  (embeds the full Cowork system prompt and MCP tool config). Sibling
  `local_<session-uuid>/audit.jsonl` is the full transcript, one record per
  line, mirroring the Anthropic Messages API content-block shape. An
  `_audit_hmac` field appears on records (cryptographically invalid against
  the anonymized content but the field is retained for schema fidelity).
- Samples: 3 sessions (opus-4-6, opus-4-5 older format, sonnet-4-6 with an
  `api_retry` 529-overload storm). Same workspace UUID for all because only
  one workspace existed on the source machine.
- The web chat history at
  `~/Library/Application Support/Claude/IndexedDB/https_claude.ai_0.indexeddb.leveldb/`
  is described in `claude-app/schema-notes.md` but not captured (binary
  LevelDB, separate extraction work).

### claude-code (Claude Code CLI)

- Source path: `~/.claude/projects/<encoded-project-path>/<session-uuid>.jsonl`
- Layout: one JSONL per session, one directory per encoded project path.
  Encoded path mirrors the project cwd with `/` replaced by `-`. Lines are
  typed entries with `parentUuid` -> `uuid` chains. Tool results arrive as
  `user` entries whose `message.content[]` contains `tool_result` blocks,
  plus a parallel `toolUseResult` field carrying richer structured data
  (`structuredPatch` for Edits, file contents for Reads, etc.).
- Samples: 3 sessions across 3 projects, 3 CLI versions. Version 2.1.68 is
  the older format (no SessionStart attachment, no `last-prompt` / `permission-mode`
  standalone events, no per-row `entrypoint` / `gitBranch`). Versions 2.1.104
  and 2.1.132 are the modern hook + attachment flow with
  `deferred_tools_delta`, `mcp_instructions_delta`, `skill_listing`,
  `stop_hook_summary`, etc.

### claude-managed-agents (Anthropic API Managed Agents)

- Source: API playground export. No local on-disk format.
- Layout: single JSON file, flat array of events with `type` discriminator.
  Event types observed: `session.status_running`, `user.message`,
  `agent.message`, `agent.thinking`, `agent.tool_use`, `agent.tool_result`,
  `span.model_request_start`, `span.model_request_end`, `session.status_idle`.
  ID back-references express relationships (`tool_use_id` -> `agent.tool_use.id`,
  `model_request_start_id` -> `span.model_request_start.id`); there is no
  nesting.
- Samples: 1 (the included session is a comparison of two public GitHub
  repositories via the `web_fetch` tool; preserved verbatim from the
  exporter since it contains no sensitive content).
- See `claude-managed-agents/schema-notes.md` for a detailed event-stream
  walkthrough.

### codex (OpenAI Codex CLI)

- Source path: `~/.codex/sessions/<year>/<month>/<day>/rollout-<ts>-<uuid>.jsonl`
- Layout: date-partitioned. Each line is an envelope `{timestamp, type, payload}`.
  Top-level `type` values: `session_meta` (initial config; cwd, originator,
  cli_version, model_provider, base_instructions, git info), `event_msg`
  (lifecycle events: `task_started`, `token_count`, `user_message`,
  `agent_message`, `agent_reasoning`), `response_item` (model interaction
  items: `message`, `reasoning`, `function_call`, `function_call_output`,
  `custom_tool_call`), `turn_context` (per-turn config: model, sandbox /
  approval / truncation / reasoning policies). Reasoning items carry
  `encrypted_content` Fernet-encrypted opaque payloads (pond cannot decrypt
  them; preserved for schema fidelity). MCP tools appear as flattened names
  like `surf__surf_amazon_search`.
- Samples: 3 sessions across 3 dates, 2 originators (`codex_cli_rs`
  interactive vs `codex_exec` headless), models `gpt-5` and `gpt-5-codex`.

### nanoclaw

- Source path:
  `~/pj/nanoclaw/data/v2-sessions/<agentGroupId>/.claude-shared/projects/-workspace-agent/<sessionUUID>.jsonl`
- Layout: Claude-Code-style JSONL with nanoclaw `queue-operation` records
  interleaved (no uuid / parentUuid - they are tracking events for nanoclaw's
  own queue). Sidecar directories per session: `<sessionUUID>/subagents/`
  contains `agent-<id>.jsonl` per subagent transcript plus a minimal
  `agent-<id>.meta.json` carrying `{agentType, description}`;
  `<sessionUUID>/tool-results/` contains `*.txt` files for spilled-to-disk
  tool outputs (filename conventions: opaque IDs like `bn4hhsiry.txt` or
  MCP-prefixed like `mcp-surf-surf_github_get-<unix-ms>.txt`).
- Samples: 1 top-level session, 1 subagent (from a different parent session
  so the subagent sidecar layout can be demonstrated alongside the
  top-level shape), 1 `.meta.json` example, 2 `tool-results/*.txt` examples.

### openclaw

- Source path: `~/.openclaw/agents/<agent>/sessions/<uuid>.jsonl` plus
  `~/.openclaw/sessions.json` (index).
- Layout: one JSONL per session per agent. Lines are typed events forming a
  parent-linked tree via `id` / `parentId` (not a flat list). Event types:
  `session`, `model_change`, `thinking_level_change`, `custom`, `message`.
  Tool calls live inline in assistant message content as `{type:"toolCall"}`;
  results follow as separate top-level `message` entries with
  `role:"toolResult"`. Sessions rotate on reset by appending
  `.reset.<ISO-timestamp>` to the original filename (frozen snapshot).
- Samples: 3 sessions across 3 delivery channels (telegram, subagent,
  heartbeat) plus the `sessions.json` index. The index entries' `sessionFile`
  paths are updated to resolve against the local sample layout.

### opencode

- Source path: `~/.local/share/opencode/storage/{session,message,part}/...`
- Layout: fan-out tree, one file per object.
  - `session/<projectID>/<sessionID>.json` - session metadata
    (`id`, `version`, `projectID`, `directory`, `title`, `time`)
  - `message/<sessionID>/<messageID>.json` - one file per message (user
    messages are tiny stubs; assistant messages carry `system` prompt array,
    `modelID`, `providerID`, `mode`, `path`, `cost`, `tokens` including
    cache read/write)
  - `part/<messageID>/<partID>.json` - one file per part. Types: `text`,
    `reasoning`, `tool` (state-union with status `pending` / `running` /
    `completed` / `error`), `step-start`, `step-finish`, `file` (with `mime`,
    `filename`, `url`, optional `source.text` span), `patch`. Tool calls
    use `callID` (e.g. Anthropic `toolu_*`).
  - ULID-style IDs (`ses_*`, `msg_*`, `prt_*`).
- Samples: 3 sessions, 43 message files, 137 part files. Single projectID
  because only one existed on the source machine; session-internal
  placeholders distinguish `myproject-a` from `myproject-b`.

### pi (pi-mono CLI)

- Source path: `~/.pi/agent/sessions/<encoded-cwd>/<timestamp>_<ulid>.jsonl`
- Layout: encoded-cwd as dir name (one per project), `<timestamp>_<ulid>.jsonl`
  filename. Newline-delimited JSON with a version-3 envelope. First line is
  `{type:"session", id, timestamp, cwd}`. Subsequent lines are events with
  `id` + `parentId` forming a DAG / tree rather than a flat sequence. Event
  types: `session`, `model_change`, `thinking_level_change`, `message`.
  Assistant messages carry rich provenance: `usage` (input / output /
  cacheRead / cacheWrite / totalTokens), `stopReason`, `api`, `provider`,
  `model`, `responseId`, full `cost` breakdown.
- Samples: 3 sessions across 3 anonymized projects.

## Cross-platform schema variation

Where formats fundamentally disagree (informs canonical type design in
`docs/design.md` section 3.1):

| Concern | Variants observed |
|---|---|
| Top-level file shape | JSONL stream (claude-code, codex, pi, nanoclaw, openclaw, claude-app audit) vs JSON array (claude-managed-agents) vs fan-out tree (opencode) vs metadata + audit pair (claude-app) |
| Message-to-event granularity | Coalesced messages (claude-code, opencode, openclaw, claude-app audit) vs per-event stream where one assistant turn produces many events (claude-managed-agents, codex with separate `response_item`s) |
| Tool call / result linking | Same-line content blocks (claude-code, claude-app, claude-managed-agents) vs separate top-level events (pi, codex) vs side-table parts (opencode) vs inline content with separate `role:"toolResult"` (openclaw) |
| Inter-message linking | parentUuid chain (claude-code) vs parentId tree (pi, openclaw) vs flat sequence with span IDs (claude-managed-agents) vs file order only (codex, opencode, claude-app audit) |
| Sidecar files | `tool-results/`, `subagents/` (nanoclaw) vs per-message part dirs (opencode) vs `uploads/` + `outputs/` (claude-app) vs none (most others) |
| Provider / model recording | Per-assistant-message (most) vs per-span via `span.model_request_*` events (claude-managed-agents) vs per-line `turn_context` (codex) |
| Encrypted opaque payloads | Codex `encrypted_content` Fernet blobs vs none |
| HMAC over content | claude-app `_audit_hmac` vs none |
| Streaming on disk | None of the captured samples persists streaming deltas; all are coalesced to final-state |

## Anonymization rules applied

Applied consistently across all samples. Captured here so the same rules can
be applied to refreshed samples.

### Replaced

- Local username -> `user`
- Real first names (English and Cyrillic forms) -> `User` or role placeholders
  (`FriendOne`, `AgentName`, `OwnerName`, etc.)
- Real email addresses -> `user@example.com` or `someone@example.com`
- `/Users/<name>/` and `/home/<name>/` paths -> `/Users/user/...`
- API keys, bearer tokens, JWTs -> `REDACTED`
- Real project / product / repo names -> `myproject-a`, `myproject-b`, ...
  (consistent across files in the same scope)
- Real third-party social handles -> `someone`
- Internal service hostnames -> `companyone.example.com`,
  `companytwo.example.com`, ... (consistent within a file)
- Real IPs -> RFC 5737 documentation ranges (`192.0.2.x`, `198.51.100.x`,
  `203.0.113.x`)
- Real wallet addresses -> well-known public placeholders (USDC mint, system
  program) or `0x000...0002`
- Slack-format user / channel IDs -> `U00000000000` / `C00000000000`
- Discord snowflakes in Discord context -> `000000000000000000`
- Telegram chat IDs -> `00000000`
- Real product / payment-service names embedded in MCP tool prefixes
  (`surf`, `cascade`, `tempo`, `payai`, etc.) -> `paymentservice-a`,
  `paymentservice-b`, ...
- Operational content blocks (wallet balances and tx listings, bug-report
  substance, persona / memory files) -> `<redacted: short description>`
  preserving the JSON envelope
- Long product-internal system prompts beyond standard runtime boilerplate
  -> `<redacted: ~Nk-char product system prompt>` (Cowork system prompts
  are preserved verbatim because they are identical across all Claude
  Desktop users; see `claude-app/schema-notes.md`)

### Preserved verbatim

- All event / message / part / session / account / workspace UUIDs and
  ULIDs and opaque IDs (`sevt_*`, `sesn_*`, `ses_*`, `msg_*`, `prt_*`,
  `toolu_*`, etc.)
- All timestamps (ISO 8601, unix milliseconds)
- All JSON schema field names and structure
- Type discriminator values (role enums, type / subtype values, customType
  values)
- Token / cost / usage counters (`input_tokens`, `output_tokens`,
  `cache_creation_input_tokens`, `cache_read_input_tokens`, cost breakdowns)
- Model names (`claude-opus-4-7`, `gpt-5`, `gpt-5-codex`, etc.)
- Provider / api names (`anthropic`, `openai`, `ollama`, `zai-coding-plan`,
  etc.)
- MIME types
- Generic tool names (`Bash`, `Read`, `Edit`, `webfetch`, etc.) and public
  MCP tool names (`Apify`, `Filesystem`, `time`, etc.)
- `processName` slugs (humanized random tokens like `confident-awesome-gauss`)
- Anthropic and OpenAI API field names
- Codex `encrypted_content` Fernet payloads (opaque; pond cannot decrypt;
  preserved for schema fidelity)
- claude-app `_audit_hmac` field values (cryptographically invalid against
  modified content but the field is kept so SourceAdapters see it)
- Cowork system prompts (identical across all Claude Desktop users;
  preserved for schema fidelity)
- Public OSS-product hostnames in scraped tool-result content where they
  are the natural output of a public web crawl

## How to refresh

To replace a sample with a fresh capture:

1. Locate the source on the host running that runtime (paths above).
2. Pick a session that demonstrates the schema variation worth showing
   (multi-turn, tool calls, version skew, etc.).
3. Apply the rules in "Anonymization rules applied" above. Verify with the
   pre-commit checks below.
4. Place the sample under the matching native path inside the platform's
   subdir.

## Verification

Pre-commit checks run against this directory:

- `trufflehog filesystem <dir> --no-verification` - expect 0 verified or
  unverified secrets.
- `gitleaks detect --no-git --source <dir>` - any findings reviewed; current
  findings are Apify MCP tool registry hex suffixes (content-addressed tool
  IDs), false positives.
- Targeted regex sweeps for project-specific personal identifiers.
- JSON / JSONL parse validation on every file.
