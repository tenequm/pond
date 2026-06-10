# Claude Desktop (macOS) - local session storage notes

Captured: 2026-05-13 (a fourth session, `local_5c09adfc`, was staged and
captured 2026-05-14 from a newer Claude Desktop version - see below).
Source machine had Claude Desktop installed at
`~/Library/Application Support/Claude/`.

## Two distinct conversation stores

Claude Desktop on macOS keeps conversations in two unrelated places:

### 1. claude.ai web conversations - Chromium IndexedDB (LevelDB)

Path: `~/Library/Application Support/Claude/IndexedDB/https_claude.ai_0.indexeddb.leveldb/`

Files observed:
- `000007.log` (~1.4 MB) - LevelDB write-ahead log
- `000009.ldb` (18 KB) - sorted SST table
- `CURRENT`, `LOCK`, `LOG`, `LOG.old`, `MANIFEST-000001`

A `strings` scan of `000007.log` reveals an object store named
`conversations_v2` with keys shaped like `conversations_v2:<unix-ms>`
(e.g. `conversations_v2:1776188481885`). Binary blobs (large message
payloads, attachments) are externalized to
`IndexedDB/https_claude.ai_0.indexeddb.blob/1/<00-ff>/...`.

This store is not human-readable without a LevelDB / IDB parser
(e.g. `node-indexeddb`, `leveldb` bindings, or Chromium's
`indexeddb_dump` utility). It is the storage backing the standard
web chat history that is synced with claude.ai.

### 2. Cowork / local-agent-mode sessions - plain JSON + JSONL

This is the rich, easily-parseable store. Two parallel directory trees:

```
~/Library/Application Support/Claude/
  claude-code-sessions/<account-uuid>/<workspace-uuid>/
    local_<session-uuid>.json          # lightweight session metadata
  local-agent-mode-sessions/<account-uuid>/<workspace-uuid>/
    spaces.json                        # workspace / "space" index (newer)
    local_<session-uuid>.json          # richer session metadata
    local_<session-uuid>/
      audit.jsonl                      # full transcript (one record per line)
      .audit-key                       # binary HMAC key for audit.jsonl (newer)
      .claude/                         # per-session claude-code state:
                                       #   .claude.json, projects/<enc>/<uuid>.jsonl,
                                       #   backups/, .last-cleanup
      uploads/                         # files the user attached (populated)
      outputs/                         # agent cwd anchor - stays empty
```

The `local-agent-mode-sessions` tree is keyed by a stable account UUID
and workspace UUID, then by a per-conversation `local_<uuid>` directory.
The `spaces.json` index, `.audit-key`, and the nested `.claude/`
Claude Code environment are present on newer Claude Desktop versions and
were not in the 2026-05-13 capture; `local_5c09adfc` (2026-05-14) has
them.

## Sample layout in this repo

The captured samples mirror the on-disk native layout:

```
claude-app/
  schema-notes.md                              # this file
  local-agent-mode-sessions/
    <account-uuid>/
      <workspace-uuid>/
        spaces.json                            # workspace index (5c09adfc only)
        local_<session-uuid>.json              # session metadata
        local_<session-uuid>/
          audit.jsonl                          # transcript
          .audit-key                           # binary HMAC key (5c09adfc only)
          .claude/                             # nested CC env  (5c09adfc only)
          uploads/                             # attached files (5c09adfc only)
```

Account UUID and workspace UUID directory names are opaque IDs and
kept as-is. Four sessions are bundled, all from the same workspace
(only one workspace was present on the source machine):

- `local_40d67183-...` - opus-4-6, Cowork sandbox, 73 audit lines.
  Multi-turn capability sanity-check, exercises `permission_request`
  / `permission_response` system events, third-party paid MCP tools,
  and Cowork permission flow.
- `local_1b98a569-...` - opus-4-5-20251101 (older snapshot, Claude
  Code 2.1.15). 20 audit lines. Lacks `_audit_hmac` and
  `fast_mode_state` fields; shows the earlier schema variant. Uses
  `Claude in Chrome` MCP server, Bash + Read tool calls into a
  sandboxed `mnt/` project workspace.
- `local_4f2429ff-...` - sonnet-4-6, 26 audit lines. Demonstrates
  `<uploaded_files>` markup inside `initialMessage`, repeated
  `system.subtype: api_retry` records with `error_status: 529`,
  and a final terminal `API Error: 529` assistant text. Useful for
  exercising retry / overload handling paths.
- `local_5c09adfc-...` - a deliberately benign staged session (generic
  CSV analysis, no personal content by construction), captured
  2026-05-14 from a newer Claude Desktop version. The only session
  with a populated `uploads/` sidecar, plus the newer `.audit-key`,
  nested `.claude/` Claude Code environment, and sibling `spaces.json`.
  The `.audit-key` binary is replaced with a zero-filled dummy of
  identical length.

Together they cover: two schema variants (with / without
`_audit_hmac`), four model strings, presence of file-upload markup,
presence of api retries, the Cowork permission-prompt flow, a
populated `uploads/` sidecar, and the newer `.audit-key` / `.claude/`
/ `spaces.json` structure.

## `local_<uuid>.json` (session metadata) - top-level fields

Captured sample: `local_40d67183-.../local_40d67183-...json`
(~260 KB after anonymization).

```
sessionId            string  "local_<uuid>"
processName          string  human-friendly slug (e.g. "confident-awesome-gauss")
cliSessionId         string  uuid linking to the inner claude-code session
cwd                  string  virtual sandbox path, e.g. "/sessions/<slug>"
userSelectedFolders  string[]
createdAt            number  unix ms
lastActivityAt       number  unix ms
model                string  e.g. "claude-opus-4-5-20251101"
isArchived           bool
title                string  auto-generated conversation title
vmProcessName        string
initialMessage       string  first user prompt
slashCommands        string[]
enabledMcpTools      object  map of "local:<server>:<tool>" -> config
systemPrompt         string  the full Cowork system prompt
accountName          string
emailAddress         string
```

Notably, the metadata file embeds the entire Cowork system prompt and
the list of enabled MCP tools per session.

## `audit.jsonl` - the transcript

Captured sample: `local_40d67183-.../local_40d67183-.../audit.jsonl`
(63 KB, 73 records, multi-turn).

One JSON object per line. Every record carries:

```
type                 string  "user" | "assistant" | "system" | "tool_result" | "result" | ...
uuid                 string  per-record id
session_id           string  the inner CLI session id (matches cliSessionId above)
parent_tool_use_id   string|null  links tool_result records back to a tool_use
_audit_timestamp     string  ISO-8601 UTC
```

Variants observed:

- `type: "user"` - `message: { role, content }`, where `content` is a string OR
  an array of typed blocks (`{ type: "text" | "tool_result", ... }`).
- `type: "assistant"` - `message: { id: "msg_...", model, role, type: "message",
  content: [ { type: "text"|"tool_use", ... } ], stop_reason, usage }`.
  This mirrors the Anthropic Messages API response shape verbatim,
  including `usage.input_tokens`, `cache_creation_input_tokens`,
  `cache_read_input_tokens`, `output_tokens`.
- `type: "system", subtype: "init"` - a one-time bootstrap record listing
  `tools`, `mcp_servers` (with connection status), `model`,
  `permissionMode`, `slash_commands`, `agents`, `skills`, `plugins`,
  `claude_code_version`, `apiKeySource`, `output_style`.
- `type: "result"` - end-of-turn summary with `duration_ms`, `num_turns`,
  token totals, and an optional final `result` text.

## Structural quirks

- Two parallel session uuids: `sessionId` (the "local_..." app session)
  vs `cliSessionId` / `session_id` in audit records (the inner
  claude-code agent loop). They are linked via the metadata file.
- The audit log mixes API-shaped assistant messages (Anthropic Messages
  format with `content` blocks) with simpler `{role, content: string}`
  user messages.
- Tool calls and their results are linked by `parent_tool_use_id` rather
  than nested - reconstructing a turn requires joining by uuid.
- The init record is effectively a snapshot of the agent's full
  capability surface at session start (tools, MCP servers, skills,
  plugins, agents) - useful for replay / reproducibility.
- User-attached files live out-of-band under `local_<uuid>/uploads/`,
  not inline in the JSONL. The sibling `outputs/` dir exists but is not
  a deliverable sink - the agent writes deliverables to the user's
  selected workspace folder; `outputs/` is only the agent's cwd anchor
  and stays empty.
- Newer Claude Desktop versions add, under `local_<uuid>/`: `.audit-key`
  (binary HMAC key for `audit.jsonl`) and a nested `.claude/` Claude
  Code environment (`.claude.json`, `projects/<encoded-path>/<uuid>.jsonl`,
  `backups/`, `.last-cleanup`). A `spaces.json` index sits beside the
  session files.
- `accountName` and `emailAddress` are stored in plaintext in the
  session metadata.
- The claude.ai web history (IndexedDB) and the Cowork session history
  (JSONL) are completely independent stores - editing one does not
  affect the other.

## Anonymization applied to the samples here

- Local username -> `user`
- Real first name -> `User`
- Email addresses -> `user@example.com`
- `/Users/<user>/...` -> `/Users/user/...`
- Real project folder names -> `myproject-a` (etc.)
- Third-party social handles -> `@someone`
- Internal IPs / hostnames -> `internal.example.com`
- `sk-ant-*`, `Bearer ...`, GitHub tokens -> `REDACTED`
- Long pieces of personal content inside tool results were replaced
  with `<snipped: ~N lines of <kind>>` placeholders.
- All opaque ids (account UUID, workspace UUID, session UUIDs),
  timestamps, model names, schema field names, MCP tool listings,
  and event shape preserved.
