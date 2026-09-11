# Session samples

Curated, anonymized (or, where a real capture is infeasible, fully synthetic)
session samples from 13 agentic-client platforms. These files
ground pond's canonical-type design (see `docs/spec.md`) and
serve as the test fixtures for the v1 adapter implementations (see
`docs/spec.md#adapters`).

Snapshot date: 2026-05-13 (claude_code subagent sample added 2026-05-20;
claude_code nested workflow-subagent sample added 2026-06-04; opencode
`opencode.db` SQLite fixture generated 2026-07-14 from opencode 1.17.15;
synthetic hermes `state.db` fixtures generated 2026-07-23; letta-code
transcripts captured 2026-08-24 from letta-code 0.30.30; grok-build sessions
captured 2026-08-24 from grok-build 1.0.5; agy conversations captured
2026-09-10 from agy 1.2.0 and its ACP server).

## Why

pond ingests sessions from many runtimes. Each runtime writes its own on-disk
format. Designing canonical types without ground truth produces hallucinated
schemas. This directory is that ground truth: one or more real sessions per
platform, captured from local storage or platform exports, anonymized.

Constraints baked in:

- **Native on-disk layout preserved.** An adapter that walks a real user
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
adapter/
  README.md                  this file
  agy/                       Antigravity CLI (`agy`) and its ACP server, a Gemini home (`~/.gemini`)
  claude_ai_export/          claude.ai data export (synthetic conversations.json)
  claude_code/               Claude Code CLI
  claude_desktop_app/        Claude Desktop (macOS), Cowork / local-agent-mode
  claude_managed_agents/     Anthropic API Managed Agents (playground export)
  codex_cli/                 OpenAI Codex CLI
  grok-build/                grok-build (xAI `grok` CLI) session directories
  hermes/                    Hermes Agent runtime (single SQLite state.db per profile)
  letta-code/                letta-code (`letta` CLI) client-side transcripts
  nanoclaw/                  nanoclaw runtime (Claude Code Agent SDK in containers)
  oh-my-pi/                  oh-my-pi (`omp`), a pi fork with its own sessions root
  openclaw/                  openclaw runtime
  openclaw-captures/         openclaw state roots captured from the real runtime
  opencode/                  opencode CLI
  pi-coding-agent/           pi-coding-agent CLI
```

Each platform subdir mirrors that platform's native on-disk path layout so a
adapter can be tested by pointing its discovery code directly at the
sample tree.

## Per-platform notes

### agy (Antigravity CLI and its ACP server)

- Source path: the Gemini home, `~/.gemini/` (`%USERPROFILE%\.gemini` on
  Windows; `$GEMINI_HOME` relocates it - the ACP server's startup log says so
  itself: `Gemini home resolved to ... (default; $GEMINI_HOME is unset)`). Two
  writers share it, each with its own subtree: `antigravity-cli/` (the `agy`
  binary, TUI and `-p` headless) and `antigravity-acp/` (Google's ACP server,
  `agy_acp_server.par`). Both keep one SQLite database per conversation at
  `<lane>/conversations/<uuid>.db`: WAL mode, `PRAGMA user_version = 1`, tables
  `steps`, `trajectory_meta`, `trajectory_metadata_blob`, `parent_references`,
  `battle_mode_infos`, `gen_metadata`, `executor_metadata`, every payload a
  protobuf blob (`steps.step_payload` is a `gemini_coder.Step`). Sidecars: the
  CLI lane writes `brain/<uuid>/.system_generated/` (the `logs/transcript*.jsonl`
  display mirror, `steps/<n>/output.txt` tool spill, `tasks/*.log`,
  `messages/*.json`, `subagents/<child>.json`), a lagging
  `conversation_summaries.db` index and the TUI's `history.jsonl` prompt recall;
  the ACP lane writes `conversations/<uuid>.meta` (`{"cwd": ...}`) and no brain.
- Samples: captured 2026-09-10 by sandbox self-capture on Linux (NixOS) - agy
  1.2.0 and the ACP server build `release blaze-2026.08.18-1`, under a
  throwaway `HOME` at the neutral base path `/tmp/agy-fixture/home`, project cwd
  `/tmp/agy-fixture/project` (a `README.md` and a `calc.py`, not a git repo).
  Auth: two fresh Google OAuth sign-ins performed inside the sandbox (the CLI's
  pasted authorization code, the ACP server's loopback callback), with the
  onboarding data-sharing opt-in unticked; both tokens
  (`antigravity-cli/antigravity-oauth-token`, `antigravity-acp/acp_token.json`)
  were deleted before anything was copied out. Models: agy's default Gemini 3.8
  Flash (High), `claude-sonnet-4-6` for the reasoning session, the ACP server's
  default. Drivers: headless `agy -p ... --output-format json` (resumed with
  `--conversation`), the TUI under tmux for `/rewind` and `/fork`, SIGTERM
  mid-command for the interrupts; the ACP tool session through `acpx exec`, the
  multi-turn ACP session through a minimal JSON-RPC client (`initialize`,
  `session/new`, `session/prompt`, then `session/load` from a fresh server
  process) because acpx 0.15.1's named sessions resolved to its codex agent
  under `--agent`. Each WAL was folded into its database with `PRAGMA
  wal_checkpoint(TRUNCATE)`; the files stay in WAL mode (the production shape,
  so a read-only open materializes gitignored `-wal`/`-shm`). No other edit:
  nothing needed anonymizing.
- Census (13 databases, 62 `steps` rows; step type / status):
  - `antigravity-cli/conversations/`:
    - `109948f1` no-workspace: a headless one-shot without `--add-dir`, so no
      workspace appears anywhere in the database (only
      `project_id: "default-cli-project"`). 2 steps: 1 USER_INPUT, 1
      PLANNER_RESPONSE.
    - `bb24ae0d` text + resume: two headless turns, the second through
      `--conversation`; the resume injects a SYSTEM_MESSAGE (the "subagents and
      background tasks have been stopped due to server restart" notice). 5
      steps: 2 USER_INPUT, 2 PLANNER_RESPONSE, 1 SYSTEM_MESSAGE.
    - `3f72cf51` tools: four tool calls as GENERIC steps - `ls`, a view of a
      missing file (status ERROR, `error_details` populated), `false` (exit 1,
      status DONE), a view of `README.md`. 10 steps: 1 USER_INPUT, 5
      PLANNER_RESPONSE, 3 GENERIC/DONE, 1 GENERIC/ERROR.
    - `3570c3c6` reasoning: `claude-sonnet-4-6`; the planner response carries
      `thinking` text and a thinking signature. 2 steps.
    - `121e0cfc` interrupted, never resumed: SIGTERM 20 s into `sleep 45`; the
      command step is left RUNNING (with `task_details`, a background task). 4
      steps: 1 USER_INPUT, 2 PLANNER_RESPONSE, 1 GENERIC/RUNNING.
    - `5d458282` interrupted, then resumed: the same interrupt, then a headless
      resume, which rewrote the RUNNING step to CANCELED in place and injected
      the restart SYSTEM_MESSAGE. 7 steps: 2 USER_INPUT, 3 PLANNER_RESPONSE, 1
      SYSTEM_MESSAGE, 1 GENERIC/CANCELED.
    - `6479151c` subagent parent: the subagent is spawned through a GENERIC
      tool call (`Subagents` argument) and reports back as a SYSTEM_MESSAGE
      whose sender is the child; `brain/.../subagents/11b9a01c-....json` is the
      parent-side record. 6 steps: 1 USER_INPUT, 3 PLANNER_RESPONSE, 1
      GENERIC/DONE, 1 SYSTEM_MESSAGE.
    - `11b9a01c` subagent child: its own `trajectory_metadata_blob` names
      `parent_conversation_id: 6479151c-...`, `nesting_depth: 1` and the
      `research` agent script. 6 steps: 1 USER_INPUT, 3 PLANNER_RESPONSE, 2
      GENERIC/DONE.
    - `07439cf3` TUI, rewound: two turns, `/rewind` over the second (its two
      steps deleted, `trajectory_meta` rotated to a new `trajectory_id`, one
      `parent_references` FORK row pointing at the conversation's own previous
      trajectory), then a new turn that reuses step indexes 2-3. 4 steps: 2
      USER_INPUT, 2 PLANNER_RESPONSE.
    - `3abd71a7` `/fork` of `07439cf3`, plus one turn of its own: the parent's
      steps are copied verbatim (still carrying the parent's trajectory ids),
      and two `parent_references` FORK rows name the parent - the inherited
      rewind and the fork cut at the parent's step 3. 6 steps: 3 USER_INPUT, 3
      PLANNER_RESPONSE.
  - `antigravity-acp/conversations/` (every ACP database writes an empty
    `trajectory_metadata_blob` and a `trajectory_meta` row whose
    `trajectory_id` equals the conversation id; the cwd lives only in the
    `.meta` sidecar):
    - `7337857a` empty: created by the sign-in handshake (`session/new`, never
      prompted) - every table present, zero steps.
    - `85732767` two turns + reload: two prompts, then `session/load` from a new
      server process and a third prompt. 6 steps: 3 USER_INPUT, 3
      PLANNER_RESPONSE.
    - `884ff681` tool: `acpx exec`; the client-side `client_view_file` tool is an
      AGENCY_TOOL_CALL step. 4 steps: 1 USER_INPUT, 2 PLANNER_RESPONSE, 1
      AGENCY_TOOL_CALL.
  - Totals: 19 USER_INPUT, 30 PLANNER_RESPONSE, 9 GENERIC (6 DONE, 1 ERROR, 1
    RUNNING, 1 CANCELED), 3 SYSTEM_MESSAGE, 1 AGENCY_TOOL_CALL = 62.
- Left out on purpose: per-install state (`bin/`, `builtin/`, `cache/`,
  `log/`, `crashes/`, `installation_id`, `jetski_state.pbtxt`, `implicit/*.pb`,
  `annotations/*.pbtxt`, `presence/`, `knowledge/`, `scratch/`, both lanes'
  `settings.json`), the opaque (encrypted) `<uuid>.pb` that `/fork` writes
  beside the fork's `.db`, and a second empty ACP conversation that
  `acpx sessions ensure` created (the same shape as `7337857a`).
  `agy --model not-a-real-model` is rejected before any conversation is
  written, so there is no failed-start database to include.
- Sweeps: trufflehog 0 over the tree (databases included); gitleaks 0 over the
  text files and over `strings` of every database (gitleaks skips files it
  sniffs as `application/vnd.sqlite3`, the extracted text had the SQLite magic
  line removed); every JSON/JSONL/`.meta` file parses and every database passes
  `PRAGMA integrity_check`; no username, email, hostname or `/home/<name>` path.

### claude_ai_export (claude.ai data export)

- Source: the official claude.ai data-export `.zip` (emailed download link), whose `conversations.json` entry is one JSON array of conversation objects - many sessions per file, no per-session files. Not auto-discoverable; the adapter is pointed at the `.zip`, an extracted directory, or the bare `conversations.json`.
- Layout: `conversations.json` only. Each conversation carries `uuid`, `name`, `summary`, `created_at`, `updated_at`, `account.uuid`, and `chat_messages[]`; each message carries `uuid`, `sender` (`human` / `assistant`), `created_at`, and `content[]` blocks of `text`, `thinking`, `tool_use`, and `tool_result` (the export's `tool_result` has a tool `name` but no `tool_use_id`).
- Samples: SYNTHETIC, hand-written to the export's shape rather than captured - 5 conversations under one account uuid: plain text, a `thinking` block, a `tool_use` + `tool_result` pair (the human turn of pure `tool_result` becomes a Tool message), an empty-`name` conversation, and a 0-message conversation (skipped as Empty; 4 sessions ingest). Schema-critical field names are the export's own; ids are the obvious `1111...`/`ffff...` placeholders.

### claude_desktop_app (Claude Desktop, Cowork)

- Source path: `~/Library/Application Support/Claude/local-agent-mode-sessions/<account-uuid>/<workspace-uuid>/`
- Layout: pair per session. `local_<session-uuid>.json` is the metadata file
  (embeds the full Cowork system prompt and MCP tool config). Sibling
  `local_<session-uuid>/audit.jsonl` is the full transcript, one record per
  line, mirroring the Anthropic Messages API content-block shape. An
  `_audit_hmac` field appears on records (cryptographically invalid against
  the anonymized content but the field is retained for schema fidelity).
  Newer Claude Desktop versions also write, under `local_<session-uuid>/`:
  `uploads/` (files the user attached - populated), `.audit-key` (binary
  HMAC key; replaced with a zero-filled dummy of identical length in the
  sample), and a nested `.claude/` Claude Code environment (`.claude.json`,
  `projects/<encoded-path>/<uuid>.jsonl`, `backups/`). A `spaces.json` index
  sits beside the session files (the workspace / "space" concept). The
  `outputs/` sidecar dir exists but stays empty - the agent writes
  deliverables to the user's selected workspace folder, not into `outputs/`
  (it is only the agent's cwd anchor).
- Samples: 4 sessions. Three (opus-4-6, opus-4-5 older format, sonnet-4-6
  with an `api_retry` 529-overload storm) predate the `.claude/` /
  `.audit-key` / `spaces.json` structure. `local_5c09adfc` is a deliberately
  benign staged session (a generic CSV analysis) added to capture a
  populated `uploads/` sidecar and the newer structure. Same workspace UUID
  for all because only one workspace existed on the source machine.
- The web chat history at
  `~/Library/Application Support/Claude/IndexedDB/https_claude.ai_0.indexeddb.leveldb/`
  is described in `claude_desktop_app/schema-notes.md` but not captured (binary
  LevelDB, separate extraction work).

### claude_code (Claude Code CLI)

- Source path: `~/.claude/projects/<encoded-project-path>/<session-uuid>.jsonl`
- Layout: one JSONL per session, one directory per encoded project path.
  Encoded path mirrors the project cwd with `/` replaced by `-`. Lines are
  typed entries with `parentUuid` -> `uuid` chains. Tool results arrive as
  `user` entries whose `message.content[]` contains `tool_result` blocks,
  plus a parallel `toolUseResult` field carrying richer structured data
  (`structuredPatch` for Edits, file contents for Reads, etc.). A session
  that used the Task tool also has a `<session-uuid>/subagents/` sidecar
  directory: one `agent-<hash>.jsonl` transcript per subagent plus a sibling
  `agent-<hash>.meta.json` (`{agentType, description, toolUseId}`). The workflow
  runner nests transcripts one level deeper, at
  `<session-uuid>/subagents/workflows/<wf-id>/agent-<hash>.jsonl` (+ sibling
  `.meta.json`); subagent detection keys off the `subagents/` ancestor at any
  depth and derives the child id from the full path below it.
- Samples: 10 sessions. The original 3 (`myproject-a/b/c`) are one session
  each across 3 projects and 3 CLI versions: 2.1.68 is the older format (no
  SessionStart attachment, no `last-prompt` / `permission-mode` standalone
  events, no per-row `entrypoint` / `gitBranch`); 2.1.104 and 2.1.132 are the
  modern hook + attachment flow with `deferred_tools_delta`,
  `mcp_instructions_delta`, `skill_listing`, `stop_hook_summary`, etc.
  `myproject-d` adds 6 deep-redacted sessions from one real project spanning
  CLI versions 2.1.71 / 2.1.92 / 2.1.98 / 2.1.109, added for Tier-1 search
  relevance / filter test diversity. The 10th (`pond`, CLI 2.1.144) is a real
  session on this repo that used the Task tool, added for the subagent
  on-disk layout the others lack: a `<parent-uuid>/subagents/agent-<hash>.jsonl`
  transcript plus its sibling `agent-<hash>.meta.json`. Its project is kept as
  `pond` - the host repo, not an undisclosed third-party project, so no
  `myproject-*` placeholder. Across all 10 sessions the set covers 8 distinct
  CLI versions and includes `queue-operation` entries (the message-queue
  feature - these carry no uuid / parentUuid and must be skipped in the
  parentUuid chain).
- `windows-projects/` is a separate root holding a real native-Windows capture
  (added 2026-08-14): two sessions under `C--dev-pond-fixture-demo-v2`, the slug
  Claude Code chose for a `cwd` of `C:\dev\pond fixture_demo.v2`, one of them
  with two subagent sidecars. It pins the project-slug encoding for drive
  colons, backslashes, spaces, underscores and dots. Its consumers are the
  Windows gate tests in `tests/integration/adapter/claude_code.rs` and the
  `native_restore_is_value_equal_to_the_windows_capture` unit test in
  `src/adapter/claude_code.rs`; the `projects/` conformance census stays 13
  sessions.

### claude_managed_agents (Anthropic API Managed Agents)

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
- See `claude_managed_agents/schema-notes.md` for a detailed event-stream
  walkthrough.

### codex_cli (OpenAI Codex CLI)

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
- Samples: 2 sessions across 2 dates from the interactive `codex_cli_rs`
  originator, models `gpt-5` and `gpt-5-codex`, plus one pre-Oct-2025 legacy
  rollout (bare header, un-enveloped payloads), plus one JS-runtime rollout
  (below).
- JS-runtime sample (`2026/09/01/rollout-...-01a05e4f-6011-7b73-b3cf-742c36deb501.jsonl`):
  captured 2026-09-01 by sandbox self-capture - `codex exec` 0.152.0
  (originator `codex_exec`, model `gpt-5.6-sol`, `-s workspace-write`,
  `approval_policy=never`) under a throwaway `HOME`/`CODEX_HOME` at the
  neutral base path `/tmp/codex-fixture`, project cwd a two-file git repo with
  one commit and no remote. The operator's `auth.json` was copied into the
  sandbox purely to authenticate and deleted before anything was copied out.
  `HOME` had to be sandboxed as well as `CODEX_HOME`: codex discovers skills
  under `$HOME/.agents/skills`, and a rehearsal with only `CODEX_HOME` set
  wrote the real username and skill catalogue into the first user turn.
  Codex 0.147+ routes every tool through a JavaScript runtime, so the rollout
  carries `custom_tool_call{name:"exec", input:<js>}` /
  `custom_tool_call_output` pairs with `event_msg item_completed`
  (`CommandExecution`, `FileChange`) rows between them; it also carries the
  newer `world_state` top-level rows. Census (57 rows): 8 `message`, 7
  `custom_tool_call`, 7 `custom_tool_call_output`, 5 `reasoning`, 8
  `token_count`, 7 `item_completed.CommandExecution` (exit codes 0,1,0,0,1,0,2),
  1 `item_completed.FileChange`, 5 `item_completed.Reasoning`, 2
  `item_completed.AgentMessage`, 1 `item_completed.UserMessage`, 1
  `task_started`, 1 `task_complete`, 2 `world_state`, 1 `turn_context`, 1
  `session_meta`. The seven calls: a script that only filters `ALL_TOOLS`
  (no `tools.` reference - stays `exec`), `ls`, `cat missing.txt` (exit 1),
  `sed -n '1,120p' notes.md`, an `apply_patch`, one script running `echo a`
  / `false` / `echo b` as three `exec_command` calls, and `sh -c "exit 2"`.

### grok-build (xAI `grok` CLI)

- Source path: `$GROK_HOME/sessions/<encoded-cwd>/<session-uuid>/` (default
  `~/.grok/sessions/`). The cwd bucket is the percent-encoded working
  directory, or `<slug>-<blake3-hex16>` plus a `.cwd` sidecar when the encoded
  form exceeds 255 bytes. The adapter ingests `updates.jsonl` (the envelope
  stream grok itself calls authoritative) plus `summary.json` for identity /
  project / lineage; every other sibling is documented non-capture
  (`docs/knowledge/references/grok-build.md` row 8).
- Samples: captured 2026-08-24 by sandbox self-capture - grok-build 1.0.5
  (binary `5115b46bc909`) under throwaway homes at neutral base paths
  (`/tmp/grok-fixture` on macOS, `C:\gf` on Windows 11 Pro x64), model
  `grok-4.6` via the operator's X-account OAuth session: the host
  `~/.grok/auth.json` was copied into each sandbox purely to authenticate and
  deleted before anything was copied out. Project cwd was a two-file git repo
  with one commit and no remote, so `summary.json` git metadata is populated
  but carries no identity. Headless `-p` runs drove most sessions; the
  rewind/compact session ran in the TUI under tmux.
- Census: 15 sessions across three buckets (12 macOS project - 11 plus the
  subagent child - 1 hash-form long-cwd, 2 Windows) plus the root
  `session_search.sqlite` sibling. Per
  session (`updates.jsonl` kind counts): text `...099c` and Windows twin
  `...d15b` (2 user / 2 thought / 2 agent / 2 turn_completed each); image
  `...24ab` (image content block, `image_dropped`, 3 tool_call / 6
  tool_call_update); tools `...4cdd` (6 tool_call / 12 tool_call_update:
  completed, exit-1 completed, and a `failed` read of a missing file;
  `terminal/` spill logs) and Windows tools `...f4ae` (3 tool_call / 6
  tool_call_update); fork `...8db1` (parent-prefixed eventIds in the inherited
  prefix, `parent_session_id` + `forked_at` in summary); subagent parent
  `...9c5a` (`subagent_spawned` + `subagent_finished`, `subagents/<id>/`
  meta.json + output.json) with child `...aead` (`session_kind: "subagent"`,
  no parent field of its own); plan `...cbdb` (1 `plan`); hook `...f1a8`
  (1 `hook_execution`); interrupted `...0c00` (a `tool_call` with no terminal
  update and no `turn_completed`); retry-failed `...4ad7` (`retry_state`
  `type: failed` and no assistant output); no-updates `...5430` (a session dir
  with no `updates.jsonl` at all); long-cwd `...5bc4` (hash bucket + `.cwd`);
  TUI `...85a0` (4 turns, 1 `rewind_marker`, reused `promptIndex`, 1
  `compaction_checkpoint` + `auto_compact_completed`, compaction sidecars).
- Anonymization: one edit. grok's Claude-compat skill discovery resolves the
  real Windows profile through the known-folder API (ignoring the sandbox
  `USERPROFILE`), so a skill path in the two Windows `chat_history.jsonl`
  sidecars carried the real username; it was replaced with `user`. Both files
  are non-capture; no ingested file needed any edit. Sweeps: trufflehog 0,
  gitleaks 0, every JSON/JSONL file parses (the zero-byte `events.jsonl` in
  `...5430` is the one intentionally empty file), no `/Users/<name>`,
  `C:\Users\<name>`, hostname, email, or key material outside xAI's own
  documentation strings (`/Users/me/photo.jpg` examples inside prompt text).
- Windows capture bytes: LF only (zero `\r`), no BOM, ASCII-clean - grok
  writes the same JSONL shape on both platforms; the bucket name pins the
  drive-colon/backslash encoding (`C:\gf\project` -> `C%3A%5Cgf%5Cproject`).

### hermes (Hermes Agent)

- Source path: `~/.hermes/state.db` (default profile) plus
  `~/.hermes/profiles/<name>/state.db` (named profiles). `$HERMES_HOME`
  overrides the home root. One SQLite DB per profile - no JSONL, no per-session
  files.
- Layout: a single `state.db` per profile holding `sessions` (session rows,
  free-form `source` gateway/platform tag, `parent_session_id` lineage,
  `started_at`/`ended_at` REAL epoch seconds) and `messages` (`id` AUTOINCREMENT
  transcript rows: `role` in user/assistant/tool/system, `content`,
  `tool_calls` JSON, `tool_call_id`/`tool_name`, `reasoning`, `active`/
  `compacted` flags, `timestamp` REAL). Message `content` is a plain string OR a
  JSON payload prefixed with the `\x00json:` sentinel (NUL + `json:`) carrying a
  multimodal part list. DDL copied verbatim from `hermes_state.py`
  (SCHEMA_VERSION 23).
- Samples: FULLY SYNTHETIC (the hermes runtime is inherently personal, so no
  real capture is committed). Two DBs built from the verbatim SCHEMA_VERSION-23
  DDL (the same schema the adapter unit tests embed):
  - `state.db` - 6 sessions / 18 messages. `sess-root` is a telegram
    conversation that exercises a reasoning column, a tool call + tool-result
    pair (`weather`), a `\x00json:` multimodal user message (text + image_url),
    and a compaction tail (a pre-compaction turn flipped to `active=0,
    compacted=1` plus a `compacted=1` summary row). `sess-comp` is a compaction
    successor (parent `sess-root` ended `end_reason='compression'`),
    `sess-branch` a `/branch` child (`model_config._branched_from` marker),
    `sess-delegate-parent` + `sess-sub` a delegate spawn pair
    (`model_config._delegate_from`), and `sess-cron` a `source='cron'` session.
  - `profiles/coder/state.db` - 1 session / 2 messages: a `source='cli'` session
    with no gateway routing, so its `project` falls back to `cwd`.
- Exercised by the `hermes` adapter: the integration suite
  (`tests/integration/adapter/hermes.rs`) ingests both DBs through a real
  `Store` (7 sessions) and asserts source_agent taxonomy
  (`hermes` / `hermes/subagent` / `hermes/cron`), the three lineage relations,
  project derivation, multimodal + tool part survival, searchability, and
  additive re-sync freshness via the rowmap oracle.

### letta-code (`letta` CLI)

- Source path: `~/.letta/transcripts/<agentId>/<conversationId>/transcript.jsonl`
  (`$LETTA_TRANSCRIPT_ROOT` overrides the root). The transcript is letta-code's
  client-side reflection log, appended on every `end_turn`; the conversation's
  full message history lives in the backend (Letta Cloud, or the local backend's
  `lc-local-backend/`), which the adapter does not read.
- Layout: one `transcript.jsonl` per conversation directory, one JSON row per
  line in two shapes: `{kind: user|assistant|reasoning|error, text}` and
  `{kind: tool_call, name?, argsText?, resultText?, resultOk?}`, each with a
  per-turn `captured_at` stamp (one value shared by every row of a turn) and
  optional `source_line_id` (the provider tool-call id on `tool_call` rows) /
  `source_message_id`. Sidecars in the same directory: `state.json` (reflection
  cursor; rewritten in place) and `payload-auto-<nonce>.json` (a `/reflect`
  payload). A per-agent `multi-reflection-payloads/` directory holds cross-conversation
  payloads only. The adapter reads `transcript.jsonl` alone.
- Samples: captured 2026-08-24 by sandbox self-capture - letta-code 0.30.30
  under a throwaway `HOME` (`/private/tmp/letta-fixture/home`, a neutral base
  path so no username appears in any row), `letta --backend local` so no
  Letta account or credential file was involved, model
  `openrouter/anthropic/claude-haiku-4.5` via `OPENROUTER_API_KEY` from the
  environment only, `--yolo` to auto-approve tools. The first agent was
  driven through the interactive TUI under tmux (the only producer that
  writes tool rows; the `-p` one-shot path never writes the transcript), the
  second and third through the headless bidirectional stream (the third on
  Windows 11, the rest on macOS). Three agents:
  - `agent-local-0ce90846-.../default` - a text-only turn, a two-tool turn
    (`Read` then `Bash`, both `resultOk: true`), a failed `Bash`
    (`resultOk: false`, exit code in `resultText`), and a reasoning turn
    (`/reasoning-tab on`, effort `low`: a `reasoning` row before its
    `assistant` sibling), plus the `state.json` and `payload-auto-yx1ua6.json`
    a `/reflect` wrote afterwards.
  - `agent-local-0ce90846-.../local-conv-2` - a `/new` conversation of the
    same agent: one turn with a `reasoning` row, an `assistant` row, a `Read`
    tool row and the final `assistant` row; its `letta-msg-<n>` ids start at
    189, showing the counter is per process, not per conversation.
  - `agent-local-0ce90846-.../local-conv-3` - a zero-byte `transcript.jsonl`
    (a `/reflect` on an empty conversation), which ingests nothing.
  - `agent-local-0ce90846-.../conversation-00000000-0000-4000-8000-000000000001` -
    SYNTHETIC, hand-written to the pre-2026-04 row shape (no
    `source_line_id` / `source_message_id`, legacy `v2_message_id` in
    `state.json`) to cover an unfinished `tool_call` (no result fields), an
    `error` row, and a result row without `resultOk`. Current letta-code cannot
    produce these, and no real legacy home exists to capture from.
  - `agent-local-61c7e9e2-.../local-conv-1` - a second agent, two text-only
    turns from the headless bidirectional path (`user-<uuid>` line ids, no
    tool rows), so conversation ids visibly repeat across agents and the
    adapter's project (= agent id) has two values.
  - `agent-local-7ea0712d-.../local-conv-1` - the native Windows capture: a
    third agent, same headless bidirectional path, written by letta-code
    itself on Windows 11 Pro x64 (10.0.26200) on 2026-08-24 with letta-code
    0.30.30 on Node v24.19.0 / npm 11.17.0, sandbox `USERPROFILE` / `HOME` /
    `APPDATA` / `LOCALAPPDATA` under `C:\lf\home` and cwd `C:\lf\project`, so
    the transcript root resolved to `C:\lf\home\.letta\transcripts`. Two
    text-only turns (2 `user` + 2 `assistant` rows). Observed bytes: no UTF-8
    BOM (file starts `7b 22 6b 69 6e 64` = `{"kind`), LF only (zero `\r` bytes),
    final byte `0a`, every byte ASCII - identical to what the macOS captures
    produce. No `C:\` path, username or hostname appears in any row; letta
    records no cwd in the transcript (the stream-json `init` event does carry
    `"cwd":"C:\\lf\\project"`, but that event is not part of the transcript).
    This is what the adapter spec's Windows row rests on, and CI's Windows leg
    ingests it with the rest of the fixture.
- Census: 5 ingestible sessions; secret sweep trufflehog 0 / gitleaks 0; every
  file parses; no host, username, `/Users/` path, `C:\` path or provider key
  string.

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
- Samples: `agentgroup-anon-001/` holds the real captured ground truth - 1
  top-level session, 1 subagent (from a different parent session so the
  subagent sidecar layout can be demonstrated alongside the top-level shape),
  1 `.meta.json` example, 2 `tool-results/*.txt` examples.
  `agentgroup-synthetic-001/` holds 2 SYNTHETIC sessions, generated by
  structural replay of the real fixture (every record shape is copied
  verbatim from `agentgroup-anon-001/`, only IDs are remapped) to cover edge
  cases the single real session does not: complete parent + own `subagents/`
  + own `tool-results/` sets, and multi-subagent fan-out (session B has 3).
  The synthetic files exist because the nanoclaw runtime is inherently
  personal (founder-assistant sessions), making real captures hard to
  anonymize; the real `agentgroup-anon-001/` set remains the schema anchor.
- Exercised by the `nanoclaw` adapter: the integration suite
  (`tests/integration/adapter/nanoclaw.rs`) ingests this whole corpus (8
  sessions - 3 top-level + 5 subagent sidecars) through a real `Store` and
  asserts session/message counts, get round-trips, and searchability. The
  opencode-provider composition and codex-provider skip cases build their
  `opencode-xdg/` stores (from the committed `opencode` DB fixture) and
  `v2.db` provider tables synthetically in-test, so no committed nanoclaw
  fixture change was needed for them.

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

### openclaw-captures

Four complete OpenClaw state roots, each produced by driving the REAL runtime
and copying what it wrote. They exist because the hand-written `openclaw/`
fixture above encodes what we believed OpenClaw does; issue #224 was a case
where that belief was wrong in a way no synthetic fixture would catch. Several
findings here contradict both the old fixture and the upstream source read:
`parentSession` holds a path rather than an id, a stored session key always
carries an `agent:<id>:` prefix, and an isolated cron run stamps its job onto
the first user message.

- Provenance: OpenClaw `2026.7.1-2` (npm, commit `0790d9f`) on official Node
  v24.21.0, captured 2026-09-10. Every run used a throwaway `$HOME` and an
  OpenAI-compatible stub model on `127.0.0.1`, so no real provider was called
  and no real conversation is present - assistant turns read `stub reply N:`.
- Layout: each pass directory IS an OpenClaw root. Point the adapter's `root`
  at it directly.

  ```
  <pass>/
    agents/main/sessions/       transcripts, .trajectory.jsonl sidecars,
                                .trajectory-path.json pointers, sessions.json
    state/openclaw.sqlite       only where the pass needs it (see below)
    capture.sh                  the script that drove the runtime
  ```

  `rotate-reply` additionally carries `lineage.jsonl`, a snapshot of its
  `sessions.json` entry taken after every rotation. It is derived evidence, not
  something OpenClaw wrote, which is why it sits beside the tree rather than in
  it - and it is the only surviving record that `usageFamilySessionIds`
  accumulated across the generations, because the final `sessions.json` has that
  field wiped by the two `sessions.reset` calls.

- The four passes and what each one is for. "ingested before #224" is how many
  transcripts the adapter picked up when it read session keys only from
  `sessions.json` `sessionId`; the rest were dropped silently.

  | pass | transcripts | ingested before #224 | exercises |
  | --- | --- | --- | --- |
  | `rotate-reply` | 3 | 2 | reply-path reset rotation, `.jsonl.reset.<ts>` archives, `usageFamilySessionIds` |
  | `cron` | 9 | 4 | isolated + main-target + `session:<key>`-target cron, `:run:` keys, reaper `.deleted.` archives, `cron_run_logs` |
  | `compaction` | 7 | 4 | `truncateAfterCompaction` successors (`<ts>_<id>.jsonl`), in-place compaction, `compactionCheckpoints`, a checkpoint branch |
  | `hooks-heartbeat` | 8 | 5 | every hook run is `forceNew`, isolated heartbeat beats, `audit_events` |

- Three passes carry a `state/openclaw.sqlite`, each trimmed to the one table
  its pass needs: `cron_run_logs` (13 rows) for `cron`, `audit_events` for
  `hooks-heartbeat` (14 rows) and `rotate-reply` (24 rows). `compaction` needs
  neither and has no `state/`. Everything else OpenClaw writes into that DB is
  unrelated to session keys and would only add weight. Each DB is checkpointed
  to a plain rollback journal (`journal_mode=delete`) so the fixture is a
  single file with no `-wal`/`-shm` sidecars, matching the opencode fixture.
- The `.trajectory.jsonl` sidecars are the bulkiest part (~150 KB each: each
  embeds the full compiled system prompt) and are committed verbatim. They are
  the single most productive recovery source - every line carries a top-level
  `sessionKey` - so trimming them would remove the evidence that the head of
  the file is enough.
- Sanitization: the capture homes were already throwaway, so the only
  substitution is the machine hostname, which OpenClaw writes into sidecars as
  `host=<hostname>` and which became `sandbox-host`. Absolute paths under
  `/tmp/openclaw-fixture/<pass>/capture-home/` are left as-is - they are the
  capture sandbox, name nobody, and `sessions.json` `sessionFile` values are
  read by basename anyway. gitleaks and trufflehog both scan clean; gitleaks'
  generic-api-key rule matches OpenClaw's per-request `idempotencyKey` uuids,
  allowlisted in `.github/gitleaks.toml`.
- To re-capture, work from each pass's `capture.sh`: install the pinned
  OpenClaw under a throwaway `$HOME`, point it at a stub OpenAI-compatible
  server on localhost, run the script, then copy `agents/` (and the trimmed
  `state/openclaw.sqlite` where the pass needs it) out. Note that a nixpkgs
  Node will NOT work - it links SQLite 3.51.2 and OpenClaw's WAL guard
  requires 3.51.3+; the official nodejs.org build bundles a new enough one.

### openclaw-captures/db-era

A fifth OpenClaw state root, captured the same way as the four passes above but
from a version past the storage rewrite. The four above are the FILE era
(OpenClaw `<= 2026.7.1-2`); this one is the DB era. OpenClaw 2026.8.1 replaced
the file session store entirely, so on 2026.9.3 there is no file tier at all:
`agents/main/sessions/` does not exist until something is deleted, and the one
file it then holds is a compressed archive blob, not a transcript. Everything
the file-era adapter reads - `sessions.json`, `<id>.jsonl`,
`.trajectory.jsonl`, `.trajectory-path.json` - is absent, and pond ingested 0
of this root's 8 session generations before the fix. Those 8 generations
(`session_windows` rows) sit across 6 routing keys (`session_nodes` rows); pond
now ingests all 8, which is what the `every_db_era_generation_is_ingested`
integration test asserts.

- Provenance: OpenClaw `2026.9.3` (commit `1391f7c`; the fixture's own
  `schema_meta` row records `2026.9.3` at agent schema version 19) on official
  Node v24.21.0, captured 2026-09-10 on a real host driven through its own
  gateway (local mode, loopback, port 18840) by `capture.sh`, which is
  committed inside the fixture directory. Throwaway `$HOME` and an
  OpenAI-compatible stub model on `127.0.0.1`, so no real provider was called
  and no real conversation is present - assistant turns read `stub reply N:`.
- Layout: the pass directory IS an OpenClaw root. Point the adapter's `root` at
  it directly. Four files, 575,446 bytes:

  | bytes | path |
  | --- | --- |
  | 413,696 | `agents/main/agent/openclaw-agent.sqlite` |
  | 151,552 | `state/openclaw.sqlite` |
  | 9,296 | `capture.sh` |
  | 902 | `agents/main/sessions/9b08ee68-1f8d-4ae1-bdf8-251b02e76fdb.jsonl.deleted.2026-09-10T18-51-19.016Z.3c20490d108847ee9f86861e3acc663d.zst` |

  That `.zst` is the whole of `agents/main/sessions/`: the directory exists only
  because the capture deleted a session. Its name is the file-era archive shape
  plus two segments - `<id>.jsonl.<reason>.<ts>.<generation-32hex>.zst`.

- Agent DB (`agents/main/agent/openclaw-agent.sqlite`), trimmed to the seven
  session tables. `session_transcript_generations` does not exist at 2026.9.3;
  `session_transcript_index_state` does.

  | table | rows |
  | --- | --- |
  | `schema_meta` | 2 |
  | `session_nodes` | 6 |
  | `session_windows` | 8 |
  | `transcript_events` | 94 |
  | `transcript_event_identities` | 94 |
  | `session_transcript_index_state` | 8 |
  | `session_transcript_archives` | 1 |

- State DB (`state/openclaw.sqlite`), trimmed to three tables:

  | table | rows |
  | --- | --- |
  | `audit_events` | 30 |
  | `task_runs` | 13 |
  | `cron_run_receipts` | 2 |

- The seven session keys the capture produced. "generations" is
  `session_windows` rows for that key; 6 nodes / 8 windows / 94 events, and the
  counts sum.

  | session key | generations | events | `created_via` | what it is |
  | --- | --- | --- | --- | --- |
  | `agent:main:main` | 1 | 29 | NULL | two baseline `--local` turns plus two in-transcript resets; the only row with `session_scope = 'shared-main'` (every other row is `'conversation'`) |
  | `agent:main:dashboard:01ee5473-...` | 1 | 27 | `operator` | the fork of `agent:main:main` |
  | `agent:main:dashboard:compactme` | 1 | 5 | `run` | three turns, then `sessions compact --max-lines 5` |
  | `agent:main:cron:556dc024-...` | 2 | 6 + 6 | `cron` | one isolated cron job, run twice |
  | `agent:main:dashboard:rollover` | 2 | 7 + 7 | `run` | idle rollover, `session.reset.mode=idle` / `idleMinutes=1` across a 70 s gap |
  | `agent:main:dashboard:doomed-archive` | 1 | 7 | `run` | `sessions archive`, a SOFT archive: it sets `session_nodes.archived_at` (`1789066281620`) and writes nothing else |
  | `agent:main:dashboard:doomed-delete` | 0 | 0 | - | `sessions delete`; no node and no window row survive it |

- The structural facts this fixture pins, each with the evidence in the tree:
  - **`session_windows` holds one row per generation.** 8 rows over 6 keys: the
    cron key has two (one per run) and the rollover key has two. This is the
    thing the file era forced a reader to reconstruct from sidecars.
  - **`session_windows.reason` is NULL on all 8 rows.**
    `SELECT count(*) FROM session_windows WHERE reason IS NOT NULL` returns 0.
    The runtime hardcodes it; the column's CHECK enum
    (`initial|reset|rollover|fork|rewind|switch|recovery|compaction`) is
    vestigial, and nothing here should be classified by it.
  - **`previous_session_id` is set on exactly 1 of 8 rows** - the second
    `rollover` generation, pointing at the first. The cron key's two
    generations are NOT chained to each other, so `previous_session_id` walks
    only the idle/daily rollover path; grouping by `session_key` is the
    reliable primitive.
  - **A reset does not rotate a session.** It appends an in-transcript event
    instead: `transcript_events` for `agent:main:main` carries
    `{"type":"reset","id":"0723e4cd",...,"reason":"reset"}` at seq 13 and
    `{"type":"reset","id":"3fc2fc88",...,"reason":"new"}` at seq 19, both inside
    the one `session_id`, with no extra window row and no archive. `reason` is
    the verbatim RPC argument and `id` is an 8-hex short id unlike every other
    event's uuid, so reset-delimited generations live on the event-sequence
    axis, not the session-id axis.
  - **Truncating compaction deletes events with no archive at all.** In
    rehearsal `--max-lines 5` cut a 25-row transcript to 5, keeping the same
    `session_id` and writing no archive row and no file. `compactme` is that
    scenario in the capture: three turns are left as 5 events re-sequenced from
    0 with a regenerated header (`session`, `custom`, `message`, `message`,
    `custom`), and the one `session_transcript_archives` row belongs to the
    delete, not to this. There is no cold copy of what was cut.
  - **Fork lineage IS first-class.** The fork's `session_nodes` row carries
    `parent_session_key`, `fork_source_session_key` (`agent:main:main`),
    `fork_source_session_id` (`53ced050-...`) and `fork_source_entry_id`
    (`8c3aeed5-...`), and that entry id is a real
    `transcript_event_identities.event_id` in the parent. A fork is a new key
    rather than a new generation, so its window row's `previous_session_id` is
    NULL. It also copies the parent's events with their `event_id`s intact -
    42 identity rows share an `event_id` with a row in another session (21
    events, twice each) - so an event id is unique only per session, and a
    naive global dedupe merges a fork into its parent.
  - **A deleted session leaves NO `session_windows` row.** `9b08ee68-...` has
    zero window rows and zero `session_nodes` rows; the delete cascades them
    away. Its only traces are the archive blob (whose
    `session_transcript_archives.session_key` names
    `agent:main:dashboard:doomed-delete`) and `audit_events`. The window table
    is therefore not a complete history of everything that ran.
  - **`session_transcript_archives.archive_sha256` covers the COMPRESSED
    bytes.** The row's `archive_sha256` is
    `341155e2100db1e8c0579eb1a1b88b546036572fb4b8d98ebab750b95bc2cbf8`, which is
    exactly `sha256sum` of the 902-byte `.zst` file, and `length(archive_blob)`
    is 902 too. `encoding` is `zstd`; the payload decompresses to 7 JSONL lines
    of file-era transcript with a `{"type":"session","version":4,...}` header.
- Sanitization: the capture home was already throwaway, so no substitution was
  needed at all - not even the hostname the file-era passes had to replace,
  because the preamble that carries `os.hostname()` lives in
  `trajectory_runtime_events`, a table this fixture drops. Absolute paths under
  `/tmp/openclaw-db/db-era/capture-home/home/` are left as-is. The agent DB was
  trimmed from 1,495,040 B by dropping 36 tables and 16 triggers (including
  `auth_profile_store`, `auth_profile_state`, `trajectory_runtime_events` and
  every `memory_*` and FTS shadow table); the state DB from 3,350,528 B by
  dropping 104 tables, among them every `device_*` table,
  `secret_store_entries`, `mcp_oauth_stores`, `worker_environment_credentials`,
  `audit_identity_keys`, `user_profiles` and `user_profile_identities`. No
  auth, identity, device or credential table remains in either file. Both DBs
  are `journal_mode=delete` with no `-wal`/`-shm` sidecars, matching the four
  file-era passes - note that a read-only `node:sqlite` open RECREATES those
  sidecars, so anything that inspects the fixture must delete them afterwards.
  gitleaks and trufflehog scan clean over the tree and over a materialized text
  dump of every TEXT cell plus the decompressed archive (the dump matters:
  gitleaks skips binaries, so it reads almost nothing of a bare SQLite
  fixture).
- Deliberate deviation, flagged: the state DB keeps `task_runs` and
  `cron_run_receipts` alongside `audit_events`. `cron_run_logs`, the table the
  `cron` pass above is trimmed to, does not exist at 2026.9.3, and
  `task_runs.child_session_key` is its functional replacement - it is the one
  place besides `audit_events` where the cron `:run:<sessionId>` keys appear,
  and its 8 distinct child keys cover every key in the capture including the
  deleted one.

### opencode

opencode has TWO on-disk formats and this fixture carries BOTH, because the
adapter must read both. opencode stopped writing the JSON fan-out tree in
v1.2.0 (2026-02-14); current releases write a SQLite database instead, and a
one-time startup migration (removed 2026-06-02) copied the old tree into it.
Users who jumped past that migration keep tree-only sessions that never reach
the DB, so completeness requires reading the DB PLUS the stale tree.

- Source-of-truth layout under the opencode data dir
  (`~/.local/share/opencode/`):
  - `opencode.db` - the primary store since v1.2.0. SQLite, normally WAL
    (checkpointed to a plain rollback-journal `.db` here so the fixture is a
    single file). Channel variants live at `opencode-<channel>.db`. Schema
    (`packages/core/src/session/sql.ts`):
    - `session`: typed columns, no JSON blob (`id`, `project_id`,
      `workspace_id`, `parent_id`, `slug`, `directory`, `path`, `title`,
      `version`, cost/token counters, `agent`, `model` JSON, `time_created`,
      `time_updated`, `time_compacting`, `time_archived`).
    - `message`: `id`, `session_id`, `time_created`, `time_updated`, `data`
      = the old per-message JSON minus `id`/`sessionID`.
    - `part`: `id`, `message_id`, `session_id`, `time_created`,
      `time_updated`, `data` = the old part JSON minus
      `id`/`sessionID`/`messageID`.
    opencode rehydrates JSON as `{...data, id, sessionID(, messageID)}`; parts
    order `ORDER BY message_id, id`. Sibling `project` / `project_directory`
    tables map `project_id` to a `directory` path. The `event` table is
    opencode's internal pub/sub log (a redundant copy of session/part events);
    the adapter ignores it, but it is kept here as realistic DB content.
  - `storage/{session,message,part}/...` - the STALE legacy fan-out tree, one
    file per object, left behind by the migration and never updated by current
    opencode. `session/<projectID>/<sessionID>.json` (session metadata),
    `message/<sessionID>/<messageID>.json` (user stubs; assistant messages
    carry `system` prompt array, `modelID`, `providerID`, `mode`, `path`,
    `cost`, `tokens`), `part/<messageID>/<partID>.json` (types `text`,
    `reasoning`, `tool` state-union, `step-start`, `step-finish`, `file`,
    `patch`; tool calls use `callID` like Anthropic `toolu_*`).
  - ULID-style IDs throughout (`ses_*`, `msg_*`, `prt_*`).

- `opencode.db` sample (the DB-era source of truth): GENERATED, not captured
  from a real user. Produced by driving the real pinned opencode CLI
  (`opencode 1.17.15`, `/opt/homebrew/bin/opencode`) non-interactively in
  sandboxed XDG dirs rooted at a NEUTRAL base path (`/tmp/oc-fixture`) so no
  username leaks into the recorded `directory` columns; project cwd was a
  throwaway git repo at `/tmp/oc-fixture/project` (macOS resolves this to
  `/private/tmp/oc-fixture/project`), plus a second repo at `.../project2` for
  a distinct `directory`/`project_id`. A copy of the host `auth.json` was
  placed in the sandbox data dir purely to authenticate, then deleted before
  the DB was finalized (`account`/`credential`/`workspace` tables are empty -
  opencode stores auth in `auth.json`, never in the DB). Model:
  `openrouter/anthropic/claude-haiku-4.5` (cheap), all runs with `--auto` to
  auto-approve tool permissions. Staged prompts (`opencode run`) drive one
  session each to reach the part types and session shapes below.
  - Census: 10 sessions (1 child with `parent_id` set, 1 doctored-archived),
    across 2 project directories; 27 messages; 69 parts. DB is ~496 KB,
    single file, no `-wal`/`-shm` sidecars.
  - Part-type coverage: `text`, `reasoning` (via `--variant high --thinking`),
    `file` (a user `-f` attachment), `patch` (an edit-tool run), `tool` in
    both `completed` and `error` states (read of an existing vs missing file;
    an `edit`; and a `task` tool call), plus `step-start`/`step-finish`
    (which also carry the git `snapshot` hash as a field - opencode 1.17.15
    embeds snapshots on step parts rather than emitting a distinct `snapshot`
    part). The child session is created by a `task`-tool prompt: the child
    `session` row carries `parent_id` and `agent = general`, and the parent
    carries a `tool` part named `task`.

- Doctored rows (no current CLI can produce these states, so they are set by a
  documented `UPDATE`; both replicate real-world DB quirks):
  - Migration-stamp row: message `msg_f6041991e001BOYvwdP1iI0H0A` (the
    assistant reply in the plain-text session
    `ses_09fbe676bffe9nYsSBi5xhBlaD`) has its `time_created` column pushed ~4
    months forward - to `1794394339614` (2026-11-11) - while its truthful
    `data.time.created` stays `1784026339614` (2026-07-14). This replicates
    the Feb-2026 migration quirk where the `time_created` COLUMN is the
    migration time, not the message time; the adapter must trust
    `data.time.created`, not the column.
  - Archived session: `ses_09fbd06d2ffewwkvIM1tMfh8o8` has `time_archived` set
    (`1784030031861`, ~1h after its `time_updated`). opencode 1.17.15 has no
    CLI archive command, so the column is set directly.

- Legacy `storage/` tree sample (the stranded-JSON source of truth): 4
  sessions, 57 message files, 172 part files, retained from the 2026-05-13
  capture and NOT regenerated (it is exactly the format current opencode no
  longer writes, so it is the fixture for the tree-only ingest path). Single
  projectID because only one existed on the source machine; session-internal
  placeholders distinguish `myproject-a` / `myproject-b` / `myproject-c`. The
  `ses_64247d48` session covers the `reasoning` part type (7 reasoning parts)
  plus an assistant-message `error` field and the `tool` `error` state. These
  session IDs do not overlap the `opencode.db` IDs, so both sources ingest
  fully; construct an overlapping-ID case in-test if the dedup path needs it.

- Secret hygiene: the generated DB was dumped (`sqlite3 .dump`) and swept -
  `trufflehog` and `gitleaks` report zero findings, no `/Users/...` path or
  username survives (a stray `project` row that opencode auto-registered for
  the real worktree cwd was deleted), and every `message.data` / `part.data`
  blob passes `json_valid`.

### pi (pi-coding-agent CLI)

- Source path: `~/.pi/agent/sessions/<encoded-cwd>/<timestamp>_<ulid>.jsonl`
- Layout: encoded-cwd as dir name (one per project), `<timestamp>_<ulid>.jsonl`
  filename. Newline-delimited JSON with a version-3 envelope. First line is
  `{type:"session", id, timestamp, cwd}`. Subsequent lines are events with
  `id` + `parentId` forming a DAG / tree rather than a flat sequence. Event
  types: `session`, `model_change`, `thinking_level_change`, `message`.
  Assistant messages carry rich provenance: `usage` (input / output /
  cacheRead / cacheWrite / totalTokens), `stopReason`, `api`, `provider`,
  `model`, `responseId`, full `cost` breakdown.
- Samples: 4 anonymized v3 sessions across 4 projects, plus the harness-v2
  formats below.
- **harness-v2 (v4 JSONL + the SQLite backend)**, added 2026-08-06. Same
  `sessions/` root - v3 and v4 files coexist, detected per file, so the
  discovery tree stays one directory. A v4 file's first line is
  `{kind:"header", version:4, id, createdAt, cwd, parentSessionId?, metadata?}`
  and every later line is a `seq`-ordered mutation: `entry` (the conversation
  tree), `record` (harness orchestration), `lane` (branch pointers), `fact`
  (session name / entry labels). `sqlite/pi-sessions.sqlite` is the
  `@earendil-works/pi-session-backend-sqlite-node` database - one file hosting
  many sessions, whose `entries` / `records` / `lane_moves` / `facts` rows carry
  the same payload shapes as the v4 mutations.
  - `--Users-user-Projects-harness-v2--/*_v4-main-session.jsonl` exercises every
    entry type, every record type, a second lane, both fact kinds, and a tool
    call whose result and usage records tie back to it;
    `*_v4-fork-session.jsonl` is a fork carrying `parentSessionId`.
  - Regenerate with `pi-coding-agent/generate-v4-fixtures.mjs`, which drives
    pi's OWN storage code (`JsonlSessionRepo`, `SqliteSessionRepository`) so the
    committed bytes are whatever pi writes; the script's header comment carries
    the exact invocation and the pi version last used. Ids are caller-supplied
    and `Date.now` is faked, so a re-run on an unchanged pi is byte-identical
    and any diff is a real format change.
  - Torn tails and unknown future mutation kinds are NOT committed as fixtures:
    both are derived in-test from a copy of the v4 file, which keeps the
    round-trip corpus exactly the set of files the codec must reproduce.
    (Codec replay is asserted for every format; `pond resume` deliberately
    emits v3 for all of them - see the adapter header for why.)

### oh-my-pi (`omp`)

- Source path: `~/.omp/agent/sessions/<bucket>/<timestamp>_<sessionId>.jsonl`, where
  `<bucket>` is scope-encoded from the cwd: `-<home-relative>` under `$HOME`,
  `-tmp-<rel>` under the temp root, else `--<encoded-absolute>--`. (A hashed
  `<scope>-<basename>-<sha256>` form exists in the wild from omp 17.2.5-17.2.8,
  which reverted it; omp migrates those dirs back into the encoded name.)
- Layout: a pi fork that kept pi's version-3 record model, so the entries are pi
  v3 (`{type:"session", id, timestamp, cwd}` header, then `message` and
  state-carrier entries chained by `id` / `parentId`). Two container differences
  matter: the bucket directory is scope-encoded rather than pi's always-absolute
  `--<encoded-cwd>--` slug, and current files begin with a fixed-width **256-byte
  `{"type":"title","v":1,...}` slot** whose line precedes the session header.
  omp's loader strips that slot and folds it into the logical header, and so does
  the adapter (into `options.source.title_slot`).
- Samples: 2 slot-fronted sessions (one carrying the opaque `parentSession`
  lineage marker, an omp-only `ttsr_injection` entry, a `blob:sha256:` image ref,
  and a `model_change` / `branch_summary` carrier) plus 1 legacy slot-less file.
- Regenerate with `oh-my-pi/generate-fixtures.mjs`, whose header comment carries
  the exact invocation and the omp version last used. It imports omp's OWN
  `serializeTitleSlot`, so the slot bytes are omp's, and a slot-shape change
  shows up as a fixture diff. omp ships raw TypeScript with extensionless
  imports, so the script runs under **bun** (omp's own runtime), not plain node -
  the script's header says the same, so the two cannot drift. Timestamps and ids
  are literal, so a re-run on an unchanged omp is byte-identical. The bucket
  directory name is the home-scope encoded form omp writes for
  `/Users/user/Projects/omp-demo`; the adapter treats it as an inert placement
  hint either way.

## Cross-platform schema variation

Where formats fundamentally disagree (informs canonical type design in
`docs/spec.md#adapters`):

| Concern | Variants observed |
|---|---|
| Top-level file shape | JSONL stream (claude_code, codex_cli, pi, nanoclaw, openclaw, claude_desktop_app audit) vs JSON array (claude_managed_agents) vs fan-out tree (opencode) vs metadata + audit pair (claude_desktop_app) |
| Message-to-event granularity | Coalesced messages (claude_code, opencode, openclaw, claude_desktop_app audit) vs per-event stream where one assistant turn produces many events (claude_managed_agents, codex_cli with separate `response_item`s) |
| Tool call / result linking | Same-line content blocks (claude_code, claude_desktop_app, claude_managed_agents) vs separate top-level events (pi, codex_cli) vs side-table parts (opencode) vs inline content with separate `role:"toolResult"` (openclaw) |
| Inter-message linking | parentUuid chain (claude_code) vs parentId tree (pi, openclaw) vs flat sequence with span IDs (claude_managed_agents) vs file order only (codex_cli, opencode, claude_desktop_app audit) |
| Sidecar files | `tool-results/`, `subagents/` (nanoclaw) vs per-message part dirs (opencode) vs `uploads/` + `outputs/` (claude_desktop_app) vs none (most others) |
| Provider / model recording | Per-assistant-message (most) vs per-span via `span.model_request_*` events (claude_managed_agents) vs per-line `turn_context` (codex_cli) |
| Encrypted opaque payloads | Codex `encrypted_content` Fernet blobs vs none |
| HMAC over content | claude_desktop_app `_audit_hmac` vs none |
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
  Desktop users; see `claude_desktop_app/schema-notes.md`)

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
- claude_desktop_app `_audit_hmac` field values (cryptographically invalid against
  modified content but the field is kept so adapters see it)
- Cowork system prompts (identical across all Claude Desktop users;
  preserved for schema fidelity)
- Public OSS-product hostnames in scraped tool-result content where they
  are the natural output of a public web crawl

## Known fixture gaps

Tracked shortfalls where a future adapter would have untested surface.
Update as gaps are closed.

- **claude_managed_agents - single session.** All 9 event types are present
  (enough to design the adapter) but there is no second session for an
  idempotency / round-trip pair and no error or version-skew case. Source is
  an API playground export with no on-disk format, so a refresh requires a
  new export.

- **opencode `subtask` / `agent` / `compaction` / `retry` / `snapshot` part
  types absent from `opencode.db`.** opencode 1.17.15's non-interactive
  `opencode run` cannot reach them: the `task` tool records a child session
  (via `parent_id` + a `tool` part named `task`) rather than a `subtask`
  part; inline `@agent` mentions are not parsed as `agent` parts by the run
  CLI; `--command compact` / a `/compact` message error out or are treated as
  plain text (real compaction needs a context-overflow, which is expensive to
  force with a cheap model); `retry` needs a mid-run provider failure; and
  1.17.15 embeds the git snapshot as a field on `step-start`/`step-finish`
  parts instead of emitting a distinct `snapshot` part. These types all flow
  through the adapter as generic `raw_record` carriers (no type-specific
  logic), so the untested surface is only carrier injection for these five
  discriminators. Close by capturing from a real long-running install, an
  interactive/TUI session, or a newer opencode that surfaces a compact CLI
  command. The child-session lineage path itself (`parent_id`,
  `source_agent`) IS covered by the `task`-tool child session.

Closed gaps (kept here briefly for history):

- **opencode DB-era (SQLite) storage** - closed by generating
  `opencode.db` (opencode 1.17.15) alongside the retained stale `storage/`
  tree; see the opencode per-platform note for coverage and doctored rows.

- **opencode `reasoning` parts** - closed by adding `ses_64247d48` (7
  reasoning parts).
- **nanoclaw single top-level session** - closed by adding
  `agentgroup-synthetic-001/` (2 synthetic structural-replay sessions; see
  the nanoclaw per-platform note).
- **claude_desktop_app populated `uploads/` sidecar** - closed by adding the
  `local_5c09adfc` staged session. Also established definitively that
  `outputs/` is not a deliverable sink (the agent writes to the user's
  workspace folder); it stays empty by design, so it is not a gap.

## How to refresh

New conformance fixtures follow the sandbox self-capture in
`.agents/skills/add-adapter/SKILL.md` (run the agent under a throwaway home,
so the capture is born clean and this file's rules become a verification
step). The host-capture procedure below is the legacy path the pre-playbook
samples came from; use it only to refresh one of those in place.

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
  IDs), false positives. trufflehog's one unverified hit on the codex-cli
  JS-runtime rollout is the same class: an `app-<hex>@openai-curated-remote`
  entry from codex's built-in app catalogue in the system prompt.
- Targeted regex sweeps for project-specific personal identifiers.
- JSON / JSONL parse validation on every file.
