# Session samples

Curated, anonymized (or, where a real capture is infeasible, fully synthetic)
session samples from 12 agentic-client platforms. These files
ground pond's canonical-type design (see `docs/spec.md`) and
serve as the test fixtures for the v1 adapter implementations (see
`docs/spec.md#adapters`).

Snapshot date: 2026-05-13 (claude_code subagent sample added 2026-05-20;
claude_code nested workflow-subagent sample added 2026-06-04; opencode
`opencode.db` SQLite fixture generated 2026-07-14 from opencode 1.17.15;
synthetic hermes `state.db` fixtures generated 2026-07-23; letta-code
transcripts captured 2026-08-24 from letta-code 0.30.30).

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
  claude_ai_export/          claude.ai data export (synthetic conversations.json)
  claude_code/               Claude Code CLI
  claude_desktop_app/        Claude Desktop (macOS), Cowork / local-agent-mode
  claude_managed_agents/     Anthropic API Managed Agents (playground export)
  codex_cli/                 OpenAI Codex CLI
  hermes/                    Hermes Agent runtime (single SQLite state.db per profile)
  letta-code/                letta-code (`letta` CLI) client-side transcripts
  nanoclaw/                  nanoclaw runtime (Claude Code Agent SDK in containers)
  oh-my-pi/                  oh-my-pi (`omp`), a pi fork with its own sessions root
  openclaw/                  openclaw runtime
  opencode/                  opencode CLI
  pi-coding-agent/           pi-coding-agent CLI
```

Each platform subdir mirrors that platform's native on-disk path layout so a
adapter can be tested by pointing its discovery code directly at the
sample tree.

## Per-platform notes

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
  originator, models `gpt-5` and `gpt-5-codex`.

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
  IDs), false positives.
- Targeted regex sweeps for project-specific personal identifiers.
- JSON / JSONL parse validation on every file.
