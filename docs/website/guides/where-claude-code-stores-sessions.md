---
title: Where Claude Code and Codex store your sessions
description: Where Claude Code conversations are stored on disk, where Codex and 12 other clients keep theirs, the JSONL transcript format, and how to read it with jq.
---

# Where are Claude Code conversations stored? [In ~/.claude/projects, as JSONL transcripts - plus where Codex and every other client keeps its sessions]

Claude Code stores every conversation on your own disk as a JSONL transcript at `~/.claude/projects/<project>/<session-id>.jsonl`: one file per session, one JSON object per line. `<project>` is the working directory path with every non-alphanumeric character replaced by `-`, and on Windows `~/.claude` is `%USERPROFILE%\.claude`. Codex CLI keeps its sessions the same way, at `~/.codex/sessions/YYYY/MM/DD/rollout-<timestamp>-<session-id>.jsonl`. The files are plain text you can read with `jq`, and Claude Code deletes its own after 30 days unless you [change the retention setting](/docs/guides/claude-code-session-retention).

## Claude Code session history on disk

Everything Claude Code records about a session sits under `~/.claude` (Claude Code docs, [Manage sessions](https://code.claude.com/docs/en/sessions#where-transcripts-are-stored) and [Explore the .claude directory](https://code.claude.com/docs/en/claude-directory#application-data), checked 2026-11-24):

| Path under `~/.claude/` | What it holds |
|---|---|
| `projects/<project>/<session-id>.jsonl` | the transcript: messages, tool calls and tool results |
| `projects/<project>/<session-id>/subagents/` | subagent transcripts, in the same format |
| `projects/<project>/<session-id>/tool-results/` | large tool outputs spilled to separate files |
| `projects/<project>/memory/` | auto memory - Claude's notes to itself, not transcripts |
| `history.jsonl` | the prompts you typed, for up-arrow recall |

A project's directory name is its path with the separators turned into dashes: sessions started in `/home/you/work/acme-api` land in `~/.claude/projects/-home-you-work-acme-api/`. Setting `CLAUDE_CONFIG_DIR` moves the whole tree somewhere else. To reopen a session rather than read its file, see [Resume a session](/docs/guides/resume-session-in-another-client).

## The Claude Code transcript format

Each line of a transcript is one JSON object with a `type`. Anthropic documents the lines as messages, tool uses and results, records of the requests Claude Code sent the model, and metadata entries, and warns that the entry format is internal and changes between versions. What Claude Code 2.1.331 writes (read off real files, 2026-11-24):

- `user` and `assistant` lines are the conversation. Each carries `uuid`, `parentUuid` (the line before it, which chains the session together), `sessionId`, `timestamp`, `cwd` and a `message` with `role` and `content`.
- `content` is a plain string or an array of blocks: `text`, `thinking` and `tool_use` on assistant lines, `tool_result` on user lines. A tool's output comes back as a `user` line, not as a separate role.
- Every other `type` (`system`, `attachment`, `ai-title`, `last-prompt` and more) is metadata around the conversation.

Three `jq` one-liners cover most of what people open these files for:

```sh
# what kinds of lines a transcript holds
jq -r '.type' session.jsonl | sort | uniq -c | sort -rn

# the conversation as plain text, without the tool traffic
jq -r 'select(.type == "user" or .type == "assistant") | .message.content | if type == "string" then . else (.[] | select(.type == "text") | .text) end' session.jsonl

# every tool the session called, by count
jq -r 'select(.type == "assistant") | .message.content[]? | select(.type == "tool_use") | .name' session.jsonl | sort | uniq -c | sort -rn
```

For a rendered transcript, `/export` inside a session copies or saves one. For scripts, Anthropic recommends `claude -p --resume <session-id> --output-format json` over parsing the files, because the line format is not a stable contract.

## Where Codex and the other clients keep sessions

These are the default locations pond's adapters read, one row per client. The Claude Code and Codex rows are confirmed against each vendor's docs and source on 2026-11-24; the rest are the paths in pond's adapter code.

| Client | Sessions on disk (macOS and Linux) | Format |
|---|---|---|
| Claude Code | `~/.claude/projects/<project>/<session-id>.jsonl` | JSONL, one file per session |
| Claude Desktop app (Cowork sessions) | `~/Library/Application Support/Claude/local-agent-mode-sessions/` (macOS) | `audit.jsonl` per session plus a `.json` metadata file |
| Claude.ai | no local files | `conversations.json` in the [official data export](/docs/guides/import-claude-ai) |
| Codex CLI | `~/.codex/sessions/YYYY/MM/DD/rollout-<timestamp>-<session-id>.jsonl` | JSONL, one file per session |
| OpenCode | `~/.local/share/opencode/` | SQLite (`opencode*.db`); a JSON file tree under `storage/` before v1.2.0 |
| OpenClaw | `~/.openclaw/agents/<agent>/` | per-agent SQLite; JSONL files through 2026.7.1 |
| nanoclaw | `<install>/data/v2-sessions/` | Claude Code JSONL, written inside the agent container |
| Hermes | `~/.hermes/state.db` | SQLite |
| Pi | `~/.pi/agent/sessions/<project-slug>/` | JSONL, one file per session |
| oh-my-pi | `~/.omp/agent/sessions/<bucket>/` | JSONL, one file per session |
| Letta Code | `~/.letta/transcripts/<agent>/<conversation>/transcript.jsonl` | JSONL |
| Grok | `~/.grok/sessions/<encoded-cwd>/<session-id>/updates.jsonl` | JSONL |
| Antigravity | `~/.gemini/antigravity-cli/conversations/<id>.db` (and `antigravity-acp/`) | SQLite, one database per conversation |
| Devin CLI | `~/.local/share/devin/cli/sessions.db` | SQLite |

On Windows the same paths sit under `%USERPROFILE%` (`~/.codex/sessions` is `%USERPROFILE%\.codex\sessions`), with two exceptions: the Claude Desktop app keeps its sessions under `%APPDATA%\Claude\local-agent-mode-sessions`, and Devin under `%APPDATA%\devin\cli`. Several clients honor an override that moves their directory: `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `OPENCLAW_STATE_DIR`, `HERMES_HOME`. For Codex in depth - the rollout format, its own history commands - see [Codex session history](/docs/guides/codex-session-history).

## One searchable copy of all of it

Fourteen clients, some writing JSONL and some SQLite, and one retention window you did not choose: reading a single file is easy, finding the session where you fixed something three months ago is not. `grep` works across the JSONL clients while the files exist, and stops at the SQLite ones and at anything already deleted.

pond reads every location in the table, converts each session into one common model, and stores the result in a directory or a bucket you own. It never modifies or deletes the source files.

```sh
pond init     # offers the clients it finds on this machine, registers the MCP server
pond sync     # ingest and index
pond search "websocket reconnect backoff"
```

One search then covers every client, and a session stays in the store after the client that wrote it deletes the file. What pond cannot do: read a session that was deleted before its first sync, or a client it has no adapter for.

## Next

- [Install](/docs/get-started/install) - Homebrew, Nix, Scoop, Cargo
- [Claude Code deletes sessions after 30 days](/docs/guides/claude-code-session-retention) - what `cleanupPeriodDays` removes
- [Codex session history](/docs/guides/codex-session-history) - the rollout files and how to search them
- [SQL over your agent sessions](/docs/guides/sql-analytics) - the same transcripts as three tables
