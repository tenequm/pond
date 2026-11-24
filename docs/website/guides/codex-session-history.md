---
title: "Codex session history: where it lives and how to search it"
description: Where Codex CLI keeps session history (~/.codex/sessions rollout files), how codex resume finds old sessions, and how to search them next to Claude Code's.
---

# Codex session history [Where Codex CLI keeps its sessions, what its own history commands do, and how to search them next to Claude Code's]

Codex CLI keeps each session as a JSONL "rollout" file under `~/.codex/sessions/YYYY/MM/DD/`, named `rollout-<timestamp>-<session-id>.jsonl`, and the prompts you typed in `~/.codex/history.jsonl`. `codex resume` is how you get back to one: it opens a picker of sessions from the current directory, `--all` widens it to every directory, and `--last` reopens the newest. Codex's docs list no automatic cleanup of these files; a session stays until you run `codex delete`.

For the same facts about Claude Code and twelve other clients, see [Where Claude Code and Codex store your sessions](/docs/guides/where-claude-code-stores-sessions). This page goes further into Codex.

## Where Codex sessions live

```text
~/.codex/                                   # or $CODEX_HOME
  sessions/
    2026/11/24/
      rollout-2026-11-24T09-15-02-<session-id>.jsonl
  archived_sessions/                        # sessions you archived
  history.jsonl                             # the prompts you typed
  config.toml
```

The layout comes from Codex's source ([`codex-rs/rollout`](https://github.com/openai/codex/tree/main/codex-rs/rollout), checked 2026-11-24): the date directories and the file name use your local time, and the id in the name is the session id that `codex resume` takes. Codex's docs describe the directory itself: all local state sits under `CODEX_HOME`, which defaults to `~/.codex` ([Config and state locations](https://learn.chatgpt.com/docs/config-file/config-advanced#config-and-state-locations)).

Three things that are easy to mix up:

- **`history.jsonl` is not the transcripts.** It holds one line per prompt you typed (`session_id`, `ts`, `text`). `history.persistence = "none"` and `history.max_bytes` in `config.toml` control that file, not the rollout files.
- **`codex exec --ephemeral`** runs without writing session files at all.
- **SQLite files next to `sessions/`** are Codex's own: runtime state and thread history. Codex 0.171.0 also ships `codex migrate-rollouts`, which moves older sessions to what it calls paginated thread history. How much of a session ends up only in SQLite is not something this page has verified. What is verified: 0.171.0 still writes a rollout file per session (seen on a machine running it, 2026-11-24).

## What a rollout file looks like

Every line is an envelope, `{"timestamp": ..., "type": ..., "payload": ...}`. The first line is `session_meta`, with the session `id` and the `cwd` it started in. `response_item` lines are the model traffic: `message`, `reasoning`, `function_call` and `function_call_output`, `custom_tool_call` and `custom_tool_call_output`. The rest (`event_msg`, `turn_context` and others) is bookkeeping.

```sh
# which session is this, and where was it started
jq -c 'select(.type == "session_meta") | .payload | {id, cwd}' rollout-2026-11-24T09-15-02-<session-id>.jsonl

# the conversation text
jq -r 'select(.type == "response_item" and .payload.type == "message" and (.payload.role == "user" or .payload.role == "assistant")) | .payload.role + ": " + ([.payload.content[]? | .text // empty] | join(" "))' rollout-2026-11-24T09-15-02-<session-id>.jsonl

# find the rollout files that mention a phrase
grep -rl "reconnect backoff" ~/.codex/sessions
```

One quirk worth knowing before you count tool calls by hand: since Codex 0.147, every tool call is recorded as a `custom_tool_call` named `exec` whose input is a JavaScript snippet, so the real tool name is inside the snippet rather than in a field.

## Codex's own history commands

From Codex CLI 0.171.0 (`--help`) and the [command reference](https://learn.chatgpt.com/docs/developer-commands?surface=cli), checked 2026-11-24:

| Command | What it does |
|---|---|
| `codex resume` | picker of sessions from the current directory; `--all` for every directory |
| `codex resume --last` | reopens the most recent session without the picker |
| `codex resume <session-id>` | reopens that session; a session name works too |
| `codex fork` | the same choices, continuing in a new session |
| `codex archive <session>` / `codex unarchive <session>` | hides a session from the lists, or brings it back, without deleting the transcript |
| `codex delete <session>` | permanently deletes the transcript |
| `/resume` | the picker inside a session, scoped to `Cwd`, `Repo` or `All` |

If an old session is missing from `codex resume`, it is usually one of four things: it was started in another directory (`--all`), it was a `codex exec` run (`--include-non-interactive`), it was archived (`codex unarchive`), or it was made under a different `CODEX_HOME`.

That covers getting back to a session you can identify. What Codex does not have is one search over the text of everything you have done, across Codex and the other clients you use, on every machine.

## Search Codex sessions next to Claude Code's

pond reads `~/.codex/sessions` alongside `~/.claude/projects` and the [other clients it supports](/docs/guides/where-claude-code-stores-sessions#where-codex-and-the-other-clients-keep-sessions), and stores all of it in one place:

```sh
pond init                     # offers every client it finds, Codex included
pond sync                     # ingest and index
pond adapters list            # codex-cli should read "yes"
pond search "reconnect backoff"
```

One query covers every client, and each hit says which one it came from (real output format, invented values):

```text
pond search: 3 matching messages (41822 searchable in scope), showing 2 hits from 2 sessions. Subagent sessions excluded; reach them via `pond sql` (parent_session_id).
key: session rules group hits by session, ordered by best hit; within a session, messages are newest-first. "--- [n] score | role | time | message_id | project | agent | session ---" delimits each hit

--- session [1] best 1.00 | 2/214 matched | /home/you/work/acme-api | codex-cli | 0195c2d4-7b1e-7a3c-9f0e-2d6b8a41c7e5 ---

--- [1] 1.00 | assistant | 2026-11-12 09:41:07Z | <message-id> | /home/you/work/acme-api | codex-cli | 0195c2d4-7b1e-7a3c-9f0e-2d6b8a41c7e5 ---
The reconnect loop never resets its backoff after a successful handshake ...

--- session [2] best 0.87 | 1/1318 matched | /home/you/work/acme-api | claude-code | 7c4e1b9a-3f52-4d8e-a6c1-9b0d2e7f5a13 ---

--- [2] 0.87 | assistant | 2026-09-03 14:22:31Z | <message-id> | /home/you/work/acme-api | claude-code | 7c4e1b9a-3f52-4d8e-a6c1-9b0d2e7f5a13 ---
Capped the reconnect backoff at 30 seconds and added jitter ...
```

Narrow it with `--project`, `--from-date` and `--to-date`; read a whole hit with `pond get-session <session-id>`. Search is full-text by default (exact whole words) and semantic if you [turn embeddings on](/docs/reference/configuration#embeddings). Over MCP an agent gets the same search as `pond_search`, which also takes a `source_agent` filter, so "search only my Codex sessions" is `source_agent: "codex-cli"`.

To count rather than find, the same sessions are rows in [`pond sql`](/docs/guides/sql-analytics). pond unwraps the `exec` snippet when it ingests, so tool calls carry the real tool name and a failed command is marked as a failure:

```sh
pond sql "SELECT tool_name, COUNT(*) AS calls FROM parts WHERE type = 'tool_call' AND session_id = '<session-id>' GROUP BY tool_name ORDER BY calls DESC"
```

What pond does not cover: sessions that never reach `~/.codex/sessions` on a machine where it runs - Codex Cloud tasks, `--ephemeral` runs - and a session you deleted before its first sync. A session it has already stored stays stored when you later `codex delete` it.

## Next

- [Install](/docs/get-started/install) - Homebrew, Nix, Scoop, Cargo
- [Resume a session in a different client](/docs/guides/resume-session-in-another-client) - `codex resume`, and continuing a Codex session in Claude Code
- [Connect your agents](/docs/get-started/connect-your-agents) - give Codex itself the search, with `codex mcp add pond -- pond mcp`
- [SQL over your agent sessions](/docs/guides/sql-analytics) - tool failures, activity by project, tokens
