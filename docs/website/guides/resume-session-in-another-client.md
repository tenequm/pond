---
title: Resume a Claude Code or Codex session in another client
description: How claude --resume and codex resume reopen a session, and how to continue a Claude Code session in Codex, or the reverse, with Codex /import or pond resume.
---

# Resume a session in a different client [claude --resume and codex resume reopen their own sessions; moving one across takes a conversion]

To resume a session in the client that made it, use that client. In Claude Code, `claude --continue` reopens the most recent session in the current directory and `claude --resume` opens a picker or takes a session id. In Codex, `codex resume` opens the picker, `codex resume --last` takes the most recent session and `codex resume <session-id>` a specific one. Neither client reads the other's session files, so continuing a Claude Code session in Codex, or a Codex session in Claude Code, needs the session converted first: Codex's own `/import` does it for recent Claude Code chats, and `pond resume` does it for any session pond has stored, in either direction.

## Resume in the same client

Flags below are from Claude Code 2.1.331 and Codex CLI 0.171.0 (`--help`, plus [Manage sessions](https://code.claude.com/docs/en/sessions) and the [Codex command reference](https://learn.chatgpt.com/docs/developer-commands?surface=cli), checked 2026-11-24).

| Claude Code | What it does |
|---|---|
| `claude --continue` (`-c`) | reopens the most recent session in the current directory |
| `claude --resume` (`-r`) | opens the session picker |
| `claude --resume <session-id>` | resumes that session; the id is looked up in the current project and its worktrees, then in every other project on the machine |
| `claude --resume <name>` | resumes a session you named with `-n` or `/rename` |
| `claude --resume <path>.jsonl` | resumes the session stored in that transcript file (absolute path) |
| `--fork-session` | with `--resume` or `--continue`: continue in a copy under a new session id |
| `/resume` | switches to another session from inside one |

| Codex CLI | What it does |
|---|---|
| `codex resume` | opens the picker, scoped to the current directory; `--all` shows every directory |
| `codex resume --last` | resumes the most recent session from the current directory; add `--all` to ignore the directory |
| `codex resume <session-id>` | resumes that session; a session name works too |
| `--include-non-interactive` | also lists sessions made by `codex exec` |
| `codex fork` | same choices, but continues in a new session |
| `codex exec resume` | the non-interactive form, with `--last` or a session id |
| `/resume` | the picker from inside a session, scoped to directory, repository or all |

If the directory you resume from differs from the one the Codex session was saved in, Codex asks which to use.

The native path stops in three places: a Claude Code session older than its [retention window](/docs/guides/claude-code-session-retention) has no file left to resume, a session made on another machine is not on this one, and a session made in a different client is in a format this one does not read.

## What the clients offer for switching

- **Codex `/import`** brings Claude Code or Cursor setup into Codex, and with it recent chats: up to 50 chats from the last 30 days ([Import from another agent](https://learn.chatgpt.com/docs/import), checked 2026-11-24). If the Claude Code session you want in Codex is recent, this is the shortest way, and it also carries your instructions, skills and MCP configuration.
- **Claude Code `/import`** brings configuration from Codex, Gemini CLI or Cursor into Claude Code: instruction files, MCP servers, commands, subagents and skills. It does not import conversations ([Commands](https://code.claude.com/docs/en/commands), checked 2026-11-24).

So there is a vendor path from Claude Code to Codex for the last 30 days, and none in the other direction.

## Move a session with pond resume

pond stores every session in one neutral model, and each client adapter can write that model back out in its own format. `pond resume` does that for one session:

```sh
pond search "websocket reconnect backoff"                      # each hit shows its session id
pond resume <session-id> --to codex-cli --out-dir ~/.codex     # a Claude Code session, as a Codex rollout
pond resume <session-id> --to claude-code --out-dir ~/.claude/projects   # a Codex session, as a Claude Code transcript
```

`--out-dir` is the root the client's own layout is written under, and it defaults to the current directory, so a bare `pond resume` touches nothing a client reads. Restoring into a scratch directory first and reading the file is a reasonable habit. The command prints what it wrote and which fidelity it served:

```text
resumed 0195c2d4-7b1e-7a3c-9f0e-2d6b8a41c7e5 -> claude-code (foreign)
  /home/you/.claude/projects/-home-you-work-acme-api/0195c2d4-7b1e-7a3c-9f0e-2d6b8a41c7e5.jsonl
```

Then open it with the client's own resume. Claude Code accepts the transcript's absolute path (`claude --resume <path>.jsonl`); in Codex, look for the session in `codex resume --all`. Read [What is not proven](#what-is-not-proven) first.

### What carries over

pond decides the fidelity; you do not choose it.

- **Native** - the target is the client the session came from. The stored source records are replayed value for value, subagent sidecar files included. This is the lossless case, and it is how you get back a session the client already deleted.
- **Foreign** - any other target. pond rebuilds a session in the target's own vocabulary, best effort.

In a foreign restore between Claude Code and Codex:

| Carries over | Does not |
|---|---|
| user and assistant text | system messages and harness records (event logs, token counters, turn bookkeeping) |
| reasoning, as the target's reasoning block | model name, token usage and other per-turn metadata |
| tool calls and their results | binary attachments going into Codex (written as a size placeholder) |
| the session id and working directory | anything the target format has no place for |

What is dropped from the restored file is not dropped from pond: the stored session is unchanged, and restoring it natively later still gives you everything.

### Which clients it can restore into

Not all of them. Of the fourteen clients pond ingests, eleven can be a `--to` target and three cannot:

- **Written in the client's own session layout:** `claude-code`, `codex-cli`, `pi-coding-agent`, `letta-code`, `grok-build`, `claude-desktop-app`, `nanoclaw`.
- **Written in an older format:** `openclaw` gets a JSONL transcript in its file-era format. OpenClaw has kept sessions in SQLite since 2026.7.2, so treat that file as an export.
- **Written as an import file:** `opencode` gets one JSON file per session in the shape `opencode import` reads. `claude-ai-export` gets a one-conversation `conversations.json`, which Claude.ai has no way to load.
- **Always foreign:** `hermes` keeps sessions in SQLite, so pond writes a JSONL dump of the rows instead of something Hermes opens.
- **Refused:** `agy`, `devin` and `oh-my-pi` are ingest-only. The command exits `2` and names the alternative (for oh-my-pi, restoring as `pi-coding-agent` into `~/.omp/agent`).

`pi-coding-agent` is always written as pi's v3 format, the one a released pi can open, whatever format the session was captured from.

### Rules it does not bend

- **Children travel with the parent.** A session restores together with its subagent sessions or not at all. A session whose child has children of its own is refused (exit `2`) rather than partly written.
- **Nothing is overwritten.** If a target file exists, nothing is written and the command exits `3`, naming the file: that session is already resumed, open it.
- **A failed write is unwound.** Exit `4` leaves no restored session file behind.

The full list is in [Exit codes](/docs/reference/exit-codes).

### What is not proven

pond's tests check that a Codex session restored as Claude Code, and a Claude Code session restored as Codex, parses back as a session with every user, assistant and tool message in it. They do not launch Claude Code or Codex against the file. A foreign restore is a reconstruction, and the client has the last word on whether it loads; if one refuses a file pond wrote, [open an issue](https://github.com/tenequm/pond/issues) with the pair of clients.

One more thing to know before restoring into a directory pond also syncs: the restored file keeps the original session id. If the client keeps appending to that file, the next `pond sync` stores the new turns under the same session id, still labelled with the client the session started in.

## Next

- [Install](/docs/get-started/install) - Homebrew, Nix, Scoop, Cargo
- [Claude Code deletes sessions after 30 days](/docs/guides/claude-code-session-retention) - why the session you want to resume may be gone
- [Where Claude Code stores sessions](/docs/guides/where-claude-code-stores-sessions) - the files each client writes
- [CLI commands](/docs/reference/cli) - `pond resume` and the rest
