---
description: Register pond as an MCP server for Claude Code, Codex, OpenCode, and other clients, exposing pond_search, pond_get_session, pond_get_message, and pond_sql over your full cross-client session history.
---

# Connect your agents

pond is an MCP server. `pond init` registers it for Claude Code and installs the bundled pond skill into Claude Code's user skills dir; for Codex it prints the exact command to run instead (pond never edits another tool's config behind your back). It may already have done this for you, so check with `claude mcp list` before adding by hand. To add the server manually:

```sh
claude mcp add -s user pond -- pond mcp   # Claude Code
codex mcp add pond -- pond mcp            # Codex
```

Clients load MCP servers at startup, so restart the client after registering - the pond tools appear on the next session.

Other clients: use `pond mcp` (alias for `pond serve --transport stdio`) as the server command. The skill installs by hand too: `mkdir -p ~/.claude/skills/pond && pond skill > ~/.claude/skills/pond/SKILL.md`. PowerShell needs a different shape - a bare `>` in Windows PowerShell 5.1 writes UTF-16 and `Out-File -Encoding utf8` prepends a BOM, either of which can corrupt the file:

```powershell
New-Item -ItemType Directory -Force ~\.claude\skills\pond | Out-Null
[IO.File]::WriteAllText("$HOME\.claude\skills\pond\SKILL.md", (pond skill | Out-String))
```

:::info[Windows]
The client spawns the server through `CreateProcess`, which searches `PATH` and appends `.exe` - the bare `pond` above works once the install's shim dir is on the client's `PATH` (restart the client after a fresh install so it picks the new `PATH` up). If it still doesn't resolve, register the native Windows path instead (`C:\Users\<you>\scoop\shims\pond.exe`, quoted if it contains spaces; `where.exe pond` prints it - `which pond` in Git Bash prints the wrong, `/c/...`, form) - never a Git Bash `/c/...` path, which registers cleanly and then never spawns.
:::

## Tools the agent gets

- `pond_search` - find relevant messages across all sessions (full-text by default, semantic when enabled)
- `pond_get_session` - read, analyze, or summarize a whole session
- `pond_get_message` - expand one message with its full tool bodies
- `pond_sql` - the escape hatch: read-only SQL over the corpus

## Agent setup prompt

Paste into your agent:

```text
Set up pond as my session memory.

1. Install it for my OS - https://pond.locker/get-started/install
   (macOS/Linux: brew install tenequm/tap/pond). Never build from
   source. Ask me before bootstrapping Scoop: it changes my
   execution policy.
2. Run pond init -y, then pond sync, then pond status. The first
   sync reads my whole history and can take minutes - Ctrl-C is
   safe, it resumes. Not on your PATH yet? Call pond by full path
   rather than waiting for a restart.
3. Run pond schedule start to keep it current - unless this shell
   is elevated, in which case ask me to run it.
4. Confirm the MCP registration in this client (Claude Code:
   claude mcp list; Codex: codex mcp list). Listed but failing is
   usually just my stale PATH - do not re-register it, tell me.
5. Tell me to restart this client, then use pond_search,
   pond_get_session and pond_get_message to answer questions
   about my past work.
```
