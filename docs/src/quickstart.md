# pond

Lossless storage and hybrid search for AI agent sessions, across every agentic client.

pond keeps every AI conversation you've ever had intact and searchable, and lets you continue any of them in any supported tool. One Rust binary that ingests sessions from registered agentic-client adapters into a canonical Session / Message / Part interlingua, stores them in Lance on object storage, and serves hybrid search over them via HTTP+JSON and MCP. Every adapter is a bidirectional codec, so any session restores into any client - not only the one that made it.

Current automatically synced agent clients:
- Claude Code CLI
- Codex CLI
- opencode CLI
- pi-coding-agent CLI

## Install

Linux and macOS are supported; Windows is not in v1 scope.

```sh
brew install tenequm/tap/pond                       # Homebrew
nix profile add github:tenequm/nur-packages#pond    # Nix
cargo install pond-db                               # crates.io (installs the `pond` command)
```

On macOS the Metal backend is selected automatically; on other systems the CPU fallback runs without extra features.

## Quickstart

1. Ingest your local sessions:

   ```sh
   pond sync
   ```

2. Add pond as an MCP server in your agent client:

   ```sh
   claude mcp add -s user pond -- pond mcp   # Claude Code
   codex mcp add pond -- pond mcp            # Codex
   ```

3. Now just ask your agent - it searches your history through pond for you:

   - "search my past sessions for how we fixed the OCC retry race"
   - "what did we decide about the storage substrate, and why?"
   - "pick up where we left off on the tokenizer experiment"
   - "find the exact command from when we set up that config"

pond runs hybrid search across every session from every client - including sessions made in a different tool than the one you're asking in. Re-run `pond sync` to pick up new sessions.
