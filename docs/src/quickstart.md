# Quickstart

Import sessions from local sources, embed them, update indexes, and search:

```sh
pond sync
pond search "how did we wire up the OCC retry loop"
```

Add pond as an MCP server (pick your client):

```sh
claude mcp add -s user pond -- pond mcp   # Claude Code
codex mcp add pond -- pond mcp            # Codex
```

Run a server:

```sh
pond serve                         # HTTP on 127.0.0.1:9797
pond serve --transport stdio       # MCP over stdio
pond mcp                           # alias for stdio MCP
```

Fetch a single session or message, or move a whole corpus:

```sh
pond get --session-id <id>
pond export -o snapshot.pond
pond import snapshot.pond
```

`pond status` reports row counts, storage size, embedding coverage, and index health.
