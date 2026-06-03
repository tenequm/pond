# Quickstart

1. Install pond (see [Install](./install.md)).

2. Ingest your local sessions:

   ```sh
   pond sync
   ```

3. Add pond as an MCP server in your agent client:

   ```sh
   claude mcp add -s user pond -- pond mcp   # Claude Code
   codex mcp add pond -- pond mcp            # Codex
   ```

4. Now just ask your agent - it searches your history through pond for you:

   - "search my past sessions for how we fixed the OCC retry race"
   - "what did we decide about the storage substrate, and why?"
   - "pick up where we left off on the tokenizer experiment"
   - "find the exact command from when we set up that config"

pond runs hybrid search across every session from every client - including sessions made in a different tool than the one you're asking in. Re-run `pond sync` to pick up new sessions.
