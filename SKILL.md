---
name: pond
description: Recall past AI agent sessions (Claude Code, Codex, opencode, and more) - lossless storage with semantic and full-text search, served over MCP.
---

# pond

pond stores every session your AI agent clients produce - losslessly, in
Lance datasets on a local disk or any S3-compatible bucket - and makes them
searchable: semantic (vector) and full-text (BM25) retrieval - the agent picks
the arm per query - plus full-transcript fetch and read-only SQL.

This file is a pointer, not a manual. pond is MCP-native: once registered,
the MCP tool descriptions, resources (`schema://pond`, `schema://pond-sql`,
`stats://pond`), and `pond --help` carry everything an agent needs.

## Setup

Install (any one):

    brew install tenequm/tap/pond
    cargo binstall pond-db
    nix profile install github:tenequm/pond-nix#pond

Initialize once (idempotent - re-run any time to repair or update):

    pond init

Register the MCP server in your client:

    claude mcp add -s user pond -- pond mcp

## Surfaces

- MCP tools: `pond_search`, `pond_get`, `pond_sql_query`.
- CLI: run `pond --help`; every command's `--help` carries copy-pasteable
  examples (`pond sync`, `pond search`, `pond status`, `pond schedule`,
  `pond copy`, ...).
- Docs: https://pond.locker/
