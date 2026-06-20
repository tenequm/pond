---
name: pond
description: Recall past AI agent sessions (Claude Code, Codex, opencode, and more) - lossless storage with semantic and full-text search, served over MCP.
---

# pond

pond is your memory across every AI coding session you have run - stored
losslessly, searchable. If a task needs context you lack, pond likely has it:
recall first, then answer. Before you say "I don't know" or re-derive something
that sounds prior, search pond.

## Which tool

- Past work, by meaning ("what did we decide", "last week", "that other repo")
  -> `pond_search` (`mode=vector` default; `mode=fts` for exact words/symbols).
- A known session or message -> `pond_get` (transcript, or one message in context).
- Counts, filters, trends across sessions -> `pond_sql_query` (read-only SQL over
  `sessions` / `messages` / `parts`).

Params and response shapes live in each tool's MCP description and the
`schema://pond`, `schema://pond-sql`, `stats://pond` resources - read them at
call time, don't guess.

## Setup

`brew install tenequm/tap/pond` (or `cargo binstall pond-db`, or `nix profile
install github:tenequm/pond-nix#pond`), then `pond init`, then `claude mcp add -s
user pond -- pond mcp`. Keep current with `pond sync`; `pond --help` for the rest.
Docs: https://pond.locker/
