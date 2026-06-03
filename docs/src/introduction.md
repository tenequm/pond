# pond

Lossless storage and hybrid search for AI agent sessions, across every agentic client.

pond keeps every AI conversation you've ever had intact and searchable, and lets you continue any of them in any supported tool. Your history, your search, your sessions - independent of the agent vendor that made them.

One Rust binary that ingests sessions from any agentic client (Claude Code, Codex, and more on the roadmap) into a canonical Session / Message / Part interlingua, stores them in Lance on object storage, and serves hybrid search over them via HTTP+JSON and MCP.

Every adapter is a bidirectional codec, so any session restores into any client - not only the one that made it.

> Status: pre-v1. Schemas, wire shapes, and config keys are subject to breaking change until v1.
