---
type: Finding
title: OpenClaw integration architecture and session storage findings
description: OpenClaw stores sessions in per-agent SQLite trees and zstd archives with short retention, positioning pond as a durable cross-harness archive rather than an in-gateway memory competitor.
tags: [openclaw, adapter, integration, storage, sqlite]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T14:49:41Z" }
stale_after: "2026-12-11T00:00:00Z"
sources:
  - id: openclaw-research
    resource: ../../researches/2607-17-openclaw-integration-research.md
    title: pond x OpenClaw integration research snapshot (2026-07-17)
---

# OpenClaw integration architecture and session storage findings

Everything below is upstream format archaeology taken from one snapshot dated
2026-07-17. It is knowledge, not state - it rots on an OpenClaw release rather
than on pond's next commit - so it carries a `stale_after` instead of a promise
of currency. Recheck against the upstream writer before relying on a shape.

## Headline claims

- OpenClaw stores session state in per-agent SQLite databases (`~/.openclaw/agents/<agentId>/agent/openclaw-agent.sqlite`) where transcripts form an append-only tree via `id` and `parentId` pointers.[^openclaw-research]
- Retention policies enforce strict local limits (`pruneAfter: 30d`, `maxEntries: 500`, `maxDiskBytes: 2gb`), meaning old sessions are evicted even from zstd archives; pond serves as the durable archive.[^openclaw-research]
- As of the 2026-07-17 snapshot, upstream had PR #100140 open to consolidate in-gateway private session recall into a built-in feature. That is upstream's to change and its state has not been rechecked here; the durable consequence either way is that pond's position is the tier beneath OpenClaw - a cross-harness archive that outlives the retention window - rather than a competitor inside `memory_search`.[^openclaw-research]
- Transcript hygiene preserves clean user prompt bodies while inter-session routed turns carry `message.provenance.kind = "inter_session"`, mapping directly to pond's provenance model.[^openclaw-research]
- The plugin system (`definePluginEntry`) is the sanctioned deep integration surface for tools, hooks, and lifecycle events without requiring changes to core write paths.[^openclaw-research]
- Subagents are allocated distinct `session_id` identifiers with `spawned_by` tracking, mapping cleanly to pond child sessions via `parent_session_id`.[^openclaw-research]
- Tree-to-linear mapping preserves `parentId` in message options while flattening in append order; stable ordering must use timestamp and tree position rather than mutable `seq` counters.[^openclaw-research]
- Ingest skips `.deleted.` archives by default and uses `pond erase` reconciliation rather than exposing destructive operations over MCP.[^openclaw-research]

## Invalidation

These findings would be invalidated if OpenClaw replaces its per-agent SQLite storage with a flat linear log, removes the 2 GB retention cap to provide long-term durability natively, or closes its plugin extension seams to external tools.

## Source

Full research report: [docs/researches/2607-17-openclaw-integration-research.md](../../researches/2607-17-openclaw-integration-research.md).

[^openclaw-research]: `docs/researches/2607-17-openclaw-integration-research.md`
