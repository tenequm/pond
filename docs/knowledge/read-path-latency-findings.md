---
type: Finding
title: Read path latency analysis and bottleneck attribution
description: 77% of MCP tool calls exceeded 5s due to unindexed message ID resolution full-scans and rowmap bypasses in the get path issuing over 10,000 S3 GET requests per query.
tags: [latency, performance, s3, mcp, read-path]
status: stable
generated: { by: "acpx/gemini-3.7-flash-medium", at: "2026-09-11T11:01:53Z" }
sources:
  - id: read-path-latency
    resource: docs/researches/2608-12-read-path-where-time-goes.md
    title: "Read path: where the time actually goes (measured, 2026-08-12)"
---

# Read path latency analysis and bottleneck attribution

## Headline claims

- Across 2,281 MCP calls over 30 days on a 2.64M-message S3 store, 77% of calls exceeded 5s, with an overall p50 latency of 17s and p90 of 92s.[^read-path-latency]
- The get path was the primary bottleneck: zero out of 90 `pond_get_message` calls and zero out of 64 `pond_get_session` calls finished in under 5s (p50 of 84s and 29s respectively).[^read-path-latency]
- A single warm `pond_get_message` issued ~10,900 S3 GET requests (~42 MiB) with 97% targeting table data files, representing 35x more requests than an entire search query (~291 GETs).[^read-path-latency]
- Message ID to session resolution without an index on `messages.id` caused ~93s cold overhead on S3 by full-scanning 2.6M rows (versus 1.1s locally).[^read-path-latency]
- Paging a 25-message session cold took 74.1s remotely versus 0.7s locally (100x slower) because the get path bypassed the resident mmap rowmap and scanned whole sessions via small range-GETs.[^read-path-latency]
- Concurrent fan-out of 8 parallel subagent gets queued ~87,000 S3 requests through a single client, causing every call to hit the 300s timeout.[^read-path-latency]
- Cold process spawning per session and 60s idle eviction of the embedding model produced a 10s search p50 despite warm search arms running in 0.2-1.1s.[^read-path-latency]
- Lance lacks a data-page cache, so repeated data reads from remote storage incur full round-trip latencies on every invocation.[^read-path-latency]

## Invalidation

These findings would be invalidated if get operations are updated to resolve message IDs and conversational text directly from the resident mmap rowmap, or if Lance introduces an in-memory data-page cache for remote object storage.

## Source

Full research report: [docs/researches/2608-12-read-path-where-time-goes.md](../researches/2608-12-read-path-where-time-goes.md).

[^read-path-latency]: `docs/researches/2608-12-read-path-where-time-goes.md`
