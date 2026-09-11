---
type: Finding
title: Per-sync watermarks must never come from Dataset::versions() on a remote store
description: The versions() row-version to commit-timestamp join costs 79s warm / 133s cold on S3 because the version list is a per-manifest fetch storm; a messages-based key is 0.5-0.6s warm, 25-160x faster.
tags: [s3, sync, performance, lance, object-store]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: sync-copy-plan
    resource: ../../plans/2606-17-sync-copy-durability-and-perf.md
    title: "Sync/copy durability and remote performance overhaul (long form)"
  - id: oracle-bench
    resource: "cargo bench --bench sync_oracle_bench -- --url <store>, run against the operator's real Hetzner S3 corpus"
    title: sync_oracle_bench against the production store
---

# Per-sync watermarks must never come from Dataset::versions() on a remote store

The per-session staleness oracle dominates remote sync time, so which key it
derives from is the single biggest lever on how long a sync takes.

## Measured

- `Store::session_last_ingested_at`, which joins `Dataset::versions()` row
  versions to commit timestamps, costs **79 s warm / 133 s cold on S3**. The
  version list is a microsecond-scale local metadata read, but over a network
  it becomes a per-manifest-object fetch storm.[^oracle-bench]
- A messages-based key (`COUNT(*)` / `MAX(timestamp)` / last-id
  `GROUP BY session_id`) costs **0.5-0.6 s warm / 3-5 s cold** - 25-160x
  faster for the same decision.[^oracle-bench]

## Consequence

Never derive a per-sync watermark from `versions()` on a remote store. This is
the same root cause as [never tuning the object store's request-rate
limiter](../decisions/object-store-request-rate-policy.md): on an object store, latency is
round-trip-bound, so the fix is always to issue fewer requests.

Source-side change detection (mtime stat versus JSONL last-record tail-peek) is
a different matter and needs no S3 measurement: adapter files never live on the
store, so that half is always local filesystem work, backend-independent at
roughly 150 ms warm to tail-peek a ~9.4k-file corpus. Only the store-side
oracle is backend-sensitive.[^sync-copy-plan]

## Invalidation

This would change if Lance gained a manifest-list read that did not scale with
accumulated version history, or if commit timestamps became available without
per-manifest fetches.

[^sync-copy-plan]: `docs/plans/2606-17-sync-copy-durability-and-perf.md`
[^oracle-bench]: `sync_oracle_bench` against the operator's real S3 corpus
