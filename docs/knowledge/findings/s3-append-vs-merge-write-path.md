---
type: Finding
title: Absent rows must append, never merge-insert, on a remote store
description: Appending absent sessions took 13.8 min and 1 commit per table; routing the same rows through merge-insert took 75.7 min and 354 commits - 5.47x slower, because merge over S3 is commit-latency-bound.
tags: [s3, write-path, performance, lance, object-store]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T14:49:41Z" }
sources:
  - id: sync-copy-plan
    resource: ../../plans/2606-17-sync-copy-durability-and-perf.md
    title: "Sync/copy durability and remote performance overhaul (long form)"
  - id: write-bench
    resource: "cargo bench --bench write_bench -- --source-url <store> --dest-url <s3> --only append|merge, full real corpus (11,185 sessions / ~276k messages), clean cold each"
    title: write_bench append vs merge, local source to S3 scratch
---

# Absent rows must append, never merge-insert, on a remote store

## Measured

Full real corpus (11,185 sessions / ~276k messages), local source to S3
scratch, clean cold for each arm:[^write-bench]

| Path | Wall clock | Commits | Objects left |
|---|---|---|---|
| Append absent sessions | 13.8 min | 1 per table | 62 |
| Same rows via merge-insert | 75.7 min | 354 | 2,685 |

That is **5.47x slower** for an identical result. The two arms fail for
different reasons: merge over S3 is **commit-latency-bound** (one commit per
chunk is one round trip), while append is bandwidth-bound. The S3 cost law is
roughly one second per commit, flat from 1 to 512 rows, so commit count is the
thing to minimize.

## Consequence

Absent rows cannot collide, so they MUST append. Any write-path unification
keeps a single shared write seam - that is what stops bespoke write paths
reappearing - but the seam MUST expose append-for-absent as a first-class mode
rather than collapsing everything into `merge_insert`.[^sync-copy-plan]
`write_bench --only append|merge` is the regression guard.

The spec carries the rule (`spec.md#session-durable-copy`); this concept
carries why. Same root cause as [the request-rate
policy](../decisions/object-store-request-rate-policy.md) and [the sync
oracle](s3-sync-change-detection-oracle.md): issue fewer round trips.

## Invalidation

This would change if Lance gained a batched multi-chunk commit that amortized
the per-commit round trip, making merge cost proportional to bytes rather than
to chunk count.

[^sync-copy-plan]: `docs/plans/2606-17-sync-copy-durability-and-perf.md`
[^write-bench]: `write_bench --only append|merge`, full real corpus, clean cold each
