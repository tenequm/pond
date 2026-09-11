---
type: Finding
title: cleanup_old_versions belongs off the per-sync hot path
description: The version-log walk is round-trip-bound and reclaims about one version per run, yet ungated it ran on all three tables every sync; gating it on a per-table interval cut 48 walks to 6 over 16 folds.
tags: [s3, sync, performance, lance, maintenance]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: write-bench-profile
    resource: "cargo bench --bench write_bench -- --profile-optimize <dir> --dest-url <s3> --grown 16, 16 consecutive incremental folds on the operator's S3 store"
    title: write_bench --profile-optimize over 16 folds
---

# cleanup_old_versions belongs off the per-sync hot path

The per-table `cleanup_old_versions` version-log walk is round-trip-bound on
object stores - the same failure mode as [the versions() manifest
storm](s3-sync-change-detection-oracle.md) - and reclaims only about one
version per run. Ungated it ran on all three tables on every sync, which is a
tax a five-minute cron sync should not pay. On the real S3 store it cost ~9 s
per table in one profile, and 2-18 s per table under a faster-growing corpus,
because it scales with accumulated version history.

## The gate

`MaintenancePolicy.cleanup_interval` makes each table clean only when its own
manifest `version` is a multiple of the interval
(`DEFAULT_SYNC_CLEANUP_INTERVAL = 16`). Only `pond sync` opts in; `pond
optimize` and `pond copy` keep interval 1 and clean every run. The gate reads
`dataset.version_id()`, a cheap in-memory manifest field, never the
`versions()` list - reading the list to decide whether to walk the list would
reintroduce the cost it avoids.

## Measured

Over 16 consecutive incremental folds on S3: **6 cleanup walks fired (4 parts,
1 messages, 1 sessions) against 48 ungated (3 tables x 16), about 87% fewer**,
with 10 of 16 syncs doing zero cleanup.[^write-bench-profile]

Read the win precisely. The cadence is per-table and workload-dependent, since
a fast-growing table cleans more often, and finalize wall time is dominated by
compaction and index-append rather than cleanup. So this removes a per-sync
tax - clearest in steady state on an established store with rare compaction -
rather than cutting a fixed amount of wall clock.

Bounded and safe: version 0 is a multiple of every interval, so cleanup always
eventually fires; skipping only defers reclamation, and the next due cleanup
sweeps the backlog. `write_bench --profile-optimize` is the regression guard.

## Invalidation

This would change if version-log walks stopped scaling with accumulated
history, or if reclamation became urgent enough that deferring it cost storage
faster than the round trips cost time.

[^write-bench-profile]: `write_bench --profile-optimize --grown 16` on the operator's S3 store
