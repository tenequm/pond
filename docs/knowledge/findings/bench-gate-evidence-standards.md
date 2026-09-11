---
type: Finding
title: What a bench-gate row does and does not prove
description: A gate row measures only the paths its probes name, one row cannot bracket a small effect on a remote store, and a read baseline is unrecoverable once the new binary has written.
tags: [benchmarking, performance, s3, methodology]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T14:49:41Z" }
sources:
  - id: issue232
    resource: https://github.com/tenequm/pond/issues/232
    title: "The sync change-detection oracle probe measured a function with no production callers"
  - id: baseline
    resource: ../../../packages/pond/benches/docs/bench-gate-baseline.jsonl
    title: The bench-gate baseline row log
  - id: results
    resource: ../../../packages/pond/benches/docs/results.md
    title: The raw measurement log with host provenance
---

# What a bench-gate row does and does not prove

`moon run pond:bench-gate` appends one row per run to the baseline jsonl and
prints the delta against the previous run.[^baseline] The row is evidence only
within the limits below; each limit was learned by a run that produced a number
nobody could use.

## A row proves only what its probes touch

The gate measures the paths its probes name and nothing else, so confirm a
probe actually reads your code before citing a row as evidence. The gate's
`[sync] change-detection oracle` probe spent months timing a function with no
production callers, which left every real sync-oracle change
unmeasured.[^issue232] Either add a probe that reaches your path, or measure
separately and say so in the write-up.

## One row does not bracket a small effect

Two runs 65 minutes apart on identical code moved `search_dated_s` by +129%,
`row_counts_ms` by -59%, and `open_store_ms` by +90%.[^results] Remote-store noise of
that size buries a few hundred milliseconds of real regression. Bracket a small
effect with a targeted A/B over many runs (`hyperfine`, medians - S3 outliers
drag means), each run on its own store path.

Rows are comparable only within the same `store` digest and `write_corpus` tag.
The delta printer warns on store changes and flags rows with no write metrics.

## The old-side read baseline is perishable

Run the pre-change row BEFORE the new binary ever writes to the real store.
Writes mutate store state, so the old-side read baseline is unrecoverable
afterwards except through scratch copies - the lance 8 to 10 upgrade lost its
sync-write baseline exactly this way.

The write metrics carry no such hazard, since they run against fixed synthetic
scratch stores either way. `POND_BIN=/path/to/pond moon run pond:bench-gate`
snapshots a prebuilt binary through the CLI probes, but the cargo-bench fields
land null because those compile HEAD; a full old-side row needs the gate run
from the old commit's checkout.

## Invalidation

These limits are properties of remote object-store measurement and of the
probe roster. The probe-coverage limit lifts for a given path once a probe
demonstrably reads it; the noise limit does not lift.

[^issue232]: [tenequm/pond#232](https://github.com/tenequm/pond/issues/232)
[^baseline]: `packages/pond/benches/docs/bench-gate-baseline.jsonl`
[^results]: `packages/pond/benches/docs/results.md`
