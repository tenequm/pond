---
type: Decision
title: Never tune Lance's AIMD request-rate limiter
description: The throttling pond saw on Hetzner was a symptom of issuing too many requests, not of the limiter being too narrow; Lance mislabels transport timeouts as throttle errors, which once seeded a wrong theory.
tags: [s3, object-store, lance, performance, policy]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: lance-aimd
    resource: "Lance cloud store AIMD limiter defaults (initial 2000 -> max 5000 req/s) as observed in the pinned lance version"
    title: Lance AIMD rate limiter defaults
---

# Never tune Lance's AIMD request-rate limiter

Lance wraps every cloud store in an AIMD rate limiter, defaulting to an initial
2000 and a maximum 5000 requests per second.[^lance-aimd] Leave it at the
defaults. Never set `LANCE_AIMD_*` or equivalent storage options to widen or
narrow it.

## Why the obvious reading is wrong

The throttling and 503s pond saw on Hetzner were a *symptom of issuing too many
requests* - the [versions() manifest
storm](../findings/s3-sync-change-detection-oracle.md), full-column rescans, and
[per-batch merge commits](../findings/s3-append-vs-merge-write-path.md) - not evidence that
the ceiling was too low.

Lance compounds the misreading by labelling the resulting transport timeouts
"Throttle error detected", which once seeded a wrong throttle theory and sent
the investigation at the limiter instead of at the request count.

On an object store, latency is round-trip-bound. The fix is therefore always to
issue *fewer* requests - append-only writes, bounded and index-resident reads,
minimized commit count - never to move the limiter. The job is to be optimal
within its boundaries, not to change them.

## Invalidation

This would change only if a measured workload were shown to be rate-limited
while already issuing the minimum necessary requests, which has not happened.

[^lance-aimd]: Lance cloud-store AIMD limiter defaults
