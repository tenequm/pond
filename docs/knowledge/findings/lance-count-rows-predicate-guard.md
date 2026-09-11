---
type: Finding
title: Guard count_rows inequalities with IsNotNull, and count a narrow column
description: A bare Ne on a nullable column in Dataset::count_rows ran over 25 minutes on ~2M rows; wrapping it as And(IsNotNull, Ne) dropped the same count to 7.35 seconds.
tags: [lance, performance, s3, query]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: count-rows-measurement
    resource: "Dataset::count_rows timings taken against the operator's real S3 store, ~2M rows, ~87% null, one distinct non-null value"
    title: count_rows predicate timings on the production store
---

# Guard count_rows inequalities with IsNotNull, and count a narrow column

## The trap

A bare inequality (`Ne`) on a nullable column in `Dataset::count_rows` hits a
pathological slow path. Measured on the real S3 store over ~2M rows that were
~87% null with a single distinct non-null value:[^count-rows-measurement]

| Predicate | Time |
|---|---|
| `count_rows(Ne("embedding_model", v))` | **over 25 min (effectively hung)** |
| `And(IsNotNull("embedding_model"), Ne("embedding_model", v))` | **7.35 s** |
| bare `IsNotNull` | fast |

It is the unguarded `Ne`, not the column, that is the trap.

## Count a narrow column, not a wide one

Lance keeps no per-column null_count metadata, so every `count_rows(filter)` is
a data-page read. Count a narrow co-set column rather than a wide one:
`embedding_model IS NOT NULL`, never `vector IS NOT NULL`. The two are co-set
per `spec.md#session-embed-from-canonical`, so they answer the same question at
very different cost.

## Invalidation

This would change if Lance began keeping per-column null counts in metadata, or
if its predicate planner learned to add the null guard itself.

[^count-rows-measurement]: `Dataset::count_rows` timings on the operator's real S3 store
