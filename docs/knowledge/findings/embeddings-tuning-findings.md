---
type: Finding
title: Hybrid search score normalization and memory footprint tuning
description: "Historical: score-normalized fusion with a 0.135:1 FTS-to-vector weight ratio raised paraphrase Success@3 to 64.9%, but cross-arm fusion and its weight constants were removed on 2026-06-20 and search now runs one arm per request."
tags: [search, hybrid, fusion, memory, macos, tuning]
status: deprecated
generated: { by: "claude-code/opus-5", at: "2026-09-11T14:49:41Z" }
sources:
  - id: embeddings-tuning
    resource: ../../researches/embeddings.md
    title: Hybrid search tuning (2026-05-27)
  - id: handlers
    resource: ../../../packages/pond/src/handlers.rs
    title: The pond_search handler, which now runs one retrieval arm per request
  - id: embed-rs
    resource: ../../../packages/pond/src/embed.rs
    title: The embedding seam and its idle-eviction constant
---

*Deprecated: the mechanism this concept tuned no longer exists. Cross-arm
fusion was removed on 2026-06-20 and `pond_search` now runs a single arm per
request. Successor:
[Embeddings are opt-in and FTS is the default search arm](../decisions/embeddings-are-opt-in.md).
The measurements below are real and are kept as the evidence behind that move -
they are not current behaviour.*

# Hybrid search score normalization and memory footprint tuning

## What was measured (2026-05-27, against the then-shipping hybrid arm)

- Score-normalized fusion with constants `FTS_FUSION_WEIGHT = 0.135` and
  `VECTOR_FUSION_WEIGHT = 1.0` achieved Success@3 of 64.9% (72/111) on
  paraphrased queries, outperforming symmetric RRF k=60 (52.3%) and asymmetric
  RRF (40.5%).[^embeddings-tuning]
- Asymmetric RRF (`k_fts=5, k_vec=20`) tuned on keyword-heavy seeds severely
  regressed on natural-language paraphrases, where the vector arm alone
  outperformed hybrid 79 to 46.[^embeddings-tuning]
- Standard `ps rss` is inaccurate on macOS (measuring 435 MiB vs 691 MiB
  actual); `phys_footprint` sampled via `proc_pid_rusage` is the only reliable
  metric for embedded ML runtimes.[^embeddings-tuning] This one still holds: it
  is a property of the platform, not of pond.
- Deploying Candle with Metal across platforms, with idle eviction set to 5
  minutes at the time of measurement, achieved a ~165 MiB time-weighted memory
  footprint with a 107 MiB idle floor.[^embeddings-tuning] Read the eviction
  note below before quoting that number.
- Cross-encoder reranking (Qwen3-Reranker-8B) improved paraphrase Success@3
  from 0.649 to 0.739 (+14% relative) at a cost of 80-600 MB of model weight
  and 20-80 ms of per-query latency.[^embeddings-tuning] It was never shipped:
  no reranker exists anywhere in `packages/pond/src`.
- The additive recency boost was capped at `RECENCY_MAX_BOOST = 0.05` to give a
  subtle cross-session tiebreaker without letting recent messages override
  relevance.[^embeddings-tuning]
- Candidate pooling needed at least top-100 FTS and top-200 vector hits before
  the fusion algorithms could detect cross-arm agreement at deeper
  ranks.[^embeddings-tuning]
- Cross-lingual queries against single-language corpora are delegated to
  callers via multi-language probing rather than by translating inside the
  index.[^embeddings-tuning] This one still holds: it is a scope decision, not
  a fusion detail.

## The idle-eviction number, reconciled

The ~165 MiB time-weighted footprint above was measured against a 5-minute idle
eviction, which was the real constant at the time: `DEFAULT_IDLE_EVICTION` was
set to `Duration::from_secs(300)` on 2026-05-27 (commit `b0fb71e`, "candle-only
backend with 5-min idle eviction"). It was cut to `Duration::from_secs(60)` on
2026-06-20 (commit `dc2f3b5`), and 60s is the value in
`packages/pond/src/embed.rs` today.[^embed-rs]

Both numbers are therefore right inside their own time frames: 5 minutes is
what the memory figures in this concept were measured under, and 60s is
current. The sibling concepts quoting 60s -
[read path latency](read-path-latency-findings.md) and
[semantic vs BM25](semantic-vs-fts-usage-findings.md) - describe the current
binary and are the ones to trust for present behaviour.

## Invalidation: already invalidated for current behaviour

The mechanism is gone, not merely at risk of going:

- There is no cross-arm fusion. `pond_search` runs exactly one arm per
  request - `fts` (BM25, the default) or `vector` (kNN), chosen by the caller -
  and the handler says so in its own module docs: "There is no hybrid fusion -
  one arm per request."[^handlers]
- `FTS_FUSION_WEIGHT`, `VECTOR_FUSION_WEIGHT` and the `fuse_arms` function went
  out with the fusion path on 2026-06-20 (commit `dc2f3b5`). Grepping
  `packages/pond/src` for any of the three returns nothing.[^handlers]
- `RECENCY_MAX_BOOST` is gone too. The surviving recency tiebreaker is
  `RECENCY_BOOST_MAGNITUDE: f64 = 0.02` in `packages/pond/src/handlers.rs` - a
  different constant with a different value, applied only to `vector` plus
  `relevance` ordering.[^handlers]
- The 0.135:1 ratio was superseded even before removal: the 2026-06-10 retune
  (commit `f85df03`) moved `FTS_FUSION_WEIGHT` to 0.3, which is the ratio
  quoted in
  [the retrieval tools redesign research](retrieval-tools-redesign-research.md).
  Neither ratio exists in the code now.

For what retrieval does today, read
[Embeddings are opt-in and FTS is the default search arm](../decisions/embeddings-are-opt-in.md)
and section 8.1 of [docs/spec.md](../../spec.md).

The one claim here with no expiry attached is the macOS measurement rule:
`phys_footprint` over `ps rss` would only be invalidated by a change in how
macOS accounts a process's memory.

## Source

Full research report: [docs/researches/embeddings.md](../../researches/embeddings.md).

[^embeddings-tuning]: `docs/researches/embeddings.md`
[^handlers]: `packages/pond/src/handlers.rs`
[^embed-rs]: `packages/pond/src/embed.rs`
