---
type: Finding
title: Hybrid search score normalization and memory footprint tuning
description: Score-normalized fusion with a 0.135:1 FTS-to-vector weight ratio improved paraphrase Success@3 to 64.9%, while macOS memory profiling showed phys_footprint is the only valid metric.
tags: [search, hybrid, fusion, memory, macos, tuning]
status: stable
generated: { by: "acpx/gemini-3.7-flash-medium", at: "2026-09-11T11:01:53Z" }
sources:
  - id: embeddings-tuning
    resource: docs/researches/embeddings.md
    title: Hybrid search tuning (2026-05-27)
---

# Hybrid search score normalization and memory footprint tuning

## Headline claims

- Score-normalized fusion with constants `FTS_FUSION_WEIGHT = 0.135` and `VECTOR_FUSION_WEIGHT = 1.0` achieved Success@3 of 64.9% (72/111) on paraphrased queries, outperforming symmetric RRF k=60 (52.3%) and asymmetric RRF (40.5%).[^embeddings-tuning]
- Asymmetric RRF (`k_fts=5, k_vec=20`) tuned on keyword-heavy seeds severely regressed on natural-language paraphrases, where vector-alone outperformed hybrid 79 to 46.[^embeddings-tuning]
- Standard `ps rss` is inaccurate on macOS (measuring 435 MiB vs 691 MiB actual); `phys_footprint` sampled via `proc_pid_rusage` is the only reliable metric for embedded ML runtimes.[^embeddings-tuning]
- Deploying Candle with Metal across platforms alongside a 5-minute idle eviction policy achieved a ~165 MiB time-weighted memory footprint with a 107 MiB idle floor.[^embeddings-tuning]
- Cross-encoder reranking (Qwen3-Reranker-8B) improved paraphrase Success@3 from 0.649 to 0.739 (+14% relative), but was deferred due to model size (80-600 MB) and 20-80ms per-query latency.[^embeddings-tuning]
- Additive recency boost was capped at `RECENCY_MAX_BOOST = 0.05` to provide a subtle cross-session tiebreaker without allowing recent messages to override relevance.[^embeddings-tuning]
- Internal search candidate pooling requires retrieving at least top-100 FTS and top-200 vector hits to ensure fusion algorithms detect cross-arm agreement at deeper ranks.[^embeddings-tuning]
- Cross-lingual queries against single-language corpora are delegated to callers via multi-language probing rather than performing internal index translation.[^embeddings-tuning]

## Invalidation

These findings would be invalidated if score distributions between Lance vector and BM25 indices drift substantially under different corpus distributions, or if Candle updates eliminate Metal buffer pool retention issues to allow zero-cost model residency.

## Source

Full research report: [docs/researches/embeddings.md](../researches/embeddings.md).

[^embeddings-tuning]: `docs/researches/embeddings.md`
