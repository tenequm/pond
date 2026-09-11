---
type: Finding
title: Semantic vs BM25 retrieval efficacy and cost evaluation
description: In a 1,126-call production trace, BM25 resolved 61% of queries versus 37% for vector search, showing semantic retrieval uniquely helped only 6-7% of calls while adding substantial memory and ingest costs.
tags: [search, vector, fts, bm25, eval, cost]
status: stable
generated: { by: "acpx/gemini-3.7-flash-medium", at: "2026-09-11T11:01:53Z" }
sources:
  - id: semantic-fts-eval
    resource: docs/researches/2608-21-semantic-vs-fts-usage-eval/README.md
    title: "Does semantic search earn its keep? A usage-trace evaluation of vector vs BM25 retrieval over an agent-session archive"
  - id: cost-side-eval
    resource: docs/researches/2608-21-semantic-vs-fts-usage-eval/cost-side.md
    title: "Cost side: what removing embeddings would change (estimate, 2026-08-21)"
---

# Semantic vs BM25 retrieval efficacy and cost evaluation

## Headline claims

- In an outcome audit of 1,126 `pond_search` calls across 63 days, BM25 (FTS) resolved agent needs 61% of the time (149/243) compared to 37% (323/883) for the vector arm.[^semantic-fts-eval]
- In a blind paired A/B evaluation of 90 queries, BM25 was judged superior in 41 cases versus 20 for vector (29 ties, McNemar chi2 = 6.56, p < 0.05), winning even on queries originally phrased for vector search.[^semantic-fts-eval]
- Replaying 120 successful vector queries through BM25 found the top session in 68% of cases, with vector-only successes (23%) concentrated in narrative natural-language paraphrases rather than identifier lookups.[^semantic-fts-eval]
- When initial vector queries failed, agents switched to BM25 87 times and resolved their need 64% of the time, while the reverse switch occurred only 10 times.[^semantic-fts-eval]
- Semantic retrieval uniquely succeeded in only 6-7% of all calls, representing a narrow niche rather than a dominant retrieval mode.[^semantic-fts-eval]
- Generating embeddings during ingest increases initial sync duration from tens of minutes to 2-11 hours on CPU/Metal contention for a ~10.5k session corpus.[^cost-side-eval]
- Serving the embedding model increases process memory from ~100 MiB idle to ~894 MiB RSS per active MCP instance, adding ~70 extra S3 GETs and reload delays after 60s idle eviction.[^cost-side-eval]
- Embeddings account for only ~4-5% of total disk storage (~1.5 KB/message), indicating that compute and memory, not disk capacity, are the primary costs.[^cost-side-eval]

## Invalidation

These findings would be invalidated if agent query workloads shift from identifier and keyword recall to predominantly abstract conceptual paraphrasing, or if compact contextual embedders eliminate runtime memory and cold-reload latency overheads.

## Source

Full research reports: [docs/researches/2608-21-semantic-vs-fts-usage-eval/README.md](../../researches/2608-21-semantic-vs-fts-usage-eval/README.md) and [docs/researches/2608-21-semantic-vs-fts-usage-eval/cost-side.md](../../researches/2608-21-semantic-vs-fts-usage-eval/cost-side.md).

[^semantic-fts-eval]: `docs/researches/2608-21-semantic-vs-fts-usage-eval/README.md`
[^cost-side-eval]: `docs/researches/2608-21-semantic-vs-fts-usage-eval/cost-side.md`
