# Embeddings benchmark

Research record for pond's hybrid retrieval (FTS + Vector + RRF). Companions:

- `methodology.md` - benchmark design, query strata, ground-truth schemes, the anchor-reachability rule that catches unbenchmarkable seed sets.
- `redesign.md` - the algorithmic changes that ship in pond, why each one, and the final per-stratum numbers.

This directory is the research artifact. The harness, queries, fixtures, simulator, and runtime scripts live alongside the code under `bench/embeddings/`. `docs/spec.md` remains the source of truth for behavior; this report explains the tuning behind the spec-allowed defaults.

## What was measured

pond serves hybrid search by default when embeddings exist for the configured model, FTS-only otherwise. The hypothesis embedded in that design is that adding embeddings improves retrieval over a real corpus in practice. This work measured the hypothesis end-to-end on the user's local pond corpus (8,049 sessions, 1.28M messages, e5-base vectors), found that the initial implementation scored 0/39 Success@3, traced the failure to four retrieval-time defects, redesigned the fusion stage, and re-measured.

## Headline numbers

| Benchmark      | n  | FTS   | Vector | Hybrid (production) | delta vs FTS |
|----------------|----|-------|--------|---------------------|--------------|
| EN-original    | 21 | 18/21 | 15/21  | 19/21               | +1           |
| UK-translated  | 21 |  9/21 | 15/21  | 15/21               | +6           |
| Combined       | 42 | 27/42 | 30/42  | 34/42               | +7           |

The oracle ceiling (top-10 union from either arm) is 38/42 on the combined set. The remaining gap of 4 queries is retrieval-limited, not fusion-limited.

## Corpus snapshot

Single corpus, single user, frozen at the time of the run:

- 8,049 sessions, 1,279,873 messages, 774,560 parts.
- Sources: `claude-code` (1,913 main-agent sessions, 676,857 messages, 64 projects) and `codex` (balance).
- 10.1 GiB on disk after full embed (vector column adds ~3.93 GiB; rest is Lance COW fragments).
- Ukrainian content: 21 sessions / 1,412 messages from nanoclaw (~0.1% of corpus by row count).

## Honest limitations

- Single corpus, single user; the conclusions are about this one machine's session history.
- 42 queries with n=3-6 per stratum; per-stratum confidence intervals are wide and paired sign tests are underpowered. The strongest evidence is the directional consistency of the per-stratum signs.
- The seed queries were authored without awareness of vector retrieval; ground truth is exact-target (a specific session id), not topical-relevance. Hybrid finds topically-adjacent sessions that may also be relevant; under exact-target Success@3 those count as misses.
- The 18 Ukrainian queries in the original 39-query seed set were unbenchmarkable (their anchor phrases never appeared in any indexed message). The UK-translated set (21 queries) is the EN benchmark translated to Ukrainian against the same English-language session-id targets; it isolates "is the retriever cross-lingual?" from "does the corpus contain this conversation?".
