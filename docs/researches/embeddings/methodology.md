# Methodology

This document records the evaluation design for the embeddings benchmark: the metric, the per-stratum reporting discipline, the ground-truth schemes, and the rule (anchor reachability) that catches unbenchmarkable seed sets before they cost a research wave.

## Question

The benchmark answers three coupled sub-questions:

- Q-hybrid-vs-fts: does the default hybrid mode beat FTS-only on a real, stratified query set?
- Q-hybrid-vs-vector: does the default hybrid mode beat pure-vector retrieval?
- Q-vector-vs-fts: which single retriever is stronger on which stratum?

The three together identify whether the fused result is a genuine improvement over both component retrievers, or whether one retriever already dominates and the fusion is decorative.

## Research-derived constraints

Inherited from `tokenizer-experiment-{plan,report}.md`'s methodology pass:

- Single MRR averaged across heterogeneous query styles is an invalid headline (Buckley & Voorhees, SIGIR 2000/2002). Primary metric is **Success@3** per stratum with 95% Wilson CIs; cross-stratum averages, if reported at all, are labelled as population-weighted harmonic means, never as the headline.
- Rank fusion gains require retriever diversity (Cormack et al., SIGIR 2009; Beitzel et al., JASIST 2004). Vector and BM25 are diverse over a coding-session corpus where some queries are paraphrastic (vector wins) and others are literal/symbolic (FTS wins).
- Anthropic's Contextual Retrieval (2024) reports BM25 + dense cuts top-20 retrieval failure ~49% versus dense alone. That is the prior the experiment is testing, not assuming.
- Lost in the Middle (Liu et al., TACL 2024) implies precision at rank 1-3 matters far more than recall at rank 20 for an agent that injects 1-3 sessions of context. Hence Success@3 as the headline metric.

## Metric and methodology

- Unit of truth: each query has one designated target identifier - either a target session/message id (or short list of acceptable target ids) or an anchor substring expected in the target message's text.
- Primary: **Success@3** per stratum with 95% Wilson CIs.
- Supporting: **P@1** per stratum. Supplementary: **MRR** per stratum (single-answer styles only).
- Reporting: one row per (stratum, mode); never a cross-stratum mean as a headline.
- Significance: paired sign test on Success@3 across modes on the same queries. Strata with n<30 are labeled "directional, underpowered"; CIs are always shown.
- Bare-keyword is reported on equal footing with every other stratum - if hybrid helps natural-language queries but regresses bare-keyword, the per-stratum table makes that visible. Hiding it behind a global mean is exactly the failure the per-stratum structure prevents.

## Query strata

The seed set ships with `bench/embeddings/queries-en.tsv` (21 English entries) and `bench/embeddings/queries-uk-translated.tsv` (21 Ukrainian-translated entries against the same English-language session-id targets).

English strata:

- natural-language (5)
- conceptual (6)
- symbol-lookup (4)
- error-message (3)
- bare-keyword (3)

UK-translated strata mirror the English ones (each EN query has a UK twin against the same target session). The translation is intentionally close, preserving identifiers (`Extracted<T>`, `Lance`) verbatim where they would survive in a real user's phrasing.

## Ground-truth schemes

The TSV format is `id\tlang\tstratum\tquery\tground_truth`. Two schemes:

- `prefix:<id1>,<id2>,...` - at least one session_id or message_id whose 8-char prefix matches must appear in the result list. The 8-char prefix avoids hard-coupling to full UUIDs that may differ between re-syncs.
- `anchor:<substring>` - the literal substring must appear in the `text` field of some hit (NFC normalized, case-insensitive). Used when the message id is not stable across re-syncs.

A hit "matches" ground truth if either scheme is satisfied for its top-N position. Rank 0 means no match in the result window.

## Modes under test

| ID | Mode | Implementation |
|----|------|----------------|
| M-fts | FTS-only (BM25 over Lance inverted index, ngram 3-5 tokenizer) | Production path; under harness, selected via `--mode fts` |
| M-vec | Vector-only (e5-base, cosine on Lance IVF_PQ index) | Selected via `--mode vector`; single-retriever path, no RRF |
| M-hyb | Hybrid (FTS + Vector + RRF, recency boost) | Production default when embeddings exist; selected via `--mode hybrid` |

The three modes share filter pushdown, recency boost, conversation grouping, and the `LIMIT_CAP` / `HIT_TEXT_FULL` constants. Only the retriever fusion differs. Each query runs at `limit=20`; scoring is computed offline.

External baseline kb (Qdrant hybrid over the same Claude Code source files, called via `mcp__kb__kb_search`) was included in the original ablation. It is no longer carried forward because the cross-system comparison validated the FTS-dominance finding but did not influence pond's redesign.

## The anchor-reachability rule

A failure mode discovered the hard way: the original 39-query seed set included 18 Ukrainian queries whose anchor phrases (`визначається при запуску`, `Головне сховище`, `обидві сторони`, etc.) literally did not exist in any indexed message - verified independently against both pond and kb. All 18 scored 0/0 under every mode. The defect was invisible until brute-forced after the fact.

Lesson and rule: before any retrieval mode is run against a new query set, run `bench/embeddings/verify-anchors.sh <queries.tsv>`. The script issues each query against both FTS and Vector top-200 and fails if any ground-truth anchor is reachable from neither. A query that fails BOTH arms is structurally unbenchmarkable; no fusion strategy can recover it.

The companion check is to cross-validate against the kb MCP: `mcp__kb__kb_search(query=<anchor>, project=pond, min_score=0.3)`. If kb finds nothing either, the seed phrase is fictional.

## Pool-size invariant

Production hybrid runs `fts_search(query, pool=100, filter)` and `vector_search(vec, vector_pool=200, filter)` internally, then fuses those candidates. If the simulator (`bench/embeddings/simulate_fusion.py`) replays arm fixtures captured at `pond search --limit 20`, it only sees the top-20 from each arm. For queries where a noise session sits at rank 30-50 in one arm, the simulator never sees the cross-arm agreement signal but production does. The simulator becomes optimistic and confidence-gating ideas that look like wins under truncated fixtures regress in production.

Always capture fixtures at `--limit 100` for FTS and `--limit 200` for Vector before drawing conclusions. Re-verify by comparing simulator's per-query rank against a production run; if they diverge by more than ~1 on average, the fixtures are wrong.

## Query expansion is a caller-layer concern

pond ships no `--expand` flag. Bare-keyword queries underperform because the corpus uses richer vocabulary than the query supplies, not because pond's retrieval has a fixable bug. The right place to expand a query is the agent that's calling pond: it has the LLM cognition to generate paraphrases, synonyms, and disambiguating context that pond's substrate cannot.

Why this is not a pond flag:

- Lexical expansion (stem variants, stopword removal) is forbidden by `spec.md#language-neutral-index` because per-language transforms silently corrupt other languages.
- Semantic expansion requires an LLM call; pond's embedder is a sentence transformer (e5-base), not a generative model. Wiring an LLM into `pond search` would couple substrate to a generative dependency the spec deliberately omits.
- Pool depth (`pool=100`, `vector_pool=200`) is already tuned to surface near-misses; making it configurable would invite "just bump it" without addressing the actual gap (which is embedding quality, not pool size).

The same logic applies to cross-lingual querying. An agent that suspects the corpus may contain text in a language different from its query is the right place to issue two searches (one in the query language, one translated to the corpus's likely language) and union the results by `session_id`. pond does not translate internally.
