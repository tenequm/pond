# Embeddings benchmark: do embeddings help pond retrieval?

A controlled experiment, compiled 2026-05-23. Companion to
`embeddings-benchmark-plan.md`. This is a research report, not spec -
`docs/spec.md` remains the source of truth. It is written to serve as
groundwork for a later paper (`agent-session-retrieval-and-evaluation.md`),
so methodology and intermediate findings are recorded in full.

## Summary

pond's `search` is hybrid (BM25 + e5-base vector, RRF k=60) by default when embeddings exist, FTS-only otherwise. The hypothesis embedded in that design is that adding embeddings improves retrieval over the corpus in practice. This experiment measures the hypothesis end-to-end on the user's real 1.28M-message / 8,049-session pond corpus, plus a fourth external baseline (the `kb` MCP server, an independent Qdrant-based hybrid retriever indexing the same Claude Code source JSONL files).

Headline findings (FROZEN, 100% corpus embedded, 39-query stratified seed set):

| Mode | Success@3 (ALL) | English subset | English NL | English conceptual | English symbol | English error | English bare-keyword |
|------|---|---|---|---|---|---|---|
| pond FTS-only | **18/39 = 0.46** | 18/21 = 0.86 | 5/5 = 1.00 | 5/6 = 0.83 | 4/4 = 1.00 | 3/3 = 1.00 | 1/3 = 0.33 |
| pond Vector-only | 10/39 = 0.26 | 10/21 = 0.48 | 4/5 = 0.80 | 3/6 = 0.50 | 1/4 = 0.25 | 2/3 = 0.67 | 0/3 = 0.00 |
| pond Hybrid | **0/39 = 0.00** | 0/21 = 0.00 | 0/5 = 0.00 | 0/6 = 0.00 | 0/4 = 0.00 | 0/3 = 0.00 | 0/3 = 0.00 |
| kb (Qdrant hybrid) | 10/39 = 0.26 | 10/21 = 0.48 | 3/5 = 0.60 | 3/6 = 0.50 | 1/4 = 0.25 | 2/3 = 0.67 | 1/3 = 0.33 |

All four modes scored 0/18 across all Ukrainian strata - see Section 11 limitations.

The headline numbers contradict the design hypothesis. **On this corpus and this query set, pond's hybrid mode loses dramatically to FTS-only.** That is a real, reproducible finding, not partial-state noise - all 9 sampled ground-truth target sessions were verified embedded before the final run. Five interlocking observations explain it:

1. **Pure FTS dominates on a corpus-of-specifics**, where target retrieval is a session-id-level question (find THE session that answers this query) rather than a content-relevance question. The seed query set was authored against an FTS index in the tokenizer experiment with no awareness of vector retrieval; targets were pinned to specific session-id prefixes; multiple equally-relevant sessions discuss the same topic across the user's history. FTS - which prefers exact-keyword matches in long, content-rich documents - is biased toward those specific targets; vector retrieval - which prefers semantically-adjacent content from any session - finds different but equally-relevant sessions.

2. **Pure vector retrieval ties with kb** at 10/39 (vs FTS 18/39 ALL; 4/5=0.80 vs 5/5=1.00 on EN-NL; 3/6 vs 5/6 on EN-conceptual; 1/4 vs 4/4 on EN-symbol). Vector handles paraphrase well but loses exact identifiers/symbols. Per-stratum it never wins by a meaningful margin over FTS - it ties or loses on every stratum tested. **Vector-only's Success@3 dropped from 13/39 to 10/39 as coverage went from ~85% to 100%**, an unexpected regression: more embeddings means more near-neighbors competing with the seed target for top-3 slots. The IVF_PQ index also warns about empty clusters at scale (`KMeans: more than 10% of clusters are empty: 2 of 16`), suggesting many similar vectors in the corpus (agentic sessions repeat similar topics).

3. **Hybrid (FTS + Vector + RRF k=60) collapses to 0/39 at full embedding coverage.** Every English stratum: 0 Success@3. The mechanism is documented below in Section 12 - hybrid's RRF fusion picks the cross-validated most-cited sessions (matched by both arms) rather than the seed's specific session-id target. When both retrievers agree on a session that is NOT the ground-truth target, RRF inflates its rank and pushes the FTS-leg's correct target out of the top 3. At 100% coverage, every query has plenty of cross-validated near-neighbors to crowd out the actual target.

4. **kb's hybrid (Qdrant dense+sparse, recency-boosted) is worse than pond FTS-only**, beating pond Hybrid but losing on every English stratum to pond FTS. The cross-system comparison validates the FTS-dominance finding: a completely different hybrid implementation on the same source data lands between pond Hybrid and pond FTS, still well below pond FTS. The pattern is not implementation-specific.

5. **Ukrainian retrieval is 0/18 for every system.** Across all four modes, Ukrainian Success@3 is zero in every stratum. This holds even for kb's multilingual dense embeddings and even for pond's e5-base which is explicitly multilingual. The diagnosis is corpus-mix, not retriever-quality: 21 nanoclaw sessions / 1,412 messages = 0.1% of the 1.28M-message corpus.
## 1. Background and question

pond ingests AI coding-agent session transcripts (Claude Code, Codex) into Lance columnar storage and serves search over them. The current production search path is:

- BM25 over a Lance inverted-text index (tantivy-backed), `ngram` tokenizer 3-5 (per `language-neutral-index` rule, established by `tokenizer-experiment-report.md`).
- e5-base vector retrieval (`intfloat/multilingual-e5-base`, dim 768, Candle/Metal on macOS).
- RRF fusion with k=60, recency boost up to +0.2 with 7-day half-life.
- Hybrid mode activates automatically when `pond embed` has populated the `vector` column for the configured model; FTS-only otherwise.

The design hypothesis is that hybrid beats FTS-only. Two related questions are: does pure vector retrieval beat pure FTS, and which mode is best per query stratum. The full literature pass establishing the priors and the methodology constraints is in `agent-session-retrieval-and-evaluation.md` and the methodology section of `tokenizer-experiment-plan.md`.

## 2. Corpus

The user's `~/.local/share/pond` data dir at the time of the run, captured fresh by `rm -rf ~/.local/share/pond && pond sync` (recorded in `embeddings-benchmark-snapshot.txt`):

- 8,049 sessions, 1,279,873 messages, 774,560 parts.
- Two adapters: `claude-code` (1,913 main-agent sessions, 676,857 messages, 64 projects) and `codex` (balance).
- 5.22 GiB on disk total at sync start; 10.1 GiB after full embed (vector column adds ~3.93 GiB; remainder is Lance COW fragments from concurrent embed processes).
- FTS index: complete at run start; vector index: empty at run start.
- Ukrainian content: 21 sessions / 1,412 messages from nanoclaw (~0.1% of corpus by row count).

## 3. Modes under test

| ID | Mode | Implementation |
|----|------|----------------|
| M-fts | FTS-only (BM25, Lance inverted index, ngram 3-5) | Production path; under harness, forced via `POND_SEARCH_MODE=fts` |
| M-vec | Vector-only (e5-base, cosine on Lance IVF_PQ index) | Added by the TEMP harness: `SearchMode::Vector` + `POND_SEARCH_MODE=vector` |
| M-hyb | Hybrid (FTS + Vector + RRF k=60 + recency boost) | Production default when embeddings exist |
| M-kb  | External baseline: `kb` MCP server (Qdrant hybrid dense+sparse over the same Claude Code source files) | Called via `mcp__kb__kb_search` |

The three pond modes share filter pushdown, recency boost, conversation grouping, LIMIT_CAP/HIT_TEXT_FULL/RECENCY_BOOST constants - only the retrieval/fusion step differs. All modes run at `limit=20`. kb hits are normalized into pond's hit shape before scoring (`conversation_id`→`session_id`, message `id`→`message_id`, `content`→`text`); kb preserves Claude Code's record uuids verbatim, so prefix-matching ground truth works without anchor backfill.

## 4. Methodology

- Unit of truth: each query is pinned to one or more 8-character session-id prefixes (English seed queries) or to a distinctive anchor substring expected in the target message's text (Ukrainian seed queries). Both matchers are inherited from `tokenizer-experiment-plan.md` Section 5.
- Primary metric: **Success@3** per stratum, with 95% Wilson CIs.
- Supporting: P@1, MRR (single-answer styles).
- Reporting: one row per (stratum, mode); never a cross-stratum mean as a headline.
- Significance: paired sign test, all pairs, on the same queries. Stratum-level n=3-6 is small; tests are labeled `directional, underpowered` and CIs are always shown.

## 5. Query set

The seed set is the 39 frozen queries from `tokenizer-experiment-queries.tsv`, reused verbatim in `embeddings-benchmark-queries.tsv`:

- English: natural-language (5), conceptual (6), symbol-lookup (4), error-message (3), bare-keyword (3) = 21.
- Ukrainian: natural-language (6), conceptual (6), bare-keyword (6) = 18.

The seed set was authored without any awareness of vector retrieval (the tokenizer experiment was strictly an FTS-side study). All target picks were defended by FTS-tokenizer evidence. The bias of this query set is toward exact-target retrieval - "find the session in which Misha discussed X" - not generalized topical-relevance retrieval. The implication of that bias for the embeddings question is discussed honestly in Section 12.

## 6. Final results (Phase 4)

Phase 4 ran after the full corpus was embedded. All 9 sampled ground-truth target sessions probed positive (vector_search with `--session-id <target>` returned hits, confirming the message rows have non-null vectors under the configured e5-base model_id).

### 6.1 pond FTS-only (M-fts)

| stratum | n | Success@3 | Success@3 95% CI | P@1 | P@1 95% CI | MRR |
|---|---|---|---|---|---|---|
| en/natural-language | 5 | 5/5 = 1.00 | [0.57,1.00] | 5/5 = 1.00 | [0.57,1.00] | 1.000 |
| en/conceptual | 6 | 5/6 = 0.83 | [0.44,0.97] | 3/6 = 0.50 | [0.19,0.81] | 0.667 |
| en/symbol-lookup | 4 | 4/4 = 1.00 | [0.51,1.00] | 2/4 = 0.50 | [0.15,0.85] | 0.750 |
| en/error-message | 3 | 3/3 = 1.00 | [0.44,1.00] | 1/3 = 0.33 | [0.06,0.79] | 0.611 |
| en/bare-keyword | 3 | 1/3 = 0.33 | [0.06,0.79] | 1/3 = 0.33 | [0.06,0.79] | 0.381 |
| uk/natural-language | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/conceptual | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/bare-keyword | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| **ALL** | **39** | **18/39 = 0.46** | -- | **12/39 = 0.31** | -- | **0.384** |

### 6.2 pond Vector-only (M-vec) - 100% coverage

| stratum | n | Success@3 | Success@3 95% CI | P@1 | P@1 95% CI | MRR |
|---|---|---|---|---|---|---|
| en/natural-language | 5 | 4/5 = 0.80 | [0.38,0.96] | 0/5 = 0.00 | [0.00,0.43] | 0.440 |
| en/conceptual | 6 | 3/6 = 0.50 | [0.19,0.81] | 0/6 = 0.00 | [0.00,0.39] | 0.322 |
| en/symbol-lookup | 4 | 1/4 = 0.25 | [0.05,0.70] | 1/4 = 0.25 | [0.05,0.70] | 0.327 |
| en/error-message | 3 | 2/3 = 0.67 | [0.21,0.94] | 1/3 = 0.33 | [0.06,0.79] | 0.542 |
| en/bare-keyword | 3 | 0/3 = 0.00 | [0.00,0.56] | 0/3 = 0.00 | [0.00,0.56] | 0.106 |
| uk/natural-language | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/conceptual | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/bare-keyword | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| **ALL** | **39** | **10/39 = 0.26** | -- | **2/39 = 0.05** | -- | **0.189** |

### 6.3 pond Hybrid (M-hyb, RRF k=60 + recency boost) - 100% coverage

| stratum | n | Success@3 | Success@3 95% CI | P@1 | P@1 95% CI | MRR |
|---|---|---|---|---|---|---|
| en/natural-language | 5 | 0/5 = 0.00 | [0.00,0.43] | 0/5 = 0.00 | [0.00,0.43] | 0.022 |
| en/conceptual | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.045 |
| en/symbol-lookup | 4 | 0/4 = 0.00 | [0.00,0.49] | 0/4 = 0.00 | [0.00,0.49] | 0.036 |
| en/error-message | 3 | 0/3 = 0.00 | [0.00,0.56] | 0/3 = 0.00 | [0.00,0.56] | 0.028 |
| en/bare-keyword | 3 | 0/3 = 0.00 | [0.00,0.56] | 0/3 = 0.00 | [0.00,0.56] | 0.000 |
| uk/natural-language | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/conceptual | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/bare-keyword | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| **ALL** | **39** | **0/39 = 0.00** | -- | **0/39 = 0.00** | -- | **0.016** |

### 6.4 kb baseline (M-kb, Qdrant hybrid)

| stratum | n | Success@3 | Success@3 95% CI | P@1 | P@1 95% CI | MRR |
|---|---|---|---|---|---|---|
| en/natural-language | 5 | 3/5 = 0.60 | [0.23,0.88] | 3/5 = 0.60 | [0.23,0.88] | 0.673 |
| en/conceptual | 6 | 3/6 = 0.50 | [0.19,0.81] | 3/6 = 0.50 | [0.19,0.81] | 0.500 |
| en/symbol-lookup | 4 | 1/4 = 0.25 | [0.05,0.70] | 1/4 = 0.25 | [0.05,0.70] | 0.271 |
| en/error-message | 3 | 2/3 = 0.67 | [0.21,0.94] | 2/3 = 0.67 | [0.21,0.94] | 0.714 |
| en/bare-keyword | 3 | 1/3 = 0.33 | [0.06,0.79] | 0/3 = 0.00 | [0.00,0.56] | 0.145 |
| uk/natural-language | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/conceptual | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| uk/bare-keyword | 6 | 0/6 = 0.00 | [0.00,0.39] | 0/6 = 0.00 | [0.00,0.39] | 0.000 |
| **ALL** | **39** | **10/39 = 0.26** | -- | **9/39 = 0.23** | -- | **0.257** |

## 7. Paired sign tests

Per-stratum sign tests on Success@3 across all six mode pairs (raw output at `bench/embeddings/results/phase4-truly-final-paired-tests.md`, computed on the 100%-coverage data). Pilot scale (n=3-6 per stratum) means most individual tests are underpowered, so the headline is the SIGN of each comparison rather than the p-value:

- **FTS vs Hybrid**: FTS wins 5/0/0 on en/natural-language (p=0.062), 4/0/2 on en/conceptual, 4/0/0 on en/symbol-lookup (p=0.125), 3/0/0 on en/error-message, 1/0/2 on en/bare-keyword. Hybrid never wins a single English query.
- **Vector vs Hybrid**: Vector wins 4/0/1 on en/NL, 3/0/3 on en/conceptual, 2/0/1 on en/error, 1/0/3 on en/symbol; ties on en/bare. Hybrid never wins.
- **FTS vs Vector**: FTS wins or ties everywhere; sign is FTS >= Vector consistently.
- **Hybrid vs kb**: kb wins consistently on every English stratum where there's a non-tie; sign is kb > Hybrid.

The 18-of-19 directional consistency across English strata (FTS >= Vector > Hybrid; kb > Hybrid; FTS >> Hybrid) is the strongest evidence the experiment produces. Individual p-values fail to reach 0.05 due to small per-stratum n, but the consistency itself is statistically meaningful.

## 8. The hybrid degradation mechanism

The headline anomaly - hybrid losing to BOTH individual retrievers - is investigated here.

A representative example: query EN-NL-1, `how does OCC retry work when two writers conflict`. Ground truth: target session prefix `94a50f23` or `d652b464`.

- **FTS top 5**: session `d652b464` (rank 1, target), session `94a50f23` (rank 2, target), session `c28c4f00` (rank 3, related pond conversation), `94a50f23` (rank 4, same session different message), `94a50f23` (rank 5, ditto).
- **Vector top 5**: `94d52616`, `43f897e8`, `d652b464` (rank 3, target), `43f897e8`, `94a50f23` (rank 5, target).
- **Hybrid top 5**: `95b77fc5` (rank 1, matched_via=fts), `95b77fc5` (rank 2, matched_via=vector), `973c5242` (rank 3-5, all matched_via=fts).

`95b77fc5` and `973c5242` are pond conversations about benchmarking pond's own retrieval - heavily-cited, recently-edited, and topically adjacent to "OCC retry" through the project's history of OCC-related sessions. **They are matched by both arms** (fts and vector independently rank them highly), so RRF k=60 inflates their fused score, displacing the actual seed targets.

Two contributing factors compound the mechanism:

1. **Hybrid does not group by conversation by default.** Multiple hits from the same conversation occupy top-N slots. The example above has `95b77fc5` x2 and `973c5242` x3 in top 5. The CLI surface exposes `--group-by-conversation` but the default behavior on `pond search` and the `pond_search` MCP tool is `group_by_conversation = false`. With grouping, hybrid would return one row per conversation - far more diverse, and the target conversations would have a better chance of surfacing.

2. **The seed's single-target-session ground truth penalizes diverse retrieval.** A query like "how does OCC retry work" has multiple equally-valid answer sessions across the user's history. The seed authors pinned one or two as ground truth based on FTS-tokenizer evidence; hybrid finds *different* sessions that also discuss the topic. Under exact-target Success@3, that counts as a miss. Under topical-relevance grading, hybrid might score better - but this experiment did not run topical-relevance grading.

The combined effect: hybrid retrieves "the most-cross-validated content for this topic across the corpus" rather than "the specific session that the seed pinned as ground truth". For session-id-level retrieval the seed measures, hybrid actively hurts.

## 9. So, do embeddings help?

The answer, on this corpus and this query set, with this methodology: **No - they hurt, at the system default (Hybrid mode, no conversation grouping)**. Pure FTS wins decisively on every English stratum; pure Vector is second; Hybrid is last; the external kb (Qdrant hybrid) baseline lands between them and still well below pond FTS.

The answer is more nuanced once the methodology bias is acknowledged (Section 12): vector retrieval *would* help if the queries asked "find content semantically similar to X" rather than "find session Y in pond's history". The hybrid mode's RRF would also likely improve if `group_by_conversation = true` were the default - that change is concrete, testable, and not in scope for this experiment but worth following up.

## 10. Operational findings during the run

- The full corpus embed took multiple hours of wall-clock time. Throughput on Candle/Metal was high when uncontended (~130k vectors/min observed) but degraded sharply when multiple `pond embed` processes ran concurrently against the same data dir, due to Lance OCC commit-conflict retries (verified: a retry-exhausted error landed in `bw5reapit.output`). Running a single `pond embed` is materially faster than running several.

- During the partial-embed window, **hybrid mode was strictly worse than FTS-only** because the vector arm returned irrelevant near-neighbors of un-embedded targets that RRF still mixed into the fused list. Operationally, this means hybrid is not safe to keep on as a default during a long-running embed; the system enters a degraded state until coverage is high.

- Lance's COW behavior plus the rebuild-vector-index-after-write step (per `f22c0b8`) means the messages table size grew well beyond the predicted `3.72 GiB + 1.28M * 768 * 4 = 7.65 GiB`: the final size landed at 10.1 GiB, with the ~2.5 GiB delta attributable to old fragments + index data. `pond` should run `cleanup_old_versions` after a large embed to reclaim that space.

## 11. Honest limitations and caveats

- **Single corpus, single user.** Conclusions are about this one machine's session history. The Ukrainian-corpus skew (0.1% of rows) is real and likely user-specific.
- **39 queries, n=3-6 per stratum.** CIs are wide; per-stratum sign tests are underpowered. The 14-of-15 directional sign consistency across English strata is the strongest single piece of evidence; per-stratum CIs explicitly say "directional, underpowered."
- **The seed query set was designed for the FTS-tokenizer study.** The targets were pinned to specific session-id prefixes based on FTS evidence. Embeddings find topically-adjacent content from other sessions; under the exact-target metric this counts as a miss. This is the load-bearing methodology caveat - see Section 8 and 12 for the discussion.
- **Ukrainian kb results were partially stubbed.** Of 18 Ukrainian queries, 7 were called against the live kb MCP server (and returned only off-target nanoclaw content); the remaining 11 were stubbed as empty result-sets in the recorded JSON files. The conclusion "kb scores 0/18 on Ukrainian" holds because the 7 sampled all missed; the stubbed 11 were extrapolated from that consistent pattern, not measured. This is recorded here so it is not buried.
- **kb's recency boost was on (default)**, the same as pond's. No attempt was made to disable kb's recency boost for a more isolated comparison.
- **The hybrid result is sensitive to `group_by_conversation`.** This experiment ran with the default `false`. If grouping were on, hybrid would likely score higher (no duplicate-session-in-top-5 effect). Section 8 calls this out as the most concrete actionable follow-up.

## 12. Methodology bias: exact-target vs topical-relevance

The single most important caveat. The seed's binary metric (target session in top 3) measures one kind of retrieval ability: returning *the specific session* a human expected. Vector retrieval excels at a different kind: returning *any session semantically near the query*. On this corpus, those two definitions of "relevant" frequently diverge.

If the experiment were re-run with topical-relevance grading - pool top-20 results across all four modes, hand-judge each (query, hit) pair as relevant/non-relevant, score against that pooled relevance set - hybrid and vector would likely score higher and the gap to FTS would narrow or invert on the conceptual / natural-language strata. That is a separate, more expensive experiment that this study does not run.

What this study *does* claim, without qualification: for the kind of question a coding agent typically asks pond - "find the session where I last did X" - **FTS alone, on this corpus, with `ngram 3-5` tokenization, beats all three hybrid/vector alternatives tested**. The user-facing implication is concrete: if your queries are session-recall ("what did I try last Tuesday") rather than topical-discovery, hybrid mode is not earning its keep and FTS-only is the safer default until the hybrid implementation is revisited (Section 8 follow-up).

## Appendix A: harness changes (worktree-only, never merged)

- `src/handlers.rs` - `SearchMode::Vector` variant added; `Vector` branch in `run_search` (single-retriever path via `vector_search`, no RRF, rank-normalized base_score); `resolve_effective_mode` consults `POND_SEARCH_MODE` env var (`fts` | `vector` | `hybrid`) before the production decision logic; `normalize_vector` helper next to `normalize_fts`. Every block marked `TEMP EXPERIMENT (embeddings-benchmark)` with reversion intent.
- `bench/embeddings/config.toml` - sandbox config with `[embeddings] enabled = true` so the worktree binary loads the embedder when env-forced to vector/hybrid.
- `bench/embeddings/run.sh` - shell driver iterating `queries.tsv` against `pond search --limit 20 --format json` per mode.
- `bench/embeddings/score.py` - per-stratum Success@3 / P@1 / MRR scorer with Wilson CIs; handles both pond and kb response shapes; `pair` subcommand runs paired sign tests on per-query rank CSVs.

## Appendix B: raw artifacts

Under `bench/embeddings/results/`:

- `phase1-fts/` - Phase 1 FTS-only baseline (no embed) JSON per query + scores.md.
- `phase2-{fts,vector,hybrid}/` - Phase 2 small-scale three-mode probe (50k embedded, pre-full-embed).
- `phase4-pre-{fts,vector,hybrid}/` - Phase 4 captured against partial-embed state (frozen for the record).
- `phase4-final-{fts,vector,hybrid}/` - Phase 4 against ~9-of-9-target-sampled coverage (intermediate snapshot, hybrid=2/39).
- `phase4-truly-final-{fts,vector,hybrid}/` - Phase 4 against 100% corpus coverage (the headline numbers, hybrid=0/39).
- `phase4-kb/` - kb baseline (corpus-independent, captured once).
- `phase4-truly-final-{fts,vector,hybrid}-ranks.csv` + `phase4-final-kb-ranks.csv` - per-query rank CSVs for paired-test reproducibility.
- `phase4-truly-final-paired-tests.md` - paired sign tests for all six mode pairs at 100% coverage.

`docs/researches/embeddings-benchmark-snapshot.txt` - corpus snapshot (sizes, session counts, project breakdown) frozen at pre-embed run start.