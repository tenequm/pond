# Embeddings benchmark plan: do embeddings help pond retrieval?

Status: plan. Drives a controlled experiment; the result is a separate report doc.
Worktree: `worktree-embeddings-benchmark`. Not spec - `docs/spec.md` stays source of truth.

## 1. Question

pond's `search` is hybrid (BM25 + e5-base vector, fused with RRF k=60) when embeddings exist for the configured model, FTS-only otherwise. The hybrid path is enabled by default whenever `pond embed` has populated the `vector` column, with no user-facing switch (the mode is server-decided; per-hit `matched_via` exposes which retrievers ranked a row).

The hypothesis embedded in pond's design is that adding embeddings improves retrieval over the corpus in practice. That hypothesis has not been measured end-to-end on pond's real corpus. This experiment measures it.

Three sub-questions, all in scope:

- Q-hybrid-vs-fts: does the default hybrid mode beat FTS-only on a real, stratified query set?
- Q-hybrid-vs-vector: does the default hybrid mode beat pure-vector retrieval?
- Q-vector-vs-fts: which single retriever is stronger on which stratum?

The point of asking all three together is to identify whether the fused result is a genuine improvement over both component retrievers, or whether one retriever already dominates and the fusion is decorative.

## 2. Research-derived constraints

The methodology is reused wholesale from `tokenizer-experiment-{plan,report}.md`, whose literature pass already grounded the per-stratum evaluation discipline:

- Single MRR averaged across heterogeneous query styles is an invalid headline (Buckley & Voorhees, SIGIR 2000/2002). Primary metric is **Success@3** per stratum with 95% Wilson CIs; cross-stratum averages, if reported at all, are explicitly labelled as population-weighted harmonic means, never as the headline.
- Rank fusion gains require retriever **diversity** (Cormack et al., SIGIR 2009; Beitzel et al., JASIST 2004). Vector and BM25 are diverse over a coding-session corpus where some queries are paraphrastic (vector wins) and others are literal/symbolic (FTS wins). Hybrid is therefore expected to help, but the gain - and its distribution across strata - is the empirical question.
- Anthropic's Contextual Retrieval blog (2024) reports BM25 + dense cuts top-20 retrieval failure ~49% versus dense alone, across mixed corpora. That is the prior the experiment is testing on pond's actual corpus, not assuming.
- Lost in the Middle (Liu et al., TACL 2024) implies precision at rank 1-3 matters far more than recall at rank 20 for an agent that injects 1-3 sessions of context. Hence Success@3 as the headline metric.

The wider landscape - what should and should not be measured for a session-retrieval substrate - is recorded in `docs/researches/agent-session-retrieval-and-evaluation.md`. The salient point for this experiment: the number it produces is a retrieval-quality number (Section 1.1 of that doc, "vacuum measure"), not a downstream task-completion number. The report will label it as such.

## 3. Corpus

The full pond corpus on the user's machine at the time of the run, captured by:

```
rm -rf ~/.local/share/pond && pond sync
```

Resulting state (recorded once, frozen in the report):

- 8,049 sessions, 1,279,873 messages, 774,560 parts.
- Source agents: `claude-code` (1,913 main-agent sessions, 676,857 messages, 64 projects) and `codex` (balance).
- FTS index: complete at run start.
- Vector index: empty at run start (no `pond embed` has been run).

Embeddings will be produced by `pond embed`, which uses the configured embedder (e5-base on Candle/Metal, per recent commits 0736209, 7785142, f22c0b8). The full backlog is the 1.28M-row messages table; throughput will be measured (rows/sec, total wall time) and reported.

This is a single-corpus experiment - one machine, one user's session history. Conclusions will be labelled as such; multi-corpus replication is out of scope.

## 4. Modes under test

| ID | Mode | Implementation |
|----|------|----------------|
| M-fts | FTS-only (BM25 over Lance inverted index, ngram 3-3 tokenizer per current production) | Existing code path when no embeddings exist; under the experiment harness, forced via `POND_SEARCH_MODE=fts` even when embeddings exist |
| M-vec | Vector-only (e5-base dense, cosine on Lance IVF_PQ index) | New code path added by the TEMP harness: `SearchMode::Vector` variant + `POND_SEARCH_MODE=vector` gate |
| M-hyb | Hybrid (FTS + Vector, RRF k=60, recency boost enabled, server defaults) | Existing code path when embeddings exist; default of `pond search` |
| M-kb  | External baseline: `kb` MCP server (Qdrant hybrid dense+sparse over the user's Claude Code conversation history) | Run via the `mcp__kb__kb_search` tool. kb ingests the same upstream Claude Code session files as pond's `claude-code` adapter, so the corpus is operationally the same (though indexed independently by a separate system with its own tokenizer + embedding stack) |

The first three modes share filter pushdown, recency boost, conversation grouping, and the LIMIT_CAP/HIT_TEXT_FULL constants. Only the retriever fusion differs. Each query is run against all four modes at `limit=20` (capture deep enough for any reasonable @k); scoring is computed offline.

M-kb is treated as a fourth modality, not a replacement for pond's three modes. Including it answers a second question alongside the pond-internal ablation: does pond's stack (Lance + e5-base + RRF) outperform a representative external session-retrieval system (Qdrant + dense+sparse fusion) on the same query set and the same source data? A win on either side is informative; the report will label kb as an external baseline, not a member of the pond mode ablation.

## 5. Metric and methodology

- Unit of truth: each query has one designated target identifier - either a target message id (or short list of acceptable target ids) or an anchor substring expected in the target message's text. The two matchers are inherited from the tokenizer experiment. The kb baseline ingests the same upstream Claude Code JSONL files as pond's `claude-code` adapter; both systems preserve Claude Code's record uuids verbatim (kb's `conversation_id` = pond's `session_id`, kb's message `id` = pond's `message_id`). Verified by probe: a query targeting session prefix `94a50f23` returns the same conversation in both kb and pond. So the existing `prefix:` ground truth works across both systems without anchor backfill; the kb hits only need a field-name normalization (`conversation_id` -> `session_id`, `id` -> `message_id`, `content` -> `text`) before scoring.
- Primary: **Success@3** - target identified in the top 3 hits. Per stratum, with 95% Wilson CIs.
- Supporting: **P@1** per stratum. Supplementary: **MRR** per stratum (single-answer styles only).
- Reporting: one row per stratum per mode, never a cross-style average as a headline. The hybrid-vs-fts delta, the hybrid-vs-vector delta, and the vector-vs-fts delta are reported per stratum.
- Significance: per-stratum paired sign test, M-hyb vs M-fts and M-hyb vs M-vec, on the same queries. Strata with n<30 are labeled "directional, underpowered"; CIs are always shown so the reader sees the uncertainty.
- The bare-keyword stratum is reported on equal footing with every other - if hybrid helps natural-language queries but regresses bare-keyword, the per-stratum table makes that visible. Hiding it behind a global mean is exactly the failure the tokenizer experiment's structure prevents.

## 6. Query sets

The seed set is the 39 frozen queries from `docs/researches/tokenizer-experiment-queries.tsv`, eight strata:

- English: natural-language (5), conceptual (6), symbol-lookup (4), error-message (3), bare-keyword (3).
- Ukrainian: natural-language (6), conceptual (6), bare-keyword (6).

These are already pinned to message-uuid prefixes and anchor substrings, and the targets exist in the current ~/.local/share/pond corpus (the source claude-code sessions are the same; pond message ids = Claude-Code record uuids per the adapter contract). A probe of EN-NL-1 against the live corpus confirms the prefix matches.

The seed set's strength is that it was constructed without any awareness of vector retrieval - all target picks were defended by FTS-tokenizer evidence. So embeddings cannot be accused of being measured against a vector-friendly query set; the bias, if any, is toward lexical matching.

Extension queries (added in Phase 5 if Phase 4 results warrant deeper resolution):

- Project-spanning queries (one query, target session lives in any of the 64 projects in the corpus) - probes whether dense retrieval helps when the user does not know which project to scope to. Pure FTS over a 1.28M-message corpus is expected to lose IDF discrimination here; embeddings are the differentiating signal if the hypothesis holds.
- Paraphrase queries (the query restates the target's content using synonyms not present in the target text) - the canonical case where embeddings are expected to help.
- Failure queries (queries the seed set's authors recall being unable to answer with FTS-only `pond search`) - sourced from the user's lived experience, not synthetic.

All extension queries get target message ids using the same `prefix:` / `anchor:` schema as the seed set, recorded in `docs/researches/embeddings-benchmark-queries.tsv`. Frozen before the run.

## 7. Harness

In-worktree, temporary, reverted before any merge to main (clearly marked `TEMP EXPERIMENT`):

- `src/handlers.rs` - `SearchMode::Vector` enum variant; vector-only branch in `run_search` (single-retriever path, no RRF; uses `vector_search` with cosine ranking, recency boost still applied so the mode is comparable to the others on the same metric stack).
- `src/handlers.rs` - `resolve_effective_mode` consults `POND_SEARCH_MODE` env var (`fts` | `vector` | `hybrid`) before falling back to the production decision. Unset = production behavior.
- `bench/embeddings/run.sh` - shell driver that takes a queries.tsv and a mode, calls `pond search --format json --limit 20` per query, writes one JSON file per query to a results directory.
- `bench/embeddings/score.py` - reads a results directory and queries.tsv, computes Success@3 / P@1 / MRR per stratum with Wilson 95% CIs, writes a Markdown table and a CSV. Uses only the standard library.

The data dir is the user's real `~/.local/share/pond` - the experiment reads from it but does not mutate session content. `pond embed` does mutate the `vector` column; that mutation is the experiment's input, not its side effect.

## 8. Execution steps

1. Capture corpus snapshot stats (`pond status`) and freeze under `docs/researches/embeddings-benchmark-snapshot.txt`.
2. Build release in the worktree (`cargo build --release`) with the TEMP harness applied.
3. **Phase 1 (FTS baseline, no embed)**: run the seed query set under `POND_SEARCH_MODE=fts`, score, write `bench/embeddings/results/phase1-fts/`. Sanity-check Success@3 numbers against the tokenizer-experiment-report baseline.
4. **Phase 2 (small-scale three-mode probe)**: `pond embed --limit 50000`, then run the seed set under `fts`, `vector`, `hybrid`. Score. Confirm the harness handles all three modes; spot-check that any per-query result list is sensible. (50k rows = 4% of corpus; the test is "does the pipeline work end to end", not "does it answer Q-hybrid-vs-fts yet".)
5. **Phase 3 (full embed)**: `pond embed` to completion. Record start/end time, total rows embedded, throughput. Capture any errors.
6. **Phase 4 (full benchmark)**: re-run the seed query set under all three pond modes on the fully-embedded corpus, plus the kb MCP baseline. Score per stratum. Compute Wilson CIs and paired sign tests pairwise across the three pond modes and against kb. Write `docs/researches/embeddings-benchmark-results-{fts,vector,hybrid,kb}.json` (raw per-query JSON, one file each) and `bench/embeddings/results/phase4/scores.csv`.
7. **Phase 5 (extension, conditional)**: if any Phase 4 stratum is too underpowered to draw a conclusion (n<10), add extension queries until n>=12 per stratum, re-run.
8. **Revert harness**: drop TEMP harness commits from the worktree before any merge. Confirm `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` clean on main.
9. Write the report: per-stratum tables, the three Q-* verdicts, the recommended search-mode default, honest limits (single corpus, pilot scale, one user's query style).

## 9. Report structure

`docs/researches/embeddings-benchmark-report.md`: question; corpus snapshot; modes; per-stratum result tables (English 5, Ukrainian 3) for each mode with CIs and paired-test deltas; the Q-hybrid-vs-fts, Q-hybrid-vs-vector, Q-vector-vs-fts verdicts; the recommended default search mode; the embedding-throughput numbers; honest limits.

All raw JSON, the queries.tsv, the scorer output CSVs, and a corpus snapshot file are kept under `docs/researches/embeddings-benchmark-*` so a future reader (or the agent-session retrieval paper of `agent-session-retrieval-and-evaluation.md` Section 6) can reproduce the run and re-score under a different metric without re-running pond.
