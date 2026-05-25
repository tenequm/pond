# Embeddings benchmark

Search-quality regression harness for pond's hybrid (FTS + Vector + RRF) retrieval.

## Files

- `queries-en.tsv` - 21 English seed queries
- `queries-uk-translated.tsv` - 21 EN queries translated to Ukrainian against EN session targets; the cross-lingual benchmark
- `run.sh <mode> <queries.tsv> <out_dir> [limit]` - executes a single retrieval mode against a query set, dumping one wire envelope per query
- `run-grouped.sh` - same as `run.sh` but with `--group-by-conversation`
- `score.py` - reads run output, scores against ground truth, emits S@3 / P@1 / MRR plus paired sign tests
- `simulate_fusion.py` - replays archived per-arm JSONs through arbitrary fusion variants without re-running pond
- `verify-anchors.sh` - sanity check before locking a new query set (see below)
- `fixtures/` (gitignored) - operator-local arm outputs captured at production pool sizes; regenerate with `run.sh` before using `simulate_fusion.py`
- `results/` (gitignored) - operator-local benchmark runs; regenerate with `run.sh`

`fixtures/` and `results/` are NOT checked in: every JSON envelope captures full message text from the operator's local pond corpus, which contains API keys, wallet addresses, and private project paths that appeared in indexed conversations. Always regenerate locally rather than sharing these directories.

## Build

Plain `cargo build --release` is sufficient. The harness scripts pass `--mode <fts|vector|hybrid>` to `pond search`; embeddings must be enabled in your local pond config for vector/hybrid runs to succeed.

## Workflow for a new benchmark

1. Write queries in the TSV format `id\tlang\tstratum\tquery\tground_truth`.
2. Run `verify-anchors.sh <new_queries.tsv>` BEFORE running any retrieval mode. If any anchor is unreachable (target not in either FTS or Vector top-200), fix the query or drop it. Wave 3 of the hybrid redesign cost a week because 18 of 18 UK queries had anchors that literally did not exist in the corpus - a fault that was invisible until brute-forced.
3. Cross-check against the kb MCP for the same anchors: `mcp__kb__kb_search(query=<anchor>, project=pond, min_score=0.3)`. If kb finds nothing either, your seed phrase is fictional. (kb runs hybrid with denser models, so kb finding nothing is a strong existence signal.)
4. Capture arm fixtures at production pool sizes: `run.sh fts <queries.tsv> fixtures/<name>/fts 100` and `run.sh vector <queries.tsv> fixtures/<name>/vector 200`. The pool sizes mirror `handlers.rs:plan_search` (FTS `pool=100`, Vector `vector_pool=200`).
5. Run the hybrid mode for the actual benchmark numbers: `run.sh hybrid <queries.tsv> results/<run>/hybrid 20`.
6. Score: `python3 score.py <queries.tsv> results/<run>/hybrid <label> /tmp/<run>-ranks.csv`.

## Simulator (`simulate_fusion.py`)

Replays archived per-arm fixtures through any fusion function in seconds. Use this BEFORE changing fusion math in `src/handlers.rs`. The simulator:

- Loads `fts_dir/<id>.json` and `vec_dir/<id>.json` per query.
- Applies a fusion variant (RRF, weighted asymmetric RRF, convex combination, CombANZ, confidence-gated, the production router).
- Optionally applies the recency boost (production applies it post-fusion in `handlers.rs:1336-1340`).
- Scores against the same ground-truth schemes as `score.py`.

Usage: `simulate_fusion.py <queries.tsv> <fts_dir> <vec_dir> [now_iso]`. Pass `now_iso` (e.g. `2026-05-24T00:00:00Z`) to enable recency.

### Pool-size invariant - read before trusting simulator predictions

Production hybrid runs `fts_search(query, pool=100, filter)` and `vector_search(vec, vector_pool=200, filter)` internally, then fuses those candidates. If you simulate against arm JSONs captured at `pond search --limit 20`, you only see the top-20 from each arm: for queries where a noise session sits at rank 30-50 in one arm, the simulator never sees the cross-arm agreement signal but production does. The simulator becomes optimistic and confidence-gating ideas that look like wins under truncated fixtures regress in production.

Always capture fixtures at `--limit 100` for FTS and `--limit 200` for Vector before drawing conclusions. Re-verify by comparing simulator's per-query rank against a production phase run; if they diverge by more than ~1 on average, the fixtures are wrong.

## Query expansion is a caller-layer concern, not a pond flag

pond ships no `--expand` flag. Bare-keyword queries (the weakest stratum, 2/6 S@3 on the current benchmark) underperform because the corpus uses richer vocabulary than the query supplies, not because pond's retrieval has a fixable bug. The right place to expand a query is the agent that's calling pond: it has the LLM cognition to generate paraphrases, synonyms, and disambiguating context that pond's substrate cannot.

Pattern for an agent layer that needs better bare-keyword recall:

1. Run the literal user query.
2. If the user (or the agent's downstream task) is unsatisfied with the top-3, rephrase with 2-3 added content words ("Lance manifest" -> "Lance dataset manifest commit version") and re-run.
3. Optionally union the result lists and dedupe by `session_id`.

The substrate stays minimal (one query, one search, deterministic output); the cognition stays at the agent layer where it belongs.

Why this is not a pond flag:
- Lexical expansion (stem variants, stopword removal) is forbidden by `spec.md#language-neutral-index` because per-language transforms silently corrupt other languages.
- Semantic expansion requires an LLM call; pond's embedder is a sentence transformer (e5-base), not a generative model. Wiring an LLM into `pond search` would couple substrate to a generative dependency the spec deliberately omits.
- Pool depth (`pool=100`, `vector_pool=200`) is already tuned to surface near-misses; making it configurable would invite "just bump it" without addressing the actual gap (which is embedding quality, not pool size).

## Stratum performance (current)

EN-original (n=21), hybrid mode:

| Stratum          | n | Hybrid S@3 |
|------------------|---|------------|
| natural-language | 5 | 5/5        |
| symbol-lookup    | 4 | 4/4        |
| error-message    | 3 | 3/3        |
| conceptual       | 6 | 6/6        |
| bare-keyword     | 3 | 1/3        |

UK-translated (n=21), hybrid mode: 11/21. The agent-layer bilingual-probe pattern (issue both EN and UK probes, union by `session_id`) lifts this to 18/21 - see `docs/researches/embeddings/redesign.md`.

Bare-keyword is the only stratum below 80%. Confidence-gating, weight tuning, and convex combination have all been simulated against pool-sized fixtures (see `docs/researches/embeddings/redesign.md`); none recover bare-keyword without regressing another stratum. The combined ceiling is structural to the corpus + e5-base embeddings.
