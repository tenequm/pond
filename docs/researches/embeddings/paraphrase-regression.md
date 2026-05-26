# Paraphrase-set hybrid regression

Companion to `redesign.md`. The redesign tuned RRF on a 21-query EN seed set whose stratum mix (~48% strict-keyword) over-represented the regime where asymmetric `k_fts=5, k_vec=20` wins. A 111-query paraphrase set (`bench/embeddings/queries-paraphrased.tsv`, built 2026-05-25 from 125 sampled real user prompts via parallel paraphraser subagents) revealed that the same configuration is net-negative against vector-alone on the regime the original bench did not cover.

## Headline

| metric | FTS | Vector (e5-base) | Hybrid (production) |
|---|---|---|---|
| S@3 (n=111) | 35/111 (.32) | **79/111 (.71)** | 46/111 (.41) |
| P@1 (n=111) | .21 | .50 | .23 |
| MRR (n=111) | 0.294 | 0.632 | 0.379 |

Paired sign tests on the task-request stratum (n=72) show vector beats hybrid 23-4 (p = 0.000). The gap is statistically unambiguous, not noise. Hybrid drags vector down by 30 points S@3 on the regime that matches realistic agent usage.

## Why the original benchmark missed it

The 39-query EN seed set had p=1.000 across all paired stratum tests after the redesign landed; it lacked the power to detect either improvement or regression. The 200-query verbatim re-ask bench saturated near 95% S@3 for all modes (queries literally appear in the corpus) and is also uninformative. The paraphrase set is the first benchmark with both realistic query distribution and statistical power. The Wave 2 grid search via `simulate_fusion.py` was correct for the data it ran on; the data was wrong.

## Mechanism

`src/handlers.rs:1109-1114` hard-codes `k_fts = base/2`, `k_vec = base*2`. With `default_rrf_k = 10` (`src/wire.rs:539`) this is `(5, 20)`. The per-hit contribution gap is large:

| | rank 1 | rank 10 | rank-1 ratio vs vector rank 1 |
|---|---|---|---|
| FTS (k=5) | 0.167 | 0.067 | 3.5x |
| Vector (k=20) | 0.048 | 0.033 | 1.0x |

Concrete failure shape on a paraphrase query (target at FTS rank 40, Vector rank 1; noise at FTS rank 1, Vector rank 30):

- Noise: `1/(5+1) + 1/(20+30) = 0.187`
- Target: `1/(5+40) + 1/(20+1) = 0.070`

Noise wins 2.7x even though Vector picked the correct answer at rank 1. The asymmetry that lifted EN-CON-3 from FTS rank 6 to Hybrid rank 3 on the literal-keyword regime is the same asymmetry actively suppressing paraphrase queries here.

Contributing factors, priority order:

1. **Tuning corpus bias.** The asymmetric `(5, 20)` plateau was identified on a corpus that was ~48% strict-keyword. The paraphrase set is ~100% natural language; outside the calibration set's support.
2. **ngram-3-5 tokenizer amplifies the wrong arm on long queries.** FTS rank-1 on paraphrase strings is almost always a content-dense "universal hit" session (`1dccdda6`, `95b77fc5`, `6b12da87` from `redesign.md` "Failure stratification"); RRF then hands that wrong-rank-1 the largest contribution in the system.
3. **No caller knob.** `rrf_k_for` hard-codes asymmetry direction. The wire `rrf_k` scales both arms together; nothing flips the shape. Callers cannot route paraphrase intents to a different fusion.
4. **Recency boost interaction.** At 0.05 it is calibrated as a tiebreaker against `k=10` dual-arm rank-1 (~25%). For paraphrase queries the surviving signal sits at Vector rank 5-30 where the boost is the same order of magnitude as the signal.

## What LanceDB officially recommends

Lance OSS docs (`/Users/tenequm/pjv/lance-format/lance/docs/src`) cover BM25, the inverted index, and vector index format; they contain no RRF, reranking, or hybrid-search material at all. All hybrid guidance lives in the LanceDB layer.

| Knob | LanceDB official | pond today |
|---|---|---|
| RRF k | Single `K=60`, "near-optimal but choice not critical" (Cormack et al.) | `k_fts=5, k_vec=20` asymmetric (Bruch 2022) |
| Per-arm asymmetry | Not exposed | Hard-coded in `rrf_k_for` |
| `LinearCombinationReranker` | Deprecated in favor of RRF | Considered/rejected (Wave 2) |
| For better quality on hard queries | Use a stronger reranker (Cohere, cross-encoder, ColBERT) | RRF only |
| Distance metric for normalized vectors | `dot` (best performance) | Worth verifying for our e5-base index |
| e5 query/passage prefix | Not mentioned anywhere | We do it (`src/embed/mod.rs:126-134`) |
| Query-type-aware fusion | Silent | Silent |
| Short-doc / chat-message corpus | Silent | This is our regime |

The crisp framing: LanceDB treats RRF as a coarse fusion default that callers escape from with a relevance-based reranker, not by tuning `k`. Their published evaluation shows Cohere reranker at 0.81 Top-3 vs RRF/LinearCombo at ~0.73 on their corpus. They do not cover the paraphrase / short-message regime, so there is no copy-paste answer; their reach for "fusion is not enough" is a reranker.

## Recommended path

### Step 1 - establish the plateau, no code change

Capture paraphrase arm fixtures at production pool sizes (`bench.py run --mode fts --limit 100` and `--mode vector --limit 200` against `queries-paraphrased.tsv`). Then re-run `simulate_fusion.py` against the new fixtures over a wider grid than Wave 2 used:

- `(k_fts=60, k_vec=60)` - the LanceDB default; baseline for "do nothing fancy" on our regime.
- `(k_fts=20, k_vec=5)` - inverted asymmetry (vector-sharp, FTS-flat).
- `(k_fts=very-large, k_vec=10)` - FTS as weak tiebreaker over vector.
- Symmetric sweep at k in {10, 30, 60, 100}.
- FTS-confidence gate at the new constants. Wave 2 rejected it under `(5, 20)`; the gate has a different shape under symmetric or vector-favoring k and is worth re-measuring.

Ship the first config that pulls hybrid within ~3 points of vector-alone S@3 on the paraphrase set without crashing EN-original numbers. EN-original is no longer statistically load-bearing (paired tests at p=1.000), so any directional non-regression is acceptable.

### Step 2 - expose the knob, do not rebake one global default

A single static fusion cannot win both the literal-keyword and the paraphrase regimes; the redesign already conceded this for the cross-lingual axis. The fix is to thread per-arm k through `SearchRequest` (`src/wire.rs:375-391`) into `rrf_k_for` (`src/handlers.rs:1109-1114`) so the caller can pick. Default stays as today; the `pond_search` MCP description tells callers to flip for paraphrastic / natural-language intents. This is consistent with the existing agent-layer pattern for cross-lingual probing (`redesign.md` "Cross-lingual retrieval is an agent-layer concern").

### Step 3 - if k-tuning plateaus below vector-alone, switch to a reranker

The official escape hatch from "RRF is not enough" is a relevance-based reranker. For pond's local-first posture the equivalent is a cross-encoder (TinyBERT/MiniLM class) over the top-K union at search time. This was the deferred Wave 2 item; it is the next investment if Step 1 confirms the gap cannot be closed by k alone. A spec amendment to name a reranker abstraction would be required.

## Side findings

1. **Verify vector index distance metric.** If `messages.vector` was indexed with cosine on already-L2-normalized e5-base vectors, switching to dot is a free performance win independent of the RRF question. Per `/Users/tenequm/pjv/lancedb/docs/docs/performance.mdx`: "Pick the distance metric based on how the embedding model was trained: cosine (unnormalized), dot (already-normalized, best performance)."
2. **Universal-hit session prior.** Wave 1 identified `1dccdda6`, `95b77fc5`, `6b12da87` as cross-arm noise anchors. On paraphrase queries with weak FTS these are exactly the sessions the noise arm promotes. A per-session prior (decreasing log-probability across recent queries) addresses the cause; k-tuning addresses the symptom. Worth measuring after Step 1.
3. **Status of the 39-query EN bench.** Paired tests at p=1.000 across strata mean it cannot statistically detect either improvement or regression. The paraphrase set is the new primary benchmark; the EN bench remains useful as a directional smoke check, not a tiebreaker.

## Rejected experiments (Wave 2, do not re-litigate without reason)

All measured via `simulate_fusion.py` against 39-query arm fixtures. None beat asymmetric RRF on that set:

- Additive magnitude bonus (`asym + 0.16*fts_norm + 0.04*vec_sim`).
- Convex combination (linear blend of normalized FTS and vector scores).
- Weighted RRF (different per-arm weights).
- CombANZ (mean over arms with hits).
- FTS-confidence gate (skip FTS arm when top BM25 below threshold) - rejected at `(5, 20)` constants; re-measure at the new constants per Step 1.

## Files

- `bench/embeddings/queries-paraphrased.tsv` - 111 paraphrased queries, built 2026-05-25.
- `bench/embeddings/bench.py` - harness; `run --mode {fts|vector|hybrid}`, `score`, `verify`, `pair`.
- `bench/embeddings/simulate_fusion.py` - fusion-variant simulator over captured arm fixtures.
- `src/handlers.rs:1109-1114` - `rrf_k_for` (the hard-coded asymmetry).
- `src/handlers.rs:1481-1524` - `rrf_merge` (session_root keying, dedup-rank).
- `src/wire.rs:375-391`, `src/wire.rs:539` - wire-level `rrf_k`.
- `src/embed/mod.rs:126-134` - e5 `query: ` / `passage: ` prefixes.
