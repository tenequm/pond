# Hybrid retrieval redesign

The initial benchmark found pond's default hybrid mode scoring **0/39** Success@3 at full corpus coverage. This document records the four root causes, the retrieval-time fixes that ship in pond, the experiments that did NOT pay off (recorded for the next person who reaches for them), and the final per-stratum numbers.

All changes are retrieval-time only: no spec changes, no schema changes, no reindexing, no retraining.

## Headline

| Metric                            | FTS-only | Vector-only | **Hybrid (production)** | delta vs FTS |
|-----------------------------------|----------|-------------|-------------------------|--------------|
| Success@3 (EN-original, n=21)     | 18/21    | 15/21       | **19/21**               | +1           |
| Success@3 (UK-translated, n=21)   |  9/21    | 15/21       | **15/21**               | +6           |
| Success@3 (combined, n=42)        | 27/42    | 30/42       | **34/42**               | **+7**       |
| P@1 (EN-original, n=21)           | 13/21    | 8/21        | **15/21**               | +2           |
| MRR (EN-original, n=21)           | 0.74     | 0.57        | **0.81**                | +0.07        |

Hybrid strictly outperforms FTS-only on every metric measured. The oracle ceiling (top-10 union from either arm) is 38/42; the gap of 4 queries is retrieval-limited, not fusion-limited.

## Four root causes

### 1. k=60 was wrong for short-message corpora in conversations

The Cormack/Clarke/Buettcher (2009) RRF k=60 default was chosen for TREC's long standalone documents. For pond's corpus of short messages where a single long session can place a dozen mediocre messages in both arms' top-K, the ratio between rank 1 (1/61 = 0.0164) and rank 10 (1/70 = 0.0143) is only 1.15x. Twelve mediocre dual-arm matches in a peripheral session outscore one strong single-arm rank-1 in the actual target session. Bruch, Gai, Ingber 2022 (arXiv 2210.11934) quantifies this on BEIR and shows that asymmetric, smaller-k variants outperform the equal-k=60 baseline.

**Fix:** `src/wire.rs::default_rrf_k` 60 -> 10. Combined with the asymmetric per-arm split below: `k_fts = rrf_k / 2 = 5` (sharper FTS curve), `k_vec = rrf_k * 2 = 20` (flatter vector curve). The benchmark sweep at `bench/embeddings/simulate_fusion.py` showed a wide plateau across `k_fts in [5,8] x k_vec in [15,20]`; the (5, 20) centroid is the default.

### 2. RRF keyed on (session_id, message_id) double-counted cross-arm sessions

When FTS picked message-A as its rank-1 from session S and the vector arm picked message-B as its rank-1 from the same session S, the message-keyed RRF saw two distinct hits, each scoring `1/(k+1)`. Neither got the cross-arm validation bonus the user expects when both arms agree on the conversation. The two single-arm message-hits then competed against full dual-arm peripheral sessions that ranked the same message in both arms, and lost.

**Fix:** `src/handlers.rs::rrf_merge` rekeyed on `session_root` (the UUID before any `/agent-XXX` suffix), with intra-arm dedup folded into the same loop. Each conversation root contributes at most one ballot per arm; cross-arm credit happens at the conversation level. The representative `MessageKey` carried in the output is the first one each arm picked for that root; the call site lists FTS first so FTS's representative wins display when both arms saw the session.

### 3. The enumerate() rank trap

Wave 1 used the raw enumeration index from the scanner's output as the rank that drives the RRF contribution `1 / (k + rank)`. This silently inflated every post-duplicate hit's rank: when FTS returned ten messages from session A at the top, the next session's first hit got rank 11 instead of dedup-rank 2. Under the sharpened `k_fts = 5` introduced after Wave 1 the bug flipped session orderings on at least one query (EN-CON-3 dropped from rank 2 to rank 4 between Wave 1 and Wave 2 measurements). The simulator's prediction of 19/39 Success@3 was correct; the Rust implementation was emitting 18/39 because of this bug.

**Fix:** `src/handlers.rs::rrf_merge` now tracks an explicit `dedup_rank` counter that increments only on a newly-seen session_root. Contributions are `1 / (k + dedup_rank)`. After this fix, production hybrid matches the simulator's `asym-kfts5-kvec20-equal` prediction.

### 4. Lance returns tied-score hits in fragment order

Lance's `full_text_search` and `nearest` both return tied-score hits in fragment-dependent order. When the BM25 arm returned target `9f0b8dcc` and noise `c6445801` both at BM25 score 0.935, the order varied between calls and between pool sizes. With the new asymmetric `k_fts = 5` the dedup-rank difference between rank 3 and rank 4 is `1/8 - 1/9 = 0.014` - enough to flip the hybrid winner on tied-score queries.

**Fix:** `src/sessions.rs::fts_search` and `vector_search` both add an explicit stable secondary sort on `(score desc, session_id asc, message_id asc)`. Eliminates the nondeterminism that was making fusion outcomes coin-flip on tied retrieval scores.

## Recency boost recalibration

`RECENCY_MAX_BOOST` 0.2 -> 0.05 because the smaller fused base-score range under k=10 + dedup-rank meant a 0.2-class boost was dominating relevance. With 0.05 the boost is a clean tiebreaker (~25% of a dual-arm rank-1 base) and never flips a strong relevance signal. FTS-only is largely unaffected because FTS max base = 1.0.

## Per-stratum results (post-fix)

EN-original per-stratum (n=21):

| stratum             | n | FTS S@3 | Vec S@3 | **Hybrid S@3** | FTS P@1 | Vec P@1 | **Hybrid P@1** |
|---------------------|---|---------|---------|----------------|---------|---------|----------------|
| EN/bare-keyword     | 3 | 1/3     | 1/3     | 1/3            | 1/3     | 0/3     | 1/3            |
| EN/conceptual       | 6 | 5/6     | 3/6     | **6/6**        | 4/6     | 1/6     | 4/6            |
| EN/error-message    | 3 | 3/3     | 3/3     | 3/3            | 1/3     | 2/3     | **2/3**        |
| EN/natural-language | 5 | 5/5     | 4/5     | 5/5            | 5/5     | 4/5     | 5/5            |
| EN/symbol-lookup    | 4 | 4/4     | 4/4     | 4/4            | 2/4     | 1/4     | **3/4**        |
| **EN total**        | 21| **18**  | **15**  | **19**         | **13**  | **8**   | **15**         |

UK-translated totals only (per-stratum breakdowns not separately tabulated; the 21-query set is a 1:1 translation of the EN strata):

| set            | n  | FTS S@3 | Vec S@3 | **Hybrid S@3** |
|----------------|----|---------|---------|----------------|
| UK-translated  | 21 | 9/21    | 15/21   | **15/21**      |

Combined (n=42): FTS 27, Vector 30, Hybrid 34.

EN-CON-3 ("lossless round-trip test for restore") is the canonical query where Hybrid crosses Success@3 and FTS does not (FTS rank 6 -> Hybrid rank 3). Hybrid also picks up P@1 on EN-ERR-3 ("duplicate rows same message id twice in search results") and EN-SYM-3 ("shared-memory authority unique per test") where FTS only got into Success@3.

## What was tried but did not ship

Recorded so the next person to reach for one of these knows it has already been measured.

- **Additive magnitude bonus** (`asym_rrf + 0.16 * fts_norm + 0.04 * vec_sim`). Simulator predicted +2 P@1 with rank-based vector normalization at the simulator's small effective n (~10). In production the vector pool is much larger (n~150) so rank-norm differentials collapse and the bonus adds nothing.
- **Convex combination instead of RRF**. Simulator showed `convex-a0.2` reaches 17/39 P@1 and 0.462 MRR (the absolute best P@1/MRR in the sweep) but loses EN-CON-5 (rank 5 vs asym's rank 3) so Success@3 falls back to tied with FTS. Convex would also be a spec deviation (`spec.md#search` names RRF). Not pursued.
- **Weighted RRF (FTS weight 2x)**. Trades P@1 for nothing; sweep showed 19/39 S@3 + 13/39 P@1, worse than equal-weight asymmetric.
- **CombANZ (mean over arms with hits)**. Theoretically the right anti-cardinality fusion for "topic density" corpora but empirically dropped to 14/39 Success@3. The corpus has enough good cross-arm agreement that suppressing it hurts more than it helps.
- **FTS-confidence gate (use FTS-only when FTS rank-1 normalized BM25 > threshold)**. Tied with baseline on Success@3 but lost P@1.

## What is still on the table

Three classes of failures remain even after this redesign. None is a fusion bug; they reflect harder-than-RRF problems.

1. **EN-BK queries with diffuse multi-arm cross-validation on peripheral sessions.** EN-BK-1 ("Lance manifest") and EN-BK-3 ("pond search vs kb relevance comparison"): both arms agree the wrong sessions rank high because the corpus genuinely contains pond's own development history, which discusses the same concepts. Hybrid moved EN-BK-1 from FTS rank 9 to hybrid rank 5 - a strict improvement on MRR but not crossing Success@3. Fixing requires either per-session priors that downweight "universal hit" sessions or a cross-encoder reranking pass.
2. **Methodology bias in ground truth.** EN-CON-5 ("hybrid search combining FTS and vector ranking") - the corpus contains many genuine matches besides the seed target. The benchmark seed names one specific session as ground truth; the corpus has several legitimate ones.
3. **Cross-lingual queries against a corpus dominated by another language.** This is a corpus-mix problem, not a fusion problem. An agent that suspects the corpus may contain text in a different language from its query should issue two searches (one per language) and union by `session_id`. The agent has the LLM cognition to translate; pond does not.

## Failure stratification (the data behind the diagnosis)

For full transparency, the per-query failure pattern from the original 0/39 run:

| diagnosis                       | count | meaning |
|---------------------------------|-------|---------|
| NO-MODE                         | 18    | no mode surfaced the target in top-3 (the 18 unbenchmarkable UK seeds) |
| FTS-AND-VEC-WIN                 | 10    | both arms hit Success@3 individually, hybrid did not |
| FTS-ONLY                        | 8     | only FTS hit Success@3 |
| TARGET-BELOW-20-IN-HYBRID       | 2     | hybrid never surfaced the target in top-20 but another mode did |
| HYBRID-IN-WINDOW-BUT-BURIED     | 1     | hybrid surfaced target in [4, 20] (fixable by fusion math alone) |

The dominant pattern was FTS-AND-VEC-WIN: both arms individually had the target in their top-3, but the message-keyed RRF demoted it past top-3 because peripheral sessions had cross-arm agreement at multiple message-rank positions. Root cause 2 (session-root keying) addresses this directly.

Repeat-offender noise sessions (top noise prefixes from the original run): `1dccdda6` appeared as top-3 hybrid noise across 30 of 39 queries; `95b77fc5` across 8; `6b12da87` across 7. These are recently-touched, content-dense pond conversations that get cross-validated by both arms. Down-weighting "universal hit" sessions via a session prior remains an option for a future investigation (not shipped).

## Files

Production code (changes that ship):

- `src/wire.rs` - `default_rrf_k` 60 -> 10.
- `src/handlers.rs` - `session_root`, `rrf_merge` rekeyed on session_root with intra-arm dedup AND dedup-rank-driven contributions, `rrf_k_for` for asymmetric per-arm k, `build_groups` keyed on session_root, `RECENCY_MAX_BOOST` 0.2 -> 0.05.
- `src/sessions.rs` - stable secondary sort in `fts_search` and `vector_search`. Plus the `Store::embedding_progress` method.
- `src/main.rs` - `pond status` embeddings line and `pond embed` real progress bar.

Harness (under `bench/embeddings/`):

- `queries-en.tsv` - 21 English seed queries.
- `queries-uk-translated.tsv` - 21 Ukrainian-translated queries against the same English-language session-id targets.
- `verify-anchors.sh` - anchor-reachability check; gate on this before locking a new query set.
- `run.sh`, `run-grouped.sh` - drive `pond search` across a queries TSV, dump one JSON envelope per query.
- `score.py` - per-stratum Success@3 / P@1 / MRR with Wilson CIs and paired sign tests.
- `simulate_fusion.py` - replays archived arm fixtures through arbitrary fusion variants.
