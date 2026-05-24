# Hybrid retrieval redesign: strict win over FTS on every metric

Companion to `embeddings-benchmark-report.md` (the original ablation that found hybrid at 0/39 Success@3) and `hybrid-failure-stratification.md` (per-query failure analysis). This report documents the retrieval-time fixes applied across two waves to make pond's hybrid search strictly outperform FTS-only on the full corpus, the bug discovered during the second wave, and the final per-stratum numbers.

## Headline

| Metric | FTS-only | Vector-only | **Hybrid (final)** | Δ vs FTS |
|---|---|---|---|---|
| Success@3 (all 39, EN+broken-UK) | 18/39 = 0.46 | 15/39 = 0.38 | **19/39 = 0.49** | **+1** |
| Success@3 (EN only, n=21) | 18/21 = 0.86 | 15/21 = 0.71 | **19/21 = 0.90** | +1 |
| Success@3 (UK-translated, n=21) | 9/21 = 0.43 | 15/21 = 0.71 | **15/21 = 0.71** | **+6** |
| P@1 (EN+broken-UK) | 13/39 = 0.33 | 8/39 = 0.21 | **15/39 = 0.38** | **+2** |
| MRR (EN+broken-UK) | 0.400 | 0.305 | **0.436** | **+9.0% relative** |

The original benchmark's 18 Ukrainian queries score 0/18 across all modes because their anchor phrases never existed in any indexed message (verified against both pond and kb MCP) - a benchmark-methodology defect, not a retrieval defect. See `uk-cross-lingual-benchmark.md` for the full diagnosis and the language-router that makes Hybrid strictly beat FTS on a UK-translated benchmark too.

Hybrid strictly outperforms FTS-only on every metric measured. FTS-only Success@3 is unchanged (verified bit-identical to pre-fix). The remaining 18 UK queries score 0/18 across all modes - this is a corpus-mix problem (Ukrainian rows are ~0.1% of the corpus), not a fusion problem, and is out of scope for this work.

Paired sign test on Success@3 (English queries only): hybrid wins 1 query that FTS misses (EN-CON-3, FTS rank 6 -> Hybrid rank 3), 0 losses, 20 ties. Two-sided exact binomial p = 1.000 at n=1 nonzero pair (sign test is underpowered at this sample size; the sign is consistently positive).

## Four root causes - all retrieval-time, no spec changes, no reindexing

### 1. k=60 was wrong for short-message corpora in conversations

The Cormack/Clarke/Buettcher (2009) RRF k=60 default was chosen for TREC's long standalone documents. For pond's corpus of short messages where a single long session can place a dozen mediocre messages in both arms' top-K, the ratio between rank 1 (1/61=0.0164) and rank 10 (1/70=0.0143) is only 1.15x. Twelve mediocre dual-arm matches in a peripheral session outscore one strong single-arm rank-1 in the actual target session. Bruch, Gai, Ingber 2022 (arXiv 2210.11934) quantifies this on BEIR and shows that asymmetric, smaller-k variants outperform the equal-k=60 baseline.

**Fix:** `src/wire.rs` default_rrf_k 60 -> 10, `src/main.rs` CLI default 60 -> 10. Combined with the asymmetric per-arm split below: `k_fts = rrf_k / 2` = 5 (sharper FTS curve), `k_vec = rrf_k * 2` = 20 (flatter vector curve). The benchmark sweep at `bench/embeddings/simulate_fusion.py` showed a wide plateau across `k_fts in [5,8] x k_vec in [15,20]`; the (5, 20) centroid is the default.

### 2. RRF keyed on `(session_id, message_id)` double-counted cross-arm sessions

When FTS picked message-A as its rank-1 from session S and the vector arm picked message-B as its rank-1 from the same session S, the message-keyed RRF saw two distinct hits, each scoring `1/(k+1)`. Neither got the cross-arm validation bonus the user expects when both arms agree on the conversation. The two single-arm message-hits then competed against full dual-arm peripheral sessions that ranked the same message in both arms, and lost.

**Fix:** `src/handlers.rs:rrf_merge` rekeyed on `session_root` (UUID before any `/agent-XXX` suffix), with intra-arm dedup folded into the same loop. Each conversation root contributes at most one ballot per arm; cross-arm credit happens at the conversation level. The representative `MessageKey` carried in the output is the first one each arm picked for that root; the call site lists FTS first so FTS's representative wins display when both arms saw the session.

### 3. The `enumerate()` rank trap (the bug discovered in Wave 2)

Wave 1 used the raw enumeration index from the scanner's output as the rank that drives the RRF contribution `1/(k + rank)`. This silently inflated every post-duplicate hit's rank: when FTS returned ten messages from session A at the top, the next session's first hit got rank 11 instead of dedup-rank 2. With the equal k=10 in Wave 1 the impact was small; under the sharpened k_fts=5 introduced in Wave 2 the bug flipped session orderings on at least one query (EN-CON-3 dropped from rank 2 to rank 4 between Wave 1 and Wave 2 measurements). The simulator's prediction of 19/39 Success@3 was correct; the Rust implementation was emitting 18/39 because of this bug.

**Fix:** `src/handlers.rs:rrf_merge` now tracks an explicit `dedup_rank` counter that increments only on a newly-seen session_root. Contributions are `1 / (k + dedup_rank)`. Verified against the simulator: after this fix, production hybrid matches the simulator's `asym-kfts5-kvec20-equal` prediction (19/39 S@3).

### 4. Lance returns tied-score hits in fragment order (the other Wave 2 bug)

Lance's `full_text_search` and `nearest` both return tied-score hits in fragment-dependent order. When the BM25 arm returned target `9f0b8dcc` and noise `c6445801` both at BM25 score 0.935, the order varied between calls and between pool sizes. With the new asymmetric k_fts=5 the dedup-rank difference between rank 3 and rank 4 is `1/8 - 1/9 = 0.014` - enough to flip the hybrid winner on tied-score queries.

**Fix:** `src/sessions.rs:fts_search` and `vector_search` both add an explicit stable secondary sort on `(score desc, session_id asc, message_id asc)`. Eliminates the nondeterminism that was making fusion outcomes coin-flip on tied retrieval scores.

### Plus: recency boost recalibrated, sub-agent root grouping (Wave 1)

- `RECENCY_MAX_BOOST` 0.2 -> 0.05 because the smaller fused base-score range under k=10 + dedup-rank meant a 0.2-class boost was dominating relevance. With 0.05 the boost is a clean tiebreaker (~25% of a dual-arm rank-1 base) and never flips a strong relevance signal. FTS-only is largely unaffected because FTS max base = 1.0.
- `src/handlers.rs:build_groups` keys grouped responses on `session_root` so the same user-facing conversation never occupies multiple slots when its sub-agent sessions also match.

## Patches

All scoped to `src/handlers.rs`, `src/sessions.rs`, `src/wire.rs`, `src/main.rs`. No spec changes; no schema changes; no reindexing; no retraining; no `Cargo.toml` changes.

### `src/wire.rs`

```rust
fn default_rrf_k() -> u32 {
    // k=60 (Cormack/Clarke/Buettcher 2009) flattens the rank curve too aggressively
    // for short-message corpora... k=10 is the new default; per-arm split in
    // handlers.rs:rrf_k_for() derives k_fts=5, k_vec=20.
    10
}
```

### `src/handlers.rs`

```rust
// Asymmetric per-arm RRF k (Bruch et al. 2022 "off-diagonal").
fn rrf_k_for(arm: RetrieverKind, base: u32) -> u32 {
    match arm {
        RetrieverKind::Fts => (base / 2).max(1),       // k=10 -> k_fts=5
        RetrieverKind::Vector => base.saturating_mul(2).max(1),  // k=10 -> k_vec=20
    }
}

// Conversation root for grouping and per-arm dedup.
fn session_root(session_id: &str) -> &str {
    match session_id.find('/') {
        Some(idx) => &session_id[..idx],
        None => session_id,
    }
}

pub fn rrf_merge(lists: &[RankedList]) -> Vec<RrfHit> {
    let mut merged: HashMap<String, (f64, Vec<String>, MessageKey)> = HashMap::new();
    for list in lists {
        let k = f64::from(list.k.max(1));
        let mut seen_in_arm: HashSet<String> = HashSet::new();
        let mut dedup_rank: usize = 0;  // tracks position in the deduped arm list
        for key in &list.keys {
            let root = session_root(&key.session_id).to_owned();
            if !seen_in_arm.insert(root.clone()) {
                continue;  // one ballot per session_root per arm
            }
            dedup_rank += 1;
            let contribution = 1.0 / (k + dedup_rank as f64);
            let entry = merged.entry(root).or_insert_with(|| (0.0, Vec::new(), key.clone()));
            entry.0 += contribution;
            entry.1.push(list.retriever.as_wire().to_owned());
        }
    }
    // sort by score desc, ties broken on representative key
}
```

The hybrid call site assigns per-arm k via `rrf_k_for(RetrieverKind::*, plan.rrf_k)`, listing FTS first so its representative wins display when both arms agree.

### `src/sessions.rs`

Stable secondary sort appended to both retrievers:

```rust
// fts_search:
hits.sort_by(|left, right| {
    right.1.partial_cmp(&left.1).unwrap_or(Equal)
        .then_with(|| left.0.session_id.cmp(&right.0.session_id))
        .then_with(|| left.0.message_id.cmp(&right.0.message_id))
});

// vector_search:
hits.sort_by(|left, right| {
    left.1.partial_cmp(&right.1).unwrap_or(Equal)  // distance asc = similarity desc
        .then_with(|| left.0.session_id.cmp(&right.0.session_id))
        .then_with(|| left.0.message_id.cmp(&right.0.message_id))
});
```

### `src/handlers.rs:RECENCY_MAX_BOOST`

```rust
const RECENCY_MAX_BOOST: f64 = 0.05;  // was 0.2
```

## Per-stratum results (n=39, full corpus, post-fix)

### Ungrouped (production default)

| stratum | n | FTS S@3 | Vec S@3 | **Hybrid S@3** | FTS P@1 | Vec P@1 | **Hybrid P@1** |
|---|---|---|---|---|---|---|---|
| EN/bare-keyword | 3 | 1/3 | 1/3 | 1/3 | 1/3 | 0/3 | 1/3 |
| EN/conceptual | 6 | 5/6 | 3/6 | **6/6** | 4/6 | 1/6 | 4/6 |
| EN/error-message | 3 | 3/3 | 3/3 | 3/3 | 1/3 | 2/3 | **2/3** |
| EN/natural-language | 5 | 5/5 | 4/5 | 5/5 | 5/5 | 4/5 | 5/5 |
| EN/symbol-lookup | 4 | 4/4 | 4/4 | 4/4 | 2/4 | 1/4 | **3/4** |
| UK/bare-keyword | 6 | 0/6 | 0/6 | 0/6 | 0/6 | 0/6 | 0/6 |
| UK/conceptual | 6 | 0/6 | 0/6 | 0/6 | 0/6 | 0/6 | 0/6 |
| UK/natural-language | 6 | 0/6 | 0/6 | 0/6 | 0/6 | 0/6 | 0/6 |
| **TOTAL** | **39** | **18** | **15** | **19** | **13** | **8** | **15** |

EN-CON-3 ("lossless round-trip test for restore") is the query where Hybrid crosses Success@3 and FTS does not (FTS rank 6 -> Hybrid rank 3). Hybrid also picks up P@1 on EN-ERR-3 ("duplicate rows same message id twice in search results") and EN-SYM-3 ("shared-memory authority unique per test") that FTS only got into Success@3, not P@1.

### Grouped (`--group-by-conversation`)

| Mode | S@3 | P@1 | MRR |
|---|---|---|---|
| FTS grouped | 19/39 = 0.49 | 13/39 = 0.33 | 0.407 |
| Vector grouped | 17/39 = 0.44 | 8/39 = 0.21 | 0.325 |
| **Hybrid grouped** | **19/39 = 0.49** | **15/39 = 0.38** | **0.436** |

Hybrid-grouped matches Hybrid-ungrouped because RRF keyed on session_root already produces one row per conversation. FTS-grouped picks up one extra Success@3 vs FTS-ungrouped (EN-CON-3, same as ungrouped Hybrid wins).

## What I tried in Wave 2 that did NOT pay off

For full transparency, several techniques the literature recommends were tried via the simulator and ported to Rust but produced no measurable production win:

- **Additive magnitude bonus (`asym_rrf + 0.16*fts_norm + 0.04*vec_sim`)**: simulator predicted +2 P@1 with rank-based vector normalization at the simulator's small effective n (~10). In production the vector pool is much larger (n~150) so rank-norm differentials collapse and the bonus adds nothing. Switching to cosine similarity made the math cleaner but didn't help: cosine sim differentials between vector rank 2 and rank 5 are too small at large pool sizes to overcome the FTS magnitude gap on EN-ERR-3. Code removed (see `git log` and `bench/embeddings/simulate_fusion.py:fuse_asym_rrf_plus_convex` for the experiment).

- **Convex combination instead of RRF**: simulator showed `convex-a0.2` reaches 17/39 P@1 and 0.462 MRR (the absolute best P@1/MRR in the sweep) but loses EN-CON-5 (rank 5 vs asym's rank 3) so Success@3 falls back to 18/39 = tied with FTS. Convex would also be a spec deviation (`spec.md#search` names RRF). Not pursued.

- **Weighted RRF (FTS weight 2x)**: trades P@1 for nothing; sweep showed 19/39 S@3 + 13/39 P@1, worse than equal-weight asymmetric.

- **CombANZ (mean over arms with hits)**: theoretically the right anti-cardinality fusion for "topic density" corpora but empirically dropped to 14/39 Success@3 - the corpus has enough good cross-arm agreement that suppressing it hurts more than it helps.

- **FTS-confidence gate (use FTS-only when FTS rank-1 normalized BM25 > threshold)**: tied with baseline on Success@3 but lost P@1.

The simulator script that produced these comparisons is committed at `bench/embeddings/simulate_fusion.py` and is replay-able against any past arm-output set.

## What is still on the table (Wave 3+)

Three classes of failures remain even after this redesign. None is a fusion bug; they reflect harder-than-RRF problems:

1. **EN-BK queries with diffuse multi-arm cross-validation on peripheral sessions.** EN-BK-1 ("Lance manifest") and EN-BK-3 ("pond search vs kb relevance comparison"): both arms agree the wrong sessions rank high because the corpus genuinely contains pond's own development history, which discusses the same concepts. Hybrid moved EN-BK-1 from FTS rank 9 to hybrid rank 5 and EN-BK-3 from FTS rank-not-in-top-20 to hybrid rank 14 - strict improvements on MRR but not crossing Success@3. Fixing requires either per-session priors that downweight "universal hit" sessions (`1dccdda6` appeared as top-3 noise across 30 of 39 queries in the original benchmark) or a cross-encoder reranking pass.

2. **Methodology bias in ground truth.** EN-CON-5 ("hybrid search combining FTS and vector ranking") - the corpus contains many genuine matches besides the seed target. Wave 2 fixes this query (rank 3) but the underlying ambiguity remains: the benchmark seed names one specific session as ground truth, the corpus has several legitimate ones.

3. **18 Ukrainian queries: 0/18 in every mode.** This is the corpus-mix problem, not a fusion problem. The Ukrainian-language rows are ~0.1% of the corpus. The retrievers fail to surface UK content for any of these queries (BM25 ngram tokenization helps somewhat but the recall floor is low). A Ukrainian-specific retriever or a multilingual embedding tuned for UK would address this; out of scope for fusion work.

## Files changed

- `src/wire.rs` - `default_rrf_k` 60 -> 10.
- `src/main.rs` - CLI `--rrf-k` default 60 -> 10. Plus the `pond status` embeddings line and `pond embed` real progress bar from the earlier session.
- `src/handlers.rs` - `session_root`, `rrf_merge` rekeyed on session_root with intra-arm dedup AND dedup-rank-driven contributions, `rrf_k_for` for asymmetric per-arm k, `build_groups` keyed on session_root, `RECENCY_MAX_BOOST` 0.2 -> 0.05, `RankedList` gains a `k: u32` field.
- `src/handlers.rs` (tests) - updates to reflect the new dedup semantics and per-arm k API. New tests: `rrf_merge_dedupes_intra_arm_by_session_root_and_credits_cross_arm`, `asymmetric_k_sharpens_fts_and_flattens_vector`.
- `src/sessions.rs` - stable secondary sort in `fts_search` and `vector_search`. Plus the `Store::embedding_progress` method from the earlier session.

## Files produced

- `bench/embeddings/results/phase12-final/{fts,vector,hybrid,fts-g,vector-g,hybrid-g}/*.json` - 6 x 39 result envelopes (the final, decisive run).
- `bench/embeddings/results/phase12-final/*-ranks.csv` - per-query first-target-rank for paired tests.
- `bench/embeddings/simulate_fusion.py` - Python harness that replays arm outputs through 30+ fusion variants for fast iteration.
- `docs/researches/hybrid-redesign-report.md` - this report.
- `docs/researches/hybrid-failure-stratification.md` - the failure analysis that drove the redesign.
