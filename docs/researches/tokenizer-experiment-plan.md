# Tokenizer experiment plan: bilingual FTS for pond

Status: plan. Drives a controlled experiment; the result is a separate report doc.
Worktree: `worktree-tokenizer-multilingual-experiment`. Not spec - `docs/spec.md` stays source of truth.

## 1. Question

pond's FTS index uses an `ngram` tokenizer fixed at 3-grams. A prior ad-hoc test showed it
fails symbol-lookup queries badly. We must pick the FTS tokenizer configuration that serves
pond's real corpus best - which is bilingual (English-dominant + Ukrainian) and code-mixed -
and prove the choice with a methodology that does not hide per-stratum regressions.

Two sub-questions, both in scope:
- Q-tok: which single-index tokenizer config wins?
- Q-dual: does a dual word+ngram index, RRF-fused, beat the best single index?

## 2. Research-derived constraints

Four parallel literature reviews (full findings archived in the report appendix). What each
pins down for this experiment:

- Evaluation (Buckley & Voorhees SIGIR 2000/2002; "Lost in the Middle" 2307.03172; Craswell
  2009; stratified-eval work): a single MRR averaged across heterogeneous query styles is an
  invalid headline - it mixes strata with different relevance structure and hides regressions.
  Primary metric must be **Success@3** (target in top 3 - ranks 4+ barely affect an agent that
  injects 1-3 sessions), reported **per stratum** with 95% Wilson CIs. MRR is kept only as a
  per-stratum supplementary number for the single-answer styles. ~30 queries/stratum is the
  bar for a 10-point delta to be credible; below that, results are labeled directional.
- N-gram length (McNamee & Mayfield 2004; CLEF 2003; SIGIR 2009): n=4 and n=5 are the
  empirical optimum across ~18 languages; n=3 is documentably the worst tested length; n>=6 is
  worse. Short tokens vanish at a fixed n>=4 (a 4-char token yields one 4-gram, a 2-char token
  zero), so a usable ngram config needs a **range with min=3**. Word+ngram combination beats
  either alone by ~10% MAP - but in morphologically-rich / cross-language settings.
- Rank fusion (Cormack et al. SIGIR 2009; Beitzel et al. JASIST 2004; Ng & Kantor 2000):
  RRF gains require retriever **diversity** (low rank-correlation). Fusing two lexical BM25
  variants over the same corpus is correlated for clean English -> ~0 gain (Beitzel, direct
  analogue). Genuine complementarity exists only for Ukrainian-inflected and identifier/typo
  queries. So the dual index must be judged **per stratum**, with the ngram arm weighted ~0.4.
- Multilingual practice (tantivy/Lance specifics): `simple` tokenizer segments Ukrainian
  correctly except it splits on the Ukrainian apostrophe (комп'ютер) - a narrow known flaw.
  `lower_case` ON is essential and Unicode-safe; `ascii_folding` ON is harmless to Cyrillic and
  helps Latin. English Snowball stemming is safe on a mixed field (disjoint scripts). No
  Ukrainian stemmer exists in the tantivy/rust-stemmers ecosystem.

Feasibility check against Lance `InvertedIndexParams` (verified in lance-index 2.0.1; sources
also at `~/pjv/`): exposes base_tokenizer (simple/whitespace/raw/ngram), ngram min/max,
lower_case, ascii_folding, stem, remove_stop_words, language. It does NOT allow a custom
camelCase/snake_case token filter, nor a Ukrainian stemmer. Those two research-suggested arms
are therefore **cut as infeasible** in pond's stack.

## 3. Corpus

A fixed bilingual corpus, ingested once into a throwaway pond data dir.

- English: the `pond` project's Claude Code sessions - 349 session files, ~355 MB. Coding
  work, all 5 query styles present, known target conversations.
- Ukrainian: the full nanoclaw agent corpus pulled from `bl:~/pj/nanoclaw` - 21 session
  files, ~27 MB, genuine heavily-inflected conversational Ukrainian + code-mixing.
- Ratio ~13:1 English:Ukrainian by size - English-dominant (realistic for pond) while leaving
  Ukrainian large enough to retrieve against. Combined ~382 MB, staged at
  `/tmp/pond-mle/corpus/projects/`, ingested via `pond sync claude-code --source-dir`.

## 4. Tokenizer matrix

Cut before testing, with reason: `ngram` >=6 (proven worse); `whitespace`/`raw` tokenizers
(strictly worse than `simple`); Ukrainian stemmer arm (no stemmer exists); custom
camelCase-splitting arm (not expressible via Lance's API). `ngram 3-3` is kept only as the
control that reproduces today's production behavior.

| ID | base | config | rationale |
|----|------|--------|-----------|
| T0 | ngram | min=3 max=3 | control = current production |
| T1 | ngram | min=3 max=5 | literature-optimal ngram; min=3 keeps short tokens |
| T2 | ngram | min=4 max=5 | purer discrimination; tests the cost of dropping short-token coverage |
| T3 | simple | lower_case + ascii_folding | word tokenizer, no stemming - language-neutral baseline |
| T4 | simple | lower_case + ascii_folding + English stem | word + English morphology; Ukrainian left intact (disjoint scripts) |
| T5 | dual | RRF(T4 word, T1 ngram), ngram weight 0.4, k=60 | the word+ngram "have both" arm |

`lower_case` is ON for every arm. T5 is not a separate index build: it is computed offline by
RRF-merging the per-query result lists already produced by the T4 and T1 runs - so the
experiment performs 5 index builds, not 6, and T5 needs no pond code change.

## 5. Metric and methodology

- Unit of truth: each query has one designated target message id (the message that best
  answers it). A query may list a few acceptable target ids where genuinely ambiguous.
- Primary: **Success@3** - target id in the top 3 hits. Per stratum, with 95% Wilson CIs.
- Supporting: **P@1** per stratum. Supplementary: **MRR** per stratum (single-answer styles).
- Reporting: one row per stratum, never a cross-style average as a headline. If one summary
  scalar is unavoidable it is a population-weighted harmonic mean, explicitly labelled.
- Significance: per-stratum paired sign test, T-variant vs T0. Strata with n<30 are labeled
  "directional, underpowered"; CIs are always shown so the reader sees the uncertainty.
- The bare-keyword stratum is reported on equal footing with every other - the prior
  experiment's hidden bare-keyword regression is exactly the failure this structure prevents.

## 6. Query sets

Eight strata. English (coding corpus) supports all five styles; the Ukrainian corpus is
conversational, so it supports three.

- English: natural-language, conceptual, symbol-lookup, error-message, bare-keyword.
- Ukrainian: natural-language, conceptual, bare-keyword.

Target ~12 queries/stratum (~96 total) - honest pilot scale; every stratum reported with CIs
and the directional/underpowered label per Section 5.

Construction:
- symbol-lookup / error-message / bare-keyword: the query carries the literal key token(s) -
  that is how these are really searched. Target = the message that defines/diagnoses it.
- natural-language / conceptual: the query is a natural question or concept phrase; target =
  the message that answers it.
- Ukrainian bare-keyword is the crux of the multilingual test: the query is deliberately
  written in a different morphological form (case/number) than the form in the target
  message. An ngram index should bridge the inflection; a word-only index should not. This is
  the single most important measurement in the experiment.
- Ground truth is read from the source JSONL (Claude Code record `uuid` = pond message id),
  so targets are pinned without depending on the tokenizer under test.

Query sets are frozen to `docs/researches/tokenizer-experiment-queries.tsv` before any run,
so no config can be tuned against them.

## 7. Harness

In-worktree, temporary, reverted before final gates (clearly marked `TEMP EXPERIMENT`):
- `src/sessions.rs` `ensure_indices`: read tokenizer config from env (POND_EXP_TOK,
  POND_EXP_NMIN, POND_EXP_NMAX, POND_EXP_FOLD, POND_EXP_STEM, POND_EXP_LANG), defaulting to
  today's ngram 3-3.
- `src/substrate.rs` `ensure_index`: drop skip-if-exists + `replace=true`, so a rebuild
  re-applies the current env config in place.
- `src/main.rs`: a temporary `Reindex` command calling `store.index_upkeep()`.
- Data dir is a copy, never production. Build once (release); each variant = set env +
  `pond reindex` + run the frozen query set, captured as JSON.

## 8. Execution steps

1. Ingest the bilingual corpus into `/tmp/pond-mle/data` (fresh).
2. Freeze the 8-stratum query set with ground-truth message ids.
3. Apply the harness; build release once.
4. For T0..T4: set env, `pond reindex`, run all queries `--format json`, capture.
5. Score Success@3 / P@1 / MRR per stratum; compute T5 offline by RRF-merging T1+T4.
6. Per-stratum significance (sign test vs T0) and Wilson CIs.
7. Revert the harness; confirm `cargo test`/`clippy`/`fmt` green.
8. Write the report: per-stratum tables, the winning config, the dual-index verdict, and a
   concrete recommendation for pond's `ensure_indices`.

## 9. Report structure

`docs/researches/tokenizer-experiment-report.md`: question; corpus; matrix; per-stratum
result tables (English 5, Ukrainian 3) with CIs; the Q-tok and Q-dual verdicts; the
recommended `ensure_indices` config; honest limits (pilot query counts, single-corpus).
