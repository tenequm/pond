# FTS tokenizer selection for a bilingual agent-session corpus

A controlled experiment, compiled 2026-05-22. Companion to
`tokenizer-experiment-plan.md`. This is a research report, not spec -
`docs/spec.md` remains the source of truth. It is written to also serve as
groundwork for a later paper, so methodology and literature are recorded in
full.

## Summary

pond's full-text index uses a character-`ngram` tokenizer fixed at 3-grams.
This report asks which tokenizer configuration serves pond's real corpus best -
a corpus that is English-dominant but contains heavily-inflected Ukrainian and
source-code identifiers - and answers it with four literature reviews and a
controlled six-configuration experiment over a 1.95M-row bilingual corpus.

Findings:

1. The **word tokenizer is ruled out.** It measurably regresses Ukrainian
   retrieval (Success@3 7/18 vs ngram's 9/18, concentrated in the
   natural-language and conceptual strata) because no Ukrainian stemmer exists
   in pond's stack, so word tokens cannot bridge Ukrainian's heavy inflection.
   Multilingual support is a hard requirement, so this is disqualifying.
2. The **dual word+ngram index is ruled out.** RRF fusion of the two showed no
   benefit (Success@3 26/39, below plain ngram's 27-28). This matches fusion
   theory: two lexical retrievers over one corpus are correlated, and rank
   fusion needs retriever diversity to help.
3. **`ngram` is the right base tokenizer**, and widening it from 3-grams to a
   **3-5-gram range** is the recommended change: the IR literature is
   near-unanimous that n=4-5 is optimal and n=3 is the worst tested length,
   while min=3 keeps short tokens searchable. This experiment found the ngram
   widths statistically indistinguishable (no downside to widening) and an
   earlier production diagnostic showed 3-5 fixes a real symbol-query failure.

Recommended change to `ensure_indices`: `ngram_max_length(3)` -> `ngram_max_length(5)`.

## Update (2026-06-18): word tokenizer adopted, superseding finding 1

The remote-read-performance work (`docs/plans/2606-17-remote-read-perf-and-index-cleanup.md`) re-weighted this decision: pond now indexes with the word-level `simple` tokenizer plus English stemming. Finding 1 above (word "ruled out") is superseded - not because the Ukrainian regression went away, but because the trade-off changed:

- The UK regression reproduced almost exactly (Success@3 **7/21 vs ngram 8/21** on the re-run, a single natural-language query; the symbol-lookup, error-message, conceptual, and bare-keyword strata all tie). Cyrillic passes through the English stemmer unstemmed, so it stays exact-matchable - the loss is confined to inflectional natural-language matching, which ngram's sub-word overlap approximated.
- Against that, the word index wins decisively on the English-dominant corpus (**EN Success@3 66/111 vs ngram 31/111, ~2x**) and is **~28x lighter** on disk (41 MB vs 1.14 GB inverted-list file on the 2.06M-message corpus).
- The index weight is the operative constraint: the ngram index dominated remote cold-start (a 175-442 s first-search page-in from S3) and server RAM. The word index removes that.

The 2026-05-22 report treated any Ukrainian regression as disqualifying ("multilingual support is a hard requirement"); the read-path constraints made the English and operational gains decisive over a one-query Ukrainian loss. The spec rule `search-language-neutral-index` is amended accordingly: the invariant is now "no transform that drops or mangles a language's tokens" (a gracefully-degrading stem is allowed), not "no monolingual transform at all."

## 1. Background and question

pond ingests AI coding-agent session transcripts into Lance and serves
full-text (BM25) search over them. The FTS index is a Lance inverted index
(tantivy-backed), configured today as: `base_tokenizer = "ngram"`,
`ngram_min_length = ngram_max_length = 3`, no stemming, no stopword removal -
chosen so the index is language-neutral.

An ad-hoc diagnostic on pond's production corpus had suggested 3-grams fail
badly on symbol-lookup queries (a code identifier such as `Extracted<T>`
fragments into low-IDF 3-grams and loses all discriminative power). Multilingual
retrieval - English and Ukrainian, with code-mixing - is a hard product
requirement. Two questions:

- Q-tok: which single-index tokenizer configuration is best?
- Q-dual: does a dual word+ngram index, RRF-fused, beat the best single index?

## 2. Literature review

Four parallel reviews of the primary IR literature. Each constrained the
experiment design; their findings stand on their own as background.

### 2.1 Character n-gram vs word tokenization

McNamee & Mayfield's CLEF programme (Information Retrieval 2004; CLEF 2003
working notes; SIGIR 2009, 18 languages) is the authority. Findings:

- n=4 and n=5 are the empirical optimum; n=3 is documentably the worst tested
  length; n>=6 is worse. CLEF 2003 across 8 languages shows 3-grams tracking
  near unnormalised words while 4/5-grams diverge upward. Russian: words MAP
  0.255, 4-grams 0.328 (+28%). Finnish 4-grams 0.540 vs words 0.336.
- In highly inflected languages, n-grams beat unnormalised words by >50%
  (SIGIR 2009). Ukrainian is not directly studied but is morphologically
  comparable to Russian (East Slavic, Cyrillic, 7 noun cases).
- Word + n-gram combination beats either alone by ~10% MAP - but the gain is
  realised in morphologically-rich / cross-language settings, not clean English.
- The IDF-flattening mechanism (Church & Gale residual-IDF) explains why short
  n-grams lose discriminativeness: a 3-gram like "ext" or "act" occurs in a
  large fraction of documents, so its IDF is near zero. A 5-char identifier
  yields only three 3-grams, all high-DF; under n>=4 a 4-char token yields one
  4-gram and a 2-char token yields none. Hence the usable ngram config is a
  range with min=3 (keep short tokens) and max=5 (gain discrimination).

### 2.2 Rank fusion and the dual-index question

- RRF (Cormack, Clarke & Buettcher, SIGIR 2009) outperforms individual systems
  by ~4-5% MAP and fixes k=60 - but the paper fused systems from *different
  groups* with different retrieval models. Its own conjecture names retriever
  *diversity* as the mechanism.
- Beitzel et al. (JASIST 2004) is the direct analogue: fusing highly-effective
  retrieval strategies *within one system* (same corpus, same scoring, varying
  only one component) yields "little or no improvement." Ng & Kantor (2000)
  formalise it: fusion beats the better input only when rankings are *dissimilar*
  (high `z`); correlated retrievers sit in the failure region.
- A word-BM25 and an ngram-BM25 index over the same corpus are correlated for
  clean English queries (both retrieve the documents containing the query
  words). Genuine complementarity exists only for Ukrainian-inflected and
  identifier/typo queries. The literature-supported fusion win is lexical+dense
  (Anthropic Contextual Retrieval, 2024: BM25+embeddings cut top-20 failure
  ~49%), not lexical+lexical.

### 2.3 Evaluation methodology

- A single MRR averaged across heterogeneous query styles is an invalid
  headline: it mixes strata with different relevance structure and hides
  per-stratum regressions (Manning/Raghavan/Schutze IIR ch. 8 "what query
  averaging hides"; Voorhees' per-type evaluation in TREC 2003 QA).
- Primary metric: **Success@3** - the target in the top 3. "Lost in the Middle"
  (Liu et al., TACL 2024) shows an agent injecting 1-3 retrieved sessions gains
  almost nothing from ranks 4+. Reported **per stratum**, with 95% Wilson CIs.
- MRR is kept as a per-stratum supplementary number for single-answer styles
  (Craswell 2009). P@1 is a supporting metric.
- Buckley & Voorhees (SIGIR 2000/2002): ~25 queries is a minimum and 50 better
  for a stable comparison; below that, report confidence intervals and treat
  deltas as directional. Paired sign test per stratum.

### 2.4 Multilingual tokenization in practice

- tantivy's `simple` tokenizer segments Ukrainian correctly (Cyrillic is
  alphanumeric) except it splits on the Ukrainian apostrophe (`комп'ютер`).
- `lower_case` is essential and Unicode-safe; `ascii_folding` is harmless to
  Cyrillic (no ASCII equivalent, passes through) and helps accented Latin.
- English Snowball stemming is safe on a mixed-language field because scripts
  are disjoint. **No Ukrainian stemmer exists** in tantivy / rust-stemmers; the
  Russian stemmer would corrupt English on a shared field. So a word index has
  no morphological normalisation available for Ukrainian.
- A custom camelCase/snake_case code-identifier filter would help symbol
  retrieval but is **not expressible** through Lance's `InvertedIndexParams`
  API, which exposes only base_tokenizer / ngram range / lower_case /
  ascii_folding / stem / stopwords. It and a Ukrainian stemmer are infeasible
  and were cut before testing.

## 3. Method

### 3.1 Corpus

A fixed bilingual corpus (preserved at `~/pj/tmp/pond-tokenizer-corpus/`):

- English: the full local Claude Code session set (`~/.claude/projects`), 7840
  session files, 71 projects, ~5.3 GB -> 1,936,302 ingested rows.
- Ukrainian: the complete nanoclaw agent corpus (`bl:~/pj/nanoclaw`), 21
  session files, ~27 MB -> 8,831 ingested rows. Genuine heavily-inflected
  conversational Ukrainian with English code-mixing.

Ingested with the claude-code adapter into one pond data dir. Search ran over
the whole corpus (no project filter); Cyrillic and Latin scripts are disjoint
so cross-language interference is negligible.

### 3.2 Tokenizer matrix

Cut before testing: `ngram` n>=6 (proven worse), `whitespace`/`raw` tokenizers,
the Ukrainian-stemmer arm (no stemmer exists) and the camelCase-filter arm (not
expressible in Lance's API).

| ID | base | configuration |
|----|------|---------------|
| T0 | ngram | min=3 max=3 - the current production control |
| T1 | ngram | min=3 max=5 |
| T2 | ngram | min=4 max=5 |
| T3 | simple | word, lower_case + ascii_folding, no stemming |
| T4 | simple | word, lower_case + ascii_folding + English stemming |
| T5 | dual | RRF(T4 word, T1 ngram), ngram arm weight 0.4, k=60 |

T5 was computed offline by RRF-merging the T1 and T4 result lists, so the
experiment performed five index builds. A temporary, env-driven harness rebuilt
the FTS index in place for each config (reverted after the run; all gates green).

### 3.3 Metric and query set

Primary metric Success@3, supporting P@1 and MRR, all per stratum, with 95%
Wilson CIs and a paired sign test vs T0. Recency boost disabled to isolate the
tokenizer.

39 queries, frozen before any run (`tokenizer-experiment-queries.tsv`):

- English (21): the pre-registered prior benchmark set - 5 styles
  (natural-language, conceptual, symbol-lookup, error-message, bare-keyword),
  ground truth = expected-conversation prefixes.
- Ukrainian (18): purpose-built, 3 styles (natural-language, conceptual,
  bare-keyword), ground truth = anchor phrases resolved to message ids.
  Bare-keyword queries are inflection-stressed: the query uses a different
  morphological form than the target message.

## 4. Results

Success@3 by stratum and configuration:

| stratum | n | T0 ng3-3 | T1 ng3-5 | T2 ng4-5 | T3 word | T4 word+stem | T5 dual |
|---------|---|------|------|------|------|------|------|
| en / natural-language | 5 | 1.00 | 1.00 | 1.00 | 0.80 | 1.00 | 1.00 |
| en / conceptual | 6 | 0.83 | 0.83 | 1.00 | 1.00 | 0.83 | 0.83 |
| en / symbol-lookup | 4 | 1.00 | 1.00 | 1.00 | 1.00 | 0.75 | 1.00 |
| en / error-message | 3 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| en / bare-keyword | 3 | 0.33 | 0.33 | 0.33 | 0.33 | 0.33 | 0.33 |
| uk / natural-language | 6 | 0.50 | 0.50 | 0.50 | 0.33 | 0.33 | 0.50 |
| uk / conceptual | 6 | 0.33 | 0.33 | 0.50 | 0.17 | 0.17 | 0.33 |
| uk / bare-keyword | 6 | 0.67 | 0.67 | 0.50 | 0.67 | 0.67 | 0.50 |
| **English total** | 21 | 18 | 18 | **19** | 18 | 17 | 18 |
| **Ukrainian total** | 18 | **9** | **9** | **9** | 7 | 7 | 8 |
| **Overall** | 39 | 27 | 27 | **28** | 25 | 24 | 26 |

Ukrainian P@1 / MRR (the discriminating side; English P@1/MRR vary only in
small-n noise):

| uk stratum | T0 | T1 | T2 | T3 word | T4 word+stem |
|------------|----|----|----|------|------|
| natural-language MRR | 0.39 | 0.43 | 0.42 | 0.39 | 0.39 |
| conceptual MRR | 0.25 | 0.35 | 0.43 | 0.21 | 0.21 |
| bare-keyword MRR | 0.58 | 0.57 | 0.54 | 0.58 | 0.58 |

Paired sign test vs T0 (Success@3, 39 queries): every config p >= 0.45 - no
result reaches significance at this sample size.

## 5. Findings

**Q-tok. The word tokenizer is disqualified; ngram is the base tokenizer.**
Ukrainian is where the tokenizers separate. The word configs (T3, T4) score
7/18 on Ukrainian against 9/18 for every ngram config - a regression
concentrated in the natural-language stratum (0.50 -> 0.33) and the conceptual
stratum (0.33 -> 0.17). The cause is structural and known in advance: Ukrainian
is heavily inflected and pond's stack has no Ukrainian stemmer, so a word index
stores `протоки`, `протоку`, `протокою` as unrelated tokens and a query in one
case form cannot reach a document in another. Character n-grams bridge those
forms by shared substrings. The word tokenizer also showed minor, inconsistent
English regressions (T3 P@1 on en/natural-language 0.40; T4 broke a symbol
query by stemming the identifier). Since multilingual support is a hard
requirement, the word tokenizer cannot be adopted regardless of its English
behaviour.

Among ngram widths the experiment could not discriminate: T0 (3-3), T1 (3-5)
and T2 (4-5) score 27/27/28 of 39, all sign tests p ~ 1.0. The recommendation
to widen nonetheless rests on convergent external evidence: the IR literature
is near-unanimous that n=4-5 beats n=3 (Section 2.1), and an earlier production
diagnostic showed a 3-5 range resolves the symbol-query failure that 3-grams
exhibit on a larger, noisier (codex-inclusive) corpus. This experiment's role
for that question is to confirm the **absence of any downside**: widening to
3-5 did not regress either language. min stays at 3 so 1-2 character tokens
remain indexable.

**Q-dual. The dual word+ngram index does not help.** T5 scored 26/39 overall -
below plain ngram (27-28) and below ngram on Ukrainian (8/18 vs 9/18). This is
the predicted outcome: word-BM25 and ngram-BM25 over one corpus are correlated
retrievers, and rank fusion needs diversity to add value (Beitzel et al.;
Ng & Kantor). The dual index also costs a second index build and a fused query
path. It is not worth implementing. If pond later wants a fusion win, the
literature points to lexical+dense (BM25 + embeddings), not lexical+lexical.

## 6. Recommendation

Change `ensure_indices` in `src/sessions.rs`: keep the `ngram` base tokenizer,
keep `ngram_min_length(3)`, change `ngram_max_length(3)` to `ngram_max_length(5)`.

Rationale: ngram is the only base tokenizer compatible with the hard
multilingual requirement; the 3-5 range is the IR-literature optimum and the
fix for the known symbol-query failure mode; this experiment confirms it
carries no measured regression on either language. Do not adopt the word
tokenizer and do not build the dual index. A re-sync (or index rebuild) is
required for the change to take effect.

## 7. Limitations

Honest scope, relevant for any paper built on this:

- Pilot scale. 3-6 queries per stratum, 39 total - below the ~25-50 Buckley &
  Voorhees threshold. No result reaches statistical significance; all deltas
  are directional. Confidence intervals are wide and overlap.
- The English side was near-ceiling and non-discriminating. On the
  claude-code-only corpus the queries reliably retrieve their targets under
  every config; the dramatic 3-gram symbol-query failure seen in the earlier
  ad-hoc diagnostic did **not** reproduce here. That failure is therefore
  corpus-dependent - it needs the larger, noisier codex-inclusive production
  corpus to surface - and 3-gram's English weakness is not universal.
- Single corpus, single domain owner. The Ukrainian corpus is one operator's
  agent history (1 main session + 20 sub-agent sessions); its diversity is
  limited.
- FTS-only. No dense embeddings were active; this measures lexical retrieval in
  isolation. The strongest fusion architecture in the literature (lexical+dense)
  is untested here and is the natural next investigation.
- The Ukrainian apostrophe split in the word tokenizer (`комп'ютер`) and code
  identifier splitting were identified as real issues but were not separately
  isolated as experiment arms.

## 8. References

n-gram tokenization: McNamee & Mayfield, "Character N-Gram Tokenization for
European Language Text Retrieval," Information Retrieval 7(1-2), 2004; JHU/APL
CLEF 2003 working notes; McNamee, Nicholas & Mayfield, SIGIR 2009; Dolamic &
Savoy, "Indexing and Searching Strategies for the Russian Language," JASIST
2009; Church & Gale residual-IDF, 1995.

Rank fusion: Cormack, Clarke & Buettcher, "Reciprocal Rank Fusion," SIGIR 2009;
Beitzel et al., "Disproving the Fusion Hypothesis," JASIST 2004; Ng & Kantor,
JASIST 2000; Vogt & Cottrell, Information Retrieval 1999; Anthropic, "Contextual
Retrieval," 2024.

Evaluation: Manning, Raghavan & Schutze, Introduction to Information Retrieval,
ch. 8, 2008; Buckley & Voorhees, "Evaluating Evaluation Measure Stability,"
SIGIR 2000, and "Topic Set Size," SIGIR 2002; Craswell, "Mean Reciprocal Rank,"
Encyclopedia of Database Systems, 2009; Liu et al., "Lost in the Middle," TACL
2024; Jarvelin & Kekalainen, nDCG, ACM TOIS 2002.

Multilingual tokenization: tantivy tokenizer source; Elastic "Language
Pitfalls"; Wikimedia search-team Ukrainian analysis; Lucene
WordDelimiterGraphFilter.

Artifacts: `tokenizer-experiment-plan.md` (design),
`tokenizer-experiment-queries.tsv` (frozen query set), corpus snapshot at
`~/pj/tmp/pond-tokenizer-corpus/`.
