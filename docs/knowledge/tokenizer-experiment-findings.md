---
type: Finding
title: Bilingual FTS tokenizer evaluation and word tokenizer adoption
description: Character 3-5 n-grams initially preserved Ukrainian inflection without English regression, but word tokenization with English stemming was ultimately adopted for 28x smaller index size and 2x English retrieval gains.
tags: [tokenizer, fts, bm25, multilingual, tantivy]
status: stable
generated: { by: "acpx/gemini-3.7-flash-medium", at: "2026-09-11T11:01:53Z" }
sources:
  - id: tokenizer-report
    resource: docs/researches/tokenizer-experiment-report.md
    title: FTS tokenizer selection for a bilingual agent-session corpus (2026-05-22, updated 2026-06-18)
  - id: tokenizer-plan
    resource: docs/researches/tokenizer-experiment-plan.md
    title: "Tokenizer experiment plan: bilingual FTS for pond"
---

# Bilingual FTS tokenizer evaluation and word tokenizer adoption

## Headline claims

- In a 39-query bilingual experiment on a 1.95M-row corpus, character n-gram tokenizers scored 9/18 Success@3 on Ukrainian compared to 7/18 for word tokenizers lacking Ukrainian stemmers.[^tokenizer-report]
- Widening n-grams from fixed 3-grams to a 3-5 range matched baseline accuracy (27/39 Success@3) while resolving symbol-lookup failures on code identifiers without regressing either language.[^tokenizer-report]
- Dual word and n-gram indices fused via RRF (26/39 Success@3) failed to beat single n-gram indices (27-28/39), confirming that fusing correlated lexical retrievers over the same corpus provides no gain.[^tokenizer-report]
- A subsequent operational update (2026-06-18) adopted the `simple` word tokenizer with English stemming, superseding the pure n-gram recommendation due to read-path constraints.[^tokenizer-report]
- On an English-dominant corpus, word tokenization with English stemming doubled retrieval Success@3 (66/111 vs 31/111 for n-gram) while reducing inverted index size by ~28x (41 MB vs 1.14 GB on 2.06M messages).[^tokenizer-report]
- The 28x lighter word index eliminated severe remote S3 cold-start page-in delays that had taken 175-442s on the large n-gram index.[^tokenizer-report]
- The Ukrainian regression from word tokenization was limited to a single natural-language query (7/21 vs 8/21 Success@3) because Cyrillic passes through unstemmed and matches exact keywords across all other strata.[^tokenizer-report]
- The `search-language-neutral-index` contract was revised to allow gracefully degrading stemming rather than banning all monolingual transforms.[^tokenizer-report]

## Invalidation

These findings would be invalidated if the archive corpus shifts from English dominance to predominantly morphologically rich languages lacking stemmers, or if storage and memory constraints are relaxed sufficiently to tolerate multi-gigabyte inverted indices.

## Source

Full research reports: [docs/researches/tokenizer-experiment-report.md](../researches/tokenizer-experiment-report.md) and [docs/researches/tokenizer-experiment-plan.md](../researches/tokenizer-experiment-plan.md).

[^tokenizer-report]: `docs/researches/tokenizer-experiment-report.md`
[^tokenizer-plan]: `docs/researches/tokenizer-experiment-plan.md`
