# Does semantic search earn its keep? A usage-trace evaluation of vector vs BM25 retrieval over an agent-session archive

Draft v0.2 - 2026-08-21 (v0.1 same day; v0.2 adds measured cost side, related work, embedder-gating caveat). Working paper; numbers are final for the data described, prose is a draft.

## Abstract

pond is a lossless archive of AI-agent sessions that exposes one MCP search tool, `pond_search`, with two single-arm retrievers: a vector arm (embeddings, cosine, the default) and a BM25 full-text arm (`mode=fts`). Since the arms were split in v0.10.0 (2026-06-20) the archive's own owner has run it as a daily tool, and because pond ingests the agent sessions that call it, every search call, its result, and what the agent did next is in the corpus. We use that trace - 1,126 `pond_search` calls across 267 sessions and 63 days - to ask whether the vector arm justifies the cost it imposes on ingest (inline embedding of every message) and serving (a resident model, ~2.5x query latency).

Three designs converge on the same answer. (1) An outcome audit of every call by an LLM judge (second-judge agreement 92%, kappa 0.85 on found-vs-not): fts calls found what the agent needed 61% of the time, vector calls 37%. The gap is stable across time slices and survives session-clustered bootstrap (vector 31-42%, fts 51-70%, 95% CI). (2) Within-query paired evidence from the trace: when an agent retried a failed vector query in fts mode (87 cases) fts resolved it 64% of the time; the reverse switch happened 10 times. (3) A blind paired test on 90 queries re-run through both arms the same day, A/B randomised: fts judged better on 41, vector on 20, tied on 29 (McNemar chi2 6.6, p < 0.05); fts contained relevant material for 94% of needs, vector for 83%. Vector-only value is real but small: on queries where vector had succeeded, re-running fts found the same top session 68% of the time; the 23% it missed were paraphrase-style queries (not longer ones - median length is the same) ("how did we fix the production 500s", "where did we leave off last time").

Conclusion: on this corpus and this usage, BM25 is the stronger default and the vector arm is a minority-case complement, not the workhorse. The data do not support paying the embedding cost on every ingest for every user; they support making fts the default, keeping vector opt-in or lazy, and investing instead in query rewriting for the paraphrase case.

## 1. Setting

- System: pond v0.10.0 through v0.14.11. Retrieval per `docs/spec.md` section 8: one arm per query, no fusion; vector = cosine over per-message embeddings of conversational text (`search_text`), recency tiebreak; fts = raw BM25 over the same text with the `simple` tokenizer + English stemming (ascii-fold on, no positions).
- Corpus at evaluation time: ~12.5k sessions, ~2.3M messages, 6 harnesses; embeddings present for every message (inline embedding at ingest was on for the whole window - the default-mode calls never fell back to fts; verified below).
- Callers: Claude Code main sessions and subagents, Codex CLI, nanoclaw agents; all using the MCP tool with the same descriptions.
- Window: 2026-06-20 (v0.10.0, the arm split) to 2026-08-20.

## 2. Data

All 1,126 `pond_search` tool calls in the window, extracted from pond itself via `pond_sql` over `parts` (tool_call + tool_result joined on `call_id`) and `messages`. Per call: parameters, the first 2,500 chars of the result, and a window of the next 8 events in the session (tool calls, results, assistant text) plus the 3 preceding user/assistant texts.

Arm attribution. 764 calls omitted `mode` (default), 243 passed `fts`, 119 passed `vector`. The default resolves to vector whenever embeddings exist; a header-wording check confirms no fallback happened: vector responses read "N nearest messages" from v0.12.0 (2026-07-03) and "N matching messages" before it, and every default-mode call after 07-03 carries the "nearest" header (478/478), every one before it the older wording. So: **vector n = 883 (78%), fts n = 243 (22%).**

Files: `data/calls_judged.csv` (one row per call, ids hashed, no query or result text) and `data/replay_fts_on_vector_found.csv`. The raw windows, result heads and per-call judge verdicts contain private session text and are not published; the scripts regenerate them from any pond store.

## 3. Methods

### 3.1 Outcome audit (all calls)

Each call window was classified by an LLM judge (Claude Opus, 29 independent instances, 40 calls each) under a fixed rubric (`scripts/judge-rubric.md`):

- FOUND - the agent followed up on an id present in this result, or its next text cites a fact visible in it, or it stops searching and proceeds.
- PARTIAL - relevant hits, but the agent re-queried the same need (reworded, re-filtered, switched mode, went to SQL).
- NOT_FOUND - empty, irrelevant, or error result; abandoned or switched.
- UNCLEAR - window insufficient (parallel bursts where follow-ups cannot be attributed).

The judge also recorded `next_action` and whether the agent switched arm on the same need.

Reliability: a second, independent judge re-labelled a random 120 calls. Agreement 92% on FOUND-vs-rest (kappa 0.85), 78% on the 4-class label (kappa 0.67); disagreement concentrates in NOT_FOUND vs PARTIAL on vector calls. Per-arm FOUND rates reproduced (fts 14/24 by both judges; vector 40 vs 43 of 96).

### 3.2 Why the raw rates are not causal, and what corrects for it

Arm is chosen by the agent, not assigned: `fts` tends to be picked when the literal words are known (an easier class), vector is the default for everything else including vague, exploratory and first-attempt queries. Calls also cluster (parallel bursts, retries) within 267 sessions. We therefore report (a) session-clustered bootstrap CIs, (b) need-level resolution (consecutive calls within 10 min in one session = one need), and most importantly (c) two paired designs where the same query meets both arms.

### 3.3 Paired design A - mode switches in the trace

For every call the judge flagged as followed by the same need in the other arm, pair the call's outcome with the outcome of the next call in the other arm.

### 3.4 Paired design B - replay of vector successes through fts

120 random post-07-03 calls judged FOUND in vector mode were re-run today through the `pond search` CLI in both arms with the original filters (limit 10). We record whether the original top-ranked session appears in today's fts top-10 (and, as a control for index drift, in today's vector top-10), plus wall-clock latency per arm.

### 3.5 Paired design C - blind A/B on the same query, same day

90 queries (50 originally issued in vector mode post-07-03, 40 originally fts) were re-run through both arms (limit 6); the two rendered result sets were labelled A/B at random and judged blind (2 Opus instances, rubric `scripts/paired-rubric.md`) for which better contained what the agent needed, given the query and its preceding context. Unblinded afterwards; McNemar's test on discordant pairs.

## 4. Results

### 4.1 Outcome audit

| Arm | n | FOUND | PARTIAL | NOT_FOUND | UNCLEAR |
|---|---|---|---|---|---|
| vector | 883 | 323 (37%) | 185 (21%) | 356 (40%) | 19 |
| fts | 243 | 149 (61%) | 44 (18%) | 47 (19%) | 3 |

Stability: post-2026-07-03 only (after the scoring/wording fix): vector 34% / fts 61%. August only: vector 32% / fts 59%. Session-clustered bootstrap 95% CI on FOUND rate: vector 31-42%, fts 51-70%.

Result-size check (a truncation-bias control on the judge's 1,800-char view): FOUND rises with result size in both arms alike (vector: <2k 0%, 2-10k 27%, >10k 46%; fts: 7% / 64% / 65%), so the truncated view does not favour one arm. The 111 vector results under 2k chars are empties/errors - vector mode does return "no matches" when filters empty the scope.

Need level (337 needs): needs that used only vector resolved 63% (after on average ~3 attempts), only fts 50% (n = 12), both arms 74%. In the 68 mixed needs the FOUND came from fts alone in 31, vector alone in 9, both in 10, neither in 18.

### 4.2 Paired A - switches

87 vector -> fts switches on the same need: fts FOUND 56 (64%) - 41 from NOT_FOUND, 14 from PARTIAL, 1 from FOUND; 14 PARTIAL, 17 NOT_FOUND. 10 fts -> vector switches in the whole window.

### 4.3 Paired B - replay

n = 120 vector-FOUND queries. Today's vector reproduces the original top session in 108 (90%; the 10% drift is 6 weeks of new data). **fts finds the original top session in 82 (68%).** Vector-only: 27 (23%); fts-only: 1; neither: 11. Mean latency per query on the local store: fts 0.39 s, vector 0.99 s.

The 27 vector-only queries are characteristically long natural-language paraphrases - e.g. "how did we fix the production API 500 errors from the database migration", "where did we leave off last time what was the plan we agreed on before pausing", "tradeoffs comparison of two options and which one we chose and why", "memory leak or OOM investigation root cause". Query length does not separate the sets: vector-only wins have a median of 9 words, queries both arms found a median of 10; across all calls vector queries are longer than fts ones (median 9 vs 7 words, 72% vs 42% at 8+ words). The discriminator is phrasing - a narrative paraphrase of a past exchange versus topic words - not length.

### 4.4 Paired C - blind A/B

n = 90. Winner: **fts 41, vector 20, both 29, neither 0.** McNemar on discordant pairs: chi2 = 6.56, p < 0.05. Contains relevant material for the need: fts 85/90 (94%), vector 75/90 (83%).

By stratum (the mode the agent originally chose, i.e. the query's "native" shape): originally-vector queries (n = 48): fts 20, vector 12, both 16. Originally-fts queries (n = 40): fts 19, vector 8, both 13. fts wins even on queries the agent had phrased for the vector arm.

## 5. Interpretation

1. The raw 61% vs 37% gap is confounded by query selection, but every design that removes the confound (switch rescue, replay, blind A/B on identical queries) still favours BM25, and by a margin that is statistically clear at these sample sizes.
2. The vector arm's unique contribution is bounded: roughly a quarter of its successes are ones BM25 would have missed, concentrated in long paraphrase queries. In call terms that is ~70-80 of 1,126 calls (6-7%) where semantic retrieval was the thing that worked.
3. Agents already compensate: they retry vector ~3x per need, switch to fts 9x more often than the reverse, and route 659 lexical `contains_tokens`/`fts()` queries through `pond_sql` in the same window (2.7x the explicit fts calls) - lexical retrieval is where recall actually happens, under several names.
4. Cost side is measured, in [cost-side.md](cost-side.md): per-instance RSS floor ~100 MiB without the model vs ~900 MiB-1 GB once any instance has synced or served a vector query; first sync CPU-bound for hours on CPU-only hosts (field receipt: three pond instances exhausted an N100's 8 GB swap, and a 38-session backfill took 38 minutes at 167% CPU); ~70 extra S3 GETs and a model reload measured in seconds on most real-world vector calls because the embedder idle-evicts between an agent's calls. Disk is only ~5%. That is what buys the 6-7%.
5. The result is about agent-issued recall queries over session text, not about embeddings in general. The one rigorous external measurement we found ([frankensearch](https://github.com/Dicklesworthstone/frankensearch), the engine under cass, `docs/NEGATIVE_EVIDENCE.md` "HEADLINE CORRECTION") says the dense tier's value over a *stemmed* BM25 is +0.6-3% nDCG@10 with a static embedder but +8-21% with a contextual one (BGE) on BEIR. pond's BM25 is stemmed (consistent with our gap) and its embedder, E5-small, is contextual - so BEIR would predict more semantic lift than we observe. The reconciling facts are the query and corpus shape: agents query an archive of their own prior work, usually with the vocabulary that work used, over code- and tool-heavy, multilingual text; the cases where vector uniquely won are narrative paraphrases of a conversation ("how did we fix...", "where did we leave off..."), and they are a minority of what agents ask. A stronger contextual embedder is an untested variable (Section 7).

## 6. Recommendation

- Make `fts` the default arm; keep `vector` as an explicit opt-in, or as a lazy second pass the tool runs only when the fts scope count is low or the query is long and identifier-free.
- Decouple embedding from ingest (off by default; `pond optimize --only embed` for users who want the vector arm) so the archive's write path and first-run experience stop paying for it.
- Cheaper route to the paraphrase case than embeddings: tool-side query rewriting into keyword form, and the documented agent guidance to start with fts. If the vector arm stays, the two shapes in the field are (a) deja-vu's lexical-first cascade - BM25 candidates, optional rerank of the top 64 by `0.5*lexical + 0.5*cosine`, vector only as fallback on an empty lexical result - and (b) cass's stock RRF (k=60, equal weights) over both arms. Our data favour (a): it keeps the always-on path model-free and spends the embedder only where lexical evidence is exhausted. Score fusion is unstable across corpora (frankensearch measures k~10 at +2.6% over k=60 and its default tiebreak as systematically demoting vector-only hits) and would need its own paired measurement here before adoption.

## 7. Related work: what the lane has measured

Checked 2026-08-21 against fresh clones (receipts kept in the author's private notes):

- **deja-vu** (lexical-only by design, optional external embedder): publishes LongMemEval-S hit@1 84.9% / hit@5 94.3% and LoCoMo hit@1 69.8%, all lexical ("no LLM, no embeddings"). Its own issue #553 states the semantic rerank has never been run on the benchmark, that 48 of 79 misses are lexical ties needing "semantic discrimination", and that preference/paraphrase questions score 36.7% hit@1 "by design". The lexical posture is a zero-dependency packaging decision (ROADMAP "Not planned: embeddings in the base binary"), not a measured retrieval finding.
- **cass** (hybrid by default, opt-in MiniLM): has never measured hybrid vs lexical vs vector - the test that would (`test_hybrid_search_improves_recall`) was planned and never implemented; its bakeoff compares embedder models, not modes; open issue #404 reports the default hybrid returning a page of confident hits for out-of-vocabulary queries where lexical correctly returns none. cass is excluded from any benchmark table here because its license rider forbids "benchmarking, testing, analyzing" by Anthropic-affiliated parties.
- **frankensearch** (cass's engine): the one reproducible lexical-vs-dense measurement we found (pure-Python BEIR harness), with the baseline correction and embedder-gating result cited in Section 5.

To our knowledge no tool in this category has published a lexical-vs-semantic comparison on its own production stack; this study is the first, and it is observational rather than benchmark-based.

## 8. Threats to validity

- Single archive, single owner, mostly Claude Code callers; query mix is one engineer's.
- LLM judges, not humans; reliability measured (kappa 0.85 binary) but the rubric's FOUND criterion (visible follow-up) is conservative for calls inside parallel bursts.
- "Neither = 0" in the blind test suggests the paired judges were lenient on `has_relevant`; the relative comparison is unaffected.
- The corpus grew between the original calls and the replay/blind runs (6 weeks); the 90% self-reproduction rate of vector bounds that drift.
- Scoring bug window (NaN/0.00 scores, 06-20 to 07-02) is included in the full-window numbers; the post-07-03 slice excludes it and shows the same gap.
- `pond_sql` lexical queries (659) were sized but not judged; including them would move share further toward lexical, not away.
- One embedder (multilingual-e5-small, 384-d) and one tokenizer configuration (`simple` + English stem). frankensearch's embedder-gating result means a larger contextual embedder could narrow the gap; it would also raise every cost in cost-side.md. Not tested.
- Queries were authored by agents that had read pond's tool descriptions, which steer toward keyword-style queries and name fts for literal words; the query distribution is partly a product of that guidance.

## 9. Reproduction

`scripts/build.py` (windows from the two `pond_sql` exports), `scripts/replay.py` (design B), `scripts/paired.py` (design C), rubrics in `scripts/`. `data/` holds the hashed, text-free call table and replay table; raw exports and per-call verdicts are not published (they contain session text).

Changelog of the evaluated system: v0.10.0 (2026-06-20) "drop server-side hybrid fusion for single-arm retrieval - mode=vector (default) or mode=fts", tokenizer `ngram` -> `simple`+stem; v0.12.0 (2026-07-03) vector header wording "nearest" + fts caveat.
