# Remote-read performance and index cleanup

## How to use this doc

You are a fresh agent picking up a completed investigation. This doc is self-contained: the problem, the measured evidence, the change plan, and the explicit non-goals. Read `docs/spec.md` sections 3 (storage substrate) and 8 (search and embeddings) before touching behavior; the rule mnemonics referenced below (`spec.md#search`, `spec.md#search-language-neutral-index`) live there.

Scope is narrow on purpose: **making `pond mcp` against a remote (S3) store fast, and removing redundant indexes from the read path.** It does NOT cover retrieval *semantics* or response *shape* (recency/supersession ordering, per-cell truncation, conversation-first defaults, FM-index substring search) - those are a separate, later effort (see "Separate efforts").

**Tools and artifacts you have (committed):** the regression/measurement benches `benches/tokenizer_quality_bench.rs` (per-tokenizer Success@K) and `benches/serve_mem_bench.rs` (RSS per phase + `--tokenizer-sweep` index-size/RAM/latency), plus the committed 39-query set `docs/researches/tokenizer-experiment-queries.tsv` (runnable as-is). **Artifacts you must regenerate locally** (corpus-derived, gitignored under `ops/search-benchmarks/` per `.gitignore` policy, so NOT in your checkout): the 111-query paraphrased set and the UK set used for the #8 decision. They are recoverable from pond's own stored sessions via the `pond_*` MCP tools (that is how they were produced) or regenerated from the corpus; do this before running the #8 multilingual eval.

**Assume `docs/plans/2606-17-sync-copy-durability-and-perf.md` is already implemented** by the time this work starts: the messages-based sync oracle (B6), the append-for-absent write fast-path (C8), the unconditional copy verify (A2), and commit-row-last (A1) have all landed. This plan therefore does NOT re-plan the write path; it only tunes one write-adjacent knob (retention) that affects remote *open* cost.

## The problem

Working with `pond` connected to remote storage is close to unusable, and the index set carries dead weight. Measured on the real corpus (local `~/.local/share/pond`; remote `s3+https://nbg1.your-objectstorage.com/pondarium/pond`, 11,185 sessions / 2,035,076 messages; benchmarking copy `pondarium/pond-full-corpus-benchmarking-copy`):

1. **Cold first-search is catastrophic: 175-442 s.** The first `pond_search` after a (re)start pages the FTS inverted index + IVF_PQ vector index from S3 partition-by-partition. Cold hybrid search measured 186-442 s wall; a single trace showed ~166 s in index/data fetch. Warm (cached) it is sub-second to a few seconds. The warm MCP server hides this; any cold process (or the first query after restart) eats it.

2. **The FTS index is the wrong design - 2x worse retrieval and ~5-6x heavier than it needs to be.** pond's FTS uses a character-`ngram` (3-5) tokenizer. On 111 realistic *paraphrased* queries against the full 2M-message corpus (Success@3 over top-10 sessions), a word tokenizer beats it roughly 2x: `simple+stem` 66/111, `simple` 56/111, vs ngram-3-5 (current production) 31/111, ngram-3-3 16/111. The ngram index is also ~5x heavier in query RAM (1,868 MB vs 379 MB at 2M rows under capped caches) and ~4x larger on disk - which is exactly what makes #1 slow and the server fat.

3. **Server RAM is unbounded by default.** `LANCE_DEFAULT_IO_BUFFER_SIZE` defaults to 2 GiB, so a scan can balloon RSS; a warm remote `pond mcp` was measured at ~1.4 GB RSS. The embedding model is ~790 MB resident. The idle floor *can* drop to ~200 MB (model eviction already exists), but nothing caps the scan buffer or the cache for a memory-bounded server.

4. **Every SQL query opens all three tables.** `transport.rs` `try_join`s `sessions`+`messages`+`parts` on every `pond_sql_query`, even single-table queries - so each cold query pays the open of `parts.lance` (the slowest) needlessly. Cold open is ~1.0-1.3 s/process (namespace `__manifest` + 3 sequential table-manifest reads).

5. **Two indexes are dead weight.** `messages_project_btree` is never used - the project filter is only ever `LikeContains`/`Regex` (substring), which a BTree cannot accelerate (`handlers.rs:1786`). `messages_role_bitmap` is never used - `role` is only projected, never filtered. Both cost build time, storage, and compaction-remap surface for nothing. (Of the 10 index intents, the other 8 map 1:1 to a real hot predicate.)

## Design principles

- **The remote-read engine is fine on v7; the wins are in using its knobs, not upgrading.** lance 7.0.0 already has segmented/incremental FTS, `prewarm_index`, the ordered-listing latest-version fast path (Hetzner S3 is lexically ordered, so version resolution is already fast), and all the IO/cache tuning knobs. v8 is beta with 6 breaking API changes and its read-path deltas are narrow; **do not upgrade** (see "What NOT to do").
- **Separate the two text jobs.** Ranked retrieval (the dominant use) wants a word-tokenizer FTS + BM25. Exact substring/symbol lookup (`8/8`, `cf_clearance`) is a *different* job and already has a home in `pond_sql_query` (`contains_tokens`/`LIKE`); it does not justify degrading the ranked index with ngram. Do not conflate them.
- **Warm once, then stay warm and bounded.** The cold cost is paid once per process; prewarm it at startup so no user query eats it, and cap the caches/IO buffer so the warm process sits inside a fixed memory budget and spikes only during requests.
- **An index must match its column's predicate.** Keep only indexes whose type accelerates the query actually issued against that column; a BTree on a substring-filtered column is not an optimization, it is overhead.

## The change plan (sequenced)

### Step 0 - enabler (do first)

- **T0. Make the tokenizer/index bench prep-once/run-many.** Refactor `benches/tokenizer_quality_bench.rs` (and the `--tokenizer-sweep` mode in `serve_mem_bench.rs`) so index builds are a one-time prep (APFS-clone the corpus per tokenizer, build each index once) and the measurement runs are query-only. Today each run rebuilds all 5 indexes serially (the ngram build alone is minutes on 2M rows), which makes re-measuring #8 and #7 prohibitively slow. This unblocks the gated decisions below.

### P0 - the remote-read fix

Critical path: **T0 -> #8 -> #1**. The rest of P0 is independent and parallel.

- **#8. Switch the FTS tokenizer from ngram(3-5) to word (`simple`+English stem).** Hard prerequisite to #1 - do not prewarm an index we are about to replace. Reindex `messages.search_text`; amend `spec.md#search-language-neutral-index` (it currently pins ngram). Fold #9 into this reindex. **Gated on the multilingual decision** - see Open items. The substring/symbol need that ngram served stays on the `pond_sql_query` `LIKE`/`contains_tokens` path; do not add a substring mode to `pond_search`.
- **#1. Prewarm indexes on `pond mcp` startup** (background, after #8). Vector via `Dataset::prewarm_index`; FTS via a one-shot synthetic warmup query (FTS has no public prewarm API). Converts the 175-442 s cold first-search into a startup cost the user never waits on. Emit a "warming" trace so it is observable.
- **#9. Raise `LANCE_FTS_TARGET_SIZE`** at index build so the FTS index is fewer, larger partitions (fewer S3 round-trips per query). Land it inside #8's reindex; size to ~1 partition for the corpus and confirm via the index dir.
- **#2. Bound server RAM.** Cap `LANCE_DEFAULT_IO_BUFFER_SIZE` (2 GiB -> ~256 MiB) and set lean remote cache caps so a warm `pond mcp` sits near the idle floor and spikes only in-request. Target: idle well under 500 MB.
- **#3. Open only the tables a SQL query references.** In `transport.rs`, parse the statement and open just those datasets instead of `try_join`ing all three. Removes the needless `parts.lance` open (slowest) for messages/sessions-only queries and ~1/3 of cold open.
- **#4. Lower idle model eviction 300 s -> 60 s** (`embed.rs:177` `DEFAULT_IDLE_EVICTION`; optionally expose as `[runtime]`). Already implemented at 300 s - this is a one-line tune. Cost: a ~358 ms + cached-load model reload on the first query after each idle gap.
- **#12. Lower `cleanup_older_than` toward the 1 h floor** (`main.rs`). Fewer retained manifest versions = cheaper remote open. (The append-vs-merge half of version bloat is already handled by 2606-17/C8 - do not re-plan it.)

### P1 - index cleanup (independent, parallel)

- **#5. Drop `messages_project_btree`** - unused (project filter is substring/regex only).
- **#6. Drop `messages_role_bitmap`** - unused (`role` never filtered).
- **#7. Verify then maybe drop `messages_timestamp_zonemap`** - **(DONE, dropped in #75:** the ZoneMap mis-prunes the tz-aware column and returned empty date filters; bounds now run as a refine over the candidate set.) `analyze_plan` a date-range query on the corpus; if zones do not prune (timestamps are not clustered by sync-append order), drop it. The fallback is a scan of a small i64 column, usually over an already-narrowed candidate set.

Removed indexes are reaped on the next `pond optimize`; `enable_stable_row_ids` makes the drop a metadata change with no remap.

### Decided (no work)

- **#10. Stay on lance v7.0.0.** No upgrade.

### Cross-cutting

Startup prewarm and any long index build emit structured progress through the existing `pond::output`/`indicatif`/tracing stack (a 175 s silent warm is as bad as the merge-run anti-pattern from 2606-17). Validate every change with `cargo fmt` + `cargo clippy --all-targets -- -D warnings` + `cargo test`; present the diff before applying.

## Measured data (do not re-run)

Tokenizer quality, 111 paraphrased queries (gitignored `ops/search-benchmarks/queries-paraphrased.tsv`), full 2M corpus, Success@3 over top-10 sessions, FTS-only (`tokenizer_quality_bench`):

```
T4 simple+stem  66/111   MRR .491   P@1 41
T3 simple       56/111   MRR .457   P@1 39
T2 ngram 4-5    48/111   MRR .410   P@1 37
T5 RRF(T1,T4)   38/111   MRR .331
T1 ngram 3-5*   31/111   MRR .263   P@1 20    (* current production)
T0 ngram 3-3    16/111   MRR .131
```

FTS index weight: 53k-corpus index size ngram 11.2 MiB vs simple 2.7 MiB (4.2x), build 1.2 s vs 0.16 s. 2M-corpus FTS query peak RSS (caps 256/128 MiB) ngram 1,868 MB vs simple 379 MB (~5x); whitespace 677 MB.

Remote latency: cold hybrid search 186-442 s; comprehensive run at 256/128 caps - `first_hybrid` 69 s, `hybrid_steady` p50 88 s / p95 176 s; warm SQL `MIN/MAX(timestamp)` 1.27 s, filtered count 0.57 s, the 4-aggregate that was 9.2 s cold-cache ran 1.03 s warm. Cold open ~1.0-1.3 s/process (namespace manifest + 3 sequential table manifests). Env IO knobs (`LANCE_IO_THREADS` 128/256, `io_buffer` 4-8 GiB) measured NOT to help scans - do not set them.

Memory: warm remote `pond mcp` ~1.4 GB RSS; embedder ~790 MB resident, drops to ~106 MB on eviction; idle floor after model drop ~198 MB.

Index usage (verified in code): `project` filtered only by `LikeContains`/`Regex` (`handlers.rs:1786`); `role` never filtered; the other 8 indexes each serve a real hot predicate.

## What NOT to do

- Do NOT upgrade to lance v8. Beta, 6 breaking API changes (`load_indices` removed, `IndexSegmentBuilder` removed, `finish()` signature, bitmap workflow, distributed BTree, index listing), and its read-path wins (#7129/#6903) are narrow and do not fix the FTS cold-load or the scan tax. (If ever reconsidered, first verify the v8-removed `__manifest` table-version path is not pond's lance-namespace Directory `__manifest`.)
- Do NOT set `LANCE_IO_THREADS`/`io_buffer` high to "speed up" scans - measured no help; the bottleneck is round-trip latency and cache coldness.
- Do NOT add a substring mode to `pond_search` or an FTS index over tool outputs - substring stays the SQL escape hatch; tool outputs stay id-addressable only.
- Do NOT prewarm before the tokenizer switch (#8 before #1) - it would warm an index we are discarding.
- Do NOT commit any corpus-derived query set - they live gitignored under `ops/search-benchmarks/` (`.gitignore` policy); the bench default stays the committed 39-query `docs/researches/tokenizer-experiment-queries.tsv`.

## Open items

- **The multilingual decision (gates #8).** The 111-query result is English-only; the original ngram choice was justified specifically for Ukrainian inflection (no Ukrainian stemmer), and that case is untested at full-corpus scale. Procedure: build/extend a UK paraphrased eval (a `queries-uk-translated.tsv` already exists in `ops/search-benchmarks/`), run `tokenizer_quality_bench` on it under T0-T4, and decide. The English win is large (2x) and the RAM/cold-start payoff is the whole point of this plan, so the expected outcome is "switch to word, accept any modest UK regression" - but it must be measured, and the spec amendment must record the result. Until decided, #1/#9 wait on #8.
- **ZoneMap effectiveness (#7)** is unverified pending the `analyze_plan` check.

## Separate efforts - NOT in this plan

Retrieval semantics and response shape, from the external field test (`docs/researches/pond-retrieval-ergonomics-fieldtest-2026-06-16.md`): recency/supersession ordering, per-cell/match-centered truncation, conversation-first defaults, the `mode: hybrid|fts` surfacing, and an FM-index for substring search. These are a distinct effort with their own plan and commit; they reshape the tool surface, not the index/remote-read path this plan fixes.
