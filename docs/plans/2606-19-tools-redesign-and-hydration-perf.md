# Tools redesign + search hydration perf - handoff (2026-06-19)

## START HERE (authoritative; everything below is context or superseded)

The design was reworked with the user on 2026-06-19. **The two authoritative sections are "Decisions locked" and "Hydration architecture" (immediately below).** Sections 0-10 are the original handoff - kept for measurements, mechanics, and the validation playbook, but where they conflict with the two authoritative sections, the authoritative sections win. In particular the original "do A first / drop hybrid to unblock B" framing is **wrong and overruled** (see Outcome): hybrid is being dropped, but for product reasons (vector-default + recency boost), not to unblock B.

**Build order for the next session:**
1. **Retrieval-shape redesign** - drop hybrid + `fuse_arms`; `mode=vector|fts` (default vector); split scoring (gate = raw cosine `min_score` default 0; order = cosine + recency-boost 0.1/30d post-gate); fts = BM25 + `sort_by`, `min_score`->error; `sort_by=relevance|recency` both modes; drop `include_subagents`/`source_agent`/`format`/`hybrid` params (subagents via SQL only). Rebuild the take_rows hydration on the now-1:1 single-arm surface (it becomes the cache-miss fallback). Validate with the **free local** recall harness.
2. **`pond_get`** - one tool, prefixed params (`session_*` + `message_*`), `message_context_before/after=3`; drop `response_mode`/`offset`/`after_id`.
3. **`pond_sql_query`** - drop `json` (keep `text|parquet|ndjson`).
4. **Output discipline** - 10k per-item truncation + pagination/expansion markers; no-cap session grouping + "N newer messages" footer; tool descriptions carry the decision rule.
5. **Resident meta cache** (the real hydration win - see "Hydration architecture") - extend `src/rowmap.rs` to hold `search_text` + meta resident; then per-session aggregate; then `summary_parts`. Each step independently shippable.

**Working-tree state to inherit:** the current uncommitted code (take_rows hydration bolted onto hybrid, with rowid carried through fusion) is **superseded - do not commit it as-is; rework it in step 1**. The take_rows *method* (`Store::message_metas_by_rowids`) and the bench flags (`--recall-mode`, `--recall-min-score`) are worth keeping. `AGENTS.md` has an unrelated change - leave it out. Nothing is pushed. The recall-panic blocker is **resolved** (take_rows sidesteps it; remote recall now completes - see Outcome), so sections 7-8's "push is blocked" framing is historical.

**Validation:** local recall is FREE and reproduces the panic/recall numbers - `serve_mem_bench --recall-mode {fts|vector|hybrid} --recall-min-score <f>` against `~/.local/share/pond` (section 8 has the full playbook + the remote io-trace commands + the throttle caveats). Local gate always: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.

Always run cargo with `--manifest-path /Users/tenequm/Projects/pond/Cargo.toml` (a stale cwd into `~/pjv/lance-format/lance` has bitten this repo before - builds ran in the Lance tree).

## Outcome (2026-06-19) - workstream B landed, hybrid-removal premise refuted

**Done and verified (commit-ready, not yet committed):** the take_rows hydration win (workstream B) landed *with hybrid kept*. `Store::message_metas_by_rowids` (take_rows by stable rowid) replaces the `message_metas_by_keys` IN-scan when the row-key map is loaded; the arms now return `SearchHit { rowid, key, score }` and carry each hit's rowid through fusion (`FusedHit.rowid`, `Candidate.rowid`) to the representative, so even a fused hybrid hit hydrates by exact rowid. Files: `src/sessions.rs`, `src/handlers.rs`, plus `benches/serve_mem_bench.rs` (`--recall-mode` / `--recall-min-score` for A/B). Local validation: fmt, clippy (default + io-trace), 226 tests; hybrid recall byte-identical before/after (0.667 -> 0.667, recall-neutral - ranking logic untouched).

**Measured remote deltas** (`pond-full-corpus-benchmarking-copy`, warm, per warm query p50, before -> after):
- pond_search bytes: 6.0 MB -> 1.16 MB (**-81%**)
- hydration GETs: 130 -> 109 (-16%); pond_search GETs 133 -> 112
- `index/page_data.lance` reads (summed over 20q): 862 -> 545 (**-37%**); `data` bytes 112.6 MB -> 31.4 MB

The GET-count drop is moderate, **not** the projected ~-97%: only `message_metas` became take_rows. `summary_parts_for_messages` and `session_message_counts` remain IN-scans and floor the round-trip count. The byte drop is the real latency win. Remaining-GET follow-ups: targeted hydration for those two sub-queries (different table / aggregation - not a trivial take_rows).

**Recall-panic blocker (section 7) RESOLVED.** The Lance decoder panic (`primitive.rs:3656` `front_mut().unwrap()` - the structural decoder ran out of loaded pages mid-drain) lived in the `message_metas_by_keys` IN-scan decode of the remote copy's `search_text` encoding; it never reproduced on the local source-of-truth corpus. take_rows hydration uses a different read path and **sidesteps it**: the full remote recall now completes end-to-end (was crashing at query ~15). It is an upstream Lance bug, but pond no longer triggers it on the default path.

**Recall A/B (for the record, then overruled).** Measured local fixed-mode recall (N=21): hybrid Success@3=**0.667** vs vector-only **0.333** vs fts-only **0.476**. This briefly looked like a reason to keep hybrid, but the comparison is wrong: it forces every query through one arm, whereas the design has the **agent pick the arm per query**, and the 21-query set is tiny/overfit. Decision (see "Decisions locked" above): **drop hybrid**, vector default + recency boost, fts as the keyword fallback. The current commit (take_rows bolted onto hybrid with `rowid`-through-fusion) is therefore **superseded** - the take_rows method is kept, the fusion plumbing is removed and B is redone on the single-arm surface.

## Decisions locked (2026-06-19) - authoritative; supersedes sections 4-5 where they conflict

Settled with the user after the recall A/B and an agent-ergonomics discussion. This block is the spec to build to; read sections 4-5 only for the rationale they still carry.

**`pond_search` surface:**
- `mode = vector | fts`, **default vector**. Hybrid and `fuse_arms` are **deleted** (server-side fusion replaced by the agent choosing the arm per query). The recall objection to dropping hybrid was withdrawn: it rested on a fixed-mode A/B over a tiny 21-query set, which does not represent per-query agent routing; vector-default + recency boost is what the user actually wants (it served them well in `~/pj/claude-kb/`; pond's hybrid felt degraded mainly for lack of a recency boost).
- `vector`: ordering = cosine + recency-boost (additive, magnitude 0.1, 30-day exp decay, **post-gate tiebreaker** - never makes strongly-relevant old content invisible). `min_score` gates on raw cosine, **default 0** (calibrate later against the recall set; present/absent cosine overlap, so 0.3 risks the false-negative problem - `searchable_in_scope` carries the absence honesty). `sort_by` honored.
- `fts`: BM25 as the **matcher**; **default ordering = relevance (BM25)** because agents are trained to expect relevance from anything called "search". `min_score` is **disallowed -> error** (BM25 is unbounded and not comparable across queries, unlike bounded cosine). `sort_by` honored.
- `sort_by = relevance | recency` on **both** modes. When a response is recency-sorted it must be **labeled** so the agent does not misread rank-1 as best-match.
- Dropped params: `include_subagents`, `source_agent`, `format`, and the wire `hybrid` mode. Subagents are reachable **only** via `pond_sql_query` (`parent_session_id`) - intentional; they pollute main search too much.
- take_rows hydration (workstream B) is **redone** on the single-arm surface (each hit is natively 1:1 with a rowid - no `RankedList`/`FusedHit`/`rep_rowid` fusion threading). The already-verified take_rows method (`message_metas_by_rowids`) stays - but as the **cache-miss fallback** under the resident meta cache (see "Hydration architecture" below), not the primary hydration path.

**`pond_get` - single tool kept (split deferred).** Params are **prefixed** so they self-document under the names-only constraint (agents gloss the descriptions):
- `session_id`, `session_limit=20`, `session_from=start|end`, `session_after_message_id`, `session_before_message_id`
- `message_id` (any message, incl. `tool_call`/`tool_result` bodies - the one fact worth a description line), `message_context_before=3`, `message_context_after=3` (mirrors `grep -B/-A`)
- exactly one of `session_id`/`message_id`; render implied by scope (session -> user/assistant conversational + one-line tool refs + bidirectional page markers; message -> target full parts + before/after conversational siblings).
- Dropped: `response_mode`, `offset`, the generic `after_id`.
- The `pond_get_message` / `pond_get_session` split from section 4 is **deferred**, not cancelled - revisit on its own merits after this lands.

**Unchanged from the plan (implement as written):** `pond_sql_query` drops `json` (keep `text|parquet|ndjson`); 10k per-item truncation + pagination/expansion markers; no reasoning rendered or indexed; tool descriptions carry the decision rule (concepts -> vector, exact words -> fts, symbols/analytics/subagents -> sql).

**`pond_search` response shape:** sessions grouped, ordered by best (recency-boosted) hit; within a session, matching messages newest-first. **No per-session match cap** (the old `MAX_MATCHES_PER_SESSION=3` is removed) - the 10k per-item char budget is the only limiter. Each over-budget session carries a footer "`N newer messages in this session - conclusions may be revised; read with session_from=end`" (the intra-session supersession signal; the recency boost only handles cross-session ordering). With hydration in memory (see next section) there is no S3-roundtrip reason for a row-ceiling; the char budget alone bounds output.

## Hydration architecture (2026-06-19): resident meta cache, not per-query take_rows

The take_rows hydration (workstream B) cut hydration *bytes* -81% but left a ~109-GET *round-trip* floor (round-trips, not bytes, dominate S3 latency). The decided endgame is to **eliminate per-query hydration S3 entirely** by holding the hydration data resident, the same way the vector index is held in memory. take_rows is **demoted to the cache-miss fallback**.

**What goes resident (extends the existing `src/rowmap.rs` mmap design):**
- Per-message meta keyed by stable rowid: `search_text` (the heavy column, ~133 MB corpus-wide) + `role, project, source_agent, timestamp`. Turns `message_metas_by_*` into an in-memory lookup.
- Per-session aggregate (`session_id -> count, max_timestamp, last_message_id`) - replaces the `session_message_counts` S3 scan. **Reused later as the `pond sync` staleness oracle** (same MAX/COUNT/last-id key) - *noted, deferred, do not build the sync side now.*
- `summary_parts` (the file/tool one-line refs for user-role hits) - the last hydration S3 read. Can phase: meta map first, this second.

Net: a default (vector) search becomes in-memory neighbor lookup + in-memory hydration = ~0 per-query S3. FTS keeps small BM25 posting-list reads.

**Retrieval-side perf (decided, for completeness):** dropping hybrid also makes the *default* lighter independent of hydration - one arm instead of two arms + fusion + the doubled candidate over-fetch. And there is **no nprobes tax** for leaning on vector: with the IVF index prewarmed in memory, a higher `nprobes` is just more in-memory distance math, not more S3 (the "one S3 read per partition" cost only applies to a cold/unwarmed store, which the long-lived server is not). So vector-default + resident hydration is a clean win on both the retrieval and the hydration halves.

**Concurrency (dozens of remote writers, N local reader processes - no machine owns the writes):**
- Never mutate a shared mapping in place. Publish **immutable, version-named snapshot files** (`...-v{N}`) + **atomic rename**; readers mmap read-only and switch files when they observe a new version. Old inodes stay valid for in-flight readers (POSIX unlink). No torn reads, no cross-process write lock.
- Remote writers are irrelevant to local file safety - they only bump the remote `messages_version` that local readers observe.
- Coordinate only the *build* with a local advisory `flock` so N local instances don't all re-read S3 at once (build is safe-but-wasteful otherwise; the atomic rename already prevents corruption). Reads are perfectly shared: one physical mmap copy in OS page cache across all local instances.
- The build is **reader-driven and local**, NOT owned by `pond sync` (writers are remote/dozens). A one-shot `pond search` CLI mmaps an existing map for free; it only skips *building* a cold map (falls back to take_rows for that one invocation).

**Freshness:** fresh to within one poll interval. Poll `messages_version` (a cheap metadata read) per request; on a bump, pull only the delta (new fragments since the map's version - append-only + stable row-ids make this cheap; immutable base + sorted delta segments searched together, LSM-style) and refresh index + map together. This is the *same* lag the vector/FTS indexes already have; the map just tracks the same version. Correctness never wrong: rows in the newer version not yet in the map fall back to take_rows.

**Footprint:** ~150 MB, dominated by `search_text`, mmap'd -> OS page cache (shared across local instances, **evictable without swap**, so it can never OOM - worst case is a page fault that reloads from disk). ~2% of an 8 GB machine; ~30% of the self-imposed 500 MB server target (the figure to watch). Non-issue at current corpus size; revisit only at ~10x data.

**Sequencing:** this is a larger build than take_rows. Land the retrieval-shape redesign + the already-verified take_rows hydration first (take_rows is the correct fallback either way), then build the resident meta map on top, then the per-session aggregate and `summary_parts` residency. Each step is independently shippable.

## 0. Status of the committed work

Two commits are **local, not pushed** (push is blocked - see section 8):

- `fda30d9` `perf(search): resolve FTS/vector hits via mmap'd row-key map` - new `src/rowmap.rs` (mmap'd, process-shareable `row_id -> (session_id, message_id)` map built at prewarm, keyed by `(store_key, messages_version)`); the FTS/vector arms go index-only (`_rowid` + score, no `TakeExec`) and resolve hits through the map, with a `take_rows` miss-fallback. Falls back to a data-projection scan when no map is loaded.
- `aaf6a56` `perf(search): drop subagent SQL prefilter, exclude in memory` - removed `NOT source_agent LIKE '%/%'`; subagents now dropped in memory via `retain_non_subagents` (session_id contains `/`) before fusion, pool over-fetched x3/2; `build_filter` collapsed into `build_scope_filter`; `SearchPlan` gained `exclude_subagents`; the condition is shared via `default_excludes_subagents`.

Both passed `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and all 226 tests. A polish pass already fixed: unique temp filename in `RowKeyMap::build` (parallel-prewarm race), strictly-older-only `sweep_stale_rowmaps`, total `lookup` via checked `.get()`, `fts_scanner` extraction.

## 1. The measured perf landscape (do not re-derive)

Measured with `serve_mem_bench --io-trace --prewarm` against the real store `s3://pondarium/pond-full-corpus-benchmarking-copy`, warm cache, pool=100, per warm query:

| component       | GETs p50 | GETs p95 | bytes p50 |
| --------------- | -------- | -------- | --------- |
| `fts_search`    | 3        | 4        | 36 KB     |
| `vector_search` | 0        | 0        | -         |
| `scope_count`   | 0        | 1        | -         |
| `pond_search` (full request) | **133** | **206** | **6 MB** |
| **hydration** (derived = full - arms) | **~130** | **~201** | - |

Request breakdown (summed over 20 queries): `pond_search` issues ~92 scattered `data` GETs/query (~5.6 MB) plus ~43 `index/page_data.lance` GETs/query; a pathological **p95 tail of 206 GETs / 261 MB** for some sessions.

**Conclusion: the retrieval arms are at their floor (3 GETs = FTS posting lists; vector 0 after prewarm). Hydration is ~97% of the request and was never touched.** The row-key-map work cut the arm from ~60 -> 3; the full request is still ~133 because hydration dominates. The arm fix optimized ~2% of the request.

Baseline reference: before the map, the FTS arm was ~60 GETs/query (54 scattered `session_id`/`id` takes + 3 posting lists).

## 2. Root cause of the 130-GET hydration

Today's search flow:

```
arm (index-only) -> _rowid -> rowmap.lookup -> (session_id, message_id) keys
  -> fusion (hybrid) / normalize -> select_top_hits (on keys)
  -> message_metas_by_keys(IN-predicate scan) + summary_parts_for_messages + session_message_counts
```

`Store::message_metas_by_keys` (`src/sessions.rs`) **throws away the `_rowid` the arm already produced** and re-finds the rows with `session_id IN (...) AND id IN (...)`. That `IN x IN` is a cross-product scalar-index scan - it drives the ~43 `page_data.lance` index-page reads plus the scattered data reads, for rows we had exact addresses for moments earlier. `summary_parts_for_messages` adds parts-table reads for user-role hits (it already narrows to `SUMMARY_PART_TYPES`, so the big Text/Reasoning bodies are not read - good).

Note: the snippet text already comes from `search_text` on the messages row (read inside `message_metas_by_keys`), **not** from part bodies. So "skip part bodies" is largely already true; the cost is the IN-scan re-location plus reading `search_text` for the hits.

## 3. Workstream B - the hydration fix (`take_rows(row_id)`)

> Superseded as the *endgame* by the "Hydration architecture" section above: take_rows is now the **cache-miss fallback**, the resident meta cache is the primary path. The measured outcome below (bytes -81%, GETs only -16%) is exactly why: take_rows cuts bytes but leaves a round-trip floor that only the resident cache removes. Section kept for the measurement and the implementation mechanics, which the fallback still uses.

Carry the arm's `_rowid` through to hydration and replace the `IN`-predicate scan with `dataset.take_rows(row_ids, meta_columns)` - a precise by-row-id take of exactly K rows, no scalar-index scan. This is the **same primitive the rowmap miss-fallback already uses** (`Store::message_keys_by_rowids` in `src/sessions.rs`); generalize it to also project the meta columns (`role, project, source_agent, timestamp, search_text`). Expected: hydration ~130 -> ~K GETs (K = number of displayed hits), and the ~43 `page_data.lance` index reads disappear entirely.

**Proven vs projected - calibrate expectations before claiming a win.** What is *banked*: the arm 60 -> 3 GETs is committed and measured; queries are already faster than before this work regardless of B. What is *projected* (this fix): the "~130 -> ~K" is a reasoned hypothesis from the breakdown, **not yet measured** - validate it with the io-trace bench before reporting a number. Three honest caveats on magnitude: (1) the GET *count* drop is the high-confidence part - the `IN x IN` scan's ~43 `page_data.lance` index reads provably vanish when taking by exact row_id - but `take_rows` still issues GETs for K scattered rows, so it is 130 -> K-ish, not 130 -> ~0; (2) the ~5.6 MB of `search_text` *bytes* for the displayed hits largely remain (you must read what you render), so the latency win is mostly fewer round-trips, not less data; (3) the **p95 206 GETs / 261 MB tail** is inherent to a few huge sessions and needs the redesign's per-item truncation + row-ceiling backstop (section 5) to tame - `take_rows` alone will not. And it is all moot until the recall panic (section 7) is resolved: "fast" means nothing if hydrating certain rows still crashes. Measure, then claim.

Concrete changes:
- The arms (`fts_search_rowids` / `vector_search_rowids`, `src/sessions.rs`) already have `_rowid`. Stop discarding it: have the arm path surface `(row_id, key, score)` and carry `row_id` on the handler's `Candidate` (`src/handlers.rs`).
- Add `Store::message_metas_by_rowids(row_ids)` mirroring `message_keys_by_rowids` but projecting the full meta set; use it in `run_search` when row_ids are available (map loaded). Keep `message_metas_by_keys` as the no-map fallback.
- `select_top_hits` and the `min_score` gate operate before hydration, so only gated/selected rows get taken.

**The obstacle, and why A must come first:** hybrid **fusion** dedups by session-root and merges two arms, so a fused hit no longer owns a single `_rowid`. Removing hybrid (workstream A: `mode = vector | fts`) makes each hit **1:1 with a row_id**, so carrying it to `take_rows` is trivial. Until then, B is only cleanly doable for the single-arm (FTS-only / vector-only override) paths.

When no map is loaded (local tests, pre-prewarm) there are no row_ids from the index-only path - keep the existing `message_metas_by_keys` IN-scan as the fallback.

## 4. Workstream A - the tools redesign spec (as agreed)

**Surface: 4 tools.** `pond_get` splits into `pond_get_message` + `pond_get_session`. Plus `pond_search`, `pond_sql_query`.

**Global rules (all tools):**
- 10k-char output ceiling, enforced as **per-item truncation that fits all `limit` items** - never drops below `limit`, never a whole-response guillotine. Each truncated item carries a continuation marker.
- Progressive-disclosure markers everywhere: pagination up/down cursors, `pond_get_message` expansion ids, `fts`->SQL handoff. The output tells the agent how to get more.
- Conversational render = `search_text` (user/assistant text) only + tool_call/tool_result/file as one-line refs. **No reasoning parts rendered or indexed, ever.**
- One-line tool-call ref format (call-only): `<message_id> <tool_name>(<~100-char input preview>)`.

**`pond_search`** - params: `query, project, from_date, to_date, session_id, mode=vector|fts, limit (10, max 20 sessions), min_score (default 0.3)`. Removed vs today: `include_subagents`, `source_agent`, `format`; `hybrid` -> `vector|fts`; no `sort_by`.

| mode               | matches           | scored | gate                                  | ordering                     |
| ------------------ | ----------------- | ------ | ------------------------------------- | ---------------------------- |
| `vector` (default) | meaning           | cosine | raw cosine >= `min_score`             | cosine + recency-boost, desc |
| `fts`              | exact whole-words | no     | `min_score` disallowed -> error       | recency, desc                |

- Grouped by session, sessions ordered by best boosted hit; within a session, matching messages **newest-first**. **No per-session cap** - the 10k budget is the soft limit.
- Per-session footer (replaces the old cap, the supersession fix): `N newer messages in this session - conclusions may be revised; read from=end`.
- Each hit shows the matched user/assistant snippet + its one-line tool-call refs.
- `fts` zero-result -> SQL handoff: `WHERE search_text LIKE '%...%' AND project='...'`.
- `min_score` is the visibility gate -> "no results" is a trustworthy absence signal.

**`pond_get_message`** - params: `message_id, context_depth`. `message_id` can be ANY message (incl. tool_call/tool_result/system) -> returns its full parts (bounded + markers); this is how a tool body gets expanded from a ref. `context_depth` siblings render conversational. Top/bottom expansion markers.

**`pond_get_session`** - params: `session_id, limit, after_message_id, before_message_id, from=start|end`. Removed: `offset`, `response_mode`, `context_depth`. Returns user/assistant messages only, conversational + inline one-line tool-call refs; never expands tool bodies. Bidirectional pagination markers (top=`before_message_id`, bottom=`after_message_id`). `from=end` = latest state / post-compaction recovery.

**`pond_sql_query`** - params: `query, format=text|parquet|ndjson`. Removed: `json`. `text` = per-cell truncation + row ceiling, over-ceiling marker to `parquet|ndjson`; `parquet|ndjson` = full result to file + small resource link (bypasses 10k). Errors bounded and name the fix. The escape hatch for: substring/grep (`LIKE`), symbols (`8/8`, `cf_clearance`), cross-session analytics, subagents (`parent_session_id`), tool-body archaeology.

**Scoring (prerequisite that makes `min_score` real):**
- Split the score's two jobs: gating = raw cosine [0,1] (feeds `min_score`); ordering = cosine + recency-boost.
- Recency boost: additive, magnitude 0.1, scale 30 days, post-gate, exp decay - a gentle cross-session tiebreaker that never makes old content invisible (the gate does the filtering).
- Intra-session supersession is handled by the footer + newest-first, not the boost.

**Decision rule (bake into tool descriptions):** concepts -> `pond_search vector` | known exact words -> `pond_search fts` | symbols/substrings/chars, analytics, subagents -> `pond_sql_query`. Find a thread -> `pond_search` -> read the arc -> `pond_get_session` -> expand a tool/any message -> `pond_get_message` -> latest state -> `from=end`.

Open decision (set): `min_score` default `0.3`. **Caveat: 0.3 is on RAW cosine and is currently a guess - calibrate it against the recall-TSV known-good pairs before locking, or it reintroduces the false-negative problem the gate is meant to kill** (see the `pond_search false-negative` memory). Everything else above is agreed.

## 5. Sequencing and the synergy with B

1. Land the redesign's retrieval shape first: `mode = vector | fts` (drop hybrid/fusion) + the split gating/ordering score with a real `min_score` cosine gate.
2. Then land B (`take_rows(row_id)` hydration) - now trivial because each hit is 1:1 with a row_id and only gated rows are hydrated.

Why they compound:
- **No hybrid -> no fusion -> row_id is 1:1 with each hit** (the single biggest unblock for B).
- **Real `min_score` gate -> fewer rows hydrated** (multiplies with the take_rows reduction).
- The committed `aaf6a56` subagent exclusion becomes **always-on** under the new surface (no `include_subagents`/`source_agent` params; subagents go to `pond_sql_query` via `parent_session_id`). `default_excludes_subagents` collapses to a constant; the x3/2 over-fetch stays relevant.
- The committed `src/rowmap.rs` is exactly the structure B reuses.

Tension to design around: "no per-session cap, 10k budget is the soft limit" sizes hydration to fill a char budget, and the p95 tail is 206 GETs / 261 MB. **Add a hard row-ceiling backstop under the char budget and hydrate incrementally (stop reading once the budget is met) rather than hydrate-then-trim.** Also bound the number of one-line tool-call refs rendered per message (each ref's name + ~100-char preview is a parts read).

## 6. Per-piece implementation map (files / current -> target)

- **`pond_search` params + modes** - `src/wire.rs` (`SearchRequest`/`SearchFilters`: drop `include_subagents`, `source_agent`, `format`, `sort_by`; `hybrid` -> `vector|fts`); `src/handlers.rs` (`plan_search`, `run_search` - remove the `SearchMode::Hybrid` fusion branch and `fuse_arms`; keep single-arm normalize; `build_sessions` grouping + newest-first + footer); `src/transport.rs` (`render_search_transcript`).
- **Real min_score gate + score split** - `src/handlers.rs` (gate on raw cosine before `select_top_hits`); recency boost in the ordering score (`src/embed.rs` has the model; boost is post-gate arithmetic in the handler). See the `pond_search false-negatives` and `hybrid fusion over-hydration` memories.
- **Hydration via row_id (B)** - `src/sessions.rs` (`message_metas_by_rowids` new, mirror `message_keys_by_rowids`); `src/handlers.rs` (`Candidate` carries `row_id`; `run_search` uses the rowid hydrate when a map is loaded).
- **`pond_get` split** - `src/handlers.rs`, `src/wire.rs`, `src/transport.rs`, and tool registration (`pond_get_message` any-message full-parts; `pond_get_session` user/assistant-only + bidirectional cursors).
- **`pond_sql_query`** - drop `json` format (`src/handlers.rs` / `src/sql.rs` / `src/transport.rs`); keep `text|parquet|ndjson`.
- **10k per-item truncation + markers** - the render layer (`src/transport.rs`); shared truncation helper that fits all `limit` items.
- **Conversational render = search_text + one-line tool refs, no reasoning** - `src/transport.rs` render + confirm `search_text` extraction excludes reasoning (`src/sessions.rs` `search_text`).

## 7. Open blocker - the recall panic (must resolve before push)

A `--recall` run (`ops/search-benchmarks/queries-en.tsv`, hybrid, --prewarm) scored 14/21 queries with healthy ranks (in line with the baseline cluster), then **panicked inside Lance**: `called Option::unwrap() on a None value` at `lance .../src/encodings/logical/primitive.rs:3656` during hydration of query ~15. This crashes the search path on real data and **gates the push**.

Not yet diagnosed. Hypotheses (the new agent must confirm with `RUST_BACKTRACE=1`):
- Most likely a Lance decoder bug on one row's encoding, hit in shared hydration (`message_metas_by_keys` decoding `search_text`, or `summary_parts` decoding `variant_data`) - code unchanged by the two commits, so pre-existing.
- Possible that `aaf6a56`'s x3/2 over-fetch changed which rows reach hydration and thereby **exposed** a latent bad row the old selection happened to miss (root cause still Lance, but our change surfaces it).
- The new index-only arms and the `take_rows` fallback decode fewer/no data columns and (on this static store) trigger no map misses, so they are unlikely to be the unwrapping decoder.

How to settle it: re-run recall with `RUST_BACKTRACE=1` to get the pond call path; if it points at shared hydration, run the same recall on `e5bcfb3` (pre-change) to confirm pre-existing-vs-exposed. The workstream-B `take_rows(row_id)` hydration may sidestep the bug (different read path) - verify, do not assume.

## 8. Validation playbook

Store + creds (resolved from the pond config, no inline secrets needed):
- `--storage-path "s3+https://nbg1.your-objectstorage.com/pondarium/pond-full-corpus-benchmarking-copy"`
- `--config /Users/tenequm/.config/pond/config.toml` (its `[creds.default]` block carries the pondarium key/secret)

Commands (build the bench with the io-trace feature):
- Hydration / arm GETs: `cargo bench --manifest-path /Users/tenequm/Projects/pond/Cargo.toml --bench serve_mem_bench --features io-trace -- --storage-path <URL> --config <CFG> --prewarm --io-trace` - prints the component table incl. the `pond_search` full-request row and the derived `hydration` line.
- Recall: `... --features io-trace -- --storage-path <URL> --config <CFG> --prewarm --recall ops/search-benchmarks/queries-en.tsv` - baseline to beat/match: `Success@3=0.762 P@1=0.381 MRR=0.562`.
- Local gate (always): `cargo fmt --check`; `cargo clippy --all-targets -- -D warnings`; `cargo test` (226 currently).

Operational caveats (cost the last session a lot of time):
- Each remote run pays a **cold prewarm** (cold FTS index load is 175-442 s; the rowmap build adds ~3 s). Running cold remote benches back-to-back **throttles the Hetzner endpoint** - we saw a ~32-minute prewarm stall after ~5 cold runs. **Run one at a time and space them out;** if a run stalls in prewarm with no output, it is throttle, not a hang - back off.
- `serve_mem_bench`'s `sql_cold`/`sql_steady` warmup runs a `SELECT MIN(timestamp), MAX(timestamp)` that **flakily trips pond's 30 s SQL guard** on a cold store and aborts the whole run. The uncommitted bench change (section 9) gates those phases off for the isolation modes (`--io-trace`/`--recall`/`--attribute`); keep it.
- AIMD throttle knobs are Lance-owned - **do not touch them** (prior explicit instruction).

## 9. Working-tree state to inherit

- **`benches/serve_mem_bench.rs` (uncommitted - KEEP).** Two changes: (a) `sql_cold`/`sql_steady` gated behind `let isolation = args.attribute || args.io_trace || args.recall.is_some();` so isolation modes skip the flaky SQL warmup; (b) a `pond_search` full-request component in the `--io-trace` mode plus a derived `hydration` line. This is the tool that measures workstream B. Commit it as e.g. `test(bench): isolate pond_search hydration GETs, gate sql warmups`.
- **`AGENTS.md` (uncommitted - NOT this work).** An unrelated change replaced the repo's `## Comments` guidance with a different `<comment_instructions>` block; it diverges from `CLAUDE.md` (which still has the original) and was not made as part of this task. Leave or revert per the user; do **not** fold it into a task commit.
- Commits `fda30d9` and `aaf6a56` are **local, unpushed**; push is blocked on section 7.

## 10. References

- Spec: [`docs/spec.md`](../spec.md) sections on search and datasets (read before changing behavior).
- Prior plans: [`2606-17-remote-read-perf-and-index-cleanup.md`](2606-17-remote-read-perf-and-index-cleanup.md), [`pond-get-three-mode-redesign.md`](pond-get-three-mode-redesign.md).
- Research: [`docs/researches/embeddings.md`](../researches/embeddings.md) (fusion/scoring rationale).
- Relevant memories (background, verify before relying): pond_search false-negative causes; pond remote S3 read perf; pond hybrid fusion over-hydration; pond unimplemented optimizations; Lance-native no-direct-FS.
