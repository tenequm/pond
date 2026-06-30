# Remote read-path: kill cold-start, fix date filtering, bound per-instance RAM

Status: ready to implement. Owner: TBD. Prereqs: none (pre-1.0, breaking changes free). Storage of record: `s3+https://nbg1.your-objectstorage.com/pondarium/pond`.

This plan makes `pond_search` fast and correct against remote S3, for the real deployment shape: **many short-lived `pond mcp` processes** (one per agent session), not one long-lived server. Line numbers are approximate - verify by symbol name. Read the cited `docs/spec.md` sections before changing behavior.

## 0. Read first

- `docs/spec.md`: 3.2 (`lance-chokepoints` - esp. `storage`), 3.5 (`lance-handle-freshness`), 8 (search; `search-prefilter-pushdown`, `search-absence-honesty`).
- Prior plan this completes: `docs/plans/2606-19-tools-redesign-and-hydration-perf.md` - the "Hydration architecture" section built the on-disk **rowmap** but left the retrieval **index** in memory on a "long-lived server" assumption (line 69) that does not hold here. This plan finishes that design.
- GitHub issue #75 (the date-filter bug) - root cause + fix already written up there; Workstream A is its implementation.

## 1. The problem, measured (do not re-derive)

Measured this cycle on the real corpus (~2.14M messages, 292,594 embedded):

- **Cold first-search is the slowness: ~36-50 s** (measured 37 s and 50 s via `pond search` CLI on S3). The query itself is cheap once warm - a verbose run showed the filtered read at `iops=7, bytes_read=133 KB, indices_loaded=0`. The ~40 s is the **one-time load of the IVF + FTS index from S3**. (The old "175-442 s" figure is stale - refuted by the 2026-06-20 e2e to ~36 s; do not cite it.)
- **Why it is paid every session:** the IVF + FTS index lives in Lance's **in-memory** cache (no on-disk cache in Lance; confirmed). A fresh `pond mcp` reloads it from S3 every time. The **rowmap** (hydration/meta) is already on disk (`~/.cache/pond`: rowkeymap ~210 MB + rowmetamap ~286 MB, version-named, mmap'd, cross-session) - so hydration is warm across sessions, but the index is not. Half the design shipped.
- **Date filtering returns empty (#75):** `from_date`/`to_date` collapse the in-scope set to ~0 on S3 (a far-past `from_date=2020` also returns 0 - the always-false signature). Correctness bug, independent of the perf work.
- **Index footprint (full corpus, current set):** vector `auxiliary.idx` ~14 MB, FTS `part_0_invert.lance` ~54 MB + `tokens` ~8 MB, `session_id` BTree `page_data.lance` ~51 MB (paged, small resident), ZoneMap ~12 KB, Bitmap <1 MB. Per-instance **resident** parsed index ~30-100 MB; ~0.3-1 GB across 10 active instances (heap, not the shared mmap). The embedder (~790 MB, already idle-evicts to ~106 MB) dwarfs it.

## 2. Approach (and what we rejected)

Complete the 2606-19 design: extend the on-disk caching that already works for the rowmap to the **index files**, so a fresh process loads the index from local disk, not S3. Keep Lance's indexes and retrieval as-is.

Rejected (with reasons, so a fresh agent does not re-propose them):
- **Brute-force vector in RAM / drop the IVF** - introduces a scaling cliff, a retrieval rewrite, and only fixes the vector arm. No.
- **A `project` index** - `project` is already a cheap `refine_filter` over the candidate set (confirmed via `--explain`); the BTree was correctly dropped in 2606-17. No.
- **Moving the scalar prefilter to a RAM post-filter / amending `search-prefilter-pushdown`** - unneeded; the prefilter is cheap and Lance has no row-id-mask API anyway. No.
- **Tuning the AIMD limiter** - banned (`CLAUDE.md`).

## 3. Scope & sequencing

- **A - #75 date filter** (correctness; ship first, standalone).
- **B - disk index cache** (the cold-start fix; the core change).
- **C - rowmap in every search entry point** (rides B; the CLI/one-shot win).
- **D - idle-eviction of the parsed index** (bounds per-instance RAM for many concurrent sessions).
- **Optional - `scope_count` from the rowmap; `summary_parts` resident** (small, not bugs).

Each is independently shippable. Validate every change with `cargo fmt --check` + `cargo clippy --all-targets -- -D warnings` + `cargo test`, and the perf claims on the **real S3 store** (a clean cold process), not just locally.

## 4. Workstream A - fix #75 (date filter returns empty)

**Root cause:** `date_bound` (`src/handlers.rs:~1760`) emits a timezone-NAIVE literal:
```rust
let time = if end_of_day { "23:59:59" } else { "00:00:00" };
Ok(format!("timestamp '{date} {time}'"))
```
wrapped as `Predicate::Gte/Lte("timestamp", ScalarValue::Raw(...))` (`handlers.rs:~1727-1738`); `ScalarValue::Raw.to_lance()` emits it verbatim (`substrate.rs:~1483`) so the pushed filter is `timestamp >= timestamp '2026-06-16 00:00:00'`. The column is `Timestamp(Microsecond, Some("UTC"))` (`sessions.rs:~4381,4395`) - tz-aware. DataFusion (the `pond_sql_query` path) coerces naive-vs-aware; Lance's scanner/zonemap prefilter does not -> always-false -> prunes everything.

**Change:**
- `date_bound`: emit a **tz-aware UTC** literal. Try `timestamp '{date} {time}+00:00'` (or `...Z`) first; if Lance's filter grammar rejects the offset form, fall back to `arrow_cast('{date} {time}Z', 'Timestamp(Microsecond, Some("UTC"))')`. Decide empirically against the real column.
- **Preserve pushdown:** confirm via `pond search --explain` on S3 that the corrected literal still resolves through `ScalarIndexQuery ... @messages_timestamp_zonemap` and does **not** degrade to a full `timestamp`-column scan.
- **Replace the string-only test** `build_scope_filter_pushes_down_each_predicate_and_handles_empty` (`handlers.rs:~1935`) - which only asserts the `to_lance()` string and is why this shipped green - with an **execution** test: build a tiny store with a `Timestamp(us,"UTC")` column, assert `from_date`/`to_date` return the right rows AND that a far-past `from_date` returns everything.
- **Validate on S3:** `from_date=2026-06-16` reports ~17,221 in scope (non-subagent) and real hits, not 0.

No spec change (this restores documented behavior).

## 5. Workstream B - disk index cache (the cold-start fix)

**Goal:** a fresh process reads the IVF + FTS (and any `_indices/*`) from a local on-disk cache; only the first process to touch a given index version pays the S3 fetch.

**Mechanism - a `WrappingObjectStore` (Lance's own seam; spec-compliant because it sits *inside* the object-store layer, not a direct-FS reach-around per `lance-chokepoints-storage`):**
- New `CachingObjectStore` implementing `lance_io::object_store::WrappingObjectStore` (trait at `lance-io object_store.rs:163`; model on `IOTracker` at `lance-io tracking_store.rs:94`). It wraps the inner store; for `get`/`get_opts`/`get_range` on a path under `_indices/`, serve from the local cache file if present, else fetch from the inner store, write it (temp + atomic rename), and serve. Pass everything else straight through. Whole-object caching (index files are small; range reads slice the local copy).
- **Wire it in** at `open_or_create_via_ns` (`src/substrate.rs:~3099`), where pond already injects a wrapper (`object_store_wrapper`, `:3119-3136`, currently `io_trace::wrapper()` as an either/or). Change that to **chain** the cache wrapper with the io-trace wrapper via `ChainedWrappingObjectStore` (`lance-io object_store.rs:172`, `add_wrapper`). Construct the cache wrapper once per store; ensure it is applied on **every** dataset open AND the freshness re-open path (verify the handle-refresh re-open carries the same params).
- **Cache location:** `~/.cache/pond/<store_key>/indices/...` mirroring the object path. Reuse `default_cache_dir()` (`main.rs:~966`) and `store_key()` (`sessions.rs:~1558`, blake3 of the store URL) so the cache is per-store and shared by all local processes.
- The cache sits **outside** Lance's AIMD limiter, so hits never touch S3 or the limiter.

**GC:** sweep cached `_indices/<uuid>/` dirs whose UUID is not in the current manifest's index list (unlink-safe for in-flight readers, mirroring `sweep_stale_rowmaps`, `sessions.rs:~1720`); bound with an LRU + size cap. Current index set is ~130 MB; without GC the stale orphan segments would accumulate (the live store already shows a 368 MB `_indices` dir that is mostly orphans).

**Result:** cold-start drops from ~40 s to a local read for every process after the first; correctness is trivial because index files are immutable + UUID-addressed (a new index = a new UUID = an automatic miss = one fetch).

## 6. Workstream C - rowmap in every search entry point

`Command::Search` (`src/main.rs:~1238-1280`) opens the store and calls `handlers::pond_search` directly - it never calls `ensure_rowmap`, so a one-shot CLI search falls back to S3 IN-scan hydration. The map is already on disk; just open it.

**Change:** add a **load-existing-only** variant of `ensure_rowmap` (`sessions.rs:~1586`) - open a sibling-published chain if one exists at the current version; do **not** take the build `flock` / full-scan for a one-shot (fall back to take_rows for that single invocation, per 2606-19 line 75). Call it in `Command::Search` before `pond_search`. With B, a CLI search with any warm sibling = local index + local rowmap = fast; a truly-cold machine pays S3 once to populate both caches.

## 7. Workstream D - idle-eviction of the parsed index (bound per-instance RAM)

The disk cache makes a reload cheap (local), which lets us free the parsed index from a *quiet* process and reload it on demand - so N concurrent idle sessions do not each pin ~30-100 MB of heap.

**Mechanism (option b - drop the cached `Dataset` handle; mirrors the embedder):**
- The embedder already idle-evicts: `LazyEmbedder` with `DEFAULT_IDLE_EVICTION` (`src/embed.rs:~177`). Reuse the same pattern for the dataset handle.
- The `Handle` caches `Dataset`(s) behind a freshness gate (`substrate.rs` `dataset()`, `~1522`; refresh logic `~1616-1622/1751-1755`). Track last-access; after the idle window, drop the cached `Arc<Dataset>` (set the slot to `None`). This frees Lance's per-process index cache held by that dataset. **`Arc`-safe:** an in-flight query holds its own `Arc`, so the heap frees only when the last ref drops; the next query re-opens via the existing freshness path - which, with B, re-reads the index from the local disk cache (fast), not S3.
- Make the idle window configurable under `[runtime]` (default conservative, e.g. 60-120 s), same shape as the embedder knob.

**Interaction with prewarm:** `spawn_prewarm` (`main.rs:~965`) + its 30 s refresh loop (`~979-984`) stay - prewarm still warms the **shared disk cache** for the first process (so siblings read disk, not S3) and keeps the rowmap fresh. With B, a query that races prewarm is no longer catastrophic (it reads the disk cache if a sibling already populated it). D simply releases the parsed heap when a process goes quiet; the next query re-warms from disk.

**Net:** idle instances -> ~0 parsed-index RAM; active -> ~30-100 MB; every (re)load from the shared local disk cache. 10+ concurrent `pond mcp` is comfortable.

## 8. Multi-instance concurrency (the load-bearing constraint)

10+ `pond mcp` on the same store must share the on-disk caches and track remote-index updates the same way the rowmap already does. The disk index cache mirrors the rowmap's proven model:

- **Per-store, shared files:** `store_key()` namespaces the cache dir; all instances on one store share the files; distinct stores never collide.
- **Path-keyed, immutable, zero invalidation:** `_indices/<uuid>/...` files are immutable. Hit -> serve; miss -> fetch + atomic-rename. Concurrent identical writes are safe (same bytes, last rename wins).
- **No thundering herd:** a per-UUID `flock` (mirroring the rowmap build lock, `sessions.rs:~1627-1636`) dedupes the fetch when all instances miss a new index at once - one fetches, the rest wait and read.
- **Continuous update, for free:** handle freshness (`lance-handle-freshness`, ~5 s on S3) refreshes the manifest -> a new index UUID is referenced -> cache miss -> one (flock-deduped) fetch -> cached. No separate refresh loop; the index cache follows the manifest exactly as the rowmap follows `messages_version`.
- **Unlink-safe GC** of UUIDs no longer in the manifest (POSIX keeps in-flight readers' inodes alive), like `sweep_stale_rowmaps`.

**Sharing boundary (honest):** the disk **files** are shared (one copy in OS page cache across all instances) and reloads are local-fast; the **parsed** index is still per-process heap (Lance's index cache is not mmap-shareable). The rowmap stays *fully* shared (mmap). D keeps the per-process parsed copy small/transient. Truly sharing the parsed index would require the rejected brute-force rewrite.

## 9. Optional finishes (not bugs; lower priority)

- **`scope_count` from the rowmap:** `searchable_in_scope` filtered path (`sessions.rs:~2199`) does an S3 `count_rows`; the rowmap holds `project`/`timestamp`/`session_id`/`search_text`, so count resident records matching the scope in RAM instead. Also yields the correct #75 absence count. Caveat: rowmap stores null `search_text` as `""`, so "searchable = non-empty" (near-exact).
- **`summary_parts` resident:** the last hydration S3 read (`summary_parts_for_messages`, `sessions.rs:~2908`) scans the parts table for user-role hits. Make it resident (extend the rowmap build to capture user-role summary refs). Finishes 2606-19's phase 3.

## 10. Out of scope / do NOT do

- No brute-force vector / drop-IVF; no `project` index; no prefilter->post-filter or `search-prefilter-pushdown` amendment; no AIMD tuning.
- Leave the small indexes (ZoneMap, `source_agent` Bitmap) as-is - negligible RAM, not worth touching now.
- Do not cache data files or manifests in the wrapper - the rowmap already serves hydration, and manifests need the latest (freshness). Scope the cache to `_indices/*` only.

## 11. Validation

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.
- **A (#75):** execution test (not string-only); `--explain` on S3 confirms zonemap pushdown preserved; `from_date=2026-06-16` reports ~17,221, not 0.
- **B (disk cache):** measure cold-start on the real S3 store - first process ~40 s (populates cache), **second process local-read fast**. Confirm GC reclaims stale UUIDs. Confirm the wrapper composes with the existing io-trace wrapper (`ChainedWrappingObjectStore`) and sits outside AIMD.
- **C:** a one-shot `pond search` with a warm sibling hydrates from the local rowmap (no S3 IN-scan).
- **D:** an idle process's RSS drops by the parsed-index footprint after the idle window; the next query re-warms from the disk cache (local-fast), not S3. No crash/race with an in-flight query at eviction time.
- Multi-instance: run several `pond search` against the S3 scratch concurrently; confirm one fetch per new index UUID (flock), shared cache files, no corruption.

## 12. Measured evidence appendix

- Cold first-search: ~36-50 s (index load from S3); warm query iops=7 / 133 KB.
- Current index set on disk: vector `auxiliary.idx` ~14 MB; FTS `invert` ~54 MB + `tokens` ~8 MB; `session_id` BTree `page_data` ~51 MB (paged); ZoneMap ~12 KB; Bitmap <1 MB. `_indices` dir 368 MB total (mostly stale orphans -> GC target). 292,594 embedded vectors.
- Per-instance resident parsed index ~30-100 MB; ~0.3-1 GB across 10 active instances (heap). Embedder ~790 MB (idle-evicts to ~106 MB).
- Rowmap on disk: rowkeymap ~210 MB + rowmetamap ~286 MB, version-named, mmap'd, cross-session shared.
- #75: `from_date` -> 0 in scope on S3 (incl. `from_date=2020`); same predicate via `pond_sql_query` returns 17,221 (DataFusion coerces).
