# Read path: get-family optimizations (ship 2026-08-12)

Status: implementing. Evidence base: `docs/researches/2608-12-read-path-where-time-goes.md` (all numbers measured on the live `s3+https://nbg1.../pondarium/pond` store, 14.3k sessions / 2.64M messages, 2026-08-12).

## Problem

The get family is the slowest read surface pond has: 30-day observed p50 is 29s (`pond_get_session`) to 84s (`pond_get_message`), with zero calls under 5s. Cold CLI probes: 73s to page a 25-message session by session id, 166s by message id, repeat runs identical. A warm `pond_get_message` issues ~10,900 S3 range-GETs (~42 MiB in ~4 KiB reads), 97% against table data files - vs 291 GETs for a full `pond_search`. Two mechanisms dominate:

1. **Message-id -> session resolution is a full remote scan.** `messages.id` lost its btree in the post-overhaul cleanup ("rare full-scan") before the MCP tool split made message-id gets the standard post-search flow. Measured: ~93s of the 166s probe.
2. **The get path bypasses the rowmap.** `session_view` scans the whole session's messages from S3 to serve one page, and Lance never caches data pages - while the mmap'd rowmap (`~/.cache/pond/rowmetamap-*`) already holds, resident, every message's `(row_id, session_id, message_id, role, project, source_agent, timestamp, search_text)` plus per-session aggregates. Search hydration uses it; gets do not.

## Fixes in this change

### A. Resolve message ids from the rowmap

`RowMetaMap` gains a by-message-id lookup (linear scan over the mmap'd records, decompressing nothing but the matched row; ~2.6M records is tens of ms resident, vs 93,000ms for the remote scan). `Store::session_id_for_message` consults the loaded rowmap chain first.

Correctness: pond is append-only and message ids are immutable, so **a rowmap hit is definitive at any map version** - no version gate needed on hits. Only a miss (a message newer than the map, or no map loaded) falls back to today's full scan, which stays the authority.

### B. Serve `session_view` pages from the rowmap, version-gated

When the loaded map's version equals the current `messages` version (one cheap manifest read, already freshness-cached), `session_view` builds its conversational page from the map: filter records by session, treat non-empty `search_text` as conversational (the map stores storage-null as `""`, and storage guarantees `search_text` is null-or-non-empty), sort by `(timestamp, id)`, page, decompress text blocks only for the emitted window. On any version mismatch or missing map it falls back to today's scan path unchanged - staleness can never drop a newly synced message from a page.

`find_session` (session metadata) and `summary_parts_for_messages` (the page window's part badges) keep their current reads; parts residency ("5c") is explicitly out of scope today.

### C. One-shot read commands get the warm-state plumbing

- `pond get-session` / `pond get-message` / `pond sql` open the store with the disk `_indices` cache (today only serve/mcp/search do), so cold scalar-index loads read local disk instead of S3.
- The get CLI paths load an already-published rowmap chain when present (`load_rowmap_if_present`, as `pond search` does), so fixes A/B apply to one-shot invocations, not just the MCP server.

## Out of scope today (deliberate)

- **5c parts-summary residency** - the remaining S3 read on search hits and get pages; own change.
- **`message_view`'s whole-session window scan** - fix A removes its ~93s resolution cost; the window scan needs `content` and non-conversational rows the map does not carry.
- **FTS postings prewarm, concurrency semaphore, serve-topology change** (`pond serve --with-sync` + HTTP MCP registration), **Lance v10 upgrade track** (unblocks timestamp zonemap under DataFusion 54, COUNT(*) pushdown with stable row ids, FTS format v2, miniblock chunk tuning) - each its own PR.

## Verification protocol (all against the live remote corpus)

1. `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` - green before any remote run.
2. Unit tests: by-message-id lookup (hit, miss->fallback), map-served vs scan-served `session_view` equivalence on a seeded store, version-mismatch fallback.
3. Cold CLI probes, patched binary vs 0.14.6 baselines already recorded: `get-session <session-id>` (73s baseline), `get-session <message-id>` (166s), `get-message <message-id>` (153s). Expect the message-id delta (~93s) to collapse to ~0 and the session-id case to drop to parts-scan cost.
4. `cargo bench --bench serve_mem_bench --features io-trace -- --storage-path <store> --io-trace`: `pond_get_message` GETs must drop from ~10,900 p50 to the parts/window residual; `pond_search` components must not regress.

## Ship

Feature branch `feat/read-performance-optimizations` -> PR -> `main`; then merge the `chore: release vX.Y.Z` PR release-plz opens (patch bump; commits are `perf:`/`docs:` types).

## Session state (2026-08-12 pre-compaction checkpoint - resume here)

Working tree: the `feat/read-performance-optimizations` worktree at `.claude/worktrees/feat+read-performance-optimizations`, `origin/main` (827ba3c, the pi PR) merged in. **Implementation is COMPLETE and validated locally but NOT yet committed** - src changes are uncommitted by design until the final remote verification passes.

Implemented (all in working tree):
- `rowmap.rs`: `RowMetaMap::session_id_for_message` (linear header scan, length-check first), `RowMetaMap::session_row_ids` (all roles), both mirrored on `RowMetaSet` (deltas-first for the id lookup); unit test `message_and_conversational_lookups_cover_the_chain`.
- `sessions.rs`: `session_id_for_message` consults the map first (hit definitive - append-only + immutable ids); `session_rows_resident` (version-gated map source) feeding `conversational_rows_resident` (session_view) and `message_scan_rows_resident` (message_view; returns None for a system-role target so its `content` - absent from the map - survives via the scan). Store test `get_paths_serve_from_resident_map_and_fall_back_on_staleness` covers map/scan page equality, map id-resolution, staleness fallback (uses `ingest_events`, NOT `IngestValidator`, for the second batch - the validator enforces Session-first ordering), message_view siblings, and the system-target content fallback.
- `main.rs`: GetSession/GetMessage/Sql now open with the disk index cache; the two gets call `load_rowmap_if_present` (installs only on exact version match - fine because sync re-extends the chain before exiting).

Validation state: `cargo fmt` clean, `cargo clippy --all-targets -- -D warnings` clean, full `cargo test` green (277+32+83 tests). Remote (live store, baselines from pond 0.14.6 measured today): get-session by session id 73s -> 67/50s; by MESSAGE id 166s -> ~68s (the ~93s unindexed-id scan eliminated); get-message 153s -> ~61s; map-vs-scan CLI output **byte-identical** (probe script `get_probes_patched.sh` in the session scratchpad checks this via XDG_CACHE_HOME override). io-trace after fix A only: warm pond_get_message 10,900 -> 3,977 GETs; the message_view map path (added after that run) targets the remaining ~4k.

Probe ids: session `8b7b9e47-66d2-464b-8ec6-0ad70855ff57`, message `419caaa5-13d7-448a-807c-5fb5105112a7`. Bench: `cargo bench --bench serve_mem_bench --features io-trace -- --storage-path 's3+https://nbg1.your-objectstorage.com/pondarium/pond' --io-trace`.

REMAINING (in order):
1. Collect the in-flight background run: release rebuild + probe rerun (expect get-message to drop further; equivalence must stay OK).
2. Final io-trace run: warm `pond_get_message` GETs should collapse toward the parts-window residual (hundreds, not thousands); `pond_search` components must not regress vs (fts 32-50 / vector ~101 / search ~291-331 iops p50).
3. Commit src + tests as `perf(read): resolve message ids and serve session pages from the resident rowmap` (user authorized ship); researches doc `docs/researches/2608-12-read-path-where-time-goes.md` rides the same PR.
4. Push branch, open the feature PR (NOT a release PR), merge to main after CI.
5. release-plz auto-opens `chore: release vX.Y.Z` (patch); enrich the changelog entry under the canonical headers with the measured numbers, then merge it - that merge publishes.

Known follow-ups (explicitly deferred, do not fold into this PR): erase-vs-stale-map can return `internal` instead of `not_found` for <=30s in long-lived servers (downgrade in handler later); 5c parts-summary residency; FTS postings prewarm; concurrency semaphore; serve-topology recommendation (`pond serve --with-sync` + HTTP MCP registration); Lance v10 upgrade track (unblocks timestamp zonemap under DataFusion 54, COUNT(*) pushdown with stable row ids, FTS format v2 via index rebuild, `LANCE_MINIBLOCK_MAX_VALUES` for variant_data, exact-IS-NULL zonemaps).
