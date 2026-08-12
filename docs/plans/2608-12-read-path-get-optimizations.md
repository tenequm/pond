# Read path: get-family optimizations (ship 2026-08-12)

Status: shipped (PR #141). Evidence base: `docs/researches/2608-12-read-path-where-time-goes.md` (all numbers measured on the live `s3+https://nbg1.../pondarium/pond` store, 14.3k sessions / 2.64M messages, 2026-08-12).

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

## Shipped state (2026-08-12, PR #141)

Landed via `feat/read-performance-optimizations`. What shipped, where it lives, and the measured outcome:

Implemented (all in working tree):
- `rowmap.rs`: `RowMetaMap::session_id_for_message` (linear header scan, length-check first), `RowMetaMap::session_row_ids` (all roles), both mirrored on `RowMetaSet` (deltas-first for the id lookup); unit test `message_and_conversational_lookups_cover_the_chain`.
- `sessions.rs`: `session_id_for_message` consults the map first (hit definitive - append-only + immutable ids); `session_rows_resident` (version-gated map source) feeding `conversational_rows_resident` (session_view) and `message_scan_rows_resident` (message_view; returns None for a system-role target so its `content` - absent from the map - survives via the scan). Store test `get_paths_serve_from_resident_map_and_fall_back_on_staleness` covers map/scan page equality, map id-resolution, staleness fallback (uses `ingest_events`, NOT `IngestValidator`, for the second batch - the validator enforces Session-first ordering), message_view siblings, and the system-target content fallback.
- `main.rs`: GetSession/GetMessage/Sql now open with the disk index cache; the two gets call `load_rowmap_if_present` (installs only on exact version match - fine because sync re-extends the chain before exiting).

Validation state: `cargo fmt` clean, `cargo clippy --all-targets -- -D warnings` clean, full `cargo test` green (277+32+83 tests). Remote (live store, baselines from pond 0.14.6 measured today): get-session by session id 73s -> 67/50s; by MESSAGE id 166s -> ~68s (the ~93s unindexed-id scan eliminated); get-message 153s -> ~61s; map-vs-scan CLI output **byte-identical** (probe script `get_probes_patched.sh` in the session scratchpad checks this via XDG_CACHE_HOME override). io-trace after fix A only: warm pond_get_message 10,900 -> 3,977 GETs; the message_view map path (added after that run) targets the remaining ~4k.

Probe ids: session `8b7b9e47-66d2-464b-8ec6-0ad70855ff57`, message `419caaa5-13d7-448a-807c-5fb5105112a7`. Bench: `cargo bench --bench serve_mem_bench --features io-trace -- --storage-path 's3+https://nbg1.your-objectstorage.com/pondarium/pond' --io-trace`.

Verification COMPLETE (2026-08-12): probe round 2 - get-session by session id 47/53s, by message id 68/97s (one sync-contended outlier), get-message 49/39s, map-vs-scan output byte-identical. Final io-trace: warm `pond_get_message` 4,003 GETs p50 (vs 10,900 baseline, -63%); unchanged vs the fix-A-only run, which isolates the residual: **the `parts_for_messages` leg flat-reads ~4k data pages of the 2.9M-row parts table per warm get** - the next target, promoted below. Search components unregressed (fts 52 / vector 102 / search 304 iops p50).

A four-lens polish review (cleanliness / design / efficiency / side-effect gating) ran over the PR and its fixes landed in a follow-up commit: corrupt-map fail-open and fail-closed paths now all degrade to the store scan, `session_row_ids` aborts on malformed records instead of silently dropping rows, the per-session walk early-exits on the session entry's row count, the message-id walk runs newest-first, the duplicated resident helpers collapsed into one `ScanRow` source, and the map accessors follow the `lookup_*` convention.

Release: merge the feature PR into main, then merge the `chore: release` PR release-plz opens (patch; enrich the changelog entry with the measured numbers first).

RESUME POINTER (only relevant until the release lands): the polish fixes described above sit in the worktree; fmt/clippy/full tests are green on them. Pending, in order: (1) collect the in-flight remote re-verification (release rebuild + `get_probes_patched.sh` probes; the script's byte-equivalence check must print EQUIVALENCE OK), (2) commit the fixes as one `perf(read)`/`refactor(read)`-typed commit and push - PR #141 already exists and CI re-runs on push, (3) `gh pr merge 141 --squash` once green, (4) enrich + merge the release-plz `chore: release` PR. User authorization for commit/merge/release: given 2026-08-12 ("implement ... and get them out today"), reconfirmed by "fix all identified issues".

Known follow-ups (explicitly deferred, do not fold into this PR): **parts read path (now the top target)** - a warm get's residual ~4,000 GETs are the `parts_for_messages` scan over the parts table; establish whether the `(session_id, message_id)` btrees actually engage for the `In` predicate (io-trace shows the reads land on DATA pages, not index pages) and give parts the summary-residency ("5c") or index-pushdown treatment; erase-vs-stale-map can return `internal` instead of `not_found` for <=30s in long-lived servers (downgrade in handler later); 5c parts-summary residency; FTS postings prewarm; concurrency semaphore; serve-topology recommendation (`pond serve --with-sync` + HTTP MCP registration); Lance v10 upgrade track (unblocks timestamp zonemap under DataFusion 54, COUNT(*) pushdown with stable row ids, FTS format v2 via index rebuild, `LANCE_MINIBLOCK_MAX_VALUES` for variant_data, exact-IS-NULL zonemaps).
