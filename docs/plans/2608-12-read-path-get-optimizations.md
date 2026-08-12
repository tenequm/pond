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
