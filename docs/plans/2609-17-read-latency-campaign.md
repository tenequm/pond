# Read latency campaign (plan of record, 2026-09-17)

Goal: `pond_search`, `pond_get_session` and `pond_get_message` each answer in
under 10 s against the live S3 store
(`s3+https://nbg1.your-objectstorage.com/pondarium/pond`, Hetzner nbg1).

Baselines as the campaign opens, measured through MCP: cold first query 267 s;
warm `get_session` 30-37 s; warm `get_message` 42 s; warm `search` under 1 s.
So `search` already meets the target when warm - the campaign is about the two
get paths and about the cold start that precedes all three.

Scope is data-access speed only. Correctness items surfaced along the way are
explicitly out of this campaign and stay in their own issues.

## 1. Measurements (all 2026-09-17, live store)

### 1.1 Store shape

| table | data | rows |
|---|---|---|
| total | 42.5 GiB | - |
| `sessions` | 50.0 MiB | 19,799 |
| `messages` | 11.5 GiB | 3,942,635 |
| `parts` | 30.8 GiB | 2,586,113 |

Of the 30.8 GiB in `parts`, **26.3 GiB is pending cleanup**: live parts data is
only ~4.5 GiB, which matches the ~4.6 GB logical `variant_data` the corpus
should hold. The dead bytes are not a one-off backlog - 23 GiB of `parts` data
files were written between Sep 14 and Sep 17 alone, and dead bytes grew ~2.1 GiB
over a few hours during the day. Compaction churn, not corpus growth, owns most
of the store.

`docs/benchmarks/results.md`'s 12.2 GB store figure (2026-08-25) is stale; a
dated note there points here.

### 1.2 `pond_sql` usage audit

3,699 successful calls plus 410 failed ones. Exports and per-shape analysis in
`/home/tenequm/pond-sql-audit/`.

What the calls are for:

- 31.8% exact-string search over `search_text`
- 19.4% tool-body retrieval from `parts`
- 18.3% corpus aggregation
- ~29% of all calls project bodies onto the wire
- hot JSON paths: `$.params.command` x898, `$.result` x709
- 534 calls hand-roll a preview via `substr(json_extract(...))`
- 23.1% of calls duplicate something an existing tool already does

Why the calls fail:

- **timeouts 40.7%**, and they are *not* parts-heavy: `parts` is
  under-represented among timeouts (21% of timeouts vs a 36% base rate).
  120 of 167 timeouts are messages-only shapes - unscoped `GROUP BY`,
  `contains_tokens`, leading-wildcard `LIKE`, and a
  `timestamp + INTERVAL '0 seconds'` shape that defeats zonemap pruning.
- **JSONB mechanics ~24.6%**: CAST guardrail 54, unknown function 47,
  unknown column 46.
- **memory exhaustion 17**, of which 88% touch `parts`.

Body-search shapes specifically: 641 calls, 89% already session-scoped. Only 70
are unscoped, and of those just 16 target `$.result`.

### 1.3 io-trace experiment: where a warm get spends its round trips

25 runs, stock binary vs a narrow-projection binary, against the live store
(run log in the treehouse worktree `pond-38f3f2/1`).

- A warm `get_message` issues **~9,900 GETs**. 9,652 of them (97%) are ~126 B
  reads against `sessions.lance` from `Store::find_session`, taking ONE
  index-matched row. The count is bit-identical across all runs.
- The `parts` btrees **do** engage: a miss probe reads zero data ranges.
- Dropping `variant_data` from the `parts` projection cuts the `parts` leg by
  84-92% of bytes, and wall clock does not move - `parts` is ~0.8% of round
  trips.
- `parts` reads are page-granular and payload-independent: a 1-part message
  read 6.16 MB of `parts` pages, a 13-tool-result message 5.09 MB - a 30-40x
  over-read in the small case.
- `session_scan_rows_resident` falls back to a FULL remote `messages` scan (up
  to 28,000 GETs / 86 MB) whenever the rowmap version mismatches the store,
  i.e. on every get that straddles the 5-minute sync.

Conclusion: blob v2 is a **bandwidth** fix only. `get_message` latency is
`find_session` round trips.

### 1.4 Prior art that constrains the design

- Issue #47 (ngram over a materialized `Utf8` tool-body column). The v8-era
  probe: ngram 480 MiB index, 25 s build, 0.2-0.4 s queries; FM-Index 5.5x
  bigger and 25x slower; FTS is semantically wrong for substring search.
- `contains_tokens` timeouts are cold-FTS load, 47-300 s on the first query per
  process (#165 item 2).
- `lower(project) LIKE` is unindexed by explicit decision.
- Covering indexes are a dead end: no upstream builder writes
  `covering_fields`.

### 1.5 lance v12.0.0 GA (released 2026-09-17)

- Mixed data-file versions (#8581 / #8584): incremental in-place 2.1 -> 2.2
  migration via compaction.
- `WrappingObjectStore::wrap_paginated` becomes mandatory for pond's wrappers.
- The `stable` file format now resolves to 2.2; pond pins `V2_1` explicitly.
- The blob compaction re-pack cost regression persists at v12.

## 2. Decisions

- **Chosen**: approach #2 (local parts-summary map) and approach #3b (blob v2),
  the latter demoted to a gated bandwidth pass.
- **Covering indexes rejected** - unimplemented upstream.
- **Result bodies are deliberately not indexed.** Only 16 of 3,699 calls are
  unscoped `$.result` hunts. The fallback is scoped scans plus absence-honest
  messaging; indexing failure bodies only is a possible later increment.
- **Preview renderer**: known-param-keys with a compact-JSON fallback, with the
  renderer version stamped so previews can be re-derived later.
- **Memory guardrails** (post-#245 posture): every local map is mmap, never
  heap; an ngram index requires a gate mem scenario under a fixed index-cache
  cap before it may be enabled.

## 3. Issue set (filed 2026-09-17)

| id | issue | title | depends on / gate |
|---|---|---|---|
| M0 | #282 | lance 11 -> 12 upgrade | PR #289 MERGED 2026-09-22 (main `9d43f85`); verified on a full S3 copy of the real corpus (read parity vs lance 11 byte-identical; sync/optimize/storage check exit 0); lance-12 bench/mem baselines pending |
| M1 | #283 | MCP via long-lived `pond serve` | PR #292 open, green, conflict-free post-#289; merge decision pending (topology vs deeper-caching discussion, 2026-09-22) |
| M2 | #284 | derived preview / `body_text` columns + local summary map | PR #293 open; polished 2026-09-22 (22 findings applied incl 6 correctness bugs, head `38b8b1e`); bench gate still to run before merge |
| M3a | #285 | warm-get fast path: `find_session` fan-out diagnosis and fix, straddle-fallback delta extension, io-trace instrumentation kept behind a feature flag | PR #298 CLOSED 2026-09-23, branch `perf/285-warm-get-fast-path` parked at `18ab06f`: premise refuted (section 5), threshold-0 fold rejected against spec's `lance-index-maintenance` and N-writer commit scaling; straddle delta parked for cherry-pick when #292 lands; io-trace extracted as PR #303; keymap follow-up DROPPED |
| M3c | #285 | compaction re-encode: stop `TryBinaryCopy` planting tiny pages in `sessions`/`messages`, one-time `pond optimize --reencode` migration | PR #302 open (tip `85a9431`), polished + CI green; e2e-proven on a real-S3 full-corpus copy 2026-09-23: sessions take 11,300 -> 14 GETs, get-session ~31 s -> ~2.6 s, get-message 80-106 s -> 8.4-13 s after both re-encodes, outputs byte-identical; gate row recorded for `1a0fa5a` - write_fold +59% / index_build +6% expected, `write_ms_per_commit` +58% on the append sweep under reconciliation (read deltas from that run discarded: same-bucket e2e overlap); migration measured: sessions 68 s, messages 21m25s, store doubles until version cleanup |
| M3b | #286 | blob v2 bandwidth pass | GATED on a re-measure after M2 + M3a; preconditioned on #288 and on a storage-version guard (`classify_schema` compares names only) |
| M4 | #287 | `pond_sql` just-works + params-only ngram (backlog) | #284 |

Filed alongside, outside the campaign scope but blocking M3b: **#288**
`bug(maintenance): parts compaction rewrite loop - ~80 GiB/day rewritten for a
4.5 GiB table` (root cause and proposed fix in the issue). Hotfixed the same
evening as PR #290 (row-aware veto floor, settled-fragment absorb veto,
cleanup interval 16 -> 8). Polished and MERGED 2026-09-22: rebased onto lance
12 with the row floor aligned to lance 12's writer (floor, not ceil - the
writer spreads rows evenly). A/B-verified on server-side duplicates of the
live store: both old and new veto no-op identically on lance 12 (0 rewrites,
same 31 vetoes, live rows intact), so the fix costs no merges. Copies deleted
after verification. Open observation: the planner re-plans the same vetoed
tasks every run (starvation, harmless); durable cleanup bound deferred -
re-check live "pending cleanup" ~2026-09-25 after Lance's 7-day age threshold
passes before filing a follow-up. A second polish pass
the same day (`e990a46`) flagged a lance-12 divergence: the branch's `div_ceil`
output-count formula matches lance 11, while lance 12 (now on main) uses
`max(1, floor(live_rows/target))`, so post-merge the veto over-predicts by one
off-boundary (errs safe - over-veto, no loop risk). Rebase onto main plus
veto-fixture re-derivation wanted before merge.

Implementation starts with **M1, M2 and M3a only**.

## 4. Open unknowns

Each is owned by one of the issues above. Three were resolved later the same
day (evidence in the linked issues):

- `find_session` fan-out mechanism: RESOLVED 2026-09-17, then CORRECTED
  2026-09-23 - the un-indexed-fragment fan-out below is real but was never the
  dominant cost on a compacted store; the 9/17 "collapsed to 2-3 s" observation
  probed a same-day message in a small fresh fragment. The dominant mechanism is
  the per-page metadata storm in section 5. Original 9/17 finding: one small GET per
  `sessions.lance` fragment not covered by `sessions_id_btree`'s
  fragment_bitmap, paid on every `find_session` (pond never sets
  `fast_search`, so un-indexed fragments are loaded and refined). Linear in
  the un-indexed fragment count; fold thresholds never trip at 1-3 appended
  sessions per 5-min sync, so the tail grows unboundedly (~1,379 fragments at
  storm time; 9,652 GETs = ~1,379 x ~7 requests/fragment). The 62-object
  listing was a post-compaction snapshot. A compaction on 2026-09-17 already
  collapsed warm gets to ~2-3 s. Fix (under #285, gated on #289): unconditional
  sessions index fold per sync + resident sessions keymap as the durable
  follow-up. Evidence in the #285 hand-back; probes preserved on the slot-1
  worktree branch (commit 9d51c70), pushed as `origin/chore/285-io-trace-probes`.
- Compaction churn root cause: RESOLVED - it is the rewrite loop, with cleanup
  lag as a secondary amplifier; root cause and fix proposal in #288.
- ngram size and RSS under v12 on a params-only corpus. OPEN - re-measure under
  #287.
- Body-length distribution percentiles: RESOLVED - params 437 MiB (p99 6.9 KB)
  vs results 3.35 GiB (7.84x); table in the #287 comment.
- Part-group atomicity across appends: RESOLVED - groups DO split (grown-session
  re-sync and intra-commit fragment straddle), so M2's union-across-segments
  design is mandatory; details in the #284 discussion context.

## 5. 2026-09-23 update - full-corpus validation and the real mechanism

PR #298 (M3a) was A/B-validated against a byte-identical 33 GiB copy of the
live store (`pond-pr298-copy`, 20,332 sessions / 4.10M messages), main
`fc798a1` vs PR `026dd6b`, re-checked on the polished tip `18ab06f`.

### What validation showed

- The sessions index fold works but changes nothing measurable: the btree
  already covered 20,326/20,332 rows, and warm `get_message` stays at ~11.5k
  GETs / 20-45 s on BOTH binaries. The #285 acceptance (warm get < 10 s, GETs
  in the low hundreds) is met by neither.
- The straddle delta is real but narrow: mid-run straddle drops 196 GETs /
  9.9 MB (~6 s) to 11 GETs / 17 KB (~1.4 s); a one-shot CLI with a stale
  trailing chain never engages it (chain loads only at exact version match).
- Byte-equivalence held on every probe across 6 binary/state combinations;
  old binary reads the folded store cleanly. #298 is correct - its premise
  was wrong.

### The real mechanism (evidence in the 2026-09-23 #285 comment)

Two facts combine:

1. Pond's compaction uses `CompactionMode::TryBinaryCopy`
   (`substrate.rs` ~3234), which joins fragments WITHOUT re-encoding pages.
   Each 5-min sync appends ~10 sessions = one tiny page; the compacted
   `sessions.lance` fragment holds 20,200 rows in 2,057 pages per column
   (~10 rows/page), 14,401 pages across the 7 columns.
2. lance 12 initializes the metadata of EVERY page of every projected column
   before a take (`StructuralPrimitiveFieldScheduler::initialize`), one tiny
   GET per page (9,699 of the GETs are 2 bytes). 11,277 of the 11,298 traced
   byte ranges match page-metadata buffers exactly. Structural, not a
   coalescing bug; per-column cost is ~2,060 GETs regardless of which column.

Projection narrowing and every scan/config knob measured: no effect. The two
large binary-copied `messages` files have the same disease (476-758
pages/column, est. 5-8k GETs per take).

### The fix (M3c) and its measurements

- Re-encoding compaction on a lab sub-copy: 1 page per column; the same take
  went from 11,300 GETs / ~21 s to 15 GETs / 0.75-1.06 s cold, 0.08 s warm.
- Regrowth guard required: under `TryBinaryCopy` at ~288 syncs/day the storm
  rebuilds to ~1.7k GETs/take within a day. Re-encode costs the ~27% slower
  compaction already noted in the code.
- lance v13 makes take cost independent of page count (measured on the
  untouched bad layout: 14 GETs / 0.76 s with 13.0.0-beta.9) via upstream
  #7465/#9278, but has no final crates.io release yet - upgrade later, layout
  fix now.
- The resident sessions keymap is NOT needed for this problem: after the
  rewrite it would save only ~0.7 s (mostly cold btree load) and would need
  persistence to help one-shot CLI calls. Dropped from the plan.
