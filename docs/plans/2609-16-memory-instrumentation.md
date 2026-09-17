# Memory instrumentation and fix plan for #245

Plan of record for resolving [#245](https://github.com/tenequm/pond/issues/245)
(unbounded memory growth: 21 OOM kills in 9 days on deployment B, a no-op sync
allocating 3.8 GB in 8 s, per-query monotonic growth in `pond mcp`, and the
kill -> restart -> full re-read amplifier). Two workstreams run in parallel:
instrumentation (track A) and fixes (track B). Instrumentation is not a
prerequisite for writing the fixes - every fix target is already attributed
(#61 massif/vmmap, #229 massif, and the io-buffer evidence below) - it is the
prerequisite for *merging* the two fixes whose value must be proven by
measurement.

## 1. Why this shape

Every pathology in #245 was root-caused once, by hand, with one-off tooling:

- #61: rowmap rebuild materializes ~2.1 GB transient (`Vec<RowMetaEntry>` +
  blob); ~636 MiB of the idle floor is freed-but-retained allocator memory.
- #229: sync flush peaks at `~110 MB + 7 KB x messages + ~10x payload bytes`
  because the flush batch is bounded by session count, not bytes.
- #245: growth follows query load and is never returned until exit; the sync
  cursor is not persisted, so every OOM kill schedules a full re-read.

None of that left repeatable measurement behind. The bench suite samples RSS in
two places (`serve_mem_bench`, `commands_bench`) but nothing covers the sync
path, separates heap from RSS, measures retention or growth slope, or fails a
run on a memory number. That is how the regressions reached 0.17.x releases.

A key attribution lead found while writing this plan: `cap_serve_io_buffer`
(`main.rs`) caps Lance's `LANCE_DEFAULT_IO_BUFFER_SIZE` (default 2 GiB) to
256 MiB **only in serve/mcp**; `sync`/`copy` deliberately keep the default.
That seemed to predict deployment B's 3.8 GB no-op sync against a fast local
Lance dir vs deployment C's 243 MB no-op sync against the same store accessed
remotely. **Phase 0 tested it (results below): not confirmed as the dominant
factor** - the cap trims the warm-path peak ~16% (207 -> 173 MB) and even the
cold full re-read peaks at 738 MB, nowhere near 3.8 GB. The prime suspects
for the spike are now (a) a full rowmap rebuild (`collect_row_metas`
materializing all ~3.84M `RowMetaEntry` in one Vec) fired because the cached
chain was invalid or lock-contended at that moment, and (b) allocator
behavior under the extreme memory pressure the host was already in.

## 2. Phases and tracks

Status 2026-09-16: plan committed to main; B2/B3/B4 launched as parallel
worktree agents; Phase 0 done (below) - B1 demoted to a cheap bound, B5 added
from Phase 0's bonus finding; track A starting.

Status 2026-09-17 (campaign close-out): every PR in the #245 lane is merged -
thirteen in all (#250, #254, #253, #251, #249, #255, #244, #252, #259, #260,
#256, #262, #258), the last wave being #252 (`6c2d09d`, bounded ingest flush
memory), #259 (`3b6c609`, sync cursor oracle preference order pinned), #260
(`03d2a46`, cold rowmap build in a bounded chunk window), #256
(`f253629`, stops the per-message options stamp clones) and #258
(`abc49b9`, bench-gate extensions). The memory
ceiling plus supervised self-restart for `mcp`/`serve` moved to follow-up
issue #266; the allocator-trim half of that old either/or item already
shipped in #249. The closing measurement sweep ran 2026-09-16/17 with #258
in place: the mem gate on both sides, `moon run
repo:bench-gate` on both sides, and read latency on an unoptimized store
ingested with the post-#252 binary - all built on rustc 1.98.1. Its numbers
are in section 6 below. Refs #245.

### Phase 0 - zero-code experiment (DONE 2026-09-16, hypothesis not confirmed)

On deployment B (pond 0.17.3, s3+https Hetzner store, 19,461 sessions /
3,842,164 messages, the host with the 3.8 GB no-op sync on 09-15):

```sh
LANCE_DEFAULT_IO_BUFFER_SIZE=268435456 /usr/bin/time -v pond sync --no-wait
```

| # | Path | IO buffer cap | Wall | Peak RSS | FS inputs |
|---|---|---|---|---|---|
| 1 control, cold | `first sync from this host` full re-read; +0 sessions, +3 msgs | none (Lance default) | 74.4 s | **738 MB** | 115,912 |
| 2 capped, warm | normal incremental; +0/+0 | 256 MiB | 7.2 s | **173 MB** | 136 |
| 3 control, warm | normal incremental; +0/+1 | none (Lance default) | 16.5 s | **207 MB** | 47,896 |

Verdict:

1. The cap is not the fix for the 3.8 GB spike: ~16% (~34 MB) off the matched
   warm pair. Still worth shipping as a bound (B1), but it does not close
   #245's sync item.
2. The 3.8 GB no-op spike does not reproduce on a healthy host - even the
   cold full re-read peaked at 738 MB over 74 s, vs ~3.8 GB anon within 8 s
   of process start on 09-15. Whatever produced it needs a condition absent
   in these runs; prime suspects: (a) full rowmap rebuild
   (`collect_row_metas`, `sessions.rs:2622`), (b) allocator behavior under
   already-extreme memory pressure (swap 0 free for hours).
3. Bonus finding, caught live: run 1 took the `first sync from this host`
   full re-read path on a host that had completed a normal sync 3 minutes
   earlier. Mechanism: `extend_rowmap_coordinated` returns `Ok(None)` on
   build-lock contention (silently), `RowmapOracle::is_empty()` is then
   true, and `main.rs:4833` misreports it as a first sync and re-reads every
   source. This is why the post-OOM restart loop is so expensive - and it
   fires on ordinary lock contention too. Fix is B5.
4. Live ratchet datapoint: `pond serve` on this host grew 439 MB ->
   2.07-2.3 GB RSS in ~100 min of ordinary operation (sync-every-5, no heavy
   queries) - the retained-floor behavior in #245.

Next discriminating experiment (operator, when convenient): heaptrack on the
**cold** path - `heaptrack pond sync --no-wait` after renaming the rowmap
chain out of `~/.cache/pond` - which forces the `collect_row_metas` full
rebuild, i.e. suspect (a). Full run log: deployment B operator notes,
2026-09-16 ~10:30-10:45 UTC.

### Track A - instrumentation core (1-2 days)

Branch `feat/memory-instrumentation`. The minimal slice that lets a fix carry a
before/after number. Detailed spec in section 3.

1. `memprobe` module behind `mem-probe`/`dhat-heap` features
2. `mem_bench` with the 4 scenarios mapping to the active fires
3. `ops/scripts/profile-mem.sh` (heaptrack/dhat) + `[profile.profiling]`
4. JSON per scenario, appended to `docs/benchmarks/mem-gate-baseline.jsonl`

Explicitly NOT in track A: CI wiring, thresholds/tiering, the other 4
scenarios, serve_mem_bench refactor, PeakRecordingPool, telemetry. All deferred
to hardening (section 5) so fixes are not blocked on gate plumbing.

### Track B - fixes (parallel worktrees, one PR each)

| # | Branch | Fix | Verified by | Merge gate |
|---|---|---|---|---|
| B1 | `fix/sync-io-buffer` | extend the io-buffer cap to sync/copy (small change, config-overridable) - a bound, not the spike fix | Phase 0 already measured it: ~16% off warm-path peak (207 -> 173 MB); bounds worst-case scanner buffering on larger stores | merge when green; does NOT close #245's sync item |
| B2 | `fix/sync-cursor-persist` | persist the sync cursor for `serve --with-sync` so a kill does not schedule a full re-read (`syncstate.rs` is the seam) | `first sync from this host` marker gone from the journal after restart | field/journal; merge when green |
| B3 | `fix/sync-flush-byte-budget` | #229: byte budget (~32-64 MB) on the buffered flush batch + chunked encode/append in `messages_batches`; partial flush is already idempotent | `ingest-large-session` + `sync-incremental` before/after rows | **blocked on track A rows** |
| B4 | `fix/linux-alloc-retention` | glibc retention on Linux (gnu builds): `malloc_trim(0)` after sync/rowmap peaks and/or `MALLOC_ARENA_MAX` guidance; `cfg(linux)` only - #61 proved allocator swaps regress macOS | `mcp-query-growth` + `rowmap-build-cold` retained-bytes before/after | **blocked on track A rows** |
| B5 | `fix/rowmap-oracle-fallback` | on rowmap build-lock contention, fall back to the newest stale chain as a trailing oracle instead of an empty one, so `main.rs` stops misreporting a first sync and re-reading every source (Phase 0 verdict 3) | no `first sync from this host` full re-reads on a host with a warm chain (journal over several days, outside genuine first syncs) | field/journal; merge when green - confirmed live, cheapest high-impact change |

B2+B5 kill the full-re-read amplifier (the two independent triggers: lost
cursor and empty-oracle fallback); with B1 as the cheap bound they likely
turn deployment B from "21 OOM kills in 9 days" into "stable". All three
merge on field evidence without waiting for track A.

"Before" rows are never lost by this parallelism: once `mem_bench` lands, the
pre-fix commit is checked out into a pool worktree and the scenario runs there,
producing the before row retroactively.

### Later - structural fixes (measured first, then designed)

Measured 2026-09-16 (large-profile heaptrack/dhat attribution; synthesis and
per-trace reports in the orchestrator archive, gate rows on PR #253): the
rowmap cold build is a 497 MiB transient for a 4.56 MiB mmap product (160 MiB
un-reserved entries spine, 138.6 MiB of 6M per-row Strings, 121.8 MiB in-RAM
serialization blob, 45.8 MiB distinct_sorted scratch); the mcp query loop has
NO leak (live heap flat at ~5 B/iter) - the RSS floor is build residue; ingest
peak is 1358 MiB of which 2.9% is payload (491 MiB per-message options.pond
stamp deep-clones, 155.6 MiB dense all-null embedding buffer). Follow-ups cut
from this: B6 streaming rowmap build (PR #255, tiers 1-2 landed), B7 ingest
stamp sharing (PR #256; its null-embedding omission was reverted - lance
binary-copy compaction corrupts partial fragments). The residual backlog
(B6 tier 3, the 155.6 MiB embedding term via #252 chunk budgeting, B8
part_rows double-hold, B9 search fan-out churn) is consolidated in issue
#257 - no further items tracked here.

Merged 2026-09-16: #250, #254, #253 (this lane), #251, #255. The #251
merge composed its trailing-oracle fallback with #254's persisted cursor
(new Store::sync_oracle_map records the map the planner used; adversarial
review approved). Sharpest residual edge: a foreign-but-prefix-sharing
chain passing Coverage::Trailing validation now feeds a durable cursor -
self-heals on the next messages-changing sync, and usable_sync_cursor
re-validates identity on read. Also merged: #249 (glibc trim; polish
narrowed the cfg gate to target_env = gnu and added an unconditional
post-prewarm trim). Then, after #244 (Rust 1.98, kache k31): #252 (flush
budget + 772 B/row vector-width rider; the mem-gate row understates it ~4x
because mem_bench's ingest_batched skips the byte budget - wiring goes into
#258), #259 (test-only oracle-order pin), #260 (B6 tier 3: streaming build
+ row-id-ordered scan plan with verified-disjoint live ranges and a counted
fallback, rowmap_scan_fallbacks(); sorted fragments alone proven
insufficient; a store compacted while disordered keeps the fallback
forever - tracked in #257). Both then-open riders have since merged: #256
(refresh reconciling the stamp collision with #252) and #258 (bench-lane
scenarios, record-only, plus the ingest_batched budget fix and a
partial-embed rowmap scenario), so final main for the closing sweep is
abc49b9. CI note:
windows-verify died repo-wide on 2026-09-16 ~17:30Z (kache k27 prefix
outgrew the runner disk); #244 resolved it by rotating prefixes to k31 (see
docs/plans/2609-16-ci-nix-toolchain-revamp.md). The #245 closing sweep -
build BOTH sides (44886ce baseline and final main) under 1.98, run both
the mem gate and the read/write `moon run repo:bench-gate` (never run for
these storage-path PRs), check read latency on a store ingested under #252
and not optimized (messages 1->9 fragments at 200k messages), and re-record
mem-gate baseline rows on 1.98 - ran 2026-09-16/17. Results in section 6
below; the re-recorded rows are on PR #270.

- Streaming rowmap build (#61 names the blocker: dict indexes borrow into
  `entries`; own the dict keys up front). Target peak ~400 MB. 1-2 weeks.
  -> superseded by measurement: B6/PR #255 landed tiers 1-2 from main; the
  497->~110-150 MiB path is tier 3 (design in the B6 report).
- Shared daemon + thin stdio shim per session. Architectural; sequenced last -
  B1-B4 shrink what each per-session process costs, which may soften it.
- `oom_score_adj` documentation/drop-in (+200 confirmed correct on two
  deployments) and `MemoryMax=` guidance.

### Later - hardening (after fixes are landing)

The remaining 4 scenarios (`sync-first-full`, `rowmap-delta-compact`,
`sql-heavy`, plus a serve-idle-floor port of serve_mem_bench's check),
thresholds file with two tiers (tier 1: regression ceilings from measured HEAD;
tier 2: targets from #61/#245, flipped to enforced as fixes land), CI job on
`pond-ci` with the 4 cheapest scenarios per PR (warn-only first), sync/serve
telemetry lines (VmHWM/VmRSS/RssAnon at sync end and on the 30 s refresh loop),
`PeakRecordingPool` under `mem-probe` in `pond_sql`, a startup warning when
`LANCE_BYPASS_SPILLING` is set, and optionally a gungraun hard-limit lane.

## 3. Track A spec

### memprobe (`packages/pond/src/memprobe.rs`, feature-gated)

```toml
[features]
mem-probe = []            # counting allocator + RSS probes; benches only
dhat-heap = ["dep:dhat"]  # allocation-site attribution; ad hoc only

[dependencies]
dhat = { version = "0.3.3", optional = true }
```

- Counting allocator: ~25-line wrapper over `System`, `AtomicUsize`
  current/peak/total with `fetch_max` on alloc. Global atomics, accepted
  contention cost in bench builds, because thread-local schemes undercount
  multi-threaded peaks and the true global peak is the metric. Existing crates
  are stale (`cap` 2023, `stats_alloc` 2022) or single-threaded
  (`allocation-counter`). Feature off = the `#[global_allocator]` item does not
  exist = zero impact on shipped binaries.
- RSS probes: Linux writes `5` to `/proc/self/clear_refs` to reset the kernel
  high-water mark, then reads `VmHWM`/`VmRSS`/`RssAnon` from
  `/proc/self/status`. macOS keeps the existing `ru_maxrss`/`phys_footprint`
  helpers (moved here from serve_mem_bench in the hardening phase, not now).
- Sampler: 200 ms thread sampling `VmRSS`; emits max/final/series so a
  scenario distinguishes one spike from monotonic growth.
- Output per scenario: `{scenario, peak_heap_bytes, end_heap_bytes,
  total_alloc_bytes, peak_rss_kb, end_rss_kb, rss_anon_end_kb,
  growth_slope_bytes_per_iter?, wall_ms}`.
- dhat mode: same binary, `--features dhat-heap`, writes `dhat-heap.json` for
  the online DHAT viewer (per-site attribution, sorted by at-t-gmax or
  at-t-end). Diagnosis lane only - dhat is slow and cannot reset mid-run.

### mem_bench (`benches/mem_bench.rs`, harness = false)

One scenario per process invocation (`mem_bench --scenario <name>`): peak RSS
is a process-lifetime high-water mark and dhat cannot reset, so the runner
invokes the binary once per scenario.

| Scenario | Exercises | Catches |
|---|---|---|
| `sync-noop-local` | full sync pipeline, zero new data, local store | the 3.8 GB no-op spike; store-size-proportional sync memory |
| `sync-incremental` | +1 session / +N messages then sync | peak scaling with delta vs store |
| `rowmap-build-cold` | `ensure_rowmap` from nothing | the #61 ~2 GB transient |
| `mcp-query-growth` | N iterations of search + get + sql | the per-query ratchet; retained-bytes slope per iteration after warmup |
| `ingest-large-session` | one session, parameterized steps (#229 shape) | flush-batch byte scaling (B3 needs it, so it lands with or before B3's merge) |

Corpus: generated through the existing `session_events`/`ingest_batched` path,
`--profile ci` (~100k messages) and `--profile large` (1M+), content-addressed
by (generator version, profile) and cached locally.

### Runner and baseline

`ops/scripts/mem-gate.sh` v1: build once, invoke per scenario, append one
combined row to `docs/benchmarks/mem-gate-baseline.jsonl`, print delta vs the
previous row. No thresholds yet - the human reads the delta. Thresholds and CI
arrive in hardening.

### profile-mem.sh

`ops/scripts/profile-mem.sh <scenario> [heaptrack|dhat]` against the same
corpus; heaptrack needs `[profile.profiling] inherits = "release", debug = 1`
(workspace root; dist builds keep the normal release profile). Never wraps
`cargo run` (that profiles cargo). Replaces the ad-hoc recipe in the #245
comments.

## 4. Design principles (bind both tracks)

1. Zero impact on shipped binaries: all allocator instrumentation is
   feature-gated off; production changes are limited to a few `/proc` reads
   and tracing lines (hardening phase).
2. One scenario, one process.
3. Heap and RSS always reported together - the gap is the allocator-retention
   signal.
4. A failing number must point at the cause: every scenario is re-runnable
   under dhat/heaptrack with one command.
5. Fixes merge on evidence: field evidence for B1/B2, before/after scenario
   rows for B3/B4 and everything after.

## 5. Acceptance

- Track A: `mem-gate.sh` produces a full baseline row on the `ci` corpus; each
  #245 pathology has a scenario that visibly exhibits it on pre-fix HEAD
  (rowmap transient, sync spike shape, mcp slope > 0).
- B1: already field-measured in Phase 0 (~16% warm-path); acceptance is the
  bound being in place for sync/copy with the escape hatch documented.
- B2: a killed/restarted `serve --with-sync` resumes from the cursor (no
  `first sync from this host` full re-read).
- B3/B4: before/after rows show the intended reduction with no equivalence or
  throughput regression (`write_bench` guards the write path).
- B5: several days of journal show no spurious `first sync from this host`
  re-reads outside genuine first syncs.
- Deployment B's OOM cadence is the ultimate metric: target zero pond OOM
  kills over a 7-day window after B1+B2+B5 deploy.

## 6. Closing numbers (2026-09-17)

PRE is `44886ce`, the pre-campaign baseline commit (the tip of the #253
harness branch, squash-merged to main as `41b3663`, so it is not itself a
main commit); POST is `abc49b9`, final main. Both sides were built and
measured under the same toolchain
(`rustc 1.98.1 (48a229cea 2026-09-01)`, `cargo 1.98.1 (797e8a9bc
2026-08-05)`), so these deltas are code, not compiler. The re-recorded rows
are on PR [#270](https://github.com/tenequm/pond/pull/270).

### mem-gate, `large` profile (20,000 sessions x 50 messages)

| scenario | wall_ms | peak_rss_kb | peak_heap_bytes | note |
|---|---|---|---|---|
| sync-noop-local | 1434 -> 665 (-53.6%) | 681152 -> 179196 (-73.7%) | 519885409 -> 81196613 (-84.4%) | |
| sync-incremental | 1601 -> 948 (-40.8%) | 686600 -> 181272 (-73.6%) | 519884992 -> 78633217 (-84.9%) | |
| rowmap-build-cold | 1393 -> 650 (-53.3%) | 691512 -> 181204 (-73.8%) | 519885413 -> 79967868 (-84.6%) | |
| mcp-query-growth | 3973 -> 4062 (+2.2%) | 589164 -> 346608 (-41.2%) | 60752939 -> 60162182 (-1.0%) | |
| ingest-large-session | 3378 -> 3094 (-8.4%) | 1529564 -> 535984 (-65.0%) | 1435934878 -> 296530349 (-79.3%) | harness changed - not comparable |

`ci` profile, same direction and smaller magnitude: on sync-noop-local,
sync-incremental and rowmap-build-cold, peak RSS moved -24.6% / -19.2% /
-24.9% and peak heap -53.0% / -54.4% / -49.6%; mcp-query-growth was peak RSS
-9.7% with peak heap +1.6%; ingest-large-session again not comparable.

`ingest-large-session` is labelled not comparable because #258 wired the
byte-budget flush into `mem_bench`'s `ingest_batched` (the gap called out
above under #252): the bench had been flushing only at session boundaries,
so a one-session scenario never fired the budget. The POST row carries
fragment accounting the PRE row has no field for, and on `large` the budget
now cuts 9 fragments per table where the ci corpus stays under it at 1. The
two rows measure different work, not a memory win of that size.

The five scenarios #258 added (`search-query-latency`, `ingest-throughput`,
`serve-sync-retention`, `sync-under-contention`,
`rowmap-build-cold-partial-embed`) are record-only: they have no PRE
counterpart and are baselines for the next campaign, not deltas.

### bench-gate - only four fields are readable

Read this table with the caveat below it, not on its own.

| field | PRE -> POST | reading |
|---|---|---|
| `vector_iops` | 91 -> 91 (+0.0%) | unchanged |
| `search_iops` | 274 -> 246 (-10.2%) | the one credible read regression |
| `open_store_ms` | 1166 -> 548 (-53.0%) | real improvement |
| `write_copy_delta_ms` | 649 -> 974 (+50.1%) | credible regression |

Caveat. Both bench-gate rows were measured with the operator's live
`pond-sync.timer` firing every 5 minutes against the measured store -
like-for-like on both sides, matching how the committed baseline row was
made. CLI probes are best-of-2 and noisy: inside the PRE run alone the two
`search` attempts differed 2.6x (14.3 -> 5.5 s). Store content also drifts
between the two sides (19,651 -> 19,676 sessions, 3,904,429 -> 3,908,280
messages). Two POST attempts of the *same* binary at the *same* commit two
hours apart disagree on most fields by more than PRE -> POST does, up to
24.2x on `search_s`; only the four fields above held a run-to-run spread of
1.1x or less, so only large consistent deltas on those four are signal.
Everything else in the row is dominated by S3 and by concurrent sync
traffic. For absolute scale on the noisy remainder: `rowmap_cold_ms` was
277,097 ms PRE on the live S3 store - one sample of a minutes-long
operation, not a gating number. Separately, the bench-gate script prints a
delta against the committed 2026-09-10 row; that comparison spans a
different binary, toolchain and store size, is not a code delta, and must
not be quoted. Precise numbers for this campaign come from the mem gate and
the read-latency ladder.

### Read-latency ladder (M0-M3)

Measured on a 200k-message local store ingested with the post-#252 binary and
left unoptimized. The quotable pair is M2 (7 messages fragments, indexes
present) versus M3 (the same store after `pond optimize`, 2 fragments). The
#252 chunked-flush path itself was not exercised: the M2 tail came from six
small fixture ingests (~10 MB in total), so the 32 MiB flush budget never
bound and those 7 fragments are per-commit fragments, not byte-budget cuts.

| metric | M2 | M3 | M2 worse by |
|---|---|---|---|
| `fts_search` p50 | 9 ms | 1 ms | +800% |
| `get_steady` p50 | 33 ms | 7 ms | +371% |
| `fts_steady` p50 | 19 ms | 6 ms | +217% |
| `sql_steady` p50 | 9 ms | 9 ms | 0% |

Search and hydration must consult every fragment's index segment, so they
track fragment count directly; the analytics shapes read manifest metadata
and column statistics, which compaction does not change. The remedy is
cheap: `pond optimize` ran in **1.00 s** (0.40 s at M3) and restored the
folded state.

One correction to how that remedy reads: `pond sync` already runs the same
optimize pass - compaction, index fold and version cleanup - after every
sync that brought in new rows, and has done since `2680cea` (2026-05-16).
Sync and `pond optimize` build that policy through the same function
(`configured_maintenance_policy`, `main.rs:1540` and `main.rs:4609`) and both
inherit `DEFAULT_COMPACTION_FRAGMENT_CAP = 64` (`substrate.rs:901`); sync
differs only in its cleanup interval and index-fold thresholds
(`main.rs:4610-4612`). The cap is a per-task bypass, not a floor: in
`task_veto_reason` (`substrate.rs:1096`) a task of 64 fragments or more skips
the width and amplification checks outright, while a narrower task is still
allowed through unless it trips `cannot_shrink`, `row_target_unattainable` or
`absorb_veto`. So the cap does not by itself explain a small fragmented tail.
The ladder's M2 store was built by bench ingest, which never calls the
optimize stage at all, so its 7 fragments are the unmanaged worst case rather
than what a synced store would carry - and this run therefore says nothing
about what sync leaves behind. Measuring a synced store directly is the
follow-up; auto-compaction already exists and does not need adding.

M0 reproduced #252's shape closely - 8 messages fragments and 9 versions at
200k messages, against the 9 / 11 that report recorded.

Two bounds on those percentages. The bigger M0 -> M1 figure (`fts_search`
p50 88 ms -> under 1 ms) measures *index presence*, not fragmentation - M0
had no usable FTS index - which is why the M2/M3 pair is the one to quote.
And the ladder ran on a local filesystem, where an extra fragment costs one
more open file; S3 charges a round-trip per extra object, so these are a
lower bound on production cost.

### Carried out of this plan

- Memory ceiling + supervised self-restart for `mcp` and `serve`, and the
  `oom_score_adj` guidance folded into it: issue
  [#266](https://github.com/tenequm/pond/issues/266).
- Bench-harness follow-ups found while closing the sweep - corpus generator
  versioning, `write_bench` `ingest_batched` parity, record-only scenario
  policy, a toolchain field on mem-gate rows: issue
  [#269](https://github.com/tenequm/pond/issues/269).

Refs #266 #269 #270.
