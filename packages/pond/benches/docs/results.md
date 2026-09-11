# Benchmark results log

An append-only lab notebook, not a regression gate. Every number here is host-, disk-, OS-, and corpus-shape-dependent, so a figure means nothing without the environment it was taken in: compare within one environment, or compare ratios across them. CI never runs these benches - every entry was produced by hand on the machine it names.

Entries are newest first within each bench. Output is pasted verbatim: the bench's printed table is the schema, and retyping it into a normalized one is where transcription drift enters.

Numbers that have hardened into rules live in AGENTS.md as prose (the S3 commit-latency law, the sync change-detection oracle, the `count_rows` inequality trap). This file is the underlying measurements, including runs whose only conclusion was "no change".

`write_bench` was named `copy_bench` until v0.11.1 (`ad9134f`, 2026-07-01); recovered entries predating the rename say so. Older runs not yet transcribed here can be recovered from pond itself - the bench output is a tool body, so it is reachable through `pond_sql` on `parts.variant_data`, not through `pond_search`.

The bench gate is now the moon task `moon run pond:bench-gate`. Entries below that predate this branch name the invocation of their day instead (`bash ops/scripts/bench-gate.sh`, a script since deleted, or `moon run repo:bench-gate`, the task id before it moved into the crate), so read those lines as a record of what was run rather than as a command to run now.

## Environments

- **mac-m1max** - Apple M1 Max, macOS 26.6.2, APFS on internal SSD.
- **win-5700x3d** - AMD Ryzen 7 5700X3D (8C/16T), 16 GB, Windows 11 Pro 10.0.26200, NTFS on NVMe SSD.
- **s3-nbg1** - Hetzner object storage at `nbg1.your-objectstorage.com`, driven from mac-m1max.

- **ws-pond-01** - Devboxes sandbox VM, NixOS (Linux 6.18.50), AMD EPYC 16 vCPU, 62 GB, ext4 on virtio SSD. Drives s3-nbg1 from Nuremberg-adjacent network rather than over a home link, which is why its S3 numbers are not comparable to mac-m1max's.

The 2026-09-10 bench-gate row is the first Linux entry in this file. Before it, on 2026-08-17, every recorded session mentioning a benchmark was searched for output carrying a Linux path or target triple and none existed. CI still does not run benches.

## write_bench --append-sweep

Per-commit write cost against commit size - the per-commit latency floor at small batches versus the bandwidth ceiling at large ones.

### 2026-08-17 - Windows durability wrapper A/B (PR #162)

Does extending the fsync durability wrapper to Windows cost anything? No: under 1 ms per commit at every batch size, and 0.8 ms (3.4%) at the largest.

`cargo bench --bench write_bench -- --sessions 2000 --messages 8 --append-sweep 1,8,64,512 --sweep-commits-cap 100`

win-5700x3d, `229e73b` (wrapper attached):

```
 batch      rows    commits    wall_ms    ms/commit
     1       100        100       1253       12.5    (80 rows/s)
     8       800        100       1378       13.8    (581 rows/s)
    64      6400        100       1726       17.3    (3708 rows/s)
   512     16000         32        775       24.2    (20645 rows/s)
```

win-5700x3d, `55043fe` (origin/main, no wrapper):

```
 batch      rows    commits    wall_ms    ms/commit
     1       100        100       1236       12.4    (81 rows/s)
     8       800        100       1377       13.8    (581 rows/s)
    64      6400        100       1710       17.1    (3743 rows/s)
   512     16000         32        750       23.4    (21333 rows/s)
```

mac-m1max, `5221146`, same command as a cross-platform control:

```
 batch      rows    commits    wall_ms    ms/commit
     1       100        100       1030       10.3    (97 rows/s)
     8       800        100       1046       10.5    (765 rows/s)
    64      6400        100       1260       12.6    (5079 rows/s)
   512     16000         32        498       15.6    (32129 rows/s)
```

Windows trails macOS by 21% at batch 1 and 55% at batch 512, but the wrapper is not the cause - the no-wrapper arm trails by the same margin. The two hosts are different machines (M1 Max versus Ryzen 5700X3D), so this gap is not attributable to the OS alone. Note the platforms do unequal work per published object: macOS pays `F_FULLFSYNC` plus a parent-directory fsync to make the name durable, Windows pays one `FlushFileBuffers` and lets NTFS journal the name (`substrate.rs`, `sync_file_and_parent`).

### 2026-08-17 - win-5700x3d, real corpus as source (scan-bound, not a write measurement)

`cargo bench --bench write_bench -- --source-url D:\pond-corpus\pond --append-sweep 1,8,64,512` against an 11.0 GB copy of the real store, `229e73b`:

```
 batch      rows    commits    wall_ms    ms/commit
     1        30         30     182376     6079.2    (0 rows/s)
     8       240         30     388033    12934.4    (1 rows/s)
    64      1920         30     508482    16949.4    (4 rows/s)
   512     15360         30     640755    21358.5    (24 rows/s)
```

Recorded as a trap, not as a write-path result. `append_absent_rows` issues a fresh `source_scan` with an `IN` predicate per call (`sessions.rs`), so against a large source every commit pays a full-column scan: the 6 s floor at batch 1 is the read. Any write-path cost is invisible under it, so do not source an append sweep from a large store when the write side is what is being measured.

### 2026-06-30 - s3-nbg1, the commit-latency law (recovered from session history; `copy_bench`)

Roughly 1 s per commit, flat from 1 to 512 rows - the measurement behind the AGENTS.md rule that S3 write cost is commit-count-bound, not bandwidth-bound. Source was a local 1000 x 30 corpus, cap 30 commits.

```
 batch      rows    commits    wall_ms    ms/commit
     1        30         30      30788     1026.3    (1 rows/s)
     8       240         30      26839      894.6    (9 rows/s)
    50      1500         30      29360      978.7    (51 rows/s)
   512     15360         30      29145      971.5    (527 rows/s)
  bulk     30000          1       4061     4061.0    (7387 rows/s)
```

### 2026-06-30 - mac-m1max, local (recovered from session history; `copy_bench`)

Source 200 x 30, cap 20 commits. A different corpus shape and cap from the 2026-08-17 runs, so it is a same-order sanity check, not a directly comparable arm.

```
 batch      rows    commits    wall_ms    ms/commit
     1        20         20        331       16.6    (60 rows/s)
     8       160         20        282       14.1    (567 rows/s)
    50      1000         20        389       19.4    (2571 rows/s)
   512      6000         12        614       51.2    (9772 rows/s)
  bulk      6000          1         98       98.0    (61224 rows/s)
```

## write_bench --only append|merge

Full real corpus, local source to a clean S3 destination - the measurement behind the rule that absent rows must append and must never be routed through merge-insert on a remote store.

### 2026-06-17 - mac-m1max to s3-nbg1 (recovered from session history; `copy_bench`)

11,185 sessions, clean cold run each, `--source-url ~/.local/share/pond`:

```
[append-only] full copy streaming  :  829982 ms  (delta sessions=11185, messages commits=1)
[merge-only]  full copy merge-insert: 4541496 ms  (delta sessions=11185, messages commits=354)
```

13.8 min against 75.7 min, 5.47x slower, and merge left 2,685 objects against 62. Append is bandwidth-bound under one commit per table; merge is commit-latency-bound at one commit per chunk.

## release A/B: CLI reads and full first import (embeddings opt-in)

One-off release verification for PR #168 (`feat/164-embeddings-opt-in`): old = v0.14.11 release binary built from main, new = branch `ba3e352`. Real full corpus against the production store; not a cargo bench - hyperfine over the CLI for reads, `time pond sync` into fresh scratch prefixes for the import. The absolute ~2 min search figures are the pre-existing one-shot CLI cold start (index open + first-query warmup over S3), not a branch effect; long-lived `mcp`/`serve` does not pay it per query.

### 2026-08-21 - s3-nbg1, old 0.14.11 vs new ba3e352, read matrix

Store `s3+https://nbg1.your-objectstorage.com/pondarium/pond` (~14.3k sessions / 2.87M messages at run time). `hyperfine -w 1 -r 3` per cell. `new-disabled` = default config, `new-enabled` = `POND_EMBEDDINGS_ENABLED=1`.

```
== status (default verbosity) ==
Benchmark 1: old
  Time (mean +/- sigma):      6.330 s +/-  0.984 s    [User: 1.474 s, System: 1.620 s]
  Range (min ... max):    5.317 s ...  7.282 s    3 runs

Benchmark 2: new-disabled
  Time (mean +/- sigma):      5.373 s +/-  0.294 s    [User: 1.462 s, System: 1.671 s]
  Range (min ... max):    5.034 s ...  5.552 s    3 runs

Summary
  new-disabled ran
    1.18 +/- 0.19 times faster than old
== status -v ==
Benchmark 1: old
  Time (mean +/- sigma):     79.115 s +/- 21.896 s    [User: 7.398 s, System: 5.012 s]
  Range (min ... max):   63.175 s ... 104.081 s    3 runs

Benchmark 2: new-disabled
  Time (mean +/- sigma):     21.563 s +/-  5.805 s    [User: 1.708 s, System: 1.870 s]
  Range (min ... max):   15.901 s ... 27.502 s    3 runs

Benchmark 3: new-enabled
  Time (mean +/- sigma):     72.139 s +/- 30.303 s    [User: 6.099 s, System: 4.233 s]
  Range (min ... max):   51.654 s ... 106.949 s    3 runs

Summary
  new-disabled ran
    3.35 +/- 1.67 times faster than new-enabled
    3.67 +/- 1.42 times faster than old
== search fts ==
Benchmark 1: old
  Time (mean +/- sigma):     112.384 s +/-  3.177 s    [User: 8.323 s, System: 4.460 s]
  Range (min ... max):   109.358 s ... 115.692 s    3 runs

Benchmark 2: new-disabled
  Time (mean +/- sigma):     117.016 s +/- 18.010 s    [User: 8.064 s, System: 4.409 s]
  Range (min ... max):   100.323 s ... 136.103 s    3 runs

Summary
  old ran
    1.04 +/- 0.16 times faster than new-disabled
== search default mode (upgrade experience: omitted --mode) ==
Benchmark 1: old-default
  Time (mean +/- sigma):     122.856 s +/- 14.638 s    [User: 8.396 s, System: 4.615 s]
  Range (min ... max):   113.779 s ... 139.743 s    3 runs

Benchmark 2: new-default
  Time (mean +/- sigma):     111.895 s +/-  9.605 s    [User: 7.773 s, System: 4.345 s]
  Range (min ... max):   105.965 s ... 122.977 s    3 runs

Summary
  new-default ran
    1.10 +/- 0.16 times faster than old-default
```

`status` 1.18x, `status -v` 3.67x with embeddings disabled (it no longer pays the embedding-coverage scan) and parity when enabled, `search --mode fts` parity (1.04 +/- 0.16), omitted-mode search 1.10x from the default flip vector -> fts. No regression in any cell.

### 2026-08-21 - s3-nbg1, old 0.14.11 vs new ba3e352, full first import to fresh scratch prefix

Same local adapter corpus as source, each leg into its own clean prefix (`pond-perf-new` / `pond-perf-old`, deleted after the run). Row counts differ slightly between legs because the source corpus is live and grew ~100 messages between the 18:08Z and 18:48Z starts.

```
== full first import: local adapter corpus -> fresh S3 scratch prefix ==
-- NEW branch, default disabled (18:08:58Z) --
stored    12,054 sessions, 2,328,304 messages
done - sync complete in 00:39:22

real	39m22.957s
user	5m36.334s
sys	1m4.735s
-- OLD 0.14.11, always embeds (18:48:21Z) --
stored    12,055 sessions, 2,328,401 messages
done - sync complete in 01:53:55

real	113m56.185s
user	9m21.515s
sys	2m42.220s
```

39m22s against 113m56s: first import is 2.89x faster with embeddings off (the new default). The gap is the inline embedding cost the old binary always paid; a new-enabled leg would land near the old number.

## bench-gate: Lance 8 -> 10 upgrade (#145)

Three-point bracket for the lance 8.0.0 -> 10.0.0 upgrade, all on s3-nbg1 (store digest `5fcd5e32b8dd` in `bench-gate-baseline.jsonl`) driven from mac-m1max, same day, so only the code under test differs. The 2026-08-12 jsonl row is NOT comparable to these rows: it predates the #168 search/bench rework, and until 2026-08-25 the gate ran ops_bench without `--url`, so that row's ops fields measured whatever store the operator config named at the time. Cross-row ops comparisons are valid from 2026-08-25 onward only.

### 2026-08-25 - s3-nbg1, lance 8 (dd2562a) vs lance 10 + zonemap intent (33bf790)

Gate delta, verbatim (`bash ops/scripts/bench-gate.sh`, read-only, so the store has no timestamp zonemap yet in either row):

```
metric                      prev         now     delta   (2026-08-25T13:30:59Z dd2562a -> 2026-08-25T14:42:16Z 33bf790)
get_session_sid_s           23.8        52.8     +122%
get_session_mid_s           51.8        50.7       -2%
get_message_s               51.2        47.4       -7%
search_s                   145.3       126.0      -13%
search_dated_s             135.8       150.6      +11%
sql_count_s                  2.1         1.3      -38%
fts_iops                      17          16       -6%
vector_iops                   92          93       +1%
get_message_iops              18          29      +61%
search_iops                  159         218      +37%
open_store_ms               1330        5342     +302%
row_counts_ms               1132         802      -29%
oracle_warm_ms             50106       24640      -51%
```

Reconciliations (a second lance-10 `ops_bench --url` run, same store, minutes later):

- `oracle_warm_ms` -51% is real and reproduces: rerun 23,724 ms vs lance 8's 50,106 ms.
- `open_store_ms` +302% is transient S3 weather, not a regression: rerun 1,165 ms vs lance 8's 1,330 ms - parity.
- `row_counts_ms` improvement reproduces: rerun 681 ms.
- `sql_count_s` 2.1 -> 1.3 is the COUNT(*) pushdown on stable-row-id datasets landing.
- The CLI search/get probes are one-shot cold-start dominated (~50 s gets, ~2 min search; see the #168 release A/B above) - parity within their noise. Lance 8's `get_session_sid_s` 23.8 was the outlier against its sibling probes (~51 s); lance 10's 52.8 is in line with them.

### 2026-08-25 - s3-nbg1, timestamp zonemap materialized (lance 10)

`pond optimize --only index` built `messages_timestamp_zonemap` on the store in 49 s. Date-filtered vector search (`--from-date` 7 days back), after vs before:

- One-shot CLI: the date-filter penalty over unfiltered search is gone. Pre-zonemap dated ran +24.6 s over unfiltered (150.6 vs 126.0); post-zonemap dated ran 113.1/125.0 s - at or below unfiltered, i.e. parity within noise. The remaining ~2 min is the arm-independent one-shot cold start (#168 measured fts at ~112-117 s too), not the filter.
- Served path (warm `pond serve`, `POST /v1/search`): unfiltered 0.16-0.18 s; dated 5.3/6.2 s with correct results (10 sessions, 11,643 messages in the 7-day scope). Issue #145's documented served dated figure on lance 8 was ~2 minutes: ~20x.

### 2026-08-25 - s3-nbg1, write A/B: lance 8 vs lance 10 sync replay (matched scratch copies)

Method: the production store was s5cmd-copied to identical scratch prefixes (parity-verified: 417 objects / 12.2 GB / 2,931,634 message rows each), and the same frozen 4-session / +741-row / +46-searchable delta (a snapshot of the local source dir) was synced into each copy once per binary. `warm` = local index/rowmap cache primed by an immediately preceding `--dry-run` against the same copy, which is the recurring-cron shape (cache persists across runs on one store path); `cold` = first-ever run against that store path. Embeddings disabled, matching the read-gate rows. Every run converged to the identical end state (2,932,375 rows).

| state | lance 8 (pond 0.15.1) | lance 10 (feat/lance-10-upgrade) | verdict |
|---|---|---|---|
| cold sync | 133.3 s | 134.3 s | parity |
| warm sync | 12.2 s | 10.4 s | parity (~15% ahead, single-run noise) |

Writes neither regressed nor meaningfully improved 8 -> 10: sync cost is pond-logic and S3-round-trip bound, and cold runs are dominated by the same arm-independent cold start the read probes documented. Caveats: n=1 per cell; the fold in this delta hit the deferred-small-tail path (0-2 s), so a large-tail fold was not A/B'd (the one-sided lance-10 datapoint for that is the 48 s incremental fold in the production sync above).

Side findings from the setup, both pre-existing and lance-independent:

- Hetzner list-after-write lag: a store opened seconds after being s5cmd-copied can list as absent, which routes pond into the store-create path. A visibility poll + settle delay before first open avoids it.
- The store-create path itself, when it fired spuriously, attempted a PUT with the bucket segment missing from the URL (`NoSuchBucket`). Store creation on a genuinely fresh prefix works (verified by write_bench scratch stores), so the malformed URL appears specific to the create-after-failed-detection flow; worth its own issue.

### 2026-08-25 - s3-nbg1, full-gate rows (read + write): lance 8 baseline (dd2562a-dirty) and lance 10 (be14674)

Two complete jsonl rows produced with the current gate (CLI probes + ops_bench + write_bench), same store digest `5fcd5e32b8dd`, same machine, same day.

Rig for the v8 row: a worktree at dd2562a with only the current `ops/scripts/bench-gate.sh` overlaid plus a 1-line `write_bench` merge-loop fix (bench harness, not product code), so the measured binary is pure lance 8; the row is stamped `dd2562a-dirty` to make the overlay visible.

**`search_dated_s 2.7` in the v8 row is not a performance number - it is the old-reader hazard measured.** Lance 8 consumes the store's `messages_timestamp_zonemap` in the wrong id domain (see the 0.16.0 changelog), so the dated probe returned zero results and did almost no work: 2.7 s against the same row's unfiltered `search_s 178.1`. On top of that, the store's zonemap was corrupt at probe time (see below). Treat the field as invalid for any comparison; the lance-10 row's dated figure is the real one.

Incident, discovered when the first lance-10 gate run failed at the dated probe: the machine's scheduled pond 0.15.1 sync had kept compacting the store for ~3.5 h after the zonemap was built, orphaning the index's address-domain fragment references - lance 10 then hard-errors date-filtered search in both modes (`fragment 5225 referenced by an address-domain index result was not found`). Contained by `pond schedule stop` + `pond optimize --rebuild` (10m13s, store healed, dated search correct again); the compaction-staleness self-heal ships in 0.16.1. The write path is identical in lance 8 and 10 - any pre-0.16.1 writer's compaction of covered fragments does this - so the gate row here was produced with no concurrent sync.

Gate delta, verbatim (v8 full row -> lance 10 rerun after the store rebuild; `be14674-dirty` = this worktree carried the uncommitted jsonl/results.md docs edits, product code is be14674 exactly):

```
metric                      prev         now     delta   (2026-08-25T18:42:30Z dd2562a-dirty [pond 0.15.1 (aarch64-macos)] -> 2026-08-25T19:52:42Z be14674-dirty [pond 0.15.1 (aarch64-macos)])
get_session_sid_s           23.2        21.7       -6%
get_session_mid_s           53.5        53.6       +0%
get_message_s               47.4        52.1      +10%
search_s                   178.1       105.9      -41%
search_dated_s               2.7       102.0    +3678%
sql_count_s                  1.4         1.3       -7%
fts_iops                      16          12      -25%
vector_iops                   91           0     -100%
get_message_iops              18          18       +0%
search_iops                  159          40      -75%
open_store_ms               1053        1821      +73%
row_counts_ms               1066        1438      +35%
oracle_warm_ms             28592       28099       -2%
write_copy_ms               4034        3671       -9%
write_copy_merge_ms          607         447      -26%
write_copy_noop_ms           401         515      +28%
write_copy_delta_ms          547         658      +20%
write_ms_per_commit        446.2       359.6      -19%
write_rows_per_s            1121        1390      +24%
write_index_build_ms       14747       16291      +10%
write_fold_ms               2195        1528      -30%
```

Reconciliations:

- `search_dated_s` +3678% is the v8 field being invalid (above), not a regression: 102.0 vs unfiltered 105.9 is the zonemap's date-filter parity, matching the earlier 113.1/125.0 post-zonemap observations.
- The warm-query S3-IO drops (`search_iops` 159 -> 40, `vector_iops` 91 -> 0, `fts_iops` 16 -> 12; iops = S3 requests per warm query, lower is better) are real but not a lance-10 code effect: the `optimize --rebuild` an hour earlier consolidated every index into a single fresh segment, so warm queries page far less. Prior rows measured organically-grown multi-segment indexes; treat cross-row iops comparisons against this row accordingly.
- Write-side fields are parity within single-run noise (copy 4034 -> 3671, per-commit 446 -> 360, fold 2195 -> 1528, index build 14747 -> 16291), consistent with the matched-copy sync A/B above: lance 8 -> 10 does not move writes.

## bench-gate: Lance 10 -> 11 upgrade (feat/upgrade-to-lance-v11)

Two-point same-day A/B for the lance 10.0.0 -> 11.0.0 upgrade on s3-nbg1 (store digest `5fcd5e32b8dd`), driven from mac-m1max, so only the code under test differs; the 08-25 lance-10 row is kept as the week-over-week reference. The upgrade is a dependency pin plus a `recursion_limit` attribute - no pond code path changed - so the interesting content here is the two things lance 11 did change underneath: the stemmer (behavioral, documented below) and, possibly, the zonemap-filtered search path (one unreconciled field).

### 2026-09-01 - s3-nbg1, full-gate rows: lance 11 (61c1498-dirty, 14:25Z) and same-day lance-10 control (61c1498-dirty, 15:33Z)

Two complete jsonl rows on store digest `5fcd5e32b8dd`, same machine (mac-m1max), same day, back to back, with the machine's two launchd pond jobs (`io.kolesnik.pond-mirror` hourly S3 mirror, `sh.pond.sync` 5-min local sync) paused for each window. Other hosts still push to the store hourly, so it is not fully quiet. **Both rows carry the stamp `61c1498-dirty`; tell them apart by date**: `2026-09-01T14:25:59Z` is the lance-11 binary (the 6376bad tree measured before it was committed), `2026-09-01T15:33:32Z` is the lance-10 control (a detached worktree at 61c1498 with only the fixed `bench-gate.sh` overlaid - the same rig shape as the v8 row on 08-25). A second v11 row on the committed tree was planned and skipped: the control already gives a same-day A/B.

Store state at gate time: `messages.lance` version 7464, 3,122,185 message rows / 15,728 sessions (2,931,634 rows on 08-25), all indexes `ready` including `messages_timestamp_zonemap`, with a week of organically appended index segments on top of the 08-25 `optimize --rebuild`. That is why the warm-query iops are back at the pre-rebuild levels of the `dd2562a` / `33bf790` rows (search_iops 155-156, vector_iops 91) rather than the `be14674` row's single-segment lows (40 / 0) - a store-state effect, identical in both of today's rows.

Gate-script fix carried by this branch (`ops/scripts/bench-gate.sh`): the CLI probes (`get-session`, `get-message`, `search`, `sql`, the equivalence gets) now pass `--storage-path "$STORE_URL"`. They previously read the operator config, so with the config pointing at the local store (the case since the mirror setup) a `STORE_URL` run measured local CLI probes next to S3 bench fields - the first lance-11 attempt returned 0.1-0.8 s "S3" gets and was discarded. Both rows here were produced by the fixed script.

Same-day A/B, verbatim (lance-10 control -> lance-11; both `pond 0.16.3 (aarch64-macos)`):

```
metric                      prev         now     delta   (2026-09-01T15:33:32Z 61c1498-dirty [lance 10] -> 2026-09-01T14:25:59Z 61c1498-dirty [lance 11])
get_session_sid_s           23.9        21.6      -10%
get_session_mid_s           53.9        51.3       -5%
get_message_s               46.8        48.7       +4%
search_s                   122.9       123.7       +1%
search_dated_s             119.0       147.8      +24%
sql_count_s                  1.7         2.2      +29%
fts_iops                      18          18       +0%
vector_iops                   91          91       +0%
get_message_iops              16          15       -6%
search_iops                  156         155       -1%
open_store_ms               1790        1927       +8%
row_counts_ms               1291        1261       -2%
oracle_warm_ms             26283       25313       -4%
write_copy_ms               5329        4194      -21%
write_copy_merge_ms          906         637      -30%
write_copy_noop_ms           931         525      -44%
write_copy_delta_ms          593         660      +11%
write_ms_per_commit        637.6       566.4      -11%
write_rows_per_s             784         883      +13%
write_index_build_ms       24979       16088      -36%
write_fold_ms               2075        2439      +18%
```

Raw probe runs (best of 2 is what the row keeps): lance 11 sid 26.9/21.6, mid 52.0/51.3, msg 48.7/86.0, search 127.3/123.7, dated 174.2/147.8; lance 10 sid 29.0/23.9, mid 53.9/59.0, msg 46.8/51.6, search 137.0/122.9, dated 119.0/124.1.

Against the 08-25 lance-10 row for the week-over-week view (the gate's own delta, `be14674-dirty` -> lance 11): sid 21.7 -> 21.6, mid 53.6 -> 51.3, msg 52.1 -> 48.7, search 105.9 -> 123.7 (+17%), dated 102.0 -> 147.8 (+45%), sql_count 1.3 -> 2.2, open_store 1821 -> 1927, row_counts 1438 -> 1261, oracle_warm 28099 -> 25313, write_copy 3671 -> 4194 (+14%), merge 447 -> 637, noop 515 -> 525, delta 658 -> 660, ms_per_commit 359.6 -> 566.4 (+58%), rows_per_s 1390 -> 883 (-36%), index_build 16291 -> 16088, fold 1528 -> 2439 (+60%). The control's deltas against the same 08-25 row are: search +16%, dated +17%, write_copy +45%, merge +103%, noop +81%, ms_per_commit +77%, rows_per_s -44%, index_build +53%, fold +36% - i.e. the week-over-week movement is the store and the day, not lance 11 (next paragraph).

Reconciliations:

- Reads are parity. Every get/search probe is within one run's spread of the control (sid -10%, mid -5%, msg +4%, search +1%), and every warm-query iops field is identical (18 / 91 / 15-16 / 155-156) - lance 11 issues the same S3 requests per query as lance 10 on this store. `sql_count_s` 1.7 -> 2.2 is a single cold-start run each; the 08-25 rows sat at 1.3-1.4 with a smaller store.
- Writes are parity-or-better, and the apparent week-over-week write regression is not lance 11. Against the same-day control, lance 11 is faster on every write field but `write_copy_delta_ms` (+11%) and `write_fold_ms` (+18%), including copy -21%, noop -44%, index build -36%. Against the 08-25 row both of today's rows look 15-100% slower on writes, with the lance-10 control the slower of the two - S3 write latency on 09-01 was simply worse than on 08-25 (the 08-25 rows themselves already showed a 4034 -> 3671 same-day spread on `write_copy_ms`). n=1 per cell; do not read the -21%/-36% as a lance-11 win either.
- **`search_dated_s` +24% (119.0 -> 147.8) was the one field that moved against lance 11, and a dedicated re-measure shows it was noise.** The gate keeps the best of 2 runs; the lance-11 pair was 174.2 / 147.8 against the control's 119.0 / 124.1, with unfiltered search at parity (122.9 vs 123.7), which looked like a zonemap-path effect. Re-measured the same afternoon (16:00-16:12 UTC, same store, launchd jobs paused, the two binaries interleaved ABBA so S3 drift hits both arms alike; `pond search --mode vector "read performance optimization lance" --limit 10 --from-date 2026-08-25`, 5 runs per arm, plus 2 unfiltered runs per arm):

  ```
  dated      lance 10: 140.5  125.7  119.6  129.4  166.8   median 129.4  min 119.6
  dated      lance 11: 121.3  134.2  125.5  126.4  121.9   median 125.5  min 121.3
  unfiltered lance 10: 112.2  132.9
  unfiltered lance 11: 115.8  127.7
  ```

  Parity: the arms overlap completely, and the largest single value in the whole set (166.8) is a lance-10 run. Cold-start dated search on this store is ~120-135 s with a 25-45 s tail on either binary, so a best-of-2 gate cell can land 20% apart between arms without any code difference. Nothing in lance 11's scanner/zonemap/prefilter diff (`dataset/scanner.rs`, `index/prefilter.rs`, `io/exec/scalar_index.rs`, `lance-index/src/scalar/zonemap.rs`, checked at the v10.0.0..v11.0.0 tags) needs to explain anything. No zonemap rebuild required.
- `open_store_ms` +8% (1790 -> 1927) and the 08-25 -> 09-01 `row_counts_ms` / `oracle_warm_ms` drops (-10..-12% / -6..-10%, on both arms) are within the spread the 08-25 pair already showed (1053 -> 1821 open_store on the same day); nothing to attribute.

Stemmer drift - the one behavioral change in lance 11 for pond. Lance 11 replaced `rust-stemmers 1.2.0` with `frostem` (lance-format/lance#8183, to fix a Greek-stemmer panic). The PR describes the swap as output-identical; measured side by side (both crates linked into one binary, English), it is not:

- `/usr/share/dict/words`: 484 / 235,976 words stem differently, 398 of them `-ogist` (`anthropologist`: old `anthropologist`, new `anthropolog`).
- pond's real corpus, a 30,000-row `search_text` sample from the local store (3,064,041 tokens, 29,966 distinct words): 40 words (0.13%) but 7,052 token occurrences (0.23%). Top offenders are ordinary transcript vocabulary: `added`/`adding` (`ad` -> `add`, 4,315 occurrences), `internal`/`internally`/`internals` (`intern` -> `internal`, 1,695), `paste`/`pasted` (`past` -> `paste`, 400), then `evening`, `universal`, `interval`, `organization`, `emergency`.
- Sandboxed fixture store (2,295 messages, index built by pond 0.16.3 / lance 10, queried via `fts('messages', ...)` counts): a lance-11 binary returned `internal` 4 -> 0 and `paste` 1 -> 0 against the lance-10 index. After `pond optimize --rebuild` with the lance-11 binary: `internal` 4, `added`/`adding` 19 -> 32 (`add`, `added`, `adding` now share a stem; the old `paste` hit was a collision with `past`). The lance-10 binary querying the rebuilt index returned `added` 0 and `internal` 0 - the hazard is symmetric.

Consequence: a store's writers must all move to the lance-11 pond before its FTS index is rebuilt, and each store then needs `pond optimize --rebuild` exactly once; until then whole-word FTS misses the drifted forms in whichever direction the index and binary disagree. Neither of today's gate rows is affected (the gate's search probes run in `vector` mode and the equivalence check is get-based), and the s3-nbg1 store was deliberately not rebuilt because other hosts still write to it with lance-10 binaries. On this store the rebuild measured 10m13s on 08-25.

## bench-gate: quiet Linux re-run (openclaw DB era, #228)

### 2026-09-10 - ws-pond-01, `92c32c4`, pond 0.17.1 (x86_64-linux) - the clean row

The re-run the section below deferred, taken after [#232](https://github.com/tenequm/pond/pull/232) merged so the row carries `rowmap_cold_ms` / `rowmap_warm_ms` instead of the dead `oracle_warm_ms`. Machine quiet this time: `pond schedule stop` for the window, no cargo running (every binary the gate needs was pre-built, so its own `cargo build --release` finished in 0.63s), nothing else on the VM. `EQUIVALENCE OK`. Clean tree at the merge commit, so this is the first row all day whose `commit` field is not `-dirty`.

**It refutes half of the contamination call in the section below, and the half it refutes is the half that named a cause.** Four rows on one machine, one store (`5fcd5e32b8dd`), one day:

```
metric                  19:24        20:29        21:06        22:09
                      e3e6a26      5838108   5bc4f8b(*)      92c32c4
write_copy_ms            2812         2954         7353         3777
write_copy_noop_ms        393          266         1086          481
write_index_build_ms     9063        12982        14480        14284
search_dated_s            4.5         10.3         15.0         10.0
search_s                  3.6          4.8          5.5          4.1
get_session_sid_s        14.7         19.9         16.7         15.8
                                            (*) the loaded run
```

- **Contention confirmed for the `write_copy` family.** `write_copy_ms` 7353 -> 3777 and `write_copy_noop_ms` 1086 -> 481 on a quiet machine. Those two spikes were the operator's builds, as claimed.
- **Refuted for `write_index_build_ms`.** It did not recover: 14480 loaded, 14284 quiet, a 1% difference. It instead steps 9063 -> 12982 between the 19:24 and 20:29 rows and stays at 13-14.5s for the three rows after. Three of four rows agree; the 9063 row is the outlier, not the norm. The section below called this column "precisely the CPU-bound" evidence of contention, and that was wrong twice - it did not move with load, and it is not CPU-bound, because `write_bench` builds its index against an S3 scratch prefix and the time is dominated by round trips to nbg1.
- **Partly refuted for `search_dated_s`.** 15.0 -> 10.0 quiet, so load was worth ~5s, but the floor is 10s and the 19:24 row's 4.5s does not come back. The 20:29 row independently read 10.3s.

No PR in the bracket touches the write path or the scanner, so the honest reading of the step between 19:24 and 20:29 is store- or endpoint-side drift that one gate row cannot separate from a code effect. That is the same conclusion #226 reached from the other direction, and it is the durable finding here: **on this store a single gate row brackets a change to within roughly a factor of two, and no amount of quiet fixes that.** Deltas printed against the immediately preceding row should be read as "nothing moved by more than the noise floor" or not at all.

First gate reading of the new columns: `rowmap_cold_ms` 141478, `rowmap_warm_ms` 1119. Cold is the ~2m21s a first sync on a fresh host pays before printing anything (#226 measured 183805 ms on the same path an hour earlier - a 23% spread on a number neither run had any reason to move, which is the noise floor stated again).

`bash ops/scripts/bench-gate.sh`, `pond schedule start` afterwards.

## bench-gate: second Linux row (openclaw DB era, #228)

### 2026-09-10 - ws-pond-01, `5bc4f8b-dirty`, pond 0.17.1 (x86_64-linux) - CONTAMINATED, but see the clean re-run above

**Read nothing into this row's deltas. The machine was not quiet, and the operator is the one who made it noisy.** For the whole 30-minute window (20:36-21:06Z) the same VM was running `cargo bench ingest_bench` (a 9-minute compile plus two bench passes), `cargo test --locked`, `cargo clippy --all-targets`, and a `cargo build` - and the gate's own `write_bench` leg spent 2m24s compiling against that. The row is in `bench-gate-baseline.jsonl` because the gate appends it itself, and this entry exists so nobody later reads it as a regression.

The delta the gate printed, against the `e3e6a26-dirty` row 1h42m earlier on the same machine and the same store digest `5fcd5e32b8dd`. That was the last row in this branch's copy of the jsonl at the time; the `5838108-dirty` row from #226 at 20:29Z landed in `main` in between, so in the merged file the row above this one is that one, not the one the delta names:

```
metric                      prev         now     delta   (2026-09-10T19:24:03Z e3e6a26-dirty -> 2026-09-10T21:06:44Z 5bc4f8b-dirty)
get_session_sid_s           14.7        16.7      +14%
get_session_mid_s           14.0        17.5      +25%
get_message_s               16.3        14.1      -13%
search_s                     3.6         5.5      +53%
search_dated_s               4.5        15.0     +233%
sql_count_s                  0.8         0.6      -25%
fts_iops                      42          27      -36%
vector_iops                   92          92       +0%
get_message_iops             620          75      -88%
search_iops                  398         215      -46%
open_store_ms                455         806      +77%
row_counts_ms                778         369      -53%
oracle_warm_ms             27624       33447      +21%
write_copy_ms               2812        7353     +161%
write_copy_merge_ms          743         645      -13%
write_copy_noop_ms           393        1086     +176%
write_copy_delta_ms          415        1009     +143%
write_ms_per_commit        338.2       381.0      +13%
write_rows_per_s            1478        1312      -11%
write_index_build_ms        9063       14480      +60%
write_fold_ms               2192        2588      +18%
```

Why this is contention and not code: **neither PR in the bracket touches the read or write substrate.** #225 adds an adapter, #228 fixes a different adapter and the CLI surfaces that report counts. Nothing between the two rows changed a Lance call, a commit path, or an index policy. Yet the columns that blew out are precisely the CPU-bound ones - `write_copy_ms` +161%, `write_index_build_ms` +60%, `write_copy_noop_ms` +176% - while `vector_iops` (S3 requests per warm query, a count rather than a time) is identical at 92. A code regression does not move wall-clock and leave request counts untouched; a busy machine does exactly that.

**The quiet re-run above kept the first and third of those and threw out the second.** `write_copy_ms` and `write_copy_noop_ms` came back down; `write_index_build_ms` did not, and calling it CPU-bound was an error - it is S3 round trips. The paragraph is left as written because being able to see which half of a call held is the point of an append-only notebook, but read it with that correction attached.

`equivalence: OK` - the map-vs-scan hard gate passed, which is the one assertion in this row that a loaded machine cannot falsify.

`get_message_iops` 620 -> 75 is the one delta worth a second look, since iops is a request count. It is not a win: warm-query iops track how many index segments a query pages through, and the store is written by other hosts on a schedule, so segment count moves between runs independently of anything measured here (the 08-25 entry records the same effect after an `optimize --rebuild`). Treat cross-row iops as a property of the store's shape that day.

**A clean re-run is deferred to after [#232](https://github.com/tenequm/pond/pull/232) merges** (done - it is the section above), deliberately rather than for scheduling reasons. #232 replaces `oracle_warm_ms` - which times `Store::session_last_message_ids`, a function with no production callers - with `rowmap_cold_ms` / `rowmap_warm_ms` against `ensure_rowmap`, the path sync actually takes. Re-running before that lands would buy a quiet row with a dead column still in it. After it lands, one quiet run gets both the right columns and a usable Linux noise floor, which is what the first two Linux rows were supposed to establish and neither can.

`bash ops/scripts/bench-gate.sh`, 30m 08s wall. Local pond sync schedule stopped for the window (`pond schedule stop`) per the 2026-08-25 incident below; other hosts still push to this store hourly.
## rowmap identity guard (#226)

### 2026-09-10 - ws-pond-01 -> s3-nbg1, `234f5e7` vs installed 0.17.1

The guard that stops a rebuilt store inheriting the previous store's rowmap adds one probe read per `ensure_rowmap`. Measured on the path that actually changed, because **the bench gate did not reach it** - and that gap is fixed in the same PR.

`ops_bench`'s `[sync] change-detection oracle` timed `Store::session_last_message_ids`, a full scan of messages that **no production path has called since the resident rowmap replaced it**: the only callers left are two `mod tests` blocks and `sync_oracle_bench`, where comparing oracle strategies is the point. So the gate spent ~90 s per run measuring a path `pond sync` does not take, reported it as the sync oracle, and a change to the real oracle passed it untouched. It now times `ensure_rowmap` - what sync actually calls - as COLD (full build into an empty scratch cache) and WARM (a second `Store`, so the chain must be discovered, validated and mapped from disk rather than reused from memory). The jsonl row drops `oracle_warm_ms` and gains `rowmap_cold_ms` / `rowmap_warm_ms`; rows before 2026-09-10 carry the old column and are not comparable to the new ones.

First readings of the live path, same store:

```
ensure_rowmap COLD                        183805.2 ms
ensure_rowmap WARM                           743.0 ms
```

Three minutes to build the map from scratch is what a first sync on a fresh host pays before it prints anything, and it had never been measured. WARM at 743 ms includes the identity probe this PR adds.

`pond sync --dry-run` builds the freshness map through `ensure_rowmap` (`main.rs:4476`) and writes nothing to the store, so it isolates the change. `XDG_CACHE_HOME` was redirected per arm, both arms seeded from one cold build of the operator's real chain (base `v8917`, 477 MB, plus delta `d8921`), so neither arm read or wrote `~/.cache/pond`.

`hyperfine --warmup 2 --runs 15`, medians rather than means because S3 throws the occasional multi-second outlier and the effect is a few hundred ms (one earlier run had a 6.1 s flyer that dragged an arm's mean below the other's):

```
before   median  599.6 ms   p25  499.3   p75  712.9   [457.6 … 837.5]
after    median 1056.2 ms   p25  966.6   p75 1256.5   [916.7 … 1631.1]
```

**+457 ms per `ensure_rowmap`, x1.76 on medians.** The two distributions do not overlap at all here - the slowest before-run (837.5 ms) is faster than the fastest after-run (916.7 ms) across 15 runs each - so this is the guard, not jitter. In proportion: it roughly doubles `sync --dry-run`, which is sub-second either way, and is ~1% of a real sync against this store, where the oracle alone is ~27 s. A sync that imports rows calls `ensure_rowmap` twice and pays it twice.

The guard reads a row count as well as probing rows: identity alone accepts a foreign chain that happens to share a prefix, which is not exotic - a store re-synced from the same sources replays its oldest session first, and only diverges where a session grew. The count costs ~13 ms against the probe-only version measured earlier (444 ms), because it is manifest metadata rather than data pages.

**No false positive on the real store**, which was the thing worth checking: the guard kept the 477 MB chain (`v8917` + `d8921` still present in the after arm), rather than deciding a live production map was foreign and forcing a full rebuild over ~2M rows.

Gate run for the "nothing else moved" half: `moon run repo:bench-gate` on the branch, `equivalence: OK`, row appended to `bench-gate-baseline.jsonl` (`5838108-dirty`). Its delta against the row 65 minutes earlier is **not readable as a code comparison** - same store, same host, and the probes swing both ways by 30-130% (`search_dated` +129%, `row_counts` -59%, `open_store` +90%) on paths this change does not touch. That spread is the honest measure of how much a single gate row can bracket a read-path change on this store: not much.

## bench-gate: first Linux row (agy adapter, #225)

### 2026-09-10 - ws-pond-01, `e3e6a26-dirty`, pond 0.17.1 (x86_64-linux)

The gate run that accompanied the `agy` adapter. **The delta this run printed is a change of environment, not a change of code**: the previous row is mac-m1max on 0.16.3, this one is a Linux VM on 0.17.1, so read it as "what this store looks like from another machine" and compare future Linux rows to this one instead. `equivalence: OK` (the hard gate). Store digest `5fcd5e32b8dd`, same store as the Lance 8/10/11 brackets above.

`moon run repo:bench-gate`, 30m 37s wall.

```
--- CLI probes (2 runs each, best kept) ---
get_session_sid              run1  14.7s
get_session_sid              run2  16.1s
get_session_mid              run1  16.7s
get_session_mid              run2  14.0s
get_message                  run1  16.3s
get_message                  run2  26.7s
search                       run1  135.2s
search                       run2  3.6s
search_dated                 run1  4.5s
search_dated                 run2  14.9s
sql_count                          0.8s
```

The `search` pair is the whole story of this column: 135.2 s cold against 3.6 s warm, and the gate keeps the best, so `search_s: 3.6` in the jsonl is a warm-cache number. `search_dated` ran the other way round (4.5 s then 14.9 s), which is how much this store's read latency moves with cache state alone - a reminder that the probe columns are not stable enough to bracket a code change on their own.

```
=== S3 IO per warm query (component isolated) ===
component         iops_p50  iops_p95    bytes_p50    bytes_p95
scope_count              0         1            0            0
fts_search              42        64      1049135      7503233
vector_search           92        99       504710      5548148
pond_get_message       620       628     12669221     12753073
pond_search            398       521      4750380      8030193
  hydration            264       357    (derived: pond_search - scope - fts - vector)
```

`pond_get_message` at 620 iops / 12.7 MB per warm query is the outlier worth chasing: one message expansion pulls more objects and more bytes than a whole search. Previous rows recorded 15-16 iops for it on mac-m1max, so this is not a regression in kind but a different measurement - #168 reworked the probe, and the io-trace here counts the hydration reads that the older row did not attribute.

```
ops_bench: s3+https://nbg1.your-objectstorage.com/pondarium/pond

  open store (manifests)                       455.1 ms

[status] (read-only)
  row_counts                                   778.0 ms
  table_sizes                                 2030.1 ms
  index_status                                  58.0 ms
  embedding_progress                         37539.1 ms
  adapter_names(false)                        6331.4 ms

[sync] change-detection oracle (read-only)
  session_last_message_ids COLD              64787.2 ms
  session_last_message_ids WARM              27624.7 ms

[optimize] backlog probes (read-only)
  stale_embedding_count                      10989.1 ms
  embedding_progress                         23318.0 ms

[copy] delta detection, self-to-self (read-only)
  plan_incremental_from                      32698.2 ms
  (delta sessions: 0)
```

`open store` 455 ms and `row_counts` 778 ms are 3-4x better than the mac rows (1790 ms / 1291 ms); the oracle warm path is within 5% (27.6 s vs 26.3 s). Manifest reads move with the network, the oracle moves with the corpus - which is the shape you would expect if the oracle is doing real work and the manifest reads are latency-bound.

```
append write-path sweep (messages table, fresh dest per point, cap 10 commits):
 batch      rows    commits    wall_ms    ms/commit
   512      2500          5       1691      338.2    (1478 rows/s)
```

Against the mac row's 637.6 ms/commit and 784 rows/s. Same bucket, same batch size, different driving host.

```
--- delta vs previous run ---
metric                      prev         now     delta   (2026-09-01T15:33:32Z 61c1498-dirty [pond 0.16.3 (aarch64-macos)] -> 2026-09-10T19:24:03Z e3e6a26-dirty [pond 0.17.1 (x86_64-linux)])
get_session_sid_s           23.9        14.7      -38%
get_session_mid_s           53.9        14.0      -74%
get_message_s               46.8        16.3      -65%
search_s                   122.9         3.6      -97%
search_dated_s             119.0         4.5      -96%
sql_count_s                  1.7         0.8      -53%
fts_iops                      18          42     +133%
vector_iops                   91          92       +1%
get_message_iops              16         620    +3775%
search_iops                  156         398     +155%
open_store_ms               1790         455      -75%
row_counts_ms               1291         778      -40%
oracle_warm_ms             26283       27624       +5%
write_copy_ms               5329        2812      -47%
write_copy_merge_ms          906         743      -18%
write_copy_noop_ms           931         393      -58%
write_copy_delta_ms          593         415      -30%
write_ms_per_commit        637.6       338.2      -47%
write_rows_per_s             784        1478      +89%
write_index_build_ms       24979        9063      -64%
write_fold_ms               2075        2192       +6%
```

The `-dirty` on the commit is uncommitted `ingest_bench.rs` work in the same tree, not a modified read or write path.

## ingest_bench: openclaw, both storage eras (#228)

### 2026-09-10 - ws-pond-01, local store, release profile

Two rows rather than one because openclaw's two eras share no read code. The file era (through 2026.7.1) walks a directory, parses JSONL and decompresses zstd archives. The DB era (2026.8.1+) issues rusqlite queries against a `transcript_events` table and never opens a transcript file - on 2026.9.3 there is no file tier at all. Same adapter, two source layouts, no shared read path between them.

`cargo bench --bench ingest_bench -- --adapter openclaw --source-dir tests/fixtures/adapter/openclaw-captures/<pass> --passes 2`

```
=== openclaw-captures/db-era [openclaw] (3 source files) [pass 1/2] ===
wall                 0.06 s
peak rss              217 MB
rows              inserted=112  matched=0
stages            decode=0.01s (25%)  validator=0.04s (73%)  other=0.00s ( 0%)
calls             decode_calls=113  validator_calls=113
merge_insert sessions    calls=    1  total= 0.01s  mean=  8.0ms  min=  8ms  max=   8ms  rows=8
merge_insert SUM  0.01s  (15% of total)

=== openclaw-captures/db-era [openclaw] (3 source files) [pass 2/2] ===
wall                 0.03 s
rows              inserted=0  matched=112
stages            decode=0.01s (42%)  validator=0.01s (54%)  other=0.00s ( 0%)
merge_insert SUM  0.00s  (0% of total)

=== openclaw-captures/cron [openclaw] (13 source files) [pass 1/2] ===
wall                 0.06 s
peak rss              196 MB
rows              inserted=80  matched=0
stages            decode=0.01s (10%)  validator=0.05s (88%)  other=0.00s ( 0%)
calls             decode_calls=81  validator_calls=81
merge_insert sessions    calls=    1  total= 0.01s  mean=  8.0ms  min=  8ms  max=   8ms  rows=11
merge_insert SUM  0.01s  (15% of total)

=== openclaw-captures/cron [openclaw] (13 source files) [pass 2/2] ===
wall                 0.03 s
rows              inserted=0  matched=80
merge_insert SUM  0.00s  (0% of total)
```

Validator 73-88% against decode's 10-25%, matching the agy row's 68/30 below on a completely different read path. Three source layouts now agree - protobuf-in-SQLite, JSONL directory walk, and rusqlite - which is the evidence [#229](https://github.com/tenequm/pond/issues/229) wanted for the cost sitting downstream of the adapter seam rather than in any one decoder.

Pass 2 is `inserted=0 matched=<all>` with `merge_insert SUM 0.00s` in both, which is the additive-sync property measured rather than asserted.

**These are not a before/after, and the absolute times are not usable.** The corpora are 80 and 112 rows, so only the decode/validator ratio travels. And the old adapter ingested 0 of 6 sessions on the db-era root and 4 of 11 on cron, so a decode ratio against it would compare different amounts of work rather than the same work done faster - there is no honest "before" to divide by. This is a baseline row.

`peak rss` is a process high-water mark, so on `--passes 2` it reports the larger pass (pass 1, the cold insert), not the last one.

## ingest_bench: agy adapter (#225)

### 2026-09-10 - ws-pond-01, local store, release profile

First run of the registry-driven `--adapter` flag: before this the bench had a hand-written enum with two of the fourteen adapters in it, so most of the registry could not be benched at all.

`cargo bench --bench ingest_bench -- --adapter agy --source-dir tests/fixtures/adapter/agy`

```
=== tests/fixtures/adapter/agy [agy] (62 source files) ===
wall                 0.06 s
peak rss              196 MB
rows              inserted=154  matched=0
stages            decode=0.02s (30%)  validator=0.04s (68%)  other=0.00s ( 0%)
calls             decode_calls=156  validator_calls=155
merge_insert sessions    calls=    1  total= 0.01s  mean=  6.0ms  min=  6ms  max=   6ms  rows=12
merge_insert SUM  0.01s  (11% of total)
```

Validator at 68% against decode's 30% is the same split every adapter shows: protobuf decoding of 62 steps is cheaper than the fan-out that stores them.

End-to-end through the CLI, `hyperfine`, one fresh store path per run (see the warning below):

```
fixture: 13 databases, 12 sessions, 77 messages   mean  226.2 ms   sd  9.2   n=10
one conversation: 20,000 steps, 82 MB            mean 1629.9 ms   sd 55.9   n=5
```

~12,300 messages/s on the large conversation, which is what confirms the adapter's 512-row keyset paging is not paying a round-trip penalty against the single scan it replaced.

**Measurement warning, cost three wrong numbers here before it was caught.** `pond` keeps a row-metadata cache at `~/.cache/pond/rowmetamap-<hash>` keyed by the *store path*, and it outlives the store directory. Deleting the store and syncing again into the same path reports "up to date", ingests nothing, and times a no-op: the same fixture measured 104 ms that way against 226 ms honestly, and the 20,000-step conversation measured 92 ms against 1630 ms. Give every timed run its own store path (`--parameter-scan` with `--runs 1`), and assert the row count afterwards. Tracked as [#226](https://github.com/tenequm/pond/issues/226).

## ingest_bench decode: codex-cli JS-runtime decode (#216)

### 2026-09-01 - mac-m1max, local `~/.codex/sessions/2026/09`, old 0ab11c7 vs new (fix/codex-exec-wrapper, uncommitted)

The #216 fix adds a per-file index pass to the codex-cli adapter (`JsonlTree::file_state`: `call_id -> wrapped tool name` and `call_id -> {command, cwd, exit_code}` of the `CommandExecution` rows in each call's window) plus a byte scan of every `exec` snippet for `tools.<name>(`. This is the decode-stage before/after that gates it. `ingest_bench` gained `--adapter codex-cli` for the purpose (the harness file is identical on both sides; only the three adapter/seam sources differ).

Setup: dev profile (both sides, so the delta is relative; absolute decode is ~3x slower than release), one worktree, old side built by swapping `codex_cli.rs` / `extract.rs` / `jsonl.rs` to their `origin/main` content and snapshotting the binary, then restoring. Corpus: the 0.151+/0.152 rollouts under `~/.codex/sessions/2026/09` - 42-47 files / 19.3k-20.2k rows (live, grew during the run; every JS-runtime row type present, 1,289 `CommandExecution` rows in the 18:39Z snapshot). Store: TempDir, NoopOracle, pass 1. Runs interleaved old/new. The host was NOT quiet: load average 20-30 throughout (other sessions compiling), which is what the spread below is.

```
pair   old decode   new decode   old wall   new wall   note
1      0.32 s       0.35 s       2.54 s     2.60 s     new = whole-item clone in the index
2      0.33 s       0.34 s       2.54 s     2.56 s     new = whole-item clone in the index
3      0.34 s       0.38 s       3.78 s     3.28 s     slim index (three fields) from here on
4      0.45 s       0.66 s       3.13 s     3.44 s     loaded
5      0.36 s       0.36 s       2.73 s     2.71 s
6      0.41 s       0.42 s       3.72 s     5.58 s     loaded
7      1.22 s       0.59 s       6.12 s     4.83 s     loaded (old side hit)
8      0.39 s       0.38 s       3.01 s     2.84 s
```

Reconciliation: min decode old 0.32 s vs new 0.34 s; the quiet pairs (1, 2, 5, 8) differ by 0.00-0.03 s on ~20k rows, below the run-to-run spread of either side under this load (old alone ranged 0.32-1.22 s). Validator (the merge_insert fan-out) is 85-90% of wall on both sides and unchanged. Pair 3 onward carries the slim index (the first cut cloned each `CommandExecution` item whole, stdout/stderr included; the index now keeps only `command` / `cwd` / `exit_code`) - pairs 5 and 8 are the cleanest read of the shipped code: parity. Nothing here is a release-profile number; rerun with `cargo bench --bench ingest_bench -- --adapter codex-cli --source-dir <codex 0.151+ corpus>` on a quiet host if a release-profile row is ever needed.
