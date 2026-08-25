# Benchmark results log

An append-only lab notebook, not a regression gate. Every number here is host-, disk-, OS-, and corpus-shape-dependent, so a figure means nothing without the environment it was taken in: compare within one environment, or compare ratios across them. CI never runs these benches - every entry was produced by hand on the machine it names.

Entries are newest first within each bench. Output is pasted verbatim: the bench's printed table is the schema, and retyping it into a normalized one is where transcription drift enters.

Numbers that have hardened into rules live in CLAUDE.md as prose (the S3 commit-latency law, the sync change-detection oracle, the `count_rows` inequality trap). This file is the underlying measurements, including runs whose only conclusion was "no change".

`write_bench` was named `copy_bench` until v0.11.1 (`ad9134f`, 2026-07-01); recovered entries predating the rename say so. Older runs not yet transcribed here can be recovered from pond itself - the bench output is a tool body, so it is reachable through `pond_sql` on `parts.variant_data`, not through `pond_search`.

## Environments

- **mac-m1max** - Apple M1 Max, macOS 26.6.2, APFS on internal SSD.
- **win-5700x3d** - AMD Ryzen 7 5700X3D (8C/16T), 16 GB, Windows 11 Pro 10.0.26200, NTFS on NVMe SSD.
- **s3-nbg1** - Hetzner object storage at `nbg1.your-objectstorage.com`, driven from mac-m1max.

No Linux entry exists yet: on 2026-08-17 every recorded session mentioning a benchmark was searched for bench output carrying a Linux path or target triple, and none exists. CI does not run benches, so a Linux number would have to be produced deliberately.

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

Roughly 1 s per commit, flat from 1 to 512 rows - the measurement behind the CLAUDE.md rule that S3 write cost is commit-count-bound, not bandwidth-bound. Source was a local 1000 x 30 corpus, cap 30 commits.

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
