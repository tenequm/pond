# Live-write: activation plan for spec 9.4

Status: PLANNED, gated on #342 (lance 13 pin) and the #310 re-measure.
Issue: #43. Spec: section 9.4 (micro-batched single-writer ingest), 7.8 (`pond serve --with-sync`).
Sources: the 2026-09-09 design assessment (three-agent research pass: pond architecture,
Lance MemWAL maturity, lance-context history), and rpavlovs' fleet measurements in
#310 / #43 (2026-10-08/09).

## 1. What live-write is, and is not

Goal: new session events visible to readers (search/get/SQL, local and cross-host)
at a cadence of seconds-to-a-minute, instead of the 5-minute interval sync.

Not a goal: sub-second visibility (`lance-forward-compat-no-subsecond-freshness`
deliberately forbids promising it), and NOT a request-cost reduction. A shorter
cadence multiplies changed cycles; each changed cycle pays a fixed read overhead
(#310). Live-write saves source polling, which is local disk and free in
object-store requests. The cost work is #310/#342, not this plan.

## 2. Decided architecture (2026-09-09, reaffirmed 2026-10-13)

**A: micro-batched appends inside the resident `pond serve --with-sync` process.**
One process per host buffers canonical events and flushes one append batch per
interval or size threshold. No new substrate: the existing write chokepoint, OCC,
and `lance-deterministic-pk` idempotency. Durability of unflushed events comes from
the sources (`session-movement-complete`: a re-sync replays them) - pond stays a
mirror for anything not yet flushed, per spec 9.4.

**Intake decision (2026-10-13, answered on #43):** the server watches its own
configured adapter sources. fs-notify marks a source dirty; the changed file is
re-read WHOLE through the existing adapter parse face; the periodic full walk stays
as the completeness backstop (watchers drop events). No byte-offset tailing - that
couples ingest to mid-write file state and was explicitly rejected. No separate
producer process. A buffered HTTP receive (clients push canonical events, server
batches) is a later phase, only for sources with no local files; it requires a
producer that turns native records into canonical events, which today exists only
inside the adapters, and `/v1/ingest` validates and flushes per request (confirmed
against 0.19.5 by rpavlovs, #43).

**C (optional later): in-process read overlay** - serve daemon answers reads over
base-table-union-buffer for sub-second in-daemon visibility. Legitimate because
durability comes from sources, so the memtable needs no WAL. Build only if a
consumer demands sub-second; nothing today does (herdr desk bar: 15 s; rpavlovs'
bar: ~1 minute).

**B: MemWAL - rejected, stays deferred.** Head-to-head evidence (2026-09-09):
- Its write-side win (commit-free high-throughput ingest) solves a problem pond
  does not have (peak is a few messages/sec); its read-side cost lands exactly
  where pond hurts (per-query shard-manifest reads + per-generation dataset opens;
  lance-context measured ~654 ms at 200 unmerged generations vs ~50 ms compacted).
- It splits the read surface: `Dataset::scanner()` never sees MemWAL data;
  `pond_sql` would be structurally blind to the fresh tier; every foreign reader
  sees base-only, silently.
- Each row transits the store ~3x (WAL -> SSTable -> merge-insert into base);
  merge-insert is the commit-latency-bound path measured 5.47x slower than append.
- No in-tree compactor or GC (upstream asks lance-context#163/164/165 open);
  lance-context needed ~5 months, an etcd control plane, and still carries a
  permanent-duplicate-rows defect (lance-context#180) that would violate
  `adapter-integrity` outright.
- Pond's write model fits neither MemWAL mode: PK mode requires one-writer-per-PK
  (cannot express one session written from two machines - the real pond topology,
  see #146); append-only mode drops the PK and with it the dedup floor.
- Possible hard incompatibility: stable row ids (`lance-table-creation-stable-row-ids`)
  vs `defer_index_remap`, which MemWAL-present compaction needed at lance-context's pin.

Adoption trigger (unchanged, spec 9.4): hosted multi-writer ingest - many
independent producers contending on one table - and at that point MemWAL behind
pond's own HTTP ingest with pond as the single sharded writer, not at the
per-machine edge. Watch upstream for a first-class compactor API
(lance-context#163) and the #180 replay-cursor fix before ever reconsidering.

## 3. The cost model (what rpavlovs' data added, 2026-10-08/09)

The 2026-09-09 assessment costed a flush as "2 commits, only when pending" and
missed the fixed READ overhead per changed cycle. Measured (Moto fixture, 0.19.5,
128 sessions / 7 messages fragments): a quiet resident minute = 0 GET / 5 LIST;
importing two messages = 52 GET; catching up on another writer's commit = 22 GET.
On his fleet, 200 of 288 daily 5-minute cycles import something, so the per-changed-
cycle fixed cost, times cadence, times hosts, is the binding constraint on a
request-billed store (R2) - not commit count.

Design metric for this plan: **fixed object-store requests per changed cycle**.
The flush-cadence bench sweeps it (requests and fragments vs cadence), and the
default cadence is chosen from that sweep, not assumed.

Dominant known component: the unbounded `message_store_probe` (unfiltered
`scan(id) LIMIT 3`), which runs on the sync oracle, cursor persistence, and rowmap
extension (#310). MEASURED FIXED by lance 13's #9344 (2026-10-13, isolated
13-pin build, 128-fragment fixture, local + shared-memory stores): fragments
planned 128 -> 1, probe GETs 88-355 -> 4-5 deterministic, ~4.5 KB read,
identical probe results, deletions included. #342 tracks the pin; the
probe-local first-fragment fallback is dropped - no pond-side bound needed on
this evidence. Numbers on #310, compile findings on #342.

## 4. Ordering (deliberate, posted on #43)

1. **#342 + #310 re-measure** - make a changed cycle cheap. Nothing else first.
2. **Shorter `--sync-every` on the released serve-with-sync topology** - no new
   code; likely reaches ~1-minute freshness. rpavlovs is running exactly this and
   offered before/after fleet request counts; those numbers decide step 3.
3. **Phase work below** - only if step 2 shows the interval source walk or cadence
   floor is the remaining bottleneck.

## 5. Phases (from the 2026-09-09 assessment, updated)

- **Phase 0 - lock granularity (prerequisite, standalone win).** Reads currently
  take the same per-table mutex the writer holds across whole commits, compactions,
  and index builds. Narrow it to guard only the cached-handle read/replace; Lance
  OCC is the correctness mechanism. Note: #337 (merged 2026-10-13) already fixed
  the retry-staleness half (a failed write refreshes the cached handle before
  retrying), which live-write's higher commit frequency would have tripped.
- **Phase 1 - time-triggered flush.** `IngestValidator` is already the buffer with
  a count trigger (100 substreams); add the time threshold (order of 5 s local /
  30 s remote - final values from the cadence bench).
- **Phase 2 - fs-notify intake.** Watcher feeds the pending set; whole-file re-read
  through the adapter; periodic walk stays as reconciliation. Converts freshness
  from "up to sync interval" to "flush cadence" and cuts no-op cycle cost to zero.
- **Phase 3 - read-freshness seams.** (a) Delta-extend the resident rowmap as part
  of each flush (the parked #298 straddle-delta, branch
  `perf/285-warm-get-fast-path` @ 18ab06f, is the validated machinery - cherry-pick
  when PR #292 lands; see the #292 comment of 2026-09-23). (b) Fragment-count and
  time triggers on the index-fold gate so the unindexed tail stays bounded under
  many small flushes. (c) Pin or lengthen the LazyEmbedder idle eviction in daemon
  mode.
- **Phase 4 - maintenance decoupled from flushes.** Slower independent tick running
  budgeted compaction (hot tail excluded) and the fold ladder. Safe only after
  Phase 0.

## 6. New obligations live-write creates

- **Duplicate reconcile (#146).** Two hosts racing the same new session produce
  exact-content duplicate PKs (Append-vs-Append is an OCC non-conflict; measured:
  194 dups). Live-write's single-writer-per-host lowers the window but multi-host
  fleets keep it open. Off-hot-path reconcile in the maintenance tick (or
  `pond repair --dedup` for the exact-content class) moves up with this plan.
- **Spec edit on activation**: activate 9.4, amend 7.8's "does not activate
  live-write" sentence. The design implemented must stay verbatim 9.4's.
- **Multi-writer integration test**: resident writer + external cron sync on one
  store (OCC + #337's retry refresh are the mechanisms under test).

## 7. Acceptance

- Event -> visible (search/get/SQL): ~flush cadence; 1-5 s local, 10-30 s remote.
- Fixed requests per changed cycle: measured in the bench gate; no regression vs
  the post-#342 baseline interval sync.
- Reads do not stall during flush or maintenance (Phase 0).
- Crash loses nothing adapter-sourced; HTTP-pushed events remain mirror-semantics
  until flushed (documented).
- A quiet cycle stays ~zero object-store requests.
