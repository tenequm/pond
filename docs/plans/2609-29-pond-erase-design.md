# pond erase: design (#45)

Status: design, revision 2. This revision answers two independent external reviews (gpt-6-astra and claude-fable-5). Both concluded "not implementable as written, fixable within the locked decisions"; the section "Review resolution" maps every finding to where it is addressed or disputed. Ten decisions are locked: the original seven plus three that came out of the reviews (8-10). No decision is open. The remaining open items are the verification items V1-V10 in the test plan. Tracks [#45](https://github.com/tenequm/pond/issues/45). The existing contract is spec 5.4 `session-append-only-exception` plus the 7.8 `pond erase` bullet.

## Goal

`pond erase` is the one sanctioned deletion. It retires whole sessions (with descendants per D6), purges their bytes from this store, and keeps them from coming back.

The hard requirements:
- No resurrection from any upgraded ingest path.
- Purge success is proven, not reported blindly. The #45 review showed that a naive optimize-based erase reports success while leaving the bytes on disk.
- Every other guarantee stays intact (`session-durable-copy`, `adapter-integrity-additive-sync`, `session-movement-complete`).
- Erased content is unreadable once phase 1 completes, including through each host's local caches. The residual anomaly windows - a racing write, self-heal, a pre-erase binary - are named and bounded in D5 rather than claimed away.

## Locked decisions (user, 2026-09-29)

1. **No writer version gate.** An older pond binary that syncs a still-present source after an erase will resurrect the session, because it does not know the denylist. The release note carries a hard requirement: upgrade every host that touches the store (writers and serve hosts) before the first erase. The only guard is detection: phase 2 and `pond status` report resurrected sessions (D5). A capability gate - a manifest key that newer binaries honor before writing - was rejected as unwanted complexity. It also could not protect against today's binaries, since none of them read such a key.
2. **Two-phase purge.** Phase 1 runs at invocation: intent, delete, verify, re-sweep, epoch, targeted rewrite, rebuild of every index. Phase 2 runs only after the spec 3.4 retention window: a cleanup anchored on a version, not a time. It is driven by a per-operation record that `pond sync` and `pond optimize` pick up (D5).
3. **The 7-day unverified-orphan window is accepted and reported**, as an eligibility date rather than a deadline (D5). There is no quiesce requirement and `delete_unverified` stays off.
4. **HTTP ships in the same iteration as the CLI**, as `POST /v1/x/erase`. It sits behind `require_allowed_host` like `/v1/x/sql` and gets its own 7.4 error rows.
5. **Cascade: spawn edges only; continuations refuse without a flag (D6).** Spawn edges cascade automatically. A closure that reaches any continuation or fork edge is refused, with the full closure printed, unless `--include-continuations` is passed. There is no partial erase either way: the flag erases the whole closure. The spec-literal cascade over every edge (today's 5.4 text) is rejected, because continuations carry copies of the parent's history and may be live conversations.
6. **#146 remedy: `pond erase --allow-reingest` only; no second deletion exception (D4).** Erase stays the single deletion pond performs. The remedy has two known holes, stated in D4 and in the #146 remedy text: once the source is rotated or gone there is no remedy, and it loses anything the source has dropped since pond stored it. An exact-content dedup repair is the named future escalation, not designed here.
7. **A never-stored id returns `not_found` (D8).** There is no pre-emptive denylisting: `pond erase` only erases what the store holds (or already denylists).
8. **Only exact spawn branding cascades (D6; review decision N1).** A subagent edge cascades automatically only when the child's adapter declares its spawn branding exact (`LineageFidelity.spawn_brand_exact`). Every other edge is refused like a continuation unless `--include-continuations` is passed. The accepted cost: real hermes and oh-my-pi subagents, and sessions of any unregistered brand, always need the flag.
9. **Ship with documented lineage gaps (D6; review decision N2).** Erase follows only recorded edges. The per-adapter lineage table in D6 is a documented limitation, and the plan output prints `lineage:` warnings wherever a closure member's adapter records an edge kind partially or not at all. There is no prerequisite openclaw closure hook.
10. **`pond_sql` export artifacts are out of scope for deletion (D9; review decision N3).** Erase does not touch `<store>/exports/`. A blanket delete cannot be targeted at erased content without scanning every file, and it would create a second deletion surface, against the single-exception principle. When export artifacts exist, erase's output says so and the operator decides. A general export pruning or TTL mechanism is a separate follow-up (see "Follow-ups").

## Findings the design builds on (verified in code at HEAD)

1. **Rowmap regrow trap.** `collect_row_metas_delta` rebuilds only when the live row count falls below the base count (`sessions.rs:3016`), and emits only row ids above the base high-water mark (`:3044`). An erase followed by enough appends to regrow the count therefore extends the old base, keeping the erased rows' entries. The installed chain gets no `Coverage::Complete` check after a build (`sessions.rs:2034-2040`). Two failures follow:
   - Read leak: after `--allow-reingest` re-creates the session row, `session_view` and the message view serve the old erased rows out of the map. `session_scan_rows_resident` is gated only by version (`:1756` already flags this).
   - Silent skip: the stale per-session watermark in the map makes the freshness gate report a re-ingest-allowed session `Fresh`, so it never re-imports.
2. **Cursor trap and cursor provenance.** `usable_sync_cursor` accepts a cursor whenever `row_count <= probe.row_count` and the oldest three rows match (`main.rs:4689-4691`). `persist_sync_cursor` combines the planner's map (`sync_oracle_snapshot`, unvalidated at that point, `sessions.rs:2071`) with a later store probe (`main.rs:4715-4730`), so stamping the store's current epoch at persist time would bless a pre-erase map.
3. **Un-gated readers of the resident map.** Beyond the four chain-install paths, several readers use the map with no check at all:
   - `open_cached_rowmap` (status, `sessions.rs:2120`);
   - the resident fast path in `trailing_rowmap_oracle` (`:2084`);
   - `session_id_for_message` ("definitive at any map version", `:1836-1847`);
   - `message_metas_by_rowids` (search hydration, `:3340-3366`).
   Serve refreshes its map every 30 s (`main.rs:1223`).
4. **Local copies of content, per host.**
   - Rowmap segments project `search_text` (`ROW_META_COLUMNS`, `sessions.rs:1981-1989`).
   - Build temporaries are swept separately from `.rmm` purges (`:2341`, `:2356`).
   - On Windows an unlink of a mapped segment fails and is only logged (`:2285-2293`).
   - Remote stores also keep an on-disk index cache (`<cache>/<store_key>/indices/<uuid>/`, `substrate.rs:4290-4296`, `:4959-4966`) holding FTS postings and IVF vectors. It is pruned best-effort only when no manifest references the uuid (`prune_index_cache`, `:2986-3008`).
5. **Store-resident SQL exports.** `pond_sql` exports are written to `<store>/exports/<uuid>.<ext>` (`substrate.rs:2192-2230`, `transport.rs:1553`) and served back via `pond-sql-export://` (`transport.rs:1715`). Nothing prunes them.
6. **Copy and archive restore bypass the ingest chokepoint.**
   - Store-to-store copy appends source scans directly (`sessions.rs:787-866`), and scans the source wholesale when `session_ids.len() == source_sessions` (`:858`).
   - `.pond` restore merges `dataset.scan()` unfiltered (`:563-575`).
   - `.pond` export writes fresh datasets and copies no config (`:452-557`).
7. **Index enumeration must come from the manifest, not the intents.**
   - `rebuild_index` returns early when an intent's trigger does not fire (`substrate.rs:3936`).
   - The IVF intent is absent when embeddings are off (`sessions.rs:5543`), and orphans match no intent.
   - `__`-prefixed names are Lance system indexes, which `index_findings` skips (`sessions.rs:5483`).
   - Only `rebuild_index` restamps `pond.fts.stems` (`substrate.rs:3951`).
8. **Tags and cleanup.**
   - pond calls `cleanup_old_versions(older_than, false, false)` (`substrate.rs:2848`, `:3463`).
   - Lance never deletes a tagged version (`cleanup.rs:586-597`).
   - `CleanupPolicy.before_version` exists as a public field (`cleanup.rs:1349`, `:1372`). The builder has no setter, so pond sets the field directly.
   - Unverified files become eligible after 7 days, evaluated only when a cleanup runs (`cleanup.rs:348`, `:698-721`).
9. **Config commits.**
   - `UpdateConfig` rebases over `Append`, `Delete`, `Rewrite` and `CreateIndex`, and conflicts only with another `UpdateConfig` on an overlapping key (`conflict_resolver.rs:1609-1650`).
   - An Append also rebases over config commits, so no config write can fence an in-flight append.
   - Same-name index replacements do conflict with each other (`conflict_resolver.rs:727-737`).
10. **Delete provenance is not returned.** `DeleteResult` carries only the new dataset and a count (`lance delete.rs:25-31`), and a later predicate scan cannot rediscover rows already deleted. Each fragment's `DeletionFile` records a `read_version` (`lance-table fragment.rs:421-428`), which is the durable handle this design uses (D5).
11. **Single-fragment rewrite is a house pattern.** `reencode_fragment` builds one `CompactionTask` per fragment and commits through `commit_compaction`, bypassing planner and veto (`substrate.rs:3521-3544`). pond's embed path creates deletion vectors of its own (`merge_update` with `WhenMatched::UpdateAll`, `substrate.rs:2306-2317`), so "every fragment with a deletion file" is not a safe proxy for "every fragment the erase touched".
12. **Handle staleness.** A remote handle may serve a manifest up to the staleness window (a few seconds) old (`substrate.rs:1996-2002`, `:2139`). `upsert_session_batch` probes `sessions` per batch through that handle (`sessions.rs:1204-1222`), and may embed inline before committing.
13. **Local self-heal** walks back to the newest fully readable version and quarantines everything newer (`heal_local_dataset`, `substrate.rs:5169`). It has no notion of an erase boundary, so it can roll a table back across an erase commit.
14. **The freshness seam is one function** (`adapter/mod.rs:263-317`). Some adapters skip it entirely on an empty oracle (reported at `agy.rs:220`, `jsonl.rs:234`, `pi_coding_agent.rs:1227`).
15. **Lineage recording is incomplete and sometimes misbranded.** See the per-adapter table in D6.
16. **Measured costs.**
    - Whole-store `pond optimize --rebuild`: 10m13s (`results.md:274`, `:374`).
    - Cold rowmap build: about 141 s (`results.md:402`).
    - Per-table version-log walk: 2-18 s on S3, growing with history (CLAUDE.md).
    - `config.rs:272` documents `cleanup_older_than` as default `1d`, but the code default is `1h` (`substrate.rs:922-927`). The comment gets fixed in PR 2a.

## Design

### Pipeline at a glance

```
pond erase <id>... [--include-continuations] [--allow-reingest] [--dry-run] [--yes] [--format json]

plan     closure over recorded edges (one sessions projection scan); edge classes (D6);
         per-table row counts; tag + branch probe; index list; cost; lineage-gap warnings
         -> printed; confirm on a TTY, or --yes; op id = plan digest (D8)
phase 1  holds the per-host sync flock (waits); resumable from the op record
  1  intent    sessions config: pond.erased.<id> (portable) for each closure id,
               pond.erase.op.<op> (local record: ids, pre = HEAD version per table,
               reingest flag, state=erasing)
               messages config: pond.erased.<id> (replica), pond.erase.epoch = <op>:inflight
  2  delete    messages, parts, sessions   (session_id / id IN closure)
  3  verify    zero rows on a freshly checked-out HEAD (bypassing the staleness window)
  4  settle    wait the settle interval (remote only), then delete + verify again
  5  epoch     messages config: pond.erase.epoch = <fresh uuid>  (no longer inflight)
  6  rewrite   every HEAD fragment whose deletion file has read_version >= pre[table];
               one commit per table; step 7 runs even if step 6 fails part-way
  7  indexes   each non-__ index in each manifest: rebuild_index with its intent
               (vector intent included regardless of the embedding switch) when the
               trigger fires, otherwise drop
  8  assert    no HEAD fragment has a deletion file with read_version >= pre; every
               non-__ index was built from a snapshot at or after step 6's commit;
               zero rows. Rows found -> set the epoch back to <op>:inflight, then
               back to step 2 (at most 3 loops, then fail with state=erasing)
  9  local     drop the resident map; purge rowmap chain + temps; remove sync cursor;
               prune the index disk cache; failures recorded as pending local purge
  10 exports   count <store>/exports/* (one listing); report it, delete nothing (decision 10)
  11 publish   op -> state=purge_pending, anchor = HEAD version per table;
               --allow-reingest ops remove their pond.erased.<id> keys now (release);
               this host records first-seen time for the op
phase 2  on any upgraded host that has itself seen the op pending for >= the window
  1  verify    zero rows for enforced ids; rows -> op state=resurrected, report,
               continue (no phase-1 work inside sync)
  2  cleanup   cleanup_with_policy(before_version = anchor, delete_unverified = false,
               error_if_tagged_old_versions = true) per table
  3  assert    checkout_version(anchor - 1) no longer resolves on each table
  4  watch     op -> state=orphan_watch (eligible_at = phase-1 end + 7 days); the first
               cleanup per table after eligible_at retires the op record
```

### D1. Storage: portable intent, local operation record

Two kinds of config keys, separated because they travel differently (astra 1, 9; fable 15).

**Portable intent** is one key per erased session, `pond.erased.<session_id>` = `{"at":"2026-09-29T14:12:03Z","root":"<named id>"}`. It is written on BOTH `sessions` and `messages`, and enforcement reads the union of the two. It holds no versions and no state, only the fact that this id is erased here. It is the only kind that travels through copy and archives.

**The local operation record** is one key per erase operation, `pond.erase.op.<op_id>` on `sessions` only. It holds `ids`, `pre` (HEAD version per table read at step 1), `reingest`, `state` (`erasing` | `purge_pending` | `resurrected` | `orphan_watch`), `anchor`, and the phase-1 end time. It never travels: its versions are meaningful only in this store's manifest chains. It is removed when the orphan watch completes.

**The erase epoch** is `pond.erase.epoch` on `messages` only (D2).

Why config keys and not a fourth table (unchanged reasoning):
- **Zero added requests on every path.** Every writer already holds fresh `sessions` and `messages` handles, the search path holds `messages`, and the config arrives in memory with the manifest.
- **Disjoint keys never conflict** with data commits or with each other (F9). The shared keys - the epoch, an op's state transitions, the index names - do conflict, and D5 gives their retry rules.
- **No 5.1 change and no new `Table` variant.**

Why replicate the intent on `messages`:
- **Self-heal (F13).** A heal that rolls one table back across step 1 cannot drop the denylist unless both tables roll back across it in the same crash (D5).
- **Read suppression.** Search reads only `messages`, so it can suppress enforced ids at zero cost (D3).

Growth, an estimate to be measured (V7):
- Intent is about 70 bytes per key per table. Op records are transient.
- Every manifest carries its config, and `messages` is rewritten on every sync commit. Tens of erasures are noise. Around a thousand is where it starts to cost.
- The escape is a sidecar table - option C shape from `2609-01-scoped-access-tenants-design.md`. It is not built now.

Rejected:
- **A fourth `erasures` table.** It puts a dataset open on the sync path and costs a 5.1 amendment, for a scale single-operator erasure will not reach.
- **One JSON-array key.** Every erase would conflict with every other.
- **Tombstone session rows.** They mutate canonical rows and synthesize sessions (`model-no-synthesis`).
- **Putting versions and state inside the portable key.** That was revision 1, and both reviews showed it carries source-store anchors into foreign stores.

### D2. Erase epoch (cache and cursor invalidation)

`pond.erase.epoch` on `messages` takes two forms:
- `<op>:inflight` from step 1 to step 5;
- a fresh uuid from step 5 onward.

Readers fail closed: only a value that parses as a uuid is settled, and any other value reads as in flight. A later binary can therefore change the in-flight form without an older binary ever admitting a stale signal under it.

**Validity rule.** A derived signal - a rowmap segment chain, a sync cursor, a resident map - is valid only if two things hold:
- the epoch it recorded equals the store's current epoch;
- the current epoch is not in-flight.

Otherwise the signal is discarded and rebuilt from stored data. It is never extended.

**The in-flight form closes the crash gap** (astra 6, fable 5). Revision 1 argued that a map built between the delete and the bump is post-delete, and astra is right that this is false: a build in that gap can delta-extend an old base (F1). Under the in-flight form no signal built during an erase is ever valid, and a crash mid-erase leaves the epoch in-flight. Until someone re-runs `pond erase` (which `pond status` names):
- every host falls back to store scans for reads;
- every host falls back to an empty oracle (full re-read) for sync.
That is correct and slow, which is the right incentive.

**Snapshot provenance** (astra 6, fable 5d):
- A chain records the epoch from the same `Dataset` snapshot its scan read - the handle's `config()` at build time - never from a later re-read.
- `persist_sync_cursor` takes `erase_epoch` from the map it persists (`sync_oracle_snapshot`'s recorded epoch), never from the store.
- The rowmap segment header gains a fixed-width epoch field (16-byte uuid plus an in-flight flag; the header is a Pod, `rowmap.rs:56-67`) with a MAGIC bump to `PONDRMM6`, which costs one full rebuild per host on upgrade.
- `RowMetaSet::open` requires every segment in a chain to carry the same epoch.

**Readers** (F3). Revision 1 listed four; the complete set is two chokepoints plus one explicit gate:
- **Install chokepoint.** `ensure_rowmap_inner`, `extend_rowmap_coordinated`, `load_rowmap_if_present`, `trailing_rowmap_oracle` and `open_cached_rowmap` all apply the validity rule before using or extending a chain. On mismatch they call `purge_rowmaps` plus `sweep_orphan_temps` under the build lock.
- **Resident chokepoint.** Whenever a process refreshes its `messages` handle and sees an epoch different from the resident map's, it drops the map (`rowmap.store(None)`) before serving anything else. `session_scan_rows_resident`, `session_id_for_message` and `message_metas_by_rowids` then fall back to store reads until a valid map is installed. This one check replaces per-reader gates and removes the "definitive at any map version" assumption in `session_id_for_message`.
- **Cursor gate.** `usable_sync_cursor` applies the validity rule (absent on both sides counts as equal) and removes the file on mismatch.

The `Coverage::Complete` equality in `rowmap_matches_store` stays an equality, because every chain that predates an erase is discarded. Update the comments at `sessions.rs:2938-2942` and `:1756`.

Rejected:
- **Folding the epoch into `store_key`.** It strands old chains, erased text included.
- **A denylist-membership check on chains.** It misses `--allow-reingest`.
- **Walking manifest history.** That is the `versions()` storm.

### D3. Enforcement points

The ingest chokepoint is authoritative for writes. Copy and restore are separate chokepoints because they bypass ingest (F6). The oracle is an optimization. "Enforced" means the id has a `pond.erased.<id>` key on either `sessions` or `messages`.

| # | Point | Behavior |
|---|---|---|
| 1 | `upsert_session_batch` (sync, `/v1/ingest`, serve sync) | Before the in-batch dedup, drop every substream whose id is enforced, using the batch's `sessions` handle. Each dropped row gets `OutcomeStatus::Denylisted` (not an error; the existing matches at `handlers.rs:611-612`, `:655-657` and `sessions.rs:4314` gain the arm). The ingest summary gets an additive `denylisted` count, and the wire per-row status gains `denylisted`. After each commit the writer checks the post-commit manifest's config for free (the committed dataset is in hand). If it just wrote rows of an id that became enforced during the batch (the F12 race), it logs a loud warning and adds `wrote_erased` to the summary. The erase's re-sweep (steps 4 and 8) or phase 2 removes or reports those rows. |
| 2 | Copy plan (`plan_incremental_from`) | Skip every id enforced on the destination or the source, in all three tables, and count them as `withheld`. `source_sessions` stays the unfiltered source count. The one-commit wholesale scan stays on under a denylist: it applies whenever a table's `append` covers every carried session (`append.len() == source_sessions - withheld`), and it scans with `NOT IN <erased union>` (the predicate archive restore uses), which also drops orphan rows a source that crashed mid-erase holds without their session row (fable 6). Disabling it would cost every copy of an erased-from store ~22 chunk commits per table on 11k sessions, permanently, since copy spreads intent. |
| 3 | Copy closing verify (`verify_stores`, also `--verify-only`) | Filter enforced ids out of the source key stream AND out of `source_rows` (so `ensure_source_not_empty` counts the same thing), and report them as `withheld: N rows of M erased sessions`, exit 0. Separately count destination rows belonging to enforced ids (`erased_present`), which a duplicate count cannot catch (astra 14). It is reported loudly with the repair command (`pond erase <ids> --storage-path <to>`), exit 0 - it is not a copy gap. |
| 4 | Intent travel (copy and archive) | Carry only `pond.erased.*` keys. Merge rule: a key absent on the destination is inserted (on both tables); a key present on the destination is kept unchanged. Import never removes or lifts a destination key, and never carries op records or the epoch. Only a local `pond erase --allow-reingest` lifts a key. |
| 5 | `.pond` archive | Export writes intent keys into the archive's `sessions.lance` and `messages.lance` config. Restore merges them per rule 4, then filters each table's scan by `session_id NOT IN` (or `id NOT IN`) over the destination's enforced set. |
| 6 | Freshness gate | `SkipOracle::is_erased` (default false). `sync_skip_oracle` wraps whichever oracle it chose with the store's enforced set. `is_session_fresh` skips erased ids undecoded, and `SyncPlan.erased` counts them. Adapters that bypass the gate on an empty oracle (F14) still re-decode erased sessions until a watermark exists; point 1 drops them either way. |
| 7 | Read suppression | `find_session` (get-session, get-message, resume, `restore_lineage`) treats an enforced id as absent. Search hydration drops hits whose session is enforced. In steady state an enforced id has no rows, so this costs nothing and removes nothing. It only acts during the anomaly windows in D5, and it also covers the U1 uncertainty about whether index search honors deletion vectors. It is a safety net over already-deleted content, not a caller filter, so `search-prefilter-pushdown` is unaffected; the spec gets a one-line note (amendments). `pond_sql` is NOT filtered: it is the raw escape hatch, and an anomaly window is visible there until repaired. That is stated in D9. |

Rejected: enforcement inside the substrate write chokepoint. The substrate knows nothing of sessions (3.1).

### D4. `--allow-reingest` (the 7.6 migration primitive) and #146

`pond erase <id> --allow-reingest` runs the identical purge. Its keys are fully enforced from step 1 until publish (step 11), so no writer can repopulate the session while the purge runs (astra 4). At step 11:
- the op removes the ids' `pond.erased.*` keys from both tables;
- phase 2 skips the zero-verify for those ids, since new rows are then legitimate.

Rows admitted after the release are re-ingest by definition. Because the epoch was already republished at step 5, no stale watermark (F1, F2) can gate the session `Fresh`.

**Lift-only.** `--allow-reingest` on an already-erased id first counts rows in all three tables (astra 10):
- rows present (imported intent onto an old copy, a racing write, an old binary) -> a full purge op in reingest mode;
- no rows -> remove the keys.
The end state is always the same: no stored rows, not denylisted.

This is the 7.6 migration primitive: correcting a stored label, or re-deriving under a fixed adapter, becomes "erase with re-ingest, then sync". Its hazard is `session-durable-copy`. The confirmation text states both halves: re-ingest recovers nothing once the source is gone, and it silently drops anything the source no longer holds. It prints `pond copy --from @ --to <file>.pond` as the recovery floor - a migration may keep such a snapshot, a compliance erase should not. `--dry-run` shows how many messages would have to be re-supplied.

**#146 (locked decision 6).** `pond erase --allow-reingest <id>` is the only remedy. Bare erase is explicitly not a remedy: it permanently destroys a healthy session. There is no second deletion exception. The two holes, stated in the #146 remedy text:
- **No remedy once the source is gone.** The operator can leave the duplicates, and `pond copy`'s closing verify keeps failing (exit 6) on every copy of that store. The alternative is losing the session. Nothing here repairs that store.
- **Silent loss unless the source is complete.** Any message pond kept that the source has since dropped - the devin `/revert` class, a partial rotation - is gone after re-ingest, unreported. The remedy is safe only while the source still holds everything pond stored.

The named future escalation is an exact-content dedup repair: removing one of two rows that agree on key and content loses nothing and needs no source. It would be a second, named deletion exception, so it is built only if a duplicated store with a dead source is ever observed. It is not designed here.

### D5. Two-phase purge, crash recovery, concurrency, and reporting

**Why two phases.** A reader pins the manifest it opened for the length of its request (3.4). Phase 1 only adds files and manifests, so it is safe at any moment. Phase 2 deletes what phase 1 superseded, and must wait out the window.

**Crash recovery (astra 2).** Every phase-1 step is resumable from the op record. Step 6 targets HEAD fragments whose deletion file has `read_version >= pre[table]` (F10):
- A fragment rewritten or removed since - by this erase, by another host's compaction, or by a fully deleted fragment being dropped - has had its deletions materialized, so it no longer qualifies.
- Deletion vectors from embed `merge_update`s after `pre` are included too. That is harmless over-inclusion, bounded to the erase's own window.
- The rule needs one integer per table, survives any crash, and is idempotent.
- Verification item V5 must confirm that a delete rebased under OCC writes `read_version >= pre`.
- If the op record itself was lost (self-heal, below), step 6 falls back to every deletion-bearing fragment. That is correct and possibly expensive, and the dry run reports the count.

**How phase 2 is scheduled** (astra 11, 12; fable 3):
- Eligibility is measured on one clock. A host may run phase 2 for an op only after it has itself observed the op in `purge_pending` for at least the retention window (`cleanup_older_than`, floor 1 h). The first-seen time is recorded in the per-host state dir (`erase-seen-<store_key>.json`). Because the op's last phase-1 commit precedes any host's first sight of it, elapsed time since that commit is at least the local elapsed time. No cross-host clock comparison and no skew assumption.
- The erasing host records first-seen at step 11. Another host that first sees the op later waits a full window from then, which errs toward waiting.
- `pond sync` (including serve's in-process sync) checks op records after import. When one is eligible it runs phase 2, bypassing the `any_new_rows` and `cleanup_interval` gates for the anchored tables. `pond optimize` always checks. A re-run of `pond erase` completes whatever is eligible.
- Phase 2 is deletion-only in every branch. When its zero-verify finds rows, it does NOT re-run phase 1 inside sync (that would put a ~10 min rebuild in a cron sync, against "pond never self-initiates a large rewrite", spec 7.8). It sets `state=resurrected`, still runs the anchored cleanup (reclaiming what it can), and reports: `N erased sessions have rows again (a racing write, a self-heal rollback, or a host running pond < X); run pond erase <ids>`. The rerun is a new op with fresh anchors and a fresh window.
- Cost is one anchored walk per table, 2-18 s each on S3 (F16), per op.

**Why version anchors.** Phase 2 selects by `before_version = anchor` (F8). The anchor is a local version of this store's own tables, never imported (D1), so `checkout_version(anchor - 1)` failing after cleanup is meaningful: that version existed here.

**Concurrent writers** (astra 5, fable 10). The layers, in order:
1. The intent is committed before the delete (step 1).
2. Any batch whose `sessions` probe sees it drops the session (D3.1). Probes can see a manifest up to the staleness window old, and batches can embed for minutes before committing (F12).
3. No config write can fence an append (F9), so the design re-sweeps rather than blocks:
   - step 3 verifies against a freshly checked-out HEAD;
   - step 4 waits a settle interval, then deletes and verifies again. The interval is twice the remote staleness window plus 30 s by default, zero on local stores, and exists to cover probe staleness plus ordinary commit latency;
   - step 8 re-verifies after the ~10-minute index rebuild. Rows found there set the epoch back to in-flight and loop back to step 2, since they may already be inside the rebuilt indexes;
   - phase 2 re-verifies after the window and reports.
4. Writers detect their own late writes for free (D3.1), and reads suppress enforced ids throughout (D3.7).

A per-batch forced manifest refresh before commit was rejected: it adds a request to every sync batch on the S3 write path (memory `no-added-s3-write-latency`), and would still leave a commit-latency window.

Resulting visibility windows:
- A racing upgraded writer's rows are readable only through `pond_sql`, and only until the next sweep (at most the settle interval, or the rebuild, after its commit).
- Self-heal or a pre-erase binary: until the operator reruns erase. Both are reported by phase 2, by `pond status`, and after a heal by the opening process.

**Self-heal** (astra 3, fable 7). Local stores only.
- Intent survives any single-table rollback, because it is replicated on `sessions` and `messages` and read as a union (D1).
- A heal that rolls back `messages` also purges this host's rowmap chains and cursor for the store. A rollback can restore an older epoch value that a pre-erase chain would match again.
- After any heal, the opening process runs the zero-verify for enforced ids: one indexed count per table, read-only, so MCP-safe under `mcp-read-only-heal-exception`. It extends heal's notice with `pond erase <ids>` when rows came back.
- An op record lost with `sessions` is covered by the fallback in "Crash recovery". The rerun finds the ids through their intent keys.
- Residual: a crash that tears step-1 commits on both tables at once rolls back the intent entirely. Nothing was deleted yet at that point, so the erase simply never happened.

**Contention between erases** (astra 16). Disjoint intent keys never conflict. The epoch key, an op's state transitions, and same-name index replacements do (F9). All of these re-read and retry through `retry_lance`. Two hosts finishing the same phase-2 op race on its state key, and the loser re-reads and finds it done. Concurrent erases from two hosts are safe but may redo the index rebuild, so the output recommends one erase at a time.

**Unverified orphans (locked decision 3; astra 15).**
- Files no manifest ever referenced (a writer that died after writing data but before committing) become *eligible* for deletion 7 days after they were written. Lance deletes them only during a later cleanup (F8).
- The op therefore stays in `orphan_watch` until a cleanup has run on each table after `eligible_at`. `pond sync` forces that one cleanup when due.
- A writer that dies later creates a later eligibility date. The report states "eligible from", not "reclaimed by".

**Tags and branches.** The plan refuses when any table has a tag or a Lance branch, naming them: tags pin versions cleanup never removes (F8), and branch retention is unaudited (V8). Phase 2 passes `error_if_tagged_old_versions=true` as a second fence.

**Output** (plain-text form; `--format json` carries the same facts):

```
erase: 3 sessions - 1 named, 2 spawned
  8f1c...  claude-code              412 messages   1,907 parts
  a03e...  claude-code/Explore       88 messages     301 parts
  c77d...  claude-code/general       40 messages     122 parts
lineage: claude-code does not record resume/fork continuations; copies of this history in other sessions are not in this closure
denylist: 3 sessions now blocked from sync, ingest, copy and restore into this store
purge: rows deleted and verified gone; 14 fragments rewritten; 11 indexes rebuilt (10m02s)
purge: superseded versions still hold these bytes; the first pond sync or pond optimize at least 1h after each host first sees this erase completes the purge (this host: after 15:13 UTC)
purge: files from writes that never committed become eligible for removal from 2026-10-06 14:13 UTC and are removed by the first cleanup after that
local: this host's caches purged; other hosts purge theirs on their next sync or serve refresh
exports: 4 export artifacts exist under exports/ and may retain erased content; erase does not touch them
scope: this store only - other stores, .pond snapshots, JSONL and pond_sql exports, hosts that never touch this store again, and bucket versioning are not touched
```

`pond status` shows the following when present:
- op records by state, with the local eligibility time;
- in-flight or `erasing` ops, with the rerun command;
- `resurrected` ops;
- pending local purge failures.

### D6. Cascade semantics (locked decision 5) over incomplete lineage

**Policy (locked).** Spawn edges cascade automatically. A closure that reaches a continuation or fork is refused with the full closure printed, unless `--include-continuations` is passed. There is never a partial erase.

**What pond can see.** Both reviews showed the recorded edges are incomplete and sometimes wrong (fable 1, 2, 11, 18, 19; astra 8). Every row of the table below was checked against the code except where marked:

| Adapter (root brand) | Spawn edges recorded | Continuation edges recorded | Spawn-branded child is always a real spawn |
|---|---|---|---|
| claude-code | Yes: subagent files, including `/fork` subagents (`claude-code/fork`) (`claude_code.rs:625-660`) | No: a top-level session's parent is always None, so resume, `--fork-session` and compaction are unlinked (`claude_code.rs:670`) | Yes |
| openclaw | Partial: file and archive era none (`openclaw.rs:1070-1093`); DB v1 resolves the parent key to its current route, possibly a later generation (`:940-944`, `:1893-1899`); DB v2 only for single-generation keys (`:919-927`, `:1994-2013`) | Compaction successors and checkpoint forks, via the header path (`:1852-1881`) | Yes for subagent-kind children. Spawns whose child key is Main-kind carry the root brand and classify as continuation (conservative). |
| hermes | Yes | Yes: every parent is recorded verbatim; only the relation and brand degrade when the parent row is missing | **No.** A missing parent row or `end_reason` degrades to Spawn and brands `hermes/subagent` (`hermes.rs:516-525`, `:553-567`). The cron override brands a cron branch or compaction child `hermes/cron` (`:569-570`; occurrence unverified, U3). |
| oh-my-pi | Yes (path-linked) | Partial: `/tan` forks are path-linked (as spawns); header `parentSession` forks are not | **No.** A `/tan` fork is branded `oh-my-pi/subagent` (`oh_my_pi.rs:324-326`). Nested spawns share the brand, so they classify as continuation (conservative, `:547-560`). |
| devin | Yes (`devin/subagent`) | No: forks record no parent (`devin.rs:1541`) | Yes |
| agy | Yes (`agy/subagent`) | Forks, with cut point, root brand (`agy.rs:740-769`) | Yes |
| opencode | Yes, in both eras (only the task tool sets `parentID`). DB-era children are branded `opencode/<agent>`; tree-era children keep the root brand, so they classify as continuation (conservative) (`opencode.rs:891-899`) | No: forks record no parent | Yes |
| pi-coding-agent | No: pi persists no spawned children | Partial: v4 and SQLite forks via `parentSessionId` (`pi_coding_agent.rs:831-834`); a v3 header's path-valued `parentSession` stays unresolved (`:719`) | Never classifies as spawn |
| nanoclaw | Yes: `subagents/` sidecars, and opencode-provider task children | No: top-level transcripts never record a parent | Yes: the parent and the `/subagent` brand come from one `subagent_descriptor` match (`nanoclaw.rs:357-383`), and `opencode::reattribute` brands only parented children |
| grok-build | Yes: subagents via the parent-side meta (`grok_build.rs:246-262`) | Yes: fork and worktree sessions carry `parent_session_id` under the root brand | **No.** A `subagent_resume` child is branded `grok-build/subagent`, but its recorded parent is the subagent it resumes (`:269-276`) |
| codex-cli, letta-code, claude-ai-export, claude-desktop-app | None (parent always None: `codex_cli.rs:507`, `letta_code.rs:201`, `:819`, `claude_ai_export.rs:290`, `claude_desktop_app.rs:554`) | None | n/a |

Session rows are immutable after first write, so a missing edge stays missing for already-stored data. A later linking pass in an adapter cannot repair stored rows.

**(a) Classify from recorded edges, and declare completeness per adapter.**
- Each adapter declares a static `LineageFidelity { spawns: Complete | Partial | None, continuations: Complete | Partial | None, spawn_brand_exact: bool }`. This is a generic seam contract: the values live in each adapter's module and `adapter/mod.rs` only defines the type (seam rule).
- `spawns` and `continuations` state which edges the adapter records, whatever brand the child carries. How a recorded edge classifies is `spawn_brand_exact`'s concern alone.
- The plan looks up the fidelity of every closure member's root brand, and of the named session's.
- Whenever a relevant kind is `Partial` or `None`, the plan prints a `lineage:` warning naming the gap: spawned sessions or continuations may exist outside the closure. That is the honest limit of erase, and it goes into the D9 scope statement.

**(b) Close the dangerous direction.** An edge classifies as spawn only if both hold:
- the brand rule: the child's `source_agent` carries a `/`-subpath and differs from the parent's;
- the child's adapter declares `spawn_brand_exact = true`.

Every other edge is treated as a continuation, and so is refused without the flag. This includes spawn-branded edges from hermes and oh-my-pi, and any brand no registered adapter claims (for example, sessions ingested over `/v1/ingest` from an unknown client).

Justification: the cost of the conservative direction is one extra flag on a refusal that prints the full closure; the cost of the other direction is silently deleting a live conversation. This refines decision 5's classification. It changes observable behavior - real hermes and oh-my-pi subagents stop cascading automatically - and the user locked it with that cost accepted (decision 8).

Dispute (fable 2, claude-code part): a `claude-code/fork` session is a subagent transcript written under the parent's `subagents/` directory (`claude_code.rs:604-615`, `:625-660`). It is spawned, subordinate work that replays the parent's context. Cascading it is correct, and desirable for compliance. It is not a continuation misbranded as a spawn.

**(c) Lineage gaps: limitations, not prerequisites.**
- claude-code, devin, codex-cli, letta-code, claude-ai-export and claude-desktop-app continuations are not stated by the sources in a form pond records. Linking them would be synthesis (`model-no-synthesis`), so this is permanent and documented.
- openclaw spawn edges are a real, fixable gap for new data, and a permanent one for stored rows. The fix for stored rows would be an adapter-owned closure hook that derives edges from stored `options.openclaw` (`spawnedBy`, generation windows). That is a new seam surface and is deferred.
- **Locked (decision 9):** erase ships with these gaps documented. The table above is a documented limitation, the plan prints `lineage:` warnings, and the openclaw hook is not a prerequisite.

**Mechanics** (unchanged):
- One `sessions` projection scan, then BFS from each named id with a visited set. A cycle is possible: openclaw `resolve_route` may self-loop (U4).
- The full closure, with classes and row counts, is printed before any write. A TTY prompts through dialoguer. Off a TTY, `--yes` is required.
- Ancestors are never touched.
- On HTTP, a refusal is `validation_failed` with the closure in `details`.

**Rejected:**
- **The spec-literal cascade over every edge.** It silently erases live successors under `--yes`.
- **Reading adapter relation tags in core.** That is source policy in the seam, and the tags carry the same misclassifications as the brands (hermes records `relation=spawn` for the degraded case).

### D7. Per-erase cost and batching

| Step | Cost on the real store |
|---|---|
| Plan | One `sessions` projection scan, indexed counts, one tag and branch listing per table |
| Delete, verify, settle, re-sweep | 3 commits (+ retries), 6 indexed counts, the settle interval (remote) |
| Rewrite | 1 commit per table; bytes = the targeted fragments (at most 256 MiB each) |
| Index rebuild | Every non-`__` index on all three tables, dominated by FTS and IVF; the whole-store rebuild measured 10m13s (F16) |
| Every other host | One full rowmap rebuild at its next sync or serve refresh, about 141 s cold, once per epoch; plus an index-cache prune |
| Phase 2 | One anchored walk per table (2-18 s each on S3), plus one orphan-watch cleanup per table after 7 days |

Batching is explicit: `pond erase <id> <id> ...` (a JSON `ids` array on HTTP) shares one op. The openclaw reconciliation stays detection-only and prints one batched `pond erase` command in place of the "not yet implemented" line (`main.rs:4985`).

Rejected:
- **Deferring the rewrite and rebuild into `pond optimize`.** HEAD keeps referencing erased bytes indefinitely, and sync must not run the rebuild.
- **Rebuilding only FTS and IVF.** The BTree pages hold the erased ids themselves.

**Vector index with embeddings off** (fable 17). Erase uses the vector intent regardless of the switch. An IVF index at or above the activation threshold is rebuilt from the stored vectors - no model needed. Below the threshold it is dropped: flat scan serves those queries, and an enabled instance recreates the index at threshold. That is the one sanctioned departure from "a disabled instance leaves the vector index alone", and it is stated in the 8.8 amendment.

### D8. Surfaces

**CLI.** `pond erase <SESSION_ID>... [--include-continuations] [--allow-reingest] [--dry-run] [--yes] [--format json]`, plus `pond erase --list` (intent keys and op records).
- `--dry-run` prints the plan and writes nothing.
- Erase waits for the per-host sync flock, naming the holder. `/v1/ingest`, copy, restore and optimize do not take it, and on filesystems without flock it degrades to unlocked (`syncstate.rs:164-175`). The lock only reduces local interleaving; D5's sweeps provide correctness.
- Exit codes are distinct per class, following `pond resume`: ok, not found, refused (tags or branches, continuations, no `--yes` off a TTY), storage or conflict failure.
- A never-stored, non-enforced id returns `not_found` and writes nothing (locked decision 7).
- An enforced id is handled per astra 10:
  - rows present in any table -> a new op, full phase 1;
  - an op record exists -> resume or complete it;
  - otherwise (intent imported, no rows ever here) -> idempotent success.

**Operation identity** (astra 13). The op id is the plan digest: sorted closure ids, flags, and the epoch at planning time. It is persisted in the op record at step 1.
- CLI: a rerun finds the op through any of its ids.
- HTTP: a retry presenting a token whose op record exists resumes that op, even though the closure has since been deleted and the epoch moved. A token with no op record is re-validated against a fresh plan, and a mismatch returns `conflict` (re-plan).

**HTTP (locked decision 4).** `POST /v1/x/erase`, with the `require_allowed_host` route layer as on `/v1/x/sql` (`transport.rs:205-211`).
- The `dry_run: true` call returns the plan plus `plan_token`. `dry_run: false` with the token executes.
- Execution is synchronous and runs for minutes, so clients need a long timeout. The handler waits for the flock when an in-serve sync cycle holds it.
- Whether a client disconnect cancels the handler is unverified (V6). Either way, an interrupted op resumes from its record on retry or on a CLI rerun.
- It is never exposed on MCP.

**Error rows (7.4).**
- `not_found` (404): neither stored nor enforced.
- `validation_failed` (400): shape errors, and refusals (tags or branches, continuations without the flag), with `details`.
- `conflict` (409): OCC exhausted, or a stale token with no op record.
- `storage_unavailable`: unchanged.
- No new code.

### D9. Purge scope and local caches

**Local cache purge** (astra 7; fable 4, 23), run at step 9 on the erasing host and on every other host at its first epoch mismatch:
- drop the resident map, then `purge_rowmaps` plus `sweep_orphan_temps` under the build lock;
- remove the sync cursor;
- `prune_index_cache`: replaced index uuids are no longer referenced, so their `indices/<uuid>/` dirs go.
Any unlink that fails - a Windows mapped segment held by another process, a permission error - is recorded in a per-host `erase-local-pending-<store_key>.json`, retried on every open, and reported by `pond status` until it succeeds. Other hosts purge at their next sync or serve refresh (every 30 s in serve).

**Store-resident exports (fable 8; decision 10).** `<store>/exports/*` may hold `pond_sql` query results that contain erased content. They are out of scope for deletion:
- A blanket delete cannot be targeted at erased content without scanning every file.
- It would create a second deletion surface, against the single-exception principle.

Instead, step 10 counts the artifacts with one listing. When any exist, the output says `N export artifacts exist under exports/ and may retain erased content`, and the JSON document carries the count, so the operator decides what to do with them. Exports accumulate forever today whether or not anything is erased; the general pruning mechanism is a separate follow-up.

**Erase purges** rows, rewritten fragments, replaced index files, superseded manifests (phase 2), unverified orphans once eligible and cleaned, and this host's local caches immediately.

It does not reach:
- other stores, and `.pond` snapshots or JSONL exports taken before the erase;
- `pond_sql` export artifacts under `<store>/exports/` (decision 10: counted and reported, never deleted);
- hosts that never touch the store again (their rowmap and index-cache files keep erased content);
- in-process Lance caches until eviction or restart;
- object-store bucket versioning, replication, and backups;
- quotations of erased content inside other sessions (a parent's tool result, a continuation not included);
- sessions linked by lineage the adapter never recorded (D6 table);
- the session ids themselves, which stay in the denylist by design, and in Lance transaction files and heal-quarantined `.manifest.corrupt` files until those are cleaned (V8);
- `pond_sql`, which can show rows of an erased session during an anomaly window (D3.7, D5) until the sweep or rerun.

## Review resolution

Accepted and folded unless marked. Section references are to this revision.

| Finding | Where |
|---|---|
| astra 1, 9; fable 15: portable intent vs local purge authority; merge rule | D1, D3 row 4 |
| astra 2: durable rewrite provenance for crash-resume | D5 "Crash recovery" (`pre` + `read_version`; V5) |
| astra 3; fable 7: self-heal rolls back erase and denylist | D1 (replication), D5 "Self-heal". Fable's "in the same manifest" is partially disputed: the denylist (step 1) and the deletes are separate commits on separate tables. The substance is accepted. |
| astra 4: allow-reingest opens the gate early | D4 (enforced until step 11) |
| astra 5; fable 10: TOCTOU and the staleness window; reads during races | D5 "Concurrent writers", D3 rows 1 and 7 |
| astra 6; fable 5: epoch provenance, crash gap, unlisted readers | D2 |
| astra 7; fable 4, 23: index disk cache, temps, Windows | D9 "Local cache purge" |
| astra 8; fable 1, 2, 11, 18, 19: edge classification unreliable | D6 (fidelity table, exactness rule; decisions 8 and 9). Fable 2's claude-code `/fork` part is disputed, with evidence in D6. |
| astra 10: already-purged no-op branch | D8 (enforced-id handling), D4 lift-only |
| astra 11: `due_at` clock skew | D5 (first-seen eligibility on the local clock) |
| astra 12; fable 3: phase 2 reruns phase 1 inside sync | D5 (`resurrected` state; operator rerun) |
| astra 13: HTTP retry token identity | D8 "Operation identity" |
| astra 14; fable 20: verify of destination erased rows; `source_rows` | D3 row 3 |
| astra 15: 7 days is eligibility, not a deadline | D5 "Unverified orphans", output |
| astra 16: contention overclaim | D1, D5 "Contention" |
| fable 6: copy wholesale shortcut | D3 row 2. Clarification: revision 1's "falls away naturally" held only if `source_sessions` stays unfiltered, which it now states. The shortcut stays, gated on `source_sessions - withheld` and scanning with `NOT IN` the erased union, so orphan rows never travel. |
| fable 8: store-resident SQL exports | D9 (decision 10: reported, not deleted; pruning is a follow-up) |
| fable 9, 12; astra passed-checks: `__` indexes, `dataset_version` wording | Pipeline steps 7-8 (snapshot version, `__` excluded) |
| fable 13: `pond.fts.stems` restamp | Step 7 uses `rebuild_index`, which restamps |
| fable 14: "blob purge" dropped from spec text | Restored in the 5.4 amendment |
| fable 16: ZoneMap ordering | Step 6 note (the indices step always runs, as in the `reencode_fragments` precedent, `sessions.rs:3760-3796`) |
| fable 17: vector index with embeddings off | D7 |
| fable 21: empty-oracle bypass | D3 row 6 |
| fable 22: flock coverage; HTTP inside serve | D8 |
| fable 24: cleanup cost range | D5, D7 (2-18 s) |
| fable 25, 26: spec splice gaps; brand convention; restore asymmetry | Spec amendments |
| fable 27: `cleanup_older_than` doc drift | F16; comment fixed in PR 2a |

## Spec amendments (draft wording)

**3.2 `lance-chokepoints-write`** - append: "A delete primitive also lives here, with one caller: `pond erase` (`session-append-only-exception`, Section 5.4)."

**3.3 `lance-append-only`** - append: "Whole-session erasure is the single exception (`session-append-only-exception`, Section 5.4)."

**3.3 `local-store-self-heal`** - append: "Heal preserves erase intent: the erased-key denylist is replicated on two tables and read as their union, so rolling one table back cannot lift it. A heal that rolls `messages` back also discards this host's derived caches for the store. After any heal, the opening process checks erased keys for restored rows and names `pond erase` when it finds some. The check is read-only, so the MCP carve-out is unchanged."

**3.4** - append: "`pond erase` never cleans inside the window. Its completing cleanup removes every version below the erase's own final commit, selected by version rather than time. A host runs it only once that host has itself observed the finished erase for at least the window, measured on its own clock, so clock skew between hosts cannot shorten the window. Files no manifest ever referenced become eligible under Lance's seven-day unverified-file guard, which pond never overrides: doing so would delete a concurrent writer's in-flight files."

**4.5** - append to the Session prose: "A spawned child (a sub-agent) carries a `/`-subpath `source_agent` its parent's lacks; continuations and forks do not. Adapters declare how completely and how exactly they record each edge kind (`session-append-only-exception` relies on the declaration)."

**5.1** - no change. Intent, op records and the epoch are dataset-level config keys (the `pond.fts.stems` precedent), not a fourth dataset.

**5.4 `session-movement-complete`** - append: "An erase invalidates every derived 'already ingested' signal - the freshness cursor, the resident row map, and every host's cached copy - through a store-level erase epoch. The epoch is marked in-flight for the erase's duration and republished only after the delete is verified durable. A signal built under another epoch, or during an in-flight one, is discarded and rebuilt from stored data, never extended. Why: a row count that regrew past an erase no longer proves the rows it covers exist, and a watermark that survives an erase can gate a re-allowed session fresh forever."

**5.4 `session-append-only-exception`** - replace with:

> Erasing whole sessions is the single exception to pond's append-only design and the only deletion pond performs. Append-only governs events within a session: a stored Message or Part is never mutated, reordered, or removed. This rule retires entire session objects.
>
> It is operator-only - `pond erase` on the CLI and `POST /v1/x/erase` over HTTP, never on the MCP surface. It cascades over recorded spawn edges that the child's adapter declares exact. It refuses a closure that reaches any other edge - a fork, branch, compaction successor, or unverifiable spawn - unless the operator explicitly includes it, because such a session may be live and usually replays the history being erased. It never erases part of a closure. The full closure is shown before anything is written, together with any lineage the adapters do not record.
>
> Erasure is a true byte purge: a delete, then fragment rewrite, index rebuild, version-history cleanup and blob purge. Rows are deleted, re-swept, and verified gone. Every fragment that held them is rewritten, and every index is rebuilt or dropped, so the current version references nothing erased. Once the retention window (Section 3.4) has passed, a version-anchored cleanup removes every older version. Erase reports when that becomes due, when Lance's unverified-file window opens, and what lies outside its scope: other stores, snapshots, `pond_sql` export artifacts (whose count it reports), hosts that never touch the store again, and bucket versioning.
>
> Before any delete, each erased key enters a denylist that every write path consults - ingest, copy, archive restore - and that the get and search surfaces suppress. A later sync from a still-present source therefore cannot resurrect it; the denylist is the subtraction term that keeps `session-movement-complete` sound (storage is the union of reachable sources minus erased keys). The denylist travels with `pond copy` and `.pond` archives as intent only: an import never weakens a destination's denylist and never carries another store's purge state.
>
> `--allow-reingest` purges identically, keeps the key enforced until the purge is verified, then lifts it, so the next sync re-derives the session from whatever the source still holds. It is the deliberate-migration primitive of Section 7.6, and it recovers nothing a source has lost.
>
> A pond binary predating this rule does not consult the denylist. Every host must run an erase-aware binary before a store's first erase, and resurrected rows are detected and reported, not prevented.
>
> Why: right-to-erasure compliance needs real deletion with no resurrection and no retained bytes. Making it the single named operator-only exception leaves every other guarantee (`session-durable-copy`, `adapter-integrity-additive-sync`) intact and unambiguous.

**6.2 `adapter-lineage-complete-restore`** - append: "Erasure is its deletion mirror only over spawn edges: restore carries every child, while erase refuses a closure containing a continuation unless explicitly included (`session-append-only-exception`)."

**6.6 `adapter-integrity-additive-sync`** - last sentence becomes: "Changing or removing an already-stored row is a deliberate migration, never a side effect of re-ingest; `pond erase --allow-reingest` (Section 5.4) is that migration for a whole session."

**7.4** - extend three rows' "When":
- `not_found`: "... or a `pond erase` target that is neither stored nor denylisted".
- `validation_failed`: "... an erase refused by a precondition (a tag or branch; a continuation or unverifiable spawn in the closure without the explicit flag)".
- `conflict`: "... or an erase `plan_token` with no recorded operation that no longer matches the store (re-plan)".

**7.5** - add operation 6: "**`pond_erase`** (`POST /v1/x/erase`, unstable) - the single sanctioned deletion (`session-append-only-exception`, 5.4). A `dry_run` call returns the closure, per-table row counts, and a `plan_token`. Execution requires the token, which also identifies the operation, so a retry resumes rather than re-plans. It is served behind the same Host allowlist as operation 5 and never on MCP. It lives under `/v1/x/` until its response shape settles." Also change the closing sentence "its one fenced HTTP exposure, operation 5" to "its fenced HTTP exposure, operation 5 (operation 6 shares the fence)".

**7.6** - replace from "Correcting a stored label is a deliberate migration" through "deletes it rather than repairing it." with: "Correcting a stored label is a deliberate migration (`adapter-integrity-additive-sync`), and `pond erase --allow-reingest` is that path: it purges the session and then lifts its key, so the next sync re-derives it under the current adapter. A bare `pond erase` denylists the key, and so deletes rather than repairs. Either form recovers only what a still-present source holds (`session-durable-copy`)."

**7.8** - replace the `pond erase` bullet:

> `pond erase <session-id>...` - the single sanctioned deletion (`session-append-only-exception`, Section 5.4). It plans the closure (exact spawn descendants; anything else only with `--include-continuations`) and prints it with row counts and any unrecorded-lineage warnings. It then asks for confirmation on a TTY, or requires `--yes` off one. It refuses a store whose tables carry tags or branches. Phase 1 records intent, deletes, re-sweeps, verifies, rewrites, and rebuilds every index. The completing cleanup runs on the first `pond sync` or `pond optimize` after the window. A rerun resumes an interrupted erase, and repairs one whose rows came back. `--dry-run` writes nothing. `--allow-reingest` purges and then lifts the denylist (Section 7.6). `--list` prints the denylist and every pending operation. It waits for the per-host sync lock. It is available on the CLI and over HTTP (7.5), never on MCP.

Additions to other 7.8 bullets:
- `pond sync`: "sources of erased sessions are skipped undecoded and counted as `erased`; a run completes an erase purge that has become eligible on this host, and reports erased sessions whose rows came back".
- `pond copy`: "the denylists of both ends withhold erased sessions (reported; the closing verify excludes them and separately reports erased rows present on the destination); the source's intent is merged into the destination without weakening it".
- `pond status`: "pending and interrupted erase operations, resurrected erased sessions, and local cache purges still pending".
- `pond serve`: "`/v1/x/erase` shares the `/v1/x/sql` Host gate".

**8.1 `search-prefilter-pushdown`** - append: "Dropping hits of erased sessions (`session-append-only-exception`) is not a caller filter and is exempt: it acts only on rows an erase already deleted, during a bounded anomaly window."

**8.8** - append: "The one exception is `pond erase`: it rebuilds an existing vector index from stored vectors, or drops it below the activation threshold, whatever the switch, because the index holds erased vectors."

## Test plan

Placement rule:
- Store-level and single-module tests are unit tests in `mod tests` of the file under test.
- `tests/integration/erase.rs` (plus `#[path = "integration/erase.rs"] mod erase;` in `tests/integration.rs`) holds only cross-module flows.
- `shared-memory://pond-test-<unique>/` is used only where 2+ `Store`s share bytes; everything else uses `TempDir`.

Unit (`packages/pond/src/...`):
- `substrate.rs`:
  - the delete primitive;
  - step-6 targeting by `read_version >= pre`, including after a simulated crash between the delete and the rewrite, and alongside unrelated `merge_update` deletion vectors;
  - the fallback targeting when there is no op record;
  - index rebuild over manifest indexes: `__` system indexes untouched, IVF below threshold dropped, IVF with embeddings off rebuilt, orphan dropped, the stems restamp present;
  - anchored cleanup: `anchor - 1` unresolvable;
  - the tag and branch refusal.
- `sessions.rs`:
  - intent and op codecs; the merge rule (never weakens, never imports op records);
  - union enforcement across the two tables;
  - `upsert_session_batch` produces `Denylisted`, and the post-commit `wrote_erased` detection fires;
  - copy plan exclusion with the shortcut disabled;
  - the regrow trap: no erased row id is installed after an erase plus regrowth;
  - an in-flight epoch invalidates every chain;
  - the resident chokepoint drops the map on an epoch change, so `session_id_for_message` and hydration never answer from a stale map;
  - the heal of `messages` purges chains;
  - read suppression in `find_session` and hydration;
  - closure BFS with a cycle; edge classification against a fake fidelity table (exact vs inexact brand, unknown brand -> continuation).
- `adapter/*.rs`: each adapter's `LineageFidelity` values asserted in its own module. hermes and oh-my-pi assert `spawn_brand_exact = false`, with a comment citing the misbranding path.
- `adapter/mod.rs`: `SyncPlan.erased`; `is_session_fresh` skips erased ids.
- `syncstate.rs` / `main.rs`: the cursor epoch comes from the map, not the store; a stale or in-flight epoch rejects the cursor; the first-seen record round-trips.
- `handlers.rs`: op-id digest; a token with an existing op record resumes; a stale token without one conflicts; `restore_lineage` of an enforced id returns `NotFound`.

Integration (`tests/integration/erase.rs`):
- sync -> erase -> sync over the claude-code fixture: nothing resurrected, dry-run shows `erased`.
- `--allow-reingest` -> sync with a pre-seeded regrown cursor and chain: the session re-imports fully, and no writer repopulates it before the release.
- lift-only on an id whose rows were re-imported by a copy: a full purge runs.
- Copy both directions, with `withheld`, `erased_present` and `--verify-only`.
- Archive round trips: an old archive into the erased store is filtered; intent carries into a fresh store; destination intent is never weakened.
- Crash injection after every phase-1 commit (steps 1, 2 per table, 5, 6, 7, 11): a rerun completes and the assertions pass.
- Two-phase: not eligible before the window on each host's first-seen clock; completes after; the orphan watch retires the op.
- Multi-`Store` race (shared-memory, unique authority): B probes before step 1 and commits after step 3 -> the settle re-sweep removes the rows. B commits after step 8 -> phase 2 marks `resurrected` and runs no rewrite.
- Self-heal on a local store: roll `messages` back across the delete -> rows detected and reported, intent kept, chains purged.
- Cascade: an openclaw successor refuses without the flag and erases the full closure with it. A hermes degraded-spawn child refuses (decision 8). A store with export artifacts reports their count, and the artifacts are untouched afterwards (decision 10). Lineage warnings are printed for claude-code and openclaw closures.

HTTP (`tests/integration/transport_http.rs`): Host gate 403 and allow; dry_run -> token -> execute; interrupted execute plus retry with the same token resumes; stale token 409; unknown id 404; refusal 400 with details. `transport_mcp.rs`: erase is never listed.

**Open verification items** (from both reviews' unverified sections; each must be settled by a test or a source citation before PR 2b merges):
- **V1 (fable U1) - SETTLED 2026-09-29, honored.** Every index query ANDs a per-manifest-version live-row-id deletion mask into the index exec (lance v12.0.0 `rust/lance/src/index/prefilter.rs:77-106`, `:149-238`, stable-row-id branch `:331-337`), applied at IVF partition scan (`lance-index/src/vector/flat/index.rs:185-204`) and BM25 posting level (`lance-index/src/scalar/inverted/index/search.rs:594`) with or without `fast_search`; flat tail paths read through deletion vectors (`filtered_read.rs:754-758`). Measured on pond's exact shape (`_rowid` + score, no take, `prefilter(true)`, stable row ids, FTS + IVF_SQ + unindexed tail): 0 deleted row ids leaked across 32 combinations after delete and again after compaction. D3 row 7 suppression is therefore a safety net, not load-bearing. Residual pond-side facts, both already handled: `resolve_rowid_hits` (`sessions.rs:2387-2421`) does no liveness check, so a process on a pre-delete dataset version serves erased rows until it re-checks out (covered by the resident chokepoint, D2); index files keep erased postings/codes, only masked (covered by step 6 `replace=true`). PR 2b adds a pond-side regression test: delete, run `fts_search` and `vector_search`, assert the erased keys are absent.
- **V2 (fable U2) - SETTLED 2026-09-29, honored for picked fragments only.** The rewrite reads through deletion vectors (`optimize.rs:2428-2437`, `:1989-2027`; binary copy refused for fragments with a deletion file, `:603`, `:693-695`), and with stable row ids index files are not remapped (`:2396`). But a fragment is picked alone only when `deletion_percentage() > materialize_deletions_threshold` (default 0.1, `:342`, `:904-919`), and pond's `compaction_options` (`substrate.rs:3486-3494`) keeps the default. Measured: 76 deleted rows present after delete, 10 after compaction at 0.1 (a 3.9%-deleted fragment kept its deletion file), 0 at 0.0. So erase must never rely on the planner: the per-fragment `CompactionTask` + `commit_compaction` rewrite (F11, the `reencode_fragment` pattern) bypasses both the threshold and pond's task-veto hook (`substrate.rs:3376-3384`), and that is load-bearing, not a convenience. Routine optimize never purges.
- **V3 (fable U3):** whether hermes ever emits a cron-branded branch or compaction child. Moot under decision 8's rule, needed for the fidelity doc.
- **V4 (fable U4):** openclaw `resolve_route` self-loops. The visited set is required either way.
- **V5:** a delete rebased under OCC writes `DeletionFile.read_version >= pre`.
- **V6 (astra):** whether an HTTP client disconnect cancels the handler in the pinned axum/hyper. Resume is required either way.
- **V7 (astra):** measured intent-key size and manifest growth on `messages`, and the cost of status's indexed counts.
- **V8 (astra):** Lance branches, a tag created during cleanup, and `.manifest.corrupt` quarantine artifacts - what each retains, and whether the refusal and cleanup cover them.
- **V9 (astra):** Windows unlink behavior for mapped rowmap segments and index-cache files under the pending-local-purge retry.
- **V10:** `CreateIndex(replace)` against a concurrent `optimize_indices` on another host. Step 8's snapshot-version assertion is the guard either way.

## Phasing

Two PRs, each landing as its own release (release-plz picks the versions):

1. **PR 2a - invalidation, enforcement and fidelity plumbing, no verb.**
   - Commit 1: substrate delete and purge primitives plus unit tests.
   - Commit 2: intent and op codecs, epoch with the in-flight form, rowmap MAGIC bump, cursor field, the resident and install chokepoints, heal extensions, enforcement points 1-7, plus unit tests.
   - Commit 3: `SkipOracle::is_erased`, `SyncPlan.erased`, and `LineageFidelity` declarations per adapter.
   - Commit 4: the `cleanup_older_than` comment fix.
   Behavior is unchanged apart from the one-time rowmap rebuild. Shipping it first puts erase-aware binaries on the fleet before any erase exists. It is a storage-path change, so the perf gate applies.
   - Moved to 2b: the post-heal zero-verify (D5 "Self-heal"). Its only remedy is `pond erase <ids>`, which a 2a binary does not have, and 2a has no other path that deletes rows; read suppression (D3.7) already hides them. A warning must name a working fix, so the check ships with the verb. 2a keeps the other heal extensions: the chain purge and a one-shot cursor discard.
2. **PR 2b - `pond erase`.**
   - Commit 1: plan, closure and classification, phase 1 and 2 orchestration, op records, first-seen eligibility, status reporting, and the post-heal zero-verify moved from 2a.
   - Also in Commit 1: the `pond erase <ids>` suggestion in the copy closing verify's `erased_present` line and in the ingest `wrote_erased` warning. 2a states only the counts and ids, because a 2a binary has no verb to name.
   - Step 8's loop back to step 2 re-publishes `<op>:inflight` before re-deleting. Otherwise a chain built under the settled epoch, which may map the racing rows, stays admitted after the re-delete removes them (the regrow trap).
   - Commit 2: CLI verb, JSON document and exit codes; the reconciliation line prints the batched command.
   - Commit 3: `/v1/x/erase`, wire types, error rows.
   - Commit 4: integration and transport tests, plus V1-V10 settled.
   - Commit 5: spec amendments; the #146 remedy text.
   The `## Release note` carries the `[release-note]` upgrade block: every host must run 2a or later before the store's first erase, and a pre-erase binary syncing a still-present source resurrects erased sessions.

## Perf gate

Both PRs change storage paths.
- Run `moon run repo:bench -- --only perf` at the pre-change commit, and append the old-side row to `docs/benchmarks/baseline.jsonl` **before any 2a binary writes to the real store**. The MAGIC bump rewrites every chain, and a real erase consolidates index segments (results.md:306), so the old-side baseline is unrecoverable afterwards.
- Run the new-side row after.
- The hot-path additions should be flat: in-memory config reads, a set lookup per batch, a post-commit config read of an in-hand dataset, and the manifest growth from replicated intent (V7). Bracket any move beyond the noise floor with a targeted `hyperfine` A/B.
- The gate has no erase probe. Measure erase separately on an s5cmd scratch copy of the real store: per-step wall time for one-session and ten-session batches, the settle re-sweep, the phase-2 walk, and the other-host rowmap rebuild plus index-cache prune. Append the numbers to `docs/benchmarks/results.md` with host and commit, and state that the gate does not cover them.

## Follow-ups (outside this design)

- **Export pruning or a TTL for `<store>/exports/`** (its own small issue). `pond_sql` export artifacts accumulate forever today, whether or not anything is erased (F5). A general age-based pruning mechanism belongs to the export feature, not to erase (decision 10).
- **An openclaw closure hook** that derives spawn edges from stored `options.openclaw` (`spawnedBy`, generation windows). This is optional hardening of D6's documented gap, not a prerequisite (decision 9).
- **An exact-content dedup repair for #146**, built only if a duplicated store whose source is dead is ever observed (decision 6).

## Open decisions

None. All ten decisions are locked. V1 and V2 are settled (2026-09-29); V3-V10 remain open in the test plan.
