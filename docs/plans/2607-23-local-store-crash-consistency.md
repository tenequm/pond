# Local-store crash consistency: fsync-on-put prevention + self-healing open (fix #110)

Implements [tenequm/pond#110](https://github.com/tenequm/pond/issues/110). This document is self-contained: a fresh session should be able to implement from it without re-deriving the investigation. All upstream claims below were verified against the pinned crate sources (lance 8.0.0, lance-table 8.0.0, object_store 0.13.2 per `Cargo.lock`); pond citations are against `main` at the time of writing (2026-07-23) - line numbers may drift, symbol names will not.

## 1. The bug

A hard host stop (VM reset, power loss, kernel panic) during a commit against a **local** store leaves a zero-byte (or truncated) manifest at the head of `_versions/` in one or more tables. Every subsequent open of that table fails permanently with:

```
Error: failed to open table messages
Caused by:
    0: LanceError(IO): Generic memory error: Invalid range 0..0 for object of size 0 bytes, .../lance-8.0.0/src/dataset.rs:655:26
```

Nothing self-heals, the error names no fix, and the first thing a human tries (rolling back `latest_version_hint.json`) does not work. Remote (S3) stores are immune: PUT visibility is atomic and durability is server-side at ack, so a killed writer leaves no object rather than an empty one. This is a local-store-only failure mode - which is the default `pond init` configuration.

## 2. Verified root cause (upstream, pinned sources)

All confirmed by reading the exact pinned crates; citations are `crate/path:line`.

1. **Local commits route through `ConditionalPutCommitHandler`.** `commit_handler_from_url` builds it for `file` schemes on non-Windows (`lance-table-8.0.0/src/io/commit.rs:1070`, handler at `:1078`). Its `commit` serializes the manifest to memory and writes it via `object_store.inner.put_opts(&path, ..., PutMode::Create)` (`commit.rs:1506`, mode at `:1510`).
2. **`LocalFileSystem` never fsyncs.** `put_opts` (`object_store-0.13.2/src/local.rs:330`) writes the payload to a staging file `<dest>#<n>` (`:352`), then for `PutMode::Create` does `std::fs::hard_link(&staging, &path)` (`:372`); `PutMode::Overwrite` uses `std::fs::rename` (`:367`). Multipart - how Lance writes data files - is the same hole: `put_multipart_opts` stages (`local.rs:404-419`) and `LocalUpload::complete` just `std::fs::rename`s (`local.rs:882-901`). `copy`/`copy_if_not_exists` use `hard_link` (`:561-592`). There is **no `sync_all`/`sync_data`/fsync anywhere in local.rs** - not on the file, not on the parent directory. The link/rename makes the *name* durable via directory metadata while the *bytes* live only in the page cache; a hard stop persists the name and drops the bytes.
3. **Latest-version resolution has no fallback.** For local stores Lance lists `_versions/` and takes the highest parsed version with zero validity checks (`current_manifest_local`, `commit.rs:655-711`). `latest_version_hint.json` is not even consulted on local FS (the hint path is S3-Express-only; `write_version_hint` no-ops unless `uses_version_hint`, `commit.rs:327-343`). `load_manifest` then does `read_last_block` on the winner (`lance-8.0.0/src/dataset.rs:649`), and `read_metadata_offset` errors on anything under 16 bytes (`lance-io-8.0.0/src/utils.rs:128-133`). A zero-byte head manifest is therefore fatal and permanent even though N-1 is fully intact.
4. **No upstream relief.** Lance 8.0.0 has no fsync knob (no env, no storage option), and the v9.1.0-beta.8 tree is byte-identical on this path (still `put_opts(PutMode::Create)`, still object_store 0.13.2, no fsync anywhere). Neither `lance-format/lance` nor `apache/arrow-rs` has a tracking issue.

Two additional verified facts the design depends on:

5. **Version-pinned opens bypass the poisoned head entirely.** `DatasetBuilder::from_uri(...).with_version(N-1).load()` resolves the manifest path *deterministically* - `resolve_version_location` -> `default_resolve_version` (`commit.rs:959`) does a single `head` on the constructed V2 path, falling back to the V1 path, and **never lists the directory or reads manifest N** (`builder.rs:239`, `:640`, `:816`, `:840-842`; the VersionNotFound->latest fallback at `builder.rs:845-856` is not triggered when N-1 exists). So pond can probe older versions safely while the head is corrupt.
6. **A `.manifest.corrupt` rename is invisible to Lance.** `ManifestNamingScheme::detect_scheme` requires the filename to end with `.manifest` (`commit.rs:138-153`); `current_manifest_local` skips anything `detect_scheme` rejects. Renaming `<v>.manifest` -> `<v>.manifest.corrupt` is an atomic same-directory rename that removes the file from Lance's world without deleting it. `cleanup_old_versions` also ignores it (manifest discovery goes through the same scheme detection).

Manifest naming (needed by the heal walk): **V2 is the default for datasets pond creates** (`WriteParams::default()` sets `enable_v2_manifest_paths: true`, `lance-8.0.0/src/dataset/write.rs:410`; scheme picked at `dataset/write/commit.rs:335-341`). V2 filenames are `{u64::MAX - version:020}.manifest` (20 digits + `.manifest`); V1 is `{version}.manifest`; detached versions are `d`-prefixed and must be skipped. `parse_version` splits on the first `.` and parses; V2 maps back via `u64::MAX - n` (`commit.rs:114-123`).

## 3. Why this is fixed in pond (and why repair is lossless)

The aborted commit is unrecoverable by construction (its bytes never reached disk), but for pond that is fine: source histories (JSONL etc.) are the source of truth and the next `pond sync` reconstructs whatever the aborted commit held. A rollback-to-last-readable-version repair is therefore **always lossless** for pond. The design goal per the issue discussion: **no `pond doctor` command** - prevention so the corrupt state (almost) never exists, plus silent self-heal on open for whatever prevention cannot cover.

## 4. Design: three layers

### Layer 1 - Prevention: fsync-on-put object-store wrapper (local stores, unix only)

A `WrappingObjectStore` that makes every write durable before it returns, applied only when `config::is_local(location)`.

- New `mod durability` in `packages/pond/src/substrate.rs`, sibling of the existing `pub mod index_cache` (`substrate.rs:3483`) and modeled on `IndexDiskCache` (`substrate.rs:3516`) - same shape: a factory struct implementing `lance_io::object_store::WrappingObjectStore` whose `wrap()` returns the intercepting store.
- Intercepts, and after the inner call succeeds fsyncs the destination file (`File::open` + `sync_all`) and its parent directory (open the dir, `sync_all`; unix allows fsync on an O_RDONLY dir fd):
  - `put` / `put_opts` (covers manifests via the commit handler, `.txn` files, `latest_version_hint.json`);
  - `put_multipart` / `put_multipart_opts` - wrap the returned `Box<dyn MultipartUpload>`; on `complete()` fsync the final path + parent dir (covers data files and index files); `put_part`/`abort` pass through;
  - `copy` / `copy_if_not_exists` and `rename` / `rename_if_not_exists` - fsync the destination + parent dir.
  - Everything else delegates.
- Path mapping: the wrapper receives `object_store::path::Path`; convert with `lance_io::local::to_local_path` (lance-io is already a direct dependency; it is the same helper `current_manifest_local` uses). Fsync failures should be hard errors (a durability layer that silently no-ops is worse than none) - but tolerate `NotFound` on the parent-dir open defensively.
- Gate: `#[cfg(unix)]`. Windows routes commits through `RenameCommitHandler` and has different dir-sync semantics; it keeps current behavior and relies on Layer 2.
- Wiring: extend `index_store_wrapper` (`substrate.rs:3793-3815`) - rename it `store_wrapper` since it is no longer index-only - to push the fsync wrapper when `config::is_local(location)`. Two more call sites must carry the wrapper:
  - the **create path** in `open_or_create_via_ns`: `Dataset::write_into_namespace`'s `write_params.store_params` (`substrate.rs:3876-3888`) currently passes `storage_options_accessor` only - add `object_store_wrapper` there;
  - verify no other `DatasetBuilder`/`Dataset::write` path in substrate constructs store params without the wrapper (grep `ObjectStoreParams`).
  - Out of scope by decision: `Handle::export_write` (`substrate.rs:1965-1975`) and the `pond storage check` probe write raw sidecar artifacts through their own `ObjectStore::from_uri_and_params`; exports are non-critical and not part of the table state - leave them unsynced.
- **Why a wrapper and not the issue's post-cycle sweep:** every artifact becomes durable *before Lance proceeds to the next step*, so ordering is automatically correct - data files and `.txn` are on disk before the manifest commit starts, and the manifest itself is synced before the commit returns. The "durable manifest referencing a zeroed data file" state becomes impossible to produce going forward. A post-cycle sweep would leave the whole cycle exposed and cannot fix ordering. The wrapper also stays inside the object-store seam Lance explicitly provides (consistent with the Lance-native, no-direct-FS rule) and covers `sync`, `optimize`, `copy`-to-local-dest, embed, and store creation through one seam.
- Cost: a few dozen fsyncs per sync cycle on local SSD, sub-millisecond each, against a 5-minute cadence. Remote stores: zero change, zero added round-trips.
- Residual window: between `hard_link`/`rename` publishing the name and our fsync - microseconds. Layer 2 covers it.

### Layer 2 - Self-heal on open (no new command)

Trigger: in `open_or_create_via_ns` (`substrate.rs:3817`), when `builder.load()` (`substrate.rs:3856-3859`) fails **and** the table location is local (`config::local_path` on the namespace-returned location URL). Because `messages` opens eagerly (`substrate.rs:1900-1911`) and `sessions`/`parts` open lazily through the same helper (`lazy_cached`, `substrate.rs:2835-2851`), every surface - CLI, serve, MCP - heals transparently on first touch.

Heal algorithm, `heal_local_dataset(table_fs_path, ...)` in substrate.rs:

1. List `_versions/`, parse `(version, filename)` scheme-aware: V2 (20-digit reversed) vs V1, skip `d`-prefixed detached manifests, skip anything not ending in `.manifest` (this automatically skips prior `.corrupt` quarantines and Lance's `.tmp_*` staging leftovers). Sort descending.
2. Walk from the head down to the newest **fully readable** version:
   - a manifest under 16 bytes is definitively unreadable (Lance's own minimum-footer check, `utils.rs:128-133`) - no probe needed;
   - otherwise probe: `DatasetBuilder::from_uri(table_uri).with_version(v)` plus the same `Session` and store params the real open uses, `.load()`, then **drain a real scan** projecting one narrow column - real data-page reads, which is what catches the zeroed-referenced-data-file case that manifest metadata hides (`pond status` reports rows from manifest metadata and lied in the incident; only a scan surfaced the damage). Pick the narrowest primitive column from the dataset schema (`id` exists on all three tables).
3. Quarantine every manifest above that version: atomic in-place rename to `<name>.manifest.corrupt`. **Never delete anything.** Leave orphaned zero-byte data/txn files alone - unreferenced files are inert.
4. Retry the normal open once. On success, emit one loud notice line naming: which files were quarantined, which version the table rolled back to, and that the next `pond sync` re-ingests the aborted commit from source histories. Emit via `tracing::warn!` and verify it actually reaches CLI stderr with the default subscriber config in `main.rs`; if warnings are not rendered by default, surface the notice through the CLI layer (the heal function should return a summary the caller can print) - a silent heal fails the "loud one-line notice" requirement.
5. If nothing is quarantinable (head manifest is >=16 bytes and probes clean, i.e. the failure is something else) or no version is readable at all: do not touch anything; return the original error enriched per Layer 3.

Bounds and safety:

- The walk is bounded by the number of versions; in practice 1-2 quarantines. Guard against pathological stores with a sane cap (e.g. stop after 32 candidate probes and fall through to the enriched error).
- Race-safety: a zero-byte manifest at a final path can only be a crashed writer's remnant - a live commit publishes name and bytes atomically via `hard_link` through the page cache, so readers on a running system never observe a partial file. If two pond processes heal concurrently, the rename loser gets `NotFound` -> treat as already-healed and proceed to the retry. No locks (OCC-only concurrency rule holds).
- Heal never runs for remote stores (gate is `is_local`), never deletes, and only renames files that are provably unreadable (or that a pinned-open + scan probe proved unreadable).

**Why the scan-verify step stays (decided):** Layer 1 makes the durable-manifest-with-zeroed-data-file state impossible *going forward on unix*, but heal exists precisely for stores damaged before this fix and for Windows. One sync cycle makes several commits seconds apart (append, index fold, compaction), so a pre-fix crash can zero manifest N *and* a data file referenced by N-1, both still in the writeback window. Manifest-only heal would roll back to N-1, declare success, and hand over a store that opens but dies on the next real read (`failed to fill whole buffer`, `lance-io-8.0.0/src/scheduler.rs:302`) with no recovery path. The scan runs **only inside heal**, i.e. only after an open already failed on a crashed store - zero cost on every normal action.

### Layer 3 - Error enrichment fallback

When heal cannot fix the store (no readable version, or the failure is not manifest-shaped), the returned error must name what was inspected and found instead of the raw `Invalid range 0..0` - per the errors-name-the-fix rule (model: the copy-verify pointer at `main.rs:3163`, the `sql.rs::enrich` layer at `sql.rs:1037`). Example shape: `table messages: newest manifest <file> is unreadable (interrupted commit during a hard host stop) and no older version passed verification; quarantined files are under _versions/*.corrupt; restore this store from a copy (pond copy) or re-run pond init`.

## 5. Spec and rules updates (required, part of this change)

1. **spec.md** (read `docs/spec.md` section 3, storage substrate, before editing): add a subsection on local-store durability and self-heal covering: the fsync-on-put rule and why it is ordering-correct; the heal-on-open algorithm and its lossless argument; the quarantine policy (rename-only, never delete); and the **MCP read-only carve-out** - this is a deliberate, documented exclusion from the "every pond MCP action is read-only" rule: self-heal may rename provably-unreadable store artifacts during an open performed by the MCP server, because it is substrate integrity restoration (quarantine-only, no user-data writes, no deletes), and the alternative is an MCP server that stays bricked until a CLI command runs. Write it as an explicit exception so it reads as intentional, not as drift.
2. **CLAUDE.md**: amend the "MCP surfaces are hard-enforced read-only" section with one line pointing at the spec carve-out, so a future agent does not "fix" heal as a rule violation.
3. **substrate.rs:3053-3057**: the comment "No pond-side FS sweep here (spec.md#concurrency: Lance-native maintenance only)" predates this design - update it to reference the durability wrapper/heal seam so it does not read as contradicting them.

## 6. Tests

Placement rule (CLAUDE.md): unit tests in `#[cfg(test)] mod tests` in the file they test; cross-module suites in `packages/pond/tests/integration/<name>.rs` wired via `#[path]` in `tests/integration.rs`.

Integration - extend `packages/pond/tests/integration/recovery.rs` (it already models exactly this: `Store::open_local(temp.path())`, fixture ingest via `ingest_adapter`, drop/reopen, byte-identical assertions; fixtures at `tests/fixtures/adapter/claude_code/projects`):

1. **Zero-byte head manifest heals**: build a store with 2+ versions (sync twice), record row count, truncate the newest manifest in `messages.lance/_versions/` to zero bytes (`std::fs::write(path, b"")`), reopen -> open succeeds, scan count equals the N-1 count, and a subsequent sync restores the full corpus (mirror `rm_and_resync_round_trips_to_identical_state`).
2. **Truncated (non-zero) head manifest heals**: same, but write 8 junk bytes.
3. **Two consecutive poisoned versions heal**: zero the top two manifests; heal walks down twice.
4. **Scan-verify catches zeroed data files**: arrange version N-1 to reference a data file F (ingest -> note the file under `data/`), zero both manifest N and F; heal must quarantine N *and* N-1 and land on N-2 (or fail with the enriched error if no readable version remains - assert whichever the constructed store yields, deterministically).
5. **Never-delete**: after each heal, assert quarantined files exist as `*.manifest.corrupt` with original byte content, and that no file was removed from `_versions/`.
6. **Non-manifest failures do not trigger quarantine**: corrupt something heal should not touch (or simulate an unrelated open failure) and assert `_versions/` is untouched and the error is the enriched passthrough.

Unit (substrate.rs `#[cfg(test)]`, model: `cleanup_due_gates_on_version_interval` at `substrate.rs:4674`):

- manifest filename parsing: V1, V2 (20-digit reversal), detached `d*` skipped, `.corrupt` and `.tmp_*` skipped;
- wrapper path mapping (object_store `Path` -> fs path) and that a `put` through the wrapper round-trips.

The fsync wrapper needs no dedicated durability test (you cannot assert power-loss semantics in-process); it is exercised functionally by every existing local-store test once wired, which is the real regression guard. Confirm `cargo test` stays green and reasonably fast - if the per-put fsyncs measurably slow the suite, that is acceptable evidence the wrapper is live, but do NOT gate it off in tests (tests must run the production path).

Manual validation (optional, matches the issue repro): build a small local store, `: > store/messages.lance/_versions/<newest>.manifest`, then `pond status` - must heal and print the notice instead of failing.

## 7. Implementation order

1. Layer 2 heal + Layer 3 enrichment + integration tests 1-6 (this is the user-visible fix and is independently shippable).
2. Layer 1 wrapper + wiring + unit tests.
3. spec.md + CLAUDE.md + comment updates.
4. `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test` - all green locally before any commit (validate-before-push rule).

Branch: `fix/local-store-crash-consistency` (this file lands on it first). Commits: Conventional Commits, `fix(...)` type - release-plz will derive a patch bump; never open a release PR or pick a version (see CLAUDE.md Releasing).

## 8. Upstream follow-ups (separate work, NOT this PR)

Contribution-first sequence (file issues with the repro evidence first; offer PRs after maintainer signal):

- **apache/arrow-rs (object_store)**: `LocalFileSystem` publishes names without syncing bytes (`put_opts` staging+hard_link, multipart staging+rename, no fsync in local.rs) - a crash persists the name and drops the data. Likely landing shape: opt-in durability (e.g. a `LocalFileSystem` builder flag), since unconditional fsync would regress every consumer's benchmarks.
- **lance-format/lance**: (a) an unreadable head manifest permanently poisons a dataset whose N-1 is intact - no fallback, no repair API, and the error does not name the file; (b) even with object_store durability opt-in available, Lance's local commit path would need to opt in. Note pond keeps its own layers regardless of upstream outcome: pond pins lance 8.0.0, upstream fixes land in v9+, and already-damaged and Windows stores need heal anyway.
