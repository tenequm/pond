# Changelog## [0.10.1](https://github.com/tenequm/pond/compare/v0.10.0...v0.10.1) - 2026-06-20

### <!-- 2 -->🐛 Bug Fixes
- **sync:** rebuild rowmap when the base version's manifest was reclaimed
- **schedule:** gate ScheduleEvery::secs/from_secs to Unix

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.10.0...v0.10.1

## [0.10.0](https://github.com/tenequm/pond/compare/v0.9.0...v0.10.0) - 2026-06-20

This release rebuilds the admin CLI, the retrieval model, and the sync/copy write path - and makes remote-S3 operation dramatically faster.

### <!-- 0 -->🛠 Breaking Changes
- **cli:** retire the overloaded "sources" - `[sources.*]`/`pond sources` -> `[adapters.*]`/`pond adapters`; config auto-migrates on `pond init`
- **cli:** `pond storage use` is switch-only (a pure pointer flip, copies nothing); data copy moves to the new top-level `pond copy`; bare `pond storage` removed (use `pond status`)
- **cli:** remove `pond export` / `pond import` - snapshot to a `.pond` archive or `.jsonl` stream and restore from one is now `pond copy --to <file>` / `pond copy --from <file>`
- **cli:** split maintenance out of sync - `pond sync` runs import+embed+index by default; the new `pond optimize` verb runs embed+index on demand, and `sync --no-optimize` defers to it
- **cli:** `pond sync` no longer discovers or auto-enables adapters - enabling is the explicit job of `pond adapters`/`pond init`, so a scheduled sync can never grow the adapter set
- **cli:** verb convergence + flag renames - `--config`->`--config-file`, init `--schedule`->`--every`, `sync --source-dir`->`--path`, sync stage `update-indexes`->`index`, `--format pretty`->`text`; `--storage-path`/`--config-file` are now root-global selectors
- **cli:** `pond copy` requires explicit endpoints, adds the `@` (configured store) and `local` keywords; self-verifying with an id-set completeness check (exit 0 SYNCED / exit 6 missing rows)
- **storage:** first-class `pond creds {add,list,delete}` for URL-scoped credential sets; `pond init` captures remote creds inline (masked prompt, never argv)
- **search:** drop server-side hybrid fusion for single-arm retrieval - `mode=vector` (default) or `mode=fts`, plus `--sort-by recency`
- **search:** vector index IVF_PQ -> IVF_SQ (drop the refine pass); FTS moves from character-ngram to a word `simple` tokenizer with English stemming
- **sync:** durable idempotent-replay sync/copy with a cheap messages-based S3 oracle and `sync --verify`; resident per-session `max_ts` watermark replaces the version-resolution oracle
- **tools:** redesign `pond_get`/`pond_sql_query` and unify the transcript renderer
- **copy:** append fast-path + per-table write plan (absent rows append, grown rows merge)

### <!-- 1 -->🎉 New Features
- **cli:** `pond skill` prints the bundled agent-onboarding SKILL.md, in lockstep with the binary
- **copy:** incremental store-to-store copy with no temp staging; streams the source scan straight into the destination
- **search:** resident per-message meta cache (mmap'd, LSM version-delta refresh) shared across pond processes
- **storage:** self-verifying migrate and `pond storage verify`

### <!-- 3 -->🚀 Performance
*Measured on the real ~2M-message S3 corpus (Hetzner nbg1); baseline = pre-optimization on this branch.*

Sync & status:
- per-session staleness oracle: **79s warm / 133s cold -> ~1s / ~4s** (messages-based key replaces the `versions()` per-manifest fetch storm)
- warm re-sync of the full corpus: **~928s -> ~25s** (append fast-path + the new oracle)
- `status -v`: **130s -> ~14s**; the stale-embedding count that runs in every default sync: **59.5s -> ~7s**

Copy:
- append fast-path vs merge-insert for absent rows: **5.47x faster** (13.8 min vs 75.7 min full-corpus; 62 vs 2,685 objects written)
- streaming store-to-store, no temp staging: **1.92x faster** (24.1s -> 12.6s, local 500-session set); unchanged-source re-copy **~90 ms**

Search:
- FTS arm latency **2043ms -> 2ms p50** (6667ms -> 125ms p95) and **~60 -> 3 object GETs/query** via the resident row-key map; per-query S3 bytes **-81%** (6.0 -> 1.16 MB)
- FTS index **1.14 GB -> 41 MB (28x)** and query RAM **2248 -> 379 MB (5.9x)** after the word-tokenizer switch; English Success@3 **31/111 -> 66/111 (2.1x)**
- FTS cold query (full corpus) **76s -> 27s p50** (148.9s -> 48.5s p95); cold server prewarm **175-442s -> ~81s**
- vector arm **~393 -> 0-1 object GETs/query** after IVF_PQ -> IVF_SQ and dropping the refine pass
- bounded server RAM lowers `sql` cold **18s -> 5.9s**
- resident row-key map: **281.7 MiB** for 2.1M messages, built in **~3.8s**, removing the per-query hydration scan (and a remote Lance decoder panic)

### <!-- 5 -->📚 Documentation
- **spec:** add the `session-movement-complete` completeness rule, the session-erasure exception, and micro-batch live-write
- migrate the docs site from mdBook to vocs (pond.locker); correct the search model to single-arm across spec/README/site; refresh SKILL.md for agent ergonomics

### <!-- 2 -->🐛 Bug Fixes
- **cli:** phantom embed backlog, progress-bar wrapping, and verify memory
- **sync:** restore the per-session staleness watermark from the row version
- **build:** gate the `RLIMIT_NOFILE` bump to Unix so the `x86_64-pc-windows-gnu` release binary builds

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.9.0...v0.10.0

## [0.9.0](https://github.com/tenequm/pond/compare/v0.8.1...v0.9.0) - 2026-06-12

### <!-- 0 -->🛠 Breaking Changes
- **init:** [**breaking**] redesign storage onboarding and add 5m sync cadence

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.8.1...v0.9.0
## [0.8.1](https://github.com/tenequm/pond/compare/v0.8.0...v0.8.1) - 2026-06-12

### <!-- 2 -->🐛 Bug Fixes
- **init:** offer the local default when a storage probe fails

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.8.0...v0.8.1
## [0.8.0](https://github.com/tenequm/pond/compare/v0.7.0...v0.8.0) - 2026-06-11

### <!-- 0 -->🛠 Breaking Changes
- **config:** [**breaking**] URL-scoped creds, storage URLs, introspection, and migrate

### <!-- 6 -->🧹 Chores
- **substrate:** add real-S3 concurrent multi-writer OCC benchmark

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.7.0...v0.8.0
## [0.7.0](https://github.com/tenequm/pond/compare/v0.6.0...v0.7.0) - 2026-06-11

### <!-- 0 -->🛠 Breaking Changes
- **mcp:** [**breaking**] minimize pond_search/pond_sql_query param surface

### <!-- 2 -->🐛 Bug Fixes
- **sql:** make pond_sql_query first-try-correct for agents

### <!-- 6 -->🧹 Chores
- lance-style release notes and point nix install at pond-nix

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.6.0...v0.7.0

## [0.6.0](https://github.com/tenequm/pond/compare/v0.5.2...v0.6.0) - 2026-06-10

### Added

- *(ingest)* stamp host provenance on inserted message rows ([#40](https://github.com/tenequm/pond/pull/40))
- *(search)* [**breaking**] per-message session fusion, raw-magnitude scoring, absence honesty

## [0.5.2](https://github.com/tenequm/pond/compare/v0.5.1...v0.5.2) - 2026-06-10

### Other

- *(maintenance)* veto absorb-heavy compaction tasks and derive byte-based fragment targets

## [0.5.1](https://github.com/tenequm/pond/compare/v0.5.0...v0.5.1) - 2026-06-10

### Added

- *(sql)* harden pond_sql_query and add error-guided recovery

## [0.5.0](https://github.com/tenequm/pond/compare/v0.4.0...v0.5.0) - 2026-06-05

### Added

- *(mcp)* [**breaking**] add pond_sql_query read-only SQL tool (table/json/ndjson/parquet) + pond sql CLI

## [0.4.0](https://github.com/tenequm/pond/compare/v0.3.2...v0.4.0) - 2026-06-05

### Other

- *(maintenance)* [**breaking**] gate compaction, drop unsafe vacuum, carve out [maintenance] config

## [0.3.2](https://github.com/tenequm/pond/compare/v0.3.1...v0.3.2) - 2026-06-04

### Added

- *(adapter)* add claude-desktop-app and claude-ai-export adapters

## [0.3.1](https://github.com/tenequm/pond/compare/v0.3.0...v0.3.1) - 2026-06-04

### Fixed

- *(adapter)* recognize nested workflow-subagent transcripts

## [0.3.0](https://github.com/tenequm/pond/compare/v0.2.8...v0.3.0) - 2026-06-03

### Added

- *(cli)* redesign sync/status output and gate sources behind enabled ([#26](https://github.com/tenequm/pond/pull/26))
- *(adapter)* add pi and opencode source adapters

### Fixed

- *(substrate)* handle wrapped namespace table-not-found errors
- *(adapter)* apply polish-review fixes across opencode, pi, seam, and writer
- *(adapter)* harden pi and opencode adapters per review

### Other

- *(substrate)* collapse namespace error-chain walker
- rename pi adapter to pi-coding-agent

## [0.2.8](https://github.com/tenequm/pond/compare/v0.2.7...v0.2.8) - 2026-06-03

### Added

- *(mcp)* enrich the tool surface for better agent discoverability
- *(docs)* add an mdBook documentation site
- *(release)* cargo-binstall metadata and richer crates.io package fields, so prebuilt binaries install via `cargo binstall pond-db`

### Changed

- *(release)* replace goreleaser-Pro with a release-plz + moon publishing pipeline (crates.io, Homebrew tap, NUR)

### Other

- *(moon)* exclude local .claude/.agents tooling from input hashing

## [0.2.7](https://github.com/tenequm/pond/compare/v0.2.6...v0.2.7) - 2026-06-02

### Other

- bump kache to v0.4.1 and persist buildkit cache via PVC

## [0.2.6](https://github.com/tenequm/pond/compare/v0.2.5...v0.2.6) - 2026-06-02

### Fixed

- *(build)* deterministic rcodesign sign + split package step; darwin-first; 2x buildkit

## [0.2.5](https://github.com/tenequm/pond/compare/v0.2.4...v0.2.5) - 2026-06-02

### Fixed

- *(build)* darwin via zig 0.16 + post-link sdk rewrite & re-sign

## [0.2.4](https://github.com/tenequm/pond/compare/v0.2.3...v0.2.4) - 2026-06-02

### Fixed

- *(build)* pin zig 0.15.2 so darwin binary records sdk<26

### Other

- drop redundant setup-protoc; cite real zig tickets for dylib bug

## [0.2.3](https://github.com/tenequm/pond/compare/v0.2.2...v0.2.3) - 2026-06-02

### Fixed

- *(build)* pin macOS SDK to 15.5 to avoid dyld duplicate-dylib abort
- *(release)* publish binaries to public homebrew-tap

### Other

- split moon format/lint/test into separate steps
- disable release-plz semver-checks to speed up release PRs

## [0.2.2](https://github.com/tenequm/pond/compare/v0.2.1...v0.2.2) - 2026-05-29

### Other

- *(readme)* replace standard-readme badge with crates.io version
- *(readme)* drop CI badge
- export KUBECONFIG so buildx subprocess inherits it
- set KUBECONFIG from $RUNNER_TEMP in-step, not job env
- fix goreleaser dirty-tree + add release recovery dispatch

## [0.2.1](https://github.com/tenequm/pond/compare/v0.2.0...v0.2.1) - 2026-05-28

### Fixed

- *(.gitignore)* anchor .claude patterns to root so fixture paths are not double-tracked
- *(get)* default to conversational view; consolidate spec.md rules

### Other

- chain publish-release on release-plz releases_created output
- *(release-plz)* enable release-pr flow alongside dry-run release
- rename jobs for clarity (build-and-test, release-plz, publish-release)
- *(release)* publish binaries + homebrew + nur via goreleaser
- preserve target/ between runs with checkout clean=false
- *(release-plz)* run in dry-run mode
- bracket cargo commands with kache stats steps in both jobs
- scope concurrency to github.ref so newer runs supersede older
- split into ci + release jobs, both on the self-hosted runner
- collapse release into the ci job (single self-hosted job, conditional release step)
- cancel in-flight CI runs on the same pull_request head
- switch CI to self-hosted runner on bl
- prep repo for public release + cross-compile pipeline
