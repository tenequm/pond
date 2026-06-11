# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.8.0](https://github.com/tenequm/pond/compare/v0.7.0...v0.8.0) - 2026-06-11

### <!-- 0 -->🛠 Breaking Changes
- **config:** [**breaking**] URL-scoped creds, storage URLs, introspection, and migrate

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
