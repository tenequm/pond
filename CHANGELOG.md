# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2](https://github.com/tenequm/pond/compare/v0.2.1...v0.2.2) - 2026-05-28

### Added

- *(get)* three-mode response, compact summaries, flat scope-tagged shape; drop SSE
- *(mcp)* redesign tool response surface (single-mode search, size caps, cursor pagination)
- *(mcp)* sharpen pond_search surface and add similar_to + stats health
- *(search)* score-normalized hybrid fusion + bench reorg
- *(optimize)* split indices/compaction + add retention controls
- *(substrate)* atomic data + index commit at the write seam
- *(embed,status)* SIGINT-aware drain, visible rebuild phase, vector index freshness
- *(search)* drop language router, lazy embedder, --mode flag
- *(search)* hybrid redesign with cross-lingual router, benchmark harness, simulator
- *(search)* widen FTS tokenizer to ngram 3-5, add `sync --reindex`
- *(search)* language-neutral ngram FTS tokenizer
- *(search)* search correctness and Part provenance
- *(sessions)* scope part and embedding primary keys by session_id
- *(adapter)* bidirectional codec with serialize/restore face
- initial commit of Pond design specification v1
- remove LanceDB reference documentation and add lance-format skill
- add essential Rust references for crate shortlist, error handling, ownership and types, traits and generics
- *(substrate)* bump Lance to v7.0.0-beta.14, adopt lance-namespace + Store::scan + NamespaceIdent (inv 11/21/22)
- *(status)* --include-subagents flag for per-sub-agent rollup
- *(sync)* per-session staleness skip, insert-only writes
- *(cli)* embeddings opt-in flag, sync auto-maintenance, drop maintenance verb
- *(cli)* pond search/get commands, status overhaul, output stack
- *(ingest)* per-event drop, batched flush, level-2 self-heal, live progress
- align v1 to design.md, ready substrate for S3 backend
- *(embed)* cost-aware batching, length-bucketing, and cap-as-identity
- Stage 3 - HTTP and MCP transports, background maintenance
- add pond_ingest handler, Stage 2 search tests, and CUDA device support
- XDG data dir and the `pond setup` command
- Stage 2 - hybrid search and the embedding worker
- *(ingest)* storage spine - canonical types, Lance datasets, claude-code adapter
- add essential Rust references for crates, error handling, ownership, traits, and generics
- initial design spec

### Fixed

- *(sync)* treat empty/metadata files as benign skips and clean up output
- *(.gitignore)* anchor .claude patterns to root so fixture paths are not double-tracked
- *(get)* default to conversational view; consolidate spec.md rules
- *(substrate)* move fold-on-write to outermost public boundary
- *(embed,bench)* cut per-embed disk bloat, add pond compact, consolidate bench harness
- *(search)* gate benchmark overrides behind cargo feature
- *(embed)* rebuild vector index after column-update writes
- *(search)* accurate `sync --reindex` message on an empty corpus
- *(adapter)* harden the seam against oversized values and Arrow i32-offset overflow
- *(codex-cli)* ingest pre-Oct-2025 legacy rollout format
- *(codex-cli)* cap oversized function_call_output to prevent variant_data overflow

### Other

- scope push trigger to main, add workflow_dispatch dry-run, gate release-plz to PR merge
- *(get)* add pond_get three-mode redesign plan
- release v0.2.1 ([#4](https://github.com/tenequm/pond/pull/4))
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
- bump version to 0.2.0
- *(deps)* bump lance to v7.0.0, trim features, bump toolchain to 1.95
- *(embed)* candle-only backend with 5-min idle eviction
- *(plans)* add MCP tool response redesign plan
- *(mcp)* trim read path + cap Lance caches for the 500 MiB budget
- stop tracking skills-lock.json
- *(plans)* pond mcp/serve 500 MiB memory budget
- *(cli)* consolidate sync stages and archive-based transfer
- use nix profile add, add Nix install command
- add Nix install command
- *(readme)* add homebrew install instructions
- update readme.md pond description
- *(remote)* cut bucket-backed write/search latency by 30-50%
- *(readme)* rewrite per standard-readme spec
- *(s3)* add s3s-fs smoke test and backend latency benchmark
- *(cargo)* trim verbose comments to load-bearing one-liners
- post-overhaul cleanup + Lance v7.0.0-beta.16 compact workarounds
- *(plans)* post-overhaul cleanup plan
- *(spec)* overhaul §3.7 to index-write-decoupled; reframe CLI verbs; trim
- *(embed)* load e5 weights at FP16 to halve resident model memory
- *(embeddings)* consolidate research docs into docs/researches/embeddings/
- *(embed)* write vectors once per length-sort window
- *(embed)* run e5-base on Candle/Metal
- *(embed)* collapse embeddings into nullable columns on messages
- *(spec)* collapse embeddings into nullable columns on messages
- *(embed)* migrate from qwen3 to e5-small
- *(research)* add agent-session retrieval and value-benchmark landscape
- *(adapter)* log value truncation at debug, not warn
- ignore .claude directory
- *(spec)* add storage-via-lance invariant
- *(plan)* single-chunk execution, drop staged-commit ceremony
- add search-correctness-plan
- plan adapter seam hardening and spec bounded-values invariant
- *(plans)* add codex-cli legacy rollout format investigation plan
- *(adapter)* share open() config plumbing
- amend spec to v1 contract and add the alignment plan
- update spec references and clarify minimalism guidelines
- *(tests)* consolidate and trim test suite according to CLAUDE.md guidelines
- replace design.md with rewritten v1 spec, archive prior draft
- add recursive language models study PDF reference
- make design.md section references native links
- convert design.md section refs to ASCII
- slim design.md - cut redundancy, fix code drift
- *(deps)* bump lance to v7.0.0-beta.16 and Rust to 1.91.1
- *(claude)* drop --locked from local commands
- collapse integration suite into one binary, fix non-tty picker hang
- polish pass on the design.md restructure
- *(claude)* add commands, toolchain, deps, errors, layout sections
- restructure design.md to schemas/protocol shape with RFC normative language + stable anchors
- *(plans)* add commit-2 design.md restructure plan
- *(plans)* add e5-small migration plan
- *(claude)* affirmative comment policy with worked examples
- *(tests)* apply cargo fmt and extract vector_test_setup helper
- move unit tests inline and trim low-signal tests
- *(design)* tighten long invariants + collapse absent-canonical tables
- lance-namespace adoption, invariants 21-28, MemWAL forward-looking seam, archeology cleanup
- *(plan)* open question on closing two claude-code adapter losses
- cargo fmt
- in-batch dedup via FirstSeen, Session.project contract
- *(adapter)* type-enforced seam via Source/Extracted, collocate unit tests
- *(plan)* drop pond_ingest MCP tool, soften Stage 4 to side-by-side
- reorganize pond module architecture
- plan the 8-entry refactor
- *(embed)* drop chunking, embed whole messages
- fold embedding into `pond ingest`, drop the embed-worker verb
- *(plan)* make embedding+write batching load-bearing in Stage 2
- *(fixtures)* refresh claude-app schema-notes for the 4th session
- *(fixtures)* add claude-app session with populated uploads/ sidecar
- *(plan)* reference lance tag without stale commit hash
- *(scaffold)* set up pond crate, CI, and Lance smoke tests
- *(fixtures)* close opencode reasoning + nanoclaw thinness gaps
- *(fixtures)* add 6 redacted claude-code session samples
- *(design)* drop redundant ASCII-only invariant
- harden plan.md against review findings, soften ASCII rule
- add v1 implementation plan, fix design.md verification bugs, relocate session samples
- lock approach A (denormalize hot filter columns) + adapter rules
- *(design)* lock search_text population flow and event ordering contract
- *(design)* fix PK contradictions, narrow project convention to v1 scope, refresh section 5
- *(design)* resolve all remaining open questions (section 5 emptied)
- *(design)* route response metadata to options.<provider>.*, follow Effect
- *(design)* rewrite section 3.2 datasets as direct 3.1 serialization
- *(references)* add anonymized session samples for 8 agentic platforms
- *(design)* drop resolved open questions; remove strikethrough convention
- *(design)* canonical Session/Message/Part schemas + open questions workspace
- rewrite design around lance-direct substrate
- *(references)* add Effect-TS canonical AI source snapshot
- add CLAUDE.md with ASCII-only documentation rule
- *(references)* add LanceDB and Lance ecosystem reference snapshot
- add substrate spec and 2026-05-08 design notes
- add Standard Readme-compliant README and Apache-2.0 LICENSE
- *(design)* drop streaming variants from §6; add unresolved questions
- *(references)* snapshot opencode, kilocode, pi-mono, OTel GenAI semconv

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
