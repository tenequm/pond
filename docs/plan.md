# Pond - v1 implementation plan

> Goal: a single binary that fully replaces the personal `kb` MCP. All v1 foundational
> features from `docs/design.md` sections 1-4, plus exactly one SourceAdapter (Claude
> Code) implemented and tested end to end against `tests/fixtures/session-samples/claude-code/`.
>
> Scope discipline: this plan covers v1 only. Section 4 "Deferred" items in `design.md`
> stay deferred. No additional source adapters. No replay. No live-write.
>
> Process: each stage below is one committable, self-contained unit of work - code plus
> tests plus any doc updates. A stage is settled (reviewed, all "Done when" criteria
> green) before the next begins. No time estimates anywhere; stages are sized by
> coherence, not duration.

---

## Definition of done (the whole plan)

Pond replaces `kb` when all of the following hold:

1. `pond mcp` runs a stdio MCP server (and `pond serve` exposes the same over the
   `/mcp` HTTP route) with `pond_search` and `pond_get` tools that a Claude Code
   session can use in place of `kb_search` / `kb_get` with equivalent behavior (see
   "kb parity contract" below).
2. `pond ingest --from claude-code` ingests the real `~/.claude/projects/` tree
   losslessly, idempotently, and incrementally.
3. Hybrid search (vector + BM25 + RRF) returns relevant results over that corpus with
   working `project` / `session_id` / `from_date` / `to_date` / `role` / `source_agent`
   / `min_score` / `boost_recent` / `group_by_conversation` filters.
4. The HTTP+JSON transport serves the same handlers (`POST /v1/<op>` + SSE).
5. Conversations that exist in `kb` but no longer have source `.jsonl` on disk are
   back-filled from kb's Qdrant store (gap-fill only, never overwriting local data).
6. CI is green: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`.

### kb parity contract

The MCP surface a Claude session sees must match `kb` behavior:

| kb tool / behavior | pond equivalent |
|---|---|
| `kb_search(query, limit, project, conversation_id, from_date, to_date, role, min_score, boost_recent, group_by_conversation)` | `pond_search` - same filters; `conversation_id` maps to `session_id` filter |
| `kb_get(message_id, conversation_id, up_to, context_depth, max_messages, include_tool_results, include_thinking)` | `pond_get` - `conversation_id` maps to `session_id`; `up_to` and `max_messages` per the design.md 3.6.3 edits |
| excluded thinking / tool-result shown as `[... N chars]` placeholder | MCP transport renders the placeholder (design.md 3.6.3 MCP-transport note); HTTP strips |
| `min_score` default `0.5` | pond default `0.0` - deliberate: pond's hybrid score is RRF (~0.02-0.03) + recency boost, a different scale; `0.5` would filter everything |

---

## Locked decisions (resolved during planning)

- **Build order**: risk-first - storage spine, then search, then transports. Each is a stage.
- **Module layout**: 5 source files. `src/main.rs`, `src/lib.rs` (core as inline `mod`s,
  splitting to files only when a module gets long), `src/substrate.rs` (Lance store +
  staleness window + retry + maintenance), `src/embed.rs` (embedding worker),
  `src/transport.rs` (`mod http` + `mod mcp`). Plus `tests/`. The Qdrant migration is a
  throwaway `examples/migrate_qdrant.rs`, `git rm`'d after its one run.
- **Lance crates**: git dependencies pinned to tag `v7.0.0-beta.8` (`lance`,
  `lance-table`, `lance-io`, `lance-encoding`, `lance-index`, `lance-namespace`,
  `lance-namespace-impls`). The 7.x beta line is not published to crates.io (latest
  there is 6.0.0); `v7.0.0-beta.8` is the latest 7.x beta tag.
- **Timestamps**: `chrono 0.4` (stability over `jiff`'s pre-1.0 churn).
- **kb corpus migration**: (a) fresh-ingest from `~/.claude/projects/` is the cutover;
  (b) a separate one-off Qdrant gap-fill, skip-if-exists by `session_id` (never
  overwrites local data). Idempotency is verified in Stage 1 by a same-adapter
  double-run, not by the Qdrant re-ingest.
- **Test fixtures**: `tests/fixtures/session-samples/` (all 8 platforms moved here from
  `docs/references/`). v1 tests consume only `claude-code/`; the rest are design
  reference until their adapters ship.
- **CI**: GitHub Actions on push/PR - `cargo fmt --check`, `cargo clippy --locked
  -D warnings`, `cargo test --locked`. `rust-toolchain.toml` pins stable. `Cargo.lock`
  is committed (pond is a binary crate) - `--locked` makes every CI run reproducible,
  which matters because the `lance` crates resolve from a git tag.

---

## Stage 0 - Scaffold and dependency validation

**Goal**: a compiling, CI-wired empty project with every pinned dependency proven to
expose the load-bearing APIs `design.md` depends on. The crates.io dependencies are
verified against their published artifacts; the `lance` crates are verified against
the git-pinned `v7.0.0-beta.8` tag (the 7.x beta line is not published to crates.io).

**Build**:
- `Cargo.toml` (edition 2024, the opinionated lint block from the rust-dev skill),
  `rust-toolchain.toml`, `rustfmt.toml`, `.gitignore` (does NOT ignore `Cargo.lock` -
  pond is a binary crate, the lockfile is committed).
- **First task - prove the dependency table resolves.** Write `Cargo.toml` with the
  full pinned set below, run `cargo generate-lockfile`, confirm every entry resolves
  (especially the git-pinned `lance` crates against tag `v7.0.0-beta.8`), commit
  `Cargo.lock`. This happens before any source code - if the table doesn't resolve,
  that is the finding, and it surfaces on line one of the project.
- The 5 source files as stubs: `main.rs` (clap skeleton, verb enum), `lib.rs`,
  `substrate.rs`, `embed.rs`, `transport.rs`.
- Pinned dependency set in `Cargo.toml`, exact versions from the planning-stage
  verification (re-confirmed via `cargo info` on 2026-05-14). The `lance*` crates are
  git dependencies pinned to a tag (they are not published to crates.io); the rest are
  caret-pinned at the verified crates.io version:

  | Crate | Version | Notes |
  |---|---|---|
  | `lance`, `lance-table`, `lance-io`, `lance-encoding`, `lance-index`, `lance-namespace`, `lance-namespace-impls` | git, tag `v7.0.0-beta.8` | NOT on crates.io - the 7.x beta line is unpublished (latest crates.io stable is 6.0.0). Each crate is a git dependency: `{ git = "https://github.com/lance-format/lance", tag = "v7.0.0-beta.8" }` (tag commit `338fc35f`, 2026-05-13 - the latest 7.x beta tag). `Cargo.lock` pins the exact commit; do not float. |
  | `tokio` | `1.52` | `features = ["full"]` |
  | `serde` | `1.0.228` | `features = ["derive"]` |
  | `serde_json` | `1.0.149` | |
  | `anyhow` | `1.0.102` | app-level errors |
  | `thiserror` | `2.0.18` | adapter error enums |
  | `clap` | `4.6` | `features = ["derive"]` |
  | `tracing` | `0.1.44` | |
  | `tracing-subscriber` | `0.3.23` | |
  | `toml` | `1.1` | config.toml - note 1.x API differs from the long-lived 0.8 line |
  | `uuid` | `1.23` | `features = ["v7"]` |
  | `chrono` | `0.4.44` | RFC3339 timestamps |
  | `axum` | `0.8.9` | HTTP + SSE; MSRV well under edition-2024 |
  | `rmcp` | `1.7` | MCP; pin `"1.7"` (HEAD commit msg drift noted in verification) |
  | `fastembed` | `5.13.4` | `features = ["qwen3"]` - candle path for Qwen3 |
  | `tokenizers` | `0.22` | MUST match fastembed 5.13.4's transitive `tokenizers` (0.22.2): pond builds its own `Tokenizer` and passes it into `Qwen3TextEmbedding::new`, so the type identity must match. Latest is 0.23.1 but pond deliberately does not use it. |
  | `reqwest` | `0.13` | dev-dependency only, for `examples/migrate_qdrant.rs` |

- `.github/workflows/ci.yml` - `cargo fmt --check`, `cargo clippy --locked
  -D warnings`, `cargo test --locked`. Includes an `actions/cache` step keyed on the
  Qwen3 model id with `HF_HOME` pointed at the cache dir: the first Tier-2 run is a
  cache miss and downloads the weights (~600MB) over the network, then populates the
  cache; every run after restores from cache with no download (see Stage 2 test
  split). Tier-1 jobs need no model and no cache.
- `tests/lance_smoke.rs` - exercises the four load-bearing Lance APIs against the
  git-pinned `v7.0.0-beta.8` lance crates: (1) unenforced-PK `merge_insert`
  find-or-create, (2) `WhenNotMatched::DoNothing` insert-only mode, (3) Blob v2
  `Struct<data,uri>` column round-trip, (4) `cleanup_old_versions` signature +
  `delete_unverified`.

**Done when**:
- `Cargo.lock` is generated, committed, and the full dependency table resolves
  (git-pinned `lance` crates included).
- `cargo build --locked`, `cargo clippy --locked -D warnings`, `cargo fmt --check` all
  pass.
- `tests/lance_smoke.rs` passes - all four APIs confirmed present and behaving as the
  planning verification reported.
- CI runs green on a pushed branch.

**design.md coverage**: 2.1 stack, 2.1.1 defaults skeleton.

**Risk note**: the planning-stage verification read the lance git source at the
`v7.0.0-beta.8` tag - the exact commit this stage pins - so the four APIs should hold.
`lance_smoke.rs` confirms they compile and behave inside pond's own build (vs the
agents' read of the source), surfacing any gap here before real code is built on it.
That is the entire point of this stage.

---

## Stage 1 - Storage spine

**Goal**: lossless, idempotent, incremental ingest of Claude Code sessions into the
four Lance datasets, with read-back via the `pond_get` handler. This stage proves the
architecture: canonical types, the streaming `SourceAdapter`, the event-ordering
buffer, `merge_insert` on canonical PKs, Blob v2.

**Build** (`lib.rs` inline modules + `substrate.rs`):
- `mod types` - canonical `Session` / `Message` / `Part` Rust types (design.md 3.1),
  serde-derived, snake_case, with the `ProviderOptions` bag.
- `mod datasets` - the four Lance schemas (3.2.1-3.2.4), `WriteParams` per 3.2.0
  (`data_storage_version` 2.2, `enable_v2_manifest_paths`, `enable_stable_row_ids`,
  unenforced PKs, `auto_cleanup` window). Scalar indexes (BTREE/BITMAP) and the FTS +
  vector index slots declared at table creation; population/optimization is Stage 2-3.
- `substrate.rs` - connection open, `merge_insert` helper keyed on canonical PKs,
  retry-with-jitter (3 attempts, exponential backoff, per-op labels - invariant 3),
  the pond-owned staleness window (`checkout_latest()` refresh policy - invariant 4).
- `mod ingest` - `IngestEvent` enum, the event-ordering contract validator (3.4),
  the per-role `search_text` concatenation policy (3.3.1), the buffer-to-boundary
  ingest flow that writes a Message row + its Part rows in one pass. Per-event
  `merge_insert`. `source_agent` / `project` denormalization onto `messages`. The
  validator's abort unit is the offending session's event substream - a violation
  drops the rest of that session's events and continues with other sessions. This is
  one validator with one rule, shared verbatim by the CLI streaming adapter and the
  HTTP batch handler (no transport-specific behavior).
- `mod adapter` - the `SourceAdapter` trait (3.4), then `claude_code` impl: `discover`
  walks `~/.claude/projects/<encoded>/<uuid>.jsonl`; `decode` parses the JSONL
  (parentUuid chains, `tool_result` user-entry blocks, `toolUseResult` sidecar field)
  into ordered `IngestEvent`s. Handles the 3 CLI-version format variants in the
  fixtures (2.1.68 old format, 2.1.104 / 2.1.132 modern hook+attachment flow).
- `mod get` - the `pond_get` handler (`Json -> Json`): session-scope and message-scope
  reads, `context_depth`, `up_to`, `max_messages`, `include_thinking` /
  `include_tool_results` stripping (design.md 3.6.3, with the parity edits).
- `mod wire` - request envelope (`protocol_version`), error envelope + closed code
  enum (3.6.1), `request_id` generation. Namespace defaults to `local`.
- `pond ingest --from claude-code` and a minimal `pond status` wired in `main.rs` so
  the stage is dogfoodable from the CLI.

**Tests** (`tests/conformance.rs`):
- Round-trip: ingest each of the 3 `claude-code` fixtures, read back via `pond_get`,
  assert structural equivalence with a re-parse of the source (design.md 3.5).
- Idempotency double-run: ingest the same fixture twice; second run reports all
  `matched`, zero `inserted`; dataset row counts unchanged.
- Ordering-contract enforcement: a deliberately mis-ordered event stream surfaces
  `validation_failed` and aborts (invariant 5, 3.4).
- Blob v2: a FilePart payload round-trips through the `parts.data` column.

**Done when**:
- All `tests/conformance.rs` cases pass on the 3 real fixtures.
- `pond ingest --from claude-code --data-dir <tmp>` against a copy of
  `~/.claude/projects/` completes without `no silent drops` violations.
- `pond_get` returns a full session and a message-with-context correctly.
- clippy + fmt + CI green.

**design.md coverage**: 2.3 invariants, 2.4 concurrency, 3.1 types, 3.2 datasets
(schema + write params), 3.3.1 concatenation, 3.4 ingest, 3.5 conformance, 3.6.1 error
envelope, 3.6.3 pond_get, 3.6.4 pond_ingest handler logic.

---

## Stage 2 - Search

**Goal**: hybrid search over the ingested corpus, at message granularity, with the
`pond_search` handler and proven filter-pushdown.

**Build** (`lib.rs` inline modules + `embed.rs`):
- `embed.rs` - the embedding worker: fastembed-rs `Qwen3TextEmbedding::from_hf` (the
  `qwen3` feature / candle path), pond-owned `tokenizers::Tokenizer` for the
  token-aware chunker (1024-token chunks, 128 overlap), deterministic chunking, writes
  `embeddings` rows with denormalized filter columns (3.2.4). `pond embed-worker` CLI
  verb. Reads `messages.search_text` directly - no second concat path.
- `mod search` - the `pond_search` handler: vector kNN + BM25 FTS retrievers, each
  with `Scanner::prefilter(true)` (load-bearing - design.md 3.3 implementation note),
  RRF merge on `message_id` (k=60), recency boost (3.3 formula), `min_score`
  postfilter, `group_by_conversation` collapse, `search_mode` override
  (hybrid/vector/fts). All filters: `project` + `project_match`, `session_id`,
  `from_date` / `to_date`, `role`, `source_agent`, `limit` (cap 200).
- Embedding model registry: config-driven `[[embeddings.models]]`, validated against
  pond's own known-model set, built-in Qwen3 default (design.md 3.2.4, as corrected).
- Index creation/population: FTS index on `messages.search_text`, IVF_PQ on
  `embeddings.vector` (defaults per 3.2.4), with the flat-scan fallback below the
  10k-row activation threshold.

**Tests**:
- Prefilter assertion: an integration test runs a filtered hybrid search and asserts
  via `Scanner::explain_plan` that the scalar predicate appears as a
  `ScalarIndexQuery` / `ScalarIndexExec` node, NOT a top-level `FilterExec`. This is
  the load-bearing test design.md 3.3 demands.
- Hybrid relevance: known query against the fixture corpus returns the expected
  message in the top hits; `matched_via` reports the contributing retrievers.
- Filter correctness: `project`, `role`, `from_date`/`to_date`, `session_id`,
  `source_agent` each narrow results as expected; `project_match: is_null` works.
- `group_by_conversation` collapses to one summary per session with correct
  `best_score` / `message_count`.
- Determinism: same `(model_id, search_text)` produces identical chunks across runs.

**CI test tiers** (the embedding tests cannot all run in a vanilla CI runner - Qwen3
weights are a ~600MB HuggingFace download):
- **Tier 1 - always-run, no model.** Chunker determinism, registry validation, RRF
  merge math, recency-boost formula, filter-predicate construction, the `explain_plan`
  prefilter assertion (uses a tiny vector written directly, no model needed). These
  run in the standard `cargo test` CI job.
- **Tier 2 - real-model, cached job.** The actual Qwen3 embedding round-trip and
  end-to-end hybrid relevance over the fixture corpus. Gated to a dedicated CI job
  that restores the `actions/cache` model cache from Stage 0; locally these run
  whenever the model is cached. Marked so they are skippable (`#[ignore]` + an
  explicit job, or a cfg/env gate) - never blocking a contributor without the weights.

**Done when**:
- The `explain_plan` prefilter test passes - pushdown confirmed on real data.
- Hybrid / vector / fts modes all return ranked results over the fixture corpus.
- All filters verified.
- `pond embed-worker` populates `embeddings` for an ingested corpus.
- Vector-index activation is verified against an expanded fixture set (10k+ message
  rows - additional `claude-code` fixtures provided alongside this stage): the IVF_PQ
  index builds past the 10k-row threshold, and a query confirms it is used (not the
  flat-scan fallback). This criterion is explicitly separated from the Tier-1 fixture
  tests, which run on the original 3-session set and stay below the threshold.
- clippy + fmt + CI green.

**design.md coverage**: 2.5 search defaults, 3.2.4 embeddings, 3.3 search surface,
3.6.2 pond_search.

---

## Stage 3 - Transports and operations

**Goal**: the same Stage 1-2 handlers exposed over HTTP+JSON and MCP, plus the
operational verbs. After this stage `pond serve` is a working `kb` replacement.

**Build** (`transport.rs` + `main.rs` + `substrate.rs`):
- `transport.rs::http` - axum server: `POST /v1/search`, `/v1/get`, `/v1/ingest` as
  thin adapters over the wire handlers; `GET /v1/sessions/{id}/events` SSE (catch-up
  reads per 3.6.5, `axum::response::sse` with 15s keepalive); the `/mcp` route
  carrying rmcp's streamable-HTTP MCP transport. Binds `127.0.0.1:9797`, env
  overrides, `--port 0` support, `0.0.0.0` security notice (2.1.1).
- `transport.rs::mcp` - the rmcp MCP layer, transport-agnostic: exposes `pond_search`
  / `pond_get` / `pond_ingest` tools and `schema://pond` / `stats://pond` resources as
  thin adapters over the same wire handlers, mounted both on the `/mcp` HTTP route
  (via `pond serve`) and on stdio (via `pond mcp`). Renders the `[... N chars]`
  placeholders for stripped Parts (design.md 3.6.3 MCP-transport note). Defines its
  own `-32000`-family error codes (rmcp ships none).
- Transport / logging split: `pond serve` runs the HTTP server (with `/mcp`) and logs
  normally (stdout = output, stderr = tracing). `pond mcp` runs a stdio MCP server
  only and routes ALL logging - tracing, diagnostics, everything - to stderr; stdout
  is reserved exclusively for JSON-RPC frames (design.md 2.1.1).
- `mod config` (in `lib.rs`) - full `config.toml` schema + load/validate,
  `pond config --print-schema` (2.1.1).
- `substrate.rs` - the maintenance task: `cleanup_old_versions` + `optimize_indices`
  background tokio task spawned by `pond serve`, `[maintenance]` config block, and the
  `pond maintenance` one-shot verb (3.2.0).
- `main.rs` - remaining CLI verbs: `pond serve` (HTTP + `/mcp`), `pond mcp` (stdio MCP
  only), and admin verbs `pond versions list`, `pond checkout <version>`,
  `pond restore <version> --force` (3.6, mapping to Lance `versions()` /
  `checkout_version()` / `restore()`).

**Tests**:
- HTTP integration: each `POST /v1/<op>` round-trips against a real ingested dataset;
  error envelope shapes (3.6.1) verified for `validation_failed`, `not_found`,
  `version_unsupported`.
- SSE: `GET /v1/sessions/{id}/events` streams `session` / `message` / `end` events in
  canonical order; `since` resume works.
- MCP integration: drive the rmcp stdio server, call `pond_search` and `pond_get`,
  assert the responses match the kb parity contract (including placeholder rendering).
- Maintenance: the task runs `cleanup_old_versions` + `optimize_indices` without
  crashing `serve`; `pond maintenance` one-shot produces the same effect.

**Done when**:
- `pond serve` runs; HTTP and MCP both answer over the same handlers.
- The MCP `pond_search` / `pond_get` behavior matches the kb parity contract.
- Maintenance task verified; admin verbs work against a real dataset.
- clippy + fmt + CI green.

**design.md coverage**: 2.1.1 personal defaults, 2.2 wire interface, 3.2.0 maintenance,
3.6 wire operations (HTTP + MCP + CLI + admin verbs), 3.6.5 SSE.

---

## Stage 4 - Cutover

**Goal**: pond is live as the personal knowledge base, fed from real local data,
replacing `kb` in the MCP config.

**Build**:
- No new source files. This stage is operational.
- Run `pond ingest --from claude-code` against the real `~/.claude/projects/` into the
  personal data dir (`$XDG_DATA_HOME/pond/`).
- Run `pond embed-worker` to populate embeddings and build the vector index.
- Swap the MCP server entry in the Claude Code MCP config: `kb` -> `pond mcp` (stdio
  MCP server; stdout reserved for JSON-RPC, all logs to stderr).
- Parity smoke: a named, written-down checklist re-run against `pond_search` /
  `pond_get` and compared to the `kb` baseline. The checklist (committed to the repo,
  e.g. `docs/parity-checklist.md`, so parity does not depend on memory):
  1. `pond_search` plain query, default filters - top hits are relevant and ranked.
  2. `pond_search` with `project` filter (`exact`) - results scoped to one project.
  3. `pond_search` with `project_match: is_null` - results from sources with no project.
  4. `pond_search` with `role: assistant` and a `from_date`/`to_date` window.
  5. `pond_search` with `source_agent` filter.
  6. `pond_search` with `group_by_conversation: true` - one summary per session.
  7. `pond_search` with `boost_recent: false` - recency boost reported as 0.
  8. `pond_get` by `session_id` - full session returned.
  9. `pond_get` by `message_id` with `context_depth: 5` - thread context above/below.
  10. `pond_get` by `session_id` with `up_to` + `max_messages` - restore-to-a-point.
  11. `pond_get` with `include_thinking` / `include_tool_results` toggled - placeholder
      rendering over MCP matches `kb`.
  Each line records the `kb` result shape and the `pond` result shape side by side;
  equivalence is a human judgement but the cases are fixed, not improvised.

**Done when**:
- A Claude Code session uses `pond_*` tools in place of `kb_*` with no loss of
  function on the parity checklist.
- The full local corpus is searchable.

**design.md coverage**: 1.1 personal deployment ("replaces the personal kb MCP").

---

## Stage 5 - Qdrant gap-fill (operational runbook, not a code stage)

> Unlike Stages 0-3, this is post-v1 operational work, not committable product code:
> the script is a throwaway, deleted after its single run. It is numbered as a stage
> only because it is sequenced after Stage 4 and has its own done-criteria. Treat it
> as a cutover runbook.

**Goal**: back-fill conversations that exist in kb's Qdrant store but whose source
`.jsonl` is gone from disk - the only data Stage 4 cannot recover.

**Build** (throwaway):
- `examples/migrate_qdrant.rs` - a one-off script:
  1. Enumerate `session_id`s already present in pond's `sessions` dataset.
  2. Read kb's Qdrant store; for each session NOT already in pond, project its records
     into canonical `IngestEvent`s (best-effort fidelity - Qdrant is lossy vs the
     source `.jsonl`; this data is gap-fill, not a replacement).
  3. Ingest the complement only. Skip-if-exists by `session_id` - structurally
     incapable of overwriting Stage 4's high-fidelity local rows.
- Log: sessions skipped (already local), sessions migrated, sessions failed.

**Tests**:
- Pre/post assertion: sessions present from Stage 4 are byte-identical before and
  after the migration run (zero overwrites); only-in-Qdrant sessions are added.

**Done when**:
- The migration runs once; the pre/post assertion holds.
- `examples/migrate_qdrant.rs` is `git rm`'d in the same commit that records the run
  outcome - it never ships in the binary, never becomes a module.

**design.md coverage**: none - this is operational migration, outside the v1 design
surface.

---

## What this plan deliberately excludes

Per `design.md` section 4, all of these stay deferred and are NOT in any stage above:
additional source adapters (Codex, OpenCode, etc.), cross-provider replay,
live-write tools, the resources application, wire-fidelity capture, hosted-tier facade
extensions, graph traversal, AuditSink, EventBus, SecretsRedactor, remote embedding
providers, nested namespaces, wire-surfaced time-travel, FilePart content-hash dedup.

The hosted deployment (object-store URL, opaque namespaces) is *designed for* by the
code written in Stages 1-3 - namespace is a wire field and a storage-path prefix from
day one, so hosted is additive rather than a rewrite - but it is **not proven**: no
stage here exercises object-store backends (S3/GCS/Azure) or non-`local` namespaces.
v1's acceptance is the personal pond only. Treat hosted as "the door is left open,"
not "hosted is supported."
