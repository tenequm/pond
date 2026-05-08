# Pond - Design v2

> Status: in progress, built section by section. Section 5 (Open Questions) is the workspace for unresolved items. As each question is decided, the answer moves up into the relevant section and the question is struck through in section 5. Goal: an empty section 5.

---

## 1. What this is + non-goals

Pond is a Rust crate that wraps `lance-format/lance` directly with sessions-aware ingest, storage, and a JSON wire interface. One binary. Two transports: HTTP and MCP. Two deployments: a personal pond on a laptop, or a multi-tenant backend for hosted agent infrastructure.

Lance (and the file format underneath it) is the substrate. Pond does not introduce a separate "substrate layer" of its own and does not depend on the `lancedb` wrapper crate. Pond owns canonical session types, source adapters, the wire schema, the HTTP and MCP transports, and the conventions for using Lance consistently across deployments.

### 1.1 v1 scope

- **One application: sessions.** Lossless ingest, storage, and hybrid search of agentic-client sessions.
- **v1 source: Claude Code.** Other clients (Codex, OpenCode, Cursor, aider, Gemini CLI, others) on roadmap.
- **Two transports day 1**:
  1. **HTTP+JSON** (primary). RPC-shaped over `POST /v1/<operation>`. Streaming reads via SSE.
  2. **MCP** (specialization). Same handlers, stdio JSON-RPC 2.0, exposed as MCP tools and resources.
- **Two deployments**:
  1. **Personal**: one binary, one local Lance directory, single hardcoded `local` namespace. Replaces the personal `kb` MCP.
  2. **Hosted**: same binary against an object-store URL (S3, GCS, Azure). Each namespace is an integrator-supplied opaque string; the integrator owns identity, access, and routing.

### 1.2 Non-goals (philosophy, never)

These are stable positions, not v1 cuts. They will not be reopened.

- **Reinvent what Lance provides.** Storage abstraction, indexing (vector kNN, BM25 FTS, scalar), schema migration, OCC, blob columns, time-travel, namespaces, manifest versioning: all Lance. Pond does not wrap these in extra traits, ship parallel substrate primitives, or hide them behind seams. We use Lance directly.
- **Invent a wire format for canonical types.** Pond's session types are owned serde structs in the shape of `effect/unstable/ai` Prompt and Response unions, copied not depended on. We control schema versioning. No upstream surprises.
- **Provide authn, authz, identity, or tenancy policy.** Integrators decide who can access which namespace before any pond call. Namespace is an opaque string filled by the integrator.
- **Be a runtime, harness, tool executor, compaction engine, renderer, or observability platform.** Pond stores. The runtime decides when to compact, which tools to invoke, how to render, what to emit as telemetry.
- **Be a zero-knowledge store.** Operators with bucket and KMS access have full visibility. Encryption is bucket SSE plus filesystem encryption. No application-level crypto.
- **Ship a UI, SQL surface, or sidecar daemon.** Lance embedded is the only engine. v1 surface is HTTP+JSON plus MCP plus CLI verbs.

Items deferred for later (resources, replay, live-write, additional sources, hosted facade extensions) live in section 4. Items still uncertain live in section 5.

---

## 2. Foundations and locked-in stack

### 2.1 Stack

| Concern | Choice |
|---|---|
| Language | Rust, edition 2024 |
| Async runtime | tokio |
| Storage and search engine | `lance-format/lance` crates direct: `lance`, `lance-table`, `lance-io`, `lance-encoding`, `lance-index`, `lance-namespace`, `lance-namespace-impls`. No `lancedb` crate dependency. |
| Lance file format | `stable` (2.2+) for new datasets. Blob v2 for FilePart payloads. |
| Object store backends | `object_store` via Lance: local filesystem, S3 (native conditional writes), GCS, Azure. |
| HTTP server | axum (tokio-native, JSON-first, SSE built in). |
| MCP server | rmcp (official Anthropic Rust SDK), wrapping the same handlers as HTTP. |
| Wire format | JSON. Single evolving schema with top-level `protocol_version` field. Additive-only changes; formal `v2` only on breaking changes. |
| Default embedding model | bge-small-en-v1.5 via fastembed-rs (local ONNX, 384-dim). Pond's embedding registry supports multi-model coexistence. |
| Output | single static binary via `cargo build --release`. |

No SQL anywhere. No additional database. No `lancedb` crate dependency. Personal pond = one binary, one local directory. Hosted pond = same binary, object-store URL.

### 2.2 Wire interface

The wire interface is the contract. Internal serde types evolve freely behind a projection layer.

- **Transport-agnostic handlers.** Every operation is a function `Json request -> Json response` (with optional streaming response for SSE). HTTP and MCP transports are thin adapters that dispatch to the same handler functions.
- **Request envelope.** Every request carries a `protocol_version` field at the top level. Server validates the field and returns a typed error on mismatch.
- **Schema evolution.** Additive-only within a major version. Adding fields is allowed; removing or retyping fields requires a major bump.
- **Published schema artifacts.** JSON Schema files generated from Rust types, committed to the repo, versioned with the binary.
- **HTTP shape.** `POST /v1/<operation>` with JSON body; RPC-shaped, no REST resource model. Streaming responses use SSE on `GET /v1/sessions/{session_id}/events?since=<event_id>`.
- **MCP shape.** Same operations exposed as MCP tools (`pond_search`, `pond_get`, `pond_ingest`, etc.). MCP `tools/list` returns the operation set.

### 2.3 Operational invariants

These are constraints every pond write and read must satisfy. Code review rules.

1. **Append-only writes.** Existing rows are never mutated. Updates produce new rows or new manifest versions.
2. **Deterministic primary keys.** Client-supplied IDs (UUIDv7 for sessions, content-hash for derived rows where applicable). All writes use Lance `merge_insert` on the PK so retries are no-ops.
3. **Retry-with-jitter on every Lance call.** Pond-side helper (3 attempts default, 300ms-5000ms exponential backoff, 0.2 jitter, per-operation labels). Connection-level retry on top.
4. **No cached table handles forever.** Pond is a long-lived server. Use Lance's `read_consistency_interval` (set to a small integer for hosted, `0` for personal) so external writers are picked up. No "open at startup, hold forever" pattern.
5. **No silent drops.** Malformed input surfaces with line offset and error context. Ingest fails closed by default.
6. **Opaque IDs, not paths.** `namespace_id`, `workspace_id`, `project`, `agent_id` are opaque strings. The Claude Code SourceAdapter decodes path-encoded session directories once at ingest and stores the decoded values; readers never re-parse.
7. **ASCII-only docs.** All Markdown files in this repo use ASCII characters only. Per `CLAUDE.md`.
8. **No SQL.** Lance scalar predicates and search APIs are the only query mechanism.
9. **Encryption is operational.** Bucket SSE plus filesystem encryption. No application-level crypto, no `is_encrypted` columns, no KeyProvider.
10. **Schema versioning at the dataset level.** Lance manifest version plus dataset-level metadata key. No per-row `schema_version` columns.

### 2.4 Concurrency model

Stateless workers. Multiple pond processes can write concurrently to the same namespace. Lance OCC handles append conflicts via manifest versioning. Content-addressed payloads make worker crashes and retries idempotent.

No external coordinator on plain S3 (native conditional writes since mid-2025). GCS and Azure have native atomic conditional writes. Local filesystem uses Lance's internal commit lock.

No in-process write queue. Concurrent HTTP requests dispatch to handlers in parallel. Lance OCC plus retry-with-jitter resolves contention. The single-lane gateway antipattern observed in OpenClaw deployments (forcing 4-subprocess fanout for parallelism) is explicitly rejected.

### 2.5 Search defaults

- **Hybrid by default.** Every search runs vector kNN plus BM25 FTS, merged with RRF (`k=60`, no weighting), unless the request specifies otherwise.
- **Wire-level override.** Optional `search_mode` enum field on search requests: `hybrid` (default), `vector`, `fts`. Optional `rrf_k` integer field overrides the default `k`.
- **Both indexes always present.** FTS index on text columns and vector index on embedding columns are created at table creation. The schema does not branch on whether an embedding model is configured. Turning a model on or off does not require a schema migration.

---

## 3. v1 Application: sessions

(skeleton - content to be authored in a follow-up pass)

### 3.1 Canonical types

Owned Rust serde structs and enums in the shape of `effect/unstable/ai` Prompt and Response Part unions. Copied, not depended on. The Part union is pond's moat for the sessions application.

Variants to be specified: `TextPart`, `ReasoningPart`, `FilePart` (Blob v2), `ToolCallPart` (with `tool_type` discriminator: `function`, `server`, `mcp`, `extension`), `ToolResultPart`, `ToolApprovalPart` (`Request`, `Response`). Harness extensions: `CompactionPart`, `RetryPart`, `SnapshotPart` / `PatchPart`, `StepStartPart` / `StepFinishPart`, `SubtaskPart`, `AgentPart`.

Tree topology: every entry carries `(session_id, entry_id, parent_id)`. Branching is a leaf-pointer cursor over the tree; no copy-on-fork. Conversation context is a path-walk from leaf to root with compaction-aware projection.

Synthetic sentinels for orphaned tool calls preserved on ingest (do not drop assistant messages with no matching tool result).

### 3.2 Datasets

Working shape (to be specified):

- `sessions` - container, source provenance, project, workspace_path, agent_id, source_agent, source_version, schema_version.
- `messages` - per-turn, role, model, provider (OTel `gen_ai.provider.name` registry), input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, cost, finish_reason, parent_message_id.
- `parts` - per content block, Part union materialized, `search_text` column, blob columns for FilePart payloads, content_hash.
- `embeddings` - backfilled vectors keyed `(part_id, model_id, model_version, chunk_index)`. Multi-model coexistence supported.

### 3.3 Search surface

Hybrid (vector + BM25 + RRF) by default. Filters mirror the kb tool surface: `project`, `conversation_id`, `from_date` / `to_date`, `role`, `min_score`, `boost_recent`, `group_by_conversation`, `include_tool_results`, `include_thinking`, `limit`. Project filter supports both exact match and `LIKE` substring (per kb behavior).

Retrieval modes: single message, single message with N thread-context messages above and below, full conversation, conversation up to a message.

### 3.4 Ingest surface

`SourceAdapter` trait: `discover()` plus `decode()`, varies per agentic client. v1 ships the Claude Code adapter. `discover` scans Claude Code's session directories; `decode` parses JSONL into pond canonical types and writes via `merge_insert` on `(session_id, entry_id)`.

Per-event append granularity, not batched. End-of-session import is the v1 default; live-write is deferred (section 4) but the per-event write path is the same shape, so activating live-write is additive.

### 3.5 Conformance fixture set

Pi-mono's cross-provider fixture data ported as committed test assets (JSON files derived from `packages/ai/test/cross-provider-handoff.test.ts` fixtures plus the provider-specific shape tests). v1 tests are storage round-trip only: ingest fixture, store, retrieve, assert structural equivalence with input. The transport-to-provider layer reactivates with replay (section 4).

### 3.6 Wire operations

Working set (to be finalized):

- `pond_search` - `POST /v1/search`
- `pond_get` - `POST /v1/get`
- `pond_ingest` - `POST /v1/ingest` (single event or batched events)
- `pond_session_events` - `GET /v1/sessions/{id}/events?since=<id>` (SSE stream)
- `schema://pond` - resource (search fields and filter documentation)
- `stats://pond` - resource (dataset counts, embedding model, storage stats)

CLI verbs (out-of-band): `pond ingest --from claude-code`, `pond status`, `pond embed-worker`, `pond serve`.

---

## 4. Deferred (yes-later, with activation conditions)

Each entry: what it is, why deferred, activation condition. None require schema migrations or call-site changes elsewhere when activated.

- **Resources application.** Per-namespace knowledge-base files (blobs plus metadata) as a second consumer alongside sessions. Activation: a concrete second consumer that needs blob+metadata storage outside the sessions schema. Adding it is mechanical: new Lance table, new schema, same connection.
- **Cross-provider replay engine.** Re-projecting canonical Parts into provider-specific request shapes (Anthropic, OpenAI, Bedrock, Gemini, etc.). Activation: first integrator demand. Pond canonical types are stored as future-proofing; replay is projection over Parts, not a schema concern. Activation also gates the full pi-mono cross-provider conformance test matrix as the acceptance gate.
- **Live-write tools.** `pond_commit`, `pond_session_open` (and HTTP equivalents). Streaming events written as they arrive instead of via retrospective ingest. Activation: first runtime that wants to plug pond in mid-session. v1 ingest is per-event already; live-write is a transport into the same handlers.
- **Wire-fidelity capture.** `raw_request` / `raw_response` columns plus middleware capturing provider wire bytes. Activation: when replay reactivates and audit-grade fidelity is required.
- **Additional source adapters.** Codex, OpenCode, Cursor, aider, Gemini CLI, ChatGPT, others. Activation: per-source demand. Each is a new `SourceAdapter` impl with no impact on substrate or other adapters.
- **Hosted-tier facade extensions.** Federated namespace credential vending, per-tenant KMS isolation via separate buckets, distributed read-through cache. Activation: first hosted-tier customer with these requirements.
- **Cross-provider replay tests with live API calls.** The full pi-mono test that sends fixtures to real provider APIs. v1 ships fixture data plus storage round-trip tests only.
- **Graph traversal layer** (Kuzu or `lance-graph`). OpenCypher engine over the same Lance storage when `parent_id` recursive lookups become a bottleneck. Activation: when path-walks are measurably slow on real data.
- **AuditSink.** First-class audit-event subsystem. Activation: hosted-tier compliance requirement.
- **EventBus.** Change-event notifications via channel or PubSub. Activation: external systems need to react to pond writes.
- **SecretsRedactor.** Indexer hook scrubbing API keys, tokens, PII from `search_text` and embedding inputs before write. Activation: hosted-tier with sensitivity requirements.
- **Cross-session attachment dedup.** Background job over `content_hash` merging duplicate FilePart payloads. Activation: when storage cost from duplicates is measurable.
- **Per-namespace bucket separation.** Operational upgrade for KMS isolation when prefix-only is insufficient. Activation: hosted-tier KMS isolation requirement.
- **Remote embedding providers.** OpenAI, Voyage, Cohere, custom. Activation: model demand beyond local fastembed-rs default.
- **Nested namespaces.** Hard isolation between sub-spaces within a tenant (e.g. separate Lance dataset per project). Activation: opt-in user/tenant request. v1 uses single namespace per tenant with project as a column.

---

## 5. Open Questions

Workspace for unresolved items. When a question is decided, its answer moves into the relevant section above and the question is struck through here with a one-line note pointing to where the decision landed.

### ~~OQ1. Resources application: canonical type and storage shape~~

Resolved 2026-05-08. Resources deferred to v2 (see section 4). Goal: ship sessions-only v1 with the smallest possible codebase. Adding a second Lance-backed application later is mechanical given Lance's table-level isolation.

(Section currently empty beyond the resolved entry. New questions land here as they arise.)
