# Pond - Design v1

---

## 1. What this is

A unified storage and retrieval layer for sessions produced by any agentic client (Claude Code, Codex, OpenCode, Cursor, aider, ChatGPT, Gemini CLI, ...). One codebase, two deployments: personal pond and multi-tenant backend for hosted agent infrastructure.

## 2. Use cases (both day 1)

1. **Personal**: replace `kb` MCP. Ingest local Claude Code sessions, search them semantically, replay through any provider.
2. **Hosted**: storage and search backend for multi-tenant agent deployments. Each namespace is an isolation boundary; the integrator owns identity, access, and routing.

## 3. Hard requirements

- **Lossless multi-source ingest** - Claude Code v1, Codex on roadmap, others later. No loss of tool calls, reasoning, attachments, provider-specific metadata.
- **Retrospective ingest (Day 1)** - pond is fed by reading completed session files via SourceAdapter (CLI verb `pond ingest`). Live-write MCP tools for runtimes that plug in during a session are deferred (section 15).
- **Cross-provider replay** - re-project a stored session into any modern provider's request shape (OpenAI, Anthropic, Bedrock, Gemini) from canonical Parts. No wire-bytes capture in v1.
- **Hybrid search** - keyword (BM25) and semantic (vector kNN); results fused via Reciprocal Rank Fusion. All three modes native to LanceDB. Embeddings per part, multiple models / variants per part allowed.
- **Multi-tenant from day 1** - namespace = bucket prefix; default-deny resolution.
- **Encryption-at-rest** - delegated to bucket SSE (S3 / GCS CMEK / Azure CMK) + filesystem encryption for any local cache. Zero application-level crypto.
- **Source of truth = object storage** - LanceDB datasets in the bucket are authoritative. Search indexes (FTS / vector / columnar) live within Lance; rebuild = re-run ingest, not a separate verb.
- **Stateless workers** - many MCP processes write concurrently to the same namespace. Lance optimistic concurrency control handles append-write conflicts; content-addressed payload makes retries idempotent. No external coordinator.
- **Backfillable embeddings, voluntary worker** - ingest never blocks on embed compute. Embedding backfill is idempotent, keyed by `(part_id, model, variant, chunk_index)`; multiple workers can run safely.
- **kb feature parity (v1 acceptance test)** - filter set: `project`, `conversation_id`, `from_date` / `to_date`, `role`, `min_score`, `boost_recent`, `group_by_conversation`, `include_tool_results`, `include_thinking`, `limit`. Retrieval modes: single message + +/-N thread context, full conversation, conversation up to a message.

## 4. Non-goals

- Inventing a new wire format. Adopt provider-canonical Prompt/Response shapes (effect/unstable/ai is the design reference) but copy as our own serde types - this is the moat.
- Authentication, access control, user identity. Integrator owns these.
- Container orchestration, routing, channel adapters. Integrator's responsibility.
- Observability platform, agent memory framework, notes engine.
- UI (MCP only in v1).

## 5. Locked-in stack

- **Language**: Rust
- **Async runtime**: tokio
- **Storage abstraction**: [`object_store`](https://docs.rs/object_store) crate (S3 / GCS / Azure / local fs)
- **Search and storage engine**: [`LanceDB`](https://github.com/lancedb/lancedb) (native FTS BM25 + vector kNN + columnar filters + RRF hybrid + Lance blob columns + content-addressed dedup)
- **MCP server**: [`rmcp`](https://github.com/modelcontextprotocol/rust-sdk) (official Anthropic Rust SDK, stdio JSON-RPC 2.0)
- **Serialization**: serde
- **Output**: single static binary via `cargo build --release`

No SQL anywhere. No additional database. Personal pond = one binary, one local dir. Hosted pond = same binary, S3 URL.

## 6. Canonical types

Own Rust serde structs/enums in the shape of the `effect/unstable/ai` Prompt + Response part unions. Copied, not depended on. This is the application moat - we control schema versioning and evolution, no upstream surprises.

Part union (each variant is a column-materialized row in the `parts` dataset):
- Text, Reasoning, File, ToolCall, ToolResult, ToolApproval{Request, Response}
- Streaming Start/Delta/End variants are not part of v1 storage; live-write capture deferred to section 15.

Harness extensions (not in upstream):
- CompactionPart (auto / overflow / tail_start_id)
- RetryPart (attempt, error, time)
- SnapshotPart / PatchPart (working-tree state at turn boundaries)
- StepStartPart / StepFinishPart (per-step tokens / cost / finish reason within an assistant turn)
- SubtaskPart (sub-agent / Task tool pointer)
- AgentPart (`@agent` mention spans)

`editorContext` on UserMessage, `ResourceSource` for MCP resources.

`ToolCallPart` covers both user-defined tool calls (with handlers) and provider-defined tool calls (e.g. OpenAI WebSearch, executed server-side, no handler) - pond stores both identically.

## 7. Schema (six Lance datasets)

Layout within a single bucket, per namespace:

```
<bucket>/<namespace>/
|-- sessions.lance       container; source provenance, project, default model/provider, fork pointer, agent_id, schema_version
|-- messages.lance       per-turn; role, model, provider, tokens, cost, finish_reason, parent_message_id
|-- parts.lance          per content block; part union materialized; search_text col; blob_data (blob col) for FilePart payloads; content_hash col
|-- embeddings.lance     backfilled vectors; keyed by (part_id, model, variant, chunk_index)
|-- resources.lance      per-agent knowledge base files (content-addressed)
`-- agents.lance         per-namespace agent definitions / registry
```

**Attachments are not a separate dataset.** A FilePart row in `parts` carries its bytes inline via the Lance `blob` column type, lazily fetched on `select`. Cross-session dedup is deferred to a background job over `content_hash` if and when it matters; not a schema concern.

**Source of truth.** All payload bytes (FilePart contents) live as Lance blob columns within the datasets above. There is no separate `chunks/` directory in the bucket. Lance handles blob storage internally (separate physical files within the dataset, transparent to the API).

Forward-compat columns on every dataset:
- `namespace_id` (opaque string, populated by NamespaceResolver)
- `schema_version` (per-row, for migrations)

Forward-compat columns specific to `sessions`:
- `agent_id` (opaque string; namespace contains many agents, each session belongs to exactly one)

## 8. Seams (four)

| # | Seam | Purpose | Default impl |
|---|------|---------|--------------|
| 1 | **ObjectStorage** | The only storage seam. LanceDB on top of `object_store`. Reads/writes datasets and blobs. | `object_store::aws` / `object_store::gcp` / `object_store::azure` / `object_store::local` |
| 2 | **ReplayProvider** | Re-projects canonical Parts into provider-specific request shape | Anthropic + OpenAI + Bedrock + Gemini request schemas |
| 3 | **EmbeddingProvider** | Text -> vector | bge-small-en-v1.5 via fastembed-rs (local ONNX, 384-dim). Single impl Day 1. |
| 4 | **SourceAdapter** | `discover()` + `decode()`; varies per agentic client | Claude Code adapter v1; Codex on roadmap |

Collapsed-into-config (no separate seam needed):
- **NamespaceResolver** - just a function `request -> bucket+prefix`. Reads from request context for hosted, env for personal.
- **SecretsProvider** - env config for object storage credentials and API keys.
- **KeyProvider** - does not exist. Encryption is bucket SSE (operational), not app concern.

**Hybrid search** is a function over LanceDB's native search modes, not a seam. RRF helper merges BM25 and vector results.

## 9. Concurrency model

Stateless workers. Multiple MCP processes can write concurrently to the same namespace. Lance OCC handles append conflicts via manifest versioning - conflicts are rare on append-only writes (only manifest pointer contention). Content-addressed payload (`content_hash`) makes worker crashes / retries idempotent.

No external coordinator, no per-namespace ingester pool, no leader election. If a hot tenant produces retry storms (write rate >> realistic), introduce an in-process write batcher (10ms timeout flush) as a local optimization - architecture does not change.

Embedding backfill is voluntary: any worker can pick unembedded parts via `WHERE embedding IS NULL`, compute, write. Concurrent workers may duplicate work briefly; OCC retries cheaply, no leasing required.

## 10. Encryption-at-rest

Operational, not application-level. Bucket SSE (S3-SSE / SSE-KMS / GCS CMEK) covers all object storage payload (datasets, blob columns, manifests). Filesystem encryption (LUKS / APFS / BitLocker) covers any local cache.

Zero app crypto. No `is_encrypted` columns. No `KeyProvider` seam. No envelope encryption code in the pond.

Per-namespace KMS isolation can be added later by giving each namespace its own bucket (instead of prefix) and wiring a separate KMS key to the bucket policy. This is a pure operational change, no schema impact.

**Threat model.** Operators with bucket access + KMS access have full visibility - pond is not zero-knowledge. Process memory, query logs, replication streams, and embedding API call payloads are not encrypted by pond. Search text and embedding vectors are stored plaintext in Lance datasets - they leak n-grams of original content and must be redacted at indexer time if your threat model treats them as sensitive (deferred to section 15).

## 11. Source of truth and rebuild

LanceDB datasets in object storage are source of truth. Indexes (FTS, vector, columnar) are part of the Lance dataset format - no separate index files to manage. Schema migrations work in-place via Lance column add / drop / type evolution.

There is no `rebuild` verb. Rebuild = re-run the ingest path:
- On startup, MCP checks `dataset_schema_version` against the binary; mismatch triggers lazy background re-ingest from existing datasets.
- Manual rerun: `pond replay <namespace>` - idempotent over content-addressed records.

Index cache freshness for long-lived MCP processes: ETag check on dataset manifest before serving each query. If manifest changed since cached version, refresh in-process Lance handle. Cheap (single HEAD request).

## 12. Public API surface

v1 facade: rmcp MCP server (`pond --mcp`) over stdio JSON-RPC 2.0. Replaces `kb` for the personal use case.

**Tools** (mirror claude-kb surface, params identical):
- `pond_search` - 12 params: `query`, `limit`, `project`, `conversation_id`, `from_date`, `to_date`, `role`, `min_score`, `boost_recent`, `include_tool_results`, `include_thinking`, `group_by_conversation`. Returns SearchResult / ConversationSearchResult / ErrorResult.
- `pond_get` - 7 params: `message_id`, `conversation_id`, `up_to`, `context_depth`, `max_messages`, `include_tool_results`, `include_thinking`. Modes: single message, message + +/-N context, full conversation, conversation up to a point. Returns GetResult / ErrorResult.

**Resources**:
- `schema://pond` - search fields and filter documentation
- `stats://pond` - dataset counts, embedding model, storage stats

**CLI verbs** (out-of-band from MCP, on-demand only):
- `pond ingest --from claude-code [--path <jsonl-dir>]` - one-shot import
- `pond replay <namespace>` - manual re-ingest from existing datasets
- `pond status` - bucket health, dataset stats
- `pond embed-worker [--namespace <ns>]` - run idempotent embedding backfill (optional, can run forever or one-shot)

HTTP and richer CLI facades deferred.

## 13. Boundary rules

When any further facade is added, these boundary rules are non-negotiable:

- **Wire schema is the contract.** Endpoints versioned (`/v1/...`); internal serde types evolve freely behind a projection layer. No internal types cross the wire as-is.
- **No raw object storage URLs cross the wire.** Clients receive pond-mediated handles or pre-signed URLs with explicit expiry.
- **No DEKs cross the wire** (n/a today since we have no DEKs - operational reminder for any future per-tenant key features).

## 14. What this is NOT

- Not authn/authz. Integrator decides who can access which namespace before any pond call.
- Not a namespace identity service. Namespace is an opaque string filled by integrator.
- Not a resource injector. Pond stores agent resources; runtime owns retrieval and injection.
- Not a compaction engine. Pond stores `CompactionPart` events; runtime decides when to compact.
- Not a renderer. Consumers format Parts for their own UI.
- Not a tool executor. Live replay takes a tool toolkit parameter; record-only replay needs none.
- Not a transport protocol designer. We adopt MCP stdio JSON-RPC 2.0; we do not invent wire formats.
- Not zero-knowledge. Operators with bucket and KMS access have full visibility.
- Not a runtime. Pond is the storage substrate a runtime writes into.

## 15. Deferred (additive when activated)

These earn forward-compat columns or a future Layer-wrap slot but aren't authored as v1 interfaces:

- **Graph traversal layer (Kuzu)** - bolt OpenCypher engine over the same Lance storage when `parent_message_id` recursive lookups become a bottleneck. `parent_message_id` is already a column; backfill an edges table from it in one pass. No schema migration.
- **AgentRegistry as live API** - day 1 it's just rows in `agents.lance`. Resource injection and runtime hooks are integrator concerns.
- **AuditSink** - `audit_events.lance` dataset; pond wraps writes with append events.
- **RateLimiter** - wraps `ReplayProvider`; aspectual.
- **EventBus** - emits change events via channel/PubSub abstraction.
- **HTTP / richer CLI facades** - on roadmap. Boundary rules in section 13 apply.
- **Published wire-contract package** - JSON Schemas, OpenAPI, MCP tool manifest as committed artifacts. On roadmap.
- **SecretsRedactor** - indexer hook that scrubs `search_text` and embedding inputs of API keys, tokens, PII before write.
- **Cross-session attachment dedup** - background job over `content_hash` column merging duplicate FilePart payloads.
- **Per-namespace bucket separation** - operational upgrade for KMS isolation when prefix-only is insufficient.
- **ChunkCompactor** - optional Lance compaction pass for old datasets.
- **Live-write MCP tools** (`pond_commit`, `pond_session_open`) for runtimes that plug in directly during a session, instead of via retrospective JSONL ingest.
- **Wire-fidelity capture (L2)** - raw_request / raw_response columns + ReplayProvider middleware in live path. On roadmap.
- **Remote embedding providers** (OpenAI / Voyage / Cohere / custom). On roadmap.
- **Postgres or other SQL backends**. On roadmap (default has no SQL).

All are pure additions when activated. None require schema migrations or call-site changes elsewhere.

## 16. Reference patterns lifted

- **Loki / Quickwit single-store**: thin index over object storage with periodic flush, lazy download on read. LanceDB is exactly this pattern, productized.
- **Turbopuffer architecture**: namespace = bucket prefix, content-addressed records, three-tier hot/warm/cold cache. We get cache via LanceDB read-through.
- **uni-db design** (rustic-ai): embedded library combining graph + vector + Lance columnar over object storage. Architectural inspiration; we use mature pieces (LanceDB, plus Kuzu later) instead of a 31-star dep.
- **claude-kb tool surface**: `kb_search` + `kb_get` parameter set proven over a year of personal use.
- **Pi-mono leaf-cursor branching**: `parent_message_id` graph, conformance test matrix (cross-provider handoff, image-tool-result, tool-call-without-result, unicode-surrogate).
- **Multi-tenant integration patterns**: opaque-namespace + integrator-owned access check, tagged-union access decisions carrying a `reason`.

Patterns explicitly rejected:
- Two SQL backends (SQLite + Postgres). Replaced by single LanceDB on object_store.
- Field-level encryption. Replaced by bucket SSE.
- Sidecar daemon (Chroma-style). Replaced by embedded LanceDB.
- Effect-TS / Bun runtime. Rust binary instead.
- Pi-mono's silent-skip-malformed-line ingest (multi-source needs schema-validated decode).

## 17. Decision summary

- Rust binary, single static deploy.
- LanceDB as the only storage and search engine. No SQL.
- Object storage (`object_store` crate) as the only storage substrate.
- Four seams (ObjectStorage, ReplayProvider, EmbeddingProvider, SourceAdapter).
- Six Lance datasets. Attachments merged into `parts` via blob columns.
- Stateless workers, Lance OCC for concurrency, no external coordinator.
- Encryption operational (bucket SSE + FS), not app-level.
- Append-only domain events; replay = re-ingest, no `rebuild` verb.
- v1 MCP surface = `pond_search` + `pond_get` + two resources. CLI verbs out-of-band.
- Own canonical types in shape of effect/unstable/ai - this is the moat.
- Multi-tenancy via bucket prefix per namespace; separate buckets when KMS isolation matters.
- Default never deletes; retention/compaction are deferred policies.

## 18. Open calibration items

Not architecturally blocking, decisions to make at implementation time:

- **Index cache refresh policy** - ETag check is the chosen mechanism; tune polling vs query-time check based on observed latency.
- **Streaming ingest** - end-of-session import is v1 default. Tail-mode (live indexing during active session) deferred.
- **FTS tokenizer / language strategy** - per-namespace config for stemmer / language detection. Default to English + code-aware tokenizer for v1.
- **Namespace identifier scheme for personal** - single hardcoded `local` namespace, or path-derived per project. Operational choice.

## 19. Outstanding grill questions

Pending stress-test before v1 implementation lock. Each entry: question, why it matters, current default, working recommendation.

### 19.1 Collapse to 4 datasets (in flight)

Drop `agents.lance` and `resources.lance` from section 7. Keep four: `sessions`, `messages`, `parts`, `embeddings`.

- `agents.lance` - "per-namespace agent definitions / registry". Day 1 in personal pond holds 1-2 placeholder rows (claude-code, codex). Source agent metadata (`source_agent`, `source_version`) belongs as columns on `sessions.lance`. Section 15 already defers AgentRegistry-as-live-API.
- `resources.lance` - MCP resources attached to UserMessage are already covered by `ResourceSource` field (section 6) and FilePart blob in `parts.lance`. Cross-session attachment dedup is already a deferred background job over `content_hash`. No Day 1 use case for a separate dataset.

Working recommendation: collapse to 4. Adds `source_agent` / `source_version` columns to `sessions.lance`. Section 17 decision summary updates "Six Lance datasets" to "Four Lance datasets". If Day 2 brings real demand for agent registry as data, add a new dataset then (Lance new dataset = new prefix, near-zero cost).

### 19.2 Schema_version per row vs per dataset

Section 7 says `schema_version` is a per-row column on every dataset. This is a SQL/Postgres pattern where ALTER TABLE backfills heterogeneous rows.

Lance has native manifest versioning (each commit = new manifest version) plus tags + branches. Adding nullable column via `add_columns` is metadata-only; old rows naturally read NULL for new column. Per-row `schema_version` is redundant noise.

Working recommendation: drop per-row `schema_version` column. Use Lance manifest version + dataset-level metadata key (`schema_version` in dataset config, not as column) to track major schema generations. If a semantic migration needs "this row was written under schema rules X" tracking, add the column then for the affected dataset only.

### 19.3 Lance file format version lock

Design doesn't pin the Lance file format version. Implications:
- `2.2+` enables Blob v2 (separate-file blob columns, lazy stream via `take_blobs`, external URI support, Map type)
- `<= 2.1` only has legacy LargeBinary blob via metadata flag

Working recommendation: lock to `2.2+` for new datasets. Document FilePart `blob_data` column uses Blob v2 via `blob_field` / `blob_array`. Add to section 5 (locked-in stack): "Lance file format: 2.2+".

### 19.4 Cross-provider replay surface

Section 3 promises cross-provider replay; section 12 has no `pond_replay` MCP tool. Where does replay actually execute?

Two options:
1. Library helper: `pond::replay::project(session_id, target_provider) -> ProviderRequest` available as Rust API. CLI verb `pond replay --to anthropic` for one-shot. No MCP tool.
2. MCP tool: `pond_replay { session_id, target_provider }` returns provider-shaped request payload. Runtime / integrator picks up and executes the actual API call.

Working recommendation: option 1 for Day 1. Replay is projection logic over Parts; integrator calls Rust library or CLI as needed. Adding MCP tool when first integrator demands it (Day 2). Updates section 12 to clarify that replay surface is library + CLI, not MCP.

### 19.5 Branching primitive: parent_message_id graph vs Lance branches

Section 7 has `parent_message_id` column on messages (graph branching pattern from pi-mono). Section 7 also has "fork pointer" on sessions. Lance natively supports git-like branches at the dataset level.

Three possible primitives, partially overlapping:
- `parent_message_id` graph - per-message DAG, branching at message level inside a session
- `fork_pointer` on sessions - one session forked from another at a specific message
- Lance dataset branches - whole-dataset alternative timelines

Working recommendation: keep `parent_message_id` (per-message graph, needed for tree-of-thoughts / leaf-cursor patterns). Drop separate `fork_pointer` on sessions - subsumed by `parent_message_id` reaching across session boundaries with `source_session_id` reference if needed. Do NOT use Lance dataset branches for session forks - too coarse, every fork would need a whole new dataset version. Lance branches reserved for pond-level operations (release branches, audit snapshots), not user-level session forks.

### 19.6 search_text generation discipline

Section 7 describes `search_text` column on `parts.lance`. Unclear what goes there for non-text part variants:
- TextPart - obvious, the text content
- ReasoningPart - reasoning text content
- FilePart - filename? extracted text? content hash? mime type?
- ToolCallPart - tool name + serialized arguments? formatted summary?
- ToolResultPart - serialized result? truncated stdout?
- StepStartPart / StepFinishPart - step metadata?

This affects FTS recall quality and what queries make sense.

Working recommendation: define `search_text` extraction per part variant in section 6 explicitly. Default rules:
- TextPart, ReasoningPart - direct text
- FilePart - filename + first N bytes if text mime, just filename otherwise
- ToolCallPart - `<tool_name> <serialized_args_truncated_to_2KB>`
- ToolResultPart - `<tool_name_result> <truncated_to_2KB>`
- Streaming Start/Delta/End - skip (no search_text)
- Compaction/Retry/Snapshot/Patch/Subtask/Agent parts - skip or short metadata

Adds explicit table in section 6 mapping each part variant to its search_text contribution.

### 19.7 NamespaceResolver: collapsed-into-config or real seam?

Section 8 collapses NamespaceResolver into config: "just a function `request -> bucket+prefix`. Reads from request context for hosted, env for personal."

For Day 1 personal pond (single hardcoded `local` namespace), env is fine. For Day 2 hosted, integrator needs to plug in their own namespace lookup logic (per-tenant routing, cross-region sharding, etc.). At that point env-based config is insufficient; needs to be a trait.

Working recommendation: define NamespaceResolver as a Rust trait Day 1, ship single env-based default impl. Treat as fourth-and-a-half seam: "config-shaped Day 1, trait-shaped from the start so Day 2 swap is mechanical." Updates section 8 to list NamespaceResolver as fifth seam alongside the four.

## 20. References to review

External work to read through before locking v1 implementation:

- [agx-dev/agx](https://github.com/agx-dev/agx) - portable agent container format (Rust + SQLite)
- [agent-life/agent-life-data-format](https://github.com/agent-life/agent-life-data-format) - Agent Life Format (ALF), 4-layer agent state spec
- [letta-ai/agent-file](https://github.com/letta-ai/agent-file) - Letta .af, portable agent file format
- [Electric SQL: durable streams as agent-loop primitive](https://electric.ax/blog/2026/04/08/data-primitive-agent-loop) - addressable persistent streams over Postgres
- [MinIO AIStor Tables GA](https://markets.financialcontent.com/wss/article/bizwire-2026-2-3-minio-introduces-ga-of-aistor-tables-unifying-enterprise-data-for-agentic-ai) - Iceberg V3 over object storage for agentic AI
- [Durable agents: async queue workflow checkpoint](https://tianpan.co/blog/2026-04-23-durable-agents-async-queue-workflow-checkpoint) - long-running agent state via queue + checkpoint pattern
