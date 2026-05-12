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

### 3.1 Canonical types

The canonical types describe pond's three core objects - **Session**, **Message**, **Part** - at the LLM-conversation level (provider/protocol layer), below any specific harness. Harness-specific behaviors (compaction, retry, snapshot, step accounting, editor context, etc.) are absorbed via the per-object `options` metadata bag, not as canonical fields.

The Part union derives from Effect v4's Prompt-side Part union (`effect/unstable/ai/Prompt.ts`), copied not depended on. Effect's Response-side variants (streaming start/delta/end, FinishPart, ResponseMetadataPart, ErrorPart, source-citation Parts) are not stored as Parts in pond - Response-side metadata is projected at ingest into Message-level Lance columns; streaming variants are not persisted (assembled state only).

Pond adds a Session container layer Effect doesn't define, plus Session-level fork pointers. Effect has no branching concept and Anthropic Managed Agents has no in-session branching either - the Session-level fork pointers are a deliberate one-step extension beyond Effect+Anthropic, motivated by lossless absorption of opencode (session-level fork) and pi-mono (per-entry branch graph, projected into multiple pond Sessions by the SourceAdapter).

#### 3.1.1 Conventions

- **Notation**: TypeSpec syntax in code blocks. The canonical types defined here are the source of truth; Rust implementation types and JSON Schema wire artifacts are derived from these. The doc is not run through `tsp compile` - readers verify consistency manually until the schemas stabilize.

- **Casing**: all field names AND discriminator values use `snake_case`. This deviates from Effect's source (which uses camelCase fields and kebab discriminator values) and matches OTel's `gen-ai-input-messages.json` schema, Anthropic's wire conventions, and pond's Rust/Lance column conventions. Internal consistency wins over verbatim Effect fidelity.

- **Discriminator field**: every Part has `type: "<literal>"`. Every Message has `role: "system" | "user" | "assistant" | "tool"`.

- **IDs**: branded scalars (`SessionID`, `MessageID`, `PartID`) distinct from `string` for clarity in the spec. They serialize as strings on the wire. Source-supplied where the source provides one (preserves pi-mono short IDs, opencode IDs, etc., losslessly); UUIDv7 generated by the SourceAdapter where the source doesn't supply one.

- **Timestamps**: `utcDateTime` (RFC 3339 string in JSON; microsecond-precision int64 in Lance). Source-recorded timestamps live on canonical types; pond's ingest timestamps live as Lance row-level columns, separately.

- **`options: ProviderOptions`** is the universal extensibility bag, present on every canonical object (Session, every Message variant, every Part variant). Namespacing convention:
  - `options.<provider_key>.*` for provider-namespaced extensions (`anthropic`, `openai`, `gcp.gen_ai`, `aws.bedrock`, etc.); matches Effect's `ProviderOptions` per-Part pattern
  - `options.source.*` for pond-internal namespace for source/adapter-populated facts (e.g., `source.parent_entry_id`, `source.version`, `source.workspace_path`, `source.project`, `source.tools`, `source.editor_context`, `source.compaction`, `source.retry`)
  - `options.pond.*` for pond-operational facts (rare; Lance columns are usually preferred)
  - `options.title` (and similar pond-canonical-but-not-promoted keys) for near-universal fields kept in the bag to keep canonical types minimal

#### 3.1.2 Common types

```typespec
scalar SessionID extends string;
scalar MessageID extends string;
scalar PartID extends string;

/** Arbitrary JSON value (string | number | boolean | null | array | object). */
scalar JsonValue;

/** Universal extensibility bag. See 3.1.1 for namespacing conventions. */
alias ProviderOptions = Record<string, JsonValue | null>;
```

#### 3.1.3 Session

```typespec
model Session {
  id: SessionID;

  // Session-level fork pointers. Both null for fresh (non-forked) sessions - the common case.
  parent_session_id?: SessionID;     // session this one forked from
  parent_message_id?: MessageID;     // cut-point: the last message in the parent that's part of this session's ancestry

  // Provenance: the source harness brand. One source per session.
  // Common values: "claude-code", "opencode", "pi-mono", "anthropic-managed-agents",
  // "openclaw", "nanoclaw", "kilocode", ... (open string)
  source_agent: string;

  // Source's session-creation time. NOT pond's ingest time
  // (which is tracked as a Lance row-level column, separately from canonical).
  created_at: utcDateTime;

  // Extensibility bag. See 3.1.1 for namespacing.
  options: ProviderOptions;
}
```

What's intentionally NOT on Session (and where it lives instead):

| Field | Where it lives | Why |
|---|---|---|
| Token-usage aggregates | Lance columns derived from messages | Sync risk if stored canonically; cheap to derive |
| Status (running/idle/...) | Not stored | Pond stores captured sessions, not live ones |
| Resources / environment | `options.source.*` | Harness-runtime metadata |
| Agent / tool snapshot | `options.source.tools` etc. | Harness-runtime metadata |
| Outcome evaluations | `options.source.*` | Anthropic-specific |
| Title | `options.title` | Display-only, not load-bearing |
| Default model / agent_id | Per-message fields or `options.source.*` | Per-message is more accurate (model_change events) |
| `updated_at` | Not stored | Derivable from `max(message.timestamp)` |
| `source_version` | `options.source.version` per Message | A session can span multiple source versions |
| `namespace_id` / `project` / `workspace_path` | Storage path (namespace) and `options.source.*` (others) | Storage-routing / filesystem-harness-specific |

#### 3.1.4 Message

Four role variants with per-role content allowlists, mirroring Effect's `Prompt.Message` shape. SystemMessage's `content` is a `string` (not a Parts array), per Effect convention.

```typespec
model BaseMessage {
  id: MessageID;
  session_id: SessionID;             // back-reference to containing session
  timestamp: utcDateTime;            // source-recorded; canonical ordering within the session
  options: ProviderOptions;
}

model SystemMessage extends BaseMessage {
  role: "system";
  content: string;                   // plain text; per Effect
}

model UserMessage extends BaseMessage {
  role: "user";
  content: Array<TextPart | FilePart>;
}

model AssistantMessage extends BaseMessage {
  role: "assistant";
  content: Array<TextPart | FilePart | ReasoningPart | ToolCallPart | ToolResultPart | ToolApprovalRequestPart>;
}

model ToolMessage extends BaseMessage {
  role: "tool";
  content: Array<ToolResultPart | ToolApprovalResponsePart>;
}

@discriminator("role")
union Message {
  system: SystemMessage,
  user: UserMessage,
  assistant: AssistantMessage,
  tool: ToolMessage,
}
```

Notes:

- Per-role content allowlists are enforced at the type level: a `ToolResultPart` inside a `UserMessage` is a category error.
- **Append-only log.** Messages within a session form a linear, append-only log per invariant 1 and the Anthropic Managed Agents session-as-event-log framing. Ordering within a session is `ORDER BY (timestamp, id)` with `id` as tiebreak. The SourceAdapter walks source data in source order and appends in that order.
- **No `parent_message_id` field.** Branching exists only at the Session level (3.1.3). In an append-only linear log, position in the log IS the next-after relationship - no per-message parent pointers are needed.
- **No turn-level metadata fields** (model, provider, finish_reason, tokens, response_id, error). Effect places these on Response-side metadata Parts (`FinishPart`, `ResponseMetadataPart`, `ErrorPart`); pond projects them at ingest into Lance columns on the message row. Canonical Message stays Effect-shaped.

#### 3.1.5 Part

Pond mirrors Effect v4's Prompt-side Part union: seven variants. Field names follow snake_case per 3.1.1; one rename from Effect: Effect's `id` on tool-call / tool-result / approval Parts is renamed to `call_id` / `approval_id` in pond to disambiguate from `id: PartID` (the storage-row identity, which Effect doesn't have because Effect models Parts as array members).

```typespec
@discriminator("type")
union Part {
  text: TextPart,
  reasoning: ReasoningPart,
  file: FilePart,
  tool_call: ToolCallPart,
  tool_result: ToolResultPart,
  tool_approval_request: ToolApprovalRequestPart,
  tool_approval_response: ToolApprovalResponsePart,
}

model BasePart {
  id: PartID;
  message_id: MessageID;             // back-reference to containing message (pond-additive over Effect)
  options: ProviderOptions;
}

model TextPart extends BasePart {
  type: "text";
  text: string;
}

model ReasoningPart extends BasePart {
  type: "reasoning";
  text: string;
}

model FilePart extends BasePart {
  type: "file";
  media_type: string;                // MIME type
  file_name?: string;
  /**
   * File data union. Storage layer materializes each variant:
   *   - string: base64-encoded inline (small payloads)
   *   - bytes:  inline raw bytes (Lance Blob v2 column)
   *   - url:    external reference preserved verbatim, OR pond's content-addressed
   *             scheme `pond://blob/<sha256>` for deduplicated stored blobs
   */
  data: string | bytes | url;
}

model ToolCallPart extends BasePart {
  type: "tool_call";
  call_id: string;                   // wire-level tool-call identifier; matches the corresponding ToolResultPart
  name: string;                      // tool name
  params: JsonValue;                 // tool arguments (untyped at canonical level)
  provider_executed: boolean;        // false = framework executes; true = provider/server-side execution
}

model ToolResultPart extends BasePart {
  type: "tool_result";
  call_id: string;                   // matches the originating ToolCallPart.call_id
  name: string;                      // tool name (denormalized for query convenience)
  is_failure: boolean;
  result: JsonValue;                 // tool result (untyped at canonical level)
}

model ToolApprovalRequestPart extends BasePart {
  type: "tool_approval_request";
  approval_id: string;
  tool_call_id: string;              // references a ToolCallPart.call_id awaiting approval
}

model ToolApprovalResponsePart extends BasePart {
  type: "tool_approval_response";
  approval_id: string;               // matches the originating ToolApprovalRequestPart.approval_id
  approved: boolean;
  reason?: string;
}
```

Notes:

- Brand keys present in Effect's source (`PartTypeId = "~effect/ai/Prompt/Part"`, `MessageTypeId`) are runtime-only and omitted from the wire; pond does not include them.
- `id: PartID` and `message_id: MessageID` on `BasePart` are pond-additive over Effect. Effect models Parts as array members of `Message.content`; pond stores them as separate Lance rows with back-references for queryability.
- All Part variants carry `options: ProviderOptions`. Provider-specific extensions (Anthropic reasoning signatures, OpenAI service tier, Bedrock metadata, kilocode editor context as it bubbles down to specific Parts) ride there.

#### 3.1.6 What's deliberately absent from canonical, and where it lives

Behaviors that some references model as first-class Parts or message-level fields, but pond keeps out of canonical and absorbs via `options` or storage projection:

| Concern | Where pond puts it |
|---|---|
| Compaction / branch-summary events | SourceAdapter projects them as synthetic Messages (typically `user`-role with `content` "the prior conversation was compacted into: <summary>"); compaction details on `options.source.compaction` |
| Retry attempts (failed-and-retried turns) | `options.source.retry.{attempt, prior_error}` on the affected Message |
| Step start/finish (sub-turn LLM-call brackets) | Token/cost accounting captured as Lance columns; step boundary metadata in `options.source.steps` if needed |
| Snapshot / patch references | `options.source.snapshot.*` on the Message or relevant tool Part |
| Subtask / sub-agent spawn events | A separate pond Session per subtask, linked via session-level `parent_session_id` |
| Editor context (kilocode, etc.) | `options.source.editor_context` on the `UserMessage` where captured |
| Streaming start/delta/end variants (Effect Response-side) | Not persisted (assembled state only) |
| Source/citation Parts (Effect's DocumentSourcePart, UrlSourcePart) | Deferred; not v1 |
| Tool definitions / available-tools list | `options.source.tools` per Session if the source provides it |
| Per-message error string / error tagged union (opencode AssistantError) | Lance column on the message row, projected at ingest from Effect's `ErrorPart` |
| Finish reason, token usage, model, provider, response_id | Lance columns on the message row, projected at ingest from Effect's `FinishPart` and `ResponseMetadataPart` |
| Tool approvals as side-table (opencode pattern) | Pond keeps as Parts (Effect's pattern), inside canonical |
| Per-Part timestamps (opencode's `text?:{start,end?}` on TextPart) | `options.source.time.*` on the Part |
| Synthetic orphan-tool-result sentinels (pi-mono inserts these at replay time) | Not in canonical storage; replay layer (deferred, section 4) generates them on demand |

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

Workspace for unresolved items. When a question is decided, its answer moves into the relevant section above and the question is removed entirely from this list. Goal: an empty section 5. Git history preserves the trail of resolved questions.

### OQ4. S3 native conditional writes claim - verify against current `lance-io`

**Where**: 2.4.

**Issue**: 2.4 asserts "no external coordinator on plain S3 (native conditional writes since mid-2025)." Archived U1 (from earlier in 2026) said S3 commits still required `s3+ddb://`. Need to verify Lance's current S3 commit implementation actually uses `If-None-Match` before locking the claim.

**Lean**: read `lance-io` source. If confirmed, claim stands. If not, qualify 2.4 with "AWS hosted requires `s3+ddb://` or single-writer-per-namespace lease."

### OQ6. Embeddings chunking strategy

**Where**: 3.2 (`chunk_index` column exists but rule is unspecified).

**Issue**: bge-small-en-v1.5 caps at 512 tokens. Long parts (`TextPart`, `ToolResultPart`) exceed this. Chunking rule must be deterministic for the PK `(part_id, model_id, model_version, chunk_index)` to be stable.

**Lean**: fixed-window character chunking at ingest. 1500 chars per chunk, 200 chars overlap. No semantic splitter in v1. Revisit when retrieval quality is measurable.

### OQ7. SourceAdapter trait shape - sync/async, streaming

**Where**: 3.4.

**Issue**: 3.4 names the trait but does not show signatures. Async vs sync, streaming vs Vec materially affect memory for large session dirs.

**Lean**: async + streaming both methods. `discover() -> impl Stream<Item = Result<SessionRef>>` and `decode(ref: SessionRef) -> impl Stream<Item = Result<Event>>`. Bounded memory on huge JSONL files.

### OQ8. Incremental ingest checkpoint mechanism

**Where**: 3.4.

**Issue**: On re-runs how does pond skip already-ingested events? File offset table? Event ID lookup? mtime?

**Lean**: rely on deterministic `(session_id, entry_id)` PK plus `merge_insert` (invariant 2 already requires this). Re-ingest is a no-op for matched rows. Add a discover-time checkpoint table only if scan becomes a measurable bottleneck.

### OQ9. `project` filter LIKE semantics

**Where**: 3.3 ("exact match and LIKE substring (per kb behavior)").

**Issue**: case sensitivity, wildcard support, prefilter-vs-postfilter not specified.

**Lean**: case-insensitive substring (`contains`), expressed as a Lance scalar filter expression so prefilter works on the scan.

### OQ10. `boost_recent` formula

**Where**: 3.3.

**Issue**: flag named, decay function not picked.

**Lean**: exponential decay with 30-day half-life on `timestamp_unix`, multiplied into the RRF score. Check kb's current implementation and match it if reasonable.

### OQ11. `group_by_conversation` return shape

**Where**: 3.3.

**Issue**: collapse to top-1 per conversation, top-K, or return conversation IDs only?

**Lean**: top-1 hit per conversation, with `match_count` integer on the result row. Matches kb behavior.

### OQ12. `protocol_version` exact format

**Where**: 2.1, 2.2.

**Issue**: integer, string, semver, MCP-style date?

**Lean**: integer (`1`, `2`, ...). Cheapest to compare server-side, matches MCP's JSON-RPC pattern, fewer string-format bugs.

### OQ13. `read_consistency_interval` concrete values

**Where**: 2.3 invariant 4 ("small integer for hosted, `0` for personal").

**Issue**: "small integer" not picked.

**Lean**: personal = `0` (every-read check, local FS is cheap). Hosted = `5s` (caps manifest fetch overhead, acceptable lag for human-driven queries).

### OQ14. JSON Schema artifact - OTel-anchored or pond-native?

**Where**: 2.2 (published schema artifacts).

**Issue**: 2.2 says "JSON Schema files generated from Rust types." Does not say whether they align with OTel GenAI schemas (`gen-ai-input-messages.json`, etc).

**Lean**: derive from OTel for the overlapping parts (messages, tool definitions); pond-additive for harness extensions (`CompactionPart`, etc). Saves design time, gives free interop with observability tooling.

### OQ15. HTTP auth seam shape

**Where**: 1.2 non-goal (integrator owns auth) vs 2.

**Issue**: non-goal says integrators decide who can access which namespace, but the seam for them to do that is not specified.

**Lean**: axum middleware that resolves request to `(namespace_string, opaque_auth_context)`. Default `local` middleware ships for personal pond. Handlers receive the resolved namespace, never raw headers.

### OQ16. Personal pond bind defaults

**Where**: not specified.

**Issue**: port, host, config file path not chosen.

**Lean**: `127.0.0.1:8787`. Override via `--bind` or `POND_BIND` env var. Config file at `$XDG_CONFIG_HOME/pond/config.toml` (macOS: `~/Library/Application Support/pond/config.toml`).

### OQ17. `model_version` format in embeddings PK

**Where**: 3.2.

**Issue**: format of `model_version` not picked. semver? hash? model card date?

**Lean**: semver string from the model card. Fallback `unknown` allowed but logged at ingest time.

### OQ18. Code organization - single crate vs workspace

**Where**: not in `design.md`; resolved in archived `design-notes-2026-05-08.md`.

**Issue**: archived note picked "single Cargo crate for v1" but the decision did not carry into `design.md`.

**Lean**: keep the decision. Add a one-liner to 2.1.

### OQ19. Live-write event handling design constraint (was U11)

**Where**: section 4 deferred entry for live-write.

**Issue**: section 4 lists live-write as deferred but does not record the activation-time design constraint: streaming events do NOT enter `parts.lance`; only the final assembled message Part is persisted. Pi-mono settled this; if it is not noted now we will rediscover it.

**Lean**: add a one-liner to the section 4 live-write entry.

### OQ20. Time-travel / version pinning exposure

**Where**: silent in v1 sections.

**Issue**: Lance supports manifest-version time travel natively. Pond does not say whether reads can pin to a version, or whether retention prunes old manifests.

**Lean**: defer (section 4). No v1 use case. Note as "available in Lance, not surfaced by pond" so future revisits do not treat it as new work.
