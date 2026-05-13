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
| Default embedding model | Qwen3-Embedding-0.6B via fastembed-rs (candle backend, 1024-dim with Matryoshka 32-1024, 32K context, Apache 2.0). Pond's embedding registry supports multi-model coexistence. |
| Output | single static binary via `cargo build --release`. |
| Code organization | Single Cargo crate. Strict module discipline separates substrate from consumer (sessions) code internally. Workspace split deferred until a second consumer (resources, archives) ships real code. |

No SQL anywhere. No additional database. No `lancedb` crate dependency. Personal pond = one binary, one local directory. Hosted pond = same binary, object-store URL.

### 2.1.1 Personal pond defaults

- **Bind**: `--host 127.0.0.1 --port 9797`. Env overrides: `POND_HOST`, `POND_PORT`. `--port 0` selects an OS-assigned free port. `--host 0.0.0.0` is accepted but logs a security notice at startup (personal pond is single-user; LAN exposure is opt-in).
- **Config**: `$XDG_CONFIG_HOME/pond/config.toml` (Linux and macOS; XDG-strict so cross-platform path stays consistent). TOML format. Schema is documented in this doc; `pond config --print-schema` emits a fully-annotated example.
- **Data**: `$XDG_DATA_HOME/pond/` (Linux and macOS; XDG-strict). Override via `--data-dir <path>` or `POND_DATA_DIR`.
- **Logs**: stdout for normal program output (search results, status), stderr for structured tracing diagnostics.
- **Platform scope**: Linux and macOS for v1. Windows not in scope.

### 2.2 Wire interface

The wire interface is the contract. Internal serde types evolve freely behind a projection layer.

- **Transport-agnostic handlers.** Every operation is a function `Json request -> Json response` (with optional streaming response for SSE). HTTP and MCP transports are thin adapters that dispatch to the same handler functions.
- **Request envelope.** Every request carries a `protocol_version` field at the top level. Value is a positive integer (`1`, `2`, ...); v1 ships `1`. Server validates the field and returns a typed error on unknown version.
- **Namespace per request.** Every wire request carries a `namespace: string?` field. Omitted means `"local"` (personal pond's hardcoded namespace). Hosted integrators populate it from their own pre-pond auth resolution. Pond performs no authn/authz of its own; the bucket's IAM is the storage boundary and the integrator's gateway is the application boundary. No HTTP middleware seam in pond core.
- **Schema evolution.** Additive-only within a major version. Adding fields is allowed; removing or retyping fields requires a major bump.
- **Published schema artifacts.** JSON Schema files generated from Rust types, committed to the repo, versioned with the binary. Pond's canonical types derive from Effect's `Prompt` shape (3.1), not from OTel GenAI. OTel's `gen-ai-input-messages.json`, `gen-ai-output-messages.json`, `gen-ai-tool-definitions.json`, and related schemas are noted as inspiration where shapes overlap (messages, tool definitions, finish reasons) but pond does not claim derivation. An OTel-compatible projection schema is deferred to section 4 for the day a consumer wants OTel observability interop.
- **HTTP shape.** `POST /v1/<operation>` with JSON body; RPC-shaped, no REST resource model. Streaming responses use SSE on `GET /v1/sessions/{session_id}/events?since=<event_id>`.
- **MCP shape.** Same operations exposed as MCP tools (`pond_search`, `pond_get`, `pond_ingest`, etc.). MCP `tools/list` returns the operation set.

### 2.3 Operational invariants

These are constraints every pond write and read must satisfy. Code review rules.

1. **Append-only writes.** Existing rows are never mutated. Updates produce new rows or new manifest versions.
2. **Deterministic primary keys.** Client-supplied IDs (UUIDv7 for sessions, content-hash for derived rows where applicable). All writes use Lance `merge_insert` on the PK so retries are no-ops.
3. **Retry-with-jitter on every Lance call.** Pond-side helper (3 attempts default, 300ms-5000ms exponential backoff, 0.2 jitter, per-operation labels). Connection-level retry on top.
4. **No cached table handles forever.** Pond is a long-lived server. Use Lance's `read_consistency_interval` so external writers are picked up. Default is keyed off the connection URI scheme: local filesystem = `0` (manifest reads are microseconds), object store (`s3://`, `s3+ddb://`, `gs://`, `az://`) = `5s` (caps manifest fetch overhead; acceptable lag for human-driven queries). Configurable override. Table handles may be reused between requests but must not be opened at startup and held without refresh.
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

  // User attribution: the shared-state scope this session belongs to.
  // Adapter-derived from the source's native mechanism (cwd for most harnesses;
  // explicit projectID for opencode; null for sources with no project notion
  // such as claude-managed-agents). Case-preserved verbatim. See 3.4.
  project?: string;

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
| `namespace_id` / `workspace_path` | Storage path (namespace) and `options.source.workspace_path` | Storage-routing and filesystem-harness execution context |

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
- **No turn-level metadata fields** (model, provider, finish_reason, tokens, response_id, error). Effect places these on Response-side Parts (`FinishPart`, `ResponseMetadataPart`, `ErrorPart`) that pond does not store. Sources record this metadata on their assistant turns; SourceAdapters route it to `options.<provider>.*` matching Effect's own declaration-merging pattern for `AssistantMessageOptions` (e.g. Anthropic-shape usage to `options.anthropic.usage.*`, OpenAI to `options.openai.*`). Cross-source normalization is a search/replay-layer concern (section 4), not a storage-shape concern. Canonical Message stays Effect-shaped.

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
| Per-message error (Effect's `ErrorPart`, opencode `AssistantError`) | `options.<provider>.error` per the source's wire format |
| Finish reason, token usage, model, provider, response_id (Effect's `FinishPart` and `ResponseMetadataPart`) | `options.<provider>.*` per the source's wire format (e.g. `options.anthropic.usage.input_tokens`, `options.openai.finish_reason`) |
| Tool approvals as side-table (opencode pattern) | Pond keeps as Parts (Effect's pattern), inside canonical |
| Per-Part timestamps (opencode's `text?:{start,end?}` on TextPart) | `options.source.time.*` on the Part |
| Synthetic orphan-tool-result sentinels (pi-mono inserts these at replay time) | Not in canonical storage; replay layer (deferred, section 4) generates them on demand |

### 3.2 Datasets

Pond stores the canonical types in 3.1 as four Lance datasets. Each dataset is a direct serialization of the corresponding canonical object - no projections, no promotions, no schema design beyond what 3.1 already defines. Open-ended fields (`options`, Part variant payloads) live as JSON; canonical scalars live as typed Lance columns; FilePart binary data uses Lance Blob v2.

#### 3.2.0 Lance write parameters

Applied to all four tables on create (via `lance::dataset::WriteParams`):

- `data_storage_version`: latest stable (2.2+). Per 2.1.
- `enable_v2_manifest_paths`: true (Lance default). Constant-time latest-manifest lookups.
- `enable_stable_row_ids`: true. Required so `_rowid` joins (parts to embeddings) survive compaction.
- `auto_cleanup`: `older_than: 30 days` for personal pond, `older_than: 90 days` for hosted. The longer-than-Lance-default window establishes pond as a viable recovery surface for scenarios where re-ingesting from source data isn't possible (sources deleted, API sessions expired, PKs corrupted such that `merge_insert` can't overwrite). Storage cost is negligible for append-only workloads (old manifests reference an immutable subset of fragments; nothing duplicated). Per the lance-format/lance maintainer guidance, `interval` (how often cleanup actually runs) follows Lance defaults.
- Unenforced primary keys declared at the schema level via `Field.unenforced_primary_key_position`. `merge_insert` defaults to using them, satisfying invariant 2 with no per-call boilerplate.

#### 3.2.1 sessions

One row per Session.

| Column | Type | Notes |
|---|---|---|
| id | Utf8 | PK pos=1 |
| parent_session_id | Utf8? | session fork pointer |
| parent_message_id | Utf8? | cut-point in parent session |
| source_agent | Utf8 | BTREE |
| created_at | timestamp_micros | source-recorded |
| project | Utf8? | user attribution per 3.1.3; BTREE (case-sensitive equality and prefix filter pushable) |
| options | Utf8 | JSON-serialized ProviderOptions |

#### 3.2.2 messages

One row per Message (any role).

| Column | Type | Notes |
|---|---|---|
| id | Utf8 | PK pos=1 |
| session_id | Utf8 | clustering pos=1; BTREE |
| timestamp | timestamp_micros | clustering pos=2; source-recorded |
| role | Utf8 | "system" / "user" / "assistant" / "tool"; BTREE |
| content | Utf8? | non-null only for system role (Effect Prompt convention: SystemMessage.content is a plain string); non-system content lives as Part rows in 3.2.3 |
| options | Utf8 | JSON-serialized ProviderOptions; response metadata (model, provider, finish_reason, tokens, response_id, error) lands under `options.<provider>.*` per the source's wire format (Effect's declaration-merging pattern); source/harness facts under `options.source.*` |

Clustering by `(session_id, timestamp)` keeps all messages of a session contiguous on disk for sequential session-walk reads.

#### 3.2.3 parts

One row per Part. Non-system message content lives here.

| Column | Type | Notes |
|---|---|---|
| id | Utf8 | PK pos=1 |
| message_id | Utf8 | clustering pos=1; BTREE |
| ordinal | Int32 | position within `message.content[]`; preserves the array order canonical to 3.1.4 |
| type | Utf8 | Part discriminator (`text` / `reasoning` / `file` / `tool_call` / `tool_result` / `tool_approval_request` / `tool_approval_response`); BTREE |
| options | Utf8 | JSON-serialized ProviderOptions |
| variant_data | Utf8 | JSON-serialized variant-specific fields (TextPart.text, ReasoningPart.text, ToolCallPart.{call_id, name, params, provider_executed}, ToolResultPart.{call_id, name, is_failure, result}, ToolApprovalRequestPart.{approval_id, tool_call_id}, ToolApprovalResponsePart.{approval_id, approved, reason}, FilePart.{media_type, file_name}) |
| data | Struct&lt;data: LargeBinary?, uri: Utf8?&gt; with `ARROW:extension:name = lance.blob.v2` | FilePart.data only; null on other Part types. Lance Blob v2 natively carries the inline-bytes-OR-uri union from 3.1.5. Blobs above `blob_pack_file_size_threshold` (Lance default 1 GiB) auto-routed to dedicated `.blob` pack files within the dataset. |

Search-layer derived columns (BM25 FTS targets) and FilePart content-hashing are owned by 3.3 and section 4, not by canonical storage.

#### 3.2.4 embeddings

One row per (Part, embedding model, chunk).

| Column | Type | Notes |
|---|---|---|
| part_id | Utf8 | PK pos=1 |
| model_id | Utf8 | PK pos=2; free-form string. Adapter may include a revision suffix (e.g. `BAAI/bge-small-en-v1.5@<hf-sha>`, `Qwen/Qwen3-Embedding-0.6B@abc123`) when strict cache invalidation across upstream weight updates is required. Without a suffix, re-embeds with the same `model_id` and `text` overwrite prior rows. Pond does not parse this field. |
| chunk_index | Int32 | PK pos=3 |
| vector | FixedSizeList&lt;Float32, N&gt; | dim N is per-model |

Vector index (IVF_PQ default per Lance auto-index) on `vector`. Multi-model coexistence: multi-row-per-part within one table while dims match; a second embeddings table per model is the activation path when a model with a different dim ships.

Chunking: token-aware, applied at embed-worker time. The chunker uses the model's own tokenizer (the HuggingFace `tokenizers` crate; already transitive via fastembed-rs) so chunk budgets match what the embedding model actually sees. Chunks are deterministic: same `(model_id, text)` always produces the same chunks, keeping the PK stable across retries. Chunk size and overlap are per-model parameters declared in pond's embedding registry; for Qwen3-Embedding-0.6B (the v1 default), values are 1024 tokens per chunk with 128 tokens overlap. Chunk size is calibrated for retrieval-quality plateau (~1K-2K tokens), not model-context capacity (Qwen3's 32K window is far larger than retrieval needs - longer chunks dilute the embedding signal). Output dim is the Matryoshka-full 1024 by default.

v1 scope: only `TextPart.text` and `ReasoningPart.text` are embedded. Other Part types (FilePart, ToolCallPart, ToolResultPart, approvals) are not vector-indexed in v1; BM25/FTS coverage is a 3.3 concern.

### 3.3 Search surface

Hybrid (vector + BM25 + RRF) by default. Filters mirror the kb tool surface: `project`, `conversation_id`, `from_date` / `to_date`, `role`, `min_score`, `boost_recent`, `group_by_conversation`, `include_tool_results`, `include_thinking`, `limit`.

`project` is a canonical Session field (3.1.3) stored case-sensitive verbatim. The filter accepts `project: <value>` plus `project_match: "exact" | "contains"` (default `exact`). Exact equality pushes down to a BTREE prefilter on the scan; contains falls back to expression-engine substring match. Case-insensitive search is the caller's responsibility (fold case before submitting); pond does not normalize at storage or filter time.

`boost_recent` is a boolean on the search request (default `true`). When set, an additive exponential-decay boost is applied to each result's base score: `boost = 0.2 * exp(-age_seconds / 604800)` where `age_seconds` is `now - message.timestamp` and `604800` is 7 days in seconds. The boost caps at `+0.2` (at `age = 0`) and decays to near-zero past a few weeks. Formula inherited verbatim from kb (`claude-kb/src/claude_kb/search.py:_apply_recency_boost`) for behavioral parity; constants are not empirically tuned by pond and should be revisited when retrieval-quality measurement is available.

`group_by_conversation` is a boolean on the search request (default `false`). When `true`, results collapse to one summary object per `session_id`, with fields: `session_id`, `project`, `first_timestamp` and `last_timestamp` (min/max across matching messages), `message_count` (total messages in the session, via a separate count query against the `messages` table - NOT the count of matches), `preview` (text extracted from the first matching Part with text content), and `best_score` (`max(score)` across matches in the session). Summaries are sorted by `best_score` descending then limited. Behavior inherited from kb's `_group_by_conversation` for parity.

Retrieval modes: single message, single message with N thread-context messages above and below, full conversation, conversation up to a message.

### 3.4 Ingest surface

`SourceAdapter` is pond's per-source plug-in trait. v1 ships the Claude Code adapter; section 4 lists the others on the roadmap.

```rust
// IngestEvent is the canonical-shape unit the adapter emits.
// Each variant carries the locked-3.1 canonical type.
pub enum IngestEvent {
    Session(Session),
    Message(Message),
    Part(Part),
}

pub trait SourceAdapter: Send + Sync {
    /// Adapter-specific handle that identifies one session within the source.
    /// Opaque to pond core. Examples:
    ///   - claude-code, codex, openclaw, pi: PathBuf to the .jsonl file
    ///   - claude-managed-agents: session id String
    ///   - opencode: (project_dir, session_id) tuple
    ///   - claude-app: (metadata_path, audit_path) pair
    type SessionRef: Send + Sync;

    /// Typed error enum (per-adapter, `thiserror`-derived).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Enumerate sessions available from this source. Bounded memory; yields
    /// one `SessionRef` at a time so adapters can scan large directories or
    /// paginate remote APIs without buffering.
    fn discover(&self)
        -> impl Stream<Item = Result<Self::SessionRef, Self::Error>> + Send;

    /// Decode one session's source-shape data into canonical `IngestEvent`s.
    /// The adapter owns source-format-to-canonical projection: all source-
    /// specific facts that have no canonical home land in `options.<provider>.*`
    /// or `options.source.*` per 3.1.1. Malformed input surfaces as
    /// `Result::Err` (no silent drops, invariant 5).
    fn decode(&self, session: Self::SessionRef)
        -> impl Stream<Item = Result<IngestEvent, Self::Error>> + Send;
}
```

Async + streaming both methods. Stream-based design gives pull-driven backpressure for free (pond core controls flow), bounds memory for huge JSONL files, and maps cleanly to all 8 source shapes pond plans to absorb (see `docs/references/session-samples/`). Adapter implementations use tokio I/O primitives (`tokio::fs`, `tokio::io::BufReader::lines` for JSONL, the `serde` / `serde_json` stack for parsing).

Each adapter is responsible for populating `Session.project` from its source's native attribution mechanism. Convention per source: claude-code, codex, pi, openclaw, nanoclaw populate from `cwd`; opencode populates from `projectID`; claude-app from the primary folder in `userSelectedFolders[]`; claude-managed-agents leaves it null (no source notion) or uses an attached `memory_store_id` when present. Adapter discretion is the only mechanism in v1; no user-facing override flag.

Per-event append granularity, not batched. Pond core consumes the `decode` stream and writes one Lance row per `IngestEvent` via `merge_insert` keyed on the canonical PKs (`Session.id`, `(session_id, Message.id)`, `(message_id, Part.id)`). Re-ingest is idempotent (invariant 2): re-reading an already-ingested session is a no-op for matched rows. This pattern is validated against `lance-format/cocoindex-lancedb-demo` (uses the same `merge_insert`-on-stable-PK approach for incremental ingest). Discover-time efficiency optimizations (per-source mtime+size checkpoint table, modeled on `lancedb/lancedb-claw`'s engine state-row pattern) are deferred until scan cost becomes measurable. End-of-session import is the v1 default; live-write is deferred (section 4) and activates additively by adding a `follow(SessionRef) -> impl Stream<Item = Result<IngestEvent>>` method (or a separate `LiveSourceAdapter` trait) - same Stream shape, infinite stream.

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

Admin CLI verbs (for recovery / inspection, not user-facing search):

- `pond versions list` - enumerate Lance manifest version history (version_id, commit timestamp, fragment summary).
- `pond checkout <version>` - open a read-only handle pinned to that version for inspection (read-only; does not replace current state).
- `pond restore <version> --force` - dangerous: rolls the dataset back to a prior version. Requires `--force`. Used for recovery from corrupted ingest, bad adapter shipments, or PK-mangled writes that can't be fixed by re-ingest. Bounded by the 2.3 invariant 4 retention window (30 days personal / 90 days hosted).

---

## 4. Deferred (yes-later, with activation conditions)

Each entry: what it is, why deferred, activation condition. None require schema migrations or call-site changes elsewhere when activated.

- **Resources application.** Per-namespace knowledge-base files (blobs plus metadata) as a second consumer alongside sessions. Activation: a concrete second consumer that needs blob+metadata storage outside the sessions schema. Adding it is mechanical: new Lance table, new schema, same connection.
- **Cross-provider replay engine.** Re-projecting canonical Parts into provider-specific request shapes (Anthropic, OpenAI, Bedrock, Gemini, etc.). Activation: first integrator demand. Pond canonical types are stored as future-proofing; replay is projection over Parts, not a schema concern. Activation also gates the full pi-mono cross-provider conformance test matrix as the acceptance gate.
- **Live-write tools.** `pond_commit`, `pond_session_open` (and HTTP equivalents). Streaming events written as they arrive instead of via retrospective ingest. Activation: first runtime that wants to plug pond in mid-session. v1 ingest is per-event already; live-write is a transport into the same handlers. Activation-time constraint (locked): streaming-event variants (Effect's `text-start`/`text-delta`/`text-end`, `reasoning-*`, `tool-params-*`) do not enter `parts.lance`; only the assembled final Part is persisted on turn completion. This preserves the append-only invariant and keeps OCC contention rare. Aligns with pi-mono, Effect, and opencode (all of which persist assembled state only).
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
- **Wire-API-surfaced time-travel / friendly historical queries.** Lance natively versions every commit and supports `Dataset::checkout(version)`, `list_versions`, tags. v1 surfaces these via the admin CLI verbs in 3.6 (`pond versions list`, `pond checkout`, `pond restore`) and the 30/90-day retention window in 2.3 invariant 4 - sufficient for operational recovery. Activation: friendlier UX needs (`--as-of "yesterday at 3pm"` time-string queries, audit endpoints returning historical search results, automated rollback workflows). When activated, adds `version: int?` to read-side wire requests and a `pond_versions` operation; no schema changes since Lance owns the mechanism.

---

## 5. Open Questions

Workspace for unresolved items. When a question is decided, its answer moves into the relevant section above and the question is removed entirely from this list. Goal: an empty section 5. Git history preserves the trail of resolved questions.

