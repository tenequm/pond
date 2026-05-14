# Pond - Design v2

> Status: sections 1-4 are the source of truth. Section 5 (Open Questions) is empty; all v1 design decisions are locked into the relevant sections above. Git history preserves the trail of resolved questions (OQ1-OQ10).

---

## 1. What this is + non-goals

Pond is a Rust crate that wraps `lance-format/lance` directly with sessions-aware ingest, storage, and a JSON wire interface. One binary. Two transports: HTTP and MCP. Two deployments: a personal pond on a laptop, or a multi-tenant backend for hosted agent infrastructure.

Lance (and the file format underneath it) is the substrate. Pond does not introduce a separate "substrate layer" of its own and does not depend on the `lancedb` wrapper crate. Pond owns canonical session types, source adapters, the wire schema, the HTTP and MCP transports, and the conventions for using Lance consistently across deployments.

### 1.1 v1 scope

- **One application: sessions.** Lossless ingest, storage, and hybrid search of agentic-client sessions.
- **v1 source: Claude Code.** Other clients (Codex, OpenCode, Cursor, aider, Gemini CLI, others) on roadmap.
- **Two transports day 1**:
  1. **HTTP+JSON** (primary). RPC-shaped over `POST /v1/<operation>`. Streaming reads via SSE.
  2. **MCP** (specialization). Same handlers, exposed as MCP tools and resources over two channels: streamable-HTTP at the `/mcp` route of the `pond serve` HTTP server, and a stdio JSON-RPC 2.0 server started by `pond mcp`.
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
| Storage and search engine | `lance-format/lance` crates direct: `lance`, `lance-table`, `lance-io`, `lance-encoding`, `lance-index`, `lance-namespace`, `lance-namespace-impls`. Pinned via git dependency to tag `v7.0.0-beta.8` (the verified API surface; the 7.x beta line is not published to crates.io, where the latest stable is 6.0.0). No `lancedb` crate dependency. |
| Lance file format | `stable` (2.2+) for new datasets. Blob v2 for FilePart payloads. |
| Object store backends | `object_store` via Lance: local filesystem, S3 (native conditional writes), GCS, Azure. |
| HTTP server | axum (tokio-native, JSON-first, SSE built in). |
| MCP server | rmcp (official Anthropic Rust SDK), wrapping the same handlers as HTTP. |
| Wire format | JSON. Single evolving schema with top-level `protocol_version` field. Additive-only changes; formal `v2` only on breaking changes. |
| Default embedding model | Qwen3-Embedding-0.6B via fastembed-rs (candle backend, behind fastembed-rs's opt-in `qwen3` Cargo feature; fixed 1024-dim output, 32K context, Apache 2.0). The candle backend runs on the Metal GPU on macOS (target-gated `metal` feature, day-one), on CPU by default on Linux, and on an NVIDIA GPU on Linux when built with the opt-in `cuda` feature; pond selects the device via candle's `metal_if_available` / `cuda_if_available` helpers and passes the resulting `candle-core` `Device` into `Qwen3TextEmbedding::from_hf`. Qwen3 is NOT in fastembed-rs's standard `EmbeddingModel` enum - it is reached via the separate `Qwen3TextEmbedding::from_hf` API, distinct from the ort-backed enum models. Pond's embedding registry is config-driven (TOML `[[embeddings.models]]`, validated against pond's own known-model set, not fastembed-rs's enum); built-in default ships in the binary. Multi-model coexistence in one table while dims match. |
| Output | single static binary via `cargo build --release`. |
| Code organization | Single Cargo crate. Strict module discipline separates substrate from consumer (sessions) code internally. Workspace split deferred until a second consumer (resources, archives) ships real code. |

No SQL anywhere. No additional database. No `lancedb` crate dependency. Personal pond = one binary, one local directory. Hosted pond = same binary, object-store URL.

### 2.1.1 Personal pond defaults

- **Bind**: `--host 127.0.0.1 --port 9797`. Env overrides: `POND_HOST`, `POND_PORT`. `--port 0` selects an OS-assigned free port. `--host 0.0.0.0` is accepted but logs a security notice at startup (personal pond is single-user; LAN exposure is opt-in).
- **Config**: `$XDG_CONFIG_HOME/pond/config.toml` (Linux and macOS; XDG-strict so cross-platform path stays consistent). TOML format. Schema is documented in this doc; `pond config --print-schema` emits a fully-annotated example. Key blocks: `[[embeddings.models]]` (3.2.4 registry; built-in default if absent), `[embeddings.overrides.<namespace>.<model_id>]` (per-namespace embedding tuning), `[maintenance]` (3.2.0 background cleanup + index optimization).
- **Data**: `$XDG_DATA_HOME/pond/` (Linux and macOS; XDG-strict). Override via `--data-dir <path>` or `POND_DATA_DIR`.
- **Logs and output - two channels, each with one owner**:
  - *Diagnostics channel*: all structured logging (spans, debug/info/warn/error) goes through `tracing`. The `tracing-subscriber` is initialized exactly once at process start (env-filter via `RUST_LOG` / `POND_LOG`) and always writes to **stderr**. No module configures logging itself.
  - *Results channel*: the actual output of a verb (search results, status) is not logging - it is written by a single `output` helper. For `pond serve` and the CLI verbs it goes to **stdout** (human-readable, or JSON where the verb is machine-facing). For `pond mcp` (the stdio MCP server) stdout is reserved exclusively for JSON-RPC frames, so the `output` helper emits the result as a JSON-RPC frame instead. `pond serve` has no stdout restriction because its MCP channel is the `/mcp` HTTP route, not stdout.
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
4. **No cached table handles forever.** Pond is a long-lived server. The `lance` crates expose no `read_consistency_interval` option (that is a `lancedb`-wrapper concept, absent from the `lance` Rust API), so pond owns the staleness window itself: each cached `Dataset` handle records its last-refresh time, and pond calls `Dataset::checkout_latest()` (a cheap manifest read) to pick up external writers before serving a read whenever the handle is older than the configured interval. The interval is keyed off the connection URI scheme: local filesystem = `0` (always refresh; manifest reads are microseconds), object store (`s3://`, `s3+ddb://`, `gs://`, `az://`) = `5s` (caps manifest fetch overhead; acceptable lag for human-driven queries). Configurable override. Table handles may be reused between requests but must not be opened at startup and held without refresh.
5. **No silent drops.** Malformed input surfaces with line offset and error context. Ingest fails closed by default.
6. **Opaque IDs, not paths.** `namespace_id`, `workspace_id`, `project`, `agent_id` are opaque strings. The Claude Code SourceAdapter decodes path-encoded session directories once at ingest and stores the decoded values; readers never re-parse.
7. **No SQL.** Lance scalar predicates and search APIs are the only query mechanism.
8. **Encryption is operational.** Bucket SSE plus filesystem encryption. No application-level crypto, no `is_encrypted` columns, no KeyProvider.
9. **Schema versioning at the dataset level.** Lance manifest version plus dataset-level metadata key. No per-row `schema_version` columns.

### 2.4 Concurrency model

Stateless workers. Multiple pond processes can write concurrently to the same namespace. Lance OCC handles append conflicts via manifest versioning. Content-addressed payloads make worker crashes and retries idempotent.

No external coordinator on plain S3 (native conditional writes since mid-2025). GCS and Azure have native atomic conditional writes. Local filesystem uses Lance's internal commit lock.

No in-process write queue. Concurrent HTTP requests dispatch to handlers in parallel. Lance OCC plus retry-with-jitter resolves contention. The single-lane gateway antipattern observed in OpenClaw deployments (forcing 4-subprocess fanout for parallelism) is explicitly rejected.

### 2.5 Search defaults

- **Hybrid by default.** Every search runs vector kNN plus BM25 FTS, merged with RRF (`k=60`, no weighting), unless the request specifies otherwise.
- **Wire-level override.** Optional `search_mode` enum field on search requests: `hybrid` (default), `vector`, `fts`. Optional `rrf_k` integer field overrides the default `k`.
- **Both indexes always present.** FTS index on `messages.search_text` (3.2.2) and vector index on `embeddings.vector` (3.2.4) are created at table creation. Both retrievers operate at message granularity over the same input string; the schema does not branch on whether an embedding model is configured. Turning a model on or off does not require a schema migration.
- **Search corpus is fixed by concatenation policy.** What gets indexed is determined by 3.3.1 (TextPart, ToolCallPart name+string-leaf params, FilePart name/media/url; reasoning, tool results, approvals, system messages excluded). Excluded Parts remain canonically stored and retrievable via `pond_get` (3.6).

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
  //
  // Validation: trimmed at ingest; rejected if empty after trim or if it contains
  // Unicode control characters (Cc, Cf). No casing rule, no length cap, no
  // kebab-case enforcement. Preserved verbatim; filter equality is case-sensitive.
  // Adapter selection is a separate concern (CLI flag --from <name>, code-level
  // registry); the source_agent string is set by the adapter at canonical
  // projection time and stored as-is.
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
- `auto_cleanup`: `older_than: 30 days` for personal pond (URI scheme: local filesystem), `older_than: 90 days` for hosted (URI scheme: `s3://`, `gs://`, `az://`). The longer-than-Lance-default window establishes pond as a viable recovery surface for scenarios where re-ingesting from source data isn't possible (sources deleted, API sessions expired, PKs corrupted such that `merge_insert` can't overwrite). Storage cost is negligible for append-only workloads (old manifests reference an immutable subset of fragments; nothing duplicated). Cleanup only removes old Lance manifest versions and any fragments referenced only by them; it does NOT delete logical rows. Stored sessions/messages/parts/embeddings accumulate indefinitely until an operator runs an explicit delete operation (not in v1 wire surface). The retention window IS the recovery window for `pond restore`.
- Unenforced primary keys declared at the schema level via `Field.unenforced_primary_key_position`. `merge_insert` defaults to using them, satisfying invariant 2 with no per-call boilerplate.

Maintenance execution: a background tokio task spawned by `pond serve` runs two operations per interval on each table:

1. `Dataset::cleanup_old_versions(older_than)` to remove old manifest versions and unreferenced fragments per the `auto_cleanup` window above. Default `delete_unverified: false` (in-flight write files newer than the verification threshold are skipped, preventing a cleanup-vs-write race; see `rust/lance/src/dataset/cleanup.rs:357-363`).
2. `Dataset::optimize_indices(append)` to extend each scalar / FTS / vector index `fragment_bitmap` to fragments appended since the last build. Without this, freshly-ingested data has degraded filter pushdown until the next interval (Lance falls back to full-scan-with-predicate on uncovered fragments per `rust/lance/src/io/exec/filtered_read.rs:712-747`).

Interval default: 6h. Both operations logged at info level (versions removed, bytes reclaimed, fragments newly indexed, duration). Failures logged at warn and retried at the next interval; maintenance failures do not crash `pond serve`. A `pond maintenance` CLI verb runs the same logic one-shot (for cron-style ops or systems without long-running `pond serve`); supports `--older-than <duration>` override for ad-hoc reclaim and `--skip-cleanup` / `--skip-optimize` flags to run either half. Both paths are safe against concurrent reads and writes (Lance OCC; Append-vs-Append commutes per `rust/lance/src/io/commit/conflict_resolver.rs:873-899`). Multiple pond processes running maintenance against the same dataset converge harmlessly. Configuration lives under `[maintenance]` in `config.toml` (`enabled`, `interval`, `retention`); CLI verb runs regardless of `enabled`.

#### 3.2.1 sessions

One row per Session.

| Column | Type | Notes |
|---|---|---|
| id | Utf8 | PK pos=1 |
| parent_session_id | Utf8? | session fork pointer |
| parent_message_id | Utf8? | cut-point in parent session |
| source_agent | Utf8 | NOT NULL; Bitmap (low cardinality per 3.4 canonical-strings table) |
| created_at | timestamp_micros | source-recorded |
| project | Utf8? | user attribution per 3.1.3; BTREE (case-sensitive equality and prefix filter pushable) |
| options | Utf8 | JSON-serialized ProviderOptions |

#### 3.2.2 messages

One row per Message (any role).

| Column | Type | Notes |
|---|---|---|
| session_id | Utf8 | PK pos=1; clustering pos=1; BTREE |
| id | Utf8 | PK pos=2; unique within session (source IDs may be locally-scoped per 3.1.1) |
| timestamp | timestamp_micros | clustering pos=2; source-recorded; BTREE |
| role | Utf8 | "system" / "user" / "assistant" / "tool"; Bitmap (4-value low-cardinality column; BTREE would prune nothing because every page's [min,max] covers every value) |
| source_agent | Utf8 | NOT NULL; denormalized from `sessions.source_agent` at ingest by pond core; Bitmap (typically 5-20 distinct values across the corpus). Filter pushdown surface only; `sessions` is the authoritative source for reads outside of search |
| project | Utf8? | denormalized from `sessions.project` at ingest by pond core; BTREE (moderate cardinality, supports exact and prefix predicates). Filter pushdown surface only; `sessions` is the authoritative source for reads outside of search |
| content | Utf8? | non-null only for system role (Effect Prompt convention: SystemMessage.content is a plain string); non-system content lives as Part rows in 3.2.3 |
| search_text | Utf8? | indexed retrieval surface; populated at ingest by pond core via the concatenation policy in 3.3.1. Non-null for user and assistant roles when at least one indexable Part exists; null for system and tool roles. FTS-indexed and consumed by the embedding worker (same string feeds both retrievers). |
| options | Utf8 | JSON-serialized ProviderOptions; response metadata (model, provider, finish_reason, tokens, response_id, error) lands under `options.<provider>.*` per the source's wire format (Effect's declaration-merging pattern); source/harness facts under `options.source.*`. Stored as JSON string (not Lance Struct) for additive-only evolution: any new provider key requires zero schema change. Empty options serialize as `"{}"` (no NULLs). Hot keys may be promoted to dedicated typed sibling columns additively (e.g. `messages.input_tokens Int64?` populated forward at ingest); the JSON column stays intact, promotion is reversible. |

Composite PK `(session_id, id)` lets pond preserve source-supplied IDs verbatim (per 3.1.1) without requiring them to be globally unique across sources or sessions. Clustering by `(session_id, timestamp)` keeps all messages of a session contiguous on disk for sequential session-walk reads.

Denormalized columns (`source_agent`, `project`) are immutable post-write: invariant 1 (append-only) plus pond core writers stamping them once at ingest from the Session event buffered per the 3.4 ordering contract. They are not user-writable via the wire surface (see 3.6.4). If a `sessions` row's `project` ever needs correcting, the recovery path is re-ingest, not in-place column update - matching the universal pattern observed across production Lance/LanceDB applications.

#### 3.2.3 parts

One row per Part. Non-system message content lives here.

| Column | Type | Notes |
|---|---|---|
| message_id | Utf8 | PK pos=1; clustering pos=1; BTREE |
| id | Utf8 | PK pos=2; unique within message (Part IDs in source data, where present, are array-local; otherwise SourceAdapter-generated UUIDv7 per 3.1.1) |
| ordinal | Int32 | position within `message.content[]`; preserves the array order canonical to 3.1.4 |
| type | Utf8 | Part discriminator (`text` / `reasoning` / `file` / `tool_call` / `tool_result` / `tool_approval_request` / `tool_approval_response`); Bitmap (7-value low-cardinality column) |
| options | Utf8 | JSON-serialized ProviderOptions |
| variant_data | Utf8 | JSON-serialized variant-specific fields (TextPart.text, ReasoningPart.text, ToolCallPart.{call_id, name, params, provider_executed}, ToolResultPart.{call_id, name, is_failure, result}, ToolApprovalRequestPart.{approval_id, tool_call_id}, ToolApprovalResponsePart.{approval_id, approved, reason}, FilePart.{media_type, file_name}) |
| data | Struct&lt;data: LargeBinary?, uri: Utf8?&gt; with `ARROW:extension:name = lance.blob.v2` | FilePart.data only; null on other Part types. Lance Blob v2 carries the inline-bytes-OR-uri union from 3.1.5 as a nullable-field struct (exactly-one-of semantics), not an Arrow Union type. Blobs above the per-field `BLOB_DEDICATED_SIZE_THRESHOLD` (Lance default 4 MB) are auto-routed to dedicated `.blob` pack files within the dataset; the pack-file size ceiling is the hardcoded `PACK_FILE_MAX_SIZE` constant (1 GiB), not a configurable parameter. |

No search-layer columns live on `parts` - retrieval is message-level and `search_text` lives on `messages` (3.2.2). FilePart content-hashing is deferred (section 4).

#### 3.2.4 embeddings

One row per (Message, embedding model). Granularity is the Message - not the Part, and not a sub-message chunk: retrieval returns messages (per 3.3) and vector + FTS agree on row identity for RRF.

| Column | Type | Notes |
|---|---|---|
| message_id | Utf8 | PK pos=1; BTREE (enables `message_id IN (...)` prefilter for cross-table joins when needed) |
| model_id | Utf8 | PK pos=2; free-form string. Adapter may include a revision suffix (e.g. `Qwen/Qwen3-Embedding-0.6B@abc123`) when strict cache invalidation across upstream weight updates is required. Without a suffix, re-embeds with the same `model_id` and `text` overwrite prior rows. Pond does not parse this field. |
| vector | FixedSizeList&lt;Float32, N&gt; | dim N is per-model |
| session_id | Utf8 | NOT NULL; denormalized from `messages.session_id` at ingest by pond core; BTREE (high cardinality, supports `session_id = X` prefilter on vector kNN) |
| source_agent | Utf8 | NOT NULL; denormalized from `messages.source_agent` at ingest by pond core; Bitmap (low cardinality) |
| project | Utf8? | denormalized from `messages.project` at ingest by pond core; BTREE |
| role | Utf8 | NOT NULL; denormalized from `messages.role` at ingest by pond core; Bitmap (4-value column; only `user` and `assistant` rows actually exist in this table since system/tool produce no embeddings per 3.3.1, but the column is declared for filter-pushdown completeness) |
| timestamp | timestamp_micros | denormalized from `messages.timestamp` at ingest by pond core; BTREE (supports `from_date`/`to_date` prefilter on vector kNN) |

Denormalized columns are immutable post-write (same rule as 3.2.2). They exist to enable single-stage filter pushdown for vector kNN without cross-table joins (Lance has no relational join planner with the crate stack pond uses; see 3.3). Dictionary encoding for low-cardinality denorm columns (`source_agent`, `role`) is auto-detected by Lance v2.2+ at fragment-write time (`rust/lance-encoding/src/encodings/logical/primitive.rs:4869-4934`); small ingest batches under the 100-row threshold under-encode until compaction merges fragments (handled by the maintenance task per 3.2.0).

Input text for embedding is `messages.search_text` (3.2.2), produced by the concatenation policy in 3.3.1. Messages with NULL `search_text` (system and tool rows) produce no embedding rows.

Vector index: IVF_PQ on `vector`. Distance: cosine (Qwen3 vectors are L2-normalized by the model; matches its training objective). Defaults for the 1024-dim Qwen3-Embedding-0.6B case: `num_partitions = max(32, min(4096, round(sqrt(num_rows))))` (recomputed at each index build); `num_sub_vectors = 64` (16-float PQ codebooks); `num_bits = 8` (Lance default, 256 centroids per codebook). Activation threshold: 10,000 rows in the embeddings table - below that, queries use a flat exact scan (Lance handles this transparently when no index exists). When a future model with a different `dim` lands, the chosen `num_sub_vectors` for that model lives in the registry entry; no universal formula is committed in code.

Index rebuilds: auto-triggered by Lance when fragments added since last build exceed the auto-index threshold; manual via `pond optimize` is not exposed in v1 (auto-trigger only).

Multi-model coexistence: multiple rows per message within one table - one per model - while dims match; a second embeddings table per model is the activation path when a model with a different dim ships.

Embedding granularity: one vector per message, no chunking. Measured against the real `~/.claude/projects/` corpus (689,702 messages), `search_text` is 20 tokens at the median and ~98% of messages are under 1024 tokens; chunking would multiply `embeddings` rows for the ~2% that exceed a chunk size only for RRF to dedup them straight back to one hit per message - work done purely to be undone. The embedding stage of `pond ingest` embeds each message's `search_text` whole, truncated to a `max_embed_tokens`-token prefix (default 4096): `max_embed_tokens` is passed as the `Qwen3TextEmbedding::from_hf` tokenizer `max_length`, so fastembed-rs truncates input past it before inference - pond owns no tokenizer of its own. The cap is a model-cost bound on the ~1% of messages past p99 (and the rare multi-100k-token outlier), not a retrieval-quality knob: the full uncapped `search_text` is still indexed by FTS (3.3), and the full message is always retrievable via `pond_get` (3.6.3). Truncation is deterministic - the same `search_text` always yields the same capped input - keeping the embedding stable across retries and re-ingest a no-op. Output dim is the model's fixed 1024: fastembed-rs exposes no Matryoshka dimension truncation, so the full hidden size is stored as-is. If a future need for variable dims arises, pond truncates and re-normalizes the vector itself (deferred, section 4).

Embedding model registry: configuration-driven (TOML in `config.toml` under `[[embeddings.models]]`). Built-in defaults shipped in the binary so a pond instance with no user config still works. User config adds or overrides entries. Each entry: `{ id, dim, max_embed_tokens, num_sub_vectors, distance, normalize, default }` (where `id` doubles as the HuggingFace repo passed to the loader, minus any `@revision` suffix). Validated at startup against pond's own known-model set (fastembed-rs's standard `EmbeddingModel` enum does not include Qwen3 - the v1 default is loaded via fastembed-rs's separate `Qwen3TextEmbedding::from_hf` path behind the `qwen3` feature; ort-backed enum models are a distinct registry path); pond fails to start with a clear error on an unknown model code, dim mismatch, unsupported distance, or zero `default = true` entries. Adding another model pond already knows how to load is config-only (no release); adding a model on a loader pond doesn't yet support, or a remote provider (deferred per section 4), still requires code. Per-namespace tunable overrides via `[embeddings.overrides.<namespace>.<model_id>]` are limited to `max_embed_tokens`, `num_sub_vectors`; the immutable fields (`dim`, `distance`, `normalize`) cannot be overridden because they would invalidate stored vectors.

### 3.3 Search surface

Hybrid (vector + BM25 + RRF) by default, at message granularity (vector index keyed on message_id per 3.2.4; FTS index on `messages.search_text` per 3.2.2). Filters: `project`, `session_id`, `from_date` / `to_date`, `role`, `source_agent`, `min_score`, `boost_recent`, `group_by_conversation`, `limit`. The kb-inherited `include_tool_results` / `include_thinking` toggles are NOT search filters; they live on `pond_get` (3.6) and govern which Part types are returned at retrieval time. The search corpus is fixed by the concatenation policy in 3.3.1 - what isn't in `search_text` cannot be found via search.

`project` is a canonical Session field (3.1.3) stored case-sensitive verbatim, denormalized onto `messages` and `embeddings` for filter pushdown (3.2.2 / 3.2.4). The filter accepts `project: <value>` plus `project_match: "exact" | "contains" | "is_null"` (default `exact`).

- `exact`: pushes down to the BTREE prefilter on the queried table's `project` column.
- `contains`: falls back to expression-engine substring match (no index pushdown).
- `is_null`: emits `project IS NULL` (Lance BTREE supports this natively per `rust/lance-index/src/scalar/btree.rs:1537, 824-830`); the `project` value field is ignored when `is_null` is set. Required for filtering source harnesses that have no project notion (claude-managed-agents per 3.4).

Case-insensitive search is the caller's responsibility (fold case before submitting); pond does not normalize at storage or filter time. Same convention applies to `source_agent` and `session_id` (without `is_null`; both are NOT NULL).

`role` accepts a single value (`"user"` | `"assistant"` | `"system"` | `"tool"`). System and tool values are accepted on the wire but always return empty (those rows have NULL `search_text` and no embeddings per 3.2.2 / 3.2.4).

`boost_recent` is a boolean on the search request (default `true`). When set, an additive exponential-decay boost is applied to each result's base score: `boost = 0.2 * exp(-age_seconds / 604800)` where `age_seconds` is `now - message.timestamp` and `604800` is 7 days in seconds. The boost caps at `+0.2` (at `age = 0`) and decays to near-zero past a few weeks. Formula inherited verbatim from kb (`claude-kb/src/claude_kb/search.py:_apply_recency_boost`) for behavioral parity; constants are not empirically tuned by pond and should be revisited when retrieval-quality measurement is available.

`group_by_conversation` is a boolean on the search request (default `false`). When `true`, results collapse to one summary object per `session_id`, with fields: `session_id`, `project`, `source_agent`, `first_timestamp` and `last_timestamp` (min/max across matching messages), `message_count` (total messages in the session, via a separate count query against the `messages` table - NOT the count of matches), `preview` (truncated `search_text` from the best-scoring matched message), and `best_score` (`max(score)` across matches in the session). Summaries are sorted by `best_score` descending then limited.

Filter pushdown: every search filter column is colocated on the queried table via the denormalization in 3.2.2 (messages: `project`, `source_agent`, `role`, `session_id`, `timestamp`) and 3.2.4 (embeddings: same set). The FTS query on `messages.search_text` and the vector kNN on `embeddings.vector` each push their predicates into the table-level scalar indexes (BTREE for `project`/`session_id`/`timestamp`, Bitmap for `source_agent`/`role`) before retriever ranking - produces correct top-k without postfilter underrun and without cross-table joins. RRF merges on `message_id` - with one embedding row per message per model, vector results are already message-unique (no per-chunk dedup). `min_score` is applied postfilter after RRF and recency boost (not a Lance-pushable predicate). Implementation note: prefilter pushdown is **opt-in** on the raw `lance` `Scanner` - it defaults to `false` (only the `lancedb` wrapper defaults it to `true`, which is why the LanceDB docs describe prefilter as "the default"; that statement does not hold for the lance-direct stack). Pond MUST call `Scanner::prefilter(true)` on every vector kNN and FTS query; without it Lance silently postfilters in memory and ignores the scalar indexes entirely (recall loss, fewer than `limit` results returned). This is load-bearing: an integration test on real data must assert via `Scanner::explain_plan` that the scalar predicate appears as a `ScalarIndexQuery` / `ScalarIndexExec` node (prefilter pushdown) and not as a top-level `FilterExec` (postfilter).

Retrieval modes (handled by `pond_get` per 3.6, not search): single message, single message with N thread-context messages above and below, full conversation, conversation up to a message.

#### 3.3.1 Indexed content and concatenation policy

`messages.search_text` is populated at ingest by a single pond-core function applied uniformly to every canonical Message. The function is the only knob; per-source customization is rejected to keep search corpus shape predictable across all adapters.

Per-role concatenation:

| Role | search_text content |
|---|---|
| system | NULL (not indexed; retrievable via `pond_get`) |
| user | TextPart.text values, plus `FilePart.file_name`, `FilePart.media_type`, and the URL string when `FilePart.data` is the `url` variant. All concatenated with newline separators in `ordinal` order. |
| assistant | Above plus `ToolCallPart.name` and the recursive string-valued leaves of `ToolCallPart.params` (numbers, booleans, nulls, and object/array keys are skipped; only string values are emitted, space-separated). |
| tool | NULL (not indexed; retrievable via `pond_get`) |

What's deliberately NOT indexed:

- `ReasoningPart.text` - thinking traces. Stored canonically, retrievable via `pond_get` with `include_thinking: true`. Excluded from search to keep BM25 corpus tight and to align with the "search the conversation, not the plumbing" intent.
- `ToolResultPart.*` - tool output, often megabytes of structured data or scraped content. Stored canonically, retrievable via `pond_get` with `include_tool_results: true`.
- `ToolApprovalRequestPart`, `ToolApprovalResponsePart` - operational plumbing, never search-relevant.
- `FilePart.data` payload (decoded base64 / bytes) - indexing file contents is a deferred feature (section 4).
- SystemMessage content - boilerplate harness prompts dominate BM25 IDF and cluster vector embeddings; indexing them adds bytes without retrieval value.

`ToolCallPart.params` string-leaf extraction: a recursive walk of the JsonValue. For every leaf node whose JSON type is `string`, the value is emitted into the concatenation. Object keys, array indices, and non-string leaves are skipped. This avoids polluting BM25 with JSON structural tokens (`{`, `}`, `"`, field names) while preserving the rare-token search precision BM25 is best at (file paths, CLI commands, URLs, API names). Nested objects flatten to space-separated strings.

The embedding worker reads `messages.search_text` directly (no second concatenation path). Vector input and FTS input are byte-identical.

Ingest flow (search_text population). Per 3.4, the SourceAdapter emits `IngestEvent`s one at a time via the `decode` stream in the order specified there. Pond core's ingest handler buffers a whole session's substream - the `Session` event, then every `Message` with its `Part` events - to the session boundary (the next `Session` event or end of stream). At each Message boundary within the buffer it computes `search_text` for that Message from its Parts using the per-role concatenation policy above. At the session boundary it writes the buffered session in at most three `merge_insert` batches: one `sessions` row, all `messages` rows (search_text set), all `parts` rows. Per-event / per-row commits are not used - they multiply Lance manifest versions and fragment rewrites for no benefit.

This keeps the concatenation policy in pond core (3.3.1 single-knob requirement) without a background indexer or a two-pass update to the messages table. Memory footprint per ingest stream is bounded by a single session's events (tens to low hundreds per the source-sample survey in `tests/fixtures/session-samples/`).

Concat policy changes require re-ingest (run the SourceAdapter again); re-ingest is idempotent per invariant 2 - matching PKs with matching content are no-ops, matching PKs with new content (e.g. a new policy producing a different `search_text`) overwrite via merge_insert. Embedding rebuilds follow naturally because the embedded `search_text` changes.

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

Async + streaming both methods. Stream-based design gives pull-driven backpressure for free (pond core controls flow), bounds memory for huge JSONL files, and maps cleanly to all 8 source shapes pond plans to absorb (see `tests/fixtures/session-samples/`). Adapter implementations use tokio I/O primitives (`tokio::fs`, `tokio::io::BufReader::lines` for JSONL, the `serde` / `serde_json` stack for parsing).

**Dispatch.** Because `discover` and `decode` return `impl Stream` in trait-method position (return-position `impl Trait` in trait), `SourceAdapter` is NOT dyn-compatible - `Box<dyn SourceAdapter>` does not compile. This is intentional and costs nothing: v1 ships a single adapter, and multi-adapter dispatch is via an `enum Adapter { ClaudeCode(..), .. }` (the `--from <name>` CLI flag and the code-level registry select the variant). Pond never needs `dyn SourceAdapter`.

**Event ordering contract** (load-bearing for 3.3.1's search_text-population flow). The `decode` stream MUST emit events in this order for a single session:

1. The `Session` event first (exactly one per session).
2. For each Message in canonical source order: the `Message` event, followed by all of its `Part` events in `ordinal` order, before the next `Message` event.

In other words: Parts always immediately follow their parent Message, and the transition from a Part event to any non-Part event (Session or Message) signals end-of-Parts for the preceding Message. Pond core relies on this boundary to compute `messages.search_text` per 3.3.1 without buffering across Message boundaries. Adapters that produce events in any other order are non-conformant.

**Ordering enforcement.** Pond core's ingest path validates the contract per-stream: (a) the first event of a session's substream must be `Session`; (b) every `Part` event must carry a `message_id` matching the most recent `Message` event since the last boundary; (c) a `Message` event must reference an already-seen `Session.id` (in the current stream or in the dataset). The unit of abort is the offending session's event substream: a violation surfaces as `validation_failed` (3.6.1) and aborts the remaining events of that session - silent partial ingest of a session is invariant 5 (no silent drops). Events belonging to other sessions in the same stream or batch are unaffected and still processed. The CLI `decode` stream carries exactly one session, so a violation there aborts the whole stream; the HTTP batch (3.6.4) may carry many sessions, so a violation drops only the offending one. Both transports run the identical validator with identical semantics - there is no transport-specific behavior. The check is O(1) per event; the buffered Session and current-Message handles are the only state.

**Canonical `source_agent` strings.** The adapter is responsible for stamping `Session.source_agent` at canonical-projection time per 3.1.3 (trimmed, non-empty, no control chars). To keep cross-source filters predictable, pond reserves the following canonical strings for v1 + roadmap adapters; each adapter MUST emit one of these values:

| Adapter | `source_agent` value(s) |
|---|---|
| Claude Code (v1) | `claude-code` |
| Codex CLI - interactive | `codex-cli` |
| Codex CLI - exec/sandbox | `codex-exec` |
| OpenCode | `opencode` |
| OpenClaw | `openclaw` |
| nanoClaw | `nanoclaw` |
| pi-mono | `pi` |
| Claude desktop (local-agent-mode) | `claude-app` |
| Anthropic Managed Agents | `anthropic-managed-agents` |

Where one source surface ships multiple operationally distinct runtimes (codex's `codex_cli_rs` vs `codex_exec`), the adapter splits them into distinct `source_agent` strings rather than collapsing - real codex samples show these have very different `cwd` patterns (real project paths vs `/tmp/workspace`) and bucketing them together corrupts the `project` filter surface. Additional source surfaces ship adapter-defined strings; the table here is amendment-only.

**Per-adapter `Session.project` derivation rules.** Each adapter populates `Session.project` from its source's native attribution mechanism. Concrete rules (motivated by stress-testing real source samples in `tests/fixtures/session-samples/`):

- **claude-code, codex-cli, codex-exec, pi, nanoclaw**: session-level `cwd` field. For codex, the session-level `cwd` from `session_meta` is canonical; per-turn `turn_context.cwd` drift goes to `options.source.codex.turn_cwd[]` (preserved verbatim, not promoted to `project`). For nanoclaw, container `cwd` (e.g. `/workspace/agent`) is acceptable as `project` since it identifies the agent's working root.
- **opencode**: per-session `directory` field (the user-meaningful working dir), NOT the source's `projectID` hash. The `projectID` value is stashed under `options.source.opencode_project_id` for cross-reference; three real samples (`opencode/storage/session/0c929829.../ses_*.json`) collapse three different repos under one hashed `projectID`, so it's unusable as a filter. Per-message `path.cwd` drift goes to `options.source.opencode.message_cwd[]`.
- **openclaw**: session header `cwd` field, with a denylist: when `cwd` matches `$HOME`, `/`, `/tmp`, `/var/tmp`, `/private/tmp` (resolved at ingest), the adapter emits `project = null` with an info-level log. The raw value goes to `options.source.openclaw.cwd_raw`. Rationale: a real sample (`openclaw/agents/main/sessions/a5ecbacb-...jsonl.reset.2026-04-03T16-08-18.440Z`) has `cwd=/Users/user`, which is not a meaningful project filter.
- **claude-app**: `userSelectedFolders[0]` from the metadata sidecar (the "primary folder"). The full array goes to `options.source.claude_app.user_selected_folders[]`. A single audit.jsonl file can contain rows for multiple inner `session_id` values (real sample `local_4f2429ff-.../audit.jsonl` has three); the adapter splits per-inner-session_id and projects each as a separate pond Session, applying the sidecar's `cwd`/`userSelectedFolders`/`systemPrompt` only to the Session whose id matches the sidecar's `cliSessionId`. Other inner sessions get `project = null` plus an `options.source.claude_app.split_origin` marker.
- **anthropic-managed-agents**: `project = null`. No source attribution mechanism exists; `memory_store_id` (when attached) goes to `options.source.anthropic.memory_store_id` (not `project`, since `memory_store_id` doesn't identify a project).

**openclaw `.reset.<ts>` rotation handling (E6c).** OpenClaw rotates active session files to `<id>.jsonl.reset.<iso8601>` and starts fresh files reusing the same inner `Session.id`. When `discover` enumerates rotated files alongside the live file, the adapter emits events from both into the same `Session.id`; per-event `merge_insert` on canonical PKs deduplicates message and part rows by content. Each event from a rotated file carries `options.source.openclaw.rotation_origin = "<basename of source file>"` so consumers can partition by rotation epoch later (the v1 search and get surface ignores this tag; a future rotation-aware view can read it without re-ingest). The resulting Session's message log is the union of rotation epochs, ordered by `(timestamp, message_id)` per the canonical Message ordering rule (3.1.4).

**`parent_session_id` is a soft foreign key.** Pond core does not validate that a forked session's `parent_session_id` references an existing session row at ingest. Forks against missing parents (real case: nanoclaw subagent files referencing parent sessions absent from disk) are stored as-is; consumers traversing fork lineage handle dangling pointers. This avoids ordering constraints between independent adapter runs and matches the append-only invariant (the parent might land in a later ingest pass).

Session-batched append granularity. Pond core buffers a whole session's `decode` substream and writes it in at most three `merge_insert` batches per session - one `sessions`, one `messages`, one `parts` - each keyed on the canonical PKs (`Session.id`, `(session_id, Message.id)`, `(message_id, Part.id)`). Per-event / per-row commits are explicitly not used: they multiply Lance manifest versions and fragment rewrites with no benefit. Re-ingest is idempotent (invariant 2): re-reading an already-ingested session is a no-op for matched rows. The `merge_insert`-on-stable-PK approach is validated against `lance-format/cocoindex-lancedb-demo`. Discover-time efficiency optimizations (per-source mtime+size checkpoint table, modeled on `lancedb/lancedb-claw`'s engine state-row pattern) are deferred until scan cost becomes measurable. End-of-session import is the v1 default; live-write is deferred (section 4) and activates additively by adding a `follow(SessionRef) -> impl Stream<Item = Result<IngestEvent>>` method (or a separate `LiveSourceAdapter` trait) - same Stream shape, infinite stream.

### 3.5 Conformance fixture set

v1 conformance tests run against the committed Claude Code session fixtures under `tests/fixtures/session-samples/claude-code/`: ingest a fixture, store it, retrieve it via `pond_get`, assert structural equivalence with a re-parse of the source. Storage round-trip only - the transport-to-provider layer (and pi-mono's cross-provider fixture matrix) reactivates with replay (section 4).

### 3.6 Wire operations

Operations:

- `pond_search` - `POST /v1/search`
- `pond_get` - `POST /v1/get`
- `pond_ingest` - `POST /v1/ingest` (always-batched events)
- `pond_session_events` - `GET /v1/sessions/{id}/events?since=<id>` (SSE stream; HTTP-only, no MCP equivalent in v1)
- `schema://pond` - resource (search fields and filter documentation)
- `stats://pond` - resource (dataset counts, embedding model, storage stats)

Every request body carries `protocol_version: 1` (per 2.2) and an optional `namespace` (defaults to `"local"`). Every response body (success or error) carries `request_id` (server-generated UUIDv7) for log correlation.

CLI verbs (out-of-band): `pond setup` (resolve the data dir, write a default config, fetch + verify the model), `pond ingest --from <adapter>` (parse, store, embed, and index in one pass - embedding is a stage of ingest, not a separate verb), `pond status`, `pond serve` (HTTP server, including the `/mcp` streamable-HTTP MCP route), `pond mcp` (stdio MCP server only; stdout reserved for JSON-RPC frames), `pond maintenance` (runs cleanup_old_versions + optimize_indices one-shot per 3.2.0).

Admin CLI verbs (for recovery / inspection, not user-facing search):

- `pond versions list` - enumerate Lance manifest version history (version_id, commit timestamp, fragment summary).
- `pond checkout <version>` - open a read-only handle pinned to that version for inspection (read-only; does not replace current state).
- `pond restore <version> --force` - dangerous: rolls the dataset back to a prior version. Requires `--force`. Used for recovery from corrupted ingest, bad adapter shipments, or PK-mangled writes that can't be fixed by re-ingest. Bounded by the 3.2.0 `auto_cleanup` retention window (30 days personal / 90 days hosted).

#### 3.6.1 Error envelope

Same envelope for HTTP and MCP. Success and error are mutually exclusive at the body level.

Error body:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "filters.project_match must be one of: exact, contains",
    "details": { "field": "filters.project_match", "value": "wildcard" }
  },
  "request_id": "req_01HXY..."
}
```

Codes (closed enum for v1):

| Code | When | HTTP | Retryable |
|---|---|---|---|
| `validation_failed` | Bad request shape, missing required field, type mismatch, enum out of range, batch over 8MB | 400 | No |
| `version_unsupported` | `protocol_version` value pond doesn't understand | 400 | No |
| `not_found` | `pond_get` for a session/message/part that doesn't exist | 404 | No |
| `namespace_unknown` | Hosted: integrator's gateway routed an unprovisioned namespace string | 403 | No |
| `storage_unavailable` | Lance/object-store error after retry-with-jitter exhausted | 503 | Yes |
| `conflict` | OCC retries exhausted on a write | 409 | Yes |
| `internal` | Bug / unhandled exception | 500 | Maybe (once) |

Retryability is conveyed by the code; no separate `retryable` field. Per-code `details` shapes:

- `validation_failed`: `{ "field": "<JSON Pointer or dotted path>", "value"?: <offending>, "expected"?: "<short desc>" }`
- `version_unsupported`: `{ "received": <int>, "supported": [<int>, ...] }`
- `not_found`: `{ "kind": "session" | "message" | "part", "pk": <scalar | tuple> }`
- `namespace_unknown`: `{ "namespace": "<string>" }`
- `storage_unavailable`: `{ "retry_after_ms"?: <int>, "underlying"?: "<one-line cause>" }`
- `conflict`: `{ "attempts": <int> }`
- `internal`: `{}` (empty; `request_id` carries everything actionable)

Per-row errors in `pond_ingest` reuse the inner shape (just the `error` object). MCP surfaces the same domain error in JSON-RPC's `error.data`; rmcp sets the JSON-RPC `code` (-32000 family). The `message` field stays short (one line); long context goes to server logs.

#### 3.6.2 pond_search

Request `POST /v1/search`:

```json
{
  "protocol_version": 1,
  "namespace": "local",
  "query": "rust lifetime error",
  "search_mode": "hybrid",
  "rrf_k": 60,
  "filters": {
    "project": null,
    "project_match": "exact",
    "session_id": null,
    "source_agent": null,
    "from_date": null,
    "to_date": null,
    "role": null,
    "min_score": 0.0
  },
  "boost_recent": true,
  "group_by_conversation": false,
  "limit": 10
}
```

- `query`: required, non-empty after trim.
- `search_mode`: `"hybrid"` (default) | `"vector"` | `"fts"`. Per 2.5.
- `rrf_k`: default 60. Consulted only when `search_mode = hybrid`.
- `filters.project_match`: `"exact"` (default) | `"contains"` | `"is_null"`. `is_null` ignores the `project` value field and emits `project IS NULL` (required for filtering anthropic-managed-agents per 3.3 / 3.4).
- `filters.role`: `"user"` | `"assistant"` | `"system"` | `"tool"`. System/tool always return empty (NULL search_text).
- `filters.min_score`: default `0.0` (no threshold).
- `boost_recent`: default `true`; formula in 3.3.
- `group_by_conversation`: default `false`.
- `limit`: default 10, server-enforced cap 200.

Response (default):

```json
{
  "hits": [
    {
      "session_id": "01HXY...",
      "message_id": "msg_01ABC...",
      "role": "assistant",
      "timestamp": "2026-04-15T10:23:45.123Z",
      "project": "/Users/me/Projects/pond",
      "source_agent": "claude-code",
      "preview": "Let me check the lifetime bound on `T: 'a` in the trait...",
      "score": 0.83,
      "base_score": 0.71,
      "recency_boost": 0.12,
      "matched_via": ["vector", "fts"]
    }
  ],
  "total": 17,
  "request_id": "req_01HXY..."
}
```

- `preview`: first 500 chars of `messages.search_text`, truncated at code-point boundary, `"..."` appended if truncated. v1 does not generate matched-token-highlighted snippets (Lance FTS does not surface token offsets); upgrade is deferred.
- `score`: final ranking score. `hybrid` = RRF + recency_boost; `vector` = normalized cosine + recency; `fts` = normalized BM25 + recency.
- `base_score`: score before recency boost. Always reported.
- `recency_boost`: additive bump; `0` when `boost_recent: false`.
- `matched_via`: which retriever(s) ranked this row in their top-K. Useful for debugging hybrid.
- `total`: count of returned hits (= `hits.length`); not the global match count.

Response (`group_by_conversation: true`): the `hits` field is replaced by `groups` per 3.3 shape.

#### 3.6.3 pond_get

Request `POST /v1/get` (one of `session_id` or `message_id` required):

```json
{
  "protocol_version": 1,
  "namespace": "local",
  "session_id": "01HXY...",
  "message_id": null,
  "up_to": null,
  "context_depth": 0,
  "max_messages": 100,
  "include_thinking": false,
  "include_tool_results": false
}
```

- `session_id` alone: return the whole session (Session + all Messages + all Parts).
- `message_id` alone: return one Message (with its Parts), `context_depth` messages before, `context_depth` messages after (within the same session).
- `up_to`: optional; valid only alongside `session_id` (mutually exclusive with `message_id`). When set, the session is returned truncated at and including the message whose `id` equals `up_to`, in canonical `(timestamp, id)` order. An `up_to` value not present in the session returns `not_found`. Mirrors kb's restore-conversation-up-to-a-point workflow.
- `max_messages`: default `100`, server-enforced cap `1000`. Applies to session-scope reads (`session_id`, with or without `up_to`). After any `up_to` truncation, only the last `max_messages` messages (those closest to the cut point / most recent) are returned. Ignored for `message_id` reads, which are bounded by `context_depth` instead.
- `include_thinking`: default `false`. When `false`, `ReasoningPart` entries are stripped from returned Messages.
- `include_tool_results`: default `false`. When `false`, `ToolResultPart` entries are stripped. ToolMessages whose only Parts are ToolResultPart become empty and are omitted from the response.
- `ToolApprovalRequestPart` and `ToolApprovalResponsePart` are always stripped from default responses (no toggle).

**MCP-transport rendering note.** Over the MCP transport, stripped `ReasoningPart` / `ToolResultPart` Parts are replaced with a compact placeholder string (`[reasoning: N chars]`, `[tool_result: N chars]`) rather than removed silently, so a calling agent knows retrievable content exists and can re-request with the toggle. The HTTP transport strips with no placeholder (structured clients re-request explicitly). This is the only intentional HTTP-vs-MCP response-shape divergence; it lives in the MCP adapter, not the shared handler.

Response shape carries the canonical Session/Message/Part types verbatim plus `request_id`. Specific shape omitted from this section (mirrors 3.1).

#### 3.6.4 pond_ingest

Request `POST /v1/ingest`:

```json
{
  "protocol_version": 1,
  "namespace": "local",
  "events": [
    { "kind": "session", "data": { /* Session per 3.1.3 */ } },
    { "kind": "message", "data": { /* Message per 3.1.4 */ } },
    { "kind": "part",    "data": { /* Part per 3.1.5 */ } }
  ]
}
```

- Always-batched (single-event is a length-1 array). `events` required and non-empty.
- Envelope `kind` discriminator (`"session"` | `"message"` | `"part"`) avoids collision with canonical types' internal discriminators (`Message.role`, `Part.type`).
- Events processed in array order. Pond does not reorder.
- Batch caps: 1000 events OR 8MB body size, whichever first. Over: `validation_failed`.
- No batch atomicity across sessions. Events are grouped by session substream and each session is written in at most three batched `merge_insert`s (per 3.4); partial success across sessions is normal and explicit via per-row `status`.
- Transport-level errors (bad JSON, version_unsupported, namespace_unknown, empty batch) fail the whole request via 3.6.1 envelope.
- **Event ordering enforced** (3.4): first event in any new-session substream must be `Session`; every `Part` event's `message_id` must match the most recent `Message` in the same substream; every `Message`'s `session_id` must reference a `Session` already seen (this batch or the dataset). A violation surfaces as per-row `validation_failed` and aborts the remaining events of the offending session's substream; events of other sessions in the batch still process. This is the identical semantics the CLI streaming adapter applies per 3.4 - the two transports do not differ.
- **Immutable session-level fields.** `Session.source_agent` and `Session.project` are immutable post-first-write. A `kind: "session"` event whose `data.id` matches an existing session row and whose `source_agent` or `project` differs from the stored row returns per-row `validation_failed` with `details: { field: "source_agent" | "project", reason: "immutable" }`. Other Session fields (`options`, `parent_session_id`, `parent_message_id`, `created_at`) re-write idempotently via `merge_insert` (matching content = no-op; non-matching content overwrites). Rationale: the denormalized copies on `messages` and `embeddings` (3.2.2 / 3.2.4) are stamped once at ingest from the Session event; mutating the canonical source post-hoc would silently desynchronize them. Recovery from a wrong `source_agent`/`project`: delete the affected session rows and re-ingest (delete is not in the v1 wire surface; admin operation only).

Response:

```json
{
  "accepted": 2,
  "rejected": 1,
  "results": [
    { "index": 0, "kind": "session", "pk": "01HXY...",                            "status": "inserted" },
    { "index": 1, "kind": "message", "pk": ["01HXY...", "msg_01ABC..."],          "status": "matched"  },
    { "index": 2, "kind": "part",    "pk": ["msg_01ABC...", "part_01XYZ..."],     "status": "error",
      "error": { "code": "validation_failed", "message": "ToolCallPart.call_id missing", "details": { "field": "call_id" } } }
  ],
  "request_id": "req_01HXY..."
}
```

- `status`: `"inserted"` (new row), `"matched"` (PK existed, `merge_insert` no-op per invariant 2), `"error"` (per-row failure).
- `pk` shape mirrors canonical PKs: scalar for session, tuple for message/part. Echoed so retries reconcile.
- `accepted` = inserted + matched; `rejected` = error.

#### 3.6.5 pond_session_events (SSE)

`GET /v1/sessions/{session_id}/events?since=<event_id>&include_thinking=false&include_tool_results=false&namespace=local`

v1 scope: catch-up reads only. The server scans messages + parts strictly after `since`, emits them in `(timestamp, message_id)` order, then closes. Live-tail activates with live-write (section 4) on the same endpoint without a wire change.

Query params:
- `since`: optional. Falls back to the `Last-Event-ID` HTTP header (EventSource auto-reconnect). Explicit `since` wins if both set.
- `include_thinking` / `include_tool_results`: same defaults and semantics as `pond_get`.
- `namespace`: default `"local"`.

Three event types:

```
event: session
id: session:01HXY...
data: {"id":"01HXY...","source_agent":"claude-code","project":"...","created_at":"...","options":{...}}

event: message
id: msg_01ABC...
data: {"message":{"id":"...","role":"assistant","timestamp":"...","options":{...}},"parts":[{...},{...}]}

event: end
id: end:01HXY...
data: {"reason":"caught_up"}
```

Plus SSE keepalive: `: keepalive` comment lines every 15 seconds during long scans.

- `session` event: emitted first only when no `since` provided.
- `message` event: bundles message + its parts (filtered by include_thinking / include_tool_results), one per message in canonical order.
- `end` event: emitted after the last message; server closes the connection.

Event ids: `session:<session_id>` for the header, `<message_id>` verbatim for messages, `end:<session_id>` for the terminator. Prefixes prevent collision in the message_id space.

Resume: client reconnects with `since=<last id received>`.
- `since` matches a message id: scan resumes from `(timestamp, message_id) > (since.ts, since.id)`.
- `since = session:<id>`: server emits messages from start (header already seen).
- `since = end:<id>`: server emits `end` immediately and closes (idempotent re-fetch).
- `since` unknown: `400 validation_failed`; client falls back to no `since` for a full re-read.

Sequencing source: direct scan of `messages` and `parts` tables; no separate event log. Lossless replay; any pond instance can serve any session's stream.

Server-side idle timeout: 30 minutes. Client disconnect cancels the scan. No "session deleted" event in v1 (sessions are append-only; deletes are not in the v1 wire surface).

MCP equivalent: not exposed in v1. Future deferred.

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
- **Wire-API-surfaced time-travel / friendly historical queries.** Lance natively versions every commit and supports `Dataset::checkout(version)`, `list_versions`, tags. v1 surfaces these via the admin CLI verbs in 3.6 (`pond versions list`, `pond checkout`, `pond restore`) and the 30/90-day retention window in 3.2.0 `auto_cleanup` - sufficient for operational recovery. Activation: friendlier UX needs (`--as-of "yesterday at 3pm"` time-string queries, audit endpoints returning historical search results, automated rollback workflows). When activated, adds `version: int?` to read-side wire requests and a `pond_versions` operation; no schema changes since Lance owns the mechanism.

---

## 5. Open Questions

Empty. Sections 1-4 are the source of truth; git history preserves the trail of resolved questions (OQ1-OQ10).

