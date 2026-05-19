# Pond - Design v2

> Status: sections 1-4 are the source of truth. All v1 design decisions are locked into the relevant sections above; git history preserves the trail.

---

## 1. What this is + non-goals

Pond is a Rust crate that wraps `lance-format/lance` directly with sessions-aware ingest, storage, and a JSON wire interface. One binary. Two transports: HTTP and MCP. Two deployments: a personal pond on a laptop, or a multi-tenant backend for hosted agent infrastructure.

Lance (and the file format underneath it) is the substrate. Pond does not introduce a separate "substrate layer" of its own. Pond owns canonical session types, source adapters, the wire schema, the HTTP and MCP transports, and the conventions for using Lance consistently across deployments.

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
| Storage and search engine | `lance-format/lance` crates direct: `lance`, `lance-table`, `lance-io`, `lance-encoding`, `lance-index`. Pinned via git dependency; tag in `Cargo.toml`. No `lancedb` crate dependency. |
| Catalog / table discovery | `lance-namespace` (the `LanceNamespace` trait) + `lance-namespace-impls` (Directory v2 backend day one; REST / Glue / Unity / Polaris / Iceberg REST adapters activated by hosted-tier per section 4). Pond opens every Lance dataset through this trait (invariant 21). Pond's wire `namespace` field is a tenant routing tag, distinct from the Lance `LanceNamespace` catalog concept; see 2.2. |
| Lance file format | `stable` (2.2+) for new datasets. Blob v2 for FilePart payloads. |
| Object store backends | `object_store` via Lance: local filesystem, S3 (native conditional writes), GCS, Azure. |
| HTTP server | axum (tokio-native, JSON-first, SSE built in). |
| MCP server | rmcp (official Anthropic Rust SDK), wrapping the same handlers as HTTP. |
| Wire format | JSON. Single evolving schema with top-level `protocol_version` field. Additive-only changes; formal `v2` only on breaking changes. |
| Default embedding model | Qwen3-Embedding-0.6B (1024-dim, 32K context, Apache 2.0) via fastembed-rs `Qwen3TextEmbedding::from_hf` on the candle backend. Full configuration in 3.2.4. |
| Output | single static binary via `cargo build --release`. |
| Code organization | Single Cargo crate. Strict module discipline separates substrate from consumer (sessions) code internally. Workspace split deferred until a second consumer (resources, archives) ships real code. |

### 2.1.1 Personal pond defaults

- **Bind**: `--host 127.0.0.1 --port 9797`. Env overrides: `POND_HOST`, `POND_PORT`. `--port 0` selects an OS-assigned free port. `--host 0.0.0.0` is accepted but logs a security notice at startup (personal pond is single-user; LAN exposure is opt-in).
- **Config**: `$XDG_CONFIG_HOME/pond/config.toml` (Linux and macOS; XDG-strict so cross-platform path stays consistent). TOML format. Schema is documented in this doc; `pond config --print-schema` emits a fully-annotated example. Key blocks: `[[embeddings.models]]` (3.2.4 registry; built-in default if absent), `[embeddings.overrides.<namespace>.<model_id>]` (per-namespace embedding tuning), `[maintenance]` (3.2.0 background cleanup + index optimization), `[storage]` (3.2.0 object-store credentials/region/endpoint, passed verbatim to Lance; required only when `--data-dir` is an `s3://` / `gs://` / `az://` URI). `[sources]` and `[storage]` are flat in v1 on the single-namespace assumption (2.3 inv 11); multi-namespace pond will wrap them under `[namespaces.<ns>.{sources,storage}]`. `[[embeddings.models]]` + `[maintenance]` stay process-global.
- **Data**: `$XDG_DATA_HOME/pond/` (Linux and macOS; XDG-strict). Override via `--data-dir <path>` or `POND_DATA_DIR`. Object-store URIs (`s3://bucket/pond`, `gs://...`, `az://...`) are accepted directly; pair with the `[storage]` config block above for credentials/region. The data dir is the root of a **Directory v2 Lance namespace**: pond opens it via `connect("dir", { root: <data-dir-uri>, storage.*: <forwarded> })` and the four tables (`sessions`, `messages`, `parts`, `embeddings`) are registered as `["<table>"]` in the root namespace's `__manifest` Lance table. V2 hash-prefixed directory names (e.g., `a1b2c3d4_sessions/`) replace the readable `<table>.lance` layout; operators using `lance-cli` directly dereference table locations via `__manifest`. The `config.toml` file always lives locally even when the data dir is remote (it names the bucket and any creds).
- **Logs and output - two channels, each with one owner**:
  - *Diagnostics channel*: all structured logging (spans, debug/info/warn/error) goes through `tracing`. The `tracing-subscriber` is initialized exactly once at process start (env-filter via `RUST_LOG` / `POND_LOG`) and always writes to **stderr**. No module configures logging itself.
  - *Results channel*: the actual output of a verb (search results, status) is not logging - it is written by a single `output` helper. For `pond serve` and the CLI verbs it goes to **stdout** (human-readable, or JSON where the verb is machine-facing). For `pond mcp` (the stdio MCP server) stdout is reserved exclusively for JSON-RPC frames, so the `output` helper emits the result as a JSON-RPC frame instead. `pond serve` has no stdout restriction because its MCP channel is the `/mcp` HTTP route, not stdout.
- **Platform scope**: Linux and macOS for v1. Windows not in scope.

### 2.2 Wire interface

The wire interface is the contract. Internal serde types evolve freely behind a projection layer.

- **Transport-agnostic handlers.** Every operation is a function `Json request -> Json response` (with optional streaming response for SSE). HTTP and MCP transports are thin adapters that dispatch to the same handler functions.
- **Request envelope.** Every request carries a `protocol_version` field at the top level. Value is a positive integer (`1`, `2`, ...); v1 ships `1`. Server validates the field and returns a typed error on unknown version.
- **Namespace per request.** Every wire request carries a `namespace: string?` field. Omitted means `"local"` (personal pond's hardcoded namespace). Hosted integrators populate it from their own pre-pond auth resolution. Pond performs no authn/authz of its own; the bucket's IAM is the storage boundary and the integrator's gateway is the application boundary. No HTTP middleware seam in pond core. **Name collision warning:** pond's wire `namespace` is a tenant routing tag and is distinct from Lance's `LanceNamespace` catalog concept (the trait that owns table discovery and location resolution; see the 2.1 stack row). In single-tenant v1 the wire `namespace` is always `"local"` and resolves to the root Lance namespace `[]`. In hosted multi-tenant (deferred, section 4) the wire `namespace` resolves to a Lance child namespace `[<tenant>]` and table identifiers become `[<tenant>, "sessions"]` etc. Same word, distinct concepts.
- **Schema evolution.** Additive-only within a major version. Adding fields is allowed; removing or retyping fields requires a major bump.
- **Published schema artifacts.** JSON Schema files generated from Rust types, committed to the repo, versioned with the binary. Pond's canonical types derive from Effect's `Prompt` shape (3.1), not from OTel GenAI. OTel's `gen-ai-input-messages.json`, `gen-ai-output-messages.json`, `gen-ai-tool-definitions.json`, and related schemas are noted as inspiration where shapes overlap (messages, tool definitions, finish reasons) but pond does not claim derivation. An OTel-compatible projection schema is deferred to section 4 for the day a consumer wants OTel observability interop.
- **HTTP shape.** `POST /v1/<operation>` with JSON body; RPC-shaped, no REST resource model. Streaming responses use SSE on `GET /v1/sessions/{session_id}/events?since=<event_id>`.
- **MCP shape.** Same operations exposed as MCP tools (`pond_search`, `pond_get`, `pond_ingest`, etc.). MCP `tools/list` returns the operation set.

### 2.3 Operational invariants

These are constraints every pond write and read must satisfy. Code review rules.

1. **Append-only writes.** Existing rows are never mutated. Updates produce new rows or new manifest versions.
2. **Deterministic primary keys.** Client-supplied IDs (UUIDv7 for sessions, content-hash for derived rows where applicable). All writes use Lance `merge_insert` on the PK so retries are no-ops.
3. **Retry-with-jitter on every Lance call.** Pond-side helper (3 attempts default, 300ms-5000ms exponential backoff, 0.2 jitter, per-operation labels). Connection-level retry on top.
4. **No cached table handles forever.** Pond is a long-lived server, so it owns the staleness window: each cached `Dataset` handle records its last-refresh time, and pond calls `Dataset::checkout_latest()` (a cheap manifest read) to pick up external writers before serving a read whenever the handle is older than the configured interval. The interval is keyed off the connection URI scheme: local filesystem = `0` (always refresh; manifest reads are microseconds), object store (`s3://`, `s3+ddb://`, `gs://`, `az://`) = `5s` (caps manifest fetch overhead; acceptable lag for human-driven queries). Configurable override. Table handles may be reused between requests but must not be opened at startup and held without refresh.
5. **No silent drops.** Malformed input surfaces with line offset and error context. Ingest fails closed by default.
6. **Opaque IDs, not paths.** `namespace_id`, `workspace_id`, `project`, `agent_id` are opaque strings. The Claude Code SourceAdapter decodes path-encoded session directories once at ingest and stores the decoded values; readers never re-parse.
7. **No SQL.** Lance scalar predicates and search APIs are the only query mechanism.
8. **Encryption is operational.** Bucket SSE plus filesystem encryption. No application-level crypto, no `is_encrypted` columns, no KeyProvider.
9. **Schema versioning at the dataset level.** Lance manifest version plus dataset-level metadata key. No per-row `schema_version` columns.
10. **pond is the durable copy.** Once a session lands in pond, it MUST survive source rotation, source deletion, and session expiration. Sources are upstream collectors that pond ingests from; pond is the canonical record once ingestion completes. Wipe-and-resync is NOT a general recovery path — it loses any session whose source has since rotated, been deleted, or expired. Recovery for corruption goes through Lance's manifest history (3.2.0 `auto_cleanup` retention) and `pond export` snapshots (3.6); re-ingest is the contract for landing new rows under invariant 14, not a substitute for durability. Operators MAY still re-ingest at any time without risk because writes are idempotent on canonical PKs (invariants 1 + 2), but the durability guarantee comes from pond's own storage, not from the source still being reachable.
11. **Namespace resolution lives in one function.** `handlers::resolve_namespace(namespace) -> Result<NamespaceIdent, ErrorEnvelope>` is the only code path that decides whether a request's namespace is acceptable, and returns the resolved Lance namespace identifier path (per invariant 21). No wire handler hand-rolls the check. v1 returns `[]` (root); hosted multi-tenant pond returns `[<tenant>]` and every call site picks up the correct child namespace without further edits. The signature evolves once for multi-tenant; call sites do not.
12. **Per-tenant state lives on `Store`.** `AppState` carries `Store` plus stateless services only (today: `Option<Arc<dyn EmbedBackend>>`). New features that need persistent state take `&Store` or get added through the router seam, not as a sibling field on `AppState`. New backends ship as `Store::open(...)` constructors, not as parallel `S3Store` / `RemoteStore` types.
13. **Prefilter pushdown is opt-in on every Lance `Scanner`.** Pond MUST call `Scanner::prefilter(true)` on every vector kNN and FTS query; the `lance` crate defaults it to `false`. Without it, Lance silently postfilters in memory and ignores the scalar indexes entirely (recall loss; fewer than `limit` results returned). Load-bearing: an integration test on real data MUST assert via `Scanner::explain_plan` that the scalar predicate appears as a `ScalarIndexQuery` / `ScalarIndexExec` node (prefilter) and not as a top-level `FilterExec` (postfilter).
14. **Sync is purely additive at the row level.** `merge_insert` uses `WhenMatched::DoNothing` across every table: matched rows are never overwritten, never tombstoned. Source data is not authoritative against the stored row; pond's storage is. Adapter output must stay monotone across versions (a new adapter MUST produce a superset of prior canonical rows); changing or dropping previously-emitted rows is a per-row migration, not a sync flag (deferred). Enforced at `src/substrate.rs::merge_insert`.
15. **No synthesized values - enforced by the seam types, not by convention.** Adapters cannot substitute sentinels, defaults, or placeholder strings (`"unknown"`, `""`, `"default"`, `"function"`, `"server_tool"`, ...) for missing source data, because the schema fields that hold such data are typed as `Option<Extracted<T>>` and `Extracted<T>` has no public constructor reachable from adapter code. The only producers of an `Extracted<T>` are the `extract_*` helpers in `src/adapter/extract.rs`: `extract_str` / `extract_bool` / `extract_value` take a `&dyn Source` and return `Option<Extracted<T>>` for a real field lookup; `extract_self_str` views the source itself as a string; `extract_compact_repr` encodes the whole row losslessly as a fallback. There is no `unwrap_or("unknown")` path that compiles - synthesis is a type error, not a code review violation. What this invariant locks in is "couldn't resolve, so I made one up" must not compile. See `src/adapter/extract.rs` for the closed seam.

    *Allowed non-sentinel defaults* (these describe transport defaults or absence semantics, not invented field values): timestamps falling back to the session anchor; `is_failure: false` when the source row carries no error marker (typed as plain `bool`, not `Option<Extracted<bool>>`); `ordinal` clamps; MIME type fallbacks like `"application/octet-stream"`.
16. **Schema-honesty corollary.** A non-`Option<Extracted<T>>` field in the canonical schema is the adapter contract asserting "the source data always carries this in a form `Source` can extract." If any supported adapter cannot guarantee that, the schema MUST become `Option<Extracted<T>>`, not the adapter MUST invent a value. Concretely: `PartKind::ToolResult.name` is `Option<Extracted<String>>` because the source tool_result row doesn't carry the name itself - it's resolved via a per-file `tool_use_id -> name` map (`HashMap<String, Extracted<String>>`) and misses surface as `None`, never as a fabricated string. The CLI / wire layer renders `None` as nothing-at-all (the absent field is simply omitted from output), never as a translated placeholder.
17. **Adapter-level dedup is the contract; substrate FirstSeen is the floor.** Adapters SHOULD detect duplicate-PK emissions using the source format's own mechanism (e.g. claude-code's `messageSet`, mirrored as `seen_uuids: HashSet<String>` since claude-code's dedup occasionally races on `/resume`). The substrate runs `merge_insert` with `SourceDedupeBehavior::FirstSeen` (`src/substrate.rs::merge_insert`) so storage stays correct even when an adapter misses; the skip count surfaces on the `pond::perf` info line. The validator's in-batch HashSet charges duplicates to `dropped_events` with `drop_reasons["duplicate_*"]`, keeping noisy adapters visible in the summary rather than hidden inside `skipped`.
18. **Adapter seam is transport-agnostic.** The `Source` trait abstracts one row of source data behind four primitive accessors (`str_field`, `bool_field`, `value_field`, `nested`) plus two optional self-views (`as_str`, `compact_repr`). Pond ships `impl Source for serde_json::Value` for JSON-flavored adapters; other adapters `impl Source for MyRow` for any shape (deserialized struct, HTTP body, stream frame, database row). The `Adapter::events` stream is similarly source-agnostic: `Stream<Item = Result<IngestEvent, AdapterError>>` regardless of whether events arrive from a file walker, long-poll loop, WebSocket, or queue subscription.
19. **`Session.parent_message_id` implies `Session.parent_session_id`.** The two pointers capture different relationships: `parent_session_id` alone covers the spawn case (claude-code subagents, nanoclaw subagents - the dominant case across real corpora); both together cover the fork-with-cut-point case (pi-mono DAG branches projected into separate pond Sessions). A `parent_message_id` without a `parent_session_id` is incoherent (a cut-point without a session to cut from), so the validator rejects such Session events with `DROP_REASON_PARENT_MESSAGE_WITHOUT_SESSION`.
20. **`Session.project` is non-empty.** Adapters MUST emit a non-empty `Extracted<String>` per session. The schema field is `Extracted<String>` (not `Option<...>`), so the seam refuses synthesized sentinels. Per-adapter extraction chains live in each adapter's source docstring (the implementation is the documentation); an adapter that genuinely cannot resolve a project MUST drop the session with `DROP_REASON_MISSING_PROJECT`, never invent one.

Invariants 21-28 below are **forward-looking structural seams**: they were deliberate choices through v1 design to keep pond MemWAL- and namespace-eligible by construction. Adopting `lance-namespace` is a 2.1 stack choice; adopting MemWAL / LSM scanner / HNSW-on-memtable / ShardWriter is deferred (section 4). These invariants are what make those activations a substrate swap behind `Store` rather than a cross-cutting rewrite.

21. **Catalog seam: all dataset opens go through `LanceNamespace::describe_table`.** No code path constructs a path-based URI to a Lance dataset. The `Store::open` seam in `src/substrate.rs` calls `nm.describe_table([table_name]) -> location` and only then hands the location to `DatasetBuilder::from_uri`. This is the load-bearing catalog seam: swapping Directory v2 for REST / Glue / Unity / Polaris / Iceberg REST (section 4 hosted-tier activation) is a `connect("<impl>", props)` change with no call-site edits.

22. **Read seam: all scans go through `Store::scan(table, opts)`.** Handlers do not call `Dataset::scan()` directly. Centralizing scanner construction in one helper makes invariant 13 (prefilter pushdown on every `Scanner`) enforceable in one place, and makes the future swap of `Scanner` for the LSM scanner (when MemWAL activates) a single-file change. See `src/substrate.rs::Store::scan`.

23. **Write seam: all writes go through `Store::merge_insert(...)`.** Handlers do not call `Dataset::merge_insert`, `Dataset::write`, or any other `Dataset` write API directly. The centralized helper at `src/substrate.rs::merge_insert` is what invariants 14 and 17 already reference; promoting it to a structural invariant makes MemWAL's merger (itself merge-insert-shaped) a drop-in substrate swap rather than a per-call-site rewrite.

24. **Every table declares an unenforced primary key.** `Field.unenforced_primary_key_position` is set on the PK column(s) at table creation. Already true on the four v1 tables (3.2.0); promoted to invariant so future tables (resources, archives, any new substrate consumer) cannot skip it. MemWAL has this as a hard precondition; this invariant keeps every pond table MemWAL-eligible by construction.

25. **`enable_stable_row_ids: true` on every table.** Already set on the four v1 tables (3.2.0) for compaction-friendly index survival. Promoted to invariant because the LSM scanner's dedup key `(_gen, _rowaddr)` (the protocol that lets reads merge across base + flushed + in-memory generations under MemWAL) requires stable row ids to be on universally.

26. **PK position 1 on high-volume tables is a coarse-grain shardable attribute.** `session_id` on `messages`, `parts`, `embeddings`; `id` on `sessions`. New high-volume tables MUST place an attribute at PK pos 1 that can serve as the input to a `bucket(col, N)` (or equivalent DataFusion expression) sharding spec. This keeps the future ShardWriter activation path open without requiring a PK redesign or row migration on existing data: the shard spec attaches to the existing column and new writes route through it from that point forward.

27. **No wire operation promises reads-see-writes within milliseconds.** Invariant 4 sets the floor (5s on object stores). Pond does not contract sub-second freshness on any read path; features that would require it are out of scope. This keeps the door open for MemWAL's async-merge latency (writes durable on WAL flush, visible to the base table after merger, indexed after async index catchup) to fit the same read-staleness envelope when it activates.

28. **No write batch spans multiple PK shards atomically.** Each batch is keyed on one PK family: `sessions` batch on `Session.id`, `messages` batch on `session_id`, `parts` batch on `message_id` (which colocates with `session_id` via the 3.4 event-ordering contract). Cross-PK-family atomic writes are absent by construction. ShardWriter assigns each PK family to one shard; cross-shard atomicity is structurally unavailable under any MemWAL deployment, and pond's session-batched append granularity (3.4) already respects this. New write paths MUST too.

### 2.4 Concurrency model

Stateless workers. Multiple pond processes can write concurrently to the same namespace. Lance OCC handles append conflicts via manifest versioning. Content-addressed payloads make worker crashes and retries idempotent.

No external coordinator: S3, GCS, and Azure all provide native atomic conditional writes. Local filesystem uses Lance's internal commit lock.

No in-process write queue. Concurrent HTTP requests dispatch to handlers in parallel. Lance OCC plus retry-with-jitter resolves contention.

### 2.5 Search defaults

- **Embeddings are opt-in by config.** Sync (`pond sync`) and embed (`pond embed`) are separate verbs. The query-side embedding model loads only when `[embeddings] enabled = true` is set in `config.toml`; the flag defaults to `false`, so `pond serve` / `pond mcp` / `pond search` run FTS-only out of the box and never touch the model. `pond embed` itself is independent of the flag, so vectors can be pre-populated before flipping the switch.
- **Hybrid when enabled and populated.** With `enabled = true` plus at least one embedding row for the active `(model_id, max_embed_tokens)` identity, every search runs vector kNN plus BM25 FTS, merged with RRF (`k=60`, no weighting). If the flag is on but the embeddings table is empty for that identity, the per-request fallback in `handlers.rs::resolve_effective_mode` skips the vector leg silently.
- **Server-determined mode, no wire field.** Mode is not a request input and the response does not carry a `search_mode` field. The server picks hybrid or FTS based on `embeddings.enabled` plus per-identity row presence; retriever provenance is reported per-hit via `matched_via` (`"vector"` and/or `"fts"`). A vector-only mode is intentionally absent for v1: hybrid is monotonically >= vector-only at any embedding coverage > 0%. Optional `rrf_k` integer field on requests overrides the default `k` (consulted only in hybrid mode).
- **Both indexes always present.** FTS index on `messages.search_text` (3.2.2) and vector index on `embeddings.vector` (3.2.4) are created at table creation. Both retrievers operate at message granularity over the same input string; the schema does not branch on whether an embedding model is configured. Turning embeddings on or off does not require a schema migration.
- **Search corpus is fixed by concatenation policy.** What gets indexed is determined by 3.3.1 (TextPart, FilePart name/media/url only; reasoning, tool calls, tool results, approvals, system messages all excluded). Excluded Parts remain canonically stored and retrievable via `pond_get` (3.6).

---

## 3. v1 Application: sessions

### 3.1 Canonical types

The canonical types describe pond's three core objects - **Session**, **Message**, **Part** - at the LLM-conversation level (provider/protocol layer), below any specific harness. Harness-specific behaviors (compaction, retry, snapshot, step accounting, editor context, etc.) are absorbed via the per-object `options` metadata bag, not as canonical fields.

The Part union derives from Effect v4's Prompt-side Part union (`effect/unstable/ai/Prompt.ts`), copied not depended on. Effect's Response-side variants (streaming start/delta/end, FinishPart, ResponseMetadataPart, ErrorPart, source-citation Parts) are not stored as Parts in pond - Response-side metadata is projected at ingest into Message-level Lance columns; streaming variants are not persisted (assembled state only).

Pond adds a Session container layer Effect doesn't define, plus Session-level fork pointers. Effect has no branching concept and Anthropic Managed Agents has no in-session branching either - the Session-level fork pointers are a deliberate one-step extension beyond Effect+Anthropic, motivated by lossless absorption of opencode (session-level fork) and pi-mono (per-entry branch graph, projected into multiple pond Sessions by the SourceAdapter).

#### 3.1.1 Conventions

- **Notation**: TypeSpec syntax in code blocks. The canonical types defined here are the source of truth; Rust implementation types and JSON Schema wire artifacts are derived from these. The doc is not run through `tsp compile` - readers verify consistency manually until the schemas stabilize.

- **Casing**: all field names AND discriminator values use `snake_case`. This deviates from Effect's source (which uses camelCase fields and kebab discriminator values) and matches OTel's `gen-ai-input-messages.json` schema, Anthropic's wire conventions, and pond's Rust/Lance column conventions.

- **Tiebreaker**: Effect's `effect/unstable/ai/Prompt.ts` is the reference shape for anything this doc doesn't fully constrain. When in doubt, follow Effect.

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

  // Session-level parent pointers. Both null for root sessions (most
  // captures). Spawn-only sources (claude-code subagents, nanoclaw - the
  // dominant case in real corpora) populate `parent_session_id` while
  // leaving `parent_message_id` null. Fork-with-cut-point sources
  // (pi-mono DAG branches) populate both. Invariant 19 forbids the
  // inverse (cut-point without a parent session).
  parent_session_id?: SessionID;     // session this one spawned from / forked from
  parent_message_id?: MessageID;     // cut-point in the parent session (fork-with-cut-point only)

  // Provenance: the source harness brand. One source per session.
  // Common values: "claude-code", "opencode", "pi-mono", "anthropic-managed-agents",
  // "openclaw", "nanoclaw", "kilocode", ... (open string)
  //
  // Validation: trimmed at ingest; rejected if empty after trim. No control-
  // character class check, no casing rule, no length cap, no kebab-case
  // enforcement. Preserved verbatim; filter equality is case-sensitive.
  // Adapter selection is a separate concern (CLI flag --from <name>, code-level
  // registry); the source_agent string is set by the adapter at canonical
  // projection time and stored as-is.
  source_agent: string;

  // Source's session-creation time. NOT pond's ingest time
  // (which is tracked as a Lance row-level column, separately from canonical).
  created_at: utcDateTime;

  // User attribution: the shared-state scope this session belongs to.
  // Non-empty per invariant 20. Each adapter chooses an extraction chain
  // ending in a deterministic source-derived value (cwd, repo URL, agent
  // id). Case-preserved verbatim.
  project: string;

  // Extensibility bag. See 3.1.1 for namespacing.
  options: ProviderOptions;
}
```

What's intentionally NOT on Session (and where it lives instead):

| Field | Where it lives | Why |
|---|---|---|
| Token-usage aggregates | Derived from messages | Sync risk if canonical |
| Status (running/idle/...) | Not stored | Pond stores captured sessions, not live |
| Title | `options.title` | Display-only |
| Default model / agent_id | Per-message field | model_change events make per-session inaccurate |
| Harness runtime (resources, env, agent/tool snapshot, outcomes, `source_version`) | `options.source.*` | Pond is harness-agnostic |

#### 3.1.4 Message

Four role variants with per-role content allowlists, mirroring Effect's `Prompt.Message` shape. SystemMessage's `content` is a `string` (not a Parts array).

```typespec
model BaseMessage {
  id: MessageID;
  session_id: SessionID;             // back-reference to containing session
  timestamp: utcDateTime;            // source-recorded; canonical ordering within the session
  options: ProviderOptions;
}

model SystemMessage extends BaseMessage {
  role: "system";
  content: string;                   // plain text
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
- **No turn-level metadata fields** (model, provider, finish_reason, tokens, response_id, error). Effect places these on Response-side Parts (`FinishPart`, `ResponseMetadataPart`, `ErrorPart`) that pond does not store. Sources record this metadata on their assistant turns; SourceAdapters route it to `options.<provider>.*` (e.g. Anthropic-shape usage to `options.anthropic.usage.*`, OpenAI to `options.openai.*`). Cross-source normalization is a search/replay-layer concern (section 4), not a storage-shape concern.

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
| Compaction / branch-summary events | Synthetic `user`-role Message with summary text; details on `options.source.compaction` |
| Retry attempts | `options.source.retry.{attempt, prior_error}` on the Message |
| Step start/finish (sub-turn LLM-call brackets) | Token/cost as Lance columns; boundary metadata in `options.source.steps` |
| Snapshot / patch references | `options.source.snapshot.*` on Message or tool Part |
| Subtask / sub-agent spawn events | Separate Session per subtask, linked via `parent_session_id` |
| Editor context | `options.source.editor_context` on the `UserMessage` |
| Streaming start/delta/end variants | Not persisted (assembled state only) |
| Source/citation Parts | Deferred; not v1 |
| Tool definitions / available-tools list | `options.source.tools` per Session if the source provides it |
| Per-message error | `options.<provider>.error` per the source's wire format |
| Finish reason, token usage, model, provider, response_id | `options.<provider>.*` (e.g. `options.anthropic.usage.input_tokens`, `options.openai.finish_reason`) |
| Tool approvals as side-table | Pond keeps as Parts (Effect's shape), inside canonical |
| Per-Part timestamps | `options.source.time.*` on the Part |
| Synthetic orphan-tool-result sentinels | Not in canonical storage; replay layer (deferred, section 4) generates them |

#### 3.1.7 Adapter seam types

The adapter contract is enforced by three load-bearing types in `src/adapter/extract.rs`. Together they make invariant 15 ("no synthesized values") a compile error rather than a code-review violation, and invariant 18 (transport-agnostic) is true by construction because none of them know or care where source data came from.

**`Extracted<T>` (the seal).** Opaque wrapper around a value pulled from real source data. Has `Deref<Target=T>` for read access. Has **no public constructor reachable from adapter code**: the only producer is a module-private `wrap` function inside `extract.rs`, and the only callers of `wrap` are the `extract_*` helpers in the same module (plus a `#[cfg(test)] from_test_value` for in-crate unit tests and a `pub(crate) from_stored` for the Lance decode path in `src/sessions.rs`). An adapter that wants to put a string into `PartKind::ToolResult.name` cannot construct an `Extracted<String>` from a literal, a `.into()`, a `Default`, or any conversion - the only path is `extract_str(source, "name")`. Synthesis becomes a type error.

**`Source` trait (the transport-agnostic input).** Four primitive accessors over one row of source data: `str_field(key)`, `bool_field(key)`, `value_field(key)`, `nested(key)`. Two optional self-views: `as_str()` (when the row IS a primitive string) and `compact_repr()` (lossless string encoding of the whole row). Pond ships `impl Source for serde_json::Value` for JSON-flavored adapters; other adapters `impl Source for MyRow` where `MyRow` is whatever shape their format uses (deserialized struct, HTTP body type, database row, stream frame). The trait carries zero transport assumptions - file vs HTTP vs WebSocket vs queue subscription is invisible at this seam.

**`extract_*` helpers (the only producers).** `extract_str` / `extract_bool` / `extract_value` for field lookups; `extract_self_str` for "the source itself viewed as a string"; `extract_compact_repr` for lossless whole-row encoding fallbacks (always succeeds, used when an adapter wants to preserve bytes for an unrecognized subtype). These are the entire public surface for producing `Option<Extracted<T>>` from adapter code. Authors compose them with `or_else`, `map`, etc. to express conditional fallbacks - none of which can fabricate a value.

The same constraint that makes adapters honest also makes new-adapter onboarding trivial: an author writing the 7th adapter copies the pattern from claude-code or codex-cli, swaps in their `impl Source for MyRow`, and the compiler tells them every place they tried to take a shortcut.

### 3.2 Datasets

Pond stores the canonical types in 3.1 as four Lance datasets. Each dataset is a direct serialization of the corresponding canonical object - no projections, no promotions, no schema design beyond what 3.1 already defines. Open-ended fields (`options`, Part variant payloads) live as JSON; canonical scalars live as typed Lance columns; FilePart binary data uses Lance Blob v2.

#### 3.2.0 Lance write parameters

Applied to all four tables at create time. Tables are created via `nm.create_table(["<name>"], schema, write_params)` against the root Directory v2 Lance namespace (2.1.1, invariant 21); the write params flow through that call into Lance, not via a hand-rolled `Dataset::write`. The list below applies to every `nm.create_table` invocation:

- `data_storage_version`: latest stable (2.2+). Per 2.1.
- `enable_v2_manifest_paths`: true (Lance default). Constant-time latest-manifest lookups.
- `enable_stable_row_ids`: true. Load-bearing for compaction efficiency: with stable row ids, secondary indexes (IVF_PQ vector, BM25 FTS, BTREE / Bitmap scalar) survive compaction without being rewritten to point at new row positions. With it off (the Lance default), every compaction pass remaps every index entry, which on pond's index footprint (one vector index, one FTS index, ~9 scalar indexes across the four datasets) would multiply maintenance cost. The `_rowid`-join use case is a downstream consequence; the index-survival property is the primary reason.
- `auto_cleanup`: `older_than: 30 days` for personal pond (URI scheme: local filesystem), `older_than: 90 days` for hosted (URI scheme: `s3://`, `gs://`, `az://`). The longer-than-Lance-default window is defense-in-depth Lance manifest retention - the primary recovery path is "re-ingest from sources" (invariant 10), this window covers the rare case where source data is no longer reachable (sources deleted, API sessions expired). Cleanup only removes old Lance manifest versions and any fragments referenced only by them; it does NOT delete logical rows. Stored sessions/messages/parts/embeddings accumulate indefinitely until an operator runs an explicit delete operation (not in v1 wire surface). Operators with filesystem access can use `lance-cli` directly to inspect or roll back to a retained manifest version; pond does not expose CLI verbs for this in v1.
- One `lance::Session` is constructed at pond startup and shared across the four datasets **and the namespace's `__manifest` Lance table**. The Session carries the metadata + index caches and the `ObjectStoreRegistry` (the underlying object_store / S3 client). Sharing it means one cache pool covers all five Lance opens and one S3 client serves all of them - load-bearing on object-store backends, where per-dataset Sessions would mean 5x connection pools and 5x credential refreshes. Routed through `DatasetBuilder::with_session` on open (under the `Store` seam from invariant 21) and `WriteParams.session` on write.
- Object-store credentials, region, endpoint, and tunables flow from `config.toml [storage]` (or matching environment variables read by `object_store`) into `DatasetBuilder::with_storage_options` on open and `WriteParams.store_params` on write. Keys are the standard `object_store` config names (`AWS_ACCESS_KEY_ID`, `AWS_REGION`, `AWS_ENDPOINT`, etc.); pond does not parse or validate them. Empty block on local-FS installs.
- Unenforced primary keys declared at the schema level via `Field.unenforced_primary_key_position`. `merge_insert` defaults to using them, satisfying invariant 2 with no per-call boilerplate.

Maintenance execution: a background tokio task spawned by `pond serve` runs two operations per interval on each table:

1. `Dataset::cleanup_old_versions(older_than)` to remove old manifest versions and unreferenced fragments per the `auto_cleanup` window above. Default `delete_unverified: false` so in-flight write files newer than the verification threshold are skipped, preventing a cleanup-vs-write race.
2. `Dataset::optimize_indices(append)` to extend each scalar / FTS / vector index `fragment_bitmap` to fragments appended since the last build. Without this, freshly-ingested data has degraded filter pushdown until the next interval (Lance falls back to full-scan-with-predicate on uncovered fragments).

Interval default: 6h. Both operations logged at info level (versions removed, bytes reclaimed, fragments newly indexed, duration). Failures logged at warn and retried at the next interval; maintenance failures do not crash `pond serve`. `pond sync` always tail-calls the same logic after its `ensure_indices()` pass, so a CLI-only operator never has to remember a second verb; the call is a no-op when no rows were inserted. Both paths are safe against concurrent reads and writes (Lance OCC; Append-vs-Append commutes). Multiple pond processes running maintenance against the same dataset converge harmlessly. Configuration lives under `[maintenance]` in `config.toml` (`enabled`, `interval`, `retention`); the sync tail-call runs regardless of `enabled`.

#### 3.2.1 sessions

Registered as `["sessions"]` in the root Lance namespace (invariant 21). One row per Session.

| Column | Type | Notes |
|---|---|---|
| id | Utf8 | PK pos=1 |
| parent_session_id | Utf8? | session fork pointer |
| parent_message_id | Utf8? | cut-point in parent session |
| source_agent | Utf8 | NOT NULL; Bitmap (low cardinality per 3.4 canonical-strings table) |
| created_at | timestamp_micros | source-recorded |
| project | Utf8 | NOT NULL per invariant 20; BTREE (case-sensitive equality and prefix filter pushable) |
| options | Utf8 | JSON-serialized ProviderOptions |

#### 3.2.2 messages

Registered as `["messages"]` in the root Lance namespace (invariant 21). One row per Message (any role).

| Column | Type | Notes |
|---|---|---|
| session_id | Utf8 | PK pos=1; clustering pos=1; BTREE |
| id | Utf8 | PK pos=2; unique within session (source IDs may be locally-scoped per 3.1.1) |
| timestamp | timestamp_micros | clustering pos=2; source-recorded; BTREE |
| role | Utf8 | "system" / "user" / "assistant" / "tool"; Bitmap (4-value low-cardinality column; BTREE would prune nothing because every page's [min,max] covers every value) |
| source_agent | Utf8 | NOT NULL; denormalized from `sessions.source_agent` at ingest by pond core; Bitmap (typically 5-20 distinct values across the corpus). Filter pushdown surface only; `sessions` is the authoritative source for reads outside of search |
| project | Utf8 | NOT NULL; denormalized from `sessions.project` at ingest by pond core; BTREE (moderate cardinality, supports exact and prefix predicates). Filter pushdown surface only; `sessions` is the authoritative source for reads outside of search |
| content | Utf8? | non-null only for system role (Effect Prompt convention: SystemMessage.content is a plain string); non-system content lives as Part rows in 3.2.3 |
| search_text | Utf8? | indexed retrieval surface; populated at ingest by pond core via the concatenation policy in 3.3.1. Non-null for user and assistant roles when at least one indexable Part (TextPart or FilePart metadata) exists; null for system and tool roles, and null for any user/assistant message whose only content is non-indexed Part types (e.g. a bare ToolCallPart). FTS-indexed and consumed by the embedding worker (same string feeds both retrievers). |
| options | Utf8 | JSON-serialized ProviderOptions; response metadata (model, provider, finish_reason, tokens, response_id, error) lands under `options.<provider>.*` per the source's wire format (Effect's declaration-merging pattern); source/harness facts under `options.source.*`. Stored as JSON string (not Lance Struct) for additive-only evolution: any new provider key requires zero schema change. Empty options serialize as `"{}"` (no NULLs). Hot keys may be promoted to dedicated typed sibling columns additively (e.g. `messages.input_tokens Int64?` populated forward at ingest); the JSON column stays intact, promotion is reversible. |

Composite PK `(session_id, id)` lets pond preserve source-supplied IDs verbatim (per 3.1.1) without requiring them to be globally unique across sources or sessions. Clustering by `(session_id, timestamp)` keeps all messages of a session contiguous on disk for sequential session-walk reads.

Denormalized columns (`source_agent`, `project`) are immutable post-write: invariant 1 (append-only) plus pond core writers stamping them once at ingest from the Session event buffered per the 3.4 ordering contract. They are not user-writable via the wire surface (see 3.6.4). If a `sessions` row's `project` ever needs correcting, the recovery path is re-ingest, not in-place column update - matching the universal pattern observed across production Lance/LanceDB applications.

#### 3.2.3 parts

Registered as `["parts"]` in the root Lance namespace (invariant 21). One row per Part. Non-system message content lives here.

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

Registered as `["embeddings"]` in the root Lance namespace (invariant 21). One row per (Message, embedding model). Granularity is the Message - not the Part, and not a sub-message chunk: retrieval returns messages (per 3.3) and vector + FTS agree on row identity for RRF.

| Column | Type | Notes |
|---|---|---|
| message_id | Utf8 | PK pos=1; BTREE (enables `message_id IN (...)` prefilter for cross-table joins when needed) |
| model_id | Utf8 | PK pos=2; free-form string. Adapter may include a revision suffix (e.g. `Qwen/Qwen3-Embedding-0.6B@abc123`) when strict cache invalidation across upstream weight updates is required. Without a suffix, re-embeds with the same `model_id`, `max_embed_tokens`, and `text` overwrite prior rows. Pond does not parse this field. |
| max_embed_tokens | Int32 | PK pos=3; the tokenizer truncation point this vector was embedded under. Part of the key because it selects which prefix of an over-cap message is embedded, so it is part of the vector's identity: changing the cap produces a distinct row under this key rather than silently leaving a stale vector. The embed worker's anti-join (which messages still need embedding) matches on this column too. |
| vector | FixedSizeList&lt;Float32, N&gt; | dim N is per-model |
| session_id | Utf8 | NOT NULL; denormalized from `messages.session_id` at ingest by pond core; BTREE (high cardinality, supports `session_id = X` prefilter on vector kNN) |
| source_agent | Utf8 | NOT NULL; denormalized from `messages.source_agent` at ingest by pond core; Bitmap (low cardinality) |
| project | Utf8 | NOT NULL; denormalized from `messages.project` at ingest by pond core; BTREE |
| role | Utf8 | NOT NULL; denormalized from `messages.role` at ingest by pond core; Bitmap (4-value column; only `user` and `assistant` rows actually exist in this table since system/tool produce no embeddings per 3.3.1, but the column is declared for filter-pushdown completeness) |
| timestamp | timestamp_micros | denormalized from `messages.timestamp` at ingest by pond core; BTREE (supports `from_date`/`to_date` prefilter on vector kNN) |

Denormalized columns are immutable post-write (same rule as 3.2.2). They exist to enable single-stage filter pushdown for vector kNN without cross-table joins (Lance has no relational join planner with the crate stack pond uses; see 3.3). Dictionary encoding for low-cardinality denorm columns (`source_agent`, `role`) is auto-detected by Lance v2.2+ at fragment-write time; small ingest batches under the 100-row threshold under-encode until compaction merges fragments (handled by the maintenance task per 3.2.0).

Input text for embedding is `messages.search_text` (3.2.2), produced by the concatenation policy in 3.3.1. Messages with NULL `search_text` (system and tool rows) produce no embedding rows.

Vector index: IVF_PQ on `vector`. Distance: cosine (Qwen3 vectors are L2-normalized by the model; matches its training objective). Defaults for the 1024-dim Qwen3-Embedding-0.6B case: `num_partitions = max(32, min(4096, round(sqrt(num_rows))))` (recomputed at each index build); `num_sub_vectors = 64` (16-float PQ codebooks); `num_bits = 8` (Lance default, 256 centroids per codebook). Activation threshold: 10,000 rows in the embeddings table - below that, queries use a flat exact scan (Lance handles this transparently when no index exists). When a future model with a different `dim` lands, the chosen `num_sub_vectors` for that model lives in the registry entry; no universal formula is committed in code.

Index rebuilds: auto-triggered by Lance when fragments added since last build exceed the auto-index threshold; manual via `pond optimize` is not exposed in v1 (auto-trigger only).

Multi-model coexistence: multiple rows per message within one table - one per model - while dims match; a second embeddings table per model is the activation path when a model with a different dim ships.

Loader mechanics: Qwen3 is reached via fastembed-rs's `Qwen3TextEmbedding::from_hf` API (distinct from fastembed-rs's standard ort-backed `EmbeddingModel` enum, which does not include Qwen3); the loader runs on the candle backend, gated by fastembed-rs's opt-in `qwen3` Cargo feature. Device selection: Metal GPU on macOS (target-gated `metal` feature, day-one), CPU by default on Linux, NVIDIA GPU on Linux when built with the opt-in `cuda` feature. Pond passes the device via candle's `metal_if_available` / `cuda_if_available` helpers into `Qwen3TextEmbedding::from_hf`.

Embedding granularity: one vector per message, no chunking. On the `~/.claude/projects/` corpus (689,702 messages), `search_text` is 20 tokens at the median and ~98% of messages are under 1024 tokens; chunking would multiply `embeddings` rows for the ~2% over the cap only for RRF to dedup them back to one hit per message. `max_embed_tokens` (default 1024) is passed as `Qwen3TextEmbedding::from_hf`'s tokenizer `max_length`, so fastembed-rs truncates input before inference - pond owns no tokenizer. The cap is a model-cost bound on the p99 tail, not a retrieval-quality knob: FTS (3.3) indexes the uncapped `search_text` and `pond_get` (3.6.3) always returns the full message. Truncation is deterministic, so re-ingest is a no-op; `max_embed_tokens` is a PK column on this table for exactly this reason - changing the cap re-embeds the affected over-cap tail under a distinct row instead of silently overwriting. Output dim is the model's fixed 1024 (no Matryoshka in fastembed-rs); variable dims are deferred (section 4).

Embedding model registry: TOML at `[[embeddings.models]]`, with built-in defaults shipped in the binary (pond runs without a user config). Each entry: `{ id, dim, max_embed_tokens, num_sub_vectors, distance, normalize, default }`; `id` doubles as the HuggingFace repo (minus any `@revision` suffix). pond validates against its known-model set at startup and fails fast on unknown id, dim mismatch, unsupported distance, or zero `default = true` entries. Adding a model pond already knows how to load is config-only; new loaders / remote providers (section 4) still require code. Per-namespace overrides at `[embeddings.overrides.<namespace>.<model_id>]` are limited to `max_embed_tokens` and `num_sub_vectors`; `dim` / `distance` / `normalize` are immutable because changing them would invalidate stored vectors. `max_embed_tokens` is overridable because it is a PK column on `embeddings`: an override produces vectors under a distinct key rather than corrupting existing ones.

### 3.3 Search surface

Hybrid (vector + BM25 + RRF) by default, at message granularity (vector index keyed on message_id per 3.2.4; FTS index on `messages.search_text` per 3.2.2). Filters: `project`, `session_id`, `from_date` / `to_date`, `role`, `source_agent`, `min_score`, `boost_recent`, `group_by_conversation`, `limit`. The `include_tool_results` / `include_thinking` toggles are NOT search filters; they live on `pond_get` (3.6) and govern which Part types are returned at retrieval time. The search corpus is fixed by the concatenation policy in 3.3.1 - what isn't in `search_text` cannot be found via search.

`project` is a canonical Session field (3.1.3) stored case-sensitive verbatim, denormalized onto `messages` and `embeddings` for filter pushdown (3.2.2 / 3.2.4). The filter is a tagged enum: `{"contains": "<substring>"}` or `{"regex": "<pattern>"}`. The `contains` form emits `LIKE '%<value>%' ESCAPE '\\'` and is the CLI default (`pond search --project pond` -> contains "pond"). The `regex` form emits `regexp_like(project, '<pattern>')` for cases callers can't express as a substring; regex never pushes down to BTREE so it's a full-fragment scan-with-predicate, acceptable for human-driven queries but not for hot paths. There is no `is_null` variant - invariant 20 requires `Session.project` to be non-empty, so no rows match.

Case-insensitive search is the caller's responsibility (fold case before submitting, or use a `(?i)` prefix in the regex form); pond does not normalize at storage or filter time. Same convention applies to `source_agent` and `session_id`.

`role` accepts a single value (`"user"` | `"assistant"` | `"system"` | `"tool"`). System and tool values are accepted on the wire but always return empty (those rows have NULL `search_text` and no embeddings per 3.2.2 / 3.2.4).

`boost_recent` is a boolean on the search request (default `true`). When set, an additive exponential-decay boost is applied to each result's base score: `boost = 0.2 * exp(-age_seconds / 604800)` where `age_seconds` is `now - message.timestamp` and `604800` is 7 days in seconds. The boost caps at `+0.2` (at `age = 0`) and decays to near-zero past a few weeks. Constants are not empirically tuned by pond and should be revisited when retrieval-quality measurement is available.

`group_by_conversation` is a boolean on the search request (default `false`). When `true`, results collapse to one summary object per `session_id`, with fields: `session_id`, `project`, `source_agent`, `first_timestamp` and `last_timestamp` (min/max across matching messages), `message_count` (total messages in the session, via a separate count query against the `messages` table - NOT the count of matches), `preview` (truncated `search_text` from the best-scoring matched message), and `best_score` (`max(score)` across matches in the session). Summaries are sorted by `best_score` descending then limited.

Filter pushdown: every search filter column is colocated on the queried table via the denormalization in 3.2.2 (messages: `project`, `source_agent`, `role`, `session_id`, `timestamp`) and 3.2.4 (embeddings: same set). The FTS query on `messages.search_text` and the vector kNN on `embeddings.vector` each push their predicates into the table-level scalar indexes (BTREE for `project`/`session_id`/`timestamp`, Bitmap for `source_agent`/`role`) before retriever ranking - produces correct top-k without postfilter underrun and without cross-table joins. RRF merges on `message_id` - with one embedding row per message per model, vector results are already message-unique (no per-chunk dedup). `min_score` is applied postfilter after RRF and recency boost (not a Lance-pushable predicate). The `Scanner::prefilter(true)` call this depends on is required by invariant 13.

Retrieval modes (handled by `pond_get` per 3.6, not search): single message, single message with N thread-context messages above and below, full conversation, conversation up to a message.

#### 3.3.1 Indexed content and concatenation policy

`messages.search_text` is populated at ingest by a single pond-core function applied uniformly to every canonical Message. The function is the only knob; per-source customization is rejected to keep search corpus shape predictable across all adapters.

Per-role concatenation:

| Role | search_text content |
|---|---|
| system | NULL (not indexed; retrievable via `pond_get`) |
| user | TextPart.text values, plus `FilePart.file_name`, `FilePart.media_type`, and the URL string when `FilePart.data` is the `url` variant. All concatenated with newline separators in `ordinal` order. |
| assistant | Same as user: TextPart and FilePart metadata only. |
| tool | NULL (not indexed; retrievable via `pond_get`) |

Only `TextPart` (any role) and `FilePart` metadata feed the index. Every other Part type is canonically stored and retrievable via `pond_get`, but never via search. The motivating rule: "search the conversation, not the plumbing." A tool call body, a thinking block, a tool result, and an approval round-trip are all plumbing - they belong on the retrieval side, where the operator asks for them explicitly via `include_thinking` / `include_tool_results`.

What's deliberately NOT indexed:

- `ReasoningPart.text` - thinking traces.
- `ToolCallPart.*` - tool name, params, the lot. Tool calls remain retrievable via `pond_get`.
- `ToolResultPart.*` - tool output, often megabytes of structured data or scraped content.
- `ToolApprovalRequestPart`, `ToolApprovalResponsePart` - operational plumbing, never search-relevant.
- `FilePart.data` payload (decoded base64 / bytes) - indexing file contents is a deferred feature (section 4).
- SystemMessage content - boilerplate harness prompts dominate BM25 IDF and cluster vector embeddings.

Consequence: an assistant message with no `TextPart` and no indexable `FilePart` (e.g. a turn that is one bare tool call) produces empty content; the builder returns `None` and the row's `search_text` is stored as NULL. NULL rows are absent from the FTS index and produce no embedding rows per 3.2.4 - they vanish from search but are still returned in full by `pond_get`.

The embedding worker reads `messages.search_text` directly (no second concatenation path). Vector input and FTS input are byte-identical.

`search_text` is computed at each Message boundary inside pond core's session-substream buffer; the surrounding ingest flow (Session -> Message -> Parts substream buffering, three `merge_insert` batches per session) lives in 3.4 "Session-batched append granularity."

Concat policy changes require re-ingest. Per invariant 14 sync is purely additive: `merge_insert` uses `WhenMatched::DoNothing`, so existing rows keep their old `search_text` until they're explicitly migrated (a deferred per-row migration operation, not a sync flag). The clean-store path - drop the affected rows and re-ingest - rebuilds `search_text` and the embeddings that depend on it.

### 3.4 Ingest surface

`SourceAdapter` is pond's per-source plug-in trait. v1 ships the Claude Code and Codex adapters; section 4 lists the others on the roadmap.

**Source configuration.** Each adapter type gets one `[sources.<adapter>]` block in `config.toml`:

```toml
[sources.claude-code]
path = "~/.claude/projects"

[sources.codex-cli]
path = "~/.codex/sessions"
```

`~` and `$HOME/` prefixes in `path` are expanded at load time. `pond sync` with no `<adapter>` argument syncs every registered entry; `pond sync <adapter>` syncs just that one. An empty `[sources]` block triggers interactive discovery: each adapter probes its own canonical install location (`<Adapter>::discover_under(home)`), `pond sync` presents a multi-select prompt over the candidates, and the chosen rows are written back to `config.toml` via `toml_edit` (preserves user comments) before the sync proceeds. Non-tty stdin errors with a "configure manually" message rather than hanging on the prompt.

Per-adapter discovery rules live on each adapter type, not in a centralized name->path table: a new adapter ships its own `discover()` heuristic alongside its parse code.

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

**Ordering enforcement.** Pond core's ingest path validates the contract per-event: (a) the first event of a session's substream must be `Session`; (b) every `Part` event must carry a `message_id` matching the most recent `Message` event since the last boundary; (c) a `Message` event must reference an already-seen `Session.id` (in the current stream or in the dataset); (d) message_ids within a substream are unique; part_ids within a message are unique.

**The unit of abort is the offending event, not the substream.** A violation surfaces as one `validation_failed` outcome (3.6.1) for the offending event and the substream continues from the next valid anchor. The rest of the session's events still land. Two consequences worth naming:

1. *Per-event drop, not cascade.* A Part with the wrong `message_id` removes that one Part, not the 130 events around it. The `IngestSummary` reports `dropped_events` (in-session per-event drops) and `dropped_sessions` (whole-substream rejections) as distinct populations so the operator can tell one corrupted line from one corrupted session.
2. *Whole-substream rejection survives only for Session-level invariants.* The `source_agent` / `project` immutability check (3.6.4) compares the buffered Session against a previously stored row; if they disagree, the buffered Session has no valid anchor to write under and its substream is dropped wholesale - that's the `dropped_sessions` case. Within-substream ordering / duplicate violations stay per-event.

Invariant 5 (no silent drops) is met by surfacing every drop via a `SyncEvent` (CLI sync) or a per-row `validation_failed` outcome (HTTP `pond_ingest`). The CLI `decode` stream carries exactly one session, so drops there happen inline as the file is parsed; the HTTP batch (3.6.4) may carry many sessions, with drops scoped per session. Both transports run the identical validator with identical semantics - there is no transport-specific behavior. The check is O(1) per event; the buffered Session, current Message, and seen-id sets are the only state.

**Canonical `source_agent` strings.** The adapter is responsible for stamping `Session.source_agent` at canonical-projection time per 3.1.3 (trimmed, non-empty). To keep cross-source filters predictable, pond reserves the following canonical strings for v1 + roadmap adapters; each adapter MUST emit one of these values:

| Adapter | `source_agent` value(s) |
|---|---|
| Claude Code (v1) | `claude-code` |
| Codex CLI (v1) | `codex-cli` |
| OpenCode | `opencode` |
| OpenClaw | `openclaw` |
| nanoClaw | `nanoclaw` |
| pi-mono | `pi` |
| Claude desktop (local-agent-mode) | `claude-app` |
| Anthropic Managed Agents | `anthropic-managed-agents` |

Additional source surfaces ship adapter-defined strings; the table here is amendment-only.

**Per-adapter `Session.project` derivation rules** live as docstrings on each adapter's source-decode function (the implementation is the documentation; design.md stays focused on the cross-adapter contract). Invariant 20 fixes the contract: every adapter MUST emit a non-empty `Extracted<String>`. The `Source` / `Extracted<T>` seam (3.1.7) makes synthesized sentinels a compile error - the chain ends in a deterministic source-derived value (a path-encoded directory name, an agent id, a repo URL) or the session is dropped with `DROP_REASON_MISSING_PROJECT`.

**`parent_session_id` is a soft foreign key.** Pond core does not validate that a forked session's `parent_session_id` references an existing session row at ingest. Forks against missing parents (real case: nanoclaw subagent files referencing parent sessions absent from disk) are stored as-is; consumers traversing fork lineage handle dangling pointers. This avoids ordering constraints between independent adapter runs and matches the append-only invariant (the parent might land in a later ingest pass).

**Session-batched append granularity.** Pond core buffers a whole session's `decode` substream and writes it in at most three `merge_insert` batches per session - one `sessions`, one `messages`, one `parts` - each keyed on the canonical PKs (`Session.id`, `(session_id, Message.id)`, `(message_id, Part.id)`). Per-event / per-row commits are explicitly not used: they multiply Lance manifest versions and fragment rewrites with no benefit. Re-ingest is idempotent (invariant 2): re-reading an already-ingested session is a no-op for matched rows.

**Per-session staleness skip.** `pond sync` skips re-decoding a session when the source file's `mtime` is at or before the wall-clock time at which pond last wrote that session's row. The watermark is `Lance manifest version timestamp keyed by sessions._row_last_updated_at_version` (Lance's per-row system column, available because pond enables stable row ids per 3.2.0): one scan over `sessions` projecting `id` + `_row_last_updated_at_version`, joined against `Dataset::versions()` for commit times. No new column, no checkpoint table, no `Session.options` stamping - the watermark is built-in Lance metadata. Adapters opt in by overriding their streaming method to consult the watermark and emit a `Skipped` outcome; adapters with no per-session file model (rotation siblings, DAG forks, HTTP-backed live streams) ignore the hook and decode every time.

**Live-write deferred.** End-of-session import is the v1 default; live-write is deferred (section 4) and activates additively by adding a `follow(SessionRef) -> impl Stream<Item = Result<IngestEvent>>` method (or a separate `LiveSourceAdapter` trait) - same Stream shape, infinite stream.

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

CLI verbs (out-of-band):

- `pond sync [<adapter>]` - parse, store, and index data from one configured `[sources.<adapter>]` (or every entry, with no arg). On an empty `[sources]` block runs interactive adapter discovery against each adapter's canonical install location, writes the picks back to `config.toml`, and continues. Always applies the per-session staleness skip where the adapter supports it (3.4). Wire layer stays `ingest`-named (`pond_ingest`, `IngestRequest`, `IngestEvent`); only the CLI verb is `sync`.
- `pond embed` - walk the un-embedded message backlog and produce vectors under the default model identity. Idempotent on `(message_id, model_id, max_embed_tokens)`; safe to re-run.
- `pond status` - row counts across the four datasets.
- `pond serve` - HTTP server, including the `/mcp` streamable-HTTP MCP route. Loads the embedding model only when `[embeddings] enabled = true`.
- `pond mcp` - stdio MCP server only; stdout reserved for JSON-RPC frames. Same embeddings-flag gate as `pond serve`.
- `pond config --print-schema` - emit the annotated config.toml template.
- `pond export [--session <id>] [-o <path>]` - stream every stored session as JSONL `IngestEvent`s, byte-identical with what `pond_ingest` / `pond ingest` consume on input. Default output is stdout; `--session` filters to one session id. Useful as a portable backup before risky operations, for cross-machine migration, and as the cheap snapshot primitive recovery flows depend on.

pond has no separate `setup` verb: the data dir is created on first `Store::open`, and the embedding model is fetched on the first run that loads it (hf-hub cache).

**Recovery model.** pond is the durable copy (invariant 10): sessions persist past their source's lifetime, so recovery cannot assume the source is still reachable. The primary recovery surface is Lance's manifest history - 3.2.0 `auto_cleanup` keeps old manifest versions for 30 days locally / 90 days on object stores, inspectable and rollback-able via `lance-cli` directly. Beyond that window, the operator's defense is `pond export` ahead of risky operations: a portable JSONL snapshot of every stored session, byte-identical with `pond_ingest` input, so `pond export -o backup.jsonl` plus `pond ingest backup.jsonl` is a clean restore path that does not depend on the original adapter sources. `rm -rf $POND_DATA_DIR && pond sync` is a re-ingest, not a recovery: re-ingest is idempotent on the canonical PKs (invariants 1 + 2) and brings back exactly the sessions whose sources are still reachable, no others. pond does not ship `versions list` / `checkout` / `restore` CLI verbs in v1; the time-travel-as-feature use case (point-in-time queries, audit endpoints) is deferred to section 4.

#### 3.6.1 Error envelope

Same envelope for HTTP and MCP. Success and error are mutually exclusive at the body level.

Error body:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "filters.project must be one of: {contains: string} | {regex: string}",
    "details": { "field": "filters.project", "value": "wildcard" }
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
  "rrf_k": 60,
  "filters": {
    "project": { "contains": "pond" },
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
- The mode is not a request input. The server picks hybrid or fts based on the embedder state; the response carries no top-level mode field - per-hit `matched_via` reports which retriever(s) ranked each row (see 2.5).
- `rrf_k`: default 60. Consulted only when the server runs hybrid mode.
- `filters.project`: `null` (no filter) | `{ "contains": "<substring>" }` | `{ "regex": "<pattern>" }`. Regex never pushes down to the BTREE index (full-fragment scan); contains uses LIKE pushdown. There is no `is_null` form - invariant 20 makes `Session.project` non-empty.
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
- `score`: final ranking score. Hybrid mode = RRF + recency_boost; FTS-only mode = normalized BM25 + recency.
- `matched_via`: per-hit retriever provenance (`["vector"]`, `["fts"]`, or `["vector", "fts"]`). Carries the retriever-state signal at per-hit granularity instead of a top-level `search_mode` field; also useful for debugging hybrid ranking.
- `base_score`: score before recency boost. Always reported.
- `recency_boost`: additive bump; `0` when `boost_recent: false`.
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
- **Immutable session-level fields.** `Session.source_agent` and `Session.project` are immutable post-first-write. A `kind: "session"` event whose `data.id` matches an existing session row and whose `source_agent` or `project` differs from the stored row returns per-row `validation_failed` with `details: { field: "source_agent" | "project", reason: "immutable" }`. Other Session fields (`options`, `parent_session_id`, `parent_message_id`, `created_at`) on a re-submitted session are silently ignored (per invariant 14, sync is purely additive: matched rows are no-ops). Recovery from a wrong `source_agent`/`project`: delete the affected session rows and re-ingest (delete is not in the v1 wire surface; admin operation only).

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

**Functional extensions** (single-tenant or substrate-shape work):

- **Resources application.** Per-namespace knowledge-base files (blobs + metadata) as a second consumer alongside sessions. Activation: a concrete second consumer; adding it is mechanical (new Lance table, same connection).
- **Cross-provider replay engine.** Re-projecting canonical Parts into provider-specific request shapes (Anthropic, OpenAI, Bedrock, Gemini, etc.). Activation: first integrator demand. Also gates the full pi-mono cross-provider conformance matrix.
- **Cross-provider replay tests with live API calls.** The full pi-mono test that sends fixtures to real provider APIs. v1 ships fixture data plus storage round-trip tests only.
- **Live-write tools.** `pond_commit`, `pond_session_open` (and HTTP equivalents) for streaming events written as they arrive. Substrate: Lance **MemWAL** (per-shard WAL + in-memory MemTable for durability without per-event base-table commits), the **LSM scanner** (unifies reads across base table + flushed MemTables + in-memory MemTables; uses `index_catchup` so reads stay indexed during async rebuild), and **HNSW-on-memtable** (fresh-tier vector search so live-written rows are searchable within seconds without waiting for IVF_PQ rebuild on the base table). **ShardWriter** with a `bucket(session_id, N)` sharding spec activates only when write concurrency exceeds OCC retry tolerance (cron-sync from a handful of VMs does not; hosted live-write fan-out does). Activation cost is bounded to a substrate swap because invariants 21-28 already align pond with MemWAL preconditions (centralized open / scan / write seams, unenforced PKs, stable row ids, shardable PK pos 1, no sub-second freshness contract, no cross-shard atomicity). Streaming-event variants (Effect's `text-start`/`text-delta`/`text-end`, `reasoning-*`, `tool-params-*`) land in the WAL on every chunk and are queryable from the MemTable before flush; only assembled final Parts ever merge into `parts.lance`, so the base table stays append-only per invariant 1.
- **Wire-fidelity capture.** `raw_request` / `raw_response` columns plus middleware capturing provider wire bytes. Activation: when replay reactivates and audit-grade fidelity is required.
- **Additional source adapters.** OpenCode, Cursor, aider, Gemini CLI, ChatGPT, others. Each is a new `SourceAdapter` impl with no impact on substrate.
- **Remote embedding providers.** OpenAI, Voyage, Cohere, custom. Activation: model demand beyond local fastembed-rs default.
- **Cross-session attachment dedup.** Background job over `content_hash` merging duplicate FilePart payloads. Activation: when storage cost from duplicates is measurable.
- **Graph traversal layer** (Kuzu or `lance-graph`). OpenCypher engine over the same Lance storage when `parent_id` recursive lookups become a bottleneck.
- **Wire-API-surfaced time-travel / friendly historical queries.** Lance's per-commit versioning is preserved by 3.2.0 `auto_cleanup` retention but kept out of pond's CLI / wire surface (recovery rides invariant 10, not in-place rollback). Activation: time-string `--as-of` queries, audit endpoints, `pond versions list` / `pond checkout` / `pond restore` verbs. Adds `version: int?` to read-side wire requests and a `pond_versions` operation; no schema changes since Lance owns the mechanism.

**Hosted multi-tenant deferrals** (activate when the first hosted tenant lands; share the multi-namespace router seam from invariants 11-12):

- **Hosted-tier multi-tenant via Lance namespace nesting.** Each pond tenant maps to a child Lance namespace `[<tenant>]` under the root; tables become `[<tenant>, "sessions"]` etc. Pond is a Lance namespace consumer at the storage seam (invariant 21), so swapping backends is `connect("<impl>", props)` with no other code change. The `lance-namespace-impls` Directory v2 backend handles in-bucket isolation; the REST namespace impl (or any Glue / Unity / Polaris / Iceberg REST adapter) handles the case where the integrator already runs a catalog with credential vending, listing, and access control. KMS isolation (separate bucket per tenant) and sub-tenant project namespacing (separate child namespace per project) both ride this seam without further pond changes.
- **AuditSink.** First-class audit-event subsystem for compliance.
- **EventBus.** Change-event notifications via channel or PubSub for external systems reacting to pond writes.
- **SecretsRedactor.** Indexer hook scrubbing API keys, tokens, PII from `search_text` and embedding inputs before write.

