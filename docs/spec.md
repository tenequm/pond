# Pond - Specification v1

pond stores agentic-client sessions: it ingests them from many client formats into one canonical form, keeps them in Lance, searches them, and hands them back. One static binary, two transports, two deployments. This document specifies pond v1.

## Contents

1. [Overview](#overview) - what pond is, the interchange-hub model, the stack, how to read this document.
2. [Scope](#scope) - what v1 ships and the stable non-goals.
3. [Storage substrate](#substrate) - the generic Lance engine every consumer builds on.
4. [Canonical model](#model) - the Session / Message / Part interlingua.
5. [Session datasets](#datasets) - how the canonical model persists in Lance.
6. [Adapters](#adapters) - the bidirectional codec between client formats and canonical.
7. [Protocol](#protocol) - the wire interface, operations, and CLI verbs.
8. [Search and embeddings](#search) - hybrid retrieval and the embedding seam.
9. [Deferred](#deferred) - work scoped out of v1.
10. [References](#references) - external work that informed this design.

---

## 1. Overview {#overview}

This section states what pond is and the single idea the rest of the document elaborates. Read it first - it is the map for everything below.

### 1.1 What pond is

pond ingests sessions from agentic clients - Claude Code, Codex, and others on the roadmap - into one canonical form, stores them in Lance, and serves hybrid search (vector and keyword, fused) over them at message granularity. It ships as a single static binary that exposes two transports - an HTTP+JSON API and an MCP server - over one shared set of handlers, and runs in two deployments (Section 2.2).

### 1.2 The interchange-hub model

The canonical Session / Message / Part schema is not merely how pond stores data - it is a format-neutral interchange representation, an interlingua. Every adapter is a bidirectional codec: it parses a client's format into canonical, and serializes canonical back into a client's format. Because every session passes through one canonical form, any adapter can restore any session - a session need not return to the client that produced it. The richness and stability of this schema is pond's product; everything else is machinery around it.

### 1.3 "Lossless" means value-complete

Throughout this document, *lossless* means every value round-trips as an equal value - it does not mean the bytes are identical. Restoring a session is a rederivation from canonical, not a byte replay, so incidental encoding (whitespace, JSON key order, equivalent number forms) is not data and is not preserved. Restoring a session with the adapter that produced it is lossless in this value-complete sense.

### 1.4 Preservation over convenience

When a design choice pits faithful preservation of a session against convenience - readability, storage size, a tidier schema - preservation wins. pond is, before anything else, a lossless record of agentic sessions.

### 1.5 The stack

- Language: Rust.
- Storage and search: the Lance columnar format, used through the `lance-format/lance` crates directly. pond does not depend on the `lancedb` crate, and does not wrap Lance behind a storage abstraction of its own - Lance is the engine, not something hidden behind one.
- Async runtime: tokio.
- HTTP transport: axum. MCP transport: rmcp.
- Object stores: local filesystem, S3, GCS, and Azure, all through Lance.
- Wire format: JSON - one schema, versioned additively.

### 1.6 The shape

A session's path through pond:

```
  client formats    canonical (interlingua)            restore targets

  claude-code --.                                  .--> claude-code
  codex       --+--> Session / Message / Part  ----+--> codex
  others      --'                                  '--> provider APIs
                                 |
                                 v
                     storage substrate (Lance)
                                 |
                                 v
                       search  /  get
```

Many client formats parse into one canonical model; any adapter can serialize that model back out, to a harness format or - deferred - to a provider API shape. Canonical persists in the storage substrate - a generic Lance engine that search and get both read from. The session datasets are merely its first consumer; Section 9 names the rest.

### 1.7 How to read this document

1. Sections 3 through 8 are ordered foundation-first: the storage substrate (3) before the canonical model and the consumer built on it (4 through 8). The substrate is the engine every current and future consumer shares; specifying it first, with no reference to sessions, is what keeps it honestly generic.
2. The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are used as defined in RFC 2119 and RFC 8174.
3. Operational rules carry a short mnemonic identifier prefixed by topic - for example, `lance-append-only` (substrate), `model-no-synthesis` (canonical), `adapter-integrity-additive-sync` (adapter). Source code references a rule by its identifier, so the identifiers are stable. Each rule states its constraint and, compactly, why it exists; the reason is part of the contract - it is there so the rule is not later "simplified" away by someone who cannot see what it defends.
4. This document specifies contracts and behavior. Implementation specifics - exact type and method names, tuning constants, file and module layout - live in the code, which is their source of truth.

---

## 2. Scope {#scope}

pond v1 is deliberately narrow: one application, an adapter registry, two deployments. This section fixes that boundary. Work scoped out of v1 is in Section 9; the non-goals here are different - they are stable positions, not deferrals.

### 2.1 What v1 ships

1. **One application: sessions.** Lossless ingest, storage, and hybrid search of agentic-client sessions. Sessions are the first consumer of the storage substrate (Section 3); future consumers are in Section 9.
2. **An open adapter registry.** Source-format support is defined by the registry in code, not enumerated or capped by this specification. Every registered adapter is a bidirectional codec under Section 6: it parses its own format into canonical and serializes canonical back to a client format.
3. **Two transports.** An HTTP+JSON API (primary) and an MCP server, both dispatching to one shared set of handlers.
4. **Two deployments: personal and hosted.** Described next.

### 2.2 Deployments

- **Personal.** One binary, one local Lance directory, a single hardcoded namespace. The whole pond belongs to the operator. It is single-user and binds to localhost by default; configuration and data follow the XDG base-directory convention.
- **Hosted.** The same binary against an object-store URL. Each tenant is an opaque `namespace` string the integrator supplies; the integrator owns identity, access, and request routing.

### 2.3 Non-goals

These are stable positions. pond will not:

- **Reinvent what Lance provides.** Storage, indexing, schema evolution, optimistic concurrency, blob columns, versioning, and time-travel are all Lance. pond uses Lance directly, not behind a parallel abstraction.
- **Invent a wire format for its canonical types.** The canonical types (Section 4) are pond's own serde structs - pond owns their schema and controls their evolution, with no upstream wire format to track.
- **Authenticate, authorize, or model identity or tenancy.** An integrator decides who may reach which namespace before any pond call; `namespace` on the wire is an opaque routing string pond does not interpret. On hosted deployments the object store's IAM is the storage boundary and the integrator's gateway is the application boundary.
- **Encrypt at the application layer.** Encryption is bucket server-side encryption plus filesystem encryption; pond holds no keys and adds no cryptography of its own. pond is not a zero-knowledge store - an operator with bucket and key access can read everything.
- **Act as a runtime.** pond does not execute tools, run an agent loop, compact context, render output, or emit telemetry. It stores what those systems produce.
- **Offer a SQL surface, a UI, or a sidecar daemon.** The query surface is the search and filter API of Section 8, which compiles to Lance scalar predicates and search calls; there is no SQL. The only engine is embedded Lance.

### 2.4 Platform

Linux and macOS. Windows is not in v1 scope.

---

## 3. Storage substrate {#substrate}

The storage substrate is the layer that owns how pond uses Lance - opening datasets, scanning, writing, concurrency, retention. It knows nothing of sessions: a consumer hands it table schemas and gets a place to store and query rows. It is specified first, and generically, because that is what keeps it reusable by every consumer that follows.

### 3.1 Purpose

A consumer - the session datasets in v1, others later (Section 9) - does not use Lance directly. It declares its tables to the substrate and then stores and queries rows through it. The substrate guarantees durable append-only storage, safe concurrent writers, retry around transient faults, and bounded read staleness. It does not interpret rows: column meaning, indexes, and denormalization belong to the consumer (Section 5 for sessions).

### 3.2 Lance chokepoints

Every interaction with Lance funnels through one of four chokepoints, each a single code path. Together they are the **`lance-chokepoints`** rule {#lance-chokepoints}; each chokepoint below is referenced as `lance-chokepoints-<name>`.

**catalog** {#lance-chokepoints-catalog} - Every dataset open MUST resolve the table's location through one catalog lookup; no code constructs a dataset path directly. Why: the catalog is where a local directory layout is swapped for a hosted catalog - centralizing it makes hosted multi-tenancy a configuration change, not a cross-cutting edit.

**read** {#lance-chokepoints-read} - Every scan and search query MUST be built through the substrate's read path. Why: it is the one place `search-prefilter-pushdown` (Section 8) is enforced, and the one place a future scanner change lands.

**write** {#lance-chokepoints-write} - Every write MUST go through the substrate's merge-insert path. Writes through this chokepoint never fold indexes - index lifecycle lives under `lance-index-maintenance` (Section 3.7). Why: `lance-append-only` and `adapter-integrity-additive-sync` (Section 6) hold only with a single write chokepoint; a direct write bypasses both.

**storage** {#lance-chokepoints-storage} - Every read, list, or write of dataset bytes MUST go through Lance's object-store layer; no code resolves a dataset to a local path. Why: a `file://` pond and an `s3://` pond behave identically only when no code reaches around Lance; a single direct-FS access silently backend-locks that operation.

### 3.3 Data integrity

**`lance-append-only`** {#lance-append-only} - Stored rows MUST NOT be mutated; an update produces a new row or a new manifest version. Why: it forecloses corruption-by-mutation and makes every write idempotent under retry.

**`lance-deterministic-pk`** {#lance-deterministic-pk} - Every row MUST have a deterministic primary key - source-supplied where the source carries a stable id, content-derived otherwise. Writes merge-insert on the key, so a retried or re-run write is a no-op for rows already present. Why: idempotent ingest depends on the key being reproducible from the source data alone.

**`lance-dataset-schema-version`** {#lance-dataset-schema-version} - Schema versioning lives at the dataset level - the Lance manifest and a dataset-level metadata key - never as a per-row column. Why: a per-row version column pays storage on every row for a fact that is per-dataset.

### 3.4 Dataset parameters

Every table is created with the current stable Lance file format, constant-time latest-manifest lookup, and a short manifest-retention window. v1 recovery beyond the window is via `pond export` snapshots; deferred named-snapshot preservation via Lance tags is in Section 9. The retention window MUST stay above the longest single read (floor: one hour): a reader pins the manifest version it opened only for that request's duration, and version cleanup that reclaims a pinned version's files breaks the in-flight read on object-store backends.

**`lance-table-creation`** {#lance-table-creation} - Every table MUST be created with:

a. **stable row ids** {#lance-table-creation-stable-row-ids} - so secondary indexes survive compaction without being rewritten to follow moved rows; without them, every compaction pass rewrites every index.

b. **unenforced primary key** {#lance-table-creation-unenforced-pk} on the primary-key columns - so merge-insert defaults to the right key with no per-call wiring, and so the forward-compat seams below have something to attach to.

c. **`session_id` leading the primary key** {#lance-table-creation-session-scoped-pk} on every table below `sessions` - a source's message and part ids are unique only within their originating session; lineage operations (spawn, `/compact`, resume, fork) copy a parent's history into a new session and replay its ids unchanged. A key omitting `session_id` collides on every replayed id; the unenforced PK means the substrate will not catch the collision. Leading with `session_id` also satisfies `lance-forward-compat-shardable` for these tables.

All of a consumer's datasets share one Lance cache and one object-store client. Why: one pool, rather than one per table, avoids multiplying connections and credential refreshes on object-store backends.

### 3.5 Concurrency

pond processes are stateless workers. Several may write the same namespace at once; Lance optimistic concurrency control resolves append conflicts through manifest versioning. There is no external coordinator - object stores provide atomic conditional writes, the local filesystem uses Lance's commit lock - and no in-process write queue.

**`lance-retry-jitter`** {#lance-retry-jitter} - Every call into Lance MUST be wrapped in bounded retry with exponential backoff and jitter. Why: transient object-store faults and lost concurrency races are expected, not exceptional; retry turns them into latency rather than errors.

**`lance-handle-freshness`** {#lance-handle-freshness} - A cached dataset handle MUST be freshness-checked before serving a read, and refreshed if older than the staleness window. The window is keyed to the backend: zero for a local filesystem, where a manifest re-read costs microseconds; a few seconds for an object store, capping manifest-fetch overhead. Why: a long-lived server owns the window between an external commit and a reader seeing it - making the window explicit and backend-keyed keeps it bounded.

### 3.6 The conflict contract

When retry is exhausted on a write, the substrate raises a typed conflict signal carrying the attempt count. The wire layer (Section 7) maps it to the retryable `conflict` error code. The dependency is one-way: the wire layer knows the substrate's conflict signal; the substrate knows nothing of the wire error model.

### 3.7 Index lifecycle

**`lance-index-maintenance`** {#lance-index-maintenance} - Writes commit data without folding indexes; index maintenance is operator-triggered via the update-indexes stage of `pond sync` (Section 7.8). A trailing index is not a correctness problem: Lance reads merge index results with a flat scan over unindexed fragments, so a query before maintenance returns complete results, just slower. The fold strategy is determined by index family, not by the write that preceded it:

| Family | Fold on `pond sync --only update-indexes` | Why |
|---|---|---|
| BTree (scalar) | `create_index(replace=true)` | Lance v7.0.0-beta.16's `optimize_indices` BTree path tripped `RowAddrTreeMap::from_sorted_iter` on column-update commits; rebuild from scratch avoids the bug. Switch to `optimize_indices(append)` once upstream is fixed. |
| Bitmap (scalar) | `optimize_indices(append)` | Incremental fold is safe. |
| Inverted (FTS) | `optimize_indices(append)` | Incremental fold is safe. |
| IVF_PQ (vector) | `optimize_indices(append)` | Stable-row-id IVF_PQ supports incremental fold via `IvfIndexBuilder::new_incremental`; centroids and PQ codebook carry forward. |

Index maintenance skips indexes whose columns no write has touched; this is sound because Lance prunes index coverage only for indexes whose fields overlap a write's modified fields. Why operator-triggered: matching Lance's own design (`table.optimize()` is the canonical periodic-maintenance op) avoids per-write index rebuilds that would otherwise dominate write cost.

### 3.8 Forward-compatibility seams

**`lance-forward-compat`** {#lance-forward-compat} - Three sub-rules cost almost nothing in v1 and keep horizontal-scale work (Section 9) a substrate swap rather than a rewrite:

a. **shardable** {#lance-forward-compat-shardable} - On a high-volume table, the first primary-key column MUST be an attribute coarse enough to shard on. A sharded writer attaches a shard spec to an existing column; if no first-position column is shardable, enabling sharding later means a primary-key redesign and a migration of every existing row.

b. **no-subsecond-freshness** {#lance-forward-compat-no-subsecond-freshness} - No operation MAY promise that a write is visible to a read within milliseconds; the floor is the `lance-handle-freshness` window. An in-memory write-ahead layer makes a write durable at once but visible to the base table only after an asynchronous merge - had pond contracted sub-second read-after-write, adding that layer would break the contract.

c. **no-cross-shard-atomic-write** {#lance-forward-compat-no-cross-shard-atomic-write} - No write batch MAY span more than one primary-key family atomically; each batch is keyed on a single family. A sharded writer assigns each PK family to one shard, so cross-shard atomicity is structurally unavailable under sharding.

### 3.9 Storage addresses and credentials

Addresses are URLs; credentials are URL-scoped sets. There is no named-storage registry and no "active storage" process state - that design was considered and rejected: an address is a static infrastructure fact, not per-invocation state.

**`storage-url-grammar`** {#storage-url-grammar} - A storage destination is one URL: a local path (`/abs`, `~/`, `file://`), `s3://bucket/prefix`, `s3+https://host/bucket/prefix` / `s3+http://host:port/bucket/prefix` (S3-compatible endpoints; TLS and `allow_http` are scheme-derived, never config), `gs://bucket/prefix`, `az://account/container/prefix`, or the test-only `memory://` / `shared-memory://`. The `s3+` form carries the endpoint inside the URL so it can never desync from the bucket - the out-of-band-endpoint failure class litestream patched three times. The region is never required: real AWS buckets auto-resolve their region inside Lance, and `s3+` endpoints get a deterministic default (S3-compatible stores ignore the SigV4 region; a fixed default beats an env-dependent fallback), overridable via the creds-set field or `?region=`. Embedded userinfo MUST be rejected at parse: argv, history, and logs are one leak class. Recognized query params (`creds`, `region`, `virtual_hosted_style_request`) are stripped before the URL reaches Lance and beat the matched set's same-named fields; an unrecognized param is a hard error.

**`creds-scope-match`** {#creds-scope-match} - A credential set binds to a URL by match, never by activation: `?creds=<name>` pointer (missing set = error) > longest matching `scope` prefix > the single scope-less catch-all > none. Scopes compare canonicalized (scheme/host lowercased, default ports stripped), match only at `/` segment boundaries (`.../pond` does not match `.../pond-2`), and never across schemes; duplicate canonical scopes are a parse error, so ties cannot exist. A set matching no URL in a remote-touching invocation is named in a warning - misbinding must never be silent. Why match-not-activate: the git `[credential "url"]` / DuckDB scoped-secrets model gives multi-storage commands zero extra syntax and keeps rotation out of argv (pond's primary callers are cron and MCP, where the invocation is frozen).

**`storage-configless`** {#storage-configless} - Every command MUST work with no config file: URLs plus env vars are a complete configuration. A URL that resolves to no set is passed with no credential options, so the ambient cloud SDK chain (instance profiles, task roles, OIDC, `aws sso login`) applies. Why: the disaster-recovery posture - restore and migrate must run on a machine where the file never existed - and the zero-config container/CI path.

**`storage-env-mirror`** {#storage-env-mirror} - The env mirror is mechanical: `storage.path` <-> `POND_STORAGE_PATH`, `creds.<name>.<field>` <-> `POND_CREDS_<NAME>_<FIELD>`; env sets merge field-by-field over same-named file sets; `extra` has no env form. Set names MUST match `[a-z][a-z0-9]{0,15}` - field names contain underscores, so the name charset is what keeps the env grammar splittable. One precedence ladder everywhere: CLI flag > `POND_*` env > config file > ambient chain > built-in defaults.

**`storage-redaction`** {#storage-redaction} - Secrets MUST NOT appear in URLs or CLI flags; they travel via config file, env, `<field>_file`, or `<field>_command` only (command output cached per process, one trailing newline stripped). Introspection redacts any field whose name contains `key` / `secret` / `token` / `password` (including `extra` keys); the `_file` / `_command` variants print literally - the path or command is the safe part.

---

## 4. Canonical model {#model}

The canonical model is the interlingua: what every adapter parses into and serializes from, what the substrate stores, what search and restore operate on. It is defined here independently of how it is stored (Section 5) or transported (Section 7) - the model an adapter author or an API client codes against.

### 4.1 Shape

The model has three nested types - a Session contains Messages, a Message contains Parts. A message's embedding vector is derived storage with no canonical counterpart, not a separate type (Sections 5 and 8). It is deliberately LLM-conversation-shaped: it models the conversational layer of an agent session - roles, turns, tool calls, reasoning - below any particular harness. Harness-specific behavior (compaction, retries, step accounting, editor context) is absorbed into the `options` bag, not added as canonical fields. This LLM-conversation shape is also why a flat social-content corpus is a separate consumer rather than a coerced session (Section 9).

### 4.2 Canonical is the source of truth

The stored canonical form is authoritative - not derived from some other representation, not a cache of one. There is no second, "raw" copy of a session. A derived canonical would invite a parallel raw store and a re-derivation step, and the moment those exist the canonical form is no longer the contract. Completeness of the canonical form is instead guaranteed by `model-lossless-projection` below.

### 4.3 Conventions

- Field names and discriminator values are `snake_case`.
- `SessionID`, `MessageID`, `PartID` are branded string scalars - distinct types in the spec, plain strings on the wire. IDs are source-supplied where the source provides a stable one, generated otherwise.
- Timestamps are RFC 3339 strings on the wire, microsecond integers in storage. Canonical timestamps are source-recorded; pond's own ingest time is a separate storage column.
- `options: ProviderOptions` is an extensibility bag on every object. Namespacing: `options.<provider>.*` for provider extensions, `options.source.*` for source and harness facts, `options.pond.*` for pond-operational facts.

### 4.4 Common types

```typespec
scalar SessionID extends string;
scalar MessageID extends string;
scalar PartID extends string;

/** Arbitrary JSON value (string | number | boolean | null | array | object). */
scalar JsonValue;

/** Extensibility bag, present on every canonical object. */
alias ProviderOptions = Record<string, JsonValue | null>;
```

### 4.5 Session

```typespec
model Session {
  id: SessionID;
  parent_session_id?: SessionID;   // set when this session spawned or forked from another
  parent_message_id?: MessageID;   // the cut-point in the parent; fork-with-cut-point only
  source_agent: string;            // the source harness brand, e.g. "claude-code"
  created_at: utcDateTime;         // source-recorded; not pond's ingest time
  project: string;                 // the shared-state scope this session belongs to
  options: ProviderOptions;
}
```

Branching exists only between sessions: a session itself is a linear log of messages with no per-message parent pointers. `parent_session_id` records that a session was spawned or forked from another - a sub-agent, a fork; `parent_message_id` additionally records the cut-point in the parent, for a fork-with-cut-point. A plain spawn (a sub-agent) populates only `parent_session_id`. `parent_session_id` is a soft reference: pond does not require the parent to be present at ingest, since independent adapter runs land in any order. Because a message id is unique only within its session, `parent_message_id` identifies the cut-point only together with `parent_session_id`; it is never resolved on its own.

**`model-parent-pointer-coherence`** {#model-parent-pointer-coherence} - A `parent_message_id` MUST NOT be present without a `parent_session_id`. Why: a cut-point with no parent session to cut from is incoherent; the validator rejects such a session.

**`model-project-non-empty`** {#model-project-non-empty} - `Session.project` MUST be a non-empty value extracted from real source data. Why: project is the attribution scope every filter and grouping relies on; an adapter that cannot resolve a project drops the session rather than inventing one.

### 4.6 Message

```typespec
model BaseMessage {
  id: MessageID;
  session_id: SessionID;           // back-reference to the containing session
  timestamp: utcDateTime;          // source-recorded; canonical ordering key within the session
  options: ProviderOptions;
}

model SystemMessage extends BaseMessage { role: "system"; content: string; }
model UserMessage extends BaseMessage { role: "user"; content: Array<TextPart | FilePart>; }
model AssistantMessage extends BaseMessage {
  role: "assistant";
  content: Array<TextPart | FilePart | ReasoningPart | ToolCallPart | ToolResultPart | ToolApprovalRequestPart>;
}
model ToolMessage extends BaseMessage {
  role: "tool";
  content: Array<ToolResultPart | ToolApprovalResponsePart>;
}

@discriminator("role")
union Message { system: SystemMessage, user: UserMessage, assistant: AssistantMessage, tool: ToolMessage }
```

Four role variants with per-role content allowlists enforced at the type level - a tool-result Part inside a user message is a category error. SystemMessage content is a plain string, not Parts; it may be empty when the SystemMessage is a placement-rule-3 carrier (Section 6.5), which records absence and is not synthesis. Messages within a session form a linear append-only log ordered by `(timestamp, id)`. Turn-level metadata - model, token usage, finish reason, error - is not a canonical field; sources record it on their assistant turns and adapters route it to `options.<provider>.*`.

### 4.7 Part

```typespec
/** Whether a Part's content is conversation or harness-injected scaffolding. */
enum Provenance { conversational, injected }

model BasePart {
  id: PartID;
  session_id: SessionID;           // back-reference to the containing session
  message_id: MessageID;           // back-reference to the containing message
  provenance: Provenance;          // conversation vs harness-injected (Section 4.8)
  options: ProviderOptions;
}

model TextPart extends BasePart { type: "text"; text: string; }
model ReasoningPart extends BasePart { type: "reasoning"; text: string; }
model FilePart extends BasePart {
  type: "file";
  media_type: string;
  file_name?: string;
  data: string | bytes | url;      // base64-inline, raw bytes, or a URL / pond://blob/<sha256>
}
model ToolCallPart extends BasePart {
  type: "tool_call";
  call_id: string;                 // matches the corresponding ToolResultPart
  name: string;
  params: JsonValue;
  provider_executed: boolean;
}
model ToolResultPart extends BasePart {
  type: "tool_result";
  call_id: string;                 // matches the originating ToolCallPart
  name: string;
  is_failure: boolean;
  result: JsonValue;
}
model ToolApprovalRequestPart extends BasePart {
  type: "tool_approval_request";
  approval_id: string;
  tool_call_id: string;
}
model ToolApprovalResponsePart extends BasePart {
  type: "tool_approval_response";
  approval_id: string;             // matches the originating ToolApprovalRequestPart
  approved: boolean;
  reason?: string;
}

@discriminator("type")
union Part {
  text: TextPart, reasoning: ReasoningPart, file: FilePart,
  tool_call: ToolCallPart, tool_result: ToolResultPart,
  tool_approval_request: ToolApprovalRequestPart,
  tool_approval_response: ToolApprovalResponsePart,
}
```

`id`, `session_id`, `message_id`, and `provenance` on `BasePart` are pond-additive: the model stores Parts as addressable rows with back-references, not as array members. `provenance` records whether a Part is conversation or harness-injected scaffolding (`model-part-provenance`, Section 4.8); it is orthogonal to the Part's `type` and to the containing message's `role`. FilePart payloads use the storage layer's blob mechanism (Section 5).

### 4.8 Honesty of the model

Five rules keep the stored canonical form trustworthy and complete. They are enforced by the adapter seam (Section 6) or by core ingest, not by convention.

**`model-no-synthesis`** {#model-no-synthesis} - An adapter MUST NOT substitute a sentinel, default, or placeholder for source data it could not find. A field that may be absent is typed as an optional sealed value whose only producers are the extractor helpers of Section 6; no path constructs one from a literal in adapter code. Why: a synthesized value is indistinguishable, downstream, from a real one - it is silent corruption. Making synthesis a compile error rather than a code-review rule is the only enforcement that holds. Defaults that describe transport or absence rather than invented field values are allowed and are not synthesis - a timestamp falling back to the session anchor, a failure flag defaulting to false, a generic MIME type.

**`model-schema-honesty`** {#model-schema-honesty} - A canonical field that is not optional is a claim that every adapter can always extract it from real source data. If any supported adapter cannot guarantee that, the field MUST become optional - the adapter MUST NOT invent a value to satisfy a non-optional field. Why: optionality is the schema telling the truth about what the sources actually carry.

**`model-lossless-projection`** {#model-lossless-projection} - For every source record an adapter ingests, every field that record carried MUST be recoverable from the stored canonical form - mapped to a typed field or Part, or preserved in `options`. An adapter MUST NOT store a proper subset of a record's fields. The only permitted non-capture is a source the adapter deliberately does not ingest at all, which MUST be stated in that adapter's documented contract. A field whose value exceeds the substrate's representable size is preserved as a truncation sentinel recording its original byte count (`adapter-bounded-values`, Section 6), not silently dropped - it remains a marked, attributable truncation, which `adapter-integrity-no-silent-drops` requires and which mere omission would violate. Why: `model-no-synthesis` forbids inventing values; this forbids dropping them - together they make the stored session a complete and honest record. Section 6 gives the placement procedure that satisfies this rule.

**`model-part-provenance`** {#model-part-provenance} - Every Part MUST be classified `conversational` (its content was authored by the human user or generated by the model as part of the exchange) or `injected` (its content was produced by the runtime or harness and inserted into the transcript - environment context, memory or rules injection, system reminders, task notifications, command echoes, tool output). A Part is provenance-homogeneous: where a source record fuses authored and injected content in one span, the adapter splits it into separate Parts (Section 6.5). Why: harness-injected content occupies a conversational slot and is byte-identical in shape to a real turn, yet it is not the conversation - search MUST exclude it (Section 8) while restore MUST preserve it. `role` records the conversational slot, not the author; without this marker the distinction is unrecoverable. It cannot be decided once for all harnesses - only the adapter for a given source knows its injection patterns, so the classification is a per-adapter obligation the seam compels (`adapter-provenance-required`, Section 6). The enum is two variants in v1 and extends additively as a consumer needs finer kinds; the specific injected kind meanwhile survives in `options`.

**`model-pond-options`** {#model-pond-options} - `options.pond` is the pond-owned namespace: adapters and wire clients MUST NOT populate it, and core ingest strips and restamps it unconditionally on every incoming Message, so it cannot be spoofed through any ingest surface. It carries the ingest host provenance stamp, `{"ingest": {"host": {"username", "hostname", "device_name"}}}` - the host of the process that inserted the row, resolved once per process. This is an audit fact, not identity, tenancy, or authorization. Fields whose lookup fails are omitted, never synthesized (the stamp itself is omitted when nothing resolves - the `model-no-synthesis` posture applied to pond's own metadata). Only Message rows are stamped: a Part travels in the same ingest substream as its Message, and a Session row would always duplicate its first message's stamp while inviting an identity reading - session attribution is a derived query over message stamps, and a session legitimately accumulates messages stamped by multiple hosts (live-write, reconnects, repair). Rows are stamped at insert only - matched rows are merge_insert no-ops, so re-ingest never restamps and backfill, if ever wanted, is an explicit command, not a sync side effect.

---

## 5. Session datasets {#datasets}

This section is how the canonical model of Section 4 persists on the substrate of Section 3. It is the sessions consumer's storage schema - the first consumer's tables and indexes. A future consumer registers its own tables the same way.

### 5.1 Three datasets

The sessions consumer registers three Lance tables: `sessions`, `messages`, and `parts`. Each is a direct serialization of its canonical type - no projections, no promotions - except that `messages` additionally carries a message's derived embedding (5.5).

`sessions` - one row per Session:

| Column | Notes |
|---|---|
| `id` | primary key |
| `parent_session_id`, `parent_message_id` | nullable fork pointers |
| `source_agent` | scalar-indexed, low cardinality |
| `created_at` | source-recorded |
| `project` | scalar-indexed; equality and prefix |
| `options` | JSON (Lance `pa.json_()`, stored as JSONB) |

`messages` - one row per Message:

| Column | Notes |
|---|---|
| `session_id`, `id` | composite primary key; clustered on `(session_id, timestamp)` |
| `timestamp` | scalar-indexed; canonical ordering key |
| `role` | scalar-indexed |
| `source_agent`, `project` | denormalized; filter-pushdown surface |
| `content` | non-null only for system messages |
| `search_text` | the indexed retrieval text (Section 8); full-text indexed |
| `vector` | Float16 embedding of `search_text` (5.5, Section 8); nullable - null until embedded |
| `embedding_model` | the model that produced `vector`; nullable - set with `vector` |
| `options` | JSON (Lance `pa.json_()`, stored as JSONB) |

`parts` - one row per Part:

| Column | Notes |
|---|---|
| `session_id`, `message_id`, `id` | composite primary key; clustered on `(session_id, message_id)` |
| `ordinal` | position within the message's content |
| `type` | the Part discriminator; scalar-indexed |
| `variant_data` | JSON (Lance `pa.json_()`, stored as JSONB); the variant-specific fields |
| `data` | Lance blob; FilePart payload only |
| `options` | JSON (Lance `pa.json_()`, stored as JSONB) |

### 5.2 Composite keys

`messages` and `parts` use composite primary keys that lead with `session_id` (`lance-table-creation-session-scoped-pk`). A source's own message and part ids are preserved verbatim without requiring global uniqueness: such an id is unique only within its session, and lineage operations - sub-agent spawn, `/compact`, resume, and fork - copy a parent session's history into a new session and replay its message and part ids unchanged. The leading `session_id` keeps each session's copy distinct; a key omitting it collides on every replayed id. Clustering on `(session_id, ...)` keeps a session's messages and parts contiguous on disk for sequential reads.

### 5.3 Denormalization

`messages` carries `source_agent` and `project` copied from its `sessions` parent. A denormalized column is populated by pond core at ingest, is immutable thereafter, and exists solely as a filter-pushdown surface; `sessions` remains authoritative for any read outside search. Why denormalize: a vector or full-text query filters and ranks in one pass over `messages`, and Lance has no relational join planner in pond's crate set - the filter columns must be on the table being searched.

### 5.4 Durability

**`session-durable-copy`** {#session-durable-copy} - Once a session is stored, it MUST survive the loss of its source - source rotation, deletion, or expiry. pond is the canonical record after ingest; re-ingest is not a recovery path, because a source that has since rotated or been deleted can no longer supply the rows. Why: being the durable record is the value of pond; a design that silently depended on the source still being reachable would not be one. Recovery runs through `pond export` snapshots taken ahead of risky operations; the manifest-retention window (Section 3.4) is short and not a recovery floor on its own.

### 5.5 Embeddings are derived

A message's embedding has no canonical-type counterpart - it is produced by pond, not supplied by a source. It is two nullable columns on `messages`: `vector`, the embedding, and `embedding_model`, the model that produced it. Both stay null until the embed stage of `pond sync` fills them (Section 8); a message ingested with embedding disabled simply keeps them null.

**`session-embed-from-canonical`** {#session-embed-from-canonical} - A message's embedding MUST be derived from its stored `search_text`, never from the source record. Why: `search_text` is durable (`session-durable-copy`) and the source is not - deriving from canonical is what lets pond re-embed under a new or changed model at any later time with no source present, making a model change a re-derivation, not a migration.

Re-embedding rewrites only `vector` and `embedding_model`; no canonical column is touched, and each rewrite lands as a new manifest version, not a row mutation (`lance-append-only`). A model swap is a single conditional `merge_update` keyed on `target.embedding_model != source.embedding_model`: stale rows update, up-to-date rows are left alone, and the IVF_PQ is dropped before new vectors arrive (centroids belong to one distance space; the next update-indexes stage rebuilds it). A same-dimension swap rewrites `vector` in place; a different-dimension swap adds a new column, backfills from `search_text`, drops the old, and renames - all on `messages`, never a new table. Lance's manifest history retains prior vectors, so a regressed swap rolls back without a re-ingest. Section 8 covers how embeddings are produced and queried.

---

## 6. Adapters {#adapters}

An adapter is the codec between one client format and the canonical model. This section specifies the codec contract - both directions - and the seam that makes ingest's correctness rules compile-enforced rather than convention.

### 6.1 Bidirectional codec

Every adapter is a codec with two faces:

- *parse* - client format to canonical. This face is configured against a source (a directory, an HTTP endpoint) and streams canonical events.
- *serialize* - canonical to client format. This face is a pure function of a canonical session; it holds no source.

The two faces have genuinely different shapes - one source-configured and streaming, the other source-free - so an adapter is not a single object carrying both: the read face and the write face are separate.

### 6.2 Restore is hub-and-spoke

Serializing is restore. Any adapter can restore any stored session, because every session is in canonical form and the serialize face needs only canonical. A session need not return to the client that produced it.

**`adapter-lineage-complete-restore`** {#adapter-lineage-complete-restore} - Restoring a session MUST also restore its child sessions: the sessions that name it in `parent_session_id`. Why: a restored artifact must stand on its own in the target client - a Claude Code session that called the Task tool, restored without its subagent transcripts, is a set of dangling references rather than a working session. `parent_session_id` records a spawn or a fork (Section 4). The spawn graph is one level deep, capped structurally by the agent model - a Claude Code subagent cannot spawn subagents, and Managed Agents enforces a delegation depth of one. Multi-level fork lineage is deferred (Section 9); no v1 source emits it, so every stored graph is depth-one today. A graph found nesting deeper - a relaxed spawn cap, or fork lineage - MUST surface as a typed error, never a silent partial restore (`adapter-integrity-no-silent-drops`).

### 6.3 Origin and restore fidelity

Each session records the brand of the source that produced it (`Session.source_agent`), and each adapter has a matching origin identity. Restore fidelity is decided by the system, by comparing the two - never chosen by the adapter:

**`adapter-native-restore-lossless`** {#adapter-native-restore-lossless} - Restoring a session with the adapter whose origin matches the session's origin is *native* restore and MUST be lossless (value-complete, per Section 1). Restoring with any other adapter is *foreign* restore: best-effort - a valid, idiomatic session in the target's own feature set, dropping whatever the target cannot express (the dropped content remains in canonical). A value truncated under `adapter-bounded-values` (Section 6) restores as its truncation sentinel, not its original bytes: a value the substrate physically cannot represent cannot round-trip, and the sentinel records the loss explicitly rather than hiding it. Why the system decides and not the adapter: native losslessness is a contract a caller relies on; leaving "am I native?" to the adapter would make it a convention.

### 6.4 The no-synthesis seam

The parse face builds canonical values only through a small set of extractor helpers that read one record of source data. The type holding a possibly-missing extracted value has no constructor reachable from adapter code - the helpers are its only producers - so an adapter physically cannot place a literal, a default, or a sentinel into a canonical field. This is what makes `model-no-synthesis` and `model-schema-honesty` (Section 4) compile errors rather than review rules. The serialize face needs no such seam: canonical is already trusted input.

**`adapter-provenance-required`** {#adapter-provenance-required} - The Part constructor reachable from adapter code MUST require a `provenance` value (`model-part-provenance`, Section 4.8); a Part with an unclassified or implicitly-defaulted provenance MUST NOT compile. Why: provenance cannot be defaulted to `conversational` without silently mislabelling harness machinery as conversation, and it cannot be added after the fact because only the parse of a specific source record carries the signal. Like `model-no-synthesis`, making the classification a structural obligation rather than a review rule is the only enforcement that holds - and it forces every future adapter to confront its own harness's injection patterns rather than inheriting a guess.

**`adapter-transport-agnostic-seam`** {#adapter-transport-agnostic-seam} - The parse seam abstracts one record of source data behind a small set of value accessors and carries no assumption about where that record came from. Why: the same seam serves a file adapter today and an HTTP or stream adapter later, with no change to the seam.

**`adapter-bounded-values`** {#adapter-bounded-values} - Every value an adapter places into a text column passes through the seam's size bound: a value whose encoding exceeds the substrate's per-value limit is truncated in place to a marked sentinel recording the original byte count, with the rest of the record preserved intact. The bound is a property of the seam's extractor helpers - an adapter cannot emit an unbounded value any more than it can emit a synthesized one. Binary payloads stored as blobs are exempt; the limit is a property of the text-column representation, not of the data. Why: the storage substrate cannot represent a text value at or beyond a hard size, so an unbounded value is not a large row but a process abort - bounding at the seam turns it into an attributable, recoverable truncation.

### 6.5 Placement procedure

To satisfy `model-lossless-projection` (Section 4), an adapter places every field of every record it ingests by one of three rules:

1. Message content becomes typed Parts, each classified `conversational` or `injected` (`model-part-provenance`, Section 4.8). Where a source record fuses authored and harness-injected content in one span - a human turn wrapped in runtime context tags, an attachment-scaffolding prefix on a typed prompt - the adapter splits it at the exact byte boundary into separate, provenance-homogeneous Parts. The split is value-complete-lossless (Section 1.3): native restore reconcatenates the Parts in `ordinal` order.
2. Harness or runtime metadata goes into `options` - on the Message or Part the record maps to. This includes any field of a mapped record left over once its typed fields are taken.
3. A record that maps to no Message at all - a standalone log entry that is neither a conversational turn nor metadata on one - is carried whole: a system-role Message with empty `content` and the record's whole-record encoding in its `options`, kept in log order by the record's own timestamp. Its id follows `lance-deterministic-pk` (Section 3) and its timestamp the record's own value, or the session-anchor fallback that `model-no-synthesis` (Section 4) permits.

The third rule is the catch-all that makes losslessness reachable for any record - including record types that did not exist when the adapter was written.

### 6.6 Ingest order and integrity

**`adapter-integrity`** {#adapter-integrity} - The parse face's contract on output:

a. **event-ordering** {#adapter-integrity-event-ordering} - For each session: the Session first, then each Message immediately followed by its Parts in order, before the next Message. pond core computes a message's indexed text at the message boundary without buffering across messages - the transition off a Part stream is the signal the message is complete.

b. **no-silent-drops** {#adapter-integrity-no-silent-drops} - Malformed source input MUST surface as a typed error carrying the adapter and the location of the fault; never silently skipped. A silent drop is invisible data loss, a surfaced one is a fixable report.

c. **opaque-ids** {#adapter-integrity-opaque-ids} - Identifiers on canonical objects are opaque strings. An adapter decodes any structure a source encodes into a path or name once, at ingest, and stores the decoded value; readers never re-parse.

d. **additive-sync** {#adapter-integrity-additive-sync} - A write MUST NOT overwrite a row already present under its primary key - matched rows are no-ops. Adapter output is monotone across versions: a newer adapter produces a superset of the rows a prior version produced. The source is not authoritative against pond's stored copy - a re-parse from a since-corrupted source must not be able to overwrite good data. Changing or removing an already-stored row is a deliberate migration, never a side effect of re-ingest.

e. **dedup** {#adapter-integrity-dedup} - An adapter SHOULD detect duplicate primary keys in its own output using the source format's own mechanism; the write path drops duplicates as a floor regardless. Catching them in the adapter keeps the count visible in the ingest summary, while the write-path floor keeps storage correct when an adapter misses one.

### 6.7 The registry

Adapters are listed in one registry; adding an adapter is a new file plus one line in that list - there is no central enum or dispatch to edit, and no code generation. Why: a low, fixed cost per adapter is what keeps the source list open-ended.

### 6.8 Conformance

Each adapter has a round-trip codec test: parse a committed fixture to canonical, serialize it back native, and assert the result is value-equal to the fixture - this is what enforces `adapter-native-restore-lossless` and exercises `model-lossless-projection`. Foreign serialization is tested for validity in the target format and reviewed against a golden file.

### 6.9 Adapter set

The adapter set is intentionally not listed here. The registry in `src/adapter/mod.rs` is the source of truth for which formats a build supports, and Section 6 is the contract every registered adapter must satisfy. Per-source extraction detail - how an adapter resolves `project`, what its `source_agent` brand is, its on-disk layout, and which source records it deliberately does not ingest - lives in that adapter's own code, which is its documentation.

---

## 7. Protocol {#protocol}

The protocol is how requests reach pond and responses leave it, across both transports. HTTP and MCP are thin dispatchers over one shared set of handlers; the handlers know nothing of either transport.

### 7.1 Transport-agnostic handlers

Every operation is a handler function from a request value to a response value. The HTTP transport (axum) and the MCP transport (rmcp) each only decode their wire form into that request value and encode the response back - no operation logic lives in a transport, and a handler cannot tell which transport invoked it.

### 7.2 The request envelope

Every request carries `protocol_version` (a positive integer; v1 is `1`) and an optional `namespace`. Every request is associated with a server-generated request id for log correlation. HTTP exposes it via the `X-Pond-Request-Id` response header; MCP correlation uses the JSON-RPC envelope `id`. Schema evolution within a major version is additive only; removing or retyping a field is a major version bump. The precise wire schema is published as JSON Schema generated from the Rust types: this document specifies the contract, the generated schema is the exact artifact.

### 7.3 Namespace

`namespace` is an opaque tenant-routing string; omitted, it selects the personal pond's single namespace. It is distinct from the Lance namespace concept of Section 3 - the same word at two layers: the wire `namespace` selects a tenant, the Lance namespace is how the catalog seam locates that tenant's tables.

**`wire-namespace-resolution`** {#wire-namespace-resolution} - Whether a request's namespace is acceptable, and which stored tables it maps to, MUST be decided in exactly one place. Why: hosted multi-tenancy turns one namespace into many; centralizing the decision makes that a single change, not an edit at every call site.

### 7.4 The error model

Success and error are mutually exclusive at the body level. An error body is one shape:

```json
{ "error": { "code": "validation_failed", "message": "...", "details": {} } }
```

The code set is closed:

| Code | When | HTTP | Retryable |
|---|---|---|---|
| `validation_failed` | bad request shape, missing field, type mismatch, batch over a cap | 400 | no |
| `version_unsupported` | a `protocol_version` pond does not understand | 400 | no |
| `not_found` | a `pond_get` target that does not exist | 404 | no |
| `namespace_unknown` | a namespace string not provisioned | 403 | no |
| `storage_unavailable` | a Lance or object-store failure after retry was exhausted | 503 | yes |
| `conflict` | optimistic-concurrency retry exhausted on a write | 409 | yes |
| `internal` | an unhandled fault | 500 | maybe once |

Retryability is conveyed by the code; there is no separate field. `conflict` is the wire mapping of the substrate's conflict signal (Section 3).

### 7.5 Operations

1. **`pond_search`** (`POST /v1/search`) - hybrid search; Section 8 specifies retrieval. Returns ranked message hits grouped by session, with the top-scoring matches per session. Takes a `format`: `text` (the default - a rendered transcript of the ranked hits) or `json` (the same hits as structured data).
2. **`pond_get`** (`POST /v1/get`) - fetch a whole session, or one message with surrounding context. Session mode takes a `response_mode`: `conversational` (the default - human/model text, with a compact `parts_summary` per message), `complete` (all messages including system/tool carriers, with `parts_summary`), or `verbatim` (all messages with full Parts inline). Session mode also takes `session_from`: `start` (the default - oldest messages first) or `end` (the most recent messages, still in chronological order - e.g. recovering recent context after compaction). Message mode returns the target's full Parts (paginated) plus `context_depth` sibling messages each side; the siblings follow `response_mode` - conversational by default, so system/tool carriers do not crowd the conversation out of the window - while the target itself returns regardless of role. Pages are bounded by a size budget and never cut mid-message; when `messages_remaining` (session) or `target_parts_remaining` (message) is non-zero, the caller pages on by passing the last returned id as `after_id`. Not for bulk export - that is the restore/export path.
3. **`pond_ingest`** (`POST /v1/ingest`) - accept a batch of canonical events. Always batched, bounded by an event count and a body-size cap. Events are grouped by session and applied per session; partial success across sessions is normal and reported per row.

Two resources, `schema://pond` and `stats://pond`, expose the search-field documentation and dataset statistics.

### 7.6 Ingest events

A `pond_ingest` event is one canonical object - a Session, a Message, or a Part - tagged with its kind. Within a session's substream the order is fixed (`adapter-integrity-event-ordering`, Section 6). `Session.source_agent` and `Session.project` are immutable after first write: a re-submitted session with a differing value for either is rejected for that row, since both are denormalized onto `messages`.

### 7.7 MCP surface

The MCP transport exposes the read operations - `pond_search`, `pond_get`, and `pond_sql_query` - as tools, plus the resources. Ingest stays HTTP-and-CLI only. Why: MCP's role is read access for an agent; ingest is an operator action.

### 7.8 CLI verbs

The same handlers back a set of command-line verbs:

- `pond init` - idempotent setup-and-repair wizard: storage destination (with an end-to-end probe for remote URLs), source adapter selection, MCP registration, and an opt-in sync schedule, written to `config.toml` in a single pass at the end. Every section is flag-answerable (`--storage-path`, `--adapters`, `--schedule`, `--skip-mcp`, `--yes`) for non-interactive use; `--yes` alone never schedules.
- `pond sync` - run the import, embed, and update-indexes stages in order. `--only <stage>` runs exactly one stage; `--skip <stage>` omits one. Stages are `import`, `embed`, and `update-indexes`. `--force-embed` re-embeds rows whose `embedding_model` differs from the current model via conditional merge.
- `pond search [--explain]` - hybrid search from the command line. `--explain` returns Lance's `analyze_plan` output instead of results.
- `pond get` - fetch a session, or one message with context, from the command line.
- `pond status` - row counts, dataset statistics, embedding coverage, index health, and the sync-schedule state. It prints the cheap storage summary first, then completes the longer checks.
- `pond serve --transport http|stdio` - run HTTP by default, or MCP over stdio.
- `pond mcp` - alias for `pond serve --transport stdio`.
- `pond schedule start|stop|status|logs` - manage the automatic `pond sync -q` registration with the OS scheduler: launchd on macOS, systemd user timers (or a fenced crontab block) on Linux. The scheduled job never passes `--yes`, so an unattended run cannot auto-enable adapters.
- `pond export` - write a compact `.pond` archive containing clean index-free Lance datasets plus a manifest. Embeddings remain data columns and are preserved.
- `pond import <archive.pond>` - restore a compact archive into the current corpus. Existing rows are deduped through the same merge-insert path as adapter import. (`pond import` is the only archive-restore path; the former `pond sync --import-from` alias is removed.)
- `pond config show|path|schema` - introspection for Section 3.9: `show` prints the resolved config with redacted values (`storage-redaction`), per-field source attribution along the precedence ladder, and the active URL's creds binding.
- `pond storage [check|use|migrate]` - the storage cluster. Bare `pond storage` shows the resolved destination, creds binding, and per-table sizes. `check <URL>` probes a destination end-to-end (parse, creds resolution, the conditional-put OCC primitive, write/read/delete) with a distinct exit code per failure class (formerly `pond config check`). `use <URL>` is the validate-then-activate switch: probe, optional copy of existing data, row-count verification, and only then the `[storage].path` flip. `migrate --from <URL> --to <URL>` copies canonical data between storages over the export+import path - idempotent union merge (`lance-deterministic-pk` + merge-insert): re-runnable, valid onto a populated destination, never deletes the source (formerly top-level `pond migrate`).
- `pond completions <shell>` - emit shell completion scripts; release packages ship them pre-installed.

### 7.9 Versioning

The wire protocol versions through `protocol_version` and additive-only schema changes. The canonical model and the storage schema evolve additively too; the Lance manifest carries the storage schema version (`lance-dataset-schema-version`, Section 3). pond is pre-release: there are no compatibility shims, and a breaking change is a major version bump, not a migration layer.

---

## 8. Search and embeddings {#search}

Search returns messages. It is hybrid - a vector retriever and a keyword retriever, fused - and runs at message granularity. This section also specifies the embedding seam, a generic capability the session datasets consume rather than a part of them.

- **Hybrid retrieval.** A search runs two retrievers over the same corpus: a BM25 full-text retriever over each message's indexed text, and a vector retriever over the message embeddings produced by the configured model. Each arm scores by raw magnitude - BM25 for full-text, cosine similarity for the vector arm - never by rank position, so a hit's score does not shift with pool size or the caller's limit. The arms are fused by per-arm score normalization: each arm's surviving raw scores (post intra-arm dedup by fusion key) are min-max normalized to [0, 1], then summed across arms with fixed per-arm weights identified by the search-quality harness in `ops/search-benchmarks/`. The fusion key is the `session_root` by default - cross-arm agreement is credited at the conversation level - and the individual message when the caller pinned one conversation via the session filter, where root keying would collapse the whole response to a single hit. A fused group's representative message is the one with the highest single weighted arm contribution, so the displayed snippet reflects why the group ranked where it did. Both retrievers operate at message granularity and agree on row identity, so fusion needs no per-chunk deduplication. When no message is embedded under the configured model the search runs full-text only; the mode is decided by the server from embedding availability, not requested on the wire. Retriever attribution is operator-only and exposed via `pond search --explain`.

  **`search-prefilter-pushdown`** {#search-prefilter-pushdown} - Every vector and full-text query MUST push its scalar filters into the table's scalar indexes before the retriever ranks, never as an in-memory post-filter. Why: a post-filter ranks first and filters second, so it silently returns fewer than the requested number of results and ignores the scalar indexes entirely - correctness depends on the filter running first.

- **Indexed text.** `search_text` is the conversation: one text field per message, built at ingest by one pond-core function applied uniformly to every message - per-source customization is rejected so the search corpus has one predictable shape. It concatenates, in order, the text of TextParts and the metadata of FileParts that carry `provenance: conversational` (Section 4.7). It is null for system and tool messages, and for any message left with no conversational text - a bare tool call, or a message whose only content is harness-injected. Reasoning text, tool-call bodies, tool results, approval parts, and harness-injected parts are deliberately not indexed; they live in `parts` and reach the caller only via `pond_get` (verbatim session mode, or message mode for a single message). Excluding injected parts is not per-source customization: the conversational-or-injected decision is made once at the adapter seam (`model-part-provenance`, Section 6) and recorded as `Part.provenance`, so this function reads a canonical field and stays uniform.

  **`search-language-neutral-index`** {#search-language-neutral-index} - The full-text index MUST tokenize every language alike; it MUST NOT apply a transform keyed to a single language. Why: pond ingests sessions in any language; a monolingual transform silently under-indexes every other. Pond indexes with a character-`ngram` tokenizer (3-5 range; rationale and experiment: `docs/researches/tokenizer-experiment-report.md`).

- **Filters and ranking.** A search accepts filters on project, session, source agent, and a time range, plus a minimum score. Results are grouped to one summary per session, with up to a small fixed number of top-scoring matches per session (the cap lives in code); a session-scoped search returns one session, so the cap widens to the requested limit there. Filter columns are denormalized onto the searched tables (Section 5) so every filter pushes down without a cross-table join. By default a search excludes subagent (child) sessions - those whose `source_agent` carries a `/`-delimited subpath - because a search wants the human-facing main sessions; an `include_subagents` flag opts back in, and an explicit session or source-agent filter disables the default exclusion (the caller is already scoping deliberately).

  **`search-absence-honesty`** {#search-absence-honesty} - Every search response MUST report how many searchable messages the caller's filters left in scope, including (especially) when that count is zero. Why: retrieval always fills its top-k from whatever scope it gets, so without the scope size the caller cannot distinguish "nothing relevant exists" from "my filters excluded everything" - measured agent behavior converts the first reading into false "this was never discussed" conclusions. Scores carry no absence signal (present and absent content score in overlapping bands; see `docs/researches/embeddings.md`), so the scope count is the one honest cue the response can give.
- **Hit payload.** A search hit carries enough of the matched message to judge relevance without a second fetch: the message's indexed text in full when it is small, and when it is large a bounded prefix of that text plus a match-windowed snippet drawn around the query terms. The size bounds are tuning constants and live in the code, not this document. A user-role hit additionally carries a compact `parts_summary` (the same per-Part descriptor `pond_get` returns), so a prompt that attached files is distinguishable from a plain-text one without a second fetch; other roles omit it. The full message - including the parts excluded from the indexed text - remains available through `pond_get`.
- **The embedding seam.** Turning text into vectors is a generic capability, not a session concept. It sits behind one seam - a backend interface that takes text and returns vectors - so a local model today and a remote provider later are the same shape to everything above. The engine ships a fixed set of models it has loaders for; configuration selects one and supplies its vector parameters. No model is mandatory and none is named in this document - the choice and its default are configuration.
- **Producing embeddings.** Embeddings are derived, not source data: they are produced after ingest, never during it. The embed stage of `pond sync` walks the backlog of un-embedded messages - those whose `vector` is null, or whose `embedding_model` is not the configured model - and fills `vector` and `embedding_model` through the embedding seam, one vector per message. The seam is generic: a future consumer that wants vectors reuses it over its own table.
- **Opt-in.** Embedding is opt-in by configuration. With it off, `pond serve`, `pond mcp`, and `pond search` run full-text only and never load a model. With it on and at least one message embedded under the configured model, search is hybrid.
- **Index lifecycle.** Vector and full-text columns exist from table creation; turning embeddings on or off never needs a schema migration. Index maintenance follows `lance-index-maintenance` (Section 3.7). Vector search uses brute-force flat scan below the activation threshold (currently 100,000 non-null vectors); above it the trained IVF_PQ takes over. Partitions = `num_rows // 4096`; `[search].nprobes` and `[search].refine_factor` are operator-tunable for recall.

---

## 9. Deferred {#deferred}

These are scoped out of v1. None requires a schema migration or a cross-cutting change when it activates - the v1 design forecloses none of them.

1. **Future consumers.** The storage substrate (Section 3) is generic; the session datasets are its first consumer, and these are the next. Each is a separate consumer with its own canonical model and its own tables on the same substrate - not an extension of sessions.
   - **Resources and blobs** - per-namespace knowledge-base files.
   - **Social and web content archives** - exports from Telegram, Discord, Twitter, Reddit, GitHub. These are flat-message content, not LLM conversations, so they get their own canonical model rather than being coerced into Session/Message/Part.
   - **A file and blob store shaped like the Files API** - upload, reference, download.
   - **A versioned-document store shaped like agent memory stores** - small text documents with an immutable version history. The substrate's manifest versioning aligns naturally with this.
2. **Future source adapters.** A new adapter adds no substrate or schema change - it is a new file and a registry line (Section 6).
   - **A Managed Agents adapter**, including multi-agent sessions: a coordinator and its delegated agent threads map onto linked Sessions through `parent_session_id` and `parent_message_id`. The spawn case already works for v1 sources (Claude Code subagents ingest as linked child Sessions); the Managed Agents adapter is the next step.
   - **Other clients** - additional agentic clients such as Cursor, aider, Gemini CLI, and more.
3. **Provider-target restore.** Restoring a canonical session into a provider API request shape (Anthropic, OpenAI, Bedrock, Gemini), as opposed to a harness session-log format. Always foreign, and additionally constrained to produce API-valid output.
4. **Live-write.** Ingesting events as a session runs, rather than after it ends. The substrate work this needs - a per-shard write-ahead layer, a scanner that merges in-memory and on-disk generations, a sharded writer - is real implementation work; the forward-compatibility seams of Section 3 are what keep their activation a substrate swap rather than a rewrite.
5. **Hosted multi-tenant.** Mapping each tenant to a child Lance namespace, and swapping the directory catalog for a hosted one. The catalog seam and the single namespace-resolution point (Sections 3 and 7) are the seams this rides.
6. **Other.**
   1. Remote embedding providers - pluggable embedding backends behind the existing seam.
   2. Cross-session attachment deduplication - dedup identical FilePart payloads.
   3. Indexing file-attachment contents.
   4. Typed image arrays for image-typed FileParts - `EncodedImage` / `FixedShapeImageTensor` for `image/*` media types.
   5. JSON-path scalar indices on `options` - index hot paths via pond's index policy and add JSON-path filter predicates.
   6. Graph-traversal layer over fork lineage - queryable traversal over `parent_session_id` / `parent_message_id`.
   7. Wire-surfaced time-travel queries - expose Lance's version pinning on the wire.
   8. OTel-compatible projection of the canonical model.
   9. `pond tag` verb - create / list / delete Lance tags as the named-snapshot recovery floor.
   10. BFloat16 embedding storage - swap Float16 for BFloat16 once Lance's IVF_PQ build path accepts the `lance.bfloat16` extension (today it rejects `FixedSizeBinary(2)` at `infer_vector_element_type`). Same 2 bytes per element; wider dynamic range, lower precision than Float16.
   11. Blob v2 part storage - swap legacy `LargeBinary + lance-encoding:blob=true` for the `lance.blob.v2` Struct extension (and bump `data_storage_version` V2.1 -> V2.2) once Lance's compact path dispatches the BlobLayout fast-path for blob.v2 fields. At v7.0.0-beta.16, `compact_files` reads blob.v2 through the generic struct branch (`rust/lance-encoding/src/decoder.rs:758-791`) and errors with "there were more fields in the schema than provided column indices / infos"; legacy blob writes `BlobLayout` pages which compact handles correctly. Same one-payload-per-row model; blob v2 adds the sidecar-file optimization for large external blobs and a separate `uri` Arrow sub-field (today the URL string is stored as UTF-8 bytes in the `data` column with the variant tag in `variant_data.data_kind`).
   12. Candidate-pool reranker - optional cross-encoder pass over the hybrid candidate pool before final ranking, behind the Section 8 search seam.
7. **Open questions.** Undecided: what event first activates the multi-tenant router; what use case first activates live-write; which catalog backend the hosted tier uses.

---

## 10. References {#references}

External work that informed this design. These are inspiration and corroboration; the contract is Sections 1 through 9.

- [Scaling Managed Agents](https://www.anthropic.com/engineering/managed-agents) - Anthropic Engineering. The session-as-append-only-event-log framing, and the meta-harness idea of modeling the stable conversational layer while pushing volatile harness behavior outward - the shape the canonical model and the `options` bag follow.
- [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) - Anthropic Engineering. On curating what enters an agent's context window; background for why a durable, searchable store of session history is worth building.
- [Context Rot](https://www.trychroma.com/research/context-rot) - Chroma research. On the degradation of model performance as input context grows - the same motivation, seen from the retrieval-quality side.
- [Recursive Language Models](https://arxiv.org/html/2512.24601v3) - arXiv 2512.24601. Treats long context as an external, queryable environment and recursion as sub-agent spawning; a recursive run captures as linked Sessions, which corroborated the branching model of Section 4.
