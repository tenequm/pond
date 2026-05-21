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
9. [Deferred](#deferred) - work scoped out of v1, with activation triggers.
10. [References](#references) - external work that informed this design.

---

## 1. Overview {#overview}

This section states what pond is and the single idea the rest of the document elaborates. Read it first - it is the map for everything below.

### 1.1 What pond is

pond ingests sessions from agentic clients - Claude Code, Codex, and others on the roadmap - into one canonical form, stores them in Lance, and serves hybrid search (vector and keyword, fused) over them at message granularity. It ships as a single static binary that exposes two transports - an HTTP+JSON API and an MCP server - over one shared set of handlers, and runs in two deployments: a personal pond on a laptop, or a multi-tenant pond against object storage.

### 1.2 The interchange-hub model

The canonical Session / Message / Part schema is not merely how pond stores data - it is a format-neutral interchange representation, an interlingua. Every adapter is a bidirectional codec: it parses a client's format into canonical, and serializes canonical back into a client's format. Because every session passes through one canonical form, any adapter can restore any session - a session need not return to the client that produced it. The richness and stability of this schema is pond's product; everything else is machinery around it.

### 1.3 "Lossless" means value-complete

Throughout this document, *lossless* means every value round-trips as an equal value - it does not mean the bytes are identical. Restoring a session is a rederivation from canonical, not a byte replay, so incidental encoding (whitespace, JSON key order, equivalent number forms) is not data and is not preserved. Restoring a session with the adapter that produced it is lossless in this value-complete sense; restoring it with any other adapter is best-effort, target-optimized transcoding.

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
                  search  /  get  /  session-events
```

Many client formats parse into one canonical model; any adapter can serialize that model back out, to a harness format or - deferred - to a provider API shape. Canonical persists in the storage substrate - a generic Lance engine that search, get, and session-events all read from. The session datasets are merely its first consumer; Section 9 names the rest.

### 1.7 How to read this document

1. Sections 3 through 8 are ordered foundation-first: the storage substrate (3) before the canonical model and the consumer built on it (4 through 8). The substrate is the engine every current and future consumer shares; specifying it first, with no reference to sessions, is what keeps it honestly generic.
2. The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are used as defined in RFC 2119 and RFC 8174.
3. Operational rules carry a short mnemonic identifier - for example, `append-only`. Source code references a rule by that identifier, so the identifiers are stable. Each rule states its constraint and, compactly, why it exists; the reason is part of the contract - it is there so the rule is not later "simplified" away by someone who cannot see what it defends.
4. This document specifies contracts and behavior. Implementation specifics - exact type and method names, tuning constants, file and module layout - live in the code, which is their source of truth.

---

## 2. Scope {#scope}

pond v1 is deliberately narrow: one application, two source formats, two deployments. This section fixes that boundary. Work scoped out of v1 is in Section 9; the non-goals here are different - they are stable positions, not deferrals.

### 2.1 What v1 ships

1. **One application: sessions.** Lossless ingest, storage, and hybrid search of agentic-client sessions. Sessions are the first consumer of the storage substrate (Section 3); future consumers are in Section 9.
2. **Two source formats: Claude Code and Codex.** Each is a bidirectional codec - it parses its own format into canonical and serializes canonical back, including the cross pairs (a Codex session restored as Claude Code, and the reverse).
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

A consumer - the session datasets in v1, others later (Section 9) - does not use Lance directly. It declares its tables to the substrate and then stores and queries rows through it. The substrate guarantees durable append-only storage, safe concurrent writers, retry around transient faults, and bounded read staleness. It does not interpret rows: column meaning, indexes, and denormalization belong to the consumer (Section 5 for sessions). Specifying the substrate with no reference to sessions is what keeps it reusable.

### 3.2 The three seams

Every interaction with Lance funnels through one of three paths, each a single chokepoint:

**`catalog-seam`** {#catalog-seam} - Every dataset open MUST resolve the table's location through one catalog lookup; no code constructs a dataset path directly. Why: the catalog is where a local directory layout is swapped for a hosted catalog - centralizing it makes hosted multi-tenancy a configuration change, not a cross-cutting edit.

**`read-seam`** {#read-seam} - Every scan and search query MUST be built through the substrate's read path. Why: it is the one place `prefilter-pushdown` (Section 8) is enforced, and the one place a future scanner change lands.

**`write-seam`** {#write-seam} - Every write MUST go through the substrate's merge-insert path. Why: `append-only` and `additive-sync` (Section 6) hold only with a single write chokepoint; a direct write bypasses both.

### 3.3 Data integrity

**`append-only`** {#append-only} - Stored rows MUST NOT be mutated; an update produces a new row or a new manifest version. Why: it forecloses corruption-by-mutation and makes every write idempotent under retry.

**`deterministic-pk`** {#deterministic-pk} - Every row MUST have a deterministic primary key - source-supplied where the source carries a stable id, content-derived otherwise. Writes merge-insert on the key, so a retried or re-run write is a no-op for rows already present. Why: idempotent ingest depends on the key being reproducible from the source data alone.

**`dataset-schema-version`** {#dataset-schema-version} - Schema versioning lives at the dataset level - the Lance manifest and a dataset-level metadata key - never as a per-row column. Why: a per-row version column pays storage on every row for a fact that is per-dataset.

### 3.4 Dataset parameters

Every table is created with the current stable Lance file format, constant-time latest-manifest lookup, and a manifest-retention window long enough to serve as a recovery floor (Section 5). Two creation settings are rules, because a future table must not skip them:

**`stable-row-ids`** {#stable-row-ids} - Every table MUST enable stable row ids. Why: with them, secondary indexes survive compaction without being rewritten to follow moved rows; without them, every compaction pass rewrites every index.

**`unenforced-pk`** {#unenforced-pk} - Every table MUST declare its primary-key columns as an unenforced primary key. Why: it lets merge-insert default to the right key with no per-call wiring, and it is a precondition for the forward-compatibility seams below.

All of a consumer's datasets share one Lance cache and one object-store client. Why: one pool, rather than one per table, avoids multiplying connections and credential refreshes on object-store backends.

### 3.5 Concurrency

pond processes are stateless workers. Several may write the same namespace at once; Lance optimistic concurrency control resolves append conflicts through manifest versioning. There is no external coordinator - object stores provide atomic conditional writes, the local filesystem uses Lance's commit lock - and no in-process write queue.

**`retry-jitter`** {#retry-jitter} - Every call into Lance MUST be wrapped in bounded retry with exponential backoff and jitter. Why: transient object-store faults and lost concurrency races are expected, not exceptional; retry turns them into latency rather than errors.

**`handle-freshness`** {#handle-freshness} - A cached dataset handle MUST be freshness-checked before serving a read, and refreshed if older than the staleness window. The window is keyed to the backend: zero for a local filesystem, where a manifest re-read costs microseconds; a few seconds for an object store, capping manifest-fetch overhead. Why: a long-lived server owns the window between an external commit and a reader seeing it - making the window explicit and backend-keyed keeps it bounded.

### 3.6 The conflict contract

When retry is exhausted on a write, the substrate raises a typed conflict signal carrying the attempt count. The wire layer (Section 7) maps it to the retryable `conflict` error code. The dependency is one-way: the wire layer knows the substrate's conflict signal; the substrate knows nothing of the wire error model.

### 3.7 Maintenance

A background task periodically compacts small fragments, extends each index to cover newly appended data, and removes manifest versions older than the retention window. Maintenance is concurrency-safe and converges harmlessly when several processes run it against one dataset.

### 3.8 Forward-compatibility seams

Three rules cost almost nothing in v1 and keep horizontal-scale work (Section 9) a substrate swap rather than a rewrite. v1 builds none of that machinery; these rules only ensure the v1 data shape does not foreclose it - and that reasoning is itself the contract, kept here so the rules are not later mistaken for needless ceremony and removed.

**`shardable-pk-pos1`** {#shardable-pk-pos1} - On a high-volume table, the first primary-key column MUST be an attribute coarse enough to shard on. Why: a sharded writer attaches a shard spec to an existing column; if no first-position column is shardable, enabling sharding later means a primary-key redesign and a migration of every existing row. Choosing a shardable attribute now costs nothing and removes that future migration entirely.

**`no-subsecond-freshness`** {#no-subsecond-freshness} - No operation MAY promise that a write is visible to a read within milliseconds; the floor is the `handle-freshness` window. Why: an in-memory write-ahead layer makes a write durable at once but visible to the base table only after an asynchronous merge. Had pond contracted sub-second read-after-write, adding that layer would break the contract. Not contracting it costs nothing - human-driven queries do not need sub-second freshness - and keeps the option open.

**`no-cross-shard-atomic-write`** {#no-cross-shard-atomic-write} - No write batch MAY span more than one primary-key family atomically; each batch is keyed on a single family. Why: a sharded writer assigns each PK family to one shard, so cross-shard atomicity is structurally unavailable under sharding. A v1 that wrote atomically across PK families could not be sharded later without breaking that batch. v1's write granularity already respects this, so the rule only holds the line.

---

## 4. Canonical model {#model}

The canonical model is the interlingua: what every adapter parses into and serializes from, what the substrate stores, what search and restore operate on. It is defined here independently of how it is stored (Section 5) or transported (Section 7) - the model an adapter author or an API client codes against.

### 4.1 Shape

The model has three nested types - a Session contains Messages, a Message contains Parts - plus Embedding, a derived type with no canonical counterpart (it is storage only; Sections 5 and 8). It is deliberately LLM-conversation-shaped: it models the conversational layer of an agent session - roles, turns, tool calls, reasoning - below any particular harness. Harness-specific behavior (compaction, retries, step accounting, editor context) is absorbed into the `options` bag, not added as canonical fields. This LLM-conversation shape is also why a flat social-content corpus is a separate consumer rather than a coerced session (Section 9).

### 4.2 Canonical is the source of truth

The stored canonical form is authoritative - not derived from some other representation, not a cache of one. There is no second, "raw" copy of a session. Why this is a rule and not just a fact: a derived canonical would invite a parallel raw store and a re-derivation step, and the moment those exist the canonical form is no longer the contract. Completeness of the canonical form is instead guaranteed by `lossless-projection` below.

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

Branching exists only between sessions: a session itself is a linear log of messages with no per-message parent pointers. `parent_session_id` records that a session was spawned or forked from another - a sub-agent, a fork; `parent_message_id` additionally records the cut-point in the parent, for a fork-with-cut-point. A plain spawn (a sub-agent) populates only `parent_session_id`. `parent_session_id` is a soft reference: pond does not require the parent to be present at ingest, since independent adapter runs land in any order.

**`parent-pointer-coherence`** {#parent-pointer-coherence} - A `parent_message_id` MUST NOT be present without a `parent_session_id`. Why: a cut-point with no parent session to cut from is incoherent; the validator rejects such a session.

**`project-non-empty`** {#project-non-empty} - `Session.project` MUST be a non-empty value extracted from real source data. Why: project is the attribution scope every filter and grouping relies on; an adapter that cannot resolve a project drops the session rather than inventing one.

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
model BasePart {
  id: PartID;
  message_id: MessageID;           // back-reference to the containing message
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

Seven variants. `id` and `message_id` on `BasePart` are pond-additive: the model stores Parts as addressable rows with back-references, not as array members. FilePart payloads use the storage layer's blob mechanism (Section 5).

### 4.8 Honesty of the model

Three rules keep the stored canonical form trustworthy and complete. They are enforced by the adapter seam (Section 6), not by convention.

**`no-synthesis`** {#no-synthesis} - An adapter MUST NOT substitute a sentinel, default, or placeholder for source data it could not find. A field that may be absent is typed as an optional sealed value whose only producers are the extractor helpers of Section 6; no path constructs one from a literal in adapter code. Why: a synthesized value is indistinguishable, downstream, from a real one - it is silent corruption. Making synthesis a compile error rather than a code-review rule is the only enforcement that holds. Defaults that describe transport or absence rather than invented field values are allowed and are not synthesis - a timestamp falling back to the session anchor, a failure flag defaulting to false, a generic MIME type.

**`schema-honesty`** {#schema-honesty} - A canonical field that is not optional is a claim that every adapter can always extract it from real source data. If any supported adapter cannot guarantee that, the field MUST become optional - the adapter MUST NOT invent a value to satisfy a non-optional field. Why: optionality is the schema telling the truth about what the sources actually carry.

**`lossless-projection`** {#lossless-projection} - For every source record an adapter ingests, every field that record carried MUST be recoverable from the stored canonical form - mapped to a typed field or Part, or preserved in `options`. An adapter MUST NOT store a proper subset of a record's fields. The only permitted non-capture is a source the adapter deliberately does not ingest at all, which MUST be stated in that adapter's documented contract. A field whose value exceeds the substrate's representable size is preserved as a truncation sentinel recording its original byte count (`bounded-values`, Section 6), not silently dropped - it remains a marked, attributable truncation, which `no-silent-drops` requires and which mere omission would violate. Why: `no-synthesis` forbids inventing values; this forbids dropping them - together they make the stored session a complete and honest record. Section 6 gives the placement procedure that satisfies this rule.

---

## 5. Session datasets {#datasets}

This section is how the canonical model of Section 4 persists on the substrate of Section 3. It is the sessions consumer's storage schema - the first consumer's tables, indexes, and derived embedding store. A future consumer registers its own tables the same way.

### 5.1 Four datasets

The sessions consumer registers four Lance tables: `sessions`, `messages`, `parts`, and `embeddings`. Each is a direct serialization of its canonical type - no projections, no promotions. Typed scalars are typed columns; the open-ended `options` bag and Part variant payloads are stored as JSON text; FilePart binary uses Lance blob storage.

`sessions` - one row per Session:

| Column | Notes |
|---|---|
| `id` | primary key |
| `parent_session_id`, `parent_message_id` | nullable fork pointers |
| `source_agent` | scalar-indexed, low cardinality |
| `created_at` | source-recorded |
| `project` | scalar-indexed; equality and prefix |
| `options` | JSON text |

`messages` - one row per Message:

| Column | Notes |
|---|---|
| `session_id`, `id` | composite primary key; clustered on `(session_id, timestamp)` |
| `timestamp` | scalar-indexed; canonical ordering key |
| `role` | scalar-indexed |
| `source_agent`, `project` | denormalized; filter-pushdown surface |
| `content` | non-null only for system messages |
| `search_text` | the indexed retrieval text (Section 8); full-text indexed |
| `options` | JSON text |

`parts` - one row per Part:

| Column | Notes |
|---|---|
| `message_id`, `id` | composite primary key; clustered on `message_id` |
| `ordinal` | position within the message's content |
| `type` | the Part discriminator; scalar-indexed |
| `variant_data` | JSON text; the variant-specific fields |
| `data` | Lance blob; FilePart payload only |
| `options` | JSON text |

`embeddings` - one row per message per embedding model:

| Column | Notes |
|---|---|
| `message_id`, `model_id`, `max_embed_tokens` | composite primary key |
| `vector` | the embedding; vector-indexed |
| `session_id`, `source_agent`, `project`, `role`, `timestamp` | denormalized; filter-pushdown surface |

### 5.2 Composite keys

`messages` and `parts` use composite primary keys so a source's own ids can be preserved verbatim without requiring them to be globally unique - a message id need only be unique within its session. Clustering keeps a session's messages, and a message's parts, contiguous on disk for sequential reads.

### 5.3 Denormalization

`messages` and `embeddings` carry columns copied from a parent table. A denormalized column is populated by pond core at ingest, is immutable thereafter, and exists solely as a filter-pushdown surface; the parent table remains authoritative for any read outside search. Why denormalize: a vector or full-text query filters and ranks in one pass over one table, and Lance has no relational join planner in pond's crate set - the filter columns must be on the table being searched.

### 5.4 Durability

**`durable-copy`** {#durable-copy} - Once a session is stored, it MUST survive the loss of its source - source rotation, deletion, or expiry. pond is the canonical record after ingest; re-ingest is not a recovery path, because a source that has since rotated or been deleted can no longer supply the rows. Why: being the durable record is the value of pond; a design that silently depended on the source still being reachable would not be one. Recovery runs through Lance's manifest history (retained for the substrate's retention window) and through `pond export` snapshots taken ahead of risky operations.

### 5.5 Embeddings are derived

An `embeddings` row has no canonical-type counterpart - it is produced by pond, not supplied by a source. Its key includes `model_id` and `max_embed_tokens` so vectors from different models, or under a different truncation cap, coexist as distinct rows rather than overwriting one another. Section 8 covers how they are produced and queried.

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

**`lineage-complete-restore`** {#lineage-complete-restore} - Restoring a session MUST also restore its child sessions: the sessions that name it in `parent_session_id`. Why: a restored artifact must stand on its own in the target client - a Claude Code session that called the Task tool, restored without its subagent transcripts, is a set of dangling references rather than a working session. `parent_session_id` records a spawn or a fork (Section 4). The spawn graph is one level deep, capped structurally by the agent model - a Claude Code subagent cannot spawn subagents, and Managed Agents enforces a delegation depth of one. Multi-level fork lineage is deferred (Section 9); no v1 source emits it, so every stored graph is depth-one today. A graph found nesting deeper - a relaxed spawn cap, or fork lineage - MUST surface as a typed error, never a silent partial restore (`no-silent-drops`).

### 6.3 Origin and restore fidelity

Each session records the brand of the source that produced it (`Session.source_agent`), and each adapter has a matching origin identity. Restore fidelity is decided by the system, by comparing the two - never chosen by the adapter:

**`native-restore-lossless`** {#native-restore-lossless} - Restoring a session with the adapter whose origin matches the session's origin is *native* restore and MUST be lossless (value-complete, per Section 1). Restoring with any other adapter is *foreign* restore: best-effort - a valid, idiomatic session in the target's own feature set, dropping whatever the target cannot express (the dropped content remains in canonical). A value truncated under `bounded-values` (Section 6) restores as its truncation sentinel, not its original bytes: a value the substrate physically cannot represent cannot round-trip, and the sentinel records the loss explicitly rather than hiding it. Why the system decides and not the adapter: native losslessness is a contract a caller relies on; leaving "am I native?" to the adapter would make it a convention.

### 6.4 The no-synthesis seam

The parse face builds canonical values only through a small set of extractor helpers that read one record of source data. The type holding a possibly-missing extracted value has no constructor reachable from adapter code - the helpers are its only producers - so an adapter physically cannot place a literal, a default, or a sentinel into a canonical field. This is what makes `no-synthesis` and `schema-honesty` (Section 4) compile errors rather than review rules. The serialize face needs no such seam: canonical is already trusted input.

**`transport-agnostic-seam`** {#transport-agnostic-seam} - The parse seam abstracts one record of source data behind a small set of value accessors and carries no assumption about where that record came from. Why: the same seam serves a file adapter today and an HTTP or stream adapter later, with no change to the seam.

**`bounded-values`** {#bounded-values} - Every value an adapter places into a text column passes through the seam's size bound: a value whose encoding exceeds the substrate's per-value limit is truncated in place to a marked sentinel recording the original byte count, with the rest of the record preserved intact. The bound is a property of the seam's extractor helpers - an adapter cannot emit an unbounded value any more than it can emit a synthesized one. Binary payloads stored as blobs are exempt; the limit is a property of the text-column representation, not of the data. Why: the storage substrate cannot represent a text value at or beyond a hard size, so an unbounded value is not a large row but a process abort - bounding at the seam turns it into an attributable, recoverable truncation.

### 6.5 Placement procedure

To satisfy `lossless-projection` (Section 4), an adapter places every field of every record it ingests by one of three rules:

1. Conversational content becomes a typed Part.
2. Harness or runtime metadata goes into `options` - on the Message or Part the record maps to. This includes any field of a mapped record left over once its typed fields are taken.
3. A record that maps to no Message at all - a standalone log entry that is neither a conversational turn nor metadata on one - is carried whole: a system-role Message with empty `content` and the record's whole-record encoding in its `options`, kept in log order by the record's own timestamp. Its id follows `deterministic-pk` (Section 3) and its timestamp the record's own value, or the session-anchor fallback that `no-synthesis` (Section 4) permits.

The third rule is the catch-all that makes losslessness reachable for any record - including record types that did not exist when the adapter was written.

### 6.6 Ingest order and integrity

**`event-ordering`** {#event-ordering} - An adapter's event stream MUST emit, for each session: the Session first, then each Message immediately followed by its Parts in order, before the next Message. Why: pond core computes a message's indexed text at the message boundary without buffering across messages - the transition off a Part stream is the signal the message is complete.

**`no-silent-drops`** {#no-silent-drops} - Malformed source input MUST surface as a typed error carrying the adapter and the location of the fault; it is never silently skipped. Why: a silent drop is invisible data loss, a surfaced one is a fixable report.

**`opaque-ids`** {#opaque-ids} - Identifiers on canonical objects are opaque strings. An adapter decodes any structure a source encodes into a path or name once, at ingest, and stores the decoded value; readers never re-parse. Why: re-parsing an id at read time couples every reader to a source's encoding.

**`additive-sync`** {#additive-sync} - A write MUST NOT overwrite a row already present under its primary key - matched rows are no-ops. Adapter output is therefore monotone across versions: a newer adapter produces a superset of the rows a prior version produced. Why: the source is not authoritative against pond's stored copy - a re-parse from a since-corrupted source must not be able to overwrite good data. Changing or removing an already-stored row is a deliberate migration, never a side effect of re-ingest.

**`adapter-dedup`** {#adapter-dedup} - An adapter SHOULD detect duplicate primary keys in its own output using the source format's own mechanism; the write path drops duplicates as a floor regardless. Why: sources do emit duplicates - a resumed session can replay events - and catching them in the adapter keeps the count visible in the ingest summary, while the write-path floor keeps storage correct when an adapter misses one.

### 6.7 The registry

Adapters are listed in one registry; adding an adapter is a new file plus one line in that list - there is no central enum or dispatch to edit, and no code generation. Why: a low, fixed cost per adapter is what keeps the source list open-ended.

### 6.8 Conformance

Each adapter has a round-trip codec test: parse a committed fixture to canonical, serialize it back native, and assert the result is value-equal to the fixture - this is what enforces `native-restore-lossless` and exercises `lossless-projection`. Foreign serialization is tested for validity in the target format and reviewed against a golden file.

### 6.9 v1 adapters

Claude Code and Codex, each with both faces, including the cross pairs. Per-source extraction detail - how each adapter resolves `project`, what its `source_agent` brand is, its on-disk layout - lives in that adapter's own code, which is its documentation.

---

## 7. Protocol {#protocol}

The protocol is how requests reach pond and responses leave it, across both transports. HTTP and MCP are thin dispatchers over one shared set of handlers; the handlers know nothing of either transport.

### 7.1 Transport-agnostic handlers

Every operation is a handler function from a request value to a response value. The HTTP transport (axum) and the MCP transport (rmcp) each only decode their wire form into that request value and encode the response back - no operation logic lives in a transport, and a handler cannot tell which transport invoked it.

### 7.2 The request envelope

Every request carries `protocol_version` (a positive integer; v1 is `1`) and an optional `namespace`. Every response - success or error - carries a server-generated `request_id` for log correlation. Schema evolution within a major version is additive only; removing or retyping a field is a major version bump. The precise wire schema is published as JSON Schema generated from the Rust types: this document specifies the contract, the generated schema is the exact artifact.

### 7.3 Namespace

`namespace` is an opaque tenant-routing string; omitted, it selects the personal pond's single namespace. It is distinct from the Lance namespace concept of Section 3 - the same word at two layers: the wire `namespace` selects a tenant, the Lance namespace is how the catalog seam locates that tenant's tables.

**`namespace-resolution`** {#namespace-resolution} - Whether a request's namespace is acceptable, and which stored tables it maps to, MUST be decided in exactly one place. Why: hosted multi-tenancy turns one namespace into many; centralizing the decision makes that a single change, not an edit at every call site.

### 7.4 The error model

Success and error are mutually exclusive at the body level. An error body is one shape:

```json
{ "error": { "code": "validation_failed", "message": "...", "details": {} },
  "request_id": "..." }
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

1. **`pond_search`** (`POST /v1/search`) - hybrid search; Section 8 specifies retrieval. Returns ranked message hits, each reporting which retrievers matched it.
2. **`pond_get`** (`POST /v1/get`) - fetch a whole session, or one message with surrounding context. Toggles control whether reasoning and tool-result Parts are included; by default they are stripped.
3. **`pond_ingest`** (`POST /v1/ingest`) - accept a batch of canonical events. Always batched, bounded by an event count and a body-size cap. Events are grouped by session and applied per session; partial success across sessions is normal and reported per row.
4. **`pond_session_events`** (`GET /v1/sessions/{id}/events`, SSE) - stream a session's messages and parts in order. v1 is catch-up only: it scans, emits, and closes. Live-tail activates with live-write (Section 9) on the same endpoint.

Two resources, `schema://pond` and `stats://pond`, expose the search-field documentation and dataset statistics.

### 7.6 Ingest events

A `pond_ingest` event is one canonical object - a Session, a Message, or a Part - tagged with its kind. Within a session's substream the order is fixed (`event-ordering`, Section 6). `Session.source_agent` and `Session.project` are immutable after first write: a re-submitted session with a differing value for either is rejected for that row, since both are denormalized across the other tables.

### 7.7 MCP surface

The MCP transport exposes only the read operations - `pond_search` and `pond_get` - as tools, plus the two resources. Ingest and session-events stay HTTP-and-CLI only. Why: MCP's role is read access for an agent; ingest is an operator action.

### 7.8 CLI verbs

The same handlers back a set of command-line verbs:

- `pond sync` - parse, store, and index from the configured sources.
- `pond embed` - embed the backlog of un-embedded messages (Section 8).
- `pond search`, `pond get` - the read operations from the command line.
- `pond export` - export stored sessions as canonical ingest events, a portable snapshot byte-compatible with `pond_ingest` input, for one named session or the whole pond. Restore is a distinct mode: it serializes one named session, with its lineage (`lineage-complete-restore`, 6.2), into a target client format through that adapter's serializer. Restore is always rooted at a single named session - there is no bulk restore-to-client-format - because a restore targets a session the caller has identified, while whole-pond transfer is already served by the canonical snapshot.
- `pond serve` - run the HTTP server, including the MCP route.
- `pond mcp` - run the MCP server over stdio.
- `pond status` - row counts and dataset statistics.
- `pond config` - emit an annotated configuration template.

### 7.9 Versioning

The wire protocol versions through `protocol_version` and additive-only schema changes. The canonical model and the storage schema evolve additively too; the Lance manifest carries the storage schema version (`dataset-schema-version`, Section 3). pond is pre-release: there are no compatibility shims, and a breaking change is a major version bump, not a migration layer.

---

## 8. Search and embeddings {#search}

Search returns messages. It is hybrid - a vector retriever and a keyword retriever, fused - and runs at message granularity. This section also specifies the embedding seam, a generic capability the session datasets consume rather than a part of them.

- **Hybrid retrieval.** A search runs two retrievers over the same corpus: a BM25 full-text retriever over each message's indexed text, and a vector retriever over message embeddings. Their ranked results are fused by reciprocal-rank fusion. Both retrievers operate at message granularity and agree on row identity, so fusion needs no per-chunk deduplication. When embeddings are absent the search runs full-text only; the mode is decided by the server from embedding availability, not requested on the wire, and each hit reports which retrievers matched it.

  **`prefilter-pushdown`** {#prefilter-pushdown} - Every vector and full-text query MUST push its scalar filters into the table's scalar indexes before the retriever ranks, never as an in-memory post-filter. Why: a post-filter ranks first and filters second, so it silently returns fewer than the requested number of results and ignores the scalar indexes entirely - correctness depends on the filter running first.

- **Indexed text.** What is searchable is one text field per message, built at ingest by one pond-core function applied uniformly to every message - per-source customization is rejected so the search corpus has one predictable shape. The field concatenates, in order, the text of TextParts and the metadata of FileParts. It is null for system and tool messages, and for any message whose only content is non-text (a bare tool call). Reasoning text, tool-call bodies, tool results, and approval parts are deliberately not indexed - they are retrievable through `pond_get` but they are plumbing, not the conversation. A message with null indexed text is absent from search but still returned in full by `pond_get`.
- **Filters and ranking.** A search accepts filters on project, session, source agent, role, and a time range, plus a minimum score. A recency boost is applied additively by default. Results may be grouped to one summary per session. Filter columns are denormalized onto the searched tables (Section 5) so every filter pushes down without a cross-table join.
- **The embedding seam.** Turning text into vectors is a generic capability, not a session concept. It sits behind one seam - a backend interface that takes text and returns vectors - so a local model today and a remote provider later are the same shape to everything above. The set of usable models is a configuration registry: each entry declares a model's identity and vector parameters. The registry is generic; no specific model is enshrined in this document or in the engine - the default is a configuration value.
- **The embedding worker.** Embeddings are derived, not source data, and are produced after ingest by a worker that walks the backlog of un-embedded messages and writes one vector per message. The worker is the session datasets' use of the embedding seam; a future consumer that wants vectors brings its own worker over its own table, reusing the same seam. v1 runs the worker only on the explicit `pond embed` verb - it is not automatic.
- **Opt-in.** Embedding is opt-in by configuration. With it off, `pond serve`, `pond mcp`, and `pond search` run full-text only and never load a model. With it on and at least one vector present, search is hybrid.
- **Index lifecycle.** The indexed-text and vector columns exist from table creation; turning embeddings on or off never needs a schema migration. The indexes over them are built lazily as data lands, not up front. The full-text index has no training step and is built once the table holds data. The vector index needs training data, so it cannot be built on an empty table and trains poorly at small scale - it is built once the embeddings table crosses an activation threshold, and below that threshold vector search runs a brute-force flat scan, which is fast at small and medium scale. Retrieval is correct and complete whether or not an index exists yet: against an unindexed table, or against rows appended since the last build, the engine transparently flat-scans the uncovered rows and merges them with any indexed results. The maintenance pass (Section 3) folds appended rows into the indexes.

---

## 9. Deferred {#deferred}

These are scoped out of v1. None requires a schema migration or a cross-cutting change when it activates - the v1 design forecloses none of them. Each entry names the work and the trigger that would start it.

1. **Future consumers.** The storage substrate (Section 3) is generic; the session datasets are its first consumer, and these are the next. Each is a separate consumer with its own canonical model and its own tables on the same substrate - not an extension of sessions.
   - **Resources and blobs** - per-namespace knowledge-base files. Trigger: a concrete need for file storage alongside sessions.
   - **Social and web content archives** - exports from Telegram, Discord, Twitter, Reddit, GitHub. These are flat-message content, not LLM conversations, so they get their own canonical model rather than being coerced into Session/Message/Part. Trigger: a decision to bring an export corpus into pond.
   - **A file and blob store shaped like the Files API** - upload, reference, download. Trigger: integrator demand.
   - **A versioned-document store shaped like agent memory stores** - small text documents with an immutable version history. The substrate's manifest versioning aligns naturally with this. Trigger: integrator demand.
2. **Future source adapters.** A new adapter adds no substrate or schema change - it is a new file and a registry line (Section 6).
   - **A Managed Agents adapter**, including multi-agent sessions: a coordinator and its delegated agent threads map onto linked Sessions through `parent_session_id` and `parent_message_id`. The spawn case already works for v1 sources (Claude Code subagents ingest as linked child Sessions); the Managed Agents adapter is the next step.
   - **Other clients** - OpenCode, Cursor, aider, Gemini CLI, and more.
3. **Provider-target restore.** Restoring a canonical session into a provider API request shape (Anthropic, OpenAI, Bedrock, Gemini), as opposed to a harness session-log format. Always foreign, and additionally constrained to produce API-valid output. Trigger: integrator demand.
4. **Live-write.** Ingesting events as a session runs, rather than after it ends. The substrate work this needs - a per-shard write-ahead layer, a scanner that merges in-memory and on-disk generations, a sharded writer - is real implementation work; the forward-compatibility seams of Section 3 are what keep their activation a substrate swap rather than a rewrite. Trigger: a use case needing in-flight search of a running session, or hosted write fan-out beyond optimistic-concurrency tolerance.
5. **Hosted multi-tenant.** Mapping each tenant to a child Lance namespace, and swapping the directory catalog for a hosted one. The catalog seam and the single namespace-resolution point (Sections 3 and 7) are the seams this rides. Trigger: the first hosted tenant.
6. **Other.** Remote embedding providers; cross-session attachment deduplication; a graph-traversal layer over fork lineage; wire-surfaced time-travel queries; an OTel-compatible projection of the canonical model; indexing the contents of file attachments.
7. **Open questions.** Undecided, each pending a trigger: what event first activates the multi-tenant router; what use case first activates live-write; which catalog backend the hosted tier uses.

---

## 10. References {#references}

External work that informed this design. These are inspiration and corroboration; the contract is Sections 1 through 9.

- [Scaling Managed Agents](https://www.anthropic.com/engineering/managed-agents) - Anthropic Engineering. The session-as-append-only-event-log framing, and the meta-harness idea of modeling the stable conversational layer while pushing volatile harness behavior outward - the shape the canonical model and the `options` bag follow.
- [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) - Anthropic Engineering. On curating what enters an agent's context window; background for why a durable, searchable store of session history is worth building.
- [Context Rot](https://www.trychroma.com/research/context-rot) - Chroma research. On the degradation of model performance as input context grows - the same motivation, seen from the retrieval-quality side.
- [Recursive Language Models](https://arxiv.org/html/2512.24601v3) - arXiv 2512.24601. Treats long context as an external, queryable environment and recursion as sub-agent spawning; a recursive run captures as linked Sessions, which corroborated the branching model of Section 4.
