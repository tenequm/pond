---
title: Specification
description: The pond v1 contract - storage substrate, canonical Session / Message / Part model, adapters, protocol, search and embeddings.
---

# Pond - Specification v1

pond stores agentic-client sessions: it ingests them from many client formats into one canonical form, keeps them in Lance, searches them, and hands them back. One static binary, two transports, two deployments. This document specifies pond v1.

## Contents

1. [Overview](#1-overview) - what pond is, the interchange-hub model, the stack, how to read this document.
2. [Scope](#2-scope) - what v1 ships and the stable non-goals.
3. [Storage substrate](#3-storage-substrate) - the generic Lance engine every consumer builds on.
4. [Canonical model](#4-canonical-model) - the Session / Message / Part interlingua.
5. [Session datasets](#5-session-datasets) - how the canonical model persists in Lance.
6. [Adapters](#6-adapters) - the bidirectional codec between client formats and canonical.
7. [Protocol](#7-protocol) - the wire interface, operations, and CLI verbs.
8. [Search and embeddings](#8-search-and-embeddings) - single-arm retrieval (vector or full-text) and the embedding seam.
9. [Deferred](#9-deferred) - work scoped out of v1.
10. [References](#10-references) - external work that informed this design.

---

## 1. Overview

This section states what pond is and the single idea the rest of the document elaborates. Read it first - it is the map for everything below.

### 1.1 What pond is

pond ingests sessions from agentic clients - Claude Code, Codex, and others on the roadmap - into one canonical form, stores them in Lance, and serves search (vector or keyword, one arm per query) over them at message granularity. It ships as a single static binary that exposes two transports - an HTTP+JSON API and an MCP server - over one shared set of handlers, and runs in two deployments (Section 2.2).

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

## 2. Scope

pond v1 is deliberately narrow: one application, an adapter registry, two deployments. This section fixes that boundary. Work scoped out of v1 is in Section 9; the non-goals here are different - they are stable positions, not deferrals.

### 2.1 What v1 ships

1. **One application: sessions.** Lossless ingest, storage, and search of agentic-client sessions. Sessions are the first consumer of the storage substrate (Section 3); future consumers are in Section 9.
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
- **Become a SQL database, a UI, or a sidecar daemon.** The primary query surface is the search and filter API of Section 8, which compiles to Lance scalar predicates and search calls. Alongside it sits one read-only SQL escape hatch - the `pond_sql` tool (Section 7.7) and the `pond sql` verb (Section 7.8): a single SELECT planned by embedded DataFusion over the same Lance datasets, never a write path and never a second storage engine. The only engine is embedded Lance; there is no UI and no daemon beyond `pond serve`.

### 2.4 Platform

Linux, macOS, and Windows (x64). The published Windows binary is a native `x86_64-pc-windows-msvc` build, statically linked against the VC runtime and verified by native Windows CI that runs the full test suite on every pull request and release. `protobuf-src` cannot vendor protoc on Windows, so a system `protoc` (and NASM, for `aws-lc-sys`) is needed only when building from source there. The scheduler backs onto Task Scheduler in place of launchd/systemd. `local-store-durability` (Section 3.3) applies, as a file flush without the directory fsync Windows lacks. Embeddings run on the CPU backend (Metal is macOS-only, CUDA is Linux opt-in).

---

## 3. Storage substrate

The storage substrate is the layer that owns how pond uses Lance - opening datasets, scanning, writing, concurrency, retention. It knows nothing of sessions: a consumer hands it table schemas and gets a place to store and query rows. It is specified first, and generically, because that is what keeps it reusable by every consumer that follows.

### 3.1 Purpose

A consumer - the session datasets in v1, others later (Section 9) - does not use Lance directly. It declares its tables to the substrate and then stores and queries rows through it. The substrate guarantees durable append-only storage, safe concurrent writers, retry around transient faults, and bounded read staleness. It does not interpret rows: column meaning, indexes, and denormalization belong to the consumer (Section 5 for sessions).

### 3.2 Lance chokepoints

Every interaction with Lance funnels through one of four chokepoints, each a single code path. Together they are the **`lance-chokepoints`** rule; each chokepoint below is referenced as `lance-chokepoints-<name>`.

#### `lance-chokepoints-catalog`

Every dataset open MUST resolve the table's location through one catalog lookup; no code constructs a dataset path directly. Why: the catalog is where a local directory layout is swapped for a hosted catalog - centralizing it makes hosted multi-tenancy a configuration change, not a cross-cutting edit.

#### `lance-chokepoints-read`

Every scan and search query MUST be built through the substrate's read path. Why: it is the one place `search-prefilter-pushdown` (Section 8) is enforced, and the one place a future scanner change lands.

#### `lance-chokepoints-write`

Every write MUST go through the substrate's write path - merge-insert for rows that may collide, append for rows that cannot (absent rows under the deterministic PK). Writes through this chokepoint never fold indexes - index lifecycle lives under `lance-index-maintenance` (Section 3.7). Why: `lance-append-only` and `adapter-integrity-additive-sync` (Section 6) hold only with a single write chokepoint; a direct write bypasses both.

#### `lance-chokepoints-storage`

Every read, list, or write of dataset bytes MUST go through Lance's object-store layer; no code resolves a dataset to a local path. Why: a `file://` pond and an `s3://` pond behave identically only when no code reaches around Lance; a single direct-FS access silently backend-locks that operation.

### 3.3 Data integrity

#### `lance-append-only`

Stored rows MUST NOT be mutated; an update produces a new row or a new manifest version. Why: it forecloses corruption-by-mutation and makes every write idempotent under retry.

#### `lance-deterministic-pk`

Every row MUST have a deterministic primary key - source-supplied where the source carries a stable id, content-derived otherwise. Writes are idempotent on the key - merge-insert no-ops a present row, the append path writes only absent rows - so a retried or re-run write is a no-op for rows already present. Why: idempotent ingest depends on the key being reproducible from the source data alone.

#### `lance-dataset-schema-version`

Schema versioning lives at the dataset level - the Lance manifest and a dataset-level metadata key - never as a per-row column. Why: a per-row version column pays storage on every row for a fact that is per-dataset.

#### `local-store-durability`

On a local-filesystem store, every object-store write MUST be made durable before the write returns - on unix the file bytes and the parent directory, on Windows the file bytes alone, since `FlushFileBuffers` fails on a directory handle (object_store, RocksDB, and SQLite all skip it for the same reason) and `local-store-self-heal` covers the residual window. Applied through Lance's own object-store wrapping seam so `lance-chokepoints-storage` still holds (the wrapper wraps the store; no code reaches around Lance). Why: `LocalFileSystem` publishes a file's name via hard-link/rename without syncing its bytes, and NTFS journals metadata but not file data, so a hard host stop (power loss, VM reset, kernel panic) can persist a manifest's name while dropping its content - a zero-byte manifest at the head of `_versions/` that permanently poisons the table. Syncing at the seam is ordering-correct by construction: each artifact is durable before Lance proceeds to the next, so data files and transaction manifests are on disk before the commit that names them.

#### `local-store-self-heal`

When a local table fails to open, the substrate MUST attempt self-heal before surfacing the error: walk `_versions/` head-down to the newest fully readable version - readable means the manifest loads AND a real scan over its data pages completes, because a crash inside a multi-commit cycle can zero a data file that an older, intact manifest references; the scan MUST project every column, because a column-update commit puts later-added columns in their own per-fragment data files that a narrower scan would never read - quarantine every unreadable manifest above that version by atomic same-directory rename to `<name>.manifest.corrupt` (never delete anything), retry the open once, and emit a loud notice naming the quarantined files, the version rolled back to, and that the next `pond sync` re-ingests the aborted commit. Heal is lossless for pond because source histories are the source of truth (`session-movement-complete`); the rolled-back rows are reconstructed, not lost. When no version is readable, or the failure is not manifest-shaped, heal MUST touch nothing and return the original error enriched with what was inspected and the concrete recovery. Why self-heal on open instead of a repair command: the damaged state is a crash remnant, not an operator decision - every surface (CLI, HTTP, MCP) opens through the same path, so healing there fixes the store on first touch with no human in the loop, and quarantine-by-rename keeps the evidence without leaving Lance anything to trip over.

### 3.4 Dataset parameters

Every table is created with the current stable Lance file format, constant-time latest-manifest lookup, and a short manifest-retention window. v1 recovery beyond the window is via `pond copy --from <store> --to <file>` snapshots; deferred named-snapshot preservation via Lance tags is in Section 9. The retention window MUST stay above the longest single read (floor: one hour): a reader pins the manifest version it opened only for that request's duration, and version cleanup that reclaims a pinned version's files breaks the in-flight read on object-store backends.

#### `lance-table-creation`

Every table MUST be created with:

##### `lance-table-creation-stable-row-ids`

Stable row ids, so secondary indexes survive compaction without being rewritten to follow moved rows; without them, every compaction pass rewrites every index.

##### `lance-table-creation-unenforced-pk`

An unenforced primary key on the primary-key columns, so merge-insert defaults to the right key with no per-call wiring, and so the forward-compat seams below have something to attach to.

##### `lance-table-creation-session-scoped-pk`

`session_id` leading the primary key on every table below `sessions` - a source's message and part ids are unique only within their originating session; lineage operations (spawn, `/compact`, resume, fork) copy a parent's history into a new session and replay its ids unchanged. A key omitting `session_id` collides on every replayed id; the unenforced PK means the substrate will not catch the collision. Leading with `session_id` also satisfies `lance-forward-compat-shardable` for these tables.

All of a consumer's datasets share one Lance cache and one object-store client. Why: one pool, rather than one per table, avoids multiplying connections and credential refreshes on object-store backends.

#### `lance-compaction-filter`

Automatic compaction runs only Lance-planned tasks that pass pond's filter. Tasks above the deletion-materialization threshold always run - reclaiming tombstones is useful even when layout does not improve. Any other task must strictly reduce the fragment count, and when its total bytes exceed one output file's byte budget, every input fragment's observed row width must let an output reach the row target within half that budget (headroom because re-encoding can widen rows relative to the input estimate). Missing file sizes fail closed and skip the optional rewrite. Why: Lance selects candidates by row count while the re-encoding writer splits output by bytes, so a task whose rows are too wide for the target emits outputs that are immediately re-selected - an unbounded rewrite loop (measured: a ~1 GB table rewrote ~30 GB across 31 syncs with zero net progress). A task whose whole volume fits one output cannot strand sub-target outputs, so the width test applies only beyond that point.

### 3.5 Concurrency

pond processes are stateless workers. Several may write the same namespace at once; Lance optimistic concurrency control resolves append conflicts through manifest versioning. There is no external coordinator - object stores provide atomic conditional writes, the local filesystem uses Lance's commit lock - and no in-process write queue.

#### `lance-retry-jitter`

Every call into Lance MUST be wrapped in bounded retry with exponential backoff and jitter. Why: transient object-store faults and lost concurrency races are expected, not exceptional; retry turns them into latency rather than errors. Exception: a non-idempotent batch append (no row-level PK dedup) MUST gate retry to commit-conflict only - a transient fault arriving after the manifest commit landed but before its ack would re-append duplicate rows, so it surfaces instead and the caller re-plans from current destination state on its next run.

#### `lance-handle-freshness`

A cached dataset handle MUST be freshness-checked before serving a read, and refreshed if older than the staleness window. The window is keyed to the backend: zero for a local filesystem, where a manifest re-read costs microseconds; a few seconds for an object store, capping manifest-fetch overhead. Why: a long-lived server owns the window between an external commit and a reader seeing it - making the window explicit and backend-keyed keeps it bounded.

### 3.6 The conflict contract

When retry is exhausted on a write, the substrate raises a typed conflict signal carrying the attempt count. The wire layer (Section 7) maps it to the retryable `conflict` error code. The dependency is one-way: the wire layer knows the substrate's conflict signal; the substrate knows nothing of the wire error model.

### 3.7 Index lifecycle

#### `lance-index-maintenance`

Writes commit data without folding indexes; index maintenance is operator-triggered via the index stage of `pond optimize` (run by default at the tail of `pond sync`; Section 7.8). A trailing index is not a correctness problem: Lance reads merge index results with a flat scan over unindexed fragments, so a query before maintenance returns complete results, just slower. The fold strategy is determined by index family, not by the write that preceded it:

| Family | Fold on `pond optimize --only index` | Why |
|---|---|---|
| BTree (scalar) | `optimize_indices(append)` | Merges existing sorted index pages with only the new fragments' data; never re-scans already-indexed source. |
| Bitmap (scalar) | `optimize_indices(append)` | Incremental fold is safe. |
| Inverted (FTS) | `optimize_indices(append)` | Incremental fold is safe. |
| IVF_SQ (vector) | `optimize_indices(append)` | Stable-row-id IVF supports incremental fold via `IvfIndexBuilder::new_incremental`; centroids and per-dimension SQ ranges carry forward. |

Index maintenance skips indexes whose columns no write has touched; this is sound because Lance prunes index coverage only for indexes whose fields overlap a write's modified fields. Why operator-triggered: matching Lance's own design (`table.optimize()` is the canonical periodic-maintenance op) avoids per-write index rebuilds that would otherwise dominate write cost.

### 3.8 Forward-compatibility seams

#### `lance-forward-compat`

Three sub-rules cost almost nothing in v1 and keep horizontal-scale work (Section 9) a substrate swap rather than a rewrite:

##### `lance-forward-compat-shardable`

On a high-volume table, the first primary-key column MUST be an attribute coarse enough to shard on. A sharded writer attaches a shard spec to an existing column; if no first-position column is shardable, enabling sharding later means a primary-key redesign and a migration of every existing row.

##### `lance-forward-compat-no-subsecond-freshness`

No operation MAY promise that a write is visible to a read within milliseconds; the floor is the `lance-handle-freshness` window. An in-memory write-ahead layer makes a write durable at once but visible to the base table only after an asynchronous merge - had pond contracted sub-second read-after-write, adding that layer would break the contract.

##### `lance-forward-compat-no-cross-shard-atomic-write`

No write batch MAY span more than one primary-key family atomically; each batch is keyed on a single family. A sharded writer assigns each PK family to one shard, so cross-shard atomicity is structurally unavailable under sharding.

### 3.9 Storage addresses and credentials

Addresses are URLs; credentials are URL-scoped sets. There is no named-storage registry and no "active storage" process state - that design was considered and rejected: an address is a static infrastructure fact, not per-invocation state.

#### `storage-url-grammar`

A storage destination is one URL: a local path (`/abs`, `~/`, `file://`), `s3://bucket/prefix`, `s3+https://host/bucket/prefix` / `s3+http://host:port/bucket/prefix` (S3-compatible endpoints; TLS and `allow_http` are scheme-derived, never config), `gs://bucket/prefix`, `az://account/container/prefix`, or the test-only `memory://` / `shared-memory://`. The `s3+` form carries the endpoint inside the URL so it can never desync from the bucket - the out-of-band-endpoint failure class litestream patched three times. The region is never required: real AWS buckets auto-resolve their region inside Lance, and `s3+` endpoints get a deterministic default (S3-compatible stores ignore the SigV4 region; a fixed default beats an env-dependent fallback), overridable via the creds-set field or `?region=`. Embedded userinfo MUST be rejected at parse: argv, history, and logs are one leak class. Recognized query params (`creds`, `region`, `virtual_hosted_style_request`) are stripped before the URL reaches Lance and beat the matched set's same-named fields; an unrecognized param is a hard error.

#### `creds-scope-match`

A credential set binds to a URL by match, never by activation: `?creds=<name>` pointer (missing set = error) > longest matching `scope` prefix > the single scope-less catch-all > none. Scopes compare canonicalized (scheme/host lowercased, default ports stripped), match only at `/` segment boundaries (`.../pond` does not match `.../pond-2`), and never across schemes; duplicate canonical scopes are a parse error, so ties cannot exist. A set matching no URL in a remote-touching invocation is named in a warning - misbinding must never be silent. Why match-not-activate: the git `[credential "url"]` / DuckDB scoped-secrets model gives multi-storage commands zero extra syntax and keeps rotation out of argv (pond's primary callers are cron and MCP, where the invocation is frozen).

#### `storage-configless`

Every command MUST work with no config file: URLs plus env vars are a complete configuration. A URL that resolves to no set is passed with no credential options, so the ambient cloud SDK chain (instance profiles, task roles, OIDC, `aws sso login`) applies. Why: the disaster-recovery posture - restore and migrate must run on a machine where the file never existed - and the zero-config container/CI path.

#### `storage-env-mirror`

The env mirror is mechanical: `storage.path <-> POND_STORAGE_PATH`, `creds.<name>.<field> <-> POND_CREDS_<NAME>_<FIELD>`; env sets merge field-by-field over same-named file sets; `extra` has no env form. Set names MUST match `[a-z][a-z0-9]{0,15}` - field names contain underscores, so the name charset is what keeps the env grammar splittable. One precedence ladder everywhere: CLI flag > `POND_*` env > config file > ambient chain > built-in defaults.

#### `storage-redaction`

Secrets MUST NOT appear in URLs or CLI flags; they travel via config file, env, `<field>_file`, or `<field>_command` only (command output cached per process, one trailing newline stripped). Introspection redacts any field whose name contains `key` / `secret` / `token` / `password` (including `extra` keys); the `_file` / `_command` variants print literally - the path or command is the safe part.

---

## 4. Canonical model

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

#### `model-parent-pointer-coherence`

A `parent_message_id` MUST NOT be present without a `parent_session_id`. Why: a cut-point with no parent session to cut from is incoherent; the validator rejects such a session.

#### `model-project-non-empty`

`Session.project` MUST be a non-empty value extracted from real source data. Why: project is the attribution scope every filter and grouping relies on; an adapter that cannot resolve a project drops the session rather than inventing one.

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

Four role variants with per-role content allowlists enforced at the type level - a tool-result Part inside a user message is a category error. SystemMessage content is a plain string, not Parts; it may be empty when the SystemMessage is a placement-rule-3 carrier (Section 6.5), which records absence and is not synthesis. Messages within a session form a linear append-only log ordered by `(timestamp, id)`. The tiebreaker after `timestamp` MUST be source-intrinsic - the id, or a preserved source position - never a write-time counter, which would differ across re-ingests and break idempotency (`lance-deterministic-pk`). Turn-level metadata - model, token usage, finish reason, error - is not a canonical field; clients record it on their assistant turns and adapters route it to `options.<provider>.*`.

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

#### `model-no-synthesis`

An adapter MUST NOT substitute a sentinel, default, or placeholder for source data it could not find. A field that may be absent is typed as an optional sealed value whose only producers are the extractor helpers of Section 6; no path constructs one from a literal in adapter code. Why: a synthesized value is indistinguishable, downstream, from a real one - it is silent corruption. Making synthesis a compile error rather than a code-review rule is the only enforcement that holds. Defaults that describe transport or absence rather than invented field values are allowed and are not synthesis - a timestamp falling back to the session anchor, a failure flag defaulting to false, a generic MIME type.

#### `model-schema-honesty`

A canonical field that is not optional is a claim that every adapter can always extract it from real source data. If any supported adapter cannot guarantee that, the field MUST become optional - the adapter MUST NOT invent a value to satisfy a non-optional field. Why: optionality is the schema telling the truth about what the sources actually carry.

#### `model-lossless-projection`

For every source record an adapter ingests, every field that record carried MUST be recoverable from the stored canonical form - mapped to a typed field or Part, or preserved in `options`. An adapter MUST NOT store a proper subset of a record's fields. The only permitted non-capture is a source the adapter deliberately does not ingest at all, which MUST be stated in that adapter's documented contract. A field whose value exceeds the substrate's representable size is preserved as a truncation sentinel recording its original byte count (`adapter-bounded-values`, Section 6), not silently dropped - it remains a marked, attributable truncation, which `adapter-integrity-no-silent-drops` requires and which mere omission would violate. Why: `model-no-synthesis` forbids inventing values; this forbids dropping them - together they make the stored session a complete and honest record. Section 6 gives the placement procedure that satisfies this rule.

#### `model-part-provenance`

Every Part MUST be classified `conversational` (its content was authored by the human user or generated by the model as part of the exchange) or `injected` (its content was produced by the runtime or harness and inserted into the transcript - environment context, memory or rules injection, system reminders, task notifications, command echoes, tool output). A Part is provenance-homogeneous: where a source record fuses authored and injected content in one span, the adapter splits it into separate Parts (Section 6.5). Why: harness-injected content occupies a conversational slot and is byte-identical in shape to a real turn, yet it is not the conversation - search MUST exclude it (Section 8) while restore MUST preserve it. `role` records the conversational slot, not the author; without this marker the distinction is unrecoverable. It cannot be decided once for all harnesses - only the adapter for a given source knows its injection patterns, so the classification is a per-adapter obligation the seam compels (`adapter-provenance-required`, Section 6). The enum is two variants in v1 and extends additively as a consumer needs finer kinds; the specific injected kind meanwhile survives in `options`.

#### `model-pond-options`

`options.pond` is the pond-owned namespace: adapters and wire clients MUST NOT populate it, and core ingest strips and restamps it unconditionally on every incoming Message, so it cannot be spoofed through any ingest surface. It carries the ingest host provenance stamp, `{"ingest": {"host": {"username", "hostname", "device_name"}}}` - the host of the process that inserted the row, resolved once per process. This is an audit fact, not identity, tenancy, or authorization. Fields whose lookup fails are omitted, never synthesized (the stamp itself is omitted when nothing resolves - the `model-no-synthesis` posture applied to pond's own metadata). Only Message rows are stamped: a Part travels in the same ingest substream as its Message, and a Session row would always duplicate its first message's stamp while inviting an identity reading - session attribution is a derived query over message stamps, and a session legitimately accumulates messages stamped by multiple hosts (live-write, reconnects, repair). Rows are stamped at insert only - matched rows are merge_insert no-ops, so re-ingest never restamps and backfill, if ever wanted, is an explicit command, not a sync side effect.

---

## 5. Session datasets

This section is how the canonical model of Section 4 persists on the substrate of Section 3. It is the sessions consumer's storage schema - the first consumer's tables and indexes. A future consumer registers its own tables the same way.

### 5.1 Three datasets

The sessions consumer registers three Lance tables: `sessions`, `messages`, and `parts`. Each is a direct serialization of its canonical type plus a small, named set of derived storage columns with no canonical counterpart: `messages` carries the message's derived embedding (5.5), and `parts` carries the materialized tool-identity columns (5.6). Nothing else is projected or promoted.

`sessions` - one row per Session:

| Column | Notes |
|---|---|
| `id` | primary key |
| `parent_session_id`, `parent_message_id` | nullable fork pointers |
| `source_agent` | low cardinality; the scalar-indexed copy is the denormalized `messages.source_agent` (5.3) |
| `created_at` | source-recorded |
| `project` | the denormalized copy is `messages.project` (5.3) |
| `options` | JSON (Lance `pa.json_()`, stored as JSONB) |

`messages` - one row per Message:

| Column | Notes |
|---|---|
| `session_id`, `id` | composite primary key; clustered on `(session_id, timestamp)` |
| `timestamp` | canonical ordering key |
| `role` | message role |
| `source_agent` | denormalized; scalar-indexed filter-pushdown surface |
| `project` | denormalized filter column |
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
| `type` | the Part discriminator |
| `provenance` | conversational vs injected (`model-part-provenance`, 4.8); search reads it to exclude injected scaffolding |
| `tool_name` | derived tool-identity column (5.6); scalar-indexed - tool analytics run on the narrow columns instead of scanning `variant_data` |
| `call_id` | derived tool-call correlation id (5.6) |
| `is_failure` | derived ToolResult flag (5.6); non-null only on `tool_result` rows |
| `variant_data` | JSON (Lance `pa.json_()`, stored as JSONB); the variant-specific fields |
| `data` | Lance blob; FilePart payload only |
| `options` | JSON (Lance `pa.json_()`, stored as JSONB) |

### 5.2 Composite keys

`messages` and `parts` use composite primary keys that lead with `session_id` (`lance-table-creation-session-scoped-pk`). A source's own message and part ids are preserved verbatim without requiring global uniqueness: such an id is unique only within its session, and lineage operations - sub-agent spawn, `/compact`, resume, and fork - copy a parent session's history into a new session and replay its message and part ids unchanged. The leading `session_id` keeps each session's copy distinct; a key omitting it collides on every replayed id. Clustering on `(session_id, ...)` keeps a session's messages and parts contiguous on disk for sequential reads.

### 5.3 Denormalization

`messages` carries `source_agent` and `project` copied from its `sessions` parent. A denormalized column is populated by pond core at ingest, is immutable thereafter, and exists solely as a filter-pushdown surface; `sessions` remains authoritative for any read outside search. Why denormalize: a vector or full-text query filters and ranks in one pass over `messages`, and Lance has no relational join planner in pond's crate set - the filter columns must be on the table being searched.

### 5.4 Durability

#### `session-durable-copy`

Once a session is stored, it MUST survive the loss of its source - source rotation, deletion, or expiry. pond is the canonical record after ingest; re-ingest is not a recovery path, because a source that has since rotated or been deleted can no longer supply the rows. Why: being the durable record is the value of pond; a design that silently depended on the source still being reachable would not be one. Recovery runs through `pond copy --from <store> --to <file>` snapshots taken ahead of risky operations; the manifest-retention window (Section 3.4) is short and not a recovery floor on its own.

#### `session-movement-complete`

Storage MUST be the union of every still-reachable source: the completeness complement to `adapter-integrity-additive-sync`'s monotonicity (Section 6) - together, losing nothing already stored and skipping nothing a source still holds. The freshness skip that lets `pond sync` avoid re-decoding unchanged sources, and the per-session delta key that lets `pond copy` move only what changed, are optimizations: neither may leave an un-ingested row unstored. Because the datasets commit non-atomically (`no-cross-shard-atomic-write`, Section 3.8), any "already ingested" signal MUST derive from stored data, never from a marker written before that data is durable - so a partial flush re-ingests on the next run rather than latching a false "done". `pond copy` enforces this with an unconditional closing composite-PK verify (exit 6 on a missing row, or on a destination duplicate - a row appearing under more than one copy of the same key), every run. The signal MUST stay cheap on every backend - bounded by the data scanned, not by stored history. The freshness skip MAY also skip a source that a bounded whole-source inspection proves currently holds nothing ingestible; that proof MUST be re-derived from current source content on every run - never a cached marker - and any source the gate cannot cheaply classify MUST re-read. Why: monotone-but-incomplete is still silent loss - a skip that outruns durability, or a delta with no backstop, drops data while reporting success.

#### `session-append-only-exception`

Erasing a whole session is the single exception to pond's append-only design and the only deletion pond performs. Append-only governs events within a session - a stored Message or Part is never mutated, reordered, or removed; this rule retires an entire session object and its descendants. It is operator-only - `pond erase <session-id>` on CLI and HTTP, never on the MCP read surface - and cascades to child sessions (those naming the target in `parent_session_id`), the deletion mirror of `adapter-lineage-complete-restore` (Section 6.2). Erasure is a true byte purge, not a tombstone: a delete predicate, then compaction, version-history cleanup, and blob purge, so time-travel retains nothing. An erased key enters a denylist the ingest path consults, so a later `pond sync` from a still-present source cannot resurrect it - the denylist is the subtraction term that keeps `session-movement-complete` sound under erasure (storage is the union of reachable sources minus erased keys). The operation names what it erased and what the denylist now blocks. Why: right-to-erasure compliance needs real deletion with no resurrection and no retained bytes; making it the single named operator-only exception leaves every other guarantee (`session-durable-copy`, `adapter-integrity-additive-sync`) intact and unambiguous.

### 5.5 Embeddings are derived

A message's embedding has no canonical-type counterpart - it is produced by pond, not supplied by a source. It is two nullable columns on `messages`: `vector`, the embedding, and `embedding_model`, the model that produced it. Both are populated in the ingest commit when embedding is enabled (Section 8); a message ingested with embedding disabled keeps them null until a later `pond optimize` embed pass fills them.

#### `session-embed-from-canonical`

A message's embedding MUST be derived from its stored `search_text`, never from the source record. Why: `search_text` is durable (`session-durable-copy`) and the source is not - deriving from canonical is what lets pond re-embed under a new or changed model at any later time with no source present, making a model change a re-derivation, not a migration.

Re-embedding rewrites only `vector` and `embedding_model`; no canonical column is touched, and each rewrite lands as a new manifest version, not a row mutation (`lance-append-only`). A model swap is a single conditional `merge_update` keyed on `target.embedding_model != source.embedding_model`: stale rows update, up-to-date rows are left alone, and the vector index is dropped before new vectors arrive (centroids belong to one distance space; the next index stage rebuilds it). A same-dimension swap rewrites `vector` in place; a different-dimension swap adds a new column, backfills from `search_text`, drops the old, and renames - all on `messages`, never a new table. Lance's manifest history retains prior vectors, so a regressed swap rolls back without a re-ingest. Section 8 covers how embeddings are produced and queried.

### 5.6 Derived analytics columns and additive schema migration

`parts` carries three nullable columns materialized at ingest from the tool Part bodies: `tool_name`, `call_id` (also carrying the approval request's `tool_call_id` - the same correlation key), and `is_failure` (non-null only on tool results). NULL means the part is not a tool part or the source did not carry the field (`model-no-synthesis`). They exist because analytics must run on narrow native columns: a JSON getter over `variant_data` reads the whole multi-GB column, which on an object store cannot finish inside the query timeout. `variant_data` remains the verbatim record; the materialized columns are projections of it, never independently writable.

#### `session-additive-schema-backfill`

A schema change that adds derived nullable columns MUST upgrade existing data in place, never by re-ingest: on open, a store missing known derivable columns backfills them from data already stored (one `add_columns` commit per table; concurrent openers race benignly under OCC), and a `.pond` archive predating the change restores by deriving the missing cells at the read boundary - an archive is a snapshot and restore never mutates it. Why: re-ingest is not a migration path (`session-durable-copy` - a rotated source cannot supply its rows again), so an additive change that stranded existing stores or archives would violate the durability contract it sits on. The backfill derives only from stored data, preserving `model-no-synthesis`: a cell the stored record cannot justify stays NULL.

---

## 6. Adapters

An adapter is the codec between one client format and the canonical model. This section specifies the codec contract - both directions - and the seam that makes ingest's correctness rules compile-enforced rather than convention.

### 6.1 Bidirectional codec

Every adapter is a codec with two faces:

- *parse* - client format to canonical. This face is configured against a source (a directory, an HTTP endpoint) and streams canonical events.
- *serialize* - canonical to client format. This face is a pure function of a canonical session; it holds no source.

The two faces have genuinely different shapes - one source-configured and streaming, the other source-free - so an adapter is not a single object carrying both: the read face and the write face are separate.

### 6.2 Restore is hub-and-spoke

Serializing is restore. Any adapter can restore any stored session, because every session is in canonical form and the serialize face needs only canonical. A session need not return to the client that produced it.

#### `adapter-restore-distinct-reconstruction`

A restored file lands at a path the adapter chooses in its own layout (Section 10). Where the adapter replays a captured source file verbatim, restoring to that same path is the point: the file already there IS the transcript, so the no-overwrite refusal (Section 10) reads as "already resumed - open the file you have". A *reconstruction* is not that file - a foreign restore, or a native origin downgraded because the client cannot load its own captured format (`adapter-native-restore-lossless`, Section 6.3) - and MUST be named distinctly from the source file it was reconstructed away from, deterministically, so a second restore refuses against its own previous output. Why: reusing the source name makes every downgraded restore collide with the exact file the downgrade exists to route around, and the refusal then hands the caller back an artifact its client cannot open - the failure mode is silent, because collision is the same answer a successful earlier restore gives. Determinism is what keeps the second attempt an "already resumed" refusal rather than a second file.

#### `adapter-lineage-complete-restore`

Restoring a session MUST also restore its child sessions: the sessions that name it in `parent_session_id`. Why: a restored artifact must stand on its own in the target client - a Claude Code session that called the Task tool, restored without its subagent transcripts, is a set of dangling references rather than a working session. `parent_session_id` records a spawn or a fork (Section 4). The spawn graph is one level deep, capped structurally by the agent model - a Claude Code subagent cannot spawn subagents, and Managed Agents enforces a delegation depth of one. Multi-level fork lineage is deferred (Section 9); no v1 source emits it, so every stored graph is depth-one today. A graph found nesting deeper - a relaxed spawn cap, or fork lineage - MUST surface as a typed error, never a silent partial restore (`adapter-integrity-no-silent-drops`).

### 6.3 Origin and restore fidelity

Each session records the brand of the source that produced it (`Session.source_agent`), and each adapter has a matching origin identity. Restore fidelity is decided by the system, by comparing the two - never chosen by the adapter:

#### `adapter-native-restore-lossless`

Restoring a session with the adapter whose origin matches the session's origin is *native* restore and MUST be lossless (value-complete, per Section 1). Restoring with any other adapter is *foreign* restore: best-effort - a valid, idiomatic session in the target's own feature set, dropping whatever the target cannot express (the dropped content remains in canonical). A value truncated under `adapter-bounded-values` (Section 6) restores as its truncation sentinel, not its original bytes: a value the substrate physically cannot represent cannot round-trip, and the sentinel records the loss explicitly rather than hiding it. Why the system decides and not the adapter: native losslessness is a contract a caller relies on; leaving "am I native?" to the adapter would make it a convention.

### 6.4 The no-synthesis seam

The parse face builds canonical values only through a small set of extractor helpers that read one record of source data. The type holding a possibly-missing extracted value has no constructor reachable from adapter code - the helpers are its only producers - so an adapter physically cannot place a literal, a default, or a sentinel into a canonical field. This is what makes `model-no-synthesis` and `model-schema-honesty` (Section 4) compile errors rather than review rules. The serialize face needs no such seam: canonical is already trusted input.

#### `adapter-provenance-required`

The Part constructor reachable from adapter code MUST require a `provenance` value (`model-part-provenance`, Section 4.8); a Part with an unclassified or implicitly-defaulted provenance MUST NOT compile. Why: provenance cannot be defaulted to `conversational` without silently mislabelling harness machinery as conversation, and it cannot be added after the fact because only the parse of a specific source record carries the signal. Like `model-no-synthesis`, making the classification a structural obligation rather than a review rule is the only enforcement that holds - and it forces every future adapter to confront its own harness's injection patterns rather than inheriting a guess.

#### `adapter-transport-agnostic-seam`

The parse seam abstracts one record of source data behind a small set of value accessors and carries no assumption about where that record came from. Why: the same seam serves a file adapter today and an HTTP or stream adapter later, with no change to the seam.

#### `adapter-bounded-values`

Every value an adapter places into a text column passes through the seam's size bound: a value whose encoding exceeds the substrate's per-value limit is truncated in place to a marked sentinel recording the original byte count, with the rest of the record preserved intact. The bound is a property of the seam's extractor helpers - an adapter cannot emit an unbounded value any more than it can emit a synthesized one. Binary payloads stored as blobs are exempt; the limit is a property of the text-column representation, not of the data. Why: the storage substrate cannot represent a text value at or beyond a hard size, so an unbounded value is not a large row but a process abort - bounding at the seam turns it into an attributable, recoverable truncation.

### 6.5 Placement procedure

To satisfy `model-lossless-projection` (Section 4), an adapter places every field of every record it ingests by one of three rules:

1. Message content becomes typed Parts, each classified `conversational` or `injected` (`model-part-provenance`, Section 4.8). Where a source record fuses authored and harness-injected content in one span - a human turn wrapped in runtime context tags, an attachment-scaffolding prefix on a typed prompt - the adapter splits it at the exact byte boundary into separate, provenance-homogeneous Parts. The split is value-complete-lossless (Section 1.3): native restore reconcatenates the Parts in `ordinal` order.
2. Harness or runtime metadata goes into `options` - on the Message or Part the record maps to. This includes any field of a mapped record left over once its typed fields are taken.
3. A record that maps to no Message at all - a standalone log entry that is neither a conversational turn nor metadata on one - is carried whole: a system-role Message with empty `content` and the record's whole-record encoding in its `options`, kept in log order by the record's own timestamp. Its id follows `lance-deterministic-pk` (Section 3) and its timestamp the record's own value, or the session-anchor fallback that `model-no-synthesis` (Section 4) permits.

The third rule is the catch-all that makes losslessness reachable for any record - including record types that did not exist when the adapter was written.

### 6.6 Ingest order and integrity

#### `adapter-integrity`

The parse face's contract on output:

##### `adapter-integrity-event-ordering`

For each session: the Session first, then each Message immediately followed by its Parts in order, before the next Message. pond core computes a message's indexed text at the message boundary without buffering across messages - the transition off a Part stream is the signal the message is complete.

##### `adapter-integrity-no-silent-drops`

Malformed source input MUST surface as a typed error carrying the adapter and the location of the fault; never silently skipped. A silent drop is invisible data loss, a surfaced one is a fixable report.

##### `adapter-integrity-opaque-ids`

Identifiers on canonical objects are opaque strings. An adapter decodes any structure a source encodes into a path or name once, at ingest, and stores the decoded value; readers never re-parse.

##### `adapter-integrity-additive-sync`

A write MUST NOT overwrite a row already present under its primary key - matched rows are no-ops. Adapter output is monotone across versions: a newer adapter produces a superset of the rows a prior version produced. The source is not authoritative against pond's stored copy - a re-parse from a since-corrupted source must not be able to overwrite good data. Changing or removing an already-stored row is a deliberate migration, never a side effect of re-ingest.

##### `adapter-integrity-dedup`

An adapter SHOULD detect duplicate primary keys in its own output using the source format's own mechanism; the write path drops duplicates as a floor regardless. Catching them in the adapter keeps the count visible in the ingest summary, while the write-path floor keeps storage correct when an adapter misses one. A duplicate is two records that agree on primary key and content; two records sharing a source-supplied id but differing in content are not duplicates - dropping one is invisible data loss (`adapter-integrity-no-silent-drops`). An adapter that dedups on a source id alone MUST confirm content-identity before dropping, or distinguish the records by deriving a content-keyed primary key; a collision it cannot resolve surfaces as a visible drop, never a silent one.

### 6.7 The registry

Adapters are listed in one registry; adding an adapter is a new file plus one line in that list - there is no central enum or dispatch to edit, and no code generation. Why: a low, fixed cost per adapter is what keeps the adapter list open-ended.

### 6.8 Conformance

Each adapter has a round-trip codec test: parse a committed fixture to canonical, serialize it back native, and assert the result is value-equal to the fixture - this is what enforces `adapter-native-restore-lossless` and exercises `model-lossless-projection`. Foreign serialization is tested for validity in the target format and reviewed against a golden file.

### 6.9 Adapter set

The adapter set is intentionally not listed here. The registry in `packages/pond/src/adapter/mod.rs` is the source of truth for which formats a build supports, and Section 6 is the contract every registered adapter must satisfy. Per-adapter extraction detail - how an adapter resolves `project`, what its `source_agent` brand is, its on-disk layout, and which source records it deliberately does not ingest - lives in that adapter's own code, which is its documentation.

---

## 7. Protocol

The protocol is how requests reach pond and responses leave it, across both transports. HTTP and MCP are thin dispatchers over one shared set of handlers; the handlers know nothing of either transport.

### 7.1 Transport-agnostic handlers

Every operation is a handler function from a request value to a response value. The HTTP transport (axum) and the MCP transport (rmcp) each only decode their wire form into that request value and encode the response back - no operation logic lives in a transport, and a handler cannot tell which transport invoked it.

### 7.2 The request envelope

Every request carries `protocol_version` (a positive integer; v1 is `1`) and an optional `namespace`. Every request is associated with a server-generated request id for log correlation. HTTP exposes it via the `X-Pond-Request-Id` response header; MCP correlation uses the JSON-RPC envelope `id`. Schema evolution within a major version is additive only; removing or retyping a field is a major version bump. The precise wire schema is published as JSON Schema generated from the Rust types: this document specifies the contract, the generated schema is the exact artifact.

### 7.3 Namespace

`namespace` is an opaque tenant-routing string; omitted, it selects the personal pond's single namespace. It is distinct from the Lance namespace concept of Section 3 - the same word at two layers: the wire `namespace` selects a tenant, the Lance namespace is how the catalog seam locates that tenant's tables.

#### `wire-namespace-resolution`

Whether a request's namespace is acceptable, and which stored tables it maps to, MUST be decided in exactly one place. Why: hosted multi-tenancy turns one namespace into many; centralizing the decision makes that a single change, not an edit at every call site.

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
| `not_found` | a `pond_get_session` / `pond_get_message` target that does not exist | 404 | no |
| `namespace_unknown` | a namespace string not provisioned | 403 | no |
| `storage_unavailable` | a Lance or object-store failure after retry was exhausted | 503 | yes |
| `conflict` | optimistic-concurrency retry exhausted on a write | 409 | yes |
| `internal` | an unhandled fault | 500 | no |

Retryability is conveyed by the code; there is no separate field. `conflict` is the wire mapping of the substrate's conflict signal (Section 3).

### 7.5 Operations

1. **`pond_search`** (`POST /v1/search`) - search; Section 8 specifies retrieval. Returns ranked message hits grouped by session, with the top-scoring matches per session. Takes a `format`: `text` (the default - a rendered transcript of the ranked hits) or `json` (the same hits as structured data).
2. **`pond_get_session`** (`POST /v1/get-session`) - fetch a whole session as the conversational view (human/model text, with a compact `parts_summary` per message). Takes one `id`: a session id reads that session; a message id resolves up to its parent session with the page anchored at that message, and the response records the resolution (`resolved_from_message_id`) - intent comes from the operation, so upcasting is always safe. `from`: `start` (the default - oldest messages first) or `end` (the most recent messages, still in chronological order - e.g. recovering recent context after compaction). Pages are bounded by `limit` and a size budget and never cut mid-message; the caller pages on with `after_message_id` / `before_message_id` using the id a page marker shows. Not for bulk export - that is the restore/export path.
3. **`pond_get_message`** (`POST /v1/get-message`) - fetch one message with surrounding context: the target's full Parts (budget-bounded, `target_parts_remaining` signals the cut) plus `context_before` / `context_after` conversational sibling messages each side - siblings stay conversational so system/tool carriers do not crowd the conversation out of the window, while the target itself returns regardless of role. The reverse of the get_session resolution never happens: a session id cannot pick one message, so it is rejected with a hint naming `pond_get_session`.
4. **`pond_ingest`** (`POST /v1/ingest`) - accept a batch of canonical events. Always batched, bounded by an event count and a body-size cap. Events are grouped by session and applied per session; partial success across sessions is normal and reported per row.

Three resources - `schema://pond`, `schema://pond-sql`, and `stats://pond` - expose the search-field documentation, the SQL surface's table schemas, and dataset statistics. The read-only SQL query surface is not an HTTP operation: it is exposed as the `pond_sql` MCP tool (7.7) and the `pond sql` verb (7.8).

### 7.6 Ingest events

A `pond_ingest` event is one canonical object - a Session, a Message, or a Part - tagged with its kind. Within a session's substream the order is fixed (`adapter-integrity-event-ordering`, Section 6). `Session.source_agent` and `Session.project` are immutable after first write: a re-submitted session with a differing value for either is rejected for that row, since both are denormalized onto `messages`.

### 7.7 MCP surface

The MCP transport exposes the read operations - `pond_search`, `pond_get_session`, `pond_get_message`, and `pond_sql` - as tools, plus the resources. Ingest stays HTTP-and-CLI only. Why: MCP's role is read access for an agent; ingest is an operator action.

Tool metadata is a routing surface, designed for clients that defer tool descriptions behind tool search (Claude Code, Codex): the server `instructions` carry all routing (search finds, get reads - including analyze/review/summarize a session - and `pond_sql` is the self-demoting escape hatch), tool names alone must route correctly, descriptions lead with the verbs they claim, cookbook detail lives in the `schema://` resources, and error messages teach the correct next query at the point of failure.

#### `mcp-read-only-heal-exception`

`local-store-self-heal` (Section 3.3) is the single deliberate carve-out from the MCP surface's read-only rule: an open performed by the MCP server MAY execute heal's quarantine renames. This is substrate integrity restoration, not a write surface - rename-only, no user-data writes, no deletes, triggered only by an open that already failed - and the alternative is an MCP server that stays bricked on a crash-damaged store until an operator runs a CLI command. Every other MCP action remains read-only, hard-enforced; this exception is intentional and MUST NOT be read as precedent for MCP write paths.

### 7.8 CLI verbs

The same handlers back a set of command-line verbs. The storage destination, config file, and state directory are `global = true` selectors (`--storage-path` / `POND_STORAGE_PATH`, `--config-file` / `POND_CONFIG_FILE`, `--state-dir`), resolved at the root or after any subcommand alike; `--state-dir` is the argument form of `XDG_STATE_HOME` and must be absolute, exists because a Windows scheduled task has no environment block to pin one in, and is hidden from help everywhere since the scheduler writes it rather than a user typing it; `pond init` is the one exception - it ignores an env-sourced `--storage-path`, since writing ephemeral env state into config would be a silent surprise.

- `pond init` - idempotent setup-and-repair wizard: storage destination (with an end-to-end probe for remote URLs), adapter selection, MCP registration (one consent also installs the bundled agent skill - the same bytes `pond skill` prints - into Claude Code's user skills dir, kept in sync on re-runs; a hand-edited copy is never overwritten without an explicit confirm), and an opt-in sync schedule, written to `config.toml` in a single pass at the end. Every section is flag-answerable for non-interactive use: the destination via the global `--storage-path`, the rest via `--adapters`, `--every`, `--skip-mcp`, `--yes`, and `--force` (start from built-in defaults, ignoring existing config). `--yes` alone never schedules. The embedding model is deliberately not an init concern: overriding it is an advanced, re-embed-forcing act that lives only under `[embeddings]` in config. After the config write, an interactive init offers to run the first sync in the foreground (full progress UI) and registers the schedule only once that sync completes - a fresh systemd timer fires immediately on registration, and two concurrent syncs only slow each other down. An opted-in schedule survives Ctrl-C during that first sync: the interrupt path registers it on the way out, so "the next sync resumes where this one stopped" stays true unattended.
- `pond sync` - the additive ingest verb: import fresh sessions - each message embedded inline in its ingest commit - then fold the search indexes and run the `[maintenance]` compaction pass (amortizing the version-cleanup walk over several runs since it is round-trip-bound on object stores) so the lake is queryable on exit. A positional `<adapter>` syncs just that one enabled adapter; `--path <DIR>` (requires `<adapter>`) is a one-off path override that bypasses `[adapters.<adapter>]` and never writes config. Sync ingests only already-enabled `[adapters.*]`; it never discovers, enables, or writes adapter state - that is the explicit job of `pond adapters` and `pond init`. With no enabled adapters it does nothing but name the fix. Per host and store, sync is single-flight: a local flock in the state dir (never on the Lance store - cross-host writers stay pure OCC) makes a second sync wait, naming the holder; `--no-wait` skips instead (exit 0), which is what the scheduled run passes so ticks never queue. `--dry-run` prints the freshness gate's per-adapter verdict (sessions, fresh, pending) and writes nothing to the store (it may build the local freshness cache - the same one-time scan a real sync starts with, and what makes the preview accurate). `--format json` emits one machine-readable summary document on stdout for every outcome - ok, skipped, or error (progress stays on stderr). The embedding-model preload is best-effort: a caught-up sync embeds nothing, so an offline host with no cached weights still completes as a no-op - only a run that actually needs to embed fails. Every phase that can run long has a live face - the freshness-map build, the embedding-model download, the per-adapter import bar (with an ETA from the bar's recent-rate estimator, never a whole-run average an early fresh-skip burst would poison), and the inline-embed counter inside each commit - and off-TTY (cron, agents) the bar line is emitted as a plain heartbeat every ~30s, including through the embed/commit phase. A run that ingested rows re-extends the local freshness cache before exiting, so `pond status` pending counts are as-of sync end. Each run finishes by writing a per-host last-sync record (outcome, deltas, duration) to the state dir for `pond status`.
- `pond optimize` - the maintenance verb: embed the un-embedded backlog, then fold the text + semantic indexes (the index stage also runs the `[maintenance]` compaction / version-cleanup pass). `--only <stage>` runs exactly one stage; `--skip <stage>` omits one. Stages are `embed` and `index`. `--force-embed` re-embeds rows whose `embedding_model` differs from the current model via conditional merge. `pond sync` embeds inline at ingest, folds indexes, and compacts on every run (cleaning versions only periodically); `optimize` is the on-demand catch-up - backlog embed (when embedding was off at ingest) and model-swap re-embed - and cleans versions every run.
- `pond adapters list|discover|enable|disable` - manage which adapters `pond sync` ingests (the `[adapters.*]` entries) and nothing else. `list` shows configured adapters (enabled state, path) plus detected-but-unconfigured adapters; `discover` probes the machine and enables the interactive picks, skipping any entry that already names a source `path` so a customized or multi-path entry is never overwritten by the probed default (a pathless decline stub stays re-offerable, so declining an adapter does not dead-end the route back to it); `enable`/`disable` flip one by name, `enable` discovering a default path when the entry has none. Enabling an adapter is therefore always an explicit action - `pond sync` reads these entries but never writes them. An entry's `path` accepts a single dir or an array of dirs (one tool keeping several transcript trees, e.g. a relocated `CLAUDE_CONFIG_DIR` for a second subscription); an array fans out at resolution into one single-path pass per dir, so every listed dir rides the same sync and adapters only ever see a scalar `path`. Ingest idempotency under `lance-deterministic-pk` (Section 3) is what makes several dirs merging into one store safe. A malformed array fails the whole resolve rather than skipping just that entry - the same posture as any other bad config blob, which `factory.open` already refuses - and the error names the unaffected adapters and the scoped `pond sync <adapter>` that still works.
- `pond search [--explain]` - search from the command line. `--mode vector|fts` picks the retrieval arm and `--sort-by relevance|recency` the order. `--explain` returns Lance's `analyze_plan` output instead of results.
- `pond get-session` / `pond get-message` - the two get operations (7.5) from the command line: a whole session as a transcript, or one message with context.
- `pond sql` - one read-only SQL query over the corpus: a DataFusion-planned SELECT/WITH over the three session datasets, with the same two-layer read-only enforcement as the `pond_sql` tool (Section 7.7). Results render inline (row-capped) or export to parquet/ndjson.
- `pond status` - row counts, dataset statistics, embedding coverage, index health, and the sync-schedule state, plus this host's relationship to the store: what each enabled adapter sees on local disk, how many sessions are pending sync (classified against the locally cached freshness map - never a remote scan; unknown until the host's first sync), the last sync's outcome (including a surfaced FAILURE from a scheduled run), and the estimated next scheduled run. `--hosts` adds the fleet view of a shared store - sessions and latest activity per ingest-host stamp. It prints the cheap storage summary first, then completes the longer checks.
- `pond serve --transport http|stdio` - run HTTP by default, or MCP over stdio. `--with-sync` folds the periodic sync into the serving process (interval `--sync-every <minutes>`, default 5), so one process serves the read tools and keeps the store fresh with a single shared embedding model instead of a separate `pond sync` child cold-loading a second one. This is a process-topology consolidation only - "no daemon beyond `pond serve`" stays literally true and it does not activate live-write (Section 9.4): the in-serve loop is the same interval sync, takes the same per-host lock with `--no-wait` (a cycle that lands while another sync holds it skips cleanly), writes the same last-sync breadcrumb, and routes all its output to stderr/tracing so stdout stays reserved for the transport. A failed cycle is logged and never takes the serving half down. `--bootstrap <adapter>` is an operator-opt-in, init-equivalent step at serve startup for supervised deploys (a plugin-managed sidecar being the motivating case): only when no `[adapters.*]` entries exist at all does it discover and enable the named adapter - a disabled entry counts as configured and is never touched - and it completes before the sync loop spawns, so the sync-never-enables rule above is preserved verbatim. Discovery failure downgrades to a logged warning naming `pond init`; serve still starts and an empty store answers with degraded (fts-only) search.
- `pond mcp` - alias for `pond serve --transport stdio`.
- `pond schedule start|stop|status|logs` - manage the automatic `pond sync -q --no-wait` registration with the OS scheduler: launchd on macOS, systemd user timers (or a fenced crontab block) on Linux, Task Scheduler on Windows (the task Execs `pondw.exe`, a windowless launcher that runs the sync without flashing a console and exits with its status, so the task's `Last Result` is the sync's own; the task XML sets `DisallowStartIfOnBatteries=false`, `StopIfGoingOnBatteries=false`, and `StartWhenAvailable=true` for a launchd/systemd-equivalent posture - `Hidden` is set too but only affects the Task Scheduler UI listing). An unattended scheduled run can only ingest already-enabled adapters: `pond sync` never enables an adapter (that is `pond adapters` / `pond init`), so a background job can never grow the adapter set on its own; and it passes `--no-wait`, so a tick that lands while another sync holds the per-host lock skips cleanly instead of queueing. Registration pins the resolved `XDG_STATE_HOME` into the job's environment: the scheduler daemon never sources shell rc files, and a shell-only override would otherwise split the lock and last-sync record between the scheduled and manual syncs. On Windows the same invariant is carried by argument rather than environment - a Task Scheduler `Exec` action has no environment block, so the registration-time state dir is baked into the task's `--state-dir` (and the sync log path into the launcher's `--log`, an action having no `StandardOutPath` equivalent either); registration warns when `XDG_STATE_HOME` is set interactively, naming the pinned path the task will not follow away from.
- `pond config show|path|schema` - introspection for Section 3.9: `show` prints the resolved config with redacted values (`storage-redaction`), per-field source attribution along the precedence ladder, and the active URL's creds binding.
- `pond storage [check|use]` - the storage-destination cluster. `check <URL>` probes a destination end-to-end (parse, creds resolution, the conditional-put OCC primitive, write/read/delete) with a distinct exit code per failure class (formerly `pond config check`). `use <URL>` is switch-only: probe the destination end-to-end, then flip `[storage].path` - it copies nothing, so switching between distinct stores, or rolling back to a previous one, leaves every store's data untouched. The bare keyword `local` (synonym `default`) resolves to the platform default local data dir wherever a storage URL is accepted (`use`, `check`, `copy`, `--storage-path`), so a rollback needs no path memorized; a directory literally named `local` is reachable as `./local`. Where data lives and how big it is is `pond status`, not a storage subcommand.
- `pond creds <add|list|delete>` - manage the URL-scoped `[creds.*]` sets in config.toml: `add` captures a set (name defaulting to `default`, secret via a hidden prompt) and writes `[creds.<name>]`, `list` shows configured sets with secrets redacted, `delete` removes one. A CLI affordance over the 3.9 model - sets bind to URLs by scope match, never by activation, so there is no "current" set.
- `pond copy --from <url|file> --to <url|file> [--verify-only]` - move canonical data between stores, `.pond` archives, and the JSONL wire stream. Both `--from` and `--to` are required (`local` resolves to the local default dir, `@` to the configured store - the one every other command uses); each is sniffed by strict suffix: `*.pond` is a compact restorable archive (clean index-free Lance datasets plus a manifest; embeddings remain data columns), `*.jsonl` or `-` (stdio) is the JSONL wire stream, anything else is a pond store URL. Store-to-store is an incremental, idempotent union merge (`lance-deterministic-pk`), re-runnable, valid onto a populated destination, never deletes or modifies the source: it streams the source scan straight into the destination (no staging copy) and transfers only the sessions absent or grown on the destination. It picks the write primitive per session so the copy is bandwidth-bound, not commit-latency-bound on remote object stores: absent sessions cannot collide, so they are **appended** (no merge join, no target probe, one commit per table); the rare grown sessions take the same **append** path after a per-row pre-existence filter drops the rows already present (the shared ingest write path), so a grown session moves only its delta rows, one commit per table. Append-only storage is what makes the append safe - a re-run re-plans from current destination state, so an interrupted-then-resumed copy never double-appends (landed sessions are no longer absent). "Grown" is decided by a data-intrinsic per-session key, the message count - pond being append-only (a message row is never rewritten or deleted), it rises iff a session gained messages, and a count is source-authored so it survives the copy unchanged and compares soundly across two stores with independent clocks. A re-run after a small delta moves and indexes only that delta; an unchanged source plans an empty delta (0 sessions copied) and skips the index rebuild. On completion it folds the new fragments into the destination indexes. Completeness is guaranteed by `session-movement-complete` (Section 5.4): the closing composite-PK verify runs on every copy - exit 0 synced, exit 6 if the destination is missing source rows or holds duplicate rows (a write anomaly) - and `--verify-only` runs just that read-only check (store-to-store only), copying nothing. Archive/JSONL endpoints export from or restore into a store; JSONL is export-only. Distinct from `pond sync`: sync re-reads adapters, and a rotated or deleted source can no longer supply its rows (`session-durable-copy`), so `copy` is the only path that carries the durable corpus to a new backend. It is always an explicit step, separate from the `use` pointer switch.
- `pond erase <session-id>` - the single sanctioned deletion (`session-append-only-exception`, Section 5.4): operator-only true-byte-purge of a session and its child sessions, adding their keys to the resurrection denylist the ingest path consults. CLI and HTTP only, never on the MCP read surface. Names what it erased and what is now denylisted.
- `pond resume <session-id> --to <adapter>` - resume a stored session into a client's native format (invokes the adapter serialize face; fidelity per `adapter-native-restore-lossless`, Section 6.2). The session restores together with its child sessions or not at all (`adapter-lineage-complete-restore`); a graph nested deeper than that is a typed error, never a partial write. Fidelity is the system's decision, never the caller's: a target adapter whose origin matches the session's `source_agent` (exact or subpath) serves native - a value-complete replay of the stored source records - and every other pairing serves a best-effort idiomatic reconstruction. An adapter MAY also downgrade a matching origin when the native bytes are ones its client cannot load (a session captured from a newer on-disk format than the installed client reads): a byte-faithful file the client refuses to open is not a restore. Either way the fidelity actually served is reported per session, so a downgrade is visible rather than silent. `--out-dir` (default: the current directory) is the root the adapter's own relative layout is written under. Restore never overwrites and never deletes: the destination is a live client's data directory, so an existing target path fails the whole operation before the first byte is written, naming every collision - which is what lets a caller treat "already resumed" as "open the file you already have". Exit codes separate the cases a caller must branch on: not stored, unanswerable request (unknown adapter, too-deep lineage), collision, and a failed write - the last unwinds the files and directories it created (best-effort: no restored session file is left behind; a directory something else has since populated survives). `--format json` emits one document - session ids, fidelity, absolute paths, or the error - on stdout. Operator-only, mirroring ingest: CLI in v1, never on the MCP read surface; HTTP exposure is deferred.
- `pond completions <shell>` - emit shell completion scripts; release packages ship them pre-installed.
- `pond skill` - print the bundled SKILL.md - the agent-onboarding pointer - to stdout. It is emitted from the binary, not a separate copy, so it never drifts.

### 7.9 Versioning

The wire protocol versions through `protocol_version` and additive-only schema changes. The canonical model and the storage schema evolve additively too; the Lance manifest carries the storage schema version (`lance-dataset-schema-version`, Section 3). pond is pre-release: there are no compatibility shims, and a breaking change is a major version bump, not a migration layer.

---

## 8. Search and embeddings

Search returns messages, at message granularity. Each query runs one of two retrievers - a vector retriever or a keyword retriever - chosen by the caller per query; there is no server-side fusion. This section also specifies the embedding seam, a generic capability the session datasets consume rather than a part of them.

### 8.1 Single-arm retrieval

Two retrievers are available over the same corpus: a BM25 full-text retriever (`fts`) over each message's indexed text, and a vector retriever (`vector`, the default) over the message embeddings produced by the configured model. The caller picks one arm per query - there is no server-side fusion of the two. The vector arm ranks by cosine similarity with a gentle recency tiebreaker; the full-text arm ranks by raw BM25. Either arm scores by raw magnitude, never by rank position, so a hit's score does not shift with pool size or the caller's limit. The vector arm falls back to full-text when no message is embedded under the configured model. Results group to one summary per session, keyed on `session_root` by default - and on the individual message when the caller pinned one conversation via the session filter, where root keying would collapse the whole response to a single hit; a group's representative message is its highest-scoring one. Independently of the arm, a `recency` sort returns the in-scope messages strictly newest-first and labels the response as recency-sorted. Arm and plan attribution is operator-only and exposed via `pond search --explain`.

#### `search-prefilter-pushdown`

Every vector and full-text query MUST push its scalar filters into the table's scalar indexes before the retriever ranks, never as an in-memory post-filter. Why: a post-filter ranks first and filters second, so it silently returns fewer than the requested number of results and ignores the scalar indexes entirely - correctness depends on the filter running first.

### 8.2 Indexed text

`search_text` is the conversation: one text field per message, built at ingest by one pond-core function applied uniformly to every message - per-adapter customization is rejected so the search corpus has one predictable shape. It concatenates, in order, the text of TextParts and the metadata of FileParts that carry `provenance: conversational` (Section 4.7). It is null for system and tool messages, and for any message left with no conversational text - a bare tool call, or a message whose only content is harness-injected. Reasoning text, tool-call bodies, tool results, approval parts, and harness-injected parts are deliberately not indexed; they live in `parts` and reach the caller only via `pond_get_message` (full part bodies for a single message) or `pond_sql` over `parts`. Excluding injected parts is not per-adapter customization: the conversational-or-injected decision is made once at the adapter seam (`model-part-provenance`, Section 6) and recorded as `Part.provenance`, so this function reads a canonical field and stays uniform.

#### `search-language-neutral-index`

The full-text index MUST keep every language searchable: it MUST NOT apply a transform that drops or mangles another language's tokens. A monolingual stemmer is permitted only when it degrades gracefully - tokens it does not recognize pass through unchanged and stay exact-matchable - so no language is silently under-indexed. Why: pond ingests sessions in any language; a lossy monolingual transform under-indexes every other, but an additive one that leaves other languages exact-matchable does not. Pond indexes with the word-level `simple` tokenizer plus English stemming (ascii-folding on, stop-words and phrase-positions off). Word retrieval beats the former character-`ngram` (3-5) tokenizer ~2x on the real corpus (EN Success@3 66/111 vs 31/111) at a fraction of the index weight, and Ukrainian holds within one query of ngram (7/21 vs 8/21) because Cyrillic passes through the English stemmer unstemmed and still matches exactly (rationale and experiment: `docs/researches/tokenizer-experiment-report.md`).

### 8.3 Filters and ranking

A search accepts filters on project, session, source agent, and a time range, plus a minimum score. The source-agent filter is exact-or-subpath: the value itself plus its `/`-delimited subpaths (`openclaw` also matches `openclaw/subagent`, but never a sibling brand like `openclaw-x`), so naming a harness reaches its derived kinds and naming a subpath targets exactly that kind. A root value's subpath arm is still subject to the default subagent exclusion below - `openclaw` returns the harness's main sessions unless `include_subagents` opts in - while naming a subpath is itself the deliberate opt-in for that kind. Results are grouped to one summary per session, with up to a small fixed number of top-scoring matches per session (the cap lives in code); a session-scoped search returns one session, so the cap widens to the requested limit there. Filter columns are denormalized onto the searched tables (Section 5) so every filter pushes down without a cross-table join. By default a search excludes subagent (child) sessions - those whose `source_agent` carries a `/`-delimited subpath - because a search wants the human-facing main sessions; an `include_subagents` flag opts back in, and an explicit session filter or a source-agent filter naming a subpath disables the default exclusion (the caller is already scoping to a subagent deliberately). A root source-agent value does not: it stays main-sessions-only, matching the un-scoped default.

#### `search-absence-honesty`

Every search response MUST report how many searchable messages the caller's filters left in scope, including (especially) when that count is zero. Why: retrieval always fills its top-k from whatever scope it gets, so without the scope size the caller cannot distinguish "nothing relevant exists" from "my filters excluded everything" - measured agent behavior converts the first reading into false "this was never discussed" conclusions. Scores carry no absence signal (present and absent content score in overlapping bands; see `docs/researches/embeddings.md`), so the scope count is the one honest cue the response can give.

### 8.4 Hit payload

A search hit carries enough of the matched message to judge relevance without a second fetch: the message's indexed text in full when it is small, and when it is large a bounded prefix of that text plus a match-windowed snippet drawn around the query terms. The size bounds are tuning constants and live in the code, not this document. A user-role hit additionally carries a compact `parts_summary` (the same per-Part descriptor the get operations return), so a prompt that attached files is distinguishable from a plain-text one without a second fetch; other roles omit it. The full message - including the parts excluded from the indexed text - remains available through `pond_get_message`.

### 8.5 The embedding seam

Turning text into vectors is a generic capability, not a session concept. It sits behind one seam - a backend interface that takes text and returns vectors - so a local model today and a remote provider later are the same shape to everything above. The engine ships a fixed set of models it has loaders for; configuration selects one and supplies its vector parameters. No model is mandatory and none is named in this document - the choice and its default are configuration.

### 8.6 Producing embeddings

Embeddings are derived, not source data, but are produced inline at ingest: when embedding is enabled the ingest path embeds each message from its just-computed `search_text` and writes `vector` and `embedding_model` in the same append commit (`session-embed-from-canonical`, Section 5.5 - the derivation source stays canonical, only the timing is at ingest). `pond optimize`'s embed stage walks the leftover backlog - rows whose `vector` is null (embedding was off at ingest) or whose `embedding_model` is not the configured model (a model swap) - through the same seam. The seam is generic: a future consumer that wants vectors reuses it over its own table.

### 8.7 Opt-in

Embedding is opt-in by configuration. With it off, `pond serve`, `pond mcp`, and `pond search` run full-text only and never load a model. With it on and at least one message embedded under the configured model, the `vector` arm is available and is the default; otherwise every query runs full-text.

### 8.8 Index lifecycle

Vector and full-text columns exist from table creation; turning embeddings on or off never needs a schema migration. Index maintenance follows `lance-index-maintenance` (Section 3.7). Vector search uses brute-force flat scan below the activation threshold (currently 100,000 non-null vectors); above it the trained IVF_SQ takes over. Partitions = `num_rows // 4096`; `[search].nprobes` is operator-tunable for recall.

---

## 9. Deferred

These are scoped out of v1. None requires a schema migration or a cross-cutting change when it activates - the v1 design forecloses none of them.

1. **Future consumers.** The storage substrate (Section 3) is generic; the session datasets are its first consumer, and these are the next. Each is a separate consumer with its own canonical model and its own tables on the same substrate - not an extension of sessions.
   - **Resources and blobs** - per-namespace knowledge-base files.
   - **Social and web content archives** - exports from Telegram, Discord, Twitter, Reddit, GitHub. These are flat-message content, not LLM conversations, so they get their own canonical model rather than being coerced into Session/Message/Part.
   - **A file and blob store shaped like the Files API** - upload, reference, download.
   - **A versioned-document store shaped like agent memory stores** - small text documents with an immutable version history. The substrate's manifest versioning aligns naturally with this.
2. **Future adapters.** A new adapter adds no substrate or schema change - it is a new file and a registry line (Section 6).
   - **A Managed Agents adapter**, including multi-agent sessions: a coordinator and its delegated agent threads map onto linked Sessions through `parent_session_id` and `parent_message_id`. The spawn case already works for v1 sources (Claude Code subagents ingest as linked child Sessions); the Managed Agents adapter is the next step.
   - **Other clients** - additional agentic clients such as Cursor, aider, Gemini CLI, and more.
3. **Provider-target restore.** Restoring a canonical session into a provider API request shape (Anthropic, OpenAI, Bedrock, Gemini), as opposed to a harness session-log format. Always foreign, and additionally constrained to produce API-valid output.
4. **Live-write.** Ingesting events as a session runs, rather than after it ends. The activating design is micro-batched single-writer ingest: a long-running process buffers live events and flushes one merge-insert per interval or size threshold. It needs no new substrate - the existing write chokepoint and OCC (Section 3) - so it is identical on local and object stores; the write problem on object storage is fragment/version churn, not latency, so the batch is the point. In-flight events not yet flushed are durable only via their source (a re-sync replays them, `session-movement-complete`), so pond is a mirror unless a store-of-record backstops it. MemWAL (a per-shard write-ahead layer, a scanner merging in-memory and on-disk generations, a sharded writer) stays the deferred escalation for many-independent-writers contention; the forward-compatibility seams of Section 3 keep its later activation a substrate swap, not a rewrite. Adopt MemWAL only when hosted multi-writer ingest exists (the former Lance-version condition is satisfied at the 8.0.0 pin).
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
   10. BFloat16 embedding storage - swap Float16 for BFloat16 once Lance's IVF_SQ build path accepts the `lance.bfloat16` extension (today it rejects `FixedSizeBinary(2)` at `infer_vector_element_type`). Same 2 bytes per element; wider dynamic range, lower precision than Float16.
   11. Blob v2 part storage - swap legacy `LargeBinary + lance-encoding:blob=true` for the `lance.blob.v2` Struct extension (and bump `data_storage_version` V2.1 -> V2.2). Unblocked at the 8.0.0 pin: the compact blocker measured at v7.0.0-beta.16 (`compact_files` errored with "there were more fields in the schema than provided column indices / infos" through the generic struct branch) is gone - re-verified 2026-08-12 by compacting a 2.2 dataset whose blob.v2 payloads covered inline, packed-sidecar, dedicated-file, and null shapes, all round-tripping byte-exact; v8 ships a dedicated blob-v2 compaction path with its own test suite. Same one-payload-per-row model; blob v2 adds the sidecar-file optimization for large external blobs and a separate `uri` Arrow sub-field (today the URL string is stored as UTF-8 bytes in the `data` column with the variant tag in `variant_data.data_kind`).
   12. Candidate-pool reranker - optional cross-encoder pass over the retriever's candidate pool before final ranking, behind the Section 8 search seam.
7. **Open questions.** Undecided: what event first activates the multi-tenant router; what use case first activates live-write; which catalog backend the hosted tier uses.

---

## 10. References

External work that informed this design. These are inspiration and corroboration; the contract is Sections 1 through 9.

- [Scaling Managed Agents](https://www.anthropic.com/engineering/managed-agents) - Anthropic Engineering. The session-as-append-only-event-log framing, and the meta-harness idea of modeling the stable conversational layer while pushing volatile harness behavior outward - the shape the canonical model and the `options` bag follow.
- [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) - Anthropic Engineering. On curating what enters an agent's context window; background for why a durable, searchable store of session history is worth building.
- [Context Rot](https://www.trychroma.com/research/context-rot) - Chroma research. On the degradation of model performance as input context grows - the same motivation, seen from the retrieval-quality side.
- [Recursive Language Models](https://arxiv.org/html/2512.24601v3) - arXiv 2512.24601. Treats long context as an external, queryable environment and recursion as sub-agent spawning; a recursive run captures as linked Sessions, which corroborated the branching model of Section 4.
