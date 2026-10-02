---
title: Specification
description: The pond v1 contract - storage substrate, canonical Session / Message / Part model, adapters, protocol, search and embeddings.
---

# Pond - Specification v1

pond ingests, stores, searches, and restores agentic-client sessions. This document specifies pond v1.

---

## 1. Overview

### 1.1 What pond is

pond ingests agentic-client sessions (Claude Code, Codex, others) into one canonical form in Lance and serves message-granular search (full-text, plus semantic when enabled, Section 8.7; one arm per query, Section 8.1). One static binary serves HTTP+JSON and MCP over shared handlers, in two deployments (Section 2.2).

### 1.2 The interchange-hub model

The canonical Session / Message / Part schema is a format-neutral interlingua, not merely a storage format. Every adapter is a bidirectional codec, so any adapter can restore any session - not only the producing client's. This schema's richness and stability is pond's product; the rest is machinery.

### 1.3 "Lossless" means value-complete

*Lossless* means every value round-trips as an equal value, not identical bytes: restore rederives from canonical, so incidental encoding (whitespace, key order, number forms) is not data. Restore by the producing adapter is lossless in this sense.

### 1.4 Preservation over convenience

When faithful preservation conflicts with convenience (readability, size, a tidier schema), preservation wins - pond is first a lossless record.

### 1.5 The stack

- Rust, tokio, axum (HTTP), rmcp (MCP).
- Lance via `lance-format/lance` directly - no `lancedb`, no wrapper.
- Local, S3, GCS, and Azure stores, all through Lance.
- JSON wire, one schema, versioned additively.

### 1.6 The shape

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

Formats parse into one canonical model any adapter serializes back out (provider APIs deferred), persisted in the generic Lance substrate that search and get read; session datasets are its first consumer (Section 9).

### 1.7 How to read this document

1. Sections 3-8 go foundation-first; the substrate (3) never references sessions, keeping it generic.
2. MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY follow RFC 2119 and RFC 8174.
3. Rules have stable topic-prefixed ids, cited in code as `spec.md#<rule-id>` and checked in CI by `ops/scripts/check-spec-refs.sh`.
4. Each rule states its constraint and why (the reason is contract, so the rule is not "simplified away"); implementation specifics live in code.

---

## 2. Scope

### 2.1 What v1 ships

1. **One application: sessions** - lossless ingest, storage, and search; the first substrate consumer (Section 3), others in Section 9.
2. **An open adapter registry** - defined in code, not enumerated or capped here; every adapter is a bidirectional codec (Sections 1.2, 6).
3. **Two transports** - HTTP+JSON (primary) and MCP over one handler set.
4. **Two deployments** - personal and hosted (2.2).

### 2.2 Deployments

- **Personal.** One binary, one local Lance directory, one hardcoded namespace owned by the operator; single-user, localhost-bound by default, XDG config and data paths.
- **Hosted.** The same binary against an object-store URL; each tenant is an opaque integrator-supplied `namespace`, and the integrator owns identity, access, and routing.

### 2.3 Non-goals

Stable positions, not deferrals (Section 9). pond will not:

- **Reinvent Lance** - storage, indexing, schema evolution, OCC, blobs, versioning, and time-travel are Lance, used directly.
- **Invent a wire format** - canonical types are pond's own serde structs (Section 4), with no upstream format to track.
- **Authenticate, authorize, or model identity or tenancy** - the integrator gates namespaces before any call; `namespace` is an opaque routing string; on hosted deployments bucket IAM is the storage boundary and the integrator's gateway the application boundary.
- **Encrypt at the application layer** - bucket SSE plus filesystem encryption; pond holds no keys and is not zero-knowledge (bucket-plus-key access reads everything).
- **Act as a runtime** - no tool execution, agent loop, context compaction, rendering, or telemetry.
- **Become a SQL database, UI, or sidecar daemon** - search and filter (Section 8) is the primary query surface; one read-only SQL escape hatch exists (`protocol-sql-read-only`, Section 7.5), never a write path or a second storage engine (the only engine is embedded Lance); no UI, no daemon beyond `pond serve`.

### 2.4 Platform

Linux, macOS, Windows (x64). The Windows binary is a native `x86_64-pc-windows-msvc` build, VC-runtime-static, verified by native Windows CI running the full suite on every PR and release (source builds also need system `protoc` and NASM); there the scheduler uses Task Scheduler, `local-store-durability` applies in its file-bytes-only form, and embeddings (Section 8.7) run on CPU (Metal is macOS-only, CUDA a Linux opt-in).

---

## 3. Storage substrate

### 3.1 Purpose

The substrate owns pond's Lance use and knows nothing of sessions; consumers (Sections 5, 9) declare tables and do all I/O through it. It guarantees durable append-only storage, safe concurrent writers, fault retry, and bounded read staleness; column meaning, indexes, and denormalization are the consumer's.

### 3.2 Lance chokepoints

#### `lance-chokepoints`

Every Lance interaction MUST go through one of four single-code-path chokepoints:

##### `lance-chokepoints-catalog`

Dataset opens MUST resolve location through one catalog lookup, never a constructed path. Why: a hosted catalog then swaps in by configuration.

##### `lance-chokepoints-read`

Scans and searches MUST be built via the substrate read path. Why: the one place `search-prefilter-pushdown` is enforced and scanner changes land.

##### `lance-chokepoints-write`

Writes MUST use the substrate write path - merge-insert where rows may collide, append for absent rows - which never folds indexes (`lance-index-maintenance`). Why: `lance-append-only` and `adapter-integrity-additive-sync` need one write chokepoint.

##### `lance-chokepoints-storage`

Dataset bytes MUST be read, listed, and written only via Lance's object-store layer, never a local path. Why: one direct-FS access silently backend-locks that operation.

### 3.3 Data integrity

#### `lance-append-only`

Stored rows MUST NOT be mutated; updates are new rows or manifest versions. Canonical values are immutable (sole exception `session-append-only-exception`); derived columns (`vector`, `embedding_model`, backfilled analytics) MAY be rewritten - a new manifest version never licenses changing a canonical value. Why: no corruption-by-mutation; retries stay idempotent.

#### `lance-deterministic-pk`

Every row MUST have a deterministic PK (source id where stable, else content-derived), and writes are idempotent on it. Why: idempotent ingest needs a key reproducible from source data alone.

#### `lance-dataset-schema-version`

Schema version lives in the manifest and a dataset metadata key, never a per-row column. Why: a per-dataset fact should not cost per-row storage.

#### `local-store-durability`

Local-filesystem writes MUST be durable before returning - bytes plus parent directory on unix, bytes only on Windows (`local-store-self-heal` covers the gap) - via Lance's object-store wrapping seam (`lance-chokepoints-storage` holds). Why: `LocalFileSystem` and NTFS publish a file's name without its bytes, so a hard stop can leave a zero-byte head manifest that poisons the table; the seam orders each artifact before the commit naming it.

#### `local-store-self-heal`

On a failed local open the substrate MUST self-heal first: pick the newest `_versions/` entry whose manifest loads AND all-column scan completes (crashes can zero referenced data; added columns sit in files narrow scans skip); rename unreadable manifests above it to `<name>.manifest.corrupt` (atomic, same directory, never delete); retry once; loudly name the quarantined files, the rolled-back version, and that rolled-back rows re-ingest from sources still holding them (`session-movement-complete`; rotated sources cannot, `session-durable-copy`). If nothing is readable or the failure is not manifest-shaped, heal MUST touch nothing and return the original error plus what was inspected and the recovery step. Why on open: the damage is a crash remnant and every surface (CLI, HTTP, MCP) opens here, so heal needs no human; heal restores openability and keeps the evidence, while recovering the rolled-back rows depends on their sources.

### 3.4 Dataset parameters

Tables use the current stable Lance format, constant-time latest-manifest lookup, and short manifest retention; a consumer's datasets share one Lance cache and object-store client (no per-table connections or credential refreshes).

#### `lance-manifest-retention-floor`

Retention MUST exceed the longest single read, floor one hour. Why: readers pin a manifest version per request, and cleaning a pinned version breaks in-flight object-store reads. Beyond it, recovery is `pond copy` snapshots; Lance tags are deferred (Section 9).

#### `lance-table-creation`

Every table MUST be created with:

##### `lance-table-creation-stable-row-ids`

Stable row ids. Why: otherwise every compaction rewrites every index.

##### `lance-table-creation-unenforced-pk`

An unenforced PK. Why: merge-insert defaults to it without per-call wiring, and the 3.8 seams attach to it.

##### `lance-table-creation-session-scoped-pk`

`session_id` leading the PK of every table below `sessions`. Why: source ids are unique only per session, and lineage operations (sub-agent spawn, `/compact`, resume, fork) replay a parent's ids into new sessions, colliding without `session_id` (undetected under an unenforced PK); it also keeps source ids verbatim and satisfies `lance-forward-compat-shardable`.

#### `lance-compaction-filter`

Automatic compaction runs only Lance-planned tasks that pass:

1. Tasks above the deletion-materialization threshold always run (tombstone reclaim pays regardless).
2. Others MUST strictly cut fragment count under BOTH writer caps - byte budget (outputs = total bytes / budget, rounded up) and row target (outputs = LIVE rows / target, rounded down, at least one); when the task's total bytes exceed one output's budget, every input's physical row width MUST let an output reach the row target within half the budget.
3. A task MUST earn its write amplification: the volume beside its largest fragment, and the volume beside the sum of its settled fragments (each already holding at least half an output by either cap), MUST each reach a fixed fraction of it; an all-settled task uses the largest alone.
4. A task spanning at least the fragment-count cap skips the width test of (2) and all of (3) once it can cut the fragment count.
5. Missing file sizes fail closed.
6. Rewrites re-encode, never binary-copy.

Why: (2) row-selected input against byte-split output loops forever (~30 GB over 31 syncs; ~80 GiB/day under byte-only prediction); (3) tiny appends must not ride full copies of settled peers (size-tiered amortization); (4) bounds manifest growth; (6) binary copies keep tiny pages whose metadata every take reads (11,300 GETs vs 15). See substrate.rs `task_veto_reason`, docs/benchmarks/results.md.

### 3.5 Concurrency

pond processes are stateless; concurrent writers resolve via Lance OCC on manifest versions (conditional writes on object stores, Lance's commit lock locally) - no external coordinator, no in-process queue.

#### `lance-retry-jitter`

Every Lance call MUST retry with bounded exponential backoff and jitter. Why: transient faults and lost races are routine. Exception: a non-idempotent batch append (no row-level PK dedup) MUST retry only on commit conflict (a fault between commit and ack would duplicate rows); other faults surface and the next run re-plans.

#### `lance-handle-freshness`

A cached handle MUST be freshness-checked before a read and refreshed past the backend-keyed staleness window (zero locally, seconds on object stores). Why: a long-lived server owns the commit-to-visibility window; it must be explicit and bounded.

### 3.6 The conflict contract

Exhausted write retry raises a typed conflict signal (with attempt count) that the wire layer maps to the retryable `conflict` code; the substrate knows nothing of wire errors.

### 3.7 Index lifecycle

#### `lance-index-maintenance`

Writes never fold indexes; operator-triggered maintenance does, via `pond optimize`'s index stage (by default ending `pond sync`; Section 7.8). A trailing index stays correct: Lance flat-scans unindexed fragments, only slower. All families fold via `optimize_indices(append)` (IVF_SQ under stable row ids), except two full recreates: a ZoneMap whose fragment ids compaction orphaned (a hard error on date filters), probed payload-vs-live after compaction so same-run damage heals; and an FTS index whose `pond.fts.stems` stamp (canary words stemmed by the building binary) differs from or predates the running binary's, recreated before any fold appends mismatched stems, since a stemmer swap silently breaks whole-word matching - until then `pond status` shows `stemmer outdated` and each process's first FTS query warns. Indexes on columns no write touched are skipped (sound: Lance prunes coverage only on overlapping fields); which exist follows Section 8.7. Why operator-triggered: per-write rebuilds would dominate write cost.

### 3.8 Forward-compatibility seams

#### `lance-forward-compat`

Three near-free v1 rules keep horizontal scale (Section 9) a substrate swap, not a rewrite:

##### `lance-forward-compat-shardable`

A high-volume table's first PK column MUST be coarse enough to shard on. Why: shard specs attach to an existing column; otherwise sharding means a PK redesign and full migration.

##### `lance-forward-compat-no-subsecond-freshness`

No operation MAY promise millisecond read-after-write; the floor is the `lance-handle-freshness` window. Why: a write-ahead layer makes writes visible only after an async merge, which such a promise would forbid.

##### `lance-forward-compat-no-cross-shard-atomic-write`

No write batch MAY span more than one PK family atomically. Why: each family maps to one shard, so cross-shard atomicity is unavailable.

### 3.9 Storage addresses and credentials

Addresses are URLs; credentials are URL-scoped sets. A named-storage registry with active-storage state was rejected: an address is static, not per-invocation state.

#### `storage-url-grammar`

A destination is one URL:

- Forms: local (`/abs`, `~/`, `file://`), `s3://bucket/prefix`, `s3+https://host/bucket/prefix`, `s3+http://host:port/bucket/prefix`, `gs://bucket/prefix`, `az://account/container/prefix`, test-only `memory://` / `shared-memory://`.
- `s3+` embeds the endpoint (no desync from the bucket); TLS and `allow_http` follow the scheme, never config.
- No region needed: AWS resolves it in Lance; `s3+` gets a fixed default (SigV4 region is ignored there), overridable by set field or `?region=`.
- Userinfo MUST be rejected at parse (argv, history, logs leak alike).
- Known params (`creds`, `region`, `virtual_hosted_style_request`) are stripped before Lance and beat the set's fields; unknown params are a hard error.

#### `creds-scope-match`

A set binds by match, never activation: `?creds=<name>` (missing = error) > longest matching `scope` > the single scope-less catch-all > none. Scopes compare canonicalized (lowercase scheme/host, no default ports), match only at `/` boundaries (`.../pond` misses `.../pond-2`), never across schemes; duplicates are a parse error. A set matching no URL in a remote-touching run is named in a warning. Why: no extra syntax for multi-storage commands, and rotation stays out of cron/MCP argv (git/DuckDB model).

#### `storage-configless`

Every command MUST work with no config file: URLs plus env are complete, and an unmatched URL gets no credential options, leaving the ambient SDK chain (instance profiles, task roles, OIDC, `aws sso login`). Why: disaster recovery runs where no file exists; containers and CI need zero config.

#### `storage-env-mirror`

Env mirrors config: `storage.path <-> POND_STORAGE_PATH`, `creds.<name>.<field> <-> POND_CREDS_<NAME>_<FIELD>`, merged per field over file sets; `extra` has no env form. Set names MUST match `[a-z][a-z0-9]{0,15}` (fields contain underscores; this keeps env names splittable). Precedence: flag > `POND_*` env > file > ambient chain > defaults.

#### `storage-redaction`

Secrets MUST NOT appear in URLs or CLI flags - only config, env, `<field>_file`, or `<field>_command` (output cached per process, one trailing newline stripped). Introspection redacts fields whose names contain `key`/`secret`/`token`/`password` (including `extra`) but prints `_file`/`_command` literally - the path or command is safe.

---

## 4. Canonical model

### 4.1 Shape

The interlingua (1.2), independent of storage (Section 5) and transport (Section 7): a Session contains Messages, a Message contains Parts; embeddings are derived storage, not canonical. It is deliberately LLM-conversation-shaped (roles, turns, tool calls, reasoning) below any harness; harness behavior (compaction, retries, step accounting, editor context) goes to `options`, never canonical fields - hence a flat social-content corpus is a separate consumer (Section 9).

### 4.2 Canonical is the source of truth

Stored canonical is authoritative - not derived, no second "raw" copy. Why: a raw store plus re-derivation would displace canonical as the contract; `model-lossless-projection` guarantees completeness instead.

### 4.3 Conventions

- Field names and discriminator values are `snake_case`.
- `SessionID`, `MessageID`, `PartID` are branded string scalars, plain strings on the wire; source-supplied where stable, generated otherwise.
- Timestamps are RFC 3339 on the wire, microsecond integers in storage; canonical timestamps are source-recorded, never pond's ingest time.
- `options` sits on every object: `options.<provider>.*` provider extensions, `options.source.*` source and harness facts, `options.pond.*` pond-operational facts.

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

Branching exists only between sessions; a session is a linear message log. `parent_session_id` records a spawn, fork, or continuation (sub-agent, fork, compaction successor); `parent_message_id` adds the parent cut-point for a fork-with-cut-point. A rotation that starts fresh context without carrying history is NOT lineage - recording one would assert a relationship the source does not state (`model-no-synthesis`). `parent_session_id` is soft: the parent need not be ingested first (adapter runs land in any order). Message ids are unique only per session (`lance-table-creation-session-scoped-pk`), so `parent_message_id` is never resolved alone.

#### `model-parent-pointer-coherence`

A `parent_message_id` MUST NOT be present without a `parent_session_id`. Why: a cut-point with no parent is incoherent; the validator rejects it.

#### `model-project-non-empty`

`Session.project` MUST be non-empty and extracted from real source data. Why: it is the attribution scope every filter and grouping uses; an adapter that cannot resolve it drops the session rather than inventing one.

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

Per-role content allowlists are type-level: a tool-result Part in a user message is a category error. SystemMessage content is a plain string; it MAY be empty as a placement-rule-3 carrier (Section 6.5), recording absence, not synthesis. Messages form an append-only log ordered by `(timestamp, id)`; the tiebreaker MUST be source-intrinsic (id or preserved source position), never a write-time counter, which would differ across re-ingests and break idempotency (`lance-deterministic-pk`). Turn metadata (model, token usage, finish reason, error) is not canonical; adapters route it to `options.<provider>.*`.

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

`id`, `session_id`, `message_id`, and `provenance` are pond-additive: Parts are addressable rows with back-references. `provenance` is orthogonal to `type` and `role`. FilePart payloads use the blob mechanism (Section 5).

### 4.8 Honesty of the model

Five rules, enforced by the adapter seam (Section 6) or core ingest, not by convention.

#### `model-no-synthesis`

An adapter MUST NOT substitute a sentinel, default, or placeholder for source data it could not find; a maybe-absent field is an optional sealed value produced only by the Section 6 extractor helpers, never from a literal in adapter code. Transport or absence defaults are not synthesis (timestamp falling back to the session anchor, failure flag false, generic MIME type). Why: a synthesized value is indistinguishable downstream from a real one (silent corruption), and only a compile error, not code review, reliably prevents it.

#### `model-schema-honesty`

A non-optional canonical field claims every adapter can always extract it from real source data; if any cannot, the field MUST become optional and the adapter MUST NOT invent a value. Why: optionality tells the truth about what sources carry.

#### `model-lossless-projection`

Every field of every ingested source record MUST be recoverable from stored canonical - a typed field, a Part, or `options`; an adapter MUST NOT store a proper subset. The only permitted non-capture is a source deliberately not ingested at all, which MUST be stated in the adapter's documented contract. An oversized value becomes a truncation sentinel with its byte count (`adapter-bounded-values`) - marked and attributable per `adapter-integrity-no-silent-drops`, never omitted. Why: with `model-no-synthesis` (no inventing), this (no dropping) makes the stored session complete and honest; Section 6 gives placement.

#### `model-part-provenance`

Every Part MUST be `conversational` (authored by the user or generated by the model in the exchange) or `injected` (inserted by runtime or harness: environment context, memory or rules injection, system reminders, task notifications, command echoes, tool output), and provenance-homogeneous - a span fusing both is split (Section 6.5). Why: injected content fills a conversational slot and looks like a real turn, yet search MUST exclude it while restore MUST preserve it, and `role` records the slot, not the author. Only a source's adapter knows its injection patterns, so the seam compels classification per adapter (`adapter-provenance-required`); the enum extends additively, finer kinds living in `options` meanwhile.

#### `model-pond-options`

`options.pond` is pond-owned: adapters and wire clients MUST NOT populate it, and core ingest strips and restamps it on every incoming Message, so no ingest surface can spoof it. It holds the ingest host stamp `{"ingest": {"host": {"username", "hostname", "device_name"}}}` (the inserting process's host, resolved once per process) - an audit fact, not identity, tenancy, or authorization; unresolved fields, or the whole stamp, are omitted, never synthesized. Only Messages are stamped: Parts share their Message's substream, and a Session stamp would duplicate its first message's and invite an identity reading, though sessions span hosts. Stamping is insert-only (matched rows are merge-insert no-ops), so re-ingest never restamps; backfill would be an explicit command.

---

## 5. Session datasets

### 5.1 Three datasets

The sessions consumer registers `sessions`, `messages`, and `parts`, each its canonical type plus named derived columns - the embedding on `messages` (5.5), tool-identity columns on `parts` (5.6). Nothing else is projected or promoted as of the current schema version; derived nullable columns extend additively under `session-additive-schema-backfill`.

`sessions` - one row per Session:

| Column | Notes |
|---|---|
| `id` | primary key |
| `parent_session_id`, `parent_message_id` | nullable fork pointers |
| `source_agent` | indexed copy on `messages` (5.3) |
| `created_at` | source-recorded |
| `project` | copied to `messages` (5.3) |
| `options` | JSON (`pa.json_()`, JSONB) |

`messages` - one row per Message:

| Column | Notes |
|---|---|
| `session_id`, `id` | composite primary key; clustered on `(session_id, timestamp)` |
| `timestamp` | ordering key; zonemap-indexed |
| `role` | message role |
| `source_agent` | denormalized; scalar-indexed |
| `project` | denormalized |
| `content` | system messages only |
| `search_text` | retrieval text (Section 8); FTS-indexed |
| `vector` | Float16 embedding of `search_text` (5.5); null until embedded |
| `embedding_model` | producer of `vector`; set with it |
| `options` | JSON (`pa.json_()`, JSONB) |

`parts` - one row per Part:

| Column | Notes |
|---|---|
| `session_id`, `message_id`, `id` | composite primary key; clustered on `(session_id, message_id)` |
| `ordinal` | position in the message |
| `type` | Part discriminator |
| `provenance` | conversational vs injected; search excludes injected |
| `tool_name` | derived (5.6); scalar-indexed |
| `call_id` | derived (5.6) |
| `is_failure` | derived (5.6); `tool_result` only |
| `variant_data` | JSON; variant-specific fields |
| `data` | Lance blob; FilePart payload |
| `options` | JSON (`pa.json_()`, JSONB) |

### 5.2 Composite keys

`messages` and `parts` keys lead with `session_id` (`lance-table-creation-session-scoped-pk`); clustering on `(session_id, ...)` keeps a session contiguous on disk for sequential reads.

### 5.3 Denormalization

Core copies `source_agent` and `project` onto `messages` at ingest - immutable, solely filter-pushdown surfaces; `sessions` stays authoritative outside search. Why: search filters and ranks in one pass over `messages`, and pond's Lance crates have no join planner.

### 5.4 Durability

#### `session-durable-copy`

A stored session MUST survive loss of its source (rotation, deletion, expiry): pond is the record after ingest, and re-ingest cannot recover rows a vanished source no longer holds. Recovery is `pond copy --from <store> --to <file>` snapshots taken before risky operations; retention (`lance-manifest-retention-floor`) is no recovery floor. Why: being the durable record is pond's value.

#### `session-movement-complete`

Storage MUST be the union of every still-reachable source - the completeness complement to `adapter-integrity-additive-sync`: lose nothing stored, skip nothing a source still holds.

- The `pond sync` freshness skip and the `pond copy` per-session delta are optimizations and MUST NOT leave an un-ingested row unstored.
- Datasets commit non-atomically (`lance-forward-compat-no-cross-shard-atomic-write`), so any "already ingested" signal MUST derive from stored data, never a pre-durability marker; a partial flush then re-ingests instead of latching "done".
- The signal MUST stay cheap on every backend, bounded by data scanned, not stored history.
- `pond copy` always ends with a composite-PK verify (exit 6 on a missing row or destination duplicate).
- The freshness skip MAY also skip a source that a bounded whole-source inspection proves holds nothing ingestible; the proof MUST be re-derived from current content every run, never cached, and an unclassifiable source MUST be re-read.

Why: monotone-but-incomplete is still silent loss - a skip outrunning durability, or an unverified delta, drops data while reporting success.

#### `session-append-only-exception`

Specified, not yet implemented: until `pond erase` ships, pond performs no deletion. Erasing a whole session is the single exception to append-only and pond's only deletion; within a session no stored Message or Part is ever mutated, reordered, or removed. It is operator-only (`pond erase <session-id>`, CLI and HTTP, never MCP), cascades to child sessions (mirroring `adapter-lineage-complete-restore`), and is a true byte purge - delete, compaction, version-history cleanup, blob purge - so time-travel keeps nothing. Erased keys enter an ingest denylist so still-present sources cannot resurrect them - the subtraction term keeping `session-movement-complete` sound. It names what it erased and what the denylist blocks. Why: right-to-erasure needs deletion without resurrection or retained bytes; one operator-only exception leaves `session-durable-copy` and `adapter-integrity-additive-sync` intact.

### 5.5 Embeddings are derived

An embedding is pond-produced, never source-supplied: nullable `vector` and `embedding_model` on `messages`, filled in the ingest commit when embedding is on (Section 8), otherwise later by a `pond optimize` embed pass.

#### `session-embed-from-canonical`

A message's embedding MUST derive from its stored `search_text`, never the source record. Why: `search_text` is durable and the source is not, so a model change is a re-derivation, not a migration. Re-embedding rewrites only the two derived columns (`lance-append-only`): a swap touches only rows with a differing `embedding_model`, dropping the vector index first (centroids are per distance space); a new dimension migrates the column additively on `messages`, never a new table. Manifest history keeps prior vectors, so a bad swap rolls back without re-ingest.

### 5.6 Derived analytics columns and additive schema migration

`parts` materializes three nullable columns at ingest from tool Part bodies: `tool_name`, `call_id` (also the approval request's `tool_call_id`), and `is_failure` (tool results only); NULL means not a tool part or not carried by the source (`model-no-synthesis`). Why: analytics need narrow native columns - a JSON getter over `variant_data` reads the whole multi-GB column, which cannot finish inside the query timeout on an object store. `variant_data` stays the verbatim record; these are projections, never independently writable.

#### `session-additive-schema-backfill`

A schema change adding derived nullable columns MUST upgrade existing data in place, never by re-ingest: on open, a store missing derivable columns backfills them from stored data (one `add_columns` commit per table; concurrent openers race benignly under OCC), and an older `.pond` archive restores by deriving missing cells at the read boundary, never mutating the snapshot. Derivation uses stored data only; a cell it cannot justify stays NULL (`model-no-synthesis`). Why: re-ingest is no migration path (`session-durable-copy`), so stranding existing stores or archives would break durability.

---

## 6. Adapters

An adapter is the codec between one client format and the canonical model.

### 6.1 Bidirectional codec

Two faces with genuinely different shapes, hence separate objects: *parse* (client format to canonical; configured against a source - a directory, an HTTP endpoint - and streaming) and *serialize* (canonical to client format; a pure function of a session, holding no source).

### 6.2 Restore is hub-and-spoke

Serializing is restore: any adapter can restore any stored session (Section 1.2).

#### `adapter-restore-distinct-reconstruction`

Restores land in the adapter's own layout under the no-overwrite rule of `cli-resume` (Section 7.8); a native replay of a captured file targets its original path on purpose (that file IS the transcript, so the refusal means "already resumed"). A *reconstruction* (foreign, or a downgraded native origin) MUST be named distinctly from the source file, deterministically. Why: under the source name a downgrade collides with the file it routes around and the refusal silently hands back a file the client cannot open; determinism makes a retry refuse instead of writing a second file.

#### `adapter-lineage-complete-restore`

Restoring a session MUST also restore its children (sessions naming it in `parent_session_id`), so the restore stands alone rather than with dangling subagent references; a child that is itself a parent MUST fail with a typed error naming it, never a silent partial restore (`adapter-integrity-no-silent-drops`). Depth one is a RESTORE limit only (spawn is one level deep; fork and continuation lineage is not, e.g. OpenClaw): ingest records every source-stated edge at any depth (`model-lossless-projection`), and an adapter MUST NOT drop, flatten, or refuse an edge to fit (edges would then depend on ingest order, which `adapter-integrity-additive-sync` forbids).

### 6.3 Origin and restore fidelity

The system, never the adapter, decides fidelity by comparing `Session.source_agent` with the adapter's origin identity.

#### `adapter-native-restore-lossless`

A matching origin (exact or subpath) is *native* restore and MUST be lossless (value-complete, Section 1.3); any other pairing is *foreign*: a valid, idiomatic best-effort session, dropping what the target cannot express (canonical keeps it). An adapter MAY downgrade a matching origin when the client cannot load the captured format; served fidelity is reported per session, so a downgrade is never silent. Values truncated under `adapter-bounded-values` restore as their sentinel. Why the system decides: callers rely on native losslessness; adapter self-assessment would be a convention.

### 6.4 The no-synthesis seam

Parse builds canonical values only through extractor helpers over one source record; the possibly-missing value type has no adapter-reachable constructor, so a literal, default, or sentinel cannot compile - enforcing `model-no-synthesis` and `model-schema-honesty`. Serialize needs no seam (canonical is trusted).

#### `adapter-provenance-required`

The adapter-reachable Part constructor MUST require `provenance` (`model-part-provenance`); a defaulted or unclassified one MUST NOT compile. Why: a `conversational` default silently mislabels harness machinery, provenance cannot be retrofitted (only the source parse carries the signal), and the obligation forces each adapter to confront its harness's injection patterns.

#### `adapter-transport-agnostic-seam`

The parse seam exposes one source record through a few value accessors, assuming nothing about its origin. Why: file, HTTP, and stream adapters share it unchanged.

#### `adapter-bounded-values`

Every text-column value passes the seam's size bound: one over the substrate's per-value limit is truncated in place to a sentinel recording its byte count, the record otherwise intact; blobs are exempt. Why: an unrepresentable text value aborts the process; the bound makes it an attributable, recoverable truncation.

### 6.5 Placement procedure

To satisfy `model-lossless-projection`, every field of every ingested record is placed by one of three rules:

1. Message content becomes typed Parts, each `conversational` or `injected` (`model-part-provenance`). A span fusing authored and injected content (a turn wrapped in runtime context tags, an attachment-scaffolding prefix on a prompt) is split at the exact byte boundary into provenance-homogeneous Parts; native restore reconcatenates them in `ordinal` order, so the split is value-complete-lossless (Section 1.3).
2. Harness or runtime metadata goes into `options` on the Message or Part the record maps to, including any leftover field of a mapped record.
3. A record mapping to no Message (a standalone log entry, neither a turn nor metadata on one) is carried whole: a system-role Message with empty `content` and the whole-record encoding in `options`, in log order by its own timestamp (or the session-anchor fallback `model-no-synthesis` permits), its id per `lance-deterministic-pk`.

Rule 3 is the catch-all that makes losslessness reachable for any record, including record types that postdate the adapter.

### 6.6 Ingest order and integrity

#### `adapter-integrity`

The parse face's contract on output:

##### `adapter-integrity-event-ordering`

Per session: the Session first, then each Message immediately followed by its Parts, in order. Core computes indexed text at each message boundary without buffering across messages (leaving a Part stream signals completion). Core MAY flush a completed prefix of a large in-flight session to bound memory, but MUST write the session row only after the substream closes, so the freshness signal cannot outrun its rows.

##### `adapter-integrity-no-silent-drops`

Malformed source input MUST surface as a typed error naming the adapter and the fault's location, never be silently skipped. Why: a silent drop is invisible loss; a surfaced one is a fixable report.

##### `adapter-integrity-opaque-ids`

Canonical identifiers are opaque strings: structure a source encodes in a path or name is decoded once at ingest and stored, never re-parsed by readers.

##### `adapter-integrity-additive-sync`

A write MUST NOT overwrite a row present under its primary key (matched rows are no-ops). Adapter output is monotone across versions (a newer adapter yields a superset of rows). The source is not authoritative over the stored copy - a since-corrupted source must not overwrite good data; changing or removing a stored row is a deliberate migration, never a re-ingest side effect.

##### `adapter-integrity-dedup`

An adapter SHOULD detect duplicate primary keys in its own output via the source's own mechanism (keeping the count visible in the ingest summary); the write path drops duplicates as a floor regardless. A duplicate matches on key AND content - a shared source id with different content is not one, and dropping it is invisible loss (`adapter-integrity-no-silent-drops`) - so an adapter deduping on source id alone MUST confirm content identity first or derive a content-keyed key; an unresolvable collision surfaces as a visible drop.

### 6.7 The registry

One registry; a new adapter is a new file plus one line there - no central enum, dispatch, or codegen. Why: a low fixed cost keeps the adapter list open-ended.

### 6.8 Conformance

Each adapter's round-trip test (fixture to canonical to native, value-equal) enforces `adapter-native-restore-lossless` and exercises `model-lossless-projection`; the shared harness (`packages/pond/tests/integration/adapter/mod.rs`) proves the canonical fixed point, or, where native output targets an external import tool, names the test owning value-equality. Foreign output is tested for target-format validity and reviewed against a golden file.

### 6.9 Adapter set

The registry (`packages/pond/src/adapter/mod.rs`) is the source of truth for supported formats; per-adapter extraction detail lives in its code, format archaeology in `docs/adapters/<source_agent>.md` (playbook adapters).

---

## 7. Protocol

### 7.1 Transport-agnostic handlers

Every operation is a request-to-response handler; the HTTP and MCP transports only decode and encode, so a handler cannot tell which invoked it.

### 7.2 The request envelope

Every request carries `protocol_version` (v1 is `1`) and an optional `namespace`, and gets a request id (HTTP `X-Pond-Request-Id`; MCP the JSON-RPC `id`). Within a major version the schema is additive-only (removal or retyping is a major bump); experimental `/v1/x/` operations still carry the envelope but may change or vanish within a major. Generated JSON Schema is exact.

### 7.3 Namespace

`namespace` is an opaque tenant-routing string (default: the personal namespace), not the Lance namespace that locates that tenant's tables.

#### `wire-namespace-resolution`

Namespace acceptance and its table mapping MUST be decided in exactly one place. Why: multi-tenancy then changes one place.

### 7.4 The error model

Success and error are mutually exclusive at the body level. An error body is one shape:

```json
{ "error": { "code": "validation_failed", "message": "...", "details": {} } }
```

The code set is closed:

| Code | When | HTTP | Retryable |
|---|---|---|---|
| `validation_failed` | bad request shape, missing field, type mismatch, batch over a cap; a request for a capability this instance has disabled (`mode=vector` with embedding off) | 400 | no |
| `version_unsupported` | a `protocol_version` pond does not understand | 400 | no |
| `not_found` | a `pond_get_session` / `pond_get_message` target that does not exist | 404 | no |
| `namespace_unknown` | a namespace string not provisioned | 403 | no |
| `storage_unavailable` | a Lance or object-store failure after retry was exhausted | 503 | yes |
| `conflict` | optimistic-concurrency retry exhausted on a write | 409 | yes |
| `internal` | an unhandled fault | 500 | no |

Retryability is conveyed by the code alone; `conflict` maps the substrate conflict signal (3.6). Per-row partial ingest outcomes live in the success body.

### 7.5 Operations

1. **`pond_search`** (`POST /v1/search`) - one arm (`mode`: `fts` default, or `vector`; Section 8); hits grouped by session; `format` `text` (default) or `json`.
2. **`pond_get_session`** (`POST /v1/get-session`) - a session's conversational view (text plus `parts_summary`). `id` is a session id, or a message id upcast to a containing session, page anchored there and `resolved_from_message_id` flagging the upcast (safe because the operation states the intent); a message replayed into several sessions (`lance-table-creation-session-scoped-pk`) resolves to an unspecified one of them, named by the response's `session` - read by session id to pick a copy. `from` is `start` or `end`; pages are bounded by `limit` and a size budget, never cut mid-message. Not for bulk export.
3. **`pond_get_message`** (`POST /v1/get-message`) - the target's full Parts, any role (budget-bounded; `target_parts_remaining` signals the cut), plus conversational siblings (so system/tool carriers cannot crowd the window). A session id is rejected with a hint naming `pond_get_session` (only message-to-session is well-defined).
4. **`pond_ingest`** (`POST /v1/ingest`) - batches capped by event count and bytes, applied per session, partial success per row.
5. **`pond_sql`** (`POST /v1/x/sql`) - one query per `protocol-sql-read-only`, rejected before any dataset opens; capped JSON rows, cut signalled; experimental because its columns follow the storage schema; no tenant scoping yet.

Resources: `schema://pond`, `schema://pond-sql`, `stats://pond` (search fields, SQL schemas, statistics).

#### `protocol-sql-read-only`

Every SQL surface (MCP tool, `pond sql`, `/v1/x/sql`) MUST be read-only in two layers: a pre-parse gate admitting exactly one query (SELECT, WITH, or a set operation) or an `EXPLAIN [ANALYZE]` of one, rejecting EXPLAIN of anything else; then DataFusion with DDL and DML disallowed and statements allowed only for an EXPLAIN (whose inner query the gate vetted); fresh context per query. Why: engine options alone miss multi-statement input, `DESCRIBE` / `SET` / `SHOW` / `COPY`, and EXPLAIN of a write.

### 7.6 Ingest events

An event is one Session, Message, or Part tagged with its kind, ordered per `adapter-integrity-event-ordering`.

#### `wire-ingest-relabel`

`source_agent` and `project` are immutable after first write (denormalized, 5.3): a re-submission with a different value MUST keep the STORED labels, its rows written under them, and the disagreement MUST be counted (`relabeled_sessions`) and logged, never silent, never a per-row error. Why not reject: the stored value is already safe, so rejecting only discards new messages - permanent loss on exactly the hosts running a corrected adapter. Stale labels still mis-attribute (`model-project-non-empty`; 8.3); fixing one is an undefined migration (`adapter-integrity-additive-sync`), never `pond erase`, whose denylist (`session-append-only-exception`) blocks the re-sync.

### 7.7 MCP surface

MCP exposes the read operations and resources; ingest (an operator action) is HTTP and CLI only. For clients deferring tool descriptions, server `instructions` carry all routing (search finds; get reads and summarizes; `pond_sql` is the self-demoting escape hatch), names alone must route, descriptions lead with their verbs, cookbook detail lives in `schema://`, errors teach the next query.

#### `protocol-self-describing-capabilities`

Instructions and tool descriptions MUST offer only the serving instance's capabilities, and default-behavior statements MUST come from the running binary, never an installed file. Why: fleets upgrade host by host, so files go stale; a refused arm wastes agent turns.

#### `mcp-read-only-heal-exception`

The MCP surface MUST expose no write operation. The substrate's open path MAY write on any surface, MCP included: creating the empty tables of a fresh store, `local-store-self-heal` quarantine renames, and `session-additive-schema-backfill`. All three are open-triggered maintenance, never user-data writes, and MUST NOT be read as precedent for MCP write paths. Why: all surfaces share one open path; forbidding them would brick MCP on a fresh, crash-damaged, or older-schema store.

### 7.8 CLI verbs

Global selectors: `--storage-path` / `POND_STORAGE_PATH`, `--config-file` / `POND_CONFIG_FILE`, `--state-dir` (`XDG_STATE_HOME` as an argument; MUST be absolute; hidden, as the scheduler writes it). `init` ignores an env-sourced `--storage-path` (persisting env state would surprise).

#### `cli-json-summary`

With `--format json` a verb MUST print one summary document on stdout per outcome (ok, skipped, error), progress on stderr. Documents evolve additively: new keys may appear and are omitted when empty, and within a major version an existing key never changes meaning or vanishes; an uncomputed count is null, never guessed. Serde types and integration tests pin shapes.

| Verb | Contract | Rules |
|---|---|---|
| `init` | Setup wizard | `cli-init` |
| `sync [<adapter>]` | Ingest, fold | `cli-sync` |
| `optimize` | Embed, fold, diagnose | `cli-optimize` |
| `adapters list\|discover\|enable\|disable` | Manages `[adapters.*]` (with `init` and `serve --bootstrap` the only writers; enabling is always explicit); `discover` never overwrites an entry naming a `path`; a `path` array fans out to single-path passes (adapters see a scalar); a malformed array fails the whole resolve, naming the unaffected adapters and the scoped `pond sync <adapter>` that still works | `lance-deterministic-pk`, `cli-sync` |
| `search`, `get-session`, `get-message` | The read operations; `search --explain` shows the plan | 7.5 |
| `sql` | One SELECT/WITH, inline or parquet/ndjson | `protocol-sql-read-only` |
| `status` | Counts, index health, schedule, host view (pending from the local cache, never remote); `--hosts` fleet | `model-pond-options` (`--hosts`), `cli-optimize` (read-only diagnosis) |
| `serve`, `mcp` | HTTP or stdio MCP | `cli-serve` |
| `schedule start\|stop\|status\|logs` | Scheduler registration | `cli-schedule` |
| `config show\|path\|schema` | Redacted resolved config with per-field source | `storage-redaction` |
| `storage check\|use <URL>` | End-to-end probe (exit code per failure class); `use` then switches | `cli-storage-use` |
| `creds add\|list\|delete` | `[creds.*]` sets, secrets redacted | `creds-scope-match` |
| `copy` | Move data | `cli-copy` |
| `erase <id>` | Pending (not yet implemented): operator-only purge plus denylist; never MCP | `session-append-only-exception` |
| `resume <id> --to <adapter>` | Restore | `cli-resume` |
| `completions`, `skill` | Shell completions; SKILL.md from the binary | - |

#### `cli-sync`

Ingests enabled `[adapters.*]`, then folds indexes and compacts, amortizing the round-trip-bound version-cleanup walk over several runs (`pond optimize` and `pond copy` clean every run). MUST NOT discover, enable, or write adapter state (unattended runs never grow the set); `--path` is a one-off override that never writes config. Source trouble is per-adapter under stable reason tokens (missing path fails the adapter; lossy runs degraded, losses counted) and the run exits 0 (`session-movement-complete`: other sources stay reachable, and a shared fleet config legitimately enables adapters for tools a host lacks) - but a named adapter errors hard and a malformed config fails the resolve. Single-flight per host and store via a state-dir flock, never on the store (cross-host stays pure OCC): a second sync waits, naming the holder; `--no-wait` skips with exit 0. `--dry-run` writes no session data, but may create an empty store at a new destination and build the local freshness cache; runs record per-host freshness and last-sync state for `pond status`.

#### `cli-optimize`

Embed stage (backlog), then index stage (fold, compaction, cleanup every run); `--only` / `--skip` select, `--force-embed` re-embeds other-model rows. A bare run diagnoses costly conditions (model swap, index delta pile-up, orphaned indexes, tiny-page fragments) with each heal's cost, also shown by `pond status`. Only `--full` heals: pond MUST NOT self-initiate a large rewrite.

#### `cli-init`

Idempotent wizard (storage probe, adapters, MCP, opt-in schedule) writing config once, at the end; every section is flag-answerable, and `--yes` alone never schedules. MCP consent installs the bundled skill; a hand-edited copy MUST NOT be overwritten unconfirmed. An opted-in schedule registers after the first sync when init runs one - on success, failure, or Ctrl-C - and immediately otherwise. Why: a fresh systemd timer fires on registration and would race the long first sync.

#### `cli-serve`

`/mcp` MUST check `Host` against a loopback-only default allowlist (DNS-rebinding defence; `--allowed-host` widens); `/v1/x/sql` shares the gate because it reads arbitrary corpus rows; the other `/v1/*` routes, `/v1/ingest` included, are not gated. `--with-sync` runs the interval sync in-process after bind - topology only, not live-write: same sync, lock (`--no-wait`), and records, stdout left to the transport, a failed cycle never stopping serving. `--bootstrap <adapter>` (operator opt-in) enables it only when no `[adapters.*]` entry exists (disabled counts), before the sync loop starts; a discovery failure is a logged warning naming `pond init`, and serve still starts. Unix `--socket` is owner-only 0600 (permissions are the access control), with an exclusive sidecar lock keeping a second server off the path.

#### `cli-schedule`

Registers `pond sync -q --no-wait` with launchd, systemd timers (or crontab), or Task Scheduler (via windowless `pondw.exe`). Registration MUST pin the resolved state dir and config file into the job (on Windows by `--state-dir` and `--config-file` arguments, `Exec` having no environment), warning when `XDG_STATE_HOME` is set interactively. Why: schedulers never source shell rc files, so an unpinned override splits lock, state, or configuration between scheduled and manual runs.

#### `cli-copy`

Moves canonical data between stores, `.pond` archives, and JSONL (export-only), endpoints sniffed by suffix. Store-to-store is an incremental idempotent union merge, re-runnable onto a populated destination, that MUST NOT delete or modify the source; absent sessions append, grown ones (message count rose - monotone under append-only, comparable across clocks) append their delta, keeping remote copy bandwidth-bound. Every run ends with the `session-movement-complete` verify (exit 6); `--verify-only` runs it alone. Only copy carries the durable corpus to a new backend (`session-durable-copy`).

#### `cli-resume`

Restores a session with its children or not at all (`adapter-lineage-complete-restore`) at system-decided, per-session-reported fidelity (6.3). MUST NOT overwrite or delete: a preflight fails the whole operation before the first byte, naming every collision. Why: the destination is a live client's data directory, which is what makes a collision mean "already resumed - open the file you have". Exit codes separate not stored, unanswerable, collision, and failed write; a failed write unwinds what it created, best-effort. Operator-only: CLI in v1, never MCP; HTTP deferred.

#### `cli-storage-use`

Probes, then flips `[storage].path`; copies nothing, leaving every store untouched. `local` (or `default`) names the platform default dir wherever a storage URL is accepted.

### 7.9 Versioning

Wire, canonical model, and storage schema all evolve additively (storage version per `lance-dataset-schema-version`). Pre-release: no shims; a breaking change is a major bump.

---

## 8. Search and embeddings

Search returns messages. The embedding seam (8.5) is generic, not part of the session datasets.

### 8.1 Single-arm retrieval

Two arms over one corpus: BM25 full-text (`fts`, default) over indexed text and `vector` over embeddings; the caller picks one per query, with no server-side fusion. Vector ranks by cosine similarity plus a gentle recency tiebreaker, fts by raw BM25; both score by raw magnitude, never rank position (so scores do not shift with pool size or limit). Vector availability follows 8.7. Hits group to one summary per session on `session_root`, represented by the best hit - or per message when a session filter pins one conversation (root keying would collapse it to one hit). A `recency` sort returns in-scope messages newest-first, labeled as such. Arm and plan attribution is operator-only (`pond search --explain`).

#### `search-prefilter-pushdown`

Every vector and full-text query MUST push its scalar filters into the scalar indexes before ranking, never post-filter in memory. Why: a post-filter ranks first, so it silently returns fewer than the requested results and ignores the indexes.

### 8.2 Indexed text

`search_text` is the conversation: one field per message, built at ingest by one uniform core function (per-adapter customization rejected, so the corpus has one shape), concatenating in order the text of TextParts and the metadata of FileParts with `provenance: conversational`; null for system and tool messages and for messages with no conversational text. Reasoning, tool calls and results, approval parts, and injected parts are deliberately unindexed, reachable only via `pond_get_message` or `pond_sql`. Excluding injected parts is not customization: the seam decides provenance once (`adapter-provenance-required`) and this function reads `Part.provenance`.

#### `search-language-neutral-index`

The full-text index MUST keep every language searchable and MUST NOT drop or mangle another language's tokens; a monolingual stemmer is permitted only if unrecognized tokens pass through unchanged and stay exact-matchable. Why: sessions arrive in any language; a lossy transform under-indexes all others, an additive one does not. Pond uses the word-level `simple` tokenizer with English stemming (Cyrillic passes through unstemmed), which outretrieved character `ngram` at a lighter index (`docs/researches/tokenizer-experiment-report.md`).

### 8.3 Filters and ranking

Filters (project, session, source agent, time range) are denormalized onto the searched table (5.3), so all push down. Source agent is exact-or-subpath (`openclaw` matches `openclaw/subagent`, never `openclaw-x`). Subagent sessions (a `source_agent` subpath) are excluded by default - a search wants main sessions - unless `include_subagents`, a session filter, or a subpath-naming source filter scopes to them deliberately; a root value stays main-only. Each group carries a small fixed number of top matches (cap in code), widened to the limit for a session-scoped search.

#### `search-absence-honesty`

Every search response MUST report how many searchable messages the caller's filters left in scope, especially when zero. Why: retrieval always fills its top-k from whatever scope it gets, so without the scope size a caller cannot tell "nothing relevant exists" from "my filters excluded everything" - measured agent behavior turns the first reading into false "never discussed" conclusions. Scores carry no absence signal (present and absent content score in overlapping bands; `docs/researches/embeddings.md`), so the scope count is the one honest cue.

### 8.4 Hit payload

A hit carries enough to judge relevance without a second fetch: the full indexed text when small, else a bounded prefix plus a match-windowed snippet (bounds in code); user-role hits add `parts_summary`, so a prompt that attached files is distinguishable. The full message stays available via `pond_get_message`.

### 8.5 The embedding seam

Text-to-vector sits behind one backend seam (text in, vectors out), so a local model and a remote provider look alike to everything above. The engine ships a fixed model set; configuration selects one and supplies its vector parameters - no model is mandatory or named here.

### 8.6 Producing embeddings

When enabled (8.7), ingest embeds each message from its just-computed `search_text` in the same append commit - the derivation stays canonical (`session-embed-from-canonical`), only the timing is at ingest. `pond optimize`'s embed stage covers the backlog (null `vector`, or a different `embedding_model`) through the same seam, which future consumers can reuse.

### 8.7 Opt-in

`[embeddings].enabled`, default off. Off: no model loads, ingest writes no vectors, maintenance ignores the vector index, and `vector` requests are refused. On: ingest embeds inline, `pond optimize --only embed` fills the backlog, and `vector` is available; `fts` stays the default. Instances sharing a store may differ: an enabled instance's embed stage fills rows others left null.

### 8.8 Index lifecycle

Vector and full-text columns exist from table creation, so toggling embeddings needs no migration. An instance with embedding off neither folds nor rebuilds the vector index and leaves an existing one untouched (`lance-index-maintenance`). Vector search brute-forces below an activation threshold and uses IVF_SQ above it (threshold and partitioning in code); `[search].nprobes` tunes recall.

---

## 9. Deferred

Scoped out of v1. None needs a migration or cross-cutting change to activate, except where an item names its own representation change (BFloat16, blob v2).

1. **Future consumers** - own canonical models and tables on the generic substrate: resources and blobs; social and web archives (flat messages, never coerced into Session/Message/Part); a Files-API-shaped blob store; a versioned-document store.
2. **Future adapters** - a file plus a registry line: Managed Agents (coordinator and threads as linked Sessions), Cursor, aider, Gemini CLI.
3. **Provider-target restore** - canonical into provider API request shapes; always foreign, API-valid.
4. **Live-write** - micro-batched single-writer ingest (buffer live events, one merge-insert per interval or size threshold) through the existing chokepoint and OCC, identical on every backend (object-store cost is version churn, so the batch is the point). Unflushed events are durable only via their source (`session-movement-complete`), so pond is a mirror unless a store-of-record backstops it; distinct from the `cli-serve` interval sync. MemWAL is the escalation for many-writer contention (a substrate swap per 3.8), adopted only once hosted multi-writer ingest exists.
5. **Hosted multi-tenant** - a child Lance namespace per tenant and a hosted catalog, via `lance-chokepoints-catalog` and `wire-namespace-resolution`.
6. **Other** - remote embedding providers; cross-session FilePart dedup; attachment-content indexing; typed image arrays for `image/*`; JSON-path indices and predicates on `options`; lineage graph traversal; wire time-travel; an OTel projection; a `pond tag` verb (named-snapshot recovery floor); BFloat16 vectors once Lance's IVF_SQ accepts `lance.bfloat16`; blob v2 part storage (`lance.blob.v2`, V2.2; unblocked at the 10.0.0 pin); a candidate-pool reranker; a `min_score` floor (removed for zero use; re-add behind the vector arm only on measured need).
7. **Open questions** - what first activates the multi-tenant router and live-write; which catalog backend hosting uses.

---

## 10. References

Inspiration and corroboration; the contract is Sections 1 through 9.

- [Scaling Managed Agents](https://www.anthropic.com/engineering/managed-agents) - Anthropic Engineering. The append-only session log and the stable-conversational-layer meta-harness that the canonical model and `options` follow.
- [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) - Anthropic Engineering. Why a durable, searchable session history is worth building.
- [Context Rot](https://www.trychroma.com/research/context-rot) - Chroma research. The same motivation from the retrieval side.
- [Recursive Language Models](https://arxiv.org/html/2512.24601v3) - arXiv 2512.24601. Recursion as sub-agent spawning, corroborating Section 4's branching model.
