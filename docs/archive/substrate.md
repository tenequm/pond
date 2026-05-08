# Pond Substrate Specification

## 1. Purpose

The substrate is Pond's engine. It provides primitives for storing, searching, and embedding typed records and content-addressed blobs over object storage, without knowing the shape of any consumer domain.

The substrate is the part that stays the same when sessions, archives, resources, or any future consumer evolve. The substrate's stability is what keeps consumer evolution cheap.

## 2. Mental model

Three layers, top to bottom:

- **Facade**: MCP tools, CLI verbs, JSON wire types. Knows about consumers.
- **Consumers**: sessions, resources, (eventually) archives. Each owns its canonical types and ingest adapters.
- **Substrate**: generic storage, search, embedding, blob, and adapter primitives. Knows nothing about consumer types.

The substrate never imports from facade or consumers. Consumers never reach around the substrate. Facade never reaches around consumers.

## 3. Scope: included

The substrate owns mechanics that are the same regardless of what's being stored:

1. **Object storage abstraction.** A unified handle wrapping `object_store` (S3 / GCS / Azure / local fs).
2. **Lance dataset operations.** Open, read, append-write with OCC retry, schema migration via column add, blob column support, manifest version tracking, read consistency control.
3. **Generic record schema trait.** The contract a consumer implements to plug its canonical types in.
4. **Hybrid search primitives.** BM25 FTS, vector kNN, RRF fusion, all parameterized by the schema being searched.
5. **Embedding pipeline.** Trait for text to vector. Generic backfill loop that finds unembedded rows by predicate and writes vectors.
6. **Content-addressed blob store.** Put bytes by content hash, get bytes by reference, dedup as a property of the store.
7. **Namespace resolution.** Trait that maps a request to `(bucket, prefix)`. Default implementations: env-based, path-derived.
8. **SourceAdapter trait.** Generic ingest contract: `discover` plus `decode`. Adapters yield records of the consumer's schema.
9. **Forward-compat utilities.** Helpers for nullable column addition, schema version metadata, lazy reads of evolved datasets.
10. **Concurrency primitives.** OCC retry helpers, content-addressed idempotency.

## 4. Scope: excluded

These belong to consumers, never the substrate:

- Session concepts: Part, role, agent_id, parent_message_id semantics, harness extensions, tool_call/tool_result vocabulary
- Archive concepts: Author, Reaction, Edit, Quote, Channel, Thread, platform-specific IDs
- Resource concepts: KB metadata, citation links
- MCP tool surface (`pond_search`, `pond_get`, schema/stats resources)
- CLI verb implementations
- Wire JSON Schemas (those are facade-level; substrate doesn't know what shape consumers expose)
- Consumer-specific search filters (`include_thinking`, `boost_recent`, `role`, `from_date`; these are consumer vocabulary, expressed as parameters substrate accepts but doesn't define)
- Replay logic
- Authentication, authorization, identity
- Tenancy policy (the substrate exposes the *mechanism* for namespace-prefixed layout; it does not know how to map a user to a tenant)

## 5. Public contract (illustrative trait shapes)

Names are illustrative. The principle is what matters: every signature is generic, no domain types appear. Real shapes will be discovered while building sessions and resources.

```
trait RecordSchema {
    type Record: Serialize + DeserializeOwned;
    const DATASET_NAME: &'static str;
    fn arrow_schema() -> ArrowSchema;
    fn record_id(record: &Self::Record) -> RecordId;
    fn search_text(record: &Self::Record) -> Option<String>;
    fn embed_targets(record: &Self::Record) -> Vec<EmbedTarget>;
}

trait EmbeddingProvider {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
    fn dimension(&self) -> usize;
    fn model_id(&self) -> &str;
}

trait NamespaceResolver {
    fn resolve(&self, ctx: &RequestContext) -> Result<NamespaceRef>;
}

trait SourceAdapter {
    type Schema: RecordSchema;
    fn discover(&self, root: &Path) -> Result<Vec<SourceRef>>;
    fn decode(&self, source: SourceRef) -> Result<Vec<<Self::Schema as RecordSchema>::Record>>;
}

// Substrate operations:
fn store<S: RecordSchema>(ns: &NamespaceRef, records: impl Iterator<Item = S::Record>) -> Result<WriteStats>;
fn fetch<S: RecordSchema>(ns: &NamespaceRef, ids: &[RecordId]) -> Result<Vec<S::Record>>;
fn search<S: RecordSchema>(ns: &NamespaceRef, query: SearchQuery) -> Result<Vec<Ranked<S::Record>>>;
fn put_blob(ns: &NamespaceRef, bytes: Bytes) -> Result<BlobRef>;
fn get_blob(ns: &NamespaceRef, blob: &BlobRef) -> Result<Bytes>;
fn embed_backfill<S: RecordSchema>(ns: &NamespaceRef, embedder: &dyn EmbeddingProvider) -> Result<EmbedStats>;
```

The acid property: **no public signature mentions Part, Session, Author, Channel, ToolCall, Resource, or any other consumer-domain term.** If a signature can't change without naming one, the substrate has drifted.

## 6. Invariants the substrate guarantees

1. **Append-only.** Substrate never mutates an existing record's payload. Updates produce new records.
2. **Content-addressed dedup.** Writing the same blob twice is a no-op; same content always returns the same `BlobRef`.
3. **Optimistic concurrency.** Concurrent writers do not corrupt; conflicts surface as retryable errors.
4. **Read-after-write per writer.** A record written and immediately read by the same writer returns the written value.
5. **Namespace isolation.** A request scoped to namespace A cannot read or write namespace B unless explicitly given access to B.
6. **Non-breaking schema additions.** Adding a nullable column never breaks existing readers.
7. **No silent drops.** Ingest errors surface; substrate never swallows malformed input.
8. **Stateless.** No in-process state required for correctness; any worker can perform any operation.

## 7. Acid tests

Continuous tests that fail when substrate drifts. Implement them before the first consumer schema.

### 7.1 Toy schema test (the most important one)

Implement a `Note { id, title, body, tags, created_at }` schema in the substrate's test suite. Exercise: store, fetch, FTS search, vector search, hybrid search with RRF, blob attachment, embedding backfill, namespace isolation, schema migration (add a column, read old rows).

Rule: **any substrate change that breaks the Note tests is drift.** Either revert the change or fix it so Notes still work without modification.

### 7.2 Two-schema coexistence test

Add a `Bookmark { id, url, title, snippet, captured_at }` schema alongside Notes. Tests: independent dataset paths, independent search, federated search ranking across both with no schema-specific code in the search call site.

Catches: substrate accidentally specialized to a single schema's shape.

### 7.3 Domain-leakage grep

A CI check that fails if the substrate module contains any of: `Part`, `Session`, `Author`, `Reaction`, `ToolCall`, `Message`, `Channel`, `Resource`, `Agent`, or any consumer-canonical type name.

Trivial to maintain; catches accidental imports and copy-pasted code.

### 7.4 Build-without-consumers test

The substrate module compiles successfully with the consumer modules excluded from the build graph. Cargo features are sufficient to express this in one crate; later crate split is mechanical if this stays true.

### 7.5 Doc-purity check

Every public substrate item has a doc comment that does not reference any consumer. If you can't describe a function without saying "for sessions" or "used by parts," it doesn't belong in substrate.

## 8. Forbidden patterns

Patterns to reject in review without exception:

1. Helper functions taking domain types: `embed_part(part: Part)` instead of `embed<S: RecordSchema>(record: &S::Record)`.
2. Branching on consumer kind: `if record_kind == "session" { ... } else if record_kind == "archive" { ... }`.
3. "Generic" types instantiated with only one type ever: `Storage<T>` always becoming `Storage<Part>` is not generic.
4. Domain enums: `enum RecordType { Part, Author, Resource }`. Substrate does not enumerate consumer kinds.
5. Substrate logic that renders, formats, or summarizes a record. Storage/search/blob/embed only.
6. Substrate importing from any consumer module. Absolutely forbidden, in any direction.
7. "Just for now" exceptions to any rule above. They become permanent.

## 9. Promotion criteria (from consumer code into substrate)

A piece of code graduates to substrate only when **all** of these are true:

1. Two or more existing consumers use the same logic, not "might use later," not "looks reusable."
2. The logic can be expressed without naming any consumer's domain types.
3. A third toy schema (the Note tests) can use it without modification.
4. It does not violate any invariant in §6.

If any check fails, the code stays in the consumer.

## 10. Demotion criteria (from substrate back to a consumer)

Move code out of substrate back to a consumer when:

- Only one consumer turns out to use it.
- Its interface would need domain-specific parameters to remain useful for a new use case.
- Generalizing it to fit a new consumer changes its signature in a domain-aware way.

When demotion happens, move it down to the actual consumer. Don't keep it "in case." A junk drawer with one user is a junk drawer.

## 11. Stability commitments

- The substrate's public types and traits are treated as 1.0 from day one in spirit. Breaking changes to substrate require deliberate review against §9, even within a single crate.
- Schema migrations within consumer datasets are the consumer's concern; the substrate only provides the migration helpers.
- The substrate's on-disk layout (dataset placement, blob storage, namespace prefixing) is invariant. Consumers do not place files outside the layout.

## 12. Review protocol

- Every PR touching substrate code requires a one-line justification: which §9 criteria does this addition satisfy?
- Every new public substrate item is accompanied by a toy-schema test exercising it.
- Quarterly substrate audit: re-run §7 checks, scan §8 forbidden patterns, prune anything that's drifted, log the audit.

## 13. Anti-goals

This spec deliberately does **not** define:

- A canonical record format that all consumers share. There is no universal Part. Consumers own their canonical types.
- A unified search vocabulary. Consumers expose their own filters; substrate accepts opaque predicates.
- A "core schema" that's shared across consumers. The substrate has no schema of its own beyond what `RecordSchema` requires.
- A facade. MCP tools, CLI, JSON wire shapes are not substrate concerns.
