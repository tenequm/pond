# Pond refactor: 8-entry layout

> Status: pending review. Do not implement until tenequm approves.
>
> Companion to `docs/design.md`. Resolves drift between current `src/` and the
> design's stated module discipline (§2.1) and invariants (§2.3). No new
> functionality - pure shape work.

## 1. Target layout

```
src/
|-- main.rs              CLI dispatch + verb bodies + maintenance scheduler
|-- lib.rs               cross-cutting: Clock, RetryPolicy, Output, Error
|-- wire.rs              canonical 3.1 types == 3.6 envelopes == ErrorCode
|-- config.rs            Config + StorageLocation + EmbeddingModel registry
|                        + per-namespace override resolver
|-- substrate.rs         thin Lance: Handle, refresh, scanner_with_prefilter,
|                        merge_insert, Predicate, Blob v2, maintenance actions
|-- sessions.rs          Arrow schemas + batch codec + Store facade
|                        + search_text policy + denorm stamping
|                        + PendingMessage + MessageMeta + canonical projection
|                        + ordering-contract validator
|-- handlers.rs          pond_search + pond_get + pond_ingest + pond_session_events
|                        + Retriever trait + RRF/recency + error mapping
|-- transport.rs         axum + rmcp + AppState
|                        + namespace defaulting + HTTP/MCP render divergence
|-- adapter/             plural sources
|   |-- mod.rs           AdapterFactory + Adapter + Registry + Env
|   |                    + interactive discovery picker
|   |-- claude_code.rs
|   `-- codex.rs
`-- embed/               plural backends
    |-- mod.rs           EmbedBackend trait + worker + device select
    |                    + IVF_PQ params + hf-hub fetch
    `-- qwen3.rs         fastembed + candle loader
```

Layer dependency, top -> bottom only:

```
   main + transport
        v
   handlers
        v
   sessions::Store
        v
   substrate
        v
   Lance
```

`wire`, `config`, `lib` are utilities; anyone imports them.
`adapter` and `embed` sit beside `handlers` (handlers depend on them).

## 2. Constraints

- Pond is pre-release (CLAUDE.md). Breaking changes are free; no migration
  shims, no changelog, no compatibility code paths.
- Every commit compiles. `cargo test --locked` green. `cargo clippy -- -D warnings`
  green.
- No new functionality - behavior identical before and after.
- ASCII only (CLAUDE.md "prefer ASCII").
- No new comments beyond what existing files already carry.

## 3. File-mapping (current -> target)

| Current | Target |
|---|---|
| `src/main.rs` (CLI dispatch + verbs) | `src/main.rs` (kept) |
| `src/main.rs::spawn_maintenance` | `src/main.rs` (stays - lives with `serve`) |
| `src/main.rs::resolve_model` | `src/config.rs` |
| `src/lib.rs` (re-exports only) | `src/lib.rs` (expanded: Clock, RetryPolicy, Output, Error) |
| `src/types.rs` | `src/wire.rs` (merged - wire == canonical per 3.6) |
| `src/wire.rs` | `src/wire.rs` (kept, absorbs types.rs) |
| `src/config.rs` | `src/config.rs` (kept; absorbs per-namespace resolver) |
| `src/substrate.rs` (Lance + retry + refresh + maintenance actions) | `src/substrate.rs` (kept, slimmed) |
| `src/substrate.rs::{PendingMessage, MessageMeta, MessageWrite, UpsertStatus}` | `src/sessions.rs` |
| `src/substrate.rs::{sql_string, sql_like_contains, embedding_identity_filter, sql_in}` | deleted; replaced by `Predicate` in `substrate.rs` |
| `src/substrate.rs::ensure_embedding_indices` (IVF params) | `src/embed/mod.rs` |
| `src/datasets.rs` | `src/sessions.rs` |
| `src/ingest.rs::IngestValidator` | `src/sessions.rs` |
| `src/ingest.rs::pond_ingest` | `src/handlers.rs` |
| `src/ingest.rs::search_text` | `src/sessions.rs` |
| `src/search.rs` | `src/handlers.rs` |
| `src/get.rs` | `src/handlers.rs` |
| `src/transport.rs` | `src/transport.rs` (kept; absorbs namespace defaulting + MCP render note) |
| `src/adapter/mod.rs` | `src/adapter/mod.rs` (kept; absorbs `discovery.rs`) |
| `src/adapter/claude_code.rs` | unchanged |
| `src/adapter/codex.rs` | unchanged |
| `src/discovery.rs` | `src/adapter/mod.rs` |
| `src/embed.rs` | `src/embed/mod.rs` + `src/embed/qwen3.rs` |

End state: 10 files, 8 top-level entries.

## 4. Stage sequence

Each stage is one committable, self-contained unit. Compiles and tests green
at every commit. Order is bottom-up: clean substrate first, build sessions on
it, handlers on sessions, transport on handlers.

### Stage 0 - scaffold

- This file lands in `docs/refactor.md`.
- Branch: `refactor/8-entries`.
- No code change.

### Stage 1 - lib.rs: cross-cutting

Introduce in `src/lib.rs`:
- `pub trait Clock { fn now(&self) -> DateTime<Utc>; }` + `SystemClock` default.
- `pub mod output` - the single helper from 2.1.1 (stdout vs JSON-RPC frame).
- `pub enum Error` + `From` impls - the domain error used internally.

Leave `RetryPolicy` in `substrate.rs` for now (moves in Stage 2). Existing
re-exports stay.

Touches: `lib.rs` only. No consumer changes yet.

### Stage 2 - substrate: extract sessions DTOs, kill SQL helpers

Move out of `substrate.rs` into `sessions.rs` (file created here):
- `PendingMessage` + `pending_messages_stream`
- `MessageMeta` + `message_metas_by_ids` + `session_message_counts`
- `MessageWrite` + `UpsertStatus` + the upsert helpers that consume them
- All Arrow schemas from `datasets.rs` (delete `datasets.rs`)
- All batch builders and row extractors from `datasets.rs`
- `search_text` extraction (moved from `ingest.rs`)
- Ordering-contract validator (`IngestValidator`, from `ingest.rs`)
- Denorm stamping rules

In `substrate.rs`:
- Introduce `Predicate` enum:
  ```rust
  pub enum Predicate {
      Eq(&'static str, ScalarValue),
      IsNull(&'static str),
      In(&'static str, Vec<ScalarValue>),
      Between(&'static str, ScalarValue, ScalarValue),
      LikeContains(&'static str, String),
      And(Vec<Predicate>),
      Or(Vec<Predicate>),
  }
  ```
  with `to_lance(&self) -> lance::expr::Expr` (or whichever Lance type compiles
  the scalar predicate today).
- Replace every call site of `sql_string`, `sql_like_contains`,
  `embedding_identity_filter`, `sql_in` with `Predicate` construction.
- Delete the SQL helpers (honors 2.3 invariant 7).
- Add `scanner_with_prefilter(handle, predicate)` - always sets
  `prefilter(true)` per 3.3.
- Move `RetryPolicy` to `lib.rs`.
- Formalize `refresh_if_stale(handle)` from 2.3 inv 4.
- Split `Maintenance::{cleanup, optimize}` actions cleanly.

After this stage `substrate.rs` is ~400-500 LOC, no domain types, no SQL.

Acceptance: `Scanner::explain_plan` test asserts every search predicate
appears as a `ScalarIndexQuery`/`ScalarIndexExec` node, not a top-level
`FilterExec` (3.3 prefilter-pushdown invariant).

### Stage 3 - sessions.rs: Store facade

Build `sessions::Store` as the consumer API:
- `upsert_session / upsert_messages / upsert_parts / upsert_embeddings`
- `get_session`, `get_message_context`
- `fts_for_messages(Query) -> Vec<(MessageId, f32)>`
- `vector_for_messages(Query) -> Vec<(MessageId, f32)>`
- `message_metas_by_ids`, `pending_stream`
- `ensure_indices` (calls `embed::index_params` for the vector index)
- `iter_session(session_id) -> impl Stream<Item = (Message, Vec<Part>)>` -
  used by both `pond_get` full-session reads and `pond_session_events` SSE
  (seam 18 in the audit).

All return canonical types (`Session`, `Message`, `Part`); `Stored*` shapes
become internal to sessions.rs.

Touches: sessions.rs (definition), substrate.rs (callers from store dropped
or wrapped).

### Stage 4 - embed/: split into module dir

Create `src/embed/`:
- `mod.rs`: `EmbedBackend` trait, `EmbedWorker`, `Device` selection,
  `index_params(model) -> VectorIndexParams` (moved from substrate's
  `ensure_embedding_indices`), hf-hub fetch wrapper.
- `qwen3.rs`: today's `Qwen3Embedder`.

Delete `src/embed.rs`.

Touches: substrate.rs (delete `ensure_embedding_indices` body),
sessions.rs::Store (calls `embed::index_params` when creating vector index).

### Stage 5 - wire.rs: collapse types.rs, canonical projection

- Merge `src/types.rs` into `src/wire.rs`.
- Drop `StoredSession` / `StoredMessage` from any wire-facing surface.
- `pond_get` returns canonical `Session` + `Vec<Message>` + `Vec<Part>` per
  3.6 ("Response shape carries the canonical Session/Message/Part types
  verbatim").
- ErrorEnvelope and ErrorCode stay; add `From<Error>` for `ErrorEnvelope` as
  the one-and-only mapping (seam 10 in the audit).
- `options: ProviderOptions` codec helpers live here (seam 15).

Delete `src/types.rs`.

Touches: wire.rs (definition), sessions.rs (read-side projection
`StoredMessage -> Message`), handlers.rs callers.

### Stage 6 - handlers.rs: consolidate

Merge `src/search.rs` + `src/get.rs` + `src/ingest.rs` (handler half) +
`pond_session_events` into `src/handlers.rs`:
- `pond_search`, `pond_get`, `pond_ingest`, `pond_session_events`.
- Introduce `trait Retriever` + `FtsRetriever` + `VectorRetriever` in-file.
  Stringly-typed `"vector"`/`"fts"` -> `enum RetrieverKind` serialised to the
  wire strings only at projection time.
- RRF + recency boost + min_score as pure functions taking `&dyn Clock`.
- Single `fn map_error(e: Error) -> ErrorEnvelope` is the only path from
  domain error to wire envelope.
- Filter building constructs `substrate::Predicate`, never strings.

Delete `src/search.rs`, `src/get.rs`, `src/ingest.rs`.

### Stage 7 - adapter/: absorb discovery

Move `src/discovery.rs` (interactive picker + `prompt_and_persist`) into
`src/adapter/mod.rs`.

Delete `src/discovery.rs`.

Touches: adapter/mod.rs, main.rs (import path change), call sites that did
`use crate::discovery::*`.

### Stage 8 - transport.rs: namespace + render divergence

In `transport.rs`:
- Namespace `Option<String>` -> default-to-`"local"` lives here, before
  dispatch to handlers (seam 5 in the audit).
- HTTP/MCP Part placeholder render divergence (3.6.3) stays in the MCP
  adapter inside transport.rs, not in handlers.

Touches: transport.rs only; handlers.rs gets a non-Optional namespace.

### Stage 9 - main.rs: shrink

- Move `resolve_model` -> `config.rs`.
- Keep `spawn_maintenance` (the scheduler) in main.rs; the actions it calls
  are `substrate::Maintenance::{cleanup, optimize}`.
- Keep CLI verb bodies.

Result: `main.rs` is dispatch + verb bodies only, ~300 LOC.

### Stage 10 - verify

- `cargo test --locked` on Linux and macOS.
- `cargo clippy -- -D warnings`.
- `cargo bench --bench embed_bench -- --help` compiles.
- Synthetic ingest + search against `tests/fixtures/session-samples/claude-code/`.
- Run `pond serve`, drive `pond_search` + `pond_get` + `pond_ingest` end-to-end
  against a real Lance dataset.
- Diff `design.md` claims against new code paths; confirm the seam table in
  Section 6 below maps 1:1 to code locations.

## 5. Risks

- **Stage 2 (Predicate)**: Lance scalar predicate API surface must cover every
  current filter shape. If `LikeContains` doesn't push down as a Lance
  predicate, document it as an explicit postfilter case rather than smuggling
  SQL back in. Verify via `Scanner::explain_plan`.
- **Stage 3 (Store)**: returning canonical types means a read-side projection
  joining `sessions` + `messages` + `parts`. Today the code returns storage-
  shape `StoredMessage` directly. Watch perf - mitigate by batching the parts
  query keyed on `IN (message_ids)`.
- **Stage 5 (wire == canonical)**: every external client (CLI verbose output,
  tests, MCP tool schema) must switch to canonical types. Grep
  `StoredSession|StoredMessage` before starting.
- **Stage 1 (Clock)**: every test that asserts against recency boost has an
  implicit time dependency today. Audit and fix when the trait lands.
- **Lance prefilter coverage**: design "no SQL" commitment assumes Lance
  scalar predicates cover every filter pond needs. If a rare shape doesn't
  push down, Stage 2 needs a documented postfilter escape hatch named in
  `Predicate` (e.g. `Postfilter(...)`) so the cost is visible.

## 6. Seam ownership (post-refactor)

Every seam from the audit has one named home. This table is the acceptance
criterion for Stage 10.

| Seam | Owner |
|---|---|
| Clock / `now()` | `lib.rs` |
| Predicate (typed; replaces SQL strings) | `substrate.rs` |
| Retriever trait | `handlers.rs` |
| `search_text` concatenation policy | `sessions.rs` |
| Namespace defaulting | `transport.rs` |
| Per-namespace config resolution | `config.rs` |
| Maintenance scheduler vs actions | scheduler `main.rs`; actions `substrate.rs` |
| HF Hub fetch / progress | `embed/mod.rs` |
| Output channel (stdout vs JSON-RPC) | `lib.rs::output` |
| Error mapping (domain -> wire code) | `handlers.rs` (codes in `wire.rs`) |
| HTTP/MCP render divergence | `transport.rs` |
| Denormalization stamping | `sessions.rs` |
| Schema startup check | `sessions.rs` |
| Lance row -> canonical projection | `sessions.rs` |
| `options` JSON codec | `wire.rs` |
| Blob v2 encode/decode | `substrate.rs` |
| Stable row IDs (`_rowid` joins) | `substrate.rs` (see open question 1) |
| Session iterator (get + SSE) | `sessions.rs::iter_session` |
| Adapter discovery vs registration | `adapter/mod.rs` |
| Live-write activation surface | `adapter/mod.rs` + `handlers.rs` |

## 7. Open questions (resolve before Stage 2)

1. **Stable row IDs**: design 3.2.0 mandates `enable_stable_row_ids = true`
   for `_rowid` joins, but no current code path joins on rowid. Either keep
   the invariant and document the intended future use, drop the invariant,
   or wire up a real rowid-join (e.g. parts -> embeddings) during this
   refactor. Decision affects substrate Stage 2 scope.
2. **`spawn_maintenance` location**: confirmed staying in `main.rs` (lives
   with `serve`). Alternative was `sessions.rs`; rejected because
   scheduling is a process concern, not a store concern.
3. **`pond_session_events` SSE**: HTTP-only in v1 per 3.6.5. Refactor
   introduces no new transport for it. Confirm.
4. **Test fixture compatibility**: do existing fixture tests assert against
   `StoredMessage` shape directly? If yes, Stage 5 rewrites them. Audit
   first (`grep -rn 'StoredMessage\|StoredSession' tests/`).
5. **`Predicate::Postfilter` escape hatch**: include from the start, or only
   when the first un-pushable case appears? Lean toward including from the
   start so future cases have a typed home rather than smuggling SQL back.

## 8. Out of scope

- Workspace split (deferred per 2.1 until resources consumer ships).
- A second `EmbedBackend` impl (Qwen3 is the only one until 4 activates).
- A second consumer module beside `sessions.rs`.
- Any 4 deferred feature.
- Performance work beyond preserving current parity.
- Re-spec'ing anything in `design.md`. If the doc and code disagree, code
  changes; doc is the source of truth.

## 9. Acceptance

- All four canonical operations (`pond_search`, `pond_get`, `pond_ingest`,
  `pond_session_events`) pass current tests.
- `cargo test --locked` green.
- `cargo clippy -- -D warnings` green.
- `substrate.rs` < 500 LOC; no `sql_*` helpers anywhere; no `Stored*` types
  cross the wire layer.
- The seam ownership table in Section 6 maps 1:1 to code locations.
- No new comments beyond what existing files already carry.
