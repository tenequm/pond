# pond mcp / pond serve - 500 MiB Memory Budget

## How to use this document

This is the committed-to design for fitting a steady-state `pond mcp` (or
`pond serve`) process under a 500 MiB RSS ceiling on local-FS pond installs.
Every decision in Section 3 was settled in design review and is binding; the
staged work in Section 6 is how it gets built. The measurements in Section 2
came from `benches/serve_mem_bench.rs` against the real
`~/.local/share/pond/` corpus (8,168 sessions / 1,318,285 messages /
799,667 parts; 5.2 GiB on disk).

Status: design complete, implementation not started. Line and symbol
references are snapshots - verify against current code before acting.

## 1. Problem

A long-running `pond mcp` instance on the author's machine reached **1.17
GiB RSS** after ~2 h of intermittent search activity. With several Claude
Code sessions each holding their own MCP server, this multiplies linearly
across the laptop. The target is 500 MiB per instance so eight concurrent
sessions stay under 4 GiB total.

Measured baseline today (read-only, real corpus, sequential queries):

```
phase          start_M    end_M   peak_M  gpeak_M   p50_ms   p95_ms  max_ms
cold_open        14.9     29.8     29.8     29.8        8        8       8
fts_warm         29.8    744.6    744.6    744.6      428      428     428
fts_steady      744.6    792.4    792.4    792.4       94      100     100
first_hybrid    792.4   1319.1   1706.0   1706.0    11813    11813   11813
hybrid_warm    1319.1   1340.1   1340.1   1706.0      888      888     888
hybrid_steady  1340.1   1350.6   1350.6   1706.0      898      906     906
get_calls      1350.6   1402.8   1494.7   1706.0      817     1287    1287

PEAK RSS  1706.0 MiB   target 500 MiB   FAIL (-1206 MiB)
```

Decomposition of the steady-state hybrid floor (~1350 MiB):

| Component                              | MiB |
|----------------------------------------|----:|
| Process baseline (Rust + tokio)        |  30 |
| E5 embedder resident                   | 431 |
| Lance metadata cache (default 1 GiB)   | 750 |
| Lance index cache (warm IVF_PQ)        |  70 |
| parts.lance metadata pressure          |  ~80 |
| Scan transients                        |  ~50 |

The candle FP16 embedder isolated under `--probe-embedder` revealed that
416 of the 431 MiB resident is held by the **host system allocator after
`candle_core::safetensors::load` drops its 470 MiB FP32 `Vec<u8>`**
(`std::fs::read` at `safetensors.rs:401`). Forward passes do not grow RSS
further (steady state is stable). Dropping the embedder reclaims only
~59 MiB; a subsequent reload makes RSS strictly worse (911 MiB resident,
1300 MiB peak). So "evict on idle and reload on demand" is not viable
with the candle path.

## 2. Decisions (settled in review)

Three independent levers compose to fit the budget. Each is useful alone;
each attacks a different line item. Together they overshoot the target
with headroom for spikes.

| # | Topic | Decision |
|---|---|---|
| - | Approach | Three workstreams: (A) read-path redesign so `pond_search` and `pond_get` touch the minimum tables and bounded data, (B) Lance metadata/index cache caps, (C) split embedder paths - candle FP16 stays on write (initial sync throughput is load-bearing), ONNX int8 returns for query-only paths. |
| - | Constraint | Write path stays exactly as today. `pond sync` / `pond embed` throughput must not regress. |
| - | Non-constraint | Originally stated "no onnxruntime" (Stage 0). Re-opened: the binary-size cost (~70-80 MB libonnxruntime + ~118 MB bundled int8 weights) is acceptable given the ~250-300 MiB resident saving per `pond mcp` and per-instance multiplier on multi-session laptops. |
| Q1 | `pond_search` table touches | Stays messages-only (already true; no change). Drops `full` flag. Snippet size grows from 400 -> 600 chars centered on first informative query term (existing `query_snippet` logic). |
| Q2 | `pond_get` default response | Body is `messages.search_text`, not the nullable `content` column. parts.lance is **not** touched unless `include_parts=true`. Drops `include_thinking` and `include_tool_results` flags (also in `pond_session_events`). |
| Q3 | `pond_get` pagination | Cap each response at ~10000 tokens (~40000 chars; chars/4 estimator). Cursor-based: cursor is the `message_id` of the next message to return. Responses carry `has_more: bool` and `next_cursor: Option<String>`. Single-message overflow returns the message alone rather than truncating mid-message. |
| Q4 | Lance cache caps | `Session::default()` replaced by `Session::new(index_cache_size_bytes, metadata_cache_size_bytes, store_registry)` plumbed through `Handle::open_with_options`. Caps come from a new `[runtime]` config block with backend-aware defaults: local FS gets tighter caps, remote object stores stay near the Lance defaults. |
| Q5 | Initial cap targets (local FS) | metadata=128 MiB, index=256 MiB. Calibration is empirical via `benches/serve_mem_bench.rs --cap-sweep`; these are the starting points, not final values. |
| Q6 | Embedder split | Two `EmbedBackend` impls coexist. Write path (`pond embed`, `pond sync` background embed worker) keeps the current candle FP16 XLM-RoBERTa on Metal. Query path (`pond mcp`, `pond serve`, `pond search` CLI) uses ONNX int8 multilingual-e5-small via fastembed-rs's `UserDefinedEmbeddingModel`. |
| Q7 | Query embedder source | Bundled via `include_bytes!` of the int8 ONNX (`Xenova/multilingual-e5-small/onnx/model_quantized.onnx`, 118 MB on disk). No runtime download for the query path. Write path keeps the existing `hf-hub` download flow for the FP16 candle weights. |
| Q8 | Vector-space compatibility | Verified by historical precedent: commit `0736209 refactor(embed): run e5-base on Candle/Metal` documents A/B-tested cosine = 1.000000 between fastembed ONNX FP32 and candle FP32 for the same e5 checkpoint. Int8 vs FP16 of the same checkpoint drops cosine to ~0.985-0.998, typically <1% recall@10 hit on high-resource languages. No re-embedding of stored rows required. |
| Q9 | Idle eviction (query embedder) | After the swap, ORT's allocator does not retain the model bytes the way candle's heap does. Idle-evict after N minutes becomes viable (recoverable ~150 MiB on idle MCP processes). Not in v1 scope; documented as a follow-up. |
| Q10 | parts.lance lazy-open | parts.lance dataset opens lazily on the first `pond_get(include_parts=true)` rather than eagerly at `Handle::open_with_options`. Saves cold-open metadata pages and the file handle on MCP processes that only serve `pond_search` and content-only `pond_get`. |

## 3. Target budget under the locked design

| Component                       | Today  | After all three workstreams |
|---------------------------------|-------:|----------------------------:|
| Process baseline                |  30    |  30                         |
| Embedder resident               | 431    | 150 (ONNX int8 in-binary)   |
| Lance metadata cache (capped)   | 750    |  64                         |
| Lance index cache (capped)      |  70    | 128                         |
| parts.lance metadata pressure   |  80    |  10 (lazy)                  |
| Scan transients                 |  50    |  50                         |
| **`pond mcp` steady RSS**       | **1340 MiB** | **~430 MiB**          |
| **First-hybrid peak**           | **1706 MiB** | **~550 MiB**          |

Margin to the 500 MiB target: ~70 MiB steady, ~50 MiB peak. Tight at peak;
the cap sweep in Q5 may push index_cache down to 192 or 224 MiB if the
elbow allows it without latency regressions.

## 4. Wire-level changes

### `SearchRequest` (`src/wire.rs:366-401`)

- **Drop** `full: bool` field and its serde default.

### `SearchResponse` snippet behavior (`src/handlers.rs:1088-1092, 1629-1703`)

- `HIT_SNIPPET_CHARS: usize = 400` -> `600`.
- Delete the `full=true` branch of `hit_payload` (lines 1641-1647).
- Delete `HIT_TEXT_FULL` constant.

### `GetRequest` (`src/wire.rs:306-325`)

```
// Before
pub struct GetRequest {
    protocol_version, namespace,
    session_id, message_id, up_to,
    context_depth, max_messages,
    include_thinking, include_tool_results,
}

// After
pub struct GetRequest {
    protocol_version, namespace,
    session_id, message_id, up_to,
    context_depth, max_messages,
    include_parts: bool,                   // default false
    cursor: Option<String>,                // default None
}
```

### `GetResponse` (`src/wire.rs:327-347`)

- Add `has_more: bool`.
- Add `next_cursor: Option<String>` (present iff `has_more`).

### `SseEvent` / `pond_session_events` (`src/handlers.rs:615-742`)

- Drop `include_thinking` and `include_tool_results` from the request shape
  and rendering path. Live session events emit canonical-content placeholders
  uniformly; agents that need parts get them via `pond_get(message_id,
  include_parts=true)`.

### `[runtime]` config (`src/config.rs`)

New block with backend-aware defaults:

```
[runtime]
# Optional. Local-FS defaults: index=256 MiB, metadata=128 MiB.
# Remote (s3://, gs://, az://) defaults: index=2 GiB, metadata=512 MiB.
# Set explicitly to override. Lance ships 6 GiB / 1 GiB defaults; pond's
# defaults are calibrated for typical agent-session workloads where the
# metadata cache fills faster than the index cache.
index_cache_bytes = "256 MiB"
metadata_cache_bytes = "128 MiB"
```

### MCP / CLI / HTTP shims

- `transport.rs`: rmcp tool descriptions regenerated for `pond_get` (new
  flags, pagination guidance), `pond_search` (no `full`).
- `main.rs`: `pond search --full` CLI flag deleted; `pond get` grows
  `--include-parts`, `--cursor`.

## 4.1 Wire-shape cleanups (lands with Section 4 in the same commit)

These trim agent-facing JSON to what is load-bearing for agent action. All
are breaking changes, but pond is pre-release and `PROTOCOL_VERSION` bumps
freely. They land in the same commit as the Section 4 changes; tests are
updated in A6 (Section 6, Workstream A).

### `pond_search` group shape (grouped mode, default)

Before:

    { session_id, best_hit_message_id, project, source_agent,
      first_timestamp, last_timestamp, message_count, text, snippet,
      best_score }

After:

    // single-hit group (the common case):
    { session_id, best_hit_message_id, project, source_agent,
      first_timestamp,         // matched-hit timestamp
      session_messages_count,  // was message_count
      text,                    // 600-char centered snippet
      best_score }             // normalized to [0.0, 1.0]

    // multi-hit group (when matches span more than one timestamp):
    { session_id, best_hit_message_id, project, source_agent,
      first_timestamp,         // earliest matched-hit timestamp
      last_timestamp,          // latest matched-hit timestamp (only emitted when > first)
      session_messages_count,
      text,
      best_score }

- `first_timestamp` always present (earliest matched-hit timestamp).
- `last_timestamp` is `Option<DateTime<Utc>>` with
  `#[serde(skip_serializing_if = "Option::is_none")]`, emitted only
  when `last > first`. Single-hit groups collapse to one timestamp
  on the wire; multi-hit groups preserve the span (the demo run hit
  a 12-hour span on one group), which agents use to disambiguate
  "which version of this conversation do I drill into."
- Agents that want a single "how recent" signal:
  `last_timestamp.unwrap_or(first_timestamp)`. Agents that want the
  span: check whether `last_timestamp` is present.
- `message_count` rename makes the meaning load-bearing on the wire (it
  is whole-session count, not matched count - `sessions.rs:1188-1212`).
- `snippet` field drops; the 600-char windowed body is `text`.
- `best_score` divides by `FTS_FUSION_WEIGHT + VECTOR_FUSION_WEIGHT +
  RECENCY_MAX_BOOST = 0.135 + 1.0 + 0.05 = 1.185` at the response-shaping
  boundary in `build_groups` (`handlers.rs:1099-1111`). Internal ranking
  is unaffected because every score in a result set gets divided by the
  same constant.

### `pond_search` hit shape (ungrouped mode, `group_by_conversation=false`)

Before:

    { session_id, message_id, project, source_agent, timestamp, text,
      best_score, matched_via }

After:

    { session_id, message_id, project, source_agent, timestamp, text,
      score }                  // best_score renamed; matched_via dropped

- `best_hit_message_id` does not appear (would tautologically equal
  `message_id`).
- `score` rename matches the absence of a "best" qualifier in single-hit
  mode.
- `matched_via` drops. Which retriever ranked a hit is operator-tooling
  info; agents do not tune retrievers (hybrid mode is server-decided).
  If diagnostic surface is needed, expose it via `pond search --debug`
  on the CLI only.
- `base_score` and `recency_boost` drop. Today's ungrouped response
  exposes the unscaled fusion score and recency contribution as
  separate fields (observable via `pond_search(group_by_conversation=
  false)`). Pure operator/debug surface; the normalized `score` is what
  agents act on.

### `pond_get` default response

Before (`include_parts=false` is new; this is today's response with all
fields):

    { session: { id, parent_session_id, parent_message_id, source_agent,
                 created_at, project, options: {...} },
      messages: [
        { id, session_id, timestamp, role, options: {...}, ... },
        ...
      ],
      parts: [...] }

After (default; `include_parts=false`):

    { session: { id, source_agent, project, created_at },
      messages: [
        { id, role, timestamp, text },
        ...
      ],
      has_more, next_cursor }

- `session.options` drops. Its fields are redundant: `source.adapter` ==
  `source_agent`; `source.workspace_path` == `project`; `source.project_dir`
  is the same path with hyphens; `source.version` is pond's adapter
  version (operator-only).
- `session.parent_session_id` / `session.parent_message_id` drop. Restore
  tooling goes through `restore_lineage` (separate handler), not
  `pond_get`.
- per-message `session_id` drops. The response is session-keyed already;
  repeating it on every message is pure overhead (36 B * N).
- per-message `options` drops. Largest per-message metadata bucket
  (provider usage stats, source.cwd, source.git_branch, source.raw_record,
  parent_uuid, etc.) and not acted on by agents.
- `text` is the body sourced from `messages.search_text` (per Q2). null
  search_text rows render as empty string.

### Size impact (typical 50-message session-scope `pond_get`)

| Layer                 | Today | Trimmed |
|-----------------------|------:|--------:|
| session header        | ~300 B | ~150 B  |
| per-message metadata  | ~500 B * 50 = 25 KB | ~120 B * 50 = 6 KB |
| message text (body)   | varies | varies  |
| **metadata overhead** | **~25 KB** | **~6 KB** |

~75% reduction in non-content overhead, compounding with the 10k-token
pagination budget so more content fits per page within Claude Code's
context warning threshold.

### `pond_session_events` SSE shape

Same content-stripping principles apply: drop `include_thinking` and
`include_tool_results` toggles (Section 4); drop per-event `session_id`
and per-event `options` for symmetry with `pond_get`. Live events emit
canonical message stubs uniformly. Full-fidelity event replay (with parts
and metadata) is not in scope here; if needed later it gets its own path.

### `PROTOCOL_VERSION` bump

`PROTOCOL_VERSION` (`src/wire.rs`) bumps to mark the break. Pre-release;
no migration shim.

## 4.2 Cursor is self-contained

Agents make mistakes. `pond_get(cursor="...")` without a re-supplied
`session_id` or `message_id` should still work - the cursor carries the
originating query context so the server can infer what was intended.

Cursor encoding (opaque base64 of a small JSON shape, the agent never
parses it):

```
{ "scope": "session" | "message",
  "anchor_id": "<session_id or message_id from the originating request>",
  "after_message_id": "<next message to start at>",
  "include_parts": true|false }
```

- When `cursor` is set, `session_id` / `message_id` / `include_parts` on
  the request are ignored if they conflict; the cursor wins (it's
  continuation semantics).
- When `cursor` is set and the agent did NOT pass any of the originating
  fields, the server decodes the cursor and proceeds. No error.
- When `cursor` is set alongside a *consistent* originating field, the
  request is honored as-is.
- Cursor format is opaque to clients; bumping `PROTOCOL_VERSION` is the
  invalidation path if the schema ever changes.

### What stays as forward-compat seam

`SearchRequest.namespace` and `GetRequest.namespace` (`Option<String>`)
stay. Pond is single-namespace in v1 (`spec.md#namespace-resolution`);
the field is a forward-compat seam per CLAUDE.md (`never cut a forward-
compatibility seam, they exist precisely so they are not "simplified
away"`).
- HTTP server (`transport::http`): request validation updated; response
  serialization includes pagination fields.

## 5. Storage-side changes

### `Handle::open_with_options` (`src/substrate.rs:532-628`)

Replace the single line

```rust
let session = Arc::new(Session::default());
```

with a constructor that pulls the caps from the new `[runtime]` config
block (threaded through as a parameter on `open_with_options`), routed
through `Session::new(index_cache_size_bytes, metadata_cache_size_bytes,
store_registry)`. The `store_registry` stays at `ObjectStoreRegistry::
default()`. Backend-aware defaults live in a helper that mirrors the
existing `apply_remote_storage_defaults` (`substrate.rs:1554`) shape.

### Lazy-open `parts.lance`

`DatasetSet` (`substrate.rs:498-502`) becomes:

```
struct DatasetSet {
    sessions: Mutex<CachedDataset>,
    messages: Mutex<CachedDataset>,
    parts:    Mutex<Option<CachedDataset>>,   // None until first include_parts
}
```

A `parts_dataset` accessor on `Handle` opens-on-demand with the same OCC
retry policy as the existing two. The opened dataset is then cached for
the lifetime of the `Handle`.

### `Store::get_message_context` and `Store::get_session` pagination

Both grow `cursor: Option<String>` + `budget_chars: usize` parameters. The
implementation accumulates messages chronologically (or context-window
order for `message_id` scope), stopping when the next message would push
the budget past `budget_chars`. Returns
`(messages, next_cursor: Option<String>)`. Token budget (~10000 tokens) is
applied as `40000 chars`.

When `include_parts=false`, the body for each returned message is
`messages.search_text` projected explicitly. parts.lance is not opened
or scanned.

When `include_parts=true`, parts.lance opens lazily (Q10) and parts for
each returned message are fetched alongside.

## 5.1 Single shared message-list helper

Three `Store` methods today fan out to the same underlying scan-and-
materialize logic on `messages.lance`:

| Site (`src/sessions.rs`) | Purpose                                  |
|--------------------------|------------------------------------------|
| `messages_for_session:1518` | All messages (+ parts) for a session   |
| `get_message_context:780`   | Single message + N siblings each side  |
| `get_session:672`           | Session header + `messages_for_session` |

`get_session` calls `messages_for_session`. `get_message_context` calls
`messages_for_session` then slices in memory. So `messages_for_session`
is the bottom of the stack for every list-returning read path.

**Refactor target**: collapse to one shared helper with the signature

```rust
async fn paged_messages(
    session_id: &str,
    cursor: Option<&Cursor>,   // None = start from beginning or anchor
    anchor: MessageAnchor,     // Whole | Window { around: &str, depth: usize }
    budget_chars: usize,       // ~40_000 (== 10k token budget)
    include_parts: bool,
) -> Result<PagedMessages>
```

returning `(messages: Vec<MessageView>, next_cursor: Option<Cursor>)`.
The three call sites become thin wrappers:

- `messages_for_session` -> `paged_messages(session_id, cursor, Whole,
  budget, include_parts)`
- `get_message_context` -> `paged_messages(session_id, cursor,
  Window{around=message_id, depth=context_depth}, budget, include_parts)`
- `get_session` -> unchanged signature; calls `paged_messages` for the
  message body, builds the session header separately.

`find_message:1495` is a single-row lookup (no list), stays separate.
`child_sessions:695` returns sessions, not messages; stays separate.

This is load-bearing for keeping pagination correctness in one place:
adding a budget check or cursor format change touches one function, not
three.

## 6. Implementation order

Three workstreams. Each is independently shippable and verifiable via the
bench. Recommended order: read-path first (smallest blast radius, no new
deps, biggest user-visible UX win), then Lance caps (one-line behavioral
change with bench-driven calibration), then embedder split (largest diff,
strongest gain).

### Workstream A - read-path redesign

| Step | Files | Notes |
|------|-------|-------|
| A1 | `src/wire.rs` | `GetRequest`/`GetResponse` shape changes; `SearchRequest` drops `full`. |
| A2 | `src/handlers.rs` (search) | Snippet constant -> 600; delete `full=true` branch + `HIT_TEXT_FULL`. |
| A3 | `src/handlers.rs` (get) | Default body = `search_text`; parts only on `include_parts=true`; cursor accumulator + `has_more`. |
| A4 | `src/handlers.rs` (session events) | Drop both render-toggle flags. |
| A5 | `src/transport.rs`, `src/main.rs` | Regenerate MCP / CLI flag surfaces. |
| A6 | tests | Update integration tests for new shape; add pagination tests. **See Section 6.1 for the test-consolidation rule** - new behavior tests land against `pond_search` / `pond_get` handlers in `tests/integration/search.rs`; surface integration tests in `transport_http.rs` and `transport_mcp.rs` collapse to smoke ("the request reaches the handler with field X populated"). |
| A7 | docs/spec.md | Reflect read-path changes in §6-7 surface description. |
| A8 | benches/backend_bench.rs | Update to the new `SearchRequest` shape; drop `full=true` test arms. |

## 6.1 Surface inheritance and test consolidation

pond has three caller surfaces for `pond_search` / `pond_get`:

| Surface | Today | Target |
|---------|-------|--------|
| HTTP    | `Json<SearchRequest>` -> pass through (`transport.rs:123-126`) | unchanged; already pure inheritance |
| MCP     | Separate `McpSearchParams` / `McpGetParams` (`transport.rs:313+, 371+`) translated into `SearchRequest` / `GetRequest` | collapse: the rmcp tool deserializes directly into `SearchRequest` / `GetRequest`. Field renames (e.g. MCP's `conversation_id` -> wire's `session_id`) live as `#[serde(alias = "conversation_id")]` on `session_id`. |
| CLI     | Per-command clap structs (`main.rs:639+, 737+`) hand-build a `SearchRequest` / `GetRequest` | thin clap wrappers populate `SearchRequest` / `GetRequest` and pass through; no per-surface validation logic. |

**Principle**: the wire types own the contract. Surface modules deserialize
(MCP, HTTP) or parse args (CLI) into the wire types and call the handler.
Behavior (filter validation, mode resolution, default-namespace handling,
the dropped-`full`-flag, the new cursor-self-containment rule from
Section 4.2) lives in the handler.

**Test consequence**: today behavior is exercised three times -
`tests/integration/search.rs` (handler-level), `tests/integration/
transport_http.rs` (HTTP surface), `tests/integration/transport_mcp.rs`
(MCP surface). With surfaces collapsed to pass-through, the same behavior
test runs once at the handler level. Surface tests collapse to "the
request was deserialized correctly and reached the handler" smokes.

| Test file | Before | After |
|-----------|--------|-------|
| `search.rs` | Per-handler behavior (filters, modes, snippets, pagination, recency, recall) | unchanged - this is the canonical surface; gains coverage for the new shape (snippets, pagination, cursor-self-containment) |
| `transport_http.rs` | Re-runs all the handler behaviors over HTTP | shrinks to smoke: routes correctly resolved, `Json<SearchRequest>` deserializes, errors mapped to status codes |
| `transport_mcp.rs` | Re-runs all the handler behaviors over MCP | shrinks to smoke: rmcp tool registered, serde alias for `conversation_id` works, error envelopes serialize |

Net: one source of behavior coverage, lighter surface tests, no risk of
mode-flag / filter / default-namespace logic drifting across three places.
This is also why removing `pond search --mode` from main.rs's clap is
trivial - the surface doesn't define modes; the wire type and handler
do.

### Workstream B - Lance cache caps

| Step | Files | Notes |
|------|-------|-------|
| B1 | `src/config.rs` | New `[runtime]` block; parse `index_cache_bytes` / `metadata_cache_bytes` with `humansize`-style suffixes. |
| B2 | `src/substrate.rs:532-628` | Thread caps through `Handle::open_with_options`; `Session::new(...)` replaces `Session::default()`. |
| B3 | `src/substrate.rs` | `apply_remote_storage_defaults`-style helper for backend-aware cap defaults. |
| B4 | `benches/serve_mem_bench.rs` | Add `--cap-sweep` mode that runs the full workload at (32, 64, 128, 256, 512, 1024) MiB cap pairs and prints peak-RSS-vs-p50/p95-latency. |
| B5 | Calibrate | Run B4, pick the elbow on local FS. Update Q5 defaults if the data argues differently. |

### Workstream C - embedder split

Reference commits in git history for the structural shape of each path:

- **`d47b5d2 refactor(embed): migrate from qwen3 to e5-small`** is the exact
  prior wire-up of fastembed::TextEmbedding for the e5-small ONNX path.
  `src/embed/e5_small.rs` in that commit is the structural reference for
  the new ONNX query-side embedder. Key shapes: `fastembed` dep with
  `ort-download-binaries-rustls-tls` keeps the binary self-contained (no
  system libonnxruntime needed); `Mutex<TextEmbedding>` because
  fastembed's `embed` is `&mut self` while `EmbedBackend` is shared as
  `Arc<dyn>`.
- **`0736209 refactor(embed): run e5-base on Candle/Metal`** is the current
  candle-transformers wire-up. The commit message documents
  "Vectors are bit-identical to the prior ONNX path (cosine 1.000000 in
  a direct A/B)" - load-bearing evidence that ONNX FP32 and candle FP32
  for the same e5 checkpoint produce the same vector space. Int8 vs FP16
  introduces only quantization drift on top of an already-aligned space.

| Step | Files | Notes |
|------|-------|-------|
| C1 | New `src/embed/query_onnx.rs` | Adapt the historical `e5_small.rs` (`d47b5d2`) to use `UserDefinedEmbeddingModel`. Load via `include_bytes!` of the bundled int8 ONNX. CPU EP only (CoreML EP is documented-broken for BERT-class graphs - see microsoft/onnxruntime#22007, #14455, #22275; do not enable). Disable `enable_cpu_mem_arena` if cap-sweep shows it matters. |
| C2 | New `src/embed/write_candle.rs` | Rename the current `E5Embedder` (in `src/embed.rs`) into a focused write-path module. No behavioral change to the embedder itself - structural reorg only. |
| C3 | `src/embed.rs` | Becomes a router: `LazyEmbedder` picks the query backend in `pond mcp` / `pond serve` / `pond search`, the write backend in `pond embed` / `pond sync`. The selection is by call site, not by config (matches the existing `LazyEmbedder` pattern). |
| C4 | `Cargo.toml` | Add `fastembed` with `default-features = false, features = ["ort-download-binaries-rustls-tls"]` (the historical `d47b5d2` choice). Keep `candle-*` deps for the write path. |
| C5 | `assets/multilingual-e5-small-int8.onnx` | Bundled int8 weights (118 MB). One-time download from `Xenova/multilingual-e5-small/onnx/model_quantized.onnx`; commit the file via Git LFS or fetch-on-build into `target/` and `include_bytes!` from there. |
| C6 | `benches/serve_mem_bench.rs` | Add `--query-backend onnx|candle` arm so the bench measures both. Verify cosine drift on stored vectors (sample N hits from the existing `messages.lance`, embed the same query strings both ways, compare). |

## 7. Verification

### Bench coverage (already in `benches/serve_mem_bench.rs`)

- **`--probe-embedder`**: isolates the embedder load/run/drop lifecycle.
  Verify ONNX int8 backend lands at the expected 120-180 MiB resident vs
  candle's 431 MiB. Verify drop reclaims (vs the candle 59 MiB stuck).
- **Default mode (full hybrid workload)**: peak RSS under 500 MiB on the
  real `~/.local/share/pond/` corpus, with p50 hybrid latency within 2x of
  today's 898 ms.
- **`--cap-sweep`** (new in B4): chart for cap selection.
- **`--query-backend onnx|candle`** (new in C6): cosine drift on actual
  stored vectors + retrieval recall@10 agreement.

### Pass criteria

| Bench arm                                    | Pass condition                                  |
|----------------------------------------------|-------------------------------------------------|
| `--probe-embedder` (ONNX int8)               | resident <= 200 MiB, drop reclaims >= 80%       |
| Default hybrid bench                         | peak RSS <= 500 MiB on local FS                 |
| Default hybrid bench (latency)               | p50 hybrid <= 1500 ms (1.7x of today)           |
| `--query-backend onnx vs candle`             | mean cosine drift >= 0.985 on real corpus       |
| `--query-backend onnx vs candle` (recall@10) | Jaccard overlap >= 0.7 with candle FP16 path    |

If the cosine-drift or recall arm fails, the ONNX query swap is held back
and only workstreams A + B ship. That would land pond at ~700 MiB steady
- short of the 500 MiB target, but a major improvement over today's 1340.

## 8. What this does not cover

- **Object-store backends (S3/GCS/Azure)**: backend-aware cap defaults in
  B3 are sketched but not benchmarked. A separate calibration pass against
  a real S3-backed pond is needed before the remote defaults can ship.
  Until then, remote installs should keep the Lance defaults (1+6 GiB).
- **Idle eviction (Q9)**: ORT allocator behavior makes drop-and-reload
  safe (unlike candle), but the eviction timer, watchdog, and reload
  policy are out of scope here. Tracked separately.
- **Multilingual recall on low-resource languages**: int8 quantization
  drift is documented as worst on Yoruba (-0.07 NDCG@10) per the elastic/
  multilingual-e5-small-optimized model card. pond's primary workload is
  English/code; if multilingual coverage becomes load-bearing, revisit.
- **Binary size growth**: ~70-80 MB libonnxruntime + ~118 MB bundled int8
  ONNX = ~200 MB on top of today's pond binary. Acceptable for the
  Homebrew distribution story; explicitly re-opened (Section 2, third
  row) from the original Stage 0 "no onnxruntime" decision.
- **Backwards-compatibility for existing clients of the wire**: not in
  scope. pond is pre-release, no external clients; bumping
  `PROTOCOL_VERSION` is the only break-marker. CLAUDE.md: "Don't write
  migration notes or compatibility shims".

## 9. References

- Lance perf guide: `~/pjv/lance-format/lance/docs/src/guide/performance.md` -
  metadata cache (1 GiB default), index cache (6 GiB default), session
  sharing recommendation, scan memory model.
- LanceDB indexing reference: `~/pjv/lancedb/docs/docs/indexing/index.mdx`.
- candle Metal allocator (the FP32-Vec retention finding):
  `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/candle-core-0.10.2/src/metal_backend/device.rs:44-57`.
- ONNX `commit_from_memory` semantics: pykeio/ort#71 confirms ORT copies
  the model bytes into the session (not borrowed); `use_ort_model_bytes_
  directly` is opt-in for `.ort` format only.
- Historical pond embedder commits:
  - `d47b5d2 refactor(embed): migrate from qwen3 to e5-small` - ONNX wire-up
    reference; the historical `src/embed/e5_small.rs` is the shape to
    re-introduce for the query path.
  - `0736209 refactor(embed): run e5-base on Candle/Metal` - candle wire-up
    reference; documents cosine = 1.000000 A/B against the ONNX FP32 path
    for the same e5 checkpoint.
  - `dc1acfb perf(embed): load e5 weights at FP16 to halve resident model
    memory` - FP16 conversion already in place on the write path.
- Bench harness: `benches/serve_mem_bench.rs` - the measurement engine
  for everything in Section 7.
