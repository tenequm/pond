# MCP Tool Response Redesign - Single-Mode Search, Lean pond_get, Per-Tool Size Caps

## How to use this document

This is the committed-to design for one coordinated wire change covering both pond MCP tools (`pond_search` and `pond_get`) and their HTTP/CLI siblings. Every decision in Section 2 was reached in a design review and is binding except where Section 8 lists it as still open. The implementation order in Section 7 is how it gets built. Line and symbol references are snapshots - verify against current code before acting.

Status: design complete, implementation not started. CLAUDE.md authorizes breaking changes (pond is pre-release); no migration shim or changelog entry is owed. `docs/spec.md` sections 7-8 must be read and updated as part of this work.

## 1. Problem

Three coupled problems on the MCP surface:

**Tool output overflows Claude Code's warning/cap.** Claude Code warns at 10K tokens per MCP tool response and truncates at 25K tokens (default `MAX_MCP_OUTPUT_TOKENS=25000`, equivalent to a `bytes/4` chars cap of 100K). `pond_search` has no server-side budget at all; at `limit=200, group_by_conversation=false` it returns ~170KB / ~42K tokens, triggering Claude Code's `[OUTPUT TRUNCATED]` banner. `pond_get`'s budget exists (40_000 chars, ~10K tokens) but uses `text.chars().count()` rather than `text.len()`, so CJK/emoji content can serialize to ~3x the budgeted byte count.

**Response shape carries fields with no agent-facing value.** The `SearchResponse` envelope and `Hit`/`Group` rows include `request_id` (duplicate of JSON-RPC envelope `id`), `total` (duplicate of `result.{hits,groups}.len()`), `matched_via` (documented in `SCHEMA_DOC` but not implemented on the struct), `best_hit_message_id`+`best_score` (redundant with the first element of any matches list), `first_timestamp`/`last_timestamp` (conditionally emitted, agent rarely uses), and per-row `project`/`source_agent` that don't carry message-level signal. The grouped/ungrouped split via `SearchResultBody::{Hits,Groups}` adds a discriminated union that no agent intent actually needs once groups carry their own message-level evidence.

**Vocabulary drift across the surface.** The MCP request schemas use `conversation_id` while every response field, wire type, and internal handler uses `session_id`. `McpGetParams` uses `max_messages` while `McpSearchParams` uses `limit` for the same concept. Each translation layer is mental tax on the agent.

## 2. Decisions (settled in review)

| # | Topic | Decision |
|---|---|---|
| D1 | Token approximation | Use `bytes / 4` as the approximation. Matches Claude Code's own pre-check (verified in leaked `mcpValidation.ts`). Do not reuse the Lance ngram tokenizer (wrong domain - emits FTS character n-grams) or the e5/XLM-RoBERTa SentencePiece tokenizer from `src/embed.rs` (wrong vocab, gated behind embed lifecycle, configured with 512-token truncation that silently caps the count). |
| D2 | `pond_get` budget unit | Switch `text.chars().count()` to `text.len()` at `sessions.rs:882`. Bytes, not codepoints. Aligns with Claude Code's estimator on every script. |
| D3 | Per-tool size cap annotation | Declare `_meta["anthropic/maxResultSizeChars"]` in the `tools/list` response per tool. `pond_search`: 80_000. `pond_get`: 200_000. rmcp v1.7's `Tool.meta: Option<Meta>` field serializes to the wire shape Claude Code reads; the `#[tool]` macro does not expose it so a manual `list_tools` override is required. Hard ceiling per Anthropic docs is 500_000. Available in Claude Code v2.1.91+; older clients ignore the field. |
| D4 | Search mode collapse | Drop `group_by_conversation` from the request schema. Drop the `SearchResultBody::{Hits,Groups}` discriminated union. One shape, always grouped by session. The realistic agent intents that ungrouped previously served are equally served by grouped-with-top-3-matches-per-session - and the latter saves follow-up `pond_get` calls. |
| D5 | Matches per session | Up to 3 top-scoring matches per session, sorted score-desc. Fewer when fewer matches exist (no padding). No request flag; hardcoded constant `MAX_MATCHES_PER_SESSION: usize = 3`. Add a request flag the day a second caller needs it (per CLAUDE.md). |
| D6 | Field removals from response | Drop `total`, `request_id`, `first_matched_at`/`last_matched_at`, `matched_via`/`RetrieverArm`, `best_hit_message_id`, `best_score`. Each is either duplicate, derivable from `matches[0]`, an operator-only concern (belongs on `--explain`), or below the value-vs-bytes threshold. |
| D7 | Field renames | MCP request: `conversation_id` -> `session_id` (both `McpSearchParams` and `McpGetParams`). `max_messages` -> `limit` (both `McpGetParams` and the wire `GetRequest`). |
| D8 | Pagination on `pond_search` | Add `cursor: Option<String>` to `McpSearchParams`. Cursor is opaque base64url of a JSON `SearchCursor { query, similar_to, filters, offset }`. Resume by rank offset (re-runs ranking; result rank may shift if corpus updates between pages - documented in tool description). Same opaque-cursor pattern as `pond_get`. |
| D9 | Server-side budget gate on `pond_search` | `SEARCH_BUDGET_BYTES: usize = 60_000` (~15K tokens, leaves headroom under the 80_000-char `_meta` declared cap). After building the sorted `Vec<Session>`, iterate, sum serialized byte length, stop when crossing the budget; set `has_more = true` and emit `next_cursor` encoding the rank offset where truncation happened. The `limit` parameter and the byte budget both cap output; whichever bites first wins. |
| D10 | Wire shape uniformity | One `SearchResponse` shape across MCP, HTTP, and CLI transports. Operator-only retrieval mechanics (`matched_via` etc.) accessed via the existing `explain_search_plan` path (handlers.rs:1256) and `pond search --explain`. No transport-specific projection types. |
| D11 | Tests placement | `tests/integration/transport_http.rs` keeps placement (genuine cross-module integration: axum router + Store + corpus + serialization). `tests/integration/transport_mcp.rs` keeps placement but is rewritten to ~80 lines / 3 focused tests. Pure-transformation pieces (`to_error_data`, `_meta` injection, `json_result` shape) move to `#[cfg(test)] mod tests` inside `transport.rs`. Matches the CLAUDE.md placement rule: `tests/` reserved for genuine cross-module integration, unit tests live next to code. |

## 3. Final shapes

### 3.1 Response types (wire.rs)

These types are shared across MCP, HTTP, and CLI (per D10).

```rust
struct SearchResponse {
    sessions: Vec<Session>,
    matched_total: usize,           // total matched messages pre-limit (replaces `total`)
    has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    // see Section 8 for the open request_id decision
}

struct Session {
    session_id: String,
    project: String,
    source_agent: String,
    session_messages_count: usize,
    matched_message_count: usize,   // matches in this session (numerator vs session_messages_count)
    matches: Vec<SearchResult>,     // 1..=3 entries, sorted by score desc
}

struct SearchResult {
    message_id: String,
    role: Role,                     // enum already exists in wire.rs (used by GetMessage)
    timestamp: DateTime<Utc>,
    text: String,                   // 600-char centered window (HIT_SNIPPET_CHARS unchanged)
    score: f64,
}
```

Removed: `Hit`, `Group`, `SearchResultBody`, plus the `RetrieverArm` enum that was never landed.

### 3.2 Request types (wire.rs / transport.rs)

Wire `SearchRequest` keeps `protocol_version`, `namespace`, `mode_override` (consumed by HTTP/CLI). Drop `group_by_conversation` and `boost_recent`. Add `cursor: Option<String>`.

`McpSearchParams` (transport.rs:297):

```rust
struct McpSearchParams {
    query: Option<String>,           // required unless similar_to set
    similar_to: Option<String>,
    limit: Option<usize>,            // default 10
    project: Option<String>,
    session_id: Option<String>,      // renamed from conversation_id
    source_agent: Option<String>,
    role: Option<String>,
    from_date: Option<String>,
    to_date: Option<String>,
    cursor: Option<String>,
}
```

`McpGetParams` (transport.rs:350):

```rust
struct McpGetParams {
    message_id: Option<String>,
    session_id: Option<String>,      // renamed from conversation_id
    up_to: Option<String>,
    context_depth: Option<usize>,
    limit: Option<usize>,            // renamed from max_messages
    include_parts: Option<bool>,
    cursor: Option<String>,
}
```

Wire `GetRequest.max_messages` -> `GetRequest.limit`; helper `default_max_messages` renamed accordingly.

### 3.3 Search cursor format

Opaque base64url of:

```rust
#[derive(Serialize, Deserialize)]
struct SearchCursor {
    query: String,
    similar_to: Option<String>,
    filters: SearchFilters,
    offset: usize,                  // rank offset to resume after
}
```

Tool description states that result rank may shift between pages if the corpus updates - same caveat every search API has.

## 4. Byte budget (verification)

Per `Session` (no first/last timestamps, no matched_via):
- Metadata: ~190B (session_id 50, project 41, source_agent 28, two counts 62, JSON overhead 10)
- Per `SearchResult`: ~745B (message_id 50, role 14, timestamp 37, score 19, text 615, overhead 10)

Distribution-weighted (30% 1 match, 50% avg 2.5 matches, 20% capped at 3): ~1,590B/session.

Limits:

| Call | Bytes | Tokens (`bytes/4`) | Status |
|---|---|---|---|
| `limit=5, grouped` | ~8KB | ~2K | Trivially fits |
| `limit=10, grouped` | ~16KB | ~4K | Comfortable |
| `limit=20, grouped` | ~32KB | ~8K | Under 10K warning |
| `limit=30, grouped` | ~48KB | ~12K | Above warning, well under 25K cap |
| `limit=50, grouped` | ~80KB | ~20K | At cap edge; budget gate truncates |
| `limit=200, grouped` | ~320KB | ~80K | Budget gate truncates; paginated via cursor |

The budget gate from D9 (60_000 bytes) bites around `limit=30-40` in the typical case, sets `has_more: true`, returns `next_cursor`. Below that range, responses ride well under the warning. Above that, pagination is the contract.

## 5. Implementation surface

### 5.1 wire.rs

- New types: `Session`, `SearchResult`. Removed: `Hit`, `Group`, `SearchResultBody`.
- `SearchResponse`: drop `total`, add `matched_total`, add `has_more`, add `next_cursor` (Option). `request_id` per Section 8.
- `SearchRequest`: add `cursor: Option<String>`. Drop `group_by_conversation`, `boost_recent`. Keep `protocol_version`, `namespace`, `mode_override` (HTTP/CLI surface).
- `GetRequest.max_messages` -> `GetRequest.limit`. Rename `default_max_messages` -> `default_get_limit` (or similar reading-cleanly name).
- New `SearchCursor` struct (encode/decode helpers in handlers.rs).
- `Role` enum at wire.rs:351 is unchanged; `SearchResult` reuses it.

### 5.2 handlers.rs

- Remove the `if plan.group_by_conversation { ... } else { ... }` branch at handlers.rs:1488. Always call the rewritten session-builder.
- Rewrite `build_groups` (handlers.rs:1888) as `build_sessions`:
  - `Acc { project, source_agent, matched_count, matches: Vec<SearchResult> }` accumulator.
  - Increment `matched_count` every iteration.
  - After accumulation, sort each session's `matches` by score desc, truncate to `MAX_MATCHES_PER_SESSION = 3`.
  - Sort sessions by `matches[0].score` desc, truncate to `limit`.
- `matched_total` is `scored.len()` - free, available pre-truncation at handlers.rs:1488.
- Add `SEARCH_BUDGET_BYTES: usize = 60_000` constant. After session list is built and sorted, iterate sessions accumulating `serde_json::to_string(&session)?.len()`; stop when crossing the budget. Truncate the session list at that point, set `has_more`, encode `SearchCursor { ..., offset: truncate_at }`.
- Switch `sessions.rs:882` from `text.chars().count()` to `text.len()` (applies to pond_get's existing budget gate).
- Add cursor encode/decode for `SearchCursor` mirroring the existing pond_get cursor pattern (handlers.rs:878-904).
- Drop dead code: `best_hit_message_id`/`best_score` fields in the old `Acc`, `first_timestamp`/`last_timestamp` accumulation, `last_timestamp` conditional emission logic.

### 5.3 transport.rs

- `McpSearchParams` and `McpGetParams` (transport.rs:297, transport.rs:350): apply the field renames and the `boost_recent` / `group_by_conversation` drops. Pass `params.cursor` through on search.
- In the MCP handlers (transport.rs:404, transport.rs:453), update field reads (`params.conversation_id` -> `params.session_id`, `params.max_messages` -> `params.limit`).
- Override `list_tools` in the existing `#[tool_handler(router = self.tool_router)] impl ServerHandler for PondMcp` block at transport.rs:478:

```rust
async fn list_tools(
    &self,
    request: Option<PaginatedRequestParams>,
    context: RequestContext<RoleServer>,
) -> Result<ListToolsResult, ErrorData> {
    let mut result = self.tool_router.list_tools(request, context).await?;
    for tool in &mut result.tools {
        let chars = match tool.name.as_ref() {
            "pond_search" => 80_000,
            "pond_get"    => 200_000,
            _ => continue,
        };
        let mut obj = serde_json::Map::new();
        obj.insert("anthropic/maxResultSizeChars".to_owned(), serde_json::json!(chars));
        tool.meta = Some(rmcp::model::Meta(obj));
    }
    Ok(result)
}
```

- Rewrite `SCHEMA_DOC` (transport.rs:267-293):
  - Drop the grouped/ungrouped split language.
  - Drop references to `best_hit_message_id`, `best_score`, `first_timestamp`, `last_timestamp`, `matched_via`, `boost_recent`, `group_by_conversation`.
  - Document the renamed field (`session_id`), the renamed `limit`, the cursor pagination contract, the `matched_total` semantics, the up-to-3 matches behavior.
  - Note that result rank may shift between cursor pages if the corpus updates.
- Update the `#[tool(description = ...)]` blocks at transport.rs:397-402 (pond_search) and transport.rs:444-451 (pond_get). Each stays under 2KB.
- Drop the kb-parity comment in `McpSearchParams` and `McpGetParams` docstrings.
- For `request_id`: see Section 8.

### 5.4 main.rs (CLI rendering)

- `pond search` CLI rendering currently branches on `SearchResultBody::{Hits,Groups}`. Collapse to single-mode session-with-matches rendering using `comfy-table` (per CLAUDE.md output stack).
- The `--mode` operator flag stays (`mode_override` is still on the wire `SearchRequest`).

### 5.5 docs/spec.md

Sections 7 (protocol) and 8 (search and embeddings) update to match. Per CLAUDE.md, read the sections before locking the wire shape; spec changes land in the same commit as the code.

### 5.6 Tests

- `tests/integration/transport_http.rs`: keep. Update one `GetResult::Session` destructure (line 133) to the new shape. SSE-related tests are unaffected by this redesign.
- `tests/integration/transport_mcp.rs`: rewrite to ~80 lines / 3 focused tests. Drop kb-parity framing, `total` assertion, `SearchResultBody` discriminator, `conversation_id` mapping. New tests:
  1. `tools/list` returns both tools with `_meta["anthropic/maxResultSizeChars"]` set to 80_000 (search) and 200_000 (get).
  2. `pond_search` and `pond_get` round-trip success with the new shape. Includes `include_parts=false` default and `include_parts=true` reasoning surfacing for get.
  3. Unknown session surfaces as a JSON-RPC tool error with `data.pond_code="not_found"` and `data.retryable=false`.
- `tests/integration/search.rs`: assertions touching `Hit`, `Group`, `SearchResultBody`, `total`, `best_hit_message_id`, `best_score`, `first_timestamp`, `last_timestamp`, `group_by_conversation` need updating. Expect ~half the file to change.
- `tests/integration/claude_code_ingest.rs`: grep for `GetResponse` field accesses; likely a one-line update.
- New unit tests inside `src/transport.rs` (`#[cfg(test)] mod tests`):
  - `to_error_data` returns correct JSON-RPC code + `pond_code` + `retryable` for each `ErrorCode` variant.
  - The `list_tools` `_meta` injection produces the expected `maxResultSizeChars` per tool on a synthetic `ListToolsResult`.
  - `json_result` serializes a `SearchResponse` into a single-text-block `CallToolResult` matching the wire shape (and, post-decision, without `request_id`).

## 6. Things explicitly ruled out

- Top-level `sessions: BTreeMap<session_id, SessionRef>` dedup map. Math doesn't pay off at realistic distributions (break-even requires rows-per-session > ~2, which only happens at high ungrouped limits, and grouped mode is 1:1 with sessions so it loses outright). Inline `project`/`source_agent` on `Session` is cheaper.
- `LIMIT_CAP` reduction from 200 -> 50. Budget gate (D9) self-regulates response size; an explicit cap on `limit` is redundant once the gate is in place.
- `tiktoken-rs` for token approximation. Heuristic (D1) is what Claude Code itself uses; tiktoken-rs would add a dependency and a different vocab (OpenAI's) for no measurable improvement vs `bytes/4`.
- Reusing the e5 tokenizer or Lance ngram tokenizer. See D1 reasoning.
- `matches_per_group` request flag. Hardcoded 3 (D5). Add the flag the day a second caller needs it.
- `include_group_preview` request flag. Once groups always carry up to 3 matches, the "snippet vs no snippet" toggle has no purpose.
- Per-tool MCP wire shape (different response struct per transport). One shape across surfaces (D10).
- Migration shims or changelog entries. CLAUDE.md: pond is pre-release, breaking changes are free, no changelog.

## 7. Land order

The wire-shape changes are interlocked. The clean atomic commits are:

1. Read `docs/spec.md` sections 7-8. Draft the spec text update.
2. **One wire-redesign commit** covering: wire.rs type changes + spec.md update + handlers.rs rewrite (build_sessions, single-mode run_search, budget gate, byte-based budget unit, cursor encode/decode) + transport.rs (MCP schema renames, SCHEMA_DOC rewrite, tool descriptions, `_meta` injection) + tests update (transport_mcp.rs rewrite, transport_http.rs one-liner, search.rs assertions, new unit tests in transport.rs) + main.rs CLI rendering collapse. Per CLAUDE.md "no migration notes / compatibility shims," there is no clean staged path; the redesign is one commit.
3. If choosing the request_id option B (Section 8): add `X-Pond-Request-Id` header on HTTP routes. Independent of step 2, can land separately.

## 8. Decisions still open

- **`request_id` strategy.** Two options:
  - **Option A (surgical):** Keep `SearchResponse.request_id`, `GetResponse.request_id`, `IngestResponse.request_id` on the wire. Strip in `transport.rs::json_result` (line 595) before MCP serialization. HTTP and CLI keep the field. Smaller scope, transport-divergent wire.
  - **Option B (clean):** Drop `request_id` from all wire response types. Add `X-Pond-Request-Id` HTTP header on the axum routes (use `new_request_id()` once per request, attach to response). MCP gets nothing extra (JSON-RPC envelope `id` provides correlation). One wire shape everywhere.

  Recommended: Option B. Touches more files but eliminates a transport divergence and matches how modern APIs do correlation. If choosing Option A, the wire `SearchResponse` keeps `request_id`; `transport.rs::json_result` strips it.

- **`first_matched_at` / `last_matched_at` on wire.** With Option B (D10's "one wire shape") these are gone everywhere - the consolidated decision is to drop them. With Option A (transport-divergent), they could survive on HTTP/CLI and be stripped on MCP. Recommended: gone everywhere, regardless of the request_id choice; agent-irrelevant on MCP, and the HTTP/CLI use case for them is weak.

## 9. Out of scope (deferred)

- HTTP-only behavior (SSE `/v1/sessions/{id}/events`, error code -> status mapping) - tested in `transport_http.rs`, unaffected by this redesign.
- `pond_ingest` and its `IngestResponse` shape - different tool, different surface; no contract change here.
- `pond search --explain` / `explain_search_plan` (handlers.rs:1256) - operator-only path, unchanged. Continues to expose retrieval-arm details that the production response no longer carries.
- Object-store backend (separate plan per the `project_s3_imminent` memory item).
- Bench harness changes (`scripts/search-benchmarks/`). The harness uses CLI/HTTP and `mode_override`; its inputs are unchanged. Output assertions touching `Hit`/`Group` need a follow-up.

## 10. References

- Claude Code MCP output limits: `https://code.claude.com/docs/en/mcp` -> "MCP output limits and warnings"
- `_meta["anthropic/maxResultSizeChars"]` introduced in Claude Code v2.1.91 (anthropics/claude-code#42869)
- Claude Code internal validation logic (chars/4 heuristic, IMAGE_TOKEN_ESTIMATE=1600, MCP_TOKEN_COUNT_THRESHOLD_FACTOR=0.5): leaked source at `leaf-kit/claude-analysis/src/utils/mcpValidation.ts`
- Related plan: `docs/plans/mcp-memory-budget.md` (companion work on MCP memory and context budget)
- rmcp `Tool.meta` serialization: `rmcp-1.7.0/src/model/tool.rs:42`, `rmcp-1.7.0/src/model/meta.rs:199`
