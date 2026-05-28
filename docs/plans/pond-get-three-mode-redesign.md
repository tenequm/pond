# pond_get three-mode redesign

Status: planned (not started)

## Summary

Reshape pond's read surface. Replace pond_get's `include_parts: bool` flag and opaque cursor with a three-mode response (`conversational | complete | verbatim`) and a named `after_id` pagination field. Add compact `parts_summary` on per-message views (and on user-role search hits). Delete the SSE `pond_session_events` endpoint entirely.

Wire-breaking, pre-release - OK per CLAUDE.md.

## Context

Why this redesign:

- The `include_parts: bool` flag conflated two distinct intents: "skim the conversation but see what tools were used" (only needs metadata) and "give me everything for restore" (needs content). Splitting parts retrieval into a separate intent per scope is structurally cleaner.
- The opaque base64 cursor was 228 chars at peak, hostile to agent context and to operator log readability. Named `after_id` is self-documenting; the spec's append-only invariant means pagination state is one id.
- `up_to` (truncate a session at a message id) is cut from `pond_get`. Its one unique power is a precise upper bound that excludes everything after the anchor - precision the agent surface does not need: trailing context is free rather than wrong, and the real agent needs are covered by the window (`message_id` + `context_depth`) and forward paging (`after_id`). No test exercises it and no caller sets a non-None value. The cut is non-breaking to re-add later; precise-bound retrieval, if a real consumer ever appears, belongs to the restore/export path, not the agent tool. Restoration as a system trait is unaffected - it serializes whole sessions (lineage-complete) and never read `pond_get`.
- pond_session_events (SSE) duplicated what pond_get does, was speculative for the deferred live-write use case, and exposed no MCP surface anyway. YAGNI.
- Empirical corpus sampling showed ~10% of claude-code assistant hits and ~40% of messages carry parts the snippet doesn't surface; a compact `parts_summary` closes that gap without paying for full content.
- MCP best practices: tools should return curated results, not raw API dumps. Default response budget remains the floor; we are responsible for not destroying agent context.

## Locked design

### Request

```rust
struct GetRequest {
    protocol_version: u16,
    namespace: Option<String>,

    // Mutually exclusive scopes
    session_id: Option<String>,
    message_id: Option<String>,

    // message_id only
    context_depth: usize,                    // default 0

    // Both modes
    limit: usize,                            // default 20, max 1000
    response_mode: ResponseMode,             // default Conversational; ignored in message_id mode

    // Continuation
    after_id: Option<String>,                // unified anchor: last message_id (session mode)
                                             // or last part_id (message mode)
}

enum ResponseMode {
    Conversational,   // search_text IS NOT NULL filter + part summaries
    Complete,         // all messages (including carriers) + part summaries
    Verbatim,         // all messages + full parts inline (heaviest mode)
}
```

### Response

```rust
enum GetResult {
    Session {
        session: GetSession,
        messages: Vec<MessageView>,
        messages_remaining: usize,
    },
    Message {
        session: GetSession,
        target: MessageView,
        target_parts: Vec<Part>,
        target_parts_remaining: usize,
        siblings: Vec<MessageView>,          // 2*context_depth around target
    },
}

struct MessageView {
    id: String,
    role: Role,
    timestamp: DateTime<Utc>,
    text: Option<String>,                    // from search_text
    content: Option<String>,                 // for system messages with content string
    parts_summary: Vec<PartSummary>,         // always present, may be empty
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parts: Option<Vec<Part>>,                // populated only in Verbatim mode
}

struct PartSummary {
    kind: String,                            // "text" | "tool_call" | "tool_result" | "reasoning" | "file"
    label: Option<String>,                   // per-kind derived; see rules below
    call_id: Option<String>,                 // populated for tool_call and tool_result only
}
```

### PartSummary label rules

| kind | label |
|---|---|
| text | first 80 chars of text, ellipsized if longer |
| reasoning | None (presence is the signal) |
| file | file_name if present, else media_type |
| tool_call | name (e.g. "Bash") |
| tool_result | name + " (failed)" if is_failure, else just name |
| tool_approval_request | approval_id |
| tool_approval_response | approval_id + " (approved)"/" (denied)" |

### pond_search update

`SearchResult` gains:

```rust
#[serde(default, skip_serializing_if = "Vec::is_empty")]
parts_summary: Vec<PartSummary>,             // populated only for role=User hits
```

Rationale: user hits can carry FilePart attachments (~1,500-3,000 FileParts in current corpus) and occasional multi-part scaffolding splits; the summary distinguishes plain-text prompts from attached-file prompts. Assistant hits don't get parts_summary because reasoning/tool_call structure splits across separate messages in both v1 adapters anyway.

### Behavior contracts

- `response_mode` is ignored when `message_id` is set - message_id mode always returns the requested target with its full parts (paginated), regardless of mode.
- Server response budget kept, sized to match declared `_meta["anthropic/maxResultSizeChars"]` (~200KB). Server stops adding messages when next message would exceed budget. `messages_remaining > 0` signals to paginate.
- Conversational and Complete responses are usually limited by `limit` (default 20); Verbatim responses are usually limited by the size budget.
- `after_id` is exclusive (rows where `(timestamp, id) > (anchor.timestamp, anchor.id)` in session mode; parts where `ordinal > anchor.ordinal` in message mode after looking up the anchor part).
- Page boundaries always align with message boundaries (server never cuts mid-message).
- Message ids are unique only within a session per spec `lance-table-creation-session-scoped-pk`; `after_id` in session mode disambiguates via the implicit session_id from the request or prior page.

## Files to change

### 1. `Cargo.toml`
Revert the uncommitted `postcard = { version = "1.1", features = ["alloc"] }` line. Cargo.lock will regenerate.

### 2. `src/wire.rs` (large rewrite)
- Drop fields on `GetRequest`: `cursor` (and anything cursor-related), `up_to`.
- Drop fields on `GetResponse`: `next_cursor`, `has_more` (response side).
- Add `after_id: Option<String>`, `response_mode: ResponseMode` on `GetRequest`.
- Add new types: `ResponseMode` enum, `MessageView` struct, `PartSummary` struct.
- Rewrite `GetResult` enum: new `Session` and `Message` variants per design above.
- Update `SearchResult`: add `parts_summary: Vec<PartSummary>` (default empty, populated for user-role hits).

### 3. `src/handlers.rs` (large rewrite)
- **Delete entirely**: `mod session_events_handler` block (~lines 577-717), the `pub use session_events_handler::*` re-export.
- **Delete entirely** from `mod get_handler`: `Cursor` struct, `CursorScope` enum, `encode_cursor`, `decode_cursor`, `build_next_cursor`, plus the uncommitted `CursorId` enum and postcard usage.
- **Rewrite**: `pond_get` function to dispatch on `(session_id, message_id, response_mode)` and call new `session_view` / `message_view` helpers.
- **Delete**: the `up_to` dispatch/validation branch (the `up_to is valid only with session_id` error path); the field no longer exists.
- **Update**: `pond_search`'s hit-assembly path to populate `parts_summary` on user-role results. Reuse the `part_summary` function from sessions.rs.

### 4. `src/sessions.rs`
- Rename `paged_session_view` to `session_view`. Rewrite to branch on `ResponseMode`:
  - Conversational: calls `scan_conversational_messages` (existing primitive); attaches part summaries.
  - Complete: scans all messages (filter: `Predicate::Eq("session_id", X)`); attaches part summaries.
  - Verbatim: same scan as Complete but additionally fetches parts and populates `parts: Some(...)`.
- Add `message_view` function for the message_id path: looks up target, scans 2*context_depth siblings, fetches target's parts paginated by `after_id` (part_id -> ordinal -> rows with greater ordinal).
- Add `part_summary` helper that derives `{kind, label?, call_id?}` from a Part per the label rules.
- `session_view` does not carry over the `up_to` truncation block (old `paged_session_view`, lines 865-870).
- Drop: `PagedScope` (including its `up_to` field), `PagedSessionView`, `MessageRow`. (`BUDGET_BYTES` lives in `handlers.rs`, not here - drop it under section 3.)

### 5. `src/transport.rs`
- Delete the `/v1/sessions/{id}/events` route handler and supporting code.
- Rewrite `SCHEMA_DOC` constant for the new pond_get shape (three modes, parts_summary, after_id).
- Update `pond_get` MCP tool description: name the three modes explicitly; add "not for bulk export, use `pond export`" note.
- Drop the `up_to` MCP tool param and its "`up_to` truncates" mention in `SCHEMA_DOC`.

### 6. `src/main.rs`
- Drop the `--up-to` CLI flag (the `up_to: Option<String>` arg with `requires = "session_id"`, `conflicts_with = "message_id"`) and stop threading it into the `GetRequest`.

### 7. Tests
- `tests/integration/transport_http.rs`: delete the 6 SSE tests (`session_events_*`).
- `tests/integration/transport_mcp.rs`: update fixtures for new request/response shape.
- `tests/integration/claude_code_ingest.rs`: update pond_get calls for new field shape.
- `tests/integration/search.rs`: migrate all 6 `include_parts` references, not just the named test - the 4 `GetRequest` literals (incl. the one in `injected_task_notification_is_excluded_from_search_but_kept_for_get`) move to `response_mode` (`complete` where the test asserts parts are reachable), and the 2 assertion-message strings that say "include_parts=true" get reworded.
- Remove the now-dropped `up_to: None` line from every `GetRequest` literal across `tests/integration/` (claude_code_ingest.rs, search.rs).

### 8. `docs/spec.md`
- §1 (overview): drop `session-events` from the operations diagram (the `search / get / session-events` line) and from the prose "search, get, and session-events all read from".
- §7.5: drop operation #4 (`pond_session_events`) in full - this also removes the "live-tail activates with live-write (Section 9) on the same endpoint" sentence, which lives inside op #4 (there is no separate §9.4 SSE reference). Update pond_get description to name three modes, `after_id` field, page-boundary-alignment rule, and the "not for bulk export" note.
- §7.7: change "Ingest and session-events stay HTTP-and-CLI only" to "Ingest stays HTTP-and-CLI only."
- §8: add a sentence noting search hits include `parts_summary` for user-role messages.

## Out of scope (separate follow-up commits)

### Adapter bugs (found during the FilePart-presence investigation)

`src/adapter/claude_code.rs:918-963` `file_part()` function:

1. `media_type` is read from the wrong nesting level. Claude Code's JSONL has `{"type": "image", "source": {"media_type": "image/png", ...}}`, but the adapter reads `value.get("media_type")` at the top level, always falling back to `"application/octet-stream"`. Fix: also check `value.get("source").and_then(|s| s.get("media_type"))`.

2. Base64 data stored as a string in `variant_data` JSON instead of decoded bytes in the blob column. Not data loss - recoverable - but it's a fidelity bug, costs storage size, and breaks the spec §5.1 "data: Lance blob; FilePart payload only" contract.

These are real bugs but unrelated to the API redesign. Land them in a separate commit immediately after this PR.

### Future structural question (out of scope, worth thinking about later)

codex-cli emits separate messages for reasoning / text / tool_call (one source row -> one message). A single conceptual "assistant action" spans 3-4 message IDs. Search hits on the text part miss the surrounding reasoning/tool structure entirely (null search_text on those rows). Worth considering a "unified assistant turn" abstraction in a future PR.

## What stays unchanged

- `pond_ingest` (request, response, handler).
- `pond_search`'s `SearchCursor` (still JSON-base64; ranked pagination needs the carrier - rank can shift between calls).
- `pond_search`'s overall response shape (just adds optional `parts_summary` on SearchResult).
- The conversational scan primitive `scan_conversational_messages` (from commit `fdedb8c`) - the new `session_view` calls it.
- The `SearchText` newtype (still load-bearing).

## Verification

Before commit:
- `cargo fmt --check` clean
- `cargo clippy --all-targets -- -D warnings` clean
- `cargo test` passes (expect ~70 unit + ~30 integration after SSE-test deletion)
- Manual end-to-end via release CLI on the regression session `c6a7b96b-c44f-4d4e-b242-aaa7acfeda04`:
  - `pond get --session-id <X>` returns conversational view (small response, no system rows)
  - `pond get --session-id <X> --response-mode complete` returns all messages with summaries
  - `pond get --session-id <X> --response-mode verbatim` returns all messages with parts (paginates via budget)
  - `pond get --message-id <Y>` returns target message + parts + siblings

## Commit shape

One commit on top of `3eb5902` (current HEAD; the uncommitted postcard/CursorId WIP already sits here). Conventional commit message:

```
feat(get): three-mode response with compact part summaries; drop SSE

pond_get gains three response modes (conversational/complete/verbatim),
compact PartSummary on per-message views, unified after_id pagination
field, and a server-side response budget honoring the declared
_meta["anthropic/maxResultSizeChars"] cap. Search hits gain
parts_summary for user-role messages.

Removes:
- pond_session_events SSE operation and handler (YAGNI; live-tail is deferred)
- Cursor abstraction in pond_get (replaced by named after_id field)
- include_parts: bool flag (replaced by response_mode enum)
- up_to session-truncation field (no agent caller; precise-bound retrieval is not an agent-surface need)
- Server-side BUDGET_BYTES override-of-limit truncation

Wire-breaking; pond is pre-release.
```

Scope estimate: ~600-900 LOC net, mostly deletion.

## Open questions resolved during design

| Question | Resolution |
|---|---|
| Cursor vs named field for pagination | Named `after_id`; cursor was opaque, hostile to agent context |
| Merge `after_message_id` + `after_part_ordinal` | Yes, unified to `after_id`; mode determines interpretation |
| Keep `messages_remaining` count or just `has_more` bool | Keep count - answers "how much more" not just "any more" |
| Drop response budget entirely | No - we're responsible for protecting agent context per MCP best practices |
| Restore via MCP (Restore-A vs B) | response_mode="verbatim" in same tool; HTTP-only MCP clients exist |
| parts_summary on search hits | Yes, but only user-role (FileParts signal; assistant hits don't earn it empirically) |
| PartSummary shape | Compact: `{kind, label?, call_id?}`; no id, no ordinal |
| SSE - keep or delete | Delete; YAGNI; live-tail is deferred to live-write per spec §9 (Deferred) |
| Mode names | `conversational | complete | verbatim` (after evaluating 6 options) |
| Keep `up_to` on `pond_get` | Cut. Its unique power (precise upper bound, excluding everything after the anchor) is precision the agent surface doesn't need; restoration is a system trait served by the restore/export path, not `pond_get`. No test/caller uses it; non-breaking to re-add. |
| `context_depth` naming | Keep. It mirrors `grep -C` (symmetric context radius); the term and the symmetric-only shape are the right agent-surface default. Directional `before`/`after` (grep `-B`/`-A`) is the blessed extension if a real need ever appears. |
