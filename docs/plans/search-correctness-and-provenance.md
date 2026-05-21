# Plan: search correctness and Part provenance

Status: ready to implement. `docs/spec.md` is already updated to describe the target
state - this plan brings the code into conformance. Written for a fresh session with
no prior context; read it top to bottom before starting.

## Summary

Five fixes, decided in a long design review. Together they make `pond search`
actually correct and add a canonical `Part.provenance` marker so harness-injected
scaffolding is excluded from search while still preserved for lossless restore.

- **D1** - dataset directories are wrongly named `*.lance.lance/` and `pond status`
  size accounting is broken (every dataset reports `0 B`).
- **D4** - new canonical `Part.provenance` field (`conversational | injected`);
  adapters classify every Part; search indexes only conversational parts.
- **D2 / D3** - index build+extend moves onto the write path; the `maintenance()`
  bundle is dissolved; fragment compaction and manifest-version GC are deferred.
- **D5** - a search hit carries full-or-truncated message text plus a match snippet,
  instead of a head-truncated `preview`.

All five require a one-time data-dir rebuild (re-sync from source). That is expected
and approved - pond is pre-release, there is no migration path and none is wanted.

## Background - why this exists

A hands-on investigation of the live pond install found:

- The data dir holds `sessions.lance.lance/`, `messages.lance.lance/`, etc. - a
  double `.lance` suffix. `pond status` consequently reports every dataset as `0 B`
  and dumps 3.55 GiB into `other`.
- `messages.lance.lance/_indices/` does **not exist** - the messages table has zero
  indexes. `pond search` therefore runs an unindexed, degraded scan: searching a
  verbatim 9-word phrase that exists in a known message does not return that message
  anywhere in the top 50. Search is silently wrong, not just slow.
- An 8-harness fixture investigation plus a provider-API/agent-framework web survey
  established that harness-injected content (background-task notifications, injected
  environment context, system reminders, slash-command echoes) sits inside
  conversational slots across most harnesses, is byte-identical in shape to real
  turns, and must be excluded from search yet preserved for restore. `role` cannot
  express this; a new typed marker is needed, classified per-adapter at the seam.

## Spec basis

`docs/spec.md` was updated for this plan. These sections are the contract - read
them, and do not "simplify away" their rules:

- **3.7 Index upkeep** - index fold on the write path; never a from-scratch rebuild.
- **4.7 Part** - the `Provenance` enum and the `provenance` field on `BasePart`.
- **4.8 `part-provenance`** - every Part MUST be classified; provenance-homogeneous Part.
- **6.4 `provenance-required`** - constructing a Part without provenance is a compile error.
- **6.5 placement rule 1** - content becomes typed Parts, each classified.
- **8 Indexed text / Hit payload / Index lifecycle** - search reads `provenance`;
  hit payload shape; write-path index fold.
- **9 Deferred** - fragment compaction + manifest-version GC, trigger = disk growth.

## Code map (verified locations; line numbers approximate)

- Canonical wire/canonical types: `src/wire.rs` - `Part` struct (~150), `PartKind`
  enum (~162), `Role` enum (~131), `Hit` struct (~400), `Group` struct (~415).
- Adapter seam: `src/adapter/extract.rs` - `Extracted<T>`, `Source`, `extract_*`.
- Adapters: `src/adapter/claude_code.rs`, `src/adapter/codex_cli.rs`; registry in
  `src/adapter/mod.rs` (`registry()` ~316). Two adapters exist; both must be updated.
- Search-text builder: `src/sessions.rs` `search_text()` (~1805).
- Parts table schema: `src/sessions.rs` `part_schema()` (~2034); part write path
  (~1125-1135) and read path `row_to_part`-style (~2439).
- Table-name constants: `src/sessions.rs` `SESSIONS/MESSAGES/PARTS/EMBEDDINGS` (~1973-1976).
- Storage size accounting: `src/sessions.rs` `StorageSizes::from_local_dir` (~146);
  `src/main.rs` `storage_sizes_for` (~1040), `render_status` (~1055).
- Indexing: `src/sessions.rs` `ensure_indices` (~956), `fts_search` (~796);
  `src/substrate.rs` `ensure_index` (~514), `maintenance`/`maintain_table` (~541-579).
- Ingest handlers: `src/handlers.rs` `ingest_events` (~502), `pond_ingest` (~448),
  `ingest_adapter` (~171).
- Search handler: `src/handlers.rs` `pond_search` (~1075), `make_preview` (~1372),
  `ScoredHit::into_hit` (~1395).
- Sync CLI arm: `src/main.rs` `Command::Sync` (~322), the `ensure_indices` +
  `maintenance` calls (~383-390); `render_hit` (~1284), `render_group` (~1312).

## Build / test / lint (from CLAUDE.md)

- Build `cargo build`; test `cargo test`; lint `cargo clippy -- -D warnings`;
  format `cargo fmt` (check with `--fmt --check`).
- Unit tests live in `#[cfg(test)] mod tests` next to the code. Integration suites
  are modules under `tests/integration/` wired into `tests/integration.rs` via
  `#[path]`. Do not add loose `tests/*.rs` files.

---

# Work item D1 - table naming and `pond status` sizes

## Problem

`src/sessions.rs` table-name constants embed the `.lance` extension
(`pub(crate) const MESSAGES: &str = "messages.lance"`). The lance-namespace Directory
implementation appends `.lance` itself when forming the table directory, so the real
directory is `messages.lance.lance/`. `StorageSizes::from_local_dir` strips exactly
one `.lance` suffix and matches bare names, so nothing matches and every byte lands
in `other`.

## Decision (D1 = option B)

1. Make the table-name constants bare logical names - the namespace owns the `.lance`
   suffix.
2. Stop hand-rolling directory-name knowledge in the size walk: ask the namespace for
   each table's location, then size those locations. This removes pond's duplicated
   copy of the namespace's layout convention (spec.md#catalog-seam intent) and is the
   shape the S3 backend will need.

## Steps

- `src/sessions.rs`: change `SESSIONS`, `MESSAGES`, `PARTS`, `EMBEDDINGS` to bare
  `"sessions"`, `"messages"`, `"parts"`, `"embeddings"`.
- Expose the four table locations from the substrate. `open_or_create_via_ns`
  (`src/substrate.rs` ~650) already resolves each table's location via
  `nm.describe_table(...).location`. Add a `Handle`/`Store` method that returns the
  four `(label, location-Url)` pairs.
- Rewrite `StorageSizes::from_local_dir` (or replace it) to: for a local data dir,
  walk each of the four namespace-reported locations and sum file sizes; compute
  `other` as the total under the data-dir root minus the four. Remote backends keep
  returning `None` (unchanged - `storage_sizes_for` already does this).
- Delete the `strip_suffix(".lance")` heuristic and the bare-name match table.

## Acceptance

- After a rebuild, the data dir contains `sessions.lance/`, `messages.lance/`,
  `parts.lance/`, `embeddings.lance/` (single suffix).
- `pond status` reports non-zero per-dataset sizes; `other` is small.
- No code outside the namespace constructs or parses a `.lance` path.

## Tests

- Unit test for the size accounting against a `tempfile::TempDir`-backed `Store`
  with a few rows ingested: each of the four datasets reports `> 0`.

---

# Work item D4 - `Part.provenance`

This is the keystone and the largest item. It is a canonical-model change, so it
touches the wire types, the parts table schema, the adapter seam, both adapters, and
search.

## Decision

- Add a typed `Provenance` enum to the canonical model: exactly two variants,
  `conversational` and `injected`. Additively extensible later; ship two now.
- `provenance` is a **mandatory** field on every Part. Classification is a per-adapter
  obligation enforced at the seam (compile error to omit).
- `search_text` indexes only `conversational` parts.
- The canonical Part stays fully intact - provenance never changes role, never drops
  content. An `injected` part is excluded from search but returned by `pond_get` and
  restored normally.

## Granularity note - no content splitting needed for v1

spec.md 6.5 says an adapter must split a source span that fuses authored and injected
content into provenance-homogeneous Parts. The harness investigation confirmed that
**neither v1 adapter (claude_code, codex_cli) ever mixes provenance within a single
message** - claude_code injects whole messages, codex injects whole records. So v1
implements **classification only; do not build content-splitting**. The splitting
contract in spec 6.5 activates when a future adapter that interleaves (opencode,
nanoclaw, openclaw) is built. Each message's parts are uniformly one provenance for
v1 adapters; the field is still per-Part because that is the canonical model.

## Steps

### D4.1 - canonical types (`src/wire.rs`)

- Add `pub enum Provenance { Conversational, Injected }` with
  `#[serde(rename_all = "snake_case")]`. **Do not** derive or implement `Default` for
  it, and **do not** put `#[serde(default)]` on the field below.
- Add `pub provenance: Provenance` to the `Part` struct. With no `Default` and no
  serde default, every existing `Part { .. }` struct-literal becomes a compile error
  until the author supplies `provenance` - that is exactly `provenance-required`
  (spec 6.4), achieved for free. Keep it that way; resist adding a default.

### D4.2 - parts table schema (`src/sessions.rs`)

- Add a non-nullable `provenance` column (`DataType::Utf8`) to `part_schema()`.
- Update the part write path (~1125-1135) to write `provenance` and the part read
  path (~2439) to read it back into `Part`.

### D4.3 - seam (`src/adapter/`)

- The mandatory field from D4.1 already forces every adapter Part construction to
  supply provenance. If adapters construct Parts through a helper, give that helper a
  mandatory `provenance` parameter. Acceptance: deleting a provenance assignment from
  an adapter must fail to compile.

### D4.4 - claude_code adapter (`src/adapter/claude_code.rs`)

Classify each Part. The harness investigation is the ground truth; no claude_code
field reliably flags injection (`isMeta` is true for some injected turns, null for
others), so classify by content wrapper and record type.

Mark `injected`:
- user message whose content is a `<task-notification>...</task-notification>` block;
- user message that is a slash-command echo (`<command-name>` / `<command-message>` /
  `<command-args>`);
- user message `<local-command-caveat>...`;
- user message with `isMeta: true` (expanded skill / command body);
- user message `[Request interrupted by user...]`;
- `tool_result` parts (tool output is runtime-produced);
- `system` subtype `local_command` (`<local-command-stdout>`);
- any parts on `attachment`-derived carrier messages.

Mark `conversational`:
- genuine human user text;
- assistant `text` and `reasoning` parts (model-authored);
- `tool_call` parts (the model authored the call).

### D4.5 - codex_cli adapter (`src/adapter/codex_cli.rs`)

Mark `injected`:
- `response_item` `message` with `role: developer` (harness instruction blocks);
- `response_item` `message` with `role: user` whose content is `<environment_context>`
  or `# AGENTS.md instructions ...` - i.e. a user-slot record that is not a genuine
  prompt. Codex also emits an `event_msg` `user_message` stream that enumerates
  exactly the genuine human prompts; cross-referencing it is a reliable way to tell a
  real prompt from an injected user-slot record.
- `function_call_output` (tool output).

Mark `conversational`:
- genuine human prompts; `agent_message` / `response_item` assistant messages;
  `reasoning`; `function_call` (model-authored).

### D4.6 - search-text builder (`src/sessions.rs` `search_text()`)

- Extend the existing `(role, part-kind)` filter with `provenance`: a part
  contributes to `search_text` only when `provenance == Conversational`. Reasoning,
  tool-call, tool-result, approval parts stay excluded by kind as before.

### D4.7 - restore / fixtures

- Native serialize ignores `provenance` (it is pond-additive, not a source field) -
  the codec round-trip tests (parse fixture, serialize native, assert value-equal)
  stay green because provenance never reaches the native output.
- Any committed canonical / wire / `IngestEvent` fixtures must be regenerated to
  include the new mandatory field (deserialization fails without it - that is
  intended). Regenerate them; do not add a serde default to paper over it.

## Acceptance

- `cargo build` fails if any adapter omits a provenance classification.
- A claude_code `<task-notification>` message produces `injected` parts and has null
  `search_text`; a genuine human prompt produces `conversational` parts and non-null
  `search_text`.
- `pond search` never returns a `<task-notification>` message as a hit.

## Tests

- Unit tests in each adapter: a known injected fixture record -> `injected`; a known
  human/model record -> `conversational`.
- Unit test for `search_text()`: an `injected` text part is excluded; a
  `conversational` one is included.
- A search-level integration test: ingest a session containing a task-notification,
  assert it is absent from search results but present via `pond_get`.

---

# Work item D2 / D3 - indexing on the write path

## Problem

Index creation has a single trigger - `store.ensure_indices()` in the `pond sync` CLI
arm (`src/main.rs` ~383). HTTP/MCP ingest never triggers it, nothing verifies it
succeeded, and the live messages table has no indexes at all. `maintenance()` bundles
two unrelated operations (`optimize_indices` and `cleanup_old_versions`) and is also
CLI-sync-only.

## Decision

- Index create + incremental fold runs on the **write path** - at the tail of the
  shared ingest handler - so every ingest route (CLI sync, HTTP, MCP) builds and
  extends indexes. spec.md 3.7 and 8 already mandate this; the code must conform.
- The fold is **incremental** (`optimize_indices`), never a from-scratch rebuild
  (D3). `ensure_index` stays create-if-missing.
- A failed fold is **soft**: logged via `tracing::warn`, not propagated as an error
  that aborts the write. The next write batch retries it (idempotent).
- Dissolve `maintenance()`: drop `cleanup_old_versions` entirely. Fragment compaction
  and manifest-version GC are deferred (spec.md 9). Keep the manifest-retention
  *window* as a table-creation parameter - not running GC simply over-retains
  history, which is safe.
- `pond search` checks for an unfolded index backlog and, when large, returns correct
  results plus a warning that the index is behind.

## Steps

- Add an index-upkeep step (create-if-missing + incremental `optimize_indices` for
  the FTS index, and scalar indexes) at the tail of `ingest_events`
  (`src/handlers.rs` ~502) so it runs for every ingest path. Reuse `ensure_indices` /
  `ensure_index` logic; extract `optimize_indices` from `maintain_table`.
- Make it soft-fail: wrap in a log-and-continue, do not `?`-propagate.
- `src/main.rs` `Command::Sync` (~383-390): remove the `store.ensure_indices()` and
  `store.maintenance(...)` calls and the `maintenance:` output line - indexing now
  happens inside ingest.
- `src/substrate.rs`: remove `maintenance()` / `maintain_table()` and the
  `cleanup_old_versions` call (and `MaintenanceReport`). Remove the now-dead
  `maintenance` config (`retention_days`, `enabled`) if nothing else uses it.
- `src/handlers.rs` `pond_search` (~1075): query the unindexed-row count
  (Lance `index_stats`-style) and, when it exceeds a threshold, attach a warning to
  the response / log it. Search still returns correct results regardless.

## Acceptance

- After `pond sync`, `messages.lance/_indices/` exists and contains the FTS index.
- After a `POST /v1/ingest` against `pond serve`, indexes are likewise present -
  indexing is not CLI-only.
- A simulated fold failure logs a warning and does not abort the ingest.
- No `cleanup_old_versions` call remains in the codebase.
- `pond search` emits a warning when run against a table with a large unfolded tail.

## Tests

- Integration test: ingest into a fresh `Store`, assert the FTS index exists and a
  message is retrievable by a verbatim multi-word phrase it contains.
- Integration test: ingest via the HTTP/handler path, assert the index is built.

---

# Work item D5 - search hit payload

## Problem

A `Hit` carries `preview: String` - the first ~160 chars of the message head. It is
often uninformative (leading filler) and never enough to judge relevance without a
second `pond_get`.

## Decision

A hit carries the matched message's indexed text:
- text length <= 2000 chars: return it in full;
- text length > 2000 chars: return the text truncated to 2000 chars **plus** a
  match-windowed snippet (drawn around the query terms, capped ~400 chars, within the
  200-500 range).

Thresholds are code constants, not spec. The CLI default output format stays `pretty`
(flipping it to JSON was discussed and explicitly left unchanged).

## Steps

- `src/wire.rs`: replace `Hit.preview` with `text: String` and
  `snippet: Option<String>` (`snippet` present only when `text` was truncated). Apply
  the same shape to `Group` or keep `Group.preview` as a short summary - implementer's
  call; note which in the commit.
- `src/handlers.rs`: replace `make_preview` (~1372) with a function that takes the
  message text and the query terms and produces `(text, snippet)`. Update
  `ScoredHit::into_hit` (~1395) and the group construction (~1403, ~1475). The query
  is available in `pond_search`.
- `src/main.rs`: update `render_hit` (~1284) and `render_group` (~1312) to render
  `text` / `snippet` in the pretty output.

## Acceptance

- A small message's hit shows full text and no snippet.
- A large message's hit shows 2000-char text plus a query-centered snippet.
- `pond search --format json` emits `text` / `snippet`, not `preview`.

## Tests

- Unit tests for the `(text, snippet)` builder: short input passes through; long
  input truncates and produces a snippet windowed on a query term.

---

# Rebuild and verification

After all four code items land:

1. Destroy the old data dir (it holds the orphaned `*.lance.lance/` datasets):
   `rm -rf ~/.local/share/pond`.
2. Re-sync from source: `pond sync`.
3. Verify:
   - `pond status` - per-dataset sizes are non-zero; the data dir holds
     `*.lance/` (single suffix), and `messages.lance/_indices/` exists.
   - Retrieval correctness (D2/D4): the verbatim-phrase probe must now work -
     `pond search "dead for a structural reason in the retry layer" --project pond`
     must return message `3baf1b63-8a19-4a0f-880b-0a399a90e120` at or near rank 1.
     (Before this work it was absent from the top 50.)
   - Provenance exclusion (D4):
     `pond search "task-notification" --project pond` must not return any message
     whose body is a `<task-notification>` block.
   - Hit payload (D5): `pond search ... --format json` shows `text` / `snippet`.
4. `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` all green.

---

# Execution

One continuous chunk, reviewed once when complete - not a staged rollout. The five
changes (D1, D4, D2/D3, D5) have no interdependencies; edit in whatever order is
convenient (file-by-file is least context-reloading - `wire.rs`, `substrate.rs`,
`main.rs`, `handlers.rs`, and `sessions.rs` are each touched by two or more items).

Intermediate states need not compile. D4's mandatory no-`Default` `provenance` field
(spec 6.4) turns every unconverted `Part { .. }` site into a compile error the moment
its types land - that is the worklist, not a hazard. Drive the whole chunk to green
once, at the end, then run the rebuild + verification pass above. When it is all
green, stop for review.

# Out of scope

- The pond-vs-kb search-relevance benchmark - resume it only after this work lands
  and the rebuild is verified; the comparison is meaningless against a broken index.
- Fragment compaction and manifest-version GC - deferred (spec.md 9).
- Richer `Provenance` variants - ship `conversational | injected` only; the enum
  extends additively when a consumer needs finer kinds.
- Content-splitting for mixed-provenance source spans - no v1 adapter needs it.
- Flipping the CLI default output to JSON - left as `pretty`.
- `tests/fixtures/README.md` corrections - the investigation found several
  undocumented fields (`isMeta`, opencode `synthetic`, codex `developer` role,
  claude_app `isReplay`); fixing the README is a separate task, not part of this plan.
