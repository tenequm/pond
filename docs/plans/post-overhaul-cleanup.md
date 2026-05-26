# Post-overhaul cleanup

Implementation plan for bringing the codebase in line with the rewritten `docs/spec.md` (commit `9c9914d`, branch `worktree-embeddings-benchmark`). This plan is executed from a fresh session - it must stand on its own.

## Context

A heavy `docs/spec.md` rewrite landed earlier on this branch. Key shifts:

- `fold-on-write` rule deleted; replaced with `index-write-decoupled` and `index-per-family` (§3.7).
- `WriteShape` enum dropped from the design. Fold strategy is encoded per `IndexParamsKind`, not per write.
- `pond compact` verb removed; its work subsumed by `pond index optimize`.
- New CLI verb group `pond index status / optimize [--wait] / rebuild [<intent>]` (§7.8).
- `pond search --explain` added (§7.8).
- `options` and `variant_data` column types change from Utf8 to `pa.json_()` JSONB (§5.1).
- `messages.vector` changes from Float32 to BFloat16 (§5.1).
- IVF_PQ partitions formula: `num_rows // 4096` (§8), not `sqrt(N).clamp(32, 4096)`.
- `[search].nprobes` and `[search].refine_factor` become operator-tunable (§8).
- `messages_id_btree` dropped (rare `--message-id` lookups full-scan).
- `pond embed --force` becomes a single conditional `merge_update` keyed on `target.embedding_model != source.embedding_model` (§5.5).
- Fragment Reuse Index (`defer_index_remap=True`) becomes the default inside `pond index optimize` (§7.8).
- `auto_cleanup` window goes from 90 days to a short window; long-term recovery is `pond export` + deferred Lance tags (§3.4).

The branch is currently +3195 / -582 lines vs main. After this plan lands, target diff is roughly +500 / -200 vs main: every surviving line must justify its existence under the new spec.

## Pre-reading (mandatory)

Before any change:

1. `docs/spec.md` in full. Pay attention to §3.4, §3.7, §5.1, §5.5, §7.8, §8, §9.
2. `CLAUDE.md` - especially `## Minimalism`, `## Comments`, `## Tests`, `## Adapter seam (load-bearing)`.

Both are short. Re-read them before each phase.

## Scope guardrails (do not violate)

- DO NOT modify `docs/spec.md`. The spec is the contract this plan implements.
- DO NOT touch `src/adapter/` beyond the comment cleanup explicitly listed. The adapter seam is unchanged.
- DO NOT touch `docs/researches/embeddings/*`. Published research.
- DO NOT bump the Lance pin (`v7.0.0-beta.16`). The BTree workaround in `index-per-family` depends on this exact version.
- DO NOT implement any of §9 deferred items: `pond tag`, image arrays, JSON-path scalar indices, FRI trim verb, OTel, time-travel, cross-session dedup, file-attachment indexing, graph traversal, remote embedding providers, future consumers, future adapters, provider-target restore, live-write, hosted multi-tenant.
- DO NOT write migration notes, compat shims, or changelog entries (pre-release, breaking changes free).
- DO NOT add dependencies. Per the audit, no new direct dep is justified for this work.
- DO NOT split `src/sessions.rs` or `src/handlers.rs` even if large. CLAUDE.md flat-crate rule.

## Substantive context the agents could not capture

Two facts to lock in your mental model before editing:

1. **Lance v7.0.0-beta.16 column-overlap pruning is column-overlap-only**. Read `~/pjv/lance-format/lance/rust/lance/src/dataset/transaction.rs:2581-2603` (`prune_updated_fields_from_indices`) on the `v7.0.0-beta.16` tag if needed. Only indexes whose covered fields intersect `fields_modified` lose coverage on a column-update commit. For a pond embed `merge_update(session_id, id, vector, embedding_model)`, only 3 of 8 messages indexes trail: `messages_id_btree`, `messages_session_id_btree`, `messages_vector_ivfpq`. After this plan drops `messages_id_btree`, only 2 of 7 trail per embed write.

2. **The BTree-flat-scan bug is real at the pin and unfixed at HEAD**. `RowAddrTreeMap::from_sorted_iter called with non-sorted input` fires when `optimize_indices` re-encodes a flat BTREE through `combine_old_new`. The bug was verified between v7.0.0-beta.16 and v7.1.0-beta.2: nothing in the drift window changes pruning behavior. The per-family fold table in §3.7 codifies the workaround: BTree always rebuilds via `create_index(replace=true)`; Bitmap / Inverted / IVF_PQ use `optimize_indices(append)`.

## Phase ordering

Execute in order. Each phase ends with `cargo build && cargo clippy -- -D warnings && cargo test` clean. Do not commit until the final phase.

---

## Phase 1: Substrate seam refactor

Remove the fold-on-write infrastructure; introduce the new operator-triggered index lifecycle.

### 1.1 Drop WriteShape

- Delete `WriteShape` enum and its variants `Append` / `ColumnUpdate` (`src/substrate.rs:202-206`).
- Delete the doc block at `src/substrate.rs:182-201` that explains the enum.
- Delete every `use` and every call site reference. Compiler will surface them.

### 1.2 Drop fold-on-write open-time reconciliation

- Delete the open-time fold pass in `Handle::open_with_options` (`src/substrate.rs:580-598` - the `for table in [Sessions, Messages, Parts]` block that calls `fold_and_create`).
- Open now opens datasets. No index work.

### 1.3 Drop `Handle::fold_and_create_indices` and the free `fold_and_create`

- Delete `Handle::fold_and_create_indices(table, WriteShape)` (`src/substrate.rs:721-747`).
- Delete the free `fold_and_create` function (`src/substrate.rs:976-1092`).
- Both are subsumed by `Handle::optimize_indices` (Phase 1.5).

### 1.4 Drop `IndexPolicy` and the policy field

- Delete the `IndexPolicy` struct + impl (`src/substrate.rs:163-180`).
- Remove the `policy: Arc<IndexPolicy>` field from `Handle` (`src/substrate.rs:404-407`) and from its constructor.
- Delete `policy_summary` helper (`src/substrate.rs:424-430`).
- `Handle::open_with_options` no longer takes an `IndexPolicy` argument. Update its signature.
- `Store::open_with_policy` collapses (Phase 2.2).

### 1.5 Introduce `Handle::optimize_indices` and friends

Add three new methods to `Handle`:

```
pub async fn optimize_indices(&self, table: Table, intents: &[IndexIntent]) -> Result<()>
pub async fn rebuild_index(&self, table: Table, intent: &IndexIntent) -> Result<()>
pub async fn index_status(&self, table: Table, intents: &[IndexIntent]) -> Result<Vec<IndexStatus>>
```

- `optimize_indices`: per-intent dispatch on `intent.params`:
  - `IndexParamsKind::Scalar(BuiltinIndexType::BTree)` -> `dataset.create_index(..., replace = true)`.
  - `IndexParamsKind::Scalar(BuiltinIndexType::Bitmap)` -> `dataset.optimize_indices(OptimizeOptions::append().index_names(vec![intent.name.into()]))`.
  - `IndexParamsKind::InvertedFtsNgram` -> same `optimize_indices(append)` path.
  - `IndexParamsKind::IvfPqCosine` -> same `optimize_indices(append)` path.
  - For intents not yet built where the trigger fires, run `create_index(..., replace = false)` first.
- `rebuild_index`: always `create_index(..., replace = true)`.
- `index_status` returns `(intent_name, fragments_covered, unindexed_rows, exists)` per intent.

Define a small `IndexStatus` struct in `src/substrate.rs` (no separate module). Wrap calls in `self.retry_lance(...)`.

### 1.6 FRI default

Lance's `optimize_indices` does not directly expose a `defer_index_remap` knob - that lives on `compact_files`. `pond index optimize` is supposed to do all three of compaction + index update + version cleanup in one atomic transaction per `docs/spec.md§7.8`. Inside `Handle::optimize_indices`, run Lance's full `optimize()` per table, which packages compaction + cleanup + index update. Pass the FRI flag where Lance accepts it. If Lance's at-pin API doesn't take a single `optimize()` call but exposes the three separately, sequence them in one retry-wrapped block and document the limitation in a one-line code comment.

### 1.7 Update IVF_PQ partition formula

- `src/substrate.rs:147-148`: replace `(count as f64).sqrt().round() as usize.clamp(32, 4096)` with `count.checked_div(4096).unwrap_or(0).max(1)` (or similar - floor at 1 so a tiny corpus still gets a partition).
- Update the doc comment at `src/substrate.rs:89-96` to cite the new formula per LanceDB's documented recommendation.

### 1.8 Update `merge_insert` / `merge_update` / shared `merge` docstrings

- `src/substrate.rs:620-624` (merge_insert): drop the "data + index in one atomic seam" claim; state: "Insert-only merge: append new rows, never overwrite a matched PK. Returns rows inserted. The fold lives separately under `Handle::optimize_indices` (`spec.md#index-write-decoupled`)."
- `src/substrate.rs:643-647` (merge_update): same pattern. State: "Update-only merge: `WhenMatched::UpdateAll` on matched PKs; unmatched rows dropped. The fold lives separately under `Handle::optimize_indices`."
- `src/substrate.rs:665-672` (shared `merge` body): keep the function, remove the prose about "the sessions layer enforces the contract by calling the fold at the end of every public write method" - that contract is dead.

### 1.9 Update `unindexed_row_count` docstring

- `src/substrate.rs:807-826`: drop "With the fold-on-write contract this is normally zero between commits." Replace with: "Count rows in `table` not yet covered by `index_name`. Manifest-only; a missing index reports the whole table. Powers `pond index status`."

### 1.10 Update `ConflictExhausted` and `is_commit_conflict`

No change needed. They are still load-bearing for `spec.md#protocol` typed conflict wire mapping. Leave intact.

### 1.11 `ensure_schema_matches` dim-rejection

`src/substrate.rs:1183-1228` rejects dim changes with "delete the data dir and re-ingest". Spec §5.5 now says a different-dim swap is an in-place column add + backfill + drop + rename. Soften the rejection: log a warning, allow open to proceed. The actual column-swap migration is operator-driven (out of scope here); the spec describes the procedure but `pond` does not need to automate it in v1.

### Verification after Phase 1

```
cargo build
grep -E "WriteShape|fold_and_create|IndexPolicy" src/  # zero hits
```

---

## Phase 2: Sessions layer cascade

### 2.1 Drop `Store::flush_indices`

- Delete `Store::flush_indices(shape: WriteShape)` (`src/sessions.rs:1096-1112`).
- Every caller becomes a no-op delete (Phase 3 for handlers; Phase 4 for tests).

### 2.2 Collapse `open_minimal` / `open_with_policy`

- Delete `Store::open_minimal` (`src/sessions.rs:181-186`).
- Delete `Store::open_with_policy` (`src/sessions.rs:188-200`).
- Delete `Store::open_local_with_vector_threshold` (`src/sessions.rs:214-226`).
- The only opener becomes `Store::open_with_options(location, storage_options)`. Update its signature to not take any policy parameter.
- A test-only constructor that accepts a custom vector activation threshold can stay if any test needs it, but it should pass the threshold via the runtime `pond_index_policy_with_vector_threshold` invocation when the test calls `optimize_indices` - not as part of open. Inline if possible.

### 2.3 Drop trailing fold calls in `upsert_*`

- `src/sessions.rs:228-242` (upsert_sessions): delete the `self.handle.fold_and_create_indices(...)` trailing call.
- `src/sessions.rs:462-486` (upsert_messages): delete trailing fold call.
- `src/sessions.rs:488-498` (upsert_parts): delete trailing fold call.
- Update the "Direct-API path" comment at lines 233-237 - it described the now-dead per-call fold. Replace with the one-liner: "Per-row outcomes; no fold here (see `pond index optimize`)."

### 2.4 Drop two-step `--force` model swap

- Delete `Store::stale_embedding_keys` (`src/sessions.rs:1042-1074`).
- Delete `Store::clear_embeddings` (`src/sessions.rs:1076-1094`).
- Delete `embedding_clear_batch` helper (`src/sessions.rs:2380-2403`).
- Keep `Store::drop_vector_index` (`src/sessions.rs:1117-1134`) - still used by the new --force flow.
- Keep `Store::stale_embedding_count` (`src/sessions.rs:1024-1040`) as a status probe only.

### 2.5 Update `pending_embedding_messages` to optionally include stale rows

Add an arg or a sibling method:

```
pub fn pending_or_stale_messages(&self) -> impl Stream<Item = Result<PendingMessage>> + '_
```

Filter: `(Predicate::IsNull("vector"), Or, Predicate::Ne("embedding_model", current_model))`. Drives the `--force` flow.

### 2.6 Drop `messages_id_btree`

- `src/sessions.rs:2063`: delete the line `("id", BuiltinIndexType::BTree, "messages_id_btree"),`.
- `pond get --message-id` lookups now full-scan. Add a one-line comment on the function that filters by `id` standalone (likely `find_message`, `src/sessions.rs:1205-1226`) noting the rare-path full-scan trade. No code change needed there.

### 2.7 BFloat16 vector column

- `src/sessions.rs:embedding_vector_type()` (around line 2358-2363): change child `DataType::Float32` to `DataType::Float16` (use `arrow_schema::DataType::Float16`, or if Lance requires BFloat16 specifically use the Lance extension). Lance v7 supports BFloat16 via the `BFLOAT16_EXT_NAME` extension on `FixedSizeBinary(2)`. Use whichever API Lance exposes natively at our pin.
- `embedding_update_batch` (`src/sessions.rs:2406-2445`): convert `Vec<f32>` source to BFloat16 array at write time. The `EmbeddedMessage::vector: Vec<f32>` field stays Float32 in memory.
- Read path: `vector_search` builds query keys as `Float32Array::from(query.to_vec())` (`src/sessions.rs:846, 887`). If Lance v7 at our pin accepts a Float32 query against a BFloat16 column with implicit conversion, leave as-is. If not, convert at the query boundary.
- The vector index params struct in `src/substrate.rs:92-96` may need a dim/dtype adjustment; verify `sub_vectors = embedding_dim / 8` still applies (it does for both Float32 and BFloat16; the PQ subspace count is dimension-based, not dtype-based).

If Lance at our pin doesn't expose a stable BFloat16 path you can wire today, fall back: keep Float32 in the schema but note in a code comment that the spec mandates BFloat16 and link `spec.md#datasets`. Surface the gap clearly so the next pass can address it.

### 2.8 Promote `options` to `pa.json_()`

- `src/sessions.rs:session_schema, message_schema, part_schema`: change `options` column from `DataType::Utf8` to Lance's `pa.json_()` extension type. In arrow-rs this is `DataType::LargeBinary` with metadata `{"ARROW:extension:name": "arrow.json"}` (or `"lance.json"` - check Lance's Arrow extension registry at the pin).
- Update `json_string` / `json_parse` helpers (search `src/sessions.rs` for these names) to convert `Value <-> Vec<u8>` JSONB encoding instead of `Value <-> String`. JSONB encoding is defined by Lance's `lance-format` JSON support - read `~/pjv/lance-format/lance/rust/lance-format/src/...` for the canonical reader/writer if needed.
- Every batch builder (`sessions_batches`, `messages_batches`, `parts_batches`) updates the options column array type accordingly.
- Every batch reader path that pulls `options` updates from `StringArray` to the JSONB binary array type.
- The byte-budget guards (`COLUMN_BYTE_BUDGET` and friends) still apply to the encoded byte count; no logic change.

### 2.9 Promote `variant_data` to `pa.json_()`

Same treatment as options for the `parts.variant_data` column. The serialization of Part variants (TextPart, FilePart, ToolCallPart, etc.) into a single JSON column stays - only the storage encoding changes from Utf8 text to JSONB binary.

### 2.10 Update `pond_index_policy` doc

- `src/sessions.rs:2160-2164`: rewrite. New text: "Pond's production IndexPolicy: the per-table intent set `Store::open_with_options` registers with the substrate." Drop the `spec.md#fold-on-write` anchor.

Note: if `IndexPolicy` itself is being removed at the substrate layer (Phase 1.4), keep it as a lightweight struct here in sessions.rs - it still serves as the intent registry that `Store` passes to `Handle::optimize_indices`. The change is that it is no longer baked into the substrate at open; it is just data the sessions layer maintains and hands to substrate ops.

### 2.11 New Store wrappers for the index verbs

Add to `Store`:

```
pub async fn optimize_indices(&self) -> Result<()>
pub async fn rebuild_indices(&self, intent: Option<&str>) -> Result<()>
pub async fn index_status(&self) -> Result<Vec<IndexStatus>>
```

Each iterates the three tables and calls the corresponding `Handle::*` method with `pond_index_policy()` intents.

### Verification after Phase 2

```
cargo build
grep -E "flush_indices|clear_embeddings|stale_embedding_keys|embedding_clear_batch|open_minimal" src/  # zero hits in src/
grep "messages_id_btree" src/  # zero hits
```

---

## Phase 3: Handlers + Main (CLI surface)

### 3.1 Remove `flush_indices` calls in handlers

- `src/handlers.rs:374-376` (ingest_adapter end): delete the `flush_indices` call.
- `src/handlers.rs:527-530` (ingest_events end): delete the `flush_indices` call.
- The comments above each of these calls (Findings 82, 84 in the audit) shorten accordingly.

### 3.2 Remove the INDEX_BACKLOG_WARN block

- `src/handlers.rs:1115-1173` block (the constant + the warn-on-large-backlog code path in `pond_search`): delete entirely. Trailing indexes are no longer "should self-heal on next open" - they are the steady state until `pond index optimize` runs. Operators discover backlog via `pond index status`, not via opportunistic warnings.

### 3.3 Add `Command::Index` group

In `src/main.rs`:

- Add a top-level `Index { #[command(subcommand)] command: IndexCommand }` to the `Command` enum.
- Add `IndexCommand` enum: `Status`, `Optimize { #[arg(long)] wait: bool }`, `Rebuild { intent: Option<String> }`.
- Match arm dispatches to `Store::index_status`, `Store::optimize_indices`, `Store::rebuild_indices`.
- `--wait` polls `Store::index_status` until every intent reports `num_unindexed_rows == 0` (with a reasonable timeout - choose 10 minutes by default).

### 3.4 Add `--explain` to `Command::Search`

- Add `#[arg(long)] explain: bool` to the existing `Search` command struct.
- When set, route through `Store::explain_search_plan` (or a renamed `explain_vector_plan`) that returns Lance's `analyze_plan` output for both retrievers and prints it. The function at `src/sessions.rs:879-893` is the starting point; extend or duplicate as needed for the FTS arm.

### 3.5 Drop `pond sync` / `pond embed` index work

- `src/main.rs:531-546` (Embed handler spinner + `flush_indices(WriteShape::ColumnUpdate)`): delete the "folding indices..." spinner block and the flush call.
- `src/main.rs:416-431` (sync post-write backlog warning yellow message): delete. `pond status` and `pond index status` are the surfaces for index health now.
- `pond sync` and `pond embed` become pure data writes per the spec.

### 3.6 Refactor `pond embed --force`

- `src/main.rs:443-477`: replace the current four-step (count stale, enumerate keys, clear_embeddings, drop_vector_index) flow.
- New flow:
  1. `store.stale_embedding_count()` - still useful for the operator print before the swap runs.
  2. `store.drop_vector_index()` - run before the merge so the old IVF_PQ centroids are gone.
  3. Run the embed worker over `store.pending_or_stale_messages()` (the new method from Phase 2.5).
  4. The worker's `write_embeddings` becomes a conditional merge_update. Two implementation options - pick one:
     - **Option A (simpler)**: keep `write_embeddings` unconditional. The `--force` filter at the worker level (`pending_or_stale_messages`) already only feeds stale rows. End behavior matches spec.
     - **Option B (literal match to spec §5.5 prose)**: add `Handle::merge_update_conditional(table, batch, predicate)` where predicate is a SQL string passed as `WhenMatched::UpdateIf(...)`. Predicate: `target.embedding_model IS NULL OR target.embedding_model != source.embedding_model`. Use this from the `--force` path. The NULL-handling is critical: without it, never-embedded rows (NULL embedding_model) would not match because SQL `NULL != 'x'` evaluates to NULL (false).
  - Recommend Option A. It is simpler, behaviorally equivalent, and the spec prose "conditional merge_update keyed on..." can be read as a semantic description rather than a literal `WhenMatched::UpdateIf` requirement. Add a one-line code comment citing `spec.md#embeddings-are-derived` and noting the equivalence.

### 3.7 Update `pond status` rendering

- `src/main.rs:1147-1237` (render_status): keep totals, storage breakdown, embedding coverage, per-adapter/project rollup.
- Drop the yellow "self-heals on next open" warnings tied to backlog (`src/main.rs:1198-1237`).
- Add a final block that summarizes `pond index status` output: one line per (table, intent) with covered fragments / unindexed counts and a hint pointing at `pond index optimize` if any is non-zero.

### 3.8 Drop `open_minimal` plumbing in CLI

- `src/main.rs:333-352, 605-647, 661-680, 696-700`: every site that calls `Store::open_minimal` switches to `Store::open_with_options`. The "Read-only verb -> skip the open-time fold-and-create pass" comments go away (Phase 5 covers comment cleanup).

### 3.9 Add `[search]` config

- `src/config.rs`: add `pub struct SearchConfig { pub nprobes: Option<usize>, pub refine_factor: Option<u32> }`. Wire as `Config.search: SearchConfig` with serde default.
- `DEFAULT_CONFIG_TOML` (around `src/config.rs:97-160`): add a `[search]` block with both fields commented out.
- `Store::vector_search` (`src/sessions.rs:838-875`) accepts an optional `SearchConfig` argument and threads to `scanner.nearest(...).nprobes(...).refine_factor(...)`.
- Pass through from `pond_search` handler.

### Verification after Phase 3

```
cargo build
cargo run -- index status   # smoke test
cargo run -- index optimize # smoke test
cargo run -- search --explain "foo"  # smoke test
```

---

## Phase 4: Test rationalization

### 4.1 Delete tests outright

Delete these tests (function body + any `#[cfg(test)] mod tests` block lines they require):

- `src/sessions.rs::tests::fold_on_write_holds_after_ingest_and_embed` (around line 3627)
- `src/sessions.rs::tests::open_recreates_an_implied_missing_index` (around line 3660)
- `src/sessions.rs::tests::model_swap_force_path_clears_and_rebuilds_ivf_pq` (around line 3730) - see 4.3 for replacement
- `src/sessions.rs::tests::fold_on_write_invariants_hold_after_random_writes` (proptest, around line 3814)

### 4.2 Delete `tests/integration/fold_on_write.rs` entirely

- Delete the file `tests/integration/fold_on_write.rs`.
- Remove the corresponding `#[path = "integration/fold_on_write.rs"] mod fold_on_write;` line from `tests/integration.rs`.

The one assertion in that file worth keeping (`pond_search` retrieves a verbatim phrase) is already covered by `tests/integration/search.rs::search_picks_hybrid_or_fts_based_on_embedder_state`. Confirm coverage; if not, fold a single phrase-retrieval assertion into search.rs with an explicit `store.optimize_indices().await?` call between ingest and search.

### 4.3 Replacement test for `model_swap_force_path...`

In `src/sessions.rs::tests`, add `model_swap_force_re_embeds_only_stale_rows_and_rebuilds_ivf_pq`:

- Ingest N messages, embed them under model A.
- Run `optimize_indices` to build IVF_PQ.
- Reconfigure embedder to model B (use the test backend that returns deterministic vectors tagged by model id).
- Drive the `--force` flow.
- Assert: (a) rows previously embedded under A now show `embedding_model = B`; (b) IVF_PQ is dropped post-merge; (c) `optimize_indices` rebuilds it; (d) up-to-date rows under B are not rewritten (use a row-version check or a write-count probe).

### 4.4 Update 2 tests for new optimize hook

- `src/sessions.rs::tests::filtered_vector_scan_pushes_scalar_predicate_into_the_index` (around line 3494): replace `store.flush_indices(WriteShape::ColumnUpdate)` with `store.optimize_indices()`. Keep all assertions verbatim.
- `src/sessions.rs::tests::vector_index_activates_when_threshold_is_crossed_inline` (around line 3534): rename to `vector_index_activates_when_threshold_is_crossed`. Replace every `flush_indices` call with `optimize_indices`. Drop comments about "fold-on-write" / "inline" (Phase 5).

### 4.5 Drop `proptest` dev-dep + regressions

- `Cargo.toml`: remove `proptest = "1"` from `[dev-dependencies]`.
- Delete the `proptest-regressions/` directory at the repo root.

### Verification after Phase 4

```
cargo test
# Test count drops from ~97 to ~92 (-6 delete, +1 replacement).
```

---

## Phase 5: Comment cleanup

CLAUDE.md `## Comments` rule: a good pond comment names the WHY a reader cannot see from the code itself. Apply ruthlessly.

### 5.1 Delete banner dividers

Seven banner lines (`// ----- foo -----` / `// -- foo -----------`):

- `src/sessions.rs:3286` (`// -- ingest_immutable: ...`)
- `src/sessions.rs:3402` (`// -- vector search and index activation ---`)
- `src/adapter/mod.rs:342` (`// -- shared helpers used by file-tree-based adapters ---`)
- `src/adapter/extract.rs:157` (`// ----- serde integration -----`)
- `src/adapter/extract.rs:181` (`// ----- Source trait -----`)
- `src/adapter/extract.rs:236` (`// ----- extract_* helpers -----`)
- `src/adapter/extract.rs:291` (`// ----- impl Source for serde_json::Value -----`)

### 5.2 Delete WHAT-not-WHY narrations

- `src/sessions.rs:279-282` (`// Step 2 - in-batch dedup...`): keep one line of WHY about ordering matching input substreams.
- `src/sessions.rs:351-352` (`// Step 1 - immutable check...`): delete.
- `src/sessions.rs:384` (`// Build the three flat record batches...`): delete.
- `src/handlers.rs:347-348` (`// Close the last in-flight substream...`): delete.
- `src/sessions.rs:1126` (`// The index simply was not there - fine...`): delete.

### 5.3 Drop stale `spec.md#fold-on-write` anchors

Every site referencing `(spec.md#fold-on-write)`. Use rg to find them:

```
rg 'spec\.md#fold-on-write' src/
```

For each hit, drop the parenthetical anchor. If a useful WHY remains in the same comment, keep it (cite `spec.md#index-write-decoupled` or `spec.md#index-per-family` if a new anchor applies). If the whole comment was anchor + restatement, drop the whole comment.

### 5.4 Rewrite contradiction comments

Four places where a doc claims fold-is-atomic-with-write (the new code makes them separate):

- `src/substrate.rs:620-624` (merge_insert doc) - Phase 1.8 covers this.
- `src/substrate.rs:643-647` (merge_update doc) - Phase 1.8 covers this.
- `src/substrate.rs:665-672` (merge body doc) - Phase 1.8 covers this.
- `src/main.rs:465-468` (`// ... fold-on-write-prunes the rewritten fragments...`) - rewrite to: "Drop the IVF_PQ outright before the merge; centroids belong to the prior distance space."

### 5.5 Drop PR-archeology language

Comments using "previously", "the previous X", "new pattern replaces old Y", "removed", "after refactor". Sweep:

- `src/sessions.rs:1395-1404` (drop "(which previously carried only a count)")
- `src/sessions.rs:1432-1440` (drop "Keeping the populations distinct is the whole point of the new...")
- `src/sessions.rs:1515-1531` (drop "The N-events-per-rejection cascade from the prior contract is gone")
- `src/sessions.rs:1841-1847` (drop "Earlier versions cascaded N error rows...")
- `src/handlers.rs:310-314` (drop "We never reset the validator on these any more")
- `src/handlers.rs:368-376` (drop "the regression that moved this call out of upsert_session_batch")
- `src/wire.rs:217-220` (drop "the previous" framing on the synthesis-sentinel list)
- `src/wire.rs:226-237` (same)
- `src/wire.rs:520-526` (drop "per invariant 2")
- `src/wire.rs:18-22` (drop "invariant 19")
- `src/adapter/codex_cli.rs:584-586` (drop "The previous join-aggregation hack")
- `src/adapter/codex_cli.rs:755-758` (drop "the previous `function` sentinel")
- `src/embed/e5.rs:18-20` (drop "unlike the previous ONNX backend")

### 5.6 Replace numbered spec citations

The spec uses named-anchor mnemonics, not numbered subsections. Replace:

- `src/transport.rs:204-205`: drop "(per 3.6.5 ...)".
- `src/transport.rs:247-248`: drop "(3.6.1)".
- (already covered above) `src/wire.rs:18, 520`: drop "invariant 19" / "per invariant 2".

### 5.7 Shorten overlong WHY blocks

Per CLAUDE.md "Keep each as short as the WHY allows". The audit identified ~58 multi-line blocks where a single sentence carries the WHY. Compress in place. Examples:

- `src/substrate.rs:43-48` (IndexIntent doc) -> "Declarative description of one index pond keeps on a table. Created when its trigger fires; folded forward by `pond index optimize`."
- `src/sessions.rs:243-268` (upsert_session_batch step list) -> "Batched write path: validates, dedupes (substream-level), and commits three parallel merge_insert calls. One commit per table per batch."
- `src/sessions.rs:1515-1531` (IngestValidator doc) -> "Turns the events: array into RowOutcomes in input-array order. Per-event validation errors drop one event and continue the substream; only Session-level invariant violations drop the whole substream. Writes batch through `flush` / `finish`."

The full list of rewrite targets is in the comment audit report; use it as a checklist. Net target: drop comment count from ~2412 to ~2200 lines (~8-10%).

### 5.8 Comments that stay

The audit explicitly preserved these as load-bearing. Do not touch:

- `src/substrate.rs:209-211` (ConflictExhausted attribution)
- `src/substrate.rs:592-594` (open-time conservative-shape comment - though the block it documents is being deleted; remove with the block)
- `src/substrate.rs:694-696` (FirstSeen dedupe WHY)
- `src/substrate.rs:779-783` (Lance autoprojection deprecation note - keep)
- `src/sessions.rs:564-571` (oldest_visible_ts fallback for pruned versions)
- `src/sessions.rs:795-801, 862-866` (stable secondary sort for tied scores)
- `src/sessions.rs:2256-2260` (write_params_for_create WHY) - update the "90 days" to match the new short window per spec §3.4
- `src/handlers.rs:153-159` (ADAPTER_FLUSH_BATCH calibration)
- `src/handlers.rs:1424-1430` (RankedList asymmetric-k citation)
- `src/handlers.rs:1466-1474, 1488-1492` (RRF session-root keying / dedup_rank WHYs)
- `src/handlers.rs:1555-1562` (unicode case-fold byte-offset pitfall)
- `src/handlers.rs:1650-1652` (block_in_place CPU-bound)
- `src/main.rs:443-447` (model-swap silent-correctness landmine WHY)
- `src/main.rs:1124-1126` (humansize-not-needed decision)
- `src/main.rs:1371-1375` (render_search_envelope pipe semantics)
- `src/main.rs:1627-1632, 1657-1662` (faithful None vs sentinel rendering)
- `src/transport.rs:60-64` (HTTP_BODY_LIMIT_BYTES WHY)
- `src/transport.rs:451-454` (MCP placeholder rewrite WHY)
- `src/wire.rs:149-152` (Provenance no-Default WHY) - keep, drop only the `spec.md#provenance-required` anchor if it does not survive (verify against current spec.md)
- `src/wire.rs:532-538` (default_rrf_k citation)

### Verification after Phase 5

```
rg 'spec\.md#fold-on-write|spec\.md#atomic-data' src/  # zero hits
rg 'previous|previously|formerly|prior to' src/ | wc -l   # near zero
rg '// ----- |// -- ' src/ # zero banner hits
```

---

## Phase 6: Module + dependency hygiene

### 6.1 Inline `src/embed/` folder

Per the module audit: with qwen3 phased out, the folder holds one backend. Fold it.

- Move the content of `src/embed/e5.rs` (163 lines) into `src/embed/mod.rs`.
- Rename the file: move `src/embed/mod.rs` to `src/embed.rs` (top-level file like `src/wire.rs`).
- Remove the empty `src/embed/` directory.
- Update the module declaration in `src/lib.rs` from `pub mod embed { ... }` form to `pub mod embed;` if needed (verify the existing form).
- Update imports across the codebase. `use pond::embed::e5::E5Embedder` becomes `use pond::embed::E5Embedder`.
- Update `Cargo.toml` `path = "..."` if any direct path was used (unlikely).

### 6.2 Dependencies

Per the dep audit: nothing to remove except `proptest` (already covered in Phase 4.5). All other deps are justified. Verify:

```
cargo tree -e normal --depth 1
```

No bumps. `humantime` is not a direct dep at our pin (it shows up only transitively in `Cargo.lock`).

### Verification after Phase 6

```
cargo build
cargo clippy -- -D warnings
cargo test
```

---

## Phase 7: Final verification

### 7.1 Hygiene checks

```
# No stale terminology
rg 'fold-on-write|WriteShape|pond compact|Atomic data \+ index commit|ColumnUpdate' src/   # zero
rg 'messages_id_btree' src/                                                                # zero
rg 'flush_indices' src/                                                                    # zero
rg 'open_minimal' src/                                                                     # zero

# Build / lint / test
cargo build
cargo clippy -- -D warnings
cargo fmt --check
cargo test
```

### 7.2 Branch diff target

```
git diff main..HEAD --stat
```

Target net diff against main: roughly +500 / -200 lines (down from current +3195 / -582). Major sources of the reduction:

- WriteShape + IndexPolicy + fold_and_create infrastructure removed from substrate.rs.
- Store::flush_indices + clear_embeddings + stale_embedding_keys + embedding_clear_batch removed from sessions.rs.
- INDEX_BACKLOG_WARN block removed from handlers.rs.
- Two-step --force replaced with one-step in main.rs.
- 4 tests in src/sessions.rs deleted + 1 added; whole tests/integration/fold_on_write.rs deleted.
- ~200 comment lines removed across all files.

### 7.3 Smoke test against a real data dir

```
# Pre-existing data dir or fresh one
cargo run -- status
cargo run -- index status
cargo run -- index optimize --wait
cargo run -- search --explain "embedding"
cargo run -- get --session-id <some-id>
```

All four should run cleanly. `pond index optimize --wait` should complete in finite time on a non-trivial corpus.

### 7.4 Commit shape

When ready, the work commits as a focused sequence (not one giant commit):

- `refactor(substrate): drop WriteShape / IndexPolicy / fold_and_create; introduce optimize_indices` (Phases 1.1-1.6, 1.10)
- `refactor(substrate): IVF_PQ partitions = num_rows // 4096; soften dim-mismatch` (Phases 1.7, 1.11)
- `refactor(sessions): drop flush_indices and the two-step --force model swap` (Phases 2.1, 2.3, 2.4, 2.5)
- `refactor(sessions): drop open_minimal; collapse Store openers` (Phase 2.2)
- `feat(schema): promote options + variant_data to pa.json_() JSONB; vector to BFloat16; drop messages_id_btree` (Phases 2.6, 2.7, 2.8, 2.9)
- `feat(cli): pond index status/optimize/rebuild; pond search --explain; drop sync/embed fold work` (Phases 3.1-3.8)
- `feat(config): [search] section with nprobes / refine_factor` (Phase 3.9)
- `test: rationalize the suite for index-write-decoupled` (Phase 4)
- `style: comment hygiene per CLAUDE.md` (Phase 5)
- `refactor: inline src/embed/ to top-level embed.rs` (Phase 6.1)

Each commit should pass `cargo build && cargo clippy -- -D warnings && cargo test` independently. Squash if/when the final PR back to main is prepared.

---

## What this plan does NOT do (deferred per spec §9)

For codex's confidence: explicitly do not implement any of these. They appear in `docs/spec.md§9` as deferred items.

- §9.1 Future consumers (resources/blobs, social archives, files-API store, versioned-document store)
- §9.2 Future source adapters (Managed Agents, OpenCode, Cursor, aider, Gemini CLI)
- §9.3 Provider-target restore (Anthropic, OpenAI, Bedrock, Gemini API shapes)
- §9.4 Live-write (MemWAL-style streaming ingest)
- §9.5 Hosted multi-tenant
- §9.6.1 Remote embedding providers
- §9.6.2 Cross-session attachment deduplication
- §9.6.3 Indexing file-attachment contents
- §9.6.4 Typed image arrays for image-typed FileParts
- §9.6.5 JSON-path scalar indices on options
- §9.6.6 Graph-traversal layer over fork lineage
- §9.6.7 Wire-surfaced time-travel queries
- §9.6.8 OTel-compatible projection
- §9.6.9 `pond tag` verb
- §9.7 Open questions

Anything in those sections is out of scope for this cleanup. If a phase appears to require any of them, stop and re-read the spec to confirm; if confirmed required, surface it as a question rather than implementing.

## Reference: the substrate seam after cleanup

For codex's mental model after Phase 1 lands:

`Handle` keeps:

- `open_with_options(location, storage_options) -> Handle` (no policy arg).
- `merge_insert(table, batch, row_count) -> u64` (data only).
- `merge_update(table, batch, row_count) -> u64` (data only).
- `scan / scanner / count_rows` (read seam, unchanged).
- `optimize_indices(table, intents) -> ()` (new; runs from `pond index optimize`).
- `rebuild_index(table, intent) -> ()` (new; runs from `pond index rebuild`).
- `index_status(table, intents) -> Vec<IndexStatus>` (new; powers `pond index status`).
- `drop_index(table, name) -> ()` (kept; used by `--force`).
- `unindexed_row_count(table, index_name) -> usize` (kept; powers index_status internals).
- `retry_lance` (private helper, kept).
- `ConflictExhausted` + `is_commit_conflict` (kept; wire mapping).

`Handle` loses:

- `IndexPolicy` struct and `policy` field.
- `fold_and_create_indices(table, shape) -> ()` method.
- Free `fold_and_create` function.
- `WriteShape` enum.
- Open-time fold pass.

`Store` keeps every public read/write method but:

- One opener: `open_with_options`. `open_minimal` / `open_with_policy` / `open_local_with_vector_threshold` are gone.
- `flush_indices` is gone.
- `clear_embeddings`, `stale_embedding_keys`, `embedding_clear_batch` are gone.
- `unindexed_message_backlog` / `unindexed_vector_backlog` stay (now powering `pond index status`).
- New wrappers: `optimize_indices`, `rebuild_indices`, `index_status`, `pending_or_stale_messages`.

`IndexIntent` stays as the declarative description; the fold strategy lives inside `Handle::optimize_indices` per the per-family table.

Per-family fold table (matches `spec.md#index-per-family`):

| `IndexParamsKind` | Fold strategy |
|---|---|
| `Scalar(BTree)` | `create_index(replace = true)` (Lance v7.0.0-beta.16 BTree-flat-scan bug workaround) |
| `Scalar(Bitmap)` | `optimize_indices(append)` |
| `InvertedFtsNgram` | `optimize_indices(append)` |
| `IvfPqCosine` | `optimize_indices(append)` |

IVF_PQ partition formula: `num_rows // 4096` (floor at 1).

Activation threshold for IVF_PQ build: 100,000 non-null vectors (kept from current).
