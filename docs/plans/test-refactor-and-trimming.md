# Test refactor and trimming plan

Goal: bring `tests/` into line with CLAUDE.md ("unit tests live in `#[cfg(test)] mod tests` at the bottom of the source file they test; `tests/` is reserved for genuine cross-module integration suites only"), and trim tests that exist for the sake of tests.

Status: not started. No edits land before each stage is approved.

## Baseline

- 11 files / 2,816 lines under `tests/`.
- Inline `#[cfg(test)] mod tests` already exists in 5 source files: `src/sessions.rs`, `src/adapter/claude_code.rs`, `src/adapter/codex_cli.rs`, `src/adapter/extract.rs`, `src/embed/qwen3.rs`. Their style is the template:

```
#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use crate::{...};
    use tempfile::TempDir;
    // helpers + tests
}
```

- `Cargo.toml` dev-dependencies (`tempfile`, `tower`, `rmcp[client]`) are already available to inline tests; no Cargo changes required.
- Bonus from going inline: `Extracted::from_test_value` is `#[cfg(test)]` and crate-private, so external integration tests in `tests/` cannot see it. Today, six `tests/` files carry an `s()` helper that wraps a JSON shim just to build an `Extracted<String>`. When tests move inline they call the test constructor directly and those shims disappear.

Expected end state: ~7 files / ~1,200 lines under `tests/`, ~+850 lines of inline tests in `src/`.

## Stage 0: baseline

1. `cargo test --workspace --no-fail-fast` and capture the passing-test list. Every retained test must keep passing under its name (renames excepted).
2. `cargo test -- --list` for the diff target.

## Stage 1: move pure-unit tests inline (mechanical)

One commit per destination source file.

### 1a. `src/config.rs` (new inline `mod tests`)

From `tests/embed.rs`:
- `builtin_registry_validates` (trim, see Stage 3)
- `registry_rejects_unknown_model`
- `registry_rejects_dim_mismatch`
- `registry_rejects_missing_and_duplicate_defaults`
- `registry_rejects_oversized_max_embed_tokens`
- `config_load_merges_namespace_overrides`
- `config_load_missing_file_falls_back_to_builtin`
- `default_config_toml_loads_to_the_builtin_registry`
- `resolve_data_dir_follows_explicit_then_xdg_then_home`

From `tests/sources_and_discovery.rs`:
- `expand_home_under_handles_tilde_forms`
- `resolve_sources_returns_one_or_all_or_errors`

From `tests/remote_backend.rs`:
- `memory_uri_is_classified_as_remote`

### 1b. `src/adapter/mod.rs` (new inline `mod tests`)

From `tests/sources_and_discovery.rs`:
- `each_factory_probes_its_default_under_an_injected_home`
- `known_names_covers_every_registered_adapter` (trim, see Stage 3)
- `prompt_and_persist_errors_on_non_tty_stdin`

After 1a+1b, `tests/sources_and_discovery.rs` is empty and gets deleted.

### 1c. `src/substrate.rs`

No moves. The only candidate (`conflict_exhausted_sentinel_round_trips_through_anyhow_chain`) is deleted in Stage 3 Pattern A as a test of `anyhow` rather than pond.

### 1d. `src/wire.rs` (new inline `mod tests`)

From `tests/conflict.rs`:
- `wire_envelope_carries_conflict_code_and_attempts_detail`

### 1e. `src/embed/mod.rs` (new inline `mod tests`)

From `tests/embed.rs`:
- `qwen3_query_instruction_wraps_the_query_in_the_model_card_prefix`

From `tests/search.rs`:
- `metric_type_maps_each_registry_distance`

### 1f. `src/handlers.rs` (new inline `mod tests`)

From `tests/search.rs` (the "Pure functions" block, lines 30-223):
- `rrf_merge_fuses_retrievers_and_reports_provenance`
- `recency_boost_matches_the_kb_formula`
- `make_preview_truncates_at_code_point_boundary`
- `build_filter_pushes_down_each_predicate`
- `build_filter_rejects_bad_role_and_date`
- `empty_filters_produce_no_predicate` (trim, see Stage 4)
- `build_filter_contains_escapes_like_wildcards`
- `plan_search_trims_query_caps_limit_and_sizes_pools`
- `plan_search_keeps_small_limits_from_starving_retrievers`
- `plan_search_builds_the_shared_filter_predicate` (trim, see Stage 4)
- `plan_search_rejects_invalid_composition_before_execution`

After Stage 1, `tests/search.rs` shrinks to its synthetic-dataset and handler-integration sections. `tests/conflict.rs` has only the multi-Store concurrency test left.

## Stage 2: move Store-method unit tests inline

`tests/search.rs` has three tests that drive `Store` methods directly (no adapter, no wire, no transport). They belong in `src/sessions.rs` next to the existing inline tests:

- `filtered_vector_scan_pushes_scalar_predicate_into_the_index`
- `vector_index_activates_past_the_row_threshold`
- `vector_search_is_scoped_to_one_embedding_identity`

The `synthetic_rows` helper (~30 lines) moves with them.

After Stage 2, `tests/search.rs` is ~250 lines, all handler-level integration.

## Stage 3: trim "for sake of tests" historical tests

These are independent deletes / fold-ins. Each is a judgment call. Approval required per item.

### A. Tests of someone else's library, not pond

- DELETE `tests/conflict.rs::conflict_exhausted_sentinel_round_trips_through_anyhow_chain` - asserts the `anyhow::Error::context(...)` + `downcast_ref` contract, not pond logic. If `is_commit_conflict` regresses, the Store concurrent-writer test catches it.
- DELETE `tests/lance_smoke.rs::merge_insert_do_nothing_skips_insert_only_rows` - pond never uses `WhenNotMatched::DoNothing`.
- DELETE `tests/lance_smoke.rs::blob_v2_struct_column_round_trips` - pond's blob storage is covered by `src/sessions.rs::file_part_blob_v2_round_trips_through_get`.
- DELETE `tests/lance_smoke.rs::cleanup_old_versions_accepts_delete_unverified` - covered by `tests/maintenance.rs`.
- KEEP `tests/lance_smoke.rs::merge_insert_uses_unenforced_primary_key_for_find_or_create` - the load-bearing primitive `sessions.rs` upserts depend on.

After this section: `tests/lance_smoke.rs` shrinks to ~50 lines / 1 test.

### B. Smoke tests with no behavioral assertion

- DELETE `tests/remote_backend.rs::store_open_with_options_threads_storage_options_through_lance` - the options are inert against `memory://`; the test asserts only that the call did not error. Re-add a real test when the S3 backend lands.
- DELETE `src/adapter/extract.rs::the_seal_documentation_smoke` - empty function body; the comment says "this test does nothing, the doctest above is the real check." Rustdoc picks up the `compile_fail` block regardless.

### C. Tests of hard-coded constants against themselves

- DELETE `tests/sources_and_discovery.rs::known_names_covers_every_registered_adapter` - `known_names()` returns `["claude-code", "codex-cli"]`; the test asserts both strings appear. The implementation is the assertion.
- DELETE `tests/embed.rs::builtin_registry_validates` - every other test using `Config::builtin().embeddings.default_model("local")` exercises validation by construction.

### D. Redundant variants of the same probe

- FOLD `tests/search.rs::empty_filters_produce_no_predicate` into `build_filter_pushes_down_each_predicate` as a `vec![(SearchFilters::default(), "")]` row.
- FOLD `tests/search.rs::plan_search_builds_the_shared_filter_predicate` into a single table-driven `plan_search_planner` test together with the other `plan_search_*` cases.
- DELETE `tests/embed.rs::embed_worker_caps_batch_cost_for_long_messages` - strictly weaker than `embed_worker_respects_cost_budget`, which asserts the numeric `count * max_cost^2 <= budget` invariant directly.

### E. Synthetic-shim tests of code paths nothing in production reaches

- DELETE `src/adapter/claude_code.rs::system_message_content_none_round_trips_as_none` - its own comment is candid: claude-code's format does not produce `content: None`; the test bypasses the claude-code adapter and exercises `Store` directly. The codex-cli developer-frame path covers the real round-trip.

## Stage 4: tighten weak assertions (don't delete)

- TIGHTEN `tests/conformance.rs::ingest_adapter_emits_discovered_then_session_done_for_each_session` - replace `assert!(done_count >= discovered_total)` with `assert_eq!(done_count, discovered_total)`, allowing an explicit set of legitimate skipped-file extras. The `>=` form is satisfied by buggy implementations that emit extras.
- TIGHTEN `tests/recovery.rs::export_filtered_to_one_session_carries_only_that_session` - the `Part` branch currently `continue`s because parts only carry `message_id`. Build a `message_id -> session_id` map from the Session and Message events in the filtered export, then assert every Part's `message_id` belongs to the requested session.

## Stage 5: rename for clarity

- `tests/conformance.rs` -> `tests/claude_code_ingest.rs` (what it tests).
- `tests/conflict.rs` -> rename to `tests/store_concurrency.rs` (one surviving test after Stage 3 trims the anyhow round-trip and Stage 1d moves the envelope test to `src/wire.rs`).

## Stage 6: verify

1. `cargo test --workspace --no-fail-fast` - every baseline name from Stage 0 must still pass (deletes and renames excepted).
2. `cargo clippy --all-targets -- -D warnings` - inline `mod tests` must satisfy clippy under the existing `#![allow(clippy::expect_used, clippy::unwrap_used)]`.
3. `cargo build --release` - confirm no inline test code leaks into release.
4. `git diff --stat` - confirm `tests/` net-shrunk and `src/` net-grew by roughly the same magnitude.

## Final shape

Files under `tests/` after Stage 5:

- `tests/claude_code_ingest.rs` (was `conformance.rs`, ~170 lines)
- `tests/maintenance.rs` (33 lines, unchanged)
- `tests/recovery.rs` (~200 lines if the concurrency test folds in)
- `tests/remote_backend.rs` (~100 lines after the unit test leaves and one smoke test is dropped)
- `tests/search.rs` (~250 lines, handler integration only)
- `tests/transport_http.rs` (317 lines, unchanged)
- `tests/transport_mcp.rs` (268 lines, unchanged)
- `tests/embed.rs` (~150 lines, worker integration only)
- `tests/lance_smoke.rs` (~50 lines, one test left) OR deleted if its surviving test moves into `src/sessions.rs::tests`
- `tests/fixtures/` (unchanged)

Deleted: `tests/sources_and_discovery.rs`, `tests/conflict.rs`.

## Decisions

- Q1: all 8 Stage 3 trims approved.
- Q2: both Stage 4 tighten edits approved.
- Q3: `tests/conflict.rs` -> `tests/store_concurrency.rs` (rename, do not fold).
- Q4: entire refactor lands as ONE commit, verified healthy.

## Execution order rationale

Stages 1-2 are mechanical and risk-free: every test keeps its name, body, and assertions; only its physical location and (where applicable) the `s()` helper change. Per-destination commits keep diffs small.

Stage 3 is independent trim work; each item is approvable on its own.

Stage 4 is two tighten edits, low risk.

Stage 5 is renaming, lowest priority. Stage 6 is verification.
