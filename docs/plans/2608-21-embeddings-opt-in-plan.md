# Embeddings opt-in: fts is the default arm, the model loads only when asked (#164)

Self-contained implementation plan for [issue #164](https://github.com/tenequm/pond/issues/164), developed on PR #168 (branch `feat/164-embeddings-opt-in`). Written so that an implementer does not need to re-derive anything: every change site has the symbol name, the current code, and the target code. Line numbers cite `main` at `d47ec3c` (2026-08-21); **they drift, symbol names do not - locate with `rg -n '<symbol>'`, never by line number alone.**

Decisions in section 1 were settled with the operator on 2026-08-21 after three engineering reviews (storage/Lance, API/integrations, ops/release). Implement them; do not re-litigate. If the code you find contradicts a claim here, stop and report before improvising.

Read first: `CLAUDE.md` (repo rules - minimalism, test placement, release-plz), `docs/spec.md` sections 7.4, 7.8, 8.

---

## 1. Decisions (settled)

| # | Decision |
|---|---|
| 1 | New config field `[embeddings].enabled: bool`, default `false`. The gate is a **local config read** installed into a process-wide `OnceLock` at startup. No store probe, no auto-detect of existing vectors, no migration. |
| 2 | Default `mode` for `pond_search` flips from `vector` to `fts` on every surface: MCP stdio (`pond mcp`), HTTP (`pond serve`), CLI (`pond search`). |
| 3 | When disabled, `mode=vector` is **refused**. The check is the first thing `resolve_effective_mode` does - before `has_embeddings()` (which on a never-embedded store is a full `vector`-column read). No fallback to fts, no model load. On MCP the refusal is a `CallToolResult::error` (the model reads it in-turn and retries with fts), not a JSON-RPC error (see 5.2 for why). |
| 4 | When disabled, no process ever constructs an embedder or loads a model: `sync`, `serve`, `serve --with-sync`, `mcp`, `optimize`, `copy`, archive restore. Ingest writes null `vector` / `embedding_model`. |
| 5 | When disabled, the IVF intent is **absent from `pond_index_intents()`** - so it is absent from fold, rebuild, cleanup, status, and copy-verify in one place. Lance 8 keeps an index the intents list does not name (compaction recalculates its fragment bitmap at commit; verified in `lance` 8.0.0 `optimize.rs` / `transaction.rs`). Existing vectors and the IVF index stay on disk untouched. |
| 6 | When disabled, `prewarm` does not page the IVF index in. |
| 7 | When disabled, **no user-facing wording about embeddings**: `pond status`, the sync summary, `stats://pond` omit the semantic half. JSON surfaces keep their keys with `null` values. No "stale / coverage" checks anywhere, on any path - every one is a data-page scan over `messages`. |
| 8 | When enabled, behaviour is today's: inline embed at ingest, backlog via `pond optimize --only embed`, model-swap guard (`--force-embed`), IVF activation at 100k, degrade-to-fts until the first row is embedded - with ONE fix: the embed stage decides "is there a backlog" from `embed_backlog_count()` alone, never from the manifest fast-path `unindexed_vector_backlog()` (section 3.2 explains why that path now lies). |
| 9 | Mixed fleet (one machine enabled, one disabled, same remote store) is supported and documented: the disabled machine's rows stay un-embedded until the enabled machine runs `pond optimize --only embed`. `sync` does not grow a backlog probe. |
| 10 | `POND_EMBEDDINGS_ENABLED=true|false` works as an env mirror (containers and CI have no config file). Value must be the literal `true`/`false` - figment stringifies `1`, which fails the bool field. |
| 11 | Scheduled-sync units pin the config path the same way they already pin `XDG_STATE_HOME`, so a scheduled sync sees the same `enabled` as a manual one. |
| 12 | No compatibility shim, no auto-enable for stores that already hold vectors (repo rule: pre-release). A config containing `enabled` fails to load on an older binary (`deny_unknown_fields`) - accepted; the CHANGELOG says so. |
| 13 | Ships as ONE `feat!:` commit with a real diff (squash subject keeps the `!`) so release-plz cuts `0.15.0`. The three plugins' text changes ride in the same PR. |
| 14 | The `--min-score` flag and `min_score` param keep rejecting fts mode; after the flip a caller passing `min_score` without an explicit `mode` gets that error. Documented in the CHANGELOG upgrade note; help text corrected. |

Spec 8.7 already says "Embedding is opt-in by configuration"; the code never had the switch. This change closes a spec-vs-code gap, then tightens 8.1/8.7 wording.

## 2. Why this is small (verified)

- `Store.embedder: Option<Arc<LazyEmbedder>>` (`sessions.rs:58`); `embed_message_rows` returns all-`None` without one (`sessions.rs:1012-1020`). Ingest already writes null vectors when no embedder is attached.
- `vector` / `embedding_model` are nullable columns created at table creation (spec 8.8); a zero-vector store builds FTS and serves.
- The IVF intent triggers on `OnNonNullCount { threshold: 100_000 }` (`pond_index_intents_with_vector_threshold`, `sessions.rs`).
- The embed stage is a post-ingest seam: `run_embed_stage` is called from `finalize_indexes`, which `optimize`, `copy`, and archive restore share; `sync` calls only the model-swap guard.
- `has_embeddings()` is already the only read-path gate; it becomes unreachable when disabled.

No Lance API changes. No interaction with the Lance 10 bump (#145).

## 3. Upgrade hazards found in review (each has a test in section 8)

### 3.1 The IVF fold has no all-null guard
`optimize_table_indices` (`substrate.rs` ~3275-3320) folds every intent whose index exists. The zero-non-null guard there is `matches!(IndexParamsKind::InvertedFtsWord)`. Lance 8 folds an all-null IVF tail without corruption (the null filter applies to the data, the fragment bitmap is seeded from the fragment list), but an empty delta segment then wins every rebalance and each later no-op fold writes and discards a full rebuild into a fresh `_indices/<uuid>/` dir. Decision 5 removes the exposure on disabled instances. Enabled instances still fold null fragments written by a disabled peer - see 3.2.

### 3.2 The manifest backlog fast-path lies after a null fold
`run_embed_stage` (`main.rs` ~4817) short-circuits when `unindexed_vector_backlog() == 0`. In a mixed fleet the enabled machine folds the disabled machine's all-null fragments into IVF; those fragments are now "indexed", the manifest count reads 0, and `pond optimize --only embed` returns `EmbedSummary::default()` with nothing embedded - silently. This breaks the invariant documented at `sessions.rs` ~3084 ("a row folds only after it embeds"). Decision 8 fixes it: gate on `embed_backlog_count()` (narrow `embedding_model IS NULL AND search_text IS NOT NULL` count, ~7 s on S3 - see CLAUDE.md "count_rows predicates") and delete the fast-path. This cost lands only in `optimize`/`copy`, never in the 5-minute sync.

### 3.3 `copy` and archive restore would embed the whole corpus
Both call `finalize_indexes` -> `run_embed_stage`. On a store synced with embedding off every row is backlog, so the model downloads and the whole corpus embeds. And the backlog probe itself is a column scan. The disabled early-return must be the **first statement** of `run_embed_stage`.

### 3.4 `prewarm` pages the IVF index in on every start
`Store::prewarm` (`sessions.rs` ~1895) calls `prewarm_index(MESSAGES_VECTOR_INDEX)` unconditionally from `spawn_prewarm` (`main.rs` ~1119), called by `serve` and `mcp`. One-line gate.

### 3.5 hermes-pond respawns `pond serve` on every refusal
`packages/hermes-pond/service.py` ~142-145: any `McpError` calls `self._drop(...)`, which kills the child; the next call re-dials cold. A vector refusal delivered as a JSON-RPC error = one cold `pond serve` start per search. Decision 3 (refusal as `CallToolResult::error`, which is NOT an `McpError`) avoids it; hermes additionally gets a guard (5.8).

### 3.6 Scheduled syncs would diverge from manual ones
`schedule.rs` unit templates (launchd `plist_body` ~432-467, `systemd_service_body` ~609-621, `cron_entry` ~741-750) pin only `XDG_STATE_HOME`. A user enabling via env or a shell-set `XDG_CONFIG_HOME` gets embedding manual syncs and non-embedding scheduled ones. Decision 11.

### 3.7 Plugins describe the old contract
`hermes-pond/tools.py:70`, `pi-pond/src/tools.ts:75`, `openclaw-pond/src/tools.ts:146` (NOT `schemas.ts:23`, which is only the enum and stays valid).

### 3.8 `--min-score` without `mode` hard-errors after the flip
`handlers.rs` ~1205 rejects `min_score > 0` in fts mode. Decision 14.

### 3.9 Accepted / left alone
- Rollback: no store or schema change; only a config carrying `enabled` breaks an older binary. `pond init` writes the `[embeddings]` block commented out (`DEFAULT_CONFIG_TOML`), keep it that way.
- `pond optimize --rebuild` on a disabled instance no longer rebuilds the IVF index (intent absent). Document: enable first, or clean up with `pond optimize --drop-index messages_vector_ivfpq`.
- `.pond` archive import still checks `embedding_dim` (`import_pond_archive`, `main.rs` ~5073). Leave.
- On an enabled machine sharing a store with a disabled one, `pond status -v` shows a large embed backlog. True statement, leave.

---

## 4. Implementation order

Do the steps in order; each ends with a command that must pass before the next.

| Step | Scope | Gate command |
|---|---|---|
| 1 | Config field + runtime flag (5.1) | `cargo build && cargo test --lib config::` |
| 2 | Write path: no embedder when disabled (5.3); intents + prewarm (5.4); embed stage (5.5) | `cargo test --lib sessions::tests::` - the inline-embed unit test (`sessions.rs` ~6111-6148, "inline embed rides the birth append") MUST stay green; it does only if the gate lives at the `main.rs` attach sites, not inside `Store` |
| 3 | Default arm + refusal (5.2) | `cargo test --test integration -- transport_mcp:: transport_http:: search::` then `cargo insta accept` after reading the 6 snapshot diffs |
| 4 | Output gating (5.6) | `cargo test` |
| 5 | Descriptions, skill, plugins, docs, spec (5.7, 5.8, 5.9) | plugin tests: `pnpm -C packages/openclaw-pond test`, `pnpm -C packages/pi-pond test`, `cd packages/hermes-pond && python -m pytest` |
| 6 | Scheduler units (5.10), harness + benches (5.11) | `cargo clippy --all-targets -- -D warnings` (CI runs tests WITHOUT `--all-targets`; benches only compile under `lint-msvc`, skipped on fork PRs - this local run is the only bench gate) |
| 7 | Acceptance matrix (section 8) against a copy of a real store | all rows pass |
| 8 | CHANGELOG enrichment happens on the release PR, not here (section 9) | - |

---

## 5. Change list with code

### 5.1 Config field and runtime flag

**`packages/pond/src/config.rs`, `struct EmbeddingsConfig`** - current:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EmbeddingsConfig {
    pub model: String,
    pub dim: usize,
}
impl Default for EmbeddingsConfig {
    fn default() -> Self {
        Self { model: crate::embed::DEFAULT_MODEL_ID.to_owned(), dim: crate::sessions::DEFAULT_EMBEDDING_DIM }
    }
}
```

Target: add `pub enabled: bool` as the first field, `enabled: false` in `Default`. Keep `deny_unknown_fields`. Rewrite the doc comment above the struct (it currently says "There is no master switch - a `vector` search degrades to FTS when no vectors exist"):

```rust
/// `[embeddings]`: the opt-in switch, model selector, and vector dimension.
/// `enabled = false` (default) means no process loads a model, ingest writes
/// null vectors, and `mode=vector` is refused. `model` and `dim` are installed
/// into the process at startup via `install_runtime`.
```

**Three test struct literals** in `config.rs` (`validate_catches_empty_model_and_bad_dim`, ~1121/1128/1134) build `EmbeddingsConfig { model, dim }` - add `..Default::default()` to each or they fail to compile.

**`EmbeddingsConfig::install_runtime`** - current:

```rust
pub fn install_runtime(&self) {
    crate::embed::init_model_id(self.model.clone());
    crate::sessions::init_embedding_dim(self.dim);
}
```

Target: add `crate::embed::init_enabled(self.enabled);` as the first line.

**`packages/pond/src/embed.rs`**, next to `MODEL_ID_RUNTIME` / `model_id()` / `init_model_id()` - add the same pattern:

```rust
/// Process-wide opt-in switch, seeded once at startup from
/// `[embeddings].enabled` via [`init_enabled`]. Uninitialized -> `false`, so a
/// process that never loaded config (tests, ad-hoc tooling) behaves like a
/// fresh install. Tests that need the vector arm call `init_enabled(true)`.
static ENABLED_RUNTIME: OnceLock<bool> = OnceLock::new();

pub fn embeddings_enabled() -> bool {
    ENABLED_RUNTIME.get().copied().unwrap_or(false)
}

pub fn init_enabled(enabled: bool) {
    ENABLED_RUNTIME.get_or_init(|| enabled);
}
```

Why a `OnceLock` and not a parameter: the search path receives only `SearchConfig` (`handlers.rs` ~1130, holds `nprobes`), and `AppState { store, embedder, search }` is built literally in three integration tests (`transport_http.rs` ~75, `transport_mcp.rs` ~154, `sql.rs` ~174). Adding a field or plumbing a parameter churns every one of them for nothing. Do not plumb.

Test fallout: `OnceLock` is first-call-wins per process, and the integration suite is one binary. So (a) every integration fixture that exercises `mode=vector` calls `pond::embed::init_enabled(true)` at the top of its shared setup (`searchable_corpus` in `tests/integration/search.rs`, and the store setup in `transport_mcp.rs`, `transport_http.rs`, `sql.rs`); (b) the refusal itself is tested at the unit level with an explicit argument, never via the global - `resolve_effective_mode` takes `embeddings_enabled: bool` and the public caller passes `crate::embed::embeddings_enabled()` (5.2). The same parameter pattern applies to `pond_index_intents_with_vector_threshold` (5.4).

**`DEFAULT_CONFIG_TOML`** (`config.rs` ~236-253) - the commented `[embeddings]` block currently documents `model`/`dim` and says "Search defaults to the vector arm". Target text (keep it commented out so an older binary still loads a fresh config):

```toml
# [embeddings]
# Semantic (vector) search is opt-in. Off: no model is downloaded or loaded,
# new messages get no vectors, and pond_search mode="vector" is refused.
# On: messages embed at ingest; run `pond optimize --only embed` to fill the
# backlog. Measured: off keeps a pond process ~100 MiB; on costs ~500-900 MiB
# once any vector work ran, a 466 MiB one-time download, and CPU-bound first
# syncs on hosts without Metal/CUDA.
# enabled = false
# model = "intfloat/multilingual-e5-small"
# dim = 384
```

**`env_mirror()`** (`config.rs` ~754-763) - current filter:

```rust
key == "storage_path" || (key.starts_with("creds_") && !key.ends_with("_extra"))
```

Target:

```rust
key == "storage_path"
    || key == "embeddings_enabled"
    || (key.starts_with("creds_") && !key.ends_with("_extra"))
```

The non-creds mapping branch replaces exactly one underscore, so `embeddings_enabled` -> `embeddings.enabled` already; verify by reading the `.map(...)` closure below the filter. Update the comment above (it says "Filtered to exactly those two shapes"). Add a unit test beside the existing env-mirror tests: `POND_EMBEDDINGS_ENABLED=true` loads with `enabled == true`; `=1` fails to load (document that in the test name; do not "fix" it with a custom deserializer).

### 5.2 Default arm and the refusal

**`packages/pond/src/wire.rs`, `enum SearchModeWire`** - move `#[default]` from `Vector` to `Fts`. Rewrite the three doc comments on `SearchRequest.mode`, the enum, and `SearchResponse`'s mode field (~596-599, ~615-616, ~652-654):

```rust
/// Retrieval arm (spec.md#search). `fts` (default) matches exact whole words
/// via BM25; `vector` matches on meaning and is available only when the serving
/// instance has `[embeddings].enabled = true` - otherwise it is refused. The
/// agent picks per query; there is no server-side fusion.
```

**`packages/pond/src/transport.rs`, `fn parse_search_mode`** - current:

```rust
match value {
    None | Some("vector") => Some(SearchModeWire::Vector),
    Some("fts") => Some(SearchModeWire::Fts),
    Some(_) => None,
}
```

Target:

```rust
match value {
    None | Some("fts") => Some(SearchModeWire::Fts),
    Some("vector") => Some(SearchModeWire::Vector),
    Some(_) => None,
}
```

Fix its doc comment ("unknown / absent defaults to vector" -> "absent defaults to fts").

**`packages/pond/src/handlers.rs`, `fn resolve_effective_mode`** - current:

```rust
async fn resolve_effective_mode(store: &Store, requested: SearchMode) -> Result<SearchMode, ErrorEnvelope> {
    if matches!(requested, SearchMode::Fts) { return Ok(SearchMode::Fts); }
    let has = store.has_embeddings().await.map_err(map_storage)?;
    Ok(if has { SearchMode::Vector } else { SearchMode::Fts })
}
```

Target:

```rust
/// Literal text of the `mode=vector` refusal. One constant: the MCP tool, the
/// REST route, the CLI, and `optimize --only embed` all print this exact line.
pub const SEMANTIC_DISABLED_MESSAGE: &str = "semantic search is off on this pond instance: \
    set [embeddings].enabled = true in pond's config (or POND_EMBEDDINGS_ENABLED=true), \
    run `pond optimize --only embed`, then retry with mode=\"vector\" - or use mode=\"fts\"";

/// Pick the effective arm. `fts` stays `fts`. `vector` is refused when
/// embedding is disabled on this instance - checked before any store probe,
/// because `has_embeddings()` on a never-embedded store is a full column read.
/// With embedding enabled, `vector` degrades to `fts` until a row is embedded.
async fn resolve_effective_mode(
    store: &Store,
    requested: SearchMode,
    embeddings_enabled: bool,
) -> Result<SearchMode, ErrorEnvelope> {
    if matches!(requested, SearchMode::Fts) {
        return Ok(SearchMode::Fts);
    }
    if !embeddings_enabled {
        return Err(crate::wire::error(
            crate::wire::ErrorCode::ValidationFailed,
            SEMANTIC_DISABLED_MESSAGE,
            serde_json::json!({ "field": "mode", "config_key": "embeddings.enabled", "retryable": false }),
        ));
    }
    let has = store.has_embeddings().await.map_err(map_storage)?;
    Ok(if has { SearchMode::Vector } else { SearchMode::Fts })
}
```

Caller in `pond_search` (~1197): `plan.mode = resolve_effective_mode(store, plan.mode, crate::embed::embeddings_enabled()).await?;` and rewrite the comment above it. Check how other `ValidationFailed` envelopes in this file are built (`map_error(crate::Error::validation_field(...))` at the `min_score` check ~1205) and use the same constructor if it yields the same `details` shape - consistency over the snippet above. `ValidationFailed` already maps to `pond_code:"validation_failed"`, `retryable:false`, JSON-RPC `-32010` (`transport.rs` ~1296) - no new error code; spec 7.4's code set is closed.

**MCP surface**, `transport.rs` `pond_search` tool fn (~755-806): today `SearchEnvelope::Error(envelope) => Err(to_error_data(&envelope))`. Add a branch BEFORE the generic one so the refusal is an in-turn tool error, exactly like the sibling unknown-mode case a few lines above:

```rust
SearchEnvelope::Error(envelope)
    if envelope.error.details.get("config_key").and_then(|v| v.as_str()) == Some("embeddings.enabled") =>
{
    Ok(CallToolResult::error(vec![Content::text(envelope.error.message.clone())]))
}
SearchEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
```

Why: a JSON-RPC error makes hermes-pond tear down and respawn the `pond serve` child on every call (3.5), and Claude Code renders a tool error in-turn so the model simply retries with `mode="fts"`. REST `POST /v1/search` keeps mapping `ValidationFailed` to HTTP 400 (`transport.rs` ~117-121) - correct for a client-fixable request.

**CLI** `pond search`: `main.rs` ~512 and ~533-536 `--mode` help: `fts` default, `vector` needs `[embeddings].enabled`. `--min-score` help (~558-562) add: "Requires mode=vector, which is no longer the default - pass --mode vector explicitly." The refusal already surfaces through the existing error printing; verify `pond search --mode vector x` prints `SEMANTIC_DISABLED_MESSAGE` and exits non-zero.

**Test helper** `handlers.rs` ~2067 hardcodes `mode: SearchModeWire::Vector` in `search_request()`; change to `SearchModeWire::default()` and fix the assertion at ~2287 (`assert_eq!(plan.mode, SearchMode::Vector)` becomes `Fts`). Add a unit test `vector_refused_when_embeddings_disabled` calling `resolve_effective_mode(&store, SearchMode::Vector, false)` and asserting `error.code == ValidationFailed` and `details.config_key == "embeddings.enabled"`.

### 5.3 Write path: no embedder when disabled

Three attach sites in `packages/pond/src/main.rs`, all the same shape. Current (`serve`, ~1499-1507; `mcp`, ~1547-1553):

```rust
let embedder = Arc::new(LazyEmbedder::candle());
embedder.spawn_idle_reaper();
let store = Arc::new(open_store(...).await?.1.with_embedder(embedder.clone()));
let state = AppState { store, embedder, search: config.search.clone() };
```

`AppState.embedder` is `Arc<LazyEmbedder>` (not `Option`) and `LazyEmbedder::candle()` is lazy - constructing it loads nothing. So the minimal, non-churning change is: keep constructing it, but only ATTACH it to the store (and only spawn the reaper) when enabled:

```rust
let embedder = Arc::new(LazyEmbedder::candle());
let store = open_store(...).await?.1;
let store = Arc::new(if pond::embed::embeddings_enabled() {
    embedder.spawn_idle_reaper();
    store.with_embedder(embedder.clone())
} else {
    store
});
```

With the refusal in 5.2 nothing on the read path can call `embedder.get()` when disabled; verify by `rg -n "embedder.get\(\)|load_embedder" packages/pond/src` and confirming each call site is behind a vector-mode branch.

`sync` (~4036-4071) - current attaches the embedder, the ingest-embed HUD, and then eagerly preloads the model (`embedder.get().await` with the "embedding model ready" stage line). Target: wrap all three in `if pond::embed::embeddings_enabled() { ... }`. Do NOT delete the preload: enabled users still want the 466 MiB download to finish before the progress bars own the terminal (the comment there explains it).

`serve --with-sync` shares the serve embedder (~1515-1525) - covered by the serve site.

`benches/commands_bench.rs` ~346-359 reimplements the embed phase with `CandleEmbedder::load()` directly; wrap in `if config.embeddings.enabled` (`config: &Config` is threaded at ~273).

### 5.4 Index intents and prewarm

**`packages/pond/src/sessions.rs`, `fn pond_index_intents_with_vector_threshold`** - currently always pushes the IVF intent (`name: MESSAGES_VECTOR_INDEX, column: "vector", trigger: OnNonNullCount{...}, params: IvfSqCosine{...}`). Target: wrap that single `messages.push(IndexIntent { name: MESSAGES_VECTOR_INDEX, ... })` in `if crate::embed::embeddings_enabled() { ... }`. That one edit covers all six callers of `pond_index_intents()` (`optimize_indices`, `build_indices_only`, the two test variants, `cleanup_old_versions`, `rebuild_indices`, and the open-time registration at ~3308). Do not edit the callers. Tests in `sessions.rs::tests` that exercise IVF activation (`optimize_indices_with_vector_threshold`, `unindexed_vector_backlog_*`) must call `crate::embed::init_enabled(true)` first - or, cleaner, give `pond_index_intents_with_vector_threshold` an explicit `include_vector: bool` parameter, have `pond_index_intents()` pass `embeddings_enabled()`, and have tests pass `true`. Prefer the parameter: it keeps the unit tests order-independent.

**`Store::prewarm`** (~1895) - current:

```rust
if let Err(error) = messages.prewarm_index(MESSAGES_VECTOR_INDEX).await {
    tracing::debug!(%error, "vector index prewarm skipped");
}
```

Target: `if crate::embed::embeddings_enabled() { ...same block... }`.

### 5.5 Embed stage

**`main.rs`, `fn run_embed_stage_with_limit`** (the body behind `run_embed_stage`, ~4786-4900). Target, in order:

1. First statement: `if !pond::embed::embeddings_enabled() { return Ok(EmbedSummary::default()); }` - before the model-swap check, before any store read. This covers `optimize`, `copy`, and archive restore via `finalize_indexes` (3.3).
2. Keep the model-swap guard (`store.embedding_model_swapped()`, the `--force-embed` bail and `drop_vector_index()`). It is the only thing preventing two embedding spaces in one IVF index.
3. Replace the two-step backlog gate. Current:

```rust
let backlog = if !swapped && store.unindexed_vector_backlog().await? == 0 {
    0
} else {
    store.embed_backlog_count().await?
};
```

Target:

```rust
// The manifest-only `unindexed_vector_backlog` cannot be trusted as a zero
// proof any more: an enabled peer folds a disabled peer's all-null fragments
// into IVF, which marks them indexed while every row is still unembedded.
// `embed_backlog_count` is the narrow co-set count (embedding_model IS NULL
// AND search_text IS NOT NULL) - ~7 s on S3, paid only here, never in sync.
let backlog = store.embed_backlog_count().await?;
```

Delete the long comment above the old gate (it describes the fast path). Check whether `Store::unindexed_vector_backlog` still has callers (`rg -n unindexed_vector_backlog packages/pond/src`); if `status -v`'s `embedding_progress` is its only remaining user, keep it; if none, delete it and its tests (minimalism rule).

4. `pond optimize --only embed` and `--force-embed` when disabled: in the `Command::Optimize` arm (~1434-1450) and `OptimizeStages::resolve` (~3690-3714), `bail!("{}", SEMANTIC_DISABLED_MESSAGE)` when `stages.embed && !stages.index` (i.e. the user explicitly asked for embed) or `force_embed` is set and embedding is disabled. A full `pond optimize` with embedding off silently skips the embed stage (step 1) and prints `fold` only - that is correct, do not error there.

5. `sync`'s invariant comment at ~4158 ("sync can never leave a searchable row un-embedded") - rewrite to: "On an instance with embedding enabled, sync embeds inline, so every row IT ingested carries its vector; rows ingested by a disabled peer are healed by `pond optimize --only embed` on an enabled instance. The finalize embed worker therefore does not run in sync."

### 5.6 Output: nothing about semantic when disabled

**`main.rs`, `struct IndexHealth { text, semantic }` and `fn render_indexes_line`** (~6345-6430). Make `semantic: Option<IndexHealthState>`; `classify_index_health` sets it `None` when `!embeddings_enabled()`. Render:

```rust
let body = match (&health.text, health.semantic.as_ref()) {
    (Ready, None) => "text ready".to_owned(),
    (Pending(n), None) => format!("text {} pending", format_thousands(*n)),
    (NotBuilt, None) => "text not built".to_owned(),
    (Ready, Some(Ready)) => "text + semantic ready".to_owned(),
    (text, Some(semantic)) => { /* today's two-part branch */ }
};
```

The `-v` backlog override (~6390-6395) that flips `semantic` to `Pending(backlog)` runs only when `Some`.

- `status --verbose` (~1280-1287): skip `store.embedding_progress()` when disabled (it is a data scan) - `Ok(None)`.
- `status` text: the stderr hint "(use -v for searchable message count + embedding backlog)" (~6062) - drop "+ embedding backlog" when disabled.
- `status --format json` (~5882 `.embedding`): emit `null` when disabled; keep the key.
- Sync summary (~4949-4979) uses `render_indexes_line` - covered.
- `copy`/restore recap strings "text + semantic rebuilt on destination" (~2644, ~2755): `"text rebuilt on destination"` when disabled.
- `struct IndexCoverage { fts_present, vector_present_or_below_activation }` (~3530) and its builder (~3585): when disabled set `vector_present_or_below_activation: true` unconditionally, otherwise `copy --verify` prints a permanent "indexes pending - run pond optimize --only index" on a store that has >= 100k embedded rows and no IVF intent. Extend the `classify_index_health` tests (~7177, ~7185) with a disabled case.
- `stats://pond` (`transport.rs` ~1129-1170): skip the `store.embedding_progress()` CALL (not just its rendering) when disabled; emit the block's keys as `null`.
- `init.rs` ~344 prompt text "and embeds": drop the two words.
- Sync HUD / first-sync notice (~4589-4608, ~4711-4715): reachable only when the embedder is attached after 5.3; read them once to confirm, leave.

### 5.7 Server instructions, tool descriptions, skill

`transport.rs`:
- Server `instructions` block (~1032-1064, the `get_info` text): it says "keep queries semantic (concepts, not project names)". With BM25 the default that advice degrades results. Replace with: "phrase queries with the distinctive words you expect in the conversation (error strings, symbol names, product names); use mode=\"vector\" for paraphrase-style recall when this instance has embeddings enabled". Keep it as short as the current sentence; instructions are the always-loaded routing surface (CLAUDE.md "MCP tool routing is deliberate").
- `query` param doc (~569-573) carries the same "semantic - concepts, not project names" phrase: same fix.
- `mode` param doc (~576-579) and the tool description (~743-746): `"fts" (default) exact words, BM25; "vector" meaning - needs [embeddings].enabled on the serving instance`.
- Other mentions (~234-235, ~252, ~257-258, ~316-319): read each, apply the same wording. Do not lengthen descriptions.

`packages/pond/SKILL.md` line ~16: current `(`mode=vector` default; `mode=fts` for exact whole words)` -> `(`mode=fts` default: exact words; `mode=vector` for meaning, only where embeddings are enabled)`. This file is `include_str!`-embedded and installed by `pond init`; keep it in the crate (not in Cargo `exclude`).

### 5.8 Plugins (same PR)

- `packages/openclaw-pond/src/tools.ts` ~146: `"vector" (default, meaning) or "fts" (exact words, BM25)` -> `"fts" (default, exact words, BM25) or "vector" (meaning; only when pond has embeddings enabled)`.
- `packages/pi-pond/src/tools.ts` ~75: same.
- `packages/hermes-pond/tools.py` ~70: same.
- `packages/hermes-pond/service.py` ~142-145: today every `McpError` calls `self._drop(...)`. The refusal no longer arrives as `McpError` (5.2), but add the guard anyway so any future non-retryable app error does not respawn the child: if `exc.error.code` is in the pond app range (`-32010..-32016`) or `exc.error.data.get("retryable") is False`, return `(False, f"pond: {exc.error.message}")` WITHOUT `_drop`. Keep `_drop` for transport faults.
- Each plugin: one test asserting the `mode` description string contains `"fts" (default`. Each plugin's README: flip the default wording.
- All three resolve `pond` from `PATH` with no version gate, and nothing in CI publishes them. Minimum: log the resolved `pond --version` once at dial time in each plugin (openclaw `resolvePondBinary`, pi `service.ts` ~136, hermes equivalent) so a binary/plugin skew is visible in logs.

### 5.9 Docs and spec

- `README.md` lines ~46, 126, 162, 221, 222; `docs/site/src/pages/reference/mcp-tools.mdx` ~18; `reference/cli.mdx` ~20-30; `reference/configuration.mdx` (add the `[embeddings]` section - it is undocumented today - with `enabled`, the env mirror, the mixed-fleet rule from decision 9, and the `--drop-index messages_vector_ivfpq` cleanup); `get-started/connect-your-agents.mdx`; `ops/examples/pi-fleet/README.md` lines ~10, 46, 58, 59 ("there is no switch to turn that off" / "the vector arm (the default)").
- `ops/examples/pi-fleet/docker-compose.yml`: note in README that enabling in a container needs `POND_EMBEDDINGS_ENABLED=true` plus a volume for `/pi/.cache/huggingface`, or the 466 MiB model re-downloads on every pod replace.
- `.github/workflows/ci.yml` ~511 (nix `meta.description`) and ~553 (brew `desc`) say "hybrid search" - already wrong; change to "full-text search, optional semantic search".
- `server.json` is at 0.14.5 vs Cargo 0.14.11 and nothing syncs it; bump it to match in this PR and leave a one-line comment in the PR description (not in code) that it is unsynced.

`docs/spec.md`:
- 7.4 error table row `validation_failed` "When" column: append "; a request for a capability this instance has disabled (`mode=vector` with embedding off)".
- 7.8 verbs `init`, `sync`, `optimize`, `status`, `serve` (~695-703): strike unconditional embed/model wording; `sync` "embeds inline when embedding is enabled"; `status` "reports the semantic index only when embedding is enabled"; `serve` loses "degraded".
- 8.1 (~727): "`vector`, the default" -> "`fts`, the default"; replace "The vector arm falls back to full-text when no message is embedded under the configured model" with "The vector arm is available only when embedding is enabled on the serving instance; a `vector` request on an instance with embedding off is refused with `validation_failed` naming the setting. With embedding on and no message yet embedded under the configured model, the vector arm degrades to full-text."
- 8.7 (~763): replace the whole paragraph: "Embedding is opt-in by configuration (`[embeddings].enabled`, default off). With it off, no pond process loads a model, ingest writes no vectors, index maintenance ignores the vector index, and a `vector` request is refused. With it on, ingest embeds inline, `pond optimize --only embed` fills any backlog, and the `vector` arm is available on request; `fts` is the default arm either way. Instances sharing one store may differ: rows ingested by an instance with embedding off are embedded when an enabled instance next runs `pond optimize --only embed`."
- 8.8 (~767): add "The vector index is maintained only by instances with embedding enabled; an instance with it off leaves an existing vector index untouched."
- 3.7 (~212, ~219): "FTS + vector fold at the sync tail" -> "FTS (and, when embedding is enabled, vector) fold".
- Contents line ~19 and 1.1 line ~31: only if they name the vector default.

### 5.10 Scheduler units

`packages/pond/src/schedule.rs`: the three templates (`plist_body`, `systemd_service_body`, `cron_entry`) set `XDG_STATE_HOME` from the registration-time value with a char-rejection guard (~353-364). Add the config path the same way: resolve the active config file at registration (the same path `config_path(config)` yields in `main.rs`) and emit `POND_CONFIG_FILE=<path>` in each template; extend the char guard to it. Unit args stay `sync -q --no-wait`. Add the path to the existing template unit tests. This way a scheduled sync honours the same `enabled` the user set for manual syncs (3.6).

### 5.11 Tests, benches, harness

Rust:
- `tests/integration/transport_mcp.rs` ~213-221: the request `json!({"query":"answer"})` expects "pond_search: 1 nearest message ...". This is the one hard break. Keep it as the vector test by adding `"mode":"vector"` (the fixture enables embeddings), and add a sibling test without `mode` expecting "1 matching message". The disabled-path refusal is covered by the `handlers.rs` unit test with the explicit `false` argument (5.2) plus a unit test on the MCP match-arm predicate (`details.config_key == "embeddings.enabled"`); do not add a test binary for it (CLAUDE.md forbids loose test binaries).
- `tests/integration/transport_http.rs` ~116: add `"mode":"vector"` so the vector arm stays covered; add a no-mode sibling asserting BM25 wording.
- `src/snapshots/pond__tests__help_{search,sync,optimize,status,root,serve}.snap`: regenerate with `cargo insta accept` after reading every diff (`cargo insta review` to inspect).
- A CLI regression test for decision 14: `pond search --min-score 0.2 foo` (no `--mode`) exits non-zero with the existing min_score message.

Benches (compile-check with `cargo clippy --all-targets`):
- `benches/serve_mem_bench.rs` ~1145-1183 `vector_first` / `vector_steady` send explicit `SearchModeWire::Vector`; with the refusal they `bail!`. Gate: when `!embeddings_enabled()` skip both phases, print `vector phases skipped: embeddings disabled`, and mark the "candle E5 model (serving)" delta as `n/a` so the `idle_target_mib` gate (~1319-1339) cannot pass against an FTS query labelled vector. Add a `--no-embedder` flag that measures a never-loaded idle floor (see section 8, case A, for why).
- `benches/backend_bench.rs` ~165 `mode.unwrap_or_default()`: make every caller pass an explicit mode.
- `benches/ops_bench.rs` ~108, 128-129: staleness probes - gate on enabled.
- `ops/scripts/bench-gate.sh`: ~48 add `--mode vector` to the `search_s` probe; ~69-74 give `iops()` and `ms()` a `:-null` default so a missing row cannot emit `"vector_iops":,`; seed a bench-gate-owned config with `enabled = true` (today it scrapes the operator's `~/.config/pond/config.toml`); stamp `"search_mode"` into the JSONL row so the 2026-08-12 baseline row is not compared to an fts timing.
- `ops/e2e/run.py`: `seed_sandbox()` (~166-187) gets an `embeddings_enabled: bool` parameter that emits the `[embeddings]` section; ~261-262 add `--mode vector` and an fts-default case; ~355 add an `--only embed` case under an enabled sandbox; ~245 the `status -v` anchor stays valid once 5.6 is done.
- `docs/researches/2608-21-semantic-vs-fts-usage-eval/scripts/replay.py` ~17-19 swallows subprocess failures and returns an empty hit list with a plausible latency; make it raise on non-zero return code, otherwise acceptance B passes while measuring nothing.

---

## 6. Literal strings (copy exactly)

| Where | Text |
|---|---|
| Refusal (`SEMANTIC_DISABLED_MESSAGE`) | `semantic search is off on this pond instance: set [embeddings].enabled = true in pond's config (or POND_EMBEDDINGS_ENABLED=true), run `pond optimize --only embed`, then retry with mode="vector" - or use mode="fts"` |
| Indexes line, disabled | `indexes   text ready` / `indexes   text 1,234 pending` / `indexes   text not built` |
| Copy recap, disabled | `text rebuilt on destination` |
| Commit subject | `feat!(search): fts is the default arm; embeddings opt-in via [embeddings].enabled` |

## 7. Do not

- Do not plumb an `enabled` parameter through `AppState`, `SearchConfig`, `Store`, or handler signatures. Use the `OnceLock`.
- Do not put the attach gate inside `Store` - the inline-embed unit test depends on `with_embedder` working unconditionally.
- Do not delete the model-swap guard or `--force-embed`.
- Do not delete sync's model preload; gate it.
- Do not add any backlog/staleness probe to `sync`, `status` (non-verbose), `mcp`/`serve` startup, or the search path.
- Do not auto-enable when vectors exist, write config files, or add a compatibility shim for older binaries.
- Do not extend the FTS all-null fold guard to IVF as a substitute for removing the intent.
- Do not fatten MCP tool descriptions; routing lives in the server instructions.
- Do not rename `MESSAGES_VECTOR_INDEX` ("messages_vector_ivfpq" is a stable on-disk identifier).
- Do not open a release PR or pick a version; release-plz does both.

---

## 8. Acceptance

All against `cargo build --release` (`target/release/pond`). C, D, E need a copy of a real store that already has an IVF index: `pond copy --from <s3 store> --to file:///tmp/pond-upgrade` with embedding enabled on the copying machine.

| Case | Steps | Expect |
|---|---|---|
| A fresh, disabled | sandboxed `HOME`/XDG dirs (CLAUDE.md "Testing interactive CLI flows"), `pond init`, `pond sync` on a CPU-only box, no config edit | no HF download (assert `$HOME/.cache/huggingface` absent), no `embed` stage line, `indexes   text ready`; `pond search foo` returns BM25 hits; `pond search --mode vector foo` prints `SEMANTIC_DISABLED_MESSAGE`, exit != 0; `pond mcp` `pond_search` default -> "matching"; `mode=vector` -> tool `isError` with that text. Record `serve_mem_bench --no-embedder` idle phys_footprint; the number is informational (#61's floor is the rowmap transient plus ~200 MiB FTS cache, not embeddings) - the two robust assertions are "no download" and "no embed stage" |
| B fresh, enabled | `enabled = true` (file) and separately `POND_EMBEDDINGS_ENABLED=true` (env); `pond sync`; `pond optimize --only embed` | model loads once, vectors written, `mode=vector` works, `indexes   text + semantic ready`; the eval's `scripts/replay.py` runs and raises on failure |
| C upgraded store, disabled | store with IVF index + >= 100k vectors; new binary; `enabled` absent; `pond sync` x3 with new sessions; `pond optimize`; `pond status`; `pond copy --verify` | `RUST_LOG=pond::perf=debug` shows no `messages_vector_ivfpq` fold (the per-intent line is in `substrate.rs` `optimize_table_indices`); no error; index still listed by `pond optimize --drop-index` dry inspection or `pond_sql` over the manifest; `mode=vector` refused; FTS finds the new sessions; copy verify says ready |
| D re-enable | same store, `enabled = true`; `pond optimize --only embed`; `mode=vector` | embed backlog = exactly the rows ingested in C (not 0 - this is the 3.2 regression test); they fold into the existing IVF index without a rebuild; vector search returns new-session hits |
| E mixed fleet | machine 1 enabled, machine 2 disabled, same store; each syncs its own sources; machine 1 runs `optimize --only embed` | no new OCC conflicts; after machine 1's optimize, machine 2's rows are embedded and vector-searchable from machine 1 |
| F scheduled sync | `pond schedule install` with `enabled = true` in a non-default `XDG_CONFIG_HOME` | the written unit carries `POND_CONFIG_FILE=<that path>`; the scheduled run embeds (check `indexes text + semantic ready` in its log) |
| G bench gate | `ops/scripts/bench-gate.sh` before and after on the same store | rows for both; valid JSON; `search_mode` stamped |
| H min_score | `pond search --min-score 0.2 foo` | the existing min_score/fts error, exit != 0 |

---

## 9. Release

- One squash-merged PR; subject `feat!(search): fts is the default arm; embeddings opt-in via [embeddings].enabled` with a real diff -> release-plz opens `chore: release v0.15.0`.
- On the release PR branch, enrich `CHANGELOG.md` under `🛠 Breaking Changes` with a bolded `**Upgrading:**` preamble (precedent: `CHANGELOG.md:7`): "Embeddings are off by default. `mode=vector` is refused until `[embeddings].enabled = true` (or `POND_EMBEDDINGS_ENABLED=true`); existing vectors and the vector index are kept and resume on the next `pond optimize --only embed` from an enabled instance. Default search mode is `fts`; `--min-score` now needs an explicit `--mode vector`. A config file containing `enabled` does not load on pond < 0.15. Update openclaw-pond / pi-pond / hermes-pond alongside the binary." Then the grouped bullets with the measured numbers from the eval.
- After release: add "implemented in v0.15.0" to section 6 of `docs/researches/2608-21-semantic-vs-fts-usage-eval/README.md` and move roadmap row 7 to done in `README.md`.
