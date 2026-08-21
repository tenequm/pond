# Embeddings opt-in: fts is the default arm, the model loads only when asked (#164)

Self-contained implementation plan for [issue #164](https://github.com/tenequm/pond/issues/164). A fresh session implements from this document without re-deriving the investigation. Line numbers cite `main` at `d47ec3c` (2026-08-21); they drift, symbol names do not. Decisions in section 1 were settled with the operator on 2026-08-21 - implement them, do not re-litigate.

## 1. Decisions (settled)

| # | Decision |
|---|---|
| 1 | `[embeddings].enabled: bool`, default `false`. The gate is a local config read. No store probe, no env heuristics, no auto-detect of existing vectors. |
| 2 | Default `mode` for `pond_search` (MCP, HTTP, CLI) flips from `vector` to `fts`. |
| 3 | When disabled, `mode=vector` is **refused** with an error envelope that names the config key. No fallback to fts, no `has_embeddings()` probe, no model load - on reads too. |
| 4 | When disabled, `pond sync`, `serve --with-sync`, `optimize`, `copy`, `restore` attach no embedder and never load a model. Rows get null `vector` / `embedding_model`. |
| 5 | When disabled, the IVF intent is removed from index maintenance entirely (fold, compact-remap accounting, rebuild, status). Not just guarded - removed, so the fold issues no S3 round-trips for it. |
| 6 | No user-facing wording about embeddings when disabled: `pond status`, the sync summary, `stats://pond` omit the semantic half. No "stale vectors" checks anywhere - a coverage check is a data-page scan over `messages` (the 20-81 s remote waste `sync` already avoids, `main.rs:4158`). |
| 7 | When enabled, behaviour is exactly today's: inline embed at ingest, backlog via `optimize --only embed`, model-swap guard, IVF activation at 100k, degrade-to-fts when no row is embedded yet. |
| 8 | No migration, no compatibility shim, no auto-enable for stores that already hold vectors (repo rule: pre-release, breaking changes are free). The CHANGELOG entry carries the one-line upgrade note. |
| 9 | Ships as a `feat!:` commit (real diff) so release-plz cuts a minor bump. |

Spec 8.7 already says "Embedding is opt-in by configuration" - the code never had the switch. This change makes the code match the spec, then tightens 8.1/8.7 wording (section 6).

## 2. Why this is small

Verified in code: the store already treats vectors as optional.

- `Store.embedder` is `Option<Arc<LazyEmbedder>>` (`sessions.rs:58`); `embed_message_rows` returns all-`None` when no embedder is attached (`sessions.rs:1012-1020`). Ingest already writes null vectors in that case.
- `vector` / `embedding_model` are nullable columns created at init; a zero-vector store builds FTS and serves (spec 8.8, `sessions.rs:5027`).
- The IVF intent triggers on `OnNonNullCount { threshold: 100_000 }` (`sessions.rs:4808-4818`) - dormant by construction on a fresh store.
- The embed stage is a post-ingest seam: `run_embed_stage` (`main.rs:4786`) is called from `finalize_indexes` (`main.rs:4966`), which `sync`, `optimize`, `copy`, `restore` share.
- `has_embeddings()` (`sessions.rs:2707`) is already the only gate on the read path; it becomes unreachable on the disabled path.

No Lance API is touched. This does not interact with the Lance 10 bump (#145), which is why it ships first.

## 3. Upgrade hazards (found in review - each needs a test)

### 3.1 The IVF fold runs on every sync and has no all-null guard

`optimize_table_indices` (`substrate.rs:3275-3320`) folds every intent whose index exists. An upgraded store has the IVF index, so with embedding off each sync would fold fragments whose `vector` is all null into IVF_SQ. The zero-non-null guard at `substrate.rs:3304` is `matches!(InvertedFtsWord)` - FTS only. The comment above it explains the failure it prevents on Lance 7/8: an empty delta segment breaks every later merge of that index. IVF's behaviour on an all-null delta is unverified. Decision 5 removes the exposure: the IVF intent is not in the intents list when disabled. Test: section 7, case C.

### 3.2 Mixed fleet silently decays vector coverage

`main.rs:4158` documents the invariant "sync can never leave a searchable row un-embedded" and skips the backlog worker in `sync` because of it. After this change an enabled machine (Mac) and a disabled machine (home server) sharing one S3 store break the invariant: the disabled machine ingests null-vector rows; the enabled machine embeds only its own rows inline. Nothing errors. Resolution: the documented rule is "the enabled machine runs `pond optimize --only embed` on a schedule"; the `sync` path does not grow a backlog probe (the manifest count `unindexed_vector_backlog` is non-zero after any deferred fold, so the confirming scan would fire on most syncs - the #71/#78 cost). Update the `main.rs:4158` comment to state the new invariant: "sync never leaves a row it ingested un-embedded on an enabled instance". Document in `docs/site` configuration page and the CHANGELOG.

### 3.3 Plugins ship the old contract

`packages/hermes-pond/tools.py:70`, `packages/pi-pond/src/tools.ts:75`, `packages/openclaw-pond/src/schemas.ts:23` describe `vector` as the default. Agents following that hit the refusal on their first search. Plugin text must change in the same PR; plugin releases go out with the pond release.

### 3.4 Smaller

- `EmbeddingsConfig` is `deny_unknown_fields` (`config.rs:468`): a config carrying `enabled` fails to load on an older binary. Configs are per machine; only synced dotfiles are exposed. Accepted.
- Rollback is safe: no store or schema change.
- `pond copy --verify` (`main.rs:3585`) checks index presence, not coverage - unaffected.

## 4. Change list

Grouped by file. "-" lines are deletions.

### 4.1 Config and runtime flag

- `config.rs:469-485` `EmbeddingsConfig`: add `pub enabled: bool`, `Default` false. Keep `deny_unknown_fields`.
- `config.rs:459-464`: rewrite the doc comment ("There is no master switch" is now false).
- `config.rs:1121,1128,1134`: three bare `EmbeddingsConfig { model, dim }` literals - add `enabled` or `..Default::default()` (compile break otherwise).
- `config.rs:236-253` `DEFAULT_CONFIG_TOML`: document `enabled = false` with the two-line why; drop "Search defaults to the vector arm".
- `config.rs:1029` `install_runtime`: also call `embed::init_enabled(cfg.embeddings.enabled)`.
- `config.rs:754-763` `env_mirror`: the filter admits only `storage_path` and `creds_*`, so `POND_EMBEDDINGS_ENABLED` is silently dropped. Widen it to admit `embeddings_enabled` (needed by containers, CI fixtures, and the e2e harness, which have no config file).
- `embed.rs:336-354`: add an `ENABLED: OnceLock<bool>` beside `MODEL_ID`, with `init_enabled` / `embeddings_enabled()`. Same pattern as `init_model_id` / `model_id`.

### 4.2 Default arm and refusal

- `wire.rs:619-623` `SearchModeWire`: move `#[default]` to `Fts`. Rewrite the doc comments at `wire.rs:596-599, 615-616, 652-654`.
- `transport.rs:706-712` `parse_search_mode`: `None` resolves to `Fts`.
- `handlers.rs:1410-1423` `resolve_effective_mode`: when `requested == Vector && !embeddings_enabled()` return an error envelope (kind and retry semantics per spec.md#error-model - non-retryable, client error) with the message: `semantic search is off on this instance; set [embeddings].enabled = true in pond's config, run pond optimize --only embed, and retry with mode="vector" - or use mode="fts"`. When enabled, keep today's degrade-to-fts. Drop the degrade comments at `handlers.rs:1197-1200, 1234-1244` that describe the disabled case.
- `handlers.rs:2067, 2285`: the test helper hardcodes `SearchModeWire::Vector`; switch to `SearchModeWire::default()` so a test guards the default, and fix the comment.
- `main.rs:512, 533-536` `pond search --mode` help text.

### 4.3 Write path: no embedder when disabled

- `main.rs:1501, 1547` (`serve`, `mcp`): construct `LazyEmbedder` and attach only when enabled. Keep `spawn_idle_reaper` on the enabled path.
- `main.rs:4036-4071` (`sync`): gate `with_embedder`, `with_ingest_embed_progress`, and the eager preload block on `enabled`. Do not delete the preload - enabled users still want the 466 MiB download to happen before the progress bars own the terminal.
- `main.rs:4786` `run_embed_stage`: first statement `if !embeddings_enabled() { return Ok(EmbedSummary::default()) }`. Keep the model-swap guard (`embedding_model_swapped`, `--force-embed`, `guard_embedding_model_unchanged` at `main.rs:4762`) on the enabled path - it is the only thing preventing two embedding spaces in one IVF index.
- `main.rs:1434-1450, 3690-3714` `optimize --only embed`: when disabled, error with the same message as the refusal (it names the fix). `--force-embed` when disabled: same error.
- `main.rs:4158` comment: restate the invariant per 3.2.
- `benches/commands_bench.rs:346-359` reimplements the embed phase with `CandleEmbedder::load()` directly; gate it on `config.embeddings.enabled` (`config` is already threaded at `:273`).

### 4.4 Index maintenance: IVF intent removed when disabled

- `sessions.rs:4780-4840` `pond_index_intents` (and the test variant with a threshold): push the IVF intent only when `embeddings_enabled()`. This removes it from `optimize_indices`, `build_indices_only`, `rebuild_indices`, `cleanup_old_versions`, and `index_status` in one place.
- Verify the consequences the storage review is asked to confirm (section 8): that compaction keeps an existing index the intents list does not name, that `index_status` does not report it as missing, and that `--rebuild` does not drop it. If compaction drops or invalidates an un-named index on Lance 8, the plan changes to "keep the intent, skip the fold" and section 3.1 needs the all-null guard extended to `IvfSqCosine`.

### 4.5 Output: nothing about semantic when disabled

- `main.rs:6345-6430` `IndexHealth` / `render`: `semantic` becomes `Option`; the line reads `text ready` / `text pending` when disabled. Enabled keeps `text + semantic ready`.
- `main.rs:4949-4952, 4979` sync summary doc + output: same.
- `main.rs:1280-1287` `status --verbose`: skip `embedding_progress()` when disabled (it is a data scan).
- `main.rs:6036-6064, 5832-5838`: other status surfaces naming semantic - gate.
- `main.rs:2644, 2755` (`copy`/`restore` recap "text + semantic rebuilt on destination"): gate.
- `main.rs:4589-4608, 4711-4715` sync HUD and first-sync notice: wording is reached only on the enabled path after 4.3; verify, leave.
- `transport.rs:1129-1170` `stats://pond`: drop the embeddings block when disabled.
- `init.rs:344`: the prompt text "and embeds" - drop.

### 4.6 Descriptions, skill, docs

- `transport.rs:234-235, 252, 257-258, 316-319, 576-579, 743-746`: server instructions and tool descriptions. `mode="fts"` (default) exact words, BM25; `mode="vector"` when this instance has embeddings enabled - paraphrase-style recall. Keep the instructions short (repo rule: routing lives in instructions, do not fatten descriptions).
- `packages/pond/SKILL.md:16`: default wording.
- `packages/hermes-pond/tools.py:70,78`, `packages/pi-pond/src/tools.ts:75`, `packages/openclaw-pond/src/schemas.ts:23` and each plugin's README: flip the default wording; enum stays `["fts","vector"]`.
- `README.md:46, 126, 162, 221, 222`; `docs/site/src/pages/reference/{mcp-tools,cli,configuration}.mdx` (`[embeddings]` is undocumented on the site today - add it with `enabled`); `docs/site/src/pages/get-started/connect-your-agents.mdx`; `ops/examples/pi-fleet/README.md:10, 58, 59` ("there is no switch to turn that off").
- `docs/researches/2608-21-semantic-vs-fts-usage-eval/README.md` section 6: add one line "implemented in vX.Y.Z" after release (post-merge, not in this PR).

### 4.7 Tests and harness

- `tests/integration/transport_mcp.rs:213-221`: the only hard Rust break - the transcript expectation "1 nearest message" flips to "1 matching message". Add `"mode": "vector"` to the request AND add a sibling test for the new default wording.
- `tests/integration/transport_http.rs:116`: add `"mode": "vector"` so the vector arm stays covered.
- `src/snapshots/pond__tests__help_{search,sync,optimize,status,root,serve}.snap`: `cargo insta accept` after reviewing the diff.
- `benches/serve_mem_bench.rs:1145-1183, 1295-1339`: the `vector_first` / `vector_steady` phases must skip-with-reason when disabled, otherwise the `idle_target_mib` gate passes falsely against an FTS query labelled vector.
- `benches/backend_bench.rs:165` `mode.unwrap_or_default()`: make the mode explicit.
- `benches/ops_bench.rs:108, 128-129`: staleness probes call into the enabled-only path; gate.
- `ops/scripts/bench-gate.sh:48`: add `--mode vector` to the search probe; `:69-71` give `iops()` a `:-null` default so a missing `vector_search` row cannot emit invalid JSON.
- `ops/e2e/run.py:261-262`: add `--mode vector` and an fts-default case; `:355` add `--only embed` under an enabled config.
- CI gap: `moon.yml:31-36` runs `test` without `--all-targets`; benches compile only under `lint-msvc`, which fork PRs skip. Run `cargo clippy --all-targets -- -D warnings` locally before pushing.

### 4.8 Spec (`docs/spec.md`)

- 8.1 (line 727): "`vector`, the default" -> "`fts`, the default"; replace "The vector arm falls back to full-text when no message is embedded under the configured model" with: "The vector arm is available only when embedding is enabled on the serving instance; a `vector` request on an instance with embedding off is refused with an error that names the setting. With embedding on and no message yet embedded under the configured model, the vector arm degrades to full-text."
- 8.7 (line 763): keep sentences 1-2; replace sentence 3 with "With it on, the `vector` arm is available on request; `fts` stays the default arm either way."
- 8.6 (line 759): unchanged in substance; "when embedding is enabled" now has a referent.
- 8.8 (line 767): add "The IVF intent is part of index maintenance only while embedding is enabled; an existing vector index on a store served by an instance with embedding off is left untouched."
- 7.8 (lines 695-703) verbs `init`, `sync`, `optimize`, `status`, `serve`: strike the unconditional embed/model wording; `sync` "embeds inline when embedding is enabled"; `status` drops the semantic line when off.
- 3.7 (lines 212, 219): "FTS + vector fold at the sync tail" -> conditional.
- Contents line 19 and 1.1 line 31 if they name the vector default.

## 5. Implementation order

1. Config + runtime flag (4.1). `cargo build` green with the three literals fixed.
2. Write path gates (4.3) and intents (4.4). Run `cargo test --lib sessions::tests::` - the inline-embed unit test at `sessions.rs:6111-6148` must stay green; it passes only if the gate lives at the attach sites in `main.rs`, not inside `Store`.
3. Default arm + refusal (4.2). Fix `transport_mcp.rs:213`, `cargo insta accept`.
4. Output gating (4.5).
5. Descriptions, plugins, docs, spec (4.6, 4.8).
6. Harness and benches (4.7). `cargo clippy --all-targets -- -D warnings`.
7. Acceptance (section 7) against a real store copy.
8. CHANGELOG: release-plz generates the entry; enrich it on the release PR under `🛠 Breaking Changes` with the upgrade note: "Embeddings are off by default. `mode=vector` is refused until `[embeddings].enabled = true`; existing vectors are kept and resume on the next `pond optimize --only embed`. Default search mode is `fts`."

Commit subject: `feat!(search): fts is the default arm; embeddings opt-in via [embeddings].enabled`. One PR, squash-merged; keep the `!` in the squash subject.

## 6. Error envelope

The refusal must be non-retryable (an MCP client that retries on transient kinds would loop). Use the client-error kind spec.md#error-model defines for invalid requests; include `config_key: "embeddings.enabled"` in the envelope's detail object so a plugin can render the fix without parsing prose. Over `pond serve` HTTP the refusal rides inside the JSON-RPC result like every other tool error (not a transport 4xx), so Claude Code shows the message verbatim.

## 7. Acceptance

All against `cargo build --release`; C and D against a copy of a real store that already has an IVF index (`pond copy` from the S3 store to a local dir).

| Case | Steps | Expect |
|---|---|---|
| A fresh, disabled | `pond init && pond sync` on a CPU-only box, no config edit | no model download, no embed stage, `indexes text ready`, `pond_search` default returns BM25 hits, `mode=vector` returns the refusal naming `embeddings.enabled`, steady RSS of `pond mcp` after 20 fts queries < 200 MiB (measure; if the rowmap transient from #61 breaks this, record the real floor and amend the issue - it is not an embeddings cost) |
| B fresh, enabled | `enabled = true`, `pond sync`, `pond optimize --only embed` | today's behaviour byte-for-byte: model loads, vectors written, `mode=vector` works, `indexes text + semantic ready`; `scripts/replay.py` from the eval runs |
| C upgraded store, disabled | store with IVF index + 2M vectors; new binary, `enabled` absent; `pond sync` x3 with new sessions; `pond optimize`; `pond status` | no fold of the IVF index (trace `pond::perf` shows no IVF intent), no error, IVF index still present in the manifest, `mode=vector` refused, FTS finds the new sessions |
| D re-enable | same store, set `enabled = true`; `pond optimize --only embed`; `mode=vector` | backlog = exactly the rows ingested in C; they fold into the existing IVF index without a rebuild; vector search returns new-session hits |
| E mixed fleet | machine 1 enabled, machine 2 disabled, same store; each syncs its own sources | no OCC conflict beyond today's; machine 2's rows are null-vector until machine 1 runs `optimize --only embed` (documented in 3.2) |
| F bench gate | `ops/scripts/bench-gate.sh` before and after on the same store | rows for both; no invalid JSON |

## 8. Open items pending the 2026-08-21 reviews

Three reviews (storage/Lance, API/integrations, ops/release) are running on this plan. Expected to amend: the 4.4 consequence check (does Lance 8 compaction keep an intent-less index), the envelope kind (section 6), container/CI enablement path (`env_mirror`), and the true RSS floor for case A. Update this document and the PR description when they land; do not start step 2 before 4.4 is confirmed.
