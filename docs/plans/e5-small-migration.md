# e5-small migration plan

Goal: replace the default embedding model in pond from `Qwen/Qwen3-Embedding-0.6B` to `intfloat/multilingual-e5-small`. Switch the loader path from fastembed-rs's candle/Qwen3 backend to fastembed-rs's standard ORT-backed `EmbeddingModel::MultilingualE5Small`.

Status: not started. No edits land before each stage is approved.

## Why - the reasoning trail

### Triggering observation

Qwen3-Embedding-0.6B is heavy on load: 1.2 GB resident at bf16 in candle, 596 M total params (440 M active). The user's prior baseline in claude-kb was `BAAI/bge-small-en-v1.5` (~285 MB resident, 33 M params, English-only), which felt right-sized. Pond's "one model loaded per MCP process per session" pattern multiplies the Qwen3 footprint across concurrent sessions.

### Constraint set the candidate model has to satisfy

Pulled from `docs/design.md` plus user requirements:

1. **Multilingual**. design.md never says "multilingual" outright but picks Qwen3 (100+ languages); pond's implicit position is multilingual.
2. **Provider-shipped quantization preferred**. User stated preference for vendor-blessed int8 over community ONNX/GGUF.
3. **Runs well on Metal / GPU** ideally; CPU acceptable if fast enough.
4. **Meaningful token capacity per record**. Pond's `max_embed_tokens` default is 1024; design.md notes ~98% of `~/.claude/projects/` messages fit under that cap.
5. **Resident memory close to `bge-small-en-v1.5`** (~285 MB target).
6. **Non-commercial license acceptable** for personal use; commercial-friendly preferred.

### What fastembed-rs 5.13.4 (pond's pinned version) can actually load

Cargo features in pond today: `qwen3 + ort-load-dynamic + metal + hf-hub-rustls-tls`.

Two backends in fastembed-rs:
- **candle path** (`Qwen3TextEmbedding::from_hf`): Metal-capable on macOS, CUDA-capable on Linux. Currently only Qwen3.
- **ORT path** (`EmbeddingModel` enum): CPU-only in pond today (no CoreML EP wiring). Many models registered, including the multilingual e5 family, BGE-M3, EmbeddingGemma300M (third-party mirror), Snowflake Arctic family.

What's **not** in fastembed-rs's enum: granite-r2 family, harrier-oss, jina-v3/v4/v5 (only v2-base-en and v2-base-code). Adding any of these requires a fastembed-rs PR (small for standard-shape ONNX models, larger for custom loaders).

### Practical option space narrowed to three

| Model | Active params | Dim | Ctx | Resident | Hakari mean | License | Already loadable |
|---|---|---|---|---|---|---|---|
| `multilingual-e5-small` | 22 M | 384 | 512 | ~270 MB | 53.2 | MIT | yes |
| `multilingual-e5-base` | 86 M | 768 | 512 | ~620 MB | 55.4 | MIT | yes |
| `jina-embeddings-v5-text-nano` | 140 M | 768 (Matryoshka) | 8192 | ~575 MB | 63.4 | CC-BY-NC-4.0 | no (fastembed-rs PR 250) |

Qwen3-Embedding-0.6B baseline mean for comparison: 57.97.

### Quality impact of the chosen direction

Switching Qwen3 -> e5-small drops mean Hakari score from 57.97 to 53.23: **-8.2% relative**, roughly "1 in 12 hits lost" on retrieval benchmarks. For pond's median-20-token messages the gap is expected to compress further - short text plays to e5-small's strengths (less semantic surface area to differentiate). Quality regression is the price paid for the resource win below.

### Resource impact of the chosen direction

- Resident memory per MCP process: 1.2 GB -> ~270 MB (4.4x reduction).
- Vector dim: 1024 -> 384. Vector storage at fp32 for 689 k messages: 2.8 GB -> 1.06 GB. With fp16 storage (separate decision): 530 MB.
- max_embed_tokens cap: 1024 -> 512. Drops the over-cap tail from ~2% of messages to a slightly larger fraction; FTS still indexes the full text per 3.3.1.
- Inference path: Metal (candle) -> CPU (ORT). For a 22 M-param encoder on Apple Silicon CPU, expected throughput is ~200-500 docs/sec - likely faster than Qwen3-on-Metal for pond's short-message workload.

### Quantization caveat

`intfloat/multilingual-e5-small` ships an official int8 ONNX (`onnx/model_qint8_avx512_vnni.onnx`, ~118 MB), but fastembed-rs's `MultilingualE5Small` entry loads the fp32 ONNX (`onnx/model.onnx`, ~470 MB). Consuming the vendor int8 needs a fastembed-rs patch pointing at the int8 file. Out of scope for this migration; tracked as a follow-up.

## Scope

In scope:
- Replace the Qwen3 backend implementation with an e5-small backend.
- Update the embedding model registry default and known-models validator.
- Adapt asymmetric query/document prefixing: e5 needs `"query: "` and `"passage: "` rather than Qwen3's `"Instruct: ... Query: ..."`.
- Update IVF_PQ `num_sub_vectors` for the 384-dim case.
- Drop the existing `embeddings` table and let `pond embed` repopulate. Per CLAUDE.md, no migration shim.
- Update tests that hardcode Qwen3 specifics (device label assertions, dim assertions).
- Update design.md section 3.2.4 to reflect the new default; full design.md restructuring is a separate effort.

Out of scope (each a follow-up):
- Loading the vendor int8 ONNX variant.
- Storing embedding vectors at fp16 instead of fp32.
- Adding jina-v5-text-nano, granite-r2, or other models requiring upstream fastembed-rs PRs.
- Rewriting design.md to separate requirements from implementation.

## Stages

### Stage 0 - decide open questions

Resolve before code:

1. **`num_sub_vectors` for 384-dim**: 32 (12-float subspaces, textbook default) vs 48 vs 64. Pick after a quick measurement on a representative corpus slice, or accept 32 as a sensible default and revisit if recall is poor.
2. **Trait shape**: extend `EmbedBackend` with `embed_query` / `embed_document` (cleaner, mirrors icm's approach, makes future asymmetric models cheap), or push the prefix into call sites (smaller diff, but every future asymmetric model touches every call site).
3. **`max_embed_tokens` default**: 512 (matches e5's training cap) or 256 (stays well under, smaller per-batch cost budget).

### Stage 1 - backend swap

- Rename `src/embed/qwen3.rs` -> `src/embed/e5_small.rs`. Replace `Qwen3TextEmbedding::from_hf` with `TextEmbedding::try_new(InitOptions::new(EmbeddingModel::MultilingualE5Small))`. Drop the candle device-selection helpers.
- Rename `Qwen3Embedder` -> `E5SmallEmbedder`. Update the `pub use` re-export in `src/embed/mod.rs`.
- Update 4 call sites in `src/main.rs`.

Done-when: pond builds without the `qwen3` or `metal` fastembed features; embedding inference produces 384-dim L2-normalized vectors on CPU.

### Stage 2 - asymmetric prefix

Pick the trait shape from Stage 0. Implement on `E5SmallEmbedder`; update the query path in `src/handlers.rs` to call `embed_query` instead of prefixing inline.

Done-when: a unit test asserts that the worker side embeds `"passage: <text>"` and the search side embeds `"query: <text>"`; verified against a `MockEmbedder` that records the exact strings handed to `embed()`.

### Stage 3 - registry and config

- Add `EmbeddingModel::e5_small_default()` in `src/config.rs` next to `qwen3_default()`. Fields: `id = "intfloat/multilingual-e5-small"`, `dim = 384`, `max_embed_tokens = 512`, `num_sub_vectors = 32` (or whatever Stage 0 picks), `distance = Cosine`, `normalize = true`, `default = true`.
- Swap `builtin_models()` to return it.
- Update the 6 test spreads in `src/config.rs::tests` that use `..qwen3_default()`.

Done-when: `pond config --print-schema` shows e5-small as the default; `cargo test --lib config` passes.

### Stage 4 - schema and table reset

- Drop the existing `embeddings` table on first run after this commit if its vector column dim is 1024. Detect via the dataset schema, not a config flag - new pond instances also pass this check.
- The `FixedSizeList<Float32, N>` schema gets `N` from the registry entry's `dim` field at table creation; no schema struct hard-codes 1024.

Done-when: a fresh `pond ingest` followed by `pond embed` on the `~/.claude/projects/` corpus produces 384-dim rows under `model_id = "intfloat/multilingual-e5-small"`.

### Stage 5 - Cargo and tests

- `Cargo.toml`: drop `qwen3` and `metal` from `fastembed` features. Keep `ort-load-dynamic`, `hf-hub-rustls-tls`.
- Delete `macos_selects_the_metal_device` test in the renamed file.
- Audit test assertions against `dim == 1024` or `model_id` containing `"Qwen3"`; update.

Done-when: `cargo test` and `cargo clippy --all-targets -- -D warnings` both clean.

### Stage 6 - design.md 3.2.4 touch-up

Replace the candle/Metal/Qwen3 sentences in section 3.2.4 with the e5-small/ORT/CPU equivalents. Keep the section's structure; full requirements-vs-implementation restructuring of design.md is tracked as a separate effort.

Done-when: design.md no longer references Qwen3 as the default; the embedding section names e5-small as the current implementation with the resource/quality numbers from this plan as justification.

## Verification

- Bulk-embed a 10 k-message slice of `~/.claude/projects/`, record throughput in docs/sec.
- Run `pond search` against a fixed query set on the same slice, compare top-K hits qualitatively to the same query set under Qwen3 (re-embedded once for the comparison). Spot-check ~20 queries; not a formal eval.
- Confirm resident memory of the `pond mcp` process via `ps -o rss` stays under 400 MB at steady state.
- Run the existing FTS + vector hybrid integration tests; RRF behavior should be unchanged - only the vector leg's model identity differs.

## Risk register

- **Recall regression on pond's specific corpus** beyond the 8% Hakari-projected drop. Mitigation: keep Qwen3 reachable as a non-default registry entry so a user can flip back via config without code change.
- **CPU-only inference too slow for bulk re-embedding** on a fresh ingest. Mitigation: measure in Stage 4; if too slow, fall back is sticking with Qwen3 on Metal until a candle loader for e5-small lands in fastembed-rs.
- **fastembed-rs default-loads the fp32 ONNX** even though int8 exists. Accepted for this migration; tracked as a follow-up to upstream a PR that toggles between the two.
