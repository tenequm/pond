# Hybrid search tuning

Research record for pond's hybrid retrieval. `docs/spec.md#search` is the source of truth for behavior; this file explains the tuning behind the spec-allowed defaults and the experiments that produced them. The harness lives at `ops/search-benchmarks/`.

## What is measured and how

pond serves hybrid search by default when embeddings exist for the configured model, FTS-only otherwise. The hypothesis the design embeds is that adding embeddings improves retrieval over a real session corpus in practice.

- **Headline metric.** Success@3 per stratum with 95% Wilson CIs. A single MRR averaged across heterogeneous query styles is an invalid headline (Buckley & Voorhees, SIGIR 2000/2002). Strata with n<30 are labelled "directional, underpowered"; cross-stratum totals are reported but never as the lede.
- **Supporting metrics.** P@1 per stratum; MRR per stratum. Significance: paired sign test on Success@3 across modes on the same queries.
- **Why S@3 instead of S@10.** "Lost in the Middle" (Liu et al., TACL 2024) implies precision at rank 1-3 matters far more than recall at rank 20 for an agent that injects 1-3 sessions of context.

Three coupled sub-questions:
1. Q-hybrid-vs-fts: does the default hybrid mode beat FTS-only on a real, stratified query set?
2. Q-hybrid-vs-vector: does the default hybrid mode beat pure-vector retrieval?
3. Q-vector-vs-fts: which single retriever is stronger on which stratum?

Together they identify whether fusion is a genuine improvement over both component retrievers, or whether one already dominates and the fusion is decorative.

### Query strata

Two seed sets ship in `ops/search-benchmarks/`:

- `queries-en.tsv` (21 English entries) and `queries-uk-translated.tsv` (21 Ukrainian-translated entries against the same English-language session-id targets). Strata cover natural-language, conceptual, symbol-lookup, error-message, bare-keyword. The UK twin isolates "is the retriever cross-lingual?" from "does the corpus contain this conversation?".

Derived/private sets (gitignored, regenerate locally):

- `queries-paraphrased.tsv` - 111 paraphrases of real user prompts. The primary benchmark for natural-language retrieval; the EN seed set has too few queries per stratum (paired tests at p=1.000) to detect either improvement or regression on this regime.
- `queries-real-use*.tsv` - corpus-derived sampling; useful for descriptive checks but **not a clean signal** since the queries are authored against the same corpus they search (labels are circular).

### Ground-truth schemes

TSV format: `id\tlang\tstratum\tquery\tground_truth`. Two schemes:

- `prefix:<id1>,<id2>,...` - at least one session_id or message_id whose 8-char prefix matches must appear in the result list. The 8-char prefix avoids hard-coupling to full UUIDs that may differ between re-syncs.
- `anchor:<substring>` - the literal substring must appear in the `text` field of some hit (NFC normalized, case-insensitive). Used when the message id is not stable across re-syncs.

### Anchor-reachability rule

An earlier benchmark wave burned a week because 18 of 18 Ukrainian queries had anchors that literally did not appear in any indexed message - verified independently against both pond and kb. All 18 scored 0/0 under every mode. Invisible until brute-forced.

Rule: **before any retrieval mode is run against a new query set, run `bench.py verify --queries <queries.tsv>`**. It issues each query against both FTS and Vector at top-200 and fails if any ground-truth anchor is reachable from neither. A query that fails BOTH arms is structurally unbenchmarkable; no fusion strategy can recover it. If the cross-check against `mcp__kb__kb_search` at `min_score=0.3` also returns nothing, the seed phrase is fictional.

### Pool-size invariant

Production hybrid runs `fts_search(pool=100)` and `vector_search(vector_pool=200)` internally (`src/handlers.rs::plan_search`) and fuses those candidates. If a fusion simulator (`bench.py sweep`) replays arm fixtures captured at `pond search --limit 20`, it only sees the top-20 from each arm and misses cross-arm agreement signal at deeper ranks. Always capture fixtures at `--limit 100` (FTS) and `--limit 200` (Vector) before drawing conclusions.

## Current production fusion: score-normalized

After per-arm score shaping (max-norm BM25 for FTS, rank-norm `1 - idx/n` for Vector), each arm's surviving (post intra-arm dedup by `session_root`) hits are min-max normalized over the full arm pool, then combined as `score = FTS_FUSION_WEIGHT * norm_fts + VECTOR_FUSION_WEIGHT * norm_vec`. Constants in `src/handlers.rs`:

- `FTS_FUSION_WEIGHT = 0.135`
- `VECTOR_FUSION_WEIGHT = 1.0`
- `RECENCY_MAX_BOOST = 0.05` (additive tiebreaker)

The 0.135:1 ratio sits in the centre of a wide plateau ([0.09, 0.14]) identified by random search across log-uniform weight space on the 111-query paraphrase set.

### Numbers (paraphrased, n=111)

| variant | S@3 | P@1 | MRR |
|---|---|---|---|
| asymmetric RRF (prior production: `k_fts=5, k_vec=20`) | 45/111 = 0.405 | 22/111 = 0.198 | 0.345 |
| symmetric RRF k=60 (LanceDB default) | 58/111 = 0.523 | 40/111 = 0.360 | 0.479 |
| **score-norm w_fts=0.135, w_vec=1.0 (current)** | **72/111 = 0.649** | **47/111 = 0.423** | **0.559** |

+0.244 absolute / +60% relative S@3 over the prior production. No stratum regresses (conceptual 0.68, factoid 0.69, lookup 0.71, task-request 0.62). Verified end-to-end via `bench.py run --mode hybrid` against the released binary.

## History

This file replaces three older artifacts; the full version of each is reachable from `git log docs/researches/embeddings/`:

- **Wave 1 (RRF redesign)** found the initial benchmark scoring 0/39 due to four bugs: TREC k=60 too flat for short-message corpora, message-keyed RRF double-counted cross-arm sessions, enumerate-rank-trap, and Lance's tied-score fragment-order nondeterminism. Fixes: session_root keying with intra-arm dedup-rank, asymmetric RRF `(k_fts=5, k_vec=20)`, stable secondary sorts in arm search, `RECENCY_MAX_BOOST` 0.2 -> 0.05.
- **Wave 2 (asymmetric-RRF plateau)** validated `(5, 20)` on a 39-query EN seed mix that was ~48% strict-keyword. Rejected (on that seed set) additive magnitude bonus, convex combination, weighted RRF, CombANZ, FTS-confidence gate. Per-stratum paired tests landed at p=1.000 across strata; the seed lacked power.
- **Wave 3 (paraphrase regression)** built `queries-paraphrased.tsv` (111 queries paraphrased from real user prompts) and showed asymmetric RRF was tuned on the wrong distribution: vector-alone beat hybrid 79-46 on Success@3 there (p=0.000 on the task-request stratum, n=72). Re-sweeping over weighted RRF, score-normalized, alpha-blend, multiplicative score*RRF, session-aggregation variants identified score-normalized fusion with FTS:Vector weight ratio ~0.135 as the wide-plateau optimum, beating any RRF variant by ~12 absolute points S@3 and no stratum regressing.

## What is still on the table

Three classes of failures remain. None is a fusion bug; they are harder problems.

1. **Diffuse multi-arm cross-validation on peripheral sessions.** A small set of content-dense pond conversations (`1dccdda6`, `95b77fc5`, `6b12da87` in the original benchmark) shows up across many queries because both arms cross-validate them. Per-session priors that downweight "universal hit" sessions were tried in the Wave 3 sweep and failed: the target sessions themselves are often the universal hits.
2. **Cross-encoder reranker.** Top-K rerank with Qwen3-Reranker-8B (Fireworks `qwen3-reranker-8b`) lifted paraphrased S@3 from 0.649 to 0.739 (+0.09 absolute, +14% relative). Deferred for v1: the gain doesn't justify the operational surface of a new model dependency (local 80MB-600MB ONNX + 20-80ms/query, or API key + ~200ms network + per-query cost). Revisit when real users (not benchmarks) report quality issues.
3. **Cross-lingual queries against a corpus dominated by another language.** Handled at the agent layer (the caller issues probes in both languages and unions hits by `session_id`); pond does not translate internally. The `pond_search` MCP description carries this guidance.

Query expansion is also explicitly a caller-layer concern. Lexical expansion is forbidden by `spec.md#search-language-neutral-index` (per-language transforms silently corrupt other languages); semantic expansion needs an LLM call pond's substrate deliberately omits.

## Files

- `ops/search-benchmarks/bench.py` - the harness; subcommands `run` / `verify` / `score` / `pair` (end-to-end) and `sweep` / `variant` (fixture replay through fusion variants).
- `ops/search-benchmarks/queries-en.tsv`, `queries-uk-translated.tsv` - the shipped seed sets.
- `src/handlers.rs::fuse_arms` - the production fusion.
- `src/handlers.rs::FTS_FUSION_WEIGHT`, `VECTOR_FUSION_WEIGHT` - the constants.

## Runtime / memory footprint

Bench harness: `benches/serve_mem_bench.rs` (`--probe-embedder` for isolated load/embed/drop/reload phases, full hybrid mode for end-to-end peak). The bench samples both `ps -o rss=` and macOS `phys_footprint` via `libc::proc_pid_rusage(RUSAGE_INFO_V4)`.

**Load-bearing finding**: on macOS `ps rss` is wrong in both directions - overcounts shared dyld pages (Metal frameworks, CoreFoundation, libonnxruntime.dylib) and undercounts compressed pages. The correct metric is `phys_footprint`, which Activity Monitor, the Jetsam OOM killer, `top` MEM, and `footprint(1)` all read. Steady-state PF for candle Metal e5-small was measured at 691 MiB vs RSS 435 MiB - opposite of what the candle 0.10 + Apple Silicon "buffer pool" diagnosis would predict from RSS alone. Switching to `phys_footprint` reranked every backend comparison.

**Decision (2026-05-27)**: drop the alternative ORT query backend, candle on every platform, evict the cached model after `DEFAULT_IDLE_EVICTION = 5 min` of no use. Time-weighted PF for typical MCP usage ~165 MiB; idle PF (post-eviction) is the 107 MiB process floor regardless of backend.

### Linked sources

candle:
- [PR #3197 - bound temporary buffer cache](https://github.com/huggingface/candle/pull/3197). Adds `CANDLE_METAL_CACHE_LIMIT_MB` / `CANDLE_METAL_PENDING_LIMIT_MB` env vars to force pool eviction; confirmed by pcuenca to flatten Stable Diffusion macOS RSS. Stalled in review since 2026-02. Not in 0.10.2. The cleanest knob for candle-Metal phys_footprint if it ever lands.
- [PR #3419 - PreallocKvCache + ConcatKvCache leak fix](https://github.com/huggingface/candle/pull/3419). Fixes a `BackpropOp` leak in `ConcatKvCache` that affects every candle decoder model. Not directly relevant to BERT-class encoder embedding (no KV cache), but shared evidence that candle's memory bookkeeping has open structural issues.
- [PR #3503 - BF16 CPU matmul via F32 cast](https://github.com/huggingface/candle/pull/3503). Adds BF16 matmul on the gemm / MKL / Accelerate CPU backends by casting to F32 and back via SIMD bulk conversion. Open. Not directly memory-relevant but unlocks BF16 weight loading on CPU - useful if we ever ship a BF16-quantized e5-small variant.
- [PR #3511 - Metal concurrent dispatching](https://github.com/huggingface/candle/pull/3511). Concurrent kernel dispatch with automatic memory barriers via read/write dependency detection. +10-20% tok/s on multiple models. Companion to #3532 (intra-encoder vs inter-encoder parallelism). Merged.
- [PR #3532 - improved Metal inter-encoder sync and gemv](https://github.com/huggingface/candle/pull/3532). Mutex-guard around compute/blit encoders, switches to untracked mtl buffers with explicit Input/Output dependency tracking so independent encoders run in parallel. Targets Metal command-buffer overhead and synchronization, not buffer-pool retention per se, but in the same path as the Metal-pool issues that inflate our phys_footprint. Merged.
- [PR #3555 - expose `kernel_mul_mm_id` MoE matmul wrapper](https://github.com/huggingface/candle/pull/3555). Lifts the single-pass MoE-fused matmul shader to callable Rust. Not relevant to e5-small (not MoE), tracked for completeness as part of candle's Metal-kernel surface evolution.
- [Issue #3464 - StorageModeShared weights pool](https://github.com/huggingface/candle/issues/3464). `new_buffer_with_data` puts permanent weight buffers into the same Metal pool as transients; pool walk on every allocation, weights stay shared/CPU-coherent forever instead of uploaded-and-freed-from-host. Direct contributor to `iokit_mapped` inflation we measured. No fix in flight.
- [Issue #2271 - autoreleasepool workaround](https://github.com/huggingface/candle/issues/2271). The objc2 migration (#3064, merged 2025-08-29) did not fully fix the leak; reproduced on master in 2026-03.

ONNX Runtime:
- [microsoft/onnxruntime#28231 - M5 Pro YOLOv8n RSS 381 MB vs PF 53 MB](https://github.com/microsoft/onnxruntime/issues/28231). The cleanest published RSS-to-phys_footprint ratio for a BERT-class ORT workload on Apple Silicon. KleidiAI LHS-cache identified as the dominant grow-then-plateau source; fix in [PR #28363](https://github.com/microsoft/onnxruntime/pull/28363).
- [microsoft/onnxruntime#11627 - `enable_cpu_mem_arena = false`](https://github.com/microsoft/onnxruntime/issues/11627). 2 MB model went 5792 MiB → 217 MiB RSS by disabling the arena. The single biggest documented ORT memory lever.
- [microsoft/onnxruntime#9152 - BERT per-thread scaling](https://github.com/microsoft/onnxruntime/issues/9152). 160 / 310 / 610 MB at 1 / 2 / 4 intra-op threads, explains why fastembed's `with_intra_threads(num_cpus)` is the second-biggest RSS leak on Apple Silicon.
- [microsoft/onnxruntime PR #27136 - `mlas.disable_kleidiai`](https://github.com/microsoft/onnxruntime/pull/27136). Session config to disable the per-thread int8-GEMM workspace. Lands in ORT 1.25+; pyke's prebuilt is 1.24.2, so the key is silently ignored. Tested, no movement.
- [microsoft/onnxruntime#23768 - `arena_extend_strategy = kSameAsRequested`](https://github.com/microsoft/onnxruntime/issues/23768). Middle-ground option that keeps the arena but bounds its growth via `OrtArenaCfg`. Not exposed by `pykeio/ort` 2.0.0-rc.12 SessionBuilder; reachable only via `Environment::create_and_register_allocator` + `.with_env_allocators()`.
- [microsoft/onnxruntime#27767 - 15-43 GB RSS on M3 Pro Node.js](https://github.com/microsoft/onnxruntime/issues/27767). Evidence that macOS ARM64 has unique ORT allocator pathologies in JS bindings - not our case, but rules out "ORT macOS is exactly like ORT Linux."
- [k2-fsa/sherpa-onnx#3032 - "2x model size" macOS rule](https://github.com/k2-fsa/sherpa-onnx/issues/3032). SenseVoice INT8 230 MB → 450 MB RSS by default; confirms the macOS-specific arena retention.

mistral.rs:
- [issue #1738 - severe Metal memory leak on Apple Silicon](https://github.com/EricLBuehler/mistral.rs/issues/1738). Throughput collapse on M3 Ultra continuous batching - swap thrashing, scheduler starvation. Same iokit_mapped retention family as candle. Closed but symptomatic of the broader Apple-Metal/Rust ML-runtime memory accounting gap.

macOS memory accounting:
- [bazhenov.me - Activity Monitor anatomy](https://bazhenov.me/posts/activity-monitor-anatomy). Definitive walkthrough of `phys_mem` vs `phys_footprint` ledgers and the `proc_pid_rusage` path.
- [alexey-pelykh.com - the RSS illusion](https://alexey-pelykh.com/blog/rss-illusion-hidden-gpu-memory). 4.1 GB RSS vs 62.7 GB phys_footprint across 95 Claude processes; documents the iokit_mapped category we hit.
- [rmk40/memlimit](https://github.com/rmk40/memlimit). C reference for cross-platform process-memory: phys_footprint on macOS, PSS on Linux (`/proc/PID/smaps_rollup`).
- [Bun PR #22352](https://github.com/oven-sh/bun/pull/22352). Bun's `process.memoryUsage.rss()` switched from RSS to `task_vm_info.phys_footprint`; concrete reference pattern for the Rust FFI.
- [psutil commit 36bd1b5](https://github.com/giampaolo/psutil/commit/36bd1b5214664672e7d1fcfd68a367553e3e76dc). psutil docs recommend phys_footprint over RSS for macOS memory monitoring.
