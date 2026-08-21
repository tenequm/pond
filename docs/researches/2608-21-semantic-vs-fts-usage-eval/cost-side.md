# Cost side: what removing embeddings would change (estimate, 2026-08-21)

Companion to README.md. Every figure below is a pond measurement from the repo or the session corpus, with its source; the "without embeddings" column is derived from those and labelled [est].

Scope: remove the vector arm entirely - no embed at ingest, no model, no `vector` / `embedding_model` columns, no IVF index, no vector fold in `optimize`, no `has_embeddings` / backlog gates. Not the "deferred embed" design that pond already tried and measured its way out of (#71, #78, #82) - that design kept the backlog and paid for it on every sync; removal has no backlog.

Corpus for all numbers: 2.1-2.64M messages, 11-14k sessions, store on an S3-compatible object store (~59 ms RTT) unless stated.

## 1. Sync (write path)

| Stage | With embeddings (measured) | Without [est] | Source |
|---|---|---|---|
| Embed a typical 5-min delta (tens of messages) | < 1 s compute, but requires the model: **~0.6-4 s load** (arena on/off) on a cold process, ~500-790 MiB RSS step | 0 s, no model | #82; embed arena bench 2026-05-27; pond-sb RSS table 2026-07-22 |
| Model preload on a caught-up sync | skipped (best-effort preload; no-op sync never loads) | 0 | spec 7.8 |
| Backlog / coverage gates | `unindexed_vector_backlog` manifest fast path ~0 s; before guarding: **7.35 s per run on S3**, >25 min unguarded | gone entirely (no gate to get wrong) | #71/#78, thread 2026-07-09 |
| Vector index fold in `optimize` (tail of every sync) | batched; before batching **15-445 s** per sync for FTS+vector fold combined | FTS fold only - roughly half the fold work, and no IVF retrain on model swap | #82 |
| Initial full-corpus sync | **hours**: embedding dominates (observed ETA 2-11 h for ~10.5k sessions with two syncs contending for Metal), ~3 cores flat, 650 MiB flat | bounded by parse + S3 commits; the 44 s steady-state sync is already embed-free on a caught-up store, so first sync [est] drops from hours to tens of minutes for a 2M-message corpus (parse + append, no GPU/CPU embed phase) | 2026-07-06 session; #82 (44 s) |
| Steady-state remote sync | **~44 s** (post-#82) | ~40 s [est]; the embed share of a small delta is < 1 s, the gates are already near-zero | #82 |

Net on the write path: steady-state sync barely changes (< 1 s + a model load when a delta exists); **first sync and any machine without Metal/CUDA change a lot** (hours -> minutes, 3 cores -> ~1). The structural win is deleting the backlog/gate machinery that produced three perf PRs.

## 2. Storage

| Item | With | Without [est] | Source |
|---|---|---|---|
| `vector` column | ~1.5 KB/message (stored Float16 + JSONB overhead quoted in-session) -> **~3-4 GB** on 2.6M messages | 0 | 2026-07-22 thread (6.7 GB messages data incl. "a 1.5 KB embedding vector") |
| `messages.lance` indices (FTS + IVF) | **1.5 GB** | FTS only [est] ~0.5-0.8 GB (IVF_SQ over 2.6M x 384 dims is the larger of the two) | same thread |
| Model cache on disk | **466 MiB** one-time per machine | 0 | pond-sb measurement |
| Per-message disk | ~38-40 KB/message (heavy corpus) | ~36-38 KB [est]; embeddings are ~4-5% of bytes | same thread |

Disk is not where embeddings hurt; ~5% of the store.

## 3. Read path / serving

| Item | With (measured) | Without [est] | Source |
|---|---|---|---|
| Idle RSS, `pond mcp`/serve, never served a vector query | **~100 MiB** | same | pond-sb table |
| After first vector call or embed | **~894 MiB** (model ~500-790 MiB) and stays until next idle eviction | ~100-200 MiB; the ~200 MiB live Lance cache floor for FTS postings remains | pond-sb table; issue #61 |
| serve_mem_bench idle floor (local 2.1M corpus) | **877 MiB** phys_footprint, of which ~470 MiB transient from the f32->f16 model cast | [est] ~400 MiB: rowmap transient (2.1 GB peak, #61 lever 1) remains and is unrelated to embeddings | issue #61 |
| Warm `pond_search`, vector arm | embed query ~20 ms warm, **seconds after the 60 s idle-evict reload**; IVF probe ~70 ms, **101 GETs / 2.8 MiB** | fts arm: **32 GETs / 296 KiB**, ~0.1 s; no model reload ever | #79; [2608-12 read-path doc](../2608-12-read-path-where-time-goes.md) |
| CLI replay in this study (local store) | vector **0.99 s** mean | fts **0.39 s** mean | README 4.3 |
| `has_embeddings` decision | ~0 ms now; was **6.8-11.7 s per query** (full vector-column scan) before #79 | the decision does not exist | #79 |
| Cold first fts query | 47-300 s (postings not prewarmed) - **independent of embeddings**, would still need fixing | same | 2608-12 doc |
| Get family (29-166 s cold) | unrelated to embeddings | same | 2608-12 doc, #141 |

Read-path truth: the dominant latencies today (gets, cold FTS postings, parts hydration, per-session cold process) are **not** embedding costs. Removing embeddings takes out the model reload (seconds per real-world vector query because calls arrive minutes apart and the embedder evicts at 60 s), ~70 extra S3 GETs per search, and ~700 MiB of RSS per process once a vector query has run - with N Claude Code sessions each spawning their own `pond mcp`, that last one is N x 700 MiB.

## 3b. Field receipt: a home server, 2026-08-20

Intel N100 (4 cores, 16 GB, 8 GB swap) running the home media stack plus three pond instances against the remote store:

| Process | Age | RSS | Swap | Total |
|---|---|---|---|---|
| `pond mcp` (stdio child of a Claude Code session) | 15 d | 1.97 GB | 3.29 GB | ~5.2 GB |
| `pond serve --port 9797` (agents' MCP endpoint) | 5.7 d | 1.05 GB | 2.17 GB | ~3.2 GB |
| `pond sync nanoclaw` (backfill) | 20 min | 1.56 GB | - | 1.6 GB |

Swap 100% used, 250-450 MB free; pond alone ~4.6 GB RSS + 5.5 GB swap, more than the entire media-server stack on the same box. The backfill ran at 167% CPU with one thread pinned ("CPU-bound embedding on an N100, not S3"): 38 sessions / 2,967 messages in 38 min. After cleanup, `pond serve --with-sync` grew 312 MB -> 1.05 GB on its first sync cycle because it loads the embedding model to sync. Attribution: roughly half of each long-lived process is embedding-related (model residency + f32->f16 load transient + IVF share of the index cache); the rest is the rowmap build transient (issue #61), which is independent of embeddings. All of the sync CPU is embedding.

## 4. Engineering surface removed

Candle + Metal/CUDA feature matrix (the macOS-only subsystems CI never executes, 2026-07-30 finding), f16 safetensors asset and its loader, model download UX and the 466 MiB first-run hit, `LazyEmbedder` eviction, the embed stage of `optimize`, `--force-embed` model-swap merge, IVF retrain on swap, Windows CPU-only embedding caveat, `[embeddings]` config, the `vector`/`embedding_model` schema and its backfill rules (spec 5.5, 8.5-8.8). Roughly a third of spec section 8 and the entirety of 5.5.

## 5. Bottom line

- **Resources:** per-process RSS ceiling ~900 MiB -> ~100-200 MiB; first sync hours -> minutes; no 466 MiB download; no GPU/3-core embed phase; store ~5% smaller.
- **Speed:** steady-state sync unchanged (< 1 s); search loses the model-reload seconds and ~70 GETs per query; nothing else on the read path moves, because the slow paths were never the vector arm.
- **What it buys back in quality:** the ~6-7% of calls where only semantic retrieval worked (README 5.2), concentrated in paraphrase queries - the case a lexical-first cascade with query rewriting, or an opt-in external embedder (deja-vu's shape), can cover without putting a model in every user's sync.
