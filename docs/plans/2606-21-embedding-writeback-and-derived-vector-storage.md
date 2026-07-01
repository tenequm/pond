# Embedding writeback: stop merging derived vectors into the canonical table

Status: design note, undecided. Owner: TBD. Prereqs: none (pre-1.0, breaking changes are free).

## Problem

`pond sync`'s embed stage writes vectors back into `messages` via `merge_update` on the schema primary key `(session_id, id)`. Measured on the real 2.1M-row local store, updating **8 vectors took 6.36 s** and read **143 MiB** - the merge *join* scanning the full `session_id`+`id` key columns to locate the rows. The write itself is tiny (1 data file + 1 deletion vector); the join is the cost. Lance 7.0.0 only index-accelerates **single-column** merge keys (`merge_insert.rs::join_key_as_scalar_index`: `on.len() != 1 -> None`), so pond's composite key gets no index path - a full key scan on every embed.

This cost is fixed per sync regardless of how few rows are new, so it dominates every active-coding sync (which always ingests a message or two from the live session).

## Root cause (not the writeback - the design that forces it)

The `vector` is **lazily-derived, separately-written data stored as a mutable column in the append-once `messages` table**. That one choice forces an UPDATE, and in a columnar immutable-fragment store an update is the expensive operation. The same root produces three measured costs we have so far treated separately:

- the 6.36 s composite-key merge join (this note);
- embedding's `merge_update` rewriting message fragments, which churns the resident rowmap (we taught the rowmap's delta path to cope in `perf(sync): delta-extend the rowmap`, but the churn remains);
- `vector IS NULL` scanning the 1.2 GB Float16 column (dodged via the co-set `embedding_model IS NULL` swap in `perf(embed): scan the narrow embedding_model column`).

Those three commits are correct and worth keeping, but they file down symptoms. The cure is to make embedding an **append, not an update**.

## The constraint that makes this non-trivial

The naive "just move vectors to their own dataset" answer collides with a deliberate optimization: pond's vector arm does **scalar-prefilter pushdown** - it prefilters the kNN by `project` / date / `session_id` *in the same scan over `messages`* (`spec.md#search-prefilter-pushdown`, the load-bearing fix for remote vector-arm latency; see memory `project_pond_remote_vector_arm_slow`). Separate the vector out and the filter columns are no longer co-located with the vector, so you either denormalize those columns into the vector store or lose pushdown. That tension - write-path win vs read-path regression - is the actual decision.

## Options

### C - separate append-only vector dataset
Vectors live in their own Lance dataset keyed by `(session_id, id)`; the IVF_SQ index moves there; embedding is an `append_stream` (bandwidth-bound, no join, no fragment rewrite). Search's vector arm runs kNN there, returns keys, hydrates metadata via the row-key map that already exists.

- Pro: kills the merge join AND the rowmap churn AND removes the 1.2 GB column from `messages` (cheaper message scans). Best fit for the imminent S3 backend - the copy benchmark already shows append is bandwidth-bound while merge is commit-latency-bound on S3 (5.47x; memory `project_pond_s3_imminent`, `write_bench`).
- Con: breaks prefilter pushdown unless the 4 narrow filter columns (`session_id`, `project`, `source_agent`, `timestamp`) are **denormalized** into the vector store. They are cheap (no `search_text`, no vector), so duplicating them keeps pushdown at a small write cost. Touches schema, embed write path, search vector arm, and IVF index location. Largest change.

### D - embed at ingest
Compute embeddings during ingest so the vector rides the initial row append; no later merge for fresh data.

- Pro: kills the merge for the common path; keeps single-table prefilter pushdown unchanged.
- Con: couples ingest to the embedder (model load + inference inline). `pond sync --no-optimize` (deliberately fast, embed-less ingest) and model-swap re-embed still need the merge/rewrite path, so the expensive path does not disappear, it only stops being the default. Foreign/copy ingest of already-embedded data is fine (append with vectors).

### E - single-column surrogate key + scalar index
Add a surrogate `pk` column (e.g. `session_id || '/' || id`) with a BTree, and `merge_update` on `["pk"]` so Lance's `join_key_as_scalar_index` fires (`on.len() == 1`) and maps the keys through the index instead of scanning 143 MiB.

- Pro: smallest change; directly kills the key scan; read path and schema model otherwise untouched.
- Con: adds a column and a per-sync BTree maintenance cost (the same index-rebuild-on-new-fragment behavior we already see on `messages.session_id`). Partly trades the merge scan for index maintenance rather than removing the update; a workaround, not a model fix. Needs a measurement that the index path actually beats the rebuild it adds.

## Recommendation

Decide against two facts: **(a)** how soon the S3 backend lands, **(b)** whether denormalizing the 4 filter columns into a vector store is acceptable.

- If S3 is near: **C**, accept the denormalization - it keeps pushdown for a small write cost and pays off triply (writeback, rowmap churn, message-table size) exactly where S3 hurts most.
- If you want the smallest safe step that kills the cost today and S3 is not imminent: **E**, gated on a benchmark proving the index path nets out ahead of the BTree maintenance it adds.
- Avoid **B (batch the writeback)** as anything but a stopgap: it trades embedding-freshness semantics to hide a write-path bug.

Do not ship any of C/D/E as a hot patch. Each is a schema/spec change; land it with a `spec.md` amendment, a `write_bench`/embed micro-benchmark for the write path, and tests that the vector arm + prefilter still return identical results.

## Not in scope / keep

`perf(embed)` (cheap counts, narrow-column backlog scan) and the rowmap delta-extend stay regardless of which option lands - they are correct independent of where the vector is stored.
