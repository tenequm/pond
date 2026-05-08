# LanceDB reference

Unopinionated reference snapshot of LanceDB and its ecosystem. Reads top to bottom; cross-links into the upstream repositories on GitHub `main` so each claim can be traced.

Snapshot date: 2026-05-08.

## Files

| File | Contents |
|------|----------|
| [`LANCEDB.md`](LANCEDB.md) | Single-file capability reference. What LanceDB is, current feature surface, what it can and can't do, and a tour of every repository under the `lancedb` GitHub org. |
| [`EVOLUTION.md`](EVOLUTION.md) | Timeline of how Lance and LanceDB got to where they are: file format generations (1.0 -> 2.0 -> 2.1 -> 2.2), API renames, manifest path schemes, async/sync split, blob API generations, and the OSS / Cloud / Enterprise positioning shifts. |

## Source repositories

All references resolve against `main` of the repos listed below. Local clones live under `~/pjv/lancedb/<repo>` for offline reading and grep.

| Repo | Purpose |
|------|---------|
| [`lancedb/lancedb`](https://github.com/lancedb/lancedb) | Core database. Rust crate + Python (PyO3) + TypeScript (napi-rs) + Java bindings. |
| [`lancedb/docs`](https://github.com/lancedb/docs) | The Mintlify documentation site backing `docs.lancedb.com`. Markdown + code snippets. |
| [`lancedb/lance-bench`](https://github.com/lancedb/lance-bench) | Benchmark harness; results stored back into LanceDB. |
| [`lancedb/vectordb-recipes`](https://github.com/lancedb/vectordb-recipes) | ~96 examples, tutorials, and applications. The richest source of working patterns. |
| [`lancedb/lancedb-claw`](https://github.com/lancedb/lancedb-claw) | OpenClaw plugins: `memory-lancedb-claw` (long-term memory) and the `lancedb-claw` context engine. |
| [`lancedb/lancedb-duckdb-demo`](https://github.com/lancedb/lancedb-duckdb-demo) | DuckDB Lance extension interop on a multimodal Amazon dataset. |
| [`lancedb/locomo-eval`](https://github.com/lancedb/locomo-eval) | LOCOMO long-conversation memory benchmark with three OpenClaw memory backends compared head-to-head. |
| [`lancedb/chat-with-videos`](https://github.com/lancedb/chat-with-videos) | Hybrid search + lazy blob streaming end-to-end app. |
| [`lancedb/openclaw-lancedb-demo`](https://github.com/lancedb/openclaw-lancedb-demo) | Tutorial for the proprietary `memory-lancedb-pro` plugin under OpenClaw. |
| [`lancedb/hf-upload-demo`](https://github.com/lancedb/hf-upload-demo) | Lance datasets on the HuggingFace Hub: schema evolution, blob columns, FTS. |
| [`lancedb/cocoindex-lancedb-demo`](https://github.com/lancedb/cocoindex-lancedb-demo) | Incremental indexing pipeline driven by CocoIndex. |
| [`lancedb/ocra`](https://github.com/lancedb/ocra) | Object-store read-through cache. Arrow `object_store` companion. |
| [`lancedb/lance-research`](https://github.com/lancedb/lance-research) | Source artifacts for the Lance 2.1 random-access paper. |
| [`lancedb/lancedb-mcp-server`](https://github.com/lancedb/lancedb-mcp-server) | Minimal MCP server exposing `ingest_docs` / `query_table` over a LanceDB table. |

### lance-format org

LanceDB is built on the **Lance** file format, which lives in its own GitHub org [`lance-format`](https://github.com/lance-format) and ships a small ecosystem of its own. Local clones live under `~/pjv/lance-format/<repo>`.

| Repo | Purpose |
|------|---------|
| [`lance-format/lance`](https://github.com/lance-format/lance) | The file format, table format, catalog spec, query engine, and language bindings (Rust + Python + Java). LanceDB pins this at `=7.0.0-beta.4`; upstream is moving fast. |
| [`lance-format/lance-namespace`](https://github.com/lance-format/lance-namespace) | The catalog-abstraction spec and language SDKs (Python + Java; Rust impl lives back in `lance`). Decouples Lance from any one catalog (Directory, REST, Polaris, Unity Catalog, Hive, Iceberg). |
| [`lance-format/lance-duckdb`](https://github.com/lance-format/lance-duckdb) | The official DuckDB extension. `INSTALL lance; LOAD lance;` then `ATTACH '...' TYPE LANCE`. Read + write, plus SQL surface for `lance_vector_search`, `lance_fts`, `lance_hybrid_search`. |
| [`lance-format/lance-ray`](https://github.com/lance-format/lance-ray) | Ray Data integration: Lance datasets as Ray Datasource / sink, distributed indexing and compaction. |
| [`lance-format/lance-graph`](https://github.com/lance-format/lance-graph) | Cypher + SQL query engine over Lance. Knowledge-graph CLI / API / web service on top. |
| [`lance-format/lance-context`](https://github.com/lance-format/lance-context) | Lance-native versioned agent-memory store. Dedicated `ContextRecord` schema (role, content, embedding, plan_id, step, tokens, timestamp). |
| [`lance-format/pglance`](https://github.com/lance-format/pglance) | PostgreSQL FDW (pgrx-based). Read-only `CREATE EXTENSION lance;` -> Lance datasets and namespaces as foreign tables. |
| [`lance-format/lance-data-viewer`](https://github.com/lance-format/lance-data-viewer) | Read-only FastAPI + browser inspector. Multi-version Docker images covering legacy Lance versions back to 0.3.1. |
| [`lance-format/lance-python-doc`](https://github.com/lance-format/lance-python-doc) | CI/automation repo that publishes the `pylance` Python SDK docs. |

## Maintenance

To refresh: `git -C ~/pjv/lancedb/<repo> pull`, re-read the files this snapshot draws from, and bump the snapshot date at the top of this README. Permalinks point at `main` and will follow upstream automatically; line ranges in `LANCEDB.md` may drift over time.
