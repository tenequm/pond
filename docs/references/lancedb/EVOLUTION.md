# LanceDB and Lance: evolution

A timeline of the format generations, API rewrites, and platform changes that shaped the current shape of the project. Snapshot date: 2026-05-08.

This file is reference-only. For the current capability surface, see [`LANCEDB.md`](LANCEDB.md).

---

## Table of contents

1. [The two products: Lance vs LanceDB](#1-the-two-products-lance-vs-lancedb)
2. [Current versions at a glance](#2-current-versions-at-a-glance)
3. [Lance file format: 1.x -> 2.0 -> 2.1 -> 2.2](#3-lance-file-format-1x---20---21---22)
4. [Manifest paths v1 vs v2](#4-manifest-paths-v1-vs-v2)
5. [Stable row IDs](#5-stable-row-ids)
6. [Blob API: v1 metadata flag -> v2 separate-file blobs](#6-blob-api)
7. [Sync to Async (Python)](#7-sync-to-async-python)
8. [Concurrency: from no-S3-writes to commit stores](#8-concurrency)
9. [Indexes and quantization](#9-indexes-and-quantization)
10. [Search API evolution](#10-search-api-evolution)
11. [Cloud and Enterprise](#11-cloud-and-enterprise)
12. [Lance moves to its own GitHub org](#12-lance-moves-to-its-own-github-org)
13. [Deprecations and renames](#13-deprecations-and-renames)
14. [Decisions and signals visible only in GitHub conversations](#14-decisions-and-signals-visible-only-in-github-conversations)
15. [Areas under active iteration](#15-areas-under-active-iteration)

---

## 1. The two products: Lance vs LanceDB

The project ships as two layers:

- **Lance** - the file format, table format, and catalog spec. An open lakehouse format for multimodal AI. Cargo workspace under [`lance-format/lance`](https://github.com/lance-format/lance) (formerly `lancedb/lance`).
- **LanceDB** - the database that wraps Lance with disk-based indexes, an embedding registry, query API, multi-language SDKs, and Cloud / Enterprise managed offerings. Cargo workspace + Python + TypeScript + Java in [`lancedb/lancedb`](https://github.com/lancedb/lancedb).

The split has been deliberate from early on: Lance is consumable as a file or table format by any compute engine (DuckDB, Pandas, Polars, PyArrow, Spark, Ray); LanceDB is the search-and-retrieval product built on top.

The official framing from the OSS FAQ: "LanceDB is the multimodal lakehouse that's built on top of Lance, and utilizes the underlying optimized storage format to build efficient disk-based indexes." See [`docs/faq/faq-oss.mdx#L15-L19`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L15-L19).

---

## 2. Current versions at a glance

From [`lancedb/Cargo.toml`](https://github.com/lancedb/lancedb/blob/main/Cargo.toml) (workspace root) and [`lancedb/.bumpversion.toml`](https://github.com/lancedb/lancedb/blob/main/.bumpversion.toml):

| Component | Version | Notes |
|-----------|---------|-------|
| LanceDB Rust crate | `0.28.0-beta.11` | shared workspace version across rust / python / nodejs. Latest [release](https://github.com/lancedb/lancedb/releases) at snapshot time was published 2026-04-29. |
| Lance Rust crates | `=7.0.0-beta.4` (pinned in LanceDB workspace); upstream at `v7.0.0-beta.7` | pulled from git tag at `https://github.com/lance-format/lance.git`. Lance ships beta tags every few days; LanceDB pins specific ones. v6.0.0-rc.3 was the last v6 release on 2026-05-04, v7.0.0-beta.1 cut 2026-05-03 - the v6 -> v7 jump corresponds to a breaking namespace / object-store API change, see [Lance PR #6647](https://github.com/lance-format/lance/pull/6647). |
| Rust edition | `2024` | rust-version `1.91.0` |
| Lance file format | `2.2+` (`stable`) | `legacy` falls back to older formats for compat |
| Apache Arrow | `58.0.0` | pinned across the workspace |
| Python | `pyarrow>=16`, `pydantic>=1.10`, `numpy>=1.24.0` | see [`python/pyproject.toml`](https://github.com/lancedb/lancedb/blob/main/python/pyproject.toml) |
| Lance namespace | `>=0.3.2` | Python dep |

Earlier tagged releases visible in `git tag --sort=-committerdate` go back to `v0.1.2-dev.17`. The 0.x line has been long-running; LanceDB has not cut a 1.0 yet at the time of this snapshot.

---

## 3. Lance file format: 1.x -> 2.0 -> 2.1 -> 2.2

The Lance file format has evolved through three numbered revisions visible in user-facing config:

| Format | Notes | When |
|--------|-------|------|
| **1.x ("legacy")** | Original Arrow-style row-group columnar layout. Still selectable via `new_table_data_storage_version: legacy` for back-compat. |
| **2.0** | Arrow-style top-level structural encoding. Used through 2024. Reference baseline in the Lance 2.1 paper ([`lance-research/file_2_1/`](https://github.com/lancedb/lance-research/tree/main/file_2_1)). |
| **2.1** | "Adaptive structural encodings": new top-level structural encoding scheme with much better random-access performance on NVMe (paper title: "Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings"). Also brings struct packing, better nested-data random access. Reproducibility artifacts at [`lance-research/file_2_1/README.md`](https://github.com/lancedb/lance-research/blob/main/file_2_1/README.md). |
| **2.2** | Current default for new tables when `new_table_data_storage_version: stable`. Adds Blob v2 (separate-file blob columns, `take_blobs` lazy stream, external URI support) and Map type. |

The format choice is a connection-level option set at table creation - **not** retroactive. From [`docs/storage/configuration.mdx#L139-L143`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L139-L143):

> | `new_table_data_storage_version` | `legacy`, `stable` | `stable` | Lance file format version for new tables. Use `legacy` for backward compatibility with older clients, or `stable` for the current format with better performance. |

The previous `data_storage_version` parameter on `create_table()` was deprecated in favor of `new_table_data_storage_version` in `storage_options`. Same doc, [`#L183-L187`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L183-L187).

**Why the rewrite.** The 2.1 paper explicitly motivates the shift: Parquet (and Arrow-style approaches) were never designed for the random-access patterns of vector search, point queries, and shuffled training. The paper measured up to 1000x random-access speedup over Parquet on NVMe ([`docs/faq/faq-oss.mdx#L21-L23`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L21-L23)):

> [Our benchmarks](https://lancedb.com/blog/benchmarking-random-access-in-lance/) show that Lance is up to 1000x faster than Parquet for random access, which we believe justifies our decision to create a new data format for AI.

---

## 4. Manifest paths v1 vs v2

The dataset manifest is the per-version metadata file that points at fragments. The naming scheme has two generations:

- **v1** (default): older naming
- **v2**: opt-in via `new_table_enable_v2_manifest_paths: true`. **Requires LanceDB >= 0.10.0 to read.**

Source: [`docs/storage/configuration.mdx#L142-L143`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L142-L143).

Like the format flag, this is a per-table creation-time setting; existing tables aren't migrated automatically.

---

## 5. Stable row IDs

Originally, row IDs in Lance were positional and could change when a fragment was compacted or rows were deleted. This was fine for inserts but uncomfortable for systems wanting persistent references back to specific rows.

`new_table_enable_stable_row_ids: true` (default `false`) keeps row IDs stable across compaction, delete, and merge operations ([`docs/storage/configuration.mdx#L144-L145`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L144-L145)).

The hybrid-search docs note that `_rowid` is useful for joining or deduping across multiple sub-queries ([`docs/search/hybrid-search.mdx#L246-L271`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx#L246-L271)). Stable row IDs make those joins durable across mutation.

---

## 6. Blob API

Two generations:

**Blob v1** - large-binary support via metadata flags on a `LargeBinary` Arrow column. The bytes were stored alongside the rest of the columnar payload. Workable, but every read pulled the column data into memory.

**Blob v2** - separate-file blob columns. Marked with metadata `{"lance-encoding:blob": "true"}` on a `pa.large_binary()` / `LargeBinary` / `DataType::LargeBinary` field. Lance stores those columns in **separate files within the dataset** and exposes them via `take_blobs()`, returning a `BlobFile` handle that supports `seek()` and `read()` for random-access lazy loading. Available with file format 2.2 (`stable`).

API reference at [`docs/tables/multimodal.mdx#L152-L195`](https://github.com/lancedb/docs/blob/main/docs/tables/multimodal.mdx#L152-L195). Working production usage in [`chat-with-videos`](https://github.com/lancedb/chat-with-videos), where 100MB+ video files stream via HTTP Range requests with ~1MB peak memory per stream:

> Video files can be large (100MB+), but the application never loads an entire video into memory. This is achieved through Lance's blob encoding, which enables random access reads directly from S3 or disk.
>
> A 114MB video serving 1MB chunks uses roughly 1MB of memory, not 114MB.

(Source: [`chat-with-videos/README.md#L80-L86`](https://github.com/lancedb/chat-with-videos/blob/main/README.md#L80-L86).)

The deeper Blob v2 mechanics (random access internals, file-like reading, external URI support) live in the Lance project docs at <https://lance.org/guide/blob/>.

---

## 7. Sync to Async (Python)

LanceDB Python originally exposed a sync-only API built on `pylance` (the pre-async Lance Python wrapper). The async API arrived as a port built directly on the Rust `lancedb` crate, intended to keep the language bindings in sync.

From [`python/ASYNC_MIGRATION.md`](https://github.com/lancedb/lancedb/blob/main/python/ASYNC_MIGRATION.md):

> A new asynchronous API has been added to LanceDb. This API is built on top of the rust lancedb crate (instead of being built on top of pylance). This will help keep the various language bindings in sync.

Concrete changes:

- Almost all functions are now async (`asyncio` coroutines; require `await`).
- `Connection` and `Table` gained `close()` methods and can be used as context managers.
- `Table.schema` was a property; it is now an **async method**.
- `Table.__len__` was removed; `len(table)` no longer works. Use `await table.count_rows()` instead.

The sync API has been preserved as `Table` / `LanceTable` (and `RemoteTable`) abstract base + concrete classes; the sync methods delegate to the corresponding `AsyncTable` methods through a shared event loop (`LOOP.run()`). See the contributor guide at [`AGENTS.md#L60-L80`](https://github.com/lancedb/lancedb/blob/main/AGENTS.md#L60-L80) for the dual-track shape.

The Rust SDK is async-only (`tokio`); TypeScript is async-only (Promise-based). Java is sync (no event loop).

---

## 8. Concurrency

Concurrent writes have always been a moving target on object stores. The current state is documented in [`docs/storage/configuration.mdx#L223-L255`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L223-L255), but the path getting there:

| Era | Behaviour |
|-----|-----------|
| Early OSS | Single-writer assumption on object stores. Local-FS only had a commit lock. |
| Adding S3 | OCC over manifest versions, but S3 lacks atomic conditional writes -> two writers could both think they won. Documented as "use only one writer per dataset on S3". |
| **DynamoDB commit store** | The `s3+ddb://` URI scheme: DynamoDB acts as the atomic-commit coordinator. Hash key `base_uri`, range key `version`. Small provisioned throughput suffices. IAM: `dynamodb:GetItem`, `PutItem`, `DescribeTable` on the commit table. |
| **S3 Express One Zone** | S3 Express has atomic writes natively; plain `s3://...` works without DynamoDB if you point at an S3 Express endpoint. |
| **AWS S3 conditional writes (Aug 2024)** | AWS shipped [S3 conditional writes](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/). Lance integrated support, and as of mid-2025 plain `s3://` supports safe concurrent writes without DynamoDB. From maintainer comment on [lancedb/lancedb#2002](https://github.com/lancedb/lancedb/issues/2002) (2025-07-22): "S3 and S3 express now work well out-of-the-box." The storage configuration docs page still leads with the `s3+ddb://` recipe at the time of this snapshot - see the doc-vs-reality note in [`LANCEDB.md` Section 16](LANCEDB.md#16-concurrency-conflicts-commit-stores). |
| **GCS / Azure Blob** | Native atomic conditional writes. No commit store needed. |

The OSS FAQ frames the trade-off honestly ([`docs/faq/faq-oss.mdx#L81-L84`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L81-L84)):

> For writes, we support concurrent writing, though too many concurrent writers can lead to failing writes as there is a limited number of times a writer retries a commit.

In other words, the OCC retry budget has a ceiling; high-concurrency workloads should batch.

### Operational lessons from production users

A 2026-03 issue ([lancedb/lancedb#3086](https://github.com/lancedb/lancedb/issues/3086)) from a multi-tenant ECS Fargate user surfaced two production lessons confirmed by maintainer (`wjones127`):

- **Don't run `optimize()` after every write.** "Avoid running optimize on every write, and instead do it either every N writes or on a certain interval. That will avoid some write amplification due to compaction."
- **`cleanup_older_than` must exceed your longest write latency.** "Usually we set it to 1 or two weeks." A short threshold (e.g., `timedelta(seconds=0)`) can delete manifests still being read by an in-flight commit, leaving the DynamoDB commit store referencing a version whose manifest no longer exists on S3 - which manifests as a permanent "Too many concurrent writers" failure on subsequent operations. The underlying issue is [lance-format/lance#3718](https://github.com/lance-format/lance/issues/3718).
- **Append vs compaction write amplification.** Same maintainer thread: "Generally, writes shouldn't duplicate the whole database. If you append a row, you should only need to write that row in a file, and then a new manifest. We don't duplicate the whole database with each write. Compaction (which runs during optimize) can cause duplication, which is why I suggest running it less often if you want to see less write amplification."

---

## 9. Indexes and quantization

Vector index types have grown over time:

- **IVF_FLAT** - earliest. IVF partitions only.
- **IVF_PQ** - default disk-based vector index. Product Quantization on subvectors.
- **IVF_SQ** - Scalar Quantization. **TypeScript still does not expose IvfSq** (see [`docs/indexing/index.mdx#L37-L40`](https://github.com/lancedb/docs/blob/main/docs/indexing/index.mdx#L37-L40)).
- **IVF_HNSW_FLAT / IVF_HNSW_SQ / IVF_HNSW_PQ** - HNSW graphs *within* IVF partitions. Adds HNSW recall to IVF's partitioning scalability. The HNSW description in [`docs/indexing/index.mdx#L95-L153`](https://github.com/lancedb/docs/blob/main/docs/indexing/index.mdx#L95-L153) is one of the more thorough explanations in the docs.
- **IVF_RQ** - RaBitQ quantization. 1 bit/dim by default; 2/4/8 also configurable. Vector dimension must be divisible by 8. Most aggressive compression.

Scalar indexes:

- **BTREE** - sorted-block index, 4096 rows per block. The general default for high-cardinality scalar columns.
- **BITMAP** - one bitmap per distinct value. For columns with few thousand or fewer distinct values.
- **LABEL_LIST** - for `List<T>` columns: array containment via `array_contains_any` / `array_contains_all`. Underlying structure is a bitmap.

FTS evolved from a basic BM25 index toward a configurable tokenizer pipeline: stemming, stop-word removal, ASCII folding, case folding, custom stop words, phrase queries via `with_position`, multiple tokenizer types (`simple`, `whitespace`, `raw`, `ngram`), language-specific stemmers. See [`docs/indexing/fts-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/fts-index.mdx).

Auto-index in the Rust API ([`rust/lancedb/src/lib.rs#L119-L143`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L119-L143)):

> If a column has a data type of `FixedSizeList<Float16/Float32>`, LanceDB will create a `IVF-PQ` vector index with default parameters. Otherwise, it creates a `BTree` index by default.

Enterprise additionally builds and maintains indexes automatically (`async` build with `wait_timeout`); OSS still requires manual `create_index`.

---

## 10. Search API evolution

The query builder API has accumulated rather than been rewritten. The current shape (single `.search(...)` entry point with `query_type=` choosing vector / fts / hybrid / multivector) replaced separate per-mode methods, but those still resolve through the same builder.

Notable additions:

- `query_type="hybrid"` - vector + FTS in one call, default RRF reranker. See [`docs/search/hybrid-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx).
- `query_type="multivector"` - late-interaction (ColBERT, ColPaLi). Cosine only.
- `.distance_range(lower, upper)` - half-open distance band on the vector half ([`docs/search/hybrid-search.mdx#L271-L298`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx#L271-L298)).
- `.with_row_id(True)` - return `_rowid` for joining/deduping.
- `.rerank(reranker)` - pluggable reranker step that wraps an `eval()` method.
- `.where(predicate, prefilter=True/False)` - prefilter is the default; postfilter via `prefilter=False` (Python) or `.postfilter()` chain (TypeScript).
- `explain_plan(verbose=True)` and `analyze_plan()` - introspection, with runtime metrics like `_elapsed_compute_`, `_output_rows_`, `_bytes_read_`, `_index_comparisons_`, `_iops_`. See [`docs/search/optimize-queries.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/optimize-queries.mdx).

The Reranker API surface grew from RRF-only into a pluggable system (RRF, LinearCombination, MRR, Cohere, CrossEncoder, ColBERT, Jina, Voyage, OpenAI, AnswerDotAI, plus a custom-reranker hook). See the integrations index at [`docs/integrations/reranking/`](https://github.com/lancedb/docs/tree/main/docs/integrations/reranking).

The embedding registry was a later addition to the SDK: `get_registry().get(provider).create(...)` returning an embedding function that can be attached to a Pydantic `LanceModel` schema via `SourceField()` / `VectorField()`. Not all SDKs auto-embed at query time; the **Rust SDK does not** (callers compute query vectors explicitly).

---

## 11. Cloud and Enterprise

The OSS-only era covered roughly the 0.1 - 0.6 line. **LanceDB Cloud** entered public beta with `db://` URIs and an API key + region (and optional `host_override`) auth model. The README banner ([`README.md#L1-L3`](https://github.com/lancedb/lancedb/blob/main/README.md#L1-L3)) currently advertises the public beta.

**LanceDB Enterprise** is the production deployment: the same `db://` URI plus a private-cloud cluster runtime. Enterprise differentiators that did not exist in OSS:

- Automatic indexing (no manual `create_index` calls; async build).
- Federated namespaces with per-request credential vending (multi-tenant story).
- REST API with OpenAPI spec ([`docs/api-reference/rest/openapi.yml`](https://github.com/lancedb/docs/blob/main/docs/api-reference/rest/openapi.yml)).
- Geneva (managed UDF runtime over Lance tables; not present in OSS).
- Deployment-configured consistency (`weak_read_consistency_interval_seconds`) instead of per-connection setting.
- mTLS, RBAC, KMS-managed encryption.

The split is strict in the docs: any `<Badge color="red">Enterprise</Badge>` annotation marks a non-OSS feature. From [`docs/storage/configuration.mdx#L40-L44`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L40-L44):

> In LanceDB Enterprise, you connect with `db://...` and the cluster owns the storage credentials, so `storage_options` are not passed at runtime. Cloud auth is set at deployment time. For federated databases, the namespace service vends per-request credentials automatically.

The **Geneva** feature engineering platform ([`docs/geneva/`](https://github.com/lancedb/docs/tree/main/docs/geneva)) is the largest Enterprise-only addition by surface area: a UDF runtime, scalar/batch UDFs, built-in providers (OpenAI, Gemini, sentence-transformers), backfill jobs, materialised views, Ray / KubeRay execution contexts, Helm-based Kubernetes deployment.

---

## 12. Lance moves to its own GitHub org

A recent and structurally significant change: the **Lance** crates have moved out from `github.com/lancedb/lance` to **`github.com/lance-format/lance`**, and that org has expanded to host an entire format-side ecosystem distinct from the LanceDB database product. Visible in the workspace pin in LanceDB's [`Cargo.toml`](https://github.com/lancedb/lancedb/blob/main/Cargo.toml):

```toml
lance = { version = "=7.0.0-beta.4", default-features = false, tag = "v7.0.0-beta.4", git = "https://github.com/lance-format/lance.git" }
```

All the `lance-*` sub-crates (`lance-core`, `lance-file`, `lance-io`, `lance-index`, `lance-linalg`, `lance-namespace`, `lance-table`, `lance-encoding`, `lance-arrow`, `lance-datafusion`, etc.) are pinned at the same `=7.0.0-beta.4` tag from that org.

### The lance-format org as an ecosystem

By the snapshot date the [`lance-format`](https://github.com/lance-format) org hosts 9 repos, each a distinct project rather than a sub-component of LanceDB:

- [`lance-format/lance`](https://github.com/lance-format/lance) - the file format, table format, catalog spec, query engine, and language bindings
- [`lance-format/lance-namespace`](https://github.com/lance-format/lance-namespace) - the catalog-abstraction spec and SDKs (Python, Java; Rust impl in `lance`)
- [`lance-format/lance-duckdb`](https://github.com/lance-format/lance-duckdb) - the DuckDB extension
- [`lance-format/lance-ray`](https://github.com/lance-format/lance-ray) - Ray Data integration
- [`lance-format/lance-graph`](https://github.com/lance-format/lance-graph) - Cypher + SQL graph engine over Lance
- [`lance-format/lance-context`](https://github.com/lance-format/lance-context) - versioned multimodal agent-memory store, the Lance team's own answer to long-running agent memory
- [`lance-format/pglance`](https://github.com/lance-format/pglance) - PostgreSQL FDW extension (read-only)
- [`lance-format/lance-data-viewer`](https://github.com/lance-format/lance-data-viewer) - browser inspector with multi-version Docker images
- [`lance-format/lance-python-doc`](https://github.com/lance-format/lance-python-doc) - pylance docs publishing automation

This is more than a repo rename. The pattern is **one format / many consumers**: Lance the format and engine, plus integrations into Postgres / DuckDB / Ray / a graph layer / a viewer / an agent-memory primitive - all built on the same columnar storage. LanceDB is the largest consumer but no longer the only first-class one. The org split mirrors this: format-side projects under `lance-format`, database product (and its demos / plugins) under `lancedb`.

Per-repo status as of the snapshot:

| Repo | Version pin | Stability | Lance pinned |
|------|------------|-----------|--------------|
| `lance` | beta - upstream `7.0.0-beta.7` | active development; releases every few days | n/a (this is Lance) |
| `lance-namespace` | `0.7.6` | spec stabilising | uses Lance via SDK |
| `lance-duckdb` | unversioned (ships with DuckDB extensions) | usable; pins older Lance | `4.0.1` |
| `lance-ray` | `0.4.0-beta.1` | alpha | `pylance>=6.0.0rc3` |
| `lance-graph` | unreleased; workspace still shifting (see [issue #92](https://github.com/lance-format/lance-graph/issues/92)) | preview | recent |
| `lance-context` | unreleased | preview | recent |
| `pglance` | `0.0.0` | development | `1.0` (older) |
| `lance-data-viewer` | `0.2.0` | usable; ships images for 6 historical LanceDB versions back to `0.3.1` | per-image |
| `lance-python-doc` | n/a (tooling only) | passive | n/a |

Two consequences worth flagging:

1. **Lance version skew across consumers is real.** `lance-duckdb` pins `lance 4.0.1`, `pglance` pins `1.0`, `lance-ray` tracks `pylance 6.0.0rc3+`, and LanceDB pins `=7.0.0-beta.4`. Different format consumers see different feature sets at any given time. For format-feature checks, the upstream `lance` release tags are authoritative.

2. **The community signal is "Lance as a substrate."** The format is being treated as a stable enough substrate for an ecosystem - graph engine, FDW, embeddable extensions, an agent-memory primitive - rather than an internal implementation detail of one database. This is the strongest evidence in the codebase for "Lance the format vs LanceDB the database" as a meaningful distinction.

Older LanceDB documentation still links to `github.com/lancedb/lance` in places ([`README.md#L23`](https://github.com/lancedb/lancedb/blob/main/README.md#L23), [`docs/faq/faq-oss.mdx#L17`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L17), [`rust/lancedb/src/lib.rs#L5`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L5)) - those redirects appear to still resolve, but the canonical home is now `lance-format/lance`. The LanceDB OSS repo remains under `lancedb/`.

---

## 13. Deprecations and renames

A running list of API changes worth being aware of:

- `data_storage_version` (parameter on `create_table()`) -> `new_table_data_storage_version` (in `storage_options`). See [`docs/storage/configuration.mdx#L183-L187`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L183-L187).
- `table.storage_options()` (sync method on `AsyncTable`) -> deprecated; use `await table.initial_storage_options()` and `await table.latest_storage_options()`. See [`docs/storage/configuration.mdx#L107-L111`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L107-L111).
- `Table.schema` (property) -> async method. See [`python/ASYNC_MIGRATION.md#L36`](https://github.com/lancedb/lancedb/blob/main/python/ASYNC_MIGRATION.md#L36).
- `Table.__len__` -> removed; use `table.count_rows()` (async) or `len()`-equivalent helpers. See same migration doc.
- Integrations docs redirects in [`docs/docs.json`](https://github.com/lancedb/docs/blob/main/docs/docs.json):
  - `/integrations/platforms/:slug*` -> `/integrations/data/:slug*`
  - `/integrations/frameworks/:slug*` -> `/integrations/ai/:slug*`
  - `/integrations/data/phidata` -> `/integrations/ai/agno` (PhiData renamed to Agno)
  - `/tutorials/rag/:slug*` -> `/tutorials/agents/:slug*`
  - `/tutorials/vector-search/:slug*` -> `/tutorials/search/:slug*`
  - `/geneva/udfs/built-in` -> `/geneva/udfs/providers`
  - `/enterprise/performance` -> `/enterprise/benchmarks`

The Mintlify config keeps these as redirects so old links keep resolving.

---

## 14. Decisions and signals visible only in GitHub conversations

These are the threads that document direction and tradeoffs the docs and code don't spell out. Useful for understanding *why* something is the way it is, or where it's headed.

### Multi-language SDK release discipline

The pace at which Lance changes propagate into LanceDB is documented inline in [lancedb/lancedb#2002](https://github.com/lancedb/lancedb/issues/2002): "Once it's implemented in Lance, it will be available in LanceDB within a week." The sequence is Lance lands -> LanceDB pins the new tag in [`Cargo.toml`](https://github.com/lancedb/lancedb/blob/main/Cargo.toml) -> next LanceDB release. This is why the LanceDB version moves slowly (0.x with frequent betas) while Lance ticks much faster (sub-week beta cadence).

### Lance v6 -> v7 boundary

Visible in [lance-format/lance releases](https://github.com/lance-format/lance/releases): v6.0.0-rc.3 cut 2026-05-04, v7.0.0-beta.1 cut 2026-05-03 (one day apart). The breaking change driving v7 is "make dataset object store access base-aware" ([Lance PR #6647](https://github.com/lance-format/lance/pull/6647)) - the object-store binding API is no longer flat; it's scoped to a per-dataset "base", which feeds the federated-namespace credential-vending model.

### Distributed primitives under construction

Lance v6/v7 has been adding the building blocks for **external systems to drive Lance commits** without holding the dataset lock for the whole operation. Visible across the v6.0.0-rc release notes:

- **Two-phase commits** for distributed writes - documented at <https://lance.org/guide/distributed_write/>.
- **Distributed index build**: segmented inverted (FTS) index ([PR #6305](https://github.com/lance-format/lance/pull/6305)), distributed bitmap index build ([PR #6598](https://github.com/lance-format/lance/pull/6598)), segmented btree indices ([PR #6605](https://github.com/lance-format/lance/pull/6605)), and FTS exec internals exposed for distributed planning ([PR #6648](https://github.com/lance-format/lance/pull/6648)).
- **ANN proto codecs** ([PR #6503](https://github.com/lance-format/lance/pull/6503), [PR #6612](https://github.com/lance-format/lance/pull/6612), [PR #6613](https://github.com/lance-format/lance/pull/6613)) - vector index plans serialised over the wire.
- **Two-phase delete and two-phase vector-index commits** are pending public API (see [Lance #6658](https://github.com/lance-format/lance/issues/6658) and [#6666](https://github.com/lance-format/lance/issues/6666)).
- **Multi-dataset atomic commit primitive** is an open community ask ([Lance #6668](https://github.com/lance-format/lance/issues/6668)) - currently no public surface to atomically commit transactions across multiple Lance datasets. Workaround is "single coordinating dataset whose atomic commit serves as the linearization point for everything else".

### MemWAL (write-ahead log)

A new write path landed in v7 betas: write-ahead log appender + tailer primitives ([Lance PR #6669](https://github.com/lance-format/lance/pull/6669)) and unified `WalAppender` -> `ShardWriter` via `enable_memtable` ([PR #6675](https://github.com/lance-format/lance/pull/6675)). Performance: HNSW for the in-memory vector index ([PR #6701](https://github.com/lance-format/lance/pull/6701)).

A v7 production bug at the time of this snapshot ([Lance #6713](https://github.com/lance-format/lance/issues/6713)) - "memtable_flusher panics on first 1M-row threshold flush" with `merge_insert` upserts via `ShardWriter` - flags that the MemWAL path is still settling.

### Stable row IDs: cold-cache cost is significant

westonpace's measurements on [Lance #6707](https://github.com/lance-format/lance/issues/6707) (2026-05-07) on the take path with 1000 fragments, no updates:

| Cache state | `stable_off` | `stable_on` | Gap |
|-------------|--------------|-------------|-----|
| Cold (re-open per call) | 0.20 ms | 312.72 ms | **1565x** |
| Warm (one Dataset instance) | 0.158 ms | 0.164 ms | 6 us |

Build cost is super-linear in fragment count: ~50us/fragment at N=100, ~325us/fragment at N=1000 (76x cost for 10x fragments). Likely culprits: per-fragment proto-decode of inline `RowIdMeta`, `RangeInclusiveMap` insert behavior at large N. The `RowIdIndex` is memoized per `manifest.version` in `Dataset.metadata_cache`, so cost is paid once per Dataset instance per manifest version. **Practical implication:** stable row IDs in long-lived processes are essentially free; in per-request-open serverless patterns with many fragments, cold-cache build time is significant. Plan accordingly.

### Tantivy FTS removal

The legacy Tantivy-based FTS implementation was removed in v0.28.0-beta.10 (2026-04-28) by [PR #3282](https://github.com/lancedb/lancedb/pull/3282). Indexes built with the old Tantivy path no longer work; rebuild with the current FTS index. The current path uses a Lance-native tokenizer stack vendored into the Lance crate via [Lance PR #6512](https://github.com/lance-format/lance/pull/6512).

### Geo indexing work

[Lance #4632](https://github.com/lance-format/lance/pull/4632) (open since 2025-09): BKD-tree-based geographic index supporting `st_within` interception. Adds spatial-query capabilities. Storage layout: `bkd_tree_inner.lance` for internal-node metadata, plus leaf segments. Active 27-comment review thread.

### Namespace and namespace-server spec evolution

`lance-namespace` is on its own version line (`>=0.3.2` in LanceDB Python pyproject; `0.7.2` referenced in Lance v6.0.0-rc.3 [PR #6608](https://github.com/lance-format/lance/pull/6608)). The spec lives at [`docs/lance-namespace/`](https://github.com/lancedb/docs/tree/main/lance-namespace) and underwent a "manifest-enabled directory namespace mode" addition in [LanceDB PR #3332](https://github.com/lancedb/lancedb/pull/3332) (v0.28.0-beta.11). The model is converging on namespace-as-credential-vending-service, mirroring federated multi-tenant deployments.

### Python free-threading on the radar

[Lance #6690](https://github.com/lance-format/lance/issues/6690) (2026-05-05): user request for Python 3.14 free-threaded mode (`cp314t` wheel). Currently `pylance` only ships abi3 wheels. Tracking but not yet committed.

### Community CLI tooling

[Lance #6702](https://github.com/lance-format/lance/issues/6702): community-maintained Lance CLI [`arrs`](https://github.com/jonasdedden/arrs) for inspecting datasets. Published on crates.io and PyPI. Not part of the official toolchain but referenced in user discussions.

### Maintainers' guidance on operational shape

The wjones127 reply on [lancedb/lancedb#3086](https://github.com/lancedb/lancedb/issues/3086) (2026-03) is the most direct source for "how to run LanceDB on S3 in production":

- `cleanup_older_than` >= 1-2 weeks for production. Anything shorter risks stuck-state on S3+DDB.
- Don't `optimize()` per write. Either every N writes or on a schedule.
- Append writes only write the new fragment + new manifest; they do not duplicate the whole table. Compaction is what duplicates (temporarily) - that's why running it less often reduces write amplification.

These are not in the docs; they live in issue threads.

---

## 15. Areas under active iteration

Things explicitly marked unstable / iterating in the source as of the snapshot:

- **`lancedb-claw`** - both the `memory-lancedb-claw` plugin and the `lancedb-claw` context engine plugin are tagged "currently under rapid iteration. The package is still under active development, and the implementation should be treated as an evolving prototype rather than a stable integration target. Expect frequent changes to code structure, configuration, internal APIs, and install flow." See [`memory/README.md#L11-L17`](https://github.com/lancedb/lancedb-claw/blob/main/memory/README.md#L11-L17) and [`lancedb-claw/README.md`](https://github.com/lancedb/lancedb-claw/blob/main/README.md).

- **Multivector search** - cosine-only at present ([`docs/search/multivector-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/multivector-search.mdx)).

- **Stable row IDs** - opt-in flag (`new_table_enable_stable_row_ids`); not the default yet.

- **v2 manifest paths** - opt-in flag (`new_table_enable_v2_manifest_paths`); needs LanceDB >= 0.10.0 readers. Not the default.

- **Geneva** - several pages under [`docs/geneva/`](https://github.com/lancedb/docs/tree/main/docs/geneva) are recent additions; the API and execution-context surface (Ray, KubeRay) is still expanding.

- **TypeScript IVF_SQ** - "TypeScript currently doesn't support `IvfSq`" ([`docs/indexing/index.mdx#L38-L40`](https://github.com/lancedb/docs/blob/main/docs/indexing/index.mdx#L38-L40)).

- **`apache-arrow` peer-dep** - the `@lancedb/lancedb` Node package does not bundle `apache-arrow`; the OpenClaw demo's setup notes call it out as a manual install required for runtime ([`openclaw-lancedb-demo/README.md#L117-L125`](https://github.com/lancedb/openclaw-lancedb-demo/blob/main/README.md#L117-L125)). On macOS it's also reported that "upstream package may not ship darwin native bindings" - see the runtime-loader fallback at [`memory-lancedb-claw/index.ts#L49-L60`](https://github.com/lancedb/lancedb-claw/blob/main/memory/index.ts#L49-L60).

- **0.x version line** - LanceDB has not cut a 1.0; the most recent visible version is `0.28.0-beta.11`. Lance the format crates are at `7.x-beta`. Both are widely adopted in production despite the version markers.
