# LanceDB

Single-file unopinionated reference of what LanceDB is, what it can do, and what it can't (yet). Every capability claim links to the upstream code or docs on GitHub `main` so it can be traced. Snapshot date: 2026-05-08.

For evolution and historical context (file format generations, deprecations, API renames), see [`EVOLUTION.md`](EVOLUTION.md).

---

## Table of contents

1. [What LanceDB is](#1-what-lancedb-is)
2. [Distribution surface (OSS, Cloud, Enterprise)](#2-distribution-surface)
3. [SDKs and language bindings](#3-sdks-and-language-bindings)
4. [Connections and storage backends](#4-connections-and-storage-backends)
5. [Tables: create, ingest, mutate](#5-tables-create-ingest-mutate)
6. [Schema evolution](#6-schema-evolution)
7. [Versioning, time travel, tags](#7-versioning-time-travel-tags)
8. [Consistency model](#8-consistency-model)
9. [Multimodal data and the Blob API](#9-multimodal-data-and-the-blob-api)
10. [Indexing](#10-indexing)
11. [Search](#11-search)
12. [Filtering](#12-filtering)
13. [Reranking](#13-reranking)
14. [Embeddings](#14-embeddings)
15. [Namespaces](#15-namespaces)
16. [Concurrency, conflicts, commit stores](#16-concurrency-conflicts-commit-stores)
17. [Performance and tuning](#17-performance-and-tuning)
18. [Geneva (feature engineering)](#18-geneva-feature-engineering)
19. [Enterprise platform](#19-enterprise-platform)
20. [Integrations](#20-integrations)
21. [REST API](#21-rest-api)
22. [Capability matrix: what it can and can't](#22-capability-matrix)
23. [Ecosystem repositories](#23-ecosystem-repositories)
24. [Direct file index](#24-direct-file-index)

---

## 1. What LanceDB is

LanceDB is an embedded multimodal database that bundles vector kNN, full-text search (BM25), scalar indexes, and columnar analytics over a single open-source file format called [Lance](https://github.com/lancedb/lance). It runs in-process (like SQLite) for OSS and as a managed remote service for Cloud / Enterprise. The same SDK calls work against both backends: only the connect string changes.

The official short description from the Rust crate root:

> LanceDB is an open-source database for vector-search built with persistent storage, which greatly simplifies retrieval, filtering and management of embeddings.

Source: [`rust/lancedb/src/lib.rs#L1-L16`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L1-L16)

The README extends that: vector + FTS + SQL search, multimodal storage (text/images/video/point clouds), zero-copy schema evolution, automatic versioning, and "no servers to manage" as the marketing line. See [`README.md#L42-L57`](https://github.com/lancedb/lancedb/blob/main/README.md#L42-L57).

The split between **Lance** and **LanceDB** is load-bearing. Lance is the file format and table format; LanceDB is the database that wraps Lance with disk-based indexes, a query API, embedding registry, and a remote client. From the FAQ: "Lance is a modern lakehouse format... LanceDB is the multimodal lakehouse that's built on top of Lance, and utilizes the underlying optimized storage format to build efficient disk-based indexes." See [`docs/faq/faq-oss.mdx#L15-L19`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L15-L19).

A separate research paper covers Lance 2.1's structural encodings and random-access performance vs Parquet: [`lance-research/file_2_1/`](https://github.com/lancedb/lance-research/tree/main/file_2_1).

---

## 2. Distribution surface

Three deployments share one SDK shape:

| Tier | URI scheme | What you run | Auth |
|------|-----------|--------------|------|
| **OSS / embedded** | local path, `s3://`, `gs://`, `az://`, `s3+ddb://` | the SDK, in-process | cloud creds for the bucket |
| **LanceDB Cloud** (public beta) | `db://...` | nothing; managed | API key + region |
| **LanceDB Enterprise** | `db://...` with `host_override` | private-cloud cluster | API key + region + host override |

The Rust SDK lists the URI forms it accepts: see [`rust/lancedb/src/lib.rs#L46-L52`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L46-L52).

Cloud and Enterprise expose **automatic indexing** (no manual `create_index` calls), **federated namespaces** with credential vending, and a **REST API** with OpenAPI spec. The OSS path ships every storage and search primitive but leaves index lifecycle and namespace identity to the caller.

The LanceDB Cloud public-beta link sits at the top of the README: [`README.md#L1-L3`](https://github.com/lancedb/lancedb/blob/main/README.md#L1-L3). Enterprise architecture, security model, and benchmarks live under [`docs/enterprise/`](https://github.com/lancedb/docs/tree/main/docs/enterprise) (`architecture.mdx`, `security.mdx`, `benchmarks.mdx`).

---

## 3. SDKs and language bindings

Repository layout per [`AGENTS.md#L7-L12`](https://github.com/lancedb/lancedb/blob/main/AGENTS.md#L7-L12):

| Language | Path | Bindings tech | Async support |
|----------|------|---------------|---------------|
| **Rust** | [`rust/lancedb/`](https://github.com/lancedb/lancedb/tree/main/rust/lancedb) | native | async only (`tokio`) |
| **Python** | [`python/`](https://github.com/lancedb/lancedb/tree/main/python) | PyO3 | sync wraps async via a shared event loop; see [`python/ASYNC_MIGRATION.md`](https://github.com/lancedb/lancedb/blob/main/python/ASYNC_MIGRATION.md) |
| **TypeScript / Node.js** | [`nodejs/`](https://github.com/lancedb/lancedb/tree/main/nodejs) | napi-rs | async (Promise-based) |
| **Java** | [`java/`](https://github.com/lancedb/lancedb/tree/main/java) | JNI | sync |

Python has both sync and async classes (`Table` + `LanceTable` vs `AsyncTable`); the sync variants delegate to the async ones via `LOOP.run()`. TypeScript is async-only. The Rust API is async-only and uses builder patterns (`db.create_table(...).execute().await`).

Method-parity discipline is documented: any new method on `Table` must be added in Rust core, Python sync + async, TypeScript, and the abstract base. See the [`AGENTS.md#L45-L80`](https://github.com/lancedb/lancedb/blob/main/AGENTS.md#L45-L80) "adding a new method" example.

**Crate features** (Rust, listed in [`rust/lancedb/src/lib.rs#L26-L36`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L26-L36)):

- `aws` - S3 object store
- `dynamodb` - DynamoDB manifest store
- `azure` - Azure Blob
- `gcs` - Google Cloud Storage
- `oss` - Alibaba Cloud OSS
- `remote` - LanceDB Cloud client
- `huggingface` - HuggingFace Hub dataset loading
- `fp16kernels` - FP16 kernels for faster CPU vector search
- `polars` - Polars conversions

**Common dev commands** (`AGENTS.md#L14-L22`): `cargo check --features remote --tests --examples`, `cargo test`, `cargo clippy`, `cargo fmt --all`.

Top-level Rust crate modules: `arrow`, `connection`, `data`, `database`, `dataloader`, `embeddings`, `error`, `expr`, `index`, `io`, `ipc`, `query`, `remote`, `rerankers`, `table`, `utils`. See [`rust/lancedb/src/lib.rs#L165-L185`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L165-L185).

---

## 4. Connections and storage backends

A LanceDB **connection** is opened with a URI and optional `storage_options`. The URI scheme picks the backend; storage options layer credentials, timeouts, and per-table feature flags on top.

Supported URI forms (from [`rust/lancedb/src/lib.rs#L46-L52`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L46-L52)):

- `data/sample-lancedb` - local filesystem path
- `s3://bucket/path/...` - AWS S3 or S3-compatible
- `s3+ddb://bucket/path?ddbTableName=...` - S3 with DynamoDB commit store (concurrent writers)
- `gs://bucket/path/...` - Google Cloud Storage
- `az://container/path/...` - Azure Blob
- `db://dbname` - LanceDB Cloud / Enterprise

The full storage configuration surface is documented at [`docs/storage/configuration.mdx`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx).

### General object-store options

These are the cross-backend options recognised on every cloud target ([`docs/storage/configuration.mdx#L113-L128`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L113-L128)):

| Key | Purpose |
|-----|---------|
| `allow_http` | allow non-TLS connections |
| `allow_invalid_certificates` | skip TLS cert validation |
| `connect_timeout` / `timeout` | connect-phase / full-request timeouts |
| `user_agent` | UA header |
| `proxy_url` / `proxy_ca_certificate` / `proxy_excludes` | HTTP proxy support |
| `download_retry_count` | retries on object download |
| `client_max_retries` / `client_retry_timeout` | object-store client retry policy |

Keys are case-insensitive; lowercase in `storage_options`, uppercase in env vars.

### New-table configuration keys

Set at connection or per-table level; evaluated only when the table is **created** ([`docs/storage/configuration.mdx#L135-L156`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L135-L156)):

| Key | Values | Default | Effect |
|-----|--------|---------|--------|
| `new_table_data_storage_version` | `legacy`, `stable` | `stable` | Lance file format version used for new tables. `stable` = current format; `legacy` for back-compat with older readers. |
| `new_table_enable_v2_manifest_paths` | `true`, `false` | `false` | v2 manifest path naming. Requires LanceDB >= 0.10.0 to read. |
| `new_table_enable_stable_row_ids` | `true`, `false` | `false` | Keep row IDs stable across compaction / delete / merge. |

The deprecated `data_storage_version` parameter on `create_table()` is replaced by `new_table_data_storage_version` in `storage_options`. See [`docs/storage/configuration.mdx#L183-L187`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L183-L187).

### Backends

**AWS S3** ([`docs/storage/configuration.mdx#L189-L255`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L189-L255))
Env vars: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, optional `AWS_REGION`. Minimum IAM: `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket`, `s3:GetBucketLocation` on the bucket/prefix. Server-side encryption with KMS via `aws_server_side_encryption=aws:kms` + `aws_sse_kms_key_id`. **S3 Express** is supported (different networking; AWS docs apply). **MinIO and any other S3-compatible store**: set the endpoint and `allow_http=True` on `http://` endpoints.

**S3 + DynamoDB commit store**
S3 lacks atomic writes, so concurrent writers on plain S3 are unsafe. The `s3+ddb://` scheme uses a DynamoDB table for commit coordination. Hash key `base_uri` (string), range key `version` (number); `dynamodb:GetItem`, `PutItem`, `DescribeTable` on the commit table. Set `dynamodb_endpoint` for non-AWS endpoints (e.g. LocalStack). A multipart-upload-cleanup S3 lifecycle rule is recommended after crashes. See [`docs/storage/configuration.mdx#L223-L255`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L223-L255).

**Google Cloud Storage** ([`#L272-L286`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L272-L286))
Env: `GOOGLE_SERVICE_ACCOUNT` (path to JSON). GCS defaults to HTTP/1; `HTTP1_ONLY=false` to enable HTTP/2.

**Azure Blob** ([`#L287-L313`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L287-L313))
Env: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`. Also: SAS tokens (`azure_storage_sas_token`), service principal (`azure_client_id`, `azure_client_secret`, `azure_tenant_id`), managed identities, custom endpoints.

**Tigris** ([`#L315-L330`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L315-L330))
S3-compatible. `AWS_ENDPOINT=https://t3.storage.dev`, `AWS_DEFAULT_REGION=auto`.

**Alibaba OSS** - feature-flagged Rust crate (`oss`); no detailed docs page.

**Tigris is the only "Tier-1" non-hyperscaler S3-compatible target with a dedicated docs section.** Other S3-compatible stores (MinIO, Cloudflare R2, etc.) work via the generic S3 path with custom endpoint.

### Inspecting effective options

`AsyncTable` exposes `await table.initial_storage_options()` (options the table was opened with) and `await table.latest_storage_options()` (current after refresh). The deprecated synchronous `table.storage_options()` will be removed. See [`#L107-L111`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L107-L111).

### Enterprise

Enterprise connects with `db://...` and the cluster owns storage credentials; `storage_options` are not passed at runtime. For federated databases, the namespace service vends per-request credentials. See [`docs/storage/configuration.mdx#L40-L44`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L40-L44) and [`docs/enterprise/quickstart.mdx`](https://github.com/lancedb/docs/blob/main/docs/enterprise/quickstart.mdx).

---

## 5. Tables: create, ingest, mutate

A LanceDB table maps 1:1 to a Lance dataset on the underlying object store. The connection holds a session that can list, open, create, drop, or rename tables.

Table operations doc index: [`docs/tables/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/index.mdx). Concrete CRUD: [`docs/tables/create.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/create.mdx), [`docs/tables/update.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/update.mdx).

### Creation

Sources accepted by `create_table`: `list[dict]`, PyArrow `Table` / `RecordBatch` / dataset, Pandas DataFrame, Polars DataFrame, Pydantic models (via `LanceModel`), iterators of any of the above.

The Rust API uses an `arrow_array::RecordBatch` and a builder. Vector columns must be `FixedSizeList<Float16/Float32>`. Example: [`rust/lancedb/src/lib.rs#L82-L117`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L82-L117).

Modes: `mode="overwrite"` (replace), `mode="append"` (default if table exists is to error in OSS; merging is via `merge_insert`).

**Bulk ingest path.** From the OSS FAQ ([`docs/faq/faq-oss.mdx#L47-L62`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L47-L62)):

> LanceDB auto-parallelizes large writes when you call `table.add()` with materialized data such as `pa.Table`, `pd.DataFrame`, or `pa.dataset()`. No extra configuration is needed - writes are automatically split into partitions of ~1M rows or 2GB.

The recommended idiom is **create empty, then add**: `db.create_table(name, schema=...)` then `table.add(data)`. Passing data directly to `create_table()` does not trigger auto-parallelism. Avoid one-row-at-a-time inserts (each insert creates a new fragment).

### Mutation surface

| Operation | Semantics |
|-----------|-----------|
| `table.add(data)` | append rows |
| `table.update(where=..., values=...)` | update matching rows in place |
| `table.merge_insert(data, on=[...])` | upsert: match by key, then `whenMatchedUpdateAll` / `whenNotMatchedInsertAll` / `whenNotMatchedBySourceDelete` |
| `table.delete(where=...)` | delete by SQL predicate |
| `table.optimize()` | compact fragments, prune old versions, refresh indexes |

The `merge_insert` API used by the OpenClaw context engine plugin shows the mergeBuilder shape: [`lancedb-claw/context/.../store/helper.ts#L470-L497`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/store/helper.ts#L470-L497) - `mergeInsert(on).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute(rows)`.

Deletion is **soft** at the fragment level: each fragment carries up to one deletion file, so data is recoverable from any retained version until a `cleanup_older_than` pass on `optimize()` removes it. See [`docs/lance.mdx#L80-L89`](https://github.com/lancedb/docs/blob/main/docs/lance.mdx#L80-L89).

### Compaction

`table.optimize()` does three things ([`docs/lance.mdx#L65-L79`](https://github.com/lancedb/docs/blob/main/docs/lance.mdx#L65-L79); see also [`cocoindex-lancedb-demo/README.md#L147-L168`](https://github.com/lancedb/cocoindex-lancedb-demo/blob/main/README.md#L147-L168)):

- **Compaction**: merge small fragments into larger ones.
- **Pruning**: remove old version manifests (default retention 7 days, controlled by `cleanup_older_than`).
- **Indexing**: re-train indexes on new data; rebuild deltas.

Disk usage can temporarily increase during compaction because new files are written before old versions are dereferenced.

---

## 6. Schema evolution

Lance treats schema changes as **metadata-only operations**: adding a column does not rewrite existing fragments; old rows naturally read NULL for the new column. From the Lance format page ([`docs/lance.mdx#L40-L50`](https://github.com/lancedb/docs/blob/main/docs/lance.mdx#L40-L50)):

> **Zero-copy** data evolution, meaning you can easily add derived columns (like features or embeddings) at a later time, **without full table rewrites**. Only new data is written; expensive existing data (like images/videos) remain untouched.

API surface ([`docs/tables/schema.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/schema.mdx)):

| Method | What it does |
|--------|--------------|
| `add_columns({"name": expr})` | Add new columns; values can be SQL expressions over existing columns, NULL, or defaults. |
| `alter_columns({"col": {"name": ..., "data_type": ..., "nullable": ...}})` | Rename, change type, change nullability. |
| `drop_columns(["col"])` | Drop columns. Metadata-only. |

All three are ACID via Lance's manifest-versioning. Concurrent readers see the old schema until they refresh.

The "Lance tables are 'two-dimensional' - they grow horizontally and vertically at zero cost" framing comes from the CocoIndex demo's overview ([`cocoindex-lancedb-demo/README.md#L20-L29`](https://github.com/lancedb/cocoindex-lancedb-demo/blob/main/README.md#L20-L29)):

> say you want to use an LLM to extract new features from one of the columns in a Lance table: you would run your pipeline, update the table schema to add a new column, and backfill it with the required values by running the transform.
> In traditional data lakes (e.g., based on Iceberg), this would require a full table rewrite, but in Lance, only the new data is being written (no table locks while writes happen).

The `hf-upload-demo` shows the same pattern in a HuggingFace-Hub workflow: [`hf-upload-demo/update_dataset.py`](https://github.com/lancedb/hf-upload-demo/blob/main/update_dataset.py).

---

## 7. Versioning, time travel, tags

Every commit to a Lance dataset (insert, update, delete, schema change, index build, optimize) creates a new manifest version. Old versions are retained until pruned. From [`docs/lance.mdx#L52-L63`](https://github.com/lancedb/docs/blob/main/docs/lance.mdx#L52-L63):

> Each version contains metadata and just the new/updated data in your transaction. So if you have 100 versions, they aren't 100 duplicates of the same data. However, they do have 100x the metadata overhead of a single version, which can result in slower queries.

API ([`docs/tables/versioning.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/versioning.mdx)):

| Call | Purpose |
|------|---------|
| `table.version` | current numeric version |
| `table.list_versions()` | enumerate every version |
| `table.checkout(v)` | open a read-only handle pinned to version `v` |
| `table.checkout_latest()` | refresh to head |
| `table.restore()` | promote the checked-out version to a new head (i.e., "rollback" via copy-forward) |
| `table.tag()` | attach a named label to a version (CRUD: create, list, update, delete) |

Tag semantics (verbatim from [`docs/tables/versioning.mdx#L191-L219`](https://github.com/lancedb/docs/blob/main/docs/tables/versioning.mdx#L191-L219)):

> Tagged versions are preserved when old versions are pruned... Deleting a tag only removes the label, not the version it points to. After deletion, the underlying table version becomes eligible for cleanup again.

A typical version sequence on a fresh table ([`#L262-L271`](https://github.com/lancedb/docs/blob/main/docs/tables/versioning.mdx#L262-L271)):

1. v1: `create_table`
2. v2: `update`
3. v3: `add`
4. v4: `restore` from v2
5. v5: `delete`

Read-only operations (`list_versions`, `checkout`, `checkout_latest`) do not create new versions. System operations like `optimize()`, index updates, and table compaction **do**.

`optimize()` defaults to pruning manifests older than 7 days. Tagged versions are exempt from cleanup ([`#L273-L283`](https://github.com/lancedb/docs/blob/main/docs/tables/versioning.mdx#L273-L283)).

Time-travel is the foundation for the Time-Travel RAG tutorial: [`docs/tutorials/agents/time-travel-rag/`](https://github.com/lancedb/docs/tree/main/docs/tutorials/agents/time-travel-rag).

---

## 8. Consistency model

LanceDB OSS connections take a `read_consistency_interval` parameter that controls how often a long-lived `Table` handle re-checks for newer manifest versions written by other processes. From [`docs/tables/consistency.mdx#L21-L31`](https://github.com/lancedb/docs/blob/main/docs/tables/consistency.mdx#L21-L31):

| Setting | Behavior |
|---------|----------|
| **Unset (default)** | no automatic cross-process refresh checks |
| **`timedelta(seconds=0)`** | check every read (strong; max staleness 0) |
| **`timedelta(seconds=N)`** | refresh after N seconds elapsed since last check |

Manual refresh: `await table.checkout_latest()` (or sync equivalent). A `Tip` from the storage doc: `await table.initial_storage_options()` returns the options the table was opened with; `await table.latest_storage_options()` returns the current options after a refresh.

In **Enterprise / Cloud (`RemoteTable`)**, consistency is deployment-configured (cluster parameter `weak_read_consistency_interval_seconds`), not an SDK setting. `checkout_latest` still works for explicit refresh.

### Bad-vector handling (Python only)

The Python SDK supports an `on_bad_vectors` parameter for ingest of vectors that are wrong-dimension, contain NaN, or are null on a non-nullable field ([`docs/tables/consistency.mdx#L100-L125`](https://github.com/lancedb/docs/blob/main/docs/tables/consistency.mdx#L100-L125)):

- default: raise
- `drop`: ignore the row
- `fill`: replace bad values with `fill_value` (e.g., `[1.0, NaN, 3.0]` -> `[1.0, 0.0, 3.0]`)
- `null`: replace the vector with NULL (column must be nullable)

---

## 9. Multimodal data and the Blob API

Two patterns for storing binary payload (images, audio, video, PDF):

### 1. Standard binary columns

For sub-megabyte blobs and tables that fit comfortably in fragments. Define an Arrow binary field (`pa.binary()` / `Binary` / `DataType::Binary`) and `table.add(data)` with raw bytes. See [`docs/tables/multimodal.mdx#L40-L114`](https://github.com/lancedb/docs/blob/main/docs/tables/multimodal.mdx#L40-L114).

### 2. Lance Blob API (large binary, lazy stream)

For large or rarely-touched payload (videos, high-res images, long documents). Use `pa.large_binary()` / `LargeBinary` / `DataType::LargeBinary` and tag the field with metadata `{"lance-encoding:blob": "true"}`. Lance then stores those columns in **separate files within the dataset** and exposes them via `take_blobs()` returning a `BlobFile` handle that supports `seek()` and `read()` on demand. See [`docs/tables/multimodal.mdx#L152-L195`](https://github.com/lancedb/docs/blob/main/docs/tables/multimodal.mdx#L152-L195).

The `chat-with-videos` repo is a working end-to-end demonstration of HTTP Range streaming over Lance blobs ([`chat-with-videos/README.md#L80-L86`](https://github.com/lancedb/chat-with-videos/blob/main/README.md#L80-L86)):

> Video files can be large (100MB+), but the application never loads an entire video into memory. This is achieved through Lance's blob encoding, which enables random access reads directly from S3 or disk.
>
> A 114MB video serving 1MB chunks uses roughly 1MB of memory, not 114MB.

Implementation reference: [`chat-with-videos/src/api/services/video_service.py`](https://github.com/lancedb/chat-with-videos/blob/main/src/api/services/video_service.py) and [`chat-with-videos/src/storage/blob_utils.py`](https://github.com/lancedb/chat-with-videos/blob/main/src/storage/blob_utils.py). Notes from the README:

> Row index mappings and blob sizes are cached in memory (both are immutable after ingest), but `BlobFile` handles are never cached - each range read gets a fresh handle to avoid stale seek/read state with S3-backed blobs.

For deeper Lance Blob v2 mechanics (random access, file-like reading, external URI support), the [Lance format docs](https://lance.org/guide/blob/) are the authority - LanceDB's docs link out to them at [`docs/tables/multimodal.mdx#L192-L195`](https://github.com/lancedb/docs/blob/main/docs/tables/multimodal.mdx#L192-L195).

The HuggingFace upload demo also stores binary image blobs in Lance and walks the create / inspect / update lifecycle: [`hf-upload-demo/create_dataset.py`](https://github.com/lancedb/hf-upload-demo/blob/main/create_dataset.py).

---

## 10. Indexing

LanceDB's three index families ([`docs/indexing/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/index.mdx)):

| Family | Use case |
|--------|----------|
| **Vector** | kNN over `FixedSizeList<Float16/Float32>` columns (or binary vectors with hamming) |
| **Full-Text Search (FTS)** | BM25 keyword search on string columns |
| **Scalar** | Filtering / sorting on numeric, temporal, string, or list columns |

All three are **disk-based** (rather than memory-resident) - this is one of the headline differences from in-memory vector DBs. See [`docs/indexing/index.mdx#L53-L62`](https://github.com/lancedb/docs/blob/main/docs/indexing/index.mdx#L53-L62).

In **Enterprise**, indexes are built and maintained automatically. In **OSS**, indexes are built explicitly via `create_index` calls.

### Vector indexes

Index types ([`docs/indexing/index.mdx#L29-L36`](https://github.com/lancedb/docs/blob/main/docs/indexing/index.mdx#L29-L36) and [`docs/indexing/vector-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/vector-index.mdx)):

| Index | Notes |
|-------|-------|
| `IVF_FLAT` | IVF partitions only, no quantization. Best recall, largest index. Hamming-only for binary vectors. |
| `IVF_PQ` | IVF + Product Quantization. Default for general use. |
| `IVF_SQ` | IVF + Scalar Quantization. Faster build, less compression than PQ. **TypeScript: not supported.** |
| `IVF_RQ` | IVF + RaBitQ (1 bit/dim default; 2/4/8 bit alternatives). Maximum compression. **Vector dimension must be divisible by 8.** |
| `IVF_HNSW_FLAT` | IVF partitions, HNSW within each. Highest recall variant. |
| `IVF_HNSW_SQ` | Best recall/latency tradeoff in benchmarks; higher variance under selective filters. |
| `IVF_HNSW_PQ` | HNSW graphs + PQ compression per partition. |

Distance metrics (fixed at index-build time) - from the Rust crate enum [`rust/lancedb/src/lib.rs#L196-L223`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L196-L223):

- **`L2`** (default) - Euclidean distance, range `[0, inf)`
- **`Cosine`** - cosine distance, range `[0, 2]`. Undefined when a vector is all zeros.
- **`Dot`** - dot product, range `(-inf, inf)`. Equivalent to cosine when vectors are normalised.
- **`Hamming`** - position-difference count for binary vectors only

The metric must match what the embedding model was trained for (cosine for most text models, dot for normalised vectors).

Tuning parameters (from [`docs/indexing/vector-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/vector-index.mdx)):

- `num_partitions` - rough rule of thumb: `rows / 4096` for IVF_PQ / IVF_RQ, `rows / 1_000_000` for HNSW.
- `ef_construction` - HNSW build-time graph quality, ~150 typical.
- `num_sub_vectors` - PQ subvector count, ~`dim / 8`.

Auto-index in Rust: `Index::Auto` picks IVF_PQ for vector columns and BTree for scalars - see [`rust/lancedb/src/lib.rs#L119-L143`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L119-L143):

> If a column has a data type of `FixedSizeList<Float16/Float32>`, LanceDB will create a `IVF-PQ` vector index with default parameters. Otherwise, it creates a `BTree` index by default.

### Quantization

| Type | Compression | Notes |
|------|-------------|-------|
| `None` / `Flat` | 1x | raw vectors, highest recall |
| `SQ` (Scalar) | ~1/4 | per-dimension quantization |
| `PQ` (Product) | ~1/64-1/16 | subvectors + codebooks; default |
| `RQ` (RaBitQ) | ~1/32 (1 bit/dim) | dim must be divisible by 8 |

See [`docs/indexing/quantization.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/quantization.mdx).

A **refine factor** (`refine_factor`) reranks the top-k PQ candidates against full-precision vectors at query time. From the FAQ ([`docs/faq/faq-oss.mdx#L65-L72`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L65-L72)):

> if you're retrieving the top 10 results and set `refine_factor` to 25, LanceDB will fetch the 250 most similar vectors (according to PQ), compute the distances again based on the full vectors for those 250 and then re-rank... it's recommended you set a `refine_factor` of anywhere between 5-50.

### Full-Text Search index

`table.create_fts_index("text_col")` or `create_index("text_col", config=Index.fts())`.

Tokenizer config ([`docs/indexing/fts-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/fts-index.mdx)):

- `base_tokenizer`: `simple`, `whitespace`, `raw`, `ngram`
- `language`: stemmer language (English, Spanish, French, German, ...)
- `lower_case`: true/false
- `stem`: enable stemming
- `remove_stop_words`: built-in or `custom_stop_words`
- `ascii_folding`: collapse diacritics
- `max_token_length`: filter base64 / long URLs out of the index
- `with_position`: enable phrase queries (larger index)
- `ngram` length and prefix-only flags

In Enterprise, FTS index builds are async and accept a `wait_timeout`.

### Scalar indexes

| Index | Best for |
|-------|----------|
| `BTREE` | many unique values; numeric / temporal / string with high cardinality. 4096 rows per block. |
| `BITMAP` | < 1k unique values; categorical filtering |
| `LABEL_LIST` | `List<T>` columns; supports `array_contains_any` / `array_contains_all` |

Build: `table.create_scalar_index("col", index_type="BTREE")`. After bulk add: `table.optimize()` to fold delta into the existing index. See [`docs/indexing/scalar-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/scalar-index.mdx).

The OpenClaw context engine plugin sets up scalar indexes on `session_key`, `session_id`, `ordinal`, `message_pk` for the messages table, and equivalent ones for summaries and state - see [`lancedb-claw/context/.../store/retrieval.ts#L37-L107`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/store/retrieval.ts#L37-L107). The same file also conditionally creates either a vector index or an FTS index on the summary text column depending on whether an embedding client is configured.

### GPU index building

CUDA-accelerated index build for large datasets. **Python only** ([`docs/indexing/gpu-indexing.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/gpu-indexing.mdx); footnote in [`rust/lancedb/src/lib.rs#L13-L16`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L13-L16)).

### Reindexing

Once an index is built, new data lands in a delta. `optimize()` folds deltas into the main index. Manual rebuild is also available. See [`docs/indexing/reindexing.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/reindexing.mdx).

---

## 11. Search

Search modes are query-builder method calls on a `Table`, all chainable.

| Mode | API entry |
|------|-----------|
| Vector / kNN | `.search(vec)` or `.search(text, query_type="vector")` (auto-embed) |
| FTS / BM25 | `.search(query, query_type="fts")` |
| Hybrid (vector + FTS) | `.search(text, query_type="hybrid")` |
| Multivector (late-interaction, e.g. ColBERT/ColPaLi) | `.search(matrix, query_type="multivector")` |
| Pure SQL filter | `.where(...).to_pandas()` (no search; just filtered scan) |

Source files:
- [`docs/search/vector-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/vector-search.mdx)
- [`docs/search/full-text-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/full-text-search.mdx)
- [`docs/search/hybrid-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx)
- [`docs/search/multivector-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/multivector-search.mdx)
- [`docs/search/optimize-queries.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/optimize-queries.mdx)

### Vector search

Returns rows ordered by `_distance` (ascending). Brute force is fast on disk: per the FAQ ([`#L41-L45`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L41-L45)), 100K pairs of 1000-dim vectors takes <20ms. Below ~100K rows or with ~100ms latency budget, an index is often unnecessary.

### Full-text search

BM25-based. Requires a prior `create_fts_index`. Returns rows ordered by `_score` (descending). Phrase queries require `with_position=True` at index time.

### Hybrid search

Combines vector and FTS halves and merges them with a reranker. Default reranker is RRF. From [`docs/search/hybrid-search.mdx#L188-L242`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx#L188-L242):

```python
results = (
    table.search("flower moon", query_type="hybrid",
                 vector_column_name="vector", fts_columns="text")
    .rerank(reranker)
    .limit(10)
    .to_pandas()
)
```

Or pass vector and text explicitly:

```python
table.search(query_type="hybrid")
    .vector(vector_query)
    .text(text_query)
    .limit(5)
    .to_pandas()
```

A vector distance band can be applied to the vector half: `.distance_range(lower_bound=0.0, upper_bound=0.4)`. Half-open interval `[lower, upper)`. Either bound can be omitted ([`#L271-L298`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx#L271-L298)).

A working production hybrid query lives in `chat-with-videos`: 0.8 vector + 0.2 FTS weighting via the linear-combination reranker. See the architecture diagram in [`chat-with-videos/README.md#L14-L48`](https://github.com/lancedb/chat-with-videos/blob/main/README.md#L14-L48) and the `src/search/engine.py` source.

### Multivector search

For late-interaction retrieval (ColBERT, ColPaLi). Each document carries multiple per-token embeddings; the score is MaxSim - sum over query tokens of the max similarity against any document token. Cosine distance only ([`docs/search/multivector-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/multivector-search.mdx)).

### Query optimization tools

Two introspection methods on a query builder ([`docs/search/optimize-queries.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/optimize-queries.mdx)):

- `explain_plan(verbose=True)` - logical plan: index selection, filter order
- `analyze_plan()` - runtime metrics: `_elapsed_compute_`, `_output_rows_`, `_bytes_read_`, `_index_comparisons_`, `_iops_`

Use these to spot missing indexes or non-pushdownable filters.

### Returning row IDs

Every query supports `with_row_id(True)` (Python) / `withRowId()` (TypeScript). The `_rowid` column joins back to the primary table or dedupes across multiple sub-queries. See [`docs/search/hybrid-search.mdx#L246-L271`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx#L246-L271).

When `new_table_enable_stable_row_ids=true` is set at table creation, those row IDs survive compaction, delete, and merge.

### Enterprise SQL

A SQL-first interface for complex queries including FTS in SQL. Enterprise only. See [`docs/search/sql/`](https://github.com/lancedb/docs/tree/main/docs/search/sql).

---

## 12. Filtering

Two filter positions ([`docs/search/filtering.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/filtering.mdx)):

- **Prefilter (default)**: apply `where(...)` before the vector / FTS scan. Benefits from scalar indexes; results are guaranteed to satisfy `limit`.
- **Postfilter**: scan top-k, then apply `where(...)`. Faster when the predicate is non-selective or unindexed; may return fewer than `limit` rows.

Python: `where("category = 'film'", prefilter=True)` or `prefilter=False`. TypeScript: `.where(...)` is prefilter; chain `.postfilter()` after `.where(...)` for postfilter.

Predicates are SQL expressions evaluated against scalar columns: comparisons (`=`, `<`, `>`, `<=`, `>=`, `!=`), `IN`, `BETWEEN`, `IS NULL`, plus standard SQL functions. The full grammar comes from Lance's expression engine.

Use `explain_plan` on a hybrid query to see whether the filter pushed into the scan or ran as a separate `FilterExec` step. Reference: [`docs/search/hybrid-search.mdx#L300-L339`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx#L300-L339).

---

## 13. Reranking

A reranker reorders the merged result set from a hybrid query, or rescores a single-source query. Pluggable interface; built-ins under [`integrations/reranking/`](https://github.com/lancedb/docs/tree/main/docs/integrations/reranking).

| Reranker | What it does |
|----------|--------------|
| `RRFReranker` | Reciprocal Rank Fusion (default). Score = `1 / (k + rank)`. |
| `LinearCombinationReranker` | weighted sum of normalised scores |
| `CohereReranker`, `JinaReranker`, `VoyageAIReranker`, `OpenAIReranker` | hosted cross-encoder APIs |
| `CrossEncoderReranker` | local sentence-transformers cross-encoder |
| `ColbertReranker` | ColBERT late-interaction reranker |
| `AnswerdotaiReranker` | answerdotai/answerai-colbert |
| `MRR` | reciprocal rank |

API ([`docs/reranking/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/reranking/index.mdx)): `Reranker` ABC with an `eval()` method that takes candidate lists and returns scores. Custom rerankers ([`docs/reranking/custom-reranker.mdx`](https://github.com/lancedb/docs/blob/main/docs/reranking/custom-reranker.mdx)) only have to implement `rerank_hybrid`, `rerank_vector`, `rerank_fts` as needed.

Multi-vector reranking (rerank across multiple vector queries) requires `_rowid` to dedupe. Reference: [`docs/reranking/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/reranking/index.mdx).

Reranker evaluation patterns: [`docs/reranking/eval.mdx`](https://github.com/lancedb/docs/blob/main/docs/reranking/eval.mdx).

The `LOCOMO` benchmark ([`locomo-eval/`](https://github.com/lancedb/locomo-eval)) compares three OpenClaw memory backends head-to-head. On a 50-row LOCOMO subset, the reported numbers are ([`locomo-eval/README.md#L196-L205`](https://github.com/lancedb/locomo-eval/blob/main/README.md#L196-L205)):

| Backend | Correct | Wrong | Avg latency (s) |
|---------|--------:|------:|---------------:|
| `memory-core` (SQLite chunks) | 27 / 50 | 23 | 6.7 |
| `memory-lancedb` (built-in plugin) | 32 / 50 | 18 | 4.1 |
| `memory-lancedb-pro` (CortexReach plugin with retrieval tuning) | 36 / 50 | 14 | 10.9 |

The `memory-lancedb-pro` plugin reuses the same chunks and embeddings as `memory-lancedb` (migration via the plugin's supported path), so the lift is attributable to retrieval tuning rather than corpus differences.

---

## 14. Embeddings

LanceDB ships an **embedding registry** that lets you attach a model to a table schema; the model then runs automatically on insert and query.

Registry call: `get_registry().get("provider").create(model="...", ...)`. See [`docs/embedding/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/embedding/index.mdx) and [`docs/embedding/quickstart.mdx`](https://github.com/lancedb/docs/blob/main/docs/embedding/quickstart.mdx).

Built-in providers (15) under [`docs/integrations/embedding/`](https://github.com/lancedb/docs/tree/main/docs/integrations/embedding):

`huggingface`, `aws` (SageMaker / Bedrock), `cohere`, `colpali`, `gemini`, `ibm`, `imagebind`, `instructor`, `jina`, `ollama`, `openai`, `openclip`, `sentence-transformers`, `voyageai`, `superlinked`.

Pydantic-style schema attaching ([`docs/search/hybrid-search.mdx#L107-L116`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx#L107-L116)):

```python
embeddings = get_registry().get("sentence-transformers").create()

class Documents(LanceModel):
    text: str = embeddings.SourceField()
    vector: Vector(embeddings.ndims()) = embeddings.VectorField()

table = db.create_table("name", schema=Documents)
```

Once attached, `table.add([{"text": "..."}])` and `table.search("query text")` both auto-embed.

The Rust SDK does not auto-embed at query time - callers compute query vectors explicitly. The Rust embeddings module is at [`rust/lancedb/src/embeddings.rs`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/embeddings.rs).

The `lancedb-mcp-server` repo ([`lancedb_mcp.py`](https://github.com/lancedb/lancedb-mcp-server/blob/main/lancedb_mcp.py)) shows the registry pattern for a minimal MCP server: configurable embedding provider via env vars (`LANCEDB_URI`, `TABLE_NAME`, `EMBEDDING_FUNCTION`, `MODEL_NAME`).

The `cocoindex-lancedb-demo` shows a multimodal embedding pipeline with `nomic-embed-text` (Ollama) for instructions and `openai/clip-vit-base-patch32` (HF) for images, all in the same table. See [`cocoindex-lancedb-demo/main.py`](https://github.com/lancedb/cocoindex-lancedb-demo/blob/main/main.py).

The `lancedb-duckdb-demo` does the same on the Amazon Berkeley Objects dataset with CLIP and `intfloat/multilingual-e5-base`. See [`lancedb-duckdb-demo/create_lancedb.py`](https://github.com/lancedb/lancedb-duckdb-demo/blob/main/create_lancedb.py).

---

## 15. Namespaces

Hierarchical catalog organisation. A path-based addressing scheme (`["prod", "search", "user"]`) maps to nested namespace structure inside a connection.

API ([`docs/namespaces/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/namespaces/index.mdx) and [`docs/namespaces/usage.mdx`](https://github.com/lancedb/docs/blob/main/docs/namespaces/usage.mdx)):

- `connection.create_namespace(path)`
- `connection.list_namespaces(parent=...)`
- `connection.drop_namespace(path)`
- `create_table(name, ..., namespace=path)` and `open_table(name, namespace=path)`

The protocol behind it is its own project: [`lance-format/lance-namespace`](https://github.com/lance-format/lance-namespace). That repo holds the spec markdowns, the OpenAPI specification, and language SDKs (Python in [`lance_namespace`](https://github.com/lance-format/lance-namespace/tree/main/python/lance_namespace) and the auto-generated [`lance_namespace_urllib3_client`](https://github.com/lance-format/lance-namespace/tree/main/python/lance_namespace_urllib3_client); Java SDK alongside). The Rust implementations of the Directory and REST namespaces live back in [`lance/rust/lance-namespace`](https://github.com/lance-format/lance/tree/main/rust/lance-namespace) and [`lance/rust/lance-namespace-impls`](https://github.com/lance-format/lance/tree/main/rust/lance-namespace-impls). At the snapshot date the SDK package is at v0.7.6. The design is three-layered: (1) Client Spec (catalog-agnostic API), (2) Implementation Specs (Directory, REST), (3) Language SDKs auto-generated from OpenAPI. See [`lance-namespace/docs/src/client/index.md`](https://github.com/lance-format/lance-namespace/blob/main/docs/src/client/index.md).

A copy of the spec also lives under [`lancedb/docs/lance-namespace/`](https://github.com/lancedb/docs/tree/main/lance-namespace) - this mirror is rendered in the Mintlify site but the source-of-truth is the `lance-format` repo.

**Enterprise** has *federated namespaces*: the namespace service vends per-request storage credentials (the cluster does not hold a single "all tenants" credential). This is the production multi-tenancy story.

In **OSS**, namespaces are flat directories under the connection URI; isolation depends on bucket-level or prefix-level policy in the underlying object store.

The Rust API has `connect_namespace` alongside `connect`: see [`rust/lancedb/src/lib.rs#L262-L264`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L262-L264).

---

## 16. Concurrency, conflicts, commit stores

LanceDB uses **optimistic concurrency control** at the manifest layer. Every commit attempts to bump the dataset version atomically; on conflict, the writer reads the new manifest, replays any non-conflicting changes, and retries. From [`docs/faq/faq-oss.mdx#L81-L89`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L81-L89):

> LanceDB can handle concurrent reads very well, and can scale horizontally. The main constraint is how well the storage layer you've chosen, scales. For writes, we support concurrent writing, though too many concurrent writers can lead to failing writes as there is a limited number of times a writer retries a commit.

A Python multiprocessing warning from the same FAQ:

> If you use Python's multiprocessing, you should probably not use `fork` as Lance is multi-threaded internally and `fork` and multi-threaded Python do not work well together.

Use `spawn`.

### Commit-store options

> **Note: docs lag.** The upstream [`docs/storage/configuration.mdx#L223-L255`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L223-L255) page still leads with the `s3+ddb://` recipe as the only safe path for concurrent writers on S3. As of mid-2025, AWS S3 supports [conditional writes natively](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/), and the LanceDB maintainers have stated on [`lancedb/lancedb#2002`](https://github.com/lancedb/lancedb/issues/2002) that "S3 and S3 express now work well out-of-the-box" - i.e., plain `s3://` is safe for multi-writer workloads. The DDB path remains supported for older Lance versions and for environments where the conditional-writes path is unavailable. See [`EVOLUTION.md` Section 8](EVOLUTION.md#8-concurrency) for the full timeline.

Three options for S3 multi-writer atomicity:

1. **Plain `s3://`** with a current LanceDB / Lance build (post-2025-07). Uses S3's native conditional-write semantics. No external commit store required.
2. **`s3+ddb://` scheme** with a DynamoDB commit table. Hash key `base_uri` (string), range key `version` (number). Small provisioned throughput suffices. Requires `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:DescribeTable` IAM permissions on the commit table. See [`docs/storage/configuration.mdx#L223-L249`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L223-L249). Still supported for back-compat or operational preference.
3. **S3 Express One Zone** has atomic writes natively; plain `s3://` against an S3 Express endpoint works without DynamoDB ([`#L210-L220`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L210-L220)).

GCS and Azure Blob have native atomic conditional writes; no commit store needed.

Note also: a too-aggressive `cleanup_older_than` on `s3+ddb://` deployments can permanently brick a dataset by deleting manifests that the commit store still references. Maintainer-recommended threshold is 1-2 weeks. See [`EVOLUTION.md` Section 8](EVOLUTION.md#8-concurrency) for the production-incident thread that established this guidance.

### Local commit lock

The local-filesystem path uses an internal commit lock; concurrent writers within a single host work without configuration.

### Idempotency

`merge_insert` with deterministic keys is idempotent across retries. Content-addressed payload (hashing source text into a stable ID) is the canonical pattern - see for example the OpenClaw context engine's `summary_id` derived from `sha256(sessionId, firstKeptEntryId, summaryText).slice(0, 24)` at [`lancedb-claw/.../store/helper.ts#L122-L129`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/store/helper.ts#L122-L129) and used in `addSummaries` via `mergeInsert(on="summary_id")` at [`store/retrieval.ts#L178-L192`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/store/retrieval.ts#L178-L192).

### Multipart-upload cleanup

S3 graceful shutdown aborts in-flight multipart uploads, but crashes can leave incomplete uploads. The docs explicitly recommend an S3 lifecycle rule deleting in-progress uploads after a few days. See [`docs/storage/configuration.mdx#L251-L255`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx#L251-L255).

---

## 17. Performance and tuning

The performance overview lives at [`docs/performance.mdx`](https://github.com/lancedb/docs/blob/main/docs/performance.mdx). Practical knobs:

**Ingest**
- Bulk vs streaming: `pa.Table` / `pd.DataFrame` / `pa.dataset()` triggers auto-parallelism; row-by-row `add()` does not (each call -> new fragment).
- Create empty, then add. Passing data to `create_table()` skips the parallel path.
- Iterators over Arrow batches: best memory profile for large file imports.

**Index choice**
- IVF_PQ default. Switch to IVF_HNSW_SQ if recall/latency benchmarks justify (best tradeoff in published numbers).
- IVF_RQ for maximum compression (1 bit/dim default). Dim must be divisible by 8.
- Set `refine_factor` 5-50 to recover recall lost to PQ compression.

**Search**
- Pre-filter by default; postfilter only when the predicate is unindexed and non-selective.
- Build scalar indexes on every column you filter on.
- Use `explain_plan` and `analyze_plan` to verify pushdown and find missing indexes.

**Maintenance**
- `optimize()` periodically. Frequency depends on commit rate; the CocoIndex demo recommends ~weekly for "trickle-volume" pipelines ([`cocoindex-lancedb-demo/README.md#L147-L168`](https://github.com/lancedb/cocoindex-lancedb-demo/blob/main/README.md#L147-L168)).
- Compaction has computational overhead and temporarily increases disk usage (new files written before old versions removed).

**Concurrency**
- Stay below the OCC retry ceiling per writer; if hot, batch writes (e.g., 10ms timeout flush) before commit.
- Use `s3+ddb://` (or S3 Express) for multi-writer S3.
- Avoid `fork`; use `spawn`.

**Caching**
The [`ocra`](https://github.com/lancedb/ocra) crate is an Arrow-native read-through object-store cache. Not bundled into LanceDB, but solves the "S3 IOPS / latency" side of the equation for read-heavy workloads. See [`ocra/src/lib.rs`](https://github.com/lancedb/ocra/blob/main/src/lib.rs) for `ReadThroughCache`, `InMemoryCache`, `PageCache`. Published as a separate crate.

### Scale targets

From the OSS FAQ ([`#L35-L39`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L35-L39)):

> LanceDB OSS can comfortably handle millions of vectors on a single node... If you need to scale to hundreds of millions of vectors or work with terabytes of data, we recommend LanceDB Enterprise. Enterprise customers regularly operate on billions of rows.

And from `#L76-L79`:

> We target good performance on ~10-50 billion rows and ~10-30 TB of data.

Recall/latency calibration on GIST-1M with IVF-PQ: >0.95 recall at <10ms with ~50 probes and `refine_factor=50`. Source [`#L70-L74`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx#L70-L74).

### Benchmark infrastructure

The [`lance-bench`](https://github.com/lancedb/lance-bench) repo runs Rust Criterion and Python pytest-benchmark suites and writes the results back into a LanceDB table on `s3://lance-bench-results` (or local `~/.lance-bench`). Schema lives at [`lance-bench/packages/lance_bench_db/models.py`](https://github.com/lancedb/lance-bench/blob/main/packages/lance_bench_db/models.py); GitHub Actions workflows orchestrate runs ([`lance-bench/.github/workflows/`](https://github.com/lancedb/lance-bench/tree/main/.github/workflows)).

The published Lance 2.1 random-access paper artifacts (NVMe vs Parquet, full-scan vs random access) are at [`lance-research/file_2_1/`](https://github.com/lancedb/lance-research/tree/main/file_2_1).

---

## 18. Geneva (feature engineering)

Geneva is **Enterprise-only**. It's a managed UDF runtime that runs feature transforms over Lance tables at scale and persists the results as new columns or materialized views.

Capabilities ([`docs/geneva/`](https://github.com/lancedb/docs/tree/main/docs/geneva)):

- **Scalar UDFs**: 1-row -> 1-row.
- **Batch UDFs**: N-row -> N-row.
- **Built-in providers**: OpenAI, Gemini, sentence-transformers (out-of-the-box embedding/inference UDFs).
- **Backfill jobs**: compute features over historical data.
- **Bulk-load columns**: import precomputed feature columns.
- **Materialized views**: persist computed columns.
- **Execution contexts**: local, Ray cluster, KubeRay.
- **Lifecycle**: version tracking, replay, rollback, conflict handling.
- **Console + metrics**.
- **Helm-based Kubernetes deployment**.

Reference: [`docs/geneva/reference.mdx`](https://github.com/lancedb/docs/blob/main/docs/geneva/reference.mdx) (`geneva.connect()`, Connection, Table, UDF).

The "incremental indexing keeps Lance fresh" pattern is shown in OSS form by [`cocoindex-lancedb-demo`](https://github.com/lancedb/cocoindex-lancedb-demo) as a CocoIndex `cocoindex update main -L` watcher driving small-batch upserts. CocoIndex itself depends on a Postgres long-lived connection between source and target; it's external to LanceDB but ships a [`built-in target for LanceDB`](https://cocoindex.io/docs/targets/lancedb).

---

## 19. Enterprise platform

Surface ([`docs/enterprise/`](https://github.com/lancedb/docs/tree/main/docs/enterprise)):

- [`index.mdx`](https://github.com/lancedb/docs/blob/main/docs/enterprise/index.mdx) - landing
- [`quickstart.mdx`](https://github.com/lancedb/docs/blob/main/docs/enterprise/quickstart.mdx) - `db://` URI, RemoteTable, the same SDK as OSS
- [`architecture.mdx`](https://github.com/lancedb/docs/blob/main/docs/enterprise/architecture.mdx) - distributed multimodal lakehouse, automatic indexing, federated namespaces
- [`security.mdx`](https://github.com/lancedb/docs/blob/main/docs/enterprise/security.mdx) - API keys, mTLS, RBAC, encryption at rest and in flight
- [`benchmarks.mdx`](https://github.com/lancedb/docs/blob/main/docs/enterprise/benchmarks.mdx) - latency, throughput, cost vs alternatives
- [`deployment/azure.mdx`](https://github.com/lancedb/docs/blob/main/docs/enterprise/deployment/azure.mdx) - Azure-native deployment

Differentiators vs OSS:

| Feature | OSS | Enterprise |
|---------|-----|------------|
| `create_index` | manual | automatic |
| Index build mode | sync (blocking) | async (returns immediately, `wait_timeout` to await) |
| Storage credentials | passed in `storage_options` | cluster-managed |
| Federated multi-tenant namespaces | n/a (use bucket prefix) | namespace service vends per-request creds |
| Geneva (feature engineering) | n/a | included |
| REST API | n/a | included (OpenAPI) |
| Consistency | per-connection `read_consistency_interval` | deployment-configured `weak_read_consistency_interval_seconds` |
| `optimize()` retention | manual | auto-managed |

The `chat-with-videos` demo runs against either OSS local LanceDB (`--local`) or LanceDB Enterprise (default) using the same code paths and switching only the `db_config.py`. See [`chat-with-videos/src/api/db_config.py`](https://github.com/lancedb/chat-with-videos/blob/main/src/api/db_config.py).

---

## 20. Integrations

### Embedding providers

Same 15 listed in Section 14, each with its own page under [`docs/integrations/embedding/`](https://github.com/lancedb/docs/tree/main/docs/integrations/embedding). Auth, model name, params per provider.

### Rerankers

10 built-ins under [`docs/integrations/reranking/`](https://github.com/lancedb/docs/tree/main/docs/integrations/reranking): `answerdotai`, `cohere`, `colbert`, `cross_encoder`, `jina`, `linear_combination`, `mrr`, `openai`, `rrf`, `voyageai`.

### Data frameworks

[`docs/integrations/data/`](https://github.com/lancedb/docs/tree/main/docs/integrations/data):

- **Pydantic** - `LanceModel`, `Vector(N)`, `SourceField`/`VectorField`
- **DuckDB** - SQL over Lance via the official extension at [`lance-format/lance-duckdb`](https://github.com/lance-format/lance-duckdb). `INSTALL lance; LOAD lance;` then `ATTACH '...' TYPE LANCE`. The extension exposes SQL functions for vector search (`lance_vector_search`), FTS (`lance_fts`), and hybrid search (`lance_hybrid_search`); also supports `COPY ... TO ... (FORMAT lance)` for writes. See [`lance-duckdb/docs/sql.md`](https://github.com/lance-format/lance-duckdb/blob/main/docs/sql.md). Working LanceDB-side example: [`lancedb-duckdb-demo/`](https://github.com/lancedb/lancedb-duckdb-demo) with [`image_search.py`](https://github.com/lancedb/lancedb-duckdb-demo/blob/main/image_search.py) and [`text_search.py`](https://github.com/lancedb/lancedb-duckdb-demo/blob/main/text_search.py).
- **Pandas / PyArrow**
- **Polars / Arrow**
- **dlt** - data load tool
- **Voxel51** - computer-vision dataset format

### AI frameworks

[`docs/integrations/ai/`](https://github.com/lancedb/docs/tree/main/docs/integrations/ai):

- **HuggingFace** - dataset loading via `hf://datasets/...`; the [`hf-upload-demo/`](https://github.com/lancedb/hf-upload-demo) repo shows the Hub upload story
- **Agno** (formerly PhiData) - agent framework
- **LangChain** - vector store + retriever
- **LlamaIndex** - vector store
- **Genkit** - Google's agent kit
- **Kiln** - evaluation harness
- **PromptTools**
- **Synthetic Data Kit**

### MCP

Not in the official integrations index, but [`lancedb-mcp-server`](https://github.com/lancedb/lancedb-mcp-server) is a single-file FastMCP server exposing `ingest_docs`, `query_table`, and `table_details` tools. See [`lancedb_mcp.py`](https://github.com/lancedb/lancedb-mcp-server/blob/main/lancedb_mcp.py). It uses `LanceModel` schemas and the embedding registry under the hood.

### Tutorial collection

[`vectordb-recipes`](https://github.com/lancedb/vectordb-recipes) is the recipes monorepo: 69 examples + 18 tutorials + 9 applications, in Python and Node.js. Coverage spans hybrid search (`Inbuilt-Hybrid-Search`), multimodal (`multimodal_clip_diffusiondb`, `multimodal_meme_finder`), RAG patterns (`RAG-On-PDF`, `Contextual-RAG`, `RAG-from-Scratch`, `Local-RAG-from-Scratch`), recommenders (`Music_Recommendation`, `article_recommender`), agents (CrewAI / Langgraph / Swarm patterns), and chunking strategies. See [`vectordb-recipes/README.md`](https://github.com/lancedb/vectordb-recipes/blob/main/README.md).

---

## 21. REST API

Enterprise only. OpenAPI spec is published in the docs repo at [`docs/api-reference/rest/openapi.yml`](https://github.com/lancedb/docs/blob/main/docs/api-reference/rest/openapi.yml). The Mintlify site renders it under [`/api-reference/rest`](https://github.com/lancedb/docs/blob/main/docs/api-reference/rest/index.mdx). Coverage: connection, table CRUD, search endpoints, management endpoints. Authentication: API key, region, optional host override.

---

## 22. Capability matrix

What the system can and can't do today, in one place. Links go to the section that backs the claim.

### Storage

| Capability | Status | Reference |
|------------|--------|-----------|
| Local filesystem | yes | Section 4 |
| AWS S3 + S3-compatible (MinIO, Cloudflare R2, etc.) | yes | Section 4 |
| AWS S3 with multi-writer atomic commits | yes (DynamoDB commit store, or S3 Express) | Section 16 |
| Google Cloud Storage | yes | Section 4 |
| Azure Blob | yes | Section 4 |
| Tigris | yes | Section 4 |
| Alibaba Cloud OSS | yes (`oss` crate feature) | Section 3 |
| HuggingFace Hub paths (`hf://datasets/...`) | yes | Section 3, [`hf-upload-demo`](https://github.com/lancedb/hf-upload-demo) |
| Arbitrary `object_store::ObjectStore` impls (custom backends) | yes (Rust only via `Session::new` with `ObjectStoreRegistry`; see [`rust/lancedb/src/lib.rs#L266-L268`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs#L266-L268)) | Section 3 |
| External read-through cache | available as separate crate ([`ocra`](https://github.com/lancedb/ocra)) | Section 17 |

### Schema and data

| Capability | Status | Reference |
|------------|--------|-----------|
| Vectors as `FixedSizeList<Float16/Float32>` | yes | Section 5 |
| Binary blobs in standard columns | yes | Section 9 |
| Lance Blob v2 (large blobs, lazy stream, `take_blobs`) | yes | Section 9 |
| Map type (Lance 2.2) | yes (`stable` format) | Section 4, see [`EVOLUTION.md`](EVOLUTION.md) |
| Add columns without rewrite | yes | Section 6 |
| Drop / rename / retype columns | yes (`alter_columns`, metadata-only) | Section 6 |
| Row-id stability across compaction/delete | opt-in (`new_table_enable_stable_row_ids=true`) | Section 4 |
| Time-travel to any retained version | yes | Section 7 |
| Tag versions (preserved against pruning) | yes | Section 7 |
| Forking a table at a given version | partial - `restore` to a tagged version creates a new head; no first-class `branch` operation in the public LanceDB SDK (Lance underneath has more) | Section 7 |

### Search

| Capability | Status | Reference |
|------------|--------|-----------|
| Vector kNN (L2 / cosine / dot / hamming) | yes | Section 11 |
| ANN with disk-based index | yes (IVF-PQ default) | Section 10 |
| Brute-force kNN | yes (no index needed up to ~100K rows) | Section 11 |
| BM25 full-text search | yes | Section 11 |
| Phrase queries | yes (`with_position=true` at index time) | Section 10 |
| Multilingual stemming / tokenizers | yes | Section 10 |
| Hybrid search (vector + FTS) | yes (RRF default) | Section 11 |
| RRF / Cohere / CrossEncoder / ColBERT / Jina / Voyage / OpenAI rerankers | yes | Section 13 |
| Custom rerankers | yes (`Reranker` ABC) | Section 13 |
| Multivector / late-interaction (ColBERT, ColPaLi) | yes (cosine only) | Section 11 |
| Distance bounds on vector search | yes (`distance_range(lower, upper)`) | Section 11 |
| Pre-filter / post-filter | yes | Section 12 |
| Returning `_rowid` | yes | Section 11 |
| SQL filter expressions | yes | Section 12 |
| Full SQL surface (joins, CTEs) | partial - Enterprise SQL only | Section 11 |
| GPU-accelerated index build | Python only | Section 10 |

### Concurrency

| Capability | Status | Reference |
|------------|--------|-----------|
| Concurrent reads | unlimited (object-store-bound) | Section 16 |
| Concurrent writes - local FS | yes (internal lock) | Section 16 |
| Concurrent writes - S3 | yes natively (S3 conditional writes since mid-2025); `s3+ddb://` and S3 Express also supported | Section 16 |
| Concurrent writes - GCS / Azure | yes (native conditional writes) | Section 16 |
| OCC retries | yes (with bounded retry budget per writer) | Section 16 |
| Stable row IDs across mutation | opt-in | Section 4 |

### Operational

| Capability | Status | Reference |
|------------|--------|-----------|
| Encryption at rest | bucket SSE / SSE-KMS at the storage layer | Section 4 |
| Application-level encryption | no (operational concern) | Section 4 |
| Bucket-level credentials per namespace | OSS: prefix only; Enterprise: federated namespace service | Section 15 |
| Built-in compaction / pruning | `optimize()` (with `cleanup_older_than`, default 7 days) | Section 5, Section 7 |
| Built-in audit log | no | - |
| Built-in CDC / streaming | no | - |
| Backup / point-in-time recovery | use cloud snapshots; tags pin versions | Section 7 |

### Bindings

| Capability | Status | Reference |
|------------|--------|-----------|
| Rust SDK | yes (async-only) | Section 3 |
| Python SDK (sync) | yes | Section 3 |
| Python SDK (async) | yes (`AsyncTable`) | Section 3 |
| TypeScript SDK | yes (async-only) | Section 3 |
| Java SDK | yes (sync) | Section 3 |
| Auto-embedding at query time | Python and TypeScript yes; Rust no | Section 14 |
| Auto-embedding at insert time | yes (all SDKs that ship the embedding registry) | Section 14 |
| Pydantic LanceModel | Python only | Section 14 |

### Cross-engine and tooling

| Capability | Status | Reference |
|------------|--------|-----------|
| DuckDB SQL over Lance (read + write) | yes via [`lance-format/lance-duckdb`](https://github.com/lance-format/lance-duckdb) extension | Section 20 |
| Ray Data integration (source + sink + distributed indexing) | yes via [`lance-format/lance-ray`](https://github.com/lance-format/lance-ray) | Section 23 |
| PostgreSQL foreign-data wrapper | yes (read-only) via [`lance-format/pglance`](https://github.com/lance-format/pglance) | Section 23 |
| Cypher graph queries over Lance | yes via [`lance-format/lance-graph`](https://github.com/lance-format/lance-graph); preview status | Section 23 |
| Standalone agent-memory primitive | yes via [`lance-format/lance-context`](https://github.com/lance-format/lance-context); preview | Section 23 |
| Visual dataset inspector | yes via [`lance-format/lance-data-viewer`](https://github.com/lance-format/lance-data-viewer) | Section 23 |

### Things LanceDB does not include

- Authentication, authorisation, user identity (cluster owns API keys; OSS has none).
- Container orchestration, routing, channel adapters.
- Observability platform (no built-in metrics dashboard; export to your own).
- A built-in relational join planner that crosses datasets. Cross-table joins go through DuckDB (`lance-duckdb`) or other compute engines mounted on top of Lance.
- Streaming ingest with incremental indexing inside a single transaction. Each commit creates a new fragment; index deltas land at `optimize()`.
- Wire-format capture of provider-specific request/response bytes. Storage is application-level - you decide what columns to materialise.
- A renderer / UI / client-facing playback engine. (`lance-data-viewer` is read-only inspection, not a feature-rich frontend.)

---

## 23. Ecosystem repositories

LanceDB sits at the centre of two GitHub orgs: [`lancedb`](https://github.com/lancedb) (the database product) and [`lance-format`](https://github.com/lance-format) (the file format and engine that LanceDB depends on, plus a small ecosystem around it). Both halves are covered here.

### lance-format org

The Lance file format and its surrounding tooling. As of mid-2026 these are 9 repos under [`lance-format`](https://github.com/lance-format).

**[`lance-format/lance`](https://github.com/lance-format/lance)** - the core Rust workspace (23+ crates) plus Python (PyO3) and Java bindings. This is what LanceDB pins at `=7.0.0-beta.4`. Top-level layout: [`rust/`](https://github.com/lance-format/lance/tree/main/rust) (the workspace - `lance`, `lance-core`, `lance-io`, `lance-arrow`, `lance-encoding`, `lance-index`, `lance-table`, `lance-namespace`, `lance-namespace-impls`, `lance-namespace-datafusion`, `lance-datagen`, `lance-testing`, plus compression sub-crates like `compression/fsst`, `compression/bitpacking`), [`python/`](https://github.com/lance-format/lance/tree/main/python) (the `pylance` package), [`java/`](https://github.com/lance-format/lance/tree/main/java), [`docs/src/`](https://github.com/lance-format/lance/tree/main/docs/src) (format spec + integration guides for DuckDB, Polars, Spark, Ray), [`protos/`](https://github.com/lance-format/lance/tree/main/protos) (protobuf schemas including the file-format spec at [`protos/file2.proto`](https://github.com/lance-format/lance/blob/main/protos/file2.proto)), [`notebooks/`](https://github.com/lance-format/lance/tree/main/notebooks). Has an [`AGENTS.md`](https://github.com/lance-format/lance/blob/main/AGENTS.md) with workspace dev commands. [Releases](https://github.com/lance-format/lance/releases) come every few days; v6.0.0-rc.3 was the last v6 (2026-05-04), v7.0.0-beta.1 cut a day earlier (2026-05-03). The v7 break was the "base-aware object-store access" change ([Lance PR #6647](https://github.com/lance-format/lance/pull/6647)). Recent v7 additions: MemWAL write-ahead log primitives ([PR #6669](https://github.com/lance-format/lance/pull/6669)), HNSW for the in-memory vector index ([PR #6701](https://github.com/lance-format/lance/pull/6701)), distributed FTS exec internals ([PR #6648](https://github.com/lance-format/lance/pull/6648)), branch/tag metadata maps ([PR #6364](https://github.com/lance-format/lance/pull/6364)), zonemap index segments ([PR #6593](https://github.com/lance-format/lance/pull/6593)).

**[`lance-format/lance-namespace`](https://github.com/lance-format/lance-namespace)** - the catalog-abstraction spec and language SDKs. SDK package version `0.7.6` at the snapshot date. Contains: spec markdown at [`docs/src/client/index.md`](https://github.com/lance-format/lance-namespace/blob/main/docs/src/client/index.md), implementation specs for the Directory and REST namespaces, OpenAPI specification, Python workspace under [`python/`](https://github.com/lance-format/lance-namespace/tree/main/python) split into `lance_namespace` (core SDK) and `lance_namespace_urllib3_client` (auto-generated REST client). The Rust impls of Directory and REST namespaces live back in the main `lance` repo. Architecture: 3-layer (Client Spec -> Implementation Specs -> Language SDKs auto-generated from OpenAPI). The repo's [`AGENTS.md`](https://github.com/lance-format/lance-namespace/blob/main/AGENTS.md) clarifies contribution scope - spec changes go here; new namespace impls go to `lance-namespace-impls`; Directory/REST impls live in `lance-format/lance`.

**[`lance-format/lance-duckdb`](https://github.com/lance-format/lance-duckdb)** - the official DuckDB extension. Hybrid build: C++ FFI (`src/*.cpp`) + Rust core (`rust/lib.rs`). DuckDB users `INSTALL lance; LOAD lance;`. SQL surface: `ATTACH '...' TYPE LANCE` for directory-as-catalog, scans on Lance tables, `COPY ... FORMAT lance` for writes, plus three search functions exposed as DuckDB SQL: `lance_vector_search()`, `lance_fts()`, `lance_hybrid_search()`. Cloud creds via `TYPE LANCE` secrets (S3/GCS/Azure). Pins Lance `4.0.1` (older than LanceDB's `=7.0.0-beta.4`); zero-copy Arrow FFI between DuckDB and Rust. Reference: [`docs/sql.md`](https://github.com/lance-format/lance-duckdb/blob/main/docs/sql.md), [`rust/lib.rs`](https://github.com/lance-format/lance-duckdb/blob/main/rust/lib.rs), [`src/lance_scan.cpp`](https://github.com/lance-format/lance-duckdb/blob/main/src/lance_scan.cpp), [`Cargo.toml`](https://github.com/lance-format/lance-duckdb/blob/main/Cargo.toml).

**[`lance-format/lance-ray`](https://github.com/lance-format/lance-ray)** - Ray Data integration. Lance datasets as a Ray Datasource and sink, plus distributed indexing and compaction launched as Ray tasks. Python package at v0.4.0-beta.1 (alpha). Requires `pylance>=6.0.0rc3`, `ray>=2.41.0`, `pyarrow>=17.0.0`. Top-level: [`lance_ray/datasource.py`](https://github.com/lance-format/lance-ray/blob/main/lance_ray/datasource.py) (Datasource impl), [`lance_ray/io.py`](https://github.com/lance-format/lance-ray/blob/main/lance_ray/io.py) (`read_lance` / `write_lance` entry points), [`lance_ray/index.py`](https://github.com/lance-format/lance-ray/blob/main/lance_ray/index.py) (distributed indexing), [`lance_ray/compaction.py`](https://github.com/lance-format/lance-ray/blob/main/lance_ray/compaction.py), [`lance_ray/fragment.py`](https://github.com/lance-format/lance-ray/blob/main/lance_ray/fragment.py) (fragment-writer API), [`lance_ray/pandas.py`](https://github.com/lance-format/lance-ray/blob/main/lance_ray/pandas.py). Examples and tests under [`examples/`](https://github.com/lance-format/lance-ray/tree/main/examples) and [`tests/`](https://github.com/lance-format/lance-ray/tree/main/tests).

**[`lance-format/lance-graph`](https://github.com/lance-format/lance-graph)** - Cypher + SQL query engine over Lance. Rust workspace with three crates: [`crates/lance-graph/`](https://github.com/lance-format/lance-graph/tree/main/crates/lance-graph) (Cypher engine, AST, planner), [`crates/lance-graph-catalog/`](https://github.com/lance-format/lance-graph/tree/main/crates/lance-graph-catalog) (catalog/metadata), [`crates/lance-graph-benches/`](https://github.com/lance-format/lance-graph/tree/main/crates/lance-graph-benches). Python bindings via PyO3 in [`python/`](https://github.com/lance-format/lance-graph/tree/main/python) split into `lance_graph` (thin Rust wrapper) and `knowledge_graph` (higher-level CLI / API / web service that ships heuristic-and-LLM-driven entity / relationship extraction from text). Supports Cypher (`MATCH`, `WHERE`, path expansion) and falls back to SQL via DataFusion. Unity Catalog integration to discover Delta / Parquet tables alongside Lance. Workspace pattern still being shaped per [issue #92](https://github.com/lance-format/lance-graph/issues/92); proposed split into `lance-graph-core`, `lance-graph-planner`, `lance-graph-simple` documented at [`docs/project_structure.md`](https://github.com/lance-format/lance-graph/blob/main/docs/project_structure.md). Reported throughput: ~1.35 Gelem/s on node filtering at 1M records. **Status: preview.**

**[`lance-format/lance-context`](https://github.com/lance-format/lance-context)** - the Lance team's own answer to versioned multimodal agent-memory storage. Independent of (and architecturally similar in spirit to) the OpenClaw `lancedb-claw` and `memory-lancedb-pro` plugins under [Section 23 OpenClaw integrations](#openclaw-integrations) below. Rust workspace with two crates: [`crates/lance-context-core/`](https://github.com/lance-format/lance-context/tree/main/crates/lance-context-core) (engine, no Python deps) and [`crates/lance-context/`](https://github.com/lance-format/lance-context/tree/main/crates/lance-context) (re-export facade). Python bindings via PyO3 + maturin in [`python/api.py`](https://github.com/lance-format/lance-context/blob/main/python/api.py). The `ContextRecord` schema includes `role`, content type (text / binary), embedding, plus orchestration metadata (`step`, `plan_id`, `tokens`, `timestamp`, `run_id`, `active_plan_id`, `tokens_used`). Multimodal-first - images stored as binary blobs alongside text. Time-travel via Lance manifest snapshots; background compaction to manage fragment overhead from frequent appends. Storage: local or remote via the standard `storage_options` dict. **Status: preview, no released version.**

**[`lance-format/pglance`](https://github.com/lance-format/pglance)** - PostgreSQL Foreign Data Wrapper (pgrx-based). `CREATE EXTENSION lance;`, then SQL functions `lance_import()`, `lance_attach_namespace()`, `lance_sync_namespace()` mount Lance datasets and namespaces as foreign tables. **Read-only intentionally.** Targets PostgreSQL 13-17 via pgrx 0.14.3 feature flags. Native-first type mapping (Arrow `list` <-> PG `array`, `struct` <-> composite types, `map` <-> `jsonb`). Schema-aware import auto-creates composite types. Async tokio runtime under pgrx's sync wrapper. Pins Lance v1.0 (notably older). Currently v0.0.0 (development; no release tag yet). Files: [`src/lib.rs`](https://github.com/lance-format/pglance/blob/main/src/lib.rs) (pgrx SQL function definitions + FDW boilerplate), [`src/fdw/`](https://github.com/lance-format/pglance/tree/main/src/fdw) (handler logic, scan, filter pushdown, type mapping), [`Cargo.toml`](https://github.com/lance-format/pglance/blob/main/Cargo.toml), [`lance.control`](https://github.com/lance-format/pglance/blob/main/lance.control) (PostgreSQL extension metadata).

**[`lance-format/lance-data-viewer`](https://github.com/lance-format/lance-data-viewer)** - read-only browser-based dataset inspector. FastAPI backend ([`backend/app.py`](https://github.com/lance-format/lance-data-viewer/blob/main/backend/app.py)) + multi-version Docker images. Notable: ships pre-built images for **6 historical LanceDB versions** (`0.3.1`, `0.3.4`, `0.5`, `0.16.0`, `0.24.3`, `0.29.2`) with per-image PyArrow constraint files in [`backend/constraints-*.txt`](https://github.com/lance-format/lance-data-viewer/tree/main/backend), so users can inspect older datasets that don't read on the latest LanceDB build. Recommends `0.29.2` for new projects (PyArrow 21.0.0). Mounted directories are `:ro`. Auto-detects CLIP 512-dim embeddings; shows norm / sparsity / sparklines per vector column. Server-side pagination for large tables. App version `0.2.0`. Dockerfile at [`docker/Dockerfile`](https://github.com/lance-format/lance-data-viewer/blob/main/docker/Dockerfile) uses a `LANCEDB_VERSION` build arg.

**[`lance-format/lance-python-doc`](https://github.com/lance-format/lance-python-doc)** - minimal CI / automation repo that publishes `pylance` Python SDK docs to `lance.org`. README is one line: "Repository for automating the publication of Python SDK Generated Documentation." Doesn't ship runtime code; mirrors `pylance` releases.

### lancedb org

A guide to all 14 repositories under the [`lancedb` GitHub org](https://github.com/lancedb), in rough order of breadth.

### Core

**[`lancedb/lancedb`](https://github.com/lancedb/lancedb)** - the core multi-language repo. Rust crate, Python (PyO3) bindings, TypeScript (napi-rs) bindings, and Java JNI bindings. The [`AGENTS.md`](https://github.com/lancedb/lancedb/blob/main/AGENTS.md) at the repo root documents layout, dev commands, and review guidelines (use `Into<T>`/`AsRef<T>` for public Rust APIs; doctests as functions when they need a connection).

**[`lancedb/docs`](https://github.com/lancedb/docs)** - Mintlify documentation site backing `docs.lancedb.com`. The full sitemap is in [`docs/docs.json`](https://github.com/lancedb/docs/blob/main/docs/docs.json). Organisation: Get Started -> Tables -> Namespaces -> Embeddings -> Indexing -> Search -> Reranking -> Storage -> Training -> Geneva -> Enterprise -> Support, plus separate tabs for Integrations, Tutorials, Demos, API Reference. Includes a separate `lance-namespace/` spec dir.

### Benchmarking and research

**[`lancedb/lance-bench`](https://github.com/lancedb/lance-bench)** - benchmark-running infrastructure. Stores results back into LanceDB. Schema (with nested struct fields for TestBed, DutBuild, SummaryValues) at [`packages/lance_bench_db/models.py`](https://github.com/lancedb/lance-bench/blob/main/packages/lance_bench_db/models.py). Python 3.12+, uv, AWS credentials for S3 (`s3://lance-bench-results` or local `~/.lance-bench`). GitHub Actions orchestration in [`.github/workflows/`](https://github.com/lancedb/lance-bench/tree/main/.github/workflows). Docs in [`docs/`](https://github.com/lancedb/lance-bench/tree/main/docs). [`CLAUDE.md`](https://github.com/lancedb/lance-bench/blob/main/CLAUDE.md) is a 9.7K instruction file for agents working in the repo.

**[`lancedb/lance-research`](https://github.com/lancedb/lance-research)** - papers and reproducibility artifacts. Currently contains [`file_2_1/`](https://github.com/lancedb/lance-research/tree/main/file_2_1), the source artifacts for "Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings". Subdirs `figures/`, `experiments/`, `results/`, `chart-scripts/`, `data/`, `paper/`. Compares Lance 2.1 against Parquet and Lance 2.0 (Arrow-style) on NVMe random access and full-scan throughput.

**[`lancedb/ocra`](https://github.com/lancedb/ocra)** - Rust object-store read-through cache crate (v0.1.1, on crates.io as `ocra`). Implements `ReadThroughCache`, `InMemoryCache`, `PageCache` over the Arrow `object_store::ObjectStore` trait. See [`src/lib.rs`](https://github.com/lancedb/ocra/blob/main/src/lib.rs), [`src/memory.rs`](https://github.com/lancedb/ocra/blob/main/src/memory.rs), [`src/read_through.rs`](https://github.com/lancedb/ocra/blob/main/src/read_through.rs). Not bundled with LanceDB; usable separately for any `object_store` consumer.

### Integration / interop demos

**[`lancedb/lancedb-duckdb-demo`](https://github.com/lancedb/lancedb-duckdb-demo)** - DuckDB Lance-extension interop on the Amazon Berkeley Objects multimodal dataset. CLIP image embeddings (`openai/clip-vit-base-patch32`) + `intfloat/multilingual-e5-base` text embeddings, written into a Lance table; DuckDB SQL queries via `ATTACH '...' TYPE LANCE` and joined against a synthetic local DuckDB sales table. Files: [`create_lancedb.py`](https://github.com/lancedb/lancedb-duckdb-demo/blob/main/create_lancedb.py), [`image_search.py`](https://github.com/lancedb/lancedb-duckdb-demo/blob/main/image_search.py), [`text_search.py`](https://github.com/lancedb/lancedb-duckdb-demo/blob/main/text_search.py).

**[`lancedb/cocoindex-lancedb-demo`](https://github.com/lancedb/cocoindex-lancedb-demo)** - incremental indexing pipeline driven by [CocoIndex](https://cocoindex.io/). Recipes dataset (13k rows, multimodal) with `nomic-embed-text` (Ollama) text embeddings and CLIP image embeddings, plus DSPy-driven LLM allergen extraction backfilled into new columns. Demonstrates: `cocoindex update main -L` watcher, FastAPI + Vite frontend, `table.optimize()` cadence, upsert pipelines on `id`. Source: [`main.py`](https://github.com/lancedb/cocoindex-lancedb-demo/blob/main/main.py), [`app.py`](https://github.com/lancedb/cocoindex-lancedb-demo/blob/main/app.py), [`data_generator.py`](https://github.com/lancedb/cocoindex-lancedb-demo/blob/main/data_generator.py). Postgres for CocoIndex source<->target tracking via Docker compose.

**[`lancedb/hf-upload-demo`](https://github.com/lancedb/hf-upload-demo)** - HuggingFace Hub Lance dataset workflow. Creates a multimodal table with struct fields, binary image blobs, and OpenAI `text-embedding-3-small` vectors; uploads to `hf://datasets/lancedb/magical_kingdom`; demonstrates schema evolution + `merge_insert` for updates. Files: [`create_dataset.py`](https://github.com/lancedb/hf-upload-demo/blob/main/create_dataset.py), [`update_dataset.py`](https://github.com/lancedb/hf-upload-demo/blob/main/update_dataset.py), [`inspect_dataset.py`](https://github.com/lancedb/hf-upload-demo/blob/main/inspect_dataset.py), [`query.py`](https://github.com/lancedb/hf-upload-demo/blob/main/query.py). Includes [`HF_DATASET_CARD.md`](https://github.com/lancedb/hf-upload-demo/blob/main/HF_DATASET_CARD.md) as a reference dataset card.

**[`lancedb/chat-with-videos`](https://github.com/lancedb/chat-with-videos)** - hybrid search + lazy blob streaming end-to-end app (Python FastAPI backend + Next.js frontend). PostgreSQL-talk YouTube playlist; transcripts indexed in LanceDB; video bytes stored as Lance blobs (`take_blobs()` + `BlobFile`) and streamed via HTTP Range requests. Hybrid search 0.8 vector + 0.2 FTS + PydanticAI query rewriter and context-ranker agents. Local + LanceDB Enterprise modes. See [`README.md`](https://github.com/lancedb/chat-with-videos/blob/main/README.md), [`src/api/services/video_service.py`](https://github.com/lancedb/chat-with-videos/blob/main/src/api/services/video_service.py), [`src/storage/blob_utils.py`](https://github.com/lancedb/chat-with-videos/blob/main/src/storage/blob_utils.py), [`src/search/engine.py`](https://github.com/lancedb/chat-with-videos/blob/main/src/search/engine.py).

**[`lancedb/vectordb-recipes`](https://github.com/lancedb/vectordb-recipes)** - the canonical recipe collection. Three top-level dirs: [`examples/`](https://github.com/lancedb/vectordb-recipes/tree/main/examples) (~69 examples), [`tutorials/`](https://github.com/lancedb/vectordb-recipes/tree/main/tutorials) (~18 step-by-step), [`applications/`](https://github.com/lancedb/vectordb-recipes/tree/main/applications) (~9 production apps). Both Python and Node.js. The README ([~50K chars](https://github.com/lancedb/vectordb-recipes/blob/main/README.md)) is itself a navigable index of all examples. Notable: `Inbuilt-Hybrid-Search`, `multimodal_clip_diffusiondb`, `Contextual-RAG`, `Local-RAG-from-Scratch`, `Music_Recommendation`, `article_recommender`, `multimodal_meme_finder`, `Geospatial-Recommendation-System`. Multi-agent integrations include CrewAI, Langgraph, Swarm.

**[`lancedb/lancedb-mcp-server`](https://github.com/lancedb/lancedb-mcp-server)** - single-file FastMCP server. Tools: `ingest_docs`, `query_table`, `table_details`. Configurable via env: `LANCEDB_URI`, `TABLE_NAME`, `EMBEDDING_FUNCTION`, `MODEL_NAME`. Uses `LanceModel` schemas + the embedding registry. Source: [`lancedb_mcp.py`](https://github.com/lancedb/lancedb-mcp-server/blob/main/lancedb_mcp.py).

### OpenClaw integrations

OpenClaw is a separate agent framework ([`openclaw/openclaw`](https://github.com/openclaw/openclaw)) with a plugin system. The next three repos are LanceDB plugins for it.

**[`lancedb/lancedb-claw`](https://github.com/lancedb/lancedb-claw)** - houses two distinct OpenClaw plugins.

The [`memory/`](https://github.com/lancedb/lancedb-claw/tree/main/memory) package is `memory-lancedb-claw`, an OpenClaw long-term memory plugin forked from `openclaw/extensions/memory-lancedb`. Single LanceDB table named `memories` with schema `{id, text, vector, importance, category, createdAt}`. Vector search converts L2 distance to similarity via `1 / (1 + distance)`. Auto-recall on `before_agent_start` lifecycle hook injects the top 3 matches into the agent's context (with prompt-injection escape; see `escapeMemoryForPrompt` in [`memory/index.ts#L236-L263`](https://github.com/lancedb/lancedb-claw/blob/main/memory/index.ts#L236-L263)). Auto-capture on `agent_end` runs regex triggers and a small ML-style category detector over user messages (only) to filter what to persist - skips emoji-heavy responses, prompt-injection-shaped text, summaries with markdown formatting. Tools: `memory_recall`, `memory_store`, `memory_forget`. CLI: `openclaw ltm list|search|stats`. The README at [`memory/README.md`](https://github.com/lancedb/lancedb-claw/blob/main/memory/README.md) and code at [`memory/index.ts`](https://github.com/lancedb/lancedb-claw/blob/main/memory/index.ts) (700 lines) and [`memory/config.ts`](https://github.com/lancedb/lancedb-claw/blob/main/memory/config.ts).

The [`context/`](https://github.com/lancedb/lancedb-claw/tree/main/context) package is the `lancedb-claw` context engine plugin (a different OpenClaw plugin slot, `plugins.slots.contextEngine = "lancedb-claw"`). It uses **four** LanceDB tables (`context_messages`, `context_summaries`, `context_state`, `skills`) with explicit Apache Arrow schemas; uses `mergeInsert` for upserts on stable PKs (`message_pk = "${sessionId}:${ordinal}"`, content-hashed `summary_id`); creates scalar indexes on every common filter column and either a vector index on `summary_vector` (when an embedding client is configured) or an FTS index on `summary_text` (when not). All operations are wrapped in the local `retryAsync` helper. See [`context/bindings/typescript/openclaw-context-engine/src/store/helper.ts`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/store/helper.ts), [`store/retrieval.ts`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/store/retrieval.ts), [`store/skill-search.ts`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/store/skill-search.ts), [`engine/retrieval.ts`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/engine/retrieval.ts), [`engine/skill-search.ts`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/src/engine/skill-search.ts). The package README at [`context/bindings/typescript/openclaw-context-engine/README.md`](https://github.com/lancedb/lancedb-claw/blob/main/context/bindings/typescript/openclaw-context-engine/README.md). Both packages mark themselves as actively iterating prototypes; expect refactors.

**[`lancedb/openclaw-lancedb-demo`](https://github.com/lancedb/openclaw-lancedb-demo)** - tutorial repo showing how to wire `memory-lancedb-pro` (a separate, CortexReach-published proprietary plugin) into OpenClaw, with a "Dungeon Buddy" use case where the agent persists player preferences across sessions. Important integration notes from the README:

- LanceDB Node.js needs `apache-arrow@18.1.0` as a peer dependency installed manually into `~/.openclaw/extensions/memory-lancedb-pro/` (the upstream package does not bundle it). See [`README.md#L117-L125`](https://github.com/lancedb/openclaw-lancedb-demo/blob/main/README.md#L117-L125).
- The plugin opens the LanceDB table at gateway start, so changes outside the gateway require a gateway restart for visibility ([`#L201-L203`](https://github.com/lancedb/openclaw-lancedb-demo/blob/main/README.md#L201-L203)).
- Plugin slot wiring: `plugins.slots.memory = memory-lancedb-pro`, `plugins.allow = ["memory-lancedb-pro"]` ([`#L132-L141`](https://github.com/lancedb/openclaw-lancedb-demo/blob/main/README.md#L132-L141)).
- Rule-of-thumb framing: trust gateway telemetry over agent self-description for memory provenance ([`#L298-L301`](https://github.com/lancedb/openclaw-lancedb-demo/blob/main/README.md#L298-L301)).

The demo's lightweight memory-store implementation (different from the `lancedb-claw` plugin) is at [`src/memory.ts`](https://github.com/lancedb/openclaw-lancedb-demo/blob/main/src/memory.ts) - a `memories` table with schema `{id, text, vector, category, scope, importance, timestamp, metadata}`, OpenAI `text-embedding-3-small`, scope-filtered list, and a "smart category map" projecting OpenClaw memory categories onto profile/preferences/entities/events/cases/patterns.

**[`lancedb/locomo-eval`](https://github.com/lancedb/locomo-eval)** - LOCOMO long-conversation memory benchmark harness. Three OpenClaw memory backends compared:

- `memory-core` (built-in OpenClaw, SQLite + chunked markdown)
- `memory-lancedb` (built-in plugin)
- `memory-lancedb-pro` (CortexReach proprietary plugin)

The setup writes the LOCOMO session markdown into the OpenClaw workspace, lets `memory-core` index it (chunks + embeddings), then for the LanceDB legs reads back the indexed chunks and writes them into the plugin's table - guaranteeing the corpus is identical across legs so accuracy differences are attributable to retrieval. For the Pro leg, the plugin's migration path materialises a `memory-lancedb-pro` store from the `memory-lancedb` store without re-embedding. Setup scripts: [`setup_memory_core.sh`](https://github.com/lancedb/locomo-eval/blob/main/setup_memory_core.sh), [`setup_memory_lancedb.sh`](https://github.com/lancedb/locomo-eval/blob/main/setup_memory_lancedb.sh), [`setup_memory_lancedb_pro.sh`](https://github.com/lancedb/locomo-eval/blob/main/setup_memory_lancedb_pro.sh). Build scripts: [`scripts/build_memory_core_corpus.py`](https://github.com/lancedb/locomo-eval/blob/main/scripts/build_memory_core_corpus.py), [`build_memory_lancedb_corpus.py`](https://github.com/lancedb/locomo-eval/blob/main/scripts/build_memory_lancedb_corpus.py), [`build_memory_lancedb_pro_corpus.py`](https://github.com/lancedb/locomo-eval/blob/main/scripts/build_memory_lancedb_pro_corpus.py). Run scripts: [`run_memory_core.py`](https://github.com/lancedb/locomo-eval/blob/main/scripts/run_memory_core.py), [`run_memory_lancedb.py`](https://github.com/lancedb/locomo-eval/blob/main/scripts/run_memory_lancedb.py), [`run_memory_lancedb_pro.py`](https://github.com/lancedb/locomo-eval/blob/main/scripts/run_memory_lancedb_pro.py), [`run_parallel.py`](https://github.com/lancedb/locomo-eval/blob/main/scripts/run_parallel.py). The OpenClaw gateway serializes requests through a single lane queue, so in-process concurrency for QA calls is ineffective; the harness ships a 4-subprocess parallel wrapper for large runs.

Run artifacts (`outputs/<run>/`): `selected_rows.jsonl`, `ingest_log.jsonl`, `reindex.log`, `document_log.jsonl`, `memory_status_before.json`, `memory_status_after.json`, `qa_results.jsonl`, `judged_results.jsonl`, `summary.json`. Default judge concurrency is 10; gateway QA is serial per process.

---

## 24. Direct file index

Common starting points:

| Topic | File |
|-------|------|
| Crate root + URI forms + module map | [`lancedb/rust/lancedb/src/lib.rs`](https://github.com/lancedb/lancedb/blob/main/rust/lancedb/src/lib.rs) |
| Storage configuration | [`docs/docs/storage/configuration.mdx`](https://github.com/lancedb/docs/blob/main/docs/storage/configuration.mdx) |
| Lance file format (concepts) | [`docs/docs/lance.mdx`](https://github.com/lancedb/docs/blob/main/docs/lance.mdx) |
| Tables overview | [`docs/docs/tables/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/index.mdx) |
| Versioning | [`docs/docs/tables/versioning.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/versioning.mdx) |
| Consistency | [`docs/docs/tables/consistency.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/consistency.mdx) |
| Schema evolution | [`docs/docs/tables/schema.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/schema.mdx) |
| Multimodal / blob | [`docs/docs/tables/multimodal.mdx`](https://github.com/lancedb/docs/blob/main/docs/tables/multimodal.mdx) |
| Indexing overview | [`docs/docs/indexing/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/index.mdx) |
| Vector indexes | [`docs/docs/indexing/vector-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/vector-index.mdx) |
| Quantization | [`docs/docs/indexing/quantization.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/quantization.mdx) |
| FTS index | [`docs/docs/indexing/fts-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/fts-index.mdx) |
| Scalar indexes | [`docs/docs/indexing/scalar-index.mdx`](https://github.com/lancedb/docs/blob/main/docs/indexing/scalar-index.mdx) |
| Vector search | [`docs/docs/search/vector-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/vector-search.mdx) |
| FTS | [`docs/docs/search/full-text-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/full-text-search.mdx) |
| Hybrid | [`docs/docs/search/hybrid-search.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/hybrid-search.mdx) |
| Filtering | [`docs/docs/search/filtering.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/filtering.mdx) |
| Query optimization | [`docs/docs/search/optimize-queries.mdx`](https://github.com/lancedb/docs/blob/main/docs/search/optimize-queries.mdx) |
| Reranking | [`docs/docs/reranking/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/reranking/index.mdx) |
| Custom rerankers | [`docs/docs/reranking/custom-reranker.mdx`](https://github.com/lancedb/docs/blob/main/docs/reranking/custom-reranker.mdx) |
| Embedding registry | [`docs/docs/embedding/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/embedding/index.mdx) |
| Namespaces | [`docs/docs/namespaces/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/namespaces/index.mdx) |
| Performance | [`docs/docs/performance.mdx`](https://github.com/lancedb/docs/blob/main/docs/performance.mdx) |
| OSS FAQ | [`docs/docs/faq/faq-oss.mdx`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-oss.mdx) |
| Enterprise FAQ | [`docs/docs/faq/faq-enterprise.mdx`](https://github.com/lancedb/docs/blob/main/docs/faq/faq-enterprise.mdx) |
| Geneva (overview) | [`docs/docs/geneva/index.mdx`](https://github.com/lancedb/docs/blob/main/docs/geneva/index.mdx) |
| Sitemap | [`docs/docs/docs.json`](https://github.com/lancedb/docs/blob/main/docs/docs.json) |

API reference (auto-generated):

- Python: <https://lancedb.github.io/lancedb/python/python/>
- TypeScript: <https://lancedb.github.io/lancedb/js/globals/>
- Rust: <https://docs.rs/lancedb/latest/lancedb/index.html>
- REST (Enterprise OpenAPI): [`docs/docs/api-reference/rest/openapi.yml`](https://github.com/lancedb/docs/blob/main/docs/api-reference/rest/openapi.yml)
