use crate::{
    RetryPolicy,
    config::{self},
    handlers::NamespaceIdent,
    sessions::{self},
};
use anyhow::{Context, Result};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode};
use lance::deps::arrow_array::{RecordBatch, RecordBatchIterator};
use lance::index::DatasetIndexExt;
use lance::index::DatasetIndexInternalExt;
use lance::index::vector::VectorIndexParams;
use lance::session::Session;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams};
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
};
use lance_linalg::distance::MetricType;
use lance_namespace::LanceNamespace;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::DescribeTableRequest;
use lance_namespace_impls::ConnectBuilder;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OnceCell};
use tokio_stream::StreamExt;
use url::Url;
/// Embedded-row count at which pond builds the IVF_PQ vector index on
/// `messages.vector` (spec.md#search). Below it, vector search runs a
/// brute-force flat scan - exact and fast at small and medium scale, and
/// IVF_PQ cannot train well on fewer vectors anyway.
pub const VECTOR_INDEX_ACTIVATION_ROWS: usize = 100_000;

/// Default minimum unindexed-fragment count required before a per-intent
/// append/rebuild step is admitted into `optimize_table_indices`. Lower
/// values make each commit smaller and more frequent (bad on remote
/// stores); higher values let fragments accumulate behind the brute-force
/// fallback. 4 is the floor of the documented 4-8 band.
pub const DEFAULT_INDEX_LAG_THRESHOLD: usize = 4;

static INDEX_LAG_THRESHOLD_RUNTIME: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Seed the process-wide index-lag threshold from `[search].index_lag_threshold`.
/// First call wins (mirrors `embed::init_model_id` / `sessions::init_embedding_dim`).
pub fn init_index_lag_threshold(value: usize) {
    INDEX_LAG_THRESHOLD_RUNTIME.get_or_init(|| value);
}

pub fn index_lag_threshold() -> usize {
    INDEX_LAG_THRESHOLD_RUNTIME
        .get()
        .copied()
        .unwrap_or(DEFAULT_INDEX_LAG_THRESHOLD)
}

/// Declarative description of one index pond keeps on a table. Created when
/// its trigger fires; folded forward by `pond index optimize`.
#[derive(Debug, Clone)]
pub struct IndexIntent {
    /// Stable on-disk name. Must match across runs so existence checks
    /// resolve.
    pub name: &'static str,
    /// Column the index covers.
    pub column: &'static str,
    /// Condition evaluated against the live dataset before each cycle.
    pub trigger: IndexTrigger,
    /// How the params are built at create time. Some intents have static
    /// params (FTS, scalars); IVF_PQ needs the row count to size partitions.
    pub params: IndexParamsKind,
}

/// When an [`IndexIntent`] should exist on disk.
#[derive(Debug, Clone)]
pub enum IndexTrigger {
    /// Build whenever the table has any rows. Used for FTS and scalar
    /// indices: there is no training cost worth delaying.
    OnAnyRows,
    /// Build when `count(<column> IS NOT NULL) >= threshold`. Used for the
    /// IVF_PQ vector index, which trains poorly on too few vectors.
    OnNonNullCount {
        column: &'static str,
        threshold: usize,
    },
}

/// The lance-native shape of an [`IndexIntent`]'s params, dispatched to the
/// right `IndexParams` at create time.
#[derive(Debug, Clone)]
pub enum IndexParamsKind {
    /// `BuiltinIndexType::BTree` -> [`IndexType::BTree`];
    /// `BuiltinIndexType::Bitmap` -> [`IndexType::Bitmap`]; etc.
    Scalar(BuiltinIndexType),
    /// `InvertedIndexParams` with a character `ngram` tokenizer in the
    /// `[min, max]` range and stemming / stop-words off
    /// (spec.md#language-neutral-index).
    InvertedFtsNgram { min: u32, max: u32 },
    /// `VectorIndexParams::ivf_pq` with cosine metric (e5 vectors are
    /// L2-normalized). `sub_vectors = embedding_dim / 8` and `num_bits = 8`
    /// are pond's conventions; `max_iters` caps kmeans. Partitions follow
    /// LanceDB's documented `num_rows // 4096` guidance, floored at one.
    IvfPqCosine {
        sub_vectors: usize,
        num_bits: u8,
        max_iters: usize,
    },
}

impl IndexTrigger {
    async fn should_create(&self, dataset: &Dataset) -> Result<bool> {
        match self {
            Self::OnAnyRows => Ok(dataset.count_rows(None).await? > 0),
            Self::OnNonNullCount { column, threshold } => {
                let count = dataset
                    .count_rows(Some(format!("{column} IS NOT NULL")))
                    .await?;
                Ok(count >= *threshold)
            }
        }
    }
}

impl IndexParamsKind {
    fn index_type(&self) -> IndexType {
        match self {
            Self::Scalar(BuiltinIndexType::Bitmap) => IndexType::Bitmap,
            Self::Scalar(_) => IndexType::BTree,
            Self::InvertedFtsNgram { .. } => IndexType::Inverted,
            Self::IvfPqCosine { .. } => IndexType::Vector,
        }
    }

    async fn build(&self, dataset: &Dataset) -> Result<Box<dyn lance::index::IndexParams>> {
        match self {
            Self::Scalar(kind) => Ok(Box::new(ScalarIndexParams::for_builtin(kind.clone()))),
            Self::InvertedFtsNgram { min, max } => Ok(Box::new(
                InvertedIndexParams::default()
                    .base_tokenizer("ngram".to_owned())
                    .ngram_min_length(*min)
                    .ngram_max_length(*max)
                    .stem(false)
                    .remove_stop_words(false),
            )),
            Self::IvfPqCosine {
                sub_vectors,
                num_bits,
                max_iters,
            } => {
                let count = dataset
                    .count_rows(Some("vector IS NOT NULL".to_owned()))
                    .await?;
                let partitions = count.checked_div(4096).unwrap_or(0).max(1);
                Ok(Box::new(VectorIndexParams::ivf_pq(
                    partitions,
                    *num_bits,
                    *sub_vectors,
                    MetricType::Cosine,
                    *max_iters,
                )))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStatus {
    pub table: Table,
    pub intent_name: String,
    pub fragments_covered: usize,
    pub unindexed_rows: usize,
    pub exists: bool,
}

/// Anyhow-chain sentinel pond attaches when `retry_lance` exhausts attempts
/// against an OCC commit-conflict failure (spec.md#protocol). The wire layer
/// downcasts to this type to classify the outcome as `conflict` rather than
/// the generic `storage_unavailable`.
#[derive(Debug, Clone, Copy)]
pub struct ConflictExhausted {
    pub attempts: u8,
}

impl std::fmt::Display for ConflictExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "commit conflict exhausted after {} attempt(s)",
            self.attempts
        )
    }
}

impl std::error::Error for ConflictExhausted {}

/// Per-phase result for one table's pass through `Handle::optimize_table`.
/// spec.md#substrate 3.7 (`index-write-decoupled`): the indices phase and the
/// compaction phase get independent retry budgets and independent commits,
/// so a hot writer that starves the Rewrite cannot abort the index Update.
#[derive(Debug)]
pub enum PhaseOutcome {
    /// Phase attempted and committed work.
    Ok,
    /// Phase attempted; no work was needed.
    Noop,
    /// Phase attempted; OCC retry budget exhausted on conflict (the operator
    /// can rerun later once the hot writer quiesces).
    SkippedConflict,
    /// Phase failed with a non-conflict error.
    Failed(anyhow::Error),
    /// Phase not requested by the caller (e.g. compaction skipped under
    /// `Store::build_indices_only`).
    NotAttempted,
}

impl PhaseOutcome {
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
}

/// What `Handle::optimize_table` did for one table.
#[derive(Debug)]
pub struct TableOptimizeOutcome {
    pub table: Table,
    pub indices: PhaseOutcome,
    pub compaction: PhaseOutcome,
}

/// Boundary event during one `Handle::optimize_table` pass. The CLI binds a
/// progress callback to render a live spinner; library callers pass `None`.
#[derive(Debug, Clone)]
pub enum OptimizeEvent {
    PhaseStart {
        table: Table,
        phase: OptimizePhase,
        detail: Option<String>,
    },
    PhaseDone {
        table: Table,
        phase: OptimizePhase,
        elapsed_ms: u64,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum OptimizePhase {
    Compact,
    Cleanup,
    IndexCreate,
    IndexRebuild,
    IndexAppend,
}

impl OptimizePhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Cleanup => "cleanup",
            Self::IndexCreate => "index-create",
            Self::IndexRebuild => "index-rebuild",
            Self::IndexAppend => "index-append",
        }
    }
}

pub type OptimizeProgressFn = Box<dyn Fn(OptimizeEvent) + Send + Sync>;

fn emit(progress: Option<&OptimizeProgressFn>, event: OptimizeEvent) {
    if let Some(callback) = progress {
        callback(event);
    }
}

/// True when the chain root is one of Lance's commit-conflict variants
/// (`CommitConflict`, `RetryableCommitConflict`, `TooMuchWriteContention`).
/// Everything else (timeouts, IAM denials, disk errors) is not a conflict.
pub fn is_commit_conflict(error: &anyhow::Error) -> bool {
    error.downcast_ref::<lance::Error>().is_some_and(|err| {
        matches!(
            err,
            lance::Error::CommitConflict { .. }
                | lance::Error::RetryableCommitConflict { .. }
                | lance::Error::TooMuchWriteContention { .. }
        )
    })
}

/// True when `retry_lance` exhausted retries against an OCC conflict and
/// attached `ConflictExhausted` to the chain head.
fn is_conflict_exhausted(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<ConflictExhausted>())
}

/// On-disk byte totals for the three session datasets, plus everything else
/// under the data-dir root. Sized by listing through Lance's object-store
/// layer (spec.md#storage-via-lance) so `file://` and `s3://` behave alike.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableSizes {
    pub sessions: u64,
    pub messages: u64,
    pub parts: u64,
    pub other: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    String(String),
    Int32(i32),
    Raw(String),
}
impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}
impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}
impl From<i32> for ScalarValue {
    fn from(value: i32) -> Self {
        Self::Int32(value)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    Eq(&'static str, ScalarValue),
    Ne(&'static str, ScalarValue),
    IsNull(&'static str),
    IsNotNull(&'static str),
    In(&'static str, Vec<ScalarValue>),
    LikeContains(&'static str, String),
    /// Regex match. Emitted as `regexp_like(<col>, '<pat>')`. Never pushes
    /// down to BTREE indexes (Lance's scalar-index-expr parser ignores it),
    /// so the filter is a full-scan-with-predicate - acceptable for
    /// human-driven `--project re:...` queries, not for hot paths.
    Regex(&'static str, String),
    Gte(&'static str, ScalarValue),
    Lte(&'static str, ScalarValue),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
}
impl Predicate {
    pub fn to_lance(&self) -> String {
        match self {
            Self::Eq(column, value) => format!("{column} = {}", value.to_lance()),
            Self::Ne(column, value) => format!("{column} <> {}", value.to_lance()),
            Self::IsNull(column) => format!("{column} IS NULL"),
            Self::IsNotNull(column) => format!("{column} IS NOT NULL"),
            Self::In(column, values) => {
                let values = values
                    .iter()
                    .map(ScalarValue::to_lance)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{column} IN ({values})")
            }
            Self::LikeContains(column, value) => {
                format!("{column} LIKE {} ESCAPE '\\'", like_contains(value))
            }
            Self::Regex(column, pattern) => {
                format!("regexp_like({column}, {})", quoted_string(pattern))
            }
            Self::Gte(column, value) => format!("{column} >= {}", value.to_lance()),
            Self::Lte(column, value) => format!("{column} <= {}", value.to_lance()),
            Self::And(predicates) => predicates
                .iter()
                .map(Self::to_lance)
                .filter(|predicate| !predicate.is_empty())
                .collect::<Vec<_>>()
                .join(" AND "),
            Self::Or(predicates) => {
                // Wrap in parens so the disjunction composes safely as a child
                // of an outer `And` (SQL `OR` binds looser than `AND`).
                let body = predicates
                    .iter()
                    .map(Self::to_lance)
                    .filter(|predicate| !predicate.is_empty())
                    .collect::<Vec<_>>()
                    .join(" OR ");
                if body.is_empty() {
                    String::new()
                } else {
                    format!("({body})")
                }
            }
        }
    }
}
/// Read-side options for `Handle::scan`: optional prefilter predicate and
/// optional projection. Default = no filter, all columns.
#[derive(Default)]
pub struct ScanOpts<'a> {
    pub predicate: Option<&'a Predicate>,
    pub projection: Option<&'a [&'a str]>,
}

impl<'a> ScanOpts<'a> {
    pub fn project_only(projection: &'a [&'a str]) -> Self {
        Self {
            predicate: None,
            projection: Some(projection),
        }
    }
    pub fn with_predicate_and_projection(
        predicate: &'a Predicate,
        projection: &'a [&'a str],
    ) -> Self {
        Self {
            predicate: Some(predicate),
            projection: Some(projection),
        }
    }
}

impl ScalarValue {
    fn to_lance(&self) -> String {
        match self {
            Self::String(value) => quoted_string(value),
            Self::Int32(value) => value.to_string(),
            Self::Raw(value) => value.clone(),
        }
    }
}
/// Lance cache caps in bytes. `None` lets the substrate pick the backend-aware
/// default (local FS gets a tighter cap; object stores stay near Lance's
/// defaults). Wired through `Store::open_with_options` from `[runtime]`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCaps {
    pub index_cache_bytes: Option<usize>,
    pub metadata_cache_bytes: Option<usize>,
}

impl RuntimeCaps {
    pub fn from_config(config: &crate::config::RuntimeConfig) -> Self {
        Self {
            index_cache_bytes: config.index_cache_bytes,
            metadata_cache_bytes: config.metadata_cache_bytes,
        }
    }
}

/// Local-FS default: tight enough that a long-lived `pond mcp` lands well
/// under the 500 MiB target without measurable latency cost vs Lance's 6 GiB
/// default (see `benches/serve_mem_bench.rs --cap-sweep`).
const LOCAL_INDEX_CACHE_BYTES: usize = 256 * 1024 * 1024;
const LOCAL_METADATA_CACHE_BYTES: usize = 128 * 1024 * 1024;
/// Object-store defaults: latency to refill is per-page, so keep more in cache.
const REMOTE_INDEX_CACHE_BYTES: usize = 2 * 1024 * 1024 * 1024;
const REMOTE_METADATA_CACHE_BYTES: usize = 512 * 1024 * 1024;

fn resolve_cache_caps(location: &Url, caps: RuntimeCaps) -> (usize, usize) {
    let (index_default, metadata_default) = if config::is_local(location) {
        (LOCAL_INDEX_CACHE_BYTES, LOCAL_METADATA_CACHE_BYTES)
    } else {
        (REMOTE_INDEX_CACHE_BYTES, REMOTE_METADATA_CACHE_BYTES)
    };
    (
        caps.index_cache_bytes.unwrap_or(index_default),
        caps.metadata_cache_bytes.unwrap_or(metadata_default),
    )
}

pub struct Handle {
    datasets: DatasetSet,
    retry: RetryPolicy,
    /// One `lance::Session` shared across all three datasets. Carries the
    /// metadata + index caches and the `ObjectStoreRegistry` (which holds
    /// the underlying object_store / S3 client). Sharing the session means
    /// one cache pool covers all three tables and one S3 client serves all
    /// three datasets - load-bearing on object-store backends where a
    /// per-dataset client would mean 3x the connection pools and 3x the
    /// credential refreshes (lance/src/dataset/builder.rs:509-517).
    #[allow(dead_code)]
    session: Arc<Session>,
    /// The `lance-namespace` catalog seam. v1 uses the Directory impl;
    /// future hosted pond swaps to "rest" without touching read/write paths
    /// (spec.md#catalog-seam).
    nm: Arc<dyn LanceNamespace>,
    /// Namespace identifier this handle binds to. v1 is always `root()`; the
    /// typed seam matches `resolve_namespace`'s return so multi-namespace
    /// routing can land without churning call sites (spec.md#namespace-resolution).
    nm_ident: NamespaceIdent,
    /// Object-store options threaded through every `DatasetBuilder` and
    /// `Dataset::write` call so refresh / index-creation paths inherit the
    /// same credentials and region as the initial open. Empty on local-FS
    /// installs.
    storage_options: HashMap<String, String>,
    /// Data-dir URL the handle was opened against. `pond status` reads this
    /// to display where the bytes live and to decide whether to walk a local
    /// directory or issue a remote `LIST` for sizing.
    location: Url,
    /// Cached `parts.lance` open metadata, used the first time a caller asks
    /// for parts. Holds the namespace probe shape so the lazy open re-uses the
    /// same `catalog-seam` path as the eager opens for sessions/messages.
    parts_refresh_after: Duration,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Handle")
            .field("datasets", &self.datasets)
            .field("retry", &self.retry)
            .field("nm_ident", &self.nm_ident)
            .field("storage_options", &self.storage_options)
            .field("location", &self.location)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Sessions,
    Messages,
    Parts,
}
impl Table {
    pub fn as_str(self) -> &'static str {
        self.label()
    }

    fn label(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Messages => "messages",
            Self::Parts => "parts",
        }
    }
}
#[derive(Debug)]
struct DatasetSet {
    sessions: Mutex<CachedDataset>,
    messages: Mutex<CachedDataset>,
    /// `parts.lance` opens lazily on the first read or write that needs it
    /// (`pond_get(include_parts=true)`, ingest with Part events, hydration
    /// during grouped search). MCP processes that only serve trimmed `pond_get`
    /// and `pond_search` never touch this file, saving its metadata pages and
    /// file handle at cold-open. The OnceCell makes init single-flight; the
    /// inner `Mutex<CachedDataset>` then behaves identically to the other two.
    parts: OnceCell<Mutex<CachedDataset>>,
}
#[derive(Debug)]
struct CachedDataset {
    dataset: Dataset,
    last_refresh: Instant,
    refresh_after: Duration,
}
impl CachedDataset {
    async fn latest(&mut self) -> Result<Dataset> {
        if self.last_refresh.elapsed() >= self.refresh_after {
            self.dataset.checkout_latest().await?;
            self.last_refresh = Instant::now();
        }
        Ok(self.dataset.clone())
    }
    fn replace(&mut self, dataset: Dataset) {
        self.dataset = dataset;
        self.last_refresh = Instant::now();
    }
}
impl Handle {
    /// Open without storage options or explicit cache caps. Backend-aware
    /// defaults from `[runtime]` apply.
    pub async fn open(location: &Url) -> Result<Self> {
        Self::open_with_options(location, HashMap::new(), RuntimeCaps::default()).await
    }

    /// Open with object-store options handed through to Lance verbatim, plus
    /// the resolved `[runtime]` cache caps. Object-store keys are the
    /// `object_store` crate's standard config names; pond does not parse them.
    /// Opening datasets never performs index work; index lifecycle lives under
    /// `Handle::optimize_table`. `parts.lance` opens lazily on first use.
    pub async fn open_with_options(
        location: &Url,
        mut storage_options: HashMap<String, String>,
        caps: RuntimeCaps,
    ) -> Result<Self> {
        if let Some(path) = config::local_path(location) {
            tokio::fs::create_dir_all(&path)
                .await
                .with_context(|| format!("failed to create data dir {}", path.display()))?;
        } else {
            apply_remote_storage_defaults(&mut storage_options);
        }
        // One Session shared across all three datasets so metadata/index
        // caches and the object_store registry (and thus any S3 client) are
        // pooled rather than duplicated three times. Caps are sized by the
        // `[runtime]` block; explicit values from `caps` win, otherwise the
        // local/remote backend default kicks in.
        let (index_cache_bytes, metadata_cache_bytes) = resolve_cache_caps(location, caps);
        let session = Arc::new(Session::new(
            index_cache_bytes,
            metadata_cache_bytes,
            Arc::new(ObjectStoreRegistry::default()),
        ));
        // Build the lance-namespace catalog seam once (spec.md#catalog-seam).
        // The `root` property is whatever URL the Directory impl understands;
        // `uri_to_url` (lance-io/object_store.rs) accepts both bare paths and
        // URLs, so passing the scheme-qualified URL for local FS works the
        // same as the bare-path form. Trailing slash stripped for clean logs.
        let root = location.as_str().trim_end_matches('/').to_string();
        let mut connect = ConnectBuilder::new("dir")
            .property("root", root)
            .session(session.clone());
        // Object-store credentials/region/endpoint flow into the namespace
        // via the `storage.<key>` property convention (lance-namespace-impls
        // dir.rs from_properties: lines 423-436).
        for (key, value) in &storage_options {
            connect = connect.property(format!("storage.{key}"), value.clone());
        }
        let nm: Arc<dyn LanceNamespace> = connect
            .connect()
            .await
            .context("failed to connect lance Directory namespace")?;
        let nm_ident = NamespaceIdent::root();
        // spec.md#handle-freshness: refresh window is scheme-keyed. Local-FS
        // manifest reads are microsecond-cheap, so `0` (always-refresh) is
        // essentially free and removes the stale-read window entirely. Object
        // stores have real per-call cost; `5s` caps manifest fetch overhead at
        // acceptable lag for human-driven queries.
        let refresh_after = if config::is_local(location) {
            Duration::ZERO
        } else {
            Duration::from_secs(5)
        };
        let handle = Self {
            datasets: DatasetSet {
                sessions: Mutex::new(CachedDataset {
                    dataset: open_or_create_via_ns(
                        &nm,
                        &nm_ident,
                        sessions::SESSIONS,
                        sessions::session_schema(),
                        &session,
                        &storage_options,
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                messages: Mutex::new(CachedDataset {
                    dataset: open_or_create_via_ns(
                        &nm,
                        &nm_ident,
                        sessions::MESSAGES,
                        sessions::message_schema(),
                        &session,
                        &storage_options,
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                parts: OnceCell::new(),
            },
            retry: RetryPolicy::default(),
            session,
            nm,
            nm_ident,
            storage_options,
            location: location.clone(),
            parts_refresh_after: refresh_after,
        };
        Ok(handle)
    }

    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Read-only view of the `storage_options` the handle was opened with.
    /// `pond status` needs them to instantiate a raw `object_store` client
    /// that can `LIST` the remote bucket for sizing.
    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    pub async fn row_counts(&self) -> Result<(usize, usize, usize)> {
        Ok((
            self.count_rows(Table::Sessions).await?,
            self.count_rows(Table::Messages).await?,
            self.count_rows(Table::Parts).await?,
        ))
    }

    /// Insert-only merge: append new rows, never overwrite a matched PK.
    /// Returns rows inserted. The fold lives separately under
    /// `Handle::optimize_table` (spec.md#index-write-decoupled).
    pub(crate) async fn merge_insert(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
    ) -> Result<u64> {
        self.merge(
            table,
            batch,
            row_count,
            "merge_insert",
            WhenMatched::DoNothing,
            WhenNotMatched::InsertAll,
        )
        .await
    }

    /// Update-only merge: `WhenMatched::UpdateAll` on matched PKs; unmatched
    /// rows dropped. The fold lives separately under `Handle::optimize_table`.
    pub(crate) async fn merge_update(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
    ) -> Result<u64> {
        self.merge(
            table,
            batch,
            row_count,
            "merge_update",
            WhenMatched::UpdateAll,
            WhenNotMatched::DoNothing,
        )
        .await
    }

    /// Shared merge path for [`Self::merge_insert`] and [`Self::merge_update`].
    /// Returns the number of rows affected (inserted or updated, whichever the
    /// behaviors produce).
    async fn merge(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
        op: &'static str,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<u64> {
        if row_count == 0 {
            return Ok(0);
        }
        let started = Instant::now();
        let result = self
            .retry_lance(table.label(), || async {
                let mut cached = self.cached(table).await?.lock().await;
                let existing = cached.latest().await?;
                let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
                let mut builder = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?;
                builder.when_matched(when_matched.clone());
                builder.when_not_matched(when_not_matched.clone());
                // pond presents each PK at most once per batch; FirstSeen keeps
                // the first occurrence rather than failing (Lance's default).
                builder.source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
                // Cleanup is operator-driven via `pond index optimize`; the
                // per-commit auto hook would add a LIST per write on remote
                // backends without changing the steady-state retention.
                builder.skip_auto_cleanup(true);
                let (dataset, stats) = builder
                    .try_build()?
                    .execute_reader(Box::new(reader))
                    .await?;
                cached.replace(dataset.as_ref().clone());
                Ok((
                    stats.num_inserted_rows + stats.num_updated_rows,
                    stats.num_skipped_duplicates,
                ))
            })
            .await;
        let skipped = result.as_ref().map(|(_, s)| *s).unwrap_or(0);
        tracing::info!(
            target: "pond::perf",
            op,
            table = %table.label(),
            rows = row_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            skipped,
            "merge",
        );
        result.map(|(affected, _)| affected)
    }

    /// Run the table-local maintenance cycle for the supplied index intents.
    /// BTree is rebuilt from scratch to dodge Lance v7.0.0-beta.16's flat
    /// BTree combine bug; Bitmap, FTS, and IVF_PQ fold via append.
    ///
    /// spec.md#substrate 3.7 (`index-write-decoupled`): indices and compaction
    /// commit independently and use independent retry budgets, so a hot writer
    /// that starves compaction (Rewrite) does not abort the index build
    /// (Update) the operator actually asked for.
    pub async fn optimize_table(
        &self,
        table: Table,
        intents: &[IndexIntent],
        progress: Option<&OptimizeProgressFn>,
        cleanup: crate::sessions::CleanupConfig,
    ) -> TableOptimizeOutcome {
        let compaction = self
            .run_optimize_compact_phase(table, progress, cleanup)
            .await;
        let indices = self
            .run_optimize_indices_phase(table, intents, progress)
            .await;
        TableOptimizeOutcome {
            table,
            indices,
            compaction,
        }
    }

    /// Run only the indices phase for one table. Used by `pond embed`'s tail
    /// to fold newly written vectors into the indices without paying the
    /// compaction retry budget while embed itself may still be writing.
    pub async fn optimize_table_indices_only(
        &self,
        table: Table,
        intents: &[IndexIntent],
        progress: Option<&OptimizeProgressFn>,
    ) -> PhaseOutcome {
        self.run_optimize_indices_phase(table, intents, progress)
            .await
    }

    async fn run_optimize_indices_phase(
        &self,
        table: Table,
        intents: &[IndexIntent],
        progress: Option<&OptimizeProgressFn>,
    ) -> PhaseOutcome {
        if intents.is_empty() {
            return PhaseOutcome::Noop;
        }
        let result = self
            .retry_lance(table.label(), || async {
                let mut guard = self.cached(table).await?.lock().await;
                let mut dataset = guard.latest().await?;
                let did_work =
                    optimize_table_indices(&mut dataset, intents, table, progress).await?;
                guard.replace(dataset);
                Ok::<_, anyhow::Error>(did_work)
            })
            .await;
        match result {
            Ok(true) => PhaseOutcome::Ok,
            Ok(false) => PhaseOutcome::Noop,
            Err(error) if is_conflict_exhausted(&error) => PhaseOutcome::SkippedConflict,
            Err(error) => PhaseOutcome::Failed(error),
        }
    }

    async fn run_optimize_compact_phase(
        &self,
        table: Table,
        progress: Option<&OptimizeProgressFn>,
        cleanup: crate::sessions::CleanupConfig,
    ) -> PhaseOutcome {
        let result = self
            .retry_lance(table.label(), || async {
                let mut guard = self.cached(table).await?.lock().await;
                let mut dataset = guard.latest().await?;
                optimize_table_compact(&mut dataset, table, progress, cleanup).await?;
                guard.replace(dataset);
                Ok::<_, anyhow::Error>(())
            })
            .await;
        match result {
            Ok(()) => PhaseOutcome::Ok,
            Err(error) if is_conflict_exhausted(&error) => PhaseOutcome::SkippedConflict,
            Err(error) => PhaseOutcome::Failed(error),
        }
    }

    pub async fn rebuild_index(&self, table: Table, intent: &IndexIntent) -> Result<()> {
        self.retry_lance(table.label(), || async {
            let mut guard = self.cached(table).await?.lock().await;
            let mut dataset = guard.latest().await?;
            rebuild_index(&mut dataset, intent).await?;
            guard.replace(dataset);
            Ok(())
        })
        .await
    }

    pub async fn index_status(
        &self,
        table: Table,
        intents: &[IndexIntent],
    ) -> Result<Vec<IndexStatus>> {
        let dataset = self.dataset(table).await?;
        index_status(table, &dataset, intents).await
    }

    pub(crate) async fn dataset(&self, table: Table) -> Result<Dataset> {
        let mut cached = self.cached(table).await?.lock().await;
        cached.latest().await
    }
    /// Build a prefiltered `Scanner` for `table`. Composable read entry
    /// point for callers that need to layer extra builder calls
    /// (`full_text_search`, `nearest`) on top of pond's predicate seam.
    /// Routine scans should prefer `Handle::scan`.
    pub(crate) async fn scanner(
        &self,
        table: Table,
        predicate: Option<&Predicate>,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let dataset = self.dataset(table).await?;
        scanner_with_prefilter(&dataset, predicate)
    }
    /// Single read entry point: prefilter via `predicate`, optionally
    /// project, return the prepared `Scanner` (spec.md#read-seam).
    pub async fn scan(
        &self,
        table: Table,
        opts: ScanOpts<'_>,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let mut scanner = self.scanner(table, opts.predicate).await?;
        if let Some(projection) = opts.projection {
            scanner.project(projection)?;
        }
        Ok(scanner)
    }
    pub(crate) async fn scan_batch(
        &self,
        table: Table,
        predicate: Option<&Predicate>,
        projection: &[&str],
    ) -> Result<RecordBatch> {
        let opts = ScanOpts {
            predicate,
            projection: (!projection.is_empty()).then_some(projection),
        };
        self.scan(table, opts)
            .await?
            .try_into_batch()
            .await
            .context("scan failed")
    }
    pub async fn count_rows(&self, table: Table) -> Result<usize> {
        self.dataset(table)
            .await?
            .count_rows(None)
            .await
            .map_err(Into::into)
    }
    /// Names of every index on `messages` - the vector-index tests read this.
    #[cfg(test)]
    pub(crate) async fn messages_index_names(&self) -> Result<Vec<String>> {
        let dataset = self.dataset(Table::Messages).await?;
        let indices = dataset.load_indices().await?;
        Ok(indices.iter().map(|index| index.name.clone()).collect())
    }

    /// Count rows in `table` not yet covered by `index_name`. Manifest-only;
    /// a missing index reports the whole table. Powers `pond index status`.
    pub(crate) async fn unindexed_row_count(
        &self,
        table: Table,
        index_name: &str,
    ) -> Result<usize> {
        let dataset = self.dataset(table).await?;
        let fragments = dataset
            .unindexed_fragments(index_name)
            .await
            .with_context(|| format!("unindexed_fragments failed for {}", table.label()))?;
        Ok(fragments
            .iter()
            .map(|fragment| fragment.num_rows().unwrap_or(0))
            .sum())
    }

    /// Drop the named index. Used by the `pond embed --force` model-swap path
    /// to retire an IVF_PQ whose centroids belong to the old distance
    /// space, before the next write re-bootstraps it over the new model's
    /// vectors. Errors when the index does not exist; callers may swallow
    /// that.
    pub(crate) async fn drop_index(&self, table: Table, name: &str) -> Result<()> {
        let mut guard = self.cached(table).await?.lock().await;
        let mut dataset = guard.latest().await?;
        dataset
            .drop_index(name)
            .await
            .with_context(|| format!("drop_index({name}) failed for {}", table.label()))?;
        guard.replace(dataset);
        Ok(())
    }

    /// Resolve each table's stored location through the namespace catalog
    /// (spec.md#catalog-seam) - no hardcoded `.lance` suffix.
    async fn table_location(&self, table_name: &str) -> Result<String> {
        let request = DescribeTableRequest {
            id: Some(self.nm_ident.as_table_id(table_name)),
            ..Default::default()
        };
        let response = self
            .nm
            .describe_table(request)
            .await
            .with_context(|| format!("failed to describe table {table_name}"))?;
        response
            .location
            .with_context(|| format!("namespace returned no location for table {table_name}"))
    }

    /// On-disk byte totals for the three datasets plus the data-dir remainder.
    /// Every byte is sized by listing through Lance's object store
    /// (spec.md#storage-via-lance), identical for `file://` and `s3://`.
    pub async fn table_sizes(&self) -> Result<TableSizes> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        let params = ObjectStoreParams {
            storage_options_accessor: (!self.storage_options.is_empty()).then(|| {
                Arc::new(StorageOptionsAccessor::with_static_options(
                    self.storage_options.clone(),
                ))
            }),
            ..Default::default()
        };

        let sessions = self
            .listed_size(
                &registry,
                &params,
                &self.table_location(sessions::SESSIONS).await?,
            )
            .await?;
        let messages = self
            .listed_size(
                &registry,
                &params,
                &self.table_location(sessions::MESSAGES).await?,
            )
            .await?;
        let parts = self
            .listed_size(
                &registry,
                &params,
                &self.table_location(sessions::PARTS).await?,
            )
            .await?;
        // `other` is whatever sits under the data-dir root but not in the three
        // tables (config.toml, stray index temp files): root total minus them.
        let root_total = self
            .listed_size(&registry, &params, self.location.as_str())
            .await?;
        let other = root_total.saturating_sub(sessions + messages + parts);
        Ok(TableSizes {
            sessions,
            messages,
            parts,
            other,
        })
    }

    /// Sum `ObjectMeta.size` for every object recursively under `uri`.
    async fn listed_size(
        &self,
        registry: &Arc<ObjectStoreRegistry>,
        params: &ObjectStoreParams,
        uri: &str,
    ) -> Result<u64> {
        let (store, base) = ObjectStore::from_uri_and_params(registry.clone(), uri, params)
            .await
            .with_context(|| format!("failed to open object store for {uri}"))?;
        let mut listing = store.list(Some(base));
        let mut total = 0u64;
        while let Some(meta) = listing.next().await {
            let meta = meta.with_context(|| format!("listing {uri} failed"))?;
            total += meta.size;
        }
        Ok(total)
    }
    async fn cached(&self, table: Table) -> Result<&Mutex<CachedDataset>> {
        match table {
            Table::Sessions => Ok(&self.datasets.sessions),
            Table::Messages => Ok(&self.datasets.messages),
            Table::Parts => self.parts_cached().await,
        }
    }

    /// Open `parts.lance` on first use (spec.md#datasets). Single-flight via
    /// `OnceCell`; once initialized, behaves identically to the other two.
    async fn parts_cached(&self) -> Result<&Mutex<CachedDataset>> {
        self.datasets
            .parts
            .get_or_try_init(|| async {
                let dataset = open_or_create_via_ns(
                    &self.nm,
                    &self.nm_ident,
                    sessions::PARTS,
                    sessions::part_schema(),
                    &self.session,
                    &self.storage_options,
                )
                .await?;
                Ok::<_, anyhow::Error>(Mutex::new(CachedDataset {
                    dataset,
                    last_refresh: Instant::now(),
                    refresh_after: self.parts_refresh_after,
                }))
            })
            .await
    }
    async fn retry_lance<T, Fut, Op>(&self, label: &str, mut operation: Op) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>>,
        Op: FnMut() -> Fut,
    {
        let mut attempt = 0u8;
        loop {
            attempt = attempt.saturating_add(1);
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < self.retry.attempts => {
                    let backoff = self.backoff(attempt);
                    // `{:#}` walks anyhow's cause chain inline; `%error` (Display)
                    // drops everything below the top-level message.
                    let error_chain = format!("{error:#}");
                    tracing::warn!(
                        label,
                        attempt,
                        ?backoff,
                        error = %error_chain,
                        "retrying Lance operation"
                    );
                    tokio::time::sleep(backoff).await;
                }
                Err(error) => {
                    let error_chain = format!("{error:#}");
                    tracing::warn!(
                        label,
                        attempt,
                        error = %error_chain,
                        "Lance operation exhausted retries"
                    );
                    // spec.md#protocol: surface OCC failures as a typed `conflict`
                    // rather than the generic `storage_unavailable` bucket. The
                    // chain root is a `lance::Error` (commit-conflict family) when
                    // pond's retry layer exhausted because the manifest could not
                    // be advanced; everything else (timeouts, IAM, disk) stays
                    // `storage_unavailable`.
                    if is_commit_conflict(&error) {
                        return Err(error.context(ConflictExhausted { attempts: attempt }));
                    }
                    return Err(error);
                }
            }
        }
    }
    fn backoff(&self, attempt: u8) -> Duration {
        let shift = u32::from(attempt.saturating_sub(1));
        let multiplier = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let base = self.retry.initial_backoff.saturating_mul(multiplier);
        // Symmetric +/- `jitter` factor de-correlates concurrent retriers on
        // a contended manifest (spec.md#retry-jitter); clamped to `max_backoff`.
        let factor = (1.0 + self.retry.jitter * (fastrand::f64() * 2.0 - 1.0)).max(0.0);
        base.mul_f64(factor).min(self.retry.max_backoff)
    }
}
/// Compaction phase: `compact_files` + `cleanup_old_versions`, both inside one
/// retry block. Distinct from the indices phase so a hot writer that loses the
/// Rewrite race here does not abort index work the operator actually asked for.
///
/// spec.md#index-write-decoupled mandates FRI on by default, but at
/// v7.0.0-beta.16 `defer_index_remap=true` together with `stable-row-ids`
/// panics in `optimize.rs::commit_compaction` with "defer_index_remap
/// requires row_addrs but none were provided": `rewrite_files` skips
/// row_addrs when stable row ids are on, then the FRI builder demands
/// them. With stable_row_ids the remap step is already a no-op
/// (`optimize.rs:1490`: `needs_remapping = !uses_stable_row_ids() &&
/// !defer_index_remap`), so running without FRI is correct - we only
/// lose the documented concurrency-with-index-build benefit. Flip to
/// `true` once upstream fixes the conflict.
async fn optimize_table_compact(
    dataset: &mut Dataset,
    table: Table,
    progress: Option<&OptimizeProgressFn>,
    cleanup: crate::sessions::CleanupConfig,
) -> Result<()> {
    let compaction = CompactionOptions {
        defer_index_remap: false,
        ..CompactionOptions::default()
    };

    emit(
        progress,
        OptimizeEvent::PhaseStart {
            table,
            phase: OptimizePhase::Compact,
            detail: None,
        },
    );
    let started = Instant::now();
    compact_files(dataset, compaction, None).await?;
    emit(
        progress,
        OptimizeEvent::PhaseDone {
            table,
            phase: OptimizePhase::Compact,
            elapsed_ms: started.elapsed().as_millis() as u64,
        },
    );

    emit(
        progress,
        OptimizeEvent::PhaseStart {
            table,
            phase: OptimizePhase::Cleanup,
            detail: None,
        },
    );
    let started = Instant::now();
    // delete_unverified=true is the operator opt-in via `--cleanup-older-than`
    // / `--vacuum` that bypasses Lance's 7-day in-progress safety guard
    // (UNVERIFIED_THRESHOLD_DAYS in lance/dataset/cleanup.rs). Required to
    // reclaim files younger than 7 days; unsafe under concurrent writers.
    dataset
        .cleanup_old_versions(
            cleanup.older_than,
            Some(cleanup.delete_unverified),
            Some(false),
        )
        .await
        .context("cleanup_old_versions failed during index optimize")?;
    emit(
        progress,
        OptimizeEvent::PhaseDone {
            table,
            phase: OptimizePhase::Cleanup,
            elapsed_ms: started.elapsed().as_millis() as u64,
        },
    );

    Ok(())
}

/// Indices phase: per-intent create/rebuild + batched `optimize_indices(append)`
/// for incremental families. Returns `true` if anything committed.
async fn optimize_table_indices(
    dataset: &mut Dataset,
    intents: &[IndexIntent],
    table: Table,
    progress: Option<&OptimizeProgressFn>,
) -> Result<bool> {
    let existing = dataset.load_indices().await?;
    let existing_names: std::collections::HashSet<String> =
        existing.iter().map(|index| index.name.clone()).collect();

    let mut append_indices: Vec<String> = Vec::new();
    let mut did_work = false;

    for intent in intents {
        let exists = existing_names.contains(intent.name);

        if !exists {
            if !intent.trigger.should_create(dataset).await? {
                continue;
            }
            let params = intent.params.build(dataset).await?;
            let index_type = intent.params.index_type();
            tracing::info!(
                index = intent.name,
                column = intent.column,
                "creating Lance index (trigger fired)",
            );
            emit(
                progress,
                OptimizeEvent::PhaseStart {
                    table,
                    phase: OptimizePhase::IndexCreate,
                    detail: Some(intent.name.to_owned()),
                },
            );
            let started = Instant::now();
            dataset
                .create_index(
                    &[intent.column],
                    index_type,
                    Some(intent.name.to_owned()),
                    params.as_ref(),
                    false,
                )
                .await
                .with_context(|| format!("failed to create index {}", intent.name))?;
            emit(
                progress,
                OptimizeEvent::PhaseDone {
                    table,
                    phase: OptimizePhase::IndexCreate,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                },
            );
            did_work = true;
            continue;
        }

        let unindexed = dataset.unindexed_fragments(intent.name).await?;
        if unindexed.is_empty() {
            continue;
        }
        // Lag guard: let fragments accumulate behind the brute-force fallback
        // rather than firing a commit per tiny append. Threshold is operator-
        // tunable via `[search].index_lag_threshold`.
        if unindexed.len() < index_lag_threshold() {
            continue;
        }
        match intent.params {
            IndexParamsKind::Scalar(BuiltinIndexType::BTree) => {
                let params = intent.params.build(dataset).await?;
                let index_type = intent.params.index_type();
                tracing::debug!(
                    target: "pond::perf",
                    index = intent.name,
                    column = intent.column,
                    "rebuilding Lance BTree index",
                );
                emit(
                    progress,
                    OptimizeEvent::PhaseStart {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        detail: Some(intent.name.to_owned()),
                    },
                );
                let started = Instant::now();
                dataset
                    .create_index(
                        &[intent.column],
                        index_type,
                        Some(intent.name.to_owned()),
                        params.as_ref(),
                        true,
                    )
                    .await
                    .with_context(|| format!("failed to rebuild index {}", intent.name))?;
                emit(
                    progress,
                    OptimizeEvent::PhaseDone {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    },
                );
                did_work = true;
            }
            IndexParamsKind::Scalar(BuiltinIndexType::Bitmap)
            | IndexParamsKind::InvertedFtsNgram { .. }
            | IndexParamsKind::IvfPqCosine { .. } => {
                append_indices.push(intent.name.to_owned());
            }
            IndexParamsKind::Scalar(_) => {
                let params = intent.params.build(dataset).await?;
                emit(
                    progress,
                    OptimizeEvent::PhaseStart {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        detail: Some(intent.name.to_owned()),
                    },
                );
                let started = Instant::now();
                dataset
                    .create_index(
                        &[intent.column],
                        intent.params.index_type(),
                        Some(intent.name.to_owned()),
                        params.as_ref(),
                        true,
                    )
                    .await
                    .with_context(|| format!("failed to rebuild index {}", intent.name))?;
                emit(
                    progress,
                    OptimizeEvent::PhaseDone {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    },
                );
                did_work = true;
            }
        }
    }

    if !append_indices.is_empty() {
        let to_append = append_indices.clone();
        emit(
            progress,
            OptimizeEvent::PhaseStart {
                table,
                phase: OptimizePhase::IndexAppend,
                detail: Some(append_indices.join(", ")),
            },
        );
        let started = Instant::now();
        dataset
            .optimize_indices(&OptimizeOptions::append().index_names(to_append))
            .await
            .context("optimize_indices(append) failed during index optimize")?;
        emit(
            progress,
            OptimizeEvent::PhaseDone {
                table,
                phase: OptimizePhase::IndexAppend,
                elapsed_ms: started.elapsed().as_millis() as u64,
            },
        );
        tracing::debug!(
            target: "pond::perf",
            indices = ?append_indices,
            "appended trailing fragments into indices",
        );
        did_work = true;
    }

    Ok(did_work)
}

async fn rebuild_index(dataset: &mut Dataset, intent: &IndexIntent) -> Result<()> {
    if !intent.trigger.should_create(dataset).await? {
        return Ok(());
    }
    let params = intent.params.build(dataset).await?;
    dataset
        .create_index(
            &[intent.column],
            intent.params.index_type(),
            Some(intent.name.to_owned()),
            params.as_ref(),
            true,
        )
        .await
        .with_context(|| format!("failed to rebuild index {}", intent.name))?;
    Ok(())
}

async fn index_status(
    table: Table,
    dataset: &Dataset,
    intents: &[IndexIntent],
) -> Result<Vec<IndexStatus>> {
    let existing = dataset.load_indices().await?;
    let existing_names: std::collections::HashSet<String> =
        existing.iter().map(|index| index.name.clone()).collect();
    let total_fragments = dataset.get_fragments().len();
    let total_rows = dataset.count_rows(None).await?;
    let mut statuses = Vec::with_capacity(intents.len());
    for intent in intents {
        let exists = existing_names.contains(intent.name);
        if !exists {
            statuses.push(IndexStatus {
                table,
                intent_name: intent.name.to_owned(),
                fragments_covered: 0,
                unindexed_rows: total_rows,
                exists,
            });
            continue;
        }
        let unindexed = dataset
            .unindexed_fragments(intent.name)
            .await
            .with_context(|| format!("unindexed_fragments failed for {}", table.label()))?;
        let unindexed_rows = unindexed
            .iter()
            .map(|fragment| fragment.num_rows().unwrap_or(0))
            .sum();
        statuses.push(IndexStatus {
            table,
            intent_name: intent.name.to_owned(),
            fragments_covered: total_fragments.saturating_sub(unindexed.len()),
            unindexed_rows,
            exists,
        });
    }
    Ok(statuses)
}

/// Open the table at `table_name` via the namespace; create + initialize on
/// `TableNotFound`. Schema-checks the on-disk dataset against pond's
/// expectation so a stale data dir surfaces early.
///
/// Probes via `nm.describe_table` directly rather than `DatasetBuilder::from_namespace`:
/// the builder re-wraps an already-`Namespace`-wrapped error
/// (lance/src/dataset/builder.rs:142), so going through it would force a
/// chain-walk to classify `TableNotFound`. The direct probe stays at one
/// wrap level and downcasts cleanly. Managed-versioning hookup (REST
/// namespace external-manifest commits) is not wired here; v1 ships
/// Directory v2 only.
async fn open_or_create_via_ns(
    nm: &Arc<dyn LanceNamespace>,
    nm_ident: &NamespaceIdent,
    table_name: &str,
    schema: lance::deps::arrow_schema::SchemaRef,
    session: &Arc<Session>,
    storage_options: &HashMap<String, String>,
) -> Result<Dataset> {
    let table_id = nm_ident.as_table_id(table_name);

    let request = DescribeTableRequest {
        id: Some(table_id.clone()),
        ..Default::default()
    };
    match nm.describe_table(request).await {
        Ok(response) => {
            let location = response.location.with_context(|| {
                format!("namespace returned no location for table {table_name}")
            })?;
            let mut builder = DatasetBuilder::from_uri(&location).with_session(session.clone());
            if !storage_options.is_empty() {
                builder = builder.with_storage_options(storage_options.clone());
            }
            let dataset = builder
                .load()
                .await
                .with_context(|| format!("failed to open table {table_name}"))?;
            ensure_schema_matches(&dataset, schema.as_ref(), table_name)?;
            return Ok(dataset);
        }
        Err(error) => match &error {
            lance::Error::Namespace { source, .. }
                if matches!(
                    source.downcast_ref::<NamespaceError>(),
                    Some(NamespaceError::TableNotFound { .. })
                ) =>
            {
                // fall through to create
            }
            _ => {
                return Err(anyhow::Error::from(error))
                    .with_context(|| format!("failed to describe table {table_name}"));
            }
        },
    }

    // Create path: pond seeds an empty dataset with the canonical schema so
    // every subsequent open lands on a real Lance dataset, not a phantom.
    let mut write_params = sessions::write_params_for_create();
    write_params.session = Some(session.clone());
    write_params.mode = WriteMode::Create;
    if !storage_options.is_empty() {
        write_params.store_params = Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                storage_options.clone(),
            ))),
            ..Default::default()
        });
    }
    let reader = sessions::empty_reader(schema)?;
    Dataset::write_into_namespace(reader, nm.clone(), table_id, Some(write_params))
        .await
        .with_context(|| format!("failed to create table {table_name}"))
}

fn scanner_with_prefilter(
    dataset: &Dataset,
    predicate: Option<&Predicate>,
) -> Result<lance::dataset::scanner::Scanner> {
    let mut scanner = dataset.scan();
    scanner.prefilter(true);
    if let Some(predicate) = predicate {
        let filter = predicate.to_lance();
        if !filter.is_empty() {
            scanner.filter(&filter)?;
        }
    }
    Ok(scanner)
}
fn ensure_schema_matches(
    dataset: &Dataset,
    expected: &lance::deps::arrow_schema::Schema,
    table_name: &str,
) -> Result<()> {
    use lance::deps::arrow_schema::DataType;
    use std::collections::BTreeSet;
    let actual = lance::deps::arrow_schema::Schema::from(dataset.schema());
    let actual_names: BTreeSet<&str> = actual.fields().iter().map(|f| f.name().as_str()).collect();
    let expected_names: BTreeSet<&str> = expected
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    if actual_names != expected_names {
        anyhow::bail!(
            "table {table_name} has columns {actual_names:?} but this pond build expects \
             {expected_names:?} - the on-disk store predates a schema change; delete the \
             data directory and re-run `pond ingest`",
        );
    }
    // Catch a vector-dim change (configured `[embeddings].dim` differs from
    // the on-disk vector column width) early with a friendly message. Lance
    // would otherwise reject the next write with an opaque schema-mismatch
    // error inside the `merge_update` path.
    for actual_field in actual.fields() {
        let Some(expected_field) = expected.field_with_name(actual_field.name()).ok() else {
            continue;
        };
        if let (DataType::FixedSizeList(_, actual_dim), DataType::FixedSizeList(_, expected_dim)) =
            (actual_field.data_type(), expected_field.data_type())
            && actual_dim != expected_dim
        {
            tracing::warn!(
                table = table_name,
                column = actual_field.name(),
                actual_dim,
                expected_dim,
                "embedding dimension differs from config; open proceeds because model swaps are operator-driven",
            );
        }
    }
    Ok(())
}
/// Object-store defaults injected for any non-local pond location. Each key
/// is only set when neither the user-provided key nor its env-var-form alias
/// is already present, so explicit overrides in `[storage]` always win.
/// `aws_unsigned_payload` is gated on a custom endpoint (the marker for
/// S3-compatible stores like Hetzner, MinIO, R2), where the SHA256 payload
/// signature is wasted work the server does not validate.
fn apply_remote_storage_defaults(options: &mut HashMap<String, String>) {
    fn set_default(options: &mut HashMap<String, String>, aliases: &[&str], value: &str) {
        if aliases
            .iter()
            .any(|alias| options.keys().any(|k| k.eq_ignore_ascii_case(alias)))
        {
            return;
        }
        options.insert(aliases[0].to_owned(), value.to_owned());
    }
    set_default(options, &["pool_idle_timeout"], "300 seconds");
    set_default(options, &["connect_timeout"], "10 seconds");
    let has_custom_endpoint = ["aws_endpoint", "endpoint"]
        .iter()
        .any(|alias| options.keys().any(|k| k.eq_ignore_ascii_case(alias)));
    if has_custom_endpoint {
        set_default(
            options,
            &["aws_unsigned_payload", "unsigned_payload"],
            "true",
        );
    }
}

fn quoted_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn like_contains(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''");
    format!("'%{escaped}%'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Round-trip: opening a fresh data dir through `lance-namespace`
    /// produces all three tables, and `Handle::scan` returns an empty batch
    /// for each (no spurious schema mismatch, no namespace error).
    #[tokio::test]
    async fn store_opens_via_namespace_and_scan_works() -> Result<()> {
        let temp = TempDir::new()?;
        let url = Url::from_directory_path(temp.path())
            .map_err(|()| anyhow::anyhow!("temp path is not absolute"))?;
        let handle = Handle::open(&url).await?;
        // Each table has its own PK column; project the canonical one so the
        // scan is exercised end-to-end (catalog -> dataset -> scanner -> batch).
        let cases: [(Table, &[&str]); 3] = [
            (Table::Sessions, &["id"]),
            (Table::Messages, &["id"]),
            (Table::Parts, &["id"]),
        ];
        for (table, projection) in cases {
            let scanner = handle
                .scan(table, ScanOpts::project_only(projection))
                .await?;
            let batch = scanner.try_into_batch().await?;
            assert_eq!(batch.num_rows(), 0, "fresh table should be empty");
        }
        Ok(())
    }
}
