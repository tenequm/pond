use crate::{
    RetryPolicy,
    config::{self},
    handlers::NamespaceIdent,
    sessions::{self},
};
use anyhow::{Context, Result};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode};
use lance::deps::arrow_array::{RecordBatch, RecordBatchIterator};
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
};
use lance_namespace::LanceNamespace;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::DescribeTableRequest;
use lance_namespace_impls::ConnectBuilder;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use url::Url;
/// Embedded-row count at which pond builds the IVF_PQ vector index on
/// `messages.vector` (spec.md#search). Below it, vector search runs a
/// brute-force flat scan - exact and fast at small and medium scale, and
/// IVF_PQ cannot train well on fewer vectors anyway.
pub const VECTOR_INDEX_ACTIVATION_ROWS: usize = 100_000;

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
    parts: Mutex<CachedDataset>,
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
    /// Open without storage options (local FS or backends that don't need
    /// auth). Most tests and the no-config CLI path land here.
    pub async fn open(location: &Url) -> Result<Self> {
        Self::open_with_options(location, HashMap::new()).await
    }

    /// Open with object-store options handed through to Lance verbatim.
    /// Keys are the `object_store` crate's standard config names; pond does
    /// not parse them. Used by `pond serve --data-dir s3://...` once
    /// `config.toml` carries an `[storage]` block (spec.md#substrate).
    pub async fn open_with_options(
        location: &Url,
        storage_options: HashMap<String, String>,
    ) -> Result<Self> {
        if let Some(path) = config::local_path(location) {
            tokio::fs::create_dir_all(&path)
                .await
                .with_context(|| format!("failed to create data dir {}", path.display()))?;
        }
        // One Session shared across all three datasets so metadata/index
        // caches and the object_store registry (and thus any S3 client) are
        // pooled rather than duplicated three times. `Session::default()`
        // ships sensible cache capacities (lance/src/dataset.rs:149,153)
        // and a default ObjectStoreRegistry that knows file/s3/gs/az.
        let session = Arc::new(Session::default());
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
        Ok(Self {
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
                parts: Mutex::new(CachedDataset {
                    dataset: open_or_create_via_ns(
                        &nm,
                        &nm_ident,
                        sessions::PARTS,
                        sessions::part_schema(),
                        &session,
                        &storage_options,
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
            },
            retry: RetryPolicy::default(),
            session,
            nm,
            nm_ident,
            storage_options,
            location: location.clone(),
        })
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

    /// Insert-only merge: append new rows, never overwrite a matched PK
    /// (`WhenMatched::DoNothing`). pond is durable storage, not a
    /// source-derived cache; sync fills gaps. Returns rows inserted.
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

    /// Update-only merge: a partial-schema source sets its columns on every
    /// matched PK (`WhenMatched::UpdateAll`) and unmatched source rows are
    /// dropped, never inserted. `pond embed` uses this to fill `vector` and
    /// `embedding_model` on existing `messages` rows. Returns rows updated.
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
                let mut cached = self.cached(table).lock().await;
                let existing = cached.latest().await?;
                let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
                let mut builder = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?;
                builder.when_matched(when_matched.clone());
                builder.when_not_matched(when_not_matched.clone());
                // pond presents each PK at most once per batch; FirstSeen keeps
                // the first occurrence rather than failing (Lance's default).
                builder.source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
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
    pub(crate) async fn dataset(&self, table: Table) -> Result<Dataset> {
        let mut cached = self.cached(table).lock().await;
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
    pub(crate) async fn ensure_scalar_index(
        &self,
        table: Table,
        column: &str,
        kind: &BuiltinIndexType,
        name: &str,
        replace: bool,
    ) -> Result<()> {
        let index_type = match kind {
            BuiltinIndexType::Bitmap => IndexType::Bitmap,
            _ => IndexType::BTree,
        };
        let params = ScalarIndexParams::for_builtin(kind.clone());
        self.ensure_index(table, column, name, index_type, &params, replace)
            .await
    }
    /// Create `name` on `table`.`column`. With `replace = false` the build is
    /// skipped when an index of that name already exists; `replace = true`
    /// forces a full rebuild - the `pond sync --reindex` recovery path.
    pub(crate) async fn ensure_index(
        &self,
        table: Table,
        column: &str,
        name: &str,
        index_type: IndexType,
        params: &dyn lance::index::IndexParams,
        replace: bool,
    ) -> Result<()> {
        let mut guard = self.cached(table).lock().await;
        let mut dataset = guard.latest().await?;
        if !replace {
            let existing = dataset.load_indices().await?;
            if existing.iter().any(|index| index.name == name) {
                return Ok(());
            }
        }
        tracing::info!(index = name, column, replace, "creating Lance index");
        dataset
            .create_index(
                &[column],
                index_type,
                Some(name.to_owned()),
                params,
                replace,
            )
            .await
            .with_context(|| format!("failed to create index {name}"))?;
        guard.replace(dataset);
        Ok(())
    }
    /// Names of every index on `messages` - the vector-index tests read this.
    #[cfg(test)]
    pub(crate) async fn messages_index_names(&self) -> Result<Vec<String>> {
        let dataset = self.dataset(Table::Messages).await?;
        let indices = dataset.load_indices().await?;
        Ok(indices.iter().map(|index| index.name.clone()).collect())
    }

    /// Count rows in `table` not yet covered by index `index_name`
    /// (spec.md#index-upkeep). Manifest-only, no index I/O; a missing index
    /// reports the whole table.
    pub(crate) async fn unindexed_row_count(
        &self,
        table: Table,
        index_name: &str,
    ) -> Result<usize> {
        use lance::index::DatasetIndexInternalExt;
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
    /// Fold newly-appended rows into `table`'s indexes incrementally
    /// (spec.md#index-upkeep). Never a from-scratch rebuild; idempotent and
    /// concurrency-safe, so it is safe to run on every write batch.
    pub(crate) async fn optimize_indices(&self, table: Table) -> Result<()> {
        let started = Instant::now();
        let mut guard = self.cached(table).lock().await;
        let mut dataset = guard.latest().await?;
        dataset
            .optimize_indices(&OptimizeOptions::default())
            .await
            .with_context(|| format!("optimize_indices failed for {}", table.label()))?;
        guard.replace(dataset);
        tracing::info!(
            table = table.label(),
            duration_ms = started.elapsed().as_millis(),
            "index fold complete for table",
        );
        Ok(())
    }

    /// Reclaim manifest versions older than `older_than` on `table` along with
    /// the orphaned data, index, transaction, and deletion files they uniquely
    /// reference. The current manifest is preserved unconditionally and Lance's
    /// `delete_unverified=false` floor protects files younger than ~7 days.
    /// `error_if_tagged_old_versions=false` matches Lance's auto-cleanup hook
    /// so a tagged old version downgrades the entry to a skip, not an error.
    pub(crate) async fn cleanup_old_versions(
        &self,
        table: Table,
        older_than: std::time::Duration,
    ) -> Result<lance::dataset::cleanup::RemovalStats> {
        let dataset = self.dataset(table).await?;
        let chrono_older_than = chrono::Duration::from_std(older_than)
            .with_context(|| format!("cleanup duration out of range: {older_than:?}"))?;
        dataset
            .cleanup_old_versions(chrono_older_than, None, Some(false))
            .await
            .with_context(|| format!("cleanup_old_versions failed for {}", table.label()))
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
    fn cached(&self, table: Table) -> &Mutex<CachedDataset> {
        match table {
            Table::Sessions => &self.datasets.sessions,
            Table::Messages => &self.datasets.messages,
            Table::Parts => &self.datasets.parts,
        }
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
                    tracing::warn!(label, attempt, ?backoff, %error, "retrying Lance operation");
                    tokio::time::sleep(backoff).await;
                }
                Err(error) => {
                    tracing::warn!(label, attempt, %error, "Lance operation exhausted retries");
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
    Ok(())
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
