use crate::{
    RetryPolicy,
    config::{self},
    sessions::{self},
};
use anyhow::{Context, Result};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{MergeInsertBuilder, WhenMatched};
use lance::deps::arrow_array::{RecordBatch, RecordBatchIterator};
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use url::Url;
pub const VECTOR_INDEX_ACTIVATION_ROWS: usize = 10_000;

/// Anyhow-chain sentinel pond attaches when `retry_lance` exhausts attempts
/// against an OCC commit-conflict failure (design.md 3.6.1). The wire layer
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub versions_removed: u64,
    pub bytes_reclaimed: u64,
    pub tables_optimized: usize,
    pub tables_failed: usize,
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
    IsNull(&'static str),
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
}
impl Predicate {
    pub fn to_lance(&self) -> String {
        match self {
            Self::Eq(column, value) => format!("{column} = {}", value.to_lance()),
            Self::IsNull(column) => format!("{column} IS NULL"),
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
#[derive(Debug)]
pub struct Handle {
    datasets: DatasetSet,
    retry: RetryPolicy,
    /// One `lance::Session` shared across all four datasets. Carries the
    /// metadata + index caches and the `ObjectStoreRegistry` (which holds
    /// the underlying object_store / S3 client). Sharing the session means
    /// one cache pool covers all four tables and one S3 client serves all
    /// four datasets - load-bearing on object-store backends where a
    /// per-dataset client would mean 4x the connection pools and 4x the
    /// credential refreshes (lance/src/dataset/builder.rs:509-517).
    #[allow(dead_code)]
    session: Arc<Session>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Sessions,
    Messages,
    Parts,
    Embeddings,
}
impl Table {
    fn label(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Messages => "messages",
            Self::Parts => "parts",
            Self::Embeddings => "embeddings",
        }
    }
}
#[derive(Debug)]
struct DatasetSet {
    sessions: Mutex<CachedDataset>,
    messages: Mutex<CachedDataset>,
    parts: Mutex<CachedDataset>,
    embeddings: Mutex<CachedDataset>,
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
    /// `config.toml` carries an `[storage]` block (design.md 3.2.0 storage
    /// block / 3.6 "Recovery model").
    pub async fn open_with_options(
        location: &Url,
        storage_options: HashMap<String, String>,
    ) -> Result<Self> {
        if let Some(path) = config::local_path(location) {
            tokio::fs::create_dir_all(&path)
                .await
                .with_context(|| format!("failed to create data dir {}", path.display()))?;
        }
        // One Session shared across all four datasets so metadata/index
        // caches and the object_store registry (and thus any S3 client) are
        // pooled rather than duplicated four times. `Session::default()`
        // ships sensible cache capacities (lance/src/dataset.rs:149,153)
        // and a default ObjectStoreRegistry that knows file/s3/gs/az.
        let session = Arc::new(Session::default());
        // design.md 2.3 inv 4: refresh window is scheme-keyed. Local-FS
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
                    dataset: open_or_create(
                        location,
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
                    dataset: open_or_create(
                        location,
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
                    dataset: open_or_create(
                        location,
                        sessions::PARTS,
                        sessions::part_schema(),
                        &session,
                        &storage_options,
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                embeddings: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        location,
                        sessions::EMBEDDINGS,
                        sessions::embedding_schema(),
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

    pub async fn row_counts(&self) -> Result<(usize, usize, usize, usize)> {
        Ok((
            self.count_rows(Table::Sessions, None).await?,
            self.count_rows(Table::Messages, None).await?,
            self.count_rows(Table::Parts, None).await?,
            self.count_rows(Table::Embeddings, None).await?,
        ))
    }
    pub(crate) async fn merge_insert(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
    ) -> Result<u64> {
        if row_count == 0 {
            return Ok(0);
        }
        let label = table.label();
        let started = Instant::now();
        // Level 2 self-heal (design.md 2.3 invariant): rows that already
        // exist (matched by the schema's `lance-schema:unenforced-primary-key`
        // columns) have their non-PK fields *refreshed* from source on
        // every sync. Insertions still happen as before. This means a bug
        // in an earlier pond version that wrote stale field values gets
        // corrected on the next `pond sync` against the same source -
        // without re-syncing requiring a wipe, and without the operator
        // having to think about which version of pond wrote which row.
        //
        // Adapters must never produce a *subset* of what a prior version
        // produced for the same source - that invariant is what makes the
        // omission of `when_not_matched_by_source` safe (no orphan-purge
        // step needed). See design.md 2.3.
        //
        // Embeddings are excluded: their data column is the computed vector
        // and re-running `pond sync` should never silently re-emit them.
        // `pond embed` is the only owner of that table's data.
        let when_matched = match table {
            Table::Sessions | Table::Messages | Table::Parts => WhenMatched::UpdateAll,
            Table::Embeddings => WhenMatched::DoNothing,
        };
        let result = self
            .retry_lance(label, || async {
                let mut cached = self.cached(table).lock().await;
                let existing = cached.latest().await?;
                let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
                let mut builder = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?;
                builder.when_matched(when_matched.clone());
                // pond's ingest contract is idempotent at the PK: callers may
                // present the same row more than once in a single batch and
                // the substrate keeps the first occurrence. Lance's default
                // is to fail; FirstSeen aligns it with the contract.
                builder.source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
                let (dataset, stats) = builder
                    .try_build()?
                    .execute_reader(Box::new(reader))
                    .await?;
                cached.replace(dataset.as_ref().clone());
                Ok((stats.num_inserted_rows, stats.num_skipped_duplicates))
            })
            .await;
        let skipped = result.as_ref().map(|(_, s)| *s).unwrap_or(0);
        tracing::info!(
            target: "pond::perf",
            table = %label,
            rows = row_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            skipped,
            "merge_insert"
        );
        result.map(|(inserted, _)| inserted)
    }
    pub(crate) async fn dataset(&self, table: Table) -> Result<Dataset> {
        let mut cached = self.cached(table).lock().await;
        cached.latest().await
    }
    pub(crate) async fn scan_batch(
        &self,
        table: Table,
        predicate: Option<&Predicate>,
        projection: &[&str],
    ) -> Result<RecordBatch> {
        let dataset = self.dataset(table).await?;
        let mut scanner = scanner_with_prefilter(&dataset, predicate)?;
        if !projection.is_empty() {
            scanner.project(projection)?;
        }
        scanner.try_into_batch().await.context("scan failed")
    }
    pub async fn count_rows(&self, table: Table, predicate: Option<String>) -> Result<usize> {
        self.dataset(table)
            .await?
            .count_rows(predicate)
            .await
            .map_err(Into::into)
    }
    pub(crate) async fn ensure_scalar_index(
        &self,
        table: Table,
        column: &str,
        kind: &BuiltinIndexType,
        name: &str,
    ) -> Result<()> {
        let index_type = match kind {
            BuiltinIndexType::Bitmap => IndexType::Bitmap,
            _ => IndexType::BTree,
        };
        let params = ScalarIndexParams::for_builtin(kind.clone());
        self.ensure_index(table, column, name, index_type, &params)
            .await
    }
    pub(crate) async fn ensure_index(
        &self,
        table: Table,
        column: &str,
        name: &str,
        index_type: IndexType,
        params: &dyn lance::index::IndexParams,
    ) -> Result<()> {
        let mut guard = self.cached(table).lock().await;
        let mut dataset = guard.latest().await?;
        let existing = dataset.load_indices().await?;
        if existing.iter().any(|index| index.name == name) {
            return Ok(());
        }
        tracing::info!(index = name, column, "creating Lance index");
        dataset
            .create_index(&[column], index_type, Some(name.to_owned()), params, false)
            .await
            .with_context(|| format!("failed to create index {name}"))?;
        guard.replace(dataset);
        Ok(())
    }
    pub async fn embedding_index_names(&self) -> Result<Vec<String>> {
        let dataset = self.dataset(Table::Embeddings).await?;
        let indices = dataset.load_indices().await?;
        Ok(indices.iter().map(|index| index.name.clone()).collect())
    }
    pub async fn maintenance(
        &self,
        retention: chrono::Duration,
        skip_cleanup: bool,
        skip_optimize: bool,
    ) -> MaintenanceReport {
        let mut report = MaintenanceReport::default();
        for table in [
            Table::Sessions,
            Table::Messages,
            Table::Parts,
            Table::Embeddings,
        ] {
            match self
                .maintain_table(table, retention, skip_cleanup, skip_optimize)
                .await
            {
                Ok((versions_removed, bytes_reclaimed)) => {
                    report.versions_removed += versions_removed;
                    report.bytes_reclaimed += bytes_reclaimed;
                    report.tables_optimized += 1;
                }
                Err(error) => {
                    report.tables_failed += 1;
                    tracing::warn!(table = table.label(), %error, "maintenance pass failed for table");
                }
            }
        }
        report
    }
    async fn maintain_table(
        &self,
        table: Table,
        retention: chrono::Duration,
        skip_cleanup: bool,
        skip_optimize: bool,
    ) -> Result<(u64, u64)> {
        let started = Instant::now();
        let mut guard = self.cached(table).lock().await;
        let mut dataset = guard.latest().await?;
        let (versions_removed, bytes_reclaimed) = if skip_cleanup {
            (0, 0)
        } else {
            let stats = dataset
                .cleanup_old_versions(retention, None, None)
                .await
                .with_context(|| format!("cleanup_old_versions failed for {}", table.label()))?;
            (stats.old_versions, stats.bytes_removed)
        };
        if !skip_optimize {
            dataset
                .optimize_indices(&OptimizeOptions::default())
                .await
                .with_context(|| format!("optimize_indices failed for {}", table.label()))?;
        }
        guard.replace(dataset);
        tracing::info!(
            table = table.label(),
            versions_removed,
            bytes_reclaimed,
            duration_ms = started.elapsed().as_millis(),
            "maintenance pass complete for table",
        );
        Ok((versions_removed, bytes_reclaimed))
    }
    fn cached(&self, table: Table) -> &Mutex<CachedDataset> {
        match table {
            Table::Sessions => &self.datasets.sessions,
            Table::Messages => &self.datasets.messages,
            Table::Parts => &self.datasets.parts,
            Table::Embeddings => &self.datasets.embeddings,
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
                    // design.md 3.6.1: surface OCC failures as a typed `conflict`
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
        // a contended manifest (design.md 2.3 inv 3); clamped to `max_backoff`.
        let factor = (1.0 + self.retry.jitter * (fastrand::f64() * 2.0 - 1.0)).max(0.0);
        base.mul_f64(factor).min(self.retry.max_backoff)
    }
}
async fn open_or_create(
    location: &Url,
    suffix: &str,
    schema: Arc<lance::deps::arrow_schema::Schema>,
    session: &Arc<Session>,
    storage_options: &HashMap<String, String>,
) -> Result<Dataset> {
    let uri = config::child_uri(location, suffix);
    let mut write_params = sessions::write_params(location);
    // Tie new-dataset writes to the shared Session so the created dataset
    // inherits the same caches + ObjectStoreRegistry the open path uses.
    write_params.session = Some(session.clone());
    // For object-store backends pond hands raw `storage_options` (S3 creds,
    // region, endpoint, ...) verbatim to Lance via the `ObjectStoreParams`
    // accessor (lance/src/dataset/builder.rs:305 doc). Empty map = use the
    // session's default registry (env-var-driven object_store).
    if !storage_options.is_empty() {
        write_params.store_params = Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                storage_options.clone(),
            ))),
            ..Default::default()
        });
    }
    if let Some(local_base) = config::local_path(location) {
        // Local-FS fast path: a plain `Path::exists` check keeps the
        // missing-dataset branch cheap and surfaces real open errors as
        // open errors (not as misclassified "needs create" cases).
        let path = local_base.join(suffix);
        if path.exists() {
            let dataset = open_with_session(&uri, session, storage_options).await?;
            ensure_schema_matches(&dataset, &schema, &uri)?;
            Ok(dataset)
        } else {
            let reader = sessions::empty_reader(schema)?;
            Dataset::write(reader, &uri, Some(write_params))
                .await
                .with_context(|| format!("failed to create dataset {uri}"))
        }
    } else {
        // Object-store path: no portable cheap "exists" predicate, so try
        // open first and fall back to write. If write also fails we surface
        // both errors so the operator sees the underlying transport problem
        // rather than a misleading "already exists" from the create attempt.
        match open_with_session(&uri, session, storage_options).await {
            Ok(dataset) => {
                ensure_schema_matches(&dataset, &schema, &uri)?;
                Ok(dataset)
            }
            Err(open_err) => {
                let reader = sessions::empty_reader(schema)?;
                Dataset::write(reader, &uri, Some(write_params))
                    .await
                    .with_context(|| {
                        format!("failed to open or create dataset {uri} (open error: {open_err})")
                    })
            }
        }
    }
}

/// Open a dataset bound to the shared `Session` and any object-store options.
/// Routes through `DatasetBuilder::with_session` + `with_storage_options`
/// rather than `Dataset::open(&str)` so the returned dataset reuses the
/// pooled metadata/index caches and the `ObjectStoreRegistry` (which holds
/// the S3/local object_store client).
async fn open_with_session(
    uri: &str,
    session: &Arc<Session>,
    storage_options: &HashMap<String, String>,
) -> Result<Dataset> {
    let mut builder = DatasetBuilder::from_uri(uri).with_session(session.clone());
    if !storage_options.is_empty() {
        builder = builder.with_storage_options(storage_options.clone());
    }
    builder
        .load()
        .await
        .with_context(|| format!("failed to open dataset {uri}"))
}
pub(crate) fn scanner_with_prefilter(
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
    uri: &str,
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
            "dataset {uri} has columns {actual_names:?} but this pond build expects \
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
