use crate::{
    RetryPolicy,
    config::StorageLocation,
    sessions::{self},
};
use anyhow::{Context, Result};
use lance::Dataset;
use lance::dataset::MergeInsertBuilder;
use lance::deps::arrow_array::{RecordBatch, RecordBatchIterator};
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
pub const VECTOR_INDEX_ACTIVATION_ROWS: usize = 10_000;
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
    pub async fn open(location: impl Into<StorageLocation>) -> Result<Self> {
        let location = location.into();
        if let Some(path) = location.local_path() {
            tokio::fs::create_dir_all(path)
                .await
                .with_context(|| format!("failed to create data dir {}", path.display()))?;
        }
        let refresh_after = Duration::from_millis(250);
        Ok(Self {
            datasets: DatasetSet {
                sessions: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        &location,
                        sessions::SESSIONS,
                        sessions::session_schema(),
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                messages: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        &location,
                        sessions::MESSAGES,
                        sessions::message_schema(),
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                parts: Mutex::new(CachedDataset {
                    dataset: open_or_create(&location, sessions::PARTS, sessions::part_schema())
                        .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                embeddings: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        &location,
                        sessions::EMBEDDINGS,
                        sessions::embedding_schema(),
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
            },
            retry: RetryPolicy::default(),
        })
    }
    pub async fn row_counts(&self) -> Result<(usize, usize, usize, usize)> {
        Ok((
            self.count_rows(Table::Sessions, None).await?,
            self.count_rows(Table::Messages, None).await?,
            self.count_rows(Table::Parts, None).await?,
            self.count_rows(Table::Embeddings, None).await?,
        ))
    }
    pub async fn merge_insert(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
    ) -> Result<u64> {
        if row_count == 0 {
            return Ok(0);
        }
        self.retry_lance(table.label(), || async {
            let mut cached = self.cached(table).lock().await;
            let existing = cached.latest().await?;
            let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
            let (dataset, stats) = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?
                .try_build()?
                .execute_reader(Box::new(reader))
                .await?;
            cached.replace(dataset.as_ref().clone());
            Ok(stats.num_inserted_rows)
        })
        .await
    }
    pub async fn dataset(&self, table: Table) -> Result<Dataset> {
        let mut cached = self.cached(table).lock().await;
        cached.latest().await
    }
    pub async fn scan_batch(
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
    pub async fn ensure_scalar_index(
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
    pub async fn ensure_index(
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
                    return Err(error);
                }
            }
        }
    }
    fn backoff(&self, attempt: u8) -> Duration {
        let shift = u32::from(attempt.saturating_sub(1));
        let multiplier = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let base = self.retry.initial_backoff.saturating_mul(multiplier);
        base.min(self.retry.max_backoff)
    }
}
async fn open_or_create(
    location: &StorageLocation,
    suffix: &str,
    schema: Arc<lance::deps::arrow_schema::Schema>,
) -> Result<Dataset> {
    let uri = location.child_uri(suffix);
    match location {
        StorageLocation::LocalPath(base) => {
            let path = base.join(suffix);
            if path.exists() {
                let dataset = Dataset::open(&uri)
                    .await
                    .with_context(|| format!("failed to open dataset {uri}"))?;
                ensure_schema_matches(&dataset, &schema, &uri)?;
                Ok(dataset)
            } else {
                let reader = sessions::empty_reader(schema)?;
                Dataset::write(reader, &uri, Some(sessions::write_params()))
                    .await
                    .with_context(|| format!("failed to create dataset {uri}"))
            }
        }
        StorageLocation::Uri(_) => match Dataset::open(&uri).await {
            Ok(dataset) => {
                ensure_schema_matches(&dataset, &schema, &uri)?;
                Ok(dataset)
            }
            Err(open_err) => {
                let reader = sessions::empty_reader(schema)?;
                Dataset::write(reader, &uri, Some(sessions::write_params()))
                    .await
                    .with_context(|| {
                        format!("failed to open or create dataset {uri} (open error: {open_err})")
                    })
            }
        },
    }
}
pub fn scanner_with_prefilter(
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
