use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use lance::Dataset;
use lance::dataset::MergeInsertBuilder;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::deps::arrow_array::{Float32Array, RecordBatchIterator};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_index::scalar::{
    BuiltinIndexType, FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams,
};
use lance_linalg::distance::MetricType;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

use crate::{
    config::{Distance, EmbeddingModel},
    datasets::{self, EmbeddingRow},
    types::{Message, Part, Session, StoredMessage, StoredSession},
};

/// Row-count threshold below which vector queries use a flat exact scan; at or
/// above it the IVF_PQ index is built and used (design.md 3.2.4).
pub const VECTOR_INDEX_ACTIVATION_ROWS: usize = 10_000;

/// A message awaiting embedding: its `search_text` plus the columns the
/// `embeddings` rows denormalize (design.md 3.2.4).
#[derive(Debug, Clone, PartialEq)]
pub struct PendingMessage {
    pub message_id: String,
    pub session_id: String,
    pub source_agent: String,
    pub project: Option<String>,
    pub role: String,
    pub timestamp: DateTime<Utc>,
    pub search_text: String,
}

/// Message metadata used to hydrate search hits after retriever ranking.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageMeta {
    pub message_id: String,
    pub session_id: String,
    pub role: String,
    pub project: Option<String>,
    pub source_agent: String,
    pub timestamp: DateTime<Utc>,
    pub search_text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub attempts: u8,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            initial_backoff: Duration::from_millis(300),
            max_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertStatus {
    Inserted,
    Matched,
}

#[derive(Debug, Clone, Copy)]
pub struct MessageWrite<'a> {
    pub message: &'a Message,
    pub parts: &'a [Part],
    pub search_text: Option<&'a str>,
}

#[derive(Debug)]
pub struct PondStore {
    datasets: DatasetSet,
    retry: RetryPolicy,
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

impl PondStore {
    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        tokio::fs::create_dir_all(data_dir)
            .await
            .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;

        let refresh_after = Duration::from_millis(250);
        Ok(Self {
            datasets: DatasetSet {
                sessions: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        data_dir.join(datasets::SESSIONS),
                        datasets::session_schema(),
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                messages: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        data_dir.join(datasets::MESSAGES),
                        datasets::message_schema(),
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                parts: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        data_dir.join(datasets::PARTS),
                        datasets::part_schema(),
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                embeddings: Mutex::new(CachedDataset {
                    dataset: open_or_create(
                        data_dir.join(datasets::EMBEDDINGS),
                        datasets::embedding_schema(),
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
            },
            retry: RetryPolicy::default(),
        })
    }

    pub async fn upsert_session(&self, session: &Session) -> Result<UpsertStatus> {
        let mut statuses = self.upsert_sessions(std::slice::from_ref(session)).await?;
        statuses
            .pop()
            .context("single session upsert returned no status")
    }

    pub async fn upsert_sessions(&self, sessions: &[Session]) -> Result<Vec<UpsertStatus>> {
        if sessions.is_empty() {
            return Ok(Vec::new());
        }
        let batch = datasets::sessions_batch(sessions)?;
        self.retry_lance("upsert_session", || async {
            let mut cached = self.datasets.sessions.lock().await;
            let existing = cached.latest().await?;
            let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
            let (dataset, stats) = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?
                .try_build()?
                .execute_reader(Box::new(reader))
                .await?;
            cached.replace(dataset.as_ref().clone());
            Ok(statuses_from_inserted(
                sessions.len(),
                stats.num_inserted_rows,
            ))
        })
        .await
    }

    pub async fn upsert_session_bundle(
        &self,
        session: &Session,
        messages: &[MessageWrite<'_>],
    ) -> Result<Vec<UpsertStatus>> {
        let mut statuses = self.upsert_sessions(std::slice::from_ref(session)).await?;
        statuses.extend(self.upsert_messages(session, messages).await?);

        let mut parts = Vec::with_capacity(messages.iter().map(|write| write.parts.len()).sum());
        for write in messages {
            parts.extend_from_slice(write.parts);
        }
        statuses.extend(self.upsert_parts(&parts).await?);

        Ok(statuses)
    }

    pub async fn upsert_message(
        &self,
        message: &Message,
        parts: &[Part],
        session: &Session,
        search_text: Option<&str>,
    ) -> Result<Vec<UpsertStatus>> {
        let mut statuses = self
            .upsert_messages(
                session,
                &[MessageWrite {
                    message,
                    parts,
                    search_text,
                }],
            )
            .await?;
        statuses.extend(self.upsert_parts(parts).await?);

        Ok(statuses)
    }

    pub async fn upsert_messages(
        &self,
        session: &Session,
        messages: &[MessageWrite<'_>],
    ) -> Result<Vec<UpsertStatus>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let rows = messages
            .iter()
            .map(|write| datasets::MessageBatchRow {
                message: write.message,
                source_agent: &session.source_agent,
                project: session.project.as_deref(),
                search_text: write.search_text,
            })
            .collect::<Vec<_>>();
        let batch = datasets::messages_batch(&rows)?;

        self.retry_lance("upsert_messages", || async {
            let mut cached = self.datasets.messages.lock().await;
            let existing = cached.latest().await?;
            let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
            let (dataset, stats) = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?
                .try_build()?
                .execute_reader(Box::new(reader))
                .await?;
            cached.replace(dataset.as_ref().clone());
            Ok(statuses_from_inserted(
                messages.len(),
                stats.num_inserted_rows,
            ))
        })
        .await
    }

    pub async fn upsert_parts(&self, parts: &[Part]) -> Result<Vec<UpsertStatus>> {
        if parts.is_empty() {
            return Ok(Vec::new());
        }
        let batch = datasets::parts_batch(parts)?;
        self.retry_lance("upsert_parts", || async {
            let mut cached = self.datasets.parts.lock().await;
            let existing = cached.latest().await?;
            let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
            let (dataset, stats) = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?
                .try_build()?
                .execute_reader(Box::new(reader))
                .await?;
            cached.replace(dataset.as_ref().clone());
            Ok(statuses_from_inserted(parts.len(), stats.num_inserted_rows))
        })
        .await
    }

    pub async fn upsert_part(&self, part: &Part) -> Result<UpsertStatus> {
        let mut statuses = self.upsert_parts(std::slice::from_ref(part)).await?;
        statuses
            .pop()
            .context("single part upsert returned no status")
    }

    pub async fn get_session(&self, session_id: &str) -> Result<Option<StoredSession>> {
        let Some(session) = self.find_session(session_id).await? else {
            return Ok(None);
        };
        let messages = self.messages_for_session(session_id).await?;
        Ok(Some(StoredSession { session, messages }))
    }

    /// Every session id currently in the store, unsorted.
    pub async fn session_ids(&self) -> Result<Vec<String>> {
        let mut cached = self.datasets.sessions.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.project(&["id"])?;
        let batch = scanner.try_into_batch().await?;
        let mut ids = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if let Some(id) = datasets::string(&batch, "id", row)? {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    pub async fn get_message_context(
        &self,
        message_id: &str,
        context_depth: usize,
    ) -> Result<Option<(Session, Vec<StoredMessage>)>> {
        let Some(target) = self.find_message(message_id).await? else {
            return Ok(None);
        };
        let session_id = target.session_id().to_owned();
        let session = self.find_session(&session_id).await?.with_context(|| {
            format!("message {message_id} references missing session {session_id}")
        })?;
        let session_messages = self.messages_for_session(&session_id).await?;

        let target_pos = session_messages
            .iter()
            .position(|message| message.message.id() == message_id)
            .unwrap_or_default();
        let start = target_pos.saturating_sub(context_depth);
        let end = (target_pos + context_depth + 1).min(session_messages.len());
        Ok(Some((session, session_messages[start..end].to_vec())))
    }

    pub async fn row_counts(&self) -> Result<(usize, usize, usize, usize)> {
        let mut sessions = self.datasets.sessions.lock().await;
        let mut messages = self.datasets.messages.lock().await;
        let mut parts = self.datasets.parts.lock().await;
        let mut embeddings = self.datasets.embeddings.lock().await;
        Ok((
            sessions.latest().await?.count_rows(None).await?,
            messages.latest().await?.count_rows(None).await?,
            parts.latest().await?.count_rows(None).await?,
            embeddings.latest().await?.count_rows(None).await?,
        ))
    }

    /// Merge-insert embedding rows keyed on `(message_id, model_id)`.
    /// Re-running over already-embedded messages is a no-op for matched rows.
    pub async fn upsert_embeddings(&self, rows: &[EmbeddingRow]) -> Result<Vec<UpsertStatus>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let batch = datasets::embeddings_batch(rows)?;
        self.retry_lance("upsert_embeddings", || async {
            let mut cached = self.datasets.embeddings.lock().await;
            let existing = cached.latest().await?;
            let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
            let (dataset, stats) = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?
                .try_build()?
                .execute_reader(Box::new(reader))
                .await?;
            cached.replace(dataset.as_ref().clone());
            Ok(statuses_from_inserted(rows.len(), stats.num_inserted_rows))
        })
        .await
    }

    /// The set of `message_id`s that already have `embeddings` rows for
    /// `model_id`. IDs only - the large `search_text` payload is never
    /// materialized here, so peak memory is the id-set, not the corpus. The
    /// embed worker anti-joins this against [`pending_messages_stream`].
    pub async fn embedded_message_ids(&self, model_id: &str) -> Result<HashSet<String>> {
        let mut cached = self.datasets.embeddings.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.filter(&format!("model_id = {}", sql_string(model_id)))?;
        scanner.project(&["message_id"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut set = HashSet::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                if let Some(id) = datasets::string(&batch, "message_id", row)? {
                    set.insert(id);
                }
            }
        }
        Ok(set)
    }

    /// A streaming scan over `messages` rows with non-NULL `search_text`,
    /// projecting exactly the columns [`PendingMessage`] needs. The returned
    /// stream owns its execution plan; the dataset mutex is released before the
    /// caller iterates, so the embed worker holds no lock while embedding and
    /// never materializes the whole corpus at once.
    pub async fn pending_messages_stream(&self) -> Result<DatasetRecordBatchStream> {
        let mut cached = self.datasets.messages.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.filter("search_text IS NOT NULL")?;
        scanner.project(&[
            "id",
            "session_id",
            "source_agent",
            "project",
            "role",
            "timestamp",
            "search_text",
        ])?;
        scanner
            .try_into_stream()
            .await
            .context("failed to open messages stream")
    }

    /// BM25 full-text retriever over `messages.search_text`. Returns
    /// `(message_id, bm25_score)` pairs, higher score better. `filter` is the
    /// pushed-down scalar predicate (empty string for no filter).
    pub async fn fts_search(
        &self,
        query: &str,
        limit: usize,
        filter: &str,
    ) -> Result<Vec<(String, f32)>> {
        let mut cached = self.datasets.messages.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.prefilter(true);
        if !filter.is_empty() {
            scanner.filter(filter)?;
        }
        scanner.full_text_search(
            FullTextSearchQuery::new(query.to_owned()).with_column("search_text".to_owned())?,
        )?;
        scanner.project(&["id"])?;
        scanner.limit(Some(i64::try_from(limit).unwrap_or(i64::MAX)), None)?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let id = datasets::string(&batch, "id", row)?.context("fts hit id is null")?;
            hits.push((id, datasets::float32(&batch, "_score", row)?));
        }
        Ok(hits)
    }

    /// Vector kNN retriever over `embeddings.vector`. Returns
    /// `(message_id, distance)` pairs, lower distance better. Each message has
    /// exactly one vector, so the returned `message_id`s are distinct.
    pub async fn vector_search(
        &self,
        query: &[f32],
        limit: usize,
        filter: &str,
    ) -> Result<Vec<(String, f32)>> {
        let mut cached = self.datasets.embeddings.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.prefilter(true);
        if !filter.is_empty() {
            scanner.filter(filter)?;
        }
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        scanner.project(&["message_id"])?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let id =
                datasets::string(&batch, "message_id", row)?.context("vector hit id is null")?;
            hits.push((id, datasets::float32(&batch, "_distance", row)?));
        }
        Ok(hits)
    }

    /// The DataFusion plan string for a filtered hybrid scan. Used by the
    /// load-bearing prefilter-pushdown test (design.md 3.3): the scalar predicate
    /// must appear as a `ScalarIndexQuery` node, not a top-level `FilterExec`.
    pub async fn explain_vector_plan(
        &self,
        query: &[f32],
        limit: usize,
        filter: &str,
    ) -> Result<String> {
        let mut cached = self.datasets.embeddings.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.prefilter(true);
        if !filter.is_empty() {
            scanner.filter(filter)?;
        }
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        scanner
            .explain_plan(true)
            .await
            .context("explain_plan failed")
    }

    /// Hydrate search hits: fetch message metadata for a set of `message_id`s.
    pub async fn message_metas_by_ids(&self, ids: &[String]) -> Result<Vec<MessageMeta>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut cached = self.datasets.messages.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.filter(&sql_in("id", ids))?;
        scanner.project(&[
            "id",
            "session_id",
            "role",
            "project",
            "source_agent",
            "timestamp",
            "search_text",
        ])?;
        let batch = scanner.try_into_batch().await?;
        let mut metas = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            metas.push(MessageMeta {
                message_id: datasets::string(&batch, "id", row)?.context("id is null")?,
                session_id: datasets::string(&batch, "session_id", row)?
                    .context("session_id is null")?,
                role: datasets::string(&batch, "role", row)?.context("role is null")?,
                project: datasets::string(&batch, "project", row)?,
                source_agent: datasets::string(&batch, "source_agent", row)?
                    .context("source_agent is null")?,
                timestamp: datasets::datetime(&batch, "timestamp", row)?,
                search_text: datasets::string(&batch, "search_text", row)?.unwrap_or_default(),
            });
        }
        Ok(metas)
    }

    /// Total message count per session, for `group_by_conversation` summaries
    /// (design.md 3.3 - this is the session size, not the count of matches).
    pub async fn session_message_counts(
        &self,
        session_ids: &[String],
    ) -> Result<BTreeMap<String, usize>> {
        if session_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        // Snapshot the dataset once under the lock; `Dataset` is Arc-backed and
        // `count_rows` takes `&self`, so the per-session counts run concurrently
        // and lock-free, each riding the `session_id` scalar index instead of
        // materializing every matched message row.
        let dataset = {
            let mut cached = self.datasets.messages.lock().await;
            cached.latest().await?
        };
        let mut tasks = tokio::task::JoinSet::new();
        for session_id in session_ids {
            let dataset = dataset.clone();
            let session_id = session_id.clone();
            tasks.spawn(async move {
                let filter = format!("session_id = {}", sql_string(&session_id));
                let count = dataset.count_rows(Some(filter)).await?;
                anyhow::Ok((session_id, count))
            });
        }
        let mut counts = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (session_id, count) = joined.context("session count task panicked")??;
            counts.insert(session_id, count);
        }
        Ok(counts)
    }

    /// Create the FTS index on `messages` plus the scalar indexes on all three
    /// content tables, if absent. Called by `pond ingest` after a successful
    /// run; idempotent. Each table is guarded by its own row-count check, since
    /// `create_index` on an empty table errors.
    pub async fn ensure_indices(&self) -> Result<()> {
        if self.row_count(&self.datasets.messages).await? > 0 {
            self.ensure_index(
                &self.datasets.messages,
                "search_text",
                "messages_search_text_fts",
                IndexType::Inverted,
                &InvertedIndexParams::default(),
            )
            .await?;
            for (column, kind, name) in MESSAGE_SCALAR_INDICES {
                self.ensure_scalar_index(&self.datasets.messages, column, kind, name)
                    .await?;
            }
        }
        if self.row_count(&self.datasets.parts).await? > 0 {
            for (column, kind, name) in PARTS_SCALAR_INDICES {
                self.ensure_scalar_index(&self.datasets.parts, column, kind, name)
                    .await?;
            }
        }
        if self.row_count(&self.datasets.sessions).await? > 0 {
            for (column, kind, name) in SESSIONS_SCALAR_INDICES {
                self.ensure_scalar_index(&self.datasets.sessions, column, kind, name)
                    .await?;
            }
        }
        Ok(())
    }

    /// Create the scalar indexes on `embeddings`, and the IVF_PQ vector index
    /// once the table crosses [`VECTOR_INDEX_ACTIVATION_ROWS`]. Called by
    /// `pond ingest` after the embedding pass; idempotent.
    pub async fn ensure_embedding_indices(&self, model: &EmbeddingModel) -> Result<()> {
        self.ensure_embedding_indices_with_threshold(model, VECTOR_INDEX_ACTIVATION_ROWS)
            .await
    }

    /// As [`ensure_embedding_indices`](Self::ensure_embedding_indices), but with
    /// a caller-supplied vector-index activation threshold. The production entry
    /// point passes [`VECTOR_INDEX_ACTIVATION_ROWS`]; tests pass a low value to
    /// exercise the identical activation + build path without the data volume.
    pub async fn ensure_embedding_indices_with_threshold(
        &self,
        model: &EmbeddingModel,
        vector_index_threshold: usize,
    ) -> Result<()> {
        let rows = self.row_count(&self.datasets.embeddings).await?;
        if rows == 0 {
            return Ok(());
        }
        for (column, kind, name) in EMBEDDING_SCALAR_INDICES {
            self.ensure_scalar_index(&self.datasets.embeddings, column, kind, name)
                .await?;
        }
        if rows >= vector_index_threshold {
            let num_partitions = ivf_num_partitions(rows);
            let params = VectorIndexParams::ivf_pq(
                num_partitions,
                8,
                model.num_sub_vectors,
                metric_type(model.distance),
                15,
            );
            self.ensure_index(
                &self.datasets.embeddings,
                "vector",
                "embeddings_vector_ivfpq",
                IndexType::Vector,
                &params,
            )
            .await?;
        }
        Ok(())
    }

    /// Names of the indexes currently built on the `embeddings` dataset.
    pub async fn embedding_index_names(&self) -> Result<Vec<String>> {
        let mut cached = self.datasets.embeddings.lock().await;
        let dataset = cached.latest().await?;
        let indices = dataset.load_indices().await?;
        Ok(indices.iter().map(|index| index.name.clone()).collect())
    }

    async fn row_count(&self, cached: &Mutex<CachedDataset>) -> Result<usize> {
        let mut guard = cached.lock().await;
        Ok(guard.latest().await?.count_rows(None).await?)
    }

    async fn ensure_scalar_index(
        &self,
        cached: &Mutex<CachedDataset>,
        column: &str,
        kind: &BuiltinIndexType,
        name: &str,
    ) -> Result<()> {
        let index_type = match kind {
            BuiltinIndexType::Bitmap => IndexType::Bitmap,
            _ => IndexType::BTree,
        };
        let params = ScalarIndexParams::for_builtin(kind.clone());
        self.ensure_index(cached, column, name, index_type, &params)
            .await
    }

    async fn ensure_index(
        &self,
        cached: &Mutex<CachedDataset>,
        column: &str,
        name: &str,
        index_type: IndexType,
        params: &dyn lance::index::IndexParams,
    ) -> Result<()> {
        let mut guard = cached.lock().await;
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

    async fn find_session(&self, session_id: &str) -> Result<Option<Session>> {
        let mut cached = self.datasets.sessions.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.filter(&format!("id = {}", sql_string(session_id)))?;
        let batch = scanner.try_into_batch().await?;
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(datasets::session_from_batch(&batch, 0)?))
        }
    }

    async fn find_message(&self, message_id: &str) -> Result<Option<Message>> {
        let mut cached = self.datasets.messages.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.filter(&format!("id = {}", sql_string(message_id)))?;
        scanner.project(&[
            "session_id",
            "id",
            "timestamp",
            "role",
            "content",
            "options",
        ])?;
        let batch = scanner.try_into_batch().await?;
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(datasets::message_from_batch(&batch, 0)?))
        }
    }

    async fn messages_for_session(&self, session_id: &str) -> Result<Vec<StoredMessage>> {
        let mut cached = self.datasets.messages.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.filter(&format!("session_id = {}", sql_string(session_id)))?;
        scanner.project(&[
            "session_id",
            "id",
            "timestamp",
            "role",
            "content",
            "options",
        ])?;
        let batch = scanner.try_into_batch().await?;
        let mut messages = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let message = datasets::message_from_batch(&batch, row)?;
            messages.push(message);
        }
        messages.sort_by(|left, right| {
            left.timestamp()
                .cmp(&right.timestamp())
                .then_with(|| left.id().cmp(right.id()))
        });

        let message_ids = messages
            .iter()
            .map(|message| message.id().to_owned())
            .collect::<Vec<_>>();
        let mut parts_by_message = self.parts_for_messages(&message_ids).await?;

        Ok(messages
            .into_iter()
            .map(|message| {
                let parts = parts_by_message.remove(message.id()).unwrap_or_default();
                StoredMessage { message, parts }
            })
            .collect())
    }

    async fn parts_for_messages(
        &self,
        message_ids: &[String],
    ) -> Result<BTreeMap<String, Vec<Part>>> {
        if message_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut cached = self.datasets.parts.lock().await;
        let dataset = cached.latest().await?;
        let mut scanner = dataset.scan();
        scanner.filter(&sql_in("message_id", message_ids))?;
        scanner.project(&[
            "message_id",
            "id",
            "ordinal",
            "type",
            "options",
            "variant_data",
        ])?;
        let batch = scanner.try_into_batch().await?;
        let mut parts_by_message = BTreeMap::<String, Vec<Part>>::new();
        for row in 0..batch.num_rows() {
            let part = datasets::part_from_batch(&batch, row)?;
            parts_by_message
                .entry(part.message_id.clone())
                .or_default()
                .push(part);
        }
        for parts in parts_by_message.values_mut() {
            parts.sort_by_key(|part| part.ordinal);
        }
        Ok(parts_by_message)
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
    path: PathBuf,
    schema: Arc<lance::deps::arrow_schema::Schema>,
) -> Result<Dataset> {
    let uri = path.to_string_lossy().into_owned();
    if path.exists() {
        Dataset::open(&uri)
            .await
            .with_context(|| format!("failed to open dataset {uri}"))
    } else {
        let reader = datasets::empty_reader(schema)?;
        Dataset::write(reader, &uri, Some(datasets::write_params()))
            .await
            .with_context(|| format!("failed to create dataset {uri}"))
    }
}

/// Scalar indexes on `messages` (design.md 3.2.2): BTREE for high-cardinality
/// and range columns, BITMAP for low-cardinality columns.
const MESSAGE_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] = &[
    ("id", BuiltinIndexType::BTree, "messages_id_btree"),
    ("project", BuiltinIndexType::BTree, "messages_project_btree"),
    (
        "session_id",
        BuiltinIndexType::BTree,
        "messages_session_id_btree",
    ),
    (
        "timestamp",
        BuiltinIndexType::BTree,
        "messages_timestamp_btree",
    ),
    (
        "source_agent",
        BuiltinIndexType::Bitmap,
        "messages_source_agent_bitmap",
    ),
    ("role", BuiltinIndexType::Bitmap, "messages_role_bitmap"),
];

/// Scalar indexes on `embeddings` (design.md 3.2.4): the same filter set,
/// denormalized so vector kNN pushes predicates down without a cross-table join.
const EMBEDDING_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] = &[
    (
        "message_id",
        BuiltinIndexType::BTree,
        "embeddings_message_id_btree",
    ),
    (
        "session_id",
        BuiltinIndexType::BTree,
        "embeddings_session_id_btree",
    ),
    (
        "project",
        BuiltinIndexType::BTree,
        "embeddings_project_btree",
    ),
    (
        "timestamp",
        BuiltinIndexType::BTree,
        "embeddings_timestamp_btree",
    ),
    (
        "source_agent",
        BuiltinIndexType::Bitmap,
        "embeddings_source_agent_bitmap",
    ),
    ("role", BuiltinIndexType::Bitmap, "embeddings_role_bitmap"),
];

/// Scalar index on `parts`: `message_id` is the hot-path lookup key for
/// `parts_for_messages` (hydration on every `get` and grouped search).
const PARTS_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] = &[(
    "message_id",
    BuiltinIndexType::BTree,
    "parts_message_id_btree",
)];

/// Scalar index on `sessions`: `id` is filtered by `find_session` on every
/// `get` and every grouped search.
const SESSIONS_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] =
    &[("id", BuiltinIndexType::BTree, "sessions_id_btree")];

/// Map a registry [`Distance`] to the Lance vector-index metric. Keeps the
/// IVF_PQ index consistent with the model's declared distance instead of
/// hardcoding one metric.
pub fn metric_type(distance: Distance) -> MetricType {
    match distance {
        Distance::Cosine => MetricType::Cosine,
        Distance::L2 => MetricType::L2,
        Distance::Dot => MetricType::Dot,
    }
}

/// IVF partition count: `max(32, min(4096, round(sqrt(num_rows))))` (design.md 3.2.4).
fn ivf_num_partitions(num_rows: usize) -> usize {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let sqrt = (num_rows as f64).sqrt().round() as usize;
    sqrt.clamp(32, 4096)
}

pub fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A quoted `LIKE` pattern matching `value` as a literal substring. Escapes the
/// SQL string quote *and* the `LIKE` metacharacters (`%`, `_`, `\`) so a path
/// like `my_project` matches literally rather than treating `_` as a wildcard.
/// Pair with an `ESCAPE '\'` clause in the predicate.
pub fn sql_like_contains(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''");
    format!("'%{escaped}%'")
}

fn sql_in(column: &str, values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| sql_string(value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{column} IN ({values})")
}

fn statuses_from_inserted(total: usize, inserted_rows: u64) -> Vec<UpsertStatus> {
    let inserted = usize::try_from(inserted_rows)
        .unwrap_or(usize::MAX)
        .min(total);
    let mut statuses = Vec::with_capacity(total);
    statuses.extend(std::iter::repeat_n(UpsertStatus::Inserted, inserted));
    statuses.extend(std::iter::repeat_n(
        UpsertStatus::Matched,
        total.saturating_sub(inserted),
    ));
    statuses
}
