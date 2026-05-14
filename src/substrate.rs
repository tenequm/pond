use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use lance::Dataset;
use lance::dataset::MergeInsertBuilder;
use lance::deps::arrow_array::RecordBatchIterator;
use tokio::sync::Mutex;

use crate::{
    datasets,
    types::{Message, Part, Session, StoredMessage, StoredSession},
};

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

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
