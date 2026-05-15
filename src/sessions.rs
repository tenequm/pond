use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use async_stream::try_stream;
use chrono::{DateTime, TimeZone, Utc};
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::{AutoCleanupParams, WriteParams};
use lance::deps::arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray, TimestampMicrosecondArray,
};
use lance::deps::arrow_schema::{DataType, Field, Schema, TimeUnit};
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, FullTextSearchQuery, InvertedIndexParams};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_stream::{Stream, StreamExt};

use crate::{
    config::{self, EmbeddingModel},
    embed,
    substrate::{
        Handle, MaintenanceReport, Predicate, ScalarValue, Table, VECTOR_INDEX_ACTIVATION_ROWS,
        scanner_with_prefilter,
    },
    wire::{FileData, Message, Part, PartKind, Role, Session},
};
use url::Url;

#[derive(Debug)]
pub struct Store {
    handle: Handle,
}

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

impl Store {
    /// Open against a local filesystem URL or a remote one for which the
    /// caller has no extra options to pass (env vars suffice). CLI verbs
    /// that load `[storage]` from config should call
    /// [`Store::open_with_options`] instead so the same options flow into
    /// every dataset open and write.
    pub async fn open(location: &Url) -> Result<Self> {
        Ok(Self {
            handle: Handle::open(location).await?,
        })
    }

    /// Open with object-store options (S3 creds, region, endpoint, ...)
    /// threaded through Lance verbatim. Keys are the standard `object_store`
    /// config names; pond does not parse them. Empty options is equivalent
    /// to [`Store::open`].
    pub async fn open_with_options(
        location: &Url,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            handle: Handle::open_with_options(location, storage_options).await?,
        })
    }

    /// Convenience for tests and CLI verbs holding a `&Path`: wraps the path in
    /// a `file://...` URL via [`config::url_for_path`] before opening.
    pub async fn open_local(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let url = config::url_for_path(path)?;
        Self::open(&url).await
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
        let batch = sessions_batch(sessions)?;
        let inserted = self
            .handle
            .merge_insert(Table::Sessions, batch, sessions.len())
            .await?;
        Ok(statuses_from_inserted(sessions.len(), inserted))
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
            .map(|write| MessageBatchRow {
                message: write.message,
                source_agent: &session.source_agent,
                project: session.project.as_deref(),
                search_text: write.search_text,
            })
            .collect::<Vec<_>>();
        let batch = messages_batch(&rows)?;
        let inserted = self
            .handle
            .merge_insert(Table::Messages, batch, messages.len())
            .await?;
        Ok(statuses_from_inserted(messages.len(), inserted))
    }

    pub async fn upsert_parts(&self, parts: &[Part]) -> Result<Vec<UpsertStatus>> {
        if parts.is_empty() {
            return Ok(Vec::new());
        }
        let batch = parts_batch(parts)?;
        let inserted = self
            .handle
            .merge_insert(Table::Parts, batch, parts.len())
            .await?;
        Ok(statuses_from_inserted(parts.len(), inserted))
    }

    pub async fn upsert_part(&self, part: &Part) -> Result<UpsertStatus> {
        let mut statuses = self.upsert_parts(std::slice::from_ref(part)).await?;
        statuses
            .pop()
            .context("single part upsert returned no status")
    }

    pub async fn get_session(&self, session_id: &str) -> Result<Option<SessionWithMessages>> {
        let Some(session) = self.find_session(session_id).await? else {
            return Ok(None);
        };
        let messages = self.messages_for_session(session_id).await?;
        Ok(Some(SessionWithMessages { session, messages }))
    }

    /// Every session id currently in the store, unsorted.
    pub async fn session_ids(&self) -> Result<Vec<String>> {
        let batch = self
            .handle
            .scan_batch(Table::Sessions, None, &["id"])
            .await?;
        let mut ids = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if let Some(id) = string(&batch, "id", row)? {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    pub async fn get_message_context(
        &self,
        message_id: &str,
        context_depth: usize,
    ) -> Result<Option<(Session, Vec<MessageWithParts>)>> {
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
        self.handle.row_counts().await
    }

    /// Merge-insert embedding rows keyed on `(message_id, model_id,
    /// max_embed_tokens)`. Re-running over already-embedded messages is a no-op
    /// for matched rows.
    pub async fn upsert_embeddings(&self, rows: &[EmbeddingRow]) -> Result<Vec<UpsertStatus>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let batch = embeddings_batch(rows)?;
        let inserted = self
            .handle
            .merge_insert(Table::Embeddings, batch, rows.len())
            .await?;
        Ok(statuses_from_inserted(rows.len(), inserted))
    }

    /// The set of `message_id`s that already have `embeddings` rows for this
    /// `(model_id, max_embed_tokens)` identity.
    pub async fn embedded_message_ids(
        &self,
        model_id: &str,
        max_embed_tokens: i32,
    ) -> Result<HashSet<String>> {
        let dataset = self.handle.dataset(Table::Embeddings).await?;
        let identity = embedding_identity_predicate(model_id, max_embed_tokens, None);
        let mut scanner = scanner_with_prefilter(&dataset, Some(&identity))?;
        scanner.project(&["message_id"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut set = HashSet::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                if let Some(id) = string(&batch, "message_id", row)? {
                    set.insert(id);
                }
            }
        }
        Ok(set)
    }

    /// Stream every message awaiting embedding as a domain [`PendingMessage`].
    pub fn pending_messages_stream(&self) -> impl Stream<Item = Result<PendingMessage>> + '_ {
        try_stream! {
            let dataset = self.handle.dataset(Table::Messages).await?;
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
            let mut batches = scanner
                .try_into_stream()
                .await
                .context("failed to open messages stream")?;
            while let Some(batch) = batches.next().await {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let pending = pending_message_from_batch(&batch, row)?;
                    yield pending;
                }
            }
        }
    }

    /// BM25 full-text retriever over `messages.search_text`.
    pub async fn fts_search(
        &self,
        query: &str,
        limit: usize,
        filter: &Predicate,
    ) -> Result<Vec<(String, f32)>> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        let mut scanner = scanner_with_prefilter(&dataset, Some(filter))?;
        scanner.full_text_search(
            FullTextSearchQuery::new(query.to_owned()).with_column("search_text".to_owned())?,
        )?;
        scanner.project(&["id"])?;
        scanner.limit(Some(i64::try_from(limit).unwrap_or(i64::MAX)), None)?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let id = string(&batch, "id", row)?.context("fts hit id is null")?;
            hits.push((id, float32(&batch, "_score", row)?));
        }
        Ok(hits)
    }

    /// Whether the `embeddings` table holds any row for this `(model_id,
    /// max_embed_tokens)` identity.
    pub async fn has_embeddings(&self, model_id: &str, max_embed_tokens: i32) -> Result<bool> {
        let dataset = self.handle.dataset(Table::Embeddings).await?;
        let identity = embedding_identity_predicate(model_id, max_embed_tokens, None);
        let mut scanner = scanner_with_prefilter(&dataset, Some(&identity))?;
        scanner.project(&["message_id"])?;
        scanner.limit(Some(1), None)?;
        let batch = scanner.try_into_batch().await?;
        Ok(batch.num_rows() > 0)
    }

    /// Vector kNN retriever over `embeddings.vector`.
    pub async fn vector_search(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
        model_id: &str,
        max_embed_tokens: i32,
    ) -> Result<Vec<(String, f32)>> {
        let dataset = self.handle.dataset(Table::Embeddings).await?;
        let identity = embedding_identity_predicate(model_id, max_embed_tokens, Some(filter));
        let mut scanner = scanner_with_prefilter(&dataset, Some(&identity))?;
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        scanner.project(&["message_id"])?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let id = string(&batch, "message_id", row)?.context("vector hit id is null")?;
            hits.push((id, float32(&batch, "_distance", row)?));
        }
        Ok(hits)
    }

    /// The DataFusion plan string for a filtered hybrid scan.
    pub async fn explain_vector_plan(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
        model_id: &str,
        max_embed_tokens: i32,
    ) -> Result<String> {
        let dataset = self.handle.dataset(Table::Embeddings).await?;
        let identity = embedding_identity_predicate(model_id, max_embed_tokens, Some(filter));
        let mut scanner = scanner_with_prefilter(&dataset, Some(&identity))?;
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
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&in_predicate("id", ids)),
                &[
                    "id",
                    "session_id",
                    "role",
                    "project",
                    "source_agent",
                    "timestamp",
                    "search_text",
                ],
            )
            .await?;
        let mut metas = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            metas.push(MessageMeta {
                message_id: string(&batch, "id", row)?.context("id is null")?,
                session_id: string(&batch, "session_id", row)?.context("session_id is null")?,
                role: string(&batch, "role", row)?.context("role is null")?,
                project: string(&batch, "project", row)?,
                source_agent: string(&batch, "source_agent", row)?
                    .context("source_agent is null")?,
                timestamp: datetime(&batch, "timestamp", row)?,
                search_text: string(&batch, "search_text", row)?.unwrap_or_default(),
            });
        }
        Ok(metas)
    }

    /// Total message count per session, for `group_by_conversation` summaries.
    pub async fn session_message_counts(
        &self,
        session_ids: &[String],
    ) -> Result<BTreeMap<String, usize>> {
        if session_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let dataset = self.handle.dataset(Table::Messages).await?;
        let mut tasks = tokio::task::JoinSet::new();
        for session_id in session_ids {
            let dataset = dataset.clone();
            let session_id = session_id.clone();
            tasks.spawn(async move {
                let filter = Predicate::Eq("session_id", session_id.as_str().into()).to_lance();
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

    /// Create the FTS index on `messages` plus scalar indexes on content tables.
    pub async fn ensure_indices(&self) -> Result<()> {
        if self.handle.count_rows(Table::Messages, None).await? > 0 {
            self.handle
                .ensure_index(
                    Table::Messages,
                    "search_text",
                    "messages_search_text_fts",
                    IndexType::Inverted,
                    &InvertedIndexParams::default(),
                )
                .await?;
            for (column, kind, name) in MESSAGE_SCALAR_INDICES {
                self.handle
                    .ensure_scalar_index(Table::Messages, column, kind, name)
                    .await?;
            }
        }
        if self.handle.count_rows(Table::Parts, None).await? > 0 {
            for (column, kind, name) in PARTS_SCALAR_INDICES {
                self.handle
                    .ensure_scalar_index(Table::Parts, column, kind, name)
                    .await?;
            }
        }
        if self.handle.count_rows(Table::Sessions, None).await? > 0 {
            for (column, kind, name) in SESSIONS_SCALAR_INDICES {
                self.handle
                    .ensure_scalar_index(Table::Sessions, column, kind, name)
                    .await?;
            }
        }
        Ok(())
    }

    /// Create scalar indexes on `embeddings`, and IVF_PQ once the table crosses
    /// [`VECTOR_INDEX_ACTIVATION_ROWS`].
    pub async fn ensure_embedding_indices(&self, model: &EmbeddingModel) -> Result<()> {
        self.ensure_embedding_indices_with_threshold(model, VECTOR_INDEX_ACTIVATION_ROWS)
            .await
    }

    pub async fn ensure_embedding_indices_with_threshold(
        &self,
        model: &EmbeddingModel,
        vector_index_threshold: usize,
    ) -> Result<()> {
        let rows = self.handle.count_rows(Table::Embeddings, None).await?;
        if rows == 0 {
            return Ok(());
        }
        for (column, kind, name) in EMBEDDING_SCALAR_INDICES {
            self.handle
                .ensure_scalar_index(Table::Embeddings, column, kind, name)
                .await?;
        }
        if rows >= vector_index_threshold {
            let params = embed::index_params(model, rows);
            self.handle
                .ensure_index(
                    Table::Embeddings,
                    "vector",
                    "embeddings_vector_ivfpq",
                    IndexType::Vector,
                    &params,
                )
                .await?;
        }
        Ok(())
    }

    pub async fn embedding_index_names(&self) -> Result<Vec<String>> {
        self.handle.embedding_index_names().await
    }

    pub async fn maintenance(
        &self,
        retention: chrono::Duration,
        skip_cleanup: bool,
        skip_optimize: bool,
    ) -> MaintenanceReport {
        self.handle
            .maintenance(retention, skip_cleanup, skip_optimize)
            .await
    }

    async fn find_session(&self, session_id: &str) -> Result<Option<Session>> {
        let batch = self
            .handle
            .scan_batch(
                Table::Sessions,
                Some(&Predicate::Eq("id", session_id.into())),
                &[],
            )
            .await?;
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(session_from_batch(&batch, 0)?))
        }
    }

    async fn find_message(&self, message_id: &str) -> Result<Option<Message>> {
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&Predicate::Eq("id", message_id.into())),
                &[
                    "session_id",
                    "id",
                    "timestamp",
                    "role",
                    "content",
                    "options",
                ],
            )
            .await?;
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(message_from_batch(&batch, 0)?))
        }
    }

    /// Return every message of `session_id` in canonical `(timestamp, id)`
    /// order, each paired with its parts. Public face of the session
    /// iteration seam used by both `pond_get` (full-session reads) and
    /// `pond_session_events` (catch-up SSE per design.md 3.6.5).
    pub async fn session_messages(
        &self,
        session_id: &str,
    ) -> Result<Vec<MessageWithParts>> {
        self.messages_for_session(session_id).await
    }

    async fn messages_for_session(&self, session_id: &str) -> Result<Vec<MessageWithParts>> {
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&Predicate::Eq("session_id", session_id.into())),
                &[
                    "session_id",
                    "id",
                    "timestamp",
                    "role",
                    "content",
                    "options",
                ],
            )
            .await?;
        let mut messages = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            messages.push(message_from_batch(&batch, row)?);
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
                MessageWithParts { message, parts }
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
        let batch = self
            .handle
            .scan_batch(
                Table::Parts,
                Some(&in_predicate("message_id", message_ids)),
                &[
                    "message_id",
                    "id",
                    "ordinal",
                    "type",
                    "options",
                    "variant_data",
                ],
            )
            .await?;
        let mut parts_by_message = BTreeMap::<String, Vec<Part>>::new();
        for row in 0..batch.num_rows() {
            let part = part_from_batch(&batch, row)?;
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum IngestEvent {
    Session(Session),
    Message(Message),
    Part(Part),
}

/// Aggregate accounting for an ingest pass (CLI sync, adapter-driven).
/// The wire layer (`pond_ingest`) instead returns per-row results; the
/// aggregate is derived from those at the wire boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestSummary {
    pub inserted: usize,
    pub matched: usize,
    pub errors: usize,
}

impl IngestSummary {
    pub fn accepted(&self) -> usize {
        self.inserted + self.matched
    }

    pub fn add_outcomes(&mut self, outcomes: &[RowOutcome]) {
        for outcome in outcomes {
            match outcome.status {
                OutcomeStatus::Inserted => self.inserted += 1,
                OutcomeStatus::Matched => self.matched += 1,
                OutcomeStatus::Error => self.errors += 1,
            }
        }
    }
}

/// Per-row outcome surfaced by [`IngestValidator`] (design.md 3.6.4). One
/// row per input event from the request's `events` array. The validator
/// returns these in array order so the wire layer can pack them directly
/// into [`crate::wire::IngestResult`] entries.
#[derive(Debug, Clone, PartialEq)]
pub struct RowOutcome {
    pub index: usize,
    pub kind: &'static str,
    pub pk: Value,
    pub status: OutcomeStatus,
    pub error: Option<RowError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeStatus {
    Inserted,
    Matched,
    Error,
}

/// Structured per-row error body. Mirrors the wire shape so the handler
/// can pass it straight through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowError {
    pub message: String,
    pub field: Option<&'static str>,
    pub reason: Option<&'static str>,
}

/// Buffered session events tagged with their input array index, so the
/// per-row outcomes can be re-attributed once `merge_insert` returns its
/// per-row Inserted/Matched stats.
#[derive(Debug)]
struct BufferedSession {
    index: usize,
    session: Session,
}

#[derive(Debug)]
struct BufferedMessage {
    index: usize,
    message: Message,
    parts: Vec<BufferedPart>,
    search_text: Option<String>,
}

#[derive(Debug)]
struct BufferedPart {
    index: usize,
    part: Part,
}

/// State machine that turns the `events: Vec<IngestEvent>` array into a
/// flat `Vec<RowOutcome>` matching the array's index space. Buffers a whole
/// session substream so `merge_insert` runs once per substream (three
/// batches: sessions, messages, parts). A validation error on any event in
/// the substream marks every event of that substream as
/// [`OutcomeStatus::Error`]; the next `IngestEvent::Session` resets the
/// state and ingest continues.
#[derive(Debug, Default)]
pub struct IngestValidator {
    session: Option<BufferedSession>,
    current_message: Option<BufferedMessage>,
    current_parts: Vec<BufferedPart>,
    messages: Vec<BufferedMessage>,
    failed_message: Option<String>,
    aborted_indices: Vec<(usize, &'static str, Value)>,
}

impl IngestValidator {
    /// Drive one input event through the validator. Returns the per-row
    /// outcomes the event triggered: empty when the event is just buffered,
    /// or N entries when a session substream just flushed (success or
    /// failure). `Err` is reserved for catastrophic storage failures that
    /// should fail the whole `pond_ingest` request.
    pub async fn push(
        &mut self,
        store: &Store,
        index: usize,
        event: IngestEvent,
    ) -> Result<Vec<RowOutcome>> {
        match event {
            IngestEvent::Session(session) => self.push_session(store, index, session).await,
            IngestEvent::Message(message) => Ok(self.push_message(index, message)),
            IngestEvent::Part(part) => Ok(self.push_part(index, part)),
        }
    }

    /// Final flush at end-of-batch. Empties any buffered substream.
    pub async fn finish(&mut self, store: &Store) -> Result<Vec<RowOutcome>> {
        if self.failed_message.is_some() {
            return Ok(self.drain_failed_substream());
        }
        self.flush_session(store).await
    }

    async fn push_session(
        &mut self,
        store: &Store,
        index: usize,
        mut session: Session,
    ) -> Result<Vec<RowOutcome>> {
        let mut outcomes = if self.failed_message.is_some() {
            self.drain_failed_substream()
        } else if self.session.is_some() {
            self.flush_session(store).await?
        } else {
            Vec::new()
        };

        // design.md 3.1.3: `source_agent` is trimmed at ingest and rejected
        // if empty after trim. The validator is the safety net regardless
        // of adapter discipline.
        let trimmed = session.source_agent.trim();
        if trimmed.is_empty() {
            outcomes.push(RowOutcome {
                index,
                kind: "session",
                pk: Value::String(session.id.clone()),
                status: OutcomeStatus::Error,
                error: Some(RowError {
                    message: format!("session {} has empty source_agent after trim", session.id),
                    field: Some("source_agent"),
                    reason: None,
                }),
            });
            // No buffered substream to abort: this is the first event.
            return Ok(outcomes);
        }
        if trimmed.len() != session.source_agent.len() {
            session.source_agent = trimmed.to_owned();
        }

        self.session = Some(BufferedSession { index, session });
        Ok(outcomes)
    }

    fn push_message(&mut self, index: usize, message: Message) -> Vec<RowOutcome> {
        let pk = Value::Array(vec![
            Value::String(message.session_id().to_owned()),
            Value::String(message.id().to_owned()),
        ]);
        if self.failed_message.is_some() {
            self.aborted_indices.push((index, "message", pk));
            return Vec::new();
        }
        let Some(session) = &self.session else {
            self.fail_substream(
                index,
                "message",
                pk,
                "first event in a session stream must be Session",
                None,
            );
            return Vec::new();
        };
        if message.session_id() != session.session.id {
            let msg = format!(
                "message {} references session {}, expected {}",
                message.id(),
                message.session_id(),
                session.session.id
            );
            self.fail_substream(index, "message", pk, msg, Some("session_id"));
            return Vec::new();
        }
        self.flush_current_message();
        self.current_message = Some(BufferedMessage {
            index,
            message,
            parts: Vec::new(),
            search_text: None,
        });
        Vec::new()
    }

    fn push_part(&mut self, index: usize, part: Part) -> Vec<RowOutcome> {
        let pk = Value::Array(vec![
            Value::String(part.message_id.clone()),
            Value::String(part.id.clone()),
        ]);
        if self.failed_message.is_some() {
            self.aborted_indices.push((index, "part", pk));
            return Vec::new();
        }
        let Some(current) = &self.current_message else {
            self.fail_substream(
                index,
                "part",
                pk,
                "part event appeared before a message",
                None,
            );
            return Vec::new();
        };
        if part.message_id != current.message.id() {
            let msg = format!(
                "part {} references message {}, expected {}",
                part.id,
                part.message_id,
                current.message.id()
            );
            self.fail_substream(index, "part", pk, msg, Some("message_id"));
            return Vec::new();
        }
        self.current_parts.push(BufferedPart { index, part });
        Vec::new()
    }

    fn flush_current_message(&mut self) {
        let Some(mut buffered) = self.current_message.take() else {
            return;
        };
        let parts = std::mem::take(&mut self.current_parts);
        let mut canonical_parts = Vec::with_capacity(parts.len());
        for part in &parts {
            canonical_parts.push(part.part.clone());
        }
        buffered.search_text = search_text(&buffered.message, &canonical_parts);
        buffered.parts = parts;
        self.messages.push(buffered);
    }

    async fn flush_session(&mut self, store: &Store) -> Result<Vec<RowOutcome>> {
        self.flush_current_message();
        let Some(BufferedSession {
            index: session_index,
            session,
        }) = self.session.take()
        else {
            return Ok(Vec::new());
        };
        let buffered_messages = std::mem::take(&mut self.messages);

        if let Err(failure) = validate_batch_keys(&buffered_messages) {
            return Ok(error_outcomes_for_substream(
                session_index,
                &session,
                &buffered_messages,
                failure,
                None,
            ));
        }

        // design.md 3.6.4: source_agent + project are immutable post-first-
        // write because messages/embeddings denormalize them. Probe the
        // existing row before write; reject with the typed field name so the
        // wire layer can attach `details: { field, reason: "immutable" }`.
        if let Some(existing) = store.find_session(&session.id).await?
            && let Err(failure) = ensure_immutable_match(&existing, &session)
        {
            let field = match &failure {
                IngestError::ImmutableField { field, .. } => Some(*field),
            };
            return Ok(error_outcomes_for_substream(
                session_index,
                &session,
                &buffered_messages,
                failure.to_string(),
                field,
            ));
        }

        let writes = buffered_messages
            .iter()
            .map(|message| {
                let parts = message
                    .parts
                    .iter()
                    .map(|part| part.part.clone())
                    .collect::<Vec<_>>();
                (message, parts)
            })
            .collect::<Vec<_>>();
        let message_writes = writes
            .iter()
            .map(|(message, parts)| MessageWrite {
                message: &message.message,
                parts,
                search_text: message.search_text.as_deref(),
            })
            .collect::<Vec<_>>();

        let statuses = store
            .upsert_session_bundle(&session, &message_writes)
            .await?;

        Ok(success_outcomes_from_statuses(
            session_index,
            &session,
            &buffered_messages,
            &statuses,
        ))
    }

    /// Tag the current substream as failed: the offending event gets an
    /// error outcome immediately, all subsequent buffered events of this
    /// substream get tagged on the next flush/reset.
    fn fail_substream(
        &mut self,
        index: usize,
        kind: &'static str,
        pk: Value,
        message: impl Into<String>,
        field: Option<&'static str>,
    ) {
        let message = message.into();
        self.failed_message = Some(message.clone());
        self.aborted_indices.push((index, kind, pk));
        // The session, current_message, and current_parts buffers remain so
        // `drain_failed_substream` can tag those too with the same error.
        let _ = field; // currently only the message is propagated to all outcomes
    }

    /// Drain all buffered events of the failed substream into Error outcomes
    /// and reset state for the next session. Called when (a) the next Session
    /// event arrives, signaling recovery, or (b) `finish()` is called at end
    /// of batch.
    fn drain_failed_substream(&mut self) -> Vec<RowOutcome> {
        let Some(message) = self.failed_message.take() else {
            return Vec::new();
        };
        let aborted = std::mem::take(&mut self.aborted_indices);

        let mut outcomes = Vec::new();

        if let Some(BufferedSession {
            index,
            session,
        }) = self.session.take()
        {
            outcomes.push(error_outcome(
                index,
                "session",
                Value::String(session.id),
                &message,
                None,
            ));
        }
        for buffered in self.messages.drain(..) {
            let pk = Value::Array(vec![
                Value::String(buffered.message.session_id().to_owned()),
                Value::String(buffered.message.id().to_owned()),
            ]);
            outcomes.push(error_outcome(buffered.index, "message", pk, &message, None));
            for part in buffered.parts {
                let pk = Value::Array(vec![
                    Value::String(part.part.message_id.clone()),
                    Value::String(part.part.id.clone()),
                ]);
                outcomes.push(error_outcome(part.index, "part", pk, &message, None));
            }
        }
        if let Some(buffered) = self.current_message.take() {
            let pk = Value::Array(vec![
                Value::String(buffered.message.session_id().to_owned()),
                Value::String(buffered.message.id().to_owned()),
            ]);
            outcomes.push(error_outcome(buffered.index, "message", pk, &message, None));
        }
        for buffered in self.current_parts.drain(..) {
            let pk = Value::Array(vec![
                Value::String(buffered.part.message_id.clone()),
                Value::String(buffered.part.id.clone()),
            ]);
            outcomes.push(error_outcome(buffered.index, "part", pk, &message, None));
        }
        for (index, kind, pk) in aborted {
            outcomes.push(error_outcome(index, kind, pk, &message, None));
        }
        outcomes.sort_by_key(|outcome| outcome.index);
        outcomes
    }
}

fn error_outcome(
    index: usize,
    kind: &'static str,
    pk: Value,
    message: &str,
    field: Option<&'static str>,
) -> RowOutcome {
    RowOutcome {
        index,
        kind,
        pk,
        status: OutcomeStatus::Error,
        error: Some(RowError {
            message: message.to_owned(),
            field,
            reason: None,
        }),
    }
}

fn error_outcomes_for_substream(
    session_index: usize,
    session: &Session,
    messages: &[BufferedMessage],
    message: impl Into<String>,
    field: Option<&'static str>,
) -> Vec<RowOutcome> {
    let message = message.into();
    let reason = field.map(|_| "immutable");
    let mk_err = |index: usize, kind: &'static str, pk: Value| RowOutcome {
        index,
        kind,
        pk,
        status: OutcomeStatus::Error,
        error: Some(RowError {
            message: message.clone(),
            field,
            reason,
        }),
    };
    let mut outcomes = Vec::with_capacity(1 + messages.len());
    outcomes.push(mk_err(
        session_index,
        "session",
        Value::String(session.id.clone()),
    ));
    for buffered in messages {
        let pk = Value::Array(vec![
            Value::String(buffered.message.session_id().to_owned()),
            Value::String(buffered.message.id().to_owned()),
        ]);
        outcomes.push(mk_err(buffered.index, "message", pk));
        for part in &buffered.parts {
            let part_pk = Value::Array(vec![
                Value::String(part.part.message_id.clone()),
                Value::String(part.part.id.clone()),
            ]);
            outcomes.push(mk_err(part.index, "part", part_pk));
        }
    }
    outcomes.sort_by_key(|outcome| outcome.index);
    outcomes
}

fn success_outcomes_from_statuses(
    session_index: usize,
    session: &Session,
    messages: &[BufferedMessage],
    statuses: &[UpsertStatus],
) -> Vec<RowOutcome> {
    // upsert_session_bundle returns statuses ordered: [sessions, messages..., parts...].
    let mut outcomes = Vec::with_capacity(statuses.len());
    let mut cursor = 0usize;
    let session_status = statuses
        .get(cursor)
        .copied()
        .unwrap_or(UpsertStatus::Inserted);
    cursor += 1;
    outcomes.push(success_outcome(
        session_index,
        "session",
        Value::String(session.id.clone()),
        session_status,
    ));

    for buffered in messages {
        let pk = Value::Array(vec![
            Value::String(buffered.message.session_id().to_owned()),
            Value::String(buffered.message.id().to_owned()),
        ]);
        let status = statuses
            .get(cursor)
            .copied()
            .unwrap_or(UpsertStatus::Inserted);
        cursor += 1;
        outcomes.push(success_outcome(buffered.index, "message", pk, status));
    }
    for buffered in messages {
        for part in &buffered.parts {
            let pk = Value::Array(vec![
                Value::String(part.part.message_id.clone()),
                Value::String(part.part.id.clone()),
            ]);
            let status = statuses
                .get(cursor)
                .copied()
                .unwrap_or(UpsertStatus::Inserted);
            cursor += 1;
            outcomes.push(success_outcome(part.index, "part", pk, status));
        }
    }
    outcomes.sort_by_key(|outcome| outcome.index);
    outcomes
}

fn success_outcome(
    index: usize,
    kind: &'static str,
    pk: Value,
    status: UpsertStatus,
) -> RowOutcome {
    let status = match status {
        UpsertStatus::Inserted => OutcomeStatus::Inserted,
        UpsertStatus::Matched => OutcomeStatus::Matched,
    };
    RowOutcome {
        index,
        kind,
        pk,
        status,
        error: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum IngestError {
    /// design.md 3.6.4: `Session.source_agent` and `Session.project` are
    /// immutable post-first-write because the denormalized copies on
    /// `messages` and `embeddings` were stamped from the prior Session at
    /// first ingest. A re-write that changes either would silently desync.
    ImmutableField {
        field: &'static str,
        session_id: String,
        stored: String,
        attempted: String,
    },
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ImmutableField {
                field,
                session_id,
                stored,
                attempted,
            } => write!(
                formatter,
                "session {session_id} {field} is immutable: stored {stored:?}, attempted {attempted:?}",
            ),
        }
    }
}

impl std::error::Error for IngestError {}

/// Reject in-substream duplicate keys before the merge_insert commits the
/// batch. Returns a one-line operator-facing message; the caller wraps each
/// affected event into a per-row Error outcome with this message.
fn validate_batch_keys(messages: &[BufferedMessage]) -> std::result::Result<(), String> {
    let mut message_ids = HashSet::with_capacity(messages.len());
    let mut part_ids = HashSet::new();

    for message in messages {
        let message_id = message.message.id();
        if !message_ids.insert(message_id.to_owned()) {
            return Err(format!(
                "duplicate message id {message_id} in session substream"
            ));
        }

        for part in &message.parts {
            let key = (part.part.message_id.as_str(), part.part.id.as_str());
            if !part_ids.insert((key.0.to_owned(), key.1.to_owned())) {
                return Err(format!(
                    "duplicate part id {} for message {} in session substream",
                    part.part.id, part.part.message_id
                ));
            }
        }
    }

    Ok(())
}

/// Compare an incoming Session row against the stored row on the two
/// immutable fields (design.md 3.6.4). The `Option<String>` `project` field
/// counts a NULL-vs-non-NULL change as a mismatch.
fn ensure_immutable_match(
    existing: &Session,
    incoming: &Session,
) -> std::result::Result<(), IngestError> {
    if existing.source_agent != incoming.source_agent {
        return Err(IngestError::ImmutableField {
            field: "source_agent",
            session_id: incoming.id.clone(),
            stored: existing.source_agent.clone(),
            attempted: incoming.source_agent.clone(),
        });
    }
    if existing.project != incoming.project {
        return Err(IngestError::ImmutableField {
            field: "project",
            session_id: incoming.id.clone(),
            stored: existing.project.clone().unwrap_or_default(),
            attempted: incoming.project.clone().unwrap_or_default(),
        });
    }
    Ok(())
}

pub fn search_text(message: &Message, parts: &[Part]) -> Option<String> {
    let mut chunks = Vec::new();
    for part in parts {
        match (message.role(), &part.kind) {
            (Role::User | Role::Assistant, PartKind::Text { text }) => {
                chunks.push(text.clone());
            }
            (Role::Assistant, PartKind::ToolCall { name, params, .. }) => {
                chunks.push(name.clone());
                collect_string_leaves(params, &mut chunks);
            }
            (
                Role::User | Role::Assistant,
                PartKind::File {
                    media_type,
                    file_name,
                    data,
                },
            ) => {
                if let Some(file_name) = file_name {
                    chunks.push(file_name.clone());
                }
                chunks.push(media_type.clone());
                if let FileData::Url(uri) = data {
                    chunks.push(uri.clone());
                }
            }
            (
                Role::System | Role::Tool,
                PartKind::Text { .. }
                | PartKind::Reasoning { .. }
                | PartKind::File { .. }
                | PartKind::ToolCall { .. }
                | PartKind::ToolResult { .. }
                | PartKind::ToolApprovalRequest { .. }
                | PartKind::ToolApprovalResponse { .. },
            )
            | (
                Role::User | Role::Assistant,
                PartKind::Reasoning { .. }
                | PartKind::ToolResult { .. }
                | PartKind::ToolApprovalRequest { .. }
                | PartKind::ToolApprovalResponse { .. },
            )
            | (Role::User, PartKind::ToolCall { .. }) => {}
        }
    }

    let text = chunks
        .into_iter()
        .filter(|chunk| !chunk.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() { None } else { Some(text) }
}

fn collect_string_leaves(value: &serde_json::Value, chunks: &mut Vec<String>) {
    match value {
        serde_json::Value::String(text) => chunks.push(text.clone()),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_string_leaves(value, chunks);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_string_leaves(value, chunks);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageWithParts {
    pub message: Message,
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionWithMessages {
    pub session: Session,
    pub messages: Vec<MessageWithParts>,
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

fn embedding_identity_predicate(
    model_id: &str,
    max_embed_tokens: i32,
    extra: Option<&Predicate>,
) -> Predicate {
    let mut predicates = vec![
        Predicate::Eq("model_id", model_id.into()),
        Predicate::Eq("max_embed_tokens", max_embed_tokens.into()),
    ];
    if let Some(extra) = extra {
        predicates.push(extra.clone());
    }
    Predicate::And(predicates)
}

fn in_predicate(column: &'static str, values: &[String]) -> Predicate {
    Predicate::In(
        column,
        values.iter().cloned().map(ScalarValue::String).collect(),
    )
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

pub(crate) const SESSIONS: &str = "sessions.lance";
pub(crate) const MESSAGES: &str = "messages.lance";
pub(crate) const PARTS: &str = "parts.lance";
pub(crate) const EMBEDDINGS: &str = "embeddings.lance";

/// Fixed embedding vector dimension (Qwen3-Embedding-0.6B, design.md 3.2.4).
/// A future model with a different dim activates a second `embeddings` table.
pub const EMBEDDING_DIM: usize = 1024;

/// `auto_cleanup` retention is scheme-keyed (design.md 3.2.0): local-FS gets
/// 30 days; object stores get 90 days because hosted recovery scenarios
/// (sources deleted, sessions expired, re-ingest impossible) need a longer
/// rollback window and storage cost is negligible for append-only workloads.
pub(crate) fn write_params(location: &Url) -> WriteParams {
    let retention_days = if config::is_local(location) { 30 } else { 90 };
    WriteParams {
        data_storage_version: Some(LanceFileVersion::V2_2),
        enable_v2_manifest_paths: true,
        enable_stable_row_ids: true,
        auto_cleanup: Some(AutoCleanupParams {
            interval: 20,
            older_than: chrono::TimeDelta::days(retention_days),
        }),
        ..WriteParams::default()
    }
}

pub(crate) fn session_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("id", DataType::Utf8, false),
        Field::new("parent_session_id", DataType::Utf8, true),
        Field::new("parent_message_id", DataType::Utf8, true),
        Field::new("source_agent", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("project", DataType::Utf8, true),
        Field::new("options", DataType::Utf8, false),
    ]))
}

pub(crate) fn message_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("session_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("role", DataType::Utf8, false),
        Field::new("source_agent", DataType::Utf8, false),
        Field::new("project", DataType::Utf8, true),
        Field::new("content", DataType::Utf8, true),
        Field::new("search_text", DataType::Utf8, true),
        Field::new("options", DataType::Utf8, false),
    ]))
}

pub(crate) fn part_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("message_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new("ordinal", DataType::Int32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("options", DataType::Utf8, false),
        Field::new("variant_data", DataType::Utf8, false),
        blob_field("data", true),
    ]))
}

pub(crate) fn embedding_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("message_id", DataType::Utf8, false),
        primary_field("model_id", DataType::Utf8, false),
        // Part of the PK: `max_embed_tokens` is the tokenizer truncation point,
        // so it changes which prefix of a long message is embedded and thus the
        // vector itself. Folding it into the key means a cap change re-embeds
        // the affected (over-cap) tail under a distinct row instead of silently
        // leaving a stale vector under `(message_id, model_id)`. See design 3.2.4.
        primary_field("max_embed_tokens", DataType::Int32, false),
        Field::new("vector", embedding_vector_type(), false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("source_agent", DataType::Utf8, false),
        Field::new("project", DataType::Utf8, true),
        Field::new("role", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]))
}

pub(crate) fn empty_batch(schema: Arc<Schema>) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| lance::deps::arrow_array::new_empty_array(field.data_type()))
        .collect();
    RecordBatch::try_new(schema, arrays).context("failed to build empty Lance batch")
}

pub(crate) fn empty_reader(
    schema: Arc<Schema>,
) -> Result<
    RecordBatchIterator<
        std::vec::IntoIter<Result<RecordBatch, lance::deps::arrow_schema::ArrowError>>,
    >,
> {
    let batch = empty_batch(schema.clone())?;
    Ok(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

pub(crate) struct MessageBatchRow<'a> {
    pub message: &'a Message,
    pub source_agent: &'a str,
    pub project: Option<&'a str>,
    pub search_text: Option<&'a str>,
}

/// One row of the `embeddings` dataset: a (message, model) vector with the
/// filter columns denormalized from `messages` (design.md 3.2.4). One message
/// produces exactly one vector.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingRow {
    pub message_id: String,
    pub model_id: String,
    /// Tokenizer truncation point this vector was embedded under - a PK
    /// component, since it determines which prefix of the message was embedded.
    pub max_embed_tokens: i32,
    pub vector: Vec<f32>,
    pub session_id: String,
    pub source_agent: String,
    pub project: Option<String>,
    pub role: String,
    pub timestamp: DateTime<Utc>,
}

fn embedding_vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, true)),
        EMBEDDING_DIM as i32,
    )
}

pub(crate) fn embeddings_batch(rows: &[EmbeddingRow]) -> Result<RecordBatch> {
    let schema = embedding_schema();
    let mut flat = Vec::with_capacity(rows.len() * EMBEDDING_DIM);
    for row in rows {
        if row.vector.len() != EMBEDDING_DIM {
            anyhow::bail!(
                "embedding for message {} has dim {}, expected {EMBEDDING_DIM}",
                row.message_id,
                row.vector.len(),
            );
        }
        flat.extend_from_slice(&row.vector);
    }
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        EMBEDDING_DIM as i32,
        Arc::new(Float32Array::from(flat)),
        None,
    )
    .context("failed to build embedding vector column")?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.model_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter()
                    .map(|row| row.max_embed_tokens)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(vectors),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.session_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.source_agent.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.project.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.role.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| micros(row.timestamp))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
        ],
    )
    .context("failed to build embeddings batch")
}

pub(crate) fn sessions_batch(sessions: &[Session]) -> Result<RecordBatch> {
    let schema = session_schema();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.parent_session_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.parent_message_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.source_agent.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    sessions
                        .iter()
                        .map(|session| micros(session.created_at))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.project.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| json_string(&session.options))
                    .collect::<Result<Vec<_>>>()?,
            )),
        ],
    )
    .context("failed to build session batch")
}

pub(crate) fn messages_batch(rows: &[MessageBatchRow<'_>]) -> Result<RecordBatch> {
    let schema = message_schema();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message.session_id())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.message.id()).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| micros(row.message.timestamp()))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message.role().as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.source_agent).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.project).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message.system_content())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.search_text).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| json_string(row.message.options()))
                    .collect::<Result<Vec<_>>>()?,
            )),
        ],
    )
    .context("failed to build message batch")
}

pub(crate) fn parts_batch(parts: &[Part]) -> Result<RecordBatch> {
    let schema = part_schema();
    let mut variant_data = Vec::with_capacity(parts.len());
    let mut blobs = BlobArrayBuilder::new(parts.len());
    for part in parts {
        variant_data.push(part_variant_json(&part.kind)?);
        match &part.kind {
            PartKind::File { data, .. } => match data {
                FileData::String(value) => blobs.push_bytes(value.as_bytes())?,
                FileData::Bytes(value) => blobs.push_bytes(value)?,
                FileData::Url(value) => blobs.push_uri(value)?,
            },
            PartKind::Text { .. }
            | PartKind::Reasoning { .. }
            | PartKind::ToolCall { .. }
            | PartKind::ToolResult { .. }
            | PartKind::ToolApprovalRequest { .. }
            | PartKind::ToolApprovalResponse { .. } => blobs.push_empty()?,
        }
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| part.message_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| part.id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                parts.iter().map(|part| part.ordinal).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| part.kind.type_name())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| json_string(&part.options))
                    .collect::<Result<Vec<_>>>()?,
            )),
            Arc::new(StringArray::from(variant_data)),
            blobs.finish()?,
        ],
    )
    .context("failed to build parts batch")
}

pub(crate) fn session_from_batch(batch: &RecordBatch, row: usize) -> Result<Session> {
    Ok(Session {
        id: string(batch, "id", row)?.context("session id is null")?,
        parent_session_id: string(batch, "parent_session_id", row)?,
        parent_message_id: string(batch, "parent_message_id", row)?,
        source_agent: string(batch, "source_agent", row)?.context("source_agent is null")?,
        created_at: datetime(batch, "created_at", row)?,
        project: string(batch, "project", row)?,
        options: json_parse(&string(batch, "options", row)?.context("options is null")?)?,
    })
}

pub(crate) fn message_from_batch(batch: &RecordBatch, row: usize) -> Result<Message> {
    let id = string(batch, "id", row)?.context("message id is null")?;
    let session_id = string(batch, "session_id", row)?.context("message session_id is null")?;
    let timestamp = datetime(batch, "timestamp", row)?;
    let options = json_parse(&string(batch, "options", row)?.context("message options is null")?)?;

    match string(batch, "role", row)?
        .context("message role is null")?
        .as_str()
    {
        "system" => Ok(Message::System {
            id,
            session_id,
            timestamp,
            content: string(batch, "content", row)?.unwrap_or_default(),
            options,
        }),
        "user" => Ok(Message::User {
            id,
            session_id,
            timestamp,
            options,
        }),
        "assistant" => Ok(Message::Assistant {
            id,
            session_id,
            timestamp,
            options,
        }),
        "tool" => Ok(Message::Tool {
            id,
            session_id,
            timestamp,
            options,
        }),
        other => anyhow::bail!("unknown message role {other}"),
    }
}

/// Decode one `messages` row (projected by `Store::pending_messages_stream`)
/// into a `PendingMessage` for the embed worker.
pub(crate) fn pending_message_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<PendingMessage> {
    Ok(PendingMessage {
        message_id: string(batch, "id", row)?.context("message id is null")?,
        session_id: string(batch, "session_id", row)?.context("session_id is null")?,
        source_agent: string(batch, "source_agent", row)?.context("source_agent is null")?,
        project: string(batch, "project", row)?,
        role: string(batch, "role", row)?.context("role is null")?,
        timestamp: datetime(batch, "timestamp", row)?,
        search_text: string(batch, "search_text", row)?.context("search_text is null")?,
    })
}

pub(crate) fn part_from_batch(batch: &RecordBatch, row: usize) -> Result<Part> {
    let type_name = string(batch, "type", row)?.context("part type is null")?;
    let variant_data = string(batch, "variant_data", row)?.context("variant_data is null")?;
    Ok(Part {
        message_id: string(batch, "message_id", row)?.context("part message_id is null")?,
        id: string(batch, "id", row)?.context("part id is null")?,
        ordinal: int32(batch, "ordinal", row)?,
        options: json_parse(&string(batch, "options", row)?.context("part options is null")?)?,
        kind: part_kind_from_json(&type_name, &variant_data)?,
    })
}

pub(crate) fn string(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<String>> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("column {name} is not Utf8"))?;
    if array.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(array.value(row).to_owned()))
    }
}

fn int32(batch: &RecordBatch, name: &str, row: usize) -> Result<i32> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .with_context(|| format!("column {name} is not Int32"))?;
    Ok(array.value(row))
}

pub(crate) fn float32(batch: &RecordBatch, name: &str, row: usize) -> Result<f32> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<Float32Array>()
        .with_context(|| format!("column {name} is not Float32"))?;
    Ok(array.value(row))
}

pub(crate) fn datetime(batch: &RecordBatch, name: &str, row: usize) -> Result<DateTime<Utc>> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .with_context(|| format!("column {name} is not timestamp_micros"))?;
    Utc.timestamp_micros(array.value(row))
        .single()
        .context("timestamp is out of range")
}

fn primary_field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(
        [(
            "lance-schema:unenforced-primary-key".to_owned(),
            "true".to_owned(),
        )]
        .into(),
    )
}

fn micros(timestamp: DateTime<Utc>) -> i64 {
    timestamp.timestamp_micros()
}

fn json_string<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("failed to serialize JSON field")
}

fn json_parse<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).context("failed to parse JSON field")
}

fn part_variant_json(kind: &PartKind) -> Result<String> {
    let value = serde_json::to_value(kind)?;
    let mut object = value
        .as_object()
        .cloned()
        .context("part variant did not serialize to an object")?;
    object.remove("type");
    serde_json::to_string(&object).context("failed to serialize part variant")
}

fn part_kind_from_json(type_name: &str, variant_data: &str) -> Result<PartKind> {
    let mut value = json_parse::<Value>(variant_data)?;
    let object = value
        .as_object_mut()
        .context("part variant data is not an object")?;
    object.insert("type".to_owned(), Value::String(type_name.to_owned()));
    serde_json::from_value(value).context("failed to parse part kind")
}
