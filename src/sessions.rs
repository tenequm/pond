use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
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
    pub project: String,
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
    pub project: String,
    pub source_agent: String,
    pub timestamp: DateTime<Utc>,
    pub search_text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertStatus {
    Inserted,
    Matched,
}

/// What `pond status` reports: where the data lives, total rows per table,
/// and a per-(adapter, project) breakdown built from one `messages` scan.
#[derive(Debug, Clone)]
pub struct CorpusStats {
    pub data_url: Url,
    pub totals: RowTotals,
    /// One entry per adapter present in the corpus. When `include_subagents`
    /// is false (the CLI default), sub-agent rows (`source_agent` containing
    /// `/`) are filtered out so only the main-agent sessions appear. When
    /// true, each distinct `source_agent` (e.g. `claude-code/general-purpose`)
    /// gets its own entry. Always in alphabetical order; the CLI re-sorts
    /// this into registry order at render time so the tree matches the
    /// discovery picker.
    pub adapters: Vec<AdapterStats>,
    /// Whether the rollup includes sub-agent sessions. The renderer prints a
    /// hint about `--include-subagents` when this is false so users know the
    /// `totals` row above counts sessions that aren't broken down below.
    pub include_subagents: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowTotals {
    pub sessions: u64,
    pub messages: u64,
    pub parts: u64,
    pub embeddings: u64,
}

#[derive(Debug, Clone)]
pub struct AdapterStats {
    /// Either the main-agent name (`claude-code`) when sub-agents are filtered
    /// out, or the full `source_agent` (`claude-code/general-purpose`) when
    /// `include_subagents` is on.
    pub adapter: String,
    pub sessions: u64,
    pub messages: u64,
    /// Projects under this adapter, sorted by message count desc, then by
    /// project name asc.
    pub projects: Vec<ProjectStats>,
}

#[derive(Debug, Clone)]
pub struct ProjectStats {
    pub project: String,
    pub sessions: u64,
    pub messages: u64,
}

#[derive(Default)]
struct GroupAccumulator {
    messages: u64,
    session_ids: HashSet<String>,
}

/// Disk usage for a local data dir, attributed to a top-level table dir.
/// Returned only when the data dir is on the local filesystem; remote
/// backends populate via [`CorpusStats::query_remote_sizes`] instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageSizes {
    pub sessions: u64,
    pub messages: u64,
    pub parts: u64,
    pub embeddings: u64,
    pub other: u64,
}

impl StorageSizes {
    pub fn total(&self) -> u64 {
        self.sessions + self.messages + self.parts + self.embeddings + self.other
    }

    /// Walk `root` (a local directory) recursively and attribute file sizes
    /// to the table they belong to by the top-level child directory name.
    /// Anything outside the four known dirs (config.toml, index temp files,
    /// ...) is counted under `other`.
    pub fn from_local_dir(root: &std::path::Path) -> Result<Self> {
        let mut sizes = StorageSizes::default();
        if !root.exists() {
            return Ok(sizes);
        }
        let mut stack: Vec<(PathBuf, Option<&'static str>)> = Vec::new();
        for entry in
            std::fs::read_dir(root).with_context(|| format!("read data dir {}", root.display()))?
        {
            let entry = entry?;
            let name = entry.file_name();
            // Lance writes each table as `<name>.lance/`; strip the suffix
            // before matching so the four known tables are attributed
            // correctly. Anything else (config.toml, index temp files, ...)
            // falls through to `other`.
            let stem = name.to_str().map(|s| s.strip_suffix(".lance").unwrap_or(s));
            let attribute = match stem {
                Some("sessions") => Some("sessions"),
                Some("messages") => Some("messages"),
                Some("parts") => Some("parts"),
                Some("embeddings") => Some("embeddings"),
                _ => None,
            };
            stack.push((entry.path(), attribute));
        }
        while let Some((path, attribute)) = stack.pop() {
            let metadata = std::fs::symlink_metadata(&path)
                .with_context(|| format!("stat {}", path.display()))?;
            if metadata.is_dir() {
                for entry in
                    std::fs::read_dir(&path).with_context(|| format!("read {}", path.display()))?
                {
                    let entry = entry?;
                    stack.push((entry.path(), attribute));
                }
            } else if metadata.is_file() {
                let bytes = metadata.len();
                match attribute {
                    Some("sessions") => sizes.sessions += bytes,
                    Some("messages") => sizes.messages += bytes,
                    Some("parts") => sizes.parts += bytes,
                    Some("embeddings") => sizes.embeddings += bytes,
                    _ => sizes.other += bytes,
                }
            }
        }
        Ok(sizes)
    }
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

    /// Batched write path used by the adapter ingest loop and by the wire
    /// handler's final flush. Receives N completed substreams from the
    /// validator and:
    ///
    ///   1. Runs the immutable-fields check (3.6.4) against the stored row
    ///      per session, sequentially. Sessions that fail produce one Error
    ///      outcome and are excluded from the write batch.
    ///   2. Deduplicates in-batch at the substream level: when two substreams
    ///      in the same batch share a `session_id` (Claude Code's subagent
    ///      files reuse their parent's id), the first occurrence wins. The
    ///      second is either *merged* (same `source_agent` + `project`:
    ///      messages/parts append, no duplicate rows) or *rejected*
    ///      (different `project` - the subagent-vs-parent case). Row-level
    ///      duplicates that slip past here are caught downstream by Lance's
    ///      `SourceDedupeBehavior::FirstSeen` in `substrate::merge_insert`
    ///      (invariant 17): this layer's job is preserving substream merge
    ///      semantics, not policing the PK uniqueness Lance handles itself.
    ///   3. Builds one combined `RecordBatch` per table (sessions, messages,
    ///      parts) across every valid substream.
    ///   4. Fires the three `merge_insert` calls in parallel via
    ///      `tokio::try_join!`. Cross-table mutex on `CachedDataset` is
    ///      independent, so these proceed concurrently. Single-commit-per-
    ///      table replaces the previous one-commit-per-session-per-table
    ///      pattern (3 commits N times -> 3 commits total).
    ///   5. Composes per-session [`RowOutcome`]s in original substream order.
    async fn upsert_session_batch(
        &self,
        substreams: Vec<CompletedSubstream>,
    ) -> Result<Vec<RowOutcome>> {
        if substreams.is_empty() {
            return Ok(Vec::new());
        }

        let mut outcomes: Vec<RowOutcome> = Vec::with_capacity(substreams.len());

        // Step 2 - in-batch dedup. Build an ordered map: first occurrence of
        // each session_id wins; later occurrences either merge or get
        // rejected. Iteration order preserves original substream order so
        // outcomes index correctly.
        let mut merged: Vec<CompletedSubstream> = Vec::with_capacity(substreams.len());
        let mut by_session_id: std::collections::HashMap<String, usize> =
            std::collections::HashMap::with_capacity(substreams.len());
        for substream in substreams {
            if let Some(&existing_idx) = by_session_id.get(&substream.session.id) {
                let existing = &merged[existing_idx];
                if existing.session.source_agent != substream.session.source_agent
                    || existing.session.project != substream.session.project
                {
                    // Subagent-vs-parent class. The first occurrence's
                    // metadata stays authoritative; this substream is
                    // rejected on the same immutable-field axis as the
                    // storage-side check.
                    let reason = if existing.session.source_agent != substream.session.source_agent
                    {
                        IngestError::ImmutableField {
                            field: "source_agent",
                            session_id: substream.session.id.clone(),
                            stored: existing.session.source_agent.clone(),
                            attempted: substream.session.source_agent.clone(),
                        }
                    } else {
                        IngestError::ImmutableField {
                            field: "project",
                            session_id: substream.session.id.clone(),
                            stored: (*existing.session.project).clone(),
                            attempted: (*substream.session.project).clone(),
                        }
                    };
                    let field = match &reason {
                        IngestError::ImmutableField { field, .. } => Some(*field),
                    };
                    let reason_key = match field {
                        Some("project") => DROP_REASON_IMMUTABLE_PROJECT,
                        Some("source_agent") => DROP_REASON_IMMUTABLE_SOURCE_AGENT,
                        _ => DROP_REASON_UNCATEGORIZED,
                    };
                    outcomes.extend(error_outcomes_for_substream(
                        substream.session_index,
                        &substream.session,
                        &substream.messages,
                        reason.to_string(),
                        field,
                        reason_key,
                    ));
                    continue;
                }
                // Same session, same metadata: merge messages. Dedup message
                // ids defensively (within one batch, the validator's seen
                // sets are per-substream so cross-substream dups can happen
                // legally if both files re-emit the same row).
                let existing = &mut merged[existing_idx];
                let mut seen: std::collections::HashSet<String> = existing
                    .messages
                    .iter()
                    .map(|m| m.message.id().to_owned())
                    .collect();
                for msg in substream.messages {
                    if seen.insert(msg.message.id().to_owned()) {
                        existing.messages.push(msg);
                    }
                }
                continue;
            }
            by_session_id.insert(substream.session.id.clone(), merged.len());
            merged.push(substream);
        }

        // Step 1 - immutable check against storage, sequentially per
        // surviving substream.
        let mut writeable: Vec<CompletedSubstream> = Vec::with_capacity(merged.len());
        for substream in merged {
            if let Some(existing) = self.find_session(&substream.session.id).await?
                && let Err(failure) = ensure_immutable_match(&existing, &substream.session)
            {
                let field = match &failure {
                    IngestError::ImmutableField { field, .. } => Some(*field),
                };
                let reason_key = match field {
                    Some("project") => DROP_REASON_IMMUTABLE_PROJECT,
                    Some("source_agent") => DROP_REASON_IMMUTABLE_SOURCE_AGENT,
                    _ => DROP_REASON_UNCATEGORIZED,
                };
                outcomes.extend(error_outcomes_for_substream(
                    substream.session_index,
                    &substream.session,
                    &substream.messages,
                    failure.to_string(),
                    field,
                    reason_key,
                ));
                continue;
            }
            writeable.push(substream);
        }

        if writeable.is_empty() {
            outcomes.sort_by_key(|outcome| outcome.index);
            return Ok(outcomes);
        }

        // Build the three flat record batches across every valid substream.
        let sessions_owned: Vec<Session> = writeable
            .iter()
            .map(|substream| substream.session.clone())
            .collect();
        let message_rows: Vec<MessageBatchRow<'_>> = writeable
            .iter()
            .flat_map(|substream| {
                substream.messages.iter().map(|buffered| MessageBatchRow {
                    message: &buffered.message,
                    source_agent: &substream.session.source_agent,
                    project: &substream.session.project,
                    search_text: buffered.search_text.as_deref(),
                })
            })
            .collect();
        let part_rows: Vec<Part> = writeable
            .iter()
            .flat_map(|substream| {
                substream.messages.iter().flat_map(|buffered| {
                    buffered
                        .parts
                        .iter()
                        .map(|buffered_part| buffered_part.part.clone())
                })
            })
            .collect();

        let sessions_batch = sessions_batch(&sessions_owned)?;
        let messages_batch = messages_batch(&message_rows)?;
        let parts_batch = parts_batch(&part_rows)?;

        let sessions_count = sessions_owned.len();
        let messages_count = message_rows.len();
        let parts_count = part_rows.len();

        let (sessions_inserted, messages_inserted, parts_inserted) = tokio::try_join!(
            self.handle
                .merge_insert(Table::Sessions, sessions_batch, sessions_count),
            async {
                if messages_count == 0 {
                    Ok::<u64, anyhow::Error>(0)
                } else {
                    self.handle
                        .merge_insert(Table::Messages, messages_batch, messages_count)
                        .await
                }
            },
            async {
                if parts_count == 0 {
                    Ok::<u64, anyhow::Error>(0)
                } else {
                    self.handle
                        .merge_insert(Table::Parts, parts_batch, parts_count)
                        .await
                }
            },
        )?;

        // Per-session success outcomes: each substream's own status row plus
        // per-message and per-part rows. The Lance `merge_insert` returns a
        // single batch-level "inserted vs matched" count, not per-row; we
        // can't tell which row matched which, so for the batched path each
        // session/message/part is marked `Inserted` if the batch had any
        // inserts, else `Matched`. This is the same semantic the
        // single-session path used (see `statuses_from_inserted`).
        let sessions_status = if sessions_inserted == sessions_count as u64 {
            UpsertStatus::Inserted
        } else if sessions_inserted == 0 {
            UpsertStatus::Matched
        } else {
            // Mixed - mark per index by re-checking? Cheaper to just count
            // all as Inserted; the operator can read the precise insert
            // count from the `IngestSummary`.
            UpsertStatus::Inserted
        };
        let _ = messages_inserted; // counted via summary, not per-row here
        let _ = parts_inserted;

        for substream in &writeable {
            outcomes.extend(success_outcomes_for_substream(
                substream.session_index,
                &substream.session,
                &substream.messages,
                sessions_status,
            ));
        }

        outcomes.sort_by_key(|outcome| outcome.index);
        Ok(outcomes)
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
                project: &session.project,
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

    /// `session_id -> wall-clock time of the Lance manifest version that
    /// last wrote the row` for the per-session staleness skip
    /// (design.md 3.4). Reads Lance's `_row_last_updated_at_version` system
    /// column (available because pond enables stable row ids per 3.2.0)
    /// and joins it against `Dataset::versions()` for commit timestamps.
    pub async fn session_last_ingested_at(&self) -> Result<HashMap<String, DateTime<Utc>>> {
        use lance::deps::arrow_array::UInt64Array;

        let dataset = self.handle.dataset(Table::Sessions).await?;
        let versions: HashMap<u64, DateTime<Utc>> = dataset
            .versions()
            .await?
            .into_iter()
            .map(|v| (v.version, v.timestamp))
            .collect();

        let mut scanner = dataset.scan();
        scanner.project(&["id", "_row_last_updated_at_version"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut out: HashMap<String, DateTime<Utc>> = HashMap::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let version_array = batch
                .column_by_name("_row_last_updated_at_version")
                .context("missing _row_last_updated_at_version column")?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("_row_last_updated_at_version is not UInt64")?;
            for row in 0..batch.num_rows() {
                let Some(id) = string(&batch, "id", row)? else {
                    continue;
                };
                if version_array.is_null(row) {
                    continue;
                }
                let version = version_array.value(row);
                if let Some(ts) = versions.get(&version).copied() {
                    out.insert(id, ts);
                }
            }
        }
        Ok(out)
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

    /// Compute the per-adapter / per-project rollup that drives
    /// `pond status`. One scan over `messages` projecting the three
    /// columns the rollup keys on (`source_agent`, `project`, `session_id`),
    /// aggregated in-memory. Bounded by the cross product of adapters and
    /// projects, which stays small on real corpora.
    pub async fn corpus_stats(&self, include_subagents: bool) -> Result<CorpusStats> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        let mut scanner = dataset.scan();
        scanner.project(&["source_agent", "project", "session_id"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut groups: HashMap<(String, String), GroupAccumulator> = HashMap::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let source_agent = string(&batch, "source_agent", row)?.unwrap_or_default();
                let project = string(&batch, "project", row)?.unwrap_or_default();
                let session_id = string(&batch, "session_id", row)?.unwrap_or_default();
                let is_subagent = source_agent.contains('/');
                if is_subagent && !include_subagents {
                    continue;
                }
                let entry = groups.entry((source_agent, project)).or_default();
                entry.messages += 1;
                entry.session_ids.insert(session_id);
            }
        }

        let (totals_sessions, totals_messages, totals_parts, totals_embeddings) =
            self.handle.row_counts().await?;
        let totals = RowTotals {
            sessions: totals_sessions as u64,
            messages: totals_messages as u64,
            parts: totals_parts as u64,
            embeddings: totals_embeddings as u64,
        };

        let mut by_adapter: BTreeMap<String, Vec<ProjectStats>> = BTreeMap::new();
        for ((adapter, project), acc) in groups {
            by_adapter.entry(adapter).or_default().push(ProjectStats {
                project,
                sessions: acc.session_ids.len() as u64,
                messages: acc.messages,
            });
        }

        let mut adapters = Vec::with_capacity(by_adapter.len());
        for (adapter, mut projects) in by_adapter {
            projects.sort_by(|a, b| {
                b.messages
                    .cmp(&a.messages)
                    .then_with(|| a.project.cmp(&b.project))
            });
            let sessions: u64 = projects.iter().map(|p| p.sessions).sum();
            let messages: u64 = projects.iter().map(|p| p.messages).sum();
            adapters.push(AdapterStats {
                adapter,
                sessions,
                messages,
                projects,
            });
        }

        Ok(CorpusStats {
            data_url: self.handle.location().clone(),
            totals,
            adapters,
            include_subagents,
        })
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
        // Lance ships an autoprojection that silently appends `_score` to FTS
        // output when the projection omits it. That behavior is going away;
        // we opt into the future explicit-projection contract here so the
        // scanner stops emitting a per-call deprecation warning, and we list
        // `_score` ourselves since the loop below reads it.
        scanner.disable_scoring_autoprojection();
        scanner.project(&["id", "_score"])?;
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
        // Mirror the explicit-projection contract from `fts_search`: opt out
        // of `_distance` autoprojection and list it ourselves since the loop
        // below reads it.
        scanner.disable_scoring_autoprojection();
        scanner.project(&["message_id", "_distance"])?;
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
                project: string(&batch, "project", row)?.context("project is null")?,
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

    pub async fn maintenance(&self, retention: chrono::Duration) -> MaintenanceReport {
        self.handle.maintenance(retention).await
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
    pub async fn session_messages(&self, session_id: &str) -> Result<Vec<MessageWithParts>> {
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
///
/// Fields are bucketed by population so the summary never conflates "100
/// validator-rejected rows in 1 bad session" with "100 separate failures."
/// The shape is set by design.md 3.4 (post-2026-05-15 rewrite).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IngestSummary {
    /// Rows actually written to Lance.
    pub inserted: usize,
    /// Rows that already existed (merge_insert no-op match).
    pub matched: usize,
    /// Events the validator dropped under per-event-drop policy (ordering
    /// violation, orphan part, mismatched parent, adapter parse failure,
    /// duplicate-id collision, ...). Counted by event, not by session: a
    /// session with one bad part stays in this bucket as 1, not as "the
    /// whole substream." Per design.md 3.x, adapters SHOULD dedupe their
    /// own emissions upstream when source replay is expected; the
    /// validator's in-batch HashSet is a safety net, not a feature
    /// adapters may rely on. If this bucket grows on a clean adapter,
    /// inspect `drop_reasons` for the top contributors.
    pub dropped_events: usize,
    /// Sessions whose Session-level invariants (immutable `source_agent` /
    /// `project` against a previously-stored row) failed at flush time and
    /// whose substream got rejected wholesale. Always small relative to
    /// `inserted`; if not, there's a real problem to investigate.
    pub dropped_sessions: usize,
    /// Files the adapter couldn't decode at all (no Session header
    /// extractable: empty `.jsonl`, missing required field).
    pub skipped_files: usize,
    /// Sessions short-circuited via the per-session staleness skip
    /// (design.md 3.4): file `mtime` was at or before the wall-clock time
    /// pond last wrote that session's row, so re-decode was bypassed.
    pub skipped_fresh: usize,
    /// Storage-layer failures whose retries were exhausted (commit
    /// conflicts, transient IO that didn't recover). Hard zero on healthy
    /// runs.
    pub storage_errors: usize,
    /// Histogram of stable reason keys for the combined `dropped_events +
    /// dropped_sessions` populations. Keys are `&'static str` (see the
    /// `DROP_REASON_*` constants) so consumers can match by identity.
    /// Empty on a clean run. Used by `pond sync` to print the top reasons
    /// and by `benches/ingest_bench.rs` to bucket Partial drops (which
    /// previously carried only a count, no reason).
    pub drop_reasons: BTreeMap<&'static str, usize>,
}

/// Stable reason keys for the `IngestSummary::drop_reasons` histogram and
/// the per-row `RowError::reason_key`. `&'static str` so consumers can
/// match by identity rather than prose. Adding a new variant: pick a short
/// snake_case identifier, route it from the validator/adapter, and update
/// the doc table in `docs/design.md` 3.4.
pub const DROP_REASON_DUPLICATE_MESSAGE_ID: &str = "duplicate_message_id";
pub const DROP_REASON_DUPLICATE_PART_KEY: &str = "duplicate_part_key";
pub const DROP_REASON_MESSAGE_BEFORE_SESSION: &str = "message_before_session";
pub const DROP_REASON_MESSAGE_SESSION_MISMATCH: &str = "message_session_mismatch";
pub const DROP_REASON_PART_BEFORE_MESSAGE: &str = "part_before_message";
pub const DROP_REASON_PART_MESSAGE_MISMATCH: &str = "part_message_mismatch";
pub const DROP_REASON_EMPTY_SOURCE_AGENT: &str = "empty_source_agent";
pub const DROP_REASON_PARENT_MESSAGE_WITHOUT_SESSION: &str = "parent_message_without_session";
pub const DROP_REASON_MISSING_PROJECT: &str = "missing_project";
pub const DROP_REASON_IMMUTABLE_PROJECT: &str = "immutable_project";
pub const DROP_REASON_IMMUTABLE_SOURCE_AGENT: &str = "immutable_source_agent";
pub const DROP_REASON_ADAPTER_PARSE: &str = "adapter_parse";
pub const DROP_REASON_ADAPTER_IO: &str = "adapter_io";
pub const DROP_REASON_ADAPTER_SCHEMA: &str = "adapter_schema";
pub const DROP_REASON_UNCATEGORIZED: &str = "uncategorized";

impl IngestSummary {
    pub fn accepted(&self) -> usize {
        self.inserted + self.matched
    }

    pub fn add_outcomes(&mut self, outcomes: &[RowOutcome]) {
        for outcome in outcomes {
            match outcome.status {
                OutcomeStatus::Inserted => self.inserted += 1,
                OutcomeStatus::Matched => self.matched += 1,
                OutcomeStatus::Error => {
                    // A whole-substream rejection arrives as exactly one
                    // session-kind Error outcome (see
                    // `error_outcomes_for_substream`). Per-event drops
                    // arrive as one Error each on message/part. Keeping the
                    // populations distinct is the whole point of the new
                    // `IngestSummary` shape.
                    if outcome.kind == "session" {
                        self.dropped_sessions += 1;
                    } else {
                        self.dropped_events += 1;
                    }
                    let reason = outcome
                        .error
                        .as_ref()
                        .and_then(|e| e.reason_key)
                        .unwrap_or(DROP_REASON_UNCATEGORIZED);
                    *self.drop_reasons.entry(reason).or_insert(0) += 1;
                }
            }
        }
    }

    /// Record a drop-reason charge that did not flow through a `RowOutcome`
    /// (currently used by the adapter path for bad-line parse failures
    /// mid-session, where `handlers.rs` bumps `dropped_events` directly).
    pub fn record_drop_reason(&mut self, reason: &'static str) {
        *self.drop_reasons.entry(reason).or_insert(0) += 1;
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
    /// Stable key for histogramming - see `DROP_REASON_*` constants. The
    /// `reason` field above is human-prose; `reason_key` is the machine
    /// bucket. `None` means uncategorized; consumers attribute to
    /// `DROP_REASON_UNCATEGORIZED`.
    pub reason_key: Option<&'static str>,
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
/// batches: sessions, messages, parts). A validation error on a single event
/// drops *that event* (one [`OutcomeStatus::Error`] outcome) and the substream
/// continues; only Session-level invariants (immutable source_agent / project
/// on re-write) drop the whole substream. The N-events-per-rejection cascade
/// from the prior contract is gone (see design.md 3.4 "Ordering enforcement").
///
/// Writes are batched at flush time. As complete substreams arrive (a new
/// `Session` event closes out the previous one), they accumulate in
/// `completed` rather than each one calling `merge_insert` immediately.
/// The caller drains the buffer via [`Self::flush`] / [`Self::finish`],
/// at which point one batched 3-parallel-merge-insert covers all pending
/// substreams. This is the load-bearing perf change: per-substream commit
/// overhead dominated the ingest profile (see `benches/ingest_bench.rs`),
/// and amortizing it across N sessions cuts wall time materially.
#[derive(Debug, Default)]
pub struct IngestValidator {
    session: Option<BufferedSession>,
    current_message: Option<BufferedMessage>,
    current_parts: Vec<BufferedPart>,
    messages: Vec<BufferedMessage>,
    /// Message ids already buffered in the current substream. Duplicate ids
    /// drop the offending event in-line rather than failing the whole batch
    /// downstream.
    seen_message_ids: HashSet<String>,
    /// `(message_id, part_id)` keys already buffered in the current
    /// substream. Same in-line duplicate-drop policy as `seen_message_ids`.
    seen_part_keys: HashSet<(String, String)>,
    /// Substreams whose end-of-stream boundary has been observed but whose
    /// rows haven't been written yet. Flushed in batched mode by
    /// [`Self::flush`].
    completed: Vec<CompletedSubstream>,
}

/// One closed substream ready for the batched flush path.
#[derive(Debug)]
struct CompletedSubstream {
    session_index: usize,
    session: Session,
    messages: Vec<BufferedMessage>,
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

    /// Final flush at end-of-batch. Closes the in-flight substream and
    /// drains the pending-flush buffer.
    pub async fn finish(&mut self, store: &Store) -> Result<Vec<RowOutcome>> {
        self.close_current_substream();
        self.flush(store).await
    }

    /// Drain every completed substream into batched 3-parallel-merge_insert
    /// writes. Caller invokes this periodically (every N completed
    /// substreams) to keep memory bounded; in adapter-driven sync that
    /// happens via the BATCH_SIZE check in `ingest_adapter`. The current
    /// in-flight substream stays buffered - close it explicitly via
    /// [`Self::finish`] or by feeding the next Session event.
    pub async fn flush(&mut self, store: &Store) -> Result<Vec<RowOutcome>> {
        if self.completed.is_empty() {
            return Ok(Vec::new());
        }
        let completed = std::mem::take(&mut self.completed);
        store.upsert_session_batch(completed).await
    }

    /// Number of fully-buffered substreams awaiting batched write. Used by
    /// the adapter caller to decide when to call [`Self::flush`].
    pub fn pending_substreams(&self) -> usize {
        self.completed.len()
    }

    async fn push_session(
        &mut self,
        _store: &Store,
        index: usize,
        mut session: Session,
    ) -> Result<Vec<RowOutcome>> {
        // Close out the previous substream (if any) - move it to the pending
        // buffer instead of writing immediately. The actual write happens
        // when the caller invokes `flush` / `finish`.
        self.close_current_substream();

        // design.md 3.1.3: `source_agent` is trimmed at ingest and rejected
        // if empty after trim. A Session event with empty source_agent is
        // dropped on the spot - the substream that would follow has nothing
        // to anchor on, so subsequent message/part events will also drop.
        let trimmed = session.source_agent.trim();
        if trimmed.is_empty() {
            return Ok(vec![RowOutcome {
                index,
                kind: "session",
                pk: Value::String(session.id.clone()),
                status: OutcomeStatus::Error,
                error: Some(RowError {
                    message: format!("session {} has empty source_agent after trim", session.id),
                    field: Some("source_agent"),
                    reason: None,
                    reason_key: Some(DROP_REASON_EMPTY_SOURCE_AGENT),
                }),
            }]);
        }
        if trimmed.len() != session.source_agent.len() {
            session.source_agent = trimmed.to_owned();
        }

        if session.parent_message_id.is_some() && session.parent_session_id.is_none() {
            return Ok(vec![RowOutcome {
                index,
                kind: "session",
                pk: Value::String(session.id.clone()),
                status: OutcomeStatus::Error,
                error: Some(RowError {
                    message: format!(
                        "session {} has parent_message_id without parent_session_id",
                        session.id,
                    ),
                    field: Some("parent_message_id"),
                    reason: None,
                    reason_key: Some(DROP_REASON_PARENT_MESSAGE_WITHOUT_SESSION),
                }),
            }]);
        }

        self.seen_message_ids.clear();
        self.seen_part_keys.clear();
        self.session = Some(BufferedSession { index, session });
        Ok(Vec::new())
    }

    fn close_current_substream(&mut self) {
        self.flush_current_message();
        let Some(BufferedSession {
            index: session_index,
            session,
        }) = self.session.take()
        else {
            return;
        };
        let messages = std::mem::take(&mut self.messages);
        self.seen_message_ids.clear();
        self.seen_part_keys.clear();
        self.completed.push(CompletedSubstream {
            session_index,
            session,
            messages,
        });
    }

    fn push_message(&mut self, index: usize, message: Message) -> Vec<RowOutcome> {
        let pk = Value::Array(vec![
            Value::String(message.session_id().to_owned()),
            Value::String(message.id().to_owned()),
        ]);
        let Some(session) = &self.session else {
            return vec![error_outcome(
                index,
                "message",
                pk,
                "first event in a session stream must be Session",
                None,
                DROP_REASON_MESSAGE_BEFORE_SESSION,
            )];
        };
        if message.session_id() != session.session.id {
            let msg = format!(
                "message {} references session {}, expected {}",
                message.id(),
                message.session_id(),
                session.session.id
            );
            return vec![error_outcome(
                index,
                "message",
                pk,
                &msg,
                Some("session_id"),
                DROP_REASON_MESSAGE_SESSION_MISMATCH,
            )];
        }
        if !self.seen_message_ids.insert(message.id().to_owned()) {
            // Per invariant 17, substrate FirstSeen would silently drop a
            // same-PK row at the storage layer; we still trap here so the
            // duplicate stays visible in `dropped_events` instead of being
            // absorbed into the substrate's `skipped` counter. Adapters
            // are expected to dedupe upstream (see claude-code's per-file
            // `seen_uuids`); a hit here means an unrecognized replay
            // pattern worth investigating.
            let msg = format!("duplicate message id {} in session substream", message.id());
            return vec![error_outcome(
                index,
                "message",
                pk,
                &msg,
                None,
                DROP_REASON_DUPLICATE_MESSAGE_ID,
            )];
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
        let Some(current) = &self.current_message else {
            return vec![error_outcome(
                index,
                "part",
                pk,
                "part event appeared before a message",
                None,
                DROP_REASON_PART_BEFORE_MESSAGE,
            )];
        };
        if part.message_id != current.message.id() {
            let msg = format!(
                "part {} references message {}, expected {}",
                part.id,
                part.message_id,
                current.message.id()
            );
            return vec![error_outcome(
                index,
                "part",
                pk,
                &msg,
                Some("message_id"),
                DROP_REASON_PART_MESSAGE_MISMATCH,
            )];
        }
        let part_key = (part.message_id.clone(), part.id.clone());
        if !self.seen_part_keys.insert(part_key) {
            // Same visibility rationale as `push_message`: substrate FirstSeen
            // would absorb this silently; trapping here keeps it in
            // `dropped_events` for operator-visible signal.
            let msg = format!(
                "duplicate part id {} for message {} in session substream",
                part.id, part.message_id
            );
            return vec![error_outcome(
                index,
                "part",
                pk,
                &msg,
                None,
                DROP_REASON_DUPLICATE_PART_KEY,
            )];
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
}

fn error_outcome(
    index: usize,
    kind: &'static str,
    pk: Value,
    message: &str,
    field: Option<&'static str>,
    reason_key: &'static str,
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
            reason_key: Some(reason_key),
        }),
    }
}

/// Session-level rejection (immutable `source_agent` / `project` violation):
/// emit exactly one Error outcome on the Session row. The buffered messages
/// and parts of this substream are *not* surfaced as per-row errors - their
/// loss is implied by the single session-rejection. Earlier versions
/// cascaded N error rows per rejected substream; that inflated the operator
/// view ("12,297 errors") for what is structurally one decision
/// ("1 session-level rejection"). See design.md 3.4.
fn error_outcomes_for_substream(
    session_index: usize,
    session: &Session,
    _messages: &[BufferedMessage],
    message: impl Into<String>,
    field: Option<&'static str>,
    reason_key: &'static str,
) -> Vec<RowOutcome> {
    let reason = field.map(|_| "immutable");
    vec![RowOutcome {
        index: session_index,
        kind: "session",
        pk: Value::String(session.id.clone()),
        status: OutcomeStatus::Error,
        error: Some(RowError {
            message: message.into(),
            field,
            reason,
            reason_key: Some(reason_key),
        }),
    }]
}

/// Batched-path success helper: every row in a substream takes the same
/// status (the batch-level `Inserted` vs `Matched` decision from
/// `merge_insert.num_inserted_rows`). The single-session path uses
/// `success_outcomes_from_statuses` instead, which threads per-row
/// statuses from `upsert_session_bundle`'s sequential calls.
fn success_outcomes_for_substream(
    session_index: usize,
    session: &Session,
    messages: &[BufferedMessage],
    status: UpsertStatus,
) -> Vec<RowOutcome> {
    let mut outcomes = Vec::with_capacity(1 + messages.len());
    outcomes.push(success_outcome(
        session_index,
        "session",
        Value::String(session.id.clone()),
        status,
    ));
    for buffered in messages {
        let pk = Value::Array(vec![
            Value::String(buffered.message.session_id().to_owned()),
            Value::String(buffered.message.id().to_owned()),
        ]);
        outcomes.push(success_outcome(buffered.index, "message", pk, status));
        for part in &buffered.parts {
            let part_pk = Value::Array(vec![
                Value::String(part.part.message_id.clone()),
                Value::String(part.part.id.clone()),
            ]);
            outcomes.push(success_outcome(part.index, "part", part_pk, status));
        }
    }
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
            stored: (*existing.project).clone(),
            attempted: (*incoming.project).clone(),
        });
    }
    Ok(())
}

pub fn search_text(message: &Message, parts: &[Part]) -> Option<String> {
    let mut chunks: Vec<String> = Vec::new();
    for part in parts {
        match (message.role(), &part.kind) {
            (Role::User | Role::Assistant, PartKind::Text { text }) => {
                if let Some(text) = text {
                    chunks.push(text.to_string());
                }
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
                | PartKind::ToolCall { .. }
                | PartKind::ToolResult { .. }
                | PartKind::ToolApprovalRequest { .. }
                | PartKind::ToolApprovalResponse { .. },
            ) => {}
        }
    }

    let text = chunks
        .into_iter()
        .filter(|chunk| !chunk.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() { None } else { Some(text) }
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
        Field::new("project", DataType::Utf8, false),
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
        Field::new("project", DataType::Utf8, false),
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
        Field::new("project", DataType::Utf8, false),
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
    pub project: &'a str,
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
    pub project: String,
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
                    .map(|row| row.project.as_str())
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
                    .map(|session| session.project.as_str())
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
        project: crate::adapter::Extracted::from_stored(
            string(batch, "project", row)?.context("project is null")?,
        ),
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
            // `content` is nullable in the schema; preserve the distinction
            // between "no content row stored" (`None`) and "empty string
            // stored" (`Some(extracted_empty)`). The value originally
            // came from a `Source` extraction at ingest time; rewrap via
            // the storage-internal `from_stored` so the type-system seal
            // for adapters stays intact.
            content: string(batch, "content", row)?.map(crate::adapter::Extracted::from_stored),
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
        project: string(batch, "project", row)?.context("project is null")?,
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{
        adapter::Extracted,
        handlers::ingest_events,
        wire::{FileData, Message, Part, PartKind, ProviderOptions, Session},
    };
    use chrono::{Duration, Utc};
    use serde_json::json;
    use tempfile::TempDir;

    fn synthetic_session(id: &str) -> Session {
        Session {
            id: id.to_owned(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: "claude-code".to_owned(),
            created_at: Utc::now(),
            project: crate::adapter::Extracted::from_test_value("/tmp/pond".to_owned()),
            options: ProviderOptions::new(),
        }
    }

    #[tokio::test]
    async fn ordering_violation_drops_only_the_offending_event() -> anyhow::Result<()> {
        // Per-event drop semantics (design.md 3.4): a Part with no preceding
        // Message is dropped on the spot, with one Error outcome surfaced. The
        // rest of the substream continues normally - subsequent valid messages
        // and parts get written.
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("ordering");
        let orphan_part = Part {
            id: "orphan-part".to_owned(),
            message_id: "missing-message".to_owned(),
            ordinal: 0,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: Some(Extracted::from_test_value("orphan".to_owned())),
            },
        };
        let valid_message = Message::User {
            id: "valid-message".to_owned(),
            session_id: session.id.clone(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };
        let valid_part = Part {
            id: "valid-part".to_owned(),
            message_id: valid_message.id().to_owned(),
            ordinal: 0,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: Some(Extracted::from_test_value("kept".to_owned())),
            },
        };

        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session.clone()))
            .await?;
        let part_outcomes = validator
            .push(&store, 1, IngestEvent::Part(orphan_part))
            .await?;
        assert_eq!(part_outcomes.len(), 1);
        assert_eq!(part_outcomes[0].kind, "part");
        assert_eq!(part_outcomes[0].status, OutcomeStatus::Error);
        assert!(
            part_outcomes[0]
                .error
                .as_ref()
                .map(|e| e.message.contains("part event appeared before a message"))
                .unwrap_or(false),
            "error message must explain the ordering violation: {part_outcomes:?}"
        );
        validator
            .push(&store, 2, IngestEvent::Message(valid_message))
            .await?;
        validator
            .push(&store, 3, IngestEvent::Part(valid_part))
            .await?;
        validator.finish(&store).await?;

        let (sessions, messages, parts, _) = store.row_counts().await?;
        assert_eq!(sessions, 1, "session committed despite the orphan part");
        assert_eq!(messages, 1, "valid message committed");
        assert_eq!(parts, 1, "valid part committed; the orphan was dropped");

        Ok(())
    }

    #[tokio::test]
    async fn duplicate_message_id_drops_the_second_keeps_the_first() -> anyhow::Result<()> {
        // Per-event drop: a duplicate message id within a substream drops the
        // *duplicate* and surfaces an Error outcome for it. The first wins; the
        // session still commits.
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("duplicate-message");
        let first = Message::User {
            id: "message-1".to_owned(),
            session_id: session.id.clone(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };
        let second = Message::Assistant {
            id: "message-1".to_owned(),
            session_id: session.id.clone(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };

        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session.clone()))
            .await?;
        validator
            .push(&store, 1, IngestEvent::Message(first))
            .await?;
        let dup_outcomes = validator
            .push(&store, 2, IngestEvent::Message(second))
            .await?;
        assert_eq!(dup_outcomes.len(), 1);
        assert_eq!(dup_outcomes[0].status, OutcomeStatus::Error);
        assert!(
            dup_outcomes[0]
                .error
                .as_ref()
                .map(|e| e.message.contains("duplicate message id message-1"))
                .unwrap_or(false),
            "duplicate-id rejection must name the offending id: {dup_outcomes:?}"
        );

        validator.finish(&store).await?;
        let (sessions, messages, _, _) = store.row_counts().await?;
        assert_eq!(sessions, 1, "session committed");
        assert_eq!(messages, 1, "only the first message committed");

        Ok(())
    }

    #[tokio::test]
    async fn file_part_blob_v2_round_trips_through_get() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("blob");
        let message = Message::User {
            id: "message-1".to_owned(),
            session_id: session.id.clone(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };
        let part = Part {
            id: "part-1".to_owned(),
            message_id: message.id().to_owned(),
            ordinal: 0,
            options: ProviderOptions::new(),
            kind: PartKind::File {
                media_type: "text/plain".to_owned(),
                file_name: Some("payload.txt".to_owned()),
                data: FileData::Bytes(b"pond".to_vec()),
            },
        };

        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session.clone()))
            .await?;
        validator
            .push(&store, 1, IngestEvent::Message(message.clone()))
            .await?;
        validator
            .push(&store, 2, IngestEvent::Part(part.clone()))
            .await?;
        validator.finish(&store).await?;

        let stored = store
            .get_session(&session.id)
            .await?
            .expect("session should exist");
        let stored_part = &stored.messages[0].parts[0];
        assert_eq!(stored_part, &part);

        Ok(())
    }

    // -- ingest_immutable: Session-level immutable field checks ---------------
    //
    // `Session.source_agent` and `Session.project` are immutable
    // post-first-write because messages/embeddings denormalize them at
    // first ingest; a silent overwrite would desync the denormalized
    // copies. pond core's `IngestValidator` probes the existing session
    // before the merge_insert and emits a per-row `validation_failed`
    // outcome with the typed field name when either changes. Other Session
    // fields (options, parent_session_id, created_at, parent_message_id)
    // re-write idempotently via merge_insert.

    fn base_session() -> Session {
        Session {
            id: "01HXY00000000001".to_owned(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: "claude-code".to_owned(),
            created_at: Utc::now(),
            project: crate::adapter::Extracted::from_test_value("/home/me/proj".to_owned()),
            options: ProviderOptions::new(),
        }
    }

    fn count_status(outcomes: &[RowOutcome], target: OutcomeStatus) -> usize {
        outcomes
            .iter()
            .filter(|outcome| outcome.status == target)
            .count()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn re_ingesting_a_session_with_unchanged_immutable_fields_is_idempotent()
    -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;

        let first = ingest_events(&store, vec![IngestEvent::Session(base_session())]).await?;
        assert_eq!(count_status(&first, OutcomeStatus::Inserted), 1);

        let mut again = base_session();
        again.options.insert("title".to_owned(), json!("renamed"));
        let second = ingest_events(&store, vec![IngestEvent::Session(again)]).await?;
        assert_eq!(
            count_status(&second, OutcomeStatus::Error),
            0,
            "options is mutable; the re-ingest must not surface an error: {second:?}",
        );
        assert_eq!(
            count_status(&second, OutcomeStatus::Matched),
            1,
            "unchanged immutable fields must match-insert via merge_insert",
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn re_ingesting_with_changed_source_agent_is_rejected() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;

        let first = ingest_events(&store, vec![IngestEvent::Session(base_session())]).await?;
        assert_eq!(count_status(&first, OutcomeStatus::Error), 0);

        let mut tampered = base_session();
        tampered.source_agent = "codex-cli".to_owned();
        let second = ingest_events(&store, vec![IngestEvent::Session(tampered)]).await?;
        assert_eq!(count_status(&second, OutcomeStatus::Error), 1);
        let err_row = second
            .iter()
            .find(|outcome| outcome.status == OutcomeStatus::Error)
            .expect("error outcome present");
        let err = err_row.error.as_ref().expect("error body present");
        assert_eq!(err.field, Some("source_agent"));
        assert_eq!(err.reason, Some("immutable"));

        // The stored row stayed on the original adapter - no silent rewrite.
        let stored = store
            .get_session(&base_session().id)
            .await?
            .expect("session row survives the rejected re-ingest");
        assert_eq!(stored.session.source_agent, "claude-code");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn re_ingesting_with_changed_project_is_rejected() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;

        let first = ingest_events(&store, vec![IngestEvent::Session(base_session())]).await?;
        assert_eq!(count_status(&first, OutcomeStatus::Error), 0);

        let mut tampered = base_session();
        tampered.project = crate::adapter::Extracted::from_test_value("/somewhere/else".to_owned());
        let second = ingest_events(&store, vec![IngestEvent::Session(tampered)]).await?;
        let err_row = second
            .iter()
            .find(|outcome| outcome.status == OutcomeStatus::Error)
            .expect("project change must surface an error outcome");
        assert_eq!(err_row.error.as_ref().unwrap().field, Some("project"));

        let stored = store
            .get_session(&base_session().id)
            .await?
            .expect("session row survives");
        assert_eq!(
            stored.session.project.as_str(),
            "/home/me/proj",
            "stored project must remain the original",
        );

        Ok(())
    }

    // -- vector search and index activation --------------------------------

    /// Build `count` synthetic embedding rows with deterministic pseudo-random
    /// vectors of the production dimension, spread across a handful of sessions.
    fn synthetic_rows(count: usize, model_id: &str) -> Vec<EmbeddingRow> {
        let now = Utc::now();
        (0..count)
            .map(|i| {
                let mut vector = Vec::with_capacity(EMBEDDING_DIM);
                let mut state = (i as u64)
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(1);
                for _ in 0..EMBEDDING_DIM {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    #[allow(clippy::cast_precision_loss)]
                    let unit = (state >> 33) as f32 / (1u64 << 31) as f32;
                    vector.push(unit - 1.0);
                }
                EmbeddingRow {
                    message_id: format!("msg-{i}"),
                    model_id: model_id.to_owned(),
                    max_embed_tokens: 1024,
                    vector,
                    session_id: format!("session-{}", i % 8),
                    source_agent: "claude-code".to_owned(),
                    project: format!("/proj/{}", i % 4),
                    role: if i % 2 == 0 { "user" } else { "assistant" }.to_owned(),
                    timestamp: now - Duration::seconds(i as i64),
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn filtered_vector_scan_pushes_scalar_predicate_into_the_index() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let model = crate::config::Config::builtin()
            .embeddings
            .default_model("local")?;

        // 4 synthetic rows: `synthetic_rows` cycles `session-{i % 8}`, so 4 is the
        // smallest count where `session-3` (the filter value below) is a real
        // partition. Scalar-index pushdown is volume-independent - the planner emits
        // a `ScalarIndexQuery` for an indexed equality whenever the index exists, so
        // a larger corpus produces the identical plan.
        store
            .upsert_embeddings(&synthetic_rows(4, &model.id))
            .await?;
        store.ensure_embedding_indices(&model).await?;

        let query = vec![0.01_f32; EMBEDDING_DIM];
        let plan = store
            .explain_vector_plan(
                &query,
                10,
                &crate::substrate::Predicate::Eq("session_id", "session-3".into()),
                &model.id,
                model.max_embed_tokens as i32,
            )
            .await?;

        // The load-bearing assertion (design.md 3.3): the predicate is served by a
        // scalar-index node, not a postfilter `FilterExec`. (A `FilterExec` for the
        // KNN-internal `_distance IS NOT NULL` is expected and unrelated.)
        assert!(
            plan.contains("ScalarIndexQuery"),
            "expected a ScalarIndexQuery node in the plan:\n{plan}",
        );
        let predicate_postfiltered = plan
            .lines()
            .any(|line| line.contains("FilterExec") && line.contains("session_id"));
        assert!(
            !predicate_postfiltered,
            "the scalar predicate must not fall back to a FilterExec postfilter:\n{plan}",
        );
        Ok(())
    }

    #[tokio::test]
    async fn vector_index_activates_past_the_row_threshold() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let model = crate::config::Config::builtin()
            .embeddings
            .default_model("local")?;

        // 256 rows is the hard floor: the IVF_PQ index uses `num_bits = 8`, so its
        // PQ trainer needs one row per code centroid (2^8 = 256) - fewer fails with
        // "Not enough rows to train PQ". The thresholds below straddle that count by
        // exactly one, so the test exercises the `row_count >= threshold` boundary.
        let rows = synthetic_rows(256, &model.id);
        let planted = rows[0].clone();
        store.upsert_embeddings(&rows).await?;

        // Just below threshold (256 < 257): no vector index yet.
        store
            .ensure_embedding_indices_with_threshold(&model, 257)
            .await?;
        assert!(
            !store
                .embedding_index_names()
                .await?
                .iter()
                .any(|name| name == "embeddings_vector_ivfpq"),
            "vector index must not build below the activation threshold",
        );

        // At the threshold (256 >= 256): the IVF_PQ index builds.
        store
            .ensure_embedding_indices_with_threshold(&model, 256)
            .await?;
        let indices = store.embedding_index_names().await?;
        assert!(
            indices.iter().any(|name| name == "embeddings_vector_ivfpq"),
            "IVF_PQ index should build past the activation threshold: {indices:?}",
        );

        // A query whose vector is a planted row returns that row.
        let hits = store
            .vector_search(
                &planted.vector,
                10,
                &crate::substrate::Predicate::And(Vec::new()),
                &model.id,
                model.max_embed_tokens as i32,
            )
            .await?;
        assert!(
            hits.iter().any(|(id, _)| id == &planted.message_id),
            "planted vector should be retrievable via the index",
        );
        Ok(())
    }

    #[tokio::test]
    async fn vector_search_is_scoped_to_one_embedding_identity() -> anyhow::Result<()> {
        // Regression guard: `max_embed_tokens` is part of the embeddings PK, so one
        // message can have several rows (one per cap). Vector search must scan only
        // the identity that produced the query vector - never mix caps, never return
        // a message_id twice (RRF would double-count it).
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let model = crate::config::Config::builtin()
            .embeddings
            .default_model("local")?;

        // Same message, two caps, deliberately opposite vectors so the result
        // distance tells us which row the scan actually ranked.
        let near = vec![0.1_f32; EMBEDDING_DIM];
        let far = vec![-0.1_f32; EMBEDDING_DIM];
        let base = synthetic_rows(1, &model.id)
            .pop()
            .expect("one synthetic row");
        let row_1024 = EmbeddingRow {
            max_embed_tokens: 1024,
            vector: near.clone(),
            ..base.clone()
        };
        let row_4096 = EmbeddingRow {
            max_embed_tokens: 4096,
            vector: far,
            ..base.clone()
        };
        // Distinct PKs (same message_id, different cap), so both rows persist.
        store.upsert_embeddings(&[row_1024, row_4096]).await?;

        // Scoped to cap 1024: the message appears exactly once - not once per cap -
        // ranked against the cap-1024 (near) vector, so distance ~0.
        let hits_1024 = store
            .vector_search(
                &near,
                10,
                &crate::substrate::Predicate::And(Vec::new()),
                &model.id,
                1024,
            )
            .await?;
        assert_eq!(
            hits_1024.len(),
            1,
            "the message must appear exactly once, not once per cap: {hits_1024:?}",
        );
        assert_eq!(hits_1024[0].0, base.message_id);
        assert!(
            hits_1024[0].1 < 0.01,
            "cap-1024 scan must rank the cap-1024 vector, got distance {}",
            hits_1024[0].1,
        );

        // Scoped to cap 4096: same single message, but ranked against the opposite
        // (far) vector - so the same query is now distant. Proves the scan switched
        // rows on the cap, rather than just deduplicating.
        let hits_4096 = store
            .vector_search(
                &near,
                10,
                &crate::substrate::Predicate::And(Vec::new()),
                &model.id,
                4096,
            )
            .await?;
        assert_eq!(hits_4096.len(), 1);
        assert_eq!(hits_4096[0].0, base.message_id);
        assert!(
            hits_4096[0].1 > 1.0,
            "cap-4096 scan must rank the cap-4096 vector, got distance {}",
            hits_4096[0].1,
        );

        Ok(())
    }
}
