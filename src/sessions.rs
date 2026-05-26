use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use async_stream::try_stream;
use chrono::{DateTime, TimeZone, Utc};
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::{AutoCleanupParams, WriteParams};
use lance::deps::arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray, TimestampMicrosecondArray, UInt64Array, new_null_array,
};
use lance::deps::arrow_schema::{DataType, Field, Schema, TimeUnit};
use lance_file::version::LanceFileVersion;
use lance_index::scalar::{BuiltinIndexType, FullTextSearchQuery};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_stream::{Stream, StreamExt};

use crate::{
    config, embed,
    substrate::{
        Handle, IndexIntent, IndexParamsKind, IndexPolicy, IndexTrigger, Predicate, ScalarValue,
        ScanOpts, Table, TableSizes, VECTOR_INDEX_ACTIVATION_ROWS, WriteShape,
    },
    wire::{FileData, Message, Part, PartKind, Role, Session},
};
use url::Url;

#[derive(Debug)]
pub struct Store {
    handle: Handle,
}

/// A message awaiting embedding: its primary key plus the `search_text` to
/// embed. The vector lives on the same `messages` row, so no denormalized
/// filter columns are needed (spec.md#embeddings-are-derived).
#[derive(Debug, Clone, PartialEq)]
pub struct PendingMessage {
    pub session_id: String,
    pub id: String,
    pub search_text: String,
}

/// One embedded message: a primary key and the vector to store. `pond embed`
/// writes a batch of these into `messages.vector` keyed on `(session_id, id)`.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddedMessage {
    pub session_id: String,
    pub id: String,
    pub vector: Vec<f32>,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageKey {
    pub session_id: String,
    pub message_id: String,
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
}

/// Embedding coverage for `pond status` / `pond embed`. `total` is the count of
/// `messages` rows that carry `search_text` (i.e. are eligible to embed); rows
/// without `search_text` produce no vector. `embedded` is the subset of those
/// already carrying a vector under the current [`embed::model_id()`]. The pending
/// backlog is `total - embedded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingProgress {
    pub embedded: usize,
    pub total: usize,
    pub model: &'static str,
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
    /// to [`Store::open`]. The substrate enforces pond's full
    /// [`IndexPolicy`] at open (spec.md#fold-on-write); read-only paths that
    /// don't touch indices (`pond export`) should use [`Store::open_minimal`]
    /// to skip the open-time index work.
    pub async fn open_with_options(
        location: &Url,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        Self::open_with_policy(location, storage_options, pond_index_policy()).await
    }

    /// Open without pond's index policy: no indices are created, no trail
    /// is folded. Used by `pond export`, which streams raw data through the
    /// canonical model and never reads an index.
    pub async fn open_minimal(
        location: &Url,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        Self::open_with_policy(location, storage_options, IndexPolicy::default()).await
    }

    /// Open with an explicit [`IndexPolicy`]. Tests pass a custom policy via
    /// [`pond_index_policy_with_vector_threshold`] to drive IVF_PQ activation
    /// at a much lower row count than the production
    /// [`VECTOR_INDEX_ACTIVATION_ROWS`].
    pub async fn open_with_policy(
        location: &Url,
        storage_options: std::collections::HashMap<String, String>,
        policy: IndexPolicy,
    ) -> Result<Self> {
        Ok(Self {
            handle: Handle::open_with_options(location, storage_options, policy).await?,
        })
    }

    /// Convenience for tests and CLI verbs holding a `&Path`: wraps the path in
    /// a `file://...` URL via [`config::url_for_path`] before opening. Routes
    /// through [`Store::open_with_options`] so the production policy is
    /// applied.
    pub async fn open_local(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let url = config::url_for_path(path)?;
        Self::open_with_options(&url, std::collections::HashMap::new()).await
    }

    /// Test-only convenience matching [`Store::open_local`] but with a custom
    /// IVF_PQ activation threshold so the unit tests can exercise the index
    /// activation boundary without writing 100k vectors.
    #[cfg(test)]
    pub(crate) async fn open_local_with_vector_threshold(
        path: impl AsRef<std::path::Path>,
        threshold: usize,
    ) -> Result<Self> {
        let url = config::url_for_path(path)?;
        Self::open_with_policy(
            &url,
            std::collections::HashMap::new(),
            pond_index_policy_with_vector_threshold(threshold),
        )
        .await
    }

    pub async fn upsert_sessions(&self, sessions: &[Session]) -> Result<Vec<UpsertStatus>> {
        if sessions.is_empty() {
            return Ok(Vec::new());
        }
        let batches = sessions_batches(sessions)?;
        let inserted = merge_insert_chunks(&self.handle, Table::Sessions, batches).await?;
        // Direct-API path. spec.md#fold-on-write: callers that drive many
        // upsert_* calls in a loop should defer the fold via
        // `flush_indices` for O(N) cost; a single isolated call gets a
        // single fold here.
        self.handle
            .fold_and_create_indices(Table::Sessions, WriteShape::Append)
            .await?;
        Ok(statuses_from_inserted(sessions.len(), inserted))
    }

    /// Batched write path used by the adapter ingest loop and by the wire
    /// handler's final flush. Receives N completed substreams from the
    /// validator and:
    ///
    ///   1. Runs the immutable-fields check (spec.md#protocol) against the stored row
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

        let session_batches = sessions_batches(&sessions_owned)?;
        let message_batches = messages_batches(&message_rows)?;
        let part_batches = parts_batches(&part_rows)?;

        let sessions_count = sessions_owned.len();

        let (sessions_inserted, messages_inserted, parts_inserted) = tokio::try_join!(
            merge_insert_chunks(&self.handle, Table::Sessions, session_batches),
            merge_insert_chunks(&self.handle, Table::Messages, message_batches),
            merge_insert_chunks(&self.handle, Table::Parts, part_batches),
        )?;
        // spec.md#fold-on-write: this is the per-batch substream commit
        // path - the outer handler (`ingest_adapter` / `ingest_events`)
        // calls `Store::flush_indices` once at end to fold the full ingest.
        // Folding per-batch would push ingest cost to O(N^2) on a sync
        // that flushes every 100 substreams.

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
        let batches = messages_batches(&rows)?;
        let inserted = merge_insert_chunks(&self.handle, Table::Messages, batches).await?;
        self.handle
            .fold_and_create_indices(Table::Messages, WriteShape::Append)
            .await?;
        Ok(statuses_from_inserted(messages.len(), inserted))
    }

    pub async fn upsert_parts(&self, parts: &[Part]) -> Result<Vec<UpsertStatus>> {
        if parts.is_empty() {
            return Ok(Vec::new());
        }
        let batches = parts_batches(parts)?;
        let inserted = merge_insert_chunks(&self.handle, Table::Parts, batches).await?;
        self.handle
            .fold_and_create_indices(Table::Parts, WriteShape::Append)
            .await?;
        Ok(statuses_from_inserted(parts.len(), inserted))
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

    pub async fn child_sessions(&self, parent_session_id: &str) -> Result<Vec<Session>> {
        let batch = self
            .handle
            .scan_batch(
                Table::Sessions,
                Some(&Predicate::Eq(
                    "parent_session_id",
                    parent_session_id.into(),
                )),
                &[
                    "id",
                    "parent_session_id",
                    "parent_message_id",
                    "source_agent",
                    "created_at",
                    "project",
                    "options",
                ],
            )
            .await?;
        let mut sessions = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            sessions.push(session_from_batch(&batch, row)?);
        }
        sessions.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(sessions)
    }

    /// `session_id -> wall-clock time of the Lance manifest version that
    /// last wrote the row` for the per-session staleness skip
    /// (spec.md#event-ordering). Reads Lance's `_row_last_updated_at_version` system
    /// column (available because pond enables stable row ids per spec.md#stable-row-ids)
    /// and joins it against `Dataset::versions()` for commit timestamps.
    pub async fn session_last_ingested_at(&self) -> Result<HashMap<String, DateTime<Utc>>> {
        use lance::deps::arrow_array::UInt64Array;

        let dataset = self.handle.dataset(Table::Sessions).await?;
        let version_list = dataset.versions().await?;
        let versions: HashMap<u64, DateTime<Utc>> = version_list
            .iter()
            .map(|v| (v.version, v.timestamp))
            .collect();
        // `Dataset::cleanup_old_versions` (and the auto_cleanup hook) drops
        // pruned versions from the manifest list, leaving rows whose
        // `_row_last_updated_at_version` points at a version that no longer
        // resolves. Those rows are still real and were ingested at some time
        // <= the oldest still-visible version's commit timestamp - so falling
        // back to that bound preserves a sound `mtime <= ingested` upper edge
        // and keeps the staleness skip working after cleanup.
        let oldest_visible_ts = version_list.iter().map(|v| v.timestamp).min();

        let scanner = self
            .handle
            .scan(
                Table::Sessions,
                ScanOpts::project_only(&["id", "_row_last_updated_at_version"]),
            )
            .await?;
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
                let ts = versions.get(&version).copied().or(oldest_visible_ts);
                if let Some(ts) = ts {
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

    pub async fn row_counts(&self) -> Result<(usize, usize, usize)> {
        self.handle.row_counts().await
    }

    /// Compute the per-adapter / per-project rollup that drives
    /// `pond status`. One scan over `messages` projecting the three
    /// columns the rollup keys on (`source_agent`, `project`, `session_id`),
    /// aggregated in-memory. Bounded by the cross product of adapters and
    /// projects, which stays small on real corpora.
    pub async fn corpus_stats(&self, include_subagents: bool) -> Result<CorpusStats> {
        let scanner = self
            .handle
            .scan(
                Table::Messages,
                ScanOpts::project_only(&["source_agent", "project", "session_id"]),
            )
            .await?;
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

        let (totals_sessions, totals_messages, totals_parts) = self.handle.row_counts().await?;
        let totals = RowTotals {
            sessions: totals_sessions as u64,
            messages: totals_messages as u64,
            parts: totals_parts as u64,
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

    /// Write a batch of embeddings into `messages`: set `vector` and
    /// `embedding_model` on each row by `(session_id, id)`
    /// (spec.md#embeddings-are-derived). The column update goes through the
    /// write seam and lands as a new manifest version (`append-only`).
    pub async fn write_embeddings(&self, rows: &[EmbeddedMessage]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = embedding_update_batch(rows)?;
        self.handle
            .merge_update(Table::Messages, batch, rows.len())
            .await?;
        // spec.md#fold-on-write: the embed worker drains in many windows;
        // folding per call would slow embed by ~O(windows) full-table
        // index rebuilds (and per-window optimize_indices(append) on a
        // stable-row-id-flat-BTREE-after-column-update repeatedly trips
        // lance v7.0.0-beta.16's combine_old_new bug). The outer
        // `pond embed` handler calls `Store::flush_indices` once at end.
        Ok(())
    }

    /// Stream the backlog of messages needing embedding: rows with `search_text`
    /// set whose `vector` is null (spec.md#embeddings-are-derived). Model swaps
    /// are handled explicitly by `pond embed --force`, which clears stale rows
    /// (setting `vector = NULL` on every row not under the current model) before
    /// the worker pulls them - so by the time this stream runs, the only rows
    /// with non-null `vector` are under the active model.
    pub fn pending_embedding_messages(&self) -> impl Stream<Item = Result<PendingMessage>> + '_ {
        try_stream! {
            let filter = Predicate::And(vec![
                Predicate::IsNull("vector"),
                Predicate::IsNotNull("search_text"),
            ]);
            let projection: &[&str] = &["session_id", "id", "search_text"];
            let scanner = self
                .handle
                .scan(
                    Table::Messages,
                    ScanOpts::with_predicate_and_projection(&filter, projection),
                )
                .await?;
            let mut batches = scanner
                .try_into_stream()
                .await
                .context("failed to open messages stream")?;
            while let Some(batch) = batches.next().await {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    yield PendingMessage {
                        session_id: string(&batch, "session_id", row)?
                            .context("session_id is null")?,
                        id: string(&batch, "id", row)?.context("message id is null")?,
                        search_text: string(&batch, "search_text", row)?
                            .context("search_text is null")?,
                    };
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
    ) -> Result<Vec<(MessageKey, f32)>> {
        let mut scanner = self.handle.scanner(Table::Messages, Some(filter)).await?;
        scanner.full_text_search(
            FullTextSearchQuery::new(query.to_owned()).with_column("search_text".to_owned())?,
        )?;
        // Lance ships an autoprojection that silently appends `_score` to FTS
        // output when the projection omits it. That behavior is going away;
        // we opt into the future explicit-projection contract here so the
        // scanner stops emitting a per-call deprecation warning, and we list
        // `_score` ourselves since the loop below reads it.
        scanner.disable_scoring_autoprojection();
        scanner.project(&["session_id", "id", "_score"])?;
        scanner.limit(Some(i64::try_from(limit).unwrap_or(i64::MAX)), None)?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = MessageKey {
                session_id: string(&batch, "session_id", row)?.context("session_id is null")?,
                message_id: string(&batch, "id", row)?.context("fts hit id is null")?,
            };
            hits.push((key, float32(&batch, "_score", row)?));
        }
        // Stable secondary sort: Lance returns tied-BM25-score hits in fragment
        // order, which varies between runs and across calls with different pool
        // sizes (the hybrid arm's `pool=100` and FTS-only's `limit=20` produce
        // different orderings at the same tied score). Without an explicit
        // tiebreak the downstream RRF dedup-rank for a tied target session can
        // flip session-to-session, making fusion outcomes nondeterministic.
        // Sort by `score desc`, then `(session_id, message_id)` asc.
        hits.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.0.session_id.cmp(&right.0.session_id))
                .then_with(|| left.0.message_id.cmp(&right.0.message_id))
        });
        Ok(hits)
    }

    /// Whether any `messages` row carries a vector (spec.md#search) - the
    /// signal that flips search from FTS-only to hybrid. The single-active-
    /// model invariant (see `MESSAGE_SCALAR_INDICES`) means any non-null
    /// vector belongs to the current model.
    pub async fn has_embeddings(&self) -> Result<bool> {
        let scope = Predicate::IsNotNull("vector");
        let mut scanner = self
            .handle
            .scan(
                Table::Messages,
                ScanOpts::with_predicate_and_projection(&scope, &["id"]),
            )
            .await?;
        scanner.limit(Some(1), None)?;
        let batch = scanner.try_into_batch().await?;
        Ok(batch.num_rows() > 0)
    }

    /// Vector kNN retriever over `messages.vector`, prefiltered by the caller's
    /// scalar predicate (spec.md#prefilter-pushdown). Combines the caller's
    /// filter with `vector IS NOT NULL` to exclude un-embedded rows from the
    /// scan; the brute-force kNN path requires this (the IVF_PQ path would
    /// skip them anyway). The single-active-model invariant lets pond drop
    /// the per-row model filter: every non-null vector belongs to the current
    /// model.
    pub async fn vector_search(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
    ) -> Result<Vec<(MessageKey, f32)>> {
        let scope = embedded_scope(filter);
        let mut scanner = self.handle.scanner(Table::Messages, Some(&scope)).await?;
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        // Mirror the explicit-projection contract from `fts_search`: opt out
        // of `_distance` autoprojection and list it ourselves since the loop
        // below reads it.
        scanner.disable_scoring_autoprojection();
        scanner.project(&["session_id", "id", "_distance"])?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = MessageKey {
                session_id: string(&batch, "session_id", row)?.context("session_id is null")?,
                message_id: string(&batch, "id", row)?.context("message id is null")?,
            };
            hits.push((key, float32(&batch, "_distance", row)?));
        }
        // Stable secondary sort: same reasoning as `fts_search` - IVF_PQ can
        // emit hits with effectively identical `_distance` in fragment-dependent
        // order, which makes RRF dedup-ranks nondeterministic for tied
        // neighbors. Sort by distance asc (smaller = more similar), then by
        // `(session_id, message_id)` asc.
        hits.sort_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.0.session_id.cmp(&right.0.session_id))
                .then_with(|| left.0.message_id.cmp(&right.0.message_id))
        });
        Ok(hits)
    }

    /// The DataFusion plan string for a filtered vector scan - the
    /// `prefilter-pushdown` regression guard reads it.
    pub async fn explain_vector_plan(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
    ) -> Result<String> {
        let scope = embedded_scope(filter);
        let mut scanner = self.handle.scanner(Table::Messages, Some(&scope)).await?;
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        scanner
            .explain_plan(true)
            .await
            .context("explain_plan failed")
    }

    /// Hydrate search hits: fetch message metadata for `(session_id, message_id)` keys.
    pub async fn message_metas_by_keys(&self, keys: &[MessageKey]) -> Result<Vec<MessageMeta>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let wanted = keys.iter().cloned().collect::<HashSet<_>>();
        let session_ids = keys
            .iter()
            .map(|key| key.session_id.clone())
            .collect::<Vec<_>>();
        let message_ids = keys
            .iter()
            .map(|key| key.message_id.clone())
            .collect::<Vec<_>>();
        let predicate = Predicate::And(vec![
            in_predicate("session_id", &session_ids),
            in_predicate("id", &message_ids),
        ]);
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&predicate),
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
            let message_id = string(&batch, "id", row)?.context("id is null")?;
            let session_id = string(&batch, "session_id", row)?.context("session_id is null")?;
            if !wanted.contains(&MessageKey {
                session_id: session_id.clone(),
                message_id: message_id.clone(),
            }) {
                continue;
            }
            metas.push(MessageMeta {
                message_id,
                session_id,
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

    /// Rows appended to `messages` since the FTS index was last folded
    /// (spec.md#fold-on-write). A missing index reports the whole table; the
    /// query is manifest-only - no index I/O. With the fold-on-write contract
    /// this is normally zero between commits; a non-zero value at open time
    /// is reconciled before [`Store::open_with_options`] returns.
    pub async fn unindexed_message_backlog(&self) -> Result<usize> {
        self.handle
            .unindexed_row_count(Table::Messages, MESSAGES_FTS_INDEX)
            .await
    }

    /// Rows added or rewritten in `messages` since the IVF_PQ vector index
    /// was last folded (spec.md#fold-on-write). Below
    /// [`VECTOR_INDEX_ACTIVATION_ROWS`] no index exists yet, so the caller
    /// must read [`embedding_progress`](Self::embedding_progress) too and
    /// distinguish "index not built yet" from "index trails data".
    pub async fn unindexed_vector_backlog(&self) -> Result<usize> {
        self.handle
            .unindexed_row_count(Table::Messages, MESSAGES_VECTOR_INDEX)
            .await
    }

    /// Embedding coverage: how many `messages` rows carry a vector and how
    /// many are still eligible. Drives the `pond status` embeddings line and
    /// the `pond embed` progress bar's known total. `embedded` reads the
    /// `vector IS NOT NULL` count directly - the single-active-model invariant
    /// (see `MESSAGE_SCALAR_INDICES`) means there is no need to scope by the
    /// `embedding_model` column.
    pub async fn embedding_progress(&self) -> Result<EmbeddingProgress> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        let embedded = dataset
            .count_rows(Some(Predicate::IsNotNull("vector").to_lance()))
            .await?;
        let total = dataset
            .count_rows(Some(Predicate::IsNotNull("search_text").to_lance()))
            .await?;
        Ok(EmbeddingProgress {
            embedded,
            total,
            model: embed::model_id(),
        })
    }

    /// Count rows whose `embedding_model` is not the currently configured
    /// model AND whose `vector` is still populated - the signal `pond embed`
    /// uses to detect a model swap and require `--force`. With `--force`,
    /// these rows are cleared via [`Self::clear_embeddings`] before the
    /// worker runs.
    pub async fn stale_embedding_count(&self) -> Result<usize> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        dataset
            .count_rows(Some(
                Predicate::And(vec![
                    Predicate::IsNotNull("vector"),
                    Predicate::Ne("embedding_model", embed::model_id().into()),
                ])
                .to_lance(),
            ))
            .await
            .map_err(Into::into)
    }

    /// Stream the `(session_id, id)` keys for stale-model rows. Used by
    /// `pond embed --force` to enumerate the rows that need clearing before
    /// the new model writes its vectors.
    pub fn stale_embedding_keys(&self) -> impl Stream<Item = Result<MessageKey>> + '_ {
        try_stream! {
            let filter = Predicate::And(vec![
                Predicate::IsNotNull("vector"),
                Predicate::Ne("embedding_model", embed::model_id().into()),
            ]);
            let projection: &[&str] = &["session_id", "id"];
            let scanner = self
                .handle
                .scan(
                    Table::Messages,
                    ScanOpts::with_predicate_and_projection(&filter, projection),
                )
                .await?;
            let mut batches = scanner
                .try_into_stream()
                .await
                .context("failed to open stale-embedding stream")?;
            while let Some(batch) = batches.next().await {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    yield MessageKey {
                        session_id: string(&batch, "session_id", row)?
                            .context("session_id is null")?,
                        message_id: string(&batch, "id", row)?.context("message id is null")?,
                    };
                }
            }
        }
    }

    /// Set `vector = NULL, embedding_model = NULL` on every `messages` row in
    /// `keys`. Used by the `pond embed --force` model-swap path: the worker
    /// only picks up rows whose `vector IS NULL`, so clearing the stale ones
    /// puts them back in the backlog. The merge_update folds-on-write, which
    /// prunes the rewritten fragments from the IVF_PQ's coverage; the embed
    /// handler then drops the old IVF_PQ (its centroids belong to the prior
    /// distance space) before the new vectors arrive.
    pub async fn clear_embeddings(&self, keys: &[MessageKey]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let batch = embedding_clear_batch(keys)?;
        self.handle
            .merge_update(Table::Messages, batch, keys.len())
            .await?;
        // Same batching shape as `write_embeddings`: outer handler folds
        // at end via `Store::flush_indices`.
        Ok(())
    }

    /// Fold every maintained index on every table forward, creating any
    /// implied-but-missing ones (spec.md#fold-on-write). Outer handlers
    /// (`ingest_adapter`, `ingest_events`, `pond embed`) call this once
    /// at end so the spec contract holds at the seam without paying
    /// per-batch fold cost during a long ingest or embed pass.
    ///
    /// The [`WriteShape`] picks the right scalar / FTS strategy: pure
    /// inserts get the cheap incremental `optimize_indices(append)`;
    /// column updates get the safe rebuild that dodges the
    /// v7.0.0-beta.16 flat-BTREE bug. Vector folds incrementally either
    /// way.
    pub async fn flush_indices(&self, shape: WriteShape) -> Result<()> {
        for table in [Table::Sessions, Table::Messages, Table::Parts] {
            self.handle.fold_and_create_indices(table, shape).await?;
        }
        Ok(())
    }

    /// Drop the IVF_PQ index on `messages.vector`. Used by `pond embed
    /// --force` before re-bootstrapping under a different model. Silent
    /// when the index does not exist.
    pub async fn drop_vector_index(&self) -> Result<()> {
        match self
            .handle
            .drop_index(Table::Messages, MESSAGES_VECTOR_INDEX)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                let msg = error.to_string();
                // The index simply was not there - fine, nothing to drop.
                if msg.contains("not found") || msg.contains("does not exist") {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// On-disk byte totals per dataset, sized through Lance's object store
    /// (spec.md#storage-via-lance) so `pond status` works on any backend.
    pub async fn table_sizes(&self) -> Result<TableSizes> {
        self.handle.table_sizes().await
    }

    /// Histogram of Unicode script classes in `messages.search_text`, computed
    /// from a sample of up to `max_messages` non-null rows. Returned classes
    /// are sorted descending by character count. Lets `pond status` tell an
    /// agent whether the corpus is monolingual or mixed - the agent then knows
    /// whether bilingual querying is worth attempting (cross-lingual recall is
    /// a caller-layer concern; pond does not translate internally).
    pub async fn text_script_histogram(&self, max_messages: usize) -> Result<Vec<(String, usize)>> {
        use std::collections::HashMap;
        let filter = Predicate::IsNotNull("search_text");
        let projection: &[&str] = &["search_text"];
        let scanner = self
            .handle
            .scan(
                Table::Messages,
                ScanOpts::with_predicate_and_projection(&filter, projection),
            )
            .await?;
        let mut batches = scanner
            .try_into_stream()
            .await
            .context("failed to open messages stream for script histogram")?;
        let mut counts: HashMap<&'static str, usize> = HashMap::new();
        let mut sampled = 0usize;
        'outer: while let Some(batch) = batches.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                if sampled >= max_messages {
                    break 'outer;
                }
                if let Some(text) = string(&batch, "search_text", row)? {
                    for ch in text.chars() {
                        if let Some(class) = classify_script(ch) {
                            *counts.entry(class).or_default() += 1;
                        }
                    }
                    sampled += 1;
                }
            }
        }
        let mut histogram: Vec<(String, usize)> = counts
            .into_iter()
            .map(|(name, count)| (name.to_owned(), count))
            .collect();
        histogram.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        Ok(histogram)
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
        let mut parts_by_message = self.parts_for_messages(session_id, &message_ids).await?;

        Ok(messages
            .into_iter()
            .map(|message| {
                let key = (message.session_id().to_owned(), message.id().to_owned());
                let parts = parts_by_message.remove(&key).unwrap_or_default();
                MessageWithParts { message, parts }
            })
            .collect())
    }

    async fn parts_for_messages(
        &self,
        session_id: &str,
        message_ids: &[String],
    ) -> Result<BTreeMap<(String, String), Vec<Part>>> {
        if message_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let predicate = Predicate::And(vec![
            Predicate::Eq("session_id", session_id.into()),
            in_predicate("message_id", message_ids),
        ]);
        let dataset = std::sync::Arc::new(self.handle.dataset(Table::Parts).await?);
        let mut scanner = self
            .handle
            .scan(
                Table::Parts,
                ScanOpts::with_predicate_and_projection(
                    &predicate,
                    &[
                        "session_id",
                        "message_id",
                        "id",
                        "ordinal",
                        "type",
                        "provenance",
                        "variant_data",
                        "options",
                    ],
                ),
            )
            .await?;
        scanner.with_row_address();
        let batch = scanner.try_into_batch().await.context("scan failed")?;
        let row_addresses = uint64(&batch, "_rowaddr")?;
        let mut file_payloads = BTreeMap::<usize, FileData>::new();
        let mut file_rows = Vec::<(usize, u64, String)>::new();
        for row in 0..batch.num_rows() {
            if string(&batch, "type", row)?.as_deref() == Some("file") {
                let variant_data =
                    string(&batch, "variant_data", row)?.context("variant_data is null")?;
                file_rows.push((row, row_addresses.value(row), variant_data));
            }
        }
        if !file_rows.is_empty() {
            let addresses = file_rows
                .iter()
                .map(|(_, address, _)| *address)
                .collect::<Vec<_>>();
            let blobs = dataset.take_blobs_by_addresses(&addresses, "data").await?;
            for ((row, _, variant_data), blob) in file_rows.into_iter().zip(blobs) {
                let payload = if file_data_kind(&variant_data)? == "url" {
                    FileData::Url(
                        blob.uri()
                            .context("file URL payload has no blob URI")?
                            .to_owned(),
                    )
                } else {
                    file_data_from_blob(&variant_data, &blob.read().await?)?
                };
                file_payloads.insert(row, payload);
            }
        }
        let mut parts_by_message = BTreeMap::<(String, String), Vec<Part>>::new();
        for row in 0..batch.num_rows() {
            let part = part_from_batch(&batch, row, file_payloads.remove(&row))?;
            parts_by_message
                .entry((part.session_id.clone(), part.message_id.clone()))
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
/// The shape is set by spec.md#event-ordering.
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
    /// whole substream." Per spec.md#adapter-dedup, adapters SHOULD dedupe their
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
    /// (spec.md#event-ordering): file `mtime` was at or before the wall-clock time
    /// pond last wrote that session's row, so re-decode was bypassed.
    pub skipped_fresh: usize,
    /// Storage-layer failures whose retries were exhausted (commit
    /// conflicts, transient IO that didn't recover). Hard zero on healthy
    /// runs.
    pub storage_errors: usize,
    /// Oversized values truncated to a bounded sentinel at the seam
    /// (spec.md#bounded-values); the rest of each such record is intact.
    pub truncated_values: usize,
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
/// the per-row outcome docs in `docs/spec.md#event-ordering`.
pub const DROP_REASON_DUPLICATE_MESSAGE_ID: &str = "duplicate_message_id";
pub const DROP_REASON_DUPLICATE_PART_KEY: &str = "duplicate_part_key";
pub const DROP_REASON_MESSAGE_BEFORE_SESSION: &str = "message_before_session";
pub const DROP_REASON_MESSAGE_SESSION_MISMATCH: &str = "message_session_mismatch";
pub const DROP_REASON_PART_BEFORE_MESSAGE: &str = "part_before_message";
pub const DROP_REASON_PART_MESSAGE_MISMATCH: &str = "part_message_mismatch";
pub const DROP_REASON_EMPTY_SOURCE_AGENT: &str = "empty_source_agent";
pub const DROP_REASON_PARENT_MESSAGE_WITHOUT_SESSION: &str = "parent_message_without_session";
pub const DROP_REASON_IMMUTABLE_PROJECT: &str = "immutable_project";
pub const DROP_REASON_IMMUTABLE_SOURCE_AGENT: &str = "immutable_source_agent";
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
}

/// Per-row outcome surfaced by [`IngestValidator`] (spec.md#protocol). One
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
/// from the prior contract is gone (see spec.md#event-ordering).
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

        // spec.md#datasets: `source_agent` is trimmed at ingest and rejected
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
            // Keep same-substream duplicate ids visible in `dropped_events`;
            // adapters are expected to dedupe upstream (see claude-code's
            // per-file `seen_uuids`), so a hit here is worth investigating.
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
            Value::String(part.session_id.clone()),
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
        if part.session_id != current.message.session_id() {
            let msg = format!(
                "part {} references session {}, expected {}",
                part.id,
                part.session_id,
                current.message.session_id()
            );
            return vec![error_outcome(
                index,
                "part",
                pk,
                &msg,
                Some("session_id"),
                DROP_REASON_PART_MESSAGE_MISMATCH,
            )];
        }
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
/// ("1 session-level rejection"). See spec.md#event-ordering.
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
/// `merge_insert.num_inserted_rows`).
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
                Value::String(part.part.session_id.clone()),
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
    /// spec.md#protocol: `Session.source_agent` and `Session.project` are
    /// immutable post-first-write because the denormalized copies on
    /// `messages` were stamped from the prior Session at first ingest.
    /// A re-write that changes either would silently desync.
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
/// immutable fields (spec.md#protocol). The `Option<String>` `project` field
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
    use crate::wire::Provenance;
    let mut chunks: Vec<String> = Vec::new();
    for part in parts {
        // spec.md#search: only conversational parts contribute to the indexed
        // text; harness-injected scaffolding is excluded from search.
        if part.provenance != Provenance::Conversational {
            continue;
        }
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

/// Scalar indexes on `messages` (spec.md#datasets): BTREE for high-cardinality
/// and range columns, BITMAP for low-cardinality columns. There is no index
/// on `embedding_model`: pond's invariant is one active model at a time
/// (a model swap goes through `pond embed --force` which drops the IVF_PQ,
/// clears stale rows, and re-bootstraps), so `embedding_model` is never a
/// query-time predicate - the only embedding-state filter is `vector IS NOT
/// NULL`. The column remains for audit and for `pond embed`'s model-swap
/// detection.
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

/// Scalar indexes on `parts`: `(session_id, message_id)` is the hot-path lookup key for
/// `parts_for_messages` (hydration on every `get` and grouped search).
const PARTS_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] = &[
    (
        "session_id",
        BuiltinIndexType::BTree,
        "parts_session_id_btree",
    ),
    (
        "message_id",
        BuiltinIndexType::BTree,
        "parts_message_id_btree",
    ),
];

/// Scalar index on `sessions`: `id` is filtered by `find_session` on every
/// `get` and every grouped search.
const SESSIONS_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] =
    &[("id", BuiltinIndexType::BTree, "sessions_id_btree")];

fn in_predicate(column: &'static str, values: &[String]) -> Predicate {
    Predicate::In(
        column,
        values.iter().cloned().map(ScalarValue::String).collect(),
    )
}

/// Combine the caller's filter with `vector IS NOT NULL` so the kNN scanner
/// never sees a null-vector row. Under the single-active-model invariant,
/// `vector IS NOT NULL` is equivalent to "row is currently embedded under
/// the configured model" - no per-row `embedding_model` filter needed.
fn embedded_scope(filter: &Predicate) -> Predicate {
    Predicate::And(vec![Predicate::IsNotNull("vector"), filter.clone()])
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

// Bare logical table names: the lance-namespace Directory impl owns the
// `.lance` directory suffix (spec.md#catalog-seam). No consumer reconstructs
// a `.lance` path.
pub(crate) const SESSIONS: &str = "sessions";
pub(crate) const MESSAGES: &str = "messages";
pub(crate) const PARTS: &str = "parts";

/// FTS index name on `messages.search_text`. Stable so the unindexed-backlog
/// query (spec.md#fold-on-write) and index creation name the same index.
pub(crate) const MESSAGES_FTS_INDEX: &str = "messages_search_text_fts";

/// IVF_PQ index name on `messages.vector` (spec.md#search). Stable so the
/// activation check and index creation name the same index.
pub(crate) const MESSAGES_VECTOR_INDEX: &str = "messages_vector_ivfpq";

/// IVF_PQ tuning constants (spec.md#search):
/// - num_bits = 8 (256 centroids per PQ subspace; needs >= 256 vectors)
/// - sub_vectors = embedding_dim / 8 (8-float PQ subspaces)
/// - max_iters = 15 (kmeans cap)
/// - cosine metric (e5 vectors are L2-normalized)
const IVF_PQ_NUM_BITS: u8 = 8;
const IVF_PQ_SUB_VECTOR_STRIDE: usize = 8;
const IVF_PQ_MAX_ITERS: usize = 15;

/// FTS tokenizer constants (spec.md#language-neutral-index): character ngrams
/// in `[3, 5]`. 4-5-grams discriminate, min=3 keeps 3-char tokens
/// (`FTS`, `OCC`) searchable.
const FTS_NGRAM_MIN: u32 = 3;
const FTS_NGRAM_MAX: u32 = 5;

/// Pond's production index policy (spec.md#fold-on-write). Sessions
/// registers this with the substrate at every [`Store::open_with_options`].
/// The substrate then enforces it on every merge and at open: missing
/// intents that trigger-imply are created, existing indices are folded
/// forward, accumulated segments are collapsed.
pub fn pond_index_policy() -> IndexPolicy {
    pond_index_policy_with_vector_threshold(VECTOR_INDEX_ACTIVATION_ROWS)
}

/// Same as [`pond_index_policy`] but with an overridable IVF_PQ activation
/// threshold. Used by tests that need to exercise the activation boundary
/// without writing 100k vectors.
pub(crate) fn pond_index_policy_with_vector_threshold(vector_threshold: usize) -> IndexPolicy {
    let mut messages = Vec::with_capacity(MESSAGE_SCALAR_INDICES.len() + 2);
    messages.push(IndexIntent {
        name: MESSAGES_FTS_INDEX,
        column: "search_text",
        trigger: IndexTrigger::OnAnyRows,
        params: IndexParamsKind::InvertedFtsNgram {
            min: FTS_NGRAM_MIN,
            max: FTS_NGRAM_MAX,
        },
    });
    for (column, kind, name) in MESSAGE_SCALAR_INDICES {
        messages.push(IndexIntent {
            name,
            column,
            trigger: IndexTrigger::OnAnyRows,
            params: IndexParamsKind::Scalar(kind.clone()),
        });
    }
    messages.push(IndexIntent {
        name: MESSAGES_VECTOR_INDEX,
        column: "vector",
        trigger: IndexTrigger::OnNonNullCount {
            column: "vector",
            threshold: vector_threshold,
        },
        params: IndexParamsKind::IvfPqCosine {
            sub_vectors: embedding_dim() / IVF_PQ_SUB_VECTOR_STRIDE,
            num_bits: IVF_PQ_NUM_BITS,
            max_iters: IVF_PQ_MAX_ITERS,
        },
    });
    let parts = PARTS_SCALAR_INDICES
        .iter()
        .map(|(column, kind, name)| IndexIntent {
            name,
            column,
            trigger: IndexTrigger::OnAnyRows,
            params: IndexParamsKind::Scalar(kind.clone()),
        })
        .collect();
    let sessions = SESSIONS_SCALAR_INDICES
        .iter()
        .map(|(column, kind, name)| IndexIntent {
            name,
            column,
            trigger: IndexTrigger::OnAnyRows,
            params: IndexParamsKind::Scalar(kind.clone()),
        })
        .collect();
    IndexPolicy {
        sessions,
        messages,
        parts,
    }
}

/// Default width of the `messages.vector` embedding column (spec.md#search):
/// matches [`embed::DEFAULT_MODEL_ID`] (`intfloat/multilingual-e5-small`,
/// 384). Used when `[embeddings].dim` is absent.
pub const DEFAULT_EMBEDDING_DIM: usize = 384;

/// Process-wide vector dimension, seeded once at startup from `[embeddings].dim`
/// via [`init_embedding_dim`]. `OnceLock` (not `const`) so a temporary config
/// file can pick a different-dim model (e.g. e5-small at 384) for an experiment
/// without touching every site. Uninitialized -> [`DEFAULT_EMBEDDING_DIM`],
/// which keeps unit tests config-free.
static EMBEDDING_DIM_RUNTIME: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// The active embedding dimension. Returns whatever [`init_embedding_dim`]
/// installed, or [`DEFAULT_EMBEDDING_DIM`] when nothing has installed one.
pub fn embedding_dim() -> usize {
    EMBEDDING_DIM_RUNTIME
        .get()
        .copied()
        .unwrap_or(DEFAULT_EMBEDDING_DIM)
}

/// Seed [`embedding_dim`] from config. First call wins.
pub fn init_embedding_dim(dim: usize) {
    EMBEDDING_DIM_RUNTIME.get_or_init(|| dim);
}

/// Initial-`CREATE` write params for the namespace-mediated path. The
/// substrate seam stamps in `session`, `mode`, and `store_params`.
/// `auto_cleanup` window defaults to 90 days; hosted recovery scenarios
/// (sources deleted, sessions expired, re-ingest impossible) need a long
/// rollback window and storage cost is negligible for append-only workloads.
pub(crate) fn write_params_for_create() -> WriteParams {
    WriteParams {
        data_storage_version: Some(LanceFileVersion::V2_2),
        enable_v2_manifest_paths: true,
        enable_stable_row_ids: true,
        auto_cleanup: Some(AutoCleanupParams {
            interval: 20,
            older_than: chrono::TimeDelta::days(90),
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
        // The message's derived embedding (spec.md#embeddings-are-derived):
        // both null until `pond embed` fills them, set together thereafter.
        Field::new("vector", embedding_vector_type(), true),
        Field::new("embedding_model", DataType::Utf8, true),
        Field::new("options", DataType::Utf8, false),
    ]))
}

pub(crate) fn part_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("session_id", DataType::Utf8, false),
        primary_field("message_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new("ordinal", DataType::Int32, false),
        Field::new("type", DataType::Utf8, false),
        // spec.md#part-provenance: conversation vs harness-injected; search
        // reads this column to exclude injected scaffolding.
        Field::new("provenance", DataType::Utf8, false),
        Field::new("variant_data", DataType::Utf8, false),
        blob_field("data", true),
        Field::new("options", DataType::Utf8, false),
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

fn embedding_vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, true)),
        embedding_dim() as i32,
    )
}

/// The partial-schema source for the embedding column update: the `messages`
/// primary key plus the two columns `pond embed` fills. The field definitions
/// match `message_schema` exactly so Lance accepts it as a subset upsert.
fn embedding_update_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("session_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new("vector", embedding_vector_type(), true),
        Field::new("embedding_model", DataType::Utf8, true),
    ]))
}

/// Source batch for [`Store::clear_embeddings`]: one row per key carrying
/// `(session_id, id, NULL, NULL)`. `merge_update` then nulls the `vector`
/// and `embedding_model` columns on each matched row, putting it back in
/// the embed worker's backlog (`vector IS NULL`).
fn embedding_clear_batch(keys: &[MessageKey]) -> Result<RecordBatch> {
    let session_ids = StringArray::from(
        keys.iter()
            .map(|key| key.session_id.as_str())
            .collect::<Vec<_>>(),
    );
    let ids = StringArray::from(
        keys.iter()
            .map(|key| key.message_id.as_str())
            .collect::<Vec<_>>(),
    );
    let null_vectors = new_null_array(&embedding_vector_type(), keys.len());
    let null_models = new_null_array(&DataType::Utf8, keys.len());
    RecordBatch::try_new(
        embedding_update_schema(),
        vec![
            Arc::new(session_ids),
            Arc::new(ids),
            null_vectors,
            null_models,
        ],
    )
    .context("failed to build embedding-clear batch")
}

/// Build the merge-update source batch for [`Store::write_embeddings`]: one row
/// per embedded message carrying `(session_id, id, vector, embedding_model)`.
pub(crate) fn embedding_update_batch(rows: &[EmbeddedMessage]) -> Result<RecordBatch> {
    let dim = embedding_dim();
    let mut flat = Vec::with_capacity(rows.len() * dim);
    for row in rows {
        if row.vector.len() != dim {
            anyhow::bail!(
                "embedding for message {} has dim {}, expected {dim}",
                row.id,
                row.vector.len(),
            );
        }
        flat.extend_from_slice(&row.vector);
    }
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim as i32,
        Arc::new(Float32Array::from(flat)),
        None,
    )
    .context("failed to build embedding vector column")?;

    RecordBatch::try_new(
        embedding_update_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.session_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(vectors),
            Arc::new(StringArray::from(vec![embed::model_id(); rows.len()])),
        ],
    )
    .context("failed to build embedding update batch")
}

/// The runtime backstop against Arrow's 2 GiB `i32` offset wall: a flush batch
/// is split before the running total of its text columns reaches this, and a
/// single cell at or above it is rejected rather than left to panic inside
/// `StringArray::from` (spec.md#bounded-values).
const COLUMN_BYTE_BUDGET: usize = 1 << 30;

/// Contiguous row ranges whose summed text-column byte cost each stays within
/// `COLUMN_BYTE_BUDGET`. Budgeting the all-column total bounds every individual
/// column too, since no single column's total can exceed it. `cells[i]` is row
/// `i`'s byte cost summed across every text column.
fn chunk_ranges(cells: &[usize]) -> Vec<std::ops::Range<usize>> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut running = 0usize;
    for (index, &row) in cells.iter().enumerate() {
        if running + row > COLUMN_BYTE_BUDGET && index > start {
            chunks.push(start..index);
            start = index;
            running = 0;
        }
        running += row;
    }
    if start < cells.len() {
        chunks.push(start..cells.len());
    }
    chunks
}

fn guard_cell(table: &str, pk: &str, bytes: usize) -> Result<()> {
    if bytes >= COLUMN_BYTE_BUDGET {
        anyhow::bail!(
            "{table} row {pk}: a {bytes}-byte text cell meets the per-cell ceiling and would \
             overflow Arrow's i32 offset buffer"
        );
    }
    Ok(())
}

async fn merge_insert_chunks(
    handle: &Handle,
    table: Table,
    batches: Vec<RecordBatch>,
) -> Result<u64> {
    let mut inserted = 0u64;
    for batch in batches {
        let rows = batch.num_rows();
        inserted += handle.merge_insert(table, batch, rows).await?;
    }
    Ok(inserted)
}

pub(crate) fn sessions_batches(sessions: &[Session]) -> Result<Vec<RecordBatch>> {
    let options = sessions
        .iter()
        .map(|session| json_string(&session.options))
        .collect::<Result<Vec<_>>>()?;
    let mut cells = Vec::with_capacity(sessions.len());
    for (session, encoded) in sessions.iter().zip(&options) {
        let columns = [
            session.id.len(),
            session.parent_session_id.as_deref().map_or(0, str::len),
            session.parent_message_id.as_deref().map_or(0, str::len),
            session.source_agent.len(),
            session.project.as_str().len(),
            encoded.len(),
        ];
        for bytes in columns {
            guard_cell("sessions", &session.id, bytes)?;
        }
        cells.push(columns.iter().sum());
    }
    chunk_ranges(&cells)
        .into_iter()
        .map(|range| sessions_chunk(&sessions[range.clone()], &options[range]))
        .collect()
}

fn sessions_chunk(sessions: &[Session], options: &[String]) -> Result<RecordBatch> {
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
                options.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .context("failed to build session batch")
}

pub(crate) fn messages_batches(rows: &[MessageBatchRow<'_>]) -> Result<Vec<RecordBatch>> {
    let options = rows
        .iter()
        .map(|row| json_string(row.message.options()))
        .collect::<Result<Vec<_>>>()?;
    let mut cells = Vec::with_capacity(rows.len());
    for (row, encoded) in rows.iter().zip(&options) {
        let columns = [
            row.message.session_id().len(),
            row.message.id().len(),
            row.message.role().as_str().len(),
            row.source_agent.len(),
            row.project.len(),
            row.message.system_content().map_or(0, str::len),
            row.search_text.map_or(0, str::len),
            encoded.len(),
        ];
        for bytes in columns {
            guard_cell("messages", row.message.id(), bytes)?;
        }
        cells.push(columns.iter().sum());
    }
    chunk_ranges(&cells)
        .into_iter()
        .map(|range| messages_chunk(&rows[range.clone()], &options[range]))
        .collect()
}

fn messages_chunk(rows: &[MessageBatchRow<'_>], options: &[String]) -> Result<RecordBatch> {
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
            // `vector` / `embedding_model` are written null at ingest; every
            // message starts un-embedded and `pond embed` fills them later
            // (spec.md#embeddings-are-derived).
            new_null_array(&embedding_vector_type(), rows.len()),
            new_null_array(&DataType::Utf8, rows.len()),
            Arc::new(StringArray::from(
                options.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .context("failed to build message batch")
}

pub(crate) fn parts_batches(parts: &[Part]) -> Result<Vec<RecordBatch>> {
    let variant_data = parts
        .iter()
        .map(|part| part_variant_json(&part.kind))
        .collect::<Result<Vec<_>>>()?;
    let options = parts
        .iter()
        .map(|part| json_string(&part.options))
        .collect::<Result<Vec<_>>>()?;
    let mut cells = Vec::with_capacity(parts.len());
    // The blob column is a BinaryArray, exempt from the text-column bound
    // (spec.md#bounded-values); only the StringArray columns are budgeted.
    for ((part, variant), encoded) in parts.iter().zip(&variant_data).zip(&options) {
        let columns = [
            part.session_id.len(),
            part.message_id.len(),
            part.id.len(),
            part.kind.type_name().len(),
            part.provenance.as_str().len(),
            variant.len(),
            encoded.len(),
        ];
        for bytes in columns {
            guard_cell("parts", &part.id, bytes)?;
        }
        cells.push(columns.iter().sum());
    }
    chunk_ranges(&cells)
        .into_iter()
        .map(|range| {
            parts_chunk(
                &parts[range.clone()],
                &variant_data[range.clone()],
                &options[range],
            )
        })
        .collect()
}

fn parts_chunk(parts: &[Part], variant_data: &[String], options: &[String]) -> Result<RecordBatch> {
    let schema = part_schema();
    let mut blobs = BlobArrayBuilder::new(parts.len());
    for part in parts {
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
                    .map(|part| part.session_id.as_str())
                    .collect::<Vec<_>>(),
            )),
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
                    .map(|part| part.provenance.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                variant_data.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            blobs.finish()?,
            Arc::new(StringArray::from(
                options.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
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

pub(crate) fn part_from_batch(
    batch: &RecordBatch,
    row: usize,
    file_data: Option<FileData>,
) -> Result<Part> {
    let type_name = string(batch, "type", row)?.context("part type is null")?;
    let variant_data = string(batch, "variant_data", row)?.context("variant_data is null")?;
    let provenance = string(batch, "provenance", row)?.context("part provenance is null")?;
    Ok(Part {
        session_id: string(batch, "session_id", row)?.context("part session_id is null")?,
        message_id: string(batch, "message_id", row)?.context("part message_id is null")?,
        id: string(batch, "id", row)?.context("part id is null")?,
        ordinal: int32(batch, "ordinal", row)?,
        provenance: provenance_from_str(&provenance)?,
        options: json_parse(&string(batch, "options", row)?.context("part options is null")?)?,
        kind: part_kind_from_json(&type_name, &variant_data, file_data)?,
    })
}

fn provenance_from_str(value: &str) -> Result<crate::wire::Provenance> {
    match value {
        "conversational" => Ok(crate::wire::Provenance::Conversational),
        "injected" => Ok(crate::wire::Provenance::Injected),
        other => anyhow::bail!("unknown part provenance {other}"),
    }
}

fn file_data_from_blob(variant_data: &str, bytes: &[u8]) -> Result<FileData> {
    let kind = file_data_kind(variant_data)?;
    match kind.as_str() {
        "string" => {
            let text = std::str::from_utf8(bytes)
                .context("file string payload is not UTF-8")?
                .to_owned();
            Ok(FileData::String(text))
        }
        "bytes" => Ok(FileData::Bytes(bytes.to_vec())),
        "url" => Ok(FileData::Url(
            std::str::from_utf8(bytes)
                .context("file URL payload is not UTF-8")?
                .to_owned(),
        )),
        other => anyhow::bail!("unknown file data_kind {other}"),
    }
}

fn file_data_kind(variant_data: &str) -> Result<String> {
    let value = json_parse::<Value>(variant_data)?;
    value
        .get("data_kind")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .context("file part variant_data missing data_kind")
}

fn uint64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .with_context(|| format!("column {name} is not UInt64"))
}

/// Map a character to its Unicode script class name, or `None` for
/// non-alphabetic characters (digits, punctuation, whitespace). Used by
/// `Store::text_script_histogram` to surface corpus language mix in
/// `pond status`. The ranges cover the scripts most likely to appear in
/// agent-session transcripts; everything else collapses to `"Other"` so the
/// histogram stays bounded.
fn classify_script(ch: char) -> Option<&'static str> {
    if !ch.is_alphabetic() {
        return None;
    }
    let code = ch as u32;
    match code {
        0x0041..=0x005A | 0x0061..=0x007A | 0x00C0..=0x024F => Some("Latin"),
        0x0370..=0x03FF => Some("Greek"),
        0x0400..=0x052F => Some("Cyrillic"),
        0x0590..=0x05FF => Some("Hebrew"),
        0x0600..=0x06FF | 0x0750..=0x077F => Some("Arabic"),
        0x0900..=0x097F => Some("Devanagari"),
        0x0E00..=0x0E7F => Some("Thai"),
        0x3040..=0x309F => Some("Hiragana"),
        0x30A0..=0x30FF => Some("Katakana"),
        0x4E00..=0x9FFF | 0x3400..=0x4DBF => Some("Han"),
        0xAC00..=0xD7AF | 0x1100..=0x11FF => Some("Hangul"),
        _ => Some("Other"),
    }
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
    if let PartKind::File {
        media_type,
        file_name,
        data,
    } = kind
    {
        let data_kind = match data {
            FileData::String(_) => "string",
            FileData::Bytes(_) => "bytes",
            FileData::Url(_) => "url",
        };
        return serde_json::to_string(&serde_json::json!({
            "media_type": media_type,
            "file_name": file_name,
            "data_kind": data_kind,
        }))
        .context("failed to serialize file part variant");
    }
    let value = serde_json::to_value(kind)?;
    let mut object = value
        .as_object()
        .cloned()
        .context("part variant did not serialize to an object")?;
    object.remove("type");
    serde_json::to_string(&object).context("failed to serialize part variant")
}

fn part_kind_from_json(
    type_name: &str,
    variant_data: &str,
    file_data: Option<FileData>,
) -> Result<PartKind> {
    let mut value = json_parse::<Value>(variant_data)?;
    let object = value
        .as_object_mut()
        .context("part variant data is not an object")?;
    object.insert("type".to_owned(), Value::String(type_name.to_owned()));
    if let Some(data) = file_data {
        object.remove("data_kind");
        object.insert("data".to_owned(), serde_json::to_value(data)?);
    }
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
    use chrono::Utc;
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

    #[test]
    fn search_text_excludes_injected_parts() {
        use crate::wire::Provenance;
        let message = Message::User {
            id: "m1".to_owned(),
            session_id: "s1".to_owned(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };
        let text_part = |id: &str, text: &str, provenance: Provenance| Part {
            session_id: "s1".to_owned(),
            id: id.to_owned(),
            message_id: "m1".to_owned(),
            ordinal: 0,
            provenance,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: Some(Extracted::from_test_value(text.to_owned())),
            },
        };

        // A conversational part contributes; an injected one is excluded
        // (spec.md#search).
        let conversational = search_text(
            &message,
            &[text_part(
                "p1",
                "real human prompt",
                Provenance::Conversational,
            )],
        );
        assert_eq!(conversational.as_deref(), Some("real human prompt"));

        let injected = search_text(
            &message,
            &[text_part(
                "p2",
                "<task-notification>...</task-notification>",
                Provenance::Injected,
            )],
        );
        assert!(
            injected.is_none(),
            "a message whose only part is injected has null search_text"
        );
    }

    #[test]
    fn chunk_ranges_splits_on_byte_budget() {
        assert!(chunk_ranges(&[]).is_empty());
        assert_eq!(chunk_ranges(&[10, 10, 10]), vec![0..3]);

        let two_thirds = COLUMN_BYTE_BUDGET * 2 / 3;
        assert_eq!(
            chunk_ranges(&[two_thirds, two_thirds, two_thirds]),
            vec![0..1, 1..2, 2..3],
        );

        // An oversized single row gets its own chunk, never an infinite loop.
        assert_eq!(
            chunk_ranges(&[10, COLUMN_BYTE_BUDGET + 1, 10]),
            vec![0..1, 1..2, 2..3],
        );
    }

    #[tokio::test]
    async fn ordering_violation_drops_only_the_offending_event() -> anyhow::Result<()> {
        // Per-event drop semantics (spec.md#event-ordering): a Part with no preceding
        // Message is dropped on the spot, with one Error outcome surfaced. The
        // rest of the substream continues normally - subsequent valid messages
        // and parts get written.
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("ordering");
        let orphan_part = Part {
            session_id: session.id.clone(),
            id: "orphan-part".to_owned(),
            message_id: "missing-message".to_owned(),
            ordinal: 0,
            provenance: crate::wire::Provenance::Conversational,
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
            session_id: session.id.clone(),
            id: "valid-part".to_owned(),
            message_id: valid_message.id().to_owned(),
            ordinal: 0,
            provenance: crate::wire::Provenance::Conversational,
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

        let (sessions, messages, parts) = store.row_counts().await?;
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
        let (sessions, messages, _) = store.row_counts().await?;
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
            session_id: session.id.clone(),
            id: "part-1".to_owned(),
            message_id: message.id().to_owned(),
            ordinal: 0,
            provenance: crate::wire::Provenance::Conversational,
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
    // post-first-write because `messages` denormalizes them at first
    // ingest; a silent overwrite would desync the denormalized
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

    /// Ingest `count` synthetic messages spread across a handful of sessions
    /// and projects, each with conversational `search_text`. Returns the store
    /// and the message keys in `msg-{i}` order; every `vector` starts null.
    async fn store_with_messages(
        temp: &TempDir,
        count: usize,
    ) -> anyhow::Result<(Store, Vec<MessageKey>)> {
        store_with_messages_at_threshold(temp, count, VECTOR_INDEX_ACTIVATION_ROWS).await
    }

    /// Same as [`store_with_messages`] but opens the store with a custom
    /// IVF_PQ activation threshold so tests can exercise the activation
    /// boundary without writing 100k vectors.
    async fn store_with_messages_at_threshold(
        temp: &TempDir,
        count: usize,
        vector_threshold: usize,
    ) -> anyhow::Result<(Store, Vec<MessageKey>)> {
        let store = Store::open_local_with_vector_threshold(temp.path(), vector_threshold).await?;
        let sessions = 8.min(count.max(1));
        let mut events = Vec::new();
        for s in 0..sessions {
            events.push(IngestEvent::Session(Session {
                id: format!("session-{s}"),
                parent_session_id: None,
                parent_message_id: None,
                source_agent: "claude-code".to_owned(),
                created_at: Utc::now(),
                project: Extracted::from_test_value(format!("/proj/{}", s % 4)),
                options: ProviderOptions::new(),
            }));
            for i in (s..count).step_by(sessions) {
                let message_id = format!("msg-{i}");
                events.push(IngestEvent::Message(Message::User {
                    id: message_id.clone(),
                    session_id: format!("session-{s}"),
                    timestamp: Utc::now(),
                    options: ProviderOptions::new(),
                }));
                events.push(IngestEvent::Part(Part {
                    session_id: format!("session-{s}"),
                    id: format!("{message_id}-part"),
                    message_id,
                    ordinal: 0,
                    provenance: crate::wire::Provenance::Conversational,
                    options: ProviderOptions::new(),
                    kind: PartKind::Text {
                        text: Some(Extracted::from_test_value(format!("synthetic message {i}"))),
                    },
                }));
            }
        }
        ingest_events(&store, events).await?;
        let keys = (0..count)
            .map(|i| MessageKey {
                session_id: format!("session-{}", i % sessions),
                message_id: format!("msg-{i}"),
            })
            .collect();
        Ok((store, keys))
    }

    /// A deterministic pseudo-random vector of the production dimension.
    fn synthetic_vector(seed: usize) -> Vec<f32> {
        let mut state = (seed as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(1);
        (0..embedding_dim())
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                #[allow(clippy::cast_precision_loss)]
                let unit = (state >> 33) as f32 / (1u64 << 31) as f32;
                unit - 1.0
            })
            .collect()
    }

    /// One [`EmbeddedMessage`] per key, vectors seeded by slice position.
    fn embedded(keys: &[MessageKey]) -> Vec<EmbeddedMessage> {
        keys.iter()
            .enumerate()
            .map(|(seed, key)| EmbeddedMessage {
                session_id: key.session_id.clone(),
                id: key.message_id.clone(),
                vector: synthetic_vector(seed),
            })
            .collect()
    }

    #[tokio::test]
    async fn filtered_vector_scan_pushes_scalar_predicate_into_the_index() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        // 4 messages cycle session-0..session-3, so `session-3` is a real
        // partition. Scalar-index pushdown is volume-independent - the planner
        // emits a `ScalarIndexQuery` for an indexed equality whenever the index
        // exists, so a larger corpus produces the identical plan. With
        // fold-on-write, the ingest inside `store_with_messages` already
        // created the session_id BTREE; the column update below re-folds it
        // inline. No separate upkeep call.
        let (store, keys) = store_with_messages(&temp, 4).await?;
        store.write_embeddings(&embedded(&keys)).await?;
        // Direct `write_embeddings` usage (outside a handler) - flush
        // explicitly so the BTREE on session_id covers the rewritten
        // fragments before the planner asserts ScalarIndexQuery pushdown.
        store.flush_indices(WriteShape::ColumnUpdate).await?;

        let query = vec![0.01_f32; embedding_dim()];
        let plan = store
            .explain_vector_plan(&query, 10, &Predicate::Eq("session_id", "session-3".into()))
            .await?;

        // The load-bearing assertion (spec.md#prefilter-pushdown): the predicate
        // is served by a scalar-index node, not a postfilter `FilterExec`. (A
        // `FilterExec` for the KNN-internal `_distance IS NOT NULL` is expected
        // and unrelated.)
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
    async fn vector_index_activates_when_threshold_is_crossed_inline() -> anyhow::Result<()> {
        // spec.md#fold-on-write: at the outermost public boundary, the
        // IVF_PQ exists once `count(vector IS NOT NULL) >= threshold`.
        // We emulate the boundary here with explicit `flush_indices` after
        // each write batch (handlers wrap this automatically).
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages_at_threshold(&temp, 300, 256).await?;

        // First batch: 255 vectors, one below threshold. The handler-end
        // fold does not create the IVF_PQ (trigger not met).
        store.write_embeddings(&embedded(&keys[..255])).await?;
        store.flush_indices(WriteShape::ColumnUpdate).await?;
        assert!(
            !store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "IVF_PQ must not exist below the activation threshold",
        );

        // Next batch: one more vector. Total reaches 256; the next
        // flush_indices creates the IVF_PQ.
        store.write_embeddings(&embedded(&keys[255..256])).await?;
        store.flush_indices(WriteShape::ColumnUpdate).await?;
        assert!(
            store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "fold-on-write must create the IVF_PQ once the threshold is crossed",
        );

        // The remaining 44 rows stay un-embedded; the IVF_PQ trains over the
        // non-null subset and a planted vector is retrievable.
        let hits = store
            .vector_search(&synthetic_vector(0), 10, &Predicate::And(Vec::new()))
            .await?;
        assert!(
            hits.iter().any(|(key, _)| key == &keys[0]),
            "an embedded row is retrievable via the index",
        );
        Ok(())
    }

    #[tokio::test]
    async fn session_last_ingested_at_falls_back_when_versions_pruned() -> anyhow::Result<()> {
        // Regression: `_row_last_updated_at_version` can point at a Lance
        // manifest version that `cleanup_old_versions` or the auto_cleanup
        // hook has since dropped from `Dataset::versions()`. The old code
        // silently dropped any session whose row-version was not in the
        // visible list, collapsing the staleness-skip map down to recent
        // commits and forcing `pond sync` to re-touch every file. The fix
        // falls back to the oldest still-visible commit timestamp - a
        // sound upper bound on the row's true ingest time.
        let temp = TempDir::new()?;
        let (store, _keys) = store_with_messages(&temp, 4).await?;

        // Produce several distinct manifest versions on `sessions` so the
        // older ones become eligible for cleanup. Each upsert_sessions
        // commits one merge_insert manifest plus its fold-on-write commit.
        for tag in 0..3 {
            let extra = synthetic_session(&format!("extra-{tag}"));
            store.upsert_sessions(&[extra]).await?;
        }

        // Prune everything older than ~now, leaving only the latest manifest.
        // `delete_unverified=None` and `error_if_tagged=Some(false)` mirror
        // Lance's auto-cleanup hook semantics. The chrono 0-duration is fine:
        // Lance's `delete_unverified` floor still protects in-flight files.
        let dataset = store.handle.dataset(Table::Sessions).await?;
        dataset
            .cleanup_old_versions(chrono::Duration::zero(), None, Some(false))
            .await
            .context("cleanup_old_versions failed")?;

        let map = store.session_last_ingested_at().await?;
        let session_count = store.row_counts().await?.0;
        assert!(
            map.len() >= session_count,
            "watermark map ({}) must still cover every session ({}) after \
             version cleanup; an empty fallback regresses pond sync to a \
             full re-scan",
            map.len(),
            session_count,
        );
        Ok(())
    }

    #[tokio::test]
    async fn fold_on_write_holds_after_ingest_and_embed() -> anyhow::Result<()> {
        // spec.md#fold-on-write at the outermost public boundary: after
        // an ingest + embed pair plus one final `flush_indices` (the
        // pond embed handler does this), the FTS and IVF_PQ backlogs are
        // both zero and the IVF_PQ exists.
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages_at_threshold(&temp, 300, 256).await?;
        store.write_embeddings(&embedded(&keys)).await?;
        store.flush_indices(WriteShape::ColumnUpdate).await?;

        assert_eq!(
            store.unindexed_message_backlog().await?,
            0,
            "FTS index must fully cover messages after fold-on-write",
        );
        assert_eq!(
            store.unindexed_vector_backlog().await?,
            0,
            "IVF_PQ must fully cover vectors after fold-on-write",
        );
        assert!(
            store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "IVF_PQ must exist after embed crosses the activation threshold",
        );
        Ok(())
    }

    #[tokio::test]
    async fn open_recreates_an_implied_missing_index() -> anyhow::Result<()> {
        // spec.md#fold-on-write: index state is a pure function of data
        // state. Drop an index that data implies should exist (bypass the
        // contract from outside), reopen the store, and the open-time
        // fold-and-create pass must recreate it before returning a handle.
        // No recovery verb anywhere - the seam owns the contract.
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("recover");
        let message = Message::User {
            id: "m-1".to_owned(),
            session_id: session.id.clone(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };
        let part = Part {
            session_id: session.id.clone(),
            id: "m-1:0".to_owned(),
            message_id: message.id().to_owned(),
            ordinal: 0,
            provenance: crate::wire::Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: Some(Extracted::from_test_value("recoverable".to_owned())),
            },
        };
        ingest_events(
            &store,
            vec![
                IngestEvent::Session(session.clone()),
                IngestEvent::Message(message),
                IngestEvent::Part(part),
            ],
        )
        .await?;
        assert!(
            store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_FTS_INDEX),
            "FTS index must exist after ingest (sanity)",
        );

        // Drop the FTS index outside the contract; reopen.
        store
            .handle
            .drop_index(Table::Messages, MESSAGES_FTS_INDEX)
            .await?;
        drop(store);
        let reopened = Store::open_local(temp.path()).await?;
        assert!(
            reopened
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_FTS_INDEX),
            "open-time fold-and-create must recreate the missing FTS index",
        );
        assert_eq!(
            reopened.unindexed_message_backlog().await?,
            0,
            "the recreated FTS index must fully cover existing rows",
        );
        Ok(())
    }

    #[tokio::test]
    async fn model_swap_force_path_clears_and_rebuilds_ivf_pq() -> anyhow::Result<()> {
        // The `pond embed --force` model-swap workflow at the substrate
        // level: drop_vector_index removes the IVF_PQ; clear_embeddings
        // nulls `vector` + `embedding_model` on the stale rows; the next
        // write (or open) re-bootstraps the IVF_PQ from scratch under the
        // current model. (`init_model_id` is process-global and seeded
        // once, so the test drives the mechanics directly rather than
        // swapping model ids.)
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages_at_threshold(&temp, 300, 256).await?;
        store.write_embeddings(&embedded(&keys)).await?;
        store.flush_indices(WriteShape::ColumnUpdate).await?;
        assert!(
            store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "IVF_PQ must exist after the first embed pass + flush (sanity)",
        );

        // Drop the IVF_PQ (production-equivalent: `Store::drop_vector_index`),
        // clear half the rows, then reopen. Open-time fold-and-create sees
        // an implied-missing IVF_PQ and re-bootstraps it on the now-current
        // vector population.
        store.drop_vector_index().await?;
        store.clear_embeddings(&keys[..150]).await?;
        // Re-embed the cleared half so the count stays >= threshold.
        store.write_embeddings(&embedded(&keys[..150])).await?;
        drop(store);
        let reopened = Store::open_local_with_vector_threshold(temp.path(), 256).await?;
        assert!(
            reopened
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "open-time fold-and-create must rebuild the IVF_PQ after drop",
        );
        assert_eq!(
            reopened.unindexed_vector_backlog().await?,
            0,
            "the rebuilt IVF_PQ must fully cover the current vector set",
        );
        Ok(())
    }

    #[tokio::test]
    async fn embedding_progress_counts_embedded_and_eligible_rows() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages(&temp, 10).await?;

        let before = store.embedding_progress().await?;
        assert_eq!(before.embedded, 0);
        assert_eq!(before.total, 10);
        assert_eq!(before.model, crate::embed::model_id());

        store.write_embeddings(&embedded(&keys[..4])).await?;
        let partial = store.embedding_progress().await?;
        assert_eq!(partial.embedded, 4);
        assert_eq!(partial.total, 10);

        store.write_embeddings(&embedded(&keys[4..])).await?;
        let full = store.embedding_progress().await?;
        assert_eq!(full.embedded, 10);
        assert_eq!(full.total, 10);
        Ok(())
    }

    // spec.md#fold-on-write invariants under random write sequences. After
    // every public write through `Store`, every maintained index on the
    // touched table must fully cover its data - `unindexed_row_count` is
    // zero. Open-time reconciliation must hold the same invariant. The
    // sequence is small (proptest is slow against a TempDir-backed Store)
    // but the case count is enough to exercise interleavings of
    // upsert + write_embeddings + open/close.
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 8,
            .. proptest::prelude::ProptestConfig::default()
        })]
        #[test]
        fn fold_on_write_invariants_hold_after_random_writes(
            ops in proptest::collection::vec(write_op_strategy(), 1..6),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            runtime.block_on(async move {
                let temp = TempDir::new()?;
                // Threshold = 256 keeps IVF_PQ inactive at proptest scale
                // (we never write 256 vectors in 6 ops). Lower would force
                // the activation path which carries its own dedicated test.
                let store = Store::open_local_with_vector_threshold(temp.path(), 256).await?;
                let mut messages_written = 0usize;
                for op in &ops {
                    apply_proptest_op(&store, *op, &mut messages_written).await?;
                    assert_fold_invariants(&store).await?;
                }
                // Reopen: open-time fold-and-create must hold the invariant.
                drop(store);
                let reopened = Store::open_local_with_vector_threshold(temp.path(), 256).await?;
                assert_fold_invariants(&reopened).await?;
                Ok::<_, anyhow::Error>(())
            }).expect("proptest case");
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum WriteOp {
        IngestSession { messages: u8 },
        ClearAllEmbeddings,
    }

    fn write_op_strategy() -> impl proptest::strategy::Strategy<Value = WriteOp> {
        use proptest::prelude::*;
        prop_oneof![
            (1u8..6).prop_map(|messages| WriteOp::IngestSession { messages }),
            Just(WriteOp::ClearAllEmbeddings),
        ]
    }

    async fn apply_proptest_op(
        store: &Store,
        op: WriteOp,
        messages_written: &mut usize,
    ) -> Result<()> {
        use crate::wire::Provenance;
        match op {
            WriteOp::IngestSession { messages } => {
                let session_id = format!("proptest-session-{}", *messages_written);
                let mut events = vec![IngestEvent::Session(Session {
                    id: session_id.clone(),
                    parent_session_id: None,
                    parent_message_id: None,
                    source_agent: "claude-code".to_owned(),
                    created_at: Utc::now(),
                    project: Extracted::from_test_value("/tmp/proptest".to_owned()),
                    options: ProviderOptions::new(),
                })];
                for index in 0..messages {
                    let message_id = format!("msg-{}-{index}", *messages_written);
                    events.push(IngestEvent::Message(Message::User {
                        id: message_id.clone(),
                        session_id: session_id.clone(),
                        timestamp: Utc::now(),
                        options: ProviderOptions::new(),
                    }));
                    events.push(IngestEvent::Part(Part {
                        session_id: session_id.clone(),
                        id: format!("{message_id}-part"),
                        message_id,
                        ordinal: 0,
                        provenance: Provenance::Conversational,
                        options: ProviderOptions::new(),
                        kind: PartKind::Text {
                            text: Some(Extracted::from_test_value(format!(
                                "proptest message {index}"
                            ))),
                        },
                    }));
                    *messages_written += 1;
                }
                ingest_events(store, events).await?;
            }
            WriteOp::ClearAllEmbeddings => {
                // Embed every pending row, then clear them all. Exercises
                // the merge_update + fold path twice.
                let keys: Vec<MessageKey> = {
                    use tokio_stream::StreamExt;
                    let mut stream = Box::pin(store.pending_embedding_messages());
                    let mut out = Vec::new();
                    while let Some(pending) = stream.next().await {
                        let pending = pending?;
                        out.push(MessageKey {
                            session_id: pending.session_id,
                            message_id: pending.id,
                        });
                    }
                    out
                };
                if !keys.is_empty() {
                    store.write_embeddings(&embedded(&keys)).await?;
                    store.clear_embeddings(&keys).await?;
                    store.flush_indices(WriteShape::ColumnUpdate).await?;
                }
            }
        }
        Ok(())
    }

    async fn assert_fold_invariants(store: &Store) -> Result<()> {
        // Every maintained scalar / FTS index on `messages` fully covers
        // the data (vector index is the only one allowed to trail under
        // the activation threshold, and our proptest threshold keeps it
        // unbuilt).
        assert_eq!(
            store.unindexed_message_backlog().await?,
            0,
            "FTS index must fully cover messages after fold-on-write",
        );
        Ok(())
    }
}
