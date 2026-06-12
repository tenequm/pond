//! The session datasets (spec.md#datasets): the three Lance tables, the
//! `Store` facade, ingest validation, and `search_text` extraction.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use async_stream::try_stream;
use chrono::{DateTime, TimeZone, Utc};
use lance::Dataset;
use lance::dataset::{AutoCleanupParams, WriteMode, WriteParams};
use lance::deps::arrow_array::{
    Array, FixedSizeListArray, Float16Array, Float32Array, Int32Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray,
    UInt64Array, new_null_array,
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
        Handle, IndexIntent, IndexParamsKind, IndexStatus, IndexTrigger, MaintenancePolicy,
        OptimizeProgressFn, PhaseOutcome, Predicate, ScalarValue, ScanOpts, Table,
        TableOptimizeOutcome, TableSizes, VECTOR_INDEX_ACTIVATION_ROWS,
    },
    wire::{
        FileData, Message, Part, PartKind, ResponseMode, Role, SUMMARY_PART_TYPES, Session,
        SessionFrom,
    },
};
use url::Url;

#[derive(Debug)]
pub struct Store {
    handle: Handle,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanceArchiveCounts {
    pub sessions: usize,
    pub messages: usize,
    pub parts: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanceArchiveVersions {
    pub sessions: u64,
    pub messages: u64,
    pub parts: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanceArchiveExport {
    pub rows: LanceArchiveCounts,
    pub source_versions: LanceArchiveVersions,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanceArchiveImport {
    pub rows: LanceArchiveCounts,
    pub inserted: LanceArchiveCounts,
}

#[derive(Debug, Clone, Default)]
pub struct IndexIntents {
    pub sessions: Vec<IndexIntent>,
    pub messages: Vec<IndexIntent>,
    pub parts: Vec<IndexIntent>,
}

impl IndexIntents {
    fn all(&self) -> [(Table, &[IndexIntent]); 3] {
        [
            (Table::Sessions, &self.sessions),
            (Table::Messages, &self.messages),
            (Table::Parts, &self.parts),
        ]
    }
}

/// A message awaiting embedding: its primary key plus the `search_text` to
/// embed. The vector lives on the same `messages` row, so no denormalized
/// filter columns are needed (spec.md#session-embed-from-canonical).
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

/// What one `Store::optimize_indices` or `Store::build_indices_only` pass did
/// across every table. Each [`TableOptimizeOutcome`] reports phase-by-phase
/// results so the CLI can render compaction-skipped (under writer contention)
/// distinctly from index-build failure (real problem).
#[derive(Debug, Default)]
pub struct OptimizeOutcome {
    pub tables: Vec<TableOptimizeOutcome>,
}

impl OptimizeOutcome {
    /// True if any table's indices phase reported a non-conflict failure.
    /// `SkippedConflict` is expected under contention and does not count.
    pub fn any_indices_failed(&self) -> bool {
        self.tables.iter().any(|t| t.indices.is_failed())
    }

    /// Treat any `Failed` phase as an error. Tests that don't run under
    /// contention use this to keep their existing `.await?` style: a real
    /// failure becomes an `Err`, while `SkippedConflict` is impossible there.
    pub fn into_result(self) -> Result<Self> {
        for table in &self.tables {
            if let PhaseOutcome::Failed(error) = &table.indices {
                anyhow::bail!(
                    "indices phase failed on {}: {error:#}",
                    table.table.as_str()
                );
            }
            if let PhaseOutcome::Failed(error) = &table.compaction {
                anyhow::bail!(
                    "compaction phase failed on {}: {error:#}",
                    table.table.as_str()
                );
            }
        }
        Ok(self)
    }
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
    /// config names; pond does not parse them. Empty options + default caps
    /// is equivalent to [`Store::open`]. Cache caps come from the `[runtime]`
    /// config block via [`crate::substrate::RuntimeCaps`].
    pub async fn open_with_options(
        location: &Url,
        storage_options: std::collections::HashMap<String, String>,
        caps: crate::substrate::RuntimeCaps,
    ) -> Result<Self> {
        Ok(Self {
            handle: Handle::open_with_options(location, storage_options, caps).await?,
        })
    }

    /// Convenience for tests and CLI verbs holding a `&Path`: wraps the path in
    /// a `file://...` URL via [`config::url_for_path`] before opening. Routes
    /// through [`Store::open_with_options`] so the production policy is
    /// applied; tests get the backend-aware local-FS defaults.
    pub async fn open_local(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let url = config::url_for_path(path)?;
        Self::open_with_options(
            &url,
            std::collections::HashMap::new(),
            crate::substrate::RuntimeCaps::default(),
        )
        .await
    }

    /// Export clean, index-free Lance datasets into `dest`.
    ///
    /// This rewrites the visible rows of each table instead of copying the
    /// dataset roots. The resulting manifests therefore contain no references
    /// to the source store's `_indices`, while `messages.vector` and
    /// `messages.embedding_model` remain ordinary data columns and are
    /// preserved.
    pub async fn export_clean_lance_datasets(&self, dest: &Path) -> Result<LanceArchiveExport> {
        std::fs::create_dir_all(dest)
            .with_context(|| format!("failed to create archive staging dir {}", dest.display()))?;
        let (sessions, sessions_version) = self
            .export_clean_table(Table::Sessions, &dest.join("sessions.lance"))
            .await?;
        let (messages, messages_version) = self
            .export_clean_table(Table::Messages, &dest.join("messages.lance"))
            .await?;
        let (parts, parts_version) = self
            .export_clean_table(Table::Parts, &dest.join("parts.lance"))
            .await?;
        Ok(LanceArchiveExport {
            rows: LanceArchiveCounts {
                sessions,
                messages,
                parts,
            },
            source_versions: LanceArchiveVersions {
                sessions: sessions_version,
                messages: messages_version,
                parts: parts_version,
            },
        })
    }

    pub async fn import_clean_lance_datasets(&self, source: &Path) -> Result<LanceArchiveImport> {
        let sessions_dataset =
            open_archive_table(Table::Sessions, &source.join("sessions.lance")).await?;
        let messages_dataset =
            open_archive_table(Table::Messages, &source.join("messages.lance")).await?;
        let parts_dataset = open_archive_table(Table::Parts, &source.join("parts.lance")).await?;
        let (sessions, sessions_inserted) = self
            .import_clean_table(Table::Sessions, sessions_dataset)
            .await?;
        let (messages, messages_inserted) = self
            .import_clean_table(Table::Messages, messages_dataset)
            .await?;
        let (parts, parts_inserted) = self.import_clean_table(Table::Parts, parts_dataset).await?;
        Ok(LanceArchiveImport {
            rows: LanceArchiveCounts {
                sessions,
                messages,
                parts,
            },
            inserted: LanceArchiveCounts {
                sessions: sessions_inserted,
                messages: messages_inserted,
                parts: parts_inserted,
            },
        })
    }

    async fn export_clean_table(&self, table: Table, dest: &Path) -> Result<(usize, u64)> {
        let dataset = self.handle.dataset(table).await?;
        let source_version = dataset.version_id();
        let schema = export_schema(table);
        let mut scan = dataset.scan();
        // The default scan projects blob columns as descriptor structs
        // (position/size into the source's blob storage) - meaningless in an
        // archive and unwritable at V2_1. `AllBinary` materializes the bytes
        // so the rewritten table is self-contained.
        scan.blob_handling(lance::datatypes::BlobHandling::AllBinary);
        let mut stream = scan
            .try_into_stream()
            .await
            .with_context(|| format!("failed to scan {} for archive export", table.as_str()))?;
        let dest_uri = dest
            .to_str()
            .with_context(|| format!("archive path is not UTF-8: {}", dest.display()))?;

        let mut rows = 0usize;
        let mut wrote = false;
        while let Some(batch) = stream.next().await {
            let batch = batch
                .with_context(|| format!("failed to read {} archive batch", table.as_str()))?;
            rows += batch.num_rows();
            let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
            let mut params = write_params_for_create();
            if wrote {
                params.mode = WriteMode::Append;
            }
            Dataset::write(reader, dest_uri, Some(params))
                .await
                .with_context(|| format!("failed to write {} archive table", table.as_str()))?;
            wrote = true;
        }

        if !wrote {
            let batch = RecordBatch::new_empty(schema.clone());
            let reader = RecordBatchIterator::new([Ok(batch)], schema);
            Dataset::write(reader, dest_uri, Some(write_params_for_create()))
                .await
                .with_context(|| {
                    format!("failed to write empty {} archive table", table.as_str())
                })?;
        }
        Ok((rows, source_version))
    }

    async fn import_clean_table(&self, table: Table, dataset: Dataset) -> Result<(usize, usize)> {
        // Force the destination table into existence up front: an empty
        // archive table yields zero batches, so merge_insert alone would
        // leave a lazily-created table (parts) missing on the destination.
        let _ = self.handle.dataset(table).await?;
        let mut scan = dataset.scan();
        // Mirror of the export side: materialize blob bytes, not descriptor
        // structs - merge_insert writes them into the destination's schema.
        scan.blob_handling(lance::datatypes::BlobHandling::AllBinary);
        let mut stream = scan
            .try_into_stream()
            .await
            .with_context(|| format!("failed to scan {} archive table", table.as_str()))?;
        let mut rows = 0usize;
        let mut inserted = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch
                .with_context(|| format!("failed to read {} archive batch", table.as_str()))?;
            let row_count = batch.num_rows();
            rows += row_count;
            inserted += self
                .handle
                .merge_insert(table, batch, row_count)
                .await
                .with_context(|| format!("failed to import {} archive table", table.as_str()))?
                as usize;
        }
        Ok((rows, inserted))
    }

    /// Flat write path. Per-row insert/match truth is not synthesized here -
    /// honest outcomes come from the pre-existence scan on
    /// [`Self::upsert_session_batch`]; the CLI sync and wire ingest paths use
    /// that, so these helpers only need to surface write failure.
    pub async fn upsert_sessions(&self, sessions: &[Session]) -> Result<()> {
        if sessions.is_empty() {
            return Ok(());
        }
        let batches = sessions_batches(sessions)?;
        merge_insert_chunks(&self.handle, Table::Sessions, batches).await?;
        Ok(())
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
    ///      independent, so these proceed concurrently.
    ///   5. Composes per-session [`RowOutcome`]s in original substream order.
    async fn upsert_session_batch(
        &self,
        substreams: Vec<CompletedSubstream>,
    ) -> Result<(Vec<RowOutcome>, BatchCounts)> {
        if substreams.is_empty() {
            return Ok((Vec::new(), BatchCounts::default()));
        }

        let mut outcomes: Vec<RowOutcome> = Vec::with_capacity(substreams.len());
        let mut counts = BatchCounts::default();

        // In-batch dedup. First occurrence of each session_id wins; later
        // occurrences either merge or get rejected. Iteration order preserves
        // original substream order so outcomes index correctly.
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

        // Pre-existence sweep: one scan per table keyed on the batch's
        // session_ids, capped at the substream count. Replaces the prior
        // N-sequential `find_session` calls and gives us honest per-row
        // Inserted/Matched attribution downstream (spec.md#adapter-integrity-additive-sync).
        let session_id_values: Vec<ScalarValue> = merged
            .iter()
            .map(|substream| ScalarValue::String(substream.session.id.clone()))
            .collect();
        let existing_sessions: std::collections::HashMap<String, Session> =
            if session_id_values.is_empty() {
                std::collections::HashMap::new()
            } else {
                let batch = self
                    .handle
                    .scan_batch(
                        Table::Sessions,
                        Some(&Predicate::In("id", session_id_values.clone())),
                        &[],
                    )
                    .await?;
                let mut map = std::collections::HashMap::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let session = session_from_batch(&batch, row)?;
                    map.insert(session.id.clone(), session);
                }
                map
            };
        let existing_message_pks: HashSet<(String, String)> = if session_id_values.is_empty() {
            HashSet::new()
        } else {
            let batch = self
                .handle
                .scan_batch(
                    Table::Messages,
                    Some(&Predicate::In("session_id", session_id_values.clone())),
                    &["session_id", "id"],
                )
                .await?;
            let mut set = HashSet::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let sid = string(&batch, "session_id", row)?.context("session_id is null")?;
                let mid = string(&batch, "id", row)?.context("message id is null")?;
                set.insert((sid, mid));
            }
            set
        };
        let existing_part_pks: HashSet<(String, String, String)> = if session_id_values.is_empty() {
            HashSet::new()
        } else {
            let batch = self
                .handle
                .scan_batch(
                    Table::Parts,
                    Some(&Predicate::In("session_id", session_id_values)),
                    &["session_id", "message_id", "id"],
                )
                .await?;
            let mut set = HashSet::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let sid = string(&batch, "session_id", row)?.context("session_id is null")?;
                let mid = string(&batch, "message_id", row)?.context("message_id is null")?;
                let pid = string(&batch, "id", row)?.context("part id is null")?;
                set.insert((sid, mid, pid));
            }
            set
        };

        let mut writeable: Vec<CompletedSubstream> = Vec::with_capacity(merged.len());
        for substream in merged {
            if let Some(existing) = existing_sessions.get(&substream.session.id)
                && let Err(failure) = ensure_immutable_match(existing, &substream.session)
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
            return Ok((outcomes, counts));
        }

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

        // Merge_insert returns a batch-level inserted count which we cross-
        // check against our pre-existence sets, but for per-row truth we
        // attribute through the sets themselves (next loop). Under
        // single-writer the two agree exactly; under a hostile concurrent
        // writer the sets are authoritative for THIS request's wire shape -
        // matched-no-op (spec.md#adapter-integrity-additive-sync) makes the
        // distinction informational, not behavioral.
        let (_sessions_inserted, _messages_inserted, _parts_inserted) = tokio::try_join!(
            merge_insert_chunks(&self.handle, Table::Sessions, session_batches),
            merge_insert_chunks(&self.handle, Table::Messages, message_batches),
            merge_insert_chunks(&self.handle, Table::Parts, part_batches),
        )?;

        for substream in &writeable {
            outcomes.extend(success_outcomes_for_substream(
                substream.session_index,
                &substream.session,
                &substream.messages,
                &existing_sessions,
                &existing_message_pks,
                &existing_part_pks,
                &mut counts,
            ));
        }

        outcomes.sort_by_key(|outcome| outcome.index);
        Ok((outcomes, counts))
    }

    pub async fn upsert_messages(
        &self,
        session: &Session,
        messages: &[MessageWrite<'_>],
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
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
        merge_insert_chunks(&self.handle, Table::Messages, batches).await?;
        Ok(())
    }

    pub async fn upsert_parts(&self, parts: &[Part]) -> Result<()> {
        if parts.is_empty() {
            return Ok(());
        }
        let batches = parts_batches(parts)?;
        merge_insert_chunks(&self.handle, Table::Parts, batches).await?;
        Ok(())
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
    /// (spec.md#adapter-integrity-event-ordering). Reads Lance's `_row_last_updated_at_version` system
    /// column (available because pond enables stable row ids per spec.md#lance-table-creation-stable-row-ids)
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

    /// Whole-session view for `pond_get` session mode (spec.md#protocol).
    /// Conversational filters to `search_text IS NOT NULL`; Complete and
    /// Verbatim scan every message. Every mode attaches compact part summaries;
    /// Verbatim additionally inlines full parts. `after_id` is an exclusive
    /// lower bound (a message id); the page is bounded by `limit` and a byte
    /// budget and never cuts mid-message.
    pub async fn session_view(
        &self,
        session_id: &str,
        params: SessionViewParams<'_>,
    ) -> Result<GetLookup<SessionPage>> {
        let Some(session) = self.find_session(session_id).await? else {
            return Ok(GetLookup::NotFound);
        };

        let mut rows = match params.mode {
            ResponseMode::Conversational => self
                .scan_conversational_messages(session_id)
                .await?
                .into_iter()
                .map(|row| ScanRow {
                    id: row.message_id,
                    role: row.role,
                    timestamp: row.timestamp,
                    text: Some(row.text.into_inner()),
                    content: None,
                })
                .collect(),
            ResponseMode::Complete | ResponseMode::Verbatim => {
                self.scan_all_messages(session_id).await?
            }
        };
        rows.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));

        let start_at = match params.after_id {
            // Append-only stream: a real anchor never vanishes, so an unknown
            // `after_id` is a stale/mistyped client cursor, not "start over".
            Some(after) => match rows.iter().position(|row| row.id == after) {
                Some(idx) => idx + 1,
                None => return Ok(GetLookup::UnknownAfterId),
            },
            None => 0,
        };
        let remaining = rows.get(start_at..).unwrap_or(&[]);
        let (emitted, messages_remaining) = match params.session_from {
            SessionFrom::Start => {
                let n = page_by(remaining, params.limit, params.budget_bytes, |row| {
                    row.text.as_deref().map_or(0, str::len)
                });
                (&remaining[..n], remaining.len() - n)
            }
            // Tail: the newest messages that fit `limit` and the byte budget,
            // dropping oldest first; the newest is always kept and the page
            // stays chronological so the agent reads the flow forward.
            SessionFrom::End => {
                let mut bytes = 0usize;
                let mut start = remaining.len();
                for row in remaining.iter().rev() {
                    if remaining.len() - start >= params.limit {
                        break;
                    }
                    let size = row.text.as_deref().map_or(0, str::len);
                    if start < remaining.len() && bytes + size > params.budget_bytes {
                        break;
                    }
                    bytes += size;
                    start -= 1;
                }
                (&remaining[start..], start)
            }
        };
        let ids: Vec<String> = emitted.iter().map(|row| row.id.clone()).collect();

        // Conversational/Complete only summarize parts; Verbatim inlines every
        // part (blobs included).
        let mut parts_by_message = match params.mode {
            ResponseMode::Verbatim => self.parts_for_messages(session_id, &ids).await?,
            ResponseMode::Conversational | ResponseMode::Complete => {
                self.summary_parts_for_messages(session_id, &ids).await?
            }
        };
        let messages = emitted
            .iter()
            .map(|row| RetrievedMessage {
                id: row.id.clone(),
                role: row.role,
                timestamp: row.timestamp,
                text: row.text.clone(),
                content: row.content.clone(),
                parts: parts_by_message
                    .remove(&(session_id.to_owned(), row.id.clone()))
                    .unwrap_or_default(),
            })
            .collect();

        Ok(GetLookup::Found(SessionPage {
            session,
            messages,
            messages_remaining,
        }))
    }

    /// Message-scope retrieval for `pond_get` message mode (spec.md#protocol):
    /// the target with its full parts (paginated by `after_id` over part
    /// ordinals, then budget) plus up to `2*context_depth` siblings around it.
    /// `None` when no stored message carries `message_id`. Sibling parts are
    /// carried for summarizing; the target's parts ride `target_parts`.
    pub async fn message_view(
        &self,
        message_id: &str,
        params: MessageViewParams<'_>,
    ) -> Result<GetLookup<MessagePage>> {
        let Some(session_id) = self.session_id_for_message(message_id).await? else {
            return Ok(GetLookup::NotFound);
        };
        let Some(session) = self.find_session(&session_id).await? else {
            return Ok(GetLookup::NotFound);
        };
        let mut rows = self.scan_all_messages(&session_id).await?;
        // spec.md#protocol: context siblings follow the response mode, and the
        // default is the conversational view - in carrier-heavy sessions the
        // system/tool rows would otherwise fill the whole +-depth window and
        // push the actual conversation out of it. The target stays regardless
        // of its own role: the caller asked for that message.
        if matches!(params.mode, ResponseMode::Conversational) {
            rows.retain(|row| row.text.is_some() || row.id == message_id);
        }
        rows.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
        let Some(target_pos) = rows.iter().position(|row| row.id == message_id) else {
            return Ok(GetLookup::NotFound);
        };

        let start = target_pos.saturating_sub(params.context_depth);
        let end = (target_pos + params.context_depth + 1).min(rows.len());
        let window = &rows[start..end];
        let window_ids: Vec<String> = window.iter().map(|row| row.id.clone()).collect();
        // The target's full parts (blobs included) ride the response; siblings
        // are only summarized, but they share this one window scan.
        let mut parts_by_message = self.parts_for_messages(&session_id, &window_ids).await?;

        let all_parts = parts_by_message
            .remove(&(session_id.clone(), message_id.to_owned()))
            .unwrap_or_default();
        let start_part = match params.after_id {
            // Exclusive over ordinals: parts are ordinal-sorted, so the first
            // part past the anchor's ordinal is the page start. An anchor absent
            // from the target's parts is a stale/mistyped client cursor.
            Some(after) => match all_parts.iter().find(|part| part.id == after) {
                Some(anchor) => all_parts
                    .iter()
                    .position(|part| part.ordinal > anchor.ordinal)
                    .unwrap_or(all_parts.len()),
                None => return Ok(GetLookup::UnknownAfterId),
            },
            None => 0,
        };
        let remaining_parts = all_parts.get(start_part..).unwrap_or(&[]);
        let part_count = page_by(remaining_parts, params.limit, params.budget_bytes, |part| {
            serde_json::to_string(part).map_or(0, |json| json.len())
        });
        let target_parts = remaining_parts[..part_count].to_vec();
        let target_parts_remaining = remaining_parts.len() - part_count;

        let target_row = &rows[target_pos];
        let target = RetrievedMessage {
            id: target_row.id.clone(),
            role: target_row.role,
            timestamp: target_row.timestamp,
            text: target_row.text.clone(),
            content: target_row.content.clone(),
            // Target structure is carried in full by `target_parts`.
            parts: Vec::new(),
        };
        let siblings = window
            .iter()
            .enumerate()
            .filter(|(idx, _)| start + idx != target_pos)
            .map(|(_, row)| RetrievedMessage {
                id: row.id.clone(),
                role: row.role,
                timestamp: row.timestamp,
                text: row.text.clone(),
                content: row.content.clone(),
                parts: parts_by_message
                    .get(&(session_id.clone(), row.id.clone()))
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();

        Ok(GetLookup::Found(MessagePage {
            session,
            target,
            target_parts,
            target_parts_remaining,
            siblings,
        }))
    }

    async fn scan_all_messages(&self, session_id: &str) -> Result<Vec<ScanRow>> {
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&Predicate::Eq("session_id", session_id.into())),
                &["id", "timestamp", "role", "search_text", "content"],
            )
            .await?;
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let id = string(&batch, "id", row)?.context("message id is null")?;
            let role =
                role_from_str(&string(&batch, "role", row)?.context("message role is null")?)?;
            let timestamp = datetime(&batch, "timestamp", row)?;
            rows.push(ScanRow {
                id,
                role,
                timestamp,
                text: string(&batch, "search_text", row)?,
                content: string(&batch, "content", row)?,
            });
        }
        Ok(rows)
    }

    /// Conversational scan over one session: rows ordered by
    /// `(timestamp, id)`, `IsNotNull("search_text")` pushed down at the
    /// read seam (spec.md#search-prefilter-pushdown).
    pub async fn scan_conversational_messages(
        &self,
        session_id: &str,
    ) -> Result<Vec<ConversationalRow>> {
        let filter = Predicate::And(vec![
            Predicate::Eq("session_id", session_id.into()),
            Predicate::IsNotNull("search_text"),
        ]);
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&filter),
                &["id", "timestamp", "role", "search_text"],
            )
            .await?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let message_id = string(&batch, "id", row)?.context("message id is null")?;
            let role =
                role_from_str(&string(&batch, "role", row)?.context("message role is null")?)?;
            let timestamp = datetime(&batch, "timestamp", row)?;
            let text_str = string(&batch, "search_text", row)?.context(
                "search_text null after IsNotNull pushdown - storage invariant violated",
            )?;
            rows.push(ConversationalRow {
                session_id: session_id.to_owned(),
                message_id,
                role,
                timestamp,
                text: SearchText(text_str),
            });
        }
        rows.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                .then_with(|| a.message_id.cmp(&b.message_id))
        });
        Ok(rows)
    }

    /// Locate the session id for a stored message. Cheap when only the routing
    /// hint is needed - callers that need the messages use `scan_all_messages`.
    pub async fn session_id_for_message(&self, message_id: &str) -> Result<Option<String>> {
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&Predicate::Eq("id", message_id.into())),
                &["session_id"],
            )
            .await?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        string(&batch, "session_id", 0)
    }

    pub async fn row_counts(&self) -> Result<(usize, usize, usize)> {
        self.handle.row_counts().await
    }

    /// A point-in-time `Arc<Dataset>` for `table`, for registering as a
    /// DataFusion `LanceTableProvider` in `pond_sql_query`. Goes through the
    /// handle's freshness gate, so each query sees a current snapshot.
    pub async fn dataset(&self, table: Table) -> Result<Arc<Dataset>> {
        Ok(Arc::new(self.handle.dataset(table).await?))
    }

    /// Write a `pond_sql_query` export artifact.
    pub async fn export_write(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.handle.export_write(name, bytes).await
    }

    /// Read a `pond_sql_query` export artifact back.
    pub async fn export_read(&self, name: &str) -> Result<Vec<u8>> {
        self.handle.export_read(name).await
    }

    /// Local filesystem path of an export artifact on `file://` installs.
    pub fn export_local_path(&self, name: &str) -> Option<std::path::PathBuf> {
        self.handle.export_local_path(name)
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
    /// (spec.md#session-embed-from-canonical). The column update goes through the
    /// write seam and lands as a new manifest version (`append-only`).
    pub async fn write_embeddings(&self, rows: &[EmbeddedMessage]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = embedding_update_batch(rows)?;
        self.handle
            .merge_update(Table::Messages, batch, rows.len())
            .await?;
        Ok(())
    }

    /// Stream the backlog of messages needing embedding: rows with `search_text`
    /// set whose `vector` is null (spec.md#session-embed-from-canonical).
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

    /// Stream messages that are either never embedded or stale under the
    /// current model. `pond embed --force` feeds this to the same unconditional
    /// merge_update as the normal backlog; the filter makes that semantically
    /// equivalent to the conditional update in spec.md#session-embed-from-canonical.
    pub fn pending_or_stale_messages(&self) -> impl Stream<Item = Result<PendingMessage>> + '_ {
        try_stream! {
            let filter = Predicate::And(vec![
                Predicate::IsNotNull("search_text"),
                Predicate::Or(vec![
                    Predicate::IsNull("vector"),
                    Predicate::Ne("embedding_model", embed::model_id().into()),
                ]),
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
                .context("failed to open pending-or-stale messages stream")?;
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

    /// Count of searchable messages (non-null `search_text`) inside the
    /// caller's filter scope - the universe a search actually ran over.
    /// Powers the response's absence honesty (spec.md#search): "no relevant
    /// hits" only means something relative to how many messages were
    /// searchable at all, and 0 tells the caller their filters excluded
    /// everything before retrieval even started.
    pub async fn searchable_in_scope(&self, filter: &Predicate) -> Result<usize> {
        let scope = Predicate::And(vec![Predicate::IsNotNull("search_text"), filter.clone()]);
        let dataset = self.handle.dataset(Table::Messages).await?;
        dataset
            .count_rows(Some(scope.to_lance()))
            .await
            .map_err(Into::into)
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
    /// scalar predicate (spec.md#search-prefilter-pushdown). Combines the caller's
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
        search: Option<&config::SearchConfig>,
    ) -> Result<Vec<(MessageKey, f32)>> {
        let scope = embedded_scope(filter);
        let mut scanner = self.handle.scanner(Table::Messages, Some(&scope)).await?;
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        if let Some(nprobes) = search.and_then(|cfg| cfg.nprobes) {
            scanner.nprobes(nprobes);
        }
        if let Some(refine_factor) = search.and_then(|cfg| cfg.refine_factor) {
            scanner.refine(refine_factor);
        }
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
    /// `search-prefilter-pushdown` regression guard reads it.
    pub async fn explain_vector_plan(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
        search: Option<&config::SearchConfig>,
    ) -> Result<String> {
        let scope = embedded_scope(filter);
        let mut scanner = self.handle.scanner(Table::Messages, Some(&scope)).await?;
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        if let Some(nprobes) = search.and_then(|cfg| cfg.nprobes) {
            scanner.nprobes(nprobes);
        }
        if let Some(refine_factor) = search.and_then(|cfg| cfg.refine_factor) {
            scanner.refine(refine_factor);
        }
        scanner
            .explain_plan(true)
            .await
            .context("explain_plan failed")
    }

    pub async fn explain_fts_plan(
        &self,
        query: &str,
        limit: usize,
        filter: &Predicate,
    ) -> Result<String> {
        let mut scanner = self.handle.scanner(Table::Messages, Some(filter)).await?;
        scanner.full_text_search(
            FullTextSearchQuery::new(query.to_owned()).with_column("search_text".to_owned())?,
        )?;
        scanner.project(&["session_id", "id"])?;
        scanner.limit(Some(i64::try_from(limit).unwrap_or(i64::MAX)), None)?;
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

    /// Total message count per session, for search session summaries.
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

    /// Rows appended to `messages` since the FTS index was last optimized.
    /// A missing index reports the whole table; the query is manifest-only.
    pub async fn unindexed_message_backlog(&self) -> Result<usize> {
        self.handle
            .unindexed_row_count(Table::Messages, MESSAGES_FTS_INDEX)
            .await
    }

    /// Rows added or rewritten in `messages` since the IVF_PQ vector index
    /// was last optimized. Below
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
    /// uses to detect a model swap and require `--force`.
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

    /// Run the per-table maintenance cycle (compact + indices) across every
    /// table, never short-circuiting. spec.md#lance-index-maintenance: indices
    /// and compaction commit independently, so a hot writer that starves
    /// compaction on one table does not abort the index work the operator
    /// asked for on other tables (or even on the same table).
    pub async fn optimize_indices(
        &self,
        progress: Option<OptimizeProgressFn>,
        maintenance: &MaintenancePolicy,
    ) -> Result<OptimizeOutcome> {
        let intents = pond_index_intents();
        let mut tables = Vec::with_capacity(3);
        for (table, intents) in intents.all() {
            let outcome = self
                .handle
                .optimize_table(table, intents, progress.as_ref(), maintenance)
                .await;
            tables.push(outcome);
        }
        Ok(OptimizeOutcome { tables })
    }

    /// Fold trailing fragments into existing indices across every table,
    /// without running compaction. Used by `pond embed`'s tail so newly
    /// written vectors land in the FTS / IVF_PQ / btree / bitmap indices
    /// without paying the compaction retry budget while embed itself may
    /// still be writing in a sibling process.
    pub async fn build_indices_only(
        &self,
        progress: Option<OptimizeProgressFn>,
    ) -> Result<OptimizeOutcome> {
        let policy = pond_index_intents();
        let mut tables = Vec::with_capacity(3);
        for (table, intents) in policy.all() {
            let indices = self
                .handle
                .optimize_table_indices_only(table, intents, progress.as_ref())
                .await;
            tables.push(TableOptimizeOutcome {
                table,
                indices,
                compaction: PhaseOutcome::NotAttempted,
            });
        }
        Ok(OptimizeOutcome { tables })
    }

    #[cfg(test)]
    async fn optimize_indices_with_vector_threshold(
        &self,
        vector_threshold: usize,
    ) -> Result<OptimizeOutcome> {
        let intents = pond_index_intents_with_vector_threshold(vector_threshold);
        let policy = MaintenancePolicy::always_compact();
        let mut tables = Vec::with_capacity(3);
        for (table, intents) in intents.all() {
            let outcome = self
                .handle
                .optimize_table(table, intents, None, &policy)
                .await;
            tables.push(outcome);
        }
        Ok(OptimizeOutcome { tables })
    }

    pub async fn rebuild_indices(&self, intent_name: Option<&str>) -> Result<()> {
        let policy = pond_index_intents();
        let mut matched = false;
        for (table, intents) in policy.all() {
            for intent in intents {
                if intent_name.is_none_or(|name| name == intent.name) {
                    matched = true;
                    self.handle.rebuild_index(table, intent).await?;
                }
            }
        }
        if let Some(name) = intent_name
            && !matched
        {
            anyhow::bail!("unknown index intent {name:?}");
        }
        Ok(())
    }

    pub async fn index_status(&self) -> Result<Vec<IndexStatus>> {
        let policy = pond_index_intents();
        let mut statuses = Vec::new();
        for (table, intents) in policy.all() {
            statuses.extend(self.handle.index_status(table, intents).await?);
        }
        Ok(statuses)
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
                if msg.contains("not found") || msg.contains("does not exist") {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// On-disk byte totals per dataset, sized through Lance's object store
    /// (spec.md#lance-chokepoints-storage) so `pond status` works on any backend.
    pub async fn table_sizes(&self) -> Result<TableSizes> {
        self.handle.table_sizes().await
    }

    pub async fn initialized(&self) -> Result<bool> {
        self.handle.initialized().await
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

    /// Every part of these messages, full fidelity (file blobs included). The
    /// canonical read primitive - restore/export, verbatim mode, and the
    /// message-mode target all need the complete set.
    pub async fn parts_for_messages(
        &self,
        session_id: &str,
        message_ids: &[String],
    ) -> Result<BTreeMap<(String, String), Vec<Part>>> {
        self.scan_parts(session_id, message_ids, None).await
    }

    /// Only the parts that yield a [`PartSummary`] ([`SUMMARY_PART_TYPES`]),
    /// skipping `text`/`reasoning` (and their blobs) that would summarize to
    /// nothing. For the summary-only reads (conversational/complete session
    /// views, search hits) - it never feeds restore/export.
    pub async fn summary_parts_for_messages(
        &self,
        session_id: &str,
        message_ids: &[String],
    ) -> Result<BTreeMap<(String, String), Vec<Part>>> {
        self.scan_parts(session_id, message_ids, Some(SUMMARY_PART_TYPES))
            .await
    }

    async fn scan_parts(
        &self,
        session_id: &str,
        message_ids: &[String],
        part_types: Option<&[&str]>,
    ) -> Result<BTreeMap<(String, String), Vec<Part>>> {
        if message_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut clauses = vec![
            Predicate::Eq("session_id", session_id.into()),
            in_predicate("message_id", message_ids),
        ];
        if let Some(types) = part_types {
            clauses.push(Predicate::In(
                "type",
                types.iter().map(|&t| t.into()).collect(),
            ));
        }
        let predicate = Predicate::And(clauses);
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
        let mut file_rows = Vec::<(usize, u64, Vec<u8>)>::new();
        for row in 0..batch.num_rows() {
            if string(&batch, "type", row)?.as_deref() == Some("file") {
                let variant_data =
                    json_column(&batch, "variant_data", row)?.context("variant_data is null")?;
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
                // Legacy blob (lance-encoding:blob): payload is bytes; the
                // url variant stored its URL as UTF-8 bytes, recovered via
                // `file_data_from_blob`'s `data_kind = "url"` branch.
                let payload = file_data_from_blob(&variant_data, &blob.read().await?)?;
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
/// The shape is set by spec.md#adapter-integrity-event-ordering.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IngestSummary {
    /// Rows actually written to Lance, summed across all three tables.
    /// Use the per-table fields below for user-facing counts; this stays
    /// for `accepted()` and existing wire callers.
    pub inserted: usize,
    /// Rows that already existed (merge_insert no-op match).
    pub matched: usize,
    /// Session rows inserted this pass.
    pub sessions_inserted: usize,
    /// Message rows inserted this pass (total - includes tool calls,
    /// tool results, and other non-searchable messages).
    pub messages_inserted_total: usize,
    /// Subset of `messages_inserted_total` whose `search_text` is non-null
    /// (eligible for FTS + semantic indexing). The user-facing "messages"
    /// count in `pond sync` / `pond status` reads this field.
    pub messages_inserted_searchable: usize,
    /// Part rows inserted this pass.
    pub parts_inserted: usize,
    /// Session rows already-present (merge_insert matched).
    pub sessions_matched: usize,
    /// Message rows already-present (merge_insert matched), total.
    pub messages_matched_total: usize,
    /// Subset of `messages_matched_total` with `search_text`.
    pub messages_matched_searchable: usize,
    /// Part rows already-present.
    pub parts_matched: usize,
    /// Events the validator dropped under per-event-drop policy (ordering
    /// violation, orphan part, mismatched parent, adapter parse failure,
    /// duplicate-id collision, ...). Counted by event, not by session: a
    /// session with one bad part stays in this bucket as 1, not as "the
    /// whole substream." Per spec.md#adapter-integrity-dedup, adapters SHOULD dedupe their
    /// own emissions upstream when source replay is expected; the
    /// validator's in-batch HashSet is a safety net, not a feature
    /// adapters may rely on. If this bucket grows on a clean adapter,
    /// inspect `drop_reasons` for the top contributors.
    pub dropped_events: usize,
    /// Sessions whose Session-level invariants (immutable `source_agent` /
    /// `project` against the stored row) failed at flush time and
    /// whose substream got rejected wholesale. Always small relative to
    /// `inserted`; if not, there's a real problem to investigate.
    pub dropped_sessions: usize,
    /// Files the adapter couldn't decode at all (no Session header
    /// extractable: empty `.jsonl`, missing required field).
    pub skipped_files: usize,
    /// Files that produced no importable session and were benignly skipped:
    /// empty `.jsonl`, sidecar-only rows (e.g. an `ai-title`/`agent-name`
    /// metadata file), or an unextractable header. Never an error or a drop;
    /// the underlying cause is logged at `-vv` (debug) verbosity.
    pub skipped_empty: usize,
    /// Sessions short-circuited via the per-session staleness skip
    /// (spec.md#adapter-integrity-event-ordering): file `mtime` was at or before the wall-clock time
    /// pond last wrote that session's row, so re-decode was bypassed.
    pub skipped_fresh: usize,
    /// Storage-layer failures whose retries were exhausted (commit
    /// conflicts, transient IO that didn't recover). Hard zero on healthy
    /// runs.
    pub storage_errors: usize,
    /// Oversized values truncated to a bounded sentinel at the seam
    /// (spec.md#adapter-bounded-values); the rest of each such record is intact.
    pub truncated_values: usize,
    /// Histogram of stable reason keys for the combined `dropped_events +
    /// dropped_sessions` populations. Keys are `&'static str` (see the
    /// `DROP_REASON_*` constants) so consumers can match by identity.
    /// Empty on a clean run. Used by `pond sync` to print the top reasons
    /// and by `benches/ingest_bench.rs` to bucket Partial drops by cause.
    pub drop_reasons: BTreeMap<&'static str, usize>,
}

/// Stable reason keys for the `IngestSummary::drop_reasons` histogram and
/// the per-row `RowError::reason_key`. `&'static str` so consumers can
/// match by identity rather than prose. Adding a new variant: pick a short
/// snake_case identifier, route it from the validator/adapter, and update
/// the per-row outcome docs in `docs/spec.md#adapter-integrity-event-ordering`.
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

/// Honest per-table outcome of one batched flush. Built from `merge_insert`'s
/// returned counts together with the pre-existence sets captured by
/// `upsert_session_batch`. Folded into a per-sync summary via
/// [`IngestSummary::add_batch`]. spec.md#adapter-integrity-additive-sync: matched
/// is a no-op write, so the inserted/matched split is informational - we still
/// surface it because both `pond sync` and `pond_ingest` clients reconcile
/// against "which rows landed this call."
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchCounts {
    pub sessions_inserted: usize,
    pub sessions_matched: usize,
    pub messages_inserted_total: usize,
    pub messages_inserted_searchable: usize,
    pub messages_matched_total: usize,
    pub messages_matched_searchable: usize,
    pub parts_inserted: usize,
    pub parts_matched: usize,
}

impl IngestSummary {
    pub fn accepted(&self) -> usize {
        self.inserted + self.matched
    }

    /// Sole writer of the per-table counters on the CLI batched flush path.
    /// The wire single-row path keeps using [`Self::add_outcomes`]; emitting
    /// both for the same rows would double-count.
    pub fn add_batch(&mut self, counts: &BatchCounts) {
        self.sessions_inserted += counts.sessions_inserted;
        self.sessions_matched += counts.sessions_matched;
        self.messages_inserted_total += counts.messages_inserted_total;
        self.messages_inserted_searchable += counts.messages_inserted_searchable;
        self.messages_matched_total += counts.messages_matched_total;
        self.messages_matched_searchable += counts.messages_matched_searchable;
        self.parts_inserted += counts.parts_inserted;
        self.parts_matched += counts.parts_matched;
        self.inserted +=
            counts.sessions_inserted + counts.messages_inserted_total + counts.parts_inserted;
        self.matched +=
            counts.sessions_matched + counts.messages_matched_total + counts.parts_matched;
    }

    /// Sum every counter from `other` into `self`. Used by the multi-source
    /// `pond sync` loop so adding a new field to this struct doesn't silently
    /// drop on aggregation - the prior hand-rolled `+=` block grew bugs.
    pub fn merge(&mut self, other: &Self) {
        self.inserted += other.inserted;
        self.matched += other.matched;
        self.sessions_inserted += other.sessions_inserted;
        self.messages_inserted_total += other.messages_inserted_total;
        self.messages_inserted_searchable += other.messages_inserted_searchable;
        self.parts_inserted += other.parts_inserted;
        self.sessions_matched += other.sessions_matched;
        self.messages_matched_total += other.messages_matched_total;
        self.messages_matched_searchable += other.messages_matched_searchable;
        self.parts_matched += other.parts_matched;
        self.dropped_events += other.dropped_events;
        self.dropped_sessions += other.dropped_sessions;
        self.skipped_files += other.skipped_files;
        self.skipped_empty += other.skipped_empty;
        self.skipped_fresh += other.skipped_fresh;
        self.storage_errors += other.storage_errors;
        self.truncated_values += other.truncated_values;
        for (key, value) in &other.drop_reasons {
            *self.drop_reasons.entry(key).or_insert(0) += value;
        }
    }

    /// Same dispatch as [`Self::add_outcomes`] but ignores
    /// `Inserted`/`Matched` rows. The CLI batched path drives those counters
    /// via [`Self::add_batch`] and uses this method to attribute per-row
    /// `Error` outcomes from the same flush.
    pub fn add_outcomes_errors_only(&mut self, outcomes: &[RowOutcome]) {
        for outcome in outcomes {
            if !matches!(outcome.status, OutcomeStatus::Error) {
                continue;
            }
            if outcome.kind == "session" {
                self.dropped_sessions += 1;
            } else {
                self.dropped_events += 1;
            }
            let reason = outcome
                .error
                .as_ref()
                .and_then(|error| error.reason_key)
                .unwrap_or(DROP_REASON_UNCATEGORIZED);
            *self.drop_reasons.entry(reason).or_insert(0) += 1;
        }
    }

    pub fn add_outcomes(&mut self, outcomes: &[RowOutcome]) {
        for outcome in outcomes {
            match outcome.status {
                OutcomeStatus::Inserted => {
                    self.inserted += 1;
                    match outcome.kind {
                        "session" => self.sessions_inserted += 1,
                        "message" => {
                            self.messages_inserted_total += 1;
                            if outcome.searchable {
                                self.messages_inserted_searchable += 1;
                            }
                        }
                        "part" => self.parts_inserted += 1,
                        _ => {}
                    }
                }
                OutcomeStatus::Matched => {
                    self.matched += 1;
                    match outcome.kind {
                        "session" => self.sessions_matched += 1,
                        "message" => {
                            self.messages_matched_total += 1;
                            if outcome.searchable {
                                self.messages_matched_searchable += 1;
                            }
                        }
                        "part" => self.parts_matched += 1,
                        _ => {}
                    }
                }
                OutcomeStatus::Error => {
                    // Session-level rejection: exactly one session-kind Error
                    // outcome (see `error_outcomes_for_substream`). Per-event
                    // drop: one Error per message/part. The two populations
                    // are counted separately so the operator can tell a
                    // structural reject from a row-level skip.
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
    /// True iff `kind == "message"` AND the underlying row carries
    /// `search_text`. Drives `IngestSummary::messages_inserted_searchable`
    /// so the CLI can show "searchable" message deltas distinct from raw
    /// inserts. Always false for session/part rows.
    pub searchable: bool,
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
/// on re-write) drop the whole substream (spec.md#adapter-integrity-event-ordering).
///
/// Writes are batched at flush time. As complete substreams arrive (a new
/// `Session` event closes out the current one), they accumulate in
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

/// Ingest host provenance (`options.pond`, spec.md#model-pond-options),
/// computed once per process. An audit fact - "the process that inserted this
/// row" - not identity. Fallible lookups are omitted, never synthesized as
/// placeholders.
fn ingest_host_stamp() -> Option<&'static Value> {
    static STAMP: std::sync::OnceLock<Option<Value>> = std::sync::OnceLock::new();
    STAMP
        .get_or_init(|| {
            let mut host = serde_json::Map::new();
            if let Ok(username) = whoami::username() {
                host.insert("username".to_owned(), username.into());
            }
            if let Ok(hostname) = whoami::hostname() {
                host.insert("hostname".to_owned(), hostname.into());
            }
            if let Ok(devicename) = whoami::devicename() {
                host.insert("device_name".to_owned(), devicename.into());
            }
            (!host.is_empty()).then(|| serde_json::json!({ "ingest": { "host": host } }))
        })
        .as_ref()
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
    /// drains the pending-flush buffer. Returns the per-row outcomes (for
    /// the wire layer) alongside the honest per-table counts (for
    /// `IngestSummary::add_batch`).
    pub async fn finish(&mut self, store: &Store) -> Result<(Vec<RowOutcome>, BatchCounts)> {
        self.close_current_substream();
        self.flush(store).await
    }

    /// Drain every completed substream into batched 3-parallel-merge_insert
    /// writes. Caller invokes this periodically (every N completed
    /// substreams) to keep memory bounded; in adapter-driven sync that
    /// happens via the BATCH_SIZE check in `ingest_adapter`. The current
    /// in-flight substream stays buffered - close it explicitly via
    /// [`Self::finish`] or by feeding the next Session event.
    pub async fn flush(&mut self, store: &Store) -> Result<(Vec<RowOutcome>, BatchCounts)> {
        if self.completed.is_empty() {
            return Ok((Vec::new(), BatchCounts::default()));
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
        // Close out the current substream (if any) - move it to the pending
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
                searchable: false,
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
                searchable: false,
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

    fn push_message(&mut self, index: usize, mut message: Message) -> Vec<RowOutcome> {
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
        // `options.pond` is core-owned (spec.md#model-pond-options): stripped
        // and restamped at ingest so neither adapters nor wire clients can
        // spoof provenance. Matched rows are merge_insert no-ops, so re-ingest
        // never restamps stored rows.
        match ingest_host_stamp() {
            Some(stamp) => {
                message
                    .options_mut()
                    .insert("pond".to_owned(), stamp.clone());
            }
            None => {
                message.options_mut().remove("pond");
            }
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
        searchable: false,
    }
}

/// Session-level rejection (immutable `source_agent` / `project` violation):
/// emit exactly one Error outcome on the Session row. The buffered messages
/// and parts of this substream are *not* surfaced as per-row errors - their
/// loss is implied by the single session-rejection (spec.md#adapter-integrity-event-ordering).
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
        searchable: false,
    }]
}

/// Batched-path success helper. Each row's Inserted/Matched status is read
/// from the pre-existence sets captured by `upsert_session_batch` before its
/// `merge_insert` calls, so the per-row outcome is honest (spec.md#adapter-integrity-additive-sync).
/// Also accumulates the per-table totals into `counts` so the CLI summary
/// gets the same truth without re-walking the outcomes.
fn success_outcomes_for_substream(
    session_index: usize,
    session: &Session,
    messages: &[BufferedMessage],
    existing_sessions: &std::collections::HashMap<String, Session>,
    existing_message_pks: &HashSet<(String, String)>,
    existing_part_pks: &HashSet<(String, String, String)>,
    counts: &mut BatchCounts,
) -> Vec<RowOutcome> {
    let session_was_present = existing_sessions.contains_key(&session.id);
    let session_status = if session_was_present {
        counts.sessions_matched += 1;
        UpsertStatus::Matched
    } else {
        counts.sessions_inserted += 1;
        UpsertStatus::Inserted
    };

    let mut outcomes = Vec::with_capacity(1 + messages.len());
    outcomes.push(success_outcome(
        session_index,
        "session",
        Value::String(session.id.clone()),
        session_status,
        false,
    ));
    for buffered in messages {
        let key = (
            buffered.message.session_id().to_owned(),
            buffered.message.id().to_owned(),
        );
        let searchable = buffered.search_text.is_some();
        let message_status = if existing_message_pks.contains(&key) {
            counts.messages_matched_total += 1;
            if searchable {
                counts.messages_matched_searchable += 1;
            }
            UpsertStatus::Matched
        } else {
            counts.messages_inserted_total += 1;
            if searchable {
                counts.messages_inserted_searchable += 1;
            }
            UpsertStatus::Inserted
        };
        let pk = Value::Array(vec![Value::String(key.0), Value::String(key.1)]);
        outcomes.push(success_outcome(
            buffered.index,
            "message",
            pk,
            message_status,
            searchable,
        ));
        for part in &buffered.parts {
            let part_key = (
                part.part.session_id.clone(),
                part.part.message_id.clone(),
                part.part.id.clone(),
            );
            let part_status = if existing_part_pks.contains(&part_key) {
                counts.parts_matched += 1;
                UpsertStatus::Matched
            } else {
                counts.parts_inserted += 1;
                UpsertStatus::Inserted
            };
            let part_pk = Value::Array(vec![
                Value::String(part_key.0),
                Value::String(part_key.1),
                Value::String(part_key.2),
            ]);
            outcomes.push(success_outcome(
                part.index,
                "part",
                part_pk,
                part_status,
                false,
            ));
        }
    }
    outcomes
}

fn success_outcome(
    index: usize,
    kind: &'static str,
    pk: Value,
    status: UpsertStatus,
    searchable: bool,
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
        searchable,
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
                if let Some(media_type) = media_type {
                    chunks.push(media_type.clone());
                }
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

/// Non-empty conversational text (spec.md#search).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchText(String);

impl SearchText {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for SearchText {
    fn as_ref(&self) -> &str {
        &self.0
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

#[derive(Debug, Clone)]
pub struct SessionViewParams<'a> {
    pub mode: ResponseMode,
    pub after_id: Option<&'a str>,
    pub limit: usize,
    pub budget_bytes: usize,
    pub session_from: SessionFrom,
}

#[derive(Debug, Clone)]
pub struct MessageViewParams<'a> {
    pub context_depth: usize,
    /// Which siblings fill the context window: conversational (default)
    /// keeps the window on the human/model exchange; complete/verbatim
    /// include system/tool carriers.
    pub mode: ResponseMode,
    pub after_id: Option<&'a str>,
    pub limit: usize,
    pub budget_bytes: usize,
}

/// Outcome of a `pond_get` lookup. Separates a missing target (the handler
/// maps it to `not_found`) from a stale/unknown `after_id` (mapped to
/// `validation_failed`): the message/part stream is append-only, so an anchor
/// that was ever valid never disappears - an unknown one is always a client
/// error, never a reason to silently restart the page.
#[derive(Debug, Clone, PartialEq)]
pub enum GetLookup<T> {
    NotFound,
    UnknownAfterId,
    Found(T),
}

/// Canonical retrieval result for `pond_get` session mode: the stored session
/// plus the page of messages (each with its `Part`s) and a remaining count.
/// Protocol-shaping into `GetResult`/`MessageView` happens in the handler.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionPage {
    pub session: Session,
    pub messages: Vec<RetrievedMessage>,
    pub messages_remaining: usize,
}

/// Canonical retrieval result for `pond_get` message mode. `target.parts` is
/// empty - the target's parts ride `target_parts` (paginated); `siblings` carry
/// their parts so the handler can summarize them.
#[derive(Debug, Clone, PartialEq)]
pub struct MessagePage {
    pub session: Session,
    pub target: RetrievedMessage,
    pub target_parts: Vec<Part>,
    pub target_parts_remaining: usize,
    pub siblings: Vec<RetrievedMessage>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetrievedMessage {
    pub id: String,
    pub role: Role,
    pub timestamp: DateTime<Utc>,
    pub text: Option<String>,
    pub content: Option<String>,
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone)]
struct ScanRow {
    id: String,
    role: Role,
    timestamp: DateTime<Utc>,
    text: Option<String>,
    content: Option<String>,
}

/// One row of the conversational scan. `text` is non-empty by
/// `IsNotNull("search_text")` pushdown (spec.md#search).
#[derive(Debug, Clone)]
pub struct ConversationalRow {
    pub session_id: String,
    pub message_id: String,
    pub role: Role,
    pub timestamp: DateTime<Utc>,
    pub text: SearchText,
}

/// Number of leading `items` that fit within `limit` and the byte budget,
/// sizing each by `size`. Always emits at least one (a single oversize item
/// never blocks its own page); the budget then stops the page at the next item
/// boundary.
fn page_by<T>(items: &[T], limit: usize, budget_bytes: usize, size: impl Fn(&T) -> usize) -> usize {
    let capped = items.len().min(limit.clamp(1, 1000));
    let mut acc = 0usize;
    let mut emitted = 0usize;
    for item in &items[..capped] {
        let next = acc.saturating_add(size(item));
        if emitted > 0 && next > budget_bytes {
            break;
        }
        acc = next;
        emitted += 1;
    }
    emitted
}

fn role_from_str(value: &str) -> Result<Role> {
    match value {
        "system" => Ok(Role::System),
        "user" => Ok(Role::User),
        "assistant" => Ok(Role::Assistant),
        "tool" => Ok(Role::Tool),
        other => anyhow::bail!("unknown message role {other}"),
    }
}

/// Scalar indexes on `messages` (spec.md#datasets): BTREE for high-cardinality
/// and range columns, BITMAP for low-cardinality columns. There is no index
/// on `embedding_model`: pond's invariant is one active model at a time
/// (a model swap goes through `pond embed --force` which drops the IVF_PQ,
/// clears stale rows, and re-bootstraps), so `embedding_model` is never a
/// query-time predicate - the only embedding-state filter is `vector IS NOT
/// NULL`. `id` lookups are rare and full-scan.
const MESSAGE_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] = &[
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

// Bare logical table names: the lance-namespace Directory impl owns the
// `.lance` directory suffix (spec.md#lance-chokepoints-catalog). No consumer reconstructs
// a `.lance` path.
pub(crate) const SESSIONS: &str = "sessions";
pub(crate) const MESSAGES: &str = "messages";
pub(crate) const PARTS: &str = "parts";

/// FTS index name on `messages.search_text`. Stable so status and index
/// creation name the same index.
pub const MESSAGES_FTS_INDEX: &str = "messages_search_text_fts";

/// IVF_PQ index name on `messages.vector` (spec.md#search). Stable so the
/// activation check and index creation name the same index.
pub const MESSAGES_VECTOR_INDEX: &str = "messages_vector_ivfpq";

/// IVF_PQ tuning constants (spec.md#search):
/// - num_bits = 8 (256 centroids per PQ subspace; needs >= 256 vectors)
/// - sub_vectors = embedding_dim / 8 (8-float PQ subspaces)
/// - max_iters = 15 (kmeans cap)
/// - cosine metric (e5 vectors are L2-normalized)
const IVF_PQ_NUM_BITS: u8 = 8;
const IVF_PQ_SUB_VECTOR_STRIDE: usize = 8;
const IVF_PQ_MAX_ITERS: usize = 15;

/// FTS tokenizer constants (spec.md#search-language-neutral-index): character ngrams
/// in `[3, 5]`. 4-5-grams discriminate, min=3 keeps 3-char tokens
/// (`FTS`, `OCC`) searchable.
const FTS_NGRAM_MIN: u32 = 3;
const FTS_NGRAM_MAX: u32 = 5;

/// Pond's production IndexIntents: the per-table intent set
/// `Store::open_with_options` registers with the substrate.
pub fn pond_index_intents() -> IndexIntents {
    pond_index_intents_with_vector_threshold(VECTOR_INDEX_ACTIVATION_ROWS)
}

/// Same as [`pond_index_intents`] but with an overridable IVF_PQ activation
/// threshold. Used by tests that need to exercise the activation boundary
/// without writing 100k vectors.
pub(crate) fn pond_index_intents_with_vector_threshold(vector_threshold: usize) -> IndexIntents {
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
    IndexIntents {
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
/// `auto_cleanup` is short; long-term recovery is `pond export` snapshots
/// plus deferred Lance tags (spec.md#session-durable-copy). `skip_auto_cleanup`
/// suppresses the per-commit hook so cleanup stays operator-driven via
/// `pond index optimize` (one LIST per command instead of per write).
pub(crate) fn write_params_for_create() -> WriteParams {
    WriteParams {
        data_storage_version: Some(LanceFileVersion::V2_1),
        enable_v2_manifest_paths: true,
        enable_stable_row_ids: true,
        auto_cleanup: Some(AutoCleanupParams {
            interval: 20,
            older_than: chrono::TimeDelta::days(1),
        }),
        skip_auto_cleanup: true,
        ..WriteParams::default()
    }
}

fn export_schema(table: Table) -> Arc<Schema> {
    match table {
        Table::Sessions => session_schema(),
        Table::Messages => message_schema(),
        Table::Parts => part_schema(),
    }
}

fn ensure_schema_matches_archive(dataset: &Dataset, table: Table) -> Result<()> {
    let expected = export_schema(table);
    let actual = lance::deps::arrow_schema::Schema::from(dataset.schema());
    let actual_names: Vec<_> = actual.fields().iter().map(|field| field.name()).collect();
    let expected_names: Vec<_> = expected.fields().iter().map(|field| field.name()).collect();
    if actual_names != expected_names {
        anyhow::bail!(
            "{} archive table has columns {actual_names:?} but this pond build expects {expected_names:?}",
            table.as_str(),
        );
    }
    Ok(())
}

async fn open_archive_table(table: Table, source: &Path) -> Result<Dataset> {
    let source_uri = source
        .to_str()
        .with_context(|| format!("archive path is not UTF-8: {}", source.display()))?;
    let dataset = Dataset::open(source_uri)
        .await
        .with_context(|| format!("failed to open {} archive table", table.as_str()))?;
    ensure_schema_matches_archive(&dataset, table)?;
    Ok(dataset)
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
        json_field("options", false),
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
        // The message's derived embedding (spec.md#session-embed-from-canonical):
        // both null until `pond embed` fills them, set together thereafter.
        Field::new("vector", embedding_vector_type(), true),
        Field::new("embedding_model", DataType::Utf8, true),
        json_field("options", false),
    ]))
}

pub(crate) fn part_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("session_id", DataType::Utf8, false),
        primary_field("message_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new("ordinal", DataType::Int32, false),
        Field::new("type", DataType::Utf8, false),
        // spec.md#model-part-provenance: conversation vs harness-injected; search
        // reads this column to exclude injected scaffolding.
        Field::new("provenance", DataType::Utf8, false),
        json_field("variant_data", false),
        legacy_blob_field("data", true),
        json_field("options", false),
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

// Lance v7.0.0-beta.16's IVF_PQ build path (`rust/lance/src/index/vector/utils.rs`
// `infer_vector_element_type_impl`) accepts only Float16/Float32/Float64/UInt8/Int8;
// `FixedSizeBinary(2)`-backed `lance.bfloat16` is rejected. The format docs list
// BFloat16 as a future-supported embedding type; until the Rust IVF_PQ build
// path catches up, store as Float16 (half-precision, also 2 bytes/element).
fn embedding_vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float16, true)),
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
        flat.extend(row.vector.iter().map(|value| half::f16::from_f32(*value)));
    }
    let values = Float16Array::from(flat);
    let item_field = Arc::new(Field::new("item", DataType::Float16, true));
    let vectors = FixedSizeListArray::try_new(item_field, dim as i32, Arc::new(values), None)
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
/// `StringArray::from` (spec.md#adapter-bounded-values).
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
        .map(|session| json_bytes(&session.options))
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

fn sessions_chunk(sessions: &[Session], options: &[Vec<u8>]) -> Result<RecordBatch> {
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
            Arc::new(LargeBinaryArray::from_iter_values(
                options.iter().map(Vec::as_slice),
            )),
        ],
    )
    .context("failed to build session batch")
}

pub(crate) fn messages_batches(rows: &[MessageBatchRow<'_>]) -> Result<Vec<RecordBatch>> {
    let options = rows
        .iter()
        .map(|row| json_bytes(row.message.options()))
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

fn messages_chunk(rows: &[MessageBatchRow<'_>], options: &[Vec<u8>]) -> Result<RecordBatch> {
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
            // (spec.md#session-embed-from-canonical).
            new_null_array(&embedding_vector_type(), rows.len()),
            new_null_array(&DataType::Utf8, rows.len()),
            Arc::new(LargeBinaryArray::from_iter_values(
                options.iter().map(Vec::as_slice),
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
        .map(|part| json_bytes(&part.options))
        .collect::<Result<Vec<_>>>()?;
    let mut cells = Vec::with_capacity(parts.len());
    // The blob column is a BinaryArray, exempt from the text-column bound
    // (spec.md#adapter-bounded-values); only the StringArray columns are budgeted.
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

fn parts_chunk(
    parts: &[Part],
    variant_data: &[Vec<u8>],
    options: &[Vec<u8>],
) -> Result<RecordBatch> {
    let schema = part_schema();
    // Legacy blob (`legacy_blob_field`) is a plain LargeBinary; the URL
    // variant is stored as UTF-8 bytes and recovered through `variant_data`'s
    // `data_kind = "url"` discriminator (see `file_data_from_blob`).
    let blob_payloads: Vec<Option<&[u8]>> = parts
        .iter()
        .map(|part| match &part.kind {
            PartKind::File { data, .. } => Some(match data {
                FileData::String(value) => value.as_bytes(),
                FileData::Bytes(value) => value.as_slice(),
                FileData::Url(value) => value.as_bytes(),
            }),
            PartKind::Text { .. }
            | PartKind::Reasoning { .. }
            | PartKind::ToolCall { .. }
            | PartKind::ToolResult { .. }
            | PartKind::ToolApprovalRequest { .. }
            | PartKind::ToolApprovalResponse { .. } => None,
        })
        .collect();
    let blob_array = LargeBinaryArray::from_iter(blob_payloads);

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
            Arc::new(LargeBinaryArray::from_iter_values(
                variant_data.iter().map(Vec::as_slice),
            )),
            Arc::new(blob_array),
            Arc::new(LargeBinaryArray::from_iter_values(
                options.iter().map(Vec::as_slice),
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
        options: json_parse(&json_column(batch, "options", row)?.context("options is null")?)?,
    })
}

pub(crate) fn message_from_batch(batch: &RecordBatch, row: usize) -> Result<Message> {
    let id = string(batch, "id", row)?.context("message id is null")?;
    let session_id = string(batch, "session_id", row)?.context("message session_id is null")?;
    let timestamp = datetime(batch, "timestamp", row)?;
    let options =
        json_parse(&json_column(batch, "options", row)?.context("message options is null")?)?;

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
    let variant_data = json_column(batch, "variant_data", row)?.context("variant_data is null")?;
    let provenance = string(batch, "provenance", row)?.context("part provenance is null")?;
    Ok(Part {
        session_id: string(batch, "session_id", row)?.context("part session_id is null")?,
        message_id: string(batch, "message_id", row)?.context("part message_id is null")?,
        id: string(batch, "id", row)?.context("part id is null")?,
        ordinal: int32(batch, "ordinal", row)?,
        provenance: provenance_from_str(&provenance)?,
        options: json_parse(&json_column(batch, "options", row)?.context("part options is null")?)?,
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

fn file_data_from_blob(variant_data: &[u8], bytes: &[u8]) -> Result<FileData> {
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

fn file_data_kind(variant_data: &[u8]) -> Result<String> {
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

fn json_column(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<Vec<u8>>> {
    // Lance can return a `lance.json` column either as raw JSONB bytes
    // (LargeBinary) or auto-converted to the Arrow text form (Utf8 /
    // LargeUtf8), depending on the read path. Handle both.
    let column = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?;
    if let Some(array) = column.as_any().downcast_ref::<LargeBinaryArray>() {
        return if array.is_null(row) {
            Ok(None)
        } else {
            Ok(Some(
                lance_arrow::json::decode_json(array.value(row)).into_bytes(),
            ))
        };
    }
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return if array.is_null(row) {
            Ok(None)
        } else {
            Ok(Some(array.value(row).as_bytes().to_vec()))
        };
    }
    if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
        return if array.is_null(row) {
            Ok(None)
        } else {
            Ok(Some(array.value(row).as_bytes().to_vec()))
        };
    }
    anyhow::bail!("column {name} is not a JSON-compatible array")
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

// Legacy blob storage (`LargeBinary` + `lance-encoding:blob=true`). Blob v2's
// `Struct<data, uri>` extension requires `data_storage_version >= 2.2`, which
// is marked unstable in Lance docs (`format/file/versioning.md`) and at
// v7.0.0-beta.16 trips a `compact_files` bug: the AllBinary blob_handling
// path leaves the field as a 2-child struct but `BlobV2StructuralEncoder`
// allocated only one column_info, so the decoder's second `expect_next()`
// fires `"there were more fields in the schema than provided column
// indices / infos"`. Legacy blob writes `BlobLayout` pages, which compact
// handles correctly (covered by Lance's own `test_compact_blob_columns`).
fn legacy_blob_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::LargeBinary, nullable).with_metadata(
        [(lance_arrow::BLOB_META_KEY.to_owned(), "true".to_owned())]
            .into_iter()
            .collect(),
    )
}

fn json_field(name: &str, nullable: bool) -> Field {
    lance_arrow::json::json_field(name, nullable)
}

fn micros(timestamp: DateTime<Utc>) -> i64 {
    timestamp.timestamp_micros()
}

fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    // Write JSONB bytes (not plain UTF-8 JSON text) so the on-disk encoding
    // matches the `lance.json` extension contract. Lance's compact path
    // (`optimize.rs:908`) reads through `DatasetRecordBatchStream` which
    // applies `decode_json -> encode_json` on this column; with proper JSONB
    // on disk that roundtrip is idempotent, with plain UTF-8 it corrupts
    // (the analogous fix landed for `update.rs` in PR #6741 by switching to
    // `try_into_dfstream`; compact still goes through the adapter).
    let text = serde_json::to_string(value).context("failed to serialize JSON field")?;
    lance_arrow::json::encode_json(&text)
        .map_err(|err| anyhow::anyhow!("failed to encode JSON field as JSONB: {err}"))
}

fn json_parse<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    serde_json::from_slice(value).context("failed to parse JSON field")
}

fn part_variant_json(kind: &PartKind) -> Result<Vec<u8>> {
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
        return json_bytes(&serde_json::json!({
            "media_type": media_type,
            "file_name": file_name,
            "data_kind": data_kind,
        }));
    }
    let value = serde_json::to_value(kind)?;
    let mut object = value
        .as_object()
        .cloned()
        .context("part variant did not serialize to an object")?;
    object.remove("type");
    json_bytes(&object)
}

fn part_kind_from_json(
    type_name: &str,
    variant_data: &[u8],
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
        // Per-event drop semantics (spec.md#adapter-integrity-event-ordering): a Part with no preceding
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
    async fn initialized_flips_only_after_first_ingest() -> anyhow::Result<()> {
        // `open` eagerly creates sessions/messages but `parts` is lazy, so a
        // configured-but-never-synced store reports uninitialized - the signal
        // `pond status`/`pond storage` use to render an empty state instead of
        // erroring on the first parts describe.
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        assert!(
            !store.initialized().await?,
            "fresh store has no parts table"
        );

        let session = synthetic_session("initialized-probe");
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
            kind: PartKind::Text {
                text: Some(Extracted::from_test_value("hello".to_owned())),
            },
        };
        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session))
            .await?;
        validator
            .push(&store, 1, IngestEvent::Message(message))
            .await?;
        validator.push(&store, 2, IngestEvent::Part(part)).await?;
        validator.finish(&store).await?;

        assert!(store.initialized().await?, "ingest creates the parts table");
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
    async fn ingest_stamps_host_provenance_on_messages_and_strips_spoofed_pond_key()
    -> anyhow::Result<()> {
        // spec.md#model-pond-options: `options.pond` is core-owned. A stored
        // message carries the process's host stamp (when resolvable) and never
        // a client-supplied value; session and part options stay untouched.
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("host-provenance");
        let mut spoofed = ProviderOptions::new();
        spoofed.insert("pond".to_owned(), json!({"ingest": {"host": "spoofed"}}));
        let message = Message::User {
            id: "message-1".to_owned(),
            session_id: session.id.clone(),
            timestamp: Utc::now(),
            options: spoofed,
        };
        let part = Part {
            session_id: session.id.clone(),
            id: "part-1".to_owned(),
            message_id: "message-1".to_owned(),
            ordinal: 0,
            provenance: crate::wire::Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: Some(Extracted::from_test_value("hello".to_owned())),
            },
        };

        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session.clone()))
            .await?;
        validator
            .push(&store, 1, IngestEvent::Message(message))
            .await?;
        validator.push(&store, 2, IngestEvent::Part(part)).await?;
        validator.finish(&store).await?;

        let stored = store
            .get_session(&session.id)
            .await?
            .expect("ingested session is readable");
        assert!(
            !stored.session.options.contains_key("pond"),
            "session rows are not stamped (attribution derives from messages)"
        );
        let stored_message = &stored.messages[0].message;
        match ingest_host_stamp() {
            Some(stamp) => {
                assert_eq!(
                    stored_message.options().get("pond"),
                    Some(stamp),
                    "stored message carries the real stamp, never the spoof"
                );
                let host = stamp
                    .pointer("/ingest/host")
                    .and_then(Value::as_object)
                    .expect("stamp shape is {ingest: {host: {..}}}");
                assert!(!host.is_empty(), "an all-empty stamp must be None instead");
                assert!(
                    host.values()
                        .all(|v| v.as_str().is_some_and(|s| !s.is_empty())),
                    "stamp fields are omitted when unavailable, never empty: {host:?}"
                );
            }
            None => assert!(
                stored_message.options().get("pond").is_none(),
                "with no resolvable stamp the spoofed key is still stripped"
            ),
        }
        assert!(
            !stored.messages[0].parts[0].options.contains_key("pond"),
            "part rows are not stamped (covered by their message's stamp)"
        );

        Ok(())
    }

    /// Regression: compact_files on `parts` with the blob column tripped a
    /// Lance v7.0.0-beta.16 dispatch bug under `lance.blob.v2`. Two upsert
    /// batches give compact fragments to merge; every `FileData` variant
    /// exercises the blob round-trip. All-File batches sidestep a debug-only
    /// `debug_assert_eq!` in Lance's legacy blob encoder that trips when one
    /// write batch mixes null + valid rows in the blob column - benign in
    /// release, irrelevant to this regression's scope.
    #[tokio::test(flavor = "multi_thread")]
    async fn optimize_indices_compacts_parts_with_blob_column() -> anyhow::Result<()> {
        use crate::wire::{FileData, PartKind, Provenance};
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;

        let session = synthetic_session("compact-blob");
        store
            .upsert_sessions(std::slice::from_ref(&session))
            .await?;

        let make_part = |idx: usize, kind: PartKind| Part {
            session_id: session.id.clone(),
            message_id: format!("msg-{idx}"),
            id: format!("part-{idx}"),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind,
        };

        let batch_a = vec![
            make_part(
                0,
                PartKind::File {
                    media_type: Some("text/plain".to_owned()),
                    file_name: Some("a.txt".to_owned()),
                    data: FileData::Bytes(b"alpha".to_vec()),
                },
            ),
            make_part(
                1,
                PartKind::File {
                    media_type: Some("text/plain".to_owned()),
                    file_name: Some("b.txt".to_owned()),
                    data: FileData::String("beta".to_owned()),
                },
            ),
        ];
        store.upsert_parts(&batch_a).await?;

        let batch_b = vec![
            make_part(
                2,
                PartKind::File {
                    media_type: Some("application/octet-stream".to_owned()),
                    file_name: None,
                    data: FileData::Url("https://example.com/file".to_owned()),
                },
            ),
            make_part(
                3,
                PartKind::File {
                    media_type: Some("image/png".to_owned()),
                    file_name: Some("c.png".to_owned()),
                    data: FileData::Bytes(vec![0x89, 0x50, 0x4e, 0x47]),
                },
            ),
        ];
        store.upsert_parts(&batch_b).await?;

        store
            .optimize_indices(None, &MaintenancePolicy::always_compact())
            .await?
            .into_result()?;

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
                media_type: Some("text/plain".to_owned()),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn batched_flush_attributes_new_messages_on_existing_session() -> anyhow::Result<()> {
        // Regression guard: re-ingesting an existing session with NEW
        // messages must surface as sessions_inserted=0, messages_inserted_*>0
        // on `BatchCounts`, and per-row outcomes must mark the new message
        // rows `Inserted` while the session row is `Matched`. The prior
        // implementation derived all per-row statuses from the batch-level
        // session inserted count, which silently flipped the new messages
        // into `Matched` (visible as "up to date" in the CLI bar tail).
        use crate::wire::Provenance;
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = base_session();

        let text_part = |part_id: &str, message_id: &str, body: &str| Part {
            session_id: session.id.clone(),
            id: part_id.to_owned(),
            message_id: message_id.to_owned(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: Some(Extracted::from_test_value(body.to_owned())),
            },
        };
        let user_message = |id: &str| Message::User {
            id: id.to_owned(),
            session_id: session.id.clone(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };

        // First pass: 2 messages land fresh.
        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session.clone()))
            .await?;
        validator
            .push(&store, 1, IngestEvent::Message(user_message("m1")))
            .await?;
        validator
            .push(&store, 2, IngestEvent::Part(text_part("p1", "m1", "alpha")))
            .await?;
        validator
            .push(&store, 3, IngestEvent::Message(user_message("m2")))
            .await?;
        validator
            .push(&store, 4, IngestEvent::Part(text_part("p2", "m2", "beta")))
            .await?;
        let (_first_outcomes, first_counts) = validator.finish(&store).await?;
        assert_eq!(first_counts.sessions_inserted, 1);
        assert_eq!(first_counts.messages_inserted_total, 2);
        assert_eq!(first_counts.messages_inserted_searchable, 2);

        // Second pass: same session id, 3 NEW messages.
        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session.clone()))
            .await?;
        for (idx, mid) in ["m3", "m4", "m5"].iter().enumerate() {
            let pid = format!("p{}", idx + 3);
            validator
                .push(&store, idx * 2 + 1, IngestEvent::Message(user_message(mid)))
                .await?;
            validator
                .push(
                    &store,
                    idx * 2 + 2,
                    IngestEvent::Part(text_part(&pid, mid, "gamma")),
                )
                .await?;
        }
        let (second_outcomes, second_counts) = validator.finish(&store).await?;

        assert_eq!(
            second_counts.sessions_inserted, 0,
            "existing session row must report as Matched, not Inserted",
        );
        assert_eq!(second_counts.sessions_matched, 1);
        assert_eq!(
            second_counts.messages_inserted_total, 3,
            "the three NEW messages must register as Inserted in BatchCounts",
        );
        assert_eq!(
            second_counts.messages_inserted_searchable, 3,
            "all three new messages carry conversational text -> searchable",
        );
        assert_eq!(second_counts.messages_matched_total, 0);
        assert_eq!(second_counts.parts_inserted, 3);
        assert_eq!(second_counts.parts_matched, 0);

        // Per-row outcomes mirror the BatchCounts shape: the session row is
        // Matched, every new message + part row is Inserted.
        let session_outcome = second_outcomes
            .iter()
            .find(|outcome| outcome.kind == "session")
            .expect("session-row outcome present");
        assert_eq!(session_outcome.status, OutcomeStatus::Matched);
        for outcome in &second_outcomes {
            if outcome.kind == "message" || outcome.kind == "part" {
                assert_eq!(
                    outcome.status,
                    OutcomeStatus::Inserted,
                    "new row must be Inserted, got: {outcome:?}",
                );
            }
        }
        Ok(())
    }

    /// Ingest `count` synthetic messages spread across a handful of sessions
    /// and projects, each with conversational `search_text`. Returns the store
    /// and the message keys in `msg-{i}` order; every `vector` starts null.
    async fn store_with_messages(
        temp: &TempDir,
        count: usize,
    ) -> anyhow::Result<(Store, Vec<MessageKey>)> {
        store_with_messages_at_threshold(temp, count, VECTOR_INDEX_ACTIVATION_ROWS).await
    }

    /// Same as [`store_with_messages`] but tests optimize with a custom
    /// IVF_PQ activation threshold.
    async fn store_with_messages_at_threshold(
        temp: &TempDir,
        count: usize,
        _vector_threshold: usize,
    ) -> anyhow::Result<(Store, Vec<MessageKey>)> {
        let store = Store::open_local(temp.path()).await?;
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

    fn embedding_update_batch_with_model(
        rows: &[EmbeddedMessage],
        model: &str,
    ) -> Result<RecordBatch> {
        let mut batch = embedding_update_batch(rows)?;
        let columns = batch
            .columns()
            .iter()
            .take(3)
            .cloned()
            .chain(std::iter::once(
                Arc::new(StringArray::from(vec![model; rows.len()])) as _,
            ))
            .collect::<Vec<_>>();
        batch = RecordBatch::try_new(batch.schema(), columns)?;
        Ok(batch)
    }

    #[tokio::test]
    async fn filtered_vector_scan_pushes_scalar_predicate_into_the_index() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        // 4 messages cycle session-0..session-3, so `session-3` is a real
        // partition. Scalar-index pushdown is volume-independent: the planner
        // emits `ScalarIndexQuery` whenever the index exists.
        let (store, keys) = store_with_messages(&temp, 4).await?;
        store.write_embeddings(&embedded(&keys)).await?;
        store
            .optimize_indices(None, &MaintenancePolicy::always_compact())
            .await?
            .into_result()?;

        let query = vec![0.01_f32; embedding_dim()];
        let plan = store
            .explain_vector_plan(
                &query,
                10,
                &Predicate::Eq("session_id", "session-3".into()),
                None,
            )
            .await?;

        // The load-bearing assertion (spec.md#search-prefilter-pushdown): the predicate
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
    async fn vector_index_activates_when_threshold_is_crossed() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages_at_threshold(&temp, 300, 256).await?;

        // First batch: 255 vectors, one below threshold. Optimize does not
        // create the IVF_PQ because the trigger is not met.
        store.write_embeddings(&embedded(&keys[..255])).await?;
        store
            .optimize_indices_with_vector_threshold(256)
            .await?
            .into_result()?;
        assert!(
            !store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "IVF_PQ must not exist below the activation threshold",
        );

        // Next batch: one more vector. Total reaches 256; optimize creates
        // the IVF_PQ.
        store.write_embeddings(&embedded(&keys[255..256])).await?;
        store
            .optimize_indices_with_vector_threshold(256)
            .await?
            .into_result()?;
        assert!(
            store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "optimize must create the IVF_PQ once the threshold is crossed",
        );

        // The remaining 44 rows stay un-embedded; the IVF_PQ trains over the
        // non-null subset and a planted vector is retrievable.
        let hits = store
            .vector_search(&synthetic_vector(0), 10, &Predicate::And(Vec::new()), None)
            .await?;
        assert!(
            hits.iter().any(|(key, _)| key == &keys[0]),
            "an embedded row is retrievable via the index",
        );
        Ok(())
    }

    #[tokio::test]
    async fn model_swap_force_re_embeds_only_stale_rows_and_rebuilds_ivf_pq() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages_at_threshold(&temp, 300, 256).await?;
        let old_rows = embedded(&keys);
        let old_batch = embedding_update_batch_with_model(&old_rows, "old-model")?;
        store
            .handle
            .merge_update(Table::Messages, old_batch, old_rows.len())
            .await?;
        store
            .optimize_indices_with_vector_threshold(256)
            .await?
            .into_result()?;
        assert!(
            store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "IVF_PQ must exist before a model swap",
        );
        assert_eq!(store.stale_embedding_count().await?, keys.len());

        store.drop_vector_index().await?;
        let mut pending = Vec::new();
        let stream = store.pending_or_stale_messages();
        tokio::pin!(stream);
        while let Some(row) = stream.next().await {
            pending.push(row?);
        }
        assert_eq!(
            pending.len(),
            keys.len(),
            "force stream should see stale rows"
        );
        store.write_embeddings(&embedded(&keys)).await?;
        assert_eq!(store.stale_embedding_count().await?, 0);
        store
            .optimize_indices_with_vector_threshold(256)
            .await?
            .into_result()?;
        assert!(
            store
                .handle
                .messages_index_names()
                .await?
                .iter()
                .any(|name| name == MESSAGES_VECTOR_INDEX),
            "optimize must rebuild IVF_PQ after force re-embed",
        );

        let stream = store.pending_or_stale_messages();
        tokio::pin!(stream);
        assert!(stream.next().await.is_none(), "up-to-date rows are skipped");
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
        // older ones become eligible for cleanup.
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
}
