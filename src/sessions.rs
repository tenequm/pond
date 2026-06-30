//! The session datasets (spec.md#datasets): the three Lance tables, the
//! `Store` facade, ingest validation, and `search_text` extraction.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use arrow_select::filter::filter_record_batch;
use async_stream::try_stream;
use chrono::{DateTime, TimeZone, Utc};
use lance::Dataset;
use lance::dataset::{AutoCleanupParams, ProjectionRequest, WriteMode, WriteParams};
use lance::deps::arrow_array::builder::{FixedSizeListBuilder, Float16Builder};
use lance::deps::arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float16Array, Float32Array, Int32Array,
    LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchIterator, StringArray,
    TimestampMicrosecondArray, UInt64Array, new_null_array,
};
use lance::deps::arrow_schema::{DataType, Field, Schema, TimeUnit};
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::scalar::{BuiltinIndexType, FullTextSearchQuery};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_stream::{Stream, StreamExt};

use crate::{
    config, embed,
    rowmap::{RowMetaEntry, RowMetaMap, RowMetaSet, discover_chain},
    substrate::{
        Handle, IndexIntent, IndexParamsKind, IndexStatus, IndexTrigger, MaintenancePolicy,
        OptimizeProgressFn, PhaseOutcome, Predicate, ScalarValue, ScanOpts, Table,
        TableOptimizeOutcome, TableSizes, VECTOR_INDEX_ACTIVATION_ROWS,
    },
    wire::{FileData, Message, Part, PartKind, Role, SUMMARY_PART_TYPES, Session, SessionFrom},
};
use url::Url;

#[derive(Debug)]
pub struct Store {
    handle: Handle,
    /// Resident per-message meta map for index-only hit resolution and in-memory
    /// hydration (see [`crate::rowmap`]). `None` until [`Store::ensure_rowmap`]
    /// builds it (local tests, pre-prewarm), where the arms fall back to a
    /// data-projection scan and hydration to `take_rows`. `ArcSwap` so a
    /// version-bump rebuild swaps it under concurrent searches.
    rowmap: ArcSwapOption<RowMetaSet>,
    /// Resident embedder for inline embed-at-ingest. `None` keeps ingest
    /// writing null vectors (tests, search-only stores); the CLI write paths
    /// attach one via [`Store::with_embedder`]. Lazy, so a store that never
    /// ingests an embeddable row never loads the model.
    embedder: Option<Arc<crate::embed::LazyEmbedder>>,
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

/// One table's slice of a store-to-store copy plan: which sessions' rows for
/// that table can be **appended** versus **merged** (spec.md#session-durable-copy).
/// The choice is made per table by row presence on the destination, because the
/// three tables are written by separate commits and an interrupted copy can
/// leave them in different states (e.g. the small `sessions` table committed but
/// `messages` not):
/// - `append`: the destination has **zero** rows for the session in this table,
///   so they cannot collide -> append (no merge join, no target probe; the
///   bandwidth-bound fast path).
/// - `merge`: the destination already has *some* rows but the source has more ->
///   append only the rows still absent (filtered against the destination's PKs).
#[derive(Debug, Clone, Default)]
pub struct TablePlan {
    pub append: Vec<String>,
    pub merge: Vec<String>,
}

impl TablePlan {
    pub fn is_empty(&self) -> bool {
        self.append.is_empty() && self.merge.is_empty()
    }
}

/// A store-to-store `pond copy` plan, decided per table (see [`TablePlan`]).
/// `source_sessions` is the full source session count, kept so the caller can
/// tell "destination already up to date" (empty plan, non-empty source) from
/// "empty source", and so each table can recognize a from-empty/resumed run
/// (`append.len() == source_sessions`) and skip the per-session `IN` filter.
#[derive(Debug, Clone, Default)]
pub struct DeltaPlan {
    pub sessions: TablePlan,
    pub messages: TablePlan,
    pub parts: TablePlan,
    pub source_sessions: usize,
}

impl DeltaPlan {
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty() && self.messages.is_empty() && self.parts.is_empty()
    }

    /// Sessions whose own row is absent on the destination - the "new" count for
    /// the plan receipt. Sessions never grow in row count (one immutable row
    /// each), so the `sessions` table only ever appends.
    pub fn new_sessions(&self) -> usize {
        self.sessions.append.len()
    }

    /// Distinct sessions touched by the copy across all three tables - the
    /// figure the progress bar totals against.
    pub fn total(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        for plan in [&self.sessions, &self.messages, &self.parts] {
            seen.extend(plan.append.iter());
            seen.extend(plan.merge.iter());
        }
        seen.len()
    }
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

/// One embedded message: a primary key and the vector to store. `pond optimize`
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

/// One retrieval-arm hit. `rowid` is `Some` when the row meta map (or its
/// take_rows miss-fallback) resolved a stable row id, which lets hydration
/// `take_rows` the exact row instead of re-finding it with an `IN`-predicate
/// scan; `None` on the no-map fallback path (local tests, pre-prewarm).
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub rowid: Option<u64>,
    pub key: MessageKey,
    pub score: f32,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowTotals {
    pub sessions: u64,
    pub messages: u64,
    pub parts: u64,
}

/// Embedding coverage for `pond status` / `pond optimize`. `total` is the count of
/// `messages` rows that carry `search_text` (i.e. are eligible to embed); rows
/// without `search_text` produce no vector. `embedded` is the subset of those
/// already carrying a vector under the current [`embed::model_id()`]. `backlog`
/// is the authoritative count still owed an embedding (`total - embedded` by
/// construction), read live from the dataset rather than derived by subtracting
/// the FTS `num_docs`, which over-counts deleted-but-unpurged docs and would
/// otherwise report a phantom backlog that never clears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingProgress {
    pub embedded: usize,
    pub total: usize,
    pub backlog: usize,
    pub model: &'static str,
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
            rowmap: ArcSwapOption::empty(),
            embedder: None,
        })
    }

    /// Attach a resident embedder so [`Store::upsert_session_batch`] embeds
    /// eligible messages inline, in the same append commit as the rows
    /// (spec.md#session-embed-from-canonical). The CLI write paths set this;
    /// every other open leaves it `None` and writes null vectors as before.
    #[must_use]
    pub fn with_embedder(mut self, embedder: Arc<crate::embed::LazyEmbedder>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Live byte size of the shared Lance session caches (index + metadata).
    /// Diagnostic only - walks the caches.
    pub fn lance_cache_bytes(&self) -> u64 {
        self.handle.lance_cache_bytes()
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
            rowmap: ArcSwapOption::empty(),
            embedder: None,
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
        self.merge_scanner(table, dataset.scan(), "archive import")
            .await
    }

    /// Stream a prepared source `scanner` into this store's `table` via
    /// `merge_insert_stats`, materializing blob bytes (not descriptor structs)
    /// so the merge writes them into the destination's own schema. Shared by
    /// the archive-restore and store-to-store copy paths; `context` names the
    /// caller in error messages. Returns (rows scanned, rows inserted).
    async fn merge_scanner(
        &self,
        table: Table,
        mut scanner: lance::dataset::scanner::Scanner,
        context: &'static str,
    ) -> Result<(usize, usize)> {
        scanner.blob_handling(lance::datatypes::BlobHandling::AllBinary);
        let mut stream = scanner
            .try_into_stream()
            .await
            .with_context(|| format!("failed to scan {} for {context}", table.as_str()))?;
        let mut rows = 0usize;
        let mut inserted = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch
                .with_context(|| format!("failed to read {} {context} batch", table.as_str()))?;
            let row_count = batch.num_rows();
            rows += row_count;
            let stats = self
                .handle
                .merge_insert_stats(table, batch, row_count)
                .await
                .with_context(|| format!("failed to merge {} during {context}", table.as_str()))?;
            inserted += (stats.num_inserted_rows + stats.num_updated_rows) as usize;
        }
        Ok((rows, inserted))
    }

    /// Per-session message count - the data-intrinsic freshness key for
    /// incremental `pond copy`. pond is append-only (merge is
    /// `WhenMatched::DoNothing`; no edits or deletes), so this count rises iff a
    /// session gained messages, catching growth a `MAX(timestamp)` key would
    /// miss when a new message shares the session's latest timestamp. The count
    /// is source-authored and survives the copy unchanged, so it compares
    /// soundly across two stores with independent clocks
    /// (spec.md#session-durable-copy). Projects only
    /// the one column it counts; resolves the `session_id` array once per batch
    /// and allocates a key only on a session's first row. Distinct from
    /// `session_message_counts`, which counts a supplied id list one query each;
    /// this counts every session in a single scan.
    pub async fn all_session_message_counts(&self) -> Result<HashMap<String, usize>> {
        self.all_session_row_counts(Table::Messages).await
    }

    pub async fn all_session_part_counts(&self) -> Result<HashMap<String, usize>> {
        self.all_session_row_counts(Table::Parts).await
    }

    /// Count rows per `session_id` across one table in a single scan, projecting
    /// only the `session_id` column and allocating a key on a session's first
    /// row. Both `messages` and `parts` lead their primary key with `session_id`
    /// (`lance-table-creation-session-scoped-pk`).
    async fn all_session_row_counts(&self, table: Table) -> Result<HashMap<String, usize>> {
        let scanner = self
            .handle
            .scan(table, ScanOpts::project_only(&["session_id"]))
            .await?;
        let mut stream = scanner.try_into_stream().await?;
        let mut out: HashMap<String, usize> = HashMap::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let session_ids = batch
                .column_by_name("session_id")
                .context("scan projection dropped the session_id column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("session_id column is not Utf8")?;
            for row in 0..batch.num_rows() {
                if session_ids.is_null(row) {
                    continue;
                }
                let session_id = session_ids.value(row);
                if let Some(count) = out.get_mut(session_id) {
                    *count += 1;
                } else {
                    out.insert(session_id.to_owned(), 1);
                }
            }
        }
        Ok(out)
    }

    /// Plan an incremental store-to-store copy into `self` from `source`,
    /// deciding **per table** whether each source session's rows can be appended
    /// (the destination has none, so they cannot collide) or must be merged (the
    /// destination has some, source has more). Reads both id-sets plus
    /// per-session message and part counts. Parts have their own data-derived
    /// signal so a part added under an existing message routes through merge
    /// instead of relying on the closing verify to catch it
    /// (spec.md#session-movement-complete).
    pub async fn plan_incremental_from(&self, source: &Store) -> Result<DeltaPlan> {
        let (
            source_ids,
            dest_ids,
            source_msg_counts,
            dest_msg_counts,
            source_part_counts,
            dest_part_counts,
        ) = tokio::try_join!(
            source.collect_ids(Table::Sessions),
            self.collect_ids(Table::Sessions),
            source.all_session_message_counts(),
            self.all_session_message_counts(),
            source.all_session_part_counts(),
            self.all_session_part_counts(),
        )?;
        let source_sessions = source_ids.len();
        let mut plan = DeltaPlan {
            source_sessions,
            ..DeltaPlan::default()
        };
        for id in &source_ids {
            // The `sessions` table holds one immutable row per session, so it
            // only ever appends an absent id - a present row is identical.
            if !dest_ids.contains(id) {
                plan.sessions.append.push(id.clone());
            }
            let source_msgs = source_msg_counts.get(id).copied().unwrap_or(0);
            let dest_msgs = dest_msg_counts.get(id).copied().unwrap_or(0);
            if dest_msgs == 0 {
                if source_msgs > 0 {
                    plan.messages.append.push(id.clone());
                }
            } else if source_msgs > dest_msgs {
                plan.messages.merge.push(id.clone());
            }
            let source_parts = source_part_counts.get(id).copied().unwrap_or(0);
            let dest_parts = dest_part_counts.get(id).copied().unwrap_or(0);
            if dest_parts == 0 {
                if source_parts > 0 {
                    plan.parts.append.push(id.clone());
                }
            } else if source_parts > dest_parts {
                plan.parts.merge.push(id.clone());
            }
        }
        Ok(plan)
    }

    /// Copy the planned delta from `source` into `self`, streaming the source
    /// scan straight into the destination - no local staging copy. Each table
    /// picks its primitive per session from its [`TablePlan`]
    /// (spec.md#session-durable-copy): **append** the sessions whose rows are
    /// absent here (cannot collide; one commit per scan, bandwidth-bound), then
    /// for the grown ones append just their absent rows after filtering out the
    /// ones already present. Append-only storage is what makes the append safe: a
    /// re-run re-plans from current destination state, so an
    /// interrupted-then-resumed copy never double-appends (landed rows are no
    /// longer absent).
    pub async fn copy_delta_from(
        &self,
        source: &Store,
        plan: &DeltaPlan,
    ) -> Result<LanceArchiveImport> {
        // The three tables are independent Lance datasets with separate write
        // locks, so copy them concurrently - mirrors the ingest path's
        // three-table `try_join!` (see `upsert_session_batch`).
        let ((sessions, sessions_inserted), (messages, messages_inserted), (parts, parts_inserted)) =
            tokio::try_join!(
                self.copy_table(
                    source,
                    Table::Sessions,
                    "id",
                    &plan.sessions,
                    plan.source_sessions,
                ),
                self.copy_table(
                    source,
                    Table::Messages,
                    "session_id",
                    &plan.messages,
                    plan.source_sessions,
                ),
                self.copy_table(
                    source,
                    Table::Parts,
                    "session_id",
                    &plan.parts,
                    plan.source_sessions,
                ),
            )?;
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

    /// Copy one table's slice of the plan: append the absent sessions, then
    /// append the grown sessions' absent rows. Sequential within a table (one
    /// write lock); `copy_delta_from` runs the three tables in parallel. Returns
    /// (rows, inserted), equal since both paths append and neither dedups.
    async fn copy_table(
        &self,
        source: &Store,
        table: Table,
        key_column: &'static str,
        table_plan: &TablePlan,
        source_sessions: usize,
    ) -> Result<(usize, usize)> {
        // Force the destination table into existence up front so a lazily
        // created table (parts) is never left missing when its slice is empty -
        // same reason as the archive import path.
        let _ = self.handle.dataset(table).await?;

        let appended = self
            .append_sessions(
                source,
                table,
                key_column,
                &table_plan.append,
                source_sessions,
            )
            .await?;

        // `Sessions` never reaches here: its row is immutable, so a present
        // session is identical and routes to neither append nor merge.
        let mut grown = 0usize;
        for chunk in table_plan.merge.chunks(COPY_SESSION_IN_CHUNK) {
            let values: Vec<ScalarValue> = chunk
                .iter()
                .map(|id| ScalarValue::String(id.clone()))
                .collect();
            let predicate = Predicate::In("session_id", values.clone());
            let stream = Self::source_scan(source, table, Some(&predicate)).await?;
            grown += match table {
                Table::Messages => {
                    let present = Arc::new(self.present_message_pks(&values).await?);
                    self.append_filtered(table, stream, Self::message_keep(present))
                        .await?
                }
                Table::Parts => {
                    let present = Arc::new(self.present_part_pks(&values).await?);
                    self.append_filtered(table, stream, Self::part_keep(present))
                        .await?
                }
                Table::Sessions => 0,
            };
        }
        Ok((appended + grown, appended + grown))
    }

    /// Append one table's slice for the listed sessions. A from-empty or resumed
    /// copy (`session_ids.len() == source_sessions`: every session's rows for
    /// this table are absent on the destination) scans the source wholesale
    /// under one commit; a partial copy chunks the `IN` predicate (btree-pushed)
    /// but still commits once per chunk, not per scan batch. Returns rows
    /// appended.
    async fn append_sessions(
        &self,
        source: &Store,
        table: Table,
        key_column: &'static str,
        session_ids: &[String],
        source_sessions: usize,
    ) -> Result<usize> {
        if session_ids.is_empty() {
            return Ok(0);
        }
        if session_ids.len() == source_sessions {
            return self.append_scanner(source, table, None).await;
        }
        let mut rows = 0usize;
        for chunk in session_ids.chunks(COPY_SESSION_IN_CHUNK) {
            let predicate = in_predicate(key_column, chunk);
            rows += self.append_scanner(source, table, Some(&predicate)).await?;
        }
        Ok(rows)
    }

    /// Append a prepared source scan into this store's `table` via
    /// `Handle::append_stream`, materializing blob bytes (`AllBinary`) so the
    /// write is self-contained. The closure is a *factory*: `append_stream`
    /// rebuilds the one-shot scan stream on each OCC attempt. Returns rows
    /// appended.
    async fn append_scanner(
        &self,
        source: &Store,
        table: Table,
        predicate: Option<&Predicate>,
    ) -> Result<usize> {
        let make_source = || Self::source_scan(source, table, predicate);
        let stats = self.handle.append_stream(table, make_source).await?;
        Ok(stats.rows as usize)
    }

    /// Append source rows whose `filter_column` is in `values`. Absent rows
    /// can't collide, so append is safe where the count-based plan would merge
    /// (spec.md#session-durable-copy). Drives the `copy_bench` append-vs-merge
    /// regression guard.
    pub async fn append_absent_rows(
        &self,
        source: &Store,
        table: Table,
        filter_column: &'static str,
        values: &[String],
    ) -> Result<usize> {
        if values.is_empty() {
            return Ok(0);
        }
        let _ = self.handle.dataset(table).await?;
        let mut rows = 0usize;
        for chunk in values.chunks(COPY_SESSION_IN_CHUNK) {
            let predicate = in_predicate(filter_column, chunk);
            rows += self.append_scanner(source, table, Some(&predicate)).await?;
        }
        Ok(rows)
    }

    /// Stored message PKs for the given sessions. On the full `(session_id, id)`
    /// PK, not `id` alone: a message id is unique only within its session.
    async fn present_message_pks(
        &self,
        session_id_values: &[ScalarValue],
    ) -> Result<HashSet<(String, String)>> {
        if session_id_values.is_empty() {
            return Ok(HashSet::new());
        }
        let batch = self
            .handle
            .scan_batch(
                Table::Messages,
                Some(&Predicate::In("session_id", session_id_values.to_vec())),
                pk_columns(Table::Messages),
            )
            .await?;
        let mut set = HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let sid = string(&batch, "session_id", row)?.context("session_id is null")?;
            let mid = string(&batch, "id", row)?.context("message id is null")?;
            set.insert((sid, mid));
        }
        Ok(set)
    }

    /// Stored part PKs for the given sessions, on the full
    /// `(session_id, message_id, id)` PK (see [`Self::present_message_pks`]).
    async fn present_part_pks(
        &self,
        session_id_values: &[ScalarValue],
    ) -> Result<HashSet<(String, String, String)>> {
        if session_id_values.is_empty() {
            return Ok(HashSet::new());
        }
        let batch = self
            .handle
            .scan_batch(
                Table::Parts,
                Some(&Predicate::In("session_id", session_id_values.to_vec())),
                pk_columns(Table::Parts),
            )
            .await?;
        let mut set = HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let sid = string(&batch, "session_id", row)?.context("session_id is null")?;
            let mid = string(&batch, "message_id", row)?.context("message_id is null")?;
            let pid = string(&batch, "id", row)?.context("part id is null")?;
            set.insert((sid, mid, pid));
        }
        Ok(set)
    }

    /// The filter-and-append write seam, shared by ingest and grown-session
    /// copy. Collects only the kept rows (the absent delta), so it never stages
    /// a full copy. The terminal `append_batches` retries only a commit
    /// conflict (not a transient post-commit fault), so a lost-ack retry cannot
    /// re-append the same rows; an interrupted write heals on the next
    /// re-planned re-run instead (spec.md#lance-deterministic-pk).
    async fn append_filtered<S, K>(&self, table: Table, mut batches: S, keep: K) -> Result<usize>
    where
        S: Stream<Item = std::result::Result<RecordBatch, DataFusionError>> + Unpin,
        K: Fn(&RecordBatch, usize) -> Result<bool>,
    {
        let mut kept_batches: Vec<RecordBatch> = Vec::new();
        let mut kept = 0usize;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            let mut mask = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                mask.push(keep(&batch, row)?);
            }
            let selected = filter_record_batch(&batch, &BooleanArray::from(mask))?;
            if selected.num_rows() > 0 {
                kept += selected.num_rows();
                kept_batches.push(selected);
            }
        }
        self.handle.append_batches(table, kept_batches).await?;
        Ok(kept)
    }

    /// One-shot source scan with blob bytes materialized (`AllBinary`) so the
    /// appended rows are self-contained.
    async fn source_scan(
        source: &Store,
        table: Table,
        predicate: Option<&Predicate>,
    ) -> Result<SendableRecordBatchStream> {
        let mut scanner = source
            .handle
            .scan(
                table,
                ScanOpts {
                    predicate,
                    projection: None,
                },
            )
            .await?;
        scanner.blob_handling(lance::datatypes::BlobHandling::AllBinary);
        Ok(scanner
            .try_into_stream()
            .await
            .with_context(|| format!("failed to scan {} for copy", table.as_str()))?
            .into())
    }

    /// `append_filtered` keep for `messages`: a row survives iff its
    /// `(session_id, id)` PK is absent from `present`.
    fn message_keep(
        present: Arc<HashSet<(String, String)>>,
    ) -> impl Fn(&RecordBatch, usize) -> Result<bool> {
        move |batch, row| {
            let sid = string(batch, "session_id", row)?.context("session_id is null")?;
            let mid = string(batch, "id", row)?.context("message id is null")?;
            Ok(!present.contains(&(sid, mid)))
        }
    }

    /// `append_filtered` keep for `parts`, on the full
    /// `(session_id, message_id, id)` PK.
    fn part_keep(
        present: Arc<HashSet<(String, String, String)>>,
    ) -> impl Fn(&RecordBatch, usize) -> Result<bool> {
        move |batch, row| {
            let sid = string(batch, "session_id", row)?.context("session_id is null")?;
            let mid = string(batch, "message_id", row)?.context("message_id is null")?;
            let pid = string(batch, "id", row)?.context("part id is null")?;
            Ok(!present.contains(&(sid, mid, pid)))
        }
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

    /// Embeddings aligned to `rows` so they ride the rows' birth append. A row
    /// is embedded iff it has `search_text` and is absent from `present` (an
    /// idempotent re-sync re-embeds nothing the append would drop); otherwise,
    /// and whenever no embedder is attached, the slot is `None` and the batch
    /// writes a null vector for `pond optimize` to fill later.
    async fn embed_message_rows(
        &self,
        rows: &[MessageBatchRow<'_>],
        present: &HashSet<(String, String)>,
    ) -> Result<Vec<Option<Vec<f32>>>> {
        let mut out = vec![None; rows.len()];
        let Some(embedder) = &self.embedder else {
            return Ok(out);
        };
        let targets: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.search_text.is_some()
                    && !present.contains(&(
                        row.message.session_id().to_owned(),
                        row.message.id().to_owned(),
                    ))
            })
            .map(|(index, _)| index)
            .collect();
        if targets.is_empty() {
            return Ok(out);
        }
        let backend = embedder.get().await?;
        let texts: Vec<&str> = targets
            .iter()
            .map(|&index| rows[index].search_text.unwrap_or_default())
            .collect();
        let vectors = crate::embed::embed_passages(
            backend.as_ref(),
            &texts,
            crate::embed::DEFAULT_BATCH_SIZE,
            |_| {},
        )?;
        for (&index, vector) in targets.iter().zip(vectors) {
            out[index] = Some(vector);
        }
        Ok(out)
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
    ///   4. Commits messages + parts first, then sessions. The session row is
    ///      the freshness-bearing row; writing it last makes a partial
    ///      non-atomic flush re-ingest and heal (spec.md#session-movement-complete).
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
        let existing_message_pks = Arc::new(self.present_message_pks(&session_id_values).await?);
        let existing_part_pks = Arc::new(self.present_part_pks(&session_id_values).await?);

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
        // Drop only in-batch duplicates here (spec.md#adapter-integrity-dedup);
        // `append_filtered` drops the rows already present on the destination.
        let mut seen_messages: HashSet<(String, String)> = HashSet::new();
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
            .filter(|row| {
                seen_messages.insert((
                    row.message.session_id().to_owned(),
                    row.message.id().to_owned(),
                ))
            })
            .collect();
        let mut seen_parts: HashSet<(String, String, String)> = HashSet::new();
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
            .filter(|part| {
                seen_parts.insert((
                    part.session_id.clone(),
                    part.message_id.clone(),
                    part.id.clone(),
                ))
            })
            .collect();

        // Embed before the append so the vector rides the message rows' birth
        // commit (spec.md#session-durable-copy: one append, no extra commit).
        let message_vectors = self
            .embed_message_rows(&message_rows, &existing_message_pks)
            .await?;

        let session_batches = sessions_batches(&sessions_owned)?;
        let message_stream = tokio_stream::iter(
            messages_batches(&message_rows, &message_vectors)?
                .into_iter()
                .map(Ok::<_, DataFusionError>),
        );
        let part_stream = tokio_stream::iter(
            parts_batches(&part_rows)?
                .into_iter()
                .map(Ok::<_, DataFusionError>),
        );
        let (_messages_appended, _parts_appended) = tokio::try_join!(
            self.append_filtered(
                Table::Messages,
                message_stream,
                Self::message_keep(existing_message_pks.clone()),
            ),
            self.append_filtered(
                Table::Parts,
                part_stream,
                Self::part_keep(existing_part_pks.clone()),
            ),
        )?;
        let _sessions_inserted =
            merge_insert_chunks(&self.handle, Table::Sessions, session_batches).await?;

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
        let batches = messages_batches(&rows, &vec![None; rows.len()])?;
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

    /// `session_id -> last durable message id` for the sync freshness gate.
    /// Scans stored message data only, never Lance version history:
    /// `Dataset::versions()` is remote-manifest-bound on object stores, and a
    /// write timestamp can exist even when a non-atomic ingest did not commit
    /// the messages (spec.md#session-movement-complete).
    ///
    /// Only emits a key when the session row is ALSO durable. `upsert_session_batch`
    /// commits messages+parts before the session row, so a partial flush can leave
    /// a session whose messages are stored but whose session row is not; keying on
    /// messages alone would report it fresh and orphan the missing row. Intersecting
    /// with the sessions id-set forces a re-ingest that heals it
    /// (spec.md#session-movement-complete).
    pub async fn session_last_message_ids(&self) -> Result<HashMap<String, String>> {
        let (session_ids, latest) = tokio::try_join!(self.collect_ids(Table::Sessions), async {
            let scanner = self
                .handle
                .scan(
                    Table::Messages,
                    ScanOpts::project_only(&["session_id", "id", "timestamp"]),
                )
                .await?;
            let mut stream = scanner.try_into_stream().await?;
            let mut latest: HashMap<String, (DateTime<Utc>, String)> = HashMap::new();
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let session_ids = batch
                    .column_by_name("session_id")
                    .context("scan projection dropped the session_id column")?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("session_id column is not Utf8")?;
                for row in 0..batch.num_rows() {
                    if session_ids.is_null(row) {
                        continue;
                    }
                    let session_id = session_ids.value(row);
                    let Some(id) = string(&batch, "id", row)? else {
                        continue;
                    };
                    let timestamp = datetime(&batch, "timestamp", row)?;
                    match latest.get_mut(session_id) {
                        Some((stored_ts, stored_id))
                            if timestamp > *stored_ts
                                || (timestamp == *stored_ts
                                    && id.as_str() > stored_id.as_str()) =>
                        {
                            *stored_ts = timestamp;
                            *stored_id = id;
                        }
                        None => {
                            latest.insert(session_id.to_owned(), (timestamp, id));
                        }
                        _ => {}
                    }
                }
            }
            Ok::<_, anyhow::Error>(latest)
        })?;
        Ok(latest
            .into_iter()
            .filter(|(session_id, _)| session_ids.contains(session_id))
            .map(|(session_id, (_, message_id))| (session_id, message_id))
            .collect())
    }

    /// Whole-session view for `pond_get` session scope (spec.md#protocol).
    /// Always the conversational view (`search_text IS NOT NULL`) with one-line
    /// part summaries - full part bodies are reached by `message_id` scope, not
    /// here. The page is the window selected by the anchors (`after_message_id`
    /// pages forward, `before_message_id` pages backward) or, with neither,
    /// `session_from` (start/end); it is bounded by `limit` and a byte budget,
    /// never cutting mid-message. `before_remaining`/`after_remaining` drive the
    /// bidirectional page markers.
    pub async fn session_view(
        &self,
        session_id: &str,
        params: SessionViewParams<'_>,
    ) -> Result<GetLookup<SessionPage>> {
        let Some(session) = self.find_session(session_id).await? else {
            return Ok(GetLookup::NotFound);
        };
        let mut rows: Vec<ScanRow> = self
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
            .collect();
        rows.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));

        let size = |row: &ScanRow| row.text.as_deref().map_or(0, str::len);
        let total = rows.len();
        // Append-only stream: a real anchor never vanishes, so an unknown
        // anchor is a stale/mistyped client cursor, not "start over".
        let (win_start, win_end) = match (params.after_message_id, params.before_message_id) {
            (Some(after), _) => {
                let pos = match rows.iter().position(|row| row.id == after) {
                    Some(idx) => idx + 1,
                    None => return Ok(GetLookup::UnknownAnchor),
                };
                let n = page_by(&rows[pos..], params.limit, params.budget_bytes, size);
                (pos, pos + n)
            }
            (None, Some(before)) => {
                let pos = match rows.iter().position(|row| row.id == before) {
                    Some(idx) => idx,
                    None => return Ok(GetLookup::UnknownAnchor),
                };
                let n = page_tail(&rows[..pos], params.limit, params.budget_bytes, size);
                (pos - n, pos)
            }
            (None, None) => match params.session_from {
                SessionFrom::Start => (0, page_by(&rows, params.limit, params.budget_bytes, size)),
                SessionFrom::End => {
                    let n = page_tail(&rows, params.limit, params.budget_bytes, size);
                    (total - n, total)
                }
            },
        };
        let emitted = &rows[win_start..win_end];
        let before_remaining = win_start;
        let after_remaining = total - win_end;
        let ids: Vec<String> = emitted.iter().map(|row| row.id.clone()).collect();

        let mut parts_by_message = self.summary_parts_for_messages(session_id, &ids).await?;
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
            before_remaining,
            after_remaining,
        }))
    }

    /// Message-scope retrieval for `pond_get` message scope (spec.md#protocol):
    /// the target with its full parts (budget-bounded) plus `context_before`
    /// conversational siblings before and `context_after` after it. `NotFound`
    /// when no stored message carries `message_id`. Sibling parts are carried
    /// for summarizing; the target's parts ride `target_parts`.
    pub async fn message_view(
        &self,
        message_id: &str,
        params: MessageViewParams,
    ) -> Result<GetLookup<MessagePage>> {
        let Some(session_id) = self.session_id_for_message(message_id).await? else {
            return Ok(GetLookup::NotFound);
        };
        let Some(session) = self.find_session(&session_id).await? else {
            return Ok(GetLookup::NotFound);
        };
        let mut rows = self.scan_all_messages(&session_id).await?;
        // Siblings are always the conversational view: in carrier-heavy sessions
        // the system/tool rows would otherwise fill the whole window and push
        // the actual conversation out of it. The target stays regardless of its
        // own role - the caller asked for that message.
        rows.retain(|row| row.text.is_some() || row.id == message_id);
        rows.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
        let Some(target_pos) = rows.iter().position(|row| row.id == message_id) else {
            return Ok(GetLookup::NotFound);
        };

        let start = target_pos.saturating_sub(params.context_before);
        let end = (target_pos + params.context_after + 1).min(rows.len());
        let window = &rows[start..end];
        let window_ids: Vec<String> = window.iter().map(|row| row.id.clone()).collect();
        // The target's full parts (blobs included) ride the response; siblings
        // are only summarized, but they share this one window scan.
        let mut parts_by_message = self.parts_for_messages(&session_id, &window_ids).await?;

        let all_parts = parts_by_message
            .remove(&(session_id.clone(), message_id.to_owned()))
            .unwrap_or_default();
        // Target parts are budget-bounded (no per-part pagination cursor). The
        // 1000 cap is the page_by hard ceiling; the budget is the real bound.
        let part_count = page_by(&all_parts, 1000, params.budget_bytes, |part| {
            serde_json::to_string(part).map_or(0, |json| json.len())
        });
        let target_parts = all_parts[..part_count].to_vec();
        let target_parts_remaining = all_parts.len() - part_count;

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

    /// The primary-key (`id`) set for `table`. Powers storage verification
    /// (`pond copy --verify-only` and copy's closing check).
    pub async fn collect_ids(&self, table: Table) -> Result<std::collections::HashSet<String>> {
        self.handle.collect_ids(table).await
    }

    /// This store's set of composite primary keys for `table`, plus the row
    /// count. `rows - keys.len()` is the duplicate count - zero is the invariant
    /// (the append path has no row-level dedup, so a non-zero count is a write
    /// anomaly the copy verify reports rather than calling "synced"), and the
    /// key set drives the verify's completeness membership. One scan over only
    /// the PK columns yields both, holding a single composite-PK set per table.
    pub async fn composite_pk_index(&self, table: Table) -> Result<(HashSet<Vec<String>>, usize)> {
        let pk = pk_columns(table);
        let scanner = self.handle.scan(table, ScanOpts::project_only(pk)).await?;
        let mut stream = scanner.try_into_stream().await?;
        let mut keys: HashSet<Vec<String>> = HashSet::new();
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                rows += 1;
                keys.insert(composite_key(&batch, pk, row)?);
            }
        }
        Ok((keys, rows))
    }

    /// Stream `table`'s composite primary keys and return `(rows_scanned, rows
    /// whose key is absent from `present`)`. Composite-keyed, not bare `id`: a
    /// message id replayed into a new session by a fork/compaction is matched
    /// per session, so a wholly-absent replayed session whose ids collide with
    /// present ones is counted missing - a bare-`id` check would false-negative
    /// it as "present". Streams the scanned side, holding only `present`.
    pub async fn composite_pk_diff_against(
        &self,
        table: Table,
        present: &HashSet<Vec<String>>,
    ) -> Result<(usize, usize)> {
        let pk = pk_columns(table);
        let scanner = self.handle.scan(table, ScanOpts::project_only(pk)).await?;
        let mut stream = scanner.try_into_stream().await?;
        let (mut rows, mut absent) = (0usize, 0usize);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                rows += 1;
                if !present.contains(&composite_key(&batch, pk, row)?) {
                    absent += 1;
                }
            }
        }
        Ok((rows, absent))
    }

    /// A point-in-time `Arc<Dataset>` for `table`, for registering as a
    /// DataFusion `LanceTableProvider` in `pond_sql_query`. Goes through the
    /// handle's freshness gate, so each query sees a current snapshot.
    pub async fn dataset(&self, table: Table) -> Result<Arc<Dataset>> {
        Ok(Arc::new(self.handle.dataset(table).await?))
    }

    /// Page the heavy search indices in from storage so the first user query
    /// after process start never eats the 175-442 s cold S3 load
    /// (spec.md#search). Vector via `prewarm_index` (loads the IVF_SQ partition
    /// storage). FTS is warmed with one synthetic query: Lance's full FTS
    /// `prewarm_index` loads *every* token's posting list, which resident-sets
    /// the whole inverted index (~1.7 GiB on the 2M-row corpus) and blows the
    /// server RAM budget - so we settle the term dictionary + a hot token
    /// instead and let real queries page their own postings. Best effort: a
    /// missing index (IVF_SQ below activation, or no FTS yet on an empty store)
    /// is logged, not fatal.
    pub async fn prewarm(&self, cache_dir: &Path) -> Result<()> {
        let messages = self.dataset(Table::Messages).await?;
        if let Err(error) = messages.prewarm_index(MESSAGES_VECTOR_INDEX).await {
            tracing::debug!(%error, "vector index prewarm skipped");
        }
        // Best-effort: on failure `rowmap` stays empty and the arms fall back to
        // the data-take path, so search still works (slower on a remote store).
        if let Err(error) = self.ensure_rowmap(cache_dir).await {
            tracing::warn!(%error, "rowmap build skipped; arms fall back to data-take resolution");
        }
        // Warm the FTS posting lists; the rowmap build above touched only the
        // data columns.
        if let Err(error) = self
            .fts_search("pond", 1, &Predicate::And(Vec::new()))
            .await
        {
            tracing::debug!(%error, "fts index prewarm skipped");
        }
        Ok(())
    }

    /// Stable filesystem-safe cache key: same store URL -> same key, so sibling
    /// pond processes share one map file and distinct stores never collide.
    fn store_key(&self) -> String {
        let digest = blake3::hash(self.handle.location().as_str().as_bytes());
        digest.to_hex()[..16].to_owned()
    }

    /// Max delta segments before the chain is compacted into a fresh base.
    const MAX_ROWMAP_DELTAS: usize = 16;

    /// Columns the resident meta map is built from. The full scan and the delta
    /// scan MUST project the same set in the same order - both feed
    /// [`row_meta_entry`], so a column added to one only would silently corrupt
    /// delta hydration.
    const ROW_META_COLUMNS: [&str; 7] = [
        "session_id",
        "id",
        "role",
        "project",
        "source_agent",
        "timestamp",
        "search_text",
    ];

    /// Install the resident meta map covering the current `messages` version.
    /// Idempotent - a chain already at that version is kept. On a version bump
    /// it layers a delta segment (scanning only the new fragments), compacts the
    /// deltas locally once they pile up, and full-rebuilds the base only on a
    /// store compaction - all under a build `flock` so N local processes don't
    /// rescan the store at once.
    pub async fn ensure_rowmap(&self, cache_dir: &Path) -> Result<()> {
        let version = self.messages_version().await?;
        if let Some(current) = self.rowmap.load_full()
            && current.version() == version
        {
            return Ok(());
        }
        std::fs::create_dir_all(cache_dir)
            .with_context(|| format!("create cache dir {}", cache_dir.display()))?;
        let store_key = self.store_key();

        // A sibling may already have published a chain at this version; install
        // it without rebuilding.
        if let Some(chain) = discover_chain(cache_dir, &store_key)
            && chain.version() == version
            && let Ok(set) = RowMetaSet::open(&chain)
        {
            self.rowmap.store(Some(Arc::new(set)));
            Self::sweep_stale_rowmaps(cache_dir, &store_key, chain.base_version);
            return Ok(());
        }
        if let Some(set) = self
            .extend_rowmap_coordinated(cache_dir, &store_key, version)
            .await?
        {
            self.rowmap.store(Some(Arc::new(set)));
        }
        Ok(())
    }

    /// Extend the chain to `version` under the build `flock` (spec: lock the
    /// build only; atomic rename already prevents corruption). `None` when
    /// another local process holds the lock - this caller keeps its current map
    /// (or the take_rows fallback) until a later refresh opens what the winner
    /// published.
    async fn extend_rowmap_coordinated(
        &self,
        cache_dir: &Path,
        store_key: &str,
        version: u64,
    ) -> Result<Option<RowMetaSet>> {
        let lock_path = cache_dir.join(format!("rowmetamap-{store_key}.lock"));
        let lock = std::fs::File::create(&lock_path)
            .with_context(|| format!("create rowmap build lock {}", lock_path.display()))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(error).context("lock rowmap build");
            }
        }

        // Re-check after acquiring: a sibling may have published `version`. An
        // open failure here (older MAGIC after an upgrade, or corruption) falls
        // through to the purge+rebuild below rather than erroring.
        if let Some(chain) = discover_chain(cache_dir, store_key)
            && chain.version() == version
            && let Ok(set) = RowMetaSet::open(&chain)
        {
            return Ok(Some(set));
        }

        // Holding the lock makes us the only builder, so every build temp is a
        // dead orphan from a crashed build - clear them before writing ours.
        Self::sweep_orphan_temps(cache_dir, store_key);

        // Validate any existing chain opens; an unreadable segment (an older
        // MAGIC after a pond upgrade, or a corrupt file) is purged so the build
        // below is a clean full rebuild instead of erroring forever or appending
        // a fresh delta onto an unreadable base. The opened set also feeds the
        // delta its high-water mark and row count (cheap mmap reads).
        let chain = discover_chain(cache_dir, store_key);
        let existing = match &chain {
            Some(paths) => match RowMetaSet::open(paths) {
                Ok(set) => Some((paths, set)),
                Err(error) => {
                    tracing::warn!(%error, store = store_key, "rowmap unreadable; purging and rebuilding");
                    Self::purge_rowmaps(cache_dir, store_key);
                    None
                }
            },
            None => None,
        };
        // A row-id-keyed append delta (None on a reclaimed base or net deletion)
        // decides the path.
        let delta = match &existing {
            Some((_, set)) => {
                self.collect_row_metas_delta(
                    set.version(),
                    set.max_row_id().unwrap_or(0),
                    set.len(),
                )
                .await?
            }
            None => None,
        };

        let base_version = match (&existing, delta) {
            // Append with room: layer a new delta segment.
            (Some((paths, _)), Some(entries)) if paths.deltas.len() < Self::MAX_ROWMAP_DELTAS => {
                let path = RowMetaMap::delta_path(cache_dir, store_key, version);
                RowMetaMap::build(&path, version, entries)?;
                paths.base_version
            }
            // Append but the deltas are full: compact the existing segments
            // (read locally from their mmaps) plus this delta into a fresh base -
            // no full store re-read.
            (Some((_, set)), Some(entries)) => {
                let mut merged = set.merged_entries();
                merged.extend(entries);
                let path = RowMetaMap::path_for(cache_dir, store_key, version);
                RowMetaMap::build(&path, version, merged)?;
                version
            }
            // No chain, or a reclaimed base / deletion since it: full scan -> base.
            _ => {
                let entries = self.collect_row_metas().await?;
                let path = RowMetaMap::path_for(cache_dir, store_key, version);
                RowMetaMap::build(&path, version, entries)?;
                version
            }
        };

        let chain =
            discover_chain(cache_dir, store_key).context("rowmap chain missing after build")?;
        let set = RowMetaSet::open(&chain)?;
        Self::sweep_stale_rowmaps(cache_dir, store_key, base_version);
        Ok(Some(set))
    }

    /// Remove this store's segment files (`-v{V}` bases, `-d{V}` deltas) for
    /// versions strictly older than `keep` (best-effort). A newer file belongs
    /// to a sibling that advanced past us; unlinking a superseded file is safe
    /// even if a sibling has it mapped - Unix keeps the inode alive until unmap.
    fn sweep_stale_rowmaps(cache_dir: &Path, store_key: &str, keep: u64) {
        let prefix = format!("rowmetamap-{store_key}-");
        let Ok(entries) = std::fs::read_dir(cache_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(rest) = name
                .to_str()
                .and_then(|name| name.strip_prefix(&prefix))
                .and_then(|rest| rest.strip_suffix(".rmm"))
            else {
                continue;
            };
            let version = rest
                .strip_prefix('v')
                .or_else(|| rest.strip_prefix('d'))
                .and_then(|digits| digits.parse::<u64>().ok());
            if let Some(version) = version
                && version < keep
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    /// Remove every segment file (`-v{V}` / `-d{V}`) for this store regardless of
    /// version - used when a discovered chain is unreadable (older MAGIC after an
    /// upgrade, or corruption) so the next build starts clean. Sound under the
    /// build lock; POSIX keeps any inode a sibling still has mapped alive.
    fn purge_rowmaps(cache_dir: &Path, store_key: &str) {
        let prefix = format!("rowmetamap-{store_key}-");
        let Ok(entries) = std::fs::read_dir(cache_dir) else {
            return;
        };
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && name.starts_with(&prefix)
                && name.ends_with(".rmm")
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    /// Remove abandoned build temp files (`*.tmp-*`) for this store. Best-effort,
    /// and only sound under the build lock - the holder is the sole builder, so
    /// any temp present is a crashed-build orphan, not a live write.
    fn sweep_orphan_temps(cache_dir: &Path, store_key: &str) {
        let prefix = format!("rowmetamap-{store_key}-");
        let Ok(entries) = std::fs::read_dir(cache_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with(&prefix) && name.contains(".tmp-") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn rowmap_delta_count(&self) -> Option<usize> {
        self.rowmap.load_full().map(|set| set.delta_count())
    }

    /// The currently-installed resident meta map, if any. `pond sync` reads it
    /// (via [`RowmapOracle`]) as the freshness oracle (max timestamp per
    /// session); `None` falls back to re-reading every source.
    pub fn rowmap_snapshot(&self) -> Option<Arc<RowMetaSet>> {
        self.rowmap.load_full()
    }

    /// Resolve index-only `(row_id, score)` hits to keys via the map; row ids the
    /// map lacks (appended since build) fall back to one `take_rows` batch. The
    /// caller re-sorts, so the misses appended at the end carry no order meaning.
    async fn resolve_rowid_hits(
        &self,
        map: &RowMetaSet,
        hits: Vec<(u64, f32)>,
    ) -> Result<Vec<SearchHit>> {
        let mut resolved = Vec::with_capacity(hits.len());
        let mut misses: Vec<(u64, f32)> = Vec::new();
        for (rowid, score) in hits {
            match map.lookup(rowid) {
                Some((session_id, message_id)) => resolved.push(SearchHit {
                    rowid: Some(rowid),
                    key: MessageKey {
                        session_id: session_id.to_owned(),
                        message_id: message_id.to_owned(),
                    },
                    score,
                }),
                None => misses.push((rowid, score)),
            }
        }
        // A miss still knows its rowid; carry it so hydration can take_rows it
        // alongside the hits the map resolved.
        if !misses.is_empty() {
            let rowids: Vec<u64> = misses.iter().map(|(rowid, _)| *rowid).collect();
            let keys = self.message_keys_by_rowids(&rowids).await?;
            for ((rowid, score), key) in misses.into_iter().zip(keys) {
                resolved.push(SearchHit {
                    rowid: Some(rowid),
                    key,
                    score,
                });
            }
        }
        Ok(resolved)
    }

    /// Resolve stable row ids to `(session_id, id)` via `take_rows`, which
    /// returns rows in `rowids` order - the caller's `zip` relies on that.
    async fn message_keys_by_rowids(&self, rowids: &[u64]) -> Result<Vec<MessageKey>> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        let projection = ProjectionRequest::from_columns(["session_id", "id"], dataset.schema());
        let batch = dataset.take_rows(rowids, projection).await?;
        let mut keys = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            keys.push(MessageKey {
                session_id: string(&batch, "session_id", row)?.context("session_id is null")?,
                message_id: string(&batch, "id", row)?.context("fts hit id is null")?,
            });
        }
        Ok(keys)
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

    /// Distinct adapter names present in the corpus, sorted. Scans only the
    /// `source_agent` column of the small `sessions` table, so `pond status`
    /// gets its adapter count without touching the 2M-row `messages` table.
    /// `include_subagents=false` drops `source_agent` values containing `/`
    /// (e.g. `claude-code/general-purpose`).
    pub async fn adapter_names(&self, include_subagents: bool) -> Result<Vec<String>> {
        let scanner = self
            .handle
            .scan(Table::Sessions, ScanOpts::project_only(&["source_agent"]))
            .await?;
        let mut stream = scanner.try_into_stream().await?;
        let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let agent = string(&batch, "source_agent", row)?.unwrap_or_default();
                if !include_subagents && agent.contains('/') {
                    continue;
                }
                names.insert(agent);
            }
        }
        Ok(names.into_iter().collect())
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
            // Filter on `embedding_model IS NULL`, not `vector IS NULL`: the two
            // are co-set (write_embeddings sets both, spec.md#session-embed-from-canonical),
            // but evaluating the predicate over the narrow model-id column reads
            // ~50x fewer bytes than scanning the Float16 vector column - the
            // difference between a whole-table vector decode and a cheap scan.
            let filter = Predicate::And(vec![
                Predicate::IsNull("embedding_model"),
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
    /// current model. `pond optimize --force-embed` feeds this to the same unconditional
    /// merge_update as the normal backlog; the filter makes that semantically
    /// equivalent to the conditional update in spec.md#session-embed-from-canonical.
    pub fn pending_or_stale_messages(&self) -> impl Stream<Item = Result<PendingMessage>> + '_ {
        try_stream! {
            // `embedding_model IS NULL` (co-set with `vector IS NULL`, but a ~50x
            // narrower column read) for the never-embedded rows, OR a model
            // mismatch for the stale ones - both decided off the model-id column.
            let filter = Predicate::And(vec![
                Predicate::IsNotNull("search_text"),
                Predicate::Or(vec![
                    Predicate::IsNull("embedding_model"),
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

    /// BM25 full-text retriever over `messages.search_text`. With the row meta map
    /// loaded the scan is index-only (no data columns -> no `TakeExec`, no
    /// scattered GETs) and hits resolve through the map; otherwise it falls back
    /// to `fts_search_keys` so search works before prewarm.
    pub async fn fts_search(
        &self,
        query: &str,
        limit: usize,
        filter: &Predicate,
    ) -> Result<Vec<SearchHit>> {
        let mut hits = if let Some(map) = self.rowmap.load_full() {
            let rowid_hits = self.fts_search_rowids(query, limit, filter).await?;
            self.resolve_rowid_hits(&map, rowid_hits).await?
        } else {
            self.fts_search_keys(query, limit, filter).await?
        };
        // Stable secondary sort: Lance returns tied-BM25-score hits in fragment
        // order, which varies between runs and across calls with different pool
        // sizes. Without an explicit tiebreak the downstream session grouping and
        // rank for a tied target can flip session-to-session, making results
        // nondeterministic. Sort by `score desc`, then `(session_id, message_id)` asc.
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.key.session_id.cmp(&right.key.session_id))
                .then_with(|| left.key.message_id.cmp(&right.key.message_id))
        });
        Ok(hits)
    }

    /// Shared FTS-scan setup: scope filter, the `search_text` full-text query,
    /// `fast_search` when indexed, and `limit`. Callers set only their projection.
    async fn fts_scanner(
        &self,
        query: &str,
        limit: usize,
        filter: &Predicate,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let mut scanner = self.handle.scanner(Table::Messages, Some(filter)).await?;
        scanner.full_text_search(
            FullTextSearchQuery::new(query.to_owned()).with_column("search_text".to_owned())?,
        )?;
        if self.handle.messages_has_index(MESSAGES_FTS_INDEX).await? {
            scanner.fast_search();
        }
        // Lance ships an autoprojection that silently appends `_score` to FTS
        // output when the projection omits it. That behavior is going away;
        // we opt into the future explicit-projection contract here so the
        // scanner stops emitting a per-call deprecation warning, and each caller
        // lists `_score` in its own projection.
        scanner.disable_scoring_autoprojection();
        scanner.limit(Some(i64::try_from(limit).unwrap_or(i64::MAX)), None)?;
        Ok(scanner)
    }

    /// No-map FTS fallback: project the key columns plus `_score` directly,
    /// taking the `TakeExec` cost. Unsorted; `fts_search` applies the sort.
    async fn fts_search_keys(
        &self,
        query: &str,
        limit: usize,
        filter: &Predicate,
    ) -> Result<Vec<SearchHit>> {
        let mut scanner = self.fts_scanner(query, limit, filter).await?;
        scanner.project(&["session_id", "id", "_score"])?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = MessageKey {
                session_id: string(&batch, "session_id", row)?.context("session_id is null")?,
                message_id: string(&batch, "id", row)?.context("fts hit id is null")?,
            };
            hits.push(SearchHit {
                rowid: None,
                key,
                score: float32(&batch, "_score", row)?,
            });
        }
        Ok(hits)
    }

    /// Current `messages` dataset version - the key a `RowMetaMap` is built
    /// against (pond's stable row ids keep a built map valid until this advances).
    pub async fn messages_version(&self) -> Result<u64> {
        Ok(self
            .handle
            .dataset(Table::Messages)
            .await?
            .version()
            .version)
    }

    /// Scan the hydration columns with row ids into a `Vec`, the input to
    /// `RowMetaMap::build`. One large sequential scan (few big reads), unlike the
    /// scattered per-hit take it replaces; `search_text` dominates the bytes.
    pub async fn collect_row_metas(&self) -> Result<Vec<RowMetaEntry>> {
        let mut scanner = self.handle.scanner(Table::Messages, None).await?;
        scanner.with_row_id();
        scanner.project(&Self::ROW_META_COLUMNS)?;
        let mut stream = scanner.try_into_stream().await?;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let rowids = uint64(&batch, "_rowid")?;
            for row in 0..batch.num_rows() {
                out.push(row_meta_entry(&batch, rowids.value(row), row)?);
            }
        }
        Ok(out)
    }

    /// Row metas for the rows appended since the base segment - the input to a
    /// delta layered on a base whose high-water mark is `base_max_row_id` and
    /// which covers `base_row_count` rows. `None` (caller rebuilds the base from
    /// a full scan) when the chain can't be cheaply extended:
    /// - `base_version`'s manifest was reclaimed by the cleanup retention window
    ///   (spec.md#concurrency), so the version no longer resolves; or
    /// - the live row count dropped below the base: rows were deleted, and a
    ///   pure append can't remove the base's now-stale entries.
    ///
    /// Stable row ids (`enable_stable_row_ids`) make this an append: embedding's
    /// `merge_update` and compaction rewrite message fragments but preserve
    /// row_ids and never touch a ROW_META column, so existing base entries stay
    /// valid under that churn. Only genuine appends carry `row_id >
    /// base_max_row_id`; emitting just those keeps the delta disjoint from the
    /// base, which the per-segment count sums depend on.
    async fn collect_row_metas_delta(
        &self,
        base_version: u64,
        base_max_row_id: u64,
        base_row_count: usize,
    ) -> Result<Option<Vec<RowMetaEntry>>> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        let Ok(old) = dataset.checkout_version(base_version).await else {
            return Ok(None);
        };
        if dataset.count_rows(None).await? < base_row_count {
            return Ok(None);
        }
        // Restrict the scan to fragments added since the base (recent churn -
        // not the untouched bulk). Rewritten/compacted fragments carry only
        // existing row_ids (<= base_max_row_id) and are filtered out row-wise;
        // genuine appends carry higher ids and are kept.
        let old_ids: HashSet<u64> = old.get_fragments().iter().map(|f| f.id() as u64).collect();
        let added: Vec<_> = dataset
            .get_fragments()
            .iter()
            .filter(|fragment| !old_ids.contains(&(fragment.id() as u64)))
            .map(|fragment| fragment.metadata().clone())
            .collect();
        if added.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut scanner = dataset.scan();
        scanner.with_fragments(added);
        scanner.with_row_id();
        scanner.project(&Self::ROW_META_COLUMNS)?;
        let mut stream = scanner.try_into_stream().await?;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let rowids = uint64(&batch, "_rowid")?;
            for row in 0..batch.num_rows() {
                let row_id = rowids.value(row);
                if row_id > base_max_row_id {
                    out.push(row_meta_entry(&batch, row_id, row)?);
                }
            }
        }
        Ok(Some(out))
    }

    /// Index-only FTS retriever: `_rowid` + `_score` only, so Lance inserts no
    /// `TakeExec` and issues no scattered GETs. `fts_search` resolves the row ids.
    async fn fts_search_rowids(
        &self,
        query: &str,
        limit: usize,
        filter: &Predicate,
    ) -> Result<Vec<(u64, f32)>> {
        let mut scanner = self.fts_scanner(query, limit, filter).await?;
        scanner.with_row_id();
        scanner.project(&["_score"])?;
        let batch = scanner.try_into_batch().await?;
        let rowids = uint64(&batch, "_rowid")?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            hits.push((rowids.value(row), float32(&batch, "_score", row)?));
        }
        Ok(hits)
    }

    /// Count of searchable messages (non-null `search_text`) inside the
    /// caller's filter scope - the universe a search actually ran over.
    /// Powers the response's absence honesty (spec.md#search): "no relevant
    /// hits" only means something relative to how many messages were
    /// searchable at all, and 0 tells the caller their filters excluded
    /// everything before retrieval even started.
    pub async fn searchable_in_scope(&self, filter: &Predicate) -> Result<usize> {
        // Unfiltered: the FTS index already counts non-null search_text rows
        // (`num_docs`), and fast_search only searches those indexed docs - so
        // num_docs is exactly the universe a search ran over. Reading it avoids
        // the ~133 MB `IsNotNull(search_text)` column scan Lance pays per query
        // (no per-column null metadata). Filtered scopes fall back to the scan.
        if matches!(filter, Predicate::And(clauses) if clauses.is_empty())
            && let Some(count) = self.fts_num_docs().await?
        {
            return Ok(count);
        }
        let scope = Predicate::And(vec![Predicate::IsNotNull("search_text"), filter.clone()]);
        let dataset = self.handle.dataset(Table::Messages).await?;
        let count = dataset.count_rows(Some(scope.to_lance())).await?;
        Ok(count)
    }

    /// Non-null `search_text` count read from the FTS index's `num_docs`
    /// statistic (summed across delta segments). `None` when the FTS index is
    /// absent (empty store) so the caller falls back to the count scan.
    async fn fts_num_docs(&self) -> Result<Option<usize>> {
        if !self.handle.messages_has_index(MESSAGES_FTS_INDEX).await? {
            return Ok(None);
        }
        let dataset = self.handle.dataset(Table::Messages).await?;
        let json = dataset.index_statistics(MESSAGES_FTS_INDEX).await?;
        let parsed: Value =
            serde_json::from_str(&json).context("failed to parse FTS index_statistics")?;
        let total: u64 = parsed["indices"]
            .as_array()
            .map(|segments| {
                segments
                    .iter()
                    .filter_map(|segment| segment["num_docs"].as_u64())
                    .sum()
            })
            .unwrap_or(0);
        Ok(Some(usize::try_from(total).unwrap_or(usize::MAX)))
    }

    /// Whether any `messages` row carries a vector (spec.md#search) - the
    /// signal that lets the `vector` arm run instead of degrading to `fts`. The single-active-
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

    /// One embedded row's model id, or `None` when nothing is embedded yet. A
    /// `LIMIT 1` point read: the single-active-model invariant (see
    /// `has_embeddings`) means any embedded row's model is representative, so a
    /// model swap is detectable by comparing this to the configured model -
    /// without the full-column `stale_embedding_count` scan that ran every sync.
    pub async fn sample_embedded_model(&self) -> Result<Option<String>> {
        let scope = Predicate::IsNotNull("embedding_model");
        let mut scanner = self
            .handle
            .scan(
                Table::Messages,
                ScanOpts::with_predicate_and_projection(&scope, &["embedding_model"]),
            )
            .await?;
        scanner.limit(Some(1), None)?;
        let batch = scanner.try_into_batch().await?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        string(&batch, "embedding_model", 0)
    }

    /// Vector kNN retriever over `messages.vector`, prefiltered by the caller's
    /// scalar predicate alone (spec.md#search-prefilter-pushdown) - see
    /// `embedded_scope` for why pond does NOT add `vector IS NOT NULL`. nprobes
    /// falls back to [`DEFAULT_NPROBES`] when `[search]` leaves it unset, so a
    /// default install never inherits Lance's unbounded "probe every partition"
    /// behavior on a remote store. No refine (see `apply_vector_search_knobs`).
    /// Index-only + map resolve when loaded, else key projection - see `fts_search`.
    pub async fn vector_search(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
        search: Option<&config::SearchConfig>,
    ) -> Result<Vec<SearchHit>> {
        let mut hits = if let Some(map) = self.rowmap.load_full() {
            let rowid_hits = self
                .vector_search_rowids(query, limit, filter, search)
                .await?;
            self.resolve_rowid_hits(&map, rowid_hits).await?
        } else {
            self.vector_search_keys(query, limit, filter, search)
                .await?
        };
        // Stable secondary sort: same reasoning as `fts_search` - IVF_SQ can
        // emit hits with effectively identical `_distance` in fragment-dependent
        // order, which makes RRF dedup-ranks nondeterministic for tied
        // neighbors. Sort by distance asc (smaller = more similar), then by
        // `(session_id, message_id)` asc.
        hits.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.key.session_id.cmp(&right.key.session_id))
                .then_with(|| left.key.message_id.cmp(&right.key.message_id))
        });
        Ok(hits)
    }

    /// Shared vector-scan setup: scope, `nearest`, knobs, `fast_search`.
    async fn vector_scanner(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
        search: Option<&config::SearchConfig>,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let scope = embedded_scope(filter);
        let mut scanner = self.handle.scanner(Table::Messages, Some(&scope)).await?;
        let key = Float32Array::from(query.to_vec());
        scanner.nearest("vector", &key, limit)?;
        apply_vector_search_knobs(&mut scanner, search);
        if self
            .handle
            .messages_has_index(MESSAGES_VECTOR_INDEX)
            .await?
        {
            scanner.fast_search();
        }
        scanner.disable_scoring_autoprojection();
        Ok(scanner)
    }

    /// Index-only vector retriever: `_rowid` + `_distance` only, so no `TakeExec`.
    /// `vector_search` resolves the row ids. Mirrors `fts_search_rowids`.
    async fn vector_search_rowids(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
        search: Option<&config::SearchConfig>,
    ) -> Result<Vec<(u64, f32)>> {
        let mut scanner = self.vector_scanner(query, limit, filter, search).await?;
        scanner.with_row_id();
        scanner.project(&["_distance"])?;
        let batch = scanner.try_into_batch().await?;
        let rowids = uint64(&batch, "_rowid")?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            hits.push((rowids.value(row), float32(&batch, "_distance", row)?));
        }
        Ok(hits)
    }

    /// No-map vector fallback: project the key columns plus `_distance` directly.
    /// Unsorted; `vector_search` sorts. Mirrors `fts_search_keys`.
    async fn vector_search_keys(
        &self,
        query: &[f32],
        limit: usize,
        filter: &Predicate,
        search: Option<&config::SearchConfig>,
    ) -> Result<Vec<SearchHit>> {
        let mut scanner = self.vector_scanner(query, limit, filter, search).await?;
        scanner.project(&["session_id", "id", "_distance"])?;
        let batch = scanner.try_into_batch().await?;
        let mut hits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = MessageKey {
                session_id: string(&batch, "session_id", row)?.context("session_id is null")?,
                message_id: string(&batch, "id", row)?.context("message id is null")?,
            };
            hits.push(SearchHit {
                rowid: None,
                key,
                score: float32(&batch, "_distance", row)?,
            });
        }
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
        apply_vector_search_knobs(&mut scanner, search);
        if self
            .handle
            .messages_has_index(MESSAGES_VECTOR_INDEX)
            .await?
        {
            scanner.fast_search();
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
        if self.handle.messages_has_index(MESSAGES_FTS_INDEX).await? {
            scanner.fast_search();
        }
        scanner.project(&["session_id", "id"])?;
        scanner.limit(Some(i64::try_from(limit).unwrap_or(i64::MAX)), None)?;
        scanner
            .explain_plan(true)
            .await
            .context("explain_plan failed")
    }

    /// Hydrate search hits by stable row id (spec.md#search). Resolves each
    /// rowid from the resident meta map in memory (no object-store round-trip -
    /// Lance caches index/metadata but never data column values, so a `take_rows`
    /// re-reads `search_text` from storage every query). Rowids the map lacks
    /// (appended since it was built, or no map loaded) fall back to a single
    /// `take_rows` batch. The caller indexes the result by key, so order is
    /// irrelevant.
    pub async fn message_metas_by_rowids(&self, rowids: &[u64]) -> Result<Vec<MessageMeta>> {
        if rowids.is_empty() {
            return Ok(Vec::new());
        }
        let mut metas = Vec::with_capacity(rowids.len());
        let misses: Vec<u64> = if let Some(map) = self.rowmap.load_full() {
            let (hits, misses) = map.hydrate(rowids);
            metas.extend(hits.into_iter().map(|entry| MessageMeta {
                message_id: entry.message_id,
                session_id: entry.session_id,
                role: entry.role,
                project: entry.project,
                source_agent: entry.source_agent,
                timestamp:
                    DateTime::from_timestamp_micros(entry.timestamp_micros).unwrap_or_default(),
                search_text: entry.search_text,
            }));
            misses
        } else {
            rowids.to_vec()
        };
        if !misses.is_empty() {
            metas.extend(self.message_metas_by_rowids_take(&misses).await?);
        }
        Ok(metas)
    }

    /// `take_rows` hydration of exactly `rowids` - the cache-miss fallback for
    /// rows the resident meta map lacks. `take_rows` coalesces the reads per
    /// fragment (Lance's own batching), so a scattered take is few requests, not
    /// one per row.
    async fn message_metas_by_rowids_take(&self, rowids: &[u64]) -> Result<Vec<MessageMeta>> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        let projection = ProjectionRequest::from_columns(
            [
                "id",
                "session_id",
                "role",
                "project",
                "source_agent",
                "timestamp",
                "search_text",
            ],
            dataset.schema(),
        );
        let batch = dataset.take_rows(rowids, projection).await?;
        let mut metas = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            metas.push(message_meta_from_batch(&batch, row)?);
        }
        Ok(metas)
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
            // The IN x IN predicate is a cross-product, so the scan can return
            // pairs that were never asked for; keep only the wanted keys.
            let meta = message_meta_from_batch(&batch, row)?;
            if wanted.contains(&MessageKey {
                session_id: meta.session_id.clone(),
                message_id: meta.message_id.clone(),
            }) {
                metas.push(meta);
            }
        }
        Ok(metas)
    }

    /// Total message count per session, for search session summaries. One
    /// `session_id IN (...)` scan projecting only `session_id`, aggregated in
    /// pond, instead of `N` concurrent `count_rows(session_id = X)` round-trips
    /// against `messages_session_id_btree`. Same wire shape for any backend,
    /// but one S3 operation instead of `N` on remote stores. Sessions with
    /// zero matching messages are present in the map with count `0` so the
    /// caller can distinguish "filter excluded everything" from "session
    /// missing from the response."
    pub async fn session_message_counts(
        &self,
        session_ids: &[String],
    ) -> Result<BTreeMap<String, usize>> {
        if session_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        // A version-matched resident map covers every current row, so its
        // per-session counts are authoritative (a session absent from it has 0
        // messages) - serve them with no scan. The version gate is load-bearing:
        // unlike meta hydration, a count cannot detect staleness by a row-id
        // miss, so a map that predates appended rows would undercount. A stale
        // or absent map falls through to the IN-scan.
        if let Some(map) = self.rowmap.load_full()
            && map.version() == self.messages_version().await?
        {
            return Ok(session_ids
                .iter()
                .map(|id| (id.clone(), map.lookup_count(id).unwrap_or(0)))
                .collect());
        }
        let predicate = in_predicate("session_id", session_ids);
        let scanner = self
            .handle
            .scan(
                Table::Messages,
                ScanOpts::with_predicate_and_projection(&predicate, &["session_id"]),
            )
            .await?;
        let mut stream = scanner
            .try_into_stream()
            .await
            .context("failed to open session_message_counts stream")?;
        let mut counts: BTreeMap<String, usize> =
            session_ids.iter().map(|id| (id.clone(), 0)).collect();
        while let Some(batch) = stream.next().await {
            let batch = batch.context("failed to read session_message_counts batch")?;
            let column = batch
                .column_by_name("session_id")
                .context("session_message_counts: session_id column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("session_message_counts: session_id column is not Utf8")?;
            for value in column.iter().flatten() {
                if let Some(entry) = counts.get_mut(value) {
                    *entry += 1;
                }
            }
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

    /// Rows added or rewritten in `messages` since the IVF_SQ vector index
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
    /// the `pond optimize` progress bar's known total.
    pub async fn embedding_progress(&self) -> Result<EmbeddingProgress> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        // `embedded` counts `embedding_model IS NOT NULL`, not `vector`: the two
        // are co-set (spec.md#session-embed-from-canonical) so the count is
        // identical, but the model-id string column is ~50x narrower than the
        // Float16 vector (Lance 7.0.0 has no per-column null_count, so this is a
        // data-page read).
        let embedded = dataset
            .count_rows(Some(Predicate::IsNotNull("embedding_model").to_lance()))
            .await?;
        // `backlog` and `total` come from live, deletion-aware counts, not the
        // FTS `num_docs`: num_docs counts indexed docs incl. deleted-but-unpurged
        // ones, so `num_docs - embedded` reports a phantom backlog that survives
        // every embed. `embedded` (model present) + `backlog` (model absent,
        // search_text present) is exactly the live eligible set, since embedding
        // a row requires its search_text.
        let backlog = self.embed_backlog_count().await?;
        Ok(EmbeddingProgress {
            embedded,
            total: embedded + backlog,
            backlog,
            model: embed::model_id(),
        })
    }

    /// Messages eligible but not yet embedded (`search_text` present,
    /// `embedding_model` null) - the exact set [`crate::embed::EmbedWorker`]
    /// processes. Read straight from the dataset so it is correct right after
    /// ingest, unlike the FTS `num_docs` `embedding_progress` shows (which lags
    /// until the index is rebuilt - the embed stage runs before that).
    pub async fn embed_backlog_count(&self) -> Result<usize> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        let filter = Predicate::And(vec![
            Predicate::IsNull("embedding_model"),
            Predicate::IsNotNull("search_text"),
        ]);
        Ok(dataset.count_rows(Some(filter.to_lance())).await?)
    }

    /// Count rows whose `embedding_model` is not the currently configured
    /// model AND whose `vector` is still populated - the signal `pond optimize`
    /// uses to detect a model swap and require `--force-embed`.
    pub async fn stale_embedding_count(&self) -> Result<usize> {
        let dataset = self.handle.dataset(Table::Messages).await?;
        // Same shape as the original (IsNotNull AND Ne), but the null check is on
        // the narrow model-id column, not the ~50x-wider Float16 vector: the two
        // are co-set (spec.md#session-embed-from-canonical), so `embedding_model
        // IS NOT NULL` equals `vector IS NOT NULL`, and the model-id page read is
        // far cheaper than the vector's.
        dataset
            .count_rows(Some(
                Predicate::And(vec![
                    Predicate::IsNotNull("embedding_model"),
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
    /// without running compaction. Used by `pond optimize`'s tail so newly
    /// written vectors land in the FTS / IVF_SQ / btree / bitmap indices
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

    /// Reclaim superseded data/index files across every indexed table (Lance
    /// `cleanup_old_versions`), without compaction. `pond optimize --rebuild`
    /// runs this after the rebuild so the index segments it just replaced are
    /// dropped immediately. The retention floor still protects versions a live
    /// reader may have pinned (spec.md#concurrency).
    pub async fn cleanup_old_versions(&self, older_than: chrono::Duration) -> Result<()> {
        for (table, _) in pond_index_intents().all() {
            self.handle
                .cleanup_table_versions(table, older_than)
                .await?;
        }
        Ok(())
    }

    pub async fn rebuild_indices(
        &self,
        intent_name: Option<&str>,
        progress: Option<OptimizeProgressFn>,
    ) -> Result<()> {
        let policy = pond_index_intents();
        let mut matched = false;
        for (table, intents) in policy.all() {
            for intent in intents {
                if intent_name.is_none_or(|name| name == intent.name) {
                    matched = true;
                    self.handle
                        .rebuild_index(table, intent, progress.as_ref())
                        .await?;
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

    /// Drop a named index from whichever table owns it. Used by `pond optimize
    /// --drop-index <name>` to clean up orphaned indices (e.g. after renaming
    /// an intent whose on-disk name no longer matches the policy). Finds the
    /// owning table via parallel `load_indices` lookups, then drops on just
    /// that table - so real I/O errors surface with the right context instead
    /// of being hidden behind "no such index" from the wrong table.
    pub async fn drop_index_by_name(&self, name: &str) -> Result<()> {
        let Some(owner) = self.handle.find_index_owner(name).await? else {
            anyhow::bail!("no index named {name:?} found on any table");
        };
        self.handle.drop_index(owner, name).await
    }

    pub async fn index_status(&self) -> Result<Vec<IndexStatus>> {
        let policy = pond_index_intents();
        let mut statuses = Vec::new();
        for (table, intents) in policy.all() {
            statuses.extend(self.handle.index_status(table, intents).await?);
        }
        Ok(statuses)
    }

    /// Drop the IVF_SQ index on `messages.vector`. Used by `pond optimize
    /// --force-embed` before re-bootstrapping under a different model. Silent
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
    /// Page forward: messages strictly after this id.
    pub after_message_id: Option<&'a str>,
    /// Page backward: messages strictly before this id.
    pub before_message_id: Option<&'a str>,
    pub limit: usize,
    pub budget_bytes: usize,
    /// First-page end when neither anchor is set.
    pub session_from: SessionFrom,
}

#[derive(Debug, Clone)]
pub struct MessageViewParams {
    /// Conversational siblings before the target (`grep -B`).
    pub context_before: usize,
    /// Conversational siblings after the target (`grep -A`).
    pub context_after: usize,
    pub budget_bytes: usize,
}

/// Outcome of a `pond_get` lookup. Separates a missing target (the handler
/// maps it to `not_found`) from a stale/unknown pagination anchor (mapped to
/// `validation_failed`): the message stream is append-only, so an anchor that
/// was ever valid never disappears - an unknown one is always a client error,
/// never a reason to silently restart the page.
#[derive(Debug, Clone, PartialEq)]
pub enum GetLookup<T> {
    NotFound,
    UnknownAnchor,
    Found(T),
}

/// Canonical retrieval result for `pond_get` session mode: the stored session
/// plus the page of messages (each with its `Part`s) and a remaining count.
/// Protocol-shaping into `GetResult`/`MessageView` happens in the handler.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionPage {
    pub session: Session,
    pub messages: Vec<RetrievedMessage>,
    pub before_remaining: usize,
    pub after_remaining: usize,
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

/// Like `page_by` but counts from the tail: how many trailing items fit
/// `limit` and the byte budget, dropping oldest first. The last (newest) item
/// is always kept, so the returned count is >= 1 for a non-empty slice and the
/// emitted page (`items[len - n..]`) stays chronological.
fn page_tail<T>(
    items: &[T],
    limit: usize,
    budget_bytes: usize,
    size: impl Fn(&T) -> usize,
) -> usize {
    let cap = limit.clamp(1, 1000);
    let mut acc = 0usize;
    let mut emitted = 0usize;
    for item in items.iter().rev() {
        if emitted >= cap {
            break;
        }
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

/// Scalar indexes on `messages` (spec.md#datasets): only columns whose index
/// type matches the predicate actually issued against them. `project` is
/// filtered solely by `LikeContains`/`Regex` (substring), which a BTree cannot
/// accelerate, and `role` is never filtered - both are deliberately unindexed
/// (substring lookup stays on the SQL `LIKE` path). There is no index on
/// `embedding_model`: pond's invariant is one active model at a time (a model
/// swap goes through `pond optimize --force-embed` which drops the IVF_SQ,
/// clears stale rows, and re-bootstraps), so the only embedding-state filter is
/// `vector IS NOT NULL`. `id` lookups are rare and full-scan.
const MESSAGE_SCALAR_INDICES: &[(&str, BuiltinIndexType, &str)] = &[
    (
        "session_id",
        BuiltinIndexType::BTree,
        "messages_session_id_btree",
    ),
    // Range-only column (`from_date`/`to_date` -> `timestamp >=` / `<=`,
    // never exact-equality, never `ORDER BY` against the index). ZoneMap's
    // per-fragment min/max prunes those filters with no recall loss (measured:
    // 258 zones -> ~6, ~42x fewer rows scanned on the real S3 corpus), and
    // skips the global ExternalSort that a BTree would pay during build.
    (
        "timestamp",
        BuiltinIndexType::ZoneMap,
        "messages_timestamp_zonemap",
    ),
    (
        "source_agent",
        BuiltinIndexType::Bitmap,
        "messages_source_agent_bitmap",
    ),
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

/// Session ids per `session_id IN (...)` chunk in an incremental copy: large
/// enough to amortize per-scan setup, small enough to keep the pushed-down
/// predicate string and its btree lookup batch bounded.
const COPY_SESSION_IN_CHUNK: usize = 512;

fn in_predicate(column: &'static str, values: &[String]) -> Predicate {
    Predicate::In(
        column,
        values.iter().cloned().map(ScalarValue::String).collect(),
    )
}

/// The kNN prefilter is the caller's scalar filter alone - pond does NOT add
/// `vector IS NOT NULL`. That looks like a safe guard but it is a remote-read
/// trap: Lance v2 keeps no per-column null metadata, so `IsNotNull(vector)`
/// forces a full read of the ~3 GiB `vector` column from the object store on
/// every query (the ANN prefilter is evaluated as a `LanceScan` over the
/// column) - measured at ~57 s/query on the 2M-row S3 corpus, dwarfing the
/// IVF probe itself. It is also redundant: the IVF_SQ index only contains
/// embedded rows, and Lance's `_distance IS NOT NULL` post-filter (present in
/// both the ANN and brute-force branches of the plan) already drops any
/// null-vector row the brute-force tail might surface. So an empty caller
/// filter yields an empty prefilter and a pure index probe (spec.md#search,
/// spec.md#search-prefilter-pushdown).
fn embedded_scope(filter: &Predicate) -> Predicate {
    filter.clone()
}

/// IVF `nprobes` applied when `[search].nprobes` is unset. Left unset, Lance
/// probes up to every partition (~num_rows/4096, ~500 on the 2M-row corpus),
/// one object-store read each - the dominant cost of a vector scan on a remote
/// store. 32 bounds the reads while keeping recall (benchmarked, spec.md#search).
pub const DEFAULT_NPROBES: usize = 32;

/// Apply pond's vector-search tuning to a kNN scanner, defaulting any unset
/// `[search]` knob so a default install never inherits Lance's unbounded
/// probe-every-partition behavior. No refine: IVF_SQ's per-dimension codes are
/// precise enough to rank from the prewarmed partition, so pond never re-reads
/// exact vectors from the data files (the remote-store GET storm PQ+refine
/// incurred).
fn apply_vector_search_knobs(
    scanner: &mut lance::dataset::scanner::Scanner,
    search: Option<&config::SearchConfig>,
) {
    let nprobes = search
        .and_then(|cfg| cfg.nprobes)
        .unwrap_or(DEFAULT_NPROBES);
    scanner.nprobes(nprobes);
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

/// IVF_SQ index name on `messages.vector` (spec.md#search). Stable so the
/// activation check, optimize/append, and status all name the same index. The
/// literal keeps the historical `_ivfpq` suffix as a stable identifier:
/// renaming it would orphan the existing segment under a new name. A plain
/// `optimize` folds into whatever index type already exists, so switching an
/// existing IVF_PQ store to IVF_SQ needs `pond optimize --rebuild`.
pub const MESSAGES_VECTOR_INDEX: &str = "messages_vector_ivfpq";

/// IVF_SQ tuning constants (spec.md#search):
/// - num_bits = 8 (per-dimension scalar quantization)
/// - max_iters = 15 (kmeans cap)
/// - cosine metric (e5 vectors are L2-normalized)
const IVF_SQ_NUM_BITS: u16 = 8;
const IVF_SQ_MAX_ITERS: usize = 15;

/// Pond's production IndexIntents: the per-table intent set
/// `Store::open_with_options` registers with the substrate.
pub fn pond_index_intents() -> IndexIntents {
    pond_index_intents_with_vector_threshold(VECTOR_INDEX_ACTIVATION_ROWS)
}

/// Same as [`pond_index_intents`] but with an overridable IVF_SQ activation
/// threshold. Used by tests that need to exercise the activation boundary
/// without writing 100k vectors.
pub(crate) fn pond_index_intents_with_vector_threshold(vector_threshold: usize) -> IndexIntents {
    let mut messages = Vec::with_capacity(MESSAGE_SCALAR_INDICES.len() + 2);
    messages.push(IndexIntent {
        name: MESSAGES_FTS_INDEX,
        column: "search_text",
        trigger: IndexTrigger::OnAnyRows,
        params: IndexParamsKind::InvertedFtsWord,
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
        params: IndexParamsKind::IvfSqCosine {
            num_bits: IVF_SQ_NUM_BITS,
            max_iters: IVF_SQ_MAX_ITERS,
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
/// `auto_cleanup` is short; long-term recovery is `pond copy --to <file>`
/// snapshots plus deferred Lance tags (spec.md#session-durable-copy).
/// `skip_auto_cleanup` suppresses the per-commit hook so cleanup stays
/// operator-driven via `pond optimize` (one LIST per command instead of per write).
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

/// The composite primary-key columns each table's schema declares
/// (spec.md#lance-table-creation-session-scoped-pk): a message/part id is
/// unique only within its session, so the key leads with `session_id`. The one
/// source of truth for the PK structure - kept beside the schemas it mirrors,
/// not in the schema-agnostic substrate seam.
pub(crate) fn pk_columns(table: Table) -> &'static [&'static str] {
    match table {
        Table::Sessions => &["id"],
        Table::Messages => &["session_id", "id"],
        Table::Parts => &["session_id", "message_id", "id"],
    }
}

/// One scanned row's composite primary key as owned strings, in `pk` order.
fn composite_key(batch: &RecordBatch, pk: &[&str], row: usize) -> Result<Vec<String>> {
    pk.iter()
        .map(|column| string(batch, column, row)?.with_context(|| format!("{column} is null")))
        .collect()
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
        // filled inline at ingest when embedding is on, else null until a later
        // `pond optimize` embed pass; `vector` and `embedding_model` set together.
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

// Lance v7.0.0-beta.16's IVF_SQ build path (`rust/lance/src/index/vector/utils.rs`
// `infer_vector_element_type_impl`) accepts only Float16/Float32/Float64/UInt8/Int8;
// `FixedSizeBinary(2)`-backed `lance.bfloat16` is rejected. The format docs list
// BFloat16 as a future-supported embedding type; until the Rust IVF_SQ build
// path catches up, store as Float16 (half-precision, also 2 bytes/element).
fn embedding_vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float16, true)),
        embedding_dim() as i32,
    )
}

/// The partial-schema source for the embedding column update: the `messages`
/// primary key plus the two columns `pond optimize` fills. The field definitions
/// match `message_schema` exactly so Lance accepts it as a subset upsert.
fn embedding_update_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("session_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new("vector", embedding_vector_type(), true),
        Field::new("embedding_model", DataType::Utf8, true),
    ]))
}

/// The `messages` `vector` + `embedding_model` columns for an inline-embed
/// batch: `Some` rows carry the embedding and the current model id, `None` rows
/// are null in both. Returned aligned to `vectors` for [`messages_chunk`].
fn embedding_columns(vectors: &[Option<Vec<f32>>]) -> Result<(ArrayRef, ArrayRef)> {
    let dim = embedding_dim();
    // The common case (no embedder, or every row already present) is all-null:
    // build both columns with one bulk allocation instead of dim per-row appends.
    if vectors.iter().all(Option::is_none) {
        return Ok((
            new_null_array(&embedding_vector_type(), vectors.len()),
            new_null_array(&DataType::Utf8, vectors.len()),
        ));
    }
    let mut builder = FixedSizeListBuilder::new(
        Float16Builder::with_capacity(vectors.len() * dim),
        dim as i32,
    )
    .with_field(Arc::new(Field::new("item", DataType::Float16, true)));
    let mut models: Vec<Option<&str>> = Vec::with_capacity(vectors.len());
    for vector in vectors {
        match vector {
            Some(values) => {
                if values.len() != dim {
                    anyhow::bail!("inline embedding has dim {}, expected {dim}", values.len());
                }
                for value in values {
                    builder.values().append_value(half::f16::from_f32(*value));
                }
                builder.append(true);
                models.push(Some(embed::model_id()));
            }
            None => {
                for _ in 0..dim {
                    builder.values().append_null();
                }
                builder.append(false);
                models.push(None);
            }
        }
    }
    Ok((
        Arc::new(builder.finish()) as ArrayRef,
        Arc::new(StringArray::from(models)) as ArrayRef,
    ))
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

/// `vectors` is aligned to `rows` (same length): `Some` carries the inline
/// embedding for that row, `None` writes a null `vector`/`embedding_model`.
pub(crate) fn messages_batches(
    rows: &[MessageBatchRow<'_>],
    vectors: &[Option<Vec<f32>>],
) -> Result<Vec<RecordBatch>> {
    debug_assert_eq!(rows.len(), vectors.len(), "vectors must align with rows");
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
        .map(|range| {
            messages_chunk(
                &rows[range.clone()],
                &options[range.clone()],
                &vectors[range],
            )
        })
        .collect()
}

fn messages_chunk(
    rows: &[MessageBatchRow<'_>],
    options: &[Vec<u8>],
    vectors: &[Option<Vec<f32>>],
) -> Result<RecordBatch> {
    let schema = message_schema();
    let (vector_column, embedding_model) = embedding_columns(vectors)?;
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
            // `vector` / `embedding_model` carry the inline embedding when one
            // was produced for the row, null otherwise (embedder disabled, or a
            // non-embeddable row); `pond optimize` fills any remaining nulls
            // (spec.md#session-embed-from-canonical).
            vector_column,
            embedding_model,
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

/// [`SkipOracle`](crate::adapter::SkipOracle) over the resident row-meta map:
/// `pond sync` reads each session's stored max message timestamp from memory, so
/// the staleness check costs zero S3 (the map is rebuilt from the store, so the
/// check stays deterministic with no local cursor). A `None` map (never
/// prewarmed, or the build failed) yields no watermark, so every source
/// re-reads - safe, just slower.
pub struct RowmapOracle(pub Option<Arc<RowMetaSet>>);

impl crate::adapter::SkipOracle for RowmapOracle {
    fn session_max_ts(&self, session_id: &str) -> Option<i64> {
        self.0.as_ref()?.lookup_max_ts(session_id)
    }

    fn is_empty(&self) -> bool {
        self.0.as_ref().is_none_or(|set| set.is_empty())
    }
}

fn row_meta_entry(batch: &RecordBatch, row_id: u64, row: usize) -> Result<RowMetaEntry> {
    Ok(RowMetaEntry {
        row_id,
        session_id: string(batch, "session_id", row)?.context("session_id is null")?,
        message_id: string(batch, "id", row)?.context("message id is null")?,
        role: string(batch, "role", row)?.context("role is null")?,
        project: string(batch, "project", row)?.context("project is null")?,
        source_agent: string(batch, "source_agent", row)?.context("source_agent is null")?,
        timestamp_micros: datetime(batch, "timestamp", row)?.timestamp_micros(),
        search_text: string(batch, "search_text", row)?.unwrap_or_default(),
    })
}

pub(crate) fn message_meta_from_batch(batch: &RecordBatch, row: usize) -> Result<MessageMeta> {
    Ok(MessageMeta {
        message_id: string(batch, "id", row)?.context("id is null")?,
        session_id: string(batch, "session_id", row)?.context("session_id is null")?,
        role: string(batch, "role", row)?.context("role is null")?,
        project: string(batch, "project", row)?.context("project is null")?,
        source_agent: string(batch, "source_agent", row)?.context("source_agent is null")?,
        timestamp: datetime(batch, "timestamp", row)?,
        search_text: string(batch, "search_text", row)?.unwrap_or_default(),
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

    /// Counts the texts handed to the backend so a test can assert how many rows
    /// were embedded.
    #[derive(Default)]
    struct CountingEmbedder {
        texts: std::sync::atomic::AtomicUsize,
    }
    impl crate::embed::Embedder for CountingEmbedder {
        fn device(&self) -> &str {
            "test"
        }
        fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
            self.texts
                .fetch_add(texts.len(), std::sync::atomic::Ordering::SeqCst);
            Ok(texts
                .iter()
                .map(|_| vec![0.0_f32; embedding_dim()])
                .collect())
        }
    }

    /// A session with `count` conversational user messages, each carrying a text
    /// part so its `search_text` is non-null (hence embeddable).
    fn conversational_events(session_id: &str, count: usize) -> Vec<IngestEvent> {
        let mut events = vec![IngestEvent::Session(synthetic_session(session_id))];
        for index in 0..count {
            events.push(IngestEvent::Message(Message::User {
                id: format!("msg-{index}"),
                session_id: session_id.to_owned(),
                timestamp: Utc::now(),
                options: ProviderOptions::new(),
            }));
            events.push(IngestEvent::Part(Part {
                session_id: session_id.to_owned(),
                id: format!("msg-{index}:0001"),
                message_id: format!("msg-{index}"),
                ordinal: 0,
                provenance: crate::wire::Provenance::Conversational,
                options: ProviderOptions::new(),
                kind: PartKind::Text {
                    text: Some(Extracted::from_test_value(format!("body {index}"))),
                },
            }));
        }
        events
    }

    /// Inline embed-at-ingest: with an embedder attached the vectors are filled
    /// in the message rows' birth append (no extra embed commit - the version
    /// matches a plain ingest), and a re-sync embeds no already-present row.
    #[tokio::test(flavor = "multi_thread")]
    async fn ingest_embeds_inline_in_the_birth_commit() -> anyhow::Result<()> {
        let plain = Store::open(&Url::parse("shared-memory://pond-test-inline-plain/")?).await?;
        ingest_events(&plain, conversational_events("01HXYINLINE000PLAIN", 5)).await?;
        assert!(
            !plain.has_embeddings().await?,
            "no embedder attached -> every vector null",
        );
        let plain_version = plain.messages_version().await?;

        let backend = Arc::new(CountingEmbedder::default());
        let embedder = Arc::new(crate::embed::LazyEmbedder::from_loaded(
            backend.clone() as Arc<dyn crate::embed::Embedder>
        ));
        let store = Store::open(&Url::parse("shared-memory://pond-test-inline-embed/")?)
            .await?
            .with_embedder(embedder);
        ingest_events(&store, conversational_events("01HXYINLINE000EMBED", 5)).await?;
        assert!(
            store.has_embeddings().await?,
            "embedder attached -> vectors filled at ingest",
        );
        assert_eq!(
            backend.texts.load(std::sync::atomic::Ordering::SeqCst),
            5,
            "every conversational message embedded once",
        );
        assert_eq!(
            store.messages_version().await?,
            plain_version,
            "inline embed must ride the append, not add a separate commit",
        );

        ingest_events(&store, conversational_events("01HXYINLINE000EMBED", 5)).await?;
        assert_eq!(
            backend.texts.load(std::sync::atomic::Ordering::SeqCst),
            5,
            "an idempotent re-sync embeds no already-present row",
        );
        Ok(())
    }

    /// The verify's duplicate count (`rows - distinct composite PKs` from
    /// [`Store::composite_pk_index`]) keys on the composite PK, so the same
    /// message id in two different sessions is NOT a duplicate (the bare-id
    /// false-positive trap), while a genuinely doubled `(session_id, id)` row
    /// counts as one.
    #[tokio::test(flavor = "multi_thread")]
    async fn composite_pk_index_counts_duplicates_by_composite_key() -> anyhow::Result<()> {
        async fn duplicates(store: &Store, table: Table) -> anyhow::Result<usize> {
            let (keys, rows) = store.composite_pk_index(table).await?;
            Ok(rows - keys.len())
        }
        let store = Store::open(&Url::parse("shared-memory://pond-test-dupcount/")?).await?;
        ingest_events(&store, conversational_events("01HXYDUP00000SESS1", 1)).await?;
        ingest_events(&store, conversational_events("01HXYDUP00000SESS2", 1)).await?;
        assert_eq!(
            duplicates(&store, Table::Messages).await?,
            0,
            "the same message id in two sessions is not a duplicate (composite PK)",
        );
        assert_eq!(duplicates(&store, Table::Sessions).await?, 0);
        assert_eq!(duplicates(&store, Table::Parts).await?, 0);

        // Inject a real duplicate via the low-level append (no dedup) - the
        // write anomaly the copy verify must catch.
        let message = Message::User {
            id: "dup-msg".to_owned(),
            session_id: "01HXYDUP00000SESS1".to_owned(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        };
        let row = MessageBatchRow {
            message: &message,
            source_agent: "claude-code",
            project: "/tmp",
            search_text: None,
        };
        let batches = messages_batches(&[row], &[None])?;
        store
            .handle
            .append_batches(Table::Messages, batches.clone())
            .await?;
        store
            .handle
            .append_batches(Table::Messages, batches)
            .await?;
        assert_eq!(
            duplicates(&store, Table::Messages).await?,
            1,
            "the doubled (session_id, id) row is exactly one duplicate",
        );
        Ok(())
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
    async fn resident_meta_map_hydration_matches_take_rows_fallback() -> anyhow::Result<()> {
        // The resident meta map must hydrate hits identically to the take_rows
        // fallback - same fields, and the microsecond timestamp survives the
        // i64 round-trip through the mmap blob.
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("hydration-parity");

        let messages = [
            (
                "m1",
                "the auth refactor landed cleanly",
                1_700_000_000_123_456_i64,
            ),
            (
                "m2",
                "balance handler now retries on rpc timeout",
                1_700_000_050_654_321,
            ),
        ];
        let mut validator = IngestValidator::default();
        validator
            .push(&store, 0, IngestEvent::Session(session.clone()))
            .await?;
        let mut seq = 1;
        for (mid, text, micros) in messages {
            let message = Message::User {
                id: mid.to_owned(),
                session_id: session.id.clone(),
                timestamp: DateTime::from_timestamp_micros(micros).unwrap(),
                options: ProviderOptions::new(),
            };
            validator
                .push(&store, seq, IngestEvent::Message(message))
                .await?;
            seq += 1;
            let part = Part {
                session_id: session.id.clone(),
                id: format!("{mid}-p0"),
                message_id: mid.to_owned(),
                ordinal: 0,
                provenance: crate::wire::Provenance::Conversational,
                options: ProviderOptions::new(),
                kind: PartKind::Text {
                    text: Some(Extracted::from_test_value(text.to_owned())),
                },
            };
            validator.push(&store, seq, IngestEvent::Part(part)).await?;
            seq += 1;
        }
        validator.finish(&store).await?;

        let rowids: Vec<u64> = store
            .collect_row_metas()
            .await?
            .into_iter()
            .map(|entry| entry.row_id)
            .collect();
        assert_eq!(rowids.len(), 2);

        let sort_by_id = |mut metas: Vec<MessageMeta>| {
            metas.sort_by(|left, right| left.message_id.cmp(&right.message_id));
            metas
        };

        let fallback = sort_by_id(store.message_metas_by_rowids(&rowids).await?);

        // Build and install the resident meta map; the same call now hydrates
        // from memory (zero misses - the map covers the whole table).
        store.ensure_rowmap(&temp.path().join("cache")).await?;
        let resident = sort_by_id(store.message_metas_by_rowids(&rowids).await?);

        assert_eq!(
            resident, fallback,
            "resident-map hydration must match the take_rows fallback"
        );
        assert_eq!(
            resident[0].timestamp.timestamp_micros(),
            1_700_000_000_123_456
        );
        Ok(())
    }

    #[tokio::test]
    async fn initialized_flips_only_after_first_ingest() -> anyhow::Result<()> {
        // `open` eagerly creates sessions/messages but `parts` is lazy, so a
        // configured-but-never-synced store reports uninitialized - the signal
        // `pond status` uses to render an empty state instead of
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
    /// IVF_SQ activation threshold.
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
        // create the IVF_SQ because the trigger is not met.
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
            "IVF_SQ must not exist below the activation threshold",
        );

        // Next batch: one more vector. Total reaches 256; optimize creates
        // the IVF_SQ.
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
            "optimize must create the IVF_SQ once the threshold is crossed",
        );

        // The remaining 44 rows stay un-embedded; the IVF_SQ trains over the
        // non-null subset and a planted vector is retrievable.
        let hits = store
            .vector_search(&synthetic_vector(0), 10, &Predicate::And(Vec::new()), None)
            .await?;
        assert!(
            hits.iter().any(|hit| hit.key == keys[0]),
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
            "IVF_SQ must exist before a model swap",
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
            "optimize must rebuild IVF_SQ after force re-embed",
        );

        let stream = store.pending_or_stale_messages();
        tokio::pin!(stream);
        assert!(stream.next().await.is_none(), "up-to-date rows are skipped");
        Ok(())
    }

    #[tokio::test]
    async fn session_last_message_ids_come_from_durable_messages() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session = synthetic_session("oracle");
        store
            .upsert_sessions(std::slice::from_ref(&session))
            .await?;
        let timestamp =
            chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp");
        let message_a = Message::User {
            id: "oracle-a".to_owned(),
            session_id: session.id.clone(),
            timestamp,
            options: ProviderOptions::new(),
        };
        let message_b = Message::User {
            id: "oracle-b".to_owned(),
            session_id: session.id.clone(),
            timestamp,
            options: ProviderOptions::new(),
        };
        store
            .upsert_messages(
                &session,
                &[
                    MessageWrite {
                        message: &message_a,
                        parts: &[],
                        search_text: Some("a"),
                    },
                    MessageWrite {
                        message: &message_b,
                        parts: &[],
                        search_text: Some("b"),
                    },
                ],
            )
            .await?;

        let empty_session = synthetic_session("session-row-only");
        store.upsert_sessions(&[empty_session]).await?;

        // Orphan: messages committed but the session row never was (the crash
        // window `upsert_session_batch`'s write order can leave). The gate must
        // NOT key on it, so the source re-ingests and heals the missing row.
        let orphan = synthetic_session("messages-no-row");
        let orphan_message = Message::User {
            id: "orphan-a".to_owned(),
            session_id: orphan.id.clone(),
            timestamp,
            options: ProviderOptions::new(),
        };
        store
            .upsert_messages(
                &orphan,
                &[MessageWrite {
                    message: &orphan_message,
                    parts: &[],
                    search_text: Some("a"),
                }],
            )
            .await?;

        let map = store.session_last_message_ids().await?;
        assert_eq!(map.get("oracle").map(String::as_str), Some("oracle-b"));
        assert!(
            !map.contains_key("session-row-only"),
            "a session row without durable messages must not produce a freshness key",
        );
        assert!(
            !map.contains_key("messages-no-row"),
            "messages without a durable session row must not produce a freshness key",
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
        assert_eq!(before.backlog, 10);
        assert_eq!(before.model, crate::embed::model_id());

        store.write_embeddings(&embedded(&keys[..4])).await?;
        let partial = store.embedding_progress().await?;
        assert_eq!(partial.embedded, 4);
        assert_eq!(partial.total, 10);
        assert_eq!(partial.backlog, 6);

        store.write_embeddings(&embedded(&keys[4..])).await?;
        let full = store.embedding_progress().await?;
        assert_eq!(full.embedded, 10);
        assert_eq!(full.total, 10);
        // The pending signal is the live un-embedded count and matches the
        // authoritative backlog - never derived from FTS num_docs.
        assert_eq!(full.backlog, 0);
        assert_eq!(full.backlog, store.embed_backlog_count().await?);
        Ok(())
    }

    #[tokio::test]
    async fn ensure_rowmap_layers_a_delta_on_new_ingest() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, _keys) = store_with_messages(&temp, 6).await?;
        let cache = temp.path().join("cache");

        store.ensure_rowmap(&cache).await?;
        assert_eq!(
            store.rowmap_delta_count(),
            Some(0),
            "first build is a lone base"
        );

        // A new session's message bumps the version with a fresh fragment.
        ingest_events(
            &store,
            vec![
                IngestEvent::Session(Session {
                    id: "session-new".to_owned(),
                    parent_session_id: None,
                    parent_message_id: None,
                    source_agent: "claude-code".to_owned(),
                    created_at: Utc::now(),
                    project: Extracted::from_test_value("/proj/new".to_owned()),
                    options: ProviderOptions::new(),
                }),
                IngestEvent::Message(Message::User {
                    id: "m-new".to_owned(),
                    session_id: "session-new".to_owned(),
                    timestamp: Utc::now(),
                    options: ProviderOptions::new(),
                }),
                IngestEvent::Part(Part {
                    session_id: "session-new".to_owned(),
                    id: "m-new-part".to_owned(),
                    message_id: "m-new".to_owned(),
                    ordinal: 0,
                    provenance: crate::wire::Provenance::Conversational,
                    options: ProviderOptions::new(),
                    kind: PartKind::Text {
                        text: Some(Extracted::from_test_value("brand new message".to_owned())),
                    },
                }),
            ],
        )
        .await?;

        // The refresh scans only the new fragment and layers a delta - not a
        // full rebuild.
        store.ensure_rowmap(&cache).await?;
        assert_eq!(
            store.rowmap_delta_count(),
            Some(1),
            "new ingest layered a delta"
        );

        // The new session's count is served from the chain (base + delta sum).
        let counts = store
            .session_message_counts(&["session-new".to_owned()])
            .await?;
        assert_eq!(counts.get("session-new").copied(), Some(1));
        Ok(())
    }

    /// Regression for the v0.10.0 sync death-spiral: the 1h cleanup retention can
    /// reclaim the dataset version the on-disk chain was last built at. The delta
    /// extender's `checkout_version(base)` then errored, the error nuked
    /// `ensure_rowmap`, and the oracle silently fell back to re-reading every
    /// source on every sync forever (the chain never advanced past the reclaimed
    /// base). A reclaimed base must degrade to a full rebuild, like compaction.
    #[tokio::test]
    async fn ensure_rowmap_rebuilds_when_base_manifest_reclaimed() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, _keys) = store_with_messages(&temp, 6).await?;
        let cache = temp.path().join("cache");

        // Build the chain at the current version, then snapshot the manifests
        // that exist at-or-below it - these are exactly what cleanup reclaims.
        store.ensure_rowmap(&cache).await?;
        assert_eq!(store.rowmap_delta_count(), Some(0), "first build is a base");
        let base_version = store.messages_version().await?;
        let versions_dir = temp.path().join("messages.lance").join("_versions");
        let base_manifests: Vec<_> = std::fs::read_dir(&versions_dir)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "manifest"))
            .collect();
        assert!(
            !base_manifests.is_empty(),
            "the base version has a manifest"
        );

        // A new session bumps the version, so the on-disk chain now trails the
        // dataset and a refresh would normally delta from `base_version`.
        ingest_events(
            &store,
            vec![
                IngestEvent::Session(Session {
                    id: "session-after".to_owned(),
                    parent_session_id: None,
                    parent_message_id: None,
                    source_agent: "claude-code".to_owned(),
                    created_at: Utc::now(),
                    project: Extracted::from_test_value("/proj/after".to_owned()),
                    options: ProviderOptions::new(),
                }),
                IngestEvent::Message(Message::User {
                    id: "m-after".to_owned(),
                    session_id: "session-after".to_owned(),
                    timestamp: Utc::now(),
                    options: ProviderOptions::new(),
                }),
                IngestEvent::Part(Part {
                    session_id: "session-after".to_owned(),
                    id: "m-after-part".to_owned(),
                    message_id: "m-after".to_owned(),
                    ordinal: 0,
                    provenance: crate::wire::Provenance::Conversational,
                    options: ProviderOptions::new(),
                    kind: PartKind::Text {
                        text: Some(Extracted::from_test_value("after the base".to_owned())),
                    },
                }),
            ],
        )
        .await?;
        assert!(
            store.messages_version().await? > base_version,
            "the new ingest advanced the dataset past the chain's base"
        );

        // Reclaim the base version's manifest exactly as `cleanup_old_versions`
        // would: `checkout_version(base_version)` can no longer resolve.
        for manifest in &base_manifests {
            std::fs::remove_file(manifest)?;
        }

        // A fresh Store finds the trailing chain on disk, tries to delta from the
        // reclaimed base, and must fall back to a full rebuild - Ok, not Err.
        let reopened = Store::open_local(temp.path()).await?;
        reopened.ensure_rowmap(&cache).await?;
        assert!(
            reopened.rowmap_snapshot().is_some(),
            "map rebuilt after the base manifest was reclaimed"
        );
        assert_eq!(
            reopened.rowmap_delta_count(),
            Some(0),
            "a reclaimed base forces a fresh full-scan base, not a stuck chain"
        );

        // The rebuilt base covers the post-base ingest, so the oracle is whole.
        let counts = reopened
            .session_message_counts(&["session-after".to_owned()])
            .await?;
        assert_eq!(counts.get("session-after").copied(), Some(1));
        Ok(())
    }

    /// The steady-state hot path: embedding rewrites the message fragments every
    /// sync (merge_update on the `vector` column). Keying the delta off fragment
    /// identity made that rewrite force a full 2.1M-row rebuild every sync.
    /// Stable row ids preserve row_ids across the rewrite, so the refresh must
    /// layer a cheap append-only delta of just the new rows - and must NOT
    /// double-count the rewritten rows that still live in the base.
    #[tokio::test]
    async fn ensure_rowmap_deltas_across_embedding_fragment_rewrite() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages(&temp, 6).await?;
        let cache = temp.path().join("cache");
        store.ensure_rowmap(&cache).await?;
        assert_eq!(store.rowmap_delta_count(), Some(0), "first build is a base");

        // Embedding rewrites every message fragment (new fragment ids, same
        // stable row_ids, untouched ROW_META columns).
        store.write_embeddings(&embedded(&keys)).await?;

        // A new session appends one row on top of the rewritten fragments.
        ingest_events(
            &store,
            vec![
                IngestEvent::Session(Session {
                    id: "session-after".to_owned(),
                    parent_session_id: None,
                    parent_message_id: None,
                    source_agent: "claude-code".to_owned(),
                    created_at: Utc::now(),
                    project: Extracted::from_test_value("/proj/after".to_owned()),
                    options: ProviderOptions::new(),
                }),
                IngestEvent::Message(Message::User {
                    id: "m-after".to_owned(),
                    session_id: "session-after".to_owned(),
                    timestamp: Utc::now(),
                    options: ProviderOptions::new(),
                }),
                IngestEvent::Part(Part {
                    session_id: "session-after".to_owned(),
                    id: "m-after-part".to_owned(),
                    message_id: "m-after".to_owned(),
                    ordinal: 0,
                    provenance: crate::wire::Provenance::Conversational,
                    options: ProviderOptions::new(),
                    kind: PartKind::Text {
                        text: Some(Extracted::from_test_value("after embedding".to_owned())),
                    },
                }),
            ],
        )
        .await?;

        // The refresh layers a delta of just the appended row, not a full
        // rebuild - despite every prior fragment having been rewritten.
        store.ensure_rowmap(&cache).await?;
        assert_eq!(
            store.rowmap_delta_count(),
            Some(1),
            "fragment rewrite + append must layer a delta, not full-rebuild"
        );

        // Counts stay honest: the rewritten base rows are not re-emitted into the
        // delta, so nothing is double-counted across base + delta segments.
        let counts = store
            .session_message_counts(&["session-after".to_owned(), "session-0".to_owned()])
            .await?;
        assert_eq!(counts.get("session-after").copied(), Some(1));
        assert_eq!(
            counts.get("session-0").copied(),
            Some(1),
            "a base row survived the rewrite without being double-counted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rowmap_chain_compacts_and_stays_bounded() -> anyhow::Result<()> {
        // Many version bumps (the remote-writers case) must not grow the chain
        // unboundedly: deltas cap at MAX, then compact into a fresh base.
        let temp = TempDir::new()?;
        let (store, _keys) = store_with_messages(&temp, 4).await?;
        let cache = temp.path().join("cache");
        store.ensure_rowmap(&cache).await?;

        let mut reached_cap = false;
        let mut compacted = false;
        for i in 0..(Store::MAX_ROWMAP_DELTAS + 2) {
            let session = format!("session-x{i}");
            ingest_events(
                &store,
                vec![
                    IngestEvent::Session(Session {
                        id: session.clone(),
                        parent_session_id: None,
                        parent_message_id: None,
                        source_agent: "claude-code".to_owned(),
                        created_at: Utc::now(),
                        project: Extracted::from_test_value("/proj/x".to_owned()),
                        options: ProviderOptions::new(),
                    }),
                    IngestEvent::Message(Message::User {
                        id: format!("mx{i}"),
                        session_id: session.clone(),
                        timestamp: Utc::now(),
                        options: ProviderOptions::new(),
                    }),
                    IngestEvent::Part(Part {
                        session_id: session.clone(),
                        id: format!("mx{i}-part"),
                        message_id: format!("mx{i}"),
                        ordinal: 0,
                        provenance: crate::wire::Provenance::Conversational,
                        options: ProviderOptions::new(),
                        kind: PartKind::Text {
                            text: Some(Extracted::from_test_value(format!("msg {i}"))),
                        },
                    }),
                ],
            )
            .await?;
            store.ensure_rowmap(&cache).await?;
            let deltas = store.rowmap_delta_count().unwrap();
            assert!(
                deltas <= Store::MAX_ROWMAP_DELTAS,
                "delta count {deltas} exceeded the cap",
            );
            if deltas == Store::MAX_ROWMAP_DELTAS {
                reached_cap = true;
            }
            if reached_cap && deltas < Store::MAX_ROWMAP_DELTAS {
                compacted = true;
            }
        }
        assert!(reached_cap, "deltas accumulated to the cap (append path)");
        assert!(compacted, "the chain compacted back into a base");

        // Files stay bounded and no build temps leak.
        let mut rmm = 0;
        for entry in std::fs::read_dir(&cache)? {
            let name = entry?.file_name().into_string().unwrap_or_default();
            assert!(!name.contains(".tmp-"), "leaked build temp: {name}");
            if name.ends_with(".rmm") {
                rmm += 1;
            }
        }
        assert!(
            rmm <= Store::MAX_ROWMAP_DELTAS + 1,
            "files unbounded: {rmm}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn embed_backlog_count_tracks_eligible_unembedded_rows() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, keys) = store_with_messages(&temp, 10).await?;

        // Read straight from the dataset (no FTS index here), so it is correct
        // right after ingest - the case that lagged `embedding_progress`.
        assert_eq!(store.embed_backlog_count().await?, 10);

        store.write_embeddings(&embedded(&keys[..4])).await?;
        assert_eq!(store.embed_backlog_count().await?, 6);

        store.write_embeddings(&embedded(&keys[4..])).await?;
        assert_eq!(store.embed_backlog_count().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn session_message_counts_returns_per_session_counts_with_zeros_for_unknown_sessions()
    -> anyhow::Result<()> {
        // store_with_messages stripes `count` messages across 8 sessions
        // round-robin. 32 messages -> 4 per session, 0..8 deterministic.
        let temp = TempDir::new()?;
        let (store, _keys) = store_with_messages(&temp, 32).await?;

        let mut requested: Vec<String> = (0..8).map(|s| format!("session-{s}")).collect();
        requested.push("session-unknown-a".to_owned());
        requested.push("session-unknown-b".to_owned());
        let counts = store.session_message_counts(&requested).await?;

        // Map has an entry for every requested id (the contract): known
        // sessions hit 4, unknown sessions sit at 0.
        assert_eq!(counts.len(), requested.len());
        for s in 0..8 {
            assert_eq!(
                counts.get(&format!("session-{s}")).copied(),
                Some(4),
                "session-{s} should have 4 messages",
            );
        }
        assert_eq!(counts.get("session-unknown-a").copied(), Some(0));
        assert_eq!(counts.get("session-unknown-b").copied(), Some(0));

        // Empty input is the documented zero-path.
        let empty = store.session_message_counts(&[]).await?;
        assert!(empty.is_empty());
        Ok(())
    }
}
