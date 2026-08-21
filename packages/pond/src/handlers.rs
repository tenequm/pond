//! Transport-agnostic wire handlers (spec.md#protocol), one inner module per
//! operation.

fn map_error(error: crate::Error) -> crate::wire::ErrorEnvelope {
    error.into()
}

/// Typed identifier for the namespace a wire request targets. v1 is
/// single-namespace, so every successful resolve returns `root()`; the
/// type lets future multi-namespace routing land without churning call
/// sites (spec.md#wire-namespace-resolution).
#[derive(Debug, Clone)]
pub struct NamespaceIdent(pub Vec<String>);

impl NamespaceIdent {
    pub fn root() -> Self {
        Self(vec![])
    }
    pub fn as_table_id(&self, table_name: &str) -> Vec<String> {
        let mut id = self.0.clone();
        id.push(table_name.to_string());
        id
    }
}

/// The one and only namespace-resolution point; every wire handler funnels
/// through this. v1 accepts `None` or the default and returns the singleton
/// root namespace; everything else is a hard reject.
pub fn resolve_namespace(
    namespace: Option<&str>,
) -> Result<NamespaceIdent, crate::wire::ErrorEnvelope> {
    match namespace {
        None | Some(crate::wire::DEFAULT_NAMESPACE) => Ok(NamespaceIdent::root()),
        Some(other) => Err(map_error(crate::Error::namespace_unknown(other))),
    }
}

fn map_storage(error: anyhow::Error) -> crate::wire::ErrorEnvelope {
    // Classify before bucketing: an OCC commit-conflict exhaustion has its own
    // wire code (spec.md#protocol). Everything else lands in `storage_unavailable`.
    if let Some(conflict) = error.downcast_ref::<crate::substrate::ConflictExhausted>() {
        return map_error(crate::Error::Conflict {
            attempts: conflict.attempts,
        });
    }
    map_error(crate::Error::Storage(error))
}

mod ingest_handler {
    use anyhow::Result;
    use tokio_stream::StreamExt;

    use crate::{
        adapter::{Adapter, AdapterYield, SkipOracle, SkipReason},
        sessions::{IngestEvent, IngestSummary, IngestValidator, OutcomeStatus, RowOutcome, Store},
        wire::{
            ErrorBody, ErrorCode, IngestEnvelope, IngestRequest, IngestResponse, IngestResult,
            IngestStatus, validate_protocol,
        },
    };

    use super::{map_error, map_storage};

    /// Hard cap on events per `pond_ingest` batch (spec.md#protocol).
    pub const MAX_INGEST_EVENTS: usize = 1000;

    /// Progress signals emitted by [`ingest_adapter`] for the CLI bar (and
    /// any other observer). One [`SyncEvent::Discovered`] fires up front
    /// once `adapter.discover()` returns; then one [`SyncEvent::SessionDone`]
    /// fires per session as the validator commits it or the adapter skips
    /// it. The adapter path never errors at the event level - every
    /// per-session outcome is surfaced through this enum.
    #[derive(Debug, Clone)]
    pub enum SyncEvent {
        /// Up-front session count from `adapter.discover()`. Emitted exactly
        /// once before any `SessionDone`. When discovery fails, the field is
        /// `None` and the bar runs in rolling-counter mode.
        Discovered { total: Option<usize> },
        /// One session finished: committed, skipped (undecodable source),
        /// or rejected by the validator.
        SessionDone(SessionOutcome),
        /// Aggregate skip: one callback for N files (typically `Fresh`).
        SkippedBulk { status: SyncStatus, count: usize },
        /// A flush batch of `pending` staged sessions is about to embed + write:
        /// the slow phase during which no `SessionDone` fires. Lets the bar show
        /// the commit is in progress instead of freezing between drains.
        Flushing { pending: usize },
    }

    /// What happened to one session in an adapter-driven sync.
    #[derive(Debug, Clone)]
    pub struct SessionOutcome {
        /// Project/cwd the session ran in, when the adapter could parse it.
        pub project: Option<String>,
        /// Session id, when the source was decodable far enough to read one.
        /// `None` means the file was unreadable before any `Session` event.
        pub session_id: Option<String>,
        /// Messages observed in the source stream (not the same as rows
        /// written: validator-rejected sessions still report the count).
        pub messages: usize,
        pub status: SyncStatus,
    }

    /// Per-session outcome class.
    ///
    /// - `Ok` - committed cleanly, zero drops.
    /// - `Partial` - committed, but the validator dropped N events from this
    ///   session (per-event drop policy: bad-line skips, ordering violations,
    ///   duplicate ids). The non-bad events landed.
    /// - `Skipped` - the adapter couldn't extract a Session header from this
    ///   file at all (empty `.jsonl`, header corruption). Nothing written.
    /// - `Rejected` - the validator rejected the session at flush time on a
    ///   Session-level invariant (`source_agent` / `project` immutability).
    ///   The substream is dropped wholesale. This is the rare case where the
    ///   *whole* session is lost; for everything else use `Partial`.
    #[derive(Debug, Clone)]
    pub enum SyncStatus {
        Ok,
        Partial {
            dropped_events: usize,
            /// First drop's error message; subsequent drops counted, not
            /// retained. Full detail at `-vv` (debug) verbosity.
            first_drop_reason: Option<String>,
        },
        Skipped {
            reason: String,
        },
        Rejected {
            reason: String,
        },
        /// Per-session staleness skip (spec.md#adapter-integrity-event-ordering): adapter short-circuited
        /// the file decode because `mtime < MAX(messages.timestamp)`.
        Fresh,
        /// File produced no importable session (empty `.jsonl`, sidecar-only
        /// rows, or an unextractable header). Benign: counted in
        /// `skipped_empty`, never an error or a drop.
        Empty,
        /// Session present in more than one source form; this copy is
        /// superseded by an authoritative copy the same run ingests (e.g.
        /// opencode's legacy tree copy of a DB-resident session). Content
        /// identity is not verified - supersession is by session id (the
        /// source's documented migration contract). Counted in
        /// `skipped_superseded`, never folded into `Empty`.
        Superseded,
    }

    #[derive(Debug, Default)]
    struct InFlight {
        project: Option<String>,
        session_id: String,
        messages: usize,
        /// Events the adapter dropped mid-stream (skip-bad-line) that belong
        /// to this in-flight session. Summed with the validator's per-event
        /// drops at flush time to compute the final `SyncStatus::Partial`
        /// count.
        dropped_events: usize,
        first_drop_reason: Option<String>,
        /// The `index` value used when the Session event was pushed to the
        /// validator. After batched flush, `RowOutcome.index` lets us match
        /// per-session outcomes back to the originating session.
        session_index: usize,
    }

    /// One session that has been fully observed but whose write hasn't
    /// completed yet (queued in the validator's batched-flush buffer).
    /// Emitted as `SyncEvent::SessionDone` after the corresponding flush
    /// returns its outcomes.
    #[derive(Debug)]
    struct PendingDone {
        project: Option<String>,
        session_id: String,
        messages: usize,
        dropped_events: usize,
        first_drop_reason: Option<String>,
        session_index: usize,
    }

    /// Batch size used by the adapter ingest loop: flush every N completed
    /// substreams to amortize per-commit cost. 100 is the value validated in
    /// `benches/ingest_bench.rs` against the measured profile (substream
    /// flushes were 78-88% of wall time at batch=1; ~25x fewer commits at
    /// batch=100 closes most of that gap). Memory bound: ~N x (avg events
    /// per session) staged in RAM, ~tens of MB at this scale.
    const ADAPTER_FLUSH_BATCH: usize = 100;

    /// Drain `adapter.events()` into `store`, accumulating an [`IngestSummary`]
    /// and reporting progress through `on_event`. The adapter path is
    /// CLI-driven (`pond sync`) and reports aggregates, not per-row results -
    /// the wire-level [`pond_ingest`] handler keeps the per-row contract for
    /// HTTP clients.
    ///
    /// Undecodable session substreams are skipped, not warned: the design
    /// contract (no silent drops) is met by surfacing each skip through
    /// `on_event` as [`SyncStatus::Skipped`]. The tracing line stays available
    /// at DEBUG for deep-debug; default verbosity is silent.
    pub async fn ingest_adapter<F>(
        store: &Store,
        adapter: &dyn Adapter,
        oracle: &dyn SkipOracle,
        mut on_event: F,
    ) -> Result<IngestSummary>
    where
        F: FnMut(SyncEvent),
    {
        let mut summary = IngestSummary::default();
        let truncations_before = crate::adapter::extract::truncated_values_count();
        // Discovery is best-effort: a failure (no read perm, bad config)
        // still lets the bar run as a rolling counter. We surface the count
        // upfront when we can; otherwise the bar uses `set_length(0)`.
        let discover_started = std::time::Instant::now();
        let total = adapter
            .discover()
            .await
            .map_err(|error| tracing::debug!(%error, "adapter discover failed"))
            .ok();
        tracing::debug!(target: "pond::perf", stage = "discover", elapsed_ms = discover_started.elapsed().as_millis() as u64, total = total.unwrap_or(0), "sync stage");
        on_event(SyncEvent::Discovered { total });

        let mut events = adapter.events_with(oracle);
        let mut validator = IngestValidator::default();
        // Adapter events have no stable input index (they stream from disk);
        // assign a monotonic counter so RowOutcome.index stays unique even
        // though the values aren't surfaced anywhere.
        let mut index = 0usize;
        let mut in_flight: Option<InFlight> = None;
        // Sessions whose end-of-stream we've observed but whose write is
        // still pending in the validator's batch buffer. Drained in FIFO
        // order against `validator.flush()`'s outcome stream.
        let mut pending_dones: std::collections::VecDeque<PendingDone> =
            std::collections::VecDeque::new();
        // Perf probe accumulators. Logged once at the end of the run at `-v`
        // (info) verbosity so a single sync emits one tidy summary plus
        // per-merge_insert lines from substrate. Visible only at INFO; never
        // affects normal output.
        let mut decode_total = std::time::Duration::ZERO;
        let mut decode_count = 0u64;
        let mut validator_total = std::time::Duration::ZERO;
        let mut validator_count = 0u64;
        let run_started = std::time::Instant::now();

        loop {
            let decode_start = std::time::Instant::now();
            let next = events.next().await;
            decode_total += decode_start.elapsed();
            decode_count += 1;
            let event = match next {
                Some(event) => event,
                None => break,
            };
            match event {
                Ok(AdapterYield::Skipped {
                    session_id,
                    project,
                    reason,
                }) => {
                    let status = match reason {
                        SkipReason::Fresh => {
                            summary.skipped_fresh += 1;
                            SyncStatus::Fresh
                        }
                        SkipReason::Empty => {
                            summary.skipped_empty += 1;
                            SyncStatus::Empty
                        }
                        SkipReason::Superseded => {
                            summary.skipped_superseded += 1;
                            SyncStatus::Superseded
                        }
                        SkipReason::Unsupported(reason) => {
                            summary.skipped_files += 1;
                            SyncStatus::Skipped { reason }
                        }
                    };
                    on_event(SyncEvent::SessionDone(SessionOutcome {
                        project,
                        session_id,
                        messages: 0,
                        status,
                    }));
                }
                Ok(AdapterYield::SkippedBatch { reason, count }) => {
                    let status = match reason {
                        SkipReason::Fresh => {
                            summary.skipped_fresh += count;
                            SyncStatus::Fresh
                        }
                        SkipReason::Empty => {
                            summary.skipped_empty += count;
                            SyncStatus::Empty
                        }
                        SkipReason::Superseded => {
                            summary.skipped_superseded += count;
                            SyncStatus::Superseded
                        }
                        SkipReason::Unsupported(reason) => {
                            summary.skipped_files += count;
                            SyncStatus::Skipped { reason }
                        }
                    };
                    on_event(SyncEvent::SkippedBulk { status, count });
                }
                Ok(AdapterYield::Event(event)) => {
                    // A new Session means the current one is being closed
                    // out by the validator (moved to its `completed` buffer
                    // for batched flush). Stage the PendingDone so we can
                    // emit SessionDone with proper status after flush.
                    if matches!(&event, IngestEvent::Session(_))
                        && let Some(prev) = in_flight.take()
                    {
                        pending_dones.push_back(PendingDone {
                            project: prev.project,
                            session_id: prev.session_id,
                            messages: prev.messages,
                            dropped_events: prev.dropped_events,
                            first_drop_reason: prev.first_drop_reason,
                            session_index: prev.session_index,
                        });
                    }
                    let event_index = index;
                    match &event {
                        IngestEvent::Session(session) => {
                            in_flight = Some(InFlight {
                                project: Some((*session.project).clone()),
                                session_id: session.id.clone(),
                                messages: 0,
                                dropped_events: 0,
                                first_drop_reason: None,
                                session_index: event_index,
                            });
                        }
                        IngestEvent::Message(_) => {
                            if let Some(slot) = in_flight.as_mut() {
                                slot.messages += 1;
                            }
                        }
                        IngestEvent::Part(_) => {}
                    }

                    let validator_start = std::time::Instant::now();
                    let push_outcomes = validator.push(store, index, event).await?;
                    validator_total += validator_start.elapsed();
                    validator_count += 1;
                    // Per-event drops returned synchronously by push (ordering
                    // / dup-id violations) attribute to the in-flight
                    // session's drop count. Session-level errors (e.g. empty
                    // source_agent) come back here too; we don't currently
                    // distinguish them - they're rare and end up in
                    // `summary.dropped_events`.
                    for outcome in &push_outcomes {
                        if matches!(outcome.status, OutcomeStatus::Error)
                            && outcome.kind != "session"
                            && let Some(slot) = in_flight.as_mut()
                        {
                            slot.dropped_events += 1;
                            if slot.first_drop_reason.is_none() {
                                slot.first_drop_reason =
                                    outcome.error.as_ref().map(|err| err.message.clone());
                            }
                        }
                    }
                    summary.add_outcomes(&push_outcomes);
                    index += 1;

                    // Drain the batch periodically. The validator's
                    // `pending_substreams()` count grows by one each time we
                    // close a substream; once it hits the batch threshold we
                    // commit them in one parallel 3-table merge_insert.
                    if validator.pending_substreams() >= ADAPTER_FLUSH_BATCH {
                        on_event(SyncEvent::Flushing {
                            pending: validator.pending_substreams(),
                        });
                        let flush_start = std::time::Instant::now();
                        let (flush_outcomes, flush_counts) = validator.flush(store).await?;
                        validator_total += flush_start.elapsed();
                        validator_count += 1;
                        // Counts come from the pre-existence sweep inside the
                        // flush, not from per-row outcomes (which would
                        // double-count if we also called `add_outcomes`).
                        summary.add_outcomes_errors_only(&flush_outcomes);
                        summary.add_batch(&flush_counts);
                        drain_pending_dones(&mut pending_dones, &flush_outcomes, &mut on_event);
                    }
                }
                Err(error) => {
                    // Per-event drop semantics: the adapter's error is either
                    // a pre-Session header failure (whole file unusable) or a
                    // mid-session bad-line skip. The validator is not reset
                    // on either case so subsequent good lines from the same
                    // file still land.
                    tracing::debug!(
                        %error,
                        "adapter event error (per-line drop by design)"
                    );
                    match in_flight.as_mut() {
                        Some(slot) => {
                            // Mid-session bad line. Charge one dropped event
                            // to this session; the bar will render the per-
                            // session summary at SessionDone time.
                            slot.dropped_events += 1;
                            if slot.first_drop_reason.is_none() {
                                slot.first_drop_reason = Some(error.to_string());
                            }
                            summary.dropped_events += 1;
                        }
                        None => {
                            // Pre-Session decode failure: no in-flight
                            // session to attribute to. This is a whole-file
                            // skip - surface it as a SessionDone with
                            // session_id=None and status=Skipped.
                            summary.skipped_files += 1;
                            on_event(SyncEvent::SessionDone(SessionOutcome {
                                project: None,
                                session_id: None,
                                messages: 0,
                                status: SyncStatus::Skipped {
                                    reason: error.to_string(),
                                },
                            }));
                        }
                    }
                }
            }
        }

        if let Some(prev) = in_flight.take() {
            pending_dones.push_back(PendingDone {
                project: prev.project,
                session_id: prev.session_id,
                messages: prev.messages,
                dropped_events: prev.dropped_events,
                first_drop_reason: prev.first_drop_reason,
                session_index: prev.session_index,
            });
        }
        if validator.pending_substreams() > 0 {
            on_event(SyncEvent::Flushing {
                pending: validator.pending_substreams(),
            });
        }
        let validator_start = std::time::Instant::now();
        let (final_outcomes, final_counts) = validator.finish(store).await?;
        validator_total += validator_start.elapsed();
        validator_count += 1;
        summary.add_outcomes_errors_only(&final_outcomes);
        summary.add_batch(&final_counts);
        drain_pending_dones(&mut pending_dones, &final_outcomes, &mut on_event);

        summary.truncated_values = crate::adapter::extract::truncated_values_count()
            .saturating_sub(truncations_before) as usize;

        let total = run_started.elapsed();
        let other = total
            .saturating_sub(decode_total)
            .saturating_sub(validator_total);
        tracing::info!(
            target: "pond::perf",
            total_ms = total.as_millis() as u64,
            decode_ms = decode_total.as_millis() as u64,
            validator_ms = validator_total.as_millis() as u64,
            other_ms = other.as_millis() as u64,
            decode_calls = decode_count,
            validator_calls = validator_count,
            rows_inserted = summary.inserted as u64,
            rows_matched = summary.matched as u64,
            dropped_events = summary.dropped_events as u64,
            dropped_sessions = summary.dropped_sessions as u64,
            skipped_files = summary.skipped_files as u64,
            skipped_fresh = summary.skipped_fresh as u64,
            skipped_superseded = summary.skipped_superseded as u64,
            truncated_values = summary.truncated_values as u64,
            "ingest_adapter complete"
        );
        Ok(summary)
    }

    /// Match the validator's flush outcomes back to the queued PendingDone
    /// entries (FIFO; `RowOutcome.index` aligns with `PendingDone.session_index`).
    /// Each matched PendingDone yields one `SyncEvent::SessionDone`. The queue
    /// drains in order; if outcomes are missing for any (shouldn't happen with
    /// a well-formed validator path), the SessionDone is emitted as Ok using
    /// only the adapter-side drop count.
    fn drain_pending_dones<F>(
        queue: &mut std::collections::VecDeque<PendingDone>,
        outcomes: &[RowOutcome],
        on_event: &mut F,
    ) where
        F: FnMut(SyncEvent),
    {
        // Index session-kind outcomes by their `index` value so we can look
        // them up by `session_index` regardless of relative ordering.
        let mut session_outcome_by_index: std::collections::HashMap<usize, &RowOutcome> =
            std::collections::HashMap::new();
        for outcome in outcomes {
            if outcome.kind == "session" {
                session_outcome_by_index.insert(outcome.index, outcome);
            }
        }

        while let Some(done) = queue.pop_front() {
            let session_outcome = session_outcome_by_index.get(&done.session_index).copied();
            let rejection_reason = session_outcome.and_then(|outcome| {
                if matches!(outcome.status, OutcomeStatus::Error) {
                    Some(
                        outcome
                            .error
                            .as_ref()
                            .map(|err| err.message.clone())
                            .unwrap_or_else(|| "session-level rejection".to_owned()),
                    )
                } else {
                    None
                }
            });
            let status = if let Some(reason) = rejection_reason {
                SyncStatus::Rejected { reason }
            } else if done.dropped_events > 0 {
                SyncStatus::Partial {
                    dropped_events: done.dropped_events,
                    first_drop_reason: done.first_drop_reason,
                }
            } else {
                SyncStatus::Ok
            };
            on_event(SyncEvent::SessionDone(SessionOutcome {
                project: done.project,
                session_id: Some(done.session_id),
                messages: done.messages,
                status,
            }));
        }
    }

    /// The `pond_ingest` wire handler (spec.md#protocol): validate the transport
    /// envelope, then drive the event batch through [`ingest_events`]. Transport
    /// failures (bad protocol, unknown namespace, empty or oversized batch) fail
    /// the whole request via the spec.md#protocol; per-event failures land
    /// in the response's `results[]` with `status: "error"`.
    pub async fn pond_ingest(store: &Store, request: IngestRequest) -> IngestEnvelope {
        if let Err(envelope) = validate_protocol(request.protocol_version) {
            return IngestEnvelope::Error(envelope);
        }
        if let Err(envelope) = super::resolve_namespace(request.namespace.as_deref()) {
            return IngestEnvelope::Error(envelope);
        }
        if request.events.is_empty() {
            return IngestEnvelope::Error(map_error(crate::Error::validation_field(
                "events must be a non-empty array",
                "events",
                Some(serde_json::json!([])),
                Some("non-empty array".to_owned()),
            )));
        }
        if request.events.len() > MAX_INGEST_EVENTS {
            return IngestEnvelope::Error(map_error(crate::Error::validation_field(
                format!("ingest batch exceeds the event cap: at most {MAX_INGEST_EVENTS} events"),
                "events",
                Some(serde_json::json!(request.events.len())),
                Some(format!("at most {MAX_INGEST_EVENTS} events")),
            )));
        }

        match ingest_events(store, request.events).await {
            Ok(outcomes) => {
                let mut accepted = 0;
                let mut rejected = 0;
                for outcome in &outcomes {
                    match outcome.status {
                        OutcomeStatus::Inserted | OutcomeStatus::Matched => accepted += 1,
                        OutcomeStatus::Error => rejected += 1,
                    }
                }
                let results = outcomes
                    .into_iter()
                    .map(outcome_to_result)
                    .collect::<Vec<_>>();
                IngestEnvelope::Success(IngestResponse {
                    accepted,
                    rejected,
                    results,
                })
            }
            Err(failure) => IngestEnvelope::Error(map_storage(failure)),
        }
    }

    /// Drive a flat event batch through [`IngestValidator`], returning per-row
    /// outcomes in input-array order. A substream that fails validation has
    /// every one of its events tagged with [`OutcomeStatus::Error`] (the
    /// offending event and any others in the same substream); ingest of later
    /// sessions in the batch continues (spec.md#protocol).
    pub async fn ingest_events(store: &Store, events: Vec<IngestEvent>) -> Result<Vec<RowOutcome>> {
        let mut validator = IngestValidator::default();
        let mut outcomes = Vec::with_capacity(events.len());
        for (index, event) in events.into_iter().enumerate() {
            let mut chunk = validator.push(store, index, event).await?;
            outcomes.append(&mut chunk);
        }
        // HTTP wire path keeps using per-row outcomes for `IngestResult`;
        // the batch counts are CLI-only.
        let (mut tail, _counts) = validator.finish(store).await?;
        outcomes.append(&mut tail);
        outcomes.sort_by_key(|outcome| outcome.index);
        Ok(outcomes)
    }

    fn outcome_to_result(outcome: RowOutcome) -> IngestResult {
        let (status, error) = match (outcome.status, outcome.error) {
            (OutcomeStatus::Inserted, _) => (IngestStatus::Inserted, None),
            (OutcomeStatus::Matched, _) => (IngestStatus::Matched, None),
            (OutcomeStatus::Error, error) => {
                let body = error
                    .map(|err| {
                        let mut details = serde_json::Map::new();
                        if let Some(field) = err.field {
                            details.insert("field".to_owned(), serde_json::json!(field));
                        }
                        if let Some(reason) = err.reason {
                            details.insert("reason".to_owned(), serde_json::json!(reason));
                        }
                        ErrorBody {
                            code: ErrorCode::ValidationFailed,
                            message: err.message,
                            details: serde_json::Value::Object(details),
                        }
                    })
                    .unwrap_or_else(|| ErrorBody {
                        code: ErrorCode::ValidationFailed,
                        message: "ingest failed".to_owned(),
                        details: serde_json::json!({}),
                    });
                (IngestStatus::Error, Some(body))
            }
        };
        IngestResult {
            index: outcome.index,
            kind: outcome.kind.to_owned(),
            pk: outcome.pk,
            status,
            error,
        }
    }
}

pub use crate::sessions::{IngestEvent, IngestSummary, IngestValidator, search_text};
pub use ingest_handler::{
    MAX_INGEST_EVENTS, SessionOutcome, SyncEvent, SyncStatus, ingest_adapter, ingest_events,
    pond_ingest,
};

mod export_handler {
    //! `pond_export` (spec.md#protocol): walk every session in the store and
    //! emit its canonical event stream as JSONL - one `IngestEvent` per line.
    //! The output is byte-identical with what `pond ingest` / `pond_ingest`
    //! accepts on input, so `export | ingest` is a portable backup loop.
    //! Sessions are emitted in lexicographic id order; within each session,
    //! messages run in `(timestamp, message_id)` order and each message's
    //! parts immediately follow in `ordinal` order. Matches the
    //! spec.md#adapter-integrity-event-ordering ordering contract so the output
    //! re-imports without re-ordering.

    use anyhow::{Context, Result};
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    use crate::sessions::{IngestEvent, Store};

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct ExportSummary {
        pub sessions: usize,
        pub messages: usize,
        pub parts: usize,
    }

    pub async fn pond_export<W>(
        store: &Store,
        session_filter: Option<&str>,
        writer: &mut W,
    ) -> Result<ExportSummary>
    where
        W: AsyncWrite + Unpin,
    {
        let mut session_ids = match session_filter {
            Some(id) => vec![id.to_owned()],
            None => store.session_ids().await?,
        };
        session_ids.sort();

        let mut summary = ExportSummary::default();
        for session_id in session_ids {
            let Some(stored) = store
                .get_session(&session_id)
                .await
                .with_context(|| format!("export: failed to load session {session_id}"))?
            else {
                if session_filter.is_some() {
                    anyhow::bail!("export: session not found: {session_id}");
                }
                continue;
            };
            write_event(writer, &IngestEvent::Session(stored.session)).await?;
            summary.sessions += 1;
            for message_with_parts in stored.messages {
                write_event(writer, &IngestEvent::Message(message_with_parts.message)).await?;
                summary.messages += 1;
                for part in message_with_parts.parts {
                    write_event(writer, &IngestEvent::Part(part)).await?;
                    summary.parts += 1;
                }
            }
        }
        writer.flush().await.context("export: flush failed")?;
        Ok(summary)
    }

    async fn write_event<W>(writer: &mut W, event: &IngestEvent) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let line = serde_json::to_string(event).context("export: serialize event")?;
        writer
            .write_all(line.as_bytes())
            .await
            .context("export: write event")?;
        writer
            .write_all(b"\n")
            .await
            .context("export: write newline")?;
        Ok(())
    }
}

pub use export_handler::{ExportSummary, pond_export};

mod restore_handler {
    //! `restore_lineage` (spec.md#adapter-lineage-complete-restore): collect the named
    //! session plus its direct subagent children for the `pond copy` restore
    //! path. The spawn graph is one level deep; a collected
    //! child that is itself a parent means a deeper graph, which is a typed
    //! error - never a silently flattened restore.

    use anyhow::{Context, Result};

    use crate::sessions::{SessionWithMessages, Store};

    /// The two ways a lineage can be unanswerable, separated from io failure so
    /// a CLI can map them to distinct exit codes without matching on prose.
    #[derive(Debug)]
    pub enum Lineage {
        /// The named session plus its direct children, in that order.
        Complete(Vec<SessionWithMessages>),
        /// Nothing is stored under that id (erased and denylisted sessions
        /// report the same way - they are simply not there).
        NotFound,
        /// A child is itself a parent, so restoring would either flatten the
        /// graph or write only part of it.
        TooDeep { child_id: String },
    }

    pub async fn restore_lineage(store: &Store, session_id: &str) -> Result<Lineage> {
        let Some(parent) = store.get_session(session_id).await? else {
            return Ok(Lineage::NotFound);
        };
        let children = store.child_sessions(session_id).await?;
        // The grandchild probes are independent of each other; on a remote
        // store each is a round trip, so they run together rather than in
        // series behind K awaits.
        let deeper = futures::future::try_join_all(
            children
                .iter()
                .map(|child| async { store.child_sessions(&child.id).await }),
        )
        .await?;
        if let Some(child) = children
            .iter()
            .zip(&deeper)
            .find(|(_, grandchildren)| !grandchildren.is_empty())
            .map(|(child, _)| child)
        {
            return Ok(Lineage::TooDeep {
                child_id: child.id.clone(),
            });
        }

        let mut sessions = vec![parent];
        for child in children {
            let child_id = child.id;
            // A child that lists but will not load is a broken lineage, never a
            // session to quietly leave out of the restore.
            let stored = store
                .get_session(&child_id)
                .await?
                .with_context(|| format!("child session disappeared: {child_id}"))?;
            sessions.push(stored);
        }
        Ok(Lineage::Complete(sessions))
    }
}

pub use restore_handler::{Lineage, restore_lineage};

mod get_handler {
    use crate::{
        sessions::{GetLookup, MessageViewParams, RetrievedMessage, SessionViewParams, Store},
        wire::{
            GetEnvelope, GetMessageRequest, GetResponse, GetResult, GetSession, GetSessionRequest,
            MessageView, PartSummary, ResponsePart, validate_protocol,
        },
    };

    use super::{map_error, map_storage};

    /// Project canonical retrieval data into the conversational response DTO:
    /// `text`/`content` plus one-line part summaries. Full part bodies are never
    /// inlined here - they ride `GetResult::Message.target_parts`, reached by
    /// `message_id` scope.
    fn to_message_view(message: RetrievedMessage) -> MessageView {
        let parts_summary = message
            .parts
            .iter()
            .filter_map(|part| PartSummary::for_kind(&part.kind))
            .collect();
        MessageView {
            id: message.id,
            role: message.role,
            timestamp: message.timestamp,
            text: message.text,
            content: message.content,
            parts_summary,
        }
    }

    /// Server response budget, sized to the declared
    /// `_meta["anthropic/maxResultSizeChars"]` cap (~200KB / ~50k tokens). The
    /// server stops adding messages (or parts) when the next would exceed it;
    /// `before_remaining` / `after_remaining` (session) and
    /// `target_parts_remaining` (message) then signal pagination.
    const BUDGET_BYTES: usize = 200_000;

    pub async fn pond_get_session(store: &Store, request: GetSessionRequest) -> GetEnvelope {
        if let Err(error) = validate_protocol(request.protocol_version) {
            return GetEnvelope::Error(error);
        }
        if let Err(envelope) = super::resolve_namespace(request.namespace.as_deref()) {
            return GetEnvelope::Error(envelope);
        }
        match session_result(store, &request).await {
            Ok(response) => GetEnvelope::Success(response),
            Err(error) => GetEnvelope::Error(error),
        }
    }

    pub async fn pond_get_message(store: &Store, request: GetMessageRequest) -> GetEnvelope {
        if let Err(error) = validate_protocol(request.protocol_version) {
            return GetEnvelope::Error(error);
        }
        if let Err(envelope) = super::resolve_namespace(request.namespace.as_deref()) {
            return GetEnvelope::Error(envelope);
        }
        match message_result(store, &request).await {
            Ok(response) => GetEnvelope::Success(response),
            Err(error) => GetEnvelope::Error(error),
        }
    }

    /// Map a stale/unknown pagination anchor to a `validation_failed` naming
    /// the field and the fix (spec.md#protocol).
    fn unknown_anchor(field: &str, value: Option<&str>) -> crate::wire::ErrorEnvelope {
        map_error(crate::Error::validation_field(
            format!("{field} not found (stale or mistyped pagination anchor)"),
            field,
            value.map(|v| serde_json::Value::String(v.to_owned())),
            Some("a message id from a prior page of this read".to_owned()),
        ))
    }

    async fn session_result(
        store: &Store,
        request: &GetSessionRequest,
    ) -> Result<GetResponse, crate::wire::ErrorEnvelope> {
        if request.after_message_id.is_some() && request.before_message_id.is_some() {
            return Err(map_error(crate::Error::validation_field(
                "after_message_id and before_message_id are mutually exclusive",
                "before_message_id",
                request
                    .before_message_id
                    .clone()
                    .map(serde_json::Value::String),
                Some("set only one pagination anchor".to_owned()),
            )));
        }
        let params = SessionViewParams {
            at_message_id: None,
            after_message_id: request.after_message_id.as_deref(),
            before_message_id: request.before_message_id.as_deref(),
            limit: request.limit,
            budget_bytes: BUDGET_BYTES,
            session_from: request.from,
        };
        let (session_id, resolved_from) = match store
            .session_view(&request.id, params.clone())
            .await
            .map_err(map_storage)?
        {
            GetLookup::NotFound => {
                // The id may be a message id: resolve it up to its parent
                // session (intent is unambiguous - the caller asked for a
                // session) and anchor the page at that message.
                match store
                    .session_id_for_message(&request.id)
                    .await
                    .map_err(map_storage)?
                {
                    Some(session_id) => (session_id, Some(request.id.clone())),
                    None => {
                        return Err(map_error(crate::Error::not_found(
                            "session",
                            serde_json::json!(request.id),
                            format!(
                                "session not found: {} (not a message id either)",
                                request.id
                            ),
                        )));
                    }
                }
            }
            GetLookup::UnknownAnchor => {
                let (field, value) = match &request.after_message_id {
                    Some(value) => ("after_message_id", Some(value.as_str())),
                    None => ("before_message_id", request.before_message_id.as_deref()),
                };
                return Err(unknown_anchor(field, value));
            }
            GetLookup::Found(view) => {
                return Ok(session_response(view, None));
            }
        };
        let anchored = SessionViewParams {
            at_message_id: resolved_from.as_deref(),
            ..params
        };
        match store
            .session_view(&session_id, anchored)
            .await
            .map_err(map_storage)?
        {
            GetLookup::Found(view) => Ok(session_response(view, resolved_from)),
            // The parent session of a stored message always exists; anchor
            // misses degrade inside session_view, never to UnknownAnchor.
            GetLookup::NotFound | GetLookup::UnknownAnchor => Err(map_error(
                crate::Error::internal("resolved parent session lookup failed"),
            )),
        }
    }

    fn session_response(
        view: crate::sessions::SessionPage,
        resolved_from_message_id: Option<String>,
    ) -> GetResponse {
        GetResponse {
            session: GetSession::from_session(&view.session),
            result: GetResult::Session {
                messages: view.messages.into_iter().map(to_message_view).collect(),
                before_remaining: view.before_remaining,
                after_remaining: view.after_remaining,
                resolved_from_message_id,
            },
        }
    }

    async fn message_result(
        store: &Store,
        request: &GetMessageRequest,
    ) -> Result<GetResponse, crate::wire::ErrorEnvelope> {
        let message_id = &request.id;
        let params = MessageViewParams {
            context_before: request.context_before,
            context_after: request.context_after,
            budget_bytes: BUDGET_BYTES,
        };
        let view = match store
            .message_view(message_id, params)
            .await
            .map_err(map_storage)?
        {
            GetLookup::NotFound => {
                // A session id cannot resolve to one message (which one?), so
                // teach at the failure point instead of resolving.
                if store
                    .find_session(message_id)
                    .await
                    .map_err(map_storage)?
                    .is_some()
                {
                    return Err(map_error(crate::Error::validation_field(
                        format!(
                            "{message_id} is a session id, not a message id - read it with \
                             pond_get_session (CLI: pond get-session), or pass a message id \
                             from its transcript"
                        ),
                        "id",
                        Some(serde_json::Value::String(message_id.clone())),
                        Some("a message id from a search hit or transcript".to_owned()),
                    )));
                }
                return Err(map_error(crate::Error::not_found(
                    "message",
                    serde_json::json!(message_id),
                    format!("message not found: {message_id}"),
                )));
            }
            // message scope has no pagination anchor, so this is unreachable.
            GetLookup::UnknownAnchor => {
                return Err(map_error(crate::Error::internal(
                    "message_view returned UnknownAnchor for an anchorless lookup",
                )));
            }
            GetLookup::Found(view) => view,
        };
        // The target's body rides `target_parts` (full); carrying `text`/
        // `content` on the header too would just duplicate it.
        let target = MessageView {
            id: view.target.id,
            role: view.target.role,
            timestamp: view.target.timestamp,
            text: None,
            content: None,
            parts_summary: Vec::new(),
        };
        Ok(GetResponse {
            session: GetSession::from_session(&view.session),
            result: GetResult::Message {
                target,
                target_parts: view
                    .target_parts
                    .into_iter()
                    .map(ResponsePart::from_part)
                    .collect(),
                target_parts_remaining: view.target_parts_remaining,
                siblings: view.siblings.into_iter().map(to_message_view).collect(),
                context_before: request.context_before,
                context_after: request.context_after,
            },
        })
    }
}

pub use get_handler::{pond_get_message, pond_get_session};

mod search_handler {
    //! The `pond_search` handler: single-arm retrieval at message granularity -
    //! `vector` (kNN, default) or `fts` (BM25), chosen per query, no fusion -
    //! with filter pushdown and session-grouped responses (spec.md#search).

    use crate::{
        Clock, SystemClock,
        embed::{Embedder, LazyEmbedder, format_query},
        sessions::{MessageKey, MessageMeta, SearchHit, Store},
        substrate::{Predicate, ScalarValue},
        wire::{
            ErrorEnvelope, PartSummary, ProjectFilter, Role, SearchEnvelope, SearchFilters,
            SearchRequest, SearchResponse, SearchResult, SearchSession, SortBy, validate_protocol,
        },
    };
    use chrono::{DateTime, NaiveDate, Utc};
    use std::collections::HashMap;

    use super::{map_error, map_storage};

    /// Internal retrieval arm. The caller picks per query via the wire `mode`
    /// field (`pond search --mode`): `Vector` (default) on meaning, `Fts` on
    /// exact whole words. There is no hybrid fusion - one arm per request. A
    /// `Vector` request degrades to `Fts` when the store has no embeddings.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SearchMode {
        Fts,
        Vector,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SearchPlan {
        pub mode: SearchMode,
        pub query: String,
        /// User filters only. Drives both the arms and `searchable_in_scope`;
        /// empty for an unfiltered search so the count reads the FTS `num_docs`
        /// stat, not the search_text scan. The subagent exclusion is applied
        /// in-memory (`exclude_subagents`), not as a SQL clause, so the arms stay
        /// index-only - no `source_agent` materialization on a remote store.
        pub filter: Predicate,
        pub filters: SearchFilters,
        pub sort_by: SortBy,
        pub pool: usize,
        pub vector_pool: usize,
        pub limit: usize,
        /// Drop subagent hits (session id contains `/`) from the arm results in
        /// memory - the spec.md#search retrieval default. See `plan_search` for
        /// when it is on.
        pub exclude_subagents: bool,
    }

    const LIMIT_CAP: usize = 200;
    /// Centered query-windowed body returned on every hit (spec.md#search).
    /// Calibrated for the agent-context budget: ~600 code points fits a typical
    /// match site without crowding the 10k-token `pond_get_session` page.
    const HIT_SNIPPET_CHARS: usize = 600;

    /// Recency tiebreaker for `vector` + `relevance` ordering (spec.md#search):
    /// an additive bonus of up to [`RECENCY_BOOST_MAGNITUDE`] that decays
    /// exponentially over [`RECENCY_BOOST_SCALE_DAYS`]. It is a gentle post-gate
    /// nudge so that, among comparably-relevant hits, the more recent
    /// conversation wins - it must NOT lift an off-topic recent hit over a
    /// strongly-relevant old one.
    ///
    /// Magnitude is deliberately small. e5 cosines for relevant content cluster
    /// tightly (~0.78-0.86), so the typical relevance gap between the right
    /// answer and a near-miss is only ~0.02-0.05. The boost must stay below
    /// that band or it swamps relevance and tanks recall - measured on
    /// ops/search-benchmarks/queries-en.tsv, magnitude 0.1 collapsed
    /// Success@3 from 0.33 (no boost) to 0.10; 0.02 keeps it tie-breaking.
    const RECENCY_BOOST_MAGNITUDE: f64 = 0.02;
    const RECENCY_BOOST_SCALE_DAYS: f64 = 30.0;

    /// Run a single-arm search. The caller picks the arm via the wire `mode`
    /// field; `vector` degrades to `fts` only when the store has no embeddings.
    /// The embedder is `LazyEmbedder`-loaded on the first vector call, so
    /// fts-only corpora never pay the model load. The response has no top-level
    /// mode field; retriever attribution stays in `explain_search_plan`.
    ///
    /// Must run on a multi-threaded Tokio runtime: the vector arm embeds the
    /// query via `block_in_place`, which panics on a `current_thread` runtime.
    pub async fn pond_search(
        store: &Store,
        embedder: &LazyEmbedder,
        request: SearchRequest,
        search: &crate::config::SearchConfig,
    ) -> SearchEnvelope {
        match run_search(store, embedder, request, search, &SystemClock).await {
            Ok(response) => SearchEnvelope::Success(response),
            Err(envelope) => SearchEnvelope::Error(envelope),
        }
    }

    pub async fn explain_search_plan(
        store: &Store,
        embedder: &LazyEmbedder,
        request: SearchRequest,
        search: &crate::config::SearchConfig,
    ) -> Result<String, ErrorEnvelope> {
        let mut plan = plan_search(request)?;
        plan.mode = resolve_effective_mode(store, plan.mode).await?;
        let mut out = String::new();
        match plan.mode {
            SearchMode::Fts => {
                let fts = store
                    .explain_fts_plan(&plan.query, plan.pool, &plan.filter)
                    .await
                    .map_err(map_storage)?;
                out.push_str("fts:\n");
                out.push_str(&fts);
                out.push('\n');
            }
            SearchMode::Vector => {
                let backend = load_embedder(embedder).await?;
                let vector = embed_query(backend.as_ref(), &plan.query)?;
                let vector_plan = store
                    .explain_vector_plan(&vector, plan.vector_pool, &plan.filter, Some(search))
                    .await
                    .map_err(map_storage)?;
                out.push_str("vector:\n");
                out.push_str(&vector_plan);
                out.push('\n');
            }
        }
        Ok(out)
    }

    async fn run_search(
        store: &Store,
        embedder: &LazyEmbedder,
        request: SearchRequest,
        search: &crate::config::SearchConfig,
        clock: &dyn Clock,
    ) -> Result<SearchResponse, ErrorEnvelope> {
        // Per-stage timing for the search hot path. `pond::perf=debug` surfaces
        // it; off, each call is a no-op. Cumulative from request start.
        let stage_start = std::time::Instant::now();
        macro_rules! stage {
            ($label:literal) => {
                tracing::debug!(
                    target: "pond::perf",
                    stage = $label,
                    elapsed_ms = stage_start.elapsed().as_millis() as u64,
                );
            };
        }
        let mut plan = plan_search(request)?;

        // A `vector` request degrades to `fts` when the store has no
        // embeddings (nothing to match against); `fts` stays `fts`.
        plan.mode = resolve_effective_mode(store, plan.mode).await?;
        stage!("resolve_mode");

        // The scope count (spec.md#search-absence-honesty: how many searchable
        // messages the filters left in scope, so "no relevant hits" is
        // distinguishable from "my filters excluded everything") overlaps
        // retrieval instead of preceding it - serialized, its count_rows
        // round-trip would be pure added latency on every search, and round
        // trips are what object-store backends pay for.
        let candidates_fut = async {
            match plan.mode {
                SearchMode::Fts => {
                    let mut hits = store
                        .fts_search(&plan.query, plan.pool, &plan.filter)
                        .await
                        .map_err(map_storage)?;
                    retain_non_subagents(&mut hits, plan.exclude_subagents);
                    Ok(normalize_fts(hits))
                }
                // Vector arm (default): embed `plan.query` and run kNN. The
                // hit score is raw cosine similarity (`1 - distance`), which
                // the recency boost later tweaks.
                SearchMode::Vector => {
                    let backend = load_embedder(embedder).await?;
                    let vector = embed_query(backend.as_ref(), &plan.query)?;
                    stage!("embed_query(+model)");
                    let mut vector_raw = store
                        .vector_search(&vector, plan.vector_pool, &plan.filter, Some(search))
                        .await
                        .map_err(map_storage)?;
                    stage!("vector_search");
                    retain_non_subagents(&mut vector_raw, plan.exclude_subagents);
                    Ok(normalize_vector(vector_raw))
                }
            }
        };
        let scope_fut = async {
            store
                .searchable_in_scope(&plan.filter)
                .await
                .map_err(map_storage)
        };
        let (candidates, searchable_in_scope) = tokio::try_join!(candidates_fut, scope_fut)?;
        stage!("arms+scope joined");

        if candidates.is_empty() {
            return Ok(empty_response(searchable_in_scope));
        }

        // Reduce to the hits the response will actually emit *before* any S3
        // hydration, so the metadata/parts/count fetches below are sized to the
        // top-`limit` sessions' candidates, not the full arm pool (~150). No
        // per-session cap: the surviving candidates are already bounded by the
        // arm pool, and the byte budget bounds the rendered output.
        let (mut selected, mut total_sessions, mut matched_total) =
            select_top_hits(candidates, plan.limit);
        if selected.is_empty() {
            return Ok(empty_response(searchable_in_scope));
        }

        // Hydrate hit metadata (timestamp, role, project, preview source) from
        // the `messages` table - the retrievers return only keys (+ rowids). When
        // every selected hit carries a stable rowid (row meta map loaded), take
        // exactly those rows by id - no `IN x IN` cross-product scan and none of
        // its scalar-index page reads. Otherwise fall back to the keyed IN-scan.
        let rowids: Option<Vec<u64>> = selected.iter().map(|candidate| candidate.rowid).collect();
        let mut metas = match &rowids {
            Some(rowids) => store
                .message_metas_by_rowids(rowids)
                .await
                .map_err(map_storage)?,
            None => {
                let keys = selected
                    .iter()
                    .map(|candidate| MessageKey {
                        session_id: candidate.session_id.clone(),
                        message_id: candidate.message_id.clone(),
                    })
                    .collect::<Vec<_>>();
                store
                    .message_metas_by_keys(&keys)
                    .await
                    .map_err(map_storage)?
            }
        };
        stage!("metas_hydrated");

        // Authoritative subagent exclusion (spec.md#search). `retain_non_subagents`
        // above is the cheap pre-hydration drop, but it keys on a `/` in the
        // composite session_id (claude-code-shaped subagent ids) and so misses
        // harnesses whose subagent sessions carry a plain id and encode
        // subagent-ness only in a `/`-subpath `source_agent` (openclaw). The
        // hydrated meta already carries `source_agent` for display, so enforce
        // the rule here at zero extra requests. Mirror the id-based retain's
        // drop-before-count by subtracting the removed hits and their now-empty
        // session roots from the pool-derived counters. Rows outside this
        // top-`limit` hydration window keep `source_agent` unmaterialized, so a
        // subagent match beyond the window can still sit in `matched_total` (the
        // un-hydrated pool tail) - the accepted cost of not re-adding the
        // `source_agent` materialization the S3 request-law removed.
        if plan.exclude_subagents {
            let excluded: std::collections::HashSet<String> = metas
                .iter()
                .filter(|meta| meta.source_agent.contains('/'))
                .map(|meta| meta.session_id.clone())
                .collect();
            if !excluded.is_empty() {
                matched_total = matched_total.saturating_sub(
                    selected
                        .iter()
                        .filter(|candidate| excluded.contains(&candidate.session_id))
                        .count(),
                );
                total_sessions = total_sessions.saturating_sub(
                    selected
                        .iter()
                        .map(|candidate| session_root(&candidate.session_id))
                        .filter(|root| excluded.contains(*root))
                        .collect::<std::collections::HashSet<_>>()
                        .len(),
                );
                selected.retain(|candidate| !excluded.contains(&candidate.session_id));
                metas.retain(|meta| !meta.source_agent.contains('/'));
                if selected.is_empty() {
                    return Ok(empty_response(searchable_in_scope));
                }
            }
        }

        let meta_index = metas
            .iter()
            .map(|meta| ((meta.session_id.as_str(), meta.message_id.as_str()), meta))
            .collect::<std::collections::HashMap<_, _>>();

        // Final ordering score (spec.md#search). `relevance` ranks vector hits
        // by cosine plus a gentle recency tiebreaker and fts hits by BM25;
        // `recency` ranks both strictly newest-first (the timestamp itself is
        // the key). The boost only reorders comparably-relevant hits.
        let now = clock.now();
        let mut scored = Vec::with_capacity(selected.len());
        for candidate in selected {
            let Some(meta) =
                meta_index.get(&(candidate.session_id.as_str(), candidate.message_id.as_str()))
            else {
                continue;
            };
            let order_score = match (plan.sort_by, plan.mode) {
                (SortBy::Recency, _) => recency_rank(meta.timestamp),
                (SortBy::Relevance, SearchMode::Vector) => {
                    candidate.base_score + recency_boost(meta.timestamp, now)
                }
                (SortBy::Relevance, SearchMode::Fts) => candidate.base_score,
            };
            scored.push(ScoredHit {
                meta: (*meta).clone(),
                display_score: candidate.base_score,
                order_score,
            });
        }
        scored.sort_by(|left, right| {
            right
                .order_score
                .partial_cmp(&left.order_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.meta.session_id.cmp(&right.meta.session_id))
                .then_with(|| left.meta.message_id.cmp(&right.meta.message_id))
        });

        let sessions = build_sessions(store, &scored, &plan.query).await?;
        stage!("build_sessions(parts)");
        page_sessions(
            sessions,
            matched_total,
            total_sessions,
            searchable_in_scope,
            &plan,
        )
    }

    /// Additive recency tiebreaker for `vector` + `relevance` ordering: a bonus
    /// of up to [`RECENCY_BOOST_MAGNITUDE`] decaying exponentially over
    /// [`RECENCY_BOOST_SCALE_DAYS`]. Future timestamps (clock skew) clamp to age
    /// 0 -> full bonus.
    fn recency_boost(ts: DateTime<Utc>, now: DateTime<Utc>) -> f64 {
        let age_days = (now - ts).num_seconds().max(0) as f64 / 86_400.0;
        RECENCY_BOOST_MAGNITUDE * (-age_days / RECENCY_BOOST_SCALE_DAYS).exp()
    }

    /// Ordering key for `sort_by = recency`: epoch seconds, so a plain
    /// descending sort puts the newest message first.
    fn recency_rank(ts: DateTime<Utc>) -> f64 {
        ts.timestamp() as f64
    }

    /// Pick the effective retrieval arm. `fts` always stays `fts`. `vector`
    /// degrades to `fts` when the store has no embeddings - there is nothing to
    /// match against (`has_embeddings()` is the only gate).
    async fn resolve_effective_mode(
        store: &Store,
        requested: SearchMode,
    ) -> Result<SearchMode, ErrorEnvelope> {
        if matches!(requested, SearchMode::Fts) {
            return Ok(SearchMode::Fts);
        }
        let has = store.has_embeddings().await.map_err(map_storage)?;
        Ok(if has {
            SearchMode::Vector
        } else {
            SearchMode::Fts
        })
    }

    /// Materialize the lazy embedder on the first vector branch that needs it.
    /// Wraps the load error in an Internal envelope - candle/Metal load failure
    /// is a server-side problem, not a caller error.
    async fn load_embedder(
        embedder: &LazyEmbedder,
    ) -> Result<std::sync::Arc<dyn Embedder>, ErrorEnvelope> {
        embedder.get().await.map_err(|error| {
            map_error(crate::Error::internal(format!(
                "embedder load failed: {error}"
            )))
        })
    }

    pub fn plan_search(request: SearchRequest) -> Result<SearchPlan, ErrorEnvelope> {
        validate_protocol(request.protocol_version)?;

        let _ns = super::resolve_namespace(request.namespace.as_deref())?;

        let mode = match request.mode {
            crate::wire::SearchModeWire::Fts => SearchMode::Fts,
            crate::wire::SearchModeWire::Vector => SearchMode::Vector,
        };
        let sort_by = request.sort_by;
        let filters = request.filters;
        let query = request.query.trim().to_owned();
        if query.is_empty() {
            return Err(map_error(crate::Error::validation_field(
                "query must be non-empty after trim",
                "query",
                Some(serde_json::json!(request.query)),
                Some("non-empty string after trim".to_owned()),
            )));
        }
        if request.limit == 0 {
            return Err(map_error(crate::Error::validation_field(
                "limit must be at least 1",
                "limit",
                Some(serde_json::json!(request.limit)),
                Some("integer >= 1".to_owned()),
            )));
        }
        let limit = request.limit.min(LIMIT_CAP);
        let filter = build_scope_filter(&filters)?;
        let exclude_subagents = default_excludes_subagents(&filters);
        // Retriever candidate pool: wider than `limit` so grouping and the
        // recency reorder have material to work with. When excluding subagents
        // in-memory, over-fetch by half (subagents are ~30% of the corpus) so
        // ~`pool` non-subagent candidates survive the drop.
        let mut pool = limit.saturating_mul(5).max(50);
        let mut vector_pool = pool.saturating_mul(2);
        if exclude_subagents {
            pool = pool.saturating_mul(3) / 2;
            vector_pool = vector_pool.saturating_mul(3) / 2;
        }
        Ok(SearchPlan {
            mode,
            query,
            filter,
            filters,
            sort_by,
            pool,
            vector_pool,
            limit,
            exclude_subagents,
        })
    }

    /// Conversation root for grouping. The Claude Code adapter
    /// stores sub-agent sessions under ids of the form `<parent-uuid>/agent-<id>`;
    /// stripping at the first `/` yields the user-facing conversation root. Other
    /// adapters (codex, etc.) use ids without `/` and pass through unchanged.
    fn session_root(session_id: &str) -> &str {
        match session_id.find('/') {
            Some(idx) => &session_id[..idx],
            None => session_id,
        }
    }

    /// Early, pre-hydration half of the subagent exclusion: drop hits whose
    /// composite session id carries a `/` (claude-code-shaped `<parent>/agent-x`
    /// ids, the same marker `session_root` splits on). This is the cheap path -
    /// it needs only the retriever's keys, no `messages` read. It is NOT
    /// authoritative on its own: a harness whose subagent sessions have plain
    /// ids and mark subagent-ness only via a `/`-subpath `source_agent`
    /// (openclaw) slips through here and is caught by the authoritative
    /// `source_agent`-subpath check at the hydrated-meta stage in `pond_search`.
    /// The two together replace the old `NOT source_agent LIKE` SQL prefilter,
    /// which forced a `source_agent` materialization that cost scattered GETs on
    /// a remote store.
    fn retain_non_subagents(hits: &mut Vec<SearchHit>, exclude: bool) {
        if exclude {
            hits.retain(|hit| !hit.key.session_id.contains('/'));
        }
    }

    /// Minimum query-term length considered "informative" for snippet
    /// anchoring. Shorter terms ("how", "the", "is", "my", "at") attract the
    /// `.min()` anchor to offset-near-0 because they occur very early in any
    /// text, masking the real match site.
    const ANCHOR_MIN_TERM_CHARS: usize = 4;

    /// Build a hit's `text` payload (spec.md#search): the message body when
    /// it fits within the snippet window, otherwise a query-windowed slice
    /// centered on the first informative term. Bounded for the agent-context
    /// budget; callers fetch the full body via `pond_get_message`.
    pub fn hit_payload(text: &str, query: &str) -> String {
        let chars_len = text.chars().count();
        if chars_len <= HIT_SNIPPET_CHARS {
            return text.to_owned();
        }
        query_snippet(text, query)
    }

    /// A snippet windowed around the first informative query term found in
    /// `text`, capped at [`HIT_SNIPPET_CHARS`] code points. Falls back to the
    /// text head when no term matches.
    ///
    /// Terms shorter than [`ANCHOR_MIN_TERM_CHARS`] are excluded from anchor
    /// selection because they pull the window to offset-0 (a snippet audit on
    /// the live corpus found ~25-30% of conversational queries had their
    /// anchor degraded by short stop-word-like terms like "how", "the", "my").
    /// If every term is short, the filter is bypassed.
    ///
    /// TODO(snippet-anchor): reassess for vector-only hits (paraphrase queries
    /// where no literal term matches): the fallback to offset-0 is OK but not
    /// great. Possible upgrades: ngram match overlap, or
    /// skip-window-around-most-distinctive-substring. See snippet audit in
    /// tier-0 findings.
    fn query_snippet(text: &str, query: &str) -> String {
        let lower_text = text.to_lowercase();
        let terms: Vec<String> = query
            .split_whitespace()
            .filter(|term| !term.is_empty())
            .map(str::to_lowercase)
            .collect();
        let any_informative = terms
            .iter()
            .any(|term| term.chars().count() >= ANCHOR_MIN_TERM_CHARS);
        let hit = terms
            .iter()
            .filter(|term| !any_informative || term.chars().count() >= ANCHOR_MIN_TERM_CHARS)
            .filter_map(|term| lower_text.find(term.as_str()))
            .min();
        let chars: Vec<char> = text.chars().collect();
        // `find` returned a byte offset into the lowercased copy; index that
        // copy, not `text` - lowercasing can change byte length, so the offset
        // is not necessarily a valid char boundary in the original.
        let center = hit
            .map(|byte| lower_text[..byte].chars().count())
            .unwrap_or(0);
        let half = HIT_SNIPPET_CHARS / 2;
        let start = center.saturating_sub(half);
        let end = (start + HIT_SNIPPET_CHARS).min(chars.len());
        let start = end.saturating_sub(HIT_SNIPPET_CHARS);
        // Truncation markers carry the omitted-char counts so the reader knows
        // this is a windowed slice and roughly how much it's missing. The fetch
        // verb is left to the transcript's key line (surface-specific: `pond_get_message`
        // for MCP, `pond get-message` for the CLI); naming it here would be
        // redundant and wrong on one surface.
        let mut snippet = String::new();
        if start > 0 {
            snippet.push_str(&format!("[{start} chars before] "));
        }
        snippet.extend(&chars[start..end]);
        if end < chars.len() {
            snippet.push_str(&format!(" [+{} more chars]", chars.len() - end));
        }
        snippet
    }

    struct Candidate {
        rowid: Option<u64>,
        session_id: String,
        message_id: String,
        base_score: f64,
    }

    struct ScoredHit {
        meta: MessageMeta,
        /// Shown to the caller: raw cosine (vector) or pool-normalized BM25
        /// (fts). Relative within one response - not a cross-query threshold.
        display_score: f64,
        /// Internal ranking key: cosine + recency boost (vector/relevance),
        /// BM25 (fts/relevance), or epoch seconds (recency). Drives both the
        /// global sort and the per-session rank.
        order_score: f64,
    }

    impl ScoredHit {
        fn to_search_result(
            &self,
            query: &str,
            summaries: &HashMap<(String, String), Vec<PartSummary>>,
        ) -> Result<SearchResult, ErrorEnvelope> {
            let text = hit_payload(&self.meta.search_text, query);
            let role = match self.meta.role.as_str() {
                "system" => Role::System,
                "user" => Role::User,
                "assistant" => Role::Assistant,
                "tool" => Role::Tool,
                other => {
                    return Err(map_error(crate::Error::internal(format!(
                        "stored message has unknown role: {other}"
                    ))));
                }
            };
            // Only user hits earn a parts_summary (FilePart signal); see the
            // rationale in spec.md#search.
            let parts_summary = if matches!(role, Role::User) {
                summaries
                    .get(&(self.meta.session_id.clone(), self.meta.message_id.clone()))
                    .cloned()
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            Ok(SearchResult {
                message_id: self.meta.message_id.clone(),
                role,
                timestamp: self.meta.timestamp,
                text,
                score: self.display_score.clamp(0.0, 1.0),
                parts_summary,
            })
        }
    }

    fn normalize_fts(hits: Vec<SearchHit>) -> Vec<Candidate> {
        let max = hits.iter().map(|hit| hit.score).fold(0.0_f32, f32::max);
        hits.into_iter()
            .map(|hit| Candidate {
                rowid: hit.rowid,
                session_id: hit.key.session_id,
                message_id: hit.key.message_id,
                base_score: if max > 0.0 {
                    f64::from(hit.score / max)
                } else {
                    0.0
                },
            })
            .collect()
    }

    // Cosine similarity (`1 - distance`): raw, bounded [0, 1], so the value is
    // stable across pool sizes (unlike the old rank-norm `1 - idx/n`, which
    // shifted whenever `limit` changed).
    fn normalize_vector(hits: Vec<SearchHit>) -> Vec<Candidate> {
        hits.into_iter()
            .map(|hit| Candidate {
                rowid: hit.rowid,
                session_id: hit.key.session_id,
                message_id: hit.key.message_id,
                base_score: 1.0 - f64::from(hit.score),
            })
            .collect()
    }

    fn embed_query(embedder: &dyn Embedder, query: &str) -> Result<Vec<f32>, ErrorEnvelope> {
        let prompt = format_query(query);
        // Model inference is synchronous and CPU-bound; `block_in_place` keeps
        // it from stalling other tasks on the async worker thread. (Requires a
        // multi-threaded runtime - see `pond_search`.)
        let vectors =
            tokio::task::block_in_place(|| embedder.embed(&[prompt])).map_err(|error_value| {
                map_error(crate::Error::internal(format!(
                    "failed to embed query: {error_value}"
                )))
            })?;
        vectors.into_iter().next().ok_or_else(|| {
            map_error(crate::Error::internal(
                "embedder returned no vector for query",
            ))
        })
    }

    /// Pick the candidates that will actually be hydrated, using only the keys
    /// and `base_score` the arm already produced - no S3. Keeps every candidate
    /// belonging to the top-`limit` session roots (no per-session cap: the arm
    /// pool already bounds the count, and the byte budget bounds the rendered
    /// output). Hydration and rendering then touch those rows instead of the
    /// full arm pool (~150). Returns the selected candidates, the total
    /// distinct-session-root count (for `has_more`), and the candidate count
    /// (for `matched_total`). There is no score floor: absence honesty comes
    /// from `searchable_in_scope`, not from dropping low-scoring hits (present
    /// and absent content overlap in the cosine band; see
    /// docs/researches/embeddings.md).
    fn select_top_hits(
        mut candidates: Vec<Candidate>,
        limit: usize,
    ) -> (Vec<Candidate>, usize, usize) {
        let matched_total = candidates.len();
        candidates.sort_by(|left, right| {
            right
                .base_score
                .partial_cmp(&left.base_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.session_id.cmp(&right.session_id))
                .then_with(|| left.message_id.cmp(&right.message_id))
        });
        // Distinct session roots in best-score order (candidates are sorted),
        // then keep the top `limit` - the most sessions the response can emit.
        let (total_sessions, keep) = {
            let mut order: Vec<&str> = Vec::new();
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for candidate in &candidates {
                let root = session_root(&candidate.session_id);
                if seen.insert(root) {
                    order.push(root);
                }
            }
            let total = order.len();
            let keep: std::collections::HashSet<String> =
                order.into_iter().take(limit).map(str::to_owned).collect();
            (total, keep)
        };
        let selected = candidates
            .into_iter()
            .filter(|candidate| keep.contains(session_root(&candidate.session_id)))
            .collect();
        (selected, total_sessions, matched_total)
    }

    async fn build_sessions(
        store: &Store,
        scored: &[ScoredHit],
        query: &str,
    ) -> Result<Vec<SearchSession>, ErrorEnvelope> {
        use std::collections::BTreeMap;

        struct Acc {
            project: String,
            source_agent: String,
            matched_count: usize,
            /// Highest `order_score` among the session's matches - the session's
            /// rank. Sessions sort on this; matches sort newest-first within.
            rank: f64,
            matches: Vec<(DateTime<Utc>, SearchResult)>,
        }
        // Precompute part summaries for user-role hits, grouped by their actual
        // session id (a subagent hit's parts live under `root/agent-...`, not
        // the grouping root).
        let mut user_ids_by_session: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for hit in scored {
            if hit.meta.role == "user" {
                user_ids_by_session
                    .entry(hit.meta.session_id.clone())
                    .or_default()
                    .push(hit.meta.message_id.clone());
            }
        }
        // Per-session parts scans are independent S3 round trips - run them
        // concurrently, not in a sequential await loop (latency would sum).
        let summary_futs = user_ids_by_session
            .iter()
            .map(|(session_id, message_ids)| async move {
                store
                    .summary_parts_for_messages(session_id, message_ids)
                    .await
                    .map_err(map_storage)
            });
        let mut summaries: HashMap<(String, String), Vec<PartSummary>> = HashMap::new();
        for parts_by_message in futures::future::try_join_all(summary_futs).await? {
            for (key, parts) in parts_by_message {
                summaries.insert(
                    key,
                    parts
                        .iter()
                        .filter_map(|part| PartSummary::for_kind(&part.kind))
                        .collect(),
                );
            }
        }

        let mut groups: BTreeMap<String, Acc> = BTreeMap::new();
        for hit in scored {
            let root = session_root(&hit.meta.session_id).to_owned();
            let entry = groups.entry(root).or_insert_with(|| Acc {
                project: hit.meta.project.clone(),
                source_agent: hit.meta.source_agent.clone(),
                matched_count: 0,
                rank: f64::NEG_INFINITY,
                matches: Vec::new(),
            });
            entry.matched_count += 1;
            entry.rank = entry.rank.max(hit.order_score);
            entry
                .matches
                .push((hit.meta.timestamp, hit.to_search_result(query, &summaries)?));
        }

        let session_ids = groups.keys().cloned().collect::<Vec<_>>();
        let counts = store
            .session_message_counts(&session_ids)
            .await
            .map_err(map_storage)?;

        // Within a session, matches render newest-first: the latest message
        // most likely carries the session's current conclusion (intra-session
        // supersession). Sessions themselves sort by `rank` (best order_score),
        // so a session's lead match need not be its newest.
        let mut result = groups
            .into_iter()
            .map(|(session_id, mut acc)| {
                acc.matches.sort_by(|left, right| {
                    right
                        .0
                        .cmp(&left.0)
                        .then_with(|| left.1.message_id.cmp(&right.1.message_id))
                });
                let matches = acc.matches.into_iter().map(|(_, result)| result).collect();
                (
                    acc.rank,
                    SearchSession {
                        session_messages_count: counts
                            .get(&session_id)
                            .copied()
                            .unwrap_or_default(),
                        session_id,
                        project: acc.project,
                        source_agent: acc.source_agent,
                        matched_message_count: acc.matched_count,
                        matches,
                    },
                )
            })
            .collect::<Vec<_>>();
        result.sort_by(|left, right| {
            right
                .0
                .partial_cmp(&left.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.1.session_id.cmp(&right.1.session_id))
        });
        Ok(result.into_iter().map(|(_, session)| session).collect())
    }

    fn page_sessions(
        sessions: Vec<SearchSession>,
        matched_total: usize,
        total_sessions: usize,
        searchable_in_scope: usize,
        plan: &SearchPlan,
    ) -> Result<SearchResponse, ErrorEnvelope> {
        // Emit the top `limit` sessions with all their matches (no per-session
        // cap). The structured response carries the full ranked set (bounded by
        // the arm pool); the rendered-transcript char budget (transport) is the
        // only output limiter, so `limit` sessions always render at least their
        // top hit. `has_more` warns the ranked set was cut by `limit` - there
        // is no pagination cursor (a wider `limit` dominates page-walking).
        let emitted: Vec<SearchSession> = sessions.into_iter().take(plan.limit).collect();
        let has_more = total_sessions > emitted.len();

        Ok(SearchResponse {
            sessions: emitted,
            matched_total,
            searchable_in_scope,
            has_more,
        })
    }

    /// Escape regex metacharacters so a `source_agent` brand is matched as a
    /// literal inside the anchored `regexp_like` subpath predicate (the `^` and
    /// `(/|$)` anchors stay live; the value between them is inert).
    fn regex_escape_literal(value: &str) -> String {
        let mut out = String::with_capacity(value.len());
        for ch in value.chars() {
            if matches!(
                ch,
                '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\'
            ) {
                out.push('\\');
            }
            out.push(ch);
        }
        out
    }

    /// User-scope clauses (project/session/date) shared by the arm and
    /// `searchable_in_scope`. The subagent exclusion is not here, nor a SQL
    /// clause anywhere - it is applied in-memory (see `retain_non_subagents`).
    fn build_scope_clauses(filters: &SearchFilters) -> Result<Vec<Predicate>, ErrorEnvelope> {
        let mut clauses = Vec::new();

        match &filters.project {
            None => {}
            Some(ProjectFilter::Contains(value)) => {
                clauses.push(Predicate::LikeContains("project", value.clone()));
            }
            Some(ProjectFilter::Regex(pattern)) => {
                clauses.push(Predicate::Regex("project", pattern.clone()));
            }
        }

        if let Some(session_id) = &filters.session_id {
            clauses.push(Predicate::Eq("session_id", session_id.clone().into()));
        }
        if let Some(source_agent) = &filters.source_agent {
            // Exact-or-subpath as an anchored regex: matches `<value>` and its
            // `<value>/...` subpaths, never a sibling brand like `openclaw-x`. A
            // LIKE-prefix would be rejected by the bitmap index on `source_agent`
            // (Lance: "LIKE prefix queries are not supported for bitmap
            // indexes"); `regexp_like` is a scan-side predicate the bitmap index
            // does not intercept, so it evaluates correctly.
            clauses.push(Predicate::Regex(
                "source_agent",
                format!("^{}(/|$)", regex_escape_literal(source_agent)),
            ));
        }
        if let Some(from_date) = &filters.from_date {
            clauses.push(Predicate::Gte(
                "timestamp",
                ScalarValue::Raw(date_bound(from_date, "filters.from_date", false)?),
            ));
        }
        if let Some(to_date) = &filters.to_date {
            clauses.push(Predicate::Lte(
                "timestamp",
                ScalarValue::Raw(date_bound(to_date, "filters.to_date", true)?),
            ));
        }

        Ok(clauses)
    }

    /// Scope predicate for `searchable_in_scope`: user filters only. Empty
    /// `And` for an unfiltered search, which lets the count read the FTS
    /// `num_docs` stat instead of the ~133 MB search_text scan.
    pub fn build_scope_filter(filters: &SearchFilters) -> Result<Predicate, ErrorEnvelope> {
        Ok(Predicate::And(build_scope_clauses(filters)?))
    }

    /// spec.md#search: subagents are excluded from `pond_search` results -
    /// always, except when the caller scopes to a subagent deliberately. That
    /// means a `session_id` (which may itself be a subagent session) or a
    /// `source_agent` value naming a subpath (`"openclaw/subagent"`,
    /// `"claude-code/general-purpose"`) - there the exclusion would fight the
    /// explicit filter. A root `source_agent` (`"openclaw"`, `"claude-code"`)
    /// keeps the default exclusion: its exact-or-subpath match still reaches
    /// only the harness's main sessions, matching core `sessions_search` UX.
    /// Subagents are otherwise reachable via `pond_sql`
    /// (`parent_session_id`).
    pub fn default_excludes_subagents(filters: &SearchFilters) -> bool {
        filters.session_id.is_none()
            && !filters
                .source_agent
                .as_deref()
                .is_some_and(|agent| agent.contains('/'))
    }

    /// Parse a `YYYY-MM-DD` filter date into a timestamp literal. `end_of_day`
    /// pushes `to_date` to the inclusive end of the day.
    fn date_bound(date: &str, field: &str, end_of_day: bool) -> Result<String, ErrorEnvelope> {
        NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| {
            map_error(crate::Error::validation_field(
                format!("{field} must be in YYYY-MM-DD format; got {date}"),
                field,
                Some(serde_json::json!(date)),
                Some("YYYY-MM-DD".to_owned()),
            ))
        })?;
        let time = if end_of_day { "23:59:59" } else { "00:00:00" };
        Ok(format!("timestamp '{date} {time}'"))
    }

    fn empty_response(searchable_in_scope: usize) -> SearchResponse {
        SearchResponse {
            sessions: Vec::new(),
            matched_total: 0,
            searchable_in_scope,
            has_more: false,
        }
    }

    #[cfg(test)]
    mod grouping_helpers_tests {
        #![allow(clippy::expect_used, clippy::unwrap_used)]

        use super::*;

        #[test]
        fn session_root_strips_agent_suffix_for_claude_code_subagents() {
            assert_eq!(
                session_root("94a50f23-1234-5678-9abc-def012345678"),
                "94a50f23-1234-5678-9abc-def012345678",
            );
            assert_eq!(
                session_root("94a50f23-1234-5678-9abc-def012345678/agent-abc123"),
                "94a50f23-1234-5678-9abc-def012345678",
            );
            // Multiple slashes: still cut at the first one (defensive).
            assert_eq!(session_root("root/a/b"), "root");
        }

        #[test]
        fn retain_non_subagents_drops_slash_ids_only_when_excluding() {
            let hit = |sid: &str| SearchHit {
                rowid: None,
                key: crate::sessions::MessageKey {
                    session_id: sid.to_owned(),
                    message_id: "m1".to_owned(),
                },
                score: 1.0_f32,
            };
            let base = vec![hit("root-a"), hit("root-b/agent-x"), hit("root-c")];

            let mut excluded = base.clone();
            retain_non_subagents(&mut excluded, true);
            let ids: Vec<&str> = excluded
                .iter()
                .map(|hit| hit.key.session_id.as_str())
                .collect();
            assert_eq!(ids, ["root-a", "root-c"]);

            let mut kept = base;
            retain_non_subagents(&mut kept, false);
            assert_eq!(kept.len(), 3);
        }
    }
}

pub use search_handler::{
    SearchMode, SearchPlan, build_scope_filter, default_excludes_subagents, explain_search_plan,
    hit_payload, plan_search, pond_search,
};

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::wire::{ProjectFilter, SearchFilters, SearchRequest};
    use chrono::Utc;

    fn search_request(query: &str) -> SearchRequest {
        SearchRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            query: query.to_owned(),
            mode: crate::wire::SearchModeWire::Vector,
            sort_by: crate::wire::SortBy::Relevance,
            filters: SearchFilters::default(),
            limit: 20,
        }
    }

    #[test]
    fn hit_payload_returns_short_text_in_full() {
        let short = "a short message body";
        let text = hit_payload(short, "message");
        assert_eq!(text, short, "small text is returned as-is");
    }

    #[test]
    fn hit_payload_windows_long_text_around_the_query_term() {
        // ~2400 chars: filler head, query term mid-body, filler tail.
        let body = format!("{}NEEDLE{}", "a".repeat(2000), "b".repeat(394));
        let text = hit_payload(&body, "needle");
        assert!(
            text.contains("NEEDLE"),
            "text is the match-windowed snippet: {text}"
        );
        // The <=600-char window is wrapped with truncation markers
        // ("[N chars before] " / " [+N more chars]"); allow for their length.
        assert!(
            text.chars().count() <= 600 + 64,
            "snippet window is bounded by HIT_SNIPPET_CHARS plus markers: {}",
            text.chars().count()
        );
    }

    #[test]
    fn hit_payload_snippet_survives_case_folding_that_changes_byte_length() {
        // `to_lowercase` of 'İ' is two code points, so the lowercased copy has
        // a different byte layout than the original. A query offset taken from
        // that copy must never be sliced into the original text.
        let body = format!("İÉÉÉ{}", "a".repeat(2100));
        let text = hit_payload(&body, "ééé");
        assert!(
            text.contains("ÉÉÉ"),
            "snippet windows on the matched term: {text}"
        );
    }

    #[tokio::test]
    async fn restore_lineage_rejects_a_graph_nesting_deeper_than_one_level() {
        use crate::adapter::Extracted;
        use crate::sessions::Store;
        use crate::wire::{ProviderOptions, Session};
        use tempfile::TempDir;

        let session = |id: &str, parent: Option<&str>| Session {
            id: id.to_owned(),
            parent_session_id: parent.map(str::to_owned),
            parent_message_id: None,
            source_agent: "claude-code".to_owned(),
            created_at: Utc::now(),
            project: Extracted::from_test_value("/tmp/pond".to_owned()),
            options: ProviderOptions::new(),
        };

        let dir = TempDir::new().unwrap();
        let store = Store::open_local(dir.path()).await.unwrap();
        // A -> B -> C is a two-level spawn graph; spec 6.2 caps lineage at one.
        store
            .upsert_sessions(&[
                session("a", None),
                session("b", Some("a")),
                session("c", Some("b")),
            ])
            .await
            .unwrap();

        // Restoring A reaches child B, then finds B is itself a parent of C.
        // The verdict is typed, so a caller branches on it instead of on prose.
        let Lineage::TooDeep { child_id } = restore_lineage(&store, "a").await.unwrap() else {
            panic!("a two-level graph must report TooDeep");
        };
        assert_eq!(child_id, "b");

        // Restoring B is a clean one-level graph: B plus its single child C.
        let Lineage::Complete(lineage) = restore_lineage(&store, "b").await.unwrap() else {
            panic!("a one-level graph restores complete");
        };
        let ids: Vec<&str> = lineage.iter().map(|s| s.session.id.as_str()).collect();
        assert_eq!(ids, ["b", "c"]);

        // A session nobody stored is a distinct outcome from a bad graph.
        assert!(matches!(
            restore_lineage(&store, "nope").await.unwrap(),
            Lineage::NotFound
        ));
    }

    #[test]
    fn build_scope_filter_pushes_down_each_predicate_and_handles_empty() {
        let filters = SearchFilters {
            project: Some(ProjectFilter::Contains("/Users/me/pond".to_owned())),
            session_id: Some("01HXY".to_owned()),
            source_agent: None,
            from_date: Some("2026-01-01".to_owned()),
            to_date: Some("2026-05-01".to_owned()),
        };
        let sql = build_scope_filter(&filters).unwrap().to_lance();
        assert!(sql.contains("project LIKE '%/Users/me/pond%'"));
        assert!(sql.contains("session_id = '01HXY'"));
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("timestamp <="));
        // The subagent exclusion is never a SQL clause; it is applied in memory.
        assert!(!sql.contains("source_agent"));

        // Unfiltered: empty `And` so `searchable_in_scope` reads the FTS num_docs
        // stat instead of the ~133 MB search_text scan.
        assert_eq!(
            build_scope_filter(&SearchFilters::default())
                .unwrap()
                .to_lance(),
            "",
        );
    }

    #[test]
    fn build_scope_filter_rejects_bad_date() {
        let bad_date = SearchFilters {
            from_date: Some("01-01-2026".to_owned()),
            ..SearchFilters::default()
        };
        assert!(build_scope_filter(&bad_date).is_err());
    }

    #[test]
    fn build_scope_filter_escapes_like_wildcards() {
        let filters = SearchFilters {
            project: Some(ProjectFilter::Contains("/Users/me/my_project".to_owned())),
            ..SearchFilters::default()
        };
        let sql = build_scope_filter(&filters).unwrap().to_lance();
        // `_` is a LIKE wildcard and is everywhere in real paths; it must be escaped
        // so `my_project` matches literally, with an ESCAPE clause naming the char.
        assert!(
            sql.contains(r"my\_project"),
            "underscore must be escaped: {sql}"
        );
        assert!(
            sql.contains(r"ESCAPE '\'"),
            "predicate must declare the escape char: {sql}"
        );
    }

    #[test]
    fn source_agent_filter_is_exact_or_subpath_never_a_sibling_prefix() {
        let filters = SearchFilters {
            source_agent: Some("openclaw".to_owned()),
            ..SearchFilters::default()
        };
        let sql = build_scope_filter(&filters).unwrap().to_lance();
        // Anchored regex: matches `openclaw` exactly and `openclaw/<subpath>`,
        // never a sibling like `openclaw-x`. A LIKE prefix is rejected by the
        // source_agent bitmap index, so this is the correct index-safe form.
        assert_eq!(
            sql, "regexp_like(source_agent, '^openclaw(/|$)')",
            "anchored exact-or-subpath: {sql}"
        );
        // Never a LIKE form (prefix errors on the bitmap; contains leaks).
        assert!(!sql.contains("LIKE"), "no LIKE form: {sql}");

        // A subpath value targets exactly that kind and its own children.
        let sub = SearchFilters {
            source_agent: Some("openclaw/subagent".to_owned()),
            ..SearchFilters::default()
        };
        let sql = build_scope_filter(&sub).unwrap().to_lance();
        assert_eq!(
            sql, "regexp_like(source_agent, '^openclaw/subagent(/|$)')",
            "{sql}"
        );

        // A brand carrying a regex metacharacter is escaped so it stays literal.
        let meta = SearchFilters {
            source_agent: Some("a.b".to_owned()),
            ..SearchFilters::default()
        };
        let sql = build_scope_filter(&meta).unwrap().to_lance();
        assert_eq!(sql, "regexp_like(source_agent, '^a\\.b(/|$)')", "{sql}");
    }

    #[test]
    fn source_agent_subpath_disables_exclusion_but_root_value_keeps_it() {
        // No scope -> subagents excluded by default.
        assert!(default_excludes_subagents(&SearchFilters::default()));
        // Naming a subpath explicitly is deliberate subagent scoping -> return
        // those rows.
        assert!(!default_excludes_subagents(&SearchFilters {
            source_agent: Some("openclaw/subagent".to_owned()),
            ..SearchFilters::default()
        }));
        // A root value keeps the exclusion: its exact-or-subpath match still
        // reaches only the harness's main sessions (the OpenClaw plugin passes
        // "openclaw" on every call; it must not flood callers with subagent/
        // cron/hook/probe noise).
        assert!(default_excludes_subagents(&SearchFilters {
            source_agent: Some("openclaw".to_owned()),
            ..SearchFilters::default()
        }));
    }

    #[test]
    fn plan_search_shapes_request_for_each_planning_input() {
        let mut request = search_request("  vector memory  ");
        request.limit = 500;
        // Default request mode is vector.
        let plan = plan_search(request).unwrap();
        assert_eq!(plan.mode, SearchMode::Vector);
        assert_eq!(plan.query, "vector memory");
        assert_eq!(plan.limit, 200);
        // Default filters exclude subagents, so the pools over-fetch by half
        // (200*5=1000 -> 1500, *2 -> 3000) to survive the in-memory drop.
        assert!(plan.exclude_subagents);
        assert_eq!(plan.pool, 1500);
        assert_eq!(plan.vector_pool, 3000);

        // Case 2: an explicit fts mode + a tiny limit floors the pools so the
        // arm doesn't starve (50 floor -> 75 after the over-fetch).
        let mut request = search_request("tiny pool");
        request.mode = crate::wire::SearchModeWire::Fts;
        request.limit = 1;
        let plan = plan_search(request).unwrap();
        assert_eq!(plan.mode, SearchMode::Fts);
        assert_eq!(plan.limit, 1);
        assert_eq!(plan.pool, 75);
        assert_eq!(plan.vector_pool, 150);

        // Case 3: a session_id scope turns the exclusion off (the scope may
        // itself be a subagent session), so no over-fetch - base pools
        // (20*5=100, *2=200) - and the filter plumbs through.
        let mut request = search_request("filtered");
        request.filters.project = Some(ProjectFilter::Contains("/Users/me/pond".to_owned()));
        request.filters.session_id = Some("01HXY".to_owned());
        let plan = plan_search(request).unwrap();
        assert!(!plan.exclude_subagents);
        assert_eq!(plan.pool, 100);
        assert_eq!(plan.vector_pool, 200);
        let sql = plan.filter.to_lance();
        assert!(sql.contains("project LIKE"));
        assert!(sql.contains("session_id = '01HXY'"));
    }

    #[test]
    fn plan_search_rejects_invalid_composition_before_execution() {
        let mut blank = search_request("   ");
        let error = plan_search(blank.clone()).unwrap_err().error;
        assert_eq!(error.code, crate::wire::ErrorCode::ValidationFailed);
        assert_eq!(error.details["field"], "query");

        blank.query = "valid".to_owned();
        blank.limit = 0;
        let error = plan_search(blank.clone()).unwrap_err().error;
        assert_eq!(error.details["field"], "limit");

        blank.limit = 1;
        blank.namespace = Some("remote".to_owned());
        let error = plan_search(blank).unwrap_err().error;
        assert_eq!(error.code, crate::wire::ErrorCode::NamespaceUnknown);
        assert_eq!(error.details["namespace"], "remote");
    }
}

#[cfg(test)]
mod get_tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use crate::sessions::Store;
    use crate::wire::{
        GetEnvelope, GetResult, GetSessionRequest, IngestEnvelope, IngestRequest, Message, Part,
        PartKind, Provenance, ProviderOptions, Session, SessionFrom,
    };
    use chrono::{TimeZone, Utc};
    use tempfile::TempDir;

    fn text_part(session_id: &str, message_id: &str, part_id: &str, body: &str) -> Part {
        Part {
            session_id: session_id.to_owned(),
            id: part_id.to_owned(),
            message_id: message_id.to_owned(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: crate::adapter::extract_str(&serde_json::json!({ "x": body }), "x"),
            },
        }
    }

    async fn ingest(store: &Store, events: Vec<super::IngestEvent>) {
        let envelope = super::pond_ingest(
            store,
            IngestRequest {
                protocol_version: crate::PROTOCOL_VERSION,
                namespace: Some("local".to_owned()),
                events,
            },
        )
        .await;
        assert!(
            matches!(envelope, IngestEnvelope::Success(_)),
            "ingest should succeed: {envelope:?}"
        );
    }

    fn session(id: &str, project_marker: &str) -> Session {
        Session {
            id: id.to_owned(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: "claude-code".to_owned(),
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            project: crate::adapter::extract_str(&serde_json::json!({ "x": project_marker }), "x")
                .unwrap(),
            options: ProviderOptions::new(),
        }
    }

    /// `pond_get_session` paginates over the response byte budget: a session
    /// whose `search_text` exceeds the budget reports `after_remaining > 0`,
    /// and re-requesting with `after_message_id` set to the last returned id
    /// surfaces the rest, disjoint from the first page.
    #[tokio::test(flavor = "multi_thread")]
    async fn pond_get_paginates_session_via_after_message_id() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session_id = "paginate-session";

        // ~80KB per message; three exceed the ~200KB page budget so the first
        // page stops mid-session.
        let huge_text = "abc def ghi jkl ".repeat(5000);
        let mut events = vec![super::IngestEvent::Session(session(
            session_id,
            "pond-paginate",
        ))];
        for index in 0..3 {
            let message_id = format!("paginate-msg-{index}");
            events.push(super::IngestEvent::Message(Message::User {
                id: message_id.clone(),
                session_id: session_id.to_owned(),
                timestamp: Utc
                    .with_ymd_and_hms(2026, 1, 1, 0, index as u32 + 1, 0)
                    .unwrap(),
                options: ProviderOptions::new(),
            }));
            events.push(super::IngestEvent::Part(text_part(
                session_id,
                &message_id,
                &format!("paginate-part-{index}"),
                &huge_text,
            )));
        }
        ingest(&store, events).await;

        let page_request = |after: Option<String>| GetSessionRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            id: session_id.to_owned(),
            limit: 1000,
            from: SessionFrom::Start,
            after_message_id: after,
            before_message_id: None,
        };

        let GetEnvelope::Success(first) = super::pond_get_session(&store, page_request(None)).await
        else {
            panic!("first page must succeed");
        };
        let GetResult::Session {
            messages: first_messages,
            after_remaining,
            ..
        } = first.result
        else {
            panic!("first page is session-scope");
        };
        assert!(after_remaining > 0, "long corpus must trip the page budget");
        let after = first_messages.last().expect("non-empty page").id.clone();

        let GetEnvelope::Success(second) =
            super::pond_get_session(&store, page_request(Some(after))).await
        else {
            panic!("continuation page must succeed");
        };
        let GetResult::Session {
            messages: second_messages,
            ..
        } = second.result
        else {
            panic!("continuation is session-scope");
        };
        assert!(
            !second_messages.is_empty(),
            "continuation surfaces the rest"
        );
        let first_ids: std::collections::HashSet<&str> =
            first_messages.iter().map(|m| m.id.as_str()).collect();
        assert!(
            second_messages
                .iter()
                .all(|m| !first_ids.contains(m.id.as_str())),
            "after_message_id pages must be disjoint"
        );
        Ok(())
    }

    /// `pond_get_session(from = "end")` returns the newest `limit` messages
    /// chronologically (the compaction-recovery path) with the older messages
    /// reported as `before_remaining`; `start` returns the oldest with the
    /// newer ones as `after_remaining`. The two are disjoint ends.
    #[tokio::test(flavor = "multi_thread")]
    async fn pond_get_session_from_end_returns_the_recent_tail() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session_id = "tail-session";

        let mut events = vec![super::IngestEvent::Session(session(
            session_id,
            "pond-tail",
        ))];
        for index in 0..5u32 {
            let message_id = format!("tail-msg-{index}");
            events.push(super::IngestEvent::Message(Message::User {
                id: message_id.clone(),
                session_id: session_id.to_owned(),
                timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, index + 1, 0).unwrap(),
                options: ProviderOptions::new(),
            }));
            events.push(super::IngestEvent::Part(text_part(
                session_id,
                &message_id,
                &format!("tail-part-{index}"),
                &format!("message {index}"),
            )));
        }
        ingest(&store, events).await;

        let request = |from: SessionFrom| GetSessionRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            id: session_id.to_owned(),
            limit: 2,
            from,
            after_message_id: None,
            before_message_id: None,
        };
        let page = |envelope: GetEnvelope| -> (Vec<String>, usize, usize) {
            let GetEnvelope::Success(response) = envelope else {
                panic!("get must succeed");
            };
            let GetResult::Session {
                messages,
                before_remaining,
                after_remaining,
                ..
            } = response.result
            else {
                panic!("session-scope result expected");
            };
            (
                messages.into_iter().map(|m| m.id).collect(),
                before_remaining,
                after_remaining,
            )
        };

        let (end_ids, end_before, _) =
            page(super::pond_get_session(&store, request(SessionFrom::End)).await);
        assert_eq!(
            end_ids,
            ["tail-msg-3", "tail-msg-4"],
            "end returns the newest two, chronologically"
        );
        assert_eq!(end_before, 3, "three older messages precede the tail");

        let (start_ids, _, start_after) =
            page(super::pond_get_session(&store, request(SessionFrom::Start)).await);
        assert_eq!(
            start_ids,
            ["tail-msg-0", "tail-msg-1"],
            "start returns the oldest two"
        );
        assert_eq!(start_after, 3, "three newer messages follow the head");
        Ok(())
    }

    /// The id-misuse paths: `pond_get_session` given a message id resolves up
    /// to the parent session with the page anchored at that message and the
    /// resolution recorded; `pond_get_message` given a session id rejects with
    /// a hint naming `pond_get_session` (a session cannot pick one message).
    #[tokio::test(flavor = "multi_thread")]
    async fn get_session_resolves_message_id_and_get_message_rejects_session_id()
    -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let session_id = "resolve-session";

        let mut events = vec![super::IngestEvent::Session(session(
            session_id,
            "pond-resolve",
        ))];
        for index in 0..4u32 {
            let message_id = format!("resolve-msg-{index}");
            events.push(super::IngestEvent::Message(Message::User {
                id: message_id.clone(),
                session_id: session_id.to_owned(),
                timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, index + 1, 0).unwrap(),
                options: ProviderOptions::new(),
            }));
            events.push(super::IngestEvent::Part(text_part(
                session_id,
                &message_id,
                &format!("resolve-part-{index}"),
                &format!("message {index}"),
            )));
        }
        ingest(&store, events).await;

        let by_message = GetSessionRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            id: "resolve-msg-2".to_owned(),
            limit: 20,
            from: SessionFrom::Start,
            after_message_id: None,
            before_message_id: None,
        };
        let GetEnvelope::Success(response) = super::pond_get_session(&store, by_message).await
        else {
            panic!("message id must resolve to its parent session");
        };
        assert_eq!(response.session.id, session_id);
        let GetResult::Session {
            messages,
            before_remaining,
            resolved_from_message_id,
            ..
        } = response.result
        else {
            panic!("session-scope result expected");
        };
        assert_eq!(resolved_from_message_id.as_deref(), Some("resolve-msg-2"));
        assert_eq!(
            messages.first().map(|m| m.id.as_str()),
            Some("resolve-msg-2"),
            "the page is anchored at the resolved message (inclusive)"
        );
        assert_eq!(before_remaining, 2, "the two earlier messages page up");

        let by_session = crate::wire::GetMessageRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            id: session_id.to_owned(),
            context_before: 3,
            context_after: 3,
        };
        let GetEnvelope::Error(error) = super::pond_get_message(&store, by_session).await else {
            panic!("a session id must not resolve to one message");
        };
        assert!(
            error.error.message.contains("pond_get_session"),
            "the rejection teaches the session read: {}",
            error.error.message
        );
        Ok(())
    }
}
