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
        let total = adapter
            .discover()
            .await
            .map_err(|error| tracing::debug!(%error, "adapter discover failed"))
            .ok();
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
    //! session plus its direct subagent children for the `pond export session
    //! --as` restore path. The spawn graph is one level deep; a collected
    //! child that is itself a parent means a deeper graph, which is a typed
    //! error - never a silently flattened restore.

    use anyhow::{Context, Result, bail};

    use crate::sessions::{SessionWithMessages, Store};

    pub async fn restore_lineage(
        store: &Store,
        session_id: &str,
    ) -> Result<Vec<SessionWithMessages>> {
        let Some(parent) = store.get_session(session_id).await? else {
            bail!("export: session not found: {session_id}");
        };
        let mut sessions = vec![parent];
        for child in store.child_sessions(session_id).await? {
            if !store.child_sessions(&child.id).await?.is_empty() {
                bail!(
                    "adapter-lineage-complete-restore supports one subagent level; session {} has child sessions",
                    child.id
                );
            }
            let child_id = child.id;
            let stored = store
                .get_session(&child_id)
                .await?
                .with_context(|| format!("export: child session disappeared: {child_id}"))?;
            sessions.push(stored);
        }
        Ok(sessions)
    }
}

pub use restore_handler::restore_lineage;

mod get_handler {
    use crate::{
        sessions::{GetLookup, MessageViewParams, RetrievedMessage, SessionViewParams, Store},
        wire::{
            GetEnvelope, GetRequest, GetResponse, GetResult, GetSession, MessageView, PartSummary,
            ResponseMode, ResponsePart, validate_protocol,
        },
    };

    use super::{map_error, map_storage};

    /// Project canonical retrieval data into the response DTO. In `verbatim` the
    /// full parts ride `parts` and `text`/`content` are dropped - they would just
    /// duplicate the inlined text part; otherwise the compact summary rides along
    /// and full parts are elided.
    fn to_message_view(message: RetrievedMessage, verbatim: bool) -> MessageView {
        if verbatim {
            return MessageView {
                id: message.id,
                role: message.role,
                timestamp: message.timestamp,
                text: None,
                content: None,
                parts_summary: Vec::new(),
                parts: Some(
                    message
                        .parts
                        .into_iter()
                        .map(ResponsePart::from_part)
                        .collect(),
                ),
            };
        }
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
            parts: None,
        }
    }

    /// Server response budget, sized to the declared
    /// `_meta["anthropic/maxResultSizeChars"]` cap (~200KB / ~50k tokens). The
    /// server stops adding messages (or parts) when the next would exceed it;
    /// `messages_remaining` / `target_parts_remaining` then signal pagination.
    const BUDGET_BYTES: usize = 200_000;

    pub async fn pond_get(store: &Store, request: GetRequest) -> GetEnvelope {
        if let Err(error) = validate_protocol(request.protocol_version) {
            return GetEnvelope::Error(error);
        }
        if let Err(envelope) = super::resolve_namespace(request.namespace.as_deref()) {
            return GetEnvelope::Error(envelope);
        }

        let response = match (&request.session_id, &request.message_id) {
            (Some(session_id), None) => session_result(store, session_id, &request).await,
            (None, Some(message_id)) => message_result(store, message_id, &request).await,
            (Some(_), Some(_)) => Err(map_error(crate::Error::validation_field(
                "session_id and message_id are mutually exclusive",
                "message_id",
                request.message_id.clone().map(serde_json::Value::String),
                Some("omit when session_id is present".to_owned()),
            ))),
            (None, None) => Err(map_error(crate::Error::validation(
                "one of session_id or message_id is required",
            ))),
        };

        match response {
            Ok(response) => GetEnvelope::Success(response),
            Err(error) => GetEnvelope::Error(error),
        }
    }

    /// Map a stale/unknown `after_id` to a `validation_failed` naming the fix
    /// (spec.md#protocol); `anchor_of` describes the id the client should supply.
    fn unknown_after_id(request: &GetRequest, anchor_of: &str) -> crate::wire::ErrorEnvelope {
        map_error(crate::Error::validation_field(
            "after_id not found (stale or mistyped continuation anchor)",
            "after_id",
            request.after_id.clone().map(serde_json::Value::String),
            Some(format!("a {anchor_of} from a prior page of this read")),
        ))
    }

    async fn session_result(
        store: &Store,
        session_id: &str,
        request: &GetRequest,
    ) -> Result<GetResponse, crate::wire::ErrorEnvelope> {
        let params = SessionViewParams {
            mode: request.response_mode,
            after_id: request.after_id.as_deref(),
            limit: request.limit,
            budget_bytes: BUDGET_BYTES,
            session_from: request.session_from,
        };
        let view = match store
            .session_view(session_id, params)
            .await
            .map_err(map_storage)?
        {
            GetLookup::NotFound => {
                return Err(map_error(crate::Error::not_found(
                    "session",
                    serde_json::json!(session_id),
                    format!("session not found: {session_id}"),
                )));
            }
            GetLookup::UnknownAfterId => return Err(unknown_after_id(request, "message id")),
            GetLookup::Found(view) => view,
        };
        let verbatim = matches!(request.response_mode, ResponseMode::Verbatim);
        Ok(GetResponse {
            session: GetSession::from_session(&view.session),
            result: GetResult::Session {
                messages: view
                    .messages
                    .into_iter()
                    .map(|message| to_message_view(message, verbatim))
                    .collect(),
                messages_remaining: view.messages_remaining,
            },
        })
    }

    async fn message_result(
        store: &Store,
        message_id: &str,
        request: &GetRequest,
    ) -> Result<GetResponse, crate::wire::ErrorEnvelope> {
        let params = MessageViewParams {
            context_depth: request.context_depth,
            mode: request.response_mode,
            after_id: request.after_id.as_deref(),
            limit: request.limit,
            budget_bytes: BUDGET_BYTES,
        };
        let view = match store
            .message_view(message_id, params)
            .await
            .map_err(map_storage)?
        {
            GetLookup::NotFound => {
                return Err(map_error(crate::Error::not_found(
                    "message",
                    serde_json::json!(message_id),
                    format!("message not found: {message_id}"),
                )));
            }
            GetLookup::UnknownAfterId => return Err(unknown_after_id(request, "part id")),
            GetLookup::Found(view) => view,
        };
        // The target's body rides `target_parts` (paginated, full); carrying
        // `text`/`content` on the header too would just duplicate it.
        let target = MessageView {
            id: view.target.id,
            role: view.target.role,
            timestamp: view.target.timestamp,
            text: None,
            content: None,
            parts_summary: Vec::new(),
            parts: None,
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
                siblings: view
                    .siblings
                    .into_iter()
                    .map(|sibling| to_message_view(sibling, false))
                    .collect(),
            },
        })
    }
}

pub use get_handler::pond_get;

mod search_handler {
    //! The `pond_search` handler: hybrid (vector + BM25) retrieval at message
    //! granularity, with filter pushdown and session-grouped responses
    //! (spec.md#search).

    use crate::{
        Clock, SystemClock,
        embed::{Embedder, LazyEmbedder, format_query},
        sessions::{MessageKey, MessageMeta, Store},
        substrate::{Predicate, ScalarValue},
        wire::{
            ErrorEnvelope, PartSummary, ProjectFilter, Role, SearchEnvelope, SearchFilters,
            SearchRequest, SearchResponse, SearchResult, SearchSession, validate_protocol,
        },
    };
    use chrono::NaiveDate;
    use std::collections::HashMap;

    use super::{map_error, map_storage};

    /// Internal branching enum for the retrieval mode. Production callers
    /// never pick: the server decides hybrid-vs-FTS from embedder availability.
    /// `Vector` exists for operator tooling - selected via `pond search --mode`
    /// or the `mode_override` wire field.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SearchMode {
        Hybrid,
        Fts,
        Vector,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SearchPlan {
        pub mode: SearchMode,
        pub query: String,
        pub filter: Predicate,
        pub filters: SearchFilters,
        pub pool: usize,
        pub vector_pool: usize,
        pub limit: usize,
        pub min_score: f64,
    }

    const LIMIT_CAP: usize = 200;
    const MAX_MATCHES_PER_SESSION: usize = 3;
    const SEARCH_BUDGET_BYTES: usize = 60_000;
    /// Centered query-windowed body returned on every hit (spec.md#search).
    /// Calibrated for the agent-context budget: ~600 code points fits a typical
    /// match site without crowding the 10k-token `pond_get` page.
    const HIT_SNIPPET_CHARS: usize = 600;
    const SCORE_DENOMINATOR: f64 = FTS_FUSION_WEIGHT + VECTOR_FUSION_WEIGHT;

    // Score-normalized hybrid fusion weights (spec.md#search). Per-arm
    // base_score (raw BM25 / raw cosine similarity) is min-max normalized
    // within the arm's pool, then the two arms are combined as
    // w_fts * norm_fts + w_vec * norm_vec. The 0.3:1 ratio comes from the
    // 2026-06-10 re-sweep (ops/search-benchmarks/bench.py) after the
    // vector arm switched from rank-norm to raw cosine: w_fts 0.2-0.3 ties
    // the old 0.135 rank-norm config on the 111-query paraphrase set
    // (S@3 67/111, sign test p=0.727) and beats it on the 12-query
    // false-negative regression set (S@3 10/12 vs 9/12). Symmetric across
    // arms - no per-query routing; cross-lingual queries are an agent-layer
    // concern (see the `pond_search` MCP description).
    const FTS_FUSION_WEIGHT: f64 = 0.3;
    const VECTOR_FUSION_WEIGHT: f64 = 1.0;

    /// Run a hybrid or FTS-only search. The mode is server-determined - hybrid
    /// when the store has any vectors, FTS-only otherwise. The embedder is
    /// `LazyEmbedder`-loaded on the first hybrid/vector call, so FTS-only
    /// corpora never pay the model load. The response has no top-level mode
    /// field; retriever attribution stays in `explain_search_plan`.
    ///
    /// Must run on a multi-threaded Tokio runtime: hybrid mode embeds the query via
    /// `block_in_place`, which panics on a `current_thread` runtime.
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
        let override_mode = request.mode_override.map(wire_mode_to_internal);
        let mut plan = plan_search(request, SearchMode::Fts)?;
        plan.mode = resolve_effective_mode(store, override_mode).await?;
        let mut out = String::new();
        if matches!(plan.mode, SearchMode::Fts | SearchMode::Hybrid) {
            let fts = store
                .explain_fts_plan(&plan.query, plan.pool, &plan.filter)
                .await
                .map_err(map_storage)?;
            out.push_str("fts:\n");
            out.push_str(&fts);
            out.push('\n');
        }
        if matches!(plan.mode, SearchMode::Vector | SearchMode::Hybrid) {
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
        Ok(out)
    }

    async fn run_search(
        store: &Store,
        embedder: &LazyEmbedder,
        request: SearchRequest,
        search: &crate::config::SearchConfig,
        _clock: &dyn Clock,
    ) -> Result<SearchResponse, ErrorEnvelope> {
        let override_mode = request.mode_override.map(wire_mode_to_internal);
        let mut plan = plan_search(request, SearchMode::Fts)?;

        // Mode is server-determined unless the caller passed an explicit
        // override (operator tooling). `has_embeddings()` is the only
        // gate: hybrid when the store has any vectors, FTS-only when empty.
        plan.mode = resolve_effective_mode(store, override_mode).await?;

        // A session_id filter pins one conversation, so root-keyed fusion
        // would collapse the whole response to a single hit. Key fusion by
        // message there and let the one session carry up to `limit` hits.
        let single_session = plan.filters.session_id.is_some();
        let fusion_key = if single_session {
            FusionKey::Message
        } else {
            FusionKey::SessionRoot
        };

        // The scope count (spec.md#search-absence-honesty: how many searchable
        // messages the filters left in scope, so "no relevant hits" is
        // distinguishable from "my filters excluded everything") overlaps
        // retrieval instead of preceding it - serialized, its count_rows
        // round-trip would be pure added latency on every search, and round
        // trips are what object-store backends pay for.
        let candidates_fut = async {
            match plan.mode {
                SearchMode::Fts => {
                    let hits = store
                        .fts_search(&plan.query, plan.pool, &plan.filter)
                        .await
                        .map_err(map_storage)?;
                    Ok(normalize_fts(hits))
                }
                SearchMode::Hybrid => {
                    let backend = load_embedder(embedder).await?;
                    let vector = embed_query(backend.as_ref(), &plan.query)?;
                    // The two retrievers hit disjoint datasets (and disjoint mutexes),
                    // so run them concurrently rather than back-to-back.
                    let fts_fut = async {
                        store
                            .fts_search(&plan.query, plan.pool, &plan.filter)
                            .await
                            .map_err(map_storage)
                    };
                    let vector_fut = async {
                        store
                            .vector_search(&vector, plan.vector_pool, &plan.filter, Some(search))
                            .await
                            .map_err(map_storage)
                    };
                    let (fts, vector_raw) = tokio::try_join!(fts_fut, vector_fut)?;
                    // Per-arm scores carry raw magnitude into fusion: BM25 for
                    // FTS, cosine similarity (`1 - distance`) for the vector arm.
                    // Fusion (`fuse_arms`) min-max normalizes each arm over its
                    // full pool and weights the arms by FTS_FUSION_WEIGHT and
                    // VECTOR_FUSION_WEIGHT. Magnitude (not rank position) is the
                    // load-bearing signal: rank-norm made a hit's score depend on
                    // pool size - and pools scale with `limit` - while raw cosine
                    // is stable across pool sizes (2026-06-10 benchmark).
                    let fts_entries: Vec<(MessageKey, f64)> = fts
                        .into_iter()
                        .map(|(key, score)| (key, f64::from(score)))
                        .collect();
                    let vector_entries: Vec<(MessageKey, f64)> = vector_raw
                        .into_iter()
                        .map(|(key, distance)| (key, 1.0 - f64::from(distance)))
                        .collect();
                    let lists = [
                        RankedList {
                            retriever: RetrieverKind::Fts,
                            entries: fts_entries,
                            weight: FTS_FUSION_WEIGHT,
                        },
                        RankedList {
                            retriever: RetrieverKind::Vector,
                            entries: vector_entries,
                            weight: VECTOR_FUSION_WEIGHT,
                        },
                    ];
                    Ok(fuse_arms(&lists, fusion_key)
                        .into_iter()
                        .map(|hit| Candidate {
                            session_id: hit.key.session_id,
                            message_id: hit.key.message_id,
                            base_score: hit.score,
                        })
                        .collect())
                }
                // Vector-only branch. Reached when the caller set
                // `mode_override = Vector` (operator tooling / benchmark
                // harness): embed `plan.query` and run kNN.
                SearchMode::Vector => {
                    let backend = load_embedder(embedder).await?;
                    let vector = embed_query(backend.as_ref(), &plan.query)?;
                    let vector_raw = store
                        .vector_search(&vector, plan.vector_pool, &plan.filter, Some(search))
                        .await
                        .map_err(map_storage)?;
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

        if candidates.is_empty() {
            return Ok(empty_response(searchable_in_scope));
        }

        // Hydrate hit metadata (timestamp, role, project, preview source) from
        // the `messages` table - the retrievers return only message keys.
        let keys = candidates
            .iter()
            .map(|candidate| MessageKey {
                session_id: candidate.session_id.clone(),
                message_id: candidate.message_id.clone(),
            })
            .collect::<Vec<_>>();
        let metas = store
            .message_metas_by_keys(&keys)
            .await
            .map_err(map_storage)?;
        let meta_index = metas
            .iter()
            .map(|meta| ((meta.session_id.as_str(), meta.message_id.as_str()), meta))
            .collect::<std::collections::HashMap<_, _>>();

        let mut scored = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let Some(meta) =
                meta_index.get(&(candidate.session_id.as_str(), candidate.message_id.as_str()))
            else {
                continue;
            };
            let score = candidate.base_score;
            if score < plan.min_score {
                continue;
            }
            scored.push(ScoredHit {
                meta: (*meta).clone(),
                score,
            });
        }
        scored.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.meta.session_id.cmp(&right.meta.session_id))
                .then_with(|| left.meta.message_id.cmp(&right.meta.message_id))
        });

        let matched_total = scored.len();
        // Session-scoped searches return one session; the per-session match
        // cap would silently discard most of what the caller asked for, so
        // it widens to the requested limit there.
        let matches_cap = if single_session {
            plan.limit
        } else {
            MAX_MATCHES_PER_SESSION
        };
        let sessions = build_sessions(store, &scored, &plan.query, matches_cap).await?;
        page_sessions(sessions, matched_total, searchable_in_scope, &plan)
    }

    /// Pick the retrieval mode. An explicit caller override (operator tooling
    /// via the wire `mode_override` field / `pond search --mode`) wins; in its
    /// absence the server runs hybrid when the store has any vectors and
    /// FTS-only when it doesn't (`has_embeddings()` is the only gate).
    async fn resolve_effective_mode(
        store: &Store,
        override_mode: Option<SearchMode>,
    ) -> Result<SearchMode, ErrorEnvelope> {
        if let Some(mode) = override_mode {
            return Ok(mode);
        }
        let has = store.has_embeddings().await.map_err(map_storage)?;
        Ok(if has {
            SearchMode::Hybrid
        } else {
            SearchMode::Fts
        })
    }

    /// Materialize the lazy embedder on the first hybrid/vector branch that
    /// needs it. Wraps the load error in an Internal envelope - candle/Metal
    /// load failure is a server-side problem, not a caller error.
    async fn load_embedder(
        embedder: &LazyEmbedder,
    ) -> Result<std::sync::Arc<dyn Embedder>, ErrorEnvelope> {
        embedder.get().await.map_err(|error| {
            map_error(crate::Error::internal(format!(
                "embedder load failed: {error}"
            )))
        })
    }

    pub fn plan_search(
        request: SearchRequest,
        mode: SearchMode,
    ) -> Result<SearchPlan, ErrorEnvelope> {
        validate_protocol(request.protocol_version)?;

        let _ns = super::resolve_namespace(request.namespace.as_deref())?;

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
        let min_score = filters.min_score;
        let filter = build_filter(&filters)?;
        // Retriever candidate pool: wider than `limit` so the fuser has
        // material to merge.
        let pool = limit.saturating_mul(5).max(50);
        Ok(SearchPlan {
            mode,
            query,
            filter,
            filters,
            pool,
            vector_pool: pool.saturating_mul(2),
            limit,
            min_score,
        })
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RetrieverKind {
        Vector,
        Fts,
    }

    impl RetrieverKind {
        fn as_wire(self) -> &'static str {
            match self {
                Self::Vector => "vector",
                Self::Fts => "fts",
            }
        }
    }

    /// A retriever-ranked arm: scored hits best-first plus the arm's fusion
    /// weight. `entries` carries each hit's raw `base_score` (BM25 for FTS,
    /// cosine-similarity for the vector arm); the fuser min-max normalizes
    /// those within the arm before combining across arms. `weight` is the
    /// per-arm scalar that controls relative arm influence after
    /// normalization.
    pub struct RankedList {
        pub retriever: RetrieverKind,
        pub entries: Vec<(MessageKey, f64)>,
        pub weight: f64,
    }

    /// Wire-to-internal mode mapping. Kept here so the wire type stays free of
    /// handler-internal concerns and the conversion is one obvious place.
    fn wire_mode_to_internal(wire: crate::wire::SearchModeWire) -> SearchMode {
        match wire {
            crate::wire::SearchModeWire::Fts => SearchMode::Fts,
            crate::wire::SearchModeWire::Vector => SearchMode::Vector,
            crate::wire::SearchModeWire::Hybrid => SearchMode::Hybrid,
        }
    }

    /// One merged hybrid-fusion result.
    #[derive(Debug, Clone, PartialEq)]
    pub struct FusedHit {
        pub key: MessageKey,
        pub score: f64,
        pub matched_via: Vec<String>,
    }

    /// Conversation root for grouping and per-arm dedup. The Claude Code adapter
    /// stores sub-agent sessions under ids of the form `<parent-uuid>/agent-<id>`;
    /// stripping at the first `/` yields the user-facing conversation root. Other
    /// adapters (codex, etc.) use ids without `/` and pass through unchanged.
    fn session_root(session_id: &str) -> &str {
        match session_id.find('/') {
            Some(idx) => &session_id[..idx],
            None => session_id,
        }
    }

    /// How fusion groups candidates. `SessionRoot` is the default: cross-arm
    /// agreement is credited at the conversation level, one fused hit per
    /// root. `Message` is for session-scoped searches (a `session_id`
    /// filter): every candidate shares the root there, so root keying would
    /// collapse the whole response to exactly one hit (spec.md#search).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FusionKey {
        SessionRoot,
        Message,
    }

    /// Score-normalized hybrid fusion: for each arm, the surviving (post
    /// intra-arm dedup-by-key) raw `base_score` values are min-max normalized
    /// to [0, 1] across that arm's pool, then summed across arms weighted by
    /// `RankedList.weight`. The representative message for a fused group is
    /// the one with the highest single weighted contribution - the message
    /// the strongest arm actually matched - so the displayed snippet reflects
    /// why the group ranked where it did. Ties break on the representative
    /// key for determinism (spec.md#search).
    ///
    /// Why session-root keying by default instead of `(session_id,
    /// message_id)`: a long session whose best FTS message and best vector
    /// message differ would otherwise appear as two separate fused hits,
    /// neither getting the cross-arm validation bonus. Keying on the root
    /// credits cross-arm agreement at the conversation level - which is what
    /// the user sees.
    ///
    /// Why per-arm score normalization instead of RRF: RRF discards score
    /// magnitude (rank 1 contributes the same whether the vector cosine is
    /// 0.85 or 0.55), and on paraphrase queries that magnitude is the load-
    /// bearing signal. See `ops/search-benchmarks/bench.py` and
    /// `docs/researches/embeddings.md`.
    pub fn fuse_arms(lists: &[RankedList], key_by: FusionKey) -> Vec<FusedHit> {
        struct Group {
            score: f64,
            matched_via: Vec<String>,
            rep: MessageKey,
            rep_contribution: f64,
        }
        let group_key = |key: &MessageKey| match key_by {
            FusionKey::SessionRoot => session_root(&key.session_id).to_owned(),
            FusionKey::Message => format!("{}\u{0}{}", key.session_id, key.message_id),
        };
        let mut merged: std::collections::HashMap<String, Group> = std::collections::HashMap::new();
        for list in lists {
            if list.entries.is_empty() {
                continue;
            }
            // Min-max normalize across the FULL arm pool BEFORE dedup. The
            // benchmark replay (ops/search-benchmarks/) and the
            // production code MUST agree on the normalization basis;
            // dedupping first would narrow [lo, hi] to only the surviving
            // groups' scores and skew the normalized signal away from what
            // the benchmark reports. A degenerate arm where every hit ties
            // on raw score collapses to zero contribution; the other arm
            // then decides the order.
            let mut lo = f64::INFINITY;
            let mut hi = f64::NEG_INFINITY;
            for (_, raw) in &list.entries {
                if *raw < lo {
                    lo = *raw;
                }
                if *raw > hi {
                    hi = *raw;
                }
            }
            let range = hi - lo;
            // Intra-arm dedup keeps the highest-scoring message each arm
            // returned for a given group: without it a long session whose
            // top-N hits all share a root would crowd out cross-arm signal
            // from other sessions. (Under Message keying every entry is its
            // own group, so this is a no-op there.)
            let mut seen_in_arm: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for (key, raw) in &list.entries {
                let group = group_key(key);
                if !seen_in_arm.insert(group.clone()) {
                    continue;
                }
                let norm = if range > 0.0 { (raw - lo) / range } else { 0.0 };
                let contribution = list.weight * norm;
                let entry = merged.entry(group).or_insert_with(|| Group {
                    score: 0.0,
                    matched_via: Vec::new(),
                    rep: key.clone(),
                    rep_contribution: f64::NEG_INFINITY,
                });
                entry.score += contribution;
                entry.matched_via.push(list.retriever.as_wire().to_owned());
                if contribution > entry.rep_contribution {
                    entry.rep = key.clone();
                    entry.rep_contribution = contribution;
                }
            }
        }
        let mut hits = merged
            .into_values()
            .map(|group| FusedHit {
                key: group.rep,
                score: group.score,
                matched_via: group.matched_via,
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.key.cmp(&right.key))
        });
        hits
    }

    /// Minimum query-term length considered "informative" for snippet
    /// anchoring. Shorter terms ("how", "the", "is", "my", "at") attract the
    /// `.min()` anchor to offset-near-0 because they occur very early in any
    /// text, masking the real match site.
    const ANCHOR_MIN_TERM_CHARS: usize = 4;

    /// Build a hit's `text` payload (spec.md#search): the message body when
    /// it fits within the snippet window, otherwise a query-windowed slice
    /// centered on the first informative term. Bounded for the agent-context
    /// budget; callers fetch the full body via `pond_get`.
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
        // Truncation markers carry the omitted-char counts so the agent knows
        // this is a windowed slice and roughly how much it's missing; the hit's
        // `message_id` is the handle to fetch the rest via `pond_get`.
        let mut snippet = String::new();
        if start > 0 {
            snippet.push_str(&format!("[{start} chars before] "));
        }
        snippet.extend(&chars[start..end]);
        if end < chars.len() {
            snippet.push_str(&format!(
                " [+{} more chars; pond_get for full]",
                chars.len() - end
            ));
        }
        snippet
    }

    struct Candidate {
        session_id: String,
        message_id: String,
        base_score: f64,
    }

    struct ScoredHit {
        meta: MessageMeta,
        score: f64,
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
                score: normalize_score(self.score),
                parts_summary,
            })
        }
    }

    fn normalize_score(score: f64) -> f64 {
        (score / SCORE_DENOMINATOR).clamp(0.0, 1.0)
    }

    fn normalize_fts(hits: Vec<(MessageKey, f32)>) -> Vec<Candidate> {
        let max = hits.iter().map(|(_, score)| *score).fold(0.0_f32, f32::max);
        hits.into_iter()
            .map(|(key, score)| Candidate {
                session_id: key.session_id,
                message_id: key.message_id,
                base_score: if max > 0.0 {
                    f64::from(score / max)
                } else {
                    0.0
                },
            })
            .collect()
    }

    // Cosine similarity (`1 - distance`), matching the hybrid arm: the score
    // carries magnitude and is stable across pool sizes, instead of the old
    // rank-norm `1 - idx/n` whose value shifted whenever `limit` changed.
    fn normalize_vector(hits: Vec<(MessageKey, f32)>) -> Vec<Candidate> {
        hits.into_iter()
            .map(|(key, distance)| Candidate {
                session_id: key.session_id,
                message_id: key.message_id,
                base_score: 1.0 - f64::from(distance),
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

    async fn build_sessions(
        store: &Store,
        scored: &[ScoredHit],
        query: &str,
        matches_cap: usize,
    ) -> Result<Vec<SearchSession>, ErrorEnvelope> {
        use std::collections::BTreeMap;

        struct Acc {
            project: String,
            source_agent: String,
            matched_count: usize,
            matches: Vec<SearchResult>,
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
        let mut summaries: HashMap<(String, String), Vec<PartSummary>> = HashMap::new();
        for (session_id, message_ids) in &user_ids_by_session {
            for (key, parts) in store
                .summary_parts_for_messages(session_id, message_ids)
                .await
                .map_err(map_storage)?
            {
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
                matches: Vec::new(),
            });
            entry.matched_count += 1;
            entry.matches.push(hit.to_search_result(query, &summaries)?);
        }

        let session_ids = groups.keys().cloned().collect::<Vec<_>>();
        let counts = store
            .session_message_counts(&session_ids)
            .await
            .map_err(map_storage)?;

        let mut result = groups
            .into_iter()
            .map(|(session_id, mut acc)| {
                acc.matches.sort_by(|left, right| {
                    right
                        .score
                        .partial_cmp(&left.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| left.message_id.cmp(&right.message_id))
                });
                acc.matches.truncate(matches_cap);
                SearchSession {
                    session_messages_count: counts.get(&session_id).copied().unwrap_or_default(),
                    session_id,
                    project: acc.project,
                    source_agent: acc.source_agent,
                    matched_message_count: acc.matched_count,
                    matches: acc.matches,
                }
            })
            .collect::<Vec<_>>();
        result.sort_by(|left, right| {
            let left_score = left
                .matches
                .first()
                .map(|hit| hit.score)
                .unwrap_or_default();
            let right_score = right
                .matches
                .first()
                .map(|hit| hit.score)
                .unwrap_or_default();
            right_score
                .partial_cmp(&left_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        Ok(result)
    }

    fn page_sessions(
        sessions: Vec<SearchSession>,
        matched_total: usize,
        searchable_in_scope: usize,
        plan: &SearchPlan,
    ) -> Result<SearchResponse, ErrorEnvelope> {
        let mut emitted = Vec::new();
        let mut used_bytes = 0usize;
        for session in sessions.iter() {
            if emitted.len() >= plan.limit {
                break;
            }
            let bytes = serde_json::to_string(session)
                .map_err(|error| {
                    map_error(crate::Error::internal(format!(
                        "failed to size search response session: {error}"
                    )))
                })?
                .len();
            if !emitted.is_empty() && used_bytes.saturating_add(bytes) > SEARCH_BUDGET_BYTES {
                break;
            }
            used_bytes = used_bytes.saturating_add(bytes);
            emitted.push(session.clone());
        }

        // `has_more` warns the caller that `limit` or the byte budget cut the
        // ranked set short - raise `limit` (up to the cap) to see the rest.
        // There is no pagination cursor: the result set is relevance-ranked and
        // capped, so a wider `limit` dominates page-walking (spec.md#search).
        let has_more = emitted.len() < sessions.len();

        Ok(SearchResponse {
            sessions: emitted,
            matched_total,
            searchable_in_scope,
            has_more,
        })
    }

    /// Build the shared scalar filter predicate pushed into both retrievers.
    /// Both the FTS and vector retrievers scan `messages` (spec.md#datasets),
    /// so one predicate serves both.
    pub fn build_filter(filters: &SearchFilters) -> Result<Predicate, ErrorEnvelope> {
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
            clauses.push(Predicate::Eq("source_agent", source_agent.clone().into()));
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

        // spec.md#search: subagent sessions (`source_agent` with a "/") are
        // excluded by default; an explicit session_id/source_agent filter means
        // the caller is already scoping, so the exclusion would only fight it.
        if !filters.include_subagents
            && filters.session_id.is_none()
            && filters.source_agent.is_none()
        {
            clauses.push(Predicate::Not(Box::new(Predicate::LikeContains(
                "source_agent",
                "/".to_owned(),
            ))));
        }

        Ok(Predicate::And(clauses))
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
    mod fusion_helpers_tests {
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
        fn fuse_arms_dedupes_intra_arm_by_session_root_and_credits_cross_arm() {
            let mk = |sid: &str, mid: &str| crate::sessions::MessageKey {
                session_id: sid.to_owned(),
                message_id: mid.to_owned(),
            };
            // FTS pool (raw BM25): session-A msg-1 (10.0), session-A msg-2
            // (9.0, same root, dropped by intra-arm dedup), session-B msg-3
            // (6.0), session-A/agent-x msg-4 (5.0, same root as A, dropped).
            // Vector pool (raw cosine): session-B msg-7 (0.9, different
            // message than FTS's pick for B), session-A msg-9 (0.6).
            let fts = RankedList {
                retriever: RetrieverKind::Fts,
                entries: vec![
                    (mk("session-A", "msg-1"), 10.0),
                    (mk("session-A", "msg-2"), 9.0),
                    (mk("session-B", "msg-3"), 6.0),
                    (mk("session-A/agent-x", "msg-4"), 5.0),
                ],
                weight: 0.135,
            };
            let vec_arm = RankedList {
                retriever: RetrieverKind::Vector,
                entries: vec![
                    (mk("session-B", "msg-7"), 0.9),
                    (mk("session-A", "msg-9"), 0.6),
                ],
                weight: 1.0,
            };
            let merged = fuse_arms(&[fts, vec_arm], FusionKey::SessionRoot);
            // Output: one row per session_root after intra-arm dedup.
            assert_eq!(merged.len(), 2);
            // Per-arm min-max over the FULL pool BEFORE dedup:
            // FTS pool [10, 9, 6, 5]: range 5. A's first hit (10) -> 1.0;
            // B's first hit (6) -> 0.2.
            // Vector pool [0.9, 0.6]: range 0.3. B -> 1.0; A -> 0.0.
            // session-A: 0.135 * 1.0 + 1.0 * 0.0 = 0.135.
            // session-B: 0.135 * 0.2 + 1.0 * 1.0 = 1.027. B wins.
            assert_eq!(merged[0].key.session_id, "session-B");
            // Representative = highest single weighted contribution: Vector's
            // msg-7 (1.0 * 1.0) beats FTS's msg-3 (0.135 * 0.2) for B.
            assert_eq!(merged[0].key.message_id, "msg-7");
            assert_eq!(merged[0].matched_via, vec!["fts", "vector"]);
            assert_eq!(merged[1].key.session_id, "session-A");
            // For A the FTS contribution (0.135 * 1.0) beats Vector's
            // (1.0 * 0.0), so FTS's msg-1 is the representative.
            assert_eq!(merged[1].key.message_id, "msg-1");
            assert_eq!(merged[1].matched_via, vec!["fts", "vector"]);
        }

        #[test]
        fn fuse_arms_message_key_keeps_per_message_hits_within_one_session() {
            // Session-scoped searches (session_id filter) fuse per message:
            // root keying would collapse everything below to one hit.
            let mk = |sid: &str, mid: &str| crate::sessions::MessageKey {
                session_id: sid.to_owned(),
                message_id: mid.to_owned(),
            };
            let fts = RankedList {
                retriever: RetrieverKind::Fts,
                entries: vec![
                    (mk("session-A", "msg-1"), 10.0),
                    (mk("session-A", "msg-2"), 6.0),
                ],
                weight: 0.3,
            };
            let vec_arm = RankedList {
                retriever: RetrieverKind::Vector,
                entries: vec![
                    (mk("session-A", "msg-2"), 0.9),
                    (mk("session-A", "msg-3"), 0.6),
                ],
                weight: 1.0,
            };
            let merged = fuse_arms(&[fts, vec_arm], FusionKey::Message);
            assert_eq!(merged.len(), 3, "one fused hit per message, not per root");
            // FTS pool [10, 6]: msg-1 -> 1.0, msg-2 -> 0.0. Vector pool
            // [0.9, 0.6]: msg-2 -> 1.0, msg-3 -> 0.0. So msg-2 = 1.0 (both
            // arms), msg-1 = 0.3 (FTS only), msg-3 = 0.0 (vector only).
            assert_eq!(merged[0].key.message_id, "msg-2");
            assert_eq!(merged[0].matched_via, vec!["fts", "vector"]);
            assert_eq!(merged[1].key.message_id, "msg-1");
            assert_eq!(merged[1].matched_via, vec!["fts"]);
        }

        #[test]
        fn fuse_arms_collapses_degenerate_tied_arm_to_zero_contribution() {
            // When every surviving hit in an arm shares the same raw score,
            // min-max normalization has zero range; that arm contributes 0
            // and the other arm decides the order on its own normalized
            // signal. This protects fusion from "flat" arms (e.g. an FTS arm
            // whose BM25 scores all tie at the same low magnitude).
            let mk = |sid: &str, mid: &str| crate::sessions::MessageKey {
                session_id: sid.to_owned(),
                message_id: mid.to_owned(),
            };
            let fts = RankedList {
                retriever: RetrieverKind::Fts,
                entries: vec![(mk("session-A", "a"), 1.0), (mk("session-B", "b"), 1.0)],
                weight: 0.135,
            };
            let vec_arm = RankedList {
                retriever: RetrieverKind::Vector,
                entries: vec![(mk("session-A", "a"), 0.9), (mk("session-B", "b"), 0.3)],
                weight: 1.0,
            };
            let merged = fuse_arms(&[fts, vec_arm], FusionKey::SessionRoot);
            // Vector arm alone decides: A's normalized 1.0 beats B's 0.0.
            assert_eq!(merged[0].key.session_id, "session-A");
            assert!((merged[0].score - 1.0).abs() < 1e-9);
            assert!(merged[1].score.abs() < 1e-9);
        }
    }
}

pub use search_handler::{
    FusedHit, FusionKey, RankedList, RetrieverKind, SearchMode, SearchPlan, build_filter,
    explain_search_plan, fuse_arms, hit_payload, plan_search, pond_search,
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
            mode_override: None,
            filters: SearchFilters::default(),
            limit: 20,
        }
    }

    fn key(session: &str, id: &str) -> crate::sessions::MessageKey {
        crate::sessions::MessageKey {
            session_id: session.to_owned(),
            message_id: id.to_owned(),
        }
    }

    #[test]
    fn fuse_arms_fuses_retrievers_and_reports_provenance() {
        // Each session contributes at most one ballot per arm; cross-arm
        // agreement is credited per session_root, not per message_id.
        // Vector pool (raw cosine): a=0.9, b=0.7, c=0.5.
        // FTS pool (raw BM25):     b=10.0, a=8.0, d=4.0.
        let lists = [
            RankedList {
                retriever: RetrieverKind::Vector,
                entries: vec![
                    (key("session-a", "a"), 0.9),
                    (key("session-b", "b"), 0.7),
                    (key("session-c", "c"), 0.5),
                ],
                weight: 1.0,
            },
            RankedList {
                retriever: RetrieverKind::Fts,
                entries: vec![
                    (key("session-b", "b"), 10.0),
                    (key("session-a", "a"), 8.0),
                    (key("session-d", "d"), 4.0),
                ],
                weight: 0.135,
            },
        ];
        let merged = fuse_arms(&lists, FusionKey::SessionRoot);

        // Vector normalized: a=1.0, b=0.5, c=0.0.
        // FTS normalized:    b=1.0, a=2/3, d=0.0.
        // session-a: 1.0 * 1.0 + 0.135 * 2/3 = 1.090
        // session-b: 1.0 * 0.5 + 0.135 * 1.0 = 0.635
        // session-c: 1.0 * 0.0 + 0 = 0
        // session-d: 0 + 0.135 * 0.0 = 0
        assert_eq!(merged[0].key.session_id, "session-a");
        assert_eq!(merged[1].key.session_id, "session-b");
        assert_eq!(merged[0].matched_via, vec!["vector", "fts"]);
        assert!(merged[0].score > merged[1].score);

        let c = merged
            .iter()
            .find(|hit| hit.key.session_id == "session-c")
            .unwrap();
        assert_eq!(c.matched_via, vec!["vector"]);
        let d = merged
            .iter()
            .find(|hit| hit.key.session_id == "session-d")
            .unwrap();
        assert_eq!(d.matched_via, vec!["fts"]);
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
        // ("[N chars before] " / " [+N more chars; pond_get for full]"); allow for their length.
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
        let err = restore_lineage(&store, "a").await.unwrap_err();
        assert!(
            err.to_string().contains("one subagent level"),
            "expected the deeper-graph error, got: {err}"
        );

        // Restoring B is a clean one-level graph: B plus its single child C.
        let lineage = restore_lineage(&store, "b").await.unwrap();
        let ids: Vec<&str> = lineage.iter().map(|s| s.session.id.as_str()).collect();
        assert_eq!(ids, ["b", "c"]);
    }

    #[test]
    fn build_filter_pushes_down_each_predicate_and_handles_empty() {
        let filters = SearchFilters {
            project: Some(ProjectFilter::Contains("/Users/me/pond".to_owned())),
            session_id: Some("01HXY".to_owned()),
            source_agent: Some("claude-code".to_owned()),
            from_date: Some("2026-01-01".to_owned()),
            to_date: Some("2026-05-01".to_owned()),
            min_score: 0.0,
            include_subagents: false,
        };
        let sql = build_filter(&filters).unwrap().to_lance();
        assert!(sql.contains("project LIKE '%/Users/me/pond%'"));
        assert!(sql.contains("session_id = '01HXY'"));
        assert!(sql.contains("source_agent = 'claude-code'"));
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("timestamp <="));
        // session_id/source_agent set => default subagent exclusion is skipped.
        assert!(!sql.contains("NOT ("));

        assert_eq!(
            build_filter(&SearchFilters::default()).unwrap().to_lance(),
            "NOT (source_agent LIKE '%/%' ESCAPE '\\')",
        );
        assert_eq!(
            build_filter(&SearchFilters {
                include_subagents: true,
                ..SearchFilters::default()
            })
            .unwrap()
            .to_lance(),
            "",
        );
    }

    #[test]
    fn build_filter_rejects_bad_date() {
        let bad_date = SearchFilters {
            from_date: Some("01-01-2026".to_owned()),
            ..SearchFilters::default()
        };
        assert!(build_filter(&bad_date).is_err());
    }

    #[test]
    fn build_filter_contains_escapes_like_wildcards() {
        let filters = SearchFilters {
            project: Some(ProjectFilter::Contains("/Users/me/my_project".to_owned())),
            ..SearchFilters::default()
        };
        let sql = build_filter(&filters).unwrap().to_lance();
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
    fn plan_search_shapes_request_for_each_planning_input() {
        let mut request = search_request("  vector memory  ");
        request.limit = 500;
        request.filters.min_score = 0.42;
        let plan = plan_search(request, SearchMode::Hybrid).unwrap();
        assert_eq!(plan.mode, SearchMode::Hybrid);
        assert_eq!(plan.query, "vector memory");
        assert_eq!(plan.limit, 200);
        assert_eq!(plan.pool, 1000);
        assert_eq!(plan.vector_pool, 2000);
        assert_eq!(plan.min_score, 0.42);

        // Case 2: a tiny limit floors the pools so retrievers don't starve.
        let mut request = search_request("tiny pool");
        request.limit = 1;
        let plan = plan_search(request, SearchMode::Fts).unwrap();
        assert_eq!(plan.mode, SearchMode::Fts);
        assert_eq!(plan.limit, 1);
        assert_eq!(plan.pool, 50);
        assert_eq!(plan.vector_pool, 100);

        // Case 3: filters get plumbed into the shared filter predicate.
        let mut request = search_request("filtered");
        request.filters.project = Some(ProjectFilter::Contains("/Users/me/pond".to_owned()));
        request.filters.source_agent = Some("claude-code".to_owned());
        let plan = plan_search(request, SearchMode::Fts).unwrap();
        let sql = plan.filter.to_lance();
        assert!(sql.contains("project LIKE"));
        assert!(sql.contains("source_agent = 'claude-code'"));
    }

    #[test]
    fn plan_search_rejects_invalid_composition_before_execution() {
        let mut blank = search_request("   ");
        let error = plan_search(blank.clone(), SearchMode::Fts)
            .unwrap_err()
            .error;
        assert_eq!(error.code, crate::wire::ErrorCode::ValidationFailed);
        assert_eq!(error.details["field"], "query");

        blank.query = "valid".to_owned();
        blank.limit = 0;
        let error = plan_search(blank.clone(), SearchMode::Fts)
            .unwrap_err()
            .error;
        assert_eq!(error.details["field"], "limit");

        blank.limit = 1;
        blank.namespace = Some("remote".to_owned());
        let error = plan_search(blank, SearchMode::Fts).unwrap_err().error;
        assert_eq!(error.code, crate::wire::ErrorCode::NamespaceUnknown);
        assert_eq!(error.details["namespace"], "remote");
    }
}
