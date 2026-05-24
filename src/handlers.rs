fn map_error(error: crate::Error) -> crate::wire::ErrorEnvelope {
    error.into()
}

/// Typed identifier for the namespace a wire request targets. v1 is
/// single-namespace, so every successful resolve returns `root()`; the
/// type lets future multi-namespace routing land without churning call
/// sites (spec.md#namespace-resolution).
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
            IngestStatus, new_request_id, validate_protocol,
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
        },
        Skipped {
            reason: String,
        },
        Rejected {
            reason: String,
        },
        /// Per-session staleness skip (spec.md#event-ordering): adapter short-circuited
        /// the file decode because `mtime < MAX(messages.timestamp)`.
        Fresh,
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
        // Perf probe accumulators. Logged once at the end of the run under
        // `POND_LOG=pond=info` so a single sync emits one tidy summary plus
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
                        SkipReason::Fresh => SyncStatus::Fresh,
                    };
                    summary.skipped_fresh += 1;
                    on_event(SyncEvent::SessionDone(SessionOutcome {
                        project,
                        session_id: Some(session_id),
                        messages: 0,
                        status,
                    }));
                }
                Ok(AdapterYield::Event(event)) => {
                    // A new Session means the previous one is being closed
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
                        let flush_outcomes = validator.flush(store).await?;
                        validator_total += flush_start.elapsed();
                        validator_count += 1;
                        summary.add_outcomes(&flush_outcomes);
                        drain_pending_dones(&mut pending_dones, &flush_outcomes, &mut on_event);
                    }
                }
                Err(error) => {
                    // Per-event drop semantics: the adapter's error is either
                    // a pre-Session header failure (whole file unusable) or a
                    // mid-session bad-line skip. We never reset the validator
                    // on these any more - subsequent good lines from the same
                    // file should still land.
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

        // Close the last in-flight substream (if any) and final-flush all
        // pending substreams in one batched write.
        if let Some(prev) = in_flight.take() {
            pending_dones.push_back(PendingDone {
                project: prev.project,
                session_id: prev.session_id,
                messages: prev.messages,
                dropped_events: prev.dropped_events,
                session_index: prev.session_index,
            });
        }
        let validator_start = std::time::Instant::now();
        let final_outcomes = validator.finish(store).await?;
        validator_total += validator_start.elapsed();
        validator_count += 1;
        summary.add_outcomes(&final_outcomes);
        drain_pending_dones(&mut pending_dones, &final_outcomes, &mut on_event);

        summary.truncated_values = crate::adapter::extract::truncated_values_count()
            .saturating_sub(truncations_before) as usize;

        // spec.md#index-upkeep: fold the appended rows into the indexes on the
        // write path. Soft-fail - a failed fold is logged and retried by the
        // next write batch, never an error that aborts a committed ingest.
        if let Err(error) = store.index_upkeep().await {
            tracing::warn!(%error, "index upkeep failed after sync; will retry on next batch");
        }

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
                    request_id: new_request_id(),
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
        let mut tail = validator.finish(store).await?;
        outcomes.append(&mut tail);
        outcomes.sort_by_key(|outcome| outcome.index);
        // spec.md#index-upkeep: fold the newly-appended rows into the indexes
        // on the write path, for every ingest route (CLI sync, HTTP, MCP). A
        // failed fold is soft - logged and retried by the next write batch -
        // never an error that aborts the committed write.
        if let Err(error) = store.index_upkeep().await {
            tracing::warn!(%error, "index upkeep failed after ingest; will retry on next batch");
        }
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

mod session_events_handler {
    //! `pond_session_events` (spec.md#protocol): catch-up SSE stream over a
    //! stored session's messages. v1 scope is read-after-`since`: scan
    //! messages strictly after the resume point in `(timestamp, message_id)`
    //! order, emit one `message` event per row (with its parts, filtered by
    //! include_thinking / include_tool_results), then emit `end` and close.
    //! Live-tail activates with live-write (section 4) on the same endpoint
    //! without a wire change.

    use crate::{
        sessions::{MessageWithParts, Store},
        wire::{ErrorEnvelope, PartKind},
    };
    use serde_json::{Value, json};

    use super::{map_error, map_storage};

    /// Parsed `since` query parameter.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Since {
        /// No `since`: emit `session` header + every message + `end`.
        None,
        /// `since=session:<id>`: emit every message + `end`; the client has
        /// already seen the header from a prior connection.
        Header,
        /// `since=end:<id>`: idempotent terminator; emit `end` and close.
        End,
        /// `since=<message_id>`: resume from `(timestamp, id) > since.row`.
        Message(String),
    }

    /// Decode a `since` query value. Empty / missing -> `None`. Prefixes
    /// `session:` / `end:` map to the matching `Header` / `End` variants
    /// regardless of the value carried after the colon (the server already
    /// knows the session id from the path). A bare value is treated as a
    /// message id; `since=unknown` cases bubble up to the per-row resume
    /// logic, which surfaces them as `validation_failed`.
    pub fn parse_since(value: Option<&str>) -> Since {
        match value {
            None => Since::None,
            Some(raw) => {
                if raw.is_empty() {
                    Since::None
                } else if let Some(rest) = raw.strip_prefix("session:") {
                    let _ = rest;
                    Since::Header
                } else if let Some(rest) = raw.strip_prefix("end:") {
                    let _ = rest;
                    Since::End
                } else {
                    Since::Message(raw.to_owned())
                }
            }
        }
    }

    /// Server-Sent Events event ready to encode by the transport layer.
    /// Identity (`event` + `id` + `data`) is spec.md#protocol verbatim.
    #[derive(Debug, Clone, PartialEq)]
    pub struct SseEvent {
        pub event: &'static str,
        pub id: String,
        pub data: Value,
    }

    /// Plan a session-events response: validate the session exists, parse
    /// `since`, locate the cutoff, and emit the ordered list of events the
    /// transport will stream. Returns a typed error envelope on `not_found`
    /// (unknown session id) or `validation_failed` (unknown `since`); all
    /// storage errors map to `storage_unavailable`.
    pub async fn pond_session_events(
        store: &Store,
        session_id: &str,
        since: Since,
        include_thinking: bool,
        include_tool_results: bool,
    ) -> Result<Vec<SseEvent>, ErrorEnvelope> {
        let stored = store.get_session(session_id).await.map_err(map_storage)?;
        let Some(stored) = stored else {
            return Err(map_error(crate::Error::not_found(
                "session",
                json!(session_id),
                "session not found",
            )));
        };

        // `end:<id>` is idempotent - emit terminator + close, no scan.
        if since == Since::End {
            return Ok(vec![end_event(session_id)]);
        }

        let mut events = Vec::new();
        if since == Since::None {
            events.push(SseEvent {
                event: "session",
                id: format!("session:{session_id}"),
                data: serde_json::to_value(&stored.session).unwrap_or(Value::Null),
            });
        }

        let messages = stored.messages;
        let start_at = match &since {
            Since::None | Since::Header | Since::End => 0,
            Since::Message(message_id) => {
                let position = messages
                    .iter()
                    .position(|message| message.message.id() == message_id);
                match position {
                    Some(index) => index + 1,
                    None => {
                        return Err(map_error(crate::Error::validation_field(
                            "since references a message id that does not exist in this session",
                            "since",
                            Some(json!(message_id)),
                            Some("message_id present in this session".to_owned()),
                        )));
                    }
                }
            }
        };

        for message in &messages[start_at..] {
            events.push(message_event(
                message,
                include_thinking,
                include_tool_results,
            ));
        }
        events.push(end_event(session_id));
        Ok(events)
    }

    fn message_event(
        message: &MessageWithParts,
        include_thinking: bool,
        include_tool_results: bool,
    ) -> SseEvent {
        let mut parts = message.parts.clone();
        parts.retain(|part| match &part.kind {
            PartKind::Reasoning { .. } => include_thinking,
            PartKind::ToolResult { .. } => include_tool_results,
            PartKind::ToolApprovalRequest { .. } | PartKind::ToolApprovalResponse { .. } => false,
            PartKind::Text { .. } | PartKind::File { .. } | PartKind::ToolCall { .. } => true,
        });

        SseEvent {
            event: "message",
            id: message.message.id().to_owned(),
            data: json!({
                "message": message.message,
                "parts": parts,
            }),
        }
    }

    fn end_event(session_id: &str) -> SseEvent {
        SseEvent {
            event: "end",
            id: format!("end:{session_id}"),
            data: json!({ "reason": "caught_up" }),
        }
    }
}

pub use session_events_handler::{Since, SseEvent, parse_since, pond_session_events};

mod export_handler {
    //! `pond_export` (spec.md#protocol): walk every session in the store and
    //! emit its canonical event stream as JSONL - one `IngestEvent` per line.
    //! The output is byte-identical with what `pond ingest` / `pond_ingest`
    //! accepts on input, so `export | ingest` is a portable backup loop.
    //! Sessions are emitted in lexicographic id order; within each session,
    //! messages run in `(timestamp, message_id)` order and each message's
    //! parts immediately follow in `ordinal` order. Matches the
    //! spec.md#event-ordering ordering contract so the output
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
    //! `restore_lineage` (spec.md#lineage-complete-restore): collect the named
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
                    "lineage-complete-restore supports one subagent level; session {} has child sessions",
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
        sessions::{MessageWithParts, SessionWithMessages, Store},
        wire::{GetEnvelope, GetRequest, GetResponse, GetResult, validate_protocol},
        wire::{Message, Part, PartKind},
    };

    use super::{map_error, map_storage};

    pub async fn pond_get(store: &Store, request: GetRequest) -> GetEnvelope {
        if let Err(error) = validate_protocol(request.protocol_version) {
            return GetEnvelope::Error(error);
        }
        if let Err(envelope) = super::resolve_namespace(request.namespace.as_deref()) {
            return GetEnvelope::Error(envelope);
        }

        let result = match (&request.session_id, &request.message_id, &request.up_to) {
            (Some(session_id), None, up_to) => {
                session_scope(store, &request, session_id, up_to.as_deref()).await
            }
            (None, Some(message_id), None) => message_scope(store, &request, message_id).await,
            (None, Some(_), Some(_)) => Err(map_error(crate::Error::validation_field(
                "up_to is valid only with session_id",
                "up_to",
                request.up_to.clone().map(serde_json::Value::String),
                Some("only valid with session_id".to_owned()),
            ))),
            (Some(_), Some(_), _) => Err(map_error(crate::Error::validation_field(
                "session_id and message_id are mutually exclusive",
                "message_id",
                request.message_id.clone().map(serde_json::Value::String),
                Some("omit when session_id is present".to_owned()),
            ))),
            (None, None, _) => Err(map_error(crate::Error::validation(
                "one of session_id or message_id is required",
            ))),
        };

        match result {
            Ok(result) => GetEnvelope::Success(GetResponse {
                result,
                request_id: crate::wire::new_request_id(),
            }),
            Err(error) => GetEnvelope::Error(error),
        }
    }

    async fn session_scope(
        store: &Store,
        request: &GetRequest,
        session_id: &str,
        up_to: Option<&str>,
    ) -> Result<GetResult, crate::wire::ErrorEnvelope> {
        let Some(mut stored) = store.get_session(session_id).await.map_err(map_storage)? else {
            return Err(map_error(crate::Error::not_found(
                "session",
                serde_json::json!(session_id),
                format!("session not found: {session_id}"),
            )));
        };

        if let Some(up_to) = up_to {
            let Some(index) = stored
                .messages
                .iter()
                .position(|message| message.message.id() == up_to)
            else {
                return Err(map_error(crate::Error::not_found(
                    "message",
                    serde_json::json!([session_id, up_to]),
                    format!("up_to message not found in session: {session_id}/{up_to}"),
                )));
            };
            stored.messages.truncate(index + 1);
        }

        let max_messages = request.max_messages.min(1000);
        if stored.messages.len() > max_messages {
            stored.messages = stored.messages[stored.messages.len() - max_messages..].to_vec();
        }
        filter_session(
            &mut stored,
            request.include_thinking,
            request.include_tool_results,
        );
        let session = stored.session;
        let (messages, parts) = into_canonical(stored.messages);
        Ok(GetResult::Session {
            session,
            messages,
            parts,
        })
    }

    async fn message_scope(
        store: &Store,
        request: &GetRequest,
        message_id: &str,
    ) -> Result<GetResult, crate::wire::ErrorEnvelope> {
        let Some((session, mut messages)) = store
            .get_message_context(message_id, request.context_depth)
            .await
            .map_err(map_storage)?
        else {
            return Err(map_error(crate::Error::not_found(
                "message",
                serde_json::json!(message_id),
                format!("message not found: {message_id}"),
            )));
        };
        filter_messages(
            &mut messages,
            request.include_thinking,
            request.include_tool_results,
        );
        let (messages, parts) = into_canonical(messages);
        Ok(GetResult::Message {
            session,
            messages,
            parts,
        })
    }

    fn filter_session(
        session: &mut SessionWithMessages,
        include_thinking: bool,
        include_tool_results: bool,
    ) {
        filter_messages(
            &mut session.messages,
            include_thinking,
            include_tool_results,
        );
    }

    fn filter_messages(
        messages: &mut Vec<MessageWithParts>,
        include_thinking: bool,
        include_tool_results: bool,
    ) {
        for message in messages.iter_mut() {
            message.parts.retain(|part| match &part.kind {
                PartKind::Reasoning { .. } => include_thinking,
                PartKind::ToolResult { .. } => include_tool_results,
                PartKind::ToolApprovalRequest { .. } | PartKind::ToolApprovalResponse { .. } => {
                    false
                }
                PartKind::Text { .. } | PartKind::File { .. } | PartKind::ToolCall { .. } => true,
            });
        }

        messages.retain(|message| {
            message.message.role() != crate::wire::Role::Tool || !message.parts.is_empty()
        });
    }

    fn into_canonical(messages: Vec<MessageWithParts>) -> (Vec<Message>, Vec<Part>) {
        let mut canonical_messages = Vec::with_capacity(messages.len());
        let mut canonical_parts = Vec::new();
        for message in messages {
            canonical_messages.push(message.message);
            canonical_parts.extend(message.parts);
        }
        (canonical_messages, canonical_parts)
    }
}

pub use get_handler::pond_get;

mod search_handler {
    //! The `pond_search` handler: hybrid (vector + BM25 + RRF) retrieval at message
    //! granularity, with filter pushdown, recency boost, and conversation grouping
    //! (spec.md#search, spec.md#search).

    use crate::{
        Clock, SystemClock,
        embed::{EmbedBackend, e5_query},
        sessions::{MessageKey, MessageMeta, Store},
        substrate::{Predicate, ScalarValue},
        wire::{
            ErrorEnvelope, Group, Hit, ProjectFilter, SearchEnvelope, SearchFilters, SearchRequest,
            SearchResponse, SearchResultBody, new_request_id, validate_protocol,
        },
    };
    use chrono::{DateTime, NaiveDate, Utc};

    use super::{map_error, map_storage};

    /// Internal-only branching enum for the retrieval mode. The wire layer doesn't
    /// expose this - per-hit `matched_via` already tells clients which retrievers
    /// ranked a row, and the request never asks for a specific mode.
    // TEMP EXPERIMENT (embeddings-benchmark): `Vector` variant added so the harness
    // can force vector-only retrieval via `POND_SEARCH_MODE=vector`. Revert before merge.
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
        pub pool: usize,
        pub vector_pool: usize,
        pub limit: usize,
        pub rrf_k: u32,
        pub boost_recent: bool,
        pub group_by_conversation: bool,
        pub min_score: f64,
    }

    /// Server-enforced cap on `limit` (spec.md#search).
    const LIMIT_CAP: usize = 200;
    /// Hit-payload size bounds in code points (spec.md#search): a message's
    /// indexed text is returned in full at or below `HIT_TEXT_FULL`; above it,
    /// the text is truncated to `HIT_TEXT_FULL` and a match-windowed snippet of
    /// up to `HIT_SNIPPET_CHARS` is added.
    const HIT_TEXT_FULL: usize = 2000;
    const HIT_SNIPPET_CHARS: usize = 400;
    /// Recency-boost constants (spec.md#search).
    // Additive recency boost (spec.md#search). The cap is calibrated to act as
    // a tiebreaker, not a primary signal: with RRF k=10 the fused base score
    // tops out near 0.18 (dual-arm rank 1), so a 0.2-class boost would let a
    // fresh-but-irrelevant hit outscore a perfectly-relevant old one. 0.05
    // keeps recency at roughly 25% of a dual-arm rank-1 base, still material
    // enough to break ties among comparably-scored hits but not enough to flip
    // a strong relevance signal.
    const RECENCY_MAX_BOOST: f64 = 0.05;
    const RECENCY_DECAY_SECONDS: f64 = 604_800.0;

    // Asymmetric per-arm RRF k (Bruch, Gai, Ingber 2022 - "off-diagonal" finding,
    // arXiv 2210.11934). For pond's keyword-heavy corpus FTS is the higher-
    // precision arm, so we sharpen its rank curve (smaller k) and flatten the
    // vector arm's (larger k). The wire-level `rrf_k` is treated as a global
    // scaling parameter; we derive `k_fts = rrf_k / 2` and `k_vec = rrf_k * 2`
    // from it so a caller raising or lowering `rrf_k` still slides both arms
    // along the same axis. Sweep at `bench/embeddings/simulate_fusion.py`
    // identified a wide plateau at k_fts in [5,8], k_vec in [15,20]; the
    // (5, 20) centroid is the default.
    fn rrf_k_for(arm: RetrieverKind, base: u32) -> u32 {
        match arm {
            RetrieverKind::Fts => (base / 2).max(1),
            RetrieverKind::Vector => base.saturating_mul(2).max(1),
        }
    }

    /// Per-query fusion config. The default (Latin-dominant queries) uses the
    /// asymmetric `k_fts = rrf_k/2, k_vec = rrf_k*2` ratio that wins the
    /// EN benchmark. When the query is non-Latin-dominant (cross-lingual:
    /// Ukrainian/Russian/etc. query against a mostly-English corpus), the FTS
    /// ngram tokenizer can no longer reach the answer; the vector arm becomes
    /// the load-bearing signal, so we collapse to balanced k and double the
    /// vector weight (`w_vec = 2`). See `docs/researches/uk-cross-lingual-
    /// benchmark.md`.
    struct FusionConfig {
        k_fts: u32,
        k_vec: u32,
        w_fts: f64,
        w_vec: f64,
    }

    fn fusion_config_for(query: &str, base_rrf_k: u32) -> FusionConfig {
        if is_non_latin_dominant(query) {
            FusionConfig {
                k_fts: base_rrf_k,
                k_vec: base_rrf_k,
                w_fts: 1.0,
                w_vec: 2.0,
            }
        } else {
            FusionConfig {
                k_fts: rrf_k_for(RetrieverKind::Fts, base_rrf_k),
                k_vec: rrf_k_for(RetrieverKind::Vector, base_rrf_k),
                w_fts: 1.0,
                w_vec: 1.0,
            }
        }
    }

    /// Returns true when more than ~30% of the query's alphabetic characters
    /// are non-Latin (Cyrillic, CJK, Greek, Arabic, etc.). A short heuristic
    /// keyed off Unicode general-category rather than locale guessing: the
    /// FTS arm's character-ngram tokenizer cannot bridge such queries to the
    /// pond corpus's predominantly-English search_text, so the fusion needs
    /// to lean on the vector arm.
    fn is_non_latin_dominant(query: &str) -> bool {
        let mut latin = 0usize;
        let mut non_latin = 0usize;
        for ch in query.chars() {
            if ch.is_alphabetic() {
                if ch.is_ascii() {
                    latin += 1;
                } else {
                    non_latin += 1;
                }
            }
        }
        let total = latin + non_latin;
        total > 0 && (non_latin * 10) >= (total * 3)
    }

    /// Unindexed-row count above which `pond_search` logs that the FTS index is
    /// behind (spec.md#index-upkeep). Search results stay correct regardless -
    /// the engine flat-scans the not-yet-folded tail.
    const INDEX_BACKLOG_WARN: usize = 10_000;

    /// Run a hybrid or FTS-only search. The mode is server-determined - hybrid when
    /// `embedder` is `Some` AND at least one message is embedded under the
    /// configured model, FTS-only otherwise. The response has no top-level mode
    /// field; per-hit `matched_via` reports the retrievers that ranked each row.
    ///
    /// Must run on a multi-threaded Tokio runtime: hybrid mode embeds the query via
    /// `block_in_place`, which panics on a `current_thread` runtime.
    pub async fn pond_search(
        store: &Store,
        embedder: Option<&dyn EmbedBackend>,
        request: SearchRequest,
    ) -> SearchEnvelope {
        match run_search(store, embedder, request, &SystemClock).await {
            Ok(response) => SearchEnvelope::Success(response),
            Err(envelope) => SearchEnvelope::Error(envelope),
        }
    }

    async fn run_search(
        store: &Store,
        embedder: Option<&dyn EmbedBackend>,
        request: SearchRequest,
        clock: &dyn Clock,
    ) -> Result<SearchResponse, ErrorEnvelope> {
        let mut plan = plan_search(request, SearchMode::Fts)?;

        // The mode is server-determined: hybrid only when both the embedder is
        // loaded AND messages are embedded under the configured model. Anything
        // else degrades to FTS-only - a vector retriever over zero rows would
        // just be wasted work.
        plan.mode = resolve_effective_mode(store, embedder).await?;

        // spec.md#index-upkeep: results stay correct against the not-yet-folded
        // tail (the engine flat-scans it), but a large backlog hurts latency -
        // surface it so an operator knows the index fold is behind.
        match store.unindexed_message_backlog().await {
            Ok(backlog) if backlog > INDEX_BACKLOG_WARN => {
                tracing::warn!(
                    backlog,
                    "messages FTS index is behind; search is correct but slower until the fold catches up"
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, "could not measure FTS index backlog");
            }
        }
        let candidates = match plan.mode {
            SearchMode::Fts => {
                let hits = store
                    .fts_search(&plan.query, plan.pool, &plan.filter)
                    .await
                    .map_err(map_storage)?;
                normalize_fts(hits)
            }
            SearchMode::Hybrid => {
                // `resolve_effective_mode` only returns Hybrid when `embedder` is
                // `Some` and `has_embeddings` returned true; an `Internal` error
                // here would only fire under a logic bug.
                let Some(embedder) = embedder else {
                    return Err(map_error(crate::Error::internal(
                        "hybrid mode resolved without an embedder",
                    )));
                };
                let vector = embed_query(embedder, &plan.query)?;
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
                        .vector_search(&vector, plan.vector_pool, &plan.filter)
                        .await
                        .map_err(map_storage)
                };
                let (fts, vector_raw) = tokio::try_join!(fts_fut, vector_fut)?;
                // Query-language-aware fusion config: Latin queries use the
                // EN-tuned asymmetric k; non-Latin (cross-lingual) queries
                // collapse to balanced k with vector-heavy weighting because
                // the FTS arm cannot bridge across languages.
                let cfg = fusion_config_for(&plan.query, plan.rrf_k);
                // FTS first: when both arms picked different messages from the
                // same session_root, RRF will keep FTS's representative (better
                // for hit display since BM25 highlights the lexical match).
                let lists = [
                    RankedList {
                        retriever: RetrieverKind::Fts,
                        keys: fts.into_iter().map(|(key, _)| key).collect(),
                        k: cfg.k_fts,
                        weight: cfg.w_fts,
                    },
                    RankedList {
                        retriever: RetrieverKind::Vector,
                        keys: vector_raw.into_iter().map(|(key, _)| key).collect(),
                        k: cfg.k_vec,
                        weight: cfg.w_vec,
                    },
                ];
                rrf_merge(&lists)
                    .into_iter()
                    .map(|hit| Candidate {
                        session_id: hit.key.session_id,
                        message_id: hit.key.message_id,
                        base_score: hit.score,
                        matched_via: hit.matched_via,
                    })
                    .collect()
            }
            // TEMP EXPERIMENT (embeddings-benchmark): vector-only retrieval for
            // the FTS-vs-Vector-vs-Hybrid ablation. Revert before merge.
            SearchMode::Vector => {
                let Some(embedder) = embedder else {
                    return Err(map_error(crate::Error::internal(
                        "vector mode resolved without an embedder",
                    )));
                };
                let vector = embed_query(embedder, &plan.query)?;
                let vector_raw = store
                    .vector_search(&vector, plan.vector_pool, &plan.filter)
                    .await
                    .map_err(map_storage)?;
                normalize_vector(vector_raw)
            }
        };

        if candidates.is_empty() {
            return Ok(empty_response(plan.group_by_conversation));
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

        let now = clock.now();
        let mut scored = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let Some(meta) =
                meta_index.get(&(candidate.session_id.as_str(), candidate.message_id.as_str()))
            else {
                continue;
            };
            let recency_boost = if plan.boost_recent {
                recency_boost(meta.timestamp, now)
            } else {
                0.0
            };
            let score = candidate.base_score + recency_boost;
            if score < plan.min_score {
                continue;
            }
            scored.push(ScoredHit {
                meta: (*meta).clone(),
                base_score: candidate.base_score,
                recency_boost,
                score,
                matched_via: candidate.matched_via,
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

        if plan.group_by_conversation {
            let groups = build_groups(store, &scored, plan.limit, &plan.query).await?;
            let total = groups.len();
            Ok(SearchResponse {
                result: SearchResultBody::Groups { groups },
                total,
                request_id: new_request_id(),
            })
        } else {
            let hits = scored
                .into_iter()
                .take(plan.limit)
                .map(|hit| hit.into_hit(&plan.query))
                .collect::<Vec<_>>();
            let total = hits.len();
            Ok(SearchResponse {
                result: SearchResultBody::Hits { hits },
                total,
                request_id: new_request_id(),
            })
        }
    }

    /// Pick the retrieval mode based on the embedder state. Hybrid requires both a
    /// loaded embedder and at least one message embedded under the configured
    /// model; otherwise FTS-only.
    async fn resolve_effective_mode(
        store: &Store,
        embedder: Option<&dyn EmbedBackend>,
    ) -> Result<SearchMode, ErrorEnvelope> {
        // TEMP EXPERIMENT (embeddings-benchmark): `POND_SEARCH_MODE` overrides the
        // server-decided mode so the harness can run the same query under
        // {fts,vector,hybrid} against the same corpus. Revert before merge.
        if let Ok(forced) = std::env::var("POND_SEARCH_MODE") {
            let mode = match forced.as_str() {
                "fts" => SearchMode::Fts,
                "vector" => SearchMode::Vector,
                "hybrid" => SearchMode::Hybrid,
                other => {
                    return Err(map_error(crate::Error::internal(format!(
                        "POND_SEARCH_MODE must be one of fts|vector|hybrid, got `{other}`"
                    ))));
                }
            };
            if matches!(mode, SearchMode::Vector | SearchMode::Hybrid) && embedder.is_none() {
                return Err(map_error(crate::Error::internal(format!(
                    "POND_SEARCH_MODE=`{forced}` requires an embedder, but none is loaded"
                ))));
            }
            return Ok(mode);
        }
        match embedder {
            None => Ok(SearchMode::Fts),
            Some(_) => {
                let has = store.has_embeddings().await.map_err(map_storage)?;
                Ok(if has {
                    SearchMode::Hybrid
                } else {
                    SearchMode::Fts
                })
            }
        }
    }

    pub fn plan_search(
        request: SearchRequest,
        mode: SearchMode,
    ) -> Result<SearchPlan, ErrorEnvelope> {
        validate_protocol(request.protocol_version)?;

        let _ns = super::resolve_namespace(request.namespace.as_deref())?;

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
        let filter = build_filter(&request.filters)?;
        // Retriever candidate pool: wider than `limit` so RRF has material to merge.
        let pool = limit.saturating_mul(5).max(50);
        Ok(SearchPlan {
            mode,
            query,
            filter,
            pool,
            vector_pool: pool.saturating_mul(2),
            limit,
            rrf_k: request.rrf_k,
            boost_recent: request.boost_recent,
            group_by_conversation: request.group_by_conversation,
            min_score: request.filters.min_score,
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

    /// A retriever-ranked list of message primary keys, best-first. `k` is the
    /// RRF constant for THIS arm's contribution - asymmetric per-arm k lets
    /// pond reward the more reliable arm's top ranks more sharply. For pond's
    /// keyword-heavy corpus FTS is the higher-precision arm, so the call site
    /// pairs `k_fts` ~5 (sharper) with `k_vec` ~20 (flatter); see RRF_K_FTS,
    /// RRF_K_VECTOR. The asymmetric-k pattern is the "off-diagonal" finding of
    /// Bruch, Gai, Ingber 2022 (arXiv 2210.11934).
    pub struct RankedList {
        pub retriever: RetrieverKind,
        pub keys: Vec<MessageKey>,
        pub k: u32,
        /// Linear multiplier on this arm's contributions, defaults to 1.0.
        /// Used by the query-language router to up-weight the vector arm when
        /// the query is non-Latin-dominant (cross-lingual): the FTS arm's
        /// ngram tokenizer cannot bridge a Ukrainian query to an English
        /// answer, so vector becomes the load-bearing arm.
        pub weight: f64,
    }

    /// One merged RRF result.
    #[derive(Debug, Clone, PartialEq)]
    pub struct RrfHit {
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

    /// Reciprocal Rank Fusion keyed on the conversation root: each retriever
    /// contributes at most one ballot per session_root (the highest-ranked
    /// message it returned for that root), and ballots are summed across
    /// retrievers as `sum(1 / (k + rank))`. The representative message_id is
    /// the first one each arm picked for the root; when both arms picked
    /// different messages from the same root, the first arm in the `lists`
    /// argument wins the representative (callers should list FTS first when
    /// FTS-side provenance is preferred for the displayed hit). Ties break on
    /// the representative key for determinism (spec.md#search).
    ///
    /// Why session-root keying instead of `(session_id, message_id)`: a long
    /// session whose best FTS message and best vector message differ would
    /// otherwise appear as two separate fused hits, neither getting the
    /// cross-arm validation bonus. Keying on the root credits cross-arm
    /// agreement at the conversation level - which is what the user sees.
    pub fn rrf_merge(lists: &[RankedList]) -> Vec<RrfHit> {
        let mut merged: std::collections::HashMap<String, (f64, Vec<String>, MessageKey)> =
            std::collections::HashMap::new();
        for list in lists {
            let k = f64::from(list.k.max(1));
            let mut seen_in_arm: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            // The rank that drives the RRF contribution is the position in the
            // DEDUPED arm list, not the raw scanner output. Without this, an
            // arm that returns several messages from one long session at its
            // top inflates the ranks of every subsequent session by the size
            // of the duplicate run, suppressing real cross-arm agreement.
            let mut dedup_rank: usize = 0;
            for key in &list.keys {
                let root = session_root(&key.session_id).to_owned();
                if !seen_in_arm.insert(root.clone()) {
                    continue;
                }
                dedup_rank += 1;
                let contribution = list.weight / (k + dedup_rank as f64);
                let entry = merged
                    .entry(root)
                    .or_insert_with(|| (0.0, Vec::new(), key.clone()));
                entry.0 += contribution;
                entry.1.push(list.retriever.as_wire().to_owned());
            }
        }
        let mut hits = merged
            .into_values()
            .map(|(score, matched_via, key)| RrfHit {
                key,
                score,
                matched_via,
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

    /// Additive exponential-decay recency boost (spec.md#search): caps at
    /// [`RECENCY_MAX_BOOST`] at `age = 0`, decays to near-zero past a few weeks.
    pub fn recency_boost(timestamp: DateTime<Utc>, now: DateTime<Utc>) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let age_seconds = (now - timestamp).num_seconds().max(0) as f64;
        RECENCY_MAX_BOOST * (-age_seconds / RECENCY_DECAY_SECONDS).exp()
    }

    /// Build a hit's `(text, snippet)` payload (spec.md#search): the matched
    /// message's indexed text in full when small, or a bounded prefix plus a
    /// query-windowed snippet when it exceeds [`HIT_TEXT_FULL`].
    pub fn hit_payload(text: &str, query: &str) -> (String, Option<String>) {
        let chars: Vec<char> = text.chars().collect();
        if chars.len() <= HIT_TEXT_FULL {
            return (text.to_owned(), None);
        }
        let truncated = chars[..HIT_TEXT_FULL].iter().collect::<String>();
        (truncated, Some(query_snippet(text, query)))
    }

    /// A snippet windowed around the first query term found in `text`, capped
    /// at [`HIT_SNIPPET_CHARS`] code points. Falls back to the text head when
    /// no term matches.
    fn query_snippet(text: &str, query: &str) -> String {
        let lower_text = text.to_lowercase();
        let hit = query
            .split_whitespace()
            .filter(|term| !term.is_empty())
            .filter_map(|term| lower_text.find(&term.to_lowercase()))
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
        let mut snippet = String::new();
        if start > 0 {
            snippet.push_str("...");
        }
        snippet.extend(&chars[start..end]);
        if end < chars.len() {
            snippet.push_str("...");
        }
        snippet
    }

    struct Candidate {
        session_id: String,
        message_id: String,
        base_score: f64,
        matched_via: Vec<String>,
    }

    struct ScoredHit {
        meta: MessageMeta,
        base_score: f64,
        recency_boost: f64,
        score: f64,
        matched_via: Vec<String>,
    }

    impl ScoredHit {
        fn into_hit(self, query: &str) -> Hit {
            let (text, snippet) = hit_payload(&self.meta.search_text, query);
            Hit {
                session_id: self.meta.session_id,
                message_id: self.meta.message_id,
                role: self.meta.role,
                timestamp: self.meta.timestamp,
                project: self.meta.project,
                source_agent: self.meta.source_agent,
                text,
                snippet,
                score: self.score,
                base_score: self.base_score,
                recency_boost: self.recency_boost,
                matched_via: self.matched_via,
            }
        }
    }

    /// Unbounded BM25 scores to a `[0, 1]` base score by dividing by the max in the
    /// result set.
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
                matched_via: vec![RetrieverKind::Fts.as_wire().to_owned()],
            })
            .collect()
    }

    // TEMP EXPERIMENT (embeddings-benchmark): rank-based normalization for vector-only
    // mode. The raw `_distance` is cosine distance from Lance; converting to a
    // monotone-in-rank `[0, 1]` score keeps the Hit payload comparable to FTS and
    // Hybrid (where `base_score` is also monotone in rank). Revert before merge.
    fn normalize_vector(hits: Vec<(MessageKey, f32)>) -> Vec<Candidate> {
        let n = hits.len() as f64;
        hits.into_iter()
            .enumerate()
            .map(|(idx, (key, _))| Candidate {
                session_id: key.session_id,
                message_id: key.message_id,
                base_score: if n > 0.0 { 1.0 - (idx as f64 / n) } else { 0.0 },
                matched_via: vec![RetrieverKind::Vector.as_wire().to_owned()],
            })
            .collect()
    }

    fn embed_query(embedder: &dyn EmbedBackend, query: &str) -> Result<Vec<f32>, ErrorEnvelope> {
        let prompt = e5_query(query);
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

    async fn build_groups(
        store: &Store,
        scored: &[ScoredHit],
        limit: usize,
        query: &str,
    ) -> Result<Vec<Group>, ErrorEnvelope> {
        use std::collections::BTreeMap;

        // `scored` is already sorted by score descending, so the first hit seen for
        // a session is its best-scoring match.
        struct Acc {
            project: String,
            source_agent: String,
            first_timestamp: DateTime<Utc>,
            last_timestamp: DateTime<Utc>,
            text: String,
            snippet: Option<String>,
            best_score: f64,
        }
        // Key by conversation root: a Claude Code sub-agent session under
        // `<parent>/agent-<id>` collapses into its parent, so one user-facing
        // conversation never occupies two slots in the grouped output.
        let mut groups: BTreeMap<String, Acc> = BTreeMap::new();
        for hit in scored {
            let root = session_root(&hit.meta.session_id).to_owned();
            let entry = groups.entry(root).or_insert_with(|| {
                let (text, snippet) = hit_payload(&hit.meta.search_text, query);
                Acc {
                    project: hit.meta.project.clone(),
                    source_agent: hit.meta.source_agent.clone(),
                    first_timestamp: hit.meta.timestamp,
                    last_timestamp: hit.meta.timestamp,
                    text,
                    snippet,
                    best_score: hit.score,
                }
            });
            entry.first_timestamp = entry.first_timestamp.min(hit.meta.timestamp);
            entry.last_timestamp = entry.last_timestamp.max(hit.meta.timestamp);
            entry.best_score = entry.best_score.max(hit.score);
        }

        let session_ids = groups.keys().cloned().collect::<Vec<_>>();
        let counts = store
            .session_message_counts(&session_ids)
            .await
            .map_err(map_storage)?;

        let mut result = groups
            .into_iter()
            .map(|(session_id, acc)| Group {
                message_count: counts.get(&session_id).copied().unwrap_or_default(),
                session_id,
                project: acc.project,
                source_agent: acc.source_agent,
                first_timestamp: acc.first_timestamp,
                last_timestamp: acc.last_timestamp,
                text: acc.text,
                snippet: acc.snippet,
                best_score: acc.best_score,
            })
            .collect::<Vec<_>>();
        result.sort_by(|left, right| {
            right
                .best_score
                .partial_cmp(&left.best_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        result.truncate(limit);
        Ok(result)
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
        if let Some(role) = &filters.role {
            if !matches!(role.as_str(), "user" | "assistant" | "system" | "tool") {
                return Err(map_error(crate::Error::validation_field(
                    format!(
                        "filters.role must be one of: user, assistant, system, tool; got {role}"
                    ),
                    "filters.role",
                    Some(serde_json::json!(role)),
                    Some("one of: user, assistant, system, tool".to_owned()),
                )));
            }
            clauses.push(Predicate::Eq("role", role.clone().into()));
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

    fn empty_response(group_by_conversation: bool) -> SearchResponse {
        let result = if group_by_conversation {
            SearchResultBody::Groups { groups: Vec::new() }
        } else {
            SearchResultBody::Hits { hits: Vec::new() }
        };
        SearchResponse {
            result,
            total: 0,
            request_id: new_request_id(),
        }
    }

    #[cfg(test)]
    mod fusion_helpers_tests {
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
        fn rrf_merge_dedupes_intra_arm_by_session_root_and_credits_cross_arm() {
            let mk = |sid: &str, mid: &str| crate::sessions::MessageKey {
                session_id: sid.to_owned(),
                message_id: mid.to_owned(),
            };
            // FTS: session-A msg-1 (rank 1), session-A msg-2 (rank 2, same root,
            // dropped by intra-arm dedup), session-B msg-3 (rank 3 -> effective 2),
            // session-A/agent-x msg-4 (rank 4, same root as A, dropped).
            // Vector: session-B msg-7 (rank 1, different message than FTS's pick
            // for B), session-A msg-9 (rank 2).
            let fts = RankedList {
                retriever: RetrieverKind::Fts,
                keys: vec![
                    mk("session-A", "msg-1"),
                    mk("session-A", "msg-2"),
                    mk("session-B", "msg-3"),
                    mk("session-A/agent-x", "msg-4"),
                ],
                k: 10,
                weight: 1.0,
            };
            let vec_arm = RankedList {
                retriever: RetrieverKind::Vector,
                keys: vec![mk("session-B", "msg-7"), mk("session-A", "msg-9")],
                k: 10,
                weight: 1.0,
            };
            let merged = rrf_merge(&[fts, vec_arm]);
            // Output: one row per session_root, sorted by fused score.
            assert_eq!(merged.len(), 2);
            // session-A: FTS rank 1 (1/11) + Vector rank 2 (1/12) = 0.174
            // session-B: FTS rank 2 (1/12) + Vector rank 1 (1/11) = 0.174
            // Equal fused scores; tie breaks on the representative key, where
            // session-A's `msg-1` sorts before session-B's `msg-3`.
            assert_eq!(merged[0].key.session_id, "session-A");
            assert_eq!(merged[0].key.message_id, "msg-1");
            assert_eq!(merged[0].matched_via, vec!["fts", "vector"]);
            assert_eq!(merged[1].key.session_id, "session-B");
            // FTS was listed first, so FTS's pick (msg-3) wins the representative
            // over Vector's pick (msg-7) for session-B.
            assert_eq!(merged[1].key.message_id, "msg-3");
            assert_eq!(merged[1].matched_via, vec!["fts", "vector"]);
        }

        #[test]
        fn fusion_config_routes_by_query_language() {
            // Latin-dominant queries get the EN-tuned asymmetric setup.
            let en = fusion_config_for("how does OCC retry work", 10);
            assert_eq!(en.k_fts, 5);
            assert_eq!(en.k_vec, 20);
            assert!((en.w_fts - 1.0).abs() < 1e-9);
            assert!((en.w_vec - 1.0).abs() < 1e-9);
            // Non-Latin-dominant queries (Ukrainian/Cyrillic) collapse to
            // balanced k with double vector weight.
            let uk = fusion_config_for("як працює OCC retry коли два писці", 10);
            assert_eq!(uk.k_fts, 10);
            assert_eq!(uk.k_vec, 10);
            assert!((uk.w_vec - 2.0).abs() < 1e-9);
            // Mixed queries with isolated identifiers stay Latin-dominant.
            let mixed = fusion_config_for("Extracted<T> Source primitive адаптер", 10);
            assert_eq!(mixed.k_fts, 5);
            assert_eq!(mixed.k_vec, 20);
        }

        #[test]
        fn is_non_latin_dominant_threshold_is_thirty_percent() {
            assert!(!is_non_latin_dominant("how does OCC retry work"));
            assert!(is_non_latin_dominant("як працює OCC retry"));
            // Threshold ~30%: a query with a single Cyrillic word among
            // mostly-English text stays Latin-dominant unless the Cyrillic
            // fraction crosses 30% of alphabetic characters.
            assert!(is_non_latin_dominant("test тест"));
            assert!(!is_non_latin_dominant("how does this work then тест"));
        }

        #[test]
        fn asymmetric_k_sharpens_fts_and_flattens_vector() {
            let mk = |sid: &str, mid: &str| crate::sessions::MessageKey {
                session_id: sid.to_owned(),
                message_id: mid.to_owned(),
            };
            // Scenario: a single-arm FTS rank-1 hit (target) versus a dual-arm
            // hit whose FTS rank is mediocre but vector rank is high.
            //   target: FTS rank 1, NOT in vector.
            //   noise:  FTS rank 3, vector rank 1.
            // Under equal k=10, target = 1/11 = 0.091; noise = 1/13 + 1/11 = 0.168.
            //   noise wins.
            // Under asymmetric k_fts=5, k_vec=20:
            //   target = 1/6 = 0.167; noise = 1/8 + 1/21 = 0.173. Tight.
            // The asymmetric setup is calibrated for the broader plateau on
            // pond's benchmark, not this single-query toy.
            let fts = RankedList {
                retriever: RetrieverKind::Fts,
                keys: vec![mk("target", "t1"), mk("filler", "f1"), mk("noise", "n1")],
                k: 5,
                weight: 1.0,
            };
            let vec_arm = RankedList {
                retriever: RetrieverKind::Vector,
                keys: vec![mk("noise", "n2")],
                k: 20,
                weight: 1.0,
            };
            let merged = rrf_merge(&[fts, vec_arm]);
            // Verify per-arm k applied as documented.
            let target = merged
                .iter()
                .find(|h| h.key.session_id == "target")
                .unwrap();
            let noise = merged.iter().find(|h| h.key.session_id == "noise").unwrap();
            let expected_target = 1.0 / (5.0 + 1.0);
            let expected_noise = 1.0 / (5.0 + 3.0) + 1.0 / (20.0 + 1.0);
            assert!((target.score - expected_target).abs() < 1e-9);
            assert!((noise.score - expected_noise).abs() < 1e-9);
        }
    }
}

pub use search_handler::{
    RankedList, RetrieverKind, RrfHit, SearchMode, SearchPlan, build_filter, hit_payload,
    plan_search, pond_search, recency_boost, rrf_merge,
};

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::wire::{ProjectFilter, SearchFilters, SearchRequest};
    use chrono::{Duration, Utc};

    fn search_request(query: &str) -> SearchRequest {
        SearchRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            query: query.to_owned(),
            rrf_k: 60,
            filters: SearchFilters::default(),
            boost_recent: true,
            group_by_conversation: false,
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
    fn rrf_merge_fuses_retrievers_and_reports_provenance() {
        // Each session contributes at most one ballot per arm; cross-arm
        // agreement is credited per session_root, not per message_id.
        let lists = [
            RankedList {
                retriever: RetrieverKind::Vector,
                keys: vec![
                    key("session-a", "a"),
                    key("session-b", "b"),
                    key("session-c", "c"),
                ],
                k: 60,
                weight: 1.0,
            },
            RankedList {
                retriever: RetrieverKind::Fts,
                keys: vec![
                    key("session-b", "b"),
                    key("session-a", "a"),
                    key("session-d", "d"),
                ],
                k: 60,
                weight: 1.0,
            },
        ];
        let merged = rrf_merge(&lists);

        // session-a (vector rank 1, FTS rank 2) and session-b (vector rank 2,
        // FTS rank 1) have equal fused scores; tie breaks on (session_id,
        // message_id) of the representative, so session-a sorts first. Both
        // beat the single-retriever session-c and session-d.
        assert_eq!(merged[0].key.session_id, "session-a");
        assert_eq!(merged[1].key.session_id, "session-b");
        assert_eq!(merged[0].matched_via, vec!["vector", "fts"]);
        assert!(merged[0].score > merged[2].score);

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
    fn recency_boost_matches_the_kb_formula() {
        let now = Utc::now();
        // Caps at +0.05 at age zero (tiebreaker-scale boost; see RECENCY_MAX_BOOST).
        assert!((recency_boost(now, now) - 0.05).abs() < 1e-6);
        // One half-life (7 days) decays by exactly 1/e.
        let week = recency_boost(now - Duration::days(7), now);
        assert!((week - 0.05 / std::f64::consts::E).abs() < 1e-3);
        // A year out is effectively zero.
        assert!(recency_boost(now - Duration::days(365), now) < 1e-3);
        // Future timestamps clamp to the cap rather than exceeding it.
        assert!((recency_boost(now + Duration::days(1), now) - 0.05).abs() < 1e-6);
    }

    #[test]
    fn hit_payload_returns_short_text_in_full_with_no_snippet() {
        let short = "a short message body";
        let (text, snippet) = hit_payload(short, "message");
        assert_eq!(text, short);
        assert!(snippet.is_none(), "small text needs no snippet");
    }

    #[test]
    fn hit_payload_truncates_long_text_and_windows_the_snippet() {
        // 2400 chars: a filler head, the query term mid-body, a filler tail.
        let body = format!("{}NEEDLE{}", "a".repeat(2000), "b".repeat(394));
        let (text, snippet) = hit_payload(&body, "needle");
        assert_eq!(text.chars().count(), 2000, "text truncates to the bound");
        let snippet = snippet.expect("long text carries a snippet");
        assert!(
            snippet.contains("NEEDLE"),
            "snippet windows on the query term: {snippet}"
        );
        assert!(snippet.chars().count() <= 400 + 6, "snippet is bounded");
    }

    #[test]
    fn hit_payload_snippet_survives_case_folding_that_changes_byte_length() {
        // `to_lowercase` of 'İ' is two code points, so the lowercased copy has
        // a different byte layout than the original. A query offset taken from
        // that copy must never be sliced into the original text.
        let body = format!("İÉÉÉ{}", "a".repeat(2100));
        let (text, snippet) = hit_payload(&body, "ééé");
        assert_eq!(
            text.chars().count(),
            2000,
            "long text truncates to the bound"
        );
        let snippet = snippet.expect("long text carries a snippet");
        assert!(
            snippet.contains("ÉÉÉ"),
            "snippet windows on the matched term: {snippet}"
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
            role: Some("assistant".to_owned()),
            from_date: Some("2026-01-01".to_owned()),
            to_date: Some("2026-05-01".to_owned()),
            min_score: 0.0,
        };
        let sql = build_filter(&filters).unwrap().to_lance();
        assert!(sql.contains("project LIKE '%/Users/me/pond%'"));
        assert!(sql.contains("session_id = '01HXY'"));
        assert!(sql.contains("source_agent = 'claude-code'"));
        assert!(sql.contains("role = 'assistant'"));
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("timestamp <="));

        // Empty filters produce no predicate.
        assert_eq!(
            build_filter(&SearchFilters::default()).unwrap().to_lance(),
            "",
        );
    }

    #[test]
    fn build_filter_rejects_bad_role_and_date() {
        let bad_role = SearchFilters {
            role: Some("wizard".to_owned()),
            ..SearchFilters::default()
        };
        assert!(build_filter(&bad_role).is_err());

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
        // Case 1: large limit + group + filters + min_score. Query gets trimmed,
        // limit caps at 200, pools size off `limit * k`.
        let mut request = search_request("  vector memory  ");
        request.limit = 500;
        request.group_by_conversation = true;
        request.boost_recent = false;
        request.filters.min_score = 0.42;
        let plan = plan_search(request, SearchMode::Hybrid).unwrap();
        assert_eq!(plan.mode, SearchMode::Hybrid);
        assert_eq!(plan.query, "vector memory");
        assert_eq!(plan.limit, 200);
        assert_eq!(plan.pool, 1000);
        assert_eq!(plan.vector_pool, 2000);
        assert!(plan.group_by_conversation);
        assert!(!plan.boost_recent);
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
        request.filters.role = Some("assistant".to_owned());
        let plan = plan_search(request, SearchMode::Fts).unwrap();
        let sql = plan.filter.to_lance();
        assert!(sql.contains("project LIKE"));
        assert!(sql.contains("role = 'assistant'"));
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
