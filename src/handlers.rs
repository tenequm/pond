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
            /// First drop's error message; subsequent drops counted, not
            /// retained. Full detail at `POND_LOG=pond=debug`.
            first_drop_reason: Option<String>,
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
        let final_outcomes = validator.finish(store).await?;
        validator_total += validator_start.elapsed();
        validator_count += 1;
        summary.add_outcomes(&final_outcomes);
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
    //! order, emit a canonical-content stub per row, then `end` and close.
    //! Live-tail activates with live-write (section 4) on the same endpoint
    //! without a wire change. Full-fidelity event replay (parts, metadata)
    //! is not in scope - agents fetch parts via `pond_get(include_parts=true)`.

    use crate::{
        sessions::{MessageWithParts, Store},
        wire::ErrorEnvelope,
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
    ) -> Result<Vec<SseEvent>, ErrorEnvelope> {
        let stored = store.get_session(session_id).await.map_err(map_storage)?;
        let Some(stored) = stored else {
            return Err(map_error(crate::Error::not_found(
                "session",
                json!(session_id),
                "session not found",
            )));
        };

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
            events.push(message_event(message));
        }
        events.push(end_event(session_id));
        Ok(events)
    }

    fn message_event(message: &MessageWithParts) -> SseEvent {
        SseEvent {
            event: "message",
            id: message.message.id().to_owned(),
            data: json!({ "message": message.message }),
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
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde::{Deserialize, Serialize};

    use crate::{
        sessions::{MessageWindow, PagedScope, PagedSessionView, Store},
        wire::{GetEnvelope, GetRequest, GetResponse, GetResult, validate_protocol},
    };

    use super::{map_error, map_storage};

    /// ~10000-token response budget at the `chars/4` estimator.
    const BUDGET_CHARS: usize = 40_000;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum CursorScope {
        Session,
        Message,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Cursor {
        scope: CursorScope,
        anchor_id: String,
        after_message_id: String,
        #[serde(default)]
        include_parts: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        up_to: Option<String>,
        #[serde(default)]
        context_depth: usize,
    }

    fn encode_cursor(cursor: &Cursor) -> String {
        // Cursor is plain owned types (String / Option<String> / bool / usize /
        // enum); serde_json can't fail on it, so an explicit panic message
        // beats threading an infallible Result up to the response shape.
        #[allow(clippy::expect_used)]
        let bytes = serde_json::to_vec(cursor).expect("cursor encodes as JSON");
        URL_SAFE_NO_PAD.encode(bytes)
    }

    fn decode_cursor(raw: &str) -> Result<Cursor, crate::wire::ErrorEnvelope> {
        let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| {
            map_error(crate::Error::validation_field(
                "cursor is malformed (expected opaque value from a prior response)",
                "cursor",
                Some(serde_json::json!(raw)),
                Some("opaque base64url".to_owned()),
            ))
        })?;
        serde_json::from_slice(&bytes).map_err(|_| {
            map_error(crate::Error::validation_field(
                "cursor is malformed (decode failed)",
                "cursor",
                Some(serde_json::json!(raw)),
                Some("opaque cursor from a prior response".to_owned()),
            ))
        })
    }

    pub async fn pond_get(store: &Store, request: GetRequest) -> GetEnvelope {
        if let Err(error) = validate_protocol(request.protocol_version) {
            return GetEnvelope::Error(error);
        }
        if let Err(envelope) = super::resolve_namespace(request.namespace.as_deref()) {
            return GetEnvelope::Error(envelope);
        }

        let cursor = match request.cursor.as_deref() {
            Some(raw) => Some(match decode_cursor(raw) {
                Ok(cursor) => cursor,
                Err(envelope) => return GetEnvelope::Error(envelope),
            }),
            None => None,
        };

        let outcome = match cursor {
            Some(cursor) => match cursor.scope {
                CursorScope::Session => {
                    session_page(
                        store,
                        cursor.anchor_id.clone(),
                        cursor.up_to.clone(),
                        Some(cursor.after_message_id.clone()),
                        request.max_messages,
                        cursor.include_parts,
                    )
                    .await
                }
                CursorScope::Message => {
                    message_page(
                        store,
                        cursor.anchor_id.clone(),
                        cursor.context_depth,
                        Some(cursor.after_message_id.clone()),
                        request.max_messages,
                        cursor.include_parts,
                    )
                    .await
                }
            },
            None => match (&request.session_id, &request.message_id, &request.up_to) {
                (Some(session_id), None, up_to) => {
                    session_page(
                        store,
                        session_id.clone(),
                        up_to.clone(),
                        None,
                        request.max_messages,
                        request.include_parts,
                    )
                    .await
                }
                (None, Some(message_id), None) => {
                    message_page(
                        store,
                        message_id.clone(),
                        request.context_depth,
                        None,
                        request.max_messages,
                        request.include_parts,
                    )
                    .await
                }
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
                    "one of session_id, message_id, or cursor is required",
                ))),
            },
        };

        match outcome {
            Ok((result, has_more, next_cursor)) => GetEnvelope::Success(GetResponse {
                result,
                has_more,
                next_cursor,
                request_id: crate::wire::new_request_id(),
            }),
            Err(error) => GetEnvelope::Error(error),
        }
    }

    async fn session_page(
        store: &Store,
        session_id: String,
        up_to: Option<String>,
        start_after: Option<String>,
        max_messages: usize,
        include_parts: bool,
    ) -> Result<(GetResult, bool, Option<String>), crate::wire::ErrorEnvelope> {
        let scope = PagedScope {
            up_to: up_to.as_deref(),
            start_after: start_after.as_deref(),
            window: None,
            max_messages,
            budget_chars: BUDGET_CHARS,
            include_parts,
        };
        let view = store
            .paged_session_view(&session_id, scope)
            .await
            .map_err(map_storage)?;
        let Some(view) = view else {
            return Err(map_error(crate::Error::not_found(
                "session",
                serde_json::json!(session_id),
                format!("session not found: {session_id}"),
            )));
        };
        let (next_cursor, has_more) = build_next_cursor(
            &view,
            CursorScope::Session,
            &session_id,
            up_to.clone(),
            0,
            include_parts,
        );
        Ok((
            GetResult::Session {
                session: view.session,
                messages: view.messages,
                parts: view.parts,
            },
            has_more,
            next_cursor,
        ))
    }

    async fn message_page(
        store: &Store,
        message_id: String,
        context_depth: usize,
        start_after: Option<String>,
        max_messages: usize,
        include_parts: bool,
    ) -> Result<(GetResult, bool, Option<String>), crate::wire::ErrorEnvelope> {
        let session_id = store
            .session_id_for_message(&message_id)
            .await
            .map_err(map_storage)?
            .ok_or_else(|| {
                map_error(crate::Error::not_found(
                    "message",
                    serde_json::json!(message_id),
                    format!("message not found: {message_id}"),
                ))
            })?;
        let scope = PagedScope {
            up_to: None,
            start_after: start_after.as_deref(),
            window: Some(MessageWindow {
                anchor: &message_id,
                depth: context_depth,
            }),
            max_messages,
            budget_chars: BUDGET_CHARS,
            include_parts,
        };
        let view = store
            .paged_session_view(&session_id, scope)
            .await
            .map_err(map_storage)?;
        let Some(view) = view else {
            return Err(map_error(crate::Error::not_found(
                "session",
                serde_json::json!(session_id),
                format!("session not found: {session_id}"),
            )));
        };
        let (next_cursor, has_more) = build_next_cursor(
            &view,
            CursorScope::Message,
            &message_id,
            None,
            context_depth,
            include_parts,
        );
        Ok((
            GetResult::Message {
                session: view.session,
                messages: view.messages,
                parts: view.parts,
            },
            has_more,
            next_cursor,
        ))
    }

    fn build_next_cursor(
        view: &PagedSessionView,
        scope: CursorScope,
        anchor_id: &str,
        up_to: Option<String>,
        context_depth: usize,
        include_parts: bool,
    ) -> (Option<String>, bool) {
        match view.next_after.as_deref() {
            Some(after) => (
                Some(encode_cursor(&Cursor {
                    scope,
                    anchor_id: anchor_id.to_owned(),
                    after_message_id: after.to_owned(),
                    include_parts,
                    up_to,
                    context_depth,
                })),
                view.has_more,
            ),
            None => (None, false),
        }
    }

    #[cfg(test)]
    mod tests {
        #![allow(clippy::unwrap_used)]
        use super::*;

        #[test]
        fn cursor_round_trips_through_base64url_json() {
            let cursor = Cursor {
                scope: CursorScope::Session,
                anchor_id: "s-1".to_owned(),
                after_message_id: "m-7".to_owned(),
                include_parts: true,
                up_to: Some("m-9".to_owned()),
                context_depth: 0,
            };
            let raw = encode_cursor(&cursor);
            let back = decode_cursor(&raw).unwrap();
            assert!(matches!(back.scope, CursorScope::Session));
            assert_eq!(back.anchor_id, "s-1");
            assert_eq!(back.after_message_id, "m-7");
            assert!(back.include_parts);
            assert_eq!(back.up_to.as_deref(), Some("m-9"));
        }

        #[test]
        fn malformed_cursor_returns_validation_failed() {
            let envelope = decode_cursor("not-base64!!!").unwrap_err();
            assert_eq!(
                envelope.error.code,
                crate::wire::ErrorCode::ValidationFailed
            );
        }
    }
}

pub use get_handler::pond_get;

mod search_handler {
    //! The `pond_search` handler: hybrid (vector + BM25 + RRF) retrieval at message
    //! granularity, with filter pushdown, recency boost, and conversation grouping
    //! (spec.md#search, spec.md#search).

    use crate::{
        Clock, SystemClock,
        embed::{EmbedBackend, LazyEmbedder, format_query},
        sessions::{MessageKey, MessageMeta, Store},
        substrate::{Predicate, ScalarValue},
        wire::{
            ErrorEnvelope, Group, Hit, ProjectFilter, SearchEnvelope, SearchFilters, SearchRequest,
            SearchResponse, SearchResultBody, new_request_id, validate_protocol,
        },
    };
    use chrono::{DateTime, NaiveDate, Utc};

    use super::{map_error, map_storage};

    /// Internal branching enum for the retrieval mode. Production callers
    /// never pick: the server decides hybrid-vs-FTS from embedder availability
    /// (per-hit `matched_via` tells clients which retrievers ranked a row).
    /// `Vector` exists for operator tooling - selected via `pond search --mode`
    /// or the `mode_override` wire field consumed by `scripts/search-benchmarks/`.
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
        /// When set, vector-only "find similar messages to this stored
        /// message" mode: the stored vector for `similar_to` is the query;
        /// `query` and FTS arm are ignored.
        pub similar_to: Option<String>,
        pub filter: Predicate,
        pub pool: usize,
        pub vector_pool: usize,
        pub limit: usize,
        pub boost_recent: bool,
        pub group_by_conversation: bool,
        pub min_score: f64,
    }

    const LIMIT_CAP: usize = 200;
    /// Centered query-windowed body returned on every hit (spec.md#search).
    /// Calibrated for the agent-context budget: ~600 code points fits a typical
    /// match site without crowding the 10k-token `pond_get` page.
    const HIT_SNIPPET_CHARS: usize = 600;
    /// Additive recency boost (spec.md#search). Caps at 0.05 so it stays a
    /// tiebreaker; with `FTS_FUSION_WEIGHT + VECTOR_FUSION_WEIGHT = 1.135`,
    /// `SCORE_DENOMINATOR` divides best_score by 1.185 to land in `[0, 1]`.
    const RECENCY_MAX_BOOST: f64 = 0.05;
    const RECENCY_DECAY_SECONDS: f64 = 604_800.0;
    const SCORE_DENOMINATOR: f64 = FTS_FUSION_WEIGHT + VECTOR_FUSION_WEIGHT + RECENCY_MAX_BOOST;

    // Score-normalized hybrid fusion weights (spec.md#search). Per-arm
    // base_score is min-max normalized within the arm's pool, then the two
    // arms are combined as w_fts * norm_fts + w_vec * norm_vec. The 0.135:1
    // ratio was identified by `scripts/search-benchmarks/simulate_fusion.py` on the
    // 111-query paraphrase set: a wide plateau at w_fts in [0.09, 0.14]
    // (S@3 = 0.640-0.649), centroid 0.135. Symmetric across arms — no
    // per-query routing; cross-lingual queries are an agent-layer concern
    // (see the `pond_search` MCP description).
    const FTS_FUSION_WEIGHT: f64 = 0.135;
    const VECTOR_FUSION_WEIGHT: f64 = 1.0;

    /// Run a hybrid or FTS-only search. The mode is server-determined - hybrid
    /// when the store has any vectors, FTS-only otherwise. The embedder is
    /// `LazyEmbedder`-loaded on the first hybrid/vector call, so FTS-only
    /// corpora never pay the model load. The response has no top-level mode
    /// field; per-hit `matched_via` reports the retrievers that ranked each
    /// row.
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
        clock: &dyn Clock,
    ) -> Result<SearchResponse, ErrorEnvelope> {
        let override_mode = request.mode_override.map(wire_mode_to_internal);
        let mut plan = plan_search(request, SearchMode::Fts)?;

        // `similar_to` pins mode to Vector regardless of override or store
        // state: we are running kNN over a stored vector, not embedding a
        // query, so there is no FTS arm and no query-side embedder load.
        if plan.similar_to.is_some() {
            plan.mode = SearchMode::Vector;
        } else {
            // Mode is server-determined unless the caller passed an explicit
            // override (operator tooling). `has_embeddings()` is the only
            // gate: hybrid when the store has any vectors, FTS-only when empty.
            plan.mode = resolve_effective_mode(store, override_mode).await?;
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
                // Per-arm score shaping before fusion:
                //   - FTS: max-normalize raw BM25 (`score / max`) so the
                //     unbounded BM25 head doesn't dominate fusion when the
                //     query has one term that hits a rare phrase very hard.
                //   - Vector: rank-normalize the cosine-distance-ordered
                //     output (`1 - idx/n`); this is what the bench harness
                //     `simulate_fusion.py` was tuned against and what the
                //     vector-only path already emits as `base_score`.
                // Fusion (`fuse_arms`) min-max normalizes again over the
                // arm's full pool and weights the two arms by
                // FTS_FUSION_WEIGHT and VECTOR_FUSION_WEIGHT.
                let fts_max = fts.iter().map(|(_, s)| *s).fold(0.0_f32, f32::max);
                let fts_entries: Vec<(MessageKey, f64)> = fts
                    .into_iter()
                    .map(|(key, score)| {
                        let normed = if fts_max > 0.0 {
                            f64::from(score / fts_max)
                        } else {
                            0.0
                        };
                        (key, normed)
                    })
                    .collect();
                let vec_n = vector_raw.len() as f64;
                let vector_entries: Vec<(MessageKey, f64)> = vector_raw
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (key, _))| {
                        let normed = if vec_n > 0.0 {
                            1.0 - (idx as f64 / vec_n)
                        } else {
                            0.0
                        };
                        (key, normed)
                    })
                    .collect();
                // FTS first: when both arms picked different messages from the
                // same session_root, the fuser keeps FTS's representative
                // (better for hit display since BM25 highlights the lexical
                // match).
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
                fuse_arms(&lists)
                    .into_iter()
                    .map(|hit| Candidate {
                        session_id: hit.key.session_id,
                        message_id: hit.key.message_id,
                        base_score: hit.score,
                    })
                    .collect()
            }
            // Vector-only branch. Reached two ways:
            //   - caller pinned `similar_to`: pond fetches the stored vector
            //     for that message_id and uses it directly as the query (no
            //     embedder load, no query-side text formatting).
            //   - caller set `mode_override = Vector` (operator tooling /
            //     benchmark harness): embed `plan.query` and run kNN.
            SearchMode::Vector => {
                let vector = if let Some(similar_id) = &plan.similar_to {
                    let stored = store
                        .message_vector_by_id(similar_id)
                        .await
                        .map_err(map_storage)?;
                    let Some(vector) = stored else {
                        return Err(map_error(crate::Error::not_found(
                            "message",
                            serde_json::json!(similar_id),
                            format!(
                                "no embedded message with id {similar_id} (the message may not \
                                 exist, or it exists but is not yet embedded - run `pond embed`)"
                            ),
                        )));
                    };
                    vector
                } else {
                    let backend = load_embedder(embedder).await?;
                    embed_query(backend.as_ref(), &plan.query)?
                };
                let vector_raw = store
                    .vector_search(&vector, plan.vector_pool, &plan.filter, Some(search))
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
            let recency = if plan.boost_recent {
                recency_boost(meta.timestamp, now)
            } else {
                0.0
            };
            let score = candidate.base_score + recency;
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
    ) -> Result<std::sync::Arc<dyn EmbedBackend>, ErrorEnvelope> {
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

        let query = request.query.trim().to_owned();
        let similar_to = request
            .similar_to
            .as_ref()
            .map(|id| id.trim().to_owned())
            .filter(|id| !id.is_empty());
        if similar_to.is_none() && query.is_empty() {
            return Err(map_error(crate::Error::validation_field(
                "query must be non-empty after trim",
                "query",
                Some(serde_json::json!(request.query)),
                Some("non-empty string after trim, or pass `similar_to`".to_owned()),
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
        // Retriever candidate pool: wider than `limit` so the fuser has
        // material to merge.
        let pool = limit.saturating_mul(5).max(50);
        Ok(SearchPlan {
            mode,
            query,
            similar_to,
            filter,
            pool,
            vector_pool: pool.saturating_mul(2),
            limit,
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

    /// Score-normalized hybrid fusion keyed on the conversation root: for
    /// each arm, the surviving (post intra-arm dedup-by-root) raw `base_score`
    /// values are min-max normalized to [0, 1] across that arm's pool, then
    /// summed across arms weighted by `RankedList.weight`. The representative
    /// message_id is the first one each arm picked for the root; when both
    /// arms picked different messages from the same root, the first arm in
    /// the `lists` argument wins the representative (callers should list FTS
    /// first when FTS-side provenance is preferred for the displayed hit).
    /// Ties break on the representative key for determinism
    /// (spec.md#search).
    ///
    /// Why session-root keying instead of `(session_id, message_id)`: a long
    /// session whose best FTS message and best vector message differ would
    /// otherwise appear as two separate fused hits, neither getting the
    /// cross-arm validation bonus. Keying on the root credits cross-arm
    /// agreement at the conversation level - which is what the user sees.
    ///
    /// Why per-arm score normalization instead of RRF: RRF discards score
    /// magnitude (rank 1 contributes the same whether the vector cosine is
    /// 0.85 or 0.55), and on paraphrase queries that magnitude is the load-
    /// bearing signal. See `scripts/search-benchmarks/simulate_fusion.py` and
    /// `docs/researches/embeddings/`.
    pub fn fuse_arms(lists: &[RankedList]) -> Vec<FusedHit> {
        let mut merged: std::collections::HashMap<String, (f64, Vec<String>, MessageKey)> =
            std::collections::HashMap::new();
        for list in lists {
            if list.entries.is_empty() {
                continue;
            }
            // Min-max normalize across the FULL arm pool BEFORE dedup. The
            // benchmark simulator (scripts/search-benchmarks/simulate_fusion.py) and
            // the production code MUST agree on the normalization basis;
            // dedupping first would narrow [lo, hi] to only the surviving
            // session-roots' scores and skew the normalized signal away
            // from what the benchmark reports. A degenerate arm where every
            // hit ties on raw score collapses to zero contribution; the
            // other arm then decides the order.
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
            // Intra-arm dedup-by-root keeps the highest-scoring message
            // each arm returned for a given conversation: without it a long
            // session whose top-N hits all share a root would crowd out
            // cross-arm signal from other sessions.
            let mut seen_in_arm: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for (key, raw) in &list.entries {
                let root = session_root(&key.session_id).to_owned();
                if !seen_in_arm.insert(root.clone()) {
                    continue;
                }
                let norm = if range > 0.0 { (raw - lo) / range } else { 0.0 };
                let contribution = list.weight * norm;
                let entry = merged
                    .entry(root)
                    .or_insert_with(|| (0.0, Vec::new(), key.clone()));
                entry.0 += contribution;
                entry.1.push(list.retriever.as_wire().to_owned());
            }
        }
        let mut hits = merged
            .into_values()
            .map(|(score, matched_via, key)| FusedHit {
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
    /// TODO(snippet-anchor): reassess for vector-only hits (e.g. similar_to,
    /// paraphrase queries where no literal term matches): the fallback to
    /// offset-0 is OK but not great. Possible upgrades: ngram match overlap,
    /// or skip-window-around-most-distinctive-substring. See snippet audit
    /// in tier-0 findings.
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
    }

    struct ScoredHit {
        meta: MessageMeta,
        score: f64,
    }

    impl ScoredHit {
        fn into_hit(self, query: &str) -> Hit {
            let text = hit_payload(&self.meta.search_text, query);
            Hit {
                session_id: self.meta.session_id,
                message_id: self.meta.message_id,
                role: self.meta.role,
                timestamp: self.meta.timestamp,
                project: self.meta.project,
                source_agent: self.meta.source_agent,
                text,
                score: normalize_score(self.score),
            }
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

    fn normalize_vector(hits: Vec<(MessageKey, f32)>) -> Vec<Candidate> {
        let n = hits.len() as f64;
        hits.into_iter()
            .enumerate()
            .map(|(idx, (key, _))| Candidate {
                session_id: key.session_id,
                message_id: key.message_id,
                base_score: if n > 0.0 { 1.0 - (idx as f64 / n) } else { 0.0 },
            })
            .collect()
    }

    fn embed_query(embedder: &dyn EmbedBackend, query: &str) -> Result<Vec<f32>, ErrorEnvelope> {
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

    async fn build_groups(
        store: &Store,
        scored: &[ScoredHit],
        limit: usize,
        query: &str,
    ) -> Result<Vec<Group>, ErrorEnvelope> {
        use std::collections::BTreeMap;

        // `scored` is already sorted by score descending, so the first hit seen for
        // a session is its best-scoring match - its message_id is the
        // `best_hit_message_id` callers use to drill in via `pond_get`.
        struct Acc {
            best_hit_message_id: String,
            project: String,
            source_agent: String,
            first_timestamp: DateTime<Utc>,
            last_timestamp: DateTime<Utc>,
            text: String,
            best_score: f64,
        }
        let mut groups: BTreeMap<String, Acc> = BTreeMap::new();
        for hit in scored {
            let root = session_root(&hit.meta.session_id).to_owned();
            let entry = groups.entry(root).or_insert_with(|| Acc {
                best_hit_message_id: hit.meta.message_id.clone(),
                project: hit.meta.project.clone(),
                source_agent: hit.meta.source_agent.clone(),
                first_timestamp: hit.meta.timestamp,
                last_timestamp: hit.meta.timestamp,
                text: hit_payload(&hit.meta.search_text, query),
                best_score: hit.score,
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
                session_messages_count: counts.get(&session_id).copied().unwrap_or_default(),
                session_id,
                best_hit_message_id: acc.best_hit_message_id,
                project: acc.project,
                source_agent: acc.source_agent,
                first_timestamp: acc.first_timestamp,
                last_timestamp: if acc.last_timestamp > acc.first_timestamp {
                    Some(acc.last_timestamp)
                } else {
                    None
                },
                text: acc.text,
                best_score: normalize_score(acc.best_score),
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
            let merged = fuse_arms(&[fts, vec_arm]);
            // Output: one row per session_root after intra-arm dedup.
            assert_eq!(merged.len(), 2);
            // Per-arm min-max over the FULL pool BEFORE dedup:
            // FTS pool [10, 9, 6, 5]: range 5. A's first hit (10) -> 1.0;
            // B's first hit (6) -> 0.2.
            // Vector pool [0.9, 0.6]: range 0.3. B -> 1.0; A -> 0.0.
            // session-A: 0.135 * 1.0 + 1.0 * 0.0 = 0.135.
            // session-B: 0.135 * 0.2 + 1.0 * 1.0 = 1.027. B wins.
            assert_eq!(merged[0].key.session_id, "session-B");
            // FTS was listed first, so FTS's pick (msg-3) wins the
            // representative over Vector's pick (msg-7) for session-B.
            assert_eq!(merged[0].key.message_id, "msg-3");
            assert_eq!(merged[0].matched_via, vec!["fts", "vector"]);
            assert_eq!(merged[1].key.session_id, "session-A");
            assert_eq!(merged[1].key.message_id, "msg-1");
            assert_eq!(merged[1].matched_via, vec!["fts", "vector"]);
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
            let merged = fuse_arms(&[fts, vec_arm]);
            // Vector arm alone decides: A's normalized 1.0 beats B's 0.0.
            assert_eq!(merged[0].key.session_id, "session-A");
            assert!((merged[0].score - 1.0).abs() < 1e-9);
            assert!(merged[1].score.abs() < 1e-9);
        }
    }
}

pub use search_handler::{
    FusedHit, RankedList, RetrieverKind, SearchMode, SearchPlan, build_filter, explain_search_plan,
    fuse_arms, hit_payload, plan_search, pond_search, recency_boost,
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
            mode_override: None,
            similar_to: None,
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
        let merged = fuse_arms(&lists);

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
        // `query_snippet` adds up to "..." on each side, so account for the +6.
        assert!(
            text.chars().count() <= 600 + 6,
            "snippet is bounded by HIT_SNIPPET_CHARS"
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
