fn map_error(error: crate::Error) -> crate::wire::ErrorEnvelope {
    error.into()
}

fn map_storage(error: anyhow::Error) -> crate::wire::ErrorEnvelope {
    map_error(crate::Error::Storage(error))
}

mod ingest_handler {
    use anyhow::Result;
    use tokio_stream::StreamExt;

    use crate::{
        adapter::Adapter,
        sessions::{IngestEvent, IngestSummary, IngestValidator, Store},
        wire::{
            DEFAULT_NAMESPACE, IngestEnvelope, IngestRequest, IngestResponse, new_request_id,
            validate_protocol,
        },
    };

    use super::{map_error, map_storage};

    /// Hard cap on events per `pond_ingest` batch (design.md 3.6.4).
    pub const MAX_INGEST_EVENTS: usize = 1000;

    /// Drain `adapter.events()` into `store`, accumulating an [`IngestSummary`].
    /// A session substream that fails validation or decoding is dropped (buffered
    /// events discarded) and ingest continues with the next session; the
    /// rejected-substream count lands in [`IngestSummary::errors`].
    pub async fn ingest_adapter(store: &Store, adapter: &dyn Adapter) -> Result<IngestSummary> {
        let mut summary = IngestSummary {
            inserted: 0,
            matched: 0,
            errors: 0,
        };
        let mut events = adapter.events();
        let mut validator = IngestValidator::default();
        let mut skipping = false;

        while let Some(event) = events.next().await {
            match event {
                Ok(event) => {
                    if skipping {
                        if matches!(event, IngestEvent::Session(_)) {
                            skipping = false;
                            validator = IngestValidator::default();
                        } else {
                            continue;
                        }
                    }
                    match validator.push(store, event).await {
                        Ok(statuses) => summary.add_statuses(&statuses),
                        Err(failure) => {
                            summary.errors += 1;
                            tracing::warn!(%failure, "aborting invalid session substream");
                            validator = IngestValidator::default();
                            skipping = true;
                        }
                    }
                }
                Err(error) => {
                    summary.errors += 1;
                    tracing::warn!(%error, "aborting undecodable session substream");
                    validator = IngestValidator::default();
                    skipping = true;
                }
            }
        }
        if !skipping {
            let statuses = validator.finish(store).await?;
            summary.add_statuses(&statuses);
        }
        Ok(summary)
    }

    /// The `pond_ingest` wire handler (design.md 3.6.4): validate the transport
    /// envelope, then drive the event batch through [`ingest_events`]. Transport
    /// failures (bad protocol, unknown namespace, empty or oversized batch) fail the
    /// whole request via the 3.6.1 error envelope.
    pub async fn pond_ingest(store: &Store, request: IngestRequest) -> IngestEnvelope {
        if let Err(envelope) = validate_protocol(request.protocol_version) {
            return IngestEnvelope::Error(envelope);
        }
        if request.namespace.as_deref() != Some(DEFAULT_NAMESPACE) {
            return IngestEnvelope::Error(map_error(crate::Error::NamespaceUnknown(
                request.namespace.unwrap_or_else(|| "<missing>".to_owned()),
            )));
        }
        if request.events.is_empty() {
            return IngestEnvelope::Error(map_error(crate::Error::Validation(
                "events must be a non-empty array".to_owned(),
            )));
        }
        if request.events.len() > MAX_INGEST_EVENTS {
            return IngestEnvelope::Error(map_error(crate::Error::Validation(format!(
                "ingest batch exceeds the event cap: at most {MAX_INGEST_EVENTS} events"
            ))));
        }

        match ingest_events(store, request.events).await {
            Ok(summary) => IngestEnvelope::Success(IngestResponse {
                accepted: summary.accepted(),
                rejected: summary.errors,
                inserted: summary.inserted,
                matched: summary.matched,
                request_id: new_request_id(),
            }),
            Err(failure) => IngestEnvelope::Error(map_storage(failure)),
        }
    }

    /// Drive a flat event batch through [`IngestValidator`], grouped into session
    /// substreams. A substream that fails validation is aborted - its buffered
    /// events are dropped and events are skipped until the next `Session` event -
    /// while later sessions in the batch still process (design.md 3.6.4). The
    /// rejected-substream count lands in [`IngestSummary::errors`].
    pub async fn ingest_events(store: &Store, events: Vec<IngestEvent>) -> Result<IngestSummary> {
        let mut summary = IngestSummary {
            inserted: 0,
            matched: 0,
            errors: 0,
        };
        let mut validator = IngestValidator::default();
        let mut skipping = false;
        for event in events {
            if skipping {
                if matches!(event, IngestEvent::Session(_)) {
                    skipping = false;
                } else {
                    continue;
                }
            }
            match validator.push(store, event).await {
                Ok(statuses) => summary.add_statuses(&statuses),
                Err(failure) => {
                    summary.errors += 1;
                    tracing::warn!(%failure, "aborting invalid session substream");
                    validator = IngestValidator::default();
                    skipping = true;
                }
            }
        }
        if !skipping {
            let statuses = validator.finish(store).await?;
            summary.add_statuses(&statuses);
        }
        Ok(summary)
    }
}

pub use crate::sessions::{IngestError, IngestEvent, IngestSummary, IngestValidator, search_text};
pub use ingest_handler::{MAX_INGEST_EVENTS, ingest_adapter, ingest_events, pond_ingest};

mod get_handler {
    use crate::{
        sessions::{MessageWithParts, SessionWithMessages, Store},
        wire::{
            DEFAULT_NAMESPACE, GetEnvelope, GetRequest, GetResponse, GetResult, validate_protocol,
        },
        wire::{Message, Part, PartKind},
    };

    use super::{map_error, map_storage};

    pub async fn pond_get(store: &Store, request: GetRequest) -> GetEnvelope {
        if let Err(error) = validate_protocol(request.protocol_version) {
            return GetEnvelope::Error(error);
        }
        if request.namespace.as_deref() != Some(DEFAULT_NAMESPACE) {
            return GetEnvelope::Error(map_error(crate::Error::NamespaceUnknown(
                request.namespace.unwrap_or_else(|| "<missing>".to_owned()),
            )));
        }

        let result = match (&request.session_id, &request.message_id, &request.up_to) {
            (Some(session_id), None, up_to) => {
                session_scope(store, &request, session_id, up_to.as_deref()).await
            }
            (None, Some(message_id), None) => message_scope(store, &request, message_id).await,
            (None, Some(_), Some(_)) => Err(map_error(crate::Error::Validation(
                "up_to is valid only with session_id".to_owned(),
            ))),
            (Some(_), Some(_), _) => Err(map_error(crate::Error::Validation(
                "session_id and message_id are mutually exclusive".to_owned(),
            ))),
            (None, None, _) => Err(map_error(crate::Error::Validation(
                "one of session_id or message_id is required".to_owned(),
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
            return Err(map_error(crate::Error::NotFound(format!(
                "session not found: {session_id}"
            ))));
        };

        if let Some(up_to) = up_to {
            let Some(index) = stored
                .messages
                .iter()
                .position(|message| message.message.id() == up_to)
            else {
                return Err(map_error(crate::Error::NotFound(format!(
                    "up_to message not found in session: {session_id}/{up_to}"
                ))));
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
            return Err(map_error(crate::Error::NotFound(format!(
                "message not found: {message_id}"
            ))));
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
    //! (design.md 3.3, 3.6.2).

    use crate::{
        Clock, SystemClock,
        embed::{EmbedBackend, qwen3_query_instruction},
        sessions::{MessageMeta, Store},
        substrate::{Predicate, ScalarValue},
        wire::{
            DEFAULT_NAMESPACE, ErrorEnvelope, Group, Hit, ProjectMatch, SearchEnvelope,
            SearchFilters, SearchRequest, SearchResponse, SearchResultBody, new_request_id,
            validate_protocol,
        },
    };
    use chrono::{DateTime, NaiveDate, Utc};

    use super::{map_error, map_storage};

    /// Internal-only branching enum for the retrieval mode. The wire layer doesn't
    /// expose this - per-hit `matched_via` already tells clients which retrievers
    /// ranked a row, and the request never asks for a specific mode.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum EffectiveMode {
        Hybrid,
        Fts,
    }

    /// Server-enforced cap on `limit` (design.md 3.6.2).
    const LIMIT_CAP: usize = 200;
    /// Preview length in code points (design.md 3.6.2).
    const PREVIEW_CHARS: usize = 500;
    /// Recency-boost constants, inherited verbatim from kb (design.md 3.3).
    const RECENCY_MAX_BOOST: f64 = 0.2;
    const RECENCY_DECAY_SECONDS: f64 = 604_800.0;

    /// Run a hybrid or FTS-only search. The mode is server-determined - hybrid when
    /// `embedder` is `Some` AND the store holds at least one embedding row for the
    /// embedder's `(model_id, max_embed_tokens)` identity, FTS-only otherwise. The
    /// response has no top-level mode field; per-hit `matched_via` reports the
    /// retrievers that ranked each row.
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
        validate_protocol(request.protocol_version)?;

        // v1 serves a single namespace; reject anything else loudly rather than
        // silently treating it as `local`.
        if request.namespace.as_deref() != Some(DEFAULT_NAMESPACE) {
            return Err(map_error(crate::Error::NamespaceUnknown(
                request.namespace.unwrap_or_else(|| "<missing>".to_owned()),
            )));
        }

        let query = request.query.trim().to_owned();
        if query.is_empty() {
            return Err(map_error(crate::Error::Validation(
                "query must be non-empty after trim".to_owned(),
            )));
        }
        if request.limit == 0 {
            return Err(map_error(crate::Error::Validation(
                "limit must be at least 1".to_owned(),
            )));
        }
        let limit = request.limit.min(LIMIT_CAP);
        let filter = build_filter(&request.filters)?;
        // Retriever candidate pool: wider than `limit` so RRF has material to merge.
        let pool = limit.saturating_mul(5).max(50);

        // The mode is server-determined: hybrid only when both the embedder is
        // loaded AND embeddings exist for its identity. Anything else degrades to
        // FTS-only - a vector retriever over zero rows would just be wasted work.
        let effective_mode = resolve_effective_mode(store, embedder).await?;
        let candidates = match effective_mode {
            EffectiveMode::Fts => {
                let hits = store
                    .fts_search(&query, pool, &filter)
                    .await
                    .map_err(map_storage)?;
                normalize_fts(hits)
            }
            EffectiveMode::Hybrid => {
                // `resolve_effective_mode` only returns Hybrid when `embedder` is
                // `Some` and `has_embeddings` returned true; an `Internal` error
                // here would only fire under a logic bug.
                let Some(embedder) = embedder else {
                    return Err(map_error(crate::Error::Internal(
                        "hybrid mode resolved without an embedder".to_owned(),
                    )));
                };
                let vector = embed_query(embedder, &query)?;
                // The two retrievers hit disjoint datasets (and disjoint mutexes),
                // so run them concurrently rather than back-to-back.
                let fts_fut = async {
                    store
                        .fts_search(&query, pool, &filter)
                        .await
                        .map_err(map_storage)
                };
                let vector_fut = async {
                    store
                        .vector_search(
                            &vector,
                            pool.saturating_mul(2),
                            &filter,
                            embedder.model_id(),
                            embedder.max_embed_tokens(),
                        )
                        .await
                        .map_err(map_storage)
                };
                let (fts, vector_raw) = tokio::try_join!(fts_fut, vector_fut)?;
                let vector_retriever = VectorRetriever;
                let fts_retriever = FtsRetriever;
                let lists = [
                    RankedList {
                        retriever: vector_retriever.kind(),
                        ids: vector_raw.into_iter().map(|(id, _)| id).collect(),
                    },
                    RankedList {
                        retriever: fts_retriever.kind(),
                        ids: fts.into_iter().map(|(id, _)| id).collect(),
                    },
                ];
                rrf_merge(&lists, request.rrf_k)
                    .into_iter()
                    .map(|hit| Candidate {
                        message_id: hit.message_id,
                        base_score: hit.score,
                        matched_via: hit.matched_via,
                    })
                    .collect()
            }
        };

        if candidates.is_empty() {
            return Ok(empty_response(request.group_by_conversation));
        }

        // Hydrate hit metadata (timestamp, role, project, preview source) from the
        // canonical `messages` table - the denormalized columns on `embeddings`
        // exist for filter pushdown, not result hydration.
        let ids = candidates
            .iter()
            .map(|candidate| candidate.message_id.clone())
            .collect::<Vec<_>>();
        let metas = store
            .message_metas_by_ids(&ids)
            .await
            .map_err(map_storage)?;
        let meta_index = metas
            .iter()
            .map(|meta| (meta.message_id.as_str(), meta))
            .collect::<std::collections::HashMap<_, _>>();

        let now = clock.now();
        let mut scored = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let Some(meta) = meta_index.get(candidate.message_id.as_str()) else {
                continue;
            };
            let recency_boost = if request.boost_recent {
                recency_boost(meta.timestamp, now)
            } else {
                0.0
            };
            let score = candidate.base_score + recency_boost;
            if score < request.filters.min_score {
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
                .then_with(|| left.meta.message_id.cmp(&right.meta.message_id))
        });

        if request.group_by_conversation {
            let groups = build_groups(store, &scored, limit).await?;
            let total = groups.len();
            Ok(SearchResponse {
                result: SearchResultBody::Groups { groups },
                total,
                request_id: new_request_id(),
            })
        } else {
            let hits = scored
                .into_iter()
                .take(limit)
                .map(ScoredHit::into_hit)
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
    /// loaded embedder and at least one embedding row for its identity; otherwise
    /// FTS-only.
    async fn resolve_effective_mode(
        store: &Store,
        embedder: Option<&dyn EmbedBackend>,
    ) -> Result<EffectiveMode, ErrorEnvelope> {
        match embedder {
            None => Ok(EffectiveMode::Fts),
            Some(backend) => {
                let has = store
                    .has_embeddings(backend.model_id(), backend.max_embed_tokens())
                    .await
                    .map_err(map_storage)?;
                Ok(if has {
                    EffectiveMode::Hybrid
                } else {
                    EffectiveMode::Fts
                })
            }
        }
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

    trait Retriever {
        fn kind(&self) -> RetrieverKind;
    }

    struct FtsRetriever;

    impl Retriever for FtsRetriever {
        fn kind(&self) -> RetrieverKind {
            RetrieverKind::Fts
        }
    }

    struct VectorRetriever;

    impl Retriever for VectorRetriever {
        fn kind(&self) -> RetrieverKind {
            RetrieverKind::Vector
        }
    }

    /// A retriever-ranked list of `message_id`s, best-first.
    pub struct RankedList {
        pub retriever: RetrieverKind,
        pub ids: Vec<String>,
    }

    /// One merged RRF result.
    #[derive(Debug, Clone, PartialEq)]
    pub struct RrfHit {
        pub message_id: String,
        pub score: f64,
        pub matched_via: Vec<String>,
    }

    /// Reciprocal Rank Fusion: `sum(1 / (k + rank))` across the retrievers that
    /// ranked each id (rank is 1-based). Returns hits sorted by score descending,
    /// ties broken by `message_id` for determinism (design.md 2.5, 3.3).
    pub fn rrf_merge(lists: &[RankedList], k: u32) -> Vec<RrfHit> {
        let k = f64::from(k.max(1));
        let mut merged: std::collections::HashMap<String, (f64, Vec<String>)> =
            std::collections::HashMap::new();
        for list in lists {
            for (rank, id) in list.ids.iter().enumerate() {
                let contribution = 1.0 / (k + (rank as f64 + 1.0));
                let entry = merged
                    .entry(id.clone())
                    .or_insert_with(|| (0.0, Vec::new()));
                entry.0 += contribution;
                entry.1.push(list.retriever.as_wire().to_owned());
            }
        }
        let mut hits = merged
            .into_iter()
            .map(|(message_id, (score, matched_via))| RrfHit {
                message_id,
                score,
                matched_via,
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.message_id.cmp(&right.message_id))
        });
        hits
    }

    /// Additive exponential-decay recency boost (design.md 3.3): caps at `+0.2` at
    /// `age = 0`, decays to near-zero past a few weeks.
    pub fn recency_boost(timestamp: DateTime<Utc>, now: DateTime<Utc>) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let age_seconds = (now - timestamp).num_seconds().max(0) as f64;
        RECENCY_MAX_BOOST * (-age_seconds / RECENCY_DECAY_SECONDS).exp()
    }

    /// First [`PREVIEW_CHARS`] code points of `text`, with `"..."` appended when
    /// truncated (design.md 3.6.2).
    pub fn make_preview(text: &str) -> String {
        let mut preview = text.chars().take(PREVIEW_CHARS).collect::<String>();
        if text.chars().nth(PREVIEW_CHARS).is_some() {
            preview.push_str("...");
        }
        preview
    }

    struct Candidate {
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
        fn into_hit(self) -> Hit {
            Hit {
                session_id: self.meta.session_id,
                message_id: self.meta.message_id,
                role: self.meta.role,
                timestamp: self.meta.timestamp,
                project: self.meta.project,
                source_agent: self.meta.source_agent,
                preview: make_preview(&self.meta.search_text),
                score: self.score,
                base_score: self.base_score,
                recency_boost: self.recency_boost,
                matched_via: self.matched_via,
            }
        }
    }

    /// Unbounded BM25 scores to a `[0, 1]` base score by dividing by the max in the
    /// result set.
    fn normalize_fts(hits: Vec<(String, f32)>) -> Vec<Candidate> {
        let max = hits.iter().map(|(_, score)| *score).fold(0.0_f32, f32::max);
        hits.into_iter()
            .map(|(message_id, score)| Candidate {
                message_id,
                base_score: if max > 0.0 {
                    f64::from(score / max)
                } else {
                    0.0
                },
                matched_via: vec![RetrieverKind::Fts.as_wire().to_owned()],
            })
            .collect()
    }

    fn embed_query(embedder: &dyn EmbedBackend, query: &str) -> Result<Vec<f32>, ErrorEnvelope> {
        // The query side gets the Qwen3 instruction prefix; documents are embedded
        // bare by the worker (model-card convention).
        let prompt = qwen3_query_instruction(query);
        // Model inference is synchronous and CPU/GPU-bound; `block_in_place` keeps
        // it from stalling other tasks on the async worker thread. (Requires a
        // multi-threaded runtime - see `pond_search`.)
        let vectors =
            tokio::task::block_in_place(|| embedder.embed(&[prompt])).map_err(|error_value| {
                map_error(crate::Error::Internal(format!(
                    "failed to embed query: {error_value}"
                )))
            })?;
        vectors.into_iter().next().ok_or_else(|| {
            map_error(crate::Error::Internal(
                "embedder returned no vector for query".to_owned(),
            ))
        })
    }

    async fn build_groups(
        store: &Store,
        scored: &[ScoredHit],
        limit: usize,
    ) -> Result<Vec<Group>, ErrorEnvelope> {
        use std::collections::BTreeMap;

        // `scored` is already sorted by score descending, so the first hit seen for
        // a session is its best-scoring match.
        struct Acc {
            project: Option<String>,
            source_agent: String,
            first_timestamp: DateTime<Utc>,
            last_timestamp: DateTime<Utc>,
            preview: String,
            best_score: f64,
        }
        let mut groups: BTreeMap<String, Acc> = BTreeMap::new();
        for hit in scored {
            let entry = groups
                .entry(hit.meta.session_id.clone())
                .or_insert_with(|| Acc {
                    project: hit.meta.project.clone(),
                    source_agent: hit.meta.source_agent.clone(),
                    first_timestamp: hit.meta.timestamp,
                    last_timestamp: hit.meta.timestamp,
                    preview: make_preview(&hit.meta.search_text),
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
                message_count: counts.get(&session_id).copied().unwrap_or_default(),
                session_id,
                project: acc.project,
                source_agent: acc.source_agent,
                first_timestamp: acc.first_timestamp,
                last_timestamp: acc.last_timestamp,
                preview: acc.preview,
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
    /// Column names are identical on `messages` and `embeddings` (design.md
    /// 3.2.2 / 3.2.4) so one predicate serves both.
    pub fn build_filter(filters: &SearchFilters) -> Result<Predicate, ErrorEnvelope> {
        let mut clauses = Vec::new();

        match filters.project_match {
            ProjectMatch::IsNull => clauses.push(Predicate::IsNull("project")),
            ProjectMatch::Exact => {
                if let Some(project) = &filters.project {
                    clauses.push(Predicate::Eq("project", project.clone().into()));
                }
            }
            ProjectMatch::Contains => {
                if let Some(project) = &filters.project {
                    clauses.push(Predicate::LikeContains("project", project.clone()));
                }
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
                return Err(map_error(crate::Error::Validation(format!(
                    "filters.role must be one of: user, assistant, system, tool; got {role}"
                ))));
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
            map_error(crate::Error::Validation(format!(
                "{field} must be in YYYY-MM-DD format; got {date}"
            )))
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
}

pub use search_handler::{
    RankedList, RetrieverKind, RrfHit, build_filter, make_preview, pond_search, recency_boost,
    rrf_merge,
};
