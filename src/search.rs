//! The `pond_search` handler: hybrid (vector + BM25 + RRF) retrieval at message
//! granularity, with filter pushdown, recency boost, and conversation grouping
//! (design.md 3.3, 3.6.2).

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::json;

use crate::{
    embed::{EmbedBackend, qwen3_query_instruction},
    substrate::{MessageMeta, PondStore, sql_like_contains, sql_string},
    wire::{
        ErrorCode, ErrorEnvelope, Group, Hit, ProjectMatch, SearchEnvelope, SearchFilters,
        SearchMode, SearchRequest, SearchResponse, SearchResultBody, default_namespace, error,
        new_request_id, storage_error, validate_protocol,
    },
};

/// Server-enforced cap on `limit` (design.md 3.6.2).
const LIMIT_CAP: usize = 200;
/// Preview length in code points (design.md 3.6.2).
const PREVIEW_CHARS: usize = 500;
/// Recency-boost constants, inherited verbatim from kb (design.md 3.3).
const RECENCY_MAX_BOOST: f64 = 0.2;
const RECENCY_DECAY_SECONDS: f64 = 604_800.0;

/// Run a hybrid/vector/fts search. `embedder` produces the query vector for the
/// `vector` and `hybrid` modes; it is unused for `fts`.
///
/// Must run on a multi-threaded Tokio runtime: query embedding uses
/// `block_in_place`, which panics on a `current_thread` runtime.
pub async fn pond_search(
    store: &PondStore,
    embedder: &dyn EmbedBackend,
    request: SearchRequest,
) -> SearchEnvelope {
    match run_search(store, embedder, request).await {
        Ok(response) => SearchEnvelope::Success(response),
        Err(envelope) => SearchEnvelope::Error(envelope),
    }
}

async fn run_search(
    store: &PondStore,
    embedder: &dyn EmbedBackend,
    request: SearchRequest,
) -> Result<SearchResponse, ErrorEnvelope> {
    validate_protocol(request.protocol_version)?;

    // v1 serves a single namespace; reject anything else loudly rather than
    // silently treating it as `local`.
    if request.namespace != default_namespace() {
        return Err(error(
            ErrorCode::NamespaceUnknown,
            "unknown namespace",
            json!({ "namespace": request.namespace, "supported": [default_namespace()] }),
        ));
    }

    let query = request.query.trim().to_owned();
    if query.is_empty() {
        return Err(error(
            ErrorCode::ValidationFailed,
            "query must be non-empty after trim",
            json!({ "field": "query" }),
        ));
    }
    if request.limit == 0 {
        return Err(error(
            ErrorCode::ValidationFailed,
            "limit must be at least 1",
            json!({ "field": "limit", "value": 0 }),
        ));
    }
    let limit = request.limit.min(LIMIT_CAP);
    let filter = build_filter(&request.filters)?;
    // Retriever candidate pool: wider than `limit` so RRF has material to merge.
    let pool = limit.saturating_mul(5).max(50);

    // Each candidate carries its pre-recency base score and the retrievers that
    // ranked it.
    let candidates = match request.search_mode {
        SearchMode::Fts => {
            let hits = store
                .fts_search(&query, pool, &filter)
                .await
                .map_err(storage_error)?;
            normalize_fts(hits)
        }
        SearchMode::Vector => {
            let vector = embed_query(embedder, &query)?;
            let hits = store
                .vector_search(
                    &vector,
                    pool.saturating_mul(2),
                    &filter,
                    embedder.model_id(),
                    embedder.max_embed_tokens(),
                )
                .await
                .map_err(storage_error)?;
            normalize_vector(hits)
        }
        SearchMode::Hybrid => {
            let vector = embed_query(embedder, &query)?;
            // The two retrievers hit disjoint datasets (and disjoint mutexes),
            // so run them concurrently rather than back-to-back.
            let fts_fut = async {
                store
                    .fts_search(&query, pool, &filter)
                    .await
                    .map_err(storage_error)
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
                    .map_err(storage_error)
            };
            let (fts, vector_raw) = tokio::try_join!(fts_fut, vector_fut)?;
            let lists = [
                RankedList {
                    retriever: "vector",
                    ids: vector_raw.into_iter().map(|(id, _)| id).collect(),
                },
                RankedList {
                    retriever: "fts",
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
        .map_err(storage_error)?;
    let meta_index = metas
        .iter()
        .map(|meta| (meta.message_id.as_str(), meta))
        .collect::<std::collections::HashMap<_, _>>();

    let now = Utc::now();
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

/// A retriever-ranked list of `message_id`s, best-first.
pub struct RankedList<'a> {
    pub retriever: &'a str,
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
pub fn rrf_merge(lists: &[RankedList<'_>], k: u32) -> Vec<RrfHit> {
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
            entry.1.push(list.retriever.to_owned());
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

/// Cosine distance (Lance metric) to a `[0, 1]` similarity base score.
fn normalize_vector(hits: Vec<(String, f32)>) -> Vec<Candidate> {
    hits.into_iter()
        .map(|(message_id, distance)| Candidate {
            message_id,
            base_score: f64::from(1.0 - distance).clamp(0.0, 1.0),
            matched_via: vec!["vector".to_owned()],
        })
        .collect()
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
            matched_via: vec!["fts".to_owned()],
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
            error(
                ErrorCode::Internal,
                "failed to embed query",
                json!({ "underlying": error_value.to_string() }),
            )
        })?;
    vectors.into_iter().next().ok_or_else(|| {
        error(
            ErrorCode::Internal,
            "embedder returned no vector for query",
            json!({}),
        )
    })
}

async fn build_groups(
    store: &PondStore,
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
        .map_err(storage_error)?;

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

/// Build the shared scalar filter predicate pushed into both retrievers. Empty
/// string means no filter. Column names are identical on `messages` and
/// `embeddings` (design.md 3.2.2 / 3.2.4) so one predicate serves both.
pub fn build_filter(filters: &SearchFilters) -> Result<String, ErrorEnvelope> {
    let mut clauses = Vec::new();

    match filters.project_match {
        ProjectMatch::IsNull => clauses.push("project IS NULL".to_owned()),
        ProjectMatch::Exact => {
            if let Some(project) = &filters.project {
                clauses.push(format!("project = {}", sql_string(project)));
            }
        }
        ProjectMatch::Contains => {
            if let Some(project) = &filters.project {
                clauses.push(format!(
                    "project LIKE {} ESCAPE '\\'",
                    sql_like_contains(project)
                ));
            }
        }
    }

    if let Some(session_id) = &filters.session_id {
        clauses.push(format!("session_id = {}", sql_string(session_id)));
    }
    if let Some(source_agent) = &filters.source_agent {
        clauses.push(format!("source_agent = {}", sql_string(source_agent)));
    }
    if let Some(role) = &filters.role {
        if !matches!(role.as_str(), "user" | "assistant" | "system" | "tool") {
            return Err(error(
                ErrorCode::ValidationFailed,
                "filters.role must be one of: user, assistant, system, tool",
                json!({ "field": "filters.role", "value": role }),
            ));
        }
        clauses.push(format!("role = {}", sql_string(role)));
    }
    if let Some(from_date) = &filters.from_date {
        clauses.push(format!(
            "timestamp >= {}",
            date_bound(from_date, "filters.from_date", false)?
        ));
    }
    if let Some(to_date) = &filters.to_date {
        clauses.push(format!(
            "timestamp <= {}",
            date_bound(to_date, "filters.to_date", true)?
        ));
    }

    Ok(clauses.join(" AND "))
}

/// Parse a `YYYY-MM-DD` filter date into a timestamp literal. `end_of_day`
/// pushes `to_date` to the inclusive end of the day.
fn date_bound(date: &str, field: &str, end_of_day: bool) -> Result<String, ErrorEnvelope> {
    NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| {
        error(
            ErrorCode::ValidationFailed,
            "date must be in YYYY-MM-DD format",
            json!({ "field": field, "value": date }),
        )
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
