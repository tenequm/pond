//! The seam between the desk and everything that talks to pond or herdr: the
//! [`Api`] trait, the rows it returns, the pond wire mirrors, and every SQL
//! query the desk runs. No SQL may live anywhere else in the crate.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub(crate) const PROTOCOL_VERSION: u16 = 1;

/// Boxed so `dyn Api` stays object-safe and the mock and the HTTP client
/// interchange behind one `Arc<dyn Api>`.
pub(crate) type ApiFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ApiError>> + Send + 'a>>;

/// Everything the desk reads. The HTTP implementation resolves (or spawns) a
/// `pond serve` lazily on first use, so a call can take as long as a cold store
/// open; the desk shows its loading state for the whole wait.
pub(crate) trait Api: Send + Sync {
    fn list_sessions(&self, scope: ListingScope) -> ApiFuture<'_, Vec<SessionRow>>;
    /// One row per id that exists; order is unspecified. Empty input returns
    /// empty without a request.
    fn hydrate(&self, session_ids: Vec<String>) -> ApiFuture<'_, Vec<SessionDetail>>;
    fn search(&self, request: SearchRequest) -> ApiFuture<'_, SearchResponse>;
    /// Newest first, at most [`PREVIEW_ROWS`].
    fn preview(&self, session_id: String) -> ApiFuture<'_, Vec<TranscriptMessage>>;
    /// Chronological page strictly after `after`. The last page is the first
    /// one shorter than [`PAGE_ROWS`] that was not `truncated`.
    fn page(&self, session_id: String, after: Option<Cursor>) -> ApiFuture<'_, TranscriptPage>;
    /// Agents running in herdr panes right now, for the live-row glyph and jump.
    fn live_agents(&self) -> ApiFuture<'_, Vec<LiveAgent>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListingScope {
    /// Exact project path; sessions in its subdirectories match too.
    pub project: Option<String>,
    /// `None` is the all-time listing, an unbounded scan of every message.
    pub since: Option<DateTime<Utc>>,
    pub limit: usize,
}

impl ListingScope {
    /// The opening view: the last [`LISTING_WINDOW_DAYS`], at most [`LISTING_ROWS`].
    pub(crate) fn recent(project: Option<String>, now: DateTime<Utc>) -> Self {
        Self {
            project,
            since: Some(now - TimeDelta::days(LISTING_WINDOW_DAYS)),
            limit: LISTING_ROWS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct SessionRow {
    pub session_id: String,
    pub last_ts: DateTime<Utc>,
    pub source_agent: String,
    pub project: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct SessionDetail {
    pub session_id: String,
    /// Whole-session count, not limited to the listing window.
    pub message_count: u64,
    /// First non-empty user message, clipped server-side.
    #[serde(default)]
    pub title: Option<String>,
    /// `None` means unknown provenance (pre-stamp rows), never "this machine".
    #[serde(default)]
    pub host: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct TranscriptMessage {
    pub message_id: String,
    pub timestamp: DateTime<Utc>,
    pub role: String,
    #[serde(rename = "search_text")]
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TranscriptPage {
    pub messages: Vec<TranscriptMessage>,
    /// The server dropped rows to fit its byte budget: fetch again after the
    /// last message even when the page came back short.
    pub truncated: bool,
}

/// Keyset position `(timestamp, message_id)`. Timestamps tie, so both halves
/// are needed to neither skip nor repeat rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cursor {
    pub timestamp: DateTime<Utc>,
    pub message_id: String,
}

impl Cursor {
    pub(crate) fn after(message: &TranscriptMessage) -> Self {
        Self {
            timestamp: message.timestamp,
            message_id: message.message_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveAgent {
    pub pane_id: String,
    /// herdr's `agent_session` value: a session id, or a path whose file name
    /// contains one.
    pub session: String,
}

impl LiveAgent {
    pub(crate) fn matches(&self, session_id: &str) -> bool {
        self.session == session_id
            || self
                .session
                .rsplit('/')
                .next()
                .is_some_and(|file| file.contains(session_id))
    }
}

/// What the desk is opened on, computed by the caller from herdr's context.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DeskContext {
    /// The underlying pane's cwd (`focused_pane_cwd`, else `workspace_cwd`).
    pub project: Option<String>,
}

/// How the desk leaves: the caller runs the jump only after the terminal has
/// been restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeskExit {
    Quit,
    Jump { pane_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ApiError {
    /// A pond error envelope; `message` is pond's enriched text, shown verbatim.
    Pond { code: String, message: String },
    /// A non-envelope rejection (axum's plain-text JSON/route errors).
    Rejected { status: u16, body: String },
    /// `/v1/x/sql` is missing: the installed pond predates the endpoint.
    PondTooOld,
    /// Refused, timed out, or no serve could be started.
    Unreachable(String),
    /// The response did not match the contract.
    Decode(String),
    /// A herdr CLI call failed.
    Herdr(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pond { code, message } => write!(f, "pond {code}: {message}"),
            Self::Rejected { status, body } => write!(f, "HTTP {status}: {body}"),
            Self::PondTooOld => f.write_str(
                "this pond has no /v1/x/sql - upgrade pond (`brew upgrade pond` / `cargo install pond-db`)",
            ),
            Self::Unreachable(reason) => write!(f, "pond serve unreachable: {reason}"),
            Self::Decode(reason) => write!(f, "unexpected response from pond: {reason}"),
            Self::Herdr(reason) => write!(f, "herdr: {reason}"),
        }
    }
}

impl std::error::Error for ApiError {}

// ---- pond wire mirrors (packages/pond/src/wire.rs) ----

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct SqlRequest {
    protocol_version: u16,
    pub query: String,
    /// Always the query's own SQL `LIMIT`, so the server's default 100-row
    /// cap never cuts a page.
    pub limit: usize,
    pub timeout_seconds: u64,
}

impl SqlRequest {
    pub(crate) fn new(query: String, limit: usize, timeout_seconds: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            query,
            limit,
            timeout_seconds,
        }
    }
}

/// NULL fields are omitted from `rows`: decode with `#[serde(default)]`,
/// never by key presence.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct SqlResponse {
    pub rows: Vec<serde_json::Value>,
    pub truncated: bool,
}

impl SqlResponse {
    pub(crate) fn into_rows<T: DeserializeOwned>(self) -> Result<Vec<T>, ApiError> {
        self.rows
            .into_iter()
            .map(|row| {
                serde_json::from_value(row).map_err(|error| ApiError::Decode(error.to_string()))
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct SearchRequest {
    protocol_version: u16,
    pub query: String,
    pub filters: SearchFilters,
    pub limit: usize,
}

impl SearchRequest {
    pub(crate) fn new(query: String, limit: usize) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            query,
            filters: SearchFilters::default(),
            limit,
        }
    }

    /// The listing's scope as search filters; `from_date` is a calendar day.
    pub(crate) fn within(mut self, scope: &ListingScope) -> Self {
        self.filters = SearchFilters {
            project: scope.project.clone().map(ProjectFilter::Contains),
            from_date: scope
                .since
                .map(|since| since.format("%Y-%m-%d").to_string()),
        };
        self
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub(crate) struct SearchFilters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectFilter {
    Contains(String),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct SearchResponse {
    pub sessions: Vec<SearchSession>,
    /// 0 means the filters excluded everything before retrieval - distinct
    /// from "nothing matched".
    #[serde(default)]
    pub searchable_in_scope: usize,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct SearchSession {
    pub session_id: String,
    pub source_agent: String,
    pub session_messages_count: usize,
    pub matched_message_count: usize,
    pub matches: Vec<SearchMatch>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct SearchMatch {
    pub timestamp: DateTime<Utc>,
    pub text: String,
}

// ---- SQL ----
//
// Every query carries an explicit LIMIT, since the server's row caps apply
// only after full collection and do not bound the scan; the listing
// scans narrow columns with a literal `timestamp >=` bound the zonemap can
// prune; JSON getters run only in page-scoped (`session_id IN (...)`) queries.
// Every interpolated value goes through `quote` - no other escaping exists.

pub(crate) const READY_SQL: &str = "SELECT 1 AS ready";

pub(crate) const LISTING_ROWS: usize = 200;
pub(crate) const PREVIEW_ROWS: usize = 12;
pub(crate) const PAGE_ROWS: usize = 50;
pub(crate) const TITLE_CHARS: usize = 240;
pub(crate) const LISTING_WINDOW_DAYS: i64 = 14;

pub(crate) fn listing_sql(scope: &ListingScope) -> String {
    let mut filters = vec!["source_agent NOT LIKE '%/%'".to_owned()];
    if let Some(since) = scope.since {
        filters.push(format!(
            "timestamp >= TIMESTAMP {}",
            timestamp_literal(since)
        ));
    }
    if let Some(project) = &scope.project {
        let project = project.trim_end_matches('/');
        filters.push(format!(
            "(project = {} OR starts_with(project, {}))",
            quote(project),
            quote(&format!("{project}/"))
        ));
    }
    format!(
        "SELECT session_id, MAX(timestamp) AS last_ts, MIN(source_agent) AS source_agent, \
         MIN(project) AS project FROM messages WHERE {} GROUP BY session_id \
         ORDER BY last_ts DESC, session_id LIMIT {}",
        filters.join(" AND "),
        scope.limit
    )
}

pub(crate) fn hydrate_sql(session_ids: &[String]) -> String {
    let ids = session_ids
        .iter()
        .map(|id| quote(id))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT session_id, COUNT(*) AS message_count, \
         substr(first_value(search_text ORDER BY timestamp, message_id) \
         FILTER (WHERE role = 'user' AND search_text <> ''), 1, {TITLE_CHARS}) AS title, \
         MAX(json_get_string(options, 'pond', 'ingest', 'host', 'hostname')) AS host \
         FROM messages WHERE session_id IN ({ids}) GROUP BY session_id LIMIT {}",
        session_ids.len()
    )
}

pub(crate) fn preview_sql(session_id: &str) -> String {
    format!(
        "SELECT message_id, timestamp, role, search_text FROM messages \
         WHERE session_id = {} AND search_text <> '' \
         ORDER BY timestamp DESC, message_id DESC LIMIT {PREVIEW_ROWS}",
        quote(session_id)
    )
}

/// DataFusion rejects the row-value form `(timestamp, message_id) > (...)` with
/// a timestamp literal, so the seek predicate is spelled out.
pub(crate) fn page_sql(session_id: &str, after: Option<&Cursor>) -> String {
    let seek = after.map_or_else(String::new, |cursor| {
        let ts = timestamp_literal(cursor.timestamp);
        format!(
            " AND (timestamp > TIMESTAMP {ts} OR (timestamp = TIMESTAMP {ts} AND message_id > {}))",
            quote(&cursor.message_id)
        )
    });
    format!(
        "SELECT message_id, timestamp, role, search_text FROM messages \
         WHERE session_id = {} AND search_text <> ''{seek} \
         ORDER BY timestamp, message_id LIMIT {PAGE_ROWS}",
        quote(session_id)
    )
}

/// A single-quoted SQL string literal.
pub(crate) fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Microsecond precision: the store's resolution, so a cursor round-trips exactly.
pub(crate) fn timestamp_literal(ts: DateTime<Utc>) -> String {
    quote(&ts.to_rfc3339_opts(SecondsFormat::Micros, true))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::fake_pond::{golden, ts};

    #[test]
    fn quote_escapes_single_quotes() {
        assert_eq!(quote("it's"), "'it''s'");
    }

    #[test]
    fn listing_is_bounded_and_scoped() {
        let sql = listing_sql(&ListingScope {
            project: Some("/home/me/pj/pond/".to_owned()),
            since: Some(ts("2026-09-11T00:00:00Z")),
            limit: 200,
        });
        assert!(sql.contains("timestamp >= TIMESTAMP '2026-09-11T00:00:00.000000Z'"));
        assert!(sql.contains("project = '/home/me/pj/pond'"));
        assert!(sql.contains("starts_with(project, '/home/me/pj/pond/')"));
        assert!(sql.ends_with("LIMIT 200"));
    }

    #[test]
    fn all_time_listing_has_no_time_bound() {
        let sql = listing_sql(&ListingScope {
            project: None,
            since: None,
            limit: 10,
        });
        assert!(!sql.contains("timestamp >="));
        assert!(!sql.contains("project ="));
    }

    #[test]
    fn page_seek_uses_the_composite_cursor() {
        let cursor = Cursor {
            timestamp: ts("2026-09-22T14:24:45.991123Z"),
            message_id: "m'1".to_owned(),
        };
        let sql = page_sql("s1", Some(&cursor));
        assert!(sql.contains(
            "(timestamp > TIMESTAMP '2026-09-22T14:24:45.991123Z' OR (timestamp = TIMESTAMP \
             '2026-09-22T14:24:45.991123Z' AND message_id > 'm''1'))"
        ));
        assert!(sql.contains("ORDER BY timestamp, message_id LIMIT 50"));
        assert!(!page_sql("s1", None).contains("message_id >"));
    }

    #[test]
    fn every_query_carries_a_limit() {
        let scope = ListingScope {
            project: None,
            since: None,
            limit: 5,
        };
        for sql in [
            listing_sql(&scope),
            hydrate_sql(&["a".to_owned()]),
            preview_sql("a"),
            page_sql("a", None),
        ] {
            assert!(sql.contains(" LIMIT "), "{sql}");
        }
    }

    #[test]
    fn golden_sql_response_decodes() {
        let decode = |body| serde_json::from_str::<SqlResponse>(body).unwrap();
        let rows: Vec<SessionRow> = decode(golden::SQL_LISTING).into_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].last_ts, ts("2026-09-25T04:00:02.384123Z"));

        let details: Vec<SessionDetail> = decode(golden::SQL_HYDRATE).into_rows().unwrap();
        assert_eq!(details[1].host, None);
        assert_eq!(details[1].title, None);

        let messages: Vec<TranscriptMessage> = decode(golden::SQL_PAGE).into_rows().unwrap();
        assert_eq!(messages[0].timestamp, messages[1].timestamp);
    }

    #[test]
    fn golden_search_and_error_decode() {
        let search: SearchResponse = serde_json::from_str(golden::SEARCH).unwrap();
        assert_eq!(search.sessions[0].matches[1].text, "timer fixed");
        let empty: SearchResponse = serde_json::from_str(golden::SEARCH_OUT_OF_SCOPE).unwrap();
        assert_eq!(empty.searchable_in_scope, 0);
        let error: ErrorEnvelope = serde_json::from_str(golden::SQL_ERROR).unwrap();
        assert_eq!(error.error.code, "validation_failed");
    }

    #[test]
    fn live_agent_matches_id_or_path() {
        let by_id = LiveAgent {
            pane_id: "p1".to_owned(),
            session: "abc".to_owned(),
        };
        let by_path = LiveAgent {
            session: "/home/me/.codex/sessions/rollout-2026-abc.jsonl".to_owned(),
            ..by_id.clone()
        };
        assert!(by_id.matches("abc"));
        assert!(by_path.matches("abc"));
        assert!(!by_path.matches("xyz"));
    }
}
