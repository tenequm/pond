use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{PROTOCOL_VERSION, types::StoredSession};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    ValidationFailed,
    VersionUnsupported,
    NotFound,
    NamespaceUnknown,
    StorageUnavailable,
    Conflict,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetEnvelope {
    Success(GetResponse),
    Error(ErrorEnvelope),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetRequest {
    pub protocol_version: u16,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub up_to: Option<String>,
    #[serde(default)]
    pub context_depth: usize,
    #[serde(default = "default_max_messages")]
    pub max_messages: usize,
    #[serde(default)]
    pub include_thinking: bool,
    #[serde(default)]
    pub include_tool_results: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetResponse {
    #[serde(flatten)]
    pub result: GetResult,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GetResult {
    Session(StoredSession),
    Message {
        session: crate::types::Session,
        messages: Vec<crate::types::StoredMessage>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SearchEnvelope {
    Success(SearchResponse),
    Error(ErrorEnvelope),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    #[default]
    Hybrid,
    Vector,
    Fts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectMatch {
    #[default]
    Exact,
    Contains,
    IsNull,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    pub protocol_version: u16,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    pub query: String,
    #[serde(default)]
    pub search_mode: SearchMode,
    #[serde(default = "default_rrf_k")]
    pub rrf_k: u32,
    #[serde(default)]
    pub filters: SearchFilters,
    #[serde(default = "default_true")]
    pub boost_recent: bool,
    #[serde(default)]
    pub group_by_conversation: bool,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SearchFilters {
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub project_match: ProjectMatch,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub source_agent: Option<String>,
    #[serde(default)]
    pub from_date: Option<String>,
    #[serde(default)]
    pub to_date: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub min_score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResponse {
    #[serde(flatten)]
    pub result: SearchResultBody,
    pub total: usize,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SearchResultBody {
    Hits { hits: Vec<Hit> },
    Groups { groups: Vec<Group> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    pub session_id: String,
    pub message_id: String,
    pub role: String,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub source_agent: String,
    pub preview: String,
    pub score: f64,
    pub base_score: f64,
    pub recency_boost: f64,
    pub matched_via: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Group {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub source_agent: String,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
    pub message_count: usize,
    pub preview: String,
    pub best_score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IngestEnvelope {
    Success(IngestResponse),
    Error(ErrorEnvelope),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestRequest {
    pub protocol_version: u16,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    pub events: Vec<crate::ingest::IngestEvent>,
}

/// `pond_ingest` response (design.md 3.6.4). v1 reports the aggregate accounting
/// the CLI already prints; the per-row `results` array is deferred with the
/// HTTP/MCP transports (Stage 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResponse {
    pub accepted: usize,
    pub rejected: usize,
    pub inserted: usize,
    pub matched: usize,
    pub request_id: String,
}

fn default_rrf_k() -> u32 {
    60
}

fn default_limit() -> usize {
    10
}

fn default_true() -> bool {
    true
}

pub fn new_request_id() -> String {
    format!("req_{}", Uuid::now_v7())
}

pub fn default_namespace() -> String {
    "local".to_owned()
}

fn default_max_messages() -> usize {
    100
}

pub fn validate_protocol(version: u16) -> Result<(), ErrorEnvelope> {
    if version == PROTOCOL_VERSION {
        return Ok(());
    }

    Err(error(
        ErrorCode::VersionUnsupported,
        "unsupported protocol_version",
        serde_json::json!({
            "received": version,
            "supported": [PROTOCOL_VERSION],
        }),
    ))
}

pub fn error(code: ErrorCode, message: impl Into<String>, details: Value) -> ErrorEnvelope {
    ErrorEnvelope {
        error: ErrorBody {
            code,
            message: message.into(),
            details,
        },
        request_id: new_request_id(),
    }
}

pub fn storage_error(error_value: anyhow::Error) -> ErrorEnvelope {
    error(
        ErrorCode::StorageUnavailable,
        "storage operation failed",
        serde_json::json!({ "underlying": error_value.to_string() }),
    )
}
