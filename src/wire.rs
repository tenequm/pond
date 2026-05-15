use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::PROTOCOL_VERSION;

pub type ProviderOptions = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_message_id: Option<String>,
    pub source_agent: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default)]
    pub options: ProviderOptions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    System {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        content: String,
        #[serde(default)]
        options: ProviderOptions,
    },
    User {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        #[serde(default)]
        options: ProviderOptions,
    },
    Assistant {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        #[serde(default)]
        options: ProviderOptions,
    },
    Tool {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        #[serde(default)]
        options: ProviderOptions,
    },
}

impl Message {
    pub fn id(&self) -> &str {
        match self {
            Self::System { id, .. }
            | Self::User { id, .. }
            | Self::Assistant { id, .. }
            | Self::Tool { id, .. } => id,
        }
    }

    pub fn session_id(&self) -> &str {
        match self {
            Self::System { session_id, .. }
            | Self::User { session_id, .. }
            | Self::Assistant { session_id, .. }
            | Self::Tool { session_id, .. } => session_id,
        }
    }

    pub fn role(&self) -> Role {
        match self {
            Self::System { .. } => Role::System,
            Self::User { .. } => Role::User,
            Self::Assistant { .. } => Role::Assistant,
            Self::Tool { .. } => Role::Tool,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::System { timestamp, .. }
            | Self::User { timestamp, .. }
            | Self::Assistant { timestamp, .. }
            | Self::Tool { timestamp, .. } => *timestamp,
        }
    }

    pub fn options(&self) -> &ProviderOptions {
        match self {
            Self::System { options, .. }
            | Self::User { options, .. }
            | Self::Assistant { options, .. }
            | Self::Tool { options, .. } => options,
        }
    }

    pub fn system_content(&self) -> Option<&str> {
        match self {
            Self::System { content, .. } => Some(content),
            Self::User { .. } | Self::Assistant { .. } | Self::Tool { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Part {
    pub id: String,
    pub message_id: String,
    pub ordinal: i32,
    #[serde(default)]
    pub options: ProviderOptions,
    #[serde(flatten)]
    pub kind: PartKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartKind {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
    },
    File {
        media_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_name: Option<String>,
        data: FileData,
    },
    ToolCall {
        call_id: String,
        name: String,
        params: Value,
        provider_executed: bool,
    },
    ToolResult {
        call_id: String,
        name: String,
        is_failure: bool,
        result: Value,
    },
    ToolApprovalRequest {
        approval_id: String,
        tool_call_id: String,
    },
    ToolApprovalResponse {
        approval_id: String,
        approved: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl PartKind {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Text { .. } => "text",
            Self::Reasoning { .. } => "reasoning",
            Self::File { .. } => "file",
            Self::ToolCall { .. } => "tool_call",
            Self::ToolResult { .. } => "tool_result",
            Self::ToolApprovalRequest { .. } => "tool_approval_request",
            Self::ToolApprovalResponse { .. } => "tool_approval_response",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FileData {
    String(String),
    Bytes(Vec<u8>),
    Url(String),
}

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
    #[serde(default)]
    pub namespace: Option<String>,
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
    Session {
        session: Session,
        messages: Vec<Message>,
        parts: Vec<Part>,
    },
    Message {
        session: Session,
        messages: Vec<Message>,
        parts: Vec<Part>,
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
pub enum ProjectMatch {
    #[default]
    Exact,
    Contains,
    IsNull,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    pub protocol_version: u16,
    #[serde(default)]
    pub namespace: Option<String>,
    pub query: String,
    // No request-level `search_mode`: the server decides between hybrid and
    // FTS-only based on the embedder + embeddings-coverage state. The response
    // carries no top-level mode field either - per-hit `matched_via` reports
    // which retriever(s) ranked each row.
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
    #[serde(default)]
    pub namespace: Option<String>,
    pub events: Vec<crate::sessions::IngestEvent>,
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

pub const DEFAULT_NAMESPACE: &str = "local";

pub fn default_namespace() -> String {
    DEFAULT_NAMESPACE.to_owned()
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

impl From<crate::Error> for ErrorEnvelope {
    fn from(error_value: crate::Error) -> Self {
        match error_value {
            crate::Error::Validation(message) => {
                error(ErrorCode::ValidationFailed, message, serde_json::json!({}))
            }
            crate::Error::NotFound(message) => {
                error(ErrorCode::NotFound, message, serde_json::json!({}))
            }
            crate::Error::NamespaceUnknown(message) => {
                error(ErrorCode::NamespaceUnknown, message, serde_json::json!({}))
            }
            crate::Error::Storage(error_value) => storage_error(error_value),
            crate::Error::Internal(message) => {
                error(ErrorCode::Internal, message, serde_json::json!({}))
            }
        }
    }
}

pub fn storage_error(error_value: anyhow::Error) -> ErrorEnvelope {
    error(
        ErrorCode::StorageUnavailable,
        "storage operation failed",
        serde_json::json!({ "underlying": error_value.to_string() }),
    )
}
