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
