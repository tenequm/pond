use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredMessage {
    pub message: Message,
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredSession {
    pub session: Session,
    pub messages: Vec<StoredMessage>,
}
