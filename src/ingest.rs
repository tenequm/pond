use std::collections::HashSet;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::{
    adapter::SourceAdapter,
    substrate::{MessageWrite, PondStore, UpsertStatus},
    types::{Message, Part, PartKind, Role, Session},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum IngestEvent {
    Session(Session),
    Message(Message),
    Part(Part),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestSummary {
    pub inserted: usize,
    pub matched: usize,
    pub errors: usize,
}

impl IngestSummary {
    pub fn accepted(&self) -> usize {
        self.inserted + self.matched
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    ValidationFailed(String),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValidationFailed(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for IngestError {}

#[derive(Debug, Default)]
pub struct IngestValidator {
    session: Option<Session>,
    current_message: Option<Message>,
    current_parts: Vec<Part>,
    messages: Vec<BufferedMessage>,
    failed: bool,
}

#[derive(Debug)]
struct BufferedMessage {
    message: Message,
    parts: Vec<Part>,
    search_text: Option<String>,
}

impl IngestValidator {
    pub async fn push(
        &mut self,
        store: &PondStore,
        event: IngestEvent,
    ) -> Result<Vec<UpsertStatus>> {
        if self.failed {
            return Err(IngestError::ValidationFailed(
                "session substream already failed validation".to_owned(),
            )
            .into());
        }

        match event {
            IngestEvent::Session(session) => {
                if self.session.is_some() {
                    let statuses = self.flush_session(store).await?;
                    self.session = Some(session);
                    Ok(statuses)
                } else {
                    self.session = Some(session);
                    Ok(Vec::new())
                }
            }
            IngestEvent::Message(message) => {
                let Some(session) = &self.session else {
                    self.failed = true;
                    return Err(IngestError::ValidationFailed(
                        "first event in a session stream must be Session".to_owned(),
                    )
                    .into());
                };
                if message.session_id() != session.id {
                    self.failed = true;
                    return Err(IngestError::ValidationFailed(format!(
                        "message {} references session {}, expected {}",
                        message.id(),
                        message.session_id(),
                        session.id
                    ))
                    .into());
                }
                self.flush_message()?;
                self.current_message = Some(message);
                Ok(Vec::new())
            }
            IngestEvent::Part(part) => {
                let Some(message) = &self.current_message else {
                    self.failed = true;
                    return Err(IngestError::ValidationFailed(
                        "part event appeared before a message".to_owned(),
                    )
                    .into());
                };
                if part.message_id != message.id() {
                    self.failed = true;
                    return Err(IngestError::ValidationFailed(format!(
                        "part {} references message {}, expected {}",
                        part.id,
                        part.message_id,
                        message.id()
                    ))
                    .into());
                }
                self.current_parts.push(part);
                Ok(Vec::new())
            }
        }
    }

    pub async fn finish(&mut self, store: &PondStore) -> Result<Vec<UpsertStatus>> {
        self.flush_session(store).await
    }

    fn flush_message(&mut self) -> Result<()> {
        let Some(message) = self.current_message.take() else {
            return Ok(());
        };
        let parts = std::mem::take(&mut self.current_parts);
        let search_text = search_text(&message, &parts);
        self.messages.push(BufferedMessage {
            message,
            parts,
            search_text,
        });
        Ok(())
    }

    async fn flush_session(&mut self, store: &PondStore) -> Result<Vec<UpsertStatus>> {
        self.flush_message()?;
        let Some(session) = self.session.take() else {
            return Ok(Vec::new());
        };
        let messages = std::mem::take(&mut self.messages);
        validate_batch_keys(&messages)?;
        let writes = messages
            .iter()
            .map(|message| MessageWrite {
                message: &message.message,
                parts: &message.parts,
                search_text: message.search_text.as_deref(),
            })
            .collect::<Vec<_>>();
        store.upsert_session_bundle(&session, &writes).await
    }
}

fn validate_batch_keys(messages: &[BufferedMessage]) -> Result<()> {
    let mut message_ids = HashSet::with_capacity(messages.len());
    let mut part_ids = HashSet::new();

    for message in messages {
        let message_id = message.message.id();
        if !message_ids.insert(message_id.to_owned()) {
            return Err(IngestError::ValidationFailed(format!(
                "duplicate message id {message_id} in session substream"
            ))
            .into());
        }

        for part in &message.parts {
            let key = (part.message_id.as_str(), part.id.as_str());
            if !part_ids.insert((key.0.to_owned(), key.1.to_owned())) {
                return Err(IngestError::ValidationFailed(format!(
                    "duplicate part id {} for message {} in session substream",
                    part.id, part.message_id
                ))
                .into());
            }
        }
    }

    Ok(())
}

pub async fn ingest_adapter<A: SourceAdapter>(
    store: &PondStore,
    adapter: &A,
) -> Result<IngestSummary> {
    let mut summary = IngestSummary {
        inserted: 0,
        matched: 0,
        errors: 0,
    };
    let discovered = adapter.discover();
    tokio::pin!(discovered);

    while let Some(session_ref) = discovered.next().await {
        let session_ref = session_ref?;
        let decoded = adapter.decode(session_ref);
        tokio::pin!(decoded);
        let mut validator = IngestValidator::default();
        let mut valid = true;

        while let Some(event) = decoded.next().await {
            match event {
                Ok(event) => match validator.push(store, event).await {
                    Ok(statuses) => summary.add_statuses(&statuses),
                    Err(error) => {
                        summary.errors += 1;
                        valid = false;
                        tracing::warn!(%error, "aborting invalid session substream");
                        break;
                    }
                },
                Err(error) => {
                    summary.errors += 1;
                    valid = false;
                    tracing::warn!(%error, "aborting undecodable session substream");
                    break;
                }
            }
        }
        if valid {
            let statuses = validator.finish(store).await?;
            summary.add_statuses(&statuses);
        }
    }

    Ok(summary)
}

impl IngestSummary {
    fn add_statuses(&mut self, statuses: &[UpsertStatus]) {
        for status in statuses {
            match status {
                UpsertStatus::Inserted => self.inserted += 1,
                UpsertStatus::Matched => self.matched += 1,
            }
        }
    }
}

pub fn search_text(message: &Message, parts: &[Part]) -> Option<String> {
    let mut chunks = Vec::new();
    for part in parts {
        match (message.role(), &part.kind) {
            (Role::User | Role::Assistant, PartKind::Text { text }) => chunks.push(text.clone()),
            (Role::Assistant, PartKind::ToolCall { name, params, .. }) => {
                chunks.push(name.clone());
                collect_string_leaves(params, &mut chunks);
            }
            (
                Role::User | Role::Assistant,
                PartKind::File {
                    media_type,
                    file_name,
                    data,
                },
            ) => {
                if let Some(file_name) = file_name {
                    chunks.push(file_name.clone());
                }
                chunks.push(media_type.clone());
                if let crate::types::FileData::Url(uri) = data {
                    chunks.push(uri.clone());
                }
            }
            (
                Role::System | Role::Tool,
                PartKind::Text { .. }
                | PartKind::Reasoning { .. }
                | PartKind::File { .. }
                | PartKind::ToolCall { .. }
                | PartKind::ToolResult { .. }
                | PartKind::ToolApprovalRequest { .. }
                | PartKind::ToolApprovalResponse { .. },
            )
            | (
                Role::User | Role::Assistant,
                PartKind::Reasoning { .. }
                | PartKind::ToolResult { .. }
                | PartKind::ToolApprovalRequest { .. }
                | PartKind::ToolApprovalResponse { .. },
            )
            | (Role::User, PartKind::ToolCall { .. }) => {}
        }
    }

    let text = chunks
        .into_iter()
        .filter(|chunk| !chunk.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() { None } else { Some(text) }
}

fn collect_string_leaves(value: &serde_json::Value, chunks: &mut Vec<String>) {
    match value {
        serde_json::Value::String(text) => chunks.push(text.clone()),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_string_leaves(value, chunks);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_string_leaves(value, chunks);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}
