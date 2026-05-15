//! OpenAI Codex CLI adapter.
//!
//! Source path: `~/.codex/sessions/<year>/<month>/<day>/rollout-<ts>-<uuid>.jsonl`.
//! Each line is an envelope `{timestamp, type, payload}`. Top-level types:
//! `session_meta` (consumed up front for Session), `event_msg` /
//! `turn_context` (transport noise, skipped), `response_item` (the per-turn
//! model interaction: subtypes `message`, `reasoning`, `function_call`,
//! `function_call_output`, `custom_tool_call`).

use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::{
    config::expand_home_under,
    sessions::IngestEvent,
    wire::{Message, Part, PartKind, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, DiscoverFuture, Env, EventStream, collect_jsonl_files,
    compact_json, empty_options, part_id,
};

const NAME: &str = "codex-cli";

/// Stateless factory: opens [`CodexCliAdapter`] instances and probes for the
/// canonical install location under `~/.codex/sessions`.
pub struct CodexCliFactory;

impl AdapterFactory for CodexCliFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        #[derive(Deserialize)]
        struct Cfg {
            path: PathBuf,
        }
        let cfg: Cfg = serde_json::from_value(config)
            .map_err(|err| AdapterError::config(NAME, format!("bad config blob: {err}")))?;
        let path = match std::env::var_os("HOME") {
            Some(home) => expand_home_under(&cfg.path, Path::new(&home)),
            None => cfg.path,
        };
        Ok(Box::new(CodexCliAdapter::new(path)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = env.home.join(".codex").join("sessions");
        path.exists().then(|| json!({ "path": path }))
    }
}

#[derive(Debug, Clone)]
pub struct CodexCliAdapter {
    root: PathBuf,
}

impl CodexCliAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for CodexCliAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let root = self.root.clone();
        Box::pin(async move {
            // Header-free count: see the matching note on ClaudeCodeAdapter.
            let paths = collect_jsonl_files(&root)
                .await
                .map_err(|io| AdapterError::io(NAME, io.path, io.source))?;
            Ok(paths.len())
        })
    }

    fn events(&self) -> EventStream<'_> {
        let root = self.root.clone();
        Box::pin(stream! {
            let paths = match collect_jsonl_files(&root).await {
                Ok(paths) => paths,
                Err(io) => {
                    yield Err(AdapterError::io(NAME, io.path, io.source));
                    return;
                }
            };
            for path in paths {
                let path_display = path.display().to_string();
                let meta = match session_meta(&path, &path_display).await {
                    Ok(meta) => meta,
                    Err(error) => {
                        yield Err(error);
                        continue;
                    }
                };
                let session_id = meta.id.clone();
                let default_timestamp = meta.created_at;
                yield Ok(IngestEvent::Session(meta));

                let file = match tokio::fs::File::open(&path).await {
                    Ok(file) => file,
                    Err(source) => {
                        yield Err(AdapterError::io(NAME, path_display.clone(), source));
                        continue;
                    }
                };

                // One logical "message" per `response_item` record; the
                // function_call / function_call_output / reasoning rows
                // synthesize Assistant/Tool messages so they still hang off
                // a Part in pond's schema.
                let mut lines = BufReader::new(file).lines();
                let mut line_number = 0usize;
                loop {
                    let next = lines.next_line().await;
                    let line = match next {
                        Ok(Some(line)) => line,
                        Ok(None) => break,
                        Err(source) => {
                            yield Err(AdapterError::io(NAME, path_display.clone(), source));
                            break;
                        }
                    };
                    line_number += 1;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let row = match serde_json::from_str::<Value>(&line) {
                        Ok(value) => value,
                        Err(source) => {
                            // Same per-line skip policy as the claude-code
                            // adapter: surface the bad line, continue parsing.
                            yield Err(AdapterError::parse(
                                NAME,
                                path_display.clone(),
                                line_number,
                                source,
                            ));
                            continue;
                        }
                    };
                    match events_from_row(&session_id, line_number, &row, default_timestamp) {
                        Ok(events) => {
                            for event in events {
                                yield Ok(event);
                            }
                        }
                        Err(message) => {
                            yield Err(AdapterError::schema(
                                NAME,
                                format!("{path_display}:{line_number}"),
                                message,
                            ));
                            continue;
                        }
                    }
                }
            }
        })
    }
}

async fn session_meta(path: &Path, path_display: &str) -> Result<Session, AdapterError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?;
    let mut lines = BufReader::new(file).lines();
    let first = lines
        .next_line()
        .await
        .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?
        .ok_or_else(|| {
            AdapterError::schema(NAME, path_display.to_owned(), "empty jsonl session")
        })?;
    let row = serde_json::from_str::<Value>(&first)
        .map_err(|source| AdapterError::parse(NAME, path_display.to_owned(), 1, source))?;
    if row.get("type").and_then(Value::as_str) != Some("session_meta") {
        return Err(AdapterError::schema(
            NAME,
            format!("{path_display}:1"),
            "first row must be session_meta",
        ));
    }
    let payload = row.get("payload").cloned().unwrap_or(Value::Null);
    let id = payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                format!("{path_display}:1"),
                "session_meta missing payload.id",
            )
        })?
        .to_owned();
    let created_at = payload
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            row.get("timestamp")
                .and_then(Value::as_str)
                .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
                .map(|dt| dt.with_timezone(&Utc))
        })
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                format!("{path_display}:1"),
                "session_meta has no parseable timestamp",
            )
        })?;
    let project = payload
        .get("cwd")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": "codex-cli",
            "originator": payload.get("originator"),
            "cli_version": payload.get("cli_version"),
            "model_provider": payload.get("model_provider"),
            "git": payload.get("git"),
        }),
    );

    Ok(Session {
        id,
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "codex-cli".to_owned(),
        created_at,
        project,
        options,
    })
}

/// Map one codex-cli JSONL record into zero-or-more `IngestEvent`s. Records pond
/// keeps: `response_item` with `payload.type = "message"` (User/Assistant/
/// System message + text Parts), `function_call` (Assistant + ToolCall),
/// `function_call_output` (Tool + ToolResult), `reasoning` (Assistant +
/// Reasoning Part). `session_meta` is consumed up front; `event_msg` and
/// `turn_context` are transport noise.
fn events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
) -> Result<Vec<IngestEvent>, String> {
    let kind = row.get("type").and_then(Value::as_str).unwrap_or_default();
    if kind != "response_item" {
        return Ok(Vec::new());
    }
    let timestamp = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(default_timestamp);
    let payload = row.get("payload").unwrap_or(&Value::Null);
    let payload_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
    let message_id = format!("{session_id}:{line:06}");

    match payload_type {
        "message" => message_events(session_id, &message_id, timestamp, payload),
        "function_call" => Ok(tool_call_events(
            session_id,
            &message_id,
            timestamp,
            payload,
        )),
        "function_call_output" => Ok(tool_result_events(
            session_id,
            &message_id,
            timestamp,
            payload,
        )),
        "reasoning" => Ok(reasoning_events(
            session_id,
            &message_id,
            timestamp,
            payload,
        )),
        // Unknown response_item subtypes (newer codex-cli versions) - skip
        // rather than fail the session.
        _ => Ok(Vec::new()),
    }
}

fn message_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
) -> Result<Vec<IngestEvent>, String> {
    let role = payload
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| "message missing role".to_owned())?;
    let content = payload
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut parts = Vec::with_capacity(content.len());
    for (ordinal, item) in content.iter().enumerate() {
        let text = item
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| compact_json(item));
        parts.push(Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: empty_options(),
            kind: PartKind::Text { text },
        });
    }

    let (message, keep_parts) = match role {
        "user" => (
            Message::User {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: empty_options(),
            },
            true,
        ),
        "assistant" => (
            Message::Assistant {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: empty_options(),
            },
            true,
        ),
        // `developer` rows are codex-cli's system-prompt frames; map to System
        // and collapse text Parts into the Message's `content` per pond's
        // canonical shape, so they don't double-store and don't add to FTS.
        "developer" | "system" => (
            Message::System {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                content: parts
                    .iter()
                    .filter_map(|part| match &part.kind {
                        PartKind::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
                options: empty_options(),
            },
            false,
        ),
        other => return Err(format!("unsupported codex-cli role {other}")),
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    if keep_parts {
        events.extend(parts.into_iter().map(IngestEvent::Part));
    }
    Ok(events)
}

fn tool_call_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
) -> Vec<IngestEvent> {
    let call_id = payload
        .get("call_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let name = payload
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("function")
        .to_owned();
    let params = match payload.get("arguments") {
        Some(Value::String(text)) => {
            serde_json::from_str::<Value>(text).unwrap_or_else(|_| Value::String(text.clone()))
        }
        Some(other) => other.clone(),
        None => Value::Null,
    };
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::ToolCall {
            call_id,
            name,
            params,
            provider_executed: false,
        },
    };
    vec![
        IngestEvent::Message(Message::Assistant {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: empty_options(),
        }),
        IngestEvent::Part(part),
    ]
}

fn tool_result_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
) -> Vec<IngestEvent> {
    let call_id = payload
        .get("call_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let result = payload.get("output").cloned().unwrap_or(Value::Null);
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id,
            name: "function".to_owned(),
            is_failure: false,
            result,
        },
    };
    vec![
        IngestEvent::Message(Message::Tool {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: empty_options(),
        }),
        IngestEvent::Part(part),
    ]
}

fn reasoning_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
) -> Vec<IngestEvent> {
    let summary = payload
        .get("summary")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::Reasoning { text: summary },
    };
    vec![
        IngestEvent::Message(Message::Assistant {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: empty_options(),
        }),
        IngestEvent::Part(part),
    ]
}
