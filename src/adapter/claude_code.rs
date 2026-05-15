//! Claude Code CLI adapter.
//!
//! Source path: `~/.claude/projects/<encoded-project-path>/<session-uuid>.jsonl`.
//! Each `.jsonl` file is one session; lines are typed entries linked via a
//! `parentUuid` -> `uuid` chain. Tool results arrive as `user` entries whose
//! `message.content[]` contains `tool_result` blocks with a parallel
//! `toolUseResult` field carrying structured data.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use async_stream::stream;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::{
    config::expand_home_under,
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, Env, EventStream, collect_jsonl_files, compact_json,
    empty_options, part_id,
};

/// Stable adapter name. Surfaces as the `[sources.claude-code]` config key,
/// the `pond sync claude-code` CLI arg, and `Session.source_agent` on every
/// emitted row.
const NAME: &str = "claude-code";

/// Stateless factory: opens [`ClaudeCodeAdapter`] instances from config and
/// probes for the canonical install location under `~/.claude/projects`.
pub struct ClaudeCodeFactory;

impl AdapterFactory for ClaudeCodeFactory {
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
        Ok(Box::new(ClaudeCodeAdapter::new(path)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = env.home.join(".claude").join("projects");
        path.exists().then(|| json!({ "path": path }))
    }
}

/// Configured claude-code reader. Walks a tree of `*.jsonl` files under
/// [`Self::root`] and yields canonical events in source order per session.
#[derive(Debug, Clone)]
pub struct ClaudeCodeAdapter {
    root: PathBuf,
}

impl ClaudeCodeAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for ClaudeCodeAdapter {
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
                let session_event = match session_from_file(&path, &path_display).await {
                    Ok(session) => session,
                    Err(error) => {
                        yield Err(error);
                        continue;
                    }
                };
                let session_id = session_event.id.clone();
                let default_timestamp = session_event.created_at;
                yield Ok(IngestEvent::Session(session_event));

                let file = match tokio::fs::File::open(&path).await {
                    Ok(file) => file,
                    Err(source) => {
                        yield Err(AdapterError::io(NAME, path_display, source));
                        continue;
                    }
                };

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
                    let value = match serde_json::from_str::<Value>(&line) {
                        Ok(value) => value,
                        Err(source) => {
                            yield Err(AdapterError::parse(
                                NAME,
                                path_display.clone(),
                                line_number,
                                source,
                            ));
                            break;
                        }
                    };
                    match events_from_row(&session_id, line_number, &value, default_timestamp) {
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
                            break;
                        }
                    }
                }
            }
        })
    }
}

async fn session_from_file(path: &Path, path_display: &str) -> Result<Session, AdapterError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?;
    let mut lines = BufReader::new(file).lines();
    let mut first = None::<(usize, Value)>;
    let mut created_at = None;
    let mut project = None;
    let mut version = None;
    let mut line_number = 0usize;

    loop {
        let Some(line) = lines
            .next_line()
            .await
            .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?
        else {
            break;
        };
        line_number += 1;
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<Value>(&line).map_err(|source| {
            AdapterError::parse(NAME, path_display.to_owned(), line_number, source)
        })?;

        if first.is_none() {
            first = Some((line_number, row.clone()));
        }
        if created_at.is_none() {
            created_at = parse_timestamp(&row).ok();
        }
        if project.is_none() {
            project = row
                .get("cwd")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
        if version.is_none() {
            version = row
                .get("version")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
    }

    let Some((first_line, first)) = first else {
        return Err(AdapterError::schema(
            NAME,
            path_display.to_owned(),
            "empty jsonl session",
        ));
    };

    let id = first
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                format!("{path_display}:{first_line}"),
                format!("line {first_line} missing sessionId"),
            )
        })?
        .to_owned();
    let created_at = created_at.ok_or_else(|| {
        AdapterError::schema(
            NAME,
            format!("{path_display}:{first_line}"),
            "session has no parseable timestamp",
        )
    })?;
    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": "claude-code",
            "version": version,
            "workspace_path": project,
        }),
    );

    Ok(Session {
        id,
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at,
        project,
        options,
    })
}

fn events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
) -> Result<Vec<IngestEvent>, String> {
    let timestamp = parse_timestamp(row).unwrap_or(default_timestamp);
    let uuid = row
        .get("uuid")
        .and_then(Value::as_str)
        .map_or_else(|| format!("{session_id}:{line}"), ToOwned::to_owned);

    if let Some(message_value) = row.get("message") {
        return message_events(session_id, &uuid, timestamp, row, message_value);
    }

    let kind = row.get("type").and_then(Value::as_str).unwrap_or("system");
    let content = if kind == "attachment" {
        row.get("attachment")
            .and_then(attachment_content)
            .unwrap_or_else(|| compact_json(row))
    } else {
        row.get("subtype")
            .and_then(Value::as_str)
            .unwrap_or(kind)
            .to_owned()
    };
    let message = Message::System {
        id: uuid,
        session_id: session_id.to_owned(),
        timestamp,
        content,
        options: row_options(row),
    };
    Ok(vec![IngestEvent::Message(message)])
}

fn message_events(
    session_id: &str,
    uuid: &str,
    timestamp: DateTime<Utc>,
    row: &Value,
    message_value: &Value,
) -> Result<Vec<IngestEvent>, String> {
    let role = message_value
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| "message missing role".to_owned())?;
    let content = message_value.get("content").unwrap_or(&Value::Null);
    let mut parts = Vec::new();
    let message = match (role, content) {
        ("user", Value::String(text)) => {
            parts.push(text_part(uuid, 0, text));
            Message::User {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row),
            }
        }
        ("user", Value::Array(items)) if items.iter().all(is_tool_result) => {
            let source_tool_result = row.get("toolUseResult").cloned();
            parts.extend(items.iter().enumerate().map(|(ordinal, item)| {
                tool_result_part(uuid, ordinal, item, source_tool_result.as_ref())
            }));
            Message::Tool {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row),
            }
        }
        ("user", Value::Array(items)) => {
            parts.extend(
                items
                    .iter()
                    .enumerate()
                    .map(|(ordinal, item)| user_part(uuid, ordinal, item)),
            );
            Message::User {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row),
            }
        }
        ("assistant", Value::Array(items)) => {
            parts.extend(
                items
                    .iter()
                    .enumerate()
                    .map(|(ordinal, item)| assistant_part(uuid, ordinal, item)),
            );
            Message::Assistant {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: assistant_options(row, message_value),
            }
        }
        ("system", Value::String(text)) => Message::System {
            id: uuid.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            content: text.clone(),
            options: row_options(row),
        },
        ("system", _) => Message::System {
            id: uuid.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            content: compact_json(message_value),
            options: row_options(row),
        },
        (other, _) => {
            return Err(format!("unsupported message role {other}"));
        }
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

fn text_part(message_id: &str, ordinal: usize, text: &str) -> Part {
    Part {
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
        options: empty_options(),
        kind: PartKind::Text {
            text: text.to_owned(),
        },
    }
}

fn user_part(message_id: &str, ordinal: usize, value: &Value) -> Part {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(
            message_id,
            ordinal,
            value
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        ),
        Some("image") | Some("file") => file_part(message_id, ordinal, value),
        Some("tool_result") => tool_result_part(message_id, ordinal, value, None),
        _ => text_part(message_id, ordinal, &compact_json(value)),
    }
}

fn assistant_part(message_id: &str, ordinal: usize, value: &Value) -> Part {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(
            message_id,
            ordinal,
            value
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        ),
        Some("thinking") => Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: signature_options(value),
            kind: PartKind::Reasoning {
                text: value
                    .get("thinking")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            },
        },
        Some("tool_use") => Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: value
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                name: value
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
                params: value.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: false,
            },
        },
        Some("server_tool_use") => Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: value
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                name: value
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("server_tool")
                    .to_owned(),
                params: value.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: true,
            },
        },
        Some("image") | Some("file") => file_part(message_id, ordinal, value),
        _ => text_part(message_id, ordinal, &compact_json(value)),
    }
}

fn tool_result_part(
    message_id: &str,
    ordinal: usize,
    value: &Value,
    source_tool_result: Option<&Value>,
) -> Part {
    let name = source_tool_result
        .unwrap_or(&Value::Null)
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let result = value
        .get("content")
        .cloned()
        .or_else(|| source_tool_result.cloned())
        .unwrap_or(Value::Null);
    Part {
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id: value
                .get("tool_use_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            name,
            is_failure: value
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            result,
        },
    }
}

fn file_part(message_id: &str, ordinal: usize, value: &Value) -> Part {
    let media_type = value
        .get("media_type")
        .or_else(|| value.get("mime_type"))
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream")
        .to_owned();
    let file_name = value
        .get("file_name")
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let data = if let Some(source) = value.get("source") {
        if let Some(url) = source.get("url").and_then(Value::as_str) {
            FileData::Url(url.to_owned())
        } else if let Some(bytes) = source.get("data").and_then(Value::as_str) {
            FileData::String(bytes.to_owned())
        } else {
            FileData::String(compact_json(source))
        }
    } else if let Some(url) = value.get("url").and_then(Value::as_str) {
        FileData::Url(url.to_owned())
    } else {
        FileData::String(compact_json(value))
    };

    Part {
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
        options: empty_options(),
        kind: PartKind::File {
            media_type,
            file_name,
            data,
        },
    }
}

fn row_options(row: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    let source = json!({
        "parent_uuid": row.get("parentUuid"),
        "is_sidechain": row.get("isSidechain"),
        "user_type": row.get("userType"),
        "entrypoint": row.get("entrypoint"),
        "cwd": row.get("cwd"),
        "version": row.get("version"),
        "git_branch": row.get("gitBranch"),
        "request_id": row.get("requestId"),
        "raw_type": row.get("type"),
    });
    options.insert("source".to_owned(), source);
    options
}

fn assistant_options(row: &Value, message_value: &Value) -> ProviderOptions {
    let mut options = row_options(row);
    let anthropic = json!({
        "id": message_value.get("id"),
        "model": message_value.get("model"),
        "stop_reason": message_value.get("stop_reason"),
        "stop_sequence": message_value.get("stop_sequence"),
        "usage": message_value.get("usage"),
    });
    options.insert("anthropic".to_owned(), anthropic);
    options
}

fn signature_options(value: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    if let Some(signature) = value.get("signature").and_then(Value::as_str) {
        options.insert("anthropic".to_owned(), json!({"signature": signature}));
    }
    options
}

fn attachment_content(value: &Value) -> Option<String> {
    value
        .get("content")
        .or_else(|| value.get("stdout"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn parse_timestamp(value: &Value) -> anyhow::Result<DateTime<Utc>> {
    let timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .context("missing timestamp")?;
    Ok(DateTime::parse_from_rfc3339(timestamp)
        .context("invalid timestamp")?
        .with_timezone(&Utc))
}

fn is_tool_result(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("tool_result")
}
