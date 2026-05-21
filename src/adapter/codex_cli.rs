//! OpenAI Codex CLI adapter.
//!
//! Source path: `~/.codex/sessions/<year>/<month>/<day>/rollout-<ts>-<uuid>.jsonl`.
//! Each line is an envelope `{timestamp, type, payload}`. Top-level types:
//! `session_meta` (consumed up front for Session), `event_msg` /
//! `turn_context` (transport noise, skipped), `response_item` (the per-turn
//! model interaction: subtypes `message`, `reasoning`, `function_call`,
//! `function_call_output`, `custom_tool_call`).
//!
//! Pre-Oct-2025 legacy rollouts (spec.md#adapters) predate the envelope: the
//! first row is a bare metadata object and each data row is an un-enveloped
//! payload, interleaved with `{record_type:"state"}` noise. The adapter
//! accepts both shapes.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use async_stream::stream;
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::{
    sessions::IngestEvent,
    wire::{Message, Part, PartKind, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, by_timestamp_then_id,
    collect_jsonl_files, compact_json, config_path, empty_options,
    extract::{Extracted, extract_compact_repr, extract_self_str, extract_str},
    extracted_text, jsonl_bytes, part_id, raw_record,
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
        Ok(Box::new(CodexCliAdapter::new(config_path(NAME, config)?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = env.home.join(".codex").join("sessions");
        path.exists().then(|| json!({ "path": path }))
    }

    fn serialize(
        &self,
        session: &crate::sessions::SessionWithMessages,
        fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        serialize_session(session, fidelity)
    }
}

fn serialize_session(
    session: &crate::sessions::SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    // Native replays verbatim `options.source.raw_record` rows (session_meta,
    // then one per message); `codex_session_meta` / `codex_response_item` below
    // are foreign-only. Replay echoes a frozen snapshot - safe only while
    // canonical is append-only (spec.md#additive-sync).
    let mut records = Vec::new();
    if fidelity == RestoreFidelity::Native
        && let Some(raw) = raw_record(&session.session.options)
    {
        records.push(raw);
    } else {
        records.push(codex_session_meta(session));
    }
    let mut messages = session.messages.clone();
    messages.sort_by(by_timestamp_then_id);
    for message in &messages {
        if fidelity == RestoreFidelity::Native
            && let Some(raw) = raw_record(message.message.options())
        {
            records.push(raw);
            continue;
        }
        // Foreign restore: a System message (a rule-3 carrier, or a source's
        // own system/developer turn) has no idiomatic home in another
        // client's transcript - drop it; the content stays in canonical
        // (spec.md#native-restore-lossless, foreign clause).
        if matches!(message.message, Message::System { .. }) {
            continue;
        }
        records.push(codex_response_item(message));
    }
    Ok(vec![RestoredFile {
        relative_path: codex_relative_path(session),
        bytes: jsonl_bytes(NAME, &records)?,
    }])
}

fn codex_relative_path(session: &crate::sessions::SessionWithMessages) -> PathBuf {
    let ts = session.session.created_at;
    let filename_ts = ts.format("%Y-%m-%dT%H-%M-%S");
    PathBuf::from("sessions")
        .join(format!("{:04}", ts.year()))
        .join(format!("{:02}", ts.month()))
        .join(format!("{:02}", ts.day()))
        .join(format!(
            "rollout-{filename_ts}-{}.jsonl",
            session.session.id
        ))
}

fn codex_session_meta(session: &crate::sessions::SessionWithMessages) -> Value {
    json!({
        "timestamp": session.session.created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "type": "session_meta",
        "payload": {
            "id": session.session.id,
            "timestamp": session.session.created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            "cwd": &*session.session.project,
        }
    })
}

fn codex_response_item(message: &crate::sessions::MessageWithParts) -> Value {
    json!({
        "timestamp": message.message.timestamp().to_rfc3339_opts(SecondsFormat::Millis, true),
        "type": "response_item",
        "payload": codex_payload(message),
    })
}

fn codex_payload(message: &crate::sessions::MessageWithParts) -> Value {
    if let Some(part) = message.parts.first() {
        match &part.kind {
            PartKind::ToolCall {
                call_id,
                name,
                params,
                ..
            } if matches!(message.message, Message::Assistant { .. }) => {
                return json!({
                    "type": "function_call",
                    "call_id": extracted_text(call_id),
                    "name": extracted_text(name),
                    "arguments": compact_json(params),
                });
            }
            PartKind::ToolResult {
                call_id, result, ..
            } if matches!(message.message, Message::Tool { .. }) => {
                return json!({
                    "type": "function_call_output",
                    "call_id": extracted_text(call_id),
                    "output": result,
                });
            }
            PartKind::Reasoning { text }
                if matches!(message.message, Message::Assistant { .. }) =>
            {
                if let Some(text) = text
                    && let Ok(value) = serde_json::from_str::<Value>(text.as_ref())
                {
                    return value;
                }
                return json!({
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": extracted_text(text)}],
                });
            }
            _ => {}
        }
    }
    let is_assistant = matches!(message.message, Message::Assistant { .. });
    json!({
        "type": "message",
        "role": match message.message.role() {
            crate::wire::Role::System => "developer",
            crate::wire::Role::User => "user",
            crate::wire::Role::Assistant => "assistant",
            crate::wire::Role::Tool => "tool",
        },
        "content": message
            .parts
            .iter()
            .map(|part| codex_content_part(part, is_assistant))
            .collect::<Vec<_>>(),
    })
}

fn codex_content_part(part: &Part, is_assistant: bool) -> Value {
    // Codex tags an assistant turn's content `output_text` and a user or
    // developer turn's content `input_text` - the discriminator is the
    // owning message's role, not the part.
    let text_type = if is_assistant {
        "output_text"
    } else {
        "input_text"
    };
    match &part.kind {
        PartKind::Text { text } => json!({
            "type": text_type,
            "text": extracted_text(text),
        }),
        PartKind::File { data, .. } => json!({
            "type": text_type,
            "text": match data {
                crate::wire::FileData::String(value) => value.clone(),
                crate::wire::FileData::Bytes(value) => format!("<{} bytes>", value.len()),
                crate::wire::FileData::Url(value) => value.clone(),
            },
        }),
        other => json!({
            "type": text_type,
            "text": compact_json(&serde_json::to_value(other).unwrap_or(Value::Null)),
        }),
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

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
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

                if let Some((id, mtime)) = peek_id_and_mtime(&path).await
                    && let Some(ingested_at) = oracle.last_ingested_at(&id)
                    && mtime <= ingested_at
                {
                    yield Ok(AdapterYield::Skipped {
                        session_id: id,
                        project: None,
                        reason: SkipReason::Fresh,
                    });
                    continue;
                }

                let meta = match session_meta(&path, &path_display).await {
                    Ok(meta) => meta,
                    Err(error) => {
                        yield Err(error);
                        continue;
                    }
                };
                let session_id = meta.id.clone();
                let default_timestamp = meta.created_at;
                yield Ok(AdapterYield::Event(IngestEvent::Session(meta)));

                let file = match tokio::fs::File::open(&path).await {
                    Ok(file) => file,
                    Err(source) => {
                        yield Err(AdapterError::io(NAME, path_display.clone(), source));
                        continue;
                    }
                };

                let mut lines = BufReader::new(file).lines();
                let mut line_number = 0usize;
                let mut tool_call_names: HashMap<String, Extracted<String>> = HashMap::new();
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
                            yield Err(AdapterError::parse(
                                NAME,
                                path_display.clone(),
                                line_number,
                                source,
                            ));
                            continue;
                        }
                    };
                    capture_tool_call_name(&row, &mut tool_call_names);
                    match events_from_row(
                        &session_id,
                        line_number,
                        &row,
                        default_timestamp,
                        &tool_call_names,
                    ) {
                        Ok(events) => {
                            for event in events {
                                yield Ok(AdapterYield::Event(event));
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

/// True for a pre-Oct-2025 legacy rollout's bare first row: session metadata
/// (`id`/`timestamp`/`git`/`instructions`) at the top level with no `type`
/// envelope. spec.md#adapters: legacy rollouts predate the `session_meta`
/// wrapper, so the first row IS the payload.
fn is_legacy_session_row(row: &Value) -> bool {
    row.get("type").is_none() && row.get("id").is_some()
}

async fn peek_id_and_mtime(path: &Path) -> Option<(String, DateTime<Utc>)> {
    let metadata = tokio::fs::metadata(path).await.ok()?;
    let mtime = DateTime::<Utc>::from(metadata.modified().ok()?);
    let file = tokio::fs::File::open(path).await.ok()?;
    let mut lines = BufReader::new(file).lines();
    let line = lines.next_line().await.ok()??;
    let row: Value = serde_json::from_str(&line).ok()?;
    // Recognizing the legacy first row lets legacy files freshness-skip like
    // any other once ingested, instead of re-parsing (and re-failing) every
    // sync.
    let id = if row.get("type").and_then(Value::as_str) == Some("session_meta") {
        row.get("payload")?.get("id")?.as_str()?.to_owned()
    } else if is_legacy_session_row(&row) {
        row.get("id")?.as_str()?.to_owned()
    } else {
        return None;
    };
    Some((id, mtime))
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
    // The current rollout wraps session metadata in a `session_meta`
    // envelope; a legacy rollout (spec.md#adapters) has none - the first row
    // is a bare metadata object. Either way, read fields from `payload`.
    let payload = if row.get("type").and_then(Value::as_str) == Some("session_meta") {
        row.get("payload").cloned().unwrap_or(Value::Null)
    } else if is_legacy_session_row(&row) {
        row.clone()
    } else {
        return Err(AdapterError::schema(
            NAME,
            format!("{path_display}:1"),
            "first row must be session_meta",
        ));
    };
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
    let project = match extract_str(&payload, "cwd") {
        Some(value) => value,
        None => {
            let path_str = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path_display)
                .to_owned();
            extract_self_str(&Value::String(path_str)).ok_or_else(|| {
                AdapterError::schema(
                    NAME,
                    path_display.to_owned(),
                    "internal: Value::String produced None from Source::as_str",
                )
            })?
        }
    };
    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": "codex-cli",
            "originator": payload.get("originator"),
            "cli_version": payload.get("cli_version"),
            "model_provider": payload.get("model_provider"),
            "git": payload.get("git"),
            "base_instructions": payload.get("base_instructions"),
            "instructions": payload.get("instructions"),
            "source": payload.get("source"),
            "raw_record": row,
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
/// `turn_context` are transport noise. Legacy rows (spec.md#adapters) carry
/// the same payload shapes un-enveloped; `{record_type:"state"}` markers and
/// the bare first row are eventless.
fn events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Result<Vec<IngestEvent>, String> {
    let kind = row.get("type").and_then(Value::as_str);
    // Eventless rows: `session_meta` (current) and the legacy bare first row
    // are both consumed up front by session_meta(); legacy
    // `{record_type:"state"}` markers are transport noise (spec.md#adapters).
    if kind == Some("session_meta")
        || is_legacy_session_row(row)
        || (kind.is_none() && row.get("record_type").is_some())
    {
        return Ok(Vec::new());
    }
    // Normalize to the per-turn payload. A current row wraps it in a
    // `response_item` envelope carrying its own timestamp; a legacy data row
    // IS the payload (spec.md#adapters) and inherits the session timestamp.
    let (payload, timestamp) = if kind == Some("response_item") {
        let timestamp = row
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(default_timestamp);
        (row.get("payload").unwrap_or(&Value::Null), timestamp)
    } else {
        (row, default_timestamp)
    };
    let payload_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
    let message_id = format!("{session_id}:{line:06}");

    match payload_type {
        "message" => message_events(session_id, &message_id, timestamp, payload, row),
        "function_call" => Ok(tool_call_events(
            session_id,
            &message_id,
            timestamp,
            payload,
            row,
        )),
        "function_call_output" => Ok(tool_result_events(
            session_id,
            &message_id,
            timestamp,
            payload,
            row,
            tool_call_names,
        )),
        "reasoning" => Ok(reasoning_events(
            session_id,
            &message_id,
            timestamp,
            payload,
            row,
        )),
        "custom_tool_call" => Ok(custom_tool_call_events(
            session_id,
            &message_id,
            timestamp,
            payload,
            row,
        )),
        "custom_tool_call_output" => Ok(custom_tool_result_events(
            session_id,
            &message_id,
            timestamp,
            payload,
            row,
        )),
        _ => Ok(vec![raw_carrier_event(session_id, line, row, timestamp)]),
    }
}

fn row_options(row: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert("source".to_owned(), json!({ "raw_record": row }));
    options
}

fn raw_carrier_event(
    session_id: &str,
    line: usize,
    row: &Value,
    timestamp: DateTime<Utc>,
) -> IngestEvent {
    IngestEvent::Message(Message::System {
        id: row
            .get("id")
            .and_then(Value::as_str)
            .map_or_else(|| format!("{session_id}:{line:06}:raw"), ToOwned::to_owned),
        session_id: session_id.to_owned(),
        timestamp: row
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(timestamp),
        content: None,
        options: row_options(row),
    })
}

/// Stash one row's `function_call` (call_id -> name) into the per-file
/// map so the matching `function_call_output` row downstream can resolve
/// the tool name rather than fall back to a sentinel.
fn capture_tool_call_name(row: &Value, map: &mut HashMap<String, Extracted<String>>) {
    // Mirror events_from_row's payload normalization: a current row wraps the
    // payload under `response_item`, a legacy row IS the payload.
    let payload = match row.get("type").and_then(Value::as_str) {
        Some("response_item") => row.get("payload"),
        Some(_) => Some(row),
        None => None,
    };
    let Some(payload) = payload else {
        return;
    };
    if payload.get("type").and_then(Value::as_str) != Some("function_call") {
        return;
    }
    let Some(call_id) = payload.get("call_id").and_then(Value::as_str) else {
        return;
    };
    let Some(name) = extract_str(payload, "name") else {
        return;
    };
    map.insert(call_id.to_owned(), name);
}

fn message_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    row: &Value,
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
        // Faithful encoding of one content item: prefer the raw `text`
        // field when present; otherwise compact-encode the structured
        // body as a JSON string. The fallback is lossless (preserves the
        // item bytes) and explicit (not a synthesised "unknown" or "").
        let text = extract_str(item, "text").or_else(|| Some(extract_compact_repr(item)));
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
                options: row_options(row),
            },
            true,
        ),
        "assistant" => (
            Message::Assistant {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row),
            },
            true,
        ),
        // `developer` rows are codex-cli's system-prompt frames; map to System
        // with `content: None` and let the inner Text Parts carry the
        // body. The previous join-aggregation hack double-stored data and
        // can't reconstruct an `Extracted<String>` from synthesised text
        // under the new adapter seam; dropping it is a simplification.
        "developer" | "system" => (
            Message::System {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                content: None,
                options: row_options(row),
            },
            true,
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
    row: &Value,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
    let name = extract_str(payload, "name");
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
            options: row_options(row),
        }),
        IngestEvent::Part(part),
    ]
}

fn custom_tool_call_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    row: &Value,
) -> Vec<IngestEvent> {
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::ToolCall {
            call_id: extract_str(payload, "call_id"),
            name: extract_str(payload, "name"),
            params: payload.get("input").cloned().unwrap_or(Value::Null),
            provider_executed: true,
        },
    };
    vec![
        IngestEvent::Message(Message::Assistant {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: row_options(row),
        }),
        IngestEvent::Part(part),
    ]
}

fn custom_tool_result_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    row: &Value,
) -> Vec<IngestEvent> {
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id: extract_str(payload, "call_id"),
            name: extract_str(payload, "name"),
            is_failure: false,
            result: cap_tool_output(payload.get("output").cloned().unwrap_or(Value::Null)),
        },
    };
    vec![
        IngestEvent::Message(Message::Tool {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: row_options(row),
        }),
        IngestEvent::Part(part),
    ]
}

/// Stopgap guard against pathological codex-cli `function_call_output.output`
/// values - a 0.121.0 sandbox-denial path was observed dumping ~4GB of
/// captured stdout into a single JSON string field, which then overflows the
/// i32 offset accumulator in `parts.variant_data` (StringArray). Cap at 10MB
/// and replace with a sentinel that preserves the head bytes plus the
/// original size. See `plan.md` Stage 6 for the proper storage-seam fix.
const MAX_TOOL_OUTPUT_BYTES: usize = 10 * 1024 * 1024;
const TOOL_OUTPUT_HEAD_BYTES: usize = 2048;

fn cap_tool_output(value: Value) -> Value {
    if let Value::String(s) = &value
        && s.len() > MAX_TOOL_OUTPUT_BYTES
    {
        let original = s.len();
        let head_end = s
            .char_indices()
            .take_while(|(i, _)| *i <= TOOL_OUTPUT_HEAD_BYTES)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        let head = &s[..head_end];
        tracing::warn!(
            adapter = NAME,
            original_bytes = original,
            cap_bytes = MAX_TOOL_OUTPUT_BYTES,
            "function_call_output.output exceeded cap; truncated to sentinel"
        );
        return json!({
            "__pond_truncated": true,
            "original_bytes": original,
            "head": head,
        });
    }
    value
}

fn tool_result_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    row: &Value,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
    // Resolve tool name from the prior `function_call` row via the
    // per-file `call_id -> name` map. Misses (e.g. compaction pruned the
    // originating call) yield `None`, a faithful "unresolved" rather than
    // the previous `"function"` sentinel.
    let name = call_id
        .as_ref()
        .and_then(|id| tool_call_names.get(id.as_str()))
        .cloned();
    let result = cap_tool_output(payload.get("output").cloned().unwrap_or(Value::Null));
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id,
            name,
            is_failure: false,
            result,
        },
    };
    vec![
        IngestEvent::Message(Message::Tool {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: row_options(row),
        }),
        IngestEvent::Part(part),
    ]
}

fn reasoning_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    row: &Value,
) -> Vec<IngestEvent> {
    // The source `summary` array is the only place reasoning text lives in
    // codex-cli's format. Empty array (or missing field) -> `None`. Joined
    // text -> `Some(...)`. Don't synthesize an empty string.
    let summary = payload
        .get("summary")
        .and_then(Value::as_array)
        .and_then(|items| {
            let joined = items
                .iter()
                .filter_map(|item| extract_str(item, "text"))
                .map(|e| (*e).clone())
                .collect::<Vec<_>>()
                .join("\n");
            if joined.is_empty() {
                None
            } else {
                Some(extract_compact_repr(payload))
            }
        });
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
            options: row_options(row),
        }),
        IngestEvent::Part(part),
    ]
}

#[cfg(test)]
mod tests {
    //! End-to-end test for the codex-cli adapter: ingest the committed fixture
    //! corpus and assert pond's canonical Session/Message/Part shape comes out
    //! the other side. The fixture lives under
    //! `tests/fixtures/adapter/codex_cli/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::PartKind};
    use tempfile::TempDir;

    const FIXTURES: &str = "tests/fixtures/adapter/codex_cli/sessions";

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_fixture_corpus() -> anyhow::Result<()> {
        let adapter = CodexCliAdapter::new(FIXTURES);
        crate::adapter::test_support::assert_native_restore(
            &CodexCliFactory,
            &adapter,
            // Codex rollout paths embed the `sessions/` segment, so the corpus
            // root is FIXTURES' parent, not FIXTURES itself.
            std::path::Path::new(FIXTURES)
                .parent()
                .expect("FIXTURES is nested under a corpus root"),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn codex_cli_adapter_ingests_fixture_corpus_into_canonical_shape() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = CodexCliAdapter::new(FIXTURES);

        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert!(summary.accepted() > 0, "ingest must accept rows");
        assert_eq!(summary.dropped_events, 0, "no per-event drops expected");
        assert_eq!(
            summary.dropped_sessions, 0,
            "no session-level rejections expected"
        );
        assert_eq!(summary.skipped_files, 0, "no whole-file skips expected");

        let (sessions, messages, parts, _) = store.row_counts().await?;
        assert!(sessions > 0, "at least one codex-cli session");
        assert!(messages > 0, "at least one codex-cli message");
        assert!(parts > 0, "at least one codex-cli Part");

        let mut saw_text_part = false;
        for session_id in store.session_ids().await? {
            let session = store
                .get_session(&session_id)
                .await?
                .expect("session round-trips");
            assert_eq!(session.session.source_agent, "codex-cli");
            assert!(
                !session.messages.is_empty(),
                "session {session_id} must carry messages",
            );
            for stored in &session.messages {
                for part in &stored.parts {
                    if matches!(part.kind, PartKind::Text { .. }) {
                        saw_text_part = true;
                    }
                }
            }
        }
        assert!(
            saw_text_part,
            "codex-cli corpus must contain at least one Text Part",
        );
        Ok(())
    }

    #[test]
    fn legacy_rows_normalize_to_payloads() {
        let ts = Utc::now();
        let map: HashMap<String, Extracted<String>> = HashMap::new();

        // The bare first row and `{record_type:"state"}` markers are eventless.
        let first = json!({"id": "s1", "timestamp": "2025-09-13T04:30:17.447Z"});
        let state = json!({"record_type": "state"});
        assert!(
            events_from_row("s1", 1, &first, ts, &map)
                .expect("legacy first row parses")
                .is_empty(),
        );
        assert!(
            events_from_row("s1", 2, &state, ts, &map)
                .expect("state marker parses")
                .is_empty(),
        );

        // An un-enveloped legacy `message` row yields a Message + Text Part.
        let message = json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hi"}],
        });
        let events = events_from_row("s1", 3, &message, ts, &map).expect("legacy message parses");
        assert_eq!(events.len(), 2, "message + one Text Part");
        assert!(matches!(
            events[0],
            IngestEvent::Message(Message::User { .. })
        ));
        assert!(matches!(
            &events[1],
            IngestEvent::Part(part) if matches!(part.kind, PartKind::Text { .. }),
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_rollout_ingests_into_canonical_shape() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = CodexCliAdapter::new(FIXTURES);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        // The legacy fixture's bare first row -> Session: id and timestamp
        // read from the top level, with no `session_meta` envelope.
        let session = store
            .get_session("67c52f3f-d25e-4194-a006-93de58f28d7c")
            .await?
            .expect("legacy rollout ingests as a session");
        assert_eq!(session.session.source_agent, "codex-cli");
        assert_eq!(
            session
                .session
                .created_at
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            "2025-09-13T04:30:17.447Z",
        );
        // Eleven un-enveloped data rows -> eleven messages.
        assert_eq!(session.messages.len(), 11, "every legacy data row ingests");
        // The legacy `function_call_output` resolves its tool name from the
        // prior legacy `function_call` row via the per-file call_id map.
        let resolved = session.messages.iter().any(|message| {
            message
                .parts
                .iter()
                .any(|part| matches!(&part.kind, PartKind::ToolResult { name: Some(_), .. }))
        });
        assert!(
            resolved,
            "legacy function_call_output resolves its tool name"
        );
        Ok(())
    }
}
