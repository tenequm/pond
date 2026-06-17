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

use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use serde_json::{Value, json};

use crate::{
    sessions::IngestEvent,
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, by_timestamp_then_id, compact_json, config_path,
    empty_options,
    extract::{Extracted, extract_compact_repr, extract_raw_record, extract_self_str, extract_str},
    extracted_text,
    jsonl::{BoundedRow, JsonlTree, jsonl_tree_discover, jsonl_tree_events},
    jsonl_bytes, part_id, part_ordinal, raw_record,
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
    // canonical is append-only (spec.md#adapter-integrity-additive-sync).
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
        // (spec.md#adapter-native-restore-lossless, foreign clause).
        if matches!(message.message, Message::System { .. }) {
            continue;
        }
        records.push(codex_response_item(message));
    }
    Ok(vec![RestoredFile::new(
        codex_relative_path(session),
        jsonl_bytes(NAME, &records)?,
        fidelity,
    )])
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
        jsonl_tree_discover(self)
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        jsonl_tree_events(self, oracle)
    }
}

impl JsonlTree for CodexCliAdapter {
    type State = HashMap<String, Extracted<String>>;

    fn name(&self) -> &'static str {
        NAME
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn peek_session_id(&self, _path: &Path, first_line: &str) -> Option<String> {
        let row: Value = serde_json::from_str(first_line).ok()?;
        if row.get("type").and_then(Value::as_str) == Some("session_meta") {
            row.get("payload")?
                .get("id")?
                .as_str()
                .map(ToOwned::to_owned)
        } else if is_legacy_session_row(&row) {
            row.get("id")?.as_str().map(ToOwned::to_owned)
        } else {
            None
        }
    }

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
        session_from_rows(path, rows)
    }

    fn events_from_row(
        &self,
        session: &Session,
        row: &BoundedRow,
        state: &mut Self::State,
    ) -> Result<Vec<IngestEvent>, String> {
        capture_tool_call_name(&row.value, state);
        events_from_row(&session.id, row.line, &row.value, session.created_at, state)
    }
}

/// True for a pre-Oct-2025 legacy rollout's bare first row: session metadata
/// (`id`/`timestamp`/`git`/`instructions`) at the top level with no `type`
/// envelope. spec.md#adapters: legacy rollouts predate the `session_meta`
/// wrapper, so the first row IS the payload.
fn is_legacy_session_row(row: &Value) -> bool {
    row.get("type").is_none() && row.get("id").is_some()
}

fn session_from_rows(path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
    let path_display = path.display().to_string();
    let first = rows
        .first()
        .ok_or_else(|| AdapterError::schema(NAME, path_display.clone(), "empty jsonl session"))?;
    let row = &first.value;
    let at_first = format!("{path_display}:{}", first.line);
    // The current rollout wraps session metadata in a `session_meta` envelope;
    // a legacy rollout (spec.md#adapters) has none - the first row is a bare
    // metadata object. Either way, read fields from `payload`.
    let payload = if row.get("type").and_then(Value::as_str) == Some("session_meta") {
        row.get("payload").cloned().unwrap_or(Value::Null)
    } else if is_legacy_session_row(row) {
        row.clone()
    } else {
        return Err(AdapterError::schema(
            NAME,
            at_first,
            "first row must be session_meta",
        ));
    };
    let id = payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(NAME, at_first.clone(), "session_meta missing payload.id")
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
            AdapterError::schema(NAME, at_first, "session_meta has no parseable timestamp")
        })?;
    let project = match extract_str(&payload, "cwd") {
        Some(value) => value,
        None => {
            let path_str = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path_display.as_str())
                .to_owned();
            extract_self_str(&Value::String(path_str)).ok_or_else(|| {
                AdapterError::schema(
                    NAME,
                    path_display.clone(),
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
            "raw_record": extract_raw_record(row),
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
    options.insert(
        "source".to_owned(),
        json!({ "raw_record": extract_raw_record(row) }),
    );
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
    let Some(content) = payload.get("content").and_then(Value::as_array) else {
        return Ok(vec![message_raw_carrier_event(
            session_id, message_id, row, timestamp,
        )]);
    };
    // spec.md#model-part-provenance: a `developer` record is a harness instruction
    // block; a `user`-slot record whose body is `<environment_context>` or an
    // `# AGENTS.md instructions` blob is injected context, not a genuine
    // prompt. Everything else in a message record is conversation.
    let provenance = message_provenance(role, content);
    let mut parts = Vec::with_capacity(content.len());
    for (ordinal, item) in content.iter().enumerate() {
        // Faithful encoding of one content item: prefer the raw `text`
        // field when present; otherwise compact-encode the structured
        // body as a JSON string. The fallback is lossless (preserves the
        // item bytes) and explicit (not a synthesised "unknown" or "").
        let text = extract_str(item, "text").or_else(|| Some(extract_compact_repr(item)));
        parts.push(Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance,
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
        // with `content: None` and let the inner Text Parts carry the body.
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
        _ => {
            return Ok(vec![message_raw_carrier_event(
                session_id, message_id, row, timestamp,
            )]);
        }
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    if keep_parts {
        events.extend(parts.into_iter().map(IngestEvent::Part));
    }
    Ok(events)
}

fn message_raw_carrier_event(
    session_id: &str,
    message_id: &str,
    row: &Value,
    timestamp: DateTime<Utc>,
) -> IngestEvent {
    IngestEvent::Message(Message::System {
        id: message_id.to_owned(),
        session_id: session_id.to_owned(),
        timestamp,
        content: row
            .get("payload")
            .and_then(|payload| payload.get("role"))
            .or_else(|| row.get("role"))
            .and_then(Value::as_str)
            .and_then(|role| extract_self_str(&Value::String(role.to_owned()))),
        options: row_options(row),
    })
}

/// Provenance of a codex `message` record (spec.md#model-part-provenance). A
/// `developer` record is a harness instruction block; a `user`-slot record
/// whose only content is `<environment_context>` or `# AGENTS.md instructions`
/// is injected context rather than a typed prompt. v1 codex never interleaves
/// authored and injected content within one record.
fn message_provenance(role: &str, content: &[Value]) -> Provenance {
    if role == "developer" || role == "system" {
        return Provenance::Injected;
    }
    if role == "user" {
        let injected = content.iter().any(|item| {
            item.get("text")
                .and_then(Value::as_str)
                .is_some_and(is_injected_user_text)
        });
        if injected {
            return Provenance::Injected;
        }
    }
    Provenance::Conversational
}

/// Harness-injected user-slot content codex emits as a non-prompt record.
fn is_injected_user_text(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("<environment_context>")
        || trimmed.starts_with("<user_instructions>")
        || trimmed.starts_with("# AGENTS.md")
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
        session_id: session_id.to_owned(),
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        // spec.md#model-part-provenance: the model authored the tool call.
        provenance: Provenance::Conversational,
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
        session_id: session_id.to_owned(),
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        // spec.md#model-part-provenance: the model authored the tool call.
        provenance: Provenance::Conversational,
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
        session_id: session_id.to_owned(),
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        // spec.md#model-part-provenance: tool output is runtime-produced.
        provenance: Provenance::Injected,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id: extract_str(payload, "call_id"),
            name: extract_str(payload, "name"),
            is_failure: false,
            result: payload.get("output").cloned().unwrap_or(Value::Null),
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

fn tool_result_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    row: &Value,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
    // Resolve tool name from the earlier `function_call` row via the
    // per-file `call_id -> name` map. Misses (e.g. compaction pruned the
    // originating call) yield `None`, a faithful "unresolved" value.
    let name = call_id
        .as_ref()
        .and_then(|id| tool_call_names.get(id.as_str()))
        .cloned();
    let result = payload.get("output").cloned().unwrap_or(Value::Null);
    let part = Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        // spec.md#model-part-provenance: tool output is runtime-produced.
        provenance: Provenance::Injected,
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
        session_id: session_id.to_owned(),
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        // spec.md#model-part-provenance: model-authored reasoning.
        provenance: Provenance::Conversational,
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

    // Manifest-dir anchored: unit tests must not depend on the process cwd
    // (figment::Jail chdirs the whole test process while config tests run).
    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/codex_cli/sessions"
    );

    #[test]
    fn probe_default_finds_codex_sessions_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &CodexCliFactory,
            &[".codex", "sessions"],
        )
    }

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

        let (sessions, messages, parts) = store.row_counts().await?;
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

    /// spec.md#model-part-provenance: a `developer` record and a `user`-slot record
    /// whose body is `<environment_context>` are harness-injected; a genuine
    /// user prompt and an assistant message are conversation.
    #[test]
    fn message_provenance_separates_prompts_from_harness_records() {
        let prompt = vec![json!({"type": "input_text", "text": "refactor this"})];
        assert_eq!(
            message_provenance("user", &prompt),
            Provenance::Conversational,
        );
        assert_eq!(
            message_provenance("assistant", &[]),
            Provenance::Conversational,
        );

        let developer = vec![json!({"type": "input_text", "text": "you are an agent"})];
        assert_eq!(
            message_provenance("developer", &developer),
            Provenance::Injected,
        );

        let env = vec![json!({
            "type": "input_text",
            "text": "<environment_context>cwd=/tmp</environment_context>",
        })];
        assert_eq!(message_provenance("user", &env), Provenance::Injected);
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

    #[test]
    fn unknown_message_role_becomes_lossless_carrier() {
        let ts = Utc::now();
        let map: HashMap<String, Extracted<String>> = HashMap::new();
        let row = json!({
            "type": "response_item",
            "timestamp": "2026-06-01T00:00:00Z",
            "payload": {
                "type": "message",
                "role": "future_role",
                "content": [{"type": "input_text", "text": "keep me"}],
            },
        });

        let events = events_from_row("s1", 4, &row, ts, &map).expect("carrier is valid");
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            IngestEvent::Message(Message::System { id, content, .. })
                if id == "s1:000004" && content.as_deref().map(String::as_str) == Some("future_role")
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
