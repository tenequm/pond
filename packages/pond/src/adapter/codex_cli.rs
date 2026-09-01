//! OpenAI Codex CLI adapter.
//!
//! Source path: `~/.codex/sessions/<year>/<month>/<day>/rollout-<ts>-<uuid>.jsonl`.
//! Each line is an envelope `{timestamp, type, payload}`. Top-level types:
//! `session_meta` (consumed up front for Session), `response_item` (the
//! per-turn model interaction: subtypes `message`, `reasoning`,
//! `function_call`, `function_call_output`, `custom_tool_call`,
//! `custom_tool_call_output`), and lifecycle rows (`event_msg`,
//! `turn_context`, `world_state`, ...) that carry no conversation and are
//! kept as System-role raw carriers with no Part, so native restore can
//! replay them.
//!
//! Codex 0.151+ routes every tool through a JavaScript runtime: each call is
//! `custom_tool_call{name:"exec", input:<js snippet>}`, whose snippet calls
//! `tools.<real tool>(...)`, and its outcome lives in the `event_msg
//! item_completed` rows (`item.type == "CommandExecution"`: argv, cwd, exit
//! code) that sit between the call and its `custom_tool_call_output`. The
//! adapter names the call after the one tool the snippet wraps (a snippet
//! that wraps none or several stays `exec`), exposes the executed commands on
//! `params`, and marks the result failed when the script itself failed
//! (`Script failed` header) OR any command it ran exited non-zero - wider
//! than one exit code on purpose, since one script can run several commands.
//! That is a deliberate fork from the rule other adapters follow (grok-build:
//! a non-zero exit stays `completed`, `failed` means the tool itself failed):
//! the JS runtime reports `Script completed` for a red build, so its own
//! verdict alone would never flag a failed command. Only JS-runtime calls get
//! this rule; `function_call` rows and non-`exec` custom tools keep
//! `is_failure: false` as before. Argument parsing stops at the tool name:
//! arguments are JavaScript, not JSON (`apply_patch` takes a template
//! literal), so the raw snippet is kept as `params.script`.
//!
//! Pre-Oct-2025 legacy rollouts (spec.md#adapters) predate the envelope: the
//! first row is a bare metadata object and each data row is an un-enveloped
//! payload, interleaved with `{record_type:"state"}` noise. The adapter
//! accepts both shapes.

use std::{
    collections::HashMap,
    ops::Range,
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
    extract::{
        Extracted, extract_compact_repr, extract_raw_record, extract_self_str, extract_str,
        extract_str_range, json_or_string,
    },
    extracted_text,
    jsonl::{
        BoundedRow, JsonlTree, jsonl_tree_discover, jsonl_tree_events, peek_first_line,
        peek_last_mapped,
    },
    jsonl_bytes, part_id, part_ordinal, raw_record,
};

const NAME: &str = "codex-cli";
/// The `custom_tool_call` name Codex 0.151+ gives every JS-runtime wrapper.
const JS_RUNTIME_TOOL: &str = "exec";

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
    // The placement recorded at ingest (local-time stamped, see
    // session_from_rows) is the only faithful source; the UTC derivation
    // below is the foreign fallback and matches native only for a
    // UTC-offset-zero capture.
    let source = session.session.options.get("source");
    if let Some(name) = source
        .and_then(|source| source.get("file_name"))
        .and_then(Value::as_str)
        && let Some(Value::Array(day_dir)) = source.and_then(|source| source.get("day_dir"))
        && let Some([year, month, day]) = day_dir
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()
            .as_deref()
    {
        return PathBuf::from("sessions")
            .join(year)
            .join(month)
            .join(day)
            .join(name);
    }
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

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> crate::adapter::PlanFuture<'a> {
        crate::adapter::jsonl::jsonl_tree_plan(self, oracle)
    }
}

impl JsonlTree for CodexCliAdapter {
    type State = FileState;

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

    fn peek_watermark(&self, path: &Path) -> crate::adapter::SourceWatermark {
        // Codex message ids are physical line numbers, not tail-recoverable on a
        // multi-GB rollout, so freshness keys on the watermark timestamp instead.
        // The session's max stored timestamp is its last `response_item`'s. Scan
        // a bounded tail backward for it - never the whole file. The nested
        // `Option` keeps the walk stopping at the newest response_item even
        // when its timestamp fails to parse, exactly like the pre-seam walk.
        match peek_last_mapped(path, |line| {
            let row: Value = serde_json::from_str(line).ok()?;
            (row.get("type").and_then(Value::as_str) == Some("response_item")).then(|| {
                let text = row.get("timestamp").and_then(Value::as_str)?;
                DateTime::parse_from_rfc3339(text)
                    .ok()
                    .map(|ts| ts.with_timezone(&Utc).timestamp_micros())
            })
        }) {
            Some(Some(ts)) => crate::adapter::SourceWatermark::At(ts),
            // The newest response_item exists but its timestamp is unreadable:
            // re-read, exactly as before.
            Some(None) => crate::adapter::SourceWatermark::Opaque,
            // No response_item in the scan (a legacy rollout, or a session with
            // no completed turn yet). Every message such a rollout stores
            // carries a timestamp at or after the session-start header (legacy
            // payload rows and noise carriers default to it; carriers with own
            // timestamps are later), so the header is a valid source watermark:
            // the gate skips only when the store already holds this session at
            // or past session start - i.e. it was ingested. A never-ingested
            // rollout has no stored watermark and always re-reads.
            None => match session_start_ts(path) {
                Some(ts) => crate::adapter::SourceWatermark::At(ts),
                None => crate::adapter::SourceWatermark::Opaque,
            },
        }
    }

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
        session_from_rows(path, rows)
    }

    fn file_state(&self, rows: &[BoundedRow]) -> FileState {
        FileState::index(rows)
    }

    fn events_from_row(
        &self,
        session: &Session,
        row: &BoundedRow,
        state: &mut Self::State,
    ) -> Result<Vec<IngestEvent>, String> {
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

/// Session-start timestamp (micros) from the rollout's first line, mirroring
/// `session_from_rows`' anchor: the `session_meta` payload timestamp (envelope
/// timestamp as fallback) or a legacy bare header's own. `None` for anything
/// that is not a recognizable session header.
fn session_start_ts(path: &Path) -> Option<i64> {
    let line = peek_first_line(path)?;
    let row: Value = serde_json::from_str(&line).ok()?;
    let payload = if row.get("type").and_then(Value::as_str) == Some("session_meta") {
        row.get("payload").unwrap_or(&Value::Null)
    } else if is_legacy_session_row(&row) {
        &row
    } else {
        return None;
    };
    let text = payload
        .get("timestamp")
        .and_then(Value::as_str)
        .or_else(|| row.get("timestamp").and_then(Value::as_str))?;
    Some(
        DateTime::parse_from_rfc3339(text)
            .ok()?
            .with_timezone(&Utc)
            .timestamp_micros(),
    )
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
            // Codex names the rollout and its `<y>/<m>/<d>` directories in
            // LOCAL time, which nothing inside the file records; native
            // restore needs both to land the file where codex would have.
            "file_name": path.file_name().and_then(|name| name.to_str()),
            "day_dir": path
                .ancestors()
                .skip(1)
                .take(3)
                .filter_map(|dir| dir.file_name().and_then(|name| name.to_str()))
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>(),
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
/// System message + text Parts), `function_call` / `custom_tool_call`
/// (Assistant + ToolCall), `function_call_output` / `custom_tool_call_output`
/// (Tool + ToolResult), `reasoning` (Assistant + Reasoning Part); every other
/// row (`event_msg`, `turn_context`, `world_state`, ...) is a System-role raw
/// carrier with no Part. `session_meta` is consumed up front. Legacy rows
/// (spec.md#adapters) carry the same payload shapes un-enveloped;
/// `{record_type:"state"}` markers and the bare first row are eventless.
fn events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
    state: &FileState,
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
    // A `response_item` envelope carries its own timestamp; a legacy data row
    // inherits the session's.
    let payload = normalized_payload(row);
    let timestamp = if kind == Some("response_item") {
        row.get("timestamp")
            .and_then(Value::as_str)
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(default_timestamp)
    } else {
        default_timestamp
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
            state,
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
            state,
        )),
        "custom_tool_call_output" => Ok(custom_tool_result_events(
            session_id,
            &message_id,
            timestamp,
            payload,
            row,
            state,
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

/// The per-turn payload of a row: a current row wraps it under
/// `response_item`, a legacy row IS the payload (spec.md#adapters).
fn normalized_payload(row: &Value) -> &Value {
    if row.get("type").and_then(Value::as_str) == Some("response_item") {
        row.get("payload").unwrap_or(&Value::Null)
    } else {
        row
    }
}

/// Per-file index the row walk reads (`JsonlTree::file_state`): what a tool
/// call's result row needs from rows that come BEFORE it (the call's name)
/// and what a JS-runtime call row needs from rows that come AFTER it (the
/// commands it ran). Built in one pass over the file's rows, dropped with
/// the file.
#[derive(Default)]
pub(crate) struct FileState {
    /// `call_id -> tool name`, so an output row (which carries no name) can
    /// name its tool. For a JS-runtime `exec` call this is the wrapped tool.
    tool_call_names: HashMap<String, Extracted<String>>,
    /// Every JS-runtime `exec` call by `call_id`, with the
    /// `{command, cwd, exit_code}` of each `CommandExecution` row between it
    /// and its output row - only those three fields, never the item's
    /// stdout/stderr. Association is positional: the rows carry no call id,
    /// and the writer emits them strictly inside the call's window. Presence
    /// of the key is what marks a call as JS-runtime; other calls never get
    /// an entry, so the exit-code rule cannot reach them.
    executions: HashMap<String, Vec<Value>>,
}

impl FileState {
    fn index(rows: &[BoundedRow]) -> Self {
        let mut state = Self::default();
        let mut open_call: Option<String> = None;
        for row in rows {
            let row = &row.value;
            let kind = row.get("type").and_then(Value::as_str);
            if kind == Some("event_msg") {
                if let Some(call_id) = &open_call
                    && let Some(item) = row.get("payload").and_then(|p| p.get("item"))
                    && item.get("type").and_then(Value::as_str) == Some("CommandExecution")
                {
                    let mut run = serde_json::Map::new();
                    for key in ["command", "cwd", "exit_code"] {
                        run.insert(
                            key.to_owned(),
                            item.get(key).cloned().unwrap_or(Value::Null),
                        );
                    }
                    state
                        .executions
                        .entry(call_id.clone())
                        .or_default()
                        .push(Value::Object(run));
                }
                continue;
            }
            let payload = normalized_payload(row);
            let call_id = payload.get("call_id").and_then(Value::as_str);
            match (payload.get("type").and_then(Value::as_str), call_id) {
                (Some("function_call" | "custom_tool_call"), Some(call_id)) => {
                    if let Some(name) = tool_call_name(payload) {
                        state.tool_call_names.insert(call_id.to_owned(), name);
                    }
                    open_call = None;
                    if payload.get("name").and_then(Value::as_str) == Some(JS_RUNTIME_TOOL) {
                        state.executions.entry(call_id.to_owned()).or_default();
                        open_call = Some(call_id.to_owned());
                    }
                }
                (Some("function_call_output" | "custom_tool_call_output"), _) => {
                    open_call = None;
                }
                _ => {}
            }
        }
        state
    }

    fn tool_name(&self, call_id: Option<&Extracted<String>>) -> Option<Extracted<String>> {
        call_id
            .and_then(|id| self.tool_call_names.get(id.as_str()))
            .cloned()
    }

    /// `None` for a call that is not a JS-runtime `exec` wrapper.
    fn executions(&self, call_id: Option<&Extracted<String>>) -> Option<&[Value]> {
        call_id
            .and_then(|id| self.executions.get(id.as_str()))
            .map(Vec::as_slice)
    }
}

/// The tool a call row names: for a JS-runtime `exec` wrapper the one tool
/// its snippet calls, else the row's own `name`.
fn tool_call_name(payload: &Value) -> Option<Extracted<String>> {
    let name = extract_str(payload, "name")?;
    if name.as_str() != JS_RUNTIME_TOOL {
        return Some(name);
    }
    payload
        .get("input")
        .and_then(Value::as_str)
        .and_then(wrapped_tool_name)
        .and_then(|range| extract_str_range(payload, "input", range))
        .or(Some(name))
}

/// Where in a JS-runtime snippet the wrapped tool's name sits: the single
/// distinct `tools.<name>(` it references. `None` when it references none (a
/// script that only inspects `ALL_TOOLS`, say) or several - those stay
/// `exec`.
fn wrapped_tool_name(script: &str) -> Option<Range<usize>> {
    const MARKER: &str = "tools.";
    let bytes = script.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut found: Option<Range<usize>> = None;
    let mut cursor = 0;
    while let Some(offset) = script[cursor..].find(MARKER) {
        let marker_at = cursor + offset;
        let start = marker_at + MARKER.len();
        cursor = start;
        // `ALL_TOOLS.filter(` and `mytools.x(` are not the runtime namespace.
        if marker_at > 0 && is_ident(bytes[marker_at - 1]) {
            continue;
        }
        let end = start + bytes[start..].iter().take_while(|b| is_ident(**b)).count();
        if end == start || bytes.get(end) != Some(&b'(') {
            continue;
        }
        match &found {
            Some(prev) if script[prev.clone()] != script[start..end] => return None,
            _ => found = Some(start..end),
        }
    }
    found
}

/// `params` for a JS-runtime call: the raw snippet plus what the
/// `CommandExecution` rows in its window recorded - argv (`command`), `cwd`
/// and `exit_code` flattened when exactly one command ran (the common case),
/// or one `executions[]` entry per command when a script ran several.
/// `command` is codex's argv (`["/bin/zsh", "-lc", "<cmd>"]`), not a string
/// like claude-code's Bash `command`.
fn exec_params(script: Value, executions: &[Value]) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("script".to_owned(), script);
    match executions {
        [] => {}
        [only] => {
            if let Value::Object(fields) = only {
                params.extend(fields.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
        }
        several => {
            params.insert("executions".to_owned(), Value::Array(several.to_vec()));
        }
    }
    Value::Object(params)
}

fn any_command_failed(executions: &[Value]) -> bool {
    executions.iter().any(|item| {
        item.get("exit_code")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0)
    })
}

/// The JS runtime's own verdict on the script, the first line of its output
/// (`Script completed` / `Script failed`). Script-level only: `text(r.output)`
/// runs whatever the command's exit code was, so a failed build still reads
/// `Script completed` here - the command verdict is `any_command_failed`.
fn script_failed(output: &Value) -> bool {
    let head = match output {
        Value::String(text) => Some(text.as_str()),
        Value::Array(blocks) => blocks
            .first()
            .and_then(|block| block.get("text"))
            .and_then(Value::as_str),
        _ => None,
    };
    head.is_some_and(|text| text.starts_with("Script failed"))
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
        Some(Value::String(text)) => json_or_string(text),
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
    state: &FileState,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
    let input = payload.get("input").cloned().unwrap_or(Value::Null);
    let params = match state.executions(call_id.as_ref()) {
        Some(executions) => exec_params(input, executions),
        None => input,
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
            name: state
                .tool_name(call_id.as_ref())
                .or_else(|| extract_str(payload, "name")),
            call_id,
            params,
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
    state: &FileState,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
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
            name: state
                .tool_name(call_id.as_ref())
                .or_else(|| extract_str(payload, "name")),
            is_failure: state
                .executions(call_id.as_ref())
                .is_some_and(|executions| script_failed(&result) || any_command_failed(executions)),
            call_id,
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

fn tool_result_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    row: &Value,
    state: &FileState,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
    // Resolve tool name from the earlier `function_call` row via the
    // per-file `call_id -> name` map. Misses (e.g. compaction pruned the
    // originating call) yield `None`, a faithful "unresolved" value.
    let name = state.tool_name(call_id.as_ref());
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

    fn wrapped(script: &str) -> Option<&str> {
        wrapped_tool_name(script).map(|range| &script[range])
    }

    #[test]
    fn wrapped_tool_name_is_the_single_tool_the_snippet_calls() {
        assert_eq!(
            wrapped("const r = await tools.exec_command({\"cmd\":\"ls\"});\ntext(r.output);"),
            Some("exec_command")
        );
        assert_eq!(
            wrapped(
                "for (const c of cmds) { const r = await tools.exec_command({cmd: c}); text(r); }"
            ),
            Some("exec_command")
        );
        assert_eq!(
            wrapped(
                "const patch = `*** Begin Patch\n*** End Patch`;\ntext(await tools.apply_patch(patch));"
            ),
            Some("apply_patch")
        );
    }

    #[test]
    fn wrapped_tool_name_stays_unresolved_for_none_or_several() {
        assert_eq!(
            wrapped("text(ALL_TOOLS.filter(x => x.name.includes(\"plan\")));"),
            None
        );
        assert_eq!(wrapped("tools.foo; tools.bar"), None);
        assert_eq!(
            wrapped("await tools.exec_command({cmd: \"ls\"}); await tools.apply_patch(p);"),
            None
        );
        assert_eq!(wrapped("mytools.exec_command()"), None);
    }

    #[test]
    fn script_failed_reads_the_runtime_header_only() {
        assert!(script_failed(
            &json!([{"type":"input_text","text":"Script failed\nError: x"}])
        ));
        assert!(script_failed(&json!("Script failed\nError: x")));
        assert!(!script_failed(
            &json!([{"type":"input_text","text":"Script completed\nWall time 0.4 seconds\nOutput:\nexit 1"}])
        ));
        assert!(!script_failed(
            &json!([{"type":"input_text","text":"Wall time 0.4 seconds"}])
        ));
        assert!(!script_failed(&Value::Null));
    }

    fn row(line: usize, value: Value) -> BoundedRow {
        BoundedRow { line, value }
    }

    fn response_item(line: usize, payload: Value) -> BoundedRow {
        row(
            line,
            json!({"timestamp": "2026-09-01T18:50:56.000Z", "type": "response_item", "payload": payload}),
        )
    }

    fn command_execution(line: usize, cmd: &str, exit_code: i64) -> BoundedRow {
        row(
            line,
            json!({"timestamp": "2026-09-01T18:50:56.000Z", "type": "event_msg", "payload": {"type": "item_completed", "item": {"type": "CommandExecution", "command": ["/bin/zsh", "-lc", cmd], "cwd": "file:///tmp/codex-fixture/repo", "exit_code": exit_code, "status": if exit_code == 0 { "completed" } else { "failed" }}}}),
        )
    }

    /// The per-file index: a JS-runtime call's window (call .. output)
    /// collects the `CommandExecution` rows it ran, a row outside any window
    /// is dropped, and a `function_call` resolves by its own name but gets no
    /// window - the exit-code rule never reaches legacy rows.
    #[test]
    fn file_state_windows_command_executions_under_js_runtime_calls() {
        let rows = vec![
            command_execution(1, "stray", 1),
            response_item(
                2,
                json!({"type": "custom_tool_call", "call_id": "c1", "name": "exec", "input": "await tools.exec_command({cmd:\"echo a\"}); await tools.exec_command({cmd:\"false\"});"}),
            ),
            command_execution(3, "echo a", 0),
            command_execution(4, "false", 1),
            response_item(
                5,
                json!({"type": "custom_tool_call_output", "call_id": "c1", "output": [{"type": "input_text", "text": "Script completed"}]}),
            ),
            response_item(
                6,
                json!({"type": "custom_tool_call", "call_id": "c2", "name": "exec", "input": "text(ALL_TOOLS.length);"}),
            ),
            response_item(
                7,
                json!({"type": "custom_tool_call_output", "call_id": "c2", "output": [{"type": "input_text", "text": "Script failed\nError"}]}),
            ),
            response_item(
                8,
                json!({"type": "function_call", "call_id": "c3", "name": "shell", "arguments": "{}"}),
            ),
            command_execution(9, "ls", 1),
            response_item(
                10,
                json!({"type": "function_call_output", "call_id": "c3", "output": "ok"}),
            ),
        ];
        let state = FileState::index(&rows);
        let id = |s: &str| Some(Extracted::from_test_value(s.to_owned()));

        assert_eq!(
            state.tool_name(id("c1").as_ref()).as_deref(),
            Some(&"exec_command".to_owned())
        );
        assert_eq!(
            state.tool_name(id("c2").as_ref()).as_deref(),
            Some(&"exec".to_owned())
        );
        assert_eq!(
            state.tool_name(id("c3").as_ref()).as_deref(),
            Some(&"shell".to_owned())
        );
        let c1 = state
            .executions(id("c1").as_ref())
            .expect("js-runtime call");
        assert_eq!(c1.len(), 2);
        assert!(any_command_failed(c1));
        assert_eq!(
            state.executions(id("c2").as_ref()).map(<[Value]>::len),
            Some(0)
        );
        assert!(
            state.executions(id("c3").as_ref()).is_none(),
            "a function_call gets no window",
        );
        assert!(state.executions(None).is_none());
    }

    #[test]
    fn exec_params_flattens_only_a_single_command() {
        let run = |cmd: &str, code: i64| json!({"command": ["/bin/zsh", "-lc", cmd], "cwd": "file:///tmp/codex-fixture/repo", "exit_code": code});
        let single = exec_params(json!("s"), &[run("ls", 0)]);
        assert_eq!(single["command"], json!(["/bin/zsh", "-lc", "ls"]));
        assert_eq!(single["cwd"], json!("file:///tmp/codex-fixture/repo"));
        assert_eq!(single["exit_code"], json!(0));
        assert!(single.get("executions").is_none());
        let several = exec_params(json!("s"), &[run("echo a", 0), run("false", 1)]);
        assert!(
            several.get("command").is_none(),
            "two commands: nothing to flatten"
        );
        assert_eq!(several["executions"][1]["exit_code"], json!(1));
        assert_eq!(exec_params(json!("s"), &[]), json!({"script": "s"}));
    }

    #[test]
    fn probe_default_finds_codex_sessions_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &CodexCliFactory,
            &[".codex", "sessions"],
        )
    }

    /// `peek_watermark` is the freshness watermark for multi-GB rollouts where
    /// the line-numbered message id is not tail-recoverable. It must read the
    /// last `response_item`'s envelope timestamp - pond's stored max - and
    /// ignore the trailing `event_msg` noise (whose stored timestamp is the
    /// session default), scanning only the file tail.
    #[test]
    fn peek_watermark_targets_last_response_item_ignoring_trailing_event_msg() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rollout.jsonl");
        let lines = [
            r#"{"type":"session_meta","timestamp":"2026-03-20T03:00:00.000Z","payload":{"id":"sess-x","timestamp":"2026-03-20T03:00:00.000Z"}}"#,
            r#"{"type":"response_item","timestamp":"2026-03-20T03:10:00.000Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}"#,
            r#"{"type":"response_item","timestamp":"2026-03-20T03:20:30.500Z","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"yo"}]}}"#,
            // Trailing token-count noise, later wall-clock but stored at the
            // session default - must NOT be picked as the watermark.
            r#"{"type":"event_msg","timestamp":"2026-03-20T03:59:59.000Z","payload":{"type":"token_count","info":{}}}"#,
        ];
        std::fs::write(&path, lines.join("\n") + "\n").unwrap();

        let adapter = CodexCliAdapter::new(dir.path());
        let expected = DateTime::parse_from_rfc3339("2026-03-20T03:20:30.500Z")
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_micros();
        assert_eq!(
            adapter.peek_watermark(&path),
            crate::adapter::SourceWatermark::At(expected)
        );
    }

    /// A rollout with no `response_item` at all (legacy data rows only, or a
    /// session with no completed turn) keys freshness on the session-start
    /// header: pond stores all such rows at or after that timestamp, so a
    /// stored watermark at/past it proves the file was ingested. A
    /// never-ingested file has no stored watermark and still re-reads.
    #[test]
    fn peek_watermark_falls_back_to_session_start_without_response_items() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rollout-legacy.jsonl");
        let lines = [
            r#"{"id":"sess-legacy","timestamp":"2025-09-10T12:48:29.371Z","git":{},"instructions":null}"#,
            r#"{"record_type":"state"}"#,
            r#"{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}"#,
        ];
        std::fs::write(&path, lines.join("\n") + "\n").unwrap();

        let adapter = CodexCliAdapter::new(dir.path());
        let expected = DateTime::parse_from_rfc3339("2025-09-10T12:48:29.371Z")
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_micros();
        assert_eq!(
            adapter.peek_watermark(&path),
            crate::adapter::SourceWatermark::At(expected)
        );
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
        let map = FileState::default();

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
        let map = FileState::default();
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
