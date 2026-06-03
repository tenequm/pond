//! opencode adapter (github.com/sst/opencode).
//!
//! Unlike claude-code and codex-cli, opencode does not store one JSONL file per
//! session. It uses a content-addressed split layout under a `storage/` root:
//!
//! - `session/<projectID>/<sessionID>.json` - session metadata
//! - `message/<sessionID>/<messageID>.json` - one message header (no content)
//! - `part/<messageID>/<partID>.json`        - one content part
//!
//! So this is the first adapter to drive the [`Adapter`] seam directly rather
//! than through the JSONL helper: it walks the three levels, sorts by id (the
//! ids are lexically time-sortable, so filename order is creation order), and
//! emits `Session -> Message -> Parts` per session.
//!
//! opencode fuses a tool call and its result into one `tool` part on the
//! assistant message. Canonical keeps the two apart (a `tool_result` on an
//! assistant message is a category error, spec.md#model-part-provenance), so the
//! adapter splits it: a `ToolCall` Part stays on the assistant message and a
//! synthetic `Tool` message carries the `ToolResult`. Native restore replays
//! each real part's stored `raw_record` at its original path and skips the
//! synthetic records, so the split is value-complete-lossless.

use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::{
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, compact_json, config_path,
    extract::{bound_value, extract_raw_record, extract_str},
    part_id,
};

const NAME: &str = "opencode";

/// Event-channel bound; doubles as backpressure - the blocking reader parks on
/// `blocking_send` when the consumer lags.
const CHANNEL_CAP: usize = 256;

/// Stateless factory: opens [`OpencodeAdapter`] instances and probes for the
/// canonical install location under `~/.local/share/opencode/storage`.
pub struct OpencodeFactory;

impl AdapterFactory for OpencodeFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(OpencodeAdapter::new(config_path(NAME, config)?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = env
            .home
            .join(".local")
            .join("share")
            .join("opencode")
            .join("storage");
        path.exists().then(|| json!({ "path": path }))
    }

    fn serialize(
        &self,
        session: &crate::sessions::SessionWithMessages,
        fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        match fidelity {
            RestoreFidelity::Native => serialize_native(session),
            RestoreFidelity::Foreign => serialize_foreign(session),
        }
    }
}

/// Configured opencode reader, rooted at a `storage/` directory.
#[derive(Debug, Clone)]
pub struct OpencodeAdapter {
    root: PathBuf,
}

impl OpencodeAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for OpencodeAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let root = self.root.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                collect_session_files(&root).map(|files| files.len())
            })
            .await
            .map_err(join_error)?
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let adapter = self.clone();
        Box::pin(stream! {
            let files = {
                let root = adapter.root.clone();
                tokio::task::spawn_blocking(move || collect_session_files(&root)).await
            };
            let files = match files {
                Ok(Ok(files)) => files,
                Ok(Err(error)) => { yield Err(error); return; }
                Err(join) => { yield Err(join_error(join)); return; }
            };

            let mut survivors = Vec::with_capacity(files.len());
            for file in files {
                if let Some(ingested) = oracle.last_ingested_at(&file.session_id)
                    && let Some(mtime) = file.mtime
                    && mtime <= ingested
                {
                    yield Ok(AdapterYield::Skipped {
                        session_id: Some(file.session_id.clone()),
                        project: None,
                        reason: SkipReason::Fresh,
                    });
                    continue;
                }
                survivors.push(file);
            }

            let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
            let reader = adapter.clone();
            let handle = tokio::task::spawn_blocking(move || read_sessions(&reader, survivors, &tx));
            while let Some(item) = rx.recv().await {
                yield item;
            }
            if let Err(join) = handle.await {
                yield Err(join_error(join));
            }
        })
    }
}

/// A blocking-task panic is a pond bug, not bad source data, so it fails the
/// whole run rather than skipping a session.
fn join_error(join: tokio::task::JoinError) -> AdapterError {
    AdapterError::io(
        NAME,
        "blocking read task",
        std::io::Error::other(join.to_string()),
    )
}

/// One session file located on disk, with the cheap freshness inputs.
struct SessionFile {
    session_id: String,
    path: PathBuf,
    mtime: Option<DateTime<Utc>>,
}

/// Walk `<root>/session/<projectID>/<sessionID>.json`, sorted for deterministic
/// ingest order. A missing `session/` dir means "no sessions yet", not an error.
fn collect_session_files(root: &Path) -> Result<Vec<SessionFile>, AdapterError> {
    let session_root = root.join("session");
    let io = |path: &Path, source| AdapterError::io(NAME, path.display().to_string(), source);
    let entries = match std::fs::read_dir(&session_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io(&session_root, error)),
    };
    let mut out = Vec::new();
    for project in entries {
        let project = project.map_err(|error| io(&session_root, error))?;
        if !project
            .file_type()
            .map_err(|error| io(&project.path(), error))?
            .is_dir()
        {
            continue;
        }
        let project_dir = project.path();
        for session in std::fs::read_dir(&project_dir).map_err(|error| io(&project_dir, error))? {
            let session = session.map_err(|error| io(&project_dir, error))?;
            let path = session.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Some(session_id) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(ToOwned::to_owned)
            else {
                continue;
            };
            let mtime = std::fs::metadata(&path)
                .and_then(|meta| meta.modified())
                .ok()
                .map(DateTime::<Utc>::from);
            out.push(SessionFile {
                session_id,
                path,
                mtime,
            });
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn read_sessions(
    adapter: &OpencodeAdapter,
    sessions: Vec<SessionFile>,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) {
    for session in sessions {
        if !read_one_session(adapter, &session, tx) {
            return;
        }
    }
}

/// Returns `false` when the consumer dropped the receiver and the read should stop.
fn read_one_session(
    adapter: &OpencodeAdapter,
    file: &SessionFile,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    macro_rules! emit {
        ($item:expr) => {
            if tx.blocking_send($item).is_err() {
                return false;
            }
        };
    }

    let session_value = match read_json(&file.path) {
        Ok(value) => value,
        Err(error) => {
            emit!(Err(error));
            return true;
        }
    };
    let session = match session_from_value(&session_value, &file.path) {
        Ok(session) => session,
        Err(error) => {
            emit!(Err(error));
            return true;
        }
    };
    let session_id = session.id.clone();
    emit!(Ok(AdapterYield::Event(IngestEvent::Session(session))));

    let message_dir = adapter.root.join("message").join(&session_id);
    let message_files = match list_json_sorted(&message_dir) {
        Ok(files) => files,
        Err(error) => {
            emit!(Err(error));
            return true;
        }
    };
    for message_path in message_files {
        let message_value = match read_json(&message_path) {
            Ok(value) => value,
            Err(error) => {
                emit!(Err(error));
                continue;
            }
        };
        let Some(message_id) = message_value.get("id").and_then(Value::as_str) else {
            emit!(Err(AdapterError::schema(
                NAME,
                message_path.display().to_string(),
                "message file missing `id`",
            )));
            continue;
        };
        let part_dir = adapter.root.join("part").join(message_id);
        let part_files = match list_json_sorted(&part_dir) {
            Ok(files) => files,
            Err(error) => {
                emit!(Err(error));
                continue;
            }
        };
        let mut parts = Vec::with_capacity(part_files.len());
        let mut part_error = None;
        for part_path in part_files {
            match read_json(&part_path) {
                Ok(value) => parts.push(value),
                Err(error) => {
                    part_error = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = part_error {
            emit!(Err(error));
            continue;
        }
        match build_message_events(&session_id, &message_value, &parts) {
            Ok(events) => {
                for event in events {
                    emit!(Ok(AdapterYield::Event(event)));
                }
            }
            Err(error) => emit!(Err(error)),
        }
    }
    true
}

/// Read one JSON file, bounding every string leaf at the seam cap
/// (spec.md#adapter-bounded-values) before it leaves this module.
fn read_json(path: &Path) -> Result<Value, AdapterError> {
    let bytes = std::fs::read(path)
        .map_err(|error| AdapterError::io(NAME, path.display().to_string(), error))?;
    let mut value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| AdapterError::parse(NAME, path.display().to_string(), 1, error))?;
    bound_value(&mut value);
    Ok(value)
}

/// List `*.json` files in `dir`, sorted by filename (= creation order, ids are
/// time-sortable). A missing dir is an empty list - a message can legitimately
/// carry no parts.
fn list_json_sorted(dir: &Path) -> Result<Vec<PathBuf>, AdapterError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(AdapterError::io(NAME, dir.display().to_string(), error)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|error| AdapterError::io(NAME, dir.display().to_string(), error))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn session_from_value(value: &Value, path: &Path) -> Result<Session, AdapterError> {
    let display = path.display().to_string();
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::schema(NAME, display.clone(), "session missing `id`"))?
        .to_owned();
    let created_at = millis_at(value, &["time", "created"]).ok_or_else(|| {
        AdapterError::schema(NAME, display.clone(), "session missing `time.created`")
    })?;
    // spec.md#model-project-non-empty: opencode always records `directory` (the
    // project cwd); its absence is a malformed session, not a default.
    let project = extract_str(value, "directory")
        .ok_or_else(|| AdapterError::schema(NAME, display, "session missing `directory`"))?;

    let mut options = ProviderOptions::new();
    options.insert(
        "opencode".to_owned(),
        json!({ "raw_record": extract_raw_record(value) }),
    );

    Ok(Session {
        id,
        // opencode sub-sessions (a Task spawn) carry `parentID`; a soft
        // reference, present only when this session was spawned from another.
        parent_session_id: value
            .get("parentID")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        parent_message_id: None,
        source_agent: NAME.to_owned(),
        created_at,
        project,
        options,
    })
}

/// Build the ordered event stream for one message: the message, its parts in
/// order, then any synthetic `Tool` messages (one per `tool` part) each
/// followed by its `ToolResult`.
fn build_message_events(
    session_id: &str,
    message_value: &Value,
    part_values: &[Value],
) -> Result<Vec<IngestEvent>, AdapterError> {
    let message_id = message_value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::schema(NAME, session_id.to_owned(), "message missing `id`"))?;
    let role = message_value.get("role").and_then(Value::as_str);
    let timestamp = millis_at(message_value, &["time", "created"]).unwrap_or_else(anchor_ts);

    let options = opencode_raw(message_value);
    let message = match role {
        Some("user") => Message::User {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
        Some("assistant") => Message::Assistant {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
        // opencode v2 carries non-conversational message roles (synthetic,
        // shell, compaction); keep them as System carriers rather than drop
        // them (spec.md#adapter-integrity-no-silent-drops). The raw record
        // survives in options; the role label is the content.
        other => Message::System {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            content: other.and_then(|_| extract_str(message_value, "role")),
            options,
        },
    };

    let mut events = vec![IngestEvent::Message(message)];
    let mut deferred = Vec::new();
    for (ordinal, part_value) in part_values.iter().enumerate() {
        let mapped = map_part(session_id, message_id, ordinal, part_value, timestamp);
        events.push(IngestEvent::Part(mapped.part));
        if let Some(split) = mapped.tool_split {
            deferred.push(split);
        }
    }
    for ToolSplit {
        message: tool_message,
        result,
    } in deferred
    {
        events.push(IngestEvent::Message(tool_message));
        events.push(IngestEvent::Part(result));
    }
    Ok(events)
}

/// One mapped source part: the canonical Part it becomes, plus - for a fused
/// `tool` part - the synthetic `Tool` message and `ToolResult` it splits off.
struct MappedPart {
    part: Part,
    tool_split: Option<ToolSplit>,
}

struct ToolSplit {
    message: Message,
    result: Part,
}

fn map_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    value: &Value,
    message_ts: DateTime<Utc>,
) -> MappedPart {
    let kind = value.get("type").and_then(Value::as_str);
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .map_or_else(|| part_id(message_id, ordinal), ToOwned::to_owned);

    if kind == Some("tool") {
        return tool_part(session_id, message_id, &id, ordinal, value, message_ts);
    }

    let (provenance, part_kind) = match kind {
        Some("text") => (text_provenance(value), text_kind(value)),
        Some("reasoning") => (Provenance::Conversational, reasoning_kind(value)),
        Some("file") => (Provenance::Conversational, file_kind(value)),
        // patch / step-start / step-finish (and any other marker) are
        // harness-produced turn machinery, not conversation: keep them as
        // injected Parts whose `raw_record` round-trips the source file.
        _ => (Provenance::Injected, PartKind::Text { text: None }),
    };

    MappedPart {
        part: Part {
            session_id: session_id.to_owned(),
            id,
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            provenance,
            options: opencode_raw(value),
            kind: part_kind,
        },
        tool_split: None,
    }
}

/// spec.md#model-part-provenance: opencode marks harness-injected text parts
/// (the `Called the <tool> tool ...` echo, auto-expanded `@file` content) with
/// `synthetic: true`; a genuine prompt or model reply is `synthetic: false`.
fn text_provenance(value: &Value) -> Provenance {
    if value.get("synthetic").and_then(Value::as_bool) == Some(true) {
        Provenance::Injected
    } else {
        Provenance::Conversational
    }
}

fn text_kind(value: &Value) -> PartKind {
    PartKind::Text {
        text: extract_str(value, "text"),
    }
}

fn reasoning_kind(value: &Value) -> PartKind {
    PartKind::Reasoning {
        text: extract_str(value, "text"),
    }
}

fn file_kind(value: &Value) -> PartKind {
    // A generic MIME default is a transport descriptor, not a synthesized field
    // value (spec.md#model-no-synthesis).
    let media_type = value
        .get("mime")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream")
        .to_owned();
    let file_name = value
        .get("filename")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let data = match value.get("url").and_then(Value::as_str) {
        Some(url) => FileData::Url(url.to_owned()),
        None => FileData::String(compact_json(value)),
    };
    PartKind::File {
        media_type,
        file_name,
        data,
    }
}

/// Split one opencode `tool` part (call + result fused) into a `ToolCall` on the
/// owning assistant message and a synthetic `Tool` message carrying the
/// `ToolResult`. The `ToolCall` keeps the source part's id and stores its full
/// `raw_record`, so native restore reproduces the single source file; the
/// synthetic records carry no `raw_record` and are skipped on restore.
fn tool_part(
    session_id: &str,
    message_id: &str,
    id: &str,
    ordinal: usize,
    value: &Value,
    message_ts: DateTime<Utc>,
) -> MappedPart {
    let call_id = extract_str(value, "callID");
    let name = extract_str(value, "tool");
    let state = value.get("state");
    let status = state.and_then(|s| s.get("status")).and_then(Value::as_str);
    let input = state
        .and_then(|s| s.get("input"))
        .cloned()
        .unwrap_or(Value::Null);

    let tool_call = Part {
        session_id: session_id.to_owned(),
        id: id.to_owned(),
        message_id: message_id.to_owned(),
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
        // spec.md#model-part-provenance: the model authored the tool call.
        provenance: Provenance::Conversational,
        options: opencode_raw(value),
        kind: PartKind::ToolCall {
            call_id: call_id.clone(),
            name: name.clone(),
            params: input,
            provider_executed: false,
        },
    };

    let tool_message_id = format!("{id}/result");
    let result_ts = millis_at(value, &["state", "time", "end"]).unwrap_or(message_ts);
    let result = state
        .and_then(|s| s.get("output").or_else(|| s.get("error")))
        .cloned()
        .or_else(|| state.cloned())
        .unwrap_or(Value::Null);

    let tool_message = Message::Tool {
        id: tool_message_id.clone(),
        session_id: session_id.to_owned(),
        timestamp: result_ts,
        options: synthetic_options(),
    };
    let result_part = Part {
        session_id: session_id.to_owned(),
        id: part_id(&tool_message_id, 0),
        message_id: tool_message_id,
        ordinal: 0,
        // spec.md#model-part-provenance: tool output is runtime-produced.
        provenance: Provenance::Injected,
        options: synthetic_options(),
        kind: PartKind::ToolResult {
            call_id,
            name,
            is_failure: status == Some("error"),
            result,
        },
    };

    MappedPart {
        part: tool_call,
        tool_split: Some(ToolSplit {
            message: tool_message,
            result: result_part,
        }),
    }
}

fn opencode_raw(value: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert(
        "opencode".to_owned(),
        json!({ "raw_record": extract_raw_record(value) }),
    );
    options
}

/// Marks a canonical record the adapter synthesized (the `Tool` message and
/// `ToolResult` split off a fused `tool` part). Native restore skips records so
/// marked - they correspond to no source file.
fn synthetic_options() -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert("opencode".to_owned(), json!({ "synthetic": true }));
    options
}

fn millis_at(value: &Value, path: &[&str]) -> Option<DateTime<Utc>> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(key)?;
    }
    DateTime::from_timestamp_millis(cursor.as_i64()?)
}

/// Recover the stored source record for native restore.
fn opencode_raw_record(options: &ProviderOptions) -> Option<Value> {
    options
        .get("opencode")
        .and_then(|o| o.get("raw_record"))
        .cloned()
}

fn is_synthetic(options: &ProviderOptions) -> bool {
    options
        .get("opencode")
        .and_then(|o| o.get("synthetic"))
        .and_then(Value::as_bool)
        == Some(true)
}

fn serialize_native(
    session: &crate::sessions::SessionWithMessages,
) -> Result<Vec<RestoredFile>, AdapterError> {
    // Native replays each stored `raw_record` at its original split path; the
    // synthetic `Tool` message and `ToolResult` (which carry no `raw_record`)
    // are skipped, re-fusing into the single source `tool` part. Replay echoes
    // a frozen snapshot - safe only while canonical is append-only
    // (spec.md#adapter-integrity-additive-sync).
    let session_raw = opencode_raw_record(&session.session.options).ok_or_else(|| {
        AdapterError::schema(
            NAME,
            session.session.id.clone(),
            "native restore needs the stored session raw_record",
        )
    })?;
    let project_id = session_raw
        .get("projectID")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                session.session.id.clone(),
                "stored session raw_record missing projectID",
            )
        })?;

    let mut files = vec![RestoredFile {
        relative_path: PathBuf::from("session")
            .join(project_id)
            .join(format!("{}.json", session.session.id)),
        bytes: encode(&session_raw, &session.session.id)?,
    }];

    for message in &session.messages {
        if !is_synthetic(message.message.options())
            && let Some(raw) = opencode_raw_record(message.message.options())
        {
            files.push(RestoredFile {
                relative_path: PathBuf::from("message")
                    .join(&session.session.id)
                    .join(format!("{}.json", message.message.id())),
                bytes: encode(&raw, message.message.id())?,
            });
        }
        for part in &message.parts {
            // A part that carries a `raw_record` maps 1:1 to a source file at
            // `part/<message_id>/<part_id>.json`; synthetic split parts do not.
            if let Some(raw) = opencode_raw_record(&part.options) {
                files.push(RestoredFile {
                    relative_path: PathBuf::from("part")
                        .join(&part.message_id)
                        .join(format!("{}.json", part.id)),
                    bytes: encode(&raw, &part.id)?,
                });
            }
        }
    }
    Ok(files)
}

fn serialize_foreign(
    session: &crate::sessions::SessionWithMessages,
) -> Result<Vec<RestoredFile>, AdapterError> {
    // Foreign restore: a best-effort, idiomatic opencode tree. A non-opencode
    // session has no `projectID` hash, so derive a stable directory key from
    // the project path; tool results (canonical `Tool` messages) and System
    // carriers have no idiomatic home in opencode's part model and are dropped
    // (spec.md#adapter-native-restore-lossless, foreign clause).
    let project_id = encode_project(&session.session.project);
    let created = session.session.created_at.timestamp_millis();
    let session_record = json!({
        "id": session.session.id,
        "projectID": project_id,
        "directory": &*session.session.project,
        "time": { "created": created, "updated": created },
    });
    let mut files = vec![RestoredFile {
        relative_path: PathBuf::from("session")
            .join(&project_id)
            .join(format!("{}.json", session.session.id)),
        bytes: encode(&session_record, &session.session.id)?,
    }];

    for message in &session.messages {
        let role = match message.message {
            Message::User { .. } => "user",
            Message::Assistant { .. } => "assistant",
            // No idiomatic opencode home; content stays in canonical.
            Message::Tool { .. } | Message::System { .. } => continue,
        };
        let created = message.message.timestamp().timestamp_millis();
        let record = json!({
            "id": message.message.id(),
            "sessionID": session.session.id,
            "role": role,
            "time": { "created": created },
        });
        files.push(RestoredFile {
            relative_path: PathBuf::from("message")
                .join(&session.session.id)
                .join(format!("{}.json", message.message.id())),
            bytes: encode(&record, message.message.id())?,
        });
        for part in &message.parts {
            let Some(record) = foreign_part(&session.session.id, part) else {
                continue;
            };
            files.push(RestoredFile {
                relative_path: PathBuf::from("part")
                    .join(message.message.id())
                    .join(format!("{}.json", part.id)),
                bytes: encode(&record, &part.id)?,
            });
        }
    }
    Ok(files)
}

fn foreign_part(session_id: &str, part: &Part) -> Option<Value> {
    let mut record = match &part.kind {
        PartKind::Text { text } => json!({
            "type": "text",
            "text": text.as_deref().map(|t| &**t),
            "synthetic": part.provenance == Provenance::Injected,
        }),
        PartKind::Reasoning { text } => json!({
            "type": "reasoning",
            "text": text.as_deref().map(|t| &**t),
        }),
        PartKind::File {
            media_type,
            file_name,
            data,
        } => json!({
            "type": "file",
            "mime": media_type,
            "filename": file_name,
            "url": match data {
                FileData::Url(url) => Some(url.clone()),
                _ => None,
            },
        }),
        PartKind::ToolCall {
            call_id,
            name,
            params,
            ..
        } => json!({
            "type": "tool",
            "callID": call_id.as_deref().map(|c| &**c),
            "tool": name.as_deref().map(|n| &**n),
            "state": { "status": "completed", "input": params },
        }),
        // ToolResult / approval parts have no standalone opencode shape.
        _ => return None,
    };
    if let Value::Object(map) = &mut record {
        map.insert("id".to_owned(), json!(part.id));
        map.insert("sessionID".to_owned(), json!(session_id));
        map.insert("messageID".to_owned(), json!(part.message_id));
    }
    Some(record)
}

fn encode(value: &Value, location: &str) -> Result<Vec<u8>, AdapterError> {
    serde_json::to_vec(value).map_err(|error| {
        AdapterError::schema(
            NAME,
            location.to_owned(),
            format!("json encode failed: {error}"),
        )
    })
}

fn encode_project(project: &str) -> String {
    project
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// A message always carries `time.created`, so this is unreachable for real
/// data; a constant epoch anchor keeps the timestamp total without inventing a
/// plausible-looking "now" (and without a non-deterministic `Utc::now()`).
fn anchor_ts() -> DateTime<Utc> {
    DateTime::from_timestamp_millis(0).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    //! End-to-end test for the opencode adapter: ingest the committed
    //! split-file fixture corpus and assert pond's canonical shape comes out
    //! the other side, including the fused-tool-part split. The fixture lives
    //! under `tests/fixtures/adapter/opencode/storage/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::PartKind};
    use tempfile::TempDir;

    const FIXTURES: &str = "tests/fixtures/adapter/opencode/storage";

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_fixture_corpus() -> anyhow::Result<()> {
        let adapter = OpencodeAdapter::new(FIXTURES);
        crate::adapter::test_support::assert_native_restore(
            &OpencodeFactory,
            &adapter,
            // opencode relative paths are rooted at the `storage/` dir.
            std::path::Path::new(FIXTURES),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn opencode_adapter_ingests_fixture_corpus_into_canonical_shape() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = OpencodeAdapter::new(FIXTURES);

        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert!(summary.accepted() > 0, "ingest must accept rows");
        assert_eq!(summary.dropped_events, 0, "no per-event drops expected");
        assert_eq!(
            summary.dropped_sessions, 0,
            "no session-level rejections expected"
        );

        let (sessions, messages, parts) = store.row_counts().await?;
        assert!(sessions > 0, "at least one opencode session");
        assert!(messages > 0, "at least one opencode message");
        assert!(parts > 0, "at least one opencode Part");

        let mut saw_call = false;
        let mut saw_result = false;
        let mut saw_injected_text = false;
        for session_id in store.session_ids().await? {
            let session = store
                .get_session(&session_id)
                .await?
                .expect("session round-trips");
            assert_eq!(session.session.source_agent, NAME);
            assert!(
                !(*session.session.project).is_empty(),
                "spec.md#model-project-non-empty",
            );
            for stored in &session.messages {
                for part in &stored.parts {
                    match &part.kind {
                        PartKind::ToolCall { .. } => saw_call = true,
                        PartKind::ToolResult { .. } => saw_result = true,
                        PartKind::Text { .. } if part.provenance == Provenance::Injected => {
                            saw_injected_text = true;
                        }
                        _ => {}
                    }
                }
            }
        }
        assert!(saw_call, "fused tool parts yield ToolCall on the assistant");
        assert!(
            saw_result,
            "fused tool parts split off a ToolResult on a Tool message",
        );
        assert!(
            saw_injected_text,
            "spec.md#model-part-provenance: synthetic text parts are injected",
        );
        Ok(())
    }

    /// The synthetic `Tool` message a `tool` part splits off must carry a
    /// `Tool` role with one `ToolResult`, and must NOT collide with or
    /// overwrite the assistant message that owns the `ToolCall`.
    #[tokio::test(flavor = "multi_thread")]
    async fn fused_tool_part_splits_into_call_and_result() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = OpencodeAdapter::new(FIXTURES);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        let mut call_ids = std::collections::HashSet::new();
        let mut result_ids = std::collections::HashSet::new();
        for session_id in store.session_ids().await? {
            let session = store
                .get_session(&session_id)
                .await?
                .expect("session round-trips");
            for stored in &session.messages {
                for part in &stored.parts {
                    match &part.kind {
                        PartKind::ToolCall { call_id, .. } => {
                            if let Some(id) = call_id.as_deref() {
                                call_ids.insert(id.clone());
                            }
                        }
                        PartKind::ToolResult { call_id, .. } => {
                            assert!(
                                matches!(stored.message, Message::Tool { .. }),
                                "a ToolResult must live on a Tool-role message",
                            );
                            if let Some(id) = call_id.as_deref() {
                                result_ids.insert(id.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        assert!(!call_ids.is_empty(), "corpus has tool calls");
        assert_eq!(
            call_ids, result_ids,
            "every tool call's id is matched by its split-off result",
        );
        Ok(())
    }
}
