//! Claude Desktop app adapter - Cowork / agent-mode sessions only.
//!
//! Source path:
//! `~/Library/Application Support/Claude/local-agent-mode-sessions/<acct>/<workspace>/local_<uuid>/audit.jsonl`
//! with a sibling `local_<uuid>.json` metadata file. Each `audit.jsonl` is one
//! session (one JSON object per line); the metadata carries `sessionId`,
//! `createdAt`, the project folder, and UI fields.
//!
//! Scope is deliberately narrow (spec.md#adapters, locked product split):
//! - It NEVER reads `~/.claude/projects` or the `claude-code-sessions/`
//!   wrappers - the Desktop "Code" tab writes there in CLI format and is
//!   covered by `claude-code`.
//! - It NEVER descends into a session's nested `.claude/` directory. That is
//!   the inner Claude Code loop the Cowork sandbox runs; `audit.jsonl` already
//!   represents that conversation, so ingesting the `.claude/projects/**/*.jsonl`
//!   transcripts under it would double-count the same session. Discovery is
//!   scoped to `local_*/audit.jsonl` and the walk prunes hidden dirs, so this
//!   adapter cannot pick up the inner loop. This is why it drives the [`Adapter`]
//!   seam directly rather than through `JsonlTree` (whose blanket `*.jsonl` walk
//!   would find the inner transcripts).
//!
//! The per-line `message` object is the Anthropic Messages shape (the same
//! content blocks claude-code carries), so the part mapping mirrors that
//! adapter. Records that are not a `user`/`assistant` turn (`system`, `result`,
//! `rate_limit_event`, ...) become System carriers: their `subtype`/`type` is
//! the content and the verbatim row survives in `options.source.raw_record`, so
//! native restore reproduces the full `audit.jsonl`. `system/permission_*`
//! records carry no tool-call id to link an approval to, so they stay System
//! carriers rather than fabricate the `tool_call_id` a `ToolApproval*` part
//! requires (spec.md#model-no-synthesis).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use async_stream::stream;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use walkdir::WalkDir;

use crate::{
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, by_timestamp_then_id, compact_json,
    config_path, empty_options,
    extract::{
        Extracted, Source, bound_value, extract_compact_repr, extract_self_str, extract_str,
    },
    extracted_text,
    jsonl::{RECORD_CAP, peek_last_line},
    jsonl_bytes, part_id, part_ordinal, raw_record, source_options,
};

const NAME: &str = "claude-desktop-app";

/// Event-channel bound; doubles as backpressure - the blocking reader parks on
/// `blocking_send` when the consumer lags.
const CHANNEL_CAP: usize = 256;

/// The session-store subpath under `~/Library/Application Support/Claude`.
const SESSIONS_SUBDIR: &str = "local-agent-mode-sessions";

/// Stateless factory: opens [`ClaudeDesktopAppAdapter`] instances and probes for
/// the Cowork store under `~/Library/Application Support/Claude`.
pub struct ClaudeDesktopAppFactory;

impl AdapterFactory for ClaudeDesktopAppFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(ClaudeDesktopAppAdapter::new(config_path(
            NAME, config,
        )?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = cowork_root(&env.home);
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

/// `~/Library/Application Support/Claude/local-agent-mode-sessions`.
fn cowork_root(home: &Path) -> PathBuf {
    home.join("Library")
        .join("Application Support")
        .join("Claude")
        .join(SESSIONS_SUBDIR)
}

/// Configured Cowork reader, rooted at a `local-agent-mode-sessions/` directory.
#[derive(Debug, Clone)]
pub struct ClaudeDesktopAppAdapter {
    root: PathBuf,
}

impl ClaudeDesktopAppAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for ClaudeDesktopAppAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let root = self.root.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || collect_sessions(&root).map(|files| files.len()))
                .await
                .map_err(join_error)?
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let adapter = self.clone();
        Box::pin(stream! {
            let files = {
                let root = adapter.root.clone();
                tokio::task::spawn_blocking(move || collect_sessions(&root)).await
            };
            let files = match files {
                Ok(Ok(files)) => files,
                Ok(Err(error)) => { yield Err(error); return; }
                Err(join) => { yield Err(join_error(join)); return; }
            };

            // Freshness pre-pass: read the audit log's last-message timestamp (its
            // tail line) and skip when it is no newer than pond's watermark. Only
            // when the oracle has entries - a first ingest has nothing to compare.
            let mut survivors = Vec::with_capacity(files.len());
            for file in files {
                if !oracle.is_empty() {
                    let audit = file.audit_path.clone();
                    let last_ts =
                        match tokio::task::spawn_blocking(move || source_last_ts(&audit)).await {
                            Ok(last_ts) => last_ts,
                            Err(join) => { yield Err(join_error(join)); return; }
                        };
                    if crate::adapter::is_session_fresh(oracle, &file.session_id, last_ts) {
                        yield Ok(AdapterYield::Skipped {
                            session_id: Some(file.session_id.clone()),
                            project: None,
                            reason: SkipReason::Fresh,
                        });
                        continue;
                    }
                }
                survivors.push(file);
            }

            let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
            let handle = tokio::task::spawn_blocking(move || read_sessions(survivors, &tx));
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
/// Latest message timestamp (micros) for the freshness gate: the audit log's
/// last record. The audit stream is append-ordered, so its tail line is the
/// latest message; an unreadable file or a record without a timestamp yields
/// `None` and the session re-reads (safe). The sibling metadata file is not
/// consulted - pond never rewrites an existing session row, so a pure-metadata
/// change is a no-op.
fn source_last_ts(audit_path: &Path) -> Option<i64> {
    let last_line = peek_last_line(audit_path)?;
    let record: Value = serde_json::from_str(&last_line).ok()?;
    Some(record_timestamp(&record)?.timestamp_micros())
}

fn join_error(join: tokio::task::JoinError) -> AdapterError {
    AdapterError::io(
        NAME,
        "blocking read task",
        std::io::Error::other(join.to_string()),
    )
}

/// One Cowork session located on disk: its `audit.jsonl`, its sibling metadata
/// file, the session id (the `local_<uuid>` directory name, which equals
/// `metadata.sessionId`), and the session dir relative to the root (for restore).
struct CoworkSession {
    session_id: String,
    audit_path: PathBuf,
    meta_path: PathBuf,
    relative_dir: PathBuf,
}

/// Walk the root for `local_*/audit.jsonl`, pruning hidden directories so the
/// nested `.claude/` inner loop is never reached. Sorted for deterministic
/// ingest order. A missing root means "no sessions yet", not an error.
fn collect_sessions(root: &Path) -> Result<Vec<CoworkSession>, AdapterError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let io = |source| AdapterError::io(NAME, root.display().to_string(), source);
    let mut out = Vec::new();
    let walker = WalkDir::new(root).into_iter().filter_entry(|entry| {
        // Prune any hidden dir (`.claude`, `.audit-key` is a file). The inner
        // Claude Code loop lives under `.claude/`, so this is the structural
        // guard against double-counting it (spec.md#adapters).
        !(entry.file_type().is_dir()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with('.')))
    });
    for entry in walker {
        let entry = entry.map_err(|error| io(error.into()))?;
        if entry.file_name() != "audit.jsonl" {
            continue;
        }
        let audit_path = entry.into_path();
        let Some(dir) = audit_path.parent() else {
            continue;
        };
        let Some(dir_name) = dir.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        // The transcript dir is `local_<uuid>`; its name equals
        // `metadata.sessionId`. A stray `audit.jsonl` elsewhere is not a Cowork
        // session.
        if !dir_name.starts_with("local_") {
            continue;
        }
        let Some(workspace) = dir.parent() else {
            continue;
        };
        let meta_path = workspace.join(format!("{dir_name}.json"));
        let relative_dir = dir.strip_prefix(root).unwrap_or(dir).to_path_buf();
        out.push(CoworkSession {
            session_id: dir_name.to_owned(),
            audit_path,
            meta_path,
            relative_dir,
        });
    }
    out.sort_by(|a, b| a.audit_path.cmp(&b.audit_path));
    Ok(out)
}

fn read_sessions(
    sessions: Vec<CoworkSession>,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) {
    for session in sessions {
        if !read_one_session(session, tx) {
            return;
        }
    }
}

/// Returns `false` when the consumer dropped the receiver and the read should stop.
fn read_one_session(
    file: CoworkSession,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    macro_rules! emit {
        ($item:expr) => {
            if tx.blocking_send($item).is_err() {
                return false;
            }
        };
    }

    let meta = match read_json(&file.meta_path) {
        Ok(value) => value,
        Err(error) => {
            emit!(Err(error));
            return true;
        }
    };
    let session = match build_session(&file, &meta) {
        Ok(session) => session,
        Err(error) => {
            emit!(Err(error));
            return true;
        }
    };
    let created_at = session.created_at;
    let session_id = session.id.clone();
    emit!(Ok(AdapterYield::Event(IngestEvent::Session(session))));

    let bytes = match std::fs::read(&file.audit_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            emit!(Err(AdapterError::io(
                NAME,
                file.audit_path.display().to_string(),
                error
            )));
            return true;
        }
    };
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(_) => {
            emit!(Err(AdapterError::schema(
                NAME,
                file.audit_path.display().to_string(),
                "audit.jsonl is not valid UTF-8",
            )));
            return true;
        }
    };

    let mut tool_call_names: HashMap<String, Extracted<String>> = HashMap::new();
    for (index, line) in text.lines().enumerate() {
        let line_no = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        if line.len() > RECORD_CAP {
            emit!(Err(AdapterError::schema(
                NAME,
                format!("{}:{line_no}", file.audit_path.display()),
                format!(
                    "audit line exceeds adapter record cap: {} bytes > {RECORD_CAP}",
                    line.len()
                ),
            )));
            continue;
        }
        let mut record: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(error) => {
                emit!(Err(AdapterError::parse(
                    NAME,
                    file.audit_path.display().to_string(),
                    line_no,
                    error,
                )));
                continue;
            }
        };
        bound_value(&mut record);
        capture_tool_call_names(&record, &mut tool_call_names);
        match record_events(&session_id, line_no, &record, created_at, &tool_call_names) {
            Ok(events) => {
                for event in events {
                    emit!(Ok(AdapterYield::Event(event)));
                }
            }
            Err(message) => emit!(Err(AdapterError::schema(
                NAME,
                format!("{}:{line_no}", file.audit_path.display()),
                message,
            ))),
        }
    }
    true
}

/// Read one JSON file whole, bounding every string leaf at the seam cap
/// (spec.md#adapter-bounded-values).
fn read_json(path: &Path) -> Result<Value, AdapterError> {
    use std::io::Read;
    let io = |source| AdapterError::io(NAME, path.display().to_string(), source);
    let mut file = std::fs::File::open(path).map_err(io)?;
    let len = file.metadata().map_err(io)?.len();
    if len > RECORD_CAP as u64 {
        return Err(AdapterError::schema(
            NAME,
            path.display().to_string(),
            format!("json file exceeds adapter record cap: {len} bytes > {RECORD_CAP}"),
        ));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    file.read_to_end(&mut bytes).map_err(io)?;
    let mut value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| AdapterError::parse(NAME, path.display().to_string(), 1, error))?;
    bound_value(&mut value);
    Ok(value)
}

fn build_session(file: &CoworkSession, meta: &Value) -> Result<Session, AdapterError> {
    let display = file.meta_path.display().to_string();
    // The `local_<uuid>` dir name is authoritative for the id (it equals
    // `metadata.sessionId`) AND is what the freshness oracle is keyed on in
    // events_with, so use it directly to keep the two in lockstep - a divergence
    // would silently disable the freshness skip. The raw metadata (with its own
    // `sessionId`) is preserved in options.source.raw_record.
    let session_id = file.session_id.clone();

    let created_at = meta
        .get("createdAt")
        .and_then(Value::as_i64)
        .and_then(DateTime::from_timestamp_millis)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                display.clone(),
                "metadata missing numeric `createdAt`",
            )
        })?;

    // spec.md#model-project-non-empty: prefer the real folder the user opened
    // (`userSelectedFolders[0]`), else the sandbox `cwd` (always present). Both
    // are extracted from real source data, never synthesized.
    let project = meta
        .get("userSelectedFolders")
        .and_then(Value::as_array)
        .and_then(|folders| folders.first())
        .filter(|first| first.as_str().is_some_and(|s| !s.is_empty()))
        .and_then(|first| extract_self_str(first))
        .or_else(|| extract_str(meta, "cwd").filter(|cwd| !cwd.trim().is_empty()))
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                display,
                "metadata has neither `userSelectedFolders[0]` nor `cwd` for the project",
            )
        })?;

    let mut options = source_options(NAME, meta);
    if let Some(source) = options.get_mut("source").and_then(Value::as_object_mut) {
        source.insert(
            "relative_dir".to_owned(),
            json!(file.relative_dir.to_string_lossy()),
        );
        for key in [
            "model",
            "title",
            "cliSessionId",
            "systemPrompt",
            "initialMessage",
            "enabledMcpTools",
            "vmProcessName",
            "accountName",
        ] {
            if let Some(value) = meta.get(key) {
                source.insert(key.to_owned(), value.clone());
            }
        }
    }

    Ok(Session {
        id: session_id,
        parent_session_id: None,
        parent_message_id: None,
        source_agent: NAME.to_owned(),
        created_at,
        project,
        options,
    })
}

/// Stash every `tool_use` block's `id -> name` from an assistant record's
/// `message.content[]`, so a later `tool_result` row can resolve its name.
/// Idempotent and safe on any record (non-assistant rows contribute nothing).
fn capture_tool_call_names(record: &Value, map: &mut HashMap<String, Extracted<String>>) {
    let Some(items) = record
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return;
    };
    for item in items {
        if !matches!(
            item.get("type").and_then(Value::as_str),
            Some("tool_use") | Some("server_tool_use")
        ) {
            continue;
        }
        let (Some(id), Some(name)) = (item.str_field("id"), extract_str(item, "name")) else {
            continue;
        };
        map.insert(id.to_owned(), name);
    }
}

/// Map one audit record into canonical events. `user`/`assistant` records carry
/// an inner Anthropic `message`; everything else becomes a System carrier whose
/// verbatim row survives in `options.source.raw_record` for lossless restore.
fn record_events(
    session_id: &str,
    line: usize,
    record: &Value,
    default_timestamp: DateTime<Utc>,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Result<Vec<IngestEvent>, String> {
    let timestamp = record_timestamp(record).unwrap_or(default_timestamp);
    let uuid = record
        .get("uuid")
        .and_then(Value::as_str)
        .map_or_else(|| format!("{session_id}:{line}"), ToOwned::to_owned);
    let rtype = record.get("type").and_then(Value::as_str);

    match rtype {
        Some("user") | Some("assistant") => {
            let message_value = record.get("message").unwrap_or(&Value::Null);
            message_events(
                session_id,
                &uuid,
                timestamp,
                record,
                message_value,
                tool_call_names,
                line,
            )
        }
        // `system` (init/status/api_retry/permission_*), `result`,
        // `rate_limit_event`, `tool_use_summary`, and any future record type
        // are kept as System carriers (spec.md#adapter-integrity-no-silent-drops).
        _ => {
            let content = extract_str(record, "subtype").or_else(|| extract_str(record, "type"));
            Ok(vec![IngestEvent::Message(Message::System {
                id: uuid,
                session_id: session_id.to_owned(),
                timestamp,
                content,
                options: row_options(record, line),
            })])
        }
    }
}

fn record_timestamp(record: &Value) -> Option<DateTime<Utc>> {
    record
        .get("_audit_timestamp")
        .or_else(|| record.get("timestamp"))
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn message_events(
    session_id: &str,
    uuid: &str,
    timestamp: DateTime<Utc>,
    record: &Value,
    message_value: &Value,
    tool_call_names: &HashMap<String, Extracted<String>>,
    line: usize,
) -> Result<Vec<IngestEvent>, String> {
    let role = message_value
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| "message missing role".to_owned())?;
    let content = message_value.get("content").unwrap_or(&Value::Null);
    let mut parts = Vec::new();
    let message = match (role, content) {
        ("user", Value::String(_)) => {
            parts.push(text_part(
                session_id,
                uuid,
                0,
                extract_self_str(content),
                Provenance::Conversational,
            ));
            Message::User {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(record, line),
            }
        }
        ("user", Value::Array(items)) if !items.is_empty() && items.iter().all(is_tool_result) => {
            let source_tool_result = record.get("tool_use_result").cloned();
            parts.extend(items.iter().enumerate().map(|(ordinal, item)| {
                tool_result_part(
                    session_id,
                    uuid,
                    ordinal,
                    item,
                    source_tool_result.as_ref(),
                    tool_call_names,
                )
            }));
            Message::Tool {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(record, line),
            }
        }
        ("user", Value::Array(items)) => {
            parts.extend(items.iter().enumerate().map(|(ordinal, item)| {
                user_part(session_id, uuid, ordinal, item, tool_call_names)
            }));
            Message::User {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(record, line),
            }
        }
        ("assistant", Value::Array(items)) => {
            parts.extend(
                items
                    .iter()
                    .enumerate()
                    .map(|(ordinal, item)| assistant_part(session_id, uuid, ordinal, item)),
            );
            Message::Assistant {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: assistant_options(record, message_value, line),
            }
        }
        _ => {
            return Ok(vec![message_carrier_event(
                session_id, uuid, timestamp, record, line, role,
            )]);
        }
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

fn message_carrier_event(
    session_id: &str,
    uuid: &str,
    timestamp: DateTime<Utc>,
    record: &Value,
    line: usize,
    role: &str,
) -> IngestEvent {
    IngestEvent::Message(Message::System {
        id: uuid.to_owned(),
        session_id: session_id.to_owned(),
        timestamp,
        content: extract_self_str(&Value::String(role.to_owned())),
        options: row_options(record, line),
    })
}

fn text_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    text: Option<Extracted<String>>,
    provenance: Provenance,
) -> Part {
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        provenance,
        options: empty_options(),
        kind: PartKind::Text { text },
    }
}

fn user_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    value: &Value,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Part {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(
            session_id,
            message_id,
            ordinal,
            extract_str(value, "text"),
            Provenance::Conversational,
        ),
        Some("image") | Some("file") => file_part(
            session_id,
            message_id,
            ordinal,
            value,
            Provenance::Conversational,
        ),
        Some("tool_result") => tool_result_part(
            session_id,
            message_id,
            ordinal,
            value,
            None,
            tool_call_names,
        ),
        // Unknown user block: preserve the raw JSON in a Text slot rather than
        // drop it - a lossless encoding, not a synthesized value.
        _ => text_part(
            session_id,
            message_id,
            ordinal,
            Some(extract_compact_repr(value)),
            Provenance::Conversational,
        ),
    }
}

fn assistant_part(session_id: &str, message_id: &str, ordinal: usize, value: &Value) -> Part {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(
            session_id,
            message_id,
            ordinal,
            extract_str(value, "text"),
            Provenance::Conversational,
        ),
        Some("thinking") => Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance: Provenance::Conversational,
            options: signature_options(value),
            kind: PartKind::Reasoning {
                text: extract_str(value, "thinking"),
            },
        },
        Some(kind @ ("tool_use" | "server_tool_use")) => Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance: Provenance::Conversational,
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: extract_str(value, "id"),
                name: extract_str(value, "name"),
                params: value.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: kind == "server_tool_use",
            },
        },
        Some("image") | Some("file") => file_part(
            session_id,
            message_id,
            ordinal,
            value,
            Provenance::Conversational,
        ),
        _ => text_part(
            session_id,
            message_id,
            ordinal,
            Some(extract_compact_repr(value)),
            Provenance::Conversational,
        ),
    }
}

fn tool_result_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    value: &Value,
    source_tool_result: Option<&Value>,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Part {
    let call_id = extract_str(value, "tool_use_id");
    // The name lives on the prior `tool_use`, resolved via the per-session map;
    // a miss surfaces as `None`, never a sentinel (spec.md#model-no-synthesis).
    let name = value
        .str_field("tool_use_id")
        .and_then(|id| tool_call_names.get(id))
        .cloned();
    let result = value
        .get("content")
        .cloned()
        .or_else(|| source_tool_result.cloned())
        .unwrap_or(Value::Null);
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        // spec.md#model-part-provenance: tool output is runtime-produced.
        provenance: Provenance::Injected,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id,
            name,
            is_failure: value
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            result,
        },
    }
}

fn file_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    value: &Value,
    provenance: Provenance,
) -> Part {
    let media_type = value
        .get("media_type")
        .or_else(|| value.get("mime_type"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
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
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        provenance,
        options: empty_options(),
        kind: PartKind::File {
            media_type,
            file_name,
            data,
        },
    }
}

fn row_options(record: &Value, line: usize) -> ProviderOptions {
    let mut options = source_options(NAME, record);
    if let Some(source) = options.get_mut("source").and_then(Value::as_object_mut) {
        source.insert("line".to_owned(), json!(line));
        source.insert("raw_type".to_owned(), json!(record.get("type")));
        if let Some(subtype) = record.get("subtype") {
            source.insert("subtype".to_owned(), subtype.clone());
        }
    }
    options
}

fn assistant_options(record: &Value, message_value: &Value, line: usize) -> ProviderOptions {
    let mut options = row_options(record, line);
    let anthropic = json!({
        "id": message_value.get("id"),
        "model": message_value.get("model"),
        "stop_reason": message_value.get("stop_reason"),
        "usage": message_value.get("usage"),
    });
    options.insert("anthropic".to_owned(), anthropic);
    options
}

fn signature_options(value: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    if let Some(signature) = value.get("signature").and_then(Value::as_str) {
        options.insert("anthropic".to_owned(), json!({ "signature": signature }));
    }
    options
}

fn is_tool_result(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("tool_result")
}

fn serialize_session(
    session: &crate::sessions::SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    // Native restore replays the verbatim `audit.jsonl` rows (each message's
    // stored `raw_record`) in source-line order, plus the metadata file from the
    // session's stored `raw_record`. spec.md#adapter-native-restore-lossless:
    // a session ingested without raw records (foreign-sourced) can't be replayed
    // natively, so we re-enter forcing Foreign (which stamps actual_fidelity).
    let session_raw = raw_record(&session.session.options);
    if fidelity == RestoreFidelity::Native && session_raw.is_none() {
        return serialize_session(session, RestoreFidelity::Foreign);
    }

    let relative_dir = session
        .session
        .options
        .get("source")
        .and_then(|source| source.get("relative_dir"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(&session.session.id));

    let mut messages = session.messages.clone();
    if fidelity == RestoreFidelity::Native {
        messages.sort_by(|left, right| {
            source_line(left.message.options())
                .cmp(&source_line(right.message.options()))
                .then_with(|| by_timestamp_then_id(left, right))
        });
    } else {
        messages.sort_by(by_timestamp_then_id);
    }

    let mut records = Vec::with_capacity(messages.len());
    for message in &messages {
        if fidelity == RestoreFidelity::Native {
            if let Some(raw) = raw_record(message.message.options()) {
                records.push(raw);
            }
            continue;
        }
        if let Some(record) = foreign_record(&session.session.id, message) {
            records.push(record);
        }
    }

    let mut files = vec![RestoredFile::new(
        relative_dir.join("audit.jsonl"),
        jsonl_bytes(NAME, &records)?,
        fidelity,
    )];

    // The metadata sits a level up, named after the session dir: `local_x.json`.
    let meta_value = match fidelity {
        RestoreFidelity::Native => session_raw,
        RestoreFidelity::Foreign => Some(foreign_metadata(session)),
    };
    if let (Some(meta), Some(parent), Some(dir_name)) = (
        meta_value,
        relative_dir.parent(),
        relative_dir.file_name().and_then(|name| name.to_str()),
    ) {
        files.push(RestoredFile::new(
            parent.join(format!("{dir_name}.json")),
            serde_json::to_vec(&meta).map_err(|error| {
                AdapterError::schema(
                    NAME,
                    &session.session.id,
                    format!("json encode failed: {error}"),
                )
            })?,
            fidelity,
        ));
    }
    Ok(files)
}

/// Best-effort metadata for a foreign session: the fields `build_session`
/// reads back, derived from canonical data.
fn foreign_metadata(session: &crate::sessions::SessionWithMessages) -> Value {
    json!({
        "sessionId": session.session.id,
        "createdAt": session.session.created_at.timestamp_millis(),
        "cwd": &*session.session.project,
    })
}

/// Best-effort `audit.jsonl` row for a foreign session, mirroring the Anthropic
/// `message` envelope this adapter reads.
fn foreign_record(session_id: &str, message: &crate::sessions::MessageWithParts) -> Option<Value> {
    let (rtype, role) = match &message.message {
        Message::User { .. } => ("user", "user"),
        Message::Assistant { .. } => ("assistant", "assistant"),
        Message::Tool { .. } => ("user", "user"),
        // System carriers have no idiomatic audit row; content stays canonical.
        Message::System { .. } => return None,
    };
    let content = Value::Array(message.parts.iter().map(audit_part).collect());
    Some(json!({
        "type": rtype,
        "session_id": session_id,
        "uuid": message.message.id(),
        "message": { "role": role, "content": content },
        "_audit_timestamp": message
            .message
            .timestamp()
            .to_rfc3339_opts(SecondsFormat::Millis, true),
    }))
}

fn audit_part(part: &Part) -> Value {
    match &part.kind {
        PartKind::Text { text } => json!({ "type": "text", "text": extracted_text(text) }),
        PartKind::Reasoning { text } => {
            json!({ "type": "thinking", "thinking": extracted_text(text) })
        }
        PartKind::ToolCall {
            call_id,
            name,
            params,
            provider_executed,
        } => json!({
            "type": if *provider_executed { "server_tool_use" } else { "tool_use" },
            "id": extracted_text(call_id),
            "name": extracted_text(name),
            "input": params,
        }),
        PartKind::ToolResult {
            call_id,
            is_failure,
            result,
            ..
        } => json!({
            "type": "tool_result",
            "tool_use_id": extracted_text(call_id),
            "is_error": is_failure,
            "content": result,
        }),
        other => json!({
            "type": "text",
            "text": compact_json(&serde_json::to_value(other).unwrap_or(Value::Null)),
        }),
    }
}

/// Read the stored source line for a message (`options.source.line`), used to
/// replay `audit.jsonl` rows in their original order on native restore.
fn source_line(options: &ProviderOptions) -> Option<u64> {
    options
        .get("source")
        .and_then(|source| source.get("line"))
        .and_then(Value::as_u64)
}

#[cfg(test)]
mod tests {
    //! End-to-end tests over the committed Cowork fixture corpus
    //! (`tests/fixtures/adapter/claude_desktop_app/`), including the regression
    //! guard that the nested `.claude/` inner Claude Code loop is never ingested.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store};
    use tempfile::TempDir;

    // Manifest-dir anchored: unit tests must not depend on the process cwd
    // (figment::Jail chdirs the whole test process while config tests run).
    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/claude_desktop_app/local-agent-mode-sessions"
    );
    /// The inner Claude Code loop transcript nested under one session's
    /// `.claude/projects/`; the adapter must never surface it as a session.
    const INNER_LOOP_ID: &str = "a9985b0b-2f5e-4125-b105-7f62376f5509";

    #[test]
    fn probe_default_finds_cowork_store_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &ClaudeDesktopAppFactory,
            &[
                "Library",
                "Application Support",
                "Claude",
                "local-agent-mode-sessions",
            ],
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ingests_cowork_fixture_into_canonical_shape() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = ClaudeDesktopAppAdapter::new(FIXTURES);
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(summary.dropped_sessions, 0, "no session-level rejections");

        let ids = store.session_ids().await?;
        // Four `audit.jsonl` sessions; the nested `.claude/**/*.jsonl` inner loop
        // must NOT become a fifth session (spec.md#adapters double-count guard).
        assert_eq!(ids.len(), 4, "exactly the four audit.jsonl sessions");
        assert!(
            !ids.iter().any(|id| id.contains(INNER_LOOP_ID)),
            "the nested inner Claude Code loop must not be ingested as a session",
        );
        for id in &ids {
            assert!(
                id.starts_with("local_"),
                "session id is the metadata sessionId (local_<uuid>): {id}",
            );
        }

        let mut saw_call = false;
        let mut saw_resolved_result = false;
        let mut saw_reasoning = false;
        let mut saw_system = false;
        for id in &ids {
            let session = store.get_session(id).await?.expect("session round-trips");
            assert_eq!(session.session.source_agent, NAME);
            assert!(
                !(*session.session.project).is_empty(),
                "spec.md#model-project-non-empty",
            );
            for stored in &session.messages {
                if matches!(stored.message, Message::System { .. }) {
                    saw_system = true;
                }
                for part in &stored.parts {
                    match &part.kind {
                        PartKind::ToolCall { .. } => saw_call = true,
                        PartKind::ToolResult { name, .. } if name.is_some() => {
                            saw_resolved_result = true;
                        }
                        PartKind::Reasoning { .. } => saw_reasoning = true,
                        _ => {}
                    }
                }
            }
        }
        assert!(saw_call, "assistant tool_use -> ToolCall");
        assert!(
            saw_resolved_result,
            "tool_result name resolved via the per-session tool_use map",
        );
        assert!(saw_reasoning, "assistant thinking -> Reasoning");
        assert!(
            saw_system,
            "system/result/... records become System carriers"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_round_trips() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path().join("store")).await?;
        let adapter = ClaudeDesktopAppAdapter::new(FIXTURES);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        let original = store.session_ids().await?;

        // Native restore replays each session's audit.jsonl + metadata; collect
        // all files (distinct dirs, no collision) then write the tree once.
        let mut files = Vec::new();
        for id in &original {
            let session = store.get_session(id).await?.expect("round-trips");
            files.extend(ClaudeDesktopAppFactory.serialize(&session, RestoreFidelity::Native)?);
        }
        let restore_root = temp.path().join("restore");
        crate::adapter::write_restored_files(&restore_root, files)?;

        let restore_store = Store::open_local(temp.path().join("restore-store")).await?;
        let restored = ClaudeDesktopAppAdapter::new(&restore_root);
        ingest_adapter(
            &restore_store,
            &restored,
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(
            restore_store.session_ids().await?.len(),
            original.len(),
            "native restore re-ingests as the same session set",
        );
        Ok(())
    }

    #[test]
    fn unexpected_message_content_becomes_lossless_carrier() {
        let names = HashMap::new();
        let record = json!({
            "type": "user",
            "uuid": "local-message-1",
            "_audit_timestamp": "2026-06-01T00:00:00Z",
            "message": {
                "role": "user",
                "content": null,
            },
        });

        let events = record_events("local_session", 7, &record, Utc::now(), &names)
            .expect("carrier is valid");
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            IngestEvent::Message(Message::System { id, content, .. })
                if id == "local-message-1" && content.as_deref().map(String::as_str) == Some("user")
        ));
    }
}
