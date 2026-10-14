//! Goose adapter (github.com/aaif-goose/goose).
//!
//! Goose has two storage generations:
//!
//! - **SQLite DB** (current, >= 1.10): `<data-dir>/sessions/sessions.db`
//!   with `sessions` and `messages` tables. Schema copied verbatim from
//!   goose `session_manager.rs`.
//! - **Legacy JSONL** (pre-1.10): `<data-dir>/sessions/<id>.jsonl`.
//!   Line 1 session metadata; lines 2+ messages.
//!
//! Both are read. Session ids present in the DB supersede legacy copies
//! (counted, never silently dropped).
//!
//! Identity: DB session `id` verbatim; message id = `<session_id>:<rowid>`.
//! Legacy message id = own `id` field or `<session>:line<N>`.
//!
//! Source agent taxonomy: `goose` (default/user), `goose/subagent`
//! (sub_agent), `goose/scheduled`, `goose/hidden`, `goose/terminal`,
//! `goose/gateway`, `goose/acp` -- keyed on `session_type` column.
//!
//! Content blocks: `content_json` is a JSON array of typed blocks.
//! Each block dispatches to a canonical PartKind based on `type` field:
//! text -> Text, image -> File, thinking -> Reasoning, toolRequest ->
//! ToolCall, toolResponse -> ToolResult (resolved via per-session name
//! map), toolConfirmationRequest -> ToolApprovalRequest, actionRequired
//! (toolConfirmationResponse) -> ToolApprovalResponse, others ->
//! compact-repr TextPart. Provenance: text/image/thinking are
//! Conversational; everything else is Injected (with userVisible /
//! turnContext override).
//!
//! Timestamps: `created_timestamp` in seconds (or milliseconds above
//! `MILLISECOND_TIMESTAMP_THRESHOLD = 10_000_000_000`). Ordering:
//! `ORDER BY CASE WHEN created_timestamp > 10000000000 THEN
//! created_timestamp/1000 ELSE created_timestamp END, id`.
//!
//! Ingest-only: goose has no file-era format that pond targets.
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::{
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, config_path,
    extract::{Extracted, extract_compact_repr, extract_raw_record, extract_str, json_or_string},
    part_id, part_ordinal, source_options,
    sqlite::{self, CHANNEL_CAP, ColKind, columns_sql, emit, row_to_json},
    validate_path_id,
};

const NAME: &str = "goose";

const MILLISECOND_TIMESTAMP_THRESHOLD: i64 = 10_000_000_000;

const SESSION_COLUMNS: &[(&str, ColKind)] = &[
    ("id", ColKind::Str),
    ("name", ColKind::Str),
    ("description", ColKind::Str),
    ("user_set_name", ColKind::Int),
    ("session_type", ColKind::Str),
    ("working_dir", ColKind::Str),
    ("created_at", ColKind::Str),
    ("updated_at", ColKind::Str),
    ("extension_data", ColKind::Str),
    ("total_tokens", ColKind::Int),
    ("input_tokens", ColKind::Int),
    ("output_tokens", ColKind::Int),
    ("cache_read_tokens", ColKind::Int),
    ("cache_write_tokens", ColKind::Int),
    ("accumulated_total_tokens", ColKind::Int),
    ("accumulated_input_tokens", ColKind::Int),
    ("accumulated_output_tokens", ColKind::Int),
    ("accumulated_cache_read_tokens", ColKind::Int),
    ("accumulated_cache_write_tokens", ColKind::Int),
    ("accumulated_cost", ColKind::Real),
    ("schedule_id", ColKind::Str),
    ("recipe_json", ColKind::Str),
    ("user_recipe_values_json", ColKind::Str),
    ("provider_name", ColKind::Str),
    ("model_config_json", ColKind::Str),
    ("goose_mode", ColKind::Str),
    ("archived_at", ColKind::Str),
    ("project_id", ColKind::Str),
    ("parent_session_id", ColKind::Str),
];

const MESSAGE_COLUMNS: &[(&str, ColKind)] = &[
    ("id", ColKind::Int),
    ("message_id", ColKind::Str),
    ("session_id", ColKind::Str),
    ("role", ColKind::Str),
    ("content_json", ColKind::Str),
    ("created_timestamp", ColKind::Int),
    ("timestamp", ColKind::Str),
    ("tokens", ColKind::Int),
    ("metadata_json", ColKind::Str),
];

// -- Factory ----------------------------------------------------------------

pub struct GooseFactory;

impl AdapterFactory for GooseFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(GooseAdapter::from_config(config)?))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        resolve_data_dir(env).map(|path| json!({ "path": path }))
    }

    fn restore_unsupported(&self) -> Option<&'static str> {
        Some(
            "goose restore is not implemented; goose reads its own \
             sessions.db and legacy JSONL directly",
        )
    }

    fn serialize(
        &self,
        _session: &crate::sessions::SessionWithMessages,
        _fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        Err(AdapterError::schema(
            NAME,
            NAME,
            "goose restore is not implemented",
        ))
    }
}

/// Resolve the goose data dir from the environment:
/// 1. `$GOOSE_PATH_ROOT/data` (absolute only, per goose_paths.rs)
/// 2. `$XDG_DATA_HOME/goose/data` (or `~/.local/share/goose/data`)
/// 3. `~/Library/Application Support/Block/goose/data` (macOS)
///
/// Only offered when the resolved path holds a `sessions/` directory
/// containing `sessions.db` or `*.jsonl`.
fn resolve_data_dir(env: &Env) -> Option<PathBuf> {
    // $GOOSE_PATH_ROOT wins (must be absolute per goose_paths.rs).
    if let Some(root) = std::env::var_os("GOOSE_PATH_ROOT") {
        let root = PathBuf::from(root);
        if root.is_absolute() {
            let data = root.join("data");
            if has_sessions_dir(&data) {
                return Some(data);
            }
        }
    }
    // XDG_DATA_HOME / fallback
    let xdg = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| env.home.join(".local").join("share"));
    let data = xdg.join("goose").join("data");
    if has_sessions_dir(&data) {
        return Some(data);
    }
    // macOS path
    let mac = env
        .home
        .join("Library")
        .join("Application Support")
        .join("Block")
        .join("goose")
        .join("data");
    if has_sessions_dir(&mac) {
        return Some(mac);
    }
    None
}

fn has_sessions_dir(data: &Path) -> bool {
    let sessions = data.join("sessions");
    if !sessions.is_dir() {
        return false;
    }
    // Must have at least a sessions.db or one .jsonl file.
    if sessions.join("sessions.db").is_file() {
        return true;
    }
    has_jsonl_files(&sessions)
}

fn has_jsonl_files(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .any(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        })
        .unwrap_or(false)
}

// -- Adapter ----------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GooseAdapter {
    root: PathBuf,
}

impl GooseAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn from_config(config: Value) -> Result<Self, AdapterError> {
        Ok(Self {
            root: config_path(NAME, config)?,
        })
    }
}

impl Adapter for GooseAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                Ok(enumerate_and_peek(&adapter.root, false).entries.len())
            })
            .await
            .map_err(join_error)?
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let adapter = self.clone();
        Box::pin(stream! {
            let peek = !oracle.is_empty();
            let root = adapter.root.clone();
            let enumerated = tokio::task::spawn_blocking(move || enumerate_and_peek(&root, peek)).await;
            let Enumerated { entries, duplicates, errors } = match enumerated {
                Ok(enumerated) => enumerated,
                Err(join) => { yield Err(join_error(join)); return; }
            };
            for error in errors {
                yield Err(error);
            }
            if duplicates > 0 {
                yield Ok(AdapterYield::SkippedBatch {
                    reason: SkipReason::Superseded,
                    count: duplicates,
                });
            }

            let mut survivors = Vec::with_capacity(entries.len());
            for entry in entries {
                if super::is_session_fresh(oracle, &entry.session_id, entry.source_ts) {
                    yield Ok(AdapterYield::Skipped {
                        session_id: Some(entry.session_id),
                        project: None,
                        reason: SkipReason::Fresh,
                    });
                    continue;
                }
                survivors.push(entry);
            }

            let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
            let root = adapter.root.clone();
            let handle = tokio::task::spawn_blocking(move || read_survivors(&root, survivors, &tx));
            while let Some(item) = rx.recv().await {
                yield item;
            }
            if let Err(join) = handle.await {
                yield Err(join_error(join));
            }
        })
    }
}

// -- Enumeration ------------------------------------------------------------

struct HeadEntry {
    session_id: String,
    source: SessionSource,
    source_ts: Option<i64>,
}

enum SessionSource {
    Db { session_id: String },
    LegacyJsonl { path: PathBuf },
}

struct Enumerated {
    entries: Vec<HeadEntry>,
    duplicates: usize,
    errors: Vec<AdapterError>,
}

fn enumerate_and_peek(root: &Path, peek: bool) -> Enumerated {
    let sessions_dir = root.join("sessions");
    let mut errors = Vec::new();
    let mut db_ids: HashSet<String> = HashSet::new();
    let mut entries = Vec::new();

    // DB side: enumerate session ids and compute watermarks.
    let db_path = sessions_dir.join("sessions.db");
    if db_path.is_file() {
        match open_db(&db_path) {
            Ok(conn) => {
                let ids = match list_session_ids(&conn, &db_path) {
                    Ok(ids) => ids,
                    Err(error) => {
                        errors.push(error);
                        Vec::new()
                    }
                };
                let watermarks = if peek {
                    session_watermarks(&conn).unwrap_or_default()
                } else {
                    HashMap::new()
                };
                for session_id in ids {
                    let source_ts = watermarks.get(&session_id).copied();
                    db_ids.insert(session_id.clone());
                    entries.push(HeadEntry {
                        session_id: session_id.clone(),
                        source: SessionSource::Db { session_id },
                        source_ts,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(path = %db_path.display(), %error, "goose: opening sessions.db failed");
                errors.push(error);
            }
        }
    }

    // Legacy JSONL side: files not superseded by DB.
    let mut jsonl_count = 0usize;
    if let Ok(read) = std::fs::read_dir(&sessions_dir) {
        let mut jsonl_files: Vec<PathBuf> = read
            .flatten()
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
            .map(|entry| entry.path())
            .collect();
        jsonl_files.sort();
        for path in jsonl_files {
            let session_id = match path.file_stem().and_then(|s| s.to_str()) {
                Some(stem) => stem.to_owned(),
                None => continue,
            };
            if db_ids.contains(&session_id) {
                jsonl_count += 1;
                continue;
            }
            let source_ts = if peek {
                peek_jsonl_watermark(&path)
            } else {
                None
            };
            entries.push(HeadEntry {
                session_id,
                source: SessionSource::LegacyJsonl { path },
                source_ts,
            });
        }
    }

    Enumerated {
        entries,
        duplicates: jsonl_count,
        errors,
    }
}

fn list_session_ids(conn: &Connection, db_path: &Path) -> Result<Vec<String>, AdapterError> {
    let mut stmt = conn
        .prepare("SELECT id FROM sessions ORDER BY id")
        .map_err(|error| db_error(db_path, "prepare session list", &error))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| db_error(db_path, "query session list", &error))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| db_error(db_path, "read session id", &error))
}

fn session_watermarks(conn: &Connection) -> Option<HashMap<String, i64>> {
    let mut stmt = conn
        .prepare(
            "SELECT session_id, MAX(CASE WHEN created_timestamp > 10000000000 THEN created_timestamp/1000 ELSE created_timestamp END)
             FROM messages GROUP BY session_id",
        )
        .ok()?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .ok()?;
    let mut map = HashMap::new();
    for row in rows.flatten() {
        if let (session_id, Some(ts)) = row {
            map.insert(session_id, ts * 1_000_000);
        }
    }
    Some(map)
}

fn peek_jsonl_watermark(path: &Path) -> Option<i64> {
    // Read the last non-metadata line for the newest timestamp.
    let content = std::fs::read_to_string(path).ok()?;
    let mut newest: Option<i64> = None;
    for (idx, line) in content.lines().enumerate() {
        if idx == 0 {
            continue; // metadata line
        }
        if let Ok(value) = serde_json::from_str::<Value>(line)
            && let Some(created) = value.get("created").and_then(Value::as_i64)
        {
            let ts = normalize_ts(created);
            if newest.is_none_or(|n| ts > n) {
                newest = Some(ts);
            }
        }
    }
    newest.map(|ts| ts * 1_000_000)
}

// -- Reading ----------------------------------------------------------------

fn read_survivors(
    root: &Path,
    survivors: Vec<HeadEntry>,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) {
    let sessions_dir = root.join("sessions");
    let mut db_conn: Option<Connection> = None;

    for entry in survivors {
        match entry.source {
            SessionSource::Db { session_id } => {
                let db_path = sessions_dir.join("sessions.db");
                if db_conn.is_none() {
                    match open_db(&db_path) {
                        Ok(conn) => db_conn = Some(conn),
                        Err(error) => {
                            let _ = tx.blocking_send(Err(error));
                            continue;
                        }
                    }
                }
                if let Some(conn) = &db_conn {
                    read_db_session(conn, &db_path, &session_id, tx);
                }
            }
            SessionSource::LegacyJsonl { path } => {
                read_legacy_session(&path, &entry.session_id, tx);
            }
        }
    }
}

fn read_db_session(
    conn: &Connection,
    db_path: &Path,
    session_id: &str,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    if let Err(error) = validate_path_id(
        NAME,
        "session id",
        session_id,
        format!("{}#{session_id}", db_path.display()),
    ) {
        return tx.blocking_send(Err(error)).is_ok();
    }

    let row = match fetch_session_row(conn, session_id) {
        Ok(Some(row)) => row,
        Ok(None) => {
            let _ = tx.blocking_send(Err(AdapterError::schema(
                NAME,
                session_id.to_owned(),
                "session row vanished between enumeration and read",
            )));
            return true;
        }
        Err(error) => {
            let _ = tx.blocking_send(Err(error));
            return true;
        }
    };

    let created_at = parse_session_created(&row, session_id, tx);
    let Some(created_at) = created_at else {
        return true;
    };

    let source_agent = classify_source_agent(&row);
    let session = build_db_session(session_id, &row, &source_agent, created_at);
    emit!(tx, Ok(AdapterYield::Event(IngestEvent::Session(session))));

    let messages = match fetch_messages(conn, session_id) {
        Ok(messages) => messages,
        Err(error) => {
            return tx.blocking_send(Err(error)).is_ok();
        }
    };

    // Pre-pass: build tool name map from toolRequest blocks.
    let tool_name_map = build_tool_name_map(&messages);

    for message_row in &messages {
        match db_message_events(session_id, message_row, &tool_name_map) {
            Ok(events) => {
                for event in events {
                    emit!(tx, Ok(AdapterYield::Event(event)));
                }
            }
            Err(error) => emit!(tx, Err(error)),
        }
    }
    true
}

fn fetch_session_row(conn: &Connection, session_id: &str) -> Result<Option<Value>, AdapterError> {
    let sql = format!(
        "SELECT {} FROM sessions WHERE id = ?1",
        columns_sql(SESSION_COLUMNS)
    );
    let mut stmt = conn
        .prepare_cached(&sql)
        .map_err(|error| db_error(Path::new("sessions"), "prepare session row", &error))?;
    stmt.query_row([session_id], |row| row_to_json(row, SESSION_COLUMNS))
        .optional()
        .map_err(|error| db_error(Path::new("sessions"), "query session row", &error))
}

fn fetch_messages(conn: &Connection, session_id: &str) -> Result<Vec<Value>, AdapterError> {
    let sql = format!(
        "SELECT {} FROM messages WHERE session_id = ?1 ORDER BY CASE WHEN created_timestamp > 10000000000 THEN created_timestamp/1000 ELSE created_timestamp END, id",
        columns_sql(MESSAGE_COLUMNS)
    );
    let mut stmt = conn
        .prepare_cached(&sql)
        .map_err(|error| db_error(Path::new("messages"), "prepare messages", &error))?;
    let rows = stmt
        .query_map([session_id], |row| row_to_json(row, MESSAGE_COLUMNS))
        .map_err(|error| db_error(Path::new("messages"), "query messages", &error))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| db_error(Path::new("messages"), "read message row", &error))
}

// -- Session construction ---------------------------------------------------

fn parse_session_created(
    row: &Value,
    session_id: &str,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> Option<DateTime<Utc>> {
    // Try created_at timestamp string first.
    if let Some(dt) = row
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(parse_timestamp_str)
    {
        return Some(dt);
    }
    // Fall back to updated_at.
    if let Some(dt) = row
        .get("updated_at")
        .and_then(Value::as_str)
        .and_then(parse_timestamp_str)
    {
        return Some(dt);
    }
    let _ = tx.blocking_send(Err(AdapterError::schema(
        NAME,
        session_id.to_owned(),
        "session has no parseable created_at or updated_at timestamp",
    )));
    None
}

fn classify_source_agent(row: &Value) -> String {
    let session_type = row
        .get("session_type")
        .and_then(Value::as_str)
        .unwrap_or("user");
    session_type_to_source_agent(session_type)
}

fn session_type_to_source_agent(session_type: &str) -> String {
    match session_type {
        "user" | "" => NAME.to_owned(),
        other => format!("{}/{}", NAME, other.replace('_', "-")),
    }
}

fn build_db_session(
    session_id: &str,
    row: &Value,
    source_agent: &str,
    created_at: DateTime<Utc>,
) -> Session {
    let project = session_project(row, session_id);
    let parent_session_id = row
        .get("parent_session_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    let mut goose_opts = row.as_object().cloned().unwrap_or_default();
    goose_opts.insert(
        "relation".to_owned(),
        json!(if parent_session_id.is_some() {
            "child"
        } else {
            "root"
        }),
    );

    let mut options = source_options(NAME, row);
    options.insert("goose".to_owned(), Value::Object(goose_opts));

    Session {
        id: session_id.to_owned(),
        parent_session_id,
        parent_message_id: None,
        source_agent: source_agent.to_owned(),
        created_at,
        project,
        options,
    }
}

fn session_project(row: &Value, session_id: &str) -> Extracted<String> {
    if let Some(dir) = extract_str(row, "working_dir")
        && !(*dir).is_empty()
    {
        return dir;
    }
    if let Some(pid) = extract_str(row, "project_id")
        && !(*pid).is_empty()
    {
        return pid;
    }
    extract_compact_repr(&Value::String(session_id.to_owned()))
}

// -- Tool name map ----------------------------------------------------------

/// Pre-pass over all message rows to build a `tool_call_id -> tool_name`
/// map from successful toolRequest blocks.
fn build_tool_name_map(messages: &[Value]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for row in messages {
        let content_json = match row.get("content_json").and_then(Value::as_str) {
            Some(json_str) => json_str,
            None => continue,
        };
        let blocks: Vec<Value> = match serde_json::from_str(content_json) {
            Ok(blocks) => blocks,
            Err(_) => continue,
        };
        for block in &blocks {
            if block.get("type").and_then(Value::as_str) != Some("toolRequest") {
                continue;
            }
            let id = match block.get("id").and_then(Value::as_str) {
                Some(id) => id.to_owned(),
                None => continue,
            };
            // Extract tool name from toolCall.value.name on success.
            let name = block
                .pointer("/toolCall/value/name")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            if let Some(name) = name {
                map.insert(id, name);
            }
        }
    }
    map
}

// -- Message -> events (DB) -------------------------------------------------

fn db_message_events(
    session_id: &str,
    row: &Value,
    tool_name_map: &HashMap<String, String>,
) -> Result<Vec<IngestEvent>, AdapterError> {
    let Some(rowid) = row.get("id").and_then(Value::as_i64) else {
        return Err(AdapterError::schema(
            NAME,
            session_id.to_owned(),
            "message row has no integer id",
        ));
    };
    let message_id = format!("{session_id}:{rowid}");
    let timestamp = message_timestamp(row, &message_id)?;
    let role = row.get("role").and_then(Value::as_str);
    let metadata = row
        .get("metadata_json")
        .and_then(Value::as_str)
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or(Value::Null);

    let options = message_options(row, rowid);
    let content_json = row.get("content_json").and_then(Value::as_str);

    let blocks: Vec<Value> = content_json
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    let mut parts = Vec::new();
    let mut ordinal = 0usize;

    let message = match role {
        Some("user") => {
            for block in &blocks {
                if let Some(part) = map_content_block(
                    session_id,
                    &message_id,
                    ordinal,
                    block,
                    &metadata,
                    tool_name_map,
                ) {
                    parts.push(part);
                    ordinal += 1;
                }
            }
            Message::User {
                id: message_id,
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        Some("assistant") => {
            for block in &blocks {
                if let Some(part) = map_content_block(
                    session_id,
                    &message_id,
                    ordinal,
                    block,
                    &metadata,
                    tool_name_map,
                ) {
                    parts.push(part);
                    ordinal += 1;
                }
            }
            Message::Assistant {
                id: message_id,
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        _ => {
            // Non user/assistant role -> System carrier.
            let content = extract_str_from_content(blocks);
            Message::System {
                id: message_id,
                session_id: session_id.to_owned(),
                timestamp,
                content,
                options,
            }
        }
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

/// Map a single content block to a Part, or None for skipped/unsupported types.
fn map_content_block(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    block: &Value,
    metadata: &Value,
    tool_name_map: &HashMap<String, String>,
) -> Option<Part> {
    let block_type = block.get("type").and_then(Value::as_str)?;
    let user_visible = metadata.get("userVisible").and_then(Value::as_bool);
    let turn_context = metadata.get("turnContext").and_then(Value::as_bool);
    let force_injected = user_visible == Some(false) || turn_context == Some(true);

    match block_type {
        "text" => Some(Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance: if force_injected {
                Provenance::Injected
            } else {
                Provenance::Conversational
            },
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: extract_str(block, "text"),
            },
        }),
        "image" => {
            let Some(payload) = block.get("data").and_then(Value::as_str) else {
                // Writer always emits data; absence is corruption. Preserve
                // the block losslessly rather than storing a synthesized
                // empty payload (spec.md#model-no-synthesis).
                return Some(Part {
                    session_id: session_id.to_owned(),
                    id: part_id(message_id, ordinal),
                    message_id: message_id.to_owned(),
                    ordinal: part_ordinal(ordinal),
                    provenance: Provenance::Injected,
                    options: {
                        let mut opts = ProviderOptions::new();
                        opts.insert(
                            "goose".to_owned(),
                            json!({"raw_type": "image", "raw_record": extract_raw_record(block)}),
                        );
                        opts
                    },
                    kind: PartKind::Text {
                        text: Some(extract_compact_repr(block)),
                    },
                });
            };
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: if force_injected {
                    Provenance::Injected
                } else {
                    Provenance::Conversational
                },
                options: ProviderOptions::new(),
                kind: PartKind::File {
                    media_type: extract_str(block, "mimeType").map(|s| (*s).clone()),
                    file_name: None,
                    data: FileData::String(payload.to_owned()),
                },
            })
        }
        "thinking" => {
            let mut opts = ProviderOptions::new();
            if let Some(sig) = extract_str(block, "signature") {
                opts.insert("goose".to_owned(), json!({"signature": *sig}));
            }
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: if force_injected {
                    Provenance::Injected
                } else {
                    Provenance::Conversational
                },
                options: opts,
                kind: PartKind::Reasoning {
                    text: extract_str(block, "thinking"),
                },
            })
        }
        "redactedThinking" => {
            let text = extract_str(block, "data");
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: Provenance::Injected,
                options: ProviderOptions::new(),
                kind: PartKind::Text { text },
            })
        }
        "toolRequest" => {
            let call_id = extract_str(block, "id");
            // On error status: call_id only, name/params absent.
            let status = block.pointer("/toolCall/status").and_then(Value::as_str);
            if status == Some("error") {
                return Some(Part {
                    session_id: session_id.to_owned(),
                    id: part_id(message_id, ordinal),
                    message_id: message_id.to_owned(),
                    ordinal: part_ordinal(ordinal),
                    provenance: Provenance::Injected,
                    options: ProviderOptions::new(),
                    kind: PartKind::ToolCall {
                        call_id,
                        name: None,
                        params: Value::Null,
                        provider_executed: false,
                    },
                });
            }
            let name = block
                .pointer("/toolCall/value/name")
                .and_then(Value::as_str)
                .and_then(|s| extract_str(&json!({"n": s}), "n"));
            let params = block
                .pointer("/toolCall/value/arguments")
                .map(|a| match a {
                    Value::String(text) => json_or_string(text),
                    other => other.clone(),
                })
                .unwrap_or(Value::Null);
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: Provenance::Injected,
                options: ProviderOptions::new(),
                kind: PartKind::ToolCall {
                    call_id,
                    name,
                    params,
                    provider_executed: false,
                },
            })
        }
        "toolResponse" => {
            let call_id = extract_str(block, "id");
            let name = call_id.as_deref().and_then(|id| {
                tool_name_map
                    .get(id)
                    .and_then(|n| extract_str(&json!({"n": n}), "n"))
            });
            let status = block.pointer("/toolResult/status").and_then(Value::as_str);
            let is_failure = status == Some("error");
            let result = if is_failure {
                block
                    .pointer("/toolResult/error")
                    .cloned()
                    .unwrap_or(Value::Null)
            } else {
                block
                    .pointer("/toolResult/value")
                    .cloned()
                    .unwrap_or(Value::Null)
            };
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: Provenance::Injected,
                options: ProviderOptions::new(),
                kind: PartKind::ToolResult {
                    call_id,
                    name,
                    is_failure,
                    result,
                },
            })
        }
        "toolConfirmationRequest" | "actionRequired" => {
            // In goose confirmation records the block `id` (or `data.id`
            // for actionRequired) is the correlation key to the pending
            // tool call, so approval_id and tool_call_id are the same id.
            // A missing id is corruption: carry the whole block losslessly
            // rather than fabricating a literal (spec.md#model-no-synthesis).
            let id = extract_str(block, "id")
                .or_else(|| block.pointer("/data").and_then(|v| extract_str(v, "id")))
                .map(|e| (*e).clone());
            match id {
                Some(id) => Some(Part {
                    session_id: session_id.to_owned(),
                    id: part_id(message_id, ordinal),
                    message_id: message_id.to_owned(),
                    ordinal: part_ordinal(ordinal),
                    provenance: Provenance::Injected,
                    options: ProviderOptions::new(),
                    kind: PartKind::ToolApprovalRequest {
                        approval_id: id.clone(),
                        tool_call_id: id,
                    },
                }),
                None => Some(Part {
                    session_id: session_id.to_owned(),
                    id: part_id(message_id, ordinal),
                    message_id: message_id.to_owned(),
                    ordinal: part_ordinal(ordinal),
                    provenance: Provenance::Injected,
                    options: {
                        let mut opts = ProviderOptions::new();
                        opts.insert(
                            "goose".to_owned(),
                            json!({"raw_type": block_type, "raw_record": extract_raw_record(block)}),
                        );
                        opts
                    },
                    kind: PartKind::Text {
                        text: Some(extract_compact_repr(block)),
                    },
                }),
            }
        }
        "frontendToolRequest" => {
            let call_id = extract_str(block, "id");
            let name = block
                .pointer("/toolCall/value/name")
                .and_then(Value::as_str)
                .and_then(|s| extract_str(&json!({"n": s}), "n"));
            let params = block
                .pointer("/toolCall/value/arguments")
                .map(|a| match a {
                    Value::String(text) => json_or_string(text),
                    other => other.clone(),
                })
                .unwrap_or(Value::Null);
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: Provenance::Injected,
                options: ProviderOptions::new(),
                kind: PartKind::ToolCall {
                    call_id,
                    name,
                    params,
                    provider_executed: false,
                },
            })
        }
        "systemNotification" => {
            let text = extract_str(block, "msg");
            let mut opts = ProviderOptions::new();
            if let Some(ntype) = extract_str(block, "notificationType") {
                opts.insert("goose".to_owned(), json!({"notificationType": *ntype}));
            }
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: Provenance::Injected,
                options: opts,
                kind: PartKind::Text { text },
            })
        }
        "error" => {
            let text = extract_str(block, "message");
            let mut opts = ProviderOptions::new();
            if let Some(kind) = extract_str(block, "kind") {
                opts.insert("goose".to_owned(), json!({"errorKind": *kind}));
            }
            Some(Part {
                session_id: session_id.to_owned(),
                id: part_id(message_id, ordinal),
                message_id: message_id.to_owned(),
                ordinal: part_ordinal(ordinal),
                provenance: Provenance::Injected,
                options: opts,
                kind: PartKind::Text { text },
            })
        }
        // Unknown type -> compact-repr TextPart (forward compat).
        _ => Some(Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance: Provenance::Injected,
            options: {
                let mut opts = ProviderOptions::new();
                opts.insert(
                    "goose".to_owned(),
                    json!({"raw_type": block_type, "raw_record": extract_raw_record(block)}),
                );
                opts
            },
            kind: PartKind::Text {
                text: Some(extract_compact_repr(block)),
            },
        }),
    }
}

fn extract_str_from_content(blocks: Vec<Value>) -> Option<Extracted<String>> {
    for block in &blocks {
        if block.get("type").and_then(Value::as_str) == Some("text") {
            return extract_str(block, "text");
        }
    }
    None
}

// -- Legacy JSONL reading ---------------------------------------------------

fn read_legacy_session(
    path: &Path,
    session_id: &str,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    if let Err(error) = validate_path_id(NAME, "session id", session_id, path.display().to_string())
    {
        return tx.blocking_send(Err(error)).is_ok();
    }

    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) => {
            return tx
                .blocking_send(Err(AdapterError::io(
                    NAME,
                    path.display().to_string(),
                    error,
                )))
                .is_ok();
        }
    };

    let mut lines = content.lines();
    // Line 1: session metadata.
    let meta_line = match lines.next() {
        Some(line) => line,
        None => return true,
    };
    let meta: Value = match serde_json::from_str(meta_line) {
        Ok(value) => value,
        Err(error) => {
            let _ = tx.blocking_send(Err(AdapterError::parse(
                NAME,
                path.display().to_string(),
                1,
                error,
            )));
            return true;
        }
    };

    // Metadata must carry a creation time (either field); a session with
    // neither is corrupt and fails visibly rather than earning a synthesized
    // epoch-0 wall-clock (spec.md#model-no-synthesis).
    let Some(created_at) = meta
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(parse_timestamp_str)
        .or_else(|| {
            meta.get("updated_at")
                .and_then(Value::as_str)
                .and_then(parse_timestamp_str)
        })
    else {
        return tx
            .blocking_send(Err(AdapterError::schema(
                NAME,
                path.display().to_string(),
                "session metadata has no parseable created_at/updated_at timestamp",
            )))
            .is_ok();
    };

    let source_agent = meta
        .get("session_type")
        .and_then(Value::as_str)
        .map(session_type_to_source_agent)
        .unwrap_or_else(|| NAME.to_owned());

    let project = if let Some(dir) = meta.get("working_dir").and_then(Value::as_str) {
        if !dir.is_empty() {
            extract_str(&json!({"working_dir": dir}), "working_dir")
                .unwrap_or_else(|| extract_compact_repr(&Value::String(session_id.to_owned())))
        } else {
            extract_compact_repr(&Value::String(session_id.to_owned()))
        }
    } else {
        extract_compact_repr(&Value::String(session_id.to_owned()))
    };

    let mut goose_opts = meta.as_object().cloned().unwrap_or_default();
    goose_opts.insert("format".to_owned(), json!("legacy_jsonl"));

    let mut options = source_options(NAME, &meta);
    options.insert("goose".to_owned(), Value::Object(goose_opts));

    let session = Session {
        id: session_id.to_owned(),
        parent_session_id: meta
            .get("parent_session_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        parent_message_id: None,
        source_agent,
        created_at,
        project,
        options,
    };
    emit!(tx, Ok(AdapterYield::Event(IngestEvent::Session(session))));

    // Lines 2+: messages.
    for (line_idx, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let msg_value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(error) => {
                let _ = tx.blocking_send(Err(AdapterError::parse(
                    NAME,
                    path.display().to_string(),
                    line_idx + 2,
                    error,
                )));
                continue;
            }
        };

        let msg_id = msg_value
            .get("id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{}:line{}", session_id, line_idx + 1));

        // spec.md#model-no-synthesis sanctions timestamp fallback to the
        // session anchor; no parseable `created` -> use the session's own
        // creation time. The session row always has one (enforced above),
        // so no synthesized epoch-0 wall-clock survives here.
        let timestamp = msg_value
            .get("created")
            .and_then(Value::as_i64)
            .and_then(parse_ts_epoch_seconds)
            .unwrap_or(created_at);

        let role = msg_value.get("role").and_then(Value::as_str);

        // Content may be array, JSON-encoded string, or plain string.
        let blocks = parse_legacy_content(&msg_value);

        let mut parts = Vec::new();
        let mut part_ord = 0usize;

        let message = match role {
            Some("user") => {
                for block in &blocks {
                    let metadata = Value::Null;
                    if let Some(part) = map_content_block(
                        session_id,
                        &msg_id,
                        part_ord,
                        block,
                        &metadata,
                        &HashMap::new(),
                    ) {
                        parts.push(part);
                        part_ord += 1;
                    }
                }
                Message::User {
                    id: msg_id,
                    session_id: session_id.to_owned(),
                    timestamp,
                    options: ProviderOptions::new(),
                }
            }
            Some("assistant") => {
                for block in &blocks {
                    let metadata = Value::Null;
                    if let Some(part) = map_content_block(
                        session_id,
                        &msg_id,
                        part_ord,
                        block,
                        &metadata,
                        &HashMap::new(),
                    ) {
                        parts.push(part);
                        part_ord += 1;
                    }
                }
                Message::Assistant {
                    id: msg_id,
                    session_id: session_id.to_owned(),
                    timestamp,
                    options: ProviderOptions::new(),
                }
            }
            _ => {
                let content = extract_str_from_content(blocks);
                Message::System {
                    id: msg_id,
                    session_id: session_id.to_owned(),
                    timestamp,
                    content,
                    options: ProviderOptions::new(),
                }
            }
        };

        emit!(tx, Ok(AdapterYield::Event(IngestEvent::Message(message))));
        for part in parts {
            emit!(tx, Ok(AdapterYield::Event(IngestEvent::Part(part))));
        }
    }
    true
}

fn parse_legacy_content(row: &Value) -> Vec<Value> {
    match row.get("content") {
        Some(Value::Array(blocks)) => blocks.clone(),
        Some(Value::String(s)) => {
            // JSON-encoded string or plain text.
            if let Ok(Value::Array(blocks)) = serde_json::from_str::<Value>(s) {
                blocks
            } else {
                vec![json!({"type": "text", "text": s})]
            }
        }
        Some(other) => vec![json!({"type": "text", "text": other.to_string()})],
        None => Vec::new(),
    }
}

// -- Timestamp helpers ------------------------------------------------------

fn normalize_ts(ts: i64) -> i64 {
    if ts > MILLISECOND_TIMESTAMP_THRESHOLD {
        ts / 1000
    } else {
        ts
    }
}

fn parse_ts_epoch_seconds(ts: i64) -> Option<DateTime<Utc>> {
    let ts = normalize_ts(ts);
    DateTime::from_timestamp(ts, 0)
}

fn parse_timestamp_str(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .ok()
                .and_then(|naive| Utc.from_local_datetime(&naive).single())
        })
}

fn message_timestamp(row: &Value, message_id: &str) -> Result<DateTime<Utc>, AdapterError> {
    let created = row.get("created_timestamp").and_then(Value::as_i64);
    match created.and_then(parse_ts_epoch_seconds) {
        Some(dt) => Ok(dt),
        None => Err(AdapterError::schema(
            NAME,
            message_id.to_owned(),
            "message has no parseable created_timestamp",
        )),
    }
}

// -- Small helpers ----------------------------------------------------------

fn message_options(row: &Value, rowid: i64) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": NAME,
            "id": rowid,
            "raw_record": extract_raw_record(row),
        }),
    );
    options
}

fn open_db(path: &Path) -> Result<Connection, AdapterError> {
    sqlite::open_db(NAME, path)
}

fn db_error(path: &Path, op: &str, error: &rusqlite::Error) -> AdapterError {
    sqlite::db_error(NAME, path, op, error)
}

fn join_error(join: tokio::task::JoinError) -> AdapterError {
    sqlite::join_error(NAME, join)
}

// -- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use crate::adapter::extracted_text;
    use tempfile::TempDir;

    const GOOSE_SCHEMA: &str = "
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            user_set_name BOOLEAN DEFAULT FALSE,
            session_type TEXT NOT NULL DEFAULT 'user',
            working_dir TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extension_data TEXT DEFAULT '{}',
            total_tokens INTEGER,
            input_tokens INTEGER,
            output_tokens INTEGER,
            cache_read_tokens INTEGER,
            cache_write_tokens INTEGER,
            accumulated_total_tokens INTEGER,
            accumulated_input_tokens INTEGER,
            accumulated_output_tokens INTEGER,
            accumulated_cache_read_tokens INTEGER,
            accumulated_cache_write_tokens INTEGER,
            accumulated_cost REAL,
            schedule_id TEXT,
            recipe_json TEXT,
            user_recipe_values_json TEXT,
            provider_name TEXT,
            model_config_json TEXT,
            goose_mode TEXT NOT NULL DEFAULT 'auto',
            archived_at TIMESTAMP,
            project_id TEXT,
            parent_session_id TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT,
            session_id TEXT NOT NULL REFERENCES sessions(id),
            role TEXT NOT NULL,
            content_json TEXT NOT NULL,
            created_timestamp INTEGER NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tokens INTEGER,
            metadata_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_messages_session_created ON messages(session_id, created_timestamp, id);
    ";

    fn create_db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(GOOSE_SCHEMA).unwrap();
        conn
    }

    fn insert_session(conn: &Connection, id: &str, session_type: &str, working_dir: &str) {
        conn.execute(
            "INSERT INTO sessions (id, session_type, working_dir) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, session_type, working_dir],
        )
        .unwrap();
    }

    fn events(root: &Path) -> Vec<IngestEvent> {
        let (tx, mut rx) = mpsc::channel(1024);
        let enumerated = enumerate_and_peek(root, false);
        let survivors: Vec<HeadEntry> = enumerated.entries;
        std::thread::scope(|scope| {
            scope.spawn(move || read_survivors(root, survivors, &tx));
            let mut out = Vec::new();
            while let Some(item) = rx.blocking_recv() {
                match item.unwrap() {
                    AdapterYield::Event(event) => out.push(event),
                    other => panic!("unexpected non-event yield: {other:?}"),
                }
            }
            out
        })
    }

    #[test]
    fn goose_schema_matches_upstream() {
        let temp = TempDir::new().unwrap();
        let conn = create_db(&temp.path().join("test.db"));
        // Verify tables exist.
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('sessions','messages')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "sessions and messages tables must exist");
        // Verify index.
        let idx: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_session_created'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(idx, 1, "idx_messages_session_created must exist");
    }

    #[test]
    fn probe_default_respects_data_dir() {
        let temp = TempDir::new().unwrap();
        let env = Env::with_home(temp.path());
        // No sessions dir -> None.
        assert!(GooseFactory.probe_default(&env).is_none());
        // sessions dir without db or jsonl -> None.
        let sessions = temp
            .path()
            .join(".local")
            .join("share")
            .join("goose")
            .join("data")
            .join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        assert!(GooseFactory.probe_default(&env).is_none());
        // sessions.db present -> Some.
        std::fs::write(sessions.join("sessions.db"), b"").unwrap();
        let probe = GooseFactory.probe_default(&env);
        let path = probe
            .as_ref()
            .and_then(|v| v.get("path"))
            .and_then(Value::as_str);
        let expected = temp
            .path()
            .join(".local")
            .join("share")
            .join("goose")
            .join("data");
        assert_eq!(path, Some(expected.to_str().unwrap()));
    }

    #[test]
    fn parse_ts_epoch_seconds_normalizes_millis() {
        let above = MILLISECOND_TIMESTAMP_THRESHOLD + 1000;
        let result = parse_ts_epoch_seconds(above);
        assert!(result.is_some());
        // Should have divided by 1000.
        let below = 1_705_312_800;
        let result_below = parse_ts_epoch_seconds(below);
        assert!(result_below.is_some());
    }

    #[test]
    fn timestamp_parser_handles_rfc3339_and_space_separated() {
        let rfc = parse_timestamp_str("2026-01-15T10:00:00Z");
        assert!(rfc.is_some());
        let space = parse_timestamp_str("2026-01-15 10:00:00");
        assert!(space.is_some());
        let bad = parse_timestamp_str("not-a-timestamp");
        assert!(bad.is_none());
    }

    #[test]
    fn session_type_to_brand_taxonomy() {
        assert_eq!(session_type_to_source_agent("user"), "goose");
        assert_eq!(session_type_to_source_agent(""), "goose");
        assert_eq!(session_type_to_source_agent("sub_agent"), "goose/sub-agent");
        assert_eq!(session_type_to_source_agent("scheduled"), "goose/scheduled");
        assert_eq!(session_type_to_source_agent("hidden"), "goose/hidden");
        assert_eq!(session_type_to_source_agent("terminal"), "goose/terminal");
        assert_eq!(session_type_to_source_agent("gateway"), "goose/gateway");
        assert_eq!(session_type_to_source_agent("acp"), "goose/acp");
    }

    #[test]
    fn project_resolution_chain() {
        let with_dir = json!({"working_dir": "/home/user/project", "project_id": null});
        assert_eq!(&*session_project(&with_dir, "s"), "/home/user/project");

        let empty_dir = json!({"working_dir": "", "project_id": "proj-123"});
        assert_eq!(&*session_project(&empty_dir, "s"), "proj-123");

        let no_dir = json!({"project_id": ""});
        // Falls through to compact repr of session id (JSON-encoded string).
        let project = session_project(&no_dir, "my-session");
        assert_eq!(&*project, "\"my-session\"");
    }

    #[test]
    fn content_block_mapping_all_types() {
        let sid = "s1";
        let mid = "s1:1";
        let meta = json!({"userVisible": true});

        // text
        let block = json!({"type": "text", "text": "hello"});
        let part = map_content_block(sid, mid, 0, &block, &meta, &HashMap::new()).unwrap();
        assert!(matches!(part.kind, PartKind::Text { .. }));
        assert_eq!(part.provenance, Provenance::Conversational);

        // thinking
        let block = json!({"type": "thinking", "thinking": "hmm", "signature": "sig"});
        let part = map_content_block(sid, mid, 1, &block, &meta, &HashMap::new()).unwrap();
        assert!(matches!(part.kind, PartKind::Reasoning { .. }));
        assert_eq!(part.provenance, Provenance::Conversational);

        // redactedThinking -> Injected text
        let block = json!({"type": "redactedThinking", "data": "opaque"});
        let part = map_content_block(sid, mid, 2, &block, &meta, &HashMap::new()).unwrap();
        assert!(matches!(part.kind, PartKind::Text { .. }));
        assert_eq!(part.provenance, Provenance::Injected);

        // toolRequest (success)
        let block = json!({"type": "toolRequest", "id": "t1", "toolCall": {"status": "success", "value": {"name": "bash", "arguments": "{}"}}});
        let part = map_content_block(sid, mid, 3, &block, &meta, &HashMap::new()).unwrap();
        assert!(matches!(part.kind, PartKind::ToolCall { .. }));
        assert_eq!(part.provenance, Provenance::Injected);

        // toolRequest (error)
        let block = json!({"type": "toolRequest", "id": "t2", "toolCall": {"status": "error"}});
        let part = map_content_block(sid, mid, 4, &block, &meta, &HashMap::new()).unwrap();
        match &part.kind {
            PartKind::ToolCall { name, params, .. } => {
                assert!(name.is_none());
                assert_eq!(params, &Value::Null);
            }
            _ => panic!("error toolRequest must be ToolCall"),
        }

        // systemNotification
        let block = json!({"type": "systemNotification", "notificationType": "ThinkingMessage", "msg": "thinking..."});
        let part = map_content_block(sid, mid, 5, &block, &meta, &HashMap::new()).unwrap();
        assert!(matches!(part.kind, PartKind::Text { .. }));
        assert_eq!(part.provenance, Provenance::Injected);

        // error
        let block = json!({"type": "error", "kind": "Authentication", "message": "bad creds"});
        let part = map_content_block(sid, mid, 6, &block, &meta, &HashMap::new()).unwrap();
        assert!(matches!(part.kind, PartKind::Text { .. }));
        assert_eq!(part.provenance, Provenance::Injected);
    }

    #[test]
    fn tool_correlation_resolves_name() {
        let mut map = HashMap::new();
        map.insert("toolu-001".to_owned(), "bash".to_owned());
        let call_id = extract_str(&json!({"id": "toolu-001"}), "id");
        let name = call_id
            .as_deref()
            .and_then(|id| map.get(id).and_then(|n| extract_str(&json!({"n": n}), "n")));
        assert_eq!(name.as_ref().map(|e| e.as_str()), Some("bash"));
    }

    #[test]
    fn tool_correlation_missing_name() {
        let map: HashMap<String, String> = HashMap::new();
        let call_id = extract_str(&json!({"id": "toolu-999"}), "id");
        let name = call_id
            .as_deref()
            .and_then(|id| map.get(id).and_then(|n| extract_str(&json!({"n": n}), "n")));
        assert!(name.is_none());
    }

    #[test]
    fn error_status_tool_request() {
        let block = json!({"type": "toolRequest", "id": "t-err", "toolCall": {"status": "error"}});
        let part = map_content_block(
            "s1",
            "s1:1",
            0,
            &block,
            &json!({"userVisible": true}),
            &HashMap::new(),
        )
        .unwrap();
        match &part.kind {
            PartKind::ToolCall {
                call_id,
                name,
                params,
                ..
            } => {
                assert_eq!(extracted_text(call_id), "t-err");
                assert!(name.is_none());
                assert_eq!(params, &Value::Null);
            }
            _ => panic!("must be ToolCall"),
        }
    }

    #[test]
    fn supersession_db_wins() {
        let temp = TempDir::new().unwrap();
        let sessions_dir = temp.path().join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();

        // DB with session "shared-id".
        let conn = create_db(&sessions_dir.join("sessions.db"));
        insert_session(&conn, "shared-id", "user", "/tmp");
        conn.execute(
            "INSERT INTO messages (session_id, role, content_json, created_timestamp) VALUES ('shared-id','user','[{\"type\":\"text\",\"text\":\"from db\"}]',1705312800)",
            [],
        )
        .unwrap();
        drop(conn);

        // Legacy JSONL with same session id.
        std::fs::write(
            sessions_dir.join("shared-id.jsonl"),
            "{\"id\":\"shared-id\",\"description\":\"legacy\",\"working_dir\":\"/tmp\",\"created_at\":\"2026-01-15T10:00:00Z\",\"updated_at\":\"2026-01-15T10:00:00Z\",\"extension_data\":{},\"message_count\":1}\n{\"id\":\"m1\",\"role\":\"user\",\"created\":1705312800,\"content\":[{\"type\":\"text\",\"text\":\"from legacy\"}]}\n",
        )
        .unwrap();

        let enumerated = enumerate_and_peek(temp.path(), false);
        assert_eq!(
            enumerated.duplicates, 1,
            "legacy copy counted as superseded"
        );
        assert_eq!(enumerated.entries.len(), 1, "only DB session ingested");
        assert!(matches!(
            enumerated.entries[0].source,
            SessionSource::Db { .. }
        ));
    }

    #[test]
    fn freshness_watermark_query() {
        let temp = TempDir::new().unwrap();
        let conn = create_db(&temp.path().join("test.db"));
        insert_session(&conn, "s1", "user", "/tmp");
        // Millisecond timestamp (1705312800000 ms -> 1705312800 s after normalization).
        conn.execute(
            "INSERT INTO messages (session_id, role, content_json, created_timestamp) VALUES ('s1','user','[]',1705312800000)",
            [],
        )
        .unwrap();
        // Regular second timestamp (1705312801 s, larger after normalization).
        conn.execute(
            "INSERT INTO messages (session_id, role, content_json, created_timestamp) VALUES ('s1','assistant','[]',1705312801)",
            [],
        )
        .unwrap();
        let watermarks = session_watermarks(&conn).unwrap();
        let ts = watermarks.get("s1").copied().unwrap();
        // MAX(1705312800, 1705312801) = 1705312801 -> *1_000_000
        assert_eq!(ts, 1705312801 * 1_000_000);
    }

    #[test]
    fn legacy_reader_session_metadata() {
        let temp = TempDir::new().unwrap();
        let sessions_dir = temp.path().join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();
        let path = sessions_dir.join("leg01.jsonl");
        std::fs::write(
            &path,
            "{\"id\":\"leg01\",\"description\":\"test\",\"working_dir\":\"/tmp\",\"created_at\":\"2026-01-14T09:00:00Z\",\"updated_at\":\"2026-01-14T09:01:00Z\",\"extension_data\":{},\"message_count\":1}\n{\"id\":\"m1\",\"role\":\"user\",\"created\":1705222800,\"content\":[{\"type\":\"text\",\"text\":\"hi\"}]}\n",
        )
        .unwrap();

        let all = events(temp.path());
        let session = all.iter().find_map(|e| match e {
            IngestEvent::Session(s) => Some(s),
            _ => None,
        });
        assert!(session.is_some());
        let session = session.unwrap();
        assert_eq!(session.id, "leg01");
    }

    #[test]
    fn legacy_reader_string_content() {
        let temp = TempDir::new().unwrap();
        let sessions_dir = temp.path().join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();
        let path = sessions_dir.join("leg02.jsonl");
        std::fs::write(
            &path,
            "{\"id\":\"leg02\",\"description\":\"test\",\"working_dir\":\"/tmp\",\"created_at\":\"2026-01-14T10:00:00Z\",\"updated_at\":\"2026-01-14T10:00:30Z\",\"extension_data\":{},\"message_count\":1}\n{\"id\":\"m1\",\"role\":\"user\",\"created\":1705226400,\"content\":\"Plain string content\"}\n",
        )
        .unwrap();

        let all = events(temp.path());
        let parts: Vec<&Part> = all
            .iter()
            .filter_map(|e| match e {
                IngestEvent::Part(p) => Some(p),
                _ => None,
            })
            .collect();
        assert_eq!(parts.len(), 1);
        assert!(matches!(parts[0].kind, PartKind::Text { .. }));
    }
}
