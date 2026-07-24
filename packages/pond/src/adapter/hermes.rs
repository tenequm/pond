//! Hermes Agent adapter (github.com/NousResearch/hermes-agent).
//!
//! Hermes (Python) persists everything in ONE SQLite database per profile - no
//! JSONL, no per-session files. The default home is `~/.hermes` (env override
//! `$HERMES_HOME`, which may point anywhere - Docker installs use paths like
//! `/opt/data`). The default profile's DB is `<home>/state.db`; named profiles
//! live at `<home>/profiles/<name>/state.db`, each an independent DB. The
//! adapter enumerates both. Hermes sanctions cross-process reads via a
//! `mode=ro` URI, which is exactly what [`sqlite::open_db`] does.
//!
//! Identity (plan 3.1): pond `session_id` = `sessions.id` verbatim; message id =
//! `<session_id>:<messages.id>`. `messages.id` is SQLite `AUTOINCREMENT`, so it
//! is never reused - a deleted id never comes back carrying different content,
//! which makes additive sync safe even though hermes rewrites history in place
//! (compaction, rewind, `/retry` delete+reinsert). Rewrites appear to pond as
//! NEW rows (higher ids stamped with the current time); pond keeps the
//! superseded rows as history - a superset of the source, not a mirror. The
//! `active`/`compacted`/`observed` flags are snapshots at ingest time recorded
//! in per-message options; pond does not track later flag flips.
//!
//! `project` = `session_key` when present, else `<source>:<chat_id>`, else
//! `cwd`, else `source` (`source` is NOT NULL, so a value always resolves) -
//! every component a verbatim source field routed through the seam.
//! `source_agent` = `hermes`, `hermes/cron` (for `source='cron'`), or
//! `hermes/subagent` (delegate/spawn children). Compression-fork and branch
//! children stay plain `hermes` (they ARE the conversation continuing).
//!
//! Lineage (plan 3.1): `parent_session_id` verbatim, plus a `relation` tag in
//! `options.hermes` derived from the parent's `end_reason` and the child's
//! `model_config._branched_from` marker - `branch`, `compaction_successor`, or
//! `spawn` (hermes's own un-conflated edge kinds, `hermes_state.py` lines
//! 74-103). `parent_message_id` stays `None` (hermes tracks no cut point).
//!
//! Content encoding (`_decode_content`, `hermes_state.py` ~5567): `content` is a
//! plain string OR a JSON payload prefixed with the sentinel `"\x00json:"` (a NUL
//! byte then `json:`, illegal in normal text so it cannot collide). Stripped and
//! parsed it recovers a multimodal part list of text and image_url items; a
//! decode failure falls back to the raw string, matching hermes.
//!
//! Ordering: messages are read `ORDER BY id`. `timestamp` is non-monotonic by
//! design (every hermes read path orders by id); pond re-sorts canonically by
//! `(timestamp, id)` and keeps the source `id` in `options.source.id` so the
//! append order survives. The freshness watermark is `MAX(timestamp)` over the
//! session's messages (read once per DB via one grouped query), never a
//! `versions()`-style scan.
//!
//! Documented non-ingest (per-adapter contract): `kanban.db`, the legacy
//! `sessions/sessions.json` routing index, `cron/`, `checkpoints/` (a git object
//! store of file snapshots), `logs/`, the memory plugins (mem0 / hindsight /
//! redis), the FTS shadow tables inside `state.db`, and every JSONL side-channel
//! that DUPLICATES conversation content on disk (`moa-traces/*.jsonl`,
//! `trajectory_samples.jsonl`, `failed_trajectories.jsonl`, the trace-upload
//! `sessions/<id>.jsonl`, and `hermes sessions export` output) - `state.db` is
//! canonical.
//!
//! Restore (plan 3.3): hermes has no file-era format to target, so `serialize`
//! emits idiomatic Foreign NDJSON of the reconstructed `sessions` + `messages`
//! rows (the sanctioned fallback); native is not offered and the CLI is told so
//! via `actual_fidelity: Foreign`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use crate::{
    sessions::{IngestEvent, MessageWithParts, SessionWithMessages},
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, by_timestamp_then_id,
    extract::{Extracted, extract_compact_repr, extract_raw_record, extract_str, json_or_string},
    extracted_text, is_session_fresh, jsonl_bytes, part_id, part_ordinal, raw_record,
    source_options,
    sqlite::{self, CHANNEL_CAP, ColKind, columns_sql, emit, row_to_json},
    validate_path_id,
};

const NAME: &str = "hermes";

/// Multimodal content sentinel: a NUL byte + `json:` prefixes a JSON payload
/// (`_CONTENT_JSON_PREFIX`, `hermes_state.py` ~5530). The NUL is illegal in
/// normal text, so this never collides with real user content.
const CONTENT_JSON_PREFIX: &str = "\u{0}json:";

/// The `sessions` columns pond mirrors verbatim into `options.hermes`, in SELECT
/// order. This ONE list drives both the SELECT and the row->JSON decode, so
/// tracking hermes's schema is a one-line change (the openclaw precedent). The
/// billing/cost/handoff columns are intentionally omitted - they are operational
/// bookkeeping, not conversation provenance.
const SESSION_COLUMNS: &[(&str, ColKind)] = &[
    ("id", ColKind::Str),
    ("source", ColKind::Str),
    ("user_id", ColKind::Str),
    ("session_key", ColKind::Str),
    ("chat_id", ColKind::Str),
    ("chat_type", ColKind::Str),
    ("thread_id", ColKind::Str),
    ("display_name", ColKind::Str),
    ("origin_json", ColKind::Str),
    ("model", ColKind::Str),
    ("model_config", ColKind::Str),
    ("system_prompt", ColKind::Str),
    ("parent_session_id", ColKind::Str),
    ("started_at", ColKind::Real),
    ("ended_at", ColKind::Real),
    ("end_reason", ColKind::Str),
    ("message_count", ColKind::Int),
    ("tool_call_count", ColKind::Int),
    ("input_tokens", ColKind::Int),
    ("output_tokens", ColKind::Int),
    ("cache_read_tokens", ColKind::Int),
    ("cache_write_tokens", ColKind::Int),
    ("reasoning_tokens", ColKind::Int),
    ("cwd", ColKind::Str),
    ("git_branch", ColKind::Str),
    ("git_repo_root", ColKind::Str),
    ("title", ColKind::Str),
    ("profile_name", ColKind::Str),
    ("rewind_count", ColKind::Int),
    ("archived", ColKind::Int),
];

/// The `messages` columns pond reads. The whole row is the per-message
/// `raw_record` (native-restore fidelity); the metadata columns also mirror into
/// `options.hermes` (spec 6.5 rule 2). One list, both purposes.
const MESSAGE_COLUMNS: &[(&str, ColKind)] = &[
    ("id", ColKind::Int),
    ("session_id", ColKind::Str),
    ("role", ColKind::Str),
    ("content", ColKind::Str),
    ("tool_call_id", ColKind::Str),
    ("tool_calls", ColKind::Str),
    ("tool_name", ColKind::Str),
    ("effect_disposition", ColKind::Str),
    ("timestamp", ColKind::Real),
    ("token_count", ColKind::Int),
    ("finish_reason", ColKind::Str),
    ("reasoning", ColKind::Str),
    ("reasoning_content", ColKind::Str),
    ("reasoning_details", ColKind::Str),
    ("codex_reasoning_items", ColKind::Str),
    ("codex_message_items", ColKind::Str),
    ("platform_message_id", ColKind::Str),
    ("observed", ColKind::Int),
    ("active", ColKind::Int),
    ("compacted", ColKind::Int),
    ("api_content", ColKind::Str),
];

/// Per-message metadata columns lifted into `options.hermes` (spec 6.5 rule 2 -
/// real turn data that is not a canonical field). The JSON-text ones are parsed
/// so they land as structure, not an escaped string.
const MESSAGE_META_COLUMNS: &[&str] = &[
    "token_count",
    "finish_reason",
    "platform_message_id",
    "observed",
    "active",
    "compacted",
    "effect_disposition",
    "tool_call_id",
    "tool_name",
    "api_content",
    "reasoning_details",
    "codex_reasoning_items",
    "codex_message_items",
];

const JSON_TEXT_META: &[&str] = &[
    "reasoning_details",
    "codex_reasoning_items",
    "codex_message_items",
];

/// Stateless factory: opens [`HermesAdapter`] instances and probes for the
/// canonical `~/.hermes` (or `$HERMES_HOME`) home holding a `state.db`.
pub struct HermesFactory;

impl AdapterFactory for HermesFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(HermesAdapter::from_config(config)?))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        // `$HERMES_HOME` wins (it may point anywhere - Docker installs live
        // outside `~`), then `~/.hermes`. A home only qualifies when it actually
        // holds a `state.db`, so an empty dir never masquerades as a source.
        let override_dir = std::env::var_os("HERMES_HOME").map(PathBuf::from);
        resolve_home(&env.home, override_dir.as_deref()).map(|home| json!({ "path": home }))
    }

    fn serialize(
        &self,
        session: &SessionWithMessages,
        fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        serialize_session(session, fidelity)
    }
}

/// Resolve the hermes home for auto-discovery: the first of `override_dir` then
/// `~/.hermes` that exists and contains a `state.db`.
fn resolve_home(home: &Path, override_dir: Option<&Path>) -> Option<PathBuf> {
    let candidates = [
        override_dir.map(Path::to_path_buf),
        Some(home.join(".hermes")),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|root| root.join("state.db").is_file())
}

/// Configured hermes reader, rooted at the home dir (holding `state.db` and
/// optional `profiles/<name>/state.db`).
#[derive(Debug, Clone)]
pub struct HermesAdapter {
    root: PathBuf,
}

impl HermesAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn from_config(config: Value) -> Result<Self, AdapterError> {
        Ok(Self {
            root: super::config_path(NAME, config)?,
        })
    }
}

impl Adapter for HermesAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                Ok(enumerate_and_peek(&adapter, false).entries.len())
            })
            .await
            .map_err(join_error)?
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let adapter = self.clone();
        Box::pin(stream! {
            let peek = !oracle.is_empty();
            let enum_adapter = adapter.clone();
            let enumerated = tokio::task::spawn_blocking(move || enumerate_and_peek(&enum_adapter, peek)).await;
            let Enumerated { entries, errors } = match enumerated {
                Ok(enumerated) => enumerated,
                Err(join) => { yield Err(join_error(join)); return; }
            };
            // Per-DB enumeration failures surface as visible errors; the run
            // continues with survivors (spec.md#adapter-integrity-no-silent-drops).
            for error in errors {
                yield Err(error);
            }

            let mut survivors = Vec::with_capacity(entries.len());
            for entry in entries {
                if is_session_fresh(oracle, &entry.session_id, entry.source_ts) {
                    yield Ok(AdapterYield::Skipped {
                        session_id: Some(entry.session_id),
                        project: None,
                        reason: SkipReason::Fresh,
                    });
                    continue;
                }
                survivors.push((entry.db_path, entry.session_id));
            }

            let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
            let handle = tokio::task::spawn_blocking(move || read_survivors(survivors, &tx));
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
    db_path: PathBuf,
    session_id: String,
    source_ts: Option<i64>,
}

struct Enumerated {
    entries: Vec<HeadEntry>,
    errors: Vec<AdapterError>,
}

/// The `state.db` databases under the home: the default profile at
/// `<home>/state.db` plus every `<home>/profiles/<name>/state.db`.
fn list_dbs(root: &Path) -> Vec<PathBuf> {
    let mut dbs = Vec::new();
    let default = root.join("state.db");
    if default.is_file() {
        dbs.push(default);
    }
    if let Ok(read) = std::fs::read_dir(root.join("profiles")) {
        let mut profile_dbs: Vec<PathBuf> = read
            .flatten()
            .filter(|entry| entry.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|entry| entry.path().join("state.db"))
            .filter(|db| db.is_file())
            .collect();
        profile_dbs.sort();
        dbs.extend(profile_dbs);
    }
    dbs
}

fn enumerate_and_peek(adapter: &HermesAdapter, peek: bool) -> Enumerated {
    let mut entries = Vec::new();
    let mut errors = Vec::new();

    for db_path in list_dbs(&adapter.root) {
        let conn = match open_db(&db_path) {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(path = %db_path.display(), %error, "hermes: opening state.db failed");
                errors.push(error);
                continue;
            }
        };
        let session_ids = match list_session_ids(&conn, &db_path) {
            Ok(ids) => ids,
            Err(error) => {
                tracing::warn!(path = %db_path.display(), %error, "hermes: listing sessions failed");
                errors.push(error);
                continue;
            }
        };
        // One grouped read yields every session's watermark (indexed via
        // idx_messages_session); never a per-session scan.
        let watermarks = if peek {
            session_watermarks(&conn).unwrap_or_default()
        } else {
            HashMap::new()
        };
        for session_id in session_ids {
            let source_ts = watermarks.get(&session_id).copied();
            entries.push(HeadEntry {
                db_path: db_path.clone(),
                session_id,
                source_ts,
            });
        }
    }

    Enumerated { entries, errors }
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

/// One grouped query for every session's `MAX(timestamp)` (micros). `timestamp`
/// is `REAL` epoch seconds; the peek is the sole freshness signal.
fn session_watermarks(conn: &Connection) -> Option<HashMap<String, i64>> {
    let mut stmt = conn
        .prepare("SELECT session_id, MAX(timestamp) FROM messages GROUP BY session_id")
        .ok()?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
        })
        .ok()?;
    let mut map = HashMap::new();
    for row in rows.flatten() {
        if let (session_id, Some(secs)) = row {
            map.insert(session_id, secs_to_micros(secs));
        }
    }
    Some(map)
}

// -- Reading ----------------------------------------------------------------

fn read_survivors(
    survivors: Vec<(PathBuf, String)>,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) {
    let mut conns: HashMap<PathBuf, Connection> = HashMap::new();
    for (db_path, session_id) in survivors {
        let keep = match connection(&mut conns, &db_path) {
            Ok(conn) => read_session(conn, &db_path, &session_id, tx),
            Err(error) => tx.blocking_send(Err(error)).is_ok(),
        };
        if !keep {
            return;
        }
    }
}

fn read_session(
    conn: &Connection,
    db_path: &Path,
    session_id: &str,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    // A hostile `sessions.id` becomes a restore filename (`serialize_session`);
    // it fails here at ingest, typed and attributed, like opencode's DB ids.
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
            let error = AdapterError::schema(
                NAME,
                session_id.to_owned(),
                "session row vanished between enumeration and read",
            );
            return tx.blocking_send(Err(error)).is_ok();
        }
        Err(error) => return tx.blocking_send(Err(error)).is_ok(),
    };

    // `started_at` is NOT NULL in the source schema; a session missing it is
    // corrupt and fails visibly rather than earning a synthesized wall-clock
    // `created_at` (spec.md#model-no-synthesis).
    let Some(created_at) = row
        .get("started_at")
        .and_then(Value::as_f64)
        .and_then(secs_to_dt)
    else {
        let error = AdapterError::schema(
            NAME,
            session_id.to_owned(),
            "session has no parseable started_at timestamp",
        );
        return tx.blocking_send(Err(error)).is_ok();
    };

    let (relation, source_agent) = classify(conn, &row);
    let session = build_session(session_id, &row, relation, source_agent, created_at);
    emit!(tx, Ok(AdapterYield::Event(IngestEvent::Session(session))));

    let messages = match fetch_messages(conn, session_id) {
        Ok(messages) => messages,
        Err(error) => return tx.blocking_send(Err(error)).is_ok(),
    };
    for message_row in messages {
        match message_events(session_id, &message_row) {
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
        "SELECT {} FROM messages WHERE session_id = ?1 ORDER BY id",
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

/// The parent's `end_reason`, used to classify a child's lineage. Absent parent
/// row / column -> `None` (degrades to a `spawn` classification).
fn parent_end_reason(conn: &Connection, parent_id: &str) -> Option<String> {
    let mut stmt = conn
        .prepare_cached("SELECT end_reason FROM sessions WHERE id = ?1")
        .ok()?;
    stmt.query_row([parent_id], |row| row.get::<_, Option<String>>(0))
        .ok()
        .flatten()
}

// -- Session construction ---------------------------------------------------

#[derive(Clone, Copy)]
enum Relation {
    Compaction,
    Spawn,
    Branch,
}

impl Relation {
    fn tag(self) -> &'static str {
        match self {
            Self::Compaction => "compaction_successor",
            Self::Spawn => "spawn",
            Self::Branch => "branch",
        }
    }
}

/// Derive the lineage relation + `source_agent` from the session row and its
/// parent (`hermes_state.py` lines 74-103): a `_branched_from` marker or a
/// parent that ended `end_reason='branched'` is a `/branch`; a parent that ended
/// `end_reason='compression'` is a compaction successor; any other parented row
/// is a delegate/subagent spawn. `source='cron'` overrides to `hermes/cron`;
/// only spawns become `hermes/subagent` - branch and compaction children ARE the
/// conversation continuing, so they stay plain `hermes`.
fn classify(conn: &Connection, row: &Value) -> (Option<Relation>, String) {
    let source = row.get("source").and_then(Value::as_str);
    let relation = row
        .get("parent_session_id")
        .and_then(Value::as_str)
        .map(|parent_id| {
            let parent_end = parent_end_reason(conn, parent_id);
            let branched_marker = model_config_has(row, "_branched_from");
            if branched_marker || parent_end.as_deref() == Some("branched") {
                Relation::Branch
            } else if parent_end.as_deref() == Some("compression") {
                Relation::Compaction
            } else {
                Relation::Spawn
            }
        });
    let source_agent = if source == Some("cron") {
        format!("{NAME}/cron")
    } else if matches!(relation, Some(Relation::Spawn)) {
        format!("{NAME}/subagent")
    } else {
        NAME.to_owned()
    };
    (relation, source_agent)
}

/// Whether the session's `model_config` JSON carries `key` as a non-null value.
fn model_config_has(row: &Value, key: &str) -> bool {
    row.get("model_config")
        .and_then(Value::as_str)
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .and_then(|cfg| cfg.get(key).cloned())
        .is_some_and(|value| !value.is_null())
}

fn build_session(
    session_id: &str,
    row: &Value,
    relation: Option<Relation>,
    source_agent: String,
    created_at: DateTime<Utc>,
) -> Session {
    let project = session_project(row, session_id);
    let parent_session_id = row
        .get("parent_session_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    let mut hermes = row.as_object().cloned().unwrap_or_default();
    if let Some(relation) = relation {
        hermes.insert("relation".to_owned(), json!(relation.tag()));
    }

    let mut options = source_options(NAME, row);
    options.insert("hermes".to_owned(), Value::Object(hermes));

    Session {
        id: session_id.to_owned(),
        parent_session_id,
        parent_message_id: None,
        source_agent,
        created_at,
        project,
        options,
    }
}

/// `project` = `session_key`, else `<source>:<chat_id>`, else `cwd`, else
/// `source` - every candidate a verbatim source field routed through the seam
/// (spec.md#model-project-non-empty, spec.md#model-no-synthesis). `source` is
/// NOT NULL, so the final fallback always resolves; the compact-repr tail is
/// dead and only keeps the value total.
fn session_project(row: &Value, session_id: &str) -> Extracted<String> {
    if let Some(key) = extract_str(row, "session_key") {
        return key;
    }
    if let (Some(source), Some(chat_id)) = (
        row.get("source").and_then(Value::as_str),
        row.get("chat_id").and_then(Value::as_str),
    ) {
        let composite = format!("{source}:{chat_id}");
        if let Some(project) = extract_str(&json!({ "project": composite }), "project") {
            return project;
        }
    }
    extract_str(row, "cwd")
        .or_else(|| extract_str(row, "source"))
        .unwrap_or_else(|| extract_compact_repr(&Value::String(session_id.to_owned())))
}

// -- Message -> events ------------------------------------------------------

fn message_events(session_id: &str, row: &Value) -> Result<Vec<IngestEvent>, AdapterError> {
    // `id` and `timestamp` are both NOT NULL in the source schema; a row missing
    // either is corrupt, so it fails visibly rather than being dropped or given a
    // synthesized wall-clock timestamp (spec.md#model-no-synthesis,
    // spec.md#adapter-integrity-no-silent-drops).
    let Some(message_id_int) = row.get("id").and_then(Value::as_i64) else {
        return Err(AdapterError::schema(
            NAME,
            session_id.to_owned(),
            "message row has no integer id",
        ));
    };
    let message_id = format!("{session_id}:{message_id_int}");
    let Some(timestamp) = row
        .get("timestamp")
        .and_then(Value::as_f64)
        .and_then(secs_to_dt)
    else {
        return Err(AdapterError::schema(
            NAME,
            message_id,
            "message row has no parseable timestamp",
        ));
    };
    let options = message_options(row, message_id_int);
    let role = row.get("role").and_then(Value::as_str);
    let content = row.get("content").and_then(Value::as_str);

    let mut parts = Vec::new();
    let mut ordinal = 0usize;

    let message = match role {
        Some("user") => {
            if let Some(content) = content {
                content_parts(
                    session_id,
                    &message_id,
                    &mut ordinal,
                    content,
                    Provenance::Conversational,
                    &mut parts,
                );
            }
            Message::User {
                id: message_id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        Some("assistant") => {
            if let Some(content) = content {
                content_parts(
                    session_id,
                    &message_id,
                    &mut ordinal,
                    content,
                    Provenance::Conversational,
                    &mut parts,
                );
            }
            for key in ["reasoning", "reasoning_content"] {
                if let Some(text) = extract_str(row, key) {
                    parts.push(reasoning_part(session_id, &message_id, ordinal, text));
                    ordinal += 1;
                }
            }
            match tool_calls(row) {
                ToolCalls::Parsed(calls) => {
                    for call in calls {
                        parts.push(tool_call_part(session_id, &message_id, ordinal, &call));
                        ordinal += 1;
                    }
                }
                ToolCalls::Corrupt(raw) => {
                    let text = extract_str(&json!({ "content": raw }), "content");
                    parts.push(text_part(
                        session_id,
                        &message_id,
                        ordinal,
                        text,
                        Provenance::Conversational,
                    ));
                }
            }
            Message::Assistant {
                id: message_id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        Some("tool") => {
            parts.push(tool_result_part(session_id, &message_id, row, content));
            Message::Tool {
                id: message_id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        // `system` and any unknown role -> System carrier: the content survives
        // as text, the whole row in options; nothing is dropped
        // (spec.md#adapter-integrity-no-silent-drops).
        _ => Message::System {
            id: message_id.clone(),
            session_id: session_id.to_owned(),
            timestamp,
            content: content.and_then(decoded_text),
            options,
        },
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

/// Decode a `content` string into conversational parts. A sentinel-prefixed JSON
/// array is a multimodal part list; any other JSON is preserved as one compact
/// Text part; a plain string is one Text part.
fn content_parts(
    session_id: &str,
    message_id: &str,
    ordinal: &mut usize,
    raw: &str,
    provenance: Provenance,
    parts: &mut Vec<Part>,
) {
    match decode_content(raw) {
        Decoded::Text(text) => {
            parts.push(text_part(
                session_id,
                message_id,
                *ordinal,
                extract_str(&json!({ "text": text }), "text"),
                provenance,
            ));
            *ordinal += 1;
        }
        Decoded::Json(Value::Array(items)) => {
            for item in items {
                parts.push(multimodal_part(
                    session_id, message_id, *ordinal, &item, provenance,
                ));
                *ordinal += 1;
            }
        }
        Decoded::Json(other) => {
            parts.push(text_part(
                session_id,
                message_id,
                *ordinal,
                Some(extract_compact_repr(&other)),
                provenance,
            ));
            *ordinal += 1;
        }
    }
}

enum Decoded {
    Json(Value),
    Text(String),
}

fn decode_content(raw: &str) -> Decoded {
    match raw.strip_prefix(CONTENT_JSON_PREFIX) {
        // A decode failure falls back to the raw string (matching hermes's own
        // `_decode_content`), so a corrupt payload is preserved, not lost.
        Some(rest) => match serde_json::from_str::<Value>(rest) {
            Ok(value) => Decoded::Json(value),
            Err(_) => Decoded::Text(raw.to_owned()),
        },
        None => Decoded::Text(raw.to_owned()),
    }
}

/// Decode a `content` string to a single canonical text value (System carriers,
/// which have no Part list). Structured JSON collapses to its compact repr.
fn decoded_text(raw: &str) -> Option<Extracted<String>> {
    let text = match decode_content(raw) {
        Decoded::Text(text) => text,
        Decoded::Json(value) => value.to_string(),
    };
    extract_str(&json!({ "content": text }), "content")
}

fn multimodal_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    item: &Value,
    provenance: Provenance,
) -> Part {
    match item.get("type").and_then(Value::as_str) {
        Some("text") => text_part(
            session_id,
            message_id,
            ordinal,
            extract_str(item, "text"),
            provenance,
        ),
        Some("image_url") => {
            let url = item
                .get("image_url")
                .and_then(|inner| inner.get("url"))
                .and_then(Value::as_str);
            match url {
                Some(url) => Part {
                    session_id: session_id.to_owned(),
                    id: part_id(message_id, ordinal),
                    message_id: message_id.to_owned(),
                    ordinal: part_ordinal(ordinal),
                    provenance,
                    options: ProviderOptions::new(),
                    kind: PartKind::File {
                        media_type: None,
                        file_name: None,
                        data: FileData::Url(url.to_owned()),
                    },
                },
                None => text_part(
                    session_id,
                    message_id,
                    ordinal,
                    Some(extract_compact_repr(item)),
                    provenance,
                ),
            }
        }
        _ => text_part(
            session_id,
            message_id,
            ordinal,
            Some(extract_compact_repr(item)),
            provenance,
        ),
    }
}

/// The assistant `tool_calls` column: parsed OpenAI tool-call objects, or the
/// raw payload preserved on a decode failure (matching [`decode_content`]'s
/// preserve-corrupt-as-text policy, never a silent drop -
/// spec.md#adapter-integrity-no-silent-drops). `Ok(vec![])` means no column.
enum ToolCalls {
    Parsed(Vec<Value>),
    Corrupt(String),
}

/// Parse the assistant `tool_calls` column into OpenAI tool-call objects. Old
/// rows double-encode it as a JSON string (fixed upstream in #68856), so a
/// string result is parsed once more.
fn tool_calls(row: &Value) -> ToolCalls {
    let Some(raw) = row.get("tool_calls").and_then(Value::as_str) else {
        return ToolCalls::Parsed(Vec::new());
    };
    let mut value: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return ToolCalls::Corrupt(raw.to_owned()),
    };
    if let Value::String(inner) = &value
        && let Ok(parsed) = serde_json::from_str::<Value>(inner)
    {
        value = parsed;
    }
    match value.as_array() {
        Some(calls) => ToolCalls::Parsed(calls.clone()),
        None => ToolCalls::Corrupt(raw.to_owned()),
    }
}

fn tool_call_part(session_id: &str, message_id: &str, ordinal: usize, call: &Value) -> Part {
    let function = call.get("function");
    let name = function.and_then(|f| extract_str(f, "name"));
    let params = function
        .and_then(|f| f.get("arguments"))
        .map(|arguments| match arguments {
            Value::String(text) => json_or_string(text),
            other => other.clone(),
        })
        .unwrap_or(Value::Null);
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::ToolCall {
            call_id: extract_str(call, "id"),
            name,
            params,
            provider_executed: false,
        },
    }
}

fn tool_result_part(
    session_id: &str,
    message_id: &str,
    row: &Value,
    content: Option<&str>,
) -> Part {
    let result = match content.map(decode_content) {
        Some(Decoded::Json(value)) => value,
        Some(Decoded::Text(text)) => Value::String(text),
        None => Value::Null,
    };
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        // spec.md#model-part-provenance: tool output is runtime-produced.
        provenance: Provenance::Injected,
        options: ProviderOptions::new(),
        kind: PartKind::ToolResult {
            call_id: extract_str(row, "tool_call_id"),
            name: extract_str(row, "tool_name"),
            is_failure: false,
            result,
        },
    }
}

fn reasoning_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    text: Extracted<String>,
) -> Part {
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Reasoning { text: Some(text) },
    }
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
        options: ProviderOptions::new(),
        kind: PartKind::Text { text },
    }
}

fn message_options(row: &Value, message_id_int: i64) -> ProviderOptions {
    let mut hermes = Map::new();
    for key in MESSAGE_META_COLUMNS {
        let Some(value) = row.get(*key).filter(|v| !v.is_null()) else {
            continue;
        };
        let stored = if JSON_TEXT_META.contains(key) {
            value.as_str().map_or_else(|| value.clone(), json_or_string)
        } else {
            value.clone()
        };
        hermes.insert((*key).to_owned(), stored);
    }
    let mut options = ProviderOptions::new();
    if !hermes.is_empty() {
        options.insert("hermes".to_owned(), Value::Object(hermes));
    }
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": NAME,
            "id": message_id_int,
            "raw_record": extract_raw_record(row),
        }),
    );
    options
}

// -- Serialize (Foreign NDJSON; plan 3.3) -----------------------------------

/// Hermes has no file-era format to target, so native restore is impossible;
/// `serialize` always emits Foreign NDJSON of the reconstructed `sessions` +
/// `messages` rows (the sanctioned fallback) and reports `Foreign` so the CLI
/// warns rather than silently degrading.
fn serialize_session(
    session: &SessionWithMessages,
    _fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    let session_row = session
        .session
        .options
        .get("source")
        .and_then(|source| source.get("raw_record"))
        .cloned()
        .or_else(|| session.session.options.get("hermes").cloned())
        .unwrap_or(Value::Null);
    let mut records = vec![json!({ "table": "sessions", "row": session_row })];

    let mut messages: Vec<&MessageWithParts> = session.messages.iter().collect();
    messages.sort_by(|left, right| {
        message_source_id(left)
            .cmp(&message_source_id(right))
            .then_with(|| by_timestamp_then_id(left, right))
    });
    for message in messages {
        let row = raw_record(message.message.options()).unwrap_or_else(|| reconstruct_row(message));
        records.push(json!({ "table": "messages", "row": row }));
    }

    Ok(vec![RestoredFile::new(
        format!("{}.jsonl", session.session.id),
        jsonl_bytes(NAME, &records)?,
        RestoreFidelity::Foreign,
    )])
}

/// Source `messages.id` (an integer), the faithful append order. `timestamp` is
/// non-monotonic and the message id STRING sorts lexicographically wrong
/// (`s:10` < `s:2`), so restore must order by this integer.
fn message_source_id(message: &MessageWithParts) -> i64 {
    message
        .message
        .options()
        .get("source")
        .and_then(|source| source.get("id"))
        .and_then(Value::as_i64)
        .unwrap_or(i64::MAX)
}

/// Minimal message row when a stored `raw_record` is absent (defensive; ingest
/// always records one).
fn reconstruct_row(message: &MessageWithParts) -> Value {
    let text = message.parts.iter().find_map(|part| match &part.kind {
        PartKind::Text { text } => Some(extracted_text(text).to_owned()),
        _ => None,
    });
    json!({
        "role": message.message.role().as_str(),
        "content": text,
        "timestamp": message.message.timestamp().timestamp() as f64,
    })
}

// -- Small helpers ----------------------------------------------------------

/// Unix epoch seconds (hermes `REAL`) -> micros.
fn secs_to_micros(secs: f64) -> i64 {
    (secs * 1_000_000.0).round() as i64
}

fn secs_to_dt(secs: f64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_micros(secs_to_micros(secs))
}

fn open_db(path: &Path) -> Result<Connection, AdapterError> {
    sqlite::open_db(NAME, path)
}

fn connection<'a>(
    conns: &'a mut HashMap<PathBuf, Connection>,
    path: &Path,
) -> Result<&'a Connection, AdapterError> {
    sqlite::connection(NAME, conns, path)
}

fn db_error(path: &Path, op: &str, error: &rusqlite::Error) -> AdapterError {
    sqlite::db_error(NAME, path, op, error)
}

fn join_error(join: tokio::task::JoinError) -> AdapterError {
    sqlite::join_error(NAME, join)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use tempfile::TempDir;

    /// The subset of hermes's `SCHEMA_SQL` pond reads, copied verbatim from
    /// `hermes_state.py` (SCHEMA_VERSION 23). Tests build DBs from this so the
    /// column shapes match the real source.
    const HERMES_SCHEMA: &str = "
        CREATE TABLE schema_version (version INTEGER NOT NULL);
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            user_id TEXT,
            session_key TEXT,
            chat_id TEXT,
            chat_type TEXT,
            thread_id TEXT,
            display_name TEXT,
            origin_json TEXT,
            expiry_finalized INTEGER DEFAULT 0,
            model TEXT,
            model_config TEXT,
            system_prompt TEXT,
            parent_session_id TEXT,
            started_at REAL NOT NULL,
            ended_at REAL,
            end_reason TEXT,
            message_count INTEGER DEFAULT 0,
            tool_call_count INTEGER DEFAULT 0,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            cache_read_tokens INTEGER DEFAULT 0,
            cache_write_tokens INTEGER DEFAULT 0,
            reasoning_tokens INTEGER DEFAULT 0,
            cwd TEXT,
            git_branch TEXT,
            git_repo_root TEXT,
            title TEXT,
            profile_name TEXT,
            rewind_count INTEGER NOT NULL DEFAULT 0,
            archived INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (parent_session_id) REFERENCES sessions(id)
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL REFERENCES sessions(id),
            role TEXT NOT NULL,
            content TEXT,
            tool_call_id TEXT,
            tool_calls TEXT,
            tool_name TEXT,
            effect_disposition TEXT,
            timestamp REAL NOT NULL,
            token_count INTEGER,
            finish_reason TEXT,
            reasoning TEXT,
            reasoning_content TEXT,
            reasoning_details TEXT,
            codex_reasoning_items TEXT,
            codex_message_items TEXT,
            platform_message_id TEXT,
            observed INTEGER DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            compacted INTEGER NOT NULL DEFAULT 0,
            api_content TEXT
        );
        CREATE INDEX idx_messages_session ON messages(session_id, timestamp);
    ";

    fn create_db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(HERMES_SCHEMA).unwrap();
        conn.execute("INSERT INTO schema_version (version) VALUES (23)", [])
            .unwrap();
        conn
    }

    /// A convenience session insert with only the columns a test cares about.
    fn insert_session(conn: &Connection, id: &str, source: &str, started_at: f64) {
        conn.execute(
            "INSERT INTO sessions (id, source, started_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, source, started_at],
        )
        .unwrap();
    }

    fn events(root: &Path) -> Vec<IngestEvent> {
        let adapter = HermesAdapter::new(root);
        let (tx, mut rx) = mpsc::channel(1024);
        let enumerated = enumerate_and_peek(&adapter, false);
        let survivors: Vec<(PathBuf, String)> = enumerated
            .entries
            .into_iter()
            .map(|entry| (entry.db_path, entry.session_id))
            .collect();
        std::thread::scope(|scope| {
            scope.spawn(move || read_survivors(survivors, &tx));
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

    fn only<T>(mut items: Vec<T>, predicate: impl Fn(&T) -> bool) -> T {
        let position = items.iter().position(predicate).expect("match present");
        items.swap_remove(position)
    }

    #[test]
    fn probe_default_requires_a_home_holding_state_db() {
        if std::env::var_os("HERMES_HOME").is_some() {
            return; // developer env overrides the probe.
        }
        let temp = TempDir::new().unwrap();
        let env = Env::with_home(temp.path());
        assert!(
            HermesFactory.probe_default(&env).is_none(),
            "an empty home is not a source",
        );

        let home = temp.path().join(".hermes");
        std::fs::create_dir_all(&home).unwrap();
        assert!(
            HermesFactory.probe_default(&env).is_none(),
            "a home without state.db is not a source",
        );

        create_db(&home.join("state.db"));
        let probe = HermesFactory.probe_default(&env);
        let got = probe
            .as_ref()
            .and_then(|value| value.get("path"))
            .and_then(Value::as_str);
        assert_eq!(
            got,
            home.to_str(),
            "probe returns the home holding state.db"
        );
    }

    #[test]
    fn probe_default_honors_hermes_home_override() {
        // The override arm is exercised directly (resolve_home) to avoid touching
        // the process env under parallel tests.
        let temp = TempDir::new().unwrap();
        let custom = temp.path().join("opt-data");
        std::fs::create_dir_all(&custom).unwrap();
        let home = temp.path().join(".hermes");
        std::fs::create_dir_all(&home).unwrap();
        create_db(&home.join("state.db"));

        // Override present but empty -> falls through to ~/.hermes.
        assert_eq!(
            resolve_home(temp.path(), Some(&custom)),
            Some(home.clone()),
            "an override without state.db does not win",
        );
        // Override holding a state.db wins over ~/.hermes.
        create_db(&custom.join("state.db"));
        assert_eq!(resolve_home(temp.path(), Some(&custom)), Some(custom));
    }

    #[test]
    fn enumerates_default_and_profile_dbs() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        let default = create_db(&root.join("state.db"));
        insert_session(&default, "s-default", "cli", 1_700_000_000.0);

        let profile_dir = root.join("profiles").join("coder");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let profile = create_db(&profile_dir.join("state.db"));
        insert_session(&profile, "s-profile", "tui", 1_700_000_100.0);

        let dbs = list_dbs(root);
        assert_eq!(dbs.len(), 2, "default + one profile DB enumerated");

        let adapter = HermesAdapter::new(root);
        let enumerated = enumerate_and_peek(&adapter, false);
        let ids: Vec<&str> = enumerated
            .entries
            .iter()
            .map(|entry| entry.session_id.as_str())
            .collect();
        assert!(ids.contains(&"s-default"));
        assert!(ids.contains(&"s-profile"));
    }

    #[test]
    fn user_and_assistant_messages_map_parts_roles_and_tool_calls() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        let conn = create_db(&db_path);
        insert_session(&conn, "s1", "telegram", 1_700_000_000.0);
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1','user','hello there', 1700000001.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_calls, timestamp, reasoning) \
             VALUES ('s1','assistant','sure', ?1, 1700000002.0, 'let me think')",
            [r#"[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"x\"}"}}]"#],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_name, timestamp) \
             VALUES ('s1','tool','result-body','call_1','lookup', 1700000003.0)",
            [],
        )
        .unwrap();
        drop(conn);

        let all = events(temp.path());
        let messages: Vec<&Message> = all
            .iter()
            .filter_map(|event| match event {
                IngestEvent::Message(message) => Some(message),
                _ => None,
            })
            .collect();
        assert_eq!(messages.len(), 3);
        assert!(matches!(messages[0], Message::User { .. }));
        assert!(matches!(messages[1], Message::Assistant { .. }));
        assert!(matches!(messages[2], Message::Tool { .. }));
        assert_eq!(messages[0].id(), "s1:1", "message id = <session>:<rowid>");

        let parts: Vec<&Part> = all
            .iter()
            .filter_map(|event| match event {
                IngestEvent::Part(part) => Some(part),
                _ => None,
            })
            .collect();
        // assistant: text + reasoning + tool_call.
        let tool_call = parts
            .iter()
            .find(|part| matches!(part.kind, PartKind::ToolCall { .. }))
            .expect("assistant tool_call part");
        match &tool_call.kind {
            PartKind::ToolCall {
                call_id,
                name,
                params,
                ..
            } => {
                assert_eq!(extracted_text(call_id), "call_1");
                assert_eq!(extracted_text(name), "lookup");
                assert_eq!(params, &json!({ "q": "x" }));
            }
            _ => unreachable!(),
        }
        assert!(
            parts
                .iter()
                .any(|part| matches!(part.kind, PartKind::Reasoning { .. })),
            "reasoning column becomes a Reasoning part",
        );
        let tool_result = parts
            .iter()
            .find(|part| matches!(part.kind, PartKind::ToolResult { .. }))
            .expect("tool role -> ToolResult part");
        assert_eq!(
            tool_result.provenance,
            Provenance::Injected,
            "tool output is injected, not conversational",
        );
    }

    #[test]
    fn multimodal_sentinel_content_decodes_to_parts() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        let conn = create_db(&db_path);
        insert_session(&conn, "s1", "discord", 1_700_000_000.0);
        let payload = format!(
            "{CONTENT_JSON_PREFIX}{}",
            json!([
                {"type": "text", "text": "look at this"},
                {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}}
            ])
        );
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1','user',?1,1700000001.0)",
            [payload],
        )
        .unwrap();
        drop(conn);

        let all = events(temp.path());
        let parts: Vec<&Part> = all
            .iter()
            .filter_map(|event| match event {
                IngestEvent::Part(part) => Some(part),
                _ => None,
            })
            .collect();
        assert_eq!(parts.len(), 2, "text + image parts");
        assert!(matches!(parts[0].kind, PartKind::Text { .. }));
        match &parts[1].kind {
            PartKind::File { data, .. } => {
                assert_eq!(data, &FileData::Url("https://example.com/a.png".to_owned()));
            }
            _ => panic!("second multimodal part is a File"),
        }
    }

    #[test]
    fn decode_content_falls_back_to_raw_on_bad_json() {
        // A sentinel with a corrupt payload is preserved as the raw string, not
        // lost - matching hermes's own `_decode_content`.
        let raw = format!("{CONTENT_JSON_PREFIX}{{not valid json");
        match decode_content(&raw) {
            Decoded::Text(text) => assert_eq!(text, raw),
            Decoded::Json(_) => panic!("corrupt payload must not parse"),
        }
    }

    #[test]
    fn watermark_is_max_timestamp_micros() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        let conn = create_db(&db_path);
        insert_session(&conn, "s1", "cli", 1_700_000_000.0);
        for (row, ts) in [("a", 1_700_000_010.5), ("b", 1_700_000_005.0)] {
            conn.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1','user',?1,?2)",
                rusqlite::params![row, ts],
            )
            .unwrap();
        }
        let watermarks = session_watermarks(&conn).unwrap();
        assert_eq!(
            watermarks.get("s1").copied(),
            Some(secs_to_micros(1_700_000_010.5)),
            "watermark is MAX(timestamp), non-monotonic order notwithstanding",
        );
    }

    #[test]
    fn lineage_relation_and_source_agent_cover_all_kinds() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        let conn = create_db(&db_path);
        // Parents with distinct end_reasons.
        conn.execute(
            "INSERT INTO sessions (id, source, started_at, end_reason) VALUES ('p-comp','telegram',1.0,'compression')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, source, started_at, end_reason) VALUES ('p-branch','telegram',1.0,'branched')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, source, started_at, end_reason) VALUES ('p-spawn','telegram',1.0,'agent_close')",
            [],
        ).unwrap();
        // Children.
        conn.execute(
            "INSERT INTO sessions (id, source, started_at, parent_session_id) VALUES ('c-comp','telegram',2.0,'p-comp')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, source, started_at, parent_session_id) VALUES ('c-branch','telegram',2.0,'p-branch')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, source, started_at, parent_session_id) VALUES ('c-spawn','telegram',2.0,'p-spawn')",
            [],
        ).unwrap();
        // A cron session and a marker-based branch (parent not 'branched').
        conn.execute(
            "INSERT INTO sessions (id, source, started_at) VALUES ('c-cron','cron',2.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, source, started_at, parent_session_id, model_config) \
             VALUES ('c-marker','telegram',2.0,'p-spawn','{\"_branched_from\":\"p-spawn\"}')",
            [],
        )
        .unwrap();

        let check = |id: &str| {
            let row = fetch_session_row(&conn, id).unwrap().unwrap();
            let (relation, agent) = classify(&conn, &row);
            (relation.map(Relation::tag), agent)
        };
        assert_eq!(
            check("c-comp"),
            (Some("compaction_successor"), "hermes".to_owned())
        );
        assert_eq!(check("c-branch"), (Some("branch"), "hermes".to_owned()));
        assert_eq!(
            check("c-spawn"),
            (Some("spawn"), "hermes/subagent".to_owned())
        );
        assert_eq!(check("c-cron"), (None, "hermes/cron".to_owned()));
        assert_eq!(
            check("c-marker"),
            (Some("branch"), "hermes".to_owned()),
            "the _branched_from marker classifies a branch even when the parent did not end 'branched'",
        );
    }

    #[test]
    fn project_prefers_session_key_then_composite_then_source() {
        let with_key =
            json!({ "source": "telegram", "session_key": "tg:42:main", "chat_id": "42" });
        assert_eq!(&*session_project(&with_key, "s"), "tg:42:main");

        let composite = json!({ "source": "discord", "chat_id": "99" });
        assert_eq!(&*session_project(&composite, "s"), "discord:99");

        let source_only = json!({ "source": "cli" });
        assert_eq!(&*session_project(&source_only, "s"), "cli");
    }

    #[test]
    fn rewrite_reinsert_appears_as_new_rows_old_survive() {
        // Hermes /retry deletes then re-inserts; AUTOINCREMENT never reuses ids,
        // so the new content lands at higher ids and pond keeps both (additive).
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        let conn = create_db(&db_path);
        insert_session(&conn, "s1", "cli", 1_700_000_000.0);
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1','user','first',1700000001.0)",
            [],
        )
        .unwrap();
        // Simulate a delete+reinsert rewrite: delete row 1, insert a fresh one.
        conn.execute("DELETE FROM messages WHERE id = 1", [])
            .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1','user','second',1700000009.0)",
            [],
        )
        .unwrap();
        let new_id: i64 = conn
            .query_row("SELECT MAX(id) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(new_id, 2, "AUTOINCREMENT does not reuse the deleted id 1");
        drop(conn);

        let message = only(
            events(temp.path())
                .into_iter()
                .filter_map(|event| match event {
                    IngestEvent::Message(message) => Some(message),
                    _ => None,
                })
                .collect(),
            |m| matches!(m, Message::User { .. }),
        );
        assert_eq!(
            message.id(),
            "s1:2",
            "the surviving row carries the fresh id"
        );
    }

    #[test]
    fn foreign_serialize_emits_session_plus_message_rows() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        let conn = create_db(&db_path);
        insert_session(&conn, "s1", "cli", 1_700_000_000.0);
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1','user','q',1700000001.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1','assistant','a',1700000002.0)",
            [],
        )
        .unwrap();
        drop(conn);

        // Rebuild a SessionWithMessages from the emitted events.
        let all = events(temp.path());
        let mut session = None;
        let mut messages = Vec::new();
        let mut parts: Vec<Part> = Vec::new();
        for event in all {
            match event {
                IngestEvent::Session(s) => session = Some(s),
                IngestEvent::Message(m) => messages.push(m),
                IngestEvent::Part(p) => parts.push(p),
            }
        }
        let session = session.unwrap();
        let with_parts: Vec<MessageWithParts> = messages
            .into_iter()
            .map(|message| {
                let owned: Vec<Part> = parts
                    .iter()
                    .filter(|part| part.message_id == message.id())
                    .cloned()
                    .collect();
                MessageWithParts {
                    parts: owned,
                    message,
                }
            })
            .collect();
        let swm = SessionWithMessages {
            session,
            messages: with_parts,
        };

        let files = serialize_session(&swm, RestoreFidelity::Native).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].actual_fidelity,
            RestoreFidelity::Foreign,
            "native is impossible for hermes; the CLI is told it downgraded",
        );
        let text = std::str::from_utf8(&files[0].bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "one session row + two message rows");
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first.get("table").and_then(Value::as_str), Some("sessions"));
        let second: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(
            second.get("table").and_then(Value::as_str),
            Some("messages")
        );
        assert_eq!(
            second.pointer("/row/content").and_then(Value::as_str),
            Some("q"),
            "messages restore in source id order",
        );
    }
}
