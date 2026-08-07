//! pi-coding-agent adapter (github.com/earendil-works/pi).
//!
//! One brand, three on-disk formats of the same source - format is detected per
//! file / per database, never configured:
//!
//! - **v3 JSONL** (what every shipped pi writes today):
//!   `~/.pi/agent/sessions/<project-slug>/<ISO-ts>_<uuid>.jsonl`. Line 1 is a
//!   `{"type":"session"}` header; the rest are typed records linked via a
//!   `parentId` -> `id` chain (pi's leaf-cursor DAG): `message` (the per-turn
//!   model interaction, roles user / assistant / toolResult under `.message`),
//!   plus `model_change` / `thinking_level_change` / `compaction`
//!   session-state carriers.
//! - **v4 JSONL** (harness-v2, `packages/agent/src/harness/session/jsonl/`):
//!   same directory layout, line 1 is `{"kind":"header","version":4,...}` and
//!   the rest are `seq`-ordered mutations - `entry` (the conversation tree),
//!   `record` (harness orchestration), `lane` (branch pointers), and `fact`
//!   (session name / entry labels).
//! - **SQLite** (harness-v2's `@earendil-works/pi-session-backend-sqlite-node`):
//!   one database hosting many sessions, opened read-only and configured via
//!   `[adapters.pi-coding-agent] sqlite_path`. Its `entries` / `records` /
//!   `lane_moves` / `facts` rows carry the SAME payload shapes as the v4
//!   mutations, so they are rebuilt into v4 mutation values and mapped by the
//!   one v4 mapper.
//!
//! Only `entry`-with-`message` rows become conversational messages; every other
//! mutation is a placement-rule-3 System carrier (spec.md#model-part-provenance),
//! which keeps `search_text` free of orchestration noise automatically. The
//! `parentId` fork graph (spec.md#deferred: multi-level fork lineage) is not
//! collapsed into `parent_message_id` but preserved verbatim in
//! `options.source.raw_record` for a future branching consumer.
//!
//! Resume (`pond resume --to pi-coding-agent`, the adapter serialize face):
//! native replays the stored `raw_record` rows verbatim, so a v3-origin session
//! resumes as v3 and a v4- or SQLite-origin session resumes as a v4 `.jsonl`
//! (a file is the portable artifact - writing into a live pi database from
//! outside would race its writer lease). Foreign sessions - including
//! v3-origin pi sessions asked for a value-complete replay they cannot serve -
//! are reconstructed as **v3**, because v3 is what every shipped pi loads and
//! harness-v2 guarantees read-only v3 normalization. Revisit when pi ships v4
//! as its default write format.
//!
//! Format watch: harness-v2's v3-normalization work packages (J4/J5) and the
//! coding-agent migration were unfinished at pi commit `6fb2d766a` (0.84.1), so
//! v4 and SQLite are real and testable but not yet what a released pi writes by
//! default. On each pi release until then: `git -C ~/pjv/earendil-works/pi pull`,
//! re-run `tests/fixtures/adapter/pi-coding-agent/generate-v4-fixtures.mjs`, and
//! diff against the committed fixtures. A diff is scheduled maintenance.

use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use crate::{
    sessions::IngestEvent,
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, SourceWatermark, SyncPlan,
    by_timestamp_then_id, compact_json, empty_options,
    extract::{Extracted, extract_compact_repr, extract_raw_record, extract_str},
    extracted_text, is_session_fresh,
    jsonl::{
        BoundedRow, JsonlTree, jsonl_tree_discover, jsonl_tree_events, peek_first_line,
        peek_last_line, source_line,
    },
    jsonl_bytes, part_id, part_ordinal, raw_record,
    sqlite::{CHANNEL_CAP, ColKind, columns_sql, db_error, emit, join_error, open_db, row_to_json},
};

const NAME: &str = "pi-coding-agent";

/// The only harness-v2 JSONL version this adapter decodes. A header naming any
/// other version is a visible, counted skip that names the file - never a
/// half-understood ingest.
const SUPPORTED_JSONL_VERSION: i64 = 4;

/// Stateless factory: opens [`PiCodingAgentAdapter`] instances and probes for the
/// canonical install location under `~/.pi/agent/sessions`.
pub struct PiCodingAgentFactory;

impl AdapterFactory for PiCodingAgentFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(PiCodingAgentAdapter::from_config(config)?))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        // Only the JSONL sessions root is auto-discoverable: pi's coding agent
        // does not yet write the SQLite backend, so there is no canonical
        // database path to probe. Configuring `sqlite_path` is explicit
        // (spec.md#model-no-synthesis applied to discovery - no invented paths).
        let path = env.home.join(".pi").join("agent").join("sessions");
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

/// `[adapters.pi-coding-agent]` config blob: the JSONL sessions root, plus an
/// optional harness-v2 SQLite database. Both are read when both are present.
#[derive(Debug, Clone, Deserialize)]
struct PiCodingAgentConfig {
    path: PathBuf,
    #[serde(default)]
    sqlite_path: Option<PathBuf>,
}

// -- Serialize (the `pond resume` face) -------------------------------------

fn serialize_session(
    session: &crate::sessions::SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    // Native replays the verbatim `options.source.raw_record` rows (the header
    // line first, then one per message in source order) - so the emitted file
    // is in whatever format the session was captured from. `pi_record` below is
    // the foreign-only v3 reconstruction. Replay echoes a frozen snapshot -
    // safe only while canonical is append-only
    // (spec.md#adapter-integrity-additive-sync).
    //
    // spec.md#adapter-native-restore-lossless: if Native is requested but the
    // session has no stored `raw_record`, we downgrade to foreign and stamp
    // `actual_fidelity` so the caller can signal the downgrade. Mirrors
    // opencode's behavior - both adapters serve the best they can and tell
    // the truth about what they served.
    let session_raw = raw_record(&session.session.options);
    let actual = match fidelity {
        RestoreFidelity::Native if session_raw.is_some() => RestoreFidelity::Native,
        _ => RestoreFidelity::Foreign,
    };

    let mut records = Vec::new();
    if actual == RestoreFidelity::Native {
        records.push(session_raw.unwrap_or_else(|| pi_session_record(session)));
    } else {
        records.push(pi_session_record(session));
    }

    // Sort message references rather than cloning the whole vec; restore is a
    // hot path when users resume large sessions.
    let mut messages: Vec<&crate::sessions::MessageWithParts> = session.messages.iter().collect();
    if actual == RestoreFidelity::Native {
        messages.sort_by(|left, right| {
            source_line(left.message.options())
                .cmp(&source_line(right.message.options()))
                .then_with(|| by_timestamp_then_id(left, right))
        });
    } else {
        messages.sort_by(|left, right| by_timestamp_then_id(left, right));
    }

    for message in &messages {
        if actual == RestoreFidelity::Native
            && let Some(raw) = raw_record(message.message.options())
        {
            records.push(raw);
            continue;
        }
        // Foreign restore: a System carrier (a model/compaction record, or any
        // harness-v2 orchestration mutation) has no idiomatic home in another
        // client's transcript - drop it; the content stays in canonical
        // (spec.md#adapter-native-restore-lossless, foreign clause).
        if matches!(message.message, Message::System { .. }) {
            continue;
        }
        records.push(pi_message_record(message));
    }

    Ok(vec![RestoredFile::new(
        pi_relative_path(session),
        jsonl_bytes(NAME, &records)?,
        actual,
    )])
}

/// Reproduce the on-disk `sessions/<slug>/<file>.jsonl` path from the slug and
/// file name captured at ingest. Falls back to pi's own naming scheme when a
/// foreign or SQLite-origin session never carried them, so the emitted file
/// lands exactly where pi's session list looks for it.
fn pi_relative_path(session: &crate::sessions::SessionWithMessages) -> PathBuf {
    let source = session.session.options.get("source");
    let slug = source
        .and_then(|s| s.get("project_slug"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| encode_project(&session.session.project));
    let file_name = source
        .and_then(|s| s.get("file_name"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            let ts = session.session.created_at.format("%Y-%m-%dT%H-%M-%S-%3fZ");
            format!("{ts}_{}.jsonl", session.session.id)
        });
    PathBuf::from("sessions").join(slug).join(file_name)
}

/// pi's `sessionDirectoryName` (`jsonl/repo.ts`): strip the leading separator,
/// map `/`, `\` and `:` to `-`, and wrap in `--`. Reproduced exactly so a
/// resumed session is discoverable by pi rather than merely well-formed.
fn encode_project(project: &str) -> String {
    let body: String = project
        .trim_start_matches(['/', '\\'])
        .chars()
        .map(|c| {
            if matches!(c, '/' | '\\' | ':') {
                '-'
            } else {
                c
            }
        })
        .collect();
    format!("--{body}--")
}

fn pi_session_record(session: &crate::sessions::SessionWithMessages) -> Value {
    json!({
        "type": "session",
        "version": 3,
        "id": session.session.id,
        "timestamp": session.session.created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "cwd": &*session.session.project,
    })
}

fn pi_message_record(message: &crate::sessions::MessageWithParts) -> Value {
    json!({
        "type": "message",
        "id": message.message.id(),
        "parentId": message.message.options().get("source").and_then(|s| s.get("parent_id")),
        "timestamp": message.message.timestamp().to_rfc3339_opts(SecondsFormat::Millis, true),
        "message": pi_inner_message(message),
    })
}

fn pi_inner_message(message: &crate::sessions::MessageWithParts) -> Value {
    let epoch_ms = message.message.timestamp().timestamp_millis();
    match &message.message {
        Message::User { .. } => json!({
            "role": "user",
            "content": message.parts.iter().map(pi_content_item).collect::<Vec<_>>(),
            "timestamp": epoch_ms,
        }),
        Message::Assistant { .. } => json!({
            "role": "assistant",
            "content": message.parts.iter().map(pi_content_item).collect::<Vec<_>>(),
            "timestamp": epoch_ms,
        }),
        Message::Tool { .. } => {
            // spec.md#adapter-native-restore-lossless (foreign clause): a
            // canonical Tool message with no ToolResult part - or with parts
            // that lack call_id/name - serializes with empty-string slots.
            // That's lossy by design for foreign restore; the unaltered
            // source still lives in canonical and in `raw_record`.
            let part = message.parts.first();
            let (call_id, name, is_error, result) = match part.map(|p| &p.kind) {
                Some(PartKind::ToolResult {
                    call_id,
                    name,
                    is_failure,
                    result,
                }) => (
                    extracted_text(call_id).to_owned(),
                    extracted_text(name).to_owned(),
                    *is_failure,
                    result.clone(),
                ),
                _ => (String::new(), String::new(), false, Value::Null),
            };
            json!({
                "role": "toolResult",
                "toolCallId": call_id,
                "toolName": name,
                "content": result,
                "isError": is_error,
                "timestamp": epoch_ms,
            })
        }
        // serialize_session drops System carriers before reaching here in
        // foreign mode, and native mode replays the source row verbatim - so
        // this arm only fires if a caller invokes pi_message_record on a
        // System message directly. Unreachable on every legitimate path.
        Message::System { .. } => {
            unreachable!("System messages are not serialized through pi_inner_message")
        }
    }
}

fn pi_content_item(part: &Part) -> Value {
    match &part.kind {
        PartKind::Text { text } => json!({"type": "text", "text": extracted_text(text)}),
        PartKind::Reasoning { text } => json!({
            "type": "thinking",
            "thinking": extracted_text(text),
            "thinkingSignature": part
                .options
                .get("pi")
                .and_then(|p| p.get("thinking_signature")),
        }),
        PartKind::ToolCall {
            call_id,
            name,
            params,
            ..
        } => json!({
            "type": "toolCall",
            "id": extracted_text(call_id),
            "name": extracted_text(name),
            "arguments": params,
        }),
        other => json!({
            "type": "text",
            "text": compact_json(&serde_json::to_value(other).unwrap_or(Value::Null)),
        }),
    }
}

// -- Adapter ----------------------------------------------------------------

/// Configured pi reader: a tree of `*.jsonl` session files (v3 and v4 share the
/// directory layout) plus, when configured, one harness-v2 SQLite database.
#[derive(Debug, Clone)]
pub struct PiCodingAgentAdapter {
    root: PathBuf,
    sqlite_path: Option<PathBuf>,
}

impl PiCodingAgentAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            sqlite_path: None,
        }
    }

    /// Also read a harness-v2 SQLite database. Both sources stream in one run;
    /// a session present in both is deduped by the store's composite PK
    /// (`lance-deterministic-pk`), not here.
    pub fn with_sqlite(mut self, path: impl Into<PathBuf>) -> Self {
        self.sqlite_path = Some(path.into());
        self
    }

    fn from_config(config: Value) -> Result<Self, AdapterError> {
        let cfg: PiCodingAgentConfig = serde_json::from_value(config)
            .map_err(|err| AdapterError::config(NAME, format!("bad config blob: {err}")))?;
        let expand = |path: PathBuf| match std::env::var_os("HOME") {
            Some(home) => crate::config::expand_home_under(&path, Path::new(&home)),
            None => path,
        };
        Ok(Self {
            root: expand(cfg.path),
            sqlite_path: cfg.sqlite_path.map(expand),
        })
    }
}

impl Adapter for PiCodingAgentAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let files = jsonl_tree_discover(self);
        let Some(db_path) = self.sqlite_path.clone() else {
            return files;
        };
        Box::pin(async move {
            let db_sessions = tokio::task::spawn_blocking(move || {
                let conn = open_db(NAME, &db_path)?;
                list_db_sessions(&conn, &db_path).map(|rows| rows.len())
            })
            .await
            .map_err(|join| join_error(NAME, join))??;
            Ok(files.await? + db_sessions)
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let files = jsonl_tree_events(self, oracle);
        match self.sqlite_path.clone() {
            None => files,
            Some(db_path) => Box::pin(files.chain(sqlite_events(db_path, oracle))),
        }
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> crate::adapter::PlanFuture<'a> {
        let files = crate::adapter::jsonl::jsonl_tree_plan(self, oracle);
        let Some(db_path) = self.sqlite_path.clone() else {
            return files;
        };
        Box::pin(async move {
            let db_plan = sqlite_plan(db_path, oracle).await?;
            Ok(match (files.await?, db_plan) {
                (Some(files), db) => Some(SyncPlan {
                    sessions: files.sessions + db.sessions,
                    fresh: files.fresh + db.fresh,
                    pending: files.pending + db.pending,
                }),
                (None, db) => Some(db),
            })
        })
    }
}

impl JsonlTree for PiCodingAgentAdapter {
    // pi's `toolResult` records carry their own `toolName`, so unlike
    // claude-code / codex-cli the adapter needs no per-file call_id -> name map.
    type State = ();

    fn name(&self) -> &'static str {
        NAME
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn peek_session_id(&self, _path: &Path, first_line: &str) -> Option<String> {
        let row: Value = serde_json::from_str(first_line).ok()?;
        if !is_session_head(&row) {
            return None;
        }
        row.get("id").and_then(Value::as_str).map(ToOwned::to_owned)
    }

    fn peek_watermark(&self, path: &Path) -> SourceWatermark {
        // The transcript is append-ordered, so the LAST line is the latest
        // event - and only the last line is consulted. A v4 `lane` / `fact`
        // mutation carries no timestamp, so it maps to a carrier anchored at
        // the session's creation time; walking back past it to an older
        // timestamped line would produce a watermark the store already meets
        // and the trailing mutation would never be ingested. Judging the last
        // line alone makes that case `Opaque` (re-read) instead of silently
        // fresh (spec.md#session-movement-complete).
        let last = || -> Option<i64> {
            let row: Value = serde_json::from_str(&peek_last_line(path)?).ok()?;
            if is_session_head(&row) {
                return None;
            }
            row_timestamp(&row).map(|ts| ts.timestamp_micros())
        };
        match last() {
            Some(ts) => SourceWatermark::At(ts),
            None => SourceWatermark::Opaque,
        }
    }

    fn unsupported_reason(&self, path: &Path) -> Option<String> {
        // A harness-v2 header naming a version this build does not decode is a
        // recognized-but-unsupported sidecar: a visible, counted skip that
        // names the file and the fix, never a content-borrowed id.
        let row: Value = serde_json::from_str(&peek_first_line(path)?).ok()?;
        let version = row.get("version").and_then(Value::as_i64)?;
        (row.get("kind").and_then(Value::as_str) == Some("header")
            && version != SUPPORTED_JSONL_VERSION)
            .then(|| {
                format!(
                    "{}: pi session format version {version} is newer than this pond build \
                     understands (supported: {SUPPORTED_JSONL_VERSION}); upgrade pond",
                    path.display(),
                )
            })
    }

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
        session_from_rows(path, rows)
    }

    fn events_from_row(
        &self,
        session: &Session,
        row: &BoundedRow,
        _state: &mut Self::State,
    ) -> Result<Vec<IngestEvent>, String> {
        match session_format(session) {
            SUPPORTED_JSONL_VERSION => v4_events_from_mutation(
                &session.id,
                row.line as i64,
                &row.value,
                session.created_at,
            ),
            _ => v3_events_from_row(&session.id, row.line, &row.value, session.created_at),
        }
    }
}

/// Is this row the eventless head of a session - a v3 `session` record or a v4
/// `header` line? Both carry the session id and neither becomes a message.
fn is_session_head(row: &Value) -> bool {
    row.get("type").and_then(Value::as_str) == Some("session")
        || row.get("kind").and_then(Value::as_str) == Some("header")
}

/// A row's own event timestamp: v3 writes RFC3339 strings, v4 writes epoch
/// milliseconds, and v4 `lane` / `fact` mutations write none.
fn row_timestamp(row: &Value) -> Option<DateTime<Utc>> {
    match row.get("timestamp") {
        Some(Value::String(text)) => DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|dt| dt.with_timezone(&Utc)),
        Some(Value::Number(number)) => DateTime::from_timestamp_millis(number.as_i64()?),
        _ => None,
    }
}

fn session_format(session: &Session) -> i64 {
    session
        .options
        .get("source")
        .and_then(|source| source.get("format"))
        .and_then(Value::as_i64)
        .unwrap_or(3)
}

fn session_from_rows(path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
    let path_display = path.display().to_string();
    let first = rows
        .first()
        .ok_or_else(|| AdapterError::schema(NAME, path_display.clone(), "empty jsonl session"))?;
    let row = &first.value;
    let at_first = format!("{path_display}:{}", first.line);

    // Capture the exact on-disk path components so native restore reproduces
    // the source file byte-for-byte (the slug encoding and filename timestamp
    // are not recomputable from canonical alone).
    let placement = SourcePlacement {
        project_slug: path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .map(ToOwned::to_owned),
        file_name: path
            .file_name()
            .and_then(|n| n.to_str())
            .map(ToOwned::to_owned),
    };

    if row.get("kind").and_then(Value::as_str) == Some("header") {
        return v4_session_from_header(row, &at_first, &placement);
    }
    if row.get("type").and_then(Value::as_str) != Some("session") {
        return Err(AdapterError::schema(
            NAME,
            at_first,
            "first row must be a v3 `session` record or a v4 `header`",
        ));
    }
    v3_session_from_row(row, &at_first, &placement)
}

/// Where a JSONL session lives on disk. `None` for a SQLite-origin session,
/// which has no source file - restore then derives pi's own naming.
#[derive(Debug, Default, Clone)]
struct SourcePlacement {
    project_slug: Option<String>,
    file_name: Option<String>,
}

// -- v3 ---------------------------------------------------------------------

fn v3_session_from_row(
    row: &Value,
    at_first: &str,
    placement: &SourcePlacement,
) -> Result<Session, AdapterError> {
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(NAME, at_first.to_owned(), "session record missing id")
        })?
        .to_owned();
    let created_at = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                at_first.to_owned(),
                "session record has no parseable timestamp",
            )
        })?;
    let project = extract_str(row, "cwd").ok_or_else(|| {
        // spec.md#model-project-non-empty: pi always records `cwd` on the
        // session line; its absence is a malformed session, not a default.
        AdapterError::schema(NAME, at_first.to_owned(), "session record missing cwd")
    })?;

    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": NAME,
            "format": 3,
            "version": row.get("version"),
            "project_slug": placement.project_slug,
            "file_name": placement.file_name,
            "raw_record": extract_raw_record(row),
        }),
    );

    Ok(Session {
        id,
        parent_session_id: None,
        parent_message_id: None,
        source_agent: NAME.to_owned(),
        created_at,
        project,
        options,
    })
}

/// Map one v3 JSONL record into zero-or-more `IngestEvent`s. `session` is
/// consumed up front (eventless here); `model_change` / `thinking_level_change`
/// / `compaction` become System carriers; `message` becomes a User / Assistant
/// / Tool message plus its content Parts.
fn v3_events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
) -> Result<Vec<IngestEvent>, String> {
    let kind = row.get("type").and_then(Value::as_str);
    let timestamp = row_timestamp(row).unwrap_or(default_timestamp);
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .map_or_else(|| format!("{session_id}:{line}"), ToOwned::to_owned);
    let order = line as i64;

    match kind {
        // Consumed up front by `session_from_rows`.
        Some("session") => Ok(Vec::new()),
        Some("message") => {
            let message_value = row
                .get("message")
                .ok_or_else(|| "message record missing `message` field".to_owned())?;
            message_events(session_id, &id, timestamp, row, message_value, order)
        }
        // Session-state carriers: keep the human-meaningful field as content,
        // the rest of the record in `options.source.raw_record`.
        Some("compaction") => Ok(vec![carrier_event(
            session_id,
            &id,
            timestamp,
            row,
            order,
            extract_str(row, "summary"),
        )]),
        // Unknown record type: preserve it as a System carrier rather than
        // dropping (spec.md#adapter-integrity-no-silent-drops). The raw record
        // survives in options; the type label is the content.
        _ => Ok(vec![carrier_event(
            session_id,
            &id,
            timestamp,
            row,
            order,
            extract_str(row, "type"),
        )]),
    }
}

// -- v4 (harness-v2 JSONL, and the SQLite backend rebuilt into its shapes) ---

fn v4_session_from_header(
    row: &Value,
    at_first: &str,
    placement: &SourcePlacement,
) -> Result<Session, AdapterError> {
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::schema(NAME, at_first.to_owned(), "v4 header missing id"))?
        .to_owned();
    let created_at = row
        .get("createdAt")
        .and_then(Value::as_i64)
        .and_then(DateTime::from_timestamp_millis)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                at_first.to_owned(),
                "v4 header has no parseable createdAt",
            )
        })?;
    // spec.md#model-project-non-empty: the v4 header records the real absolute
    // cwd - strictly better than v3's lossy directory slug, which stays only as
    // the restore-placement hint above.
    let project = extract_str(row, "cwd")
        .ok_or_else(|| AdapterError::schema(NAME, at_first.to_owned(), "v4 header missing cwd"))?;

    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": NAME,
            "format": SUPPORTED_JSONL_VERSION,
            "version": row.get("version"),
            "project_slug": placement.project_slug,
            "file_name": placement.file_name,
            // spec.md#model-lossless-projection: the header's application-owned
            // metadata bag and the unresolved v3 parent path are carried
            // verbatim, never interpreted.
            "metadata": row.get("metadata"),
            "legacy_parent_session_path": row.get("legacyParentSessionPath"),
            "raw_record": extract_raw_record(row),
        }),
    );

    Ok(Session {
        id,
        parent_session_id: row
            .get("parentSessionId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        parent_message_id: None,
        source_agent: NAME.to_owned(),
        created_at,
        project,
        options,
    })
}

/// Map one v4 mutation into zero-or-more `IngestEvent`s. `entry`-with-`message`
/// is the only conversational shape; every other mutation - orchestration
/// `record`s, `lane` pointer moves, `fact` assertions, and any kind or type a
/// later pi adds - becomes a placement-rule-3 System carrier so it is preserved
/// losslessly without polluting `search_text`.
fn v4_events_from_mutation(
    session_id: &str,
    order: i64,
    row: &Value,
    default_timestamp: DateTime<Utc>,
) -> Result<Vec<IngestEvent>, String> {
    // Consumed up front by `v4_session_from_header` / the SQLite session row.
    if is_session_head(row) {
        return Ok(Vec::new());
    }
    // `lane` and `fact` mutations carry no timestamp: the seam permits an
    // absence default (spec.md#model-no-synthesis) and the session anchor is
    // the honest one, with the source's own `seq` kept in options so log order
    // is never lost.
    let timestamp = row_timestamp(row).unwrap_or(default_timestamp);
    // `lane` / `fact` mutations carry no id either. `seq` is the source's own
    // total order - the same value in the JSONL and SQLite containers - and
    // pi's session-id charset excludes `:`
    // (`jsonl/repo.ts::SESSION_ID_PATTERN`), so this shape can never collide
    // with a real entry or record id.
    let id = row.get("id").and_then(Value::as_str).map_or_else(
        || {
            let key = row.get("seq").and_then(Value::as_i64).unwrap_or(order);
            format!("{session_id}:{key}")
        },
        ToOwned::to_owned,
    );

    let carrier = |content| {
        Ok(vec![carrier_event(
            session_id, &id, timestamp, row, order, content,
        )])
    };
    match row.get("kind").and_then(Value::as_str) {
        Some("entry") => match row.get("type").and_then(Value::as_str) {
            Some("message") => {
                let message_value = row
                    .get("message")
                    .ok_or_else(|| "v4 message entry missing `message` field".to_owned())?;
                message_events(session_id, &id, timestamp, row, message_value, order)
            }
            Some("compaction" | "branch_summary") => carrier(extract_str(row, "summary")),
            Some("custom") => carrier(extract_str(row, "customType")),
            _ => carrier(extract_str(row, "type")),
        },
        Some("record") => carrier(extract_str(row, "type")),
        Some("lane") => carrier(extract_str(row, "lane")),
        Some("fact") => carrier(
            extract_str(row, "name")
                .or_else(|| extract_str(row, "label"))
                .or_else(|| extract_str(row, "fact")),
        ),
        // An unrecognized mutation kind is well-formed input from a newer pi,
        // not corruption: preserve the whole line as a carrier
        // (spec.md#adapter-integrity-no-silent-drops) rather than erroring.
        _ => carrier(extract_str(row, "kind")),
    }
}

// -- Shared message mapping (v3 rows and v4 `entry` mutations alike) ---------

fn carrier_event(
    session_id: &str,
    id: &str,
    timestamp: DateTime<Utc>,
    row: &Value,
    order: i64,
    content: Option<Extracted<String>>,
) -> IngestEvent {
    IngestEvent::Message(Message::System {
        id: id.to_owned(),
        session_id: session_id.to_owned(),
        timestamp,
        content,
        options: row_options(row, order),
    })
}

fn message_events(
    session_id: &str,
    id: &str,
    timestamp: DateTime<Utc>,
    row: &Value,
    message_value: &Value,
    order: i64,
) -> Result<Vec<IngestEvent>, String> {
    let role = message_value
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| "message missing role".to_owned())?;
    let content = message_value
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut parts = Vec::new();
    let message = match role {
        "user" => {
            // spec.md#model-part-provenance: pi user messages are genuine human
            // prompts; harness-injected context arrives as separate records
            // (compaction, model_change), not inside a user turn.
            for (ordinal, item) in content.iter().enumerate() {
                parts.push(user_part(session_id, id, ordinal, item));
            }
            Message::User {
                id: id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, order),
            }
        }
        "assistant" => {
            for (ordinal, item) in content.iter().enumerate() {
                parts.push(assistant_part(session_id, id, ordinal, item));
            }
            Message::Assistant {
                id: id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: assistant_options(row, message_value, order),
            }
        }
        "toolResult" => {
            parts.push(tool_result_part(session_id, id, message_value));
            Message::Tool {
                id: id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, order),
            }
        }
        // pi's other AgentMessage roles (bashExecution, custom, branchSummary,
        // compactionSummary) and any role a later pi adds are harness-authored,
        // not conversation. Preserve the row as a System carrier instead of
        // turning it into a counted drop.
        _ => Message::System {
            id: id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            content: extract_str(message_value, "role"),
            options: row_options(row, order),
        },
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

fn user_part(session_id: &str, message_id: &str, ordinal: usize, item: &Value) -> Part {
    let kind = match item.get("type").and_then(Value::as_str) {
        Some("text") => PartKind::Text {
            text: extract_str(item, "text"),
        },
        // Anything else (e.g. an `image` content item) is preserved losslessly
        // as a compact-JSON Text Part rather than dropped.
        _ => PartKind::Text {
            text: Some(extract_compact_repr(item)),
        },
    };
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        // spec.md#model-part-provenance: a genuine human prompt is conversation.
        provenance: Provenance::Conversational,
        options: empty_options(),
        kind,
    }
}

fn assistant_part(session_id: &str, message_id: &str, ordinal: usize, item: &Value) -> Part {
    // spec.md#model-part-provenance: assistant text, reasoning, and tool calls
    // are model-authored, hence conversational.
    let (kind, options) = match item.get("type").and_then(Value::as_str) {
        Some("text") => (
            PartKind::Text {
                text: extract_str(item, "text"),
            },
            empty_options(),
        ),
        Some("thinking") => (
            PartKind::Reasoning {
                text: extract_str(item, "thinking"),
            },
            thinking_options(item),
        ),
        Some("toolCall") => (
            PartKind::ToolCall {
                call_id: extract_str(item, "id"),
                name: extract_str(item, "name"),
                params: item.get("arguments").cloned().unwrap_or(Value::Null),
                provider_executed: false,
            },
            empty_options(),
        ),
        // Lossless fallback for an unrecognised assistant content shape.
        _ => (
            PartKind::Text {
                text: Some(extract_compact_repr(item)),
            },
            empty_options(),
        ),
    };
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        provenance: Provenance::Conversational,
        options,
        kind,
    }
}

fn tool_result_part(session_id: &str, message_id: &str, message_value: &Value) -> Part {
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        // spec.md#model-part-provenance: tool output is runtime-produced.
        provenance: Provenance::Injected,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id: extract_str(message_value, "toolCallId"),
            name: extract_str(message_value, "toolName"),
            is_failure: message_value
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            // The whole `content` array (text and/or image items) is the
            // faithful result payload.
            result: message_value.get("content").cloned().unwrap_or(Value::Null),
        },
    }
}

/// `order` is the source's own total order: the JSONL line number, or the
/// SQLite backend's `seq`. Native restore sorts on it, so a resumed file
/// replays the source log in source order regardless of which format it came
/// from.
fn row_options(row: &Value, order: i64) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "line": order,
            "seq": row.get("seq"),
            "lane": row.get("lane"),
            "parent_id": row.get("parentId"),
            "raw_type": row.get("type"),
            "raw_record": extract_raw_record(row),
        }),
    );
    options
}

fn assistant_options(row: &Value, message_value: &Value, order: i64) -> ProviderOptions {
    let mut options = row_options(row, order);
    options.insert(
        "pi".to_owned(),
        json!({
            "api": message_value.get("api"),
            "provider": message_value.get("provider"),
            "model": message_value.get("model"),
            "usage": message_value.get("usage"),
            "stop_reason": message_value.get("stopReason"),
            "response_id": message_value.get("responseId"),
        }),
    );
    options
}

fn thinking_options(item: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    if let Some(signature) = item.get("thinkingSignature") {
        options.insert("pi".to_owned(), json!({ "thinking_signature": signature }));
    }
    options
}

// -- SQLite backend ---------------------------------------------------------
//
// One database hosts many sessions. Its `entries` / `records` / `lane_moves` /
// `facts` rows store exactly the v4 mutation payloads, so each row is rebuilt
// into the v4 mutation value pi itself would have written and handed to the one
// v4 mapper - and native resume replays those values into a portable v4
// `.jsonl` (writing into a live pi database from outside would race its
// per-session writer lease, so that is deliberately out of scope).

const DB_SESSION_COLUMNS: &[(&str, ColKind)] = &[
    ("id", ColKind::Str),
    ("created_at", ColKind::Str),
    ("cwd", ColKind::Str),
    ("parent_session_id", ColKind::Str),
    ("metadata", ColKind::Str),
];

/// One session head from the database, with the fields the freshness gate and
/// the header reconstruction need.
struct DbSession {
    id: String,
    row: Value,
}

fn list_db_sessions(conn: &Connection, db_path: &Path) -> Result<Vec<DbSession>, AdapterError> {
    let sql = format!(
        "SELECT {} FROM sessions ORDER BY id",
        columns_sql(DB_SESSION_COLUMNS)
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|error| db_error(NAME, db_path, "prepare sessions", &error))?;
    let rows = stmt
        .query_map([], |row| row_to_json(row, DB_SESSION_COLUMNS))
        .map_err(|error| db_error(NAME, db_path, "query sessions", &error))?;
    let mut out = Vec::new();
    for row in rows {
        let row = row.map_err(|error| db_error(NAME, db_path, "read session row", &error))?;
        let Some(id) = row.get("id").and_then(Value::as_str).map(ToOwned::to_owned) else {
            continue;
        };
        out.push(DbSession { id, row });
    }
    Ok(out)
}

/// Timestamp of the highest-`seq` mutation, or `None` when that mutation is a
/// `lane_moves` / `facts` row (which carry none) - the same rule, and the same
/// reason, as the JSONL [`JsonlTree::peek_watermark`] above.
fn db_session_watermark(conn: &Connection, session_id: &str) -> Option<i64> {
    let text: Option<String> = conn
        .query_row(
            "SELECT timestamp FROM (
                 SELECT seq, timestamp FROM entries WHERE session_id = ?1
                 UNION ALL SELECT seq, timestamp FROM records WHERE session_id = ?1
                 UNION ALL SELECT seq, NULL FROM lane_moves WHERE session_id = ?1
                 UNION ALL SELECT seq, NULL FROM facts WHERE session_id = ?1
             ) ORDER BY seq DESC LIMIT 1",
            [session_id],
            |row| row.get(0),
        )
        .ok()?;
    parse_db_timestamp(&text?).map(|dt| dt.timestamp_micros())
}

fn parse_db_timestamp(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn sqlite_plan<'a>(
    db_path: PathBuf,
    oracle: &'a dyn SkipOracle,
) -> impl std::future::Future<Output = Result<SyncPlan, AdapterError>> + Send + 'a {
    let oracle_is_empty = oracle.is_empty();
    async move {
        let heads = tokio::task::spawn_blocking(move || db_heads(&db_path, !oracle_is_empty))
            .await
            .map_err(|join| join_error(NAME, join))??;
        if oracle_is_empty {
            return Ok(SyncPlan::all_pending(heads.len()));
        }
        Ok(SyncPlan::from_heads(
            oracle,
            heads.iter().map(|(session, ts)| {
                (
                    Some(session.id.as_str()),
                    ts.map_or(SourceWatermark::Opaque, SourceWatermark::At),
                )
            }),
        ))
    }
}

/// Every session in the database with its freshness watermark. The session ROW
/// rides along so the read pass does not re-scan `sessions` to recover what the
/// gate already read.
fn db_heads(db_path: &Path, peek: bool) -> Result<Vec<(DbSession, Option<i64>)>, AdapterError> {
    let conn = open_db(NAME, db_path)?;
    Ok(list_db_sessions(&conn, db_path)?
        .into_iter()
        .map(|session| {
            let watermark = peek
                .then(|| db_session_watermark(&conn, &session.id))
                .flatten();
            (session, watermark)
        })
        .collect())
}

fn sqlite_events<'a>(db_path: PathBuf, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
    Box::pin(async_stream::stream! {
        let peek = !oracle.is_empty();
        let list_path = db_path.clone();
        let listed = tokio::task::spawn_blocking(move || db_heads(&list_path, peek)).await;
        let heads = match listed {
            Ok(Ok(heads)) => heads,
            Ok(Err(error)) => { yield Err(error); return; }
            Err(join) => { yield Err(join_error(NAME, join)); return; }
        };

        let mut survivors = Vec::with_capacity(heads.len());
        let mut fresh = 0usize;
        for (session, watermark) in heads {
            if is_session_fresh(oracle, &session.id, watermark) {
                fresh += 1;
                continue;
            }
            survivors.push(session.row);
        }
        if fresh > 0 {
            yield Ok(AdapterYield::SkippedBatch { reason: SkipReason::Fresh, count: fresh });
        }

        let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
        let handle = tokio::task::spawn_blocking(move || read_db_sessions(&db_path, &survivors, &tx));
        while let Some(item) = rx.recv().await {
            yield item;
        }
        if let Err(join) = handle.await {
            yield Err(join_error(NAME, join));
        }
    })
}

fn read_db_sessions(
    db_path: &Path,
    sessions: &[Value],
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    let conn = match open_db(NAME, db_path) {
        Ok(conn) => conn,
        Err(error) => {
            emit!(tx, Err(error));
            return true;
        }
    };
    for row in sessions {
        if !read_db_session(&conn, db_path, row, tx) {
            return false;
        }
    }
    true
}

fn read_db_session(
    conn: &Connection,
    db_path: &Path,
    row: &Value,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    let at = |session_id: &str| format!("{}#{session_id}", db_path.display());
    let header = db_header_value(row);
    let session_id = row.get("id").and_then(Value::as_str).unwrap_or_default();
    let session =
        match v4_session_from_header(&header, &at(session_id), &SourcePlacement::default()) {
            Ok(session) => session,
            Err(error) => {
                emit!(tx, Err(error));
                return true;
            }
        };
    let mutations = match db_mutations(conn, db_path, &session.id) {
        Ok(mutations) => mutations,
        Err(error) => {
            emit!(tx, Err(error));
            return true;
        }
    };
    emit!(
        tx,
        Ok(AdapterYield::Event(IngestEvent::Session(session.clone())))
    );
    for (seq, mutation) in mutations {
        match v4_events_from_mutation(&session.id, seq, &mutation, session.created_at) {
            Ok(events) => {
                for event in events {
                    emit!(tx, Ok(AdapterYield::Event(event)));
                }
            }
            Err(message) => emit!(
                tx,
                Err(AdapterError::schema(
                    NAME,
                    format!("{}:{seq}", at(&session.id)),
                    message
                ))
            ),
        }
    }
    true
}

/// Rebuild the v4 header line pi's JSONL repo would have written for this
/// database row, so the SQLite and JSONL paths share one session mapper - and
/// so native resume emits a loadable v4 `.jsonl`.
fn db_header_value(row: &Value) -> Value {
    let mut header = serde_json::Map::new();
    header.insert("kind".to_owned(), json!("header"));
    header.insert("version".to_owned(), json!(SUPPORTED_JSONL_VERSION));
    if let Some(id) = row.get("id") {
        header.insert("id".to_owned(), id.clone());
    }
    if let Some(created_at) = row
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(parse_db_timestamp)
    {
        header.insert("createdAt".to_owned(), json!(created_at.timestamp_millis()));
    }
    if let Some(cwd) = row.get("cwd") {
        header.insert("cwd".to_owned(), cwd.clone());
    }
    if let Some(parent) = row.get("parent_session_id") {
        header.insert("parentSessionId".to_owned(), parent.clone());
    }
    // `metadata` is a JSON TEXT column; a value that will not re-parse is
    // carried as the raw string rather than dropped.
    if let Some(text) = row.get("metadata").and_then(Value::as_str) {
        header.insert(
            "metadata".to_owned(),
            serde_json::from_str(text).unwrap_or_else(|_| json!(text)),
        );
    }
    Value::Object(header)
}

/// Every mutation for one session, in `seq` order, each rebuilt as the v4 value
/// pi's JSONL codec would have written for it.
fn db_mutations(
    conn: &Connection,
    db_path: &Path,
    session_id: &str,
) -> Result<Vec<(i64, Value)>, AdapterError> {
    let mut out = Vec::new();
    collect_db_rows(
        conn,
        db_path,
        session_id,
        "SELECT seq, id, parent_id, type, timestamp, payload FROM entries WHERE session_id = ?1",
        |row| {
            let seq: i64 = row.get(0)?;
            let mut map = payload_object(&row.get::<_, String>(5)?);
            map.insert("kind".to_owned(), json!("entry"));
            map.insert("id".to_owned(), json!(row.get::<_, String>(1)?));
            map.insert(
                "parentId".to_owned(),
                row.get::<_, Option<String>>(2)?
                    .map_or(Value::Null, |p| json!(p)),
            );
            map.insert("type".to_owned(), json!(row.get::<_, String>(3)?));
            map.insert("seq".to_owned(), json!(seq));
            insert_db_timestamp(&mut map, &row.get::<_, String>(4)?);
            Ok((seq, Value::Object(map)))
        },
        &mut out,
    )?;
    collect_db_rows(
        conn,
        db_path,
        session_id,
        "SELECT seq, timestamp, payload FROM records WHERE session_id = ?1",
        |row| {
            let seq: i64 = row.get(0)?;
            // The records payload already carries id / lane / type verbatim
            // (`repo.ts::decodeRecord` only re-adds seq and timestamp).
            let mut map = payload_object(&row.get::<_, String>(2)?);
            map.insert("kind".to_owned(), json!("record"));
            map.insert("seq".to_owned(), json!(seq));
            insert_db_timestamp(&mut map, &row.get::<_, String>(1)?);
            Ok((seq, Value::Object(map)))
        },
        &mut out,
    )?;
    collect_db_rows(
        conn,
        db_path,
        session_id,
        "SELECT seq, lane, leaf_id FROM lane_moves WHERE session_id = ?1",
        |row| {
            let seq: i64 = row.get(0)?;
            Ok((
                seq,
                json!({
                    "kind": "lane",
                    "seq": seq,
                    "lane": row.get::<_, String>(1)?,
                    "leafId": row.get::<_, Option<String>>(2)?,
                }),
            ))
        },
        &mut out,
    )?;
    collect_db_rows(
        conn,
        db_path,
        session_id,
        "SELECT seq, kind, key, value FROM facts WHERE session_id = ?1",
        |row| {
            let seq: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            // Fact values are JSON-encoded scalars in SQLite; the JSONL codec
            // writes them bare.
            let decoded = row
                .get::<_, Option<String>>(3)?
                .and_then(|text| serde_json::from_str::<Value>(&text).ok());
            let mut map = serde_json::Map::new();
            map.insert("kind".to_owned(), json!("fact"));
            map.insert("seq".to_owned(), json!(seq));
            map.insert("fact".to_owned(), json!(kind));
            if let Some(target) = row.get::<_, Option<String>>(2)? {
                map.insert("targetId".to_owned(), json!(target));
            }
            if let Some(decoded) = decoded {
                map.insert(
                    if kind == "name" { "name" } else { "label" }.to_owned(),
                    decoded,
                );
            }
            Ok((seq, Value::Object(map)))
        },
        &mut out,
    )?;
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

fn collect_db_rows(
    conn: &Connection,
    db_path: &Path,
    session_id: &str,
    sql: &str,
    map: impl Fn(&rusqlite::Row) -> rusqlite::Result<(i64, Value)>,
    out: &mut Vec<(i64, Value)>,
) -> Result<(), AdapterError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|error| db_error(NAME, db_path, "prepare mutations", &error))?;
    let rows = stmt
        .query_map([session_id], map)
        .map_err(|error| db_error(NAME, db_path, "query mutations", &error))?;
    for row in rows {
        out.push(row.map_err(|error| db_error(NAME, db_path, "read mutation row", &error))?);
    }
    Ok(())
}

/// A payload column as the object the v4 mutation is built on. pi always writes
/// a JSON object there; anything else is wrapped rather than dropped, so a
/// surprise payload still reaches canonical
/// (spec.md#adapter-integrity-no-silent-drops).
fn payload_object(text: &str) -> serde_json::Map<String, Value> {
    match serde_json::from_str(text) {
        Ok(Value::Object(map)) => map,
        Ok(other) => json!({ "payload": other })
            .as_object()
            .cloned()
            .unwrap_or_default(),
        Err(_) => json!({ "payload": text })
            .as_object()
            .cloned()
            .unwrap_or_default(),
    }
}

/// An unparseable row timestamp leaves the field absent rather than failing the
/// row: the mutation still lands with the session anchor as its time (the same
/// absence default the timestamp-less `lane` / `fact` mutations take,
/// spec.md#model-no-synthesis) and the original column survives verbatim in
/// `raw_record`. Erroring would drop a row that is otherwise entirely readable.
fn insert_db_timestamp(map: &mut serde_json::Map<String, Value>, text: &str) {
    if let Some(dt) = parse_db_timestamp(text) {
        map.insert("timestamp".to_owned(), json!(dt.timestamp_millis()));
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end test for the pi-coding-agent adapter: ingest the committed fixture corpus
    //! and assert pond's canonical Session/Message/Part shape comes out the
    //! other side. The fixture lives under `tests/fixtures/adapter/pi-coding-agent/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::PartKind};
    use tempfile::TempDir;

    // Manifest-dir anchored: unit tests must not depend on the process cwd
    // (figment::Jail chdirs the whole test process while config tests run).
    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/pi-coding-agent/sessions"
    );

    #[test]
    fn probe_default_finds_pi_sessions_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &PiCodingAgentFactory,
            &[".pi", "agent", "sessions"],
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_fixture_corpus() -> anyhow::Result<()> {
        let adapter = PiCodingAgentAdapter::new(FIXTURES);
        crate::adapter::test_support::assert_native_restore(
            &PiCodingAgentFactory,
            &adapter,
            // pi-coding-agent relative paths embed the `sessions/` segment, so the corpus
            // root is FIXTURES' parent, not FIXTURES itself.
            std::path::Path::new(FIXTURES)
                .parent()
                .expect("FIXTURES is nested under a corpus root"),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pi_coding_agent_adapter_ingests_fixture_corpus_into_canonical_shape()
    -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = PiCodingAgentAdapter::new(FIXTURES);

        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert!(summary.accepted() > 0, "ingest must accept rows");
        assert_eq!(summary.dropped_events, 0, "no per-event drops expected");
        assert_eq!(
            summary.dropped_sessions, 0,
            "no session-level rejections expected"
        );
        assert_eq!(summary.skipped_files, 0, "no whole-file skips expected");

        let (sessions, messages, parts) = store.row_counts().await?;
        assert!(sessions > 0, "at least one pi-coding-agent session");
        assert!(messages > 0, "at least one pi-coding-agent message");
        assert!(parts > 0, "at least one pi-coding-agent Part");

        let mut saw_tool_call = false;
        let mut saw_tool_result = false;
        let mut saw_reasoning = false;
        for session_id in store.session_ids().await? {
            let session = store
                .get_session(&session_id)
                .await?
                .expect("session round-trips");
            assert_eq!(session.session.source_agent, NAME);
            assert!(
                !(*session.session.project).is_empty(),
                "spec.md#model-project-non-empty: project must be a real cwd",
            );
            for stored in &session.messages {
                for part in &stored.parts {
                    match &part.kind {
                        PartKind::ToolCall { .. } => saw_tool_call = true,
                        PartKind::ToolResult { .. } => saw_tool_result = true,
                        PartKind::Reasoning { .. } => saw_reasoning = true,
                        _ => {}
                    }
                }
            }
        }
        assert!(saw_tool_call, "corpus has assistant tool calls");
        assert!(saw_tool_result, "corpus has tool results");
        assert!(saw_reasoning, "corpus has assistant reasoning");
        Ok(())
    }

    #[test]
    fn unknown_nested_message_role_becomes_system_carrier() -> anyhow::Result<()> {
        let row = json!({
            "type": "message",
            "id": "mystery-message",
            "message": {
                "role": "mysteryRole",
                "content": [{"type": "text", "text": "not yet understood"}]
            }
        });
        let events = v3_events_from_row(
            "session-1",
            42,
            &row,
            DateTime::parse_from_rfc3339("2026-04-28T18:47:32.280Z")?.with_timezone(&Utc),
        )
        .map_err(anyhow::Error::msg)?;

        assert_eq!(events.len(), 1);
        let IngestEvent::Message(Message::System {
            id,
            content,
            options,
            ..
        }) = &events[0]
        else {
            panic!("unknown role must produce a System carrier");
        };
        assert_eq!(id, "mystery-message");
        assert_eq!(content.as_deref().map(String::as_str), Some("mysteryRole"));
        assert_eq!(
            raw_record(options)
                .and_then(|raw| raw.get("message").cloned())
                .and_then(|message| message.get("role").cloned()),
            Some(json!("mysteryRole")),
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fork_parent_ids_and_compaction_summary_are_preserved() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let root = temp.path().join("sessions");
        let path = root
            .join("project")
            .join("2026-05-01T00-00-00-000Z_fork.jsonl");
        write_jsonl_file(
            &path,
            &[
                json!({
                    "type": "session",
                    "version": 3,
                    "id": "pi-fork-session",
                    "timestamp": "2026-05-01T00:00:00.000Z",
                    "cwd": "/tmp/pi-fork",
                }),
                json!({
                    "type": "message",
                    "id": "parent-message",
                    "timestamp": "2026-05-01T00:00:01.000Z",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "parent"}],
                    },
                }),
                json!({
                    "type": "message",
                    "id": "child-a",
                    "parentId": "parent-message",
                    "timestamp": "2026-05-01T00:00:02.000Z",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "branch a"}],
                    },
                }),
                json!({
                    "type": "message",
                    "id": "child-b",
                    "parentId": "parent-message",
                    "timestamp": "2026-05-01T00:00:03.000Z",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "branch b"}],
                    },
                }),
                json!({
                    "type": "compaction",
                    "id": "compact-1",
                    "parentId": "child-b",
                    "timestamp": "2026-05-01T00:00:04.000Z",
                    "summary": "compact summary",
                }),
            ],
        )?;

        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(
            &store,
            &PiCodingAgentAdapter::new(&root),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(summary.dropped_events, 0);

        let session = store
            .get_session("pi-fork-session")
            .await?
            .expect("fixture session lands");
        let child_a = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == "child-a")
            .expect("first fork child lands");
        let child_b = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == "child-b")
            .expect("second fork child lands");
        for child in [child_a, child_b] {
            assert_eq!(
                child
                    .message
                    .options()
                    .get("source")
                    .and_then(|source| source.get("parent_id"))
                    .and_then(Value::as_str),
                Some("parent-message"),
            );
        }
        assert!(source_line(child_a.message.options()) < source_line(child_b.message.options()));

        let compact = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == "compact-1")
            .expect("compaction carrier lands");
        let Message::System { content, .. } = &compact.message else {
            panic!("compaction is preserved as a System carrier");
        };
        assert_eq!(
            content.as_deref().map(String::as_str),
            Some("compact summary")
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn foreign_serialization_reparses_as_pi_coding_agent() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let origin_store = Store::open_local(temp.path().join("origin-store")).await?;
        let origin = crate::adapter::OpencodeAdapter::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/adapter/opencode/storage"
        ));
        ingest_adapter(&origin_store, &origin, &crate::adapter::NoopOracle, |_| {}).await?;
        let session_id = origin_store
            .session_ids()
            .await?
            .into_iter()
            .next()
            .expect("opencode fixture has sessions");
        let session = origin_store
            .get_session(&session_id)
            .await?
            .expect("fixture session is readable");

        let restored_root = temp.path().join("pi-corpus");
        crate::adapter::write_restored_files(
            &restored_root,
            PiCodingAgentFactory.serialize(&session, RestoreFidelity::Foreign)?,
        )?;
        let restored_store = Store::open_local(temp.path().join("restored-store")).await?;
        let summary = ingest_adapter(
            &restored_store,
            &PiCodingAgentAdapter::new(restored_root.join("sessions")),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;

        assert!(summary.accepted() > 0);
        assert_eq!(summary.dropped_events, 0);
        Ok(())
    }

    /// spec.md#model-part-provenance: a tool result is harness-injected; an
    /// assistant turn's text/reasoning/tool-call parts are conversation.
    #[tokio::test(flavor = "multi_thread")]
    async fn tool_results_are_injected_assistant_parts_are_conversational() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = PiCodingAgentAdapter::new(FIXTURES);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        for session_id in store.session_ids().await? {
            let session = store
                .get_session(&session_id)
                .await?
                .expect("session round-trips");
            for stored in &session.messages {
                for part in &stored.parts {
                    match &part.kind {
                        PartKind::ToolResult { .. } => {
                            assert_eq!(part.provenance, Provenance::Injected);
                        }
                        PartKind::ToolCall { .. } | PartKind::Reasoning { .. } => {
                            assert_eq!(part.provenance, Provenance::Conversational);
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    // -- harness-v2: v4 JSONL ------------------------------------------------

    const V4_SESSION: &str = "v4-main-session";
    const V4_FORK: &str = "v4-fork-session";

    /// Copy the committed v4 fixture into a scratch sessions root so a test can
    /// mutate it (append a future mutation kind, tear the tail) without
    /// touching the corpus every other test reads.
    fn scratch_v4_corpus(temp: &TempDir) -> anyhow::Result<(PathBuf, PathBuf)> {
        let source = Path::new(FIXTURES)
            .join("--Users-user-Projects-harness-v2--")
            .join("2026-08-06T00-00-01-000Z_v4-main-session.jsonl");
        let root = temp.path().join("sessions");
        let dir = root.join("--Users-user-Projects-harness-v2--");
        std::fs::create_dir_all(&dir)?;
        let dest = dir.join("2026-08-06T00-00-01-000Z_v4-main-session.jsonl");
        std::fs::copy(&source, &dest)?;
        Ok((root, dest))
    }

    async fn ingest_into_temp_store(
        temp: &TempDir,
        adapter: &PiCodingAgentAdapter,
    ) -> anyhow::Result<(Store, crate::sessions::IngestSummary)> {
        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(&store, adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        Ok((store, summary))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn v4_header_carries_lineage_project_and_metadata() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, summary) =
            ingest_into_temp_store(&temp, &PiCodingAgentAdapter::new(FIXTURES)).await?;
        assert_eq!(summary.dropped_events, 0);
        assert_eq!(summary.dropped_sessions, 0);

        let main = store
            .get_session(V4_SESSION)
            .await?
            .expect("v4 fixture session lands");
        assert_eq!(main.session.source_agent, NAME);
        // The v4 header records the real absolute cwd, not v3's lossy slug.
        assert_eq!(&*main.session.project, "/Users/user/Projects/harness-v2");
        assert_eq!(main.session.parent_session_id, None);
        let source = main
            .session
            .options
            .get("source")
            .expect("source options present");
        assert_eq!(source.get("format"), Some(&json!(4)));
        assert_eq!(
            source.get("metadata"),
            Some(&json!({"harness": "v2", "fixture": "pond"})),
            "spec.md#model-lossless-projection: the header metadata bag is carried verbatim",
        );

        let fork = store
            .get_session(V4_FORK)
            .await?
            .expect("v4 fork session lands");
        assert_eq!(
            fork.session.parent_session_id.as_deref(),
            Some(V4_SESSION),
            "adapter-lineage: a v4 fork names its parent session",
        );
        Ok(())
    }

    /// Only `entry`-with-`message` rows are conversation. Every orchestration
    /// mutation - records, lane pointers, facts - must land as a System carrier
    /// so `search_text` stays clean without a per-adapter filter.
    #[tokio::test(flavor = "multi_thread")]
    async fn v4_orchestration_mutations_are_system_carriers() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, _) =
            ingest_into_temp_store(&temp, &PiCodingAgentAdapter::new(FIXTURES)).await?;
        let session = store
            .get_session(V4_SESSION)
            .await?
            .expect("v4 fixture session lands");

        let find = |id: &str| {
            session
                .messages
                .iter()
                .find(|stored| stored.message.id() == id)
                .unwrap_or_else(|| panic!("{id} lands"))
                .message
                .clone()
        };
        for (id, content) in [
            ("v4-run-1", "operation_started"),
            ("v4-tool-1", "tool_started"),
            ("v4-usage-1", "usage"),
            ("v4-entry-model", "model_change"),
            (
                "v4-entry-compaction",
                "Compacted the storage-rewrite discussion.",
            ),
            (
                "v4-entry-branch-summary",
                "Explored the SQLite backend, came back.",
            ),
            ("v4-entry-custom", "pond-fixture"),
        ] {
            let Message::System {
                content: got,
                options,
                ..
            } = find(id)
            else {
                panic!("{id} must be a System carrier, not conversation");
            };
            assert_eq!(got.as_deref().map(String::as_str), Some(content));
            assert!(
                raw_record(&options).is_some(),
                "{id} must keep its whole source mutation",
            );
        }

        // `lane` / `fact` mutations carry neither id nor timestamp; the id is
        // the session's own `seq` (pi ids cannot contain `:`) and the timestamp
        // falls back to the session anchor.
        let lane = find(&format!("{V4_SESSION}:19"));
        let Message::System { content, .. } = &lane else {
            panic!("a lane pointer move is a carrier");
        };
        assert_eq!(content.as_deref().map(String::as_str), Some("side"));
        let name_fact = find(&format!("{V4_SESSION}:22"));
        let Message::System { content, .. } = &name_fact else {
            panic!("a name fact is a carrier");
        };
        assert_eq!(
            content.as_deref().map(String::as_str),
            Some("harness-v2 storage rewrite"),
        );

        assert!(
            matches!(find("v4-entry-user"), Message::User { .. }),
            "a v4 message entry is real conversation",
        );
        assert!(matches!(
            find("v4-entry-assistant"),
            Message::Assistant { .. }
        ));
        assert!(matches!(find("v4-entry-tool-result"), Message::Tool { .. }));
        Ok(())
    }

    /// spec.md#adapter-integrity-no-silent-drops: a mutation kind a newer pi
    /// invents is well-formed input, not corruption - it degrades to a carrier
    /// that keeps the whole line, never an error and never a drop.
    #[tokio::test(flavor = "multi_thread")]
    async fn v4_unknown_mutation_kind_degrades_to_a_carrier() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (root, path) = scratch_v4_corpus(&temp)?;
        let mut text = std::fs::read_to_string(&path)?;
        text.push_str(
            r#"{"kind":"telepathy","seq":99,"id":"future-1","timestamp":1785974999000,"payload":{"from":"tomorrow"}}"#,
        );
        text.push('\n');
        std::fs::write(&path, text)?;

        let (store, summary) =
            ingest_into_temp_store(&temp, &PiCodingAgentAdapter::new(&root)).await?;
        assert_eq!(
            summary.dropped_events, 0,
            "unknown-but-well-formed is preserved"
        );
        let session = store
            .get_session(V4_SESSION)
            .await?
            .expect("session still lands");
        let future = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == "future-1")
            .expect("the future mutation is preserved");
        let Message::System {
            content, options, ..
        } = &future.message
        else {
            panic!("an unknown mutation kind must become a carrier");
        };
        assert_eq!(content.as_deref().map(String::as_str), Some("telepathy"));
        assert_eq!(
            raw_record(options).and_then(|raw| raw.get("payload").cloned()),
            Some(json!({"from": "tomorrow"})),
        );
        Ok(())
    }

    /// A crash mid-append leaves a truncated final line. The complete rows
    /// before it must still land, and the torn line must surface as a visible,
    /// counted skip naming the file rather than vanishing.
    #[tokio::test(flavor = "multi_thread")]
    async fn v4_torn_tail_keeps_the_whole_lines_and_counts_the_partial_one() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (root, path) = scratch_v4_corpus(&temp)?;
        let mut text = std::fs::read_to_string(&path)?;
        let whole_lines = text.lines().count();
        text.push_str(r#"{"kind":"entry","lane":"main","id":"v4-torn-part"#);
        std::fs::write(&path, text)?;

        let (store, summary) =
            ingest_into_temp_store(&temp, &PiCodingAgentAdapter::new(&root)).await?;
        assert_eq!(
            summary.skipped_files, 1,
            "the torn line surfaces as a named skip, not a silence",
        );
        let session = store
            .get_session(V4_SESSION)
            .await?
            .expect("session still lands");
        assert_eq!(
            session.messages.len(),
            whole_lines - 1,
            "every complete mutation before the tear still ingests (the header is eventless)",
        );
        Ok(())
    }

    /// A header naming a version this build cannot decode is a visible, counted
    /// skip that names the file - never a half-understood ingest.
    #[tokio::test(flavor = "multi_thread")]
    async fn v4_unsupported_version_is_a_named_skip() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (root, path) = scratch_v4_corpus(&temp)?;
        let text = std::fs::read_to_string(&path)?;
        std::fs::write(&path, text.replacen(r#""version":4"#, r#""version":5"#, 1))?;

        let (store, summary) =
            ingest_into_temp_store(&temp, &PiCodingAgentAdapter::new(&root)).await?;
        assert_eq!(summary.skipped_files, 1, "the file is skipped visibly");
        assert_eq!(summary.dropped_events, 0);
        assert!(
            store.session_ids().await?.is_empty(),
            "nothing from an undecodable version reaches the store",
        );
        Ok(())
    }

    /// A trailing `lane` / `fact` mutation carries no timestamp, so the
    /// freshness gate must NOT reuse an older line's watermark - otherwise the
    /// trailing mutation could never be ingested
    /// (spec.md#session-movement-complete).
    #[test]
    fn v4_trailing_timestampless_mutation_forces_a_reread() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (root, path) = scratch_v4_corpus(&temp)?;
        let adapter = PiCodingAgentAdapter::new(&root);
        assert_eq!(
            adapter.peek_watermark(&path),
            SourceWatermark::Opaque,
            "the fixture ends on a `fact` mutation, which has no timestamp",
        );

        let mut text = std::fs::read_to_string(&path)?;
        text.push_str(
            r#"{"kind":"entry","lane":"main","id":"later","type":"custom","customType":"x","parentId":null,"seq":24,"timestamp":1785974500000}"#,
        );
        text.push('\n');
        std::fs::write(&path, text)?;
        assert_eq!(
            adapter.peek_watermark(&path),
            SourceWatermark::At(1_785_974_500_000_000),
            "a timestamped tail yields a real watermark",
        );
        Ok(())
    }

    /// Foreign resume targets v3: it is what every shipped pi loads, so a
    /// session that cannot be replayed verbatim is reconstructed as v3 rather
    /// than as a format no released pi reads.
    #[tokio::test(flavor = "multi_thread")]
    async fn foreign_resume_of_a_v4_session_emits_v3() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, _) =
            ingest_into_temp_store(&temp, &PiCodingAgentAdapter::new(FIXTURES)).await?;
        let session = store
            .get_session(V4_SESSION)
            .await?
            .expect("v4 fixture session lands");

        let files = PiCodingAgentFactory.serialize(&session, RestoreFidelity::Foreign)?;
        let file = files.first().expect("one file per session");
        assert_eq!(file.actual_fidelity, RestoreFidelity::Foreign);
        let head: Value =
            serde_json::from_str(std::str::from_utf8(&file.bytes)?.lines().next().unwrap())?;
        assert_eq!(head.get("type"), Some(&json!("session")));
        assert_eq!(head.get("version"), Some(&json!(3)));
        Ok(())
    }

    // -- harness-v2: SQLite backend ------------------------------------------

    const SQLITE_DB: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/pi-coding-agent/sqlite/pi-sessions.sqlite"
    );

    fn sqlite_only_adapter(temp: &TempDir) -> anyhow::Result<PiCodingAgentAdapter> {
        // An empty JSONL root so the test exercises just the database half.
        let root = temp.path().join("empty-sessions");
        std::fs::create_dir_all(&root)?;
        Ok(PiCodingAgentAdapter::new(root).with_sqlite(SQLITE_DB))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_backend_maps_through_the_same_v4_mapper() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let adapter = sqlite_only_adapter(&temp)?;
        let (store, summary) = ingest_into_temp_store(&temp, &adapter).await?;
        assert_eq!(summary.dropped_events, 0);
        assert_eq!(summary.dropped_sessions, 0);

        let mut ids = store.session_ids().await?;
        ids.sort();
        assert_eq!(ids, ["sqlite-child-session", "sqlite-main-session"]);

        let main = store
            .get_session("sqlite-main-session")
            .await?
            .expect("sqlite session lands");
        assert_eq!(main.session.source_agent, NAME);
        assert_eq!(&*main.session.project, "/Users/user/Projects/harness-v2");
        assert_eq!(
            main.session
                .options
                .get("source")
                .and_then(|source| source.get("format")),
            Some(&json!(4)),
            "database rows are mapped as v4 mutations, not a fourth shape",
        );

        let child = store
            .get_session("sqlite-child-session")
            .await?
            .expect("sqlite child lands");
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some("sqlite-main-session"),
        );

        let mut saw_tool_call = false;
        let mut saw_tool_result = false;
        for stored in &main.messages {
            for part in &stored.parts {
                match &part.kind {
                    PartKind::ToolCall { name, .. } => {
                        saw_tool_call = true;
                        assert_eq!(name.as_deref().map(String::as_str), Some("bash"));
                    }
                    PartKind::ToolResult { .. } => saw_tool_result = true,
                    _ => {}
                }
            }
        }
        assert!(saw_tool_call && saw_tool_result);

        // Facts and lane moves ride the same carrier path as the JSONL side.
        let carriers: Vec<&str> = main
            .messages
            .iter()
            .filter_map(|stored| match &stored.message {
                Message::System { content, .. } => content.as_deref().map(String::as_str),
                _ => None,
            })
            .collect();
        assert!(
            carriers.contains(&"sqlite backend probe"),
            "name fact lands"
        );
        assert!(carriers.contains(&"answer"), "label fact lands");
        assert!(carriers.contains(&"side"), "lane move lands");
        Ok(())
    }

    /// A SQLite-origin session has no source file, so native resume writes the
    /// portable artifact: a v4 `.jsonl` pi can load. Round-tripping it back
    /// through the JSONL reader is the conformance check.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_native_resume_emits_a_loadable_v4_jsonl() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let adapter = sqlite_only_adapter(&temp)?;
        let (store, _) = ingest_into_temp_store(&temp, &adapter).await?;
        let session = store
            .get_session("sqlite-main-session")
            .await?
            .expect("sqlite session lands");

        let files = PiCodingAgentFactory.serialize(&session, RestoreFidelity::Native)?;
        let file = files.first().expect("one file per session");
        assert_eq!(file.actual_fidelity, RestoreFidelity::Native);
        assert_eq!(
            file.relative_path,
            Path::new("sessions")
                .join("--Users-user-Projects-harness-v2--")
                .join("2026-08-06T00-00-32-000Z_sqlite-main-session.jsonl"),
            "restore reproduces pi's own directory and file naming",
        );
        let head: Value =
            serde_json::from_str(std::str::from_utf8(&file.bytes)?.lines().next().unwrap())?;
        assert_eq!(head.get("kind"), Some(&json!("header")));
        assert_eq!(head.get("version"), Some(&json!(4)));

        let restored_root = temp.path().join("pi-home");
        crate::adapter::write_restored_files(&restored_root, files)?;
        let reread = Store::open_local(temp.path().join("reread-store")).await?;
        let summary = ingest_adapter(
            &reread,
            &PiCodingAgentAdapter::new(restored_root.join("sessions")),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(summary.dropped_events, 0);

        let round_tripped = reread
            .get_session("sqlite-main-session")
            .await?
            .expect("the resumed file reads back as the same session");
        assert_eq!(round_tripped.session.project, session.session.project);
        assert_eq!(round_tripped.session.created_at, session.session.created_at);
        assert_eq!(round_tripped.messages.len(), session.messages.len());
        Ok(())
    }

    /// Restore never overwrites: `--out-dir` is a live client's data directory,
    /// so an existing destination is refused before anything is written.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_refuses_to_overwrite_an_existing_file() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let (store, _) =
            ingest_into_temp_store(&temp, &PiCodingAgentAdapter::new(FIXTURES)).await?;
        let session = store
            .get_session(V4_SESSION)
            .await?
            .expect("v4 fixture session lands");
        let files = PiCodingAgentFactory.serialize(&session, RestoreFidelity::Native)?;

        let root = temp.path().join("pi-home");
        crate::adapter::write_restored_files(&root, files.clone())?;
        let sibling = root.join("keep-me.txt");
        std::fs::write(&sibling, b"untouched")?;

        let error = crate::adapter::write_restored_files(&root, files)
            .expect_err("a second restore into the same root must be refused");
        assert!(
            error.to_string().contains("refusing to overwrite"),
            "the refusal names what it found: {error}",
        );
        assert_eq!(
            std::fs::read(&sibling)?,
            b"untouched",
            "a refused restore leaves the directory exactly as it was",
        );
        Ok(())
    }

    fn write_jsonl_file(path: &std::path::Path, records: &[Value]) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, jsonl_bytes(NAME, records)?)?;
        Ok(())
    }
}
