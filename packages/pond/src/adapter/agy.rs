//! agy adapter: Google's Antigravity CLI (`agy`) and its ACP server.
//!
//! Source: the Gemini home (`~/.gemini`, relocated by `$GEMINI_HOME`, which is
//! configured as an explicit `path`). Two writers share it, one subtree each -
//! `antigravity-cli/` (the `agy` binary, TUI and headless) and
//! `antigravity-acp/` (the ACP server) - and both keep one SQLite database per
//! conversation at `<lane>/conversations/<uuid>.db`. Every row is a protobuf
//! blob; `steps.step_payload` is a `gemini_coder.Step`, the conversation's unit
//! of record. The schema was recovered from the descriptors embedded in the agy
//! binary; format archaeology and the decision record live in
//! `docs/adapters/agy.md`.
//!
//! The database is a mutable store, not an append-only log (measured): an
//! interrupted step stays `RUNNING` until the next resume rewrites it to
//! `CANCELED` in place, `/rewind` deletes steps and later reuses their indexes
//! under a rotated `trajectory_id`, and the ACP server rewrites payloads on load
//! to strip thought signatures. Identity follows from that. A step is keyed by
//! the `(trajectory_id, idx)` it records for itself, so a rewound branch and
//! the one that replaced it never collide, and a step read before it settled
//! carries its status in the id, so the settled row lands as a new message
//! instead of being latched in its in-flight form (pond keeps the superset,
//! the hermes posture).
//!
//! Every ingested row keeps all of its columns in `options.source.raw_record`
//! (blobs base64), so each field stays recoverable
//! (spec.md#model-lossless-projection). Restore is refused: see
//! [`RESTORE_UNSUPPORTED`].

use std::path::{Path, PathBuf};

use async_stream::stream;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, types::ValueRef};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    PlanFuture, RestoreFidelity, RestoredFile, SkipOracle, SkipReason, SourceWatermark, SyncPlan,
    config_path,
    extract::{Extracted, extract_self_str, extract_str, extract_value, json_or_string},
    part_id, part_ordinal, source_in_sync, source_options,
    sqlite::{self, CHANNEL_CAP, emit},
    validate_path_id,
};
use crate::{
    sessions::{IngestEvent, SessionWithMessages},
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Role, Session},
};

const NAME: &str = "agy";
const SUBAGENT_AGENT: &str = "agy/subagent";
const CONVERSATIONS_DIR: &str = "conversations";
/// `PRAGMA user_version` of every database agy 1.0.9 through 1.2.0 writes. A
/// higher value is a schema move this build predates: a visible, counted skip.
const SUPPORTED_USER_VERSION: i64 = 1;

/// Tables pond does not ingest (docs/adapters/agy.md row 8): per-generation
/// telemetry and executor configuration snapshots. They carry no conversation,
/// agy reserializes them on every save with nondeterministic map order, and
/// `/rewind` deletes them. `trajectory_metadata_blob` is not listed: it is the
/// session record, read once into the Session row.
const NON_CAPTURE_TABLES: [&str; 2] = ["gen_metadata", "executor_metadata"];
const STEPS_TABLE: &str = "steps";
const METADATA_TABLE: &str = "trajectory_metadata_blob";
const PARENT_REFERENCES_TABLE: &str = "parent_references";

/// Why restore is refused, shared by the capability query and `serialize` so the
/// two surfaces can never drift.
const RESTORE_UNSUPPORTED: &str = "agy keeps each conversation as protobuf rows in a SQLite \
     database it rewrites in place, and pond stores the superset of every row version it has \
     seen, so there is no single native file to write back; `pond resume <id> --to claude-code` \
     (or any other restore target) rebuilds the conversation in a client pond can write";

/// `CortexStepType` values the adapter maps (the enum lives in the agy binary's
/// `exa.cortex_pb` descriptors). Every other kind is a placement rule 3 carrier.
mod step_type {
    pub(super) const USER_INPUT: i64 = 14;
    pub(super) const PLANNER_RESPONSE: i64 = 15;
    pub(super) const ERROR_MESSAGE: i64 = 17;
    pub(super) const RUN_COMMAND: i64 = 21;
    pub(super) const FIND: i64 = 25;
    pub(super) const EPHEMERAL_MESSAGE: i64 = 90;
    pub(super) const CONVERSATION_HISTORY: i64 = 98;
    pub(super) const SYSTEM_MESSAGE: i64 = 101;
    pub(super) const AGENCY_TOOL_CALL: i64 = 103;
    pub(super) const GENERIC: i64 = 132;
}

/// Kind steps that execute a tool at the top level rather than inside a
/// `GENERIC` result: the step's own payload field, then paths to the text the
/// model was shown, best first. Both are written by the harness the ACP server
/// spawns (`docs/adapters/agy.md` row 5).
const KIND_TOOL_RESULTS: [(i64, u64, &[&[u64]]); 2] = [
    // `CortexStepRunCommand`: `combined_output` (21) is a `RunCommandOutput`
    // whose `full` (1) is the text, `truncated` (2) the shortened form agy
    // shows instead; `stdout_output` (19) is the same message for one stream,
    // and `stdout` (4) the older plain string.
    (
        step_type::RUN_COMMAND,
        28,
        &[&[21, 1], &[21, 2], &[19, 1], &[4]],
    ),
    // `CortexStepFind`: `raw_output` (11), else the error it recorded instead.
    (step_type::FIND, 34, &[&[11], &[8]]),
];

/// `CortexStepStatus` values that mean the step is still being produced.
/// Anything else (DONE, ERROR, CANCELED, INTERRUPTED, CLEARED, INVALID, and
/// values newer than this build) is treated as settled.
const IN_FLIGHT_STATUSES: [(i64, &str); 5] = [
    (1, "pending"),
    (2, "running"),
    (8, "generating"),
    (9, "waiting"),
    (11, "queued"),
];
const STATUS_ERROR: i64 = 7;

/// `CortexTrajectoryReferenceType` values that record a fork of another
/// conversation (plain `/fork`, and a subagent forked from its parent).
const FORK_REFERENCE_TYPES: [u64; 2] = [2, 6];

/// Stateless factory: opens [`AgyAdapter`] instances and probes the Gemini home
/// for either agy lane.
pub struct AgyFactory;

impl AdapterFactory for AgyFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(AgyAdapter::new(config_path(NAME, config)?)))
    }

    /// `~/.gemini` is shared with the public Gemini CLI (`~/.gemini/tmp/...`),
    /// so the home alone proves nothing: it qualifies only when one of agy's
    /// own conversation directories exists under it.
    fn probe_default(&self, env: &Env) -> Option<Value> {
        let root = env.home.join(".gemini");
        Lane::ALL
            .iter()
            .any(|lane| root.join(lane.dir()).join(CONVERSATIONS_DIR).is_dir())
            .then(|| json!({ "path": root }))
    }

    fn restore_unsupported(&self) -> Option<&'static str> {
        Some(RESTORE_UNSUPPORTED)
    }

    /// Unreachable through `pond resume`, which asks
    /// [`Self::restore_unsupported`] first; an error rather than a panic for a
    /// caller that skips the capability query.
    fn serialize(
        &self,
        _session: &SessionWithMessages,
        _fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        Err(AdapterError::schema(NAME, NAME, RESTORE_UNSUPPORTED))
    }
}

/// Which agy writer produced a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lane {
    Cli,
    Acp,
}

impl Lane {
    const ALL: [Self; 2] = [Self::Cli, Self::Acp];

    fn dir(self) -> &'static str {
        match self {
            Self::Cli => "antigravity-cli",
            Self::Acp => "antigravity-acp",
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Acp => "acp",
        }
    }
}

/// Configured reader, rooted at a Gemini home.
#[derive(Debug, Clone)]
pub struct AgyAdapter {
    root: PathBuf,
}

impl AgyAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for AgyAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let root = self.root.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                list_conversations(&root).map(|listing| listing.conversations.len())
            })
            .await
            .map_err(join_error)?
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let root = self.root.clone();
        Box::pin(stream! {
            let peek = !oracle.is_empty();
            let heads = tokio::task::spawn_blocking(move || collect_heads(&root, peek)).await;
            let heads = match heads {
                Ok(Ok(heads)) => heads,
                Ok(Err(error)) => { yield Err(error); return; }
                Err(join) => { yield Err(join_error(join)); return; }
            };
            for skip in heads.unsupported {
                yield Ok(skip);
            }

            let mut survivors = Vec::with_capacity(heads.conversations.len());
            let mut fresh = 0usize;
            for (conversation, watermark) in heads.conversations {
                if watermark.is_some_and(|mark| source_in_sync(oracle, Some(&conversation.id), mark)) {
                    fresh += 1;
                    continue;
                }
                survivors.push(conversation);
            }
            if fresh > 0 {
                yield Ok(AdapterYield::SkippedBatch { reason: SkipReason::Fresh, count: fresh });
            }

            let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
            let handle = tokio::task::spawn_blocking(move || {
                for conversation in survivors {
                    if !read_conversation(&conversation, &tx) {
                        return;
                    }
                }
            });
            while let Some(item) = rx.recv().await {
                yield item;
            }
            if let Err(join) = handle.await {
                yield Err(join_error(join));
            }
        })
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> PlanFuture<'a> {
        let root = self.root.clone();
        Box::pin(async move {
            let peek = !oracle.is_empty();
            let heads = tokio::task::spawn_blocking(move || collect_heads(&root, peek))
                .await
                .map_err(join_error)??;
            if !peek {
                return Ok(Some(SyncPlan::all_pending(heads.conversations.len())));
            }
            Ok(Some(SyncPlan::from_heads(
                oracle,
                heads
                    .conversations
                    .iter()
                    .filter_map(|(conversation, mark)| {
                        mark.map(|mark| (Some(conversation.id.as_str()), mark))
                    }),
            )))
        })
    }
}

// -- Enumeration ------------------------------------------------------------

/// One conversation database.
#[derive(Debug, Clone)]
struct Conversation {
    lane: Lane,
    db: PathBuf,
    id: String,
}

#[derive(Default)]
struct Listing {
    conversations: Vec<Conversation>,
    /// Files the adapter recognizes but cannot read, surfaced as counted
    /// `Unsupported` skips, never folded into `Empty`.
    unsupported: Vec<AdapterYield>,
}

/// Every `<lane>/conversations/*.db` under the root, sorted per lane. A
/// `<uuid>.pb` with no `.db` twin is a pre-SQLite (encrypted) conversation pond
/// cannot decode; one beside its `.db` is the opaque copy `/fork` writes, a
/// documented non-capture sidecar.
fn list_conversations(root: &Path) -> Result<Listing, AdapterError> {
    let mut listing = Listing::default();
    for lane in Lane::ALL {
        let dir = root.join(lane.dir()).join(CONVERSATIONS_DIR);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(AdapterError::io(NAME, dir.display().to_string(), error)),
        };
        let mut dbs = Vec::new();
        let mut opaque = Vec::new();
        for entry in entries {
            let path = entry
                .map_err(|error| AdapterError::io(NAME, dir.display().to_string(), error))?
                .path();
            if !path.is_file() {
                continue;
            }
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("db") => dbs.push(path),
                Some("pb") => opaque.push(path),
                _ => {}
            }
        }
        dbs.sort();
        opaque.sort();
        for db in dbs {
            match db.file_stem().and_then(|stem| stem.to_str()) {
                Some(id) => listing.conversations.push(Conversation {
                    lane,
                    id: id.to_owned(),
                    db,
                }),
                None => listing.unsupported.push(AdapterYield::Skipped {
                    session_id: None,
                    project: None,
                    reason: SkipReason::Unsupported(format!(
                        "{}: conversation file name is not UTF-8",
                        db.display()
                    )),
                }),
            }
        }
        for pb in opaque {
            if pb.with_extension("db").exists() {
                continue;
            }
            listing.unsupported.push(AdapterYield::Skipped {
                session_id: pb
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(ToOwned::to_owned),
                project: None,
                reason: SkipReason::Unsupported(format!(
                    "{}: an encrypted pre-SQLite agy conversation, which pond cannot decode; \
                     opening it once in a current agy migrates it to a .db",
                    pb.display()
                )),
            });
        }
    }
    Ok(listing)
}

struct Heads {
    conversations: Vec<(Conversation, Option<SourceWatermark>)>,
    unsupported: Vec<AdapterYield>,
}

/// The listing plus, when there is an oracle to compare against, each
/// conversation's freshness watermark.
fn collect_heads(root: &Path, peek: bool) -> Result<Heads, AdapterError> {
    let listing = list_conversations(root)?;
    let conversations = listing
        .conversations
        .into_iter()
        .map(|conversation| {
            let mark = peek.then(|| peek_watermark(&conversation.db));
            (conversation, mark)
        })
        .collect();
    Ok(Heads {
        conversations,
        unsupported: listing.unsupported,
    })
}

/// The latest `created_at` across the conversation's steps (micros) - the same
/// value its newest message is stamped with, so an unchanged database gates as
/// fresh. Reads only the small `metadata` column. A settle that rewrites a step
/// in place without appending one does not move it; the next appended step (a
/// resume always appends one) or `pond sync --verify` picks the settle up
/// (docs/adapters/agy.md row 10).
fn peek_watermark(db: &Path) -> SourceWatermark {
    match std::fs::metadata(db) {
        Ok(meta) if meta.len() == 0 => return SourceWatermark::Empty,
        Ok(_) => {}
        Err(_) => return SourceWatermark::Opaque,
    }
    let Ok(conn) = open_db(db) else {
        return SourceWatermark::Opaque;
    };
    match has_table(&conn, STEPS_TABLE) {
        Ok(true) => {}
        Ok(false) => return SourceWatermark::Empty,
        Err(_) => return SourceWatermark::Opaque,
    }
    // Steps are only ever appended, so the newest row carries the newest
    // stamp: one row, one blob, no scan. `step_payload` comes along only as
    // the same fallback the reader uses when the side column is missing.
    let newest = conn
        .query_row(
            "SELECT metadata, step_payload FROM steps ORDER BY rowid DESC LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                ))
            },
        )
        .optional();
    match newest {
        Ok(None) => return SourceWatermark::Empty,
        Ok(Some((metadata, payload))) => {
            if let Some(created) = step_created_at(metadata.as_deref(), payload.as_deref()) {
                return SourceWatermark::At(created.timestamp_micros());
            }
        }
        Err(_) => return SourceWatermark::Opaque,
    }

    // The newest row records no readable stamp. Rather than re-read the whole
    // conversation on every sync, fall back to the largest stamp any step
    // carries; only a conversation where none does is `Opaque`.
    let Ok(mut stmt) = conn.prepare("SELECT metadata FROM steps") else {
        return SourceWatermark::Opaque;
    };
    let Ok(rows) = stmt.query_map([], |row| row.get::<_, Option<Vec<u8>>>(0)) else {
        return SourceWatermark::Opaque;
    };
    let latest = rows
        .filter_map(|row| step_created_at(row.ok().flatten().as_deref(), None))
        .map(|created| created.timestamp_micros())
        .max();
    match latest {
        Some(micros) => SourceWatermark::At(micros),
        None => SourceWatermark::Opaque,
    }
}

/// One step's `created_at`, read the way [`StepView`] reads it: the `metadata`
/// side column, else the same message inside `step_payload`.
fn step_created_at(metadata: Option<&[u8]>, payload: Option<&[u8]>) -> Option<DateTime<Utc>> {
    metadata
        .or_else(|| pb::message(payload?, 5))
        .and_then(|meta| pb::message(meta, 1))
        .and_then(pb::timestamp)
}

// -- Reading ----------------------------------------------------------------

/// Stream one conversation's yields; `false` when the consumer hung up.
/// Events go out as they are built (the hermes shape), so a long conversation
/// never has its whole decoded form and its raw records resident at once.
fn read_conversation(
    conversation: &Conversation,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    match conversation_yields(conversation, tx) {
        Ok(alive) => alive,
        Err(error) => {
            emit!(tx, Err(error));
            true
        }
    }
}

fn conversation_yields(
    conversation: &Conversation,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> Result<bool, AdapterError> {
    let location = conversation.db.display().to_string();
    let skip = |reason: SkipReason| {
        Ok(tx
            .blocking_send(Ok(AdapterYield::Skipped {
                session_id: Some(conversation.id.clone()),
                project: None,
                reason,
            }))
            .is_ok())
    };

    // agy pre-creates a conversation's file empty before its harness writes
    // the schema (the ACP server's `session_store.py` says so).
    let len = std::fs::metadata(&conversation.db)
        .map_err(|error| AdapterError::io(NAME, &location, error))?
        .len();
    if len == 0 {
        return skip(SkipReason::Empty);
    }
    let conn = open_db(&conversation.db)?;
    let user_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| db_error(&conversation.db, "read user_version", &error))?;
    if user_version > SUPPORTED_USER_VERSION {
        return skip(SkipReason::Unsupported(format!(
            "{location}: agy conversation schema version {user_version} is newer than this pond \
             build understands (supported: {SUPPORTED_USER_VERSION}); upgrade pond"
        )));
    }
    // One pass over `sqlite_master` answers every "is this table here" the
    // read needs.
    let tables = table_names(&conn, &conversation.db)?;
    let has = |name: &str| tables.iter().any(|table| table == name);
    if !has(STEPS_TABLE) {
        return skip(SkipReason::Empty);
    }
    // The id names a restore target file in every foreign client.
    validate_path_id(NAME, "conversation id", &conversation.id, &location)?;

    let steps = read_table(&conn, &conversation.db, STEPS_TABLE)?;
    if steps.is_empty() {
        return skip(SkipReason::Empty);
    }
    let metadata_rows = if has(METADATA_TABLE) {
        read_table(&conn, &conversation.db, METADATA_TABLE)?
    } else {
        Vec::new()
    };
    let mut carrier_tables = Vec::new();
    for table in &tables {
        if table == STEPS_TABLE
            || table == METADATA_TABLE
            || NON_CAPTURE_TABLES.contains(&table.as_str())
        {
            continue;
        }
        let rows = read_table(&conn, &conversation.db, table)?;
        carrier_tables.push((table.clone(), rows));
    }
    let sidecar = read_meta_sidecar(conversation)?;

    let metadata = metadata_rows
        .iter()
        .find(|row| row.text("id") == Some("main"))
        .or_else(|| metadata_rows.first())
        .and_then(|row| row.blob("data"))
        .map(TrajectoryMeta::decode)
        .unwrap_or_default();
    let decoded_steps: Vec<(&Row, Result<StepView<'_>, String>)> = steps
        .iter()
        .map(|row| (row, StepView::decode(row)))
        .collect();
    let anchor = metadata
        .created_at
        .or_else(|| {
            decoded_steps
                .iter()
                .filter_map(|(_, step)| step.as_ref().ok()?.meta.created_at)
                .min()
        })
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                &location,
                "conversation records no creation time and no step timestamp",
            )
        })?;
    let current_trajectory = carrier_tables
        .iter()
        .find(|(table, _)| table == "trajectory_meta")
        .and_then(|(_, rows)| rows.first())
        .and_then(|row| row.text("trajectory_id"))
        .map(ToOwned::to_owned);
    let references: Vec<TrajectoryReference> = carrier_tables
        .iter()
        .filter(|(table, _)| table == PARENT_REFERENCES_TABLE)
        .flat_map(|(_, rows)| rows.iter())
        .filter_map(|row| row.blob("data").map(TrajectoryReference::decode))
        .collect();

    let session = build_session(
        conversation,
        &metadata,
        &metadata_rows,
        sidecar.as_ref(),
        &references,
        anchor,
    )?;
    let session_id = session.id.clone();
    let send = |item| tx.blocking_send(item).is_ok();
    if !send(Ok(AdapterYield::Event(IngestEvent::Session(session)))) {
        return Ok(false);
    }

    for (table, rows) in &carrier_tables {
        for (position, row) in rows.iter().enumerate() {
            let message = table_carrier(&session_id, table, position, rows, row, anchor);
            if !send(Ok(AdapterYield::Event(IngestEvent::Message(message)))) {
                return Ok(false);
            }
        }
    }
    for (row, step) in &decoded_steps {
        let item = match step {
            Ok(step) => {
                for event in step_events(
                    &session_id,
                    row,
                    step,
                    current_trajectory.as_deref(),
                    anchor,
                ) {
                    if !send(Ok(AdapterYield::Event(event))) {
                        return Ok(false);
                    }
                }
                continue;
            }
            Err(reason) => Err(AdapterError::schema(
                NAME,
                format!("{location}#steps/{}", row.int("idx").unwrap_or(-1)),
                reason.clone(),
            )),
        };
        if !send(item) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The ACP lane's `<uuid>.meta` sidecar (`{"cwd": ...}`, merged and rewritten
/// by `session_store.write_sidecar_metadata`), when present. The server
/// rewrites the file on every save, so a crash mid-write leaves half a JSON
/// object; that must not wall off the conversation, whose steps are intact and
/// whose project has a documented fallback. Unparseable text rides into the
/// raw record verbatim instead of becoming a project source.
fn read_meta_sidecar(conversation: &Conversation) -> Result<Option<Value>, AdapterError> {
    let path = conversation.db.with_extension("meta");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(AdapterError::io(NAME, path.display().to_string(), error)),
    };
    Ok(Some(
        serde_json::from_str(&text).unwrap_or_else(|_| json!({ "unparsed": text })),
    ))
}

// -- Session ----------------------------------------------------------------

fn build_session(
    conversation: &Conversation,
    metadata: &TrajectoryMeta,
    metadata_rows: &[Row],
    sidecar: Option<&Value>,
    references: &[TrajectoryReference],
    anchor: DateTime<Utc>,
) -> Result<Session, AdapterError> {
    let location = conversation.db.display().to_string();
    let (project, project_source) = session_project(metadata, sidecar).ok_or_else(|| {
        AdapterError::schema(
            NAME,
            &location,
            "conversation records no workspace, no cwd sidecar and no project id",
        )
    })?;

    // A subagent names its parent in its own metadata. A fork records the
    // parent and the cut point in `parent_references`; the newest reference to
    // another conversation is the fork itself (older ones are inherited
    // history, a rewind references the conversation's own trajectory).
    let subagent_parent = metadata.parent_conversation_id.clone();
    let fork = references.iter().rev().find(|reference| {
        reference
            .reference_type
            .is_some_and(|kind| FORK_REFERENCE_TYPES.contains(&kind))
            && reference
                .conversation_id
                .as_deref()
                .is_some_and(|parent| parent != conversation.id)
    });
    let (parent_session_id, parent_message_id) = match (&subagent_parent, fork) {
        (Some(parent), _) => (Some(parent.clone()), None),
        (None, Some(fork)) => (
            fork.conversation_id.clone(),
            fork.trajectory_id
                .as_deref()
                .map(|trajectory| step_message_id(trajectory, fork.step_index.unwrap_or(0))),
        ),
        (None, None) => (None, None),
    };
    let source_agent = if subagent_parent.is_some() {
        SUBAGENT_AGENT
    } else {
        NAME
    };

    let raw = json!({
        "lane": conversation.lane.tag(),
        "trajectory_metadata_blob": metadata_rows
            .iter()
            .map(|row| row.raw(METADATA_TABLE))
            .collect::<Vec<_>>(),
        "meta_sidecar": sidecar,
    });
    let mut agy = Map::new();
    agy.insert("lane".to_owned(), json!(conversation.lane.tag()));
    agy.insert("project_source".to_owned(), json!(project_source));
    for (key, value) in metadata.summary() {
        agy.insert(key.to_owned(), value);
    }
    let mut options = source_options(NAME, &raw);
    options.insert(NAME.to_owned(), Value::Object(agy));

    Ok(Session {
        id: conversation.id.clone(),
        parent_session_id,
        parent_message_id,
        source_agent: source_agent.to_owned(),
        created_at: anchor,
        project,
        options,
    })
}

/// `Session.project`, first real datum wins: the workspace folder agy recorded
/// for the conversation, its workspace URI list, the ACP sidecar's cwd, then
/// agy's own project id (a headless run without `--add-dir` records no
/// workspace at all, but always a project). File URIs are decoded once here
/// (spec.md#adapter-integrity-opaque-ids).
fn session_project(
    metadata: &TrajectoryMeta,
    sidecar: Option<&Value>,
) -> Option<(Extracted<String>, &'static str)> {
    // A `file://` URI decodes to its path; a bare path is already one. Any
    // other scheme (a remote workspace) is not a path pond can record, so it
    // falls through to the next source in the chain rather than landing
    // undecoded in `Session.project`.
    let from_uri = |uri: &str| {
        let path = match file_uri_path(uri) {
            Some(path) => path,
            None if !uri.contains("://") => uri.to_owned(),
            None => return None,
        };
        extract_self_str(&Value::String(path))
    };
    metadata
        .workspace_folders
        .iter()
        .find_map(|uri| from_uri(uri))
        .map(|project| (project, "workspace"))
        .or_else(|| {
            metadata
                .workspace_uris
                .iter()
                .find_map(|uri| from_uri(uri))
                .map(|project| (project, "workspace_uri"))
        })
        .or_else(|| {
            sidecar
                .and_then(|meta| extract_str(meta, "cwd"))
                .filter(|cwd| !cwd.is_empty())
                .map(|cwd| (cwd, "meta_cwd"))
        })
        .or_else(|| {
            metadata
                .project_id
                .clone()
                .and_then(|id| extract_self_str(&Value::String(id)))
                .map(|project| (project, "project_id"))
        })
}

/// `file:///tmp/x` -> `/tmp/x`, `file:///C:/x` -> `C:/x`, percent-decoded.
/// `None` for anything that is not a well-formed file URI.
fn file_uri_path(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("file://")?;
    let bytes = rest.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    let path = String::from_utf8(decoded).ok()?;
    let drive = path.as_bytes();
    if drive.len() >= 3 && drive[0] == b'/' && drive[1].is_ascii_alphabetic() && drive[2] == b':' {
        return Some(path[1..].to_owned());
    }
    (!path.is_empty()).then_some(path)
}

// -- Steps ------------------------------------------------------------------

/// The canonical id of a step: the trajectory it records for itself plus its
/// index. Equal on every re-read of the same step, distinct across a rewind
/// (the trajectory rotates) and shared with a fork's copy (the copy keeps the
/// parent's trajectory ids), which is what lets a fork's `parent_message_id`
/// name the cut point.
fn step_message_id(trajectory: &str, idx: u64) -> String {
    format!("{trajectory}:{idx:06}")
}

fn in_flight_status(status: i64) -> Option<&'static str> {
    IN_FLIGHT_STATUSES
        .iter()
        .find(|(value, _)| *value == status)
        .map(|(_, name)| *name)
}

/// One `steps` row, decoded as far as the mapping needs. `step_type` and
/// `status` stay optional: a row that records neither is carried as-is rather
/// than mapped against a zero the writer never wrote.
struct StepView<'a> {
    idx: u64,
    step_type: Option<i64>,
    status: Option<i64>,
    payload: &'a [u8],
    meta: StepMeta<'a>,
    error: Option<&'a [u8]>,
}

impl<'a> StepView<'a> {
    fn decode(row: &'a Row) -> Result<Self, String> {
        let idx = row.int("idx").ok_or("step row has no integer idx")?;
        // The index is half of every message id in the conversation, so a
        // value that cannot form one is a visible error, not a zero.
        let idx = u64::try_from(idx).map_err(|_| "step row has a negative idx")?;
        let payload = row
            .blob("step_payload")
            .ok_or("step row has no step_payload")?;
        pb::validate(payload).map_err(|_| "step_payload is not a well-formed protobuf message")?;
        // The side columns are byte copies of the payload's fields (measured on
        // every captured step); the column wins, the payload backs it up. A
        // column that is not a whole message is a typed error like the payload:
        // half a `metadata` would silently re-role the step.
        let meta = row.blob("metadata").or_else(|| pb::message(payload, 5));
        if let Some(meta) = meta {
            pb::validate(meta).map_err(|_| "metadata is not a well-formed protobuf message")?;
        }
        let meta = meta.map(StepMeta::decode).unwrap_or_default();
        let error = row
            .blob("error_details")
            .or_else(|| pb::message(payload, 31));
        if let Some(error) = error {
            pb::validate(error)
                .map_err(|_| "error_details is not a well-formed protobuf message")?;
        }
        Ok(Self {
            idx,
            step_type: row
                .int("step_type")
                .or_else(|| pb::uint(payload, 1).map(|value| value as i64)),
            status: row
                .int("status")
                .or_else(|| pb::uint(payload, 4).map(|value| value as i64)),
            payload,
            meta,
            error,
        })
    }

    /// Still being produced, and under which name; `None` once settled or when
    /// the row records no status at all.
    fn in_flight(&self) -> Option<&'static str> {
        self.status.and_then(in_flight_status)
    }

    fn message_id(&self, fallback_trajectory: Option<&str>) -> String {
        let idx = self.idx;
        let base = match self.meta.trajectory_id.or(fallback_trajectory) {
            Some(trajectory) => step_message_id(trajectory, idx),
            None => format!("step:{idx:06}"),
        };
        match self.in_flight() {
            Some(status) => format!("{base}:{status}"),
            None => base,
        }
    }

    fn options(&self, row: &Row) -> ProviderOptions {
        let mut agy = Map::new();
        agy.insert("step_index".to_owned(), json!(self.idx));
        if let Some(step_type) = self.step_type {
            agy.insert("step_type".to_owned(), json!(step_type));
        }
        if let Some(status) = self.status {
            agy.insert("status".to_owned(), json!(status));
        }
        if let Some(source) = self.meta.source {
            agy.insert("source".to_owned(), json!(source));
        }
        if let Some(trajectory) = self.meta.trajectory_id {
            agy.insert("trajectory_id".to_owned(), json!(trajectory));
        }
        if let Some(execution) = self.meta.execution_id {
            agy.insert("execution_id".to_owned(), json!(execution));
        }
        if let Some(usage) = self.meta.usage {
            let usage = usage_summary(usage);
            if !usage.is_empty() {
                agy.insert("usage".to_owned(), Value::Object(usage));
            }
        }
        let mut options = source_options(NAME, &row.raw(STEPS_TABLE));
        options.insert(NAME.to_owned(), Value::Object(agy));
        options
    }
}

/// `CortexStepMetadata`, the fields the mapping reads.
#[derive(Default)]
struct StepMeta<'a> {
    created_at: Option<DateTime<Utc>>,
    source: Option<u64>,
    trajectory_id: Option<&'a str>,
    execution_id: Option<&'a str>,
    tool_call: Option<ChatToolCall<'a>>,
    usage: Option<&'a [u8]>,
}

impl<'a> StepMeta<'a> {
    fn decode(meta: &'a [u8]) -> Self {
        Self {
            created_at: pb::message(meta, 1).and_then(pb::timestamp),
            source: pb::uint(meta, 3),
            tool_call: pb::message(meta, 4).map(ChatToolCall::decode),
            usage: pb::message(meta, 9),
            execution_id: pb::string(meta, 12).filter(|id| !id.is_empty()),
            trajectory_id: pb::message(meta, 20)
                .and_then(|info| pb::string(info, 1))
                .filter(|id| !id.is_empty()),
        }
    }
}

/// `ChatToolCall {id, name, arguments_json}`.
struct ChatToolCall<'a> {
    id: Option<&'a str>,
    name: Option<&'a str>,
    arguments_json: Option<&'a str>,
}

impl<'a> ChatToolCall<'a> {
    fn decode(call: &'a [u8]) -> Self {
        Self {
            id: pb::string(call, 1).filter(|id| !id.is_empty()),
            name: pb::string(call, 2).filter(|name| !name.is_empty()),
            arguments_json: pb::string(call, 3),
        }
    }
}

/// `ModelUsageStats` counters worth a column in analytics; the full message
/// stays in the raw record.
fn usage_summary(usage: &[u8]) -> Map<String, Value> {
    const FIELDS: [(u64, &str); 7] = [
        (1, "model"),
        (2, "input_tokens"),
        (3, "output_tokens"),
        (4, "cache_write_tokens"),
        (5, "cache_read_tokens"),
        (9, "thinking_output_tokens"),
        (10, "response_output_tokens"),
    ];
    FIELDS
        .iter()
        .filter_map(|(number, key)| {
            pb::uint(usage, *number).map(|value| ((*key).to_owned(), json!(value)))
        })
        .collect()
}

/// Map one step to its message and parts. A step still in flight, and any kind
/// without a canonical shape, is a placement rule 3 carrier (the whole row in
/// options); a settled kind whose content decodes to nothing is one too.
fn step_events(
    session_id: &str,
    row: &Row,
    step: &StepView<'_>,
    fallback_trajectory: Option<&str>,
    anchor: DateTime<Utc>,
) -> Vec<IngestEvent> {
    let message_id = step.message_id(fallback_trajectory);
    let timestamp = step.meta.created_at.unwrap_or(anchor);
    let options = step.options(row);
    let ids = PartIds {
        session_id,
        message_id: &message_id,
    };

    let mapped = match (step.in_flight(), step.step_type) {
        (Some(_), _) | (_, None) => None,
        (None, Some(step_type::USER_INPUT)) => {
            user_parts(&ids, step).map(|parts| (Role::User, parts))
        }
        (None, Some(step_type::PLANNER_RESPONSE)) => {
            planner_parts(&ids, step).map(|parts| (Role::Assistant, parts))
        }
        (None, Some(_)) if step.meta.tool_call.is_some() => {
            tool_result_part(&ids, step).map(|part| (Role::Tool, vec![part]))
        }
        (None, Some(_)) => None,
    };
    let (role, parts) = mapped.unwrap_or((Role::System, Vec::new()));

    let message = match role {
        // Rule 3: an unmapped step is carried whole, with whatever text a
        // harness step addressed to the model carries as its content.
        Role::System => Message::System {
            id: message_id,
            session_id: session_id.to_owned(),
            timestamp,
            content: step_text(step)
                .and_then(|text| extract_self_str(&Value::String(text.to_owned()))),
            options,
        },
        Role::User => Message::User {
            id: message_id,
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
        Role::Assistant => Message::Assistant {
            id: message_id,
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
        Role::Tool => Message::Tool {
            id: message_id,
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
    };
    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    events
}

struct PartIds<'a> {
    session_id: &'a str,
    message_id: &'a str,
}

impl PartIds<'_> {
    fn part(&self, ordinal: usize, provenance: Provenance, kind: PartKind) -> Part {
        Part {
            session_id: self.session_id.to_owned(),
            id: part_id(self.message_id, ordinal),
            message_id: self.message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance,
            options: ProviderOptions::new(),
            kind,
        }
    }
}

fn text_value(text: &str) -> Option<Extracted<String>> {
    (!text.is_empty())
        .then(|| extract_self_str(&Value::String(text.to_owned())))
        .flatten()
}

/// `CortexStepUserInput`: the typed prompt is the `items[].text` chunks (scope
/// items - `@` mentions - stay in the raw record), falling back to
/// `user_response` and the legacy `query`; images and media become file parts.
/// The prompt is the user's own words: agy's `<USER_REQUEST>` wrapping and
/// metadata blocks exist only in the brain mirror, never in the database.
fn user_parts(ids: &PartIds<'_>, step: &StepView<'_>) -> Option<Vec<Part>> {
    let input = pb::message(step.payload, 19)?;
    let mut texts: Vec<&str> = pb::messages(input, 3)
        .into_iter()
        .filter_map(|item| pb::string(item, 1))
        .filter(|text| !text.is_empty())
        .collect();
    if texts.is_empty() {
        texts.extend(
            [pb::string(input, 2), pb::string(input, 1)]
                .into_iter()
                .flatten()
                .filter(|text| !text.is_empty())
                .take(1),
        );
    }
    let mut parts = Vec::new();
    for text in texts {
        let ordinal = parts.len();
        parts.push(ids.part(
            ordinal,
            Provenance::Conversational,
            PartKind::Text {
                text: text_value(text),
            },
        ));
    }
    for image in pb::messages(input, 5) {
        let mime = pb::string(image, 2).filter(|mime| !mime.is_empty());
        let data = match (pb::string(image, 1), pb::string(image, 4)) {
            (Some(base64), _) if !base64.is_empty() => FileData::String(base64.to_owned()),
            (_, Some(uri)) if !uri.is_empty() => FileData::Url(uri.to_owned()),
            _ => continue,
        };
        let ordinal = parts.len();
        parts.push(ids.part(
            ordinal,
            Provenance::Conversational,
            PartKind::File {
                media_type: mime.map(ToOwned::to_owned),
                file_name: None,
                data,
            },
        ));
    }
    for media in pb::messages(input, 9) {
        let mime = pb::string(media, 1).filter(|mime| !mime.is_empty());
        let data = match (pb::message(media, 2), pb::string(media, 5)) {
            (Some(bytes), _) if !bytes.is_empty() => FileData::Bytes(bytes.to_vec()),
            (_, Some(uri)) if !uri.is_empty() => FileData::Url(uri.to_owned()),
            _ => continue,
        };
        let ordinal = parts.len();
        parts.push(ids.part(
            ordinal,
            Provenance::Conversational,
            PartKind::File {
                media_type: mime.map(ToOwned::to_owned),
                file_name: None,
                data,
            },
        ));
    }
    (!parts.is_empty()).then_some(parts)
}

/// `CortexStepPlannerResponse`: thinking, then the response text, then each
/// tool call the model declared. The executing tool step carries the same
/// call id, which is the pairing (docs/adapters/agy.md row 5).
fn planner_parts(ids: &PartIds<'_>, step: &StepView<'_>) -> Option<Vec<Part>> {
    let response = pb::message(step.payload, 20)?;
    let mut parts = Vec::new();
    if let Some(thinking) = pb::string(response, 3).and_then(text_value) {
        parts.push(ids.part(
            parts.len(),
            Provenance::Conversational,
            PartKind::Reasoning {
                text: Some(thinking),
            },
        ));
    }
    if let Some(text) = pb::string(response, 1).and_then(text_value) {
        parts.push(ids.part(
            parts.len(),
            Provenance::Conversational,
            PartKind::Text { text: Some(text) },
        ));
    }
    for call in pb::messages(response, 7) {
        let call = ChatToolCall::decode(call);
        let params = call
            .arguments_json
            .map(json_or_string)
            .and_then(|value| extract_value(&json!({ "params": value }), "params"))
            .map_or(Value::Null, |params| params.as_ref().clone());
        parts.push(
            ids.part(
                parts.len(),
                Provenance::Conversational,
                PartKind::ToolCall {
                    call_id: call
                        .id
                        .and_then(|id| extract_self_str(&Value::String(id.to_owned()))),
                    name: call
                        .name
                        .and_then(|name| extract_self_str(&Value::String(name.to_owned()))),
                    params,
                    provider_executed: false,
                },
            ),
        );
    }
    (!parts.is_empty()).then_some(parts)
}

/// A settled tool step: the call it executed (from its metadata) and what came
/// back - the kind's own result, else, on an errored step, its recorded error
/// text. `None` when nothing came back at all, and none is invented
/// (spec.md#model-no-synthesis).
fn tool_result_part(ids: &PartIds<'_>, step: &StepView<'_>) -> Option<Part> {
    let call = step.meta.tool_call.as_ref()?;
    let failed = step.status == Some(STATUS_ERROR);
    let result = match step.step_type {
        Some(step_type::GENERIC) => pb::message(step.payload, 140)
            .and_then(|generic| pb::message(generic, 2))
            .and_then(|result| pb::string(result, 1))
            .map(|text| Value::String(text.to_owned())),
        Some(step_type::AGENCY_TOOL_CALL) => pb::message(step.payload, 116)
            .and_then(agency_tool_response)
            .map(json_or_string),
        Some(kind) => KIND_TOOL_RESULTS
            .iter()
            .find(|(step_type, _, _)| *step_type == kind)
            .and_then(|(_, payload_field, paths)| {
                let body = pb::message(step.payload, *payload_field)?;
                paths.iter().find_map(|path| field_path(body, path))
            })
            .map(|text| Value::String(text.to_owned())),
        None => None,
    }
    .or_else(|| {
        failed
            .then(|| step.error.and_then(error_text))
            .flatten()
            .map(|text| Value::String(text.to_owned()))
    })?;
    let result = extract_value(&json!({ "result": result }), "result")?;
    Some(
        ids.part(
            0,
            // spec.md#model-part-provenance: tool output is runtime-produced.
            Provenance::Injected,
            PartKind::ToolResult {
                call_id: call
                    .id
                    .and_then(|id| extract_self_str(&Value::String(id.to_owned()))),
                name: call
                    .name
                    .and_then(|name| extract_self_str(&Value::String(name.to_owned()))),
                is_failure: failed,
                result: result.as_ref().clone(),
            },
        ),
    )
}

/// A non-empty string at a path of field numbers: every number but the last
/// steps into a submessage.
fn field_path<'a>(body: &'a [u8], path: &[u64]) -> Option<&'a str> {
    let (text, parents) = path.split_last()?;
    let mut cursor = body;
    for field in parents {
        cursor = pb::message(cursor, *field)?;
    }
    pb::string(cursor, *text).filter(|text| !text.is_empty())
}

/// `CortexStepAgencyToolCall.response_messages[]` holds `google.protobuf.Any`
/// wrappers; an `antigravity.localharness.ToolResponse` carries the tool name
/// (field 1) and its JSON result text (field 2) - observed on the ACP lane's
/// client-side tools.
fn agency_tool_response(agency: &[u8]) -> Option<&str> {
    pb::messages(agency, 4).into_iter().find_map(|any| {
        let type_url = pb::string(any, 1)?;
        if !type_url.ends_with("/antigravity.localharness.ToolResponse") {
            return None;
        }
        pb::message(any, 2).and_then(|response| pb::string(response, 2))
    })
}

/// `CortexErrorDetails`: the full error, else the short one, else the
/// user-facing message.
fn error_text(error: &[u8]) -> Option<&str> {
    [3, 2, 1]
        .into_iter()
        .find_map(|number| pb::string(error, number).filter(|text| !text.is_empty()))
}

/// The human-readable content of a harness step that becomes a System message.
fn step_text<'a>(step: &StepView<'a>) -> Option<&'a str> {
    if step.in_flight().is_some() {
        return None;
    }
    // Field numbers of each kind's own text, from the descriptors in the agy
    // binary: `CortexStepSystemMessage.message` 1, `...EphemeralMessage.content`
    // 1, `...ConversationHistory.content` 1, `...ErrorMessage.error` 3.
    let text = match step.step_type? {
        step_type::SYSTEM_MESSAGE => pb::message(step.payload, 114).and_then(|m| pb::string(m, 1)),
        step_type::EPHEMERAL_MESSAGE => {
            pb::message(step.payload, 103).and_then(|m| pb::string(m, 1))
        }
        step_type::CONVERSATION_HISTORY => {
            pb::message(step.payload, 111).and_then(|m| pb::string(m, 1))
        }
        step_type::ERROR_MESSAGE => pb::message(step.payload, 24)
            .and_then(|m| pb::message(m, 3))
            .and_then(error_text),
        _ => None,
    };
    text.filter(|text| !text.is_empty())
}

/// A row of a table that maps to no message (`trajectory_meta`,
/// `parent_references`, `battle_mode_infos`, and any table a later agy adds):
/// placement rule 3, keyed by the row's primary key and stamped with the
/// conversation anchor.
fn table_carrier(
    session_id: &str,
    table: &str,
    position: usize,
    siblings: &[Row],
    row: &Row,
    anchor: DateTime<Utc>,
) -> Message {
    let key = match row.cells.first() {
        // Every table agy writes today has its primary key first, which keeps
        // the id stable across syncs. A future table whose first column
        // repeats falls back to the row's position rather than colliding.
        Some(Cell::Int(value)) if unique_first_cell(siblings, row) => format!("{value:06}"),
        Some(Cell::Text(value)) if unique_first_cell(siblings, row) => value.clone(),
        _ => format!("row{position:06}"),
    };
    let mut agy = Map::new();
    agy.insert("table".to_owned(), json!(table));
    if table == PARENT_REFERENCES_TABLE
        && let Some(reference) = row.blob("data").map(TrajectoryReference::decode)
    {
        agy.insert("reference".to_owned(), reference.summary());
    }
    let mut options = source_options(NAME, &row.raw(table));
    options.insert(NAME.to_owned(), Value::Object(agy));
    Message::System {
        id: format!("{table}:{key}"),
        session_id: session_id.to_owned(),
        timestamp: anchor,
        content: None,
        options,
    }
}

/// Whether `row`'s first cell identifies it among `siblings` - the test that
/// decides if it can serve as the row's key.
fn unique_first_cell(siblings: &[Row], row: &Row) -> bool {
    let same = |other: &Row| match (other.cells.first(), row.cells.first()) {
        (Some(Cell::Int(a)), Some(Cell::Int(b))) => a == b,
        (Some(Cell::Text(a)), Some(Cell::Text(b))) => a == b,
        _ => false,
    };
    siblings.iter().filter(|other| same(other)).count() == 1
}

// -- Trajectory records -----------------------------------------------------

/// `CortexTrajectoryMetadata`, the fields identity, project and lineage read.
#[derive(Default)]
struct TrajectoryMeta {
    created_at: Option<DateTime<Utc>>,
    workspace_folders: Vec<String>,
    workspace_uris: Vec<String>,
    parent_conversation_id: Option<String>,
    root_conversation_id: Option<String>,
    project_id: Option<String>,
    nesting_depth: Option<u64>,
    agent_name: Option<String>,
}

impl TrajectoryMeta {
    fn decode(blob: &[u8]) -> Self {
        let owned =
            |text: Option<&str>| text.filter(|text| !text.is_empty()).map(ToOwned::to_owned);
        Self {
            created_at: pb::message(blob, 2).and_then(pb::timestamp),
            workspace_folders: pb::messages(blob, 1)
                .into_iter()
                .filter_map(|workspace| owned(pb::string(workspace, 1)))
                .collect(),
            workspace_uris: pb::strings(blob, 7)
                .into_iter()
                .filter(|uri| !uri.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
            parent_conversation_id: owned(pb::string(blob, 5)),
            root_conversation_id: owned(pb::string(blob, 6)),
            project_id: owned(pb::string(blob, 18)),
            nesting_depth: pb::uint(blob, 17),
            agent_name: pb::message(blob, 4).and_then(|script| owned(pb::string(script, 1))),
        }
    }

    fn summary(&self) -> Vec<(&'static str, Value)> {
        let mut out = Vec::new();
        if let Some(created) = self.created_at {
            out.push(("created_at", json!(created.to_rfc3339())));
        }
        if !self.workspace_folders.is_empty() {
            out.push(("workspace_folders", json!(self.workspace_folders)));
        }
        if !self.workspace_uris.is_empty() {
            out.push(("workspace_uris", json!(self.workspace_uris)));
        }
        for (key, value) in [
            ("parent_conversation_id", &self.parent_conversation_id),
            ("root_conversation_id", &self.root_conversation_id),
            ("project_id", &self.project_id),
            ("agent_name", &self.agent_name),
        ] {
            if let Some(value) = value {
                out.push((key, json!(value)));
            }
        }
        if let Some(depth) = self.nesting_depth {
            out.push(("nesting_depth", json!(depth)));
        }
        out
    }
}

/// `CortexTrajectoryReference`.
struct TrajectoryReference {
    trajectory_id: Option<String>,
    step_index: Option<u64>,
    reference_type: Option<u64>,
    conversation_id: Option<String>,
}

impl TrajectoryReference {
    fn decode(blob: &[u8]) -> Self {
        let owned =
            |text: Option<&str>| text.filter(|text| !text.is_empty()).map(ToOwned::to_owned);
        Self {
            trajectory_id: owned(pb::string(blob, 1)),
            step_index: pb::uint(blob, 2),
            reference_type: pb::uint(blob, 5),
            conversation_id: owned(pb::string(blob, 6)),
        }
    }

    fn summary(&self) -> Value {
        json!({
            "trajectory_id": self.trajectory_id,
            "step_index": self.step_index,
            "reference_type": self.reference_type,
            "conversation_id": self.conversation_id,
        })
    }
}

// -- SQLite rows ------------------------------------------------------------

/// One cell, owned, with SQLite's own storage class.
enum Cell {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    /// A TEXT value that is not valid UTF-8, kept as bytes.
    TextBytes(Vec<u8>),
    Blob(Vec<u8>),
}

impl Cell {
    fn from_ref(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Int(value),
            ValueRef::Real(value) => Self::Real(value),
            ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => Self::Text(text.to_owned()),
                Err(_) => Self::TextBytes(bytes.to_vec()),
            },
            ValueRef::Blob(bytes) => Self::Blob(bytes.to_vec()),
        }
    }

    /// Lossless JSON: blobs (and non-UTF-8 text) are base64 under a key that
    /// names the encoding, so a blob can never read back as text.
    fn to_json(&self) -> Value {
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        match self {
            Self::Null => Value::Null,
            Self::Int(value) => json!(value),
            Self::Real(value) => json!(value),
            Self::Text(value) => json!(value),
            Self::TextBytes(bytes) => json!({ "text_base64": b64(bytes) }),
            Self::Blob(bytes) => json!({ "base64": b64(bytes) }),
        }
    }
}

struct Row {
    names: std::sync::Arc<[String]>,
    cells: Vec<Cell>,
}

impl Row {
    fn get(&self, name: &str) -> Option<&Cell> {
        self.names
            .iter()
            .position(|column| column == name)
            .and_then(|index| self.cells.get(index))
    }

    fn int(&self, name: &str) -> Option<i64> {
        match self.get(name)? {
            Cell::Int(value) => Some(*value),
            _ => None,
        }
    }

    fn text(&self, name: &str) -> Option<&str> {
        match self.get(name)? {
            Cell::Text(value) => Some(value),
            _ => None,
        }
    }

    fn blob(&self, name: &str) -> Option<&[u8]> {
        match self.get(name)? {
            Cell::Blob(value) => Some(value),
            _ => None,
        }
    }

    /// The whole row, every column kept (nulls included), tagged with its table.
    fn raw(&self, table: &str) -> Value {
        let row: Map<String, Value> = self
            .names
            .iter()
            .zip(&self.cells)
            .map(|(name, cell)| (name.clone(), cell.to_json()))
            .collect();
        json!({ "table": table, "row": row })
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn has_table(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
}

/// User tables in schema order (SQLite's own `sqlite_*` tables excluded).
fn table_names(conn: &Connection, db: &Path) -> Result<Vec<String>, AdapterError> {
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY rowid",
        )
        .map_err(|error| db_error(db, "prepare table list", &error))?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| db_error(db, "query table list", &error))?;
    names
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| db_error(db, "read table name", &error))
}

/// Every row of `table`, ordered by its first column (the primary key in every
/// agy table).
fn read_table(conn: &Connection, db: &Path, table: &str) -> Result<Vec<Row>, AdapterError> {
    let sql = format!("SELECT * FROM {} ORDER BY 1", quote_ident(table));
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|error| db_error(db, &format!("prepare {table}"), &error))?;
    let names: std::sync::Arc<[String]> = stmt
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    let width = names.len();
    let rows = stmt
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get_ref(index).map(Cell::from_ref))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(|error| db_error(db, &format!("query {table}"), &error))?;
    rows.map(|cells| {
        cells.map(|cells| Row {
            names: names.clone(),
            cells,
        })
    })
    .collect::<rusqlite::Result<Vec<_>>>()
    .map_err(|error| db_error(db, &format!("read {table} row"), &error))
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

/// A tolerant protobuf wire-format reader: agy's schema is closed-source, so
/// the adapter reads fields by number (numbers recovered from the descriptors
/// in the agy binary, docs/adapters/agy.md) and never needs the whole schema.
/// Accessors return `None` for an absent or undecodable field; the whole
/// payload is validated once per step and always survives in the raw record.
mod pb {
    use chrono::{DateTime, Utc};

    #[derive(Debug, Clone, Copy)]
    pub(super) struct Malformed;

    #[derive(Debug, Clone, Copy)]
    enum Wire<'a> {
        Varint(u64),
        Fixed64,
        Bytes(&'a [u8]),
        Fixed32,
    }

    struct Fields<'a> {
        buf: &'a [u8],
        pos: usize,
        failed: bool,
    }

    impl<'a> Iterator for Fields<'a> {
        type Item = Result<(u64, Wire<'a>), Malformed>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.failed || self.pos >= self.buf.len() {
                return None;
            }
            let field = self.field();
            self.failed = field.is_err();
            Some(field)
        }
    }

    impl<'a> Fields<'a> {
        fn varint(&mut self) -> Result<u64, Malformed> {
            let mut value = 0u64;
            for shift in (0..64).step_by(7) {
                let byte = *self.buf.get(self.pos).ok_or(Malformed)?;
                self.pos += 1;
                value |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    return Ok(value);
                }
            }
            Err(Malformed)
        }

        fn take(&mut self, len: usize) -> Result<&'a [u8], Malformed> {
            let end = self.pos.checked_add(len).ok_or(Malformed)?;
            let slice = self.buf.get(self.pos..end).ok_or(Malformed)?;
            self.pos = end;
            Ok(slice)
        }

        fn field(&mut self) -> Result<(u64, Wire<'a>), Malformed> {
            let key = self.varint()?;
            let number = key >> 3;
            if number == 0 {
                return Err(Malformed);
            }
            let wire = match key & 7 {
                0 => Wire::Varint(self.varint()?),
                1 => {
                    self.take(8)?;
                    Wire::Fixed64
                }
                2 => {
                    let len = usize::try_from(self.varint()?).map_err(|_| Malformed)?;
                    Wire::Bytes(self.take(len)?)
                }
                5 => {
                    self.take(4)?;
                    Wire::Fixed32
                }
                // Groups (3, 4) are proto2-only; agy's messages never use them.
                _ => return Err(Malformed),
            };
            Ok((number, wire))
        }
    }

    fn fields(buf: &[u8]) -> Fields<'_> {
        Fields {
            buf,
            pos: 0,
            failed: false,
        }
    }

    /// The message's top level parses end to end.
    pub(super) fn validate(buf: &[u8]) -> Result<(), Malformed> {
        fields(buf).try_for_each(|field| field.map(drop))
    }

    /// Every length-delimited value of field `number`, in wire order.
    pub(super) fn messages(buf: &[u8], number: u64) -> Vec<&[u8]> {
        fields(buf)
            .map_while(Result::ok)
            .filter_map(|(field, wire)| match wire {
                Wire::Bytes(bytes) if field == number => Some(bytes),
                _ => None,
            })
            .collect()
    }

    /// The last length-delimited value of field `number` (protobuf's
    /// last-one-wins for a singular field). Scans without collecting: this is
    /// the hot accessor, called a dozen times per step.
    pub(super) fn message(buf: &[u8], number: u64) -> Option<&[u8]> {
        fields(buf)
            .map_while(Result::ok)
            .filter_map(|(field, wire)| match wire {
                Wire::Bytes(bytes) if field == number => Some(bytes),
                _ => None,
            })
            .last()
    }

    pub(super) fn string(buf: &[u8], number: u64) -> Option<&str> {
        message(buf, number).and_then(|bytes| std::str::from_utf8(bytes).ok())
    }

    pub(super) fn strings(buf: &[u8], number: u64) -> Vec<&str> {
        messages(buf, number)
            .into_iter()
            .filter_map(|bytes| std::str::from_utf8(bytes).ok())
            .collect()
    }

    /// The last varint value of field `number`.
    pub(super) fn uint(buf: &[u8], number: u64) -> Option<u64> {
        fields(buf)
            .map_while(Result::ok)
            .filter_map(|(field, wire)| match wire {
                Wire::Varint(value) if field == number => Some(value),
                _ => None,
            })
            .last()
    }

    /// A `google.protobuf.Timestamp` (absent `seconds` / `nanos` are proto3
    /// zeros, the encoding of the value, not a default pond picks). A message
    /// that does not parse is `None` rather than the epoch: a bad decode must
    /// not read as a real 1970 timestamp.
    pub(super) fn timestamp(buf: &[u8]) -> Option<DateTime<Utc>> {
        validate(buf).ok()?;
        let seconds = uint(buf, 1).unwrap_or(0) as i64;
        let nanos = u32::try_from(uint(buf, 2).unwrap_or(0)).ok()?;
        DateTime::from_timestamp(seconds, nanos)
    }

    #[cfg(test)]
    pub(super) mod encode {
        //! Test-only writer for hand-built payloads.

        pub(crate) fn varint(mut value: u64, out: &mut Vec<u8>) {
            loop {
                let byte = (value & 0x7f) as u8;
                value >>= 7;
                if value == 0 {
                    out.push(byte);
                    return;
                }
                out.push(byte | 0x80);
            }
        }

        pub(crate) fn uint(number: u64, value: u64, out: &mut Vec<u8>) {
            varint(number << 3, out);
            varint(value, out);
        }

        pub(crate) fn bytes(number: u64, value: &[u8], out: &mut Vec<u8>) {
            varint((number << 3) | 2, out);
            varint(value.len() as u64, out);
            out.extend_from_slice(value);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::collections::HashMap;

    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::adapter::NoopOracle;

    const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/adapter/agy");

    const CLI_NO_WORKSPACE: &str = "109948f1-ce5f-4ece-bbe0-241cdb686b57";
    const CLI_TOOLS: &str = "3f72cf51-666b-4a7c-a08f-1ade2eaafa0f";
    const CLI_REASONING: &str = "3570c3c6-bd86-486c-b954-751b4e51885f";
    const CLI_INTERRUPTED: &str = "121e0cfc-1014-4f42-8666-c73df1a38457";
    const CLI_INTERRUPTED_RESUMED: &str = "5d458282-26eb-46a2-b098-bc7cc3987310";
    const CLI_SUBAGENT_PARENT: &str = "6479151c-9891-477a-b60d-df51a4e1dd07";
    const CLI_SUBAGENT_CHILD: &str = "11b9a01c-2233-415d-b4be-a0204e8ab2ff";
    const CLI_REWOUND: &str = "07439cf3-11de-46cd-ae53-730229b38bac";
    const CLI_FORK: &str = "3abd71a7-c181-48a7-a473-16f564089f7a";
    const ACP_EMPTY: &str = "7337857a-125f-4db6-ab8e-9d70c9f1115f";
    const ACP_TOOL: &str = "884ff681-7470-411c-a581-b5faa1f0fb57";
    const ACP_RELOADED: &str = "85732767-bcd9-46ac-8e31-08d986363857";
    const PROJECT: &str = "/tmp/agy-fixture/project";

    /// One ingested conversation: the session and its messages with parts.
    #[derive(Default)]
    struct Ingested {
        session: Option<Session>,
        messages: Vec<(Message, Vec<Part>)>,
    }

    struct Run {
        sessions: HashMap<String, Ingested>,
        skipped: Vec<(Option<String>, SkipReason)>,
        errors: Vec<String>,
    }

    async fn run(root: &Path) -> Run {
        let adapter = AgyAdapter::new(root);
        let mut stream = adapter.events_with(&NoopOracle);
        let mut run = Run {
            sessions: HashMap::new(),
            skipped: Vec::new(),
            errors: Vec::new(),
        };
        let mut current = String::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(AdapterYield::Event(IngestEvent::Session(session))) => {
                    current = session.id.clone();
                    run.sessions.entry(current.clone()).or_default().session = Some(session);
                }
                Ok(AdapterYield::Event(IngestEvent::Message(message))) => {
                    assert_eq!(
                        message.session_id(),
                        current,
                        "messages follow their session"
                    );
                    run.sessions
                        .get_mut(&current)
                        .expect("session first")
                        .messages
                        .push((message, Vec::new()));
                }
                Ok(AdapterYield::Event(IngestEvent::Part(part))) => {
                    let (message, parts) = run
                        .sessions
                        .get_mut(&current)
                        .and_then(|ingested| ingested.messages.last_mut())
                        .expect("a part follows its message");
                    assert_eq!(part.message_id, message.id(), "parts follow their message");
                    parts.push(part);
                }
                Ok(AdapterYield::Skipped {
                    session_id, reason, ..
                }) => {
                    run.skipped.push((session_id, reason));
                }
                Ok(AdapterYield::SkippedBatch { .. }) => {}
                Err(error) => run.errors.push(error.to_string()),
            }
        }
        run
    }

    async fn fixture() -> Run {
        let run = run(Path::new(FIXTURE_ROOT)).await;
        assert!(
            run.errors.is_empty(),
            "fixture ingest errors: {:?}",
            run.errors
        );
        run
    }

    fn session<'a>(run: &'a Run, id: &str) -> &'a Ingested {
        run.sessions
            .get(id)
            .unwrap_or_else(|| panic!("session {id} ingested"))
    }

    fn header(ingested: &Ingested) -> &Session {
        ingested.session.as_ref().expect("session row")
    }

    fn parts_of(ingested: &Ingested) -> impl Iterator<Item = &Part> {
        ingested.messages.iter().flat_map(|(_, parts)| parts)
    }

    fn step_messages(ingested: &Ingested) -> Vec<&Message> {
        ingested
            .messages
            .iter()
            .map(|(message, _)| message)
            .filter(|message| message.options()[NAME].get("step_index").is_some())
            .collect()
    }

    fn copy_fixture() -> TempDir {
        let temp = TempDir::new().unwrap();
        for entry in walkdir::WalkDir::new(FIXTURE_ROOT) {
            let entry = entry.unwrap();
            let relative = entry.path().strip_prefix(FIXTURE_ROOT).unwrap();
            let target = temp.path().join(relative);
            if entry.file_type().is_dir() {
                std::fs::create_dir_all(&target).unwrap();
            } else if !relative.to_string_lossy().ends_with("-wal")
                && !relative.to_string_lossy().ends_with("-shm")
            {
                std::fs::copy(entry.path(), &target).unwrap();
            }
        }
        temp
    }

    /// A `steps` row built by hand, for shapes the captured fixture does not
    /// hold.
    fn step_row(idx: i64, step_type: i64, status: i64, payload: Vec<u8>) -> Row {
        let names: std::sync::Arc<[String]> = ["idx", "step_type", "status", "step_payload"]
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        Row {
            names,
            cells: vec![
                Cell::Int(idx),
                Cell::Int(step_type),
                Cell::Int(status),
                Cell::Blob(payload),
            ],
        }
    }

    /// The ACP lane's harness runs shell commands and file searches as
    /// top-level kind steps, not wrapped in a `GENERIC` result. Their output
    /// is the tool result; without this the call would pair with nothing.
    #[test]
    fn a_kind_step_carries_its_own_tool_result() {
        use pb::encode;
        let call = {
            let mut out = Vec::new();
            encode::bytes(1, b"call_2388674", &mut out);
            encode::bytes(2, b"run_command", &mut out);
            out
        };
        let meta = {
            let mut out = Vec::new();
            encode::bytes(4, &call, &mut out);
            out
        };
        let run_command = {
            // `combined_output` is a `RunCommandOutput`, not a string: the
            // text sits one level in, which is what the path walk is for.
            let mut output = Vec::new();
            encode::bytes(1, b"total 4\n-rw-r--r-- 1 u u 0 calc.py\n", &mut output);
            let mut out = Vec::new();
            encode::uint(6, 0, &mut out);
            encode::bytes(21, &output, &mut out);
            encode::bytes(23, b"ls -la", &mut out);
            out
        };
        let mut payload = Vec::new();
        encode::uint(1, 21, &mut payload);
        encode::uint(4, 3, &mut payload);
        encode::bytes(5, &meta, &mut payload);
        encode::bytes(28, &run_command, &mut payload);

        let row = step_row(2, step_type::RUN_COMMAND, 3, payload);
        let step = StepView::decode(&row).expect("a well-formed kind step");
        let ids = PartIds {
            session_id: "s",
            message_id: "m",
        };
        let part = tool_result_part(&ids, &step).expect("the command's own output is the result");
        let PartKind::ToolResult {
            call_id,
            name,
            is_failure,
            result,
        } = part.kind
        else {
            panic!("a tool result");
        };
        assert_eq!(call_id.as_deref().map(String::as_str), Some("call_2388674"));
        assert_eq!(name.as_deref().map(String::as_str), Some("run_command"));
        assert!(!is_failure, "a completed command is not a failure");
        assert!(result.as_str().unwrap().contains("calc.py"));

        // The same shape for a search step, whose output field is its own.
        let find = {
            let mut out = Vec::new();
            encode::bytes(1, b"*.py", &mut out);
            encode::bytes(11, b"calc.py", &mut out);
            out
        };
        let mut payload = Vec::new();
        encode::uint(1, 25, &mut payload);
        encode::uint(4, 3, &mut payload);
        encode::bytes(5, &meta, &mut payload);
        encode::bytes(34, &find, &mut payload);
        let row = step_row(3, step_type::FIND, 3, payload);
        let step = StepView::decode(&row).expect("a well-formed find step");
        let part = tool_result_part(&ids, &step).expect("the search's raw output is the result");
        let PartKind::ToolResult { result, .. } = part.kind else {
            panic!("a tool result");
        };
        assert_eq!(result.as_str(), Some("calc.py"));
    }

    #[test]
    fn probe_default_claims_a_gemini_home_only_with_an_agy_lane() {
        for lane in Lane::ALL {
            let temp = TempDir::new().unwrap();
            let env = Env::with_home(temp.path());
            let gemini = temp.path().join(".gemini");
            std::fs::create_dir_all(gemini.join("tmp")).unwrap();
            assert!(
                AgyFactory.probe_default(&env).is_none(),
                "a Gemini CLI home without agy is not an agy install",
            );
            let conversations = gemini.join(lane.dir()).join(CONVERSATIONS_DIR);
            std::fs::create_dir_all(&conversations).unwrap();
            let probe = AgyFactory.probe_default(&env).expect("lane present");
            assert_eq!(probe["path"].as_str(), gemini.to_str());
            std::fs::remove_dir_all(&conversations).unwrap();
            assert!(AgyFactory.probe_default(&env).is_none());
        }
    }

    #[test]
    fn restore_is_refused_on_both_surfaces() {
        assert!(AgyFactory.restore_unsupported().is_some());
    }

    #[test]
    fn pb_reader_decodes_nested_fields_and_rejects_truncation() {
        use pb::encode;
        let mut timestamp = Vec::new();
        encode::uint(1, 1_789_051_165, &mut timestamp);
        encode::uint(2, 323_373_711, &mut timestamp);
        let mut meta = Vec::new();
        encode::bytes(1, &timestamp, &mut meta);
        encode::uint(3, 4, &mut meta);
        encode::bytes(12, b"exec", &mut meta);

        let decoded = StepMeta::decode(&meta);
        assert_eq!(
            decoded.created_at.map(|ts| ts.timestamp_micros()),
            Some(1_789_051_165_323_373),
        );
        assert_eq!(decoded.source, Some(4));
        assert_eq!(decoded.execution_id, Some("exec"));
        assert!(pb::validate(&meta).is_ok());
        assert!(
            pb::validate(&meta[..meta.len() - 1]).is_err(),
            "a cut field is malformed"
        );
        assert!(
            pb::validate(&[0x0b]).is_err(),
            "group wire types are not agy's"
        );
    }

    #[test]
    fn file_uris_decode_once() {
        assert_eq!(
            file_uri_path("file:///tmp/agy-fixture/project").as_deref(),
            Some(PROJECT)
        );
        assert_eq!(
            file_uri_path("file:///home/u/my%20dir").as_deref(),
            Some("/home/u/my dir")
        );
        assert_eq!(
            file_uri_path("file:///C:/work/p").as_deref(),
            Some("C:/work/p")
        );
        assert_eq!(file_uri_path("https://example.com/x"), None);
        assert_eq!(file_uri_path("file:///bad%zz"), None);
    }

    #[tokio::test]
    async fn every_conversation_with_steps_becomes_a_session() {
        let run = fixture().await;
        assert_eq!(run.sessions.len(), 12, "13 databases, one of them empty");
        assert_eq!(
            run.skipped,
            vec![(Some(ACP_EMPTY.to_owned()), SkipReason::Empty)],
            "the never-prompted ACP conversation is empty, not an error",
        );
        let steps: usize = run.sessions.values().map(|s| step_messages(s).len()).sum();
        assert_eq!(
            steps, 62,
            "one message per captured step (the fixture census)"
        );
    }

    #[tokio::test]
    async fn project_resolution_follows_the_recorded_chain() {
        let run = fixture().await;
        let project = |id: &str| {
            let session = header(session(&run, id));
            (
                session.project.as_ref().clone(),
                session.options[NAME]["project_source"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            )
        };
        assert_eq!(
            project(CLI_TOOLS),
            (PROJECT.to_owned(), "workspace".to_owned()),
            "a run with a workspace records its folder",
        );
        assert_eq!(
            project(CLI_NO_WORKSPACE),
            ("default-cli-project".to_owned(), "project_id".to_owned()),
            "headless without --add-dir records only agy's project id",
        );
        assert_eq!(
            project(ACP_TOOL),
            (PROJECT.to_owned(), "meta_cwd".to_owned()),
            "the ACP lane keeps its cwd in the .meta sidecar",
        );
        assert_eq!(header(session(&run, ACP_TOOL)).options[NAME]["lane"], "acp");
        assert_eq!(
            header(session(&run, CLI_TOOLS)).options[NAME]["lane"],
            "cli"
        );
    }

    /// A workspace pond cannot turn into a path is not a project: the chain
    /// moves on rather than storing the URI undecoded.
    #[test]
    fn a_remote_workspace_uri_falls_through_the_chain() {
        let remote = TrajectoryMeta {
            workspace_folders: vec!["vscode-remote://ssh-remote%2Bbox/home/u/p".to_owned()],
            project_id: Some("default-cli-project".to_owned()),
            ..TrajectoryMeta::default()
        };
        let (project, source) = session_project(&remote, None).expect("the project id remains");
        assert_eq!(project.as_ref(), "default-cli-project");
        assert_eq!(source, "project_id");

        let local = TrajectoryMeta {
            workspace_folders: vec!["file:///tmp/agy-fixture/project".to_owned()],
            ..TrajectoryMeta::default()
        };
        let (project, source) = session_project(&local, None).expect("a file URI is a path");
        assert_eq!(project.as_ref(), PROJECT);
        assert_eq!(source, "workspace");
    }

    /// The ACP server rewrites the sidecar on every save. A crash mid-write
    /// leaves half a JSON object; the text still reaches the raw record, and
    /// the failure that remains names the missing project rather than the
    /// parse.
    #[tokio::test]
    async fn a_half_written_meta_sidecar_is_not_a_parse_error() {
        let temp = copy_fixture();
        let sidecar = temp
            .path()
            .join("antigravity-acp/conversations")
            .join(format!("{ACP_TOOL}.meta"));
        std::fs::write(&sidecar, br#"{"cwd": "/tmp/agy-fixture/pro"#).unwrap();

        let run = run(temp.path()).await;
        assert_eq!(run.errors.len(), 1, "{:?}", run.errors);
        assert!(
            run.errors[0].contains("no workspace, no cwd sidecar and no project id"),
            "{}",
            run.errors[0],
        );
        assert!(
            !run.sessions.contains_key(ACP_TOOL),
            "an ACP conversation whose only cwd is unreadable has no project to record",
        );
        assert!(
            run.sessions.contains_key(ACP_RELOADED) && run.sessions.contains_key(CLI_TOOLS),
            "its siblings ingest",
        );
    }

    #[tokio::test]
    async fn lineage_maps_subagents_and_forks() {
        let run = fixture().await;
        let child = header(session(&run, CLI_SUBAGENT_CHILD));
        assert_eq!(child.source_agent, SUBAGENT_AGENT);
        assert_eq!(
            child.parent_session_id.as_deref(),
            Some(CLI_SUBAGENT_PARENT)
        );
        assert_eq!(child.parent_message_id, None, "a spawn has no cut point");

        let fork = header(session(&run, CLI_FORK));
        assert_eq!(fork.source_agent, NAME, "a fork is a user-visible peer");
        assert_eq!(fork.parent_session_id.as_deref(), Some(CLI_REWOUND));
        let cut = fork.parent_message_id.as_deref().expect("fork cut point");
        assert!(
            step_messages(session(&run, CLI_REWOUND))
                .iter()
                .any(|message| message.id() == cut),
            "the cut point {cut} names a message of the parent",
        );

        let rewound = header(session(&run, CLI_REWOUND));
        assert_eq!(
            rewound.parent_session_id, None,
            "a rewind references the conversation's own trajectory, not a parent",
        );
        for id in [CLI_TOOLS, ACP_RELOADED] {
            assert_eq!(header(session(&run, id)).parent_session_id, None);
        }
    }

    #[tokio::test]
    async fn tool_calls_pair_with_their_results_by_call_id() {
        let run = fixture().await;
        let tools = session(&run, CLI_TOOLS);
        let calls: Vec<&str> = parts_of(tools)
            .filter_map(|part| match &part.kind {
                PartKind::ToolCall { call_id, .. } => call_id.as_deref().map(String::as_str),
                _ => None,
            })
            .collect();
        let results: Vec<(&str, bool, &Value)> = parts_of(tools)
            .filter_map(|part| match &part.kind {
                PartKind::ToolResult {
                    call_id,
                    is_failure,
                    result,
                    ..
                } => Some((call_id.as_deref()?.as_str(), *is_failure, result)),
                _ => None,
            })
            .collect();
        assert_eq!(calls.len(), 4);
        assert_eq!(results.len(), 4, "every call settled with a result");
        for (call_id, _, _) in &results {
            assert!(
                calls.contains(call_id),
                "result {call_id} pairs with a declared call"
            );
        }
        let failures: Vec<_> = results.iter().filter(|(_, failed, _)| *failed).collect();
        assert_eq!(failures.len(), 1, "only the missing-file view failed");
        assert!(
            failures[0]
                .2
                .as_str()
                .unwrap()
                .contains("no such file or directory"),
            "an errored step's result is its error text",
        );
        assert!(
            results.iter().any(|(_, failed, result)| !failed
                && result
                    .as_str()
                    .is_some_and(|text| text.contains("exited with code 1"))),
            "a non-zero exit is a completed call, not a failure",
        );
        for part in parts_of(tools) {
            let expected = match part.kind {
                PartKind::ToolResult { .. } => Provenance::Injected,
                _ => Provenance::Conversational,
            };
            assert_eq!(part.provenance, expected);
        }

        let acp = session(&run, ACP_TOOL);
        let result = parts_of(acp)
            .find_map(|part| match &part.kind {
                PartKind::ToolResult { name, result, .. } => Some((name.clone(), result.clone())),
                _ => None,
            })
            .expect("the ACP client tool settles with a result");
        assert_eq!(
            result.0.as_deref().map(String::as_str),
            Some("client_view_file")
        );
        assert!(result.1["result"].as_str().unwrap().starts_with("# calc"));
    }

    #[tokio::test]
    async fn thinking_is_a_reasoning_part() {
        let run = fixture().await;
        let reasoning = parts_of(session(&run, CLI_REASONING))
            .find_map(|part| match &part.kind {
                PartKind::Reasoning { text } => text.clone(),
                _ => None,
            })
            .expect("thinking text");
        assert!(reasoning.contains("391"));
    }

    #[tokio::test]
    async fn an_in_flight_step_is_a_status_keyed_carrier() {
        let run = fixture().await;
        let interrupted = session(&run, CLI_INTERRUPTED);
        let running: Vec<&Message> = step_messages(interrupted)
            .into_iter()
            .filter(|message| message.id().ends_with(":running"))
            .collect();
        assert_eq!(running.len(), 1);
        assert!(
            matches!(running[0], Message::System { content: None, .. }),
            "an unsettled step is carried whole, never mapped half-done",
        );

        // Settled to CANCELED by a resume: under the plain step id, and with
        // whatever the writer left on the step - here its own "still running"
        // note, which is recorded output, not a failure and not synthesized.
        let resumed = session(&run, CLI_INTERRUPTED_RESUMED);
        let (canceled, parts) = resumed
            .messages
            .iter()
            .find(|(message, _)| message.options()[NAME]["status"] == 6)
            .expect("the canceled step");
        assert!(!canceled.id().ends_with(":running"));
        assert!(matches!(canceled, Message::Tool { .. }));
        let result = parts
            .iter()
            .find_map(|part| match &part.kind {
                PartKind::ToolResult {
                    is_failure, result, ..
                } => Some((*is_failure, result.as_str()?.to_owned())),
                _ => None,
            })
            .expect("the canceled step's recorded result");
        assert!(!result.0, "canceled is not an error status");
        assert!(result.1.contains("Step is still running"), "{}", result.1);
    }

    #[tokio::test]
    async fn a_settle_in_place_lands_as_a_new_message_not_a_collision() {
        let temp = copy_fixture();
        let db = temp
            .path()
            .join("antigravity-cli/conversations")
            .join(format!("{CLI_INTERRUPTED}.db"));
        let before = run(temp.path()).await;
        let running_id = step_messages(session(&before, CLI_INTERRUPTED))
            .into_iter()
            .find(|message| message.id().ends_with(":running"))
            .unwrap()
            .id()
            .to_owned();

        // What agy's next resume does: rewrite the RUNNING row to CANCELED.
        let conn = Connection::open(&db).unwrap();
        conn.execute("UPDATE steps SET status = 6 WHERE status = 2", [])
            .unwrap();
        drop(conn);

        let after = run(temp.path()).await;
        let ids: Vec<String> = step_messages(session(&after, CLI_INTERRUPTED))
            .iter()
            .map(|message| message.id().to_owned())
            .collect();
        let settled_id = running_id.trim_end_matches(":running");
        assert!(
            ids.iter().any(|id| id == settled_id),
            "the settled row has the plain id"
        );
        assert!(
            !ids.contains(&running_id),
            "the source no longer holds the in-flight row"
        );
    }

    #[tokio::test]
    async fn every_row_is_recoverable_from_its_raw_record() {
        let run = fixture().await;
        let b64 = |value: &Value| {
            base64::engine::general_purpose::STANDARD
                .decode(value["base64"].as_str().unwrap())
                .unwrap()
        };
        for id in [CLI_TOOLS, CLI_FORK, ACP_TOOL] {
            let path = if id == ACP_TOOL {
                format!("{FIXTURE_ROOT}/antigravity-acp/conversations/{id}.db")
            } else {
                format!("{FIXTURE_ROOT}/antigravity-cli/conversations/{id}.db")
            };
            let conn = open_db(Path::new(&path)).unwrap();
            let source: HashMap<i64, Vec<u8>> = conn
                .prepare("SELECT idx, step_payload FROM steps")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let ingested = session(&run, id);
            for message in step_messages(ingested) {
                let raw = &message.options()["source"]["raw_record"];
                assert_eq!(raw["table"], "steps");
                let idx = raw["row"]["idx"].as_i64().unwrap();
                assert_eq!(
                    b64(&raw["row"]["step_payload"]),
                    source[&idx],
                    "{id} step {idx}"
                );
            }
            let tables: Vec<String> = ingested
                .messages
                .iter()
                .filter_map(|(message, _)| {
                    message.options()[NAME]["table"]
                        .as_str()
                        .map(ToOwned::to_owned)
                })
                .collect();
            assert!(tables.iter().any(|table| table == "trajectory_meta"));
            assert!(
                !tables
                    .iter()
                    .any(|table| NON_CAPTURE_TABLES.contains(&table.as_str())),
                "generator telemetry is declared non-capture",
            );
            let session_raw = &header(ingested).options["source"]["raw_record"];
            assert!(session_raw["trajectory_metadata_blob"].is_array());
        }
    }

    #[tokio::test]
    async fn the_watermark_equals_the_newest_message_so_resync_skips() {
        let run = fixture().await;
        for lane in Lane::ALL {
            let dir = Path::new(FIXTURE_ROOT)
                .join(lane.dir())
                .join(CONVERSATIONS_DIR);
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("db") {
                    continue;
                }
                let id = path.file_stem().unwrap().to_str().unwrap();
                let mark = peek_watermark(&path);
                if id == ACP_EMPTY {
                    assert_eq!(mark, SourceWatermark::Empty);
                    continue;
                }
                let newest = session(&run, id)
                    .messages
                    .iter()
                    .map(|(message, _)| message.timestamp().timestamp_micros())
                    .max()
                    .unwrap();
                let SourceWatermark::At(micros) = mark else {
                    panic!("{id}: expected a watermark, got {mark:?}");
                };
                assert!(
                    micros <= newest,
                    "{id}: watermark {micros} beyond stored {newest}"
                );
            }
        }
    }

    #[tokio::test]
    async fn unreadable_shapes_surface_instead_of_vanishing() {
        let temp = TempDir::new().unwrap();
        let conversations = temp.path().join("antigravity-cli").join(CONVERSATIONS_DIR);
        std::fs::create_dir_all(&conversations).unwrap();
        // A pre-SQLite conversation, a pre-created empty file, and a schema
        // from the future.
        std::fs::write(
            conversations.join("aaaaaaaa-0000-4000-8000-000000000001.pb"),
            b"\x00opaque",
        )
        .unwrap();
        std::fs::write(
            conversations.join("aaaaaaaa-0000-4000-8000-000000000002.db"),
            b"",
        )
        .unwrap();
        let future = conversations.join("aaaaaaaa-0000-4000-8000-000000000003.db");
        let conn = Connection::open(&future).unwrap();
        conn.execute_batch(
            "CREATE TABLE steps (idx integer primary key); PRAGMA user_version = 2;",
        )
        .unwrap();
        drop(conn);

        let run = run(temp.path()).await;
        assert!(run.sessions.is_empty());
        assert!(run.errors.is_empty(), "{:?}", run.errors);
        let reasons: Vec<&SkipReason> = run.skipped.iter().map(|(_, reason)| reason).collect();
        assert_eq!(
            reasons
                .iter()
                .filter(|reason| matches!(reason, SkipReason::Unsupported(_)))
                .count(),
            2,
            "the encrypted .pb and the future schema are counted failures: {reasons:?}",
        );
        assert!(reasons.contains(&&SkipReason::Empty));
    }

    #[tokio::test]
    async fn a_malformed_step_is_a_typed_error_and_its_siblings_still_ingest() {
        let temp = copy_fixture();
        let db = temp
            .path()
            .join("antigravity-cli/conversations")
            .join(format!("{CLI_NO_WORKSPACE}.db"));
        let conn = Connection::open(&db).unwrap();
        conn.execute("UPDATE steps SET step_payload = x'0b' WHERE idx = 1", [])
            .unwrap();
        drop(conn);

        let run = run(temp.path()).await;
        assert_eq!(run.errors.len(), 1, "{:?}", run.errors);
        assert!(run.errors[0].contains("steps/1"), "{}", run.errors[0]);
        assert_eq!(step_messages(session(&run, CLI_NO_WORKSPACE)).len(), 1);
    }
}
