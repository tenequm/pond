//! opencode adapter (github.com/sst/opencode).
//!
//! opencode moved its storage from a JSON file tree to a Drizzle-managed SQLite
//! database in v1.2.0 (2026-02-14). This adapter reads BOTH, because a user who
//! upgraded past the format's death still has stranded pre-migration JSON that
//! never reached the DB (spec.md#session-movement-complete):
//!
//! - `<data-dir>/opencode*.db` (WAL) - `session`/`message`/`part` tables. The
//!   `message`/`part` `data` blob is the old per-file JSON minus its ids;
//!   opencode rehydrates it as `{...data, id, sessionID(, messageID)}`.
//! - `<data-dir>/storage/` - the legacy content-addressed split tree:
//!   `session/<projectID>/<sessionID>.json`, `message/<sessionID>/<messageID>.json`,
//!   `part/<messageID>/<partID>.json`.
//!
//! Sessions are deduped by id (DB wins, the tree fills gaps,
//! spec.md#adapter-integrity-dedup). Both feed the same
//! `build_message_events`/`map_part` pipeline, sorted by id (ids are lexically
//! time-sortable, so id order is creation order), emitting `Session -> Message
//! -> Parts` per session.
//!
//! opencode fuses a tool call and its result into one `tool` part on the
//! assistant message. Canonical keeps the two apart (a `tool_result` on an
//! assistant message is a category error, spec.md#model-part-provenance), so the
//! adapter splits it: a `ToolCall` Part stays on the assistant message and a
//! synthetic `Tool` message carries the `ToolResult`. Native restore replays
//! each real part's stored `raw_record` at its original path and skips the
//! synthetic records, so the split is value-complete-lossless.

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::{
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, compact_json, config_path,
    extract::{bound_value, extract_str},
    jsonl::RECORD_CAP,
    part_id, part_ordinal, raw_record, source_options, validate_path_id,
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
        // The configured root is the opencode DATA DIR now (it holds both the
        // `opencode*.db` files and the legacy `storage/` tree). Only offer it
        // when it actually has one of those, so an empty `~/.local/share/opencode`
        // does not masquerade as a source.
        let data_dir = env.home.join(".local").join("share").join("opencode");
        if !data_dir.exists() {
            return None;
        }
        let has_db = db_paths(&data_dir).is_ok_and(|dbs| !dbs.is_empty());
        let has_tree = data_dir.join("storage").is_dir();
        (has_db || has_tree).then(|| json!({ "path": data_dir }))
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

/// Configured opencode reader, rooted at the opencode DATA DIR (which holds the
/// `opencode*.db` files and the legacy `storage/` tree).
#[derive(Debug, Clone)]
pub struct OpencodeAdapter {
    root: PathBuf,
}

impl OpencodeAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        // Legacy configs pointed `path` at `<data-dir>/storage`; the root is the
        // data dir now, so a configured basename of `storage` resolves to its
        // parent and existing configs keep working.
        let root = if root.file_name().and_then(|name| name.to_str()) == Some("storage") {
            root.parent()
                .map_or_else(|| root.clone(), Path::to_path_buf)
        } else {
            root
        };
        Self { root }
    }

    /// The legacy split-file tree lives at `<data-dir>/storage/`; a root that IS
    /// itself a bare tree (`session/` directly under it, e.g. a foreign-restore
    /// target) has no `storage/` subdir, so fall back to the root.
    fn tree_base(&self) -> PathBuf {
        tree_base(&self.root)
    }
}

fn tree_base(root: &Path) -> PathBuf {
    let nested = root.join("storage");
    if nested.join("session").is_dir() {
        nested
    } else {
        root.to_path_buf()
    }
}

impl Adapter for OpencodeAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let root = self.root.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let sources = enumerate_sources(&root)?;
                // Count the deduped tree copies too: the progress bar shrinks its
                // length by the `SkippedBatch` count (an Empty bulk skip), so the
                // discover total must include what that skip will subtract.
                Ok(sources.db.len() + sources.tree.len() + sources.duplicates)
            })
            .await
            .map_err(join_error)?
        })
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> super::PlanFuture<'a> {
        let root = self.root.clone();
        Box::pin(async move {
            // The events_with freshness pre-pass run standalone: the same
            // per-session peek sync's gate pays every run, classified instead of
            // read. On an empty oracle the peeks are skipped - a first sync reads
            // everything. A message-less session stays Opaque (never Empty):
            // reading it still ingests its Session row.
            let peek = !oracle.is_empty();
            let heads = tokio::task::spawn_blocking(move || peek_heads(&root, peek))
                .await
                .map_err(join_error)??;
            if !peek {
                return Ok(Some(super::SyncPlan::all_pending(heads.len())));
            }
            Ok(Some(super::SyncPlan::from_heads(
                oracle,
                heads
                    .iter()
                    .map(|(session_id, watermark)| (Some(session_id.as_str()), *watermark)),
            )))
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let adapter = self.clone();
        Box::pin(stream! {
            let root = adapter.root.clone();
            let peek = !oracle.is_empty();
            // One blocking burst enumerates both sources, dedups, and (when the
            // oracle carries watermarks) peeks each session's newest-message
            // timestamp. It returns owned, Send data so the async gate below can
            // consult the borrowed oracle without dragging a rusqlite handle
            // across an await point.
            let peeked = tokio::task::spawn_blocking(move || enumerate_and_peek(&root, peek)).await;
            let Peeked { entries, duplicates } = match peeked {
                Ok(Ok(peeked)) => peeked,
                Ok(Err(error)) => { yield Err(error); return; }
                Err(join) => { yield Err(join_error(join)); return; }
            };

            // Legacy-tree copies of DB sessions never reach the read pass; surface
            // the count so the dedup skip is visible, not silent
            // (spec.md#adapter-integrity-dedup).
            if duplicates > 0 {
                yield Ok(AdapterYield::SkippedBatch {
                    reason: SkipReason::Empty,
                    count: duplicates,
                });
            }

            let mut survivors = Vec::with_capacity(entries.len());
            for entry in entries {
                let session_id = entry.source.session_id().to_owned();
                if crate::adapter::is_session_fresh(oracle, &session_id, entry.source_ts) {
                    yield Ok(AdapterYield::Skipped {
                        session_id: Some(session_id),
                        project: None,
                        reason: SkipReason::Fresh,
                    });
                    continue;
                }
                survivors.push(entry.source);
            }

            let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
            let tree_base = adapter.tree_base();
            let handle =
                tokio::task::spawn_blocking(move || read_survivors(&tree_base, survivors, &tx));
            while let Some(item) = rx.recv().await {
                yield item;
            }
            if let Err(join) = handle.await {
                yield Err(join_error(join));
            }
        })
    }
}

/// A discovered session tagged by source, plus its freshness watermark peek in
/// micros (`None` = not peeked or unreadable -> re-read to be safe).
struct HeadEntry {
    source: SessionSource,
    source_ts: Option<i64>,
}

/// Where a session's records come from: a SQLite DB or the legacy split tree.
enum SessionSource {
    Db(Box<DbSessionHead>),
    Tree(SessionFile),
}

impl SessionSource {
    fn session_id(&self) -> &str {
        match self {
            SessionSource::Db(head) => &head.session.id,
            SessionSource::Tree(file) => &file.session_id,
        }
    }
}

/// A session read from a DB: the canonical `Session` (built from the row
/// columns) plus the DB path needed to re-open for the body read.
struct DbSessionHead {
    db_path: PathBuf,
    session: Session,
}

struct Sources {
    db: Vec<DbSessionHead>,
    tree: Vec<SessionFile>,
    /// Tree sessions whose id is already covered by a DB (DB wins).
    duplicates: usize,
}

struct Peeked {
    entries: Vec<HeadEntry>,
    duplicates: usize,
}

/// Enumerate both sources and dedup by session id: every DB session, plus the
/// legacy-tree sessions whose id no DB already carries (`adapter-integrity-dedup`).
fn enumerate_sources(root: &Path) -> Result<Sources, AdapterError> {
    let db = collect_db_heads(root)?;
    let seen: HashSet<&str> = db.iter().map(|head| head.session.id.as_str()).collect();
    let tree_base = tree_base(root);
    let mut tree = Vec::new();
    let mut duplicates = 0;
    for file in collect_session_files(&tree_base)? {
        if seen.contains(file.session_id.as_str()) {
            duplicates += 1;
        } else {
            tree.push(file);
        }
    }
    Ok(Sources {
        db,
        tree,
        duplicates,
    })
}

/// Enumerate and, when `peek`, compute each session's freshness watermark. DB
/// watermarks reuse one connection per DB; tree walks cache their listings on
/// the `SessionFile` so the read pass does not re-list.
fn enumerate_and_peek(root: &Path, peek: bool) -> Result<Peeked, AdapterError> {
    let Sources {
        db,
        tree,
        duplicates,
    } = enumerate_sources(root)?;
    let tree_base = tree_base(root);
    let mut conns: HashMap<PathBuf, Connection> = HashMap::new();
    let mut entries = Vec::with_capacity(db.len() + tree.len());
    for head in db {
        let source_ts = if peek {
            let conn = connection(&mut conns, &head.db_path)?;
            db_session_watermark(conn, &head.db_path, &head.session.id)?
        } else {
            None
        };
        entries.push(HeadEntry {
            source: SessionSource::Db(Box::new(head)),
            source_ts,
        });
    }
    for mut file in tree {
        let source_ts = if peek {
            let walk = walk_session_subtree(&tree_base, &file.session_id)?;
            let ts = newest_message_ts(&walk);
            file.cached_subtree = Some(walk);
            ts
        } else {
            None
        };
        entries.push(HeadEntry {
            source: SessionSource::Tree(file),
            source_ts,
        });
    }
    Ok(Peeked {
        entries,
        duplicates,
    })
}

/// The `plan()` heads: every session id with its `SourceWatermark`. When `!peek`
/// every watermark is `Opaque` (plan short-circuits to all-pending before use).
fn peek_heads(
    root: &Path,
    peek: bool,
) -> Result<Vec<(String, super::SourceWatermark)>, AdapterError> {
    let Peeked { entries, .. } = enumerate_and_peek(root, peek)?;
    Ok(entries
        .into_iter()
        .map(|entry| {
            let watermark = match entry.source_ts {
                Some(ts) => super::SourceWatermark::At(ts),
                None => super::SourceWatermark::Opaque,
            };
            (entry.source.session_id().to_owned(), watermark)
        })
        .collect())
}

/// Read every survivor session's body, streaming events through `tx`. Opens each
/// DB once (cached by path); tree sessions route through the legacy read path.
fn read_survivors(
    tree_base: &Path,
    survivors: Vec<SessionSource>,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) {
    let mut conns: HashMap<PathBuf, Connection> = HashMap::new();
    for source in survivors {
        let keep = match source {
            SessionSource::Db(head) => match connection(&mut conns, &head.db_path) {
                Ok(conn) => read_db_session(conn, *head, tx),
                Err(error) => tx.blocking_send(Err(error)).is_ok(),
            },
            SessionSource::Tree(file) => read_one_session(tree_base, file, tx),
        };
        if !keep {
            return;
        }
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

/// One session file located on disk. `cached_subtree` is populated only when
/// the freshness pre-walk happened (i.e. the oracle had a watermark for this
/// session); the read pass reuses the listings instead of re-walking.
struct SessionFile {
    session_id: String,
    path: PathBuf,
    cached_subtree: Option<SubtreeWalk>,
}

/// Result of one subtree walk: the message and part directory listings (so the
/// read pass doesn't redo them). The last `message_files` entry is the session's
/// latest message id for the freshness check.
struct SubtreeWalk {
    message_files: Vec<PathBuf>,
    /// One entry per message file, in the same order; each is the sorted list
    /// of part files for that message. Empty vec = message has no parts.
    part_files_by_message: Vec<Vec<PathBuf>>,
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
            validate_path_id(
                NAME,
                "session file name",
                &session_id,
                path.display().to_string(),
            )?;
            out.push(SessionFile {
                session_id,
                path,
                cached_subtree: None,
            });
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Walk one session's full subtree: the message files under `message/<sid>/` and
/// every part file under `part/<mid>/`, returning the listings so the read pass
/// can reuse them.
fn walk_session_subtree(tree_base: &Path, session_id: &str) -> Result<SubtreeWalk, AdapterError> {
    let message_dir = tree_base.join("message").join(session_id);
    let message_files = list_json_sorted(&message_dir)?;
    let mut part_files_by_message = Vec::with_capacity(message_files.len());
    for message_path in &message_files {
        let Some(message_id) = message_path.file_stem().and_then(|stem| stem.to_str()) else {
            part_files_by_message.push(Vec::new());
            continue;
        };
        validate_path_id(
            NAME,
            "message file name",
            message_id,
            message_path.display().to_string(),
        )?;
        let part_dir = tree_base.join("part").join(message_id);
        let parts = list_json_sorted(&part_dir)?;
        part_files_by_message.push(parts);
    }
    Ok(SubtreeWalk {
        message_files,
        part_files_by_message,
    })
}

/// Watermark for the freshness gate: the session's max stored message timestamp.
/// That is the last message's `time.created` or any of its tool parts'
/// `state.time.end` (a tool result completing after the message was created);
/// earlier messages' events all precede the last message, so its subtree
/// suffices. `None` on an empty session or unreadable last message -> safe
/// re-read. Reads only the last message and its parts (a handful of small files).
fn newest_message_ts(walk: &SubtreeWalk) -> Option<i64> {
    let message_path = walk.message_files.last()?;
    let message = read_json(message_path).ok()?;
    let mut newest = millis_at(&message, &["time", "created"])?;
    if let Some(parts) = walk.part_files_by_message.last() {
        for part_path in parts {
            if let Ok(part) = read_json(part_path)
                && let Some(end) = millis_at(&part, &["state", "time", "end"])
            {
                newest = newest.max(end);
            }
        }
    }
    Some(newest.timestamp_micros())
}

/// Returns `false` when the consumer dropped the receiver and the read should stop.
fn read_one_session(
    tree_base: &Path,
    file: SessionFile,
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
    if let Err(error) = validate_path_id(
        NAME,
        "session id",
        &session_id,
        file.path.display().to_string(),
    ) {
        emit!(Err(error));
        return true;
    }
    let session_created_at = session.created_at;
    emit!(Ok(AdapterYield::Event(IngestEvent::Session(session))));

    // Reuse the freshness pre-walk's listings when present; otherwise list now.
    let (message_files, mut part_files_by_message) = match file.cached_subtree {
        Some(walk) => (walk.message_files, walk.part_files_by_message),
        None => {
            let message_dir = tree_base.join("message").join(&session_id);
            let files = match list_json_sorted(&message_dir) {
                Ok(files) => files,
                Err(error) => {
                    emit!(Err(error));
                    return true;
                }
            };
            (files, Vec::new())
        }
    };
    let use_cache = !part_files_by_message.is_empty();

    for (index, message_path) in message_files.iter().enumerate() {
        let message_value = match read_json(message_path) {
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
        if let Err(error) = validate_path_id(
            NAME,
            "message id",
            message_id,
            message_path.display().to_string(),
        ) {
            emit!(Err(error));
            continue;
        }
        let part_files = if use_cache {
            std::mem::take(&mut part_files_by_message[index])
        } else {
            let part_dir = tree_base.join("part").join(message_id);
            match list_json_sorted(&part_dir) {
                Ok(files) => files,
                Err(error) => {
                    emit!(Err(error));
                    continue;
                }
            }
        };
        let mut parts = Vec::with_capacity(part_files.len());
        for part_path in part_files {
            match read_json(&part_path) {
                Ok(value) => parts.push(value),
                Err(error) => emit!(Err(error)),
            }
        }
        match build_message_events(&session_id, &message_value, &parts, session_created_at) {
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

/// All columns of the `session` table, in the order [`session_row_from_row`]
/// reads them.
const SESSION_COLUMNS: &str = "id, project_id, workspace_id, parent_id, slug, directory, path, \
     title, version, share_url, summary_additions, summary_deletions, summary_files, \
     summary_diffs, metadata, cost, tokens_input, tokens_output, tokens_reasoning, \
     tokens_cache_read, tokens_cache_write, revert, permission, agent, model, time_created, \
     time_updated, time_compacting, time_archived";

/// Every `opencode*.db` directly under `root` (WAL `-wal`/`-shm` sidecars are
/// excluded by the `.db` extension filter). A missing root is "no DB", not an
/// error - the adapter may be pointed at a bare legacy tree.
fn db_paths(root: &Path) -> Result<Vec<PathBuf>, AdapterError> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(AdapterError::io(NAME, root.display().to_string(), error)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|error| AdapterError::io(NAME, root.display().to_string(), error))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("db") {
            continue;
        }
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("opencode"))
        {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

/// Open a DB read-only with a bounded busy timeout - opencode writes it live
/// (WAL), so short read bursts must not stall on a checkpoint.
fn open_db(path: &Path) -> Result<Connection, AdapterError> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|error| db_error(path, "open", &error))?;
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(|error| db_error(path, "set busy_timeout", &error))?;
    Ok(conn)
}

fn db_error(path: &Path, op: &str, error: &rusqlite::Error) -> AdapterError {
    AdapterError::schema(
        NAME,
        path.display().to_string(),
        format!("sqlite {op} failed: {error}"),
    )
}

/// Get-or-open a cached connection for `path`.
fn connection<'a>(
    conns: &'a mut HashMap<PathBuf, Connection>,
    path: &Path,
) -> Result<&'a Connection, AdapterError> {
    match conns.entry(path.to_path_buf()) {
        Entry::Occupied(slot) => Ok(slot.into_mut()),
        Entry::Vacant(slot) => Ok(slot.insert(open_db(path)?)),
    }
}

/// Session heads across every DB under `root`, each already mapped to a canonical
/// [`Session`]. Ordered by (DB path, then session id) for deterministic ingest.
fn collect_db_heads(root: &Path) -> Result<Vec<DbSessionHead>, AdapterError> {
    let mut heads = Vec::new();
    for db_path in db_paths(root)? {
        let conn = open_db(&db_path)?;
        for row in fetch_session_rows(&conn, &db_path)? {
            heads.push(db_session_head(&db_path, row)?);
        }
    }
    Ok(heads)
}

fn fetch_session_rows(conn: &Connection, db_path: &Path) -> Result<Vec<SessionRow>, AdapterError> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {SESSION_COLUMNS} FROM session ORDER BY id"
        ))
        .map_err(|error| db_error(db_path, "prepare session", &error))?;
    let rows = stmt
        .query_map([], session_row_from_row)
        .map_err(|error| db_error(db_path, "query session", &error))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|error| db_error(db_path, "read session row", &error))?);
    }
    Ok(out)
}

/// Owned projection of one `session` row. JSON-mode columns (model, metadata,
/// permission, revert, summary_diffs) stay as their raw text and are parsed
/// lazily in [`reconstruct_session_info`].
struct SessionRow {
    id: String,
    project_id: String,
    workspace_id: Option<String>,
    parent_id: Option<String>,
    slug: String,
    directory: String,
    path: Option<String>,
    title: String,
    version: String,
    share_url: Option<String>,
    summary_additions: Option<i64>,
    summary_deletions: Option<i64>,
    summary_files: Option<i64>,
    summary_diffs: Option<String>,
    metadata: Option<String>,
    cost: f64,
    tokens_input: i64,
    tokens_output: i64,
    tokens_reasoning: i64,
    tokens_cache_read: i64,
    tokens_cache_write: i64,
    revert: Option<String>,
    permission: Option<String>,
    agent: Option<String>,
    model: Option<String>,
    time_created: i64,
    time_updated: i64,
    time_compacting: Option<i64>,
    time_archived: Option<i64>,
}

fn session_row_from_row(row: &rusqlite::Row) -> rusqlite::Result<SessionRow> {
    Ok(SessionRow {
        id: row.get(0)?,
        project_id: row.get(1)?,
        workspace_id: row.get(2)?,
        parent_id: row.get(3)?,
        slug: row.get(4)?,
        directory: row.get(5)?,
        path: row.get(6)?,
        title: row.get(7)?,
        version: row.get(8)?,
        share_url: row.get(9)?,
        summary_additions: row.get(10)?,
        summary_deletions: row.get(11)?,
        summary_files: row.get(12)?,
        summary_diffs: row.get(13)?,
        metadata: row.get(14)?,
        cost: row.get(15)?,
        tokens_input: row.get(16)?,
        tokens_output: row.get(17)?,
        tokens_reasoning: row.get(18)?,
        tokens_cache_read: row.get(19)?,
        tokens_cache_write: row.get(20)?,
        revert: row.get(21)?,
        permission: row.get(22)?,
        agent: row.get(23)?,
        model: row.get(24)?,
        time_created: row.get(25)?,
        time_updated: row.get(26)?,
        time_compacting: row.get(27)?,
        time_archived: row.get(28)?,
    })
}

/// Map one `session` row to a canonical [`Session`]. The `raw_record` is the
/// `fromRow`-shaped `SessionInfo` reconstruction (the object `opencode import`
/// accepts); reusing [`session_from_value`] threads `directory` through the
/// sealed `Extracted` path and derives `created_at` from the row `time_created`.
fn db_session_head(db_path: &Path, row: SessionRow) -> Result<DbSessionHead, AdapterError> {
    let info = reconstruct_session_info(&row);
    let mut session = session_from_value(&info, db_path)?;
    // spec.md#model-no-synthesis: a subagent session (`parent_id` set) is labeled
    // `opencode/<agent>` so children leave default search, matching claude-code;
    // a null agent column degrades to the bare `opencode/subagent` label.
    if row.parent_id.is_some() {
        session.source_agent = match row.agent.as_deref() {
            Some(agent) => format!("{NAME}/{agent}"),
            None => format!("{NAME}/subagent"),
        };
    }
    Ok(DbSessionHead {
        db_path: db_path.to_path_buf(),
        session,
    })
}

/// Rebuild the `fromRow` `SessionInfo` JSON from a row: camelCase keys, nested
/// `time`/`tokens`/`summary`, null columns omitted (spec.md#model-lossless-projection -
/// every non-null column is recoverable).
fn reconstruct_session_info(row: &SessionRow) -> Value {
    let mut info = serde_json::Map::new();
    info.insert("id".to_owned(), json!(row.id));
    info.insert("slug".to_owned(), json!(row.slug));
    info.insert("projectID".to_owned(), json!(row.project_id));
    if let Some(value) = &row.workspace_id {
        info.insert("workspaceID".to_owned(), json!(value));
    }
    info.insert("directory".to_owned(), json!(row.directory));
    if let Some(value) = &row.path {
        info.insert("path".to_owned(), json!(value));
    }
    if let Some(value) = &row.parent_id {
        info.insert("parentID".to_owned(), json!(value));
    }
    info.insert("title".to_owned(), json!(row.title));
    if let Some(value) = &row.agent {
        info.insert("agent".to_owned(), json!(value));
    }
    if let Some(value) = &row.model {
        info.insert("model".to_owned(), parse_json_column(value));
    }
    info.insert("version".to_owned(), json!(row.version));
    if row.summary_additions.is_some()
        || row.summary_deletions.is_some()
        || row.summary_files.is_some()
    {
        let mut summary = serde_json::Map::new();
        summary.insert(
            "additions".to_owned(),
            json!(row.summary_additions.unwrap_or(0)),
        );
        summary.insert(
            "deletions".to_owned(),
            json!(row.summary_deletions.unwrap_or(0)),
        );
        summary.insert("files".to_owned(), json!(row.summary_files.unwrap_or(0)));
        if let Some(value) = &row.summary_diffs {
            summary.insert("diffs".to_owned(), parse_json_column(value));
        }
        info.insert("summary".to_owned(), Value::Object(summary));
    }
    info.insert("cost".to_owned(), json!(row.cost));
    info.insert(
        "tokens".to_owned(),
        json!({
            "input": row.tokens_input,
            "output": row.tokens_output,
            "reasoning": row.tokens_reasoning,
            "cache": { "read": row.tokens_cache_read, "write": row.tokens_cache_write },
        }),
    );
    if let Some(value) = &row.share_url {
        info.insert("share".to_owned(), json!({ "url": value }));
    }
    if let Some(value) = &row.metadata {
        info.insert("metadata".to_owned(), parse_json_column(value));
    }
    if let Some(value) = &row.revert {
        info.insert("revert".to_owned(), parse_json_column(value));
    }
    if let Some(value) = &row.permission {
        info.insert("permission".to_owned(), parse_json_column(value));
    }
    let mut time = serde_json::Map::new();
    time.insert("created".to_owned(), json!(row.time_created));
    time.insert("updated".to_owned(), json!(row.time_updated));
    if let Some(value) = row.time_compacting {
        time.insert("compacting".to_owned(), json!(value));
    }
    if let Some(value) = row.time_archived {
        time.insert("archived".to_owned(), json!(value));
    }
    info.insert("time".to_owned(), Value::Object(time));
    Value::Object(info)
}

/// A Drizzle `mode: "json"` column stores JSON text; recover the value, falling
/// back to the raw string if it is somehow not valid JSON (lossless either way).
fn parse_json_column(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
}

/// Freshness watermark for a DB session (micros): the newest message by id order,
/// its `data.time.created` raised by any of its tool parts' `state.time.end`.
/// NEVER the `time_created` COLUMN - migration-stamped rows carry a future column
/// value that would re-read forever (spec.md, decision 9). `None` (empty or
/// unreadable newest message) -> safe re-read.
fn db_session_watermark(
    conn: &Connection,
    db_path: &Path,
    session_id: &str,
) -> Result<Option<i64>, AdapterError> {
    let latest = conn
        .query_row(
            "SELECT id, data FROM message WHERE session_id = ?1 ORDER BY id DESC LIMIT 1",
            [session_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|error| db_error(db_path, "query newest message", &error))?;
    let Some((message_id, data)) = latest else {
        return Ok(None);
    };
    let Ok(message) = serde_json::from_str::<Value>(&data) else {
        return Ok(None);
    };
    let Some(created) = millis_at(&message, &["time", "created"]) else {
        return Ok(None);
    };
    let mut newest = created;
    let mut stmt = conn
        .prepare("SELECT data FROM part WHERE message_id = ?1 ORDER BY id")
        .map_err(|error| db_error(db_path, "prepare newest parts", &error))?;
    let rows = stmt
        .query_map([&message_id], |row| row.get::<_, String>(0))
        .map_err(|error| db_error(db_path, "query newest parts", &error))?;
    for row in rows {
        let data = row.map_err(|error| db_error(db_path, "read newest part row", &error))?;
        if let Ok(part) = serde_json::from_str::<Value>(&data)
            && let Some(end) = millis_at(&part, &["state", "time", "end"])
        {
            newest = newest.max(end);
        }
    }
    Ok(Some(newest.timestamp_micros()))
}

/// Owned projection of one `message`/`part` row.
struct RecordRow {
    id: String,
    time_created: i64,
    data: String,
}

fn fetch_records(
    conn: &Connection,
    db_path: &Path,
    sql: &str,
    param: &str,
    kind: &str,
) -> Result<Vec<RecordRow>, AdapterError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|error| db_error(db_path, &format!("prepare {kind}"), &error))?;
    let rows = stmt
        .query_map([param], |row| {
            Ok(RecordRow {
                id: row.get(0)?,
                time_created: row.get(1)?,
                data: row.get(2)?,
            })
        })
        .map_err(|error| db_error(db_path, &format!("query {kind}"), &error))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|error| db_error(db_path, &format!("read {kind} row"), &error))?);
    }
    Ok(out)
}

/// Read one DB session's body in short bursts (messages, then per-message parts -
/// each query its own autocommit read, never one long transaction), feeding the
/// same `build_message_events` pipeline as the tree path. Returns `false` when
/// the consumer dropped the receiver.
fn read_db_session(
    conn: &Connection,
    head: DbSessionHead,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    macro_rules! emit {
        ($item:expr) => {
            if tx.blocking_send($item).is_err() {
                return false;
            }
        };
    }

    let db_path = head.db_path.clone();
    let session_id = head.session.id.clone();
    let session_anchor = head.session.created_at;
    emit!(Ok(AdapterYield::Event(IngestEvent::Session(head.session))));

    let messages = match fetch_records(
        conn,
        &db_path,
        "SELECT id, time_created, data FROM message WHERE session_id = ?1 ORDER BY id",
        &session_id,
        "message",
    ) {
        Ok(rows) => rows,
        Err(error) => {
            emit!(Err(error));
            return true;
        }
    };

    for message in messages {
        let message_id = message.id;
        // Message timestamp chain (spec, engineering defaults): the row
        // `time_created` is the fallback, the session anchor the last resort;
        // `build_message_events` prefers the `data.time.created` in the value.
        let default_ts =
            DateTime::from_timestamp_millis(message.time_created).unwrap_or(session_anchor);
        let message_value = match reconstruct_record(
            &message.data,
            &record_location(&db_path, &session_id, &message_id),
            &[("id", json!(message_id)), ("sessionID", json!(session_id))],
        ) {
            Ok(value) => value,
            Err(error) => {
                emit!(Err(error));
                continue;
            }
        };

        let part_rows = match fetch_records(
            conn,
            &db_path,
            "SELECT id, time_created, data FROM part WHERE message_id = ?1 ORDER BY id",
            &message_id,
            "part",
        ) {
            Ok(rows) => rows,
            Err(error) => {
                emit!(Err(error));
                continue;
            }
        };
        let mut parts = Vec::with_capacity(part_rows.len());
        for part in part_rows {
            match reconstruct_record(
                &part.data,
                &record_location(&db_path, &session_id, &part.id),
                &[
                    ("id", json!(part.id)),
                    ("sessionID", json!(session_id)),
                    ("messageID", json!(message_id)),
                ],
            ) {
                Ok(value) => parts.push(value),
                Err(error) => emit!(Err(error)),
            }
        }
        match build_message_events(&session_id, &message_value, &parts, default_ts) {
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

/// Reconstruct a `message`/`part` value as `{...data, <injected>}`, enforcing the
/// record cap on the raw `data` text and bounding string leaves at the seam cap
/// before it leaves this module (spec.md#adapter-bounded-values). A malformed
/// `data` JSON drops only that record, like a malformed part file in the tree.
fn reconstruct_record(
    data: &str,
    location: &str,
    inject: &[(&str, Value)],
) -> Result<Value, AdapterError> {
    if data.len() > RECORD_CAP {
        return Err(AdapterError::schema(
            NAME,
            location.to_owned(),
            format!(
                "record data exceeds adapter record cap: {} bytes > {RECORD_CAP}",
                data.len()
            ),
        ));
    }
    let mut value: Value = serde_json::from_str(data)
        .map_err(|error| AdapterError::parse(NAME, location.to_owned(), 1, error))?;
    if let Value::Object(map) = &mut value {
        for (key, injected) in inject {
            map.insert((*key).to_owned(), injected.clone());
        }
    }
    bound_value(&mut value);
    Ok(value)
}

fn record_location(db_path: &Path, session_id: &str, record_id: &str) -> String {
    format!(
        "{}::session={session_id}::record={record_id}",
        db_path.display()
    )
}

/// Read one JSON file, bounding every string leaf at the seam cap
/// (spec.md#adapter-bounded-values) before it leaves this module. One open +
/// one metadata syscall on the handle - avoids the duplicate `std::fs::metadata`
/// + `std::fs::read` pair on the ~1k-file real corpus.
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

    let options = opencode_raw(value);

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
    default_timestamp: DateTime<Utc>,
) -> Result<Vec<IngestEvent>, AdapterError> {
    let message_id = message_value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::schema(NAME, session_id.to_owned(), "message missing `id`"))?;
    let role = message_value.get("role").and_then(Value::as_str);
    let timestamp = millis_at(message_value, &["time", "created"]).unwrap_or(default_timestamp);

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
        _ => Message::System {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            content: extract_str(message_value, "role"),
            options,
        },
    };

    let mut events = vec![IngestEvent::Message(message)];
    let mut deferred = Vec::new();
    for (ordinal, part_value) in part_values.iter().enumerate() {
        let mapped = map_part(session_id, message_id, ordinal, part_value, timestamp)?;
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
) -> Result<MappedPart, AdapterError> {
    let kind = value.get("type").and_then(Value::as_str);
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::schema(NAME, message_id.to_owned(), "part missing `id`"))?
        .to_owned();

    if kind == Some("tool") {
        return Ok(tool_part(
            session_id, message_id, &id, ordinal, value, message_ts,
        ));
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

    Ok(MappedPart {
        part: Part {
            session_id: session_id.to_owned(),
            id,
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance,
            options: opencode_raw(value),
            kind: part_kind,
        },
        tool_split: None,
    })
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
    // spec.md#model-no-synthesis: an absent mime hint is faithfully `None`,
    // not a synthesized `application/octet-stream` placeholder.
    let media_type = value
        .get("mime")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
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
    let result_ts = millis_at(value, &["state", "time", "end"]).unwrap_or(message_ts);

    // Take input/output by moving them out of a single owned `state` clone
    // rather than cloning each field separately - on the real corpus a fused
    // tool part fans into three records (call + tool message + result), each
    // of which used to clone its own slice of `state`.
    let mut owned_state = state.cloned().unwrap_or(Value::Null);
    let (input, result) = match owned_state.as_object_mut() {
        Some(map) => {
            let input = map.remove("input").unwrap_or(Value::Null);
            let result = map
                .remove("output")
                .or_else(|| map.remove("error"))
                .unwrap_or_else(|| {
                    // No output/error - the rest of `state` IS the payload.
                    std::mem::take(&mut owned_state)
                });
            (input, result)
        }
        None => (Value::Null, Value::Null),
    };

    let tool_call = Part {
        session_id: session_id.to_owned(),
        id: id.to_owned(),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
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

#[inline]
fn opencode_raw(value: &Value) -> ProviderOptions {
    source_options(NAME, value)
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
    //
    // spec.md#adapter-native-restore-lossless: when the session lacks a stored
    // `raw_record` (older ingest, foreign-sourced session), native is
    // impossible. We downgrade to foreign and stamp `actual_fidelity` so the
    // caller can surface the downgrade instead of getting a silent surprise.
    let Some(session_raw) = raw_record(&session.session.options) else {
        return serialize_foreign(session);
    };
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

    let mut files = vec![RestoredFile::new(
        PathBuf::from("session")
            .join(project_id)
            .join(format!("{}.json", session.session.id)),
        encode(&session_raw, &session.session.id)?,
        RestoreFidelity::Native,
    )];

    for message in &session.messages {
        if !is_synthetic(message.message.options())
            && let Some(raw) = raw_record(message.message.options())
        {
            files.push(RestoredFile::new(
                PathBuf::from("message")
                    .join(&session.session.id)
                    .join(format!("{}.json", message.message.id())),
                encode(&raw, message.message.id())?,
                RestoreFidelity::Native,
            ));
        }
        for part in &message.parts {
            // A part that carries a `raw_record` maps 1:1 to a source file at
            // `part/<message_id>/<part_id>.json`; synthetic split parts do not.
            if let Some(raw) = raw_record(&part.options) {
                files.push(RestoredFile::new(
                    PathBuf::from("part")
                        .join(&part.message_id)
                        .join(format!("{}.json", part.id)),
                    encode(&raw, &part.id)?,
                    RestoreFidelity::Native,
                ));
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
    let mut files = vec![RestoredFile::new(
        PathBuf::from("session")
            .join(&project_id)
            .join(format!("{}.json", session.session.id)),
        encode(&session_record, &session.session.id)?,
        RestoreFidelity::Foreign,
    )];

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
        files.push(RestoredFile::new(
            PathBuf::from("message")
                .join(&session.session.id)
                .join(format!("{}.json", message.message.id())),
            encode(&record, message.message.id())?,
            RestoreFidelity::Foreign,
        ));
        for part in &message.parts {
            let Some(record) = foreign_part(&session.session.id, part) else {
                continue;
            };
            files.push(RestoredFile::new(
                PathBuf::from("part")
                    .join(message.message.id())
                    .join(format!("{}.json", part.id)),
                encode(&record, &part.id)?,
                RestoreFidelity::Foreign,
            ));
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

#[cfg(test)]
mod tests {
    //! End-to-end test for the opencode adapter: ingest the committed
    //! split-file fixture corpus and assert pond's canonical shape comes out
    //! the other side, including the fused-tool-part split. The fixture lives
    //! under `tests/fixtures/adapter/opencode/storage/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{
        adapter::extract::LEAF_CAP, handlers::ingest_adapter, sessions::Store, wire::PartKind,
    };
    use tempfile::TempDir;

    // Manifest-dir anchored: unit tests must not depend on the process cwd
    // (figment::Jail chdirs the whole test process while config tests run).
    // `FIXTURES` is the legacy split-file tree (used to exercise the tree path in
    // isolation); `DATA_DIR` is the opencode data dir holding BOTH the DB and the
    // tree beside it.
    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/opencode/storage"
    );
    const DATA_DIR: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/opencode"
    );
    const DB_FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/opencode/opencode.db"
    );
    const FRESH_SESSION_ID: &str = "ses_6405e5a5cffeIG2QHRuTmm4mA7";
    const FRESH_MESSAGE_ID: &str = "msg_zzzzfresh0001";
    const FRESH_PART_ID: &str = "prt_zzzzfresh0001";

    // DB-fixture facts (Agent A's generated corpus).
    const DB_SESSION_COUNT: usize = 10;
    const CHILD_SESSION_ID: &str = "ses_09fbc7fc1ffeUaBz77QiNdJbXa";
    const CHILD_PARENT_ID: &str = "ses_09fbc87f2ffeyYCZq51oN2HfGa";
    const DOCTORED_SESSION_ID: &str = "ses_09fbe676bffe9nYsSBi5xhBlaD";
    const DOCTORED_MESSAGE_ID: &str = "msg_f6041991e001BOYvwdP1iI0H0A";
    // The doctored row's `data.time.created` (truth) vs its future `time_created`
    // COLUMN (the migration-stamp quirk).
    const DOCTORED_DATA_CREATED_MS: i64 = 1_784_026_339_614;
    const DOCTORED_COLUMN_CREATED_MS: i64 = 1_794_394_339_614;

    /// Copy the DB fixture into a fresh temp data dir (no `storage/` tree beside
    /// it, so ingest is DB-only). Returns the data dir path.
    fn db_data_dir(temp: &std::path::Path) -> anyhow::Result<PathBuf> {
        let dir = temp.join("data");
        std::fs::create_dir_all(&dir)?;
        std::fs::copy(DB_FIXTURE, dir.join("opencode.db"))?;
        Ok(dir)
    }

    struct FixedOracle {
        session_id: &'static str,
        watermark_micros: i64,
    }

    impl crate::adapter::SkipOracle for FixedOracle {
        fn session_max_ts(&self, session_id: &str) -> Option<i64> {
            (session_id == self.session_id).then_some(self.watermark_micros)
        }
    }

    /// probe_default returns the DATA DIR (not the `storage/` subdir) and only
    /// when it actually holds a DB or a tree.
    #[test]
    fn probe_default_finds_opencode_data_dir() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data_dir = temp.path().join(".local").join("share").join("opencode");
        std::fs::create_dir_all(data_dir.join("storage"))?;
        let env = Env::with_home(temp.path());

        let probe = OpencodeFactory.probe_default(&env);
        let got = probe
            .as_ref()
            .and_then(|value| value.get("path"))
            .and_then(Value::as_str);
        assert_eq!(got, data_dir.to_str(), "probe must return the data dir");

        // An empty data dir (no DB, no tree) is not a source.
        std::fs::remove_dir_all(data_dir.join("storage"))?;
        assert!(
            OpencodeFactory.probe_default(&env).is_none(),
            "an empty data dir must not be offered as a source",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_tree_corpus() -> anyhow::Result<()> {
        // Native restore round-trips the legacy tree exactly; copy the tree into a
        // db-free temp root so restore is exercised against tree-sourced sessions
        // (the DB-native conformance is a separate test).
        let temp = TempDir::new()?;
        let source = temp.path().join("storage");
        copy_dir(std::path::Path::new(FIXTURES), &source)?;
        let adapter = OpencodeAdapter::new(&source);
        crate::adapter::test_support::assert_native_restore(&OpencodeFactory, &adapter, &source)
            .await
    }

    /// `plan` is the events_with freshness pre-pass run standalone and MUST
    /// agree with it: the sessions plan calls fresh are exactly the sessions
    /// the gate skips. A message-less session stays pending (never Empty) -
    /// reading it still ingests its Session row, so the gate must keep
    /// re-reading it.
    #[tokio::test(flavor = "multi_thread")]
    async fn plan_matches_the_events_gate() -> anyhow::Result<()> {
        use tokio_stream::StreamExt;

        let temp = TempDir::new()?;
        let source = temp.path().join("storage");
        copy_dir(std::path::Path::new(FIXTURES), &source)?;
        let empty_dir = source.join("session").join("proj-empty");
        std::fs::create_dir_all(&empty_dir)?;
        std::fs::write(
            empty_dir.join("ses_zzzzemptysession00000000.json"),
            "{\"id\":\"ses_zzzzemptysession00000000\",\"projectID\":\"proj-empty\",\
             \"directory\":\"/tmp/pond-test\",\"time\":{\"created\":1759859990000}}",
        )?;

        let adapter = OpencodeAdapter::new(&source);
        let first_sync = adapter
            .plan(&crate::adapter::NoopOracle)
            .await?
            .expect("opencode supports plan");
        assert!(first_sync.sessions > 1);
        assert_eq!(first_sync.pending, first_sync.sessions);
        assert_eq!(first_sync.fresh, 0);

        struct MaxWatermarkOracle;
        impl crate::adapter::SkipOracle for MaxWatermarkOracle {
            fn session_max_ts(&self, _session_id: &str) -> Option<i64> {
                Some(i64::MAX)
            }
        }
        let plan = adapter
            .plan(&MaxWatermarkOracle)
            .await?
            .expect("opencode supports plan");
        assert_eq!(plan.sessions, first_sync.sessions);
        assert_eq!(
            plan.pending, 1,
            "the message-less session must stay pending - its Session row only \
             lands by reading it",
        );

        let mut gate_fresh = 0usize;
        let mut stream = adapter.events_with(&MaxWatermarkOracle);
        while let Some(item) = stream.next().await {
            match item? {
                AdapterYield::Skipped {
                    reason: SkipReason::Fresh,
                    ..
                } => gate_fresh += 1,
                AdapterYield::SkippedBatch {
                    reason: SkipReason::Fresh,
                    count,
                } => gate_fresh += count,
                _ => {}
            }
        }
        assert_eq!(gate_fresh, plan.fresh, "plan and gate must agree");
        Ok(())
    }

    /// `append_fresh_opencode_turn` writes its message at this `time.created`
    /// (millis); the freshness gate keys on it in micros.
    const FRESH_TURN_MICROS: i64 = 1_759_859_999_000 * 1_000;

    /// A session whose latest message is newer than the watermark is re-read, and
    /// the appended turn lands.
    #[tokio::test(flavor = "multi_thread")]
    async fn freshness_re_reads_a_session_that_gained_a_newer_message() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let source = temp.path().join("storage");
        copy_dir(std::path::Path::new(FIXTURES), &source)?;

        let store = Store::open_local(temp.path().join("store")).await?;
        let adapter = OpencodeAdapter::new(&source);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        append_fresh_opencode_turn(&source)?;
        // Watermark sits just below the appended message's timestamp.
        let oracle = FixedOracle {
            session_id: FRESH_SESSION_ID,
            watermark_micros: FRESH_TURN_MICROS - 1,
        };
        ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;

        let session = store
            .get_session(FRESH_SESSION_ID)
            .await?
            .expect("fixture session round-trips");
        let fresh = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == FRESH_MESSAGE_ID)
            .expect("message newer than the watermark must land");
        assert!(
            fresh.parts.iter().any(|part| matches!(
                &part.kind,
                PartKind::Text { text } if text.as_deref().map(|value| value.as_str()) == Some("fresh opencode text")
            )),
            "fresh message part must land with the re-read session",
        );
        Ok(())
    }

    /// A session whose latest message is no newer than the watermark is skipped as
    /// `Fresh` - the appended turn is NOT re-read.
    #[tokio::test(flavor = "multi_thread")]
    async fn freshness_skips_a_session_not_newer_than_the_watermark() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let source = temp.path().join("storage");
        copy_dir(std::path::Path::new(FIXTURES), &source)?;

        let store = Store::open_local(temp.path().join("store")).await?;
        let adapter = OpencodeAdapter::new(&source);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        append_fresh_opencode_turn(&source)?;
        // Watermark at/above the appended timestamp: the session is fresh.
        let oracle = FixedOracle {
            session_id: FRESH_SESSION_ID,
            watermark_micros: FRESH_TURN_MICROS,
        };
        let summary = ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;

        assert!(
            summary.skipped_fresh >= 1,
            "the unchanged-vs-watermark session must be skipped, got {summary:?}",
        );
        let session = store
            .get_session(FRESH_SESSION_ID)
            .await?
            .expect("fixture session round-trips");
        assert!(
            !session
                .messages
                .iter()
                .any(|stored| stored.message.id() == FRESH_MESSAGE_ID),
            "a skipped session must not re-read the appended turn",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_part_file_drops_only_that_part() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let source = temp.path().join("storage");
        write_minimal_session(&source, "ses_badpart", "msg_badpart")?;
        let part_dir = source.join("part").join("msg_badpart");
        std::fs::write(part_dir.join("prt_000_bad.json"), b"{not json")?;
        write_json_file(
            &part_dir.join("prt_999_good.json"),
            &json!({
                "id": "prt_999_good",
                "sessionID": "ses_badpart",
                "messageID": "msg_badpart",
                "type": "text",
                "text": "valid sibling survives",
                "synthetic": false,
            }),
        )?;

        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(
            &store,
            &OpencodeAdapter::new(&source),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;

        assert_eq!(summary.dropped_events, 1);
        let session = store
            .get_session("ses_badpart")
            .await?
            .expect("session with one malformed part still lands");
        let message = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == "msg_badpart")
            .expect("message with valid sibling part still lands");
        assert!(message.parts.iter().any(|part| {
            matches!(
                &part.kind,
                PartKind::Text { text }
                    if text.as_deref().map(String::as_str) == Some("valid sibling survives")
            )
        }));
        Ok(())
    }

    #[test]
    fn missing_message_timestamp_uses_session_anchor() -> anyhow::Result<()> {
        let session_anchor =
            DateTime::parse_from_rfc3339("2026-05-05T12:13:14Z")?.with_timezone(&Utc);
        let events = build_message_events(
            "ses_anchor",
            &json!({"id": "msg_no_time", "role": "user"}),
            &[],
            session_anchor,
        )?;

        let IngestEvent::Message(message) = &events[0] else {
            panic!("first event is the message");
        };
        assert_eq!(message.timestamp(), session_anchor);
        Ok(())
    }

    #[test]
    fn source_part_without_id_is_schema_error() {
        let session_anchor = DateTime::from_timestamp_millis(1_765_000_000_000).unwrap();
        let error = build_message_events(
            "ses_missing_part_id",
            &json!({
                "id": "msg_missing_part_id",
                "role": "assistant",
                "time": { "created": 1_765_000_000_000i64 },
            }),
            &[json!({"type": "text", "text": "cannot restore its filename"})],
            session_anchor,
        )
        .expect_err("part ids are required for native filename replay");

        assert!(error.to_string().contains("part missing `id`"));
    }

    #[test]
    fn read_json_bounds_oversized_string_leaves() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let path = temp.path().join("oversized.json");
        write_json_file(
            &path,
            &json!({
                "id": "oversized",
                "text": "x".repeat(LEAF_CAP + 100),
            }),
        )?;

        let value = read_json(&path)?;
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .expect("text leaf survives as a bounded marker");
        assert!(text.len() <= LEAF_CAP);
        assert!(text.ends_with(&format!("{} bytes>", LEAF_CAP + 100)));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn opencode_adapter_ingests_fixture_corpus_into_canonical_shape() -> anyhow::Result<()> {
        // DATA_DIR ingests BOTH sources (the DB plus the tree beside it).
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = OpencodeAdapter::new(DATA_DIR);

        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert!(summary.accepted() > 0, "ingest must accept rows");
        assert_eq!(summary.dropped_events, 0, "no per-event drops expected");
        assert_eq!(
            summary.dropped_sessions, 0,
            "no session-level rejections expected"
        );

        let (sessions, messages, parts) = store.row_counts().await?;
        assert!(
            sessions >= DB_SESSION_COUNT,
            "the DB sessions plus the tree beside them all ingest",
        );
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
            assert!(
                session.session.source_agent.starts_with(NAME),
                "source_agent is `opencode` or `opencode/<agent>`, got {}",
                session.session.source_agent,
            );
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
        let adapter = OpencodeAdapter::new(DATA_DIR);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        let mut call_ids = std::collections::HashSet::new();
        let mut result_ids = std::collections::HashSet::new();
        let mut saw_failure = false;
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
                        PartKind::ToolResult {
                            call_id,
                            is_failure,
                            result,
                            ..
                        } => {
                            assert!(
                                matches!(stored.message, Message::Tool { .. }),
                                "a ToolResult must live on a Tool-role message",
                            );
                            if *is_failure {
                                saw_failure = true;
                                assert_ne!(
                                    result,
                                    &Value::Null,
                                    "failed tool results must carry the source error/output payload",
                                );
                            }
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
        assert!(
            saw_failure,
            "fixture has at least one failed opencode tool result"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn db_fixture_ingests_expected_census() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(
            &store,
            &OpencodeAdapter::new(&data),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(summary.dropped_events, 0, "no per-event drops expected");
        assert_eq!(summary.dropped_sessions, 0, "no session-level rejections");

        let (sessions, messages, parts) = store.row_counts().await?;
        assert_eq!(sessions, DB_SESSION_COUNT, "every DB session ingests");
        assert!(
            messages >= 27,
            "27 message rows plus synthetic tool carriers"
        );
        assert!(parts >= 69, "69 part rows plus split-off tool results");

        // The archived + multi-project sessions all land (census sanity), and the
        // child session's canonical shape is asserted in its own test.
        assert!(
            store.get_session(CHILD_SESSION_ID).await?.is_some(),
            "child session ingests",
        );
        assert!(
            store.get_session(DOCTORED_SESSION_ID).await?.is_some(),
            "migration-stamped session ingests",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn child_session_labeled_as_subagent() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        let store = Store::open_local(temp.path().join("store")).await?;
        ingest_adapter(
            &store,
            &OpencodeAdapter::new(&data),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;

        let child = store
            .get_session(CHILD_SESSION_ID)
            .await?
            .expect("child session ingests");
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some(CHILD_PARENT_ID),
            "child carries its parent pointer",
        );
        assert_eq!(
            child.session.source_agent, "opencode/general",
            "spec.md: a subagent session is labeled opencode/<agent>",
        );
        Ok(())
    }

    /// The doctored row's canonical message timestamp is `data.time.created`, and
    /// the session's freshness watermark ignores the future `time_created` COLUMN.
    #[tokio::test(flavor = "multi_thread")]
    async fn doctored_row_uses_data_time_not_column() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        let store = Store::open_local(temp.path().join("store")).await?;
        ingest_adapter(
            &store,
            &OpencodeAdapter::new(&data),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;

        let session = store
            .get_session(DOCTORED_SESSION_ID)
            .await?
            .expect("doctored session ingests");
        let message = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == DOCTORED_MESSAGE_ID)
            .expect("doctored message ingests");
        assert_eq!(
            message.message.timestamp().timestamp_millis(),
            DOCTORED_DATA_CREATED_MS,
            "canonical timestamp is data.time.created, not the migration column",
        );

        // The freshness watermark also ignores the column: it is derived from the
        // data timestamps (all months before the doctored column value).
        let conn = open_db(std::path::Path::new(DB_FIXTURE))?;
        let watermark =
            db_session_watermark(&conn, std::path::Path::new(DB_FIXTURE), DOCTORED_SESSION_ID)?
                .expect("session has a message");
        assert!(
            watermark < DOCTORED_COLUMN_CREATED_MS * 1_000,
            "watermark ({watermark} micros) must ignore the future time_created column",
        );
        Ok(())
    }

    /// Part types absent from the generated corpus (subtask/compaction/agent/
    /// snapshot) land as injected carriers whose `raw_record` round-trips.
    #[tokio::test(flavor = "multi_thread")]
    async fn injected_carrier_part_types_land() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        {
            let conn = Connection::open(data.join("opencode.db"))?;
            // The standalone carrier session references no `project` row; the
            // adapter reads the DB, it does not enforce opencode's FKs, so the
            // fixture writer disables them for this hand-authored session.
            conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
            insert_session(
                &conn,
                "ses_carrier00000000000000000",
                "/tmp/carrier",
                None,
                None,
                1_784_100_000_000,
            )?;
            insert_message(
                &conn,
                "msg_carrier00000000000000000",
                "ses_carrier00000000000000000",
                1_784_100_000_100,
                &json!({ "role": "assistant", "time": { "created": 1_784_100_000_100i64 } }),
            )?;
            for (part_id, body) in [
                (
                    "prt_carrier_a_subtask000000",
                    json!({ "type": "subtask", "prompt": "p", "description": "d", "agent": "general" }),
                ),
                (
                    "prt_carrier_b_compaction00",
                    json!({ "type": "compaction", "auto": true }),
                ),
                (
                    "prt_carrier_c_agent0000000",
                    json!({ "type": "agent", "name": "build" }),
                ),
                (
                    "prt_carrier_d_snapshot0000",
                    json!({ "type": "snapshot", "snapshot": "abcdef" }),
                ),
            ] {
                insert_part(
                    &conn,
                    part_id,
                    "msg_carrier00000000000000000",
                    "ses_carrier00000000000000000",
                    1_784_100_000_100,
                    &body,
                )?;
            }
        }

        let store = Store::open_local(temp.path().join("store")).await?;
        ingest_adapter(
            &store,
            &OpencodeAdapter::new(&data),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;

        let session = store
            .get_session("ses_carrier00000000000000000")
            .await?
            .expect("carrier session ingests");
        let mut seen_types = std::collections::HashSet::new();
        for stored in &session.messages {
            for part in &stored.parts {
                let Some(raw) = raw_record(&part.options) else {
                    continue;
                };
                if let Some(kind) = raw.get("type").and_then(Value::as_str) {
                    seen_types.insert(kind.to_owned());
                    assert_eq!(
                        part.provenance,
                        Provenance::Injected,
                        "carrier part {kind} is injected turn machinery",
                    );
                }
            }
        }
        for expected in ["subtask", "compaction", "agent", "snapshot"] {
            assert!(
                seen_types.contains(expected),
                "carrier type {expected} must land with its raw_record, saw {seen_types:?}",
            );
        }
        Ok(())
    }

    /// A session id present in BOTH the DB and the legacy tree is emitted once
    /// (the DB copy wins) and the deduped tree copy is counted, not silent.
    #[tokio::test(flavor = "multi_thread")]
    async fn dual_source_dedup_prefers_db() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        // A tree that overlaps one DB session id and adds one tree-only session.
        let overlap = "ses_09fb956e1ffeKLKCXceMNLRsS0";
        let tree_only = "ses_treeonly0000000000000000";
        write_tree_session(&data.join("storage"), overlap, "TREE COPY SHOULD LOSE")?;
        write_tree_session(&data.join("storage"), tree_only, "tree-only survives")?;

        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(
            &store,
            &OpencodeAdapter::new(&data),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert!(
            summary.skipped_empty >= 1,
            "the deduped tree copy must be counted, got {summary:?}",
        );

        let overlapped = store
            .get_session(overlap)
            .await?
            .expect("overlapping session ingests (DB copy)");
        assert!(
            !overlapped.messages.iter().any(|stored| stored.parts.iter().any(|part| matches!(
                &part.kind,
                PartKind::Text { text } if text.as_deref().map(String::as_str) == Some("TREE COPY SHOULD LOSE")
            ))),
            "the DB copy wins; the tree body must not leak in",
        );
        assert!(
            store.get_session(tree_only).await?.is_some(),
            "the tree-only session still ingests",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn config_path_ending_in_storage_reads_parent_db() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        std::fs::copy(DB_FIXTURE, temp.path().join("opencode.db"))?;
        let store = Store::open_local(temp.path().join("store")).await?;
        // A legacy config points at `<data-dir>/storage`; normalization resolves
        // it to the parent, where the DB lives.
        let adapter = OpencodeAdapter::new(temp.path().join("storage"));
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        let (sessions, ..) = store.row_counts().await?;
        assert_eq!(
            sessions, DB_SESSION_COUNT,
            "the normalized parent's DB is read",
        );
        Ok(())
    }

    /// `plan` agrees with the `events_with` gate on the DB source (port of the
    /// tree test): first sync is all-pending, a max watermark makes every session
    /// fresh, and the gate skips exactly those.
    #[tokio::test(flavor = "multi_thread")]
    async fn plan_matches_the_events_gate_db_source() -> anyhow::Result<()> {
        use tokio_stream::StreamExt;

        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        let adapter = OpencodeAdapter::new(&data);

        let first = adapter
            .plan(&crate::adapter::NoopOracle)
            .await?
            .expect("opencode supports plan");
        assert_eq!(first.sessions, DB_SESSION_COUNT);
        assert_eq!(first.pending, DB_SESSION_COUNT);
        assert_eq!(first.fresh, 0);

        struct MaxWatermarkOracle;
        impl crate::adapter::SkipOracle for MaxWatermarkOracle {
            fn session_max_ts(&self, _session_id: &str) -> Option<i64> {
                Some(i64::MAX)
            }
        }
        let plan = adapter
            .plan(&MaxWatermarkOracle)
            .await?
            .expect("opencode supports plan");
        assert_eq!(plan.sessions, DB_SESSION_COUNT);
        assert_eq!(plan.fresh, DB_SESSION_COUNT, "every DB session is fresh");

        let mut gate_fresh = 0usize;
        let mut stream = adapter.events_with(&MaxWatermarkOracle);
        while let Some(item) = stream.next().await {
            match item? {
                AdapterYield::Skipped {
                    reason: SkipReason::Fresh,
                    ..
                } => gate_fresh += 1,
                AdapterYield::SkippedBatch {
                    reason: SkipReason::Fresh,
                    count,
                } => gate_fresh += count,
                _ => {}
            }
        }
        assert_eq!(gate_fresh, plan.fresh, "plan and gate must agree");
        Ok(())
    }

    const APPEND_SESSION_ID: &str = "ses_09fb956e1ffeKLKCXceMNLRsS0";
    const APPEND_MESSAGE_ID: &str = "msg_zzzzfreshdb0000000000000";
    const APPEND_CREATED_MS: i64 = 1_790_000_000_000;
    const APPEND_MICROS: i64 = APPEND_CREATED_MS * 1_000;

    fn append_db_turn(data: &std::path::Path) -> anyhow::Result<()> {
        let conn = Connection::open(data.join("opencode.db"))?;
        insert_message(
            &conn,
            APPEND_MESSAGE_ID,
            APPEND_SESSION_ID,
            APPEND_CREATED_MS,
            &json!({ "role": "user", "time": { "created": APPEND_CREATED_MS } }),
        )?;
        insert_part(
            &conn,
            "prt_zzzzfreshdb0000000000000",
            APPEND_MESSAGE_ID,
            APPEND_SESSION_ID,
            APPEND_CREATED_MS,
            &json!({ "type": "text", "text": "fresh db text", "synthetic": false }),
        )?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn freshness_db_re_reads_a_session_with_a_newer_message() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        let store = Store::open_local(temp.path().join("store")).await?;
        let adapter = OpencodeAdapter::new(&data);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        append_db_turn(&data)?;
        let oracle = FixedOracle {
            session_id: APPEND_SESSION_ID,
            watermark_micros: APPEND_MICROS - 1,
        };
        ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;

        let session = store
            .get_session(APPEND_SESSION_ID)
            .await?
            .expect("session round-trips");
        assert!(
            session
                .messages
                .iter()
                .any(|stored| stored.message.id() == APPEND_MESSAGE_ID),
            "a message newer than the watermark must land",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn freshness_db_skips_a_session_not_newer_than_the_watermark() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        let store = Store::open_local(temp.path().join("store")).await?;
        let adapter = OpencodeAdapter::new(&data);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        append_db_turn(&data)?;
        let oracle = FixedOracle {
            session_id: APPEND_SESSION_ID,
            watermark_micros: APPEND_MICROS,
        };
        let summary = ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;
        assert!(
            summary.skipped_fresh >= 1,
            "the unchanged-vs-watermark session must be skipped, got {summary:?}",
        );

        let session = store
            .get_session(APPEND_SESSION_ID)
            .await?
            .expect("session round-trips");
        assert!(
            !session
                .messages
                .iter()
                .any(|stored| stored.message.id() == APPEND_MESSAGE_ID),
            "a skipped session must not re-read the appended turn",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_db_data_drops_only_that_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let data = db_data_dir(temp.path())?;
        {
            let conn = Connection::open(data.join("opencode.db"))?;
            conn.execute(
                "UPDATE part SET data = '{not json' WHERE id = (SELECT id FROM part ORDER BY id LIMIT 1)",
                [],
            )?;
        }

        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(
            &store,
            &OpencodeAdapter::new(&data),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(
            summary.dropped_events, 1,
            "only the malformed part is dropped, got {summary:?}",
        );
        let (sessions, ..) = store.row_counts().await?;
        assert_eq!(sessions, DB_SESSION_COUNT, "all sessions still ingest");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_tree_only_root_still_ingests() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let source = temp.path().join("storage");
        copy_dir(std::path::Path::new(FIXTURES), &source)?;
        let store = Store::open_local(temp.path().join("store")).await?;
        ingest_adapter(
            &store,
            &OpencodeAdapter::new(&source),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;

        let (sessions, ..) = store.row_counts().await?;
        assert!(sessions > 0, "the bare legacy tree still ingests");
        assert!(
            store.get_session(FRESH_SESSION_ID).await?.is_some(),
            "a known tree session ingests with no DB present",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn foreign_serialization_reparses_as_opencode() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let origin_store = Store::open_local(temp.path().join("origin-store")).await?;
        let origin = crate::adapter::PiCodingAgentAdapter::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/adapter/pi-coding-agent/sessions"
        ));
        ingest_adapter(&origin_store, &origin, &crate::adapter::NoopOracle, |_| {}).await?;
        let session_id = origin_store
            .session_ids()
            .await?
            .into_iter()
            .next()
            .expect("pi fixture has sessions");
        let session = origin_store
            .get_session(&session_id)
            .await?
            .expect("fixture session is readable");

        let restored_root = temp.path().join("opencode-storage");
        crate::adapter::write_restored_files(
            &restored_root,
            OpencodeFactory.serialize(&session, RestoreFidelity::Foreign)?,
        )?;
        let restored_store = Store::open_local(temp.path().join("restored-store")).await?;
        let summary = ingest_adapter(
            &restored_store,
            &OpencodeAdapter::new(&restored_root),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;

        assert!(summary.accepted() > 0);
        assert_eq!(summary.dropped_events, 0);
        Ok(())
    }

    #[test]
    fn path_ids_reject_separators_and_traversal() {
        let where_ = "session/project/session.json";
        assert!(validate_path_id(NAME, "session id", "ses_safe", where_).is_ok());
        assert!(validate_path_id(NAME, "session id", "../ses", where_).is_err());
        assert!(validate_path_id(NAME, "session id", "/tmp/ses", where_).is_err());
        assert!(validate_path_id(NAME, "message id", "msg/a", where_).is_err());
        assert!(validate_path_id(NAME, "message id", "msg\\a", where_).is_err());
    }

    fn append_fresh_opencode_turn(root: &std::path::Path) -> anyhow::Result<()> {
        let message_dir = root.join("message").join(FRESH_SESSION_ID);
        let part_dir = root.join("part").join(FRESH_MESSAGE_ID);
        std::fs::create_dir_all(&message_dir)?;
        std::fs::create_dir_all(&part_dir)?;
        std::fs::write(
            message_dir.join(format!("{FRESH_MESSAGE_ID}.json")),
            serde_json::to_vec(&json!({
                "id": FRESH_MESSAGE_ID,
                "sessionID": FRESH_SESSION_ID,
                "role": "user",
                "time": { "created": 1759859999000i64 }
            }))?,
        )?;
        std::fs::write(
            part_dir.join(format!("{FRESH_PART_ID}.json")),
            serde_json::to_vec(&json!({
                "id": FRESH_PART_ID,
                "sessionID": FRESH_SESSION_ID,
                "messageID": FRESH_MESSAGE_ID,
                "type": "text",
                "text": "fresh opencode text",
                "synthetic": false
            }))?,
        )?;
        Ok(())
    }

    fn write_minimal_session(
        root: &std::path::Path,
        session_id: &str,
        message_id: &str,
    ) -> anyhow::Result<()> {
        write_json_file(
            &root
                .join("session")
                .join("project")
                .join(format!("{session_id}.json")),
            &json!({
                "id": session_id,
                "projectID": "project",
                "directory": "/tmp/project",
                "time": { "created": 1_765_000_000_000i64, "updated": 1_765_000_000_000i64 },
            }),
        )?;
        write_json_file(
            &root
                .join("message")
                .join(session_id)
                .join(format!("{message_id}.json")),
            &json!({
                "id": message_id,
                "sessionID": session_id,
                "role": "assistant",
                "time": { "created": 1_765_000_000_001i64 },
            }),
        )?;
        std::fs::create_dir_all(root.join("part").join(message_id))?;
        Ok(())
    }

    fn write_json_file(path: &std::path::Path, value: &Value) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec(value)?)?;
        Ok(())
    }

    fn copy_dir(from: &std::path::Path, to: &std::path::Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(to)?;
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            let source = entry.path();
            let target = to.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                copy_dir(&source, &target)?;
            } else {
                std::fs::copy(&source, &target)?;
            }
        }
        Ok(())
    }

    fn insert_session(
        conn: &Connection,
        id: &str,
        directory: &str,
        parent: Option<&str>,
        agent: Option<&str>,
        created: i64,
    ) -> anyhow::Result<()> {
        conn.execute(
            "INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, \
             agent, time_created, time_updated) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
            rusqlite::params![
                id,
                "proj-test",
                parent,
                "slug",
                directory,
                "title",
                "1.0.0",
                agent,
                created
            ],
        )?;
        Ok(())
    }

    fn insert_message(
        conn: &Connection,
        id: &str,
        session_id: &str,
        created: i64,
        data: &Value,
    ) -> anyhow::Result<()> {
        conn.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) \
             VALUES (?1, ?2, ?3, ?3, ?4)",
            rusqlite::params![id, session_id, created, serde_json::to_string(data)?],
        )?;
        Ok(())
    }

    fn insert_part(
        conn: &Connection,
        id: &str,
        message_id: &str,
        session_id: &str,
        created: i64,
        data: &Value,
    ) -> anyhow::Result<()> {
        conn.execute(
            "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) \
             VALUES (?1, ?2, ?3, ?4, ?4, ?5)",
            rusqlite::params![
                id,
                message_id,
                session_id,
                created,
                serde_json::to_string(data)?
            ],
        )?;
        Ok(())
    }

    /// Write a minimal legacy-tree session (one assistant message, one text part
    /// carrying `marker`) under `tree_base`.
    fn write_tree_session(
        tree_base: &std::path::Path,
        session_id: &str,
        marker: &str,
    ) -> anyhow::Result<()> {
        let message_id = format!("msg_tree_{session_id}");
        let part_id_value = format!("prt_tree_{session_id}");
        write_json_file(
            &tree_base
                .join("session")
                .join("proj-tree")
                .join(format!("{session_id}.json")),
            &json!({
                "id": session_id,
                "projectID": "proj-tree",
                "directory": "/tmp/tree",
                "time": { "created": 1_780_000_000_000i64 },
            }),
        )?;
        write_json_file(
            &tree_base
                .join("message")
                .join(session_id)
                .join(format!("{message_id}.json")),
            &json!({
                "id": message_id,
                "sessionID": session_id,
                "role": "assistant",
                "time": { "created": 1_780_000_000_001i64 },
            }),
        )?;
        write_json_file(
            &tree_base
                .join("part")
                .join(&message_id)
                .join(format!("{part_id_value}.json")),
            &json!({
                "id": part_id_value,
                "sessionID": session_id,
                "messageID": message_id,
                "type": "text",
                "text": marker,
                "synthetic": false,
            }),
        )?;
        Ok(())
    }
}
