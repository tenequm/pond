//! Devin CLI adapter (Cognition's local `devin` agent; docs/adapters/devin.md).
//!
//! Devin keeps every local session in ONE SQLite database, `sessions.db`, under
//! `$XDG_DATA_HOME/devin/cli/` (`%APPDATA%\devin\cli\` on Windows). The adapter
//! is rooted at that directory. A session's messages live in `message_nodes` as
//! a forest: before each model call the harness writes a fresh root holding the
//! system prefix and then COPIES of the prior messages, so one message appears
//! at many nodes. `chat_message.message_id` is stable across those copies, so a
//! pond message is one `message_id`, and every node placing it rides along in
//! `options.devin.nodes`.
//!
//! Subagents run inside the parent's forest, in trees only a subagent roots.
//! devin names them only when a subagent reports back (the `subagent/agent_id`
//! and `subagent/chain_node_id` link on the parent's messages), so the
//! partition ([`group_subagents`]) groups those trees by shape instead: each
//! child, `<id>/agent-<task prompt message id>`, holds the messages placed in
//! its trees, and the parent holds the rest - the link included.
//!
//! The writer deletes nodes on `/revert` and whole sessions on `devin rm`, and
//! re-saves nodes (same content, new `row_id`) rather than editing them, so
//! pond keeps the superset it has seen. Restore is refused
//! ([`RESTORE_UNSUPPORTED`]).

use std::collections::{BTreeMap, HashMap, HashSet, hash_map::Entry};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_stream::stream;
use chrono::DateTime;
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use crate::{
    sessions::{IngestEvent, SessionWithMessages},
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    PlanFuture, RestoreFidelity, RestoredFile, SkipOracle, SkipReason, SourceWatermark, SyncPlan,
    extract::{Extracted, extract_raw_record, extract_str, json_or_string},
    part_id, part_ordinal, source_in_sync, source_options,
    sqlite::{self, CHANNEL_CAP, emit},
    validate_path_id,
};

const NAME: &str = "devin";
const SUBAGENT_AGENT: &str = "devin/subagent";
/// Sessions an internal helper agent (the summarizer) persists with
/// `hidden = 1`, which devin's own session lists filter out (migration V15).
const HELPER_AGENT: &str = "devin/helper";
const DB_FILE: &str = "sessions.db";

const RESTORE_UNSUPPORTED: &str = "devin keeps every session as rows in one live SQLite \
     database it rewrites in place (a revert deletes messages), and pond collapses the \
     context-rebuild copies that database holds, so there is no native file to write back; \
     `pond resume <id> --to claude-code` (or any other restore target) rebuilds the \
     conversation in a client pond can write";

/// Stateless factory: opens [`DevinAdapter`] instances and probes for the
/// CLI's data directory holding `sessions.db`.
pub struct DevinFactory;

impl AdapterFactory for DevinFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(DevinAdapter::new(super::config_path(
            NAME, config,
        )?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        data_roots(&env.home)
            .into_iter()
            .find(|root| root.is_dir())
            .map(|root| json!({ "path": root }))
    }

    fn restore_unsupported(&self) -> Option<&'static str> {
        Some(RESTORE_UNSUPPORTED)
    }

    /// Unreachable through `pond resume`, which asks
    /// [`Self::restore_unsupported`] first.
    fn serialize(
        &self,
        _session: &SessionWithMessages,
        _fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        Err(AdapterError::schema(NAME, NAME, RESTORE_UNSUPPORTED))
    }
}

/// Where the CLI keeps its data, in probe order: the XDG default, the Windows
/// `%APPDATA%` (derived from home, so a redirected AppData is unsupported), then
/// the pre-rename `cognition` root, which the rename left as a compat symlink.
/// `$XDG_DATA_HOME` is the discovery layer's concern; a relocated root is
/// configured as an explicit `path`.
fn data_roots(home: &Path) -> [PathBuf; 3] {
    let xdg = home.join(".local").join("share");
    [
        xdg.join("devin").join("cli"),
        home.join("AppData")
            .join("Roaming")
            .join("devin")
            .join("cli"),
        xdg.join("cognition").join("cli"),
    ]
}

/// Configured reader, rooted at the directory holding `sessions.db`.
#[derive(Debug)]
pub struct DevinAdapter {
    root: PathBuf,
    /// Heads `discover` computed, taken by the `events_with` that follows it:
    /// one sync per instance, so the node-graph pass runs once per sync.
    heads: Mutex<Option<Heads>>,
}

impl DevinAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            heads: Mutex::default(),
        }
    }

    fn db_path(&self) -> PathBuf {
        self.root.join(DB_FILE)
    }
}

impl Adapter for DevinAdapter {
    /// Pond sessions, subagent children included, so the progress total
    /// matches what the read emits.
    fn discover(&self) -> DiscoverFuture<'_> {
        let db = self.db_path();
        Box::pin(async move {
            let heads = tokio::task::spawn_blocking(move || collect_heads(&db))
                .await
                .map_err(join_error)??;
            let count = heads.pond_sessions();
            if let Ok(mut slot) = self.heads.lock() {
                *slot = Some(heads);
            }
            Ok(count)
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let db = self.db_path();
        Box::pin(stream! {
            let cached = self.heads.lock().ok().and_then(|mut slot| slot.take());
            let heads = match cached {
                Some(heads) => heads,
                None => {
                    let heads_db = db.clone();
                    let peek = tokio::task::spawn_blocking(move || collect_heads(&heads_db));
                    match peek.await {
                        Ok(Ok(heads)) => heads,
                        Ok(Err(error)) => { yield Err(error); return; }
                        Err(join) => { yield Err(join_error(join)); return; }
                    }
                }
            };
            if let Some(reason) = heads.unsupported {
                let reason = SkipReason::Unsupported(reason);
                yield Ok(AdapterYield::Skipped { session_id: None, project: None, reason });
                return;
            }

            // An empty oracle holds nothing to compare against: read it all.
            let peek = !oracle.is_empty();
            let mut survivors = Vec::with_capacity(heads.sessions.len());
            let mut fresh = 0usize;
            for head in heads.sessions {
                let in_sync = head
                    .watermarks
                    .iter()
                    .all(|(id, mark)| source_in_sync(oracle, Some(id), *mark));
                if peek && in_sync {
                    fresh += head.watermarks.len();
                } else {
                    survivors.push(head.id);
                }
            }
            if fresh > 0 {
                yield Ok(AdapterYield::SkippedBatch { reason: SkipReason::Fresh, count: fresh });
            }
            if survivors.is_empty() {
                return;
            }

            let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
            let handle = tokio::task::spawn_blocking(move || read_sessions(&db, &survivors, &tx));
            while let Some(item) = rx.recv().await {
                yield item;
            }
            if let Err(join) = handle.await {
                yield Err(join_error(join));
            }
        })
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> PlanFuture<'a> {
        let db = self.db_path();
        Box::pin(async move {
            let heads = tokio::task::spawn_blocking(move || collect_heads(&db))
                .await
                .map_err(join_error)??;
            if oracle.is_empty() {
                return Ok(Some(SyncPlan::all_pending(heads.pond_sessions())));
            }
            Ok(Some(SyncPlan::from_heads(
                oracle,
                heads
                    .sessions
                    .iter()
                    .flat_map(|head| &head.watermarks)
                    .map(|(id, mark)| (Some(id.as_str()), *mark)),
            )))
        })
    }
}

// -- Opening and heads ---------------------------------------------------------

enum Opened {
    Forest(Connection),
    Missing,
    Unsupported(String),
}

fn open_forest(db: &Path) -> Result<Opened, AdapterError> {
    if !db.is_file() {
        return Ok(Opened::Missing);
    }
    let conn = sqlite::open_db(NAME, db)?;
    let has = |table: &str| {
        sqlite::has_table(&conn, table).map_err(|error| db_error(db, "probe tables", &error))
    };
    if !has("sessions")? {
        return Ok(Opened::Missing);
    }
    // `V5__message_forest` created `message_nodes` and dropped the linear
    // `messages` table; the CLI migrates on open, so its absence means a
    // database no current CLI has opened.
    if !has("message_nodes")? {
        return Ok(Opened::Unsupported(format!(
            "{}: a Devin CLI database from before the message-forest schema (migration V5), \
             which pond does not read; copy the file aside before running a current `devin` \
             on it, because that migration drops the old `messages` table without converting it",
            db.display()
        )));
    }
    Ok(Opened::Forest(conn))
}

/// One `sessions` row as the gate sees it: its id and the watermark of every
/// pond session it yields (the root plus each subagent child). The group is
/// read or skipped whole.
#[derive(Debug)]
struct SessionHead {
    id: String,
    watermarks: Vec<(String, SourceWatermark)>,
}

#[derive(Debug, Default)]
struct Heads {
    sessions: Vec<SessionHead>,
    unsupported: Option<String>,
}

impl Heads {
    fn pond_sessions(&self) -> usize {
        self.sessions.iter().map(|head| head.watermarks.len()).sum()
    }
}

fn collect_heads(db: &Path) -> Result<Heads, AdapterError> {
    let conn = match open_forest(db)? {
        Opened::Forest(conn) => conn,
        Opened::Missing => return Ok(Heads::default()),
        Opened::Unsupported(reason) => {
            return Ok(Heads {
                unsupported: Some(reason),
                ..Heads::default()
            });
        }
    };
    let sessions = session_rows(&conn, db)?
        .into_iter()
        .map(|id| {
            let watermarks = session_watermarks(&conn, &id)
                .unwrap_or_else(|| vec![(id.clone(), SourceWatermark::Opaque)]);
            SessionHead { id, watermarks }
        })
        .collect();
    Ok(Heads {
        sessions,
        unsupported: None,
    })
}

/// Every session id, in order.
fn session_rows(conn: &Connection, db: &Path) -> Result<Vec<String>, AdapterError> {
    let mut stmt = conn
        .prepare("SELECT id FROM sessions ORDER BY id")
        .map_err(|error| db_error(db, "prepare session list", &error))?;
    let rows = stmt
        .query_map([], |row| row.get(0))
        .map_err(|error| db_error(db, "query session list", &error))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| db_error(db, "read session list", &error))
}

/// One session's nodes in `row_id` order, as the columns [`NodeRef::from_row`]
/// reads followed by `extra`. What the partition needs from `chat_message` is
/// derived here in SQL (the string `message_id` and `metadata.created_at`, and
/// the two flags `role` and the telemetry `operation` decide), so the peek
/// reads no message body; the peek and the read both go through here, so the
/// two cannot disagree. Malformed JSON yields NULLs, never a failed statement.
fn node_query(extra: &str) -> String {
    format!(
        "SELECT row_id, node_id, parent_node_id, created_at,
             iif(json_type(doc, '$.message_id') = 'text', doc ->> '$.message_id', NULL),
             iif(json_type(doc, '$.metadata.created_at') = 'text',
                 doc ->> '$.metadata.created_at', NULL),
             ifnull(doc ->> '$.role' = 'system', 0),
             parent_node_id IS NULL AND ifnull(doc ->> '$.role' = 'user'
                 OR doc ->> '$.metadata.telemetry.operation' GLOB 'subagent_*', 0){extra}
         FROM (SELECT row_id, node_id, parent_node_id, created_at, metadata, chat_message,
                   iif(json_valid(chat_message), chat_message, NULL) AS doc
               FROM message_nodes WHERE session_id = ?1)
         ORDER BY row_id"
    )
}

/// One session's nodes for the partition; `None` when a node is not JSON with
/// a string `message_id` or a column fails to read.
fn session_nodes(conn: &Connection, id: &str) -> Option<Vec<NodeRef>> {
    let mut stmt = conn.prepare_cached(&node_query("")).ok()?;
    let mut rows = stmt.query([id]).ok()?;
    let mut nodes = Vec::new();
    while let Some(row) = rows.next().ok()? {
        nodes.push(NodeRef::from_row(row).ok()??);
    }
    Some(nodes)
}

/// One session's watermarks - the root and each subagent child - from its node
/// graph alone, never message bodies. `None` (so the session re-reads, and the
/// read reports the node) when a node is unreadable ([`session_nodes`]).
fn session_watermarks(conn: &Connection, id: &str) -> Option<Vec<(String, SourceWatermark)>> {
    let forest = Forest::new(&session_nodes(conn, id)?);
    let mut marks = vec![(id.to_owned(), forest.watermark(0))];
    for (index, task_prompt) in forest.children.iter().enumerate() {
        marks.push((child_id(id, task_prompt), forest.watermark(index + 1)));
    }
    Some(marks)
}

/// A session's `subagent_heads` rows, every column kept; the table arrived in
/// V17, so a database without it has none.
fn agent_head_rows(conn: &Connection, id: &str) -> rusqlite::Result<Vec<Value>> {
    if !sqlite::has_table(conn, "subagent_heads")? {
        return Ok(Vec::new());
    }
    dynamic_rows(
        conn,
        "SELECT * FROM subagent_heads WHERE session_id = ?1 ORDER BY agent_id",
        id,
    )
}

// -- The forest ----------------------------------------------------------------

/// What the partition needs from one `message_nodes` row.
#[derive(Debug, Clone)]
struct NodeRef {
    row_id: i64,
    node_id: i64,
    parent: Option<i64>,
    node_created: i64,
    message_id: String,
    created_at: Option<String>,
    is_system: bool,
    /// A root the harness writes only for a subagent: its system prefix
    /// (telemetry operation `subagent_<profile>`), or the parentless copy of
    /// its task prompt.
    starts_subagent: bool,
}

impl NodeRef {
    /// From the leading [`node_query`] columns; `Ok(None)` when the node has
    /// no string `message_id` (or no JSON at all).
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Option<Self>> {
        let Some(message_id) = row.get(4)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            row_id: row.get(0)?,
            node_id: row.get(1)?,
            parent: row.get(2)?,
            node_created: row.get(3)?,
            message_id,
            created_at: row.get(5)?,
            is_system: row.get(6)?,
            starts_subagent: row.get(7)?,
        }))
    }

    /// The message's own `created_at`, else the node's insert second - the
    /// writer stamps both, so the fallback is a real datum, never wall-clock.
    fn micros(&self) -> i64 {
        self.created_at
            .as_deref()
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map_or(self.node_created.saturating_mul(1_000_000), |at| {
                at.timestamp_micros()
            })
    }
}

/// Which pond sessions hold each message: owner 0 is the root session, owner
/// `i` is the child named by `children[i - 1]`, the `message_id` of that
/// subagent's task prompt. A message absent from `owners` belongs to the root
/// alone; one listed with no owner waits for a sync that can name its
/// subagent.
#[derive(Default)]
struct Forest {
    owners: HashMap<String, Vec<usize>>,
    children: Vec<String>,
    rejected: Vec<String>,
    /// Newest timestamp per owner, `None` when the owner holds no message.
    newest: Vec<Option<i64>>,
}

impl Forest {
    fn new(nodes: &[NodeRef]) -> Self {
        let mut forest = if nodes.iter().any(|node| node.starts_subagent) {
            group_subagents(nodes)
        } else {
            Self::default()
        };
        let mut newest = vec![None; forest.children.len() + 1];
        // A message's time is its first placement's, the one the read stamps;
        // a later copy's node second must not move the watermark past it.
        let mut seen = HashSet::new();
        for node in nodes {
            if !seen.insert(node.message_id.as_str()) {
                continue;
            }
            let micros = node.micros();
            for &owner in forest.owners_of(&node.message_id) {
                let slot: &mut Option<i64> = &mut newest[owner];
                *slot = Some(slot.map_or(micros, |prev| prev.max(micros)));
            }
        }
        forest.newest = newest;
        forest
    }

    fn owners_of(&self, message_id: &str) -> &[usize] {
        self.owners.get(message_id).map_or(&[0], Vec::as_slice)
    }

    fn watermark(&self, owner: usize) -> SourceWatermark {
        match self.newest.get(owner).copied().flatten() {
            Some(micros) => SourceWatermark::At(micros),
            None => SourceWatermark::Empty,
        }
    }
}

/// The [`Forest`] (its `newest` left empty) the subagent trees partition
/// into. Nothing here reads the main chain, the row order or the link devin
/// writes when a subagent reports back - all of which change between syncs -
/// so what a sync stores never depends on when it ran.
fn group_subagents(nodes: &[NodeRef]) -> Forest {
    let by_node: HashMap<i64, &NodeRef> = nodes.iter().map(|node| (node.node_id, node)).collect();
    let tree_of = tree_roots(&by_node);
    let seeded = |node: &NodeRef| by_node[&tree_of[&node.node_id]].starts_subagent;

    // A main message two subagents both copied must not merge them.
    let in_main: HashSet<&str> = nodes
        .iter()
        .filter(|node| !seeded(node))
        .map(|node| node.message_id.as_str())
        .collect();
    let mut sets: HashMap<i64, i64> = HashMap::new();
    let mut first_tree: HashMap<&str, i64> = HashMap::new();
    for node in nodes.iter().filter(|node| {
        seeded(node) && !node.is_system && !in_main.contains(node.message_id.as_str())
    }) {
        let tree = tree_of[&node.node_id];
        match first_tree.entry(node.message_id.as_str()) {
            Entry::Occupied(entry) => union(&mut sets, *entry.get(), tree),
            Entry::Vacant(entry) => {
                entry.insert(tree);
            }
        }
    }

    // Per message: whether a tree outside the subagents places it, and the
    // subagent groups whose trees do.
    let mut placed: HashMap<&str, (bool, Vec<i64>)> = HashMap::new();
    // The task prompt: a non-system node that is parentless or sits directly
    // under a subagent prefix root. It is the subagent's first message, so
    // the earliest candidate wins and one arriving later cannot rename it.
    let mut names: HashMap<i64, (i64, &str)> = HashMap::new();
    for node in nodes {
        let (in_root, groups) = placed.entry(&node.message_id).or_default();
        if !seeded(node) {
            *in_root = true;
            continue;
        }
        let group = find(&sets, tree_of[&node.node_id]);
        if !groups.contains(&group) {
            groups.push(group);
        }
        let task_prompt = !node.is_system
            && node.parent.is_none_or(|parent| {
                by_node
                    .get(&parent)
                    .is_some_and(|up| up.is_system && up.starts_subagent)
            });
        if task_prompt {
            let candidate = (node.micros(), node.message_id.as_str());
            let name = names.entry(group).or_insert(candidate);
            *name = (*name).min(candidate);
        }
    }

    // The id becomes a child session id; one that cannot be leaves the
    // subagent's messages with the parent rather than minting a malformed id.
    let mut valid: Vec<(&str, i64)> = Vec::new();
    let mut rejected = Vec::new();
    for (&group, &(_, name)) in &names {
        if validate_path_id(NAME, "subagent task prompt id", name, name).is_ok() {
            valid.push((name, group));
        } else {
            rejected.push(name.to_owned());
        }
    }
    valid.sort_unstable();
    rejected.sort_unstable();
    let index: HashMap<i64, usize> = valid
        .iter()
        .enumerate()
        .map(|(position, (_, group))| (*group, position + 1))
        .collect();

    let mut owners = HashMap::new();
    for (message, (in_root, groups)) in placed {
        if groups.is_empty() {
            continue;
        }
        let mut held: Vec<usize> = groups
            .iter()
            .filter_map(|group| match index.get(group) {
                Some(&owner) => Some(owner),
                // Rejected: back with the parent. Unnamed: nobody, yet.
                None => names.contains_key(group).then_some(0),
            })
            .collect();
        if in_root {
            held.push(0);
        }
        held.sort_unstable();
        held.dedup();
        owners.insert(message.to_owned(), held);
    }
    let children = valid.into_iter().map(|(name, _)| name.to_owned()).collect();
    Forest {
        owners,
        children,
        rejected,
        newest: Vec::new(),
    }
}

/// Each node's tree, named by its root's `node_id`. A dangling parent ends
/// the walk, and so does a revisited node (a cycle the writer never
/// produces), so a corrupt forest cannot hang a sync.
fn tree_roots(by_node: &HashMap<i64, &NodeRef>) -> HashMap<i64, i64> {
    // `None` marks a node on the walk in progress: one lookup per step both
    // reuses a finished walk and detects a cycle.
    let mut roots: HashMap<i64, Option<i64>> = HashMap::with_capacity(by_node.len());
    let mut path = Vec::new();
    for &start in by_node.keys() {
        path.clear();
        let mut cursor = start;
        let root = loop {
            match roots.entry(cursor) {
                Entry::Occupied(entry) => break entry.get().unwrap_or(cursor),
                Entry::Vacant(entry) => {
                    entry.insert(None);
                }
            }
            path.push(cursor);
            match by_node[&cursor]
                .parent
                .filter(|parent| by_node.contains_key(parent))
            {
                Some(parent) => cursor = parent,
                None => break cursor,
            }
        };
        for &id in &path {
            roots.insert(id, Some(root));
        }
    }
    roots
        .into_iter()
        .map(|(node, root)| (node, root.unwrap_or(node)))
        .collect()
}

fn find(sets: &HashMap<i64, i64>, mut tree: i64) -> i64 {
    while let Some(&up) = sets.get(&tree) {
        tree = up;
    }
    tree
}

fn union(sets: &mut HashMap<i64, i64>, a: i64, b: i64) {
    let (a, b) = (find(sets, a), find(sets, b));
    if a != b {
        sets.insert(b, a);
    }
}

fn child_id(session_id: &str, task_prompt: &str) -> String {
    format!("{session_id}/agent-{task_prompt}")
}

// -- Reading -------------------------------------------------------------------

/// One message: its newest `chat_message` (a later copy can only add fields -
/// the ACP tool content lands after the fact), every older distinct variant,
/// and every node placing it.
struct Collected {
    newest: Value,
    newest_raw: String,
    first: NodeRef,
    variants: Vec<Value>,
    nodes: Vec<Value>,
}

fn read_sessions(db: &Path, ids: &[String], tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>) {
    let conn = match open_forest(db) {
        Ok(Opened::Forest(conn)) => conn,
        Ok(Opened::Missing | Opened::Unsupported(_)) => {
            let error = AdapterError::schema(
                NAME,
                db.display().to_string(),
                "the session database changed shape between listing and read; \
                 re-run `pond sync devin`",
            );
            let _ = tx.blocking_send(Err(error));
            return;
        }
        Err(error) => {
            let _ = tx.blocking_send(Err(error));
            return;
        }
    };
    let schema_version = conn
        .query_row(
            "SELECT MAX(version) FROM refinery_schema_history",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )
        .ok()
        .flatten();
    for id in ids {
        if !read_session(&conn, db, id, schema_version, tx) {
            return;
        }
    }
}

/// One `sessions` row with everything it yields: the root session, its
/// messages, then each subagent child and its messages. A failure before the
/// root session is a skip naming this session, so the ingest never charges it
/// to the session read before.
fn read_session(
    conn: &Connection,
    db: &Path,
    id: &str,
    schema_version: Option<i64>,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    let location = format!("{}#{id}", db.display());
    let skip = |reason: SkipReason| {
        let skipped = AdapterYield::Skipped {
            session_id: Some(id.to_owned()),
            project: None,
            reason,
        };
        tx.blocking_send(Ok(skipped)).is_ok()
    };
    // A root id that fails here would never reach a restore filename or the
    // default search: `/` is the subagent marker there, `:` an NTFS stream.
    if let Err(error) = validate_path_id(NAME, "session id", id, &location) {
        return skip(SkipReason::Unsupported(error.to_string()));
    }
    let mut read = match SessionRead::load(conn, db, id, &location) {
        Ok(Some(read)) => read,
        // `devin rm` between listing and read.
        Ok(None) => return skip(SkipReason::Empty),
        Err(error) => return skip(SkipReason::Unsupported(error.to_string())),
    };

    let root = read.root_session(id, schema_version);
    emit!(tx, Ok(AdapterYield::Event(IngestEvent::Session(root))));
    // Node and child-name errors wait for the root, so the ingest charges
    // them to this session.
    for error in std::mem::take(&mut read.errors) {
        emit!(tx, Err(error));
    }
    let forest = Forest::new(&read.refs);
    for message_id in &forest.rejected {
        let error = AdapterError::schema(
            NAME,
            location.clone(),
            format!(
                "subagent task prompt id {message_id:?} cannot name a child \
                 session; its messages stay with the parent"
            ),
        );
        emit!(tx, Err(error));
    }
    let tools = ToolIndex::new(&read.tool_states, read.collected.values());
    let owned = read.partition(&forest);
    for entry in &owned[0] {
        if !emit_message(tx, id, entry, &tools) {
            return false;
        }
    }
    for (index, task_prompt) in forest.children.iter().enumerate() {
        if !emit_child(tx, id, task_prompt, &owned[index + 1], &read, &tools) {
            return false;
        }
    }
    true
}

/// A subagent child, created at its task prompt's time. Apart from the
/// project, which is latched at first ingest like the root's, its row holds
/// only what devin writes with the subagent's first nodes, so a sync that
/// lands while it runs stores the same row as one after it reports back. Its
/// parent is the root even for a subagent another one spawned: that spawner
/// is written only at report time, and the link messages naming it stay
/// stored in the parent.
fn emit_child(
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
    root: &str,
    task_prompt: &str,
    messages: &[&Collected],
    read: &SessionRead,
    tools: &ToolIndex,
) -> bool {
    let id = child_id(root, task_prompt);
    let Some(created_at) = read
        .collected
        .get(task_prompt)
        .and_then(|entry| DateTime::from_timestamp_micros(entry.first.micros()))
    else {
        let reason = "the subagent's task prompt carries no usable timestamp";
        emit!(tx, Err(AdapterError::schema(NAME, id, reason)));
        return true;
    };
    let session = Session {
        id: id.clone(),
        parent_session_id: Some(root.to_owned()),
        parent_message_id: None,
        source_agent: SUBAGENT_AGENT.to_owned(),
        created_at,
        project: read.project.clone(),
        options: ProviderOptions::new(),
    };
    emit!(tx, Ok(AdapterYield::Event(IngestEvent::Session(session))));
    for entry in messages {
        if !emit_message(tx, &id, entry, tools) {
            return false;
        }
    }
    true
}

/// Everything read for one `sessions` row inside one read transaction, so the
/// row, its nodes, tool state and subagent heads come from the same snapshot
/// while devin keeps writing.
struct SessionRead {
    row: Value,
    created_at: DateTime<chrono::Utc>,
    project: Extracted<String>,
    refs: Vec<NodeRef>,
    collected: BTreeMap<String, Collected>,
    tool_states: Vec<Value>,
    agent_heads: Vec<Value>,
    errors: Vec<AdapterError>,
}

impl SessionRead {
    /// `Ok(None)` when the row vanished since the listing.
    fn load(
        conn: &Connection,
        db: &Path,
        id: &str,
        location: &str,
    ) -> Result<Option<Self>, AdapterError> {
        let snapshot = conn
            .unchecked_transaction()
            .map_err(|error| db_error(db, "begin read", &error))?;
        let Some(row) = dynamic_rows(&snapshot, "SELECT * FROM sessions WHERE id = ?1", id)
            .map_err(|error| db_error(db, "read session row", &error))?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let schema = |reason: &str| AdapterError::schema(NAME, location, reason);
        let created_at = row
            .get("created_at")
            .and_then(Value::as_i64)
            .and_then(|secs| DateTime::from_timestamp(secs, 0))
            .ok_or_else(|| schema("session has no integer created_at"))?;
        let project = extract_str(&row, "working_directory")
            .filter(|dir| !dir.is_empty())
            .ok_or_else(|| schema("session has no working_directory"))?;
        // `tool_call_state` arrived in V14; a forest database before it has none.
        let tool_states = if sqlite::has_table(&snapshot, "tool_call_state")
            .map_err(|error| db_error(db, "probe tool_call_state", &error))?
        {
            dynamic_rows(
                &snapshot,
                "SELECT * FROM tool_call_state WHERE session_id = ?1 ORDER BY rowid",
                id,
            )
            .map_err(|error| db_error(db, "read tool_call_state", &error))?
        } else {
            Vec::new()
        };
        let agent_heads = agent_head_rows(&snapshot, id)
            .map_err(|error| db_error(db, "read subagent_heads", &error))?;
        let mut read = Self {
            row,
            created_at,
            project,
            refs: Vec::new(),
            collected: BTreeMap::new(),
            tool_states,
            agent_heads,
            errors: Vec::new(),
        };
        read.collect_nodes(&snapshot, id, location)
            .map_err(|error| db_error(db, "read message_nodes", &error))?;
        Ok(Some(read))
    }

    /// Fold every node in as it streams, so only distinct message bodies stay
    /// resident: an identical copy of the newest known form only adds a
    /// placement; anything else is parsed.
    fn collect_nodes(
        &mut self,
        conn: &Connection,
        id: &str,
        location: &str,
    ) -> rusqlite::Result<()> {
        let mut stmt = conn.prepare_cached(&node_query(", metadata, chat_message"))?;
        let mut rows = stmt.query([id])?;
        while let Some(row) = rows.next()? {
            let node_id: i64 = row.get(1)?;
            let metadata: Option<String> = row.get(8)?;
            let chat_message: String = row.get(9)?;
            let node = NodeRef::from_row(row)?;
            let parsed = node.and_then(|node| {
                let known = self
                    .collected
                    .get(&node.message_id)
                    .is_some_and(|entry| entry.newest_raw == chat_message);
                if known {
                    return Some((node, None));
                }
                let message = serde_json::from_str::<Value>(&chat_message).ok()?;
                Some((node, Some(message)))
            });
            let Some((node, message)) = parsed else {
                self.errors.push(AdapterError::schema(
                    NAME,
                    format!("{location}/node {node_id}"),
                    "chat_message is not a JSON object with a string message_id",
                ));
                continue;
            };
            let placement = placement(&node, metadata.as_deref());
            match (self.collected.get_mut(&node.message_id), message) {
                (Some(entry), None) => entry.nodes.push(placement),
                (Some(entry), Some(message)) => {
                    // A copy can differ only in key order and still be the
                    // same value; only a real change is a variant.
                    if entry.newest != message {
                        let older = std::mem::replace(&mut entry.newest, message);
                        if !entry.variants.contains(&older) {
                            entry.variants.push(older);
                        }
                    }
                    entry.newest_raw = chat_message;
                    entry.nodes.push(placement);
                }
                (None, Some(message)) => {
                    self.collected.insert(
                        node.message_id.clone(),
                        Collected {
                            newest: message,
                            newest_raw: chat_message,
                            first: node.clone(),
                            variants: Vec::new(),
                            nodes: vec![placement],
                        },
                    );
                }
                (None, None) => unreachable!("an unknown message is always parsed"),
            }
            self.refs.push(node);
        }
        Ok(())
    }

    fn root_session(&self, id: &str, schema_version: Option<i64>) -> Session {
        let mut devin = Map::new();
        if let Some(version) = schema_version {
            devin.insert("schema_version".to_owned(), json!(version));
        }
        if !self.tool_states.is_empty() {
            let states = Value::Array(self.tool_states.clone());
            devin.insert("tool_call_state".to_owned(), states);
        }
        if !self.agent_heads.is_empty() {
            let heads = Value::Array(self.agent_heads.clone());
            devin.insert("subagent_heads".to_owned(), heads);
        }
        let mut options = source_options(NAME, &self.row);
        options.insert(NAME.to_owned(), Value::Object(devin));
        let hidden = self.row.get("hidden").and_then(Value::as_i64) == Some(1);
        Session {
            id: id.to_owned(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: if hidden { HELPER_AGENT } else { NAME }.to_owned(),
            created_at: self.created_at,
            project: self.project.clone(),
            options,
        }
    }

    /// Messages per owner (root first, then each child), each in
    /// `(timestamp, message id)` order.
    fn partition(&self, forest: &Forest) -> Vec<Vec<&Collected>> {
        let mut owned: Vec<Vec<&Collected>> = vec![Vec::new(); forest.children.len() + 1];
        for (message_id, entry) in &self.collected {
            for &owner in forest.owners_of(message_id) {
                owned[owner].push(entry);
            }
        }
        for list in &mut owned {
            list.sort_by_cached_key(|entry| (entry.first.micros(), entry.first.message_id.clone()));
        }
        owned
    }
}

fn emit_message(
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
    session_id: &str,
    entry: &Collected,
    tools: &ToolIndex,
) -> bool {
    match message_events(session_id, entry, tools) {
        Ok(events) => {
            for event in events {
                emit!(tx, Ok(AdapterYield::Event(event)));
            }
        }
        Err(error) => emit!(tx, Err(error)),
    }
    true
}

/// A node's own columns, `metadata` parsed when it is JSON: where the message
/// sits in the forest and why (system prefix, compaction source).
fn placement(node: &NodeRef, metadata: Option<&str>) -> Value {
    let mut placement = json!({
        "row_id": node.row_id,
        "node_id": node.node_id,
        "created_at": node.node_created,
    });
    if let Some(parent) = node.parent {
        placement["parent_node_id"] = json!(parent);
    }
    if let Some(metadata) = metadata {
        placement["metadata"] = json_or_string(metadata);
    }
    placement
}

/// Rows of a one-parameter query as JSON objects keyed by column name, every
/// column kept (`SELECT *`), so a column a later schema adds rides along.
/// JSON-text columns stay verbatim strings.
fn dynamic_rows(conn: &Connection, sql: &str, param: &str) -> rusqlite::Result<Vec<Value>> {
    let mut stmt = conn.prepare_cached(sql)?;
    let names: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    let rows = stmt.query_map([param], |row| {
        let mut map = Map::new();
        for (index, name) in names.iter().enumerate() {
            let value = sqlite::value_json(row.get_ref(index)?);
            if !value.is_null() {
                map.insert(name.clone(), value);
            }
        }
        Ok(Value::Object(map))
    })?;
    rows.collect()
}

// -- Messages ---------------------------------------------------------------

/// Tool names and outcomes by `tool_call_id`, from the ACP `tool_call_state`
/// rows and the assistant `tool_calls` that issued them.
struct ToolIndex {
    names: HashMap<String, Extracted<String>>,
    failed: HashSet<String>,
}

impl ToolIndex {
    fn new<'a>(states: &[Value], messages: impl Iterator<Item = &'a Collected>) -> Self {
        let mut names = HashMap::new();
        let mut failed = HashSet::new();
        for message in messages {
            for call in message
                .newest
                .get("tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let (Some(id), Some(name)) = (
                    call.get("id").and_then(Value::as_str),
                    extract_str(call, "name"),
                ) {
                    names.insert(id.to_owned(), name);
                }
            }
        }
        for state in states {
            let Some(id) = state.get("tool_call_id").and_then(Value::as_str) else {
                continue;
            };
            let parse = |column: &str| {
                state
                    .get(column)
                    .and_then(Value::as_str)
                    .and_then(|text| serde_json::from_str::<Value>(text).ok())
            };
            let call = parse("tool_call_json");
            let update = parse("tool_call_update_json");
            let inferred = [&update, &call].into_iter().flatten().find_map(|doc| {
                doc.get("_meta")
                    .and_then(|meta| extract_str(meta, "cognition.ai/inferenceToolName"))
            });
            if let Some(name) = inferred {
                names.insert(id.to_owned(), name);
            }
            if update
                .as_ref()
                .and_then(|update| update.get("status"))
                .and_then(Value::as_str)
                == Some("failed")
            {
                failed.insert(id.to_owned());
            }
        }
        Self { names, failed }
    }
}

fn message_events(
    session_id: &str,
    entry: &Collected,
    tools: &ToolIndex,
) -> Result<Vec<IngestEvent>, AdapterError> {
    let message = &entry.newest;
    let id = format!("{session_id}:{}", entry.first.message_id);
    let Some(timestamp) = DateTime::from_timestamp_micros(entry.first.micros()) else {
        return Err(AdapterError::schema(
            NAME,
            id,
            "message timestamp is out of range",
        ));
    };
    let options = message_options(entry);
    let content = extract_str(message, "content");
    let mut parts = Vec::new();
    let mut push = |provenance: Provenance, kind: PartKind| {
        let ordinal = parts.len();
        parts.push(Part {
            session_id: session_id.to_owned(),
            id: part_id(&id, ordinal),
            message_id: id.clone(),
            ordinal: part_ordinal(ordinal),
            provenance,
            options: ProviderOptions::new(),
            kind,
        });
    };
    let text = content.clone().filter(|text| !text.is_empty());

    let canonical = match message.get("role").and_then(Value::as_str) {
        Some("user") => {
            // The summarizer's inputs are user-role rows the harness writes;
            // only what a person typed carries `is_user_input`.
            let typed = message
                .pointer("/metadata/is_user_input")
                .and_then(Value::as_bool)
                == Some(true);
            let provenance = if typed {
                Provenance::Conversational
            } else {
                Provenance::Injected
            };
            if let Some(text) = text {
                push(provenance, PartKind::Text { text: Some(text) });
            }
            Message::User {
                id: id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        Some("assistant") => {
            if let Some(thinking) = message
                .get("thinking")
                .and_then(|thinking| extract_str(thinking, "thinking"))
                .filter(|thinking| !thinking.is_empty())
            {
                push(
                    Provenance::Conversational,
                    PartKind::Reasoning {
                        text: Some(thinking),
                    },
                );
            }
            if let Some(text) = text {
                push(
                    Provenance::Conversational,
                    PartKind::Text { text: Some(text) },
                );
            }
            for call in message
                .get("tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                push(
                    Provenance::Conversational,
                    PartKind::ToolCall {
                        call_id: extract_str(call, "id"),
                        name: extract_str(call, "name"),
                        params: call.get("arguments").cloned().unwrap_or(Value::Null),
                        provider_executed: false,
                    },
                );
            }
            Message::Assistant {
                id: id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        Some("tool") => {
            let call_id = message.get("tool_call_id").and_then(Value::as_str);
            let name = call_id.and_then(|call| tools.names.get(call)).cloned();
            let failed = call_id.is_some_and(|call| tools.failed.contains(call))
                || message
                    .pointer("/metadata/extensions/chisel~1tool_failure")
                    .is_some();
            push(
                Provenance::Injected,
                PartKind::ToolResult {
                    call_id: extract_str(message, "tool_call_id"),
                    name,
                    is_failure: failed,
                    result: message.get("content").cloned().unwrap_or(Value::Null),
                },
            );
            Message::Tool {
                id: id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options,
            }
        }
        // `system` and any role a later CLI adds: a carrier holding the text,
        // the whole row in options (spec.md#adapter-integrity-no-silent-drops).
        _ => Message::System {
            id: id.clone(),
            session_id: session_id.to_owned(),
            timestamp,
            content,
            options,
        },
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(canonical));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

fn message_options(entry: &Collected) -> ProviderOptions {
    let mut devin = Map::new();
    devin.insert("nodes".to_owned(), Value::Array(entry.nodes.clone()));
    if !entry.variants.is_empty() {
        let variants = entry.variants.iter().map(extract_raw_record).collect();
        devin.insert("variants".to_owned(), Value::Array(variants));
    }
    let mut options = ProviderOptions::new();
    options.insert(NAME.to_owned(), Value::Object(devin));
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": NAME,
            "row_id": entry.first.row_id,
            "raw_record": extract_raw_record(&entry.newest),
        }),
    );
    options
}

// -- Small helpers ----------------------------------------------------------

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
    use crate::wire::Role;
    use std::collections::BTreeSet;
    use tempfile::TempDir;

    const MACOS: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/devin/macos/cli"
    );
    const WINDOWS: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/devin/windows/cli"
    );

    #[derive(Default)]
    struct Read {
        sessions: BTreeMap<String, Session>,
        messages: BTreeMap<String, Vec<Message>>,
        parts: HashMap<String, Vec<Part>>,
        errors: Vec<AdapterError>,
        /// Session ids and errors in yield order.
        order: Vec<String>,
    }

    impl Read {
        fn parts_of(&self, message: &Message) -> &[Part] {
            self.parts.get(message.id()).map_or(&[], Vec::as_slice)
        }

        fn tool_result(&self, session: &str, needle: &str) -> (bool, Option<String>) {
            self.messages[session]
                .iter()
                .flat_map(|message| self.parts_of(message))
                .find_map(|part| match &part.kind {
                    PartKind::ToolResult {
                        is_failure,
                        name,
                        result,
                        ..
                    } if result.as_str().is_some_and(|text| text.contains(needle)) => Some((
                        *is_failure,
                        name.as_ref().map(|name| name.as_str().to_owned()),
                    )),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{session}: no tool result containing {needle:?}"))
        }
    }

    fn read(db: &Path) -> Read {
        let conn = match open_forest(db).unwrap() {
            Opened::Forest(conn) => conn,
            _ => panic!("fixture is not a forest database"),
        };
        let ids = session_rows(&conn, db).unwrap();
        drop(conn);
        let (tx, mut rx) = mpsc::channel(1024);
        let mut out = Read::default();
        std::thread::scope(|scope| {
            let ids = &ids;
            scope.spawn(move || read_sessions(db, ids, &tx));
            while let Some(item) = rx.blocking_recv() {
                match item {
                    Ok(AdapterYield::Event(IngestEvent::Session(session))) => {
                        out.order.push(session.id.clone());
                        out.messages.entry(session.id.clone()).or_default();
                        out.sessions.insert(session.id.clone(), session);
                    }
                    Ok(AdapterYield::Event(IngestEvent::Message(message))) => {
                        out.messages
                            .entry(message.session_id().to_owned())
                            .or_default()
                            .push(message);
                    }
                    Ok(AdapterYield::Event(IngestEvent::Part(part))) => {
                        out.parts
                            .entry(part.message_id.clone())
                            .or_default()
                            .push(part);
                    }
                    Ok(other) => panic!("unexpected yield {other:?}"),
                    Err(error) => {
                        out.order.push("error".to_owned());
                        out.errors.push(error);
                    }
                }
            }
        });
        out
    }

    fn macos() -> Read {
        let read = read(&Path::new(MACOS).join(DB_FILE));
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        read
    }

    #[test]
    fn probe_default_finds_each_data_root() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &DevinFactory,
            &[".local", "share", "devin", "cli"],
        )?;
        crate::adapter::test_support::assert_probe_default(
            &DevinFactory,
            &["AppData", "Roaming", "devin", "cli"],
        )?;
        crate::adapter::test_support::assert_probe_default(
            &DevinFactory,
            &[".local", "share", "cognition", "cli"],
        )
    }

    /// A one-turn headless session writes 27 nodes - three context rebuilds
    /// copying the prefix and the prompt - for 9 distinct messages; every node
    /// survives as a placement of the message it copies.
    #[test]
    fn context_rebuild_copies_collapse_to_one_message_each() {
        let read = macos();
        let messages = &read.messages["level-waterlily"];
        assert_eq!(messages.len(), 9);
        let placements: usize = messages
            .iter()
            .map(|message| message.options()[NAME]["nodes"].as_array().unwrap().len())
            .sum();
        assert_eq!(placements, 27);
        let roles: Vec<Role> = messages.iter().map(Message::role).collect();
        assert_eq!(
            roles.iter().filter(|role| **role == Role::System).count(),
            7
        );
    }

    /// Each subagent's trees yield a child holding its own messages - the
    /// system prefix it shares with the other subagents of its profile
    /// included - with a row naming only its parent; the link devin writes
    /// at report time stays stored in the parent, whole. A fork that copied a
    /// subagent link without its nodes yields no child.
    #[test]
    fn subagents_split_into_children_and_forks_yield_none() {
        let read = macos();
        let count = |id: &str| read.messages[id].len();
        assert_eq!(count("amplified-color"), 39);
        assert_eq!(
            count("amplified-color/agent-51d1dc31-ee2a-44d7-b6f8-3be0f0cf5d6e"),
            10
        );
        assert_eq!(count("chalk-twig"), 27);
        let child = "chalk-twig/agent-9b8139a3-5985-4ab6-b952-405982c113bd";
        assert_eq!(count(child), 7);
        let resumed = "chalk-twig/agent-ef9333ef-da80-48fd-b99b-ad32bc2b3d04";
        assert_eq!(count(resumed), 7);
        assert_eq!(count("power-almandine"), 15);
        assert_eq!(read.sessions.len(), 8);
        assert!(
            !read
                .sessions
                .keys()
                .any(|id| id.starts_with("power-almandine/"))
        );

        let session = &read.sessions[child];
        assert_eq!(session.source_agent, SUBAGENT_AGENT);
        assert_eq!(session.parent_session_id.as_deref(), Some("chalk-twig"));
        assert_eq!(session.parent_message_id, None);
        assert!(session.options.is_empty());
        // Created at its task prompt, not at the system prefix it shares with
        // the older sibling.
        let resumed = &read.sessions[resumed];
        let task_prompt: DateTime<chrono::Utc> = "2026-09-26T01:57:15.866591Z".parse().unwrap();
        assert_eq!(resumed.created_at, task_prompt);
        assert!(resumed.created_at > session.created_at);
        assert!(
            read.messages["chalk-twig"].iter().any(|message| {
                message.options()["source"]["raw_record"]
                    .pointer("/metadata/extensions/subagent~1chain_node_id")
                    == Some(&json!(43))
            }),
            "the report-time link is stored in the parent"
        );
        assert!(
            read.sessions["power-almandine"].parent_session_id.is_none(),
            "a fork records no parent"
        );
    }

    /// Only typed input is conversational: the compaction summarizer's
    /// user-role inputs are harness-written.
    #[test]
    fn summarizer_inputs_are_injected_and_typed_prompts_conversational() {
        let read = macos();
        let user_parts: Vec<&Part> = read.messages["amplified-color"]
            .iter()
            .filter(|message| message.role() == Role::User)
            .flat_map(|message| read.parts_of(message))
            .collect();
        let text = |part: &Part| match &part.kind {
            PartKind::Text { text: Some(text) } => text.as_str().to_owned(),
            _ => String::new(),
        };
        let summarizer = user_parts
            .iter()
            .find(|part| text(part).contains("Conversation to summarize"))
            .unwrap();
        assert_eq!(summarizer.provenance, Provenance::Injected);
        let typed = user_parts
            .iter()
            .find(|part| text(part).starts_with("What is 2+2?"))
            .unwrap();
        assert_eq!(typed.provenance, Provenance::Conversational);
    }

    /// A non-zero exit is a completed call; an interrupt (`status: failed`)
    /// and a tool validation failure are failures. Names come from the ACP
    /// record.
    #[test]
    fn tool_outcomes_follow_the_acp_status() {
        let read = macos();
        assert_eq!(
            read.tool_result("branch-candy", "No such file or directory"),
            (false, Some("exec".to_owned()))
        );
        assert_eq!(
            read.tool_result("amplified-color", "Canceled due to user interrupt"),
            (true, Some("exec".to_owned()))
        );
        assert!(read.tool_result("amplified-color", "validation failed").0);
        assert_eq!(
            read.tool_result("branch-candy", "<file-view").1.as_deref(),
            Some("read")
        );
    }

    /// A later copy that adds the ACP tool content wins; the earlier shape is
    /// kept as a variant rather than dropped.
    #[test]
    fn the_newest_copy_wins_and_older_variants_survive() {
        let read = macos();
        let with_calls = read.messages["branch-candy"]
            .iter()
            .find(|message| {
                message.role() == Role::Assistant
                    && message.options()["source"]["raw_record"]["tool_calls"]
                        .as_array()
                        .is_some_and(|calls| !calls.is_empty())
            })
            .unwrap();
        let options = with_calls.options();
        assert!(
            options["source"]["raw_record"]
                .pointer("/metadata/extensions/chisel~1tool_call_content")
                .is_some()
        );
        assert!(
            options[NAME]["variants"]
                .as_array()
                .is_some_and(|variants| !variants.is_empty())
        );

        // Copies that differ only in key order are one value, never a variant.
        for message in read.messages.values().flatten() {
            let options = message.options();
            let newest = &options["source"]["raw_record"];
            let variants = options[NAME]["variants"]
                .as_array()
                .map_or(&[][..], Vec::as_slice);
            assert!(!variants.contains(newest), "{}", message.id());
        }
    }

    const MIDRUN: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/devin/midrun"
    );
    const MIDRUN_CHILD: &str = "gilded-orca/agent-e9b73e40-5526-42b0-acae-45389ecfe004";

    fn midrun(stage: &str) -> Read {
        let read = read(&Path::new(MIDRUN).join(stage).join("cli").join(DB_FILE));
        assert!(read.errors.is_empty(), "{stage}: {:?}", read.errors);
        read
    }

    /// A subagent still running when the sync reads - its link not written
    /// yet - yields the same child row it yields once it reports back, and
    /// every message the running snapshot places keeps its session through
    /// two compactions and the re-save that renumbers every row: a sync in
    /// between stores nothing a single later sync would not.
    #[test]
    fn a_running_subagent_reads_as_it_will_once_it_reports() {
        let placed = |read: &Read| -> BTreeSet<String> {
            read.messages
                .values()
                .flatten()
                .map(|message| message.id().to_owned())
                .collect()
        };
        let (before, after) = (midrun("before"), midrun("after"));
        let later = placed(&after);
        let moved: Vec<String> = placed(&before).difference(&later).cloned().collect();
        assert!(moved.is_empty(), "{moved:?}");

        let running = &before.sessions[MIDRUN_CHILD];
        assert_eq!(running, &after.sessions[MIDRUN_CHILD]);
        assert_eq!(running.parent_session_id.as_deref(), Some("gilded-orca"));
        assert_eq!(running.parent_message_id, None);
        assert!(before.messages[MIDRUN_CHILD].len() > 1);
        assert_eq!(
            after.sessions.len(),
            2,
            "the summarizer chains stay with the root"
        );
    }

    fn peeked(db: &Path) -> BTreeMap<String, SourceWatermark> {
        collect_heads(db)
            .unwrap()
            .sessions
            .into_iter()
            .flat_map(|head| head.watermarks)
            .collect()
    }

    fn assert_peek_matches_read(db: &Path) {
        let read = read(db);
        let peeked: BTreeMap<String, SourceWatermark> = peeked(db).into_iter().collect();
        let emitted: BTreeMap<String, SourceWatermark> = read
            .messages
            .iter()
            .map(|(id, messages)| {
                let newest = messages
                    .iter()
                    .map(|message| message.timestamp().timestamp_micros())
                    .max();
                (
                    id.clone(),
                    newest.map_or(SourceWatermark::Empty, SourceWatermark::At),
                )
            })
            .collect();
        assert_eq!(peeked, emitted, "{}", db.display());
    }

    /// The peek's watermark is exactly the newest timestamp the read emits for
    /// each pond session, so an unchanged database gates fresh.
    #[test]
    fn watermark_matches_the_read() {
        for root in [MACOS, WINDOWS] {
            assert_peek_matches_read(&Path::new(root).join(DB_FILE));
        }
        for stage in ["before", "after"] {
            assert_peek_matches_read(&Path::new(MIDRUN).join(stage).join("cli").join(DB_FILE));
        }
    }

    /// Without `metadata.created_at` a message falls back to its first node's
    /// second; later copies carry later seconds and must not move the
    /// watermark past what the read stamps, or the session never gates fresh.
    #[test]
    fn a_message_without_created_at_keeps_peek_and_read_equal() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(Path::new(MACOS).join(DB_FILE), &db).unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "UPDATE message_nodes
             SET chat_message = json_remove(chat_message, '$.metadata.created_at'),
                    created_at = created_at + row_id
             WHERE session_id = 'amplified-color'",
            [],
        )
        .unwrap();
        drop(conn);
        assert_peek_matches_read(&db);
    }

    /// Subagent trees group by shape alone - the task prompt's parentless
    /// copy and every tree a subagent prefix roots, joined through shared
    /// non-system messages, a compaction continuation included - and are
    /// named by that task prompt. A message is held by every session whose
    /// trees place it: two subagents of one profile each keep the prefix they
    /// share, and a main message copied into subagent trees stays with the
    /// root too, without merging the two subagents that copied it. The
    /// earliest task prompt names a child, so a later candidate with a smaller
    /// id cannot rename it. A group nothing names yet holds its messages back,
    /// and a cycle ends the walk instead of hanging. The nodes go through the
    /// peek's own SQL, so each node's classification is exercised too.
    #[test]
    fn subagent_trees_group_by_shape_alone() {
        type Row = (i64, Option<i64>, &'static str, &'static str, &'static str);
        let node =
            |node_id, parent, role, message| -> Row { (node_id, parent, role, "normal", message) };
        let prefix =
            |node_id, operation, message| -> Row { (node_id, None, "system", operation, message) };
        let nodes = [
            prefix(1, "normal", "main-prefix"),
            node(2, Some(1), "user", "prompt"),
            node(3, Some(2), "assistant", "call"),
            node(4, Some(3), "tool", "result"),
            node(10, None, "user", "task"),
            prefix(11, "subagent_general", "sub-prefix"),
            node(12, Some(11), "user", "task"),
            node(13, Some(12), "assistant", "sub-reply"),
            prefix(14, "subagent_general", "sub-prefix"),
            node(15, Some(14), "system", "summary"),
            node(16, Some(15), "assistant", "sub-reply"),
            node(17, Some(16), "tool", "sub-tool"),
            node(18, Some(17), "user", "prompt"),
            prefix(20, "subagent_general", "sub-prefix"),
            node(21, Some(20), "user", "task-2"),
            node(22, Some(21), "user", "prompt"),
            prefix(30, "unknown", "summarizer-prefix"),
            node(31, Some(30), "user", "summarize"),
            prefix(40, "subagent_explore", "lone-prefix"),
            node(41, Some(40), "system", "lone-summary"),
            node(42, Some(41), "assistant", "lone-reply"),
            node(50, Some(51), "user", "loop-a"),
            node(51, Some(50), "user", "loop-b"),
            prefix(60, "subagent_general", "sub-prefix"),
            node(61, Some(60), "user", "a-later"),
            node(62, Some(61), "assistant", "sub-reply"),
        ];
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE message_nodes (
                 row_id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
                 node_id INTEGER NOT NULL, parent_node_id INTEGER, chat_message TEXT NOT NULL,
                 created_at INTEGER NOT NULL, metadata TEXT)",
        )
        .unwrap();
        for (node_id, parent, role, operation, message) in nodes {
            let chat_message = json!({
                "message_id": message,
                "role": role,
                "metadata": { "telemetry": { "operation": operation } },
            });
            conn.execute(
                "INSERT INTO message_nodes (session_id, node_id, parent_node_id, chat_message,
                     created_at) VALUES ('s', ?1, ?2, ?3, ?1)",
                rusqlite::params![node_id, parent, chat_message.to_string()],
            )
            .unwrap();
        }
        let forest = Forest::new(&session_nodes(&conn, "s").unwrap());
        assert_eq!(forest.children, ["task", "task-2"]);
        for (message, owners) in [
            ("main-prefix", &[0][..]),
            ("result", &[0]),
            ("task", &[1]),
            ("sub-reply", &[1]),
            ("summary", &[1]),
            ("sub-tool", &[1]),
            ("a-later", &[1]),
            ("sub-prefix", &[1, 2]),
            ("task-2", &[2]),
            ("prompt", &[0, 1, 2]),
            ("summarize", &[0]),
            ("lone-prefix", &[]),
            ("lone-reply", &[]),
            ("loop-a", &[0]),
        ] {
            assert_eq!(forest.owners_of(message), owners, "{message}");
        }
    }

    /// A database that predates the forest is a visible, counted skip naming
    /// the fix, never an empty success.
    #[test]
    fn a_pre_forest_database_is_unsupported() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, working_directory TEXT NOT NULL);
             CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, content TEXT);",
        )
        .unwrap();
        drop(conn);
        let heads = collect_heads(&db).unwrap();
        assert!(heads.unsupported.unwrap().contains("copy the file aside"));
    }

    /// A corrupt node is a typed error attributed to its node; the rest of the
    /// session still ingests, and the peek re-reads that session rather than
    /// trusting a partial graph - without dragging the others along.
    #[test]
    fn a_corrupt_node_is_a_typed_error_and_the_peek_goes_opaque() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(Path::new(MACOS).join(DB_FILE), &db).unwrap();
        Connection::open(&db)
            .unwrap()
            .execute(
                "UPDATE message_nodes SET chat_message = '{not json'
                 WHERE session_id = 'level-waterlily' AND node_id = 1",
                [],
            )
            .unwrap();
        let read = read(&db);
        assert_eq!(read.errors.len(), 1);
        assert!(read.errors[0].to_string().contains("node 1"));
        let position = |item: &str| read.order.iter().position(|entry| entry == item).unwrap();
        assert!(
            position("level-waterlily") < position("error")
                && position("error") < position("power-almandine"),
            "the node error sits between its own session and the next one",
        );
        assert_eq!(
            read.messages["level-waterlily"].len(),
            9,
            "the prompt survives through its copies"
        );

        let marks = peeked(&db);
        assert_eq!(marks["level-waterlily"], SourceWatermark::Opaque);
        assert!(
            matches!(marks["branch-candy"], SourceWatermark::At(_)),
            "only the corrupt session re-reads"
        );
    }

    /// A helper agent's hidden session keeps its rows under a kind subpath, so
    /// a `devin`-scoped search sees only the sessions devin itself lists.
    #[test]
    fn hidden_helper_sessions_take_the_helper_kind() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(Path::new(MACOS).join(DB_FILE), &db).unwrap();
        Connection::open(&db)
            .unwrap()
            .execute(
                "UPDATE sessions SET hidden = 1 WHERE id = 'level-waterlily'",
                [],
            )
            .unwrap();
        let read = read(&db);
        assert_eq!(read.sessions["level-waterlily"].source_agent, HELPER_AGENT);
        assert_eq!(read.sessions["branch-candy"].source_agent, NAME);
    }

    #[test]
    fn restore_is_refused_with_the_alternative() {
        assert!(
            DevinFactory
                .restore_unsupported()
                .unwrap()
                .contains("--to claude-code")
        );
    }
}
