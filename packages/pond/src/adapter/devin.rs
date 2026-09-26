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
//! Subagents run inside the parent's forest. The parent-side record is the only
//! link: the `run_subagent` result (and the completion notice) carry
//! `subagent/agent_id` plus `subagent/chain_node_id`, the subagent's head node,
//! mirrored by the `subagent_heads` table. Each head's ancestor chain claims the
//! subagent's messages for a child session `<id>/agent-<agent_id>`; the main
//! chain claims first, and everything unclaimed (pre-compaction history, the
//! summarizer's chain) stays with the parent.
//!
//! The writer deletes nodes on `/revert` and whole sessions on `devin rm`, and
//! never updates a node in place, so pond keeps the superset it has seen.
//! Restore is refused ([`RESTORE_UNSUPPORTED`]).

use std::collections::{BTreeMap, HashMap, HashSet, hash_map::Entry};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
#[derive(Debug, Clone)]
pub struct DevinAdapter {
    root: PathBuf,
    /// Heads `discover` computed, taken by the `events_with` that follows it
    /// in the same sync, so the node-graph pass runs once per sync.
    heads: Arc<Mutex<Option<Heads>>>,
}

impl DevinAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            heads: Arc::default(),
        }
    }

    fn take_heads(&self) -> Option<Heads> {
        self.heads.lock().ok().and_then(|mut heads| heads.take())
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
        let cache = Arc::clone(&self.heads);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let heads = collect_heads(&db, true)?;
                let count = heads.pond_sessions();
                if let Ok(mut slot) = cache.lock() {
                    *slot = Some(heads);
                }
                Ok(count)
            })
            .await
            .map_err(join_error)?
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let db = self.db_path();
        Box::pin(stream! {
            let peek = !oracle.is_empty();
            let heads = match self.take_heads() {
                Some(heads) => heads,
                None => {
                    let heads_db = db.clone();
                    match tokio::task::spawn_blocking(move || collect_heads(&heads_db, peek)).await {
                        Ok(Ok(heads)) => heads,
                        Ok(Err(error)) => { yield Err(error); return; }
                        Err(join) => { yield Err(join_error(join)); return; }
                    }
                }
            };
            if let Some(reason) = heads.unsupported {
                yield Ok(AdapterYield::Skipped { session_id: None, project: None, reason: SkipReason::Unsupported(reason) });
                return;
            }

            let mut survivors = Vec::with_capacity(heads.sessions.len());
            let mut fresh = 0usize;
            for head in heads.sessions {
                match &head.watermarks {
                    Some(marks) if peek && marks.iter().all(|(id, mark)| source_in_sync(oracle, Some(id), *mark)) => {
                        fresh += marks.len();
                    }
                    _ => survivors.push(head.id),
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

    /// Always partitions, even on a first sync, so the session count is the
    /// same one `discover` and every later plan report.
    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> PlanFuture<'a> {
        let db = self.db_path();
        Box::pin(async move {
            let heads = tokio::task::spawn_blocking(move || collect_heads(&db, true))
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
                    .flat_map(|head| head.watermarks.iter().flatten())
                    .map(|(id, mark)| (Some(id.as_str()), *mark)),
            )))
        })
    }
}

// -- Opening and heads ------------------------------------------------------

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

/// One `sessions` row as the gate sees it: its id, and - when there is an
/// oracle to compare against - the watermark of every pond session it yields
/// (the root plus each subagent child). The group is read or skipped whole.
#[derive(Debug)]
struct SessionHead {
    id: String,
    watermarks: Option<Vec<(String, SourceWatermark)>>,
}

#[derive(Debug, Default)]
struct Heads {
    sessions: Vec<SessionHead>,
    unsupported: Option<String>,
}

impl Heads {
    /// Pond sessions the heads stand for; a session the peek could not
    /// partition counts once.
    fn pond_sessions(&self) -> usize {
        self.sessions
            .iter()
            .map(|head| head.watermarks.as_ref().map_or(1, Vec::len))
            .sum()
    }
}

fn collect_heads(db: &Path, peek: bool) -> Result<Heads, AdapterError> {
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
    let rows = session_rows(&conn, db)?;
    let mut marks = if peek {
        forest_watermarks(&conn, &rows)
    } else {
        None
    };
    let sessions = rows
        .into_iter()
        .map(|(id, _)| {
            let watermarks = peek.then(|| {
                marks
                    .as_mut()
                    .and_then(|marks| marks.remove(&id))
                    .unwrap_or_else(|| vec![(id.clone(), SourceWatermark::Opaque)])
            });
            SessionHead { id, watermarks }
        })
        .collect();
    Ok(Heads {
        sessions,
        unsupported: None,
    })
}

/// `(id, main_chain_id)` of every session, in id order.
fn session_rows(conn: &Connection, db: &Path) -> Result<Vec<(String, Option<i64>)>, AdapterError> {
    let mut stmt = conn
        .prepare("SELECT id, main_chain_id FROM sessions ORDER BY id")
        .map_err(|error| db_error(db, "prepare session list", &error))?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|error| db_error(db, "query session list", &error))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| db_error(db, "read session list", &error))
}

/// Every pond session's watermark, from one pass over the node graph that
/// pulls only the fields the partition needs, never message bodies to Rust,
/// building one session's forest at a time. A node whose `chat_message` is not
/// JSON with a string id leaves only its own session opaque, so that session
/// re-reads and the read reports the node; a failed pass leaves every session
/// opaque.
fn forest_watermarks(
    conn: &Connection,
    rows: &[(String, Option<i64>)],
) -> Option<HashMap<String, Vec<(String, SourceWatermark)>>> {
    let table_heads = subagent_heads(conn).ok()?;
    let main_heads: HashMap<&str, Option<i64>> =
        rows.iter().map(|(id, head)| (id.as_str(), *head)).collect();
    let mut marks: HashMap<String, Vec<(String, SourceWatermark)>> = HashMap::new();
    let mut finish = |session: String, nodes: &[NodeRef], poisoned: bool| {
        let Some(main_head) = main_heads.get(session.as_str()) else {
            return;
        };
        let group = if poisoned {
            vec![(session.clone(), SourceWatermark::Opaque)]
        } else {
            let heads = table_heads.get(&session).map_or(&[][..], Vec::as_slice);
            let forest = Forest::new(*main_head, nodes, heads);
            let mut group = vec![(session.clone(), forest.watermark(0))];
            for (index, agent) in forest.agents.iter().enumerate() {
                group.push((
                    child_id(&session, &agent.agent_id),
                    forest.watermark(index + 1),
                ));
            }
            group
        };
        marks.insert(session, group);
    };

    let mut stmt = conn
        .prepare(
            "SELECT session_id, row_id, node_id, parent_node_id, created_at,
                    CASE WHEN json_valid(chat_message)
                          AND json_type(chat_message, '$.message_id') = 'text' THEN
                         json_extract(chat_message, '$.message_id') END,
                    CASE WHEN json_valid(chat_message)
                          AND json_type(chat_message, '$.metadata.created_at') = 'text' THEN
                         json_extract(chat_message, '$.metadata.created_at') END,
                    CASE WHEN json_valid(chat_message)
                          AND json_type(chat_message, '$.metadata.extensions.\"subagent/agent_id\"') = 'text' THEN
                         json_extract(chat_message, '$.metadata.extensions.\"subagent/agent_id\"') END,
                    CASE WHEN json_valid(chat_message)
                          AND json_type(chat_message, '$.metadata.extensions.\"subagent/chain_node_id\"') = 'integer' THEN
                         json_extract(chat_message, '$.metadata.extensions.\"subagent/chain_node_id\"') END
             FROM message_nodes ORDER BY session_id, row_id",
        )
        .ok()?;
    let mut cursor = stmt.query([]).ok()?;
    let mut current: Option<(String, Vec<NodeRef>, bool)> = None;
    while let Some(row) = cursor.next().ok()? {
        let session: String = row.get(0).ok()?;
        if current.as_ref().is_some_and(|(id, _, _)| *id != session)
            && let Some((id, nodes, poisoned)) = current.take()
        {
            finish(id, &nodes, poisoned);
        }
        let (_, nodes, poisoned) = current.get_or_insert_with(|| (session, Vec::new(), false));
        let Some(message_id) = row.get::<_, Option<String>>(5).ok()? else {
            *poisoned = true;
            continue;
        };
        nodes.push(NodeRef {
            row_id: row.get(1).ok()?,
            node_id: row.get(2).ok()?,
            parent: row.get(3).ok()?,
            node_created: row.get(4).ok()?,
            message_id,
            created_at: row.get(6).ok()?,
            agent_id: row.get(7).ok()?,
            chain_node_id: row.get(8).ok()?,
        });
    }
    if let Some((id, nodes, poisoned)) = current.take() {
        finish(id, &nodes, poisoned);
    }
    // A sessions row with no nodes yet holds nothing to ingest.
    for (id, head) in rows {
        marks
            .entry(id.clone())
            .or_insert_with(|| vec![(id.clone(), Forest::new(*head, &[], &[]).watermark(0))]);
    }
    Some(marks)
}

/// `subagent_heads` rows as `(agent_id, chain_node_id)` per session; the table
/// arrived in V17, so its absence is an empty map.
fn subagent_heads(conn: &Connection) -> rusqlite::Result<HashMap<String, Vec<(String, i64)>>> {
    let mut heads: HashMap<String, Vec<(String, i64)>> = HashMap::new();
    if !sqlite::has_table(conn, "subagent_heads")? {
        return Ok(heads);
    }
    let mut stmt = conn.prepare(
        "SELECT session_id, agent_id, chain_node_id FROM subagent_heads ORDER BY session_id, agent_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    for row in rows {
        let (session, agent, chain) = row?;
        heads.entry(session).or_default().push((agent, chain));
    }
    Ok(heads)
}

// -- The forest -------------------------------------------------------------

/// What the partition needs from one `message_nodes` row. Built from a parsed
/// row on the read path and from SQL-extracted columns on the peek path; the
/// `watermark_matches_the_read` test pins that the two agree.
#[derive(Debug, Clone)]
struct NodeRef {
    row_id: i64,
    node_id: i64,
    parent: Option<i64>,
    node_created: i64,
    message_id: String,
    created_at: Option<String>,
    agent_id: Option<String>,
    chain_node_id: Option<i64>,
}

impl NodeRef {
    fn from_row(node: &NodeRow, message: &Value) -> Option<Self> {
        let extensions = message.pointer("/metadata/extensions");
        let extension = |key: &str| extensions.and_then(|ext| ext.get(key));
        Some(Self {
            row_id: node.row_id,
            node_id: node.node_id,
            parent: node.parent_node_id,
            node_created: node.created_at,
            message_id: message.get("message_id")?.as_str()?.to_owned(),
            created_at: message
                .pointer("/metadata/created_at")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            agent_id: extension("subagent/agent_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            chain_node_id: extension("subagent/chain_node_id").and_then(Value::as_i64),
        })
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

/// One subagent the partition found messages for.
#[derive(Debug, Clone)]
struct AgentChain {
    agent_id: String,
    head: i64,
    /// `message_id` of the first parent-side message naming the agent (the
    /// `run_subagent` result), the child's `parent_message_id`.
    link_message: Option<String>,
}

/// Which pond session owns each message: owner 0 is the root session, owner
/// `i` is `agents[i - 1]`.
struct Forest {
    owner: HashMap<String, usize>,
    agents: Vec<AgentChain>,
    /// Agent ids that cannot name a child session; their messages stay with
    /// the parent.
    rejected: Vec<String>,
    /// Newest timestamp per owner, `None` when the owner holds no message.
    newest: Vec<Option<i64>>,
}

impl Forest {
    fn new(main_head: Option<i64>, nodes: &[NodeRef], table_heads: &[(String, i64)]) -> Self {
        let by_node: HashMap<i64, &NodeRef> =
            nodes.iter().map(|node| (node.node_id, node)).collect();

        // Agent heads: the writer's own `subagent_heads` record wins; otherwise
        // the newest parent-side link row (a resumed subagent advances its head).
        let mut heads: BTreeMap<String, i64> = BTreeMap::new();
        let mut link_messages: HashMap<String, String> = HashMap::new();
        for node in nodes {
            let Some(agent) = &node.agent_id else {
                continue;
            };
            link_messages
                .entry(agent.clone())
                .or_insert_with(|| node.message_id.clone());
            if let Some(chain) = node.chain_node_id {
                heads.insert(agent.clone(), chain);
            }
        }
        for (agent, chain) in table_heads {
            heads.insert(agent.clone(), *chain);
        }

        let mut owner: HashMap<String, usize> = HashMap::new();
        for id in chain_messages(&by_node, main_head) {
            owner.entry(id).or_insert(0);
        }
        let mut agents = Vec::new();
        let mut rejected = Vec::new();
        for (agent_id, head) in heads {
            // The id becomes a child session id; one that cannot be leaves
            // its messages with the parent rather than minting a malformed id.
            if validate_path_id(NAME, "subagent id", &agent_id, agent_id.clone()).is_err() {
                rejected.push(agent_id);
                continue;
            }
            let index = agents.len() + 1;
            let mut claimed = false;
            for id in chain_messages(&by_node, Some(head)) {
                if let Entry::Vacant(entry) = owner.entry(id) {
                    entry.insert(index);
                    claimed = true;
                }
            }
            // A fork copies the parent's `run_subagent` result, link and all,
            // but not the subagent's nodes: no messages, no child.
            if claimed {
                agents.push(AgentChain {
                    link_message: link_messages.get(&agent_id).cloned(),
                    agent_id,
                    head,
                });
            }
        }
        // A message's time is its first placement's, the one the read stamps;
        // a later copy's node second must not move the watermark past it.
        let mut newest = vec![None; agents.len() + 1];
        let mut seen = HashSet::new();
        for node in nodes {
            if !seen.insert(node.message_id.as_str()) {
                continue;
            }
            let slot = owner.get(&node.message_id).copied().unwrap_or(0);
            let micros = node.micros();
            newest[slot] = Some(newest[slot].map_or(micros, |prev: i64| prev.max(micros)));
        }
        Self {
            owner,
            agents,
            rejected,
            newest,
        }
    }

    fn owner_of(&self, message_id: &str) -> usize {
        self.owner.get(message_id).copied().unwrap_or(0)
    }

    fn watermark(&self, owner: usize) -> SourceWatermark {
        match self.newest.get(owner).copied().flatten() {
            Some(micros) => SourceWatermark::At(micros),
            None => SourceWatermark::Empty,
        }
    }
}

/// `message_id`s on the chain ending at `head`, walking `parent_node_id` to
/// the root. A dangling parent ends the walk; a revisited node (a cycle the
/// writer never produces) ends it too, so a corrupt forest cannot hang a sync.
fn chain_messages(by_node: &HashMap<i64, &NodeRef>, head: Option<i64>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut cursor = head;
    while let Some(node_id) = cursor {
        let Some(node) = by_node.get(&node_id) else {
            break;
        };
        if !seen.insert(node_id) {
            break;
        }
        out.push(node.message_id.clone());
        cursor = node.parent;
    }
    out
}

fn child_id(session_id: &str, agent_id: &str) -> String {
    format!("{session_id}/agent-{agent_id}")
}

// -- Reading -------------------------------------------------------------------

/// One `message_nodes` row as read. `message_id` comes from SQLite, so a copy
/// identical to one already parsed is never parsed again.
struct NodeRow {
    row_id: i64,
    node_id: i64,
    parent_node_id: Option<i64>,
    created_at: i64,
    metadata: Option<String>,
    chat_message: String,
    message_id: Option<String>,
}

/// One message: its newest `chat_message` (a later copy can only add fields -
/// the ACP tool content lands after the fact), every older distinct variant,
/// and every node placing it.
struct Collected {
    newest: Value,
    newest_raw: String,
    newest_ref: NodeRef,
    first: NodeRef,
    variants: Vec<Value>,
    nodes: Vec<Value>,
}

type Tx = mpsc::Sender<Result<AdapterYield, AdapterError>>;

fn read_sessions(db: &Path, ids: &[String], tx: &Tx) {
    let conn = match open_forest(db) {
        Ok(Opened::Forest(conn)) => conn,
        Ok(Opened::Missing | Opened::Unsupported(_)) => {
            let error = AdapterError::schema(
                NAME,
                db.display().to_string(),
                "the session database changed shape between listing and read; re-run `pond sync devin`",
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
    let heads = match subagent_heads(&conn) {
        Ok(heads) => heads,
        Err(error) => {
            let _ = tx.blocking_send(Err(db_error(db, "read subagent_heads", &error)));
            return;
        }
    };
    for id in ids {
        let heads = heads.get(id).map_or(&[][..], Vec::as_slice);
        if !read_session(&conn, db, id, schema_version, heads, tx) {
            return;
        }
    }
}

/// One `sessions` row with everything it yields: the root session, its
/// messages, then each subagent child and its messages.
fn read_session(
    conn: &Connection,
    db: &Path,
    id: &str,
    schema_version: Option<i64>,
    heads: &[(String, i64)],
    tx: &Tx,
) -> bool {
    let location = format!("{}#{id}", db.display());
    // A root id that fails here would never reach a restore filename or the
    // default search: `/` is the subagent marker there, `:` an NTFS stream.
    if let Err(error) = validate_path_id(NAME, "session id", id, &location) {
        return tx.blocking_send(Err(error)).is_ok();
    }
    // One read transaction, so the session row, its nodes and its tool state
    // come from the same snapshot while devin keeps writing.
    let snapshot = match conn.unchecked_transaction() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return tx
                .blocking_send(Err(db_error(db, "begin read", &error)))
                .is_ok();
        }
    };
    let mut read = match SessionRead::load(&snapshot, db, id, &location) {
        Ok(read) => read,
        Err(error) => return tx.blocking_send(Err(error)).is_ok(),
    };
    drop(snapshot);

    let root = read.root_session(id, schema_version, heads);
    emit!(tx, Ok(AdapterYield::Event(IngestEvent::Session(root))));
    // Node and subagent-id errors wait for the root, so the ingest charges
    // them to this session rather than to whichever one was read before it.
    for error in std::mem::take(&mut read.errors) {
        emit!(tx, Err(error));
    }
    let forest = Forest::new(read.main_head, &read.refs, heads);
    for agent in &forest.rejected {
        emit!(
            tx,
            Err(AdapterError::schema(
                NAME,
                location.clone(),
                format!(
                    "subagent id {agent:?} cannot name a child session; its messages stay with the parent"
                ),
            ))
        );
    }
    let tools = ToolIndex::new(&read.tool_states, read.collected.values());

    let mut owned: Vec<Vec<&Collected>> = vec![Vec::new(); forest.agents.len() + 1];
    for (message_id, entry) in &read.collected {
        owned[forest.owner_of(message_id)].push(entry);
    }
    for list in &mut owned {
        list.sort_by(|left, right| {
            left.first
                .micros()
                .cmp(&right.first.micros())
                .then_with(|| left.first.message_id.cmp(&right.first.message_id))
        });
    }
    for entry in &owned[0] {
        if !emit_message(tx, id, entry, &tools) {
            return false;
        }
    }
    for (index, agent) in forest.agents.iter().enumerate() {
        let messages = &owned[index + 1];
        let child = child_id(id, &agent.agent_id);
        let Some(created_at) = messages
            .iter()
            .find_map(|entry| DateTime::from_timestamp_micros(entry.first.micros()))
        else {
            emit!(
                tx,
                Err(AdapterError::schema(
                    NAME,
                    child,
                    "no message of the subagent carries a usable timestamp"
                ))
            );
            continue;
        };
        let session = child_session(id, &child, agent, created_at, &read, &forest);
        emit!(tx, Ok(AdapterYield::Event(IngestEvent::Session(session))));
        for entry in messages {
            if !emit_message(tx, &child, entry, &tools) {
                return false;
            }
        }
    }
    true
}

/// Everything read for one `sessions` row inside its snapshot.
struct SessionRead {
    row: Value,
    created_at: DateTime<chrono::Utc>,
    project: Extracted<String>,
    main_head: Option<i64>,
    refs: Vec<NodeRef>,
    collected: BTreeMap<String, Collected>,
    tool_states: Vec<Value>,
    errors: Vec<AdapterError>,
}

impl SessionRead {
    fn load(conn: &Connection, db: &Path, id: &str, location: &str) -> Result<Self, AdapterError> {
        let row = dynamic_rows(conn, "SELECT * FROM sessions WHERE id = ?1", id)
            .map_err(|error| db_error(db, "read session row", &error))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                AdapterError::schema(
                    NAME,
                    location,
                    "session row vanished between listing and read",
                )
            })?;
        let created_at = row
            .get("created_at")
            .and_then(Value::as_i64)
            .and_then(|secs| DateTime::from_timestamp(secs, 0))
            .ok_or_else(|| {
                AdapterError::schema(NAME, location, "session has no integer created_at")
            })?;
        let project = extract_str(&row, "working_directory")
            .filter(|dir| !dir.is_empty())
            .ok_or_else(|| {
                AdapterError::schema(NAME, location, "session has no working_directory")
            })?;
        let nodes =
            node_rows(conn, id).map_err(|error| db_error(db, "read message_nodes", &error))?;
        // `tool_call_state` arrived in V14; a forest database before it has none.
        let tool_states = if sqlite::has_table(conn, "tool_call_state")
            .map_err(|error| db_error(db, "probe tool_call_state", &error))?
        {
            dynamic_rows(
                conn,
                "SELECT * FROM tool_call_state WHERE session_id = ?1 ORDER BY rowid",
                id,
            )
            .map_err(|error| db_error(db, "read tool_call_state", &error))?
        } else {
            Vec::new()
        };
        let main_head = row.get("main_chain_id").and_then(Value::as_i64);
        let mut read = Self {
            row,
            created_at,
            project,
            main_head,
            refs: Vec::with_capacity(nodes.len()),
            collected: BTreeMap::new(),
            tool_states,
            errors: Vec::new(),
        };
        for node in &nodes {
            read.collect(node, location);
        }
        Ok(read)
    }

    /// Fold one node in: an identical copy of the newest known form only adds
    /// a placement; anything else is parsed and becomes the newest form.
    fn collect(&mut self, node: &NodeRow, location: &str) {
        let known = node
            .message_id
            .as_ref()
            .and_then(|message_id| self.collected.get_mut(message_id))
            .filter(|entry| entry.newest_raw == node.chat_message);
        if let Some(entry) = known {
            entry.nodes.push(placement(node));
            self.refs.push(NodeRef {
                row_id: node.row_id,
                node_id: node.node_id,
                parent: node.parent_node_id,
                node_created: node.created_at,
                ..entry.newest_ref.clone()
            });
            return;
        }
        let parsed = serde_json::from_str::<Value>(&node.chat_message)
            .ok()
            .and_then(|message| {
                NodeRef::from_row(node, &message).map(|node_ref| (message, node_ref))
            });
        let Some((message, node_ref)) = parsed else {
            self.errors.push(AdapterError::schema(
                NAME,
                format!("{location}/node {}", node.node_id),
                "chat_message is not a JSON object with a string message_id",
            ));
            return;
        };
        match self.collected.get_mut(&node_ref.message_id) {
            Some(entry) => {
                let older = std::mem::replace(&mut entry.newest, message);
                if !entry.variants.contains(&older) {
                    entry.variants.push(older);
                }
                entry.newest_raw.clone_from(&node.chat_message);
                entry.newest_ref = node_ref.clone();
                entry.nodes.push(placement(node));
            }
            None => {
                self.collected.insert(
                    node_ref.message_id.clone(),
                    Collected {
                        newest: message,
                        newest_raw: node.chat_message.clone(),
                        newest_ref: node_ref.clone(),
                        first: node_ref.clone(),
                        variants: Vec::new(),
                        nodes: vec![placement(node)],
                    },
                );
            }
        }
        self.refs.push(node_ref);
    }

    fn root_session(
        &self,
        id: &str,
        schema_version: Option<i64>,
        heads: &[(String, i64)],
    ) -> Session {
        let mut devin = Map::new();
        if let Some(version) = schema_version {
            devin.insert("schema_version".to_owned(), json!(version));
        }
        if !self.tool_states.is_empty() {
            devin.insert(
                "tool_call_state".to_owned(),
                Value::Array(self.tool_states.clone()),
            );
        }
        if !heads.is_empty() {
            let rows = heads
                .iter()
                .map(|(agent, chain)| json!({ "agent_id": agent, "chain_node_id": chain }))
                .collect();
            devin.insert("subagent_heads".to_owned(), Value::Array(rows));
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
}

/// A subagent child. Its parent is the session that owns the message naming
/// it - the root, or another subagent when one spawned it - and its raw record
/// is that message's link extensions, or the `subagent_heads` row when no
/// link message survives.
fn child_session(
    root: &str,
    child: &str,
    agent: &AgentChain,
    created_at: DateTime<chrono::Utc>,
    read: &SessionRead,
    forest: &Forest,
) -> Session {
    let link = agent.link_message.as_deref().and_then(|message_id| {
        read.collected
            .get(message_id)
            .map(|entry| (message_id, entry))
    });
    let parent = match link.map(|(message_id, _)| forest.owner_of(message_id)) {
        Some(owner) if owner > 0 => child_id(root, &forest.agents[owner - 1].agent_id),
        _ => root.to_owned(),
    };
    let raw = link
        .and_then(|(_, entry)| entry.newest.pointer("/metadata/extensions").cloned())
        .unwrap_or_else(|| json!({ "session_id": root, "agent_id": agent.agent_id, "chain_node_id": agent.head }));
    let mut options = source_options(NAME, &raw);
    options.insert(
        NAME.to_owned(),
        json!({ "agent_id": agent.agent_id, "chain_node_id": agent.head }),
    );
    Session {
        id: child.to_owned(),
        parent_message_id: link.map(|(message_id, _)| format!("{parent}:{message_id}")),
        parent_session_id: Some(parent),
        source_agent: SUBAGENT_AGENT.to_owned(),
        created_at,
        project: read.project.clone(),
        options,
    }
}

fn emit_message(tx: &Tx, session_id: &str, entry: &Collected, tools: &ToolIndex) -> bool {
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

fn node_rows(conn: &Connection, session_id: &str) -> rusqlite::Result<Vec<NodeRow>> {
    let mut stmt = conn.prepare_cached(
        "SELECT row_id, node_id, parent_node_id, created_at, metadata, chat_message,
                CASE WHEN json_valid(chat_message)
                      AND json_type(chat_message, '$.message_id') = 'text'
                     THEN json_extract(chat_message, '$.message_id') END
         FROM message_nodes WHERE session_id = ?1 ORDER BY row_id",
    )?;
    let rows = stmt.query_map([session_id], |row| {
        Ok(NodeRow {
            row_id: row.get(0)?,
            node_id: row.get(1)?,
            parent_node_id: row.get(2)?,
            created_at: row.get(3)?,
            metadata: row.get(4)?,
            chat_message: row.get(5)?,
            message_id: row.get(6)?,
        })
    })?;
    rows.collect()
}

/// A node's own columns, `metadata` parsed when it is JSON: where the message
/// sits in the forest and why (system prefix, compaction source).
fn placement(node: &NodeRow) -> Value {
    let mut placement = json!({
        "row_id": node.row_id,
        "node_id": node.node_id,
        "created_at": node.created_at,
    });
    if let Some(parent) = node.parent_node_id {
        placement["parent_node_id"] = json!(parent);
    }
    if let Some(metadata) = &node.metadata {
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
        let ids: Vec<String> = session_rows(&conn, db)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
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

    /// The main chain claims first, each subagent head chain claims its own,
    /// and everything off-chain stays with the parent; a fork that copied a
    /// subagent link without its nodes yields no child.
    #[test]
    fn subagents_split_into_children_and_forks_yield_none() {
        let read = macos();
        let count = |id: &str| read.messages[id].len();
        assert_eq!(count("amplified-color"), 39);
        assert_eq!(count("amplified-color/agent-398e395d"), 10);
        assert_eq!(count("chalk-twig"), 27);
        assert_eq!(count("chalk-twig/agent-029b10e6"), 7);
        assert_eq!(count("chalk-twig/agent-1d6d342f"), 6);
        assert_eq!(count("power-almandine"), 15);
        assert_eq!(read.sessions.len(), 8);
        assert!(
            !read
                .sessions
                .keys()
                .any(|id| id.starts_with("power-almandine/"))
        );

        let child = &read.sessions["chalk-twig/agent-029b10e6"];
        assert_eq!(child.source_agent, SUBAGENT_AGENT);
        assert_eq!(child.parent_session_id.as_deref(), Some("chalk-twig"));
        let link = child.parent_message_id.as_deref().unwrap();
        let carrier = read.messages["chalk-twig"]
            .iter()
            .find(|message| message.id() == link)
            .unwrap();
        assert_eq!(
            carrier.role(),
            Role::Tool,
            "the run_subagent result links the child"
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
    }

    fn assert_peek_matches_read(db: &Path) {
        let read = read(db);
        let conn = sqlite::open_db(NAME, db).unwrap();
        let rows = session_rows(&conn, db).unwrap();
        let peeked: BTreeMap<String, SourceWatermark> = forest_watermarks(&conn, &rows)
            .unwrap()
            .into_values()
            .flatten()
            .collect();
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
            "UPDATE message_nodes SET chat_message = json_remove(chat_message, '$.metadata.created_at'),
                    created_at = created_at + row_id
             WHERE session_id = 'amplified-color'",
            [],
        )
        .unwrap();
        drop(conn);
        assert_peek_matches_read(&db);
    }

    /// The writer's own `subagent_heads` record outranks the link rows, and a
    /// chain walk stops at a cycle instead of hanging.
    #[test]
    fn subagent_heads_table_wins_and_cycles_end_the_walk() {
        let node = |row_id: i64, node_id: i64, parent: Option<i64>, message: &str| NodeRef {
            row_id,
            node_id,
            parent,
            node_created: 1,
            message_id: message.to_owned(),
            created_at: None,
            agent_id: None,
            chain_node_id: None,
        };
        let mut link = node(3, 3, Some(1), "result");
        link.agent_id = Some("a1".to_owned());
        link.chain_node_id = Some(10);
        let nodes = vec![
            node(1, 1, None, "prompt"),
            node(2, 10, None, "stale-sub"),
            link,
            node(4, 20, Some(21), "sub-new"),
            node(5, 21, Some(20), "sub-root"),
        ];
        let forest = Forest::new(Some(3), &nodes, &[("a1".to_owned(), 20)]);
        assert_eq!(forest.agents.len(), 1);
        assert_eq!(forest.agents[0].head, 20);
        assert_eq!(forest.owner_of("sub-new"), 1);
        assert_eq!(forest.owner_of("sub-root"), 1);
        assert_eq!(
            forest.owner_of("stale-sub"),
            0,
            "unclaimed history stays with the parent"
        );
        assert_eq!(forest.agents[0].link_message.as_deref(), Some("result"));
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
        let heads = collect_heads(&db, false).unwrap();
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
                "UPDATE message_nodes SET chat_message = '{not json' WHERE session_id = 'level-waterlily' AND node_id = 1",
                [],
            )
            .unwrap();
        let read = read(&db);
        assert_eq!(read.errors.len(), 1);
        assert!(read.errors[0].to_string().contains("node 1"));
        let position = |item: &str| read.order.iter().position(|entry| entry == item).unwrap();
        assert!(
            position("level-waterlily") < position("error"),
            "the node error follows its own session, so the ingest charges it there",
        );
        assert_eq!(
            read.messages["level-waterlily"].len(),
            9,
            "the prompt survives through its copies"
        );

        let conn = sqlite::open_db(NAME, &db).unwrap();
        let rows = session_rows(&conn, &db).unwrap();
        let marks = forest_watermarks(&conn, &rows).unwrap();
        assert_eq!(
            marks["level-waterlily"],
            vec![("level-waterlily".to_owned(), SourceWatermark::Opaque)]
        );
        assert!(
            matches!(marks["branch-candy"][0].1, SourceWatermark::At(_)),
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
