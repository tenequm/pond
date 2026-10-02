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
//! its trees, and the parent holds the rest - the link included. The Local
//! Fusion sidekick is a persistent subagent: every handoff extends one live
//! tree (marked by its `subagent/handoff` briefs), so it is one child across
//! all its handoffs.
//!
//! The writer deletes nodes on `/revert` and whole sessions on `devin rm`, and
//! re-saves nodes (same content, new `row_id`) rather than editing them, so
//! pond keeps the superset it has seen. Restore is refused
//! ([`RESTORE_UNSUPPORTED`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, hash_map::Entry};
use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::DateTime;
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use crate::{
    sessions::{IngestEvent, SessionWithMessages},
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterErrorKind, AdapterFactory, AdapterYield, AdapterYieldStream,
    DiscoverFuture, EdgeFidelity, Env, LineageFidelity, PlanFuture, RestoreFidelity, RestoredFile,
    SkipOracle, SkipReason, SourceWatermark, SyncPlan, compact_json,
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
/// The telemetry operation of the persistent Local Fusion sidekick's system
/// prefix. devin keys a persistent subagent by a deterministic agent id (the
/// V17 `subagent_heads` comment), so every tree this prefix roots in one
/// session is the same agent across all its handoffs.
const PERSISTENT_PREFIX: &str = "subagent_sidekick";

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

    // Subagents are branded `devin/subagent`; forks record no parent.
    fn lineage_fidelity(&self) -> LineageFidelity {
        LineageFidelity {
            spawns: EdgeFidelity::Complete,
            continuations: EdgeFidelity::None,
            spawn_brand_exact: true,
        }
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
}

impl DevinAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
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
            Ok(heads.pond_sessions())
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let db = self.db_path();
        Box::pin(stream! {
            let heads_db = db.clone();
            let peek = tokio::task::spawn_blocking(move || collect_heads(&heads_db));
            let heads = match peek.await {
                Ok(Ok(heads)) => heads,
                Ok(Err(error)) => { yield Err(error); return; }
                Err(join) => { yield Err(join_error(join)); return; }
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
/// derived here in SQL (the string `message_id` and `metadata.created_at`,
/// and the flags `role`, the telemetry `operation` and the `subagent/handoff`
/// extension decide), so the peek reads no message body; the peek and the
/// read both go through here, so the two cannot disagree. Malformed JSON
/// yields NULLs, never a failed statement.
fn node_query(extra: &str) -> String {
    format!(
        "SELECT row_id, node_id, parent_node_id, created_at,
             iif(json_type(doc, '$.message_id') = 'text', doc ->> '$.message_id', NULL),
             iif(json_type(doc, '$.metadata.created_at') = 'text',
                 doc ->> '$.metadata.created_at', NULL),
             ifnull(doc ->> '$.role' = 'system', 0),
             ifnull(json_type(doc, '$.metadata.extensions.\"subagent/handoff\"') = 'true', 0),
             parent_node_id IS NULL AND ifnull(doc ->> '$.role' = 'user'
                 OR doc ->> '$.metadata.telemetry.operation' GLOB 'subagent_*', 0),
             parent_node_id IS NULL AND ifnull(
                 doc ->> '$.metadata.telemetry.operation' = '{PERSISTENT_PREFIX}', 0){extra}
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
    /// Carries `subagent/handoff`: a copy of a persistent subagent's brief.
    is_handoff: bool,
    /// A root the harness writes only for a subagent: its system prefix
    /// (telemetry operation `subagent_<profile>`), or the parentless copy of
    /// its task prompt.
    starts_subagent: bool,
    /// A system prefix root of the persistent sidekick ([`PERSISTENT_PREFIX`]).
    starts_persistent: bool,
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
            is_handoff: row.get(7)?,
            starts_subagent: row.get(8)?,
            starts_persistent: row.get(9)?,
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
/// alone; one listed with no owner is held back until a sync can name its
/// subagent.
#[derive(Default)]
struct Forest {
    owners: HashMap<String, Vec<usize>>,
    children: Vec<String>,
    rejected: Vec<String>,
    /// Every node in a subagent tree, mapped to its group, for checking
    /// report-time links; never read for ownership.
    grouped_nodes: HashMap<i64, i64>,
    /// How many messages are held back, and the roots of the unnamed trees
    /// placing them.
    held: usize,
    held_trees: Vec<i64>,
    /// Newest timestamp per owner, `None` when the owner holds no message.
    newest: Vec<Option<i64>>,
}

impl Forest {
    fn new(nodes: &[NodeRef]) -> Self {
        let mut forest = if nodes
            .iter()
            .any(|node| node.starts_subagent || node.is_handoff)
        {
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
        // Held-back messages are stored nowhere yet, so no stored watermark
        // can cover them: the root stays pending until a sync names them.
        if owner == 0 && self.held > 0 {
            return SourceWatermark::Opaque;
        }
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
    // The sidekick's live tree is rooted by a prefix no different from the
    // lead's (telemetry `unknown`); only the handoff brief it holds marks it.
    let handoff_trees: HashSet<i64> = nodes
        .iter()
        .filter(|node| node.is_handoff)
        .map(|node| tree_of[&node.node_id])
        .collect();
    let seeded_trees: HashSet<i64> = tree_of
        .values()
        .copied()
        .filter(|tree| handoff_trees.contains(tree) || by_node[tree].starts_subagent)
        .collect();
    let seeded = |node: &NodeRef| seeded_trees.contains(&tree_of[&node.node_id]);
    let task_prompt = |node: &NodeRef| {
        node.is_handoff
            || (!node.is_system
                && node.parent.is_none_or(|parent| {
                    by_node
                        .get(&parent)
                        .is_some_and(|up| up.is_system && up.starts_subagent)
                }))
    };

    // A main message two subagents both copied must not merge them, unless
    // both open with it: a task prompt names exactly one subagent.
    let in_main: HashSet<&str> = nodes
        .iter()
        .filter(|node| !seeded(node))
        .map(|node| node.message_id.as_str())
        .collect();
    let mut sets: HashMap<i64, i64> = HashMap::new();
    let mut first_tree: HashMap<&str, i64> = HashMap::new();
    for node in nodes.iter().filter(|node| seeded(node)) {
        if !node.is_system && (!in_main.contains(node.message_id.as_str()) || task_prompt(node)) {
            let tree = tree_of[&node.node_id];
            match first_tree.entry(node.message_id.as_str()) {
                Entry::Occupied(entry) => union(&mut sets, *entry.get(), tree),
                Entry::Vacant(entry) => {
                    entry.insert(tree);
                }
            }
        }
    }
    // One agent across every handoff, even a tree that shares no message
    // with the others yet: a fresh chain devin starts when its head is gone,
    // or a brief written before its prefix copy.
    let mut persistent = seeded_trees
        .iter()
        .filter(|&tree| handoff_trees.contains(tree) || by_node[tree].starts_persistent);
    if let Some(&first) = persistent.next() {
        for &tree in persistent {
            union(&mut sets, first, tree);
        }
    }

    // Per message: whether a tree outside the subagents places it, and the
    // subagent groups whose trees do.
    let mut placed: HashMap<&str, (bool, Vec<i64>)> = HashMap::new();
    // The task prompt is the subagent's first message, so the earliest
    // candidate wins and one arriving later (a later brief) cannot rename it.
    let mut names: HashMap<i64, (i64, &str)> = HashMap::new();
    let mut grouped_nodes = HashMap::new();
    for node in nodes {
        let (in_root, groups) = placed.entry(&node.message_id).or_default();
        if !seeded(node) {
            *in_root = true;
            continue;
        }
        let group = find(&sets, tree_of[&node.node_id]);
        grouped_nodes.insert(node.node_id, group);
        if !groups.contains(&group) {
            groups.push(group);
        }
        if task_prompt(node) {
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
    let mut held = 0;
    let mut held_groups = HashSet::new();
    for (message, (in_root, groups)) in placed {
        if groups.is_empty() {
            continue;
        }
        let mut owned: Vec<usize> = groups
            .iter()
            .filter_map(|group| match index.get(group) {
                Some(&owner) => Some(owner),
                // Rejected: back with the parent. Unnamed: nobody, yet.
                None => names.contains_key(group).then_some(0),
            })
            .collect();
        if in_root {
            owned.push(0);
        }
        owned.sort_unstable();
        owned.dedup();
        if owned.is_empty() {
            held += 1;
            held_groups.extend(groups);
        }
        owners.insert(message.to_owned(), owned);
    }
    let mut held_trees: Vec<i64> = seeded_trees
        .into_iter()
        .filter(|&tree| held_groups.contains(&find(&sets, tree)))
        .collect();
    held_trees.sort_unstable();
    let children = valid.into_iter().map(|(name, _)| name.to_owned()).collect();
    Forest {
        owners,
        children,
        rejected,
        grouped_nodes,
        held,
        held_trees,
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

/// One message: the `chat_message` its highest `node_id` placement holds, every
/// other distinct variant, and every node placing it.
struct Collected {
    newest: Value,
    newest_row_id: i64,
    first: NodeRef,
    variants: Vec<Value>,
    nodes: Vec<Value>,
    tool_states: Vec<Value>,
}

/// One message's copies as its nodes stream in, before the newest is known.
struct Copies {
    first: NodeRef,
    bodies: Vec<Body>,
    placements: Vec<Placed>,
    /// The placement with the highest `node_id`.
    top: usize,
}

/// One distinct `chat_message` value among a message's copies.
struct Body {
    /// The text an identical copy matches without parsing.
    raw: String,
    value: Value,
    lowest_node: i64,
}

struct Placed {
    node_id: i64,
    row_id: i64,
    body: usize,
    placement: Value,
}

impl Copies {
    fn new(first: NodeRef) -> Self {
        Self {
            first,
            bodies: Vec::new(),
            placements: Vec::new(),
            top: 0,
        }
    }

    /// Adds `node`'s copy, holding `raw`; `None` when `raw` is not JSON.
    fn place(&mut self, node: &NodeRef, raw: String, placement: Value) -> Option<()> {
        let body = self.body_of(raw, node.node_id)?;
        let lowest = &mut self.bodies[body].lowest_node;
        *lowest = (*lowest).min(node.node_id);
        if self
            .placements
            .get(self.top)
            .is_none_or(|top| node.node_id > top.node_id)
        {
            self.top = self.placements.len();
        }
        self.placements.push(Placed {
            node_id: node.node_id,
            row_id: node.row_id,
            body,
            placement,
        });
        Some(())
    }

    /// The body `raw` holds, added when new. A copy can differ only in key
    /// order and still be the same value, never a variant; its text then
    /// replaces the stored one, so the next such copy matches without parsing.
    fn body_of(&mut self, raw: String, node_id: i64) -> Option<usize> {
        if let Some(known) = self.bodies.iter().position(|body| body.raw == raw) {
            return Some(known);
        }
        let value: Value = serde_json::from_str(&raw).ok()?;
        if let Some(known) = self.bodies.iter().position(|body| body.value == value) {
            self.bodies[known].raw = raw;
            return Some(known);
        }
        self.bodies.push(Body {
            raw,
            value,
            lowest_node: node_id,
        });
        Some(self.bodies.len() - 1)
    }

    /// The highest `node_id` copy is the newest: node ids follow the writer's
    /// allocation and survive a re-save, which renumbers `row_id`. Every
    /// other body is a variant, ordered by the first node holding it; with
    /// more than one body, each placement names its body as `variant` (0 for
    /// the newest, `n` for `variants[n - 1]`).
    fn finish(mut self) -> Collected {
        let top = &self.placements[self.top];
        let (newest_body, newest_row_id) = (top.body, top.row_id);
        let mut others: Vec<usize> = (0..self.bodies.len())
            .filter(|&body| body != newest_body)
            .collect();
        others.sort_by_key(|&body| self.bodies[body].lowest_node);
        let mut tags = vec![0; self.bodies.len()];
        for (position, &body) in others.iter().enumerate() {
            tags[body] = position + 1;
        }
        let tagged = self.bodies.len() > 1;
        let nodes = self
            .placements
            .into_iter()
            .map(|placed| {
                let mut placement = placed.placement;
                if tagged {
                    placement["variant"] = json!(tags[placed.body]);
                }
                placement
            })
            .collect();
        let mut take = |body: usize| std::mem::take(&mut self.bodies[body].value);
        let variants = others.iter().map(|&body| take(body)).collect();
        Collected {
            newest: take(newest_body),
            newest_row_id,
            first: self.first,
            variants,
            nodes,
            tool_states: Vec::new(),
        }
    }
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
/// root carries this session's id so ingest cannot charge it to the prior one.
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
        Err(error) if matches!(&error.kind, AdapterErrorKind::Io(_)) => {
            return tx
                .blocking_send(Ok(AdapterYield::Failed {
                    session_id: id.to_owned(),
                    error,
                }))
                .is_ok();
        }
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
    for error in read.link_drift(&forest, &location) {
        emit!(tx, Err(error));
    }
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
    if forest.held > 0 {
        let error = AdapterError::schema(
            NAME,
            location.clone(),
            format!(
                "{} messages of the subagent trees rooted at nodes {:?} have no task \
                 prompt to name their child session yet, so they are held back and \
                 this session re-reads on every sync; run `pond sync` again once the \
                 subagent has started; if this persists, devin aborted the spawn, \
                 and only these messages stay unstored (nothing else to do)",
                forest.held, forest.held_trees
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
    unmatched_tool_states: Vec<Value>,
    agent_heads: Vec<Value>,
    errors: Vec<AdapterError>,
}

impl SessionRead {
    fn link_drift(&self, forest: &Forest, location: &str) -> Vec<AdapterError> {
        let mut links = BTreeSet::new();
        for entry in self.collected.values() {
            for message in std::iter::once(&entry.newest).chain(&entry.variants) {
                let head = message
                    .pointer("/metadata/extensions/subagent~1chain_node_id")
                    .and_then(Value::as_i64);
                if let Some(head) = head {
                    let agent = message
                        .pointer("/metadata/extensions/subagent~1agent_id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    links.insert((agent, head));
                }
            }
        }
        for row in &self.agent_heads {
            if let Some(head) = row.get("chain_node_id").and_then(Value::as_i64) {
                let agent = row
                    .get("agent_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                links.insert((agent, head));
            }
        }
        let present: HashSet<i64> = self.refs.iter().map(|node| node.node_id).collect();
        let mut reasons = Vec::new();
        // A persistent subagent links once per handoff and moves its head
        // each time; every head must still land in the one group it owns.
        let mut groups: BTreeMap<String, BTreeMap<i64, i64>> = BTreeMap::new();
        for (agent, head) in links {
            let Some(agent) = agent else {
                reasons.push(format!("subagent link to node {head} has no agent id"));
                continue;
            };
            // A head devin since removed (a revert, a sidekick starting
            // fresh) or a fork's copied link names nothing here.
            if !present.contains(&head) {
                continue;
            }
            match forest.grouped_nodes.get(&head) {
                Some(&group) => {
                    groups.entry(agent).or_default().entry(group).or_insert(head);
                }
                None => reasons.push(format!(
                    "subagent agent {agent} links to node {head}, outside the detected subagent trees"
                )),
            }
        }
        for (agent, heads) in groups {
            if heads.len() > 1 {
                let mut heads: Vec<i64> = heads.into_values().collect();
                heads.sort_unstable();
                reasons.push(format!(
                    "subagent agent {agent} links to nodes {heads:?} in separate subagent \
                     tree groups, so its messages are split across sessions"
                ));
            }
        }
        reasons
            .into_iter()
            .map(|reason| AdapterError::schema(NAME, location, reason))
            .collect()
    }

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
            unmatched_tool_states: Vec::new(),
            agent_heads,
            errors: Vec::new(),
        };
        read.collect_nodes(&snapshot, id, location)
            .map_err(|error| db_error(db, "read message_nodes", &error))?;
        read.unmatched_tool_states = place_tool_states(&read.tool_states, &mut read.collected);
        Ok(Some(read))
    }

    /// Fold every node in as it streams, so only distinct message bodies stay
    /// resident: an identical copy of a known body only adds a placement;
    /// anything else is parsed.
    fn collect_nodes(
        &mut self,
        conn: &Connection,
        id: &str,
        location: &str,
    ) -> rusqlite::Result<()> {
        let mut copies: BTreeMap<String, Copies> = BTreeMap::new();
        let mut stmt = conn.prepare_cached(&node_query(", metadata, chat_message"))?;
        let mut rows = stmt.query([id])?;
        while let Some(row) = rows.next()? {
            let node_id: i64 = row.get(1)?;
            let metadata: Option<String> = row.get(10)?;
            let chat_message: String = row.get(11)?;
            let placed = NodeRef::from_row(row)?.and_then(|node| {
                let at = placement(&node, metadata.as_deref());
                match copies.get_mut(&node.message_id) {
                    Some(entry) => entry.place(&node, chat_message, at)?,
                    None => {
                        let mut entry = Copies::new(node.clone());
                        entry.place(&node, chat_message, at)?;
                        copies.insert(node.message_id.clone(), entry);
                    }
                }
                Some(node)
            });
            let Some(node) = placed else {
                self.errors.push(AdapterError::schema(
                    NAME,
                    format!("{location}/node {node_id}"),
                    "chat_message is not a JSON object with a string message_id",
                ));
                continue;
            };
            self.refs.push(node);
        }
        self.collected = copies
            .into_iter()
            .map(|(message_id, entry)| (message_id, entry.finish()))
            .collect();
        Ok(())
    }

    fn root_session(&self, id: &str, schema_version: Option<i64>) -> Session {
        let mut devin = Map::new();
        if let Some(version) = schema_version {
            devin.insert("schema_version".to_owned(), json!(version));
        }
        if !self.unmatched_tool_states.is_empty() {
            let states = Value::Array(self.unmatched_tool_states.clone());
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

fn place_tool_states(states: &[Value], collected: &mut BTreeMap<String, Collected>) -> Vec<Value> {
    let mut results = HashMap::new();
    for (message_id, entry) in collected.iter() {
        if entry.newest.get("role").and_then(Value::as_str) == Some("tool")
            && let Some(call_id) = entry.newest.get("tool_call_id").and_then(Value::as_str)
        {
            results
                .entry(call_id.to_owned())
                .or_insert_with(|| message_id.clone());
        }
    }
    let mut unmatched = Vec::new();
    for state in states {
        let Some(call_id) = state.get("tool_call_id").and_then(Value::as_str) else {
            unmatched.push(state.clone());
            continue;
        };
        let target = results.get(call_id);
        if let Some(entry) = target.and_then(|id| collected.get_mut(id)) {
            entry.tool_states.push(state.clone());
        } else {
            // Session options are the only carrier when no message matches a state.
            unmatched.push(state.clone());
        }
    }
    unmatched
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
    let mut image_provenance = None;

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
            image_provenance = Some(provenance);
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
            image_provenance = Some(Provenance::Conversational);
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
            image_provenance = Some(Provenance::Injected);
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
    // Images go after every other part kind: part ids are ordinal-keyed and
    // an already-stored id is skipped, so an image ahead of the text would
    // take the stored text part's id when a pre-image store re-reads it.
    if let Some(provenance) = image_provenance {
        for image in message
            .get("images")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            push(provenance, image_part(image));
        }
    }

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(canonical));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

/// One `chat_message.images[]` entry, devin's `ImageData {width, height,
/// base64_data, mime_type, source_path, caption}` (docs/adapters/devin.md):
/// the base64 payload verbatim (the whole entry as compact JSON when it has
/// none, so the part is never dropped), `source_path` as the file name.
/// Width, height and caption have no slot and stay in the raw record.
fn image_part(image: &Value) -> PartKind {
    let data = image
        .get("base64_data")
        .and_then(Value::as_str)
        .map_or_else(|| compact_json(image), ToOwned::to_owned);
    PartKind::File {
        media_type: extract_str(image, "mime_type").map(|mime| mime.as_str().to_owned()),
        file_name: extract_str(image, "source_path").map(|path| path.as_str().to_owned()),
        data: FileData::String(data),
    }
}

fn message_options(entry: &Collected) -> ProviderOptions {
    let mut devin = Map::new();
    devin.insert("nodes".to_owned(), Value::Array(entry.nodes.clone()));
    if !entry.tool_states.is_empty() {
        devin.insert(
            "tool_call_state".to_owned(),
            Value::Array(entry.tool_states.clone()),
        );
    }
    if !entry.variants.is_empty() {
        let variants = entry.variants.iter().map(extract_raw_record).collect();
        devin.insert("variants".to_owned(), Value::Array(variants));
    }
    if let Some(phase) = entry.newest.get("phase").filter(|phase| !phase.is_null()) {
        devin.insert("phase".to_owned(), phase.clone());
    }
    let mut options = ProviderOptions::new();
    options.insert(NAME.to_owned(), Value::Object(devin));
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": NAME,
            "row_id": entry.newest_row_id,
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
    #![expect(clippy::unwrap_used, reason = "tests fail by panicking")]
    use super::*;
    use crate::wire::Role;
    use futures::StreamExt;
    use tempfile::TempDir;

    fn macos_root() -> std::path::PathBuf {
        crate::adapter::test_support::manifest_dir().join("tests/fixtures/adapter/devin/macos/cli")
    }
    fn windows_root() -> std::path::PathBuf {
        crate::adapter::test_support::manifest_dir()
            .join("tests/fixtures/adapter/devin/windows/cli")
    }

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
        let read = read(&macos_root().join(DB_FILE));
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

    /// Every captured link head lands in the one group its agent owns -
    /// including the sidekick's, whose `subagent_heads` row and link records
    /// name two heads, the stale end of its first handoff and the live one.
    #[test]
    fn fixture_link_heads_belong_to_detected_subagent_trees() {
        let mut heads_checked = 0;
        for db in [
            macos_root().join(DB_FILE),
            windows_root().join(DB_FILE),
            midrun_root().join("after").join("cli").join(DB_FILE),
            sidekick_root().join(DB_FILE),
        ] {
            let conn = match open_forest(&db).unwrap() {
                Opened::Forest(conn) => conn,
                _ => panic!("fixture is not a forest database"),
            };
            for id in session_rows(&conn, &db).unwrap() {
                let read = SessionRead::load(&conn, &db, &id, &id).unwrap().unwrap();
                let forest = Forest::new(&read.refs);
                assert!(
                    read.link_drift(&forest, &id).is_empty(),
                    "{}#{id}",
                    db.display()
                );
                heads_checked += read.agent_heads.len();
            }
        }
        assert_eq!(heads_checked, 1, "the sidekick's head row is checked");
    }

    #[test]
    fn a_link_to_a_main_tree_reports_drift_without_reassigning_messages() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
        Connection::open(&db)
            .unwrap()
            .execute(
                "UPDATE message_nodes SET chat_message = json_set(
                    chat_message, '$.metadata.extensions.\"subagent/chain_node_id\"', 1)
                 WHERE session_id = 'amplified-color'
                   AND json_extract(chat_message,
                       '$.metadata.extensions.\"subagent/chain_node_id\"') = 49",
                [],
            )
            .unwrap();
        let original = macos();
        let forged = read(&db);
        assert_eq!(forged.errors.len(), 1);
        assert!(matches!(
            &forged.errors[0].kind,
            AdapterErrorKind::Schema(_)
        ));
        let reason = forged.errors[0].to_string();
        assert!(reason.contains("398e395d") && reason.contains("node 1"));
        assert!(reason.contains("outside the detected subagent trees"));
        let position = |item: &str| forged.order.iter().position(|entry| entry == item).unwrap();
        assert!(position("amplified-color") < position("error"));
        for id in [
            "amplified-color",
            "amplified-color/agent-51d1dc31-ee2a-44d7-b6f8-3be0f0cf5d6e",
        ] {
            let ids = |read: &Read| -> BTreeSet<String> {
                read.messages[id]
                    .iter()
                    .map(|message| message.id().to_owned())
                    .collect()
            };
            assert_eq!(ids(&forged), ids(&original));
        }
    }

    #[test]
    fn a_subagent_heads_row_to_a_main_tree_reports_drift() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
        Connection::open(&db)
            .unwrap()
            .execute(
                "INSERT INTO subagent_heads (session_id, agent_id, chain_node_id, updated_at)
                 VALUES ('level-waterlily', 'forged-agent', 1, 1)",
                [],
            )
            .unwrap();
        let read = read(&db);
        assert_eq!(read.errors.len(), 1);
        let reason = read.errors[0].to_string();
        assert!(reason.contains("forged-agent") && reason.contains("node 1"));
    }

    #[test]
    fn a_seeded_tree_with_a_rejected_name_is_still_grouped() {
        let node = NodeRef {
            row_id: 1,
            node_id: 1,
            parent: None,
            node_created: 1,
            message_id: "bad/id".to_owned(),
            created_at: None,
            is_system: false,
            is_handoff: false,
            starts_subagent: true,
            starts_persistent: false,
        };
        let forest = Forest::new(&[node]);
        assert_eq!(forest.rejected, vec!["bad/id"]);
        assert!(forest.grouped_nodes.contains_key(&1));
    }

    #[test]
    fn a_link_head_without_an_agent_id_reports_drift() {
        let db = macos_root().join(DB_FILE);
        let conn = match open_forest(&db).unwrap() {
            Opened::Forest(conn) => conn,
            _ => panic!("fixture is not a forest database"),
        };
        let mut read = SessionRead::load(&conn, &db, "amplified-color", "amplified-color")
            .unwrap()
            .unwrap();
        let forest = Forest::new(&read.refs);
        let entry = read
            .collected
            .values_mut()
            .find(|entry| {
                entry
                    .newest
                    .pointer("/metadata/extensions/subagent~1chain_node_id")
                    == Some(&json!(49))
            })
            .unwrap();
        entry.newest["metadata"]["extensions"]
            .as_object_mut()
            .unwrap()
            .remove("subagent/agent_id");
        let errors = read.link_drift(&forest, "amplified-color");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("node 49 has no agent id"));
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

    #[test]
    fn raw_record_row_id_is_its_highest_node_placement() {
        let read = macos();
        for message in read.messages.values().flatten() {
            let options = message.options();
            let newest = options[NAME]["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .max_by_key(|node| node["node_id"].as_i64())
                .unwrap();
            assert_eq!(
                options["source"]["row_id"],
                newest["row_id"],
                "{}",
                message.id()
            );
        }
    }

    /// System-prefix churn: one `message_id` whose content changes between
    /// context rebuilds, with a partial re-save putting the older content at
    /// the highest `row_id`. The highest `node_id` copy is the raw record
    /// whatever the row order, every other body is a variant, and each
    /// placement names the body it held.
    #[test]
    fn drifted_copies_keep_every_body_and_tag_each_placement() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
        let conn = Connection::open(&db).unwrap();
        // The level-waterlily prefix message placed most often: at least three
        // copies to split into drifted bodies.
        let (message_id, nodes): (String, String) = conn
            .query_row(
                "SELECT chat_message ->> '$.message_id', group_concat(node_id)
                 FROM (SELECT * FROM message_nodes WHERE session_id = 'level-waterlily'
                       ORDER BY node_id)
                 WHERE chat_message ->> '$.role' = 'system'
                 GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let nodes: Vec<i64> = nodes.split(',').map(|id| id.parse().unwrap()).collect();
        assert!(nodes.len() >= 3, "{nodes:?}");
        let (lowest, middle, highest) = (nodes[0], nodes[1], nodes[nodes.len() - 1]);
        let drift = |node_id: i64, content: &str| {
            conn.execute(
                "UPDATE message_nodes SET chat_message = json_set(chat_message, '$.content', ?2)
                 WHERE session_id = 'level-waterlily' AND node_id = ?1",
                rusqlite::params![node_id, content],
            )
            .unwrap();
        };
        drift(lowest, "prompt v1");
        drift(middle, "prompt v2");
        for &node_id in &nodes[2..] {
            drift(node_id, "prompt v3");
        }
        // The partial re-save: the lowest node gets the newest row id.
        conn.execute(
            "UPDATE message_nodes SET row_id = (SELECT max(row_id) + 1 FROM message_nodes)
             WHERE session_id = 'level-waterlily' AND node_id = ?1",
            [lowest],
        )
        .unwrap();
        drop(conn);

        let read = read(&db);
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        let message = read.messages["level-waterlily"]
            .iter()
            .find(|message| message.id() == format!("level-waterlily:{message_id}"))
            .unwrap();
        let options = message.options();
        assert_eq!(
            options["source"]["raw_record"]["content"],
            json!("prompt v3")
        );
        let variants: Vec<&Value> = options[NAME]["variants"]
            .as_array()
            .unwrap()
            .iter()
            .map(|variant| &variant["content"])
            .collect();
        assert_eq!(variants, [&json!("prompt v1"), &json!("prompt v2")]);
        let tags: BTreeMap<i64, i64> = options[NAME]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| {
                (
                    node["node_id"].as_i64().unwrap(),
                    node["variant"].as_i64().unwrap(),
                )
            })
            .collect();
        let expected: BTreeMap<i64, i64> = nodes
            .iter()
            .map(|&node_id| {
                let tag = if node_id == lowest {
                    1
                } else if node_id == middle {
                    2
                } else {
                    0
                };
                (node_id, tag)
            })
            .collect();
        assert_eq!(tags, expected);
        let placed = options[NAME]["nodes"].as_array().unwrap();
        let top = placed.iter().find(|node| node["node_id"] == json!(highest));
        assert_eq!(options["source"]["row_id"], top.unwrap()["row_id"]);

        // A message with one body carries no tags.
        for message in &read.messages["level-waterlily"] {
            let options = message.options();
            if options[NAME]["variants"].is_null() {
                let nodes = options[NAME]["nodes"].as_array().unwrap();
                assert!(nodes.iter().all(|node| node.get("variant").is_none()));
            }
        }
    }

    /// `phase` rides on the message's own options when the writer set it,
    /// and no key appears when it did not.
    #[test]
    fn phase_is_surfaced_only_when_present() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
        let commentary_id: String = {
            let conn = Connection::open(&db).unwrap();
            let message_id = conn
                .query_row(
                    "SELECT chat_message ->> '$.message_id' FROM message_nodes
                     WHERE session_id = 'branch-candy' AND chat_message ->> '$.role' = 'assistant'
                       AND json_type(chat_message, '$.phase') IS NULL
                     ORDER BY node_id LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            conn.execute(
                "UPDATE message_nodes
                 SET chat_message = json_set(chat_message, '$.phase', 'commentary')
                 WHERE session_id = 'branch-candy' AND chat_message ->> '$.message_id' = ?1",
                [&message_id],
            )
            .unwrap();
            message_id
        };
        let read = read(&db);
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        let phase_of = |message: &Message| message.options()[NAME].get("phase").cloned();
        let mut phases = BTreeMap::new();
        for message in read.messages.values().flatten() {
            let raw = message.options()["source"]["raw_record"]
                .get("phase")
                .cloned();
            assert_eq!(phase_of(message), raw, "{}", message.id());
            if let Some(phase) = raw {
                phases.insert(message.id().to_owned(), phase);
            }
        }
        assert_eq!(
            phases.get(&format!("branch-candy:{commentary_id}")),
            Some(&json!("commentary"))
        );
        assert!(phases.values().any(|phase| phase == "final_answer"));
        assert!(
            read.messages["level-waterlily"]
                .iter()
                .any(|message| phase_of(message).is_none())
        );
    }

    #[test]
    fn tool_state_uses_only_results_and_unmatched_rows_stay_on_root() {
        let entry = |id: &str, newest: Value| Collected {
            newest,
            newest_row_id: 1,
            first: NodeRef {
                row_id: 1,
                node_id: 1,
                parent: None,
                node_created: 1,
                message_id: id.to_owned(),
                created_at: None,
                is_system: false,
                is_handoff: false,
                starts_subagent: false,
                starts_persistent: false,
            },
            variants: Vec::new(),
            nodes: Vec::new(),
            tool_states: Vec::new(),
        };
        let mut collected = BTreeMap::from([
            (
                "assistant".to_owned(),
                entry(
                    "assistant",
                    json!({"role": "assistant", "tool_calls": [{"id": "a"}, {"id": "b"}]}),
                ),
            ),
            (
                "result".to_owned(),
                entry("result", json!({"role": "tool", "tool_call_id": "a"})),
            ),
        ]);
        let states = vec![
            json!({"tool_call_id": "a", "extra": 1}),
            json!({"tool_call_id": "b", "extra": 2}),
            json!({"tool_call_id": "c", "extra": 3}),
        ];
        let unmatched = place_tool_states(&states, &mut collected);
        assert_eq!(
            message_options(&collected["result"])[NAME]["tool_call_state"],
            json!([states[0]])
        );
        assert_eq!(
            message_options(&collected["assistant"])[NAME]["tool_call_state"],
            Value::Null
        );
        assert_eq!(unmatched, states[1..]);

        collected.insert(
            "second_result".to_owned(),
            entry(
                "second_result",
                json!({"role": "tool", "tool_call_id": "b"}),
            ),
        );
        let later_unmatched = place_tool_states(&states[1..], &mut collected);
        assert_eq!(
            message_options(&collected["second_result"])[NAME]["tool_call_state"],
            json!([states[1]])
        );
        assert_eq!(later_unmatched, vec![states[2].clone()]);

        let read = macos();
        assert!(read.sessions["branch-candy"].options[NAME]["tool_call_state"].is_null());
        let attached: usize = read.messages["branch-candy"]
            .iter()
            .map(|message| {
                message.options()[NAME]["tool_call_state"]
                    .as_array()
                    .map_or(0, Vec::len)
            })
            .sum();
        assert_eq!(attached, 3);
    }

    fn midrun_root() -> std::path::PathBuf {
        crate::adapter::test_support::manifest_dir().join("tests/fixtures/adapter/devin/midrun")
    }
    const MIDRUN_CHILD: &str = "gilded-orca/agent-e9b73e40-5526-42b0-acae-45389ecfe004";

    fn midrun(stage: &str) -> Read {
        let read = read(&midrun_root().join(stage).join("cli").join(DB_FILE));
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
        for root in [macos_root(), windows_root()] {
            assert_peek_matches_read(&root.join(DB_FILE));
        }
        for stage in ["before", "after"] {
            assert_peek_matches_read(&midrun_root().join(stage).join("cli").join(DB_FILE));
        }
        assert_peek_matches_read(&sidekick_root().join(DB_FILE));
    }

    /// Without `metadata.created_at` a message falls back to its first node's
    /// second; later copies carry later seconds and must not move the
    /// watermark past what the read stamps, or the session never gates fresh.
    #[test]
    fn a_message_without_created_at_keeps_peek_and_read_equal() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
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

    #[tokio::test]
    async fn events_recompute_heads_after_discovery() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
        let adapter = DevinAdapter::new(temp.path());
        assert_eq!(adapter.discover().await.unwrap(), 8);
        Connection::open(&db)
            .unwrap()
            .execute(
                "INSERT INTO sessions (id, working_directory, backend_type, model,
                    agent_mode, created_at, last_activity_at)
                 SELECT 'new-session', working_directory, backend_type, model,
                    agent_mode, created_at, last_activity_at
                 FROM sessions WHERE id = 'level-waterlily'",
                [],
            )
            .unwrap();
        let mut events = adapter.events_with(&crate::adapter::NoopOracle);
        let mut sessions = 0;
        while let Some(yielded) = events.next().await {
            if matches!(
                yielded.unwrap(),
                AdapterYield::Event(IngestEvent::Session(_))
            ) {
                sessions += 1;
            }
        }
        assert_eq!(sessions, 9);
    }

    #[test]
    fn sqlite_read_failure_is_typed_and_attributed_to_its_session() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("DROP TABLE message_nodes", []).unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        assert!(read_session(&conn, &db, "level-waterlily", None, &tx));
        match rx.try_recv().unwrap().unwrap() {
            AdapterYield::Failed { session_id, error } => {
                assert_eq!(session_id, "level-waterlily");
                assert!(matches!(error.kind, AdapterErrorKind::Io(_)));
            }
            other => panic!("expected attributed failure, got {other:?}"),
        }
        assert!(rx.try_recv().is_err());

        conn.execute(
            "UPDATE sessions SET working_directory = '' WHERE id = 'level-waterlily'",
            [],
        )
        .unwrap();
        assert!(read_session(&conn, &db, "level-waterlily", None, &tx));
        match rx.try_recv().unwrap().unwrap() {
            AdapterYield::Skipped {
                session_id,
                reason: SkipReason::Unsupported(_),
                ..
            } => {
                assert_eq!(session_id.as_deref(), Some("level-waterlily"));
            }
            other => panic!("expected unsupported source shape, got {other:?}"),
        }
    }

    /// A corrupt node is a typed error attributed to its node; the rest of the
    /// session still ingests, and the peek re-reads that session rather than
    /// trusting a partial graph - without dragging the others along.
    #[test]
    fn a_corrupt_node_is_a_typed_error_and_the_peek_goes_opaque() {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
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
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
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

    /// A copy of the macOS fixture where every copy of each message carries
    /// the given `chat_message.images` - the shape no capture produced,
    /// grounded in the binary (docs/adapters/devin.md).
    fn with_images(temp: &TempDir, images: &[(&str, Value)]) -> PathBuf {
        let db = temp.path().join(DB_FILE);
        std::fs::copy(macos_root().join(DB_FILE), &db).unwrap();
        let conn = Connection::open(&db).unwrap();
        for (message_id, images) in images {
            let updated = conn
                .execute(
                    "UPDATE message_nodes
                     SET chat_message = json_set(chat_message, '$.images', json(?1))
                     WHERE json_extract(chat_message, '$.message_id') = ?2",
                    rusqlite::params![images.to_string(), message_id],
                )
                .unwrap();
            assert!(updated > 0, "{message_id}");
        }
        db
    }

    fn file_parts(parts: &[Part]) -> Vec<(Option<&str>, Option<&str>, &FileData)> {
        parts
            .iter()
            .map(|part| match &part.kind {
                PartKind::File {
                    media_type,
                    file_name,
                    data,
                } => (media_type.as_deref(), file_name.as_deref(), data),
                other => panic!("{}: not a file part: {other:?}", part.id),
            })
            .collect()
    }

    /// The typed prompt of the headless `level-waterlily` session.
    const WATERLILY_PROMPT: &str = "e5892e34-53bf-4780-98ca-fa93127b3926";

    fn png(base64: &str, path: &str) -> Value {
        json!({
            "width": 1, "height": 1, "base64_data": base64,
            "mime_type": "image/png", "source_path": path, "caption": null,
        })
    }

    /// Images land after every part the message already emitted, so a store
    /// written before images were read keeps its text, reasoning, tool-call
    /// and tool-result part ids when a re-read adds the images: every
    /// existing part keeps its ordinal, and only File parts follow.
    #[test]
    fn images_become_file_parts_after_the_existing_parts() {
        const TYPED: &str = "4efb7e3f-b402-4e54-be24-f83d5771d2aa";
        const CALLS: &str = "520f2475-34a4-4c06-b0a4-082c7b1b32d1";
        const RESULT: &str = "f3e4968f-c6dc-4108-ad89-dfa3dac613d5";
        let before = macos();
        let temp = TempDir::new().unwrap();
        let db = with_images(
            &temp,
            &[
                (TYPED, json!([png("aGk=", "/tmp/pasted-images/a.png")])),
                (CALLS, json!([png("Ynll", "/tmp/b.png")])),
                (
                    RESULT,
                    json!([png("eW8=", "/tmp/c.png"), png("d2F2", "/tmp/d.png")]),
                ),
            ],
        );
        let after = read(&db);
        assert!(after.errors.is_empty(), "{:?}", after.errors);

        for (id, earlier) in &before.parts {
            let parts = &after.parts[id];
            assert_eq!(&parts[..earlier.len()], earlier.as_slice(), "{id}");
            let added = &parts[earlier.len()..];
            let expected = match id.strip_prefix("branch-candy:") {
                Some(TYPED | CALLS) => 1,
                Some(RESULT) => 2,
                _ => 0,
            };
            assert_eq!(added.len(), expected, "{id}");
            for (offset, part) in added.iter().enumerate() {
                let ordinal = earlier.len() + offset;
                assert_eq!(part.id, part_id(id, ordinal));
                assert_eq!(part.ordinal, part_ordinal(ordinal));
            }
        }

        let parts = |message: &str| &after.parts[&format!("branch-candy:{message}")];
        let typed = parts(TYPED);
        assert!(matches!(typed[0].kind, PartKind::Text { .. }));
        assert_eq!(
            file_parts(&typed[1..]),
            [(
                Some("image/png"),
                Some("/tmp/pasted-images/a.png"),
                &FileData::String("aGk=".to_owned())
            )]
        );
        assert_eq!(typed[1].provenance, Provenance::Conversational);

        let calls = parts(CALLS);
        assert!(matches!(
            calls[calls.len() - 2].kind,
            PartKind::ToolCall { .. }
        ));
        assert_eq!(calls.last().unwrap().provenance, Provenance::Conversational);

        let result = parts(RESULT);
        assert!(matches!(result[0].kind, PartKind::ToolResult { .. }));
        assert_eq!(
            file_parts(&result[1..]),
            [
                (
                    Some("image/png"),
                    Some("/tmp/c.png"),
                    &FileData::String("eW8=".to_owned())
                ),
                (
                    Some("image/png"),
                    Some("/tmp/d.png"),
                    &FileData::String("d2F2".to_owned())
                ),
            ]
        );
        assert!(
            result[1..]
                .iter()
                .all(|part| part.provenance == Provenance::Injected)
        );
    }

    /// A pasted image with no typed text is the message's only part.
    #[test]
    fn a_message_with_only_images_yields_only_file_parts() {
        let temp = TempDir::new().unwrap();
        let db = with_images(
            &temp,
            &[(WATERLILY_PROMPT, json!([png("aGk=", "/tmp/a.png")]))],
        );
        Connection::open(&db)
            .unwrap()
            .execute(
                "UPDATE message_nodes SET chat_message = json_set(chat_message, '$.content', '')
                 WHERE json_extract(chat_message, '$.message_id') = ?1",
                [WATERLILY_PROMPT],
            )
            .unwrap();
        let read = read(&db);
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        let id = format!("level-waterlily:{WATERLILY_PROMPT}");
        let parts = &read.parts[&id];
        assert_eq!(
            file_parts(parts),
            [(
                Some("image/png"),
                Some("/tmp/a.png"),
                &FileData::String("aGk=".to_owned())
            )]
        );
        assert_eq!(parts[0].id, part_id(&id, 0));
        assert_eq!(parts[0].provenance, Provenance::Conversational);
    }

    /// An image without `mime_type` or `source_path` still yields its part,
    /// those fields absent rather than defaulted (spec.md#model-no-synthesis).
    #[test]
    fn an_image_without_a_type_or_path_leaves_them_absent() {
        let temp = TempDir::new().unwrap();
        let bare = json!([{"width": 2, "height": 3, "base64_data": "aGk="}]);
        let read = read(&with_images(&temp, &[(WATERLILY_PROMPT, bare)]));
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        let parts = &read.parts[&format!("level-waterlily:{WATERLILY_PROMPT}")];
        assert!(matches!(parts[0].kind, PartKind::Text { .. }));
        assert_eq!(
            file_parts(&parts[1..]),
            [(None, None, &FileData::String("aGk=".to_owned()))]
        );
    }

    /// An entry without `base64_data` still yields its part, the whole entry
    /// as compact JSON, its own type and path kept.
    #[test]
    fn an_image_without_base64_data_keeps_the_whole_entry() {
        let temp = TempDir::new().unwrap();
        let entry = json!({"width": 2, "mime_type": "image/png", "source_path": "/tmp/a.png"});
        let read = read(&with_images(&temp, &[(WATERLILY_PROMPT, json!([entry]))]));
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        let parts = &read.parts[&format!("level-waterlily:{WATERLILY_PROMPT}")];
        assert!(matches!(parts[0].kind, PartKind::Text { .. }));
        assert_eq!(
            file_parts(&parts[1..]),
            [(
                Some("image/png"),
                Some("/tmp/a.png"),
                &FileData::String(compact_json(&entry))
            )]
        );
    }

    fn sidekick_root() -> std::path::PathBuf {
        crate::adapter::test_support::manifest_dir()
            .join("tests/fixtures/adapter/devin/sidekick/cli")
    }
    const SIDEKICK_PARENT: &str = "third-hourglass";
    const SIDEKICK_CHILD: &str = "third-hourglass/agent-b1e7050f-7a5a-42b4-a669-ddf4c4e03361";
    const EXPLORE_CHILD: &str = "third-hourglass/agent-3fc38d38-f601-4976-8786-4d13059aa112";

    /// Every node id the stored messages of `session` place.
    fn placements(read: &Read, session: &str) -> BTreeSet<i64> {
        read.messages[session]
            .iter()
            .flat_map(|message| message.options()[NAME]["nodes"].as_array().unwrap())
            .map(|node| node["node_id"].as_i64().unwrap())
            .collect()
    }

    /// Each pond session with the message ids it holds.
    fn ownership(read: &Read) -> BTreeMap<String, BTreeSet<String>> {
        read.messages
            .iter()
            .map(|(id, messages)| {
                let ids = messages.iter().map(|m| m.id().to_owned()).collect();
                (id.clone(), ids)
            })
            .collect()
    }

    /// A copy of the sidekick capture with `edit` applied to it.
    fn forged_sidekick(temp: &TempDir, edit: impl FnOnce(&Connection)) -> PathBuf {
        let db = temp.path().join(DB_FILE);
        std::fs::copy(sidekick_root().join(DB_FILE), &db).unwrap();
        edit(&Connection::open(&db).unwrap());
        db
    }

    /// One forged node of the sidekick session, stamped `at` seconds after
    /// 2026-09-29T22:00:00Z, after every captured node; `extensions`
    /// lands in `metadata.extensions`.
    fn add_node(
        conn: &Connection,
        (node_id, parent): (i64, Option<i64>),
        (role, message_id, operation): (&str, &str, &str),
        at: i64,
        extensions: &Value,
    ) {
        let created = DateTime::from_timestamp(1_790_719_200 + at, 0).unwrap();
        let chat_message = json!({
            "message_id": message_id,
            "role": role,
            "content": message_id,
            "metadata": {
                "created_at": created.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "telemetry": { "source": role, "operation": operation },
                "extensions": extensions,
            },
        });
        conn.execute(
            "INSERT INTO message_nodes (session_id, node_id, parent_node_id, chat_message,
                 created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                SIDEKICK_PARENT,
                node_id,
                parent,
                chat_message.to_string(),
                created.timestamp()
            ],
        )
        .unwrap();
    }

    fn read_sidekick() -> Read {
        let read = read(&sidekick_root().join(DB_FILE));
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        read
    }

    /// The captured Local Fusion session: two sidekick handoffs, the second
    /// brief appended to the first handoff's report inside ONE live tree
    /// whose root carries no subagent telemetry, plus the per-handoff prefix
    /// copies (`subagent_sidekick`), a parentless brief and an explore
    /// subagent. The sidekick is one child holding its whole chain - both
    /// briefs and both reports - named by its first brief; the parent keeps
    /// the lead's chain and every link record; every node is stored.
    #[test]
    fn a_persistent_sidekick_is_one_child_across_handoffs() {
        let read = read_sidekick();
        let sessions: Vec<&str> = read.sessions.keys().map(String::as_str).collect();
        assert_eq!(sessions, [SIDEKICK_PARENT, EXPLORE_CHILD, SIDEKICK_CHILD]);
        let child = &read.sessions[SIDEKICK_CHILD];
        assert_eq!(child.source_agent, SUBAGENT_AGENT);
        assert_eq!(child.parent_session_id.as_deref(), Some(SIDEKICK_PARENT));
        let brief: DateTime<chrono::Utc> = "2026-09-29T21:31:41.519472Z".parse().unwrap();
        assert_eq!(child.created_at, brief);

        let (root, sidekick) = (
            placements(&read, SIDEKICK_PARENT),
            placements(&read, SIDEKICK_CHILD),
        );
        let work: Vec<i64> = (94..=103).chain([155]).chain(174..=178).collect();
        for node in &work {
            assert!(sidekick.contains(node) && !root.contains(node), "{node}");
        }
        for node in [105, 106, 180, 181, 207] {
            assert!(root.contains(&node) && !sidekick.contains(&node), "{node}");
        }
        for message in ["b1e7050f", "bad34eff", "04fd5885", "07f67def"] {
            let held = |session: &str| {
                read.messages[session]
                    .iter()
                    .any(|m| m.id().split(':').nth(1).unwrap().starts_with(message))
            };
            assert!(held(SIDEKICK_CHILD) && !held(SIDEKICK_PARENT), "{message}");
        }
        let stored: BTreeSet<i64> = read
            .sessions
            .keys()
            .flat_map(|id| placements(&read, id))
            .collect();
        assert_eq!(stored, (0..=207).collect(), "every node is stored");
    }

    /// #315's real sessions yielded no child where devin wrote no parentless
    /// brief; the brief's `subagent/handoff` copies name the child without it.
    #[test]
    fn a_sidekick_without_its_parentless_brief_keeps_its_name() {
        let temp = TempDir::new().unwrap();
        // The parentless brief's whole tree: the brief (85) and the two
        // system notes devin writes beneath it (92, 93).
        let db = forged_sidekick(&temp, |conn| {
            conn.execute(
                "DELETE FROM message_nodes WHERE node_id IN (85, 92, 93)",
                [],
            )
            .unwrap();
        });
        let read = read(&db);
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        let full = read_sidekick();
        assert_eq!(read.sessions[SIDEKICK_CHILD], full.sessions[SIDEKICK_CHILD]);
        assert_eq!(read.sessions.len(), 3);
        assert!(placements(&read, SIDEKICK_CHILD).is_superset(&(94..=103).collect()));
    }

    /// A sync at any point of either handoff stores a subset of what the
    /// final sync stores, each message in the same session, under the same
    /// child row: the first brief names the child, and the second brief
    /// arriving later cannot rename it.
    #[test]
    fn a_sync_mid_handoff_stores_what_a_later_one_would() {
        let full = read_sidekick();
        let later: BTreeSet<String> = ownership(&full).into_values().flatten().collect();
        for cut in [96, 103, 106, 155, 176, 178] {
            let temp = TempDir::new().unwrap();
            let db = forged_sidekick(&temp, |conn| {
                conn.execute("DELETE FROM message_nodes WHERE node_id > ?1", [cut])
                    .unwrap();
            });
            let early = read(&db);
            assert!(early.errors.is_empty(), "{cut}: {:?}", early.errors);
            assert_eq!(
                early.sessions[SIDEKICK_CHILD], full.sessions[SIDEKICK_CHILD],
                "{cut}"
            );
            let stored: BTreeSet<String> = ownership(&early).into_values().flatten().collect();
            let moved: Vec<&String> = stored.difference(&later).collect();
            assert!(moved.is_empty(), "{cut}: {moved:?}");
            assert_peek_matches_read(&db);
        }
    }

    /// The partition never reads `subagent_heads`: emptied, pointing at a
    /// node devin since removed (it then starts fresh), or naming another
    /// agent's node, the rows change nothing but the check; only a head in a
    /// main tree is drift.
    #[test]
    fn subagent_heads_are_checked_but_never_partition() {
        let full = ownership(&read_sidekick());
        for (edit, drift) in [
            ("DELETE FROM subagent_heads", None),
            ("UPDATE subagent_heads SET chain_node_id = 999", None),
            (
                "INSERT INTO subagent_heads (session_id, agent_id, chain_node_id, updated_at)
                 VALUES ('third-hourglass', 'a90f5090', 203, 1)",
                None,
            ),
            (
                "UPDATE subagent_heads SET chain_node_id = 1",
                Some("sidekick links to node 1,"),
            ),
        ] {
            let temp = TempDir::new().unwrap();
            let db = forged_sidekick(&temp, |conn| {
                conn.execute(edit, []).unwrap();
            });
            let read = read(&db);
            let reasons: Vec<String> = read.errors.iter().map(ToString::to_string).collect();
            match drift {
                None => assert!(reasons.is_empty(), "{edit}: {reasons:?}"),
                Some(needle) => {
                    assert_eq!(reasons.len(), 1, "{edit}: {reasons:?}");
                    assert!(reasons[0].contains(needle), "{reasons:?}");
                }
            }
            assert_eq!(ownership(&read), full, "{edit}");
        }
    }

    /// A link record whose head lies in no subagent tree, or one naming the
    /// sidekick while pointing into the explore subagent's trees, is drift;
    /// neither reassigns a message.
    #[test]
    fn a_sidekick_link_outside_its_group_reports_drift() {
        let full = ownership(&read_sidekick());
        for (node, head, agent, needle) in [
            (181, 1, "sidekick", "sidekick links to node 1,"),
            (205, 203, "sidekick", "sidekick links to nodes [103, 203]"),
        ] {
            let temp = TempDir::new().unwrap();
            let db = forged_sidekick(&temp, |conn| {
                conn.execute(
                    "UPDATE message_nodes SET chat_message = json_set(chat_message,
                         '$.metadata.extensions.\"subagent/chain_node_id\"', ?2,
                         '$.metadata.extensions.\"subagent/agent_id\"', ?3)
                     WHERE node_id = ?1",
                    rusqlite::params![node, head, agent],
                )
                .unwrap();
            });
            let read = read(&db);
            let reasons: Vec<String> = read.errors.iter().map(ToString::to_string).collect();
            assert_eq!(reasons.len(), 1, "{reasons:?}");
            assert!(reasons[0].contains(needle), "{reasons:?}");
            assert_eq!(ownership(&read), full);
        }
    }

    /// A third handoff that starts fresh - its stored head gone, so its live
    /// tree and prefix copy share no message with the first two - is still
    /// the same agent: one child, the same name, its brief and report
    /// inside, and its link heads accepted. That holds at every cut once a
    /// fresh tree holds the brief, before its prefix copy exists and whether
    /// or not a parentless brief comes first. A compaction continuing the
    /// live tree stays in the child; the summarizer chain stays with the root.
    #[test]
    fn a_fresh_handoff_and_a_compaction_stay_in_the_one_sidekick_child() {
        let (none, brief) = (json!({}), json!({ "subagent/handoff": true }));
        let link = json!({ "subagent/agent_id": "sidekick", "subagent/chain_node_id": 303 });
        let forged = [
            (299, None, "user", "brief-3", "unknown", 2, &brief),
            (300, None, "system", "fresh-root", "unknown", 0, &none),
            (301, Some(300), "system", "fresh-model", "unknown", 1, &none),
            (302, Some(301), "user", "brief-3", "unknown", 2, &brief),
            (
                303,
                Some(302),
                "assistant",
                "report-3",
                "inference",
                3,
                &none,
            ),
            (
                310,
                None,
                "system",
                "fresh-prefix",
                PERSISTENT_PREFIX,
                0,
                &none,
            ),
            (311, Some(310), "system", "fresh-model", "unknown", 1, &none),
            (312, Some(311), "user", "brief-3", "unknown", 2, &brief),
            (320, Some(207), "system", "done-3", "unknown", 4, &link),
            (330, Some(94), "system", "continuing", "unknown", 5, &none),
            (
                331,
                Some(330),
                "assistant",
                "after-compaction",
                "inference",
                6,
                &none,
            ),
            (340, None, "system", "summarizer", "unknown", 5, &none),
            (
                341,
                Some(340),
                "user",
                "summarize-this",
                "unknown",
                6,
                &none,
            ),
        ];
        for parentless_brief in [false, true] {
            for cut in [299, 302, 312, 341] {
                let written = |node_id: i64| node_id <= cut && (parentless_brief || node_id != 299);
                let temp = TempDir::new().unwrap();
                let db = forged_sidekick(&temp, |conn| {
                    for (node_id, parent, role, message_id, operation, at, extensions) in forged {
                        if written(node_id) {
                            add_node(
                                conn,
                                (node_id, parent),
                                (role, message_id, operation),
                                at,
                                extensions,
                            );
                        }
                    }
                    conn.execute("UPDATE subagent_heads SET chain_node_id = 303", [])
                        .unwrap();
                });
                let case = format!("parentless brief {parentless_brief}, cut {cut}");
                let read = read(&db);
                assert!(read.errors.is_empty(), "{case}: {:?}", read.errors);
                assert_eq!(read.sessions.len(), 3, "{case}: {:?}", read.sessions.keys());
                let child = placements(&read, SIDEKICK_CHILD);
                let sidekick = [299, 300, 301, 302, 303, 310, 311, 312, 330, 331];
                for node in sidekick
                    .into_iter()
                    .filter(|&node| written(node))
                    .chain([94, 178])
                {
                    assert!(child.contains(&node), "{case}: {node}");
                }
                let root = placements(&read, SIDEKICK_PARENT);
                for node in [320, 340, 341].into_iter().filter(|&node| written(node)) {
                    assert!(
                        root.contains(&node) && !child.contains(&node),
                        "{case}: {node}"
                    );
                }
                assert_peek_matches_read(&db);
            }
        }
    }

    /// A subagent tree nothing names is held back, never silently dropped:
    /// the read reports it and the root stays pending, so each sync re-reads
    /// it until a task prompt arrives.
    #[test]
    fn an_unnamed_subagent_tree_is_reported_and_keeps_the_root_pending() {
        let temp = TempDir::new().unwrap();
        let db = forged_sidekick(&temp, |conn| {
            let none = json!({});
            add_node(
                conn,
                (400, None),
                ("system", "orphan-prefix", "subagent_general"),
                0,
                &none,
            );
            add_node(
                conn,
                (401, Some(400)),
                ("system", "orphan-model", "unknown"),
                1,
                &none,
            );
        });
        let read = read(&db);
        let reasons: Vec<String> = read.errors.iter().map(ToString::to_string).collect();
        assert_eq!(reasons.len(), 1, "{reasons:?}");
        assert!(
            reasons[0].contains("2 messages of the subagent trees rooted at nodes [400]"),
            "{reasons:?}"
        );
        assert_eq!(ownership(&read), ownership(&read_sidekick()));
        assert_eq!(peeked(&db)[SIDEKICK_PARENT], SourceWatermark::Opaque);
    }
}
