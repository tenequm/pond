//! NanoClaw adapter (github.com/nanocoai/nanoclaw).
//!
//! NanoClaw runs agents in containers and has NO transcript format of its own
//! for the Claude provider: the container runs the Claude Agent SDK, which
//! writes standard Claude-Code JSONL. So nanoclaw IS a claude-JSONL source and
//! reuses `claude_code`'s record mapping verbatim ([`map_row_events`],
//! [`claude_peek_watermark`], the subagent-sidecar helpers, [`claude_serialize`]);
//! only session identity, the on-disk layout, and the SQLite metadata join
//! differ.
//!
//! Layout (under the install root, `config { "path": "<root>" }`):
//! ```text
//! <root>/data/v2.db                                    central metadata DB
//! <root>/data/v2-sessions/<agentGroupId>/
//!   .claude-shared/projects/<projectDir>/<sdkUuid>.jsonl            transcripts
//!   .claude-shared/projects/<projectDir>/<sdkUuid>.jsonl.rotated-<epochMs>  rotated (immutable, complete)
//!   .claude-shared/projects/<projectDir>/<sdkUuid>/subagents/agent-<id>.jsonl  subagent transcripts (+ .meta.json)
//!   <sessionId>/outbound.db                            container-written (session_state)
//!   <sessionId>/inbound.db                             host-written queue
//!   <sessionId>/opencode-xdg/                          opencode-provider storage (composed via the opencode reader)
//! ```
//!
//! Identity (plan 3.1): `session_id` = SDK transcript filename stem; subagent
//! sidecars are their own sessions per `claude_code` convention. `project` =
//! `<agentGroupId>` (the group is nanoclaw's shared-state scope; container `cwd`
//! is always `/workspace/agent`, so it carries no per-session identity).
//! `source_agent` = `nanoclaw` / `nanoclaw/subagent`. `queue-operation` records
//! (no uuid/message) ride placement rule 3 as System carriers, never dropped -
//! the same no-`message` path `claude_code` uses for its own metadata rows.
//!
//! Metadata join (plan 1.3, read-only, best-effort): each `<sessionId>/outbound.db`
//! `session_state` key `continuation:claude` (legacy `sdk_session_id`) links a
//! transcript SDK uuid to a nanoclaw session id; `data/v2.db` then joins
//! `sessions` -> `messaging_groups` -> `agent_groups` -> `container_configs`. The
//! result lands in `options.nanoclaw`. DBs open read-only with a busy timeout and
//! retry once on a malformed image; any unreadable DB, absent column, or orphan
//! (in either direction) degrades to metadata-absent - the transcript still
//! ingests, and nothing is ever synthesized.
//!
//! Providers (plan 1.4, 3.4): the base runtime ships the Claude provider; codex
//! and opencode are opt-in per-group skills. This adapter handles all three.
//! Claude rides the `JsonlTree` path above. For each session folder holding an
//! `opencode-xdg/` store, the `opencode` adapter's reader runs against it
//! (composition, not a second parser) and each session is re-attributed:
//! `source_agent` -> `nanoclaw` / `nanoclaw/subagent` (opencode's own taxonomy
//! decides which), `project` -> the `<agentGroupId>`, provider + nanoclaw metadata
//! -> `options.nanoclaw`; opencode's own session/message ids stay canonical. Codex
//! keeps its history server-side with no on-disk transcript, so sessions whose
//! resolved provider (`v2.db sessions.agent_provider`, else the group's
//! `container_configs.provider`, else `claude`) is `codex` surface as a visible
//! `Unsupported` skip, enumerated from `v2.db` metadata.
//!
//! Documented non-ingest (per-adapter contract): the pre-compaction Markdown
//! summaries under `groups/<folder>/conversations/*.md`; the `messages_in` /
//! `messages_out` IPC bodies in `inbound.db` / `outbound.db` (routing, not
//! transcript); the `tool-results/*.txt` spilled tool outputs (referenced by, not
//! part of, the transcript); and codex-provider sessions (server-side history,
//! surfaced as a counted skip, never ingested).

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use async_stream::stream;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use tokio_stream::StreamExt;

use crate::{
    sessions::IngestEvent,
    wire::{ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    PlanFuture, RestoreFidelity, RestoredFile, SkipOracle, SkipReason, SourceWatermark,
    claude_code::{
        FileState, SubagentDescriptor, claude_peek_watermark, claude_serialize,
        is_workflow_control_file, map_row_events, parse_timestamp, source_project_dir,
        subagent_descriptor, subagent_ids, subagent_unsupported_reason, subagents_dir,
        unresolved_subagent_error,
    },
    config_path,
    extract::{Extracted, extract_self_str},
    jsonl::{BoundedRow, JsonlTree, jsonl_tree_discover, jsonl_tree_events, jsonl_tree_plan},
    opencode, sqlite, validate_path_id,
};

const NAME: &str = "nanoclaw";

/// Install-root candidate `probe_default` checks. Installs live anywhere, so
/// config-first is the normal path; the probe only offers `~/nanoclaw` as a
/// convenience and is gated on it actually holding `data/v2-sessions/`.
const PROBE_CANDIDATES: &[&[&str]] = &[&["nanoclaw"]];

/// Stateless factory: opens [`NanoclawAdapter`] instances and probes the
/// well-known install locations for a `data/v2-sessions/` dir.
pub struct NanoclawFactory;

impl AdapterFactory for NanoclawFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(NanoclawAdapter::new(config_path(NAME, config)?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        // Installs live anywhere, so config-first is the normal path; auto-probe
        // only offers a candidate that actually holds `data/v2-sessions/`, so an
        // empty checkout never masquerades as a source.
        for segments in PROBE_CANDIDATES {
            let mut candidate = env.home.clone();
            for segment in *segments {
                candidate.push(segment);
            }
            if candidate.join("data").join("v2-sessions").is_dir() {
                return Some(json!({ "path": candidate }));
            }
        }
        None
    }

    fn serialize(
        &self,
        session: &crate::sessions::SessionWithMessages,
        fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        claude_serialize(NAME, session, fidelity, transcript_path(session)?)
    }
}

/// Configured nanoclaw reader. Walks `<root>/data/v2-sessions/.../*.jsonl`
/// (plus rotated files) and joins each transcript to its nanoclaw metadata,
/// built lazily on first read so `plan`/`discover` never pay for the DB scan.
#[derive(Clone)]
pub struct NanoclawAdapter {
    root: PathBuf,
    metadata: Arc<OnceLock<HashMap<String, Value>>>,
}

impl NanoclawAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            metadata: Arc::new(OnceLock::new()),
        }
    }

    fn metadata(&self) -> &HashMap<String, Value> {
        self.metadata.get_or_init(|| build_metadata(&self.root))
    }
}

impl Adapter for NanoclawAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        jsonl_tree_discover(self)
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        // Claude transcripts ride the JsonlTree path; opencode-provider stores and
        // codex sessions are enumerated from `data/v2-sessions` + `v2.db` and
        // appended (composition + visible skips), so the JsonlTree fast path stays
        // untouched.
        let claude = jsonl_tree_events(self, oracle);
        let root = self.root.clone();
        Box::pin(stream! {
            let mut claude = claude;
            while let Some(item) = claude.next().await {
                yield item;
            }

            let work = tokio::task::spawn_blocking(move || provider_work(&root)).await;
            let work = match work {
                Ok(work) => work,
                Err(join) => {
                    yield Err(AdapterError::io(
                        NAME,
                        "provider enumeration task",
                        std::io::Error::other(join.to_string()),
                    ));
                    return;
                }
            };

            for skip in work.codex_skips {
                yield Ok(AdapterYield::Skipped {
                    session_id: Some(skip.session_id),
                    project: Some(skip.agent_group_id),
                    reason: SkipReason::Unsupported(CODEX_SKIP_REASON.to_owned()),
                });
            }

            for session in work.opencode {
                let attribution = opencode::Attribution {
                    source_agent_root: NAME.to_owned(),
                    project: session.project,
                    extra_options: ("nanoclaw".to_owned(), session.nanoclaw_options),
                };
                let mut composed =
                    opencode::composed_events_with(session.data_dir, oracle, attribution);
                while let Some(item) = composed.next().await {
                    yield item;
                }
            }
        })
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> PlanFuture<'a> {
        jsonl_tree_plan(self, oracle)
    }
}

impl JsonlTree for NanoclawAdapter {
    type State = FileState;

    fn name(&self) -> &'static str {
        NAME
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn is_transcript(&self, path: &Path) -> bool {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return false;
        };
        name.ends_with(".jsonl") || is_rotated_transcript(name)
    }

    fn skip_source(&self, path: &Path) -> bool {
        is_workflow_control_file(path)
            || path
                .components()
                .any(|component| component.as_os_str() == OsStr::new("opencode-xdg"))
    }

    fn peek_session_id(&self, path: &Path, _first_line: &str) -> Option<String> {
        if subagents_dir(path).is_some() {
            let (parent_uuid, child_suffix, _) = subagent_ids(path)?;
            return Some(format!("{parent_uuid}/{child_suffix}"));
        }
        sdk_session_stem(path)
    }

    fn peek_watermark(&self, path: &Path) -> SourceWatermark {
        claude_peek_watermark(path)
    }

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
        session_from_rows(path, rows, self.metadata())
    }

    fn events_from_row(
        &self,
        session: &Session,
        row: &BoundedRow,
        state: &mut Self::State,
    ) -> Result<Vec<IngestEvent>, String> {
        map_row_events(&session.id, session.created_at, row, state)
    }

    fn unsupported_reason(&self, path: &Path) -> Option<String> {
        subagent_unsupported_reason(path)
    }
}

/// `<uuid>.jsonl.rotated-<epochMs>` - a rotated-out complete transcript. Its
/// extension is not `jsonl`, so the default walk skips it; nanoclaw accepts it.
fn is_rotated_transcript(name: &str) -> bool {
    match name.rsplit_once(".rotated-") {
        Some((head, digits)) => {
            head.ends_with(".jsonl")
                && !digits.is_empty()
                && digits.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

/// The SDK session uuid from a transcript filename: the stem, with a trailing
/// `.rotated-<epochMs>` stripped first so a rotated file ingests under the same
/// id as the live file it was renamed from.
fn sdk_session_stem(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let base = match name.rsplit_once(".rotated-") {
        Some((head, digits))
            if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) =>
        {
            head
        }
        _ => name,
    };
    base.strip_suffix(".jsonl").map(ToOwned::to_owned)
}

/// The `<agentGroupId>` directory name - the path component right after
/// `v2-sessions`. This is nanoclaw's shared-state scope and pond's `project`.
fn agent_group_from_path(path: &Path) -> Option<String> {
    let mut components = path.components();
    while let Some(component) = components.next() {
        if component.as_os_str() == OsStr::new("v2-sessions") {
            return components
                .next()
                .and_then(|next| next.as_os_str().to_str())
                .map(ToOwned::to_owned);
        }
    }
    None
}

fn session_from_rows(
    path: &Path,
    rows: &[BoundedRow],
    metadata: &HashMap<String, Value>,
) -> Result<Session, AdapterError> {
    let display = path.display().to_string();
    // A non-agent leaf under `subagents/` would borrow the parent's content
    // `sessionId` and silently merge; refuse structurally.
    if let Some(error) = unresolved_subagent_error(NAME, path) {
        return Err(error);
    }

    let agent_group_id = agent_group_from_path(path).ok_or_else(|| {
        AdapterError::schema(
            NAME,
            display.clone(),
            "transcript path is not under data/v2-sessions/<agentGroupId>",
        )
    })?;
    let subagent = subagent_descriptor(path);
    let project_dir = source_project_dir(path, subagent.is_some());
    let created_at = rows
        .iter()
        .find_map(|row| parse_timestamp(&row.value).ok())
        .ok_or_else(|| {
            AdapterError::schema(NAME, display.clone(), "session has no parseable timestamp")
        })?;
    let raw_session_id = rows
        .iter()
        .find_map(|row| row.value.get("sessionId").and_then(Value::as_str))
        .map(ToOwned::to_owned);

    let (session_id, parent_session_id, source_agent, subagent_options, metadata_key) =
        match subagent {
            Some(SubagentDescriptor {
                parent_uuid,
                child_suffix,
                agent_hash,
                meta,
                ..
            }) => {
                let child_id = format!("{parent_uuid}/{child_suffix}");
                // Mirror claude_code's `options.subagent` shape so the sidecar
                // restores losslessly; the metadata join keys on the parent's
                // SDK uuid (the subagent shares its continuation).
                let subagent_meta = json!({
                    "hash": agent_hash,
                    "raw_session_id": raw_session_id,
                    "meta": meta,
                });
                let key = parent_uuid.clone();
                (
                    child_id,
                    Some(parent_uuid),
                    "nanoclaw/subagent".to_owned(),
                    Some(subagent_meta),
                    key,
                )
            }
            None => {
                let id = sdk_session_stem(path)
                    .or_else(|| raw_session_id.clone())
                    .ok_or_else(|| {
                        AdapterError::schema(
                            NAME,
                            display.clone(),
                            "cannot determine SDK session id from filename",
                        )
                    })?;
                let key = id.clone();
                (id, None, "nanoclaw".to_owned(), None, key)
            }
        };

    let mut source = serde_json::Map::new();
    source.insert("adapter".to_owned(), Value::String(NAME.to_owned()));
    source.insert(
        "agent_group_id".to_owned(),
        Value::String(agent_group_id.clone()),
    );
    if let Some(dir) = &project_dir {
        source.insert("project_dir".to_owned(), Value::String(dir.clone()));
    }
    let mut options = ProviderOptions::new();
    options.insert("source".to_owned(), Value::Object(source));
    if let Some(subagent_meta) = subagent_options {
        options.insert("subagent".to_owned(), subagent_meta);
    }
    if let Some(nanoclaw) = metadata.get(metadata_key.as_str()) {
        options.insert("nanoclaw".to_owned(), nanoclaw.clone());
    }

    let project = extract_self_str(&Value::String(agent_group_id)).ok_or_else(|| {
        AdapterError::schema(
            NAME,
            display,
            "internal: agent group id produced no project",
        )
    })?;

    Ok(Session {
        id: session_id,
        parent_session_id,
        parent_message_id: None,
        source_agent,
        created_at,
        project,
        options,
    })
}

/// Native restore path from the install root: replays into the observed
/// `data/v2-sessions/<agentGroupId>/.claude-shared/projects/<projectDir>/`
/// layout, stored in `options.source` at ingest. Every id segment is validated.
fn transcript_path(
    session: &crate::sessions::SessionWithMessages,
) -> Result<PathBuf, AdapterError> {
    let source = session.session.options.get("source");
    let group = source
        .and_then(|source| source.get("agent_group_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                &session.session.id,
                "session options.source.agent_group_id missing; cannot rebuild native path",
            )
        })?;
    let project_dir = source
        .and_then(|source| source.get("project_dir"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                &session.session.id,
                "session options.source.project_dir missing; cannot rebuild native path",
            )
        })?;
    validate_path_id(NAME, "agent_group_id", group, &session.session.id)?;
    validate_path_id(NAME, "project_dir", project_dir, &session.session.id)?;

    let mut path = PathBuf::from("data")
        .join("v2-sessions")
        .join(group)
        .join(".claude-shared")
        .join("projects")
        .join(project_dir);
    if let Some(parent) = &session.session.parent_session_id {
        validate_path_id(NAME, "parent_session_id", parent, &session.session.id)?;
        let child_suffix = session
            .session
            .id
            .strip_prefix(&format!("{parent}/"))
            .unwrap_or(&session.session.id);
        for segment in child_suffix.split('/') {
            validate_path_id(NAME, "child_suffix segment", segment, &session.session.id)?;
        }
        path = path
            .join(parent)
            .join("subagents")
            .join(format!("{child_suffix}.jsonl"));
    } else {
        validate_path_id(NAME, "session_id", &session.session.id, &session.session.id)?;
        path = path.join(format!("{}.jsonl", session.session.id));
    }
    Ok(path)
}

// --- Read-only SQLite metadata join (best-effort, degrades to absent) --------

/// Every real IPC session folder under `data/v2-sessions`, as
/// `(agentGroupId, sessionId, sessionPath)`, sorted for deterministic order.
/// Skips non-dirs and dotfile entries (`.claude-shared` holds transcripts, not a
/// session folder). The metadata index and provider enumeration walk this same
/// tree, so they share one implementation.
fn walk_session_dirs(root: &Path) -> Vec<(String, String, PathBuf)> {
    let v2_sessions = root.join("data").join("v2-sessions");
    let mut out = Vec::new();
    let Ok(groups) = std::fs::read_dir(&v2_sessions) else {
        return out;
    };
    let mut group_entries: Vec<_> = groups.flatten().collect();
    group_entries.sort_by_key(std::fs::DirEntry::file_name);
    for group in group_entries {
        if !group.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Some(group_id) = group.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let Ok(sessions) = std::fs::read_dir(group.path()) else {
            continue;
        };
        let mut session_entries: Vec<_> = sessions.flatten().collect();
        session_entries.sort_by_key(std::fs::DirEntry::file_name);
        for session in session_entries {
            if !session.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            let name = session.file_name();
            let Some(session_id) = name.to_str() else {
                continue;
            };
            if session_id.starts_with('.') {
                continue;
            }
            out.push((group_id.clone(), session_id.to_owned(), session.path()));
        }
    }
    out
}

/// Build `sdk_uuid -> options.nanoclaw` once, by scanning every
/// `<group>/<sessionId>/outbound.db` for the transcript link, then joining each
/// nanoclaw session against `data/v2.db`. Any unreadable DB or orphan is skipped;
/// the result is only ever additive metadata.
fn build_metadata(root: &Path) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    let v2 = open_metadata_db(&root.join("data").join("v2.db"));
    for (group_id, session_id, session_path) in walk_session_dirs(root) {
        let outbound = session_path.join("outbound.db");
        if !outbound.exists() {
            continue;
        }
        let Some(sdk_uuid) = read_continuation(&outbound) else {
            continue;
        };
        let nanoclaw = nanoclaw_metadata_object(v2.as_ref(), &session_id, &group_id);
        map.insert(sdk_uuid, Value::Object(nanoclaw));
    }
    map
}

/// The `options.nanoclaw` object for one nanoclaw session: its id and agent group
/// (always present, straight from the layout) plus the best-effort `v2.db` join
/// against a shared connection (`None` when `v2.db` is unreadable). Shared by the
/// claude metadata index and the opencode-provider composition.
fn nanoclaw_metadata_object(
    v2: Option<&Connection>,
    session_id: &str,
    group_id: &str,
) -> serde_json::Map<String, Value> {
    let mut nanoclaw = serde_json::Map::new();
    nanoclaw.insert(
        "nanoclaw_session_id".to_owned(),
        Value::String(session_id.to_owned()),
    );
    nanoclaw.insert(
        "agent_group_id".to_owned(),
        Value::String(group_id.to_owned()),
    );
    if let Some(conn) = v2 {
        for (key, value) in read_session_metadata(conn, session_id) {
            nanoclaw.insert(key, value);
        }
    }
    nanoclaw
}

fn is_malformed(message: &str) -> bool {
    message.contains("malformed")
}

/// Open a metadata DB read-only (shared `sqlite::open_db`), retrying once on a
/// Docker-Desktop page-cache `database disk image is malformed`. `None` on any
/// other failure - metadata is best-effort and must never block ingestion. One
/// open serves every session's join, not one per session.
fn open_metadata_db(path: &Path) -> Option<Connection> {
    for attempt in 0..2 {
        match sqlite::open_db(NAME, path) {
            Ok(conn) => return Some(conn),
            Err(error) if attempt == 0 && is_malformed(&error.to_string()) => continue,
            Err(_) => return None,
        }
    }
    None
}

/// Open `path` read-only and run `query` once, retrying the open on a malformed
/// page cache. Any failure degrades to `None`. For the per-folder `outbound.db`
/// reads, where each DB is opened exactly once anyway.
fn read_db<T>(path: &Path, query: impl Fn(&Connection) -> rusqlite::Result<T>) -> Option<T> {
    let conn = open_metadata_db(path)?;
    query(&conn).ok()
}

/// The SDK session uuid this container's transcript resumes from - the
/// `continuation:claude` row (legacy key `sdk_session_id`).
fn read_continuation(path: &Path) -> Option<String> {
    read_db(path, |conn| {
        let mut stmt = conn.prepare(
            "SELECT key, value FROM session_state \
             WHERE key IN ('continuation:claude', 'sdk_session_id')",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut primary = None;
        let mut legacy = None;
        for row in rows {
            let (key, value) = row?;
            if key == "continuation:claude" {
                primary = Some(value);
            } else {
                legacy = Some(value);
            }
        }
        Ok(primary.or(legacy))
    })
    .flatten()
}

/// Join one nanoclaw session against a shared `v2.db` connection. Prefers the
/// full sessions/messaging_groups/agent_groups/container_configs join; on any
/// error (a column absent on an old install, a corrupt DB) falls back to the
/// minimal sessions-only projection, then to nothing. Only non-null columns are
/// carried.
fn read_session_metadata(conn: &Connection, session_id: &str) -> Vec<(String, Value)> {
    join_full(conn, session_id)
        .ok()
        .flatten()
        .or_else(|| join_minimal(conn, session_id).ok().flatten())
        .unwrap_or_default()
}

fn str_column(row: &rusqlite::Row, index: usize) -> rusqlite::Result<Option<Value>> {
    Ok(row.get::<_, Option<String>>(index)?.map(Value::String))
}

fn int_column(row: &rusqlite::Row, index: usize) -> rusqlite::Result<Option<Value>> {
    Ok(row
        .get::<_, Option<i64>>(index)?
        .map(|number| Value::Number(number.into())))
}

fn present(pairs: Vec<(&str, Option<Value>)>) -> Vec<(String, Value)> {
    pairs
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key.to_owned(), value)))
        .collect()
}

fn join_full(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<Option<Vec<(String, Value)>>> {
    let mut stmt = conn.prepare(
        "SELECT s.thread_id, s.agent_provider, s.status, \
                mg.channel_type, mg.platform_id, mg.name, mg.is_group, \
                ag.name, ag.folder, \
                cc.provider, cc.model, cc.assistant_name \
         FROM sessions s \
         LEFT JOIN messaging_groups mg ON mg.id = s.messaging_group_id \
         LEFT JOIN agent_groups ag ON ag.id = s.agent_group_id \
         LEFT JOIN container_configs cc ON cc.agent_group_id = s.agent_group_id \
         WHERE s.id = ?1",
    )?;
    stmt.query_row([session_id], |row| {
        Ok(present(vec![
            ("thread_id", str_column(row, 0)?),
            ("agent_provider", str_column(row, 1)?),
            ("status", str_column(row, 2)?),
            ("channel_type", str_column(row, 3)?),
            ("platform_id", str_column(row, 4)?),
            ("chat_name", str_column(row, 5)?),
            ("is_group", int_column(row, 6)?),
            ("agent_group_name", str_column(row, 7)?),
            ("folder", str_column(row, 8)?),
            ("provider", str_column(row, 9)?),
            ("model", str_column(row, 10)?),
            ("assistant_name", str_column(row, 11)?),
        ]))
    })
    .optional()
}

fn join_minimal(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<Option<Vec<(String, Value)>>> {
    let mut stmt =
        conn.prepare("SELECT thread_id, agent_provider, status FROM sessions WHERE id = ?1")?;
    stmt.query_row([session_id], |row| {
        Ok(present(vec![
            ("thread_id", str_column(row, 0)?),
            ("agent_provider", str_column(row, 1)?),
            ("status", str_column(row, 2)?),
        ]))
    })
    .optional()
}

// --- Providers: opencode composition + codex visible skips (plan 1.4, 3.4) -----

/// User-facing reason a codex-provider session yields no transcript.
const CODEX_SKIP_REASON: &str =
    "codex provider keeps history server-side; pond ingests no transcript for it";

/// The non-claude work enumerated from `data/v2-sessions` + `v2.db`: opencode
/// stores to compose and codex sessions to skip visibly.
struct ProviderWork {
    opencode: Vec<OpencodeProviderSession>,
    codex_skips: Vec<CodexSkip>,
}

/// One nanoclaw session whose opencode store the `opencode` reader composes, with
/// the re-attribution inputs (the group project and the nanoclaw metadata object).
struct OpencodeProviderSession {
    data_dir: PathBuf,
    project: Extracted<String>,
    nanoclaw_options: Value,
}

struct CodexSkip {
    session_id: String,
    agent_group_id: String,
}

/// One session's resolved provider and its group, from the `v2.db` join.
struct ResolvedSession {
    agent_group_id: String,
    provider: Option<String>,
}

/// Enumerate the opencode-provider stores (folder-driven: any session folder with
/// an `opencode-xdg/`) and the codex sessions to skip (metadata-driven: resolved
/// provider is `codex`). Both degrade to empty if `v2.db` is unreadable; opencode
/// composition still runs from the filesystem alone.
fn provider_work(root: &Path) -> ProviderWork {
    let v2 = open_metadata_db(&root.join("data").join("v2.db"));
    let resolved = resolve_providers(v2.as_ref());

    let mut codex_skips: Vec<CodexSkip> = resolved
        .iter()
        .filter(|(_, info)| info.provider.as_deref() == Some("codex"))
        .map(|(session_id, info)| CodexSkip {
            session_id: session_id.clone(),
            agent_group_id: info.agent_group_id.clone(),
        })
        .collect();
    codex_skips.sort_by(|a, b| a.session_id.cmp(&b.session_id));

    let mut opencode = Vec::new();
    for (group_id, session_id, session_path) in walk_session_dirs(root) {
        let Some(data_dir) = opencode_data_dir(&session_path) else {
            continue;
        };
        let Some(project) = extract_self_str(&Value::String(group_id.clone())) else {
            continue;
        };
        let nanoclaw_options = Value::Object(nanoclaw_metadata_object(
            v2.as_ref(),
            &session_id,
            &group_id,
        ));
        opencode.push(OpencodeProviderSession {
            data_dir,
            project,
            nanoclaw_options,
        });
    }

    ProviderWork {
        opencode,
        codex_skips,
    }
}

/// The opencode data dir inside a session's `opencode-xdg/` (OpenCode's XDG data
/// dir): the `opencode/` app subdir when present, else the XDG root itself. `None`
/// when there is no `opencode-xdg/` at all.
fn opencode_data_dir(session_dir: &Path) -> Option<PathBuf> {
    let xdg = session_dir.join("opencode-xdg");
    if !xdg.is_dir() {
        return None;
    }
    let nested = xdg.join("opencode");
    Some(if nested.is_dir() { nested } else { xdg })
}

/// Resolve every session's provider from a shared `v2.db` connection:
/// `sessions.agent_provider`, else the group's `container_configs.provider`
/// (default `claude` is applied by the caller when both are null). Degrades to an
/// empty map when `v2.db` is unreadable or the query errors.
fn resolve_providers(v2: Option<&Connection>) -> HashMap<String, ResolvedSession> {
    let Some(conn) = v2 else {
        return HashMap::new();
    };
    let query = |conn: &Connection| -> rusqlite::Result<HashMap<String, ResolvedSession>> {
        let mut stmt = conn.prepare(
            "SELECT s.id, s.agent_group_id, s.agent_provider, cc.provider \
             FROM sessions s \
             LEFT JOIN container_configs cc ON cc.agent_group_id = s.agent_group_id",
        )?;
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let agent_group_id: String = row.get(1)?;
            let session_provider: Option<String> = row.get(2)?;
            let group_provider: Option<String> = row.get(3)?;
            Ok((id, agent_group_id, session_provider.or(group_provider)))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (id, agent_group_id, provider) = row?;
            map.insert(
                id,
                ResolvedSession {
                    agent_group_id,
                    provider,
                },
            );
        }
        Ok(map)
    };
    query(conn).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    //! Conformance tests for the nanoclaw adapter: fixture-driven claude-JSONL
    //! mapping, queue-operation rule-3 carriers, subagent sidecar sessions, the
    //! read-only metadata join (synthetic DBs from the real DDL), graceful DB
    //! degradation, rotated-file ingestion, and native restore.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::Message};
    use tempfile::TempDir;

    const FIXTURE_ROOT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/nanoclaw"
    );

    const ANON_GROUP: &str = "agentgroup-anon-001";
    const ANON_MAIN: &str = "cc6ea1c9-cab4-43e6-8fdf-7346aae26cbb";
    const ANON_SUB_PARENT: &str = "a03cd144-8240-49e6-9c7d-4bb2f9c20f50";
    const ANON_SUB_HASH: &str = "a02bb3e0917c9a078";

    async fn ingest(root: &Path) -> anyhow::Result<(Store, TempDir)> {
        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = NanoclawAdapter::new(root);
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        Ok((store, store_dir))
    }

    #[test]
    fn is_rotated_and_stem_strip_the_epoch_suffix() {
        assert!(is_rotated_transcript("abc.jsonl.rotated-1777000000000"));
        assert!(!is_rotated_transcript("abc.jsonl"));
        assert!(!is_rotated_transcript("abc.jsonl.rotated-notdigits"));
        assert_eq!(
            sdk_session_stem(Path::new("/x/abc.jsonl.rotated-1777000000000")),
            Some("abc".to_owned()),
        );
        assert_eq!(
            sdk_session_stem(Path::new("/x/abc.jsonl")),
            Some("abc".to_owned())
        );
    }

    /// `probe_default` returns the install root only when it holds
    /// `data/v2-sessions/`; an empty candidate must not masquerade as a source.
    /// (Bespoke, not `assert_probe_default`: that helper asserts the returned
    /// path equals the created subpath, but nanoclaw returns the install root
    /// while requiring the `data/v2-sessions/` subdir - the same reason
    /// openclaw/opencode write bespoke probe tests.)
    #[test]
    fn probe_default_offers_a_root_that_holds_v2_sessions() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let home = temp.path();
        let env = Env::with_home(home);
        assert!(NanoclawFactory.probe_default(&env).is_none());

        let root = home.join("nanoclaw");
        std::fs::create_dir_all(root.join("data").join("v2-sessions"))?;
        let probe = NanoclawFactory.probe_default(&env);
        let got = probe
            .as_ref()
            .and_then(|value| value.get("path"))
            .and_then(Value::as_str);
        assert_eq!(got, root.to_str(), "probe must offer the install root");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fixture_maps_claude_jsonl_with_nanoclaw_identity() -> anyhow::Result<()> {
        let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

        let main = store
            .get_session(ANON_MAIN)
            .await?
            .expect("anon main session ingests");
        assert_eq!(main.session.source_agent, "nanoclaw");
        assert_eq!(
            &*main.session.project, ANON_GROUP,
            "project is the agent group"
        );
        assert_eq!(main.session.parent_session_id, None);
        assert_eq!(
            main.session
                .options
                .get("source")
                .and_then(|s| s.get("adapter")),
            Some(&Value::String("nanoclaw".to_owned())),
        );
        assert_eq!(
            main.session
                .options
                .get("source")
                .and_then(|s| s.get("agent_group_id"))
                .and_then(Value::as_str),
            Some(ANON_GROUP),
        );
        // No SQLite DBs in the fixtures -> metadata join is absent, transcript
        // still ingests (the documented degradation).
        assert!(
            !main.session.options.contains_key("nanoclaw"),
            "metadata absent without the join DBs",
        );

        // queue-operation records (no uuid/message) ride rule 3 as System carriers.
        let saw_queue_carrier = main.messages.iter().any(|stored| {
            matches!(&stored.message, Message::System { content, .. }
                if content.as_deref().is_some_and(|c| c.contains("queue-operation")))
        });
        assert!(
            saw_queue_carrier,
            "queue-operation must survive as a System carrier"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn subagent_sidecar_becomes_its_own_session() -> anyhow::Result<()> {
        let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;
        let child_id = format!("{ANON_SUB_PARENT}/agent-{ANON_SUB_HASH}");
        let child = store
            .get_session(&child_id)
            .await?
            .expect("subagent sidecar surfaces under the derived child id");
        assert_eq!(child.session.source_agent, "nanoclaw/subagent");
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some(ANON_SUB_PARENT)
        );
        assert_eq!(&*child.session.project, ANON_GROUP);
        assert!(
            child.session.options.contains_key("subagent"),
            "subagent metadata must be stored for lossless restore",
        );
        Ok(())
    }

    /// Multi-subagent fan-out from the synthetic fixture: session B spawns 3.
    #[tokio::test(flavor = "multi_thread")]
    async fn synthetic_fan_out_yields_three_subagent_sessions() -> anyhow::Result<()> {
        let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;
        let parent = "bebe90ad-7812-475f-b96c-bd7558d7d8d5";
        for hash in [
            "a0e5a13152ff2a47c",
            "a30e47928e2cef281",
            "aea3786d34dd9e61c",
        ] {
            let child_id = format!("{parent}/agent-{hash}");
            let child = store
                .get_session(&child_id)
                .await?
                .unwrap_or_else(|| panic!("subagent {child_id} must ingest"));
            assert_eq!(child.session.parent_session_id.as_deref(), Some(parent));
        }
        Ok(())
    }

    fn write_transcript(root: &Path, group: &str, uuid: &str) -> PathBuf {
        let dir = root
            .join("data")
            .join("v2-sessions")
            .join(group)
            .join(".claude-shared")
            .join("projects")
            .join("-workspace-agent");
        std::fs::create_dir_all(&dir).unwrap();
        let user = json!({
            "type": "user",
            "uuid": "u-1",
            "sessionId": uuid,
            "cwd": "/workspace/agent",
            "timestamp": "2026-04-27T19:30:03.003Z",
            "message": {"role": "user", "content": "hello nanoclaw"},
        });
        let assistant = json!({
            "type": "assistant",
            "uuid": "a-1",
            "sessionId": uuid,
            "cwd": "/workspace/agent",
            "timestamp": "2026-04-27T19:30:05.000Z",
            "message": {"role": "assistant", "content": [{"type": "text", "text": "hi"}]},
        });
        let path = dir.join(format!("{uuid}.jsonl"));
        std::fs::write(&path, format!("{user}\n{assistant}\n")).unwrap();
        path
    }

    /// v2.db + outbound.db built from the real DDL: the SDK uuid links to the
    /// nanoclaw session, and the join surfaces channel/model in options.nanoclaw.
    #[tokio::test(flavor = "multi_thread")]
    async fn metadata_join_enriches_from_synthetic_dbs() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let group = "grp-A";
        let session_id = "sess-1777000000000-abcdef";
        let sdk = "11111111-1111-1111-1111-111111111111";
        write_transcript(root.path(), group, sdk);

        // outbound.db: the transcript link.
        let sess_dir = root
            .path()
            .join("data")
            .join("v2-sessions")
            .join(group)
            .join(session_id);
        std::fs::create_dir_all(&sess_dir)?;
        let outbound = Connection::open(sess_dir.join("outbound.db"))?;
        outbound.execute_batch(
            "CREATE TABLE session_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);",
        )?;
        outbound.execute(
            "INSERT INTO session_state (key, value, updated_at) VALUES ('continuation:claude', ?1, '2026-04-27T00:00:00Z')",
            [sdk],
        )?;
        drop(outbound);

        // v2.db: the join tables (subset of the real schema).
        let v2 = Connection::open(root.path().join("data").join("v2.db"))?;
        v2.execute_batch(
            "CREATE TABLE agent_groups (id TEXT PRIMARY KEY, name TEXT NOT NULL, folder TEXT NOT NULL, agent_provider TEXT, created_at TEXT NOT NULL);
             CREATE TABLE messaging_groups (id TEXT PRIMARY KEY, channel_type TEXT NOT NULL, platform_id TEXT NOT NULL, instance TEXT NOT NULL, name TEXT, is_group INTEGER DEFAULT 0, created_at TEXT NOT NULL);
             CREATE TABLE container_configs (agent_group_id TEXT PRIMARY KEY, provider TEXT, model TEXT, assistant_name TEXT, updated_at TEXT NOT NULL);
             CREATE TABLE sessions (id TEXT PRIMARY KEY, agent_group_id TEXT NOT NULL, messaging_group_id TEXT, thread_id TEXT, agent_provider TEXT, status TEXT, created_at TEXT NOT NULL);
             INSERT INTO agent_groups VALUES ('ag-1', 'Founder Assistant', 'founder', 'claude', '2026-04-01T00:00:00Z');
             INSERT INTO messaging_groups VALUES ('mg-1', 'telegram', 'tg-123', 'telegram', 'Ops Channel', 1, '2026-04-01T00:00:00Z');
             INSERT INTO container_configs VALUES ('ag-1', 'claude', 'claude-opus-4', 'Nyx', '2026-04-01T00:00:00Z');
             INSERT INTO sessions VALUES ('sess-1777000000000-abcdef', 'ag-1', 'mg-1', 'thread-7', 'claude', 'active', '2026-04-27T00:00:00Z');",
        )?;
        drop(v2);

        let (store, _guard) = ingest(root.path()).await?;
        let session = store.get_session(sdk).await?.expect("session ingests");
        let nanoclaw = session
            .session
            .options
            .get("nanoclaw")
            .expect("metadata join lands in options.nanoclaw");
        assert_eq!(
            nanoclaw.get("nanoclaw_session_id").and_then(Value::as_str),
            Some(session_id)
        );
        assert_eq!(
            nanoclaw.get("channel_type").and_then(Value::as_str),
            Some("telegram")
        );
        assert_eq!(
            nanoclaw.get("chat_name").and_then(Value::as_str),
            Some("Ops Channel")
        );
        assert_eq!(
            nanoclaw.get("model").and_then(Value::as_str),
            Some("claude-opus-4")
        );
        assert_eq!(
            nanoclaw.get("assistant_name").and_then(Value::as_str),
            Some("Nyx")
        );
        assert_eq!(
            nanoclaw.get("agent_group_name").and_then(Value::as_str),
            Some("Founder Assistant")
        );
        assert_eq!(nanoclaw.get("is_group").and_then(Value::as_i64), Some(1));
        Ok(())
    }

    /// An unreadable outbound.db (or absent v2.db) must not block ingestion, and
    /// must never synthesize metadata: the transcript ingests with no join.
    #[tokio::test(flavor = "multi_thread")]
    async fn corrupt_db_degrades_to_metadata_absent() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let group = "grp-B";
        let sdk = "22222222-2222-2222-2222-222222222222";
        write_transcript(root.path(), group, sdk);

        let sess_dir = root
            .path()
            .join("data")
            .join("v2-sessions")
            .join(group)
            .join("sess-broken");
        std::fs::create_dir_all(&sess_dir)?;
        std::fs::write(
            sess_dir.join("outbound.db"),
            b"this is not a sqlite database",
        )?;

        let (store, _guard) = ingest(root.path()).await?;
        let session = store
            .get_session(sdk)
            .await?
            .expect("session still ingests");
        assert!(
            !session.session.options.contains_key("nanoclaw"),
            "an unreadable DB degrades to metadata-absent, never synthesized",
        );
        Ok(())
    }

    /// A rotated `<uuid>.jsonl.rotated-<epochMs>` transcript (immutable, complete)
    /// is walked despite its non-`.jsonl` extension and ingests under the uuid.
    #[tokio::test(flavor = "multi_thread")]
    async fn rotated_transcript_is_ingested() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let dir = root
            .path()
            .join("data")
            .join("v2-sessions")
            .join("grp-C")
            .join(".claude-shared")
            .join("projects")
            .join("-workspace-agent");
        std::fs::create_dir_all(&dir)?;
        let uuid = "33333333-3333-3333-3333-333333333333";
        let row = json!({
            "type": "user",
            "uuid": "u-rot",
            "sessionId": uuid,
            "cwd": "/workspace/agent",
            "timestamp": "2026-04-27T19:30:03.003Z",
            "message": {"role": "user", "content": "rotated"},
        });
        std::fs::write(
            dir.join(format!("{uuid}.jsonl.rotated-1777000000000")),
            format!("{row}\n"),
        )?;

        let (store, _guard) = ingest(root.path()).await?;
        let session = store
            .get_session(uuid)
            .await?
            .expect("rotated transcript ingests under its uuid");
        assert_eq!(session.session.source_agent, "nanoclaw");
        assert!(!session.messages.is_empty());
        Ok(())
    }

    /// The freshness gate: an empty oracle marks everything pending (walk cost
    /// only); a saturated oracle marks every readable-id session fresh. Exercises
    /// `peek_session_id` (filename stem + subagent child id) and `peek_watermark`.
    #[tokio::test(flavor = "multi_thread")]
    async fn plan_classifies_fresh_vs_pending() -> anyhow::Result<()> {
        use crate::adapter::test_support::MaxWatermarkOracle;

        let adapter = NanoclawAdapter::new(FIXTURE_ROOT);
        let first = adapter
            .plan(&crate::adapter::NoopOracle)
            .await?
            .expect("jsonl-tree adapters support plan");
        assert!(first.sessions > 0);
        assert_eq!(first.pending, first.sessions);
        assert_eq!(first.fresh, 0);

        let caught_up = adapter
            .plan(&MaxWatermarkOracle)
            .await?
            .expect("jsonl-tree adapters support plan");
        assert_eq!(caught_up.sessions, first.sessions);
        assert!(caught_up.fresh > 0, "fixture sessions must gate as fresh");
        assert_eq!(caught_up.fresh + caught_up.pending, caught_up.sessions);
        Ok(())
    }

    /// Native restore round-trips a constructed source tree (main + subagent +
    /// a valid `.meta.json`). A minimal tree is built rather than reusing the
    /// committed fixtures: `assert_native_restore` requires the restored set to
    /// equal ALL json/jsonl under the root, and the fixtures' empty `.meta.json`
    /// sidecars are not valid JSON and are not reproduced by serialize.
    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_round_trips_constructed_tree() -> anyhow::Result<()> {
        let source = TempDir::new()?;
        let group = "grp-R";
        let main_uuid = "44444444-4444-4444-4444-444444444444";
        let projects = source
            .path()
            .join("data")
            .join("v2-sessions")
            .join(group)
            .join(".claude-shared")
            .join("projects")
            .join("-workspace-agent");
        std::fs::create_dir_all(&projects)?;

        let main_user = json!({
            "type": "user",
            "uuid": "u-main",
            "sessionId": main_uuid,
            "cwd": "/workspace/agent",
            "timestamp": "2026-04-27T19:30:03.003Z",
            "message": {"role": "user", "content": "restore me"},
        });
        let main_assistant = json!({
            "type": "assistant",
            "uuid": "a-main",
            "sessionId": main_uuid,
            "cwd": "/workspace/agent",
            "timestamp": "2026-04-27T19:30:05.000Z",
            "message": {"role": "assistant", "content": [{"type": "text", "text": "done"}]},
        });
        std::fs::write(
            projects.join(format!("{main_uuid}.jsonl")),
            format!("{main_user}\n{main_assistant}\n"),
        )?;

        let sub_hash = "a0deadbeef00cafe1";
        let sub_dir = projects.join(main_uuid).join("subagents");
        std::fs::create_dir_all(&sub_dir)?;
        let sub_row = json!({
            "type": "user",
            "uuid": "u-sub",
            "sessionId": main_uuid,
            "cwd": "/workspace/agent",
            "isSidechain": true,
            "timestamp": "2026-04-27T19:31:00.000Z",
            "message": {"role": "user", "content": "subagent prompt"},
        });
        std::fs::write(
            sub_dir.join(format!("agent-{sub_hash}.jsonl")),
            format!("{sub_row}\n"),
        )?;
        // A VALID meta.json object so serialize reproduces it (empty is invalid).
        std::fs::write(
            sub_dir.join(format!("agent-{sub_hash}.meta.json")),
            r#"{"agentType":"researcher","description":"dig"}"#,
        )?;

        let adapter = NanoclawAdapter::new(source.path());
        crate::adapter::test_support::assert_native_restore(
            &NanoclawFactory,
            &adapter,
            source.path(),
        )
        .await
    }
}
