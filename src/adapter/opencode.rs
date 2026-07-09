//! opencode adapter (github.com/sst/opencode).
//!
//! Unlike claude-code and codex-cli, opencode does not store one JSONL file per
//! session. It uses a content-addressed split layout under a `storage/` root:
//!
//! - `session/<projectID>/<sessionID>.json` - session metadata
//! - `message/<sessionID>/<messageID>.json` - one message header (no content)
//! - `part/<messageID>/<partID>.json`        - one content part
//!
//! So this is the first adapter to drive the [`Adapter`] seam directly rather
//! than through the JSONL helper: it walks the three levels, sorts by id (the
//! ids are lexically time-sortable, so filename order is creation order), and
//! emits `Session -> Message -> Parts` per session.
//!
//! opencode fuses a tool call and its result into one `tool` part on the
//! assistant message. Canonical keeps the two apart (a `tool_result` on an
//! assistant message is a category error, spec.md#model-part-provenance), so the
//! adapter splits it: a `ToolCall` Part stays on the assistant message and a
//! synthetic `Tool` message carries the `ToolResult`. Native restore replays
//! each real part's stored `raw_record` at its original path and skips the
//! synthetic records, so the split is value-complete-lossless.

use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::{
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, compact_json, config_roots,
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
        let roots = config_roots(NAME, &config)?;
        if roots.is_empty() {
            return Err(AdapterError::config(NAME, "missing `path`/`paths`"));
        }
        Ok(Box::new(OpencodeAdapter::with_roots(roots)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = env
            .home
            .join(".local")
            .join("share")
            .join("opencode")
            .join("storage");
        path.exists().then(|| json!({ "path": path }))
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

/// Configured opencode reader, rooted at one or more `storage/` directories.
#[derive(Debug, Clone)]
pub struct OpencodeAdapter {
    roots: Vec<PathBuf>,
}

impl OpencodeAdapter {
    /// Single-root constructor: the common case, and what every existing
    /// test builds against.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            roots: vec![root.into()],
        }
    }

    /// Pools every root into one adapter identity
    /// (spec.md#adapter-multi-root); what `AdapterFactory::open` uses once
    /// config resolves to more than one directory.
    pub fn with_roots(roots: Vec<PathBuf>) -> Self {
        Self { roots }
    }
}

impl Adapter for OpencodeAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let roots = self.roots.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                collect_session_files_multi(&roots).map(|files| files.len())
            })
            .await
            .map_err(join_error)?
        })
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> super::PlanFuture<'a> {
        let roots = self.roots.clone();
        Box::pin(async move {
            // The events_with freshness pre-pass run standalone: the same
            // per-session subtree walk sync's gate pays every run, classified
            // instead of read. On an empty oracle the walks are skipped - a
            // first sync reads everything. A message-less session stays Opaque
            // (never Empty): reading it still ingests its Session row.
            let peek = !oracle.is_empty();
            let heads = tokio::task::spawn_blocking(move || {
                collect_session_files_multi(&roots)?
                    .into_iter()
                    .map(|file| {
                        let watermark = if peek {
                            let walk =
                                walk_session_subtree(&file.root, &file.path, &file.session_id)?;
                            match newest_message_ts(&walk) {
                                Some(ts) => super::SourceWatermark::At(ts),
                                None => super::SourceWatermark::Opaque,
                            }
                        } else {
                            super::SourceWatermark::Opaque
                        };
                        Ok((file.session_id, watermark))
                    })
                    .collect::<Result<Vec<_>, AdapterError>>()
            })
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
        let roots = self.roots.clone();
        Box::pin(stream! {
            let files = {
                let roots = roots.clone();
                tokio::task::spawn_blocking(move || collect_session_files_multi(&roots)).await
            };
            let files = match files {
                Ok(Ok(files)) => files,
                Ok(Err(error)) => { yield Err(error); return; }
                Err(join) => { yield Err(join_error(join)); return; }
            };

            // Subtree walks happen ONLY when the oracle has entries - on a first
            // ingest (or NoopOracle) every walk is wasted work since there's
            // nothing to compare against. The walk also yields the session's
            // newest stored timestamp (last message + its tool parts) for the
            // freshness gate.
            let mut survivors = Vec::with_capacity(files.len());
            for mut file in files {
                if !oracle.is_empty() {
                    let walked = {
                        let root = file.root.clone();
                        let session_path = file.path.clone();
                        let session_id = file.session_id.clone();
                        tokio::task::spawn_blocking(move || {
                            let walk = walk_session_subtree(&root, &session_path, &session_id)?;
                            let last_ts = newest_message_ts(&walk);
                            Ok::<_, AdapterError>((walk, last_ts))
                        })
                        .await
                    };
                    let (walk, last_ts) = match walked {
                        Ok(Ok(pair)) => pair,
                        Ok(Err(error)) => { yield Err(error); return; }
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
                    // Cache the walk so the read pass doesn't re-list the
                    // same dirs (we already paid for them above).
                    file.cached_subtree = Some(walk);
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
fn join_error(join: tokio::task::JoinError) -> AdapterError {
    AdapterError::io(
        NAME,
        "blocking read task",
        std::io::Error::other(join.to_string()),
    )
}

/// One session file located on disk. `root` is the storage root it was found
/// under - needed downstream to locate its `message/`/`part/` subtree once a
/// multi-root adapter can no longer assume "the" root. `cached_subtree` is
/// populated only when the freshness pre-walk happened (i.e. the oracle had
/// a watermark for this session); the read pass reuses the listings instead
/// of re-walking.
struct SessionFile {
    root: PathBuf,
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
                root: root.to_owned(),
                session_id,
                path,
                cached_subtree: None,
            });
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// [`collect_session_files`] over every root, concatenated and re-sorted for
/// deterministic order independent of root order. A root that doesn't exist
/// is already tolerated by `collect_session_files` itself (its `session/`
/// read_dir NotFound arm returns an empty list), so no separate skip logic
/// is needed here - unlike the JSONL-tree walk, this reader never errors on
/// a missing directory in the first place.
fn collect_session_files_multi(roots: &[PathBuf]) -> Result<Vec<SessionFile>, AdapterError> {
    let mut out = Vec::new();
    for root in roots {
        out.extend(collect_session_files(root)?);
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Walk one session's full subtree: the message files under `message/<sid>/` and
/// every part file under `part/<mid>/`, returning the listings so the read pass
/// can reuse them. `session_path` is unused now that freshness keys on the latest
/// message id rather than a subtree mtime.
fn walk_session_subtree(
    root: &Path,
    _session_path: &Path,
    session_id: &str,
) -> Result<SubtreeWalk, AdapterError> {
    let message_dir = root.join("message").join(session_id);
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
        let part_dir = root.join("part").join(message_id);
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

fn read_sessions(
    sessions: Vec<SessionFile>,
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
            let message_dir = file.root.join("message").join(&session_id);
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
            let part_dir = file.root.join("part").join(message_id);
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
    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/opencode/storage"
    );
    const FRESH_SESSION_ID: &str = "ses_6405e5a5cffeIG2QHRuTmm4mA7";
    const FRESH_MESSAGE_ID: &str = "msg_zzzzfresh0001";
    const FRESH_PART_ID: &str = "prt_zzzzfresh0001";

    struct FixedOracle {
        session_id: &'static str,
        watermark_micros: i64,
    }

    impl crate::adapter::SkipOracle for FixedOracle {
        fn session_max_ts(&self, session_id: &str) -> Option<i64> {
            (session_id == self.session_id).then_some(self.watermark_micros)
        }
    }

    #[test]
    fn probe_default_finds_opencode_storage_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &OpencodeFactory,
            &[".local", "share", "opencode", "storage"],
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_fixture_corpus() -> anyhow::Result<()> {
        let adapter = OpencodeAdapter::new(FIXTURES);
        crate::adapter::test_support::assert_native_restore(
            &OpencodeFactory,
            &adapter,
            // opencode relative paths are rooted at the `storage/` dir.
            std::path::Path::new(FIXTURES),
        )
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
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = OpencodeAdapter::new(FIXTURES);

        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert!(summary.accepted() > 0, "ingest must accept rows");
        assert_eq!(summary.dropped_events, 0, "no per-event drops expected");
        assert_eq!(
            summary.dropped_sessions, 0,
            "no session-level rejections expected"
        );

        let (sessions, messages, parts) = store.row_counts().await?;
        assert!(sessions > 0, "at least one opencode session");
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
            assert_eq!(session.session.source_agent, NAME);
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
        let adapter = OpencodeAdapter::new(FIXTURES);
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
}
