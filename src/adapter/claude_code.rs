//! Claude Code CLI adapter.
//!
//! Source path: `~/.claude/projects/<encoded-project-path>/<session-uuid>.jsonl`.
//! Each `.jsonl` file is one session; lines are typed entries linked via a
//! `parentUuid` -> `uuid` chain. Tool results arrive as `user` entries whose
//! `message.content[]` contains `tool_result` blocks with a parallel
//! `toolUseResult` field carrying structured data.

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use crate::{
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, by_timestamp_then_id, compact_json, config_path,
    empty_options,
    extract::{
        Extracted, Source, extract_compact_repr, extract_raw_record, extract_self_str, extract_str,
    },
    extracted_text,
    jsonl::{
        BoundedRow, JsonlTree, TAIL_CAP, jsonl_tree_discover, jsonl_tree_events, peek_last_mapped,
        source_line,
    },
    jsonl_bytes, part_id, part_ordinal, raw_record,
};

/// Per-file streaming state that persists across rows of one JSONL file.
/// Lives inside [`Adapter::events`]'s per-file loop and is reset whenever
/// the loop advances to the next file.
///
/// Two responsibilities:
///
/// 1. **Replay dedup.** Claude Code's `/resume` and `/compact` paths
///    occasionally re-emit byte-identical rows with the same `uuid` (the
///    stale-`messageSet`-cache bug in claude-code, see
///    `utils/sessionStorage.ts`). The adapter dedupes only byte-identical
///    replays; same-uuid/different-content reaches the validator visibly
///    (spec.md#adapter-integrity-dedup).
///
/// 2. **`tool_use_id -> tool name` resolution.** The raw `tool_result` row
///    carries only `tool_use_id`, not the tool name; the name lives on the
///    prior `tool_use` row in the same file. We populate this map when we
///    see a `tool_use` part, then look it up when we see the matching
///    `tool_result` part. Misses (e.g. compaction pruned the tool_use)
///    surface as `name: None` in `PartKind::ToolResult` rather than the
///    old `"unknown"` sentinel - faithful to the source rather than
///    inventing a value.
#[derive(Debug, Default)]
pub(crate) struct FileState {
    seen_records: HashSet<(String, u64)>,
    tool_call_names: HashMap<String, Extracted<String>>,
}

/// Stable adapter name. Surfaces as the `[adapters.claude-code]` config key,
/// the `pond sync claude-code` CLI arg, and `Session.source_agent` on every
/// emitted row.
const NAME: &str = "claude-code";

/// Stateless factory: opens [`ClaudeCodeAdapter`] instances from config and
/// probes for the canonical install location under `~/.claude/projects`.
pub struct ClaudeCodeFactory;

impl AdapterFactory for ClaudeCodeFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(ClaudeCodeAdapter::new(config_path(NAME, config)?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = env.home.join(".claude").join("projects");
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

fn serialize_session(
    session: &crate::sessions::SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    let mut messages = session.messages.clone();
    if fidelity == RestoreFidelity::Native {
        messages.sort_by(|left, right| {
            source_line(left.message.options())
                .cmp(&source_line(right.message.options()))
                .then_with(|| by_timestamp_then_id(left, right))
        });
    } else {
        messages.sort_by(by_timestamp_then_id);
    }
    // Native replays the verbatim `options.source.raw_record`; `claude_record`
    // below is foreign-only. Replay echoes a frozen snapshot - safe only while
    // canonical is append-only (spec.md#adapter-integrity-additive-sync).
    let mut records = Vec::with_capacity(messages.len());
    let mut parent_uuid = None::<String>;
    for message in &messages {
        if fidelity == RestoreFidelity::Native
            && let Some(raw) = raw_record(message.message.options())
        {
            parent_uuid = raw
                .get("uuid")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or(parent_uuid);
            records.push(raw);
            continue;
        }
        // `claude_record` returns `None` for a dropped System message;
        // `parent_uuid` then stays put so the chain skips over the gap.
        let Some(record) = claude_record(session, message, parent_uuid.as_deref()) else {
            continue;
        };
        parent_uuid = record
            .get("uuid")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        records.push(record);
    }

    let mut files = vec![RestoredFile::new(
        claude_relative_path(session),
        jsonl_bytes(NAME, &records)?,
        fidelity,
    )];
    if session.session.parent_session_id.is_some()
        && let Some(meta) = subagent_meta_record(session)
    {
        let mut meta_path = files[0].relative_path.clone();
        meta_path.set_extension("meta.json");
        files.push(RestoredFile::new(
            meta_path,
            serde_json::to_vec(&meta).map_err(|err| {
                AdapterError::schema(
                    NAME,
                    &session.session.id,
                    format!("json encode failed: {err}"),
                )
            })?,
            fidelity,
        ));
    }
    Ok(files)
}

fn claude_relative_path(session: &crate::sessions::SessionWithMessages) -> PathBuf {
    let encoded_project = session
        .session
        .options
        .get("source")
        .and_then(|source| source.get("project_dir"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| encode_project(&session.session.project));
    if let Some(parent) = &session.session.parent_session_id {
        // The child id is `<parent>/<child_suffix>`; the suffix is the file's
        // path under `subagents/` (`agent-<hash>` flat, or
        // `workflows/<wf-id>/agent-<hash>` nested), so stripping the parent
        // prefix reconstructs the on-disk path verbatim.
        let child_suffix = session
            .session
            .id
            .strip_prefix(&format!("{parent}/"))
            .unwrap_or(&session.session.id);
        return PathBuf::from(encoded_project)
            .join(parent)
            .join("subagents")
            .join(format!("{child_suffix}.jsonl"));
    }
    PathBuf::from(encoded_project).join(format!("{}.jsonl", session.session.id))
}

fn encode_project(project: &str) -> String {
    project.replace(['/', '.'], "-")
}

fn subagent_meta_record(session: &crate::sessions::SessionWithMessages) -> Option<Value> {
    // Restore the sidecar `.meta.json` verbatim from the stored copy. A
    // subagent ingested without a meta file stored `meta: null` - nothing
    // to write back.
    let meta = session.session.options.get("subagent")?.get("meta")?;
    meta.is_object().then(|| meta.clone())
}

fn claude_record(
    session: &crate::sessions::SessionWithMessages,
    message: &crate::sessions::MessageWithParts,
    parent_uuid: Option<&str>,
) -> Option<Value> {
    // Foreign restore into Claude Code (native restore re-emits the stored
    // `raw_record` and never reaches here). Claude Code's transcript has only
    // `user` and `assistant` rows: a tool result is a `user` row, and there
    // is no in-transcript system turn - a System message (a rule-3 carrier or
    // a source's own system/developer turn) has no idiomatic home and is
    // dropped; the content stays in canonical (spec.md#adapter-native-restore-lossless,
    // foreign clause).
    let row_role = match &message.message {
        Message::System { .. } => return None,
        Message::User { .. } | Message::Tool { .. } => "user",
        Message::Assistant { .. } => "assistant",
    };
    let mut envelope = serde_json::Map::new();
    envelope.insert("role".to_owned(), Value::String(row_role.to_owned()));
    if row_role == "assistant" {
        // `type:"message"` is the Anthropic Messages API object discriminator
        // - a constant, always present on a real assistant row.
        envelope.insert("type".to_owned(), Value::String("message".to_owned()));
    }
    envelope.insert(
        "content".to_owned(),
        Value::Array(message.parts.iter().map(claude_part).collect()),
    );
    Some(json!({
        "parentUuid": parent_uuid,
        "isSidechain": false,
        "userType": "external",
        "cwd": &*session.session.project,
        "sessionId": &session.session.id,
        "type": row_role,
        "message": Value::Object(envelope),
        "uuid": message.message.id(),
        "timestamp": message.message.timestamp().to_rfc3339_opts(SecondsFormat::Millis, true),
    }))
}

fn claude_part(part: &Part) -> Value {
    match &part.kind {
        PartKind::Text { text } => json!({"type": "text", "text": extracted_text(text)}),
        PartKind::Reasoning { text } => {
            json!({"type": "thinking", "thinking": extracted_text(text)})
        }
        PartKind::ToolCall {
            call_id,
            name,
            params,
            provider_executed,
        } => json!({
            "type": if *provider_executed { "server_tool_use" } else { "tool_use" },
            "id": extracted_text(call_id),
            "name": extracted_text(name),
            "input": params,
        }),
        PartKind::ToolResult {
            call_id,
            is_failure,
            result,
            ..
        } => json!({
            "type": "tool_result",
            "tool_use_id": extracted_text(call_id),
            "is_error": is_failure,
            "content": result,
        }),
        PartKind::File {
            media_type,
            file_name,
            data,
        } => json!({
            "type": "file",
            "media_type": media_type,
            "file_name": file_name,
            "source": file_source(data),
        }),
        other => {
            json!({"type": "text", "text": compact_json(&serde_json::to_value(other).unwrap_or(Value::Null))})
        }
    }
}

fn file_source(data: &FileData) -> Value {
    match data {
        FileData::String(value) => json!({"type": "text", "data": value}),
        FileData::Bytes(value) => json!({"type": "base64", "data": value}),
        FileData::Url(value) => json!({"type": "url", "url": value}),
    }
}

/// Configured claude-code reader. Walks a tree of `*.jsonl` files under
/// [`Self::root`] and yields canonical events in source order per session.
#[derive(Debug, Clone)]
pub struct ClaudeCodeAdapter {
    root: PathBuf,
}

impl ClaudeCodeAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for ClaudeCodeAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        jsonl_tree_discover(self)
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        jsonl_tree_events(self, oracle)
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> crate::adapter::PlanFuture<'a> {
        crate::adapter::jsonl::jsonl_tree_plan(self, oracle)
    }
}

impl JsonlTree for ClaudeCodeAdapter {
    type State = FileState;

    fn name(&self) -> &'static str {
        NAME
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn peek_session_id(&self, path: &Path, first_line: &str) -> Option<String> {
        // A file under `subagents/` takes its id from the path, never from the
        // row's content `sessionId` (that's the parent's). A recognized child
        // peeks to its child id; an unrecognized one returns `None` so it stays
        // out of the freshness gate and its `unsupported_reason` failure
        // re-surfaces on every sync rather than being skipped as `Fresh` under
        // the parent's borrowed watermark. See spec.md#datasets.
        if subagents_dir(path).is_some() {
            let (parent_uuid, child_suffix, _) = subagent_ids(path)?;
            return Some(format!("{parent_uuid}/{child_suffix}"));
        }
        let row: Value = serde_json::from_str(first_line).ok()?;
        row.get("sessionId")?.as_str().map(ToOwned::to_owned)
    }

    fn peek_watermark(&self, path: &Path) -> crate::adapter::SourceWatermark {
        // Claude Code appends trailing metadata rows (`last-prompt`,
        // `permission-mode`, `bridge-session`, ...) with no timestamp after the
        // conversation, so the literal last line is usually not a message. Walk
        // back to the latest row that carries a timestamp - the real watermark.
        // Taking only the last line stranded ~2k sessions perpetually un-fresh,
        // re-decoding ~1.2M already-stored rows every sync.
        if let Some(ts) = peek_last_mapped(path, |line| {
            let row: Value = serde_json::from_str(line).ok()?;
            Some(parse_timestamp(&row).ok()?.timestamp_micros())
        }) {
            return crate::adapter::SourceWatermark::At(ts);
        }
        // No timestamped row found. When the scan covered the WHOLE file
        // (len <= TAIL_CAP) that is a proof of nothing ingestible: session
        // anchoring runs the same `parse_timestamp` over the same lines
        // (`session_from_rows`), so this file cannot anchor a session and
        // ingests nothing - the invariant is locked by
        // `keyless_file_peeks_empty_and_ingests_nothing`. A larger file's scan
        // is a window, not a proof, so it stays opaque and re-reads.
        match std::fs::metadata(path) {
            Ok(meta) if meta.len() <= TAIL_CAP => crate::adapter::SourceWatermark::Empty,
            _ => crate::adapter::SourceWatermark::Opaque,
        }
    }

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
        session_from_rows(path, rows)
    }

    fn events_from_row(
        &self,
        session: &Session,
        row: &BoundedRow,
        state: &mut Self::State,
    ) -> Result<Vec<IngestEvent>, String> {
        if let Some(uuid) = row.value.get("uuid").and_then(Value::as_str)
            && !state
                .seen_records
                .insert((uuid.to_owned(), source_record_hash(&row.value)))
        {
            return Ok(Vec::new());
        }
        capture_tool_call_names(&row.value, &mut state.tool_call_names);
        events_from_row(&session.id, row.line, &row.value, session.created_at, state)
    }

    fn unsupported_reason(&self, path: &Path) -> Option<String> {
        // A `.jsonl` under a `subagents/` ancestor that we can't resolve to a
        // child id (its leaf isn't `agent-<hash>.jsonl`) must NOT fall back to
        // its content `sessionId` - that id is the parent's, so it would
        // silently merge into the parent session. Fail visibly and wait for an
        // adapter update instead. The Workflow runner's `journal.jsonl` never
        // reaches this check - `skip_source` excludes it from the walk - and if
        // it ever did, a visible skip is the safe answer. See spec.md#datasets.
        if subagents_dir(path).is_some() && subagent_ids(path).is_none() {
            return Some(format!(
                "{}: subagent transcript layout not recognized by this pond version; \
                 skipped so it is not merged into the parent session - update pond and \
                 re-run `pond sync`",
                path.display()
            ));
        }
        None
    }

    fn skip_source(&self, path: &Path) -> bool {
        is_workflow_control_file(path)
    }
}

// spec.md#adapter-integrity-dedup: hash only semantic fields so noise-field
// replays (timestamp, requestId, isMeta, gitBranch, version, ...) dedupe;
// real content diffs still reach the validator.
fn source_record_hash(value: &Value) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let pick = |path: &[&str]| -> &Value {
        let mut cur = value;
        for key in path {
            match cur.get(*key) {
                Some(next) => cur = next,
                None => return &Value::Null,
            }
        }
        cur
    };
    for path in [
        &["type"][..],
        &["parentUuid"][..],
        &["message", "role"][..],
        &["message", "content"][..],
        &["toolUseResult"][..],
    ] {
        compact_json(pick(path)).hash(&mut hasher);
    }
    hasher.finish()
}

/// The Workflow runner writes `journal.jsonl` (its resume/cache journal of agent
/// `started`/`result` events) beside the `agent-<hash>.jsonl` transcripts under
/// `subagents/workflows/<wf-id>/`. It carries no `sessionId` and only duplicates
/// content already in those transcripts, so it is a control file excluded from
/// the walk outright (`skip_source`): never a source, never read, never pending.
/// One accumulates per Workflow run, and none can ever earn a freshness key -
/// left in the walk they'd grow `pond status`'s pending count without bound.
/// See spec.md#datasets.
fn is_workflow_control_file(path: &Path) -> bool {
    subagents_dir(path).is_some()
        && path.file_name().and_then(|n| n.to_str()) == Some("journal.jsonl")
}

/// Walk one raw row's `message.content[]` array (if any) and stash every
/// `tool_use` part's `id -> name` mapping into the per-file map. Idempotent
/// and safe to call on every row regardless of role; non-assistant rows
/// just don't contribute entries.
fn capture_tool_call_names(row: &Value, map: &mut HashMap<String, Extracted<String>>) {
    let Some(items) = row
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return;
    };
    for item in items {
        let kind = item.get("type").and_then(Value::as_str);
        if !matches!(kind, Some("tool_use") | Some("server_tool_use")) {
            continue;
        }
        let (Some(id), Some(name)) = (item.str_field("id"), extract_str(item, "name")) else {
            continue;
        };
        map.insert(id.to_owned(), name);
    }
}

fn session_from_rows(path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
    let path_display = path.display().to_string();
    // A non-agent leaf under `subagents/` (e.g. the Workflow runner's
    // journal.jsonl) would borrow the parent's content `sessionId` and silently
    // merge; refuse structurally rather than rely on the row lacking one.
    // spec.md#datasets.
    if subagents_dir(path).is_some() && subagent_ids(path).is_none() {
        return Err(AdapterError::schema(
            NAME,
            path_display,
            "sidecar/control file under subagents/ has no session of its own",
        ));
    }
    let mut created_at = None;
    let mut project: Option<Extracted<String>> = None;
    let mut version = None;
    for row in rows {
        if created_at.is_none() {
            created_at = parse_timestamp(&row.value).ok();
        }
        if project.is_none() {
            project = extract_str(&row.value, "cwd");
        }
        if version.is_none() {
            version = row
                .value
                .get("version")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
    }

    let first = rows
        .first()
        .ok_or_else(|| AdapterError::schema(NAME, path_display.clone(), "empty jsonl session"))?;
    let at_first = format!("{path_display}:{}", first.line);
    // A forked subagent transcript (Claude Code >= 2.1.117 `/fork`) opens with a
    // `fork-context-ref` header row that carries no `sessionId` - the id first
    // appears on the following message row. Scan for it rather than demanding it
    // on record 0, or the whole transcript is dropped
    // (spec.md#adapter-integrity-no-silent-drops). A subagent derives its id from
    // the path regardless, so it tolerates the id being absent entirely; only a
    // top-level session (below) genuinely requires one.
    let raw_session_id = rows
        .iter()
        .find_map(|row| row.value.get("sessionId").and_then(Value::as_str))
        .map(ToOwned::to_owned);
    let created_at = created_at.ok_or_else(|| {
        AdapterError::schema(NAME, at_first.clone(), "session has no parseable timestamp")
    })?;

    // Subagent detection. Claude Code stores each subagent's transcript under
    // the session's `subagents/` sidecar - either flat
    // (`<parent_dir>/<parent_uuid>/subagents/agent-<hash>.jsonl`) or, for the
    // workflow runner, nested
    // (`.../subagents/workflows/<wf-id>/agent-<hash>.jsonl`) - with a sibling
    // `agent-<hash>.meta.json` carrying `{agentType, description}`. Every such
    // file shares the parent's `sessionId` in row content, so ingesting it under
    // that id collides with the parent (the validator's "project is immutable"
    // rule rejects a cwd-shifted one, and a same-cwd one silently merges). The
    // fix is to derive a child id from the path - keyed off the `subagents/`
    // ancestor at any depth - and link back via `parent_session_id`. See
    // spec.md#datasets.
    let subagent = subagent_descriptor(path);
    let project_dir = source_project_dir(path, subagent.is_some());
    let (session_id, parent_session_id, source_agent, subagent_options) = match subagent {
        Some(SubagentDescriptor {
            parent_uuid,
            child_suffix,
            agent_hash,
            agent_type,
            meta,
        }) => {
            let child_id = format!("{parent_uuid}/{child_suffix}");
            let agent_label = agent_type
                .as_deref()
                .map(|t| format!("claude-code/{t}"))
                .unwrap_or_else(|| "claude-code/subagent".to_owned());
            // `meta` is the verbatim `.meta.json`; `hash` and `raw_session_id`
            // are pond-derived (filename hash + parent sessionId). Storing the
            // whole meta keeps native restore of the sidecar lossless.
            let metadata = json!({
                "hash": agent_hash,
                "raw_session_id": raw_session_id,
                "meta": meta,
            });
            (child_id, Some(parent_uuid), agent_label, Some(metadata))
        }
        None => {
            // A top-level session has no path-derived id, so it genuinely
            // requires a `sessionId` somewhere in the file.
            let id = raw_session_id.ok_or_else(|| {
                AdapterError::schema(
                    NAME,
                    at_first,
                    format!("line {} missing sessionId", first.line),
                )
            })?;
            (id, None, "claude-code".to_owned(), None)
        }
    };

    let project = match project {
        Some(value) => value,
        None => {
            let decoded = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .map(|s| s.replace('-', "/"))
                .ok_or_else(|| {
                    AdapterError::schema(
                        NAME,
                        path_display.clone(),
                        "no `cwd` field in any row and source path is not UTF-8",
                    )
                })?;
            extract_self_str(&Value::String(decoded)).ok_or_else(|| {
                AdapterError::schema(
                    NAME,
                    path_display.clone(),
                    "internal: Value::String produced None from Source::as_str",
                )
            })?
        }
    };

    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": "claude-code",
            "version": version,
            "project_dir": project_dir,
            "workspace_path": &*project,
        }),
    );
    if let Some(metadata) = subagent_options {
        options.insert("subagent".to_owned(), metadata);
    }

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

fn source_project_dir(path: &Path, is_subagent: bool) -> Option<String> {
    // The project dir is the grandparent of `subagents/` regardless of how
    // deeply the transcript nests below it (`.../<project>/<parent_uuid>/
    // subagents/...`), so climb from the `subagents/` ancestor rather than a
    // fixed number of `.parent()` hops.
    let project_dir = if is_subagent {
        subagents_dir(path)?.parent()?.parent()
    } else {
        path.parent()
    };
    project_dir
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .map(ToOwned::to_owned)
}

/// The `subagents/` directory in `path`'s ancestry, if any. Depth-independent:
/// matches both the flat `<parent_uuid>/subagents/agent-<hash>.jsonl` and the
/// nested workflow `<parent_uuid>/subagents/workflows/<wf-id>/agent-<hash>.jsonl`
/// layouts. The directory directly above it is the parent session uuid.
fn subagents_dir(path: &Path) -> Option<&Path> {
    let mut cur = path.parent();
    while let Some(dir) = cur {
        if dir.file_name().and_then(|n| n.to_str()) == Some("subagents") {
            return Some(dir);
        }
        cur = dir.parent();
    }
    None
}

/// Resolved metadata for one subagent JSONL file. `agent_type` is read from
/// the sibling `.meta.json` for the `source_agent` label; `meta` keeps that
/// file's full verbatim content so native restore reproduces it
/// (spec.md#adapter-native-restore-lossless). Both are `None` when the meta file is
/// absent or unreadable (the label falls back to `claude-code/subagent`).
struct SubagentDescriptor {
    parent_uuid: String,
    child_suffix: String,
    agent_hash: String,
    agent_type: Option<String>,
    meta: Option<Value>,
}

/// `(parent_uuid, child_suffix, agent_hash)` for a subagent transcript, or
/// `None` for any path without a `subagents/` ancestor or a non-`agent-<hash>`
/// leaf (the common case: top-level session files). `child_suffix` is the file's
/// path relative to its `subagents/` ancestor with `.jsonl` stripped -
/// `agent-<hash>` flat, `workflows/<wf-id>/agent-<hash>` nested - so the derived
/// child id `<parent_uuid>/<child_suffix>` round-trips back to the on-disk path
/// on native restore. `agent_hash` keys the sibling `.meta.json` lookup.
fn subagent_ids(path: &Path) -> Option<(String, String, String)> {
    let file_name = path.file_name()?.to_str()?;
    let agent_hash = file_name
        .strip_prefix("agent-")?
        .strip_suffix(".jsonl")?
        .to_owned();
    let subagents = subagents_dir(path)?;
    let parent_uuid = subagents.parent()?.file_name()?.to_str()?.to_owned();
    // The child id must be `/`-canonical on every platform (the rest of the
    // adapter and `claude_relative_path` assume `/`), but a relative path carries
    // the OS separator - normalize it. No-op on POSIX.
    let child_suffix = path
        .strip_prefix(subagents)
        .ok()?
        .with_extension("")
        .to_str()?
        .replace(std::path::MAIN_SEPARATOR, "/");
    Some((parent_uuid, child_suffix, agent_hash))
}

/// [`subagent_ids`] plus the sibling `agent-<hash>.meta.json` - `agentType` for
/// the `source_agent` label, the whole file for lossless sidecar restore.
fn subagent_descriptor(path: &Path) -> Option<SubagentDescriptor> {
    let (parent_uuid, child_suffix, agent_hash) = subagent_ids(path)?;
    let meta_path = path.parent()?.join(format!("agent-{agent_hash}.meta.json"));
    let (agent_type, meta) = match std::fs::read(&meta_path) {
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => (
                value
                    .get("agentType")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                Some(value),
            ),
            Err(error) => {
                tracing::debug!(
                    target: "pond::adapter::claude_code",
                    meta = %meta_path.display(),
                    %error,
                    "subagent .meta.json present but unparseable; falling back to 'claude-code/subagent'",
                );
                (None, None)
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (None, None),
        Err(error) => {
            tracing::debug!(
                target: "pond::adapter::claude_code",
                meta = %meta_path.display(),
                %error,
                "subagent .meta.json IO error; falling back to 'claude-code/subagent'",
            );
            (None, None)
        }
    };

    Some(SubagentDescriptor {
        parent_uuid,
        child_suffix,
        agent_hash,
        agent_type,
        meta,
    })
}

fn events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
    state: &FileState,
) -> Result<Vec<IngestEvent>, String> {
    let timestamp = parse_timestamp(row).unwrap_or(default_timestamp);
    let uuid = row
        .get("uuid")
        .and_then(Value::as_str)
        .map_or_else(|| format!("{session_id}:{line}"), ToOwned::to_owned);

    if let Some(message_value) = row.get("message") {
        return message_events(
            session_id,
            &uuid,
            timestamp,
            row,
            message_value,
            state,
            line,
        );
    }

    // Rows with no `message` field are session-metadata records:
    // `queue-operation`, `permission-mode`, `last-prompt`, `attachment`,
    // `progress`, `system`, `custom-title`, etc. We preserve them as
    // System messages with the row's compact JSON in `content` so a future
    // exporter could reconstruct the original transcript; the `subtype`
    // becomes the human label via `options.source.raw_type`.
    let raw_type = row.get("type").and_then(Value::as_str);
    let content = if raw_type == Some("attachment") {
        row.get("attachment")
            .and_then(attachment_content)
            .or_else(|| Some(extract_compact_repr(row)))
    } else {
        extract_str(row, "subtype").or_else(|| extract_str(row, "type"))
    };
    let message = Message::System {
        id: uuid,
        session_id: session_id.to_owned(),
        timestamp,
        content,
        options: row_options(row, line),
    };
    Ok(vec![IngestEvent::Message(message)])
}

fn message_events(
    session_id: &str,
    uuid: &str,
    timestamp: DateTime<Utc>,
    row: &Value,
    message_value: &Value,
    state: &FileState,
    line: usize,
) -> Result<Vec<IngestEvent>, String> {
    let role = message_value
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| "message missing role".to_owned())?;
    let content = message_value.get("content").unwrap_or(&Value::Null);
    let mut parts = Vec::new();
    let message = match (role, content) {
        ("user", Value::String(text)) => {
            // spec.md#model-part-provenance: a user-slot turn is conversation only
            // when it is a genuine human prompt; harness-injected wrappers and
            // `isMeta` rows are scaffolding.
            let provenance = user_text_provenance(row, text);
            parts.push(text_part(
                session_id,
                uuid,
                0,
                extract_self_str(content),
                provenance,
            ));
            Message::User {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            }
        }
        ("user", Value::Array(items)) if items.iter().all(is_tool_result) => {
            let source_tool_result = row.get("toolUseResult").cloned();
            parts.extend(items.iter().enumerate().map(|(ordinal, item)| {
                tool_result_part(
                    session_id,
                    uuid,
                    ordinal,
                    item,
                    source_tool_result.as_ref(),
                    state,
                )
            }));
            Message::Tool {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            }
        }
        ("user", Value::Array(items)) => {
            // Classify the whole user message once: v1 claude-code never mixes
            // provenance within a single message (spec.md#model-part-provenance).
            let provenance = user_array_provenance(row, items);
            parts.extend(items.iter().enumerate().map(|(ordinal, item)| {
                user_part(session_id, uuid, ordinal, item, state, provenance)
            }));
            Message::User {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            }
        }
        ("assistant", Value::Array(items)) => {
            parts.extend(
                items
                    .iter()
                    .enumerate()
                    .map(|(ordinal, item)| assistant_part(session_id, uuid, ordinal, item)),
            );
            Message::Assistant {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: assistant_options(row, message_value, line),
            }
        }
        ("system", Value::String(_)) => Message::System {
            id: uuid.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            content: extract_self_str(content),
            options: row_options(row, line),
        },
        ("system", _) => Message::System {
            id: uuid.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            // Fallback for system messages without a string content: serialize
            // the structured body as JSON. This is not a synthesized value
            // (the row genuinely had this content), just a lossless string
            // encoding of structured data.
            content: Some(extract_compact_repr(message_value)),
            options: row_options(row, line),
        },
        // spec.md#adapters rule-3: a record that maps to no typed Message is
        // carried whole as a system-role Message, not rejected, so an unknown or
        // future role stays lossless (the full row lives in options.raw_record).
        _ => Message::System {
            id: uuid.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            content: Some(extract_compact_repr(message_value)),
            options: row_options(row, line),
        },
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
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
        options: empty_options(),
        kind: PartKind::Text { text },
    }
}

fn user_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    value: &Value,
    state: &FileState,
    provenance: Provenance,
) -> Part {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(
            session_id,
            message_id,
            ordinal,
            extract_str(value, "text"),
            provenance,
        ),
        Some("image") | Some("file") => {
            file_part(session_id, message_id, ordinal, value, provenance)
        }
        Some("tool_result") => {
            tool_result_part(session_id, message_id, ordinal, value, None, state)
        }
        // Unknown user part shapes: preserve the raw JSON in the Text slot
        // rather than dropping. This is not a synthesized value - it's a
        // lossless encoding of structured data the schema doesn't model.
        _ => text_part(
            session_id,
            message_id,
            ordinal,
            Some(extract_compact_repr(value)),
            provenance,
        ),
    }
}

fn assistant_part(session_id: &str, message_id: &str, ordinal: usize, value: &Value) -> Part {
    // spec.md#model-part-provenance: assistant content - text, reasoning, tool calls -
    // is model-authored, hence conversational. `tool_result` parts never appear
    // on an assistant message.
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(
            session_id,
            message_id,
            ordinal,
            extract_str(value, "text"),
            Provenance::Conversational,
        ),
        Some("thinking") => Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance: Provenance::Conversational,
            options: signature_options(value),
            kind: PartKind::Reasoning {
                text: extract_str(value, "thinking"),
            },
        },
        Some("tool_use") => Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance: Provenance::Conversational,
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: extract_str(value, "id"),
                name: extract_str(value, "name"),
                params: value.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: false,
            },
        },
        Some("server_tool_use") => Part {
            session_id: session_id.to_owned(),
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: part_ordinal(ordinal),
            provenance: Provenance::Conversational,
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: extract_str(value, "id"),
                name: extract_str(value, "name"),
                params: value.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: true,
            },
        },
        Some("image") | Some("file") => file_part(
            session_id,
            message_id,
            ordinal,
            value,
            Provenance::Conversational,
        ),
        // Same rationale as `user_part`'s fallback: lossless encoding of
        // an unrecognised structured shape, not synthesised data.
        _ => text_part(
            session_id,
            message_id,
            ordinal,
            Some(extract_compact_repr(value)),
            Provenance::Conversational,
        ),
    }
}

fn tool_result_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    value: &Value,
    source_tool_result: Option<&Value>,
    state: &FileState,
) -> Part {
    let call_id = extract_str(value, "tool_use_id");
    // `tool_result` source rows don't carry the tool name; it's resolved
    // via the per-file `tool_use_id -> name` map. Misses (compaction pruned
    // the originating `tool_use`) surface as `None` per spec.md#model-no-synthesis
    // (schema-honesty: the field is `Option<Extracted<T>>`, not a fabricated
    // string).
    let name = value
        .str_field("tool_use_id")
        .and_then(|id| state.tool_call_names.get(id))
        .cloned();
    let result = value
        .get("content")
        .cloned()
        .or_else(|| source_tool_result.cloned())
        .unwrap_or(Value::Null);
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        // spec.md#model-part-provenance: tool output is runtime-produced, not
        // conversation.
        provenance: Provenance::Injected,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id,
            name,
            is_failure: value
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            result,
        },
    }
}

fn file_part(
    session_id: &str,
    message_id: &str,
    ordinal: usize,
    value: &Value,
    provenance: Provenance,
) -> Part {
    let media_type = value
        .get("media_type")
        .or_else(|| value.get("mime_type"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let file_name = value
        .get("file_name")
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let data = if let Some(source) = value.get("source") {
        if let Some(url) = source.get("url").and_then(Value::as_str) {
            FileData::Url(url.to_owned())
        } else if let Some(bytes) = source.get("data").and_then(Value::as_str) {
            FileData::String(bytes.to_owned())
        } else {
            FileData::String(compact_json(source))
        }
    } else if let Some(url) = value.get("url").and_then(Value::as_str) {
        FileData::Url(url.to_owned())
    } else {
        FileData::String(compact_json(value))
    };

    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        provenance,
        options: empty_options(),
        kind: PartKind::File {
            media_type,
            file_name,
            data,
        },
    }
}

fn row_options(row: &Value, line: usize) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    let source = json!({
        "line": line,
        "parent_uuid": row.get("parentUuid"),
        "is_sidechain": row.get("isSidechain"),
        "user_type": row.get("userType"),
        "entrypoint": row.get("entrypoint"),
        "cwd": row.get("cwd"),
        "version": row.get("version"),
        "git_branch": row.get("gitBranch"),
        "request_id": row.get("requestId"),
        "raw_type": row.get("type"),
        "raw_record": extract_raw_record(row),
    });
    options.insert("source".to_owned(), source);
    options
}

fn assistant_options(row: &Value, message_value: &Value, line: usize) -> ProviderOptions {
    let mut options = row_options(row, line);
    let anthropic = json!({
        "id": message_value.get("id"),
        "model": message_value.get("model"),
        "stop_reason": message_value.get("stop_reason"),
        "stop_sequence": message_value.get("stop_sequence"),
        "usage": message_value.get("usage"),
    });
    options.insert("anthropic".to_owned(), anthropic);
    options
}

fn signature_options(value: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    if let Some(signature) = value.get("signature").and_then(Value::as_str) {
        options.insert("anthropic".to_owned(), json!({"signature": signature}));
    }
    options
}

fn attachment_content(value: &Value) -> Option<Extracted<String>> {
    extract_str(value, "content").or_else(|| extract_str(value, "stdout"))
}

fn parse_timestamp(value: &Value) -> anyhow::Result<DateTime<Utc>> {
    let timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .context("missing timestamp")?;
    Ok(DateTime::parse_from_rfc3339(timestamp)
        .context("invalid timestamp")?
        .with_timezone(&Utc))
}

fn is_tool_result(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("tool_result")
}

/// True when the row carries `isMeta: true` - claude-code's marker for an
/// expanded skill or command body injected into a user slot.
fn is_meta_row(row: &Value) -> bool {
    row.get("isMeta").and_then(Value::as_bool) == Some(true)
}

/// Harness-injected wrappers claude-code places inside a user-slot turn
/// (spec.md#model-part-provenance): task notifications, slash-command echoes,
/// local-command caveats, interrupt notices.
fn is_injected_user_text(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("<task-notification>")
        || trimmed.starts_with("<command-name>")
        || trimmed.starts_with("<command-message>")
        || trimmed.starts_with("<command-args>")
        || trimmed.starts_with("<local-command-caveat>")
        || trimmed.starts_with("<local-command-stdout>")
        || trimmed.starts_with("[Request interrupted by user")
}

/// Provenance of a string-content user message: `injected` for an `isMeta`
/// row or a harness wrapper, `conversational` for a genuine human prompt.
fn user_text_provenance(row: &Value, text: &str) -> Provenance {
    if is_meta_row(row) || is_injected_user_text(text) {
        Provenance::Injected
    } else {
        Provenance::Conversational
    }
}

/// Provenance of an array-content user message. `isMeta` flags the whole row;
/// otherwise a leading text item carrying a harness wrapper marks it injected.
/// v1 claude-code never interleaves both within one message.
fn user_array_provenance(row: &Value, items: &[Value]) -> Provenance {
    if is_meta_row(row) {
        return Provenance::Injected;
    }
    let wrapped = items.iter().any(|item| {
        item.get("type").and_then(Value::as_str) == Some("text")
            && item
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(is_injected_user_text)
    });
    if wrapped {
        Provenance::Injected
    } else {
        Provenance::Conversational
    }
}

#[cfg(test)]
mod tests {
    //! Conformance tests for the claude-code adapter's data-shape contract:
    //! subagent path derivation, replay dedup, tool-name resolution, and the
    //! "no synthesized values" invariant (spec.md#model-no-synthesis, spec.md#model-schema-honesty, and spec.md#model-lossless-projection).
    //!
    //! Each test builds a tiny synthetic corpus under a `TempDir` so the
    //! assertions exercise the real adapter end-to-end without depending on
    //! committed fixtures.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::PartKind};
    use tempfile::TempDir;

    // Manifest-dir anchored: unit tests must not depend on the process cwd
    // (figment::Jail chdirs the whole test process while config tests run).
    const FIXTURE_ROOT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/claude_code/projects"
    );

    #[test]
    fn probe_default_finds_claude_projects_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &ClaudeCodeFactory,
            &[".claude", "projects"],
        )
    }

    /// `source_record_hash` must dedupe noise-field replays (whitespace,
    /// `timestamp`, `requestId`) and let semantic-content differences through
    /// so a same-uuid row with a different `message.content` still reaches
    /// the validator (spec.md#adapter-integrity-dedup).
    #[test]
    fn source_record_hash_ignores_noise_keeps_semantic_diffs() {
        let base = serde_json::json!({
            "uuid": "u1",
            "type": "user",
            "parentUuid": null,
            "message": {"role": "user", "content": "hi"},
            "timestamp": "2026-06-17T00:00:00Z",
            "requestId": "req-A",
            "isMeta": false,
            "gitBranch": "main",
            "version": "2.1.56",
        });
        let noise_diff = serde_json::json!({
            "uuid": "u1",
            "type": "user",
            "parentUuid": null,
            "message": {"role": "user", "content": "hi"},
            "timestamp": "2026-06-17T00:00:05Z",
            "requestId": "req-B",
            "isMeta": true,
            "gitBranch": "feat/x",
            "version": "2.1.57",
        });
        let content_diff = serde_json::json!({
            "uuid": "u1",
            "type": "user",
            "parentUuid": null,
            "message": {"role": "user", "content": "different"},
            "timestamp": "2026-06-17T00:00:00Z",
        });
        assert_eq!(
            source_record_hash(&base),
            source_record_hash(&noise_diff),
            "noise-field differences must dedupe",
        );
        assert_ne!(
            source_record_hash(&base),
            source_record_hash(&content_diff),
            "semantic content differences must not dedupe",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_fixture_corpus() -> anyhow::Result<()> {
        let adapter = ClaudeCodeAdapter::new(FIXTURE_ROOT);
        crate::adapter::test_support::assert_native_restore(
            &ClaudeCodeFactory,
            &adapter,
            std::path::Path::new(FIXTURE_ROOT),
        )
        .await
    }

    /// `plan` is the events_with freshness gate run standalone: an empty
    /// oracle marks everything pending (walk cost only), a saturated oracle
    /// marks every readable-id session fresh, and the counts always partition
    /// `sessions`.
    #[tokio::test(flavor = "multi_thread")]
    async fn plan_classifies_fresh_vs_pending_without_decoding() -> anyhow::Result<()> {
        use crate::adapter::{Adapter, test_support::MaxWatermarkOracle};

        let adapter = ClaudeCodeAdapter::new(FIXTURE_ROOT);
        let first_sync = adapter
            .plan(&crate::adapter::NoopOracle)
            .await?
            .expect("jsonl-tree adapters support plan");
        assert!(first_sync.sessions > 0);
        assert_eq!(first_sync.pending, first_sync.sessions);
        assert_eq!(first_sync.fresh, 0);

        let caught_up = adapter
            .plan(&MaxWatermarkOracle)
            .await?
            .expect("jsonl-tree adapters support plan");
        assert_eq!(caught_up.sessions, first_sync.sessions);
        assert!(caught_up.fresh > 0, "fixture sessions must gate as fresh");
        assert_eq!(caught_up.fresh + caught_up.pending, caught_up.sessions);
        Ok(())
    }

    /// Sessions that can never earn a stored watermark - a zero-byte file, a
    /// metadata-only transcript with no timestamped row - gate `Empty` (proven
    /// nothing to ingest), and a workflow `journal.jsonl` leaves the walk
    /// entirely. Otherwise a store that syncs clean reports as forever out of
    /// date. The Empty proof is locked to real ingest below
    /// (`keyless_file_peeks_empty_and_ingests_nothing`).
    #[tokio::test(flavor = "multi_thread")]
    async fn plan_gates_keyless_sessions_empty_and_excludes_journals() -> anyhow::Result<()> {
        use crate::adapter::{Adapter, test_support::MaxWatermarkOracle};

        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let session_uuid = "99999999-9999-9999-9999-999999999999";
        let wf_dir = project_dir
            .join(session_uuid)
            .join("subagents")
            .join("workflows")
            .join("wf_11111111-abc");
        std::fs::create_dir_all(&wf_dir)?;

        let session_row = serde_json::json!({
            "type": "user",
            "uuid": "u-1",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-06-04T00:00:00.000Z",
            "message": {"role": "user", "content": "hi"},
        });
        std::fs::write(
            project_dir.join(format!("{session_uuid}.jsonl")),
            format!("{session_row}\n"),
        )?;
        std::fs::write(project_dir.join("empty-session.jsonl"), "")?;
        let title_row = serde_json::json!({
            "type": "ai-title",
            "aiTitle": "title only",
            "sessionId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        });
        std::fs::write(
            project_dir.join("title-only.jsonl"),
            format!("{title_row}\n"),
        )?;
        std::fs::write(wf_dir.join("journal.jsonl"), "{\"type\":\"started\"}\n")?;

        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let plan = adapter
            .plan(&MaxWatermarkOracle)
            .await?
            .expect("jsonl-tree adapters support plan");
        assert_eq!(
            plan.sessions, 3,
            "journal.jsonl must not count as a session"
        );
        assert_eq!(
            plan.fresh, 3,
            "keyless sessions gate Empty and count as fresh",
        );
        assert_eq!(plan.pending, 0, "a clean corpus must read as fully synced");
        Ok(())
    }

    /// The `Empty` proof's lock: a file the peek judges `Empty` (no timestamped
    /// row in a whole-file scan) MUST ingest zero rows through the real ingest
    /// path - peek and session anchoring share `parse_timestamp`, and this test
    /// fails the moment ingest learns to anchor such files without the peek
    /// being updated in the same change (spec.md#session-movement-complete).
    #[tokio::test(flavor = "multi_thread")]
    async fn keyless_file_peeks_empty_and_ingests_nothing() -> anyhow::Result<()> {
        use crate::adapter::SourceWatermark;

        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let title_row = serde_json::json!({
            "type": "ai-title",
            "aiTitle": "title only",
            "sessionId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        });
        let path = project_dir.join("title-only.jsonl");
        std::fs::write(&path, format!("{title_row}\n"))?;

        let adapter = ClaudeCodeAdapter::new(corpus.path());
        assert_eq!(
            adapter.peek_watermark(&path),
            SourceWatermark::Empty,
            "a whole-file scan with no timestamped row is a proof of emptiness",
        );

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(
            summary.accepted(),
            0,
            "an Empty-judged file must ingest nothing - if this fails, ingest \
             learned to anchor keyless files and peek_watermark must be updated",
        );
        assert!(
            store.session_ids().await?.is_empty(),
            "no session row may land from an Empty-judged file",
        );
        Ok(())
    }

    /// `<root>/<encoded-cwd>/<parent_uuid>.jsonl` plus
    /// `<root>/<encoded-cwd>/<parent_uuid>/subagents/agent-<hash>.jsonl` plus
    /// `agent-<hash>.meta.json`. The subagent file must:
    ///   - emit a Session whose `id = "{parent_uuid}/agent-{hash}"`
    ///   - have `parent_session_id = Some(parent_uuid)`
    ///   - have `source_agent = "claude-code/{agentType}"` from the meta file
    ///   - have `options.subagent` carrying the hash + agent_type + description
    #[tokio::test(flavor = "multi_thread")]
    async fn subagent_file_derives_child_session_with_parent_link() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "11111111-1111-1111-1111-111111111111";
        let agent_hash = "abc123def456";
        std::fs::create_dir_all(project_dir.join(parent_uuid).join("subagents"))?;

        // Parent session file (one user row to anchor a Session).
        let parent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-parent-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "version": "2.1.121",
            "message": {"role": "user", "content": "hi parent"},
        });
        std::fs::write(
            project_dir.join(format!("{parent_uuid}.jsonl")),
            format!("{parent_row}\n"),
        )?;

        // Subagent file + sibling meta. Carries the SAME sessionId as the parent
        // in row content; the adapter must derive a child id from the path.
        let subagent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-sub-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "isSidechain": true,
            "agentId": agent_hash,
            "timestamp": "2026-05-16T00:01:00.000Z",
            "version": "2.1.121",
            "message": {"role": "user", "content": "subagent prompt"},
        });
        std::fs::write(
            project_dir
                .join(parent_uuid)
                .join("subagents")
                .join(format!("agent-{agent_hash}.jsonl")),
            format!("{subagent_row}\n"),
        )?;
        std::fs::write(
            project_dir
                .join(parent_uuid)
                .join("subagents")
                .join(format!("agent-{agent_hash}.meta.json")),
            r#"{"agentType":"general-purpose","description":"do a thing"}"#,
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());

        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(
            summary.dropped_sessions, 0,
            "subagent file must NOT collide with parent (pre-fix this was the project-immutable rejection)"
        );

        let parent = store
            .get_session(parent_uuid)
            .await?
            .expect("parent session should ingest as the bare uuid");
        assert_eq!(parent.session.source_agent, "claude-code");
        assert_eq!(parent.session.parent_session_id, None);

        let child_id = format!("{parent_uuid}/agent-{agent_hash}");
        let child = store
            .get_session(&child_id)
            .await?
            .expect("subagent session must surface under the derived id");
        assert_eq!(
            child.session.source_agent, "claude-code/general-purpose",
            "agent_type from .meta.json should suffix the source_agent label"
        );
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some(parent_uuid),
            "subagent must link back to parent via parent_session_id",
        );
        let subagent_meta = child
            .session
            .options
            .get("subagent")
            .expect("options.subagent must carry the hash + verbatim meta.json");
        assert_eq!(subagent_meta["hash"], serde_json::json!(agent_hash));
        assert_eq!(
            subagent_meta["meta"]["agentType"],
            serde_json::json!("general-purpose")
        );
        assert_eq!(
            subagent_meta["meta"]["description"],
            serde_json::json!("do a thing")
        );
        Ok(())
    }

    /// A forked subagent transcript (Claude Code >= 2.1.117 `/fork`) opens with
    /// a `fork-context-ref` header row that carries no `sessionId`; the id first
    /// appears on the following message row. The whole transcript must still
    /// ingest - id derived from the path, header preserved as a System message,
    /// the conversation turns landing as their own messages - not be dropped as
    /// "line 1 missing sessionId" (that pre-fix drop silently lost every forked
    /// subagent's conversation). Fails on `main`; passes with the fix.
    #[tokio::test(flavor = "multi_thread")]
    async fn fork_subagent_transcript_ingests_despite_headerless_first_row() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "33333333-3333-3333-3333-333333333333";
        let agent_hash = "afork0001";
        std::fs::create_dir_all(project_dir.join(parent_uuid).join("subagents"))?;

        // Parent anchor.
        let parent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-parent-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-06-10T00:00:00.000Z",
            "version": "2.1.170",
            "message": {"role": "user", "content": "hi parent"},
        });
        std::fs::write(
            project_dir.join(format!("{parent_uuid}.jsonl")),
            format!("{parent_row}\n"),
        )?;

        // Fork transcript: a `fork-context-ref` header (NO sessionId, NO
        // timestamp) followed by the inherited-context conversation turns.
        let header = serde_json::json!({
            "type": "fork-context-ref",
            "agentId": agent_hash,
            "parentSessionId": parent_uuid,
            "parentLastUuid": "u-parent-1",
            "contextLength": 74,
        });
        let user_row = serde_json::json!({
            "type": "user",
            "uuid": "u-fork-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "isSidechain": true,
            "agentId": agent_hash,
            "timestamp": "2026-06-10T00:01:00.000Z",
            "version": "2.1.170",
            "message": {"role": "user", "content": "do the fork task"},
        });
        let assistant_row = serde_json::json!({
            "type": "assistant",
            "uuid": "a-fork-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "isSidechain": true,
            "agentId": agent_hash,
            "timestamp": "2026-06-10T00:01:05.000Z",
            "version": "2.1.170",
            "message": {"role": "assistant", "content": [{"type": "text", "text": "done"}]},
        });
        std::fs::write(
            project_dir
                .join(parent_uuid)
                .join("subagents")
                .join(format!("agent-{agent_hash}.jsonl")),
            format!("{header}\n{user_row}\n{assistant_row}\n"),
        )?;
        std::fs::write(
            project_dir
                .join(parent_uuid)
                .join("subagents")
                .join(format!("agent-{agent_hash}.meta.json")),
            r#"{"agentType":"fork","description":"do the fork task"}"#,
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());

        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(
            summary.dropped_sessions, 0,
            "fork transcript must ingest, not drop on the headerless first row"
        );

        let child_id = format!("{parent_uuid}/agent-{agent_hash}");
        let child = store
            .get_session(&child_id)
            .await?
            .expect("forked subagent must surface under the path-derived child id");
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some(parent_uuid),
            "fork must link back to its parent",
        );
        assert_eq!(
            child.session.source_agent, "claude-code/fork",
            "agent_type `fork` from .meta.json should suffix the source_agent label",
        );
        // The inherited-context conversation must survive - this is the data the
        // pre-fix drop was losing.
        assert!(
            child
                .messages
                .iter()
                .any(|m| matches!(m.message, Message::User { .. })),
            "the fork's user turn must persist",
        );
        assert!(
            child
                .messages
                .iter()
                .any(|m| matches!(m.message, Message::Assistant { .. })),
            "the fork's assistant turn must persist",
        );
        Ok(())
    }

    /// Subagent file present but the sibling `.meta.json` is missing. The
    /// adapter must still derive a child session (so it doesn't collide with
    /// the parent) and fall back to `source_agent = "claude-code/subagent"`.
    #[tokio::test(flavor = "multi_thread")]
    async fn subagent_without_meta_falls_back_to_generic_label() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "22222222-2222-2222-2222-222222222222";
        let agent_hash = "deadbeef";
        std::fs::create_dir_all(project_dir.join(parent_uuid).join("subagents"))?;
        let row = serde_json::json!({
            "type": "user",
            "uuid": "u-sub-only",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {"role": "user", "content": "no meta sibling here"},
        });
        std::fs::write(
            project_dir
                .join(parent_uuid)
                .join("subagents")
                .join(format!("agent-{agent_hash}.jsonl")),
            format!("{row}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let _summary =
            ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        let child = store
            .get_session(&format!("{parent_uuid}/agent-{agent_hash}"))
            .await?
            .expect("derived child id even without meta");
        assert_eq!(child.session.source_agent, "claude-code/subagent");
        Ok(())
    }

    /// Nested workflow-runner subagent:
    ///   `<parent_uuid>/subagents/workflows/<wf-id>/agent-<hash>.jsonl`.
    /// Same parent `sessionId` in row content AND a shifted `cwd`. The adapter
    /// must derive a distinct child id from the FULL path under `subagents/`
    /// (not collapse onto the parent), so it neither collides on the immutable
    /// `project` nor silently merges into the parent. Regression for the
    /// workflow-layout sync rejection. See spec.md#datasets.
    #[tokio::test(flavor = "multi_thread")]
    async fn workflow_nested_subagent_derives_distinct_child_not_parent_collision()
    -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "44444444-4444-4444-4444-444444444444";
        let wf_id = "wf_abcd1234-ef0";
        let agent_hash = "cafef00dbaadf00d1";
        let wf_dir = project_dir
            .join(parent_uuid)
            .join("subagents")
            .join("workflows")
            .join(wf_id);
        std::fs::create_dir_all(&wf_dir)?;

        let parent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-parent-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-20T00:00:00.000Z",
            "message": {"role": "user", "content": "hi parent"},
        });
        std::fs::write(
            project_dir.join(format!("{parent_uuid}.jsonl")),
            format!("{parent_row}\n"),
        )?;

        // Shifted cwd: pre-fix this collided with the parent's immutable project.
        let subagent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-wf-sub-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test/packages/sub",
            "isSidechain": true,
            "agentId": agent_hash,
            "timestamp": "2026-05-20T00:01:00.000Z",
            "message": {"role": "user", "content": "workflow subagent prompt"},
        });
        std::fs::write(
            wf_dir.join(format!("agent-{agent_hash}.jsonl")),
            format!("{subagent_row}\n"),
        )?;
        std::fs::write(
            wf_dir.join(format!("agent-{agent_hash}.meta.json")),
            r#"{"agentType":"general-purpose","description":"workflow child"}"#,
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(
            summary.dropped_sessions, 0,
            "nested workflow subagent must NOT collide with the parent project",
        );

        let parent = store
            .get_session(parent_uuid)
            .await?
            .expect("parent session ingests under the bare uuid");
        assert_eq!(&*parent.session.project, "/tmp/pond-test");
        assert_eq!(parent.session.parent_session_id, None);

        let child_id = format!("{parent_uuid}/workflows/{wf_id}/agent-{agent_hash}");
        let child = store
            .get_session(&child_id)
            .await?
            .expect("workflow subagent surfaces under the full nested child id");
        assert_eq!(child.session.source_agent, "claude-code/general-purpose");
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some(parent_uuid)
        );
        assert_eq!(
            &*child.session.project, "/tmp/pond-test/packages/sub",
            "child keeps its own cwd-derived project, distinct from the parent",
        );
        let subagent_meta = child
            .session
            .options
            .get("subagent")
            .expect("options.subagent present");
        assert_eq!(subagent_meta["hash"], serde_json::json!(agent_hash));
        Ok(())
    }

    /// A `.jsonl` under `subagents/` whose leaf is NOT `agent-<hash>.jsonl` (a
    /// layout this pond version doesn't understand) must FAIL VISIBLY rather
    /// than fall back to its content `sessionId` (the parent's) and silently
    /// merge into the parent session. It is counted as an unsupported skip and
    /// contributes no rows. See spec.md#datasets.
    #[tokio::test(flavor = "multi_thread")]
    async fn unrecognized_subagents_file_fails_visibly_not_merged() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "55555555-5555-5555-5555-555555555555";
        let unknown_dir = project_dir
            .join(parent_uuid)
            .join("subagents")
            .join("workflows")
            .join("wf_future01-aaa");
        std::fs::create_dir_all(&unknown_dir)?;

        let parent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-parent-only",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-20T00:00:00.000Z",
            "message": {"role": "user", "content": "parent message"},
        });
        std::fs::write(
            project_dir.join(format!("{parent_uuid}.jsonl")),
            format!("{parent_row}\n"),
        )?;

        // Same parent sessionId AND same cwd: pre-guard this would have merged
        // silently into the parent. The leaf name is not `agent-<hash>.jsonl`.
        let unknown_row = serde_json::json!({
            "type": "user",
            "uuid": "u-should-not-merge",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-20T00:02:00.000Z",
            "message": {"role": "user", "content": "must not land under parent"},
        });
        std::fs::write(
            unknown_dir.join("transcript-001.jsonl"),
            format!("{unknown_row}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        assert_eq!(
            summary.skipped_files, 1,
            "the unrecognized subagents/ transcript must be a visible, counted skip",
        );
        let parent = store
            .get_session(parent_uuid)
            .await?
            .expect("parent session ingests");
        assert_eq!(
            parent.messages.len(),
            1,
            "the unrecognized file's row must NOT be merged into the parent session",
        );
        assert!(
            parent
                .messages
                .iter()
                .all(|m| m.message.id() != "u-should-not-merge"),
            "parent must not absorb the unrecognized file's message",
        );
        Ok(())
    }

    /// Re-sync visibility: an unrecognized `subagents/` file must STILL surface as
    /// a visible `Unsupported` skip when the parent already carries a freshness
    /// watermark. Its content `sessionId` is the parent's, so peeking it would let
    /// the freshness gate skip the file as `Fresh` under the parent's watermark and
    /// hide the failure. `peek_session_id` returns `None` for it instead, keeping
    /// it out of the gate. Regression for the re-sync visibility leak. See
    /// spec.md#datasets.
    #[tokio::test(flavor = "multi_thread")]
    async fn unrecognized_subagents_file_stays_visible_under_parent_watermark() -> anyhow::Result<()>
    {
        struct ParentAlreadyFresh;
        impl crate::adapter::SkipOracle for ParentAlreadyFresh {
            fn session_max_ts(&self, _session_id: &str) -> Option<i64> {
                // Far-future watermark: the parent file WOULD trip the freshness
                // gate (source ts <= watermark). The guard must keep the
                // unrecognized file out of the gate regardless.
                Some(i64::MAX)
            }
            fn is_empty(&self) -> bool {
                false
            }
        }

        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "66666666-6666-6666-6666-666666666666";
        let unknown_dir = project_dir
            .join(parent_uuid)
            .join("subagents")
            .join("workflows")
            .join("wf_future02-bbb");
        std::fs::create_dir_all(&unknown_dir)?;

        let parent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-parent-fresh",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-20T00:00:00.000Z",
            "message": {"role": "user", "content": "parent message"},
        });
        std::fs::write(
            project_dir.join(format!("{parent_uuid}.jsonl")),
            format!("{parent_row}\n"),
        )?;

        // Same parent sessionId, leaf not `agent-<hash>.jsonl`: pre-fix this would
        // peek the parent's id and be fresh-skipped under the far-future watermark.
        let unknown_row = serde_json::json!({
            "type": "user",
            "uuid": "u-resync-should-stay-visible",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-20T00:02:00.000Z",
            "message": {"role": "user", "content": "must stay visible"},
        });
        std::fs::write(
            unknown_dir.join("transcript-002.jsonl"),
            format!("{unknown_row}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &ParentAlreadyFresh, |_| {}).await?;

        assert_eq!(
            summary.skipped_files, 1,
            "the unrecognized transcript must stay a visible Unsupported skip, not be fresh-skipped under the parent's watermark",
        );
        // The parent file legitimately fresh-skips under the far-future watermark;
        // the unrecognized file must NOT join it (pre-fix `skipped_fresh` would be 2).
        assert_eq!(
            summary.skipped_fresh, 1,
            "only the parent may fresh-skip; the unrecognized file must not borrow its watermark",
        );
        Ok(())
    }

    /// Three rows with the same `uuid` (the claude-code `/resume` replay
    /// pattern). The adapter must dedupe at the file-state level so the
    /// validator never sees the duplicates; `dropped_events` stays 0 and
    /// `inserted` covers the single canonical row.
    #[tokio::test(flavor = "multi_thread")]
    async fn replay_duplicates_are_dedup_at_adapter_layer() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "33333333-3333-3333-3333-333333333333";
        let dup_uuid = "u-shared-1";
        let row = serde_json::json!({
            "type": "user",
            "uuid": dup_uuid,
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {"role": "user", "content": "replayed three times"},
        });
        // Three identical rows back-to-back, same uuid.
        let body = format!("{row}\n{row}\n{row}\n");
        std::fs::write(project_dir.join(format!("{session_uuid}.jsonl")), body)?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        assert_eq!(
            summary.dropped_events, 0,
            "adapter must dedupe replays before they reach the validator"
        );
        assert!(
            !summary
                .drop_reasons
                .contains_key(crate::sessions::DROP_REASON_DUPLICATE_MESSAGE_ID),
            "duplicate_message_id bucket stays empty when adapter does its job"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn same_uuid_different_content_is_visible_duplicate_not_adapter_drop()
    -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "33333333-3333-3333-3333-333333333334";
        let dup_uuid = "u-shared-different";
        let first = serde_json::json!({
            "type": "user",
            "uuid": dup_uuid,
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {"role": "user", "content": "first content"},
        });
        let second = serde_json::json!({
            "type": "user",
            "uuid": dup_uuid,
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:01.000Z",
            "message": {"role": "user", "content": "changed content"},
        });
        std::fs::write(
            project_dir.join(format!("{session_uuid}.jsonl")),
            format!("{first}\n{second}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        assert_eq!(
            summary
                .drop_reasons
                .get(crate::sessions::DROP_REASON_DUPLICATE_MESSAGE_ID)
                .copied(),
            Some(1),
            "same uuid with changed content must reach the visible duplicate-id path",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_row_without_messages_does_not_fresh_skip_source() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "33333333-3333-3333-3333-333333333335";
        let row = serde_json::json!({
            "type": "user",
            "uuid": "u-after-partial",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {"role": "user", "content": "healed by replay"},
        });
        std::fs::write(
            project_dir.join(format!("{session_uuid}.jsonl")),
            format!("{row}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        store
            .upsert_sessions(&[Session {
                id: session_uuid.to_owned(),
                parent_session_id: None,
                parent_message_id: None,
                source_agent: "claude-code".to_owned(),
                created_at: DateTime::parse_from_rfc3339("2026-05-16T00:00:00.000Z")?
                    .with_timezone(&Utc),
                project: Extracted::from_test_value("/tmp/pond-test".to_owned()),
                options: ProviderOptions::new(),
            }])
            .await?;

        let last_ids = store.session_last_message_ids().await?;
        assert!(
            !last_ids.contains_key(session_uuid),
            "a session row without messages must not produce a freshness key",
        );
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(summary.skipped_fresh, 0);
        let session = store
            .get_session(session_uuid)
            .await?
            .expect("session row exists");
        assert_eq!(session.messages.len(), 1, "replay must heal messages");
        Ok(())
    }

    /// Claude Code appends trailing metadata rows (`last-prompt`,
    /// `permission-mode`, ...) with no timestamp after the conversation. The
    /// freshness peek must walk back past them to the last real message's
    /// timestamp - taking only the literal last line returned None and stranded
    /// ~2k sessions perpetually un-fresh, re-decoding ~1.2M stored rows every sync.
    #[test]
    fn peek_watermark_walks_back_past_trailing_metadata_rows() {
        let corpus = TempDir::new().unwrap();
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir).unwrap();
        let session_uuid = "44444444-4444-4444-4444-444444444444";
        let message = serde_json::json!({
            "type": "user",
            "uuid": "u-1",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {"role": "user", "content": "hello"},
        });
        // Metadata rows Claude Code writes after the conversation - no timestamp.
        let last_prompt =
            serde_json::json!({"type": "last-prompt", "sessionId": session_uuid, "prompt": "hi"});
        let permission = serde_json::json!({"type": "permission-mode", "sessionId": session_uuid});
        let path = project_dir.join(format!("{session_uuid}.jsonl"));
        std::fs::write(&path, format!("{message}\n{last_prompt}\n{permission}\n")).unwrap();

        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let expected = DateTime::parse_from_rfc3339("2026-05-16T00:00:00.000Z")
            .unwrap()
            .timestamp_micros();
        assert_eq!(
            adapter.peek_watermark(&path),
            crate::adapter::SourceWatermark::At(expected),
            "walk back past trailing metadata to the last message's timestamp",
        );
    }

    /// One assistant `tool_use` followed by a user `tool_result` in the same
    /// file. The adapter's per-file `tool_use_id -> name` map must resolve the
    /// result's tool name to the call's name. Pre-fix: synthesized `"unknown"`.
    #[tokio::test(flavor = "multi_thread")]
    async fn tool_result_name_resolves_from_prior_tool_use_in_same_file() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "44444444-4444-4444-4444-444444444444";
        let call_id = "toolu_test_01";

        let tool_use_row = serde_json::json!({
            "type": "assistant",
            "uuid": "u-call",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": call_id,
                    "name": "Edit",
                    "input": {"file_path": "/tmp/foo"},
                }],
            },
        });
        let tool_result_row = serde_json::json!({
            "type": "user",
            "uuid": "u-result",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:01.000Z",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": "ok",
                }],
            },
        });
        std::fs::write(
            project_dir.join(format!("{session_uuid}.jsonl")),
            format!("{tool_use_row}\n{tool_result_row}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let _summary =
            ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        let session = store
            .get_session(session_uuid)
            .await?
            .expect("session ingests");

        let mut saw_call = false;
        let mut saw_result = false;
        for stored in &session.messages {
            for part in &stored.parts {
                match &part.kind {
                    PartKind::ToolCall {
                        call_id: cid, name, ..
                    } => {
                        assert_eq!(cid.as_ref().map(|e| e.as_str()), Some(call_id));
                        assert_eq!(
                            name.as_ref().map(|e| e.as_str()),
                            Some("Edit"),
                            "tool_use carries the name directly"
                        );
                        saw_call = true;
                    }
                    PartKind::ToolResult {
                        call_id: cid, name, ..
                    } => {
                        assert_eq!(cid.as_ref().map(|e| e.as_str()), Some(call_id));
                        assert_eq!(
                            name.as_ref().map(|e| e.as_str()),
                            Some("Edit"),
                            "tool_result resolves the name via the per-file map (was 'unknown' pre-2026-05-16)"
                        );
                        saw_result = true;
                    }
                    _ => {}
                }
            }
        }
        assert!(saw_call && saw_result, "both parts must be present");
        Ok(())
    }

    /// spec.md#model-part-provenance: a genuine human prompt classifies
    /// `conversational`; a harness `<task-notification>` user-slot turn and an
    /// `isMeta` row classify `injected`.
    #[test]
    fn user_text_provenance_separates_prompts_from_harness_injection() {
        let prompt = json!({"type": "user", "uuid": "u1"});
        assert_eq!(
            user_text_provenance(&prompt, "please refactor the parser"),
            Provenance::Conversational,
        );

        let notification = json!({"type": "user", "uuid": "u2"});
        assert_eq!(
            user_text_provenance(
                &notification,
                "<task-notification>background task done</task-notification>",
            ),
            Provenance::Injected,
        );

        let meta = json!({"type": "user", "uuid": "u3", "isMeta": true});
        assert_eq!(
            user_text_provenance(&meta, "expanded skill body"),
            Provenance::Injected,
        );
    }

    /// Ingest a session carrying a `<task-notification>` user message and a
    /// genuine prompt; the notification's part must be `injected` and the
    /// prompt's `conversational` (spec.md#model-part-provenance).
    #[tokio::test(flavor = "multi_thread")]
    async fn task_notification_message_yields_injected_parts() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "66666666-6666-6666-6666-666666666666";
        let prompt = serde_json::json!({
            "type": "user",
            "uuid": "u-prompt",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {"role": "user", "content": "genuine human prompt"},
        });
        let notification = serde_json::json!({
            "type": "user",
            "uuid": "u-notify",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:01.000Z",
            "message": {
                "role": "user",
                "content": "<task-notification>a background task finished</task-notification>",
            },
        });
        std::fs::write(
            project_dir.join(format!("{session_uuid}.jsonl")),
            format!("{prompt}\n{notification}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;

        let session = store
            .get_session(session_uuid)
            .await?
            .expect("session ingests");
        let mut saw_prompt = false;
        let mut saw_notification = false;
        for stored in &session.messages {
            for part in &stored.parts {
                if stored.message.id() == "u-prompt" {
                    assert_eq!(part.provenance, crate::wire::Provenance::Conversational);
                    saw_prompt = true;
                }
                if stored.message.id() == "u-notify" {
                    assert_eq!(part.provenance, crate::wire::Provenance::Injected);
                    saw_notification = true;
                }
            }
        }
        assert!(saw_prompt && saw_notification, "both messages present");
        Ok(())
    }

    /// Orphan tool_result with no earlier tool_use in the same file: the
    /// per-file map can't resolve. The adapter must emit `name: None`, NOT
    /// the old `"unknown"` sentinel. Invariant 15 (no synthesized values).
    #[tokio::test(flavor = "multi_thread")]
    async fn orphan_tool_result_yields_name_none_not_unknown_sentinel() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "55555555-5555-5555-5555-555555555555";

        // tool_result with no earlier tool_use (simulates a compaction-pruned call).
        let row = serde_json::json!({
            "type": "user",
            "uuid": "u-orphan",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_orphan",
                    "content": "result body, no matching call",
                }],
            },
        });
        std::fs::write(
            project_dir.join(format!("{session_uuid}.jsonl")),
            format!("{row}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let _summary =
            ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        let session = store
            .get_session(session_uuid)
            .await?
            .expect("session ingests");
        let mut found = false;
        for stored in &session.messages {
            for part in &stored.parts {
                if let PartKind::ToolResult { name, call_id, .. } = &part.kind {
                    assert_eq!(call_id.as_ref().map(|e| e.as_str()), Some("toolu_orphan"));
                    assert!(
                        name.is_none(),
                        "orphan tool_result must be name=None, not synthesized 'unknown'",
                    );
                    found = true;
                }
            }
        }
        assert!(found, "orphan tool_result part must be present");
        // Sanity: even an orphan should not be reported as a drop.
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_message_role_becomes_lossless_carrier() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "66666666-6666-6666-6666-666666666666";

        // A role pond has no typed variant for must be carried whole, not
        // rejected (spec.md#adapters rule-3).
        let row = serde_json::json!({
            "type": "user",
            "uuid": "u-future",
            "sessionId": session_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-05-16T00:00:00.000Z",
            "message": {
                "role": "future_role",
                "content": "keep me",
            },
        });
        std::fs::write(
            project_dir.join(format!("{session_uuid}.jsonl")),
            format!("{row}\n"),
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert!(
            summary.drop_reasons.is_empty(),
            "an unknown role must be carried, not dropped: {:?}",
            summary.drop_reasons,
        );
        let session = store
            .get_session(session_uuid)
            .await?
            .expect("session with the carried record ingests");
        let carrier = session
            .messages
            .iter()
            .find(|stored| stored.message.id() == "u-future")
            .expect("the unknown-role record lands as a message");
        assert!(
            matches!(&carrier.message, Message::System { content, .. }
                if content.as_deref().is_some_and(|c| c.contains("future_role"))),
            "unmapped role must become a System carrier preserving the record",
        );
        Ok(())
    }

    /// The Workflow runner's `journal.jsonl` under `subagents/workflows/<wf>/`
    /// is a known control file: `skip_source` excludes it from the walk so it
    /// is never a session, never read, and never counted pending.
    #[test]
    fn workflow_journal_is_excluded_from_the_walk() {
        let adapter = ClaudeCodeAdapter::new("/tmp/pond-test-root");
        let journal = std::path::Path::new(
            "/root/-proj/55555555-5555-5555-5555-555555555555/subagents/workflows/wf_030e6487-da6/journal.jsonl",
        );
        assert!(is_workflow_control_file(journal));
        assert!(
            adapter.skip_source(journal),
            "journal.jsonl is a known control file, excluded from the walk",
        );
    }

    /// Regression guard against narrowing the net too far: a genuinely unknown
    /// leaf under `subagents/` is still flagged unsupported, while a recognized
    /// `agent-<hash>.jsonl` is not.
    #[test]
    fn unknown_subagents_leaf_is_still_unsupported() {
        let adapter = ClaudeCodeAdapter::new("/tmp/pond-test-root");
        let unknown = std::path::Path::new(
            "/root/-proj/PARENT/subagents/workflows/wf_x/transcript-001.jsonl",
        );
        assert!(
            adapter.unsupported_reason(unknown).is_some(),
            "an unrecognized non-agent, non-journal leaf must still fail visibly",
        );
        assert!(!is_workflow_control_file(unknown));
        assert!(
            !adapter.skip_source(unknown),
            "only the exact journal.jsonl leaf may leave the walk - an unknown \
             leaf stays in so its unsupported skip stays visible",
        );

        let agent = std::path::Path::new("/root/-proj/PARENT/subagents/agent-abc123def456.jsonl");
        assert!(
            adapter.unsupported_reason(agent).is_none(),
            "a recognized agent transcript is resolvable, not unsupported",
        );
    }

    /// End-to-end: a workflow dir holding both a real `agent-<hash>.jsonl`
    /// transcript and the runner's `journal.jsonl`. The agent transcript
    /// ingests as a child session; the journal is excluded from the walk (no
    /// `skipped_files` failure) and its rows never merge into the parent.
    #[tokio::test(flavor = "multi_thread")]
    async fn workflow_journal_excluded_while_sibling_agent_ingests() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "77777777-7777-7777-7777-777777777777";
        let wf_id = "wf_030e6487-da6";
        let agent_hash = "a38f4724ef3864da8";
        let wf_dir = project_dir
            .join(parent_uuid)
            .join("subagents")
            .join("workflows")
            .join(wf_id);
        std::fs::create_dir_all(&wf_dir)?;

        let parent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-parent-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-06-04T00:00:00.000Z",
            "message": {"role": "user", "content": "hi parent"},
        });
        std::fs::write(
            project_dir.join(format!("{parent_uuid}.jsonl")),
            format!("{parent_row}\n"),
        )?;

        let agent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-agent-1",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-06-04T00:01:00.000Z",
            "message": {"role": "user", "content": "workflow agent prompt"},
        });
        std::fs::write(
            wf_dir.join(format!("agent-{agent_hash}.jsonl")),
            format!("{agent_row}\n"),
        )?;

        // The Workflow journal: control events only, no sessionId.
        std::fs::write(
            wf_dir.join("journal.jsonl"),
            "{\"type\":\"started\",\"key\":\"v2:abc\",\"agentId\":\"a38f\"}\n\
             {\"type\":\"result\",\"key\":\"v2:abc\",\"agentId\":\"a38f\",\"result\":{}}\n",
        )?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(
            summary.skipped_files, 0,
            "journal.jsonl is a control file excluded from the walk, not an unsupported failure",
        );

        let child = store
            .get_session(&format!(
                "{parent_uuid}/workflows/{wf_id}/agent-{agent_hash}"
            ))
            .await?
            .expect("the sibling agent transcript still ingests as a child session");
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some(parent_uuid)
        );

        let parent = store
            .get_session(parent_uuid)
            .await?
            .expect("parent session ingests");
        assert_eq!(
            parent.messages.len(),
            1,
            "journal rows must NOT merge into the parent session",
        );
        Ok(())
    }

    /// Hardening: even a journal.jsonl whose rows DO carry the parent
    /// `sessionId` must not merge - the guard is structural, not contingent on
    /// the journal lacking one.
    #[tokio::test(flavor = "multi_thread")]
    async fn workflow_journal_with_parent_sessionid_still_not_merged() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        let parent_uuid = "88888888-8888-8888-8888-888888888888";
        let wf_dir = project_dir
            .join(parent_uuid)
            .join("subagents")
            .join("workflows")
            .join("wf_abc01234-def");
        std::fs::create_dir_all(&wf_dir)?;

        let parent_row = serde_json::json!({
            "type": "user",
            "uuid": "u-parent",
            "sessionId": parent_uuid,
            "cwd": "/tmp/pond-test",
            "timestamp": "2026-06-04T00:00:00.000Z",
            "message": {"role": "user", "content": "parent only"},
        });
        std::fs::write(
            project_dir.join(format!("{parent_uuid}.jsonl")),
            format!("{parent_row}\n"),
        )?;

        // A journal carrying the PARENT sessionId (hypothetical future shape):
        // the structural guard must still refuse to merge it.
        let journal_row = serde_json::json!({
            "type": "started",
            "key": "v2:abc",
            "agentId": "a1",
            "sessionId": parent_uuid,
            "message": {"role": "user", "content": "must not merge"},
        });
        std::fs::write(wf_dir.join("journal.jsonl"), format!("{journal_row}\n"))?;

        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let adapter = ClaudeCodeAdapter::new(corpus.path());
        let summary = ingest_adapter(&store, &adapter, &crate::adapter::NoopOracle, |_| {}).await?;
        assert_eq!(
            summary.skipped_files, 0,
            "journal is excluded from the walk, not an unsupported failure",
        );
        let parent = store
            .get_session(parent_uuid)
            .await?
            .expect("parent session ingests");
        assert_eq!(
            parent.messages.len(),
            1,
            "journal row must NOT merge even when it carries the parent sessionId",
        );
        Ok(())
    }
}
