//! Claude Code CLI adapter.
//!
//! Source path: `~/.claude/projects/<encoded-project-path>/<session-uuid>.jsonl`.
//! Each `.jsonl` file is one session; lines are typed entries linked via a
//! `parentUuid` -> `uuid` chain. Tool results arrive as `user` entries whose
//! `message.content[]` contains `tool_result` blocks with a parallel
//! `toolUseResult` field carrying structured data.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use async_stream::stream;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::{
    sessions::IngestEvent,
    wire::{FileData, Message, Part, PartKind, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, by_timestamp_then_id,
    collect_jsonl_files, compact_json, config_path, empty_options,
    extract::{Extracted, Source, extract_compact_repr, extract_self_str, extract_str},
    extracted_text, jsonl_bytes, part_id, raw_record,
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
///    `utils/sessionStorage.ts`). The adapter dedupes here so the validator
///    never sees the same PK twice; validator HashSet stays a safety net.
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
struct FileState {
    seen_uuids: HashSet<String>,
    tool_call_names: HashMap<String, Extracted<String>>,
}

/// Stable adapter name. Surfaces as the `[sources.claude-code]` config key,
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
    // canonical is append-only (spec.md#additive-sync).
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

    let mut files = vec![RestoredFile {
        relative_path: claude_relative_path(session),
        bytes: jsonl_bytes(NAME, &records)?,
    }];
    if session.session.parent_session_id.is_some()
        && let Some(meta) = subagent_meta_record(session)
    {
        let mut meta_path = files[0].relative_path.clone();
        meta_path.set_extension("meta.json");
        files.push(RestoredFile {
            relative_path: meta_path,
            bytes: serde_json::to_vec(&meta).map_err(|err| {
                AdapterError::schema(
                    NAME,
                    &session.session.id,
                    format!("json encode failed: {err}"),
                )
            })?,
        });
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
        let agent = session
            .session
            .id
            .rsplit('/')
            .next()
            .unwrap_or(&session.session.id);
        return PathBuf::from(encoded_project)
            .join(parent)
            .join("subagents")
            .join(format!("{agent}.jsonl"));
    }
    PathBuf::from(encoded_project).join(format!("{}.jsonl", session.session.id))
}

fn source_line(options: &ProviderOptions) -> Option<u64> {
    options
        .get("source")
        .and_then(|source| source.get("line"))
        .and_then(Value::as_u64)
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
    // dropped; the content stays in canonical (spec.md#native-restore-lossless,
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
        let root = self.root.clone();
        Box::pin(async move {
            // Count files only - do not pre-parse headers. Headers are read
            // once at stream time, where any decode failure is the canonical
            // skip path. Pre-reading here would error out twice for the same
            // bad file (once in discover, once in events) and would charge
            // every healthy file two opens instead of one.
            let paths = collect_jsonl_files(&root)
                .await
                .map_err(|io| AdapterError::io(NAME, io.path, io.source))?;
            Ok(paths.len())
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let root = self.root.clone();
        Box::pin(stream! {
            let paths = match collect_jsonl_files(&root).await {
                Ok(paths) => paths,
                Err(io) => {
                    yield Err(AdapterError::io(NAME, io.path, io.source));
                    return;
                }
            };
            for path in paths {
                let path_display = path.display().to_string();

                if let Some((id, mtime)) = peek_id_and_mtime(&path).await
                    && let Some(ingested_at) = oracle.last_ingested_at(&id)
                    && mtime <= ingested_at
                {
                    yield Ok(AdapterYield::Skipped {
                        session_id: id,
                        project: None,
                        reason: SkipReason::Fresh,
                    });
                    continue;
                }

                let session_event = match session_from_file(&path, &path_display).await {
                    Ok(session) => session,
                    Err(error) => {
                        yield Err(error);
                        continue;
                    }
                };
                let session_id = session_event.id.clone();
                let default_timestamp = session_event.created_at;
                yield Ok(AdapterYield::Event(IngestEvent::Session(session_event)));

                let file = match tokio::fs::File::open(&path).await {
                    Ok(file) => file,
                    Err(source) => {
                        yield Err(AdapterError::io(NAME, path_display, source));
                        continue;
                    }
                };

                let mut lines = BufReader::new(file).lines();
                let mut line_number = 0usize;
                let mut state = FileState::default();
                loop {
                    let next = lines.next_line().await;
                    let line = match next {
                        Ok(Some(line)) => line,
                        Ok(None) => break,
                        Err(source) => {
                            yield Err(AdapterError::io(NAME, path_display.clone(), source));
                            break;
                        }
                    };
                    line_number += 1;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let value = match serde_json::from_str::<Value>(&line) {
                        Ok(value) => value,
                        Err(source) => {
                            yield Err(AdapterError::parse(
                                NAME,
                                path_display.clone(),
                                line_number,
                                source,
                            ));
                            continue;
                        }
                    };
                    if let Some(uuid) = value.get("uuid").and_then(Value::as_str)
                        && !state.seen_uuids.insert(uuid.to_owned())
                    {
                        tracing::debug!(
                            target: "pond::adapter::claude_code",
                            uuid,
                            file = %path_display,
                            line = line_number,
                            "skip replay row (duplicate uuid in file)",
                        );
                        continue;
                    }
                    capture_tool_call_names(&value, &mut state.tool_call_names);
                    match events_from_row(
                        &session_id,
                        line_number,
                        &value,
                        default_timestamp,
                        &state,
                    ) {
                        Ok(events) => {
                            for event in events {
                                yield Ok(AdapterYield::Event(event));
                            }
                        }
                        Err(message) => {
                            yield Err(AdapterError::schema(
                                NAME,
                                format!("{path_display}:{line_number}"),
                                message,
                            ));
                            continue;
                        }
                    }
                }
            }
        })
    }
}

async fn peek_id_and_mtime(path: &Path) -> Option<(String, DateTime<Utc>)> {
    let metadata = tokio::fs::metadata(path).await.ok()?;
    let mtime = DateTime::<Utc>::from(metadata.modified().ok()?);
    if let Some(descriptor) = subagent_descriptor(path).await {
        let id = format!("{}/agent-{}", descriptor.parent_uuid, descriptor.agent_hash);
        return Some((id, mtime));
    }
    let file = tokio::fs::File::open(path).await.ok()?;
    let mut lines = BufReader::new(file).lines();
    let line = lines.next_line().await.ok()??;
    let row: Value = serde_json::from_str(&line).ok()?;
    let id = row.get("sessionId")?.as_str()?.to_owned();
    Some((id, mtime))
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

async fn session_from_file(path: &Path, path_display: &str) -> Result<Session, AdapterError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?;
    let mut lines = BufReader::new(file).lines();
    let mut first = None::<(usize, Value)>;
    let mut created_at = None;
    let mut project: Option<Extracted<String>> = None;
    let mut version = None;
    let mut line_number = 0usize;

    loop {
        let Some(line) = lines
            .next_line()
            .await
            .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?
        else {
            break;
        };
        line_number += 1;
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<Value>(&line).map_err(|source| {
            AdapterError::parse(NAME, path_display.to_owned(), line_number, source)
        })?;

        if first.is_none() {
            first = Some((line_number, row.clone()));
        }
        if created_at.is_none() {
            created_at = parse_timestamp(&row).ok();
        }
        if project.is_none() {
            project = extract_str(&row, "cwd");
        }
        if version.is_none() {
            version = row
                .get("version")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
    }

    let Some((first_line, first)) = first else {
        return Err(AdapterError::schema(
            NAME,
            path_display.to_owned(),
            "empty jsonl session",
        ));
    };

    let raw_session_id = first
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                format!("{path_display}:{first_line}"),
                format!("line {first_line} missing sessionId"),
            )
        })?
        .to_owned();
    let created_at = created_at.ok_or_else(|| {
        AdapterError::schema(
            NAME,
            format!("{path_display}:{first_line}"),
            "session has no parseable timestamp",
        )
    })?;

    // Subagent detection. Claude Code stores each subagent's transcript at
    // `<parent_dir>/<parent_uuid>/subagents/agent-<hash>.jsonl`, with a
    // sibling `agent-<hash>.meta.json` carrying `{agentType, description}`.
    // The subagent file shares the parent's `sessionId` in row content (so
    // the validator's "project is immutable" rule rejects it under the
    // pre-2026-05-16 layering). The fix is to derive a child id and link
    // back via `parent_session_id`. See spec.md#datasets.
    let subagent = subagent_descriptor(path).await;
    let project_dir = source_project_dir(path, subagent.is_some());
    let (session_id, parent_session_id, source_agent, subagent_options) = match subagent {
        Some(SubagentDescriptor {
            parent_uuid,
            agent_hash,
            agent_type,
            meta,
        }) => {
            let child_id = format!("{parent_uuid}/agent-{agent_hash}");
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
        None => (raw_session_id, None, "claude-code".to_owned(), None),
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
                        path_display.to_owned(),
                        "no `cwd` field in any row and source path is not UTF-8",
                    )
                })?;
            extract_self_str(&Value::String(decoded)).ok_or_else(|| {
                AdapterError::schema(
                    NAME,
                    path_display.to_owned(),
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
    let project_dir = if is_subagent {
        path.parent().and_then(Path::parent).and_then(Path::parent)
    } else {
        path.parent()
    };
    project_dir
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .map(ToOwned::to_owned)
}

/// Resolved metadata for one subagent JSONL file. `agent_type` is read from
/// the sibling `.meta.json` for the `source_agent` label; `meta` keeps that
/// file's full verbatim content so native restore reproduces it
/// (spec.md#native-restore-lossless). Both are `None` when the meta file is
/// absent or unreadable (the label falls back to `claude-code/subagent`).
struct SubagentDescriptor {
    parent_uuid: String,
    agent_hash: String,
    agent_type: Option<String>,
    meta: Option<Value>,
}

/// Recognise the on-disk subagent layout:
///   `.../-<encoded-cwd>/<parent_uuid>/subagents/agent-<hash>.jsonl`
/// and read the sibling `agent-<hash>.meta.json` for `agentType` and
/// `description`. Returns `None` for any path that does NOT match the
/// subagent shape (the common case: top-level session files).
async fn subagent_descriptor(path: &Path) -> Option<SubagentDescriptor> {
    let file_name = path.file_name()?.to_str()?;
    let agent_hash = file_name
        .strip_prefix("agent-")?
        .strip_suffix(".jsonl")?
        .to_owned();
    let subagents_dir = path.parent()?;
    if subagents_dir.file_name()?.to_str()? != "subagents" {
        return None;
    }
    let parent_dir = subagents_dir.parent()?;
    let parent_uuid = parent_dir.file_name()?.to_str()?.to_owned();

    let meta_path = subagents_dir.join(format!("agent-{agent_hash}.meta.json"));
    let (agent_type, meta) = match tokio::fs::read(&meta_path).await {
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
        ("user", Value::String(_)) => {
            parts.push(text_part(uuid, 0, extract_self_str(content)));
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
                tool_result_part(uuid, ordinal, item, source_tool_result.as_ref(), state)
            }));
            Message::Tool {
                id: uuid.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            }
        }
        ("user", Value::Array(items)) => {
            parts.extend(
                items
                    .iter()
                    .enumerate()
                    .map(|(ordinal, item)| user_part(uuid, ordinal, item, state)),
            );
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
                    .map(|(ordinal, item)| assistant_part(uuid, ordinal, item)),
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
        (other, _) => {
            return Err(format!("unsupported message role {other}"));
        }
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    Ok(events)
}

fn text_part(message_id: &str, ordinal: usize, text: Option<Extracted<String>>) -> Part {
    Part {
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
        options: empty_options(),
        kind: PartKind::Text { text },
    }
}

fn user_part(message_id: &str, ordinal: usize, value: &Value, state: &FileState) -> Part {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(message_id, ordinal, extract_str(value, "text")),
        Some("image") | Some("file") => file_part(message_id, ordinal, value),
        Some("tool_result") => tool_result_part(message_id, ordinal, value, None, state),
        // Unknown user part shapes: preserve the raw JSON in the Text slot
        // rather than dropping. This is not a synthesized value - it's a
        // lossless encoding of structured data the schema doesn't model.
        _ => text_part(message_id, ordinal, Some(extract_compact_repr(value))),
    }
}

fn assistant_part(message_id: &str, ordinal: usize, value: &Value) -> Part {
    match value.get("type").and_then(Value::as_str) {
        Some("text") => text_part(message_id, ordinal, extract_str(value, "text")),
        Some("thinking") => Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: signature_options(value),
            kind: PartKind::Reasoning {
                text: extract_str(value, "thinking"),
            },
        },
        Some("tool_use") => Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: extract_str(value, "id"),
                name: extract_str(value, "name"),
                params: value.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: false,
            },
        },
        Some("server_tool_use") => Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: extract_str(value, "id"),
                name: extract_str(value, "name"),
                params: value.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: true,
            },
        },
        Some("image") | Some("file") => file_part(message_id, ordinal, value),
        // Same rationale as `user_part`'s fallback: lossless encoding of
        // an unrecognised structured shape, not synthesised data.
        _ => text_part(message_id, ordinal, Some(extract_compact_repr(value))),
    }
}

fn tool_result_part(
    message_id: &str,
    ordinal: usize,
    value: &Value,
    source_tool_result: Option<&Value>,
    state: &FileState,
) -> Part {
    let call_id = extract_str(value, "tool_use_id");
    // `tool_result` source rows don't carry the tool name; it's resolved
    // via the per-file `tool_use_id -> name` map. Misses (compaction pruned
    // the originating `tool_use`) surface as `None` per spec.md#no-synthesis
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
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
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

fn file_part(message_id: &str, ordinal: usize, value: &Value) -> Part {
    let media_type = value
        .get("media_type")
        .or_else(|| value.get("mime_type"))
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream")
        .to_owned();
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
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
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
        "raw_record": row,
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

#[cfg(test)]
mod tests {
    //! Conformance tests for the claude-code adapter's data-shape contract:
    //! subagent path derivation, replay dedup, tool-name resolution, and the
    //! "no synthesized values" invariant (spec.md#no-synthesis, spec.md#schema-honesty, and spec.md#lossless-projection).
    //!
    //! Each test builds a tiny synthetic corpus under a `TempDir` so the
    //! assertions exercise the real adapter end-to-end without depending on
    //! committed fixtures.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::PartKind};
    use tempfile::TempDir;

    const FIXTURE_ROOT: &str = "tests/fixtures/adapter/claude_code/projects";

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

    /// Orphan tool_result with no prior tool_use in the same file: the
    /// per-file map can't resolve. The adapter must emit `name: None`, NOT
    /// the old `"unknown"` sentinel. Invariant 15 (no synthesized values).
    #[tokio::test(flavor = "multi_thread")]
    async fn orphan_tool_result_yields_name_none_not_unknown_sentinel() -> anyhow::Result<()> {
        let corpus = TempDir::new()?;
        let project_dir = corpus.path().join("-tmp-pond-test");
        std::fs::create_dir_all(&project_dir)?;
        let session_uuid = "55555555-5555-5555-5555-555555555555";

        // tool_result with no prior tool_use (simulates a compaction-pruned call).
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
}
