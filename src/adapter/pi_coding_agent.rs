//! pi-coding-agent adapter (github.com/badlogic/pi-mono).
//!
//! Source path: `~/.pi/agent/sessions/<project-slug>/<ISO-ts>_<uuid>.jsonl`.
//! One `.jsonl` file per session; each line is a typed record linked via a
//! `parentId` -> `id` chain (pi-coding-agent's leaf-cursor DAG). Top-level types:
//! `session` (consumed up front for Session), `model_change` /
//! `thinking_level_change` / `compaction` (session-state carriers kept as
//! System messages), and `message` (the per-turn model interaction, with
//! roles user / assistant / toolResult and content nested under `.message`).
//!
//! v1 stores the linear log ordered by source line; pi-coding-agent's `parentId` fork
//! graph (spec.md#deferred: multi-level fork lineage) is not collapsed into
//! `parent_message_id` but preserved verbatim in `options.source.raw_record`
//! for a future branching consumer.

use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use crate::{
    sessions::IngestEvent,
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, by_timestamp_then_id, compact_json, config_path,
    empty_options,
    extract::{Extracted, extract_compact_repr, extract_raw_record, extract_str},
    extracted_text,
    jsonl::{BoundedRow, JsonlTree, jsonl_tree_discover, jsonl_tree_events},
    jsonl_bytes, part_id, raw_record,
};

const NAME: &str = "pi-coding-agent";

/// Stateless factory: opens [`PiCodingAgentAdapter`] instances and probes for the
/// canonical install location under `~/.pi/agent/sessions`.
pub struct PiCodingAgentFactory;

impl AdapterFactory for PiCodingAgentFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(PiCodingAgentAdapter::new(config_path(
            NAME, config,
        )?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
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

fn serialize_session(
    session: &crate::sessions::SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    // Native replays the verbatim `options.source.raw_record` rows (the
    // `session` line first, then one per message in source order); `pi_record`
    // below is the foreign-only reconstruction. Replay echoes a frozen
    // snapshot - safe only while canonical is append-only
    // (spec.md#adapter-integrity-additive-sync).
    let mut records = Vec::new();
    if fidelity == RestoreFidelity::Native
        && let Some(raw) = raw_record(&session.session.options)
    {
        records.push(raw);
    } else {
        records.push(pi_session_record(session));
    }

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

    for message in &messages {
        if fidelity == RestoreFidelity::Native
            && let Some(raw) = raw_record(message.message.options())
        {
            records.push(raw);
            continue;
        }
        // Foreign restore: a System carrier (model/thinking/compaction record)
        // has no idiomatic home in another client's transcript - drop it; the
        // content stays in canonical (spec.md#adapter-native-restore-lossless,
        // foreign clause).
        if matches!(message.message, Message::System { .. }) {
            continue;
        }
        records.push(pi_message_record(session, message));
    }

    Ok(vec![RestoredFile {
        relative_path: pi_relative_path(session),
        bytes: jsonl_bytes(NAME, &records)?,
    }])
}

/// Reproduce the on-disk `sessions/<slug>/<file>.jsonl` path from the slug and
/// file name captured at ingest. Falls back to a derived name when a foreign
/// session never carried them.
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

fn encode_project(project: &str) -> String {
    project.replace(['/', '.'], "-")
}

fn source_line(options: &ProviderOptions) -> Option<u64> {
    options
        .get("source")
        .and_then(|source| source.get("line"))
        .and_then(Value::as_u64)
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

fn pi_message_record(
    session: &crate::sessions::SessionWithMessages,
    message: &crate::sessions::MessageWithParts,
) -> Value {
    json!({
        "type": "message",
        "id": message.message.id(),
        "parentId": message.message.options().get("source").and_then(|s| s.get("parent_id")),
        "timestamp": message.message.timestamp().to_rfc3339_opts(SecondsFormat::Millis, true),
        "message": pi_inner_message(session, message),
    })
}

fn pi_inner_message(
    _session: &crate::sessions::SessionWithMessages,
    message: &crate::sessions::MessageWithParts,
) -> Value {
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
        // System carriers are dropped before reaching here (foreign clause).
        Message::System { .. } => Value::Null,
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

/// Configured pi-coding-agent reader. Walks a tree of `*.jsonl` files under [`Self::root`]
/// and yields canonical events in source order per session.
#[derive(Debug, Clone)]
pub struct PiCodingAgentAdapter {
    root: PathBuf,
}

impl PiCodingAgentAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for PiCodingAgentAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        jsonl_tree_discover(self)
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        jsonl_tree_events(self, oracle)
    }
}

impl JsonlTree for PiCodingAgentAdapter {
    // pi-coding-agent's `toolResult` records carry their own `toolName`, so unlike
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
        if row.get("type").and_then(Value::as_str) == Some("session") {
            row.get("id").and_then(Value::as_str).map(ToOwned::to_owned)
        } else {
            None
        }
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
        events_from_row(&session.id, row.line, &row.value, session.created_at)
    }
}

fn session_from_rows(path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
    let path_display = path.display().to_string();
    let first = rows
        .first()
        .ok_or_else(|| AdapterError::schema(NAME, path_display.clone(), "empty jsonl session"))?;
    let row = &first.value;
    let at_first = format!("{path_display}:{}", first.line);
    if row.get("type").and_then(Value::as_str) != Some("session") {
        return Err(AdapterError::schema(
            NAME,
            at_first,
            "first row must be a `session` record",
        ));
    }
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::schema(NAME, at_first.clone(), "session record missing id"))?
        .to_owned();
    let created_at = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                at_first.clone(),
                "session record has no parseable timestamp",
            )
        })?;
    let project = extract_str(row, "cwd").ok_or_else(|| {
        // spec.md#model-project-non-empty: pi always records `cwd` on the
        // session line; its absence is a malformed session, not a default.
        AdapterError::schema(NAME, at_first, "session record missing cwd")
    })?;

    // Capture the exact on-disk path components so native restore reproduces
    // the source file byte-for-byte (the slug encoding and filename timestamp
    // are not recomputable from canonical alone).
    let project_slug = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .map(ToOwned::to_owned);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .map(ToOwned::to_owned);

    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "adapter": NAME,
            "version": row.get("version"),
            "project_slug": project_slug,
            "file_name": file_name,
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

/// Map one pi JSONL record into zero-or-more `IngestEvent`s. `session` is
/// consumed up front (eventless here); `model_change` / `thinking_level_change`
/// / `compaction` become System carriers; `message` becomes a User / Assistant
/// / Tool message plus its content Parts.
fn events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
) -> Result<Vec<IngestEvent>, String> {
    let kind = row.get("type").and_then(Value::as_str);
    let timestamp = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(default_timestamp);
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .map_or_else(|| format!("{session_id}:{line}"), ToOwned::to_owned);

    match kind {
        // Consumed up front by `session_from_rows`.
        Some("session") => Ok(Vec::new()),
        Some("message") => {
            let message_value = row
                .get("message")
                .ok_or_else(|| "message record missing `message` field".to_owned())?;
            message_events(session_id, &id, timestamp, row, message_value, line)
        }
        // Session-state carriers: keep the human-meaningful field as content,
        // the rest of the record in `options.source.raw_record`.
        Some("compaction") => Ok(vec![carrier_event(
            session_id,
            &id,
            timestamp,
            row,
            line,
            extract_str(row, "summary"),
        )]),
        Some("model_change") | Some("thinking_level_change") => Ok(vec![carrier_event(
            session_id,
            &id,
            timestamp,
            row,
            line,
            extract_str(row, "type"),
        )]),
        // Unknown record type: preserve it as a System carrier rather than
        // dropping (spec.md#adapter-integrity-no-silent-drops). The raw record
        // survives in options; the type label is the content.
        _ => Ok(vec![carrier_event(
            session_id,
            &id,
            timestamp,
            row,
            line,
            extract_str(row, "type"),
        )]),
    }
}

fn carrier_event(
    session_id: &str,
    id: &str,
    timestamp: DateTime<Utc>,
    row: &Value,
    line: usize,
    content: Option<Extracted<String>>,
) -> IngestEvent {
    IngestEvent::Message(Message::System {
        id: id.to_owned(),
        session_id: session_id.to_owned(),
        timestamp,
        content,
        options: row_options(row, line),
    })
}

fn message_events(
    session_id: &str,
    id: &str,
    timestamp: DateTime<Utc>,
    row: &Value,
    message_value: &Value,
    line: usize,
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
                options: row_options(row, line),
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
                options: assistant_options(row, message_value, line),
            }
        }
        "toolResult" => {
            parts.push(tool_result_part(session_id, id, message_value));
            Message::Tool {
                id: id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            }
        }
        other => return Err(format!("unsupported pi-coding-agent message role {other}")),
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
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
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
        ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
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

fn row_options(row: &Value, line: usize) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        json!({
            "line": line,
            "parent_id": row.get("parentId"),
            "raw_type": row.get("type"),
            "raw_record": extract_raw_record(row),
        }),
    );
    options
}

fn assistant_options(row: &Value, message_value: &Value, line: usize) -> ProviderOptions {
    let mut options = row_options(row, line);
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

#[cfg(test)]
mod tests {
    //! End-to-end test for the pi-coding-agent adapter: ingest the committed fixture corpus
    //! and assert pond's canonical Session/Message/Part shape comes out the
    //! other side. The fixture lives under `tests/fixtures/adapter/pi-coding-agent/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::PartKind};
    use tempfile::TempDir;

    const FIXTURES: &str = "tests/fixtures/adapter/pi-coding-agent/sessions";

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
}
