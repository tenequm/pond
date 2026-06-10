//! claude.ai data-export adapter - web-chat session logs.
//!
//! Source: the official claude.ai export `.zip` (emailed link), whose
//! `conversations.json` entry is a single JSON ARRAY of conversation objects -
//! many sessions in one file, not one file per session. So this adapter drives
//! the [`Adapter`] seam directly rather than through `JsonlTree`: it parses the
//! array once and emits `Session -> Message -> Parts` per conversation.
//!
//! There is no auto-discovery: an export `.zip` is a manual, point-in-time
//! download that can land anywhere, so `probe_default` returns `None` (an
//! opportunistic `~/Downloads` scan would silently latch onto a stale or wrong
//! archive). Point the adapter at the file explicitly via
//! `[sources.claude-ai-export]` config or
//! `pond sync claude-ai-export --source-dir <export.zip>`. `open` accepts a
//! `.zip`, an extracted directory, or a bare `conversations.json` path.
//!
//! Mapping (spec.md#model-*): `conversation.uuid` -> `Session.id`;
//! `account.uuid` -> `Session.project` (every conversation carries it, and 225
//! of 1488 have an empty `name`, so the title can't anchor the project);
//! `sender` `human`/`assistant` -> `User`/`Assistant`, a human turn that is
//! purely `tool_result` blocks -> a `Tool` message. Content blocks map text ->
//! `Text`, thinking -> `Reasoning`, tool_use -> `ToolCall`, tool_result ->
//! `ToolResult`. The export carries no attachment bytes, so file references stay
//! in `options.source`.

use std::path::{Path, PathBuf};

use async_stream::stream;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::{
    sessions::IngestEvent,
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYield, AdapterYieldStream, DiscoverFuture, Env,
    RestoreFidelity, RestoredFile, SkipOracle, SkipReason, compact_json, config_path,
    empty_options,
    extract::{bound_value, extract_compact_repr, extract_str},
    extracted_text, part_id, part_ordinal, raw_record, source_options,
};

const NAME: &str = "claude-ai-export";

/// The single entry inside the export `.zip` (and the file name inside an
/// extracted directory) that holds the web-chat session logs.
const CONVERSATIONS_ENTRY: &str = "conversations.json";

/// Stateless factory: opens [`ClaudeAiExportAdapter`] instances. Has no
/// `probe_default` (the export is a manual download with no canonical path), so
/// it is configured explicitly rather than auto-discovered.
pub struct ClaudeAiExportFactory;

impl AdapterFactory for ClaudeAiExportFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(ClaudeAiExportAdapter::new(config_path(
            NAME, config,
        )?)))
    }

    fn probe_default(&self, _env: &Env) -> Option<Value> {
        // No canonical install path: the export is a manual download. The user
        // points pond at it (config `path` or `pond sync ... --source-dir`).
        None
    }

    fn serialize(
        &self,
        session: &crate::sessions::SessionWithMessages,
        fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        serialize_session(session, fidelity)
    }
}

/// Configured export reader. `path` is a `.zip`, an extracted directory holding
/// `conversations.json`, or a `conversations.json` file directly.
#[derive(Debug, Clone)]
pub struct ClaudeAiExportAdapter {
    path: PathBuf,
}

impl ClaudeAiExportAdapter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl Adapter for ClaudeAiExportAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let path = self.path.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                read_conversations(&path).map(|conversations| {
                    conversations
                        .iter()
                        // Match what events_with actually emits: a session needs an
                        // honest `uuid` (its id) and at least one message.
                        .filter(|conv| {
                            conv.get("uuid").and_then(Value::as_str).is_some()
                                && !messages_of(conv).is_empty()
                        })
                        .count()
                })
            })
            .await
            .map_err(join_error)?
        })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        let path = self.path.clone();
        Box::pin(stream! {
            // Parse the (large) array off-thread; the per-conversation mapping
            // and the oracle check then run inline (the oracle borrows `self`,
            // so it can't cross into the blocking task).
            let parsed = tokio::task::spawn_blocking(move || read_conversations(&path)).await;
            let conversations = match parsed {
                Ok(Ok(conversations)) => conversations,
                Ok(Err(error)) => { yield Err(error); return; }
                Err(join) => { yield Err(join_error(join)); return; }
            };

            for mut conv in conversations {
                bound_value(&mut conv);
                let Some(session_id) = conv.get("uuid").and_then(Value::as_str).map(ToOwned::to_owned)
                else {
                    yield Err(AdapterError::schema(
                        NAME,
                        CONVERSATIONS_ENTRY,
                        "conversation missing `uuid`",
                    ));
                    continue;
                };
                if messages_of(&conv).is_empty() {
                    // A 0-message conversation produces no importable session.
                    yield Ok(AdapterYield::Skipped {
                        session_id: Some(session_id),
                        project: conv
                            .get("account")
                            .and_then(|account| account.get("uuid"))
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        reason: SkipReason::Empty,
                    });
                    continue;
                }
                // The conversation list is sorted by `updated_at`; skip a
                // conversation whose latest edit predates our watermark.
                if let Some(ingested) = oracle.last_ingested_at(&session_id)
                    && let Some(updated) = rfc3339(&conv, "updated_at")
                    && updated <= ingested
                {
                    yield Ok(AdapterYield::Skipped {
                        session_id: Some(session_id),
                        project: None,
                        reason: SkipReason::Fresh,
                    });
                    continue;
                }

                let session = match build_session(&conv, &session_id) {
                    Ok(session) => session,
                    Err(error) => { yield Err(error); continue; }
                };
                let created_at = session.created_at;
                yield Ok(AdapterYield::Event(IngestEvent::Session(session)));

                for (index, message) in messages_of(&conv).iter().enumerate() {
                    for event in message_events(&session_id, message, index, created_at) {
                        yield Ok(AdapterYield::Event(event));
                    }
                }
            }
        })
    }
}

/// A blocking-task panic is a pond bug, not bad source data.
fn join_error(join: tokio::task::JoinError) -> AdapterError {
    AdapterError::io(
        NAME,
        "blocking read task",
        std::io::Error::other(join.to_string()),
    )
}

fn messages_of(conv: &Value) -> &[Value] {
    conv.get("chat_messages")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// Read `conversations.json` from a `.zip`, an extracted directory, or a direct
/// JSON path, and return its conversation array.
fn read_conversations(path: &Path) -> Result<Vec<Value>, AdapterError> {
    use std::io::Read;
    let io = |location: String, source| AdapterError::io(NAME, location, source);

    let bytes = if path.is_dir() {
        let file = path.join(CONVERSATIONS_ENTRY);
        std::fs::read(&file).map_err(|error| io(file.display().to_string(), error))?
    } else if path.extension().and_then(|ext| ext.to_str()) == Some("zip") {
        let file =
            std::fs::File::open(path).map_err(|error| io(path.display().to_string(), error))?;
        let mut archive = zip::ZipArchive::new(file).map_err(|error| {
            AdapterError::schema(
                NAME,
                path.display().to_string(),
                format!("bad zip: {error}"),
            )
        })?;
        let mut entry = archive.by_name(CONVERSATIONS_ENTRY).map_err(|error| {
            AdapterError::schema(
                NAME,
                path.display().to_string(),
                format!("export zip has no `{CONVERSATIONS_ENTRY}`: {error}"),
            )
        })?;
        // Don't pre-allocate from the zip's self-declared size - a lying header
        // could force a huge up-front allocation. Cap the hint and let
        // read_to_end grow for genuinely large exports.
        let hint = entry.size().min(64 * 1024 * 1024) as usize;
        let mut buf = Vec::with_capacity(hint);
        entry
            .read_to_end(&mut buf)
            .map_err(|error| io(path.display().to_string(), error))?;
        buf
    } else {
        std::fs::read(path).map_err(|error| io(path.display().to_string(), error))?
    };

    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| AdapterError::parse(NAME, path.display().to_string(), 1, error))?;
    match value {
        Value::Array(conversations) => Ok(conversations),
        _ => Err(AdapterError::schema(
            NAME,
            path.display().to_string(),
            format!("`{CONVERSATIONS_ENTRY}` is not a JSON array"),
        )),
    }
}

fn build_session(conv: &Value, session_id: &str) -> Result<Session, AdapterError> {
    let created_at = rfc3339(conv, "created_at").ok_or_else(|| {
        AdapterError::schema(
            NAME,
            session_id.to_owned(),
            "conversation missing/invalid `created_at`",
        )
    })?;
    // spec.md#model-project-non-empty: `account.uuid` is present on every
    // conversation - all of an account's loose web chats group under it.
    let project = conv
        .get("account")
        .and_then(|account| extract_str(account, "uuid"))
        // spec.md#model-project-non-empty: an empty `account.uuid` is not a
        // valid project; reject rather than store an empty string.
        .filter(|uuid| !uuid.trim().is_empty())
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                session_id.to_owned(),
                "conversation missing/empty `account.uuid` for the project",
            )
        })?;

    let mut options = source_options(NAME, conv);
    if let Some(source) = options.get_mut("source").and_then(Value::as_object_mut) {
        for key in ["name", "summary", "updated_at"] {
            if let Some(value) = conv.get(key) {
                source.insert(key.to_owned(), value.clone());
            }
        }
    }

    Ok(Session {
        id: session_id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: NAME.to_owned(),
        created_at,
        project,
        options,
    })
}

fn message_events(
    session_id: &str,
    message: &Value,
    index: usize,
    default_ts: DateTime<Utc>,
) -> Vec<IngestEvent> {
    // A uuid-less message still ingests under a deterministic synthetic id (a
    // transport/absence default per spec.md#model-no-synthesis), never silently
    // dropped (spec.md#adapter-integrity-no-silent-drops).
    let message_id = message
        .get("uuid")
        .and_then(Value::as_str)
        .map_or_else(|| format!("{session_id}:{index}"), ToOwned::to_owned);
    let timestamp = rfc3339(message, "created_at").unwrap_or(default_ts);
    let blocks = message
        .get("content")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let sender = message.get("sender").and_then(Value::as_str);
    let all_tool_results = !blocks.is_empty() && blocks.iter().all(is_tool_result);

    let parts: Vec<Part> = blocks
        .iter()
        .enumerate()
        .map(|(ordinal, block)| content_part(session_id, &message_id, ordinal, block))
        .collect();

    let options = message_options(message);
    let header = match (sender, all_tool_results) {
        // A human turn that is purely tool feedback is a Tool message.
        (Some("human"), true) => Message::Tool {
            id: message_id.clone(),
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
        (Some("assistant"), _) => Message::Assistant {
            id: message_id.clone(),
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
        // `human` (default) and any unexpected sender map to a User turn; the
        // raw record in options keeps the original sender for losslessness.
        _ => Message::User {
            id: message_id.clone(),
            session_id: session_id.to_owned(),
            timestamp,
            options,
        },
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(header));
    events.extend(parts.into_iter().map(IngestEvent::Part));
    events
}

fn content_part(session_id: &str, message_id: &str, ordinal: usize, block: &Value) -> Part {
    let (provenance, kind) = match block.get("type").and_then(Value::as_str) {
        Some("text") => (
            Provenance::Conversational,
            PartKind::Text {
                text: extract_str(block, "text"),
            },
        ),
        Some("thinking") => (
            Provenance::Conversational,
            PartKind::Reasoning {
                text: extract_str(block, "thinking"),
            },
        ),
        Some("tool_use") => (
            Provenance::Conversational,
            PartKind::ToolCall {
                call_id: extract_str(block, "id"),
                name: extract_str(block, "name"),
                params: block.get("input").cloned().unwrap_or(Value::Null),
                provider_executed: true,
            },
        ),
        Some("tool_result") => (
            Provenance::Injected,
            PartKind::ToolResult {
                // The export's tool_result carries the tool `name` but no
                // tool_use_id, so there is no honest call link to populate
                // (spec.md#model-no-synthesis): `None`, never a name-matched
                // guess that could point at the wrong same-named call.
                call_id: None,
                name: extract_str(block, "name"),
                is_failure: block
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                result: block.get("content").cloned().unwrap_or(Value::Null),
            },
        ),
        // Unknown block: preserve verbatim in a Text slot (lossless encoding,
        // not a synthesized value).
        _ => (
            Provenance::Conversational,
            PartKind::Text {
                text: Some(extract_compact_repr(block)),
            },
        ),
    };
    Part {
        session_id: session_id.to_owned(),
        id: part_id(message_id, ordinal),
        message_id: message_id.to_owned(),
        ordinal: part_ordinal(ordinal),
        provenance,
        options: empty_options(),
        kind,
    }
}

fn message_options(message: &Value) -> ProviderOptions {
    let mut options = source_options(NAME, message);
    if let Some(source) = options.get_mut("source").and_then(Value::as_object_mut) {
        for key in ["sender", "updated_at", "attachments", "files"] {
            if let Some(value) = message.get(key) {
                source.insert(key.to_owned(), value.clone());
            }
        }
    }
    options
}

fn is_tool_result(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("tool_result")
}

fn rfc3339(value: &Value, key: &str) -> Option<DateTime<Utc>> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn serialize_session(
    session: &crate::sessions::SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    // The export is one shared `conversations.json`; per-session restore emits a
    // minimal one-element export holding just this conversation. Native replays
    // the stored `raw_record`; foreign (or a session that lacks raw records)
    // rebuilds a conversation object from canonical data.
    let conversation = match fidelity {
        RestoreFidelity::Native => raw_record(&session.session.options),
        RestoreFidelity::Foreign => None,
    };
    let actual_fidelity = if conversation.is_some() {
        RestoreFidelity::Native
    } else {
        RestoreFidelity::Foreign
    };
    let conversation = conversation.unwrap_or_else(|| foreign_conversation(session));

    Ok(vec![RestoredFile::new(
        PathBuf::from(CONVERSATIONS_ENTRY),
        serde_json::to_vec(&Value::Array(vec![conversation])).map_err(|error| {
            AdapterError::schema(
                NAME,
                &session.session.id,
                format!("json encode failed: {error}"),
            )
        })?,
        actual_fidelity,
    )])
}

/// Best-effort conversation object for a foreign session: the fields
/// `build_session` and `message_events` read back, from canonical data.
fn foreign_conversation(session: &crate::sessions::SessionWithMessages) -> Value {
    let chat_messages: Vec<Value> = session
        .messages
        .iter()
        .map(|message| {
            let sender = match message.message {
                Message::Assistant { .. } => "assistant",
                _ => "human",
            };
            json!({
                "uuid": message.message.id(),
                "sender": sender,
                "created_at": message.message.timestamp().to_rfc3339(),
                "content": message.parts.iter().map(content_block).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({
        "uuid": session.session.id,
        "account": { "uuid": &*session.session.project },
        "created_at": session.session.created_at.to_rfc3339(),
        "chat_messages": chat_messages,
    })
}

fn content_block(part: &Part) -> Value {
    match &part.kind {
        PartKind::Text { text } => json!({ "type": "text", "text": extracted_text(text) }),
        PartKind::Reasoning { text } => {
            json!({ "type": "thinking", "thinking": extracted_text(text) })
        }
        PartKind::ToolCall {
            call_id,
            name,
            params,
            ..
        } => json!({
            "type": "tool_use",
            "id": extracted_text(call_id),
            "name": extracted_text(name),
            "input": params,
        }),
        PartKind::ToolResult {
            name,
            is_failure,
            result,
            ..
        } => json!({
            "type": "tool_result",
            "name": extracted_text(name),
            "is_error": is_failure,
            "content": result,
        }),
        other => json!({
            "type": "text",
            "text": compact_json(&serde_json::to_value(other).unwrap_or(Value::Null)),
        }),
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end tests over the synthetic export fixture
    //! (`tests/fixtures/adapter/claude_ai_export/conversations.json`), covering
    //! the directory and `.zip` source forms, the 0-message skip, the empty-name
    //! conversation, and tool_use/tool_result linkage.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store};
    use tempfile::TempDir;

    // Manifest-dir anchored: unit tests must not depend on the process cwd
    // (figment::Jail chdirs the whole test process while config tests run).
    const FIXTURE_DIR: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/claude_ai_export"
    );
    const ACCOUNT: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";
    const TOOL_CONV: &str = "33333333-3333-3333-3333-333333333333";
    const EMPTY_NAME_CONV: &str = "44444444-4444-4444-4444-444444444444";
    const ZERO_MESSAGE_CONV: &str = "55555555-5555-5555-5555-555555555555";

    #[test]
    fn probe_default_returns_none_no_autodiscovery() {
        // The export is a manual download; no path is auto-discoverable.
        assert!(
            ClaudeAiExportFactory
                .probe_default(&Env::with_home("/tmp"))
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ingests_export_directory_into_canonical_shape() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let summary = ingest_adapter(
            &store,
            &ClaudeAiExportAdapter::new(FIXTURE_DIR),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(summary.dropped_sessions, 0);

        let ids = store.session_ids().await?;
        // 5 conversations, minus the 0-message one.
        assert_eq!(ids.len(), 4, "the 0-message conversation is skipped");
        assert!(
            !ids.iter().any(|id| id == ZERO_MESSAGE_CONV),
            "0-message conversation must not become a session",
        );
        assert!(
            ids.iter().any(|id| id == EMPTY_NAME_CONV),
            "an empty-name conversation still ingests (title can't gate it)",
        );

        for id in &ids {
            let session = store.get_session(id).await?.expect("round-trips");
            assert_eq!(session.session.source_agent, NAME);
            assert_eq!(
                &*session.session.project, ACCOUNT,
                "spec.md#model-project-non-empty: project = account.uuid",
            );
        }

        let tool = store
            .get_session(TOOL_CONV)
            .await?
            .expect("tool conversation");
        let mut saw_call = false;
        let mut saw_reasoning = false;
        let mut tool_result = None;
        let mut saw_tool_message = false;
        for stored in &tool.messages {
            if matches!(stored.message, Message::Tool { .. }) {
                saw_tool_message = true;
            }
            for part in &stored.parts {
                match &part.kind {
                    PartKind::ToolCall { name, .. }
                        if name.as_deref().map(String::as_str) == Some("web_search") =>
                    {
                        saw_call = true;
                    }
                    PartKind::Reasoning { .. } => saw_reasoning = true,
                    PartKind::ToolResult { call_id, name, .. } => {
                        tool_result = Some((call_id.as_deref().cloned(), name.as_deref().cloned()));
                    }
                    _ => {}
                }
            }
        }
        // The thinking conversation carries the Reasoning part.
        let thinking = store
            .get_session("22222222-2222-2222-2222-222222222222")
            .await?
            .expect("thinking conversation");
        for stored in &thinking.messages {
            for part in &stored.parts {
                if matches!(part.kind, PartKind::Reasoning { .. }) {
                    saw_reasoning = true;
                }
            }
        }
        assert!(saw_call, "tool_use -> ToolCall named web_search");
        assert!(saw_reasoning, "thinking -> Reasoning");
        assert!(
            saw_tool_message,
            "a human turn of pure tool_result is a Tool message",
        );
        let (call_id, name) = tool_result.expect("tool conversation has a ToolResult");
        assert_eq!(
            name.as_deref(),
            Some("web_search"),
            "tool_result name comes straight off the block",
        );
        assert_eq!(
            call_id, None,
            "the export carries no tool_use_id on tool_result, so call_id is honestly None",
        );
        Ok(())
    }

    /// spec.md#adapter-integrity-no-silent-drops: a message lacking a `uuid`
    /// must still ingest (under a deterministic synthetic id), not vanish with
    /// its parts.
    #[test]
    fn uuid_less_message_ingests_under_synthetic_id() {
        let ts = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let message = json!({
            "sender": "human",
            "content": [{ "type": "text", "text": "no uuid here" }],
        });
        let events = message_events("conv-xyz", &message, 3, ts);
        assert_eq!(
            events.len(),
            2,
            "uuid-less message still emits its Message + Part, not dropped",
        );
        match &events[0] {
            IngestEvent::Message(message) => {
                assert_eq!(message.id(), "conv-xyz:3", "deterministic synthetic id");
            }
            _ => panic!("first event must be the Message"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ingests_export_zip() -> anyhow::Result<()> {
        use std::io::Write;
        let temp = TempDir::new()?;
        let zip_path = temp.path().join("data-2026-01-15-00-00-00-batch-0000.zip");
        let conversations = std::fs::read(format!("{FIXTURE_DIR}/conversations.json"))?;
        {
            let file = std::fs::File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            zip.start_file(
                "conversations.json",
                zip::write::SimpleFileOptions::default(),
            )?;
            zip.write_all(&conversations)?;
            zip.finish()?;
        }

        let store = Store::open_local(temp.path().join("store")).await?;
        ingest_adapter(
            &store,
            &ClaudeAiExportAdapter::new(&zip_path),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(
            store.session_ids().await?.len(),
            4,
            "the same four sessions ingest from the zip",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_round_trips_one_conversation() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path().join("store")).await?;
        ingest_adapter(
            &store,
            &ClaudeAiExportAdapter::new(FIXTURE_DIR),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        let session = store
            .get_session(TOOL_CONV)
            .await?
            .expect("tool conversation");

        let files = ClaudeAiExportFactory.serialize(&session, RestoreFidelity::Native)?;
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].relative_path,
            std::path::Path::new(CONVERSATIONS_ENTRY)
        );
        let value: Value = serde_json::from_slice(&files[0].bytes)?;
        let array = value.as_array().expect("conversations.json is an array");
        assert_eq!(
            array.len(),
            1,
            "per-session restore is a one-conversation export"
        );
        assert_eq!(
            array[0].get("uuid").and_then(Value::as_str),
            Some(TOOL_CONV),
        );

        let restore_dir = temp.path().join("restore");
        std::fs::create_dir_all(&restore_dir)?;
        std::fs::write(restore_dir.join(CONVERSATIONS_ENTRY), &files[0].bytes)?;
        let restore_store = Store::open_local(temp.path().join("restore-store")).await?;
        ingest_adapter(
            &restore_store,
            &ClaudeAiExportAdapter::new(&restore_dir),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        let restored = restore_store
            .get_session(TOOL_CONV)
            .await?
            .expect("restored");
        assert_eq!(
            restored.messages.len(),
            session.messages.len(),
            "native restore replays every message",
        );
        Ok(())
    }
}
