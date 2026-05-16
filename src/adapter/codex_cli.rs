//! OpenAI Codex CLI adapter.
//!
//! Source path: `~/.codex/sessions/<year>/<month>/<day>/rollout-<ts>-<uuid>.jsonl`.
//! Each line is an envelope `{timestamp, type, payload}`. Top-level types:
//! `session_meta` (consumed up front for Session), `event_msg` /
//! `turn_context` (transport noise, skipped), `response_item` (the per-turn
//! model interaction: subtypes `message`, `reasoning`, `function_call`,
//! `function_call_output`, `custom_tool_call`).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use async_stream::stream;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::{
    config::expand_home_under,
    sessions::IngestEvent,
    wire::{Message, Part, PartKind, ProviderOptions, Session},
};

use super::{
    Adapter, AdapterError, AdapterFactory, DiscoverFuture, Env, EventStream, collect_jsonl_files,
    empty_options,
    extract::{Extracted, extract_compact_repr, extract_self_str, extract_str},
    part_id,
};

const NAME: &str = "codex-cli";

/// Stateless factory: opens [`CodexCliAdapter`] instances and probes for the
/// canonical install location under `~/.codex/sessions`.
pub struct CodexCliFactory;

impl AdapterFactory for CodexCliFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        #[derive(Deserialize)]
        struct Cfg {
            path: PathBuf,
        }
        let cfg: Cfg = serde_json::from_value(config)
            .map_err(|err| AdapterError::config(NAME, format!("bad config blob: {err}")))?;
        let path = match std::env::var_os("HOME") {
            Some(home) => expand_home_under(&cfg.path, Path::new(&home)),
            None => cfg.path,
        };
        Ok(Box::new(CodexCliAdapter::new(path)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let path = env.home.join(".codex").join("sessions");
        path.exists().then(|| json!({ "path": path }))
    }
}

#[derive(Debug, Clone)]
pub struct CodexCliAdapter {
    root: PathBuf,
}

impl CodexCliAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for CodexCliAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        let root = self.root.clone();
        Box::pin(async move {
            // Header-free count: see the matching note on ClaudeCodeAdapter.
            let paths = collect_jsonl_files(&root)
                .await
                .map_err(|io| AdapterError::io(NAME, io.path, io.source))?;
            Ok(paths.len())
        })
    }

    fn events(&self) -> EventStream<'_> {
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
                let meta = match session_meta(&path, &path_display).await {
                    Ok(meta) => meta,
                    Err(error) => {
                        yield Err(error);
                        continue;
                    }
                };
                let session_id = meta.id.clone();
                let default_timestamp = meta.created_at;
                yield Ok(IngestEvent::Session(meta));

                let file = match tokio::fs::File::open(&path).await {
                    Ok(file) => file,
                    Err(source) => {
                        yield Err(AdapterError::io(NAME, path_display.clone(), source));
                        continue;
                    }
                };

                // One logical "message" per `response_item` record; the
                // function_call / function_call_output / reasoning rows
                // synthesize Assistant/Tool messages so they still hang off
                // a Part in pond's schema.
                //
                // Per-file `tool_call_names`: `function_call` rows carry the
                // tool name on the call side, but the matching
                // `function_call_output` row only carries `call_id`. Build
                // a map as we go so `tool_result_events` can resolve the
                // name from the prior call rather than synthesising
                // `"function"` (the previous sentinel). Misses yield
                // `name: None`. Per design.md invariant N.
                let mut lines = BufReader::new(file).lines();
                let mut line_number = 0usize;
                let mut tool_call_names: HashMap<String, Extracted<String>> = HashMap::new();
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
                    let row = match serde_json::from_str::<Value>(&line) {
                        Ok(value) => value,
                        Err(source) => {
                            // Same per-line skip policy as the claude-code
                            // adapter: surface the bad line, continue parsing.
                            yield Err(AdapterError::parse(
                                NAME,
                                path_display.clone(),
                                line_number,
                                source,
                            ));
                            continue;
                        }
                    };
                    // Capture (call_id -> name) for `function_call` rows
                    // before we hand the row off to events_from_row, so the
                    // matching `function_call_output` row downstream can
                    // resolve the tool name.
                    capture_tool_call_name(&row, &mut tool_call_names);
                    match events_from_row(
                        &session_id,
                        line_number,
                        &row,
                        default_timestamp,
                        &tool_call_names,
                    ) {
                        Ok(events) => {
                            for event in events {
                                yield Ok(event);
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

async fn session_meta(path: &Path, path_display: &str) -> Result<Session, AdapterError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?;
    let mut lines = BufReader::new(file).lines();
    let first = lines
        .next_line()
        .await
        .map_err(|source| AdapterError::io(NAME, path_display.to_owned(), source))?
        .ok_or_else(|| {
            AdapterError::schema(NAME, path_display.to_owned(), "empty jsonl session")
        })?;
    let row = serde_json::from_str::<Value>(&first)
        .map_err(|source| AdapterError::parse(NAME, path_display.to_owned(), 1, source))?;
    if row.get("type").and_then(Value::as_str) != Some("session_meta") {
        return Err(AdapterError::schema(
            NAME,
            format!("{path_display}:1"),
            "first row must be session_meta",
        ));
    }
    let payload = row.get("payload").cloned().unwrap_or(Value::Null);
    let id = payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                format!("{path_display}:1"),
                "session_meta missing payload.id",
            )
        })?
        .to_owned();
    let created_at = payload
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            row.get("timestamp")
                .and_then(Value::as_str)
                .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
                .map(|dt| dt.with_timezone(&Utc))
        })
        .ok_or_else(|| {
            AdapterError::schema(
                NAME,
                format!("{path_display}:1"),
                "session_meta has no parseable timestamp",
            )
        })?;
    let project = match extract_str(&payload, "cwd") {
        Some(value) => value,
        None => {
            let path_str = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path_display)
                .to_owned();
            extract_self_str(&Value::String(path_str)).ok_or_else(|| {
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
            "adapter": "codex-cli",
            "originator": payload.get("originator"),
            "cli_version": payload.get("cli_version"),
            "model_provider": payload.get("model_provider"),
            "git": payload.get("git"),
        }),
    );

    Ok(Session {
        id,
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "codex-cli".to_owned(),
        created_at,
        project,
        options,
    })
}

/// Map one codex-cli JSONL record into zero-or-more `IngestEvent`s. Records pond
/// keeps: `response_item` with `payload.type = "message"` (User/Assistant/
/// System message + text Parts), `function_call` (Assistant + ToolCall),
/// `function_call_output` (Tool + ToolResult), `reasoning` (Assistant +
/// Reasoning Part). `session_meta` is consumed up front; `event_msg` and
/// `turn_context` are transport noise.
fn events_from_row(
    session_id: &str,
    line: usize,
    row: &Value,
    default_timestamp: DateTime<Utc>,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Result<Vec<IngestEvent>, String> {
    let kind = row.get("type").and_then(Value::as_str).unwrap_or_default();
    if kind != "response_item" {
        return Ok(Vec::new());
    }
    let timestamp = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(default_timestamp);
    let payload = row.get("payload").unwrap_or(&Value::Null);
    let payload_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
    let message_id = format!("{session_id}:{line:06}");

    match payload_type {
        "message" => message_events(session_id, &message_id, timestamp, payload),
        "function_call" => Ok(tool_call_events(
            session_id,
            &message_id,
            timestamp,
            payload,
        )),
        "function_call_output" => Ok(tool_result_events(
            session_id,
            &message_id,
            timestamp,
            payload,
            tool_call_names,
        )),
        "reasoning" => Ok(reasoning_events(
            session_id,
            &message_id,
            timestamp,
            payload,
        )),
        // Unknown response_item subtypes (newer codex-cli versions) - skip
        // rather than fail the session.
        _ => Ok(Vec::new()),
    }
}

/// Stash one row's `function_call` (call_id -> name) into the per-file
/// map so the matching `function_call_output` row downstream can resolve
/// the tool name rather than fall back to a sentinel.
fn capture_tool_call_name(row: &Value, map: &mut HashMap<String, Extracted<String>>) {
    if row.get("type").and_then(Value::as_str) != Some("response_item") {
        return;
    }
    let Some(payload) = row.get("payload") else {
        return;
    };
    if payload.get("type").and_then(Value::as_str) != Some("function_call") {
        return;
    }
    let Some(call_id) = payload.get("call_id").and_then(Value::as_str) else {
        return;
    };
    let Some(name) = extract_str(payload, "name") else {
        return;
    };
    map.insert(call_id.to_owned(), name);
}

fn message_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
) -> Result<Vec<IngestEvent>, String> {
    let role = payload
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| "message missing role".to_owned())?;
    let content = payload
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut parts = Vec::with_capacity(content.len());
    for (ordinal, item) in content.iter().enumerate() {
        // Faithful encoding of one content item: prefer the raw `text`
        // field when present; otherwise compact-encode the structured
        // body as a JSON string. The fallback is lossless (preserves the
        // item bytes) and explicit (not a synthesised "unknown" or "").
        let text = extract_str(item, "text").or_else(|| Some(extract_compact_repr(item)));
        parts.push(Part {
            id: part_id(message_id, ordinal),
            message_id: message_id.to_owned(),
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
            options: empty_options(),
            kind: PartKind::Text { text },
        });
    }

    let (message, keep_parts) = match role {
        "user" => (
            Message::User {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: empty_options(),
            },
            true,
        ),
        "assistant" => (
            Message::Assistant {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                options: empty_options(),
            },
            true,
        ),
        // `developer` rows are codex-cli's system-prompt frames; map to System
        // with `content: None` and let the inner Text Parts carry the
        // body. The previous join-aggregation hack double-stored data and
        // can't reconstruct an `Extracted<String>` from synthesised text
        // under the new adapter seam; dropping it is a simplification.
        "developer" | "system" => (
            Message::System {
                id: message_id.to_owned(),
                session_id: session_id.to_owned(),
                timestamp,
                content: None,
                options: empty_options(),
            },
            true,
        ),
        other => return Err(format!("unsupported codex-cli role {other}")),
    };

    let mut events = Vec::with_capacity(parts.len() + 1);
    events.push(IngestEvent::Message(message));
    if keep_parts {
        events.extend(parts.into_iter().map(IngestEvent::Part));
    }
    Ok(events)
}

fn tool_call_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
    let name = extract_str(payload, "name");
    let params = match payload.get("arguments") {
        Some(Value::String(text)) => {
            serde_json::from_str::<Value>(text).unwrap_or_else(|_| Value::String(text.clone()))
        }
        Some(other) => other.clone(),
        None => Value::Null,
    };
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::ToolCall {
            call_id,
            name,
            params,
            provider_executed: false,
        },
    };
    vec![
        IngestEvent::Message(Message::Assistant {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: empty_options(),
        }),
        IngestEvent::Part(part),
    ]
}

fn tool_result_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
    tool_call_names: &HashMap<String, Extracted<String>>,
) -> Vec<IngestEvent> {
    let call_id = extract_str(payload, "call_id");
    // Resolve tool name from the prior `function_call` row via the
    // per-file `call_id -> name` map. Misses (e.g. compaction pruned the
    // originating call) yield `None`, a faithful "unresolved" rather than
    // the previous `"function"` sentinel.
    let name = call_id
        .as_ref()
        .and_then(|id| tool_call_names.get(id.as_str()))
        .cloned();
    let result = payload.get("output").cloned().unwrap_or(Value::Null);
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id,
            name,
            is_failure: false,
            result,
        },
    };
    vec![
        IngestEvent::Message(Message::Tool {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: empty_options(),
        }),
        IngestEvent::Part(part),
    ]
}

fn reasoning_events(
    session_id: &str,
    message_id: &str,
    timestamp: DateTime<Utc>,
    payload: &Value,
) -> Vec<IngestEvent> {
    // The source `summary` array is the only place reasoning text lives in
    // codex-cli's format. Empty array (or missing field) -> `None`. Joined
    // text -> `Some(...)`. Don't synthesize an empty string.
    let summary = payload
        .get("summary")
        .and_then(Value::as_array)
        .and_then(|items| {
            let joined = items
                .iter()
                .filter_map(|item| extract_str(item, "text"))
                .map(|e| (*e).clone())
                .collect::<Vec<_>>()
                .join("\n");
            if joined.is_empty() {
                None
            } else {
                Some(extract_compact_repr(payload))
            }
        });
    let part = Part {
        id: part_id(message_id, 0),
        message_id: message_id.to_owned(),
        ordinal: 0,
        options: empty_options(),
        kind: PartKind::Reasoning { text: summary },
    };
    vec![
        IngestEvent::Message(Message::Assistant {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: empty_options(),
        }),
        IngestEvent::Part(part),
    ]
}

#[cfg(test)]
mod tests {
    //! End-to-end test for the codex-cli adapter: ingest the committed fixture
    //! corpus and assert pond's canonical Session/Message/Part shape comes out
    //! the other side. The fixture lives under
    //! `tests/fixtures/session-samples/codex-cli/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store, wire::PartKind};
    use tempfile::TempDir;

    const FIXTURES: &str = "tests/fixtures/session-samples/codex-cli/sessions";

    #[tokio::test(flavor = "multi_thread")]
    async fn codex_cli_adapter_ingests_fixture_corpus_into_canonical_shape() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = CodexCliAdapter::new(FIXTURES);

        let summary = ingest_adapter(&store, &adapter, |_| {}).await?;
        assert!(summary.accepted() > 0, "ingest must accept rows");
        assert_eq!(summary.dropped_events, 0, "no per-event drops expected");
        assert_eq!(
            summary.dropped_sessions, 0,
            "no session-level rejections expected"
        );
        assert_eq!(summary.skipped_files, 0, "no whole-file skips expected");

        let (sessions, messages, parts, _) = store.row_counts().await?;
        assert!(sessions > 0, "at least one codex-cli session");
        assert!(messages > 0, "at least one codex-cli message");
        assert!(parts > 0, "at least one codex-cli Part");

        let mut saw_text_part = false;
        for session_id in store.session_ids().await? {
            let session = store
                .get_session(&session_id)
                .await?
                .expect("session round-trips");
            assert_eq!(session.session.source_agent, "codex-cli");
            assert!(
                !session.messages.is_empty(),
                "session {session_id} must carry messages",
            );
            for stored in &session.messages {
                for part in &stored.parts {
                    if matches!(part.kind, PartKind::Text { .. }) {
                        saw_text_part = true;
                    }
                }
            }
        }
        assert!(
            saw_text_part,
            "codex-cli corpus must contain at least one Text Part",
        );
        Ok(())
    }
}
