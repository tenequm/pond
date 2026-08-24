//! letta-code adapter (github.com/letta-ai/letta-code, the `letta` CLI).
//!
//! Source: `~/.letta/transcripts/<agentId>/<conversationId>/transcript.jsonl`
//! (letta relocates the root via `$LETTA_TRANSCRIPT_ROOT`; pond takes a
//! relocated root as an explicit `path`), the append-only client-side
//! transcript every letta-code producer writes on `end_turn`. One JSON row per
//! line, two shapes: `{kind: user|assistant|reasoning|error, text}` and
//! `{kind: tool_call, name?, argsText?, resultText?, resultOk?}`, each with a
//! per-turn `captured_at` stamp and optional `source_line_id` /
//! `source_message_id`. Format archaeology and the decision record live in
//! `docs/adapters/letta-code.md`.
//!
//! Identity is the path: the session id is `<agent-dir>+<conversation-dir>`
//! (letta sanitizes both to `[A-Za-z0-9._-]`, so the directory names ARE the
//! ids and the out-of-alphabet `+` joins them injectively; see [`session_id`]
//! for why not `/` or `:`), the project is the agent id (the transcript carries no cwd; the agent
//! is letta's shared-state scope), and message ids are position-derived
//! (`<session>:<line:06>`) because letta's `letta-msg-<n>` line ids are
//! process-scoped counters that repeat inside one conversation.
//!
//! A `tool_call` row carries call and result together: it becomes an
//! Assistant message with a ToolCall Part and, when result fields exist, a
//! Tool message with the ToolResult Part under the same `call_id`. `error`
//! rows and unknown kinds ride placement rule 3 (a System carrier with the
//! whole record in `options`). Native restore replays `raw_record` rows into
//! the source layout; foreign restore rebuilds rows from Parts.

use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYieldStream, DiscoverFuture, Env, PlanFuture,
    RestoreFidelity, RestoredFile, SkipOracle, SourceWatermark, by_timestamp_then_id, compact_json,
    config_path, empty_options,
    extract::{Extracted, extract_bool, extract_self_str, extract_str, json_or_string},
    extracted_text,
    jsonl::{
        BoundedRow, JsonlTree, jsonl_tree_discover, jsonl_tree_events, jsonl_tree_plan,
        peek_last_line, source_line,
    },
    jsonl_bytes, part_id, raw_record, source_options, validate_path_id,
};
use crate::{
    sessions::{IngestEvent, MessageWithParts, SessionWithMessages},
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

const NAME: &str = "letta-code";
const TRANSCRIPT_FILE: &str = "transcript.jsonl";
/// The per-agent directory holding multi-conversation reflection payloads.
/// Structurally never a session (`reflection-transcript.ts` writes only
/// `payload-*.json` there), so the walk prunes it.
const PAYLOADS_DIR: &str = "multi-reflection-payloads";
/// Suffix that names a reconstructed transcript distinctly from the source
/// file it was reconstructed away from
/// (spec.md#adapter-restore-distinct-reconstruction).
const RECONSTRUCTED_SUFFIX: &str = "-reconstructed";
/// `options.source` key marking a message derived from a row another message
/// already carries as `raw_record`; native restore replays each row once.
const DERIVED_FROM_KEY: &str = "derived_from";

/// Stateless factory: opens [`LettaCodeAdapter`] instances and probes for the
/// transcript root under `~/.letta/transcripts`.
pub struct LettaCodeFactory;

impl AdapterFactory for LettaCodeFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(LettaCodeAdapter::new(config_path(NAME, config)?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let root = env.home.join(".letta").join("transcripts");
        root.exists().then(|| json!({ "path": root }))
    }

    fn serialize(
        &self,
        session: &SessionWithMessages,
        fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError> {
        serialize_session(session, fidelity)
    }
}

/// Configured letta-code reader: the transcript root, one
/// `<agent>/<conversation>/transcript.jsonl` per session.
#[derive(Debug, Clone)]
pub struct LettaCodeAdapter {
    root: PathBuf,
}

impl LettaCodeAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Adapter for LettaCodeAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        jsonl_tree_discover(self)
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        jsonl_tree_events(self, oracle)
    }

    fn plan<'a>(&'a self, oracle: &'a dyn SkipOracle) -> PlanFuture<'a> {
        jsonl_tree_plan(self, oracle)
    }
}

impl JsonlTree for LettaCodeAdapter {
    type State = ();

    fn name(&self) -> &'static str {
        NAME
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn is_transcript(&self, path: &Path) -> bool {
        path.file_name().is_some_and(|name| name == TRANSCRIPT_FILE)
    }

    fn skip_source(&self, path: &Path) -> bool {
        path.file_name().is_some_and(|name| name == PAYLOADS_DIR)
    }

    /// The id is the path, so the freshness gate never reads the file.
    fn peek_session_id(&self, path: &Path, _first_line: &str) -> Option<String> {
        placement(&self.root, path).map(|(agent, conversation)| session_id(&agent, &conversation))
    }

    fn peeks_first_line(&self) -> bool {
        false
    }

    fn peek_watermark(&self, path: &Path) -> SourceWatermark {
        // Append-only writer (`appendFile` only; `state.json` is the file that
        // gets rewritten), so the last line carries the latest stamp.
        let last = || -> Option<i64> {
            let row: Value = serde_json::from_str(&peek_last_line(path)?).ok()?;
            captured_at(&row).map(|ts| ts.timestamp_micros())
        };
        match last() {
            Some(ts) => SourceWatermark::At(ts),
            None => SourceWatermark::Opaque,
        }
    }

    fn unsupported_path(&self, path: &Path) -> Option<String> {
        // A transcript at any other depth has no agent/conversation pair to
        // name it; borrowing a neighbour's id would fold it into another
        // session, so it is a visible, counted skip instead.
        placement(&self.root, path).is_none().then(|| {
            format!(
                "{}: {TRANSCRIPT_FILE} is not at <root>/<agent>/<conversation>/ under {}; \
                 point the adapter at the transcripts root",
                path.display(),
                self.root.display(),
            )
        })
    }

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
        let schema_error =
            |message: &str| AdapterError::schema(NAME, path.display().to_string(), message);
        let (agent, conversation) = placement(&self.root, path)
            .ok_or_else(|| schema_error("transcript is not two levels deep"))?;
        // Rows are append-ordered, so the first parseable stamp is the
        // session's start; a transcript with rows but no stamp at all is not
        // a decodable session.
        let created_at = rows
            .iter()
            .find_map(|row| captured_at(&row.value))
            .ok_or_else(|| schema_error("no row carries a parseable captured_at"))?;
        // The agent id is real source data read from the path (the writer put
        // it there), viewed through the seam like codex-cli's filename fallback.
        let project = extract_self_str(&Value::String(agent.clone())).ok_or_else(|| {
            schema_error("internal: Value::String produced None from Source::as_str")
        })?;
        let mut options = ProviderOptions::new();
        options.insert(
            "source".to_owned(),
            json!({
                "adapter": NAME,
                "agent_dir": agent,
                "conversation_dir": conversation,
            }),
        );
        Ok(Session {
            id: session_id(&agent, &conversation),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: NAME.to_owned(),
            created_at,
            project,
            options,
        })
    }

    fn events_from_row(
        &self,
        session: &Session,
        row: &BoundedRow,
        _state: &mut Self::State,
    ) -> Result<Vec<IngestEvent>, String> {
        Ok(events_from_row(session, row.line, &row.value))
    }
}

/// `<agent>/<conversation>` for a transcript exactly two directories below
/// `root`, else `None`.
fn placement(root: &Path, path: &Path) -> Option<(String, String)> {
    let conversation_dir = path.parent()?;
    let agent_dir = conversation_dir.parent()?;
    if agent_dir.parent()? != root {
        return None;
    }
    let name = |dir: &Path| dir.file_name()?.to_str().map(ToOwned::to_owned);
    Some((name(agent_dir)?, name(conversation_dir)?))
}

/// `+` because it is outside letta's directory alphabet (`[A-Za-z0-9._-]`,
/// so the join is injective) and is legal in a filename on every platform.
/// Not `/` (the search layer reads it as the claude-code subagent marker and
/// drops the hit from every default search) and not `:` (an NTFS
/// alternate-data-stream name, refused when a foreign-restore target embeds
/// the id in a filename).
fn session_id(agent: &str, conversation: &str) -> String {
    format!("{agent}+{conversation}")
}

fn captured_at(row: &Value) -> Option<DateTime<Utc>> {
    let text = row.get("captured_at")?.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|ts| ts.with_timezone(&Utc))
}

fn message_id(session_id: &str, line: usize) -> String {
    format!("{session_id}:{line:06}")
}

fn row_options(row: &Value, line: usize) -> ProviderOptions {
    let mut options = source_options(NAME, row);
    if let Some(source) = options.get_mut("source").and_then(Value::as_object_mut) {
        source.insert("line".to_owned(), json!(line));
    }
    options
}

/// Map one transcript row to its canonical events. `user` / `assistant` /
/// `reasoning` rows are one message with one Part; a `tool_call` row is an
/// Assistant + ToolCall and, when the row carries result fields, a Tool +
/// ToolResult keyed by the same `call_id`; anything else is a rule 3 carrier.
fn events_from_row(session: &Session, line: usize, row: &Value) -> Vec<IngestEvent> {
    let session_id = session.id.as_str();
    let id = message_id(session_id, line);
    let timestamp = captured_at(row).unwrap_or(session.created_at);
    let kind = row.get("kind").and_then(Value::as_str);

    let text_part = |kind: PartKind| Part {
        session_id: session_id.to_owned(),
        id: part_id(&id, 0),
        message_id: id.clone(),
        ordinal: 0,
        // spec.md#model-part-provenance: the user row holds the typed prompt
        // only (letta keeps its `<system-reminder>` blocks in the backend
        // store, never in the transcript); assistant and reasoning text is
        // model-authored.
        provenance: Provenance::Conversational,
        options: empty_options(),
        kind,
    };

    match kind {
        Some("user") => vec![
            IngestEvent::Message(Message::User {
                id: id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            }),
            IngestEvent::Part(text_part(PartKind::Text {
                text: extract_str(row, "text"),
            })),
        ],
        Some(kind @ ("assistant" | "reasoning")) => {
            let text = extract_str(row, "text");
            vec![
                IngestEvent::Message(Message::Assistant {
                    id: id.clone(),
                    session_id: session_id.to_owned(),
                    timestamp,
                    options: row_options(row, line),
                }),
                IngestEvent::Part(text_part(if kind == "reasoning" {
                    PartKind::Reasoning { text }
                } else {
                    PartKind::Text { text }
                })),
            ]
        }
        Some("tool_call") => tool_call_events(session_id, &id, line, timestamp, row),
        _ => vec![IngestEvent::Message(Message::System {
            id,
            session_id: session_id.to_owned(),
            timestamp,
            content: extract_str(row, "kind"),
            options: row_options(row, line),
        })],
    }
}

/// The correlation key of a `tool_call` row: the provider tool call id letta
/// stores as the line id, else the backend message id external rows carry,
/// else the row's own position id - call and result are the same record, so
/// the key is intrinsic to the row rather than guessed.
fn call_id_of(row: &Value, message_id: &str) -> Option<Extracted<String>> {
    extract_str(row, "source_line_id")
        .or_else(|| extract_str(row, "source_message_id"))
        .or_else(|| extract_self_str(&Value::String(message_id.to_owned())))
}

fn tool_call_events(
    session_id: &str,
    id: &str,
    line: usize,
    timestamp: DateTime<Utc>,
    row: &Value,
) -> Vec<IngestEvent> {
    let call_id = call_id_of(row, id);
    let name = extract_str(row, "name");
    let params = match row.get("argsText") {
        Some(Value::String(text)) => json_or_string(text),
        Some(other) => other.clone(),
        None => Value::Null,
    };
    let mut events = vec![
        IngestEvent::Message(Message::Assistant {
            id: id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp,
            options: row_options(row, line),
        }),
        IngestEvent::Part(Part {
            session_id: session_id.to_owned(),
            id: part_id(id, 0),
            message_id: id.to_owned(),
            ordinal: 0,
            // spec.md#model-part-provenance: the model authored the call.
            provenance: Provenance::Conversational,
            options: empty_options(),
            kind: PartKind::ToolCall {
                call_id: call_id.clone(),
                name: name.clone(),
                params,
                provider_executed: false,
            },
        }),
    ];

    let result_text = row.get("resultText");
    let result_ok = extract_bool(row, "resultOk");
    // An unfinished call has neither field; either one means the tool ran.
    if result_text.is_none() && result_ok.is_none() {
        return events;
    }
    let result_id = format!("{id}:result");
    let mut options = ProviderOptions::new();
    // The result is the same source row as the call above, which already
    // carries `raw_record`; this message names its origin instead so native
    // restore replays the row exactly once.
    options.insert(
        "source".to_owned(),
        json!({ "adapter": NAME, "line": line, DERIVED_FROM_KEY: "tool_call" }),
    );
    events.push(IngestEvent::Message(Message::Tool {
        id: result_id.clone(),
        session_id: session_id.to_owned(),
        timestamp,
        options,
    }));
    events.push(IngestEvent::Part(Part {
        session_id: session_id.to_owned(),
        id: part_id(&result_id, 0),
        message_id: result_id,
        ordinal: 0,
        // spec.md#model-part-provenance: tool output is runtime-produced.
        provenance: Provenance::Injected,
        options: empty_options(),
        kind: PartKind::ToolResult {
            call_id,
            name,
            // `resultOk` absent reads as not failed: an absence default, the
            // kind spec.md#model-no-synthesis permits.
            is_failure: result_ok.as_deref().is_some_and(|ok| !*ok),
            result: result_text.cloned().unwrap_or(Value::Null),
        },
    }));
    events
}

/// Restore one session. Native replays every row's `raw_record` into the
/// source path; a session whose rows lack `raw_record`, or a foreign session,
/// is reconstructed from its Parts under a distinct name.
fn serialize_session(
    session: &SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    let mut messages: Vec<&MessageWithParts> = session.messages.iter().collect();
    // Every row of a turn shares one `captured_at`, so the recorded source
    // line is the order; the timestamp/id comparator covers foreign sessions.
    messages.sort_by(|left, right| {
        source_line(left.message.options())
            .cmp(&source_line(right.message.options()))
            .then_with(|| by_timestamp_then_id(left, right))
    });
    let source = session.session.options.get("source");
    let dirs = source.and_then(|source| {
        Some((
            source.get("agent_dir")?.as_str()?.to_owned(),
            source.get("conversation_dir")?.as_str()?.to_owned(),
        ))
    });

    if fidelity == RestoreFidelity::Native
        && let Some((agent, conversation)) = &dirs
    {
        // Every non-derived message must replay; a derived Tool message is
        // the second half of a row already replayed by its Assistant sibling.
        let rows: Option<Vec<Value>> = messages
            .iter()
            .filter(|message| !is_derived(message))
            .map(|message| raw_record(message.message.options()))
            .collect();
        if let Some(rows) = rows {
            return Ok(vec![RestoredFile::new(
                transcript_path(agent, conversation, &session.session.id)?,
                jsonl_bytes(NAME, &rows)?,
                RestoreFidelity::Native,
            )]);
        }
    }

    let (agent, conversation) = match &dirs {
        Some((agent, conversation)) => (
            agent.clone(),
            format!("{conversation}{RECONSTRUCTED_SUFFIX}"),
        ),
        None => (
            sanitize_segment(&session.session.project),
            sanitize_segment(&session.session.id),
        ),
    };
    let rows = reconstruct_rows(&messages);
    Ok(vec![RestoredFile::new(
        transcript_path(&agent, &conversation, &session.session.id)?,
        jsonl_bytes(NAME, &rows)?,
        RestoreFidelity::Foreign,
    )])
}

fn is_derived(message: &MessageWithParts) -> bool {
    message
        .message
        .options()
        .get("source")
        .and_then(|source| source.get(DERIVED_FROM_KEY))
        .is_some()
}

fn transcript_path(agent: &str, conversation: &str, at: &str) -> Result<PathBuf, AdapterError> {
    validate_path_id(NAME, "agent directory", agent, at)?;
    validate_path_id(NAME, "conversation directory", conversation, at)?;
    Ok(PathBuf::from(agent)
        .join(conversation)
        .join(TRANSCRIPT_FILE))
}

/// letta's own `sanitizePathSegment`: anything outside `[A-Za-z0-9._-]`
/// becomes `_`, and an empty result is `unknown`.
fn sanitize_segment(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unknown".to_owned()
    } else {
        sanitized
    }
}

/// Idiomatic letta rows from canonical Parts: one row per text / reasoning /
/// tool-call Part, a following Tool message folded into its call's row by
/// `call_id`, System messages dropped (they have no letta shape; the content
/// stays in canonical).
fn reconstruct_rows(messages: &[&MessageWithParts]) -> Vec<Value> {
    let stamp = |message: &MessageWithParts| {
        message
            .message
            .timestamp()
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    };
    let text_of = |text: &Option<Extracted<String>>| extracted_text(text).to_owned();
    let mut rows = Vec::new();
    let mut folded = std::collections::HashSet::new();
    for (index, message) in messages.iter().enumerate() {
        if folded.contains(&index) {
            continue;
        }
        let captured_at = stamp(message);
        match message.message {
            Message::User { .. } => {
                let text = message
                    .parts
                    .iter()
                    .filter_map(|part| match &part.kind {
                        PartKind::Text { text } => Some(text_of(text)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                rows.push(json!({ "kind": "user", "text": text, "captured_at": captured_at }));
            }
            Message::Assistant { .. } => {
                for part in &message.parts {
                    match &part.kind {
                        PartKind::Text { text } => rows.push(json!({
                            "kind": "assistant", "text": text_of(text), "captured_at": captured_at,
                        })),
                        PartKind::Reasoning { text } => rows.push(json!({
                            "kind": "reasoning", "text": text_of(text), "captured_at": captured_at,
                        })),
                        PartKind::ToolCall {
                            call_id,
                            name,
                            params,
                            ..
                        } => {
                            let mut row = json!({
                                "kind": "tool_call",
                                "name": extracted_text(name),
                                "argsText": row_text(params),
                                "captured_at": captured_at,
                            });
                            // A call without an id cannot claim a result:
                            // folding the next one would be a guessed link.
                            if let Some(call) = call_id.as_deref()
                                && let Some((position, result)) =
                                    next_result(messages, index + 1, call)
                            {
                                folded.insert(position);
                                fold_result(&mut row, result);
                            }
                            rows.push(row);
                        }
                        PartKind::ToolResult { .. }
                        | PartKind::File { .. }
                        | PartKind::ToolApprovalRequest { .. }
                        | PartKind::ToolApprovalResponse { .. } => {}
                    }
                }
            }
            Message::Tool { .. } => {
                // A result whose call was not restored ahead of it: letta has
                // no result-only row, so it rides a call row without args.
                if let Some(result) = tool_result(message, None) {
                    let mut row = json!({
                        "kind": "tool_call", "name": result.name, "captured_at": captured_at,
                    });
                    fold_result(&mut row, result);
                    rows.push(row);
                }
            }
            Message::System { .. } => {}
        }
    }
    rows
}

struct ReconstructedResult {
    name: String,
    text: String,
    ok: bool,
}

/// letta's `argsText` / `resultText` form of a canonical value.
fn row_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => compact_json(other),
    }
}

/// The first Tool message at or after `from` whose result answers `call`.
fn next_result(
    messages: &[&MessageWithParts],
    from: usize,
    call: &str,
) -> Option<(usize, ReconstructedResult)> {
    messages
        .iter()
        .enumerate()
        .skip(from)
        .find_map(|(position, candidate)| {
            matches!(candidate.message, Message::Tool { .. })
                .then(|| tool_result(candidate, Some(call)))
                .flatten()
                .map(|result| (position, result))
        })
}

/// The ToolResult Part of a Tool message, when it matches `call` (any result
/// when `call` is `None`, for a result whose call was not restored).
fn tool_result(message: &MessageWithParts, call: Option<&str>) -> Option<ReconstructedResult> {
    message.parts.iter().find_map(|part| match &part.kind {
        PartKind::ToolResult {
            call_id,
            name,
            is_failure,
            result,
        } if call.is_none_or(|call| call_id.as_deref().map(String::as_str) == Some(call)) => {
            Some(ReconstructedResult {
                name: extracted_text(name).to_owned(),
                text: row_text(result),
                ok: !is_failure,
            })
        }
        _ => None,
    })
}

fn fold_result(row: &mut Value, result: ReconstructedResult) {
    if let Some(row) = row.as_object_mut() {
        row.insert("resultText".to_owned(), Value::String(result.text));
        row.insert("resultOk".to_owned(), Value::Bool(result.ok));
    }
}

#[cfg(test)]
mod tests {
    //! Mapping decisions from `docs/adapters/letta-code.md`, checked against
    //! the committed sandbox capture under `tests/fixtures/adapter/letta-code/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{handlers::ingest_adapter, sessions::Store};
    use tempfile::TempDir;

    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/letta-code/transcripts"
    );
    const AGENT_A: &str = "agent-local-0ce90846-9803-4ab1-8d67-31baacdd5148";
    const AGENT_B: &str = "agent-local-61c7e9e2-999a-453d-99d5-cac7c76a0543";
    /// The agent captured on Windows 11, the leg that proves the writer's
    /// bytes are the same there: LF endings, no BOM, no native path anywhere.
    const AGENT_WIN: &str = "agent-local-7ea0712d-8c11-40a1-a599-6ece6ab2303b";
    const LEGACY: &str = "conversation-00000000-0000-4000-8000-000000000001";

    fn fixture_path(agent: &str, conversation: &str) -> PathBuf {
        Path::new(FIXTURES)
            .join(agent)
            .join(conversation)
            .join(TRANSCRIPT_FILE)
    }

    async fn ingest_fixtures(temp: &TempDir) -> anyhow::Result<Store> {
        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(
            &store,
            &LettaCodeAdapter::new(FIXTURES),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        anyhow::ensure!(summary.dropped_events == 0, "no event may be dropped");
        anyhow::ensure!(summary.dropped_sessions == 0, "no session may be dropped");
        Ok(store)
    }

    #[test]
    fn probe_default_finds_the_transcript_root_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &LettaCodeFactory,
            &[".letta", "transcripts"],
        )
    }

    /// The path is the identity: `<agent>/<conversation>` without reading a
    /// byte, and the agent id is the project.
    #[test]
    fn the_session_id_and_project_come_from_the_path() {
        let adapter = LettaCodeAdapter::new(FIXTURES);
        let path = fixture_path(AGENT_A, "default");
        assert_eq!(
            adapter.peek_session_id(&path, ""),
            Some(format!("{AGENT_A}+default")),
        );
        let rows = vec![BoundedRow {
            line: 1,
            value: json!({"kind": "user", "text": "hi", "captured_at": "2026-08-24T16:42:32.405Z"}),
        }];
        let session = adapter.session(&path, &rows).expect("session decodes");
        assert_eq!(session.id, format!("{AGENT_A}+default"));
        assert_eq!(&*session.project, AGENT_A);
        assert_eq!(
            session
                .created_at
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            "2026-08-24T16:42:32.405Z",
        );
        assert_eq!(
            session
                .options
                .get("source")
                .and_then(|s| s.get("conversation_dir")),
            Some(&json!("default")),
        );
    }

    /// A transcript at the wrong depth is a named skip, never a session
    /// borrowing a neighbouring directory as its id.
    #[test]
    fn a_transcript_at_the_wrong_depth_is_a_named_skip() {
        let adapter = LettaCodeAdapter::new(FIXTURES);
        let shallow = Path::new(FIXTURES).join(AGENT_A).join(TRANSCRIPT_FILE);
        assert_eq!(adapter.peek_session_id(&shallow, ""), None);
        let reason = adapter
            .unsupported_path(&shallow)
            .expect("wrong depth is unsupported");
        assert!(reason.contains("transcripts root"), "{reason}");
        assert!(
            adapter
                .unsupported_path(&fixture_path(AGENT_A, "default"))
                .is_none(),
        );
    }

    /// The watermark is the last row's stamp; an unparseable tail re-reads.
    #[test]
    fn the_watermark_is_the_last_captured_at() -> anyhow::Result<()> {
        let adapter = LettaCodeAdapter::new(FIXTURES);
        let expected = DateTime::parse_from_rfc3339("2026-08-24T16:44:38.369Z")?
            .with_timezone(&Utc)
            .timestamp_micros();
        assert_eq!(
            adapter.peek_watermark(&fixture_path(AGENT_A, "default")),
            SourceWatermark::At(expected),
        );

        let temp = TempDir::new()?;
        let dir = temp.path().join("agent-x").join("conv-x");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(TRANSCRIPT_FILE);
        std::fs::write(&path, "{\"kind\":\"user\",\"text\":\"no stamp\"}\n")?;
        assert_eq!(
            LettaCodeAdapter::new(temp.path()).peek_watermark(&path),
            SourceWatermark::Opaque,
        );
        Ok(())
    }

    fn a_session() -> Session {
        Session {
            id: format!("{AGENT_A}+default"),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: NAME.to_owned(),
            created_at: Utc::now(),
            project: crate::adapter::extract::Extracted::from_test_value(AGENT_A.to_owned()),
            options: ProviderOptions::new(),
        }
    }

    /// A finished tool row is a call and a result under one `call_id`; an
    /// unfinished row is the call alone; a result without `resultOk` is not a
    /// failure.
    #[test]
    fn tool_rows_map_to_call_and_result_pairs() {
        let session = a_session();
        let finished = json!({
            "kind": "tool_call", "name": "Bash", "argsText": "{\"command\":\"ls\"}",
            "resultText": "Exit code: 1\n", "resultOk": false,
            "captured_at": "2026-08-24T16:43:44.058Z", "source_line_id": "toolu_1",
        });
        let events = events_from_row(&session, 8, &finished);
        assert_eq!(
            events.len(),
            4,
            "assistant + call part + tool + result part"
        );
        let IngestEvent::Part(call) = &events[1] else {
            panic!("call part")
        };
        let IngestEvent::Part(result) = &events[3] else {
            panic!("result part")
        };
        assert!(matches!(
            &call.kind,
            PartKind::ToolCall { call_id: Some(id), name: Some(name), params, .. }
                if &**id == "toolu_1" && &**name == "Bash" && params == &json!({"command": "ls"})
        ));
        assert!(matches!(
            &result.kind,
            PartKind::ToolResult { call_id: Some(id), is_failure: true, result, .. }
                if &**id == "toolu_1" && result == &json!("Exit code: 1\n")
        ));
        assert_eq!(result.provenance, Provenance::Injected);
        assert_eq!(call.provenance, Provenance::Conversational);

        let unfinished = json!({
            "kind": "tool_call", "name": "Bash", "argsText": "{\"command\":\"ls\"}",
            "captured_at": "2026-03-20T09:15:02.118Z",
        });
        let events = events_from_row(&session, 4, &unfinished);
        assert_eq!(events.len(), 2, "an unfinished call has no result message");
        let IngestEvent::Part(call) = &events[1] else {
            panic!("call part")
        };
        // No source id at all: the row's own position keys the call.
        assert!(matches!(
            &call.kind,
            PartKind::ToolCall { call_id: Some(id), .. } if id.ends_with(":000004")
        ));

        let no_flag = json!({
            "kind": "tool_call", "name": "Read", "argsText": "{}", "resultText": "1\tx",
            "captured_at": "2026-03-20T09:15:02.118Z",
        });
        let events = events_from_row(&session, 6, &no_flag);
        assert_eq!(events.len(), 4);
        let IngestEvent::Part(result) = &events[3] else {
            panic!("result part")
        };
        assert!(matches!(
            &result.kind,
            PartKind::ToolResult {
                is_failure: false,
                ..
            }
        ));
    }

    /// `error` rows and kinds this build has never seen ride the carrier path
    /// with the whole record preserved (spec.md#adapter-integrity-no-silent-drops).
    #[test]
    fn error_and_unknown_rows_become_carriers() {
        let session = a_session();
        for row in [
            json!({"kind": "error", "text": "502", "captured_at": "2026-03-20T09:15:02.118Z"}),
            json!({"kind": "future", "payload": {"a": 1}, "captured_at": "2026-03-20T09:15:02.118Z"}),
        ] {
            let events = events_from_row(&session, 5, &row);
            assert_eq!(events.len(), 1);
            let IngestEvent::Message(Message::System {
                content, options, ..
            }) = &events[0]
            else {
                panic!("carrier")
            };
            assert_eq!(content.as_deref().map(String::as_str), row["kind"].as_str());
            assert_eq!(raw_record(options), Some(row.clone()));
        }
    }

    /// spec.md 6.8 literally: every captured transcript restores native to a
    /// value-equal file at its own path. The empty conversation ingests no
    /// session, so it has nothing to restore; sidecars are not transcripts.
    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_every_captured_transcript() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = ingest_fixtures(&temp).await?;
        let mut restored_paths = std::collections::BTreeSet::new();
        for id in store.session_ids().await? {
            let session = store.get_session(&id).await?.expect("session reads back");
            let files = LettaCodeFactory.serialize(&session, RestoreFidelity::Native)?;
            assert_eq!(files.len(), 1);
            let file = &files[0];
            assert_eq!(file.actual_fidelity, RestoreFidelity::Native, "{id}");
            let source = Path::new(FIXTURES).join(&file.relative_path);
            let expected: Vec<Value> = std::str::from_utf8(&std::fs::read(&source)?)?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(serde_json::from_str)
                .collect::<Result<_, _>>()?;
            let actual: Vec<Value> = std::str::from_utf8(&file.bytes)?
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<_, _>>()?;
            assert_eq!(actual, expected, "{}", source.display());
            restored_paths.insert(file.relative_path.clone());
        }
        assert_eq!(
            restored_paths.len(),
            5,
            "every non-empty transcript restores"
        );
        Ok(())
    }

    /// A session with no `raw_record` (a foreign origin, or letta rows that
    /// predate capture) is reconstructed under a distinct name that letta
    /// lists, and the rows re-ingest through this adapter.
    #[tokio::test(flavor = "multi_thread")]
    async fn foreign_restore_reconstructs_rows_under_a_distinct_name() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = ingest_fixtures(&temp).await?;
        let session = store
            .get_session(&format!("{AGENT_A}+default"))
            .await?
            .expect("default conversation");
        let files = LettaCodeFactory.serialize(&session, RestoreFidelity::Foreign)?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].actual_fidelity, RestoreFidelity::Foreign);
        assert_eq!(
            files[0].relative_path,
            Path::new(AGENT_A)
                .join(format!("default{RECONSTRUCTED_SUFFIX}"))
                .join(TRANSCRIPT_FILE),
        );
        let rows: Vec<Value> = std::str::from_utf8(&files[0].bytes)?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        // 4 user + 4 assistant + 1 reasoning + 3 tool_call rows in the capture.
        assert_eq!(rows.len(), 12);
        let bash_failure = rows
            .iter()
            .find(|row| row["name"] == "Bash" && row["resultOk"] == false)
            .expect("the failed Bash call folds its result back into one row");
        assert!(
            bash_failure["resultText"]
                .as_str()
                .unwrap()
                .starts_with("Exit code: 1")
        );

        let restore_root = TempDir::new()?;
        crate::adapter::write_restored_files(restore_root.path(), files)?;
        let verify = Store::open_local(temp.path().join("verify")).await?;
        let summary = ingest_adapter(
            &verify,
            &LettaCodeAdapter::new(restore_root.path()),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(summary.dropped_events, 0);
        let reingested = verify
            .get_session(&format!("{AGENT_A}+default{RECONSTRUCTED_SUFFIX}"))
            .await?
            .expect("reconstructed transcript re-ingests");
        // 4 user + 4 assistant + 1 reasoning + 3 calls + 3 results.
        assert_eq!(reingested.messages.len(), 15);
        Ok(())
    }

    /// A non-letta session restores under letta's own sanitized directory
    /// alphabet, so the directory is one letta lists.
    #[test]
    fn foreign_sessions_land_under_sanitized_directories() {
        let mut session = a_session();
        session.id = "5d1e9ffd/ebbc".to_owned();
        session.source_agent = "claude-code".to_owned();
        session.project =
            crate::adapter::extract::Extracted::from_test_value("/Users/user/proj x".to_owned());
        let files = LettaCodeFactory
            .serialize(
                &SessionWithMessages {
                    session,
                    messages: Vec::new(),
                },
                RestoreFidelity::Native,
            )
            .expect("foreign restore");
        assert_eq!(files[0].actual_fidelity, RestoreFidelity::Foreign);
        assert_eq!(
            files[0].relative_path,
            Path::new("_Users_user_proj_x")
                .join("5d1e9ffd_ebbc")
                .join(TRANSCRIPT_FILE),
        );
        assert_eq!(sanitize_segment(""), "unknown");
    }

    /// The census the fixture README promises: three agents, five sessions
    /// (the empty conversation ingests none), the synthetic legacy rows in.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_fixture_corpus_ingests_as_documented() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = ingest_fixtures(&temp).await?;
        let mut ids = store.session_ids().await?;
        ids.sort();
        assert_eq!(
            ids,
            vec![
                format!("{AGENT_A}+{LEGACY}"),
                format!("{AGENT_A}+default"),
                format!("{AGENT_A}+local-conv-2"),
                format!("{AGENT_B}+local-conv-1"),
                format!("{AGENT_WIN}+local-conv-1"),
            ],
        );
        let legacy = store
            .get_session(&format!("{AGENT_A}+{LEGACY}"))
            .await?
            .expect("legacy rows ingest");
        // user, reasoning, assistant, unfinished call, error carrier, call +
        // result, assistant.
        assert_eq!(legacy.messages.len(), 8);
        let legacy_stamp =
            DateTime::parse_from_rfc3339("2026-03-20T09:15:02.118Z")?.with_timezone(&Utc);
        assert!(
            legacy
                .messages
                .iter()
                .all(|m| m.message.timestamp() == legacy_stamp),
        );
        let carriers = legacy
            .messages
            .iter()
            .filter(|m| matches!(m.message, Message::System { .. }))
            .count();
        assert_eq!(carriers, 1, "the error row is the only carrier");
        for session in [
            &format!("{AGENT_A}+default"),
            &format!("{AGENT_B}+local-conv-1"),
            &format!("{AGENT_WIN}+local-conv-1"),
        ] {
            let session = store.get_session(session).await?.expect("session");
            assert_eq!(session.session.parent_session_id, None);
            assert_eq!(session.session.source_agent, NAME);
        }
        let windows = store
            .get_session(&format!("{AGENT_WIN}+local-conv-1"))
            .await?
            .expect("the Windows capture ingests");
        assert_eq!(windows.messages.len(), 4, "two text-only turns");
        assert_eq!(&*windows.session.project, AGENT_WIN);
        Ok(())
    }
}
