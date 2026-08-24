//! grok-build adapter (github.com/xai-org/grok-build, the xAI `grok` CLI).
//!
//! Source: `$GROK_HOME/sessions/<encoded-cwd>/<session-uuid>/updates.jsonl`
//! (default root `~/.grok/sessions`; a relocated `GROK_HOME` is configured as
//! an explicit `path`), the append-only envelope stream grok itself calls
//! authoritative - rewind and compaction append markers, never truncate. Each
//! line is `{timestamp, method, params: {sessionId, update, _meta}}`; `method`
//! picks the ACP family (`session/update`) or the x.ai extension family
//! (`_x.ai/session/update`). The sibling `summary.json` supplies identity,
//! project, and lineage; every other sibling is documented non-capture. Format
//! archaeology and the decision record live in `docs/adapters/grok-build.md`.
//!
//! Identity is the session directory name (a UUID, equal to `summary.json`
//! `info.id` by construction); message ids are position-derived
//! (`<session>:<line:06>`) because legal lines lack `_meta.eventId` and a fork
//! interleaves two eventId counter epochs. Subagent lineage lives parent-side
//! only (`subagents/<child-id>/meta.json`), so the adapter builds a
//! child->parent map from those sidecars in the same walk and stores each
//! child's meta in its own options - canonical stays self-contained, and
//! restoring a child re-emits the sidecar. Native restore replays `raw_record`
//! lines plus the captured summary; `chat_history.jsonl` is deliberately not
//! emitted (grok rebuilds it from `updates.jsonl` on next load).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::{
    Adapter, AdapterError, AdapterFactory, AdapterYieldStream, DiscoverFuture, Env, PlanFuture,
    RestoreFidelity, RestoredFile, SkipOracle, SourceWatermark, by_timestamp_then_id, config_path,
    empty_options,
    extract::{extract_self_str, extract_str},
    extracted_text,
    jsonl::{
        BoundedRow, JsonlTree, jsonl_tree_discover, jsonl_tree_events, jsonl_tree_plan,
        parse_bounded, peek_last_line, source_line,
    },
    jsonl_bytes, part_id, raw_record, source_options, validate_path_id,
};
use crate::{
    sessions::{IngestEvent, MessageWithParts, SessionWithMessages},
    wire::{FileData, Message, Part, PartKind, Provenance, ProviderOptions, Session},
};

const NAME: &str = "grok-build";
const SUBAGENT_AGENT: &str = "grok-build/subagent";
const UPDATES_FILE: &str = "updates.jsonl";
const SUMMARY_FILE: &str = "summary.json";
const CWD_FILE: &str = ".cwd";
/// Suffix that names a reconstructed session dir distinctly from the source
/// dir it was reconstructed away from
/// (spec.md#adapter-restore-distinct-reconstruction).
const RECONSTRUCTED_SUFFIX: &str = "-reconstructed";

/// Stateless factory: opens [`GrokBuildAdapter`] instances and probes for the
/// sessions root under `~/.grok/sessions`. Two unrelated products also write
/// under `~/.grok` (superagent's grok-cli keeps a `grok.db` SQLite plus
/// `user-settings.json`); neither creates a `sessions/` tree of per-session
/// directories, so the probe claims only that subdirectory.
pub struct GrokBuildFactory;

impl AdapterFactory for GrokBuildFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError> {
        Ok(Box::new(GrokBuildAdapter::new(config_path(NAME, config)?)))
    }

    fn probe_default(&self, env: &Env) -> Option<Value> {
        let root = env.home.join(".grok").join("sessions");
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

/// One parent-side lineage record: the verbatim `subagents/<child>/meta.json`
/// and where it was found, relative to the sessions root (`/`-joined so it is
/// portable through canonical options).
#[derive(Debug, Clone)]
struct LineageEntry {
    meta: Value,
    meta_rel_path: String,
}

/// Configured grok-build reader: the sessions root, one
/// `<encoded-cwd>/<session-uuid>/updates.jsonl` per session.
#[derive(Clone)]
pub struct GrokBuildAdapter {
    root: PathBuf,
    /// child session id -> parent-side meta, built lazily from one walk over
    /// `*/*/subagents/*/meta.json`. Lineage must resolve at first ingest
    /// (additive sync never rewrites a stored session row), which is why the
    /// map is built against the whole configured tree, not per session.
    lineage: Arc<OnceLock<HashMap<String, LineageEntry>>>,
}

impl GrokBuildAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            lineage: Arc::new(OnceLock::new()),
        }
    }

    fn lineage(&self) -> &HashMap<String, LineageEntry> {
        self.lineage.get_or_init(|| build_lineage(&self.root))
    }
}

impl Adapter for GrokBuildAdapter {
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

impl JsonlTree for GrokBuildAdapter {
    type State = ();

    fn name(&self) -> &'static str {
        NAME
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn is_transcript(&self, path: &Path) -> bool {
        path.file_name().is_some_and(|name| name == UPDATES_FILE)
    }

    fn skip_source(&self, path: &Path) -> bool {
        is_hidden(path)
    }

    /// The id is the path, so the freshness gate never reads the file.
    fn peek_session_id(&self, path: &Path, _first_line: &str) -> Option<String> {
        placement(&self.root, path).map(|(_, session_dir)| session_dir)
    }

    fn peeks_first_line(&self) -> bool {
        false
    }

    fn peek_watermark(&self, path: &Path) -> SourceWatermark {
        // Append-only writer for a live session: rewind and compaction append
        // markers, and every persisted line carries `_meta.agentTimestampMs`
        // (envelope `timestamp` seconds as the fallback - it is legitimately 0
        // on remote-hydrated lines). Only the literal last line may answer: an
        // older line's stamp would under-report and skip real content.
        match peek_last_line(path)
            .and_then(|line| serde_json::from_str::<Value>(&line).ok())
            .and_then(|row| row_timestamp_micros(&row))
        {
            Some(micros) => SourceWatermark::At(micros),
            None => SourceWatermark::Opaque,
        }
    }

    fn unsupported_path(&self, path: &Path) -> Option<String> {
        // `updates.jsonl` anywhere but `<root>/<bucket>/<session>/` has no
        // session dir to name it (a workflow scratch file, a nested copy);
        // borrowing a neighbour's id would fold it into another session.
        placement(&self.root, path).is_none().then(|| {
            format!(
                "{}: {UPDATES_FILE} is not at <root>/<encoded-cwd>/<session-id>/ under {}; \
                 point the adapter at the grok sessions root",
                path.display(),
                self.root.display(),
            )
        })
    }

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError> {
        let schema_error =
            |message: String| AdapterError::schema(NAME, path.display().to_string(), message);
        let (bucket, id) = placement(&self.root, path)
            .ok_or_else(|| schema_error("transcript is not two levels deep".to_owned()))?;
        let session_dir = path
            .parent()
            .ok_or_else(|| schema_error("transcript has no parent directory".to_owned()))?;

        // `summary.json` is the identity/project/lineage sidecar. A session
        // without one (or with an unreadable one) still ingests: the stream is
        // the record, and every summary field has a fallback below.
        let summary = read_summary(session_dir);
        let created_at = summary
            .as_ref()
            .and_then(|summary| rfc3339(summary.get("created_at")?.as_str()?))
            .or_else(|| rows.iter().find_map(|row| row_timestamp(&row.value)))
            .ok_or_else(|| {
                schema_error("no summary created_at and no row carries a timestamp".to_owned())
            })?;

        // Project: `info.cwd` (the recorded working directory), else the
        // bucket's `.cwd` sidecar (hash-form buckets), else the percent-decoded
        // bucket name when it decodes to an absolute path
        // (spec.md#model-project-non-empty - none of the three means the
        // session has no attributable scope and drops, visibly logged).
        let cwd = summary
            .as_ref()
            .and_then(|summary| summary.get("info")?.get("cwd")?.as_str())
            .map(ToOwned::to_owned)
            .or_else(|| bucket_cwd(session_dir, &bucket))
            .ok_or_else(|| {
                schema_error(format!(
                    "no summary info.cwd, no {CWD_FILE} sidecar, and bucket {bucket:?} does not \
                     decode to an absolute path"
                ))
            })?;
        let project = extract_self_str(&Value::String(cwd)).ok_or_else(|| {
            schema_error("internal: Value::String produced None from Source::as_str".to_owned())
        })?;

        let lineage = self.lineage().get(&id);
        // Fork/resume children carry the link themselves; a fresh subagent
        // never does (no writer exists for it) - the parent-side meta is the
        // only record (docs/adapters/grok-build.md row 7).
        let parent_session_id = summary
            .as_ref()
            .and_then(|summary| summary.get("parent_session_id")?.as_str())
            .map(ToOwned::to_owned)
            .or_else(|| {
                lineage.and_then(|entry| {
                    entry
                        .meta
                        .get("parent_session_id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
            });
        let session_kind = summary
            .as_ref()
            .and_then(|summary| summary.get("session_kind")?.as_str());
        // The same predicate grok's own `is_hidden()` uses: anything whose
        // kind starts with "subagent" is a child working session, kept out of
        // default search via the brand subpath. Forks and worktree sessions
        // are user-visible peers and stay on the root brand.
        let is_subagent =
            session_kind.is_some_and(|kind| kind.starts_with("subagent")) || lineage.is_some();
        let source_agent = if is_subagent {
            SUBAGENT_AGENT.to_owned()
        } else {
            NAME.to_owned()
        };

        let mut source = json!({ "adapter": NAME, "bucket": bucket });
        if let Some(summary) = summary {
            source["summary"] = summary;
        }
        if let Some(entry) = lineage {
            source["subagent_meta"] = entry.meta.clone();
            source["subagent_meta_path"] = json!(entry.meta_rel_path);
        }
        let mut options = ProviderOptions::new();
        options.insert("source".to_owned(), source);

        Ok(Session {
            id,
            parent_session_id,
            parent_message_id: None,
            source_agent,
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

/// `(bucket, session_dir)` for a transcript exactly two directories below
/// `root`, else `None`.
fn placement(root: &Path, path: &Path) -> Option<(String, String)> {
    let session_dir = path.parent()?;
    let bucket_dir = session_dir.parent()?;
    if bucket_dir.parent()? != root {
        return None;
    }
    let name = |dir: &Path| dir.file_name()?.to_str().map(ToOwned::to_owned);
    Some((name(bucket_dir)?, name(session_dir)?))
}

/// The bucket's recorded cwd: the `.cwd` sidecar grok writes beside hash-form
/// buckets, else the percent-decoded bucket name when it is an absolute path
/// (Unix `/...` or a Windows drive like `C:\...`). The hash form never decodes
/// to one, which is what disambiguates the two on read - the same rule grok's
/// own `decode_cwd_from_dirname` applies.
fn bucket_cwd(session_dir: &Path, bucket: &str) -> Option<String> {
    let sidecar = session_dir.parent()?.join(CWD_FILE);
    if let Ok(text) = std::fs::read_to_string(sidecar) {
        let text = text.trim_end_matches(['\r', '\n']).to_owned();
        if !text.is_empty() {
            return Some(text);
        }
    }
    let decoded = percent_decode(bucket)?;
    is_absolute_path(&decoded).then_some(decoded)
}

/// Dot-prefixed names are grok's own staging, never source: a relocation
/// copies the whole session dir into `.<session-id>.relocating-<nonce>/`
/// before renaming it into place, so both the transcript walk and the lineage
/// walk must ignore it or they ingest the transient duplicate.
fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with('.'))
}

fn is_absolute_path(text: &str) -> bool {
    text.starts_with('/')
        || (text.len() >= 3
            && text.as_bytes()[0].is_ascii_alphabetic()
            && text.as_bytes()[1] == b':'
            && matches!(text.as_bytes()[2], b'\\' | b'/'))
}

/// Strict RFC 3986 percent-decode; `None` on a malformed escape or non-UTF-8,
/// so a hash-form bucket (which contains no `%`) simply fails the
/// absolute-path test rather than producing a garbled project.
fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hi = (hex[0] as char).to_digit(16)?;
            let lo = (hex[1] as char).to_digit(16)?;
            out.push((hi as u8) << 4 | lo as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// RFC 3986 percent-encode with the unreserved set, byte-for-byte what grok's
/// `urlencoding::encode` produces - used only to name a foreign session's
/// bucket after its project.
fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn read_summary(session_dir: &Path) -> Option<Value> {
    let path = session_dir.join(SUMMARY_FILE);
    let bytes = std::fs::read(&path).ok()?;
    match parse_bounded(NAME, &bytes, || path.display().to_string()) {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::debug!(%error, "unreadable summary.json; ingesting from the stream alone");
            None
        }
    }
}

/// Walk `<root>/*/*/subagents/*/meta.json` once, keying each verbatim meta by
/// the child session id it names (falling back to the sidecar dir name, which
/// grok sets to the same id). Best-effort: a malformed meta only costs that
/// child its parent link, which is what the truncated source honestly shows.
fn build_lineage(root: &Path) -> HashMap<String, LineageEntry> {
    let mut map = HashMap::new();
    // Sorted so a duplicate `child_session_id` resolves the same way on every
    // host; `is_hidden` prunes the relocation staging copy, whose meta would
    // otherwise race the real one and point `meta_rel_path` at a dir that is
    // about to be renamed away.
    let dirs = |dir: &Path| {
        let mut paths = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .map(|entry| entry.path())
            .filter(|path| !is_hidden(path))
            .collect::<Vec<_>>();
        paths.sort();
        paths
    };
    for bucket in dirs(root) {
        for session_dir in dirs(&bucket) {
            for child_dir in dirs(&session_dir.join("subagents")) {
                let meta_path = child_dir.join("meta.json");
                let Ok(bytes) = std::fs::read(&meta_path) else {
                    continue;
                };
                let meta = match parse_bounded(NAME, &bytes, || meta_path.display().to_string()) {
                    Ok(meta) => meta,
                    Err(error) => {
                        tracing::debug!(%error, "unreadable subagent meta.json; child ingests without a parent link");
                        continue;
                    }
                };
                let child_id = meta
                    .get("child_session_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .or_else(|| {
                        child_dir
                            .file_name()
                            .and_then(|name| name.to_str())
                            .map(ToOwned::to_owned)
                    });
                if let Some(child_id) = child_id {
                    let meta_rel_path = meta_path
                        .strip_prefix(root)
                        .ok()
                        .map(|rel| {
                            rel.components()
                                .map(|c| c.as_os_str().to_string_lossy())
                                .collect::<Vec<_>>()
                                .join("/")
                        })
                        .unwrap_or_default();
                    map.insert(
                        child_id,
                        LineageEntry {
                            meta,
                            meta_rel_path,
                        },
                    );
                }
            }
        }
    }
    map
}

fn rfc3339(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|ts| ts.with_timezone(&Utc))
}

/// A line's timestamp: `params._meta.agentTimestampMs` (the writer's own
/// millisecond stamp), else the envelope `timestamp` in seconds when non-zero
/// (remote-hydrated lines carry a literal 0).
fn row_timestamp(row: &Value) -> Option<DateTime<Utc>> {
    if let Some(ms) = row
        .get("params")
        .and_then(|params| params.get("_meta"))
        .and_then(|meta| meta.get("agentTimestampMs"))
        .and_then(Value::as_i64)
    {
        return DateTime::from_timestamp_millis(ms);
    }
    let secs = row.get("timestamp").and_then(Value::as_i64)?;
    (secs > 0)
        .then(|| DateTime::from_timestamp(secs, 0))
        .flatten()
}

fn row_timestamp_micros(row: &Value) -> Option<i64> {
    row_timestamp(row).map(|ts| ts.timestamp_micros())
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

/// The `params` / `update` objects of an envelope line. A legacy line may be a
/// bare ACP notification with no `{timestamp, method, params}` wrapper, so
/// each level falls back to the value itself.
fn envelope_update(row: &Value) -> &Value {
    let params = row.get("params").unwrap_or(row);
    params.get("update").unwrap_or(params)
}

fn chunk_meta(update: &Value) -> Option<&Value> {
    update.get("_meta")
}

fn tool_meta(update: &Value) -> Option<&Value> {
    chunk_meta(update)?.get("x.ai/tool")
}

/// Map one envelope line to its canonical events. Text, thought, and tool
/// records become typed messages; every other kind - the x.ai extension
/// events, `plan`, mode changes, enrichment tool updates, unknown kinds - is a
/// rule 3 carrier holding the whole line.
fn events_from_row(session: &Session, line: usize, row: &Value) -> Vec<IngestEvent> {
    let session_id = session.id.as_str();
    let id = message_id(session_id, line);
    let timestamp = row_timestamp(row).unwrap_or(session.created_at);
    let update = envelope_update(row);
    let kind = update.get("sessionUpdate").and_then(Value::as_str);

    // One persisted line carries at most one content block, so every message
    // here has a single part.
    let part = |provenance: Provenance, kind: PartKind| Part {
        session_id: session_id.to_owned(),
        id: part_id(&id, 0),
        message_id: id.clone(),
        ordinal: 0,
        provenance,
        options: empty_options(),
        kind,
    };

    match kind {
        Some("user_message_chunk") => {
            // spec.md#model-part-provenance: `hostTurn` chunks are grok's own
            // zero-content turn-boundary markers and `hideFromScrollback`
            // marks harness-authored echoes; both are injected, everything
            // else is the typed prompt.
            let meta = chunk_meta(update);
            let host_authored = |key: &str| {
                meta.and_then(|meta| meta.get(key))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            };
            let provenance = if host_authored("hostTurn") || host_authored("hideFromScrollback") {
                Provenance::Injected
            } else {
                Provenance::Conversational
            };
            let mut events = vec![IngestEvent::Message(Message::User {
                id: id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            })];
            if let Some(kind) = content_part(update.get("content")) {
                events.push(IngestEvent::Part(part(provenance, kind)));
            }
            events
        }
        Some(kind @ ("agent_message_chunk" | "agent_thought_chunk")) => {
            let mut events = vec![IngestEvent::Message(Message::Assistant {
                id: id.clone(),
                session_id: session_id.to_owned(),
                timestamp,
                options: row_options(row, line),
            })];
            // An agent chunk carries the same ACP content block a user chunk
            // does, so a non-text block is a File part here too; only a text
            // block splits by kind.
            if let Some(kind) =
                content_part(update.get("content")).map(|content| match (kind, content) {
                    ("agent_thought_chunk", PartKind::Text { text }) => {
                        PartKind::Reasoning { text }
                    }
                    (_, content) => content,
                })
            {
                events.push(IngestEvent::Part(part(Provenance::Conversational, kind)));
            }
            events
        }
        Some("tool_call") => {
            // The model-facing tool name lives in `_meta["x.ai/tool"].name`;
            // `title` is display text and the honest fallback. `backend: true`
            // marks server-side (provider-executed) tool calls.
            let name = tool_meta(update)
                .and_then(|meta| extract_str(meta, "name"))
                .or_else(|| extract_str(update, "title"));
            let provider_executed = chunk_meta(update)
                .and_then(|meta| meta.get("backend"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            vec![
                IngestEvent::Message(Message::Assistant {
                    id: id.clone(),
                    session_id: session_id.to_owned(),
                    timestamp,
                    options: row_options(row, line),
                }),
                IngestEvent::Part(part(
                    Provenance::Conversational,
                    PartKind::ToolCall {
                        call_id: extract_str(update, "toolCallId"),
                        name,
                        params: update.get("rawInput").cloned().unwrap_or(Value::Null),
                        provider_executed,
                    },
                )),
            ]
        }
        // Terminal status is exactly `completed | failed` (the ACP
        // ToolCallStatus terminals; a non-zero exit code is still `completed`
        // with the code in rawOutput - `failed` means the tool itself failed).
        // A statusless enrichment update is neither call nor result and rides
        // the carrier arm below.
        Some("tool_call_update")
            if matches!(
                update.get("status").and_then(Value::as_str),
                Some("completed" | "failed")
            ) =>
        {
            let is_failure = update.get("status").and_then(Value::as_str) == Some("failed");
            let result = update
                .get("rawOutput")
                .or_else(|| update.get("content"))
                .cloned()
                .unwrap_or(Value::Null);
            vec![
                IngestEvent::Message(Message::Tool {
                    id: id.clone(),
                    session_id: session_id.to_owned(),
                    timestamp,
                    options: row_options(row, line),
                }),
                IngestEvent::Part(part(
                    // spec.md#model-part-provenance: tool output is
                    // runtime-produced.
                    Provenance::Injected,
                    PartKind::ToolResult {
                        call_id: extract_str(update, "toolCallId"),
                        // Terminal updates rarely repeat the x.ai/tool meta;
                        // an absent name stays absent (the call carries it).
                        name: tool_meta(update).and_then(|meta| extract_str(meta, "name")),
                        is_failure,
                        result,
                    },
                )),
            ]
        }
        _ => vec![IngestEvent::Message(Message::System {
            id,
            session_id: session_id.to_owned(),
            timestamp,
            content: extract_str(update, "sessionUpdate"),
            options: row_options(row, line),
        })],
    }
}

/// A chunk's single ACP content block as a Part kind. Text carries the text;
/// anything with a `data` payload (image, audio) or a `uri` (resource links)
/// is a file. A block with neither yields no Part - the raw record preserves
/// it whole.
fn content_part(block: Option<&Value>) -> Option<PartKind> {
    let block = block?;
    if block.get("type").and_then(Value::as_str) == Some("text") {
        return Some(PartKind::Text {
            text: extract_str(block, "text"),
        });
    }
    let media_type = block
        .get("mimeType")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    if let Some(data) = block.get("data").and_then(Value::as_str) {
        return Some(PartKind::File {
            media_type,
            file_name: None,
            data: FileData::String(data.to_owned()),
        });
    }
    if let Some(uri) = block.get("uri").and_then(Value::as_str) {
        return Some(PartKind::File {
            media_type,
            file_name: None,
            data: FileData::Url(uri.to_owned()),
        });
    }
    None
}

/// Restore one session. Native replays every message's `raw_record` into
/// `<bucket>/<id>/updates.jsonl` plus the captured `summary.json` (and, for a
/// subagent child, the parent-side `subagents/<id>/meta.json` sidecar).
/// `chat_history.jsonl` is deliberately absent: grok rebuilds it from the
/// update stream on next load. A session missing its capture, or a foreign
/// session, is reconstructed as an idiomatic update stream under a distinct
/// dir name.
fn serialize_session(
    session: &SessionWithMessages,
    fidelity: RestoreFidelity,
) -> Result<Vec<RestoredFile>, AdapterError> {
    let mut messages: Vec<&MessageWithParts> = session.messages.iter().collect();
    // The recorded source line is the order (one line, one message); the
    // timestamp/id comparator covers foreign sessions.
    messages.sort_by(|left, right| {
        source_line(left.message.options())
            .cmp(&source_line(right.message.options()))
            .then_with(|| by_timestamp_then_id(left, right))
    });
    let source = session.session.options.get("source");
    let bucket = source
        .and_then(|source| source.get("bucket"))
        .and_then(Value::as_str);
    let summary = source.and_then(|source| source.get("summary"));

    if fidelity == RestoreFidelity::Native
        && let Some(bucket) = bucket
        && let Some(summary) = summary
    {
        let rows: Option<Vec<Value>> = messages
            .iter()
            .map(|message| raw_record(message.message.options()))
            .collect();
        if let Some(rows) = rows {
            let dir = session_dir_path(bucket, &session.session.id)?;
            let mut files = vec![
                RestoredFile::new(
                    dir.join(UPDATES_FILE),
                    jsonl_bytes(NAME, &rows)?,
                    RestoreFidelity::Native,
                ),
                RestoredFile::new(
                    dir.join(SUMMARY_FILE),
                    pretty_json(summary)?,
                    RestoreFidelity::Native,
                ),
            ];
            // Re-emit the parent-side lineage sidecar so a restored child is
            // discoverable as a child again (docs/adapters/grok-build.md row 7).
            if let Some(meta) = source.and_then(|source| source.get("subagent_meta"))
                && let Some(rel) = source
                    .and_then(|source| source.get("subagent_meta_path"))
                    .and_then(Value::as_str)
            {
                files.push(RestoredFile::new(
                    rel_path(rel, &session.session.id)?,
                    compact_json_bytes(meta)?,
                    RestoreFidelity::Native,
                ));
            }
            return Ok(files);
        }
    }

    let (bucket, dir_name) = match bucket {
        Some(bucket) => (
            bucket.to_owned(),
            format!("{}{RECONSTRUCTED_SUFFIX}", session.session.id),
        ),
        None => (
            percent_encode(&session.session.project),
            session.session.id.clone(),
        ),
    };
    let rows = reconstruct_rows(&session.session.id, &messages);
    let dir = session_dir_path(&bucket, &dir_name)?;
    Ok(vec![RestoredFile::new(
        dir.join(UPDATES_FILE),
        jsonl_bytes(NAME, &rows)?,
        RestoreFidelity::Foreign,
    )])
}

fn json_error(err: serde_json::Error) -> AdapterError {
    AdapterError::schema(NAME, "serialize", format!("json encode failed: {err}"))
}

/// Pretty, matching grok's own `summary.json` writer.
fn pretty_json(value: &Value) -> Result<Vec<u8>, AdapterError> {
    serde_json::to_vec_pretty(value).map_err(json_error)
}

fn compact_json_bytes(value: &Value) -> Result<Vec<u8>, AdapterError> {
    serde_json::to_vec(value).map_err(json_error)
}

fn session_dir_path(bucket: &str, dir_name: &str) -> Result<PathBuf, AdapterError> {
    validate_path_id(NAME, "bucket directory", bucket, dir_name)?;
    validate_path_id(NAME, "session directory", dir_name, dir_name)?;
    Ok(PathBuf::from(bucket).join(dir_name))
}

/// A `/`-joined relative path from canonical options back into path segments,
/// each re-validated (the writer re-checks too; this keeps the error local).
/// The failure is an error, not a skip: dropping the sidecar while the rest of
/// the file set still reports Native would be a silent partial restore.
fn rel_path(rel: &str, at: &str) -> Result<PathBuf, AdapterError> {
    let mut path = PathBuf::new();
    for segment in rel.split('/') {
        validate_path_id(NAME, "sidecar path segment", segment, at)?;
        path.push(segment);
    }
    Ok(path)
}

/// Idiomatic envelope lines from canonical Parts: one line per text /
/// reasoning / tool-call / tool-result Part, System carriers dropped (their
/// content stays in canonical).
fn reconstruct_rows(session_id: &str, messages: &[&MessageWithParts]) -> Vec<Value> {
    let mut rows = Vec::new();
    for message in messages {
        let ts = message.message.timestamp();
        let envelope = |update: Value| {
            json!({
                "timestamp": ts.timestamp(),
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": update,
                    "_meta": { "agentTimestampMs": ts.timestamp_millis() },
                },
            })
        };
        for part in &message.parts {
            let update = match (&message.message, &part.kind) {
                (Message::User { .. }, PartKind::Text { text }) => json!({
                    "sessionUpdate": "user_message_chunk",
                    "content": { "type": "text", "text": extracted_text(text) },
                }),
                (Message::Assistant { .. }, PartKind::Text { text }) => json!({
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": extracted_text(text) },
                }),
                (Message::Assistant { .. }, PartKind::Reasoning { text }) => json!({
                    "sessionUpdate": "agent_thought_chunk",
                    "content": { "type": "text", "text": extracted_text(text) },
                }),
                (
                    Message::Assistant { .. },
                    PartKind::ToolCall {
                        call_id,
                        name,
                        params,
                        ..
                    },
                ) => {
                    let mut update = json!({
                        "sessionUpdate": "tool_call",
                        "title": extracted_text(name),
                        "rawInput": params,
                    });
                    if let Some(call_id) = call_id.as_deref() {
                        update["toolCallId"] = json!(&**call_id);
                    }
                    update
                }
                (
                    Message::Tool { .. },
                    PartKind::ToolResult {
                        call_id,
                        is_failure,
                        result,
                        ..
                    },
                ) => {
                    let mut update = json!({
                        "sessionUpdate": "tool_call_update",
                        "status": if *is_failure { "failed" } else { "completed" },
                        "rawOutput": result,
                    });
                    if let Some(call_id) = call_id.as_deref() {
                        update["toolCallId"] = json!(&**call_id);
                    }
                    update
                }
                _ => continue,
            };
            rows.push(envelope(update));
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    //! Mapping decisions from `docs/adapters/grok-build.md`, checked against
    //! the committed sandbox capture under `tests/fixtures/adapter/grok-build/`.
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{
        adapter::jsonl::{PEEK_TAIL_CAP, peek_metering},
        handlers::ingest_adapter,
        sessions::Store,
    };
    use tempfile::TempDir;

    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/adapter/grok-build/sessions"
    );
    const MAC_BUCKET: &str = "%2Fprivate%2Ftmp%2Fgrok-fixture%2Fproject";
    const WIN_BUCKET: &str = "C%3A%5Cgf%5Cproject";
    const HASH_BUCKET: &str = "nested-directory-014-d57c02c5ec15a4db";
    const TOOLS: &str = "01a0355c-4cdd-7c62-9d7b-f0bd11ba9d2d";
    const FORK: &str = "01a0355c-8db1-7e50-a5c7-2201f4086118";
    const SUBAGENT_PARENT: &str = "01a0355c-9c5a-71e3-8b8a-253db47e0a24";
    const SUBAGENT_CHILD: &str = "01a0355c-aead-7641-8548-7eebefb15237";
    const TUI: &str = "01a0355d-85a0-73a0-ac3c-f33aaf3747a0";
    const LONG_CWD: &str = "01a0355d-5bc4-79c2-8e9b-0cff318fc630";
    /// 15 summaries in the capture; the `no-updates` dir has no stream of
    /// record, so 14 sessions ingest.
    const FIXTURE_SESSIONS: usize = 14;

    fn fixture_path(bucket: &str, session: &str) -> PathBuf {
        Path::new(FIXTURES)
            .join(bucket)
            .join(session)
            .join(UPDATES_FILE)
    }

    async fn ingest_fixtures(temp: &TempDir) -> anyhow::Result<Store> {
        let store = Store::open_local(temp.path().join("store")).await?;
        let summary = ingest_adapter(
            &store,
            &GrokBuildAdapter::new(FIXTURES),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        anyhow::ensure!(summary.dropped_events == 0, "no event may be dropped");
        anyhow::ensure!(summary.dropped_sessions == 0, "no session may be dropped");
        Ok(store)
    }

    #[test]
    fn probe_default_finds_the_sessions_root_under_home() -> anyhow::Result<()> {
        crate::adapter::test_support::assert_probe_default(
            &GrokBuildFactory,
            &[".grok", "sessions"],
        )
    }

    /// The path is the identity: `<bucket>/<uuid>` without reading a byte, and
    /// a transcript at any other depth is a named skip.
    #[test]
    fn the_session_id_comes_from_the_path_and_depth_is_enforced() {
        let adapter = GrokBuildAdapter::new(FIXTURES);
        let path = fixture_path(MAC_BUCKET, TOOLS);
        assert_eq!(adapter.peek_session_id(&path, ""), Some(TOOLS.to_owned()));
        assert!(adapter.unsupported_path(&path).is_none());

        let shallow = Path::new(FIXTURES).join(MAC_BUCKET).join(UPDATES_FILE);
        assert_eq!(adapter.peek_session_id(&shallow, ""), None);
        let reason = adapter
            .unsupported_path(&shallow)
            .expect("wrong depth is unsupported");
        assert!(reason.contains("sessions root"), "{reason}");
    }

    /// Project resolution rows 3: `info.cwd` from the summary, and the `.cwd`
    /// sidecar for the hash-form bucket whose name cannot be decoded.
    #[tokio::test(flavor = "multi_thread")]
    async fn project_comes_from_summary_cwd_and_the_hash_bucket_sidecar() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = ingest_fixtures(&temp).await?;
        let tools = store.get_session(TOOLS).await?.expect("tools session");
        assert_eq!(&*tools.session.project, "/private/tmp/grok-fixture/project");
        let long = store
            .get_session(LONG_CWD)
            .await?
            .expect("long-cwd session");
        assert!(
            long.session
                .project
                .starts_with("/private/tmp/grok-fixture/project/nested-directory-001"),
            "the hash bucket resolves through .cwd: {}",
            &*long.session.project,
        );
        assert_eq!(long.session.source_agent, NAME);
        Ok(())
    }

    /// The percent-decoded bucket is the last-resort project: Unix and Windows
    /// absolute paths decode, the hash form does not.
    #[test]
    fn bucket_decoding_accepts_absolute_paths_only() {
        assert_eq!(
            percent_decode(MAC_BUCKET).as_deref(),
            Some("/private/tmp/grok-fixture/project"),
        );
        assert_eq!(
            percent_decode(WIN_BUCKET).as_deref(),
            Some("C:\\gf\\project")
        );
        assert!(is_absolute_path("/private/tmp/grok-fixture/project"));
        assert!(is_absolute_path("C:\\gf\\project"));
        assert!(!is_absolute_path(HASH_BUCKET));
        assert_eq!(percent_decode("bad%zz"), None);
    }

    /// Row 4/10: the watermark is the last line's own stamp; a stampless tail
    /// re-reads.
    #[test]
    fn the_watermark_is_the_last_line_stamp() -> anyhow::Result<()> {
        let adapter = GrokBuildAdapter::new(FIXTURES);
        let path = fixture_path(MAC_BUCKET, TUI);
        let last: Value = serde_json::from_str(&peek_last_line(&path).expect("tail"))?;
        let expected = row_timestamp_micros(&last).expect("stamped");
        assert_eq!(adapter.peek_watermark(&path), SourceWatermark::At(expected));

        let temp = TempDir::new()?;
        let dir = temp.path().join("bucket").join("session");
        std::fs::create_dir_all(&dir)?;
        let stampless = dir.join(UPDATES_FILE);
        std::fs::write(
            &stampless,
            "{\"timestamp\":0,\"method\":\"session/update\",\"params\":{}}\n",
        )?;
        assert_eq!(
            GrokBuildAdapter::new(temp.path()).peek_watermark(&stampless),
            SourceWatermark::Opaque,
        );
        Ok(())
    }

    /// The freshness peek reads a bounded tail, never the file twice: the
    /// budget is one window, and for a transcript under the cap that pins the
    /// peek to a single pass over it. This is the declared freshness-read
    /// budget the sync gate is held to.
    #[test]
    fn the_freshness_peek_stays_within_one_tail_window_per_file() -> anyhow::Result<()> {
        let adapter = GrokBuildAdapter::new(FIXTURES);
        let mut files = Vec::new();
        collect_updates_files(Path::new(FIXTURES), &mut files);
        assert_eq!(files.len(), FIXTURE_SESSIONS);
        for path in files {
            let (watermark, bytes) = peek_metering::measure(|| adapter.peek_watermark(&path));
            assert!(
                matches!(watermark, SourceWatermark::At(_)),
                "{}: every captured transcript has a stamped tail",
                path.display(),
            );
            let budget = std::fs::metadata(&path)?.len().min(PEEK_TAIL_CAP);
            assert!(
                bytes <= budget,
                "{}: peek read {bytes} bytes, over the {budget}-byte budget",
                path.display(),
            );
        }
        Ok(())
    }

    fn collect_updates_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("fixture dir is readable") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                collect_updates_files(&path, out);
            } else if path.file_name().is_some_and(|name| name == UPDATES_FILE) {
                out.push(path);
            }
        }
    }

    fn a_session(id: &str) -> Session {
        Session {
            id: id.to_owned(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: NAME.to_owned(),
            created_at: Utc::now(),
            project: crate::adapter::extract::Extracted::from_test_value("/tmp/p".to_owned()),
            options: ProviderOptions::new(),
        }
    }

    /// Row 5: the tool triple - a call (name from x.ai/tool, params from
    /// rawInput), an enrichment update as a carrier, and a terminal update as
    /// the result whose `failed` status is the failure signal.
    #[test]
    fn the_tool_triple_maps_to_call_carrier_and_result() {
        let session = a_session(TOOLS);
        let call = json!({
            "timestamp": 1787600733, "method": "session/update",
            "params": {"sessionId": TOOLS, "update": {
                "sessionUpdate": "tool_call", "toolCallId": "call-1",
                "title": "run_terminal_command",
                "rawInput": {"command": "echo hi"},
                "_meta": {"x.ai/tool": {"name": "run_terminal_command", "kind": "execute"}},
            }, "_meta": {"eventId": "x-1", "agentTimestampMs": 1787600733060i64}},
        });
        let events = events_from_row(&session, 4, &call);
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            IngestEvent::Message(Message::Assistant { .. })
        ));
        let IngestEvent::Part(part) = &events[1] else {
            panic!("call part")
        };
        assert_eq!(part.provenance, Provenance::Conversational);
        assert!(matches!(
            &part.kind,
            PartKind::ToolCall { call_id: Some(id), name: Some(name), params, provider_executed: false }
                if &**id == "call-1" && &**name == "run_terminal_command"
                    && params == &json!({"command": "echo hi"})
        ));

        let enrichment = json!({
            "timestamp": 1787600733, "method": "session/update",
            "params": {"sessionId": TOOLS, "update": {
                "sessionUpdate": "tool_call_update", "toolCallId": "call-1",
                "kind": "execute", "title": "Execute `echo hi`",
            }, "_meta": {"agentTimestampMs": 1787600733061i64}},
        });
        let events = events_from_row(&session, 5, &enrichment);
        assert_eq!(events.len(), 1, "a statusless update is a carrier");
        assert!(matches!(
            &events[0],
            IngestEvent::Message(Message::System { .. })
        ));

        let terminal = json!({
            "timestamp": 1787600735, "method": "session/update",
            "params": {"sessionId": TOOLS, "update": {
                "sessionUpdate": "tool_call_update", "toolCallId": "call-1",
                "status": "failed",
                "rawOutput": {"type": "ReadFile", "Error": "missing"},
            }, "_meta": {"agentTimestampMs": 1787600735690i64}},
        });
        let events = events_from_row(&session, 6, &terminal);
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            IngestEvent::Message(Message::Tool { .. })
        ));
        let IngestEvent::Part(part) = &events[1] else {
            panic!("result part")
        };
        assert_eq!(part.provenance, Provenance::Injected);
        assert!(matches!(
            &part.kind,
            PartKind::ToolResult { call_id: Some(id), name: None, is_failure: true, result }
                if &**id == "call-1" && result == &json!({"type": "ReadFile", "Error": "missing"})
        ));
    }

    /// Row 6: a `hostTurn` user chunk is injected; an image block becomes a
    /// FilePart carrying the base64 payload.
    #[test]
    fn user_chunk_provenance_and_image_blocks() {
        let session = a_session(TOOLS);
        let host_turn = json!({
            "timestamp": 1, "method": "session/update",
            "params": {"sessionId": TOOLS, "update": {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text": ""},
                "_meta": {"hostTurn": true},
            }, "_meta": {"agentTimestampMs": 1000i64}},
        });
        let events = events_from_row(&session, 1, &host_turn);
        let IngestEvent::Part(part) = &events[1] else {
            panic!("text part")
        };
        assert_eq!(part.provenance, Provenance::Injected);

        let image = json!({
            "timestamp": 1, "method": "session/update",
            "params": {"sessionId": TOOLS, "update": {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "image", "data": "aGk=", "mimeType": "image/png"},
                "_meta": {"modelId": "grok-4.6", "promptIndex": 0},
            }, "_meta": {"agentTimestampMs": 1000i64}},
        });
        let events = events_from_row(&session, 2, &image);
        let IngestEvent::Part(part) = &events[1] else {
            panic!("file part")
        };
        assert_eq!(part.provenance, Provenance::Conversational);
        assert!(matches!(
            &part.kind,
            PartKind::File { media_type: Some(mime), data: FileData::String(data), .. }
                if mime == "image/png" && data == "aGk="
        ));
    }

    /// Every x.ai extension kind, unknown kinds, and legacy un-enveloped lines
    /// ride the carrier path with the whole line preserved
    /// (spec.md#adapter-integrity-no-silent-drops).
    #[test]
    fn extension_unknown_and_legacy_rows_become_carriers() {
        let session = a_session(TOOLS);
        let retry = json!({
            "timestamp": 1, "method": "_x.ai/session/update",
            "params": {"sessionId": TOOLS, "update": {
                "sessionUpdate": "retry_state", "type": "failed",
                "error_type": "api", "message": "404",
            }, "_meta": {"agentTimestampMs": 1000i64}},
        });
        let future = json!({
            "timestamp": 1, "method": "_x.ai/session/update",
            "params": {"sessionId": TOOLS, "update": {
                "sessionUpdate": "kind_from_the_future", "payload": {"a": 1},
            }, "_meta": {"agentTimestampMs": 1000i64}},
        });
        // A legacy line is a bare notification with no envelope wrapper.
        let legacy = json!({
            "sessionId": TOOLS,
            "update": {"sessionUpdate": "current_mode_update", "currentModeId": "plan"},
        });
        for (row, kind) in [
            (&retry, "retry_state"),
            (&future, "kind_from_the_future"),
            (&legacy, "current_mode_update"),
        ] {
            let events = events_from_row(&session, 3, row);
            assert_eq!(events.len(), 1);
            let IngestEvent::Message(Message::System {
                content, options, ..
            }) = &events[0]
            else {
                panic!("carrier")
            };
            assert_eq!(content.as_deref().map(String::as_str), Some(kind));
            assert_eq!(raw_record(options), Some((*row).clone()));
        }
    }

    /// Row 7 lineage: the fork carries its parent in its own summary; the
    /// subagent child has no parent field of its own and resolves through the
    /// parent-side meta sidecar, taking the subagent brand subpath.
    #[tokio::test(flavor = "multi_thread")]
    async fn lineage_resolves_for_forks_and_subagents() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = ingest_fixtures(&temp).await?;

        let fork = store.get_session(FORK).await?.expect("fork session");
        assert_eq!(fork.session.parent_session_id.as_deref(), Some(TOOLS));
        assert_eq!(fork.session.source_agent, NAME, "a fork is a visible peer");

        let child = store.get_session(SUBAGENT_CHILD).await?.expect("child");
        assert_eq!(
            child.session.parent_session_id.as_deref(),
            Some(SUBAGENT_PARENT),
            "the parent link comes from the parent-side meta.json",
        );
        assert_eq!(child.session.source_agent, SUBAGENT_AGENT);
        let meta = child
            .session
            .options
            .get("source")
            .and_then(|source| source.get("subagent_meta"))
            .expect("the child carries the meta verbatim");
        assert_eq!(
            meta.get("subagent_type").and_then(Value::as_str),
            Some("explore"),
        );

        let parent = store.get_session(SUBAGENT_PARENT).await?.expect("parent");
        assert_eq!(parent.session.parent_session_id, None);
        assert_eq!(parent.session.source_agent, NAME);
        Ok(())
    }

    /// spec.md 6.8 value-equality: every captured session restores native to a
    /// value-equal `updates.jsonl` + `summary.json` at its own path (the
    /// sidecar-rich session dirs rule out `assert_native_restore`, which
    /// expects the source file set to be exactly the restored set).
    #[tokio::test(flavor = "multi_thread")]
    async fn native_restore_is_value_equal_to_every_captured_session() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = ingest_fixtures(&temp).await?;
        let ids = store.session_ids().await?;
        assert_eq!(ids.len(), FIXTURE_SESSIONS);
        for id in ids {
            let session = store.get_session(&id).await?.expect("session reads back");
            let files = GrokBuildFactory.serialize(&session, RestoreFidelity::Native)?;
            assert!(files.len() >= 2, "{id}: updates + summary at minimum");
            for file in &files {
                assert_eq!(file.actual_fidelity, RestoreFidelity::Native, "{id}");
                let source = Path::new(FIXTURES).join(&file.relative_path);
                let expected = std::fs::read(&source)
                    .map_err(|err| anyhow::anyhow!("read {}: {err}", source.display()))?;
                if file
                    .relative_path
                    .extension()
                    .is_some_and(|ext| ext == "jsonl")
                {
                    let parse = |bytes: &[u8]| -> anyhow::Result<Vec<Value>> {
                        std::str::from_utf8(bytes)?
                            .lines()
                            .filter(|line| !line.trim().is_empty())
                            .map(|line| serde_json::from_str(line).map_err(Into::into))
                            .collect()
                    };
                    assert_eq!(
                        parse(&file.bytes)?,
                        parse(&expected)?,
                        "jsonl mismatch at {}",
                        source.display(),
                    );
                } else {
                    let expected: Value = serde_json::from_slice(&expected)?;
                    let actual: Value = serde_json::from_slice(&file.bytes)?;
                    assert_eq!(actual, expected, "json mismatch at {}", source.display());
                }
            }
        }
        Ok(())
    }

    /// A foreign session reconstructs as an idiomatic update stream under a
    /// percent-encoded bucket named after its project, and re-ingests through
    /// this adapter.
    #[tokio::test(flavor = "multi_thread")]
    async fn foreign_restore_reconstructs_an_update_stream() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = ingest_fixtures(&temp).await?;
        let session = store.get_session(TOOLS).await?.expect("tools session");
        let files = GrokBuildFactory.serialize(&session, RestoreFidelity::Foreign)?;
        assert_eq!(files.len(), 1, "a reconstruction is the stream alone");
        assert_eq!(files[0].actual_fidelity, RestoreFidelity::Foreign);
        assert_eq!(
            files[0].relative_path,
            Path::new(MAC_BUCKET)
                .join(format!("{TOOLS}{RECONSTRUCTED_SUFFIX}"))
                .join(UPDATES_FILE),
            "a native-origin reconstruction takes a distinct dir name",
        );

        let restore_root = TempDir::new()?;
        crate::adapter::write_restored_files(restore_root.path(), files)?;
        let verify = Store::open_local(temp.path().join("verify")).await?;
        let summary = ingest_adapter(
            &verify,
            &GrokBuildAdapter::new(restore_root.path()),
            &crate::adapter::NoopOracle,
            |_| {},
        )
        .await?;
        assert_eq!(summary.dropped_events, 0);
        let restored = verify
            .get_session(&format!("{TOOLS}{RECONSTRUCTED_SUFFIX}"))
            .await?
            .expect("the reconstruction re-ingests");
        let originals = session
            .messages
            .iter()
            .filter(|m| !matches!(m.message, Message::System { .. }))
            .count();
        assert_eq!(
            restored.messages.len(),
            originals,
            "every typed message survives"
        );
        Ok(())
    }
}
