use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::PROTOCOL_VERSION;
use crate::adapter::Extracted;

pub type ProviderOptions = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    /// spec.md#model-parent-pointer-coherence: when set, `parent_session_id`
    /// MUST also be set. Spawn-only sources (claude-code subagents,
    /// nanoclaw) leave this `None`; fork-with-cut-point sources
    /// (pi-coding-agent) populate both pointers together.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_message_id: Option<String>,
    pub source_agent: String,
    pub created_at: DateTime<Utc>,
    pub project: Extracted<String>,
    #[serde(default)]
    pub options: ProviderOptions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    System {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        /// `None` when the source row carried no content. The seal on
        /// `Extracted<String>` means adapters CANNOT pass a synthesized
        /// or sentinel string here - the value either flows from a
        /// `Source` extraction or the field is `None`. Distinguishes
        /// "source said content=''" (Some(extracted_empty)) from
        /// "source had no content field" (None).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<Extracted<String>>,
        #[serde(default)]
        options: ProviderOptions,
    },
    User {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        #[serde(default)]
        options: ProviderOptions,
    },
    Assistant {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        #[serde(default)]
        options: ProviderOptions,
    },
    Tool {
        id: String,
        session_id: String,
        timestamp: DateTime<Utc>,
        #[serde(default)]
        options: ProviderOptions,
    },
}

impl Message {
    pub fn id(&self) -> &str {
        match self {
            Self::System { id, .. }
            | Self::User { id, .. }
            | Self::Assistant { id, .. }
            | Self::Tool { id, .. } => id,
        }
    }

    pub fn session_id(&self) -> &str {
        match self {
            Self::System { session_id, .. }
            | Self::User { session_id, .. }
            | Self::Assistant { session_id, .. }
            | Self::Tool { session_id, .. } => session_id,
        }
    }

    pub fn role(&self) -> Role {
        match self {
            Self::System { .. } => Role::System,
            Self::User { .. } => Role::User,
            Self::Assistant { .. } => Role::Assistant,
            Self::Tool { .. } => Role::Tool,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::System { timestamp, .. }
            | Self::User { timestamp, .. }
            | Self::Assistant { timestamp, .. }
            | Self::Tool { timestamp, .. } => *timestamp,
        }
    }

    pub fn options(&self) -> &ProviderOptions {
        match self {
            Self::System { options, .. }
            | Self::User { options, .. }
            | Self::Assistant { options, .. }
            | Self::Tool { options, .. } => options,
        }
    }

    pub fn options_mut(&mut self) -> &mut ProviderOptions {
        match self {
            Self::System { options, .. }
            | Self::User { options, .. }
            | Self::Assistant { options, .. }
            | Self::Tool { options, .. } => options,
        }
    }

    pub fn system_content(&self) -> Option<&str> {
        match self {
            // Two layers of `as_deref`: the outer `Option<Extracted<String>>`
            // becomes `Option<&Extracted<String>>`, then `Extracted: Deref`
            // unwraps to `&str`.
            Self::System { content, .. } => content.as_deref().map(|e| &**e),
            Self::User { .. } | Self::Assistant { .. } | Self::Tool { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

/// Whether a Part's content is conversation or harness-injected scaffolding
/// (spec.md#model-part-provenance). No `Default` and no `#[serde(default)]` on the
/// `Part.provenance` field below: constructing a Part without classifying it
/// MUST be a compile error (spec.md#adapter-provenance-required).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    Conversational,
    Injected,
}

impl Provenance {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conversational => "conversational",
            Self::Injected => "injected",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Part {
    pub session_id: String,
    pub id: String,
    pub message_id: String,
    pub ordinal: i32,
    /// Conversation vs harness-injected (spec.md#model-part-provenance). Mandatory,
    /// no serde default - search reads it to exclude injected scaffolding.
    pub provenance: Provenance,
    #[serde(default)]
    pub options: ProviderOptions,
    #[serde(flatten)]
    pub kind: PartKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartKind {
    Text {
        /// `None` when the source row had no text field. The seal on
        /// `Extracted<String>` means adapters CANNOT pass a synthesized
        /// empty string or any other placeholder here - the value either
        /// flows from a `Source` extraction or the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<Extracted<String>>,
    },
    Reasoning {
        /// `None` when the source row had no reasoning text. Type-system
        /// guard against `unwrap_or_default()`-style fallbacks: the
        /// `Extracted<String>` seal forces the adapter to either get the
        /// value from a `Source` or admit it is absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<Extracted<String>>,
    },
    File {
        /// `None` when the source row carried no MIME hint. Sealed against
        /// `unwrap_or("application/octet-stream")`-style fallbacks: an absent
        /// type is faithfully absent, not a synthesized default
        /// (spec.md#model-no-synthesis).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        media_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_name: Option<String>,
        data: FileData,
    },
    ToolCall {
        /// `None` when the source carried no call_id (rare; malformed).
        /// Sealed via `Extracted<String>` - empty-string sentinels are
        /// not constructable from adapter code.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<Extracted<String>>,
        /// `None` when the source carried no tool name. claude-code
        /// always carries it on `tool_use` rows; codex-cli sometimes
        /// has placeholder shapes. The seal makes synthesized names
        /// unconstructable from adapter code (spec.md#model-no-synthesis).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<Extracted<String>>,
        params: Value,
        provider_executed: bool,
    },
    ToolResult {
        /// `None` when the source carried no `tool_use_id` link.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<Extracted<String>>,
        /// `None` when the adapter could not resolve the tool name.
        /// In claude-code, name lives only on the prior `tool_use` row;
        /// the adapter resolves via a per-file `tool_use_id -> name`
        /// map and surfaces a miss (e.g. compaction pruned the originating
        /// call) as `None`, never as a fabricated string
        /// (spec.md#model-no-synthesis).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<Extracted<String>>,
        is_failure: bool,
        result: Value,
    },
    ToolApprovalRequest {
        approval_id: String,
        tool_call_id: String,
    },
    ToolApprovalResponse {
        approval_id: String,
        approved: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl PartKind {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Text { .. } => "text",
            Self::Reasoning { .. } => "reasoning",
            Self::File { .. } => "file",
            Self::ToolCall { .. } => "tool_call",
            Self::ToolResult { .. } => "tool_result",
            Self::ToolApprovalRequest { .. } => "tool_approval_request",
            Self::ToolApprovalResponse { .. } => "tool_approval_response",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FileData {
    String(String),
    Bytes(Vec<u8>),
    Url(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    ValidationFailed,
    VersionUnsupported,
    NotFound,
    NamespaceUnknown,
    StorageUnavailable,
    Conflict,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

// The success/error size gap is fine here: a `GetEnvelope` is one per-request
// return value, serialized immediately - never stored in bulk where the gap
// would waste memory.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetEnvelope {
    Success(GetResponse),
    Error(ErrorEnvelope),
}

/// Whole-session read (spec.md#protocol). `id` names either kind: a session
/// id reads that session; a message id resolves up to its parent session with
/// the page anchored at that message
/// (`GetResult::Session.resolved_from_message_id` records the resolution) -
/// intent comes from the endpoint, so upcasting is always safe. The alias
/// accepts this endpoint's own typed name; the other type's name is not an
/// alias - cross-type forgiveness lives in the value, not the param name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetSessionRequest {
    pub protocol_version: u16,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(alias = "session_id")]
    pub id: String,
    /// Max messages per page.
    #[serde(default = "default_get_limit")]
    pub limit: usize,
    /// Which end to read the first page from - `start` (oldest, default) or
    /// `end` (most recent, e.g. post-compaction recovery). Pages stay
    /// chronological. Ignored once an anchor below is set.
    #[serde(default)]
    pub from: SessionFrom,
    /// Page forward - messages strictly after this id.
    #[serde(default)]
    pub after_message_id: Option<String>,
    /// Page backward - messages strictly before this id.
    #[serde(default)]
    pub before_message_id: Option<String>,
}

/// Single-message read (spec.md#protocol): the target with its full part
/// bodies plus conversational neighbors. `id` must be a message id; a session
/// id cannot resolve to one message (which one?), so the handler rejects it
/// with a hint naming the session read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetMessageRequest {
    pub protocol_version: u16,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(alias = "message_id")]
    pub id: String,
    /// Conversational sibling messages before the target (mirrors `grep -B`).
    #[serde(default = "default_context")]
    pub context_before: usize,
    /// Conversational sibling messages after the target (mirrors `grep -A`).
    #[serde(default = "default_context")]
    pub context_after: usize,
}

/// Which end of a session `pond_get_session` reads its first page from
/// (spec.md#protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionFrom {
    /// Oldest messages first (the session's start).
    #[default]
    Start,
    /// Most recent messages (the session's tail), still chronological.
    End,
}

/// The session header is always present; `result` carries the mode-specific
/// payload, discriminated by a `scope` tag (spec.md#protocol). Flattened so a
/// client reads `session` / `scope` / payload fields off one object - no
/// `session.session` nesting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetResponse {
    pub session: GetSession,
    #[serde(flatten)]
    pub result: GetResult,
}

/// Trimmed session header (spec.md#protocol): adapter-redundant `options`,
/// parent pointers (served by `restore_lineage`), and per-message session id
/// dropped to keep get responses lean for agent context windows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetSession {
    pub id: String,
    pub source_agent: String,
    pub project: String,
    pub created_at: DateTime<Utc>,
}

impl GetSession {
    pub fn from_session(session: &Session) -> Self {
        Self {
            id: session.id.clone(),
            source_agent: session.source_agent.clone(),
            project: (*session.project).clone(),
            created_at: session.created_at,
        }
    }
}

/// Per-message view in a get response (spec.md#protocol). Always
/// conversational: `text`/`content` plus one-line part summaries. Full part
/// bodies ride `GetResult::Message.target_parts`, reached by `message_id`
/// scope - a session view never inlines them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageView {
    pub id: String,
    pub role: Role,
    pub timestamp: DateTime<Utc>,
    /// Conversational text (`search_text`); absent for carrier rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// System-message content string, when the source carried one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts_summary: Vec<PartSummary>,
}

/// Compact per-part descriptor (spec.md#protocol): enough to tell what a
/// message carries without paying for full content. `call_id` is populated
/// for `tool_call` / `tool_result` only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartSummary {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    /// One-line body excerpt from [`part_preview`] - what the tool was
    /// actually called with, so a session view distinguishes two `Bash` calls
    /// without fetching either body. `None` for a kind with nothing to
    /// preview, and for a store whose `preview` column is still NULL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

impl PartSummary {
    /// Project a canonical [`PartKind`] into its compact response descriptor, or
    /// `None` for a kind that does not earn a summary. Exhaustive on purpose - a
    /// new `PartKind` variant must decide here. `call_id` is carried for
    /// `tool_call` / `tool_result` only.
    ///
    /// `text` and `reasoning` return `None`: a text part's content already rides
    /// the message's `text`/`content` (a summary would duplicate it), and
    /// reasoning is deliberately not surfaced in the session/conversational view
    /// (its full body is still rendered when a message is fetched by `message_id`
    /// scope). The kinds that survive are exactly [`SUMMARY_PART_TYPES`].
    pub fn for_kind(kind: &PartKind) -> Option<Self> {
        let (label, call_id) = match kind {
            PartKind::Text { .. } | PartKind::Reasoning { .. } => return None,
            PartKind::File {
                media_type,
                file_name,
                ..
            } => (file_name.clone().or_else(|| media_type.clone()), None),
            PartKind::ToolCall { name, call_id, .. } => {
                (name.as_deref().cloned(), call_id.as_deref().cloned())
            }
            PartKind::ToolResult {
                name,
                call_id,
                is_failure,
                ..
            } => {
                let label = name.as_deref().map(|name| {
                    if *is_failure {
                        format!("{name} (failed)")
                    } else {
                        name.clone()
                    }
                });
                (label, call_id.as_deref().cloned())
            }
            PartKind::ToolApprovalRequest { approval_id, .. } => (Some(approval_id.clone()), None),
            PartKind::ToolApprovalResponse {
                approval_id,
                approved,
                ..
            } => {
                let verb = if *approved { "approved" } else { "denied" };
                (Some(format!("{approval_id} ({verb})")), None)
            }
        };
        Some(Self {
            kind: kind.type_name().to_owned(),
            label,
            call_id,
            preview: part_preview(kind),
        })
    }

    /// Rebuild a summary from the materialized `parts` columns - the local
    /// parts-summary map's twin of [`Self::for_kind`], which has no body to
    /// read. The two MUST agree: a page served from the map and the same page
    /// served by a `parts` scan are the same response.
    ///
    /// `tool_name` is the label for the tool kinds; every other kind's label
    /// *is* its one-line descriptor, which is exactly what `preview` stores
    /// (see [`part_preview`]), so it doubles as the label there.
    pub fn from_columns(
        kind: &str,
        tool_name: Option<&str>,
        call_id: Option<&str>,
        is_failure: Option<bool>,
        preview: Option<&str>,
    ) -> Self {
        let label = match kind {
            "tool_call" => tool_name.map(str::to_owned),
            "tool_result" => tool_name.map(|name| {
                if is_failure.unwrap_or(false) {
                    format!("{name} (failed)")
                } else {
                    name.to_owned()
                }
            }),
            _ => preview.map(str::to_owned),
        };
        Self {
            kind: kind.to_owned(),
            label,
            // `call_id` rides the response for the call/result pair only, as
            // in `for_kind` - the stored column also carries an approval
            // request's `tool_call_id` (spec.md#5.6, one correlation key), and
            // surfacing that here would make the map path answer differently.
            call_id: matches!(kind, "tool_call" | "tool_result")
                .then(|| call_id.map(str::to_owned))
                .flatten(),
            preview: preview.map(str::to_owned),
        }
    }
}

/// Bumped whenever [`part_preview`] would render a stored part differently.
/// Stamped into the `parts` schema on the `preview` field, so a store carries
/// the renderer that produced its column and a later pond can tell that the
/// column needs re-deriving (spec.md#session-additive-schema-backfill - the
/// same re-derivation seam `embedding_model` gives `vector`).
pub const PREVIEW_RENDERER_VERSION: u32 = 1;

/// Character budget for a rendered preview. Sized so a page of summaries
/// stays a page: ~160 chars is one terminal line and two orders of magnitude
/// under the p99 tool body.
const PREVIEW_MAX_CHARS: usize = 160;

/// Parameter keys the preview renderer reads, in render order. These are the
/// keys that identify a call at a glance across the harnesses pond ingests -
/// the audited hot paths (`$.params.command` alone accounts for 898 of the
/// hand-rolled `pond_sql` previews). A params object carrying none of them
/// falls back to compact JSON, so an unknown tool still previews.
const PREVIEW_PARAM_KEYS: &[&str] = &[
    "command",
    "file_path",
    "path",
    "pattern",
    "url",
    "query",
    "prompt",
    "description",
];

/// The one-line descriptor materialized into `parts.preview` and served in
/// [`PartSummary`]. `None` only for `text` and `reasoning`, whose bodies the
/// message's own text already carries.
///
/// `tool_call` renders the known parameter keys it carries, else its whole
/// params object as compact JSON; `tool_result` renders a head window of its
/// body. For the remaining kinds the descriptor is the label itself - a file's
/// name, an approval's identifiers - which is deliberate: it is what lets
/// [`PartSummary::from_columns`] rebuild the summary from the stored columns
/// with no body to read. Every arm is whitespace-collapsed to one line and
/// truncated on a char boundary, so a preview is always safe to print inline.
pub fn part_preview(kind: &PartKind) -> Option<String> {
    let rendered = match kind {
        PartKind::Text { .. } | PartKind::Reasoning { .. } => return None,
        PartKind::File {
            file_name,
            media_type,
            ..
        } => file_name.clone().or_else(|| media_type.clone())?,
        PartKind::ToolCall { params, .. } => render_params(params)?,
        PartKind::ToolResult { result, .. } => preview_text(result)?,
        PartKind::ToolApprovalRequest { approval_id, .. } => approval_id.clone(),
        PartKind::ToolApprovalResponse {
            approval_id,
            approved,
            ..
        } => {
            let verb = if *approved { "approved" } else { "denied" };
            format!("{approval_id} ({verb})")
        }
    };
    let one_line = collapse_whitespace(&rendered);
    (!one_line.is_empty()).then(|| truncate_chars(&one_line, PREVIEW_MAX_CHARS))
}

/// The materialized `parts.body_text` cell: a `tool_call`'s params as text,
/// NULL for every other kind. Result bodies are deliberately left out - they
/// are 7.8x the params corpus and only 16 of 3,699 audited `pond_sql` calls
/// hunted them unscoped (docs/plans/2609-17-read-latency-campaign.md).
pub fn part_body_text(kind: &PartKind) -> Option<String> {
    match kind {
        PartKind::ToolCall { params, .. } => value_text(params),
        PartKind::Text { .. }
        | PartKind::Reasoning { .. }
        | PartKind::File { .. }
        | PartKind::ToolResult { .. }
        | PartKind::ToolApprovalRequest { .. }
        | PartKind::ToolApprovalResponse { .. } => None,
    }
}

/// Known-param-keys renderer: `key=value` for each [`PREVIEW_PARAM_KEYS`] the
/// object carries, else the compact-JSON fallback. A single known key renders
/// bare - the common `{"command": "ls"}` shape reads as the command itself.
fn render_params(params: &Value) -> Option<String> {
    let Some(object) = params.as_object() else {
        return value_text(params);
    };
    let known: Vec<(&str, String)> = PREVIEW_PARAM_KEYS
        .iter()
        .filter_map(|key| {
            let text = object.get(*key).and_then(preview_text)?;
            Some((*key, text))
        })
        .collect();
    match known.as_slice() {
        [] => value_text(params),
        [(_, only)] => Some(only.clone()),
        many => Some(
            many.iter()
                .map(|(key, text)| format!("{key}={text}"))
                .collect::<Vec<_>>()
                .join(" "),
        ),
    }
}

/// [`value_text`] through a bounded head window. A tool result can be
/// megabytes, and a preview that cloned the whole body only to truncate it
/// would allocate the result corpus once per ingest. The window is generous
/// enough that whitespace collapse cannot pull the rendered line under its
/// budget for any realistic body.
fn preview_text(value: &Value) -> Option<String> {
    const HEAD_WINDOW_CHARS: usize = PREVIEW_MAX_CHARS * 8;
    match value {
        Value::String(text) => {
            let end = text
                .char_indices()
                .nth(HEAD_WINDOW_CHARS)
                .map_or(text.len(), |(at, _)| at);
            Some(text[..end].to_owned())
        }
        other => value_text(other),
    }
}

/// A JSON value as text: a string is its own text, anything else is compact
/// JSON. `null` and an unrenderable value are `None`, so an absent body stays
/// absent rather than rendering as the word "null".
fn value_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => serde_json::to_string(other).ok(),
    }
}

/// Collapse every whitespace run to one space and trim - a preview is one
/// line, and a tool body's newlines and indentation carry nothing at this
/// width.
fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Truncate to `max` chars (not bytes - a multibyte body must not be cut
/// mid-character), marking the cut so a reader knows the body continues.
fn truncate_chars(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((at, _)) => format!("{}...", &text[..at]),
        None => text.to_owned(),
    }
}

/// Canonical part `type` names that yield a [`PartSummary`] - every kind except
/// `text` and `reasoning` (see [`PartSummary::for_kind`], the source of truth).
/// The summary read paths filter the parts scan to these so a text/reasoning
/// heavy session never loads parts that would summarize to nothing.
pub const SUMMARY_PART_TYPES: &[&str] = &[
    "file",
    "tool_call",
    "tool_result",
    "tool_approval_request",
    "tool_approval_response",
];

/// A `Part` as it rides a get response (spec.md#protocol): the canonical
/// part minus `session_id` / `message_id`, which the enclosing session and
/// message already identify. Built from a canonical [`Part`] in the handler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponsePart {
    pub id: String,
    pub ordinal: i32,
    pub provenance: Provenance,
    #[serde(default, skip_serializing_if = "ProviderOptions::is_empty")]
    pub options: ProviderOptions,
    #[serde(flatten)]
    pub kind: PartKind,
}

impl ResponsePart {
    pub fn from_part(part: Part) -> Self {
        Self {
            id: part.id,
            ordinal: part.ordinal,
            provenance: part.provenance,
            options: part.options,
            kind: part.kind,
        }
    }
}

/// Mode-specific get payload, tagged by `scope` and flattened into
/// `GetResponse` alongside the shared session header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum GetResult {
    Session {
        messages: Vec<MessageView>,
        /// Conversational messages before the emitted page (the top marker's
        /// `before_message_id` cursor exists when this is > 0).
        before_remaining: usize,
        /// Conversational messages after the emitted page (the bottom marker's
        /// `after_message_id` cursor exists when this is > 0).
        after_remaining: usize,
        /// Set when the request's `session_id` was actually a message id that
        /// the server resolved up to this session; the page is anchored at
        /// that message.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolved_from_message_id: Option<String>,
    },
    Message {
        target: MessageView,
        target_parts: Vec<ResponsePart>,
        target_parts_remaining: usize,
        /// `context_before` + `context_after` conversational messages around
        /// the target (target excluded).
        siblings: Vec<MessageView>,
        /// Request echo so the rendered header can state the window size.
        context_before: usize,
        /// Request echo so the rendered header can state the window size.
        context_after: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SearchEnvelope {
    Success(SearchResponse),
    Error(ErrorEnvelope),
}

/// JSON shape is externally tagged: `{"contains": "pond"}` or
/// `{"regex": "^/Users/.*"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectFilter {
    Contains(String),
    Regex(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    pub protocol_version: u16,
    #[serde(default)]
    pub namespace: Option<String>,
    pub query: String,
    /// Retrieval arm (spec.md#search). `fts` (default) matches exact whole
    /// words via BM25; `vector` matches on meaning and is available only when
    /// the serving instance has `[embeddings].enabled = true` - otherwise it is
    /// refused. The agent picks per query; there is no server-side fusion.
    #[serde(default)]
    pub mode: SearchModeWire,
    /// Result ordering. `relevance` (default) ranks by match strength (vector:
    /// cosine + a gentle recency tiebreaker; fts: BM25); `recency` ranks
    /// strictly newest-first. A recency-sorted response is labeled so the
    /// caller does not misread rank-1 as the best match.
    #[serde(default)]
    pub sort_by: SortBy,
    #[serde(default)]
    pub filters: SearchFilters,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// Wire-level retrieval arm (spec.md#search). `fts` (default) matches exact
/// whole words via BM25; `vector` matches on meaning and is available only when
/// the serving instance has `[embeddings].enabled = true` - otherwise it is
/// refused. The agent picks per query; there is no server-side fusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchModeWire {
    #[default]
    Fts,
    Vector,
}

/// Result ordering for `pond_search` (spec.md#search).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortBy {
    /// Match strength: vector = cosine + recency tiebreaker, fts = BM25.
    #[default]
    Relevance,
    /// Strictly newest-first; the response is labeled as recency-sorted.
    Recency,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SearchFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Filter to one source harness with exact-or-subpath semantics: the value
    /// itself plus its `/`-subpaths (`openclaw` covers `openclaw/subagent`, not
    /// `openclaw-x`). A source_agent filter also disables the default subagent
    /// exclusion (spec.md#search) - the caller is scoping deliberately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResponse {
    pub sessions: Vec<SearchSession>,
    pub matched_total: usize,
    /// How many messages with conversational text the caller's filters left
    /// in scope - the universe the search actually ran over. The absence
    /// signal: 0 means the filters excluded everything before retrieval, and
    /// a small value warns that "no relevant hits" covers a thin slice.
    #[serde(default)]
    pub searchable_in_scope: usize,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchSession {
    pub session_id: String,
    pub project: String,
    pub source_agent: String,
    pub session_messages_count: usize,
    pub matched_message_count: usize,
    pub matches: Vec<SearchResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub message_id: String,
    pub role: Role,
    pub timestamp: DateTime<Utc>,
    pub text: String,
    pub score: f64,
    /// Populated only for user-role hits: distinguishes a plain-text prompt
    /// from one carrying file attachments or multi-part scaffolding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts_summary: Vec<PartSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IngestEnvelope {
    Success(IngestResponse),
    Error(ErrorEnvelope),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestRequest {
    pub protocol_version: u16,
    #[serde(default)]
    pub namespace: Option<String>,
    pub events: Vec<crate::sessions::IngestEvent>,
}

/// `pond_ingest` response (spec.md#protocol). `accepted = inserted + matched`,
/// `rejected = error`; both derived from `results`. Per-row `results[]` is
/// the contract clients rely on to reconcile retries (the PK is echoed so
/// the client can match outcomes back to its input even when `index` is not
/// enough). Each result reports the input event's `index`, `kind`, `pk`,
/// `status`, and an `error` body when `status = "error"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestResponse {
    pub accepted: usize,
    pub rejected: usize,
    pub results: Vec<IngestResult>,
}

/// One row of `pond_ingest` per-row output (spec.md#protocol).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestResult {
    /// Position in the request's `events` array (0-based).
    pub index: usize,
    /// `"session"` | `"message"` | `"part"`, matching `IngestEvent::kind`.
    pub kind: String,
    /// Echoed primary key: scalar for session, `[session_id, message_id]` for
    /// message, `[session_id, message_id, part_id]` for part. Lets clients reconcile
    /// against their own state on retry.
    pub pk: Value,
    pub status: IngestStatus,
    /// Set only when `status = "error"`. Carries the same shape as the
    /// envelope-level error body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestStatus {
    /// New PK; `merge_insert` wrote a fresh row.
    Inserted,
    /// PK existed; `merge_insert` matched it (no-op per spec.md#adapter-integrity-additive-sync).
    Matched,
    /// Per-row failure: validation or storage error. See `error` field.
    Error,
}

fn default_limit() -> usize {
    10
}

pub fn new_request_id() -> String {
    format!("req_{}", Uuid::now_v7())
}

pub const DEFAULT_NAMESPACE: &str = "local";

pub fn default_namespace() -> String {
    DEFAULT_NAMESPACE.to_owned()
}

fn default_get_limit() -> usize {
    20
}

fn default_context() -> usize {
    3
}

pub fn validate_protocol(version: u16) -> Result<(), ErrorEnvelope> {
    if version == PROTOCOL_VERSION {
        return Ok(());
    }

    Err(error(
        ErrorCode::VersionUnsupported,
        "unsupported protocol_version",
        serde_json::json!({
            "received": version,
            "supported": [PROTOCOL_VERSION],
        }),
    ))
}

pub fn error(code: ErrorCode, message: impl Into<String>, details: Value) -> ErrorEnvelope {
    ErrorEnvelope {
        error: ErrorBody {
            code,
            message: message.into(),
            details,
        },
    }
}

impl From<crate::Error> for ErrorEnvelope {
    fn from(error_value: crate::Error) -> Self {
        match error_value {
            crate::Error::Validation {
                message,
                field,
                value,
                expected,
            } => error(
                ErrorCode::ValidationFailed,
                message,
                validation_details(field, value, expected),
            ),
            crate::Error::NotFound { message, kind, pk } => error(
                ErrorCode::NotFound,
                message,
                serde_json::json!({ "kind": kind, "pk": pk }),
            ),
            crate::Error::NamespaceUnknown { namespace } => error(
                ErrorCode::NamespaceUnknown,
                "namespace unknown",
                serde_json::json!({ "namespace": namespace }),
            ),
            crate::Error::Conflict { attempts } => error(
                ErrorCode::Conflict,
                "commit conflict after retries exhausted",
                serde_json::json!({ "attempts": attempts }),
            ),
            crate::Error::Storage(error_value) => storage_error(error_value),
            crate::Error::Internal(message) => {
                error(ErrorCode::Internal, message, serde_json::json!({}))
            }
        }
    }
}

fn validation_details(
    field: Option<String>,
    value: Option<Value>,
    expected: Option<String>,
) -> Value {
    let mut details = Map::new();
    if let Some(field) = field {
        details.insert("field".to_owned(), Value::String(field));
    }
    if let Some(value) = value {
        details.insert("value".to_owned(), value);
    }
    if let Some(expected) = expected {
        details.insert("expected".to_owned(), Value::String(expected));
    }
    Value::Object(details)
}

pub fn storage_error(error_value: anyhow::Error) -> ErrorEnvelope {
    // Lance's "fragment N referenced by an address-domain index result was not
    // found" internal error means a compaction orphaned the timestamp
    // zonemap's fragment references - any writer without the same-run
    // self-heal does this, including this binary's own compaction in the
    // window before its indices phase heals it. The store's data is intact
    // and the index is recreatable; name the recovery instead of surfacing a
    // bare storage failure.
    let stale_address_index = error_value.chain().any(|cause| {
        cause
            .to_string()
            .contains("referenced by an address-domain index result")
    });
    if stale_address_index {
        return error(
            ErrorCode::StorageUnavailable,
            "date-filter index is stale: it references fragments that compaction rewrote; \
             run `pond optimize --rebuild`, or the next `pond sync`/`pond optimize` from \
             this pond version repairs it automatically",
            serde_json::json!({ "underlying": format!("{error_value:#}") }),
        );
    }
    error(
        ErrorCode::StorageUnavailable,
        "storage operation failed",
        serde_json::json!({ "underlying": format!("{error_value:#}") }),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::json;

    fn tool_call(params: serde_json::Value) -> PartKind {
        PartKind::ToolCall {
            call_id: Some(crate::adapter::Extracted::from_test_value("c1".to_owned())),
            name: Some(crate::adapter::Extracted::from_test_value(
                "Bash".to_owned(),
            )),
            params,
            provider_executed: false,
        }
    }

    #[test]
    fn preview_renders_one_known_param_key_bare() {
        assert_eq!(
            part_preview(&tool_call(json!({ "command": "ls -la /tmp" }))),
            Some("ls -la /tmp".to_owned()),
            "the single-key case reads as the value itself",
        );
    }

    #[test]
    fn preview_labels_several_known_param_keys() {
        assert_eq!(
            part_preview(&tool_call(json!({
                "file_path": "/etc/hosts",
                "command": "cat",
                "ignored": "not a known key",
            }))),
            Some("command=cat file_path=/etc/hosts".to_owned()),
            "known keys render in PREVIEW_PARAM_KEYS order, not the object's",
        );
    }

    #[test]
    fn preview_falls_back_to_compact_json() {
        assert_eq!(
            part_preview(&tool_call(json!({ "todos": [{ "id": 1 }] }))),
            Some("{\"todos\":[{\"id\":1}]}".to_owned()),
            "a params object with no known key still previews",
        );
        assert_eq!(
            part_preview(&tool_call(json!("a bare string body"))),
            Some("a bare string body".to_owned()),
        );
        assert_eq!(
            part_preview(&tool_call(json!(null))),
            None,
            "an absent body previews as absent, never as the word null",
        );
    }

    #[test]
    fn preview_is_one_line_and_bounded() {
        let preview = part_preview(&tool_call(json!({ "command": "echo one\n  echo two" })))
            .expect("preview rendered");
        assert_eq!(preview, "echo one echo two", "newlines collapse to spaces");

        let long = "x".repeat(400);
        let preview = part_preview(&tool_call(json!({ "command": long }))).expect("rendered");
        assert_eq!(preview.chars().count(), PREVIEW_MAX_CHARS + 3);
        assert!(preview.ends_with("..."), "the cut is marked: {preview}");

        // Truncation counts chars, not bytes: cutting mid-character would
        // panic on the slice and poison the whole preview column.
        let wide = "\u{1f300}".repeat(400);
        let preview = part_preview(&tool_call(json!({ "command": wide }))).expect("rendered");
        assert_eq!(preview.chars().count(), PREVIEW_MAX_CHARS + 3);
    }

    #[test]
    fn preview_covers_every_kind_that_earns_a_summary() {
        let file = PartKind::File {
            media_type: Some("text/plain".to_owned()),
            file_name: Some("notes.md".to_owned()),
            data: FileData::Bytes(Vec::new()),
        };
        assert_eq!(part_preview(&file), Some("notes.md".to_owned()));
        let result = PartKind::ToolResult {
            call_id: Some(crate::adapter::Extracted::from_test_value("c1".to_owned())),
            name: Some(crate::adapter::Extracted::from_test_value(
                "Bash".to_owned(),
            )),
            is_failure: true,
            result: json!("exit 1: no such file"),
        };
        assert_eq!(
            part_preview(&result),
            Some("exit 1: no such file".to_owned())
        );
        let approval = PartKind::ToolApprovalResponse {
            approval_id: "a1".to_owned(),
            approved: false,
            reason: None,
        };
        assert_eq!(part_preview(&approval), Some("a1 (denied)".to_owned()));
        // text/reasoning carry their body in the message's own text.
        assert_eq!(
            part_preview(&PartKind::Text {
                text: Some(crate::adapter::Extracted::from_test_value("hi".to_owned())),
            }),
            None,
        );
        assert_eq!(
            part_preview(&PartKind::Reasoning {
                text: Some(crate::adapter::Extracted::from_test_value("hmm".to_owned())),
            }),
            None,
        );
    }

    #[test]
    fn body_text_materializes_tool_call_params_only() {
        assert_eq!(
            part_body_text(&tool_call(json!({ "command": "ls" }))),
            Some("{\"command\":\"ls\"}".to_owned()),
            "params serialize in full, unlike the bounded preview",
        );
        let result = PartKind::ToolResult {
            call_id: None,
            name: None,
            is_failure: false,
            result: json!({ "stdout": "lots of bytes" }),
        };
        assert_eq!(
            part_body_text(&result),
            None,
            "result bodies are deliberately not materialized",
        );
        assert_eq!(
            part_body_text(&PartKind::File {
                media_type: None,
                file_name: Some("a.bin".to_owned()),
                data: FileData::Bytes(Vec::new()),
            }),
            None,
        );
    }

    /// The map path rebuilds a summary from the stored columns with no body to
    /// read; it must land on exactly what the body-reading path produces, or a
    /// page's content would depend on whether the map was warm.
    #[test]
    fn from_columns_reproduces_for_kind() {
        let kinds = vec![
            tool_call(json!({ "command": "ls -la" })),
            PartKind::ToolResult {
                call_id: Some(crate::adapter::Extracted::from_test_value("c1".to_owned())),
                name: Some(crate::adapter::Extracted::from_test_value(
                    "Bash".to_owned(),
                )),
                is_failure: true,
                result: json!("exit 1"),
            },
            PartKind::ToolResult {
                call_id: Some(crate::adapter::Extracted::from_test_value("c2".to_owned())),
                name: Some(crate::adapter::Extracted::from_test_value(
                    "Grep".to_owned(),
                )),
                is_failure: false,
                result: json!("3 matches"),
            },
            PartKind::File {
                media_type: Some("image/png".to_owned()),
                file_name: None,
                data: FileData::Bytes(Vec::new()),
            },
            PartKind::ToolApprovalRequest {
                approval_id: "a1".to_owned(),
                tool_call_id: "c1".to_owned(),
            },
            PartKind::ToolApprovalResponse {
                approval_id: "a1".to_owned(),
                approved: true,
                reason: None,
            },
        ];
        for kind in kinds {
            let expected = PartSummary::for_kind(&kind).expect("kind earns a summary");
            // Exactly the cells `parts` materializes for this part.
            let (tool_name, call_id, is_failure) = match &kind {
                PartKind::ToolCall { name, call_id, .. } => {
                    (name.as_deref().cloned(), call_id.as_deref().cloned(), None)
                }
                PartKind::ToolResult {
                    name,
                    call_id,
                    is_failure,
                    ..
                } => (
                    name.as_deref().cloned(),
                    call_id.as_deref().cloned(),
                    Some(*is_failure),
                ),
                PartKind::ToolApprovalRequest { tool_call_id, .. } => {
                    (None, Some(tool_call_id.clone()), None)
                }
                _ => (None, None, None),
            };
            let rebuilt = PartSummary::from_columns(
                kind.type_name(),
                tool_name.as_deref(),
                call_id.as_deref(),
                is_failure,
                part_preview(&kind).as_deref(),
            );
            assert_eq!(rebuilt, expected, "{} summary diverged", kind.type_name());
        }
    }

    #[test]
    fn wire_envelope_carries_conflict_code_and_attempts_detail() {
        let envelope: ErrorEnvelope = crate::Error::Conflict { attempts: 3 }.into();
        assert_eq!(envelope.error.code, ErrorCode::Conflict);
        assert_eq!(envelope.error.details, json!({ "attempts": 3 }));
    }

    #[test]
    fn storage_error_names_the_stale_zonemap_recovery() {
        let lance_internal = anyhow::anyhow!(
            "Internal error: fragment 5225 referenced by an address-domain index result \
             was not found in the dataset"
        )
        .context("scan failed");
        let envelope = storage_error(lance_internal);
        assert_eq!(envelope.error.code, ErrorCode::StorageUnavailable);
        assert!(envelope.error.message.contains("pond optimize --rebuild"));
        let underlying = envelope.error.details["underlying"]
            .as_str()
            .expect("underlying detail");
        assert!(underlying.contains("fragment 5225"));

        let generic = storage_error(anyhow::anyhow!("connection refused"));
        assert_eq!(generic.error.message, "storage operation failed");
    }
}
