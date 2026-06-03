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
        media_type: String,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetRequest {
    pub protocol_version: u16,
    #[serde(default)]
    pub namespace: Option<String>,
    // Mutually exclusive scopes.
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    /// `message_id` mode only: symmetric window radius, mirroring `grep -C`.
    /// Returns up to `2*context_depth` siblings around the target. Ignored in
    /// session mode.
    #[serde(default)]
    pub context_depth: usize,
    #[serde(default = "default_get_limit")]
    pub limit: usize,
    /// Ignored in `message_id` mode (that mode always returns the target with
    /// its full parts, paginated).
    #[serde(default)]
    pub response_mode: ResponseMode,
    /// Exclusive continuation anchor: last `message_id` in session mode, last
    /// `part_id` in message mode. The append-only invariant means one id is
    /// enough state - no opaque cursor.
    #[serde(default)]
    pub after_id: Option<String>,
}

/// How much of each message `pond_get` materializes (spec.md#protocol).
/// Ignored when `message_id` is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseMode {
    /// `search_text IS NOT NULL` conversational view + part summaries.
    #[default]
    Conversational,
    /// All messages (including carriers) + part summaries.
    Complete,
    /// All messages + full parts inline (heaviest mode).
    Verbatim,
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
/// dropped to keep `pond_get` responses lean for agent context windows.
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

/// Per-message view in a `pond_get` response. `parts_summary` is always
/// present (possibly empty); `parts` is populated only in Verbatim mode.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<Vec<ResponsePart>>,
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
}

impl PartSummary {
    /// Project a canonical [`PartKind`] into its compact response descriptor, or
    /// `None` for a kind that does not earn a summary. Exhaustive on purpose - a
    /// new `PartKind` variant must decide here. `call_id` is carried for
    /// `tool_call` / `tool_result` only.
    ///
    /// `text` and `reasoning` return `None`: a text part's content already rides
    /// the message's `text`/`content` (a summary would duplicate it), and
    /// reasoning is deliberately not surfaced by default (its full body is still
    /// reachable in verbatim mode). The kinds that survive are exactly
    /// [`SUMMARY_PART_TYPES`].
    pub fn for_kind(kind: &PartKind) -> Option<Self> {
        let (label, call_id) = match kind {
            PartKind::Text { .. } | PartKind::Reasoning { .. } => return None,
            PartKind::File {
                media_type,
                file_name,
                ..
            } => (
                Some(file_name.clone().unwrap_or_else(|| media_type.clone())),
                None,
            ),
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
        })
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

/// A `Part` as it rides a `pond_get` response (spec.md#protocol): the canonical
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

/// Mode-specific `pond_get` payload, tagged by `scope` and flattened into
/// `GetResponse` alongside the shared session header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum GetResult {
    Session {
        messages: Vec<MessageView>,
        messages_remaining: usize,
    },
    Message {
        target: MessageView,
        target_parts: Vec<ResponsePart>,
        target_parts_remaining: usize,
        /// Up to `2*context_depth` messages around the target (target excluded).
        siblings: Vec<MessageView>,
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
    // Server normally decides between hybrid and FTS-only from the embedder +
    // embeddings-coverage state (spec.md#search); `mode_override` is the
    // operator-tooling escape hatch consumed by the `scripts/search-benchmarks/`
    // harness. Production callers (MCP, HTTP agents) should leave it `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode_override: Option<SearchModeWire>,
    /// When set, retrieve messages similar to this stored message - pond uses
    /// the message's stored `vector` directly as the query, runs vector-only
    /// kNN, and ignores `query` and the FTS arm. The stored vector was
    /// derived from `search_text` (`spec.md#session-embed-from-canonical`), so the
    /// signal is already filtered of harness-injected parts. Filters and
    /// `limit` still apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub similar_to: Option<String>,
    #[serde(default)]
    pub filters: SearchFilters,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Wire-level retrieval mode override (spec.md#search). Not normally set on
/// the wire - the server decides hybrid vs FTS-only from embedding
/// availability. The variant exists so operator tooling (`pond search --mode`,
/// the embeddings-benchmark harness) can force one arm without an env-var
/// backdoor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchModeWire {
    Fts,
    Vector,
    Hybrid,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SearchFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    // Skip the default 0.0 so an unfiltered cursor/request stays compact.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub min_score: f64,
}

impl SearchFilters {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResponse {
    pub sessions: Vec<SearchSession>,
    pub matched_total: usize,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
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
pub struct SearchCursor {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub similar_to: Option<String>,
    #[serde(default, skip_serializing_if = "SearchFilters::is_default")]
    pub filters: SearchFilters,
    pub offset: usize,
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
    error(
        ErrorCode::StorageUnavailable,
        "storage operation failed",
        serde_json::json!({ "underlying": error_value.to_string() }),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::json;

    #[test]
    fn wire_envelope_carries_conflict_code_and_attempts_detail() {
        let envelope: ErrorEnvelope = crate::Error::Conflict { attempts: 3 }.into();
        assert_eq!(envelope.error.code, ErrorCode::Conflict);
        assert_eq!(envelope.error.details, json!({ "attempts": 3 }));
    }
}
