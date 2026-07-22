//! Canonical text-transcript rendering for `pond_search` / `pond_get_session` /
//! `pond_get_message`
//! responses, shared by the MCP transport and the `pond` CLI so both surfaces
//! emit one identical readable format (spec.md#protocol). The structured
//! HTTP/JSON path renders nothing here; this is the plain-text view.

use crate::handlers::default_excludes_subagents;
use crate::wire::{
    GetResponse, GetResult, MessageView, PartKind, PartSummary, ResponsePart, SearchModeWire,
    SearchRequest, SearchResponse, SortBy,
};

/// Which surface a transcript renders for. The format is identical; only the
/// follow-up vocabulary differs - the MCP tools are `pond_get_session` /
/// `pond_get_message` / `pond_sql` with `key=value` args, the CLI verbs are
/// `pond get-session <ID>` / `pond get-message <ID>` / `pond sql`. Without
/// this a human at the terminal is told to run tool syntax their shell
/// rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    Mcp,
    Cli,
}

/// `1 message` / `2 messages`: a count with a correctly pluralized noun.
fn count_noun(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// Footer for a `pond_get_session` response listing the session's spawn-only
/// subagents. Each subagent is its own session (spec.md#datasets) addressable
/// by the printed id, so the caller can open any with `pond_get_session`;
/// without this they are invisible from the MCP surface.
pub fn render_subagents_footer(children: &[crate::wire::Session], surface: Surface) -> String {
    use std::fmt::Write;
    let how = match surface {
        Surface::Mcp => "pass an id to pond_get_session(id=...)",
        Surface::Cli => "pass an id to `pond get-session <ID>`",
    };
    let mut out = String::new();
    let _ = writeln!(out);
    let _ = writeln!(out, "subagents ({}) - {how}:", children.len());
    for child in children {
        let _ = writeln!(out, "  {} | {}", child.id, child.source_agent);
    }
    out
}

/// `YYYY-MM-DD HH:MM:SSZ` - compact, sortable, timezone-explicit.
fn fmt_ts(ts: &chrono::DateTime<chrono::Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%SZ").to_string()
}

/// Inner string of an `Extracted<String>` option, or `?` when the source
/// carried none (spec.md#model-no-synthesis: absence is real, not a blank).
fn opt_name(value: &Option<crate::adapter::extract::Extracted<String>>) -> &str {
    value.as_deref().map(String::as_str).unwrap_or("?")
}

/// Append each line of `body` to `out`, so escaped `\n` in stored text
/// renders as real line breaks. A trailing blank line in the source is
/// dropped (lines() already does this).
fn push_lines(out: &mut String, body: &str, indent: &str) {
    use std::fmt::Write;
    for line in body.lines() {
        let _ = writeln!(out, "{indent}{line}");
    }
}

/// Char ceiling for a rendered `pond_search` transcript (spec.md#search).
/// Enforced as per-session fair-share truncation that always renders every
/// returned session's top hit - never a whole-response guillotine. The
/// structured response (HTTP) is unaffected; this bounds only the agent
/// transcript. Soft: a single session's header + one hit may nudge past it.
const SEARCH_TRANSCRIPT_BUDGET: usize = 10_000;

pub fn render_search_transcript(
    response: &SearchResponse,
    request: &SearchRequest,
    surface: Surface,
) -> String {
    use std::fmt::Write;
    let prefix = match surface {
        Surface::Mcp => "pond_search",
        Surface::Cli => "pond search",
    };
    let subagent_note = match (default_excludes_subagents(&request.filters), surface) {
        (false, _) => "",
        (true, Surface::Mcp) => {
            " Subagent sessions excluded; reach them via pond_sql (parent_session_id)."
        }
        (true, Surface::Cli) => {
            " Subagent sessions excluded; reach them via `pond sql` (parent_session_id)."
        }
    };
    let recency_note = if matches!(request.sort_by, SortBy::Recency) {
        " Sorted by recency (newest first) - rank is NOT match strength."
    } else {
        ""
    };
    if response.sessions.is_empty() {
        // spec.md#search-absence-honesty: name the scope size and the
        // recovery path - a zero-hit response must distinguish "nothing
        // relevant exists" from "the filters excluded everything" from "the
        // store simply has nothing stored yet".
        if response.searchable_in_scope == 0 {
            let scoped = request.filters.project.is_some()
                || request.filters.session_id.is_some()
                || request.filters.from_date.is_some()
                || request.filters.to_date.is_some();
            if scoped {
                return format!(
                    "{prefix}: 0 searchable messages in scope - the filters exclude \
                     everything before retrieval. Widen or drop project/date filters.\
                     {subagent_note}\n"
                );
            }
            // No filters were set: the corpus itself is empty, so pointing
            // at filters would send the user chasing settings they never made.
            return match surface {
                Surface::Cli => format!(
                    "{prefix}: no sessions stored yet - run `pond init` to set up \
                     adapters, then `pond sync` to import your history.\n"
                ),
                Surface::Mcp => format!(
                    "{prefix}: the store has no searchable messages yet (nothing \
                     ingested so far). Not an absence signal about the topic.\n"
                ),
            };
        }
        let fts_hint = match surface {
            Surface::Mcp => {
                " For exact strings or identifiers, try pond_sql: SELECT \
                 message_id, session_id, search_text FROM messages WHERE \
                 contains_tokens(search_text, '...')."
            }
            Surface::Cli => {
                " For exact strings or identifiers, try: pond sql \"SELECT \
                 message_id, session_id, search_text FROM messages WHERE \
                 contains_tokens(search_text, '...')\"."
            }
        };
        return format!(
            "{prefix}: no matches for {:?} across {} in \
             scope.{subagent_note}{fts_hint}\n",
            request.query,
            count_noun(response.searchable_in_scope, "searchable message"),
        );
    }
    let shown: usize = response.sessions.iter().map(|s| s.matches.len()).sum();
    // Vector mode ranks by similarity and ALWAYS returns the nearest rows,
    // even when none truly match (cosine bands for present vs absent content
    // overlap, so there is deliberately no score cutoff) - so call them
    // "nearest", not "matching", or a gibberish query looks like confident
    // relevance. fts requires real token overlap, so "matching" is honest there.
    let vector_mode = matches!(request.mode, SearchModeWire::Vector);
    let head_noun = if vector_mode {
        "nearest message"
    } else {
        "matching message"
    };
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{prefix}: {} ({} searchable in scope), showing {} from {}.{}{}",
        count_noun(response.matched_total, head_noun),
        response.searchable_in_scope,
        count_noun(shown, "hit"),
        count_noun(response.sessions.len(), "session"),
        subagent_note,
        recency_note,
    );
    let order = if matches!(request.sort_by, SortBy::Recency) {
        "newest session first"
    } else {
        "ordered by best hit"
    };
    let full_hint = match surface {
        Surface::Mcp => "pond_get_message <message_id> for full, pond_get_session for the session",
        Surface::Cli => "`pond get-message <ID>` for full, `pond get-session <ID>` for the session",
    };
    let mode_note = match (vector_mode, surface) {
        (false, _) => "",
        (true, Surface::Cli) => {
            " Vector mode returns the closest rows by meaning even when none are strong; for exact-word matching use --mode fts."
        }
        (true, Surface::Mcp) => {
            " Vector mode returns the closest rows by meaning even when none are strong; for exact-word matching set mode=\"fts\"."
        }
    };
    let _ = writeln!(
        out,
        "key: session rules group hits by session, {order}; within a session, messages are newest-first. \"--- [n] score | role | time | message_id | project | agent | session ---\" delimits each hit + matched text. {full_hint}; raise limit for more (no pagination).{mode_note}"
    );
    let mut index = 0;
    let n_sessions = response.sessions.len();
    for (session_index, session) in response.sessions.iter().enumerate() {
        // Highest score among the session's matches. Not `matches.first()`:
        // matches render newest-first, so the first need not be the best.
        let best = session
            .matches
            .iter()
            .map(|hit| hit.score)
            .fold(0.0_f64, f64::max);
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "{}",
            rule_line(&format!(
                "session [{}] best {:.2} | {}/{} matched | {} | {} | {}",
                session_index + 1,
                best,
                session.matched_message_count,
                session.session_messages_count,
                session.project,
                session.source_agent,
                session.session_id,
            )),
        );
        // Even share of the remaining budget across the sessions still to
        // render, so all of them surface at least their newest hit (never a
        // whole-response guillotine). Extra hits in a session stop once its
        // share is spent; the first hit always renders.
        let remaining = SEARCH_TRANSCRIPT_BUDGET.saturating_sub(out.len());
        let share = remaining / (n_sessions - session_index);
        let session_start = out.len();
        let mut rendered = 0usize;
        for hit in &session.matches {
            if rendered > 0 && out.len().saturating_sub(session_start) >= share {
                break;
            }
            index += 1;
            let _ = writeln!(out);
            let _ = writeln!(
                out,
                "{}",
                rule_line(&format!(
                    "[{index}] {:.2} | {} | {} | {} | {} | {} | {}",
                    hit.score,
                    hit.role.as_str(),
                    fmt_ts(&hit.timestamp),
                    hit.message_id,
                    session.project,
                    session.source_agent,
                    session.session_id,
                )),
            );
            push_lines(&mut out, &hit.text, "");
            rendered += 1;
        }
        // Intra-session supersession signal (spec.md#search): when the char
        // budget cut this session's matches short, point the agent at the
        // session's latest state, which may revise these older hits.
        let omitted = session.matches.len() - rendered;
        if omitted > 0 {
            let latest_hint = match surface {
                Surface::Mcp => "read with pond_get_session from=end",
                Surface::Cli => "read with `pond get-session <ID> --from end`",
            };
            let _ = writeln!(
                out,
                "... {omitted} more match(es) in this session not shown (char budget); \
                 {latest_hint} for the session's latest state"
            );
        }
    }
    out
}

pub fn render_get_transcript(response: &GetResponse, surface: Surface) -> String {
    use std::fmt::Write;
    let session = &response.session;
    let mut out = String::new();
    match &response.result {
        GetResult::Session {
            messages,
            before_remaining,
            after_remaining,
            resolved_from_message_id,
        } => {
            let prefix = match surface {
                Surface::Mcp => "pond_get_session",
                Surface::Cli => "pond get-session",
            };
            // A partial page must announce the session total up front: a
            // header saying only the page's count reads as the whole session
            // (field-tested miss - the rest of a long session went unread).
            let total = *before_remaining + messages.len() + *after_remaining;
            if total > messages.len() {
                let start = *before_remaining + 1;
                let end = *before_remaining + messages.len();
                let _ = writeln!(
                    out,
                    "{prefix}: session {}, messages {start}-{end} of {total} \
                     (pages are bounded by limit and a size budget).",
                    session.id,
                );
            } else {
                let _ = writeln!(
                    out,
                    "{prefix}: session {}, {}.",
                    session.id,
                    count_noun(messages.len(), "message"),
                );
            }
            // The id was a message id; say so before the transcript so the
            // caller knows the page is anchored, not the session start.
            if let Some(message_id) = resolved_from_message_id {
                let expand = match surface {
                    Surface::Mcp => format!("pond_get_message id={message_id}"),
                    Surface::Cli => format!("`pond get-message {message_id}`"),
                };
                let _ = writeln!(
                    out,
                    "note: {message_id} is a message id - resolved to its session; this page \
                     starts at that message ({expand} for its full part bodies)."
                );
            }
            let (expand_hint, page_hint) = match surface {
                Surface::Mcp => (
                    "pond_get_message id=<id> to expand any tool body",
                    "Page with before_message_id / after_message_id.",
                ),
                Surface::Cli => (
                    "`pond get-message <ID>` to expand any tool body",
                    "Page with --before-message-id / --after-message-id.",
                ),
            };
            let _ = writeln!(
                out,
                "key: \"--- [n] role | time | message_id ---\" delimits each message; \"->\" tool call, \"<-\" result; {expand_hint}. {page_hint}"
            );
            // Top marker: earlier messages precede this page (page up).
            if *before_remaining > 0
                && let Some(first) = messages.first()
            {
                let page_up = match surface {
                    Surface::Mcp => format!("pass before_message_id={}", first.id),
                    Surface::Cli => {
                        format!("pass --before-message-id {}", first.id)
                    }
                };
                let _ = writeln!(
                    out,
                    "... {before_remaining} earlier messages; {page_up} to page up",
                );
            }
            for (idx, message) in messages.iter().enumerate() {
                let _ = writeln!(out);
                render_message(
                    &mut out,
                    idx + 1,
                    message,
                    None,
                    &message.parts_summary,
                    false,
                );
            }
            let _ = writeln!(out);
            let _ = writeln!(
                out,
                "session {} | {} | {}",
                session.id, session.source_agent, session.project,
            );
            // Bottom marker: later messages follow this page (page down). The
            // supersession note guards the field-tested failure where an agent
            // reads an early page of a long session and reports a
            // since-revised conclusion as current.
            if *after_remaining > 0
                && let Some(last) = messages.last()
            {
                let page_down = match surface {
                    Surface::Mcp => format!("pass after_message_id={}", last.id),
                    Surface::Cli => format!("pass --after-message-id {}", last.id),
                };
                let latest = match surface {
                    Surface::Mcp => "from=\"end\"",
                    Surface::Cli => "--from end",
                };
                let _ = writeln!(
                    out,
                    "... {after_remaining} later messages; {page_down} to page down \
                     (conclusions may have been revised - {latest} reads the latest turns)",
                );
            }
        }
        GetResult::Message {
            target,
            target_parts,
            target_parts_remaining,
            siblings,
            context_before,
            context_after,
        } => {
            let prefix = match surface {
                Surface::Mcp => "pond_get_message",
                Surface::Cli => "pond get-message",
            };
            let _ = writeln!(
                out,
                "{prefix}: thread around {} in session {} (context -{}/+{}).",
                target.id, session.id, context_before, context_after,
            );
            let expand_hint = match surface {
                Surface::Mcp => "pond_get_message id=<id> to expand any line",
                Surface::Cli => "`pond get-message <ID>` to expand any line",
            };
            let _ = writeln!(
                out,
                "key: \"--- [n] role | time | message_id ---\" delimits each message; \">\" = the one you requested; \"->\" tool call, \"<-\" result. {expand_hint}."
            );
            // Interleave target with siblings, ordered by (timestamp, id) to
            // match storage - codex writes many messages at the same
            // timestamp, so the id is the real tiebreak (a bare timestamp
            // sort scrambles them). Drop context siblings with nothing to
            // render (carrier turns with no text/content); the requested
            // target always stays, even if empty.
            let mut thread: Vec<(&MessageView, bool)> =
                siblings.iter().map(|view| (view, false)).collect();
            thread.push((target, true));
            thread.sort_by(|a, b| {
                a.0.timestamp
                    .cmp(&b.0.timestamp)
                    .then_with(|| a.0.id.cmp(&b.0.id))
            });
            thread.retain(|(view, is_target)| *is_target || message_has_content(view));
            for (idx, (view, is_target)) in thread.iter().enumerate() {
                let _ = writeln!(out);
                // Only the target carries full parts; siblings render as
                // conversational text + one-line summaries.
                let parts: Option<&[ResponsePart]> = is_target.then_some(target_parts.as_slice());
                render_message(
                    &mut out,
                    idx + 1,
                    view,
                    parts,
                    &view.parts_summary,
                    *is_target,
                );
            }
            let _ = writeln!(out);
            let _ = writeln!(
                out,
                "session {} | {} | {}",
                session.id, session.source_agent, session.project,
            );
            if *target_parts_remaining > 0 {
                let _ = writeln!(
                    out,
                    "... {} more parts of {} omitted (response budget)",
                    target_parts_remaining, target.id,
                );
            }
        }
    }
    out
}

/// Whether a message view has anything to render below its header: real
/// text/content or a one-line part summary. Used to drop empty carrier
/// turns from message-scope context.
fn message_has_content(view: &MessageView) -> bool {
    view.text.as_deref().is_some_and(|t| !t.trim().is_empty())
        || view
            .content
            .as_deref()
            .is_some_and(|c| !c.trim().is_empty())
        || !view.parts_summary.is_empty()
}

/// Target column width for a delimiter-rule header.
const RULE_WIDTH: usize = 72;

/// Wrap `inner` as a delimiter rule: `--- {inner} ----...` padded to
/// [`RULE_WIDTH`] (always at least a 3-dash tail when `inner` is already
/// wide). Used for both search hits and get message headers.
fn rule_line(inner: &str) -> String {
    let head = format!("--- {inner} ");
    let pad = RULE_WIDTH.saturating_sub(head.chars().count()).max(3);
    format!("{head}{}", "-".repeat(pad))
}

/// One message block: an indexed `--- [n] role | time | id ---` delimiter
/// rule (unambiguous even when the body has blank lines or `##` headings),
/// then text/content as real lines, then parts - full bodies when `parts`
/// is present, else one-line summaries.
fn render_message(
    out: &mut String,
    index: usize,
    view: &MessageView,
    parts: Option<&[ResponsePart]>,
    summary: &[PartSummary],
    is_target: bool,
) {
    use std::fmt::Write;
    let marker = if is_target { "> " } else { "" };
    let _ = writeln!(
        out,
        "{}",
        rule_line(&format!(
            "[{index}] {marker}{} | {} | {}",
            view.role.as_str(),
            fmt_ts(&view.timestamp),
            view.id,
        )),
    );
    if let Some(text) = &view.text {
        push_lines(out, text, "");
    }
    if let Some(content) = &view.content {
        push_lines(out, content, "");
    }
    match parts {
        Some(parts) => {
            for part in parts {
                render_part_full(out, part);
            }
        }
        None => {
            for part in summary {
                render_part_summary(out, part);
            }
        }
    }
}

fn render_part_full(out: &mut String, part: &ResponsePart) {
    use std::fmt::Write;
    match &part.kind {
        PartKind::Text { text } => {
            if let Some(text) = text {
                push_lines(out, text, "");
            }
        }
        PartKind::Reasoning { text } => {
            let _ = writeln!(out, "  (reasoning)");
            if let Some(text) = text {
                push_lines(out, text, "  ");
            }
        }
        PartKind::ToolCall {
            name,
            call_id,
            params,
            ..
        } => {
            let _ = writeln!(out, "  -> {} [{}]", opt_name(name), opt_name(call_id));
            push_lines(out, &value_to_text(params), "     ");
        }
        PartKind::ToolResult {
            name,
            call_id,
            is_failure,
            result,
        } => {
            let status = if *is_failure { "failed" } else { "ok" };
            let _ = writeln!(
                out,
                "  <- {} [{}] ({status})",
                opt_name(name),
                opt_name(call_id),
            );
            push_lines(out, &value_to_text(result), "     ");
        }
        PartKind::File {
            media_type,
            file_name,
            ..
        } => {
            let label = file_name
                .as_deref()
                .or(media_type.as_deref())
                .unwrap_or("file");
            let _ = writeln!(out, "  [file {label}]");
        }
        PartKind::ToolApprovalRequest { approval_id, .. } => {
            let _ = writeln!(out, "  [approval request {approval_id}]");
        }
        PartKind::ToolApprovalResponse {
            approval_id,
            approved,
            ..
        } => {
            let verb = if *approved { "approved" } else { "denied" };
            let _ = writeln!(out, "  [approval {approval_id} {verb}]");
        }
    }
}

fn render_part_summary(out: &mut String, summary: &PartSummary) {
    use std::fmt::Write;
    let label = summary.label.as_deref().unwrap_or("");
    let call = summary
        .call_id
        .as_deref()
        .map(|id| format!(" [{id}]"))
        .unwrap_or_default();
    match summary.kind.as_str() {
        "tool_call" => {
            let _ = writeln!(out, "  -> {label}{call}");
        }
        "tool_result" => {
            let _ = writeln!(out, "  <- {label}{call}");
        }
        "file" => {
            let _ = writeln!(out, "  [file {label}]");
        }
        other => {
            let _ = writeln!(out, "  [{other} {label}]");
        }
    }
}

/// Render a tool param/result `Value` for the transcript: a JSON string
/// shows as its text; anything else as compact JSON. `null` shows nothing.
fn value_to_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::wire::{Role, SearchFilters, SearchModeWire, SearchResult};

    #[test]
    fn get_transcript_marks_target_and_renders_tool_parts() {
        let ts = chrono::DateTime::from_timestamp(0, 0).unwrap();
        let tool_call: ResponsePart = serde_json::from_value(serde_json::json!({
            "id": "p1", "ordinal": 0, "provenance": "conversational",
            "type": "tool_call", "name": "Bash", "call_id": "toolu_x",
            "params": { "command": "ls" }, "provider_executed": false,
        }))
        .unwrap();
        let tool_result: ResponsePart = serde_json::from_value(serde_json::json!({
            "id": "p2", "ordinal": 1, "provenance": "conversational",
            "type": "tool_result", "name": "Bash", "call_id": "toolu_x",
            "is_failure": false, "result": "file.txt",
        }))
        .unwrap();
        let target = MessageView {
            id: "m1".to_owned(),
            role: crate::wire::Role::Assistant,
            timestamp: ts,
            text: Some("Let me list files.".to_owned()),
            content: None,
            parts_summary: Vec::new(),
        };
        let response = GetResponse {
            session: crate::wire::GetSession {
                id: "s1".to_owned(),
                source_agent: "claude-code".to_owned(),
                project: "/p".to_owned(),
                created_at: ts,
            },
            result: GetResult::Message {
                target,
                target_parts: vec![tool_call, tool_result],
                target_parts_remaining: 0,
                siblings: Vec::new(),
                context_before: 3,
                context_after: 3,
            },
        };

        let transcript = crate::render::render_get_transcript(&response, Surface::Mcp);
        assert!(transcript.contains("--- [1] > assistant | 1970-01-01 00:00:00Z | m1 ---"));
        assert!(transcript.contains("Let me list files."));
        assert!(transcript.contains("  -> Bash [toolu_x]"));
        assert!(transcript.contains("  <- Bash [toolu_x] (ok)"));
        assert!(transcript.contains("session s1 | claude-code | /p"));
    }

    #[test]
    fn session_header_states_span_and_total_on_partial_pages() {
        let ts = chrono::DateTime::from_timestamp(0, 0).unwrap();
        let message = |id: &str| MessageView {
            id: id.to_owned(),
            role: crate::wire::Role::User,
            timestamp: ts,
            text: Some("hi".to_owned()),
            content: None,
            parts_summary: Vec::new(),
        };
        let session = crate::wire::GetSession {
            id: "s1".to_owned(),
            source_agent: "claude-code".to_owned(),
            project: "/p".to_owned(),
            created_at: ts,
        };
        let partial = GetResponse {
            session: session.clone(),
            result: GetResult::Session {
                messages: vec![message("m1"), message("m2")],
                before_remaining: 100,
                after_remaining: 32,
                resolved_from_message_id: None,
            },
        };
        let transcript = crate::render::render_get_transcript(&partial, Surface::Mcp);
        assert!(transcript.starts_with(
            "pond_get_session: session s1, messages 101-102 of 134 \
             (pages are bounded by limit and a size budget)."
        ));

        let full = GetResponse {
            session,
            result: GetResult::Session {
                messages: vec![message("m1"), message("m2")],
                before_remaining: 0,
                after_remaining: 0,
                resolved_from_message_id: None,
            },
        };
        let transcript = crate::render::render_get_transcript(&full, Surface::Cli);
        assert!(transcript.starts_with("pond get-session: session s1, 2 messages."));
    }

    #[test]
    fn search_transcript_renders_header_and_hits() {
        let response = SearchResponse {
            sessions: vec![crate::wire::SearchSession {
                session_id: "s1".to_owned(),
                project: "pond".to_owned(),
                source_agent: "claude-code".to_owned(),
                session_messages_count: 2,
                matched_message_count: 1,
                matches: vec![SearchResult {
                    message_id: "m1".to_owned(),
                    role: Role::User,
                    timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
                    text: "hello\nworld".to_owned(),
                    score: 1.0,
                    parts_summary: Vec::new(),
                }],
            }],
            matched_total: 1,
            searchable_in_scope: 2,
            has_more: false,
        };
        let request = SearchRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: None,
            query: "hi".to_owned(),
            mode: SearchModeWire::Vector,
            sort_by: SortBy::Relevance,
            filters: SearchFilters::default(),
            limit: 10,
        };

        let transcript = crate::render::render_search_transcript(&response, &request, Surface::Mcp);
        assert!(transcript.starts_with(
            "pond_search: 1 nearest message (2 searchable in scope), showing 1 hit from 1 \
             session."
        ));
        // Vector mode names the closest-rows caveat and points at fts.
        assert!(transcript.contains("Vector mode returns the closest rows"));
        assert!(
            transcript.contains("key: session rules group hits by session, ordered by best hit")
        );
        assert!(
            transcript
                .contains("--- session [1] best 1.00 | 1/2 matched | pond | claude-code | s1")
        );
        // Hit lines stay flat and indexed so callers can still extract
        // message_id from the same delimiter shape.
        assert!(
            transcript.contains(
                "--- [1] 1.00 | user | 1970-01-01 00:00:00Z | m1 | pond | claude-code | s1"
            )
        );
        // Stored "\n" renders as a real line break, not an escape.
        assert!(transcript.contains("hello\nworld"));
    }

    #[test]
    fn search_transcript_budget_keeps_every_session_and_footers_the_truncated_one() {
        let big = "x".repeat(600);
        let hit = |id: usize| SearchResult {
            message_id: format!("m{id}"),
            role: Role::Assistant,
            timestamp: chrono::DateTime::from_timestamp(id as i64, 0).unwrap(),
            text: big.clone(),
            score: 0.9,
            parts_summary: Vec::new(),
        };
        let session = |id: &str, matches: Vec<SearchResult>| crate::wire::SearchSession {
            session_id: id.to_owned(),
            project: "pond".to_owned(),
            source_agent: "claude-code".to_owned(),
            session_messages_count: 100,
            matched_message_count: matches.len(),
            matches,
        };
        // One fat session whose matches alone exceed the budget, plus five
        // more that must each still surface their top hit.
        let mut sessions = vec![session("fat", (0..40).map(hit).collect())];
        for s in 1..=5 {
            sessions.push(session(&format!("s{s}"), vec![hit(s * 1000)]));
        }
        let response = SearchResponse {
            sessions,
            matched_total: 45,
            searchable_in_scope: 200,
            has_more: false,
        };
        let request = SearchRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: None,
            query: "x".to_owned(),
            mode: SearchModeWire::Vector,
            sort_by: SortBy::Relevance,
            filters: SearchFilters::default(),
            limit: 10,
        };
        let transcript = crate::render::render_search_transcript(&response, &request, Surface::Mcp);

        // Bounded near the budget (soft: each session's guaranteed top hit
        // can nudge its share, so allow a per-session overshoot margin).
        assert!(
            transcript.len() < SEARCH_TRANSCRIPT_BUDGET + 3_000,
            "transcript {} exceeds the soft budget",
            transcript.len(),
        );
        // Never a whole-response guillotine: every returned session renders.
        for id in ["fat", "s1", "s2", "s3", "s4", "s5"] {
            assert!(
                transcript.contains(&format!("| {id}\n"))
                    || transcript.contains(&format!("| {id} ")),
                "session {id} did not render",
            );
        }
        // The fat session was cut short -> supersession footer pointing at
        // the session's latest state.
        assert!(transcript.contains("more match(es) in this session not shown (char budget)"));
        assert!(transcript.contains("pond_get_session from=end"));
    }
}
