//! Canonical text-transcript rendering for `pond_search` / `pond_get`
//! responses, shared by the MCP transport and the `pond` CLI so both surfaces
//! emit one identical readable format (spec.md#protocol). The structured
//! HTTP/JSON path renders nothing here; this is the plain-text view.

use crate::handlers::default_excludes_subagents;
use crate::wire::{
    GetRequest, GetResponse, GetResult, MessageView, PartKind, PartSummary, ResponsePart,
    SearchRequest, SearchResponse, SortBy,
};

/// Footer for a `pond_get` session response listing the session's spawn-only
/// subagents. Each subagent is its own session (spec.md#datasets) addressable
/// by the printed id, so the caller can open any with `pond_get(session_id)`;
/// without this they are invisible from the MCP surface.
pub fn render_subagents_footer(children: &[crate::wire::Session]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "subagents ({}) - pass an id to pond_get(session_id=...):",
        children.len()
    );
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

pub fn render_search_transcript(response: &SearchResponse, request: &SearchRequest) -> String {
    use std::fmt::Write;
    let subagent_note = if default_excludes_subagents(&request.filters) {
        " Subagent sessions excluded; reach them via pond_sql_query (parent_session_id)."
    } else {
        ""
    };
    let recency_note = if matches!(request.sort_by, SortBy::Recency) {
        " Sorted by recency (newest first) - rank is NOT match strength."
    } else {
        ""
    };
    if response.sessions.is_empty() {
        // spec.md#search-absence-honesty: name the scope size and the
        // recovery path - a zero-hit response must distinguish "nothing
        // relevant exists" from "the filters excluded everything".
        if response.searchable_in_scope == 0 {
            return format!(
                "pond_search: 0 searchable messages in scope - the filters exclude \
                 everything before retrieval. Widen or drop project/date filters.\
                 {subagent_note}\n"
            );
        }
        let fts_hint = " For exact strings or identifiers, try pond_sql_query: SELECT \
                        message_id, session_id, search_text FROM messages WHERE \
                        contains_tokens(search_text, '...').";
        return format!(
            "pond_search: no matches for {:?} across {} searchable messages in \
             scope.{subagent_note}{fts_hint}\n",
            request.query, response.searchable_in_scope
        );
    }
    let shown: usize = response.sessions.iter().map(|s| s.matches.len()).sum();
    let mut out = String::new();
    let _ = writeln!(
        out,
        "pond_search: {} matching messages ({} searchable in scope), showing {} hits from {} \
         sessions.{}{}",
        response.matched_total,
        response.searchable_in_scope,
        shown,
        response.sessions.len(),
        subagent_note,
        recency_note,
    );
    let order = if matches!(request.sort_by, SortBy::Recency) {
        "newest session first"
    } else {
        "ordered by best hit"
    };
    let _ = writeln!(
        out,
        "key: session rules group hits by session, {order}; within a session, messages are newest-first. \"--- [n] score | role | time | message_id | project | agent | session ---\" delimits each hit + matched text. pond_get <message_id> for full; raise limit for more (no pagination)."
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
            let _ = writeln!(
                out,
                "... {omitted} more match(es) in this session not shown (char budget); \
                 read with session_from=end for the session's latest state"
            );
        }
    }
    out
}

pub fn render_get_transcript(response: &GetResponse, request: &GetRequest) -> String {
    use std::fmt::Write;
    let session = &response.session;
    let mut out = String::new();
    match &response.result {
        GetResult::Session {
            messages,
            before_remaining,
            after_remaining,
        } => {
            let _ = writeln!(
                out,
                "pond_get: session {}, {} messages.",
                session.id,
                messages.len(),
            );
            let _ = writeln!(
                out,
                "key: \"--- [n] role | time | message_id ---\" delimits each message; \"->\" tool call, \"<-\" result; pond_get message_id=<id> to expand any tool body. Page with session_before_message_id / session_after_message_id."
            );
            // Top marker: earlier messages precede this page (page up).
            if *before_remaining > 0
                && let Some(first) = messages.first()
            {
                let _ = writeln!(
                    out,
                    "... {before_remaining} earlier messages; pass session_before_message_id={} to page up",
                    first.id,
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
            // Bottom marker: later messages follow this page (page down).
            if *after_remaining > 0
                && let Some(last) = messages.last()
            {
                let _ = writeln!(
                    out,
                    "... {after_remaining} later messages; pass session_after_message_id={} to page down",
                    last.id,
                );
            }
        }
        GetResult::Message {
            target,
            target_parts,
            target_parts_remaining,
            siblings,
        } => {
            let _ = writeln!(
                out,
                "pond_get: thread around {} in session {} (context -{}/+{}).",
                target.id,
                session.id,
                request.message_context_before,
                request.message_context_after,
            );
            let _ = writeln!(
                out,
                "key: \"--- [n] role | time | message_id ---\" delimits each message; \">\" = the one you requested; \"->\" tool call, \"<-\" result. pond_get message_id=<id> to expand any line."
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
    use crate::wire::{Role, SearchFilters, SearchModeWire, SearchResult, SessionFrom};

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
            },
        };
        let request = GetRequest {
            protocol_version: crate::PROTOCOL_VERSION,
            namespace: None,
            session_id: None,
            message_id: Some("m1".to_owned()),
            session_limit: 20,
            session_from: SessionFrom::default(),
            session_after_message_id: None,
            session_before_message_id: None,
            message_context_before: 3,
            message_context_after: 3,
        };

        let transcript = crate::render::render_get_transcript(&response, &request);
        assert!(transcript.contains("--- [1] > assistant | 1970-01-01 00:00:00Z | m1 ---"));
        assert!(transcript.contains("Let me list files."));
        assert!(transcript.contains("  -> Bash [toolu_x]"));
        assert!(transcript.contains("  <- Bash [toolu_x] (ok)"));
        assert!(transcript.contains("session s1 | claude-code | /p"));
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

        let transcript = crate::render::render_search_transcript(&response, &request);
        assert!(transcript.starts_with(
            "pond_search: 1 matching messages (2 searchable in scope), showing 1 hits from 1 \
             sessions."
        ));
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
        let transcript = crate::render::render_search_transcript(&response, &request);

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
        assert!(transcript.contains("session_from=end"));
    }
}
