//! `HtmlRenderer`: `SessionWithMessages` -> a self-contained HTML page.
//!
//! Design ported from agentsview's static exporter
//! (`internal/server/export.go`): CSS custom properties for light values
//! under `:root`, a `:root.dark` override block, and a plain button that
//! toggles a `dark` class on `<html>` - no JS framework, no
//! `prefers-color-scheme` media query, no persistence. It's a single
//! cacheable static file; that's the right amount of theming for one.
//!
//! Unlike agentsview (which drops image content), pond is lossless: image
//! parts render inline as `data:` URIs, size-capped per
//! `[share].max_inline_image_bytes`. See docs/overview/share-feature.md for
//! the full rationale, including why each `FileData` variant is handled
//! differently.
//!
//! No templating engine and no HTML-escaping crate exist in pond's dependency
//! tree, so - matching `render.rs`'s own convention for the plain-text
//! transcript renderer - this writes straight into a `String` via
//! `write!`/`writeln!`, with a small hand-rolled `escape_html`. Text/reasoning
//! bodies are escaped and wrapped in `white-space: pre-wrap`; unlike
//! agentsview's regex-based code-fence/inline-code formatting (which exists
//! because it works over already-flattened text), pond doesn't add a `regex`
//! dependency just to re-detect Markdown fences pond's own model doesn't
//! distinguish from prose.

use std::fmt::Write as _;

use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};

use crate::{
    adapter::Extracted,
    sessions::{MessageWithParts, SessionWithMessages},
    share::{ShareArtifact, ShareRenderer},
    wire::{FileData, Message, PartKind, Role},
};

/// Fallback cap when `[share].max_inline_image_bytes` is unset.
pub const DEFAULT_MAX_INLINE_IMAGE_BYTES: usize = 2 * 1024 * 1024;

pub struct HtmlRenderer {
    max_inline_image_bytes: usize,
}

impl HtmlRenderer {
    pub fn new() -> Self {
        Self::with_max_inline_image_bytes(DEFAULT_MAX_INLINE_IMAGE_BYTES)
    }

    pub fn with_max_inline_image_bytes(max_inline_image_bytes: usize) -> Self {
        Self {
            max_inline_image_bytes,
        }
    }
}

impl Default for HtmlRenderer {
    fn default() -> Self {
        Self::new()
    }
}

impl ShareRenderer for HtmlRenderer {
    fn render(&self, session: &SessionWithMessages) -> Result<ShareArtifact> {
        let html = render_page(session, self.max_inline_image_bytes);
        Ok(ShareArtifact {
            bytes: html.into_bytes(),
            content_type: "text/html; charset=utf-8".to_owned(),
            ext: "html".to_owned(),
        })
    }
}

fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn fmt_ts(ts: &chrono::DateTime<chrono::Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%SZ").to_string()
}

fn extracted_str(value: &Option<Extracted<String>>) -> Option<&str> {
    value.as_deref().map(String::as_str)
}

/// Mirrors `render.rs`'s `value_to_text`: a JSON string shows as its text;
/// anything else as compact JSON; `null` shows nothing.
fn value_to_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn role_class(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

const STYLE: &str = r#"
:root {
  --bg-primary: #f6f7fb; --bg-surface: #ffffff; --bg-inset: #eef0f6;
  --border: #e0e3ec; --text-primary: #1b1e27; --text-secondary: #565c6d; --text-muted: #8890a2;
  --accent: #3457d5; --user-bg: #eef1ff; --assistant-bg: #f8f8fc; --thinking-bg: #f4f0fe;
  --tool-bg: #fff8ec; --tool-fail-bg: #fdecec; --code-bg: #1e1e2a; --code-text: #d8dced;
  --radius: 6px;
  --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  --font-mono: ui-monospace, "SF Mono", "Fira Code", Menlo, Consolas, monospace;
  color-scheme: light;
}
:root.dark {
  --bg-primary: #14151b; --bg-surface: #1b1d26; --bg-inset: #23252f;
  --border: #333644; --text-primary: #e8e9f0; --text-secondary: #aab0c2; --text-muted: #767c8f;
  --accent: #7c93ff; --user-bg: #1e2440; --assistant-bg: #1f2029; --thinking-bg: #241f38;
  --tool-bg: #2b2416; --tool-fail-bg: #3a1f1f; --code-bg: #0f1016; --code-text: #d8dced;
  color-scheme: dark;
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 2rem 1rem 4rem; background: var(--bg-primary); color: var(--text-primary);
  font-family: var(--font-sans); line-height: 1.5;
}
main { max-width: 860px; margin: 0 auto; }
header { max-width: 860px; margin: 0 auto 2rem; display: flex; align-items: baseline; justify-content: space-between; gap: 1rem; }
header h1 { font-size: 1.1rem; margin: 0 0 0.25rem; }
.meta { color: var(--text-muted); font-size: 0.85rem; }
.theme-btn {
  border: 1px solid var(--border); background: var(--bg-surface); color: var(--text-secondary);
  border-radius: var(--radius); padding: 0.3rem 0.7rem; font-size: 0.8rem; cursor: pointer;
}
.message {
  background: var(--bg-surface); border: 1px solid var(--border); border-radius: var(--radius);
  padding: 0.9rem 1.1rem; margin-bottom: 0.9rem;
}
.message.user { background: var(--user-bg); }
.message.assistant { background: var(--assistant-bg); }
.message-header {
  display: flex; gap: 0.6rem; align-items: baseline; color: var(--text-muted);
  font-size: 0.78rem; margin-bottom: 0.5rem; font-family: var(--font-mono);
}
.message-role { color: var(--text-secondary); font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
.text-block { white-space: pre-wrap; word-wrap: break-word; }
.thinking-block {
  background: var(--thinking-bg); border-radius: var(--radius); padding: 0.6rem 0.8rem;
  margin: 0.4rem 0; font-size: 0.9rem; color: var(--text-secondary);
}
.thinking-label { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-muted); margin-bottom: 0.3rem; }
.tool-block {
  background: var(--tool-bg); border-radius: var(--radius); padding: 0.6rem 0.8rem;
  margin: 0.4rem 0; font-family: var(--font-mono); font-size: 0.82rem; white-space: pre-wrap; word-wrap: break-word;
}
.tool-block.failed { background: var(--tool-fail-bg); }
.tool-block-header { font-weight: 600; margin-bottom: 0.3rem; }
.file-block, .approval-block {
  background: var(--bg-inset); border-radius: var(--radius); padding: 0.5rem 0.8rem;
  margin: 0.4rem 0; font-size: 0.82rem; color: var(--text-secondary);
}
.message img { max-width: 100%; border-radius: var(--radius); margin: 0.4rem 0; display: block; }
code { font-family: var(--font-mono); }
footer { max-width: 860px; margin: 2rem auto 0; color: var(--text-muted); font-size: 0.78rem; text-align: center; }
"#;

fn render_page(session: &SessionWithMessages, max_inline_image_bytes: usize) -> String {
    let mut out = String::new();
    let project = escape_html(session.session.project.as_str());
    let _ = write!(
        out,
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{project} - pond share</title>\n<style>{STYLE}</style>\n</head>\n<body>",
    );
    let _ = writeln!(
        out,
        "<header>\n<div>\n<h1>{project}</h1>\n\
         <div class=\"meta\">session {} &middot; {} &middot; {} &middot; started {}</div>\n</div>\n\
         <button class=\"theme-btn\" onclick=\"document.documentElement.classList.toggle('dark')\">Dark</button>\n</header>",
        escape_html(&session.session.id),
        escape_html(&session.session.source_agent),
        plural(session.messages.len(), "message"),
        fmt_ts(&session.session.created_at),
    );
    out.push_str("<main>\n");
    for message in &session.messages {
        render_message(&mut out, message, max_inline_image_bytes);
    }
    out.push_str("</main>\n");
    out.push_str(
        "<footer>Published with <code>pond share</code> - full transcript, no redaction applied.</footer>\n",
    );
    out.push_str("</body>\n</html>\n");
    out
}

fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

fn message_role(message: &Message) -> Role {
    message.role()
}

fn render_message(out: &mut String, message: &MessageWithParts, max_inline_image_bytes: usize) {
    let role = message_role(&message.message);
    let _ = writeln!(
        out,
        "<div class=\"message {}\">\n<div class=\"message-header\">\
         <span class=\"message-role\">{}</span><span>{}</span><span>{}</span></div>",
        role_class(role),
        escape_html(role.as_str()),
        fmt_ts(&message.message.timestamp()),
        escape_html(message.message.id()),
    );
    if let Some(content) = message.message.system_content()
        && !content.trim().is_empty()
    {
        let _ = writeln!(out, "<div class=\"text-block\">{}</div>", escape_html(content));
    }
    for part in &message.parts {
        render_part(out, &part.kind, max_inline_image_bytes);
    }
    out.push_str("</div>\n");
}

fn render_part(out: &mut String, kind: &PartKind, max_inline_image_bytes: usize) {
    match kind {
        PartKind::Text { text } => {
            if let Some(text) = extracted_str(text)
                && !text.trim().is_empty()
            {
                let _ = writeln!(out, "<div class=\"text-block\">{}</div>", escape_html(text));
            }
        }
        PartKind::Reasoning { text } => {
            if let Some(text) = extracted_str(text)
                && !text.trim().is_empty()
            {
                let _ = writeln!(
                    out,
                    "<div class=\"thinking-block\"><div class=\"thinking-label\">Thinking</div>\
                     <div class=\"text-block\">{}</div></div>",
                    escape_html(text),
                );
            }
        }
        PartKind::ToolCall {
            name,
            call_id,
            params,
            ..
        } => {
            let name = extracted_str(name).unwrap_or("?");
            let call_id = extracted_str(call_id).unwrap_or("?");
            let body = value_to_text(params);
            let _ = writeln!(
                out,
                "<div class=\"tool-block\"><div class=\"tool-block-header\">&rarr; {} [{}]</div>{}</div>",
                escape_html(name),
                escape_html(call_id),
                escape_html(&body),
            );
        }
        PartKind::ToolResult {
            name,
            call_id,
            is_failure,
            result,
        } => {
            let name = extracted_str(name).unwrap_or("?");
            let call_id = extracted_str(call_id).unwrap_or("?");
            let status = if *is_failure { "failed" } else { "ok" };
            let class = if *is_failure { "tool-block failed" } else { "tool-block" };
            let body = value_to_text(result);
            let _ = writeln!(
                out,
                "<div class=\"{class}\"><div class=\"tool-block-header\">&larr; {} [{}] ({status})</div>{}</div>",
                escape_html(name),
                escape_html(call_id),
                escape_html(&body),
            );
        }
        PartKind::File {
            media_type,
            file_name,
            data,
        } => render_file_part(out, media_type.as_deref(), file_name.as_deref(), data, max_inline_image_bytes),
        PartKind::ToolApprovalRequest { approval_id, .. } => {
            let _ = writeln!(
                out,
                "<div class=\"approval-block\">[approval request {}]</div>",
                escape_html(approval_id),
            );
        }
        PartKind::ToolApprovalResponse {
            approval_id,
            approved,
            ..
        } => {
            let verb = if *approved { "approved" } else { "denied" };
            let _ = writeln!(
                out,
                "<div class=\"approval-block\">[approval {} {verb}]</div>",
                escape_html(approval_id),
            );
        }
    }
}

/// Approximate decoded byte size without decoding: base64 expands 4 chars per
/// 3 source bytes.
fn base64_decoded_len(encoded: &str) -> usize {
    encoded.len() * 3 / 4
}

fn render_file_part(
    out: &mut String,
    media_type: Option<&str>,
    file_name: Option<&str>,
    data: &FileData,
    max_inline_image_bytes: usize,
) {
    let label = file_name.or(media_type).unwrap_or("file");
    match media_type {
        Some(media_type) if media_type.starts_with("image/") => {
            render_image_part(out, media_type, label, data, max_inline_image_bytes);
        }
        Some(media_type) if media_type.starts_with("audio/") => {
            // Reserved, not implemented: blocked on a real voice-session
            // fixture (docs/plans/2607-09-share-sessions-public-link.md#9).
            // Same File match arm as images, so wiring in real <audio>
            // rendering later needs no restructuring.
            let _ = writeln!(
                out,
                "<div class=\"file-block\">[audio {} - not yet rendered]</div>",
                escape_html(label),
            );
        }
        _ => {
            let _ = writeln!(out, "<div class=\"file-block\">[file {}]</div>", escape_html(label));
        }
    }
}

fn render_image_part(
    out: &mut String,
    media_type: &str,
    label: &str,
    data: &FileData,
    max_inline_image_bytes: usize,
) {
    match data {
        FileData::String(base64_text) => {
            if base64_decoded_len(base64_text) > max_inline_image_bytes {
                render_oversized_image_placeholder(out, label, base64_decoded_len(base64_text));
                return;
            }
            let _ = writeln!(
                out,
                "<img src=\"data:{};base64,{}\" alt=\"{}\">",
                escape_html(media_type),
                base64_text,
                escape_html(label),
            );
        }
        FileData::Bytes(bytes) => {
            if bytes.len() > max_inline_image_bytes {
                render_oversized_image_placeholder(out, label, bytes.len());
                return;
            }
            let _ = writeln!(
                out,
                "<img src=\"data:{};base64,{}\" alt=\"{}\">",
                escape_html(media_type),
                STANDARD.encode(bytes),
                escape_html(label),
            );
        }
        FileData::Url(url) => {
            // External reference: can't be inlined without pond fetching it
            // itself (out of scope for a renderer - no network calls). Zero
            // *pond-initiated* fetches, but a viewer's browser will hit this
            // URL - a documented exception to "fully self-contained".
            let _ = writeln!(
                out,
                "<img src=\"{}\" alt=\"{}\">",
                escape_html(url),
                escape_html(label),
            );
        }
    }
}

fn render_oversized_image_placeholder(out: &mut String, label: &str, approx_bytes: usize) {
    let _ = writeln!(
        out,
        "<div class=\"file-block\">[image {} omitted - {} exceeds the inline size cap]</div>",
        escape_html(label),
        approx_bytes,
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::sessions::{MessageWithParts, SessionWithMessages};
    use crate::wire::{Part, Session};

    fn ts() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(0, 0).unwrap()
    }

    fn part(kind: serde_json::Value) -> Part {
        let mut base = serde_json::json!({
            "session_id": "s1",
            "id": "p1",
            "message_id": "m1",
            "ordinal": 0,
            "provenance": "conversational",
        });
        base.as_object_mut()
            .unwrap()
            .extend(kind.as_object().unwrap().clone());
        serde_json::from_value(base).unwrap()
    }

    fn session_with_parts(parts: Vec<Part>) -> SessionWithMessages {
        let session: Session = serde_json::from_value(serde_json::json!({
            "id": "s1",
            "source_agent": "claude-code",
            "created_at": ts(),
            "project": "/my/project",
        }))
        .unwrap();
        let message: Message = serde_json::from_value(serde_json::json!({
            "role": "assistant",
            "id": "m1",
            "session_id": "s1",
            "timestamp": ts(),
        }))
        .unwrap();
        SessionWithMessages {
            session,
            messages: vec![MessageWithParts { message, parts }],
        }
    }

    #[test]
    fn escape_html_escapes_special_characters() {
        assert_eq!(
            escape_html("<script>&\"'</script>"),
            "&lt;script&gt;&amp;&quot;&#39;&lt;/script&gt;",
        );
    }

    #[test]
    fn renders_page_shell_and_text_and_tool_parts() {
        let parts = vec![
            part(serde_json::json!({"type": "text", "text": "Let me <check> that."})),
            part(serde_json::json!({
                "type": "tool_call", "name": "Bash", "call_id": "toolu_x",
                "params": {"command": "ls"}, "provider_executed": false,
            })),
            part(serde_json::json!({
                "type": "tool_result", "name": "Bash", "call_id": "toolu_x",
                "is_failure": false, "result": "file.txt",
            })),
        ];
        let renderer = HtmlRenderer::new();
        let artifact = renderer.render(&session_with_parts(parts)).unwrap();
        let html = String::from_utf8(artifact.bytes).unwrap();

        assert_eq!(artifact.content_type, "text/html; charset=utf-8");
        assert_eq!(artifact.ext, "html");
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("classList.toggle('dark')"));
        assert!(html.contains(":root.dark"));
        assert!(html.contains("Let me &lt;check&gt; that."));
        assert!(html.contains("&rarr; Bash [toolu_x]"));
        assert!(html.contains("&larr; Bash [toolu_x] (ok)"));
        assert!(html.contains("session s1"));
    }

    #[test]
    fn reasoning_part_renders_thinking_block() {
        let parts = vec![part(
            serde_json::json!({"type": "reasoning", "text": "thinking it through"}),
        )];
        let renderer = HtmlRenderer::new();
        let html = String::from_utf8(renderer.render(&session_with_parts(parts)).unwrap().bytes).unwrap();
        assert!(html.contains("thinking-block"));
        assert!(html.contains("thinking it through"));
    }

    #[test]
    fn image_string_variant_is_already_base64_and_renders_inline() {
        let parts = vec![part(serde_json::json!({
            "type": "file", "media_type": "image/png", "file_name": "a.png",
            "data": {"kind": "string", "value": "aGVsbG8="},
        }))];
        let renderer = HtmlRenderer::new();
        let html = String::from_utf8(renderer.render(&session_with_parts(parts)).unwrap().bytes).unwrap();
        assert!(html.contains("<img src=\"data:image/png;base64,aGVsbG8=\""));
    }

    #[test]
    fn image_bytes_variant_is_encoded_to_base64() {
        let parts = vec![part(serde_json::json!({
            "type": "file", "media_type": "image/png", "file_name": "a.png",
            "data": {"kind": "bytes", "value": [104, 101, 108, 108, 111]},
        }))];
        let renderer = HtmlRenderer::new();
        let html = String::from_utf8(renderer.render(&session_with_parts(parts)).unwrap().bytes).unwrap();
        let expected = STANDARD.encode(b"hello");
        assert!(html.contains(&format!("<img src=\"data:image/png;base64,{expected}\"")));
    }

    #[test]
    fn image_url_variant_renders_direct_src_not_inlined() {
        let parts = vec![part(serde_json::json!({
            "type": "file", "media_type": "image/png", "file_name": "a.png",
            "data": {"kind": "url", "value": "https://example.com/a.png"},
        }))];
        let renderer = HtmlRenderer::new();
        let html = String::from_utf8(renderer.render(&session_with_parts(parts)).unwrap().bytes).unwrap();
        assert!(html.contains("<img src=\"https://example.com/a.png\""));
        assert!(!html.contains("data:image"));
    }

    #[test]
    fn oversized_image_falls_back_to_placeholder() {
        let big = STANDARD.encode(vec![0u8; 100]);
        let parts = vec![part(serde_json::json!({
            "type": "file", "media_type": "image/png", "file_name": "big.png",
            "data": {"kind": "string", "value": big},
        }))];
        let renderer = HtmlRenderer::with_max_inline_image_bytes(10);
        let html = String::from_utf8(renderer.render(&session_with_parts(parts)).unwrap().bytes).unwrap();
        assert!(!html.contains("<img"));
        assert!(html.contains("big.png"));
        assert!(html.contains("omitted"));
    }

    #[test]
    fn audio_part_renders_reserved_placeholder_not_a_crash() {
        let parts = vec![part(serde_json::json!({
            "type": "file", "media_type": "audio/mpeg", "file_name": "clip.mp3",
            "data": {"kind": "url", "value": "https://example.com/clip.mp3"},
        }))];
        let renderer = HtmlRenderer::new();
        let html = String::from_utf8(renderer.render(&session_with_parts(parts)).unwrap().bytes).unwrap();
        assert!(html.contains("clip.mp3"));
        assert!(html.contains("not yet rendered"));
        assert!(!html.contains("<audio"));
    }

    #[test]
    fn generic_file_part_renders_label() {
        let parts = vec![part(serde_json::json!({
            "type": "file", "media_type": "application/pdf", "file_name": "doc.pdf",
            "data": {"kind": "url", "value": "https://example.com/doc.pdf"},
        }))];
        let renderer = HtmlRenderer::new();
        let html = String::from_utf8(renderer.render(&session_with_parts(parts)).unwrap().bytes).unwrap();
        assert!(html.contains("[file doc.pdf]"));
    }
}
