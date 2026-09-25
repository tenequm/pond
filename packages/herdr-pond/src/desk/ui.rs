//! Rendering, plus the text hygiene every transcript string passes through
//! before it reaches a buffer.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Clear, HighlightSpacing, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Wrap,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::app::{App, Lane};
use crate::types::{LISTING_WINDOW_DAYS, SearchSession, SessionRow, TranscriptMessage};

const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const TAB_STOP: usize = 4;
const MACHINE: usize = 10;
const ADAPTER: usize = 12;
const AGE: usize = 4;
const COUNT: usize = 7;
/// Below this body width the preview stacks under the list instead of beside it.
const SIDE_BY_SIDE_MIN_WIDTH: u16 = 100;
const TOAST_MAX_WIDTH: u16 = 60;
/// The preview wraps on every frame, so one huge message must not reach it whole.
pub(super) const PREVIEW_CHARS: usize = 2000;
pub(super) const NO_TITLE: &str = "(no user message)";
pub(super) const PAGER_FOOTER: &str = "conversation only - tool bodies via pond_sql/get_session";

pub(super) struct DeskAreas {
    pub(super) header: Rect,
    pub(super) input: Rect,
    pub(super) columns: Rect,
    pub(super) list: Rect,
    pub(super) preview: Option<Rect>,
    pub(super) footer: Rect,
}

pub(super) fn desk_areas(area: Rect, preview: bool) -> DeskAreas {
    let [header, input, columns, body, footer] = area.layout(&Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ]));
    let (list, preview) = if !preview {
        (body, None)
    } else if body.width >= SIDE_BY_SIDE_MIN_WIDTH {
        let [list, preview] = body.layout(&Layout::horizontal([Constraint::Fill(1); 2]));
        (list, Some(preview))
    } else {
        let [list, preview] = body.layout(&Layout::vertical([Constraint::Fill(1); 2]));
        (list, Some(preview))
    };
    DeskAreas {
        header,
        input,
        columns: Rect {
            width: list.width,
            ..columns
        },
        list,
        preview,
        footer,
    }
}

pub(super) struct PagerAreas {
    pub(super) header: Rect,
    pub(super) text: Rect,
    pub(super) bar: Rect,
    pub(super) footer: Rect,
}

pub(super) fn pager_areas(area: Rect) -> PagerAreas {
    let [header, body, footer] = area.layout(&Layout::vertical([
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ]));
    let [text, bar] = body.layout(&Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(1),
    ]));
    PagerAreas {
        header,
        text,
        bar,
        footer,
    }
}

pub(super) fn render(frame: &mut Frame, app: &mut App) {
    app.relayout();
    if let Some(message) = &app.fatal {
        render_fatal(frame, message);
    } else if app.pager.is_some() {
        render_pager(frame, app);
    } else {
        render_desk(frame, app);
    }
    if let Some(toast) = &app.toast {
        render_toast(frame, toast);
    }
}

fn render_fatal(frame: &mut Frame, message: &str) {
    let text = vec![
        Line::from("pond desk cannot load sessions").bold(),
        Line::default(),
        Line::from(message.to_owned()),
        Line::default(),
        Line::from("r retry  q quit").dim(),
    ];
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(Block::bordered().title(" pond desk ")),
        frame.area(),
    );
}

fn render_desk(frame: &mut Frame, app: &mut App) {
    let areas = desk_areas(frame.area(), app.preview_open);
    frame.render_widget(Paragraph::new(header(app)), areas.header);
    render_input(frame, app, areas.input);
    frame.render_widget(Paragraph::new(column_header()).dim(), areas.columns);
    render_rows(frame, app, areas.list);
    if let Some(preview) = areas.preview {
        render_preview(frame, app, preview);
    }
    frame.render_widget(Paragraph::new(footer(app)), areas.footer);
}

fn window_label(app: &App) -> String {
    if app.all_time {
        "all time".to_owned()
    } else {
        format!("last {LISTING_WINDOW_DAYS} days")
    }
}

fn header(app: &App) -> Line<'static> {
    let scope = app.scope();
    let count = match &app.search {
        Some(search) => search.response.as_ref().map_or_else(
            || "searching".to_owned(),
            |r| format!("{} sessions match", r.sessions.len()),
        ),
        None => app.listing().map_or_else(
            || "loading".to_owned(),
            |rows| format!("{} sessions", rows.len()),
        ),
    };
    Line::from(vec![
        "pond desk".bold(),
        Span::raw(format!(
            " | {count} | {} | {} | msgs = whole-session counts",
            scope.project.as_deref().unwrap_or("all projects"),
            window_label(app)
        )),
    ])
}

fn render_input(frame: &mut Frame, app: &App, area: Rect) {
    let [prompt, field] = area.layout(&Layout::horizontal([
        Constraint::Length(2),
        Constraint::Fill(1),
    ]));
    frame.render_widget(Paragraph::new("/ ".bold()), prompt);
    if !app.typing && app.input.text.is_empty() {
        frame.render_widget(
            Paragraph::new("press / to search message content".dim()),
            field,
        );
        return;
    }
    let column = app.input.cursor_column();
    let visible = usize::from(field.width).saturating_sub(1);
    let skip = column.saturating_sub(visible);
    frame.render_widget(
        Paragraph::new(app.input.text.clone()).scroll((0, u16::try_from(skip).unwrap_or(u16::MAX))),
        field,
    );
    if app.typing && field.width > 0 {
        let x = u16::try_from(column - skip).unwrap_or(0);
        frame.set_cursor_position(Position::new(field.x + x, field.y));
    }
}

fn column_header() -> String {
    format!(
        "    {} {} {:>AGE$} {:>COUNT$} title",
        fit("machine", MACHINE),
        fit("adapter", ADAPTER),
        "age",
        "msgs"
    )
}

fn render_rows(frame: &mut Frame, app: &mut App, area: Rect) {
    let items = match row_items(app) {
        Ok(items) => items,
        Err(placeholder) => {
            frame.render_widget(
                Paragraph::new(placeholder.dim()).wrap(Wrap { trim: true }),
                area,
            );
            return;
        }
    };
    let list = List::new(items)
        .highlight_symbol("> ")
        .highlight_spacing(HighlightSpacing::Always)
        .highlight_style(Style::new().reversed())
        .scroll_padding(1);
    frame.render_stateful_widget(list, area, app.state_mut());
}

/// The rows of the current view, or the sentence that stands in for them.
fn row_items(app: &App) -> Result<Vec<ListItem<'static>>, String> {
    let project = app
        .scope()
        .project
        .unwrap_or_else(|| "all projects".to_owned());
    if let Some(search) = &app.search {
        return match &search.response {
            None => Err("searching...".to_owned()),
            Some(response) if response.searchable_in_scope == 0 => Err(format!(
                "nothing searchable in scope: the filters ({project}, {}) excluded every message before search ran - p all projects, t all time",
                window_label(app)
            )),
            Some(response) if response.sessions.is_empty() => Err(format!(
                "no matches for \"{}\" among {} searchable messages",
                search.query, response.searchable_in_scope
            )),
            Some(response) => Ok(response
                .sessions
                .iter()
                .map(|session| search_item(app, session))
                .collect()),
        };
    }
    match app.listing() {
        None if app.lane_loading(Lane::Listing) && app.all_time => {
            Err("loading the all-time listing - this can take a while".to_owned())
        }
        None if app.lane_loading(Lane::Listing) => Err("loading sessions...".to_owned()),
        None => Err("no listing loaded - r to retry".to_owned()),
        Some([]) => Err(format!(
            "no sessions in {} for {project} - p all projects, t all time",
            window_label(app)
        )),
        Some(rows) => Ok(rows.iter().map(|row| listing_item(app, row)).collect()),
    }
}

fn machine(app: &App, session_id: &str) -> Span<'static> {
    match app.details.get(session_id) {
        Some(detail) => match &detail.host {
            Some(host) => Span::raw(fit(host, MACHINE)),
            None => Span::raw(fit("local?", MACHINE)).dim(),
        },
        None => Span::raw(fit("", MACHINE)),
    }
}

fn glyph(app: &App, session_id: &str) -> Span<'static> {
    if app.live_agent(session_id).is_some() {
        "● ".fg(Color::Green)
    } else {
        Span::raw("  ")
    }
}

fn listing_item(app: &App, row: &SessionRow) -> ListItem<'static> {
    let detail = app.details.get(&row.session_id);
    let count = detail.map_or_else(String::new, |d| d.message_count.to_string());
    let title = match detail {
        Some(detail) => detail
            .title
            .as_deref()
            .map_or_else(|| NO_TITLE.dim(), |t| Span::raw(one_line(t))),
        None => "...".dim(),
    };
    ListItem::new(Line::from(vec![
        glyph(app, &row.session_id),
        machine(app, &row.session_id),
        Span::raw(format!(
            " {} {:>AGE$} {:>COUNT$} ",
            fit(&row.source_agent, ADAPTER),
            age(app.now, row.last_ts),
            count
        )),
        title,
    ]))
}

fn search_item(app: &App, session: &SearchSession) -> ListItem<'static> {
    let newest = session.matches.iter().map(|m| m.timestamp).max();
    let snippet = session
        .matches
        .first()
        .map_or_else(String::new, |m| one_line(&m.text));
    let count = format!(
        "{}/{}",
        session.matched_message_count, session.session_messages_count
    );
    ListItem::new(Line::from(vec![
        glyph(app, &session.session_id),
        machine(app, &session.session_id),
        Span::raw(format!(
            " {} {:>AGE$} {:>COUNT$} {snippet}",
            fit(&session.source_agent, ADAPTER),
            newest.map_or_else(String::new, |ts| age(app.now, ts)),
            count
        )),
    ]))
}

fn render_preview(frame: &mut Frame, app: &App, area: Rect) {
    let block = Block::bordered().title(" preview - newest first ");
    let lines = match app.selected_id() {
        None => vec![Line::from("nothing selected".dim())],
        Some(id) => match app.previews.get(id) {
            None => vec![Line::from("loading preview...".dim())],
            Some(messages) if messages.is_empty() => {
                vec![Line::from("(no conversational messages)".dim())]
            }
            Some(messages) => messages
                .iter()
                .flat_map(|message| {
                    let mut lines = vec![Line::from(vec![
                        Span::styled(message.role.clone(), role_style(&message.role)),
                        Span::raw(format!(" {} ago", age(app.now, message.timestamp))).dim(),
                    ])];
                    lines.extend(message.text.split('\n').map(|l| Line::raw(l.to_owned())));
                    lines.push(Line::default());
                    lines
                })
                .collect(),
        },
    };
    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn footer(app: &App) -> Line<'static> {
    let help = if app.typing {
        "enter done  esc clear  up/down select"
    } else {
        "/ search  enter open  space preview  p projects  t time  r refresh  q quit"
    };
    let mut spans = Vec::new();
    if app.spinner_visible() {
        spans.push(Span::raw(format!("{} ", spinner_frame(app))).fg(Color::Yellow));
    }
    spans.push(Span::raw(help).dim());
    Line::from(spans)
}

fn spinner_frame(app: &App) -> &'static str {
    SPINNER[app.spinner % SPINNER.len()]
}

fn render_pager(frame: &mut Frame, app: &App) {
    let Some(pager) = &app.pager else {
        return;
    };
    let areas = pager_areas(frame.area());
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::raw(pager.title.clone()).bold(),
            Span::raw(format!(" | {}", pager.session_id)).dim(),
        ])),
        areas.header,
    );
    let height = usize::from(areas.text.height);
    if pager.is_empty() {
        let text = if pager.eof {
            "(no conversational messages)"
        } else {
            "loading transcript..."
        };
        frame.render_widget(Paragraph::new(text.dim()), areas.text);
    } else {
        let end = (pager.offset + height).min(pager.lines.len());
        let start = pager.offset.min(end);
        frame.render_widget(Paragraph::new(pager.lines[start..end].to_vec()), areas.text);
        let mut state =
            ScrollbarState::new(pager.lines.len().saturating_sub(height)).position(pager.offset);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            areas.bar,
            &mut state,
        );
    }
    let status = if app.lane_loading(Lane::Page) {
        format!("{} loading", spinner_frame(app))
    } else if pager.eof {
        "end".to_owned()
    } else {
        "more below".to_owned()
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::raw(PAGER_FOOTER).dim(),
            Span::raw(format!(
                " | {status} | line {}/{} | q back",
                (pager.offset + 1).min(pager.lines.len()),
                pager.lines.len()
            )),
        ])),
        areas.footer,
    );
}

fn render_toast(frame: &mut Frame, text: &str) {
    let area = frame.area();
    let width = area.width.min(TOAST_MAX_WIDTH);
    let inner = usize::from(width.saturating_sub(2)).max(1);
    let lines = u16::try_from(textwrap::wrap(text, inner).len()).unwrap_or(u16::MAX);
    let height = lines.saturating_add(2).min(area.height);
    let toast = Rect {
        x: area.right() - width,
        y: area.y + area.height.saturating_sub(height + 1),
        width,
        height,
    };
    frame.render_widget(Clear, toast);
    frame.render_widget(
        Paragraph::new(text.to_owned())
            .wrap(Wrap { trim: true })
            .block(Block::bordered().title(" esc dismiss ").fg(Color::Red)),
        toast,
    );
}

fn role_style(role: &str) -> Style {
    match role {
        "user" => Style::new().fg(Color::Cyan).bold(),
        "assistant" => Style::new().fg(Color::Green).bold(),
        _ => Style::new().bold(),
    }
}

/// A transcript message as pre-wrapped lines: a role header, one or more
/// lines per source line, and a blank separator.
pub(super) fn message_lines(message: &TranscriptMessage, width: usize) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(vec![
        Span::styled(message.role.clone(), role_style(&message.role)),
        Span::raw(format!(
            " {}",
            message.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        ))
        .dim(),
    ])];
    let options =
        textwrap::Options::new(width.max(1)).wrap_algorithm(textwrap::WrapAlgorithm::FirstFit);
    for source in sanitize(&message.text).split('\n') {
        if source.is_empty() {
            lines.push(Line::default());
        } else {
            lines.extend(
                textwrap::wrap(source, &options)
                    .into_iter()
                    .map(|piece| Line::raw(piece.into_owned())),
            );
        }
    }
    lines.push(Line::default());
    lines
}

/// Ratatui drops control characters but keeps the rest of an escape sequence
/// (`[31m` would render as text), so escapes go whole: CSI and OSC sequences,
/// two-byte escapes, `\r`, and every other control except `\n`. Tabs expand
/// to spaces.
pub(super) fn sanitize(text: &str) -> String {
    enum State {
        Text,
        Escape,
        Csi,
        Osc,
        OscEscape,
    }
    let mut out = String::with_capacity(text.len());
    let mut state = State::Text;
    let mut column = 0;
    for c in text.chars() {
        state = match state {
            State::Text => match c {
                '\u{1b}' => State::Escape,
                '\u{9b}' => State::Csi,
                '\u{9d}' => State::Osc,
                '\n' => {
                    out.push('\n');
                    column = 0;
                    State::Text
                }
                '\t' => {
                    let pad = TAB_STOP - column % TAB_STOP;
                    out.extend(std::iter::repeat_n(' ', pad));
                    column += pad;
                    State::Text
                }
                c if c.is_control() => State::Text,
                c => {
                    out.push(c);
                    column += c.width().unwrap_or(0);
                    State::Text
                }
            },
            State::Escape => match c {
                '[' => State::Csi,
                ']' => State::Osc,
                ' '..='/' => State::Escape,
                _ => State::Text,
            },
            State::Csi => match c {
                '@'..='~' => State::Text,
                '\n' => {
                    out.push('\n');
                    column = 0;
                    State::Text
                }
                _ => State::Csi,
            },
            State::Osc => match c {
                '\u{7}' | '\u{9c}' => State::Text,
                '\u{1b}' => State::OscEscape,
                _ => State::Osc,
            },
            State::OscEscape if c == '\\' => State::Text,
            State::OscEscape => State::Osc,
        };
    }
    out
}

/// A preview message's text as the cache keeps it: clipped, then sanitized.
pub(super) fn preview_text(text: &str) -> String {
    sanitize(&text.chars().take(PREVIEW_CHARS).collect::<String>())
}

/// A clean single line: sanitized, whitespace runs collapsed.
pub(super) fn one_line(text: &str) -> String {
    sanitize(text)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Exactly `width` cells: padded, or cut with an ellipsis.
pub(super) fn fit(text: &str, width: usize) -> String {
    let used = text.width();
    if used <= width {
        return format!("{text}{}", " ".repeat(width - used));
    }
    let mut out = String::new();
    let mut used = 0;
    for c in text.chars() {
        let w = c.width().unwrap_or(0);
        if used + w + 1 > width {
            break;
        }
        out.push(c);
        used += w;
    }
    if width > 0 {
        out.push('…');
        used += 1;
    }
    out.push_str(&" ".repeat(width.saturating_sub(used)));
    out
}

pub(super) fn age(now: DateTime<Utc>, then: DateTime<Utc>) -> String {
    let seconds = (now - then).num_seconds().max(0);
    match seconds {
        s if s < 60 => format!("{s}s"),
        s if s < 3_600 => format!("{}m", s / 60),
        s if s < 86_400 => format!("{}h", s / 3_600),
        s if s < 14 * 86_400 => format!("{}d", s / 86_400),
        s if s < 365 * 86_400 => format!("{}w", s / (7 * 86_400)),
        s => format!("{}y", s / (365 * 86_400)),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use chrono::TimeDelta;

    use super::*;
    use crate::desk::tests::{message, now};

    #[test]
    fn sanitize_strips_escapes_and_carriage_returns() {
        assert_eq!(sanitize("a\r\nb"), "a\nb");
        assert_eq!(sanitize("\u{1b}[1;31mred\u{1b}[0m"), "red");
        assert_eq!(sanitize("\u{1b}]0;title\u{7}x"), "x");
        assert_eq!(
            sanitize("\u{1b}]8;;http://x\u{1b}\\link\u{1b}]8;;\u{1b}\\"),
            "link"
        );
        assert_eq!(sanitize("\u{1b}(Bplain\u{1b}=k"), "plaink");
        assert_eq!(sanitize("\u{9b}2Jc1"), "c1");
        assert_eq!(sanitize("bell\u{7} nul\u{0}"), "bell nul");
        assert_eq!(sanitize("\u{1b}[31\nnext"), "\nnext");
    }

    #[test]
    fn sanitize_expands_tabs_by_cell_width() {
        assert_eq!(sanitize("\tx"), "    x");
        assert_eq!(sanitize("ab\tx"), "ab  x");
        assert_eq!(sanitize("日\tx"), "日  x");
        assert_eq!(sanitize("abcd\tx\n\ty"), "abcd    x\n    y");
    }

    #[test]
    fn fit_pads_and_cuts_by_cells() {
        assert_eq!(fit("ab", 4), "ab  ");
        assert_eq!(fit("abcdef", 4), "abc…");
        assert_eq!(fit("日本語", 4), "日… ");
        assert_eq!(fit("anything", 0), "");
        assert_eq!(fit("日本語", 1).width(), 1);
    }

    #[test]
    fn age_is_compact() {
        let now = now();
        assert_eq!(age(now, now - TimeDelta::seconds(5)), "5s");
        assert_eq!(age(now, now - TimeDelta::minutes(90)), "1h");
        assert_eq!(age(now, now - TimeDelta::days(3)), "3d");
        assert_eq!(age(now, now - TimeDelta::days(30)), "4w");
        assert_eq!(age(now, now + TimeDelta::minutes(1)), "0s");
    }

    #[test]
    fn message_lines_are_one_line_per_wrapped_source_line() {
        let lines = message_lines(&message("m", now(), "one two three\n\nfour"), 8);
        let text: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(
            text,
            [
                "user 2026-09-25 05:00:00 UTC",
                "one two",
                "three",
                "",
                "four",
                "",
            ]
        );
        assert!(
            lines
                .iter()
                .all(|line| line.width() <= 8 || line == &lines[0])
        );
    }
}
