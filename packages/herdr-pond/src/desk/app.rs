//! Desk state and reducers. `on_event` and `apply` stay sync and pure: they
//! return [`Effect`]s, and the runtime in `mod.rs` performs them and feeds the
//! results back as [`Msg`]s.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Rect, Size};
use ratatui::text::Line;
use ratatui::widgets::ListState;
use unicode_width::UnicodeWidthStr;

use super::cache::{Host, Known, SavedListing, Snapshot};
use super::ui;
use crate::types::{
    ApiError, Cursor, DeskContext, DeskExit, LISTING_ROWS, ListingScope, LiveAgent, PAGE_ROWS,
    SearchRequest, SearchResponse, SessionHost, SessionRow, SessionStart, SessionStats,
    SessionTitle, TranscriptMessage, TranscriptPage,
};

pub(super) const SEARCH_DEBOUNCE: Duration = Duration::from_millis(150);
pub(super) const PREVIEW_DEBOUNCE: Duration = Duration::from_millis(80);
const SEARCH_LIMIT: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Lane {
    Listing,
    Titles,
    Stats,
    Hosts,
    Live,
    Search,
    Preview,
    Page,
}

impl Lane {
    pub(super) const COUNT: usize = Self::Page as usize + 1;
    const VIEW: [Self; 3] = [Self::Search, Self::Preview, Self::Page];

    /// View lanes answer for what is on screen, so a view transition makes
    /// their in-flight results stale; data lanes fill caches that outlive views.
    fn is_view(self) -> bool {
        Self::VIEW.contains(&self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Call {
    Listing(ListingScope),
    Titles(Vec<String>),
    Stats(Vec<String>),
    Hosts(Vec<SessionStart>),
    Live,
    Search(SearchRequest),
    Preview(String),
    Page {
        session_id: String,
        after: Option<Cursor>,
    },
}

impl Call {
    pub(super) fn lane(&self) -> Lane {
        match self {
            Self::Listing(_) => Lane::Listing,
            Self::Titles(_) => Lane::Titles,
            Self::Stats(_) => Lane::Stats,
            Self::Hosts(_) => Lane::Hosts,
            Self::Live => Lane::Live,
            Self::Search(_) => Lane::Search,
            Self::Preview(_) => Lane::Preview,
            Self::Page { .. } => Lane::Page,
        }
    }

    /// A listing's `since` moves with the clock, so listings match by scope.
    fn same_target(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Listing(a), Self::Listing(b)) => scope_key(a) == scope_key(b),
            _ => self == other,
        }
    }
}

#[derive(Debug)]
pub(super) enum Reply {
    Listing(Result<Vec<SessionRow>, ApiError>),
    Titles(Result<Vec<SessionTitle>, ApiError>),
    Stats(Result<Vec<SessionStats>, ApiError>),
    Hosts(Result<Vec<SessionHost>, ApiError>),
    Live(Result<Vec<LiveAgent>, ApiError>),
    Search(Result<SearchResponse, ApiError>),
    Preview(Result<Vec<TranscriptMessage>, ApiError>),
    Page(Result<TranscriptPage, ApiError>),
}

/// A finished request. `call` is the target identity: `apply` checks it
/// against the current view, on top of the lane generation and view epoch.
#[derive(Debug)]
pub(super) struct Msg {
    pub(super) generation: u64,
    pub(super) epoch: u64,
    pub(super) call: Call,
    pub(super) reply: Reply,
}

#[derive(Debug, PartialEq)]
pub(super) enum Effect {
    /// Replaces (aborts) whatever the call's lane has in flight.
    Fetch {
        generation: u64,
        epoch: u64,
        delay: Duration,
        call: Call,
    },
    Cancel(Lane),
    Exit(DeskExit),
}

#[derive(Debug, Default)]
struct LaneState {
    generation: u64,
    in_flight: Option<Call>,
}

#[derive(Debug, Default)]
pub(super) struct Input {
    pub(super) text: String,
    cursor: usize,
}

impl Input {
    /// In terminal cells: CJK and emoji are two wide.
    pub(super) fn cursor_column(&self) -> usize {
        self.text[..self.cursor].width()
    }

    fn previous_boundary(&self) -> Option<usize> {
        self.text[..self.cursor]
            .char_indices()
            .next_back()
            .map(|(index, _)| index)
    }

    fn insert(&mut self, c: char) {
        self.text.insert(self.cursor, c);
        self.cursor += c.len_utf8();
    }

    fn backspace(&mut self) {
        if let Some(index) = self.previous_boundary() {
            self.text.remove(index);
            self.cursor = index;
        }
    }

    fn delete(&mut self) {
        if self.cursor < self.text.len() {
            self.text.remove(self.cursor);
        }
    }

    fn left(&mut self) {
        self.cursor = self.previous_boundary().unwrap_or(0);
    }

    fn right(&mut self) {
        if let Some(c) = self.text[self.cursor..].chars().next() {
            self.cursor += c.len_utf8();
        }
    }
}

#[derive(Debug)]
pub(super) struct Search {
    pub(super) query: String,
    pub(super) response: Option<SearchResponse>,
}

/// The transcript, wrapped once per load or resize so a frame only slices it.
#[derive(Debug)]
pub(super) struct Pager {
    pub(super) session_id: String,
    pub(super) title: String,
    messages: Vec<TranscriptMessage>,
    starts: Vec<usize>,
    pub(super) lines: Vec<Line<'static>>,
    pub(super) offset: usize,
    pub(super) eof: bool,
    width: usize,
}

impl Pager {
    fn new(session_id: String, title: String, width: usize) -> Self {
        Self {
            session_id,
            title,
            messages: Vec::new(),
            starts: Vec::new(),
            lines: Vec::new(),
            offset: 0,
            eof: false,
            width,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    fn next_cursor(&self) -> Option<Cursor> {
        self.messages.last().map(Cursor::after)
    }

    fn append(&mut self, messages: Vec<TranscriptMessage>) {
        for message in messages {
            self.starts.push(self.lines.len());
            self.lines.extend(ui::message_lines(&message, self.width));
            self.messages.push(message);
        }
    }

    /// Keeps the message at the top of the viewport at the top.
    fn rewrap(&mut self, width: usize) {
        if width == self.width {
            return;
        }
        let anchor = self
            .starts
            .partition_point(|start| *start <= self.offset)
            .saturating_sub(1);
        self.width = width;
        self.lines.clear();
        self.starts.clear();
        let messages = std::mem::take(&mut self.messages);
        self.append(messages);
        self.offset = self.starts.get(anchor).copied().unwrap_or(0);
    }

    fn scroll(&mut self, delta: isize, height: usize) {
        let max = self.lines.len().saturating_sub(height);
        self.offset = self.offset.saturating_add_signed(delta).min(max);
    }
}

pub(super) struct App {
    pub(super) now: DateTime<Utc>,
    pub(super) context: DeskContext,
    pub(super) size: Size,
    epoch: u64,
    lanes: [LaneState; Lane::COUNT],
    pub(super) all_projects: bool,
    pub(super) all_time: bool,
    /// Typed search covers everything unless narrowed to the project or the
    /// listing window.
    pub(super) search_project: bool,
    pub(super) search_recent: bool,
    /// One per scope, this desk's and other projects' alike, so saving the
    /// cache keeps what other desks stored.
    listings: Vec<SavedListing>,
    pub(super) known: HashMap<String, Known>,
    /// Asked per hydration lane since the last listing landed, so an id the
    /// server has no row for is not asked again until the next refresh.
    requested: HashMap<Lane, HashSet<String>>,
    pub(super) live: Vec<LiveAgent>,
    pub(super) listing_state: ListState,
    pub(super) search_state: ListState,
    pub(super) input: Input,
    pub(super) typing: bool,
    pub(super) search: Option<Search>,
    pub(super) preview_open: bool,
    pub(super) previews: HashMap<String, Vec<TranscriptMessage>>,
    pub(super) pager: Option<Pager>,
    pub(super) toast: Option<String>,
    pub(super) fatal: Option<String>,
    pub(super) spinner: usize,
    pub(super) dirty: bool,
    /// Set by a resize, so a burst of them re-wraps the pager once, before
    /// the next frame.
    resized: bool,
}

impl App {
    pub(super) fn new(context: DeskContext, now: DateTime<Utc>, size: Size) -> Self {
        Self {
            now,
            context,
            size,
            epoch: 0,
            lanes: Default::default(),
            all_projects: false,
            all_time: false,
            search_project: false,
            search_recent: false,
            listings: Vec::new(),
            known: HashMap::new(),
            requested: HashMap::new(),
            live: Vec::new(),
            listing_state: ListState::default(),
            search_state: ListState::default(),
            input: Input::default(),
            typing: false,
            search: None,
            preview_open: false,
            previews: HashMap::new(),
            pager: None,
            toast: None,
            fatal: None,
            spinner: 0,
            dirty: true,
            resized: false,
        }
    }

    /// Paints the cached listing and facts at once; `start` then refreshes
    /// behind them.
    pub(super) fn restore(&mut self, snapshot: Snapshot) {
        self.known = snapshot.sessions;
        self.listings = snapshot.listings;
        self.restore_listing_selection(None);
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.known.clone(), self.listings.clone())
    }

    pub(super) fn start(&mut self) -> Vec<Effect> {
        let mut effects = self.refresh();
        effects.extend(self.hydrate_visible());
        effects
    }

    pub(super) fn lane_loading(&self, lane: Lane) -> bool {
        self.lanes[lane as usize].in_flight.is_some()
    }

    /// The pager shows only its own page load; the error screen, none.
    pub(super) fn spinner_visible(&self) -> bool {
        if self.fatal.is_some() {
            false
        } else if self.pager.is_some() {
            self.lane_loading(Lane::Page)
        } else {
            self.lanes.iter().any(|lane| lane.in_flight.is_some())
        }
    }

    /// Called only while [`Self::spinner_visible`].
    pub(super) fn tick(&mut self) {
        self.spinner = self.spinner.wrapping_add(1);
        self.dirty = true;
    }

    pub(super) fn scope(&self) -> ListingScope {
        let project = (!self.all_projects)
            .then(|| self.context.project.clone())
            .flatten();
        let mut scope = ListingScope::recent(project, self.now);
        if self.all_time {
            scope.since = None;
        }
        scope
    }

    pub(super) fn search_scope(&self) -> ListingScope {
        let project = self
            .search_project
            .then(|| self.context.project.clone())
            .flatten();
        let mut scope = ListingScope::recent(project, self.now);
        if !self.search_recent {
            scope.since = None;
        }
        scope
    }

    pub(super) fn listing(&self) -> Option<&[SessionRow]> {
        let key = scope_key(&self.scope());
        self.listings
            .iter()
            .find(|listing| (&listing.project, listing.all_time) == (&key.0, key.1))
            .map(|listing| listing.rows.as_slice())
    }

    pub(super) fn rows_len(&self) -> usize {
        match &self.search {
            Some(search) => search.response.as_ref().map_or(0, |r| r.sessions.len()),
            None => self.listing().map_or(0, <[SessionRow]>::len),
        }
    }

    pub(super) fn id_at(&self, index: usize) -> Option<&str> {
        match &self.search {
            Some(search) => search
                .response
                .as_ref()
                .and_then(|r| r.sessions.get(index))
                .map(|s| s.session_id.as_str()),
            None => self
                .listing()
                .and_then(|rows| rows.get(index))
                .map(|row| row.session_id.as_str()),
        }
    }

    fn state(&self) -> &ListState {
        if self.search.is_some() {
            &self.search_state
        } else {
            &self.listing_state
        }
    }

    pub(super) fn state_mut(&mut self) -> &mut ListState {
        if self.search.is_some() {
            &mut self.search_state
        } else {
            &mut self.listing_state
        }
    }

    /// `ListState` only clamps at render time (`select_last` is `usize::MAX`),
    /// so every index use goes through here.
    pub(super) fn selected_index(&self) -> Option<usize> {
        let len = self.rows_len();
        self.state()
            .selected()
            .filter(|_| len > 0)
            .map(|index| index.min(len - 1))
    }

    pub(super) fn selected_id(&self) -> Option<&str> {
        self.selected_index().and_then(|index| self.id_at(index))
    }

    fn selected_listing_id(&self) -> Option<String> {
        let rows = self.listing()?;
        let index = self
            .listing_state
            .selected()?
            .min(rows.len().checked_sub(1)?);
        Some(rows[index].session_id.clone())
    }

    pub(super) fn live_agent(&self, session_id: &str) -> Option<&LiveAgent> {
        self.live.iter().find(|agent| agent.matches(session_id))
    }

    fn area(&self) -> Rect {
        Rect::new(0, 0, self.size.width, self.size.height)
    }

    fn pager_viewport(&self) -> Rect {
        ui::pager_areas(self.area()).text
    }

    /// Single-flight: a call for what its lane is already fetching joins
    /// that request instead of restarting it.
    fn fetch(&mut self, call: Call, delay: Duration) -> Option<Effect> {
        let lane = &mut self.lanes[call.lane() as usize];
        if lane
            .in_flight
            .as_ref()
            .is_some_and(|pending| pending.same_target(&call))
        {
            return None;
        }
        lane.generation += 1;
        lane.in_flight = Some(call.clone());
        Some(Effect::Fetch {
            generation: lane.generation,
            epoch: self.epoch,
            delay,
            call,
        })
    }

    fn cancel(&mut self, lane: Lane) -> Effect {
        let state = &mut self.lanes[lane as usize];
        state.generation += 1;
        state.in_flight = None;
        Effect::Cancel(lane)
    }

    /// A view or filter change: results already in flight for the old view
    /// must not land in the new one.
    fn transition(&mut self) -> Vec<Effect> {
        self.epoch += 1;
        Lane::VIEW
            .into_iter()
            .filter(|lane| self.lane_loading(*lane))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|lane| self.cancel(lane))
            .collect()
    }

    fn refresh(&mut self) -> Vec<Effect> {
        self.previews.clear();
        let mut effects: Vec<Effect> = self
            .fetch(Call::Listing(self.scope()), Duration::ZERO)
            .into_iter()
            .chain(self.fetch(Call::Live, Duration::ZERO))
            .collect();
        if let Some(query) = self.search.as_ref().map(|s| s.query.clone()) {
            effects.extend(self.fetch_search(query, Duration::ZERO));
        }
        effects.extend(self.preview_selected(Duration::ZERO));
        effects
    }

    fn fetch_search(&mut self, query: String, delay: Duration) -> Option<Effect> {
        let request = SearchRequest::new(query, SEARCH_LIMIT).within(&self.search_scope());
        self.fetch(Call::Search(request), delay)
    }

    pub(super) fn on_event(&mut self, event: &Event) -> Vec<Effect> {
        match event {
            Event::Resize(width, height) => self.resize(*width, *height),
            Event::Key(key) if key.kind != KeyEventKind::Release => self.on_key(*key),
            _ => Vec::new(),
        }
    }

    fn resize(&mut self, width: u16, height: u16) -> Vec<Effect> {
        self.size = Size::new(width, height);
        self.dirty = true;
        self.resized = true;
        self.hydrate_visible()
    }

    /// Re-wraps the pager for the latest size, then fetches more if the new
    /// wrap left the viewport near the end. Runs before every frame.
    pub(super) fn relayout(&mut self) -> Vec<Effect> {
        if !std::mem::take(&mut self.resized) {
            return Vec::new();
        }
        let viewport = self.pager_viewport();
        if let Some(pager) = &mut self.pager {
            pager.rewrap(usize::from(viewport.width));
            pager.scroll(0, usize::from(viewport.height));
        }
        self.load_more()
    }

    fn on_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && key.code == KeyCode::Char('c') {
            return vec![Effect::Exit(DeskExit::Quit)];
        }
        self.dirty = true;
        if self.toast.take().is_some() && key.code == KeyCode::Esc {
            return Vec::new();
        }
        if self.fatal.is_some() {
            return match key.code {
                KeyCode::Char('r') => {
                    self.fatal = None;
                    self.refresh()
                }
                KeyCode::Char('q') | KeyCode::Esc => vec![Effect::Exit(DeskExit::Quit)],
                _ => Vec::new(),
            };
        }
        if self.pager.is_some() {
            return self.on_pager_key(key);
        }
        if self.typing {
            return self.on_input_key(key);
        }
        self.on_list_key(key)
    }

    fn page_height(&self) -> isize {
        let height = ui::desk_areas(self.area(), self.preview_open).list.height;
        isize::try_from(height.saturating_sub(1))
            .unwrap_or(1)
            .max(1)
    }

    fn on_list_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        match key.code {
            KeyCode::Char('q') => vec![Effect::Exit(DeskExit::Quit)],
            KeyCode::Esc if self.search.is_some() => self.leave_search(),
            KeyCode::Esc => vec![Effect::Exit(DeskExit::Quit)],
            KeyCode::Char('/') => {
                self.typing = true;
                Vec::new()
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::PageDown => self.move_selection(self.page_height()),
            KeyCode::PageUp => self.move_selection(-self.page_height()),
            KeyCode::Home | KeyCode::Char('g') => self.move_selection(isize::MIN),
            KeyCode::End | KeyCode::Char('G') => self.move_selection(isize::MAX),
            KeyCode::Enter => self.open(),
            KeyCode::Char(' ') => {
                self.preview_open = !self.preview_open;
                let mut effects: Vec<Effect> =
                    self.preview_selected(Duration::ZERO).into_iter().collect();
                effects.extend(self.hydrate_visible());
                effects
            }
            KeyCode::Char('p') => self.toggle_scope(true),
            KeyCode::Char('t') => self.toggle_scope(false),
            KeyCode::Char('r') => self.refresh(),
            _ => Vec::new(),
        }
    }

    fn on_input_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        let plain = !key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT);
        match key.code {
            KeyCode::Esc if self.input.text.is_empty() => {
                self.typing = false;
                Vec::new()
            }
            KeyCode::Esc => self.leave_search(),
            KeyCode::Enter => {
                self.typing = false;
                Vec::new()
            }
            KeyCode::Down => self.move_selection(1),
            KeyCode::Up => self.move_selection(-1),
            KeyCode::Left => {
                self.input.left();
                Vec::new()
            }
            KeyCode::Right => {
                self.input.right();
                Vec::new()
            }
            KeyCode::Home => {
                self.input.cursor = 0;
                Vec::new()
            }
            KeyCode::End => {
                self.input.cursor = self.input.text.len();
                Vec::new()
            }
            KeyCode::Backspace => {
                self.input.backspace();
                self.query_changed()
            }
            KeyCode::Delete => {
                self.input.delete();
                self.query_changed()
            }
            KeyCode::Char(c) if plain => {
                self.input.insert(c);
                self.query_changed()
            }
            _ => Vec::new(),
        }
    }

    fn on_pager_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        let height = usize::from(self.pager_viewport().height);
        let page = isize::try_from(height.max(2) - 1).unwrap_or(1);
        let delta = match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Backspace | KeyCode::Left => {
                return self.close_pager();
            }
            KeyCode::Down | KeyCode::Char('j') => 1,
            KeyCode::Up | KeyCode::Char('k') => -1,
            KeyCode::PageDown | KeyCode::Char(' ') => page,
            KeyCode::PageUp | KeyCode::Char('b') => -page,
            KeyCode::Home | KeyCode::Char('g') => isize::MIN,
            KeyCode::End | KeyCode::Char('G') => isize::MAX,
            _ => return Vec::new(),
        };
        if let Some(pager) = &mut self.pager {
            pager.scroll(delta, height);
        }
        self.load_more()
    }

    fn move_selection(&mut self, delta: isize) -> Vec<Effect> {
        let len = self.rows_len();
        if len == 0 {
            return Vec::new();
        }
        let current = self.selected_index().unwrap_or(0);
        let next = current.saturating_add_signed(delta).min(len - 1);
        self.state_mut().select(Some(next));
        self.selection_changed()
    }

    fn selection_changed(&mut self) -> Vec<Effect> {
        let mut effects = self.hydrate_visible();
        effects.extend(self.preview_selected(PREVIEW_DEBOUNCE));
        effects
    }

    /// Hydrates the rows around the selection - never the whole listing -
    /// with one request per lane, concurrently, each asking only for what is
    /// not known yet. A host is read at the session's first message, so it
    /// waits for the stats that find that start when the listing cannot.
    fn hydrate_visible(&mut self) -> Vec<Effect> {
        let len = self.rows_len();
        let selected = self.selected_index().unwrap_or(0);
        let height = usize::from(ui::desk_areas(self.area(), self.preview_open).list.height).max(1);
        let window: Vec<String> = (selected.saturating_sub(height)
            ..(selected + height + 1).min(len))
            .filter_map(|index| self.id_at(index))
            .map(str::to_owned)
            .collect();
        let listing = self.search.is_none();
        let titles = self.unknown(Lane::Titles, &window, |known| known.title().is_none());
        let stats = self.unknown(Lane::Stats, &window, |known| {
            (listing && known.count().is_none())
                || (known.host.is_none() && known.first_ts.is_none())
        });
        let hosts: Vec<SessionStart> = self
            .unknown(Lane::Hosts, &window, |known| {
                known.host.is_none() && known.first_ts.is_some()
            })
            .into_iter()
            .filter_map(|id| {
                let first_ts = self.known.get(&id)?.first_ts?;
                Some(SessionStart {
                    session_id: id,
                    first_ts,
                })
            })
            .collect();
        [
            self.request(Lane::Titles, titles.clone(), Call::Titles(titles)),
            self.request(Lane::Stats, stats.clone(), Call::Stats(stats)),
            self.request(
                Lane::Hosts,
                hosts.iter().map(|start| start.session_id.clone()).collect(),
                Call::Hosts(hosts),
            ),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// The ids `lane` has not been asked about that still `need` it; none
    /// while the lane is busy, since a new fetch would abort its request.
    fn unknown(&self, lane: Lane, ids: &[String], need: impl Fn(&Known) -> bool) -> Vec<String> {
        if self.lane_loading(lane) {
            return Vec::new();
        }
        let asked = self.requested.get(&lane);
        ids.iter()
            .filter(|id| asked.is_none_or(|asked| !asked.contains(*id)))
            .filter(|id| self.known.get(*id).is_none_or(&need))
            .cloned()
            .collect()
    }

    fn request(&mut self, lane: Lane, ids: Vec<String>, call: Call) -> Option<Effect> {
        if ids.is_empty() {
            return None;
        }
        self.requested.entry(lane).or_default().extend(ids);
        self.fetch(call, Duration::ZERO)
    }

    fn preview_selected(&mut self, delay: Duration) -> Option<Effect> {
        let wanted = self
            .selected_id()
            .filter(|id| self.preview_open && !self.previews.contains_key(*id))
            .map(str::to_owned);
        match wanted {
            Some(id) => self.fetch(Call::Preview(id), delay),
            None => self
                .lane_loading(Lane::Preview)
                .then(|| self.cancel(Lane::Preview)),
        }
    }

    fn query_changed(&mut self) -> Vec<Effect> {
        let query = self.input.text.trim().to_owned();
        if query.is_empty() {
            return if self.search.is_some() {
                self.leave_search()
            } else {
                Vec::new()
            };
        }
        let mut effects = Vec::new();
        match &mut self.search {
            Some(search) if search.query == query => return effects,
            Some(search) => search.query.clone_from(&query),
            None => {
                effects = self.transition();
                self.search = Some(Search {
                    query: query.clone(),
                    response: None,
                });
                self.search_state = ListState::default();
            }
        }
        effects.extend(self.fetch_search(query, SEARCH_DEBOUNCE));
        effects
    }

    fn leave_search(&mut self) -> Vec<Effect> {
        self.input = Input::default();
        self.search = None;
        let mut effects = self.transition();
        effects.extend(self.selection_changed());
        effects
    }

    /// `p` means "this project only / everything" and `t` "the listing
    /// window / all time", for whichever view is up: the listing and typed
    /// search keep their own scopes.
    fn toggle_scope(&mut self, projects: bool) -> Vec<Effect> {
        if projects && self.context.project.is_none() {
            self.toast = Some(
                "the desk was opened without a project: already covering all projects".to_owned(),
            );
            return Vec::new();
        }
        if let Some(search) = &mut self.search {
            search.response = None;
            let query = search.query.clone();
            if projects {
                self.search_project = !self.search_project;
            } else {
                self.search_recent = !self.search_recent;
            }
            let mut effects = self.transition();
            effects.extend(self.fetch_search(query, Duration::ZERO));
            return effects;
        }
        let selected = self.selected_listing_id();
        if projects {
            self.all_projects = !self.all_projects;
        } else {
            self.all_time = !self.all_time;
        }
        let mut effects = self.transition();
        if self.listing().is_some() {
            self.restore_listing_selection(selected.as_deref());
        } else {
            effects.extend(self.fetch(Call::Listing(self.scope()), Duration::ZERO));
        }
        effects.extend(self.selection_changed());
        effects
    }

    fn restore_listing_selection(&mut self, selected: Option<&str>) {
        let rows = self.listing().unwrap_or_default();
        let index = if rows.is_empty() {
            None
        } else {
            let by_id = selected.and_then(|id| rows.iter().position(|row| row.session_id == id));
            let fallback = self
                .listing_state
                .selected()
                .unwrap_or(0)
                .min(rows.len() - 1);
            Some(by_id.unwrap_or(fallback))
        };
        self.listing_state.select(index);
    }

    fn open(&mut self) -> Vec<Effect> {
        let Some(id) = self.selected_id().map(str::to_owned) else {
            return Vec::new();
        };
        if let Some(agent) = self.live_agent(&id) {
            return vec![Effect::Exit(DeskExit::Jump {
                pane_id: agent.pane_id.clone(),
            })];
        }
        let title = self
            .known
            .get(&id)
            .and_then(|known| known.title().flatten())
            .map_or_else(|| ui::NO_TITLE.to_owned(), ui::one_line);
        let width = usize::from(self.pager_viewport().width);
        self.pager = Some(Pager::new(id, title, width));
        self.load_more()
    }

    fn close_pager(&mut self) -> Vec<Effect> {
        self.pager = None;
        let mut effects = self.transition();
        let stale_search = self
            .search
            .as_ref()
            .filter(|search| search.response.is_none())
            .map(|search| search.query.clone());
        if let Some(query) = stale_search {
            effects.extend(self.fetch_search(query, Duration::ZERO));
        }
        effects.extend(self.selection_changed());
        effects
    }

    /// Single-flight and lazy: the next page is fetched only when the viewport
    /// is within a screen of the end of what is loaded.
    fn load_more(&mut self) -> Vec<Effect> {
        let height = usize::from(self.pager_viewport().height).max(1);
        let Some(pager) = &self.pager else {
            return Vec::new();
        };
        if pager.eof
            || self.lane_loading(Lane::Page)
            || pager.offset + 2 * height < pager.lines.len()
        {
            return Vec::new();
        }
        let call = Call::Page {
            session_id: pager.session_id.clone(),
            after: pager.next_cursor(),
        };
        self.fetch(call, Duration::ZERO).into_iter().collect()
    }

    pub(super) fn apply(&mut self, msg: Msg) -> Vec<Effect> {
        let lane = msg.call.lane();
        if msg.generation != self.lanes[lane as usize].generation
            || (lane.is_view() && msg.epoch != self.epoch)
        {
            return Vec::new();
        }
        self.lanes[lane as usize].in_flight = None;
        self.dirty = true;
        match (msg.call, msg.reply) {
            (Call::Listing(scope), Reply::Listing(result)) => self.on_listing(&scope, result),
            (Call::Titles(ids), Reply::Titles(result)) => match result {
                Ok(rows) => {
                    let mut titles: HashMap<String, Option<String>> =
                        ids.into_iter().map(|id| (id, None)).collect();
                    titles.extend(rows.into_iter().map(|row| (row.session_id, row.title)));
                    for (id, title) in titles {
                        self.known.entry(id).or_default().set_title(title);
                    }
                    self.hydrate_visible()
                }
                Err(error) => self.toast(&error),
            },
            (Call::Stats(_), Reply::Stats(result)) => match result {
                Ok(rows) => {
                    for row in rows {
                        let known = self.known.entry(row.session_id).or_default();
                        known.set_stats(row.message_count, row.first_ts);
                    }
                    self.hydrate_visible()
                }
                Err(error) => self.toast(&error),
            },
            (Call::Hosts(_), Reply::Hosts(result)) => match result {
                Ok(rows) => {
                    for row in rows {
                        self.known.entry(row.session_id).or_default().host =
                            Some(row.host.map_or(Host::Unstamped, Host::Stamped));
                    }
                    self.hydrate_visible()
                }
                Err(error) => self.toast(&error),
            },
            (Call::Live, Reply::Live(result)) => match result {
                Ok(agents) => {
                    self.live = agents;
                    Vec::new()
                }
                Err(error) => self.toast(&error),
            },
            (Call::Search(request), Reply::Search(result)) => {
                self.on_search(&request.query, result)
            }
            (Call::Preview(id), Reply::Preview(result)) => {
                if self.selected_id() != Some(id.as_str()) {
                    return Vec::new();
                }
                match result {
                    Ok(messages) => {
                        if self.previews.len() >= LISTING_ROWS {
                            self.previews.clear();
                        }
                        let clean = messages
                            .into_iter()
                            .map(|message| TranscriptMessage {
                                text: ui::preview_text(&message.text),
                                ..message
                            })
                            .collect();
                        self.previews.insert(id, clean);
                        Vec::new()
                    }
                    Err(error) => self.toast(&error),
                }
            }
            (Call::Page { session_id, after }, Reply::Page(result)) => {
                self.on_page(&session_id, after.as_ref(), result)
            }
            _ => Vec::new(),
        }
    }

    fn toast(&mut self, error: &ApiError) -> Vec<Effect> {
        self.toast = Some(error.to_string());
        Vec::new()
    }

    fn on_listing(
        &mut self,
        scope: &ListingScope,
        result: Result<Vec<SessionRow>, ApiError>,
    ) -> Vec<Effect> {
        match result {
            Ok(rows) => {
                for row in &rows {
                    let known = self.known.entry(row.session_id.clone()).or_default();
                    known.observe(row, scope.since);
                }
                let key = scope_key(scope);
                let current = key == scope_key(&self.scope());
                let selected = self.selected_listing_id();
                self.listings
                    .retain(|listing| (&listing.project, listing.all_time) != (&key.0, key.1));
                self.listings.push(SavedListing {
                    project: key.0,
                    all_time: key.1,
                    saved_at: self.now,
                    rows,
                });
                if !current {
                    return Vec::new();
                }
                self.fatal = None;
                self.requested.clear();
                self.restore_listing_selection(selected.as_deref());
                self.hydrate_visible()
            }
            Err(error) => {
                if self.listings.is_empty()
                    && matches!(error, ApiError::PondTooOld | ApiError::Unreachable(_))
                {
                    self.fatal = Some(error.to_string());
                    Vec::new()
                } else {
                    self.toast(&error)
                }
            }
        }
    }

    fn on_search(&mut self, query: &str, result: Result<SearchResponse, ApiError>) -> Vec<Effect> {
        let Some(search) = self.search.as_mut().filter(|search| search.query == query) else {
            return Vec::new();
        };
        match result {
            Ok(response) => {
                let first = (!response.sessions.is_empty()).then_some(0);
                search.response = Some(response);
                self.search_state.select(first);
                self.selection_changed()
            }
            Err(error) => self.toast(&error),
        }
    }

    fn on_page(
        &mut self,
        session_id: &str,
        after: Option<&Cursor>,
        result: Result<TranscriptPage, ApiError>,
    ) -> Vec<Effect> {
        let Some(pager) = self.pager.as_mut().filter(|pager| {
            pager.session_id == session_id && pager.next_cursor().as_ref() == after
        }) else {
            return Vec::new();
        };
        match result {
            Ok(page) if page.messages.is_empty() => {
                pager.eof = true;
                if page.truncated {
                    self.toast = Some(
                        "the next message exceeds pond's response size budget - the transcript stops here"
                            .to_owned(),
                    );
                }
                Vec::new()
            }
            Ok(page) => {
                pager.eof = page.messages.len() < PAGE_ROWS && !page.truncated;
                pager.append(page.messages);
                self.load_more()
            }
            Err(error) => self.toast(&error),
        }
    }
}

/// Listings are cached per project and window kind, not per timestamp, so
/// toggling back is instant even though `since` moves with the clock.
fn scope_key(scope: &ListingScope) -> (Option<String>, bool) {
    (scope.project.clone(), scope.since.is_none())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use crossterm::event::KeyCode;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::style::Style;

    use chrono::TimeDelta;

    use super::*;
    use crate::desk::tests::{
        MockApi, app, context, key, message, now, press, screen, settle, sql_rows,
    };
    use crate::fake_pond::golden;
    use crate::types::{LISTING_ROWS, ProjectFilter};

    fn opened(api: &MockApi, width: u16, height: u16) -> App {
        let mut app = app(width, height);
        let effects = app.start();
        assert_eq!(settle(&mut app, api, effects), None);
        app
    }

    fn row(id: &str) -> SessionRow {
        SessionRow {
            session_id: id.to_owned(),
            last_ts: now() - TimeDelta::hours(1),
            first_ts: now() - TimeDelta::hours(3),
            message_count: 7,
            source_agent: "codex-cli".to_owned(),
            project: "/home/me/pj/pond".to_owned(),
        }
    }

    fn hydrations(api: &MockApi) -> Vec<Call> {
        api.calls()
            .into_iter()
            .filter(|call| matches!(call.lane(), Lane::Titles | Lane::Stats | Lane::Hosts))
            .collect()
    }

    fn ids(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| (*id).to_owned()).collect()
    }

    fn search_response(body: &str) -> SearchResponse {
        serde_json::from_str(body).unwrap()
    }

    fn fetches(effects: &[Effect]) -> Vec<&Call> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Fetch { call, .. } => Some(call),
                _ => None,
            })
            .collect()
    }

    fn type_query(app: &mut App, api: &MockApi, text: &str) {
        for code in std::iter::once('/').chain(text.chars()).map(KeyCode::Char) {
            press(app, api, code);
        }
    }

    #[test]
    fn opening_lists_then_hydrates_the_visible_page() {
        let api = MockApi::golden();
        let mut app = opened(&api, 110, 10);
        let calls = api.calls();
        assert!(matches!(&calls[0], Call::Listing(scope) if scope.limit == LISTING_ROWS));
        assert_eq!(calls[1], Call::Live);
        let page = ids(&["s-live", "s-old"]);
        assert_eq!(
            hydrations(&api),
            [
                Call::Titles(page.clone()),
                Call::Stats(page),
                Call::Hosts(
                    sql_rows::<SessionStats>(golden::SQL_STATS)
                        .into_iter()
                        .map(|stats| SessionStart {
                            session_id: stats.session_id,
                            first_ts: stats.first_ts,
                        })
                        .collect()
                ),
            ],
            "one request per lane for the page; hosts wait for the starts"
        );
        let screen = screen(&mut app);
        assert!(screen.contains("msgs = whole-session counts"), "{screen}");
        assert!(screen.contains("● ws-pond-01 claude-code"), "{screen}");
        assert!(screen.contains("fix the timer re-arm"), "{screen}");
        assert!(screen.contains("local?"), "unstamped host: {screen}");
        assert!(screen.contains("(no user message)"), "{screen}");
        assert!(screen.contains("    94 "), "{screen}");
    }

    #[test]
    fn scrolling_hydrates_only_rows_near_the_selection() {
        let api = MockApi {
            sessions: (0..60).map(|i| row(&format!("s{i:02}"))).collect(),
            ..MockApi::default()
        };
        let mut app = opened(&api, 100, 10);
        let first = api.calls().into_iter().find_map(|call| match call {
            Call::Titles(ids) => Some(ids),
            _ => None,
        });
        assert!(first.unwrap().len() < 20, "never the whole listing");
        press(&mut app, &api, KeyCode::End);
        let Some(Call::Titles(ids)) = api
            .calls()
            .into_iter()
            .rev()
            .find(|call| matches!(call, Call::Titles(_)))
        else {
            panic!("jumping to the end hydrates the new page");
        };
        assert!(ids.contains(&"s59".to_owned()));
        assert!(!ids.contains(&"s00".to_owned()));
    }

    #[test]
    fn a_windowed_count_waits_for_the_session_start_and_all_time_needs_none() {
        let api = MockApi {
            titles_delay: Duration::from_secs(1),
            ..MockApi::golden()
        };
        let mut app = app(110, 10);
        let effects = app.start();
        let listing: Vec<Effect> = effects
            .into_iter()
            .filter(|effect| {
                matches!(
                    effect,
                    Effect::Fetch {
                        call: Call::Listing(_),
                        ..
                    }
                )
            })
            .collect();
        let hydration = settle_listing(&mut app, &api, listing);
        assert!(
            !screen(&mut app).contains("    94 "),
            "an in-window count is not the whole session's until the start is known"
        );
        assert!(
            hydration.contains(&Call::Stats(ids(&["s-live", "s-old"]))),
            "{hydration:?}"
        );

        let all_time = MockApi::golden();
        let mut app = opened(&all_time, 110, 10);
        press(&mut app, &all_time, KeyCode::Char('t'));
        let after_toggle: Vec<Call> = hydrations(&all_time).into_iter().skip(3).collect();
        assert!(
            after_toggle.is_empty(),
            "known starts and counts need no new hydration: {after_toggle:?}"
        );
        assert!(screen(&mut app).contains("    94 "));
    }

    /// Applies just the listing reply and returns the hydration it asks for.
    fn settle_listing(app: &mut App, api: &MockApi, listing: Vec<Effect>) -> Vec<Call> {
        let [
            Effect::Fetch {
                generation,
                epoch,
                call,
                ..
            },
        ] = &listing[..]
        else {
            panic!("{listing:?}");
        };
        let reply = Reply::Listing(Ok(api.sessions.clone()));
        app.apply(Msg {
            generation: *generation,
            epoch: *epoch,
            call: call.clone(),
            reply,
        })
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Fetch { call, .. } => Some(call),
            _ => None,
        })
        .collect()
    }

    #[test]
    fn hosts_render_before_titles_land() {
        let api = MockApi::golden();
        let mut app = opened(&api, 110, 10);
        app.known.clear();
        app.requested.clear();
        for id in ["s-live", "s-old"] {
            app.known
                .entry(id.to_owned())
                .or_default()
                .set_stats(5, now() - TimeDelta::days(1));
        }
        let effects = app.hydrate_visible();
        let lanes: Vec<Lane> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Fetch { call, .. } => Some(call.lane()),
                _ => None,
            })
            .collect();
        assert_eq!(
            lanes,
            [Lane::Titles, Lane::Hosts],
            "concurrent, not chained"
        );

        let hosts = effects.into_iter().filter(|effect| {
            matches!(
                effect,
                Effect::Fetch {
                    call: Call::Hosts(_),
                    ..
                }
            )
        });
        settle(&mut app, &api, hosts.collect());
        let partial = screen(&mut app);
        assert!(partial.contains("ws-pond-01"), "{partial}");
        assert!(!partial.contains("fix the timer re-arm"), "{partial}");
        assert!(app.lane_loading(Lane::Titles));
    }

    #[test]
    fn a_restored_cache_paints_at_once_and_hydrates_only_what_it_lacks() {
        let warm = MockApi::golden();
        let saved = opened(&warm, 110, 10).snapshot();

        let mut fresh_rows = warm.sessions.clone();
        let mut new_session = row("s-new");
        new_session.last_ts = now() - TimeDelta::minutes(1);
        fresh_rows.insert(0, new_session);
        let api = MockApi {
            sessions: fresh_rows,
            ..MockApi::golden()
        };
        let mut app = app(110, 10);
        app.restore(saved);
        let painted = screen(&mut app);
        assert!(painted.contains("fix the timer re-arm"), "{painted}");
        assert!(painted.contains("ws-pond-01"), "{painted}");
        assert!(painted.contains("    94 "), "{painted}");

        let effects = app.start();
        assert!(
            fetches(&effects)
                .iter()
                .all(|call| !matches!(call.lane(), Lane::Titles | Lane::Stats | Lane::Hosts)),
            "every cached row is complete: {effects:?}"
        );
        assert!(app.spinner_visible(), "the refresh runs behind the cache");
        assert!(screen(&mut app).contains("2 sessions"));
        settle(&mut app, &api, effects);
        assert!(screen(&mut app).contains("3 sessions"));
        for call in hydrations(&api) {
            let asked = match call {
                Call::Titles(ids) | Call::Stats(ids) => ids,
                Call::Hosts(starts) => starts.into_iter().map(|s| s.session_id).collect(),
                _ => unreachable!(),
            };
            assert_eq!(asked, ["s-new"], "only the new session is hydrated");
        }
    }

    #[test]
    fn hosts_show_short_names_and_this_machine() {
        let api = MockApi {
            hosts: vec![
                SessionHost {
                    session_id: "s-live".to_owned(),
                    host: Some("beelink-eq14.tail1234.ts.net".to_owned()),
                },
                SessionHost {
                    session_id: "s-old".to_owned(),
                    host: Some("DEVBOX.lan".to_owned()),
                },
            ],
            ..MockApi::golden()
        };
        let mut app = opened(&api, 110, 10);
        let screen_text = screen(&mut app);
        assert!(
            screen_text.contains("● beelink-eq14 claude-code"),
            "short name, uncut: {screen_text}"
        );
        assert!(
            screen_text.contains("  this         codex-cli"),
            "{screen_text}"
        );
        assert!(!screen_text.contains("tail1234"), "{screen_text}");
        assert!(
            screen_text.contains("    machine      adapter"),
            "{screen_text}"
        );

        let mut narrow = opened(&api, 60, 10);
        let narrow_text = screen(&mut narrow);
        assert!(narrow_text.contains("● beelink-e… claude"), "{narrow_text}");
    }

    #[test]
    fn an_empty_store_says_so() {
        let api = MockApi::default();
        let mut app = opened(&api, 100, 10);
        let screen = screen(&mut app);
        assert!(
            screen.contains("no sessions in last 14 days for /home/me/pj/pond"),
            "{screen}"
        );
    }

    #[test]
    fn pond_too_old_on_the_first_listing_is_a_full_screen_error() {
        let api = MockApi {
            listing_error: Some(ApiError::PondTooOld),
            ..MockApi::golden()
        };
        let mut app = opened(&api, 100, 12);
        assert_eq!(app.fatal, Some(ApiError::PondTooOld.to_string()));
        let screen = screen(&mut app);
        assert!(screen.contains("upgrade pond"), "{screen}");
        assert!(screen.contains("r retry"), "{screen}");

        let fixed = MockApi::golden();
        press(&mut app, &fixed, KeyCode::Char('r'));
        assert_eq!(app.fatal, None);
        assert_eq!(app.listing().map(<[SessionRow]>::len), Some(2));
    }

    #[test]
    fn later_errors_are_verbatim_toasts() {
        let mut app = opened(&MockApi::golden(), 100, 12);
        let error = ApiError::Pond {
            code: "validation_failed".to_owned(),
            message: "sql error: query exceeded the 30s limit".to_owned(),
        };
        let failing = MockApi {
            listing_error: Some(error.clone()),
            ..MockApi::golden()
        };
        press(&mut app, &failing, KeyCode::Char('r'));
        assert_eq!(app.fatal, None, "a loaded desk keeps its rows");
        assert_eq!(app.toast, Some(error.to_string()));
        assert!(screen(&mut app).contains("pond validation_failed: sql error"));
        press(&mut app, &failing, KeyCode::Esc);
        assert_eq!(app.toast, None);
        assert_eq!(app.listing().map(<[SessionRow]>::len), Some(2));
    }

    #[test]
    fn selection_survives_a_refresh_by_session_id() {
        let api = MockApi {
            sessions: vec![row("a"), row("b"), row("c")],
            ..MockApi::default()
        };
        let mut app = opened(&api, 100, 12);
        press(&mut app, &api, KeyCode::Down);
        assert_eq!(app.selected_id(), Some("b"));
        let reordered = MockApi {
            sessions: vec![row("x"), row("y"), row("c"), row("b")],
            ..MockApi::default()
        };
        press(&mut app, &reordered, KeyCode::Char('r'));
        assert_eq!(app.selected_id(), Some("b"));
    }

    #[test]
    fn select_last_is_clamped_before_indexing() {
        let api = MockApi {
            sessions: vec![row("a"), row("b")],
            ..MockApi::default()
        };
        let mut app = opened(&api, 100, 12);
        app.listing_state.select_last();
        assert_eq!(app.selected_index(), Some(1));
        press(&mut app, &api, KeyCode::Enter);
        assert_eq!(app.pager.as_ref().map(|p| p.session_id.as_str()), Some("b"));
    }

    #[test]
    fn all_time_is_a_slow_loading_state_and_toggling_back_is_cached() {
        let api = MockApi::golden();
        let mut app = opened(&api, 100, 12);
        let effects = app.on_event(&key(KeyCode::Char('t')));
        let calls = fetches(&effects);
        assert!(matches!(calls[0], Call::Listing(scope) if scope.since.is_none()));
        assert!(screen(&mut app).contains("loading the all-time listing"));
        settle(&mut app, &api, effects);
        assert!(screen(&mut app).contains("| all time |"));

        let effects = app.on_event(&key(KeyCode::Char('t')));
        assert!(
            !fetches(&effects)
                .iter()
                .any(|call| matches!(call, Call::Listing(_))),
            "toggling back is served from the cache: {effects:?}"
        );
        assert_eq!(app.listing().map(<[SessionRow]>::len), Some(2));
    }

    #[test]
    fn a_refresh_joins_requests_already_in_flight() {
        let mut app = app(100, 12);
        let first = app.start();
        assert_eq!(fetches(&first), [&Call::Listing(app.scope()), &Call::Live]);
        app.now += TimeDelta::seconds(5);
        let again = app.on_event(&key(KeyCode::Char('r')));
        assert!(fetches(&again).is_empty(), "{again:?}");
    }

    #[test]
    fn toggling_back_leaves_the_slow_listing_running_into_the_cache() {
        let api = MockApi::golden();
        let mut app = opened(&api, 100, 12);
        let all_time = app.on_event(&key(KeyCode::Char('t')));
        let listing = all_time
            .into_iter()
            .find(|effect| {
                matches!(
                    effect,
                    Effect::Fetch {
                        call: Call::Listing(_),
                        ..
                    }
                )
            })
            .unwrap();
        let back = app.on_event(&key(KeyCode::Char('t')));
        assert!(
            !back.contains(&Effect::Cancel(Lane::Listing)),
            "toggling back cancelled the listing: {back:?}"
        );
        assert!(!app.all_time);
        settle(&mut app, &api, vec![listing]);
        assert!(!app.all_time, "the late listing moved the view");
        let cached = app.on_event(&key(KeyCode::Char('t')));
        assert!(
            !fetches(&cached)
                .iter()
                .any(|call| matches!(call, Call::Listing(_))),
            "the landed listing was not cached: {cached:?}"
        );
        assert_eq!(app.listing().map(<[SessionRow]>::len), Some(2));
    }

    #[test]
    fn previews_are_cached_clipped_and_clean() {
        let api = MockApi {
            transcript: vec![message(
                "m1",
                now(),
                &format!("\u{1b}[31m{}", "x".repeat(ui::PREVIEW_CHARS * 3)),
            )],
            ..MockApi::golden()
        };
        let mut app = opened(&api, 100, 20);
        press(&mut app, &api, KeyCode::Char(' '));
        let text = &app.previews["s-live"][0].text;
        assert!(text.chars().count() <= ui::PREVIEW_CHARS);
        assert!(!text.contains('\u{1b}') && !text.contains("[31m"));
    }

    #[test]
    fn the_spinner_ticks_only_where_it_shows() {
        let mut app = opened(&MockApi::golden(), 100, 12);
        assert!(!app.spinner_visible());
        let refresh = app.on_event(&key(KeyCode::Char('r')));
        assert!(!refresh.is_empty());
        assert!(
            app.spinner_visible(),
            "the desk footer shows the listing load"
        );
        app.pager = Some(Pager::new("s-old".to_owned(), "t".to_owned(), 80));
        assert!(!app.spinner_visible(), "the pager shows only its own load");
        app.load_more();
        assert!(app.spinner_visible());
    }

    fn last_search(api: &MockApi) -> SearchRequest {
        api.calls()
            .into_iter()
            .rev()
            .find_map(|call| match call {
                Call::Search(request) => Some(request),
                _ => None,
            })
            .expect("a search was sent")
    }

    #[test]
    fn search_covers_everything_until_p_or_t_narrow_it() {
        let api = MockApi::golden();
        let mut app = opened(&api, 140, 12);
        type_query(&mut app, &api, "timer");
        let wide = last_search(&api);
        assert_eq!(wide.filters.project, None);
        assert_eq!(wide.filters.from_date, None);
        assert!(screen(&mut app).contains("| searching everything |"));

        press(&mut app, &api, KeyCode::Enter);
        press(&mut app, &api, KeyCode::Char('p'));
        let project = last_search(&api);
        assert_eq!(
            project.filters.project,
            Some(ProjectFilter::Contains("/home/me/pj/pond".to_owned()))
        );
        assert_eq!(project.filters.from_date, None);
        assert!(screen(&mut app).contains("| searching this project |"));
        assert!(
            !api.calls()
                .iter()
                .any(|call| matches!(call, Call::Listing(s) if s.project.is_none())),
            "p in search leaves the listing scope alone"
        );

        press(&mut app, &api, KeyCode::Char('t'));
        assert_eq!(
            last_search(&api).filters.from_date.as_deref(),
            Some("2026-09-11")
        );
        assert!(screen(&mut app).contains("| searching this project, last 14 days |"));
        press(&mut app, &api, KeyCode::Char('p'));
        assert!(screen(&mut app).contains("| searching all projects, last 14 days |"));

        press(&mut app, &api, KeyCode::Esc);
        assert!(
            !app.all_projects && !app.all_time,
            "the listing kept its default"
        );
        press(&mut app, &api, KeyCode::Char('p'));
        assert!(
            api.calls()
                .iter()
                .any(|call| matches!(call, Call::Listing(s) if s.project.is_none())),
            "p in the listing widens the listing"
        );
    }

    #[test]
    fn p_without_a_project_explains_itself() {
        let api = MockApi::golden();
        let mut app = App::new(
            DeskContext {
                project: None,
                ..context()
            },
            now(),
            Size::new(100, 12),
        );
        let effects = app.start();
        settle(&mut app, &api, effects);
        type_query(&mut app, &api, "timer");
        press(&mut app, &api, KeyCode::Enter);
        let before = api.calls().len();
        press(&mut app, &api, KeyCode::Char('p'));
        assert_eq!(api.calls().len(), before);
        assert!(app.toast.as_ref().unwrap().contains("without a project"));
    }

    #[test]
    fn zero_matches_and_nothing_in_scope_read_differently() {
        let empty_scope = MockApi {
            search: Some(search_response(golden::SEARCH_OUT_OF_SCOPE)),
            ..MockApi::golden()
        };
        let mut app = opened(&empty_scope, 120, 12);
        type_query(&mut app, &empty_scope, "timer");
        let screen_text = screen(&mut app);
        assert!(
            screen_text.contains("nothing searchable in scope (searching everything)"),
            "{screen_text}"
        );

        let no_matches = MockApi {
            search: Some(SearchResponse {
                searchable_in_scope: 4120,
                ..search_response(golden::SEARCH_OUT_OF_SCOPE)
            }),
            ..MockApi::golden()
        };
        let mut app = opened(&no_matches, 120, 12);
        type_query(&mut app, &no_matches, "xyz");
        let screen_text = screen(&mut app);
        assert!(
            screen_text.contains("no matches for \"xyz\" among 4120 searchable messages"),
            "{screen_text}"
        );
    }

    #[test]
    fn search_results_render_and_esc_returns_to_the_listing() {
        let api = MockApi {
            search: Some(search_response(golden::SEARCH)),
            ..MockApi::golden()
        };
        let mut app = opened(&api, 120, 12);
        type_query(&mut app, &api, "timer");
        let screen_text = screen(&mut app);
        assert!(screen_text.contains("1 sessions match"), "{screen_text}");
        assert!(
            screen_text.contains("2/94 the systemd timer stops re-arming"),
            "{screen_text}"
        );
        press(&mut app, &api, KeyCode::Esc);
        assert!(app.search.is_none());
        assert!(screen(&mut app).contains("2 sessions"));
    }

    #[test]
    fn input_cursor_counts_cells_not_chars() {
        let api = MockApi::golden();
        let mut app = opened(&api, 100, 12);
        type_query(&mut app, &api, "日本x");
        assert_eq!(app.input.cursor_column(), 5);
        press(&mut app, &api, KeyCode::Left);
        press(&mut app, &api, KeyCode::Left);
        assert_eq!(app.input.cursor_column(), 2);
        press(&mut app, &api, KeyCode::Backspace);
        assert_eq!(app.input.text, "本x");
        assert_eq!(app.search.as_ref().unwrap().query, "本x");
    }

    #[test]
    fn stale_messages_are_dropped() {
        let api = MockApi::golden();
        let mut app = opened(&api, 100, 12);
        let effects = app.on_event(&key(KeyCode::Char('/')));
        assert!(effects.is_empty());
        let first = app.on_event(&key(KeyCode::Char('a')));
        let second = app.on_event(&key(KeyCode::Char('b')));
        let reply = |effect: &Effect, epoch_shift: u64| {
            let Effect::Fetch {
                generation,
                epoch,
                call,
                ..
            } = effect
            else {
                panic!("{effect:?}");
            };
            Msg {
                generation: *generation,
                epoch: epoch - epoch_shift,
                call: call.clone(),
                reply: Reply::Search(Ok(search_response(golden::SEARCH))),
            }
        };
        let latest = second.last().unwrap();
        app.apply(reply(first.last().unwrap(), 0));
        assert!(
            app.search.as_ref().unwrap().response.is_none(),
            "old generation"
        );
        app.apply(reply(latest, 1));
        assert!(
            app.search.as_ref().unwrap().response.is_none(),
            "old view epoch"
        );
        app.apply(reply(latest, 0));
        assert!(app.search.as_ref().unwrap().response.is_some());
    }

    #[test]
    fn a_preview_for_a_session_no_longer_selected_is_dropped() {
        let api = MockApi::golden();
        let mut app = opened(&api, 100, 20);
        let effects = app.on_event(&key(KeyCode::Char(' ')));
        let preview = effects
            .into_iter()
            .find(|effect| {
                matches!(
                    effect,
                    Effect::Fetch {
                        call: Call::Preview(_),
                        ..
                    }
                )
            })
            .unwrap();
        app.listing_state.select(Some(1));
        settle(&mut app, &api, vec![preview]);
        assert!(app.previews.is_empty());

        press(&mut app, &api, KeyCode::Up);
        assert_eq!(app.previews.get("s-live").map(Vec::len), Some(2));
        assert!(screen(&mut app).contains("preview - newest first"));
    }

    #[test]
    fn enter_on_a_live_row_jumps_and_on_others_opens_the_pager() {
        let api = MockApi::golden();
        let mut app = opened(&api, 100, 12);
        assert_eq!(
            press(&mut app, &api, KeyCode::Enter),
            Some(DeskExit::Jump {
                pane_id: "p7".to_owned()
            })
        );
        press(&mut app, &api, KeyCode::Down);
        assert_eq!(press(&mut app, &api, KeyCode::Enter), None);
        let pager = app.pager.as_ref().unwrap();
        assert_eq!(pager.session_id, "s-old");
        assert!(pager.eof);
        let screen_text = screen(&mut app);
        assert!(screen_text.contains(ui::PAGER_FOOTER), "{screen_text}");
        press(&mut app, &api, KeyCode::Char('q'));
        assert!(app.pager.is_none());
    }

    #[test]
    fn the_pager_seeks_through_more_ties_than_a_page() {
        let tied = now() - TimeDelta::hours(2);
        let mut transcript: Vec<_> = (0..PAGE_ROWS * 2 + 7)
            .map(|i| message(&format!("m{i:03}"), tied, &format!("tied {i}")))
            .collect();
        transcript.push(message("a-late", tied + TimeDelta::microseconds(1), "last"));
        transcript.reverse();
        let api = MockApi {
            transcript,
            ..MockApi::golden()
        };
        let mut app = opened(&api, 100, 12);
        press(&mut app, &api, KeyCode::Down);
        press(&mut app, &api, KeyCode::Enter);
        while !app.pager.as_ref().unwrap().eof {
            press(&mut app, &api, KeyCode::End);
        }
        let pager = app.pager.as_ref().unwrap();
        let ids: Vec<&str> = pager
            .messages
            .iter()
            .map(|m| m.message_id.as_str())
            .collect();
        assert_eq!(ids.len(), PAGE_ROWS * 2 + 8, "nothing skipped or repeated");
        assert_eq!(ids.first(), Some(&"m000"));
        assert_eq!(ids.last(), Some(&"a-late"));
        let pages: Vec<Option<Cursor>> = api
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Page { after, .. } => Some(after),
                _ => None,
            })
            .collect();
        assert_eq!(pages.len(), 3, "single-flight, one request per page");
        assert_eq!(
            pages[1],
            Some(Cursor {
                timestamp: tied,
                message_id: format!("m{:03}", PAGE_ROWS - 1),
            })
        );
    }

    fn open_pager(app: &mut App) -> Effect {
        app.pager = Some(Pager::new("s-old".to_owned(), "t".to_owned(), 80));
        let mut effects = app.load_more();
        assert_eq!(effects.len(), 1);
        effects.remove(0)
    }

    fn page_reply(effect: Effect, messages: Vec<TranscriptMessage>, truncated: bool) -> Msg {
        let Effect::Fetch {
            generation,
            epoch,
            call,
            ..
        } = effect
        else {
            panic!("{effect:?}");
        };
        Msg {
            generation,
            epoch,
            call,
            reply: Reply::Page(Ok(TranscriptPage {
                messages,
                truncated,
            })),
        }
    }

    #[test]
    fn a_short_truncated_page_is_not_the_end() {
        let mut app = opened(&MockApi::golden(), 100, 30);
        let request = open_pager(&mut app);
        let short = vec![message("m1", now(), "one"), message("m2", now(), "two")];
        let next = app.apply(page_reply(request, short, true));
        let calls = fetches(&next);
        assert!(
            matches!(calls[..], [Call::Page { after: Some(Cursor { message_id, .. }), .. }] if message_id == "m2"),
            "{next:?}"
        );
        assert!(!app.pager.as_ref().unwrap().eof);

        let empty = app.apply(page_reply(
            next.into_iter().next().unwrap(),
            Vec::new(),
            true,
        ));
        assert!(empty.is_empty());
        assert!(
            app.pager.as_ref().unwrap().eof,
            "an empty page cannot advance the cursor"
        );
        assert!(app.toast.as_ref().unwrap().contains("size budget"));
    }

    #[test]
    fn a_huge_single_message_is_wrapped_once_and_sliced() {
        let mut app = opened(&MockApi::golden(), 80, 24);
        let request = open_pager(&mut app);
        let huge = "lorem ipsum dolor ".repeat(40_000);
        app.apply(page_reply(
            request,
            vec![message("m1", now(), &huge)],
            false,
        ));
        let lines = app.pager.as_ref().unwrap().lines.len();
        assert!(lines > 9_000, "{lines}");
        assert!(lines > usize::from(u16::MAX) / 8);
        press(&mut app, &MockApi::golden(), KeyCode::End);
        let screen_text = screen(&mut app);
        assert!(
            screen_text.contains(&format!("line {}/{lines}", lines - 21)),
            "{screen_text}"
        );
    }

    #[test]
    fn ansi_tab_and_crlf_are_cleaned_in_the_pager() {
        let mut app = opened(&MockApi::golden(), 60, 12);
        let request = open_pager(&mut app);
        let rows = sql_rows::<TranscriptMessage>(golden::SQL_PAGE);
        app.apply(page_reply(request, rows, false));
        let screen_text = screen(&mut app);
        assert!(
            screen_text.contains("check open issues   "),
            "{screen_text}"
        );
        assert!(screen_text.contains("red done"), "{screen_text}");
        assert!(!screen_text.contains("[31m"), "{screen_text}");
        assert!(!screen_text.contains("[0m"), "{screen_text}");
    }

    #[test]
    fn tiny_terminals_render_and_a_resize_keeps_the_pager_position() {
        for (width, height) in [(1, 1), (3, 2), (12, 4)] {
            let mut app = opened(&MockApi::golden(), width, height);
            screen(&mut app);
            press(&mut app, &MockApi::golden(), KeyCode::Char(' '));
            screen(&mut app);
            let request = open_pager(&mut app);
            app.apply(page_reply(
                request,
                vec![message("m1", now(), "hello world")],
                false,
            ));
            screen(&mut app);
        }

        let mut app = opened(&MockApi::golden(), 80, 10);
        let request = open_pager(&mut app);
        let messages: Vec<_> = (0..20)
            .map(|i| {
                message(
                    &format!("m{i:02}"),
                    now(),
                    &format!("message {i} {}", "word ".repeat(30)),
                )
            })
            .collect();
        app.apply(page_reply(request, messages, false));
        let target = app.pager.as_ref().unwrap().starts[10];
        app.pager.as_mut().unwrap().offset = target;
        let wide = app.pager.as_ref().unwrap().lines.len();

        app.on_event(&Event::Resize(50, 8));
        app.on_event(&Event::Resize(30, 6));
        assert_eq!(
            app.pager.as_ref().unwrap().lines.len(),
            wide,
            "a resize waits for the next draw to re-wrap"
        );
        let screen_text = screen(&mut app);
        let pager = app.pager.as_ref().unwrap();
        assert!(pager.lines.len() > wide, "re-wrapped narrower");
        assert_eq!(
            pager.offset, pager.starts[10],
            "the same message stays on top"
        );
        assert!(screen_text.contains("message 10"), "{screen_text}");
    }

    #[test]
    fn widening_fetches_more_once_the_rewrap_runs_short() {
        let mut app = opened(&MockApi::golden(), 30, 10);
        let width = usize::from(app.pager_viewport().width);
        app.pager = Some(Pager::new("s-old".to_owned(), "t".to_owned(), width));
        let request = app.load_more().remove(0);
        let messages: Vec<_> = (0..3)
            .map(|i| message(&format!("m{i}"), now(), &"word ".repeat(60)))
            .collect();
        assert!(app.apply(page_reply(request, messages, true)).is_empty());

        let resized = app.on_event(&Event::Resize(200, 10));
        assert!(fetches(&resized).is_empty(), "{resized:?}");
        let relaid = app.relayout();
        assert!(
            matches!(fetches(&relaid)[..], [Call::Page { after: Some(_), .. }]),
            "{relaid:?}"
        );
    }

    #[test]
    fn the_pager_frame_is_the_viewport_slice() {
        let mut app = app(40, 6);
        let mut pager = Pager::new("s-old".to_owned(), "fix it".to_owned(), 39);
        pager.append(vec![
            message("m1", now(), "one\ntwo"),
            TranscriptMessage {
                role: "assistant".to_owned(),
                ..message("m2", now(), "three")
            },
        ]);
        pager.offset = 1;
        pager.eof = true;
        app.pager = Some(pager);
        let mut terminal = Terminal::new(TestBackend::new(40, 6)).unwrap();
        terminal.draw(|frame| ui::render(frame, &mut app)).unwrap();
        let mut frame = terminal.backend().buffer().clone();
        frame.set_style(frame.area, Style::reset());
        assert_eq!(
            frame,
            Buffer::with_lines([
                "fix it | s-old                          ",
                "one                                    ▲",
                "two                                    █",
                "                                       ║",
                "assistant 2026-09-25 05:00:00 UTC      ▼",
                "conversation only - tool bodies via pond",
            ])
        );
    }
}
