//! The session desk TUI: a runtime that owns the terminal and performs the
//! effects of the pure reducers in `app`, and `ui` to draw their state.

mod app;
mod ui;

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use crossterm::event::{Event, EventStream};
use futures_util::{Stream, StreamExt};
use ratatui::Terminal;
use ratatui::backend::Backend;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use self::app::{App, Call, Effect, Lane, Msg, Reply};
use crate::types::{Api, DeskContext, DeskExit};

const SPINNER_TICK: Duration = Duration::from_millis(100);
/// How long exit waits for in-flight blocking calls (herdr's CLI) to finish.
const EXIT_GRACE: Duration = Duration::from_millis(500);

/// Builds its own current-thread runtime and owns the terminal until it
/// returns; the terminal is restored on every return path, before the api -
/// and any fallback serve it owns, whose teardown blocks - is dropped.
pub(crate) fn run(api: Arc<dyn Api>, context: DeskContext) -> anyhow::Result<DeskExit> {
    let runtime = crate::runtime()?;
    let mut terminal = ratatui::try_init().inspect_err(|_| ratatui::restore())?;
    let result = runtime.block_on(async {
        let shutdown = crate::shutdown_signal()?;
        let shutdown = async {
            shutdown.await;
        };
        event_loop(
            &mut terminal,
            Arc::clone(&api),
            context,
            EventStream::new(),
            shutdown,
        )
        .await
    });
    ratatui::restore();
    runtime.shutdown_timeout(EXIT_GRACE);
    drop(api);
    result
}

/// Ends on quit, jump, `shutdown`, or the event stream ending - a closed or
/// failing stream means the pane is gone.
async fn event_loop<B, S>(
    terminal: &mut Terminal<B>,
    api: Arc<dyn Api>,
    context: DeskContext,
    mut events: S,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<DeskExit>
where
    B: Backend,
    B::Error: Send + Sync + 'static,
    S: Stream<Item = io::Result<Event>> + Unpin,
{
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut runner = Runner::new(api, tx);
    let mut app = App::new(context, Utc::now(), terminal.size()?);
    let mut spinner = tokio::time::interval(SPINNER_TICK);
    spinner.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(shutdown);
    let mut effects = app.start();
    loop {
        for effect in effects.drain(..) {
            if let Some(exit) = runner.perform(effect) {
                return Ok(exit);
            }
        }
        if app.dirty {
            terminal.draw(|frame| ui::render(frame, &mut app))?;
            app.dirty = false;
        }
        effects = tokio::select! {
            event = events.next() => match event {
                Some(Ok(event)) => {
                    app.now = Utc::now();
                    app.on_event(&event)
                }
                Some(Err(_)) | None => return Ok(DeskExit::Quit),
            },
            Some(msg) = rx.recv() => app.apply(msg),
            _ = spinner.tick(), if app.spinner_visible() => {
                app.tick();
                Vec::new()
            }
            () = &mut shutdown => return Ok(DeskExit::Quit),
        };
    }
}

/// Performs effects: one task per lane, a new fetch aborting the old one.
struct Runner {
    api: Arc<dyn Api>,
    tx: mpsc::UnboundedSender<Msg>,
    tasks: [Option<AbortHandle>; Lane::COUNT],
}

impl Runner {
    fn new(api: Arc<dyn Api>, tx: mpsc::UnboundedSender<Msg>) -> Self {
        Self {
            api,
            tx,
            tasks: Default::default(),
        }
    }

    fn abort(&mut self, lane: Lane) {
        if let Some(task) = self.tasks[lane as usize].take() {
            task.abort();
        }
    }

    fn perform(&mut self, effect: Effect) -> Option<DeskExit> {
        match effect {
            Effect::Exit(exit) => return Some(exit),
            Effect::Cancel(lane) => self.abort(lane),
            Effect::Fetch {
                generation,
                epoch,
                delay,
                call,
            } => {
                let lane = call.lane();
                self.abort(lane);
                let api = Arc::clone(&self.api);
                let tx = self.tx.clone();
                let debounced = tokio::time::Instant::now() + delay;
                let task = tokio::spawn(async move {
                    tokio::time::sleep_until(debounced).await;
                    let reply = call_api(api.as_ref(), &call).await;
                    let _ = tx.send(Msg {
                        generation,
                        epoch,
                        call,
                        reply,
                    });
                });
                self.tasks[lane as usize] = Some(task.abort_handle());
            }
        }
        None
    }
}

async fn call_api(api: &dyn Api, call: &Call) -> Reply {
    match call.clone() {
        Call::Listing(scope) => Reply::Listing(api.list_sessions(scope).await),
        Call::Hydrate(ids) => Reply::Hydrate(api.hydrate(ids).await),
        Call::Live => Reply::Live(api.live_agents().await),
        Call::Search(request) => Reply::Search(api.search(request).await),
        Call::Preview(id) => Reply::Preview(api.preview(id).await),
        Call::Page { session_id, after } => Reply::Page(api.page(session_id, after).await),
    }
}

#[cfg(test)]
pub(super) mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::Mutex;

    use chrono::{DateTime, TimeDelta};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use futures_util::FutureExt;
    use futures_util::stream;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Size;
    use serde::de::DeserializeOwned;

    use super::*;
    use crate::fake_pond::golden;
    use crate::types::{
        ApiError, ApiFuture, Cursor, ListingScope, LiveAgent, PAGE_ROWS, PREVIEW_ROWS,
        SearchRequest, SearchResponse, SessionDetail, SessionRow, SqlResponse, TranscriptMessage,
        TranscriptPage,
    };

    pub(in crate::desk) fn now() -> DateTime<Utc> {
        "2026-09-25T05:00:00Z".parse().unwrap()
    }

    pub(in crate::desk) fn sql_rows<T: DeserializeOwned>(body: &str) -> Vec<T> {
        serde_json::from_str::<SqlResponse>(body)
            .unwrap()
            .into_rows()
            .unwrap()
    }

    pub(in crate::desk) fn message(id: &str, ts: DateTime<Utc>, text: &str) -> TranscriptMessage {
        TranscriptMessage {
            message_id: id.to_owned(),
            timestamp: ts,
            role: "user".to_owned(),
            text: text.to_owned(),
        }
    }

    /// Canned data behind the real trait: records every call, seeks pages by
    /// `(timestamp, message_id)` like the SQL does, and can delay replies.
    #[derive(Default)]
    pub(in crate::desk) struct MockApi {
        pub(in crate::desk) sessions: Vec<SessionRow>,
        pub(in crate::desk) details: Vec<SessionDetail>,
        pub(in crate::desk) transcript: Vec<TranscriptMessage>,
        pub(in crate::desk) live: Vec<LiveAgent>,
        pub(in crate::desk) listing_error: Option<ApiError>,
        pub(in crate::desk) search: Option<SearchResponse>,
        pub(in crate::desk) search_delay: Option<fn(&str) -> Duration>,
        pub(in crate::desk) calls: Mutex<Vec<Call>>,
    }

    impl MockApi {
        /// The golden listing and hydration, with `s-live` running in pane `p7`.
        pub(in crate::desk) fn golden() -> Self {
            Self {
                sessions: sql_rows(golden::SQL_LISTING),
                details: sql_rows(golden::SQL_HYDRATE),
                transcript: sql_rows(golden::SQL_PAGE),
                live: vec![LiveAgent {
                    pane_id: "p7".to_owned(),
                    session: "s-live".to_owned(),
                }],
                ..Self::default()
            }
        }

        pub(in crate::desk) fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }

        fn reply<T: Send + 'static>(
            &self,
            call: Call,
            delay: Duration,
            result: Result<T, ApiError>,
        ) -> ApiFuture<'_, T> {
            self.calls.lock().unwrap().push(call);
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                result
            })
        }
    }

    impl Api for MockApi {
        fn list_sessions(&self, scope: ListingScope) -> ApiFuture<'_, Vec<SessionRow>> {
            let result = self
                .listing_error
                .clone()
                .map_or_else(|| Ok(self.sessions.clone()), Err);
            self.reply(Call::Listing(scope), Duration::ZERO, result)
        }

        fn hydrate(&self, session_ids: Vec<String>) -> ApiFuture<'_, Vec<SessionDetail>> {
            let details = self
                .details
                .iter()
                .filter(|d| session_ids.contains(&d.session_id))
                .cloned()
                .collect();
            self.reply(Call::Hydrate(session_ids), Duration::ZERO, Ok(details))
        }

        fn search(&self, request: SearchRequest) -> ApiFuture<'_, SearchResponse> {
            let delay = self
                .search_delay
                .map_or(Duration::ZERO, |delay| delay(&request.query));
            let response = self.search.clone().unwrap_or_else(|| SearchResponse {
                sessions: Vec::new(),
                searchable_in_scope: 100,
            });
            self.reply(Call::Search(request), delay, Ok(response))
        }

        fn preview(&self, session_id: String) -> ApiFuture<'_, Vec<TranscriptMessage>> {
            let newest: Vec<_> = self
                .transcript
                .iter()
                .rev()
                .take(PREVIEW_ROWS)
                .cloned()
                .collect();
            self.reply(Call::Preview(session_id), Duration::ZERO, Ok(newest))
        }

        fn page(&self, session_id: String, after: Option<Cursor>) -> ApiFuture<'_, TranscriptPage> {
            let mut sorted = self.transcript.clone();
            sorted.sort_by(|a, b| (a.timestamp, &a.message_id).cmp(&(b.timestamp, &b.message_id)));
            let messages = sorted
                .into_iter()
                .filter(|m| {
                    after
                        .as_ref()
                        .is_none_or(|c| (m.timestamp, &m.message_id) > (c.timestamp, &c.message_id))
                })
                .take(PAGE_ROWS)
                .collect();
            let page = TranscriptPage {
                messages,
                truncated: false,
            };
            self.reply(Call::Page { session_id, after }, Duration::ZERO, Ok(page))
        }

        fn live_agents(&self) -> ApiFuture<'_, Vec<LiveAgent>> {
            self.reply(Call::Live, Duration::ZERO, Ok(self.live.clone()))
        }
    }

    pub(in crate::desk) fn app(width: u16, height: u16) -> App {
        let context = DeskContext {
            project: Some("/home/me/pj/pond".to_owned()),
        };
        App::new(context, now(), Size::new(width, height))
    }

    /// Performs effects synchronously against an undelayed mock, feeding
    /// every reply back through `apply` until nothing is left in flight.
    pub(in crate::desk) fn settle(
        app: &mut App,
        api: &MockApi,
        effects: Vec<Effect>,
    ) -> Option<DeskExit> {
        let mut queue = VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::Fetch {
                    generation,
                    epoch,
                    call,
                    ..
                } => {
                    let reply = call_api(api, &call).now_or_never().expect("undelayed mock");
                    queue.extend(app.apply(Msg {
                        generation,
                        epoch,
                        call,
                        reply,
                    }));
                }
                Effect::Cancel(_) => {}
                Effect::Exit(exit) => return Some(exit),
            }
        }
        None
    }

    pub(in crate::desk) fn key(code: KeyCode) -> Event {
        Event::Key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    pub(in crate::desk) fn press(app: &mut App, api: &MockApi, code: KeyCode) -> Option<DeskExit> {
        let effects = app.on_event(&key(code));
        settle(app, api, effects)
    }

    pub(in crate::desk) fn screen(app: &mut App) -> String {
        let mut terminal =
            Terminal::new(TestBackend::new(app.size.width, app.size.height)).unwrap();
        terminal.draw(|frame| ui::render(frame, app)).unwrap();
        buffer_text(terminal.backend())
    }

    fn buffer_text(backend: &TestBackend) -> String {
        let buffer = backend.buffer();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn searches(api: &MockApi) -> Vec<String> {
        api.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Search(request) => Some(request.query),
                _ => None,
            })
            .collect()
    }

    fn perform_all(runner: &mut Runner, effects: Vec<Effect>) {
        for effect in effects {
            assert_eq!(runner.perform(effect), None);
        }
    }

    async fn drain(app: &mut App, runner: &mut Runner, rx: &mut mpsc::UnboundedReceiver<Msg>) {
        tokio::task::yield_now().await;
        while let Ok(msg) = rx.try_recv() {
            let effects = app.apply(msg);
            perform_all(runner, effects);
            tokio::task::yield_now().await;
        }
    }

    fn type_text(app: &mut App, runner: &mut Runner, text: &str) {
        for c in text.chars() {
            let effects = app.on_event(&key(KeyCode::Char(c)));
            perform_all(runner, effects);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn search_is_debounced() {
        let api = Arc::new(MockApi::golden());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut runner = Runner::new(Arc::clone(&api) as Arc<dyn Api>, tx);
        let mut app = app(100, 20);
        let effects = app.start();
        perform_all(&mut runner, effects);
        drain(&mut app, &mut runner, &mut rx).await;

        type_text(&mut app, &mut runner, "/tim");
        tokio::time::advance(Duration::from_millis(100)).await;
        type_text(&mut app, &mut runner, "er");
        tokio::time::advance(Duration::from_millis(149)).await;
        drain(&mut app, &mut runner, &mut rx).await;
        assert!(
            searches(&api).is_empty(),
            "fired inside the debounce window"
        );

        tokio::time::advance(Duration::from_millis(2)).await;
        drain(&mut app, &mut runner, &mut rx).await;
        assert_eq!(searches(&api), ["timer"]);
        assert!(app.search.as_ref().unwrap().response.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_search_is_cancelled_by_a_newer_query() {
        let api = Arc::new(MockApi {
            search_delay: Some(|query| {
                if query == "a" {
                    Duration::from_secs(20)
                } else {
                    Duration::from_millis(10)
                }
            }),
            ..MockApi::golden()
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut runner = Runner::new(Arc::clone(&api) as Arc<dyn Api>, tx);
        let mut app = app(100, 20);

        type_text(&mut app, &mut runner, "/a");
        tokio::time::advance(Duration::from_millis(200)).await;
        tokio::task::yield_now().await;
        assert_eq!(searches(&api), ["a"], "the slow request reached the server");

        type_text(&mut app, &mut runner, "b");
        tokio::time::sleep(Duration::from_secs(30)).await;
        let mut delivered = Vec::new();
        tokio::task::yield_now().await;
        while let Ok(msg) = rx.try_recv() {
            if let Call::Search(request) = &msg.call {
                delivered.push(request.query.clone());
            }
            app.apply(msg);
        }
        assert_eq!(searches(&api), ["a", "ab"]);
        assert_eq!(delivered, ["ab"], "the aborted request never delivered");
        assert_eq!(app.search.as_ref().unwrap().query, "ab");
        assert!(!app.lane_loading(Lane::Search));
    }

    async fn run_loop(
        api: MockApi,
        events: impl Stream<Item = io::Result<Event>> + Send + 'static,
        shutdown: impl Future<Output = ()>,
    ) -> (anyhow::Result<DeskExit>, String) {
        let mut terminal = Terminal::new(TestBackend::new(100, 12)).unwrap();
        let events: Pin<Box<dyn Stream<Item = io::Result<Event>> + Send>> = Box::pin(events);
        let exit = event_loop(
            &mut terminal,
            Arc::new(api),
            DeskContext::default(),
            events,
            shutdown,
        )
        .await;
        (exit, buffer_text(terminal.backend()))
    }

    #[tokio::test]
    async fn event_stream_eof_or_error_quits() {
        let (exit, screen) =
            run_loop(MockApi::golden(), stream::empty(), std::future::pending()).await;
        assert_eq!(exit.unwrap(), DeskExit::Quit);
        assert!(screen.contains("pond desk"));

        let failing = stream::iter([Err(io::Error::other("tty closed"))]);
        let (exit, _) = run_loop(MockApi::golden(), failing, std::future::pending()).await;
        assert_eq!(exit.unwrap(), DeskExit::Quit);
    }

    #[tokio::test]
    async fn shutdown_signal_quits() {
        let (exit, _) = run_loop(MockApi::golden(), stream::pending(), async {}).await;
        assert_eq!(exit.unwrap(), DeskExit::Quit);
    }

    #[tokio::test(start_paused = true)]
    async fn enter_on_a_live_row_returns_the_jump() {
        let keys = stream::iter([Ok(key(KeyCode::Enter))]).then(|event| async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            event
        });
        let (exit, screen) = run_loop(
            MockApi::golden(),
            keys.chain(stream::pending()),
            std::future::pending(),
        )
        .await;
        assert_eq!(
            exit.unwrap(),
            DeskExit::Jump {
                pane_id: "p7".to_owned()
            }
        );
        assert!(screen.contains("fix the timer re-arm"), "{screen}");
    }

    #[test]
    fn listing_window_is_fourteen_days() {
        let mut app = app(100, 20);
        let effects = app.start();
        let Some(Effect::Fetch {
            call: Call::Listing(scope),
            ..
        }) = effects.first()
        else {
            panic!("the desk opens with a listing: {effects:?}");
        };
        assert_eq!(scope.since, Some(now() - TimeDelta::days(14)));
        assert_eq!(scope.project.as_deref(), Some("/home/me/pj/pond"));
    }
}
