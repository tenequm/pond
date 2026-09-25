//! The HTTP [`Api`] implementation over `pond serve`'s Unix socket
//! (`/v1/x/sql`, `/v1/search`), plus herdr's pane list for live agents.
//! Tested against [`crate::fake_pond`].

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::herdr::{self, Herdr};
use crate::serve::{self, Fallback, Origin};
use crate::types::{
    Api, ApiError, ApiFuture, Cursor, ErrorEnvelope, ListingScope, LiveAgent, PAGE_ROWS,
    PREVIEW_ROWS, SearchRequest, SearchResponse, SessionHost, SessionRow, SessionStart,
    SessionStats, SessionTitle, SqlRequest, SqlResponse, TranscriptMessage, TranscriptPage,
    hosts_sql, listing_sql, page_sql, preview_sql, stats_sql, titles_sql,
};

pub(crate) const SQL_PATH: &str = "/v1/x/sql";
pub(crate) const SEARCH_PATH: &str = "/v1/search";

/// The host names only the `Host` header: `/v1/x/sql` answers a loopback one.
const BASE_URL: &str = "http://localhost";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Server-side execution budgets, sent as `timeout_seconds`. The client waits
/// [`CLIENT_SLACK`] longer so pond's enriched timeout error arrives instead of
/// a bare client-side timeout.
pub(crate) const QUERY_TIMEOUT_SECS: u64 = 25;
const ALL_TIME_TIMEOUT_SECS: u64 = 60;
const CLIENT_SLACK: Duration = Duration::from_secs(5);
pub(crate) const SEARCH_DEADLINE: Duration = Duration::from_secs(30);

pub(crate) fn sql_deadline(timeout_seconds: u64) -> Duration {
    Duration::from_secs(timeout_seconds) + CLIENT_SLACK
}

/// One `pond serve --socket` path and a client bound to it.
#[derive(Debug, Clone)]
pub(crate) struct Socket {
    pub path: PathBuf,
    client: reqwest::Client,
}

impl Socket {
    pub(crate) fn new(path: PathBuf) -> Result<Self, ApiError> {
        let client = reqwest::Client::builder()
            .unix_socket(path.as_path())
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|error| {
                ApiError::Request(format!("no HTTP client for {}: {error}", path.display()))
            })?;
        Ok(Self { path, client })
    }

    pub(crate) async fn post<B, T>(
        &self,
        route: &str,
        body: &B,
        deadline: Duration,
    ) -> Result<T, ApiError>
    where
        B: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let response = self
            .client
            .post(format!("{BASE_URL}{route}"))
            .json(body)
            .timeout(deadline)
            .send()
            .await
            .map_err(|error| self.transport(&error))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|error| self.transport(&error))?;
        decode(route, status, &body)
    }

    /// Only a failed connect (a missing or refusing socket) proves the serve
    /// gone; a timeout or a dropped response may come from a live serve that
    /// is merely slow. reqwest's own message is only "error sending request"
    /// and names no socket, so both are added.
    fn transport(&self, error: &reqwest::Error) -> ApiError {
        let mut message = format!("{}: {error}", self.path.display());
        let mut source = std::error::Error::source(error);
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        if error.is_connect() {
            ApiError::Unreachable(message)
        } else {
            ApiError::Request(message)
        }
    }
}

fn decode<T: DeserializeOwned>(path: &str, status: u16, body: &str) -> Result<T, ApiError> {
    let decoded = (200..300)
        .contains(&status)
        .then(|| serde_json::from_str::<T>(body));
    if let Some(Ok(value)) = decoded {
        return Ok(value);
    }
    if let Ok(ErrorEnvelope { error }) = serde_json::from_str(body) {
        return Err(ApiError::Pond {
            code: error.code,
            message: error.message,
        });
    }
    match decoded {
        Some(Err(error)) => Err(ApiError::Decode(format!("{path}: {error}"))),
        _ if status == 404 && path == SQL_PATH => Err(ApiError::PondTooOld),
        _ => Err(ApiError::Rejected {
            status,
            body: body.trim().to_owned(),
        }),
    }
}

/// How long a failed resolution answers for later callers instead of a new
/// attempt, so callers queued behind it do not each spawn another serve.
const RESOLVE_RETRY_AFTER: Duration = Duration::from_secs(1);

/// The resolved serve. `serve` stays unset until the first call, so the
/// desk's loading state covers a cold fallback spawn.
#[derive(Default)]
struct Link {
    serve: Option<Socket>,
    fallback: Option<Fallback>,
    failed: Option<(Instant, ApiError)>,
}

/// Owned by the api and by each resolution task, so resolution survives the
/// request that started it: the desk aborts a lane on every new fetch, and a
/// cancelled resolution would kill a half-open fallback serve mid store-open.
struct Resolver {
    /// Why no serve can be found at all (not running under herdr), reported
    /// on first use rather than before the desk can draw.
    origin: Result<Origin, String>,
    link: Mutex<Link>,
}

impl Resolver {
    /// Runs with `link` locked, so concurrent callers queue behind one
    /// resolution and then reuse its result. `stale` is a socket that just
    /// refused; the same path is used again only once `connect` probed it
    /// live, since a successor owner reuses its path.
    async fn resolve(&self, stale: Option<PathBuf>) -> Result<Socket, ApiError> {
        let mut link = self.link.lock().await;
        if let Some(current) = &link.serve
            && stale.as_ref().is_none_or(|stale| current.path != *stale)
        {
            return Ok(current.clone());
        }
        if let Some((at, error)) = &link.failed
            && at.elapsed() < RESOLVE_RETRY_AFTER
        {
            return Err(error.clone());
        }
        let origin = self
            .origin
            .as_ref()
            .map_err(|error| ApiError::Unreachable(error.clone()))?;
        match serve::connect(origin, link.fallback.take()).await {
            Ok(connection) => {
                link.fallback = connection.fallback;
                link.serve = Some(connection.socket.clone());
                link.failed = None;
                Ok(connection.socket)
            }
            Err(error) => {
                link.serve = None;
                link.failed = Some((Instant::now(), error.clone()));
                Err(error)
            }
        }
    }
}

pub(crate) struct HttpApi {
    resolver: Arc<Resolver>,
    herdr: Herdr,
}

impl HttpApi {
    pub(crate) fn from_env() -> Self {
        Self {
            resolver: Arc::new(Resolver {
                origin: Origin::from_env().map_err(|error| format!("{error:#}")),
                link: Mutex::default(),
            }),
            herdr: Herdr::from_env(),
        }
    }

    /// The current serve, else a resolution run in its own task.
    async fn resolve(&self, stale: Option<PathBuf>) -> Result<Socket, ApiError> {
        if stale.is_none() {
            let current = self.resolver.link.lock().await.serve.clone();
            if let Some(socket) = current {
                return Ok(socket);
            }
        }
        let resolver = Arc::clone(&self.resolver);
        tokio::spawn(async move { resolver.resolve(stale).await })
            .await
            .map_err(|error| ApiError::Unreachable(format!("resolving pond serve: {error}")))?
    }

    /// Sends to the resolved serve. When the serve refuses the connection,
    /// the endpoint is resolved again (daemon record, else a fallback child)
    /// and the request retried there once; a refusal on the retry stands as
    /// this call's error, and the next call may fail over again.
    async fn post<B, T>(&self, route: &str, body: &B, deadline: Duration) -> Result<T, ApiError>
    where
        B: Serialize + ?Sized + Sync,
        T: DeserializeOwned,
    {
        let socket = self.resolve(None).await?;
        match socket.post(route, body, deadline).await {
            Err(ApiError::Unreachable(_)) => {
                self.resolve(Some(socket.path))
                    .await?
                    .post(route, body, deadline)
                    .await
            }
            other => other,
        }
    }

    async fn sql(
        &self,
        query: String,
        limit: usize,
        timeout_seconds: u64,
    ) -> Result<SqlResponse, ApiError> {
        let request = SqlRequest::new(query, limit, timeout_seconds);
        self.post(SQL_PATH, &request, sql_deadline(timeout_seconds))
            .await
    }

    /// A page-scoped query answering at most one row per session.
    async fn per_session<T: DeserializeOwned>(
        &self,
        sessions: usize,
        query: impl FnOnce() -> String,
    ) -> Result<Vec<T>, ApiError> {
        if sessions == 0 {
            return Ok(Vec::new());
        }
        self.sql(query(), sessions, QUERY_TIMEOUT_SECS)
            .await?
            .into_rows()
    }
}

impl Api for HttpApi {
    fn list_sessions(&self, scope: ListingScope) -> ApiFuture<'_, Vec<SessionRow>> {
        Box::pin(async move {
            let timeout = if scope.since.is_none() {
                ALL_TIME_TIMEOUT_SECS
            } else {
                QUERY_TIMEOUT_SECS
            };
            self.sql(listing_sql(&scope), scope.limit, timeout)
                .await?
                .into_rows()
        })
    }

    fn titles(&self, session_ids: Vec<String>) -> ApiFuture<'_, Vec<SessionTitle>> {
        Box::pin(async move {
            self.per_session(session_ids.len(), || titles_sql(&session_ids))
                .await
        })
    }

    fn stats(&self, session_ids: Vec<String>) -> ApiFuture<'_, Vec<SessionStats>> {
        Box::pin(async move {
            self.per_session(session_ids.len(), || stats_sql(&session_ids))
                .await
        })
    }

    fn hosts(&self, starts: Vec<SessionStart>) -> ApiFuture<'_, Vec<SessionHost>> {
        Box::pin(async move { self.per_session(starts.len(), || hosts_sql(&starts)).await })
    }

    fn search(&self, request: SearchRequest) -> ApiFuture<'_, SearchResponse> {
        Box::pin(async move { self.post(SEARCH_PATH, &request, SEARCH_DEADLINE).await })
    }

    fn preview(&self, session_id: String) -> ApiFuture<'_, Vec<TranscriptMessage>> {
        Box::pin(async move {
            let query = preview_sql(&session_id);
            self.sql(query, PREVIEW_ROWS, QUERY_TIMEOUT_SECS)
                .await?
                .into_rows()
        })
    }

    fn page(&self, session_id: String, after: Option<Cursor>) -> ApiFuture<'_, TranscriptPage> {
        Box::pin(async move {
            let query = page_sql(&session_id, after.as_ref());
            let response = self.sql(query, PAGE_ROWS, QUERY_TIMEOUT_SECS).await?;
            Ok(TranscriptPage {
                truncated: response.truncated,
                messages: response.into_rows()?,
            })
        })
    }

    fn live_agents(&self) -> ApiFuture<'_, Vec<LiveAgent>> {
        let herdr = self.herdr.clone();
        Box::pin(async move {
            let panes = tokio::task::spawn_blocking(move || herdr.pane_list(None))
                .await
                .map_err(|error| ApiError::Herdr(format!("pane list: {error}")))?
                .map_err(|error| ApiError::Herdr(format!("{error:#}")))?;
            Ok(herdr::live_agents(panes))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::fake_pond::{
        FakePond, Reply, Sandbox, endpoint, golden, missing_socket, stale_socket, ts, write_script,
    };
    use crate::serve::write_endpoint;
    use crate::types::READY_SQL;

    /// An api resolving through `sandbox`'s state, pinned to `serve` if given.
    fn api(sandbox: &Sandbox, serve: Option<Socket>) -> HttpApi {
        HttpApi {
            resolver: Arc::new(Resolver {
                origin: Ok(sandbox.origin()),
                link: Mutex::new(Link {
                    serve,
                    ..Link::default()
                }),
            }),
            herdr: Herdr::new(sandbox.path("bin/herdr")),
        }
    }

    /// Pinned to `serve`, with a `pond_bin` that points nowhere, so no
    /// re-resolution can reach a real pond.
    fn api_at(serve: Socket, sandbox: &Sandbox) -> HttpApi {
        sandbox.write_config(&format!(
            "pond_bin = \"{}\"\n",
            sandbox.path("bin/no-pond").display()
        ));
        api(sandbox, Some(serve))
    }

    /// Answers the probe and the preview query.
    async fn preview_pond() -> FakePond {
        FakePond::with_sql(
            vec![
                ("SELECT 1", Reply::json(golden::SQL_READY)),
                ("DESC LIMIT", Reply::json(golden::SQL_PAGE)),
            ],
            Reply::json(golden::SEARCH),
        )
        .await
    }

    fn sent(pond: &FakePond) -> Vec<serde_json::Value> {
        pond.recorded()
            .iter()
            .map(|request| serde_json::from_str(&request.body).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn listing_sends_its_sql_and_limit() {
        let sandbox = Sandbox::new();
        let pond = FakePond::with_sql(
            vec![("GROUP BY session_id", Reply::json(golden::SQL_LISTING))],
            Reply::json(golden::SEARCH),
        )
        .await;
        let api = api_at(pond.connect(), &sandbox);
        let scope = ListingScope {
            project: Some("/home/me/pj/pond".to_owned()),
            since: Some(ts("2026-09-11T00:00:00Z")),
            limit: 200,
        };
        let rows = api.list_sessions(scope.clone()).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].source_agent, "codex-cli");

        let all_time = ListingScope {
            since: None,
            ..scope.clone()
        };
        api.list_sessions(all_time.clone()).await.unwrap();

        let recorded = pond.recorded();
        assert!(recorded.iter().all(|request| request.path == SQL_PATH));
        assert!(recorded.iter().all(|request| request.host == "localhost"));
        let bodies = sent(&pond);
        assert_eq!(bodies[0]["query"], listing_sql(&scope));
        assert_eq!(bodies[0]["limit"], 200);
        assert_eq!(bodies[0]["protocol_version"], 1);
        assert_eq!(bodies[0]["timeout_seconds"], QUERY_TIMEOUT_SECS);
        assert_eq!(bodies[1]["query"], listing_sql(&all_time));
        assert_eq!(bodies[1]["timeout_seconds"], ALL_TIME_TIMEOUT_SECS);
    }

    #[tokio::test]
    async fn hydration_sends_three_bounded_queries_and_reads_omitted_nulls_as_none() {
        let sandbox = Sandbox::new();
        let pond = FakePond::with_sql(
            vec![
                ("AS title", Reply::json(golden::SQL_TITLES)),
                ("AS first_ts", Reply::json(golden::SQL_STATS)),
                ("AS host", Reply::json(golden::SQL_HOSTS)),
            ],
            Reply::json(golden::SEARCH),
        )
        .await;
        let api = api_at(pond.connect(), &sandbox);
        assert!(api.titles(Vec::new()).await.unwrap().is_empty());
        assert!(api.stats(Vec::new()).await.unwrap().is_empty());
        assert!(api.hosts(Vec::new()).await.unwrap().is_empty());
        assert!(pond.recorded().is_empty(), "empty hydration sent a request");

        let ids = vec!["s-live".to_owned(), "s-old".to_owned()];
        let starts = vec![SessionStart {
            session_id: "s-live".to_owned(),
            first_ts: ts("2026-09-24T21:10:00Z"),
        }];
        let (titles, stats, hosts) = tokio::join!(
            api.titles(ids.clone()),
            api.stats(ids.clone()),
            api.hosts(starts.clone())
        );
        assert_eq!(
            titles.unwrap()[0].title.as_deref(),
            Some("fix the timer re-arm")
        );
        assert_eq!(stats.unwrap()[1].message_count, 3);
        assert_eq!(hosts.unwrap()[1].host, None);

        let mut bodies = sent(&pond);
        bodies.sort_by_key(|body| body["query"].as_str().unwrap().to_owned());
        let mut expected = [
            (titles_sql(&ids), 2),
            (stats_sql(&ids), 2),
            (hosts_sql(&starts), 1),
        ];
        expected.sort();
        for (body, (query, limit)) in bodies.iter().zip(expected) {
            assert_eq!(body["query"], query);
            assert_eq!(body["limit"], limit);
        }
    }

    #[tokio::test]
    async fn preview_and_page_carry_their_limits_and_truncation() {
        let sandbox = Sandbox::new();
        let truncated = golden::SQL_PAGE.replace(r#""truncated":false"#, r#""truncated":true"#);
        let pond = FakePond::with_sql(
            vec![
                ("DESC LIMIT", Reply::json(golden::SQL_EMPTY)),
                ("message_id > 'm-a'", Reply::json(&truncated)),
                (
                    "ORDER BY timestamp, message_id",
                    Reply::json(golden::SQL_PAGE),
                ),
            ],
            Reply::json(golden::SEARCH),
        )
        .await;
        let api = api_at(pond.connect(), &sandbox);
        assert!(api.preview("s1".to_owned()).await.unwrap().is_empty());

        let first = api.page("s1".to_owned(), None).await.unwrap();
        assert!(!first.truncated);
        assert_eq!(first.messages.len(), 2);
        assert!(first.messages[0].text.contains("\u{1b}[31m"));

        let cursor = Cursor::after(&first.messages[0]);
        let next = api
            .page("s1".to_owned(), Some(cursor.clone()))
            .await
            .unwrap();
        assert!(next.truncated);

        let bodies = sent(&pond);
        assert_eq!(bodies[0]["query"], preview_sql("s1"));
        assert_eq!(bodies[0]["limit"], PREVIEW_ROWS);
        assert_eq!(bodies[1]["limit"], PAGE_ROWS);
        assert_eq!(bodies[2]["query"], page_sql("s1", Some(&cursor)));
    }

    #[tokio::test]
    async fn search_posts_the_wire_request() {
        let sandbox = Sandbox::new();
        let pond = FakePond::with_sql(Vec::new(), Reply::json(golden::SEARCH_OUT_OF_SCOPE)).await;
        let api = api_at(pond.connect(), &sandbox);
        let scope = ListingScope {
            project: Some("/pj/pond".to_owned()),
            since: None,
            limit: 1,
        };
        let response = api
            .search(SearchRequest::new("timer".to_owned(), 20).within(&scope))
            .await
            .unwrap();
        assert_eq!(response.searchable_in_scope, 0);
        assert_eq!(pond.recorded()[0].path, SEARCH_PATH);
        assert_eq!(
            sent(&pond)[0],
            serde_json::json!({
                "protocol_version": 1,
                "query": "timer",
                "filters": {"project": {"contains": "/pj/pond"}},
                "limit": 20
            })
        );
    }

    #[tokio::test]
    async fn every_failure_shape_maps_to_its_error() {
        let sandbox = Sandbox::new();
        let pond = FakePond::start(|path, body| match (path, body) {
            (SEARCH_PATH, _) => Reply::plain(422, golden::AXUM_REJECTION),
            (_, body) if body.contains("preview_error") => Reply::status(400, golden::SQL_ERROR),
            (_, body) if body.contains("s-bad") => Reply::json(r#"{"columns":[]}"#),
            _ => Reply::plain(404, ""),
        })
        .await;
        let api = api_at(pond.connect(), &sandbox);

        let Err(ApiError::Pond { code, message }) = api.preview("preview_error".to_owned()).await
        else {
            panic!("expected a pond envelope error");
        };
        assert_eq!(code, "validation_failed");
        assert!(message.starts_with("sql error: query exceeded the 30s limit"));

        let rejected = api.search(SearchRequest::new("x".to_owned(), 1)).await;
        assert_eq!(
            rejected.unwrap_err(),
            ApiError::Rejected {
                status: 422,
                body: golden::AXUM_REJECTION.to_owned()
            }
        );

        assert!(matches!(
            api.preview("s-bad".to_owned()).await,
            Err(ApiError::Decode(_))
        ));
        assert_eq!(
            api.preview("other".to_owned()).await,
            Err(ApiError::PondTooOld)
        );
    }

    #[tokio::test]
    async fn a_timeout_is_an_error_without_failover() {
        let sandbox = Sandbox::new();
        let stalled =
            FakePond::start(|_, _| Reply::json(golden::SQL_EMPTY).delayed(Duration::from_secs(5)))
                .await;
        let ready = preview_pond().await;
        write_endpoint(
            &sandbox.origin().dir.endpoint(),
            &endpoint(&ready.socket, "t"),
        )
        .unwrap();
        let api = api_at(stalled.connect(), &sandbox);
        let request = SqlRequest::new(preview_sql("s"), PREVIEW_ROWS, 1);
        let result: Result<SqlResponse, _> = api
            .post(SQL_PATH, &request, Duration::from_millis(200))
            .await;
        let Err(ApiError::Request(reason)) = result else {
            panic!("expected a request error, got {result:?}");
        };
        assert!(reason.contains("timed out"), "{reason}");
        assert!(
            ready.recorded().is_empty(),
            "a timeout re-resolved the serve"
        );
    }

    #[tokio::test]
    async fn a_missing_or_refusing_socket_is_unreachable() {
        let sandbox = Sandbox::new();
        let request = SqlRequest::new(READY_SQL.to_owned(), 1, 1);
        for socket in [missing_socket(), stale_socket(&sandbox.path("stale.sock"))] {
            let result: Result<SqlResponse, _> = socket
                .post(SQL_PATH, &request, Duration::from_secs(5))
                .await;
            let Err(ApiError::Unreachable(reason)) = result else {
                panic!("expected Unreachable, got {result:?}");
            };
            assert!(reason.contains(&*socket.path.to_string_lossy()), "{reason}");
        }
    }

    #[tokio::test]
    async fn a_refused_socket_fails_over_to_the_endpoint() {
        let sandbox = Sandbox::new();
        let ready = preview_pond().await;
        write_endpoint(
            &sandbox.origin().dir.endpoint(),
            &endpoint(&ready.socket, "t"),
        )
        .unwrap();
        let api = api_at(stale_socket(&sandbox.path("stale.sock")), &sandbox);
        assert_eq!(api.preview("s1".to_owned()).await.unwrap().len(), 2);
        assert_eq!(ready.recorded().len(), 2, "probe, then the retried preview");
    }

    #[tokio::test]
    async fn a_failed_retry_does_not_wedge_failover() {
        let sandbox = Sandbox::new();
        let endpoint_path = sandbox.origin().dir.endpoint();
        let stalling = FakePond::with_sql(
            vec![
                ("SELECT 1", Reply::json(golden::SQL_READY)),
                (
                    "DESC LIMIT",
                    Reply::json(golden::SQL_PAGE).delayed(Duration::from_secs(5)),
                ),
            ],
            Reply::json(golden::SEARCH),
        )
        .await;
        write_endpoint(&endpoint_path, &endpoint(&stalling.socket, "t")).unwrap();
        let api = api_at(missing_socket(), &sandbox);
        let request = SqlRequest::new(preview_sql("s1"), PREVIEW_ROWS, 1);
        let result: Result<SqlResponse, _> = api
            .post(SQL_PATH, &request, Duration::from_millis(300))
            .await;
        assert!(matches!(result, Err(ApiError::Request(_))), "{result:?}");

        drop(stalling);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let ready = preview_pond().await;
        write_endpoint(&endpoint_path, &endpoint(&ready.socket, "t")).unwrap();
        assert_eq!(api.preview("s1".to_owned()).await.unwrap().len(), 2);
        assert_eq!(ready.recorded().len(), 2, "probe, then the retried preview");
    }

    #[tokio::test]
    async fn an_aborted_request_leaves_the_fallback_spawn_running() {
        let sandbox = Sandbox::new();
        let pond = preview_pond().await;
        let script = write_script(
            &sandbox.path("bin/pond"),
            &format!(
                r#"printf '%s\n' "$*" >> '{calls}'
sleep 0.3
eval "socket=\${{$#}}"
ln -s '{target}' "$socket"
exec sleep 30"#,
                calls = sandbox.path("calls").display(),
                target = pond.socket.display(),
            ),
        );
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", script.display()));
        let api = Arc::new(api(&sandbox, None));

        let request = tokio::spawn({
            let api = Arc::clone(&api);
            async move { api.preview("s1".to_owned()).await }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while sandbox.lines("calls").is_empty() {
            assert!(tokio::time::Instant::now() < deadline, "no serve spawned");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());

        assert_eq!(api.preview("s1".to_owned()).await.unwrap().len(), 2);
        assert_eq!(
            sandbox.lines("calls").len(),
            1,
            "the abort killed the spawn"
        );
    }

    #[tokio::test]
    async fn callers_queued_behind_a_failed_resolution_share_its_error() {
        let sandbox = Sandbox::new();
        let script = write_script(
            &sandbox.path("bin/pond"),
            &format!(
                "echo spawned >> '{}'; sleep 0.2; exit 1",
                sandbox.path("calls").display()
            ),
        );
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", script.display()));
        let api = api(&sandbox, None);
        let (preview, titles) = tokio::join!(
            api.preview("s1".to_owned()),
            api.titles(vec!["s1".to_owned()])
        );
        let Err(error @ ApiError::Unreachable(_)) = preview else {
            panic!("expected Unreachable, got {preview:?}");
        };
        assert_eq!(titles, Err(error));
        assert_eq!(sandbox.lines("calls").len(), 1, "one spawn for both");

        tokio::time::sleep(RESOLVE_RETRY_AFTER).await;
        assert!(api.preview("s1".to_owned()).await.is_err());
        assert_eq!(
            sandbox.lines("calls").len(),
            2,
            "a later call resolves again"
        );
    }

    #[tokio::test]
    async fn a_successor_live_at_the_refused_path_is_used() {
        let sandbox = Sandbox::new();
        let owner = sandbox.origin().dir.socket("owner");
        let api = api_at(stale_socket(&owner), &sandbox);
        let successor = preview_pond().await;
        std::fs::remove_file(&owner).unwrap();
        std::os::unix::fs::symlink(&successor.socket, &owner).unwrap();
        write_endpoint(&sandbox.origin().dir.endpoint(), &endpoint(&owner, "t")).unwrap();

        let socket = api.resolve(Some(owner.clone())).await.unwrap();
        assert_eq!(socket.path, owner);
        assert_eq!(api.preview("s1".to_owned()).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn live_agents_come_from_herdrs_pane_list() {
        let sandbox = Sandbox::new();
        write_script(
            &sandbox.path("bin/herdr"),
            r#"echo '{"result":{"panes":[{"pane_id":"p1","agent":"codex","agent_session":{"kind":"path","value":"/s/rollout-abc.jsonl"}},{"pane_id":"p2"}]}}'"#,
        );
        let api = api_at(missing_socket(), &sandbox);
        let live = api.live_agents().await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(live[0].matches("abc"));

        write_script(&sandbox.path("bin/herdr"), "echo boom >&2; exit 1");
        let Err(ApiError::Herdr(reason)) = api.live_agents().await else {
            panic!("expected an error");
        };
        assert!(reason.contains("boom"), "{reason}");
    }
}
