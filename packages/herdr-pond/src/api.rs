//! The HTTP [`Api`](crate::types::Api) implementation over `pond serve`
//! (`/v1/x/sql`, `/v1/search`), plus herdr's pane list for live agents.
//! Tested against [`crate::fake_pond`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::herdr::{self, Herdr};
use crate::serve::{self, Fallback, Origin};
use crate::types::{
    Api, ApiError, ApiFuture, Cursor, ErrorEnvelope, ListingScope, LiveAgent, PAGE_ROWS,
    PREVIEW_ROWS, SearchRequest, SearchResponse, SessionDetail, SessionRow, SqlRequest,
    SqlResponse, TranscriptMessage, TranscriptPage, hydrate_sql, listing_sql, page_sql,
    preview_sql,
};

pub(crate) const SQL_PATH: &str = "/v1/x/sql";
pub(crate) const SEARCH_PATH: &str = "/v1/search";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Server-side execution budgets, sent as `timeout_seconds`. The client waits
/// [`CLIENT_SLACK`] longer so pond's enriched timeout error arrives instead of
/// a bare client-side timeout.
pub(crate) const QUERY_TIMEOUT_SECS: u64 = 25;
const ALL_TIME_TIMEOUT_SECS: u64 = 60;
const CLIENT_SLACK: Duration = Duration::from_secs(5);
pub(crate) const SEARCH_DEADLINE: Duration = Duration::from_secs(30);

/// Loopback only: an inherited `HTTP_PROXY` must never see desk traffic.
pub(crate) fn client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()?)
}

pub(crate) fn sql_deadline(timeout_seconds: u64) -> Duration {
    Duration::from_secs(timeout_seconds) + CLIENT_SLACK
}

/// One request against a known base URL.
pub(crate) async fn post<B, T>(
    client: &reqwest::Client,
    base_url: &str,
    path: &str,
    body: &B,
    deadline: Duration,
) -> Result<T, ApiError>
where
    B: Serialize + ?Sized,
    T: DeserializeOwned,
{
    let response = client
        .post(format!("{base_url}{path}"))
        .json(body)
        .timeout(deadline)
        .send()
        .await
        .map_err(transport)?;
    let status = response.status().as_u16();
    let body = response.text().await.map_err(transport)?;
    decode(path, status, &body)
}

/// Only a failed connect proves the serve gone; a timeout or a dropped
/// response may come from a live serve that is merely slow. reqwest's own
/// message is only "error sending request", so the cause chain is appended.
fn transport(error: reqwest::Error) -> ApiError {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
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

/// The resolved serve. `base_url` stays unset until the first call, so the
/// desk's loading state covers a cold fallback spawn.
#[derive(Default)]
struct Link {
    base_url: Option<String>,
    fallback: Option<Fallback>,
}

/// A URL that just refused a connection, and why.
struct Stale {
    url: String,
    reason: String,
}

/// Owned by the api and by each resolution task, so resolution survives the
/// request that started it: the desk aborts a lane on every new fetch, and a
/// cancelled resolution would kill a half-open fallback serve mid store-open.
struct Resolver {
    client: reqwest::Client,
    /// Why no serve can be found at all (not running under herdr), reported
    /// on first use rather than before the desk can draw.
    origin: Result<Origin, String>,
    link: Mutex<Link>,
    /// Set by a failover, cleared by the next successful request: while set,
    /// a refused connection is an error instead of another failover.
    failed_over: AtomicBool,
}

impl Resolver {
    /// Runs with `link` locked, so concurrent callers queue behind one
    /// resolution and then reuse its result.
    async fn resolve(&self, stale: Option<Stale>) -> Result<String, ApiError> {
        let mut link = self.link.lock().await;
        if let Some(current) = &link.base_url {
            match &stale {
                None => return Ok(current.clone()),
                Some(stale) if *current != stale.url => return Ok(current.clone()),
                Some(stale) if self.failed_over.load(Ordering::Relaxed) => {
                    return Err(ApiError::Unreachable(stale.reason.clone()));
                }
                Some(_) => {}
            }
        }
        let origin = self
            .origin
            .as_ref()
            .map_err(|error| ApiError::Unreachable(error.clone()))?;
        let connection = serve::connect(&self.client, origin, link.fallback.take()).await?;
        link.fallback = connection.fallback;
        link.base_url = Some(connection.base_url.clone());
        if let Some(stale) = stale {
            if connection.base_url == stale.url {
                return Err(ApiError::Unreachable(stale.reason));
            }
            self.failed_over.store(true, Ordering::Relaxed);
        }
        Ok(connection.base_url)
    }
}

pub(crate) struct HttpApi {
    resolver: Arc<Resolver>,
    herdr: Herdr,
}

impl HttpApi {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            resolver: Arc::new(Resolver {
                client: client()?,
                origin: Origin::from_env().map_err(|error| format!("{error:#}")),
                link: Mutex::default(),
                failed_over: AtomicBool::new(false),
            }),
            herdr: Herdr::from_env(),
        })
    }

    /// The current serve, else a resolution run in its own task.
    async fn resolve(&self, stale: Option<Stale>) -> Result<String, ApiError> {
        if stale.is_none() {
            let current = self.resolver.link.lock().await.base_url.clone();
            if let Some(url) = current {
                return Ok(url);
            }
        }
        let resolver = Arc::clone(&self.resolver);
        tokio::spawn(async move { resolver.resolve(stale).await })
            .await
            .map_err(|error| ApiError::Unreachable(format!("resolving pond serve: {error}")))?
    }

    /// Sends to the resolved serve. When the serve refuses the connection,
    /// the endpoint is resolved again (daemon record, else a fallback child)
    /// and the request retried there once; a second refusal in a row, with
    /// no success in between, stands as an error.
    async fn post<B, T>(&self, path: &str, body: &B, deadline: Duration) -> Result<T, ApiError>
    where
        B: Serialize + ?Sized + Sync,
        T: DeserializeOwned,
    {
        let client = &self.resolver.client;
        let url = self.resolve(None).await?;
        let result = match post(client, &url, path, body, deadline).await {
            Err(ApiError::Unreachable(reason)) => {
                let retry = self.resolve(Some(Stale { url, reason })).await?;
                post(client, &retry, path, body, deadline).await
            }
            other => other,
        };
        if result.is_ok() {
            self.resolver.failed_over.store(false, Ordering::Relaxed);
        }
        result
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

    fn hydrate(&self, session_ids: Vec<String>) -> ApiFuture<'_, Vec<SessionDetail>> {
        Box::pin(async move {
            if session_ids.is_empty() {
                return Ok(Vec::new());
            }
            let query = hydrate_sql(&session_ids);
            self.sql(query, session_ids.len(), QUERY_TIMEOUT_SECS)
                .await?
                .into_rows()
        })
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
        FakePond, Reply, Sandbox, dead_url, endpoint, golden, ts, write_script,
    };
    use crate::serve::write_endpoint;

    /// An api resolving through `sandbox`'s state, pinned to `base_url` if given.
    fn api(sandbox: &Sandbox, base_url: Option<&str>) -> HttpApi {
        HttpApi {
            resolver: Arc::new(Resolver {
                client: client().unwrap(),
                origin: Ok(sandbox.origin()),
                link: Mutex::new(Link {
                    base_url: base_url.map(str::to_owned),
                    fallback: None,
                }),
                failed_over: AtomicBool::new(false),
            }),
            herdr: Herdr::new(sandbox.path("bin/herdr")),
        }
    }

    /// Pinned to `base_url`, with a `pond_bin` that points nowhere, so no
    /// re-resolution can reach a real pond.
    fn api_at(base_url: &str, sandbox: &Sandbox) -> HttpApi {
        sandbox.write_config(&format!(
            "pond_bin = \"{}\"\n",
            sandbox.path("bin/no-pond").display()
        ));
        api(sandbox, Some(base_url))
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
        let api = api_at(&pond.base_url, &sandbox);
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
        let bodies = sent(&pond);
        assert_eq!(bodies[0]["query"], listing_sql(&scope));
        assert_eq!(bodies[0]["limit"], 200);
        assert_eq!(bodies[0]["protocol_version"], 1);
        assert_eq!(bodies[0]["timeout_seconds"], QUERY_TIMEOUT_SECS);
        assert_eq!(bodies[1]["query"], listing_sql(&all_time));
        assert_eq!(bodies[1]["timeout_seconds"], ALL_TIME_TIMEOUT_SECS);
    }

    #[tokio::test]
    async fn hydrate_reads_omitted_nulls_as_none() {
        let sandbox = Sandbox::new();
        let pond = FakePond::with_sql(
            vec![("COUNT(*)", Reply::json(golden::SQL_HYDRATE))],
            Reply::json(golden::SEARCH),
        )
        .await;
        let api = api_at(&pond.base_url, &sandbox);
        assert!(api.hydrate(Vec::new()).await.unwrap().is_empty());
        assert!(pond.recorded().is_empty(), "empty hydrate sent a request");

        let ids = vec!["s-live".to_owned(), "s-old".to_owned()];
        let details = api.hydrate(ids.clone()).await.unwrap();
        assert_eq!(details[0].host.as_deref(), Some("ws-pond-01"));
        assert_eq!(
            (details[1].title.clone(), details[1].host.clone()),
            (None, None)
        );
        assert_eq!(details[1].message_count, 3);
        let body = &sent(&pond)[0];
        assert_eq!(body["query"], hydrate_sql(&ids));
        assert_eq!(body["limit"], 2);
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
        let api = api_at(&pond.base_url, &sandbox);
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
        let api = api_at(&pond.base_url, &sandbox);
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
        let api = api_at(&pond.base_url, &sandbox);

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
            &endpoint(ready.port(), "t"),
        )
        .unwrap();
        let api = api_at(&stalled.base_url, &sandbox);
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
    async fn a_success_rearms_failover() {
        let sandbox = Sandbox::new();
        let endpoint_path = sandbox.origin().dir.endpoint();
        let first = preview_pond().await;
        write_endpoint(&endpoint_path, &endpoint(first.port(), "t")).unwrap();
        let api = api_at(&dead_url(), &sandbox);
        assert_eq!(api.preview("s1".to_owned()).await.unwrap().len(), 2);

        drop(first);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let second = preview_pond().await;
        write_endpoint(&endpoint_path, &endpoint(second.port(), "t")).unwrap();
        assert_eq!(api.preview("s1".to_owned()).await.unwrap().len(), 2);
        assert_eq!(
            second.recorded().len(),
            2,
            "probe, then the retried preview"
        );
    }

    #[tokio::test]
    async fn a_refusal_right_after_a_failover_stands() {
        let sandbox = Sandbox::new();
        let ready = preview_pond().await;
        write_endpoint(
            &sandbox.origin().dir.endpoint(),
            &endpoint(ready.port(), "t"),
        )
        .unwrap();
        let api = api_at(&dead_url(), &sandbox);
        api.resolver.failed_over.store(true, Ordering::Relaxed);
        assert!(matches!(
            api.preview("s1".to_owned()).await,
            Err(ApiError::Unreachable(_))
        ));
        assert!(ready.recorded().is_empty(), "failed over twice in a row");
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
eval "port_file=\${{$#}}"
printf '%s' '{addr}' > "$port_file.tmp" && mv "$port_file.tmp" "$port_file"
exec sleep 30"#,
                calls = sandbox.path("calls").display(),
                addr = pond.addr(),
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
    async fn live_agents_come_from_herdrs_pane_list() {
        let sandbox = Sandbox::new();
        write_script(
            &sandbox.path("bin/herdr"),
            r#"echo '{"result":{"panes":[{"pane_id":"p1","agent":"codex","agent_session":{"kind":"path","value":"/s/rollout-abc.jsonl"}},{"pane_id":"p2"}]}}'"#,
        );
        let api = api_at(&dead_url(), &sandbox);
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
