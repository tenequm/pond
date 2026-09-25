//! The HTTP [`Api`](crate::types::Api) implementation over `pond serve`
//! (`/v1/x/sql`, `/v1/search`), plus herdr's pane list for live agents.
//! Tested against [`crate::fake_pond`].

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::herdr::{self, Herdr};
use crate::serve::{self, Origin, ServeChild};
use crate::types::{
    Api, ApiError, ApiFuture, Cursor, ErrorEnvelope, ListingScope, LiveAgent, PAGE_ROWS,
    PREVIEW_ROWS, PROTOCOL_VERSION, SearchRequest, SearchResponse, SessionDetail, SessionRow,
    SqlRequest, SqlResponse, TranscriptMessage, TranscriptPage, hydrate_sql, listing_sql, page_sql,
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

/// `limit` is always the query's own SQL `LIMIT`, so the server's default
/// 100-row cap never cuts a page.
pub(crate) fn sql_request(query: String, limit: usize, timeout_seconds: u64) -> SqlRequest {
    SqlRequest {
        protocol_version: PROTOCOL_VERSION,
        query,
        limit: Some(limit),
        timeout_seconds: Some(timeout_seconds),
    }
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

/// reqwest's own message is only "error sending request"; the cause chain
/// says refused vs timed out.
fn transport(error: reqwest::Error) -> ApiError {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    ApiError::Unreachable(message)
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

fn rows<T: DeserializeOwned>(response: SqlResponse) -> Result<Vec<T>, ApiError> {
    response
        .rows
        .into_iter()
        .map(|row| serde_json::from_value(row).map_err(|error| ApiError::Decode(error.to_string())))
        .collect()
}

/// The resolved serve. `base_url` stays unset until the first call, so the
/// desk's loading state covers a cold fallback spawn.
#[derive(Default)]
struct Link {
    base_url: Option<String>,
    fallback: Option<ServeChild>,
    failed_over: bool,
}

pub(crate) struct HttpApi {
    client: reqwest::Client,
    /// Why no serve can be found at all (not running under herdr), reported
    /// on first use rather than before the desk can draw.
    origin: Result<Origin, String>,
    herdr: Herdr,
    link: Mutex<Link>,
}

impl HttpApi {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            client: client()?,
            origin: Origin::from_env().map_err(|error| format!("{error:#}")),
            herdr: Herdr::from_env(),
            link: Mutex::default(),
        })
    }

    async fn resolve(&self, link: &mut Link) -> Result<String, ApiError> {
        let origin = self
            .origin
            .as_ref()
            .map_err(|error| ApiError::Unreachable(error.clone()))?;
        let connection = serve::connect(&self.client, origin, link.fallback.take()).await?;
        link.fallback = connection.fallback;
        link.base_url = Some(connection.base_url.clone());
        Ok(connection.base_url)
    }

    /// Sends to the resolved serve. The first time a serve becomes unreachable
    /// the endpoint is resolved again (daemon record, else a fallback child)
    /// and the request retried there; after that, errors stand.
    async fn post<B, T>(&self, path: &str, body: &B, deadline: Duration) -> Result<T, ApiError>
    where
        B: Serialize + ?Sized + Sync,
        T: DeserializeOwned,
    {
        let url = {
            let mut link = self.link.lock().await;
            match link.base_url.clone() {
                Some(url) => url,
                None => self.resolve(&mut link).await?,
            }
        };
        let reason = match post(&self.client, &url, path, body, deadline).await {
            Err(ApiError::Unreachable(reason)) => reason,
            other => return other,
        };
        let retry = {
            let mut link = self.link.lock().await;
            match link.base_url.clone() {
                Some(current) if current != url => current,
                _ if link.failed_over => return Err(ApiError::Unreachable(reason)),
                _ => {
                    let fresh = self.resolve(&mut link).await?;
                    if fresh == url {
                        return Err(ApiError::Unreachable(reason));
                    }
                    link.failed_over = true;
                    fresh
                }
            }
        };
        post(&self.client, &retry, path, body, deadline).await
    }

    async fn sql(
        &self,
        query: String,
        limit: usize,
        timeout_seconds: u64,
    ) -> Result<SqlResponse, ApiError> {
        let request = sql_request(query, limit, timeout_seconds);
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
            rows(self.sql(listing_sql(&scope), scope.limit, timeout).await?)
        })
    }

    fn hydrate(&self, session_ids: Vec<String>) -> ApiFuture<'_, Vec<SessionDetail>> {
        Box::pin(async move {
            if session_ids.is_empty() {
                return Ok(Vec::new());
            }
            let query = hydrate_sql(&session_ids);
            rows(
                self.sql(query, session_ids.len(), QUERY_TIMEOUT_SECS)
                    .await?,
            )
        })
    }

    fn search(&self, request: SearchRequest) -> ApiFuture<'_, SearchResponse> {
        Box::pin(async move { self.post(SEARCH_PATH, &request, SEARCH_DEADLINE).await })
    }

    fn preview(&self, session_id: String) -> ApiFuture<'_, Vec<TranscriptMessage>> {
        Box::pin(async move {
            let query = preview_sql(&session_id);
            rows(self.sql(query, PREVIEW_ROWS, QUERY_TIMEOUT_SECS).await?)
        })
    }

    fn page(&self, session_id: String, after: Option<Cursor>) -> ApiFuture<'_, TranscriptPage> {
        Box::pin(async move {
            let query = page_sql(&session_id, after.as_ref());
            let response = self.sql(query, PAGE_ROWS, QUERY_TIMEOUT_SECS).await?;
            Ok(TranscriptPage {
                truncated: response.truncated,
                messages: rows(response)?,
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

    use chrono::{DateTime, Utc};

    use super::*;
    use crate::fake_pond::{FakePond, Reply, Sandbox, golden, write_script};
    use crate::serve::{Endpoint, ServeDir, write_endpoint};
    use crate::types::{ProjectFilter, SearchFilters};

    /// An api pinned to `base_url`, re-resolving through `sandbox`'s state.
    /// `pond_bin` points nowhere, so no re-resolution can reach a real pond.
    fn api_at(base_url: &str, sandbox: &Sandbox) -> HttpApi {
        sandbox.write_config(&format!(
            "pond_bin = \"{}\"\n",
            sandbox.path("bin/no-pond").display()
        ));
        HttpApi {
            client: client().unwrap(),
            origin: Ok(Origin {
                dir: ServeDir::new(&sandbox.state_dir(), &sandbox.path("herdr.sock")),
                config_dir: sandbox.config_dir(),
            }),
            herdr: Herdr::new(sandbox.path("bin/herdr")),
            link: Mutex::new(Link {
                base_url: Some(base_url.to_owned()),
                ..Link::default()
            }),
        }
    }

    fn sent(pond: &FakePond) -> Vec<serde_json::Value> {
        pond.recorded()
            .iter()
            .map(|request| serde_json::from_str(&request.body).unwrap())
            .collect()
    }

    fn dead_url() -> String {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        format!("http://127.0.0.1:{port}")
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
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
        let response = api
            .search(SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                query: "timer".to_owned(),
                filters: SearchFilters {
                    project: Some(ProjectFilter::Contains("/pj/pond".to_owned())),
                    from_date: None,
                },
                limit: 20,
            })
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

        let rejected = api
            .search(SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                query: "x".to_owned(),
                filters: SearchFilters::default(),
                limit: 1,
            })
            .await;
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
    async fn a_stalled_server_is_unreachable() {
        let pond =
            FakePond::start(|_, _| Reply::json(golden::SQL_EMPTY).delayed(Duration::from_secs(5)))
                .await;
        let request = sql_request(preview_sql("s"), PREVIEW_ROWS, 1);
        let result: Result<SqlResponse, _> = post(
            &client().unwrap(),
            &pond.base_url,
            SQL_PATH,
            &request,
            Duration::from_millis(200),
        )
        .await;
        let Err(ApiError::Unreachable(reason)) = result else {
            panic!("expected Unreachable, got {result:?}");
        };
        assert!(reason.contains("timed out"), "{reason}");
    }

    #[tokio::test]
    async fn a_dead_serve_fails_over_once() {
        let sandbox = Sandbox::new();
        let replacement = FakePond::with_sql(
            vec![
                ("SELECT 1", Reply::json(golden::SQL_READY)),
                ("DESC LIMIT", Reply::json(golden::SQL_PAGE)),
            ],
            Reply::json(golden::SEARCH),
        )
        .await;
        let api = api_at(&dead_url(), &sandbox);
        let port = replacement
            .base_url
            .rsplit(':')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let endpoint = Endpoint {
            port,
            pid: 1,
            token: "t".to_owned(),
            pond_version: "pond".to_owned(),
        };
        let dir = ServeDir::new(&sandbox.state_dir(), &sandbox.path("herdr.sock"));
        write_endpoint(&dir.endpoint(), &endpoint).unwrap();

        let messages = api.preview("s1".to_owned()).await.unwrap();
        assert_eq!(messages.len(), 2);

        drop(replacement);
        tokio::time::sleep(Duration::from_millis(50)).await;
        std::fs::remove_file(dir.endpoint()).unwrap();
        let pond = write_script(&sandbox.path("bin/pond"), "exit 9");
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        assert!(matches!(
            api.preview("s1".to_owned()).await,
            Err(ApiError::Unreachable(_))
        ));
        assert!(
            !sandbox
                .state_dir()
                .join("serve")
                .read_dir()
                .unwrap()
                .any(|entry| { entry.unwrap().path().join("desk-serve.log").exists() }),
            "a second failover spawned a fallback"
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
