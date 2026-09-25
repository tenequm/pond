//! The per-herdr-server `pond serve` owner: startup hook and detached
//! watchdog (plan 5.6).
//!
//! Startup hooks are one-shot and unserialized, so the hook only decides and
//! detaches; the `--owner` watchdog holds `lock` for its whole life, so at
//! most one serve exists per herdr server, and it never outlives that server.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::Utc;

use crate::api::{QUERY_TIMEOUT_SECS, SEARCH_PATH, SQL_PATH, client, post, sql_deadline};
use crate::config::{log_line, try_lock};
use crate::serve::{
    Endpoint, PORT_DEADLINE, ServeDir, live_endpoint, probe, read_port_file,
    remove_endpoint_if_owned, spawn_serve, terminate, write_endpoint,
};
use crate::types::{
    LISTING_ROWS, ListingScope, SearchRequest, SearchResponse, SqlRequest, SqlResponse, listing_sql,
};
use crate::{herdr, runtime};

struct Timing {
    tick: Duration,
    liveness_every: Duration,
    port_deadline: Duration,
    /// The historical 47-300s cold FTS load is paid here, not by the desk.
    warmup_deadline: Duration,
    grace: Duration,
}

const TIMING: Timing = Timing {
    tick: Duration::from_millis(500),
    liveness_every: Duration::from_secs(20),
    port_deadline: PORT_DEADLINE,
    warmup_deadline: Duration::from_secs(300),
    grace: Duration::from_secs(10),
};

const WARMUP_QUERY: &str = "session";

pub(crate) fn run(args: &[String]) -> anyhow::Result<()> {
    match args {
        [] => {
            startup();
            Ok(())
        }
        [flag] if flag == "--owner" => {
            herdr::detach();
            owner()
        }
        _ => bail!(crate::USAGE),
    }
}

/// The startup hook: exits at once, failures go to `daemon.log`.
fn startup() {
    let Ok(dir) = ServeDir::from_env() else {
        return;
    };
    let log = dir.daemon_log();
    let result = runtime().map_err(anyhow::Error::from).and_then(|runtime| {
        runtime.block_on(start(&dir, || {
            let mut command = Command::new(std::env::current_exe()?);
            command.args(["serve-daemon", "--owner"]);
            herdr::spawn_detached(command, &log)
        }))
    });
    if let Err(error) = result {
        log_line(&log, &format!("serve-daemon: {error:#}"));
    }
}

/// Spawns an owner unless one is alive (lock held) or the published
/// endpoint still answers the probe (adopted).
async fn start(
    dir: &ServeDir,
    spawn_owner: impl FnOnce() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let Some(lock) = try_lock(&dir.lock())? else {
        return Ok(());
    };
    if let Some(base_url) = live_endpoint(&client()?, dir).await {
        log_line(
            &dir.daemon_log(),
            &format!("adopted live endpoint {base_url}"),
        );
        return Ok(());
    }
    drop(lock);
    spawn_owner()
}

fn owner() -> anyhow::Result<()> {
    let dir = ServeDir::from_env()?;
    let socket = herdr::socket_path()?;
    let state_dir = herdr::state_dir()?;
    let config_dir = herdr::config_dir()?;
    let log = dir.daemon_log();
    runtime()?.block_on(own(&dir, &socket, &TIMING, || {
        herdr::resolve_pond_or_toast(&config_dir, &state_dir, &log)
    }))
}

async fn own(
    dir: &ServeDir,
    socket: &Path,
    timing: &Timing,
    resolve_pond: impl FnOnce() -> Option<PathBuf>,
) -> anyhow::Result<()> {
    let log = dir.daemon_log();
    let Some(_lock) = try_lock(&dir.lock())? else {
        return Ok(());
    };
    let client = client()?;
    if live_endpoint(&client, dir).await.is_some() {
        return Ok(());
    }
    let Some(pond) = resolve_pond() else {
        return Ok(());
    };
    let port_file = dir.port_file("owner");
    let mut child = spawn_serve(&pond, &port_file, &log)?;
    log_line(
        &log,
        &format!("owner: started {} (pid {})", pond.display(), child.id()),
    );
    let serve = Serve {
        client,
        child: &mut child,
        port_file: &port_file,
        socket,
        log: &log,
    };
    let token = serve.supervise(dir, timing).await;
    terminate(&mut child, timing.grace);
    if let Some(token) = token {
        remove_endpoint_if_owned(&dir.endpoint(), &token);
    }
    let _ = std::fs::remove_file(&port_file);
    log_line(&log, "owner: stopped");
    Ok(())
}

fn random_token() -> String {
    let state = RandomState::new();
    format!(
        "{:016x}{:016x}",
        state.hash_one(std::process::id()),
        state.hash_one(Instant::now())
    )
}

struct Serve<'a> {
    client: reqwest::Client,
    child: &'a mut Child,
    port_file: &'a Path,
    socket: &'a Path,
    log: &'a Path,
}

impl Serve<'_> {
    /// Watches the child, herdr and the port deadline until one ends the
    /// owner; publishes the endpoint once serve listens and passes the probe.
    /// Returns the published token, if any.
    async fn supervise(self, dir: &ServeDir, timing: &Timing) -> Option<String> {
        let started = Instant::now();
        let mut next_liveness = started + timing.liveness_every;
        let mut herdr_missed = false;
        let mut token = None;
        let mut warmup = None;
        let reason = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break format!("pond serve exited unexpectedly ({status})"),
                Err(error) => break format!("cannot watch pond serve: {error}"),
                Ok(None) => {}
            }
            // Two misses a tick apart, so a live handoff's socket swap is not a death.
            if Instant::now() >= next_liveness {
                match UnixStream::connect(self.socket) {
                    Err(error) if herdr_missed => break format!("herdr server is gone ({error})"),
                    Err(_) => {
                        herdr_missed = true;
                        next_liveness = Instant::now() + timing.tick;
                    }
                    Ok(_) => {
                        herdr_missed = false;
                        next_liveness = Instant::now() + timing.liveness_every;
                    }
                }
            }
            if token.is_none() {
                if let Some(addr) = read_port_file(self.port_file) {
                    let base_url = format!("http://{addr}");
                    if let Err(error) = probe(&self.client, &base_url).await {
                        break format!("capability probe failed: {error}");
                    }
                    let endpoint = Endpoint {
                        port: addr.port(),
                        token: random_token(),
                    };
                    if let Err(error) = write_endpoint(&dir.endpoint(), &endpoint) {
                        break format!("cannot publish the endpoint: {error}");
                    }
                    log_line(self.log, &format!("owner: published {base_url}"));
                    warmup = Some(tokio::spawn(warm_up(
                        self.client.clone(),
                        base_url,
                        self.log.to_path_buf(),
                        timing.warmup_deadline,
                    )));
                    token = Some(endpoint.token);
                } else if started.elapsed() > timing.port_deadline {
                    break format!(
                        "pond serve did not listen within {}s",
                        timing.port_deadline.as_secs()
                    );
                }
            }
            tokio::time::sleep(timing.tick).await;
        };
        log_line(self.log, &format!("owner: stopping - {reason}"));
        if let Some(warmup) = warmup {
            warmup.abort();
        }
        token
    }
}

/// The desk's opening listing and a first FTS search, once, so their cold
/// cost lands here instead of on the first desk open. Failure is not fatal.
async fn warm_up(client: reqwest::Client, base_url: String, log: PathBuf, deadline: Duration) {
    let started = Instant::now();
    let listing = SqlRequest::new(
        listing_sql(&ListingScope::recent(None, Utc::now())),
        LISTING_ROWS,
        QUERY_TIMEOUT_SECS,
    );
    let search = SearchRequest::new(WARMUP_QUERY.to_owned(), 1);
    let result = tokio::time::timeout(deadline, async {
        let deadline = sql_deadline(QUERY_TIMEOUT_SECS);
        post::<_, SqlResponse>(&client, &base_url, SQL_PATH, &listing, deadline).await?;
        post::<_, SearchResponse>(&client, &base_url, SEARCH_PATH, &search, deadline).await
    })
    .await;
    let outcome = match result {
        Ok(Ok(_)) => "done".to_owned(),
        Ok(Err(error)) => format!("failed: {error}"),
        Err(_) => format!("gave up after {}s", deadline.as_secs()),
    };
    log_line(
        &log,
        &format!(
            "owner: warm-up {outcome} ({:.1}s)",
            started.elapsed().as_secs_f64()
        ),
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::fs;
    use std::os::unix::net::UnixListener;

    use super::*;
    use crate::fake_pond::{FakePond, Reply, Sandbox, alive, endpoint, golden};
    use crate::serve::read_endpoint;

    const FAST: Timing = Timing {
        tick: Duration::from_millis(20),
        liveness_every: Duration::from_millis(100),
        port_deadline: Duration::from_millis(500),
        warmup_deadline: Duration::from_secs(5),
        grace: Duration::from_secs(2),
    };

    struct Setup {
        sandbox: Sandbox,
        pond: FakePond,
        socket: PathBuf,
        dir: ServeDir,
    }

    impl Setup {
        async fn new() -> Self {
            let sandbox = Sandbox::new();
            let pond = FakePond::with_sql(
                vec![
                    ("SELECT 1", Reply::json(golden::SQL_READY)),
                    ("GROUP BY session_id", Reply::json(golden::SQL_LISTING)),
                ],
                Reply::json(golden::SEARCH),
            )
            .await;
            let socket = sandbox.path("herdr.sock");
            let dir = ServeDir::new(&sandbox.state_dir(), &socket);
            Self {
                sandbox,
                pond,
                socket,
                dir,
            }
        }

        /// A fake `pond serve` that publishes the fake server's address when
        /// `publish`, then runs `after`.
        fn fake_pond(&self, publish: bool, after: &str) -> PathBuf {
            let addr = publish.then(|| self.pond.addr());
            self.sandbox.fake_serve(addr, after)
        }

        async fn own(&self, pond: &Path) -> anyhow::Result<()> {
            let pond = pond.to_path_buf();
            own(&self.dir, &self.socket, &FAST, move || Some(pond)).await
        }

        fn serve_calls(&self) -> usize {
            self.sandbox
                .lines("calls")
                .iter()
                .filter(|line| line.starts_with("serve --host 127.0.0.1 --port 0 --port-file "))
                .count()
        }

        fn log(&self) -> String {
            fs::read_to_string(self.dir.daemon_log()).unwrap_or_default()
        }
    }

    async fn wait_until(what: &str, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn one_owner_serves_until_herdr_goes_away() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(true, "exec sleep 30");
        let listener = UnixListener::bind(&setup.socket).unwrap();
        let herdr_stops = async {
            wait_until("the endpoint and warm-up", || {
                read_endpoint(&setup.dir.endpoint()).is_some()
                    && setup.pond.recorded().iter().any(|r| r.path == SEARCH_PATH)
            })
            .await;
            assert!(alive(setup.sandbox.serve_pid()));
            drop(listener);
        };
        let ((first, second), ()) = tokio::join!(
            async { tokio::join!(setup.own(&pond), setup.own(&pond)) },
            herdr_stops
        );
        first.unwrap();
        second.unwrap();

        assert_eq!(setup.serve_calls(), 1, "{}", setup.log());
        assert!(
            !setup.dir.endpoint().exists(),
            "endpoint outlived its serve"
        );
        assert!(
            !alive(setup.sandbox.serve_pid()),
            "pond serve outlived herdr"
        );
        assert!(setup.log().contains("herdr server is gone"));
        let warmup = &setup.pond.recorded()[1];
        assert_eq!(warmup.path, SQL_PATH);
        assert!(
            warmup.body.contains("timestamp >= TIMESTAMP"),
            "{}",
            warmup.body
        );
    }

    #[tokio::test]
    async fn teardown_keeps_a_successors_endpoint() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(true, "exec sleep 30");
        let listener = UnixListener::bind(&setup.socket).unwrap();
        let successor = async {
            wait_until("the endpoint", || {
                read_endpoint(&setup.dir.endpoint()).is_some()
            })
            .await;
            let mut endpoint = read_endpoint(&setup.dir.endpoint()).unwrap();
            endpoint.token = "successor".to_owned();
            write_endpoint(&setup.dir.endpoint(), &endpoint).unwrap();
            drop(listener);
        };
        let (owner, ()) = tokio::join!(setup.own(&pond), successor);
        owner.unwrap();
        assert_eq!(
            read_endpoint(&setup.dir.endpoint()).unwrap().token,
            "successor"
        );
    }

    #[tokio::test]
    async fn a_dying_serve_ends_the_owner_without_restart() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(true, "sleep 0.3; exit 1");
        let _listener = UnixListener::bind(&setup.socket).unwrap();
        setup.own(&pond).await.unwrap();
        assert!(
            setup.log().contains("exited unexpectedly"),
            "{}",
            setup.log()
        );
        assert!(!setup.dir.endpoint().exists());
        assert_eq!(setup.serve_calls(), 1);
    }

    #[tokio::test]
    async fn a_serve_that_never_listens_is_killed_at_the_deadline() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(false, "exec sleep 30");
        let _listener = UnixListener::bind(&setup.socket).unwrap();
        setup.own(&pond).await.unwrap();
        assert!(setup.log().contains("did not listen"), "{}", setup.log());
        assert!(!alive(setup.sandbox.serve_pid()));
        assert!(!setup.dir.endpoint().exists());
    }

    #[tokio::test]
    async fn start_spawns_an_owner_only_when_needed() {
        let setup = Setup::new().await;
        let spawned = std::cell::Cell::new(0);
        let spawn = || {
            spawned.set(spawned.get() + 1);
            Ok(())
        };

        start(&setup.dir, spawn).await.unwrap();
        assert_eq!(spawned.get(), 1, "no endpoint");

        fs::write(setup.dir.endpoint(), "{not json").unwrap();
        start(&setup.dir, spawn).await.unwrap();
        assert_eq!(spawned.get(), 2, "malformed endpoint");

        let held = try_lock(&setup.dir.lock()).unwrap().unwrap();
        start(&setup.dir, spawn).await.unwrap();
        assert_eq!(spawned.get(), 2, "an owner holds the lock");
        drop(held);

        write_endpoint(&setup.dir.endpoint(), &endpoint(setup.pond.port(), "t")).unwrap();
        start(&setup.dir, spawn).await.unwrap();
        assert_eq!(spawned.get(), 2, "live endpoint is adopted");
        assert!(setup.log().contains("adopted"));
    }

    #[test]
    fn tokens_differ() {
        assert_ne!(random_token(), random_token());
        assert_eq!(random_token().len(), 32);
    }
}
