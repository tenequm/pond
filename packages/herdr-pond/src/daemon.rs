//! The per-herdr-server `pond serve` owner: startup hook and detached
//! watchdog.
//!
//! Startup hooks are one-shot and unserialized, so the hook only decides and
//! detaches; the `--owner` watchdog holds `lock` for its whole life, so at
//! most one supervised serve exists per herdr server, and it never outlives
//! that server.

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::BuildHasher;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::Utc;

use crate::api::{QUERY_TIMEOUT_SECS, SEARCH_PATH, SQL_PATH, client, post, sql_deadline};
use crate::config::{cap_log, log_line, try_lock};
use crate::serve::{
    Endpoint, PORT_DEADLINE, ServeChild, ServeDir, live_endpoint, probe, remove_endpoint_if_owned,
    retire, write_endpoint,
};
use crate::types::{
    ApiError, LISTING_ROWS, ListingScope, SearchRequest, SearchResponse, SqlRequest, SqlResponse,
    listing_sql,
};
use crate::{herdr, runtime, shutdown_signal};

struct Timing {
    tick: Duration,
    liveness_every: Duration,
    /// A live handoff swaps herdr's socket, and the new server waits up to 5s
    /// for the old one to close before binding: only misses spanning longer
    /// than this mean herdr is gone.
    handoff_window: Duration,
    port_deadline: Duration,
    /// A serve that has just bound may still be settling, so a failed
    /// capability probe is retried for this long.
    probe_window: Duration,
    probe_retry: Duration,
    /// The historical 47-300s cold FTS load is paid here, not by the desk.
    warmup_deadline: Duration,
    grace: Duration,
}

const TIMING: Timing = Timing {
    tick: Duration::from_millis(500),
    liveness_every: Duration::from_secs(20),
    handoff_window: Duration::from_secs(10),
    port_deadline: PORT_DEADLINE,
    probe_window: Duration::from_secs(30),
    probe_retry: Duration::from_secs(5),
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
    let result = start(&dir, || {
        let mut command = Command::new(std::env::current_exe()?);
        command.args(["serve-daemon", "--owner"]);
        herdr::spawn_detached(command, &log)
    });
    if let Err(error) = result {
        log_line(&log, &format!("serve-daemon: {error:#}"));
    }
}

/// Spawns an owner unless a live one holds the lock. A free lock means no
/// owner supervises whatever the endpoint names, so the owner replaces it.
fn start(dir: &ServeDir, spawn_owner: impl FnOnce() -> anyhow::Result<()>) -> anyhow::Result<()> {
    if try_lock(&dir.lock())?.is_none() {
        return Ok(());
    }
    spawn_owner()
}

fn owner() -> anyhow::Result<()> {
    let dir = ServeDir::from_env()?;
    let socket = herdr::socket_path()?;
    let state_dir = herdr::state_dir()?;
    let config_dir = herdr::config_dir()?;
    let log = dir.daemon_log();
    runtime()?.block_on(async {
        let shutdown = shutdown_signal()?;
        own(
            &dir,
            &socket,
            &TIMING,
            || herdr::resolve_pond_or_toast(&config_dir, &state_dir, &log),
            shutdown,
        )
        .await
    })
}

/// Holds the lock and supervises one serve until the serve dies, herdr goes
/// away, or `shutdown` fires; every ending tears down the same way.
async fn own(
    dir: &ServeDir,
    socket: &Path,
    timing: &Timing,
    resolve_pond: impl FnOnce() -> Option<PathBuf>,
    shutdown: impl Future<Output = &'static str>,
) -> anyhow::Result<()> {
    let log = dir.daemon_log();
    let Some(_lock) = try_lock(&dir.lock())? else {
        return Ok(());
    };
    let client = client()?;
    if let Some(base_url) = live_endpoint(&client, dir).await {
        log_line(
            &log,
            &format!(
                "owner: {base_url} answers but no owner supervises it (a dead owner's orphan, \
                 or another process on that port) - starting a fresh serve"
            ),
        );
    }
    let Some(pond) = resolve_pond() else {
        return Ok(());
    };
    let mut serve = ServeChild::spawn(&pond, dir.port_file("owner"), log.clone(), timing.grace)?;
    log_line(
        &log,
        &format!("owner: started {} (pid {})", pond.display(), serve.id()),
    );
    let mut token = None;
    let reason = tokio::select! {
        reason = supervise(&mut serve, &client, dir, timing, &mut token) => reason,
        reason = herdr_gone(socket, &log, timing) => reason,
        signal = shutdown => format!("received {signal}"),
    };
    log_line(&log, &format!("owner: stopping - {reason}"));
    let _ = retire(serve).await;
    if let Some(token) = token {
        remove_endpoint_if_owned(&dir.endpoint(), &token);
    }
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

/// Publishes the endpoint once serve listens and passes the probe, warms it
/// up, and returns why the owner must stop. `token` is set on publish.
async fn supervise(
    serve: &mut ServeChild,
    client: &reqwest::Client,
    dir: &ServeDir,
    timing: &Timing,
    token: &mut Option<String>,
) -> String {
    let addr = match serve.listening(timing.port_deadline).await {
        Ok(addr) => addr,
        Err(error) => return error.to_string(),
    };
    let base_url = format!("http://{addr}");
    if let Err(reason) = probe_until_ready(serve, client, &base_url, timing).await {
        return reason;
    }
    let endpoint = Endpoint {
        port: addr.port(),
        token: random_token(),
    };
    if let Err(error) = write_endpoint(&dir.endpoint(), &endpoint) {
        return format!("cannot publish the endpoint: {error}");
    }
    let log = dir.daemon_log();
    log_line(&log, &format!("owner: published {base_url}"));
    *token = Some(endpoint.token);
    let ((), reason) = tokio::join!(
        warm_up(client, &base_url, &log, timing.warmup_deadline),
        exited(serve, timing.tick)
    );
    reason
}

/// Retries within `probe_window`, except for a pond too old for the desk,
/// which no retry fixes.
async fn probe_until_ready(
    serve: &mut ServeChild,
    client: &reqwest::Client,
    base_url: &str,
    timing: &Timing,
) -> Result<(), String> {
    let started = Instant::now();
    loop {
        match probe(client, base_url).await {
            Ok(()) => return Ok(()),
            Err(error @ ApiError::PondTooOld) => return Err(error.to_string()),
            Err(error) if started.elapsed() >= timing.probe_window => {
                return Err(format!("capability probe failed: {error}"));
            }
            Err(_) => {}
        }
        if let Some(reason) = serve.exited() {
            return Err(reason);
        }
        tokio::time::sleep(timing.probe_retry).await;
    }
}

async fn exited(serve: &mut ServeChild, tick: Duration) -> String {
    loop {
        if let Some(reason) = serve.exited() {
            return reason;
        }
        tokio::time::sleep(tick).await;
    }
}

/// Returns once herdr's socket has refused connections for longer than a live
/// handoff takes. Each check also caps `daemon.log`, which the long-lived
/// serve appends to.
async fn herdr_gone(socket: &Path, log: &Path, timing: &Timing) -> String {
    let mut first_miss: Option<Instant> = None;
    loop {
        let _ = cap_log(log);
        match UnixStream::connect(socket) {
            Ok(_) => first_miss = None,
            Err(error) => {
                if first_miss.get_or_insert_with(Instant::now).elapsed() > timing.handoff_window {
                    return format!("herdr server is gone ({error})");
                }
            }
        }
        let next = if first_miss.is_some() {
            timing.tick
        } else {
            timing.liveness_every
        };
        tokio::time::sleep(next).await;
    }
}

/// The desk's opening listing and a first FTS search, once, so their cold
/// cost lands here instead of on the first desk open. The search gets what
/// is left of `budget`. Failure is not fatal.
async fn warm_up(client: &reqwest::Client, base_url: &str, log: &Path, budget: Duration) {
    let started = Instant::now();
    let listing = SqlRequest::new(
        listing_sql(&ListingScope::recent(None, Utc::now())),
        LISTING_ROWS,
        QUERY_TIMEOUT_SECS,
    );
    let search = SearchRequest::new(WARMUP_QUERY.to_owned(), 1);
    let listing_deadline = sql_deadline(QUERY_TIMEOUT_SECS);
    let result = async {
        post::<_, SqlResponse>(client, base_url, SQL_PATH, &listing, listing_deadline).await?;
        let search_deadline = budget.saturating_sub(started.elapsed());
        post::<_, SearchResponse>(client, base_url, SEARCH_PATH, &search, search_deadline).await
    }
    .await;
    let outcome = match result {
        Ok(_) => "done".to_owned(),
        Err(error) => format!("failed: {error}"),
    };
    log_line(
        log,
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::fake_pond::{FakePond, Reply, Sandbox, alive, endpoint, golden};
    use crate::serve::read_endpoint;

    const FAST: Timing = Timing {
        tick: Duration::from_millis(20),
        liveness_every: Duration::from_millis(100),
        handoff_window: Duration::from_millis(300),
        port_deadline: Duration::from_millis(500),
        probe_window: Duration::from_millis(300),
        probe_retry: Duration::from_millis(20),
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
            Self::with(
                FakePond::with_sql(
                    vec![
                        ("SELECT 1", Reply::json(golden::SQL_READY)),
                        ("GROUP BY session_id", Reply::json(golden::SQL_LISTING)),
                    ],
                    Reply::json(golden::SEARCH),
                )
                .await,
            )
        }

        fn with(pond: FakePond) -> Self {
            let sandbox = Sandbox::new();
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
            self.own_until(pond, std::future::pending()).await
        }

        async fn own_until(
            &self,
            pond: &Path,
            shutdown: impl Future<Output = &'static str>,
        ) -> anyhow::Result<()> {
            let pond = pond.to_path_buf();
            own(&self.dir, &self.socket, &FAST, move || Some(pond), shutdown).await
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

        fn endpoint(&self) -> Option<Endpoint> {
            read_endpoint(&self.dir.endpoint())
        }

        async fn published(&self) {
            wait_until("the endpoint", || self.endpoint().is_some()).await;
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
                setup.endpoint().is_some()
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
        assert!(setup.endpoint().is_none(), "endpoint outlived its serve");
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
    async fn a_handoff_gap_is_not_herdr_leaving() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(true, "exec sleep 30");
        let old_server = UnixListener::bind(&setup.socket).unwrap();
        let handoff = async {
            setup.published().await;
            drop(old_server);
            fs::remove_file(&setup.socket).unwrap();
            tokio::time::sleep(FAST.handoff_window / 2).await;
            let new_server = UnixListener::bind(&setup.socket).unwrap();
            fs::write(setup.dir.daemon_log(), vec![b'x'; 2 << 20]).unwrap();
            tokio::time::sleep(FAST.handoff_window * 2).await;
            assert!(setup.endpoint().is_some(), "{}", setup.log());
            assert!(alive(setup.sandbox.serve_pid()));
            let log_len = fs::metadata(setup.dir.daemon_log()).unwrap().len();
            assert!(log_len < 1 << 20, "daemon.log not capped: {log_len}");
            drop(new_server);
        };
        let (owner, ()) = tokio::join!(setup.own(&pond), handoff);
        owner.unwrap();
        assert!(setup.log().contains("herdr server is gone"));
        assert!(setup.endpoint().is_none());
    }

    #[tokio::test]
    async fn a_signal_tears_down_like_herdr_leaving() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(true, "exec sleep 30");
        let _listener = UnixListener::bind(&setup.socket).unwrap();
        let signal = async {
            setup.published().await;
            "SIGTERM"
        };
        setup.own_until(&pond, signal).await.unwrap();
        assert!(setup.log().contains("received SIGTERM"), "{}", setup.log());
        assert!(setup.endpoint().is_none());
        assert!(!alive(setup.sandbox.serve_pid()));
    }

    #[tokio::test]
    async fn teardown_keeps_a_successors_endpoint() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(true, "exec sleep 30");
        let listener = UnixListener::bind(&setup.socket).unwrap();
        let successor = async {
            setup.published().await;
            let mut endpoint = setup.endpoint().unwrap();
            endpoint.token = "successor".to_owned();
            write_endpoint(&setup.dir.endpoint(), &endpoint).unwrap();
            drop(listener);
        };
        let (owner, ()) = tokio::join!(setup.own(&pond), successor);
        owner.unwrap();
        assert_eq!(setup.endpoint().unwrap().token, "successor");
    }

    #[tokio::test]
    async fn an_unsupervised_live_endpoint_is_replaced() {
        let setup = Setup::new().await;
        let orphan = FakePond::with_sql(
            vec![("SELECT 1", Reply::json(golden::SQL_READY))],
            Reply::json(golden::SEARCH),
        )
        .await;
        write_endpoint(&setup.dir.endpoint(), &endpoint(orphan.port(), "orphan")).unwrap();
        let pond = setup.fake_pond(true, "exec sleep 30");
        let listener = UnixListener::bind(&setup.socket).unwrap();
        let herdr_stops = async {
            wait_until("the fresh endpoint", || {
                setup.endpoint().is_some_and(|e| e.token != "orphan")
            })
            .await;
            assert_eq!(setup.endpoint().unwrap().port, setup.pond.port());
            drop(listener);
        };
        let (owner, ()) = tokio::join!(setup.own(&pond), herdr_stops);
        owner.unwrap();
        assert_eq!(setup.serve_calls(), 1);
        assert!(
            setup.log().contains("no owner supervises"),
            "{}",
            setup.log()
        );
    }

    #[tokio::test]
    async fn a_probe_failing_while_serve_settles_is_retried() {
        let unready = Arc::new(AtomicUsize::new(2));
        let setup = Setup::with(
            FakePond::start(move |_, body| {
                let settling = body.contains("SELECT 1")
                    && unready
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                if settling {
                    Reply::plain(503, "starting")
                } else {
                    Reply::json(golden::SQL_READY)
                }
            })
            .await,
        );
        let pond = setup.fake_pond(true, "exec sleep 30");
        let listener = UnixListener::bind(&setup.socket).unwrap();
        let herdr_stops = async {
            setup.published().await;
            drop(listener);
        };
        let (owner, ()) = tokio::join!(setup.own(&pond), herdr_stops);
        owner.unwrap();
        assert!(!setup.log().contains("probe failed"), "{}", setup.log());
    }

    #[tokio::test]
    async fn a_probe_that_never_passes_gives_up() {
        let setup = Setup::with(FakePond::start(|_, _| Reply::plain(503, "starting")).await);
        let pond = setup.fake_pond(true, "exec sleep 30");
        let _listener = UnixListener::bind(&setup.socket).unwrap();
        setup.own(&pond).await.unwrap();
        assert!(
            setup.log().contains("capability probe failed"),
            "{}",
            setup.log()
        );
        assert!(setup.pond.recorded().len() > 1, "never retried");
        assert!(!alive(setup.sandbox.serve_pid()));
        assert!(setup.endpoint().is_none());
    }

    #[tokio::test]
    async fn a_dying_serve_ends_the_owner_without_restart() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(true, "sleep 0.3; exit 1");
        let _listener = UnixListener::bind(&setup.socket).unwrap();
        setup.own(&pond).await.unwrap();
        assert!(setup.log().contains("pond serve exited"), "{}", setup.log());
        assert!(setup.endpoint().is_none());
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
        assert!(setup.endpoint().is_none());
    }

    #[tokio::test]
    async fn a_pond_without_port_file_is_named_too_old() {
        let setup = Setup::new().await;
        let pond = setup.fake_pond(false, "exit 2");
        let _listener = UnixListener::bind(&setup.socket).unwrap();
        setup.own(&pond).await.unwrap();
        assert!(setup.log().contains("too old"), "{}", setup.log());
        assert!(setup.log().contains("upgrade pond"), "{}", setup.log());
    }

    #[test]
    fn start_spawns_an_owner_unless_the_lock_is_held() {
        let sandbox = Sandbox::new();
        let dir = sandbox.origin().dir;
        let spawned = std::cell::Cell::new(0);
        let spawn = || {
            spawned.set(spawned.get() + 1);
            Ok(())
        };
        start(&dir, spawn).unwrap();
        assert_eq!(spawned.get(), 1);

        let held = try_lock(&dir.lock()).unwrap().unwrap();
        start(&dir, spawn).unwrap();
        assert_eq!(spawned.get(), 1, "an owner holds the lock");
        drop(held);
    }

    #[test]
    fn tokens_differ() {
        assert_ne!(random_token(), random_token());
        assert_eq!(random_token().len(), 32);
    }
}
