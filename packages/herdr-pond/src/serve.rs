//! Finding a usable `pond serve` for the desk: this herdr server's published
//! endpoint, else a desk-owned fallback child, both vetted by the capability
//! probe. The per-server state layout and the serve spawn/teardown are
//! shared with the daemon.

use std::fs;
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

use crate::api::{SQL_PATH, post, sql_deadline};
use crate::config::{Config, log_line, log_stdio, write_atomic};
use crate::herdr;
use crate::types::{ApiError, READY_SQL, SqlRequest, SqlResponse};

/// Store open (seconds on S3) happens before `pond serve` binds.
pub(crate) const PORT_DEADLINE: Duration = Duration::from_secs(180);
const PROBE_TIMEOUT_SECS: u64 = 5;
const FALLBACK_GRACE: Duration = Duration::from_secs(2);
const PORT_POLL: Duration = Duration::from_millis(100);
const TERMINATE_POLL: Duration = Duration::from_millis(25);
/// clap's usage-error exit: a pond from before `--port-file` rejects the flag
/// with it, before binding anything.
const USAGE_ERROR_EXIT: i32 = 2;

/// `STATE_DIR/serve/<sockhash>/`: herdr keys plugin state by plugin id only,
/// so two herdr servers on one machine share the state dir - everything a
/// serve owns is keyed by the server's socket instead.
#[derive(Debug, Clone)]
pub(crate) struct ServeDir {
    root: PathBuf,
}

impl ServeDir {
    pub(crate) fn new(state_dir: &Path, socket: &Path) -> Self {
        Self {
            root: state_dir.join("serve").join(sockhash(socket)),
        }
    }

    pub(crate) fn from_env() -> anyhow::Result<Self> {
        Ok(Self::new(&herdr::state_dir()?, &herdr::socket_path()?))
    }

    pub(crate) fn lock(&self) -> PathBuf {
        self.root.join("lock")
    }

    pub(crate) fn endpoint(&self) -> PathBuf {
        self.root.join("endpoint")
    }

    pub(crate) fn daemon_log(&self) -> PathBuf {
        self.root.join("daemon.log")
    }

    pub(crate) fn port_file(&self, owner: &str) -> PathBuf {
        self.root.join(format!("{owner}.port"))
    }

    fn desk_log(&self) -> PathBuf {
        self.root.join("desk-serve.log")
    }
}

/// FNV-1a over the canonical socket path: stable across builds and processes,
/// unlike std's hasher.
fn sockhash(socket: &Path) -> String {
    let canonical = fs::canonicalize(socket).unwrap_or_else(|_| socket.to_path_buf());
    let hash = canonical
        .as_os_str()
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    format!("{:012x}", hash & 0xffff_ffff_ffff)
}

/// The published record of a daemon-owned serve. The token names the owner,
/// so an exiting owner never removes a successor's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Endpoint {
    pub port: u16,
    pub token: String,
}

impl Endpoint {
    pub(crate) fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

/// A missing or malformed record is no record.
pub(crate) fn read_endpoint(path: &Path) -> Option<Endpoint> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

pub(crate) fn write_endpoint(path: &Path, endpoint: &Endpoint) -> std::io::Result<()> {
    let json = serde_json::to_vec(endpoint).map_err(std::io::Error::other)?;
    write_atomic(path, &json)
}

pub(crate) fn remove_endpoint_if_owned(path: &Path, token: &str) -> bool {
    read_endpoint(path).is_some_and(|endpoint| endpoint.token == token)
        && fs::remove_file(path).is_ok()
}

/// The published endpoint's base URL, if it answers the probe.
pub(crate) async fn live_endpoint(client: &reqwest::Client, dir: &ServeDir) -> Option<String> {
    let base_url = read_endpoint(&dir.endpoint())?.base_url();
    probe(client, &base_url).await.ok().map(|()| base_url)
}

/// `SELECT 1` over `/v1/x/sql`: proves both a live pond and one new enough
/// for the desk. A 405 from `/v1/search` would prove neither.
pub(crate) async fn probe(client: &reqwest::Client, base_url: &str) -> Result<(), ApiError> {
    let request = SqlRequest::new(READY_SQL.to_owned(), 1, PROBE_TIMEOUT_SECS);
    let deadline = sql_deadline(PROBE_TIMEOUT_SECS);
    let response: SqlResponse = post(client, base_url, SQL_PATH, &request, deadline).await?;
    if response.rows.is_empty() {
        return Err(ApiError::Decode(format!(
            "{base_url} answered the readiness probe with no rows"
        )));
    }
    Ok(())
}

/// The address from a `--port-file` (`host:port`, written atomically after bind).
pub(crate) fn read_port_file(path: &Path) -> Option<SocketAddr> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// A spawned `pond serve`, terminated (and its port file removed) on drop.
/// Termination blocks for up to `grace`, so async code drops one through
/// [`retire`].
pub(crate) struct ServeChild {
    child: Child,
    port_file: PathBuf,
    log: PathBuf,
    grace: Duration,
}

impl ServeChild {
    /// `pond serve` bound to loopback on a free port. `--host` is explicit
    /// because an inherited `POND_HOST` would otherwise rebind it; stdio goes
    /// to `log` because serve's output would corrupt the TUI or pin a herdr slot.
    pub(crate) fn spawn(
        pond: &Path,
        port_file: PathBuf,
        log: PathBuf,
        grace: Duration,
    ) -> std::io::Result<Self> {
        let _ = fs::remove_file(&port_file);
        let child = log_stdio(
            Command::new(pond)
                .args(["serve", "--host", "127.0.0.1", "--port", "0", "--port-file"])
                .arg(&port_file),
            &log,
        )?
        .spawn()?;
        Ok(Self {
            child,
            port_file,
            log,
            grace,
        })
    }

    pub(crate) fn id(&self) -> u32 {
        self.child.id()
    }

    /// Why serve is gone, once it has exited.
    pub(crate) fn exited(&mut self) -> Option<String> {
        match self.child.try_wait() {
            Ok(None) => None,
            Ok(Some(status)) => Some(format!("pond serve exited ({status})")),
            Err(error) => Some(format!("cannot watch pond serve: {error}")),
        }
    }

    /// Waits for serve to bind and publish its port.
    pub(crate) async fn listening(&mut self, deadline: Duration) -> Result<SocketAddr, ApiError> {
        let started = Instant::now();
        loop {
            if let Some(addr) = read_port_file(&self.port_file) {
                return Ok(addr);
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                if status.code() == Some(USAGE_ERROR_EXIT) {
                    return Err(ApiError::PondTooOld);
                }
                return Err(ApiError::Unreachable(format!(
                    "pond serve exited ({status}) before listening - see {}",
                    self.log.display()
                )));
            }
            if started.elapsed() > deadline {
                return Err(ApiError::Unreachable(format!(
                    "pond serve did not listen within {}s - see {}",
                    deadline.as_secs(),
                    self.log.display()
                )));
            }
            tokio::time::sleep(PORT_POLL).await;
        }
    }
}

impl Drop for ServeChild {
    fn drop(&mut self) {
        terminate(&mut self.child, self.grace);
        let _ = fs::remove_file(&self.port_file);
    }
}

/// Drops `serve` on the blocking pool; await the handle to know it is gone.
pub(crate) fn retire(serve: ServeChild) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || drop(serve))
}

/// SIGTERM, a bounded wait, then SIGKILL and reap: serve's own drain bounds
/// only the HTTP side, not process teardown.
fn terminate(child: &mut Child, grace: Duration) {
    if !matches!(child.try_wait(), Ok(None)) {
        return;
    }
    if let Ok(pid) = i32::try_from(child.id()) {
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if !matches!(child.try_wait(), Ok(None)) {
            return;
        }
        std::thread::sleep(TERMINATE_POLL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// A desk-owned `pond serve`, torn down when the desk drops it - on every
/// graceful exit. A SIGKILLed desk orphans it (accepted v1 risk, README).
pub(crate) struct Fallback {
    serve: ServeChild,
    base_url: String,
}

/// Where the desk finds its serve: this herdr server's state plus the plugin
/// config that names `pond`.
pub(crate) struct Origin {
    pub dir: ServeDir,
    pub config_dir: PathBuf,
}

impl Origin {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            dir: ServeDir::from_env()?,
            config_dir: herdr::config_dir()?,
        })
    }
}

pub(crate) struct Connection {
    pub base_url: String,
    pub fallback: Option<Fallback>,
}

/// The daemon's endpoint when it probes live, else the desk's existing
/// fallback when it still does, else a freshly spawned fallback.
pub(crate) async fn connect(
    client: &reqwest::Client,
    origin: &Origin,
    fallback: Option<Fallback>,
) -> Result<Connection, ApiError> {
    if let Some(base_url) = live_endpoint(client, &origin.dir).await {
        if let Some(fallback) = fallback {
            retire(fallback.serve);
        }
        return Ok(Connection {
            base_url,
            fallback: None,
        });
    }
    if let Some(fallback) = fallback {
        if probe(client, &fallback.base_url).await.is_ok() {
            return Ok(Connection {
                base_url: fallback.base_url.clone(),
                fallback: Some(fallback),
            });
        }
        retire(fallback.serve);
    }
    let fallback = spawn_fallback(origin).await?;
    if let Err(error) = probe(client, &fallback.base_url).await {
        retire(fallback.serve);
        return Err(error);
    }
    Ok(Connection {
        base_url: fallback.base_url.clone(),
        fallback: Some(fallback),
    })
}

async fn spawn_fallback(origin: &Origin) -> Result<Fallback, ApiError> {
    let log = origin.dir.desk_log();
    let pond = Config::pond(&origin.config_dir, &log)
        .map_err(|error| ApiError::Unreachable(format!("{error:#}")))?;
    let port_file = origin
        .dir
        .port_file(&format!("desk.{}", std::process::id()));
    log_line(&log, &format!("desk: starting fallback {}", pond.display()));
    let mut serve = ServeChild::spawn(&pond, port_file, log, FALLBACK_GRACE).map_err(|error| {
        ApiError::Unreachable(format!("cannot start {}: {error}", pond.display()))
    })?;
    match serve.listening(PORT_DEADLINE).await {
        Ok(addr) => Ok(Fallback {
            serve,
            base_url: format!("http://{addr}"),
        }),
        Err(error) => {
            retire(serve);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::api::client;
    use crate::fake_pond::{
        FakePond, Reply, Sandbox, alive, dead_port, dead_url, endpoint, golden, write_script,
    };

    async fn ready_pond() -> FakePond {
        FakePond::with_sql(
            vec![("SELECT 1", Reply::json(golden::SQL_READY))],
            Reply::json(golden::SEARCH),
        )
        .await
    }

    /// A fake serve publishing `addr` that stays up.
    fn fake_serve(sandbox: &Sandbox, addr: &str) -> Origin {
        sandbox.fake_serve(Some(addr), "exec sleep 30");
        sandbox.origin()
    }

    #[test]
    fn sockhash_is_stable_and_resolves_symlinks() {
        let sandbox = Sandbox::new();
        let socket = sandbox.path("herdr.sock");
        fs::write(&socket, "").unwrap();
        let link = sandbox.path("link.sock");
        std::os::unix::fs::symlink(&socket, &link).unwrap();
        assert_eq!(sockhash(&socket), sockhash(&link));
        assert_eq!(sockhash(&socket).len(), 12);
        assert_ne!(sockhash(&socket), sockhash(&sandbox.path("other.sock")));
        assert_eq!(sockhash(Path::new("/a")), sockhash(Path::new("/a")));
    }

    #[test]
    fn endpoint_is_removed_only_by_its_owner() {
        let sandbox = Sandbox::new();
        let path = sandbox.path("state/endpoint");
        write_endpoint(&path, &endpoint(9, "mine")).unwrap();
        assert_eq!(read_endpoint(&path), Some(endpoint(9, "mine")));
        assert!(!remove_endpoint_if_owned(&path, "theirs"));
        assert!(path.exists());
        assert!(remove_endpoint_if_owned(&path, "mine"));
        assert!(!path.exists());
        assert!(!remove_endpoint_if_owned(&path, "mine"));
    }

    #[test]
    fn malformed_endpoint_or_port_file_is_absent() {
        let sandbox = Sandbox::new();
        let path = sandbox.path("endpoint");
        for text in ["", "{", r#"{"port":"x"}"#, r#"{"port":1,"pid":2}"#] {
            fs::write(&path, text).unwrap();
            assert_eq!(read_endpoint(&path), None, "{text}");
            assert!(!remove_endpoint_if_owned(&path, "t"));
        }
        fs::write(&path, "127.0.0.1:54321\n").unwrap();
        assert_eq!(
            read_port_file(&path),
            Some("127.0.0.1:54321".parse().unwrap())
        );
        fs::write(&path, "127.0.0.1:").unwrap();
        assert_eq!(read_port_file(&path), None);
    }

    #[tokio::test]
    async fn probe_tells_ready_from_old_from_dead() {
        let client = client().unwrap();
        let ready = ready_pond().await;
        probe(&client, &ready.base_url).await.unwrap();
        assert!(ready.recorded()[0].body.contains(r#""limit":1"#));

        let old = FakePond::start(|_, _| Reply::plain(404, "")).await;
        assert_eq!(
            probe(&client, &old.base_url).await,
            Err(ApiError::PondTooOld)
        );

        assert!(matches!(
            probe(&client, &dead_url()).await,
            Err(ApiError::Unreachable(_))
        ));
    }

    #[tokio::test]
    async fn live_endpoint_is_used_without_a_fallback() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, "127.0.0.1:1");
        write_endpoint(&origin.dir.endpoint(), &endpoint(pond.port(), "t")).unwrap();
        let connection = connect(&client().unwrap(), &origin, None).await.unwrap();
        assert_eq!(connection.base_url, pond.base_url);
        assert!(connection.fallback.is_none());
        assert!(!sandbox.path("calls").exists(), "no pond spawned");
    }

    #[tokio::test]
    async fn dead_endpoint_falls_back_to_an_owned_child() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, pond.addr());
        write_endpoint(&origin.dir.endpoint(), &endpoint(dead_port(), "t")).unwrap();

        let connection = connect(&client().unwrap(), &origin, None).await.unwrap();
        assert_eq!(connection.base_url, pond.base_url);
        let fallback = connection.fallback.expect("fallback child");
        let calls = sandbox.lines("calls");
        assert!(
            calls[0].starts_with("serve --host 127.0.0.1 --port 0 --port-file "),
            "{calls:?}"
        );
        let log = fs::read_to_string(origin.dir.desk_log()).unwrap();
        assert!(log.contains("serve stdout") && log.contains("serve stderr"));

        let pid = fallback.serve.id();
        let port_file = fallback.serve.port_file.clone();
        assert!(alive(pid));
        drop(fallback);
        assert!(!alive(pid), "fallback serve survived the desk");
        assert!(!port_file.exists());
    }

    #[tokio::test]
    async fn a_live_fallback_is_kept_on_reconnect() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, pond.addr());
        let client = client().unwrap();
        let first = connect(&client, &origin, None).await.unwrap();
        let pid = first.fallback.as_ref().unwrap().serve.id();
        let second = connect(&client, &origin, first.fallback).await.unwrap();
        assert_eq!(second.fallback.as_ref().unwrap().serve.id(), pid);
        assert_eq!(sandbox.lines("calls").len(), 1);
    }

    #[tokio::test]
    async fn fallback_that_dies_before_listening_names_its_log() {
        let sandbox = Sandbox::new();
        let pond = write_script(&sandbox.path("bin/pond"), "echo 'no store' >&2; exit 3");
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        let origin = sandbox.origin();
        let Err(ApiError::Unreachable(reason)) = connect(&client().unwrap(), &origin, None).await
        else {
            panic!("expected Unreachable");
        };
        assert!(reason.contains("desk-serve.log"), "{reason}");
        assert!(
            fs::read_to_string(origin.dir.desk_log())
                .unwrap()
                .contains("no store")
        );
    }

    #[tokio::test]
    async fn a_pond_that_rejects_port_file_is_too_old() {
        let sandbox = Sandbox::new();
        let pond = write_script(
            &sandbox.path("bin/pond"),
            "echo \"error: unexpected argument '--port-file' found\" >&2; exit 2",
        );
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        let result = connect(&client().unwrap(), &sandbox.origin(), None).await;
        assert!(matches!(result, Err(ApiError::PondTooOld)));
    }

    #[tokio::test]
    async fn missing_pond_names_the_config_key() {
        let sandbox = Sandbox::new();
        sandbox.write_config("pond_bin = \"/nonexistent/pond\"\n");
        let origin = sandbox.origin();
        let Err(ApiError::Unreachable(reason)) = connect(&client().unwrap(), &origin, None).await
        else {
            panic!("expected Unreachable");
        };
        assert!(reason.contains("pond_bin"), "{reason}");
    }
}
