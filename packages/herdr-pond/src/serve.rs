//! Finding a usable `pond serve` for the desk: this herdr server's published
//! endpoint, else a desk-owned fallback child, both vetted by the capability
//! probe. The per-server state layout and the serve spawn/teardown are
//! shared with the daemon.

use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

use crate::api::{SQL_PATH, Socket, sql_deadline};
use crate::config::{Config, log_line, log_stdio, write_atomic};
use crate::herdr;
use crate::types::{ApiError, READY_SQL, SqlRequest, SqlResponse};

/// Store open (seconds on S3) happens before `pond serve` binds.
pub(crate) const READY_DEADLINE: Duration = Duration::from_secs(180);
const PROBE_TIMEOUT_SECS: u64 = 5;
const FALLBACK_GRACE: Duration = Duration::from_secs(2);
const READY_POLL: Duration = Duration::from_millis(100);
const TERMINATE_POLL: Duration = Duration::from_millis(25);
/// clap's usage-error exit, for any bad flag or env value; only a rejection
/// naming `--socket` marks a pond from before the flag.
const USAGE_ERROR_EXIT: i32 = 2;
/// Bind env `pond serve` reads for `--host`/`--port`: clap counts an env value
/// as given, so an inherited one would conflict with `--socket`.
const BIND_ENV: [&str; 2] = ["POND_HOST", "POND_PORT"];

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

    pub(crate) fn socket(&self, owner: &str) -> PathBuf {
        self.root.join(format!("{owner}.sock"))
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
    pub socket: PathBuf,
    pub token: String,
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

/// The published endpoint's socket, if it answers the probe.
pub(crate) async fn live_endpoint(dir: &ServeDir) -> Option<Socket> {
    let socket = Socket::new(read_endpoint(&dir.endpoint())?.socket).ok()?;
    probe(&socket).await.ok().map(|()| socket)
}

/// `SELECT 1` over `/v1/x/sql`: proves both a live pond and one new enough
/// for the desk. A 405 from `/v1/search` would prove neither.
pub(crate) async fn probe(socket: &Socket) -> Result<(), ApiError> {
    let request = SqlRequest::new(READY_SQL.to_owned(), 1, PROBE_TIMEOUT_SECS);
    let deadline = sql_deadline(PROBE_TIMEOUT_SECS);
    let response: SqlResponse = socket.post(SQL_PATH, &request, deadline).await?;
    if response.rows.is_empty() {
        return Err(ApiError::Decode(format!(
            "{} answered the readiness probe with no rows",
            socket.path.display()
        )));
    }
    Ok(())
}

/// A spawned `pond serve`, terminated (and its socket removed) on drop.
/// Termination blocks for up to `grace`, so async code drops one through
/// [`retire`].
pub(crate) struct ServeChild {
    child: Child,
    socket: PathBuf,
    log: PathBuf,
    /// Where this child's output starts in the shared `log`.
    log_start: u64,
    grace: Duration,
}

impl ServeChild {
    /// `pond serve --socket <socket>`; stdio goes to `log` because serve's
    /// output would corrupt the TUI or pin a herdr slot. A leftover socket at
    /// the path - a dead serve's, or an unsupervised orphan's - is removed
    /// first, so only this child can answer there.
    pub(crate) fn spawn(
        pond: &Path,
        socket: PathBuf,
        log: PathBuf,
        grace: Duration,
    ) -> std::io::Result<Self> {
        let _ = fs::remove_file(&socket);
        let mut command = serve_command(pond, &socket);
        log_stdio(&mut command, &log)?;
        let log_start = fs::metadata(&log).map_or(0, |meta| meta.len());
        let child = command.spawn()?;
        Ok(Self {
            child,
            socket,
            log,
            log_start,
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

    /// Waits for serve to answer the capability probe on its socket. The
    /// socket file alone proves nothing: serve binds only after the store
    /// opens, and a stale one refuses connections.
    pub(crate) async fn ready(&mut self, deadline: Duration) -> Result<Socket, ApiError> {
        let socket = Socket::new(self.socket.clone())?;
        let started = Instant::now();
        let mut last_probe = String::new();
        loop {
            if self.socket.exists() {
                match probe(&socket).await {
                    Ok(()) => return Ok(socket),
                    Err(ApiError::PondTooOld) => return Err(ApiError::PondTooOld),
                    Err(error) => last_probe = format!(" (last probe: {error})"),
                }
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                if status.code() == Some(USAGE_ERROR_EXIT) && self.rejected_socket_flag() {
                    return Err(ApiError::PondTooOld);
                }
                return Err(ApiError::Unreachable(format!(
                    "pond serve exited ({status}) before listening - see {}",
                    self.log.display()
                )));
            }
            if started.elapsed() > deadline {
                return Err(ApiError::Unreachable(format!(
                    "pond serve did not answer on {} within {}s{last_probe} - see {}",
                    self.socket.display(),
                    deadline.as_secs(),
                    self.log.display()
                )));
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }

    fn rejected_socket_flag(&self) -> bool {
        fs::read(&self.log).is_ok_and(|log| {
            usize::try_from(self.log_start)
                .ok()
                .and_then(|start| log.get(start..))
                .is_some_and(|output| String::from_utf8_lossy(output).contains("--socket"))
        })
    }
}

impl Drop for ServeChild {
    fn drop(&mut self) {
        terminate(&mut self.child, self.grace);
        let _ = fs::remove_file(&self.socket);
    }
}

fn serve_command(pond: &Path, socket: &Path) -> Command {
    let mut command = Command::new(pond);
    command.args(["serve", "--socket"]).arg(socket);
    for var in BIND_ENV {
        command.env_remove(var);
    }
    command
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
    socket: Socket,
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
    pub socket: Socket,
    pub fallback: Option<Fallback>,
}

/// The daemon's endpoint when it probes live, else the desk's existing
/// fallback when it still does, else a freshly spawned fallback.
pub(crate) async fn connect(
    origin: &Origin,
    fallback: Option<Fallback>,
) -> Result<Connection, ApiError> {
    if let Some(socket) = live_endpoint(&origin.dir).await {
        if let Some(fallback) = fallback {
            retire(fallback.serve);
        }
        return Ok(Connection {
            socket,
            fallback: None,
        });
    }
    if let Some(fallback) = fallback {
        if probe(&fallback.socket).await.is_ok() {
            return Ok(Connection {
                socket: fallback.socket.clone(),
                fallback: Some(fallback),
            });
        }
        retire(fallback.serve);
    }
    let fallback = spawn_fallback(origin).await?;
    Ok(Connection {
        socket: fallback.socket.clone(),
        fallback: Some(fallback),
    })
}

async fn spawn_fallback(origin: &Origin) -> Result<Fallback, ApiError> {
    // One socket per spawn: a retiring fallback removes its own on drop,
    // while its successor may already be listening there.
    static SPAWNED: AtomicU32 = AtomicU32::new(0);
    let log = origin.dir.desk_log();
    let pond = Config::pond(&origin.config_dir, &log)
        .map_err(|error| ApiError::Unreachable(format!("{error:#}")))?;
    let socket = origin.dir.socket(&format!(
        "desk.{}.{}",
        std::process::id(),
        SPAWNED.fetch_add(1, Ordering::Relaxed)
    ));
    log_line(&log, &format!("desk: starting fallback {}", pond.display()));
    let mut serve = ServeChild::spawn(&pond, socket, log, FALLBACK_GRACE).map_err(|error| {
        ApiError::Unreachable(format!("cannot start {}: {error}", pond.display()))
    })?;
    match serve.ready(READY_DEADLINE).await {
        Ok(socket) => Ok(Fallback { serve, socket }),
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
    use crate::fake_pond::{
        FakePond, Reply, Sandbox, alive, endpoint, golden, missing_socket, stale_socket,
        write_script,
    };

    async fn ready_pond() -> FakePond {
        FakePond::with_sql(
            vec![("SELECT 1", Reply::json(golden::SQL_READY))],
            Reply::json(golden::SEARCH),
        )
        .await
    }

    /// A fake serve answering through `pond` that stays up.
    fn fake_serve(sandbox: &Sandbox, pond: &FakePond) -> Origin {
        sandbox.fake_serve(Some(&pond.socket), "exec sleep 30");
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
    fn endpoint_round_trips_and_is_removed_only_by_its_owner() {
        let sandbox = Sandbox::new();
        let path = sandbox.path("state/endpoint");
        let socket = sandbox.path("state/owner.sock");
        write_endpoint(&path, &endpoint(&socket, "mine")).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"socket": socket.display().to_string(), "token": "mine"})
        );
        assert_eq!(read_endpoint(&path), Some(endpoint(&socket, "mine")));
        assert!(!remove_endpoint_if_owned(&path, "theirs"));
        assert!(path.exists());
        assert!(remove_endpoint_if_owned(&path, "mine"));
        assert!(!path.exists());
        assert!(!remove_endpoint_if_owned(&path, "mine"));
    }

    #[test]
    fn malformed_or_port_endpoint_is_absent() {
        let sandbox = Sandbox::new();
        let path = sandbox.path("endpoint");
        for text in [
            "",
            "{",
            r#"{"socket":1,"token":"t"}"#,
            r#"{"port":1,"token":"t"}"#,
        ] {
            fs::write(&path, text).unwrap();
            assert_eq!(read_endpoint(&path), None, "{text}");
            assert!(!remove_endpoint_if_owned(&path, "t"));
        }
    }

    #[tokio::test]
    async fn probe_tells_ready_from_old_from_dead() {
        let ready = ready_pond().await;
        probe(&ready.connect()).await.unwrap();
        assert!(ready.recorded()[0].body.contains(r#""limit":1"#));

        let old = FakePond::start(|_, _| Reply::plain(404, "")).await;
        assert_eq!(probe(&old.connect()).await, Err(ApiError::PondTooOld));

        let sandbox = Sandbox::new();
        for dead in [missing_socket(), stale_socket(&sandbox.path("stale.sock"))] {
            assert!(matches!(probe(&dead).await, Err(ApiError::Unreachable(_))));
        }
    }

    #[tokio::test]
    async fn live_endpoint_is_used_without_a_fallback() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = sandbox.origin();
        sandbox.fake_serve(None, "exec sleep 30");
        write_endpoint(&origin.dir.endpoint(), &endpoint(&pond.socket, "t")).unwrap();
        let connection = connect(&origin, None).await.unwrap();
        assert_eq!(connection.socket.path, pond.socket);
        assert!(connection.fallback.is_none());
        assert!(!sandbox.path("calls").exists(), "no pond spawned");
    }

    #[tokio::test]
    async fn dead_endpoint_falls_back_to_an_owned_child() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, &pond);
        let stale = stale_socket(&origin.dir.socket("owner"));
        write_endpoint(&origin.dir.endpoint(), &endpoint(&stale.path, "t")).unwrap();

        let connection = connect(&origin, None).await.unwrap();
        let fallback = connection.fallback.expect("fallback child");
        let socket = fallback.serve.socket.clone();
        assert_eq!(connection.socket.path, socket);
        assert_eq!(pond.recorded().len(), 1, "one readiness probe");
        let calls = sandbox.lines("calls");
        assert_eq!(calls, [format!("serve --socket {}", socket.display())]);
        let name = socket.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            name.starts_with(&format!("desk.{}.", std::process::id())) && name.ends_with(".sock"),
            "{name}"
        );
        let log = fs::read_to_string(origin.dir.desk_log()).unwrap();
        assert!(log.contains("serve stdout") && log.contains("serve stderr"));

        let pid = fallback.serve.id();
        assert!(alive(pid));
        drop(fallback);
        assert!(!alive(pid), "fallback serve survived the desk");
        assert!(fs::symlink_metadata(&socket).is_err(), "socket left behind");
    }

    #[test]
    fn serve_gets_only_the_socket_and_never_the_bind_env() {
        let command = serve_command(Path::new("/bin/pond"), Path::new("/s/owner.sock"));
        let args: Vec<_> = command.get_args().collect();
        assert_eq!(args, ["serve", "--socket", "/s/owner.sock"]);
        let removed: Vec<_> = command
            .get_envs()
            .filter(|(_, value)| value.is_none())
            .map(|(key, _)| key)
            .collect();
        assert_eq!(removed, BIND_ENV);
    }

    #[tokio::test]
    async fn a_socket_that_refuses_is_not_ready() {
        let sandbox = Sandbox::new();
        let stale = sandbox.path("stale.sock");
        stale_socket(&stale);
        let pond = sandbox.fake_serve(Some(&stale), "exec sleep 30");
        let mut serve = ServeChild::spawn(
            &pond,
            sandbox.path("s.sock"),
            sandbox.path("log"),
            FALLBACK_GRACE,
        )
        .unwrap();
        let result = serve.ready(Duration::from_millis(500)).await;
        let Err(ApiError::Unreachable(reason)) = result else {
            panic!("expected Unreachable, got {result:?}");
        };
        assert!(
            reason.contains("did not answer") && reason.contains("last probe"),
            "{reason}"
        );
        retire(serve).await.unwrap();
    }

    #[tokio::test]
    async fn a_live_fallback_is_kept_on_reconnect() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, &pond);
        let first = connect(&origin, None).await.unwrap();
        let pid = first.fallback.as_ref().unwrap().serve.id();
        let second = connect(&origin, first.fallback).await.unwrap();
        assert_eq!(second.fallback.as_ref().unwrap().serve.id(), pid);
        assert_eq!(sandbox.lines("calls").len(), 1);
    }

    #[tokio::test]
    async fn fallback_that_dies_before_listening_names_its_log() {
        let sandbox = Sandbox::new();
        let pond = write_script(&sandbox.path("bin/pond"), "echo 'no store' >&2; exit 3");
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        let origin = sandbox.origin();
        let Err(ApiError::Unreachable(reason)) = connect(&origin, None).await else {
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
    async fn a_pond_that_rejects_socket_is_too_old() {
        let sandbox = Sandbox::new();
        let pond = write_script(
            &sandbox.path("bin/pond"),
            "echo \"error: unexpected argument '--socket' found\" >&2; exit 2",
        );
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        let result = connect(&sandbox.origin(), None).await;
        assert!(matches!(result, Err(ApiError::PondTooOld)));
    }

    #[tokio::test]
    async fn another_usage_error_is_not_too_old() {
        let sandbox = Sandbox::new();
        let pond = write_script(
            &sandbox.path("bin/pond"),
            "echo \"error: invalid value 'x' for '--storage-path <PATH>'\" >&2; exit 2",
        );
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        let origin = sandbox.origin();
        let log = origin.dir.desk_log();
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(&log, "error: unexpected argument '--socket' found\n").unwrap();
        let Err(ApiError::Unreachable(reason)) = connect(&origin, None).await else {
            panic!("expected Unreachable");
        };
        assert!(reason.contains("desk-serve.log"), "{reason}");
    }

    #[tokio::test]
    async fn a_retired_fallback_keeps_its_successors_socket() {
        let sandbox = Sandbox::new();
        let origin = sandbox.origin();
        let first_pond = ready_pond().await;
        sandbox.fake_serve(Some(&first_pond.socket), "trap '' TERM; exec sleep 30");
        let first = connect(&origin, None).await.unwrap();
        let retired = first.fallback.as_ref().unwrap().serve.id();

        drop(first_pond);
        let second_pond = ready_pond().await;
        sandbox.fake_serve(Some(&second_pond.socket), "exec sleep 30");
        let second = connect(&origin, first.fallback).await.unwrap();
        let socket = second.fallback.as_ref().unwrap().serve.socket.clone();
        assert_eq!(second.socket.path, socket);
        assert_eq!(second_pond.recorded().len(), 1);

        let deadline = Instant::now() + FALLBACK_GRACE * 3;
        while alive(retired) {
            assert!(Instant::now() < deadline, "the retired fallback survived");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(socket.exists(), "the retired fallback removed it");
    }

    #[tokio::test]
    async fn missing_pond_names_the_config_key() {
        let sandbox = Sandbox::new();
        sandbox.write_config("pond_bin = \"/nonexistent/pond\"\n");
        let origin = sandbox.origin();
        let Err(ApiError::Unreachable(reason)) = connect(&origin, None).await else {
            panic!("expected Unreachable");
        };
        assert!(reason.contains("pond_bin"), "{reason}");
    }
}
