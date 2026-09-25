//! Finding a usable `pond serve` for the desk: this herdr server's published
//! endpoint, else a desk-owned fallback child (plan 5.7), both vetted by the
//! capability probe (plan 5.8). The per-server state layout and the serve
//! spawn/teardown are shared with the daemon.

use std::fs;
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

use crate::api::{SQL_PATH, post, sql_request};
use crate::config::{Config, log_line, open_log, write_atomic};
use crate::herdr;
use crate::types::{ApiError, READY_SQL, SqlResponse};

/// Store open (seconds on S3) happens before `pond serve` binds.
pub(crate) const PORT_DEADLINE: Duration = Duration::from_secs(180);
const PROBE_DEADLINE: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT_SECS: u64 = 5;
const FALLBACK_GRACE: Duration = Duration::from_secs(2);
const PORT_POLL: Duration = Duration::from_millis(100);

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
    pub pid: u32,
    pub token: String,
    pub pond_version: String,
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

/// `SELECT 1` over `/v1/x/sql`: proves both a live pond and one new enough
/// for the desk. A 405 from `/v1/search` would prove neither.
pub(crate) async fn probe(client: &reqwest::Client, base_url: &str) -> Result<(), ApiError> {
    let request = sql_request(READY_SQL.to_owned(), 1, PROBE_TIMEOUT_SECS);
    let response: SqlResponse = post(client, base_url, SQL_PATH, &request, PROBE_DEADLINE).await?;
    if response.rows.is_empty() {
        return Err(ApiError::Decode(format!(
            "{base_url} answered the readiness probe with no rows"
        )));
    }
    Ok(())
}

/// `pond serve` bound to loopback on a free port. `--host` is explicit
/// because an inherited `POND_HOST` would otherwise rebind it; stdio goes to
/// `log` because serve's output would corrupt the TUI or pin a herdr slot.
pub(crate) fn spawn_serve(pond: &Path, port_file: &Path, log: &Path) -> std::io::Result<Child> {
    let _ = fs::remove_file(port_file);
    let out = open_log(log)?;
    let err = out.try_clone()?;
    Command::new(pond)
        .args(["serve", "--host", "127.0.0.1", "--port", "0", "--port-file"])
        .arg(port_file)
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err)
        .spawn()
}

/// The base URL from a `--port-file` (`host:port`, written atomically after bind).
pub(crate) fn read_port_file(path: &Path) -> Option<SocketAddr> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// SIGTERM, a bounded wait, then SIGKILL and reap: serve's own drain bounds
/// only the HTTP side, not process teardown.
pub(crate) fn terminate(child: &mut Child, grace: Duration) {
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
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// A desk-owned `pond serve`, torn down when the desk drops it - on every
/// graceful exit. A SIGKILLed desk orphans it (accepted v1 risk, README).
pub(crate) struct ServeChild {
    child: Child,
    port_file: PathBuf,
    base_url: String,
}

impl Drop for ServeChild {
    fn drop(&mut self) {
        terminate(&mut self.child, FALLBACK_GRACE);
        let _ = fs::remove_file(&self.port_file);
    }
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
    pub fallback: Option<ServeChild>,
}

/// The daemon's endpoint when it probes live, else the desk's existing
/// fallback when it still does, else a freshly spawned fallback.
pub(crate) async fn connect(
    client: &reqwest::Client,
    origin: &Origin,
    fallback: Option<ServeChild>,
) -> Result<Connection, ApiError> {
    if let Some(endpoint) = read_endpoint(&origin.dir.endpoint()) {
        let base_url = endpoint.base_url();
        if probe(client, &base_url).await.is_ok() {
            return Ok(Connection {
                base_url,
                fallback: None,
            });
        }
    }
    if let Some(fallback) = fallback
        && probe(client, &fallback.base_url).await.is_ok()
    {
        return Ok(Connection {
            base_url: fallback.base_url.clone(),
            fallback: Some(fallback),
        });
    }
    let fallback = spawn_fallback(origin).await?;
    probe(client, &fallback.base_url).await?;
    Ok(Connection {
        base_url: fallback.base_url.clone(),
        fallback: Some(fallback),
    })
}

async fn spawn_fallback(origin: &Origin) -> Result<ServeChild, ApiError> {
    let log = origin.dir.desk_log();
    let pond = Config::load(&origin.config_dir, &log)
        .resolve_pond(&origin.config_dir)
        .map_err(|error| ApiError::Unreachable(format!("{error:#}")))?;
    let port_file = origin
        .dir
        .port_file(&format!("desk.{}", std::process::id()));
    log_line(&log, &format!("desk: starting fallback {}", pond.display()));
    let child = spawn_serve(&pond, &port_file, &log).map_err(|error| {
        ApiError::Unreachable(format!("cannot start {}: {error}", pond.display()))
    })?;
    let mut serve = ServeChild {
        child,
        port_file,
        base_url: String::new(),
    };
    let started = Instant::now();
    loop {
        if let Some(addr) = read_port_file(&serve.port_file) {
            serve.base_url = format!("http://{addr}");
            return Ok(serve);
        }
        if let Ok(Some(status)) = serve.child.try_wait() {
            return Err(ApiError::Unreachable(format!(
                "pond serve exited ({status}) before listening - see {}",
                log.display()
            )));
        }
        if started.elapsed() > PORT_DEADLINE {
            return Err(ApiError::Unreachable(format!(
                "pond serve did not listen within {}s - see {}",
                PORT_DEADLINE.as_secs(),
                log.display()
            )));
        }
        tokio::time::sleep(PORT_POLL).await;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::api::client;
    use crate::fake_pond::{FakePond, Reply, Sandbox, golden, write_script};

    fn endpoint(port: u16, token: &str) -> Endpoint {
        Endpoint {
            port,
            pid: 1,
            token: token.to_owned(),
            pond_version: "pond 0.20.0".to_owned(),
        }
    }

    fn port_of(base_url: &str) -> u16 {
        base_url.rsplit(':').next().unwrap().parse().unwrap()
    }

    /// A port nothing listens on: bound, then released.
    fn dead_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    async fn ready_pond() -> FakePond {
        FakePond::with_sql(
            vec![("SELECT 1", Reply::json(golden::SQL_READY))],
            Reply::json(golden::SEARCH),
        )
        .await
    }

    /// A fake `pond serve` that records its argv, prints to both streams,
    /// publishes `addr` through `--port-file` and stays up.
    fn fake_serve(sandbox: &Sandbox, addr: &str) -> Origin {
        let pond = write_script(
            &sandbox.path("bin/pond"),
            &format!(
                r#"printf '%s\n' "$*" >> '{calls}'
echo "serve stdout"; echo "serve stderr" >&2
eval "port_file=\${{$#}}"
printf '%s' '{addr}' > "$port_file.tmp" && mv "$port_file.tmp" "$port_file"
exec sleep 30"#,
                calls = sandbox.path("calls").display(),
            ),
        );
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        Origin {
            dir: ServeDir::new(&sandbox.state_dir(), &sandbox.path("herdr.sock")),
            config_dir: sandbox.config_dir(),
        }
    }

    fn alive(pid: u32) -> bool {
        kill(Pid::from_raw(i32::try_from(pid).unwrap()), None).is_ok()
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

        let dead = format!("http://127.0.0.1:{}", dead_port());
        assert!(matches!(
            probe(&client, &dead).await,
            Err(ApiError::Unreachable(_))
        ));
    }

    #[tokio::test]
    async fn live_endpoint_is_used_without_a_fallback() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, "127.0.0.1:1");
        write_endpoint(
            &origin.dir.endpoint(),
            &endpoint(port_of(&pond.base_url), "t"),
        )
        .unwrap();
        let connection = connect(&client().unwrap(), &origin, None).await.unwrap();
        assert_eq!(connection.base_url, pond.base_url);
        assert!(connection.fallback.is_none());
        assert!(!sandbox.path("calls").exists(), "no pond spawned");
    }

    #[tokio::test]
    async fn dead_endpoint_falls_back_to_an_owned_child() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, pond.base_url.trim_start_matches("http://"));
        write_endpoint(&origin.dir.endpoint(), &endpoint(dead_port(), "t")).unwrap();

        let connection = connect(&client().unwrap(), &origin, None).await.unwrap();
        assert_eq!(connection.base_url, pond.base_url);
        let fallback = connection.fallback.expect("fallback child");
        let calls = fs::read_to_string(sandbox.path("calls")).unwrap();
        assert!(
            calls.starts_with("serve --host 127.0.0.1 --port 0 --port-file "),
            "{calls}"
        );
        let log = fs::read_to_string(origin.dir.desk_log()).unwrap();
        assert!(log.contains("serve stdout") && log.contains("serve stderr"));

        let pid = fallback.child.id();
        let port_file = fallback.port_file.clone();
        assert!(alive(pid));
        drop(fallback);
        assert!(!alive(pid), "fallback serve survived the desk");
        assert!(!port_file.exists());
    }

    #[tokio::test]
    async fn a_live_fallback_is_kept_on_reconnect() {
        let sandbox = Sandbox::new();
        let pond = ready_pond().await;
        let origin = fake_serve(&sandbox, pond.base_url.trim_start_matches("http://"));
        let client = client().unwrap();
        let first = connect(&client, &origin, None).await.unwrap();
        let pid = first.fallback.as_ref().unwrap().child.id();
        let second = connect(&client, &origin, first.fallback).await.unwrap();
        assert_eq!(second.fallback.as_ref().unwrap().child.id(), pid);
        assert_eq!(
            fs::read_to_string(sandbox.path("calls"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn fallback_that_dies_before_listening_names_its_log() {
        let sandbox = Sandbox::new();
        let pond = write_script(&sandbox.path("bin/pond"), "echo 'no store' >&2; exit 3");
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        let origin = Origin {
            dir: ServeDir::new(&sandbox.state_dir(), &sandbox.path("herdr.sock")),
            config_dir: sandbox.config_dir(),
        };
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
    async fn missing_pond_names_the_config_key() {
        let sandbox = Sandbox::new();
        sandbox.write_config("pond_bin = \"/nonexistent/pond\"\n");
        let origin = Origin {
            dir: ServeDir::new(&sandbox.state_dir(), &sandbox.path("herdr.sock")),
            config_dir: sandbox.config_dir(),
        };
        let Err(ApiError::Unreachable(reason)) = connect(&client().unwrap(), &origin, None).await
        else {
            panic!("expected Unreachable");
        };
        assert!(reason.contains("pond_bin"), "{reason}");
    }
}
