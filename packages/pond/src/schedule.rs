//! `pond schedule` and `pond service`: the two OS-scheduler registrations.
//!
//! `schedule` registers the periodic `pond sync -q --no-wait`; `service`
//! registers one resident `pond serve --transport http --with-sync`, whose
//! whole point is that the prewarm, the rowmap build, and the FTS postings
//! load once per host instead of once per MCP client process. Both share this
//! module's OS-service machinery - there is no pond supervisor.
//!
//! macOS uses launchd ONLY (cron on macOS runs without the user's GUI
//! context, trips TCC folder-access denials, and silently drops jobs that
//! span sleep). Linux prefers systemd user timers (`Persistent=true` catches
//! up after downtime) and falls back to a fenced crontab block. Windows uses
//! Task Scheduler: the task Execs `pondw.exe`, pond's windowless launcher,
//! and the task XML provides the settings that align it with the
//! launchd/systemd posture (battery-friendly, catch-up after missed runs).
//!
//! The scheduled job is `pond sync -q --no-wait`: NOT `--yes`, so an
//! unattended run can never auto-enable freshly-detected adapters, and
//! `--no-wait` so a tick that lands while another sync holds the per-store
//! lock skips cleanly (exit 0) instead of queueing behind it.
//!
//! Bin-only module: OS-scheduler integration has no library callers.

use anyhow::{Context, Result};
use clap::{Subcommand, ValueEnum};
use pond::output::{dim, line, line_err, paint, red};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ScheduleEvery {
    #[value(name = "5m")]
    M5,
    #[value(name = "15m")]
    M15,
    #[value(name = "1h")]
    H1,
    #[value(name = "6h")]
    H6,
    #[value(name = "1d")]
    D1,
}

impl ScheduleEvery {
    pub(crate) fn secs(self) -> u32 {
        match self {
            Self::M5 => 300,
            Self::M15 => 900,
            Self::H1 => 3_600,
            Self::H6 => 21_600,
            Self::D1 => 86_400,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H6 => "6h",
            Self::D1 => "1d",
        }
    }

    fn from_secs(secs: u32) -> Option<Self> {
        [Self::M5, Self::M15, Self::H1, Self::H6, Self::D1]
            .into_iter()
            .find(|every| every.secs() == secs)
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum ScheduleCmd {
    /// Register the schedule (idempotent: safe to re-run).
    ///
    /// Re-running with a different `--every` replaces the existing
    /// registration; re-running with the same one is a no-op.
    #[command(after_long_help = "Examples:
  pond schedule start              every 5 minutes (the default)
  pond schedule start --every 1h
  pond schedule start --every 1d")]
    Start {
        /// How often to run `pond sync -q --no-wait`.
        #[arg(long, value_enum, default_value_t = ScheduleEvery::M5)]
        every: ScheduleEvery,
    },
    /// Remove the schedule.
    ///
    /// Succeeds (exit 0) when nothing was registered.
    Stop,
    /// Show whether a schedule is active.
    ///
    /// Exit 0 when active, 1 when not configured. A broken-but-registered
    /// schedule still exits 0; the status line carries the problem.
    Status,
    /// Show recent scheduled-sync output.
    Logs {
        /// Number of trailing log lines to print.
        #[arg(long, default_value_t = 50)]
        lines: usize,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ServiceCmd {
    /// Register the resident `pond serve` (idempotent: safe to re-run).
    ///
    /// Re-running with different flags replaces the registration; re-running
    /// with the same ones is a no-op.
    #[command(after_long_help = "Examples:
  pond service start                   http://127.0.0.1:9797/mcp
  pond service start --port 9800
  pond service start --sync-every 15")]
    Start {
        /// Loopback bind address for the resident server.
        #[arg(long, default_value = ServeEndpoint::DEFAULT_HOST)]
        host: String,
        /// Bind port for the resident server.
        #[arg(long, default_value_t = ServeEndpoint::DEFAULT_PORT)]
        port: u16,
        /// Minutes between the resident server's in-process syncs.
        #[arg(long, default_value_t = 5)]
        sync_every: u64,
    },
    /// Remove the registration and stop the resident server.
    ///
    /// Succeeds (exit 0) when nothing was registered.
    Stop,
    /// Show whether the resident server is registered.
    ///
    /// Exit 0 when active, 1 when not configured.
    Status,
    /// Show recent resident-server output.
    Logs {
        /// Number of trailing log lines to print.
        #[arg(long, default_value_t = 50)]
        lines: usize,
    },
}

/// Where a resident `pond serve` listens, and therefore the `/mcp` URL an MCP
/// client registers against.
///
/// Loopback by default and deliberately so: the `/mcp` route validates the
/// `Host` header against rmcp's allowlist (the MCP spec's DNS-rebinding
/// defence), whose defaults are `localhost`, `127.0.0.1`, and `::1` with no
/// port pinned - so a loopback registration satisfies it on any port without
/// `--allowed-host` widening anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServeEndpoint {
    pub host: String,
    pub port: u16,
}

impl Default for ServeEndpoint {
    fn default() -> Self {
        Self {
            host: Self::DEFAULT_HOST.to_owned(),
            port: Self::DEFAULT_PORT,
        }
    }
}

impl ServeEndpoint {
    pub(crate) const DEFAULT_HOST: &'static str = "127.0.0.1";
    pub(crate) const DEFAULT_PORT: u16 = 9797;

    pub(crate) fn mcp_url(&self) -> String {
        format!("http://{}:{}/mcp", self.host, self.port)
    }

    pub(crate) fn socket(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Whether this endpoint is one rmcp's default `Host` allowlist admits.
    /// A non-loopback bind needs `pond serve --allowed-host <name>`, which is
    /// an operator decision and not something a registration may assume.
    pub(crate) fn is_loopback(&self) -> bool {
        self.host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or_else(|_| self.host.eq_ignore_ascii_case("localhost"))
    }
}

/// One scheduler probe's answer, shared by the `pond status` text line and
/// the JSON document (which needs the fields structured, not pre-rendered).
#[derive(Default)]
pub(crate) struct ScheduleSnapshot {
    pub line: String,
    pub active: bool,
    pub backend: Option<&'static str>,
    pub every: Option<ScheduleEvery>,
    /// A registered schedule that cannot run (e.g. the task's launcher path
    /// no longer exists after a reinstall); the text names the fix.
    pub problem: Option<String>,
}

// ===========================================================================
// Shared across all platforms
// ===========================================================================

/// Internal state of the OS scheduler for the pond-sync registration.
enum State {
    Active {
        backend: &'static str,
        every: Option<ScheduleEvery>,
        /// Registered but unable to run.
        problem: Option<String>,
    },
    Inactive,
}
use State::{Active, Inactive};

impl State {
    /// A healthy registration; only `windows::probe` ever reports a problem,
    /// so every other constructor goes through here.
    fn active(backend: &'static str, every: Option<ScheduleEvery>) -> Self {
        Active {
            backend,
            every,
            problem: None,
        }
    }
}

fn render_state(state: &State) -> String {
    match state {
        Active {
            backend,
            every,
            problem,
        } => {
            let cadence = every
                .map(|every| format!(", every {}", every.label()))
                .unwrap_or_default();
            match problem {
                Some(problem) => format!(
                    "{}  {} ({backend}{cadence}) - {problem}",
                    paint("schedule", dim()),
                    paint("broken", red()),
                ),
                None => format!("{}  active ({backend}{cadence})", paint("schedule", dim())),
            }
        }
        Inactive => format!(
            "{}  not configured - run `pond schedule start` to sync automatically",
            paint("schedule", dim()),
        ),
    }
}

/// The log file path: `<pond_state_dir>/sync.log`. Single source of truth
/// used by both platform modules and the shared `logs()` function.
pub(crate) fn log_path() -> PathBuf {
    crate::syncstate::pond_state_dir().join("sync.log")
}

/// Where a launchd/cron-style registration sends the resident server's
/// stderr. systemd routes to the journal instead (see [`service_logs`]).
pub(crate) fn service_log_path() -> PathBuf {
    crate::syncstate::pond_state_dir().join("serve.log")
}

/// Internal state of the resident-service registration. Separate from
/// [`State`] because the interesting detail is an endpoint, not a cadence.
struct ServiceState {
    backend: &'static str,
    endpoint: Option<ServeEndpoint>,
}

fn render_service_state(state: Option<&ServiceState>) -> String {
    match state {
        Some(ServiceState { backend, endpoint }) => {
            let at = endpoint
                .as_ref()
                .map(|endpoint| format!(", {}", endpoint.mcp_url()))
                .unwrap_or_default();
            format!("{}   active ({backend}{at})", paint("service", dim()))
        }
        None => format!(
            "{}   not configured - run `pond service start` to keep one warm pond serve",
            paint("service", dim()),
        ),
    }
}

/// Registration entry point shared by `pond service start` and the `pond init`
/// MCP section (which registers the resident server before pointing a client
/// at it). `explicit` is the config path the caller resolved, pinned into the
/// unit for the same reason the sync registration pins it.
pub(crate) fn service_start(
    endpoint: &ServeEndpoint,
    sync_every: u64,
    explicit: Option<PathBuf>,
) -> Result<()> {
    if !endpoint.is_loopback() {
        anyhow::bail!(
            "--host {} is not loopback; the resident registration is loopback-only because \
             the /mcp route's Host allowlist is, and widening it is an operator decision - \
             run `pond serve --host {} --allowed-host <name>` under your own supervisor instead",
            endpoint.host,
            endpoint.host,
        );
    }
    service_platform_start(endpoint, sync_every, &config_file(explicit))
}

pub(crate) fn run_service(command: ServiceCmd, config: Option<PathBuf>) -> Result<()> {
    match command {
        ServiceCmd::Start {
            host,
            port,
            sync_every,
        } => service_start(&ServeEndpoint { host, port }, sync_every, config),
        ServiceCmd::Stop => service_platform_stop(),
        ServiceCmd::Status => {
            let state = service_platform_probe()?;
            line(&render_service_state(state.as_ref()))?;
            if state.is_none() {
                std::process::exit(1);
            }
            Ok(())
        }
        ServiceCmd::Logs { lines } => service_logs(lines),
    }
}

/// Wait for the freshly registered server to accept a connection. A cold
/// first start opens the store and builds the rowmap before it binds, which
/// can take minutes, so a timeout is reported as "still starting" rather than
/// as a failure - the registration itself already succeeded.
fn wait_for_endpoint(endpoint: &ServeEndpoint, budget: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + budget;
    let probe = std::time::Duration::from_millis(250);
    loop {
        if let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&endpoint.socket())
            && addrs
                .into_iter()
                .any(|addr| std::net::TcpStream::connect_timeout(&addr, probe).is_ok())
        {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(probe);
    }
}

/// Announce a completed registration: the state line, then where the logs are
/// and whether the endpoint is answering yet.
fn report_service_started(backend: &'static str, endpoint: &ServeEndpoint) -> Result<()> {
    line(&render_service_state(Some(&ServiceState {
        backend,
        endpoint: Some(endpoint.clone()),
    })))?;
    if wait_for_endpoint(endpoint, std::time::Duration::from_secs(15)) {
        line(&format!(
            "{}   {} answering",
            paint("endpoint", dim()),
            endpoint.mcp_url(),
        ))?;
    } else {
        line(&format!(
            "{}   not answering yet - a cold first start builds the rowmap before it binds; \
             `pond service logs` follows it",
            paint("endpoint", dim()),
        ))?;
    }
    Ok(())
}

/// Print the last `lines` lines of the resident server's output. On
/// Linux+systemd, delegates to journalctl; everywhere else reads the log file
/// the registration pointed the process at.
fn service_logs(lines: usize) -> Result<()> {
    #[cfg(target_os = "linux")]
    if unix::systemd_service_enabled() {
        let status = std::process::Command::new("journalctl")
            .args([
                "--user",
                "-u",
                "pond-serve.service",
                "-n",
                &lines.to_string(),
                "--no-pager",
            ])
            .status()
            .context("failed to run journalctl")?;
        if !status.success() {
            anyhow::bail!("journalctl exited {status}");
        }
        return Ok(());
    }
    tail_log(&service_log_path(), lines)
}

pub(crate) fn status_snapshot() -> ScheduleSnapshot {
    match platform_probe() {
        Ok(state) => {
            let (active, backend, every, problem) = match &state {
                Active {
                    backend,
                    every,
                    problem,
                } => (true, Some(*backend), *every, problem.clone()),
                Inactive => (false, None, None, None),
            };
            ScheduleSnapshot {
                line: render_state(&state),
                active,
                backend,
                every,
                problem,
            }
        }
        Err(_) => ScheduleSnapshot {
            line: format!(
                "{}  unknown (scheduler probe failed)",
                paint("schedule", dim())
            ),
            active: false,
            backend: None,
            every: None,
            problem: None,
        },
    }
}

pub(crate) fn run(command: ScheduleCmd, config: Option<PathBuf>) -> Result<()> {
    match command {
        ScheduleCmd::Start { every } => platform_start(every, &config_file(config)),
        ScheduleCmd::Stop => platform_stop(),
        ScheduleCmd::Status => {
            let state = platform_probe()?;
            line(&render_state(&state))?;
            match &state {
                // A broken registration gets no logs pointer: its launcher
                // never runs, so the log it points at stays empty.
                Active { problem, .. } => {
                    if problem.is_none() {
                        line(&format!(
                            "{}      {}  (pond schedule logs)",
                            paint("logs", dim()),
                            crate::config::display(&crate::config::url_for_path(log_path())?),
                        ))?;
                    }
                    Ok(())
                }
                Inactive => std::process::exit(1),
            }
        }
        ScheduleCmd::Logs { lines } => logs(lines),
    }
}

/// Print the last `lines` lines of the sync log. On Linux+systemd, delegates
/// to journalctl; everywhere else reads the log file the wrapper writes.
pub(crate) fn logs(lines: usize) -> Result<()> {
    // Linux + systemd: the unit output goes to the journal, not a file.
    #[cfg(target_os = "linux")]
    if unix::systemd_timer_enabled() {
        let status = std::process::Command::new("journalctl")
            .args([
                "--user",
                "-u",
                "pond-sync.service",
                "-n",
                &lines.to_string(),
                "--no-pager",
            ])
            .status()
            .context("failed to run journalctl")?;
        if !status.success() {
            anyhow::bail!("journalctl exited {status}");
        }
        return Ok(());
    }

    tail_log(&log_path(), lines)
}

/// Print the last `lines` lines of a registration's log file, naming the file
/// first so an empty tail is not mistaken for a missing log.
fn tail_log(path: &Path, lines: usize) -> Result<()> {
    line_err(&paint(
        &format!(
            "log file: {}",
            crate::config::display(&crate::config::url_for_path(path)?)
        ),
        dim(),
    ))?;
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            line("(no log yet - the first scheduled run hasn't happened)")?;
            return Ok(());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    let all: Vec<&str> = text.lines().collect();
    let tail = all.len().saturating_sub(lines);
    for entry in &all[tail..] {
        line(entry)?;
    }
    Ok(())
}

/// The config file pinned into a registration. clap already resolved
/// `--config-file` and `POND_CONFIG_FILE` into `explicit` on every caller
/// path, so only the XDG default remains to apply. Absolutized against the
/// invoking shell's cwd: the pinned path is re-read from the scheduler's
/// working directory, which is not ours to assume - a relative pin would
/// silently miss there and the scheduled sync would run on built-in defaults.
pub(crate) fn config_file(explicit: Option<PathBuf>) -> PathBuf {
    let path = crate::config_path(explicit);
    std::path::absolute(&path).unwrap_or(path)
}

pub(crate) const CONFIG_FILE_SOURCES: &str =
    "--config-file or POND_CONFIG_FILE, falling back to $XDG_CONFIG_HOME/pond/config.toml";
pub(crate) const STATE_DIR_SOURCES: &str = "XDG_STATE_HOME, falling back to $HOME/.local/state";

/// Registration entry point for `pond init`, which calls it after the config
/// write with the config path it resolved (a `--config-file` passed to init
/// must pin into the unit, and clap's parsed value is invisible from here).
pub(crate) fn start(every: ScheduleEvery, explicit: PathBuf) -> Result<()> {
    platform_start(every, &config_file(Some(explicit)))
}

/// Paths are embedded verbatim in plist XML, a systemd quoted `Environment=`
/// value, a crontab line (where % means newline), and Task Scheduler XML
/// (where %VAR% runtime-expands with no escape syntax) - none of which the
/// templates escape. Reject the exotic characters up front instead of writing
/// a silently broken registration. `sources` names where the path resolved
/// from: the bad character may come from a fallback (`$HOME`), where "unset
/// the env var" would be a dead-end instruction. Also called by `pond init`
/// as soon as the schedule is chosen, so a doomed registration fails before
/// the config write and first sync, not after them.
pub(crate) fn reject_unembeddable(what: &str, path: &Path, sources: &str) -> Result<()> {
    let text = path.display().to_string();
    if text.contains(['<', '>', '&', '"', '%', '\n', '\r']) {
        anyhow::bail!(
            "{what} {text:?} contains a character (< > & \" % or a newline) that cannot be \
             embedded in a scheduler registration; it resolves from {sources} - use a \
             simpler absolute path and re-run `pond schedule start`"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Platform dispatchers
// ---------------------------------------------------------------------------

#[cfg(windows)]
fn platform_probe() -> Result<State> {
    windows::probe()
}
#[cfg(unix)]
fn platform_probe() -> Result<State> {
    unix::probe()
}
#[cfg(not(any(unix, windows)))]
fn platform_probe() -> Result<State> {
    Ok(Inactive)
}

// Task Scheduler's Exec action carries no environment block, so the Windows
// backend pins its paths as `pondw` arguments instead of env vars.
#[cfg(windows)]
fn platform_start(every: ScheduleEvery, config_file: &Path) -> Result<()> {
    windows::start(every, config_file)
}
#[cfg(unix)]
fn platform_start(every: ScheduleEvery, config_file: &Path) -> Result<()> {
    unix::start(every, config_file)
}
#[cfg(not(any(unix, windows)))]
fn platform_start(_every: ScheduleEvery, _config_file: &Path) -> Result<()> {
    anyhow::bail!("pond schedule is not supported on this platform yet")
}

#[cfg(windows)]
fn platform_stop() -> Result<()> {
    windows::stop()
}
#[cfg(unix)]
fn platform_stop() -> Result<()> {
    unix::stop()
}
#[cfg(not(any(unix, windows)))]
fn platform_stop() -> Result<()> {
    anyhow::bail!("pond schedule is not supported on this platform yet")
}

#[cfg(windows)]
fn service_platform_probe() -> Result<Option<ServiceState>> {
    windows::service_probe()
}
#[cfg(unix)]
fn service_platform_probe() -> Result<Option<ServiceState>> {
    unix::service_probe()
}
#[cfg(not(any(unix, windows)))]
fn service_platform_probe() -> Result<Option<ServiceState>> {
    Ok(None)
}

#[cfg(windows)]
fn service_platform_start(
    endpoint: &ServeEndpoint,
    sync_every: u64,
    config_file: &Path,
) -> Result<()> {
    windows::service_start(endpoint, sync_every, config_file)
}
#[cfg(unix)]
fn service_platform_start(
    endpoint: &ServeEndpoint,
    sync_every: u64,
    config_file: &Path,
) -> Result<()> {
    unix::service_start(endpoint, sync_every, config_file)
}
#[cfg(not(any(unix, windows)))]
fn service_platform_start(
    _endpoint: &ServeEndpoint,
    _sync_every: u64,
    _config_file: &Path,
) -> Result<()> {
    anyhow::bail!("pond service is not supported on this platform yet")
}

#[cfg(windows)]
fn service_platform_stop() -> Result<()> {
    windows::service_stop()
}
#[cfg(unix)]
fn service_platform_stop() -> Result<()> {
    unix::service_stop()
}
#[cfg(not(any(unix, windows)))]
fn service_platform_stop() -> Result<()> {
    anyhow::bail!("pond service is not supported on this platform yet")
}

/// Recover `--host`/`--port` from a registration body pond wrote, whatever
/// the surrounding syntax: a plist's `<string>` wrappers, a systemd
/// `ExecStart=` line, or a quoted Task Scheduler `<Arguments>` value all
/// reduce to the same token stream. String surgery, not three parsers: the
/// input is always pond's own template.
fn parse_endpoint(body: &str) -> Option<ServeEndpoint> {
    let tokens: Vec<&str> = body
        .split(|c: char| c.is_whitespace() || matches!(c, '<' | '>' | '"' | '='))
        // A plist wraps every argument in a `string` element, so the tag
        // names sit between the flag and its value.
        .filter(|token| !token.is_empty() && *token != "string" && *token != "/string")
        .collect();
    let after = |flag: &str| {
        tokens
            .iter()
            .position(|token| *token == flag)
            .and_then(|at| tokens.get(at + 1))
            .copied()
    };
    Some(ServeEndpoint {
        host: after("--host")?.to_owned(),
        port: after("--port")?.parse().ok()?,
    })
}

// ===========================================================================
// Platform: unix (launchd / systemd / cron)
// ===========================================================================

#[cfg(unix)]
mod unix {
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    use anyhow::{Context, Result, bail};

    use super::{ScheduleEvery, State};
    use State::Inactive;

    const LAUNCHD_LABEL: &str = "sh.pond.sync";
    const CRON_FENCE_BEGIN: &str = "# BEGIN POND SYNC (maintained by pond; do not edit)";
    const CRON_FENCE_END: &str = "# END POND SYNC";

    pub(super) fn probe() -> Result<State> {
        match std::env::consts::OS {
            "macos" => probe_launchd(),
            "linux" => {
                if systemd_timer_enabled() {
                    return Ok(State::active("systemd", read_systemd_interval()));
                }
                if let Some(entry) = read_cron_fence_entry()? {
                    return Ok(State::active("cron", cron_entry_interval(&entry)));
                }
                Ok(Inactive)
            }
            _ => Ok(Inactive),
        }
    }

    /// Register the schedule. Shared by `pond schedule start` and the
    /// `pond init` schedule section (which calls it after the config write).
    pub(super) fn start(every: ScheduleEvery, config_file: &Path) -> Result<()> {
        let bin = pond_bin();
        let log = super::log_path();
        if let Some(parent) = log.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        // The scheduler daemon never sources shell rc files, so a shell-only
        // XDG_STATE_HOME would put the scheduled sync's flock and last-sync
        // record in a different state dir than manual syncs - splitting the
        // single-flight lock. Pin the registration-time resolution into the
        // job's environment (same precedent as the baked-in log path). The
        // config file is pinned for the same reason: a scheduled sync that
        // read a different config would run with different adapters and a
        // different [embeddings].enabled than a manual one.
        let state = crate::syncstate::state_root();
        super::reject_unembeddable("state dir", &state, super::STATE_DIR_SOURCES)?;
        super::reject_unembeddable("config file", config_file, super::CONFIG_FILE_SOURCES)?;
        match std::env::consts::OS {
            "macos" => start_launchd(&bin, every, &log, &state, config_file),
            "linux" => {
                if systemd_user_available() {
                    // Switching schedulers must not leave the other one
                    // firing: a systemd start strips any cron fence.
                    remove_cron_fence()?;
                    start_systemd(&bin, every, &state, config_file)
                } else {
                    stop_systemd()?;
                    start_cron(&bin, every, &log, &state, config_file)
                }
            }
            other => bail!("pond schedule is not supported on {other} yet"),
        }
    }

    pub(super) fn stop() -> Result<()> {
        let removed = match std::env::consts::OS {
            "macos" => stop_launchd()?,
            "linux" => {
                let systemd = stop_systemd()?;
                let cron = remove_cron_fence()?;
                systemd || cron
            }
            other => bail!("pond schedule is not supported on {other} yet"),
        };
        if removed {
            pond::output::line("schedule removed")?;
        } else {
            pond::output::line("nothing was scheduled")?;
        }
        Ok(())
    }

    /// True when the systemd pond-sync.timer is enabled. Exposed `pub(super)`
    /// so the parent module's shared `logs()` can delegate to journalctl on
    /// Linux+systemd without duplicating the probe.
    pub(super) fn systemd_timer_enabled() -> bool {
        Command::new("systemctl")
            .args(["--user", "is-enabled", "pond-sync.timer"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    /// The binary path baked into the scheduler registration. Prefer the
    /// `pond` on PATH: that's a stable symlink that survives upgrades.
    /// `current_exe()` is the fallback - on Homebrew it resolves into a
    /// versioned Cellar path that the next upgrade deletes.
    fn pond_bin() -> PathBuf {
        crate::find_on_path("pond")
            .unwrap_or_else(|| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("pond")))
    }

    // ----- launchd (macOS) -------------------------------------------------

    fn plist_path(label: &str) -> Result<PathBuf> {
        let home = std::env::var_os("HOME").context("HOME is not set")?;
        Ok(PathBuf::from(home)
            .join("Library/LaunchAgents")
            .join(format!("{label}.plist")))
    }

    fn plist_body(
        bin: &Path,
        every: ScheduleEvery,
        log: &Path,
        state: &Path,
        config_file: &Path,
    ) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- created and maintained by pond; edits may be replaced -->
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{LAUNCHD_LABEL}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{bin}</string>
		<string>sync</string>
		<string>-q</string>
		<string>--no-wait</string>
	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>XDG_STATE_HOME</key>
		<string>{state}</string>
		<key>POND_CONFIG_FILE</key>
		<string>{config_file}</string>
	</dict>
	<key>StartInterval</key>
	<integer>{secs}</integer>
	<key>StandardOutPath</key>
	<string>{log}</string>
	<key>StandardErrorPath</key>
	<string>{log}</string>
	<key>ProcessType</key>
	<string>Background</string>
</dict>
</plist>
"#,
            bin = bin.display(),
            secs = every.secs(),
            log = log.display(),
            state = state.display(),
            config_file = config_file.display(),
        )
    }

    fn launchd_registered(uid: &str, label: &str) -> bool {
        Command::new("launchctl")
            .args(["print", &format!("gui/{uid}/{label}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn start_launchd(
        bin: &Path,
        every: ScheduleEvery,
        log: &Path,
        state: &Path,
        config_file: &Path,
    ) -> Result<()> {
        let plist = plist_path(LAUNCHD_LABEL)?;
        let body = plist_body(bin, every, log, state, config_file);
        let uid = current_uid()?;
        let unchanged = std::fs::read_to_string(&plist)
            .map(|existing| existing == body)
            .unwrap_or(false);
        if unchanged && launchd_registered(&uid, LAUNCHD_LABEL) {
            pond::output::line(&format!("already scheduled (every {})", every.label()))?;
            return Ok(());
        }
        if let Some(parent) = plist.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&plist, &body)
            .with_context(|| format!("failed to write {}", plist.display()))?;
        // bootout-then-bootstrap is the modern reload; bootout fails benignly
        // when nothing is registered yet, so its result is ignored.
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("gui/{uid}/{LAUNCHD_LABEL}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let output = Command::new("launchctl")
            .args(["bootstrap", &format!("gui/{uid}")])
            .arg(&plist)
            .output()
            .context("failed to run launchctl bootstrap")?;
        if !output.status.success() {
            bail!(
                "launchctl bootstrap exited {}: {} - remove {} and retry, or load it manually with `launchctl bootstrap gui/{uid} {}`",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim(),
                plist.display(),
                plist.display(),
            );
        }
        pond::output::line(&super::render_state(&super::State::active(
            "launchd",
            Some(every),
        )))?;
        pond::output::line(&format!(
            "{}      {}  (pond schedule logs)",
            pond::output::paint("logs", pond::output::dim()),
            crate::config::display(&crate::config::url_for_path(log)?),
        ))?;
        Ok(())
    }

    fn stop_launchd() -> Result<bool> {
        let plist = plist_path(LAUNCHD_LABEL)?;
        let uid = current_uid()?;
        let was_registered = launchd_registered(&uid, LAUNCHD_LABEL);
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("gui/{uid}/{LAUNCHD_LABEL}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        // Remove unconditionally: a missing plist means nothing to clean up,
        // not an error.
        let had_plist = match std::fs::remove_file(&plist) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", plist.display()));
            }
        };
        Ok(was_registered || had_plist)
    }

    fn probe_launchd() -> Result<State> {
        let uid = current_uid()?;
        if !launchd_registered(&uid, LAUNCHD_LABEL) {
            return Ok(Inactive);
        }
        let every = std::fs::read_to_string(plist_path(LAUNCHD_LABEL)?)
            .ok()
            .and_then(|body| plist_interval(&body));
        Ok(State::active("launchd", every))
    }

    /// Pull `<integer>N</integer>` following the StartInterval key out of a
    /// plist pond wrote. String surgery, not a plist parser: the input is
    /// pond's own template.
    fn plist_interval(body: &str) -> Option<ScheduleEvery> {
        let after = body.split("<key>StartInterval</key>").nth(1)?;
        let start = after.find("<integer>")? + "<integer>".len();
        let end = after.find("</integer>")?;
        let secs: u32 = after.get(start..end)?.trim().parse().ok()?;
        ScheduleEvery::from_secs(secs)
    }

    fn current_uid() -> Result<String> {
        let output = Command::new("id")
            .arg("-u")
            .output()
            .context("failed to run `id -u`")?;
        if !output.status.success() {
            bail!("`id -u` exited {}", output.status);
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    // ----- systemd user timers (Linux) -------------------------------------

    fn systemd_user_available() -> bool {
        Command::new("systemctl")
            .args(["--user", "list-timers"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn systemd_unit_dir() -> PathBuf {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
            .unwrap_or_else(|| PathBuf::from(".config"))
            .join("systemd/user")
    }

    fn systemd_service_body(bin: &Path, state: &Path, config_file: &Path) -> String {
        format!(
            "# created and maintained by pond; edits may be replaced\n\
             [Unit]\n\
             Description=pond sync\n\n\
             [Service]\n\
             Type=oneshot\n\
             Environment=\"XDG_STATE_HOME={}\"\n\
             Environment=\"POND_CONFIG_FILE={}\"\n\
             ExecStart={} sync -q --no-wait\n",
            state.display(),
            config_file.display(),
            bin.display(),
        )
    }

    fn systemd_timer_body(every: ScheduleEvery) -> String {
        format!(
            "# created and maintained by pond; edits may be replaced\n\
             [Unit]\n\
             Description=pond sync every {}\n\n\
             [Timer]\n\
             OnBootSec=2m\n\
             OnUnitActiveSec={}s\n\
             Persistent=true\n\n\
             [Install]\n\
             WantedBy=timers.target\n",
            every.label(),
            every.secs(),
        )
    }

    fn start_systemd(
        bin: &Path,
        every: ScheduleEvery,
        state: &Path,
        config_file: &Path,
    ) -> Result<()> {
        let dir = systemd_unit_dir();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let service_path = dir.join("pond-sync.service");
        let timer_path = dir.join("pond-sync.timer");
        let service = systemd_service_body(bin, state, config_file);
        let timer = systemd_timer_body(every);
        let unchanged = std::fs::read_to_string(&service_path)
            .map(|existing| existing == service)
            .unwrap_or(false)
            && std::fs::read_to_string(&timer_path)
                .map(|existing| existing == timer)
                .unwrap_or(false);
        if unchanged && systemd_timer_enabled() {
            pond::output::line(&format!("already scheduled (every {})", every.label()))?;
            return Ok(());
        }
        std::fs::write(&service_path, service)
            .with_context(|| format!("failed to write {}", service_path.display()))?;
        std::fs::write(&timer_path, timer)
            .with_context(|| format!("failed to write {}", timer_path.display()))?;
        for args in [
            vec!["--user", "daemon-reload"],
            vec!["--user", "enable", "--now", "pond-sync.timer"],
        ] {
            let output = Command::new("systemctl")
                .args(&args)
                .output()
                .context("failed to run systemctl")?;
            if !output.status.success() {
                bail!(
                    "systemctl {} exited {}: {}",
                    args.join(" "),
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim(),
                );
            }
        }
        pond::output::line(&super::render_state(&super::State::active(
            "systemd",
            Some(every),
        )))?;
        pond::output::line(&format!(
            "{}      journalctl --user -u pond-sync.service  (pond schedule logs)",
            pond::output::paint("logs", pond::output::dim()),
        ))?;
        Ok(())
    }

    fn stop_systemd() -> Result<bool> {
        let dir = systemd_unit_dir();
        let service_path = dir.join("pond-sync.service");
        let timer_path = dir.join("pond-sync.timer");
        let was_enabled = systemd_timer_enabled();
        if was_enabled {
            let _ = Command::new("systemctl")
                .args(["--user", "disable", "--now", "pond-sync.timer"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        // Remove unconditionally; a missing unit is not an error.
        let mut removed_units = false;
        for path in [&service_path, &timer_path] {
            match std::fs::remove_file(path) {
                Ok(()) => removed_units = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to remove {}", path.display()));
                }
            }
        }
        if removed_units {
            let _ = Command::new("systemctl")
                .args(["--user", "daemon-reload"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        Ok(was_enabled || removed_units)
    }

    fn read_systemd_interval() -> Option<ScheduleEvery> {
        let body = std::fs::read_to_string(systemd_unit_dir().join("pond-sync.timer")).ok()?;
        let line = body
            .lines()
            .find_map(|line| line.trim().strip_prefix("OnUnitActiveSec="))?;
        let secs: u32 = line.trim().trim_end_matches('s').parse().ok()?;
        ScheduleEvery::from_secs(secs)
    }

    // ----- crontab fence (Linux without systemd) ---------------------------

    /// The cron line for one cadence. The minute is randomized once at
    /// registration so a fleet of pond installs doesn't synchronize load on
    /// a shared object store at :00.
    fn cron_entry(
        bin: &Path,
        every: ScheduleEvery,
        log: &Path,
        minute: u32,
        state: &Path,
        config_file: &Path,
    ) -> String {
        let command = format!(
            "XDG_STATE_HOME=\"{}\" POND_CONFIG_FILE=\"{}\" {} sync -q --no-wait >> {} 2>&1",
            state.display(),
            config_file.display(),
            bin.display(),
            log.display()
        );
        let schedule = match every {
            ScheduleEvery::M5 => format!("{}-59/5 * * * *", minute % 5),
            ScheduleEvery::M15 => {
                let m = minute % 15;
                format!("{m},{},{},{} * * * *", m + 15, m + 30, m + 45)
            }
            ScheduleEvery::H1 => format!("{} * * * *", minute % 60),
            ScheduleEvery::H6 => format!("{} */6 * * *", minute % 60),
            ScheduleEvery::D1 => format!("{} 3 * * *", minute % 60),
        };
        format!("{schedule} {command}")
    }

    /// Reverse-map a fence entry's schedule fields back onto a cadence for
    /// `status`. `None` for a hand-edited entry pond doesn't recognize.
    fn cron_entry_interval(entry: &str) -> Option<ScheduleEvery> {
        let fields: Vec<&str> = entry.split_whitespace().take(5).collect();
        if fields.len() < 5 {
            return None;
        }
        match (fields[0], fields[1]) {
            (minute, "*") if minute.contains('/') => Some(ScheduleEvery::M5),
            (minute, "*") if minute.contains(',') => Some(ScheduleEvery::M15),
            (_, "*") => Some(ScheduleEvery::H1),
            (minute, "*/6") if !minute.contains(',') && !minute.contains('/') => {
                Some(ScheduleEvery::H6)
            }
            (minute, _) if !minute.contains(',') && !minute.contains('/') => {
                Some(ScheduleEvery::D1)
            }
            _ => None,
        }
    }

    fn read_crontab() -> Result<String> {
        let output = Command::new("crontab")
            .arg("-l")
            .output()
            .context("failed to run `crontab -l`")?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            // `crontab -l` exits nonzero when the user has no crontab yet.
            Ok(String::new())
        }
    }

    fn write_crontab(body: &str) -> Result<()> {
        use std::io::Write;
        let mut child = Command::new("crontab")
            .arg("-")
            .stdin(Stdio::piped())
            .spawn()
            .context("failed to run `crontab -`")?;
        child
            .stdin
            .take()
            .context("crontab stdin unavailable")?
            .write_all(body.as_bytes())
            .context("failed to write crontab")?;
        let status = child.wait().context("crontab did not exit")?;
        if !status.success() {
            bail!("`crontab -` exited {status}");
        }
        Ok(())
    }

    /// Drop the fenced pond block (and the fence markers) from a crontab.
    fn strip_cron_fence(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut inside = false;
        for line in text.lines() {
            if line.trim() == CRON_FENCE_BEGIN {
                inside = true;
                continue;
            }
            if line.trim() == CRON_FENCE_END {
                inside = false;
                continue;
            }
            if !inside {
                out.push_str(line);
                out.push('\n');
            }
        }
        out
    }

    fn fence_block(entry: &str) -> String {
        format!("{CRON_FENCE_BEGIN}\n{entry}\n{CRON_FENCE_END}\n")
    }

    /// Pull pond's fenced cron entry out of a crontab body.
    fn fence_entry_in(text: &str) -> Option<String> {
        let after = text.split(CRON_FENCE_BEGIN).nth(1)?;
        let block = after.split(CRON_FENCE_END).next().unwrap_or_default();
        block
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_owned)
    }

    fn read_cron_fence_entry() -> Result<Option<String>> {
        Ok(fence_entry_in(&read_crontab()?))
    }

    fn start_cron(
        bin: &Path,
        every: ScheduleEvery,
        log: &Path,
        state: &Path,
        config_file: &Path,
    ) -> Result<()> {
        let existing = read_crontab()?;
        // The command-shape check keeps this a real idempotence test: a fence
        // entry written by an older pond (`sync -q` without `--no-wait`, or
        // without the pinned state dir or config file) must re-register, not be
        // kept as "already scheduled".
        if let Some(entry) = fence_entry_in(&existing)
            && cron_entry_interval(&entry) == Some(every)
            && entry.contains(&bin.display().to_string())
            && entry.contains("--no-wait")
            && entry.contains("XDG_STATE_HOME=")
            && entry.contains("POND_CONFIG_FILE=")
        {
            pond::output::line(&format!("already scheduled (every {})", every.label()))?;
            return Ok(());
        }
        let entry = cron_entry(bin, every, log, fastrand::u32(0..60), state, config_file);
        let mut body = strip_cron_fence(&existing);
        if !body.is_empty() && !body.ends_with('\n') {
            body.push('\n');
        }
        body.push_str(&fence_block(&entry));
        write_crontab(&body)?;
        pond::output::line(&super::render_state(&super::State::active(
            "cron",
            Some(every),
        )))?;
        pond::output::line(&format!(
            "{}      {}  (pond schedule logs)",
            pond::output::paint("logs", pond::output::dim()),
            crate::config::display(&crate::config::url_for_path(log)?),
        ))?;
        Ok(())
    }

    fn remove_cron_fence() -> Result<bool> {
        let existing = read_crontab()?;
        if !existing.contains(CRON_FENCE_BEGIN) {
            return Ok(false);
        }
        write_crontab(&strip_cron_fence(&existing))?;
        Ok(true)
    }

    // ----- the resident `pond serve` registration --------------------------

    const LAUNCHD_SERVE_LABEL: &str = "sh.pond.serve";
    const SYSTEMD_SERVE_UNIT: &str = "pond-serve.service";

    /// True when the systemd pond-serve.service is enabled. `pub(super)` so
    /// the parent module's `service_logs` can delegate to journalctl.
    pub(super) fn systemd_service_enabled() -> bool {
        Command::new("systemctl")
            .args(["--user", "is-enabled", SYSTEMD_SERVE_UNIT])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    pub(super) fn service_probe() -> Result<Option<super::ServiceState>> {
        match std::env::consts::OS {
            "macos" => {
                let uid = current_uid()?;
                if !launchd_registered(&uid, LAUNCHD_SERVE_LABEL) {
                    return Ok(None);
                }
                Ok(Some(super::ServiceState {
                    backend: "launchd",
                    endpoint: std::fs::read_to_string(plist_path(LAUNCHD_SERVE_LABEL)?)
                        .ok()
                        .as_deref()
                        .and_then(super::parse_endpoint),
                }))
            }
            "linux" => {
                if !systemd_service_enabled() {
                    return Ok(None);
                }
                Ok(Some(super::ServiceState {
                    backend: "systemd",
                    endpoint: std::fs::read_to_string(systemd_unit_dir().join(SYSTEMD_SERVE_UNIT))
                        .ok()
                        .as_deref()
                        .and_then(super::parse_endpoint),
                }))
            }
            _ => Ok(None),
        }
    }

    /// The command line every backend registers. `--with-sync` is the point of
    /// the resident process: one warm store, one embedder, one S3 client
    /// shared by the read tools and the periodic sync, instead of a sync child
    /// cold-loading its own.
    fn serve_args(endpoint: &super::ServeEndpoint, sync_every: u64) -> Vec<String> {
        let port = endpoint.port.to_string();
        // A zero interval would busy-sync the store.
        let every = sync_every.max(1).to_string();
        [
            "serve",
            "--transport",
            "http",
            "--with-sync",
            "--host",
            endpoint.host.as_str(),
            "--port",
            port.as_str(),
            "--sync-every",
            every.as_str(),
        ]
        .map(str::to_owned)
        .to_vec()
    }

    pub(super) fn service_start(
        endpoint: &super::ServeEndpoint,
        sync_every: u64,
        config_file: &Path,
    ) -> Result<()> {
        let bin = pond_bin();
        let log = super::service_log_path();
        if let Some(parent) = log.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        // Same pins as the sync registration, for the same reason: the service
        // manager sources no shell rc files, and the resident sync must take
        // the same per-host lock and read the same config as a manual one.
        let state = crate::syncstate::state_root();
        super::reject_unembeddable("state dir", &state, super::STATE_DIR_SOURCES)?;
        super::reject_unembeddable("config file", config_file, super::CONFIG_FILE_SOURCES)?;
        match std::env::consts::OS {
            "macos" => start_launchd_serve(&bin, endpoint, sync_every, &log, &state, config_file),
            "linux" => {
                if !systemd_user_available() {
                    bail!(
                        "no systemd user instance on this host, so there is nothing to keep \
                         `pond serve` alive (a crontab entry cannot supervise a resident \
                         process); register MCP over stdio instead \
                         (`pond init --mcp-transport stdio`), or run \
                         `pond serve --transport http --with-sync` under your own supervisor"
                    );
                }
                start_systemd_serve(&bin, endpoint, sync_every, &state, config_file)
            }
            other => bail!("pond service is not supported on {other} yet"),
        }
    }

    pub(super) fn service_stop() -> Result<()> {
        let removed = match std::env::consts::OS {
            "macos" => stop_launchd_serve()?,
            "linux" => stop_systemd_serve()?,
            other => bail!("pond service is not supported on {other} yet"),
        };
        if removed {
            pond::output::line("service removed")?;
        } else {
            pond::output::line("no resident pond serve was registered")?;
        }
        Ok(())
    }

    /// `KeepAlive` is what makes this a supervised process rather than a
    /// one-shot: launchd restarts the server if it exits, and `RunAtLoad`
    /// starts it at login.
    fn serve_plist_body(
        bin: &Path,
        endpoint: &super::ServeEndpoint,
        sync_every: u64,
        log: &Path,
        state: &Path,
        config_file: &Path,
    ) -> String {
        let args = serve_args(endpoint, sync_every)
            .iter()
            .map(|arg| format!("\t\t<string>{arg}</string>\n"))
            .collect::<String>();
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- created and maintained by pond; edits may be replaced -->
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{LAUNCHD_SERVE_LABEL}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{bin}</string>
{args}	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>XDG_STATE_HOME</key>
		<string>{state}</string>
		<key>POND_CONFIG_FILE</key>
		<string>{config_file}</string>
	</dict>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>StandardOutPath</key>
	<string>{log}</string>
	<key>StandardErrorPath</key>
	<string>{log}</string>
	<key>ProcessType</key>
	<string>Background</string>
</dict>
</plist>
"#,
            bin = bin.display(),
            log = log.display(),
            state = state.display(),
            config_file = config_file.display(),
        )
    }

    fn start_launchd_serve(
        bin: &Path,
        endpoint: &super::ServeEndpoint,
        sync_every: u64,
        log: &Path,
        state: &Path,
        config_file: &Path,
    ) -> Result<()> {
        let plist = plist_path(LAUNCHD_SERVE_LABEL)?;
        let body = serve_plist_body(bin, endpoint, sync_every, log, state, config_file);
        let uid = current_uid()?;
        let unchanged = std::fs::read_to_string(&plist)
            .map(|existing| existing == body)
            .unwrap_or(false);
        if unchanged && launchd_registered(&uid, LAUNCHD_SERVE_LABEL) {
            pond::output::line(&format!("already running ({})", endpoint.mcp_url()))?;
            return Ok(());
        }
        if let Some(parent) = plist.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&plist, &body)
            .with_context(|| format!("failed to write {}", plist.display()))?;
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("gui/{uid}/{LAUNCHD_SERVE_LABEL}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let output = Command::new("launchctl")
            .args(["bootstrap", &format!("gui/{uid}")])
            .arg(&plist)
            .output()
            .context("failed to run launchctl bootstrap")?;
        if !output.status.success() {
            bail!(
                "launchctl bootstrap exited {}: {} - remove {} and retry",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim(),
                plist.display(),
            );
        }
        super::report_service_started("launchd", endpoint)
    }

    fn stop_launchd_serve() -> Result<bool> {
        let plist = plist_path(LAUNCHD_SERVE_LABEL)?;
        let uid = current_uid()?;
        let was_registered = launchd_registered(&uid, LAUNCHD_SERVE_LABEL);
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("gui/{uid}/{LAUNCHD_SERVE_LABEL}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let had_plist = match std::fs::remove_file(&plist) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", plist.display()));
            }
        };
        Ok(was_registered || had_plist)
    }

    /// `Restart=always` is the supervision; `WantedBy=default.target` starts it
    /// at login. A user unit only survives logout on a host with lingering
    /// enabled (`loginctl enable-linger`), which is the operator's call, not
    /// a registration's.
    fn systemd_serve_body(
        bin: &Path,
        endpoint: &super::ServeEndpoint,
        sync_every: u64,
        state: &Path,
        config_file: &Path,
    ) -> String {
        format!(
            "# created and maintained by pond; edits may be replaced\n\
             [Unit]\n\
             Description=pond serve (resident HTTP + MCP endpoint)\n\n\
             [Service]\n\
             Type=simple\n\
             Environment=\"XDG_STATE_HOME={state}\"\n\
             Environment=\"POND_CONFIG_FILE={config}\"\n\
             ExecStart={bin} {args}\n\
             Restart=always\n\
             RestartSec=5\n\n\
             [Install]\n\
             WantedBy=default.target\n",
            state = state.display(),
            config = config_file.display(),
            bin = bin.display(),
            args = serve_args(endpoint, sync_every).join(" "),
        )
    }

    fn start_systemd_serve(
        bin: &Path,
        endpoint: &super::ServeEndpoint,
        sync_every: u64,
        state: &Path,
        config_file: &Path,
    ) -> Result<()> {
        let dir = systemd_unit_dir();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let unit_path = dir.join(SYSTEMD_SERVE_UNIT);
        let unit = systemd_serve_body(bin, endpoint, sync_every, state, config_file);
        let unchanged = std::fs::read_to_string(&unit_path)
            .map(|existing| existing == unit)
            .unwrap_or(false);
        if unchanged && systemd_service_enabled() {
            pond::output::line(&format!("already running ({})", endpoint.mcp_url()))?;
            return Ok(());
        }
        std::fs::write(&unit_path, unit)
            .with_context(|| format!("failed to write {}", unit_path.display()))?;
        for args in [
            vec!["--user", "daemon-reload"],
            // `restart` rather than `start`: a unit whose ExecStart just
            // changed must pick the new command line up, and the enable's
            // --now would leave an already-running old process in place.
            vec!["--user", "enable", SYSTEMD_SERVE_UNIT],
            vec!["--user", "restart", SYSTEMD_SERVE_UNIT],
        ] {
            let output = Command::new("systemctl")
                .args(&args)
                .output()
                .context("failed to run systemctl")?;
            if !output.status.success() {
                bail!(
                    "systemctl {} exited {}: {}",
                    args.join(" "),
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim(),
                );
            }
        }
        super::report_service_started("systemd", endpoint)?;
        pond::output::line(&format!(
            "{}      journalctl --user -u {SYSTEMD_SERVE_UNIT}  (pond service logs)",
            pond::output::paint("logs", pond::output::dim()),
        ))?;
        Ok(())
    }

    fn stop_systemd_serve() -> Result<bool> {
        let was_enabled = systemd_service_enabled();
        if was_enabled {
            let _ = Command::new("systemctl")
                .args(["--user", "disable", "--now", SYSTEMD_SERVE_UNIT])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let unit_path = systemd_unit_dir().join(SYSTEMD_SERVE_UNIT);
        let removed = match std::fs::remove_file(&unit_path) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to remove {}", unit_path.display()));
            }
        };
        if removed {
            let _ = Command::new("systemctl")
                .args(["--user", "daemon-reload"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        Ok(was_enabled || removed)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        const BIN: &str = "/usr/local/bin/pond";
        const LOG: &str = "/tmp/sync.log";
        const STATE: &str = "/home/user/.local/state";
        const CONFIG: &str = "/home/user/.config/pond/config.toml";

        #[test]
        fn cron_entries_reverse_map_to_their_cadence() {
            let bin = Path::new(BIN);
            let log = Path::new(LOG);
            let state = Path::new(STATE);
            let config_file = Path::new(CONFIG);
            for every in [
                ScheduleEvery::M5,
                ScheduleEvery::M15,
                ScheduleEvery::H1,
                ScheduleEvery::H6,
                ScheduleEvery::D1,
            ] {
                for minute in [0, 7, 59] {
                    let entry = cron_entry(bin, every, log, minute, state, config_file);
                    assert_eq!(cron_entry_interval(&entry), Some(every), "entry: {entry}");
                }
            }
        }

        /// A scheduled sync must read the config a manual one reads: without
        /// the pin, a shell-set POND_CONFIG_FILE (or XDG_CONFIG_HOME) makes the
        /// two diverge - different adapters, different [embeddings].enabled.
        #[test]
        fn every_template_pins_the_config_file() {
            let (bin, log, state, config_file) = (
                Path::new(BIN),
                Path::new(LOG),
                Path::new(STATE),
                Path::new(CONFIG),
            );
            let plist = plist_body(bin, ScheduleEvery::M5, log, state, config_file);
            assert!(
                plist.contains(&format!(
                    "<key>POND_CONFIG_FILE</key>\n\t\t<string>{CONFIG}</string>"
                )),
                "{plist}"
            );
            let service = systemd_service_body(bin, state, config_file);
            assert!(
                service.contains(&format!("Environment=\"POND_CONFIG_FILE={CONFIG}\"\n")),
                "{service}"
            );
            let entry = cron_entry(bin, ScheduleEvery::M5, log, 7, state, config_file);
            assert!(
                entry.contains(&format!("POND_CONFIG_FILE=\"{CONFIG}\"")),
                "{entry}"
            );
            // The pinned env has to precede the binary, or cron runs the sync
            // without it.
            assert!(entry.find("POND_CONFIG_FILE=") < entry.find(BIN), "{entry}");
        }

        /// The resident registration's whole value is one warm process that
        /// also syncs, reading the same config and state dir as a manual run.
        #[test]
        fn the_resident_templates_supervise_a_with_sync_serve() {
            let (bin, state, config_file) = (Path::new(BIN), Path::new(STATE), Path::new(CONFIG));
            let endpoint = super::super::ServeEndpoint::default();
            let unit = systemd_serve_body(bin, &endpoint, 15, state, config_file);
            assert!(
                unit.contains(&format!(
                    "ExecStart={BIN} serve --transport http --with-sync \
                     --host 127.0.0.1 --port 9797 --sync-every 15\n"
                )),
                "{unit}"
            );
            assert!(unit.contains("Restart=always\n"), "{unit}");
            assert!(
                unit.contains(&format!("POND_CONFIG_FILE={CONFIG}")),
                "{unit}"
            );
            assert!(unit.contains(&format!("XDG_STATE_HOME={STATE}")), "{unit}");

            let plist = serve_plist_body(
                bin,
                &endpoint,
                15,
                Path::new("/tmp/serve.log"),
                state,
                config_file,
            );
            assert!(plist.contains("<key>KeepAlive</key>\n\t<true/>"), "{plist}");
            assert!(plist.contains("<string>--with-sync</string>"), "{plist}");
            assert!(
                plist.contains(&format!(
                    "<key>POND_CONFIG_FILE</key>\n\t\t<string>{CONFIG}</string>"
                )),
                "{plist}"
            );
            // Both round-trip through the probe that `pond service status`
            // and init's repair pass read the live endpoint with.
            for body in [&unit, &plist] {
                assert_eq!(
                    super::super::parse_endpoint(body).as_ref(),
                    Some(&endpoint),
                    "{body}"
                );
            }

            // A zero interval would busy-sync; the template floors it at one.
            let floored = systemd_serve_body(bin, &endpoint, 0, state, config_file);
            assert!(floored.contains("--sync-every 1\n"), "{floored}");
        }
    }
}

// ===========================================================================
// Platform: windows (Task Scheduler)
// ===========================================================================

#[cfg(windows)]
mod windows {
    //! Windows Task Scheduler backend. The action is `pondw.exe` (see its own
    //! module doc), carrying the log path and the pinned state dir as arguments
    //! because an `Exec` action has neither an environment block nor a
    //! `StandardOutPath`. The task XML supplies the launchd/systemd-equivalent
    //! posture: battery-friendly, `StartWhenAvailable` for catch-up after
    //! downtime.

    use std::path::PathBuf;
    use std::process::Command;

    use anyhow::{Context, Result, bail};

    use super::{ScheduleEvery, State};
    use State::{Active, Inactive};

    const TASK_NAME: &str = "pond-sync";
    /// Shared recovery hint for the admin-owned-task trap; the literal task
    /// name rides along because a const cannot interpolate `TASK_NAME`.
    const ELEVATED_OWNERSHIP_HINT: &str = "likely registered from an elevated shell and owned by Administrators; remove it \
         by running `pond schedule stop` from an elevated shell (or \
         `schtasks /Delete /TN pond-sync /F`)";

    /// The registered task's XML, or `None` when no such task exists.
    fn query_xml() -> Result<Option<String>> {
        let output = Command::new("schtasks")
            .args(["/Query", "/TN", TASK_NAME, "/XML", "ONE"])
            .output()
            .context("failed to run schtasks /Query")?;
        Ok(output
            .status
            .success()
            .then(|| decode_console(&output.stdout)))
    }

    /// Probe existence + cadence in a single `schtasks /Query /XML ONE` call.
    pub(super) fn probe() -> Result<State> {
        let Some(xml) = query_xml()? else {
            return Ok(Inactive);
        };
        Ok(Active {
            backend: "task-scheduler",
            every: parse_interval_from_xml(&xml),
            problem: launcher_problem(&xml),
        })
    }

    /// The registered action outlives the install that wrote it (an uninstall
    /// or package-manager switch removes both exes), and Task Scheduler keeps
    /// reporting the task while every tick dies with FILE_NOT_FOUND before
    /// pondw can run or log anything. A missing `<Command>` is that whole
    /// failure class; anything past launch reaches sync.log through pondw's
    /// exit-code propagation instead.
    fn launcher_problem(xml: &str) -> Option<String> {
        between(xml, "<Command>", "</Command>")
            .map(xml_unescape)
            .filter(|launcher| !std::path::Path::new(launcher).is_file())
            .map(|launcher| {
                format!(
                    "the registered launcher no longer exists ({launcher}); \
                     run `pond schedule start` to re-register from the current install"
                )
            })
    }

    /// Register (or replace, via `/F`) the Task Scheduler job. Shared by
    /// `pond schedule start` and the `pond init` schedule section.
    pub(super) fn start(every: ScheduleEvery, config_file: &std::path::Path) -> Result<()> {
        let bin = pond_bin();
        if !bin.is_file() {
            bail!(
                "could not resolve the pond binary to register ({}); run \
                 `pond schedule start` from an installed pond",
                bin.display()
            );
        }
        let launcher = pondw_bin(&bin)?;
        let log = super::log_path();
        // state_root is what we pin into --state-dir; pond_state_dir is the
        // directory where the log, lock, and last-sync record live.
        let state_root = crate::syncstate::state_root();
        let pond_state = crate::syncstate::pond_state_dir();
        std::fs::create_dir_all(&pond_state)
            .with_context(|| format!("failed to create {}", pond_state.display()))?;

        // Gate every path that lands in the XML: Task Scheduler expands %VAR%
        // inside <Command> and <Arguments> at runtime with NO escape syntax.
        // The shared gate's wider character set costs nothing here, and each
        // entry names its own source so the error points at the actual knob.
        for (what, path, sources) in [
            (
                "launcher",
                launcher.as_path(),
                "the installed pond binary's directory",
            ),
            ("pond binary", bin.as_path(), "the installed pond location"),
            (
                "sync log",
                log.as_path(),
                "the state dir (--state-dir or XDG_STATE_HOME)",
            ),
            (
                "state dir",
                state_root.as_path(),
                "--state-dir or XDG_STATE_HOME",
            ),
            ("config file", config_file, super::CONFIG_FILE_SOURCES),
        ] {
            super::reject_unembeddable(what, path, sources)?;
        }

        let arguments = task_arguments(&log, &bin, &state_root, config_file);

        // The pin is registration-time: a later shell-only override splits the
        // lock and last-sync record between the scheduled and manual syncs, and
        // the task will not follow it.
        if std::env::var_os("XDG_STATE_HOME").is_some() {
            pond::output::line(&format!(
                "note: XDG_STATE_HOME is set; the task is pinned to {} and will not follow later changes",
                state_root.display()
            ))?;
        }

        // Already-scheduled no-op: same action, same cadence. Compared on the
        // decoded element text, not the escaped form we wrote, because Task
        // Scheduler re-serializes the XML it stores and need not escape a quote
        // in element content. A mismatch only costs an idempotent re-register.
        let launcher_str = launcher.display().to_string();
        if let Some(xml) = query_xml()?
            && between(&xml, "<Command>", "</Command>")
                .map(xml_unescape)
                .as_deref()
                == Some(launcher_str.as_str())
            && between(&xml, "<Arguments>", "</Arguments>")
                .map(xml_unescape)
                .as_deref()
                == Some(arguments.as_str())
            && parse_interval_from_xml(&xml) == Some(every)
        {
            pond::output::line(&format!("already scheduled (every {})", every.label()))?;
            return Ok(());
        }

        // A task registered elevated is owned by Administrators, which makes
        // every later `pond schedule start`/`stop` from a normal shell fail
        // with Access denied. Warn at creation - after the no-op return above,
        // so only a run that actually registers sets off the note (see the
        // /Create failure path below for the caught-in-the-trap version).
        if shell_is_elevated() {
            pond::output::line(
                "note: this shell is elevated - the task will be owned by Administrators, \
                 and `pond schedule start`/`stop` from a normal shell will fail with \
                 Access denied; re-run from a non-elevated shell unless that is intended",
            )?;
        }

        // Write the XML task definition to a temp file; schtasks /Create /XML
        // requires a file path. Pass the PathBuf directly to avoid to_str()
        // panics on non-UTF-8 paths. The bytes MUST be UTF-16LE with a BOM:
        // schtasks reads a BOM-less file through the ANSI code page, so a
        // UTF-8 write would mojibake any non-ASCII state path (e.g. a
        // non-ASCII Windows username) into a task action pointing at a
        // nonexistent launcher - registration "succeeds" and every tick
        // silently does nothing. UTF-16LE+BOM matches the declaration in
        // `task_xml` and the encoding Task Scheduler's own XML export produces.
        let xml = task_xml(&launcher, &arguments, every);
        let tmp_xml = pond_state.join("pond-sync-task.xml.tmp");
        std::fs::write(&tmp_xml, utf16le_bom(&xml))
            .with_context(|| format!("failed to write {}", tmp_xml.display()))?;
        let create_result = Command::new("schtasks")
            .args(["/Create", "/TN", TASK_NAME, "/XML"])
            .arg(&tmp_xml)
            .arg("/F")
            .output()
            .context("failed to run schtasks /Create");
        let _ = std::fs::remove_file(&tmp_xml); // best-effort temp cleanup

        let output = create_result?;
        if !output.status.success() {
            let stderr = decode_console(&output.stderr);
            let stderr = stderr.trim();
            // /F replaces an existing task, so a create that fails while the
            // task still exists is almost always an ownership problem: a task
            // registered from an elevated shell is admin-owned and a normal
            // shell can neither replace nor delete it. Checked on existence,
            // not stderr text - schtasks messages are localized. The probe is
            // advisory only, so its own failure must not mask the /Create
            // stderr - hence no `?`.
            if matches!(query_xml(), Ok(Some(_))) {
                bail!(
                    "schtasks /Create failed: {stderr}\n\
                     the existing '{TASK_NAME}' task could not be replaced - it was \
                     {ELEVATED_OWNERSHIP_HINT}, then re-run `pond schedule start` unelevated"
                );
            }
            bail!("schtasks /Create failed: {stderr}");
        }

        // The pre-pondw action chain, when this is an upgrade. Removed only
        // once the new task exists: a failed /Create must leave the old one
        // working.
        for stale in ["pond-sync.cmd", "pond-sync.vbs"] {
            let _ = std::fs::remove_file(pond_state.join(stale));
        }

        pond::output::line(&super::render_state(&super::State::active(
            "task-scheduler",
            Some(every),
        )))?;
        pond::output::line(&format!(
            "{}      {}  (pond schedule logs)",
            pond::output::paint("logs", pond::output::dim()),
            crate::config::display(&crate::config::url_for_path(log)?),
        ))?;
        Ok(())
    }

    pub(super) fn stop() -> Result<()> {
        let output = Command::new("schtasks")
            .args(["/Delete", "/TN", TASK_NAME, "/F"])
            .output()
            .context("failed to run schtasks /Delete")?;
        if output.status.success() {
            // Leave the log in place; it stays readable via
            // `pond schedule logs`.
            pond::output::line("schedule removed")?;
            return Ok(());
        }
        // Delete failed. Disambiguate TOCTOU: if the task is now gone (it
        // wasn't there, or was removed concurrently) report nothing-was-scheduled;
        // if it still exists the failure is genuine.
        match probe()? {
            Inactive => pond::output::line("nothing was scheduled")?,
            Active { .. } => {
                bail!(
                    "schtasks /Delete failed: {}\n\
                     the task is {ELEVATED_OWNERSHIP_HINT}",
                    decode_console(&output.stderr).trim()
                );
            }
        }
        Ok(())
    }

    // ----- the resident `pond serve` registration --------------------------

    const SERVE_TASK_NAME: &str = "pond-serve";

    fn serve_query_xml() -> Result<Option<String>> {
        let output = Command::new("schtasks")
            .args(["/Query", "/TN", SERVE_TASK_NAME, "/XML", "ONE"])
            .output()
            .context("failed to run schtasks /Query")?;
        Ok(output
            .status
            .success()
            .then(|| decode_console(&output.stdout)))
    }

    pub(super) fn service_probe() -> Result<Option<super::ServiceState>> {
        let Some(xml) = serve_query_xml()? else {
            return Ok(None);
        };
        Ok(Some(super::ServiceState {
            backend: "task-scheduler",
            endpoint: super::parse_endpoint(&xml),
        }))
    }

    /// The `<Arguments>` line for the resident server: pondw's own `--log`,
    /// then the pond command line it supervises.
    fn serve_task_arguments(
        log: &std::path::Path,
        bin: &std::path::Path,
        endpoint: &super::ServeEndpoint,
        sync_every: u64,
        state_root: &std::path::Path,
        config_file: &std::path::Path,
    ) -> String {
        format!(
            "--log {log} -- {bin} serve --transport http --with-sync \
             --host {host} --port {port} --sync-every {sync_every} \
             --state-dir {state} --config-file {config}",
            log = quote_arg(log),
            bin = quote_arg(bin),
            host = endpoint.host,
            port = endpoint.port,
            sync_every = sync_every.max(1),
            state = quote_arg(state_root),
            config = quote_arg(config_file),
        )
    }

    /// Task XML for the resident server. Windows has no user-scoped service
    /// manager a non-elevated install can write to, so a logon-triggered task
    /// with `RestartOnFailure` and no execution time limit is the launchd
    /// `KeepAlive` / systemd `Restart=always` equivalent.
    fn serve_task_xml(launcher: &std::path::Path, arguments: &str) -> String {
        let launcher_str = xml_escape(&launcher.display().to_string());
        let arguments = xml_escape(arguments);
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n\
             <Task version=\"1.2\" \
             xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">\n\
             \x20\x20<RegistrationInfo>\n\
             \x20\x20  <Description>pond serve (managed by pond; do not edit)</Description>\n\
             \x20\x20</RegistrationInfo>\n\
             \x20\x20<Triggers>\n\
             \x20\x20  <LogonTrigger>\n\
             \x20\x20    <Enabled>true</Enabled>\n\
             \x20\x20  </LogonTrigger>\n\
             \x20\x20</Triggers>\n\
             \x20\x20<Settings>\n\
             \x20\x20  <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n\
             \x20\x20  <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n\
             \x20\x20  <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n\
             \x20\x20  <StartWhenAvailable>true</StartWhenAvailable>\n\
             \x20\x20  <Hidden>true</Hidden>\n\
             \x20\x20  <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>\n\
             \x20\x20  <RestartOnFailure>\n\
             \x20\x20    <Interval>PT1M</Interval>\n\
             \x20\x20    <Count>99</Count>\n\
             \x20\x20  </RestartOnFailure>\n\
             \x20\x20  <Priority>7</Priority>\n\
             \x20\x20</Settings>\n\
             \x20\x20<Actions Context=\"Author\">\n\
             \x20\x20  <Exec>\n\
             \x20\x20    <Command>{launcher_str}</Command>\n\
             \x20\x20    <Arguments>{arguments}</Arguments>\n\
             \x20\x20  </Exec>\n\
             \x20\x20</Actions>\n\
             </Task>\n"
        )
    }

    pub(super) fn service_start(
        endpoint: &super::ServeEndpoint,
        sync_every: u64,
        config_file: &std::path::Path,
    ) -> Result<()> {
        let bin = pond_bin();
        if !bin.is_file() {
            bail!(
                "could not resolve the pond binary to register ({}); run \
                 `pond service start` from an installed pond",
                bin.display()
            );
        }
        let launcher = pondw_bin(&bin)?;
        let log = super::service_log_path();
        let state_root = crate::syncstate::state_root();
        let pond_state = crate::syncstate::pond_state_dir();
        std::fs::create_dir_all(&pond_state)
            .with_context(|| format!("failed to create {}", pond_state.display()))?;

        for (what, path, sources) in [
            (
                "launcher",
                launcher.as_path(),
                "the installed pond binary's directory",
            ),
            ("pond binary", bin.as_path(), "the installed pond location"),
            (
                "serve log",
                log.as_path(),
                "the state dir (--state-dir or XDG_STATE_HOME)",
            ),
            (
                "state dir",
                state_root.as_path(),
                "--state-dir or XDG_STATE_HOME",
            ),
            ("config file", config_file, super::CONFIG_FILE_SOURCES),
        ] {
            super::reject_unembeddable(what, path, sources)?;
        }

        let arguments =
            serve_task_arguments(&log, &bin, endpoint, sync_every, &state_root, config_file);
        let launcher_str = launcher.display().to_string();
        if let Some(xml) = serve_query_xml()?
            && between(&xml, "<Command>", "</Command>")
                .map(xml_unescape)
                .as_deref()
                == Some(launcher_str.as_str())
            && between(&xml, "<Arguments>", "</Arguments>")
                .map(xml_unescape)
                .as_deref()
                == Some(arguments.as_str())
        {
            pond::output::line(&format!("already running ({})", endpoint.mcp_url()))?;
            return Ok(());
        }
        if shell_is_elevated() {
            pond::output::line(
                "note: this shell is elevated - the task will be owned by Administrators, \
                 and `pond service start`/`stop` from a normal shell will fail with \
                 Access denied; re-run from a non-elevated shell unless that is intended",
            )?;
        }

        let xml = serve_task_xml(&launcher, &arguments);
        let tmp_xml = pond_state.join("pond-serve-task.xml.tmp");
        std::fs::write(&tmp_xml, utf16le_bom(&xml))
            .with_context(|| format!("failed to write {}", tmp_xml.display()))?;
        let create_result = Command::new("schtasks")
            .args(["/Create", "/TN", SERVE_TASK_NAME, "/XML"])
            .arg(&tmp_xml)
            .arg("/F")
            .output()
            .context("failed to run schtasks /Create");
        let _ = std::fs::remove_file(&tmp_xml);

        let output = create_result?;
        if !output.status.success() {
            let stderr = decode_console(&output.stderr);
            let stderr = stderr.trim();
            if matches!(serve_query_xml(), Ok(Some(_))) {
                bail!(
                    "schtasks /Create failed: {stderr}\n\
                     the existing '{SERVE_TASK_NAME}' task could not be replaced - it was \
                     likely registered from an elevated shell and is owned by Administrators; \
                     remove it with `schtasks /Delete /TN {SERVE_TASK_NAME} /F` from an \
                     elevated shell, then re-run `pond service start` unelevated"
                );
            }
            bail!("schtasks /Create failed: {stderr}");
        }
        // A logon trigger alone would leave the endpoint dead until the next
        // sign-in, so the registration also starts it now. /Run's own failure
        // is not the registration's: report it and let the probe speak.
        let run = Command::new("schtasks")
            .args(["/Run", "/TN", SERVE_TASK_NAME])
            .output()
            .context("failed to run schtasks /Run")?;
        if !run.status.success() {
            pond::output::line(&format!(
                "note: schtasks /Run failed ({}); the task starts at the next sign-in",
                decode_console(&run.stderr).trim(),
            ))?;
        }
        super::report_service_started("task-scheduler", endpoint)?;
        pond::output::line(&format!(
            "{}      {}  (pond service logs)",
            pond::output::paint("logs", pond::output::dim()),
            crate::config::display(&crate::config::url_for_path(&log)?),
        ))?;
        Ok(())
    }

    pub(super) fn service_stop() -> Result<()> {
        // /End stops the running instance; it fails benignly on an idle task,
        // which says nothing about whether the registration exists.
        let _ = Command::new("schtasks")
            .args(["/End", "/TN", SERVE_TASK_NAME])
            .output();
        let output = Command::new("schtasks")
            .args(["/Delete", "/TN", SERVE_TASK_NAME, "/F"])
            .output()
            .context("failed to run schtasks /Delete")?;
        if output.status.success() {
            pond::output::line("service removed")?;
            return Ok(());
        }
        match service_probe()? {
            None => pond::output::line("no resident pond serve was registered")?,
            Some(_) => {
                bail!(
                    "schtasks /Delete failed: {}\n\
                     the '{SERVE_TASK_NAME}' task is likely owned by Administrators; remove it \
                     with `schtasks /Delete /TN {SERVE_TASK_NAME} /F` from an elevated shell",
                    decode_console(&output.stderr).trim()
                );
            }
        }
        Ok(())
    }

    /// Quote a path for the task's command line. Trailing backslashes are
    /// doubled: `CommandLineToArgvW` reads `\"` as an escaped quote, so
    /// `"C:\dir\"` would swallow the closing quote and run on to end of line -
    /// a state dir set to `C:\dir\` would otherwise register a task that fails
    /// or writes somewhere else on every tick.
    fn quote_arg(path: &std::path::Path) -> String {
        let text = path.display().to_string();
        let trailing = text.len() - text.trim_end_matches('\\').len();
        format!("\"{text}{}\"", "\\".repeat(trailing))
    }

    /// The `<Arguments>` line: the launcher's own `--log`, then the pond
    /// command line it runs.
    fn task_arguments(
        log: &std::path::Path,
        bin: &std::path::Path,
        state_root: &std::path::Path,
        config_file: &std::path::Path,
    ) -> String {
        format!(
            "--log {log} -- {bin} sync -q --no-wait --state-dir {state} --config-file {config}",
            log = quote_arg(log),
            bin = quote_arg(bin),
            state = quote_arg(state_root),
            config = quote_arg(config_file),
        )
    }

    /// Generate Task Scheduler XML for the pond-sync task.
    ///
    /// The `<Action>` runs `pondw.exe`, pond's windowless launcher: a
    /// console-subsystem binary in an interactive `Exec` action flashes a
    /// window on every tick, and a fire-and-forget shim would report its own
    /// exit code instead of the sync's.
    fn task_xml(launcher: &std::path::Path, arguments: &str, every: ScheduleEvery) -> String {
        let trigger = match every {
            ScheduleEvery::D1 => "    <CalendarTrigger>\n\
                 \x20\x20\x20\x20  <StartBoundary>2000-01-01T03:00:00</StartBoundary>\n\
                 \x20\x20\x20\x20  <Enabled>true</Enabled>\n\
                 \x20\x20\x20\x20  <ScheduleByDay>\
                 <DaysInterval>1</DaysInterval>\
                 </ScheduleByDay>\n\
                 \x20\x20\x20\x20</CalendarTrigger>"
                .to_owned(),
            _ => {
                let interval = match every {
                    ScheduleEvery::M5 => "PT5M",
                    ScheduleEvery::M15 => "PT15M",
                    ScheduleEvery::H1 => "PT1H",
                    ScheduleEvery::H6 => "PT6H",
                    ScheduleEvery::D1 => unreachable!(),
                };
                format!(
                    "    <TimeTrigger>\n\
                     \x20\x20\x20\x20  <Repetition>\n\
                     \x20\x20\x20\x20    <Interval>{interval}</Interval>\n\
                     \x20\x20\x20\x20    <StopAtDurationEnd>false</StopAtDurationEnd>\n\
                     \x20\x20\x20\x20  </Repetition>\n\
                     \x20\x20\x20\x20  <StartBoundary>2000-01-01T00:00:00</StartBoundary>\n\
                     \x20\x20\x20\x20  <Enabled>true</Enabled>\n\
                     \x20\x20\x20\x20</TimeTrigger>"
                )
            }
        };
        // Task Scheduler expands %VAR% in <Command> and <Arguments> at runtime;
        // the % gate in start() clears every embedded path before we get here.
        let launcher_str = xml_escape(&launcher.display().to_string());
        let arguments = xml_escape(arguments);
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n\
             <Task version=\"1.2\" \
             xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">\n\
             \x20\x20<RegistrationInfo>\n\
             \x20\x20  <Description>pond sync (managed by pond; do not edit)</Description>\n\
             \x20\x20</RegistrationInfo>\n\
             \x20\x20<Triggers>\n\
             {trigger}\n\
             \x20\x20</Triggers>\n\
             \x20\x20<Settings>\n\
             \x20\x20  <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n\
             \x20\x20  <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n\
             \x20\x20  <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n\
             \x20\x20  <StartWhenAvailable>true</StartWhenAvailable>\n\
             \x20\x20  <Hidden>true</Hidden>\n\
             \x20\x20  <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>\n\
             \x20\x20  <Priority>7</Priority>\n\
             \x20\x20</Settings>\n\
             \x20\x20<Actions Context=\"Author\">\n\
             \x20\x20  <Exec>\n\
             \x20\x20    <Command>{launcher_str}</Command>\n\
             \x20\x20    <Arguments>{arguments}</Arguments>\n\
             \x20\x20  </Exec>\n\
             \x20\x20</Actions>\n\
             </Task>\n"
        )
    }

    /// Escape XML special characters in element text / attribute values.
    fn xml_escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    }

    /// Inverse of `xml_escape`, for reading values back out of the XML Task
    /// Scheduler returns. `&amp;` unescapes last so `&amp;quot;` survives as a
    /// literal `&quot;` rather than collapsing into a quote.
    fn xml_unescape(s: &str) -> String {
        s.replace("&quot;", "\"")
            .replace("&gt;", ">")
            .replace("&lt;", "<")
            .replace("&amp;", "&")
    }

    /// Recover the registered cadence from a `schtasks /Query /XML ONE` body.
    fn parse_interval_from_xml(xml: &str) -> Option<ScheduleEvery> {
        // Repetition-based cadences carry <Interval>PT5M</Interval> etc.
        if let Some(interval) = between(xml, "<Interval>", "</Interval>") {
            let secs = if let Some(m) = interval
                .strip_prefix("PT")
                .and_then(|s| s.strip_suffix('M'))
            {
                m.parse::<u32>().ok().map(|m| m * 60)
            } else {
                let h = interval
                    .strip_prefix("PT")
                    .and_then(|s| s.strip_suffix('H'))?;
                h.parse::<u32>().ok().map(|h| h * 3_600)
            }?;
            return ScheduleEvery::from_secs(secs);
        }
        // Daily tasks carry <DaysInterval>1</DaysInterval> instead.
        if between(xml, "<DaysInterval>", "</DaysInterval>").is_some() {
            return ScheduleEvery::from_secs(86_400);
        }
        None
    }

    /// Substring of `text` strictly between the first `open` and the `close`
    /// that follows it. Returns `None` when either delimiter is absent.
    fn between<'a>(text: &'a str, open: &str, close: &str) -> Option<&'a str> {
        let start = text.find(open)? + open.len();
        let rest = &text[start..];
        let end = rest.find(close)?;
        Some(&rest[..end])
    }

    /// The binary path baked into the task. Prefer `pond.exe` on PATH (a
    /// stable install location that survives upgrades); fall back to this exe.
    fn pond_bin() -> PathBuf {
        crate::find_on_path("pond.exe")
            .or_else(|| crate::find_on_path("pond"))
            .unwrap_or_else(|| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("pond")))
    }

    /// True when this process runs elevated. `whoami /groups` lists the
    /// token's groups with their SIDs, and an elevated token carries the
    /// High Mandatory Level label S-1-16-12288 - a locale-independent probe,
    /// unlike parsing schtasks/net output text. Registration-time only, so
    /// the process spawn is not on any hot path.
    fn shell_is_elevated() -> bool {
        Command::new("whoami")
            .args(["/groups"])
            .output()
            .map(|output| decode_console(&output.stdout).contains("S-1-16-12288"))
            .unwrap_or(false)
    }

    /// The launcher shipped beside `pond.exe`. `bin` comes from PATH, which
    /// under winget is a symlink in its Links dir with no pondw.exe next to it,
    /// so the running binary's own directory is the fallback. Scoop lands on
    /// the same fallback by design (its manifest shims `pond.exe` only), and
    /// the path it yields is the stable `apps\pond\current\` junction: the
    /// shim spawns the real exe as a child and GetModuleFileNameW preserves
    /// the junction (measured 2026-08-26). Do NOT canonicalize here -
    /// resolving the junction would pin the task to a versioned dir that
    /// `scoop cleanup` deletes on the next update.
    fn pondw_bin(bin: &std::path::Path) -> Result<PathBuf> {
        [
            Some(bin.with_file_name("pondw.exe")),
            std::env::current_exe()
                .ok()
                .map(|exe| exe.with_file_name("pondw.exe")),
        ]
        .into_iter()
        .flatten()
        .find(|path| path.is_file())
        .context(
            "pondw.exe not found beside pond.exe: it ships in the release zip and runs \
             the scheduled sync without a console window - reinstall pond and re-run \
             `pond schedule start`",
        )
    }

    /// Decode process output that may be UTF-16 (schtasks `/XML` and some
    /// localized consoles) or UTF-8.
    fn decode_console(bytes: &[u8]) -> String {
        if bytes.len() >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE {
            return decode_utf16le(&bytes[2..]);
        }
        if bytes.iter().take(64).filter(|&&b| b == 0).count() >= 2 {
            return decode_utf16le(bytes);
        }
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn decode_utf16le(bytes: &[u8]) -> String {
        let units: Vec<u16> = bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u16::from_le_bytes(*pair))
            .collect();
        String::from_utf16_lossy(&units)
    }

    /// Encode `text` as UTF-16LE with a BOM - the shape `schtasks /Create
    /// /XML` decodes correctly on every system code page (a BOM-less file is
    /// read as ANSI, mojibaking non-ASCII paths).
    fn utf16le_bom(text: &str) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + text.len() * 2);
        bytes.extend_from_slice(&[0xFF, 0xFE]);
        for unit in text.encode_utf16() {
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        bytes
    }

    #[cfg(test)]
    mod tests {
        #![allow(clippy::expect_used, clippy::unwrap_used)]
        use super::*;

        /// The action shape `start()` builds, for tests that need one.
        fn fixture() -> (std::path::PathBuf, String) {
            let launcher = std::path::PathBuf::from("C:\\Program Files\\pond\\pondw.exe");
            let arguments = task_arguments(
                std::path::Path::new("C:\\Users\\Adam\\AppData\\Local\\pond\\state\\sync.log"),
                std::path::Path::new("C:\\Program Files\\pond\\pond.exe"),
                std::path::Path::new("C:\\Users\\Adam\\AppData\\Local\\pond\\state"),
                std::path::Path::new("C:\\Users\\Adam\\AppData\\Roaming\\pond\\config.toml"),
            );
            (launcher, arguments)
        }

        #[test]
        fn all_cadences_round_trip_through_task_xml() {
            let (launcher, arguments) = fixture();
            for every in [
                ScheduleEvery::M5,
                ScheduleEvery::M15,
                ScheduleEvery::H1,
                ScheduleEvery::H6,
                ScheduleEvery::D1,
            ] {
                let xml = task_xml(&launcher, &arguments, every);
                let parsed = parse_interval_from_xml(&xml);
                assert_eq!(parsed, Some(every), "cadence {every:?} did not round-trip");
            }
        }

        /// The resident task is supervised and unbounded, unlike the sync
        /// task: a logon trigger instead of a repetition, `RestartOnFailure`
        /// as launchd's `KeepAlive` equivalent, and no execution time limit
        /// (`PT1H` would kill the server every hour).
        #[test]
        fn the_resident_task_supervises_an_unbounded_serve() {
            let launcher = std::path::PathBuf::from("C:\\Program Files\\pond\\pondw.exe");
            let endpoint = super::super::ServeEndpoint::default();
            let arguments = serve_task_arguments(
                std::path::Path::new("C:\\Users\\Adam\\AppData\\Local\\pond\\state\\serve.log"),
                std::path::Path::new("C:\\Program Files\\pond\\pond.exe"),
                &endpoint,
                5,
                std::path::Path::new("C:\\Users\\Adam\\AppData\\Local\\pond\\state"),
                std::path::Path::new("C:\\Users\\Adam\\AppData\\Roaming\\pond\\config.toml"),
            );
            assert!(
                arguments.contains("serve --transport http --with-sync"),
                "{arguments}"
            );
            assert!(
                arguments.contains("--host 127.0.0.1 --port 9797"),
                "{arguments}"
            );
            // An Exec action has no environment block, so both pins ride as
            // arguments - the same invariant the sync task carries.
            assert!(
                arguments.contains("--state-dir \"C:\\Users\\Adam\\AppData\\Local\\pond\\state\""),
                "{arguments}"
            );
            assert!(arguments.contains("--config-file \"C:\\"), "{arguments}");
            assert_eq!(
                super::super::parse_endpoint(&arguments).as_ref(),
                Some(&endpoint),
            );

            let xml = serve_task_xml(&launcher, &arguments);
            assert!(xml.contains("<Command>C:\\Program Files\\pond\\pondw.exe</Command>"));
            assert!(xml.contains("<LogonTrigger>"), "{xml}");
            assert!(
                xml.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"),
                "{xml}"
            );
            assert!(xml.contains("<RestartOnFailure>"), "{xml}");
            assert!(!xml.contains("<Repetition>"), "{xml}");
            // The no-op comparison reads the decoded element text back.
            assert_eq!(
                between(&xml, "<Arguments>", "</Arguments>").map(xml_unescape),
                Some(arguments),
            );
        }

        #[test]
        fn launcher_problem_flags_only_a_missing_command() {
            let dir = tempfile::tempdir().expect("tempdir");
            let real = dir.path().join("pondw.exe");
            std::fs::write(&real, b"x").expect("write");
            let present = format!("<Command>{}</Command>", real.display());
            assert_eq!(launcher_problem(&present), None);

            let gone = dir.path().join("uninstalled").join("pondw.exe");
            let problem = launcher_problem(&format!("<Command>{}</Command>", gone.display()))
                .expect("problem");
            assert!(problem.contains("pond schedule start"), "{problem}");
            assert!(problem.contains("uninstalled"), "{problem}");
        }

        #[test]
        fn between_finds_content_between_delimiters() {
            assert_eq!(between("<Foo>42</Foo>", "<Foo>", "</Foo>"), Some("42"));
            assert_eq!(between("<A>x</A><B>y</B>", "<B>", "</B>"), Some("y"));
            assert_eq!(between("no match", "<X>", "</X>"), None);
            assert_eq!(between("<Open>missing close", "<Open>", "</Open>"), None);
        }

        #[test]
        fn decode_console_handles_utf8_and_utf16le_bom() {
            assert_eq!(decode_console(b"hello world"), "hello world");
            let text = "hello";
            let mut bytes: Vec<u8> = vec![0xFF, 0xFE];
            for unit in text.encode_utf16() {
                bytes.extend_from_slice(&unit.to_le_bytes());
            }
            assert_eq!(decode_console(&bytes), "hello");
        }

        #[test]
        fn utf16le_bom_leads_with_bom_and_round_trips_non_ascii() {
            // The task XML file MUST carry a UTF-16LE BOM: schtasks reads a
            // BOM-less file as ANSI, mojibaking non-ASCII state paths.
            let text = "C:\\Users\\p\u{f6}nd \u{e9}tat\\pondw.exe";
            let bytes = utf16le_bom(text);
            assert_eq!(&bytes[..2], &[0xFF, 0xFE], "BOM must lead the file");
            assert_eq!(bytes.len(), 2 + text.encode_utf16().count() * 2);
            // decode_console is the module's own BOM-aware reader; the pair
            // must round-trip exactly.
            assert_eq!(decode_console(&bytes), text);
        }

        #[test]
        fn task_xml_execs_the_launcher_and_contains_expected_settings() {
            let (launcher, arguments) = fixture();
            let xml = task_xml(&launcher, &arguments, ScheduleEvery::M5);
            // The launcher IS the action: no wscript, no .cmd, no .vbs.
            assert!(xml.contains("<Command>C:\\Program Files\\pond\\pondw.exe</Command>"));
            assert!(!xml.contains("wscript"));
            // battery + catch-up settings
            assert!(xml.contains("<StartWhenAvailable>true</StartWhenAvailable>"));
            assert!(xml.contains("<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>"));
            assert!(xml.contains("<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>"));
            assert!(xml.contains("<Hidden>true</Hidden>"));
        }

        #[test]
        fn a_trailing_separator_does_not_escape_the_closing_quote() {
            // `--state-dir "C:\dir\"` would swallow the quote and run on.
            let args = task_arguments(
                std::path::Path::new("C:\\s\\sync.log"),
                std::path::Path::new("C:\\bin\\pond.exe"),
                std::path::Path::new("C:\\my state\\"),
                std::path::Path::new("C:\\c\\config.toml"),
            );
            assert!(args.contains("--state-dir \"C:\\my state\\\\\""), "{args}");
            // Every quote still pairs off.
            assert_eq!(args.matches('"').count() % 2, 0, "{args}");
        }

        #[test]
        fn task_arguments_pin_the_state_dir_and_quote_spaced_paths() {
            let (_, arguments) = fixture();
            // The pin an Exec action's missing environment block forces.
            assert!(
                arguments.contains("--state-dir \"C:\\Users\\Adam\\AppData\\Local\\pond\\state\""),
                "{arguments}"
            );
            assert!(arguments.contains("sync -q --no-wait"), "{arguments}");
            assert!(
                arguments.contains(
                    "--config-file \"C:\\Users\\Adam\\AppData\\Roaming\\pond\\config.toml\""
                ),
                "{arguments}"
            );
            // Every path is quoted: `C:\Program Files\...` splits otherwise.
            assert!(
                arguments.contains("-- \"C:\\Program Files\\pond\\pond.exe\""),
                "{arguments}"
            );
            assert!(arguments.starts_with("--log \"C:\\"), "{arguments}");
        }

        #[test]
        fn action_survives_the_xml_escape_round_trip() {
            // start()'s already-scheduled no-op compares the DECODED element
            // text, because Task Scheduler re-serializes what it stores and
            // need not escape a quote in element content. Both directions of
            // that comparison have to agree with the escaper.
            let (launcher, arguments) = fixture();
            let xml = task_xml(&launcher, &arguments, ScheduleEvery::M5);
            assert_eq!(
                between(&xml, "<Arguments>", "</Arguments>").map(xml_unescape),
                Some(arguments)
            );
            assert_eq!(
                between(&xml, "<Command>", "</Command>").map(xml_unescape),
                Some(launcher.display().to_string())
            );
        }

        #[test]
        fn xml_unescape_leaves_an_escaped_entity_literal() {
            // &amp; unescapes last, so a literal "&quot;" in a path does not
            // collapse into a quote and desync the no-op comparison.
            assert_eq!(xml_unescape(&xml_escape("a&quot;b")), "a&quot;b");
            assert_eq!(xml_unescape(&xml_escape("a\"<b>&c")), "a\"<b>&c");
        }
    }
}

// ===========================================================================
// Tests shared across all platforms
// ===========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn every_round_trips_through_secs_and_labels() {
        for every in [
            ScheduleEvery::M5,
            ScheduleEvery::M15,
            ScheduleEvery::H1,
            ScheduleEvery::H6,
            ScheduleEvery::D1,
        ] {
            assert_eq!(ScheduleEvery::from_secs(every.secs()), Some(every));
        }
        assert_eq!(ScheduleEvery::from_secs(123), None);
    }

    #[test]
    fn unembeddable_paths_are_rejected_before_registration() {
        for bad in [
            "/home/user/a\"b/config.toml",
            "/home/user/100%/config.toml",
            "/home/user/a<b>/config.toml",
            "/home/user/a&b/config.toml",
            "/home/user/a\nb/config.toml",
        ] {
            let error = reject_unembeddable("config file", Path::new(bad), "POND_CONFIG_FILE")
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default();
            assert!(error.contains("config file"), "accepted {bad}");
            assert!(error.contains("POND_CONFIG_FILE"), "{error}");
        }
        assert!(
            reject_unembeddable(
                "config file",
                Path::new("/home/user/.config/pond/config.toml"),
                "POND_CONFIG_FILE"
            )
            .is_ok()
        );
    }

    /// The registration is loopback-only, which is exactly what rmcp's
    /// default `Host` allowlist admits - so the local MCP registration needs
    /// no `--allowed-host` and the DNS-rebinding defence stays intact.
    #[test]
    fn the_default_endpoint_is_loopback() {
        let default = ServeEndpoint::default();
        assert!(default.is_loopback());
        assert_eq!(default.mcp_url(), "http://127.0.0.1:9797/mcp");
        assert!(
            ServeEndpoint {
                host: "localhost".to_owned(),
                port: 1,
            }
            .is_loopback()
        );
        assert!(
            ServeEndpoint {
                host: "::1".to_owned(),
                port: 1,
            }
            .is_loopback()
        );
        assert!(
            !ServeEndpoint {
                host: "0.0.0.0".to_owned(),
                port: 1,
            }
            .is_loopback()
        );
        assert!(
            !ServeEndpoint {
                host: "pond.example.com".to_owned(),
                port: 1,
            }
            .is_loopback()
        );
    }

    /// `pond service status` recovers the endpoint from whatever the backend
    /// stored, so every template's argument shape has to parse back.
    #[test]
    fn endpoints_round_trip_out_of_every_registration_shape() {
        let want = ServeEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 9800,
        };
        let plist = "\t\t<string>--host</string>\n\t\t<string>127.0.0.1</string>\n\
                     \t\t<string>--port</string>\n\t\t<string>9800</string>\n";
        assert_eq!(parse_endpoint(plist).as_ref(), Some(&want));
        let unit = "ExecStart=/usr/local/bin/pond serve --transport http --with-sync \
                    --host 127.0.0.1 --port 9800 --sync-every 5\n";
        assert_eq!(parse_endpoint(unit).as_ref(), Some(&want));
        let task = "<Arguments>--log \"C:\\s\\serve.log\" -- \"C:\\bin\\pond.exe\" serve \
                    --transport http --with-sync --host 127.0.0.1 --port 9800 \
                    --sync-every 5</Arguments>";
        assert_eq!(parse_endpoint(task).as_ref(), Some(&want));
        // A registration written by some other pond, or a hand-edited one
        // missing a flag, reports "no endpoint" rather than a guess.
        assert_eq!(parse_endpoint("ExecStart=/usr/local/bin/pond serve"), None);
        assert_eq!(parse_endpoint("--host 127.0.0.1 --port not-a-port"), None);
    }

    /// A non-loopback `--host` cannot be registered: the /mcp route would
    /// answer 403 for that name until `--allowed-host` listed it, and a
    /// registration may not make that call for the operator.
    #[test]
    fn service_start_refuses_a_non_loopback_bind() {
        let error = service_start(
            &ServeEndpoint {
                host: "0.0.0.0".to_owned(),
                port: 9797,
            },
            5,
            Some(PathBuf::from("/tmp/pond-config.toml")),
        )
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
        assert!(error.contains("not loopback"), "{error}");
        assert!(error.contains("--allowed-host"), "{error}");
    }

    /// The pinned path is re-read from the scheduler's working directory, so
    /// a relative `--config-file` must leave the registration absolute.
    #[test]
    fn config_pin_is_absolutized() {
        let pinned = config_file(Some(PathBuf::from("relative/config.toml")));
        assert!(pinned.is_absolute(), "{}", pinned.display());
    }
}
