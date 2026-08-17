//! `pond schedule`: register `pond sync -q --no-wait` with the OS scheduler.
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
use pond::output::{dim, line, line_err, paint};
use std::path::PathBuf;

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
    /// Exit 0 when active, 1 when not configured.
    Status,
    /// Show recent scheduled-sync output.
    Logs {
        /// Number of trailing log lines to print.
        #[arg(long, default_value_t = 50)]
        lines: usize,
    },
}

/// One scheduler probe's answer, shared by the `pond status` text line and
/// the JSON document (which needs the fields structured, not pre-rendered).
pub(crate) struct ScheduleSnapshot {
    pub line: String,
    pub active: bool,
    pub backend: Option<&'static str>,
    pub every: Option<ScheduleEvery>,
}

// ===========================================================================
// Shared across all platforms
// ===========================================================================

/// Internal state of the OS scheduler for the pond-sync registration.
enum State {
    Active {
        backend: &'static str,
        every: Option<ScheduleEvery>,
    },
    Inactive,
}
use State::{Active, Inactive};

fn render_state(state: &State) -> String {
    match state {
        Active { backend, every } => format!(
            "{}  active ({backend}{})",
            paint("schedule", dim()),
            every
                .map(|every| format!(", every {}", every.label()))
                .unwrap_or_default(),
        ),
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

pub(crate) fn status_line() -> String {
    status_snapshot().line
}

pub(crate) fn status_snapshot() -> ScheduleSnapshot {
    match platform_probe() {
        Ok(state) => {
            let (active, backend, every) = match &state {
                Active { backend, every } => (true, Some(*backend), *every),
                Inactive => (false, None, None),
            };
            ScheduleSnapshot {
                line: render_state(&state),
                active,
                backend,
                every,
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
        },
    }
}

pub(crate) fn run(command: ScheduleCmd) -> Result<()> {
    match command {
        ScheduleCmd::Start { every } => platform_start(every),
        ScheduleCmd::Stop => platform_stop(),
        ScheduleCmd::Status => {
            let state = platform_probe()?;
            line(&render_state(&state))?;
            if let Active { .. } = state {
                line(&format!(
                    "{}      {}  (pond schedule logs)",
                    paint("logs", dim()),
                    crate::config::display(&crate::config::url_for_path(log_path())?),
                ))?;
                Ok(())
            } else {
                std::process::exit(1);
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

    let path = log_path();
    line_err(&paint(
        &format!(
            "log file: {}",
            crate::config::display(&crate::config::url_for_path(&path)?)
        ),
        dim(),
    ))?;
    let text = match std::fs::read_to_string(&path) {
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

// Re-export `start` for `pond init`, which calls `schedule::start` directly.
#[cfg(unix)]
pub(crate) use unix::start;
#[cfg(windows)]
pub(crate) use windows::start;
#[cfg(not(any(unix, windows)))]
pub(crate) fn start(_every: ScheduleEvery) -> Result<()> {
    anyhow::bail!("pond schedule is not supported on this platform yet")
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

#[cfg(windows)]
fn platform_start(every: ScheduleEvery) -> Result<()> {
    windows::start(every)
}
#[cfg(unix)]
fn platform_start(every: ScheduleEvery) -> Result<()> {
    unix::start(every)
}
#[cfg(not(any(unix, windows)))]
fn platform_start(_every: ScheduleEvery) -> Result<()> {
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

// ===========================================================================
// Platform: unix (launchd / systemd / cron)
// ===========================================================================

#[cfg(unix)]
mod unix {
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    use anyhow::{Context, Result, bail};

    use super::{ScheduleEvery, State};
    use State::{Active, Inactive};

    const LAUNCHD_LABEL: &str = "sh.pond.sync";
    const CRON_FENCE_BEGIN: &str = "# BEGIN POND SYNC (maintained by pond; do not edit)";
    const CRON_FENCE_END: &str = "# END POND SYNC";

    pub(super) fn probe() -> Result<State> {
        match std::env::consts::OS {
            "macos" => probe_launchd(),
            "linux" => {
                if systemd_timer_enabled() {
                    return Ok(Active {
                        backend: "systemd",
                        every: read_systemd_interval(),
                    });
                }
                if let Some(entry) = read_cron_fence_entry()? {
                    return Ok(Active {
                        backend: "cron",
                        every: cron_entry_interval(&entry),
                    });
                }
                Ok(Inactive)
            }
            _ => Ok(Inactive),
        }
    }

    /// Register the schedule. Shared by `pond schedule start` and the
    /// `pond init` schedule section (which calls it after the config write).
    pub(crate) fn start(every: ScheduleEvery) -> Result<()> {
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
        // job's environment (same precedent as the baked-in log path).
        let state = crate::syncstate::state_root();
        // The path is embedded verbatim in plist XML, a systemd quoted
        // Environment= value, and a crontab line (where % means newline) -
        // none of which this template escapes. Reject the exotic characters
        // up front instead of writing a silently broken registration.
        let state_str = state.display().to_string();
        if state_str.contains(['<', '>', '&', '"', '%', '\n', '\r']) {
            // Name the resolved path AND its sources: the bad character may
            // come from $HOME (the fallback), where "unset XDG_STATE_HOME"
            // would be a dead-end instruction.
            bail!(
                "state dir {state_str:?} contains a character (< > & \" % or a newline) that \
                 cannot be embedded in a scheduler registration; it resolves from \
                 XDG_STATE_HOME, falling back to $HOME/.local/state - set XDG_STATE_HOME \
                 to a simpler absolute path and re-run `pond schedule start`"
            );
        }
        match std::env::consts::OS {
            "macos" => start_launchd(&bin, every, &log, &state),
            "linux" => {
                if systemd_user_available() {
                    // Switching schedulers must not leave the other one
                    // firing: a systemd start strips any cron fence.
                    remove_cron_fence()?;
                    start_systemd(&bin, every, &state)
                } else {
                    stop_systemd()?;
                    start_cron(&bin, every, &log, &state)
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

    fn plist_path() -> Result<PathBuf> {
        let home = std::env::var_os("HOME").context("HOME is not set")?;
        Ok(PathBuf::from(home)
            .join("Library/LaunchAgents")
            .join(format!("{LAUNCHD_LABEL}.plist")))
    }

    fn plist_body(bin: &Path, every: ScheduleEvery, log: &Path, state: &Path) -> String {
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
        )
    }

    fn launchd_registered(uid: &str) -> bool {
        Command::new("launchctl")
            .args(["print", &format!("gui/{uid}/{LAUNCHD_LABEL}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn start_launchd(bin: &Path, every: ScheduleEvery, log: &Path, state: &Path) -> Result<()> {
        let plist = plist_path()?;
        let body = plist_body(bin, every, log, state);
        let uid = current_uid()?;
        let unchanged = std::fs::read_to_string(&plist)
            .map(|existing| existing == body)
            .unwrap_or(false);
        if unchanged && launchd_registered(&uid) {
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
        pond::output::line(&super::render_state(&super::State::Active {
            backend: "launchd",
            every: Some(every),
        }))?;
        pond::output::line(&format!(
            "{}      {}  (pond schedule logs)",
            pond::output::paint("logs", pond::output::dim()),
            crate::config::display(&crate::config::url_for_path(log)?),
        ))?;
        Ok(())
    }

    fn stop_launchd() -> Result<bool> {
        let plist = plist_path()?;
        let uid = current_uid()?;
        let was_registered = launchd_registered(&uid);
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
        if !launchd_registered(&uid) {
            return Ok(Inactive);
        }
        let every = std::fs::read_to_string(plist_path()?)
            .ok()
            .and_then(|body| plist_interval(&body));
        Ok(Active {
            backend: "launchd",
            every,
        })
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

    fn systemd_service_body(bin: &Path, state: &Path) -> String {
        format!(
            "# created and maintained by pond; edits may be replaced\n\
             [Unit]\n\
             Description=pond sync\n\n\
             [Service]\n\
             Type=oneshot\n\
             Environment=\"XDG_STATE_HOME={}\"\n\
             ExecStart={} sync -q --no-wait\n",
            state.display(),
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

    fn start_systemd(bin: &Path, every: ScheduleEvery, state: &Path) -> Result<()> {
        let dir = systemd_unit_dir();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let service_path = dir.join("pond-sync.service");
        let timer_path = dir.join("pond-sync.timer");
        let service = systemd_service_body(bin, state);
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
        pond::output::line(&super::render_state(&super::State::Active {
            backend: "systemd",
            every: Some(every),
        }))?;
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
    ) -> String {
        let command = format!(
            "XDG_STATE_HOME=\"{}\" {} sync -q --no-wait >> {} 2>&1",
            state.display(),
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

    fn start_cron(bin: &Path, every: ScheduleEvery, log: &Path, state: &Path) -> Result<()> {
        let existing = read_crontab()?;
        // The command-shape check keeps this a real idempotence test: a fence
        // entry written by an older pond (`sync -q` without `--no-wait`, or
        // without the pinned state dir) must re-register, not be kept as
        // "already scheduled".
        if let Some(entry) = fence_entry_in(&existing)
            && cron_entry_interval(&entry) == Some(every)
            && entry.contains(&bin.display().to_string())
            && entry.contains("--no-wait")
            && entry.contains("XDG_STATE_HOME=")
        {
            pond::output::line(&format!("already scheduled (every {})", every.label()))?;
            return Ok(());
        }
        let entry = cron_entry(bin, every, log, fastrand::u32(0..60), state);
        let mut body = strip_cron_fence(&existing);
        if !body.is_empty() && !body.ends_with('\n') {
            body.push('\n');
        }
        body.push_str(&fence_block(&entry));
        write_crontab(&body)?;
        pond::output::line(&super::render_state(&super::State::Active {
            backend: "cron",
            every: Some(every),
        }))?;
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

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn cron_entries_reverse_map_to_their_cadence() {
            let bin = Path::new("/usr/local/bin/pond");
            let log = Path::new("/tmp/sync.log");
            let state = Path::new("/home/user/.local/state");
            for every in [
                ScheduleEvery::M5,
                ScheduleEvery::M15,
                ScheduleEvery::H1,
                ScheduleEvery::H6,
                ScheduleEvery::D1,
            ] {
                for minute in [0, 7, 59] {
                    let entry = cron_entry(bin, every, log, minute, state);
                    assert_eq!(cron_entry_interval(&entry), Some(every), "entry: {entry}");
                }
            }
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
        })
    }

    /// Register (or replace, via `/F`) the Task Scheduler job. Shared by
    /// `pond schedule start` and the `pond init` schedule section.
    pub(crate) fn start(every: ScheduleEvery) -> Result<()> {
        let bin = pond_bin();
        let launcher = pondw_bin(&bin)?;
        let log = super::log_path();
        // state_root is what we pin into --state-dir; pond_state_dir is the
        // directory where the log, lock, and last-sync record live.
        let state_root = crate::syncstate::state_root();
        let pond_state = crate::syncstate::pond_state_dir();
        std::fs::create_dir_all(&pond_state)
            .with_context(|| format!("failed to create {}", pond_state.display()))?;

        // Gate on % in every path that lands in the XML: Task Scheduler expands
        // %VAR% inside <Command> and <Arguments> at runtime with NO escape
        // syntax. Mirrors the unix gate that blocks % in cron/plist templates.
        for path in [&launcher, &bin, &log, &state_root] {
            let text = path.display().to_string();
            if text.contains('%') {
                bail!(
                    "{text:?} contains '%' which Task Scheduler expands in the task \
                     XML with no escape syntax; the state dir resolves from \
                     --state-dir or XDG_STATE_HOME, falling back to \
                     %LOCALAPPDATA%\\pond\\state - move pond or its state dir to a \
                     path without '%' and re-run `pond schedule start`"
                );
            }
        }

        let arguments = task_arguments(&log, &bin, &state_root);

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

        // The pin is registration-time: a later shell-only override splits the
        // lock and last-sync record between the scheduled and manual syncs, and
        // the task will not follow it.
        if std::env::var_os("XDG_STATE_HOME").is_some() {
            pond::output::line(&format!(
                "note: XDG_STATE_HOME is set; the task is pinned to {} and will not follow later changes",
                state_root.display()
            ))?;
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
            bail!(
                "schtasks /Create failed: {}",
                decode_console(&output.stderr).trim()
            );
        }

        pond::output::line(&super::render_state(&super::State::Active {
            backend: "task-scheduler",
            every: Some(every),
        }))?;
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
                    "schtasks /Delete failed: {}",
                    decode_console(&output.stderr).trim()
                );
            }
        }
        Ok(())
    }

    /// The `<Arguments>` line: the launcher's own `--log`, then the pond
    /// command line it runs.
    fn task_arguments(
        log: &std::path::Path,
        bin: &std::path::Path,
        state_root: &std::path::Path,
    ) -> String {
        format!(
            "--log \"{log}\" -- \"{bin}\" sync -q --no-wait --state-dir \"{state}\"",
            log = log.display(),
            bin = bin.display(),
            state = state_root.display(),
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
            } else if let Some(h) = interval
                .strip_prefix("PT")
                .and_then(|s| s.strip_suffix('H'))
            {
                h.parse::<u32>().ok().map(|h| h * 3_600)
            } else {
                return None;
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

    /// The launcher shipped beside `pond.exe`. `bin` comes from PATH, which
    /// under winget is a symlink in its Links dir with no pondw.exe next to it,
    /// so the running binary's own directory is the fallback.
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
            .chunks_exact(2)
            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
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
        fn task_arguments_pin_the_state_dir_and_quote_spaced_paths() {
            let (_, arguments) = fixture();
            // The pin an Exec action's missing environment block forces.
            assert!(
                arguments.contains("--state-dir \"C:\\Users\\Adam\\AppData\\Local\\pond\\state\""),
                "{arguments}"
            );
            assert!(arguments.contains("sync -q --no-wait"), "{arguments}");
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
}
