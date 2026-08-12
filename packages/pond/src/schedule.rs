//! `pond schedule`: register `pond sync -q --no-wait` with the OS scheduler.
//!
//! macOS uses launchd ONLY (cron on macOS runs without the user's GUI
//! context, trips TCC folder-access denials, and silently drops jobs that
//! span sleep). Linux prefers systemd user timers (`Persistent=true` catches
//! up after downtime) and falls back to a fenced crontab block. The
//! scheduled job is `pond sync -q --no-wait`: NOT `--yes`, so an unattended
//! run can never auto-enable freshly-detected adapters, and `--no-wait` so a
//! tick that lands while another sync holds the per-store lock skips cleanly
//! (exit 0) instead of queueing behind it.
//!
//! Bin-only module: OS-scheduler integration has no library callers.

#[cfg(not(any(unix, windows)))]
use anyhow::{Result, bail};
use clap::{Subcommand, ValueEnum};

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

    #[cfg(any(unix, windows))]
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

#[cfg(windows)]
pub(crate) use windows::{run, start, status_line, status_snapshot};

#[cfg(not(any(unix, windows)))]
pub(crate) fn run(_command: ScheduleCmd) -> Result<()> {
    bail!("pond schedule is not supported on this platform yet")
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn status_line() -> String {
    use pond::output::{dim, paint};
    format!(
        "{}  not supported on this platform",
        paint("schedule", dim())
    )
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn start(_every: ScheduleEvery) -> Result<()> {
    bail!("pond schedule is not supported on this platform yet")
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn status_snapshot() -> ScheduleSnapshot {
    ScheduleSnapshot {
        line: status_line(),
        active: false,
        backend: None,
        every: None,
    }
}

/// One scheduler probe's answer, shared by the `pond status` text line and
/// the JSON document (which needs the fields structured, not pre-rendered).
pub(crate) struct ScheduleSnapshot {
    pub line: String,
    pub active: bool,
    pub backend: Option<&'static str>,
    pub every: Option<ScheduleEvery>,
}

#[cfg(unix)]
pub(crate) use unix::{run, start, status_line, status_snapshot};

#[cfg(unix)]
mod unix {
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    use anyhow::{Context, Result, bail};

    use super::{ScheduleCmd, ScheduleEvery};
    use pond::output::{dim, line, line_err, paint};

    const LAUNCHD_LABEL: &str = "sh.pond.sync";
    const CRON_FENCE_BEGIN: &str = "# BEGIN POND SYNC (maintained by pond; do not edit)";
    const CRON_FENCE_END: &str = "# END POND SYNC";

    pub(crate) fn run(command: ScheduleCmd) -> Result<()> {
        match command {
            ScheduleCmd::Start { every } => start(every),
            ScheduleCmd::Stop => stop(),
            ScheduleCmd::Status => {
                let state = probe()?;
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

    /// The `pond status` schedule line: active backend + cadence, or the
    /// command that would set one up. Never errors - status must render even
    /// when the scheduler probe can't run.
    pub(crate) fn status_line() -> String {
        status_snapshot().line
    }

    /// One probe (a launchctl/systemctl spawn) serving every `pond status`
    /// need: the rendered schedule line plus the structured active/backend/
    /// cadence fields (status combines the cadence with the last-sync record
    /// to estimate the next run; JSON emits the fields directly).
    pub(crate) fn status_snapshot() -> super::ScheduleSnapshot {
        match probe() {
            Ok(state) => {
                let (active, backend, every) = match &state {
                    Active { backend, every } => (true, Some(*backend), *every),
                    Inactive => (false, None, None),
                };
                super::ScheduleSnapshot {
                    line: render_state(&state),
                    active,
                    backend,
                    every,
                }
            }
            Err(_) => super::ScheduleSnapshot {
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

    /// Register the schedule. Shared by `pond schedule start` and the
    /// `pond init` schedule section (which calls it after the config write).
    pub(crate) fn start(every: ScheduleEvery) -> Result<()> {
        let bin = pond_bin();
        let log = log_path();
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

    fn stop() -> Result<()> {
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
            line("schedule removed")?;
        } else {
            line("nothing was scheduled")?;
        }
        Ok(())
    }

    fn probe() -> Result<State> {
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

    fn logs(lines: usize) -> Result<()> {
        // systemd routes unit output to the journal; everything else writes
        // the log file named in the plist / cron entry.
        if std::env::consts::OS == "linux" && systemd_timer_enabled() {
            let status = Command::new("journalctl")
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
                bail!("journalctl exited {status}");
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

    /// The binary path baked into the scheduler registration. Prefer the
    /// `pond` on PATH: that's a stable symlink (`/opt/homebrew/bin/pond`,
    /// `~/.cargo/bin/pond`, a nix profile path) that survives upgrades.
    /// `current_exe()` is the fallback - on Homebrew it resolves into a
    /// versioned Cellar path that the next upgrade deletes, which is a known
    /// way to silently kill a schedule.
    fn pond_bin() -> PathBuf {
        crate::find_on_path("pond")
            .unwrap_or_else(|| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("pond")))
    }

    /// `$XDG_STATE_HOME/pond/sync.log`, default `~/.local/state/pond/sync.log`.
    fn log_path() -> PathBuf {
        crate::syncstate::pond_state_dir().join("sync.log")
    }

    /// Numeric uid for the `gui/<uid>` launchd domain. Shelled out to
    /// `id -u`: pond denies `unsafe`, so `libc::getuid()` is off the table.
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
            line(&format!("already scheduled (every {})", every.label()))?;
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
        line(&render_state(&Active {
            backend: "launchd",
            every: Some(every),
        }))?;
        line(&format!(
            "{}      {}  (pond schedule logs)",
            paint("logs", dim()),
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
        // not an error (stop is documented to succeed when nothing is
        // registered, and bootout/a racing stop may have removed it already).
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

    fn systemd_timer_enabled() -> bool {
        Command::new("systemctl")
            .args(["--user", "is-enabled", "pond-sync.timer"])
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
            line(&format!("already scheduled (every {})", every.label()))?;
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
        line(&render_state(&Active {
            backend: "systemd",
            every: Some(every),
        }))?;
        line(&format!(
            "{}      journalctl --user -u pond-sync.service  (pond schedule logs)",
            paint("logs", dim()),
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
        // Remove unconditionally; a missing unit is not an error (disable
        // --now or a racing stop may have already taken it).
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

    /// Pull pond's fenced cron entry out of a crontab body already in hand.
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
            line(&format!("already scheduled (every {})", every.label()))?;
            return Ok(());
        }
        let entry = cron_entry(bin, every, log, fastrand::u32(0..60), state);
        let mut body = strip_cron_fence(&existing);
        if !body.is_empty() && !body.ends_with('\n') {
            body.push('\n');
        }
        body.push_str(&fence_block(&entry));
        write_crontab(&body)?;
        line(&render_state(&Active {
            backend: "cron",
            every: Some(every),
        }))?;
        line(&format!(
            "{}      {}  (pond schedule logs)",
            paint("logs", dim()),
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

#[cfg(all(test, unix))]
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

#[cfg(windows)]
mod windows {
    //! Windows Task Scheduler backend, the peer of the launchd/systemd/cron
    //! `unix` module. The scheduled action is a small generated `.cmd` wrapper
    //! (`<state>/pond-sync.cmd`) that runs `pond sync -q --no-wait` and appends
    //! to `<state>/sync.log`. Running a wrapper file - rather than embedding a
    //! quoted command line in the `schtasks /TR` value - keeps pond out of
    //! Windows command-line quoting entirely, the same way the unix backend
    //! bakes paths into a plist/cron artifact.
    //!
    //! The task runs as the current user in their session (schtasks' default:
    //! no stored password, runs only when logged on), which matches launchd's
    //! per-user GUI-context agent. `-q --no-wait` mirrors the unix job exactly:
    //! never `--yes` (an unattended tick must not auto-enable adapters) and a
    //! tick that lands while another sync holds the per-store lock exits 0
    //! rather than queueing.

    use std::path::PathBuf;
    use std::process::Command;

    use anyhow::{Context, Result, bail};

    use super::{ScheduleCmd, ScheduleEvery};
    use pond::output::{dim, line, line_err, paint};

    const TASK_NAME: &str = "pond-sync";

    pub(crate) fn run(command: ScheduleCmd) -> Result<()> {
        match command {
            ScheduleCmd::Start { every } => start(every),
            ScheduleCmd::Stop => stop(),
            ScheduleCmd::Status => {
                let state = probe()?;
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

    pub(crate) fn status_line() -> String {
        status_snapshot().line
    }

    pub(crate) fn status_snapshot() -> super::ScheduleSnapshot {
        match probe() {
            Ok(state) => {
                let (active, backend, every) = match &state {
                    Active { backend, every } => (true, Some(*backend), *every),
                    Inactive => (false, None, None),
                };
                super::ScheduleSnapshot {
                    line: render_state(&state),
                    active,
                    backend,
                    every,
                }
            }
            Err(_) => super::ScheduleSnapshot {
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

    /// Register (or replace, via `/F`) the Task Scheduler job. Shared by
    /// `pond schedule start` and the `pond init` schedule section.
    pub(crate) fn start(every: ScheduleEvery) -> Result<()> {
        let bin = pond_bin();
        let log = log_path();
        let state = crate::syncstate::pond_state_dir();
        std::fs::create_dir_all(&state)
            .with_context(|| format!("failed to create {}", state.display()))?;

        // The task runs this wrapper (.cmd files are executed through cmd, so
        // the `>>` redirection works) instead of a quoted `schtasks /TR`
        // command line, keeping every path out of schtasks quoting.
        let wrapper = wrapper_path();
        let wrapper_body = format!(
            "@echo off\r\n\"{}\" sync -q --no-wait >> \"{}\" 2>&1\r\n",
            bin.display(),
            log.display(),
        );
        std::fs::write(&wrapper, wrapper_body)
            .with_context(|| format!("failed to write {}", wrapper.display()))?;

        let (schedule, modifier) = schtasks_cadence(every);
        let output = Command::new("schtasks")
            .args([
                "/Create",
                "/TN",
                TASK_NAME,
                "/TR",
                &format!("\"{}\"", wrapper.display()),
                "/SC",
                schedule,
                "/MO",
                &modifier,
                "/F",
            ])
            .output()
            .context("failed to run schtasks /Create")?;
        if !output.status.success() {
            bail!(
                "schtasks /Create failed: {}",
                decode_console(&output.stderr).trim()
            );
        }
        line(&format!(
            "schedule active (task-scheduler, every {})",
            every.label()
        ))?;
        Ok(())
    }

    fn stop() -> Result<()> {
        let existed = task_exists();
        if existed {
            let output = Command::new("schtasks")
                .args(["/Delete", "/TN", TASK_NAME, "/F"])
                .output()
                .context("failed to run schtasks /Delete")?;
            if !output.status.success() {
                bail!(
                    "schtasks /Delete failed: {}",
                    decode_console(&output.stderr).trim()
                );
            }
        }
        // Leave the wrapper + log in place on stop: removing the task is the
        // contract, and the log stays readable via `pond schedule logs`.
        if existed {
            line("schedule removed")?;
        } else {
            line("nothing was scheduled")?;
        }
        Ok(())
    }

    fn probe() -> Result<State> {
        if task_exists() {
            Ok(Active {
                backend: "task-scheduler",
                every: read_interval(),
            })
        } else {
            Ok(Inactive)
        }
    }

    fn logs(lines: usize) -> Result<()> {
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

    /// Does the pond task exist? `schtasks /Query /TN` exits 0 when present,
    /// non-zero when absent.
    fn task_exists() -> bool {
        Command::new("schtasks")
            .args(["/Query", "/TN", TASK_NAME])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    /// The registered task's cadence, recovered from its XML so `pond status`
    /// can name it. `None` when the task lacks a recognized interval (it is
    /// still reported active).
    fn read_interval() -> Option<ScheduleEvery> {
        let output = Command::new("schtasks")
            .args(["/Query", "/TN", TASK_NAME, "/XML", "ONE"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let xml = decode_console(&output.stdout);
        // MINUTE/HOURLY tasks carry `<Interval>PT<n>M</Interval>` /
        // `PT<n>H</Interval>` inside `<Repetition>`; a DAILY task carries
        // `<DaysInterval>1</DaysInterval>` and no `<Interval>`.
        let secs = if let Some(minutes) = between(&xml, "<Interval>PT", "M</Interval>") {
            minutes.parse::<u32>().ok().map(|m| m * 60)
        } else if let Some(hours) = between(&xml, "<Interval>PT", "H</Interval>") {
            hours.parse::<u32>().ok().map(|h| h * 3_600)
        } else if between(&xml, "<DaysInterval>", "</DaysInterval>").is_some() {
            Some(86_400)
        } else {
            None
        }?;
        ScheduleEvery::from_secs(secs)
    }

    /// The substring of `text` between the first `open` and the next `close`
    /// after it, or `None`.
    fn between<'a>(text: &'a str, open: &str, close: &str) -> Option<&'a str> {
        let start = text.find(open)? + open.len();
        let rest = &text[start..];
        let end = rest.find(close)?;
        Some(&rest[..end])
    }

    /// Map a cadence to the `schtasks /SC ... /MO ...` pair.
    fn schtasks_cadence(every: ScheduleEvery) -> (&'static str, String) {
        match every {
            ScheduleEvery::M5 => ("MINUTE", "5".to_owned()),
            ScheduleEvery::M15 => ("MINUTE", "15".to_owned()),
            ScheduleEvery::H1 => ("HOURLY", "1".to_owned()),
            ScheduleEvery::H6 => ("HOURLY", "6".to_owned()),
            ScheduleEvery::D1 => ("DAILY", "1".to_owned()),
        }
    }

    /// The binary path baked into the wrapper. Prefer `pond.exe` on PATH (a
    /// stable install location that survives upgrades); fall back to this
    /// running exe.
    fn pond_bin() -> PathBuf {
        crate::find_on_path("pond.exe")
            .or_else(|| crate::find_on_path("pond"))
            .unwrap_or_else(|| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("pond")))
    }

    /// `<state>/sync.log`, the same state dir manual syncs use.
    fn log_path() -> PathBuf {
        crate::syncstate::pond_state_dir().join("sync.log")
    }

    /// `<state>/pond-sync.cmd`, the generated task action.
    fn wrapper_path() -> PathBuf {
        crate::syncstate::pond_state_dir().join("pond-sync.cmd")
    }

    /// Decode process output that may be UTF-16 (schtasks `/XML` and some
    /// localized consoles) or UTF-8. A UTF-16LE BOM or interleaved NUL bytes
    /// select the wide decode; otherwise it is a lossy UTF-8 read.
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
}
