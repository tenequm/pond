//! `pond watch`: a long-lived daemon that backs up "running" sessions the
//! moment an agent writes to them, instead of waiting for the next `pond
//! schedule` tick.
//!
//! Two responsibilities, mirroring [`crate::schedule`]:
//!
//!   (a) [`run_watch`] is the daemon body the service executes (`pond watch
//!       run`): it resolves the enabled filesystem adapters' source roots (the
//!       SAME resolution `pond sync` uses - never a hardcoded path), runs one
//!       catch-up sync, then watches those roots and runs an incremental
//!       *no-embed* sync on every debounced batch of writes. Embeddings stay a
//!       manual `pond optimize` step; watch only backs the message up (null
//!       vector) within seconds.
//!
//!   (b) start/stop/status/logs register that daemon with the OS keepalive
//!       supervisor so it relaunches if it dies and starts at login. macOS uses
//!       a launchd agent with `KeepAlive=true` (NOT `StartInterval` - this is a
//!       resident process, not an interval job); Linux uses a systemd `--user`
//!       service with `Restart=always`. Linux without systemd bails with a
//!       clear message: cron can supervise an interval job but not a daemon, so
//!       (unlike `pond schedule`) there is no crontab fallback.
//!
//! A "running session" needs no status column: it is simply whatever source
//! file just received a write, which an fs event names exactly. pond's own
//! store lives under a different root, so its writes never feed back in.
//!
//! Bin-only module: the daemon and its service integration have no library
//! callers, same as [`crate::schedule`] and [`crate::syncstate`].

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use notify_debouncer_full::notify::RecursiveMode;
use notify_debouncer_full::{DebounceEventResult, new_debouncer};

use crate::{Config, OutputFormat, StorageUrl, SyncInvocation};

/// How long the watcher lets writes to a session settle before it syncs. One
/// appended message can reach the file as several `write(2)` calls (and the
/// agent may fsync then rename), so debouncing coalesces that burst into ONE
/// sync while still backing the message up within a couple of seconds. Kept a
/// `const` (not config) this pass: 1.5s is a safe default and adding a knob is
/// out of scope. `notify` derives its poll tick as 1/4 of this.
const DEBOUNCE: Duration = Duration::from_millis(1500);

#[derive(Debug, Subcommand)]
pub(crate) enum WatchCmd {
    /// Run the watcher in the foreground (this is what the keepalive service
    /// executes).
    ///
    /// Blocks forever: it watches the enabled adapters' source directories and
    /// runs an incremental no-embed sync whenever one changes. A single failed
    /// sync is logged and the daemon keeps running. Use `pond watch start` to
    /// have the OS keep this alive across logout and crashes.
    Run,
    /// Register the watcher with the OS keepalive supervisor (idempotent).
    ///
    /// launchd on macOS (`KeepAlive=true`), a systemd `--user` service on
    /// Linux (`Restart=always`). Re-running is a no-op.
    #[command(after_long_help = "Examples:
  pond watch start     keep sessions backed up as they're written
  pond watch status
  pond watch logs
  pond watch stop")]
    Start,
    /// Unregister the watcher.
    ///
    /// Succeeds (exit 0) when nothing was registered.
    Stop,
    /// Show whether the watcher is running.
    ///
    /// Exit 0 when active, 1 when not.
    Status,
    /// Show recent watcher output.
    Logs {
        /// Number of trailing log lines to print.
        #[arg(long, default_value_t = 50)]
        lines: usize,
    },
}

/// Dispatch the service subcommands. `Run` is handled directly by `main`
/// (it is async and needs the loaded config + storage handle), so it never
/// reaches here.
pub(crate) fn run(command: WatchCmd) -> Result<()> {
    match command {
        WatchCmd::Run => {
            bail!("internal: `pond watch run` is dispatched by main, not watch::run")
        }
        WatchCmd::Start => start(),
        WatchCmd::Stop => stop(),
        WatchCmd::Status => status(),
        WatchCmd::Logs { lines } => logs(lines),
    }
}

// ----- the daemon body (cross-platform) --------------------------------------

/// Resolve the directories to watch: every enabled filesystem adapter's
/// source roots (plural - an adapter may pool more than one directory, e.g.
/// two Claude Code homes), taken from the SAME resolution `pond sync` uses
/// ([`crate::resolve_sync_adapters`] -> [`pond::adapter::source_roots`]) so
/// the watched directories can never drift from the ones the importer reads.
/// API-backed adapters contribute no root (nothing on disk to watch) and rely
/// on the periodic scheduled sync.
fn resolve_watch_roots(loaded: &Config) -> Result<Vec<(String, PathBuf)>> {
    let adapters = crate::resolve_sync_adapters(loaded, None, None)?;
    let mut roots = Vec::new();
    for (name, blob) in adapters {
        let adapter_roots = pond::adapter::source_roots(&blob);
        if adapter_roots.is_empty() {
            tracing::debug!(
                adapter = %name,
                "watch: adapter has no local source root; left to the scheduled sync"
            );
            continue;
        }
        roots.extend(adapter_roots.into_iter().map(|root| (name.clone(), root)));
    }
    Ok(roots)
}

/// The `pond watch run` daemon body. Never returns under normal operation; a
/// single sync failure is logged and watching continues.
pub(crate) async fn run_watch(
    loaded: &Config,
    config_file: &Path,
    storage_path: Option<StorageUrl>,
) -> Result<()> {
    let roots = resolve_watch_roots(loaded)?;
    if roots.is_empty() {
        bail!(
            "pond watch: no enabled filesystem adapters to watch; enable one with \
             `pond adapters enable <name>` (or `pond init`). API-backed sources are \
             covered by `pond schedule`, not watch."
        );
    }
    // Operational lines go to stdout (`output`), which the service redirects
    // into watch.log - the same channel `serve`/`mcp` use for their startup
    // banners, and visible at the default log level (pond's tracing default is
    // WARN, so tracing::info would not reach the log). Errors below use
    // tracing::warn, which IS on at the default level.
    note(&format!(
        "watch: watching {} source {}",
        roots.len(),
        if roots.len() == 1 { "root" } else { "roots" },
    ));
    for (name, root) in &roots {
        note(&format!("watch:   {} -> {}", name, root.display()));
    }

    // Catch up anything that landed while the daemon was down before we start
    // reacting to live events (spec parity with the scheduled sync's role).
    note("watch: startup catch-up sync");
    sync_tick(loaded, config_file, storage_path.clone()).await;

    // fs events arrive on the debouncer's own thread; funnel each debounced
    // batch into the async runtime as a single "something changed" signal over
    // an unbounded channel (a non-blocking send that is safe to call from any
    // thread). The unit payload is deliberate: the batch contents don't matter,
    // only that SOME watched file changed - the sync re-derives exactly what to
    // ingest from the store's freshness oracle.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut debouncer = new_debouncer(DEBOUNCE, None, move |result: DebounceEventResult| {
        match result {
            Ok(events) if !events.is_empty() => {
                // A failed send only means run_watch has already returned (rx
                // dropped); there is nothing left to notify.
                let _ = tx.send(());
            }
            Ok(_) => {}
            Err(errors) => {
                // A transient watch error (e.g. an inotify queue overflow) must
                // never kill the daemon: log each and keep watching.
                for error in errors {
                    tracing::warn!(%error, "watch: filesystem event error");
                }
            }
        }
    })
    .context("failed to start the filesystem watcher")?;

    let mut watched = 0usize;
    for (name, root) in &roots {
        match debouncer.watch(root, RecursiveMode::Recursive) {
            Ok(()) => watched += 1,
            // A root that doesn't exist yet (a fresh install whose agent hasn't
            // written a first session) can't be watched, but must not abort the
            // whole daemon - the startup/scheduled sync still covers it once it
            // appears. Only a total failure to watch anything is fatal.
            Err(error) => tracing::warn!(
                adapter = %name,
                root = %root.display(),
                %error,
                "watch: could not watch source root (does it exist yet?); relying on the scheduled sync for it"
            ),
        }
    }
    if watched == 0 {
        bail!(
            "pond watch: none of the resolved source roots could be watched (do they exist \
             yet?); run `pond sync` once to create them, or rely on `pond schedule`"
        );
    }

    // The daemon loop: block until the next debounced batch, coalesce any that
    // queued behind it, then run one incremental no-embed sync. `debouncer`
    // stays owned by this frame (it has a Drop that stops the watch thread), so
    // the channel never closes and this loops until the process is killed.
    while rx.recv().await.is_some() {
        // Collapse the burst: drain every signal already queued so a flurry of
        // writes across several sessions costs one sync, not one per file.
        while rx.try_recv().is_ok() {}
        note("watch: change detected; running incremental no-embed sync");
        sync_tick(loaded, config_file, storage_path.clone()).await;
    }
    Ok(())
}

/// One operational line to stdout (which the keepalive service redirects into
/// watch.log). Best-effort: a broken stdout must not take the daemon down, so a
/// write error is dropped - the next `run_sync` will surface any real problem.
fn note(message: &str) {
    let _ = crate::output(message);
}

/// One incremental no-embed sync tick. Errors are logged and swallowed: a
/// single failed tick (an unreachable store, a transient FS race) must never
/// take the daemon down - the next event, or the scheduled sync, retries.
async fn sync_tick(loaded: &Config, config_file: &Path, storage_path: Option<StorageUrl>) {
    let invocation = SyncInvocation {
        adapter: None,
        path: None,
        // Watch is the cheap message-backup path: no model load, no GPU. The
        // backlog of null vectors is filled by the manual `pond optimize` step.
        embed: Some(false),
        verify: false,
        dry_run: false,
        // A tick that lands while the scheduled sync (or a prior tick) holds the
        // per-store lock skips cleanly instead of queueing: that other run
        // ingests the same messages anyway, and overlapping ticks would only
        // contend on the lock.
        no_wait: true,
        format: OutputFormat::Text,
    };
    if let Err(error) = crate::run_sync(loaded, config_file, storage_path, invocation).await {
        tracing::warn!(
            error = format!("{error:#}"),
            "watch: sync tick failed; continuing"
        );
    }
}

// ----- keepalive service registration ----------------------------------------

#[cfg(not(unix))]
fn start() -> Result<()> {
    bail!("pond watch is not supported on Windows yet; run `pond watch run` under a supervisor")
}

#[cfg(not(unix))]
fn stop() -> Result<()> {
    bail!("pond watch is not supported on Windows yet")
}

#[cfg(not(unix))]
fn status() -> Result<()> {
    bail!("pond watch is not supported on Windows yet")
}

#[cfg(not(unix))]
fn logs(_lines: usize) -> Result<()> {
    bail!("pond watch is not supported on Windows yet")
}

#[cfg(unix)]
use unix::{logs, start, status, stop};

#[cfg(unix)]
mod unix {
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    use anyhow::{Context, Result, bail};

    use pond::output::{dim, line, line_err, paint};

    /// launchd agent label and systemd unit name for the watch daemon. The
    /// `sh.pond.watch` label sits alongside `pond schedule`'s `sh.pond.sync`.
    const LAUNCHD_LABEL: &str = "sh.pond.watch";
    const SYSTEMD_UNIT: &str = "pond-watch.service";

    /// `$XDG_STATE_HOME/pond/watch.log`. The service redirects the daemon's
    /// stdout/stderr here (launchd) so `pond watch logs` can tail it; on systemd
    /// the output goes to the journal instead. Sibling of `schedule`'s sync.log.
    fn log_path() -> PathBuf {
        crate::syncstate::pond_state_dir().join("watch.log")
    }

    /// The binary path baked into the registration. Prefer the `pond` on PATH
    /// (a stable symlink that survives upgrades) over `current_exe()` (which on
    /// Homebrew resolves into a versioned Cellar path the next upgrade deletes).
    /// Mirrors `schedule::pond_bin`.
    fn pond_bin() -> PathBuf {
        crate::find_on_path("pond")
            .unwrap_or_else(|| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("pond")))
    }

    /// Numeric uid for the `gui/<uid>` launchd domain. Shelled out to `id -u`:
    /// pond denies `unsafe`, so `libc::getuid()` is off the table. (Duplicated
    /// from `schedule` rather than cross-wiring two private `mod unix` blocks.)
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

    /// The pinned `XDG_STATE_HOME` the registration embeds, checked for
    /// characters the plist / systemd templates can't escape. Same guard as
    /// `schedule::start`: the daemon never sources shell rc, so a shell-only
    /// `XDG_STATE_HOME` would split its lock/state dir from manual syncs.
    fn pinned_state() -> Result<PathBuf> {
        let state = crate::syncstate::state_root();
        let state_str = state.display().to_string();
        if state_str.contains(['<', '>', '&', '"', '%', '\n', '\r']) {
            bail!(
                "state dir {state_str:?} contains a character (< > & \" % or a newline) that \
                 cannot be embedded in a service registration; it resolves from \
                 XDG_STATE_HOME, falling back to $HOME/.local/state - set XDG_STATE_HOME \
                 to a simpler absolute path and re-run `pond watch start`"
            );
        }
        Ok(state)
    }

    pub(crate) fn start() -> Result<()> {
        let bin = pond_bin();
        let log = log_path();
        if let Some(parent) = log.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let state = pinned_state()?;
        match std::env::consts::OS {
            "macos" => start_launchd(&bin, &log, &state),
            "linux" => start_systemd(&bin, &state),
            other => bail!("pond watch is not supported on {other} yet"),
        }
    }

    pub(crate) fn stop() -> Result<()> {
        let removed = match std::env::consts::OS {
            "macos" => stop_launchd()?,
            "linux" => stop_systemd()?,
            other => bail!("pond watch is not supported on {other} yet"),
        };
        if removed {
            line("watch stopped")?;
        } else {
            line("watch was not running")?;
        }
        Ok(())
    }

    pub(crate) fn status() -> Result<()> {
        let active = match std::env::consts::OS {
            "macos" => launchd_registered(&current_uid()?),
            "linux" => systemd_enabled(),
            _ => false,
        };
        if active {
            line(&format!("{}  active", paint("watch", dim())))?;
            line(&format!(
                "{}      {}  (pond watch logs)",
                paint("logs", dim()),
                logs_hint()?,
            ))?;
            Ok(())
        } else {
            line(&format!(
                "{}  not running - run `pond watch start` to back up sessions as they're written",
                paint("watch", dim()),
            ))?;
            std::process::exit(1);
        }
    }

    /// Where `pond watch logs` reads from, for the status/start hint: the
    /// journal on systemd, otherwise the redirected log file.
    fn logs_hint() -> Result<String> {
        if std::env::consts::OS == "linux" && systemd_enabled() {
            return Ok(format!("journalctl --user -u {SYSTEMD_UNIT}"));
        }
        Ok(crate::config::display(&crate::config::url_for_path(
            log_path(),
        )?))
    }

    pub(crate) fn logs(lines: usize) -> Result<()> {
        // systemd routes unit output to the journal; launchd writes the log file.
        if std::env::consts::OS == "linux" && systemd_enabled() {
            let status = Command::new("journalctl")
                .args([
                    "--user",
                    "-u",
                    SYSTEMD_UNIT,
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
                line("(no log yet - the watcher hasn't written anything)")?;
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

    // ----- launchd (macOS) -------------------------------------------------

    fn plist_path() -> Result<PathBuf> {
        let home = std::env::var_os("HOME").context("HOME is not set")?;
        Ok(PathBuf::from(home)
            .join("Library/LaunchAgents")
            .join(format!("{LAUNCHD_LABEL}.plist")))
    }

    /// A resident-daemon plist: `RunAtLoad` starts it at login and `KeepAlive`
    /// relaunches it if it dies. Deliberately NO `StartInterval` - this is a
    /// long-running watcher, not `pond schedule`'s interval job.
    fn plist_body(bin: &Path, log: &Path, state: &Path) -> String {
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
		<string>watch</string>
		<string>run</string>
	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>XDG_STATE_HOME</key>
		<string>{state}</string>
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

    fn start_launchd(bin: &Path, log: &Path, state: &Path) -> Result<()> {
        let plist = plist_path()?;
        let body = plist_body(bin, log, state);
        let uid = current_uid()?;
        let unchanged = std::fs::read_to_string(&plist)
            .map(|existing| existing == body)
            .unwrap_or(false);
        if unchanged && launchd_registered(&uid) {
            line("watch already running")?;
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
        line(&format!("{}  active", paint("watch", dim())))?;
        line(&format!(
            "{}      {}  (pond watch logs)",
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
        // Remove unconditionally: a missing plist means nothing to clean up, not
        // an error (stop is documented to succeed when nothing is registered).
        let had_plist = match std::fs::remove_file(&plist) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", plist.display()));
            }
        };
        Ok(was_registered || had_plist)
    }

    // ----- systemd user service (Linux) ------------------------------------

    fn systemd_user_available() -> bool {
        Command::new("systemctl")
            .args(["--user", "list-units"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn systemd_enabled() -> bool {
        Command::new("systemctl")
            .args(["--user", "is-enabled", SYSTEMD_UNIT])
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

    /// A resident service: `Type=simple` + `Restart=always` is the systemd
    /// analogue of launchd's `KeepAlive`. No timer unit - this is not an
    /// interval job.
    fn systemd_service_body(bin: &Path, state: &Path) -> String {
        format!(
            "# created and maintained by pond; edits may be replaced\n\
             [Unit]\n\
             Description=pond watch (continuous session backup)\n\n\
             [Service]\n\
             Type=simple\n\
             Restart=always\n\
             RestartSec=2\n\
             Environment=\"XDG_STATE_HOME={}\"\n\
             ExecStart={} watch run\n\n\
             [Install]\n\
             WantedBy=default.target\n",
            state.display(),
            bin.display(),
        )
    }

    fn start_systemd(bin: &Path, state: &Path) -> Result<()> {
        if !systemd_user_available() {
            // Unlike `pond schedule`, there is no cron fallback: cron can fire an
            // interval job but cannot supervise a long-running daemon. Bail
            // honestly instead of writing a broken registration.
            bail!(
                "pond watch needs systemd --user to keep the daemon alive; cron cannot \
                 supervise a long-running process. Enable a systemd user session, or run \
                 `pond watch run` yourself under a supervisor (a tmux/screen session, a \
                 container entrypoint, a login-shell service manager)."
            );
        }
        let dir = systemd_unit_dir();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let service_path = dir.join(SYSTEMD_UNIT);
        let service = systemd_service_body(bin, state);
        let unchanged = std::fs::read_to_string(&service_path)
            .map(|existing| existing == service)
            .unwrap_or(false);
        if unchanged && systemd_enabled() {
            line("watch already running")?;
            return Ok(());
        }
        std::fs::write(&service_path, service)
            .with_context(|| format!("failed to write {}", service_path.display()))?;
        for args in [
            vec!["--user", "daemon-reload"],
            vec!["--user", "enable", "--now", SYSTEMD_UNIT],
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
        line(&format!("{}  active", paint("watch", dim())))?;
        line(&format!(
            "{}      journalctl --user -u {SYSTEMD_UNIT}  (pond watch logs)",
            paint("logs", dim()),
        ))?;
        Ok(())
    }

    fn stop_systemd() -> Result<bool> {
        let dir = systemd_unit_dir();
        let service_path = dir.join(SYSTEMD_UNIT);
        let was_enabled = systemd_enabled();
        if was_enabled {
            let _ = Command::new("systemctl")
                .args(["--user", "disable", "--now", SYSTEMD_UNIT])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        // Remove unconditionally; a missing unit is not an error (disable --now
        // or a racing stop may have already taken it).
        let removed_unit = match std::fs::remove_file(&service_path) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to remove {}", service_path.display()));
            }
        };
        if removed_unit {
            let _ = Command::new("systemctl")
                .args(["--user", "daemon-reload"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        Ok(was_enabled || removed_unit)
    }
}
