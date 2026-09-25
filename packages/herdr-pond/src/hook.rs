//! Sync-on-idle: the millisecond event hook and its detached per-adapter
//! worker (plan 5.5).
//!
//! No idle event may be dropped, so the worker runs trailing-edge: the hook
//! creates `pending.<adapter>`; the worker deletes it just before each
//! `pond sync`, and syncs again whenever it reappears. A hook that finds the
//! worker's flock held relies on that, and the worker re-checks `pending`
//! after releasing the flock to close the window where it was exiting.

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::bail;
use serde::Deserialize;

use crate::config::{Config, log_line, open_log, try_lock};
use crate::herdr;

const SYNC_LOG: &str = "sync.log";
/// Absorbs an event burst (min observed gap 302ms) into one sync.
const COALESCE: Duration = Duration::from_secs(2);

/// herdr agent name -> pond adapter name.
const ADAPTERS: &[(&str, &str)] = &[
    ("claude", "claude-code"),
    ("codex", "codex-cli"),
    ("pi", "pi-coding-agent"),
    ("omp", "oh-my-pi"),
    ("opencode", "opencode"),
    ("grok", "grok-build"),
    ("hermes", "hermes"),
    ("letta", "letta-code"),
    ("agy", "agy"),
];

pub(crate) fn run(args: &[String]) -> anyhow::Result<()> {
    match args {
        [] => {
            on_event();
            Ok(())
        }
        [flag, adapter] if flag == "--worker" => {
            herdr::detach();
            worker(adapter)
        }
        _ => bail!("usage: herdr-pond hook [--worker <adapter>]"),
    }
}

/// Always exits 0: herdr keeps hook stderr only in an in-memory ring, so
/// failures go to `sync.log`.
fn on_event() {
    let Ok(state_dir) = herdr::state_dir() else {
        return;
    };
    let log = state_dir.join(SYNC_LOG);
    let event = std::env::var("HERDR_PLUGIN_EVENT_JSON").unwrap_or_default();
    let result = herdr::config_dir().and_then(|config_dir| {
        handle_event(&event, &state_dir, &config_dir, |adapter| {
            let mut command = Command::new(std::env::current_exe()?);
            command.args(["hook", "--worker", adapter]);
            herdr::spawn_detached(command, &log)
        })
    });
    if let Err(error) = result {
        log_line(&log, &format!("hook: {error:#}"));
    }
}

/// The adapter to sync when this event is an agent going idle (`done` is
/// idle-but-unseen).
fn idle_adapter(event_json: &str) -> Option<&'static str> {
    #[derive(Deserialize)]
    struct Event {
        data: Data,
    }
    #[derive(Deserialize)]
    struct Data {
        agent_status: Option<String>,
        agent: Option<String>,
    }
    let data = serde_json::from_str::<Event>(event_json).ok()?.data;
    if !matches!(data.agent_status.as_deref(), Some("idle" | "done")) {
        return None;
    }
    let agent = data.agent?;
    ADAPTERS
        .iter()
        .find(|(name, _)| *name == agent)
        .map(|(_, adapter)| *adapter)
}

fn pending_path(state_dir: &Path, adapter: &str) -> PathBuf {
    state_dir.join(format!("pending.{adapter}"))
}

fn worker_lock(state_dir: &Path, adapter: &str) -> PathBuf {
    state_dir.join(format!("worker.{adapter}.lock"))
}

fn handle_event(
    event_json: &str,
    state_dir: &Path,
    config_dir: &Path,
    spawn_worker: impl FnOnce(&str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let Some(adapter) = idle_adapter(event_json) else {
        return Ok(());
    };
    if !Config::load(config_dir, &state_dir.join(SYNC_LOG)).sync_on_idle {
        return Ok(());
    }
    fs::create_dir_all(state_dir)?;
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(pending_path(state_dir, adapter))?;
    let worker_running = try_lock(&worker_lock(state_dir, adapter))?.is_none();
    if worker_running {
        return Ok(());
    }
    spawn_worker(adapter)
}

fn worker(adapter: &str) -> anyhow::Result<()> {
    let state_dir = herdr::state_dir()?;
    let config_dir = herdr::config_dir()?;
    let log = state_dir.join(SYNC_LOG);
    let config = Config::load(&config_dir, &log);
    let Some(pond) = herdr::resolve_pond_or_toast(&config, &config_dir, &state_dir, &log) else {
        return Ok(());
    };
    work(adapter, &state_dir, &pond, COALESCE)
}

/// Syncs `adapter` until no pending stamp is left. Losing the flock to
/// another worker means that worker covers the stamp.
fn work(adapter: &str, state_dir: &Path, pond: &Path, coalesce: Duration) -> anyhow::Result<()> {
    let pending = pending_path(state_dir, adapter);
    let log = state_dir.join(SYNC_LOG);
    loop {
        let Some(lock) = try_lock(&worker_lock(state_dir, adapter))? else {
            return Ok(());
        };
        while pending.exists() {
            std::thread::sleep(coalesce);
            if let Err(error) = fs::remove_file(&pending)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(error.into());
            }
            sync(adapter, pond, &log);
        }
        drop(lock);
        if !pending.exists() {
            return Ok(());
        }
    }
}

/// Without `--no-wait`: a busy store lock makes this sync wait its turn
/// instead of exiting "skipped" and silently dropping the idle event.
fn sync(adapter: &str, pond: &Path, log: &Path) {
    let started = Instant::now();
    let status = open_log(log).and_then(|out| {
        let err = out.try_clone()?;
        Command::new(pond)
            .args(["sync", adapter, "-q"])
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .status()
    });
    let outcome = match status {
        Ok(status) => status.to_string(),
        Err(error) => format!("cannot run {}: {error}", pond.display()),
    };
    log_line(
        log,
        &format!(
            "sync {adapter}: {outcome} after {:.1}s",
            started.elapsed().as_secs_f64()
        ),
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::io::Read;
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;

    use super::*;
    use crate::fake_pond::{Sandbox, write_script};

    const TEST_COALESCE: Duration = Duration::from_millis(50);
    const ROLE: &str = "HERDR_POND_TEST_ROLE";

    fn idle(agent: &str) -> String {
        format!(
            r#"{{"event":"pane_agent_status_changed","data":{{"type":"pane_agent_status_changed","pane_id":"wD:p1","workspace_id":"wD","agent_status":"idle","agent":"{agent}"}}}}"#
        )
    }

    /// A fake `pond sync` that logs start/end to `events`, holds while
    /// `store.lock` exists (pond's own per-host lock wait) and then works for
    /// `seconds`.
    fn fake_pond(sandbox: &Sandbox, seconds: &str) -> PathBuf {
        write_script(
            &sandbox.path("bin/pond"),
            &format!(
                r#"printf '%s\n' "$*" >> '{calls}'
echo "start $2" >> '{events}'
while [ -e '{lock}' ]; do sleep 0.02; done
sleep {seconds}
echo "end $2" >> '{events}'"#,
                calls = sandbox.path("calls").display(),
                events = sandbox.path("events").display(),
                lock = sandbox.path("store.lock").display(),
            ),
        )
    }

    fn lines(path: &Path) -> Vec<String> {
        fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn wait_for(what: &str, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Runs the hook leg in-process with workers on threads.
    struct Harness {
        sandbox: Sandbox,
        pond: PathBuf,
        workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    }

    impl Harness {
        fn new(sync_seconds: &str) -> Self {
            let sandbox = Sandbox::new();
            let pond = fake_pond(&sandbox, sync_seconds);
            Self {
                sandbox,
                pond,
                workers: Arc::default(),
            }
        }

        fn trigger(&self, agent: &str) {
            let state_dir = self.sandbox.state_dir();
            let pond = self.pond.clone();
            let workers = Arc::clone(&self.workers);
            handle_event(
                &idle(agent),
                &state_dir.clone(),
                &self.sandbox.config_dir(),
                move |adapter| {
                    let adapter = adapter.to_owned();
                    let worker = std::thread::spawn(move || {
                        work(&adapter, &state_dir, &pond, TEST_COALESCE).unwrap();
                    });
                    workers.lock().unwrap().push(worker);
                    Ok(())
                },
            )
            .unwrap();
        }

        fn join(&self) {
            while let Some(worker) = self.workers.lock().unwrap().pop() {
                worker.join().unwrap();
            }
        }

        fn events(&self) -> Vec<String> {
            lines(&self.sandbox.path("events"))
        }

        fn has_event(&self, event: &str) -> bool {
            self.events().iter().any(|line| line == event)
        }
    }

    #[test]
    fn only_idle_events_of_known_agents_sync() {
        assert_eq!(idle_adapter(&idle("claude")), Some("claude-code"));
        assert_eq!(idle_adapter(&idle("codex")), Some("codex-cli"));
        let done = idle("pi").replace("\"idle\"", "\"done\"");
        assert_eq!(idle_adapter(&done), Some("pi-coding-agent"));
        let working = idle("claude").replace("\"idle\"", "\"working\"");
        assert_eq!(idle_adapter(&working), None);
        assert_eq!(idle_adapter(&idle("vim")), None);
        let no_agent = r#"{"event":"pane_agent_status_changed","data":{"agent_status":"idle"}}"#;
        assert_eq!(idle_adapter(no_agent), None);
        assert_eq!(idle_adapter(""), None);
        assert_eq!(idle_adapter("{"), None);
    }

    #[test]
    fn disabled_config_does_nothing() {
        let sandbox = Sandbox::new();
        sandbox.write_config("sync_on_idle = false\n");
        handle_event(
            &idle("claude"),
            &sandbox.state_dir(),
            &sandbox.config_dir(),
            |_| panic!("spawned a worker while disabled"),
        )
        .unwrap();
        assert!(!pending_path(&sandbox.state_dir(), "claude-code").exists());
    }

    #[test]
    fn a_held_worker_lock_leaves_only_the_stamp() {
        let sandbox = Sandbox::new();
        let state_dir = sandbox.state_dir();
        let _held = try_lock(&worker_lock(&state_dir, "claude-code"))
            .unwrap()
            .unwrap();
        handle_event(&idle("claude"), &state_dir, &sandbox.config_dir(), |_| {
            panic!("spawned a second worker")
        })
        .unwrap();
        assert!(pending_path(&state_dir, "claude-code").exists());
    }

    #[test]
    fn idles_during_a_sync_coalesce_into_one_more() {
        let harness = Harness::new("0.4");
        harness.trigger("claude");
        wait_for("the first sync", || harness.has_event("start claude-code"));
        harness.trigger("claude");
        harness.trigger("claude");
        harness.join();
        assert_eq!(
            harness.events(),
            [
                "start claude-code",
                "end claude-code",
                "start claude-code",
                "end claude-code"
            ]
        );
        let log = fs::read_to_string(harness.sandbox.state_dir().join(SYNC_LOG)).unwrap();
        assert_eq!(log.matches("sync claude-code: exit status: 0").count(), 2);
    }

    #[test]
    fn two_adapters_sync_concurrently() {
        let harness = Harness::new("0.4");
        harness.trigger("claude");
        harness.trigger("codex");
        harness.join();
        let events = harness.events();
        assert_eq!(events.len(), 4, "{events:?}");
        for adapter in ["claude-code", "codex-cli"] {
            assert_eq!(
                events.iter().filter(|e| e.ends_with(adapter)).count(),
                2,
                "{events:?}"
            );
        }
        assert!(events[1].starts_with("start"), "not concurrent: {events:?}");
    }

    #[test]
    fn idle_during_a_held_store_lock_waits_and_is_not_dropped() {
        let harness = Harness::new("0.05");
        fs::write(harness.sandbox.path("store.lock"), "").unwrap();
        harness.trigger("claude");
        wait_for("the blocked sync", || {
            harness.has_event("start claude-code")
        });
        std::thread::sleep(Duration::from_millis(200));
        assert!(!harness.has_event("end claude-code"));
        harness.trigger("claude");
        fs::remove_file(harness.sandbox.path("store.lock")).unwrap();
        harness.join();
        assert_eq!(harness.events().len(), 4, "{:?}", harness.events());
        assert!(
            lines(&harness.sandbox.path("calls"))
                .iter()
                .all(|call| call == "sync claude-code -q"),
            "{:?}",
            lines(&harness.sandbox.path("calls"))
        );
    }

    #[test]
    fn a_stamp_left_after_release_is_picked_up() {
        let harness = Harness::new("0");
        let state_dir = harness.sandbox.state_dir();
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(pending_path(&state_dir, "codex-cli"), "").unwrap();
        work("codex-cli", &state_dir, &harness.pond, TEST_COALESCE).unwrap();
        assert_eq!(harness.events(), ["start codex-cli", "end codex-cli"]);
        assert!(!pending_path(&state_dir, "codex-cli").exists());
        assert!(
            try_lock(&worker_lock(&state_dir, "codex-cli"))
                .unwrap()
                .is_some()
        );
    }

    fn self_exec(role: &str, sandbox: &Sandbox) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "hook::tests::self_exec_role",
                "--test-threads=1",
                "-q",
            ])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env(ROLE, role)
            .env("HERDR_PLUGIN_STATE_DIR", sandbox.state_dir())
            .env("HERDR_PLUGIN_CONFIG_DIR", sandbox.config_dir())
            .env("HERDR_BIN_PATH", sandbox.path("bin/no-herdr"))
            .env("HERDR_PLUGIN_EVENT_JSON", idle("claude"));
        command
    }

    /// Not a test on its own: the process entry for
    /// [`hook_exits_and_closes_its_pipes_while_the_sync_runs`], which re-runs
    /// this test binary as the hook and as its detached worker.
    #[test]
    fn self_exec_role() {
        let Ok(role) = std::env::var(ROLE) else {
            return;
        };
        let state_dir = herdr::state_dir().unwrap();
        let config_dir = herdr::config_dir().unwrap();
        if role == "worker" {
            herdr::detach();
            worker("claude-code").unwrap();
            return;
        }
        let event = std::env::var("HERDR_PLUGIN_EVENT_JSON").unwrap();
        let log = state_dir.join(SYNC_LOG);
        handle_event(&event, &state_dir, &config_dir, |_| {
            let mut command = Command::new(std::env::current_exe()?);
            command
                .args([
                    "--exact",
                    "hook::tests::self_exec_role",
                    "--test-threads=1",
                    "-q",
                ])
                .env(ROLE, "worker");
            herdr::spawn_detached(command, &log)
        })
        .unwrap();
    }

    #[test]
    fn hook_exits_and_closes_its_pipes_while_the_sync_runs() {
        let sandbox = Sandbox::new();
        let pond = fake_pond(&sandbox, "1");
        sandbox.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        let started = Instant::now();
        let mut hook = self_exec("hook", &sandbox)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = hook.stdout.take().unwrap();
        let mut stderr = hook.stderr.take().unwrap();
        let stderr_reader = std::thread::spawn(move || {
            let mut text = String::new();
            stderr.read_to_string(&mut text).unwrap();
            text
        });
        let mut text = String::new();
        stdout.read_to_string(&mut text).unwrap();
        let stderr_text = stderr_reader.join().unwrap();
        let eof_after = started.elapsed();
        assert!(hook.wait().unwrap().success(), "{text}{stderr_text}");

        let events = sandbox.path("events");
        assert!(
            !lines(&events).contains(&"end claude-code".to_owned()),
            "pipes stayed open for the whole sync ({eof_after:?})"
        );
        wait_for("the detached sync to finish", || {
            lines(&events).contains(&"end claude-code".to_owned())
        });
        assert_eq!(lines(&sandbox.path("calls")), ["sync claude-code -q"]);
    }
}
