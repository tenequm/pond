//! Every herdr CLI call (`pane list`, `agent focus`, `plugin pane open|focus`,
//! `notification show`) and the plugin runtime env (plan 5.2, 5.4).

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::config::{Config, log_line, log_stdio};
use crate::types::LiveAgent;

const PLUGIN_ID: &str = "pond";
const DESK_ENTRYPOINT: &str = "desk";
/// The manifest pane title, which herdr uses as the pane label.
const DESK_LABEL: &str = "pond desk";
/// herdr answers in milliseconds; a hung CLI must not hang a hook or the desk.
const CALL_DEADLINE: Duration = Duration::from_secs(3);
const CALL_POLL: Duration = Duration::from_millis(5);

/// A plugin-runtime path herdr sets for every plugin process.
fn plugin_env(var: &str) -> anyhow::Result<PathBuf> {
    std::env::var_os(var)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .with_context(|| format!("{var} is unset - herdr-pond runs only as a herdr plugin"))
}

pub(crate) fn state_dir() -> anyhow::Result<PathBuf> {
    plugin_env("HERDR_PLUGIN_STATE_DIR")
}

pub(crate) fn config_dir() -> anyhow::Result<PathBuf> {
    plugin_env("HERDR_PLUGIN_CONFIG_DIR")
}

pub(crate) fn socket_path() -> anyhow::Result<PathBuf> {
    plugin_env("HERDR_SOCKET_PATH")
}

/// The desk's project: the underlying pane's cwd, else the workspace's.
pub(crate) fn context_project() -> Option<String> {
    project_from_context(&std::env::var("HERDR_PLUGIN_CONTEXT_JSON").ok()?)
}

fn project_from_context(json: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct Context {
        focused_pane_cwd: Option<String>,
        workspace_cwd: Option<String>,
    }
    let context: Context = serde_json::from_str(json).ok()?;
    [context.focused_pane_cwd, context.workspace_cwd]
        .into_iter()
        .flatten()
        .find(|cwd| !cwd.is_empty())
}

/// Spawns `command` so it holds no herdr command slot: herdr reads a plugin
/// command's stdout/stderr to EOF before releasing its slot, so every stdio
/// end goes to /dev/null or `log`. The child calls [`detach`] itself - a
/// pre-exec `setsid` would need `unsafe`.
pub(crate) fn spawn_detached(mut command: Command, log: &Path) -> anyhow::Result<()> {
    log_stdio(&mut command, log)
        .with_context(|| format!("opening {}", log.display()))?
        .spawn()
        .with_context(|| format!("spawning {:?}", command.get_program()))?;
    Ok(())
}

/// Leaves herdr's session, so a detached leg outlives the hook that spawned it.
pub(crate) fn detach() {
    let _ = nix::unistd::setsid();
}

/// Resolves `pond` for a headless leg. A failure is logged and toasted once;
/// the toast re-arms after the next successful resolution.
pub(crate) fn resolve_pond_or_toast(
    config_dir: &Path,
    state_dir: &Path,
    log: &Path,
) -> Option<PathBuf> {
    let marker = state_dir.join("pond-missing.toasted");
    match Config::pond(config_dir, log) {
        Ok(pond) => {
            let _ = fs::remove_file(&marker);
            Some(pond)
        }
        Err(error) => {
            log_line(log, &format!("{error:#}"));
            if !marker.exists() && fs::write(&marker, b"").is_ok() {
                let _ = Herdr::from_env().notify("pond: cannot find pond", &format!("{error:#}"));
            }
            None
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Pane {
    pub pane_id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub agent_session: Option<AgentSession>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AgentSession {
    pub value: String,
}

/// Only panes whose agent reported a session identity can be matched to a
/// pond session; the rest are not live rows.
pub(crate) fn live_agents(panes: Vec<Pane>) -> Vec<LiveAgent> {
    panes
        .into_iter()
        .filter_map(|pane| {
            Some(LiveAgent {
                session: pane.agent_session?.value,
                pane_id: pane.pane_id,
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct Herdr {
    bin: PathBuf,
    deadline: Duration,
}

impl Herdr {
    pub(crate) fn from_env() -> Self {
        Self::new(plugin_env("HERDR_BIN_PATH").unwrap_or_else(|_| PathBuf::from("herdr")))
    }

    pub(crate) fn new(bin: PathBuf) -> Self {
        Self {
            bin,
            deadline: CALL_DEADLINE,
        }
    }

    /// Runs one CLI call, killed past the deadline, and returns its `result`
    /// object. herdr reports errors on stderr with a nonzero exit, never in
    /// the stdout JSON.
    fn call(&self, args: &[&str]) -> anyhow::Result<serde_json::Value> {
        let command = args.iter().take(3).copied().collect::<Vec<_>>().join(" ");
        let mut child = Command::new(&self.bin)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("running {}", self.bin.display()))?;
        let stdout = drain(child.stdout.take());
        let stderr = drain(child.stderr.take());
        let started = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if started.elapsed() > self.deadline {
                let _ = child.kill();
                let _ = child.wait();
                bail!("herdr {command} timed out after {:?}", self.deadline);
            }
            std::thread::sleep(CALL_POLL);
        };
        let stdout = stdout.join().unwrap_or_default();
        if !status.success() {
            let stderr = stderr.join().unwrap_or_default();
            bail!(
                "herdr {command} failed ({status}): {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        let mut response: serde_json::Value = serde_json::from_slice(&stdout)
            .with_context(|| format!("herdr {command} printed no JSON response"))?;
        Ok(response["result"].take())
    }

    pub(crate) fn pane_list(&self, workspace: Option<&str>) -> anyhow::Result<Vec<Pane>> {
        let mut args = vec!["pane", "list"];
        if let Some(workspace) = workspace {
            args.extend(["--workspace", workspace]);
        }
        let mut result = self.call(&args)?;
        serde_json::from_value(result["panes"].take()).context("herdr pane list: unexpected panes")
    }

    pub(crate) fn agent_focus(&self, pane_id: &str) -> anyhow::Result<()> {
        self.call(&["agent", "focus", pane_id]).map(drop)
    }

    pub(crate) fn notify(&self, title: &str, body: &str) -> anyhow::Result<()> {
        self.call(&["notification", "show", title, "--body", body])
            .map(drop)
    }

    /// Focuses this workspace's open desk, else opens one. Any failure to find
    /// or focus an existing desk degrades to opening a new one.
    pub(crate) fn open_desk(&self, workspace: Option<&str>) -> anyhow::Result<()> {
        let existing = self.pane_list(workspace).ok().and_then(|panes| {
            panes
                .into_iter()
                .find(|pane| pane.label.as_deref() == Some(DESK_LABEL))
        });
        if let Some(pane) = existing
            && self
                .call(&["plugin", "pane", "focus", &pane.pane_id])
                .is_ok()
        {
            return Ok(());
        }
        self.call(&[
            "plugin",
            "pane",
            "open",
            "--plugin",
            PLUGIN_ID,
            "--entrypoint",
            DESK_ENTRYPOINT,
            "--focus",
        ])
        .map(drop)
    }
}

/// Reads a child's pipe to EOF on its own thread, so a large reply cannot
/// fill the pipe and stall the child before it exits.
fn drain(pipe: Option<impl Read + Send + 'static>) -> JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        if let Some(mut pipe) = pipe {
            let _ = pipe.read_to_end(&mut bytes);
        }
        bytes
    })
}

/// The `open` action: herdr sets `HERDR_WORKSPACE_ID` from the invocation
/// context, which scopes the dedupe to the focused workspace.
pub(crate) fn open_desk() -> anyhow::Result<()> {
    let workspace = std::env::var("HERDR_WORKSPACE_ID").ok();
    Herdr::from_env().open_desk(workspace.as_deref())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::fake_pond::{Sandbox, write_script};

    const PANES: &str = r#"{"id":"cli:pane:list","result":{"type":"pane_list","panes":[
        {"pane_id":"wD:pS","agent":"claude","agent_status":"idle","agent_session":{"agent":"claude","kind":"id","source":"herdr:claude","value":"0a1d69bd"}},
        {"pane_id":"wD:pT","agent":"codex","agent_status":"working"},
        {"pane_id":"wD:pU","label":"pond desk","agent_status":"unknown"}
    ]}}"#;

    /// A fake herdr that records its argv and answers `pane list` from
    /// `panes.json`; `plugin pane focus` exits with `focus_exit`.
    fn fake_herdr(sandbox: &Sandbox, panes: &str, focus_exit: i32) -> Herdr {
        fs::write(sandbox.path("panes.json"), panes).unwrap();
        let bin = write_script(
            &sandbox.path("bin/herdr"),
            &format!(
                r#"printf '%s\n' "$*" >> '{calls}'
case "$1 $2 $3" in
  "pane list"*) cat '{panes}' ;;
  "plugin pane focus") [ {focus_exit} = 0 ] && {{ echo '{{"result":{{}}}}'; exit 0; }}
    echo '{{"error":{{"code":"not_found"}}}}' >&2; exit {focus_exit} ;;
  *) echo '{{"id":"cli:x","result":{{}}}}' ;;
esac"#,
                calls = sandbox.path("calls").display(),
                panes = sandbox.path("panes.json").display(),
            ),
        );
        Herdr::new(bin)
    }

    #[test]
    fn context_prefers_the_focused_pane_cwd() {
        let both = r#"{"workspace_cwd":"/w","focused_pane_cwd":"/p","focused_pane_id":"x"}"#;
        assert_eq!(project_from_context(both).as_deref(), Some("/p"));
        let workspace_only = r#"{"workspace_cwd":"/w","focused_pane_cwd":""}"#;
        assert_eq!(project_from_context(workspace_only).as_deref(), Some("/w"));
        assert_eq!(project_from_context("{}"), None);
        assert_eq!(project_from_context("not json"), None);
    }

    #[test]
    fn live_agents_are_panes_with_a_session() {
        let sandbox = Sandbox::new();
        let herdr = fake_herdr(&sandbox, PANES, 0);
        let live = live_agents(herdr.pane_list(None).unwrap());
        assert_eq!(
            live,
            vec![LiveAgent {
                pane_id: "wD:pS".to_owned(),
                session: "0a1d69bd".to_owned(),
            }]
        );
    }

    #[test]
    fn open_focuses_an_existing_desk() {
        let sandbox = Sandbox::new();
        fake_herdr(&sandbox, PANES, 0)
            .open_desk(Some("wD"))
            .unwrap();
        assert_eq!(
            sandbox.lines("calls"),
            ["pane list --workspace wD", "plugin pane focus wD:pU"]
        );
    }

    #[test]
    fn open_degrades_to_opening_a_new_desk() {
        let open = "plugin pane open --plugin pond --entrypoint desk --focus";
        for (panes, focus_exit, expected) in [
            (PANES, 1, vec!["pane list", "plugin pane focus wD:pU", open]),
            ("not json", 0, vec!["pane list", open]),
            (
                r#"{"result":{"panes":[{"pane_id":"a","label":"other"}]}}"#,
                0,
                vec!["pane list", open],
            ),
        ] {
            let sandbox = Sandbox::new();
            fake_herdr(&sandbox, panes, focus_exit)
                .open_desk(None)
                .unwrap();
            assert_eq!(sandbox.lines("calls"), expected);
        }
    }

    #[test]
    fn a_hung_call_is_killed_at_the_deadline() {
        let sandbox = Sandbox::new();
        let bin = write_script(&sandbox.path("bin/herdr"), "exec sleep 30");
        let herdr = Herdr {
            deadline: Duration::from_millis(200),
            ..Herdr::new(bin)
        };
        let started = Instant::now();
        let error = herdr.pane_list(None).unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn a_large_reply_is_read_whole() {
        let sandbox = Sandbox::new();
        let panes: Vec<String> = (0..2000)
            .map(|i| format!(r#"{{"pane_id":"p{i}","label":"{}"}}"#, "x".repeat(64)))
            .collect();
        fs::write(
            sandbox.path("panes.json"),
            format!(r#"{{"result":{{"panes":[{}]}}}}"#, panes.join(",")),
        )
        .unwrap();
        let bin = write_script(
            &sandbox.path("bin/herdr"),
            &format!("cat '{}'", sandbox.path("panes.json").display()),
        );
        assert_eq!(Herdr::new(bin).pane_list(None).unwrap().len(), 2000);
    }

    #[test]
    fn a_failed_call_carries_herdrs_stderr() {
        let sandbox = Sandbox::new();
        let bin = write_script(
            &sandbox.path("bin/herdr"),
            "echo 'agent not found: wD:p9' >&2; exit 2",
        );
        let error = Herdr::new(bin).agent_focus("wD:p9").unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("herdr agent focus wD:p9 failed")
                && message.contains("agent not found"),
            "{message}"
        );
    }
}
