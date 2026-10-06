//! The desk's write legs: resume a stored session into its client's own
//! directory and start the client on it in a new tab, and park a live pane.
//! Design: `docs/plans/2610-05-herdr-pond-v2-resume-fork-handoff-park.md`.
//!
//! Both run detached from whatever spawned them: `pond resume` and `agent
//! start` take seconds, and neither the closed desk nor an action's herdr
//! command slot may wait on them. Failures go to `launch.log` and a toast.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::config::log_line;
use crate::herdr::{self, Herdr};
use crate::hook;

const LAUNCH_LOG: &str = "launch.log";
/// herdr agent names are `[a-z][a-z0-9_-]{0,31}`.
const AGENT_NAME_MAX: usize = 32;

/// A client the desk can start on a resumed session. `{id}` is the resumed
/// session's id and `{path}` its own file.
pub(crate) struct Client {
    pub adapter: &'static str,
    resume: &'static [&'static str],
    fork: &'static [&'static str],
}

/// The clients the desk knows how to start. Whether pond can resume into one
/// is pond's answer (`no_native_dir`), never this table's.
pub(crate) const CLIENTS: &[Client] = &[
    Client {
        adapter: "claude-code",
        resume: &["--resume", "{id}"],
        fork: &["--resume", "{id}", "--fork-session"],
    },
    Client {
        adapter: "codex-cli",
        resume: &["resume", "{id}"],
        fork: &["fork", "{id}"],
    },
    Client {
        adapter: "pi-coding-agent",
        resume: &["--session", "{path}"],
        fork: &["--fork", "{path}"],
    },
];

/// The client for a session's `source_agent`; a subagent (`claude-code/x`)
/// belongs to its root client.
pub(crate) fn client(source_agent: &str) -> Option<&'static Client> {
    let root = source_agent
        .split_once('/')
        .map_or(source_agent, |(root, _)| root);
    CLIENTS.iter().find(|client| client.adapter == root)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Resume,
    Fork,
    /// A resume into a client other than the session's own.
    HandOff,
}

impl Mode {
    fn verb(self) -> &'static str {
        match self {
            Self::Resume => "resume",
            Self::Fork => "fork",
            Self::HandOff => "hand off",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Launch {
    pub session_id: String,
    /// The client to start: the session's own, or a hand-off target.
    pub adapter: String,
    pub mode: Mode,
}

impl Launch {
    fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            "launch".to_owned(),
            self.session_id.clone(),
            self.adapter.clone(),
        ];
        match self.mode {
            Mode::Resume => {}
            Mode::Fork => args.push("--fork".to_owned()),
            Mode::HandOff => args.push("--hand-off".to_owned()),
        }
        args
    }

    fn from_args(args: &[String]) -> anyhow::Result<Self> {
        let (session_id, adapter, mode) = match args {
            [id, adapter] => (id, adapter, Mode::Resume),
            [id, adapter, flag] if flag == "--fork" => (id, adapter, Mode::Fork),
            [id, adapter, flag] if flag == "--hand-off" => (id, adapter, Mode::HandOff),
            _ => bail!(crate::USAGE),
        };
        Ok(Self {
            session_id: session_id.clone(),
            adapter: adapter.clone(),
            mode,
        })
    }
}

/// Called by the desk after it restored the terminal: hands the launch to a
/// detached `herdr-pond launch`, so the overlay closes at once.
pub(crate) fn spawn(launch: &Launch) -> anyhow::Result<()> {
    let log = herdr::state_dir()?.join(LAUNCH_LOG);
    let mut command = Command::new(std::env::current_exe()?);
    command.args(launch.to_args());
    herdr::spawn_detached(command, &log)
}

/// `herdr-pond launch <session-id> <adapter> [--fork|--hand-off]`.
pub(crate) fn run(args: &[String]) -> anyhow::Result<()> {
    herdr::detach();
    let launch = Launch::from_args(args)?;
    let state_dir = herdr::state_dir()?;
    let log = state_dir.join(LAUNCH_LOG);
    let Some(pond) = herdr::resolve_pond_or_toast(&herdr::config_dir()?, &state_dir, &log) else {
        return Ok(());
    };
    let herdr = Herdr::from_env();
    let workspace = std::env::var("HERDR_WORKSPACE_ID").ok();
    let fallback = herdr::context_project()
        .or_else(|| std::env::var("HOME").ok())
        .unwrap_or_else(|| "/".to_owned());
    if let Err(error) = launch_with(&launch, &pond, &herdr, workspace.as_deref(), &fallback) {
        log_line(&log, &format!("launch {}: {error:#}", launch.session_id));
        let _ = herdr.notify(
            &format!("pond: could not {}", launch.mode.verb()),
            &format!("{error:#}"),
        );
    }
    Ok(())
}

fn launch_with(
    launch: &Launch,
    pond: &Path,
    herdr: &Herdr,
    workspace: Option<&str>,
    fallback_cwd: &str,
) -> anyhow::Result<()> {
    let client = client(&launch.adapter)
        .with_context(|| format!("the desk cannot start {} sessions", launch.adapter))?;
    let kind = hook::agent_for(client.adapter)
        .with_context(|| format!("herdr has no agent kind for {}", client.adapter))?;
    refuse_flag_shaped(&launch.session_id)?;
    // The desk's live rows can lag or miss a pane; a second client on a file
    // its agent is still writing would interleave two transcripts.
    if launch.mode == Mode::Resume
        && let Some(live) = herdr::live_agents(herdr.pane_list(None)?)
            .into_iter()
            .find(|agent| agent.matches(&launch.session_id))
    {
        return herdr.agent_focus(&live.pane_id);
    }
    let resumed = resume(pond, &launch.session_id, client.adapter)?;
    refuse_flag_shaped(&resumed.session_id)?;
    let template = match launch.mode {
        Mode::Fork => client.fork,
        Mode::Resume | Mode::HandOff => client.resume,
    };
    let mut args = Vec::with_capacity(template.len());
    for arg in template {
        if arg.contains("{path}") {
            let path = resumed.path.as_deref().with_context(|| {
                format!(
                    "pond reported no file of {}'s own, only files of its lineage",
                    launch.session_id
                )
            })?;
            refuse_flag_shaped(path)?;
            args.push(arg.replace("{path}", path));
        } else {
            args.push(arg.replace("{id}", &resumed.session_id));
        }
    }
    // A project from another machine has no directory here; the client is
    // then started where the desk was opened.
    let cwd = resumed
        .project
        .filter(|project| Path::new(project).is_absolute() && Path::new(project).is_dir())
        .unwrap_or_else(|| fallback_cwd.to_owned());
    let short_id: String = launch.session_id.chars().take(8).collect();
    let verb = launch.mode.verb();
    let pane = herdr.tab_create(workspace, &cwd, &format!("{kind} {verb} {short_id}"))?;
    let name = agent_name(&[kind, verb, &short_id, &pane]);
    if let Err(error) = herdr.agent_start(&name, kind, &pane, &args) {
        // herdr keeps an agent that stopped at a startup prompt alive; any
        // other failure would leave the user focused on an empty shell.
        if !format!("{error:#}").contains("agent_not_ready") {
            let _ = herdr.pane_close(&pane);
            return Err(error);
        }
        if let Ok(state_dir) = herdr::state_dir() {
            log_line(
                &state_dir.join(LAUNCH_LOG),
                &format!(
                    "launch {}: {kind} waits at a startup prompt in {pane}",
                    launch.session_id
                ),
            );
        }
    }
    if launch.mode == Mode::HandOff {
        let what = match resumed.fidelity.as_deref() {
            Some("native") => "the session itself",
            Some(_) => "a reconstruction of the session",
            None => "the copy an earlier hand-off wrote",
        };
        let _ = herdr.notify(
            &format!("pond: handed off to {}", client.adapter),
            &format!("{kind} opened {what}"),
        );
    }
    Ok(())
}

/// Unique among live agents, as herdr requires: the new pane's id tells two
/// forks of one session apart.
fn agent_name(parts: &[&str]) -> String {
    let mut name: String = parts
        .iter()
        .map(|part| {
            part.chars()
                .filter(char::is_ascii_alphanumeric)
                .map(|c| c.to_ascii_lowercase())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("-");
    name.truncate(AGENT_NAME_MAX);
    name
}

/// Session ids come from source files, possibly another machine's, and land
/// on a client's argv: one starting with `-` would parse as a flag (claude's
/// `--resume` takes an optional value).
fn refuse_flag_shaped(value: &str) -> anyhow::Result<()> {
    if value.starts_with('-') {
        bail!("refusing to pass {value:?} to a client: it would parse as a flag");
    }
    Ok(())
}

struct Resumed {
    session_id: String,
    /// The requested session's own file, when pond names one.
    path: Option<String>,
    project: Option<String>,
    /// What pond served on a fresh write; `None` on exit 3.
    fidelity: Option<String>,
}

/// `pond resume --out-dir native`. Exit 3 ("already resumed") is the common
/// case for a session whose client file still exists, and launches the same.
fn resume(pond: &Path, session_id: &str, adapter: &str) -> anyhow::Result<Resumed> {
    #[derive(Deserialize)]
    struct Doc {
        project: Option<String>,
        #[serde(default)]
        sessions: Vec<Written>,
        #[serde(default)]
        existing: Vec<String>,
        error: Option<String>,
        message: Option<String>,
    }
    #[derive(Deserialize)]
    struct Written {
        session_id: String,
        actual_fidelity: Option<String>,
        files: Vec<String>,
    }
    let output = Command::new(pond)
        .args([
            "resume",
            session_id,
            "--to",
            adapter,
            "--out-dir",
            "native",
            "--format",
            "json",
        ])
        .stdin(std::process::Stdio::null())
        .output()
        .with_context(|| format!("running {}", pond.display()))?;
    let doc: Doc = serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "pond resume printed no JSON ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )
    })?;
    match output.status.code() {
        // The requested session is the first pond reports.
        Some(0) => {
            let written = doc
                .sessions
                .into_iter()
                .next()
                .context("pond resume reported no session")?;
            Ok(Resumed {
                path: written.files.into_iter().next(),
                session_id: written.session_id,
                project: doc.project,
                fidelity: written.actual_fidelity,
            })
        }
        // `existing` spans the whole lineage: only a file named for the
        // session itself is its own, never a child's that happens to survive.
        Some(3) => Ok(Resumed {
            path: doc.existing.into_iter().find(|path| {
                Path::new(path).file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .ends_with(&format!("{session_id}.jsonl"))
                })
            }),
            session_id: session_id.to_owned(),
            project: doc.project,
            fidelity: None,
        }),
        _ => match (doc.message, doc.error) {
            (Some(message), _) => bail!("{message}"),
            (None, Some(error)) => bail!("pond resume: {error}"),
            (None, None) => bail!("pond resume exited {}", output.status),
        },
    }
}

/// The `park` action: store the focused agent's session, then close its pane.
/// pond is the registry, so a parked session is just an idle row in the desk.
pub(crate) fn park() -> anyhow::Result<()> {
    let herdr = Herdr::from_env();
    let pane_id = herdr::context_pane().context("park needs a focused pane")?;
    let workspace = std::env::var("HERDR_WORKSPACE_ID").ok();
    let pane = herdr
        .pane_list(workspace.as_deref())?
        .into_iter()
        .find(|pane| pane.pane_id == pane_id)
        .with_context(|| format!("pane {pane_id} is gone"))?;
    let adapter = match park_check(&pane) {
        Ok(adapter) => adapter,
        Err(refusal) => return herdr.notify("pond: not parked", &refusal),
    };
    let log = herdr::state_dir()?.join(LAUNCH_LOG);
    let mut command = Command::new(std::env::current_exe()?);
    command.args(["park", "--worker", &pane_id, adapter]);
    herdr::spawn_detached(command, &log)
}

/// Only an agent waiting for input can be parked: a working or blocked one is
/// mid-turn, and an unknown one may be.
fn park_check(pane: &herdr::Pane) -> Result<&'static str, String> {
    let agent = pane
        .agent
        .as_deref()
        .ok_or("this pane runs no agent herdr recognizes")?;
    let adapter =
        hook::adapter_for(agent).ok_or_else(|| format!("pond does not read {agent} sessions"))?;
    match pane.agent_status.as_deref() {
        Some("idle" | "done") => Ok(adapter),
        status => Err(format!(
            "{agent} is {} - park it once it is idle",
            status.unwrap_or("in an unknown state")
        )),
    }
}

/// `herdr-pond park --worker <pane> <adapter>`: the pane closes only once its
/// session is stored.
pub(crate) fn park_worker(args: &[String]) -> anyhow::Result<()> {
    let [pane_id, adapter] = args else {
        bail!(crate::USAGE);
    };
    herdr::detach();
    let state_dir = herdr::state_dir()?;
    let log = state_dir.join(LAUNCH_LOG);
    let Some(pond) = herdr::resolve_pond_or_toast(&herdr::config_dir()?, &state_dir, &log) else {
        return Ok(());
    };
    let herdr = Herdr::from_env();
    if let Err(error) = park_with(pane_id, adapter, &pond, &herdr, &log) {
        log_line(&log, &format!("park {pane_id}: {error:#}"));
        let _ = herdr.notify("pond: not parked", &format!("{error:#}"));
    }
    Ok(())
}

/// The sync can wait behind another one, so the agent is checked again
/// before its pane closes: a prompt typed meanwhile starts a new turn.
fn park_with(
    pane_id: &str,
    adapter: &str,
    pond: &Path,
    herdr: &Herdr,
    log: &Path,
) -> anyhow::Result<()> {
    let status = hook::sync_status(adapter, pond, log)
        .with_context(|| format!("running {}", pond.display()))?;
    if !status.success() {
        bail!("pond sync {adapter} {status} - the pane stays open (see launch.log)");
    }
    let Some(pane) = herdr
        .pane_list(None)?
        .into_iter()
        .find(|pane| pane.pane_id == pane_id)
    else {
        return Ok(());
    };
    if let Err(refusal) = park_check(&pane) {
        bail!("{refusal}; its session is stored, the pane stays open");
    }
    herdr.pane_close(pane_id)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::fs;

    use super::*;
    use crate::fake_pond::{Sandbox, write_script};

    /// A fake herdr that records its argv, answers `pane list` from
    /// `panes.json` and `tab create` with pane `wD:p9`, rejects an `agent
    /// start` name the way herdr 0.9.3 does, and fails `agent start` with the
    /// contents of `agent-error` when that file is not empty.
    fn fake_herdr(sandbox: &Sandbox) -> Herdr {
        set_panes(sandbox, "[]");
        Herdr::new(write_script(
            &sandbox.path("bin/herdr"),
            &format!(
                r#"printf '%s\n' "$*" >> '{calls}'
case "$1 $2" in
  "pane list") cat '{panes}' ;;
  "tab create") echo '{{"result":{{"type":"tab_created","root_pane":{{"pane_id":"wD:p9"}}}}}}' ;;
  "agent start")
    case "$3" in [!a-z]*|*[!a-z0-9_-]*) bad=1 ;; *) bad=0 ;; esac
    if [ "$bad" = 1 ] || [ ${{#3}} -gt 32 ]; then
      echo '{{"error":{{"code":"invalid_agent_name"}}}}' >&2; exit 1
    fi
    if [ -s '{agent_error}' ]; then cat '{agent_error}' >&2; exit 1; fi
    echo '{{"result":{{"type":"agent_started"}}}}' ;;
  *) echo '{{"result":{{}}}}' ;;
esac"#,
                calls = sandbox.path("herdr-calls").display(),
                panes = sandbox.path("panes.json").display(),
                agent_error = sandbox.path("agent-error").display(),
            ),
        ))
    }

    fn set_panes(sandbox: &Sandbox, panes: &str) {
        fs::write(
            sandbox.path("panes.json"),
            format!(r#"{{"result":{{"type":"pane_list","panes":{panes}}}}}"#),
        )
        .unwrap();
    }

    /// A fake `pond` that records its argv, prints `stdout` and exits `code`.
    fn fake_pond(sandbox: &Sandbox, stdout: &str, code: i32) -> std::path::PathBuf {
        fs::write(sandbox.path("pond-out.json"), stdout).unwrap();
        write_script(
            &sandbox.path("bin/pond"),
            &format!(
                "printf '%s\\n' \"$*\" >> '{calls}'\ncat '{out}'\nexit {code}",
                calls = sandbox.path("pond-calls").display(),
                out = sandbox.path("pond-out.json").display(),
            ),
        )
    }

    fn launch(session_id: &str, adapter: &str, mode: Mode) -> Launch {
        Launch {
            session_id: session_id.to_owned(),
            adapter: adapter.to_owned(),
            mode,
        }
    }

    fn started(sandbox: &Sandbox) -> bool {
        sandbox
            .lines("herdr-calls")
            .iter()
            .any(|call| call.starts_with("tab create"))
    }

    #[test]
    fn launch_args_round_trip() {
        for request in [
            launch("s1", "codex-cli", Mode::Resume),
            launch("s1", "claude-code", Mode::Fork),
            launch("s1", "pi-coding-agent", Mode::HandOff),
        ] {
            assert_eq!(Launch::from_args(&request.to_args()[1..]).unwrap(), request);
        }
        assert!(Launch::from_args(&["s1".to_owned()]).is_err());
    }

    #[test]
    fn subagents_belong_to_their_root_client() {
        assert_eq!(
            client("claude-code/general-purpose").unwrap().adapter,
            "claude-code"
        );
        assert!(client("openclaw").is_none());
    }

    /// The client table and the hook's agent table must not drift apart.
    #[test]
    fn every_client_has_a_herdr_agent_kind() {
        for client in CLIENTS {
            let kind = hook::agent_for(client.adapter).unwrap();
            assert_eq!(hook::adapter_for(kind), Some(client.adapter));
        }
    }

    #[test]
    fn a_resume_opens_a_tab_in_the_project_and_starts_the_client() {
        let sandbox = Sandbox::new();
        let project = sandbox.path("proj");
        fs::create_dir_all(&project).unwrap();
        let pond = fake_pond(
            &sandbox,
            &format!(
                r#"{{"project":"{}","sessions":[{{"session_id":"abc12345-x","files":["/c/p/abc12345-x.jsonl"]}}]}}"#,
                project.display()
            ),
            0,
        );
        launch_with(
            &launch("abc12345-x", "claude-code", Mode::Resume),
            &pond,
            &fake_herdr(&sandbox),
            Some("wD"),
            "/fallback",
        )
        .unwrap();
        assert_eq!(
            sandbox.lines("pond-calls"),
            ["resume abc12345-x --to claude-code --out-dir native --format json"]
        );
        assert_eq!(
            sandbox.lines("herdr-calls"),
            [
                "pane list".to_owned(),
                format!(
                    "tab create --cwd {} --label claude resume abc12345 --focus --workspace wD",
                    project.display()
                ),
                "agent start claude-resume-abc12345-wdp9 --kind claude --pane wD:p9 --timeout 30000 -- --resume abc12345-x"
                    .to_owned(),
            ]
        );
    }

    #[test]
    fn a_resume_of_a_live_session_focuses_its_agent_instead() {
        let sandbox = Sandbox::new();
        let pond = fake_pond(&sandbox, "{}", 0);
        let herdr = fake_herdr(&sandbox);
        set_panes(
            &sandbox,
            r#"[{"pane_id":"wD:p1","agent_session":{"value":"s1"}}]"#,
        );
        launch_with(
            &launch("s1", "codex-cli", Mode::Resume),
            &pond,
            &herdr,
            None,
            "/",
        )
        .unwrap();
        assert_eq!(
            sandbox.lines("herdr-calls"),
            ["pane list", "agent focus wD:p1"]
        );
        assert!(sandbox.lines("pond-calls").is_empty());
    }

    #[test]
    fn already_resumed_launches_on_the_sessions_own_file() {
        let sandbox = Sandbox::new();
        let pond = fake_pond(
            &sandbox,
            r#"{"error":"already_exists","project":"/nowhere/here","existing":["/pi/sessions/s/child.jsonl","/pi/sessions/s/2026_s1.jsonl"]}"#,
            3,
        );
        launch_with(
            &launch("s1", "pi-coding-agent", Mode::Fork),
            &pond,
            &fake_herdr(&sandbox),
            None,
            "/fallback",
        )
        .unwrap();
        let calls = sandbox.lines("herdr-calls");
        assert!(
            calls[0].starts_with("tab create --cwd /fallback "),
            "{calls:?}"
        );
        assert_eq!(
            calls[1],
            "agent start pi-fork-s1-wdp9 --kind pi --pane wD:p9 --timeout 30000 -- --fork /pi/sessions/s/2026_s1.jsonl"
        );
    }

    /// A child's file surviving while the session's own is gone must not be
    /// opened as the session.
    #[test]
    fn already_resumed_with_only_a_childs_file_opens_nothing() {
        let sandbox = Sandbox::new();
        let pond = fake_pond(
            &sandbox,
            r#"{"error":"already_exists","existing":["/pi/sessions/s/child.jsonl"]}"#,
            3,
        );
        let error = launch_with(
            &launch("s1", "pi-coding-agent", Mode::Fork),
            &pond,
            &fake_herdr(&sandbox),
            None,
            "/",
        )
        .unwrap_err();
        assert!(error.to_string().contains("no file of s1's own"), "{error}");
        assert!(!started(&sandbox));
    }

    #[test]
    fn a_failed_resume_shows_ponds_message_and_opens_nothing() {
        let sandbox = Sandbox::new();
        let pond = fake_pond(
            &sandbox,
            r#"{"error":"no_native_dir","adapter":"codex-cli","message":"no native session directory for codex-cli on this machine - pass --out-dir <dir> instead"}"#,
            2,
        );
        let error = launch_with(
            &launch("s1", "codex-cli", Mode::Resume),
            &pond,
            &fake_herdr(&sandbox),
            None,
            "/fallback",
        )
        .unwrap_err();
        assert!(error.to_string().contains("pass --out-dir"), "{error}");
        assert!(!started(&sandbox));
    }

    #[test]
    fn an_unknown_client_is_refused_before_pond_runs() {
        let sandbox = Sandbox::new();
        let pond = fake_pond(&sandbox, "{}", 0);
        let error = launch_with(
            &launch("s1", "openclaw", Mode::Resume),
            &pond,
            &fake_herdr(&sandbox),
            None,
            "/",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("cannot start openclaw"),
            "{error}"
        );
        assert!(sandbox.lines("pond-calls").is_empty());
    }

    #[test]
    fn a_flag_shaped_session_id_never_reaches_a_client() {
        let sandbox = Sandbox::new();
        let pond = fake_pond(&sandbox, "{}", 0);
        let herdr = fake_herdr(&sandbox);
        let flag = "--dangerously-skip-permissions";
        let error = launch_with(
            &launch(flag, "claude-code", Mode::Resume),
            &pond,
            &herdr,
            None,
            "/",
        )
        .unwrap_err();
        assert!(error.to_string().contains("parse as a flag"), "{error}");
        assert!(sandbox.lines("pond-calls").is_empty());

        let pond = fake_pond(
            &sandbox,
            &format!(r#"{{"sessions":[{{"session_id":"{flag}","files":["/c/x.jsonl"]}}]}}"#),
            0,
        );
        assert!(
            launch_with(
                &launch("s1", "claude-code", Mode::Resume),
                &pond,
                &herdr,
                None,
                "/"
            )
            .is_err()
        );
        assert!(!started(&sandbox));
    }

    #[test]
    fn a_failed_agent_start_closes_its_empty_tab_unless_the_agent_waits() {
        for (error, closed, ok) in [
            (r#"{"error":{"code":"agent_start_failed"}}"#, true, false),
            (r#"{"error":{"code":"agent_not_ready"}}"#, false, true),
        ] {
            let sandbox = Sandbox::new();
            let herdr = fake_herdr(&sandbox);
            fs::write(sandbox.path("agent-error"), error).unwrap();
            let pond = fake_pond(
                &sandbox,
                r#"{"sessions":[{"session_id":"s1","files":["/c/s1.jsonl"]}]}"#,
                0,
            );
            let result = launch_with(
                &launch("s1", "claude-code", Mode::Fork),
                &pond,
                &herdr,
                None,
                "/",
            );
            assert_eq!(result.is_ok(), ok, "{error}");
            let calls = sandbox.lines("herdr-calls");
            assert_eq!(
                calls.iter().any(|call| call == "pane close wD:p9"),
                closed,
                "{calls:?}"
            );
        }
    }

    #[test]
    fn a_hand_off_toasts_the_fidelity_served() {
        let sandbox = Sandbox::new();
        let waiting = Sandbox::new();
        let herdr = fake_herdr(&waiting);
        fs::write(
            waiting.path("agent-error"),
            r#"{"error":{"code":"agent_not_ready"}}"#,
        )
        .unwrap();
        let pond = fake_pond(
            &waiting,
            r#"{"sessions":[{"session_id":"s1","actual_fidelity":"foreign","files":["/c/s1.jsonl"]}]}"#,
            0,
        );
        launch_with(
            &launch("s1", "claude-code", Mode::HandOff),
            &pond,
            &herdr,
            None,
            "/",
        )
        .unwrap();
        assert!(
            waiting
                .lines("herdr-calls")
                .last()
                .unwrap()
                .starts_with("notification show"),
            "an agent waiting at a startup prompt still gets its toast"
        );

        let pond = fake_pond(
            &sandbox,
            r#"{"sessions":[{"session_id":"s1","actual_fidelity":"foreign","files":["/c/s1.jsonl"]}]}"#,
            0,
        );
        launch_with(
            &launch("s1", "claude-code", Mode::HandOff),
            &pond,
            &fake_herdr(&sandbox),
            None,
            "/",
        )
        .unwrap();
        let calls = sandbox.lines("herdr-calls");
        assert!(
            !calls.contains(&"pane list".to_owned()),
            "a hand-off writes a new file: {calls:?}"
        );
        assert_eq!(
            calls.last().unwrap(),
            "notification show pond: handed off to claude-code --body claude opened a reconstruction of the session"
        );
    }

    fn pane(agent: Option<&str>, status: &str) -> herdr::Pane {
        serde_json::from_value(serde_json::json!({
            "pane_id": "wD:p1",
            "agent": agent,
            "agent_status": status,
        }))
        .unwrap()
    }

    #[test]
    fn park_takes_only_an_agent_waiting_for_input() {
        assert_eq!(park_check(&pane(Some("claude"), "idle")), Ok("claude-code"));
        assert_eq!(park_check(&pane(Some("claude"), "done")), Ok("claude-code"));
        for status in ["working", "blocked", "unknown"] {
            assert!(
                park_check(&pane(Some("claude"), status)).is_err(),
                "{status}"
            );
        }
        assert!(park_check(&pane(Some("unknown-agent"), "idle")).is_err());
        assert!(park_check(&pane(None, "idle")).is_err());
    }

    #[test]
    fn park_closes_the_pane_only_after_a_good_sync_while_still_idle() {
        for (code, status, closed) in [(0, "idle", true), (1, "idle", false), (0, "working", false)]
        {
            let sandbox = Sandbox::new();
            let pond = fake_pond(&sandbox, "", code);
            let herdr = fake_herdr(&sandbox);
            set_panes(
                &sandbox,
                &format!(r#"[{{"pane_id":"wD:p1","agent":"codex","agent_status":"{status}"}}]"#),
            );
            let result = park_with(
                "wD:p1",
                "codex-cli",
                &pond,
                &herdr,
                &sandbox.path("launch.log"),
            );
            assert_eq!(result.is_ok(), closed, "code {code}, {status}");
            assert_eq!(sandbox.lines("pond-calls"), ["sync codex-cli -q"]);
            assert_eq!(
                sandbox
                    .lines("herdr-calls")
                    .contains(&"pane close wD:p1".to_owned()),
                closed,
                "code {code}, {status}"
            );
        }
    }
}
