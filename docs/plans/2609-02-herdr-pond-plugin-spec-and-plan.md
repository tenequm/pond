# herdr-pond: one list of every session, live or not, on any machine - spec and implementation plan (2026-09-02)

Goal: ship `packages/herdr-pond`, a [herdr](https://herdr.dev) plugin that turns pond's store into the session desk herdr does not have. One overlay lists every session the operator has - live in a herdr pane, idle on this disk, on another machine, or with its native file already pruned - and the verb follows the row: jump, resume, fork, hand off, read. Two invisible mechanisms keep the list true: a sync-on-idle event hook and a per-agent sidebar token. Three small pond additions make the plugin thin.

This document is self-contained for a fresh implementation agent: verified facts about herdr, the competing plugins, pond's current surfaces, settled design decisions, and a phased plan with tests and acceptance criteria. herdr is cloned at `~/pjv/herdrdev/herdr` (commit `8633a39`, 2026-09-02; refresh with `git pull`, never clone into this repo). memex and herdr-mirror are at `~/pjv/nicosuave/memex` and `~/pjv/nikok6/herdr-mirror`. Re-verify file paths against the clones before relying on line-level details.

Naming: the plugin id is `tenequm.pond`, the package is `packages/herdr-pond`, the binary is `herdr-pond`, and every user-facing surface says **resume**, **fork**, **hand off** (never "restore"; the spec keeps restore/serialize vocabulary internally, per the naming rule in `docs/plans/2608-06-pi-pond-plugin-and-v4-adapters-implementation-plan.md`).

## 0. Read first

pond side:

- `docs/spec.md` 1.2 (interchange hub), 4.5 (`parent_session_id` / `parent_message_id`, fork-with-cut-point), 4.8 (`model-pond-options`: the ingest host stamp), 6.2-6.3 (restore is hub-and-spoke, lineage-complete, fidelity decided by the system), 7.8 (`pond resume`, `pond sync`, `pond sql`), 2.3 non-goals ("no UI... no daemon beyond `pond serve`" - this is why the TUI is a separate binary, not a pond verb).
- `packages/pond/src/main.rs` `run_resume` (search for `fn run_resume`): the exit-code and JSON contract the plugin consumes. Verified 2026-09-02: `unknown_adapter`, `restore_unsupported`, `lineage_too_deep` exit 2; `already_exists` exit 3 with `existing` paths (the idempotent "already resumed" path `packages/pi-pond/src/resume.ts` relies on); `not_found` is its own typed error. Success JSON: `{adapter, out_dir, sessions: [{session_id, source_agent, actual_fidelity, files[]}]}`.
- `packages/pond/src/adapter/mod.rs`: `registry()` (12 adapters: claude-code, claude-desktop-app, claude-ai-export, codex-cli, opencode, openclaw, nanoclaw, hermes, pi-coding-agent, oh-my-pi, letta-code, grok-build), `known_names()`, `probe_default()` (each adapter's default source dir), `restore_destinations()` (the paths a resume would write - reuse this for the `native_present` check in section 4.1).
- `packages/pi-pond/src/picker.ts`, `hits.ts`, `resume.ts`: the only existing pond picker. Its header comments carry two load-bearing facts: (a) with embeddings on, each `pond search` CLI call cold-loads the model, so a picker must not shell out per keystroke through the vector arm; (b) exit 3 is the happy path for a re-resume.
- `packages/openclaw-pond/`, `packages/pi-pond/`, `packages/hermes-pond/`: the three existing plugin packages. herdr-pond is the fourth; it is a Rust crate, not TypeScript, because herdr plugins are argv commands and the TUI needs a real terminal program.

herdr side (all under `~/pjv/herdrdev/herdr/docs/next/website/src/content/docs/`):

- `plugins.mdx`: manifest (`[[actions]]`, `[[panes]]` with `placement`, `[[events]]`, `[[startup]]`, `[[build]]`), runtime env (`HERDR_BIN_PATH`, `HERDR_PLUGIN_CONTEXT_JSON`, `HERDR_PLUGIN_STATE_DIR`, `HERDR_PLUGIN_CONFIG_DIR`, `HERDR_PLUGIN_EVENT_JSON`), install (`herdr plugin install owner/repo/subdir`, build commands run only on install, never on `link`), marketplace (GitHub topic `herdr-plugin`, manifests in subdirectories are listed).
- `integrations.mdx` + `session-state.mdx`: herdr's own hooks report each pane's native session id (`pane.report_agent_session`); herdr persists it and replays `claude --resume <id>` etc. after a server restart. "Unsupported, missing, invalid, duplicated, or stale session references restore as normal shells." That sentence is the gap this plugin closes.
- `socket-api.mdx` "Agent state reporting" and "Event subscriptions"; `agent-automation.mdx` for `tab create` / `pane run` / `agent focus` semantics.
- `src/agent_resume.rs`: herdr's per-harness resume argv table (`plan()`), the source for our launch table.
- `src/api/schema/events.rs` `PLUGIN_HOOK_EVENT_KINDS`: the events a manifest hook may name. `pane.agent_status_changed`, `pane.agent_detected`, `pane.exited`, `pane.closed`, workspace/tab/worktree lifecycle. No `server.ready`.

## 1. Verified context (2026-09-02)

### 1.1 herdr facts the design depends on

- herdr 0.8.2 is installed locally; `herdr integration status` shows claude, codex, opencode, pi, grok, antigravity hooks current. Set `min_herdr_version = "0.8.0"` (`[[startup]]` needs 0.7.5, popup placement and tokens 0.7.4; 0.8.0 is a safe floor and matches herdr-plugin-sesh).
- Session refs: `pane list` and `agent list` expose `agent_session {source, agent, kind: id|path, value}` when an official integration reported one. Read them from `pane list`, not `agent list`: a pane can carry `agent_session` while detection reports no agent and `agent list` omits it (herdr issue #803, documented by qu8n/herdr-automatic-rename).
- For Claude Code the reported value is the Claude session UUID (`hook_input.session_id`, plus `transcript_path`), which is exactly pond's `session_id` for `claude-code` sessions (verified: this machine's live session id matches in both). For pi and omp herdr stores the session PATH. Codex reports the rollout UUID; pond's `codex-cli` session ids are UUIDv7 of the same shape (`01a06158-...`), mapping to be confirmed in phase 0.
- Plugin event hooks fire on every transition with no manifest-level filter; the hook payload for `pane.agent_status_changed` carries `pane_id`, `workspace_id`, `agent_status`, `agent`. Hooks must bail in milliseconds and must never fail the session (memex's `refuse()` exits 0 in hook modes).
- Startup hooks run once after session restore and again after live handoff; they are one-shot, not supervised daemons. There is no reliable ordering between a startup hook and herdr's own native agent resume (which fires at client attach). ntindle/herdr-resurrect piggybacks on the first `workspace.created` / `pane.created` with a debounce for boot-time work.
- Panes: `placement = "overlay"` opens a temporary zoomed pane and restores focus on exit (memex's palette). Popup is session-modal, has no pane id, gets no `HERDR_PANE_ID`, and only one exists per session. Use overlay for the desk.
- Launching a session: `herdr tab create [--workspace W] --cwd DIR --label TEXT --focus` returns JSON with `.result.root_pane.pane_id`; then `herdr pane run <pane_id> "<command>"`. `herdr pane split --pane P --direction right --cwd DIR --focus` returns `.result.pane.pane_id`. Jump: `herdr pane focus --pane <id>` / `herdr agent focus <target>` (marks the tab seen). Verified against `cli-reference.mdx` lines 153-174 and memex `src/herdr.rs` lines 53-103.
- Sidebar tokens: `herdr pane report-metadata <pane> --source <src> --token k=v [--ttl-ms N]`, cleared with `--clear-token k`; rendered as `$k` in `[ui.sidebar.agents] rows`. Tokens are not restored after a cold restart; republish from startup. Herdr caps token values at 80 characters.
- Plugin dirs: `HERDR_PLUGIN_ROOT` is a managed checkout replaced on reinstall (never store state there); config in `HERDR_PLUGIN_CONFIG_DIR`, state in `HERDR_PLUGIN_STATE_DIR`. Build commands get no `HERDR_*` runtime env and a minimal PATH.

### 1.2 What the neighbouring plugins solve, and where each stops

All star counts verified 2026-09-02.

| Plugin | Pain it solves | Where it stops |
|---|---|---|
| nicosuave/memex (181) | Find a past session by content, resume it into a herdr tab from a TUI | Per-machine index; other machines over SSH with their own index; resumes only files that still exist; no fork, no cross-client |
| iviaxpow3r/herdr-session-parker (11) | Idle agent panes burn RAM and MCP processes; closing one loses cwd, tab, and session id | Needs its own registry file and marker panes; resume only when herdr holds `agent_session.value` |
| sanirudh17/herdr-agent-handoff (12) | Agent hit its limit; continue in another agent without re-explaining | Embeds the transcript in the prompt (30k-char budget), resolves the harness file by cwd, fails closed if missing |
| wilbeibi/herdr-catchup (8) | Same, via the catchup CLI: summary, fork, hand off, send to a running agent | "Pane-scoped... a session started elsewhere isn't reachable from here"; conversation only, tool calls stripped; "handing off work that started on another machine" is on its ideas list |
| ntindle/herdr-resurrect (26) | After a crash herdr restores bare shells; relaunch programs and agents | Agent resume falls back to the harness's own store keyed by cwd; nothing if the file is gone |
| nikok6/herdr-mirror (211) | Agents on N machines, one sidebar; drive remote panes | Live panes and layout only; no history |

The sentence every one of them repeats, from herdr-agent-handoff: "Herdr stores no transcripts, and terminal scrollback isn't history." Four of them ship their own per-harness table of store paths and resume flags. Pond has the transcript regardless of the file, every harness through one model, on one store for every machine.

### 1.3 pond facts the design depends on

- Session ids are harness-native (claude-code = Claude UUID; pi = pi UUID; codex = rollout UUID). `sessions.project` is the absolute workspace path (`/Users/tenequm/Projects/pond`), and `options.source.workspace_path` carries the same. Claude messages carry `git_branch`; no adapter records the git remote yet.
- Ingest host stamp: `messages.options.pond.ingest.host = {username, hostname, device_name}` (spec 4.8), Message rows only. On this machine the hostname varies with the network (`tenequm-mbp16.ts.net lan`, `tenequm-mbp16.local`, `MacBookPro.lan`) while `device_name` is stable ("Misha's Laptop"). 10,659 of ~15,000 sessions predate the stamp and are unstamped. `pond status --hosts` already aggregates by hostname.
- `pond resume <id> --to <adapter> --out-dir <dir> --format json` writes the adapter's own layout under `--out-dir`: for claude-code that is `<out-dir>/<project-slug>/<id>.jsonl`, so `--out-dir ~/.claude/projects` lands the file where `claude --resume <id>` finds it (verified 2026-09-02 against a scratch dir). Never overwrites; exit 3 names the existing file.
- CLI latency on this store (15k sessions, 2.25M messages): `pond search --mode fts --format json` 0.28 s cold; `pond sql` 0.04 s. Per-keystroke shell-outs through the fts arm are fine with a 150 ms debounce. The vector arm is not (model cold-load); the desk never requests it.
- `pond sync <adapter>` scoped to one adapter with the freshness gate is the cheap call an event hook can afford; it takes the per-host lock and `--no-wait` skips cleanly if a sync is running.
- Native fork flags, verified locally: `claude --resume <id> --fork-session`; `codex fork <id>`; `pi --fork <path|id>`; `opencode --session <id> --fork`. These four are the daily harnesses on this machine and the v1 fork targets.

## 2. Product spec

### 2.1 The one idea

Herdr knows which sessions are running. memex knows which are on this disk. Pond knows all of them, everywhere, forever. The desk shows one list with four row states and one verb set, and every action that touches a file goes through `pond resume`, so nothing is ever guessed from a cwd and nothing is ever overwritten.

### 2.2 The desk (overlay pane)

```
pond  ~/Projects/pond          [p] project: this  [m] machine: all  [l] live: off
> compaction loop_
  live   w2:p1  laptop   claude   3m   fix the compaction rewrite loop on wide rows   312
  idle          laptop   codex    2h   add SIGTERM drain to serve                      88
  remote        pond-sb  claude   1d   dockhold remote-serve worked example           140
  gone          laptop   claude   40d  release 0.14 notes                              41
  enter jump / resume   f fork   h hand off   space read   / search   esc close
```

Columns: state, live pane id, machine, harness, age, title, message count. Title is the first conversational user text of the session, one line, truncated. Age is time since the session's last message.

Row states, decided in this order:

1. **live**: a pane in this herdr session has `agent_session.value` equal to the row's `session_id` (or, for pi/omp, a path whose file name contains it). Read from `herdr pane list` once per refresh.
2. **remote**: the session's ingest host `device_name` (fallback `hostname`) differs from this machine's. Unstamped sessions are shown as local.
3. **gone**: local session whose native file is absent on this host (`native_present = false` from `pond sessions`, section 4.1).
4. **idle**: everything else.

Actions:

| Key | On a live row | On idle / gone | On remote |
|---|---|---|---|
| enter | focus that pane (`herdr agent focus`) | rematerialize if gone (`pond resume --out-dir native`), then new tab with the harness resume command | same as idle; the tab's cwd is the mapped local project (section 2.5) |
| f | fork: new tab beside it with the harness's native fork command; the live session is untouched | rematerialize if gone, then native fork command | same |
| h | hand off: pick a target harness from the installed list; `pond resume --to <target> --out-dir native`; launch the target's resume command on the written session; the toast names the fidelity served (native or reconstruction) | same | same |
| space | read: `pond get-session` rendered in a scrollable pane; `f` inside it forks from the highlighted message (phase 4) | same | same |
| / | full-text search over content (fts arm), scoped by the active filters | | |
| p / m / l | toggle project scope (this cwd / all), machine scope (this / all), live-only | | |
| esc | close; herdr restores focus | | |

Placement of new panes: `tab` by default, `split` via config, matching memex's `herdr_resume` key so users coming from it feel at home. The new tab's cwd is the session's project; the label is the harness name plus the title.

### 2.3 Sync on idle (event hook)

`[[events]] on = "pane.agent_status_changed"` runs `herdr-pond hook`. It exits immediately unless `agent_status` is `idle`, `done`, or `blocked`. It reads the pane's `agent_session` from `pane list`, maps the herdr agent name to a pond adapter (`claude -> claude-code`, `codex -> codex-cli`, `pi -> pi-coding-agent`, `omp -> oh-my-pi`, `opencode -> opencode`, `grok -> grok-build`, `hermes -> hermes`), and runs `pond sync <adapter> --no-wait -q` detached. Debounce: at most one sync per adapter per 10 s, tracked in `HERDR_PLUGIN_STATE_DIR`. Nothing is written to stdout; failures go to the plugin log.

Effect: a session is in the bucket seconds after its agent stops. This is what makes the desk's remote rows current, and what lets an orchestrator agent read a sibling's full transcript through `pond_get_session` off the id `herdr agent list` gives it instead of scraping the screen (one added line in `packages/pond/SKILL.md`).

### 2.4 The `$pond` token

After a successful sync the hook writes `herdr pane report-metadata <pane> --source tenequm.pond --token pond=synced` for the panes of that adapter; while a sync is pending it writes `pond=pending`; for a harness with no enabled adapter it writes nothing. The `[[startup]]` hook republishes tokens for every pane with an `agent_session` (tokens do not survive a cold restart). A setup action prints the `[ui.sidebar.agents] rows` snippet that shows `$pond`; the plugin never edits `config.toml`.

### 2.5 Cross-machine rows

Pond's `project` is an absolute path, so the same repo is a different project on each host. v1 matches remote rows to the current project by repo basename (`basename(project)`) when the `[p]` filter is on, and resumes them into the current pane's cwd. Storing the git remote URL at ingest (spec 9 candidate) is the proper key and is out of scope here; the plan lists it under follow-ups.

### 2.6 Park

`[[actions]] park`: from a live agent pane, `pond sync <adapter>` (foreground, so the pane is in the store before it dies), refuse if the agent is `working` unless confirmed, then `herdr pane close`. No registry, no marker pane: pond is the registry, and the desk's idle rows are the parked sessions.

### 2.7 Non-goals for this package

- No search desk over pane output, no notifications, no remote control, no layout management. Collie, herdr-remote, herdr-mirror, sessionizer own those and all render richer rows once `$pond` exists.
- No transcript-in-prompt handoff. Hand-off is foreign restore; where an adapter's foreign serialize is thin, the toast says "reconstruction". The plugin never fabricates a prompt from a transcript.
- No daemon. The hook is a short-lived process; the desk runs only while open.
- No agent.view.set. Nothing pond knows changes which agents need attention.

## 3. Package layout

```
packages/herdr-pond/
  Cargo.toml              # workspace member; bin "herdr-pond"; ratatui + crossterm + serde_json + tokio(process)
  herdr-plugin.toml       # id = "tenequm.pond", version = crate version, min_herdr_version = "0.8.0"
  herdr/install.sh        # [[build]]: reuse a current `pond` + `herdr-pond` on PATH, else download the pond release tarball
  src/main.rs             # subcommands: desk | hook | startup | park | setup-rows
  src/pond.rs             # typed wrappers over `pond sessions|search|get-session|resume|sync` (JSON in/out)
  src/herdr.rs            # typed wrappers over `herdr pane list|tab create|pane split|pane run|agent focus|pane report-metadata`
  src/launch.rs           # per-harness resume / fork argv table (section 3.2)
  src/desk/               # ratatui app: list, search, preview, action dispatch
  README.md               # install, keys, config, the two mechanisms, honest limits
```

Add `"packages/herdr-pond"` to the root `Cargo.toml` `members`. The release job that builds `pond` builds the workspace and ships `herdr-pond` inside the same per-target archive (one tarball, two binaries), so `install.sh` downloads one asset and gets both. The macOS sign/notarize step signs both Mach-O files; the notarization zip already carries the whole archive.

Manifest (initial):

```toml
id = "tenequm.pond"
name = "pond"
version = "0.1.0"
min_herdr_version = "0.8.0"
description = "Every agent session you have - live, idle, on another machine, or with its file gone - in one list. Resume, fork, or hand off any of them."
platforms = ["macos", "linux"]

[[build]]
command = ["bash", "herdr/install.sh"]

[[panes]]
id = "desk"
title = "pond"
placement = "overlay"
command = ["sh", "-c", "exec \"$HERDR_PLUGIN_ROOT/bin/herdr-pond\" desk"]

[[actions]]
id = "desk"
title = "pond: sessions"
contexts = ["pane", "workspace"]
command = ["bin/herdr-pond", "open-desk"]

[[actions]]
id = "park"
title = "pond: park this pane"
contexts = ["pane"]
command = ["bin/herdr-pond", "park"]

[[actions]]
id = "setup-rows"
title = "pond: print sidebar rows snippet"
command = ["bin/herdr-pond", "setup-rows"]

[[events]]
on = "pane.agent_status_changed"
command = ["bin/herdr-pond", "hook"]

[[startup]]
command = ["bin/herdr-pond", "startup"]
```

`open-desk` is the headless leg: it issues `herdr plugin pane open --plugin tenequm.pond --entrypoint desk --placement overlay --env POND_DESK_CWD=<focused_pane_cwd> --focus` and exits (the two-process pattern from herdr-mirror's `src/pick.rs`; the pane's cwd is taken from `HERDR_PLUGIN_CONTEXT_JSON.focused_pane_cwd`, since an overlay's own cwd is not the operator's).

Config (`$HERDR_PLUGIN_CONFIG_DIR/config.toml`, re-read on every invocation, no restart):

```toml
placement = "tab"        # tab | split
sync_on_idle = true
token = true
project_filter = true    # desk opens scoped to the focused pane's project
```

### 3.2 Launch table

Copied from herdr `src/agent_resume.rs` and the flags verified in 1.3; user-overridable per harness in config.

| pond adapter | herdr agent | resume argv | fork argv (v1) | native dir (`--out-dir native`) |
|---|---|---|---|---|
| claude-code | claude | `claude --resume {id}` | `claude --resume {id} --fork-session` | `~/.claude/projects` |
| codex-cli | codex | `codex resume {id}` | `codex fork {id}` | `~/.codex/sessions` |
| pi-coding-agent | pi | `pi --session {path}` | `pi --fork {path}` | `~/.pi/agent` |
| oh-my-pi | omp | `omp --resume={path}` | none in v1 | `~/.omp/agent` |
| opencode | opencode | `opencode --session {id}` | `opencode --session {id} --fork` | opencode storage (SQLite; foreign target only in v1 unless the adapter's serialize writes the DB) |
| grok-build | grok | `grok --resume {id}` | none in v1 | `~/.grok/sessions` |
| hermes | hermes | `hermes --resume {id}` | none in v1 | hermes home |

A harness with no fork argv shows `f` as unavailable in v1 and gets pond-side fork in phase 4. Each command runs in a tab whose cwd is the project; `{path}` is the first file `pond resume` reported.

## 4. pond additions (Rust, `packages/pond`)

### 4.1 `pond sessions` verb

```
pond sessions [--project <path|basename>] [--host <device_name|hostname>] [--source-agent <name>]
              [--query <text>] [--since <date>] [--limit N] [--format text|json]
```

One handler over the three datasets (DataFusion, same engine as `pond sql`), returning per session: `session_id`, `source_agent`, `project`, `host` (`device_name` else `hostname` from the first stamped message, null if unstamped), `created_at`, `last_activity` (max message timestamp), `message_count`, `title` (first conversational user `search_text`, one line, 120 chars), `parent_session_id`, `native_present` (true when the adapter's `restore_destinations()` for this session all exist on this host; null when the adapter is not registered here). `--query` ranks through the fts arm and keeps the filters as prefilters (`search-prefilter-pushdown`). Default sort: `last_activity` desc. Subagent sessions are excluded unless `--include-subagents`, matching search.

Spec edit: add to 7.8. Not on the MCP surface (the agent already has `pond_search` and `pond_sql`).

### 4.2 `pond resume --out-dir native`

The keyword `native` resolves to the target adapter's configured `[adapters.<name>].path`, else its `probe_default()` dir, so callers stop hard-coding harness layouts. Error if neither resolves. Spec edit: one sentence in the `pond resume` entry.

### 4.3 `pond resume --fork [--at <message-id>]` (phase 4)

Writes the session under a new id with `parent_session_id` = source and, with `--at`, `parent_message_id` = the cut-point, truncating messages after it; the file carries the new id in the harness's own format. Pond records the lineage immediately by writing the new Session row through the ingest path (parent pointers set, zero messages). When the harness later writes the forked file and `pond sync` ingests it, the Session row is a merge-insert no-op (`adapter-integrity-additive-sync`), so the pointers survive. A forked session never run shows in `pond sessions` with `message_count = 0`; `pond erase` retires it. New id format is per adapter (UUIDv4 for claude/codex, pi's own scheme for pi). Spec edit: 7.8 and 4.5.

## 5. Plan

### Phase 0: verify (half a day)

- Confirm codex and pi id mapping: start a codex and a pi session inside herdr, read `herdr pane list` `agent_session.value`, match against `pond sessions --source-agent codex-cli|pi-coding-agent --limit 5`.
- Confirm `claude --resume <id>` works from a fresh tab whose cwd is the project and that a rematerialized file (from `pond resume --out-dir ~/.claude/projects`) resumes; then confirm `--fork-session` on it.
- Confirm `herdr plugin pane open --placement overlay --env` passes env into the pane command (memex relies on it).
- Record the answers at the top of this document under "Phase 0 results".

### Phase 1: pond verbs (1-2 days)

- `pond sessions` (4.1) with unit tests on the title/host/last_activity derivation and an integration test over the conformance fixtures (one session per adapter; assert `native_present` true after a native resume into a temp dir configured as that adapter's path, false before).
- `--out-dir native` (4.2) with a test per adapter that has `probe_default`.
- Spec 7.8 edits; `docs/site` CLI reference page.
- PR 1. No plugin code yet.

### Phase 2: the desk (3-5 days)

- Crate skeleton, workspace member, `herdr/install.sh` (structure copied from memex's `herdr/install.sh`: probe PATH with a real subcommand not `--version`, then versioned tarball with sha256 verify, then `cargo build --release`; on macOS `rm -f` before replacing a signed binary), `herdr-plugin.toml` with `desk` and `open-desk` only.
- `src/pond.rs`: `sessions`, `search` (fts only), `get_session`, `resume`, typed on the JSON contracts above; exit 3 handled as success with `existing` paths.
- `src/herdr.rs`: `pane_list`, `tab_create`, `pane_split`, `pane_run`, `agent_focus`, `plugin_pane_open`; every call through `HERDR_BIN_PATH` with a PATH fallback to `herdr`.
- `src/launch.rs`: the table in 3.2.
- Desk v1: list with the four states, filters, search with 150 ms debounce, preview (space), enter (jump / resume), f (native fork where the table has one), h (hand off: target picker from installed harnesses, foreign resume, launch, fidelity toast). Placement per config.
- Tests: launch-table snapshot tests; row-state decision table (unit); a fake `pond` and `herdr` on PATH (shell scripts emitting canned JSON) driving the desk headlessly through its action dispatcher for jump, resume, already-resumed, gone, remote, fork, hand off; no live herdr in CI.
- README with keys, config, limits (post-hoc not live; token cap; hand-off fidelity; cross-machine basename matching; live rows need an official herdr integration).
- PR 2. Dogfood on this machine for a week before phase 3.

### Phase 3: the two mechanisms and park (1-2 days)

- `hook` (2.3) with the debounce file, `startup` (token republish), `setup-rows`, `park` (2.6), `$pond` token writes (2.4). Config gates for `sync_on_idle` and `token`.
- Tests: hook decision (status filter, adapter map, debounce) against fake binaries; park refuses `working` without confirm.
- PR 3.

### Phase 4: pond-side fork with cut-point (2-3 days)

- `pond resume --fork [--at]` (4.3) for claude-code, codex-cli, pi-coding-agent, opencode; conformance test that the forked file re-ingests as a child of the source with the pointers intact and the message set truncated at the cut-point.
- Desk: `f` inside the preview forks from the highlighted message; `f` on harnesses without a native fork uses the pond-side path.
- PR 4.

### Phase 5: release and listing (half a day)

- Bump `herdr-plugin.toml` version with the crate; release workflow ships `herdr-pond` in the pond archives; add the `herdr-plugin` topic to `tenequm/pond`; verify `herdr plugin install tenequm/pond/packages/herdr-pond` on a clean machine.
- Docs site page "herdr" under integrations; README section; one line in `packages/pond/SKILL.md` about reading a sibling agent's transcript by herdr session id.
- GTM: changelog post per `pjs/pond-gtm/CLAUDE.md` queue item 9 (fold "pond is a herdr plugin" into the v0.14.x changelog post or the next release post).

## 6. Acceptance criteria

1. From any herdr pane, one key opens the desk scoped to that project in under 300 ms with the list populated (measured on the 15k-session store).
2. A session whose Claude file was deleted resumes into a new tab and continues (rematerialized from the bucket), and a second resume of it is a no-op that opens the existing file.
3. A session ingested from pond-sb (remote row) resumes on the laptop into the mapped project directory.
4. `f` on a live Claude pane opens a fork beside it and the original pane keeps running untouched.
5. `h` from a Claude session into pi launches pi on a pond-written session and the toast reports the fidelity served.
6. With `sync_on_idle` on, an agent that just went idle is searchable from another pane's `pond_search` within 15 s.
7. `pond sessions --format json` is the only listing surface the desk uses; the desk contains no SQL.
8. `herdr plugin install tenequm/pond/packages/herdr-pond --yes` works on a machine with no pond installed and ends with `pond init` guidance printed.

## 7. Risks and open questions

- **Not live.** Pond ingests after the harness flushes; `blocked` and `done` are fine, `working` rows are not refreshed mid-turn. The desk labels ages from the store, never from the screen.
- **Startup ordering.** Republishing tokens at startup can race herdr's native resume; tokens are display-only so a stale `pending` is harmless. Nothing in this plan depends on running before native resume.
- **Foreign restore quality** is per adapter pair. Hand-off must say "reconstruction" on screen; do not let it look native.
- **Two binaries in one archive** changes the release job; verify Windows dist (this plugin is macOS/Linux only, but the archive layout is shared) and the Homebrew/Scoop manifests that list binaries.
- **Unstamped sessions** (10.6k here) show as local. Acceptable; a backfill command is a separate decision (spec 4.8 says backfill is explicit, never a sync side effect).
- **Codex fork semantics**: `codex fork <id>` creates a new rollout; confirm it accepts a rematerialized rollout file in phase 0.
- Open: should `pond sessions` also be an HTTP operation (`POST /v1/sessions`) so a remote-serve topology (Dockhold) can back the desk? Decide after phase 2; the handler is transport-agnostic either way.
- Open: git remote at ingest for cross-machine repo identity (2.5). Separate issue.

## 8. Follow-ups outside this plan

- Git remote URL in `options.source` at ingest (all file adapters), then `pond sessions --repo <remote>`.
- `pond sessions` on MCP if agents turn out to want a listing rather than search.
- herdr-mirror hook: on a remote row, "resume there" via `herdr-mirror remote-new-tab` when a mirror workspace for that host is open.
- Upstream: a `server.ready` plugin event in herdr (resurrect's proposal) would make token republish race-free.
