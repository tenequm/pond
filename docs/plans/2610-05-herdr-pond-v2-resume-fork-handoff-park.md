# herdr-pond v2: resume, fork, hand-off, park (2026-10-05, draft 1)

Goal: give the v1 desk ([2609-24-herdr-pond-v1-desk-plan.md](2609-24-herdr-pond-v1-desk-plan.md), PR #312) the actions it parked. The desk can then move any session into a running agent: resume it, fork it, hand it to another harness, or park a live pane into the store. This closes [#219](https://github.com/tenequm/pond/issues/219).

Status: implemented on top of PR #312, for the owner's review. It reconciles the parked half of [2609-02](2609-02-herdr-pond-plugin-spec-and-plan.md) with the decisions v1 locked in. Each `Q` in section 7 was implemented with the proposed answer, and each is cheap to reverse in review. Grounded in the v1 code at `feat/herdr-pond-desk` and herdr 0.9.3.

Phase 0 results (2026-10-06, herdr 0.9.3, a real local store): `o` resumed a Codex and a Claude Code session in a new tab in the session's project, and herdr reported the agent on the same session id, which confirms the codex rollout id mapping. A second Claude resume took the exit-3 path and still launched. `f` came up on a new session id and left the original file untouched. `h` from Codex to Claude Code wrote a reconstruction that Claude loaded. Park synced and closed the pane. The run found two bugs, both fixed: the desk pane's teardown killed the launch leg before it detached (`spawn_detached` now uses `process_group(0)`), and herdr rejects agent names with spaces.

## 1. What changed since 2609-02

| 2609-02 assumed | v1 / today | Consequence for v2 |
|---|---|---|
| Desk lists through a new `pond sessions` verb | Desk lists through `pond serve` + `/v1/x/sql` (v1 decision 2) | No `pond sessions` verb. v2 adds no listing surface. |
| A `gone` row state, from `native_present` | `pond resume` never overwrites. It returns exit 3 with the `existing` paths when the file is already there. | The `gone` state and `native_present` are dropped. Resume is always "run `pond resume --out-dir native`, then launch". Exit 0 and exit 3 both lead to a launch. |
| "No daemon" | A plugin-owned `serve-daemon` exists (v1 decision 4) | Unchanged. v2 actions do not go through serve (section 3). |
| Launch via `herdr tab create` + `herdr pane run "<cmd>"` | herdr 0.9.x has `herdr agent start <name> --kind <k> --pane <p> -- <args>`, which waits until the agent is ready | v2 launches with `agent start`. The launch table shrinks to the harness's own resume and fork args. |
| Enter = resume, Space = read | v1 shipped Enter = jump (live) or open pager (otherwise), Space = preview | Q1 |
| Desk reads no local files | Still true for reads | v2 actions write harness files through `pond resume`. They never touch the store. Docs must say so. |

## 2. Product surface

### 2.1 Keys (Q1)

| Key | Live row | Any other row (local or remote) |
|---|---|---|
| Enter | jump (v1) | open pager (v1) |
| `o` | jump | **resume**: `pond resume <id> --to <origin> --out-dir native`, then launch the harness's resume args in a new tab |
| `f` | **fork**: new tab running the harness's native fork args. The live pane is untouched. | resume step if needed, then the native fork args |
| `h` | **hand off**: pick a target harness, `pond resume <id> --to <target> --out-dir native`, then launch the target's resume args on the written session. The toast names the fidelity served (`native` / `reconstruction`). | same |

`o`, `f` and `h` also work inside the pager, on the session being read. A cut-point fork from the pager (2609-02 4.3) stays parked (section 6).

### 2.2 Park (a herdr action, not a desk key)

`[[actions]] park`, context `pane`: run `pond sync <adapter>` in a detached worker, then `herdr pane close`, so the session is stored before the pane dies. Only an agent waiting for input (`idle` or `done`) is parked (Q4), and it is checked again after the sync, since the sync can wait behind another one while the user types a new prompt. pond is the registry: a parked session is just an idle row in the desk, so there is no marker pane and no registry file.

### 2.3 Where the new tab lands

- New tab in the focused workspace. cwd = the session's `project` (reported by `pond resume`) when that path exists on this host.
- Otherwise, typically a session from another machine, the cwd is the desk's own project. Basename mapping (2609-02 2.5) is not built: it needs a search root nobody has configured (Q3).
- Tab label: `<kind> <resume|fork|hand off> <first 8 chars of the id>`. The herdr agent name adds the new pane's id, because herdr requires names unique among live agents and two forks of one session would otherwise collide.

## 3. Mechanics

### 3.1 Desk exits, a headless leg acts

v1 already exits the desk through `DeskExit::Jump { pane_id }` and runs the jump after the terminal is restored. v2 adds one variant on the same path: `DeskExit::Launch(Launch { session_id, adapter, mode })`, with `mode` one of resume, fork or hand-off. A hand-off is a resume whose `adapter` is not the session's own. In the pager, the keys act on the session being read, not the list selection, which a refresh can move.

After restore, `main.rs` spawns a detached `herdr-pond launch <id> <adapter> [--fork|--hand-off]` (v1's `spawn_detached`) and exits, so the overlay closes at once. The leg reports failure as a herdr toast (`notification show`) and a line in `launch.log`, the same channels v1's headless legs use.

Hand-off needs a target picker before the desk exits: a small list overlay of the launch table's clients other than the session's own. The desk owns it as one more modal state beside the pager. A client that is not installed fails at `agent start`, and that failure is toasted.

### 3.2 `pond` calls

Actions shell out to the `pond` CLI, which v1 already resolves (`herdr::resolve_pond_or_toast`). They do not go through `/v1/x/sql`, for two reasons. `pond resume` writes files, and v1's serve is "plugin sends only reads" (v1 decision 4). And resume is a one-shot per keypress, so a CLI process start is affordable.

- `pond resume <id> --to <adapter> --out-dir native --format json`. Exit 0: launch with the files written. Exit 3: launch with the `existing` paths. Exit 1 `not_found` and exit 2 (`unknown_adapter`, `restore_unsupported`, `lineage_too_deep`): toast the error text verbatim, because pond's messages already name the fix.
- `--out-dir native` is the one pond-side addition (2609-02 4.2, unchanged): the keyword resolves to the target adapter's configured `[adapters.<name>].path`, else its `probe_default()` dir, else it errors. Without it the plugin would hard-code every harness's layout, which is exactly what 2609-02 1.2 criticizes the neighbouring plugins for.

### 3.3 Launch

```
herdr tab create --workspace <W> --cwd <dir> --label <label> --focus   -> root pane id P
herdr agent start <label> --kind <kind> --pane P -- <args...>
```

`agent start` replaces `pane run` because it waits for readiness and fails loudly if the agent did not come up. Its timeout is 30 s by default, so it runs in the detached headless leg (v1's `spawn_detached`) and never blocks a herdr command slot.

### 3.4 Launch table (`src/launch.rs`)

`{id}` is the resumed session's id. `{path}` is the session's own file: the first `pond resume` wrote, or on exit 3 the `existing` path named for the session (a launch that needs `{path}` refuses when only lineage files exist). The `--kind` column is not stored in the table: it comes from the hook's agent-to-adapter map, so the two cannot drift.

| pond adapter | `--kind` | resume args | native fork args |
|---|---|---|---|
| claude-code | claude | `--resume {id}` | `--resume {id} --fork-session` |
| codex-cli | codex | `resume {id}` | `fork {id}` |
| pi-coding-agent | pi | `--session {path}` | `--fork {path}` |

These are the three adapters that implement `native_restore_root` (section 4). The claude and codex flags are checked against the installed CLIs' `--help`. pi is not installed on the implementing machine, so its row is taken from 2609-02 1.3 and is unverified. oh-my-pi, opencode, grok-build and hermes join once their adapters get a native root. Whether pond can resume into a client is pond's answer (`no_native_dir`, exit 2), never the table's.

## 4. pond changes

1. `pond resume --out-dir native`. The source path is the `[adapters.<name>].path` string, else the `path` from the adapter's `probe_default`. It is mapped to a restore root by a new `AdapterFactory::native_restore_root`, which defaults to `None` (spec 6.7: a new adapter is still one file plus one registry line). The mapping lives in each adapter file (spec 6.9: on-disk layout is per-adapter code): claude-code roots at the source path, while codex-cli and pi-coding-agent root one level up because their relative paths carry `sessions/`. A multi-path config or a missing directory is `no_native_dir`, exit 2. `./native` escapes the keyword, following the `local` / `./local` precedent in spec 7.8.
2. The resume JSON document (success and `already_exists`) gains the requested session's `project`, an additive change. It is what the launch leg uses as the new tab's cwd. Search results carry no project, so this is the only source for a session picked from a search.
3. Spec 7.8: one sentence on `native`, one phrase on `project`. No new verb, no HTTP operation, no MCP change.

## 5. Plan

- **Phase 0, verify (half a day).** For claude-code and codex-cli, on this machine: `pond resume --to <a> --out-dir <native dir>`, then the resume args through `herdr agent start` in a scratch tab. Check that a rematerialized file resumes, and that a second resume returns exit 3 and still launches. Check native fork on a rematerialized file. Confirm the codex rollout id matches pond's `codex-cli` session id. Record the results at the top of this doc.
- **Built in one PR into `feat/herdr-pond-desk`:** the pond change (section 4) with an integration test in `tests/integration/resume.rs` (native root resolved from the probe under a sandbox HOME, `project` reported on exit 0 and 3, `no_native_dir`). Plus herdr-pond's `launch.rs` (table, launch leg, park), `DeskExit::Launch`, the hand-off picker, and the `park` action. Tests extend v1's fake-pond/fake-herdr scripts: resume exit 0 / 3 / 2, unknown client, a cwd that falls back for a project missing on this host, park refusing a working or unknown agent, and park closing the pane only after a successful sync. Desk tests cover `o` / `f` / `h` on live and idle rows, in the pager, and on a session the desk cannot start.
- **Hardened after a pre-merge review:** a resume re-checks herdr's live panes and focuses a running agent instead of starting a second writer on its file; on exit 3 only a file named for the session itself is used for `{path}`, never a surviving child's; a failed `agent start` closes the tab it opened (except `agent_not_ready`, where herdr keeps the agent); a hand-off toasts the fidelity served; pond's JSON error documents carry the message that names the fix; `native` refuses a configured source that is not an existing directory, and codex/pi map only a `sessions` directory.

## 6. Still parked

- **Pond-side fork with a cut-point** (2609-02 4.3). It needs an ingest-path write of a Session row and spec edits to 4.5 and 7.8. It is worth its own design once v2's native fork is in use.
- **`$pond` sidebar token** (2609-02 2.4). It is display-only, and v1's sync-on-idle already makes sessions current. Revisit if users ask.
- **Gone-row display.** Resume handles a missing file transparently (3.2), so the state has no action attached.
- **Git remote at ingest** for cross-machine project identity (2609-02 2.5 follow-up).

## 7. Questions for the owner

- **Q1.** Keys: implemented as v1's Enter = read, plus `o` / `f` / `h`. Or switch to 2609-02's Enter = resume, Space = read?
- **Q2.** `native` as a keyword on `--out-dir` (implemented, after the `local` precedent), or a separate `--native` flag?
- **Q3.** Sessions whose project is missing here: implemented as falling back to the desk's project. Basename mapping waits for git-remote-at-ingest.
- **Q4.** Park on a busy agent (`working`, `blocked` or `unknown`): implemented as refuse with a toast. Or confirm?
- **Q5.** Per-harness launch overrides in plugin config: not built. Wait until someone needs one?
- **Q6.** `herdr agent start` is verified on herdr 0.9.3. Should `min_herdr_version` move from 0.9.1 to the release that introduced it?
