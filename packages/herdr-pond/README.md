# herdr-pond

A [herdr](https://herdr.dev) plugin for pond:

- **Sync-on-idle** - when an agent in a herdr pane goes idle, its adapter is synced into your pond store (`pond sync <adapter>`), so the session is searchable seconds later.
- **The desk** - one overlay listing recent sessions across every harness and machine in your store, with content search, previews, and full conversational transcripts read straight from the store. Enter on a session that is running in a herdr pane jumps to that pane.
- **Resume, fork, hand off** - from the desk or a transcript: `o` resumes the selected session into its own client in a new tab (or jumps to it, when an agent is already running it), `f` forks it with the client's native fork, `h` hands it to another client. Each runs `pond resume <id> --out-dir native`, which writes the client's own session file, or reuses it when it is already there, and then starts the client on it. Clients: Claude Code, Codex, pi.
- **Park** - the `pond.park` action stores the focused agent's session (`pond sync <adapter>`) and then closes its pane. The session stays in the desk to resume later. Only an agent waiting for input is parked; one that starts working again during the sync keeps its pane.

## Prerequisites

- `pond` installed and initialized: run `pond init` once, with the adapters you use enabled (`pond adapters enable <adapter>`). Sync-on-idle only syncs adapters that are already enabled; it never enables one.
- A pond release that includes `POST /v1/x/sql` and `pond serve --socket` ([tenequm/pond#311](https://github.com/tenequm/pond/pull/311)). With an older pond the desk and `daemon.log` say it is too old and name the upgrade command.
- For live rows (the running-agent marker and jump): the official herdr integration for each agent, e.g. `herdr integration install claude`. Without it herdr knows the agent but not its session id.

## Build and link

```sh
cargo build --release -p herdr-pond
herdr plugin link packages/herdr-pond
```

The package must be named: a bare `cargo build --release` at the repo root builds pond only.

`packages/herdr-pond/bin/herdr-pond` is a committed symlink to `../../../target/release/herdr-pond`, the default cargo target dir. If you set `CARGO_TARGET_DIR`, point that symlink at your target dir by hand.

## Open the desk

herdr has no action palette, so bind a key in your herdr config:

```toml
[[keys.command]]
key = "..."
type = "plugin_action"
command = "pond.desk"
```

Bind `pond.park` the same way to park the focused agent.

herdr runs every action against the focused pane, in the focused workspace, wherever the key or `herdr plugin action invoke` came from. To open the desk or park a particular pane, focus it first.

Then run `herdr server reload-config`. Pressing the key in a workspace whose desk is already open focuses that desk instead of opening a second one.

## Config

`config.toml` in the plugin config dir (`herdr plugin config-dir pond`), re-read on every run. A malformed file is logged and ignored.

```toml
sync_on_idle = true                    # default; false turns the idle hook off
pond_bin = "/opt/homebrew/bin/pond"    # optional, absolute; default: PATH lookup
```

herdr's PATH is fixed when the herdr server starts. If `pond` is not on it, set `pond_bin`.

## How it runs

- Each herdr server starts one `pond serve` in the background at startup, listening on a Unix socket in the plugin state dir (`serve/<hash>/owner.sock`, owner-only), and stops it when that server exits. It is a personal server only your user can reach, not a TCP port; the plugin sends it only reads. It never runs sync (`--with-sync` is not passed), and `pond schedule` stays the owner of scheduled sync.
- If that serve is missing or dead, the desk starts its own, on its own socket, for as long as it is open. If the desk is killed with SIGKILL, that serve is orphaned (visible in `ps`) until you stop it.
- If the per-server serve outlives its herdr server's watchdog (the watchdog was killed), the next watchdog for that server stops it when it answers on its socket or its command line names that socket, then starts a fresh one; a process matching neither is left alone.
- Idle syncs wait for any sync already holding the store lock, then run. Bursts of idle events coalesce; the last one always produces a sync.
- The machine column shows each session's origin host, read from its first message; sessions from the machine the desk runs on show as `this`. Sessions ingested before pond stamped the ingest host have no recorded machine. The desk shows them as `local?` - unknown provenance, not a claim that they came from this machine.
- Typed search (`/`) covers the whole store - every project and all time - until `p` narrows it to this project or `t` to the last 14 days. In the listing, `p` and `t` widen instead: it opens on this project's last 14 days.
- The desk keeps what it learned (listings, titles, counts, hosts) in `desk-cache.json` in the plugin state dir, readable only by you, and paints from it at once on the next open while pond refreshes behind it. Deleting the file only costs that head start.
- The transcript view is conversation only: user and assistant text. Tool calls and results stay reachable through `pond_sql` and `pond_get_session`.
- `o`, `f` and `h` close the desk and hand off to a detached `herdr-pond launch`. It runs `pond resume`, opens a tab in the session's project (or the desk's, when that project does not exist on this machine, as for a session from another host), and starts the client there with `herdr agent start`. A hand-off is pond's foreign restore: a reconstruction in the target client's format, not the original file, and a toast says which the target opened. Park syncs and closes the same way, in the background. Neither writes session data to the store beyond that `pond sync`.

## Logs

In the plugin state dir (herdr's state dir, `plugins/pond/`):

- `sync.log` - one line per idle sync (adapter, exit status, duration) plus pond's own output.
- `serve/<hash>/daemon.log` - the per-server serve's lifecycle and output.
- `serve/<hash>/desk-serve.log` - a desk-started serve's output.
- `launch.log` - every resume, fork, hand-off and park that failed after it started, with pond's or herdr's error, plus pond's own output from each park's sync. A park refused up front (the agent is busy) is only toasted.
- `desk.log` - what the desk did not show you: a `desk-cache.json` it could not read or write, failed background lookups of titles, counts and hosts (the rows are retried as you move), and the full text of every error it showed as a clipped toast.

Each log starts over past 1 MiB. herdr's `plugin log list` only shows that a hook exited, not that a sync ran - `sync.log` is the record.
