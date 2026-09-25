# herdr-pond

A [herdr](https://herdr.dev) plugin for pond:

- **Sync-on-idle** - when an agent in a herdr pane goes idle, its adapter is synced into your pond store (`pond sync <adapter>`), so the session is searchable seconds later.
- **The desk** - one overlay listing recent sessions across every harness and machine in your store, with content search, previews, and full conversational transcripts read straight from the store. Enter on a session that is running in a herdr pane jumps to that pane.

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
- Idle syncs wait for any sync already holding the store lock, then run. Bursts of idle events coalesce; the last one always produces a sync.
- Sessions ingested before pond stamped the ingest host have no recorded machine. The desk shows them as `local?` - unknown provenance, not a claim that they came from this machine.
- The transcript view is conversation only: user and assistant text. Tool calls and results stay reachable through `pond_sql` and `pond_get_session`.

## Logs

In the plugin state dir (herdr's state dir, `plugins/pond/`):

- `sync.log` - one line per idle sync (adapter, exit status, duration) plus pond's own output.
- `serve/<hash>/daemon.log` - the per-server serve's lifecycle and output.
- `serve/<hash>/desk-serve.log` - a desk-started serve's output.

Each log starts over past 1 MiB. herdr's `plugin log list` only shows that a hook exited, not that a sync ran - `sync.log` is the record.
