# pond

Lossless storage and hybrid search for AI agent sessions, across every agentic client.

pond keeps every AI conversation you've ever had intact and searchable, and lets you continue any of them in any supported tool. One Rust binary that ingests sessions from registered agentic-client adapters into a canonical Session / Message / Part interlingua, stores them in Lance on object storage, and serves hybrid search over them via HTTP+JSON and MCP. Every adapter is a bidirectional codec, so any session restores into any client - not only the one that made it.

Current automatically synced agent clients:
- Claude Code CLI
- Codex CLI
- opencode CLI
- pi-coding-agent CLI

## Install

Linux and macOS are supported; Windows is not in v1 scope.

```sh
brew install tenequm/tap/pond                       # Homebrew
nix profile add github:tenequm/pond-nix#pond        # Nix
cargo install pond-db                               # crates.io (installs the `pond` command)
```

On macOS the Metal backend is selected automatically; on other systems the CPU fallback runs without extra features.

## Quickstart

1. Ingest your local sessions. First run prompts you to enable each detected adapter; pass `-y` for non-interactive runs:

   ```sh
   pond sync         # interactive prompts on a TTY
   pond sync -y      # auto-accept every probe (cron, CI, headless agents)
   ```

   Each adapter you accept lands as `[sources.<name>] enabled = true` in `config.toml` (under `$XDG_CONFIG_HOME/pond/`). Sections without `enabled = true` are skipped; re-enable interactively with `pond sync <name>`.

2. Add pond as an MCP server in your agent client:

   ```sh
   claude mcp add -s user pond -- pond mcp   # Claude Code
   codex mcp add pond -- pond mcp            # Codex
   ```

3. Now just ask your agent - it searches your history through pond for you:

   - "search my past sessions for how we fixed the OCC retry race"
   - "what did we decide about the storage substrate, and why?"
   - "pick up where we left off on the tokenizer experiment"
   - "find the exact command from when we set up that config"

pond runs hybrid search across every session from every client - including sessions made in a different tool than the one you're asking in. Re-run `pond sync` to pick up new sessions.

## Remote storage

By default pond stores its data locally (under `$XDG_DATA_HOME/pond`). To put it on an object store instead, add a credential set, then switch the destination:

```sh
pond storage creds add        # interactive: set name (default), access key, hidden secret
pond storage use s3+https://nbg1.your-objectstorage.com/my-pond
```

`creds add` writes a `[creds.<name>]` block to `config.toml`; `use` probes the destination end-to-end and flips `[storage].path` to it. The result is just config you could also write by hand:

```toml
[storage]
path = "s3+https://nbg1.your-objectstorage.com/my-pond"

[creds.default]
access_key_id     = "..."
secret_access_key = "..."
```

The `s3+https://host/bucket/prefix` form carries the endpoint, bucket, and prefix in one URL - it works for any S3-compatible store (Hetzner, R2, B2, MinIO). Plain `s3://`, `gs://`, and `az://` URLs work too, using the standard cloud SDK credential chain when no `[creds.*]` set matches. Probe a destination before relying on it:

```sh
pond storage check   # parse, creds binding, conditional-put (OCC), write/read/delete
pond config show     # resolved config: redacted values, where each came from
```

Everything also works with no config file at all - `POND_STORAGE_PATH` plus `POND_CREDS_DEFAULT_ACCESS_KEY_ID` / `POND_CREDS_DEFAULT_SECRET_ACCESS_KEY` is a complete configuration (handy for containers and CI).

Several machines can share one bucket: give each the same `config.toml` and run `pond sync` from cron on each. Concurrent writers are safe - Lance's optimistic concurrency control serializes commits through the object store's conditional writes.

`use` only switches the pointer; it never moves data. To carry your existing local sessions into the bucket, copy them first, then switch:

```sh
pond storage migrate --from ~/.local/share/pond --to s3+https://nbg1.your-objectstorage.com/my-pond
pond storage use s3+https://nbg1.your-objectstorage.com/my-pond
```

Migrate is an idempotent union merge: re-runnable, safe onto a populated destination, and it never deletes or modifies the source - your local data stays put as a backup. For the full walkthrough (credentials, verification, rollback, and the agents/CI path), see [Migrate from local to remote](./migrate-local-to-remote.md).

### Troubleshooting

For more detail on any command, raise the tracing level: `pond -v sync` (info), `pond -vv sync` (debug), `pond -vvv sync` (trace). `RUST_LOG` overrides the flag when set.
