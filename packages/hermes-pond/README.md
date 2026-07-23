# hermes-pond

**Read-only. Local. Zero data egress.**

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) plugin that
projects [pond](https://github.com/tenequm/pond)'s read-only recall tools into
your agent and (managed mode) supervises a local `pond` process, so installing
the plugin and running one setup command is the complete installation.

pond is the durable, lossless archive of your past agent sessions: permanence
past a harness's disk budget, a cross-harness corpus (Claude Code, OpenClaw,
hermes, and every other ingested source), off-agent indexing, and restore. This
plugin is deliberately **tools only** - no memory-provider slot, no ambient
recall, no prompt injection, no `session_search` override. It adds four tools
under the `pond` toolset:

- `pond_search` - search a durable archive of past sessions (exact-word BM25
  full-text, with semantic vector search on top).
- `pond_get_session` - read a whole past session as a transcript.
- `pond_get_message` - expand one message with its full tool bodies.
- `pond_sql` - read-only SQL analytics over the corpus.

All four are read-only. pond's MCP surface never exposes a write path, so the
plugin is read-only by construction.

## pond vs hermes `session_search` - they coexist

hermes ships `session_search` (live FTS5 over the current profile's `state.db`).
pond does not replace or override it. They answer different questions:

- **`session_search`** - live, same-harness search of the current hermes
  sessions, including the session in progress.
- **`pond_search` and friends** - the durable **cross-harness** archive of PAST
  sessions. It survives resets and disk-budget pruning, spans every harness pond
  ingests, and its search covers user/assistant conversational text by design
  (tool calls and reasoning are excluded as low-signal noise).

The tool descriptions route between them; nothing here changes `session_search`.

## Install

The plugin has **zero third-party Python dependencies** (its MCP client is pure
stdlib), so there is nothing to pip-install - enabling it just works. You do
need the separately installed `pond` binary for the recall data.

1. Install the plugin from this monorepo subdirectory and enable it:

   ```bash
   hermes plugins install tenequm/pond/packages/hermes-pond --enable
   ```

   This clones the plugin into `~/.hermes/plugins/pond/`. (Under the hood
   `hermes plugins install` accepts an `owner/repo/subdir` shorthand and reads
   the plugin's `plugin.yaml`.)

2. Run setup - locates the `pond` binary and exposes the tools on your chat
   surfaces:

   ```bash
   hermes pond setup
   ```

3. Restart the gateway so the tools take effect:

   ```bash
   hermes gateway restart
   ```

If the `pond` binary is missing, `hermes pond setup` fails with the exact fix
(no silent downloads): `brew install tenequm/tap/pond`, `cargo install pond`, or
a binary from <https://github.com/tenequm/pond/releases>.

### What `hermes pond setup` does

- **Locates the pond binary**: config `pond.binaryPath`, then `PATH`, then
  well-known dirs (`~/.cargo/bin`, `~/.local/bin`, `/opt/homebrew/bin`,
  `/usr/local/bin`).
- **Writes the per-platform tool allowlist**. The CLI and TUI include plugin
  tools by default, but gateway platforms, the API server, and cron resolve
  their tools from `platform_toolsets.<platform>` in `config.yaml`. A plugin
  toolset is default-on for a platform you have never configured, but goes
  default-off once that platform has a saved toolset list that omits it. So
  setup adds `pond` explicitly to every already-configured platform plus
  `api_server` and `cron`, preserving each platform's native tools. Pass
  `--all-platforms` to list it on every gateway platform.
- **Bootstrap is delegated to the managed process**: on first run the managed
  child starts with `--bootstrap hermes`, which enables pond's `hermes` adapter
  only when pond has no adapters configured. An existing pond config is left
  byte-identical - the plugin never mutates it.

### Manual alternative: an `mcp_servers` recipe

If you would rather not use the plugin, hermes can talk to pond as a raw MCP
server. Add to `config.yaml` and run `pond serve` yourself:

```yaml
mcp_servers:
  pond:
    command: pond
    args: ["serve", "--transport", "stdio", "--with-sync", "--bootstrap", "hermes"]
```

This works but misses the onboarding: MCP servers are excluded from the gateway,
API, and cron surfaces by default, so the tools would only appear in the CLI/TUI
unless you also add `pond` to the relevant `platform_toolsets` lists by hand. The
plugin exists to own that story.

## Modes

Configured under `plugins.entries.pond` in `config.yaml`:

```yaml
plugins:
  enabled: [pond]
  entries:
    pond:
      pond:
        mode: managed          # default; plugin spawns and supervises pond
        syncIntervalMinutes: 5  # passed to pond's in-serve sync scheduler
        # binaryPath: /usr/local/bin/pond
      sources: ["hermes"]       # pond source_agent filter; ["*"] = whole corpus
```

- **managed** (default): the plugin locates `pond` and supervises
  `pond serve --transport stdio --with-sync`, speaking MCP over the child's
  stdio - no port, no token, no auth surface. It restarts the child with
  exponential backoff on exit, runs it at low scheduling priority (`nice -n 19`)
  so background sync never competes with interactive work, and logs to
  `~/.hermes/logs/pond-serve.log`.
- **url**: attach to an external `pond serve` over streamable HTTP; no
  supervision. The operator owns the endpoint and any auth (passed as
  `headers`):

  ```yaml
      pond:
        mode: url
        url: https://host/mcp
        headers: { Authorization: "Bearer ..." }
  ```

`sources` maps to pond_search's `source_agent` filter, which matches a source
whose value equals the entry OR starts with `<entry>/` - so `"hermes"` covers
`hermes` plus `hermes/subagent`, `hermes/cron`. `["*"]` omits the filter (whole
cross-harness corpus). pond's filter takes a single source: with several entries
the plugin forwards the first.

## v1 scoping

The tools expose **whatever the operator's pond store holds**. hermes has no
per-agent session-visibility SDK equivalent to OpenClaw's, so there is no
per-caller clamp to honor here - the operator chooses what pond ingests (its
adapters and sync configuration), and every enabled agent can recall it. If a
per-agent scoping need emerges it is a follow-up, not this version.

## Privacy model

- **Read-only by construction.** pond's MCP surface has no write path; the
  plugin cannot mutate the store or your sessions.
- **Local, zero egress (managed mode).** The managed child speaks MCP over
  stdio - no network listener, no port, no token. url mode is the only place a
  credential appears, and it is the operator's own endpoint/shim.
- **Size-bounded responses.** Every tool response is capped at 32 KB (then
  truncated with a note), so recall never floods the agent's context. With no
  memory slot, nothing is injected into prompts the agent did not ask for.
- **The operator can always read the store directly**, so scoping here is a
  usability boundary, not a security boundary against the operator - the same
  stance hermes takes toward its own sessions.

## Development

```bash
uv run --group dev pytest
```

Tests use no network and need no hermes install:

- `test_tools.py` - golden request/response for all four tools against a
  recording pond caller: query trim, limit capping, source_agent filter, byte
  budget, typed error relay, schema shape.
- `test_mcp_client.py` - the MCP handshake and tool-call seam over a fake
  transport, plus the JSON and SSE HTTP body parsers.
- `test_service.py` - `PondController` lifecycle against a stub pond binary
  (real subprocess): dial, call, connection reuse, idempotent stop, and the
  missing-binary error naming the fix.
- `test_setup.py` - the `hermes pond setup` allowlist writer against fake hermes
  config modules: new-platform seeding, append, idempotency, target selection.
- `test_plugin_load.py` - `register(ctx)` wires four tools under one toolset
  plus the `hermes pond` CLI command, against a `PluginContext` double matching
  hermes' real signatures.
