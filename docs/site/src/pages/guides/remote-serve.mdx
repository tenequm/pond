---
description: Run pond serve as a long-lived read side on a host you control - point it at the bucket without a config file, name the host for /mcp, put auth in front, give it a writable cache, and stop it cleanly under a supervisor.
---

# Run a remote serve

pond splits into two sides. Sync runs on the machines where the agents run, because that is where the session files are, and it writes into the bucket. `pond serve` is the read side: search and MCP over HTTP against the same bucket, from one long-running process with a warm cache. This guide runs that process on a host you control - a VPS, a container, a PaaS - and points your agents at it. Nothing here is specific to one platform.

`serve` reads the store and answers queries. It does not authenticate callers, and it does not sync unless told to (`--with-sync` needs the session files on the same machine, so it belongs on an agent host, not on a hosted read side).

::::steps

## Point it at the bucket

Env is a complete configuration - no `config.toml`, no `pond init`:

```sh
POND_STORAGE_PATH=s3+https://nbg1.your-objectstorage.com/my-pond
POND_CREDS_DEFAULT_ACCESS_KEY_ID=...
POND_CREDS_DEFAULT_SECRET_ACCESS_KEY=...
```

Same URL forms and credential rules as [Remote storage](/guides/remote-storage). Add `POND_EMBEDDINGS_ENABLED=true` only if the store carries embeddings and you want vector search from this host; it loads a ~500 MB model on the first vector query.

## Bind and name the host

```sh
POND_HOST=0.0.0.0
POND_PORT=9797
POND_ALLOWED_HOSTS=pond.example.com
```

The default bind is loopback, so a supervised process must ask for `0.0.0.0` (or `::`); `serve` logs a notice when it does. `POND_ALLOWED_HOSTS` matters more than it looks: the `/mcp` route validates the `Host` header against an allowlist (the MCP spec's DNS-rebinding defence) and that list is loopback only, so a server reached by any other name answers every MCP request with `403 Forbidden: Host header is not allowed` while `/v1/*` keeps working. Name every hostname clients will use, comma-separated; loopback stays allowed. A name without a port matches any port.

## Put auth in front

`serve` has no authentication of its own, and the store behind it is your whole session history. Never expose it bare. Put something that checks a credential in front: a reverse proxy with a bearer-token check, a Tailscale or WireGuard network, or your platform's private-app token gate. Terminate TLS there too; `serve` speaks plain HTTP.

## Give it a writable cache

`serve` keeps a search row map on disk and refreshes it while running. It lands under `$XDG_CACHE_HOME/pond`, else `$HOME/.cache/pond`, else `./.pond-cache`. A process with no writable home should set the variable explicitly:

```sh
XDG_CACHE_HOME=/var/cache
```

If the cache cannot be written, `serve` still starts and logs a warning, but every query pays the cold load instead of the warm one. With embeddings enabled, the model cache follows `HF_HOME` (default `~/.cache/huggingface`) the same way.

## Stop it cleanly

`serve` stops on SIGTERM as well as ctrl-c. It closes live MCP sessions first (agents hold their `/mcp` stream open for the life of the session) and bounds the drain to five seconds, so a supervisor's stop finishes in milliseconds instead of timing out into SIGKILL. Give it any grace period of five seconds or more.

::::

## Connect a client

```sh
claude mcp add -s user --transport http pond https://pond.example.com/mcp --header "Authorization: Bearer <token>"
codex mcp add pond --url https://pond.example.com/mcp
```

Drop the header if your gate is a private network rather than a token. The skill still installs locally - `pond skill > ~/.claude/skills/pond/SKILL.md` - so the agent knows how to use the tools it now reaches remotely.

## Check it

From anywhere the gate lets through:

```sh
curl -sS -X POST https://pond.example.com/v1/search -H 'content-type: application/json' -d '{"protocol_version":1,"query":"anything"}'
curl -sS -o /dev/null -w '%{http_code}\n' -X POST https://pond.example.com/mcp -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"check","version":"0"}}}'
```

The first returns a search envelope; the second prints `200`. A `403` on the second with a `200` on the first means the hostname is missing from `POND_ALLOWED_HOSTS`.

## Worked example: Dockhold

[Dockhold](https://dockhold.eu) builds the `Dockerfile` in a repo and puts it on an always-on HTTPS URL, with no servers to manage and hosting in the EU. Always-on is the part that earns its place here: the warm row map and the reused S3 client are exactly what make one long-lived `serve` faster than many cold ones, and a host that sleeps an idle app throws both away. Worth checking against whatever host you pick.

The whole repo is one `Dockerfile`:

```dockerfile
FROM debian:bookworm-slim
ARG POND_VERSION=latest
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl xz-utils; \
    case "$(dpkg --print-architecture)" in \
      amd64) target=x86_64-unknown-linux-gnu ;; \
      arm64) target=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported architecture" >&2; exit 1 ;; \
    esac; \
    if [ "$POND_VERSION" = "latest" ]; then \
      base=https://github.com/tenequm/pond/releases/latest/download; \
    else \
      base="https://github.com/tenequm/pond/releases/download/v${POND_VERSION}"; \
    fi; \
    curl -fsSL "${base}/pond-${target}.tar.xz" | tar -xJ -C /usr/local/bin pond; \
    apt-get purge -y --auto-remove curl xz-utils; \
    rm -rf /var/lib/apt/lists/*

ENV XDG_CACHE_HOME=/tmp/pond-cache
CMD exec pond serve --host 0.0.0.0 --port ${PORT:-9797}
```

Three things in that `CMD`. The port is assigned at runtime, so it is shell form, to expand `$PORT`. `exec` matters as much: without it the shell stays PID 1, the platform's stop signal never reaches pond, and the clean stop above turns back into a kill. And the container runs as an arbitrary uid with no writable home, which is what `XDG_CACHE_HOME` is for.

Connect the repo, then set four variables. The bucket URL is plain configuration; the two credentials go in the Vault, which encrypts them and injects them at runtime:

| Variable | Where |
| --- | --- |
| `POND_STORAGE_PATH` | Variables |
| `POND_CREDS_DEFAULT_ACCESS_KEY_ID` | Vault |
| `POND_CREDS_DEFAULT_SECRET_ACCESS_KEY` | Vault |
| `POND_ALLOWED_HOSTS` | Variables, on a second pass |

`POND_ALLOWED_HOSTS` comes second because the served hostname only exists once the app does. Deploy, read the hostname off the dashboard, set it, let it roll out again. In between the app shows exactly the split described above: `/v1/search` answers and every `/mcp` request is a 403.

For the gate, turn the app private. Every request then needs a token at the edge and gets a `401` without one, which covers "put auth in front" without running a proxy of your own. The boundary that creates is worth stating plainly: requests terminate TLS at the platform before they reach pond, and the token belongs to the app rather than to anything pond knows about.

`PORT` is set by the platform, so read it and never define it. The filesystem is ephemeral, which suits a read side whose data lives in the bucket, though it does mean the cache starts cold after every deploy until the row map is warm again.
