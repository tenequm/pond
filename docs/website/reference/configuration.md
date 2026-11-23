---
description: Configure pond via config.toml with POND_* environment overrides - storage backend, credentials, adapters, opt-in embeddings, and sync schedule, resolved with flag > env > config precedence.
---

# Configuration & environment

Config lives in `config.toml` - `~/.config/pond/config.toml` on macOS and Linux, `%APPDATA%\pond\config.toml` on Windows (`$XDG_CONFIG_HOME` overrides both; `pond config path` prints the resolved location). Data, cache, and state live separately: `~/.local/share/pond`, `~/.cache/pond`, `~/.local/state/pond` on macOS and Linux, all three under `%LOCALAPPDATA%\pond` on Windows. Every field has a `POND_*` env override. Precedence: flag > env > config > ambient cloud SDK chain > default. `pond config show` prints resolved values with secrets redacted and per-field source.

## config.toml

```toml
[storage]
path = "s3+https://nbg1.your-objectstorage.com/my-pond"

[creds.default]
access_key_id     = "..."
secret_access_key = "..."

[adapters.claude-code]
enabled = true
```

An adapter's `path` accepts a single dir or an array - useful when one tool keeps several transcript trees (e.g. a relocated `CLAUDE_CONFIG_DIR` for a work subscription); every listed dir rides the same sync:

```toml
[adapters.claude-code]
enabled = true
path = ["~/.claude/projects", "~/work-claude/projects"]
```

## Embeddings

Search runs one arm per query: BM25 full-text (`fts`), or semantic (`vector`) over message embeddings. `fts` is the default and always available. Semantic search is opt-in and off unless you turn it on.

```toml
[embeddings]
enabled = true                        # default false
model = "intfloat/multilingual-e5-small"
dim = 384
```

With `enabled = false` no pond process downloads or loads a model, new messages are stored without vectors, index maintenance ignores the vector index, and a `vector` request is refused with a message naming this switch. With `enabled = true` messages embed inline at ingest, and `pond optimize --only embed` fills the backlog of anything ingested while it was off.

`POND_EMBEDDINGS_ENABLED=true|false` is the env mirror, which is how containers and CI turn it on without a config file. `true`/`false` (also `1`/`0`, `yes`/`no`); any other string fails config loading.

What it costs: with embeddings off a pond process sits around 100 MiB; once any vector work has run it costs roughly 500-900 MiB, plus a one-time 466 MiB model download into `$HOME/.cache/huggingface` and CPU-bound first syncs on hosts without Metal or CUDA.

### Mixed fleets

Machines sharing one store may disagree about this switch, and that is supported. Rows ingested by a machine with embeddings off stay un-embedded until a machine with embeddings on runs `pond optimize --only embed`; `pond sync` never probes for that backlog. A machine with embeddings on shows the pending rows in `pond status -v`.

Disagreeing about the pond *version* is a different matter and is not supported - see [mixed versions](/guides/several-machines#mixed-versions).

### Turning it back off

An instance with embeddings off leaves an existing vector index alone: it neither folds nor rebuilds it, and `pond optimize --rebuild` skips it. To reclaim the space, drop it explicitly:

```sh
pond optimize --drop-index messages_vector_ivfpq
```

The stored vectors themselves stay as data columns, so re-enabling later picks up where it left off.

## Environment

- `POND_STORAGE_PATH` - storage URL (overrides `[storage].path`)
- `POND_CONFIG_FILE` - config path
- `POND_CREDS_DEFAULT_ACCESS_KEY_ID` / `POND_CREDS_DEFAULT_SECRET_ACCESS_KEY` - default creds
- `POND_EMBEDDINGS_ENABLED` - `true` or `false`, turns semantic search on or off (overrides `[embeddings].enabled`)

`POND_STORAGE_PATH` plus the two creds vars is a complete configuration with no file.

## Secrets

Keep secrets out of the URL and CLI flags. Instead of inline values: `secret_access_key_file = "/run/secrets/pond-s3"` or `secret_access_key_command = "op read op://vault/pond/secret"`. pond writes `config.toml` with `0600` permissions on macOS and Linux; on Windows the file inherits the profile directory's ACLs.
