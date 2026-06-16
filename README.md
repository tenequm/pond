# pond

[![CI](https://img.shields.io/github/actions/workflow/status/tenequm/pond/ci.yml?branch=main&style=flat-square)](https://github.com/tenequm/pond/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/pond-db.svg?style=flat-square)](https://crates.io/crates/pond-db)
[![docs](https://img.shields.io/badge/docs-pond.locker-blue?style=flat-square)](https://pond.locker/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg?style=flat-square)](LICENSE)

Lossless storage and hybrid search for AI agent sessions, across every agentic client.

**Quickstart.** Install, run guided setup, and ingest your local sessions:

```sh
brew install tenequm/tap/pond
pond init   # guided setup: storage, adapters, MCP registration, optional schedule
pond sync   # ingest, embed, update indexes - every enabled adapter
```

`pond init` registers pond as an MCP server for detected clients; to add it by hand:

```sh
claude mcp add -s user pond -- pond mcp   # Claude Code
codex mcp add pond -- pond mcp            # Codex
```

Pond keeps every AI conversation you've ever had intact and searchable, and lets you continue any of them in any supported tool - your history, your search, your sessions, independent of the agent vendor that made them. It is one Rust binary that ingests sessions from registered agentic-client adapters into a canonical Session / Message / Part interlingua, stores them in Lance on object storage, and serves hybrid search over them via HTTP+JSON and MCP. Two deployments: a personal pond on your laptop, or a multi-tenant backend for hosted agent infrastructure. No extra database, no wrapper around Lance.

Current automatically synced agent clients:
- Claude Code CLI
- Claude desktop app (local agent mode)
- Codex CLI
- opencode CLI
- pi-coding-agent CLI

You can also import a Claude.ai data export with the `claude-ai-export` adapter - a manual download, so it is not auto-discovered: `pond sync claude-ai-export --path <path>`.

Status: pre-v1. Schemas, wire shapes, and config keys are subject to breaking change until v1. Full documentation lives at [pond.locker](https://pond.locker/); the contract is [`docs/spec.md`](docs/spec.md).

## Background

Every agentic CLI ships its own session format and its own search surface. Switching tools means losing history. Replaying a Claude Code session in another provider's tooling means re-translating the wire shape by hand. Hosted multi-tenant deployments rebuild the same storage layer from scratch.

Pond is the storage and retrieval layer that sits underneath. Every adapter is a bidirectional codec between a client format and one canonical schema, so any session can be restored by any adapter - it need not return to the client that produced it. Storage, hybrid search (BM25 + vector, score-normalized fusion), and provider-agnostic replay all sit on a single Lance-on-object-storage foundation.

The v1 surface includes: full CLI, HTTP+JSON and MCP transports, hybrid search over three Lance datasets, `intfloat/multilingual-e5-small` embeddings at FP16 weights (Metal on macOS, CUDA opt-in, CPU fallback), and local-FS / S3 / GCS / Azure backends through Lance's `object_store` integration.

## Install

Linux and macOS are supported; Windows is not in v1 scope.

**Package Managers (macOS and Linux):**

```sh
brew install tenequm/tap/pond                       # Homebrew
nix profile add github:tenequm/pond-nix#pond        # Nix
cargo install pond-db                               # crates.io (installs the `pond` command)
```

**Build from source:**

```sh
git clone https://github.com/tenequm/pond.git
cd pond
cargo install --path .
```

For CUDA acceleration on Linux:

```sh
cargo install --path . --features cuda
```

On macOS the Metal backend is selected automatically; on other systems the CPU fallback runs without extra features.

## Usage

Set up storage, adapters, MCP registration, and an optional sync schedule in one pass (idempotent - re-run it any time to repair or update):

```sh
pond init
```

Then import sessions from local adapters, embed them, update indexes, and search:

```sh
pond sync
pond search "how did we wire up the OCC retry loop"
```

Run a server:

```sh
pond serve                         # HTTP on 127.0.0.1:9797
pond serve --transport stdio       # MCP over stdio
pond mcp                           # alias for stdio MCP
```

Fetch a single session or message, or move a whole corpus:

```sh
pond get --session-id <id>
pond copy --from local --to snapshot.pond
pond copy --from snapshot.pond --to local
```

Ask structured questions with read-only SQL (the same surface as the `pond_sql_query` MCP tool):

```sh
pond sql "SELECT project, count(*) FROM messages GROUP BY project ORDER BY 2 DESC"
```

Import alone, then run maintenance stages independently when needed:

```sh
pond sync --no-optimize
pond optimize --only embed
pond optimize --only index
```

Keep pond current automatically (launchd on macOS, systemd user timers or cron on Linux):

```sh
pond schedule start                # every 5m by default (--every 15m|1h|6h|1d)
pond schedule status
pond schedule logs
```

`pond status` prints a per-table storage table, then `indexes` (text/semantic readiness), `stored` (sessions + searchable messages), and `adapters` (configured adapter count). Pass `--adapters` for per-project tables and per-intent index detail. `pond search --explain` returns Lance's `analyze_plan` output for each retrieval arm.

### Remote storage

By default pond stores data locally under `$XDG_DATA_HOME/pond`. To use an object store, add credentials and switch the destination:

```sh
pond creds add                                                    # interactive: name, access key, hidden secret
pond storage use s3+https://nbg1.your-objectstorage.com/my-pond   # probe end-to-end, then flip [storage].path
pond storage check                                                # verify: parse, creds, conditional-put (OCC), write/read/delete
```

`pond init --storage-path <url>` configures a remote destination during setup and prompts for credentials inline when the destination is remote, so a bucket is one command. The `s3+https://host/bucket` form works for any S3-compatible store (Hetzner, R2, B2, MinIO); `s3://`, `gs://`, and `az://` use the standard cloud SDK credential chain when no `[creds.*]` set matches. `pond copy --from <local> --to <url>` carries existing local data into the bucket - idempotent, never deletes the source, and on completion it rebuilds the destination indexes and verifies every row landed (exit 6 if any are missing, so you never reconcile by hand). `pond copy --verify-only --from <local> --to <url>` runs that same check read-only, without copying. Full walkthrough: [pond.locker](https://pond.locker/).

### Configuration

`pond init` walks through everything below interactively and enables the adapters it finds. `pond sync` only ingests already-enabled adapters - enabling one is an explicit step (`pond adapters enable` / `pond adapters discover` / `pond init`), never a side effect of sync. Config lives under `$XDG_CONFIG_HOME/pond/`. Every `[adapters.<name>]` block needs `enabled = true` to be active; sections without it (or with `enabled = false`) are skipped.

```toml
[adapters.claude-code]
enabled = true
path = "~/.claude/projects"

[adapters.codex-cli]
enabled = false                    # kept in config, skipped on `pond sync`
path = "~/.codex/sessions"
```

### Verbosity

Root-level `-v` / `-vv` / `-vvv` raise the tracing level (info / debug / trace); `-q` / `-qq` lower it. The default surfaces warnings only. `RUST_LOG` overrides the CLI flag when set; `POND_LOG` is no longer honored.

## Design

The full contract is in [`docs/spec.md`](docs/spec.md). Key choices:

- **Lance direct, no wrapper.** The `lance-format/lance` crates are the only storage and search engine. No `lancedb`, no parallel abstraction. Storage, indexing, OCC, schema evolution, blob columns, versioning, and time-travel are all Lance. The read-only `pond sql` surface is DataFusion planning over the same Lance datasets - a query escape hatch, not a second engine.
- **Canonical Session / Message / Part interlingua.** Owned in pond, in the shape of Effect v4's `Prompt`-side Part union. This schema is pond's product; everything else is machinery around it.
- **Three Lance datasets** (`sessions`, `messages`, `parts`). `messages` carries the nullable embedding (`vector` + `embedding_model`) alongside denormalized filter columns (`source_agent` / `project` / `role` / `timestamp`) for single-stage filter pushdown.
- **No-synthesis adapter seam.** Adapters parse source records through extractor helpers that make "invent a value" a compile error - `model-no-synthesis`, `model-schema-honesty`, and `adapter-provenance-required` are structural, not review rules.
- **Index lifecycle decoupled from writes.** Writes commit data without folding indexes. `pond sync` runs index maintenance by default, and `pond optimize --only index` runs it on demand; Lance merges index results with a flat scan over unindexed fragments, so reads stay correct.
- **Score-normalized hybrid fusion.** Per-arm shaping (max-norm BM25 for FTS, rank-norm for vector), min-max to [0, 1], then weighted sum. Session-root-keyed dedup so cross-arm agreement compounds at the conversation level.
- **Language-neutral full-text.** Character `ngram` tokenizer (3-5), no monolingual stemmer - pond indexes sessions in any language alike.
- **Two transports, one handler set.** HTTP+JSON (axum) and MCP (rmcp) both dispatch into the same handlers. Wire ops: `pond_search`, `pond_get`, `pond_ingest`. MCP additionally exposes the read-only `pond_sql_query` tool and the `schema://pond`, `schema://pond-sql`, and `stats://pond` resources.
- **Opaque-string multi-tenancy.** Each tenant is a `namespace` string the integrator supplies; pond does not authenticate, authorize, or model identity. The object store's IAM is the storage boundary.
- **Encryption is operational.** Bucket SSE plus filesystem encryption; pond holds no keys and adds no application-level crypto.

## References

The upstream schemas that shaped pond's canonical model are documented in [`docs/references/`](docs/references/) (source URLs + why each matters; the vendored code itself is not redistributed). Real session captures live under `tests/fixtures/adapter/`.

| Source | Why it matters |
|--------|----------------|
| [Effect-TS/effect](https://github.com/Effect-TS/effect) | Effect v4 Prompt/Response Part unions. Pond's canonical types copy this shape. |
| [sst/opencode](https://github.com/sst/opencode) | Effect Schema canonical Part union; SDK types; storage schema. |
| [kilo-org/kilocode](https://github.com/kilo-org/kilocode) | OpenCode fork. Adds `editorContext`, plan-followup, kilocode-specific events. |
| [badlogic/pi-mono](https://github.com/badlogic/pi-mono) | pi-coding-agent leaf-cursor branching and cross-provider conformance test matrix. |
| [open-telemetry/semantic-conventions-genai](https://github.com/open-telemetry/semantic-conventions) | GenAI semantic conventions. Inspiration for shape overlap; pond does not derive from OTel. |
| `tests/fixtures/adapter/` | Real session captures for nine source harnesses (claude_ai_export, claude_code, claude_desktop_app, claude_managed_agents, codex_cli, nanoclaw, openclaw, opencode, pi-coding-agent). Drives adapter design and serves as adapter test fixtures. |

## Contributing

Issues and pull requests are welcome. The most useful contributions right now:

- Spec feedback on [`docs/spec.md`](docs/spec.md).
- Pointers to additional reference schemas or session samples worth documenting under `docs/references/`.
- Bug reports against the v1 surface (CLI verbs, wire ops, schema mismatches, OCC behavior, object-store backends).

For larger changes, open an issue first to discuss the direction. For security issues, see [SECURITY.md](.github/SECURITY.md).

## License

[Apache-2.0](LICENSE) (c) 2026 tenequm
