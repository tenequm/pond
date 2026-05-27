# pond

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg?style=flat-square)](LICENSE)

Pond keeps every AI conversation you've ever had intact and searchable, and lets you continue any of them in any supported tool. Your history, your search, your sessions - independent of the agent vendor that made them.

One Rust binary that ingests sessions from any agentic client (Claude Code, Codex, and more on the roadmap) into a canonical Session / Message / Part interlingua, stores them in Lance on object storage, and serves hybrid search over them via HTTP+JSON and MCP. Two deployments: a personal pond on your laptop, or a multi-tenant backend for hosted agent infrastructure. No SQL, no extra database, no wrapper around Lance.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Design](#design)
- [References](#references)
- [Contributing](#contributing)
- [License](#license)

## Background

Every agentic CLI ships its own session format and its own search surface. Switching tools means losing history. Replaying a Claude Code session in another provider's tooling means re-translating the wire shape by hand. Hosted multi-tenant deployments rebuild the same storage layer from scratch.

Pond is the storage and retrieval layer that sits underneath. Every adapter is a bidirectional codec between a client format and one canonical schema, so any session can be restored by any adapter - it need not return to the client that produced it. Storage, hybrid search (BM25 + vector, score-normalized fusion), and provider-agnostic replay all sit on a single Lance-on-object-storage foundation.

Pre-v1. The crate builds clean and the v1 surface is in place: full CLI, HTTP+JSON and MCP transports, hybrid search over three Lance datasets, `intfloat/multilingual-e5-small` embeddings at FP16 weights (Metal on macOS, CUDA opt-in, CPU fallback), and local-FS / S3 / GCS / Azure backends through Lance's `object_store` integration. Schemas, wire shapes, and config keys are subject to breaking change until v1. See [`docs/spec.md`](docs/spec.md) for the locked-in specification.

## Install

Linux and macOS are supported; Windows is not in v1 scope.

**macOS and Linux (Homebrew):**

```sh
brew install tenequm/tap/pond
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

Ingest sessions from local sources, embed them, and search:

```sh
pond sync
pond embed
pond search "how did we wire up the OCC retry loop"
```

Run a server (HTTP + MCP on the same binary):

```sh
pond serve            # HTTP+JSON on 127.0.0.1, MCP route mounted alongside
pond mcp              # MCP over stdio, for direct agent integration
```

Fetch a single session or message, or export the whole pond as canonical ingest events:

```sh
pond get --session-id <id>
pond export > snapshot.jsonl
```

Index maintenance is operator-triggered (writes never fold indexes; a trailing index returns complete results, just slower):

```sh
pond index status
pond index optimize --wait
pond index rebuild <intent>     # escape hatch for tokenizer-config changes
```

`pond status` reports row counts, embedding coverage, and index health. `pond search --explain` returns Lance's `analyze_plan` output for each retrieval arm.

## Design

The full contract is in [`docs/spec.md`](docs/spec.md). Key choices:

- **Lance direct, no wrapper.** The `lance-format/lance` crates are the only storage and search engine. No `lancedb`, no SQL, no parallel abstraction. Storage, indexing, OCC, schema evolution, blob columns, versioning, and time-travel are all Lance.
- **Canonical Session / Message / Part interlingua.** Owned in pond, in the shape of Effect v4's `Prompt`-side Part union. This schema is pond's product; everything else is machinery around it.
- **Three Lance datasets** (`sessions`, `messages`, `parts`). `messages` carries the nullable embedding (`vector` + `embedding_model`) alongside denormalized filter columns (`source_agent` / `project` / `role` / `timestamp`) for single-stage filter pushdown.
- **No-synthesis adapter seam.** Adapters parse source records through extractor helpers that make "invent a value" a compile error - `no-synthesis`, `schema-honesty`, and `provenance-required` are structural, not review rules.
- **Index lifecycle decoupled from writes.** Writes commit data without folding indexes. Operators run `pond index optimize` on their own cadence; Lance merges index results with a flat scan over unindexed fragments, so reads stay correct.
- **Score-normalized hybrid fusion.** Per-arm shaping (max-norm BM25 for FTS, rank-norm for vector), min-max to [0, 1], then weighted sum. Session-root-keyed dedup so cross-arm agreement compounds at the conversation level.
- **Language-neutral full-text.** Character `ngram` tokenizer (3-5), no monolingual stemmer - pond indexes sessions in any language alike.
- **Two transports, one handler set.** HTTP+JSON (axum) and MCP (rmcp) both dispatch into the same handlers. Wire ops: `pond_search`, `pond_get`, `pond_ingest`, `pond_session_events`. MCP also exposes `schema://pond` and `stats://pond` resources.
- **Opaque-string multi-tenancy.** Each tenant is a `namespace` string the integrator supplies; pond does not authenticate, authorize, or model identity. The object store's IAM is the storage boundary.
- **Encryption is operational.** Bucket SSE plus filesystem encryption; pond holds no keys and adds no application-level crypto.

## References

`docs/references/` holds frozen snapshots of upstream schemas; real session captures live under `tests/fixtures/adapter/`. Each subdirectory's README pins the source URL, the upstream commit, and the snapshot date.

| Path | Source | Why kept |
|------|--------|----------|
| `docs/references/effect/` | github.com/Effect-TS/effect | Effect v4 Prompt/Response Part unions. Pond's canonical types copy this shape. |
| `docs/references/opencode/` | github.com/sst/opencode | Effect Schema canonical Part union; SDK types; storage schema. |
| `docs/references/kilocode/` | github.com/kilo-org/kilocode | OpenCode fork. Adds `editorContext`, plan-followup, kilocode-specific events. |
| `docs/references/pi-mono/` | github.com/badlogic/pi-mono | Leaf-cursor branching and cross-provider conformance test matrix. |
| `docs/references/otel-genai-semconv.md` | github.com/open-telemetry/semantic-conventions-genai | GenAI semantic conventions. Inspiration for shape overlap; pond does not derive from OTel. |
| `docs/references/anthropic-managed-agents.pdf` | Anthropic | Session-as-event-log framing for managed agents. |
| `docs/references/recursive-language-models-study-2512.24601v3.pdf` | arXiv 2512.24601 | Long context as a queryable environment; recursion as sub-agent spawning - corroborates the linked-Sessions branching model. |
| `tests/fixtures/adapter/` | local captures | Real session captures for eight source harnesses (claude_code, claude_app, claude_managed_agents, codex_cli, opencode, openclaw, nanoclaw, pi). Drives adapter design and serves as SourceAdapter test fixtures. |

## Contributing

Issues and pull requests are welcome. The most useful contributions right now:

- Spec feedback on [`docs/spec.md`](docs/spec.md).
- Pointers to additional reference schemas or session samples worth snapshotting under `docs/references/`.
- Bug reports against the v1 surface (CLI verbs, wire ops, schema mismatches, OCC behavior, object-store backends).

For larger changes, open an issue first to discuss the direction.

## License

[Apache-2.0](LICENSE) (c) 2026 tenequm
