# pond

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg?style=flat-square)](LICENSE)

Your own small-scale data lake.

A unified storage and retrieval layer for sessions produced by any agentic client (Claude Code, Codex, OpenCode, Cursor, aider, ChatGPT, Gemini CLI, ...). One Rust binary, two deployments: a personal pond on your laptop, or a multi-tenant backend for hosted agent infrastructure. Lance file format on object storage. No SQL.

## Table of Contents

- [Status](#status)
- [Background](#background)
- [Design](#design)
- [References](#references)
- [Contributing](#contributing)
- [License](#license)

## Status

Pre-v1. The Rust crate builds clean and the v1 surface is in place: eight CLI verbs, HTTP+JSON and MCP transports, hybrid search (BM25 + vector + RRF) over four Lance datasets, Qwen3 embeddings via fastembed-rs, and local-FS / S3 / GCS / Azure backends through Lance's `object_store` integration. A background maintenance loop runs `cleanup_old_versions` + `optimize_indices` on a configurable interval. Schemas, wire shapes, and config keys are subject to breaking change until v1.

Repository layout:

- `src/` - the pond Rust crate (modules: `adapter/`, `embed/`, `config`, `handlers`, `sessions`, `substrate`, `transport`, `wire`).
- `docs/design.md` - the locked-in v1 design (sections 1-4 are the source of truth; section 5 is empty).
- `docs/references/` - frozen snapshots of the upstream schemas pond's design draws from.
- `tests/` - integration tests; `tests/fixtures/session-samples/` holds real session captures from eight source harnesses, used as SourceAdapter test fixtures.
- `docs/archive/` - historical design notes and the resolved open-questions log.

## Background

Every agentic CLI ships its own session format and its own search surface. Switching tools means losing history. Replaying a Claude Code session in another provider's tooling means re-translating the wire shape by hand. Hosted multi-tenant deployments rebuild the same storage layer from scratch.

Pond is one Rust binary that ingests sessions from any source, stores them losslessly in a canonical Part union (modeled on Effect v4's `Prompt`-side types), and serves them via HTTP+JSON or MCP. Storage, hybrid search (BM25 + vector + RRF), and provider-agnostic replay all sit on a single Lance-on-object-storage foundation.

Two day-1 use cases:

1. **Personal**: replace a per-tool knowledge base. Ingest local Claude Code sessions, hybrid-search them, retrieve them for replay.
2. **Hosted**: storage and search backend for multi-tenant agent deployments. Each namespace is an opaque-string isolation boundary; the integrator owns identity, access, and routing.

See `docs/design.md` for the full rationale.

## Design

The design doc lives at [`docs/design.md`](docs/design.md). Sections 1-4 are the source of truth.

Key choices:

- Rust + tokio, single static binary.
- `lance-format/lance` crates direct as the only storage and search engine. No `lancedb` wrapper, no SQL, no additional database.
- `object_store` (via Lance) for storage substrate: S3 / GCS / Azure / local filesystem.
- Canonical session types owned in pond, in the shape of Effect v4's `Prompt`-side Part union. This is the moat. Response-side metadata is projected into per-Message Lance columns, not stored as Parts.
- Four Lance datasets: `sessions`, `messages`, `parts`, `embeddings`. Hot filter columns are denormalized onto search rows for single-stage filter pushdown (`messages` and `embeddings` carry `source_agent` / `project` / `role` / `timestamp` for prefilter on hybrid search).
- One adapter trait, `SourceAdapter`, with a deterministic event-ordering contract. Everything else (storage, indexing, OCC, time-travel, namespaces, manifest versioning, blob storage) is Lance direct - no extra "seam" abstractions.
- Append-only writes. Replay (cross-provider re-projection) is deferred to section 4.
- v1 surface: two transports - HTTP+JSON (`POST /v1/<op>` plus SSE) and MCP (rmcp), wrapping the same handlers. Operations: `pond_search`, `pond_get`, `pond_ingest`, `pond_session_events`. CLI verbs out of band: `pond status`, `pond sync`, `pond embed`, `pond serve`, `pond mcp`, `pond maintenance`, `pond config`, `pond export`.
- Default embeddings: Qwen3-Embedding-0.6B via fastembed-rs (local, candle backend behind the `qwen3` feature, fixed 1024-dim, 32K context, Apache 2.0). Embedding registry is config-driven.
- Multi-tenancy via opaque namespace strings; bucket prefix per namespace; separate buckets when KMS isolation matters.
- Encryption is operational (bucket SSE + filesystem encryption), not application-level.

## References

`docs/references/` holds frozen snapshots of upstream schemas; real session samples live under `tests/fixtures/session-samples/`. Each subdirectory's README pins the source URL, the upstream commit, and the snapshot date.

| Path | Source | Why kept |
|------|--------|----------|
| `docs/references/effect/` | github.com/Effect-TS/effect | Effect v4 Prompt/Response Part unions. Pond's canonical types copy this shape. |
| `docs/references/opencode/` | github.com/sst/opencode | Effect Schema canonical Part union; SDK types; storage schema. |
| `docs/references/kilocode/` | github.com/kilo-org/kilocode | OpenCode fork. Adds `editorContext`, plan-followup, kilocode-specific events. |
| `docs/references/pi-mono/` | github.com/badlogic/pi-mono | Leaf-cursor branching and cross-provider conformance test matrix. |
| `docs/references/lancedb/` | github.com/lancedb + github.com/lance-format | Capability snapshot and evolution timeline for Lance + LanceDB. |
| `docs/references/otel-genai-semconv.md` | github.com/open-telemetry/semantic-conventions-genai | GenAI semantic conventions. Inspiration for shape overlap; pond does not derive from OTel. |
| `docs/references/anthropic-managed-agents.pdf` | Anthropic | Session-as-event-log framing for managed agents. |
| `tests/fixtures/session-samples/` | local captures | Real session captures for eight source harnesses (claude-code, claude-app, claude-managed-agents, codex, opencode, openclaw, nanoclaw, pi). Drives adapter design, stress-tests the schema, and serves as SourceAdapter test fixtures. |

To refresh a snapshot, see the maintenance instructions in [`docs/references/README.md`](docs/references/README.md).

## Contributing

Issues and pull requests are welcome. The most useful contributions right now are:

- Design feedback on `docs/design.md`.
- Pointers to additional reference schemas or session samples worth snapshotting under `docs/references/`.
- Bug reports against the v1 surface (CLI verbs, wire ops, schema mismatches, OCC behavior, object-store backends).
- Corrections to the design doc.

For larger changes, please open an issue first to discuss the direction.

## License

[Apache-2.0](LICENSE) (c) 2026 tenequm
