# pond

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg?style=flat-square)](LICENSE)

Your own small-scale data lake.

A unified storage and retrieval layer for sessions produced by any agentic client (Claude Code, Codex, OpenCode, Cursor, aider, ChatGPT, Gemini CLI, ...). One Rust binary, two deployments: a personal pond on your laptop, or a multi-tenant backend for hosted agent infrastructure. LanceDB on object storage. No SQL.

This repository is currently design-only. Implementation has not started.

## Table of Contents

- [Status](#status)
- [Background](#background)
- [Design](#design)
- [References](#references)
- [Contributing](#contributing)
- [License](#license)

## Status

Pre-implementation. The repository contains:

- `docs/design.md` - the locked-in v1 design (canonical types, six-or-four-dataset Lance schema, four seams, MCP surface).
- `docs/unresolved-questions.md` - decisions surfaced during the 2026-05-07 reference review that are not blocking but should be answered before schema lock.
- `docs/references/` - frozen snapshots of the upstream schemas pond's design draws from: opencode, kilocode, pi-mono, and the OpenTelemetry GenAI semantic conventions.

Once the unresolved questions are resolved, implementation will begin in Rust.

## Background

Every agentic CLI ships its own session format and its own search surface. Switching tools means losing history. Replaying a Claude Code session in OpenAI tooling means re-translating the wire shape by hand. Hosted multi-tenant deployments rebuild the same storage layer from scratch.

Pond is one Rust binary that ingests sessions from any source, stores them losslessly in a canonical Part union (modeled on `effect/unstable/ai`), and serves them via MCP for personal use or via a multi-tenant namespace layer for hosted operators. Storage, search (BM25 + vector + RRF), and replay across providers all sit on a single LanceDB-on-object-storage foundation.

Two day-1 use cases:

1. **Personal**: replace a per-tool knowledge base. Ingest local Claude Code sessions, search them semantically, replay through any provider.
2. **Hosted**: storage and search backend for multi-tenant agent deployments. Each namespace is an isolation boundary; the integrator owns identity, access, and routing.

See `docs/design.md` for the full rationale.

## Design

The design doc lives at [`docs/design.md`](docs/design.md). It is intentionally terse and locked - section 17 is the decision summary.

Key choices:

- Rust + tokio, single static binary.
- LanceDB as the only storage and search engine. No SQL.
- `object_store` crate as the only storage substrate (S3 / GCS / Azure / local fs).
- Four seams: `ObjectStorage`, `ReplayProvider`, `EmbeddingProvider`, `SourceAdapter`.
- Append-only domain events; replay = re-ingest, no `rebuild` verb.
- v1 surface = MCP server (`pond_search`, `pond_get`) plus out-of-band CLI verbs.
- Canonical types owned in pond, in the shape of `effect/unstable/ai` Prompt + Response unions. This is the moat.
- Multi-tenancy via bucket prefix per namespace; separate buckets when KMS isolation matters.
- Encryption is operational (bucket SSE + filesystem encryption), not application-level.

Open items that came out of the reference review are tracked in [`docs/unresolved-questions.md`](docs/unresolved-questions.md).

## References

`docs/references/` holds frozen snapshots of upstream schemas. Each subdirectory's README pins the source URL, the upstream commit, and the snapshot date.

| Path | Source | Why kept |
|------|--------|----------|
| `docs/references/opencode/` | github.com/anomalyco/opencode | Effect Schema canonical Part union; SDK types; storage schema. Closest existing model to pond's design. |
| `docs/references/kilocode/` | github.com/kilo-org/kilocode | Fork of opencode. Adds `editorContext`, plan-followup, kilocode-specific session events. |
| `docs/references/pi-mono/` | github.com/badlogic/pi-mono | Source of pond's leaf-cursor branching and cross-provider conformance test matrix. Also contains the silent-skip-malformed-line ingest pattern pond explicitly rejects. |
| `docs/references/otel-genai-semconv.md` | github.com/open-telemetry/semantic-conventions-genai | Synthesized GenAI semantic-conventions reference: attribute registry, span shapes, JSON schemas for input/output messages and tool definitions, metrics, value registries. |

To refresh a snapshot, see the maintenance instructions in [`docs/references/README.md`](docs/references/README.md).

## Contributing

Issues and pull requests are welcome. Because the project is pre-implementation, the most useful contributions right now are:

- Pressure on the unresolved questions in `docs/unresolved-questions.md`.
- Pointers to additional reference schemas worth snapshotting under `docs/references/`.
- Corrections to the design doc.

For larger changes, please open an issue first to discuss the direction.

## License

[Apache-2.0](LICENSE) (c) 2026 tenequm
