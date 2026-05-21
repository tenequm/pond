# Adapter Seam Hardening - Bounded Values, Streaming Ingest, Layered Adapters

## How to use this document

This is the committed-to design for three coupled pieces of work: making pond's storage path impossible to panic on oversized input, replacing the adapter read path with a bounded streaming reader, and reshaping the adapter seam so 10+ future adapters stay cheap to add. Every decision in Section 2 was settled in design review and is binding; the staged plan in Section 5 is how it gets built. Stages 1-3 are this work. Stage 4 is a documented future project, not scheduled here. Line and symbol references are snapshots - verify against current code before acting.

Status: design complete, implementation not started. The codex legacy-rollout fix (`docs/plans/codex-legacy-rollout-format.md`) is a separate, already-merged precursor.

## 1. Problem

`pond sync` aborts the process with `offset overflow` (Arrow `byte_array.rs`). Root cause: one codex `function_call_output.output` value of 3.4 GiB in a 4 GiB rollout file. Every pond text column is Arrow `Utf8` - a 32-bit `StringArray` whose offset buffer is `i32`, so no single value and no column may reach `i32::MAX` (about 2.0 GiB). `StringArray::from` is an infallible `From` impl; on overflow it panics, upstream of any `?`, so `messages_batch`'s error handling never runs.

The existing `cap_tool_output` (`src/adapter/codex_cli.rs`) caps the derived `ToolResult.result` Part but not the verbatim source row stored as `raw_record` inside `Message.options`. That second copy of the same 3.4 GiB blob overflows the `messages.options` column instead. The cap fixed one of two copies.

The invariant pond must hold: no value entering an Arrow `Utf8` column may approach `i32::MAX`, and - because a flush batches up to 100 sessions (`ADAPTER_FLUSH_BATCH`) - no column's per-batch byte sum may either. pond today has no guard before Arrow. It must detect oversize before Arrow sees it, never panic, recover the maximum salvageable data, and report the truncation.

## 2. Decisions (settled in review)

| # | Topic | Decision |
|---|---|---|
| - | Approach | Solution C: bounded size is an enforced property of the adapter seam, plus a runtime backstop. Adapter-local cap, storage-only guard, and early-skip were rejected as partial. |
| Q1 | Truncation model | In-place leaf truncation. A recursive walk truncates only the offending leaf string and preserves the rest of the record's structure. |
| Q2 | Ownership | The bounding logic lives in the seam and is inherited by every adapter. `raw_record` becomes seam-mediated rather than a raw `Value` stuffed into `options`. |
| Q3 | `Extracted<T>` shape | Stays deref-transparent. The size bound is a construction invariant enforced by the `extract_*` helpers - the same shape as `no-synthesis` (`spec.md#no-synthesis`). Consumers are unchanged; truncation is recorded out of band. |
| Q4 | Thresholds | Leaf cap 10 MiB, whole-record cap 32 MiB. Grounded in a real-corpus survey: largest legitimate single leaf 937 KB, largest legitimate whole record 10.25 MB. 32 MiB also matches Lance's historical page size. |
| Q5 | Read strategy | A-full: a streaming, string-capping pull-parser (`struson`) for any line over the record cap, so every good leaf is preserved and only the violating leaf is truncated. The normal path keeps plain `serde_json` - the streaming parser is cold code. |
| Q6 | Lance | Lance does not guard this (the i32 limit is an Arrow property); pond must guard before building the array. Lance's answer for large data is `LargeBinary` blob columns. Informational, no action. |
| Q7 | Recovery granularity | Never drop a recoverable row or break a session. Truncate only the cap-violating leaf, leaving a head-preserving `<pond:truncated N bytes>` marker. |
| Q8 | Blobs | `PartKind::File` data routes to the Lance blob column (`LargeBinary`, i64 offsets) and is exempt from the cap - the limit is a property of the text-column representation, not the data. The seam separates paths by `PartKind`. |
| - | Runtime backstop | A guard at `messages_batch` / `parts_batch` checks per-cell and cumulative-per-column bytes; on cumulative pressure it splits the batch; it returns a typed error, never panics. |
| - | Read architecture | The per-file read and parse run synchronously (`std::fs` + `struson`) on `spawn_blocking`, feeding `events_with` through a bounded `mpsc` channel (`blocking_send`). This replaces `tokio::fs`, which is itself `spawn_blocking` under the hood and benchmarks 25-64x slower than a plain sync read. Verified against the tokio repo and the std / async-book docs. |
| - | Adapter layering | A single `JsonlFormat` god-trait was rejected. An 8-platform fixture investigation found it fits 3 platforms and breaks 3 (claude_app, opencode, claude_managed_agents) - all four of its assumptions are violated by platforms pond already has fixtures for. The seam becomes three layers (Section 3). |
| - | `JsonlTreeSource` | The Layer-3 narrow generic is in scope for this work (Stage 2). |

## 3. Architecture - the three-layer seam

The eight intended source platforms diverge structurally - JSONL streams, JSON-array exports, fan-out one-file-per-object trees, metadata-plus-transcript file pairs, and live REST APIs. No single format trait can abstract "walk a tree", "page a REST API", and "fan in from three directories" without becoming a bag of escape hatches. The seam is therefore layered:

- **Layer 1 - shared core (composable helpers, not a trait).** The bounded streaming reader, the size guards, error attribution, the freshness gate (parameterized over a watermark - mtime for files, a server `updated_at` for REST), `raw_record` carrying, native-restore replay, and the rule-3 raw-carrier emitter (`spec.md#adapters`, placement rule 3). Every adapter calls these directly, regardless of transport. This removes duplication that exists between the two adapters today.

- **Layer 2 - the two-stage `RawRecord` pipeline (future, separate project).** Stage 1 emits a verbatim `RawRecord` and owns only transport and layout; a core-owned Stage 2 canonicalizer owns all canonical mapping. This is what truly delivers cheap 10+ adapters - the canonical schema stops appearing in adapter code, and a schema change stops being an N-adapter migration. It is a major change (a new `raw_records` dataset, a protocol change) and is out of scope here; see Section 8.

- **Layer 3 - one narrow generic, `JsonlTreeSource`.** Implements only the Layer-1 driver shape for the "walk a tree, one `.jsonl` per session, line equals record" family - codex_cli, pi, and top-level claude_code files (three callers, so the abstraction is justified per the minimalism floor). It does not own canonicalization or restore. claude_code keeps its subagent path hand-written. Fan-out (opencode) and REST (managed agents) adapters are written directly against Layer 1; a second fan-out or REST generic is promoted only when a second instance appears.

## 4. Constants

| Name | Value | Role |
|---|---|---|
| `LEAF_CAP` | 10 MiB | Maximum size of any single leaf string in a text column. ~10x the 937 KB observed legitimate maximum; equals today's `MAX_TOOL_OUTPUT_BYTES`. |
| `RECORD_CAP` | 32 MiB | Maximum size of one whole serialized record. 3x the 10.25 MB observed legitimate maximum; also the fast-path / slow-path read split point. |
| `COLUMN_BYTE_BUDGET` | 1 GiB | The runtime backstop splits a flush batch before any text column's running byte total would reach this - safely under the 2 GiB i32 wall. |
| truncation marker | `<pond:truncated N bytes>` | Appended after a head-preserving prefix when a leaf string is truncated; `N` is the original byte count. Only string leaves are truncated, so this single string form is the only sentinel needed. |

Constants are hard-coded, not config-tunable, until a second case demands otherwise. Blob columns (`PartKind::File` data, `LargeBinary`) are exempt from all of the above.

## 5. Staged implementation plan

Stages land in order; each is independently green (`cargo build`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`). Stages 2 and 3 are the two halves of Solution C and are tightly coupled - the Stage-2 reader's slow path calls the Stage-3 parser - so they may land as one change or two; Stage 1 ships first regardless because it is the standalone panic-proofing.

### Stage 1 - Runtime backstop

Goal: `pond sync` can never abort the process, even before the rest of the work lands.

- In `messages_batch` / `parts_batch` (`src/sessions.rs`), before each `StringArray::from`: track the running per-column byte total; when adding the next row would cross `COLUMN_BYTE_BUDGET`, split - flush the accumulated rows as one batch and start a fresh one. This makes the flush path chunk-aware (the batch builder, or its caller in the validator flush path, chunks `rows` by running byte estimate).
- A per-cell guard: a single serialized value at or above a safe per-cell ceiling returns a typed `pond::Error` rather than reaching Arrow. Once Stage 3 caps records at `RECORD_CAP` this is unreachable defense-in-depth, but it is the guarantee that nothing panics.
- Exit criteria: a sync over the 4 GiB pathological file completes without panicking - it may drop the offending session (until Stage 3 adds recovery), but the process survives and the sync summary reports it.

### Stage 2 - Layer 1 plus Layer 3: the streaming read core

Goal: replace the per-adapter `tokio::fs` + `BufReader::lines()` loops with one shared, synchronous, bounded streaming reader, built as the `JsonlTreeSource` generic.

- New seam module under `src/adapter/` holding Layer 1: the file walk (`collect_jsonl_files`), the freshness peek, line numbering, empty-line handling, and `AdapterError` attribution (`io` / `parse` / `schema`).
- The per-file read and parse run synchronously (`std::fs::File`) inside `tokio::task::spawn_blocking`; canonical events flow back to `events_with` through a bounded `tokio::sync::mpsc` channel via `blocking_send`, consumed as a `ReceiverStream`. The bounded channel supplies backpressure - the parser thread parks when the consumer lags, so memory stays bounded.
- The bounded line reader: per line, `(&mut reader).take(RECORD_CAP)` then `read_until(b'\n')`. A newline within `RECORD_CAP` is the fast path - plain `serde_json::from_str`. No newline within `RECORD_CAP` means an oversized line - the slow path (Stage 3).
- `JsonlTreeSource` (Layer 3) composes the above into the JSONL-tree adapter shape; codex_cli and claude_code (top-level files) adopt it and keep only `events_from_row` / `session_meta` / per-file state. This collapses today's three-opens-per-file (`peek_id_and_mtime`, `session_meta`, the read loop) to one.
- Exit criteria: both adapters ingest their fixture corpora unchanged; no `tokio::fs` in the adapter read path; the duplicated read loop exists once.

### Stage 3 - Seam-level bounded values

Goal: every value stored in a text column is bounded; oversized leaves are truncated in place; no session is ever broken.

- A seam bounding primitive: a recursive JSON walk that truncates any leaf string over `LEAF_CAP` to a head-preserving `<pond:truncated N bytes>` marker (UTF-8-boundary-safe), and rejects nothing.
- `extract_str` / `extract_value` / `extract_compact_repr` (`src/adapter/extract.rs`) apply the bound before `wrap`, so every `Extracted<T>` is bounded by construction - the invariant is enforced by the only constructors, transparently to consumers (Q3).
- `raw_record` becomes seam-mediated: a new bounded primitive produces an `Extracted<Value>`; `row_options` consumes it instead of a raw `&Value` (Q2).
- The struson streaming cap-parse for the slow path: for a line over `RECORD_CAP`, drive `struson` over `Cursor(buffered) .chain(reader)`, reading each string value through its incremental string reader - keep `LEAF_CAP` bytes, drain and discard the rest, continue. Because the parser consumes the whole oversized leaf, every leaf before and after it is parsed normally; only the violating leaf carries the marker (Q5, Q7).
- `cap_tool_output` and its constants are deleted - the seam now owns this (`spec.md#bounded-values`, new in Section 6).
- `PartKind::File` data is routed to the blob column and is not bounded (Q8).
- Observability: a `tracing::warn!` per truncation plus a `truncated_values` counter in `IngestSummary`.
- Exit criteria: the 4 GiB file ingests with its session intact and only the 3.4 GiB leaf truncated; a real `pond sync codex-cli` completes clean; `pond get` / `pond search` return that session's content.

### Stage 4 - Layer 2: the two-stage RawRecord pipeline (future, not scheduled)

The two-stage `RawRecord` design is the real answer to slim 10+ adapters. It is a major change with its own brief and is decided separately on its own merits. Recorded here as the architectural destination; Section 8.

## 6. Spec changes required

`docs/spec.md` is updated to make `bounded-values` a v1 invariant and to reconcile it with the two losslessness rules it qualifies. These are the proposed edits; apply after this plan is reviewed.

**Add to Section 6.4 ("The no-synthesis seam"), a new rule beside `transport-agnostic-seam`:**

> **`bounded-values`** {#bounded-values} - Every value an adapter places into a text column passes through the seam's size bound: a value whose encoding exceeds the substrate's per-value limit is truncated in place to a marked sentinel recording the original byte count, with the rest of the record preserved intact. The bound is a property of the seam's extractor helpers - an adapter cannot emit an unbounded value any more than it can emit a synthesized one. Binary payloads stored as blobs are exempt; the limit is a property of the text-column representation, not of the data. Why: the storage substrate cannot represent a text value at or beyond a hard size, so an unbounded value is not a large row but a process abort - bounding at the seam turns it into an attributable, recoverable truncation.

**Amend `native-restore-lossless` (Section 6.3) - append one clause:**

> ... A value truncated under `bounded-values` (Section 6) restores as its truncation sentinel, not its original bytes: a value the substrate physically cannot represent cannot round-trip, and the sentinel records the loss explicitly rather than hiding it.

**Amend `lossless-projection` (Section 4.8) - append one clause:**

> ... A field whose value exceeds the substrate's representable size is preserved as a truncation sentinel recording its original byte count (`bounded-values`, Section 6), not silently dropped - it remains a marked, attributable truncation, which `no-silent-drops` requires and which mere omission would violate.

No other spec section changes. The three-layer seam (Section 3) and the `RawRecord` pipeline are implementation structure and a future project respectively; neither is a v1 contract, so neither enters the spec now.

## 7. Validation

- `cargo build`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` green at every stage.
- Stage 1: a sync over the real 4 GiB rollout completes without panicking.
- Stage 3: that rollout's session ingests with messages intact and only the oversized leaf truncated; `pond get` and `pond search` return its content; a full `pond sync codex-cli` over the real corpus completes with zero `storage_errors`.
- A unit test for the bounding walk (oversized leaf in various positions - first, middle, last; nested; alongside good leaves) asserting every good leaf survives and only the violator carries the marker.
- A streaming-parse test driving `struson` over a synthetic oversized line (with a `#[cfg(test)]` small cap so no multi-GiB fixture is needed) asserting the same.
- Native-restore round-trip tests remain green for the existing fixture corpora.

## 8. Out of scope / deferred

- **Layer 2, the two-stage `RawRecord` pipeline.** The real "slim 10+ adapters" change - Stage 1 adapters emit verbatim `RawRecord`s, a core-owned canonicalizer derives canonical tables. A new `raw_records` dataset and a protocol change. Its own project and brief.
- **Fan-out and REST adapter generics.** opencode (fan-out tree) and claude managed agents (REST) are single instances today; their Stage-1 drivers are written directly against Layer 1. A `FanOutTree` or `RestSource` generic is promoted only when a second instance of either appears.
- **Config-tunable thresholds.** Hard constants until a second case demands tuning.

## 9. Open gaps (future-adapter, not blockers here)

These surfaced in the 8-platform fixture investigation and affect adapters not yet built; none blocks Stages 1-3.

- **nanoclaw `tool-results/*.txt` spill store.** The spilled tool-output files are not path-referenced from the JSONL; the correlation key (content hash? the epoch-ms in the filename?) is undetermined. nanoclaw has no `schema-notes.md`.
- **claude_app two session UUIDs.** A session carries both `sessionId` and `cliSessionId`; which becomes canonical `Session.id`, and how native restore reproduces both the metadata `.json` and the `audit.jsonl`, is an open design question.
- **opencode and openclaw** have no `schema-notes.md`; `step-start` / `step-finish` part mapping and `.reset.<timestamp>` file semantics are undetermined.
