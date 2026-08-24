---
name: add-adapter
description: Playbook for adding a new source-agent adapter to pond - spec the format from the upstream writer, capture a sandboxed fixture, implement the bidirectional codec, and prove conformance. Use when adding an adapter under packages/pond/src/adapter/ or reworking an existing one.
---

# Adding a pond adapter

An adapter is a bidirectional codec between one agent's native session format and pond's canonical schema (docs/spec.md section 6). The bar it must meet: lossless capture (every source field recoverable), no synthesized values, additive re-sync, and a restore face that is either lossless-native or an honest refusal. The registry cost is fixed and small: one file under `packages/pond/src/adapter/` plus its wiring in `src/adapter/mod.rs` - a module declaration, a re-export, and the `registry()` entry (spec 6.7).

The work splits into two phases. Phase A produces the spec doc and the fixture; Phase B implements from them. Ship both in ONE self-contained PR: spec doc + fixture + adapter + tests + doc rows. Opening a draft PR after Phase A, so the decision table gets reviewed before implementation, is recommended but not required.

Ground rules that override any instinct to improvise:

- docs/spec.md section 6 is the contract. The named rules cited below (`adapter-integrity-*`, `adapter-bounded-values`, `model-no-synthesis`, ...) are binding, and each states WHY it exists - read the rule before deviating.
- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` green is the whole bar for an adapter PR. No benchmarks: adapters are structurally isolated from the store and query layer (a guard test in `src/adapter/mod.rs` enforces it and lists its one read-only exemption with the reason), so an adapter cannot regress store performance.
- Third-party session-search parsers (franken_agent_detection, deja-vu, ctx, ...) may be read as corroborating references, never as the authority. Their bar is a search projection; pond's is a lossless round-trip codec.
- Fixtures come from sandboxed self-capture only. Never vendor another repo's fixture, and never copy files from a real (non-sandboxed) agent home.

## Phase A - spec the adapter

Output: `docs/adapters/<source_agent>.md` (named by the brand string, e.g. `grok-build.md`; the directory is created with the first spec doc) plus the committed fixture.

### 1. Read the writer, not the reader

Clone the upstream agent to `~/pjv/<owner>/<repo>` and locate the code that WRITES the session files - serializers, transcript stores, migration code - not code that reads them back. The writer is the authority on what can appear in a file; readers routinely tolerate less than writers emit. Note the agent version or commit you read: it becomes the `Last verified` line of the spec doc.

Verify every claimed shape empirically against a real capture (step 2). A field the source never actually emits does not get a mapping.

### 2. Capture the fixture (sandbox self-capture)

The conformance fixture is captured by running the target agent yourself under a sandboxed home, so the capture is born clean - sanitization collapses into verification. Generator-script synthetics are allowed only as supplementary edge-case rows, never as the round-trip ground truth.

Procedure:

- Create a throwaway home and run the agent entirely inside it: `HOME=$(mktemp -d)` plus `XDG_CONFIG_HOME`/`XDG_DATA_HOME`/`XDG_STATE_HOME` under it (`USERPROFILE` on Windows). The agent must be installed and signed in beforehand; auth material that lands in the sandbox home must not end up in the fixture.
- Script one or more sessions that exercise every shape the decision table needs: multi-turn text, a tool call with its result, a failed tool call, an unfinished/interrupted call, reasoning output if the agent supports it, a subagent/fork/spawn if the format records lineage, and an empty or aborted session. Add a CRLF variant of one file.
- Copy the session files out preserving the agent's native directory layout, under `packages/pond/tests/fixtures/adapter/<name>/`. The fixture policy in `packages/pond/tests/fixtures/README.md` governs layout, which fields must stay verbatim, and the anonymization rules - verify against it, and add the new platform's section to it (layout, quirks, provenance: producer version/commit, capture date).

### 3. Fill the decision table

Every row below is a required section of the spec doc. Every answer cites evidence: an upstream file permalink or an observed line in the capture. "Unknown" is an acceptable interim answer in a draft; it is not an acceptable answer at implementation time.

| # | Decision | What to resolve | Contract |
|---|----------|-----------------|----------|
| 1 | `source_agent` brand | The immutable brand string (e.g. `oh-my-pi`). If the harness has satellite session kinds, define the kind-subpath taxonomy (openclaw precedent: `openclaw/{subagent,cron,hook,probe}`). | brand = `AdapterFactory::name()` |
| 2 | Session identity | How the session id is built; any path/name encoding is decoded once at ingest. | `adapter-integrity-opaque-ids` |
| 3 | Project resolution | Which real source datum becomes `Session.project` (cwd, workspace key, account id, ...), and the fallback chain. | `model-project-non-empty`, `model-no-synthesis` |
| 4 | Ordering key | The source-intrinsic `(timestamp, tiebreaker)` that fixes event order per session. | `adapter-integrity-event-ordering` |
| 5 | Tool-call correlation | Where `call_id` comes from; what an unfinished call looks like; whether results can arrive without a matching call. | `model-no-synthesis` (no guessed links) |
| 6 | Provenance | Which record kinds are `Conversational` vs `Injected`; whether any fused span must split. | `adapter-provenance-required` |
| 7 | Lineage | How forks/spawns/subagents appear and map to `parent_session_id`. | `adapter-lineage-complete-restore` |
| 8 | Deliberate non-capture | Sibling files and record kinds the adapter intentionally does not ingest, each with a reason. | `model-lossless-projection` (non-capture must be declared) |
| 9 | Restore face | Native restore layout, or `restore_unsupported` with a reason naming the caller's alternative (oh-my-pi precedent). | `adapter-native-restore-lossless` |
| 10 | Freshness oracle | How "has this session changed?" is answered cheaply; whether the source can rewrite/truncate in place (if it can, a tail peek is wrong). On an unchanged re-sync the gate must fire visibly (`SkipReason::Fresh`, counted as `skipped_fresh`) - the harness rejects an adapter that merely re-ingests idempotently. | `adapter-integrity-additive-sync` |
| 11 | Windows | Where the agent writes on Windows, env overrides, encoding quirks (CRLF, UTF-16, path separators). | `windows-verify` CI runs the full test suite natively per PR |

Adapter-specific concerns beyond the table go into extra prose sections of the spec doc, not new universal rows.

### 4. Write the spec doc

`docs/adapters/<source_agent>.md`, structured as:

- Title, then one line: `Last verified: <date>, against <agent> <version or commit>.`
- Upstream pointers: the repo, the writer files read in step 1, any third-party references consulted (marked as non-authoritative).
- The 11-row decision table, filled, with evidence per row.
- Extra format notes (envelope variants, migrations, legacy shapes) as prose.

Scope split (spec 6.9): the spec doc owns format archaeology and the decision record - facts about the upstream, which do not rot when pond's code changes. The adapter code stays authoritative for extraction behavior. Maintenance is best-effort: drift is passively detected (unknown kinds still ingest losslessly; malformed input surfaces as typed errors), and the doc's `Last verified` line is updated when the adapter is next touched, not on a schedule.

## Phase B - implement from spec

Output: the adapter, wired and proven. Reference implementation: `packages/pond/src/adapter/oh_my_pi.rs` (the smallest recent adapter); `claude_ai_export.rs` shows a non-JSONL archive source driving the seam directly.

### 1. The adapter file

`src/adapter/<name>.rs`, implementing `AdapterFactory` + `Adapter` (see `src/adapter/mod.rs` for the seam docs). Use the shared helpers instead of re-deriving them:

- `jsonl.rs`: the `JsonlTree` driver for JSONL-tree sources; `parse_bounded` caps records (`adapter-bounded-values`) so no adapter reads unbounded input.
- `extract.rs`: the no-synthesis extractors (`extract_str`, `extract_bool`, `extract_value`, `extract_compact_repr`, ...). These make "invent a value" a compile error - a `Part` field is populated from an `Extracted<T>` or it stays `None`.
- `mod.rs` shared plumbing: `config_path` / `expand_home` for the `{ "path": ... }` config blob, `part_id` / `part_ordinal`, `source_options` + `raw_record` for the native-restore capture, `compact_json`, `validate_path_id` for anything that becomes a filename.

Mapping rules that trip people up:

- Placement (spec 6.5): map what has a canonical slot; anything unknown lands verbatim via placement rule 3 (a lossless catch-all carrier), never on the floor (`adapter-integrity-no-silent-drops`).
- A record the adapter cannot ingest is a typed error or a counted `SkipReason::Unsupported` naming the file and the fix - never a silent drop.
- Store the raw source record in `options.source.raw_record` (`source_options`) if the adapter will support native restore.

### 2. Registry

Three lines in `src/adapter/mod.rs`: the `mod <name>;` declaration, the `pub use` re-export, and the factory entry in `registry()`. Nothing else - no central dispatch exists to edit (spec 6.7).

### 3. Discovery

`probe_default` checks the agent's canonical install path under `Env::home`, building paths with `.join()` components only (portable across separators; `Env::from_env` resolves `USERPROFILE`/`HOME`). Test it with `test_support::assert_probe_default(&Factory, &["path", "components"])`. An agent with no canonical path (a manual export, explicit creds) returns `None` - the claude-ai-export precedent - and the config is written explicitly instead. If another product squats the same directory, discovery must distinguish them before claiming the path.

### 4. Restore face

Either implement `serialize` (native fidelity replays `raw_record`; foreign rebuilds an idiomatic best-effort form), or implement `restore_unsupported()` returning the reason string that names the caller's alternative. Never a runtime failure for a capability question.

### 5. Tests

Two layers, split by seam (single-module mapping behavior in unit tests; cross-module paths in the integration suite):

- Unit tests inside the adapter file: mapping decisions from the spec doc's table (each row that involved a choice gets a test), `probe_default` via `test_support::assert_probe_default`, and - when the native layout is exactly the source file set - round-trip via `test_support::assert_native_restore`.
- Integration suite `tests/integration/adapter/<name>.rs` using the shared conformance harness (`Conformance` in `tests/integration/adapter/mod.rs`): full-fixture ingest counts + searchable scope, re-sync-is-noop, and the round-trip mode the adapter declares (`Reingest`, `ExternalImport`, or `IngestOnly`). The adapter also declares its config face (`path_config` for the usual `{ "path": ... }` blob; a custom `fn(&Path) -> Value` for anything else) - the harness never grows adapter-specific branches. Keep adapter-specific assertions (lineage, taxonomy, project fallbacks) as extra tests in the same file.

### 6. Docs

- Commit the spec doc from Phase A under `docs/adapters/`.
- Add the adapter's row to the README supported-harnesses table, including its `Last verified` date.
- Add the fixture section to `packages/pond/tests/fixtures/README.md`.

### 7. Validate

`cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` - all green locally. CI additionally runs the identical test suite natively on Windows (`windows-verify`), which is what makes decision-table row 11 checked instead of aspirational. That is the whole bar; no benchmark run is part of an adapter PR.
