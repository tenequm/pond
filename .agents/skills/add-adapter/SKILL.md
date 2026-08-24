---
name: add-adapter
description: Playbook for adding a new source-agent adapter to pond - spec the format from the upstream writer, capture a sandboxed fixture, implement the bidirectional codec, and prove conformance. Use when adding an adapter under packages/pond/src/adapter/ or reworking an existing one.
---

# Adding a pond adapter

An adapter is a bidirectional codec between one agent's native session format and pond's canonical schema (docs/spec.md section 6). The bar it must meet: lossless capture (every source field recoverable), no synthesized values, additive re-sync, and a restore face that is either lossless-native or an honest refusal. The registry cost is fixed and small: one file under `packages/pond/src/adapter/` plus its wiring in `src/adapter/mod.rs` - a module declaration, a re-export, and the `registry()` entry (spec 6.7).

The work splits into two phases. Phase A produces the spec doc and the fixture; Phase B implements from them. Ship both in ONE self-contained PR: spec doc + fixture + adapter + tests + doc rows. Opening a draft PR after Phase A, so the decision table gets reviewed before implementation, is recommended but not required.

Ground rules that override any instinct to improvise:

- docs/spec.md section 6 is the contract. The named rules cited below (`adapter-integrity-*`, `adapter-bounded-values`, `model-no-synthesis`, ...) are binding, and each states WHY it exists - read the rule before deviating.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green is the whole bar for an adapter PR. No benchmarks: adapters are import-isolated from the store and query layer (a guard test in `src/adapter/mod.rs` enforces it and lists its exemptions with reasons), so an adapter cannot reach the store's write path, commit discipline, or query plans; what it can still get wrong - parse cost, emitted volume, freshness reads - is what the conformance harness and review look at.
- Third-party session-search parsers (franken_agent_detection, deja-vu, ctx, ...) may be read as corroborating references, never as the authority. Their bar is a search projection; pond's is a lossless round-trip codec.
- Fixtures come from sandboxed self-capture only. Never vendor another repo's fixture, and never copy files from a real (non-sandboxed) agent home.

## Phase A - spec the adapter

Output: `docs/adapters/<source_agent>.md` (named by the brand string, e.g. `grok-build.md`; the directory is created with the first spec doc) plus the committed fixture.

Preflight, before anything else: the agent binary installed at the version the spec doc will cite, a provider key in the environment (or the credential file step 2 describes), and `tmux` when the TUI is the only writer. Discovering one of these missing on the day implementation starts is the most expensive kind of blocker.

### 1. Read the writer, not the reader

Clone the upstream agent locally and locate the code that WRITES the session files - serializers, transcript stores, migration code - not code that reads them back. The writer is the authority on what can appear in a file; readers routinely tolerate less than writers emit. Note the agent version or commit you read: it becomes the `Last verified` line of the spec doc.

Verify every claimed shape empirically against a real capture (step 2). A field the source never actually emits does not get a mapping.

Trace each field's history on the writer file: `git log --follow -S'<field>' -- <writer file>` (`--follow` because renames stop `-S`; a shallow clone needs `git fetch --unshallow` first). Record it in the spec doc as a field-history section - the commit and date each row kind and field first appeared. That list decides which legacy shapes the current agent can no longer produce, which is exactly what the supplementary synthetic rows in step 2 must cover.

### 2. Capture the fixture (sandbox self-capture)

The conformance fixture is captured by running the target agent yourself under a sandboxed home, so the capture is born clean - sanitization collapses into verification. Generator-script synthetics are allowed only as supplementary edge-case rows, never as the round-trip ground truth.

Procedure:

- Create a throwaway home and run the agent entirely inside it: `HOME=$(mktemp -d)` plus `XDG_CONFIG_HOME`/`XDG_DATA_HOME`/`XDG_STATE_HOME` under it (`USERPROFILE` on Windows). Prefer a neutral base path (`/tmp/<name>-fixture`, not `$TMPDIR`, which embeds the username on macOS) because agents record their cwd and home into rows. A fresh home hides the sign-in state in your real one; if the agent takes its provider key from the environment (`OPENROUTER_API_KEY`, `ANTHROPIC_API_KEY`, ...), use that and nothing touches disk. Otherwise copy only the agent's credential file into the sandbox (the opencode capture note in `packages/pond/tests/fixtures/README.md` shows the shape) and delete it before you copy anything out; auth material never enters the fixture.
- Confirm which code path WRITES the session file before scripting (step 1): a headless / `-p` one-shot mode may skip the writer entirely, or write a reduced shape (letta-code's bidirectional stream drops tool rows; only its TUI writes them). When the TUI is the only complete writer, drive it under `tmux`: `tmux new-session -d -s cap -x 180 -y 45 "<agent> ..."`, then per prompt `tmux send-keys -t cap -l "<text>"`, `sleep 1`, `tmux send-keys -t cap C-m` - sending `Enter` in the same call as the text inserts a literal U+21B5 into the prompt instead of submitting - and read `tmux capture-pane -p -t cap` to wait for the turn. Rehearse once in a scratch home, then capture in a fresh one, so the committed fixture holds no retries.
- Script one or more sessions that exercise every shape the decision table needs: multi-turn text, a tool call with its result, a failed tool call, an unfinished/interrupted call, reasoning output if the agent supports it, a subagent/fork/spawn if the format records lineage, and an empty or aborted session. Encoding variants (CRLF, UTF-16) come from a capture on the platform that produces them, never from converting a copy - a converted file keeps the original's session id and collides with it.
- Copy the session files out preserving the agent's native directory layout, under `packages/pond/tests/fixtures/adapter/<name>/`. The fixture policy in `packages/pond/tests/fixtures/README.md` governs layout, which fields must stay verbatim, and the anonymization rules - verify against it, run its "Verification" sweep (trufflehog, gitleaks, parse validation) before committing, and add the new platform's section to it (layout, quirks, provenance: producer version/commit, capture date). Include a per-file row census in that section (`jq -r .kind transcript.jsonl | sort | uniq -c`, or the format's equivalent); every count a test asserts is derived from the census, never from memory - the letta counts drifted twice between capture and test.

### 3. Fill the decision table

Every row below is a required section of the spec doc. Every answer cites evidence: an upstream file permalink or an observed line in the capture. "Unknown" is an acceptable interim answer in a draft; it is not an acceptable answer at implementation time.

| # | Decision | What to resolve | Contract |
|---|----------|-----------------|----------|
| 1 | `source_agent` brand | The immutable brand string (e.g. `oh-my-pi`). If the harness has satellite session kinds, define the kind-subpath taxonomy (openclaw precedent: `openclaw/{subagent,cron,hook,probe}`). | brand = `AdapterFactory::name()` |
| 2 | Session identity | How the session id is built; any path/name encoding is decoded once at ingest. A root session's id must not contain `/` (the search layer reads it as the claude-code subagent marker and drops the hit from every default `pond search`) and must pass `validate_path_id` (foreign-restore targets embed the id in a filename, so `:` - an NTFS alternate-data-stream name - makes `pond resume --to claude-code` a runtime error). For a composite id, join with a character outside the source's own id alphabet (injective) that is filename-safe on every platform - letta uses `+`. The conformance harness asserts both. | `adapter-integrity-opaque-ids` |
| 3 | Project resolution | Which real source datum becomes `Session.project` (cwd, workspace key, account id, ...), and the fallback chain. | `model-project-non-empty`, `model-no-synthesis` |
| 4 | Ordering key | The source-intrinsic `(timestamp, tiebreaker)` that fixes event order per session. | `adapter-integrity-event-ordering` |
| 5 | Tool-call correlation | Where `call_id` comes from; what an unfinished call looks like; whether results can arrive without a matching call. | `model-no-synthesis` (no guessed links) |
| 6 | Provenance | Which record kinds are `Conversational` vs `Injected`; whether any fused span must split. | `adapter-provenance-required` |
| 7 | Lineage | How forks/spawns/subagents appear and map to `parent_session_id`. | `adapter-lineage-complete-restore` |
| 8 | Deliberate non-capture | Sibling files and record kinds the adapter intentionally does not ingest, each with a reason. | `model-lossless-projection` (non-capture must be declared) |
| 9 | Restore face | Native restore layout, or `restore_unsupported` with a reason naming the caller's alternative (oh-my-pi precedent). | `adapter-native-restore-lossless` |
| 10 | Freshness oracle | How "has this session changed?" is answered cheaply; whether the source can rewrite/truncate in place (if it can, a tail peek is wrong). On an unchanged re-sync the gate must fire visibly (`SkipReason::Fresh`, counted as `skipped_fresh`) for every session except the ones the suite declares as `resync_rereads` with the reason (a source that gives a session no usable watermark) - the harness rejects an adapter that merely re-ingests idempotently. | `adapter-integrity-additive-sync` |
| 11 | Windows | Where the agent writes on Windows, env overrides, encoding quirks (CRLF, UTF-16, path separators). | `windows-verify` CI runs the full test suite natively on same-repo branches; a fork PR gets it when a maintainer pushes the branch or on merge |

Adapter-specific concerns beyond the table go into extra prose sections of the spec doc, not new universal rows.

### 4. Write the spec doc

`docs/adapters/<source_agent>.md`, structured as:

- Title, then one line: `Last verified: <date>, against <agent> <version or commit>.`
- Upstream pointers: the repo, the writer files read in step 1, any third-party references consulted (marked as non-authoritative).
- The 11-row decision table, filled, with evidence per row.
- Field history from step 1: the commit and date each row kind and field first appeared.
- Extra format notes (envelope variants, migrations, legacy shapes) as prose.

Scope split (spec 6.9): the spec doc owns format archaeology and the decision record - facts about the upstream, which do not rot when pond's code changes. The adapter code stays authoritative for extraction behavior. Maintenance is best-effort: drift is passively detected (unknown kinds still ingest losslessly; malformed input surfaces as typed errors), and the doc's `Last verified` line is updated when the adapter is next touched, not on a schedule.

## Phase B - implement from spec

Output: the adapter, wired and proven. Reference implementation: `packages/pond/src/adapter/oh_my_pi.rs` (the smallest recent adapter); `claude_ai_export.rs` shows a non-JSONL archive source driving the seam directly.

### 1. The adapter file

`src/adapter/<name>.rs`, implementing `AdapterFactory` + `Adapter` (see `src/adapter/mod.rs` for the seam docs). Use the shared helpers instead of re-deriving them:

- `jsonl.rs`: the `JsonlTree` driver for JSONL-tree sources; `parse_bounded` caps records (`adapter-bounded-values`) so no adapter reads unbounded input. Three verdict hooks keep input the adapter cannot ingest out of the parser: `unsupported_path(&Path)` answers from the path alone and is honored before the freshness peek and before the file is opened; `unsupported_reason(path, rows)` answers from parsed rows; a path-identity adapter (session id from the path, not the first line) returns `false` from `peeks_first_line()` so the peek never reads a byte.
- `extract.rs`: the no-synthesis extractors (`extract_str`, `extract_bool`, `extract_value`, `extract_compact_repr`, ...). These make "invent a value" a compile error - a `Part` field is populated from an `Extracted<T>` or it stays `None`.
- `mod.rs` shared plumbing: `config_path` / `expand_home` for the `{ "path": ... }` config blob, `part_id` / `part_ordinal`, `source_options` + `raw_record` for the native-restore capture, `compact_json`, `validate_path_id` for anything that becomes a filename.

Mapping rules that trip people up:

- Placement (spec 6.5): map what has a canonical slot; anything unknown lands verbatim via placement rule 3 (a lossless catch-all carrier), never on the floor (`adapter-integrity-no-silent-drops`).
- A record the adapter cannot ingest is a typed error or a counted `SkipReason::Unsupported` naming the file and the fix - never a silent drop.
- Store the raw source record in `options.source.raw_record` (`source_options`) if the adapter will support native restore.

### 2. Registry

Three lines in `src/adapter/mod.rs`: the `mod <name>;` declaration, the `pub use` re-export, and the factory entry in `registry()`. Nothing else - no central dispatch exists to edit (spec 6.7).

### 3. Discovery

`probe_default` checks the agent's canonical install path under `Env::home`, building paths with `.join()` components only (portable across separators; `Env::from_env` resolves `USERPROFILE`/`HOME`). It reads only the injected `Env` - no `std::env` lookups inside an adapter (they cannot be injected, so their tests end up skipping on any box that sets the variable); an agent's relocation env var is the discovery layer's concern ([#148](https://github.com/tenequm/pond/issues/148)), and a relocated root is configured as an explicit `path`. Test it with `test_support::assert_probe_default(&Factory, &["path", "components"])`. An agent with no canonical path (a manual export, explicit creds) returns `None` - the claude-ai-export precedent - and the config is written explicitly instead. If another product squats the same directory, discovery must distinguish them before claiming the path.

### 4. Restore face

Either implement `serialize` (native fidelity replays `raw_record`; foreign rebuilds an idiomatic best-effort form), or implement `restore_unsupported()` returning the reason string that names the caller's alternative. Never a runtime failure for a capability question.

Two reconstruction rules review keeps catching: sort restored rows by `jsonl::source_line` first, then `by_timestamp_then_id` (claude-code, pi, desktop-app precedent), because sources stamp a whole turn with one timestamp; and `model-no-synthesis` binds the restore face too - a ToolCall without a `call_id` never folds a following result into itself.

### 5. Tests

Two layers, split by seam (single-module mapping behavior in unit tests; cross-module paths in the integration suite):

- Unit tests inside the adapter file: mapping decisions from the spec doc's table (each row that involved a choice gets a test), `probe_default` via `test_support::assert_probe_default`, and - when the native layout is exactly the source file set - round-trip via `test_support::assert_native_restore`. That helper walks every `.json`/`.jsonl` under the fixture root, so a source whose session directories hold non-transcript sidecars (`state.json`, payload files) cannot use it; write the adapter's own value-equality test instead, iterating the ingested sessions and comparing each `serialize(Native)` output against the source file at its `relative_path` (`letta_code.rs` `native_restore_is_value_equal_to_every_captured_transcript` is the pattern).
- Integration suite `tests/integration/adapter/<name>.rs` using the shared conformance harness (`Conformance` in `tests/integration/adapter/mod.rs`): full-fixture ingest counts + searchable scope (with a clean `IngestSummary`), re-sync-is-noop (every session skipped fresh except the declared `resync_rereads`), and the round-trip mode the adapter declares (`Reingest { downgraded }` naming the fixture sessions whose native restore is honestly served `Foreign` - at least one must replay natively - `ExternalImport { verified_by }` naming the CI test that owns value-equality, or `IngestOnly`, which also checks that `serialize` refuses). The adapter also declares its config face (`path_config` for the usual `{ "path": ... }` blob; a custom `fn(&Path) -> Value` for anything else) - the harness never grows adapter-specific branches. Keep adapter-specific assertions (lineage, taxonomy, project fallbacks) as extra tests in the same file.

### 6. Docs

- Commit the spec doc from Phase A under `docs/adapters/`.
- Add the adapter's row to the README supported-harnesses table, including its `Last verified` date.
- Add the fixture section to `packages/pond/tests/fixtures/README.md`.

### 7. Validate

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` - all green locally (`--all-targets` matters: Linux CI lints only the library, the Windows leg lints the tests too). CI additionally runs the identical test suite natively on Windows (`windows-verify`) for same-repo branches, which is what makes decision-table row 11 checked instead of aspirational; a fork PR gets that leg when a maintainer pushes the branch or on merge. That is the whole bar; no benchmark run is part of an adapter PR.

Before opening the PR, also run the built binary once the way a user will: `cargo build --release --bin pond`, then against a scratch store (`--config-file <empty file> --storage-path <scratch dir>`) `pond sync <name> --path <fixture root>`, `pond sync` again (must skip everything fresh), and a default-mode `pond search "<word from the fixture>"` with no `--source-agent`-style scoping that must return a hit. The test suite proves ingest, freshness and round-trip through the seam; it does not walk the CLI, the registry entry, or the search handler's default filters - the letta-code session-id defect (row 2) was invisible to a green CI and took thirty seconds to see this way.
