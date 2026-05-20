# Spec Alignment Plan

## How to use this document

This is an execution plan for a single implementing agent. Work it step by step (S1-S8) in the order of the Execution plan section; it lands in two commits. Each commit ends green: `cargo build`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` (toolchain 1.91.1, pinned in `rust-toolchain.toml`; CI runs `--locked`).

`docs/spec.md` is the source of truth and is already at the v1 contract. The spec amendments this work depends on are already applied in the working tree: `lineage-complete-restore` (6.2), the placement rule 3 carrier (6.5), the carrier clause in the Message model (4.6), the `pond export` / restore description (7.8), and the title. This plan does not change the spec - it aligns the code to it. When this plan and the spec disagree, the spec wins.

The parse adapters (`src/adapter/claude_code.rs`, `src/adapter/codex_cli.rs`) are the field-by-field documentation of each client format; the serializers are their inverse. Verify a claim against current code before acting on it - the line numbers below are investigation snapshots, not guarantees.

## Guiding principle

The only code that doesn't break is the code that doesn't exist. Drive toward the smallest codebase that fully satisfies the spec - clean and scalable, no larger.

- Every line this plan adds is minimum viable: no abstraction without a second caller, no generality without a second case, no file that could be inlined.
- The plan actively reclaims lines: dead code, redundant abstraction, premature generality, "organizational" files.
- Net intent: though this work adds a whole adapter face, the codebase ends tighter, not heavier.

The floor: the spec's contracts and every documented "Why" are load-bearing. A rule's rationale, a comment explaining a non-obvious constraint, the Section 3 forward-compatibility seams (`shardable-pk-pos1`, `no-subsecond-freshness`, `no-cross-shard-atomic-write`) exist precisely so they are not "simplified away." Remove what does not earn its place; never remove what defends a contract. The spec decides ties.

## Current state (verified)

- The `Adapter` codec is parse-only. `src/adapter/mod.rs` has the read face (`Adapter` trait: `events`, `discover`, `events_with`) and the stateless registry face (`AdapterFactory`: `name`, `open`, `probe_default`). No write face exists.
- `docs/spec.md` is current. Its working tree carries the v1-contract amendments listed above; they are committed with the feature (see Execution plan). The retired-anchor cleanup is a separate, code-only job: roughly 90 references to `docs/design.md` and the old anchor scheme (numbered `#inv-N`, fine-grained `#protocol-*` / `#schemas-*`) remain across `src/`, `tests/`, `benches/` and `README.md`. Zero `spec.md` references exist in code yet, so there is no double-fix risk - re-derive the exact count with `rg` at execution time.
- Sections 3, 4, 5, 7, 8 conform behaviorally; they are in scope only for slimming, not rebuild.
- This work adds one `dev-dependency` - `insta`, for the foreign golden tests (1.6). No runtime dependencies are added; `serde_json`, `chrono`, `clap`, `tempfile` are already in tree. `Cargo.lock` updates for `insta` and is committed.

---

## Stream 1 - Adapter serialize/restore face

Spec Section 6 defines every adapter as a bidirectional codec. v1 must add the `serialize` face (canonical -> client format = restore) for Claude Code and Codex, including the cross pairs.

### 1.1 The write face

The read face and write face are separate (spec 6.1). The read face is the `Adapter` instance - source-configured, streaming, stateful. The write face is source-free and a pure function of a canonical session (6.1), so it needs no instance: put it directly on the already-stateless, already-registry-listed `AdapterFactory`. This keeps spec 6.7's "one registry, no central dispatch": one method, no second registry, no dispatch `match`. `AdapterFactory` is not a face - it is the registry entry that exposes the write face directly and constructs the read face via `open()`; no single object carries both faces.

Add to `AdapterFactory` (`src/adapter/mod.rs`):

```
fn serialize(&self, session: &SessionWithMessages, fidelity: RestoreFidelity)
    -> Result<Vec<RestoredFile>, AdapterError>;
```

- `SessionWithMessages` / `MessageWithParts` (`src/sessions.rs`) are exactly what the read-back path produces - take that type, do not re-flatten.
- `RestoredFile { relative_path: PathBuf, bytes: Vec<u8> }` - a new 2-field struct in `src/adapter/mod.rs`. A `Vec` because a Claude Code subagent session restores to two files (`.jsonl` plus `agent-<hash>.meta.json`); the relative path carries the format's on-disk layout, which only the serializer knows.
- `serialize` emits messages in `(timestamp, id)` order (spec 4.6), regardless of the order they arrive in the input session. The parse->serialize conformance path (1.6) and the `get_session`->serialize production path (1.8) therefore produce identical output - `get_session` already returns `(timestamp, id)` order, and a serializer that re-sorts makes the test path agree with it.
- `AdapterError` carries the write-face error too. For serialization, `location` is the session id and a native-restore failure (a missing expected `options` key) uses `kind = Schema`; the parse-only `Parse { line }` kind does not apply.
- No new `src/` files. `RestoreFidelity`, `RestoredFile`, and the shared helper live in `src/adapter/mod.rs`; each adapter's `serialize` impl lives in its existing file. The serialize face needs no extractor seam - canonical is already trusted input (6.4). Synthesizing client-format scaffolding (fresh ids, envelope fields) during foreign restore is allowed: `no-synthesis` governs only the parse direction.
- The serializer receives complete FileParts: `get_session` reconstructs `FileData` (all variants) for the write face. The FilePart blob fix (3.4) changes where FilePart payloads are read from; it MUST keep `get_session` returning complete FileParts - the conformance tests verify this.

### 1.2 Native vs foreign fidelity (`native-restore-lossless`)

Fidelity is decided by the system, never the adapter (spec 6.3). Add a 2-case enum `RestoreFidelity { Native, Foreign }` and a free fn `origin_brand(&str) -> &str` (the `source_agent` prefix before any `/`, so `claude-code/general-purpose` matches the `claude-code` adapter). The caller (the restore path, 1.7) computes `fidelity = if origin_brand(&session.source_agent) == factory.name() { Native } else { Foreign }` and passes it in. The adapter never sniffs the data to decide.

- `Native`: lossless / value-complete per spec 1.3 - value-equal round trip, not byte-identical. The serializer reconstructs from the preserved `options` namespaces; a missing expected key is a bug and must surface as an `AdapterError`, not a silent drop.
- `Foreign`: best-effort - a valid, idiomatic session in the target's own feature set, dropping whatever the target cannot express (the dropped content stays in canonical).

### 1.3 Per-adapter serializer contract

The routine field mapping is the inverse of each parse adapter; read that code. Below is only what is not obvious by inversion.

Claude Code (`src/adapter/claude_code.rs`):

- One `.jsonl` per Session at `<encode(project)>/<session_id>.jsonl`, where `encode` replaces `/` and `.` with `-`. A subagent session (id shaped `<uuid>/agent-<hash>`, `parent_session_id` set): file at `<encode(project)>/<parent_session_id>/subagents/agent-<hash>.jsonl` plus a sibling `agent-<hash>.meta.json` = `{"agentType":..,"description":..}` from `options.subagent.*`.
- One JSON object per line in Message order; each Message's Parts collapse into that line's `message.content` ordered by `Part.ordinal`. Drop the pond-additive `Part.id` / `message_id` / `ordinal`.
- Rebuild the `parentUuid` chain: record N's `parentUuid` is record N-1's `uuid` (use `options.source.parent_uuid` when present).
- Un-nest envelope fields (`uuid`, `sessionId`, `timestamp`, `parentUuid`, `isSidechain`, `userType`, `entrypoint`, `cwd`, `version`, `gitBranch`, `requestId`) from `options.source.*` (snake_case -> camelCase) and the canonical fields. For a subagent, `sessionId` is `options.subagent.raw_session_id`, never the derived session id.
- Rebuild the assistant `message` from `options.anthropic.*` (`id`, `model`, `stop_reason`, `stop_sequence`, `usage`) plus `"type":"message"` and content from Parts. Reasoning Part -> `thinking` block with `signature` from `options.anthropic.signature`.
- Timestamps are RFC 3339, millisecond precision, `Z` suffix.

Codex (`src/adapter/codex_cli.rs`):

- One `.jsonl` per Session at `sessions/YYYY/MM/DD/rollout-<ts>-<id>.jsonl`. The Codex adapter ignores this layout on parse (it derives the session id from `session_meta.payload.id`), but the serializer MUST reconstruct it for `RestoredFile.relative_path`: `<id>` is `Session.id`; `<ts>` and the `YYYY/MM/DD` segments come from the `session_meta` timestamp; the filename timestamp is formatted `YYYY-MM-DDTHH-MM-SS` (hyphens, not colons - the path-safe form, distinct from the RFC 3339 envelope value).
- Line 1 is `session_meta` (envelope A: `{"timestamp":..,"type":"session_meta","payload":{..}}`); `payload.id` <- `Session.id`, `cwd` <- `project`, the rest from `options.source.*`. Emit fully compact JSON (no spaces after `:` / `,`).
- Each Message -> one `response_item` line; its Parts nest into `payload`. User/Assistant/System text -> `payload.type:"message"` with `input_text` / `output_text` items (System -> `role:"developer"`). Assistant + ToolCall -> `function_call` (re-stringify `params` into `arguments`). Tool + ToolResult -> `function_call_output`. Assistant + Reasoning -> the reasoning `payload` parsed back out of the Part text (it was stored as compact JSON of the whole payload - do not wrap it under a `text` key).
- Drop `options.source.adapter` and all pond-additive ids. Envelope `timestamp` from `Message.timestamp`.

Rule-3 carrier (both adapters): a System Message that the drain (1.4) produced as a placement-rule-3 carrier holds its whole source record, compact-JSON-encoded, under `options.source.raw_record`. The serializer detects that key and re-emits the line verbatim from it - no field-by-field reconstruction.

### 1.4 Destructive-drain parsing (prerequisite)

`lossless-projection` (spec 4.8) and the placement procedure (spec 6.5) require every field of every ingested record to be recoverable. Today both parse adapters read fields non-destructively (`Value::get`), so a field no code path reads is silently dropped - the parse face violates `lossless-projection` and `no-silent-drops`, and the violation is invisible until a fixture happens to exercise the missing field. Conformance built on that is fixture-coverage hope, not a guarantee. Fix the structure, not the symptoms: convert both parse row handlers to destructive-drain parsing.

- Parse each record from an owned `serde_json::Map`. Every field the adapter maps is taken with `Map::remove`, not `get` - mapping and consuming become the same operation. A removed value that becomes a canonical typed field still passes through the `Source` / `Extracted` extractor seam (spec 6.4): the drain changes how a field is located, not how it is built, so `no-synthesis` stays compile-enforced. Only the unconsumed residual reaches `options`, as raw JSON.
- After the explicit mapping, whatever remains in the map is, by construction, the set of fields no code consumed. A non-empty residual is preserved into `options` (placement rule 2) at that object's canonical host - the Message or Part it maps to. Drain recursively: every nested object (`message`, `payload`, ...) is itself drained and its residual preserved nested, or a nested field still leaks.
- A record that maps to no Message at all becomes a placement-rule-3 carrier (spec 6.5): a system-role Message, empty `content`, the whole record compact-JSON-encoded under `options.source.raw_record`, ordered by the record's own timestamp. Carrier id follows `deterministic-pk` (source id when the record carries one, content-derived otherwise); carrier timestamp is the record's own value, or the `no-synthesis` session-anchor fallback when the record has none.

What this guarantees: a field cannot be dropped silently. The only way to lose data is to `remove` a field and then discard its value - an explicit, greppable line, not an omission. `consumed union residual == input` holds by construction of `remove`. Annotate every deliberate discard; the Codex 10-MiB tool-output cap (`__pond_truncated` sentinel) is the one sanctioned discard today and is stated in that adapter's documented contract (a doc comment anchored to `spec.md#lossless-projection`).

The named gaps the investigation found stop being separate fixes - they are consequences of the drain. Claude Code metadata rows (`permission-mode`, `ai-title`, `attachment`, hook `system` rows) that kept only a subtype string, Codex `event_msg` / `turn_context` lines, and the `events_from_row` `_ => Ok(Vec::new())` arm all become "not removed, therefore in residual" (or a rule-3 carrier). Two placements are still worth doing explicitly, because the drain makes them lossless but not idiomatic:

- Codex `custom_tool_call` / `custom_tool_call_output` are tool calls and results - map them to `ToolCall` / `ToolResult` Parts (`custom_tool_call` carries `input`, not `arguments`). Without an explicit arm they land in a rule-3 carrier: lossless, but a tool call buried in a System message instead of an Assistant turn.
- Claude Code `tool_use.caller` and Codex `session_meta.base_instructions` / `instructions` / `source` map to their natural typed `options` homes (placement rule 2). The drain catches them if you forget; mapping them puts them where a reader expects.

Update the affected parse unit tests. A structural, fixture-free test (1.6) feeds the drain parser an object carrying invented field names and asserts nothing is lost - that test, not fixture coverage, is what certifies `lossless-projection`.

### 1.5 The one shared helper

Both serializers emit JSONL. Add exactly one shared helper in `src/adapter/mod.rs` - a function that encodes a slice of `serde_json::Value` records as compact, newline-delimited bytes. Each serializer builds its format-specific `Vec<Value>` and calls it. Reuse the existing `compact_json` helper for per-value encoding. RFC 3339 millisecond formatting is a `chrono` one-liner - inline it, do not make it a second helper. Add a further shared helper only if a second genuinely clears the second-caller-and-non-trivial bar.

### 1.6 Conformance tests (spec 6.8)

Conformance has two parts of different nature, so they live in different places. Native restore is per-adapter - a unit test in that adapter's `#[cfg(test)] mod tests`. Foreign restore is cross-adapter - all foreign tests live in one integration suite, `tests/integration/restore.rs`, which is also what CLAUDE.md's "`tests/` is cross-module only" rule wants.

Native restore (spec 6.8): one `#[test]` per adapter, calling a shared `assert_native_restore(factory, captures_dir)` helper in `adapter/mod.rs`'s `#[cfg(test)]` module. It opens the adapter on that adapter's capture directory under `tests/fixtures/adapter/`, parses every session to canonical, `serialize(.., Native)`, and asserts the produced `RestoredFile`s are value-equal to the source files - matched by `relative_path`, then every output line and every source line parsed as `serde_json::Value` and compared positionally (object equality is key-order-insensitive; whitespace is gone after parsing). The compare is positional because `serialize` emits `(timestamp, id)` order (1.1) and genuine append-only captures are already in that order - file order equals `(timestamp, id)` order. A capture whose physical line order differs from `(timestamp, id)` is a fixture defect, not a reason to weaken the compare. This is spec 6.8's literal "value-equal to the fixture": it catches a serializer that omits a field pond's own parser would default on read. A re-parse-and-compare-canonical round-trip is strictly weaker and is deliberately not used.

Drain invariant (spec `lossless-projection`): one fixture-free unit test per adapter (in the adapter's `#[cfg(test)] mod tests`) feeds the drain row handler a `serde_json::Map` carrying invented field names not in any known schema - including nested unknown fields inside known objects - and asserts every one of them reappears, in a typed `options` slot or a rule-3 carrier's `raw_record`. The real guarantee is structural: `consumed union residual == input` holds for every input by construction of `Map::remove` (1.4). This test does not re-prove that property; it exercises the generic drain path and the recursive descent, catching a handler that forgets to drain a nested object.

Foreign restore: all pair tests in `tests/integration/restore.rs`. A pair test takes its canonical input live - parse the origin adapter's own captures, then `serialize(.., Foreign)` with the target adapter - so there are no committed foreign fixtures and no cross-product of fixture directories. Two checks: (a) the output re-parses with the target adapter without error (valid in the target format); (b) a golden compare via `insta`. v1 has the two cross pairs (codex-origin as `claude_code`, claude-code-origin as `codex_cli`); each future adapter adds pair tests - test functions, never new fixtures. A shared helper in `restore.rs` takes an explicit `snapshot_name` (insta infers the name from the call site, so a shared helper would otherwise collide).

The foreign golden uses `insta` (added as a `dev-dependency` in step S6). Render the `Vec<RestoredFile>` into one reviewable string and `insta::assert_snapshot!`; snapshots land in `tests/integration/snapshots/` (insta's default beside `restore.rs`). `cargo test` needs only the `insta` crate; `cargo-insta` is a local accept/review tool, not a CI requirement. The implementing agent materializes goldens with `cargo insta accept` - but a foreign golden is not done until reviewed. Foreign restore is best-effort, so a golden certifies future regressions, not first correctness, and the re-parse (check a) is the only automated foreign gate. The agent MUST render each foreign output into its final report and treat the owner's review of it as a blocking precondition for completion.

Fixtures. The genuine captures already exist. `tests/fixtures/session-samples/` is a curated, anonymized set of real sessions from 8 platforms; schema-critical fields - ids, timestamps, discriminators, field names - are verbatim, only private content is replaced by `<redacted: ...>` markers that preserve the JSON envelope; the tree is verified (trufflehog, gitleaks, JSON parse) and already parsed by passing tests. Step S6 moves it to `tests/fixtures/adapter/`, renaming each platform dir to its adapter module name (snake_case, matching `src/adapter/`): `claude_app`, `claude_code`, `claude_managed_agents`, `codex_cli`, `nanoclaw`, `openclaw`, `opencode`, `pi`; and rewrites the tree's README for the new layout as `tests/fixtures/README.md`. The native test reuses these captures directly - no copy, nothing hand-authored; the `<redacted: ...>` markers do not weaken it, since a marker is opaque string content that `parse` -> `serialize` reproduces faithfully. `tests/fixtures/adapter/<adapter>/` holds nothing but captures - no `foreign/`, no per-adapter `snapshots/`.

Coverage is the capture set's job. The native test exercises whatever record types the captures contain; a missing type is a fixture gap closed per the README's refresh procedure, never a reason to hand-write JSON. One gap is a hard S6 precondition, not a soft check: the current `claude_code` captures contain no subagent session, and `lineage-complete-restore` (spec 6.2) plus 1.8's restore path cannot be conformance-tested without one. S6 does not complete until the `claude_code` captures include a real subagent session (`<parent>/subagents/agent-*.jsonl` + sibling `agent-*.meta.json`), captured and anonymized per the README refresh procedure.

E2e: `tests/integration/restore.rs` also holds one smoke test of `pond export session --as` (CLI -> Store -> Lance -> serialize -> files on disk), covering the subagent-lineage case of 1.8 - it asserts a parent plus one child are written. Register the suite with a `#[path]` line in `tests/integration.rs`.

### 1.7 `pond export` command structure

`pond export` splits into a bulk form and a single-session subcommand, matching spec 7.8. The `--as` restore mode exists only on the subcommand, so "restore all" cannot be expressed - the no-bulk-restore rule is enforced by command shape, not a runtime check.

```
pond export                              full canonical dump, all sessions,
                                         IngestEvent JSONL -> --out <file> or stdout
pond export session <id>                 one session, canonical JSONL
                                         -> --out <file> or stdout
pond export session <id> --as <target>   one session (+ its subagent lineage,
                                         1.8) restored to <target>'s client
                                         format -> --out <dir> (required)
```

- `session` is a clap subcommand of `export`; `<id>` is a required positional (no interactive picker in v1 - ids come from `pond search` / `pond get`).
- `pond export session <id>` with an id that does not exist MUST `bail!` with a clear error - unlike bulk `pond export`, which skips a vanished session silently.
- `--as <target>` is valid only on `export session`; `<target>` is validated against `adapter::known_names()` at dispatch, `bail!` with the `known: ..` message, mirroring `pond sync`.
- Without `--as`, `--out` is a file (default stdout). With `--as`, `--out` is a required directory; the CLI writes each `RestoredFile` at `out.join(relative_path)`, creating parent dirs. The serializer owns the native relative layout, so `--out ~/.claude/projects` lands a restore exactly where the client looks, with no special-casing.
- `--data-dir` / `--config` remain available throughout.

Handler reuse - the existing `pond_export(store, session_filter, writer)` already takes an optional filter:

- `pond export` -> `pond_export(store, None, ..)`.
- `pond export session <id>` -> `pond_export(store, Some(id), ..)`.
- `pond export session <id> --as <target>` -> the restore path of 1.8.

### 1.8 Subagent-lineage restore

`pond export session <id> --as <target>` restores the named session and its lineage, satisfying `lineage-complete-restore` (spec 6.2): a restored Claude Code session that used the Task tool is complete - its subagent transcripts are present, not dangling references.

- The session graph is one level deep - `lineage-complete-restore` (spec 6.2). A single `parent_session_id == id` scan collects the named session's children, and that scan is the contract itself, not an approximation of a transitive one: Claude's agent model caps nesting at one level structurally (Claude Code subagents cannot spawn subagents; Managed Agents enforces delegation depth 1), and this machine's corpus confirms it (803 subagent directories, 5808 transcripts, zero nesting). `parent_session_id` is a generic pointer any source may set, so a graph that nests deeper is a typed error, never a silent partial restore - see the Mechanism check below.
- Mechanism: a new `Store` read path (`src/sessions.rs`) returns the sessions whose `parent_session_id` equals the named id - a scan of the `sessions` table via the existing `Predicate::Eq` variant (no new substrate primitive). `parent_session_id` stays unindexed: indexing it would be a spec 5.1 schema change for a one-shot operator path - confirmed not warranted by the Lance wheel-check. The restore path: `get_session(id)` plus each child, `serialize` each (computing `RestoreFidelity` per session), write all `RestoredFile`s under `--out`. Before serializing, assert no collected child is itself a parent - re-run the `parent_session_id == <child id>` scan once per child and `bail!` with a typed error if any returns rows, since `lineage-complete-restore` forbids silently flattening a deeper graph. Mirror `pond_export`'s placement in `handlers.rs` if it reads cleanly, or inline in the dispatch arm - whichever is smaller. Print a one-line summary (session count, target, files written).
- The serializer does not change. Each session - parent or child - already serializes to its own native relative layout; a child's path nests under `<parent>/subagents/` because the serializer derives it from the child's own `parent_session_id` and `options.subagent.hash`.
- Codex never sets `parent_session_id`, so the child query returns nothing for a Codex-origin session - the feature is Claude-Code-effective at no extra cost. For a foreign target each child is still restored, as a standalone session in the target format (the parent link is dropped - normal foreign best-effort).
- Applies to the `--as` restore path only. `pond export session <id>` (canonical) and bulk `pond export` export exactly what is named.

---

## Stream 2 - Reference sweep (code + README)

Roughly 90 references to `design.md` across 11 `src/` files, 6 `tests/` files, 2 test-fixture `.md` files, and `README.md` use the retired anchor scheme. Re-point them, and treat the sweep as a comment audit: a comment that no longer earns its place is deleted, not re-pointed (CLAUDE.md comment policy). `docs/spec.md` itself is already current and is not touched by this stream. All code added in Streams 1 and 3 uses `spec.md` anchors from the start, so this sweep only touches pre-existing references. The `session-samples` README (one of the two `.md` files) is rewritten and moved in S6 (1.6), fixing its refs there; S8 sweeps the remainder - re-derive the exact count with `rg` at execution time.

### 2.1 Anchor mapping table

The new scheme is section anchors (`#substrate`, `#adapters`, ...) and rule mnemonics (`#append-only`, `#no-synthesis`, ...).

| Old anchor | New anchor |
|---|---|
| `#inv-3` | `#retry-jitter` |
| `#inv-4` | `#handle-freshness` |
| `#inv-10` | `#durable-copy` (comment is also stale - see 2.2) |
| `#inv-11` | `#namespace-resolution` |
| `#inv-15`, `#inv-16` | `#no-synthesis` |
| `#inv-17` | `#adapter-dedup` |
| `#inv-21` | `#catalog-seam` |
| `#inv-22` | `#read-seam` |
| `#inv-25` | `#stable-row-ids` |
| `#inv-15 through #inv-17` (range) | list the honesty rules the site means: `#no-synthesis`, `#schema-honesty`, `#lossless-projection` - read each site |
| `#protocol` | `#protocol` (unchanged) |
| `#protocol-pond-ingest`, `#protocol-pond-get`, `#protocol-pond-session-events`, `#protocol-error-envelope`, `#protocol-wire-interface` | `#protocol` |
| `#protocol-search` | `#search` |
| `#protocol-pond-search` | `#protocol` for wire-shape sites, `#search` for retrieval-mechanics sites - read each |
| `#protocol-ingest-semantics` | per site: `#event-ordering` (ordering), `#adapters` (adapter staleness-skip), `#protocol` (7.6 ingest-event shape) |
| `#schemas-session`, `#schemas-message` | `#datasets` |
| `#schemas-embedding` | `#datasets` for table-shape sites, `#search` for model-registry / embedding-seam sites (the `config.rs` sites) |
| `#schemas-write-params` | `#substrate` |
| `#invariants-concurrency` | `#substrate` (re-verify or drop the verbatim quote) |
| `#scope-personal-pond` | `#scope` |

The per-site anchors (`#protocol-pond-search`, `#protocol-ingest-semantics`, `#schemas-embedding`, the `#inv-15..17` range) require reading each comment's context before choosing - they are the only judgement calls in the sweep.

### 2.2 Comment audit - delete or rewrite, do not mechanically re-point

- `tests/integration/recovery.rs` module comment claims recovery is `rm -rf && pond sync`. `#durable-copy` says the opposite ("re-ingest is not a recovery path"; recovery runs through manifest history and `pond export` snapshots). Rewrite the comment to match `#durable-copy`.
- `tests/integration/store_concurrency.rs` verbatim-quotes a spec sentence that no longer exists. Re-quote from spec 3.5/3.6 or drop the quotation and keep the behavioral assertion.
- `src/sessions.rs` "(post-2026-05-15 rewrite)" is a dated migration note - CLAUDE.md forbids migration notes; delete the parenthetical.
- `src/sessions.rs` comments pointing at a spec "doc table" / a named subsection "Ordering enforcement": verify the referent still exists in 7.6 / is now the `#event-ordering` rule; rewrite to cite the rule, not the heading.
- `src/sessions.rs` / `src/config.rs` comments hardcoding `Qwen3-Embedding-0.6B` as "the built-in v1 default" contradict spec 8 ("no specific model is enshrined; the default is a configuration value"). Re-point the anchor and drop the "v1 default" claim from the prose; do not otherwise touch qwen3 code (its phase-out is separate work).
- `src/handlers.rs` "inherited verbatim from kb" is provenance trivia, not a load-bearing Why - trim it when re-anchoring.

### 2.3 README

Root `README.md`: update the `docs/design.md` path references (including the Markdown link) to `docs/spec.md`, and fix the stale "sections 1-4 are the source of truth; section 5 is empty" line (the spec now has 10 sections). Out of scope: `docs/plans/*` and `docs/references/*/README.md` are historical / reference artifacts; leave them.

---

## Stream 3 - Slimming audit

### 3.1 Removal manifest

| Candidate | What it is | Why it goes | Risk |
|---|---|---|---|
| `Predicate::IsNull` (+ its `to_lance` arm), `substrate.rs` | Enum variant | Never constructed anywhere; only `IsNotNull` is used | None |
| `IngestSummary::record_drop_reason`, `sessions.rs` | `pub fn` | Zero callers (verified across `src/`, `tests/`, `benches/`); the live drop-reason histogram is populated by `add_outcomes` from each outcome's `reason_key`, not by this function | Confirm no caller |
| `DROP_REASON_MISSING_PROJECT`, `DROP_REASON_ADAPTER_PARSE`, `DROP_REASON_ADAPTER_IO`, `DROP_REASON_ADAPTER_SCHEMA`, `sessions.rs` | 4 `pub const` | These four (of the 15 `DROP_REASON_*` consts) are referenced only at their own declaration - no routing ever uses them as a `reason_key` | Confirm still unreferenced; the other 11 consts, the histogram, and its `pond sync` / `benches/ingest_bench.rs` consumers all stay |
| `Store::upsert_session`, `upsert_message`, `upsert_part`, `upsert_session_bundle`, `sessions.rs` | 4 `pub async fn` | Zero callers; ingest goes through `upsert_session_batch` | Update the doc comment on `success_outcomes_for_substream` that names `upsert_session_bundle` |
| `Store::session_messages`, `sessions.rs` | `pub async fn` | Zero-caller pass-through to private `messages_for_session` | None |
| `Retriever` trait + `FtsRetriever` + `VectorRetriever`, `handlers.rs` | 1-method trait, 2 ZSTs | `kind()` returns a constant; collapses to `RetrieverKind` literals at the 2 call sites | Confirm the trait carries no other method; spec 8 fixes the retriever set at two |
| `Handle::count_rows` `predicate: Option<String>` param, `substrate.rs` | Function parameter | Every caller passes `None` - one real value | Speculative; drop the param only if still true |

The `DROP_REASON_*` row is precise on purpose: an earlier draft of this manifest miscounted. There are 15 `DROP_REASON_*` consts; the histogram (`IngestSummary::drop_reasons`) is live, populated through `add_outcomes`, and read by `pond sync` (`src/main.rs`) and `benches/ingest_bench.rs`. `DROP_REASON_IMMUTABLE_PROJECT` / `DROP_REASON_IMMUTABLE_SOURCE_AGENT` / `DROP_REASON_UNCATEGORIZED` are routed through `error_outcomes_for_substream` - do not touch them. Only `record_drop_reason` (the `pub fn`) and the four named consts above are dead.

### 3.2 Adapter scaffold dedup

The two parse adapters carry near-verbatim scaffolding. Fold it before the drain and write face land so they build on clean ground:

- `events()` is byte-identical in both adapters (a filtered wrap of `events_with(&NoopOracle)`), and the trait's default `events_with` (which wraps `events()`) is dead - both adapters override it. Invert: make `events_with` the required method and `events()` the defaulted one. Removes both `events()` impls and the dead default. First verify no other `Adapter` implementor (test mock, bench) relies on the current required/defaulted split.
- `AdapterFactory::open` bodies differ only by the adapter name - extract a shared `open_path_adapter(name, config, ctor)` helper in `adapter/mod.rs`.
- `peek_id_and_mtime` and the freshness-skip block are near-identical - extract a shared `peek_first_row(path)` (the I/O half); each adapter keeps its own id extraction.
- The "project, else path fallback" block is identical - extract one helper.

Stretch (do only if it stays clean): extract the per-file `events_with` line loop into a shared driver taking a per-adapter row callback. If it gets hairy, stop at the four items above and report the delta. Note the drain (1.4) rewrites the per-adapter row callback - keep the driver seam, if extracted, callback-shaped so the drain slots in.

### 3.3 Do not touch (looks removable, is load-bearing)

- `src/adapter/extract.rs` entirely (`Source`, `Extracted<T>`, `extract_*`, `from_stored`, `from_test_value`) - the no-synthesis seam.
- `substrate.rs` `ConflictExhausted` / `is_commit_conflict` / the `Conflict`-mapped path - the OCC conflict contract (spec 3.6).
- `Handle.session` / `Handle.nm` `#[allow(dead_code)]` fields, `NamespaceIdent` / `resolve_namespace` - Section 3 / 7.3 forward-compat seams.
- `config.rs` `[storage]` / object-store plumbing - the S3 backend is imminent.
- `PartKind::ToolApprovalRequest` / `ToolApprovalResponse` - spec 4.7 lists all seven Part variants; these are the canonical model.
- `Store::explain_vector_plan` - exists so `prefilter-pushdown` (spec 8) is testable; a contract-defending test seam, keep it.
- `embed/qwen3.rs` - on the phase-out path but still the only loader; do not invest, do not cut now.

### 3.4 FilePart payload - blob column as single source of truth

Every FilePart payload is currently stored twice. `part_variant_json` (`src/sessions.rs`) serializes the whole `PartKind::File` - including the `FileData` payload - into the `variant_data` JSON text column, and `parts_to_batch` also writes the payload into the `data` Lance blob column via `BlobArrayBuilder`. `parts_for_messages` projects only `variant_data`; nothing in `src/` ever reads the `data` column - it is write-only dead storage. (The test `file_part_blob_v2_round_trips_through_get` is misnamed: it round-trips through `variant_data`, not the blob.)

This violates spec 5.1 - which assigns the payload to `data` ("Lance blob; FilePart payload only") and `variant_data` to "the variant-specific fields" - and the spec 2.3 non-goal "Reinvent what Lance provides ... blob columns": the inline JSON copy is eagerly materialized on every `parts` scan, defeating blob v2's lazy/streamed design. The Lance wheel-check surfaced this.

Fix, all in `src/sessions.rs` - make `data` the sole payload store:

- `part_variant_json`: for `PartKind::File`, exclude the `FileData` payload from `variant_data` (keep `media_type`, `file_name`, and the `FileData` kind discriminator).
- `parts_for_messages`: add `data` to the projection.
- `part_from_batch`: reconstruct `FileData` from the `data` blob column.
- `get_session` MUST still return complete FileParts - the write face (1.1) and the conformance tests depend on it; keep (and honestly re-point) the existing FilePart round-trip test.

Pre-release: this changes the `parts` on-disk shape; no migration (CLAUDE.md). This is both slimming (removes a redundant stored copy) and Section 5 spec-conformance.

---

## Execution plan

Two commits (Conventional Commits; each ends green on all four checks):

- `feat(adapter): bidirectional codec - serialize/restore face` - all code and tests (steps S1-S7), plus the `docs/spec.md` v1-contract amendments already in the working tree (6.2, 6.5, 4.6, 7.8, title). Those amendments are the contract this commit implements, so they belong with it. The commit body summarizes the grouped work: spec amendments, slimming, blob fix, drain, write face, CLI.
- `docs: realign code references to spec.md` - the reference sweep (step S8), committed last so it does not re-point comments earlier steps delete, and catches any straggler.

One commit would be wrong: S8 is roughly 90 mechanical reference edits across roughly 19 files and would drown the feature diff, and a single commit cannot honestly carry the `feat`/`refactor`/`fix`/`test`/`docs` mix. Two is the floor and the cap.

The eight steps are the work sequence - S1-S7 inside the `feat` commit, S8 the `docs` commit. Order matters; keep the tree building as you go - the green gate is per-commit, but a step that leaves the tree broken blocks the next.

| Step | Work | Files | Order |
|---|---|---|---|
| S1 | drop dead code (3.1) | `substrate.rs`, `sessions.rs`, `handlers.rs` | independent of S3 |
| S2 | blob column is the sole FilePart payload store (3.4) | `sessions.rs` | after S1 (shared file) |
| S3 | dedup parse scaffolding (3.2) | `adapter/mod.rs`, `adapter/claude_code.rs`, `adapter/codex_cli.rs` | independent of S1 |
| S4 | convert parse adapters to destructive-drain (1.4) | `adapter/claude_code.rs`, `adapter/codex_cli.rs` | after S3 |
| S5 | add the serialize/restore write face (1.1, 1.2, 1.3, 1.5) | `adapter/mod.rs`, `adapter/claude_code.rs`, `adapter/codex_cli.rs` | after S4 |
| S6 | conformance tests + fixture move (1.6) | move `tests/fixtures/session-samples/` -> `tests/fixtures/adapter/` (8 dirs to snake_case) + README -> `tests/fixtures/README.md`; capture a `claude_code` subagent session (hard precondition); path constants in `codex_cli.rs` + `search.rs`/`embed.rs`/`recovery.rs`/`transport_http.rs`/`maintenance.rs`/`claude_code_ingest.rs`; native + drain-invariant tests in `adapter/mod.rs` and adapter `#[cfg(test)]` mods; foreign tests in `tests/integration/restore.rs` + `tests/integration.rs`; `Cargo.toml` (insta dev-dep) | after S5 |
| S7 | `pond export session` and `--as` restore (1.7, 1.8) | `main.rs`, `handlers.rs`, `sessions.rs`, `tests/integration/restore.rs` (e2e) | after S5 |
| S8 | realign code references to spec.md (Stream 2) | all `src/`, `tests/`, `benches/` refs; `README.md` | last; its own commit |

Backbone: S1 -> S2 (shared `sessions.rs`); S3 -> S4 -> S5 (the adapter chain). S1/S2 are independent of S3/S4. S6 and S7 both follow S5. S8 is last. The working tree already carries the `docs/spec.md` amendments - stage them into the `feat` commit alongside S1-S7.

The only new code file is `tests/integration/restore.rs` (foreign-restore conformance + the e2e, justified by CLAUDE.md test layout). New committed test data is the `insta` snapshots in `tests/integration/snapshots/`; the fixture captures are moved and renamed (plus one new subagent capture), not authored. Zero new `src/` files.

## Acceptance criteria

- `cargo build`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` all green (toolchain 1.91.1).
- Native conformance passes for both adapters: `serialize` output is value-equal to the fixture per spec 6.8 (semantic JSON, positional per line), including the Claude Code subagent two-file case.
- The drain-invariant test passes for both adapters: a parsed record carrying unknown field names loses nothing - every field reappears in `options` or a rule-3 carrier.
- Foreign output re-parses with the target adapter without error. The foreign `insta` goldens are committed only after the owner has reviewed the rendered foreign output, which the implementing agent surfaces in its final report - that review is a blocking precondition for completion (foreign restore has no automated correctness gate beyond the re-parse).
- `pond export session <id> --as` restores the named session and its direct subagent children, and `bail!`s with a typed error on a graph that nests deeper than one level; the e2e test asserts a parent plus one child are written, and a synthetic deeper graph triggers the error.
- FileParts round-trip with the payload held only in the `data` blob column; `variant_data` no longer carries it (3.4).
- `rg "design\.md" src tests benches README.md` returns nothing; `tests/fixtures/README.md` exists with the new layout.
- The `docs/spec.md` v1-contract amendments are committed in the `feat` commit; the implementing agent makes no further spec changes.
- `Cargo.toml` gains exactly one `dev-dependency` (`insta`); no runtime dependency is added; `Cargo.lock` is updated and committed so CI `--locked` passes.
- Net line delta reported (`git diff --stat`): production code under `src/` (excluding `#[cfg(test)]` modules) - the goal is non-positive; the Stream 3 slimming and dedup exist to offset the write face. If the honest delta is positive after full slimming, report it plainly rather than gaming the count. The test-code delta (new `#[cfg(test)]` modules, fixtures, `tests/integration/restore.rs`) is reported separately and is not bounded - spec 6.8 conformance tests are irreducibly additive.

## Decisions folded in

The questions raised while drafting this plan are resolved and folded into the streams above:

1. The spec amendments are already applied to `docs/spec.md` (working tree): `lineage-complete-restore` (6.2), the placement rule 3 carrier (6.5), the 4.6 carrier clause, the 7.8 export/restore description, and the title. This plan is implementation only.
2. `pond export --as` is a targeted single-session restore via the `export session` subcommand - no bulk restore, no interactive picker - and it also restores the session's lineage (1.7, 1.8). Bulk restore-to-client-format is removed; whole-pond transfer is served by the canonical `pond export` snapshot.
3. `lossless-projection` is enforced structurally, not by fixture coverage: both parse adapters use destructive-drain parsing, so a field cannot be dropped silently (`consumed union residual == input` holds by construction); a fixture-free invariant test exercises the generic drain path (1.4, 1.6).
4. `serialize` emits messages in `(timestamp, id)` order; the native conformance compare is positional against append-only captures (1.1, 1.6).
5. Conformance: native restore and the drain invariant are per-adapter unit tests; foreign restore is one integration suite (`tests/integration/restore.rs`) with `insta` goldens. Fixtures are the genuine `session-samples` captures, moved to `tests/fixtures/adapter/` (1.6).
6. A four-way Lance/LanceDB wheel-check (write face, substrate read/scan layer, blob storage, LanceDB recipes + docs) confirmed the plan's new work reinvents nothing: the codec is orthogonal to Lance, the `Predicate`/seam layer is a justified thin typed wrapper (LanceDB itself exposes only raw-string filters), and the unindexed `parent_session_id` scan is the correct cold-path tradeoff. The one reinvention it surfaced - FilePart payload double-storage - is folded in as 3.4 (step S2).
7. The work lands in two commits - one `feat` (all code + tests + the spec amendments, steps S1-S7), one `docs` (the reference sweep, S8). See the Execution plan.

Nothing is deferred beyond what spec Section 9 already defers.
