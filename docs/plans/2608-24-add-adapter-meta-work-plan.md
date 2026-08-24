# Adapter-addition meta-work: add-adapter skill, conformance harness, contributor flow

Date: 2026-08-24. Owner: tenequm. Tracking: [#172](https://github.com/tenequm/pond/issues/172). Status: PR1 merged as `295c6cc` ([#173](https://github.com/tenequm/pond/pull/173), 2026-08-24); PR2 (letta-code, [#170](https://github.com/tenequm/pond/issues/170)) merged as `29aadc4` ([#177](https://github.com/tenequm/pond/pull/177), 2026-08-24); PR3 (grok-build, [#171](https://github.com/tenequm/pond/issues/171)) on `feat/grok-build-adapter` - spec doc, macOS+Windows sandbox fixture, adapter, conformance suite, and the deferred freshness-read-budget assertion (implemented as thread-local `peek_metering` in `jsonl.rs` + per-adapter unit tests rather than a harness field, since a process-global counter would cross-count under parallel `cargo test`; the budget is `min(file length, PEEK_TAIL_CAP)`, so a transcript under the cap pins the peek to one pass; back-applied to letta) all done, green, polished, and smoke-tested through the built binary (14 sessions, re-sync all fresh, default search hits); zero add-adapter skill edits needed (the PR3 success bar). Note: Phase A empirically overturned the deja-vu rewind claim - grok 1.0.x `updates.jsonl` is strictly append-only (`/rewind` appends a `rewind_marker`), so the tail-peek oracle stands. Targets: [#170](https://github.com/tenequm/pond/issues/170) (letta-code), [#171](https://github.com/tenequm/pond/issues/171) (grok-build), and every adapter after them.

## Goal

Make adding an adapter routine work instead of custom work - for us and for external contributors - without ever weakening the guarantees the existing adapters carry (lossless codec, no-synthesis seam, additive sync, measured store performance). Three PRs:

- **PR1 (this plan)**: the meta-work - the `add-adapter` skill, a shared conformance harness proven against five existing adapters, the contributor flow (spec docs, CONTRIBUTING), and cheap structural guards.
- **PR2**: letta-code adapter (#170), implemented by following the skill literally; every friction point patches the skill/harness in the same PR.
- **PR3**: grok-build adapter (#171) as the validation run - success bar: zero skill edits needed. Also adds the byte-counting freshness-read assertion (see Deferred).

## Why the split is safe

Adapters are import-isolated from the store and query layer: adapter code names only the canonical model types (`IngestEvent`, `SessionWithMessages`, `MessageWithParts`) from `sessions`, never the `Store`, the substrate, the rowmap, the handlers, or the Lance layer beneath them - with one documented, read-only exemption (openclaw's deletion reconciliation asks the store what it holds and never writes). All writes funnel through `ingest_adapter -> upsert_session_batch`, which owns the append fast-path, cleanup gating, and commit discipline. What an adapter PR can still regress is (a) its own parse throughput, (b) the volume and shape of what it emits, (c) the sync scan/freshness cost, (d) discovery walk cost - and `benches/ingest_bench.rs` does drive claude-code end to end, so the isolation is of the store's write path and query plans, not of performance in general. PR1 turns the import isolation from convention into checked fact (guard test below); the harness's clean-ingest and exact re-sync checks cover (b) and (c). That is what makes "adapter PRs need fmt, clippy, and `cargo test` green, nothing else" a defensible statement.

## Decisions (resolved 2026-08-24)

1. **Sequencing**: meta-work first, letta-code second (easiest adapter = cheapest shakedown; failures attribute to the meta-work, not the format), grok-build third (genuinely different shape: multi-file, ACP stream, forks, rewind - a real generality test).
2. **Skill location**: committed at `.agents/skills/add-adapter/` with a committed symlink `.claude/skills/add-adapter` - the exact layout `npx skills` already materializes for managed skills, so the repo-native skill is indistinguishable from the tooling's own convention. Requires a gitignore unignore cascade (git never descends into ignored dirs). Not listed in `skills-lock.json` (repo-native, not CLI-managed). Accepted caveat: a Windows clone without symlink support checks the symlink out as a text file containing the path - harmless (the skill's real home is `.agents/skills/`, and Windows CI never loads skills); noted here so it is not re-litigated.
3. **Harness scope**: shared conformance helpers + retrofit onto shape-diverse adapters - oh-my-pi (single JSONL), opencode (multi-file fan-out join), claude-ai-export (archive, not a live source), plus two added at review time because they stress axes the first three do not: claude-code (subagent sidecars, `brand/kind` sessions) and pi-coding-agent (two-field config face, three formats in one corpus, honest native-to-foreign downgrade). pi paid for itself immediately: it exposed that `Reingest` had no room for a declared downgrade. The remaining five (claude-desktop-app, codex-cli, hermes, nanoclaw, openclaw) migrate opportunistically when next touched.
4. **Byte-counting freshness assertion: deferred to PR3.** Every existing adapter is tail-peek-shaped - one case. Grok's `updates.jsonl` can truncate and regrow on rewind (deja-vu finding), so a correct grok oracle reads a prefix, not a tail. The right harness API ("adapter declares its freshness read budget; harness asserts actuals stay within it") can only be designed once that second case exists. It back-applies to letta in PR3 for a few lines.
5. **Fixture policy**: sandbox self-capture is the one approved way for the conformance fixture. Run the target agent under a sandboxed `HOME`/`XDG_*` (the technique already documented in AGENTS.md for wizard testing), script one session that exercises every shape the fixture spec requires, so the capture is born clean - sanitization collapses into verification against `packages/pond/tests/fixtures/README.md`. Generator-script synthetics are allowed only as supplementary edge-case rows, never the round-trip ground truth. No vendoring of third-party fixtures (ctx et al.); external repos are read-only format references.
6. **Spec docs**: each adapter gets `docs/adapters/<source_agent>.md` (named by the brand string), containing the format archaeology and decision record - facts about the upstream, which do not rot when our code changes. Adapter code stays authoritative for extraction behavior. spec.md 6.9 gets a one-line scope amendment saying exactly that. The doc carries `Last verified: <date>, against <agent version/commit>`. The template is described inside SKILL.md (no standalone TEMPLATE.md file). No backfill for the ten existing adapters; they get one when next touched. #170/#171 bodies become the letta/grok docs in PR2/PR3 (spec + implementation ship in one PR - the self-contained rule).
7. **Contributor flow**: one self-contained PR per adapter (spec doc + fixture + adapter + tests). No GitHub-issue gate. Recommended (not required): commit the spec doc first and open the PR as a draft for a fast decision-table review before implementing.
8. **Maintenance policy**: best-effort for all adapters. Drift is passively detected and safe by design: unknown record kinds still ingest losslessly as placement-rule-3 carriers, malformed input surfaces as typed errors naming adapter and location (`adapter-integrity-no-silent-drops`). No canary infrastructure. The README adapter table gains a `Last verified` column.
9. **CONTRIBUTING.md**: yes, ~20 lines, pointing at the skill and stating the PR expectations.

## PR1 work items

### 1. gitignore cascade + skill skeleton

`.gitignore`: replace the blanket `.agents/` and `/.claude` ignores with cascades that keep everything ignored except the committed skill:

```
.agents/*
!.agents/skills/
.agents/skills/*
!.agents/skills/add-adapter/
*/**/.agents/
/.claude/*
!/.claude/skills/
/.claude/skills/*
!/.claude/skills/add-adapter
```

Preserve the existing `/.claude/*local.json` intent (covered by `/.claude/*`). Verify with `git status` that no previously-ignored files (settings, worktrees, compact-handoff, managed skills) become visible.

### 2. `.agents/skills/add-adapter/SKILL.md`

The playbook, written for an agent to execute and a human to read. Structure:

- **Phase A - spec the adapter** (output: `docs/adapters/<source_agent>.md`):
  - Clone the upstream agent to `~/pjv/<owner>/<repo>`; locate the code that writes the session files (the writer, not the reader); verify row shapes empirically against a real capture. Third-party session-search parsers (franken_agent_detection, deja-vu, ctx) may be read as corroborating references, never as the authority - pond's bar is a lossless round-trip codec, theirs is search projection.
  - Fill the decision table (below). Every row cites evidence (upstream file permalink or observed capture).
  - Capture the fixture: sandboxed `HOME`/`XDG_*`, scripted session hitting every required shape, CRLF variant included, verified against `tests/fixtures/README.md`, provenance note (producer version/commit, SHA-256, capture date).
- **Phase B - implement from spec** (output: the adapter PR):
  - Adapter file at `src/adapter/<name>.rs` (reference implementation: `oh_my_pi.rs` - the smallest recent adapter). Use the seam helpers (`jsonl.rs`, `extract.rs`); placement rule 3 is the catch-all for unknown record kinds.
  - Registry line in `src/adapter/mod.rs` (module decl + re-export + `registry()` entry).
  - Discovery: `probe_default` through `Env` (paths built with `.join()` components only), tested via `test_support::assert_probe_default`.
  - Conformance suite via the shared harness (item 3) in `tests/integration/adapter/<name>.rs` + unit tests in the adapter file.
  - Spec doc committed under `docs/adapters/`; README adapter table row.

- **The decision table** (11 rows, each a required section of the spec doc):
  1. `source_agent` brand (the immutable brand string, e.g. `oh-my-pi`, `grok-build`; plus a kind-subpath taxonomy where the harness has satellite session kinds - the openclaw precedent: `openclaw/{subagent,cron,hook,probe}`)
  2. Session identity (id construction, one-time decode - `adapter-integrity-opaque-ids`)
  3. Project resolution (real source data satisfying `model-project-non-empty`)
  4. Ordering key (source-intrinsic `(timestamp, tiebreaker)`)
  5. Tool-call correlation (call_id source, unfinished-call handling)
  6. Provenance classification (conversational vs injected per record kind; fused-span splits)
  7. Lineage (forks/spawns/subagents -> `parent_session_id`)
  8. Deliberate non-capture (ignored siblings/records, with reasons)
  9. Restore face (native restore, or `restore_unsupported` with a reason naming the alternative - the omp precedent)
  10. Freshness/skip oracle (how "changed?" is answered cheaply; note if the source can rewrite in place)
  11. Windows (where the agent writes on Windows, env overrides, encoding quirks; `windows-verify` CI runs all tests natively per PR)

- Adapter-specific concerns beyond the table land as extra prose in the spec doc, not as new universal rows.

### 3. Conformance harness

Shared helpers in `tests/integration/adapter/mod.rs` (the designated cross-adapter harness home), generalizing what each suite currently hand-writes:

- `assert_round_trip`: in the mode the adapter declares - `Reingest` (serialize native -> re-open through the adapter's own config face -> re-ingest -> canonically equal, with the honestly downgraded sessions named), `ExternalImport` (full-fidelity native output; value-equality owned by a named CI test), or `IngestOnly` (the declared refusal, and `serialize` must refuse too). The byte-level 6.8 value-equality stays in the adapter-file unit tests.
- `assert_resync_is_noop`: ingest fixture, ingest again, assert zero rows written the second time (additive-sync + freshness working together).
- `assert_ingest_counts_and_searchable`: full-corpus ingest, expected session count, `source_agent`-scoped searchable > 0 (the pipeline-ran proof).

Retrofit `oh_my_pi.rs`, `opencode.rs`, `claude_code.rs`, and (new suites) `claude_ai_export`, `pi_coding_agent` onto the helpers; keep any adapter-specific assertions those suites carry. Design constraint: helpers take the adapter + fixture root + expectations struct, no per-adapter branching inside the harness (seam rules apply to test seams too). The adapter also declares its config face (`config: fn(&Path) -> Value`, `path_config` for the usual `{ "path": ... }` blob), so an adapter with a different config shape (pi-coding-agent's `sqlite_path`) never edits the harness - adapter additions must not touch the seam. Likewise `RoundTrip::Reingest { downgraded }` names the fixture sessions the adapter honestly serves as `Foreign` on a `Native` request (at least one must replay natively) (pi: v4/SQLite-origin sessions are reconstructed as v3 because no released pi reads v4); those must still re-ingest, and a policy change shows up as a count change instead of passing silently.

Implementation order note: survey how the three target suites (and the adapter-file unit tests) do round-trip TODAY before fixing helper signatures - both `src/adapter/snapshots/` and `tests/integration/adapter/snapshots/` exist, so insta snapshots are in play and the helpers must compose with them, not fight them.

### 4. Structural guard test

A unit test (in `src/adapter/mod.rs` tests) that walks `src/adapter/` recursively and fails on any production line reaching past the seam, turning the import-isolation argument into CI fact. As implemented: an identifier-token denylist (`Store`, `substrate`, `rowmap`, `handlers`, `lance`, `lancedb`, `arrow`, `datafusion`, `object_store`, `from_stored`), matched whole or as a `<token>_` prefix because the lance and arrow workspaces are hyphenated sub-crates; plus a crate-root allowlist - every `crate::` (or `super::super::`) path, grouped imports included, must resolve to `adapter`, `wire`, `config`, or one of the three canonical model types from `sessions` - so `crate::{sql, transport, embed}` and `crate::sessions::init_embedding_dim` fail by omission rather than by a denylist that has to know every module. The scan stops at the `#[cfg(test)]` that opens a `mod` (attribute and doc lines in between allowed), not at item-level attributes (`opencode.rs` `open_db`, `extract.rs` `from_test_value`), and any column-0 item below the test module is itself a violation, so a test module cannot be used to hide production code from the scan. `//` lines are skipped. Exemptions are listed as (file, token, expected count) and the count is asserted both ways: openclaw's `reconcile_deletions(&self, store: &Store)` is a read-only detection pass (the import path, plus the identifier on the import line and the parameter); `extract.rs` defines `from_stored` for the store side. A substring match on `sessions::Store` was tried first and was green while false (brace-grouped imports never spell it); the review pass then found the crate-path denylist bypassable by grouped imports and the early-test-module break exploitable, and both were closed and verified against a probe file carrying each shape. The error message names the token and how to exempt a legitimate read-only use. What the guard proves is import isolation from the store's write path, commit discipline, and query plans - not performance isolation (see "Why the split is safe").

### 5. Docs

- `docs/adapters/` is not created in PR1: the spec.md amendment names it, and PR2's first spec doc creates it.
- `docs/spec.md` 6.9 amendment: spec docs own format archaeology + decision record; code owns extraction behavior.
- `README.md`: adapter table gains `Last verified` column (initial values from the fixture snapshot dates in `tests/fixtures/README.md`; `-` where unknown).
- `CONTRIBUTING.md` (~20 lines): adding an adapter -> the playbook is `.agents/skills/add-adapter` (usable as a Claude Code skill, readable as a doc); one self-contained PR (spec doc + fixture + adapter + tests); draft-PR review of the decision table recommended; sandbox self-capture required for the conformance fixture; fmt, clippy, and `cargo test` green is the whole bar - no benchmarks needed for adapter PRs.
- `AGENTS.md` (which the local `CLAUDE.md` symlinks to): one pointer line in the "Adapter seam" section - "adding an adapter -> follow `.agents/skills/add-adapter`". Agents read it unconditionally; skill discovery is the second line of defense, not the first.

### 6. Validation

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green locally (including the five retrofitted suites and the guard test).
- `git status` clean of formerly-ignored noise after the gitignore cascade.
- Skill loads: `/add-adapter` visible in a fresh Claude Code session in the repo.

## PR2/PR3 precondition

The sandbox self-capture rule means the target agent must be installed and signed in BEFORE its PR starts: letta-code for PR2, grok-build for PR3 (xAI account access). Set both up ahead of time so capture is not discovered as a blocker the day implementation begins.

## PR2 outline (letta-code, #170)

Follow the skill literally; patch skill/harness at every friction point in the same PR. Spec doc from the #170 body + verification against letta-code source (`src/utils/transcript-paths.ts`, `src/cli/helpers/reflection-transcript.ts` - note the `LegacyMessageIdReflectionTranscriptState` legacy variant, `src/backend/local/transcript-migration.ts`). Sandbox self-capture per the fixture rule (#170 lists the required shapes: reasoning rows, failed tool, unfinished tool, legacy id-less row).

## PR3 outline (grok-build, #171)

Validation run - zero skill edits is the success bar. Phase A must resolve, in the spec doc: the rewind truncate-and-regrow behavior of `updates.jsonl` vs the tail-peek oracle and `adapter-integrity-additive-sync` (deja-vu keeps a prefix hash; decide pond's answer before implementation). Adds the byte-counting `Source` wrapper + declared-freshness-read-budget assertion to the harness, back-applied to letta. External references for archaeology (read-only, never authority): franken_agent_detection `src/connectors/grok.rs` (envelope variants, `.cwd` >255-byte override, `chat_history.jsonl` fallback), deja-vu `internal/sources/grok.go` + `docs/registry/grok.md` (rewind, chunk-join keys, spawn tree), ctx `crates/ctx-history-native-jsonl-parsers/src/grok_build.rs` (`rawOutput` typed unions, `/_meta/x.ai~1tool/kind` pointer escape, terminal-status pin). Discovery caveat: `@vibe-kit/grok-cli` (npm) is a different product that also writes to `~/.grok` - grok-build discovery must distinguish them.

## Deferred / explicitly not in PR1

- Byte-counting freshness-read assertion (PR3, with grok as the designing case).
- Retrofitting the remaining five adapters (claude-desktop-app, codex-cli, hermes, nanoclaw, openclaw) onto the harness (opportunistic).
- Spec-doc backfill for existing adapters (opportunistic).
- Any canary/drift-detection infrastructure (rejected - passive detection is safe by design).
- Any change to `search_text` scope, MCP surfaces, or store write paths (out of scope by definition; the guard test enforces the last).
