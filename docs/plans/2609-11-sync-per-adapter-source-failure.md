# Sync: an absent source dir fails its adapter, not the run

Resolves [#236](https://github.com/tenequm/pond/issues/236). Branch: `fix/236-per-adapter-missing-source`. Draft PR: [#237](https://github.com/tenequm/pond/pull/237).

## How to use this doc

You are an agent picking up a completed investigation. This doc is self-contained: the confirmed current behavior (with file:line references on this branch, base `d475b2b`), the design decisions already made, and the work split. Read `docs/spec.md` sections 6.6 (`adapter-integrity-no-silent-drops`), 5.4 (`session-movement-complete`), and the `pond sync` bullet of 7.8 before changing behavior. Read `AGENTS.md` for test placement rules and commands.

Do NOT commit or push: leave your changes in the working tree; the coordinating session reviews and commits.

## The problem

A shared fleet `config.toml` enables adapters for tools a given host may not have. On such a host, the enabled adapter's `path` does not exist, and today:

- `pond sync --dry-run` **aborts the whole run** (exit 1) with only `Error: codex-cli io error at /...: No such file or directory`. Healthy adapters produce no verdict at all. This is the issue's repro.
- `pond sync` (real) does NOT abort - verified empirically on 0.17.2 and in code - but reports the failure almost invisibly: the adapter's final line reads `up to date  1 err`, which is both misleading ("up to date" for a source that was never readable) and unattributed (the reason line goes through `MultiProgress::println`, which is dropped off-TTY, i.e. exactly in the cron logs fleets read). The `--format json` summary carries no per-adapter failure at all.

### Confirmed mechanics (all paths verified in code, refs on base `d475b2b`)

- Dry run abort: `packages/pond/src/main.rs:4503` - `let plan = opened.plan(oracle).await?;` in `run_sync_dry_run`. The surrounding loop already has per-adapter error *rows* (`DryRunRow.error`, filled from `discover()` failures at main.rs:4506-4509, rendered as red `cannot read this source - {error}` and as JSON `"error"` with `"sessions": null`), so the abort on `plan()` contradicts the function's own design comment (main.rs:4486-4490).
- Real sync survives because the jsonl-tree event stream yields exactly one `Err` when the root walk fails (`packages/pond/src/adapter/jsonl.rs:237-239` via `collect_tree_files` read_dir error), and `ingest_adapter` maps a pre-Session `Err` to a per-file skip and keeps going (`packages/pond/src/handlers.rs:405-419`, `SyncStatus::Skipped`, `summary.skipped_files += 1`).
- `pond status` is already tolerant: `opened.plan(&oracle).await.ok().flatten()` plus a per-adapter `error` column from `discover()` (main.rs:6312-6326). No change needed.
- OpenClaw deletion reconciliation tolerates a missing root: `list_agents` returns `Ok(vec![])` on `NotFound` (`packages/pond/src/adapter/openclaw.rs:425-428`). No change needed.
- Bad config (e.g. `enabled = true` with no `path`) fails at resolve/`factory.open` and MUST keep failing the whole run: spec 7.8 fixes that posture ("a malformed array fails the whole resolve - the same posture as any other bad config blob, which `factory.open` already refuses"). The issue's "adjacent detail" agrees.
- All current adapters receive a scalar `path` in their resolved config blob (multi-path entries fan out at resolution, spec 7.8), and `pond sync` never discovers, so an enabled entry always carries an explicit `path`.

### Why the spec permits the fix

`adapter-integrity-no-silent-drops` (6.6) governs malformed source *input*: a record that was read and could not be parsed must surface. An absent source directory reads nothing, so nothing is dropped; a **named per-adapter failure** satisfies "surfaced, attributable, never silent". `session-movement-complete` (5.4) makes storage the union of every *still-reachable* source - an absent dir is not reachable, and aborting the run works against completeness by blocking the reachable sources on the same host.

## Design decisions (made - do not relitigate; flag concerns to the coordinator)

1. **An absent source root is a per-adapter failure. The run continues.** It is reported on every surface: dry-run rows (text + JSON), real-sync per-adapter line, real-sync final summary (text + JSON). It is a *failure* verdict, not a benign skip - the operator must be able to see it - but it does not take unrelated adapters down.
2. **Exit code stays 0 when the run was a whole-config sync** and at least the resolved set ran to completion (matching the existing per-session error convention: parse-failed sessions already yield exit 0 today, and real sync already exits 0 in this exact scenario). The last-sync record stays `Ok` so a fleet host missing one tool does not show a permanent FAILURE in `pond status` - status's own per-adapter error column already names the condition.
3. **Explicit narrowing keeps the hard error.** `pond sync <adapter>` (positional) or `pond sync <adapter> --path <dir>` naming a source that does not exist remains a whole-run error (exit 1): the operator named that adapter deliberately, so absence is a wrong invocation, not fleet state. This preserves today's `--path`-typo protection. (Dry run with explicit narrowing: same rule.) Post-review hardening: the check pre-scans every resolved pass before the first adapter runs, so a multi-path entry never ingests its readable dir and then errors on the absent one, leaving store writes behind a non-zero exit.
4. **Mechanism for real sync: pre-flight existence check, not error-plumbing.** In `run_import_stage`'s loop (main.rs:4853-4856), before `sync_with_progress`, resolve the entry's source root from the config blob (`config.get("path")`, home-expanded the same way adapters do - `adapter::expand_home`, main mod.rs:617) and check existence. Missing -> emit the per-adapter failure verdict and continue; never invoke the adapter. Rationale: the alternative (classifying the event stream's first `Err`) cannot distinguish "whole source absent" from "one unreadable file" without changing the adapter API, and would keep the misleading `up to date` tail. A path that *exists* but is otherwise unreadable keeps today's behavior (per-file skip + `N err`).
5. **Dry run: catch errors per adapter into the existing row machinery.** Replace the `?` on `plan()` with a match that lands `Err` in `DryRunRow.error` (same for the pre-flight missing-root case, which should short-circuit before `factory.open`/`plan` for the same message as real sync). No new row rendering: reuse `cannot read this source - {error}` / the `source missing` wording below.
6. **Wording pinned** (so impl and tests agree):
   - Real sync per-adapter line tail and dry-run row detail for the absent-root case: `source missing: <home-contracted path> - skipped this run` (painted red; `<path>` via the existing `contract_display`).
   - Real-sync JSON summary gains an additive field, present only when non-empty:
     `"failed_adapters": [{"name": "<adapter>", "path": "<path>", "error": "source missing: <path>", "reason": "source_missing"}]`
     on BOTH the ok and error summary documents. Additive per spec 7.2, so no version bump.
     `reason` is the stable machine-readable kind (post-review addition), so fleet
     tooling branches on it instead of parsing the human `error` string.
   - Real-sync text summary (after the stage lines): one red line per failed adapter:
     `import: <adapter label> source missing: <path> - skipped this run` (via `output_err`, so it is visible off-TTY/cron; label via `adapter_label` so fanned multi-path entries stay distinguishable).
   - Dry-run JSON: unchanged shape - the absent-root/plan error lands in the existing per-adapter `"error"` field with `"sessions": null`.
7. **Spec touch (one sentence class, 7.8 `pond sync` bullet):** after "With no enabled adapters it does nothing but name the fix.", add: an enabled adapter whose configured source path does not exist on this host fails that adapter - named per adapter in the sync summary and dry-run verdicts - and the run continues (`session-movement-complete`: the other sources are still reachable); explicitly naming the adapter (`pond sync <adapter>` / `--path`) keeps the hard error, and a malformed config blob still fails the whole resolve.
8. **Out of scope:** making `MultiProgress::println` skip-lines visible off-TTY for ordinary per-file skips; surfacing non-NotFound whole-source errors (permission denied) as adapter-level verdicts in real sync (they keep today's behavior); `pond status` changes; the missing-`path` config error (stays fatal, gets a pinning test). *Addendum: the same PR later shipped the per-file-skip surface after all - `degraded_adapters` (skip count + first reason, JSON + off-TTY stderr) - superseding the first item, and the second only in part: a permission-denied root is now surfaced, but still as a degraded run rather than an adapter-level verdict (`missing_source_root` keeps returning `None` on a `try_exists` error). The pond-history dig found the old silence was never a recorded decision.*

## Work split

Two agents work this branch in the same worktree, disjoint files. Do not touch the other's files. Do not commit.

### WP1 - implementation (agent `impl-236`)

Files: `packages/pond/src/main.rs` (+ unit tests in its `mod tests`), `docs/spec.md`.

1. `run_sync_dry_run` (main.rs:4442-4577):
   - Short-circuit per resolved adapter when the source root is absent -> error row `source missing: <path> - skipped this run` (before `factory.open`).
   - Replace `let plan = opened.plan(oracle).await?;` with a match: `Err(error)` -> row with `error: Some(error.to_string())`, `sessions: 0`, `plan: None` (rendered via the existing `cannot read this source` arm; do not call `discover()` after a plan error).
   - Explicit narrowing (`invocation.adapter.is_some()`): absent root stays a whole-run `Err` (pin the message: name the adapter, the path, and that the host lacks this source).
2. `run_import_stage` (main.rs:4775-4858):
   - Pre-flight each `resolved` entry as decision 4. On missing root with no explicit narrowing: record `{name, label, path, error}` into a new `Vec` on `IngestSummary` or alongside it (choose the smallest plumbing that reaches both summary renderers; `SyncReport` in `run_sync` is the natural carrier for the JSON path), emit the red `output_err` line immediately, skip the adapter.
   - With explicit narrowing: `bail!` with the pinned message.
3. `run_sync` (main.rs:3875-4019): thread the failed-adapter list into both JSON summary documents (`failed_adapters` field, omitted when empty) - text path already covered by the immediate red line; consider repeating the failures right before `done - sync complete` so they cannot scroll away behind a long import (coordinator preference: yes, repeat).
4. Home expansion: reuse `adapter::expand_home` (export it as `pub(crate)` reach for main, or add a small helper next to `source_path` at main.rs:4871-4878 that returns the expanded `PathBuf`). Must match adapter behavior for `~/` paths.
5. Unit tests (main.rs `mod tests`, per AGENTS.md placement rule): the pre-flight helper (absent vs present vs `~` expansion), and `resolve`-level behavior is already covered - don't duplicate.
6. `docs/spec.md` 7.8: the sentence from decision 7.
7. `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test --lib` green.

### WP2 - integration tests (agent `tests-236`)

Files: `packages/pond/tests/integration/missing_source.rs` (new), one `#[path]` line in `packages/pond/tests/integration.rs`. Nothing else.

Model the harness on `packages/pond/tests/integration/unreadable_source.rs` (sandboxed HOME/XDG, `NO_COLOR=1`, one JSON document per stdout). Config under test: two adapters - `claude-code` at an existing empty dir, `codex-cli` at an absent path (the issue's repro B). Cases:

1. **Dry run continues** (the issue's headline): text mode exits 0, the `claude-code` plan row is present, the `codex-cli` row carries `source missing`; JSON mode: `codex-cli` has `"sessions": null` + `"error"` containing the path, `claude-code` has `"sessions": 0`, exit 0.
2. **Real sync continues and attributes**: `--format json` exits 0, `"outcome": "ok"`, `failed_adapters` names `codex-cli` with the path; a claude-code fixture session (copy a minimal fixture jsonl from `tests/fixtures`, or an existing helper) actually lands (`sessions_inserted >= 1`) proving the healthy adapter ran.
3. **Explicit narrowing stays fatal**: `pond sync codex-cli` (and `--dry-run` variant) against the absent path exits non-zero, stderr names the path; nothing about other adapters.
4. **Config error still aborts** (pin the adjacent detail): `enabled = true` with no `path` on one adapter fails the whole run even though another adapter is healthy, exit non-zero, error names the config shape.
5. **Off-TTY visibility**: real sync (text mode) stderr contains the `source missing` line for `codex-cli` (assert on captured stderr - the process under test is already off-TTY).

Write tests against the pinned wording/JSON of decision 6. Expect them red until WP1 lands in the shared tree; coordinate through the coordinator, not by editing WP1 files. `cargo test --test integration -- missing_source::` green at the end.

## Validation (coordinator, after both WPs)

- `cargo fmt --check`, `cargo clippy -- -D warnings`, full `cargo test` in the worktree.
- Manual repro matrix from the issue (A/B configs, dry + real, text + json, plus `pond sync codex-cli` narrowing) against the built binary.
- Re-read the diff against decisions 1-8.

## Build notes for agents

- Work in THIS worktree only; default `target/` (do not set `CARGO_TARGET_DIR` elsewhere - pond fixture paths are compile-time constants and a shared target dir poisons them).
- First build is cold and slow; that is expected. Cargo file-locks the target dir, so concurrent `cargo` invocations from both agents serialize - prefer editing first, building late.
