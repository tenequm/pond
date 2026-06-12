# Pond - project instructions

## Commands

- Build:  `cargo build`
- Test:   `cargo test`
- Lint:   `cargo clippy -- -D warnings`
- Format: `cargo fmt --check` (use `cargo fmt` to fix)
- Lockfile is enforced in CI with `--locked`; locally, plain commands are fine. If `Cargo.lock` changes unexpectedly, `git status` will show it.

## Tests

- Layout: unit tests live in `#[cfg(test)] mod tests` next to the code they test (`src/...`). All integration tests are bundled into one binary at `tests/integration.rs`, with each suite as a module under `tests/integration/<name>.rs` and pulled in via `#[path = ...] mod <name>;`. Keep new integration suites in this folder and add a matching `#[path]` line - never drop a loose `tests/foo.rs` next to `integration.rs` (it would compile as a second binary and re-link the whole crate).
- Adapter-specific integration tests go in `tests/integration/adapter/<adapter>.rs` (mirroring `src/adapter/<adapter>.rs`), not in concern-level files; keep cross-adapter interop (e.g. the foreign-restore matrix) in `tests/integration/adapter/mod.rs`, the seam analog of `src/adapter/mod.rs`.
- Run everything: `cargo test`.
- Run one integration suite: `cargo test --test integration -- <module>::` (e.g. `... -- search::`).
- Run one unit-test module: `cargo test --lib <module>::` (e.g. `... --lib sessions::tests::`).
- Run one test by name: `cargo test <name>` (substring match across all binaries; add `-- --exact` to require a full match).

## Testing interactive CLI flows

The wizard prompts (`pond init`, source discovery, etc.) go through cliclack/dialoguer, which read raw keystrokes from a real TTY - they can't be exercised by piping stdin or by a `cargo test`. To verify them, drive the compiled binary under a pseudo-terminal:

- Driver: Python stdlib `pty` (no `pexpect` install). `pid, fd = pty.fork()`; in the child `os.execve` the binary; in the parent loop on `select([fd], ...)`, accumulate `os.read`, and `os.write` keystrokes when an anchor substring appears. Strip ANSI (`\x1b\[[0-9;?]*[ -/]*[@-~]` plus `\r`) before matching - cliclack repaints on every frame. Keys: Enter `b'\r'`, down `b'\x1b[B'`, up `b'\x1b[A'`, Ctrl-C `b'\x03'`.
- Sandbox every run: set `HOME`, `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_STATE_HOME` to temp dirs and `NO_COLOR=1`. An empty `HOME` makes source discovery find nothing, so the flow stays minimal. Seed `$XDG_CONFIG_HOME/pond/config.toml` to reproduce a starting state (legacy map, broken `storage.path`, etc.).
- Stop cleanly with Ctrl-C: `wiz()` turns it into "Cancelled - nothing written" and exits. `pond init` writes nothing and runs no external side effect (MCP/scheduler registration) until after the "Write config?" gate, so cancelling before it is side-effect-free - assert the seeded config is byte-identical and the sandbox `state`/`data` dirs are empty to prove it.
- Always set an overall deadline in the driver and send Ctrl-C on timeout, so a missed anchor can't hang the run.

## Source of truth

- `docs/spec.md` is the spec (sections 1-10: overview, scope, storage substrate, canonical model, session datasets, adapters, protocol, search and embeddings, deferred, references). Read the relevant section before changing behavior.

## Dependencies

- Lance crates are pinned to crates.io `7.0.0`. Don't bump without explicit ask.

## Errors

- `anyhow::Result` internally. Typed `pond::Error` (thiserror, `src/lib.rs`) at the wire boundary - the `Conflict` variant is load-bearing for OCC retry matching. `AdapterError` (`src/adapter/mod.rs`) is a struct (not enum) so adapter ingestion failures carry adapter + location for attribution.
- When a command fails or detects a recoverable bad state, its output names the fix - the exact recovery command or concrete next step, not just the symptom (e.g. an incomplete index points the user at `pond sync --reindex`).

## Repo layout

- One flat crate. `src/` holds the `adapter/` module folder alongside top-level files (`handlers.rs`, `sessions.rs`, `substrate.rs`, `transport.rs`, `wire.rs`, `embed.rs`, `config.rs`, `main.rs`, `lib.rs`). Unit tests live in `#[cfg(test)] mod tests` inside the file they test; `tests/` is for cross-module integration only.

## Documentation

- **Prefer ASCII.** Default to plain ASCII in Markdown and other repo docs - it keeps diffs clean, greps simple, and rendering predictable across terminals.

## Process

- Don't write migration notes or compatibility shims; pond is pre-release and breaking changes are free.
- Don't add or maintain changelog entries; pond has no changelog and doesn't need one.
- pond is a pre-1.0 release-plz-managed crate. Mark breaking commits with `<type>!:` (or a `BREAKING CHANGE:` footer) so release-plz bumps `0.X.0` instead of patch. When squash-merging a breaking PR, pass the subject via `gh pr merge --squash -t "feat(scope)!: ..."` so the `!` survives the squash.

## Minimalism

The only code that doesn't break is the code that doesn't exist. Keep pond the smallest codebase that satisfies the spec: no abstraction without a second caller, no generality without a second case, delete what stops earning its place. Floor: never cut a spec rule, a documented WHY, or a forward-compatibility seam - they exist precisely so they are not "simplified away."

## Comments

<comments>
A good pond comment names the WHY a reader can't see from the code itself: a hidden constraint, an invariant, an upstream workaround, behavior that would surprise someone reading just the symbols. Keep each as short as the WHY allows - one line when it fits, a few when the constraint genuinely needs more. Anchor to a `spec.md` section or rule-mnemonic anchor (e.g. `spec.md#adapters`, `spec.md#model-no-synthesis`) when one applies. Touch only comments on the code you're changing this turn; leave the rest as-is.

<example>
// spec.md#protocol: typed `conflict` for OCC failures, not `storage_unavailable`.
</example>
<example>
// Opt out of Lance's `_score` autoprojection; that default is being removed upstream.
</example>
<example>
// Local manifests are microsecond-cheap; refresh=0 removes the stale-read window for free.
</example>
</comments>

## Adapter seam (load-bearing)

- The adapter seam enforces correctness via types - synthesized values (sentinel strings, fallback defaults like `"unknown"`, `"function"`, `""`) MUST NOT compile, and the seam is transport-agnostic via `Source`/`Extracted<T>` so file, HTTP, and stream adapters share one set of primitives.
- Keep `src/adapter/mod.rs` as seam and registry only. Never put source-specific adapter details there: default install paths, fixture paths, source layout rules, source option schemas beyond generic seam contracts, restore path conventions, freshness heuristics, or adapter-specific probe tests. Put those in the concrete adapter module (`src/adapter/<adapter>.rs`) next to the factory/reader they describe.
- Unit tests live in `#[cfg(test)] mod tests` at the bottom of the source file they test; `tests/` is reserved for genuine cross-module integration suites only.

## Seam boundaries

- Seam modules define contracts, generic invariants, and cross-implementation helpers only. They must not accumulate implementation-specific policy from one source, provider, transport, storage backend, CLI surface, or fixture corpus. If a rule would need a concrete adapter/client/provider/backend name to explain it, keep it out of the seam and put it in the owning implementation module.
- Before adding a helper to a seam module, verify it has at least two real callers and no caller-specific assumptions. If it encodes a fallback path, naming scheme, freshness heuristic, restore shape, source option detail, provider request quirk, backend behavior, or UI convention for one implementation, it belongs beside that implementation instead.

## CLI output stack

- User-facing CLI output uses `clap` (parsing) + `indicatif` (progress + spinners) + `dialoguer` (interactive prompts) + `comfy-table` (width-adaptive tables) + `anstyle` (color, gated through `pond::output::paint` which honors `NO_COLOR` and non-TTY stdout). Don't reach for `crossterm`, `terminal_size`, `owo-colors`, `colored`, or `tabled`; pond standardizes on the stack above.
- New tabular surfaces should go through `pond::output` and the `new_table()` helper in `src/main.rs` so every command renders the same: borderless, dynamic-width, dim-bold headers.

## Test storage backends

- Use `shared-memory://pond-test-<unique-authority>/` only when a test needs 2+ `Store` instances against the same backing bytes (multi-writer OCC, fencing, future MemWAL). Single-`Store` tests use `tempfile::TempDir`.
- Authority MUST be unique per test (process-global cache; collisions = cross-test contamination under parallel `cargo test`).
- Never use `shared-memory://` in production code paths.
