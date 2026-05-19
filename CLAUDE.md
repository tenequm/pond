# Pond - project instructions

## Documentation

- **Prefer ASCII.** Default to plain ASCII in Markdown and other repo docs - it keeps diffs clean, greps simple, and rendering predictable across terminals.

## Process

- Don't write migration notes or compatibility shims; pond is pre-release and breaking changes are free.
- Don't add or maintain changelog entries; pond has no changelog and doesn't need one.

## Comments

<comments>
A good pond comment names the WHY a reader can't see from the code itself: a hidden constraint, an invariant, an upstream workaround, behavior that would surprise someone reading just the symbols. Keep each as short as the WHY allows - one line when it fits, a few when the constraint genuinely needs more. Anchor to `design.md <section>` or `invariant N` when one applies. Touch only comments on the code you're changing this turn; leave the rest as-is.

<example>
// design.md 3.6.1: typed `conflict` for OCC failures, not `storage_unavailable`.
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
- Unit tests live in `#[cfg(test)] mod tests` at the bottom of the source file they test; `tests/` is reserved for genuine cross-module integration suites only.

## CLI output stack

- User-facing CLI output uses `clap` (parsing) + `indicatif` (progress + spinners) + `dialoguer` (interactive prompts) + `comfy-table` (width-adaptive tables) + `anstyle` (color, gated through `pond::output::paint` which honors `NO_COLOR` and non-TTY stdout). Don't reach for `crossterm`, `terminal_size`, `owo-colors`, `colored`, or `tabled`; pond standardizes on the stack above.
- New tabular surfaces should go through `pond::output` and the `new_table()` helper in `src/main.rs` so every command renders the same: borderless, dynamic-width, dim-bold headers.

## Test storage backends

- Use `shared-memory://pond-test-<unique-authority>/` only when a test needs 2+ `Store` instances against the same backing bytes (multi-writer OCC, fencing, future MemWAL). Single-`Store` tests use `tempfile::TempDir`.
- Authority MUST be unique per test (process-global cache; collisions = cross-test contamination under parallel `cargo test`).
- Never use `shared-memory://` in production code paths.
