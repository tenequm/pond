# Testing

Rust's compiler already eliminates whole bug classes - null derefs, data races, use-after-free, most type errors. That changes the testing calculus: tests that re-verify what the type system guarantees are wasted effort. This file is about the tests that actually earn their keep, and how to organize them so the suite stays fast enough that you keep running it.

## What to test, what to skip

The ROI is in what the compiler cannot see:

- Business logic, calculations, ranking/ordering rules.
- Parsers, serializers, encoders - especially round-trips and edge cases.
- State machines and their transitions.
- Error paths - the `Err` arms, not just the happy path.
- Arithmetic that can overflow, wrap, or truncate.
- Behavior at integration boundaries (your code against a DB, an HTTP API, the filesystem).

Skip:

- Anything the type system already guarantees (an enum cannot hold an out-of-range variant; a `NonZeroU32` cannot be zero).
- Trivial getters, setters, and `Default` impls.
- Tests that assert a dependency's behavior - that is the dependency's job, and the test breaks every time you bump the version.
- "Smoke tests" whose only assertion is "did not panic" or "returned `Ok`".
- Constants checked against themselves (`assert_eq!(MAX, 100)` where the code says `MAX = 100`).

A useful filter: **write tests for features, not for code.** A good test survives a full reimplementation - if you replaced the function body with an opaque black box that produced the same outputs, the test should still pass. Tests coupled to internal structure break on every refactor and tell you nothing.

Watch for weak assertions. `assert!(count >= expected)` passes for buggy code that emits too much; a count-based assertion (`batches == 4`) goes stale the moment batching changes. Assert durable invariants and exact values.

## The reproduce-then-fix habit

The single highest-ROI testing habit: when you find a bug, write the failing test *first*, watch it fail, then fix the bug. Confirm the test actually catches the bug by reverting the fix and seeing it go red again. A regression test that still passes when you delete the fix is worthless - and that happens more often than you would think.

## Purity over "unit vs integration"

Stop sorting tests into "unit" and "integration" buckets. Sort them by **purity** - how much I/O they do:

- A pure test calls a function with in-memory inputs and checks the output. It is fast and it cannot be flaky.
- An impure test touches the filesystem, network, a database, the clock, or global state. It is slower and can flake.

Optimize purity hard. I/O - not lines of code under test - is what makes a suite slow and flaky. Push logic into pure functions and test those; keep the impure shell thin.

This is why **you should not mock your own code.** Mocking internal layers to make a test "smaller" lowers its fidelity and couples it to your current structure, so it breaks on refactor. Mock *resources*, not code: replace the external, impure things (an HTTP call, a clock, the filesystem) with a fast in-process fake, and let the test exercise as much of your real code as it naturally reaches. A hand-written fake - a `HashMap`-backed store behind your repository trait - usually beats a mocking framework.

## Test organization

Rust gives you two homes for tests.

**Default: keep tests with the code they test.** Unit tests for `src/parser.rs` belong in `src/parser.rs` - they refactor, move, and get read together with the code, and being co-located means the test layout mirrors the source layout for free. Reserve `tests/` for genuine integration tests: end-to-end exercises of the public API or cross-module behavior. Do not promote a unit test to `tests/` just because it grew - there it loses access to private items and pays a separate-binary link cost for nothing.

**Unit tests** live in the file they test, in a `#[cfg(test)]` module. They can reach private items.

```rust
// src/parser.rs

pub fn parse(input: &str) -> Result<Ast, ParseError> { /* ... */ }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_input() {
        assert_eq!(parse("").unwrap(), Ast::Empty);
    }
}
```

For a long test module, move it to its own file and declare it - Cargo then skips recompiling the library crate when you only change tests:

```rust
// at the bottom of src/parser.rs
#[cfg(test)]
mod tests;          // lives in src/parser/tests.rs
```

**Integration tests** live in `tests/`. Each file there is compiled as a *separate crate* that links your library and uses it through its public API only - the same surface your users see.

```
tests/
  api.rs            # one test binary
  common/
    mod.rs          # shared helpers - NOT a test binary
```

Auto-discovery of `tests/` is flat: each top-level `.rs` becomes its own test binary, subdirectories do not. That is why shared helpers go in `tests/common/mod.rs` (the `mod.rs` form), imported with `mod common;` in each test file. A plain `tests/common.rs` would be compiled and run as its own (empty) test binary.

One refinement: for a **published library**, also keep at least one integration test in `tests/` that drives the real public API - unit tests alone cannot catch API-ergonomics problems. If an integration suite grows enough to want structure, give it a module tree under one binary (`tests/it.rs` plus a `tests/it/` directory) so the layout can mirror the public surface without paying a separate link step per file.

## Fixtures

Put test data files - sample inputs, golden outputs, config samples - in `tests/fixtures/` by default. Cargo's flat `tests/` discovery ignores subdirectories, so `tests/fixtures/` is a safe home for pure data; it is never compiled as a test binary. Mirror the source layout under it - `tests/fixtures/adapter/claude-code/` for `src/adapter/claude-code.rs` - so a fixture's path is predictable from the module it belongs to.

Build fixture paths from `env!("CARGO_MANIFEST_DIR")`, the compile-time absolute path to the crate root. It resolves identically from a unit test in `src/` and an integration test in `tests/`, regardless of the process working directory - this is what lets a test co-located with the code load a shared `tests/fixtures/` file cleanly:

```rust
let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sample.json");
let input = std::fs::read_to_string(path).unwrap();
```

The one justified exception: a small, rarely-changing fixture tied to one module can be co-located with the source and pulled in with `include_str!` / `include_bytes!` (which resolve relative to the current file). That bakes the data into the binary and forces a recompile whenever it changes, so keep it for small, stable fixtures only - and note `include_*!` cannot pull in a directory tree, so multi-file fixtures must live under `tests/fixtures/` regardless.

For non-trivial fixtures, keep a short `tests/fixtures/README.md` noting where the data came from and any anonymization applied - it stops fixtures from becoming opaque blobs nobody dares change.

## Keep the suite fast

A slow suite is one developers stop running - then they push untested code, which is worse than no suite. Speed is the binding constraint.

- **Each file in `tests/` is a separate binary that links separately.** Linking dominates `cargo test` wall-clock for crates with heavy dependencies. Consolidate many suites into one binary - keep a single `tests/integration.rs` and pull each suite from a subdirectory:

  ```rust
  // tests/integration.rs - the only file Cargo builds as a test binary
  #[path = "integration/api.rs"] mod api;
  #[path = "integration/db.rs"]  mod db;
  ```

  `#[path]` is required: `tests/integration.rs` is a crate root, so a bare `mod api;` would resolve to `tests/api.rs`, which Cargo's flat discovery would build as *another* binary. Pointing `#[path]` into `tests/integration/` (a subdirectory Cargo ignores) keeps every suite in one binary - often a several-fold test-build speedup.
- Gate genuinely slow tests behind an environment variable so they run on CI but not in the local loop. Do not hide them with `#[cfg(...)]` - conditional compilation means they rot.
- Doc tests are compile-checked documentation and run under `cargo test` - keep them for public-API examples. The 2024 edition merges compatible doctests into one compilation unit, so the old "doctests are slow to build" concern is largely gone (they still each run in their own process).

## Async tests

Use `#[tokio::test]`. For tests that need real concurrency (two tasks actually contending), use the multi-thread flavor:

```rust
#[tokio::test]
async fn fetches_user() {
    assert_eq!(fetch_user(1).await.unwrap().id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_writers_contend() { /* ... */ }
```

Fire-and-forget concurrency (`spawn` something and drop the handle) is hard to test deterministically - you end up adding `sleep`s to "wait for it." Do not. Design for joinability: return the `JoinHandle`, or signal completion through a channel, so a test can deterministically wait.

## Test isolation

Tests run in parallel. Anything touching shared external state needs per-test isolation:

- Filesystem: `tempfile::TempDir` gives each test its own directory, cleaned up on drop.
- Databases: `#[sqlx::test]` creates and migrates a fresh database per test automatically.
- In-memory resources keyed by name: generate a unique name per test (e.g. a UUID) so parallel tests do not collide.

`cargo-nextest` runs each test in its own process, which makes isolation failures (tests that share a global, or call `std::env::set_var`) surface as real failures. Treat those as latent bugs to fix, not a reason to serialize the suite.

## Coverage is a diagnostic, not a target

Coverage percentage has sharply diminishing returns - the last 10% costs far more than the first 90% and rarely catches proportional bugs. Use a coverage tool (`cargo-llvm-cov`) to *find* untested branches you care about, then decide case by case. Do not wire a coverage threshold into CI and chase a number; that pressure produces low-value tests that exist only to move the metric.

## The tool kit

Start with what the toolchain gives you for free - `#[test]`, `#[cfg(test)]`, `#[tokio::test]`, doctests, parallel execution. Many applications need nothing more.

Add as defaults:

| Tool | Why |
|---|---|
| `cargo-nextest` | Faster runs and per-test process isolation for multi-binary workspaces. It does not run doctests - keep `cargo test --doc` as a separate step. Its `slow-timeout` with `terminate-after` auto-kills hung tests. |
| `rstest` | Parametrized tests: one `#[case(...)]` per row generates an independent, individually-named test. Genuinely cuts table-test boilerplate. |

Reach for these when a specific need appears:

| Tool | Use it when |
|---|---|
| `insta` | Output is large or structured (serializer output, API responses, CLI text). Review every diff with `cargo insta review`; never blind-run `cargo insta accept` - an unreviewed baseline certifies nothing. |
| `wiremock` | Your code calls a third-party HTTP API - run a real local mock server instead of mocking the HTTP client. |
| `#[sqlx::test]` | Database code - per-test isolated database, lighter than `testcontainers`. |
| `assert_cmd` + `predicates` | You ship a CLI binary and want to assert on exit code / stdout / stderr. |
| `proptest` | You have an invariant or a reference implementation to check against - parsers, codecs, round-trips. Property testing is a *search* strategy: it finds the input combinations you would never write by hand. |
| `cargo-llvm-cov` | You want a coverage signal (see above - signal, not target). |

Usually skip:

- **Mocking frameworks (`mockall`).** Idiomatic Rust - traits plus hand-written fakes - covers most needs without the proc-macro machinery. Reach for `mockall` only on large trait surfaces where you need many call-count/argument assertions.
- **`quickcheck`** - `proptest` is generally preferred (its integrated per-value shrinking is more flexible).
