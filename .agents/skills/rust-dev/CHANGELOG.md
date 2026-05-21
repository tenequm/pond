# Changelog

All notable changes to this skill will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this skill adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-05-21

### Added
- New reference references/testing.md: a pragmatic, anti-dogma testing guide - what to test vs what to skip, the purity-over-extent reframe, "mock resources, not your own code", reproduce-then-fix regression tests, test organization (tests with the code mirroring the source layout; `tests/` for real integration tests only; `tests/common/mod.rs`; the single-binary `#[path]` layout; fixtures in `tests/fixtures/` via `env!("CARGO_MANIFEST_DIR")` with a fixtures README), doctests, async tests, and a minimal high-value tool kit.
- New reference references/dev-environment.md: development setup and build / dev-loop speed - `cargo check` as the fast inner loop, incremental compilation, platform-aware build-speed guidance (on macOS, set up sccache as a global dependency cache and skip linker tuning; on Linux, `rust-lld` is the default linker since Rust 1.90), a minimal CI workflow recipe built on `actions-rust-lang/setup-rust-toolchain`, dev-profile tuning, and `bacon`.
- New reference references/performance.md: runtime performance of real apps - the measure-first discipline and keeping perf harnesses in a committed home (not `/tmp` or inline shell), profiling (the `profiling` build profile, samply, `dhat` for heap), benchmarking (`benches/` + `harness = false`, criterion vs divan, `std::hint::black_box`, CI regression benchmarking with CodSpeed/Bencher), the `[profile.dist]`-vs-`[profile.release]` split, and common real wins.
- references/async-basics.md: note that `tokio::fs` is backed by `spawn_blocking` (no true async file I/O on most OSes) plus batching guidance.
- SKILL.md Day-1 Setup: `cargo update`, with a note that it only moves within existing semver ranges.
- SKILL.md idioms: the compiler-as-worklist refactor trick (a mandatory no-`Default` field turns every constructor into a compile error).
- SKILL.md: formatting-discipline note in the rustfmt section; "testing", "benchmarking", and build-speed triggers added to the skill description.

### Changed
- Adopted sqlx 0.9.0 (released 2026-05-06): bumped the dependency in references/crate-shortlist.md from 0.8 to 0.9 and added a 0.9 notes callout (repo moved to the transact-rs org; MSRV raised to 1.94; runtime `query()`/`query_as()` functions now take `impl SqlSafeStr` - the `query_as!` macro is unaffected).
- Bumped tracked axum version 0.8.8 -> 0.8.9.
- SKILL.md: dropped `unwrap_used`/`expect_used` from the Minimal Cargo.toml lints - both are clippy `restriction`-group lints (a deliberate per-project opt-in, not a day-1 default), and linting `expect_used` discourages `.expect("reason")`, the recommended way to document a can't-fail invariant. Anti-pattern #4 refined accordingly; references/error-handling.md now covers `unwrap_used` as an opt-in with the companion `clippy.toml` `allow-unwrap-in-tests` setting.
- SKILL.md: reframed the `Rc<RefCell>`/`Arc<Mutex>` anti-pattern to first ask whether shared mutable state is needed at all (plain ownership or a channel is usually cleaner).

### Fixed
- references/crate-shortlist.md: axum 0.8 MSRV corrected from 1.78 to 1.80 (raised in axum 0.8.9); jiff example now imports `ToSpan` (the `1.hour()` snippet did not compile); added a reqwest 0.13 notes callout (rustls is now the default TLS backend, `query`/`form` are now opt-in features).
- references/traits-and-generics.md and async-basics.md: "object safety" updated to the current term "dyn compatibility" (renamed in Rust 1.83).
- references/async-basics.md: corrected the `tokio::task::block_in_place` description - it runs a blocking section inside the current task and panics on a `current_thread` runtime; it does not bridge async to sync.
- SKILL.md: the `?`-propagation example uses `toml::from_str` (the canonical API) instead of `toml::from_slice`; the `.gitignore` no longer teaches the retired "ignore `Cargo.lock` for libraries" rule; "Recent sugar" now names the Rust 1.95 feature `if let` guards (not let-chains); clippy `uninlined_format_args` comment corrected to Rust 1.89.0 (mid-2025).
- references/ownership-and-types.md: `Copy` types list corrected to include floats and arrays.
- references/dev-environment.md: clarified that sccache cannot cache proc-macro or other linker-invoking crates.
- references/testing.md: doctest guidance updated for the 2024 edition (compatible doctests are merged); softened two over-absolute claims.

Verified against: rust@1.95.0, axum@0.8.9, reqwest@0.13.3, sqlx@0.9.0, jiff@0.2.24

## [0.2.0] - 2026-05-07

### Changed
- Bumped axum guidance from 0.7 to 0.8 in SKILL.md and references/crate-shortlist.md (path syntax `/{id}` instead of `/:id`, `Option<T>` extractor reworked, `Host` moved to `axum-extra`, MSRV 1.78).
- Bumped reqwest guidance from 0.12 to 0.13 in references/crate-shortlist.md.
- sqlx offline-mode docs: replaced `sqlx-data.json` with `.sqlx/` directory; added notes on `--workspace`, `--check`, and `SQLX_OFFLINE=true`.
- chrono/jiff guidance rewritten: chrono soft-deprecated by maintainer (Jan 2026); jiff is recommended for new code but still pre-1.0. Updated SKILL.md crate table and references/crate-shortlist.md chrono section.
- Refreshed stale "as of April 2026" timestamp in rustfmt section.

### Added
- Brief mention of Rust 1.95 sugar (`cfg_select!` macro, let-chains in match arm guards) in the idioms section of SKILL.md.
- Comment in Cargo.toml lints noting that clippy's `uninlined_format_args` was moved to `pedantic` (allow-by-default).
- Expanded `metadata.upstream` to track volatile crates: `axum`, `reqwest`, `sqlx`, `jiff` alongside `rust`.

## [0.1.0] - 2026-04-29

### Added
- Initial release. Practical day-1 Rust development skill.
- SKILL.md covering the Rust mental model (ownership, aliasing XOR mutability, errors as values, traits not interfaces, the borrow checker as design oracle), the 3 questions for every function signature, day-1 decision table, idioms to internalize early, "coming from X" deltas (Python/JS, Go, Java/C#, C++), the crate shortlist, top anti-patterns, what to defer, minimal Cargo.toml + lints + rustfmt.toml + rust-toolchain.toml, project structure, learning path.
- references/ownership-and-types.md: ownership, borrowing, lifetimes, `String`/`&str`/`Cow`, `Vec`/slice/array, smart pointers (`Box`/`Rc`/`Arc`/`RefCell`/`Mutex`), `MutexGuard` across `.await` pitfall, self-referential struct trap.
- references/error-handling.md: `Result`, `?`, `anyhow` for apps, `thiserror` for libraries, custom error enums, `panic!` vs `unwrap` vs `expect`, `From`-driven error conversion.
- references/traits-and-generics.md: trait definitions, generic vs `impl Trait` vs `dyn Trait`, object safety, common derives, `From`/`Into`/`TryFrom`, `Display`/`Debug`, blanket impls, the orphan rule, iterator trait, common stdlib traits, generics flavors (type/lifetime/const).
- references/async-basics.md: `tokio` runtime, `#[tokio::main]`, spawning, `Send`/`Sync`/`'static` bounds, `MutexGuard` pitfalls, `spawn_blocking`, `select!`/`join!`/`try_join!`, channels, async in traits, common pitfalls.
- references/crate-shortlist.md: minimal usage examples for `serde`/`serde_json`, `tokio`, `anyhow`, `thiserror`, `clap`, `reqwest`, `tracing`, `axum`, `sqlx`, `chrono`, plus an honorable-mentions table.

Verified against: rust@1.95.0
