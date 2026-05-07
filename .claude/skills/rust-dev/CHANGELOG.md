# Changelog

All notable changes to this skill will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this skill adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
