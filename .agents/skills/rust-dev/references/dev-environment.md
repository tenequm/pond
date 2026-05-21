# Development Environment

Setting up a fast edit-compile-test loop, and keeping it fast as the project grows. None of this is needed on day 1 - reach for it when builds start to feel slow.

## The fast inner loop

The commands, fastest to slowest:

- `cargo check` - type-checks without code generation. This is your inner loop; run it constantly.
- `cargo clippy` - `check` plus lints. Set your editor to run this on save.
- `cargo build` / `cargo run` - full code generation.
- `cargo test` - build plus run tests.

In your editor, point rust-analyzer's check command at clippy so you get lint feedback inline (in VS Code: `"rust-analyzer.check.command": "clippy"`). rust-analyzer itself does most type-checking as you type; `cargo check` is the fallback the editor runs to populate diagnostics.

## Build speed

Two things make Rust builds slow: compiling code, and linking it. The fixes differ by platform.

### macOS

Set up `sccache` and stop there. It is a compilation cache: install it (`brew install sccache`) and point Cargo at it once, globally, in `~/.cargo/config.toml`:

```toml
[build]
rustc-wrapper = "sccache"
```

That gives you a persistent, global cache of compiled **dependencies** - shared across every project, and surviving `cargo clean` and branch switches. Most library dependencies are cached after building once; note that proc-macro and other linker-invoking crates cannot be cached by sccache, so those still recompile.

What sccache does *not* do: speed up rebuilds of your own workspace crates. Those use incremental compilation, which sccache cannot cache - and that is fine, leave incremental on, it already makes your own crates rebuild fast. sccache covers the other half (dependencies).

Do not spend time on alternative linkers on macOS. The fast third-party linkers (`mold`, `wild`) are ELF/Linux-only and cannot link macOS Mach-O binaries, and Apple's bundled linker is already fast. There is no win there - sccache is the whole story.

### Linux

You already have the linker win for free: since Rust 1.90, `x86_64-unknown-linux-gnu` uses the bundled `rust-lld` linker by default (around 7x faster incremental linking than GNU `ld`, no setup). `mold` is faster still if you want to go further, via `.cargo/config.toml`:

```toml
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

Profile before assuming linking is your bottleneck - often it is not. On Linux, `sccache` mainly helps cold builds and CI rather than the warm local loop.

### CI

A minimal, reproducible GitHub Actions gate is three commands behind one toolchain action:

```yaml
name: ci
on: [push, pull_request]
permissions:
  contents: read          # least-privilege GITHUB_TOKEN
jobs:
  check:
    strategy:
      fail-fast: false     # one OS failing still shows the other
      matrix:
        os: [ubuntu-latest, macos-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v6
      - uses: actions-rust-lang/setup-rust-toolchain@v1
      - run: cargo fmt --check
      - run: cargo clippy --locked -- -D warnings
      - run: cargo test --locked
```

`actions-rust-lang/setup-rust-toolchain` reads `rust-toolchain.toml` for the channel and components and bundles `Swatinem/rust-cache` - toolchain install and build cache in one step, no separate cache action. `--locked` fails CI if `Cargo.lock` is stale; `clippy -- -D warnings` turns every lint in your `[lints]` table into a CI failure. Do not add `sccache` on top - it competes with the bundled cache for the same Actions cache budget. If you do use `sccache` in CI, set `CARGO_INCREMENTAL=0` so it can cache every compilation.

## Optimizing dependencies in dev builds

Debug builds are slow at runtime because nothing is optimized. If a dependency does heavy computation (image processing, crypto, compression) and that dominates your dev-run time, optimize just the dependencies while keeping your own crates unoptimized for fast compiles:

```toml
[profile.dev.package."*"]
opt-level = 3
```

Your crates still compile fast; the dependencies, compiled once, then run fast.

To speed up your own crate's debug rebuilds, set `[profile.dev] debug = "line-tables-only"` - it keeps file and line numbers for panics and backtraces but drops the heavier per-variable debug info.

## File watchers

`bacon` runs `check`/`clippy`/`test` in a loop, re-running on save, with a compact summary view. Install with `cargo install bacon` and run `bacon` in the project. Optional, but a nice background companion to the editor.
