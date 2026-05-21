# Performance

Making a Rust app actually fast at runtime. The order matters: measure first, optimize second. Rust is fast by default, so most "optimizations" written without a profiler are guesses that add complexity for no gain.

## Measure first

Never optimize without a profiler pointing at the hot spot. The workflow:

1. Build with optimizations (`cargo build --release` - debug builds are not representative).
2. Profile under a realistic workload.
3. Fix the actual hot spot the profiler shows.
4. Measure again.

## Keep the harness

Whatever you build to answer a performance question - a benchmark, an A/B timing comparison of two builds, a memory/CPU monitor over a full run - is a reusable asset, not scratch work. Its value is being re-run after the *next* change, to confirm you did not regress. Give it a committed home: a Rust benchmark or stress harness goes in `benches/` (below); a whole-program profiling or timing script goes in a committed `scripts/` or `xtask/` directory. Never leave it in `/tmp` or as inline shell - then the next person to ask the same question rebuilds it from scratch.

## Profiling

A profiler needs optimized code *and* debug symbols, but a plain `--release` build strips the symbols - so the profile comes back as unreadable `[unknown]` frames. Add a `profiling` profile once:

```toml
[profile.profiling]
inherits = "release"
debug = true
```

- **`samply`** is the go-to sampling profiler: `cargo install --locked samply`, build with `cargo build --profile profiling`, then `samply record ./target/profiling/my-app`. It opens an interactive Firefox Profiler view in the browser; works on macOS, Linux, and Windows (on Linux, grant perf access once with `sysctl kernel.perf_event_paranoid=1`).
- **`cargo-flamegraph`** produces a static flamegraph SVG; `samply` has largely displaced it for interactive work.
- For **heap profiling** - what allocates and how much - the `dhat` crate gives in-process, native-speed heap profiling: add it, set its global allocator, and view the profile in the DHAT online viewer (cross-platform, unlike Valgrind).
- The **Rust Performance Book** (https://nnethercote.github.io/perf-book/) is the canonical guide - read it before reaching for tricks.

For a release profile that is representative and ships fast:

```toml
[profile.release]
lto = "thin"        # link-time optimization across crates
codegen-units = 1   # less parallelism, better optimization
```

## Benchmarking

To compare two implementations or guard against regressions, write a benchmark - do not eyeball it.

Benchmarks live in `benches/`, declared in `Cargo.toml` with `harness = false` so the bench crate provides its own `main`:

```toml
[[bench]]
name = "parse"
harness = false
```

`cargo bench` builds them with the optimized `bench` profile automatically (it inherits `release`) - no `--release` flag to remember.

Two harnesses:

- **`criterion`** (`criterion = "0.8"`) - the standard. Statistical analysis, warmup, outlier detection, regression tracking between runs.
- **`divan`** (`divan = "0.1"`) - simpler and lighter, less machinery.

The one thing you must get right: the compiler will delete a benchmark's work if it sees the result is unused, giving a fake "0 ns" result. Wrap inputs and outputs in `std::hint::black_box` so the optimizer cannot see through them:

```rust
use std::hint::black_box;

c.bench_function("parse", |b| {
    b.iter(|| parse(black_box(INPUT)));
});
```

A note on profiles: `cargo bench` inherits `[profile.release]`, so an aggressive release profile (`lto`, `codegen-units = 1`) makes every `cargo bench` re-do a slow whole-program link. If your bench loop feels sluggish, move the ship-grade settings into a dedicated profile and keep `release` lighter:

```toml
[profile.dist]      # build shipping artifacts with `cargo build --profile dist`
inherits = "release"
lto = "fat"
codegen-units = 1
```

To catch performance regressions automatically, run benchmarks in CI. **CodSpeed** is the low-friction default: rename your import to `codspeed-criterion-compat` (or `codspeed-divan-compat`), add its GitHub Action, and it posts low-variance per-PR comparisons; **Bencher** (https://bencher.dev) is the self-hostable alternative. For quick whole-program or CLI A/B timing with no code at all, `hyperfine` runs a command repeatedly and compares.

## The real wins

Before micro-optimizing, the changes that actually move the needle in typical Rust apps:

- **Allocations.** Needless `.clone()` on a hot path, or building a `Vec` without `Vec::with_capacity` when the size is known, are the most common real costs. A profiler will point you straight at them.
- **Data parallelism.** For CPU-bound work over a collection, `rayon` turns `.iter()` into `.par_iter()` and uses every core - often the biggest win for the least code.
- **Avoid premature `Arc`/`Box`/`dyn`.** Indirection has a cost; reach for it when the design needs it, not preemptively.
- **Async is for I/O concurrency, not speed.** If your workload is CPU-bound, async adds overhead without benefit - use threads or `rayon`.

And the discipline that ties it together: a change is only an optimization if a benchmark or profiler says so. Otherwise it is just added complexity.
