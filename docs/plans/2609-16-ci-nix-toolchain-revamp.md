# CI and dev toolchain revamp: the flake as the single tool source

Date: 2026-09-16. Owner: tenequm. Status: proposed, nothing executed.
Companion: the runner and cluster side (image, store volume, cold-start tuning,
binary-cache bucket and keys) is planned in a private ops repo.

Provenance: four research passes on 2026-09-16 over moon (repo at 2.5.5), Nix
(2.35.2 manual and release notes), kache (0.22.0 + main), and GitHub Actions/ARC
(runner-images, runner scale sets), plus the last 60 `ci.yml` runs
(2026-09-10 to 2026-09-16). Claims below marked "measured" come from that run
history; file citations are from the tools' own repos.

## 1. Where walltime goes today (measured)

| Cost | Size | Cause |
|---|---|---|
| windows-verify kache warm pull | 530-820s per run; in 9 of 31 pulling runs the test step was then a moon replay (12-36s), so ~650s was pure waste | The "is a pull needed" path regex matches more than moon's real task inputs; the prefix holds ~2x the ~1450-unit working set (pull grew 45s -> 156s -> ~650s) |
| pond-ci queue | median ~182s cold (28/60 runs) vs ~24s warm; two runs waited 944s/1510s behind another job (single-runner scale set) | the runner node pool scales to zero when idle, and the large runner image is re-pulled on every cold node |
| build-and-test | 10-15s on a moon cache hit; 90-105s on a real build | On hits, every suite still runs, including two uncached `npm ci` |
| bootstrap / flake-check | 1-4s / ~30s | Fine; not worth optimizing |

## 2. Target design

### 2.1 The flake is the single source of truth for every tool

- **Rust**: `rust-overlay` with `rust-bin.fromRustupToolchainFile ./rust-toolchain.toml`
  (pure eval; manifests are vendored in the overlay repo). `rust-toolchain.toml`
  remains the one Rust pin; the flake reads it. fenix rejected: its
  `fromToolchainFile` is either impure or import-from-derivation, and it drops
  unknown components silently.
- **Toolchain input isolation**: the overlay (and zig, cc, cmake) apply to a
  separate `nixpkgs-toolchain` flake input that is bumped deliberately. Routine
  `nixpkgs` bumps then change no compiler store paths, so cargo does not rerun
  build scripts and kache/moon keys do not miss. Without this, every lock bump
  costs one ~100s-class cold build.
- **From nixpkgs at the exact versions already pinned**: zig 0.16 (plus the
  `synchronization.def` copy as a package patch - moved out of the runner
  image), cargo-zigbuild 0.23.4, rcodesign 0.29.0, gh.
- **Custom fetch packages** (the infra-repo pattern: fetchurl + install):
  macOS SDK 15.5 (sets `SDKROOT` in the shell), moon, protoc, uv, node/npm,
  kache (static musl release tarball).
- **Windows reads the same versions as text**: `sed`/`Select-String` extraction
  from the flake's `toolVersions` attrset, plus a flake-check step that fails
  when text extraction and `nix eval --json .#lib.toolVersions` disagree. This
  removes the "bumped in lockstep, never alone" comments as a manual protocol.
- The devShell keeps cmake, protobuf, pkg-config, clang/libclang (bindgen) -
  everything `bootstrap/action.yml` and the runner image install today.
- Set `CARGO_HOME` in the devshell to a directory with no rustup proxies:
  moon's rust plugin puts `$CARGO_HOME/bin` first on PATH, and a leftover
  rustup proxy would shadow the Nix rustc (plugins `toolchains/rust/src/tier2.rs`).

### 2.2 CI devshell entry (replaces bootstrap/action.yml entirely)

One step at the top of each pond-ci job:

1. `key = hash(flake.nix, flake.lock, rust-toolchain.toml)`.
2. If profile `/nix/var/nix/profiles/pond-dev-$key` exists:
   `nix print-dev-env --json <profile>` (skips evaluation - the flake eval cache
   is keyed on the git fingerprint, so every commit misses it; the profile path
   does not). Else: `nix print-dev-env --json --profile <profile> .#` (also
   creates the GC root).
3. Pipe through `jq` into `GITHUB_ENV`/`GITHUB_PATH`, filtering `TMPDIR`,
   `HOME`, `NIX_BUILD_TOP` and similar.

Later steps run with zero Nix overhead (no `nix develop -c` wrappers).

Design rule this depends on: **the devShell must not reference `self` or the
source tree**. Its derivation then depends only on the three key files, which
is what makes the profile key sound - and keeps the shell recipe identical
across commits, so a warm run is evaluation-free.
`bootstrap/action.yml` is deleted; its two survivors move:
- the cache env exports (`CARGO_HOME`, `CARGO_TARGET_DIR`, `RUSTUP_HOME` gone,
  `PROTO_HOME` gone, npm/uv caches, `KACHE_CONFIG`, `KACHE_RUNTIME_DIR`) become
  workflow-level `env:`/devshell exports;
- the kache prefix-vs-env assertion becomes a step in the dist job (or lives in
  the build-dist script, see 2.4).

One-time cleanup once the bootstrap is gone: delete `/ci-cache/{proto,rustup,bin}`
and the rustup proxies in `/ci-cache/cargo/bin` (keep the registry cache).
This is required, not housekeeping - moon's rust plugin prepends
`$CARGO_HOME/bin` to PATH, so leftover proxies would silently shadow the Nix
rustc (the pitfall in 2.1).

nix.conf on the runner (set on the ops side; recorded here for review):
`http-connections = 50`, `max-substitution-jobs = 32`,
`download-buffer-size = 268435456`, `narinfo-cache-negative-ttl = 0`,
`sandbox = false`, single-user store on its own PVC. Nix itself must be
>= 2.35: lazy source copying stops each evaluated commit from writing the
whole source tree into the store, and uploads/substitutions got parallel
there.

### 2.3 moon

- **Upgrade 2.5.4 -> 2.5.5** (no breaking changes; cached changed-files
  aggregation, failed hydration no longer deletes outputs, `moon query`
  honors `runInCI` in CI).
- **`.moon/toolchains.yml` goes versionless**: `javascript: {}`, `node: {}`,
  `npm: {}`, and add `rust: {}`. With no `version` keys, proto downloads
  nothing and moon uses PATH (the devshell). `rust: {}` puts OS/arch/libc into
  task hashes, so a macOS dev or agent reading the remote cache can no longer
  be served a Linux result. Revisit `unstable_python`/`unstable_uv` (untested
  versionless).
- **Add `/flake.lock` to the hashed inputs** (shared fileGroups). With versions
  out of the config, nothing else ties task hashes to the toolchain; without
  this a toolchain bump silently reuses stale cached results.
- **`hasher.optimization: 'performance'`**: skips parsing the (large)
  `Cargo.lock` into every task hash; the lockfile stays a file input, so
  dependency changes still invalidate.
- **`build-and-test` switches to `moon ci`** (same targets): affected-only
  drops untouched suites before hashing, which removes the uncached `npm ci`
  runs on most PRs - likely most of the 10-15s hit-path. Full `fetch-depth: 0`
  checkout stays. Caveats: `main` diffs against HEAD~1 unless a base is given,
  so multi-commit pushes need `MOON_BASE` (or accept the default); **dist-build
  and publish-release keep `moon run`** - under affected-only, a release commit
  touching no inputs would build nothing and publish nothing.
- **Remote cache for humans and agents**: `remote.cache.localReadOnly: true`
  gives dev machines and agents read-only hits on CI-built results (only after
  `rust: {}` lands, for the OS-keyed hashes). Enable
  `experiments.casOutputsCache` + `cache.unstable_sharedWorktreeCache` so the
  main checkout and worktrees share one local cache.
- **Cleanups**: the ci.yml comment claiming background uploads abort at 60s is
  stale (300s since moon 2.5.2; the `MOON_DAEMON_RUNNING=1` inline-wait
  workaround may no longer be needed - test, then simplify or fix the comment).
  Move the `build-dist` inline script body to `ops/scripts/` files listed as
  inputs, so comment edits stop invalidating its hash.
- **Agents**: document `MOON_OUTPUT_STYLE=buffer-only-failure` + `--summary`,
  reading `.moon/cache/runReport.json` (and `ciReport.json` from `moon ci`),
  and `moon mcp` in AGENTS.md. Make the plugin `npm ci` tasks skippable via a
  v2.4 `condition` stamp against `package-lock.json` so agents stop reinstalling
  node_modules on every run.

### 2.4 kache

- **0.22.0** (from 0.21.0 in the rust-1.98 PR): `CACHE_KEY_VERSION` is 31 from
  0.17.0 through 0.22.0, so the k31 prefixes stay; no cold run. 0.22 adds the
  reflink-restore write fix, GC-responsive daemon, rustls audit bump.
- **Prewarm goes through pushes to main.** Since 0.17 kache forces read-only
  for every GitHub event except a push to a protected branch
  (`src/policy.rs:55-59`); `KACHE_REMOTE_READONLY=0` cannot override it. The
  documented "forced prewarm dispatch" therefore pulls but never writes - warm
  new prefixes with a main push instead, and fix the comment in
  `.github/kache/windows-dist.toml`.
- **Persistent Linux runner**: `prefetch_enabled = false` (docs recommendation
  for persistent runners); keep `KACHE_RUNTIME_DIR` on job scratch; if the
  local store moves to the PVC use `KACHE_CACHE_DIR` + `KACHE_TRUST_DOMAIN`
  (new in 0.21).
- **Keep kache out of build-and-test.** The warm `CARGO_TARGET_DIR` beats it;
  kache adds a wrapper per unit and strips incremental. Only reconsider after
  a PVC wipe pattern emerges, and then with preserve-incremental (below).
- **Windows**: keep the kache-action SHA pin and explicit `version:` (empty
  means "latest"); leave `cache-c-cpp` off (it swaps MSVC `cl` for clang-cl);
  a pre-set `KACHE_CONFIG` is honored, so the committed-file remote stays.
- **Diagnostics**: run `kache why-miss aws_lc_sys` once - if its build-script
  `.a` embeds `OUT_DIR`, dependents miss across target-dir locations and that
  is worth knowing before trusting cross-machine sharing. Verify the C-compile
  path under the Nix cc wrapper once with `KACHE_VERIFY=1`.

### 2.5 Windows leg (biggest measured win)

1. **Skip the warm pull when moon will replay.** Replace the path-regex guess
   with moon's own verdict (hash/dry-run check for `pond:test-msvc` etc.), or
   run the pull as a parallel background step next to the build (parallel
   steps are GA since 2026-06). Only skip on a confirmed hit - a lazy pull
   reintroduces per-object demand fetches. Expected: ~-650s on ~30% of
   windows-verify runs.
2. **Rotate the windows-verify prefix now** (the ~2x working set is the
   documented rotation trigger). Expected: real-build pulls ~600s -> ~150-300s.
3. **`TEMP`/`TMP` onto `D:`** (target and CARGO_HOME are already there; the
   image already disables Defender). Small, low-risk.
4. **Note**: `windows-2025` now resolves to the VS 2026 image; MSVC bumps will
   cause periodic legitimate kache misses. WSL2/Nix on Windows stays rejected
   (VM start + 9p I/O for no gain).

### 2.6 Workflow graph

- **A `changes` job on ubuntu-latest** (3s queue) computes path filters; job
  level `if:` keeps doc-only commits off pond-ci entirely (today they pay the
  ~180s cold start for a 24s replay). Never `paths-ignore` - required checks
  would hang pending. `release-prep` stays unfiltered. The same job flags
  toolchain-touching fork PRs (see 2.8).
- **`$/` self-repo action syntax** (runner >= 2.336; pond-runner is 2.337)
  removes the checkout-before-local-action coupling where useful.
- flake-check stays on ubuntu-latest (its ~30s is fine, and it is the
  unsandboxed-pod counterexample the comment already documents).
- Label fact for future jobs: `macos-15` is arm64 - which matches the
  aarch64-apple-darwin artifact macos-verify runs today; an Intel verify job
  would need `macos-15-intel`/`macos-15-large`.

### 2.7 Binary cache (bucket and keys are managed privately)

An S3-compatible bucket, public read, signed paths. pond consumes it as an
extra `https://` substituter + `trusted-public-keys` - no secrets needed on
hosted runners, fork runs, or dev machines, which all get the same toolchain
closure from it.

### 2.8 Fork PRs on pond-ci (policy)

Fork PRs stay on pond-ci:

1. Repo setting **"Require approval for all outside collaborators"**: every
   fork run needs an explicit approve-and-run click, every push.
2. The binary-cache signing/push credential exists only on main-branch jobs;
   new toolchain closures are published by the first main run after a
   toolchain bump.
3. A fork PR touching `flake.nix`/`flake.lock`/`rust-toolchain.toml` is
   flagged by the `changes` job for closer review.

The rationale and trust analysis live with the ops-side plan.

### 2.9 Local dev and agents

- `.envrc`: `watch_file rust-toolchain.toml` + `use flake` (nix-direnv caches
  the environment, so entry is instant and survives edits; without the
  watch_file line a toolchain bump goes unnoticed).
- Agents use `direnv exec . <cmd>` (or one `eval "$(direnv export bash)"` per
  shell) - never `nix develop -c` per command, which re-evaluates after every
  file edit. A worktree created outside the trusted prefixes (e.g. a scratch
  dir) needs one `direnv allow` first.
- **Local kache comes back**: `KACHE_PRESERVE_INCREMENTAL=1` (global config,
  not repo-level). Units that cargo passes `-C incremental` (workspace crates)
  skip the store and keep incremental; registry deps stay cached. This removes
  the 54s-vs-8s regression that got kache dropped locally. Local-only store;
  sharing CI's remote is not practical (triple/SDK/libc differences key almost
  everything).
- Not doing: building pond with Nix (crane) - it disables incremental, rebuilds
  the whole dep set on any `Cargo.lock` change, and duplicates moon+kache.

## 3. Verification checklist (before each dependent step)

- [ ] `rustc -vV` parity: rust-overlay's rustc vs rustup's (decides whether
      kache entries are shared across them; the key uses `-vV` text, not path).
- [ ] Static nix binary supports `s3://` (needs the `libstore:s3-aws-auth`
      build flag; test `nix store info --store s3://...`).
- [ ] `KACHE_VERIFY=1` run under the Nix cc wrapper (C-compile key stability).
- [ ] moon versionless `unstable_python`/`unstable_uv` actually provision
      nothing and use PATH.
- [ ] `kache why-miss aws_lc_sys` for OUT_DIR-embedded `.a` contents.
- [ ] moon background-upload inline-wait workaround still needed at 300s.
- [ ] macOS devshell: `mkShell` brings the Darwin clang wrapper and apple-sdk
      env - verify the `cc` crate and aws-lc-sys still build there, else use
      `mkShellNoCC` on Darwin only.

## 4. Rollout order

| Phase | Change | Expected effect |
|---|---|---|
| 1 | Windows pull-skip + windows-verify prefix rotation; runner-pool idle window (ops side) | -650s on ~30% of Windows runs; cold pulls halved; most ~180s Linux cold starts gone |
| 2 | kache 0.22.0 (rides the rust-1.98 PR) | none (hygiene; k31 stays) |
| 3 | moon 2.5.5; `moon ci` for build-and-test; versionless toolchains + `rust: {}` + `/flake.lock` input | hit-path 10-15s shrinks; OS-keyed hashes unblock remote reads for devs |
| 4 | Flake toolchain + `toolVersions` + Windows text-extraction + parity check; `.envrc` | single pin source; local UX/AX wins immediately |
| 5 | Runner image + /nix store volume + devshell-entry step; delete bootstrap; binary cache + fork policy settings (with the ops side) | bootstrap gone; much smaller image on cold nodes; cold-cache fills in parallel |
| 6 | Local kache preserve-incremental; moon `localReadOnly` + shared worktree cache | dep-compile hits locally; agents reuse CI results |

Phases 1-3 are independent of Nix entirely. Phase 5 is the only one touching
the runner image and can roll back by reverting the image tag on the ops side.
