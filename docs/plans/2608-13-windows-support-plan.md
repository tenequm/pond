# Windows support: staged native port (x64 msvc) + installation channels

This document is self-contained: a fresh session should be able to implement from it without re-deriving the investigation. Upstream claims were verified against pinned crate sources (lance 8.0.0, lance-table 8.0.0, object_store 0.13.2, dirs 6.0.0 per `Cargo.lock`) and current upstream docs as of 2026-08-13; pond citations are against `main` at the time of writing - line numbers may drift, symbol names will not. All decisions below were settled with the operator on 2026-08-13; do not re-litigate them, implement them.

## 1. Goal and shape

Ship native Windows support as three phases, each independently releasable through the normal release-plz flow (ordinary `feat`/`fix` commits, patch bumps - nothing here is breaking):

1. **Phase 1 - correct native binary.** Fix the runtime defects in the Windows build pond already ships, switch the target to msvc, and put a real Windows CI gate in place. Exit: a Windows user can `pond init`, sync, and search against a local store.
2. **Phase 2 - installation.** winget + Scoop + completions + docs. Exit: `winget install tenequm.pond` works and stays current automatically.
3. **Phase 3 - parity.** Code signing, then `pond schedule` on Windows, then the Windows durability wrapper. Exit: no spec rule is silently unenforced on Windows.

### 1.1 Settled decisions (from the 2026-08-13 grilling)

| # | Decision |
|---|---|
| 1 | Targets: `x86_64-pc-windows-msvc` only. gnu artifact dropped immediately (it never worked at runtime - nobody can depend on it). No arm64. |
| 2 | Windows CI: full `cargo test` on release PRs + `main` pushes + manual dispatch. Not PR-blocking on ordinary PRs. Must include the OCC and rowmap probes (3.4). |
| 3 | Directories: standard `dirs` platform mapping (config -> `%APPDATA%\pond`, data/state/cache -> `%LOCALAPPDATA%\pond\...`), XDG env override still wins when set, home via `std::env::home_dir()`. No new crate: `dirs 6.0.0` is already in the graph (lance-index, hf-hub) - promote to direct. |
| 4 | Local `file://` stores: enabled from phase 1, contingent on the CI probes passing. Interim durability posture = `local-store-self-heal` (already spec'd in section 3.3). |
| 5 | Rowmap: probe-first. Deletion tolerance already exists (`let _ =` at all four sites - the delta is logging + retry-later sweep); the probe targets the purge-then-rebuild cycle, and the fresh-suffix-on-purge-failure rule fires only if it fails (section 6). |
| 6 | `pond init` agent integration: keep shelling out to `claude`; PATHEXT-aware resolver + `cmd /c` for `.cmd`/`.bat` shims. Never write Claude's config JSON directly. |
| 7 | Adapters: portable set + claude-code with a Windows-aware decoder, gated on a captured native-Windows fixture. `claude-desktop-app`/`opencode` get Windows probe paths in the same pass. |
| 8 | Distribution identities: artifact `pond-x86_64-pc-windows-msvc.zip`, winget ID `tenequm.pond`, Scoop bucket `tenequm/scoop-bucket`. |
| 9 | Scheduler: second binary `pondw.exe` (`windows_subsystem = "windows"`); `schtasks /XML` registration; ships only after code signing. |
| 10 | Durability: Windows flush-on-publish wrapper at the existing `WrappingObjectStore` seam (phase 3). Config permissions: no ACL code - `%APPDATA%` inherited ACLs are the 0600 analog; comment states this. |
| 11 | Spec: phase 1 amends section 2.4 to a tiered statement; `windows-store-durability` rule lands with the phase-3 wrapper. Each phase = normal releases. |

## 2. Current state (verified)

- The release pipeline already cross-compiles `x86_64-pc-windows-gnu` via `cargo zigbuild` (`moon.yml` `build-dist`, deliberately its own invocation because cargo feature-unification would leak `sha2-asm`, a Windows `compile_error!`, from the linux targets) and uploads `pond-x86_64-pc-windows-gnu.zip` to every release. No CI job has ever executed it.
- It is broken at runtime: all path resolution reads `HOME` (`config.rs:481-522`, `syncstate.rs:19-33`, `adapter/mod.rs:382-390`), unset on native Windows, so adapter discovery finds nothing, the store falls to cwd-relative `.pond`, config to `.pond.toml`, and the state dir (sync lock, last-sync record) moves with `cd`. Tests inject `HOME`/`XDG_*` so CI would never catch it.
- It is broken before that, too: the whole CLI future runs on the main thread, and the main-thread stack reserve is a linker default - 1 MiB under MSVC and lld, 2 MiB under binutils ld (unix mains get 8 MiB) - so constructing and polling the giant async state machine overflows the msvc build before argument parsing; even `--version` crashes (found by PR #147; whether the gnu artifact's 2 MiB reserve survives is unverified). Fix in 3.4.
- One hard build blocker for msvc: `lance = { features = ["protoc"] }` pulls `protobuf-src`, [broken on Windows since 2022](https://github.com/MaterializeInc/rust-protobuf-native/issues/4) (CRT-mismatch link errors). Lance's own Windows CI does not enable the feature and installs a system `protoc`.
- Lance commits differently on Windows: local `file://` stores route to `RenameCommitHandler` (`lance-table-8.0.0/src/io/commit.rs:1075`) = `std::fs::hard_link` + delete, NTFS-only per [Microsoft](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-createhardlinka); `shared-memory://` and all remote schemes keep `ConditionalPutCommitHandler`, so pond's multi-writer OCC suite never exercises the Windows local path. Known unfixed: `e_tag: None` manifest cache-key mismatch (cache miss, not corruption). fp16 kernels are hard-disabled on Windows (`lance-linalg` build.rs) - pond does not enable `fp16kernels`, so no parity regression.
- The dependency graph resolves cleanly for the msvc target. Upstream Windows CI exists for lance (full suite), candle (check + test), tokenizers (+`onig`; `onig_sys` has an explicit MSVC branch, and fails to compile under real MinGW gcc - uid_t/gid_t config.h clash, [tokenizers#1581](https://github.com/huggingface/tokenizers/issues/1581), MSVC explicitly unaffected - independent corroboration for decision 1's gnu drop), tokio; rusqlite-bundled/zstd/blake3/ring all have explicit MSVC build paths (ring ships pregenerated asm - no Perl/NASM needed from crates.io). `aws-lc-sys` (via jsonwebtoken <- reqsign-azure <- opendal <- lance-io) needs NASM. No Windows CI at all: DataFusion, axum, rmcp - pond's CI is the first place they run on Windows; both are pure-Rust over tokio, low intrinsic risk.
- The terminal stack (console/indicatif/cliclack/anstyle/ctrlc) works on Windows 10+ VT consoles; `cliclack 0.5.4` has no CI anywhere and its Ctrl-C interplay with pond's no-op ctrlc handler (`init.rs:122-130`) needs manual verification on Windows in phase 1.

### 2.1 PR #147 integration (post-hoc comparison, 2026-08-13)

This plan was designed without consulting [PR #147](https://github.com/tenequm/pond/pull/147) (deliberately, to keep the design unbiased); the full comparison afterwards cross-validated both. The PR independently re-derives most of the phase-1 runtime core with matching conclusions (protoc dual-table gate, per-OS home resolution, native Windows dirs, `cmd /C` + `raw_arg` secrets, sync-lock holder sidecar, spec/README amendments, cross-platform test fixes) and contributed four findings now folded into this doc: the main-thread stack overflow (3.4), the `raw_arg` spawn shape (3.4), the UTF-16LE task-XML requirement plus `%`-expansion gate (5.2), and UTF-16 console decoding (5.2). Strategy: **merge the PR, then implement the remaining delta ourselves.** Phase-1 items the PR already delivers (3.1 home/dirs, the 3.4 secrets and lock items, part of the test portability budget) become review-verify items, not build items.

- **Merge-time asks (one narrow review round - durable on-disk state, cheap now, costly after users accumulate data):** (1) directory layout per 3.1: the PR puts the store at `%LOCALAPPDATA%\pond` with the cache *inside* it at `\pond\cache`, and leaves state at `%USERPROFILE%\.local\state`; ask for the `%LOCALAPPDATA%\pond\{data,state,cache}` split. (2) XDG consistency: the PR hard-ignores `XDG_DATA_HOME`/`XDG_CONFIG_HOME`/`XDG_CACHE_HOME` on Windows while `state_root()` still honors `XDG_STATE_HOME`; decision 3 keeps an absolute XDG override winning everywhere.
- **Accepted as-is, reworked later (invisible internals, no user-facing state):** the `.cmd`+`.vbs` scheduler mechanism (swap to `pondw.exe` in phase 3 - VBScript is deprecated and on Microsoft's removal path; the task XML, settings, and probe logic carry over); the `cfg(not(target_env = "msvc"))` protoc gate (becomes `cfg(not(windows))` when the gnu artifact drops in PR A); the env-var-only `home_dir()` (delegate to `std::env::home_dir()` per 3.1); wrapper-env state pinning (see 5.2).
- **Decision 9 sequencing, overtaken by events:** the Task Scheduler backend lands with the PR, unsigned. Accepted - Defender deleting unsigned task-registering binaries is a field risk to monitor, not a merge blocker; signing and the `pondw.exe` swap remain phase 3.
- **Re-scoped delta (what this plan still owns, in order):**
  1. **PR A - the gate, before any Windows announcement:** all of 3.2 + 3.3 (windows-verify CI, native msvc artifact with completions, drop gnu, `+crt-static`, manifest, checksums regeneration, binstall override) plus both 3.3 probes. Until PR A is green, the merged PR is a green-on-the-author's-machine claim riding the release pipeline.
  2. **PR B - remaining 3.4:** PATHEXT resolver + resolved-path claude spawn, cliclack bump, reserved-name validation, commit-retry matcher, self-heal quarantine behavior, rowmap sweep polish, dir layout if the merge-time ask doesn't land.
  3. **PR C - 3.5 adapters:** claude-code decoder + Windows fixture gate, claude-desktop MSIX glob.
  4. Phases 2 and 3 as written.

## 3. Phase 1 - correct native binary

### 3.1 Directory and home resolution

Replace the `HOME`-only resolution with a platform ladder, keeping unix behavior byte-identical:

- **Home:** one helper (in `config.rs`, used by `Env::from_env`, `expand_home_under`, `contract_home`) that returns `std::env::home_dir()`. Verified current std behavior on Windows: `USERPROFILE` if set and non-empty, else `GetUserProfileDirectory` - HOME is not consulted (dropped in Rust 1.85). This matches lance's own `~` expansion for storage URLs (`lance-io/src/object_store.rs:414` uses the same function), so pond's home and lance's URL expansion can never diverge. On unix `std::env::home_dir()` reads `HOME` - identical to today.
- **Dirs:** keep the existing ladder shape - XDG env var wins if set (absolute) - and change only the fallback leg per platform. Windows fallbacks via `dirs` (6.0.0, promoted from transitive to direct dependency; resolves through `SHGetKnownFolderPath`, which works even under a stripped environment - exactly the scheduled-task case the `XDG_STATE_HOME` pinning rule exists for):

| Function | Unix fallback (unchanged) | Windows fallback |
|---|---|---|
| `default_storage_path` (`config.rs:481`) | `~/.local/share/pond` | `dirs::data_local_dir()\pond\data` = `%LOCALAPPDATA%\pond\data` |
| `default_cache_path` (`config.rs:498`) | `~/.cache/pond` | `dirs::cache_dir()\pond\cache` = `%LOCALAPPDATA%\pond\cache` |
| `default_config_path` (`config.rs:512`) | `~/.config/pond/config.toml` | `dirs::config_dir()\pond\config.toml` = `%APPDATA%\pond\config.toml` |
| `syncstate::state_root` (`syncstate.rs:19`) | `~/.local/state` + `/pond` | `%LOCALAPPDATA%\pond\state` (`dirs::state_dir()` is `None` on Windows by design) |

  The `pond` subdirectory split under `%LOCALAPPDATA%\pond\{data,state,cache}` keeps one root for the user while preserving pond's three-role separation. Do not use Roaming for data or state - a roaming profile syncs `%APPDATA%` at logon and a Lance store must never ride that.
- **Cascading fixes that fall out:** `contract_home`/`contract_home_under` (`config.rs:826-846`) contract against the same home helper; the `local`/`default` storage keyword (`main.rs:134-139`, `init.rs:436-445`) resolves to the new data dir; bench dir resolution copies are bench-only, leave them. **Enumerate every `var_os("HOME")` call site with `rg -n 'var_os\("HOME"\)' packages/pond/src` and route each through the helper** - the red-team found four the original list missed: `init.rs:915` (gates the entire skill install - on Windows it silently prints "HOME not set", contradicting the phase-1 exit criteria; `SKILL_DISPLAY_PATH` at `init.rs:911` is also a hardcoded `~/.claude/...` literal), `main.rs:146-147` (`default_cache_dir`), `main.rs:2088-2089` (config-path resolution), and `adapter/mod.rs:609` (`expand_home`, the wrapper adapters actually call).
- **User-facing string fix:** `embed.rs:60-62` hardcodes "cached under `~/.cache/huggingface`". hf-hub 0.5.0 on Windows resolves `dirs::home_dir()` + `.cache/huggingface/hub` = `C:\Users\<user>\.cache\huggingface\hub` (a profile dotdir, not LOCALAPPDATA - matches Python huggingface_hub). Print the platform-resolved path instead of a literal. Do not upgrade hf-hub in this phase (1.0 is a full API rewrite; out of scope).
- **Tests:** unix tests keep injecting `HOME`/`XDG_*` unchanged. Add Windows-only unit tests injecting `USERPROFILE` and asserting the fallback table above; integration harnesses that sandbox `HOME` must also set `USERPROFILE` under `cfg(windows)`.
- **Budgeted work item - unix path literals in the test suites.** The red-team counted ~105 unix-absolute-path string literals in `packages/pond/src` unit tests and 19 in `packages/pond/tests` that fail on Windows independent of any Windows code change (e.g. `config.rs:1003-1021` - `PathBuf::from("/xdg").is_absolute()` is false on Windows so the XDG leg silently skips; `config.rs:911-916` - `file:///tmp/...` has no drive letter so `to_file_path()` errs). Concentrations: `adapter/claude_code.rs` (~33), `config.rs` (~23), `adapter/discovery.rs` (8), `main.rs` (7). This is the single largest phase-1 line item: audit and parameterize (platform-conditional expected values, or drive-lettered fixtures under `cfg(windows)`), file-by-file, before the first green Windows run is achievable. Budget it explicitly; do not fold it into "add a CI job". Calibration from PR #147: it reports the full suite green on a real Windows 11 machine after touching only ~a dozen test sites, which does not reconcile with the ~124-literal count - treat the count as an upper bound (many literals evidently sit in cfg-gated or non-asserting positions) and let the first `windows-verify` run settle the real number.

### 3.2 Toolchain, build, and artifact

- **protoc feature, target-conditional - in BOTH dependency tables.** Cargo features cannot be per-target, but target-specific dependency tables merge: declare `lance` in `[dependencies]` *without* `protoc`, and re-declare it under `[target.'cfg(not(windows))'.dependencies]` with `features = ["protoc"]` (empirically verified: `cargo tree --target x86_64-pc-windows-msvc` drops the feature; no duplicate-dependency error). **Blocker caught by the red-team: `lance = { features = ["protoc"] }` appears a second time in `[dev-dependencies]` (`Cargo.toml:197`), and a dev-dep re-enables the feature for the target build - so gating only `[dependencies]` makes `cargo build` pass and `cargo test` fail with the CRT-mismatch link error.** Gate both: strip the feature at `:197` and add `[target.'cfg(not(windows))'.dev-dependencies] lance = { version = "8.0.0", features = ["protoc"] }` (precedent for the table already exists at `Cargo.toml:216`). Windows CI installs `protoc` by direct download of `protoc-<ver>-win64.zip` + `$GITHUB_PATH` (what Lance's `windows-build` job does; dependency-free - preferred over `arduino/setup-protoc@v3`), version kept in lockstep with `PROTOC_VERSION` in `.github/actions/bootstrap`. Document `protoc` as a Windows build prerequisite for `cargo install pond-db` in the README.
- **NASM** for `aws-lc-sys`: install it in CI (`choco install nasm -y` + PATH, or `ilammy/setup-nasm@<sha>`). Verified: an installed NASM always wins over the `prebuilt-nasm` feature, so installing is the deterministic, upstream-CI-matching choice; do not set the feature.
- **`+crt-static`** via `.cargo/config.toml`, lancedb-style with the WHY comment (not all Windows systems have vcruntime140.dll):

  ```toml
  # Not all Windows systems ship the VC runtime; static CRT avoids "DLL not found" on clean machines.
  [target.x86_64-pc-windows-msvc]
  rustflags = ["-Ctarget-feature=+crt-static"]
  ```

  Two verified traps: (a) the Windows CI/release build MUST pass `--target x86_64-pc-windows-msvc` explicitly, or the rustflags leak into build scripts and proc-macros (Cargo docs: host artifacts receive target rustflags when `--target` is absent); (b) any `RUSTFLAGS` env set in a CI step silently replaces the config-file rustflags entirely - never set `RUSTFLAGS` in the Windows job. Watch-item, likely a non-issue: `aws-lc-sys` carries a cmake build-dependency, and cmake re-asserts the dynamic CRT after your flags ([CMP0091](https://cmake.org/cmake/help/latest/policy/CMP0091.html)); its docs say CMake is FIPS-only (pond is non-FIPS, `cc`-only path), but if the Windows link ever fails with `LNK2038` CRT-mismatch, the fix is `CMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded` - the same failure signature `protobuf-src` has.
- **Windows manifest** via `embed-manifest 1.5.0` in `build.rs`, gated on `CARGO_CFG_WINDOWS`. Verified: `new_manifest()` already defaults `longPathAware` to true (plus UTF-8 code page and asInvoker) - no builder calls needed:

  ```rust
  fn main() {
      if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
          embed_manifest::embed_manifest(embed_manifest::new_manifest("Pond.Pond"))
              .expect("embed windows manifest");
      }
      println!("cargo:rerun-if-changed=build.rs");
  }
  ```

  Honesty note (red-team corrected the original framing): the manifest's `longPathAware` is only half of a two-part gate - it takes effect only on machines where the `LongPathsEnabled` registry key is also set, and Rust std was never MAX_PATH-bound anyway (it converts to `\\?\` verbatim paths internally since 1.58). The manifest's *real* wins for pond are the UTF-8 active code page, per-monitor DPI awareness, and `asInvoker` (no UAC prompt heuristics); long-path awareness is cheap insurance for the C deps and spawned children on machines that did enable the key. The 4.4 docs item states it that way. Avoid `canonicalize()` on paths that get printed or passed to children (`\\?\` prefix breaks other software).
- **Artifact and packaging.** The Windows zip is built by the Windows CI job natively (not zigbuild) and handed to `publish-release` as a workflow artifact: `pond-x86_64-pc-windows-msvc.zip` containing `pond.exe` and `completions/` (all four shells including PowerShell `_pond.ps1`, generated by running the just-built native binary - the linux completions step at `moon.yml:112-117` cannot produce them for this artifact; today's zip ships none). Exact `moon.yml` edits (verified against the tree): drop `x86_64-pc-windows-gnu` from the `rustup target add` line (`moon.yml:56-57`); delete only the windows zigbuild invocation (`moon.yml:78`) - **the split does NOT collapse to one call**: the comment at `moon.yml:68-77` gives two reasons and the candle-`metal` half still forces the darwin invocation to stay separate from the linux pair (`Cargo.toml:174-177`), so the task goes from three invocations to two and only the xet-data/sha2-asm sentence goes; delete the `pond.exe` copy (`moon.yml:104`) and the `install`/`zip -j` lines (`moon.yml:127-129`); delete the zip `outputs:` entry (`moon.yml:151`); fix the "four-target" comments (`moon.yml:43-47`, `moon.yml:134`, and `ci.yml:118`, `:126`, `:162`). Also update `README.md:63` and `docs/site/src/pages/get-started/install.mdx:7` ("Windows is not in v1 scope"). `ops/scripts/check-package-contents.sh` needs no change (verified: zero dist/artifact awareness). Rename the binstall override (currently at `Cargo.toml:40`):

  ```toml
  [package.metadata.binstall.overrides.x86_64-pc-windows-msvc]
  pkg-fmt = "zip"
  ```

- **Windows-job hygiene:** `CARGO_TARGET_DIR: C:\t` (cargo is long-path aware but the current-dir limit surfaces as `LNK1104`/`os error 206`); `Swatinem/rust-cache` pinned >= v2.9.2 (v2.9.2 fixes a Windows-only path-validation regression) with `save-if: ${{ github.ref == 'refs/heads/main' }}`, or switch the leg to sccache if save times (>5 min known issue) dominate. Treat the warm cache as load-bearing, not incidental: Lance's full-suite Windows job runs 20-21 min warm on a paid 16-vCPU `windows-latest-4x` runner, which projects to roughly 50-75 min on the free 4-vCPU `windows-2025` - a cold run plus the second `--profile dist` build can plausibly approach the 120-min ceiling. Confirm on the first runs that rust-cache picks up the `CARGO_TARGET_DIR: C:\t` override, and that the repo's 10 GB Actions-cache LRU budget is not thrashing (a lance-class Windows `target/` can evict the Linux jobs' entries and vice versa). If the cache proves unreliable, sccache is the lever.

### 3.3 CI job

New `windows-verify` job in `ci.yml`, runner `windows-2025` (note: `windows-latest` = `windows-2025` = `windows-2025-vs2026` - Windows Server 2025 with **VS 2026** as of the June 2026 image migration; pin `windows-2022` only if VS 2022 turns out to be needed). The image ships Rust 1.97.1/rustup, CMake, 7-Zip - and neither protoc nor NASM (verified against the image software list).

- **Plain `cargo`, not moon, on this runner - and the reason is load-bearing.** Verified in moon's task-hasher source: the task fingerprint carries `command/args/deps/env/inputs/outputs/target/...` but **no OS or architecture**. If this job ran `moon run pond:test` with the shared remote cache reachable, the hash computed on Windows would equal the one the Linux `pond-ci` runner already stored for the same commit, moon would report `cached`, and the gate would go green without compiling or testing anything on Windows - the exact failure the job exists to prevent. Supporting reasons: the bootstrap action is Linux/PVC-only, `.moon/toolchains.yml` provisions nothing this job needs, and the job is a linear command sequence with no graph to gain from. Put a drift-guard comment in both `moon.yml` and the job mapping the mirrored commands: `pond:test` (`packages/pond/moon.yml:34`, `cargo test --locked`) -> `cargo test --locked --target x86_64-pc-windows-msvc`; the dist leg -> `cargo build --locked --profile dist --target x86_64-pc-windows-msvc`. Skip fmt/clippy here (OS-independent, already gated on `pond-ci`). If moon is ever wanted on Windows, the correct shape is new os-scoped tasks (`options.os: 'windows'`, since moon v1.28 - distinct `target` means distinct hash), never reuse of `pond:test`; and the cache lever is `MOON_REMOTE_HOST: ""` (empty host cleanly disables the remote per `RemoteConfig::is_enabled`). Related memory correction: moon's headless/non-TTY remote-cache disable is real but fires only for **localhost** cache hosts - pond's `grpcs://bazel-remote.cascade.fyi:443` is never affected.
- **Triggers:** add `workflow_dispatch` to the workflow `on:` block (side effect to accept: `build-and-test`/`flake-check` carry no event condition and will also run on dispatch; `release-prep` and downstream stay push-gated). Job condition (release-plz's release-PR branch prefix is the default `release-plz-`; pond's `.github/release-plz.toml` does not override it - verified):

  ```yaml
  if: >-
    github.event_name == 'workflow_dispatch' ||
    (github.event_name == 'push' && github.ref == 'refs/heads/main') ||
    (github.event_name == 'pull_request' && startsWith(github.head_ref, 'release-plz-'))
  ```

  To make the release gate binding, mark `windows-verify` as a required status check in branch protection: on ordinary PRs the `if:` skips it, and GitHub treats a skipped required check as satisfied, so only release PRs actually wait on it. `windows-verify` takes **no `needs:`** - it starts at t=0 in parallel with the Linux jobs rather than serializing the release path behind them. `timeout-minutes: 120` (the test compile and the `dist` compile are two independent dep-graph builds on a slow hosted runner), a `concurrency` group with `cancel-in-progress`, and `env: BAZEL_REMOTE_AUTH: ""` (same rule as `flake-check`/`macos-verify`: hosted runners compiling third-party build scripts get no cache credentials).
- **Steps** (full copy-pasteable fragment in Appendix A; the load-bearing details): checkout (depth 1 - no moon here, `POND_BUILD_COMMIT` needs only HEAD); protoc direct-download + `$GITHUB_PATH`; `choco install nasm -y`; `rustup toolchain install --no-self-update` (honors `rust-toolchain.toml` 1.95.0; the host triple IS the build target so no extra rust-std); `Swatinem/rust-cache` pinned >= v2.9.2 with `save-if: main`; `cargo test --locked --target x86_64-pc-windows-msvc` (**full** suite - DataFusion/axum/rmcp get their first Windows run ever here); then `POND_BUILD_COMMIT=$(git rev-parse --short HEAD)` and **`cargo build --locked --profile dist --target x86_64-pc-windows-msvc`** - `--profile dist`, NOT `--release`: `release` is pond's no-LTO iteration profile (`Cargo.toml:23-29`), every shipped binary uses `dist`, and without `POND_BUILD_COMMIT` the Windows binary loses the commit stamp every other artifact carries (`moon.yml:51` -> `main.rs:223`); launch-smoke mirroring `macos-verify` (`--version`, `--help`, `completions powershell` - redirect to a file, never pipe: a closed pipe trips clap_complete); package the zip (exe + all four completion scripts) with `Compress-Archive`; upload as `dist-windows` artifact (`if-no-files-found: error`, `retention-days: 1`).
- **`publish-release` wiring:** `needs: [dist-build, macos-verify, windows-verify]`; a second `download-artifact` for `dist-windows` into `dist/`; then - **before** the release-plz step - regenerate checksums: `checksums.txt` is produced *inside the cached moon task* (`moon.yml:131`, declared output `:152`) and structurally cannot contain the Windows hash, so the job asserts the zip exists and re-runs `( cd dist && sha256sum pond-* > checksums.txt )` over the merged dist. The `gh release upload "v$V" dist/pond-* dist/checksums.txt` glob (`ci.yml:282`) then picks the zip up unchanged; the nix/Homebrew `sha()` helpers read only `pond-<target>.tar.xz` and are unaffected.
- **The two de-risking probes**, placed per the repo's test rules:
  1. **OCC on the rename commit handler: the test already exists.** `tests/integration/store_concurrency.rs:32-34` opens two `Store::open_local` against one `TempDir` - on Windows that genuinely routes to `RenameCommitHandler` (verified in the pinned lance-table source, including the `Err(_) -> local_handler` fallback). The work item is asserting it passes on Windows and extending it to pin down the error shapes for the retry-matcher item in 3.4 - not writing a new test. Add a comment carving it out from the CLAUDE.md test-backend rule ("`shared-memory://` for 2+ Stores"): a real `TempDir` here is deliberate, it is the only thing that exercises the Windows commit path, and a reviewer must not "fix" it back.
  2. **Rowmap purge-then-rebuild probe** (cross-process, so it lands in `tests/integration/`, spawning via `CARGO_BIN_EXE_pond` or a helper subprocess): process A maps a segment; process B runs the purge (delete fails on Windows - expected) followed by a rebuild of the *same version*. This is the actual hazard (section 6): segments are already generation-named, so a fresh publish never renames over a live file - only the purge-failure-then-same-version-rebuild cycle does. Assert the cycle either succeeds or degrades per the section 6 contingency, and assert stale-segment sweep behavior.
- **Watch item riding the probes: `url` > 2.5.4 Windows path regression.** pond's lock has `url 2.5.8`, and [object_store#499](https://github.com/apache/arrow-rs-object-store/issues/499) (filed by westonpace of LanceDB) shows `LocalFileSystem` failing on Windows paths with url >= 2.5.5 - a `path_segments_mut().extend` behavior change, [rust-url#1077](https://github.com/servo/rust-url/issues/1077) (open; fix PR rust-url#1108 open). pond's own constructions use `Url::from_file_path` (the safe direction), but the Lance stack rebuilds paths from `file://` URLs internally. Lance merged a url-upgrade rework ([lance#4860](https://github.com/lance-format/lance/pull/4860)) - verify at implementation time whether lance 8.0.0 contains it. If the Windows suite hits confusing local-path failures, pinning `url = "=2.5.4"` in the lock is the first diagnostic lever.

### 3.4 Platform correctness fixes

- **Main-thread stack (startup blocker; adopt PR #147's fix):** run the tokio runtime and the CLI future on a dedicated named thread with a 16 MiB stack (`std::thread::Builder::stack_size`, `block_on` on that thread, propagate the join result as the exit code). `#[tokio::main]` polls the giant CLI state machine on the 1 MiB Windows main thread and overflows before any code runs - and `Box::pin` does not help, since the future still materializes as a stack temporary first. Behaviorally identical on unix (which already had ample stack). Verified specifics: 1 MiB is the MSVC/lld linker default reserve (MS Learn "Thread Stack Size"), rustc does not override it ([rust#85303](https://github.com/rust-lang/rust/issues/85303), still open) while spawned `std::thread`s default to 2 MiB; `Builder::stack_size` sets a true reserve on Windows (`STACK_SIZE_PARAM_IS_A_RESERVATION`); `RUST_MIN_STACK` does NOT affect the main thread, so it is no workaround. Cheaper alternative if the dedicated thread is ever unwanted: `cargo:rustc-link-arg=/STACK:8388608` from build.rs (binutils form `-Wl,--stack,...`).
- **`find_on_path` PATHEXT** (`main.rs:179-185`; the earlier `1076-1097` reference was wrong - that range is `try_raise_fd_limit`): on Windows, for each PATH entry try the name plus each `PATHEXT` extension (default set `.COM;.EXE;.BAT;.CMD` if unset); `is_executable` (`main.rs:187-197`) on Windows = extension is in PATHEXT. Serves `claude`/`codex` detection and `pond_bin()` in the scheduler.
- **Shim-aware spawn - and thread the RESOLVED path into the spawns** (`init.rs:851, 874`): both sites currently call `Command::new("claude")` with a bare name, and Windows `CreateProcess` appends only `.exe`, never PATHEXT - so fixing `find_on_path` alone leaves detection saying "claude found" while both spawns still fail for the npm `claude.cmd` shim. The resolved `PathBuf` is already bound at `init.rs:842`; pass it into both `Command` calls. Then: `.exe` spawns directly, `.cmd`/`.bat` spawns via `cmd /c "<absolute path>" <args...>` (built with `raw_arg`, not `.arg()` - the same MSVCRT-vs-cmd quoting trap as the `_command` bullet). (The native Claude installer ships `claude.exe`; the npm route ships `claude.cmd` - both exist in the field.)
- **`<field>_command` credential sources are `sh -c`** (`substrate.rs:611-615`): no `sh` on a stock Windows box, so every command-backed secret fails to spawn - load-bearing under the section 6 remote-only contingency, where remote creds are the only path. Fix: `%COMSPEC%` (fallback `cmd`) with `CommandExt::raw_arg` passing `/C <command>` verbatim - never `.arg()`, whose MSVCRT quoting mangles any command containing quotes (cmd.exe has no backslash escapes; std's `raw_arg` docs name `cmd.exe /c` as the exact use case). PR #147 lands this shape plus a space-and-quote regression test; the spec's `storage-redaction` `_command` contract keeps working on Windows.
- **cliclack 0.5.4 -> 0.5.6 bump** (one line: `cargo update -p cliclack`; `Cargo.toml:83` already allows `0.5`): verified that at 0.5.4, Ctrl-C inside a prompt never cancels on Windows (`read_key()` leaves `ENABLE_PROCESSED_INPUT` on, so Ctrl-C routes to the handler thread and the prompt keeps waiting); cliclack 0.5.5 (PR #112, "fix Ctrl-C to be handled natively without the previous workaround") switches to `read_key_raw()` + native `Key::CtrlC` handling on console 0.16 (which pond already has). Keep pond's no-op `ctrlc` handler regardless - it still carries the non-prompt exit-130 and pending-schedule-registration path. This bump must land before any Windows wizard testing.
- **Commit-retry matcher** (`substrate.rs:1447`): on Windows, additionally match sharing-violation-shaped errors surfacing from the local commit path (Lance wraps `ERROR_SHARING_VIOLATION`/access-denied from the hard-link/delete dance) as retryable, scoped to the commit/write path only - never a blanket access-denied retry. Implementation: extend the conflict classification with a `cfg(windows)` arm that string/kind-matches the object_store error variants observed in the OCC probe; the probe is where the exact shapes get pinned down.
- **Cache-retirement semantics on Windows** (corrected by the red-team + unknowns closure - the original "add deletion tolerance" item was already implemented): all four sites (`prune_rowmaps` `sessions.rs:2139`, `purge_rowmaps` `sessions.rs:2158`, `sweep_orphan_temps` `sessions.rs:2175`, index-cache dir removal `substrate.rs:4064`) already swallow delete failures via `let _ =`. The real Windows facts, verified: deleting a file another process holds mapped is **blocked** (`STATUS_CANNOT_DELETE` -> os error 5 / `PermissionDenied`; `FILE_SHARE_DELETE` explicitly does not help, and memmap2 duplicates the handle until `Drop`, so pond's readers always count as holders), and the code documents the opposite POSIX assumption in two places (`sessions.rs:2117`, `:2147`) - fix those comments. Consequence is leaked stale `.rmm` files (~270 MB per stale base on the real corpus), not a crash. Work items: add debug logging on swallowed failures, make the sweep re-attempt on later runs (retry-later, not redesign), and handle the purge-then-rebuild cycle per section 6. The rename side is safe by construction: std 1.95's `rename` is `MoveFileExW` primary (the 1.85.0 POSIX-first change was reverted in 1.85.1), and pond never renames over an existing path - segments are version-named and the build lock early-returns when the version already exists.
- **Sync-lock holder record vs mandatory locks** (`syncstate.rs:71-102`): `File::try_lock` maps to `LockFileEx`, and Windows byte-range locks are *mandatory* - the `WouldBlock` arm's `read_to_string` of the holder JSON inside the locked file fails, silently degrading "waiting on pid N since T" to nothing. Fix uniformly (no cfg divergence): write the holder JSON to a sidecar `<lockname>.holder` file after acquiring, and read the sidecar in the busy arm; the lock file itself carries no payload. Same treatment is unnecessary for the rowmap build lock (`sessions.rs:2025-2032` - it has no holder message), but verify its busy path does not read the locked file either.
- **Self-heal on Windows**: heal's quarantine step renames possibly-open manifests to `*.corrupt` (`substrate.rs:4288-4420`), which Windows refuses while another handle holds the file. Heal is the interim durability backstop (section 3.6), so its Windows behavior is load-bearing: the existing heal test suite runs in the Windows CI job as part of the full test run - explicitly confirm it passes, and make the quarantine rename surface a clear error naming the holder scenario (retry-on-next-open is the natural recovery, since opens are short-lived) rather than an opaque os error 5.
- **Reserved-name validation** (`adapter/mod.rs:652-671` `validate_path_id`): additionally reject, on all platforms (archives are portable), segments that are Windows device names case-insensitively even with extensions (`CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`, `LPT1`-`LPT9`, so `NUL.jsonl` is rejected), segments with trailing dots or spaces, and `:` anywhere in a segment (silently becomes an NTFS alternate data stream).
- **Config write** (`config.rs:875-896`): keep the `not(unix)` plain-write branch; replace the "Windows out of scope" comment with the settled rationale: files under `%APPDATA%` inherit ACLs granting only the user, SYSTEM, and Administrators - the platform analog of 0600; no ACL code, matching the unix posture of best-effort-within-the-home-dir.
- **Path/URL seams:** audit `child_uri` (`config.rs:133-145` - local branch emits backslashes by design; verify `StorageUrl::parse` and lance `uri_to_url` accept them), the object-path test helper (`substrate.rs:3993` - fix for backslash/drive-letter), and the two symlink-using tests (`adapter/pi_coding_agent.rs:2649`, `tests/integration/resume.rs:502`) get `cfg(unix)` gates or junction-based Windows variants. Add explicit unit tests for `StorageUrl::parse` with Windows inputs (`C:\foo`, `C:/foo`, `file:///C:/foo`, a UNC path - lance's `uri_to_url` special-cases single-letter schemes as drive letters and has its own Windows path tests, but pond's bare-path branch at `substrate.rs:117` is the unaudited seam). **UNC and mapped-network-drive store locations are rejected with a typed error** naming the limitation and the alternatives (local drive or a remote scheme) - not merely tested: the stack drops the UNC host today (`object_store::Path::from_absolute_path` strips it, [object_store#715](https://github.com/apache/arrow-rs-object-store/issues/715); Lance fails end-to-end on shares, [lance#6616](https://github.com/lance-format/lance/issues/6616), fix PR lance#8378 open), so an ungated UNC store fails confusingly deep inside Lance instead of at the gate. Same shape as the ReFS/Dev Drive contingency (section 6); lift the gate when the upstream fixes land. Also audit `expand_home_under` (`config.rs:810-821`): it routes raw path strings through `shellexpand::full_with_context_no_errors` with `$VAR`/`${VAR}` expansion - Windows config values are backslash paths and Windows env syntax is `%VAR%`; verify shellexpand's backslash/escape handling on Windows inputs and decide (and document) that `%VAR%` deliberately does not expand.
- **Manual verification item (not automatable):** `pond init` wizard end-to-end in Windows Terminal + PowerShell, specifically Ctrl-C during a cliclack prompt (pond's no-op ctrlc handler + `term.read_key()` interplay is mode-dependent on Windows and cliclack has no CI).

### 3.5 Adapters

- `Env::from_env` (`adapter/mod.rs:377-391`) resolves via the new home helper - this alone revives `claude-code`, `codex-cli`, `pi-coding-agent`, `openclaw`, `hermes`, `nanoclaw` (their `~`-relative layouts are identical on Windows).
- **Windows probe paths** (each in its own adapter module per the seam rule), both closed by source verification:
  - `opencode`: **no Windows-specific path needed.** Verified in the opencode source (`packages/core/src/global.ts` via `xdg-basedir@5.1.0`, which has zero Windows special-casing): opencode uses `~/.local/share/opencode` verbatim on Windows too, so pond's existing probe works once home resolution is fixed. (opencode's own troubleshooting docs claim `%APPDATA%` - that is their Electron desktop shell, not the CLI store; the docs contradict the code.) Known pre-existing gap, now the only override hook: pond ignores `XDG_DATA_HOME` for this probe (`adapter/opencode.rs:80`).
  - `claude-desktop-app`: **NOT `%APPDATA%\Claude`.** The Windows build is MSIX-packaged and Electron's `userData` is silently virtualized (verified via anthropics issues #25579/#26073): the real parent is `%LOCALAPPDATA%\Packages\<family>\LocalCache\Roaming\Claude\`. The probe must **glob** `%LOCALAPPDATA%\Packages\*Claude*\LocalCache\Roaming\Claude\local-agent-mode-sessions` (the package-family hash is install-channel-dependent) with `%APPDATA%\Claude\...` as the non-MSIX fallback - and the child dir name needs verification against a real Windows install before shipping (medium confidence; nobody has publicly dumped that listing).
- **claude-code Windows decoder - re-weighted per the red-team.** `Session.project` normally comes from the JSONL `cwd` field; the slug decode (`claude_code.rs:641-649`) is only the no-`cwd` **fallback**, so the permanent-bad-data risk is narrower than originally framed. Two work items, in priority order:
  1. **`encode_project` restore-path sanitization (the binding hazard):** `encode_project` (`claude_code.rs:208-210`) is `project.replace(['/', '.'], "-")` feeding a restore path component (`claude_code.rs:625-635`) - a Windows `cwd` of `C:\Users\foo` passes through with colon and backslashes intact, colliding with 3.4's own reserved-name/colon rule. Same shape at `adapter/pi_coding_agent.rs:284` and `adapter/opencode.rs:1755`. Encode must neutralize `\` and `:` (and the reserved names) wherever a project value becomes a path segment.
  2. **Slug decode for the fallback path**, from the evidence model (issues #33140, #14088, third-party implementations): drive-letter colon -> `-`, each `\` -> `-` (giving `C--Users-foo-bar`), dots/spaces/underscores -> `-`, UNC -> `--host-share-...`, drive-letter case NOT normalized (match case-insensitively), NFC-normalized, and mapped network drives can produce two slugs for one project. One encode/decode function pair handling posix and Windows forms.
  **Fixture gate (still required, now correctly scoped):** capture a real native-Windows `~/.claude/projects` corpus (scripted on the Windows CI runner or a manual VM capture) and commit it beside the existing fixtures with a round-trip test covering both the `cwd`-present mainline and the slug-fallback path. claude-code Windows ingest does not ship until it passes.
- `oh-my-pi`: PR #147 synced 331 oh-my-pi sessions end-to-end on Windows 11, so the `/`-based scope encoding is portable in practice - enable the Windows probe with the others and note the field verification in the module.

### 3.6 Spec amendment

Section 2.4 changes from "Linux and macOS. Windows is not in v1 scope." to a tiered statement: Windows (x64) is supported; `pond schedule` registration and the fsync durability wrapper are not yet implemented on Windows - unattended sync is a documented manual Task Scheduler setup, and local-store durability relies on `local-store-self-heal` (section 3.3 already names this posture). No new rule text ships ahead of the code that makes it true.

### 3.7 Exit criteria

- `windows-verify` green: full test suite + both probes + smoke.
- Manual: `pond init` wizard, first sync against a local store, `pond search`, `pond mcp` registration into a real Claude Code on a Windows machine/VM.
- Release artifacts: msvc zip (with completions) + updated checksums; gnu zip gone; binstall override renamed.
- Spec 2.4 amended.
- README gets a minimal Windows install note including the WSL caveat (the full docs-site Windows page is phase 2, but phase 1 must not release a Windows binary with zero written guidance).
- If the section 6 local-store contingency fires, the local-store exit criteria above are replaced by: init + sync + search against a remote store URL, and the local-store gate error names the constraint - the phase still ships.

## 4. Phase 2 - installation

### 4.1 winget

- **First submission is manual and interactive** (hard requirement - winget-releaser requires at least one version to already exist in winget-pkgs): `komac token add` (the classic PAT), then `komac new` - Komac v2.16 documents `new` with **no arguments**, it is an interactive wizard; there is no documented non-interactive form, so budget a human at a terminal for this one-time step. Manifest shape: `InstallerType: zip`, `NestedInstallerType: portable`, `NestedInstallerFiles: [{RelativeFilePath: pond.exe, PortableCommandAlias: pond}]`. A portable winget install provides PATH, Add/Remove Programs, and `winget uninstall` - no MSI, no signing required (verified: winget validation does not require signed binaries, and winget downgrades MOTW to trusted-zone after hash verification, so no SmartScreen on install).
- **Automation thereafter:** a `winget-publish` workflow on `release: [released]` using `vedantmgoyal9/winget-releaser` (pin to a SHA; it is Komac under the hood - there is no separate Komac action): inputs `identifier: tenequm.pond`, `installers-regex: '\.zip$'` (the input is optional but its default regex ignores `.zip` - this line is load-bearing), `token: ${{ secrets.WINGET_PAT }}`.
- Review latency observed for comparable tools: same-day to ~4 days per version.

### 4.2 Scoop

- Create `tenequm/scoop-bucket` from [ScoopInstaller/BucketTemplate](https://github.com/ScoopInstaller/BucketTemplate) (default branch `master`): enable Actions with read/write permissions, replace the placeholder repo string in `bin/auto-pr.ps1`, add the `scoop-bucket` repo topic. No PAT needed - template workflows use `GITHUB_TOKEN`; excavator auto-updates every 4 hours.
- `bucket/pond.json`: `"checkver": "github"` plus the autoupdate URL templated on the release tag, and **no `hash` block**: Scoop's `autoupdate.ps1` has a `github` hashmode that fires automatically for `github.com/<owner>/<repo>/releases/download/...` URLs when no `autoupdate.hash.url` is set - it reads the asset's `digest` from the GitHub releases API. Fallback if that proves flaky: `"hash": { "url": "$baseurl/checksums.txt" }` (the built-in extractor parses sha256sum-format lines both `hash filename` and `filename hash`; no custom regex).
- Install line for docs: `scoop bucket add tenequm https://github.com/tenequm/scoop-bucket && scoop install pond`.

### 4.3 Completions

PowerShell script shipped in the zip; docs show the dot-source registration (`. <path>\_pond.ps1` in `$PROFILE`) and note that executing `$PROFILE` requires execution policy `RemoteSigned` or looser. (clap deliberately defers install location to packagers; there is no upstream-blessed path.) Update the `pond completions` help text (`main.rs:791-793`), which currently documents bash/zsh/fish install paths only, to include the PowerShell line.

### 4.4 Docs

Windows section on the install page: winget (primary), Scoop, `cargo binstall pond-db`, raw zip; `protoc` + NASM prerequisites for `cargo install`; a Defender note (real-time scanning measurably slows many-small-file stores - document an exclusion for the pond data dir; mention Dev Drive with the caveat that pond on ReFS is unverified pending the hard-link question); the MAX_PATH caveat from 3.2; a WSL note (running the Linux binary in WSL is fine when sources and store live in ext4; do not sync across `/mnt/c` - throughput collapses and inotify misses Windows-side writes).

### 4.5 One-time external actions (operator)

1. Classic PAT with `public_repo` scope, saved as `WINGET_PAT` repo secret (fine-grained PATs unsupported by winget-releaser).
2. Fork `microsoft/winget-pkgs` under `tenequm`.
3. Create `tenequm/scoop-bucket` from the template.
4. First `komac new` submission after the first phase-2 release.

## 5. Phase 3 - parity

Sequencing inside the phase: **signing -> scheduler -> durability**. Signing first because registering a scheduled task from an unsigned binary is a documented Defender-deletes-the-binary trigger (resticprofile ships that exact warning).

### 5.1 Code signing

Apply to [SignPath Foundation](https://signpath.org) (free OSS tier; requires OSI license - Apache-2.0 qualifies - MFA, a published signing-policy page, defined roles, and demonstrated usage, which phases 1-2 accumulate). Integrate `signpath/github-action-submit-signing-request` into the Windows release leg. Do not buy an EV cert (EV no longer buys SmartScreen reputation - verified against Microsoft's current guidance). If SignPath rejects for insufficient reputation, re-apply later; signing is not a blocker for any phase-1/2 channel.

### 5.2 `pond schedule` Windows backend

Post-PR #147 this is a rework, not a greenfield build: the PR ships a working backend at merge (schtasks `/Create /XML` registration, single-call `/Query /XML ONE` probe, cadence round-trip with tests, UTF-16 output decoding, battery/catch-up settings, already-scheduled no-op, TOCTOU-safe stop) with a `.cmd`+`.vbs` action chain. The delta here is swapping that action chain for `pondw.exe` + `--state-dir` (VBScript deprecation, no wrapper artifacts) - everything else below describes the shipped design that carries over.

- **`pondw.exe`:** `src/bin/pondw.rs` (~20 lines), `#![cfg_attr(windows, windows_subsystem = "windows")]`, calls the same entrypoint as `pond`; built and shipped in the zip only on Windows. It has no console, so scheduled-run output goes to the sync log file in the state dir (same shape as the launchd/systemd backends); `pond schedule logs` reads it.
- **Registration:** generate task XML, register with `schtasks /Create /TN "pond-sync" /XML <file> /F`, remove with `/Delete /TN "pond-sync" /F` (delete-then-create for updates; schtasks has no in-place update). Write the XML file as UTF-16LE with a BOM and declare `encoding="UTF-16"` - the only safe combination, and entirely undocumented (Microsoft's schtasks docs say nothing about encoding): schtasks sniffs only for the `FF FE` BOM and otherwise widens the file through the ANSI code page. Verified failure matrix: undeclared non-ASCII mojibakes silently into a task that registers fine and does nothing on every tick (the mode PR #147 hit); a declared encoding that disagrees with the ANSI-widened bytes fails loudly ("cannot switch the encoding"); UTF-8 with a BOM is rejected outright. The GUI export and the files under `C:\Windows\System32\Tasks` are UTF-16LE+BOM; PowerShell's `Export-ScheduledTask` is no reference (it returns a string whose on-disk encoding depends on the redirect). XML shape (settled by research; do not use COM - the Rust `planif` crate cannot express minute-level repetition, and comparable tools migrated away from COM):
  - `Principal`: current-user SID, `LogonType: InteractiveToken`, `RunLevel: LeastPrivilege` (stores no credential - the blank-password Credential Manager trap is documented in resticprofile's tracker).
  - Trigger: `TimeTrigger` with `Repetition/Interval PT5M` and **no** `Duration` (encoding A - what `schtasks /SC MINUTE /MO 5` itself emits). Do not use `CalendarTrigger`+repetition, and deliberately omit `UseUnifiedSchedulingEngine` (Microsoft documents the unified engine as not supporting calendar-trigger repetition).
  - Settings: `DisallowStartIfOnBatteries=false` and `StopIfGoingOnBatteries=false` (battery policy belongs in-process, not in the task - a task-level gate silently skips runs), `StartWhenAvailable=true`, `MultipleInstancesPolicy=IgnoreNew` (belt to the `--no-wait` suspenders), `ExecutionTimeLimit` bounded. The `Hidden` element is UI-only and irrelevant to the console question - `pondw.exe` is what prevents window flashes.
  - Action: `Exec` with the absolute `pondw.exe` path and arguments `sync -q --no-wait --state-dir <resolved>`. Task Scheduler expands `%VAR%` inside `<Arguments>` at runtime with no escape syntax - reject `%` in the baked paths at registration, the same gate the unix templates apply.
- **State-dir pinning:** Task Scheduler `Exec` actions carry no environment block, so the launchd/systemd env-injection pattern does not port directly. (A `.cmd` wrapper can carry env - PR #147 pins `XDG_STATE_HOME` that way - but the wrapper is exactly what pops the console that then needs a shim to hide; with `pondw.exe` as the direct `Exec` target there is no wrapper, so argument pinning stays the right end-state shape.) The Windows analog: a global `--state-dir` flag on `pond` (hidden from help on unix; on Windows the scheduler bakes the registration-time resolved state dir into the task's arguments). Registration warns if `XDG_STATE_HOME` is set in the interactive session, naming the pinned path - same invariant as the spec's env-pinning rationale, different mechanism. **Spec amendments this carries (missed in the original draft):** section 7.8's "pins the resolved `XDG_STATE_HOME` into the job's environment" sentence gains the Windows argument-pinning variant, and the new `--state-dir` global gets its 7.8 entry.
- **Error handling:** branch on `schtasks` exit codes only, never stderr text (locale-dependent - `Access is denied` is `Acceso denegado` on es-ES); surface stderr verbatim as diagnostics. Decode captured output as UTF-16LE first when it leads with a BOM or carries embedded NULs - `/Query /XML` and localized consoles emit UTF-16, and a lossy-UTF-8 read garbles the diagnostics. Sanitize nothing into the task name (it is a fixed literal). Read state back with `/Query /TN pond-sync /XML`, never CSV/LIST (CSV drops fields).
- **Until this ships:** nothing to bridge - PR #147 lands a working Task Scheduler backend at merge, so unattended sync exists from phase 1 onward; the swap above is invisible to users. (The pre-PR `not(unix)` stubs this bullet used to patch are deleted by the PR.)

### 5.3 Windows durability wrapper

Windows counterpart to `FsyncOnWrite` at the same seam (`substrate.rs` `store_wrapper`, `cfg(windows)` + `is_local`): after each inner `put`/multipart-`complete`/`copy`/`rename` returns, open the destination and `FlushFileBuffers` (`File::sync_all`). No directory fsync exists on Windows (`FlushFileBuffers` on a directory handle fails; verified across object_store, RocksDB, SQLite - all skip it); the post-publish flush is the NTFS-validated recipe (WireGuard, Subversion power-loss experiments) closing the "name durable, bytes lost" window the metadata-only NTFS journal creates. Ordering argument is the same as the unix wrapper: each artifact durable before Lance proceeds, so data files precede the manifest that references them. Residual window (crash between the hard-link publish and our flush) stays covered by `local-store-self-heal`. Ships with the `windows-store-durability` spec rule documenting mechanism, the no-dir-fsync reality, and self-heal as backstop - **and amends the existing `local-store-durability` closing sentence** ("the rule is unix-only; Windows relies on `local-store-self-heal`", spec section 3.3), which stops being fully true the moment the wrapper lands.

## 6. Contingencies (named triggers, pre-decided responses)

| Trigger | Response |
|---|---|
| OCC probe fails on `RenameCommitHandler` (3.3) | Gate local `file://` stores on Windows behind a typed error naming a remote store URL as the requirement; ship phase 1 remote-only; file the upstream Lance issue; revisit after fix. |
| Rowmap purge-then-rebuild probe fails (3.3 probe 2) | Corrected trigger (segments are ALREADY generation-named - `rowmetamap-<key>-v{N}.rmm`, `rowmap.rs:153-161` - so fresh publishes never rename over a live file; the only rename-over-mapped case is: `purge_rowmaps` fails to delete a mapped segment, then the rebuild writes the SAME version filename). Response: on any purge deletion failure, the rebuild appends a fresh disambiguating suffix to the segment name (never reuse a name that still exists) and the sweep retires the stragglers later. Contained to `rowmap.rs` naming + the `sessions.rs:2148-2160` purge/rebuild call path. |
| Hard-link commit fails on ReFS/Dev Drive | Document "store must live on NTFS" + detect-and-error at store creation; upstream issue to Lance (they hit the same class on Android). |
| User points a store at a UNC path / mapped network drive | Pre-decided, not probe-gated (3.4): typed error at `StorageUrl` parse/open naming the limitation and the alternatives; upstream object_store#715 (UNC host dropped) + lance#6616 (fix PR #8378 open). Lift when fixed upstream. |
| claude-code Windows fixture contradicts the slug model | Fix the decoder to the fixture; the fixture is authoritative; ship claude-code Windows ingest only when round-trip passes. |
| SignPath rejects the application | Phase-3 scheduler waits; manual-schtasks docs remain the Windows scheduling story; optionally Certum open-source cert (~EUR 49/yr) if demand is real. |

## 7. Out of scope (decided, do not revisit without new evidence)

`aarch64-pc-windows-msvc` (no arm64 at all), `x86_64-pc-windows-gnu` (dropped), Chocolatey (human-moderated queue, gh-style community posture instead), MSI/MSIX, npm wrapper, cargo-dist (alive but duplicates release-plz's ownership), hf-hub 1.0 upgrade, `fp16kernels` (upstream-disabled on Windows), ACL code for config, COM-based Task Scheduler, WSL-first positioning.

## 8. Verified upstream reference points

- Lance Windows commit routing: `lance-table-8.0.0/src/io/commit.rs:1075` (`RenameCommitHandler` iff `cfg!(windows)` for `file` schemes); Lance Windows CI: `rust.yml` `windows-build` on `windows-latest-4x`, system protoc, full `cargo test`.
- `protobuf-src` Windows breakage: [rust-protobuf-native#4](https://github.com/MaterializeInc/rust-protobuf-native/issues/4).
- `std::env::home_dir` Windows semantics (USERPROFILE then `GetUserProfileDirectory`, HOME dropped in 1.85): [std docs](https://doc.rust-lang.org/std/env/fn.home_dir.html).
- `dirs 6.0.0` Windows mapping (config=Roaming, data_local/cache=Local, state=None): [docs.rs/dirs](https://docs.rs/dirs/latest/dirs/). Canonical repo is on Codeberg (GitHub archive is stale).
- `embed-manifest 1.5.0` defaults include `longPathAware`: [docs.rs/embed-manifest](https://docs.rs/embed-manifest/latest/embed_manifest/).
- winget portable-zip behavior (PATH + ARP + uninstall) and no-signing validation: winget-pkgs FAQ + maintainer statements; automation: [winget-releaser](https://github.com/vedantmgoyal9/winget-releaser) (`installers-regex` default ignores zips).
- Scoop template: [BucketTemplate](https://github.com/ScoopInstaller/BucketTemplate); hash-from-checksums pattern per Main-bucket `xq.json`.
- NTFS rename durability failure + flush recipe: [WireGuard f9fccd8](https://github.com/WireGuard/wireguard-windows/commit/f9fccd8266d2116d7ed8f1fa73be155115cb050f); no directory fsync on Windows (object_store `fsync_dir` is unix-only).
- Task Scheduler: `Hidden` is UI-only (rclone maintainers); COM migration-away precedent: resticprofile PR #459; encoding guidance per `/SC MINUTE` XML output.
- `+crt-static` precedent + traps: [lancedb `.cargo/config.toml`](https://github.com/lancedb/lancedb/blob/main/.cargo/config.toml); Cargo target-rustflags/`--target` host-leak rule per Cargo reference.
- aws-lc-rs Windows prerequisites (NASM; installed NASM always wins): [aws-lc-rs requirements](https://aws.github.io/aws-lc-rs/requirements/windows.html).
- Claude Code Windows paths and slug evidence: `%USERPROFILE%\.claude`, issues #33140/#14088 (slug forms, mapped-drive duality); Codex: `%USERPROFILE%\.codex`, date-partitioned rollouts (no slug problem).
- Claude Desktop Windows MSIX virtualization (`userData` -> `%LOCALAPPDATA%\Packages\<family>\LocalCache\Roaming\Claude\`): anthropics/claude-code issues #25579, #26073.
- std 1.95 Windows `rename` = `MoveFileExW` primary with `FileRenameInfoEx`+POSIX fallback only on access-denied (`library/std/src/sys/fs/windows.rs:1311-1380`); POSIX-first shipped in 1.85.0 ([rust#131072](https://github.com/rust-lang/rust/pull/131072)) and was reverted in 1.85.1 ([rust#137528](https://github.com/rust-lang/rust/pull/137528)). Delete-while-mapped is blocked regardless of `FILE_SHARE_DELETE`: [FILE_DISPOSITION_INFORMATION_EX](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-_file_disposition_information_ex) names "an existing mapped view" as a `STATUS_CANNOT_DELETE` cause.
- moon task fingerprint carries no OS/arch ([task_fingerprint.rs](https://github.com/moonrepo/moon/blob/master/crates/task-hasher/src/task_fingerprint.rs)) - the reason the Windows CI job must not run shared moon tasks; `options.os` task scoping since moon v1.28 ([project config](https://moonrepo.dev/docs/config/project)).
- cliclack Windows Ctrl-C fix: [cliclack PR #112](https://github.com/fadeevab/cliclack/pull/112) (released in 0.5.5) on top of [console PR #235](https://github.com/console-rs/console/pull/235).
- opencode data dir is XDG-verbatim on Windows: `packages/core/src/global.ts` + `xdg-basedir@5.1.0` (no platform branch).
- [PR #147](https://github.com/tenequm/pond/pull/147) (community Windows port, compared post-hoc - section 2.1): source of the main-thread-stack fix, the `raw_arg` spawn shape, the UTF-16LE task-XML requirement, the `%`-expansion gate, and UTF-16 console decoding; independently confirms the phase-1 runtime core and the oh-my-pi portability evidence.
- VBScript deprecation (why the PR's `.vbs` shim is interim and `pondw.exe` is the end state): [Microsoft's timeline](https://techcommunity.microsoft.com/blog/windows-itpro-blog/vbscript-deprecation-timelines-and-next-steps/4148301) - phase 1 (FOD enabled by default) now; phase 2 (FOD disabled by default) stated only as "around 2026-2027", no committed date per Microsoft's Sept 2025 restatement; phase 3 (removal) TBD. Windows Server 2025 is on the same path. Removing the FOD leaves `wscript.exe` present but every `.vbs` failing with "no script engine for file extension" - the exact breakage mode for a `.vbs` task action.

## Appendix A - windows-verify job and publish-release fragments (verified wiring)

Reference implementation from the moon-wiring verification pass. Line references are against the tree at the time of writing; adjust on drift. Action SHAs are placeholders - pin real SHAs at implementation time.

### A.1 The windows-verify job

```yaml
  # Native Windows gate + the source of the shipped msvc zip. Plain cargo, NOT
  # moon: moon's task fingerprint carries no OS (crates/task-hasher
  # task_fingerprint.rs), so `moon run pond:test` here would hit the hash the
  # linux runner already cached and report "cached" without building anything.
  # Commands mirror packages/pond/moon.yml:34 and moon.yml:78 - keep in lockstep.
  windows-verify:
    if: >-
      github.event_name == 'workflow_dispatch' ||
      (github.event_name == 'push' && github.ref == 'refs/heads/main') ||
      (github.event_name == 'pull_request' && startsWith(github.head_ref, 'release-plz-'))
    runs-on: windows-2025
    timeout-minutes: 120
    concurrency: { group: "ci-windows-${{ github.ref }}", cancel-in-progress: true }
    env:
      # Same rule as flake-check/macos-verify: no moon runs here, and this job
      # compiles third-party build scripts on a hosted runner.
      BAZEL_REMOTE_AUTH: ""
      # Short target dir: the default nested path blows MAX_PATH during MSVC
      # linking (LNK1104 / os error 206). Never set RUSTFLAGS in this job - it
      # would silently replace .cargo/config.toml's +crt-static.
      CARGO_TARGET_DIR: C:\t
    steps:
      # fetch-depth 1 is fine: nothing here runs moon, and POND_BUILD_COMMIT
      # only needs HEAD.
      - uses: actions/checkout@<pin-sha> # v7

      # The windows-2025 image ships neither protoc nor NASM (verified against
      # actions/runner-images Windows2025-VS2026-Readme). protoc: direct zip,
      # matching Lance's own windows-build job. NASM: aws-lc-sys.
      - name: Install protoc
        shell: pwsh
        run: |
          $ErrorActionPreference = 'Stop'
          $v = '35.1'   # keep in lockstep with PROTOC_VERSION in .github/actions/bootstrap
          Invoke-WebRequest -Uri "https://github.com/protocolbuffers/protobuf/releases/download/v$v/protoc-$v-win64.zip" -OutFile protoc.zip
          Expand-Archive protoc.zip -DestinationPath C:\protoc
          "C:\protoc\bin" | Out-File -FilePath $env:GITHUB_PATH -Encoding utf8 -Append
      - name: Install NASM
        shell: pwsh
        run: choco install nasm -y --no-progress

      # rustup honors rust-toolchain.toml (1.95.0); the host triple IS the build
      # target, so no extra rust-std download.
      - name: Materialize the pinned toolchain
        shell: pwsh
        run: |
          rustup toolchain install --no-self-update
          rustup show active-toolchain
          cargo --version

      - uses: Swatinem/rust-cache@<pin-sha>   # >= v2.9.2 (windows path-validation fix)
        with:
          save-if: ${{ github.ref == 'refs/heads/main' }}

      # Full suite: DataFusion, axum and rmcp get their first Windows run here.
      - name: Test
        run: cargo test --locked --target x86_64-pc-windows-msvc

      # --profile dist, not --release: `release` is pond's no-LTO iteration
      # profile (Cargo.toml:23-29); every shipped binary is built with `dist`.
      - name: Build release binary
        shell: pwsh
        run: |
          $env:POND_BUILD_COMMIT = (git rev-parse --short HEAD)
          cargo build --locked --profile dist --target x86_64-pc-windows-msvc

      # Mirrors macos-verify's launch smoke (ci.yml:197-207). Redirect, never
      # pipe: `| Select-String` closing the pipe trips clap_complete's unwrap.
      - name: Launch smoke
        shell: pwsh
        run: |
          $ErrorActionPreference = 'Stop'
          $exe = "$env:CARGO_TARGET_DIR\x86_64-pc-windows-msvc\dist\pond.exe"
          & $exe --version
          & $exe --help | Out-Null
          & $exe completions powershell | Set-Content -Encoding utf8 completions.ps1
          Select-String -Path completions.ps1 -Pattern 'pond' -Quiet

      - name: Package release zip
        shell: pwsh
        run: |
          $ErrorActionPreference = 'Stop'
          New-Item -ItemType Directory -Force -Path stage/completions | Out-Null
          Copy-Item "$env:CARGO_TARGET_DIR\x86_64-pc-windows-msvc\dist\pond.exe" stage\pond.exe
          # Completions come from the just-built native binary (the linux tarballs
          # get theirs the same way, moon.yml:112-117). clap_complete::Shell has a
          # `powershell` variant (main.rs:799).
          & stage\pond.exe completions powershell | Set-Content -Encoding utf8 stage\completions\_pond.ps1
          & stage\pond.exe completions bash      | Set-Content -Encoding utf8 stage\completions\pond.bash
          & stage\pond.exe completions zsh       | Set-Content -Encoding utf8 stage\completions\_pond
          & stage\pond.exe completions fish      | Set-Content -Encoding utf8 stage\completions\pond.fish
          New-Item -ItemType Directory -Force -Path dist | Out-Null
          Compress-Archive -Path stage\pond.exe,stage\completions -DestinationPath dist\pond-x86_64-pc-windows-msvc.zip -Force

      - uses: actions/upload-artifact@<pin-sha> # v7
        with:
          name: dist-windows
          path: dist/pond-x86_64-pc-windows-msvc.zip
          if-no-files-found: error
          retention-days: 1
```

### A.2 publish-release changes

```yaml
  publish-release:
    needs: [dist-build, macos-verify, windows-verify]
    # ... existing config unchanged ...
    steps:
      # ... existing checkout etc. ...
      - uses: actions/download-artifact@<pin-sha> # v8
        with: { name: dist-all, path: dist }
      # The msvc zip is built natively by windows-verify, not by the moon dist
      # task, so it arrives as its own artifact.
      - uses: actions/download-artifact@<pin-sha> # v8
        with: { name: dist-windows, path: dist }

      # checksums.txt is produced inside the cached moon task (moon.yml:131) and
      # therefore covers only the three cross-compiled tarballs. Regenerate over
      # the merged dist/ so the released file covers the Windows zip too.
      # Must run BEFORE the release-plz step (or at minimum before the
      # `gh release upload` at ci.yml:282, which globs dist/pond-*).
      - name: Merge Windows zip into checksums
        run: |
          set -euo pipefail
          test -f dist/pond-x86_64-pc-windows-msvc.zip
          ( cd dist && sha256sum pond-* > checksums.txt )
          cat dist/checksums.txt
      # ... existing release-plz / upload / nix / homebrew steps unchanged;
      # the sha() helpers at ci.yml:291/373 read only pond-<target>.tar.xz.
```

### A.3 Workflow on: block and job graph

```yaml
on:
  push:
    branches: [main]
  pull_request:
  workflow_dispatch:   # new; also makes build-and-test/flake-check dispatchable
```

```
build-and-test ---+
flake-check ------+-> release-prep -> dist-build -+-> macos-verify -+
                                                  +-----------------+-> publish-release
windows-verify -----------------------------------------------------+   (no needs: starts at t=0)
```
