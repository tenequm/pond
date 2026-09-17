#!/usr/bin/env bash
# The multi-target Linux/macOS dist build, driven by the `repo:build-dist` moon
# task. It lives in a file rather than inline in moon.yml so that editing a
# comment here does not change the task's hash: moon fingerprints an inline
# `script:` verbatim, so every prose fix in it invalidated a three-target
# cross-compile. As a file it is a declared input instead - a real change still
# invalidates, a reworded comment does not.
#
# Runs from the repo root (moon runs the task on the `repo` project), so cargo
# resolves the workspace target dir and dist/ lands where publish-release reads
# it. Needs the pond-runner image toolchain: zig 0.16 + cargo-zigbuild + the
# macOS SDK via SDKROOT + rcodesign, plus rustup on PATH.
set -euo pipefail

export POND_BUILD_COMMIT="$(git rev-parse --short HEAD)"
TD="${CARGO_TARGET_DIR:-target}"

# `profile = "minimal"` + targets-in-toml does not reliably fetch
# rust-std for non-host targets.
rustup target add aarch64-apple-darwin \
                  aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu

# cargo-zigbuild 0.23.4 on zig 0.16 detaches the -exported_symbols_list
# operand and breaks Apple cdylib links (rust-cross/cargo-zigbuild#479).
# Reporting zig <0.16 takes the old path that strips that flag pair; drop
# this once a release carries rust-cross/cargo-zigbuild#480.
export CARGO_ZIGBUILD_ZIG_VERSION=0.15.2

# kache (S3 rustc cache): warm builds ride CARGO_TARGET_DIR; this is the
# cold-start net for a fresh /ci-cache. The remote is no longer written
# here - it lives in .github/kache/linux-dist.toml, which the bootstrap
# selects through KACHE_CONFIG, so the wrapper and any daemon kache spawns
# for itself both resolve it from a file they can re-read. The old stub
# named no bucket and leaned on the job's KACHE_S3_* env, which kache
# strips from a daemon spawn. Push on EXIT, not after success: crates
# compiled before a failure still warm the retry.
# --all: one paginated LIST of the per-job prefix instead of one LIST
# per Cargo.lock crate (~833 serial LISTs, 89 s measured from a laptop,
# worse from a runner). Safe only because the prefix holds exactly this
# job's artifacts. --allow-partial: kache exits non-zero on any failed
# transfer, and under `set -euo pipefail` that would redden a job that
# would compile through fine - a partial pull is a slower build, never a
# broken one.
export RUSTC_WRAPPER=kache
kache sync --pull --all --allow-partial
trap 'kache sync --push || true' EXIT

# Cargo unifies features across all targets in one invocation, leaking
# cfg-gated deps across platforms, so macOS builds alone: it pulls
# candle's macOS-only `metal`, which the linux pair must not see.
#
# Windows is not here at all - the msvc zip is built natively by the
# windows-dist job in ci.yml and handed to publish-release as its own
# artifact. Cross-compiling Windows was dropped with the gnu target: it
# never worked at runtime and no CI job ever executed it.
cargo zigbuild --locked --profile dist --target aarch64-apple-darwin
cargo zigbuild --locked --profile dist \
  --target aarch64-unknown-linux-gnu \
  --target x86_64-unknown-linux-gnu

mkdir -p target/dist
cp "$TD/aarch64-apple-darwin/dist/pond" target/dist/pond-aarch64-apple-darwin
# zig records LC_BUILD_VERSION sdk >= 26, which makes macOS 26 dyld
# reject its duplicate libobjc load command; rewrite the recorded sdk to
# 15.0 and ad-hoc re-sign (the byte-patch invalidates the signature).
python3 ops/scripts/patch-macos-sdk.py target/dist/pond-aarch64-apple-darwin
# Developer ID + hardened runtime when the cert is present (release CI),
# ad-hoc otherwise so local and fork builds still get a loadable arm64
# binary. Gatekeeper denies unnotarized Developer ID outright, so the
# cert path is only ever taken by CI, which pairs it with notarization.
if [ -n "${APPLE_P12_BASE64:-}" ]; then
  creds="$(mktemp -d)"
  printf '%s' "$APPLE_P12_BASE64" | base64 -d > "$creds/id.p12"
  printf '%s' "${APPLE_P12_PASSWORD:-}" > "$creds/id.pass"
  rcodesign --config-file /dev/null sign \
    --p12-file "$creds/id.p12" --p12-password-file "$creds/id.pass" \
    --code-signature-flags runtime \
    target/dist/pond-aarch64-apple-darwin
  rm -rf "$creds"
else
  rcodesign --config-file /dev/null sign target/dist/pond-aarch64-apple-darwin
fi
cp "$TD/aarch64-unknown-linux-gnu/dist/pond" target/dist/pond-aarch64-unknown-linux-gnu
cp "$TD/x86_64-unknown-linux-gnu/dist/pond" target/dist/pond-x86_64-unknown-linux-gnu
ls -lh target/dist/

# Completion scripts are target-independent, so one native run of the
# linux-amd64 binary (this runner's arch) generates them for every
# tarball. They must ship pre-generated: nix can't execute the binary
# in installPhase (autoPatchelfHook only fixes the interpreter later,
# in fixupPhase).
rm -rf completions
mkdir -p completions
chmod +x target/dist/pond-x86_64-unknown-linux-gnu
target/dist/pond-x86_64-unknown-linux-gnu completions bash > completions/pond.bash
target/dist/pond-x86_64-unknown-linux-gnu completions zsh  > completions/_pond
target/dist/pond-x86_64-unknown-linux-gnu completions fish > completions/pond.fish

# Package into the archive names brew/nix reference, `pond` at the root.
# rm first: moon can hydrate a previously cached dist/ into the tree.
rm -rf dist
mkdir -p dist
for t in aarch64-apple-darwin x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu; do
  install -m755 "target/dist/pond-$t" pond
  tar -cJf "dist/pond-$t.tar.xz" pond completions
done
rm -f pond
rm -rf completions
# Covers only what this task produced. publish-release regenerates it
# over the merged dist/ once the natively-built Windows zip lands, so
# this copy never reaches a release.
( cd dist && sha256sum pond-* > checksums.txt )
ls -lh dist/
