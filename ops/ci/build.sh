#!/usr/bin/env bash
# Cross-compile pond for x86_64-unknown-linux-gnu + aarch64-apple-darwin via
# cargo-zigbuild in the pond-zigbuild image. Two separate cargo invocations
# (cargo feature unification across multi-target leaks candle's macOS-only
# `metal` feature into the Linux build -> objc2 fails on linux).
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO_ROOT"

docker run --rm \
  -v "$PWD":/io -w /io \
  -v "$HOME/.cache/zigbuild-cargo-registry:/root/.cargo/registry" \
  -v "$HOME/.cache/zigbuild-cargo-git:/root/.cargo/git" \
  -v "$HOME/.cache/kache-zigbuild:/root/.cache/kache" \
  -v "$HOME/.config/kache:/root/.config/kache:ro" \
  -e KACHE_S3_ACCESS_KEY \
  -e KACHE_S3_SECRET_KEY \
  -e KACHE_LOG=kache=warn \
  -e KACHE_PROGRESS=always \
  -e CARGO_TERM_COLOR=always \
  pond-zigbuild:local \
  bash -ec '
    trap "kache sync --push" EXIT
    kache sync --pull
    echo === macOS arm64 ===
    cargo zigbuild --profile dist --target aarch64-apple-darwin
    echo === Linux x86_64 ===
    cargo zigbuild --profile dist --target x86_64-unknown-linux-gnu
    kache stats
  '
