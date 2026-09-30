#!/usr/bin/env bash
# Compile-time reads of the checkout path make the compiler cache key per
# checkout, so every new git worktree recompiles the test and bench binaries.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)/packages/pond"

if grep -rnE --include='*.rs' 'env!\(\s*"(CARGO_MANIFEST_(DIR|PATH)|CARGO_BIN_EXE_|CARGO_TARGET_TMPDIR)' src tests benches; then
  echo "error: CARGO_MANIFEST_DIR / CARGO_MANIFEST_PATH / CARGO_BIN_EXE_* / CARGO_TARGET_TMPDIR read at compile time (above)." >&2
  echo "Resolve it at runtime instead: a cwd-relative fixture path (integration tests), manifest_dir() (lib unit tests), pond_bin(), or std::env::var_os(...)." >&2
  exit 1
fi
