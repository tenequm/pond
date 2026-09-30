#!/usr/bin/env bash
# Compile-time reads of the checkout path make the compiler cache key per
# checkout, so every new git worktree recompiles the test and bench binaries.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)/packages/pond"

if grep -rnE --include='*.rs' 'env!\(\s*"(CARGO_MANIFEST_DIR|CARGO_BIN_EXE_)' src tests benches; then
  echo "error: CARGO_MANIFEST_DIR / CARGO_BIN_EXE_* read at compile time (above)." >&2
  echo "Read it at runtime instead: std::env::var_os(...), via manifest_dir() / pond_bin()." >&2
  exit 1
fi
