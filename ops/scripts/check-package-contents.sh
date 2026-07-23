#!/usr/bin/env bash
# Every file the crate pulls in via include_str!/include_bytes! must survive
# Cargo.toml's [package].exclude, or the published crate fails to compile on
# `cargo install` (v0.10.0-v0.13.0 shipped broken: SKILL.md was excluded).
set -euo pipefail

# The crate lives at packages/pond; run from there so `src` and the
# `cargo package --list` paths line up regardless of the caller's cwd.
cd "$(git rev-parse --show-toplevel)/packages/pond"

packaged=$(cargo package --list --allow-dirty)

bad=0
while IFS= read -r hit; do
  src=${hit%%:*}
  path=$(printf '%s' "$hit" | sed -E 's/.*!\("([^"]+)"\).*/\1/')
  resolved=$(python3 -c 'import os,sys; print(os.path.normpath(os.path.join(os.path.dirname(sys.argv[1]), sys.argv[2])))' "$src" "$path")
  if ! grep -qxF "$resolved" <<< "$packaged"; then
    echo "check-package: $src embeds '$path' but '$resolved' is not in the packaged crate - remove it from Cargo.toml [package].exclude" >&2
    bad=1
  fi
done < <(grep -rnoE --include='*.rs' 'include_(str|bytes)!\("[^"]+"\)' src)

exit "$bad"
