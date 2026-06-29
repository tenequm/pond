#!/usr/bin/env bash
# Gate the top CHANGELOG.md section's `###` headers to the canonical emoji
# taxonomy from .release-plz.toml. The release body is this section verbatim
# (release-plz git_release_body default), and it fans out to the GitHub release
# + homebrew-tap + pond-nix, so a hand-edit that drops the emoji headers silently
# de-styles all three. This is the only check that stops that before it ships.
set -euo pipefail

file="${1:-CHANGELOG.md}"

allowed=(
  "🛠 Breaking Changes"
  "🎉 New Features"
  "🐛 Bug Fixes"
  "🚀 Performance"
  "🚜 Refactor"
  "📚 Documentation"
  "🧹 Chores"
  "🔧 Other"
)

section=$(awk '/^## \[/{n++} n==1{print} n==2{exit}' "$file")
[ -n "$section" ] || { echo "check-changelog: no '## [version]' section in $file" >&2; exit 1; }

bad=0
while IFS= read -r line; do
  [[ "$line" == "### "* ]] || continue
  header=$(printf '%s' "$line" | sed -E 's/^### +//; s/<!--[^>]*-->//; s/^ *//; s/ *$//')
  ok=0
  for a in "${allowed[@]}"; do [ "$header" = "$a" ] && { ok=1; break; }; done
  if [ "$ok" -eq 0 ]; then
    echo "check-changelog: non-canonical header: ### $header" >&2
    bad=1
  fi
done <<< "$section"

if [ "$bad" -ne 0 ]; then
  echo >&2
  echo "Allowed headers (keep release-plz's generated emoji form, add prose under them):" >&2
  printf '  ### %s\n' "${allowed[@]}" >&2
  exit 1
fi
