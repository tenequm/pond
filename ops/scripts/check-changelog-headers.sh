#!/usr/bin/env bash
# Gate the top CHANGELOG.md section's `###` headers to the canonical emoji
# taxonomy from .github/release-plz.toml. The release body is this section verbatim
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

# Backstop for the release-note flow: a user-visible entry with no prose under it
# means a squash commit landed with an empty body (see AGENTS.md "Changelog
# authoring"). Nothing can fix the commit now, but the release PR can still be
# hand-patched before it ships - this is the last moment anyone looks.
#
# Only checked when this change actually writes the top entry - i.e. the release
# PR. An entry already on `main` is shipped history the flow cannot fix, and
# enforcing prose there would block every later commit on someone else's miss.
base_section=$(git show origin/main:"$file" 2>/dev/null | awk '/^## \[/{n++} n==1{print} n==2{exit}')
if [ -n "$base_section" ] && [ "$section" = "$base_section" ]; then
  exit 0
fi

bare=$(awk '
  /^### / { visible = ($0 ~ /(New Features|Bug Fixes|Performance|Breaking Changes)/); next }
  /^- / { if (visible) { entry = $0; if ((getline nxt) <= 0 || nxt !~ /^  /) print entry } }
' <<< "$section")

if [ -n "$bare" ]; then
  echo "check-changelog: user-visible entries with no release-note prose:" >&2
  printf '  %s\n' "$bare" >&2
  echo >&2
  echo "Write the prose in the squash-commit body at merge time (PR's '## Release note')." >&2
  echo "For an already-merged commit, hand-patch this entry on the release PR." >&2
  exit 1
fi
