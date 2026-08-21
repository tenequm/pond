#!/usr/bin/env bash
# Publish the npm plugin packages that ride a pond release. Idempotent: a
# package publishes only when its package.json version is absent from the
# registry, so bump-less releases and job re-runs are no-ops. Auth is npm
# trusted publishing (OIDC): the npm CLI exchanges the GitHub Actions
# id-token itself - no npm token exists anywhere.
set -euo pipefail
for dir in "$@"; do
  name=$(node -p "require('./$dir/package.json').name")
  version=$(node -p "require('./$dir/package.json').version")
  if [ -n "$(npm view "$name@$version" version 2>/dev/null || true)" ]; then
    echo "$name@$version already published - skipping"
    continue
  fi
  echo "publishing $name@$version"
  (cd "$dir" && npm ci && npm publish --access public)
done
