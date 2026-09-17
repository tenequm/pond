#!/usr/bin/env bash
# The publish-release tail, after release-plz has cut v$V: merge checksums,
# upload the binaries to pond's release (their only host), point the repo flake
# at them, push the Homebrew formula. Every step converges instead of repeating,
# so "Re-run failed jobs" is the recovery path for any failure here.
#
# Env: V (version, no leading v), GH_TOKEN (release upload to tenequm/pond),
# GH_RELEASE_TOKEN (the signed commit on main and the homebrew-tap push).
# Reads dist/ relative to the repo root.
set -euo pipefail
: "${V:?}" "${GH_TOKEN:?}" "${GH_RELEASE_TOKEN:?}"

cd "$(dirname "$0")/../.."
repo=tenequm/pond
tag="v$V"
base="https://github.com/$repo/releases/download/$tag"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

sha() { sha256sum "dist/pond-$1.tar.xz" | cut -d' ' -f1; }

# True when $1 is a strictly older version than $2: a re-run of an old release
# must not roll main or the tap back past a newer one.
older() { [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$1" ]; }

# checksums.txt is produced inside the cached moon dist task, so it cannot
# cover the Windows zip built on another runner. Regenerate it over the merged
# dist/ before anything reads it.
test -f dist/pond-x86_64-pc-windows-msvc.zip
(cd dist && sha256sum pond-* > checksums.txt)
cat dist/checksums.txt

# One file per request with a hard timeout, through curl: `gh release upload`
# (2.100.0) stalled for minutes on the 81 MB Windows zip that curl posts in
# seconds. A killed upload leaves the asset in state "starter", which blocks a
# plain retry with "already exists", so each try first clears it. The skip
# compares GitHub's asset digest, not size: checksums.txt is the same size in
# every release.
#
# The timeouts are sized against the job's own `timeout-minutes`, not against a
# healthy upload: `--max-time` alone caps total duration, so a transfer crawling
# at 13 KB/s counts as progress and burns the whole budget before retrying. Three
# tries at the old 600s was a 31m30s worst case for ONE of five assets, inside a
# 30-minute job that also runs release-plz - and the job dying there leaves the
# tag cut and the crate published with no flake commit and no tap push.
# `--speed-limit`/`--speed-time` abandon a dead link instead, and `--max-time`
# 180 caps each try at 10m30s per asset across all three.
#
# `--speed-time 60`, not 30: curl's throughput average decays to zero within a
# few seconds of the last byte, so the window also covers the time GitHub spends
# processing a fully received upload before it answers. At 30 the 81 MB zip had
# ~35s to be acknowledged or a COMPLETE upload was abandoned and re-sent.
# Doubling it is free in the worst case - `--max-time` is the real bound - and
# only slows dead-link detection from ~35s to ~65s.
#
# Note what `--max-time 180` really asks of the 81 MB zip: 450 KB/s sustained,
# well above the 100 KB/s floor `--speed-limit` names. A steady 200 KB/s passes
# the stall check and still dies at max-time. That is deliberate: a hosted runner
# that cannot hold 450 KB/s to uploads.github.com is having an outage, and
# failing the tail fast (it is idempotent - "Re-run failed jobs" resumes) beats
# being SIGKILLed by the job timeout somewhere in the middle of it.
release_id=$(gh api "repos/$repo/releases/tags/$tag" --jq .id)
upload() {
  local f=$1 name want try existing id state digest present
  name=${f##*/}
  want="sha256:$(sha256sum "$f" | cut -d' ' -f1)"
  for try in 1 2 3; do
    if existing=$(gh api "repos/$repo/releases/$release_id" \
      --jq ".assets[] | select(.name == \"$name\") | [.id, .state, .digest // \"\"] | @tsv"); then
      present=false
      while IFS=$'\t' read -r id state digest; do
        [ -n "$id" ] || continue
        if [ "$state" = uploaded ] && [ "$digest" = "$want" ]; then
          present=true
        else
          echo "$name: deleting asset $id (state=$state digest=$digest, want uploaded/$want)"
          gh api -X DELETE "repos/$repo/releases/assets/$id" || true
        fi
      done <<< "$existing"
      if $present; then
        echo "$name: already uploaded"
        return 0
      fi
      # The token goes in on stdin (`-H @-`), never as an argument: an argv
      # header is world-readable in `ps` for the whole upload, and a routine ps
      # on a stalled upload is exactly how it leaked once.
      # Truncated first: curl creates the -o file only once bytes arrive, so a
      # connect-stage failure would otherwise reprint the previous asset's body.
      : > "$tmp/upload.body"
      if printf 'Authorization: Bearer %s\n' "$GH_TOKEN" \
        | curl -sS --fail-with-body --max-time 180 \
          --speed-limit 100000 --speed-time 60 \
          -o "$tmp/upload.body" -H @- \
          -H "Content-Type: application/octet-stream" \
          --data-binary "@$f" \
          "https://uploads.github.com/repos/$repo/releases/$release_id/assets?name=$name"; then
        echo "$name: uploaded"
        return 0
      fi
      # GitHub's error text is in the body; `-o /dev/null` used to discard it,
      # so a 500 or a 422 logged nothing but curl's exit code.
      if [ -s "$tmp/upload.body" ]; then
        head -c 2000 "$tmp/upload.body"
        echo
      fi
    fi
    echo "::warning::$name: upload try $try failed"
    sleep $((try * 15))
  done
  echo "::error::$name: upload failed after 3 tries" >&2
  return 1
}

for f in dist/pond-* dist/checksums.txt; do
  upload "$f"
done

# The repo flake: ops/nix/pond.nix is static and reads release.json, so the
# release commits only that file. The commit lands one after the tag, so
# tag-pinned flake refs carry the previous release's hashes and main is always
# current. It goes through GraphQL createCommitOnBranch rather than git push:
# GitHub signs API commits itself, which satisfies main's required-signatures
# rule with no key material on the runner. [skip ci] keeps it from running CI.
jq -n --arg v "$V" \
  --arg x86 "$(sha x86_64-unknown-linux-gnu)" \
  --arg arm "$(sha aarch64-unknown-linux-gnu)" \
  --arg mac "$(sha aarch64-apple-darwin)" \
  '{version: $v, hashes: {"x86_64-linux": $x86, "aarch64-linux": $arm, "aarch64-darwin": $mac}}' \
  > "$tmp/release.json"

# No `|| true` on the read: a failed lookup must not skip the rollback guard.
main_release=$(GH_TOKEN=$GH_RELEASE_TOKEN gh api "repos/$repo/contents/ops/nix/release.json?ref=main" --jq .content | base64 -d)
main_version=$(jq -r '.version // empty' <<< "$main_release" 2>/dev/null || true)
if [ "$(jq -S . <<< "$main_release" 2>/dev/null || true)" = "$(jq -S . "$tmp/release.json")" ]; then
  echo "ops/nix/release.json on main already points at $tag"
elif [ -n "$main_version" ] && older "$V" "$main_version"; then
  echo "::notice::main's release.json is at $main_version, newer than $V; leaving it"
else
  # Re-read the head OID on every try: expectedHeadOid must be main's tip at
  # commit time, and retries only lose to a push inside that window.
  committed=false
  for i in 1 2 3; do
    # shellcheck disable=SC2016 # $head etc. are GraphQL variables, not shell
    if GH_TOKEN=$GH_RELEASE_TOKEN gh api graphql \
      -f query='mutation($head: GitObjectID!, $contents: Base64String!, $message: String!) {
        createCommitOnBranch(input: {
          branch: { repositoryNameWithOwner: "tenequm/pond", branchName: "main" }
          expectedHeadOid: $head
          message: { headline: $message }
          fileChanges: { additions: [{ path: "ops/nix/release.json", contents: $contents }] }
        }) { commit { oid } }
      }' \
      -f head="$(GH_TOKEN=$GH_RELEASE_TOKEN gh api "repos/$repo/branches/main" --jq .commit.sha)" \
      -f contents="$(base64 < "$tmp/release.json" | tr -d '\n')" \
      -f message="chore(release): point flake at $tag binaries [skip ci]"; then
      committed=true
      break
    fi
    sleep "$i"
  done
  $committed || { echo "createCommitOnBranch kept losing the head-OID race" >&2; exit 1; }
fi

tap="$tmp/tap"
git clone --depth 1 "https://x-access-token:$GH_RELEASE_TOKEN@github.com/tenequm/homebrew-tap" "$tap"
tap_version=$(sed -n 's|.*/releases/download/v\([^/]*\)/.*|\1|p' "$tap/Formula/pond.rb" 2>/dev/null | head -1 || true)
if [ -n "$tap_version" ] && older "$V" "$tap_version"; then
  echo "::notice::the Homebrew formula is at $tap_version, newer than $V; leaving it"
  exit 0
fi
sed -e "s|@BASE@|$base|g" \
  -e "s|@SHA_aarch64-apple-darwin@|$(sha aarch64-apple-darwin)|g" \
  -e "s|@SHA_x86_64-unknown-linux-gnu@|$(sha x86_64-unknown-linux-gnu)|g" \
  -e "s|@SHA_aarch64-unknown-linux-gnu@|$(sha aarch64-unknown-linux-gnu)|g" \
  ops/homebrew/pond.rb.in > "$tap/Formula/pond.rb"
git -C "$tap" add Formula/pond.rb
git -C "$tap" diff --quiet --staged || git -C "$tap" -c user.name=ci -c user.email=ci@pond commit -m "pond $V"
git -C "$tap" push
