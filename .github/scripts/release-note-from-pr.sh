#!/usr/bin/env bash
# Changelog repair, run by git-cliff as a commit_preprocessor (see
# .github/release-plz.toml). Reads a commit message on stdin, writes it back on
# stdout - unchanged, except for one case.
#
# The changelog prose lives in the squash-commit body, which is the PR
# description (repo squash defaults PR_TITLE/PR_BODY). A merge that submits an
# empty description field overrides that default, and the note is gone from
# history for good - commit messages are immutable, and git-cliff's GitHub
# integration exposes pr_title/pr_number/pr_labels but no pr_body to fall back
# on. Happened once on #191, silently.
#
# So when a squashed-PR commit carries no `## Release note` section, fetch it
# from the PR that produced it and append it to the message before git-cliff
# parses. The changelog and the GitHub release body both render from this, so
# both are correct regardless of how the merge was performed. Side benefit: a
# note can be corrected after merge by editing the PR description.
#
# Every failure path prints the message unchanged - a missing `gh`, no auth, no
# network, or an unparseable subject must never break changelog generation
# (`release-plz update` runs locally too, per AGENTS.md).
set -uo pipefail

msg=$(cat)
printf_msg() { printf '%s' "$msg"; exit 0; }

case "$msg" in *"
## Release note"*) printf_msg ;; esac

subject=${msg%%"
"*}
pr=$(printf '%s' "$subject" | sed -n 's/.*(#\([0-9][0-9]*\))[[:space:]]*$/\1/p')
[ -n "$pr" ] || printf_msg
command -v gh >/dev/null 2>&1 || printf_msg

# Bounded: a hung call must not stall changelog generation either. `timeout` is
# absent from a bare macOS, where the call simply runs unbounded.
bound=""
command -v timeout >/dev/null 2>&1 && bound="timeout 20"
body=$($bound gh api "repos/${GITHUB_REPOSITORY:-tenequm/pond}/pulls/$pr" -q .body 2>/dev/null) || printf_msg
[ -n "$body" ] || printf_msg

# From the `## Release note` header to the next `## ` header or end of body.
note=$(printf '%s\n' "$body" | awk '/^## Release note/{f=1; print; next} f && /^## /{exit} f{print}')
printf '%s' "$note" | tr -d '[:space:]' | grep -q . || printf_msg

printf '%s\n\n%s\n' "$msg" "$note"
