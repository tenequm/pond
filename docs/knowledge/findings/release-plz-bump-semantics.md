---
type: Finding
title: A breaking marker only bumps the minor version when it rides a real-diff feat! or fix!
description: On v0.12.0 an empty BREAKING CHANGE commit and then a docs! commit both left the release a patch; release-plz derives the bump only from feat and fix commit types with a real diff.
tags: [release, release-plz, changelog, versioning]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: v0120
    resource: "pond v0.12.0 release, reconstructed from the repo's CHANGELOG.md and commit history"
    title: The v0.12.0 bump incident
  - id: release-plz-config
    resource: ../../../.github/release-plz.toml
    title: The repo's release-plz and git-cliff configuration
---

# A breaking marker only bumps the minor version when it rides a real-diff feat! or fix!

pond is a pre-1.0 release-plz-managed crate. release-plz derives the version
bump ONLY from `feat` and `fix` commit *types*. A breaking marker therefore
earns a minor `0.X.0` bump **only when it rides a real-diff `feat!:` or `fix!:`
commit**.

A `!` or a `BREAKING CHANGE:` footer is **silently ignored** for the bump when
it sits on any other type (`docs!:`, `chore!:`, `refactor!:`, ...) or on an
empty (`--allow-empty`) commit. The release stays a patch, and empty commits
are dropped from the changelog entirely.[^release-plz-config]

## How this was learned

On v0.12.0, an empty `BREAKING CHANGE` commit was pushed first, and then a
`docs!:` commit. Both left the release a patch. Only a `feat!:` carrying a real
diff bumped the minor.[^v0120]

## What to do about it

If breaking code has already merged under a non-breaking subject, add a NEW
real-diff `feat!:` or `fix!:` commit to carry the marker. Never force-push to
reword history.

The standing rules that follow from this - never pick a version, never open a
release PR by hand, merge the `chore: release` PR release-plz opens - live in
`AGENTS.md`, which is where an agent reads them every session. This concept is
the evidence behind them.

## Invalidation

This would change if release-plz altered which commit types feed its bump
derivation, or if the repo's `commit_parsers` configuration changed.

[^v0120]: pond v0.12.0, from CHANGELOG.md and commit history
[^release-plz-config]: `.github/release-plz.toml`
