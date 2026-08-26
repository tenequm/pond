<!-- PR title: conventional commit form `type(scope): description` (e.g.
     `feat(adapter): add letta-code`). It becomes the squash-commit subject and
     the changelog entry's headline. CI lints it. -->

## Summary

<!-- What and why, for the reviewer. -->

<!-- The section below is extracted from the squash-commit body (the whole PR
     description lands there at merge, via the repo's squash defaults) and
     becomes the changelog entry under your title - written once, never lost.
     Write for pond users, not reviewers: markdown `- ` sub-bullets, key measured
     numbers, ASCII only. CI comments a rendered preview on the PR.
     Required for feat/fix/perf PRs; leave empty for changes users don't see.
     A release-wide lead paragraph / **Upgrading:** block goes after a lone
     `[release-note]` line - it is hoisted to the top of the version entry.
     Keep comments like this one ABOVE the `## Release note` header, never
     inside the section: the changelog template cannot strip HTML comments, so
     CI rejects a section containing one. -->

## Release note
