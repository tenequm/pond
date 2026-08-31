<!-- PR title: conventional commit form `type(scope): description` (e.g.
     `feat(adapter): add letta-code`). It becomes the squash-commit subject and
     the changelog entry's headline. CI lints it. -->

## Summary

<!-- What and why, for the reviewer. -->

<!-- Adding an adapter? Add a `## Adapter evidence` section here (above the
     release note) with: the fixture's provenance - agent version, captured
     under a sandbox home, or why a capture was impossible - and the output of
     the playbook's run-it-as-a-user step: `pond sync <name> --path <fixture>`,
     the re-sync skipping every session fresh, a default-mode `pond search`
     hit, and `pond adapters list` showing the harness `detected` on a real
     install. Reviewers read it before the code. -->

<!-- The section below is extracted from the squash-commit body (the whole PR
     description lands there at merge, via the repo's squash defaults) and
     becomes the changelog entry under your title - written once, never lost.
     Write for pond users, not reviewers: ONE paragraph, max 300 characters,
     no bullets, no blank lines - CI enforces all three. One to three sentences
     on what changed for them and what they must do; ASCII only. CI comments a
     rendered preview on the PR.
     Required for feat/fix/perf PRs; leave empty for changes users don't see.
     A release-wide lead paragraph / **Upgrading:** block goes after a lone
     `[release-note]` line - it is hoisted to the top of the version entry.
     Keep comments like this one ABOVE the `## Release note` header, never
     inside the section: the changelog template cannot strip HTML comments, so
     CI rejects a section containing one. -->

## Release note
