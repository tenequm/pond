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

<!-- Adapter PRs also carry this checklist. Uncomment it, keep it above the
     release note, and tick only what is actually done - each line is a
     playbook step the reviewer checks rather than trusts:

## Adapter checklist
- [ ] `docs/adapters/<name>.md`: 11-row decision table with evidence per row, a field-history section, and a `Last verified` line
- [ ] Fixture self-captured under a sandbox home (agent version stated); per-file row census in `tests/fixtures/README.md`
- [ ] JSONL sources go through `jsonl.rs` (`parse_bounded`, tail peek); the peek read-budget test is present
- [ ] `probe_default` reads only the injected `Env`; `assert_probe_default` test
- [ ] Conformance suite through the shared harness, plus taxonomy, lineage, and project assertions
- [ ] Run-as-a-user output pasted in `## Adapter evidence` above
- [ ] README harness row, `docs/site` roster, release note
-->

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
