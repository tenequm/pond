# Contributing to pond

Issues and pull requests are welcome. For anything larger than a bug fix, comment on the matching [roadmap](README.md#roadmap) issue first - or open one - so we agree on scope before you invest; adapters are the exception, see below. Repo conventions live in [AGENTS.md](AGENTS.md); the system contract is [docs/spec.md](docs/spec.md). Security issues go through [SECURITY.md](.github/SECURITY.md).

## Adding an adapter

The most wanted contribution. The full playbook is [`.agents/skills/add-adapter/SKILL.md`](.agents/skills/add-adapter/SKILL.md) - loadable as the `/add-adapter` skill in Claude Code, readable as a document by anyone. The short form:

- One self-contained PR: spec doc (`docs/adapters/<source_agent>.md` with the filled decision table), fixture, adapter, tests.
- The conformance fixture is a sandboxed self-capture of the agent (run it under a throwaway `HOME`), verified against [`packages/pond/tests/fixtures/README.md`](packages/pond/tests/fixtures/README.md). No vendored or real-home data.
- Recommended: open the PR as a draft after the spec doc, so the decision table gets reviewed before you implement.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green is the whole bar. No benchmarks: adapters are import-isolated from the store and query layer, and a guard test enforces it.
- The PR shows the evidence, not only the code. Its `## Adapter evidence` section (the template prompts for it) carries the fixture's provenance - agent version, captured under a sandbox home, or why a capture was impossible - and the output of the playbook's run-it-as-a-user step: `pond sync <name> --path <fixture>`, the re-sync skipping every session fresh, a default-mode `pond search` hit, and `pond adapters list` showing the harness `detected` on a real install. Reviewers read that section before the code; a PR without it goes back for it.
- Agent-authored PRs are welcome on the same bar. The playbook is written to be loaded by an agent, and the steps that need a real install and a real run cannot be reasoned around. Keep a human reachable on the PR to answer review.

## Release notes

pond's changelog is generated from squash-commit messages, so what you write on the PR is what ships - there is no separate release-notes pass later.

- **PR title** must be a conventional commit (`type(scope): description`, e.g. `fix(sync): stop dropping resumed folds`). It becomes the squash-commit subject and the changelog bullet. CI lints it.
- **`## Release note` section** in the PR description (the template prompts for it) becomes the prose under that bullet. Write it for pond users rather than reviewers: **one paragraph, at most 300 characters, no bullets and no blank lines** - CI enforces all three, plus that it is the **last** section of the PR body. One to three plain sentences: what changed for the user, and what they must do about it. Short sentences, active voice, present tense, the same word for the same thing. Say the effect, not the work you did or how you validated it - that belongs in the sections above, which never reach the changelog. Required for `feat`/`fix`/`perf` PRs; leave it empty when users see nothing. A release-wide story or an `**Upgrading:**` block goes after a lone `[release-note]` line, which is exempt from the cap.
- CI comments only when something is wrong, listing every problem at once, and clears the comment once the description passes.
- If a note turns out wrong after merge, edit the PR description - the changelog is regenerated from it on the next release-PR refresh.

## Everything else

Run the same three commands before pushing. CI re-runs them on Linux, and natively on Windows for same-repo branches (a fork PR gets the Windows leg when a maintainer pushes the branch, or on merge).
