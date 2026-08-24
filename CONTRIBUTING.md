# Contributing to pond

Issues and pull requests are welcome. For anything larger than a bug fix, comment on the matching [roadmap](README.md#roadmap) issue first - or open one - so we agree on scope before you invest; adapters are the exception, see below. Repo conventions live in [AGENTS.md](AGENTS.md); the system contract is [docs/spec.md](docs/spec.md). Security issues go through [SECURITY.md](.github/SECURITY.md).

## Adding an adapter

The most wanted contribution. The full playbook is [`.agents/skills/add-adapter/SKILL.md`](.agents/skills/add-adapter/SKILL.md) - loadable as the `/add-adapter` skill in Claude Code, readable as a document by anyone. The short form:

- One self-contained PR: spec doc (`docs/adapters/<source_agent>.md` with the filled decision table), fixture, adapter, tests.
- The conformance fixture is a sandboxed self-capture of the agent (run it under a throwaway `HOME`), verified against [`packages/pond/tests/fixtures/README.md`](packages/pond/tests/fixtures/README.md). No vendored or real-home data.
- Recommended: open the PR as a draft after the spec doc, so the decision table gets reviewed before you implement.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green is the whole bar. No benchmarks: adapters are import-isolated from the store and query layer, and a guard test enforces it.

## Everything else

Run the same three commands before pushing. CI re-runs them on Linux, and natively on Windows for same-repo branches (a fork PR gets the Windows leg when a maintainer pushes the branch, or on merge).
