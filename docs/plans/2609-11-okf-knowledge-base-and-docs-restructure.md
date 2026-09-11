# OKF knowledge base, benches consolidation, moonification (2026-09-11)

Status: plan locked in discussion 2026-09-11; branch `docs/use-okf-for-knowledge-base`
in a treehouse worktree (draft PR [#238](https://github.com/tenequm/pond/pull/238)). Driver: Claude Code session; bulk transformation fanned out
to agy subagents (see Execution). This file is the locked scope; where a workstream
below and the discussion transcript disagree, this file wins.

## Decisions

1. **AGENTS.md keeps its rationale blocks.** The measured "why" paragraphs are
   load-bearing guardrails (they exist because agents tried to "fix" the AIMD
   limiter, search scope, merge-insert); a one-line law with a link only protects
   agents that follow the link. The OKF instructions fence applies going forward,
   not retroactively. AGENTS.md additionally gains, at the top: anyone working on
   the repo MUST read `docs/spec.md` in full first, and `CONTRIBUTING.md` before
   producing a PR. The added per-session token cost is accepted.
2. **OKF v0.2 bundle at `docs/knowledge/`** (marker: `okf_version` in its
   `index.md` frontmatter), per the okf-project-knowledge-base skill: one concept
   per file, YAML frontmatter with `type`/`sources`/`generated`, index + log
   bookkeeping, deprecate-never-delete. The bundle holds what AGENTS.md does not:
   decisions and findings that are not standing orders.
3. **`docs/adapters/` migrates into the bundle** as `docs/knowledge/references/`,
   one Reference concept per source agent. Their ad-hoc "Last verified: <date>,
   against <version>" headers become `verified` events + `stale_after`.
4. **All benchmark material consolidates under `packages/pond/benches/`** (the
   cargo bench targets cannot leave it, so the data moves to the code):
   `docs/benchmarks/{results.md,bench-gate-baseline.jsonl,recall-context-cost.*}`
   and `ops/search-benchmarks/` move under `packages/pond/benches/docs/`.
   No symlink at the old path: `docs/benchmarks` is removed and every reference
   is rewritten to the real path. (A symlink was considered and dropped: GitHub's
   web UI 404s on deep links through a directory symlink, so references had to
   be rewritten anyway, and the leftover value - `ls docs/` muscle memory - is
   not worth a Windows checkout stub plus a second name for the same path. The
   `docs/site/public/spec.md` symlink is different in kind: a build input vocs
   consumes on Linux, not a navigation aid.)
5. **`ops/scripts/` dissolves into moon tasks.** moon v2 `script:` tasks are the
   orchestration layer (in-repo precedent: `build-dist`, `build-dist-msvc`); the
   pre-commit hook body moves into `vcs.hooks` as `moon run` lines. Two files stay
   files by nature, not laziness (see workstream E).
6. **moon version becomes repo-owned.** Latest upstream is v2.5.4 (2026-09-03);
   this box runs 2.5.1 (Home Manager nix store), CI's pin lives in the pond-ci
   runner image, and nothing in-tree names a version. Add `.prototools` with
   `moon = "2.5.4"`; `versionConstraint: '>=2.5.4'` in `.moon/workspace.yml`
   waited for the runner image and the operator's Home Manager deploy to reach
   2.5.4 (sequencing, or CI hard-fails on the old image); both were bumped and
   the constraint shipped in this change.
7. **Bundle invariants get mechanized as a moon task** (`repo:check-knowledge`),
   wired into the moon-managed pre-commit hook and CI. The OKF skill prescribes
   the checks, not the mechanism ("the checks, not the commands, are the
   contract"), so moon is the mechanism.
8. **`docs/plans/` stays outside the bundle.** Plans are coordination state (the
   durability fence excludes them); a completed plan's durable residue is captured
   as a Decision concept citing the plan. Backfilling concepts from the most
   consequential landed plans is an optional follow-up workstream.

## Workstreams

### A. AGENTS.md wiring (driver, by hand)

- Top-of-file mandate: read `docs/spec.md` in full before working on the repo;
  `CONTRIBUTING.md` before producing a PR (strong form, decision 1).
- Bundle pointer line: location `docs/knowledge/`, load the
  okf-project-knowledge-base skill before reading or writing it, end-of-task
  capture review.
- Update AGENTS.md self-references made stale by other workstreams:
  `moon run bench-gate` -> `moon run pond:bench-gate`;
  `docs/benchmarks/results.md` -> real path under `packages/pond/benches/docs/`;
  the Process section's hook description (`.github/hooks/pre-commit` no longer
  exists; the gate is `vcs.hooks` -> moon tasks).

### B. Bundle cold start (driver, by hand)

- `docs/knowledge/index.md`: `okf_version: "0.2"` frontmatter; preamble states
  what the bundle holds and refuses (plans and benchmark logs stay out; AGENTS.md
  carries laws and links here; publicity fence; type vocabulary: Decision,
  Finding, Reference, Runbook; `adapters/` subdir convention).
- `docs/knowledge/log.md` (newest-first `## YYYY-MM-DD` headings).
- First real concept written by the driver, full frontmatter and real sources -
  it is the convention every later writer imitates (invariant 2). Candidate:
  the search-scope decision or the token-usage-accounting runbook.

### C. Adapters migration (agy)

- Move `docs/adapters/{agy,grok-build,letta-code}.md` to
  `docs/knowledge/references/`, adding frontmatter: `type: Reference`,
  `description`, `tags`, `sources` (upstream repos, descriptors as honest scope
  descriptors, issue links), `verified` from the "Last verified" lines,
  `stale_after` pegged to the next plausible upstream release. Bodies unchanged.
- Update every referencing surface, in the same change: `CONTRIBUTING.md`,
  `.agents/skills/add-adapter/SKILL.md` (3 places; new-adapter spec docs are
  born as concepts from now on), `.github/pull_request_template.md`,
  `docs/spec.md`, `packages/pond/tests/fixtures/README.md`, doc comments in
  `packages/pond/src/adapter/{agy,grok_build,letta_code}.rs`. CHANGELOG stays.
- Index bullets + log entry.

### D. Benchmarks consolidation (agy)

- `git mv` the four `docs/benchmarks/` files and `ops/search-benchmarks/` under
  `packages/pond/benches/docs/`; `docs/benchmarks` is gone afterwards (decision
  4: no symlink).
- Update every path reference: AGENTS.md, README.md, root `moon.yml` comments,
  the bench-gate script's `BASELINE=` and header comment.
- **fileGroup restructure (load-bearing):** bench CODE and bench DATA are
  different inputs. Today `benches/**/*` rides in the `tests` fileGroups of
  both root `moon.yml` and `packages/pond/moon.yml`, so the moved
  append-on-every-run jsonl would invalidate format/lint/test caches. Fix by
  splitting, not negating: a new `benches` fileGroup holding code only
  (`benches/**/*.rs`; root: `packages/pond/benches/**/*.rs`), attached to the
  tasks that actually read bench sources (`format` formats them, `lint-msvc`
  compiles them via `--all-targets`, `test`/`lint` keep them under the repo's
  stated completeness rule); `tests` shrinks back to `tests/**/*`. The data
  under `benches/docs/` belongs to NO group, so bench-gate appends never touch
  a task fingerprint.
- **Packaging guard:** `benches/` ships in the crates.io package by default;
  add `exclude` entries for `benches/docs/` in `packages/pond/Cargo.toml`, then
  prove it with `moon run pond:check-package` and the new `pond:check-publish`.

### E. Moonification sweep (driver - touches release machinery)

| Today | Becomes |
|---|---|
| `ops/scripts/bench-gate.sh` (240 ln) | inline `script:` on `pond:bench-gate` (task moves from root to the pond project; script already cd's to repo top, so cwd is a non-issue); `cache: false`, `outputStyle: stream` kept |
| `ops/scripts/check-changelog-headers.sh` (71 ln) | inline `script:` on `repo:check-changelog`; inputs unchanged |
| `ops/scripts/check-package-contents.sh` (29 ln) | inlined into `pond:check-package` |
| `ops/scripts/publish-npm.sh` (17 ln) | inline `script:` on `repo:publish-npm`; `ci.yml:767` becomes `moon run repo:publish-npm` |
| `.github/hooks/pre-commit` | deleted; `vcs.hooks.pre-commit` lists `moon run repo:secret-scan repo:check-changelog repo:check-knowledge` directly |
| (new) | `repo:secret-scan`: gitleaks over staged changes, `cache: false` (reads the git index); keep the current graceful skip-with-warning when gitleaks is absent, and the friendly real-secret/false-positive messaging |
| (new) | `pond:check-publish`: `cargo publish --dry-run --locked`, on-demand (the full verify build that `publish_no_verify` skips at release time) |

- The hand-rolled "only when staged" guards dissolve: `check-changelog` and
  `check-knowledge` declare inputs, so an untouched input is a cache replay.
  `check-knowledge` therefore checks whole-bundle consistency idempotently, not
  a staged diff.
- Windows win: moon generates `.ps1` hooks there; `moon run ...` lines are
  portable where `./.github/hooks/pre-commit` (bash) was not.
- Stays a file, on purpose: `release-note-from-pr.sh` is a git-cliff
  `replace_command` filter release-plz pipes commit messages through
  (`.github/release-plz.toml:110`) - moon is not in that call chain; relocate to
  `.github/scripts/` beside its config (update release-plz.toml and the
  `release-note.yml` comment). `patch-macos-sdk.py` is a Mach-O byte-patching
  program invoked BY the `build-dist` task - the correct relationship already;
  relocate alongside (`.github/scripts/` or leave; update the task's path and
  `inputs`). `ops/scripts/` is then deleted.
- `.prototools` with `moon = "2.5.4"` and the `versionConstraint` line both
  landed; the version is now derived from `.prototools` by the Linux bootstrap,
  the Windows bootstrap and the npm-publish job rather than retyped.
- Accepted cost: inline scripts lose shellcheck/highlighting; gained: the script
  text is part of the task hash.

### F. Loose docs + research distillation (agy)

- `docs/other/token-usage-accounting.md` -> Runbook concept (the dedup rule is
  the finding). `docs/other/fable-5-vs-5-1-usage-queries.md` -> Finding with
  `stale_after`. `docs/check-later/` retrieval-redesign research -> concept with
  honest superseded status, or folded into the search-scope decision's history;
  both source dirs end empty and are removed.
- `docs/researches/` reports stay in place as cited sources; each is distilled
  into a Finding concept (claim + what invalidates it + sources). No bulk moves.

### G. check-knowledge validator (agy, driver-reviewed)

- `repo:check-knowledge` inline script (python3 heredoc): for every
  `docs/knowledge/**` concept - frontmatter parses as YAML, `type` non-empty,
  the concept sits in the directory its type names, bundle-relative links
  resolve, index.md lists exactly the concept files on disk, log.md exists.
  Graceful skip-with-warning if python3/yaml is missing (mirroring the gitleaks
  precedent); CI is the hard gate, and the CI bootstrap installs PyYAML so that
  is true rather than aspirational.
- Known scope limit: link checking covers only links inside the bundle. A path
  naming a concept from elsewhere in the repo is invisible to this gate, so a
  rename has to sweep the repo by hand.
- Wire into the `ci.yml:66` task list (`repo:check-knowledge` joins
  `repo:check-changelog ...`) and the hook line from workstream E.

## Execution: agy fleet via herdr

- Worktree: treehouse slot on branch `docs/use-okf-for-knowledge-base`; only the
  driver runs git there (no index.lock races, clean authorship). Slot is
  returned via `treehouse return` after push.
- Driver does A, B, E by hand; C, D, F, G fan out to agy agents,
  `--model gemini-3.7-flash-medium --effort medium --mode accept-edits`, one
  herdr TAB per agent in the current workspace (never N panes in one tab),
  `--no-focus`.
- Pre-flight per herdr-faq: write the worktree path into
  `~/.gemini/trustedFolders.json` BEFORE `agent start` (the trust dialog is
  unanswerable after); read the startup banner via
  `agent read --source detection` and verify entitlement before prompting.
- Briefs are files in per-agent dirs outside the worktree (under /home; /tmp
  dies on VM reboot); briefs pre-decide the fences (exact source section ->
  exact target concept file, type, sources) so agents transform, not judge.
- Agents write concept files in-tree but index/log entries to per-agent
  fragment files; the driver consolidates index.md/log.md (three agents merging
  the same two files is the one guaranteed conflict).
- Every brief ends "write your full report to <path>, reply with only the
  path"; the driver waits on the file, never on agent state (agy has no idle
  detection rule, reports premature done, can swallow a first prompt -
  re-prompt on `agent_prompt_stalled`, never send-keys).
- Workstreams C/D/F/G have disjoint file ownership; byte-identical output
  across agents is agy's known copying quirk, not agreement.

## Verification (before PR)

- `moon run repo:check-changelog pond:check-package pond:format pond:lint
  pond:test repo:check-knowledge` green; `pond:check-publish` green after D.
- OKF invariant 7 on the bundle: frontmatter parses, types non-empty, links
  resolve, index matches directory (i.e. `repo:check-knowledge` itself).
- Path sweep: `rg -n 'ops/scripts|docs/benchmarks|docs/adapters'` finds only
  deliberate survivors (CHANGELOG history, this plans dir,
  `packages/pond/benches/docs/results.md` as historical record of past benchmark
  runs).
- Driver reviews every agent diff before commit; one PR,
  `docs(knowledge): adopt OKF bundle and consolidate benches + moon tasks`
  (or split D+E into a second PR if review size demands).

## Out of scope / follow-ups

Both follow-ups below were pulled INTO this PR on operator instruction, which
overrides decision 1 on the second one:

- Plans backfill - done for the landed plans carrying a durable decision not
  already recorded elsewhere: embeddings opt-in, the Windows msvc port, and the
  CI compiler-cache architecture. The rest needed no concept, either because
  their outcome is the code (most implementation plans), because an existing
  concept already covers them (the tools redesign, OpenClaw), or because they
  are still state rather than knowledge (designs not yet implemented, such as
  scoped access and the pond_get three-mode redesign).
- AGENTS.md narrative histories moved into concepts, which is what the bundle's
  own instructions fence asks for: AGENTS.md keeps the one-line law, the
  concept carries the measurement and the incident. Five object-store rules,
  the bench-gate evidence standards and the release-plz v0.12.0 story moved.

The bundle was also refiled from flat into per-type directories (`decisions/`,
`findings/`, `references/`, `runbooks/`) in the same change, since it roughly
doubled in size. Filing by type rather than by subject keeps the directory
derivable from the `type:` field instead of a judgment call, keeps the set
closed as pond grows, and is enforced by `repo:check-knowledge`; subject
grouping lives in the index, which is the read entry point anyway.
