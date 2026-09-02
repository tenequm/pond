# Changelog

## [0.17.1](https://github.com/tenequm/pond/compare/v0.17.0...v0.17.1) - 2026-09-02

### <!-- 5 -->📚 Documentation
- **readme:** add the vote-gated format-contract row to the roadmap ([e3073cb](https://github.com/tenequm/pond/commit/e3073cb40186848568ffd239fa2b27df07526d4a))
- **readme:** roadmap after v0.17.0 - step 9 shipped, namespaces in progress, herdr plugin #219, Antigravity CLI replaces Gemini CLI ([ac08614](https://github.com/tenequm/pond/commit/ac0861454424937ce0b78ae1796a44e802d749d1))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.17.0...v0.17.1

## [0.17.0](https://github.com/tenequm/pond/compare/v0.16.3...v0.17.0) - 2026-09-01

### <!-- 0 -->🛠 Breaking Changes
- [**breaking**] upgrade Lance 10.0.0 -> 11.0.0 with FTS stemmer self-heal ([#214](https://github.com/tenequm/pond/pull/214)) ([0ab11c7](https://github.com/tenequm/pond/commit/0ab11c72ec22e93cda7fab7b467eecfa7a698051))
  Lance 10 -> 11. Its English stemmer changed, so a text index built by an
  older pond misses some word forms; pond now rebuilds it once on the next
  sync or `pond optimize` (~10 min on a remote store) and `pond status`
  shows `stemmer outdated` until then. Upgrade every host sharing a store
  first.

### <!-- 2 -->🐛 Bug Fixes
- **codex-cli:** decode Codex 0.151+ JS-runtime tool calls ([#218](https://github.com/tenequm/pond/pull/218)) ([94ab1e3](https://github.com/tenequm/pond/commit/94ab1e3c5a28db31c2d7533bb82112afe5f28a7a))
  codex-cli now decodes Codex JS-runtime tool calls (seen from 0.147 on):
  tool_name is the wrapped tool instead of exec, is_failure is set when
  the script or any command it ran failed, and params carry the executed
  command and cwd. Native restore uses codex's local-time filename.
- **serve:** stop cleanly when a supervisor asks ([#198](https://github.com/tenequm/pond/pull/198)) ([98df4a1](https://github.com/tenequm/pond/commit/98df4a1388ea537cd6d3092415496b693f8071ef))
  `pond serve` now stops when a supervisor asks it to. It handles SIGTERM
  as well
  as ctrl-c, and shutdown no longer hangs when an agent is connected: live
  MCP
  sessions are closed and the drain is bounded, so restarts finish instead
  of
  waiting for the process to be killed.
- **serve:** let /mcp answer on the deployment's own hostname ([#196](https://github.com/tenequm/pond/pull/196)) ([1355a3e](https://github.com/tenequm/pond/commit/1355a3e54371cccfd5c9f93a03bcb1585660f01f))
  `pond serve` reached by a hostname other than localhost now answers MCP
  once
  you name that host with `--allowed-host` (or `POND_ALLOWED_HOSTS`);
  without it
  the `/mcp` route refuses every request with 403 while `/v1/*` keeps
  working.
  Local use is unchanged.

### <!-- 5 -->📚 Documentation
- **readme:** add the community-adapters row to the roadmap ([#211](https://github.com/tenequm/pond/pull/211)) ([e99d352](https://github.com/tenequm/pond/commit/e99d3520be8ec0489ceab5b3c05ec788de259f04))
- harden the Windows install path from live fresh-eyes verification ([#191](https://github.com/tenequm/pond/pull/191)) ([a7d1b5c](https://github.com/tenequm/pond/commit/a7d1b5c86ed6727484e2e85d822d27a84848ac92))
  Windows install docs now name the git prerequisite for `scoop bucket add` and the `core.longpaths` and `--target` steps a source build needs. Installed via `cargo binstall`? You have `pond.exe` only - use Scoop or the zip for scheduled sync.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.16.3...v0.17.0

## [0.16.3](https://github.com/tenequm/pond/compare/v0.16.2...v0.16.3) - 2026-08-26

### <!-- 2 -->🐛 Bug Fixes
- **init:** Windows onboarding - help defaults, non-TTY output, store dir, restart hint, docs overhaul ([#189](https://github.com/tenequm/pond/pull/189)) ([162a925](https://github.com/tenequm/pond/commit/162a925f47627923a6b3dae67906a6c56995c92b))
  - `pond --help` and `pond init` now show Windows users real Windows
  paths (`%APPDATA%\pond\config.toml`, `%LOCALAPPDATA%\pond\data`), init
  renders cleanly for agents and ssh sessions, creates the store dir it
  announces, and reminds you to restart your client after MCP/skill
  changes.
  - Docs: the release zip is a first-class install path, Scoop setup is
  fully documented, the agent setup prompt works on Windows, and a new
  [Troubleshooting](https://pond.locker/guides/troubleshooting) guide
  covers the common traps.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.16.2...v0.16.3

## [0.16.2](https://github.com/tenequm/pond/compare/v0.16.1...v0.16.2) - 2026-08-26

A Windows schedule that can never fire is now diagnosed instead of silently reported healthy. Both failure modes in this release were found live on a real Windows machine after a winget-to-Scoop migration: the scheduled task's action still pointed at the uninstalled binaries (every 5-minute tick died with 0x80070002 FILE_NOT_FOUND while `pond schedule status` said `active`), and the task itself had been registered from an elevated shell, so `pond schedule start`/`stop` from a normal shell failed with a bare `Access is denied`. Alongside the fix, Scoop becomes the working Windows install channel: the bucket now genuinely self-updates from each release (its checkver had been broken since creation), each release pings it so new versions land within minutes, and winget leaves the docs until [winget-pkgs#419055](https://github.com/microsoft/winget-pkgs/pull/419055) merges.

### <!-- 2 -->🐛 Bug Fixes
- **schedule:** detect broken Task Scheduler registrations and name the elevated-ownership fix ([8945816](https://github.com/tenequm/pond/commit/89458160a6d7b6fbb73adbb520d08183dc3985e1))
  - A registration whose launcher no longer exists (uninstall, package-manager switch) renders as `schedule  broken (task-scheduler, every 5m) - the registered launcher no longer exists (...); run pond schedule start to re-register`, in `pond status` text and as `schedule.problem` in `--format json`. The next-run estimate, the `(pond schedule logs)` pointer, and the "scheduled sync hasn't completed yet" hint are suppressed for it - that launcher never runs, so the log it points at stays empty. Gated once at derivation so text and JSON agree; exit codes unchanged (broken still exits 0, documented).
  - A task registered from an elevated shell is owned by Administrators and untouchable from normal shells. `schtasks /Create`/`/Delete` failures over an existing task now explain the trap and name the recovery - `pond schedule stop` from an elevated shell - checked on task existence, not stderr text, because schtasks messages are localized. `pond schedule start` warns before registering from an elevated shell, after the already-scheduled no-op return, so only a run that actually registers sets off the note.
  - `pondw_bin` documents why the `current_exe` fallback is the designed Scoop path - the shim spawns the real exe as a child and `GetModuleFileNameW` preserves the stable `current` junction (measured on a real box 2026-08-26) - and must never canonicalize, which would pin the task to a versioned dir `scoop cleanup` deletes.
  - Validated twice on Windows: the full suite in CI, and the schedule tests plus clippy on a physical Windows 11 machine where the originating incident was reproduced and repaired end to end (elevated delete, unelevated re-register, manual and unattended ticks both `Ready / 0`).

### <!-- 5 -->📚 Documentation
- remove winget from install docs until the winget-pkgs review merges ([dc393e3](https://github.com/tenequm/pond/commit/dc393e3bea2f17e114fdb2dba2fc1aeea953b805))
  - `winget install tenequm.pond` led the Windows install docs while resolving to "No package found" - the manifest is still in review upstream. Scoop is now the documented channel; the `winget install Google.Protobuf` / `NASM.NASM` build prerequisites stay, since those packages resolve fine. winget returns to the docs when the review merges (the CI publish job self-activates the moment the package exists upstream).
  - Off-repo but part of the same story: the scoop-bucket's `checkver` pointed at the homepage instead of the GitHub repo, so its 4-hourly autoupdate had 404ed on every run since creation, pinning the bucket at 0.14.10 - the pre-NTFS-durability Windows build. Fixed and bumped to 0.16.1 in [tenequm/scoop-bucket](https://github.com/tenequm/scoop-bucket), and each pond release now dispatches the bucket's updater directly ([e623cc8](https://github.com/tenequm/pond/commit/e623cc8)), cutting Scoop lag from up to 4h to minutes.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.16.1...v0.16.2

## [0.16.1](https://github.com/tenequm/pond/compare/v0.16.0...v0.16.1) - 2026-08-25

Date-filtered search stays correct across compaction. 0.16.0's timestamp zonemap indexes rows by *address* (fragment id plus offset), and Lance never remaps those addresses when compaction rewrites a fragment - it refreshes the index's fragment list while the index body keeps pointing at fragments that no longer exist. Every pond version's compaction does this, 0.16.0's own included. The result was a date-filtered search that either failed outright (`storage_unavailable`, naming a missing fragment) or, once a later index fold cleaned up the dangling references, silently dropped the rewritten rows from its results. Ordinary search, `pond get-session`, `pond get-message`, and `pond sql` were never affected, and no data was ever at risk - the index went stale, the store did not.

pond now checks the index against the store's live fragments at the end of every maintenance run and rebuilds it when they disagree. Because the check runs immediately after compaction, a machine repairs its own damage within the same `pond sync` or `pond optimize`, and picks up another machine's on the next one. The rebuild only fires when compaction actually rewrote indexed fragments.

**Upgrading:** just upgrade - a store that is already broken is repaired by the first `pond sync` or `pond optimize` a 0.16.1 binary runs, with no manual step (`pond optimize --rebuild` still works if you want it now). This is the version that ends the cycle: until every machine writing to a shared store is on 0.16.1, any older binary's compaction can break date filters again, so upgrade the writers, not just the readers.

### <!-- 1 -->🎉 New Features
- bench-gate measures writes and stamps the binary, one row per run ([#183](https://github.com/tenequm/pond/pull/183)) ([ec6dad3](https://github.com/tenequm/pond/commit/ec6dad321c062198ffcebd0bd605305319546bd5))

### <!-- 2 -->🐛 Bug Fixes
- self-heal the timestamp zonemap after compaction orphans its fragment references ([#185](https://github.com/tenequm/pond/pull/185)) ([eeb2676](https://github.com/tenequm/pond/commit/eeb267667529e31548782889a2338411243a1c5e))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.16.0...v0.16.1

## [0.16.0](https://github.com/tenequm/pond/compare/v0.15.1...v0.16.0) - 2026-08-25

Date-scoped search stops full-scanning. The storage engine moves to Lance 10.0.0, and `messages` gains a `timestamp` zonemap that prunes date-bounded queries at the index instead of the table: on a 2.9M-row remote store a served `--from-date` search went from ~2 minutes to 5-6 s (~20x), and the date filter now costs nothing over an unfiltered search instead of +25 s. COUNT pushdown fires under stable row ids too (`pond sql` count 2.1 s -> 1.3 s). Writes are unchanged: a matched-store A/B measured sync at parity on both cold and warm runs.

**Upgrading:** nothing manual, but this one is a fleet decision. The zonemap is created on the first `pond sync` or `pond optimize` a 0.16.0 binary runs, and pond 0.15.1 and earlier consume that index in the wrong id domain - so an older binary reading the same store returns **zero results for date-filtered searches**, with no error and no warning (`--from-date` / `--to-date` on the CLI, `from_date` / `to_date` over MCP). Unfiltered search, `pond get-session`, `pond get-message`, and `pond sql` are unaffected, and the store's data is never damaged: an old reader that upgrades is immediately correct again. **Writers** are the dangerous case (correction post-release): compaction orphans the zonemap whenever it rewrites fragments the index covers - every version's compaction does this, 0.16.0's own included - after which date-filtered searches on 0.16.0 fail with a `storage_unavailable` error until `pond optimize --rebuild` recreates the index. 0.16.1 adds the repair: every `pond sync`/`pond optimize` detects the stale index right after compaction and recreates it in the same run. If several machines share one store, upgrade every machine that reads it, not just the ones that write it, and stop the schedule on machines you cannot upgrade yet.

### <!-- 0 -->🛠 Breaking Changes
- [**breaking**] stores synced by 0.16.0 carry a timestamp zonemap that pond <= 0.15.1 reads as an empty date filter ([361abed](https://github.com/tenequm/pond/commit/361abed03ef174c1a742d92000bebbf741babccc))

### <!-- 1 -->🎉 New Features
- upgrade Lance to 10.0.0 and prune date-scoped search with a timestamp zonemap ([#182](https://github.com/tenequm/pond/pull/182)) ([232993b](https://github.com/tenequm/pond/commit/232993b11ffde67851add245a2fba441c4f19169))

### <!-- 5 -->📚 Documentation
- **readme:** mark roadmap step 8 shipped in v0.15.1 ([c80b15c](https://github.com/tenequm/pond/commit/c80b15cce12deb666f08ed1df12536a3fdae5a4d))

### <!-- 6 -->🧹 Chores
- **embed:** drive idle eviction on tokio's virtual clock ([ce7138c](https://github.com/tenequm/pond/commit/ce7138c9e7800f0f9ad519cfbca0aaa362097407))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.15.1...v0.16.0

## [0.15.1](https://github.com/tenequm/pond/compare/v0.15.0...v0.15.1) - 2026-08-24

Two new harnesses, and the machinery that made them cheap to add. pond now ingests **letta-code** and **grok-build** (xAI's `grok` CLI) sessions, bringing the registry to twelve harnesses, eleven of them auto-discovered. Both adapters were requested by [Kyle Little](https://github.com/klittle32) (#170, #171), who runs letta as the orchestrator over codex, pi, claude-code, omp and grok workers, and were the first picked by the roadmap's reaction ordering. Both were built end to end from a new `add-adapter` playbook and a shared conformance harness - the second one needed zero edits to either, which was the bar the playbook had to meet. Every adapter now carries a `docs/adapters/<name>.md` spec with an 11-row decision table and a `Last verified` date against a named upstream version, so "does pond still read this format?" has a written answer instead of a guess.

**Upgrading:** the new adapters are registered but not enabled on an existing install - `pond sync` never enables anything as a side effect. Run `pond adapters discover` (or re-run `pond init`) once; it finds `~/.letta/transcripts` and `~/.grok/sessions` and adds them to your config. A relocated `LETTA_TRANSCRIPT_ROOT` or `GROK_HOME` is configured as an explicit `path`.

### <!-- 1 -->🎉 New Features
- **adapter:** add grok-build ([#179](https://github.com/tenequm/pond/pull/179)) ([28bc262](https://github.com/tenequm/pond/commit/28bc26244bb06f61b9a9a63a3a9e73274fdc21df))
  - Reads `~/.grok/sessions/<encoded-cwd>/<session-uuid>/updates.jsonl` (`GROK_HOME` honored). Project resolves from `summary.json`'s recorded cwd, then the bucket's `.cwd` sidecar, then the decoded bucket name - the three places grok writes it, in the order they are trustworthy. Tool calls become an assistant call plus a tool result under one `call_id`, with both failure modes and an interrupted call covered by the fixture; plans, hook events, image blocks and every `x.ai/*` extension ride the lossless carrier rather than being dropped.
  - Subagent lineage: grok records the parent->child link only parent-side (`subagents/<child>/meta.json`), so the adapter builds the map once and stores each child's meta in its own session - a child restored on its own re-emits the sidecar, which is what makes the per-session round trip exact. Forks, `/rewind` and compaction are all appends in 1.0.x (measured: the pre-rewind file is a byte prefix of the post-rewind file), so freshness is a bounded tail peek.
  - Fixture: 15 sessions self-captured in a sandbox on macOS and on Windows 11, model `grok-4.6`, swept clean. Verified against grok-build 1.0.5 and the xai-org/grok-build v1.0.6 source snapshot.
- **adapter:** add letta-code ([#177](https://github.com/tenequm/pond/pull/177)) ([29aadc4](https://github.com/tenequm/pond/commit/29aadc4d1c3390c9796516ff341d253cad028c65))
  - Reads the `letta` CLI's client-side transcripts at `~/.letta/transcripts/<agent>/<conversation>/transcript.jsonl` (`LETTA_TRANSCRIPT_ROOT` honored). Session identity is the path and the project is the agent id; message ids are position-derived because letta's own `letta-msg-<n>` ids are process-scoped and repeat inside one conversation. A `tool_call` row that carries result fields yields both the call and its result under the same `call_id`.
  - Native restore replays every captured record byte-for-byte into the source layout; foreign restore reconstructs under letta's own sanitized alphabet. Verified against letta-code 0.30.30.
- make adapter addition routine - add-adapter skill, conformance harness, contributor flow ([#173](https://github.com/tenequm/pond/pull/173)) ([295c6cc](https://github.com/tenequm/pond/commit/295c6cc02bdc8a0094a647661691135d7abf1e24))
  - `/add-adapter` (`.agents/skills/add-adapter/SKILL.md`) is a two-phase playbook: spec the format from the upstream *writer* code and capture a sandboxed self-capture fixture, then implement from the spec. Its centerpiece is an 11-row decision table (identity, project, ordering, tool correlation, provenance, lineage, non-capture, restore face, freshness oracle, Windows) with each row tied to the spec rule that makes it binding.
  - One conformance harness for every adapter: clean ingest counts, every searchable row inside the brand scope, re-sync is a no-op (each session skipped fresh, by id), and a round trip in the mode the adapter declares - with a first-difference diff on failure. Retrofitted onto oh-my-pi, opencode, claude-ai-export, claude-code and pi-coding-agent, which surfaced that pi honestly reports `Foreign` for v4/SQLite-origin sessions no released pi can read; that is now a declared downgrade rather than a silent pass.
  - A guard test keeps `src/adapter/` import-isolated from the store's write path, commit discipline and query plans, which is why "fmt, clippy `--all-targets`, `cargo test` green" is the whole bar for an adapter PR. `CONTRIBUTING.md`, the README's Supported harnesses table with `Last verified` dates, and spec 6.9 (format archaeology lives in `docs/adapters/`) round out the contributor flow.

### <!-- 5 -->📚 Documentation
- **readme:** name grok-build in the auto-discovered harness list ([8c532da](https://github.com/tenequm/pond/commit/8c532da4497ca781a95ce277fe67a3a769c8f3a9))

### <!-- 6 -->🧹 Chores
- kache caching architecture (per-job k27 prefixes, bulk pull warm-up, kache 0.15.0) ([#176](https://github.com/tenequm/pond/pull/176)) ([dd51e9d](https://github.com/tenequm/pond/commit/dd51e9ddb2d3c12118800a3cbd287e66d69eb8fe))
  - The hosted Windows gate drops from 17.8-19.5 min to 12.4 min (9m 6s of moon time) at a 99.6% hit rate with zero per-hit S3 round trips: each job now has its own key-schema-tagged prefix, bulk-pulled in one 45 s pass before cargo starts, so the 287 MB `lance` unit that used to time out kache's 3 s demand ceiling and recompile on every run is restored instead. The remote lives in committed `.github/kache/*.toml` files selected by `KACHE_CONFIG` and asserted at bootstrap, which is what let both legs move to kache 0.15.0 and its under-keying fix. Windows release builds go from ~64 min cold to ~20 min warm. The plan with every measurement is `docs/plans/2608-24-ci-caching-architecture.md`.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.15.0...v0.15.1

## [0.15.0](https://github.com/tenequm/pond/compare/v0.14.11...v0.15.0) - 2026-08-22

Embeddings are opt-in. Full-text search (BM25) is now the default arm everywhere, and a fresh install no longer downloads a 466 MB embedding model or embeds anything at sync - first import of an 11k-session corpus dropped from 113m56s to 39m22s (2.89x) in the release A/B against a real S3 store. A 63-day usage-trace evaluation (1,126 real searches, `docs/researches/`) found fts matches or beats vector quality on this workload, which is why the default flipped rather than just the cost.

**Upgrading:** if you used semantic search and want to keep it, set `[embeddings].enabled = true` in config (or `POND_EMBEDDINGS_ENABLED=1`; boolean-ish `1/0/yes/no/true/false` all parse). Without it, searches run fts - degraded for meaning-style queries, never broken, and your existing embeddings and semantic index stay untouched on the store for the moment you opt back in. Then re-run `pond init` - it now detects your existing sync schedule and offers to re-register it, refreshing the unit after the upgrade (or run `pond schedule start`).

### <!-- 0 -->🛠 Breaking Changes
- **search:** fts is the default arm; embeddings opt-in via `[embeddings].enabled` ([a5b2779](https://github.com/tenequm/pond/commit/a5b277937de6cb3f3ea70a9b5c0d051b9c4c15c9))
  - An omitted `mode` now resolves to `fts` on every surface: CLI (`pond search`), MCP (`pond_search`), HTTP wire, and the openclaw/pi/hermes plugins. `mode=vector` on a disabled instance returns a typed refusal naming `embeddings.enabled` (`retryable: false`) instead of failing obscurely; the HTTP `min_score` parameter of an old client is silently ignored, not rejected.
  - Disabled instances skip the model download, the embed stage, and the vector index intent entirely; the MCP tool surface describes itself fts-only so agents are never routed to a refusal. Enabled instances behave exactly as before - the release A/B measured parity on every enabled-path read.
  - Reads got cheaper on the new default: `pond status -v` 79.1s -> 21.6s (3.67x) on the real 2.87M-message S3 store (embedding scans replaced by index-resident reads), `status` 1.18x, `search --mode fts` at parity (1.04 +/- 0.16). Baselines in `docs/benchmarks/results.md`.
- **status:** report embeddings_enabled in status --format json ([afbdd8e](https://github.com/tenequm/pond/commit/afbdd8eb50eb7c05ee77d2bc22a69288db2b4034))
  - Additive field so a consumer can tell which arm an omitted `mode` resolves to without reading config. (This commit also carries the release's breaking marker: the squash subject above used the malformed `feat!(scope):` form the conventional-commit parser ignores.)

### <!-- 1 -->🎉 New Features
- **init:** schedule repair - re-running `pond init` detects an active sync schedule and offers to re-register it, rewriting the unit with the current template (config-file pin, absolutized paths). The prompt defaults to yes with the current cadence preselected; declining leaves the unit byte-identical. Interactive only, so sandboxed `--yes` runs can never repoint a real unit. ([a5b2779](https://github.com/tenequm/pond/commit/a5b277937de6cb3f3ea70a9b5c0d051b9c4c15c9))
- **plugins:** openclaw-pond 0.2.0 and pi-pond 0.3.0 ship the version-neutral fts-default tool descriptions and now auto-publish to npm via trusted publishing (OIDC, no tokens) on each release. ([a5b2779](https://github.com/tenequm/pond/commit/a5b277937de6cb3f3ea70a9b5c0d051b9c4c15c9))

### <!-- 5 -->📚 Documentation
- **site:** add /compare page; link it from the README FAQ table ([e91fcec](https://github.com/tenequm/pond/commit/e91fcec25b9635f4ecb2d1eb0678b8564f6fdf7e))
- **readme:** add side-by-side table under the memory-tool FAQ ([e52a49d](https://github.com/tenequm/pond/commit/e52a49db8ead128f1de1aac7f5cfede2d2bacdab))
- **readme:** add the roadmap - shipped steps, what is next in order, and what is not planned ([d47ec3c](https://github.com/tenequm/pond/commit/d47ec3c2767309e4834485ba4342ddc046c76df4))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.11...v0.15.0

## [0.14.11](https://github.com/tenequm/pond/compare/v0.14.10...v0.14.11) - 2026-08-17

Finishes the Windows port 0.14.10 opened. Local-store durability now behaves the same on NTFS as it does on APFS and ext4, the background sync task no longer flashes a console window or hides its own failures, and pond installs from Scoop today.

**Upgrading on Windows:** run `pond schedule start` once. An existing scheduled task keeps its old `.cmd` + `.vbs` action chain until it is re-registered - nothing breaks if you skip this, but the console flash and the swallowed exit code stay.

### <!-- 1 -->🎉 New Features
- **windows:** flush published writes on local stores ([2ac28df](https://github.com/tenequm/pond/commit/2ac28df5d87582b548df7dd5ad5906d1c6c4c7bf))
  - The local-store durability wrapper was attached behind a `cfg(unix)` gate, so a Windows local store had none of it. NTFS journals metadata but not file data, and a local store commits by hard-link-and-rename, so a hard host stop could persist a manifest's *name* while dropping its *bytes*. The wrapper is now attached per backend (local vs remote) rather than per platform, and a unit test pins it there - a re-added `cfg(unix)` would silently un-enforce the rule, which is how the gap survived this long.
  - `sync_file_and_parent` skips only the directory half on Windows: `FlushFileBuffers` on a directory handle fails, and object_store, RocksDB, and SQLite all skip it for the same reason. The published file's bytes still go through `File::sync_all`. Ordering is unchanged, and the residual window no directory fsync can close on Windows stays covered by `local-store-self-heal`.
  - Measured free: under 1 ms per commit at every batch size, 0.8 ms (3.4%) at the largest, A/B'd on a real Windows box against the same build without the wrapper (`docs/benchmarks/results.md`).
- **windows:** Exec pondw.exe from the scheduled task ([55043fe](https://github.com/tenequm/pond/commit/55043fea28dac6ebc20bfc89e54651f63c0bf147))
  - The Task Scheduler action was a `.cmd` launching a `.vbs` to hide the console window, and the chain swallowed the sync's exit code - a failing sync reported success in Task Scheduler. It is now a single `pondw.exe`, a windowless launcher that propagates the real exit code.
  - Adds a global `--state-dir`, the argument form of `XDG_STATE_HOME`, because an Exec action carries no environment block. Gated behind a `windows-launcher` feature so unix `cargo binstall` is unaffected.
- **windows:** publish to winget and Scoop ([5c07c55](https://github.com/tenequm/pond/commit/5c07c55a3c3975fae9c72c95c8a0e5dce0ecc820))
  - Scoop is live: `scoop bucket add tenequm https://github.com/tenequm/scoop-bucket` then `scoop install pond`. The winget manifest is generated and submitted by CI, but `winget install` does not resolve until microsoft/winget-pkgs accepts the first submission - use Scoop until then. PowerShell completions and the Windows install docs ship here too.

### <!-- 5 -->📚 Documentation
- **readme:** above-the-fold rework with recall-context-cost chart ([#158](https://github.com/tenequm/pond/pull/158)) ([895a8d8](https://github.com/tenequm/pond/commit/895a8d87fe985095fcaac0f19e89fb49435ce334))

### <!-- 6 -->🧹 Chores
- **windows:** cache the Windows suite through moon and fix kache prefetch ([0ed4a0c](https://github.com/tenequm/pond/commit/0ed4a0c008c2c43c25ae019913081d59e0c718a9))
  - `windows-verify` swung between 17m54s and 51m02s on an unchanged dependency tree, with `lance` and the datafusion set flipping between hit and miss run to run. kache's 2 GiB prefetch cap sits far below a ~790-crate graph where `lance` alone is 287 MB, so what it dropped was biased toward the largest artifacts; the cap and deadline are now unlimited. The cache store also moves off the network-attached `C:` onto `D:`, the fast local volume that already holds `target/`.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.10...v0.14.11

## [0.14.10](https://github.com/tenequm/pond/compare/v0.14.9...v0.14.10) - 2026-08-14

Windows is a supported platform. This release ships the first Windows binary pond has ever natively built and tested - an `x86_64-pc-windows-msvc` zip produced and smoke-tested on a real Windows runner, replacing the cross-compiled `windows-gnu` artifact that no CI had ever executed - alongside adapters that find sessions where Windows actually puts them and unattended sync through Task Scheduler.

The port opened as a community contribution from [@BadAd84](https://github.com/BadAd84) in [#147](https://github.com/tenequm/pond/pull/147). It landed the runtime core, arrived with an end-to-end verification across 331 real sessions, and caught a main-thread stack overflow that an independent clean-room design of the same work missed entirely. Everything else here builds on that foundation.

### <!-- 1 -->🎉 New Features
- **windows:** PATHEXT resolution, network-path gate, and platform fixes ([#155](https://github.com/tenequm/pond/pull/155)) ([95e8ce4](https://github.com/tenequm/pond/commit/95e8ce4d88a1e9f210811ce8546e9b95503d3ce1))
  - `claude` installed from npm is a `claude.cmd` shim, which `CreateProcess` never finds because it only appends `.exe`. PATH resolution is now `PATHEXT`-aware, and `pond init` spawns the resolved path through `%COMSPEC% /C`.
  - UNC paths are refused at `StorageUrl::parse` with a message naming the alternatives, instead of failing deep inside Lance after `object_store` has silently dropped the host.
  - `~\pond` in config now expands like `~/pond`. lance expands both and shellexpand only the latter, so the two disagreed and one of them created a literal `~` directory.
  - `validate_path_id` rejects Windows device names, trailing dots and spaces, and `:` on every platform - archives are portable, and each of these fails silently on Windows rather than loudly.
  - cliclack floor raised to 0.5.6, where Ctrl-C finally cancels a wizard prompt on Windows.
- **ci:** native Windows msvc gate and artifact ([#152](https://github.com/tenequm/pond/pull/152)) ([ee37a63](https://github.com/tenequm/pond/commit/ee37a63887adcd1a95fc4531e3389bafd025c98c))
  - `windows-verify` runs the full test suite on a `windows-2025` runner for every pull request; `windows-dist` builds the shipped msvc zip with `+crt-static` and a Windows application manifest. The never-executed `windows-gnu` artifact is dropped, and a release cannot be cut without both jobs green.
  - Also fixes a real sync-lock bug: `flock` belongs to the open file description, so a subprocess spawned while a `SyncLockGuard` was alive inherited a duplicate and held the lock past drop, making a later acquire report `Busy` with no holder.
- **config:** accept multiple source paths per adapter entry ([#149](https://github.com/tenequm/pond/pull/149)) ([caf6994](https://github.com/tenequm/pond/commit/caf6994d4d88842a876fcd496cb53111bd1ef9a8))
  - `[adapters.<name>].path` now takes a single directory or an array; an array fans out into one single-path pass per directory, so every configured location rides the same sync while the adapters and the seam stay untouched. A malformed array fails before any store write and `pond status` names it.
  - Also fixes `pond adapters discover` overwriting customized entries, `enable` leaving a pathless entry that failed every later sync, and openclaw reconciliation across several roots.
- Windows support ([#147](https://github.com/tenequm/pond/pull/147)) ([ed59091](https://github.com/tenequm/pond/commit/ed5909185136a13dee3cb20f70f831636da66b10)) - contributed by [@BadAd84](https://github.com/BadAd84)
  - Native directory layout: data, cache, and state under `%LOCALAPPDATA%\pond`, config at `%APPDATA%\pond\config.toml`, with explicit `XDG_*` overrides still honored on every platform.
  - A dedicated 16 MiB runtime thread, fixing a main-thread stack overflow that made the previously published Windows binary unusable - it could not run `--version`.
  - `raw_arg` for `cmd /C` secret commands (MSVCRT escaping is not how cmd parses), a sibling lock-holder file, and `protoc` vendoring scoped to non-MSVC targets so the existing release build kept working.
  - A Task Scheduler backend for `pond schedule`, since verified end to end on Windows 11: registration, cadence round-trip, a tick that runs and logs, and a clean stop.

### <!-- 2 -->🐛 Bug Fixes
- **adapter:** correct project-slug encoding for Windows and posix ([#156](https://github.com/tenequm/pond/pull/156)) ([a218ef3](https://github.com/tenequm/pond/commit/a218ef33cb337b2ec8c41bb9b23b02fb76231559))
  - Claude Code project slugs are now encoded by its actual rule - every non-alphanumeric character becomes one dash per UTF-16 code unit - so Windows drive letters, backslashes, and non-BMP characters round-trip instead of silently mismatching.
  - A `cwd`-less subagent transcript derives its project by decoding the project directory rather than reading its own parent directory, which named the subagent folder instead of the project.
  - Claude Desktop probes `%APPDATA%` before the MSIX package family, the order a real Windows install actually uses; the pi adapter keeps the second separator of a UNC prefix.
  - oh-my-pi task subagents now link to the session that spawned them, read from the parent transcript's own header rather than parsed out of a directory name - correct at any nesting depth, where name-parsing fabricated a parent id for nested agents and dropped the link for underscore-free names. Subagent sessions stay out of default search results, as designed.
  - All four adapters, plus the scheduler, verified against real captures on Windows 11 hardware; the Windows fixtures are committed.
  - Note for existing stores: `project` is an immutable field, so a stored session whose project now derives differently is refused on re-ingest with `immutable_project` and reported as a partial sync rather than rewritten. Nothing already stored is lost or changed, and only a fresh store picks up the corrected value.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.9...v0.14.10

## [0.14.9](https://github.com/tenequm/pond/compare/v0.14.8...v0.14.9) - 2026-08-12

### <!-- 1 -->🎉 New Features
- **adapter:** capture oh-my-pi (omp) sessions ([#142](https://github.com/tenequm/pond/pull/142)) ([f9e949e](https://github.com/tenequm/pond/commit/f9e949e285270db67bb0f8973de51742ab994b71))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.8...v0.14.9

## [0.14.8](https://github.com/tenequm/pond/compare/v0.14.7...v0.14.8) - 2026-08-12

The get family stops paying S3 for data it already holds resident: message-id resolution and session pages are now served from the mmap'd rowmap, cutting a cold `get-session <message-id>` from 166s to ~61s and a warm get's S3 round-trips by 63%.

### <!-- 3 -->🚀 Performance
- **read:** rowmap-served message-id resolution and get-family pages ([#141](https://github.com/tenequm/pond/pull/141)) ([a7abc72](https://github.com/tenequm/pond/commit/a7abc72a656b96288abcd044e8023bc5d753a096))
  - Message-id -> session resolution consults the resident rowmap before falling back to the full remote scan of the unindexed `messages.id` column - the scan cost ~93s of every by-message-id get and is eliminated on a map hit, which is definitive at any map version because the store is append-only and message ids are immutable. Session pages are likewise served from the map when its version matches the store, with fallback to the scan path on any staleness, corruption, or decode anomaly - staleness can never drop a newly synced message from a page.
  - `pond get-session`, `pond get-message`, and `pond sql` now open with the disk index cache and load the published rowmap chain, giving one-shot CLI reads the same warm path as the MCP server.
  - Measured on the live 14.3k-session / 2.65M-message S3 store: get-session by message id 166s -> ~61s, get-message 153s -> ~40s, warm get_message S3 range-GETs 10,900 -> ~4,000 (-63%), map-served output verified byte-identical to scan-served, search components unregressed.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.7...v0.14.8

## [0.14.7](https://github.com/tenequm/pond/compare/v0.14.6...v0.14.7) - 2026-08-12

pi gets a memory that outlives the session, and pond gets a way to hand a session back: `pond resume` restores stored sessions into a client's own files, the pi adapter learns harness-v2 (v4 JSONL and SQLite), and the new `pi-pond` extension wires recall and resume into pi itself.

### <!-- 1 -->🎉 New Features
- pi integration - pond resume, harness-v2 adapters, and the pi-pond extension ([39f4cb2](https://github.com/tenequm/pond/commit/39f4cb29c464cde8cc016b065513d1ac9bfbf7e8))
  - `pond resume <id> --to <adapter>` writes a stored session back out as the target client's own files, whole child lineage or nothing. It never overwrites: every destination is pre-checked and created with `O_EXCL`, a collision fails the batch before the first byte and names every existing path (exit 3), and a mid-batch write failure unwinds everything it created so no restored file is left behind (exit 4). Fidelity is the system's decision and is reported per session - same-origin replays are `native`, everything else an honest `foreign` reconstruction - and pi resume always emits v3, the one format every shipped pi loads (verified against a real pi 0.84.1 install).
  - The `pi-coding-agent` adapter now ingests harness-v2 sessions - v4 JSONL and the SQLite backend - detected per file and per database alongside v3. v4 headers give pi sessions a real `parent_session_id` and a `cwd`-derived project. The SQLite freshness watermark runs as four index seeks instead of a `UNION ALL` over the full mutation history (measured ~59x faster on a 300-session / 300k-row database).
  - `pi-pond`, the pi extension: one managed `pond serve --transport stdio --with-sync` child serves the four read-only recall tools and keeps the store synced; `/pond <query>` searches, then enter resumes a past session in place or `i` pastes a reference to it. Install with `pi install npm:pi-pond`.
  - A runnable fleet-capture example (`ops/examples/pi-fleet/`): dockerized pi workers pushing sessions to one shared S3 store, plus the deployment reference to go with it.
  - Safety fix swept in: the restore writer previously replaced its whole output root, so the first `pond resume --out-dir ~/.pi/agent` would have deleted the user's entire pi state. It now refuses to overwrite anything, ever.
  - Both extensions are now on npm alongside this release: `pi install npm:pi-pond` (`pi-pond@0.2.0`) and `openclaw plugins install openclaw-pond` (`openclaw-pond@0.1.0`) resolve from the registry - no checkout needed.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.6...v0.14.7

## [0.14.6](https://github.com/tenequm/pond/compare/v0.14.5...v0.14.6) - 2026-08-07

### <!-- 2 -->🐛 Bug Fixes
- exit quietly on a closed pipe instead of panicking ([3961cb3](https://github.com/tenequm/pond/commit/3961cb300b8d1e58677380e04e5d6030bda3813c))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.5...v0.14.6

## [0.14.5](https://github.com/tenequm/pond/compare/v0.14.4...v0.14.5) - 2026-08-07

### <!-- 5 -->📚 Documentation
- add server.json and MCP Registry name for official registry publishing ([59c926a](https://github.com/tenequm/pond/commit/59c926a9dcd8e6fd74345fc74d5daef6fcaba46e))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.4...v0.14.5

## [0.14.4](https://github.com/tenequm/pond/compare/v0.14.3...v0.14.4) - 2026-07-30

### <!-- 2 -->🐛 Bug Fixes
- **cli:** gate log color on a tty, and cover the stdout contract ([e617b3f](https://github.com/tenequm/pond/commit/e617b3f3fce57c2b90e702fe8d90c1999680fd38))
- **cli:** remove unused tracing progress layer ([#129](https://github.com/tenequm/pond/pull/129)) ([74f2659](https://github.com/tenequm/pond/commit/74f2659bde3c3bed00a09c0374abf99c30491bc1))

### <!-- 6 -->🧹 Chores
- **ci:** prune stale lock entries and close moon cache-input gaps ([058c3c9](https://github.com/tenequm/pond/commit/058c3c98001be23a7c0d54981329538642e933fd))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.3...v0.14.4

## [0.14.3](https://github.com/tenequm/pond/compare/v0.14.2...v0.14.3) - 2026-07-28

Automatic compaction now provably converges: a task filter built on a shrinkability guarantee replaces the fixed row floor that could rewrite wide-row tables forever.

### <!-- 2 -->🐛 Bug Fixes
- **substrate:** prevent byte-capped compaction loops ([a115798](https://github.com/tenequm/pond/commit/a11579808e2b530d02a43e8142b8934e94c3bbc5)) - pond's first community contribution, reported and fixed by @alexnayko ([#123](https://github.com/tenequm/pond/issues/123), [#124](https://github.com/tenequm/pond/pull/124)). The fixed 50,000-row candidacy floor made the row target unreachable for tables averaging over ~2.7 KB/row, so every sync re-rewrote the same live fragments: on the reporting store, a 1.03 GB table accumulated 32 GB physical across 31 full rewrites with zero net progress. The floor is gone, and the compaction filter now requires every planned rewrite to strictly shrink the fragment count, applying the per-fragment width check only when the byte cap can actually split the output - so ordinary small-fragment merges keep running while unwinnable rewrites are skipped. Every veto names its reason in the perf trace (`missing_sizes`, `cannot_shrink`, `row_target_unattainable`, `absorb_veto`, `invalid_byte_budget`), and the contract is now a named spec rule, `lance-compaction-filter` (section 3.4). Verified to reach a fixpoint: the reporter's live canary went 14 -> 6 fragments in one pass and the second pass changed zero files.

### <!-- 5 -->📚 Documentation
- **maintenance:** align compaction cap comments with shrinkability gate ([8ae77e8](https://github.com/tenequm/pond/commit/8ae77e8e764d2fc4c21b722a07c31a99cdeaea8c))

### <!-- 6 -->🧹 Chores
- **nix:** add a canonical flake at the repo root ([#126](https://github.com/tenequm/pond/pull/126)) ([3fefa03](https://github.com/tenequm/pond/commit/3fefa03b994c329eac1f1b23bd1717b7c55f0ad6)) - the install line is now `nix profile add github:tenequm/pond#pond`, no quoting and no `?dir=ops/nix` leaking the repo layout into the command.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.2...v0.14.3

## [0.14.2](https://github.com/tenequm/pond/compare/v0.14.1...v0.14.2) - 2026-07-24

### <!-- 1 -->🎉 New Features
- nanoclaw + hermes adapters and hermes-pond recall plugin ([#121](https://github.com/tenequm/pond/pull/121)) ([1ed0189](https://github.com/tenequm/pond/commit/1ed01895514d8257f9bd34774b4cd73c151b5dc9))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.1...v0.14.2

## [0.14.1](https://github.com/tenequm/pond/compare/v0.14.0...v0.14.1) - 2026-07-23

### <!-- 2 -->🐛 Bug Fixes
- **openclaw:** ingest stable file-era session stores cleanly and lower plugin floor to 2026.5.18 ([74ff3ad](https://github.com/tenequm/pond/commit/74ff3ad0bc31975ccb505757f5b3fb157bf103b7))

### <!-- 5 -->📚 Documentation
- add direct founder contact links (Telegram, X) to site and README ([#115](https://github.com/tenequm/pond/pull/115)) ([0e68339](https://github.com/tenequm/pond/commit/0e68339cef8bb6f9744b815defb6da4ac6881260))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.14.0...v0.14.1

## [0.14.0](https://github.com/tenequm/pond/compare/v0.13.2...v0.14.0) - 2026-07-23

Routes the MCP tool surface by intent, adds first-class OpenClaw ingestion, and makes local Lance stores crash-consistent (fsync on write + self-heal on open).

### <!-- 0 -->🛠 Breaking Changes
- **mcp:** [**breaking**] route the tool surface - rename pond_sql, split pond_get into pond_get_session/pond_get_message ([d72bd3b](https://github.com/tenequm/pond/commit/d72bd3b117800f42c6143daab01144bb720e42f4)) - `pond_sql_query` is now `pond_sql`, and the single `pond_get` splits into `pond_get_session` (reads a whole session) and `pond_get_message` (expands one message), so each tool routes on caller intent instead of guessing what an id means.

### <!-- 1 -->🎉 New Features
- OpenClaw integration - adapter, serve --with-sync, and openclaw-pond plugin ([#114](https://github.com/tenequm/pond/pull/114)) ([2f87e3f](https://github.com/tenequm/pond/commit/2f87e3fbc2c5efbd88e68e1347b993788034d0dd)) - a native OpenClaw session adapter, a `serve --with-sync` mode that keeps the store fresh while the MCP server runs, and the `openclaw-pond` plugin to wire it up.

### <!-- 2 -->🐛 Bug Fixes
- **substrate:** self-heal crash-poisoned local stores and fsync local writes ([#118](https://github.com/tenequm/pond/pull/118)) ([05b9f24](https://github.com/tenequm/pond/commit/05b9f24a51a3a38980f09617319f8d5dab1c6bb8))
  - Local writes now fsync the file and its parent directory after every put/copy/rename (unix, local stores only), so a crash can't leave a manifest published but unflushed. Measured cost is +4.2% (130.86s -> 136.40s) on the heaviest local path - a full 3.78M-row store copy with index rebuild - and effectively nil on routine incremental syncs, since Lance writes few large files and fsyncs amortize per file, not per row.
  - A failed open now self-heals instead of staying wedged: it walks `_versions/` head-down, scan-verifies each manifest by draining a full-column scan, quarantines any crash-poisoned manifest by atomic rename to `*.manifest.corrupt` (never deletes), then retries the open once. Scan-verify projects every column because a column-update commit (embed's vector write) puts new columns in their own per-fragment data files a narrow scan would skip; `file+uring://` stores are healed too.
- **embed:** two-step backlog gate - manifest-only lag fast path, exact-count confirm ([#73](https://github.com/tenequm/pond/pull/73)) ([464f954](https://github.com/tenequm/pond/commit/464f954828f590ffaf569a1f03802aba945f24e9)) - the embedding-backlog check first reads a cheap manifest-only lag signal and only falls back to an exact `count_rows` confirm when that signal says work may be pending, avoiding a full count on every gate.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.13.2...v0.14.0

## [0.13.2](https://github.com/tenequm/pond/compare/v0.13.1...v0.13.2) - 2026-07-14

### <!-- 2 -->🐛 Bug Fixes
- **opencode:** read opencode sqlite storage ([#108](https://github.com/tenequm/pond/pull/108)) ([73273d7](https://github.com/tenequm/pond/commit/73273d7506643b7255d50702a6ec634b7db23615))

### <!-- 6 -->🧹 Chores
- ignore .playwright-cli local state ([1b22537](https://github.com/tenequm/pond/commit/1b225370c3fbe5c10cc3da4485447119ffae42b3))
- correct the binstall pkg-url comment ([5498a14](https://github.com/tenequm/pond/commit/5498a144e8adba628c5d503b04fec642dd869317))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.13.1...v0.13.2

## [0.13.1](https://github.com/tenequm/pond/compare/v0.13.0...v0.13.1) - 2026-07-13

Fixes `cargo install pond-db`, broken for every release since v0.10.0: `pond skill` embeds SKILL.md via `include_str!`, but the file was excluded from the published crate, so the .crate on crates.io could not compile. Installs via brew, nix, and cargo-binstall were unaffected (they ship prebuilt binaries). CI now gates packaging so this class of breakage cannot recur.

### <!-- 2 -->🐛 Bug Fixes
- ship SKILL.md in the published crate so cargo install compiles ([#105](https://github.com/tenequm/pond/pull/105)) ([4c2213e](https://github.com/tenequm/pond/commit/4c2213e0ab7489c740d56084059cd9acc5ae1bfd)) - drops SKILL.md from `Cargo.toml`'s exclude list and adds a `check-package` CI gate: `cargo package --list` must contain every `include_str!`/`include_bytes!` target, since publishing skips the verify build (`publish_no_verify`)

### <!-- 5 -->📚 Documentation
- **site:** lead the demo with the pond status scene ([ba67eca](https://github.com/tenequm/pond/commit/ba67ecabd34f488141fca18cfd694ed6c76d78e8))

### <!-- 6 -->🧹 Chores
- **bench:** add fmindex_probe substring-index comparison harness ([a9c38cf](https://github.com/tenequm/pond/commit/a9c38cf3cdb140b2f33bac2dbd6aa3f2c2db945f))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.13.0...v0.13.1

## [0.13.0](https://github.com/tenequm/pond/compare/v0.12.2...v0.13.0) - 2026-07-07

Tool analytics stop paying the JSON tax: the common query shapes now run on three narrow derived columns instead of the multi-GB `variant_data` blob, turning remote S3 tool GROUP BYs from hard >30s timeouts into ~9s answers (local: 1,693ms -> 48ms, ~35x). Existing stores upgrade themselves in place on first open - no re-ingest, no manual step - but once migrated they are unreadable by older pond binaries, so upgrade every machine that shares a store together.

### <!-- 0 -->🛠 Breaking Changes
- **parts:** [**breaking**] materialize tool columns + in-place schema migration; per-query SQL timeout ([#100](https://github.com/tenequm/pond/pull/100)) ([5490122](https://github.com/tenequm/pond/commit/54901228e3f3389ce9494c5d7bda5f213ee559ab))
  - **What breaks:** the `parts` table gains three derived nullable columns - `tool_name`, `call_id`, `is_failure` - plus a BTree scalar index on `tool_name`. pond <= 0.12.2 enforces strict schema equality, so a store first opened by 0.13.0 becomes **unreadable by older binaries**, and there is no downgrade path once migrated. `variant_data` stays the verbatim source of truth; the new columns are derived from it at write time.
  - **What upgrading requires:** nothing manual. The first open by a 0.13.0 binary migrates the store in place: a one-time backfill derives the three columns from stored `variant_data` (seconds on a local store; one `add_columns` commit on a remote/S3 store), announced by a single stderr notice. If multiple machines sync into one shared store, upgrade all of them before the first 0.13.0 open - any host still on <= 0.12.2 loses access the moment the store migrates.
  - Pre-0.13.0 `.pond` archives keep restoring unchanged: the columns are derived at the read boundary and the archive file is never modified.
  - Also in this change: a per-query SQL timeout - `timeout_seconds` on `pond_sql_query`, `--timeout` on `pond sql` (default 30s, clamp 1..600); the timeout error names the knob and steers toward the native columns.
  - Measured on the full real corpus (10,840 sessions / 1.81M messages / ~608K tool-call parts) over S3: tool GROUP BY timeout -> 8.5-9.7s, failure-rate self-join timeout -> 23-24s, indexed `tool_name = 'Bash'` point filter 4.7-8.0s. Ingest and read benchmarks unchanged within noise.

### <!-- 5 -->📚 Documentation
- update README for clarity and structure, add new sections on usage and maintenance ([8298db5](https://github.com/tenequm/pond/commit/8298db572481320b47075eac7ef2aafc54bb0884))
- launch fold, memory-tool FAQ, OG/meta, vocs 2.3.3, reference-page fixes ([a3a3e65](https://github.com/tenequm/pond/commit/a3a3e654612ac82caca7066afaecba4f1ca6fb16))
- **readme:** launch fold - hook quote, live search demo, real prompts ([d53226f](https://github.com/tenequm/pond/commit/d53226fe0bf619e601c009e440175f7ec1fa51f9))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.12.2...v0.13.0

## [0.12.2](https://github.com/tenequm/pond/compare/v0.12.1...v0.12.2) - 2026-07-07

### <!-- 1 -->🎉 New Features
- **deps:** upgrade lance 7.0.0 -> 8.0.0 ([56e968e](https://github.com/tenequm/pond/commit/56e968ed355581eb7464ad0cd2c19236b81b9e67))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.12.1...v0.12.2

## [0.12.1](https://github.com/tenequm/pond/compare/v0.12.0...v0.12.1) - 2026-07-06

Sync status now reports genuine work only, and forked subagent transcripts are no longer silently dropped. Verified end to end against the full real corpus (11k+ sessions / 1.8M messages) on both a local store and the S3 backend: ingestion is byte-identical to v0.12.0 except for the recovered data.

### <!-- 1 -->🎉 New Features
- **status:** pending preview for claude-desktop-app and opencode ([fa41ddf](https://github.com/tenequm/pond/commit/fa41ddf3346b962e4db1ee7b3ee23e46787e5098)) - these adapters now report an accurate per-session pending count instead of "pending unknown".

### <!-- 2 -->🐛 Bug Fixes
- **sync:** stop reporting provably-synced or empty sessions as pending ([6a05cd1](https://github.com/tenequm/pond/commit/6a05cd17f0869967d149df42f0b4b961b3d3c0ee)) - a source whose stored watermark already covers it (or that a bounded whole-source scan proves holds nothing ingestible) no longer counts as pending, so a clean store reports "up to date" instead of a permanent phantom floor (real corpus: claude-code 43 -> 0 false pending, codex-cli 4 -> 0). Skip signals derive only from stored data; anything the gate cannot cheaply classify still re-reads.
- **claude-code:** ingest forked subagent transcripts ([06a2d27](https://github.com/tenequm/pond/commit/06a2d2725ede538d5016dcae4a6af178fd636e48)) - a `/fork` subagent transcript (Claude Code >= 2.1.117) opens with a `fork-context-ref` header row that carries no `sessionId`, which the adapter rejected as "line 1 missing sessionId" - silently dropping the entire forked conversation. The id is now taken from the first row that carries one (subagents derive it from the path regardless), recovering the full transcript with lossless native restore. Real corpus: 1 of 7,843 subagent transcripts affected, recovered as 16 messages, every other row byte-identical.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.12.0...v0.12.1

## [0.12.0](https://github.com/tenequm/pond/compare/v0.11.2...v0.12.0) - 2026-07-03

Onboarding and multi-machine sync: a first run no longer looks like it hangs, the scheduled sync no longer races a manual one, and `pond status` finally reports this host's own relationship to the store. Verified end to end on a fresh install (macOS and Linux) plus cold-context first-run testing.

### <!-- 0 -->🛠 Breaking Changes
- **`pond status --format json` output shape changed** - the `adapters` field is renamed to `source_agents` (count of distinct source agents in the store), and `schedule` changes from a string to an object `{active, backend, every}`. A new top-level `pond_version` field carries the producing binary's version so consumers can pin the format going forward. Scripts parsing status JSON must update these keys. ([5e56aad](https://github.com/tenequm/pond/commit/5e56aad4cd95d0774b2ee6392bed9d5564e41f79))

### <!-- 1 -->🎉 New Features
- **sync:** per-host single-flight lock (a local flock in the state dir, never on the Lance store - cross-host writers stay pure OCC); a second sync waits and names the holder, `--no-wait` skips cleanly (exit 0) and is what the scheduled job passes so ticks never queue. Adds `--dry-run` (per-adapter freshness preview, writes nothing) and `--format json` (one summary document on stdout for every outcome, progress on stderr). Every long phase now has a live face - rowmap-build spinner, model-download stage line, per-adapter bar with a recent-rate ETA, inline-embed counter - plus a ~30s heartbeat off-TTY. `pond status` gains a local section (per-adapter sources + pending-sync counts, last sync outcome incl. a surfaced scheduled failure, next scheduled run) and `--hosts` fleet view; `pond init` runs the first sync in the foreground and registers the schedule only after it completes, so a fresh timer never races it. ([26a7c7a](https://github.com/tenequm/pond/commit/26a7c7a9ad84aec0313e0591f9e0d06142bd8067))
- **nix:** canonical flake shipped in-repo; releases are the single binary host ([a4f5e09](https://github.com/tenequm/pond/commit/a4f5e095eec9c10ba52425a056a6c89297d9c6f0))

### <!-- 2 -->🐛 Bug Fixes
- **cli:** first-run onboarding polish - the ~500 MB embedding-model download now announces itself before it starts (previously a silent multi-minute "hang"); `pond status` no longer fuses long adapter names with their counts and reads "semantic ready (brute-force; index builds at scale)" instead of the alarming "below activation threshold"; empty-store search points at `pond init` rather than blaming filters that were never set; no-adapters states name `pond adapters discover`; message deltas are labelled "searchable" so the searchable-vs-total gap stops reading as data loss. ([43d53f5](https://github.com/tenequm/pond/commit/43d53f50bdb9a820764cce63ee17513b82fc4455))
- **cli:** `pond status --format json` emits a JSON error document on the store-open failure path instead of empty stdout (matching `sync --format json`); vector search reads "N nearest messages" with a `--mode fts` caveat so a gibberish query no longer looks like confident relevance; and `pond sql`/`pond search`/`pond get` error text renders CLI verbs instead of the shared module's MCP tool/resource names. ([9423535](https://github.com/tenequm/pond/commit/94235359d2db7b05b0d2f2824e7ae76ae6a351cb))

### <!-- 4 -->🚜 Refactor
- **sync:** address PR review findings - close the Ctrl-C window in `init` schedule registration, reject un-embeddable `XDG_STATE_HOME` paths, and DRY the shared status/heartbeat helpers ([29a4b3a](https://github.com/tenequm/pond/commit/29a4b3a2a76f8c9642781e04ff04a5ed4946dd96))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.11.2...v0.12.0

## [0.11.2](https://github.com/tenequm/pond/compare/v0.11.1...v0.11.2) - 2026-07-03

### <!-- 2 -->🐛 Bug Fixes
- **index:** consolidate FTS delta segments by rebuild, never merge ([3a46cea](https://github.com/tenequm/pond/commit/3a46cea98403dd4983d52b9ed4195ac357f85981))
- **index:** guard FTS folds against all-null tails; honest pending counts ([a7a82f9](https://github.com/tenequm/pond/commit/a7a82f987ed7ab2ff83541852636ba42d3078ec8))
- **optimize:** make --rebuild reachable when the fold is broken; document claude.ai import ([7695f3f](https://github.com/tenequm/pond/commit/7695f3f8a27bbeef904cd7c6c6a86d4bad14e041))

### <!-- 3 -->🚀 Performance
- **sync:** escalating peek window, parallel peek, skip no-op sessions merge ([11d1037](https://github.com/tenequm/pond/commit/11d1037ea9c1753fc101bb09543cc33029908886))

### <!-- 6 -->🧹 Chores
- **repo:** move gitleaks/release-plz configs + git hooks under .github/, moon-manage hook setup ([2323744](https://github.com/tenequm/pond/commit/23237441d92ec569792d8b252aa966431fca48c4))

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.11.1...v0.11.2

## [0.11.1](https://github.com/tenequm/pond/compare/v0.11.0...v0.11.1) - 2026-07-01

### <!-- 3 -->🚀 Performance
- **sync:** cut remote sync from ~80-520s to ~44s
- **sync:** eliminate compaction churn + batch scalar folds + live progress

### <!-- 4 -->🚜 Refactor
- **bench:** rename copy_bench -> write_bench, add write-path profiler

### <!-- 5 -->📚 Documentation
- add logo to README

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.11.0...v0.11.1

## [0.11.0](https://github.com/tenequm/pond/compare/v0.10.2...v0.11.0) - 2026-07-01

The write path becomes append-only - incremental index folds, inline embed at ingest, and delta-only copy - and remote `pond_search` drops from ~8s to sub-second.

### <!-- 0 -->🛠 Breaking Changes
- **Append-only write path.** `pond sync` now embeds each message inline in its ingest commit, and both sync and copy fold the scalar indexes incrementally (`optimize_indices(append)`) instead of a full `create_index(replace=true)` rebuild - so the whole-source-column index rebuild that dominated each sync/copy tail (~520s on the real corpus) is gone. `pond copy` carries only absent-or-grown sessions and appends a grown session's delta rows through the shared ingest write path (append, not merge-insert), keeping remote copies bandwidth-bound rather than commit-latency-bound.

### <!-- 3 -->🚀 Performance
- **search:** warm remote `pond_search` drops from **~7.9s to sub-second** (best 224ms) on the full S3 corpus (11,788 sessions / 2.14M messages). Two stages did work the query never needed. `has_embeddings` answered "does this store have embeddings?" with an `IsNotNull(vector)` scan of the entire vector column (**6.8-11.7s per query**); it now reads the manifest (index presence, ~0ms) and only falls back to a `LIMIT 1` probe when no index exists. Per-hit part summaries fetched file blobs from S3 and scanned `parts` once per session sequentially; they now skip the blob (the label rides `variant_data` metadata) and run concurrently. The real retrieval - embed + IVF probe + hydrate - is ~0.1s.
- **search (#75):** `from_date`/`to_date` returned empty on remote stores because the `messages_timestamp_zonemap` mis-prunes the tz-aware `timestamp` column (`ScalarValue::partial_cmp` across the tz mismatch prunes every zone). The index is dropped and date bounds run as a refine over the candidate set. Stores that already built it: run `pond optimize --drop-index messages_timestamp_zonemap` once, or date filters stay empty there.

### <!-- 5 -->📚 Documentation
- add remote read-path cold-start plan and drop stale prewarm comment figures

### <!-- 6 -->🧹 Chores
- bench batch/commit sweeps, append-only write-path plan, AIMD hands-off rule
- enforce changelog header taxonomy (pre-commit + moon + CI); backfill 0.10.1/0.10.2

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.10.2...v0.11.0

## [0.10.2](https://github.com/tenequm/pond/compare/v0.10.1...v0.10.2) - 2026-06-21

A follow-up to the 0.10.1 sync work: the embed stage stops scanning wide columns to find its backlog.

### <!-- 3 -->🚀 Performance

- **embed:** the per-sync backlog check no longer scans full columns - model-swap detection is a `LIMIT 1` read and the backlog gate is a manifest-only count (idle embed-only **2.24s -> 0.67s**).
- **embed:** the worker's pending scan filters the co-set, ~50x narrower `embedding_model` column instead of decoding the 1.2 GB Float16 `vector` column to locate unembedded rows (a whole-table vector decode **-> 149 KB**).

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.10.1...v0.10.2

## [0.10.1](https://github.com/tenequm/pond/compare/v0.10.0...v0.10.1) - 2026-06-20

A sync performance and correctness release: incremental `pond sync` no longer re-reads the whole corpus on every run.

### <!-- 3 -->🚀 Performance

- **Incremental `pond sync` is dramatically faster.** Two compounding fixes to the freshness path:
  - Claude Code appends trailing metadata rows (`last-prompt`, `permission-mode`, `bridge-session`, ...) with no timestamp, so the watermark peek returned `None` and ~2,000 of ~9,800 sessions never fresh-skipped - re-decoding ~1.18M already-stored rows every sync. The peek now walks back to the last timestamped row. Measured on the real corpus: claude-code import **20.1s -> 1.76s**, rows re-decoded **1.18M -> 10.5k**, fresh-skips **7,863 -> 9,823**.
  - The resident rowmap now delta-extends across embedding's fragment rewrites (keyed on the stable row ids already enabled) instead of rewriting a full ~283 MB base every sync.

### <!-- 2 -->🐛 Bug Fixes

- **sync:** rebuild the rowmap when the base version's manifest was reclaimed by the cleanup retention window, instead of silently re-reading every source on every sync forever.
- **build:** gate the `RLIMIT_NOFILE` bump to Unix so the Windows cross-build compiles.
- **schedule:** gate `ScheduleEvery::secs`/`from_secs` to Unix (dead-code on the Windows target).
- **ci:** point `pnpm/action-setup` at `docs/site/package.json` so the docs site deploys.

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.10.0...v0.10.1

## [0.10.0](https://github.com/tenequm/pond/compare/v0.9.0...v0.10.0) - 2026-06-20

This release rebuilds the admin CLI, the retrieval model, and the sync/copy write path - and makes remote-S3 operation dramatically faster.

### <!-- 0 -->🛠 Breaking Changes
- **cli:** retire the overloaded "sources" - `[sources.*]`/`pond sources` -> `[adapters.*]`/`pond adapters`; config auto-migrates on `pond init`
- **cli:** `pond storage use` is switch-only (a pure pointer flip, copies nothing); data copy moves to the new top-level `pond copy`; bare `pond storage` removed (use `pond status`)
- **cli:** remove `pond export` / `pond import` - snapshot to a `.pond` archive or `.jsonl` stream and restore from one is now `pond copy --to <file>` / `pond copy --from <file>`
- **cli:** split maintenance out of sync - `pond sync` runs import+embed+index by default; the new `pond optimize` verb runs embed+index on demand, and `sync --no-optimize` defers to it
- **cli:** `pond sync` no longer discovers or auto-enables adapters - enabling is the explicit job of `pond adapters`/`pond init`, so a scheduled sync can never grow the adapter set
- **cli:** verb convergence + flag renames - `--config`->`--config-file`, init `--schedule`->`--every`, `sync --source-dir`->`--path`, sync stage `update-indexes`->`index`, `--format pretty`->`text`; `--storage-path`/`--config-file` are now root-global selectors
- **cli:** `pond copy` requires explicit endpoints, adds the `@` (configured store) and `local` keywords; self-verifying with an id-set completeness check (exit 0 SYNCED / exit 6 missing rows)
- **storage:** first-class `pond creds {add,list,delete}` for URL-scoped credential sets; `pond init` captures remote creds inline (masked prompt, never argv)
- **search:** drop server-side hybrid fusion for single-arm retrieval - `mode=vector` (default) or `mode=fts`, plus `--sort-by recency`
- **search:** vector index IVF_PQ -> IVF_SQ (drop the refine pass); FTS moves from character-ngram to a word `simple` tokenizer with English stemming
- **sync:** durable idempotent-replay sync/copy with a cheap messages-based S3 oracle and `sync --verify`; resident per-session `max_ts` watermark replaces the version-resolution oracle
- **tools:** redesign `pond_get`/`pond_sql_query` and unify the transcript renderer
- **copy:** append fast-path + per-table write plan (absent rows append, grown rows merge)

### <!-- 1 -->🎉 New Features
- **cli:** `pond skill` prints the bundled agent-onboarding SKILL.md, in lockstep with the binary
- **copy:** incremental store-to-store copy with no temp staging; streams the source scan straight into the destination
- **search:** resident per-message meta cache (mmap'd, LSM version-delta refresh) shared across pond processes
- **storage:** self-verifying migrate and `pond storage verify`

### <!-- 3 -->🚀 Performance
*Measured on the real ~2M-message S3 corpus (Hetzner nbg1); baseline = pre-optimization on this branch.*

Sync & status:
- per-session staleness oracle: **79s warm / 133s cold -> ~1s / ~4s** (messages-based key replaces the `versions()` per-manifest fetch storm)
- warm re-sync of the full corpus: **~928s -> ~25s** (append fast-path + the new oracle)
- `status -v`: **130s -> ~14s**; the stale-embedding count that runs in every default sync: **59.5s -> ~7s**

Copy:
- append fast-path vs merge-insert for absent rows: **5.47x faster** (13.8 min vs 75.7 min full-corpus; 62 vs 2,685 objects written)
- streaming store-to-store, no temp staging: **1.92x faster** (24.1s -> 12.6s, local 500-session set); unchanged-source re-copy **~90 ms**

Search:
- FTS arm latency **2043ms -> 2ms p50** (6667ms -> 125ms p95) and **~60 -> 3 object GETs/query** via the resident row-key map; per-query S3 bytes **-81%** (6.0 -> 1.16 MB)
- FTS index **1.14 GB -> 41 MB (28x)** and query RAM **2248 -> 379 MB (5.9x)** after the word-tokenizer switch; English Success@3 **31/111 -> 66/111 (2.1x)**
- FTS cold query (full corpus) **76s -> 27s p50** (148.9s -> 48.5s p95); cold server prewarm **175-442s -> ~81s**
- vector arm **~393 -> 0-1 object GETs/query** after IVF_PQ -> IVF_SQ and dropping the refine pass
- bounded server RAM lowers `sql` cold **18s -> 5.9s**
- resident row-key map: **281.7 MiB** for 2.1M messages, built in **~3.8s**, removing the per-query hydration scan (and a remote Lance decoder panic)

### <!-- 5 -->📚 Documentation
- **spec:** add the `session-movement-complete` completeness rule, the session-erasure exception, and micro-batch live-write
- migrate the docs site from mdBook to vocs (pond.locker); correct the search model to single-arm across spec/README/site; refresh SKILL.md for agent ergonomics

### <!-- 2 -->🐛 Bug Fixes
- **cli:** phantom embed backlog, progress-bar wrapping, and verify memory
- **sync:** restore the per-session staleness watermark from the row version
- **build:** gate the `RLIMIT_NOFILE` bump to Unix so the `x86_64-pc-windows-gnu` release binary builds

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.9.0...v0.10.0

## [0.9.0](https://github.com/tenequm/pond/compare/v0.8.1...v0.9.0) - 2026-06-12

### <!-- 0 -->🛠 Breaking Changes
- **init:** [**breaking**] redesign storage onboarding and add 5m sync cadence

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.8.1...v0.9.0
## [0.8.1](https://github.com/tenequm/pond/compare/v0.8.0...v0.8.1) - 2026-06-12

### <!-- 2 -->🐛 Bug Fixes
- **init:** offer the local default when a storage probe fails

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.8.0...v0.8.1
## [0.8.0](https://github.com/tenequm/pond/compare/v0.7.0...v0.8.0) - 2026-06-11

### <!-- 0 -->🛠 Breaking Changes
- **config:** [**breaking**] URL-scoped creds, storage URLs, introspection, and migrate

### <!-- 6 -->🧹 Chores
- **substrate:** add real-S3 concurrent multi-writer OCC benchmark

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.7.0...v0.8.0
## [0.7.0](https://github.com/tenequm/pond/compare/v0.6.0...v0.7.0) - 2026-06-11

### <!-- 0 -->🛠 Breaking Changes
- **mcp:** [**breaking**] minimize pond_search/pond_sql_query param surface

### <!-- 2 -->🐛 Bug Fixes
- **sql:** make pond_sql_query first-try-correct for agents

### <!-- 6 -->🧹 Chores
- lance-style release notes and point nix install at pond-nix

**Full Changelog**: https://github.com/tenequm/pond/compare/v0.6.0...v0.7.0

## [0.6.0](https://github.com/tenequm/pond/compare/v0.5.2...v0.6.0) - 2026-06-10

### Added

- *(ingest)* stamp host provenance on inserted message rows ([#40](https://github.com/tenequm/pond/pull/40))
- *(search)* [**breaking**] per-message session fusion, raw-magnitude scoring, absence honesty

## [0.5.2](https://github.com/tenequm/pond/compare/v0.5.1...v0.5.2) - 2026-06-10

### Other

- *(maintenance)* veto absorb-heavy compaction tasks and derive byte-based fragment targets

## [0.5.1](https://github.com/tenequm/pond/compare/v0.5.0...v0.5.1) - 2026-06-10

### Added

- *(sql)* harden pond_sql_query and add error-guided recovery

## [0.5.0](https://github.com/tenequm/pond/compare/v0.4.0...v0.5.0) - 2026-06-05

### Added

- *(mcp)* [**breaking**] add pond_sql_query read-only SQL tool (table/json/ndjson/parquet) + pond sql CLI

## [0.4.0](https://github.com/tenequm/pond/compare/v0.3.2...v0.4.0) - 2026-06-05

### Other

- *(maintenance)* [**breaking**] gate compaction, drop unsafe vacuum, carve out [maintenance] config

## [0.3.2](https://github.com/tenequm/pond/compare/v0.3.1...v0.3.2) - 2026-06-04

### Added

- *(adapter)* add claude-desktop-app and claude-ai-export adapters

## [0.3.1](https://github.com/tenequm/pond/compare/v0.3.0...v0.3.1) - 2026-06-04

### Fixed

- *(adapter)* recognize nested workflow-subagent transcripts

## [0.3.0](https://github.com/tenequm/pond/compare/v0.2.8...v0.3.0) - 2026-06-03

### Added

- *(cli)* redesign sync/status output and gate sources behind enabled ([#26](https://github.com/tenequm/pond/pull/26))
- *(adapter)* add pi and opencode source adapters

### Fixed

- *(substrate)* handle wrapped namespace table-not-found errors
- *(adapter)* apply polish-review fixes across opencode, pi, seam, and writer
- *(adapter)* harden pi and opencode adapters per review

### Other

- *(substrate)* collapse namespace error-chain walker
- rename pi adapter to pi-coding-agent

## [0.2.8](https://github.com/tenequm/pond/compare/v0.2.7...v0.2.8) - 2026-06-03

### Added

- *(mcp)* enrich the tool surface for better agent discoverability
- *(docs)* add an mdBook documentation site
- *(release)* cargo-binstall metadata and richer crates.io package fields, so prebuilt binaries install via `cargo binstall pond-db`

### Changed

- *(release)* replace goreleaser-Pro with a release-plz + moon publishing pipeline (crates.io, Homebrew tap, NUR)

### Other

- *(moon)* exclude local .claude/.agents tooling from input hashing

## [0.2.7](https://github.com/tenequm/pond/compare/v0.2.6...v0.2.7) - 2026-06-02

### Other

- bump kache to v0.4.1 and persist buildkit cache via PVC

## [0.2.6](https://github.com/tenequm/pond/compare/v0.2.5...v0.2.6) - 2026-06-02

### Fixed

- *(build)* deterministic rcodesign sign + split package step; darwin-first; 2x buildkit

## [0.2.5](https://github.com/tenequm/pond/compare/v0.2.4...v0.2.5) - 2026-06-02

### Fixed

- *(build)* darwin via zig 0.16 + post-link sdk rewrite & re-sign

## [0.2.4](https://github.com/tenequm/pond/compare/v0.2.3...v0.2.4) - 2026-06-02

### Fixed

- *(build)* pin zig 0.15.2 so darwin binary records sdk<26

### Other

- drop redundant setup-protoc; cite real zig tickets for dylib bug

## [0.2.3](https://github.com/tenequm/pond/compare/v0.2.2...v0.2.3) - 2026-06-02

### Fixed

- *(build)* pin macOS SDK to 15.5 to avoid dyld duplicate-dylib abort
- *(release)* publish binaries to public homebrew-tap

### Other

- split moon format/lint/test into separate steps
- disable release-plz semver-checks to speed up release PRs

## [0.2.2](https://github.com/tenequm/pond/compare/v0.2.1...v0.2.2) - 2026-05-29

### Other

- *(readme)* replace standard-readme badge with crates.io version
- *(readme)* drop CI badge
- export KUBECONFIG so buildx subprocess inherits it
- set KUBECONFIG from $RUNNER_TEMP in-step, not job env
- fix goreleaser dirty-tree + add release recovery dispatch

## [0.2.1](https://github.com/tenequm/pond/compare/v0.2.0...v0.2.1) - 2026-05-28

### Fixed

- *(.gitignore)* anchor .claude patterns to root so fixture paths are not double-tracked
- *(get)* default to conversational view; consolidate spec.md rules

### Other

- chain publish-release on release-plz releases_created output
- *(release-plz)* enable release-pr flow alongside dry-run release
- rename jobs for clarity (build-and-test, release-plz, publish-release)
- *(release)* publish binaries + homebrew + nur via goreleaser
- preserve target/ between runs with checkout clean=false
- *(release-plz)* run in dry-run mode
- bracket cargo commands with kache stats steps in both jobs
- scope concurrency to github.ref so newer runs supersede older
- split into ci + release jobs, both on the self-hosted runner
- collapse release into the ci job (single self-hosted job, conditional release step)
- cancel in-flight CI runs on the same pull_request head
- switch CI to self-hosted runner on bl
- prep repo for public release + cross-compile pipeline
