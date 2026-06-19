# CLI end-to-end test on the real remote corpus (2606-19)

## Goal

A committed, repeatable end-to-end harness that exercises every `pond` command-line command against the real release binary and real data, so the final release ships with proof that the whole CLI surface works - not just the unit/integration suite (which drives library entry points and fixture stores). This is the durable replacement for the ad-hoc "55 live checks" run during the storage-admin-CLI PR.

The harness drives the actual compiled `pond` binary (not library functions), including the interactive wizards, and asserts exit code plus an output anchor per command. It is gated out of CI and normal `cargo test` because it needs remote S3 credentials, mutates a scratch store, and is slow.

## Command surface under test (16 top-level commands)

From `src/main.rs` `enum Command` and the subcommand enums:

- `init` (wizard)
- `sync`
- `adapters` -> `list`, `discover` (wizard), `enable`, `disable`
- `status` (and `-v`)
- `search`
- `get`
- `sql`
- `serve`
- `mcp`
- `schedule` -> `start`, `stop`, `status`, `logs`
- `storage` -> `check`, `use`
- `creds` -> `add` (wizard), `list`, `delete`
- `copy` (and `--verify-only`)
- `config` -> `show`, `path`, `schema`
- `completions`
- `optimize`

Root-global flags `--storage-path` and `--config` (both `global = true`) are exercised both before and after the subcommand to prove the parse order, mirroring the PR's byte-identical check.

## Targets and the safety model

Three execution contexts, never crossed:

1. Real corpus, read-only. `--storage-path s3+https://nbg1.your-objectstorage.com/pondarium/pond-full-corpus-benchmarking-copy --config ~/.config/pond/config.toml`. The `[creds.default]` block in that config authenticates to the `pondarium` bucket. Used for every read-only command (`status`, `search`, `get`, `sql`, `storage check`, `serve`, `mcp` read, `copy --verify-only`).

2. Scratch base store, mutable. A full copy of the real corpus at a sibling prefix `s3+https://nbg1.your-objectstorage.com/pondarium/pond-e2e-base`, seeded once up front with `pond copy --from <real> --to <scratch-base>`. Every in-place mutating command (`optimize`, `sync`-against, and the `copy` source) runs here so the production prefix is never written. A second empty prefix `pond-e2e-copy-dest` receives the full-copy write test. Per decision: the scratch prefixes are kept after the run (no teardown) for inspection.

3. Sandbox HOME/XDG, mutable config only. Every config-writing or interactive command (`init`, `adapters enable/disable/discover`, `creds add/delete`, `storage use`, `schedule start/stop`) runs with `HOME`, `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_STATE_HOME` pointed at per-test temp dirs and `NO_COLOR=1`, seeded with a `config.toml` that carries a copy of the real `[creds.default]` block plus a `[storage] path` pointing at a scratch prefix. This is the same sandboxing discipline `CLAUDE.md` documents for interactive-flow testing. The real `~/.config/pond/config.toml`, the real launchd agents, and the real local store are never touched.

Hard rules:
- Read tests never pass a mutating verb.
- Mutating tests never resolve to the production prefix or the real config path.
- `schedule start/stop` register a launchd agent, which is user-global on macOS (`~/Library/LaunchAgents`); they run only inside the sandbox HOME and the harness asserts the plist lands under the sandbox, never the real `~/Library/LaunchAgents`. If sandbox isolation of launchd cannot be guaranteed, those two are downgraded to a Ctrl-C/refuse-to-write assertion (prove the wiring without registering).

## Harness form

A single Python standard-library `pty` driver at `ops/e2e/run.py` (no third-party deps), plus `ops/e2e/README.md`. One harness covers both non-interactive commands (spawn, capture, assert exit + anchor) and the wizards (`pty.fork`, select-loop on the pty fd, strip ANSI, write keystrokes on anchor substrings) - the pattern already proven in `CLAUDE.md`'s "Testing interactive CLI flows" section. Rust was considered and rejected: driving cliclack wizards from a Rust test needs an extra pty crate and more ceremony, and Python stdlib `pty` is the faster, already-documented path.

Structure:
- A declarative table of non-interactive checks: `(name, argv, target, expected_exit, anchor_substring)`.
- A small set of wizard scenarios with anchor->keystroke scripts.
- A runner that sets up the three contexts, executes each check, and prints a final pass/fail matrix with per-check exit code and elapsed time. Overall deadline per check; on timeout it sends Ctrl-C so a missed anchor cannot hang the run.

## Per-command plan (mode / target / drive / expected)

Non-interactive, read-only (real corpus):
- `status`, `status -v` -> exit 0, anchor `storage` / `searchable`.
- `search "<concept>"` (vector default) and `search --mode fts "<word>"` -> exit 0, anchor a session/result marker.
- `get --session-id <known>` and `get --message-id <known>` -> exit 0, anchor transcript header. Known ids are discovered at runtime from a `sql` query so the harness is corpus-agnostic.
- `sql "SELECT count(*) FROM messages"` (text) and `--format ndjson` -> exit 0.
- `storage check <real-url>` -> exit 0, anchor probe-ok.
- `config show` / `path` / `schema` -> exit 0.
- `completions zsh` -> exit 0, anchor `compdef`/`_pond`.
- `serve` -> bind on an ephemeral port, issue one HTTP request, assert a valid response, then terminate.
- `mcp` -> write a JSON-RPC `initialize` on stdin, assert `serverInfo: pond` in the response.
- Global-flag order: run `status` with `--storage-path`/`--config` before and after the subcommand; assert byte-identical stdout.
- Failure exit codes (read side): bad URL -> exit 2 (parse); missing id -> exit 1 (not_found); `copy --verify-only` against an empty dest -> exit 6 (FAILED).

Mutating (scratch base / copy-dest):
- `copy --verify-only --from <real> --to <scratch-base>` -> exit 0 SYNCED after seeding.
- `copy --from <real> --to <copy-dest>` (full write) -> exit 0, then a follow-up `--verify-only` confirms SYNCED.
- `optimize` (and `--only index`) against scratch-base -> exit 0.
- `sync` against a sandbox store with one tiny seeded adapter source -> exit 0, `+N sessions`.

Config-mutating (sandbox HOME/XDG):
- `init --yes` on an empty sandbox -> exit 0, writes `[adapters.*]`/`[storage]`; assert file contents.
- `init` legacy `[sources.*]` auto-migration -> seed a legacy config, run, assert it became `[adapters.*]`.
- `adapters list` / `enable <name>` / `disable <name>` -> exit 0, assert `enabled` toggles in the sandbox config.
- `creds list` (redacted), `creds delete <name>` -> exit 0.
- `storage use <scratch-url>` and a `use local` rollback -> exit 0, assert `[storage] path` flips in the sandbox config and no data moved.
- `schedule status` / `logs` -> exit 0; `schedule start`/`stop` under sandbox per the launchd caveat above.

Interactive wizards (PTY, sandbox HOME/XDG):
- `init` full wizard: answer the storage-location select, the schedule confirm, and the final "Write config?" confirm; assert the written config. Also a Ctrl-C-before-write run that asserts "Cancelled - nothing written" and a byte-identical seeded config (side-effect-free cancel, per `CLAUDE.md`).
- `adapters discover` multiselect: drive the picker; on an empty sandbox assert the "no adapters detected" bail; with a seeded fake source assert selection + persist.
- `creds add` hidden-secret prompt: drive the name input and the masked secret; assert the set is written and `creds list` redacts it.

## Timed operation report (required)

Beyond pass/fail, the harness records and reports the wall-clock time of every operation, with per-phase breakdown where the command exposes phases (open store, oracle, import, embed, optimize, plan, copy, verify). The headline timings the release needs:

- Cold full sync into an empty bucket: local adapters -> a fresh empty remote prefix `pond-e2e-sync`. This re-reads every adapter source and re-embeds the full backlog on-device (sync does not copy existing vectors), so it is the slowest operation and the most important release number (a new user's first sync to a fresh remote). Report total plus import/embed/optimize split.
- Copy prefix -> prefix: `pond copy --from <real-corpus-prefix> --to pond-e2e-copy-dest` in the same bucket. Embeddings ride along as data (no re-embed), so this is bandwidth/commit-bound, roughly 14 minutes per the perf notes. Report total plus copy/index/verify split.
- Sync on appends: re-run `pond sync` against `pond-e2e-sync` after the cold sync. With nothing new at the sources this is the incremental near-no-op path; report total (expected seconds, dominated by the change-detection oracle).
- Status: `pond status` and `pond status -v` on the full real corpus. Report cold and warm.
- Searches: a fixed set of `pond search` queries (vector and fts) on the full real corpus. Report cold (first query pays the index prewarm, 175-442 s per the remote-read perf notes) and warm per-query.
- Get invocations: `pond get --session-id` and `pond get --message-id` on the full corpus. Report per-call.
- SQL queries: a fixed set of `pond sql` queries (count, group-by, contains_tokens) on the full corpus. Report per-call.

Each timing is captured by the harness (wall-clock around the spawned binary) and printed in the final report as a table: operation, target, cold/warm, total, phase split. Cold vs warm is controlled by whether a prior invocation in the same run already warmed the remote cache for that store.

## Seeding and runtime

- Seed step (once, up front): `pond copy --from <real> --to pond-e2e-base`. A full corpus copy is roughly 14 minutes per the sync/copy perf notes; accepted per decision (full, not a subset). The `copy` write test seeds `pond-e2e-copy-dest` during the run (another full copy). Total wall-clock is dominated by these two copies plus the cold remote prewarms.
- The binary under test is the freshly built `target/release/pond` at the current HEAD (`6604fcf`), rebuilt because the pre-existing release binary predated that commit.

## Invocation and gating

```
python ops/e2e/run.py \
  --bin target/release/pond \
  --url s3+https://nbg1.your-objectstorage.com/pondarium/pond-full-corpus-benchmarking-copy \
  --config ~/.config/pond/config.toml \
  --scratch-prefix s3+https://nbg1.your-objectstorage.com/pondarium/pond-e2e
```

Not run by `cargo test` or CI (remote creds + cost + runtime). Optionally surfaced as a `moon run e2e` wrapper. The harness exits non-zero if any check fails and prints the failing matrix rows.

## Deliverables

- `ops/e2e/run.py` - the PTY driver and check table.
- `ops/e2e/README.md` - how to run it, the safety model, the three contexts.
- This plan, committed first.
- A completion report: the pass/fail matrix for the full surface against the seeded corpora.
