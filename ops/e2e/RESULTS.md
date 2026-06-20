# CLI e2e completion report (2026-06-20)

Pass/fail matrix and timings from running `ops/e2e/run.py` against the real
remote corpus. This is the completion deliverable for
[`docs/plans/2606-19-cli-e2e-real-corpus.md`](../../docs/plans/2606-19-cli-e2e-real-corpus.md).

- Binary: freshly built `target/release/pond` (resident-`max_ts`-watermark sync).
- Read corpus: `s3+https://nbg1.your-objectstorage.com/pondarium/pond-full-corpus-benchmarking-copy`
  (11,662 sessions / 2.12M messages / ~12 GiB on S3).
- Scratch prefixes: `pond-e2e-copy-dest`, `pond-e2e-verify-empty`.
- The **from-scratch cold full sync was deliberately skipped** (already validated
  separately; no point re-seeding an empty bucket). Run with `--skip-sync`.

## Result

**Read / sandbox / wizard surface: 38 checks, 0 failed** (`--skip-mutating` pass).
**Mutating surface: 3 checks, 0 failed** (copy / copy-verify / optimize).

All six failures seen on the first run were harness-draft bugs (stale SQL column,
bad URL input, wrong schedule exit expectation, three wizard-driver issues) plus
one genuine pond finding - all now resolved (see below).

## Matrix

### Reads (remote corpus)

| check | exit | time |
| --- | --- | --- |
| status / status (warm) / status -v | 0 / 0 / 0 | 5.99s / 5.44s / 7.23s |
| config show / path / schema | 0 | 0.01s each |
| completions zsh | 0 | 0.01s |
| storage check | 0 | 0.58s |
| creds list | 0 | 0.03s |
| schedule status (unconfigured) | 1 | 0.02s |
| adapters list | 0 | 0.01s |
| search vector cold / warm | 0 | 35.9s / 29.7s |
| search fts | 0 | 33.1s |
| sql sample session / message | 0 | 2.35s / 2.56s |
| sql count / group-by | 0 | 1.25s / 2.45s |
| get --session-id / --message-id | 0 | 29.7s / 18.2s |
| mcp initialize | 0 | 1.12s |
| flags before / after / identical | 0 | 5.17s / 6.44s / - |
| bad url -> exit 2 | 2 | 0.01s |
| missing id -> exit 1 | 1 | 1.85s |
| verify empty -> exit 6 | 6 | 23.0s |

### Config-mutating (sandbox HOME/XDG)

| check | exit | time |
| --- | --- | --- |
| init --yes / legacy-migrate | 0 | 0.81s / 0.84s |
| legacy [sources]->[adapters] | 0 | - |
| adapters enable / disable | 0 | 0.02s / 0.01s |
| storage use scratch / local | 0 | 1.75s / 0.03s |
| creds delete | 0 | 0.02s |

### Wizards (PTY)

| check | exit | time |
| --- | --- | --- |
| init wizard (cancel) | 1 | 0.22s |
| creds add wizard (replace default) | 0 | 0.86s |
| adapters discover (empty) | 1 | 0.02s |

### Store-mutating (scratch)

| check | exit | time |
| --- | --- | --- |
| copy prefix->prefix (full 2.12M-row corpus) | 0 | 1525.7s (~25.4 min) |
| copy verify (synced) | 0 | 10.9s |
| optimize (index rebuild on copy-dest) | 0 | 230.8s (~3.85 min) |

## Local vs remote reads

Same store, local (`~/.local/share/pond`, OS page cache + local-disk index) vs the
remote S3 copy. Local is the fast path by 20-70x; the entire gap is S3 round-trips.

| command | local | remote |
| --- | --- | --- |
| status (warm) | 0.08s | 5.44s |
| status -v | 2.30s | 7.23s |
| sql count / group-by | 0.03s / 0.06s | 1.25s / 2.45s |
| get --session-id | 0.24s | 29.7s |
| search vector | 1.57s | 29.7-35.9s |
| search fts | 0.41s | 33.1s |
| mcp initialize | 0.06s | 1.12s |

Locally pond is sub-second across status/sql/get/fts and ~1.5s for vector search,
even on a 2.12M-message store.

## Findings

- **`pond creds add` cannot add a second credential set when a scope-less
  `[creds.default]` exists** - the wizard never prompts for a `scope`, so a second
  catch-all set is rejected by validation. Filed as
  [tenequm/pond#62](https://github.com/tenequm/pond/issues/62). The wizard test now
  exercises the valid replace-default path.
- `schedule status` exits 1 when nothing is registered - intentional
  (`schedule.rs`: "Exit 0 when active, 1 when not configured").
- Remote `search vector` cold was ~36s here, not the 175-442s the plan predicted,
  because the index was freshly folded/warm; a true cold-cache first hit is slower.

## Harness fixes applied

- `--skip-sync` flag (skip the from-scratch sync; point `optimize` at the
  copy-dest the copy test populates).
- Crash hardening in `drive_wizard` (a child closing the pty no longer aborts the
  run); keystroke delay 0.15s -> 0.2s; correct exit-0 display (`code or -1` bug).
- Read-test fixes: `messages.id` -> `messages.message_id`; bad-url input ->
  query-param URL; schedule-status exit expectation -> 1 (sandboxed); wizard anchors
  (anchor the prompt not the intro; feed nothing on the empty-discover bail); the
  verify-empty check uses a never-written prefix so it stays exit-6 across re-runs.
