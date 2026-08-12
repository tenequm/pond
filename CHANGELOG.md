# Changelog

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
