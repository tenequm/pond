# herdr-pond v1: sync-on-idle + the read-only session desk (2026-09-24, rev 3)

Goal: ship two features. (1) **Sync-on-idle** - every herdr agent session is in the pond store seconds after its agent goes idle. (2) **The desk** - one herdr overlay listing recent sessions across all harnesses and machines, searchable by message content, with transcripts read straight from the store over HTTP - no local files, ever.

Deliberately cut from v1 (parked, not rejected): resume, fork, hand-off, park, gone-row resurrection, the `$pond` sidebar token, standalone (non-herdr) packaging, marketplace distribution. The full-featured successor spec is [2609-02-herdr-pond-plugin-spec-and-plan.md](2609-02-herdr-pond-plugin-spec-and-plan.md) (tracks [#219](https://github.com/tenequm/pond/issues/219)); this v1 is its read-only core. Rev 2 folded in three research reports (herdr surface, ratatui 0.30.2, pond codebase). Rev 3 folds in an adversarial review (gpt-6-astra, 2026-09-24, all findings re-verified against source) plus the operator's resolutions: sync waits on the lock instead of skipping, serve is behaviorally read-only with explicit loopback bind, acceptance narrowed to what the design can actually guarantee, JSON results as a new `sql::Outcome` variant.

This document is self-contained for fresh implementation agents. Where it cites `file:line`, the line numbers were verified 2026-09-24 against: herdr `~/pjv/herdrdev/herdr` @ `d11c0c34` (= 0.9.1), ratatui `~/pjv/ratatui/ratatui` tag `ratatui-v0.30.2` (the checkout may be newer - use `git show ratatui-v0.30.2:<path>`), and this repo @ main. Re-verify before relying on exact lines. References to memex/herdr-navigator are behavioral observations, not code an agent can read - do not block on them. Implementing agents MAY load the `/rust-dev` skill as background; repo conventions and this document take precedence wherever they differ.

## 1. Decisions (all resolved; do not reopen)

1. **herdr-only TUI, separate crate, no TUI in pond.** `packages/herdr-pond` (workspace member, `publish = false`, bin `herdr-pond`), ratatui. Spec 2.3's "no UI" stands for pond core. All herdr calls live in one module so a standalone desk later is a fallback impl plus packaging, not a rewrite.
2. **The desk's data plane is `pond serve` over localhost HTTP** - search ranks (`/v1/search`), SQL lists and reads (`/v1/x/sql`, new), get-session deferred (see decision 9). No new typed endpoints; the `/v1/sessions` listing endpoint and an empty-query search relaxation were both considered and dropped (SQL covers the listing and is the only surface reaching the ingest-host stamp in `options.pond`; search stays strict).
3. **`/v1/x/sql` is always on.** The `x/` prefix means "outside the stable wire contract"; spec.md:676 ("not an HTTP operation") is edited, 7.5 gains the operation marked unstable, and 7.2's additive-evolution guarantee gains an explicit `/v1/x/` carve-out (today 7.2 states the guarantee with no exception, spec.md:637). Coupling to storage schema is acceptable: herdr-pond is first-party, same repo, versions in lockstep. Posture matches the field (Arrow Flight SQL, Trino, lance-namespace `query_table`): results self-describe (column names in-band), the `schema://pond-sql` resource text is the discovery surface, the protocol is versioned and the data schema explicitly is not. Tenant scoping ([#166](https://github.com/tenequm/pond/issues/166)) is FUTURE work for this endpoint, not free compatibility: scoping SQL means auditing every provider path (raw dataset providers sql.rs:573, the ranked-FTS provider sql.rs:614), metadata/EXPLAIN exposure (bare EXPLAIN can leak whole-table stats), and auth routing. State that in the spec edit; do not claim the endpoint composes with #166 unchanged.
4. **Plugin-owned serve, bounded to the herdr server's lifetime, behaviorally read-only.** The plugin must not alter the operator's sync arrangement (`pond schedule` stays the sync owner; the idle hook adds scoped syncs that share the same per-host lock and update the same sync cursors - that is feature 1 working, not interference). The plugin's serve never passes `--with-sync` (no periodic sync task, main.rs:1783) and the plugin only ever sends read queries - but the process is NOT hard read-only: `/v1/ingest` is always routed (transport.rs:158), store open can heal/create/migrate (substrate.rs:5044,5058,5431), and prewarm touches local caches. Two consequences the code must honor: every plugin-owned spawn serves only on a Unix socket (`pond serve --socket <path>`, created owner-only 0600, #311) with `POND_HOST`/`POND_PORT` stripped from its env (clap counts an env value as given, which would conflict with `--socket`), and docs describe the serve as "personal owner-only socket server, plugin sends only reads" - never "read-only server". A hard `--read-only` serve flag was considered and deferred (add it when a second consumer wants the guarantee). Lifecycle: section 5.6; fallback: the desk spawns its own child serve (5.7). Cold cost is paid once per herdr server, in the background, with warm-up queries.
5. **Two PRs.** PR1 = pond-side `/v1/x/sql` (a `feat`, rides the release train); `pond serve --socket` followed in #311. PR2 = the herdr-pond crate + manifest + CI wiring. Nothing in PR2 compile-depends on PR1 (the desk can shell out to `pond sql` in dev until PR1's release is installed).
6. **Sync-on-idle enabled by default**, config gate to disable. Busy store lock = WAIT, not skip: the detached worker runs `pond sync <adapter> -q` WITHOUT `--no-wait` and blocks on the per-host flock until the running sync finishes (`--no-wait` exits 0 "skipped" on a busy lock, main.rs:4258 - with it, a codex-idle during a claude sync would be silently discarded). Trailing-edge coalescing per 5.5 guarantees the last idle event always produces a sync.
7. **Plugin id `pond`**, action/pane id `desk` (qualified action `pond.desk` - short for keybindings; local ids cannot contain dots, manifest.rs:600-608), binary and crate `herdr-pond` (the binary must not be named `pond` - PATH shadowing).
8. **JSON results are a new `sql::Outcome::Json` variant**, not a parallel entrypoint - one result type, and the three exhaustive `Outcome` consumers outside the SQL module (main.rs:2013, tests/integration/schema_migration.rs:111, benches/sync_oracle_bench.rs:144) gain one arm each and move into agent A's ownership.
9. **`pond_get_session` stays out of the desk until [#284](https://github.com/tenequm/pond/issues/284)/PR #293 lands.** Measured 2026-09-24 (pond 0.19.1, live S3 store, via MCP): get_session ~6-7s; the same messages via session-scoped SQL: 1.8s first / 0.27s warm. The delta is get_session's find_session leg plus parts-summary hydration - exactly what M2 removes. When #293 merges, re-measure; at ~1-2s switch the pager to it for tool/file one-liners.

## 2. Measured numbers the design rests on (2026-09-24, pond 0.19.1, live store `s3+https://nbg1.../pondarium/pond`, ~20k sessions / ~4M messages)

| Call | Latency | Desk use |
|---|---|---|
| SQL listing, unscoped GROUP BY over messages | 28.6s cold / 12.9s warm | all-time toggle ONLY (loading state + raised timeout) |
| SQL listing, 14-day timestamp scope | 4.0s first / 1.7s warm | opening view (cached, background refresh) |
| SQL transcript, session-scoped, 10 rows | 1.8s first / 0.27s warm | preview + pager |
| `pond_search` fts, warm process | <1s | typed search |
| `pond_get_session`, 10 messages | ~6-7s | not used in v1 |

Unmeasured: the first fts search in a fresh serve process ([#165](https://github.com/tenequm/pond/issues/165) item 2 historically 47-300s cold; serve runs `spawn_prewarm` at startup, main.rs:1744-1801 - note prewarm warms the token dictionary and a hot token, not every future search's postings, sessions.rs:1928). The serve-daemon's warm-up (5.6) exists precisely to absorb this; measure during dogfood.

Query discipline (from the [read-latency campaign](2609-17-read-latency-campaign.md) sql audit - unscoped messages GROUP BYs are the dominant timeout family): listing scans narrow columns only (`session_id`, `timestamp`, `source_agent`) with a `timestamp >=` bound the zonemap can prune (never arithmetic on the column side), project filter pushed down, subagents excluded via `source_agent NOT LIKE '%/%'`, and an explicit SQL `LIMIT` in EVERY query (the server's inline caps apply AFTER full collection, sql.rs:232,1206 - they are presentation limits, not scan bounds; the HTTP `limit` field alone does not bound server work). JSON getters (`options.pond` host, titles) run only per visible page (`session_id IN (...)`), never corpus-wide. ~10.6k pre-stamp sessions have no host stamp; a missing stamp means UNKNOWN provenance, not local (spec 4.8, and per-message stamps can differ within a session, spec.md:433) - render unstamped as a dim `local?` fallback and say so in the README. Backfill stays out of scope.

## 3. PR1 - pond: `POST /v1/x/sql` (agent A)

### 3.1 What exists (verified, packages/pond/src)

- Router: `transport.rs:139-162` - routes at :155-158, then `.layer(DefaultBodyLimit::max(HTTP_BODY_LIMIT_BYTES))` (8 MiB, :103), `.with_state`, `.nest_service("/mcp", ...)`. New route goes beside the others, before `.layer`. The `/mcp` Host allowlist does NOT cover `/v1/*` (:105-115). `/v1/ingest` is always registered (:158).
- Handler pattern (`search`, transport.rs:267-279): `State(state), Json(mut request)` -> `let _activity = state.track_activity();` -> `request.namespace.get_or_insert_with(default_namespace)` -> call the transport-agnostic handler -> `status_for(&code)` (:335-345) -> `with_request_id(...)` (:326-331). The request id is attached only after successful extraction; axum's JSON/missing-route rejections are NOT pond envelopes - clients must handle both shapes (6.3). Protocol/namespace validation lives in the handler (`validate_protocol` wire.rs:770, `resolve_namespace` handlers.rs:29-36 - accepts only `None`/`"local"` today), never in the transport (spec 7.1). Namespace must be validated BEFORE any dataset open, including for table-free queries (`SELECT 1`).
- SQL core: `sql::run(tables, sql, mode, inline_rows, timeout_secs)` (sql.rs:161-302). Read-only layer 1 `parse_and_gate` (:335-366, exactly one Query or EXPLAIN-of-Query), layer 2 `SQLOptions` all-false (:213-218), fresh `SessionContext` per call. The timeout applies to `collect` only (:232) - it is an execution timeout, not an end-to-end HTTP deadline; row limiting happens after full collection. Timeout default 30 / max 600 (:55-58). Inline caps: 100 rows default, 1000 max, 80k-byte budget, 1000-char cells (the char clip is ASCII-rendering-specific, :1132). The timeout error text lives in `run` (:235-250).
- **Gap:** `sql::Outcome` is only `Inline(String)` or `Export{..}` bytes (:103-108) - no JSON rows mode. Export conversion (`displayable`, :1106) renders JSONB as JSON *text* and silently drops residual binary/FSL columns; explicit `vector` projection is rejected separately (:175). Docstrings at :159-160 and :171 mentioning a JSON mode are stale.
- Wire house style (wire.rs): requests carry `protocol_version: u16` + `#[serde(default)] namespace: Option<String>`; envelopes are `#[serde(untagged)] enum XEnvelope { Success(XResponse), Error(ErrorEnvelope) }`; error constructors `wire::error(...)`, `From<crate::Error>`, `storage_error`. `DEFAULT_NAMESPACE = "local"` (:756).
- The "open only the referenced tables" `tokio::try_join!` block is duplicated in MCP (transport.rs:1175-1206) and CLI (main.rs:1987-2011); this third caller extracts it as `sql::open_tables(store, sql)`. Preserve BOTH the ordinary and ranked-FTS provider paths in the extraction.

### 3.2 What to build

1. `sql::Outcome::Json { columns: Vec<String>, rows: Vec<serde_json::Value>, row_count: usize, truncated: bool, elapsed_ms: u64 }` selected by a new `Mode` value. Semantics to freeze (golden examples in step 0, shared with B/C): one JSON object per row; arrow-json null semantics (null fields omitted - clients must not rely on key presence); JSONB columns arrive as JSON TEXT strings (the existing `displayable` conversion; the desk extracts scalars with SQL getters server-side, so nested decoding is not needed - do not build it); binary/vector columns rejected or dropped exactly as export does today (pin with a test); timestamps in RFC3339 with microsecond precision; `row_count` = rows RETURNED (post-cap), `truncated` = true iff the row cap or byte budget cut the result (a `truncated: false` response does NOT prove the query's own `LIMIT` didn't cut it - clients page by cursor, 6.3); caps reuse `DEFAULT_INLINE_ROWS`/`MAX_INLINE_ROWS` + the byte budget; `limit: 0` or > max -> `validation_failed`; timeout clamped to max with the clamp visible in the response error on breach. Bare `EXPLAIN` of a SELECT: allowed, returns its plan rows as data; `EXPLAIN ANALYZE`: must be rejected - the parse gate's Explain arm does not check the analyze flag (sql.rs:354), so add the check + test rather than assuming the gate covers it.
2. Update the three exhaustive `Outcome` consumers (decision 8). Benches are linted via `--all-targets` (packages/pond/moon.yml:44), so the bench arm is not optional.
3. Wire types: `SqlRequest { protocol_version, namespace, query (serde alias "sql"), #[serde(default)] limit: Option<usize>, timeout_seconds: Option<u64> }`, `SqlResponse { columns, rows, row_count, truncated, elapsed_ms }`, `SqlEnvelope` untagged. Errors: query-shaped failures -> `validation_failed` (400); infra -> `internal` / `storage_unavailable`. Extend the timeout text in `sql::run` with the HTTP field name.
4. `handlers::pond_sql(store, SqlRequest) -> SqlEnvelope` (validates protocol + namespace first; transport stays logic-free), route `.route("/v1/x/sql", post(sql))`, handler following the search pattern verbatim.
5. **Superseded by `pond serve --socket <path>` (#311).** The plugin never binds TCP: the serve listens on an owner-only (0600) Unix socket at a plugin-chosen path, and readiness is the capability probe (5.8) answering over that socket - serve opens the store BEFORE binding (main.rs:1768), so a socket file alone proves nothing. No port file, no port reservation race.
6. Spec edits: rewrite spec.md:676 (SQL now has one HTTP exposure, fenced), add the operation to 7.5 as a class (unstable, outside additive evolution, self-describing results, `schema://pond-sql` as discovery - note it is an MCP resource; HTTP-only consumers read the checked-in resource text in transport.rs), add the 7.2 carve-out (decision 3), and the tenant-future note (decision 3). This sets the `/v1/x/` convention - say so.
7. Tests: unit tests beside the code for the JSON mode caps, JSONB/binary/timestamp shape, EXPLAIN ANALYZE rejection, and gate behavior; HTTP tests in `packages/pond/tests/integration/transport_http.rs` (drives `router()` with `tower::ServiceExt::oneshot`; helpers at :67-112): success shape, DML/DDL rejected, bad namespace rejected before dataset open (`SELECT 1` + bad namespace), bad-JSON body (axum plain rejection - clients MUST send `Content-Type: application/json`), timeout mapping.

Known side findings, NOT in scope (file as separate issues): spec says `namespace_unknown` = 403 but `status_for` maps it 400; spec 7.5 claims search takes `format: text|json` but `SearchRequest` has no such field.

## 4. PR2 part 1 - workspace/CI mechanics (agent B, first commits)

All verified against this repo:

1. Root `Cargo.toml`: `members = ["packages/pond"]` is an explicit list -> add `"packages/herdr-pond"`. **Add `default-members = ["packages/pond"]`** - the dist builds run `cargo zigbuild`/`cargo build` from the repo root with no package selection (ops/scripts/build-dist.sh:53-55, ops/scripts/build-dist-msvc.ps1:23) and would otherwise compile herdr-pond into release/Windows builds. Two documented side effects: root `cargo test`/`clippy` then covers only pond (use `-p herdr-pond` or `--workspace`), and **the plugin build recipe must name the package**: `cargo build --release -p herdr-pond` (a bare root `cargo build --release` no longer builds it).
2. **Cargo.lock is owned by step 0, then by B.** The lockfile is CI-enforced (`--locked` in moon tasks packages/pond/moon.yml:44,47, dist scripts, and check-package-contents.sh:16), so the step-0 scaffold commits the full dependency lock update and must compile standalone. After step 0, only B touches `packages/herdr-pond/Cargo.toml` + `Cargo.lock`; C requests dependency changes through B. New deps invalidate pond moon caches once - acceptable, one-time.
3. Moon: new `packages/herdr-pond/moon.yml` (`language: rust`; format/lint/test tasks mirroring packages/pond/moon.yml incl. `--locked --all-targets` and the `nixToolchain` file group with `/ops/toolchain-id.json`; add the plugin manifest + `bin/` launcher to task inputs) + a `herdr-pond: 'packages/herdr-pond'` entry in `.moon/workspace.yml` `projects:` + add `herdr-pond:format herdr-pond:lint herdr-pond:test` to the explicit gate list at .github/workflows/ci.yml:122-127 (nothing gates the crate otherwise). Windows legs run explicit targets only - the crate stays untouched there. No devShell/toolchain-id change: the crate is pure Rust on the existing toolchain.
4. release-plz: add `[[package]] name = "herdr-pond"` with `release = false` to .github/release-plz.toml (Cargo.toml `publish = false` alone still gets tags per release-plz semantics). CI's version read filters on `pond-db` (ci.yml:524,602) and the publish tail enumerates pond assets only (ops/scripts/publish-release.sh:20,29) - both unaffected.
5. Crate conventions: `edition = "2024"`, `rust-version = "1.98"`, copy pond's `[lints]` table (unsafe_code deny, clippy::all deny + the warn set incl. `unwrap_used`/`expect_used`/`print_stdout`; tests use `#![allow(clippy::expect_used, clippy::unwrap_used)]`). No workspace-level lint blocks ratatui/crossterm; the CLAUDE.md "never crossterm" rule is pond-CLI-scoped - this crate is the deliberate TUI exception (note it in the crate's own doc comment). With `unsafe_code = deny`, all setsid/SIGTERM/flock work goes through the `nix` crate's safe wrappers - never hand-rolled `unsafe` pre-exec hooks.
6. **Do NOT depend on the pond crate** (pulls lance + datafusion + candle + protoc). herdr-pond is a thin HTTP client with its own mirrored serde structs.
7. **The launcher symlink**: `packages/herdr-pond/bin/herdr-pond` is a committed relative symlink whose target is `../../../target/release/herdr-pond` - THREE levels up (`bin/` -> `herdr-pond/` -> `packages/` -> repo root; `../../` lands at the nonexistent `packages/target/`). Herdr resolves manifest commands against the plugin root and does not repair symlinks (mod.rs:485-490). Assumes the default cargo target dir; a set `CARGO_TARGET_DIR` needs a manual link - document, don't handle. Works for `plugin link` dev; install-time distribution is parked.

Dependency block (versions verified against the ratatui tag and pond's lock where noted):

```toml
[dependencies]
ratatui = "0.30.2"                     # keep default features (crossterm_0_29 backend, layout-cache)
crossterm = { version = "0.29", features = ["event-stream"] }   # matches ratatui's backend + pond's lock TODAY (Cargo.lock: 0.29); the lockfile is the enforcement, not this comment
tokio = { version = "1.52", features = ["rt", "macros", "time", "sync", "signal", "process"] }  # lock has 1.52.3
futures-util = { version = "0.3", default-features = false }    # StreamExt::next on EventStream
reqwest = { version = "0.13", default-features = false, features = ["json"] }  # no TLS for localhost; lock has 0.13.4
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
anyhow = "1"
toml = "1.1"                           # plugin config.toml (pond pins 1.1)
nix = { version = "0.31", features = ["process", "signal", "fs"] }  # setsid, SIGTERM, flock - safe wrappers; 0.31.3 already in the lock
unicode-width = "0.2"
textwrap = "0.16"                      # pager pre-wrap (ratatui's reflow module is private)

[dev-dependencies]
tokio = { version = "1.52", features = ["test-util"] }
```

CLI parsing: hand-rolled `match args` over the five fixed subcommands (`open|tui|hook|serve-daemon|--owner`) - no clap; ANSI stripping: a small in-house scanner (state machine over CSI/OSC), no extra crate. Both decided here so C never has to edit B's manifest.

## 5. PR2 part 2 - the herdr integration shell (agent B)

### 5.1 Manifest (`packages/herdr-pond/herdr-plugin.toml`)

```toml
id = "pond"
name = "pond"
version = "0.1.0"
min_herdr_version = "0.9.1"   # floor for: agent focus moves clients, plugin-pane PWD fix, registry survives client-only updates
description = "Search and read every agent session from your pond store - all harnesses, all machines."
platforms = ["macos", "linux"]

[[actions]]
id = "desk"
title = "pond: session desk"
contexts = ["pane", "workspace"]      # metadata only in 0.9.1, nothing filters on it
command = ["bin/herdr-pond", "open"]

[[panes]]
id = "desk"
title = "pond desk"                   # becomes the pane label - the dedupe key in `pane list`
placement = "overlay"
command = ["bin/herdr-pond", "tui"]

[[events]]
on = "pane.agent_status_changed"
command = ["bin/herdr-pond", "hook"]

[[startup]]
command = ["bin/herdr-pond", "serve-daemon"]
```

Verified manifest facts (herdr @ 0.9.1, src/app/api/plugins/manifest.rs):

- Relative `command[0]` containing `/` resolves against the **plugin root** for actions, hooks, AND panes (mod.rs:485-490, plugin_command.rs:12-27; changelog 0.8.0 #1949). Only argv[0] is rewritten - later relative-path arguments resolve against the process cwd. Hooks START in the plugin root (plugin_command.rs:8) while panes start elsewhere - resolve config/binary paths ABSOLUTELY, never relative to cwd. A bare name (no slash) is a PATH lookup. `$VARS` in `command` are never expanded (no shell).
- Local ids: no dots (manifest.rs:600-608); qualified action = `pond.desk`. Unknown `[[events]] on` names are a link-time WARNING, not an error - they silently never fire, so spell `pane.agent_status_changed` exactly.
- Overlay = split of the focused pane with the tab zoomed; needs an active workspace + focused pane; closes when the process exits, then restores previous focus/zoom (custom_commands.rs:440-520, api.rs:472-508).
- Keybinding is the only real invocation surface (no in-app action palette in 0.9.1): README must ship `[[keys.command]] key = "..." type = "plugin_action" command = "pond.desk"` + `herdr server reload-config`.

### 5.2 Runtime env facts the code relies on (runtime.rs:16-181, panes.rs:236-270)

- All plugin processes get `HERDR_SOCKET_PATH`, `HERDR_BIN_PATH`, `HERDR_ENV=1`, `HERDR_PLUGIN_ID/ROOT/CONFIG_DIR/STATE_DIR`, `HERDR_PLUGIN_CONTEXT_JSON`. Startup hooks DO get the socket path (the watchdog depends on this). State/config paths are keyed by plugin id only, NOT per herdr server (plugin_paths.rs:21) - two herdr servers share them, which is why 5.6 keys its own files by socket identity.
- In an **event hook**, `HERDR_PANE_ID` and `context.focused_pane_*` describe the EVENT's pane (the agent's pane). In a **pane**, `HERDR_PANE_ID` is the plugin pane itself; the underlying pane's cwd is `context.focused_pane_cwd // .workspace_cwd` (context computed before spawn, panes.rs:51). Do not mix these up.
- `HERDR_PLUGIN_EVENT_JSON` for our event (derived from the serde types, events.rs:361-365, 540-552 - confirm against one live capture during integration, but B's CI/dev progress must not depend on a running herdr): `{"event":"pane_agent_status_changed","data":{"type":"pane_agent_status_changed","pane_id":"...","workspace_id":"...","agent_status":"idle","agent":"claude",...}}` - snake_case with underscores (the env var `HERDR_PLUGIN_EVENT` uses the dotted form). `agent` is omitted when unknown. The event does NOT carry `agent_session`.
- `agent_status` values: `idle | working | blocked | done | unknown` (common.rs:158-166). `done` = finished-but-unseen: treat `idle` and `done` alike.
- PATH is the server's env from whenever the herdr server started - possibly minimal. Resolve the `pond` binary explicitly: absolute path from plugin config, else PATH lookup; on failure, one toast + a state-log line, never a crash loop.

### 5.3 Plugin config (`HERDR_PLUGIN_CONFIG_DIR/config.toml`, re-read per run)

```toml
sync_on_idle = true          # decision 6 gate
pond_bin = "/opt/homebrew/bin/pond"   # optional; absolute; default: PATH lookup
```

Malformed config: log to state dir, fall back to defaults, never crash. No pond store/config overrides in v1 - daemon, fallback serve, and the idle worker all use the operator's default pond config resolution, so they share one store AND one state root with scheduled sync (the sync lock and cursors live under the resolved state root, syncstate.rs:28,129 - "shared with scheduled sync" holds only because nothing overrides the defaults). Prerequisite (README): the machine has run `pond init` and has the relevant adapters enabled - explicit `pond sync <adapter>` refuses unconfigured/disabled adapters (main.rs:6118); the worker logs that outcome, it does not auto-enable anything.

### 5.4 Herdr CLI wrappers (`src/herdr.rs`)

All calls through `HERDR_BIN_PATH`; success responses are `{"id":"cli:...","result":{...}}` on stdout, but errors land on stderr with nonzero exit (cli.rs:745) - every wrapper captures exit status + stderr, not just stdout.

- `pane list [--workspace ID]` -> `result.panes[]`: `pane_id`, `workspace_id`, `label?`, `cwd?`, `agent?`, `agent_status`, `agent_session? {source, agent, kind: "id"|"path", value}`, ... . `agent_session` is present only when the official integration is installed (`herdr integration install claude|codex|...`); for claude, `value` == the pond claude-code session id (verified live). Live-row match: `agent_session.value == session_id`, or for `kind:"path"` a path whose file name contains it.
- `agent focus <target>` (target = unique agent name or pane id; marks seen, so `done` -> `idle` fires another event).
- `plugin pane open --plugin pond --entrypoint desk --focus [--env K=V]` -> new pane id at `.result.plugin_pane.pane.pane_id`. CLI `--focus` defaults to true. Errors incl. `ui_busy`, `plugin_disabled`.
- Dedupe on open: `pane list` scoped to the focused workspace, find `label == "pond desk"`, focus it instead of opening a duplicate; on any parse failure degrade to open. Use plain `pane close` if ever closing (the plugin-pane registry does not survive server restarts).
- `notification show <TITLE> [--body ...]` for error toasts from headless legs.

### 5.5 The `hook` subcommand (sync-on-idle)

Constraints (runtime.rs:218-266): every hook is a fresh process, un-serialized, un-timed-out; there is a GLOBAL cap of 32 in-flight plugin commands across ALL plugins (`plugin_command_limit_reached` beyond it - the operator's usagebar plugin already burns 2 slots per status event); herdr reads stdout/stderr to EOF before releasing the slot, so **every detached child MUST have stdin/stdout/stderr redirected to /dev/null or a state-dir log** - an inherited pipe end pins a slot for the child's whole runtime. Herdr's `plugin log list` "succeeded" proves only that the hook exited (runtime.rs:142), never that a sync ran - the worker's own state log is the sync record. Measured event rate on this machine: 104 hook runs / 35 min across 5 agents, min gap 302ms; the event also re-fires on presentation-only changes and on `done`->`idle` when a user views a finished agent.

The hook must exit in milliseconds (no tokio runtime on this path) and NO idle event may be silently dropped (a dropped trigger = data invisible until the next event or scheduled sync, indefinitely on schedule-less machines). Trailing-edge coalescing via a per-adapter worker:

1. Parse `HERDR_PLUGIN_EVENT_JSON`; exit 0 unless `agent_status` in {idle, done}.
2. Map `data.agent` -> pond adapter: claude->claude-code, codex->codex-cli, pi->pi-coding-agent, omp->oh-my-pi, opencode->opencode, grok->grok-build, hermes->hermes, letta->letta-code, agy->agy. Unknown or absent agent: exit 0. (Registry names verified: adapter/mod.rs:534-550; `known_names()` :560.)
3. Config gate `sync_on_idle` (5.3); disabled: exit 0.
4. Touch `STATE_DIR/pending.<adapter>` (mtime = now).
5. Try a NON-BLOCKING flock on `STATE_DIR/worker.<adapter>.lock`. Held: a live worker will pick the pending stamp up - exit 0. Acquired: release it, spawn `herdr-pond hook --worker <adapter>` fully detached (setsid via nix, stdio -> `STATE_DIR/sync.log`), exit 0.
6. **Worker** (detached, owns the flock for its lifetime): re-acquire the flock non-blocking (lost the race: exit - the winner covers pending). Then loop: read `pending.<adapter>` mtime; if <= last-handled, break; sleep ~2s (coalesce the burst - min observed gap 302ms), set last-handled = now, run `pond sync <adapter> -q` as a CHILD (waited on, stdio -> the same log; NO `--no-wait` per decision 6 - blocks on the shared per-host flock and runs serially after any scheduled sync), append one status line (adapter, duration, exit code). After the final check: release the flock, re-stat pending; if newer than last-handled, try to re-acquire and continue (failed re-acquire = a newer worker owns it - exit). This closes the release-window race where a hook touches pending just as the worker exits.

Failures go to `sync.log` (with a size cap - truncate at ~1 MiB), never stderr (hook stderr is a 64 KiB in-memory ring, lost on restart). Exit 0 unconditionally from the hook leg.

### 5.6 The `serve-daemon` subcommand (decision 4's lifecycle)

Startup hooks are one-shot commands run at server startup AND live handoff (bootstrap.rs:88,197), not supervised daemons, and they hold a command slot until their pipes close - so the hook process itself must exit immediately, and because hooks are unserialized (runtime.rs:121), every step is lock-guarded. All serve state is keyed per herdr server: `STATE_DIR/serve/<sockhash>/` where `sockhash` = short hash of the canonical (symlink-resolved) `HERDR_SOCKET_PATH` - two herdr servers on one machine get two serves; a serve never outlives its own herdr server and never kills another's. Files: `lock` (owner-lifetime flock), `endpoint` (the published record: `{socket, token}` - written atomically, temp + rename), `owner.sock` (the serve's Unix socket), `daemon.log`.

1. **`serve-daemon`** (the startup hook): take the flock NON-BLOCKING. Held: an owner is alive - exit 0. Acquired: release, spawn `serve-daemon --owner` fully detached (setsid, stdio -> `daemon.log`), exit 0. A free lock means no owner supervises whatever `endpoint` names, so a live endpoint is never adopted.
2. **`--owner`** (the detached watchdog): re-acquire the flock, non-blocking, and HOLD it for the process lifetime (lost: exit - another owner won). Probe `endpoint` under the lock; live: it is unsupervised (a dead owner's orphan) - log it and start a fresh serve that replaces the record. Remove any leftover `owner.sock` (a dead serve's, or that orphan's, so only the new child can answer there), then spawn `pond serve --socket <state>/serve/<sockhash>/owner.sock` as a waited-on child - never `--host`/`--port`, and with `POND_HOST`/`POND_PORT` stripped from its env (clap counts an env value as given, so an inherited one would conflict with `--socket`) - stdio -> `daemon.log` (serve prints to stdout and traces to stderr - never inherit). From THIS moment supervise concurrently: child exit, herdr liveness (connect `HERDR_SOCKET_PATH` every ~20s), and deadlines. Readiness = the socket exists AND the capability probe (5.8) passes over it, polled until a 180s deadline (store open on S3 comes first; a socket file alone proves nothing - a stale one refuses); then write `endpoint` atomically with a fresh random token, then fire warm-up requests once (the 14-day SQL listing + one throwaway fts search - the historical 47-300s cold FTS load is paid here, invisibly; deadline 300s, failure logged, not fatal). Steady state: the supervise loop. Herdr gone: SIGTERM the child, wait 10s, SIGKILL, reap, remove `endpoint` only if its token matches, exit. Child died unexpectedly: log, remove owned `endpoint`, exit (no restart loop in v1 - the desk fallback covers the gap; note as a follow-up). Deadline breach: kill child as above, log, exit.
3. Serve facts: `--socket <PATH>` (#311) serves the same routes over a Unix socket created mode 0600, is exclusive with `--host`/`--port`, removes a stale socket at PATH before the store opens and its own on graceful shutdown, and binds only after store open (main.rs:1768) - the probe over the socket is the readiness signal; `/v1/x/sql` checks the Host header against a loopback allowlist, so the client sends `Host: localhost`; a pond without the flag exits 2 at clap parse naming `--socket` (mapped to "too old"); the serve SIGTERM handler is installed after open+bind (transport.rs:192,243) and the 5s drain bounds the HTTP drain, not process teardown (transport.rs:117) - hence the wait-then-SIGKILL step.

### 5.7 The desk's connection logic (`src/serve.rs`)

Read `endpoint` for this server's `sockhash` -> capability-probe it (5.8) -> use it. Missing/dead/incompatible: spawn a desk-owned fallback child - `pond serve --socket <state>/serve/<sockhash>/desk.<pid>.<n>.sock` (one socket per spawn, so a retiring fallback's cleanup never removes its successor's), **stdio redirected to a state-dir log (NEVER inherited - serve's stdout/stderr would corrupt the TUI)**, wait until the probe passes over the socket (spinner + "opening store..." status; deadline 180s), use. Teardown: a drop-guard plus signal-path handling SIGTERMs the child, waits briefly, SIGKILLs - on every graceful exit path (normal quit, error return, SIGTERM/SIGHUP, panic via the hook). This is a graceful-exit guarantee only: SIGKILL of the desk orphans the child until it idles forever - documented accepted v1 risk (the next desk open finds no endpoint record for it and starts fresh; the orphan is visible in `ps`). Both paths hand `api.rs` a socket path; it builds one reqwest client per socket (`ClientBuilder::unix_socket`, base URL `http://localhost`). A refused or missing socket (ECONNREFUSED/ENOENT, reqwest `is_connect`) is a dead serve and triggers one re-resolve and retry; a timeout is not.

### 5.8 The capability probe (shared by 5.6/5.7)

`POST /v1/x/sql` with `{"protocol_version":1,"query":"SELECT 1 AS ready"}`, small timeout, and validate the success envelope. NOT `GET /v1/search` 405: a pre-PR1 pond also answers 405 there (transport.rs:155) and would then 404 every desk query, and any unrelated localhost service can 405. Probe answers: 200 + valid envelope = usable; 404 = pond too old - surface "pond >= <PR1 release> required (`brew upgrade pond` / `cargo install pond`)" as a toast/full-screen error; connection refused, socket missing, or timeout = dead endpoint.

## 6. PR2 part 3 - the desk TUI (agent C, `src/desk/` only)

### 6.1 Event loop

Single current-thread tokio runtime built by hand for the `tui` subcommand only (never `#[tokio::main]` - the hook path must not build a runtime). Loop: `tokio::select!` over crossterm `EventStream`, an `mpsc::UnboundedReceiver<Msg>` (HTTP results), a spinner `interval` gated on `is_loading()`, and SIGTERM/SIGHUP branches. Draw only on a dirty flag, never per-tick. Treat `EventStream` returning `None`/`Err` as pane-gone and exit. Ctrl-C arrives as a key event in raw mode - handle it in the key match. `ratatui::init()` installs a panic hook that restores the terminal (init.rs:566-572); the panic hook is NOT the normal cleanup path - restore explicitly on normal return, error return, and the signal branches.

### 6.2 Request lanes (debounce + cancel, correct under races)

One `Lane { gen: u64, task: Option<AbortHandle> }` per request kind: search (150ms debounce), preview (80ms - arrow-key-hold safe), transcript pages (none, single-flight: at most one page request outstanding per pager), listing (none - fired on open/filter change only, NEVER per keystroke). Every spawned task carries its generation; `apply()` drops any `Msg` whose gen mismatches the lane's current one (abort alone is insufficient - a task can send in the gap before abort lands). Per-lane generations alone are NOT enough across views: every `Msg` also carries a **view epoch** (bumped on every view/filter transition - entering search, clearing search, changing the project/time toggles, leaving a pager) and its **target identity** (session id for preview/transcript, cursor for pages); `apply()` drops epoch mismatches and results for a no-longer-selected session. Loading flags set on `restart`, cleared on the matching-gen result. Client-side abort does NOT stop server work (the SQL timeout bounds execution, sql.rs:232, and each query builds its own runtime budget) - which is why every desk query carries an explicit SQL `LIMIT`, lanes are single-flight, and debounce keeps abandoned-request rate low; add one paused-time test for cancellation against a slow mock.

### 6.3 Views and data

All SQL lives in `types.rs` as named constants, written and reviewed in step 0 (section 7), every query with an explicit `LIMIT`. Client-side HTTP: connect + request deadlines on every call; decode errors distinctly: pond error envelope (surface its enriched text verbatim in the toast), axum plain rejection (bad JSON/route - not an envelope), 404 (old pond, 5.8 message), refused/timeout (endpoint dead - trigger 5.7 fallback path once, then error state).

- **List (opening view)**: SQL listing (14-day window, project = the underlying pane's cwd from context JSON, toggles for all-projects/all-time), deterministic `ORDER BY last_ts DESC, session_id` + `LIMIT`, then one page-scoped hydration query - exactly one row per session: title = first nonempty user-role `search_text` (a `first_value` window emits per source row - reduce in the outer query), host via the JSON getter on `options` (exact getter syntax from the `schema://pond-sql` resource; strictly `session_id IN (<page>)`-scoped), counts = whole-session message counts (not window-scoped - say so in the header). Rows: state glyph (live/recent - live = `agent_session` match from one `pane list` snapshot per refresh), machine (unstamped -> dim `local?`, an assumption not a verified host - section 2), adapter, age, title, count. Missing title -> `(no user message)`. **All-time toggle** deliberately re-enters the slow family (12.9s warm, section 2): loading state + raised client deadline, and the listing stays cached so toggling back is instant. Refresh: manual key + on-open; a fresh desk process has no cache from the daemon's warm-up (that warmed the SERVER) - the first listing still takes the ~1.7s warm number. Selection preserved across refresh by session id.
- **Typed search**: `/v1/search`, fts, project filter; request/response shapes verified in wire.rs:583-689: request `{protocol_version: 1, query, mode?, sort_by?, filters?: {project: {contains}|{regex}, source_agent, from_date, to_date}, limit}`; response `{sessions: [{session_id, project, source_agent, session_messages_count, matched_message_count, matches: [{message_id, role, timestamp, text (<=600 chars), score, parts_summary?}]}], matched_total, searchable_in_scope, has_more}`. Render `searchable_in_scope == 0` distinctly ("filters excluded everything") vs zero matches.
- **Preview** (Space or selection-dwell): session-scoped SQL transcript, newest-first, cached per session id.
- **Pager** (Enter on non-live rows): chronological, **composite cursor `(timestamp, message_id)`** - never timestamp alone: pond orders messages by `(timestamp, message_id)` (handlers.rs:686) and the `schema://pond-sql` resource prescribes the exact seek predicate (transport.rs:717,728); timestamps tie, so `timestamp > last` loses tied rows and `>=` repeats them. Microsecond precision; same composite ordering in the query's `ORDER BY`; EOF = a page shorter than the page's own SQL `LIMIT` (the response `truncated` flag says nothing about the query's LIMIT). Lazy on scroll, single-flight. The pager is the complete paginated **conversational-text** view: `search_text` deliberately excludes tool calls/results, reasoning, and system/tool-role messages (spec.md:742, transport.rs:547) - label it so (e.g. footer `conversation only - tool bodies via pond_sql/get_session`), and never widen `search_text` to fix the wording.
- **Jump** (Enter on live rows): leave the alternate screen and restore the terminal BEFORE running `herdr agent focus` (CLI output must not corrupt the screen), then exit.

### 6.4 Widget facts (all verified at the ratatui-v0.30.2 tag)

- Layout: `let [a, b] = frame.area().layout(&Layout::vertical([...]))` works at 0.30.2 (rect.rs:588). Input: `user-input` example pattern, BUT compute the cursor x with `UnicodeWidthStr::width(&input[..byte_index])`, not char count (CJK/emoji are 2 cells). List: `List`+`ListState` (`select_next` etc. do NOT clamp until render - `select_last` sets `usize::MAX`; clamp `selected()` against `results.len()` before using it as an index), `highlight_spacing(HighlightSpacing::Always)`, `scroll_padding`. Preview: `Paragraph::new(..).wrap(Wrap { trim: false }).scroll((y, x))` - scroll is (y, x) order.
- **Pager must pre-wrap**: `Paragraph` has no virtualization; wrapped-scroll re-wraps everything above the viewport every frame and caps at u16 lines (paragraph.rs:405-475). Pre-wrap once on load/resize into `Vec<Line<'static>>` (textwrap + unicode-width; ratatui's reflow module is private), keep a `usize` offset, render only the viewport slice with no `.wrap()`/`.scroll()`. Scrollbar: `ScrollbarState::new(lines.len()).position(offset)` (builder methods taking `self`, scrollbar.rs:419,445 - there is no standalone `content_length` constructor).
- **Transcript hygiene**: ratatui silently drops control chars (span.rs:314) - expand tabs, strip `\r` and ANSI escapes with the in-house scanner (the ESC is dropped but `[31m` would render as text). Build one `Line` per source line (a `Span` containing `\n` loses it).
- Toast: `Clear` + bordered `Paragraph` on a bottom-right rect, rendered last. Mouse capture stays OFF in v1 (terminals translate wheel to arrows on the alt screen; capture would break text selection); verify wheel behavior inside herdr panes during dogfood.
- Main-only APIs to avoid (in the repo's examples but not on 0.30.2): `run_with_options`, `Table::scroll_padding`, `ScrollbarState::is_at_start/is_at_end`.

### 6.5 Testability

Keep `App::on_event(Event)` and `App::apply(Msg)` sync and pure over an injected `Api` trait; reducers emit effect values, the runtime layer (also in `desk/`) performs them and feeds `Msg`s back - so C stays wholly inside `desk/`. Agent C develops against the step-0 mock AND the step-0 fake-server fixture (a canned-response HTTP server binary in `tests/` - trait mocks alone let serialization drift while C's tests stay green). Frame assertions via `TestBackend` (`assert_buffer_lines` asserts internally and returns unit, test.rs:199 - unwrap fallible terminal ops, not the assertion; the backend error type is `Infallible`). Lane timing tests with `#[tokio::test(start_paused = true)]` + `tokio::time::advance`, including stale-message (already-enqueued, wrong epoch) and slow-server cancellation cases. Synthetic keys via `Event::Key(KeyEvent::new(...))`. Shell-level: fake `pond`/`herdr` scripts on PATH plus a sandboxed `HERDR_PLUGIN_STATE_DIR` drive `open`/`hook`/`serve-daemon` headlessly - including a deliberately long-running fake `pond` child to prove the hook's pipes close immediately (EOF observed) while the child runs on. Hooks exit 0 silently; actions exit 1 with one stderr line.

## 7. Execution: parallelized

Step 0 (serialization point, main session, ~45-60 min): one commit freezing the seams, **compiling standalone with the full Cargo.lock update**:

- Crate scaffold + the `bin/herdr-pond` symlink (three-up target, 4.7) + the full dependency block (section 4) + manifest.
- `types.rs`: row/response structs, the `Api` trait (async via boxed futures - object-safe so the mock and real client interchange; error type; `Msg` enum with view epoch + target identity), ALL SQL query constants (listing, hydration, preview, pager with the composite cursor, warm-up) with a note on single-quote escaping for interpolated values, and golden `/v1/x/sql` + `/v1/search` request/response JSON examples (shared contract with A - A implements to them, B/C parse them; a contract change during step 1 is a lead-owned edit propagated to both).
- The fake-server fixture skeleton (6.5) and module signatures.
- Workspace `Cargo.toml` members + `default-members`, moon/CI/release-plz wiring stubs.

After step 0: A branches from MAIN (PR1 contains no PR2 files; A works to the golden examples); B and C branch from the step-0 scaffold. Cargo.lock/manifest changes are B-owned thereafter (4.2); C requests through B. Never run workspace-wide `cargo fmt` against another agent's files in a shared checkout - each agent formats its own scope.

Step 1 (three agents, parallel worktrees, no compile-time dependencies between them):

| Agent | Scope | Files |
|---|---|---|
| A | PR1 whole: `Outcome::Json`, wire types, handler, route, `open_tables` extraction, spec edits, tests, and the three exhaustive-match consumers | `packages/pond/src/{transport,sql,wire,handlers,main}.rs`, `packages/pond/tests/integration/{transport_http,schema_migration}.rs`, `packages/pond/benches/sync_oracle_bench.rs`, `docs/spec.md` |
| B | plugin shell: `api.rs`, `serve.rs`, `herdr.rs`, `open`/`hook`/`serve-daemon`/`--owner`, config, manifest final, moon/CI/release-plz finalization, Cargo.{toml,lock} | `packages/herdr-pond/*` except `desk/`; `.moon/workspace.yml`, `ci.yml`, `release-plz.toml` |
| C | desk TUI per section 6, against the step-0 `Api` trait + mock + fake server | `packages/herdr-pond/src/desk/` only |

First dev step for B: a throwaway hook that dumps `HERDR_PLUGIN_EVENT_JSON` to the state dir, to confirm the envelope derived from serde (5.2) against a live event - useful validation, not a blocker for B's CI-testable progress.

Step 2 (integration, main session, ~1-2h): merge, wire C to B's client, `cargo fmt` / `clippy --workspace -- -D warnings` / `cargo test --workspace` + `moon run pond:lint pond:test herdr-pond:lint herdr-pond:test` green, `cargo build --release -p herdr-pond` + `herdr plugin link packages/herdr-pond`, add the keybinding, dogfood against the live store - INCLUDING executing the manifest's actual `bin/herdr-pond` entrypoint from the linked plugin (the symlink is only proven by herdr spawning through it).

PR order: PR1 (pond) first - independently reviewable, rides the release train; PR2 (crate + wiring) after. The desk dev-loops against `pond sql` CLI or a locally-built pond until PR1's release is installed.

## 8. Acceptance

1. One key opens the desk scoped to the focused project; warm target: list populated <=2s (measured against the live S3 store, warm daemon serve, this machine; a cold desk-owned fallback serve additionally pays store open - report the number, no hard bound).
2. Typing searches message content across every harness and machine; Space previews; Enter opens the full conversational transcript (labeled as such, 6.3); no step reads a harness file.
3. Enter on a live row focuses that pane without corrupting the terminal.
4. Hook: an agent going idle is searchable from another pane's `pond_search` within ~15s under no lock contention (with a concurrent sync holding the lock, the worker syncs immediately after it releases - verified by the sync.log timeline); the hook process exits <50ms; the detached worker holds no herdr command slot (verify via `plugin log list` showing the hook `succeeded` immediately while the fake long-running child still runs, 6.5); no idle event is dropped across the debounce/lock matrix (two adapters idle concurrently; same adapter twice within 10s; idle during a held store lock).
5. Serve lifecycle: herdr server start warms a serve on its owner-only Unix socket; two concurrent `serve-daemon` runs yield exactly one owner (flock test); `kill` of that serve self-heals on next desk open (fallback child); stopping the herdr SERVER (not merely detaching a client - closing a client deliberately leaves the background server and therefore the serve running, herdr README) leaves no `pond serve` process behind; `pond schedule` registration, timers, and config are untouched by any plugin path (sync cursors/last-sync state DO advance when plugin-triggered syncs run - that is feature 1, assert it happens rather than pretending it does not).
6. Deterministic failure matrix (fake server/processes + sandboxed state dir, 6.5): empty store; zero search matches vs `searchable_in_scope == 0`; old pond (404 -> upgrade message); endpoint refused/timeout -> fallback; server death mid-query -> toast + recover; malformed/stale endpoint file; tied-timestamp pagination (more ties than a page); huge single message vs caps; ANSI/tab/CRLF transcript; tiny terminal + resize mid-pager; EOF on the event stream; SIGTERM/SIGHUP restore the terminal and kill the fallback child; `agent focus` failure surfaces an error after restore.
7. `cargo clippy --workspace -- -D warnings` and tests green via moon; CI gates the new crate; the dist scripts build pond only; a fresh-checkout `moon run herdr-pond:lint herdr-pond:test` passes with only the committed lockfile.
8. The desk contains no SQL outside `types.rs`'s named constants; every query carries an explicit LIMIT; JSON getters appear only in page-scoped queries.

## 9. Risks and follow-ups

- **Fresh-serve first-search latency** is the one unmeasured number (section 2); the serve-daemon warm-up should absorb it - measure in dogfood, and if the warm-up itself is too slow, stagger it after startup.
- **No serve restart in v1**: an unexpectedly dead serve child ends the owner (5.6); the desk fallback covers the gap. If dogfood shows real churn, add bounded restart-with-backoff to the owner.
- **Fallback-child orphan on desk SIGKILL** (5.7): accepted, documented; revisit if it bites.
- **get_session ~6-7s is not regrowth** (diagnosed 2026-09-24: live layout 1-9 pages/column ~21h after the re-encode): one-shot reads pay an unfiltered `message_store_probe` scan across many fragments (~500 GETs, ~2s; tracked in [#310](https://github.com/tenequm/pond/issues/310)), and the MCP figure was most likely a first call (warm MCP 0.35-2.1s). Independent of the desk: the serve is long-lived, so it pays the probe once at prewarm.
- **M2 upgrade path**: after PR #293, re-measure get_session; at ~1-2s the pager switches to it.
- **SQL contract drift** breaks the desk at run time, not compile time: queries live in one constants block; surface the sql handler's enriched error text verbatim in the toast; the step-0 golden examples are the shared contract.
- **32-slot budget**: our hook is millisecond-exit and workers/daemons detach with closed pipes, but a future action storm shares the cap with other plugins (usagebar) - keep every headless leg fast.
- **RSS of the session-long serve**: observe in dogfood (pond has memory instrumentation); if heavy, an idle-linger/timeout mode on the daemon is the knob.
- **Parked**: resume/fork/handoff/park (2609-02 plan), `pond sessions` verb, gone rows, host backfill, hard `--read-only` serve flag (decision 4), install/marketplace distribution (`[[build]]` + release-archive decision, and the plugin-root symlink story for installs), Navigator integration (rejected for v1: its collect/open contract cannot do per-keystroke content search), spec/code mismatch issues from section 3.2. A rowmap-backed `session_summaries()` SQL table function for desk hydration (per-session count, first/last timestamp, title from the mmap rowmap): parked, not planned - the narrowed hydration queries plus the desk disk cache were judged enough; if ever revisited it is a SQL table function, never a new typed endpoint.
