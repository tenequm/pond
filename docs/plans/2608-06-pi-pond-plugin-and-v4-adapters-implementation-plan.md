# pi integration: pi-pond plugin, `pond resume` verb, v4 + SQLite adapters, fleet packaging - implementation plan (2026-08-06)

Goal: in ONE PR, ship the full pond <-> pi integration:

1. **Phase 1** - a new `pond resume` CLI verb (Rust, `packages/pond`) exposing the adapter serialize/restore face, plus a new installable pi extension package `packages/pi-pond` (TypeScript) that projects pond's four read-only recall tools into pi, supervises a managed pond process with its own sync loop, and adds a `/pond` search-and-resume command.
2. **Phase 2** - full bidirectional (parse + serialize) support in the `pi-coding-agent` adapter for pi's new harness-v2 storage: format-4 JSONL and the SQLite session backend.
3. **Phase 3** - fleet/sidecar deployment reference doc plus a runnable docker-compose example.
4. **Phase 4** - publish `pi-pond` to npm and verify registry install.

This document is self-contained for a fresh implementation agent: it carries the verified source-format facts, all settled design decisions, and pointers into both codebases. pi is cloned for reference at `~/pjv/earendil-works/pi` (refresh with `git pull`; never clone into this repo). All pi facts below were verified against pinned commit `6fb2d766aee340bed96fe81d55603c1670b3af2a` (main, 2026-08-06). Re-verify file paths against the clone before relying on line-level details.

Naming rule (settled): the user-facing verb and every user-facing surface say **"resume"** - CLI verb `pond resume`, help text, the `/pond` picker action, the fleet doc. The spec's internal concept keeps its existing **restore/serialize** vocabulary (`adapter-native-restore-lossless`, `RestoreFidelity`, `RestoredFile`); do NOT rename spec rules or Rust types. State the mapping once ("the `pond resume` verb invokes the adapter serialize/restore face") and use "resume" from there on.

## 0. Read first (pond side)

- `docs/spec.md` section 6 (adapters: bidirectional codec, no-synthesis seam, placement rules, integrity), 6.2-6.3 (restore is hub-and-spoke, lineage-complete restore, native vs foreign fidelity decided by the system), 4.8 (model honesty rules), 7.8 (CLI verbs - `pond resume` gets added here), 5.4 (`session-durable-copy`, `session-append-only-exception`).
- `packages/pond/src/adapter/mod.rs` - `AdapterFactory` (has the `serialize` face returning `Vec<RestoredFile>`) + `Adapter` traits, registry, shared helpers (`part_id`, `raw_record`, `config_path`, `write_restored_files`), `RestoreFidelity`, `test_support` (`assert_probe_default`, `assert_native_restore`).
- `packages/pond/src/adapter/pi_coding_agent.rs` - the existing v3 adapter this plan extends. Read its header comment and `serialize_session` (native = verbatim `options.source.raw_record` replay with honest downgrade to foreign; foreign = `pi_record` reconstruction emitting `"version": 3` rows).
- `packages/pond/src/adapter/jsonl.rs` - `JsonlTree` engine (freshness gate, bounded reads, torn-tail tolerance). The v4 JSONL reader rides this.
- `packages/pond/src/adapter/sqlite.rs` - read-only SQLite plumbing (hermes/openclaw/opencode use it; the pi SQLite reader is the next caller).
- `packages/pond/src/adapter/openclaw.rs` + its integration tests - closest precedent for a multi-format adapter (legacy JSONL + SQLite flip) and skip taxonomy. OpenClaw's session format is pi-lineage (same `id`/`parentId` entry tree), so its mapping decisions transfer.
- `packages/openclaw-pond/` - the TEMPLATE for `packages/pi-pond`. Read all of: `package.json`, `openclaw.plugin.json`, `moon.yml` (the `cache: false` install comment is load-bearing), `index.ts`, `src/config.ts` (managed|url modes), `src/mcp.ts` (official MCP SDK client; managed mode spawns `pond serve --transport stdio --with-sync`; url mode = streamable HTTP), `src/service.ts` (`PondController`: binary discovery, spawn, health, backoff, cleanup), `src/tools.ts` (four tools, vendored TypeBox schemas, byte-bounded text relay, typed forbidden/error output union), `src/schemas.ts`, `test/` (in-memory transport pair against a fake pond endpoint).
- `docs/plans/2607-23-nanoclaw-and-hermes-adapters-implementation-plan.md` - structural precedent for adapter work and for a plugin package landing in the same PR.
- Binding prior decisions: capture is "tail the harness's native storage", never a push API; plugins are read-tools-only surfaces over pond primitives (no memory slot, no auto-recall, no prompt hooks); `pond sync` never enables adapters (enabling is explicit: `pond adapters`, `pond init`, or `serve --bootstrap`).

## 1. pi context (verified 2026-08-06)

pi ("Pi Agent Harness", github.com/earendil-works/pi, ~85k stars, TypeScript/Bun monorepo, npm scope `@earendil-works`) is Mario Zechner's agent toolkit: `packages/coding-agent` (the CLI users run), `packages/agent` (harness core), `packages/ai`, `packages/tui`, plus experimental `packages/server`/`client`/`protocol`.

Two storage generations coexist:

- **Shipped today (v3)**: the coding agent writes one JSONL file per session at `~/.pi/agent/sessions/--<cwd-slug>--/<ISO-ts>_<uuid>.jsonl`. First line is a `{"type":"session",...}` header row; subsequent rows are typed records (`message`, `model_change`, `thinking_level_change`, `compaction`) linked by an `id`/`parentId` tree. Documented in `packages/coding-agent/docs/session-format.md`. Pond's existing adapter reads exactly this.
- **Harness-v2 (in flight)**: a ground-up durable-harness rewrite, design in `packages/agent/docs/harness-v2.md` (~3400 lines; sections 12-13 for session/storage). Implementation is parallelized via work packages tracked in that doc's section 20; storage (JSONL v4, SQLite backend) is largely DONE on main, while the runtime harness and the coding-agent migration are NOT (public `AgentHarness` methods still throw `HarnessNotImplemented`; v3->v4 conversion packages J4/J5 unclaimed as of the pinned commit). Consequence: v4/SQLite code and formats are real and testable on main, but no released pi writes them by default yet. See "format watch" in section 5.

pi's extension system (`packages/coding-agent/docs/extensions.md`, ~3000 lines - read it in full before writing the extension): TypeScript modules auto-discovered from `~/.pi/agent/extensions/` or project `.pi/extensions/`, loaded via jiti (no build step), npm deps supported via adjacent `package.json`. Distribution: `pi install npm:<pkg>` / `git:<repo>` / local path (`packages/coding-agent/docs/packages.md`; a package declares `{"pi": {"extensions": ["./index.ts"]}}` in package.json). pi has NO MCP support by design - extensions using `pi.exec`/`fetch`/SDK clients are its integration mechanism, so a private MCP client speaking to a local pond is idiomatic.

Extension API surface used by this plan (all documented in extensions.md):

- events: `session_start`, `session_shutdown` (lifecycle; do NOT start background processes in the factory - defer to `session_start`, clean up in `session_shutdown`, idempotent)
- `pi.registerTool({name, description, parameters (TypeBox), promptSnippet, promptGuidelines, execute})`
- `pi.registerCommand("pond", {handler, getArgumentCompletions})`
- `ctx.ui`: `confirm`, `notify`, `setStatus`, `custom()` (full-screen picker component), `pasteToEditor`
- `ctx.hasUI` (true in tui+rpc; false in json/print modes) and `ctx.mode` (`"tui"` gate for `custom()`)
- `ExtensionCommandContext.switchSession(sessionPath, {withSession})` - switches the live session to a JSONL file; read the "Session replacement lifecycle and footguns" section of extensions.md before using (stale-context rules)
- `pi.exec(command, args, {signal, timeout})`

## 2. Phase 1a: `pond resume` verb (Rust)

New CLI verb in `packages/pond/src/main.rs` (same clap surface as other verbs):

```
pond resume <session-id> --to <adapter> [--out-dir <dir>] [--format json]
```

Semantics (each maps to an existing spec rule - cite the rule id in code comments):

- Loads the stored session and its child sessions (those naming it in `parent_session_id`) and serializes all of them through the target adapter's serialize face - `adapter-lineage-complete-restore`. A graph nesting deeper than one level surfaces as a typed error, never a partial write.
- Fidelity is decided by the system, never the caller: target adapter origin == session `source_agent` -> native (value-complete lossless); else foreign (best-effort idiomatic). Report `actual_fidelity` per session in the output - the pi adapter already implements the honest native->foreign downgrade when `raw_record` is absent; surface that.
- `--to <adapter>` accepts any registered adapter name (this verb is generic; restore-to-openclaw etc. come free). Unknown adapter -> error listing registered names.
- `--out-dir` defaults to the current directory. Files are written via the existing `write_restored_files` helper (`adapter/mod.rs`). NEVER overwrite: if any target path exists, fail before writing anything, naming the existing path(s). Exit code distinct from not-found. (The pi extension exploits this for idempotent resume: "already restored" -> just switch to the named existing file.)
- `<session-id>` not found -> `not_found` error that teaches the next step (suggest `pond search`). Erased/denylisted sessions report as not found.
- Human output: per-session lines (id, adapter, fidelity, files written). `--format json`: one machine-readable document (session ids, fidelity, absolute file paths) on stdout - the extension consumes this.
- CLI only in this PR (resume is an operator action, mirroring ingest; MCP surface stays read-only per `mcp-read-only-heal-exception`'s framing). HTTP exposure deferred.
- Spec edit: add the verb to `docs/spec.md` section 7.8 verb list, phrased "resume a stored session into a client's native format (invokes the adapter serialize face; fidelity per `adapter-native-restore-lossless`)".

Tests: integration test resuming (a) a pi-origin fixture session natively (byte-value round-trip against the fixture - reuse `assert_native_restore` machinery), (b) a claude-code-origin fixture foreign into `--to pi-coding-agent`, asserting valid v3 output rows and `actual_fidelity: foreign`, (c) collision refusal, (d) lineage: parent with one child restores both files.

## 3. Phase 1b: `packages/pi-pond` (TypeScript pi extension package)

### Layout and wiring

Mirror `packages/openclaw-pond` by COPY, not by extracting a shared package (settled: extract on the third consumer, not the second). Package name `pi-pond` (verified free on npm 2026-08-06; openclaw-pond is unpublished - do not touch it).

```
packages/pi-pond/
  package.json          # name "pi-pond", version 0.1.0, deps: @modelcontextprotocol/sdk, typebox
                        # peerDependencies (optional): @earendil-works/pi-coding-agent (types only)
                        # {"pi": {"extensions": ["./index.ts"]}} so `pi install npm:pi-pond` discovers it
  index.ts              # extension entry: default-export factory
  src/config.ts         # managed | url modes (copy openclaw-pond, adapt config source - see below)
  src/mcp.ts            # official MCP SDK client; managed stdio child or url streamable HTTP (copy)
  src/service.ts        # PondController (copy; adjust spawn args - see below)
  src/tools.ts          # four tools; vendored schemas; bounded text relay (copy, strip openclaw scope layer)
  src/schemas.ts        # TypeBox schemas mirroring pond tool contracts (copy)
  test/                 # vitest; in-memory transport pair against fake pond endpoint (copy pattern)
  tsconfig.json / tsconfig.build.json / vitest.config.ts / LICENSE / README.md
  moon.yml              # copy openclaw-pond's verbatim shape: install (npm ci, cache: false - the
                        # comment explaining why is load-bearing, keep it), typecheck, test
```

Wiring: add `pi-pond: 'packages/pi-pond'` to `.moon/workspace.yml` projects; append `pi-pond:typecheck pi-pond:test` to the moon run list in `.github/workflows/ci.yml` (next to the openclaw-pond/hermes-pond entries).

### Managed pond process (capture + tools in one lifecycle)

Settled design: NO per-trigger `pond sync` exec children (each would cold-load the embedding model). The extension supervises exactly one pond child whose lifetime is the pi session:

```
pond serve --transport stdio --with-sync --bootstrap pi-coding-agent
```

- Serves the MCP read tools over stdio (what the bridge dials) AND runs pond's periodic in-serve sync loop (default `--sync-every 5`; do not override by default) with one shared embedding model. This flag combination is exactly what `openclaw-pond`'s managed mode already spawns - keep arg construction consistent with its `service.ts`.
- `--bootstrap pi-coding-agent`: when NO `[adapters.*]` entries exist at all, serve discovers and enables the pi adapter before the sync loop starts (spec 7.8; designed for plugin-managed sidecars). A disabled entry counts as configured and is never touched.
- Concurrent pi sessions each spawn their own child; pond's per-host sync flock + `--no-wait` semantics make overlapping loops skip cleanly. `url` mode (external `pond serve` over streamable HTTP) is the documented escape hatch for sharing one process/model.
- Lifecycle: start lazily on FIRST TOOL CALL (never in the factory, and not on `session_start` either - this is load-bearing, see below); stop in `session_shutdown`; health-check and backoff-respawn per `PondController`; missing binary produces a named fix ("pond not found - install: <one-liner>; then pond init"), never a raw ENOENT.
- Why lazy start is load-bearing: orchestrator extensions (notably pi-subagents, see section 9) spawn headless child pi sessions that load ambient extensions. With lazy start, a child that never touches pond tools costs one loaded extension and zero processes; a child that calls `pond_search` spawns a pond that is legitimately serving recall (a feature - subagents with archive access - not an accident). Concurrent children syncing is safe by pond's own design (per-host sync flock + `--no-wait`, cross-process OCC). Therefore: NO subagent detection, NO env-var sniffing, NO special cases - children get the same extension behaving the same way.
- Config: pi has no per-extension config schema. Read an optional JSON file `~/.pi/agent/pond-pi.json` (also the one-time-prompt state store, below) with `{mode: "managed"|"url", url?, binaryPath?}`; defaults: managed, binary from PATH.

### One-time capture-enable confirm (smooth UX/AX - settled)

`--bootstrap` covers the zero-adapters cold start. The uncovered case: user has other adapters enabled (e.g. claude-code) but `pi-coding-agent` is not enabled - sync runs but captures no pi sessions. The whole check-and-confirm flow runs ONLY in UI sessions (`ctx.hasUI` true) - headless sessions (json/print modes, orchestrator-spawned children) skip both the `pond adapters` exec and the prompt entirely. Flow on `session_start` in a UI session when pond binary is present and `pond adapters list --format json` (verify exact flag against `pond adapters --help`) shows pi-coding-agent absent/disabled:

- `ctx.hasUI` true: one `ctx.ui.confirm("Pond found", "Capture pi sessions into your pond archive?")`. Yes -> run `pond adapters enable pi-coding-agent` via `pi.exec`, notify success. No -> remember and never ask again.
- Remember either answer in `~/.pi/agent/pond-pi.json` (`{captureConsent: "granted"|"declined"}`). Never re-prompt once answered; enabling later remains possible manually.
- `ctx.hasUI` false: the flow never runs (see above) - no exec, no prompt, no notify. Headless capture setups use `--bootstrap`, `pond init`, or `pond adapters enable` directly.
- The extension NEVER writes pond config by any path other than this consented `pond adapters enable`.

### The four tools (same tools as the MCP tools - settled)

Register `pond_search`, `pond_get_session`, `pond_get_message`, `pond_sql` via `pi.registerTool`, following `openclaw-pond/src/tools.ts` faithfully: names identical, vendored TypeBox parameter schemas mirroring pond's tool contracts (bounds constants: search limit clamp, query char caps, 32KB relayed-response byte budget), execution = relay through the MCP client and return pond's own rendered text unmodified (pond's MCP surface is text; its descriptions, scope counts, and error messages that teach the next query pass through - that is the point). Differences from openclaw-pond, all simplifications:

- Drop `scope.ts`/`visibility.ts` (OpenClaw multi-agent scoping). pi is single-user; tools default to the WHOLE archive - cross-agent recall is the product. No default project filter.
- Drop the redaction layer only if it is OpenClaw-API-specific; keep byte bounding.
- `promptSnippet` (one line, appears in system prompt "Available tools"): "Search and read the archive of past agent sessions (Claude Code, Codex, OpenClaw, pi - all machines)". `promptGuidelines`: 2-3 bullets, each naming its tool ("Use pond_search when the user references past work or prior sessions...", "Use pond_get_session to read a whole past session found via pond_search..."). Keep them terse; heavy routing stays in pond's own tool descriptions.
- Tools require the managed process; if pond is absent, register nothing and notify once (a tool that always errors is worse AX than no tool).

### `/pond` command (settled: search picker with resume + insert)

`pi.registerCommand("pond", ...)`:

- `/pond <query>` (bare `/pond` prompts for input): run a search via the bridge, open a `ctx.ui.custom()` full-screen picker (gate on `ctx.mode === "tui"`; rpc/json fallback: print top hits as a notify) listing pond's grouped per-session hits (session title/name, source agent, date, top snippet).
- Enter = RESUME: `pi.exec("pond", ["resume", <session-id>, "--to", "pi-coding-agent", "--out-dir", <pi sessions root>, "--format", "json"])`, parse the file list, then `ctx.switchSession(<main session file>, {withSession})` (mind the extensions.md footguns: only use the fresh context inside `withSession`). `--out-dir` is the pi sessions root (`~/.pi/agent/sessions`) - the pi serializer emits `RestoredFile` relative paths carrying the `--<cwd-slug>--/` directory layout; verify that against `serialize_session` and `write_restored_files` at implementation time, and place files exactly where `SessionManager.list()`/`/resume` will find them. Collision error from `pond resume` -> parse the named existing path and switch to IT (idempotent resume, no error surfaced to the user).
- `i` = INSERT AS CONTEXT: `ctx.ui.pasteToEditor()` a compact reference block - session id, source agent, date, top snippet, and the literal line "Full transcript: use pond_get_session with id <id>". No transcript body is injected: the model pulls detail through the tools (pond stays out of curation - non-goal in spec 2.3).
- Esc closes. Footer: `ctx.ui.setStatus("pond", ...)` maintained across the session (managed process up/down, degraded states).

### Smooth-AX acceptance criteria (Phase 1 definition of done)

- Fresh machine, pond installed, zero pond config: `pi install` the package, open pi -> bootstrap enables the adapter, sync captures the current session's project within one sync interval, tools answer, `/pond` works. Zero manual configuration.
- pond binary missing: pi works normally; exactly one notify naming the install fix; no errors elsewhere.
- Adapter-not-enabled case: exactly one confirm, answer remembered.
- Every failure path names its fix (binary missing, store empty -> "run pond init", search returns 0 in-scope -> pond's own absence-honesty text relayed).
- Headless modes (rpc/json/print) never hang on UI: prompts skipped, tools still work.
- `session_shutdown` reliably reaps the child (no orphan pond processes after pi exits - test with repeated open/close).

## 4. Phase 2: full bidirectional v4 JSONL + SQLite support (settled: implement fully now)

One adapter, one brand: `pi-coding-agent` keeps its name and `source_agent`; v3 JSONL, v4 JSONL, and SQLite are three on-disk formats of the same source. Format detection per file/DB, not per config. Discovery: keep the existing `~/.pi/agent/sessions` probe for JSONL (v3 and v4 files share the directory layout); SQLite discovery per below.

### v4 JSONL format reference (verified against the pinned commit)

Source of truth: `packages/agent/src/harness/session/jsonl/codec.ts` + `types.ts`, entry/record unions in `packages/agent/src/harness/session/types.ts`. Regenerate this summary from source if the fixture suite ever diverges.

- Line 1 header: `{"kind":"header","version":4,"id":<string>,"createdAt":<ms epoch>,"cwd":<abs path>,"parentSessionId"?:<string>,"legacyParentSessionPath"?:<string>,"metadata"?:<object>}`. `parentSessionId` and `legacyParentSessionPath` are mutually exclusive. Session id charset `[A-Za-z0-9._-]` (uuidv7 by default). File naming: same `--<cwd-slug>--/<ISO-ts>_<id>.jsonl` scheme as v3 (`repo.ts` `sessionDirectoryName`/`sessionFileName`).
- Subsequent lines are mutations, discriminated by `kind`, all carrying `seq` (positive integer, total order):
  - `{"kind":"entry","lane":<string>,...entry}` - entry fields flattened inline: `id`, `type`, `parentId` (string|null), `seq`, `timestamp` (ms). Entry types (closed set at pin): `message` (carries `message`: an AgentMessage - roles user/assistant/toolResult plus custom types; same shapes the v3 adapter already maps), `model_change`, `thinking_level_change`, `active_tools_change`, `compaction` (summary, retainedTail, tokensBefore), `branch_summary`, `custom` (requires `customType`).
  - `{"kind":"record",...}` - harness orchestration records: `id`, `lane`, `type`, `seq`, `timestamp`, type-specific fields. Record types (closed set at pin): `operation_started` (intent kind run|compaction|navigation), `abort_requested`, `operation_finished`, `step_attempt`, `tool_started`, `queue_enqueued`, `queue_cancelled`, `write_deferred`, `usage`. Records have NO `parentId` and are not part of the conversation tree.
  - `{"kind":"lane","seq","lane":<string>,"leafId":<string|null>}` - lane pointer create/move. NO timestamp field.
  - `{"kind":"fact","seq","fact":"name","name":<string>}` and `{"kind":"fact","seq","fact":"label","targetId","label"?}` - session name / entry labels. NO timestamp field.
- Torn tail: a malformed FINAL line is a crash artifact (pi truncates it on load); malformed elsewhere is corruption. The `JsonlTree`/bounded-read plumbing already tolerates torn tails - keep that behavior for v4.

### v4 mapping rules (all existing spec rules, applied)

- Header -> `Session`: `id`; `parent_session_id` from `parentSessionId`; `project` from header `cwd` (real source data - better than v3's dir-slug decode; keep slug decode as fallback and record both in options); `created_at` from `createdAt`; `source_agent` stays `pi-coding-agent`; header `metadata`, `legacyParentSessionPath`, and the verbatim header into `options.source.*` (`model-lossless-projection`).
- `entry` mutations: `message` entries map exactly as v3 message rows do today (reuse `message_events`); `model_change`/`thinking_level_change`/`active_tools_change`/`compaction`/`branch_summary`/`custom` become the same System-carrier pattern the v3 adapter uses (human-meaningful field as content, whole record in `options.source.raw_record`, provenance injected).
- `record`, `lane`, `fact` mutations: placement-rule-3 carriers (System message, empty or type-label content, whole record in options, provenance injected). They are orchestration/injected facts, never conversational - `search_text` stays clean automatically.
- Timestamps: mutation `timestamp` where present; `lane`/`fact` mutations have none - use the session-anchor fallback (`model-no-synthesis` explicitly permits transport/absence defaults; keep source `seq` in options so log order is never lost). Ordering key stays `(timestamp, id)` with source-intrinsic tiebreaker; ids are uuidv7 (time-sortable), and `seq` is preserved in options.
- Unknown FUTURE kinds/types (v4 sets are strict today, but harness-v2 is moving): unknown entry/record type -> rule-3 carrier (the v3 adapter's existing catch-all pattern); unknown mutation `kind` -> rule-3 carrier of the whole line. Additive drift degrades losslessly instead of erroring; a diff in the fixture suite is the upgrade signal (`adapter-integrity-no-silent-drops` still applies to MALFORMED input - unknown-but-well-formed is preserved, unparseable surfaces as a typed error).
- Parse dispatch: peek line 1 - `{"type":"session"}` -> v3 path (unchanged), `{"kind":"header","version":4}` -> v4 path, other versions -> typed unsupported-version error naming the file.

### v4 serialize (native resume)

- Native (v4-origin session): verbatim `raw_record` replay in source order - same mechanism as v3 native, emitting the v4 header line then mutations. Round-trip conformance: parse committed v4 fixture -> canonical -> serialize -> value-equal to fixture (`adapter-native-restore-lossless`).
- Foreign (any other origin, including v3-origin pi sessions) resumes INTO pi as **v3 format** (settled): v3 is what every shipped pi loads today, and harness-v2 guarantees read-only v3 normalization (J4). Revisit emitting v4 only when pi ships v4 as its default write format. Document this choice in the adapter header comment.

### SQLite backend reader

Source of truth: `~/pjv/earendil-works/pi/packages/session-backends/sqlite-node/src/sqlite/` - `repo.ts` (repo + writer leases: 30s TTL, 10s heartbeat), `migrations/` (schema DDL), `storage/` (per-table modules: `sessions`, `entries`, `records`, `facts`, `lanes`, `session-sequences`, `session-stats`, `writer-leases`, `branch-entries` cache), `search-backend.ts` (FTS5 - ignore, derived). One database hosts many sessions; `PRAGMA journal_mode=WAL`, `synchronous=FULL`.

- Read-only open via the existing `adapter/sqlite.rs` plumbing (WAL-safe read-only mode; NEVER `immutable=1` - same rule as hermes). Tolerate an active writer (leases are pi's concern, not a reader's).
- Map `sessions` + `entries` + `records` + `facts` + `lanes` rows through the SAME canonical mapping as v4 JSONL (the row payloads are the same shapes; entries/records store JSON payload columns - decode and reuse the v4 mapping functions; a malformed row is a typed error, not a skip).
- Per-session ingest cursor: `seq` (monotone per session) - the freshness/watermark pattern used by the other SQLite adapters (`SourceWatermark`).
- Discovery: the coding agent does not yet write this backend, so there is no canonical default DB path at the pinned commit. Ship: (a) explicit config (`[adapters.pi-coding-agent] sqlite_path` or equivalent - follow the adapter config conventions in `mod.rs`), (b) `probe_default` checks the JSONL dir as today AND, at implementation time, re-check pi main for a wired default DB location (grep `sqlite` in `packages/coding-agent/src`); if one exists by then, probe it too. Do not invent a path.
- Serialize for SQLite-origin sessions: native resume replays raw records into a v4 JSONL file (a `.jsonl` is the portable resume artifact; writing into a live pi DB from outside would race the writer lease - out of scope, document in the adapter header).

### Fixtures and conformance (both formats)

- Generate fixtures by driving pi's OWN code at the pinned commit, not by hand: a small bun/vitest script in the pi clone using `JsonlSessionRepo` (`packages/agent/src/harness/session/jsonl/repo.ts`) and the SQLite repo (`packages/session-backends/sqlite-node`) to create sessions exercising: every entry type, every record type, lanes beyond `main`, name/label facts, a fork (`parentSessionId`), a torn tail, and a session with tool calls + usage records. Commit the resulting `.jsonl` files and `.sqlite` DB under `packages/pond/tests/fixtures/pi-coding-agent/` (follow `tests/fixtures/README.md` conventions - document the generation script inline there so fixtures are regenerable).
- Conformance per `docs/spec.md` 6.8: round-trip codec test per format (v3 already has one; add v4 and SQLite), `assert_probe_default`, integrity tests (no silent drops on malformed lines/rows, dedup), unknown-kind tolerance tests (inject a synthetic future mutation kind into a fixture copy; assert carrier preservation).
- **Format watch** (write this into the adapter header comment): harness-v2's J4/J5 (v3 normalization/conversion) and the coding-agent migration were unfinished at pin `6fb2d766a`. On each pi release until v4 ships as default: `git -C ~/pjv/earendil-works/pi pull`, re-run the fixture generation script, diff against committed fixtures. A diff is scheduled maintenance, not a surprise.

## 5. Phase 3: fleet/sidecar packaging

No new pond code - packaging and documentation of the existing primitives (env-only config `storage-configless`/`storage-env-mirror`, multi-writer OCC per spec 3.5, `serve --with-sync --bootstrap`, per-host sync lock).

- **Reference doc** `docs/references/2608-06-pi-fleet-capture.md` (match the references dir's existing naming/frontmatter conventions - check siblings first): the topology (per-worker pi + pond sidecar sharing a volume; central read-side `pond serve --transport http` on the same store URL), env-only configuration (`POND_STORAGE_PATH=s3://...`, `POND_CREDS_*`), per-tenant isolation as one store URL per tenant prefix (`s3://bucket/tenants/<t>` - hosted namespace routing stays deferred, spec 9.5), embedding split (workers ingest fts-only with embedding off; one central `pond optimize --only embed` cron), compliance (`pond erase` byte-purge + denylist, CLI/HTTP only), the honest loss window (a hard-killed worker loses mutations written after its last sync tick unless the volume outlives it; pi appends per mutation so the file is always current and torn-tail-safe), and security notes (pond serve HTTP is unauthenticated by design - bind localhost/private networks only; the integrator owns identity per spec 2.3).
- **Runnable example** `ops/examples/pi-fleet/` (match `ops/` conventions): `docker-compose.yml` + README with services: `minio` (store), `worker-pi` (pi coding-agent in `-p`/rpc mode running a scripted prompt), `worker-pond` (sidecar: `pond serve --transport http --with-sync --bootstrap pi-coding-agent`, sharing the sessions volume with worker-pi, `POND_STORAGE_PATH=s3+http://minio:9000/pond/tenants/demo`), `read-side` (`pond serve --transport http` on the same URL). README walkthrough: `docker compose up`, run the scripted prompt, `curl` the read-side search, observe the captured session. Acceptance: the README commands run end-to-end on a clean machine with only docker.

## 6. Phase 4: publish `pi-pond` to npm (in-PR, settled)

Definition of done includes the package LIVE on the registry, not publish instructions:

1. `npm run build` + `npm pack` dry-run; verify `files` contains exactly what `pi install` needs (`index.ts`, `src`, `dist`, `README.md`, `LICENSE`, `package.json` with the `pi` key).
2. `npm publish --access public` (unscoped name; version `0.1.0`). npm auth is the operator's (Misha's) logged-in account - if the publish step hits an auth/2FA wall, stop and hand him the exact command to run; do not work around it.
3. Verify: `npm view pi-pond version` returns `0.1.0`, and on a machine/dir without the monorepo, `pi install npm:pi-pond` discovers and loads the extension (pi lists it; `/pond` command present).
4. README.md of the package: install one-liner, what it does (capture + recall + resume), pond install pointer, managed/url config, the one-time consent explanation.

## 7. Cross-cutting checklist (same PR)

- [ ] `docs/spec.md` 7.8: add `pond resume` verb entry (see Phase 1a wording).
- [ ] `CHANGELOG.md`: entries for the resume verb, v4+SQLite adapter support, pi-pond package.
- [ ] Adapter registry: no new registration needed (pi-coding-agent already registered) - but update its header comment (formats supported, resume semantics, format-watch note).
- [ ] Repo README / docs site: mention pi-pond wherever openclaw-pond/hermes-pond are mentioned (grep for `openclaw-pond` outside `packages/` to find every surface).
- [ ] moon: `.moon/workspace.yml` project entry; `packages/pi-pond/moon.yml`; CI run list in `.github/workflows/ci.yml`.
- [ ] `pond init`: verify the interactive adapter-discovery flow picks up pi (probe already exists); no init changes expected, but confirm.
- [ ] All new pond-core code paths keep the chokepoint discipline (`lance-chokepoints-*`) - resume reads go through the normal read path.
- [ ] Conventional commits; branch `feat/pi-pond-and-v4-adapters`; do not force-push; hooks never skipped.

## 8. Acceptance summary (whole PR)

1. `cargo test` + `moon` CI green including `pi-pond:typecheck`/`pi-pond:test`.
2. `pond resume <id> --to pi-coding-agent` produces a file pi opens via `/resume` (manual smoke documented in the PR description).
3. On a laptop with pi + pond: install extension, zero-config capture + recall + `/pond` resume/insert all work; the six smooth-AX criteria in Phase 1b hold.
4. v4 and SQLite fixture suites pass round-trip conformance; v3 behavior unchanged (existing tests untouched and green).
5. Fleet compose example runs end-to-end per its README.
6. `npm view pi-pond version` == `0.1.0`; `pi install npm:pi-pond` works from the registry.

## 9. Deferred - pi ecosystem findings (reviewed 2026-08-06, OUT of this PR)

Context: the two most-installed third-party pi extensions are by nicobailon - `pi-subagents` (github.com/nicobailon/pi-subagents, ~2.9k stars: async subagent delegation - child pi sessions, background fleets, chains, parallel runs, and since 2026-08-05 durable recurring schedules, issue #815/PR #819) and `pi-intercom` (github.com/nicobailon/pi-intercom, ~284 stars: 1:1 inter-session messaging over a local broker, plus an extension-channel API with owner election and CAS state). Reviewed against this plan on 2026-08-06; everything below is deliberately deferred.

Settled ANTI-decision (do not re-propose): capture NEVER keys on a third-party orchestrator's artifacts or registry. pi-subagents writes run artifacts (`<tmpdir>/pi-subagents-<scope>/async-subagent-runs/<id>/status.json` with `sessionFile`/`sessionId`, project-scoped `.pi-subagents/` dirs, temp chain dirs age-cleaned after 24h) and its docs invite external consumers - but ingest driven by that metadata is selective archiving: sessions get silently missed when artifacts are absent, cleaned, or format-shifted, violating the `session-movement-complete` posture (monotone-but-incomplete is still silent loss). Capture stays uniform: pond ingests pi session FILES, discovered by content, wherever they are. An orchestrator's artifacts may inform a future read-side/analytics layer, never the capture path.

Deferred items, in rough priority order:

1. **Multi-root session discovery.** pi-subagents children write real pi session files OUTSIDE `~/.pi/agent/sessions` (per-run session directories; `context: "fork"` children start from `--session <branched-file>`). Uniform fix: the adapter config accepts a LIST of session roots, and a file is ingested iff it self-identifies as a pi session (v3 `{"type":"session"}` first line / v4 header) - discovery by content, zero knowledge of who created the file. Reliably capturing short-lived tmpdir sessions is the live-write/fleet direction (spec 9.4), not artifact-chasing.
2. **v3 parent-header linkage.** v3 session headers record the parent for `/fork`, `/clone`, `newSession({parentSession})` (see pi session-format.md), but the current adapter maps `parent_session_id: None`. Map it from the header - benefits every forked session, not just subagents. (v4 `parentSessionId` mapping is already in-scope in section 4.)
3. **Subagent taxonomy from source data only.** If a spawner stamps the v4 header `metadata` bag with agent kind, it already flows into `options` via `model-lossless-projection`; a `source_agent` subpath taxonomy (`pi-coding-agent/<agent>`, per the openclaw `<name>/<kind>` precedent) may be derived from that stamped data when present. No stamped data -> no classification -> no guessing (`model-no-synthesis` applied to classification). Never derived from run artifacts.
4. **pi-intercom extension channel for a shared pond process.** Optional-when-present enhancement: elect one owner session via intercom's extension-channel API (namespace, owner election, CAS state), owner runs a single `pond serve --transport http --with-sync`, publishes its URL in channel state, peers connect in `url` mode. With lazy controller start (section 3) the problem this solves - N sessions, N processes, N embedding models - is already small; build only if real usage shows process-count pain. Must never be a hard dependency.
5. **pi-intercom message capture: nothing to build.** Intercom messages are stored in pi session history as extension entries; the adapter's carrier path captures them today. Recorded here so nobody "adds support" for what already works.
