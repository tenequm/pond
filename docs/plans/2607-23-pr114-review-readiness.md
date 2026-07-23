# PR #114 review-readiness change set

Execution plan for the `feat/openclaw-integration` branch (PR #114). Goal: make the openclaw-pond plugin and its pond-side seams maximally ready for OpenClaw-maintainer review, based on a verified audit of OpenClaw HEAD `988e640c` (2026-07-22, clone at `~/pjv/openclaw/openclaw`) and the pond-sb E2E validation run. All findings below were verified against sources on 2026-07-23; file:line references into OpenClaw are against that HEAD.

Repo rules apply throughout: repo CLAUDE.md (test placement, comment minimalism, ASCII, conventional commits), plugin validation via `npm run typecheck && npm test` in `packages/openclaw-pond/`, Rust validation via `cargo fmt --check && cargo clippy -- -D warnings && cargo test` from repo root. Never pipe test/build output through head/tail/grep. Implement in the section order below (A insulates against an upstream deadline, so it goes first).

## A. SDK insulation

Upstream context: OpenClaw's compat registry (`src/plugins/compat/registry.ts:521,538`) demotes `openclaw/plugin-sdk/session-visibility` and `openclaw/plugin-sdk/tool-results` to bundled-only, status "removed", removeAfter 2026-07-30, replacement "no external successor". Their `.d.ts` files are already stripped from the shipped npm package (no `types` field in the exports map). Our other three subpath imports (`plugin-entry`, `config-contracts`, `logging-core`) are fully public and blessed - keep them.

### A1. Vendor the `AgentToolResult` type locally

- `packages/openclaw-pond/src/tools.ts:9` imports `type { AgentToolResult } from "openclaw/plugin-sdk/tool-results"` (type-only; `test/tools.test.ts` imports it too).
- Read the real shape from `~/pjv/openclaw/openclaw/src/plugin-sdk/tool-results.ts` (generic `AgentToolResult<T>` with required `details: T`) and define a structurally identical local type, exported from `src/tools.ts` (no new file - it has one consumer module plus tests).
- Add a one-line provenance comment: vendored because upstream demoted the subpath to internal (registry `plugin-sdk-tool-results-public-demotion`, removeAfter 2026-07-30) with no public successor.
- Repoint `test/tools.test.ts`; delete `test/stubs/openclaw/plugin-sdk/tool-results.ts`; remove its mappings from `tsconfig.json` paths and `vitest.config.ts` aliases; drop the mention from README's Development section.
- Also update the `plugin-entry` stub (`test/stubs/openclaw/plugin-sdk/plugin-entry.ts`), whose `AnyAgentTool` currently imports the type from the tool-results stub - point it at the vendored type.

### A2. Vendor the session-visibility policy logic

- `packages/openclaw-pond/src/scope.ts:20-25` imports runtime functions from `openclaw/plugin-sdk/session-visibility` (read the import list from the file; it includes `createAgentToAgentPolicy` and the visibility resolvers) plus `type OpenClawConfig` from `config-contracts` (KEEP - config-contracts is blessed).
- Vendor the needed policy surface into a new `packages/openclaw-pond/src/visibility.ts`: copy the semantics (not necessarily the code verbatim - trim to only what scope.ts calls) from `~/pjv/openclaw/openclaw/src/plugin-sdk/session-visibility.ts`. That surface is stable upstream since 2026-05-24 (`createAgentToAgentPolicy` last changed in #85849), so drift risk is low. Include a provenance comment naming the source file and HEAD commit `988e640c` and the demotion rationale.
- Repoint `src/scope.ts` and `test/scope.test.ts`; delete `test/stubs/openclaw/plugin-sdk/session-visibility.ts` and its tsconfig/vitest mappings; update the README Development section's stub list (after A1+A2 the stubs directory should hold only `plugin-entry`, `config-contracts`, `logging-core`).
- Keep the existing scope.test.ts matrix green unchanged - it now exercises the vendored code directly, which is strictly better fidelity.
- Do NOT file an upstream issue about external consumership in this change set (post-publish action, operator's call).

## B. Sidecar lifecycle parity with OpenClaw core conventions

Upstream context: core never uses the raw MCP-SDK `StdioClientTransport` for sidecars. Its own `OpenClawStdioClientTransport` (`src/agents/mcp-stdio-transport.ts:64,118-165`) spawns `detached` and tears down with a SIGTERM -> 2s wait -> SIGKILL ladder via `killProcessTree`/`signalProcessTree`. Core's exec layer explicitly recognizes `nice` as a transparent wrapper (`src/infra/dispatch-wrapper-resolution.ts:456`). House health-probe pattern: `Date.now()+timeoutMs` deadline loop, 10s timeout / 200ms poll for a local child (`src/cli/gateway-cli/run-loop.ts:27-28,91`).

### B3. Graceful stop ladder in `PondController.stop()`

- Location: `packages/openclaw-pond/src/service.ts` `stop()` and the transport teardown path; the SDK transport is built in `createStdioTransport` (`src/mcp.ts:78-88`).
- First read `node_modules/@modelcontextprotocol/sdk/dist/cjs/client/stdio.js` to confirm what `close()` sends the child (it signals the direct child only). `pond serve` is a single-process child (the Rust binary spawns no descendants, and `nice` execs in place - same pid), so full tree *enumeration* is unnecessary; what we adopt from core is the *ladder*: SIGTERM, wait up to 2s for exit, then SIGKILL, then a short reap wait. Use the transport's `pid` accessor if exposed, else keep a handle to the child.
- Make `stop()` idempotent (safe to call twice; core only warn-logs stop failures and never retries - `src/plugins/services.ts:200-201`) and ensure both the service `stop` and `lifecycle.registerRuntimeLifecycle` cleanup route through it.
- Add a comment stating the single-process rationale so a reviewer sees tree-kill was considered, not missed.
- Test: extend the existing service tests (or add one) asserting stop() resolves cleanly when called twice and when the child is already dead.

### B4. `nice -n 19` spawn wrap

- Location: `packages/openclaw-pond/src/service.ts` `dialStdio()` (~line 164). `StdioClientTransport` uses cross-spawn with `shell:false`, so argv chaining is quoting-safe.

```ts
const pondBin = resolvePondBinary(this.config.binaryPath);
const posix = process.platform !== "win32";
const command = posix ? "nice" : pondBin;
const prefix = posix ? ["-n", "19", pondBin] : [];
```

- `resolvePondBinary` still runs first, preserving the named-fix install-hint error path. `nice` execs its argv tail, so the child pid IS pond (relevant to B3).
- README managed-mode paragraph gets one clause: the child runs at low scheduling priority so background sync never competes with interactive work.

### B5. 10s deadline on connect and probe

- The SDK supports `RequestOptions.timeout` on every request including `connect` (default 60s, `shared/protocol.d.ts:61-77`).
- `packages/openclaw-pond/src/mcp.ts`: thread a `{ timeoutMs }` option through `PondMcpClient.connect` and `listToolNames`; `packages/openclaw-pond/src/service.ts:144-148`: pass 10_000 to both. The initialize handshake inside `connect()` is where a hung child actually stalls - the probe alone does not cover it.
- On timeout the thrown `McpError` lands in the existing catch -> `scheduleRestart()` backoff; no new error path. Add one fake-pond test with a never-responding handler asserting the dial fails within the deadline.

## C. Zero-friction onboarding (pond Rust side, same branch)

### C6. `pond serve --bootstrap <adapter>` + plugin passes it

Spec constraint, quoted from `docs/spec.md:676`: "Sync ingests only already-enabled `[adapters.*]`; it never discovers, enables, or writes adapter state - that is the explicit job of `pond adapters` and `pond init`." Bootstrap is therefore framed and implemented as an init-equivalent step at serve STARTUP, completing before `spawn_in_serve_sync` is called - the sync loop itself still only ever sees already-enabled adapters.

- Arg: `packages/pond/src/main.rs` serve arg struct (~line 629, next to `with_sync`/`sync_every`): `#[arg(long, value_name = "ADAPTER")] bootstrap: Option<String>` with a doc comment stating the init-equivalence and the no-clobber rule.
- Handler: after `Config::load` (~main.rs:1325, becomes `let mut config`), before `spawn_in_serve_sync` (~1345):

```rust
if let Some(name) = &bootstrap {
    if config.adapters.is_empty() {
        adapters_enable(&config_file, name)?;   // main.rs:2810-2846, reused verbatim
        config = Config::load(&config_file)?;
    } else {
        tracing::info!(%name, "bootstrap skipped: adapters already configured");
    }
}
```

- `adapters_enable` already does discovery-or-enable + `persist_accept`; for openclaw with no config it discovers via `adapter/openclaw.rs::resolve_root` (`$OPENCLAW_STATE_DIR` / `~/.openclaw` / `~/.clawdbot` with an `agents/` dir). If discovery finds nothing (no OpenClaw install), `adapters_enable` errors - catch that case and downgrade to a `tracing::warn!` naming `pond init`, so serve still starts (an empty store serving fts-degraded search is the correct plugin-only state).
- spec.md: add one sentence to the serve verb section (near spec.md:676-686) documenting `--bootstrap` as an operator-opt-in init-equivalent pre-sync step, explicitly distinguishing it from the sync-never-enables rule.
- Plugin side: `dialStdio()` appends `"--bootstrap", "openclaw"` to the args (managed mode only, which is the only mode that spawns).
- README invariant reword (Install section, currently "The plugin never writes pond config."): replace with the true stronger claim - the plugin never touches an existing pond config; if pond is completely unconfigured, it bootstraps the `openclaw` adapter only (equivalent to a minimal `pond init`), and `pond init` remains the path for cross-harness corpora. Also update the service.ts header comment (~line 5) making the same claim.
- Tests: unit test for the bootstrap branch in main.rs's `#[cfg(test)]` (sandboxed `HOME`/`XDG_*` + a fake `$OPENCLAW_STATE_DIR` fixture dir containing `agents/`, assert config written with `[adapters.openclaw]` and that a second run with adapters present no-ops). The existing `resolve_sync_adapters_errors_point_at_the_adapters_commands` test (~main.rs:6151) is unaffected.

### C7. `LazyEmbedder` background idle-eviction reaper

Motivation (measured on pond-sb): after a vector burst the ~500 MiB model stays resident (~894 MiB RSS) because eviction only happens on the NEXT `get()` after the 60s idle threshold. A background tick makes "zero cost when idle" literally true.

- Location: `packages/pond/src/embed.rs`, on `LazyEmbedder`:

```rust
pub fn spawn_idle_reaper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
    let this = Arc::clone(self);
    tokio::spawn(async move {
        let tick = this.idle_threshold.min(Duration::from_secs(30)).max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(tick).await;
            let mut state = this.state.lock().await;
            if let Some(cached) = &*state {
                if Instant::now().duration_since(cached.last_use) > this.idle_threshold {
                    *state = None;
                }
            }
        }
    })
}
```

- Race-safety: in-flight embeds hold their own `Arc<dyn Embedder>` clone (embed.rs `get()` returns `Arc::clone`), so reaping mid-embed only drops the cache's reference; `last_use` refreshes on every `get()`. No primitive changes - same tokio `Mutex<Option<CachedBackend>>`.
- Call sites: after the `LazyEmbedder` construction in BOTH `Command::Serve` (~main.rs:1328-1340, after `spawn_prewarm`) and `Command::Mcp` (~main.rs:1365-1373) - both are long-lived processes with the same idle-memory concern.
- Test: in embed.rs's `#[cfg(test)]`, spawn the reaper against `with_idle_threshold(20ms)` + the existing `CountingEmbedder` fake, real-sleep past the threshold, assert the cached backend is gone WITHOUT an intervening `get()`. Note per the existing test comments (~embed.rs:645-647): eviction keys on `std::time::Instant`, immune to `tokio::time::pause` - use tiny real thresholds like the neighboring tests.

## D. Review-facing polish

### D8. README restructure (`packages/openclaw-pond/README.md`)

- New first line (before the current intro): a bold tagline "Read-only. Local. Zero data egress." - then the existing intro.
- Tool bullet for `pond_search`: reframe from "semantic / full-text search" to search over a readable local archive (exact + BM25 + semantic as accelerator); keep the other three bullets.
- New short subsection after the tool bullets: context frugality - the tools are search-then-fetch (find a few relevant messages, expand on demand), responses are size-bounded (32 KB cap, verify the exact `RESPONSE_MAX_BYTES` value in `src/schemas.ts` before quoting), so recall never floods the agent's context.
- Privacy section: mirror OpenClaw's SECURITY.md trust-boundary language verbatim where we paraphrase it today. Canonical upstream wording (SECURITY.md:11-13): "Anyone who can operate an agent can make it do anything that agent can do. Session ownership, visibility, and presence are usability features, not security boundaries." and (docs/concepts/multi-user.md:14-16) "If people must not access each other's sessions, tools, credentials, or files, give them separate agents or separate gateway/host trust boundaries."
- New "Real behavior proof" section before Development, with the pond-sb E2E numbers (measured 2026-07-22, openclaw@2026.7.2-beta.3, 16-core host, corpus 221 sessions / 3,105 messages): idle ~102 MiB RSS at ~0.3% CPU (model not loaded); fts/get/sql stay ~100 MiB; vector burst ~894 MiB at 3-4 cores; sync embed pass flat ~650 MiB; store 11 MiB disk; model cache 466 MiB one-time; child killed -> respawn ~1s; gateway killed -> zero orphaned processes (verified twice); 10 concurrent tool calls multiplex cleanly over one connection; relay latencies 8-91 ms (brute-force vector 1.4 s pre-index).
- Bootstrap invariant reword per C6; managed-mode nice note per B4; Development stub list per A1/A2.

### D9. Manifest conventions (`packages/openclaw-pond/openclaw.plugin.json`)

- Convention verified from in-tree plugins (`extensions/logbook`, `extensions/parallel`): `configSchema` carries pure structure (`type`, `default`, `additionalProperties:false` at every object level); human-facing text lives in a sibling `uiHints` map keyed by dotted path (`label`, `help`, `advanced:true`, `sensitive:true`, `placeholder`).
- Add `default` values to the schema matching `src/config.ts` `parsePluginConfig` defaults exactly (read config.ts first; expected: mode "managed", syncIntervalMinutes 5, sources ["openclaw"], groupSessions "clamp").
- Add `uiHints`: `label`+`help` for every key; `advanced:true` on `pond.binaryPath`, `pond.url`, `pond.headers`, `groupSessions`; `sensitive:true` on `pond.headers`.
- Keep top-level `required` ABSENT - verified (`src/plugins/bundled-sources.ts:87,102-108`) that only a non-empty top-level `required` flips a plugin to setup-required, which is the exact shape of open install bug #112719. All-optional + `{}` validates and auto-enables.

### D10. Trim `SEARCH_DESCRIPTION`

- `packages/openclaw-pond/src/tools.ts:118`: drop the "(Claude Code, OpenClaw, and others)" parenthetical (~35 chars of corpus flavor, no routing signal). Do NOT touch the other three descriptions: `GET_SESSION_DESCRIPTION`'s "analyzing, reviewing, or summarizing" is deliberate routing bait and `SQL_DESCRIPTION`'s "escape hatch" framing is deliberate anti-routing (mirrors the transport.rs `get_info` conventions in CLAUDE.md).

## Out of scope for this change set

- npm platform-package binary distribution (verified supported: OpenClaw's plugin installer never omits optionalDependencies and has esbuild-style platform-package repair machinery in `src/plugins/install-managed-npm.ts:399-464` - but it is release-pipeline work, separate PR).
- Upstream issue about session-visibility external consumership (post-publish).
- Any beam (#112311) integration.

## Validation and landing

1. Per-section: plugin changes -> `npm run typecheck && npm test` in `packages/openclaw-pond/` (42 tests must stay green, plus the new ones); Rust changes -> `cargo fmt --check && cargo clippy -- -D warnings && cargo test` from repo root (308 tests baseline).
2. Full-stream output always (no head/tail/grep on cargo or vitest output).
3. Commits: conventional, scoped, one per section is a sensible split - e.g. `fix(openclaw): vendor demoted plugin-sdk surfaces` (A), `fix(openclaw): sidecar stop ladder, nice spawn, dial deadlines` (B), `feat(serve): --bootstrap init-equivalent adapter enable` + `perf(embed): background idle eviction reaper` (C), `docs(openclaw): README + manifest review polish` (D). Do not commit without the operator's go-ahead if the session rules require it; validate before every commit.
4. After landing, update the PR #114 body: new test counts, the bootstrap onboarding story, the vendored-SDK rationale (one paragraph each).
5. Recommend (operator decision): re-run the pond-sb E2E suite against the new head - B3/B4/B5/C6 change externally observable lifecycle behavior; the E2E sandbox and harness live on `ssh pond-sb` under `~/pond-e2e`.
