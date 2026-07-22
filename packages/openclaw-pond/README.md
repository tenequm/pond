# openclaw-pond

Projects [pond](https://github.com/tenequm/pond)'s read-only recall tools into
OpenClaw agents and (optionally) manages a local pond process, so installing the
plugin is the complete installation.

pond is the durable, lossless tier beneath OpenClaw's own memory: permanence
past OpenClaw's disk budget, a cross-harness corpus, off-gateway indexing, and
restore. This plugin is deliberately **tools only** - no memory slot, no
auto-recall, no `before_prompt_build` hook, no CLI namespace. It adds four
tools:

- `pond_search` - semantic / full-text search over past sessions.
- `pond_get_session` - read a whole session as a transcript.
- `pond_get_message` - expand one message with its full tool bodies.
- `pond_sql` - read-only SQL analytics over the corpus.

All four are read-only. pond's MCP surface never exposes a write path.

## Install

```bash
openclaw plugins install openclaw-pond
```

In the default **managed** mode the plugin locates the `pond` binary
(config `pond.binaryPath`, else `PATH`) and supervises
`pond serve --transport stdio --with-sync`, speaking MCP over the child's stdio -
no port, no token, no auth surface. It restarts the child with backoff on exit.

If pond is missing, the service fails with a message naming the exact fix:
install pond, then run `pond init` once (which also enables the `openclaw`
adapter). An uninitialized store does not block startup - `pond serve` runs
and its sync log names the `pond init` fix. The plugin never writes pond
config.

## Configuration

```json5
{
  pond: {
    mode: "managed",        // default; plugin spawns and supervises pond
    syncIntervalMinutes: 5, // passed to pond's in-serve sync scheduler
    // binaryPath: "/usr/local/bin/pond",
  },
  // or attach to an external pond serve (operator owns auth via a shim):
  // pond: { mode: "url", url: "https://host/mcp", headers: { Authorization: "Bearer ..." } },

  sources: ["openclaw"],    // pond source_agent filter; ["*"] opts into the cross-harness corpus
  groupSessions: "clamp",   // group/channel callers clamp to tree; "inherit" to disable
}
```

`sources` maps to pond_search's `source_agent` filter, which matches a source
whose value equals the entry OR starts with `<entry>/` - so `"openclaw"` covers
`openclaw` plus `openclaw/subagent`, `/cron`, `/hook`, `/probe`. `["*"]` omits
the filter (whole corpus). pond's filter takes a **single** source: with several
entries the plugin forwards the **first** and logs a one-time warning. For
visibility below `all` the project clamp already excludes foreign-harness
sessions implicitly, so `sources` is the explicit axis and matters most at
`visibility: "all"`.

Nothing in the config is a secret in managed mode; `headers` (url mode) is the
only place a token appears and it is the integrator's shim.

`tools.sessions.visibility` and `tools.agentToAgent` are **read from your
existing OpenClaw config** through the SDK - the plugin adds no parallel
vocabulary for them.

## Privacy model (stated plainly)

Scoping here is **policy against a confused or prompt-injected agent, not a
security boundary against the operator** (who can read the pond store directly).

The plugin resolves `tools.sessions.visibility` and `tools.agentToAgent` with a
vendored copy of OpenClaw's session-visibility policy (`src/visibility.ts`;
upstream demoted that SDK subpath to bundled-only), so pond tools only reach
sessions the agent could already read via `sessions_history`. What agents see by
default and what each widening step exposes:

| `tools.sessions.visibility` | pond_search / pond_get_session / pond_get_message reach |
| --- | --- |
| `self` | only the current session |
| `tree` (default) | the caller's own agent (its sessions + spawned children) |
| `agent` | the caller's own agent |
| `all` + `tools.agentToAgent.enabled` (unrestricted `allow`) | every agent's sessions (cross-harness if `sources: ["*"]`) |

Notes and deliberate limits:

- pond's MCP `project` filter is a single substring, so a set of keys cannot be
  expressed in one call. `tree` and `agent` therefore both clamp to the caller's
  own-agent key prefix `agent:<agentId>:` - bounded to one agent (the primary
  leak risk), coarser than a strict tree (broader for same-agent siblings,
  narrower for spawned children living under another agent id - those stay
  unreachable). `self` pins the exact session key.
- `all` drops the clamp only when `tools.agentToAgent` is enabled with an
  **unrestricted** `allow` list (empty or `"*"`). Core grants cross-agent reads
  per target via its allow-list matcher; a restricted list cannot be expressed
  in one substring, so the plugin keeps the own-agent clamp (fail-closed to the
  expressible subset).
- Group/channel-context callers clamp down to `tree` unless
  `groupSessions: "inherit"` (the private-vs-shared asymmetry). This is a
  pond-specific conservatism - core has no group-context visibility downgrade.
- `pond_sql` runs arbitrary read-only SELECT over the whole corpus; a
  single substring filter cannot clamp arbitrary SQL, so it is gated on the
  operator's broad opt-in (`tools.sessions.visibility: "all"`) and returns a
  typed `forbidden` naming the knob otherwise. Use `pond_search` /
  `pond_get_session` for scoped reads.
- Subagent contexts get the pond tools hidden entirely (the tool factory
  returns `null`), sandboxed or not. Core denies `sessions_search` to leaf-role
  subagents by spawn depth, a signal the plugin tool context does not carry -
  hiding from all subagents is the conservative superset that never
  over-exposes. A subagent needing history gets it passed in by its parent.
- `sources: ["*"]` opts into foreign-harness content, which has **no OpenClaw
  redaction pass**. Snippets are still passed through `redactToolPayloadText`.
- The plugin fails **closed** (typed `forbidden`) whenever scope cannot be
  resolved (missing session identity in the tool context).

## Development

The `openclaw` package is an **optional peer dependency** - the Gateway supplies
it at runtime. This checkout does not install the OpenClaw monorepo, so
`typecheck` and `test` resolve the SDK subpaths the plugin uses
(`plugin-entry`, `config-contracts`, `logging-core`) to faithful local doubles
under `test/stubs/` via tsconfig `paths` and a Vitest alias. The two surfaces
upstream demoted to bundled-only (`tool-results`, `session-visibility`) are
vendored into `src/` instead (see `src/tools.ts` and `src/visibility.ts`).
Everything runs with a plain `npm install`:

```bash
npm install
npm run typecheck   # tsc against the SDK stubs (canonical local gate)
npm test            # vitest: golden MCP fixtures, scope matrix, GBNF conformance
```

`npm run build` (`tsconfig.build.json`) compiles against the **real** `openclaw`
peer and therefore only succeeds where the host is installed (a publish
environment); the OpenClaw extension entry is the TypeScript source `./index.ts`,
so a source-checkout install needs no prior build. Real host-compatibility is
proven with the `npm-pack:` install flow from OpenClaw's plugin docs, not the
local stub typecheck.

## Tests

- `test/tools.test.ts` - golden request/response fixtures for all four tools
  against an in-memory fake pond MCP endpoint (`test/fake-pond.ts`): asserts the
  clamped `project`, limit capping, redaction, byte budget, typed error relay,
  fail-closed, and leaf-subagent hiding.
- `test/scope.test.ts` - the scope matrix: visibility (self/tree/agent/all) x
  agent-to-agent allow/deny x group clamp x missing-context fail-closed x
  sandbox clamp.
- `test/schema.test.ts` - GBNF conformance: the tool parameter schemas carry no
  grammar-breaking features (no `oneOf`, `format`, `patternProperties`, etc.;
  unions emit `anyOf`), with a negative control proving the checker bites.
