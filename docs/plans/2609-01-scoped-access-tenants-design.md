# Scoped access: token gate, token-to-tenant map, and tenant-scoped reads

Status: design agreed, implementation not started. Tracks [#166](https://github.com/tenequm/pond/issues/166).

## Goal

Run one pond store for everything, have every session visible by default, and be able to start a `pond serve` (or hand out a token) that can only see a configured subset of sessions - "work" vs "personal", or one tenant per agent behind a shared server. Sessions must be assignable to a tenant after ingest, without denormalizing the label onto every table, and without closing the door on future live writes.

## Findings

### Upstream: Lance, LanceDB, lance-context

- Neither Lance nor LanceDB implements row-level access control. Authentication lives at the catalog boundary: the [REST namespace `Identity`](https://github.com/lance-format/lance-namespace/blob/main/docs/src/catalog/rest/index.md) (`x-api-key` / `Authorization: Bearer`, per-operation 401/403) and [LanceDB Enterprise authentication](https://docs.lancedb.com/enterprise/authentication). Scoping below that is always a caller-side filter ([LanceDB filtering](https://docs.lancedb.com/search/filtering): prefilter is the default and the only mode that guarantees every hit satisfies the predicate).
- [Credential vending](https://github.com/lance-format/lance/blob/main/rust/lance-namespace-impls/src/credentials.rs) (STS / CAB / SAS, `Read` / `Write` / `Admin`, 1 h expiry, identity-keyed cache) is the Lance answer for hosted multi-tenant storage isolation. It exists only behind `RestNamespace`, so it is a hosted-tier primitive, not a single-store one.
- The Lance team's own scoping design for agent context is lance-context [#90](https://github.com/lance-format/lance-context/issues/90), its [design doc](https://github.com/lance-format/lance-context/blob/main/docs/src/design/partitioned-namespace.md), and [PR #94](https://github.com/lance-format/lance-context/pull/94). Guidance: start with one dataset and typed scope columns filtered per query; partition only when a physical property demands it (isolation, lifecycle, per-scope index, drop-to-erase, storage ACL); cap partition count; keep high-cardinality dimensions as columns; a dimension you might ever partition on must be a typed column, never a key in a JSON blob. What shipped is Phase 1 only: identity partitions, a hand-rolled `__manifest` table, one selector to one dataset, no cross-partition search, no `lance-namespace` dependency, not wired into the server ([namespace.rs](https://github.com/lance-format/lance-context/blob/main/crates/lance-context-core/src/namespace.rs)).
- lance-context's edit and filter mechanics do not transplant. Edits append a new record with a new id and `supersedes_id` ([#81](https://github.com/lance-format/lance-context/issues/81), [PR #84](https://github.com/lance-format/lance-context/pull/84)); reads materialize the whole LSM and `retain` in Rust ([store.rs](https://github.com/lance-format/lance-context/blob/main/crates/lance-context-core/src/store.rs)). Both are forced by a MemWAL/LSM write path with no in-place update, a fact-memory lifecycle model where superseded and contradicted facts must stay queryable ([#56](https://github.com/lance-format/lance-context/issues/56), [PR #62](https://github.com/lance-format/lance-context/pull/62)), and per-context datasets small enough to materialize. pond shares none of those constraints and already pushes filters into Lance indexes.
- Their scope-boundary posture does transfer: [auto-dream](https://github.com/lance-format/lance-context/blob/main/docs/src/design/auto-dream.md) never crosses `session_id` / `tenant` by default; consolidation runs per partition. Fail-closed is the design invariant.
- The [Lance partitioning spec](https://github.com/lance-format/lance-namespace/blob/main/docs/src/partitioning-spec.md) has no engine implementation in `lance-namespace-impls`. [Data Overlay Files](https://github.com/lance-format/lance/blob/main/docs/src/format/table/data_overlay_file.md) (cell-level updates without rewriting fragments) are unstable and refused by release builds. Materialized views are REST-namespace only. There is no Lance mechanism to filter a table by a fact stored in another table other than an IN-list prefilter on an indexed key ([BTree `IsIn`](https://github.com/lance-format/lance/blob/main/docs/src/format/index/scalar/btree.md): multiple lookups, unioned).
- [Lance transactions](https://github.com/lance-format/lance/blob/main/docs/src/format/table/transaction.md): `Append` conflicts only with `Overwrite`, `Restore`, and `UpdateMemWalState`. Concurrent appends never conflict. [MemWAL](https://github.com/lance-format/lance/blob/main/docs/src/format/table/mem_wal.md) gives primary-key tables last-write-wins by PK with no partial-column update, and requires every row of one PK to map to one shard.

### pond

- The spec already carries an opaque wire `namespace` ([7.3](https://github.com/tenequm/pond/blob/main/docs/spec.md)), a `namespace_unknown` error code, a single resolution point (`resolve_namespace`, five call sites), a typed `Predicate` AST, `pond_sql` views over `LanceTableProvider`, BTree indexes on `messages.session_id` and `parts.session_id`, and a `session_id IN (...)` pushdown that `pond copy` runs today in chunks of 512.
- Spec 2.3 currently lists authentication as a non-goal. That decision predates `pond serve` being a long-lived network read side. Every real deployment since ([#194](https://github.com/tenequm/pond/discussions/194), the remote-serve guide) fronts `serve` with the same ad-hoc bearer shim; the only gate in pond itself is the `/mcp` Host allowlist ([#196](https://github.com/tenequm/pond/pull/196)).
- Per-agent scoping was already decided as a filter convention, fail-closed, in the OpenClaw work ([plan](https://github.com/tenequm/pond/blob/main/docs/plans/2607-21-openclaw-integration-implementation-plan.md), modeled on [openclaw#105057](https://github.com/openclaw/openclaw/pull/105057)): policy against confused or prompt-injected agents, not a security boundary against the operator.

## Decisions

1. Spec 2.3 splits. pond still does no identity and no tenancy resolution; it does gain an optional shared-secret gate on `serve` and a token-to-tenant map. Integrators still own who-is-who.
2. One store. With no tokens configured and a loopback bind, behavior is unchanged. With tokens configured, every request carries the token's tenant allowlist; the wire `namespace` becomes an optional narrowing within that allowlist, and a value outside it returns `namespace_unknown`.
3. The label is a session-level, editable, typed column named `tenant`. It is not denormalized onto `messages` or `parts`. Reads resolve the visible session set and push `session_id IN (...)` through the existing BTree indexes.
4. Fail-closed: a session with no tenant is visible only to unscoped callers; scoped responses report in-scope counts only and never reveal how many sessions exist outside the scope.
5. Scoped tokens can ingest. The request `namespace` must resolve to a tenant within the token's allowlist and is stamped on every Session event in the batch; a Session event carrying a different value is rejected per row.
6. Live writes do not need MemWAL. Interval appends per host, with OCC on append, are the live-write path; nothing in this design requires in-place updates on live-written tables, so a later MemWAL adoption stays a substrate swap.
7. No child Lance namespaces or partitions now. A tenant becomes a physical partition only on a physical trigger (per-tenant erase, independent index tuning, storage-level ACL, hosted tenants). Because `tenant` is a typed column from day one, that migration registers the existing store as the default partition and needs no rewrite.
8. Write model deferred between B and C (below); the read path is identical for both.
9. Naming. The column, config keys, and verbs say `tenant`; the wire field stays `namespace` and resolves to a tenant. This follows the spec's existing vocabulary (2.2: "each tenant is an opaque `namespace` string"; 9.5: "mapping each tenant to a child Lance namespace"), avoids a third meaning of `namespace` on top of the wire field and the Lance catalog concept, and keeps `scope` free for `[creds.<name>].scope`.
10. Storage credentials are the second half of the same story. `[creds.<name>].scope` binds a credential set to a URL prefix today; when tenants become physically separate (per-tenant prefixes or child Lance namespaces), it binds per-tenant credentials with no new config vocabulary.
11. This is policy at the read surface. Anyone holding the storage credentials reads everything; hard isolation remains separate stores or per-prefix credentials, with credential vending as the hosted-tier escalation.

## Design

```
config.toml
  [serve]                 token gate; required when the bind is non-loopback
  [serve.tokens.<name>]   token_file | token_command, tenants = ["work", ...]
  [adapters.<adapter>]    tenant = "work"   (or per `path` entry)

request
  -> transport layer   axum layer on /v1/* and /mcp; stdio takes --tenant flags
                       bearer -> allowlist; unknown or missing token -> 401
  -> resolve_namespace(request.namespace, allowlist) -> Scope
                       Scope::All | Scope::Tenants(set); unassigned hidden from scoped callers
  -> handlers
       search   visible ids = resolve(scope), cached per process under the freshness window
                prefilter AND session_id IN (visible ids)            (messages BTree)
       get      the session row must be in the visible set
       sql      sessions / messages / parts views gain a semi-join on the visible set
       ingest   tenant stamped from the token; mismatching Session events rejected

writes
  sync / ingest   tenant from adapter config or token at first write
  edit            B: rewrite the sessions row  |  C: append a session_tenants row
  bulk assign     pond tenant assign --project-prefix ... --tenant ...
  erase           cascades to tenant rows
```

### Where the label lives: B or C

| | B: mutable column on `sessions` | C: append-only `session_tenants` sidecar |
|---|---|---|
| Read path | resolve visible ids, `session_id IN (...)` | identical |
| Edit | in-place rewrite of one `sessions` row (`Update` transaction; a carve-out on a canonical table) | append one row; latest per session wins at read |
| History | Lance version history only (short retention) | every assignment is a row |
| Tables | three | four |
| MemWAL later | needs a PK-keyed whole-row upsert on `sessions` | nothing new |

Choose after the benchmark below. A third option - denormalizing `tenant` onto `messages` for a direct bitmap prefilter, with an edit rewriting that session's message rows - is the fallback only if the IN-list prefilter degrades at the tail.

### Gate before locking B or C

`pond search --explain` must show the `session_id IN (...)` prefilter served by the BTree index, and `serve_mem_bench --io-trace` must record GET counts warm and cold at 100, 1k, and 5k visible sessions. Expected: on par with a direct bitmap prefilter in a warm long-lived `serve` ([#165](https://github.com/tenequm/pond/issues/165) topology), bounded by index size when cold.

Adding a column or a table is a storage-path change, so `moon run bench-gate` runs on both sides (pre-change commit first, before the new binary writes to the real store) and appends its rows to `docs/benchmarks/bench-gate-baseline.jsonl` before anything lands.

### Surfaces

- Verbs: `pond tenant set <session-id> <tenant>`, `pond tenant assign --project-prefix <p> --tenant <t>`, `pond tenant list`; `pond serve --tenant <t>` for stdio; `pond status` names the active scope. CLI and HTTP only, never MCP.
- Spec: 2.3 amendment; a new section 5.x for the column, the fail-closed rule, and the chosen write model; 7.3 (the wire `namespace` resolves to a tenant within the token's allowlist); 7.5 ingest stamping; 7.8 verbs.
- Tests: a connection bound to tenant A cannot see tenant B across search, get, and sql; unknown token 401; unassigned sessions hidden from scoped callers; scoped ingest rejects a mismatching Session event.
- Migration: existing stores gain the column (or the sidecar table) empty; `pond tenant assign` backfills from project prefixes as a one-time operator step.
- Clients: Claude Code and Codex pass a static header for HTTP MCP servers, so a scoped token is one line of client config; the remote-serve guide gains the token setup and drops the "put your own auth in front" advice for the token case.

### Rules and edge cases

- One tenant per session. A session belonging to several tenants is deferred; if it is ever needed, a `List<string>` column with a label-list index (`array_has_any`) is the Lance-native shape.
- Ingest never changes an existing session's tenant. Matched Session rows stay no-ops (`adapter-integrity-additive-sync`); a scoped token re-submitting a session that exists under another tenant gets the same no-op result as any re-submission, so nothing is learned and nothing moves. Edits go only through `pond tenant set` / `assign`.
- Child sessions follow the parent. Assignment cascades over `parent_session_id` at ingest and on every edit, the same way erase and restore already cascade (`adapter-lineage-complete-restore`); a scoped get or resume of a parent requires its children to be in scope, or fails as a whole.
- Wire `namespace` defaults: omitted under a single-tenant token means that tenant; omitted under a multi-tenant token means the union; omitted with no tokens configured means everything, as today. A value outside the allowlist is `namespace_unknown`, never a narrower result.
- HTTP writes under a scoped token are scoped too: `/v1/ingest` stamps the tenant, `pond erase` over HTTP may only erase visible sessions. `pond sync`, `pond copy`, `pond resume`, and CLI `pond erase` are operator paths and stay unscoped.
- `pond copy`, `.pond` archives, and the JSONL wire stream carry the tenant; an archive predating the column restores with the tenant unset (`session-additive-schema-backfill`), and `pond tenant assign` fills it afterwards.
- The visible-session set is cached per process under the `lance-handle-freshness` window and invalidated by an edit made in the same process; another process sees the edit after the window, the same bound every read already has.
- Tokens are shared secrets, not identities: rotation is a config edit and a reload, and they follow `storage-redaction` (never in URLs or argv, redacted in `pond config show` and logs). `POND_ALLOWED_HOSTS` stays as the DNS-rebinding defence on `/mcp`.
- A scoped MCP server says it is scoped in its `instructions` and never lists the tenants it cannot see (`protocol-self-describing-capabilities`, fail-closed).
- Redaction of outbound content ([#167](https://github.com/tenequm/pond/issues/167)) is orthogonal: tenant scoping decides which sessions a caller may read, redaction decides what leaves the store on copy, export, and resume.

### Optional extension: finer allowlists on a token

A token may additionally carry `projects = [...]` and `source_agents = [...]` allowlists, ANDed into the same `Scope` at the same injection point. This answers the second open question in [#166](https://github.com/tenequm/pond/issues/166) (project allowlists per connection) without making `project` the tenancy key: `project` is source-defined (a directory for coding harnesses, a chat key for chat harnesses) and too fine to carry work-versus-personal on its own. Not part of the first cut.

## Alternatives considered

- Separate stores per context (one config, one `serve` each; [#166](https://github.com/tenequm/pond/issues/166) step 1). Hard isolation, zero code, no cross-context search, every surface assumes one store. Stays the documented answer for hard isolation and for anyone who wants no shared store at all; a docs page and `pond init --profile` remain worth doing independently of this design.
- Project or source-agent allowlists as the only scope. Cheapest, no schema change, same injection point, but `project` is a source fact rather than an operator label and cannot express work-versus-personal across harnesses. Kept as the optional extension above.
- Denormalizing `tenant` onto `messages` (and `parts`). The cheapest read (one bitmap prefilter) but every edit rewrites a session's message rows on the big table. Kept as the fallback if the IN-list benchmark degrades.
- lance-context's supersession rows and post-scan filtering. Correct for a MemWAL/LSM store with a fact lifecycle; wrong for pond's scale and prefilter model. Not adopted, see Findings.
- Child Lance namespaces or a partitioned namespace per tenant. Own indexes and drop-to-erase, but one commit per table per tenant per sync on object stores, a pond-side `__manifest` resolver to write, and cross-tenant search becomes fan-out. Deferred to a physical trigger (decision 7).
- Storage-level isolation only (per-tenant credentials, credential vending). The only boundary against a caller who holds credentials; needs separate prefixes or a REST catalog. Kept as the escalation path (decisions 10 and 11), not the everyday mechanism.

## Sizing

Roughly two weeks of focused work: token gate and map with 401 tests, about three days; the `tenant` column or sidecar, adapter and token stamping, `tenant set` / `assign` / `list`, child cascade, and the one-time backfill, about five days; scoped search, get, sql, and HTTP erase with the A-cannot-see-B suite, about four days; spec sections, remote-serve guide, and the two benchmark runs, about two days. Everything is additive; an existing store upgrades in place on open.

## References

- pond: [#166 namespaces](https://github.com/tenequm/pond/issues/166), [#167 redaction on outbound paths](https://github.com/tenequm/pond/issues/167), [#165 remote read path](https://github.com/tenequm/pond/issues/165), [#194 hosted serve discussion](https://github.com/tenequm/pond/discussions/194), [#196 allowed hosts](https://github.com/tenequm/pond/pull/196), [spec](https://github.com/tenequm/pond/blob/main/docs/spec.md).
- lance-context: [#37](https://github.com/lance-format/lance-context/issues/37), [#56](https://github.com/lance-format/lance-context/issues/56), [#81](https://github.com/lance-format/lance-context/issues/81), [#90](https://github.com/lance-format/lance-context/issues/90), [PR #62](https://github.com/lance-format/lance-context/pull/62), [PR #84](https://github.com/lance-format/lance-context/pull/84), [PR #94](https://github.com/lance-format/lance-context/pull/94), [partitioned-namespace design](https://github.com/lance-format/lance-context/blob/main/docs/src/design/partitioned-namespace.md), [auto-dream design](https://github.com/lance-format/lance-context/blob/main/docs/src/design/auto-dream.md), [rollout deployment (server-id MemWAL sharding)](https://github.com/lance-format/lance-context/blob/main/docs/src/specs/rollout-deployment.md), [lance-context creation vote](https://github.com/lance-format/lance/discussions/5716).
- Lance: [namespace spec](https://github.com/lance-format/lance-namespace/blob/main/docs/src/namespace/index.md), [directory catalog](https://github.com/lance-format/lance-namespace/blob/main/docs/src/catalog/dir/index.md), [REST catalog](https://github.com/lance-format/lance-namespace/blob/main/docs/src/catalog/rest/index.md), [partitioning spec](https://github.com/lance-format/lance-namespace/blob/main/docs/src/partitioning-spec.md), [credential vending](https://github.com/lance-format/lance/blob/main/rust/lance-namespace-impls/src/credentials.rs), [transactions](https://github.com/lance-format/lance/blob/main/docs/src/format/table/transaction.md), [MemWAL](https://github.com/lance-format/lance/blob/main/docs/src/format/table/mem_wal.md), [data overlay files](https://github.com/lance-format/lance/blob/main/docs/src/format/table/data_overlay_file.md), [BTree index](https://github.com/lance-format/lance/blob/main/docs/src/format/index/scalar/btree.md), [label-list index](https://github.com/lance-format/lance/blob/main/docs/src/format/index/scalar/label_list.md), [LSM scanner](https://github.com/lance-format/lance/blob/main/rust/lance/src/dataset/mem_wal/scanner/builder.rs).
- LanceDB: [namespaces](https://docs.lancedb.com/namespaces), [filtering](https://docs.lancedb.com/search/filtering), [scalar indexes](https://docs.lancedb.com/indexing/scalar-index), [enterprise authentication](https://docs.lancedb.com/enterprise/authentication).
- OpenClaw: [sessions_search scoping precedent](https://github.com/openclaw/openclaw/pull/105057).
