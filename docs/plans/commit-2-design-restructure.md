# commit 2: design.md restructure to C-shape + RFC layering

Reference plan for the one atomic commit that restructures `docs/design.md` into a schemas/protocol shape with RFC normative language and stable markdown anchors, plus the 75-ref ripple update across `src/` and a CLAUDE.md anchor-convention example update.

This doc is the source of truth for the work, written so a fresh-context agent can execute it without back-and-forth. If anything below conflicts with what the user said later in conversation, the conversation wins and this doc should be updated.

## Status & context (state at plan-write time)

- Branch: `main`, 7 commits ahead of `origin/main`, not pushed.
- HEAD: `ac98b6f feat(substrate): bump Lance to v7.0.0-beta.14, adopt lance-namespace + Store::scan + NamespaceIdent (inv 11/21/22)` (commit 1; already landed).
- design.md current shape: 944 lines / ~13.4k words / ~24.5k tokens.
- design.md top-level: §1 What this is, §2 Foundations, §3 v1 Application, §4 Deferred.
- design.md invariants currently number 1-28 (inv 21-28 added during the namespace-prep session pre-compaction).
- 75 code cross-references to design.md across 11 files (counts from `grep -rn "design\.md\|invariant " src/*.rs src/*/*.rs`):
  - handlers.rs (17), sessions.rs (14), config.rs (8), wire.rs (6), transport.rs (6), substrate.rs (5), adapter/*.rs (5), lib.rs (1), main.rs (1), misc (others).
- CLAUDE.md has a `## Comments` section with an `<example>` block that cites `design.md 3.6.1` style anchors; example needs updating to the new `design.md#anchor` form.

## Goal

One atomic commit on `main` containing:

1. design.md restructured to C-shape (8-section RFC-style outline) with stable `{#anchor-id}` markdown anchors on every section.
2. RFC normative keywords (MUST/SHOULD/MAY) on every invariant.
3. The 75 code cross-refs updated from `design.md <section-number>` form to `design.md#<stable-anchor>` form via mechanical replacements.
4. CLAUDE.md `## Comments` `<example>` block updated to use the new anchor form.

No code logic changes. No design content changes beyond restructuring and adding normative keywords. No new invariants. No deletions of invariant material beyond fusion of overlapping content (3.1.X canonical + 3.2.X storage merging into per-type schema blocks).

## Target design.md structure

```
# Pond - Design v2

(brief one-line intro identifying the system)

## 1. Status & abstract                                        {#status}

- Status: Draft (v1)
- Abstract: 5-line one-paragraph summary of what pond is and what v1 ships.

## 2. Scope & non-goals                                        {#scope}

### 2.1 v1 scope                                               {#scope-v1}
### 2.2 Non-goals                                              {#scope-non-goals}
### 2.3 Personal pond defaults                                 {#scope-personal-pond}

## 3. Invariants                                               {#invariants}

(brief lead-in: foundations / stack table moves here as the substrate context for the invariants)

### 3.1 Stack                                                  {#invariants-stack}
### 3.2 Operational invariants                                 {#invariants-list}

1. MUST: Append-only writes. ...                               {#inv-1}
2. MUST: Deterministic primary keys. ...                       {#inv-2}
...
28. MUST: No write batch spans multiple PK shards atomically.  {#inv-28}

### 3.3 Concurrency model                                      {#invariants-concurrency}

## 4. Schemas                                                  {#schemas}

### 4.1 Conventions                                            {#schemas-conventions}
### 4.2 Common types                                           {#schemas-common-types}
### 4.3 Lance write parameters                                 {#schemas-write-params}

(cross-cutting params block; applies to all four per-type schema blocks below)

### 4.4 Session                                                {#schemas-session}

**Canonical fields** (markdown table: field | type | source semantics)
**Storage (dataset: `sessions.lance`)** (PK columns + indexes + partition keys + immutable-field list)
**Cross-refs** (invariant refs, ingest contract, search projection)

### 4.5 Message                                                {#schemas-message}
### 4.6 Part                                                   {#schemas-part}
### 4.7 Embedding                                              {#schemas-embedding}

(per-type blocks each follow the same Canonical + Storage + Cross-refs format)

### 4.8 What's absent + where it lives                         {#schemas-absent}
### 4.9 Adapter seam types                                     {#schemas-adapter-seam}

## 5. Protocol                                                 {#protocol}

### 5.1 Wire interface                                         {#protocol-wire-interface}
### 5.2 Error envelope                                         {#protocol-error-envelope}
### 5.3 pond_search                                            {#protocol-pond-search}
### 5.4 pond_get                                               {#protocol-pond-get}
### 5.5 pond_ingest                                            {#protocol-pond-ingest}

(includes immutable-fields rule and per-row outcome semantics from current 3.6.4)

### 5.6 pond_session_events (SSE)                              {#protocol-pond-session-events}
### 5.7 Ingest semantics                                       {#protocol-ingest-semantics}

(current 3.4 prose: ordering enforcement, staleness skip, session-batched commits, live-write deferred)

### 5.8 Search semantics                                       {#protocol-search}

(current 2.5 search defaults + 3.3 indexed content and concatenation policy folded together)

### 5.9 Conformance fixture set                                {#protocol-conformance}

## 6. Alternatives considered                                  {#alternatives}

(one-paragraph rationale per major decision: WhenMatched::DoNothing vs Replace,
FirstSeen dedup at substrate, namespace-as-string at wire vs Lance-namespace
internal type, MemWAL deferral, 90d uniform retention, two scan helpers
(scan vs scanner), etc.)

## 7. Open questions                                           {#open-questions}

(explicit list with sub-anchors for each)

## 8. Deferred                                                 {#deferred}

(bullet list - not prose. Each item names the activation condition.)
```

## Per-type schema block format (the load-bearing C-shape contribution)

Each of §4.4 Session, §4.5 Message, §4.6 Part, §4.7 Embedding follows this exact shape:

```markdown
### 4.X <TypeName>                                             {#schemas-<typename>}

**Canonical fields**

| Field | Type | Source semantics |
| --- | --- | --- |
| `id` | `Extracted<String>` | content-hash / UUIDv7 / etc |
| `session_id` | `Extracted<String>` | from `Session.id` |
| ... | ... | ... |

**Storage** (dataset: `<table>.lance`)

- PK columns: `<col>` (pos 1), `<col>` (pos 2). Unenforced ([inv 24](#inv-24)).
- Stable row IDs: enabled ([inv 25](#inv-25)).
- Scalar indexes: BTREE on `<col>`; BITMAP on `<col>`; ...
- Partition keys / shardable attributes per [inv 26](#inv-26): `<col>` at PK pos 1.
- Embedding column: (yes/no, with placement note)

**Immutable fields**

- `<col>`, `<col>` (per [inv 11](#inv-11) immutable-field check)

**Cross-refs**

- Ingest contract: [§5.5](#protocol-pond-ingest)
- Search projection: [§5.3](#protocol-pond-search), [§5.8](#protocol-search)
- Adapter seam: [§4.9](#schemas-adapter-seam)
```

This block fuses the current 3.1.X canonical-type prose with the 3.2.X dataset storage prose. Single source of truth per type.

## Invariant normative-keyword policy

Default: **MUST** prefix on every invariant. The doc is a specification; the rules are load-bearing.

Three explicit exceptions where the existing text already encodes a mixed or non-MUST normative force - preserve and formalize:

1. **inv 13** (prefilter pushdown). Already explicit MUST on the pond requirement AND a nested MUST on the test assertion. Keep both:
   ```
   13. MUST: Prefilter pushdown is opt-in on every Lance Scanner. ...
       Load-bearing: an integration test on real data MUST assert via
       Scanner::explain_plan that the scalar predicate appears as a
       ScalarIndexQuery / ScalarIndexExec node and not as a top-level
       FilterExec.
   ```

2. **inv 17** (adapter-level dedup contract; substrate FirstSeen floor). Mixed SHOULD + MUST. Recast as:
   ```
   17. SHOULD: Adapters SHOULD detect duplicate-PK emissions using the
       source format's own mechanism (e.g. claude-code's messageSet).
       MUST: The substrate runs merge_insert with
       SourceDedupeBehavior::FirstSeen at src/substrate.rs::merge_insert
       so storage stays correct even when an adapter misses.
   ```
   Two clauses, two keywords; same content as today.

3. **inv 26** (PK pos 1 = shardable attribute). Descriptive lead-in + MUST tail. Recast as:
   ```
   26. MUST: PK position 1 on high-volume tables is a coarse-grain
       shardable attribute. New high-volume tables MUST place an
       attribute at PK pos 1 that can serve as the input to bucket(col, N)
       (or equivalent DataFusion expression). The v1 tables already
       satisfy this (session_id on messages/parts/embeddings; id on
       sessions).
   ```

All other invariants (1-12, 14-16, 18-25, 27-28): prepend `MUST: ` to the existing first sentence; keep body unchanged.

## Anchor mapping table (the ripple sed dictionary)

Old form on the left, new form on the right. Mechanical replacement via Edit calls with `replace_all=true` per pattern, scoped to `src/`.

| Old pattern | New anchor |
| --- | --- |
| `design.md 1.1` | `design.md#scope-v1` |
| `design.md 1.2` | `design.md#scope-non-goals` |
| `design.md 2.1` | `design.md#invariants-stack` |
| `design.md 2.1.1` | `design.md#scope-personal-pond` |
| `design.md 2.2` | `design.md#protocol-wire-interface` |
| `design.md 2.3` | `design.md#invariants` (chapter root; per-invariant refs use #inv-N below) |
| `design.md 2.3 inv N` (any N 1-28) | `design.md#inv-N` |
| `design.md 2.3 #N` (alt form, any N) | `design.md#inv-N` |
| `design.md 2.3 invariant N` (alt form) | `design.md#inv-N` |
| `design.md 2.3 invariants N-M` (range) | `design.md#inv-N` (leftmost; chain in prose) |
| `§2.3 invariants N-M` (sigil-style) | `[invariants N-M](design.md#inv-N)` (same leftmost rule) |
| `design.md 2.4` | `design.md#invariants-concurrency` |
| `design.md 2.5` | `design.md#protocol-search` |
| `design.md 2.6` | `design.md#inv-11` (stale ref today; canonicalize to the single-namespace invariant) |
| `design.md 3.1.3` | `design.md#schemas-session` |
| `design.md 3.1.4` | `design.md#schemas-message` |
| `design.md 3.1.5` | `design.md#schemas-part` |
| `design.md 3.x` (vague) | `design.md#schemas-session` (the one current vague ref at adapter/claude_code.rs:362 is about subagent files, decoded in Session) |
| `design.md 3.2.0` | `design.md#schemas-write-params` |
| `design.md 3.2.2` | `design.md#schemas-message` |
| `design.md 3.2.4` | `design.md#schemas-embedding` |
| `design.md 3.3` | `design.md#protocol-search` |
| `design.md 3.4` | `design.md#protocol-ingest-semantics` |
| `design.md 3.6.1` | `design.md#protocol-error-envelope` |
| `design.md 3.6.2` | `design.md#protocol-pond-search` |
| `design.md 3.6.3` | `design.md#protocol-pond-get` |
| `design.md 3.6.4` | `design.md#protocol-pond-ingest` |
| `design.md 3.6.5` | `design.md#protocol-pond-session-events` |
| `design.md 3.6.6` | `design.md#protocol` (only one ref at handlers.rs:713 referencing the missing 3.6.6 pond_export; canonicalize to protocol root) |
| `invariant N` (bare, in code comments without "design.md" prefix) | leave as-is; the typed cross-ref form below covers it |

Apply order: do the most-specific patterns first (`2.3 inv N` before `2.3`), so the less-specific patterns don't match the wrong text.

After the ripple, `grep -rn "design\.md [0-9]" src/` should return zero matches. Anything that does match is either a missed pattern (add it to the table) or a non-anchor mention (rare; resolve case-by-case).

## CLAUDE.md `## Comments` example update

The current example block in `/Users/tenequm/Projects/pond/CLAUDE.md` contains:

```
<example>
// design.md 3.6.1: typed `conflict` for OCC failures, not `storage_unavailable`.
</example>
```

Update to:

```
<example>
// design.md#protocol-error-envelope: typed `conflict` for OCC failures, not `storage_unavailable`.
</example>
```

The prose rule above also references `design.md <section>`; update to:

```
Anchor to `design.md#<anchor>` or `design.md#inv-N` when one applies.
```

The other two examples (`_score` autoprojection, refresh window) have no anchor today, leave unchanged.

## Execution sub-passes

Five sub-passes, with verification between each. If any verification fails, STOP and report.

### Sub-pass 1: Read design.md in full + scan all code refs

- Read all 944 lines of `docs/design.md` (one or two Read calls).
- `grep -n "design\.md\|invariant " src/*.rs src/*/*.rs` and capture the full list.
- Confirm the anchor mapping table covers every observed pattern. If any pattern in code is missing from the table, add it before proceeding.

### Sub-pass 2: Build the explicit anchor mapping table

- The table above is the starting point. Cross-check against the actual code refs.
- Resolve any vague refs (`3.x`, `2.6`, etc.) by inspecting the code comment in context and assigning a sensible anchor.

### Sub-pass 3: Write the new design.md

- One `Write` call replacing the entire file. Target structure as specified above.
- Content sources for each new section:
  - `#status` Status + Abstract: new content; 5-10 lines total.
  - `#scope`: current §1.1 + §1.2 + §2.1.1, lightly rephrased.
  - `#invariants-stack`: current §2.1 stack table + §2.1's preamble.
  - `#invariants-list`: current §2.3, with MUST/SHOULD/MAY prefixes per the keyword policy.
  - `#invariants-concurrency`: current §2.4.
  - `#schemas`: chapter intro; absorbs current §3.1 lead-in.
  - `#schemas-conventions`: current §3.1.1.
  - `#schemas-common-types`: current §3.1.2.
  - `#schemas-write-params`: current §3.2.0.
  - `#schemas-session`: FUSE current §3.1.3 + §3.2.1 into the per-type block format.
  - `#schemas-message`: FUSE current §3.1.4 + §3.2.2.
  - `#schemas-part`: FUSE current §3.1.5 + §3.2.3.
  - `#schemas-embedding`: current §3.2.4 (no §3.1 entry exists; this is the only one without a canonical-type counterpart, since embeddings are denormalized from messages per inv 14-style additive design). Add a minimal "Canonical fields" table describing what each embedding row represents.
  - `#schemas-absent`: current §3.1.6.
  - `#schemas-adapter-seam`: current §3.1.7.
  - `#protocol`: chapter intro; absorbs current §3.6 lead-in.
  - `#protocol-wire-interface`: current §2.2.
  - `#protocol-error-envelope`: current §3.6.1.
  - `#protocol-pond-search`: current §3.6.2.
  - `#protocol-pond-get`: current §3.6.3.
  - `#protocol-pond-ingest`: current §3.6.4.
  - `#protocol-pond-session-events`: current §3.6.5.
  - `#protocol-ingest-semantics`: current §3.4.
  - `#protocol-search`: FUSE current §2.5 + §3.3 + §3.3.1.
  - `#protocol-conformance`: current §3.5 (4 lines today; keep brief).
  - `#alternatives`: NEW section, ~5-8 short paragraphs. Extract rationale that currently lives buried in invariant bodies and section prose. Specific items to cover:
    - WhenMatched::DoNothing vs Replace (inv 14 rationale)
    - SourceDedupeBehavior::FirstSeen at substrate (inv 17 rationale)
    - lance-namespace adoption (inv 21 rationale)
    - MemWAL / ShardWriter deferral (§4 lead-in, inv 24-28 rationale)
    - 90d uniform auto_cleanup retention (commit 1 decision; replaces scheme-keyed 30d local / 90d remote)
    - Two scan helpers (Handle::scan + Handle::scanner) instead of single entry (commit 1 deviation)
  - `#open-questions`: NEW section. Start with explicitly-named questions currently buried in conversation:
    - Multi-namespace router activation conditions
    - Live-write deferred activation conditions
    - Hosted-tier auth model (REST namespace vs federated dir)
  - `#deferred`: current §4, converted from prose to bullets where possible. Keep activation conditions inline with each bullet.

### Sub-pass 4: Ripple-update the 75 code refs

- Apply the anchor mapping table via Edit calls with `replace_all=true` per pattern, scoped to `src/`.
- Most-specific patterns first (`2.3 inv N` before `2.3`).
- After all replacements: `cargo build --all-targets` must remain clean (docstrings should still parse).
- Verification: `grep -rn "design\.md [0-9]" src/` should return zero matches.

### Sub-pass 5: Update CLAUDE.md `## Comments` example

- One Edit, as specified in the "CLAUDE.md ## Comments example update" section above.
- No verification needed beyond visual inspection of the diff.

## Verification gates

After all sub-passes:

```
cargo build --all-targets                          # docstrings still parse, no warning regressions
cargo test                                         # 72/72 still pass; behavior unchanged
cargo clippy --all-targets -- -D warnings          # no new warnings
```

Plus the grep assertions:

```
grep -rn "design\.md [0-9]" src/                   # zero matches; all section-number refs anchored
grep -rn "design\.md#" src/ | wc -l                # roughly 75 (some refs may consolidate via shared anchor)
grep -c "^### [0-9]" docs/design.md                # subsection count matches new structure (~20)
grep -c "{#" docs/design.md                        # every targeted anchor present (~35-40)
grep -c "^[0-9]\+\. MUST\|^[0-9]\+\. SHOULD\|^[0-9]\+\. MAY" docs/design.md   # 28 invariants, all keyworded
```

## Commit message draft

```
docs: restructure design.md to schemas/protocol shape with RFC normative language + stable anchors

- Restructures design.md from §1 What/§2 Foundations/§3 Sessions/§4 Deferred
  into a C-shape 8-section RFC outline: Status & abstract / Scope / Invariants
  (with per-#inv-N anchors) / Schemas (per-type blocks fusing canonical type +
  storage layout) / Protocol (per-op blocks fusing wire ops + ingest/search
  semantics) / Alternatives considered / Open questions / Deferred.
- Adds RFC normative keywords (MUST / SHOULD / MAY) inline on every invariant.
  Default is MUST; inv 13/17/26 preserve their existing mixed normative
  structure with explicit keywords on each clause.
- Every section gets a stable {#anchor-id} markdown anchor. Future restructures
  can renumber without invalidating cross-refs.
- Ripple-updates 75 code cross-references across src/ from
  `design.md X.Y.Z` form to `design.md#stable-anchor` form via mechanical
  replacements following the anchor mapping table in
  docs/plans/commit-2-design-restructure.md. Two stale refs (2.6, 3.6.6)
  canonicalized to their intended targets.
- Updates CLAUDE.md ## Comments <example> block to use the new anchor form,
  and adjusts the prose rule from "design.md <section>" to "design.md#<anchor>"
  to match.

No code behavior changes. cargo build, cargo test (72/72), cargo clippy all
remain clean.
```

## Out of scope (do not touch)

- Any `src/` code logic changes (the only edits to `src/` are mechanical cross-ref replacements).
- Any design.md content rewrites beyond the structural fusion (3.1.X + 3.2.X -> per-type blocks; 2.5 + 3.3 -> protocol-search; etc.) and the inline MUST/SHOULD/MAY prefixes. Don't paraphrase, condense, expand, or "improve" individual invariant or paragraph wording.
- Adding new invariants, removing existing ones, renumbering them (numbers 1-28 are preserved verbatim; only anchors change).
- Adding new schemas or new wire ops.
- The Alternatives Considered and Open Questions sections are NEW additions but their content comes from facts already established in the design or conversation - do not invent new alternatives or new open questions.
- Do not modify Cargo.toml, Cargo.lock, tests/, benches/, or any non-design file other than the 11 src/ files containing the cross-refs and CLAUDE.md.

## Decisions already made (settled before this commit)

- **NamespaceIdent threaded through Handle** (not dropped). Commit 1 already implements this; design.md inv 11 already says `Result<NamespaceIdent, ErrorEnvelope>`. No change needed to inv 11.
- **`Handle::scan` AND `Handle::scanner` as two helpers** (not single entry). Commit 1 settled this. design.md inv 22 currently says "all scans go through `Store::scan(table, opts)`" - update inv 22 prose during the restructure to acknowledge both helpers ("through `Handle::scan` or its composable companion `Handle::scanner` for FTS / vector callers"). This is content drift to match reality, not a behavior change; flag it in the Alternatives Considered section.
- **90d uniform `auto_cleanup` retention** on create (not scheme-keyed). Commit 1 settled this. Document in Alternatives Considered.
- **`Predicate::IsNotNull` variant** at the typed-predicate seam. Commit 1 added it. No design.md change needed - it's an implementation detail of the typed seam already required by inv 7 (no SQL).

## Protocol with the user

Same as commit 1:

1. Execute the 5 sub-passes in order.
2. Make the commit with the message above.
3. The user runs `/polish` in a separate session against the working tree (skill not available in my Skill tool list).
4. If polish surfaces issues, `git reset --mixed HEAD~1`, apply fixes, recommit.
5. If polish surfaces no issues, the commit stands as-is.

No push without explicit user instruction.
