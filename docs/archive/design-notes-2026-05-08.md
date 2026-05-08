# Design notes: 2026-05-08

## Context

Conversation reviewing the design before implementation. Captures the framing shifts and decisions reached during the session, plus open questions to revisit. Treat this as input for future edits to `design.md` and `unresolved-questions.md`, not a replacement.

## Framing shift: substrate plus applications

Pond is reconceived as two layers:

1. **Substrate (engine).** Generic primitives for typed-record and blob storage, hybrid search, embedding, multi-tenancy, and ingest. Defined in detail in `substrate.md`.
2. **Applications (consumer schemas).** Canonical types and source adapters for specific domains. v1 ships **sessions** and **resources**. A future **archives** application is on the radar (human-conversation imports from Discord, Slack, Reddit, Twitter, Telegram, forums), deferred until sessions and resources are built.

Implication: there is no universal Part union. Sessions has its own canonical types (the existing Part union from `design.md` §6). Resources has its own. Archives, when built, will have its own. The substrate is type-parametric over a generic `RecordSchema` trait; consumer types are owned by consumers.

## v1 scope changes

- **Drop cross-provider replay from v1.** Keep canonical types as storage shape only, not as projection target.
- ReplayProvider seam moves entirely into `design.md` §15 (deferred). Reactivate when the first integrator demands it.
- Pi-mono conformance matrix (U10) is no longer a v1 acceptance gate. Gate it on the day replay is reactivated.
- Three seams remain in v1: `ObjectStorage`, `EmbeddingProvider`, `SourceAdapter`.

## Code organization decision

**Single Cargo crate for v1.** Strict module discipline separates substrate from consumer code internally. The substrate module is treated as a published surface in spirit, with the acid tests in `substrate.md` §7 as the forcing function (Note schema test, two-schema coexistence test, domain-leakage grep, build-without-consumers test, doc-purity check).

This preserves the option to split into a workspace later if a sister application (archives, or anything else) wants to depend on the substrate without taking on sessions, resources, or the MCP facade. Delay the split until that second consumer is real code, not a plan.

The reasoning behind the single-crate choice: a crate boundary prevents one specific failure mode (reverse dependencies) but does not prevent the more common failure mode (substrate becoming a junk drawer of "things multiple consumers need"). The actual anti-drift mechanism is the narrow definition plus the acid tests, which apply equally well to one crate or two. Splitting now would force commitment to an interface shape before sessions are built, which is when the boundary is actually informed.

## Surface decision (Shape 1)

At the runtime level, sessions and resources ship in **one binary** behind **one MCP server**. Archives, when built, may join the same binary as a third application or split off as a sibling product. The single-crate choice biases toward folding archives in, which is fine until evidence pushes the other way.

Three nouns at the surface (`session`, `resource`, eventually `archive`) over two stores under the hood (typed-record datasets and content-addressed blobs).

## What the substrate is

See `substrate.md` for the full spec. Short version: storage, search, embedding, blob, namespace, and adapter primitives, all generic over consumer schemas. No domain types in any substrate signature. Acid tests guard against drift. Promotion and demotion criteria define when code moves across the boundary.

## What's still open and worth revisiting

1. **Trait shapes in `substrate.md` §5 are illustrative.** Real shapes will be discovered while building sessions. Update the spec once sessions ship.
2. **§19.1 (4 vs 6 datasets)** becomes clearer under the substrate framing. Each consumer owns its dataset count. Sessions: messages plus parts (if they remain separate) plus any consumer-specific tables. Resources: a blobs-with-metadata layout. Archives later: its own dataset shape. The substrate does not enumerate datasets.
3. **§19.2 (per-row schema_version):** still favored as drop. Lance manifest versioning plus dataset-level metadata covers it. Substrate exposes the helper; consumers do not carry the column.
4. **§19.3 / U6 (Lance file format version):** still needs the LanceDB-terms rewrite ("set `new_table_data_storage_version = stable`"). Substrate concern.
5. **§19.5 (branching primitive):** `parent_message_id` is a sessions concern, not a substrate concern. Move that recommendation into the sessions application doc when one exists.
6. **§19.6 (search_text rules per Part variant):** belongs in the sessions doc; substrate just guarantees that a record's `RecordSchema` implementation provides `search_text`.
7. **§19.7 / U8 (NamespaceResolver shape):** substrate concern. Trait shape stays loose until built.
8. **U1 (S3 plus DynamoDB coordinator):** substrate concern. Decision needed before any AWS hosted deployment.
9. **U2, U3, U4 (tool_type, token columns, provider registry):** all sessions-application concerns under the new framing. Move them out of `unresolved-questions.md` or relabel as sessions-specific.
10. **U5 (`read_consistency_interval` over manifest ETag):** substrate concern. Adopt the LanceDB built-in.
11. **U7 (EmbeddingProvider via Lance registry):** substrate concern. Decision needed at substrate impl time.
12. **U9 (OTel JSON schemas as wire contract):** facade concern, not substrate. Sessions wire contract may follow OTel; archives wire contract follows the conversation platforms it imports from.
13. **U10 (pi-mono conformance matrix):** deferred together with replay.
14. **U11 (streaming variants for live-write):** deferred together with live-write tools.
15. **Archives ingest scope:** not a v1 concern, but the platforms named in scope are Discord, Slack, Reddit, Twitter, Telegram, and forums. Use that list when designing the canonical archive types later.

## Suggested edits to existing docs

Made for the next session, when the design.md edits feel locked:

- `design.md` §1: reframe as "substrate plus applications" or add a sentence acknowledging the layering.
- `design.md` §3: drop "Cross-provider replay" from hard requirements; keep canonical-types ownership.
- `design.md` §6: keep the Part union, mark it as the sessions-application canonical, not pond's universal type.
- `design.md` §7: clarify that the dataset list is per-application, not pond-wide.
- `design.md` §8: drop ReplayProvider; keep three seams.
- `design.md` §12: drop replay tools from the v1 surface section.
- `design.md` §15: ReplayProvider stays deferred (no change in content; confirms position).
- `design.md` §17: drop replay from decision summary; add the substrate-plus-applications layering.
- `design.md` §20: optionally add a pointer to `substrate.md` as the canonical companion doc.
- `unresolved-questions.md`: U1, U5, U7 become substrate concerns; U2, U3, U4, U9 become sessions concerns; U10 deferred along with replay; U8 substrate; U11 deferred along with live-write.
- `README.md`: update Background and Design sections to reflect the substrate-plus-applications framing and the v1-no-replay decision.

## Off the table for v1

- Cross-provider replay (deferred until first integrator demand)
- Live-write MCP tools (`pond_commit`, `pond_session_open`)
- Wire-fidelity capture (`raw_request` / `raw_response`)
- HTTP facade
- Pi-mono conformance matrix as acceptance gate (gates replay, not v1)
- Archives application (canonical types, ingest, search) until sessions and resources ship

## What was discussed but not decided

- Whether archives, when built, ships inside the pond binary (Shape 1) or as a sibling product (Shape 3). The single-crate decision biases toward Shape 1; revisit when archives is a real branch with code.
- Whether to publish OTel-derived JSON Schemas as the sessions wire contract or invent a thinner pond-specific shape. Either is consistent with the substrate framing.
- The exact dataset count for the sessions application (4 vs 6 from §19.1, possibly fewer under the new framing).
