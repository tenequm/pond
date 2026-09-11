---
type: Decision
title: Embeddings are opt-in and FTS is the default search arm
description: Semantic search became a local config switch defaulting off, vector mode is refused rather than silently downgraded when disabled, and no process constructs an embedder until asked.
tags: [embeddings, search, config, decision]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: opt-in-plan
    resource: ../../plans/2608-21-embeddings-opt-in-plan.md
    title: "Embeddings opt-in implementation plan (the long form, all 19 settled decisions)"
  - id: issue164
    resource: https://github.com/tenequm/pond/issues/164
    title: "Issue #164: embeddings opt-in"
---

# Embeddings are opt-in and FTS is the default search arm

Spec 8.7 had said "Embedding is opt-in by configuration" while the code carried
no such switch. This closed the gap rather than adding a new
capability.[^opt-in-plan]

## The shape of the decision

- `[embeddings].enabled` is a **local config read** installed into a
  process-wide `OnceLock` at startup. No store probe, no auto-detect of
  existing vectors, no migration. `POND_EMBEDDINGS_ENABLED` mirrors it for
  containers and CI, and must be the literal `true`/`false` - figment
  stringifies `1`, which fails the bool field.
- Default `mode` for `pond_search` is `fts` on every surface: MCP stdio, HTTP,
  and CLI.
- When disabled, `mode=vector` is **refused, never silently downgraded**. The
  refusal is the first thing `resolve_effective_mode` does, before
  `has_embeddings()`, which on a never-embedded store is a full vector-column
  read. On MCP it is a `CallToolResult::error` so the model reads it in-turn
  and retries with fts.
- When disabled, no process ever constructs an embedder or loads a model, and
  the IVF intent is absent from `pond_index_intents()` - which removes it from
  fold, rebuild, cleanup, status, and copy-verify in one place. Existing
  vectors and the IVF index stay on disk untouched.

## Why off is the right default

The evidence is in [the semantic versus BM25
evaluation](../findings/semantic-vs-fts-usage-findings.md): in a 1,126-call production
trace, BM25 resolved 61% of queries against 37% for vector search, and semantic
retrieval uniquely helped only 6-7% of calls while adding substantial memory
and ingest cost. Paying a model load and an embed stage by default to serve
that margin is the wrong trade for most installs.

The same corpus retired `--min-score`: 0 of those 1,126 real `pond_search`
calls passed it, it was never on the MCP surface, and spec 8.3 says scores
carry no absence signal.

## Rollout order does not matter, by construction

The store format is unchanged, pond MCP self-describes through server
`instructions` that always come from the running binary, plugin tool
descriptions are version-neutral (they describe both modes without naming a
default), and `SKILL.md` carries stable routing only. Every behaviour fact
lives in the one surface that cannot go stale.

A mixed fleet - one machine enabled, one disabled, same remote store - is
supported: the disabled machine's rows stay un-embedded until an enabled
machine runs `pond optimize --only embed`.

## Invalidation

This would be revisited if semantic retrieval's unique contribution rose well
above the measured 6-7%, or if model load stopped being a meaningful cost.

[^opt-in-plan]: `docs/plans/2608-21-embeddings-opt-in-plan.md`
[^issue164]: [tenequm/pond#164](https://github.com/tenequm/pond/issues/164)
