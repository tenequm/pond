---
okf_version: "0.2"
---

# Pond knowledge base

Durable project knowledge for pond: decisions with rationale, findings with
evidence, references to upstream formats, runbooks for recurring analysis.
An entry belongs here only when it is true and useful in a fresh clone, to a
reader who never used the session that produced it.

What this bundle refuses:

- **State.** Plans and in-flight work live in `docs/plans/`; benchmark
  measurement logs live in `packages/pond/benches/docs/`. A number belongs in
  a concept only when the number IS the finding.
- **Standing orders.** `AGENTS.md` carries the laws agents obey every session;
  when a decision produces both a law and a rationale, AGENTS.md gets the
  one-liner and the rationale lives here as a concept.
- **Secrets and host-local context.** Every concept is written as if the
  repository goes public tomorrow: no credentials, no absolute paths, no
  machine names.

Conventions:

- Type vocabulary: `Decision`, `Finding`, `Reference`, `Runbook`.
- One concept per file, YAML frontmatter with at least a non-empty `type`;
  full frontmatter (`description`, `sources`, `generated`, `stale_after`
  where content rots) is the standard set by the first concept.
- Adapter format references (upstream format archaeology and mapping decision
  records, one per source agent) live under `adapters/`.
- Links between concepts are relative to the linking file, so they stay
  clickable on GitHub.
- Every add, move, or removal updates this index and appends a dated entry to
  `log.md` in the same change. Concepts are deprecated
  (`status: deprecated` + successor link), never deleted.

## Concepts

- [Computing token usage and cost from a pond store](token-usage-accounting.md) -
  Turn options usage fields into correct token counts and dollars - dedup by
  provider message id first, or overstate by 2x or more.
- [Fable 5 vs Fable 5.1: the queries behind the per-prompt comparison](fable-5-vs-5-1-usage-queries.md) -
  Fable 5.1 generated 31% more output tokens per prompt with 15% fewer tool
  calls while reducing per-prompt API cost by 31% over 22,022 measured calls.
- [pond retrieval tools redesign - research and reasoning (2026-06-19)](retrieval-tools-redesign-research.md) -
  Field tests and failure traces motivated replacing auto-hybrid search with
  separate vector and FTS modes, adding recency boosting and pagination, and
  bounding tool outputs to 10k characters. Deprecated: the shipped tool
  surface is the outcome.
- [OpenClaw integration architecture and session storage findings](openclaw-integration-findings.md) -
  OpenClaw stores sessions in per-agent SQLite trees and zstd archives with
  short retention, positioning pond as a durable cross-harness archive rather
  than an in-gateway memory competitor.
- [Read path latency analysis and bottleneck attribution](read-path-latency-findings.md) -
  77% of MCP tool calls exceeded 5s due to unindexed message ID resolution
  full-scans and rowmap bypasses in the get path issuing over 10,000 S3 GET
  requests per query.
- [Semantic vs BM25 retrieval efficacy and cost evaluation](semantic-vs-fts-usage-findings.md) -
  In a 1,126-call production trace, BM25 resolved 61% of queries versus 37%
  for vector search, showing semantic retrieval uniquely helped only 6-7% of
  calls while adding substantial memory and ingest costs.
- [Value-oriented agent session retrieval and evaluation methodology](session-retrieval-evaluation-findings.md) -
  Existing memory benchmarks measure synthetic QA vacuum metrics rather than
  coding task completion, highlighting the need for an issue-resolution
  ablation using raw lossless session archives.
- [Hybrid search score normalization and memory footprint tuning](embeddings-tuning-findings.md) -
  Score-normalized fusion with a 0.135:1 FTS-to-vector weight ratio improved
  paraphrase Success@3 to 64.9%, while macOS memory profiling showed
  phys_footprint is the only valid metric.
- [Bilingual FTS tokenizer evaluation and word tokenizer adoption](tokenizer-experiment-findings.md) -
  Character 3-5 n-grams initially preserved Ukrainian inflection without
  English regression, but word tokenization with English stemming was
  ultimately adopted for 28x smaller index size and 2x English retrieval
  gains.

## Adapter references

Upstream format archaeology and mapping decision records, one per source
agent, under `adapters/`:

- [agy](adapters/agy.md) - Upstream format archaeology and mapping decision
  record for the agy adapter.
- [grok-build](adapters/grok-build.md) - Upstream format archaeology and
  mapping decision record for the grok-build adapter.
- [letta-code](adapters/letta-code.md) - Upstream format archaeology and
  mapping decision record for the letta-code adapter.
