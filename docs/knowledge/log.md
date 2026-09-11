# Log

Newest first. One `## YYYY-MM-DD` heading per day with changes.

Links point at where a concept lives now, not where it landed on the day of
the entry - the prose is the history, the link is a working reference.

## 2026-09-11

- Filed every concept under a subject-area directory (`storage/`, `search/`,
  `engineering/`, `integrations/`, `usage/`, alongside the existing
  `adapters/`) and recorded the layout rule in the index preamble. The bundle
  doubled in one day and a flat directory stopped being navigable.
- Added [Per-sync watermarks must never come from Dataset::versions() on a remote store](storage/s3-sync-change-detection-oracle.md),
  [Absent rows must append, never merge-insert, on a remote store](storage/s3-append-vs-merge-write-path.md),
  [cleanup_old_versions belongs off the per-sync hot path](storage/s3-sync-cleanup-amortization.md),
  [Guard count_rows inequalities with IsNotNull, and count a narrow column](storage/lance-count-rows-predicate-guard.md),
  [Never tune Lance's AIMD request-rate limiter](storage/object-store-request-rate-policy.md),
  [A breaking marker only bumps the minor version when it rides a real-diff feat! or fix!](engineering/release-plz-bump-semantics.md)
  and [What a bench-gate row does and does not prove](engineering/bench-gate-evidence-standards.md),
  migrated out of AGENTS.md rationale paragraphs under this bundle's own
  instructions fence - AGENTS.md keeps the law, these carry the evidence.
- Added [Embeddings are opt-in and FTS is the default search arm](search/embeddings-are-opt-in.md),
  [Windows is msvc-only, and the msvc main-thread stack reserve is the trap](engineering/windows-msvc-port-decisions.md)
  and [CI compiler-cache config is per-job and never at the repo root](engineering/ci-compiler-cache-architecture.md)
  (Decision), backfilled from landed plans in `docs/plans/`.
- Bundle created. First concept: [token-usage-accounting](usage/token-usage-accounting.md)
  (Runbook), migrated from `docs/other/2608-27-token-usage-accounting.md`.
- Added [adapters/agy](adapters/agy.md) (Reference), migrated from `docs/adapters/`.
- Added [adapters/grok-build](adapters/grok-build.md) (Reference), migrated from `docs/adapters/`.
- Added [adapters/letta-code](adapters/letta-code.md) (Reference), migrated from `docs/adapters/`.
- Added [Fable 5 vs Fable 5.1: the queries behind the per-prompt comparison](usage/fable-5-vs-5-1-usage-queries.md)
  (Finding), migrated from `docs/other/2609-02-fable-5-vs-5-1-usage-queries.md`.
- Added [pond retrieval tools redesign - research and reasoning (2026-06-19)](search/retrieval-tools-redesign-research.md)
  (Finding, deprecated), migrated from `docs/check-later/2606-19-pond-retrieval-tools-redesign-research.md`.
- Added [OpenClaw integration architecture and session storage findings](integrations/openclaw-integration-findings.md)
  (Finding), distilled from `docs/researches/2607-17-openclaw-integration-research.md`.
- Added [Read path latency analysis and bottleneck attribution](storage/read-path-latency-findings.md)
  (Finding), distilled from `docs/researches/2608-12-read-path-where-time-goes.md`.
- Added [Semantic vs BM25 retrieval efficacy and cost evaluation](search/semantic-vs-fts-usage-findings.md)
  (Finding), distilled from `docs/researches/2608-21-semantic-vs-fts-usage-eval/`.
- Added [Value-oriented agent session retrieval and evaluation methodology](search/session-retrieval-evaluation-findings.md)
  (Finding), distilled from `docs/researches/agent-session-retrieval-and-evaluation.md`.
- Added [Hybrid search score normalization and memory footprint tuning](search/embeddings-tuning-findings.md)
  (Finding), distilled from `docs/researches/embeddings.md`.
- Added [Bilingual FTS tokenizer evaluation and word tokenizer adoption](search/tokenizer-experiment-findings.md)
  (Finding), distilled from `docs/researches/tokenizer-experiment-report.md`.
