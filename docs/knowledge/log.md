# Log

Newest first. One `## YYYY-MM-DD` heading per day with changes.

Links point at where a concept lives now, not where it landed on the day of
the entry - the prose is the history, the link is a working reference.

## 2026-09-11

- Filed every concept by type (`decisions/`, `findings/`, `adapters/`,
  `runbooks/`), replacing both the original flat layout and a short-lived
  subject-area one. The bundle doubled in a day and flat stopped being
  navigable, but subject directories turned filing into a judgment call that
  would need new categories as pond grows. Type directories are derivable from
  frontmatter, closed, and now enforced by `repo:check-knowledge`. Subject
  grouping moved to the index, which is the entry point anyway. The type is
  named `Adapter`, not the generic `Reference` the format's example
  vocabulary suggests, so that the directory keeps the name that means
  something to a reader looking for adapter format archaeology.
- Added [Per-sync watermarks must never come from Dataset::versions() on a remote store](findings/s3-sync-change-detection-oracle.md),
  [Absent rows must append, never merge-insert, on a remote store](findings/s3-append-vs-merge-write-path.md),
  [cleanup_old_versions belongs off the per-sync hot path](findings/s3-sync-cleanup-amortization.md),
  [Guard count_rows inequalities with IsNotNull, and count a narrow column](findings/lance-count-rows-predicate-guard.md),
  [Never tune Lance's AIMD request-rate limiter](decisions/object-store-request-rate-policy.md),
  [A breaking marker only bumps the minor version when it rides a real-diff feat! or fix!](findings/release-plz-bump-semantics.md)
  and [What a bench-gate row does and does not prove](findings/bench-gate-evidence-standards.md),
  migrated out of AGENTS.md rationale paragraphs under this bundle's own
  instructions fence - AGENTS.md keeps the law, these carry the evidence.
- Added [Embeddings are opt-in and FTS is the default search arm](decisions/embeddings-are-opt-in.md),
  [Windows is msvc-only, and the msvc main-thread stack reserve is the trap](decisions/windows-msvc-port-decisions.md)
  and [CI compiler-cache config is per-job and never at the repo root](decisions/ci-compiler-cache-architecture.md)
  (Decision), backfilled from landed plans in `docs/plans/`.
- Bundle created. First concept: [token-usage-accounting](runbooks/token-usage-accounting.md)
  (Runbook), migrated from `docs/other/2608-27-token-usage-accounting.md`.
- Added [agy](adapters/agy.md) (Adapter), migrated from `docs/adapters/`.
- Added [grok-build](adapters/grok-build.md) (Adapter), migrated from `docs/adapters/`.
- Added [letta-code](adapters/letta-code.md) (Adapter), migrated from `docs/adapters/`.
- Added [Fable 5 vs Fable 5.1: the queries behind the per-prompt comparison](findings/fable-5-vs-5-1-usage-queries.md)
  (Finding), migrated from `docs/other/2609-02-fable-5-vs-5-1-usage-queries.md`.
- Added [pond retrieval tools redesign - research and reasoning (2026-06-19)](findings/retrieval-tools-redesign-research.md)
  (Finding, deprecated), migrated from `docs/check-later/2606-19-pond-retrieval-tools-redesign-research.md`.
- Added [OpenClaw integration architecture and session storage findings](findings/openclaw-integration-findings.md)
  (Finding), distilled from `docs/researches/2607-17-openclaw-integration-research.md`.
- Added [Read path latency analysis and bottleneck attribution](findings/read-path-latency-findings.md)
  (Finding), distilled from `docs/researches/2608-12-read-path-where-time-goes.md`.
- Added [Semantic vs BM25 retrieval efficacy and cost evaluation](findings/semantic-vs-fts-usage-findings.md)
  (Finding), distilled from `docs/researches/2608-21-semantic-vs-fts-usage-eval/`.
- Added [Value-oriented agent session retrieval and evaluation methodology](findings/session-retrieval-evaluation-findings.md)
  (Finding), distilled from `docs/researches/agent-session-retrieval-and-evaluation.md`.
- Added [Hybrid search score normalization and memory footprint tuning](findings/embeddings-tuning-findings.md)
  (Finding), distilled from `docs/researches/embeddings.md`.
- Added [Bilingual FTS tokenizer evaluation and word tokenizer adoption](findings/tokenizer-experiment-findings.md)
  (Finding), distilled from `docs/researches/tokenizer-experiment-report.md`.
