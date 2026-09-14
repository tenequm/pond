# Log

Newest first. One `## YYYY-MM-DD` heading per day with changes.

Links point at where a concept lives now, not where it landed on the day of
the entry - the prose is the history, the link is a working reference.

## 2026-09-11

- Deprecated [Hybrid search score normalization and memory footprint tuning](findings/embeddings-tuning-findings.md).
  It was `status: stable` while documenting `FTS_FUSION_WEIGHT`,
  `VECTOR_FUSION_WEIGHT`, `RECENCY_MAX_BOOST` and cross-arm fusion, none of
  which exist: the fusion path was deleted on 2026-06-20 and `pond_search` now
  runs one arm per request. The measurements are real, so they stay, moved into
  past tense under a successor link to
  [Embeddings are opt-in and FTS is the default search arm](decisions/embeddings-are-opt-in.md).
  The same pass reconciled its 5-minute idle eviction against the 60s the two
  sibling concepts quote - 5 minutes was the constant when the memory figures
  were measured (2026-05-27), 60s has been the constant since 2026-06-20 - and
  restated the deferred cross-encoder reranker as the cost constraint that was
  measured rather than as an intention.
- Gave [pond retrieval tools redesign](findings/retrieval-tools-redesign-research.md)
  the successor link its own `status: deprecated` requires, in the concept and
  in the index line; prose naming a successor is not a link. Also fixed a
  `src/handlers.rs` path that predates the crate move to `packages/pond/`,
  reconciled its `0.3:1` fusion weight against the `0.135:1` in the tuning
  concept (two successive values of one constant, both now deleted), and
  removed a citation to a personal agent-memory key no reader of this repo
  could resolve - the publicity fence covers references as well as secrets.
- Cited three footnote definitions that were defined but never referenced, so
  they rendered as stray footnotes attributing nothing: `[^results]` in
  [bench-gate evidence standards](findings/bench-gate-evidence-standards.md),
  `[^issue164]` in [embeddings are opt-in](decisions/embeddings-are-opt-in.md),
  `[^sync-copy-plan]` in [s3 append vs merge](findings/s3-append-vs-merge-write-path.md).
  Each now sits on the claim it actually supports; none was dropped, because
  each had one.
- Durability-fence pass on two concepts that stated in-flight or live upstream
  work as durable findings.
  [Session retrieval evaluation](findings/session-retrieval-evaluation-findings.md)
  now states the evaluation instrument the research settles on instead of a
  proposed ablation.
  [OpenClaw integration](findings/openclaw-integration-findings.md) keeps its
  upstream observation but dates it to the 2026-07-17 snapshot and carries a
  `stale_after`, which is the fence's own remedy for knowledge that decays on
  an upstream release rather than on pond's next commit.
- Corrected this index's claim that `repo:check-knowledge` makes a move
  "impossible to half-finish". It checks only links written inside the bundle;
  references pointing into the bundle from `docs/spec.md`, the add-adapter
  playbook and adapter doc comments are invisible to it, which is how a rename
  once left 19 dangling references behind a green gate. The preamble now says
  what the gate covers, what it does not, and to sweep the repo by hand after a
  move. The same preamble settles a convention the validator cannot enforce: a
  `sources[].resource` naming a repo file is written relative to the concept,
  like every other link. Eight resources that were repo-root-relative were
  converted.
- Split the adapter concepts' duplicated verification fact. The date now lives
  only in the frontmatter `verified` event and the body line keeps only what
  the doc was checked against, in [agy](adapters/agy.md),
  [grok-build](adapters/grok-build.md) and
  [letta-code](adapters/letta-code.md); the add-adapter playbook was updated to
  require `verified` in the frontmatter and to stop asking for a second copy of
  the date in the body. The adapters' `sources[]` entries also gained the
  `title` every other concept supplies.

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
