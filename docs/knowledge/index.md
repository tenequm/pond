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
- Concepts are filed **by type**, one directory per entry in the vocabulary:
  `decisions/`, `findings/`, `references/`, `runbooks/`. The directory is
  therefore derivable from the `type:` field rather than being a judgment
  call, the set never grows as pond does, and `repo:check-knowledge` enforces
  the match - a concept whose `type` and directory disagree fails the build.
  Changing a concept's `type` means moving the file.
- **The index groups by subject, the filesystem groups by type**, and they are
  not meant to agree. Recall is by subject ("why is sync slow on S3"), so the
  sections below cluster concepts that share a cause even when their types
  differ; filing is by type so it stays mechanical. Enter through this index.
- Links between concepts are relative to the linking file, so they stay
  clickable on GitHub. `repo:check-knowledge` fails the build on one that does
  not resolve, so a move is safe to make and impossible to half-finish.
- Every add, move, or removal updates this index and appends a dated entry to
  `log.md` in the same change. Concepts are deprecated
  (`status: deprecated` + successor link), never deleted.

## Storage and the object-store substrate

How Lance and S3 actually behave under pond's access patterns. The recurring
lesson is that on an object store latency is round-trip-bound, so nearly every
fix is "issue fewer requests".

- [Per-sync watermarks must never come from Dataset::versions() on a remote store](findings/s3-sync-change-detection-oracle.md) -
  The `versions()` row-version to commit-timestamp join costs 79s warm / 133s
  cold on S3 because the version list is a per-manifest fetch storm; a
  messages-based key is 0.5-0.6s warm, 25-160x faster.
- [Absent rows must append, never merge-insert, on a remote store](findings/s3-append-vs-merge-write-path.md) -
  Appending absent sessions took 13.8 min and 1 commit per table; routing the
  same rows through merge-insert took 75.7 min and 354 commits - 5.47x slower,
  because merge over S3 is commit-latency-bound.
- [cleanup_old_versions belongs off the per-sync hot path](findings/s3-sync-cleanup-amortization.md) -
  The version-log walk is round-trip-bound and reclaims about one version per
  run, yet ungated it ran on all three tables every sync; gating it on a
  per-table interval cut 48 walks to 6 over 16 folds.
- [Guard count_rows inequalities with IsNotNull, and count a narrow column](findings/lance-count-rows-predicate-guard.md) -
  A bare `Ne` on a nullable column in `Dataset::count_rows` ran over 25 minutes
  on ~2M rows; wrapping it as `And(IsNotNull, Ne)` dropped the same count to
  7.35 seconds.
- [Never tune Lance's AIMD request-rate limiter](decisions/object-store-request-rate-policy.md) -
  The throttling pond saw on Hetzner was a symptom of issuing too many
  requests, not of the limiter being too narrow; Lance mislabels transport
  timeouts as throttle errors, which once seeded a wrong theory.
- [Read path latency analysis and bottleneck attribution](findings/read-path-latency-findings.md) -
  77% of MCP tool calls exceeded 5s due to unindexed message ID resolution
  full-scans and rowmap bypasses in the get path issuing over 10,000 S3 GET
  requests per query.

## Search and retrieval

What pond's retrieval arms are worth, measured on real usage rather than
synthetic QA.

- [Embeddings are opt-in and FTS is the default search arm](decisions/embeddings-are-opt-in.md) -
  Semantic search became a local config switch defaulting off, vector mode is
  refused rather than silently downgraded when disabled, and no process
  constructs an embedder until asked.
- [Semantic vs BM25 retrieval efficacy and cost evaluation](findings/semantic-vs-fts-usage-findings.md) -
  In a 1,126-call production trace, BM25 resolved 61% of queries versus 37%
  for vector search, showing semantic retrieval uniquely helped only 6-7% of
  calls while adding substantial memory and ingest costs.
- [Hybrid search score normalization and memory footprint tuning](findings/embeddings-tuning-findings.md) -
  Score-normalized fusion with a 0.135:1 FTS-to-vector weight ratio improved
  paraphrase Success@3 to 64.9%, while macOS memory profiling showed
  phys_footprint is the only valid metric.
- [Bilingual FTS tokenizer evaluation and word tokenizer adoption](findings/tokenizer-experiment-findings.md) -
  Character 3-5 n-grams initially preserved Ukrainian inflection without
  English regression, but word tokenization with English stemming was
  ultimately adopted for 28x smaller index size and 2x English retrieval
  gains.
- [Value-oriented agent session retrieval and evaluation methodology](findings/session-retrieval-evaluation-findings.md) -
  Existing memory benchmarks measure synthetic QA vacuum metrics rather than
  coding task completion, highlighting the need for an issue-resolution
  ablation using raw lossless session archives.
- [pond retrieval tools redesign - research and reasoning (2026-06-19)](findings/retrieval-tools-redesign-research.md) -
  Field tests and failure traces motivated replacing auto-hybrid search with
  separate vector and FTS modes, adding recency boosting and pagination, and
  bounding tool outputs to 10k characters. Deprecated: the shipped tool
  surface is the outcome.

## Engineering process and platform

Release mechanics, CI, and portability decisions - the places where a wrong
assumption costs a release rather than a query.

- [A breaking marker only bumps the minor version when it rides a real-diff feat! or fix!](findings/release-plz-bump-semantics.md) -
  On v0.12.0 an empty `BREAKING CHANGE` commit and then a `docs!:` commit both
  left the release a patch; release-plz derives the bump only from `feat` and
  `fix` commit types with a real diff.
- [What a bench-gate row does and does not prove](findings/bench-gate-evidence-standards.md) -
  A gate row measures only the paths its probes name, one row cannot bracket a
  small effect on a remote store, and a read baseline is unrecoverable once the
  new binary has written.
- [Windows is msvc-only, and the msvc main-thread stack reserve is the trap](decisions/windows-msvc-port-decisions.md) -
  The gnu artifact shipped for releases without ever running in CI and was
  broken at runtime; msvc replaced it, but the 1 MiB msvc stack reserve
  overflows on pond's async state machine before argument parsing.
- [CI compiler-cache config is per-job and never at the repo root](decisions/ci-compiler-cache-architecture.md) -
  A repo-root `.kache.toml` would point every contributor's local build at a
  credential-less bucket, so config lives under `.github/kache/` reached only
  through `KACHE_CONFIG`, with one S3 prefix per build shape carrying its
  key-schema tag.

## Harness integrations

How the agent harnesses pond ingests actually store their sessions, and what
that implies for pond's position.

- [OpenClaw integration architecture and session storage findings](findings/openclaw-integration-findings.md) -
  OpenClaw stores sessions in per-agent SQLite trees and zstd archives with
  short retention, positioning pond as a durable cross-harness archive rather
  than an in-gateway memory competitor.

## Reading a pond store

Recurring analysis over pond's own data.

- [Computing token usage and cost from a pond store](runbooks/token-usage-accounting.md) -
  Turn options usage fields into correct token counts and dollars - dedup by
  provider message id first, or overstate by 2x or more.
- [Fable 5 vs Fable 5.1: the queries behind the per-prompt comparison](findings/fable-5-vs-5-1-usage-queries.md) -
  Fable 5.1 generated 31% more output tokens per prompt with 15% fewer tool
  calls while reducing per-prompt API cost by 31% over 22,022 measured calls.

## Adapter references

Upstream format archaeology and mapping decision records, one per source
agent. The add-adapter playbook writes one of these per new adapter:

- [agy](references/agy.md) - Upstream format archaeology and mapping decision
  record for the agy adapter.
- [grok-build](references/grok-build.md) - Upstream format archaeology and
  mapping decision record for the grok-build adapter.
- [letta-code](references/letta-code.md) - Upstream format archaeology and
  mapping decision record for the letta-code adapter.
