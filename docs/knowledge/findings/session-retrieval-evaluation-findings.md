---
type: Finding
title: Value-oriented agent session retrieval and evaluation methodology
description: Existing memory benchmarks measure synthetic QA vacuum metrics rather than coding task completion; the measurement that connects retrieval to value is an issue-resolution ablation over raw lossless session archives.
tags: [evaluation, benchmarks, memory, retrieval, swe-bench]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T14:49:41Z" }
sources:
  - id: session-eval-research
    resource: ../../researches/agent-session-retrieval-and-evaluation.md
    title: Agent-session retrieval and value-oriented evaluation (2026-05-22)
---

# Value-oriented agent session retrieval and evaluation methodology

## Headline claims

- Standard memory benchmarks like LoCoMo are structurally flawed: 6.4% of ground-truth answers contain errors and weak judges accept 62.81% of incorrect answers, establishing an artificial ~93.57% score ceiling.[^session-eval-research]
- MemoryArena (ICML 2026) demonstrated that agents achieving high scores on conversational QA benchmarks fail on multi-session interdependent tasks, separating vacuum metrics from task value.[^session-eval-research]
- Experience-following behavior causes agents to replicate errors from retrieved low-quality sessions, making outcome and error metadata filtering critical for retrieval safety.[^session-eval-research]
- Retrieval granularity literature (EMNLP 2024 Dense X Retrieval) confirms atomic message units outperform passage chunks, validating message-level indexing.[^session-eval-research]
- Attention degradation ("Lost in the Middle") implies that precision at ranks 1-3 is far more valuable to an agent than recall at rank 20.[^session-eval-research]
- Lossless raw session archives uniquely enable end-to-end task-completion benchmarks that summarization-based architectures (Mem0, Zep) cannot reconstruct.[^session-eval-research]
- The evaluation instrument that actually measures retrieval value is an issue-resolution ablation over SWE-bench Verified: issue resolution rate, step count and token cost, each measured with and without prior session retrieval. Those three are what tie retrieval to task completion; conversational QA accuracy does not.[^session-eval-research]
- Honest benchmark reporting requires disclosing retrieval-only recall metrics (R@k) rather than presenting retrieval numbers as conversational QA accuracy.[^session-eval-research]

## Invalidation

These findings would be invalidated if synthetic conversational QA benchmarks are demonstrated to correlate strongly with real software engineering task completion, or if agent reasoning architectures become fully immune to misleading retrieved context.

## Source

Full research report: [docs/researches/agent-session-retrieval-and-evaluation.md](../../researches/agent-session-retrieval-and-evaluation.md).

[^session-eval-research]: `docs/researches/agent-session-retrieval-and-evaluation.md`
