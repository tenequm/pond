# Agent-session retrieval and value-oriented evaluation

Research snapshot compiled 2026-05-22 from a parallel multi-agent deep-research pass, with every paper and benchmark verified against arxiv.org. This is a landscape review and reading list, not part of the spec contract - `docs/spec.md` remains the source of truth. It exists to ground pond's search design in the literature and, above all, to flag the paper pond should write.

---

## Standing reminder: pond should write the paper

This is the most important section of this document. Everything else is supporting material.

The benchmark literature for agent memory measures the wrong thing - see Section 1. MemoryArena (ICML 2026) proved it directly: agents that near-saturate the popular conversational-memory benchmarks collapse on interdependent multi-session tasks. But MemoryArena's value-measuring paradigm - end-to-end task completion across dependent sessions, with no QA probes - has never been applied to the coding-agent domain.

pond is the substrate that makes that benchmark buildable. pond is a lossless, raw archive of real agentic coding sessions (Claude Code, Codex). No other system has data in that shape - every competitor extracts, summarizes, or graphs, and therefore cannot reconstruct the raw record. A benchmark of 50-100 interdependent coding tasks, built from real pond-stored sessions and scored by actual issue-resolution rate with vs without session retrieval, would be the field's first genuine value benchmark for agent-session memory in software engineering.

That is a paper. Write it. Do not let this document become the place the idea went to be forgotten.

The concrete shape of the paper is in Section 6. Its preliminary results table is the ablation in Section 5, which is worth running on its own merits.

---

## 1. The reframe: pond is a retrieval substrate, not a memory system

Most "memory tool" benchmarks measure vacuum metrics: QA accuracy, recall, latency, and token counts over synthetic multi-session chat. They do not measure whether an agent completes real work better because the tool exists. The instinct that this is the wrong measure is correct, and it is now citable.

LoCoMo - the benchmark almost every memory tool reports - is empirically broken:

- 99 of 1,540 questions (6.4%) have wrong ground-truth answers: hallucinated facts, wrong date math, swapped speaker attributions.
- The standard GPT-4o-mini judge accepts 62.81% of intentionally wrong but topically-adjacent answers.
- The corrupted labels impose a ~93.57% scoring ceiling, so sub-10-point score differences are noise.
- Re-scoring with a strict judge (LoCoMo-Refined, April 2026) drops every published system 15-22 points.

This is corroborated by five-plus independent parties (the dial481/locomo-audit report, LoCoMo-Refined, the Zep vs Mem0 dispute, MemEval/Prosus, and multiple practitioner post-mortems).

MemoryArena (arXiv:2602.16313, ICML 2026) supplies the punchline: agents that near-saturate LoCoMo fail dramatically on interdependent multi-session tasks. The vacuum/value gap is measured, not a matter of taste.

The reframe that resolves how pond should be evaluated: pond is a retrieval substrate, not a memory system. That splits the literature and the metrics cleanly into two questions that must never be conflated:

1. Is pond's retrieval good? Answered by retrieval-quality research and R@k / P@k metrics. This is inherently a "vacuum" measure - and that is acceptable for a substrate, provided the numbers are labeled honestly as retrieval metrics.
2. Does pond make an agent better at its job? Answered by value benchmarks and with/without ablations. This is the harder and more meaningful question, and the reason the paper in Section 6 matters.

The model for honest reporting already exists in the wild: `agentmemory` publishes "retrieval-only recall, NOT LongMemEval QA scores." pond should adopt the same disclosure discipline for any benchmark number it ever publishes.

## 2. The landscape: icm and agentmemory

Two open-source projects were examined as the nearest neighbors to pond. Neither is a usable benchmark; both confirm that no real value benchmark exists for this problem.

`rtk-ai/icm` - 365 stars, Rust, single static binary, Apache-2.0, pre-1.0. Its `transcript` subsystem (verbatim session storage with FTS5/BM25 search, MCP-exposed) is a genuine partial overlap with pond. Differences: BM25-only on transcripts (no vector hybrid), SQLite + sqlite-vec rather than Lance, and the rest of icm is extractive memory (episodic "Memories" and a "Memoirs" knowledge graph) - a layer that would sit on top of pond. Its benchmarks are self-reported and vacuum; the headline "LongMemEval 100% retrieval" is the oracle variant (evidence sessions pre-filtered into the index), which is disclosed but easy to misread.

`rohitg00/agentmemory` - ~15.8k stars (briefly the #1 trending repo on GitHub in May 2026), TypeScript, Apache-2.0, extractive. It curates "observations" before storage; pond stores raw. It is therefore a different layer - a potential consumer of pond, not a competitor. Its one genuinely useful artifact is `coding-agent-life-v1`, a small retrieval eval built from coding-agent sessions: the right idea, but 15 questions is far too small to be statistically meaningful. Its honest benchmark disclosure (retrieval recall is labeled as such, not passed off as QA accuracy) is the practice worth copying.

## 3. Reading list: efficient conversation retrieval

### 3.1 The retrieval engine - validates pond's spec design directly

- Reciprocal Rank Fusion - Cormack, Clarke, Buettcher, SIGIR 2009 (not on arXiv). The fusion algorithm in pond's spec. The k=60 constant comes from here; the paper proves unsupervised rank fusion beats either retriever alone with no score calibration.
- Contextual Retrieval - Anthropic engineering blog, 2024 (not on arXiv). BM25 + dense retrieval cuts top-20 retrieval failure ~49% versus dense alone, across code, prose, and structured documents. Empirical backing for pond's hybrid.
- Dense X Retrieval (arXiv:2312.06648) - EMNLP 2024. A retrieval-granularity study: finer atomic units beat passage-level chunks. This is the empirical defense of message-granularity indexing - pond does not need to argue the choice, this paper already did.
- Lost in the Middle (arXiv:2307.03172) - TACL 2024. Models barely use information in the middle of their context. Implication for pond: precision at rank 1-3 matters far more than recall at rank 20. Rank well and return few.
- Toward Conversational Agents with Context- and Time-Sensitive Long-term Memory (arXiv:2406.00057). Pure semantic + BM25 retrieval fails on temporal and metadata queries ("what did I try last Tuesday"). pond's `search-prefilter-pushdown` already addresses this - temporal and metadata bounds run pre-rank (a refine for `timestamp`, which is deliberately unindexed) - a seam to keep, not add.

### 3.2 Retrieval over agent sessions - pond's actual use case

- Synapse (arXiv:2306.07863) - the closest architectural twin to pond: store raw (compressed) trajectories, retrieve by task similarity, inject as few-shot exemplars. Reports +56% relative improvement on Mind2Web from raw-trajectory retrieval alone, with no distillation step.
- Evo-Memory (arXiv:2511.20857) and ExpRAG (arXiv:2603.18272) - a fixed, read-only bank of raw trajectories, retrieved by nearest-neighbor search, is a strong baseline competitive with more elaborate systems. This validates pond's no-summarization bet.
- How Memory Management Impacts LLM Agents: Experience-Following Behavior (arXiv:2505.16067) - the single most important paper for pond's design. Agents follow retrieved experiences strongly, so a bad past session in the store actively degrades the next task - the effect is harmful, not neutral. Implication: pond must expose a session's outcome (success/failure, error signals) as retrieval-filterable metadata, not merely as content. pond's `provenance` field is a start; an outcome/quality signal needs to be filterable too.
- ReasoningBank (arXiv:2509.25140) - distilled reasoning strategies outperform raw-trajectory retrieval on web navigation. Read this as the case for the layer that sits on top of pond: pond is the raw substrate that feeds a distillation consumer.

### 3.3 The memory-system layer above pond - context, not direct guidance

These systems all extract, summarize, or graph the conversation. Among research systems, none is a lossless raw archive - which is precisely pond's differentiation. Know them as the layer that consumes pond, not as pond itself: MemGPT (arXiv:2310.08560), Mem0 (arXiv:2504.19413), Zep / Graphiti (arXiv:2501.13956), A-MEM (arXiv:2502.12110). Survey for orientation: A Survey on the Memory Mechanism of LLM-based Agents (arXiv:2404.13501).

## 4. Benchmarks: the top 5 that measure value

Honest caveat: there is no benchmark that cleanly measures "a coding agent retrieves its own past sessions and does better." The five below are the closest available; all were confirmed by live arXiv crawl; they are ranked by usefulness to pond.

1. MemoryArena (arXiv:2602.16313, ICML 2026) - the paradigm to copy. Multi-session interdependent tasks where success is defined as completing task N using memory of tasks 1..N-1, with no QA probes anywhere. Its headline finding is pond's thesis. Caveat: domains are shopping, travel, and reasoning, not coding.
2. SWE-ContextBench (arXiv:2602.08316) - real GitHub issues with real cross-issue dependencies; measures issue-resolution-rate delta with vs without prior-issue context. The closest published evidence that retrieved prior coding work raises task success and cuts token cost. Caveat: its "memory" is curated issue summaries, not raw sessions.
3. MemoryCode (arXiv:2502.13791, ACL 2025) - multi-session coding-convention tracking: the agent must apply the most-recently-updated convention from a long history, graded by code pass/fail. A direct domain match - it is the Claude-Code-onboarding scenario. Caveat: tasks are deliberately simple to isolate retrieval.
4. Mem2ActBench (arXiv:2601.19935) - measures whether an agent produces functionally correct tool calls when the required parameters live only in session history. This is pond's exact value chain: retrieve context, then ground the tool call. 91% of tasks are human-verified as memory-dependent.
5. Memory Transfer Learning / MTL (arXiv:2604.14004) - the cleanest A/B design: pass-rate on six real coding benchmarks (including SWE-bench Verified) with vs without memory. Confirms the effect is real but modest (~3.7% average) and that abstracted insights transfer better than raw traces.

Deliberately excluded:

- LoCoMo (arXiv:2402.17753) - empirically broken, see Section 1.
- LongMemEval (arXiv:2410.10813) - the pragmatic choice if pond wants one quickly-comparable retrieval number, but it is vacuum QA. If pond reports it, report retrieval R@k only and say so explicitly.
- ContextBench (arXiv:2602.05892) - an excellent real-repo coding-context retrieval benchmark, but vacuum (recall/precision against gold contexts, no task outcome).
- MemoryAgentBench (arXiv:2507.05257) - the most rigorous benchmark overall on competency coverage, but still QA-probe vacuum.

## 5. How pond should measure its own value

The benchmarks in Section 4 are for comparison and external credibility. The real measurement of pond's value is an ablation pond runs itself. Both the experiential-learning research and the benchmark research converged on this independently.

The experiment: with-pond vs without-pond on SWE-bench Verified.

- Group tasks by repository and sort chronologically. The first ~40% of each repo's issues become pond's session store; the last ~60% are the test set.
- without-pond: the agent runs on test tasks with no session retrieval. with-pond: before each test task the agent queries pond and the top-k past sessions are injected.
- Primary metric: resolved% delta. Report it split two ways - recurring-pattern tasks (a related prior issue exists in the store) versus genuinely novel tasks. pond should help on the former and, critically, not hurt on the latter.
- Control condition: with-pond-degraded, injecting deliberately poor matches (retrieval rank 10-15). If that degrades performance versus baseline, it confirms the experience-following risk (Section 3.2) and proves retrieval quality is load-bearing.
- Secondary metrics: step count and token cost. No paper in the surveyed literature measures token-cost delta from memory - reporting it is a free, honest, and novel contribution.

## 6. The paper to write

The gap. Every benchmark for agent / conversational memory measures vacuum metrics - QA accuracy over synthetic chat. MemoryArena established that this does not predict real agentic task success. No benchmark applies a value-oriented, task-completion paradigm to the coding-agent domain, even though coding agents are the most economically significant deployment of agentic memory.

The contribution. A value-oriented benchmark for agent-session memory in software engineering:

- A set of 50-100 interdependent multi-session coding tasks - sequences of related issues or features where correct execution of task N depends on decisions made during tasks 1..N-1 - built from real Claude Code / Codex session archives stored in pond.
- Scored by real resolution rate (do the patched tests pass), not QA-probe accuracy, following MemoryArena's paradigm.
- The with/without-pond ablation of Section 5 as the core methodology, including the degraded-retrieval control that isolates retrieval quality from mere memory presence.
- Token-cost delta reported as a first-class metric, which the existing literature omits.

Why pond is uniquely positioned. The benchmark needs a corpus of real, raw, losslessly preserved agentic coding sessions with their outcomes. pond produces exactly that as a byproduct of normal operation. Extraction-based systems (Mem0, Zep, A-MEM) cannot supply it - they discard the raw record. pond does not have to build the dataset; it accumulates it.

The head start. The Section 5 ablation is the paper's Table 1. It can be run before the full benchmark is designed, and it produces a publishable result on its own.

Strong reminder, restated: this is the highest-leverage opportunity surfaced by this research. It should be tracked as real work, not left as a note in a doc.

## Appendix: verified sources

All benchmark arXiv IDs below were confirmed by live arxiv.org crawl on 2026-05-22. Reading-list paper IDs are verified or high-confidence; two entries are not arXiv papers (noted inline in Section 3.1).

Retrieval: RRF (Cormack et al., SIGIR 2009); Contextual Retrieval (Anthropic, 2024); Dense X Retrieval 2312.06648; Lost in the Middle 2307.03172; Context- and Time-Sensitive Long-term Memory 2406.00057.

Agent-session retrieval: Synapse 2306.07863; Evo-Memory 2511.20857; ExpRAG 2603.18272; Experience-Following 2505.16067; ReasoningBank 2509.25140.

Memory-system layer: MemGPT 2310.08560; Mem0 2504.19413; Zep 2501.13956; A-MEM 2502.12110; memory survey 2404.13501.

Benchmarks: MemoryArena 2602.16313; SWE-ContextBench 2602.08316; MemoryCode 2502.13791; Mem2ActBench 2601.19935; MTL 2604.14004; LoCoMo 2402.17753; LongMemEval 2410.10813; ContextBench 2602.05892; MemoryAgentBench 2507.05257.
