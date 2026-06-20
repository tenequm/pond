# pond retrieval tools redesign - research and reasoning (2026-06-19)

Status: research capture for later review. This records the *why* behind a redesign of pond's three MCP tools, derived from a real failure transcript plus the [pond retrieval ergonomics field test](https://github.com/tenequm/pond/blob/50ced73d8433bb0c477a39515f90c6b59d4ba4c7/docs/researches/pond-retrieval-ergonomics-fieldtest-2026-06-16.md) ([PR #59](https://github.com/tenequm/pond/pull/59)).

This is the reasoning, not the locked spec. The locked param surface lives in `docs/plans/2606-19-tools-redesign-and-hydration-perf.md`; where this doc and that plan disagree, the plan wins. Divergences are called out explicitly at the end.

## The problem

The recurring complaint: "it's basically impossible to find anything." The dominant query class is recall - "what did we discuss / conclude / reason about X, and report it accurately" - which is exactly where pond underperformed raw `grep` in the field test (+22% tokens, +77% tool calls on a controlled A/B).

## Trigger transcript (case study)

An agent burned ~8 tool calls trying to recover GitHub repo URLs produced earlier by a research subagent, and failed every search path: `pond_search` returned conceptually-adjacent-but-wrong sessions; `contains_tokens(search_text, 'clipwise')` returned 0 rows (the URLs lived in tool-result parts, which are not indexed into `search_text`); falling back to `parts.variant_data LIKE` caused repeated CAST/type fumbling; the only thing that worked was walking the session tree via `parent_session_id`.

Important correction made during analysis: this was *not* proof that "a subagent's tool-result-derived deliverable must be retrievable from pond." The agent itself questioned whether the tool names were real repos or just labels, and noted a fresh GitHub search was the better move. So the genuine need was the underlying task; the pond archaeology was arguably a sunk-cost detour. The transcript proves pond is *hard to search* (real), not that this specific retrieval was *required*.

## Field-test findings (recap, ranked by leverage)

1. No recency / supersession signal (highest impact). Relevance ranking surfaces an early, since-overturned conclusion and the agent reports it as current. Three agents given the identical question returned three different answers because each anchored on a different temporal slice of one 3.5-hour session whose conclusion was revised partway through. The only agent that got the latest truth read `pond_get(session_from="end")`.
2. Unbounded responses pollute context. One `pond_sql_query` returned 75,716 chars; retrieval payload was 92% of all tool output in the round-1 pond run.
3. Conversation-first is correct. In the clean round, all models answered from user+assistant text alone; tool outputs were noise.
4. Exact vs semantic search is undiscoverable. Literal symbols (`8/8`, `cf_clearance`) need substring/exact matching, which only `pond_sql_query` offers; agents that start at `pond_search` never find it.
5. Docs-first helps; bake the flow into tool descriptions (docs-first runs were ~40% leaner).

## Redesign principles agreed in this thread

Global:
- Every tool output bounded to ~10k chars, enforced as per-item truncation that fits all `limit` items (never drop below `limit`, never a whole-response guillotine), each with a continuation marker.
- Progressive-disclosure markers everywhere: pagination cursors up/down, message-expansion ids, and a `fts` zero-result handoff to `pond_sql_query` with a ready LIKE example.
- Conversational render = `search_text` (user/assistant text) only, plus tool_call/tool_result/file as one-line refs (`<message_id> <tool_name>(<input-preview>)`). No reasoning parts rendered or indexed, ever. "Reasoning chain" the user cares about = assistant prose, already in `search_text`.
- Tool detail is reachable by expanding any message id (the inline refs carry the id); SQL remains the escape hatch for substrings, symbols, cross-session analytics, and subagents (`parent_session_id`).

pond_search:
- Modes `vector | fts`, replacing the auto-`hybrid`. `vector` (default) = semantic; gate by raw cosine >= `min_score`; order by cosine + recency boost. `fts` = exact whole-word match.
- Drop `include_subagents`, `source_agent`, `format`. Keep `session_id` (in-session semantic recall is a first-class query).
- `min_score` default 0.3 (vector only). It is the visibility gate, which makes "no results" a trustworthy absence signal. 0.3 raw cosine is "basically unrelated," so the gate removes near-noise with near-zero false-negative risk; the calibration risk only bites at higher thresholds.
- Grouped by session, sessions ordered by best boosted hit; within each, matching messages newest-first. No per-session cap - the byte budget is the soft limit.
- Per-session footer signals the supersession case non-destructively: "N newer messages in this session - read from the end."

pond_get (one tool, prefixed params - see divergence note):
- Returns user/assistant messages only as conversational content + inline one-line tool refs; bidirectional pagination markers (before/after cursors); `from=end` for latest-state / post-compaction recovery.
- A message-id target can be any message (incl. tool/system) and returns its full parts; context siblings render conversational.

pond_sql_query:
- Params `query`, `format=text|parquet|ndjson` (drop `json`). `text` truncates per cell + caps rows with a "use parquet/ndjson for all" marker; file formats bypass the byte budget. Error messages bounded and name the fix.

Scoring (the prerequisite that makes the gate real):
- Split the score's two jobs: score-for-gating = raw cosine [0,1] (feeds `min_score`, pool-independent); score-for-ordering = cosine + recency boost.
- Recency boost (claude-kb-derived): additive, magnitude ~0.1, decay scale ~30 days, post-gate. It is a gentle cross-session tiebreaker. It is additive and capped, so it never makes old content invisible (the gate does filtering, not the boost).
- Intra-session supersession is handled by the footer + newest-first, not the boost - a 1-week (or 30-day) decay cannot separate two messages 3 hours apart inside one session.

## Decay math (why "6 months becomes invisible" is a non-issue)

boost = 0.2 * exp(-age / 1week) in claude-kb: now=0.200, 1wk=0.074, 1mo=0.003, 6mo=~0. Because it is additive and capped, a 6-month-old hit at cosine 0.85 still ranks above a 1-day-old hit at cosine 0.60. Old content stays visible if relevant; it just loses the freshness nudge. The real tuning levers are scale (how far back the nudge reaches; ~30 days suits a 10-month corpus better than 1 week) and magnitude (how hard recency can override relevance; 0.1 is a tiebreaker, 0.2 is a 40%-of-band override given a 0.5 gate).

## The hybrid critique (the central argument)

What the current hybrid does (`src/handlers.rs`): always runs both arms, fuses `0.3 * norm_fts + 1.0 * norm_vec` (then / 1.3 into [0,1]); `norm_*` is min-max within the query's pool; mode is server-decided; weights `0.3:1` are benchmark-tuned (2026-06-10 sweep, S@3 67/111 paraphrase, 10/12 false-negative regression); no recency anywhere.

The verdict: it is competent engineering optimized for the wrong objective, and its core mechanism blocks what we need.

- min-max fusion and a stable `min_score` are mutually exclusive. Pool-relative normalization means a message's score depends on what else matched, so a threshold means a different thing per query - which is exactly why the tool description disclaims `min_score` despite the field existing. The raw-cosine gate removes this blocker; it does not just add a knob.
- The tuned weights (0.3 fts : 1.0 vec) show the result is ~77% vector already - hybrid is barely fusing, so a clean `vector` mode loses little of what it actually does, while paying half the per-query cost (one arm, not two).
- No recency component at all means the #1 field-test failure (supersession) is structurally unaddressable in the current design.

The honest cost of dropping hybrid: fusion is robust to the agent not knowing its query axis (it can lift a doc that is mid-rank in both arms). Splitting modes pushes the "concept vs exact string" decision onto the agent. Acceptable because the weights show the fusion lift is small, agents are good at that distinction, and the decision rule + SQL handoff catch misses.

## Counterpoint on file (do not ignore at review)

A fixed-mode recall A/B (N=21, local, `serve_mem_bench --recall-mode`) measured Success@3: hybrid 0.667 vs vector-only 0.333 vs fts-only 0.476. Read naively this argues *for* keeping hybrid. It was considered and overruled deliberately: the A/B forces every query through one arm, but the design lets the agent pick the arm per query (default vector), and N=21 is tiny / overfit. The felt degradation of pond's hybrid was attributed mainly to the missing recency boost, not the arms. Still, this is the one number that cuts against the "drop hybrid" decision - re-run with a larger, per-query-routed eval before treating the decision as proven.

## Expected impact

For the dominant recall workload this should improve tokens, tool-call count, and correctness together (no real trade among them): the 10k bound kills the dumps that were 92% of payload (round-1 pond ~126k tokens would fall well under grep's ~103k); markers + handoffs remove the flailing that drove the +77% calls; the footer + newest-first fix a class of silently-wrong (stale) answers. The one genuinely unproven piece is the scoring split itself - load-bearing for the absence signal - though at a 0.3 floor it is low-stakes.

## Divergences from the locked plan (resolve before implementing)

The locked plan `docs/plans/2606-19-tools-redesign-and-hydration-perf.md` and memory `project_pond_hybrid_recall_and_takerows_hydration` differ from this thread's conclusions on two points:

1. pond_get stays ONE tool with prefixed params (`session_id`/`session_limit`/`session_from`/`session_after_message_id`/`session_before_message_id`; `message_id`/`message_context_before=3`/`message_context_after=3`), so names self-document under the names-only constraint. The `pond_get_message` / `pond_get_session` split discussed here is DEFERRED.
2. `fts` is default-sorted by relevance (agents expect relevance from "search"), with `sort_by=relevance|recency` available on both modes. This thread had instead concluded `fts` = recency-sorted, no score, and `sort_by` dropped. The plan's version is the locked one.

Agreement across both: drop hybrid -> vector default + recency boost; `min_score` vector-only; `include_subagents`/`source_agent`/`format` dropped from search; `pond_sql_query` drops `json`; 10k per-item truncation + markers; no reasoning indexed.
