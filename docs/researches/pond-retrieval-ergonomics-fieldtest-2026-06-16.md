# pond retrieval ergonomics: a field test (2026-06-16)

Status: empirical findings + design recommendations. Standalone snapshot from one
external session that used pond heavily for a real transcript-analysis task. This
doc is deliberately self-contained and does NOT build on any other doc in this
repo - it is meant to be compared against them independently, not merged into them.

Author context: written by an agent (Claude Code, model Opus) that consumed pond
as an MCP client from another project (`x402-services`), not by a pond maintainer.
All numbers are real measurements from that session's subagent runs; all transcript
facts were verified by grepping the raw session JSONL on disk.

---

## TL;DR

pond was measurably *worse* than raw `grep` over the session's JSONL file for one
concrete task ("search a stored session, find a literal fact, report it"). Across
controlled A/B runs pond cost more tokens and ~2x the tool calls. Digging into why
surfaced five issues, ranked by leverage:

1. **No recency / supersession signal (highest impact).** `pond_search` ranks by
   relevance; on a long, *evolving* investigation it surfaces an early, since-overturned
   conclusion and the agent reports it as current. Three agents given the identical
   question returned three different "answers" because each anchored on a different
   temporal slice of one session. The only agent that got the *latest* truth did so
   solely because it read `pond_get(session_from="end")`. Nothing in the relevance-ranked
   path signals "this conclusion was later revised."
2. **Unbounded responses pollute the caller's context.** A single `pond_sql_query`
   returned 75,716 chars in one shot; the careful (opus) agent spent ~87k tokens mostly
   on full tool-body pulls. There is no per-item cap and no match-centered truncation, so
   the cheap path (dump the column, filter in-context) is also the default path.
3. **Conversation-first is correct, and the tool surface should enforce it.** In a
   clean test, all three models found the answer in user+assistant conversational text
   *alone*; tool outputs were noise. Tool/tool_result bodies are high-token, low-signal,
   and have no narrative to reason over. They should be reachable only by explicit
   id (message_id / part_id), never bulk-dumped or surfaced by default.
4. **Exact vs semantic search is undiscoverable.** The literal-symbol case (`8/8`,
   `cf_clearance`) needs substring/exact matching, which today lives only in
   `pond_sql_query` (`contains_tokens` / `LIKE`). Agents that reach for `pond_search`
   first never find it. The distinction the agent needs is one sentence: *are you
   searching words or characters?*
5. **Reading the docs first measurably helps - so bake the flow into the tool
   descriptions.** Agents instructed to read pond's `schema://` resources first used
   ~40% fewer tool calls and stopped flailing. That guidance should not require a
   separate resource read; it belongs in the tool descriptions themselves.

None of the recommendations below add a new tool. They reshape `pond_search`,
`pond_get`, and `pond_sql_query`.

---

## Why this exists

The session's real job was unrelated to pond: it was debugging a web-scraping
failure. Partway through, the operator asked the agent to recover a fact from a
*previous* (pre-compaction) session and ran a controlled experiment - the same
recovery task, done two ways:

- Agent A: read the raw session JSONL from disk with `rg` / `python` / `jq`.
- Agent B: use pond exclusively (`pond_search` / `pond_get` / `pond_sql_query`).

Agent B (pond) cost more and took more steps. The operator wanted to know *why*, and
whether pond's three tools could be reshaped to beat raw grep on this task class -
without adding tools. The rest of this doc is that investigation.

The task class, stated generally: **"Find, in a long stored session, what we
concluded about X, and report it accurately."** This is the dominant real use of a
session-recall tool, and it is exactly where pond underperformed.

---

## Method

Four rounds of controlled subagent runs. Every subagent was pond-only unless noted.
The "fixture" question evolved (see Confounds), but the retrieval task shape was
constant. Token / tool-call / wall-clock numbers come from each subagent's reported
usage; transcript facts were confirmed by `rg` over the raw session JSONL.

| Round | What it tested | Prompt cleanliness |
|---|---|---|
| 1 | pond vs raw-JSONL grep, same task | Contaminated: told agents "the evidence is in bash tool outputs" |
| 2 | docs-first vs not, 3 models | Contaminated (same hint) |
| 3 | forensic ledger: where did the tokens go | n/a (parsed the round-1/2 transcripts) |
| 4 | clean: natural question, agent picks strategy | Clean: no hint about where the answer lived |

---

## Results

### Round 1: pond vs grep (same task)

| Approach | Tokens | Tool calls | Wall-clock | Correct? |
|---|---|---|---|---|
| Raw JSONL + `rg`/`python` | 103,186 | 13 | 273s | yes |
| pond only | 125,951 | 23 | 321s | yes (after flailing) |

pond: +22% tokens, +77% tool calls. Both reached the right answer.

### Round 2: docs-first, three models (pond only)

| Model | Tokens | Tool calls | Wall-clock | Correct? |
|---|---|---|---|---|
| haiku | 81,551 | 17 | 94s | yes |
| sonnet | 54,808 | 13 | 131s | yes |
| opus | 85,701 | 10 | 107s | yes |

Reading the `schema://` docs first removed the dead-end flailing seen in round 1
(a `CAST(... AS VARCHAR)` error, a malformed query, duplicate probe queries before
the agent discovered the right `parts` access pattern). Note opus cost *more* tokens
than sonnet despite *fewer* calls - the cost is per-response size, not call count.

### Round 3: forensic ledger (where the tokens went)

Parsing the round-1 pond run's own transcript:

- Retrieval payload = **92%** of all tool output. Search/probe = ~8%.
- One query (`pond_sql_query`, no `LIMIT`) returned **75,716 chars** (~19k tokens) -
  a full-transcript dump *after* two earlier near-duplicate full dumps. That single
  call added ~36k tokens to the working context.
- Three later calls re-paged the same `$.result` column (9,289 -> 8,796 -> 2,801 chars).
- The agent's dragged context (cache_read) climbed 0 -> 34.8k -> 51k -> 83k -> 119.6k
  as those dumps accreted.

Parsing the round-1 grep run:

- ~25,432 tokens of genuinely new payload across 13 surgical calls; the rest was the
  cached fixed prompt. Search itself cost <1k tokens (one `rg -c` loop).
- The two heaviest calls were `Read`s of pre-saved matched regions (~18k tokens, 71%
  of payload) - i.e. it paid tokens only for matched lines + context, never the file.

pond capability audit (verified by live calls against the corpus):

- `messages.search_text` is NULL for every tool message (0 of 215 tool messages in
  the target session were searchable). `pond_search` and `contains_tokens` ride
  `search_text`, so tool-output text is unreachable by search.
- Literal search over tool outputs is only `pond_sql_query` on `parts` with
  `json_extract(variant_data,'$.result') LIKE`, and only when scoped to an indexed
  `session_id` (a whole-document LIKE is rejected at plan time; an unscoped scan over
  the corpus's ~1.38M parts does not finish).
- `pond_get` verbatim page 1 (limit 200) returned **200 empty `system` carrier
  messages** before any content - a timestamp-tie ordering artifact.
- Tool bodies in the target session's parts total ~89k tokens; a single tool body
  reached 56KB.

### Round 4: clean test (natural question, agent picks strategy, pond only)

Prompt: *"Find, in our conversations before compaction, how we could efficiently
retrieve data from the previously-unavailable laredoute.pt resources."* No hint about
where the answer lived. Each agent was asked to self-report whether user+assistant
messages alone sufficed.

| Model | Tokens | Tool calls | Wall-clock | Answer in user+assistant alone? | Verdict accuracy |
|---|---|---|---|---|---|
| haiku | 54,169 | 6 | 52s | YES | partially confabulated |
| sonnet | 58,779 | 6 | 105s | YES | most current |
| opus | 87,479 | 11 | 197s | YES | accurate but STALE |

All three confirmed the answer was fully present in conversational (user+assistant)
messages; none needed tool outputs. The clean haiku run (6 calls / 54k tokens) was
leaner than the grep agent. **But the three returned materially different answers** -
see next section.

---

## The core finding: temporal slicing and the supersession blind spot

The three round-4 agents disagreed about what the session concluded:

| Agent | What it reported | How it retrieved |
|---|---|---|
| opus | "country-pin gives ~14%, 0/8; PT proxy-pool quality is the binding constraint" | `pond_search` relevance rank -> read top-ranked (early) messages |
| sonnet | "two stacked walls; headed mode solves wall 1; wall 2 is laredoute's own WAF blocking the proxy IPs" | `pond_get(session_id, session_from="end")` -> read latest messages |
| haiku | "headed mode was never attempted; prior conclusion was false" | mixed; partly confabulated |

I resolved the contradiction by grepping the raw session JSONL directly. Ground truth:
**the investigation evolved over ~3.5 hours and its conclusion was revised.**

- Phase 1 (~07:44-08:31): country-pin shipped; measured ~14% / 0/8; concluded "PT
  proxy-pool quality is the binding constraint."
- Phase 2 (~10:21-10:57): the investigation continued. Running cloakserve **headed**
  (`--headless=false`; the Xvfb display was already running but never enabled)
  flipped behavior - the CF "Country challenge" (a managed Turnstile) now solved and
  minted `cf_clearance` (5/6 residential, 3/6 mobile). That exposed a **second wall**:
  laredoute's own Cloudflare WAF returns a branded 403 after CF clears, verbatim
  `Error reason : WAF Block / IP Block / Rate limiting`, blocking the DataImpulse
  exit IP itself. Premium residential (pristine PT ISP IPs - NOS, MEO, Vodafone,
  `hosting:false`) cleared CF 5/6 then hit the same WAF 0/6, proving **"the block is
  not about IP grade."**

Phase 2 *overturns* phase 1's diagnosis. The divergence was NOT confabulation by
sonnet - the raw transcript contains the strings (grep counts: `cf_clearance` x99,
`headed` x57, `Xvfb` x29, `WAF Block` x4, `IP Block` x3, `MEO` x42, `Managed Turnstile`
x2, `5/6` x6, `3/6` x5, `12/12` x1) alongside the phase-1 strings (`pool quality` x26,
`0/8` x37, `14%` x38). Both phases are real; they are just far apart in time.

What each agent did with that:

- **opus** retrieved by relevance and got the phase-1 messages (highly relevant, and
  they read as a confident conclusion). It never saw phase 2. It returned a
  **since-overturned conclusion as current** - and did so carefully, with citations.
  This is the dangerous failure: a good agent, good retrieval call, stale answer.
- **sonnet** retrieved from the end of the session (`session_from="end"`) and caught
  phase 2. It won on *recency of read*, not on smarter reasoning.
- **haiku** (weakest model) confabulated a third framing ("headed never attempted"),
  which the grep falsifies.

The orchestrating agent (me) had the **identical failure**: a compaction summary
earlier in the session retained phase 1 and dropped phase 2, so I twice told the
operator the stale "PT pool quality" answer with confidence. Compaction and
relevance-ranked retrieval fail the same way on long evolving threads: they keep a
slice and lose the supersession.

**Design implication:** relevance is the wrong default sort for "what did we
conclude." pond needs (a) a recency-ordered retrieval mode and (b) an explicit signal
when a returned slice is not the latest state of its thread. Neither exists today.

---

## Findings and per-tool recommendations

No new tools. All changes reshape the three existing tools and their descriptions.

### Finding 1 - recency / supersession (highest leverage)

Evidence: round-4 divergence; opus returned an overturned conclusion; sonnet won only
via `session_from="end"`.

Recommendations:

- `pond_search`: add `order: relevance | recent` (default `relevance`). `recent`
  re-sorts the matched set by timestamp descending. For "latest conclusion" queries
  this surfaces phase 2, not phase 1.
- `pond_get` (session mode): when returning oldest-first messages while newer ones
  exist in the same session, include a footer signal, e.g.
  `newer_messages_remaining: N` with a one-line hint that conclusions may have been
  revised - read `session_from="end"`. Today `session_from` exists but nothing tells
  the agent it *should* use it for a "current state" question.
- Description copy: state that conclusions in a long session can be superseded; for
  "what did we decide / latest state", read from the end.

### Finding 2 - bounded responses

Evidence: 75,716-char single dump; opus 87k tokens from full-body pulls; cache_read
climbed to 119.6k as dumps accreted.

Recommendations:

- `pond_sql_query`: truncate per *cell*, not per response. Clip each text cell to
  ~500-1000 chars with a `+N chars` marker; keep the existing 100-row cap. A
  whole-response char cap would corrupt tabular/JSON output by cutting mid-row;
  per-cell truncation kills the "SELECT the full `$.result` body" pollution without
  breaking parseability. Full cell available via the existing `format=ndjson|parquet`
  export.
- `pond_get`: cap each message body and, when a `match` is in play, return a
  match-centered window rather than a blind prefix. A 56KB body with the hit at char
  12,000 would lose the hit under a naive head-truncation. Continuation via the
  existing `after_id`.
- General: make the bounded response the default and require an explicit opt-in
  (larger `limit`, a `full` flag, or `format=ndjson`) for more. The cheap path must
  also be the default path.

### Finding 3 - conversation-first by construction

Evidence: round 4, all three models answered from user+assistant text alone; tool
outputs were never needed.

Recommendations:

- `pond_get`: keep `conversational` the default and make it genuinely clean - drop
  the empty `system` carrier messages (the 200-empty-carriers artifact) and do not
  inline tool bodies. State in the response that this is the complete human/model
  text and that tool detail is available by id - so the agent trusts it and stops
  escalating to `verbatim`.
- Tool/tool_result bodies reachable only by explicit `message_id` / `part_id` (or an
  explicit `include=['tool_result']`), never surfaced by default and never bulk-dumped.
- Do NOT index tool outputs into `search_text`. (This reverses an idea considered
  earlier in the session.) Tool outputs are high-token and low-signal; making them
  searchable re-invites the dump-everything path. Keep them id-addressable only.

### Finding 4 - exact vs semantic discoverability

Evidence: the literal-symbol case (`8/8`, `cf_clearance`) needs substring matching,
which only `pond_sql_query` offers; agents that start at `pond_search` miss it.

Recommendations:

- `pond_search`: add `mode: hybrid | fts` (default `hybrid`). `fts` = exact-term BM25
  over `search_text`. One sentence in the description carries the decision rule:
  *concepts -> hybrid; known exact words -> fts; symbols/punctuation/substrings (that
  tokenization splits, e.g. `8/8`) -> `pond_sql_query` LIKE.* Do not add a third
  substring mode to `pond_search`; substring stays the SQL escape hatch.
- `pond_sql_query`: in the description, contrast `contains_tokens` (word tokens,
  index-fast) vs `LIKE` (characters, slow, must be session-scoped). The agent only
  needs the words-vs-characters distinction.

### Finding 5 - subagent spelunking and docs-first

Evidence: rounds 1-2 agents burned calls enumerating subagent sessions (partly
because the contaminated prompt told them to); docs-first runs were ~40% leaner.

Recommendations:

- `pond_search`: drop `include_subagents`. For *finding the conversation*, the main
  thread is essentially always sufficient; the flag invites wasted spelunking.
  Subagent access, when genuinely needed, remains available via `pond_get` /
  `pond_sql_query` scoped by `parent_session_id` - an explicit, last-resort path.
- Bake the happy-path flow into the server instructions and each tool's description
  so it does not require a separate resource read:
  1. `pond_search` (`mode`, `order`) to find the thread.
  2. `pond_get` conversational (clean, bounded) to read the arc.
  3. `pond_sql_query` / by-id only for tool detail or subagents.

---

## What this explicitly does NOT recommend

- No new tools. The three existing tools suffice.
- Not indexing tool outputs into search (Finding 3).
- Not a blanket whole-response char cap (Finding 2 - per-cell / match-centered instead).
- Not removing subagent access entirely - only removing the `include_subagents` flag
  from `pond_search` and keeping subagents as an explicit id-scoped path.

---

## Confounds and honesty caveats

The numbers above are real but not laboratory-clean. Read them as direction, not
precision:

1. **Prompt contamination (rounds 1-2).** The early prompts told agents "the evidence
   is in bash tool outputs." That is what pushed them into `pond_sql_query` on `parts`
   and into subagent-chasing. It was also *false* - the answer was in assistant prose.
   Round 4 (clean) is the trustworthy comparison; rounds 1-3 over-state the
   tool-output problem.
2. **The corpus grew during the experiment.** Each round's analysis was itself stored,
   so later agents could (and sometimes did) read earlier agents' verdicts instead of
   re-deriving. Token counts are therefore not strictly comparable across rounds; the
   task got easier as it went.
3. **Model capability varied.** haiku confabulated; opus was careful but retrieved a
   stale slice. Some of the divergence is model behavior, not pond's surface. The
   recency/supersession fix matters *because* it should protect even a weak or
   unlucky-retrieval agent.
4. **Single task, single corpus.** One question class ("recover a conclusion from a
   long evolving session") against one real corpus. The recency finding generalizes by
   argument, not by N.
5. **n=1 on the headline laredoute facts.** Phase-2 results (cf_clearance 5/6, the WAF
   block) are from that session's own small experiments, quoted here only to establish
   *which agent read the latest state* - not as web-scraping guidance.

---

## Appendix: raw measurements

Per-run usage (tokens / tool calls / wall-clock):

```
Round 1  grep      103186 / 13 / 273s   correct
Round 1  pond      125951 / 23 / 321s   correct (flailed)
Round 2  haiku      81551 / 17 /  94s   correct (docs-first)
Round 2  sonnet     54808 / 13 / 131s   correct (docs-first)
Round 2  opus       85701 / 10 / 107s   correct (docs-first)
Round 4  haiku      54169 /  6 /  52s   correct-ish (confabulated framing)
Round 4  sonnet     58779 /  6 / 105s   correct + most current
Round 4  opus       87479 / 11 / 197s   accurate but stale (phase-1 only)
```

Pollution evidence (round-1 pond run): retrieval = 92% of payload; largest single
response 75,716 chars; dragged context (cache_read) peaked 121,835.

Tool-output unreachability: `search_text` NULL for 215/215 tool messages in the
target session; verbatim page 1 = 200 empty system carriers; max single tool body 56KB.

Ground-truth grep counts (raw session JSONL), proving the round-4 divergence is real
temporal slicing, not confabulation:

```
phase 2:  cf_clearance 99  headed 57  headless 52  Xvfb 29  WAF Block 4
          IP Block 3  MEO 42  NOS 10  PT Prime 5  Managed Turnstile 2
          5/6 6  3/6 5  12/12 1
phase 1:  pool quality 26  binding constraint 22  0/8 37  14% 38  PT-pinned 18
```
