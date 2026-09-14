# Recall context cost: pond_search vs grep

Method and raw numbers behind the README chart. Measured 2026-08-14 on the
author's corpus and machine (M-series MacBook, macOS). Rerun it on yours:
[`recall-context-cost.sh`](recall-context-cost.sh) - edit the five
query/pattern pairs to questions from your own history.

## Question measured

An agent needs to answer "how did we solve this before?" from past sessions.
What does each retrieval path put into the agent's context window?

- **pond arm**: `pond search "<natural-language question>"` - the complete
  response block (ranked hits with matched text, grouped by session). This is
  exactly what the `pond_search` MCP tool returns to the agent.
- **grep arm**: `rg -i "<keywords>"` over the session files already on disk
  (`~/.claude/projects` + `~/.codex/sessions`, 9.6 GB at measurement time).

## Why the grep arm is scored on files, not matched lines

Raw matched-line output is not a usable comparison: session files are JSONL
where one line is one full message object (embedded tool output included), so
naive `rg` output for these queries was 4.0-5.1 GB per query. No agent puts
that in context, and charting it would be a straw man.

The realistic grep workflow is `rg -il` to get candidate files, then read the
best candidate. So the grep arm is scored as reading **one** matching
transcript - the **median-sized** one - and choosing which of the candidates
to read is scored as **free**. Both choices favor grep. What grep still lacks
is ranking: the candidate list is unordered, and the counts below show how
much triage that leaves.

## Raw results

Corpus: 14,542 sessions / 2,711,779 messages / 6 agent clients (pond store);
11,788 Claude Code transcript files plus Codex sessions on local disk.
Tokens estimated as bytes/4.

| # | Question (pond query) | grep pattern | pond tokens | pond time | grep matching files | median matching file (tokens) | grep `rg` time |
|---|---|---|---|---|---|---|---|
| 1 | call recording split into two files after device change | `recording.*split\|split.*recording` | 2,973 | 0.8 s | 1,443 | 107,521 | 13.7 s |
| 2 | how did we wire up the OCC retry loop | `OCC.*retry\|retry.*OCC` | 1,500 | 1.3 s | 3,270 | 89,924 | 13.3 s |
| 3 | mac cannot see the printer scanner over the network | `scanner.*bonjour\|bonjour.*scanner\|_uscan` | 2,587 | 0.8 s | 23 | 254,815 | 12.9 s |
| 4 | tailscale peers relaying through DERP instead of direct | `DERP` | 2,949 | 0.8 s | 2,213 | 184,031 | 7.0 s |
| 5 | rust target directory filling up the disk | `cargo clean` | 2,619 | 0.8 s | 80 | 189,885 | 5.1 s |

Medians: pond 2,619 tokens; grep 184,031 tokens for the single median
candidate - roughly **70x**, before any triage cost.

## Observations the chart cannot carry

- **Keyword explosion is the norm, not the edge case.** `DERP` looks like a
  distinctive token; it matches 2,213 files because it appears in every
  `tailscale status` dump a session ever captured. Query 3 is grep's best case
  here (23 files) and its median candidate is still a quarter-million tokens.
- **Query 1's answer lives in a Codex session.** Grepping only
  `~/.claude/projects` - the natural first move - returns nothing relevant.
  Cross-tool coverage is a search-scope property, not a speed property.
- **Coverage is not equal.** At measurement time the pond store held 14,173
  Claude Code sessions back to 2025-10-13; the local disk held 11,788
  transcript files back to 2026-02-16. Retention had already deleted about
  four months of history that only the archive still has.

## Honest caveats

- One corpus, one machine, five queries, chosen by the author. This measures
  the shape of the problem, not a universal constant.
- Tokens are approximated as bytes/4, not tokenizer-counted.
- pond's arm requires prior `pond sync` (ingest + embedding); that cost is
  amortized across every future query and is not included here. grep requires
  nothing.
- grep wall-clock above is a cold-ish first run over 9.6 GB; repeated runs are
  faster from page cache.
- The pond response is a starting point, not always the full answer: the agent
  may follow up with `pond_get_session` on a hit, which adds tokens. The same
  is true of the grep arm, which is why both arms are scored at their first
  useful read.
