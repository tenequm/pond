# Computing token usage and cost from a pond store

How to turn `options.anthropic.usage` into correct token counts and dollar
figures. The naive query overstates by 2x or more, so the dedup rule below is
not optional.

Verified 2026-08-27 against two independent references: `ccusage`
(github.com/ryoppippi/ccusage) and Claude Code's own `cost-state` records.

## Where the usage lives

Usage rides on `messages` rows with `role = 'assistant'`, inside the `options`
JSON column:

| Path | Meaning |
|---|---|
| `options.anthropic.id` | provider message id (`msg_...`) - the dedup key |
| `options.anthropic.model` | e.g. `claude-opus-5`, `claude-fable-5` |
| `options.anthropic.usage.input_tokens` | fresh input, excludes cache |
| `options.anthropic.usage.output_tokens` | generated tokens |
| `options.anthropic.usage.cache_read_input_tokens` | served from cache |
| `options.anthropic.usage.cache_creation_input_tokens` | written to cache |
| `options.anthropic.usage.cache_creation.ephemeral_5m_input_tokens` | cache write, 5-minute TTL |
| `options.anthropic.usage.cache_creation.ephemeral_1h_input_tokens` | cache write, 1-hour TTL |

Only Anthropic calls carry this. Rows from `codex-cli`, `pi-coding-agent`,
`opencode` and `oh-my-pi` have no usage at all, so they drop out on their own.
`nanoclaw` and other SDK entrypoints do carry it.

## The dedup rule

pond is lossless: one source JSONL line becomes one row. A multi-block
assistant turn (thinking + tool_use + tool_use) is written as N lines sharing a
single provider message id, and each line repeats that turn's usage snapshot.

Summing rows therefore counts one API call several times. Measured over one
7-day window: 66,014 assistant rows for 32,298 real API calls, a 2.04x
overstatement. Month-by-month the ratio ranged 1.44x to 2.58x, so no fixed
correction factor exists - you have to dedup.

**Group by `options.anthropic.id`, take `MAX` of each usage field, then sum the
groups.** The snapshots are cumulative within a turn, so `MAX` recovers the
final value. `SELECT DISTINCT` over the usage tuple does not work: snapshots
that differ mid-turn survive as separate rows and get counted twice.

Two details that are easy to get wrong:

- **Dedup globally, not per session.** Resume and compaction replay some turns
  into a second `session_id` (122 such turns in one 7-day window). Keying on
  `(session_id, id)` keeps both copies; keying on `id` alone collapses them.
- **Do not add `request_id` to the key.** It never splits a group - in a
  1,804-turn session, 1,803 turns had exactly one distinct `request_id` and none
  had more. `ccusage` hashes `(message_id, request_id)`
  (`rust/adapters/claude/src/lib.rs:195`) and lands on figures identical to the
  message-id-only key.

Subagents need no special handling. pond stores them as their own sessions
(`<parent-uuid>/agent-<hash>`) rather than duplicating them into the parent, so
summing main and subagent sessions together is correct. To roll a subagent up to
its parent, use `split_part(session_id, '/', 1)`.

## Pricing

Rates per 1M tokens. Cache reads cost 0.1x the input rate. Cache writes cost
**1.25x at 5-minute TTL and 2x at 1-hour TTL** - use the `ephemeral_5m` /
`ephemeral_1h` split rather than assuming one rate, because agent harnesses that
default to 1-hour caching are otherwise underpriced by 60% on the write leg.

| Model | Input | Output |
|---|---|---|
| `claude-fable-5` | 10 | 50 |
| `claude-opus-5`, `claude-opus-4-8`, `claude-opus-4-7`, `claude-opus-4-6` | 5 | 25 |
| `claude-sonnet-5` | 2 | 10 |
| `claude-sonnet-4-6` | 3 | 15 |
| `claude-haiku-4-5` | 1 | 5 |

```
cost = input*pin + output*pout + cw5m*1.25*pin + cw1h*2.0*pin + cache_read*0.1*pin
```

This reproduces Claude Code's own per-model `costUSD` exactly (checked at
$25.208524 and $1.145095 on two records).

Map unmatched models to `NULL`, never to `0`, or unknown models are silently
priced at zero. Filter with `model LIKE 'claude-%'` to drop the `<synthetic>`
pseudo-model (which carries zero tokens anyway) and any non-Anthropic models
reaching the store through a router. Note that `options.anthropic.model` records
`claude-opus-5` where Claude Code's cost records say `claude-opus-5[1m]`; the
rate is the same.

## Query

```sql
WITH turns AS (
  SELECT json_get_string(options,'anthropic','id') AS turn_id,
         MIN("timestamp") AS ts,
         any_value(json_get_string(options,'anthropic','model')) AS model,
         MAX(json_get_int(options,'anthropic','usage','input_tokens')) AS in_tok,
         MAX(json_get_int(options,'anthropic','usage','output_tokens')) AS out_tok,
         MAX(json_get_int(options,'anthropic','usage','cache_read_input_tokens')) AS cr,
         MAX(json_get_int(options,'anthropic','usage','cache_creation','ephemeral_5m_input_tokens')) AS cw5m,
         MAX(json_get_int(options,'anthropic','usage','cache_creation','ephemeral_1h_input_tokens')) AS cw1h
  FROM messages
  WHERE role = 'assistant'
    AND "timestamp" >= TIMESTAMP '2026-03-01T00:00:00Z'
    AND json_get_string(options,'anthropic','id') IS NOT NULL
  GROUP BY turn_id
),
priced AS (
  SELECT ts, in_tok, out_tok, cr,
         coalesce(cw5m, 0) AS cw5m, coalesce(cw1h, 0) AS cw1h,
         CASE WHEN model = 'claude-fable-5' THEN 10.0
              WHEN model IN ('claude-opus-5','claude-opus-4-8','claude-opus-4-7','claude-opus-4-6') THEN 5.0
              WHEN model = 'claude-sonnet-5' THEN 2.0
              WHEN model = 'claude-sonnet-4-6' THEN 3.0
              WHEN model = 'claude-haiku-4-5-20251001' THEN 1.0 END AS pin,
         CASE WHEN model = 'claude-fable-5' THEN 50.0
              WHEN model IN ('claude-opus-5','claude-opus-4-8','claude-opus-4-7','claude-opus-4-6') THEN 25.0
              WHEN model = 'claude-sonnet-5' THEN 10.0
              WHEN model = 'claude-sonnet-4-6' THEN 15.0
              WHEN model = 'claude-haiku-4-5-20251001' THEN 5.0 END AS pout
  FROM turns
  WHERE model LIKE 'claude-%'
)
SELECT date_trunc('month', ts) AS month,
       COUNT(*) AS turns,
       SUM(out_tok) AS output_tokens,
       SUM(cr) AS cache_read_tokens,
       ROUND(SUM((in_tok*pin + out_tok*pout + cw5m*1.25*pin
                + cw1h*2.0*pin + cr*0.1*pin) / 1000000.0), 2) AS usd
FROM priced
GROUP BY month
ORDER BY month;
```

## Performance

`options` is a wide JSON column that no index can serve, so any `GROUP BY` or
`MAX` over a JSON path reads every candidate row's whole document. Consequences:

- Raise `timeout_seconds` (300-600 for a corpus-wide sweep). On a remote store,
  even a session-scoped query pays for reading that session's `options` pages.
- Never project a whole `options` or `variant_data` column through a sort or a
  join. Project narrow `json_extract(col, '$.field')` paths instead, or the
  query dies allocating hundreds of MB in a TopK.
- Scope by `session_id` or a `timestamp` range wherever the question allows it.

## Known limits

State these alongside any figure rather than presenting the output as billing
truth.

1. **Transcript-derived totals run roughly 11% under actual billing.** Claude
   Code also bills background API calls that never produce a transcript line -
   title generation, conversation summaries, web-search sub-calls. Measured
   across 17 sessions carrying ground-truth `cost-state` records: transcript
   math gave $1,043 against Claude Code's $1,175. `ccusage` shares this blind
   spot exactly, because it reads the same transcripts. Report the figure as a
   floor.
2. **Account attribution only exists from 2026-08-10.** `ownerAccountUuid` and
   `ownerOrganizationUuid` appear solely on `bridge-session` system-role records
   - zero of 1.24M assistant rows carry either (confirmed against the on-disk
   JSONL, so this is the source format, not an ingest gap). Attribute a turn by
   joining its root session via `split_part(session_id, '/', 1)`. Before that
   date no account split is recoverable, and no proxy substitutes when several
   accounts are used from one client on one machine.
3. **The account-uuid to email mapping is not stored anywhere.**
   `~/.claude.json` holds only the currently logged-in account and is overwritten
   on switch. Record the mapping yourself if you need it.
4. **Model labels and usage do not span the whole corpus.** Usage appears only
   for Anthropic calls; older rows predate both. Any all-time total understates.

## Cross-checking

`npx ccusage@latest monthly` is an independent second opinion on the same
method. It reads only the local `~/.claude/projects` directory, so a pond store
that also ingests other machines should come out **higher**. A run on
2026-08-27 over March to August gave pond $58,055 against ccusage $53,901
(+7.7%), with March cache-read tokens agreeing to within 0.01%.

If your pond figure comes out **lower** than ccusage over the same window, the
dedup is over-collapsing - that is the signal to check first.
