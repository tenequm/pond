# Fable 5 vs Fable 5.1: the queries behind the per-prompt comparison

The queries used for the 2026-09-02 comparison of Claude Fable 5 and Fable 5.1
over 22,022 API calls from one pond store plus one throwaway store built from
a second Claude config dir. Method and pricing follow
[2608-27-token-usage-accounting.md](2608-27-token-usage-accounting.md): one
row per provider message id, `MAX` per usage field, then aggregate. The same
shape works on raw `~/.claude/projects/**/*.jsonl` with any JSON tool.

## Headline numbers (21 days ending 2026-09-02 21:20Z)

Per API call:

| | Fable 5 | Fable 5.1 | delta |
|---|---|---|---|
| API calls / sessions | 19,065 / 202 | 2,957 / 33 | |
| output tokens, avg / median | 895 / 452 | 961 / 499 | +7% / +10% |
| output tokens, p90 / p99 | 2,015 / 6,605 | 2,044 / 7,439 | +1% / +13% |
| cache read tokens, avg | 217k | 240k | +10% |

Per prompt (a prompt is a user message with text; each session is attributed
to the model with the most calls in it, 4 mixed sessions):

| | Fable 5 | Fable 5.1 | delta |
|---|---|---|---|
| prompts | 4,344 | 661 | |
| API calls per prompt | 4.32 | 4.91 | +14% |
| tool calls per prompt | 7.00 | 5.97 | -15% |
| output tokens per prompt | 3,824 | 4,994 | +31% |
| cache read tokens per prompt | 926k | 1.26M | +36% |
| $ per prompt at API list rates | $1.52 | $1.05 | -31% |

Split by kind of work: everyday sessions +23% output tokens per prompt; a
heavy PR-review corpus (5 sessions each side) +94%, with the p90 call going
from 2,700 to 5,105 output tokens.

## Queries

All run with `pond sql --timeout 300`; add `--storage-path <dir>` for a
second store.

Per API call, one row per provider message id:

```sql
SELECT json_get_string(options,'anthropic','id') AS turn_id,
       any_value(json_get_string(options,'anthropic','model')) AS model,
       any_value(session_id) AS session_id,
       MIN("timestamp") AS ts,
       MAX(json_get_int(options,'anthropic','usage','input_tokens')) AS in_tok,
       MAX(json_get_int(options,'anthropic','usage','output_tokens')) AS out_tok,
       MAX(json_get_int(options,'anthropic','usage','cache_read_input_tokens')) AS cr,
       MAX(json_get_int(options,'anthropic','usage','cache_creation','ephemeral_5m_input_tokens')) AS cw5m,
       MAX(json_get_int(options,'anthropic','usage','cache_creation','ephemeral_1h_input_tokens')) AS cw1h
FROM messages
WHERE role = 'assistant' AND "timestamp" >= now() - INTERVAL '21 days'
  AND json_get_string(options,'anthropic','model') IN ('claude-fable-5','claude-fable-5-1')
  AND json_get_string(options,'anthropic','id') IS NOT NULL
GROUP BY turn_id;
```

Prompts per session (`search_text` is null on tool results, so this counts
typed messages only):

```sql
SELECT session_id,
       SUM(CASE WHEN role = 'user' AND search_text IS NOT NULL THEN 1 ELSE 0 END) AS prompts
FROM messages
WHERE "timestamp" >= now() - INTERVAL '21 days'
GROUP BY session_id;
```

Tool calls per session:

```sql
SELECT session_id, COUNT(*) AS tool_calls,
       SUM(CASE WHEN tool_name = 'Agent' THEN 1 ELSE 0 END) AS agent_calls
FROM parts
WHERE type = 'tool_call'
GROUP BY session_id;
```

Export each with `--format ndjson -o <store>-<name>.ndjson` and combine:

```python
import json, statistics as st
from collections import defaultdict

load = lambda f: [json.loads(l) for l in open(f)]
turns = load('main-turns.ndjson') + load('other-turns.ndjson')
prompts = defaultdict(int)
for r in load('main-prompts.ndjson') + load('other-prompts.ndjson'):
    prompts[r['session_id']] += r['prompts'] or 0
tools = {}
for r in load('main-tools.ndjson') + load('other-tools.ndjson'):
    tools[r['session_id']] = r['tool_calls'] or 0

seen = set()
uniq = [t for t in turns if not (t['turn_id'] in seen or seen.add(t['turn_id']))]

def pct(xs, p):
    xs = sorted(xs); k = (len(xs) - 1) * p; f = int(k); c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)

price = {'claude-fable-5': 1.0, 'claude-fable-5-1': 0.25}  # cache read $/MTok
def cost(t, model):
    return ((t['in_tok'] or 0) * 10 + (t['out_tok'] or 0) * 50
            + (t['cw5m'] or 0) * 12.5 + (t['cw1h'] or 0) * 20
            + (t['cr'] or 0) * price[model]) / 1e6

by = defaultdict(list)
for t in uniq:
    by[t['model']].append(t)
for m, ts in sorted(by.items()):
    o = [t['out_tok'] or 0 for t in ts]
    cr = [t['cr'] or 0 for t in ts]
    print(m, len(ts), round(st.mean(o)), st.median(o), pct(o, .9), pct(o, .99), round(st.mean(cr)))

sess = defaultdict(lambda: defaultdict(int))
for t in uniq:
    sess[t['session_id']][t['model']] += 1
dominant = {s: max(m, key=m.get) for s, m in sess.items()}
agg = defaultdict(lambda: defaultdict(float))
for t in uniq:
    m = dominant[t['session_id']]
    agg[m]['turns'] += 1
    agg[m]['out'] += t['out_tok'] or 0
    agg[m]['cr'] += t['cr'] or 0
    agg[m]['usd'] += cost(t, m)
for s, m in dominant.items():
    agg[m]['prompts'] += prompts[s]
    agg[m]['tools'] += tools.get(s, 0)
for m, a in sorted(agg.items()):
    p = a['prompts']
    print(m, p, round(a['turns'] / p, 2), round(a['tools'] / p, 2),
          round(a['out'] / p), round(a['cr'] / p), round(a['usd'] / p, 3))
```

## Prices used

From the Fable 5.1 docs (2026-09-02): input $10, output $50, 5m cache write
$12.50, 1h cache write $20, cache read $0.25 per MTok; Fable 5 identical
except cache read at $1.00. Claude Code writes 1h cache entries (about 4,500
tokens per call on both models), so the 1h rate is the one that matters.

## What this cannot see

- Transcript totals run about 11% under real billing: title generation,
  summaries and web-search sub-calls never write a transcript line.
- How a subscription plan weighs cache reads against its weekly limit is not
  documented, and no transcript records it.
- Effort level is not in the transcript.
- 33 sessions of Fable 5.1 over 27 hours against 21 days of Fable 5, and the
  task mix differs between them.
