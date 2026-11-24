---
title: "Claude Code analytics with SQL: query your agent sessions"
description: Go past /usage - run read-only SQL over Claude Code and Codex sessions with pond sql. Tool failures by tool, activity by project and week, tokens per session.
---

# SQL over your agent sessions [Claude Code analytics past /usage: tool failures, activity by project, tokens per session - as queries, across every client]

Claude Code's built-in analytics answer "how much did I use": `/usage` shows token and cost totals, `/insights` writes a report on your usage patterns, and teams can export metrics to their own [OpenTelemetry collector](https://code.claude.com/docs/en/monitoring-usage). None of them answer questions about what happened inside the sessions - which tools fail, which project ate the week, how long a session ran - because that lives in the transcripts, which are JSONL files that Claude Code deletes after 30 days. To query them you need the transcripts as rows. `pond sql` gives you that: one read-only SQL query over every stored session, from Claude Code, Codex and the other clients pond ingests.

(`/usage` and `/insights` as described in Claude Code's [application data](https://code.claude.com/docs/en/claude-directory#application-data) reference, checked 2026-11-24.)

## Three tables

```sh
pond sql "SELECT count(*) FROM sessions"
```

| Table | One row per | Columns you will use |
|---|---|---|
| `sessions` | session | `session_id`, `source_agent`, `project`, `created_at`, `parent_session_id` |
| `messages` | message | `session_id`, `message_id`, `timestamp`, `role`, `source_agent`, `project`, `search_text`, `options` |
| `parts` | piece of a message | `session_id`, `message_id`, `type`, `tool_name`, `call_id`, `is_failure`, `variant_data` |

`source_agent` is the client (`claude-code`, `codex-cli`, ...); a subagent session has a `/` in it, such as `claude-code/general-purpose`. `project` is the directory the session ran in. A tool call is a `parts` row with `type = 'tool_call'`, and its outcome is the `tool_result` row with the same `call_id`. `options` and `variant_data` are JSON: the verbatim leftovers of the source record, which is where model names and token counts are.

The dialect is DataFusion SQL, close to PostgreSQL. Only a single `SELECT` or `WITH` runs; anything that would write is rejected with exit code `2`. The full column list, the JSON functions and more worked examples are in the `schema://pond-sql` resource the MCP server exposes, and agents reach the same surface as the [`pond_sql` tool](/docs/reference/mcp-tools#pond_sql).

## Five queries

Each query below was run against a real store before it went on this page. The outputs keep the real columns and layout; the values are invented.

### Which client did the work

```sql
SELECT split_part(source_agent, '/', 1) AS client,
       COUNT(DISTINCT session_id) AS sessions,
       COUNT(*) AS messages
FROM messages
WHERE timestamp >= now() - INTERVAL '30 days'
GROUP BY client
ORDER BY messages DESC;
```

```text
3 row(s) in 1810 ms; showing 3.
+-------------+----------+----------+
| client      | sessions | messages |
+-------------+----------+----------+
| claude-code | 412      | 96310    |
| codex-cli   | 87       | 21544    |
| opencode    | 9        | 1203     |
+-------------+----------+----------+
```

`split_part` folds subagent sessions into their client. Drop it to see `claude-code/general-purpose` and friends as their own rows.

### Most active projects by week

```sql
SELECT date_trunc('week', timestamp) AS week,
       project,
       COUNT(DISTINCT session_id) AS sessions,
       COUNT(*) AS messages
FROM messages
WHERE timestamp >= now() - INTERVAL '28 days'
  AND NOT (source_agent LIKE '%/%')
GROUP BY week, project
ORDER BY week DESC, messages DESC
LIMIT 20;
```

```text
20 row(s) in 8020 ms; showing 20.
+----------------------+---------------------------+----------+----------+
| week                 | project                   | sessions | messages |
+----------------------+---------------------------+----------+----------+
| 2026-11-23T00:00:00Z | /home/you/work/acme-api   | 12       | 6397     |
| 2026-11-23T00:00:00Z | /home/you/work/acme-web   | 6        | 2904     |
| 2026-11-16T00:00:00Z | /home/you/work/acme-api   | 31       | 14722    |
| ...                  | ...                       | ...      | ...      |
+----------------------+---------------------------+----------+----------+
```

The `NOT (source_agent LIKE '%/%')` line leaves subagent sessions out, so a week of heavy delegation does not look like a week of heavy conversation.

### Tool failures by tool

```sql
SELECT c.tool_name AS tool,
       COUNT(*) AS calls,
       SUM(CASE WHEN r.is_failure THEN 1 ELSE 0 END) AS failures
FROM parts c
JOIN messages m ON m.session_id = c.session_id AND m.message_id = c.message_id
LEFT JOIN parts r ON r.session_id = c.session_id
 AND r.type = 'tool_result' AND r.call_id = c.call_id
WHERE c.type = 'tool_call'
  AND m.timestamp >= now() - INTERVAL '30 days'
GROUP BY tool
ORDER BY failures DESC
LIMIT 8;
```

```text
8 row(s) in 40957 ms; showing 8.
+--------------+-------+----------+
| tool         | calls | failures |
+--------------+-------+----------+
| Bash         | 9482  | 311      |
| exec_command | 2214  | 140      |
| Edit         | 1415  | 93       |
| Write        | 618   | 12       |
| ...          | ...   | ...      |
+--------------+-------+----------+
```

This stays on the narrow `tool_name`, `call_id` and `is_failure` columns and never opens a JSON body, which is why it can sweep a month of every client's tool calls. `Bash` is Claude Code's shell tool and `exec_command` is Codex's, so one result compares them.

### Which commands fail, in one project

Tool arguments live in `variant_data`. Pull one field out of it and scope the query first:

```sql
WITH failed AS (
  SELECT r.session_id, r.call_id
  FROM parts r
  JOIN messages m ON m.session_id = r.session_id AND m.message_id = r.message_id
  WHERE r.type = 'tool_result' AND r.is_failure AND r.tool_name = 'Bash'
    AND m.project LIKE '%acme-api%'
    AND m.timestamp >= now() - INTERVAL '7 days'
)
SELECT split_part(json_get_string(json_get(c.variant_data, 'params'), 'command'), ' ', 1) AS program,
       COUNT(*) AS failures
FROM parts c
JOIN failed f ON f.session_id = c.session_id AND f.call_id = c.call_id
WHERE c.type = 'tool_call' AND c.tool_name = 'Bash'
GROUP BY program
ORDER BY failures DESC
LIMIT 8;
```

```text
4 row(s) in 95781 ms; showing 4.
+---------+----------+
| program | failures |
+---------+----------+
| cargo   | 14       |
| git     | 6        |
| npm     | 3        |
| curl    | 1        |
+---------+----------+
```

The scoping is not decoration. The same query without the `project` filter, joining `variant_data` across a whole multi-million-message store, ran out of the 512 MB query memory pool and said so, with a hint to filter before joining. Narrow to a project or a session first, then read JSON.

### Tokens for one session

```sql
WITH turns AS (
  SELECT json_get_string(options, 'anthropic', 'id') AS turn_id,
         MAX(json_get_int(options, 'anthropic', 'usage', 'input_tokens')) AS in_tok,
         MAX(json_get_int(options, 'anthropic', 'usage', 'output_tokens')) AS out_tok,
         MAX(json_get_int(options, 'anthropic', 'usage', 'cache_read_input_tokens')) AS cache_read
  FROM messages
  WHERE session_id = '<session-id>' AND role = 'assistant'
    AND json_get_string(options, 'anthropic', 'id') IS NOT NULL
  GROUP BY turn_id
)
SELECT COUNT(*) AS turns,
       SUM(in_tok) AS input_tokens,
       SUM(out_tok) AS output_tokens,
       SUM(cache_read) AS cache_read_tokens
FROM turns;
```

```text
1 row(s) in 1154 ms; showing 1.
+-------+--------------+---------------+-------------------+
| turns | input_tokens | output_tokens | cache_read_tokens |
+-------+--------------+---------------+-------------------+
| 48    | 1310         | 27940         | 4133189           |
+-------+--------------+---------------+-------------------+
```

Two traps this query steps around. Claude Code writes one transcript line per content block, each repeating the turn's usage, so summing per row counts a turn two or three times; the `MAX` per provider message id collapses that. And the JSON path is Claude Code's: these fields are what its transcripts record, kept verbatim in `options`. Another client records usage under its own keys, or not at all.

## Running them

- **From a shell:** `pond sql "<query>"`. Text output is capped at 100 rows (`--limit`, up to 1000). `--format ndjson` or `--format parquet -o out.parquet` writes every row, for DuckDB or pandas.
- **From an agent:** ask for the number. The agent calls `pond_sql` and reads the schema resource itself.
- **Time:** the default timeout is 30 seconds; `--timeout` raises it to at most 600. The timings above are from a store in an S3 bucket holding several million messages, where the JSON-reading query took a minute and a half. Metadata queries on indexed columns come back in a second or two.

## What it is not for

Finding a conversation is [`pond search`](/docs/reference/cli#query), and reading one is `pond get-session`. SQL over message text works (`contains_tokens(search_text, 'occ retry')` filters on whole words), but search ranks and SQL does not. Semantic search is not available in SQL at all.

And it is read-only by construction: no query can change the store, from the CLI, MCP or HTTP.

## Next

- [Install](/docs/get-started/install) - Homebrew, Nix, Scoop, Cargo
- [MCP tools](/docs/reference/mcp-tools#pond_sql) - `pond_sql` and the schema resources
- [Where Claude Code stores sessions](/docs/guides/where-claude-code-stores-sessions) - the transcripts these rows come from
- [Claude Code deletes sessions after 30 days](/docs/guides/claude-code-session-retention) - why last quarter's numbers need an archive
