---
description: pond's MCP tools - pond_search (full-text search over agent sessions, semantic when enabled), pond_get_session (a whole session by id), pond_get_message (one message with full tool bodies), and pond_sql (read-only SQL over sessions, messages, and parts).
---

# MCP tools

What your agent gets when pond is registered as an MCP server ([Connect your agents](/get-started/connect-your-agents)). The wire schemas ship with the server; this page is the human-facing summary.

Three tools cover almost every request: `pond_search` finds, `pond_get_session` reads a session, `pond_get_message` expands one message. `pond_sql` is the advanced escape hatch for what those cannot express.

## pond_search

Find relevant messages across all stored sessions. Returns a readable transcript of ranked hits grouped by session, best session first.

| Parameter | Meaning |
|---|---|
| `query` | what to search for - distinctive words from the work you are looking for |
| `mode` | `fts` (default, exact whole words, BM25) or `vector` (matches on meaning) - the `vector` arm is refused unless [semantic search is enabled](/reference/configuration#embeddings) |
| `sort_by` | `relevance` (default) or `recency` |
| `project` | only sessions whose project path contains this substring |
| `session_id` | search within one session |
| `from_date` / `to_date` | date bounds (`YYYY-MM-DD`) |
| `limit` | max sessions returned (default 10) - also the "more results" knob; there is no pagination |

Every response reports how many searchable messages the filters left in scope, so an empty result distinguishes "nothing relevant exists" from "my filters excluded everything". By default subagent (child) sessions are excluded; an explicit `session_id` or source-agent filter opts back in deliberately.

Search indexes only conversational text (user and assistant turns). Tool calls, tool results, and reasoning are deliberately not indexed - reach them with `pond_sql`.

## pond_get_session

Read a whole session as a readable transcript - the tool for analyzing, reviewing, or summarizing a past session: user/assistant text plus one-line tool and file references. Takes one `id`; a message id also works and resolves to its parent session with the page anchored at that message. Page with `limit` (default 20 messages), `from: "start" | "end"` (`end` reads the most recent page first - the session's final state, e.g. recovering context after compaction), and `after_message_id` / `before_message_id` to page onward.

The first page lists the session's subagent sessions in a footer so each can be opened in turn. Not for bulk export - that is [`pond copy`](/reference/cli).

## pond_get_message

Expand one message: its full parts, including tool call and result bodies, plus conversational neighbors (`context_before` / `context_after`, default 3 each side - like `grep -B/-A`). Takes one `id` (a message id; a session id is rejected with a hint naming `pond_get_session`).

## pond_sql

The advanced escape hatch: one read-only SQL query (SELECT/WITH only, DataFusion-planned) over the three tables: `sessions`, `messages`, `parts`. Not for finding or reading conversations - that is search and get. It covers what they deliberately don't: exact strings, identifiers, counts, joins, group-bys, tool-call archaeology through `parts.variant_data` (a tool_call body is `{call_id, name, params}` - a Bash command lives at `json_extract(variant_data, '$.params.command')`), and subagent sessions (`parent_session_id`).

Two useful full-text primitives:

```sql
-- all words must match; index-accelerated
SELECT * FROM messages WHERE contains_tokens(search_text, 'OCC retry race')

-- BM25-ranked
SELECT * FROM fts('messages', '{"match":{"column":"search_text","terms":"OCC retry race"}}')
```

Read-only is enforced in two layers (statement allowlist plus a read-only session), so no query can write.

## Resources

- `schema://pond` - search-field documentation and response format
- `schema://pond-sql` - the SQL surface's table schemas
- `stats://pond` - corpus and index statistics
