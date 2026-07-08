---
name: pond
description: Recall and analyze past AI agent sessions (Claude Code, Codex, opencode, and more). Find prior work and decisions, read/review/summarize a past session transcript, or run SQL analytics over session history. Use whenever the user references past sessions, prior work, "check pond", or asks what was done or decided before.
---

# pond

pond is your memory across every AI coding session you have run - stored
losslessly, searchable over MCP. If a task needs context you lack, pond likely
has it: recall first, then answer. Before you say "I don't know" or re-derive
something that sounds prior, search pond.

## Which tool

- Find past work by meaning ("what did we decide", "have we hit this before")
  -> `pond_search` (`mode=vector` default; `mode=fts` for exact whole words).
- Read, analyze, review, or summarize a session -> `pond_get(session_id)` -
  one call, full readable transcript. `pond_get(message_id)` expands one
  message with its full tool bodies.
- Corpus-wide aggregation, exact strings inside tool bodies, subagent
  sessions, bulk export -> `pond_sql` (read-only SQL). Read resource
  `schema://pond-sql` first - do not guess columns or JSON paths.

## Rules that prevent wrong conclusions

- Long sessions supersede their own early conclusions. For "what did we
  decide / latest state", read the end (`pond_get(session_id,
  session_from="end")`) or `pond_search` with `sort_by=recency` - relevance
  rank favors the early, confident, possibly overturned phrasing.
- Search covers only user/assistant conversational text - tool output is
  excluded by design. A weak search result is NOT proof of absence: verify
  exact strings with `pond_sql` `contains_tokens(search_text, '...')` before
  concluding something never happened.
- Tool bodies in SQL: tool_call is `{call_id, name, params}` (a Bash command
  is `json_extract(variant_data, '$.params.command')`); tool_result is
  `{call_id, name, is_failure, result}`.
- On a remote store, SQL over `parts` costs seconds per round-trip: scope by
  `session_id` / `tool_name`, and raise `timeout_seconds` when a broad scan
  is genuinely needed.

## Setup

`brew install tenequm/tap/pond` (or `cargo binstall pond-db`, or `nix profile
add github:tenequm/pond#pond`), then `pond init` - it registers the MCP server
and installs this skill. Keep current with `pond sync`; `pond --help` for the
rest. Claude.ai chats are not synced automatically - request a data export
(claude.ai Settings -> Privacy -> Export data, arrives as an emailed `.zip`),
then `pond sync claude-ai-export --path <export.zip>`.
Docs: https://pond.locker/
