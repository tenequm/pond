You are judging whether individual `pond_search` calls (an MCP tool that searches an archive of past AI-agent sessions) FOUND what the calling agent needed.

Input: a JSON array of "windows". Each window has:
- n, call_id, mode ("vector" = semantic, "fts" = BM25 keyword; "null" never occurs - already normalized)
- params: the search call's parameters (query, filters)
- context_before: the last user/assistant texts before the call (what the agent was trying to find)
- result_head: the first ~1800 chars of the search response (header line gives hit counts; "no matches" = empty; "NaN"/"best 0.00" scores = a broken-scoring period, judge by content not score)
- after: the next ~8 events the agent produced after the result (tool calls, tool results, assistant texts)

Classify each call's OUTCOME with one label:
- FOUND: the result delivered what was needed. Evidence: the agent followed up on a session/message id that appears in this result (pond_get / pond_get_session / pond_get_message), or its next assistant text cites a concrete fact that is visibly present in result_head, or it stops searching this topic and proceeds using it.
- PARTIAL: relevant hits present but insufficient - the agent re-queried the same need (reworded query, changed filters, switched mode, or went to pond_sql) before moving on.
- NOT_FOUND: empty, irrelevant, or error result; the agent abandoned, switched tool/mode, or concluded nothing exists.
- UNCLEAR: the window does not let you tell (e.g. part of a parallel burst where the follow-up cannot be attributed to this call, or the window ends right after the call).

Rules:
- Judge THIS call only. In a burst of parallel searches, attribute a follow-up get to a call only if the id it fetches appears in THIS call's result_head; if the follow-up ids are not visible in result_head (truncated), and the agent's text suggests the burst overall worked, use PARTIAL-or-UNCLEAR honestly, not FOUND.
- A result that lists hits is not automatically FOUND - vector mode always returns "nearest" rows even when nothing relevant exists. Read the matched text vs the query.
- Also record `mode_switch`: if within `after` the agent re-ran pond_search on the same need with the OTHER mode, set to "to_fts" or "to_vector", else null. And `next_action`: one of get_session|get_message|research_same_mode|switched_mode|pond_sql|other_tool|answered|none.
- One-line `why` (<= 120 chars).

Write your verdicts as a JSON array to the OUTPUT PATH given, exactly one object per input window, preserving n and call_id:
[{"n":1,"call_id":"...","mode":"vector","outcome":"FOUND","mode_switch":null,"next_action":"get_session","why":"..."}, ...]
Use a Python heredoc or Write tool to write the file. Your final message should be just the counts per outcome.
