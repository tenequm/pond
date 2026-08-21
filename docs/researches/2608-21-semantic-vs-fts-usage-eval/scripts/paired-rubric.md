Blind paired evaluation of two search result sets for the same query over an archive of past AI-agent sessions.

Input: JSON array of items. Each has: n, query, filters, context_before (what the agent was trying to find), A and B (two result sets, rendered transcripts, truncated). You do NOT know which retriever produced A or B - judge content only.

For each item decide which result set better contains what the agent needed (per query + context_before):
- winner: "A" | "B" | "both" (both contain it comparably) | "neither" (neither contains it / both irrelevant)
- a_has: true/false - does A contain relevant material for the need?
- b_has: true/false - same for B
- why: <= 100 chars

Be strict: a result set that lists hits is not relevant unless the matched text actually addresses the need. "no matches" = false.

Write a JSON array [{"n":1,"winner":"A","a_has":true,"b_has":false,"why":"..."}, ...] to the OUTPUT PATH given, one object per input item. Final message: counts of winner values only.
