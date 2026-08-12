// The `/pond` picker reads pond's rendered search transcript. These fixtures
// are the exact shape `render_search_transcript` emits (packages/pond/src/
// render.rs), so a change on the pond side fails here rather than silently
// emptying the picker.
import { describe, expect, it } from "vitest";
import { hitLabel, hitReference, parsePondHits } from "../src/hits.ts";

const TRANSCRIPT = `pond_search: 2 sessions, 3 matches.
key: session rules group hits by session, best first.

--- session [1] best 0.87 | 2/41 matched | /Users/user/Projects/pond | claude-code | sess-aaa ------------

--- [1] 0.87 | user | 2026-08-05 11:02:03Z | msg-1 | /Users/user/Projects/pond | claude-code | sess-aaa ---
We decided the append fast-path is load-bearing for S3 writes.
More detail on the same hit.

--- [2] 0.71 | assistant | 2026-08-05 11:03:00Z | msg-2 | /Users/user/Projects/pond | claude-code | sess-aaa ---
A later, lower-scoring hit in the same session.

--- session [2] best 0.55 | 1/9 matched | /Users/user/Projects/other | pi-coding-agent | sess-bbb --------

--- [3] 0.55 | user | 2026-08-01 09:00:00Z | msg-3 | /Users/user/Projects/other | pi-coding-agent | sess-bbb ---
An unrelated but matching session.
`;

describe("parsePondHits", () => {
  it("reads one entry per session, newest hit first, with its provenance", () => {
    const hits = parsePondHits(TRANSCRIPT);
    expect(hits).toHaveLength(2);
    expect(hits[0]).toMatchObject({
      sessionId: "sess-aaa",
      project: "/Users/user/Projects/pond",
      sourceAgent: "claude-code",
      timestamp: "2026-08-05 11:02:03Z",
      snippet: "We decided the append fast-path is load-bearing for S3 writes.",
    });
    expect(hits[1]).toMatchObject({ sessionId: "sess-bbb", sourceAgent: "pi-coding-agent" });
  });

  it("yields nothing rather than guessing when the transcript has no session rules", () => {
    expect(
      parsePondHits("pond_search: the store has no searchable messages yet (nothing ingested)."),
    ).toEqual([]);
  });

  it("labels a row with when, which agent, and which project", () => {
    const [hit] = parsePondHits(TRANSCRIPT);
    expect(hit && hitLabel(hit)).toBe("2026-08-05  claude-code  pond");
  });

  it("inserts a reference, never a transcript - the model pulls detail via the tools", () => {
    const [hit] = parsePondHits(TRANSCRIPT);
    const reference = hit ? hitReference(hit) : "";
    expect(reference).toContain("Past session sess-aaa");
    expect(reference).toContain("Full transcript: use pond_get_session with id sess-aaa");
    expect(reference).not.toContain("A later, lower-scoring hit");
  });
});
