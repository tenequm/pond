// Golden-request / golden-response tests against a fake pond MCP endpoint over
// an in-memory transport pair: what the extension forwards, and what it hands
// back to the model.
import { afterEach, describe, expect, it } from "vitest";
import { createFakePond, type FakePond } from "./fake-pond.ts";
import { relayPondCall } from "../src/service.ts";
import { RESPONSE_MAX_BYTES } from "../src/schemas.ts";
import { createPondTools, POND_TOOL_NAMES } from "../src/tools.ts";

let fake: FakePond | undefined;

afterEach(async () => {
  await fake?.close();
  fake = undefined;
});

async function tools(options?: Parameters<typeof createFakePond>[0]) {
  fake = await createFakePond(options);
  const pond = fake;
  const byName = new Map(
    createPondTools((name, args) => relayPondCall(pond.client, name, args)).map((tool) => [
      tool.name,
      tool,
    ]),
  );
  return { pond, byName };
}

async function run(
  byName: Awaited<ReturnType<typeof tools>>["byName"],
  name: string,
  params: unknown,
) {
  const tool = byName.get(name);
  if (!tool) {
    throw new Error(`tool ${name} not registered`);
  }
  const result = await tool.execute("call-1", params as never, undefined, undefined, {} as never);
  const first = result.content[0];
  return { details: result.details, text: first && "text" in first ? first.text : "" };
}

describe("pond tools", () => {
  it("registers exactly the four pond tools under pond's own names", async () => {
    const { byName } = await tools();
    expect([...byName.keys()].sort()).toEqual(
      Object.values(POND_TOOL_NAMES).slice().sort(),
    );
  });

  it("forwards a search with a clamped limit and only the filters that were set", async () => {
    const { pond, byName } = await tools();
    await run(byName, POND_TOOL_NAMES.search, {
      query: "  occ retry  ",
      limit: 500,
      project: "pond",
      mode: "fts",
    });
    expect(pond.calls).toEqual([
      {
        name: "pond_search",
        args: { query: "occ retry", limit: 25, mode: "fts", project: "pond" },
      },
    ]);
  });

  it("defaults to the whole archive - no project or source filter is invented", async () => {
    const { pond, byName } = await tools();
    await run(byName, POND_TOOL_NAMES.search, { query: "storage rewrite" });
    expect(pond.calls[0]?.args).toEqual({ query: "storage rewrite", limit: 10 });
  });

  it("relays pond's rendered text unmodified", async () => {
    const rendered = "pond_search: 2 sessions\n--- session [1] best 0.90 ---\nhello";
    const { byName } = await tools({ responses: { pond_search: () => rendered } });
    const result = await run(byName, POND_TOOL_NAMES.search, { query: "hi" });
    expect(result.details).toEqual({ status: "ok", text: rendered });
    expect(result.text).toBe(rendered);
  });

  it("bounds an oversized response and says so", async () => {
    const huge = "x".repeat(RESPONSE_MAX_BYTES + 1024);
    const { byName } = await tools({ responses: { pond_search: () => huge } });
    const result = await run(byName, POND_TOOL_NAMES.search, { query: "hi" });
    const text = result.text;
    expect(text.length).toBeLessThan(huge.length);
    expect(text).toContain("response truncated");
  });

  it("cuts the truncation on a code-point boundary, never mid-character", async () => {
    // The last multi-byte character straddles the budget: a byte-wise cut would
    // hand the model a U+FFFD replacement char.
    const huge = `${"x".repeat(RESPONSE_MAX_BYTES - 1)}é${"y".repeat(4096)}`;
    const { byName } = await tools({ responses: { pond_search: () => huge } });
    const result = await run(byName, POND_TOOL_NAMES.search, { query: "hi" });
    const body = result.text.split("\n\n[pond:")[0] ?? "";
    expect(body).toBe("x".repeat(RESPONSE_MAX_BYTES - 1));
    expect(result.text).not.toContain("�");
    expect(Buffer.byteLength(body, "utf8")).toBeLessThanOrEqual(RESPONSE_MAX_BYTES);
  });

  it("surfaces pond's own error text - the message that teaches the next query", async () => {
    const { byName } = await tools({
      responses: {
        pond_get_session: () => ({
          rpcError: "not_found: no session with that id; try pond_search first",
        }),
      },
    });
    const result = await run(byName, POND_TOOL_NAMES.getSession, { id: "nope" });
    expect(result.details).toMatchObject({ status: "error" });
    expect(JSON.stringify(result.details)).toContain("try pond_search first");
  });

  it("rejects an empty argument locally instead of round-tripping it", async () => {
    const { pond, byName } = await tools();
    const result = await run(byName, POND_TOOL_NAMES.sql, { query: "   " });
    expect(result.details).toEqual({ status: "error", error: "query must not be empty" });
    expect(pond.calls).toEqual([]);
  });

  it("forwards pond_sql to every caller - pi is single-user, so there is no scope gate", async () => {
    const { pond, byName } = await tools();
    await run(byName, POND_TOOL_NAMES.sql, {
      query: "SELECT count(*) FROM sessions",
      format: "ndjson",
    });
    expect(pond.calls[0]).toEqual({
      name: "pond_sql",
      args: { query: "SELECT count(*) FROM sessions", format: "ndjson" },
    });
  });

  it("passes get_message context windows through as numbers", async () => {
    const { pond, byName } = await tools();
    await run(byName, POND_TOOL_NAMES.getMessage, {
      id: "m1",
      context_before: 0,
      context_after: 5,
    });
    expect(pond.calls[0]?.args).toEqual({ id: "m1", context_before: 0, context_after: 5 });
  });
});

// The description must read correctly against ANY pond binary: the default arm
// is the running instance's business, so naming one here would go stale on the
// first upgrade that flips it.
describe("mode description is version-neutral", () => {
  it("names both arms and no default", async () => {
    const { byName } = await tools();
    const description = byName.get(POND_TOOL_NAMES.search)?.description ?? "";
    expect(description).toContain('"fts"');
    expect(description).toContain('"vector"');
    // No arm may be marked as the default: catches '"fts" (default...' and
    // 'default,' alike, while allowing the closing "pond's default." sentence.
    expect(description).not.toMatch(/"(fts|vector)"\s*\(\s*default/);
    expect(description).not.toContain("default,");
  });
});
