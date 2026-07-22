import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import type { OpenClawPluginToolContext } from "openclaw/plugin-sdk/plugin-entry";
import type { AgentToolResult } from "openclaw/plugin-sdk/tool-results";
import { afterEach, describe, expect, it } from "vitest";
import type { PondPluginConfig } from "../src/config.js";
import { parsePluginConfig } from "../src/config.js";
import { RESPONSE_MAX_BYTES } from "../src/schemas.js";
import { relayPondCall } from "../src/service.js";
import { createPondToolFactories } from "../src/tools.js";
import { createFakePond, type FakePond, type FakePondOptions } from "./fake-pond.js";

const config = parsePluginConfig(undefined);

let fake: FakePond | undefined;
afterEach(async () => {
  await fake?.close();
  fake = undefined;
});

type HarnessOptions = FakePondOptions & {
  config?: PondPluginConfig;
  logger?: { warn: (message: string) => void };
};

async function harness(options: HarnessOptions = {}) {
  fake = await createFakePond(options.responses ? { responses: options.responses } : {});
  const factories = createPondToolFactories({
    config: options.config ?? config,
    ...(options.logger ? { logger: options.logger } : {}),
    // Same seam production uses (PondController.callTool -> relayPondCall), so
    // the thrown-McpError -> {ok:false} mapping is exercised, not bypassed.
    callPond: (name, args) => relayPondCall(fake!.client, name, args),
  });
  return { factories, calls: fake.calls };
}

function mainCtx(cfg: OpenClawConfig = {}): OpenClawPluginToolContext {
  return { sessionKey: "agent:main:main", agentId: "main", config: cfg };
}

function details(result: AgentToolResult<unknown>): { status: string; text?: string; error?: string } {
  return result.details as { status: string; text?: string; error?: string };
}

describe("pond_search", () => {
  it("clamps the project to the caller's scope and caps the limit (golden request)", async () => {
    const { factories, calls } = await harness({ responses: { pond_search: () => "SEARCH RESULT" } });
    const tool = factories.search(mainCtx());
    expect(tool).not.toBeNull();
    const out = await tool!.execute("id", { query: "hello world", limit: 100 });
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({
      name: "pond_search",
      args: { query: "hello world", project: "agent:main:", limit: 25, source_agent: "openclaw" },
    });
    expect(details(out)).toEqual({ status: "ok", text: "SEARCH RESULT" });
  });

  it("forwards the default source_agent filter (openclaw)", async () => {
    const { factories, calls } = await harness();
    await factories.search(mainCtx())!.execute("id", { query: "q" });
    expect(calls[0]!.args.source_agent).toBe("openclaw");
  });

  it("omits source_agent when sources is [\"*\"] (cross-harness corpus)", async () => {
    const { factories, calls } = await harness({ config: parsePluginConfig({ sources: ["*"] }) });
    await factories.search(mainCtx())!.execute("id", { query: "q" });
    expect(calls[0]!.args).not.toHaveProperty("source_agent");
  });

  it("forwards the first source and warns once when several are configured", async () => {
    const warnings: string[] = [];
    const { factories, calls } = await harness({
      config: parsePluginConfig({ sources: ["openclaw", "claude-code"] }),
      logger: { warn: (message) => warnings.push(message) },
    });
    await factories.search(mainCtx())!.execute("id", { query: "q" });
    expect(calls[0]!.args.source_agent).toBe("openclaw");
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("single source");
  });

  it("lets a caller narrow within scope but never widen it", async () => {
    const { factories, calls } = await harness();
    // Narrowing to a full key that contains the scope prefix is honored.
    await factories.search(mainCtx())!.execute("id", { query: "q", project: "agent:main:telegram:group:1" });
    expect(calls[0]!.args.project).toBe("agent:main:telegram:group:1");
    // An out-of-scope narrowing is clamped back to the scope prefix.
    await factories.search(mainCtx())!.execute("id", { query: "q", project: "agent:other:main" });
    expect(calls[1]!.args.project).toBe("agent:main:");
  });

  it("fails closed (forbidden) when the tool context has no identity", async () => {
    const { factories, calls } = await harness();
    const out = await factories.search({ config: {} })!.execute("id", { query: "q" });
    expect(details(out).status).toBe("forbidden");
    expect(calls).toHaveLength(0);
  });

  it("redacts secret-shaped text and relays pond output", async () => {
    const { factories } = await harness({
      responses: { pond_search: () => "token sk-ABCDEFGHIJKLMNOP0123 done" },
    });
    const out = await factories.search(mainCtx())!.execute("id", { query: "q" });
    expect(details(out).text).toContain("sk-ABC…0123");
    expect(details(out).text).not.toContain("sk-ABCDEFGHIJKLMNOP0123");
  });

  it("byte-caps oversized responses", async () => {
    const big = "x".repeat(RESPONSE_MAX_BYTES + 5000);
    const { factories } = await harness({ responses: { pond_search: () => big } });
    const out = await factories.search(mainCtx())!.execute("id", { query: "q" });
    expect(details(out).text!.length).toBeLessThan(big.length);
    expect(details(out).text).toContain("truncated");
  });

  it("surfaces a pond tool error as a typed error result", async () => {
    const { factories } = await harness({
      responses: { pond_search: () => ({ text: "store unavailable", isError: true }) },
    });
    const out = await factories.search(mainCtx())!.execute("id", { query: "q" });
    expect(details(out)).toEqual({ status: "error", error: "store unavailable" });
  });
});

describe("pond_get_session", () => {
  it("forwards a session read (golden request/response)", async () => {
    const { factories, calls } = await harness({ responses: { pond_get_session: () => "TRANSCRIPT" } });
    const out = await factories.getSession(mainCtx())!.execute("id", { id: "s1", from: "end" });
    expect(calls[0]).toMatchObject({ name: "pond_get_session", args: { id: "s1", from: "end" } });
    expect(details(out)).toEqual({ status: "ok", text: "TRANSCRIPT" });
  });

  it("rejects an empty id before relaying", async () => {
    const { factories, calls } = await harness();
    expect(details(await factories.getSession(mainCtx())!.execute("id", { id: "  " })).status).toBe("error");
    expect(calls).toHaveLength(0);
  });
});

describe("pond_get_message", () => {
  it("relays a pond envelope error (wrong-id hint) as readable error text", async () => {
    const hint = "abc123 is a session id, not a message id - read it with pond_get_session";
    const { factories } = await harness({
      responses: { pond_get_message: () => ({ rpcError: hint }) },
    });
    const out = await factories.getMessage(mainCtx())!.execute("id", { id: "abc123" });
    const d = details(out);
    expect(d.status).toBe("error");
    expect(d.error).toContain("pond call failed");
    expect(d.error).toContain("read it with pond_get_session");
  });

  it("forwards a message expansion (golden request/response)", async () => {
    const { factories, calls } = await harness({ responses: { pond_get_message: () => "MESSAGE" } });
    const out = await factories.getMessage(mainCtx())!.execute("id", { id: "m1", context_before: 5 });
    expect(calls[0]).toMatchObject({ name: "pond_get_message", args: { id: "m1", context_before: 5 } });
    expect(details(out)).toEqual({ status: "ok", text: "MESSAGE" });
  });

  it("rejects an empty id before relaying", async () => {
    const { factories, calls } = await harness();
    expect(details(await factories.getMessage(mainCtx())!.execute("id", { id: "" })).status).toBe("error");
    expect(calls).toHaveLength(0);
  });
});

describe("pond_sql", () => {
  it("is forbidden below visibility=all and names the knob", async () => {
    const { factories, calls } = await harness();
    const out = await factories.sql(mainCtx())!.execute("id", { query: "SELECT 1" });
    expect(details(out).status).toBe("forbidden");
    expect(details(out).error).toContain("tools.sessions.visibility=all");
    expect(calls).toHaveLength(0);
  });

  it("forwards when the operator set visibility=all", async () => {
    const { factories, calls } = await harness({ responses: { pond_sql: () => "ROWS" } });
    const cfg: OpenClawConfig = { tools: { sessions: { visibility: "all" }, agentToAgent: { enabled: true } } };
    const out = await factories.sql(mainCtx(cfg))!.execute("id", { query: "SELECT 1", format: "ndjson" });
    expect(calls[0]).toMatchObject({ name: "pond_sql", args: { query: "SELECT 1", format: "ndjson" } });
    expect(details(out)).toEqual({ status: "ok", text: "ROWS" });
  });
});

describe("subagent denial", () => {
  it("hides every pond tool from any subagent context, sandboxed or not", async () => {
    const { factories } = await harness();
    for (const sandboxed of [true, false]) {
      const subagent: OpenClawPluginToolContext = {
        sessionKey: "agent:main:subagent:abc",
        agentId: "main",
        sandboxed,
        config: {},
      };
      expect(factories.search(subagent)).toBeNull();
      expect(factories.getSession(subagent)).toBeNull();
      expect(factories.getMessage(subagent)).toBeNull();
      expect(factories.sql(subagent)).toBeNull();
    }
  });
});
