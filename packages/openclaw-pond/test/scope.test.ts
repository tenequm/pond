import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import { describe, expect, it } from "vitest";
import {
  agentIdFromSessionKey,
  isGroupContext,
  isLeafSubagentContext,
  resolvePondScope,
  resolveScopeFromContext,
  type ScopeContext,
} from "../src/scope.js";

const KEY = "agent:a1:main";

function cfg(params: {
  visibility?: string;
  a2aEnabled?: boolean;
  a2aAllow?: string[];
  sandboxClamp?: "spawned" | "all";
}): OpenClawConfig {
  return {
    tools: {
      ...(params.visibility ? { sessions: { visibility: params.visibility } } : {}),
      ...(params.a2aEnabled !== undefined || params.a2aAllow
        ? { agentToAgent: { enabled: params.a2aEnabled ?? false, allow: params.a2aAllow ?? [] } }
        : {}),
    },
    ...(params.sandboxClamp
      ? { agents: { defaults: { sandbox: { sessionToolsVisibility: params.sandboxClamp } } } }
      : {}),
  };
}

const baseCtx: ScopeContext = { sessionKey: KEY, agentId: "a1" };

describe("helpers", () => {
  it("derives agentId from an agent session key", () => {
    expect(agentIdFromSessionKey("agent:a1:subagent:x")).toBe("a1");
    expect(agentIdFromSessionKey("cron:job1")).toBeUndefined();
    expect(agentIdFromSessionKey(undefined)).toBeUndefined();
  });

  it("hides tools from sandboxed leaf subagents only", () => {
    expect(isLeafSubagentContext({ sessionKey: "agent:a1:subagent:x", sandboxed: true })).toBe(true);
    expect(isLeafSubagentContext({ sessionKey: "agent:a1:subagent:x", sandboxed: false })).toBe(false);
    expect(isLeafSubagentContext({ sessionKey: KEY, sandboxed: true })).toBe(false);
  });

  it("detects group/channel context", () => {
    expect(isGroupContext({ messageChannel: "telegram" })).toBe(true);
    expect(isGroupContext({ sessionKey: "agent:a1:telegram:group:42" })).toBe(true);
    expect(isGroupContext({ sessionKey: KEY })).toBe(false);
  });
});

describe("resolvePondScope translation matrix", () => {
  it("self pins the exact session key", () => {
    const r = resolvePondScope({ visibility: "self", a2aEnabled: false, ctx: baseCtx, groupSessions: "clamp" });
    expect(r).toEqual({ ok: true, project: KEY });
  });

  it("self fails closed without a session key", () => {
    const r = resolvePondScope({
      visibility: "self",
      a2aEnabled: false,
      ctx: { agentId: "a1" },
      groupSessions: "clamp",
    });
    expect(r.ok).toBe(false);
  });

  it("tree and agent both clamp to the own-agent prefix", () => {
    for (const visibility of ["tree", "agent"] as const) {
      const r = resolvePondScope({ visibility, a2aEnabled: false, ctx: baseCtx, groupSessions: "clamp" });
      expect(r).toEqual({ ok: true, project: "agent:a1:" });
    }
  });

  it("derives the agent prefix from the session key when agentId is absent", () => {
    const r = resolvePondScope({
      visibility: "tree",
      a2aEnabled: false,
      ctx: { sessionKey: "agent:x9:main" },
      groupSessions: "clamp",
    });
    expect(r).toEqual({ ok: true, project: "agent:x9:" });
  });

  it("all without agent-to-agent clamps to the own agent", () => {
    const r = resolvePondScope({ visibility: "all", a2aEnabled: false, ctx: baseCtx, groupSessions: "clamp" });
    expect(r).toEqual({ ok: true, project: "agent:a1:" });
  });

  it("all with agent-to-agent drops the project clamp", () => {
    const r = resolvePondScope({ visibility: "all", a2aEnabled: true, ctx: baseCtx, groupSessions: "clamp" });
    expect(r).toEqual({ ok: true });
  });

  it("fails closed when identity is missing for a scope that needs it", () => {
    const r = resolvePondScope({ visibility: "tree", a2aEnabled: false, ctx: {}, groupSessions: "clamp" });
    expect(r.ok).toBe(false);
  });

  it("group context clamps agent/all down to tree unless inherit", () => {
    const groupCtx: ScopeContext = { sessionKey: "agent:a1:telegram:group:1", agentId: "a1", messageChannel: "telegram" };
    // all + a2a would normally drop the clamp; group clamp forces tree -> own agent.
    const clamped = resolvePondScope({ visibility: "all", a2aEnabled: true, ctx: groupCtx, groupSessions: "clamp" });
    expect(clamped).toEqual({ ok: true, project: "agent:a1:" });
    const inherited = resolvePondScope({ visibility: "all", a2aEnabled: true, ctx: groupCtx, groupSessions: "inherit" });
    expect(inherited).toEqual({ ok: true });
  });
});

describe("resolveScopeFromContext (SDK-backed)", () => {
  it("reads the operator's visibility and returns both visibility and clamp", () => {
    const out = resolveScopeFromContext({ cfg: cfg({ visibility: "agent" }), ctx: baseCtx, groupSessions: "clamp" });
    expect(out.visibility).toBe("agent");
    expect(out.scope).toEqual({ ok: true, project: "agent:a1:" });
  });

  it("defaults missing visibility to tree", () => {
    const out = resolveScopeFromContext({ cfg: {}, ctx: baseCtx, groupSessions: "clamp" });
    expect(out.visibility).toBe("tree");
    expect(out.scope).toEqual({ ok: true, project: "agent:a1:" });
  });

  it("honors agent-to-agent enablement for all visibility", () => {
    const denied = resolveScopeFromContext({ cfg: cfg({ visibility: "all", a2aEnabled: false }), ctx: baseCtx, groupSessions: "clamp" });
    expect(denied.scope).toEqual({ ok: true, project: "agent:a1:" });
    const allowed = resolveScopeFromContext({ cfg: cfg({ visibility: "all", a2aEnabled: true }), ctx: baseCtx, groupSessions: "clamp" });
    expect(allowed.scope).toEqual({ ok: true });
  });

  it("sandbox clamp drops non-tree visibility to tree", () => {
    const out = resolveScopeFromContext({
      cfg: cfg({ visibility: "all", a2aEnabled: true }),
      ctx: { ...baseCtx, sandboxed: true },
      groupSessions: "clamp",
    });
    // sandbox clamp (spawned default) forces tree -> own-agent clamp despite all+a2a.
    expect(out.visibility).toBe("tree");
    expect(out.scope).toEqual({ ok: true, project: "agent:a1:" });
  });
});
