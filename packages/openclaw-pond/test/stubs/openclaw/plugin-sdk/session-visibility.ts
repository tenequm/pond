// Faithful test double of `openclaw/plugin-sdk/session-visibility`.
//
// Ports only the pure config-reading helpers the pond plugin depends on
// (visibility resolution + agent-to-agent policy compilation + spawned-key
// listing). Logic mirrors the real SDK at openclaw HEAD so scope tests exercise
// the same decision rules. The real package supplies these at runtime; this
// double exists solely so `npm test`/`tsc` run without the OpenClaw monorepo.
import type { OpenClawConfig } from "./config-contracts.js";

export type SessionToolsVisibility = "self" | "tree" | "agent" | "all";

export type AgentToAgentPolicy = {
  enabled: boolean;
  matchesAllow: (agentId: string) => boolean;
  isAllowed: (requesterAgentId: string, targetAgentId: string) => boolean;
};

function normalizeLowercase(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

export function resolveSessionToolsVisibility(cfg: OpenClawConfig): SessionToolsVisibility {
  const value = normalizeLowercase(cfg.tools?.sessions?.visibility);
  if (value === "self" || value === "tree" || value === "agent" || value === "all") {
    return value;
  }
  return "tree";
}

export function resolveEffectiveSessionToolsVisibility(params: {
  cfg: OpenClawConfig;
  sandboxed: boolean;
}): SessionToolsVisibility {
  const visibility = resolveSessionToolsVisibility(params.cfg);
  if (!params.sandboxed) {
    return visibility;
  }
  const sandboxClamp = params.cfg.agents?.defaults?.sandbox?.sessionToolsVisibility ?? "spawned";
  if (sandboxClamp === "spawned" && visibility !== "tree") {
    return "tree";
  }
  return visibility;
}

export function createAgentToAgentPolicy(cfg: OpenClawConfig): AgentToAgentPolicy {
  const routing = cfg.tools?.agentToAgent;
  const enabled = routing?.enabled === true;
  const allow = Array.isArray(routing?.allow)
    ? routing.allow.filter((entry): entry is string => typeof entry === "string")
    : [];
  const matchesAllow = (agentId: string) => {
    if (allow.length === 0) {
      return true;
    }
    return allow.some((pattern) => {
      if (pattern === "*") {
        return true;
      }
      if (!pattern.includes("*")) {
        return pattern === agentId;
      }
      const [first = "", last = ""] = [pattern.split("*")[0], pattern.split("*").at(-1)];
      const lower = agentId.toLowerCase();
      return lower.startsWith(first.toLowerCase()) && lower.endsWith(last.toLowerCase());
    });
  };
  const isAllowed = (requesterAgentId: string, targetAgentId: string) => {
    if (requesterAgentId === targetAgentId) {
      return true;
    }
    if (!enabled) {
      return false;
    }
    return matchesAllow(requesterAgentId) && matchesAllow(targetAgentId);
  };
  return { enabled, matchesAllow, isAllowed };
}

let spawnedKeysForTest = new Set<string>();

export const sessionVisibilityStubTesting = {
  setSpawnedKeys(keys: Iterable<string>) {
    spawnedKeysForTest = new Set(keys);
  },
  reset() {
    spawnedKeysForTest = new Set();
  },
};

export async function listSpawnedSessionKeys(_params: {
  requesterSessionKey: string;
  limit?: number;
}): Promise<Set<string>> {
  return new Set(spawnedKeysForTest);
}
