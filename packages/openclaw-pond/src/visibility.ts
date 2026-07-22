// Vendored from OpenClaw `src/plugin-sdk/session-visibility.ts` (HEAD 988e640c):
// upstream demoted the subpath to bundled-only (compat registry
// `plugin-sdk-session-visibility-public-demotion`, removeAfter 2026-07-30, no
// external successor), so the plugin carries the pure policy surface it needs -
// visibility resolution and the agent-to-agent allow matcher - itself.
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";

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

type CompiledAgentAllowPattern =
  | { kind: "all" }
  | { kind: "deny" }
  | { kind: "exact"; value: string }
  | { kind: "wildcard"; first: string; last: string; interior: string[] };

function compileAgentAllowPattern(pattern: unknown): CompiledAgentAllowPattern {
  const raw = typeof pattern === "string" ? pattern.trim() : "";
  if (!raw) {
    return { kind: "deny" };
  }
  if (raw === "*") {
    return { kind: "all" };
  }
  if (!raw.includes("*")) {
    return { kind: "exact", value: raw };
  }
  const parts = raw.toLowerCase().split("*");
  return {
    kind: "wildcard",
    first: parts[0] ?? "",
    last: parts[parts.length - 1] ?? "",
    interior: parts.slice(1, -1).filter(Boolean),
  };
}

// Linear-time case-insensitive glob match on precompiled `*` patterns: prefix,
// suffix, then ordered interior segments - no regex engine, no backtracking.
function matchesCompiledWildcard(
  pattern: Extract<CompiledAgentAllowPattern, { kind: "wildcard" }>,
  lower: string,
): boolean {
  let pos = 0;
  if (pattern.first) {
    if (!lower.startsWith(pattern.first)) {
      return false;
    }
    pos = pattern.first.length;
  }

  const endBound = pattern.last ? lower.length - pattern.last.length : lower.length;
  if (pattern.last && (!lower.endsWith(pattern.last) || endBound < pos)) {
    return false;
  }

  for (const part of pattern.interior) {
    const idx = lower.indexOf(part, pos);
    if (idx === -1 || idx + part.length > endBound) {
      return false;
    }
    pos = idx + part.length;
  }

  return true;
}

export function createAgentToAgentPolicy(cfg: OpenClawConfig): AgentToAgentPolicy {
  const routingA2A = cfg.tools?.agentToAgent;
  const enabled = routingA2A?.enabled === true;
  const rawAllowPatterns = Array.isArray(routingA2A?.allow) ? routingA2A.allow : [];
  const allowPatterns = rawAllowPatterns.map((pattern) => compileAgentAllowPattern(pattern));
  const hasWildcardPatterns = allowPatterns.some((pattern) => pattern.kind === "wildcard");
  const matchesAllow = (agentId: string) => {
    if (allowPatterns.length === 0) {
      return true;
    }
    const lowerAgentId = hasWildcardPatterns ? agentId.toLowerCase() : "";
    return allowPatterns.some((pattern) => {
      if (pattern.kind === "all") {
        return true;
      }
      if (pattern.kind === "deny") {
        return false;
      }
      if (pattern.kind === "exact") {
        return pattern.value === agentId;
      }
      return matchesCompiledWildcard(pattern, lowerAgentId);
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
