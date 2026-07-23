// Vendored from OpenClaw `src/plugin-sdk/session-visibility.ts` (HEAD 988e640c):
// upstream demoted the subpath to bundled-only (compat registry
// `plugin-sdk-session-visibility-public-demotion`, removeAfter 2026-07-30, no
// external successor), so the plugin carries the pure policy surface it needs -
// visibility resolution and the a2a enabled flag - itself. Upstream's per-target
// allow matcher is deliberately NOT carried: pond's single-substring project
// clamp cannot express a restricted allow list (see scope.ts), so only the
// unrestricted-list check in `a2aGrantsAllAgents` is ever consulted.
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";

export type SessionToolsVisibility = "self" | "tree" | "agent" | "all";

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

export function isAgentToAgentEnabled(cfg: OpenClawConfig): boolean {
  return cfg.tools?.agentToAgent?.enabled === true;
}
