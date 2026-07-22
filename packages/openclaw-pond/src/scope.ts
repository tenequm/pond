// Access scoping: translate the operator's session-tool visibility into a pond
// `project` substring clamp, applied before forwarding a call.
//
// This is policy against a confused or prompt-injected agent, NOT a security
// boundary against the operator (who can read the store directly). We reuse the
// OpenClaw SDK to resolve the operator's real `tools.sessions.visibility` and
// `tools.agentToAgent` policy and add no parallel vocabulary for them.
//
// pond's MCP `project` filter is a single substring (src/transport.rs maps it to
// ProjectFilter::Contains), so a set of keys cannot be expressed in one call.
// Consequently `tree` and `agent` both clamp to the requester's own-agent key
// prefix `agent:<agentId>:` - bounded to a single agent (the primary leak risk),
// coarser than a strict tree. `self` pins the exact session key. `all` drops the
// clamp only when agent-to-agent access grants every agent: core's rule is
// `enabled && matchesAllow(requester) && matchesAllow(target)` per target, and
// dropping the clamp exposes every target at once - sound only when the allow
// list is unrestricted (empty or "*"). Any restricted allow list is
// inexpressible in one substring, so it falls back to the own-agent clamp
// (fail-closed to the expressible subset; own sessions are always permitted).
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import type { GroupSessionsPolicy } from "./config.js";
import {
  createAgentToAgentPolicy,
  resolveEffectiveSessionToolsVisibility,
  type SessionToolsVisibility,
} from "./visibility.js";

export type ScopeContext = {
  sessionKey?: string;
  agentId?: string;
  sandboxed?: boolean;
  messageChannel?: string;
};

export type ScopeResolution =
  | { ok: true; project?: string }
  | { ok: false; error: string };

export function agentIdFromSessionKey(sessionKey: string | undefined): string | undefined {
  if (!sessionKey || !sessionKey.startsWith("agent:")) {
    return undefined;
  }
  const parts = sessionKey.split(":");
  return parts[1] && parts[1].length > 0 ? parts[1] : undefined;
}

export function isSubagentSessionKey(sessionKey: string | undefined): boolean {
  return typeof sessionKey === "string" && sessionKey.includes(":subagent:");
}

// Core denies sessions_search to leaf-ROLE subagents (spawn depth), but the
// tool context carries no role/depth signal, so the exact condition cannot be
// replicated. Hide pond tools from ALL subagent contexts instead - a
// conservative superset of "leaves denied" that never over-exposes. Subagents
// needing historical context get it passed in by the agent that spawned them.
export function isSubagentContext(ctx: ScopeContext): boolean {
  return isSubagentSessionKey(ctx.sessionKey);
}

export function isGroupContext(ctx: ScopeContext): boolean {
  return (
    (typeof ctx.messageChannel === "string" && ctx.messageChannel.length > 0) ||
    (typeof ctx.sessionKey === "string" && ctx.sessionKey.includes(":group:"))
  );
}

function clampToTree(visibility: SessionToolsVisibility): SessionToolsVisibility {
  return visibility === "agent" || visibility === "all" ? "tree" : visibility;
}

export type ResolveScopeInput = {
  visibility: SessionToolsVisibility;
  /** True only when a2a is enabled AND its allow list grants every agent. */
  a2aGrantsAll: boolean;
  ctx: ScopeContext;
  groupSessions: GroupSessionsPolicy;
};

// Dropping the pond clamp exposes every agent's sessions in one call, so it
// requires the a2a policy to grant every target: enabled with an unrestricted
// allow list (empty or containing "*"). A restricted list cannot be expressed
// in pond's single project substring - callers fall back to the own-agent
// clamp, matching core's per-target matchesAllow gate fail-closed.
export function a2aGrantsAllAgents(cfg: OpenClawConfig): boolean {
  if (createAgentToAgentPolicy(cfg).enabled !== true) {
    return false;
  }
  const allow = cfg.tools?.agentToAgent?.allow;
  const patterns = Array.isArray(allow)
    ? allow.filter((entry): entry is string => typeof entry === "string")
    : [];
  return patterns.length === 0 || patterns.includes("*");
}

// Pure translation from a resolved visibility to a pond project clamp. Fails
// closed when identity needed for the clamp is missing.
export function resolvePondScope(input: ResolveScopeInput): ScopeResolution {
  const { ctx } = input;
  const visibility =
    isGroupContext(ctx) && input.groupSessions === "clamp"
      ? clampToTree(input.visibility)
      : input.visibility;

  const agentId = ctx.agentId ?? agentIdFromSessionKey(ctx.sessionKey);
  const ownAgentClamp = (): ScopeResolution =>
    agentId
      ? { ok: true, project: `agent:${agentId}:` }
      : {
          ok: false,
          error:
            "pond scope resolution failed: no agent identity in tool context. " +
            "Narrow with an explicit project filter or run from an identified session.",
        };

  switch (visibility) {
    case "self":
      return ctx.sessionKey
        ? { ok: true, project: ctx.sessionKey }
        : {
            ok: false,
            error:
              "pond scope resolution failed: tools.sessions.visibility=self but the calling " +
              "session key is absent from the tool context.",
          };
    case "tree":
    case "agent":
      return ownAgentClamp();
    case "all":
      return input.a2aGrantsAll ? { ok: true } : ownAgentClamp();
  }
}

// Effective visibility after the group-context clamp (before pond translation).
export function resolveEffectiveVisibility(params: {
  cfg: OpenClawConfig;
  ctx: ScopeContext;
  groupSessions: GroupSessionsPolicy;
}): SessionToolsVisibility {
  const base = resolveEffectiveSessionToolsVisibility({
    cfg: params.cfg,
    sandboxed: params.ctx.sandboxed === true,
  });
  return isGroupContext(params.ctx) && params.groupSessions === "clamp" ? clampToTree(base) : base;
}

// Resolve scope directly from OpenClaw config + tool context, reusing the SDK.
// Returns both the group-clamped effective visibility (the SQL tool gate reads
// it) and the pond project clamp for search.
export function resolveScopeFromContext(params: {
  cfg: OpenClawConfig;
  ctx: ScopeContext;
  groupSessions: GroupSessionsPolicy;
}): { visibility: SessionToolsVisibility; scope: ScopeResolution } {
  const visibility = resolveEffectiveVisibility(params);
  // resolvePondScope applies no group clamp of its own here: `visibility` is
  // already group-clamped, so passing "clamp" would be a harmless no-op.
  const scope = resolvePondScope({
    visibility,
    a2aGrantsAll: a2aGrantsAllAgents(params.cfg),
    ctx: params.ctx,
    groupSessions: "inherit",
  });
  return { visibility, scope };
}

// When both a scope clamp and a caller-supplied project narrowing exist, honor
// the narrowing only if it stays within scope (one contains the other); pond's
// single-substring project filter cannot AND two independent substrings. Lives
// here beside the scope model it encodes so the two cannot drift.
export function combineProject(
  scopeProject: string | undefined,
  callerProject: string | undefined,
): string | undefined {
  if (!scopeProject) {
    return callerProject;
  }
  if (!callerProject) {
    return scopeProject;
  }
  return callerProject.includes(scopeProject) || scopeProject.includes(callerProject)
    ? callerProject
    : scopeProject;
}
