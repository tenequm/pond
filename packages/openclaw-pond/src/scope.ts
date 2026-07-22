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
// coarser than a strict tree. `self` pins the exact session key; `all` drops the
// clamp only when agent-to-agent access is enabled (else it clamps to the own
// agent, mirroring the SDK row checker denying cross-agent reads without a2a).
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import {
  createAgentToAgentPolicy,
  resolveEffectiveSessionToolsVisibility,
  type SessionToolsVisibility,
} from "openclaw/plugin-sdk/session-visibility";
import type { GroupSessionsPolicy } from "./config.js";

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

// The tool context carries no explicit subagent role (leaf vs orchestrator lives
// in stored session metadata, not the per-call ctx). Sandboxed subagents are the
// constrained leaves whose visibility core clamps and to which it denies
// sessions_search; mirror that posture by hiding pond tools from them.
export function isLeafSubagentContext(ctx: ScopeContext): boolean {
  return isSubagentSessionKey(ctx.sessionKey) && ctx.sandboxed === true;
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
  a2aEnabled: boolean;
  ctx: ScopeContext;
  groupSessions: GroupSessionsPolicy;
};

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
      return input.a2aEnabled ? { ok: true } : ownAgentClamp();
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
  const a2a = createAgentToAgentPolicy(params.cfg);
  // resolvePondScope applies no group clamp of its own here: `visibility` is
  // already group-clamped, so passing "clamp" would be a harmless no-op.
  const scope = resolvePondScope({
    visibility,
    a2aEnabled: a2a.enabled,
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
