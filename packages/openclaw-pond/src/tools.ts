// The four projected pond tools. Each forwards to pond over MCP, after
// resolving the caller's scope and clamping filters. pond renders its own
// agent-facing transcript text (its MCP surface is text, not structured hits),
// so the plugin relays that text redacted and byte-bounded, with a typed
// forbidden/error union - mirroring core sessions_search's contract shape.
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import { redactToolPayloadText } from "openclaw/plugin-sdk/logging-core";
import type { AnyAgentTool, OpenClawPluginToolContext } from "openclaw/plugin-sdk/plugin-entry";
import type { PondPluginConfig } from "./config.js";
import type { PondCallResult } from "./mcp.js";
import {
  combineProject,
  isSubagentContext,
  resolveScopeFromContext,
  type ScopeContext,
} from "./scope.js";
import {
  RESPONSE_MAX_BYTES,
  SEARCH_DEFAULT_LIMIT,
  SEARCH_MAX_LIMIT,
  SearchParamsSchema,
  GetSessionParamsSchema,
  GetMessageParamsSchema,
  SqlParamsSchema,
  ToolOutputSchema,
  type GetSessionParams,
  type GetMessageParams,
  type SearchParams,
  type SqlParams,
} from "./schemas.js";

// Vendored structural copy of upstream AgentToolResult (openclaw
// packages/agent-core/src/types.ts): the `openclaw/plugin-sdk/tool-results`
// subpath was demoted to bundled-only (compat registry
// `plugin-sdk-tool-results-public-demotion`, removeAfter 2026-07-30) with no
// public successor.
export type AgentToolResult<T> = {
  content: ({ type: "text"; text: string } | { type: "image"; data: string; mimeType: string })[];
  details: T;
  progress?: { text: string; visibility: "channel"; privacy: "public"; id?: string };
  terminate?: boolean;
};

export type PondCaller = (name: string, args: Record<string, unknown>) => Promise<PondCallResult>;

export type PondToolDeps = {
  callPond: PondCaller;
  config: PondPluginConfig;
  logger?: { warn: (message: string) => void };
};

// pond's pond_search `source_agent` filter matches a source_agent that equals
// the value OR starts with `<value>/`, so "openclaw" covers openclaw plus
// openclaw/subagent, /cron, /hook, /probe. The filter takes a single source:
// "*" opts into the whole cross-harness corpus (no filter); one entry forwards
// verbatim; multiple entries forward the first with a one-time warning. For
// visibility below `all` the project clamp (a session-key substring) already
// excludes foreign-harness sessions implicitly - `sources` is the explicit axis
// and matters most at visibility=all where there is no project clamp.
function resolveSourceAgent(
  sources: string[],
  warn?: (message: string) => void,
): string | undefined {
  if (sources.includes("*")) {
    return undefined;
  }
  if (sources.length > 1) {
    warn?.(
      `pond source_agent filter takes a single source; using the first of [${sources.join(", ")}] ` +
        `and ignoring the rest.`,
    );
  }
  return sources[0];
}

type OutputStatus = { status: "ok"; text: string } | { status: "forbidden" | "error"; error: string };

function result(details: OutputStatus, text: string): AgentToolResult<OutputStatus> {
  return { content: [{ type: "text", text }], details };
}

function okResult(text: string): AgentToolResult<OutputStatus> {
  return result({ status: "ok", text }, text);
}

function forbiddenResult(error: string): AgentToolResult<OutputStatus> {
  return result({ status: "forbidden", error }, error);
}

function errorResult(error: string): AgentToolResult<OutputStatus> {
  return result({ status: "error", error }, error);
}

function boundedText(raw: string): string {
  const redacted = redactToolPayloadText(raw);
  const buffer = Buffer.from(redacted, "utf8");
  if (buffer.byteLength <= RESPONSE_MAX_BYTES) {
    return redacted;
  }
  const clipped = buffer.subarray(0, RESPONSE_MAX_BYTES).toString("utf8");
  return `${clipped}\n\n[pond: response truncated to ${RESPONSE_MAX_BYTES} bytes; narrow the query or lower limit]`;
}

function resolveConfig(ctx: OpenClawPluginToolContext): OpenClawConfig {
  return ctx.getRuntimeConfig?.() ?? ctx.runtimeConfig ?? ctx.config ?? ({} as OpenClawConfig);
}

function scopeContext(ctx: OpenClawPluginToolContext): ScopeContext {
  return {
    ...(ctx.sessionKey !== undefined ? { sessionKey: ctx.sessionKey } : {}),
    ...(ctx.agentId !== undefined ? { agentId: ctx.agentId } : {}),
    ...(ctx.sandboxed !== undefined ? { sandboxed: ctx.sandboxed } : {}),
    ...(ctx.messageChannel !== undefined ? { messageChannel: ctx.messageChannel } : {}),
  };
}

async function relay(
  deps: PondToolDeps,
  name: string,
  args: Record<string, unknown>,
): Promise<AgentToolResult<OutputStatus>> {
  const response = await deps.callPond(name, args);
  if (!response.ok) {
    return errorResult(response.error);
  }
  return okResult(boundedText(response.text));
}

const SEARCH_DESCRIPTION =
  "Find relevant messages in your durable pond corpus of past agent sessions (Claude Code, OpenClaw, " +
  "and others). Returns pond's rendered transcript: results grouped by session, best first. Pick `mode`: " +
  "\"vector\" (default, meaning) or \"fts\" (exact words, BM25). Pass a hit's session_id to " +
  "pond_get_session or its message_id to pond_get_message. Results are scoped to the sessions you may " +
  "already read.";

const GET_SESSION_DESCRIPTION =
  "Read a whole past session from pond as a chronological transcript - the tool for analyzing, " +
  "reviewing, or summarizing a session. Pass an id from pond_search (a message_id also works: it " +
  "resolves to its parent session anchored at that message). from=\"end\" reads the most recent turns; " +
  "after_message_id / before_message_id page on from a page marker.";

const GET_MESSAGE_DESCRIPTION =
  "Expand one pond message with its full part bodies (tool_call / tool_result / reasoning) plus " +
  "conversational neighbors; context_before / context_after size the window (like grep -B/-A). Pass a " +
  "message_id from pond_search; for the whole session use pond_get_session.";

const SQL_DESCRIPTION =
  "Advanced escape hatch: run ONE read-only SQL SELECT over pond's corpus as three tables (sessions, " +
  "messages, parts) for analytics: filtering, joins, counts, group-by. Cross-session analytics tool; " +
  "available when your session visibility is `all`. Use pond_search / pond_get_session for scoped reads.";

export function createPondToolFactories(deps: PondToolDeps) {
  const sourceAgent = resolveSourceAgent(deps.config.sources, deps.logger?.warn);

  const search = (ctx: OpenClawPluginToolContext): AnyAgentTool | null => {
    if (isSubagentContext(scopeContext(ctx))) {
      return null;
    }
    return {
      name: "pond_search",
      label: "Pond Search",
      description: SEARCH_DESCRIPTION,
      parameters: SearchParamsSchema,
      outputSchema: ToolOutputSchema,
      execute: async (_id, rawArgs) => {
        const args = rawArgs as SearchParams;
        const query = typeof args.query === "string" ? args.query.trim() : "";
        if (!query) {
          return errorResult("query must not be empty");
        }
        const { scope } = resolveScopeFromContext({
          cfg: resolveConfig(ctx),
          ctx: scopeContext(ctx),
          groupSessions: deps.config.groupSessions,
        });
        if (!scope.ok) {
          return forbiddenResult(scope.error);
        }
        const limit = Math.min(SEARCH_MAX_LIMIT, Math.max(1, args.limit ?? SEARCH_DEFAULT_LIMIT));
        const project = combineProject(scope.project, args.project);
        const pondArgs: Record<string, unknown> = { query, limit };
        if (args.mode) pondArgs.mode = args.mode;
        if (args.sort_by) pondArgs.sort_by = args.sort_by;
        if (project) pondArgs.project = project;
        if (sourceAgent) pondArgs.source_agent = sourceAgent;
        if (args.session_id) pondArgs.session_id = args.session_id;
        if (args.from_date) pondArgs.from_date = args.from_date;
        if (args.to_date) pondArgs.to_date = args.to_date;
        return relay(deps, "pond_search", pondArgs);
      },
    };
  };

  // Gets are targeted follow-ups to a scoped search; enforce the same
  // ctx/leaf/fail-closed gate. The get tools have no project filter and their
  // MCP output is text, so per-target scope is not re-verified server-side -
  // this is deliberate (policy, not a security boundary against the operator).
  const getSession = (ctx: OpenClawPluginToolContext): AnyAgentTool | null => {
    if (isSubagentContext(scopeContext(ctx))) {
      return null;
    }
    return {
      name: "pond_get_session",
      label: "Pond Get Session",
      description: GET_SESSION_DESCRIPTION,
      parameters: GetSessionParamsSchema,
      outputSchema: ToolOutputSchema,
      execute: async (_id, rawArgs) => {
        const args = rawArgs as GetSessionParams;
        const id = typeof args.id === "string" ? args.id.trim() : "";
        if (!id) {
          return errorResult("id must not be empty");
        }
        const { scope } = resolveScopeFromContext({
          cfg: resolveConfig(ctx),
          ctx: scopeContext(ctx),
          groupSessions: deps.config.groupSessions,
        });
        if (!scope.ok) {
          return forbiddenResult(scope.error);
        }
        const pondArgs: Record<string, unknown> = { id };
        if (args.from) pondArgs.from = args.from;
        if (typeof args.limit === "number") pondArgs.limit = args.limit;
        for (const key of ["after_message_id", "before_message_id"] as const) {
          const value = args[key];
          if (typeof value === "string" && value.length > 0) {
            pondArgs[key] = value;
          }
        }
        return relay(deps, "pond_get_session", pondArgs);
      },
    };
  };

  const getMessage = (ctx: OpenClawPluginToolContext): AnyAgentTool | null => {
    if (isSubagentContext(scopeContext(ctx))) {
      return null;
    }
    return {
      name: "pond_get_message",
      label: "Pond Get Message",
      description: GET_MESSAGE_DESCRIPTION,
      parameters: GetMessageParamsSchema,
      outputSchema: ToolOutputSchema,
      execute: async (_id, rawArgs) => {
        const args = rawArgs as GetMessageParams;
        const id = typeof args.id === "string" ? args.id.trim() : "";
        if (!id) {
          return errorResult("id must not be empty");
        }
        const { scope } = resolveScopeFromContext({
          cfg: resolveConfig(ctx),
          ctx: scopeContext(ctx),
          groupSessions: deps.config.groupSessions,
        });
        if (!scope.ok) {
          return forbiddenResult(scope.error);
        }
        const pondArgs: Record<string, unknown> = { id };
        if (typeof args.context_before === "number") pondArgs.context_before = args.context_before;
        if (typeof args.context_after === "number") pondArgs.context_after = args.context_after;
        return relay(deps, "pond_get_message", pondArgs);
      },
    };
  };

  const sql = (ctx: OpenClawPluginToolContext): AnyAgentTool | null => {
    if (isSubagentContext(scopeContext(ctx))) {
      return null;
    }
    return {
      name: "pond_sql",
      label: "Pond SQL",
      description: SQL_DESCRIPTION,
      parameters: SqlParamsSchema,
      outputSchema: ToolOutputSchema,
      execute: async (_id, rawArgs) => {
        const args = rawArgs as SqlParams;
        const query = typeof args.query === "string" ? args.query.trim() : "";
        if (!query) {
          return errorResult("query must not be empty");
        }
        // pond_sql runs arbitrary read-only SELECT over the whole corpus;
        // a single substring project filter cannot clamp arbitrary SQL, so gate
        // the analytic tool on the operator's broad opt-in (visibility=all)
        // instead of silently returning unscoped rows to a narrower caller.
        const { visibility } = resolveScopeFromContext({
          cfg: resolveConfig(ctx),
          ctx: scopeContext(ctx),
          groupSessions: deps.config.groupSessions,
        });
        if (visibility !== "all") {
          return forbiddenResult(
            `pond_sql is cross-session analytics and requires tools.sessions.visibility=all; ` +
              `current effective scope is "${visibility}". Use pond_search / pond_get_session for scoped reads.`,
          );
        }
        const pondArgs: Record<string, unknown> = { query };
        if (args.format) pondArgs.format = args.format;
        if (typeof args.timeout_seconds === "number") pondArgs.timeout_seconds = args.timeout_seconds;
        return relay(deps, "pond_sql", pondArgs);
      },
    };
  };

  return { search, getSession, getMessage, sql };
}
