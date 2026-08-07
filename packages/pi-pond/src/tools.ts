// The four projected pond tools. Each forwards to pond over MCP and returns
// pond's own rendered text unmodified: pond's MCP surface is text, and its
// descriptions, scope counts, and "here is the next query to run" error
// messages are the point - rewriting them here would strip the routing that
// makes recall work. The extension only bounds the bytes.
//
// No scope layer: pi is single-user, so the tools default to the WHOLE archive.
// Cross-agent recall - finding what Claude Code or Codex did last week from
// inside pi - is the product, not a leak.
import { defineTool } from "@earendil-works/pi-coding-agent";
import type { AgentToolResult } from "@earendil-works/pi-coding-agent";
import type { Static, TSchema } from "typebox";
import type { PondCallResult } from "./mcp.ts";
import {
  RESPONSE_MAX_BYTES,
  SEARCH_DEFAULT_LIMIT,
  SEARCH_MAX_LIMIT,
  SearchParamsSchema,
  GetSessionParamsSchema,
  GetMessageParamsSchema,
  SqlParamsSchema,
  type ToolOutput,
} from "./schemas.ts";

// Single source for the projected tool names: the registered name and the
// relayed pond tool name are the same string on purpose, so a hit an agent sees
// in one surface names the tool it should call in the other.
export const POND_TOOL_NAMES = {
  search: "pond_search",
  getSession: "pond_get_session",
  getMessage: "pond_get_message",
  sql: "pond_sql",
} as const;

export type PondCaller = (name: string, args: Record<string, unknown>) => Promise<PondCallResult>;

function result(details: ToolOutput, text: string): AgentToolResult<ToolOutput> {
  return { content: [{ type: "text", text }], details };
}

function okResult(text: string): AgentToolResult<ToolOutput> {
  return result({ status: "ok", text }, text);
}

function errorResult(error: string): AgentToolResult<ToolOutput> {
  return result({ status: "error", error }, error);
}

function boundedText(raw: string): string {
  // Measure without copying: pond's untruncated text can be far larger than the
  // budget, and the copy is only needed on the rare over-budget path.
  if (Buffer.byteLength(raw, "utf8") <= RESPONSE_MAX_BYTES) {
    return raw;
  }
  const clipped = Buffer.from(raw, "utf8").subarray(0, RESPONSE_MAX_BYTES).toString("utf8");
  return `${clipped}\n\n[pond: response truncated to ${RESPONSE_MAX_BYTES} bytes; narrow the query or lower limit]`;
}

async function relay(
  callPond: PondCaller,
  name: string,
  args: Record<string, unknown>,
): Promise<AgentToolResult<ToolOutput>> {
  const response = await callPond(name, args);
  return response.ok ? okResult(boundedText(response.text)) : errorResult(response.error);
}

const SEARCH_DESCRIPTION =
  "Find relevant messages in the durable pond archive of past agent sessions - every harness on " +
  'every machine, not just pi. Returns pond\'s rendered transcript: results grouped by session, best first. Pick `mode`: "vector" ' +
  '(default, meaning) or "fts" (exact words, BM25). Pass a hit\'s session_id to pond_get_session or ' +
  "its message_id to pond_get_message.";

const GET_SESSION_DESCRIPTION =
  "Read a whole past session from pond as a chronological transcript - the tool for analyzing, " +
  "reviewing, or summarizing a session. Pass an id from pond_search (a message_id also works: it " +
  'resolves to its parent session anchored at that message). from="end" reads the most recent turns; ' +
  "after_message_id / before_message_id page on from a page marker.";

const GET_MESSAGE_DESCRIPTION =
  "Expand one pond message with its full part bodies (tool_call / tool_result / reasoning) plus " +
  "conversational neighbors; context_before / context_after size the window (like grep -B/-A). Pass a " +
  "message_id from pond_search; for the whole session use pond_get_session.";

const SQL_DESCRIPTION =
  "Advanced escape hatch: run ONE read-only SQL SELECT over pond's corpus as three tables (sessions, " +
  "messages, parts) for what search and get cannot express - corpus-wide counts, group-by, joins, " +
  "exact strings inside tool bodies. Use pond_search / pond_get_session for ordinary reads.";

export const POND_PROMPT_SNIPPET =
  "Search and read the archive of past agent sessions (Claude Code, Codex, OpenClaw, pi - all machines)";

// Flat bullets appended to the system prompt's Guidelines section, so each one
// must name its own tool - the model cannot tell what "this tool" refers to.
export const POND_PROMPT_GUIDELINES = [
  "Use pond_search when the user references past work, a prior session, or a decision made earlier - it searches sessions from every agent and machine, not just this one.",
  "Use pond_get_session to read a whole past session found via pond_search; pass from=\"end\" when the question is about the latest state.",
  "Use pond_sql only for corpus-wide aggregation or exact-string lookups inside tool bodies that pond_search cannot express.",
];

function text(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

/**
 * The four tool definitions, bound to one `callPond` seam. Registered only when
 * a pond binary is actually reachable: a tool that always errors is worse than
 * no tool.
 */
export function createPondTools(callPond: PondCaller) {
  // `defineTool` exists to preserve parameter inference through the tool
  // literal, so `toArgs` receives the schema's own Static type with no cast.
  const define = <TParams extends TSchema>(
    name: string,
    label: string,
    description: string,
    parameters: TParams,
    toArgs: (params: Static<TParams>) => Record<string, unknown> | string,
  ) =>
    defineTool<TParams, ToolOutput>({
      name,
      label,
      description,
      promptSnippet: POND_PROMPT_SNIPPET,
      promptGuidelines: POND_PROMPT_GUIDELINES,
      parameters,
      async execute(_toolCallId, params, _signal, _onUpdate, _ctx) {
        const args = toArgs(params);
        return typeof args === "string" ? errorResult(args) : relay(callPond, name, args);
      },
    });

  return [
    define(
      POND_TOOL_NAMES.search,
      "Pond Search",
      SEARCH_DESCRIPTION,
      SearchParamsSchema,
      (params) => {
        const query = text(params.query);
        if (!query) {
          return "query must not be empty";
        }
        const args: Record<string, unknown> = {
          query,
          limit: Math.min(SEARCH_MAX_LIMIT, Math.max(1, params.limit ?? SEARCH_DEFAULT_LIMIT)),
        };
        for (const key of ["mode", "sort_by", "project", "session_id", "from_date", "to_date"] as const) {
          const value = params[key];
          if (typeof value === "string" && value.length > 0) {
            args[key] = value;
          }
        }
        return args;
      },
    ),
    define(
      POND_TOOL_NAMES.getSession,
      "Pond Get Session",
      GET_SESSION_DESCRIPTION,
      GetSessionParamsSchema,
      (params) => {
        const id = text(params.id);
        if (!id) {
          return "id must not be empty";
        }
        const args: Record<string, unknown> = { id };
        if (params.from) {
          args.from = params.from;
        }
        if (typeof params.limit === "number") {
          args.limit = params.limit;
        }
        for (const key of ["after_message_id", "before_message_id"] as const) {
          const value = params[key];
          if (typeof value === "string" && value.length > 0) {
            args[key] = value;
          }
        }
        return args;
      },
    ),
    define(
      POND_TOOL_NAMES.getMessage,
      "Pond Get Message",
      GET_MESSAGE_DESCRIPTION,
      GetMessageParamsSchema,
      (params) => {
        const id = text(params.id);
        if (!id) {
          return "id must not be empty";
        }
        const args: Record<string, unknown> = { id };
        for (const key of ["context_before", "context_after"] as const) {
          const value = params[key];
          if (typeof value === "number") {
            args[key] = value;
          }
        }
        return args;
      },
    ),
    define(
      POND_TOOL_NAMES.sql,
      "Pond SQL",
      SQL_DESCRIPTION,
      SqlParamsSchema,
      (params) => {
        const query = text(params.query);
        if (!query) {
          return "query must not be empty";
        }
        const args: Record<string, unknown> = { query };
        if (params.format) {
          args.format = params.format;
        }
        if (typeof params.timeout_seconds === "number") {
          args.timeout_seconds = params.timeout_seconds;
        }
        return args;
      },
    ),
  ];
}
