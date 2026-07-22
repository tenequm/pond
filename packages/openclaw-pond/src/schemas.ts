// TypeBox parameter and output schemas for the three projected pond tools.
//
// Contract mirrors OpenClaw core's sessions_search (src/agents/tools/
// sessions-search-tool.ts): additionalProperties:false, named bound constants,
// and a typed output union carrying {status:"forbidden"|"error"}. Parameter
// schemas stay GBNF-safe (no oneOf/format/patternProperties) so they compile
// under llama.cpp grammar constraints (openclaw #108580); see test/gbnf.ts.
import { type Static, Type } from "typebox";

export const SEARCH_DEFAULT_LIMIT = 10;
export const SEARCH_MAX_LIMIT = 25;
export const SEARCH_MAX_QUERY_CHARS = 4096;
export const SQL_MAX_QUERY_CHARS = 8192;
// Whole relayed transcript budget (pond renders text, not per-hit structs).
export const RESPONSE_MAX_BYTES = 32 * 1024;
export const SQL_MAX_TIMEOUT_SECONDS = 600;

const additional = { additionalProperties: false } as const;

export const SearchParamsSchema = Type.Object(
  {
    query: Type.String({ maxLength: SEARCH_MAX_QUERY_CHARS }),
    mode: Type.Optional(Type.Union([Type.Literal("vector"), Type.Literal("fts")])),
    sort_by: Type.Optional(Type.Union([Type.Literal("relevance"), Type.Literal("recency")])),
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: SEARCH_MAX_LIMIT })),
    project: Type.Optional(Type.String({ maxLength: 512 })),
    session_id: Type.Optional(Type.String({ maxLength: 512 })),
    from_date: Type.Optional(Type.String({ maxLength: 32 })),
    to_date: Type.Optional(Type.String({ maxLength: 32 })),
  },
  additional,
);

// session_id and message_id are mutually exclusive, validated at runtime rather
// than via a top-level oneOf that GBNF cannot express.
export const GetParamsSchema = Type.Object(
  {
    session_id: Type.Optional(Type.String({ maxLength: 512 })),
    message_id: Type.Optional(Type.String({ maxLength: 512 })),
    session_limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 1000 })),
    session_from: Type.Optional(Type.Union([Type.Literal("start"), Type.Literal("end")])),
    session_after_message_id: Type.Optional(Type.String({ maxLength: 512 })),
    session_before_message_id: Type.Optional(Type.String({ maxLength: 512 })),
    message_context_before: Type.Optional(Type.Integer({ minimum: 0, maximum: 25 })),
    message_context_after: Type.Optional(Type.Integer({ minimum: 0, maximum: 25 })),
  },
  additional,
);

export const SqlParamsSchema = Type.Object(
  {
    query: Type.String({ maxLength: SQL_MAX_QUERY_CHARS }),
    format: Type.Optional(
      Type.Union([Type.Literal("text"), Type.Literal("parquet"), Type.Literal("ndjson")]),
    ),
    timeout_seconds: Type.Optional(
      Type.Integer({ minimum: 1, maximum: SQL_MAX_TIMEOUT_SECONDS }),
    ),
  },
  additional,
);

export const ToolOutputSchema = Type.Union([
  Type.Object({ status: Type.Literal("ok"), text: Type.String() }, additional),
  Type.Object(
    {
      status: Type.Union([Type.Literal("forbidden"), Type.Literal("error")]),
      error: Type.String(),
    },
    additional,
  ),
]);

export type SearchParams = Static<typeof SearchParamsSchema>;
export type GetParams = Static<typeof GetParamsSchema>;
export type SqlParams = Static<typeof SqlParamsSchema>;
