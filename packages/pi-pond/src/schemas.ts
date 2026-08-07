// TypeBox parameter and output schemas for the four projected pond tools.
//
// Vendored rather than imported from pond: the bounds are this extension's
// policy (what it will forward and how much text it will relay back), and pond
// re-validates every argument on its own side regardless. `additionalProperties:
// false` plus named bound constants keeps a malformed model call a validation
// error here instead of a confusing pond error two hops away.
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

// Mirrors pond's split get surface: intent lives in the tool name, each takes
// one required `id` (get_session also accepts a message id server-side - it
// resolves up to the parent session).
export const GetSessionParamsSchema = Type.Object(
  {
    id: Type.String({ maxLength: 512 }),
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 1000 })),
    from: Type.Optional(Type.Union([Type.Literal("start"), Type.Literal("end")])),
    after_message_id: Type.Optional(Type.String({ maxLength: 512 })),
    before_message_id: Type.Optional(Type.String({ maxLength: 512 })),
  },
  additional,
);

export const GetMessageParamsSchema = Type.Object(
  {
    id: Type.String({ maxLength: 512 }),
    context_before: Type.Optional(Type.Integer({ minimum: 0, maximum: 25 })),
    context_after: Type.Optional(Type.Integer({ minimum: 0, maximum: 25 })),
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
  Type.Object({ status: Type.Literal("error"), error: Type.String() }, additional),
]);

export type SearchParams = Static<typeof SearchParamsSchema>;
export type GetSessionParams = Static<typeof GetSessionParamsSchema>;
export type GetMessageParams = Static<typeof GetMessageParamsSchema>;
export type SqlParams = Static<typeof SqlParamsSchema>;
export type ToolOutput = Static<typeof ToolOutputSchema>;
