// Test double for `openclaw/plugin-sdk/config-contracts`.
// Only the config shape the pond plugin reads is modeled; the real type is far
// larger. Kept structurally compatible so scope resolution typechecks and tests
// can build config fixtures.
export type OpenClawConfig = {
  tools?: {
    sessions?: { visibility?: unknown };
    agentToAgent?: { enabled?: boolean; allow?: unknown[] };
  };
  agents?: {
    defaults?: { sandbox?: { sessionToolsVisibility?: "spawned" | "all" } };
  };
  [key: string]: unknown;
};
