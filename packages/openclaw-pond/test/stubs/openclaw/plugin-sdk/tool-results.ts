// Test double for `openclaw/plugin-sdk/tool-results`. Mirrors the real
// AgentToolResult<T>: generic, `details` required (agent-core types.ts).
export type AgentToolResult<T> = {
  content: Array<{ type: "text"; text: string }>;
  details: T;
  progress?: unknown;
  terminate?: boolean;
};
