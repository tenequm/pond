// Test double for `openclaw/plugin-sdk/plugin-entry`.
// Models only the registration surface the pond plugin touches. The real SDK
// supplies richer types and the live registrar at runtime; this double lets the
// entry file typecheck and (if imported) run in tests without OpenClaw present.
import type { TSchema } from "typebox";
import type { AgentToolResult } from "../../../../src/tools.js";
import type { OpenClawConfig } from "./config-contracts.js";

// AgentToolResult deliberately does NOT re-export from here: the real
// plugin-entry never exported it (the plugin vendors it in src/tools.ts).
export type AnyAgentTool = {
  name: string;
  label: string;
  description: string;
  parameters: TSchema;
  outputSchema?: TSchema;
  execute: (
    toolCallId: string,
    args: unknown,
    signal?: AbortSignal,
    onUpdate?: unknown,
  ) => Promise<AgentToolResult<unknown>>;
};

export type OpenClawPluginToolContext = {
  config?: OpenClawConfig;
  runtimeConfig?: OpenClawConfig;
  getRuntimeConfig?: () => OpenClawConfig | undefined;
  agentId?: string;
  sessionKey?: string;
  sessionId?: string;
  sandboxed?: boolean;
  messageChannel?: string;
};

export type OpenClawPluginToolFactory = (
  ctx: OpenClawPluginToolContext,
) => AnyAgentTool | AnyAgentTool[] | null | undefined;

export type OpenClawPluginServiceContext = {
  config: OpenClawConfig;
  stateDir: string;
  logger: {
    info: (msg: string) => void;
    warn: (msg: string) => void;
    error: (msg: string) => void;
  };
};

export type OpenClawPluginService = {
  id: string;
  start: (ctx: OpenClawPluginServiceContext) => void | Promise<void>;
  stop?: (ctx: OpenClawPluginServiceContext) => void | Promise<void>;
};

export type PluginLogger = {
  debug?: (message: string) => void;
  info: (message: string) => void;
  warn: (message: string) => void;
  error: (message: string) => void;
};

export type OpenClawPluginApi = {
  registrationMode:
    | "full"
    | "discovery"
    | "tool-discovery"
    | "setup-only"
    | "setup-runtime"
    | "cli-metadata";
  pluginConfig?: Record<string, unknown>;
  logger: PluginLogger;
  registerTool: (
    factory: OpenClawPluginToolFactory | AnyAgentTool,
    options?: { name?: string; optional?: boolean },
  ) => void;
  registerService: (service: OpenClawPluginService) => void;
  lifecycle: {
    registerRuntimeLifecycle: (registration: {
      id: string;
      description?: string;
      cleanup: () => void | Promise<void>;
    }) => void;
  };
};

export type PluginEntryDefinition = {
  id: string;
  name: string;
  description: string;
  register: (api: OpenClawPluginApi) => void;
};

export function definePluginEntry(definition: PluginEntryDefinition): PluginEntryDefinition {
  return definition;
}
