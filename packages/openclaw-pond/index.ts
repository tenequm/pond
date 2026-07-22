// openclaw-pond: projects pond's read-only recall tools into OpenClaw agents and
// (managed mode) supervises a local `pond serve` process. Tools only - no memory
// slot, no auto-recall, no prompt hooks (see README for the positioning).
import { definePluginEntry, type OpenClawPluginApi } from "openclaw/plugin-sdk/plugin-entry";
import { parsePluginConfig } from "./src/config.js";
import { PondController } from "./src/service.js";
import { createPondToolFactories } from "./src/tools.js";

const TOOL_NAMES = ["pond_search", "pond_get", "pond_sql_query"] as const;

function register(api: OpenClawPluginApi): void {
  // Tool-discovery loads only for ownership metadata; register inert stubs so
  // the runtime is not eagerly constructed (mirrors clickclack's pattern).
  if (api.registrationMode === "tool-discovery") {
    for (const name of TOOL_NAMES) {
      api.registerTool(() => null, { name });
    }
    return;
  }

  const config = parsePluginConfig(api.pluginConfig);
  const logger = {
    info: (message: string) => console.info(`[pond] ${message}`),
    warn: (message: string) => console.warn(`[pond] ${message}`),
    error: (message: string) => console.error(`[pond] ${message}`),
  };
  const controller = new PondController(config, logger);

  const factories = createPondToolFactories({
    config,
    logger,
    callPond: (name, args) => controller.callTool(name, args),
  });
  api.registerTool(factories.search, { name: "pond_search" });
  api.registerTool(factories.get, { name: "pond_get" });
  api.registerTool(factories.sql, { name: "pond_sql_query" });

  api.registerService({
    id: "pond",
    start: () => controller.start(),
    stop: () => controller.stop(),
  });

  api.lifecycle.registerRuntimeLifecycle({
    id: "pond",
    description: "Stops the managed pond process / MCP connection.",
    cleanup: () => controller.stop(),
  });
}

export default definePluginEntry({
  id: "pond",
  name: "Pond",
  description: "Durable, lossless recall over past agent sessions via read-only pond tools.",
  register,
});
