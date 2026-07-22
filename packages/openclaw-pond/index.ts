// openclaw-pond: projects pond's read-only recall tools into OpenClaw agents and
// (managed mode) supervises a local `pond serve` process. Tools only - no memory
// slot, no auto-recall, no prompt hooks (see README for the positioning).
import { definePluginEntry, type OpenClawPluginApi } from "openclaw/plugin-sdk/plugin-entry";
import { parsePluginConfig } from "./src/config.js";
import { PondController } from "./src/service.js";
import { createPondToolFactories } from "./src/tools.js";

const TOOL_NAMES = ["pond_search", "pond_get_session", "pond_get_message", "pond_sql"] as const;

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
  // api.logger routes into OpenClaw's structured plugins logger.
  const logger = {
    info: (message: string) => api.logger.info(`[pond] ${message}`),
    warn: (message: string) => api.logger.warn(`[pond] ${message}`),
    error: (message: string) => api.logger.error(`[pond] ${message}`),
  };
  const controller = new PondController(config, logger);

  const factories = createPondToolFactories({
    config,
    logger,
    callPond: (name, args) => controller.callTool(name, args),
  });
  api.registerTool(factories.search, { name: "pond_search" });
  api.registerTool(factories.getSession, { name: "pond_get_session" });
  api.registerTool(factories.getMessage, { name: "pond_get_message" });
  api.registerTool(factories.sql, { name: "pond_sql" });

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
