import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const stub = (name: string) =>
  fileURLToPath(new URL(`./test/stubs/openclaw/plugin-sdk/${name}.ts`, import.meta.url));

// The `openclaw` package is a peer dependency, absent in this checkout. Tests
// resolve the handful of SDK subpaths the plugin uses to faithful local doubles
// so `npm test` runs without the OpenClaw monorepo installed.
export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
  },
  resolve: {
    alias: {
      "openclaw/plugin-sdk/plugin-entry": stub("plugin-entry"),
      "openclaw/plugin-sdk/session-visibility": stub("session-visibility"),
      "openclaw/plugin-sdk/logging-core": stub("logging-core"),
      "openclaw/plugin-sdk/config-contracts": stub("config-contracts"),
    },
  },
});
