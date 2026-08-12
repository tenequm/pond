import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { describe, expect, it } from "vitest";
import { parsePluginConfig } from "../src/config.js";
import { PondMcpClient } from "../src/mcp.js";
import { PondController, pondChildEnv, type PondLogger } from "../src/service.js";

function collectingLogger(): { logger: PondLogger; errors: string[] } {
  const errors: string[] = [];
  return {
    errors,
    logger: {
      info: () => {},
      warn: () => {},
      error: (message) => errors.push(message),
    },
  };
}

// A transport that completes the MCP initialize handshake and then swallows
// every request, modeling a child that spawned but hung.
function hangingTransport(opts: { completeHandshake: boolean; onClose?: () => void }): Transport {
  const transport: Transport & { onmessage?: (message: JSONRPCMessage) => void } = {
    async start() {},
    async send(message: JSONRPCMessage) {
      const request = message as { id?: number | string; method?: string };
      if (!opts.completeHandshake || request.method !== "initialize" || request.id === undefined) {
        return;
      }
      queueMicrotask(() => {
        transport.onmessage?.({
          jsonrpc: "2.0",
          id: request.id!,
          result: {
            protocolVersion: "2024-11-05",
            capabilities: {},
            serverInfo: { name: "hung-pond", version: "0.0.0" },
          },
        } as JSONRPCMessage);
      });
    },
    async close() {
      opts.onClose?.();
    },
  };
  return transport;
}

describe("PondController.stop", () => {
  it("is idempotent: resolves when never started and when called twice", async () => {
    const controller = new PondController(parsePluginConfig(undefined), collectingLogger().logger);
    await controller.stop();
    await controller.stop();
  });

  it("clears a pending restart and stops cleanly after a failed dial", async () => {
    const { logger, errors } = collectingLogger();
    // url mode without a url fails the dial synchronously and schedules a
    // backoff restart - stop() must clear that timer and resolve, twice.
    const controller = new PondController(
      parsePluginConfig({ pond: { mode: "url" } }),
      logger,
    );
    await controller.start();
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("pond.url");
    await controller.stop();
    await controller.stop();
  });

  it("reports a typed not-connected error for tool calls after stop", async () => {
    const controller = new PondController(parsePluginConfig(undefined), collectingLogger().logger);
    await controller.stop();
    const out = await controller.callTool("pond_search", { query: "q" });
    expect(out.ok).toBe(false);
  });
});

describe("child env", () => {
  it("forwards the XDG and POND_ knobs the SDK safelist would drop", () => {
    const env = pondChildEnv({
      HOME: "/home/u",
      PATH: "/usr/bin",
      XDG_DATA_HOME: "/home/u/data",
      // pond's own default_cache_dir() reads it; dropping it sends the child to
      // a different cache than every other pond on this machine.
      XDG_CACHE_HOME: "/home/u/cache",
      XDG_CONFIG_HOME: "/home/u/config",
      XDG_STATE_HOME: "/home/u/state",
      POND_STORAGE_PATH: "s3://bucket/pond",
      UNRELATED: "no",
    });
    expect(env).toEqual({
      HOME: "/home/u",
      PATH: "/usr/bin",
      XDG_DATA_HOME: "/home/u/data",
      XDG_CACHE_HOME: "/home/u/cache",
      XDG_CONFIG_HOME: "/home/u/config",
      XDG_STATE_HOME: "/home/u/state",
      POND_STORAGE_PATH: "s3://bucket/pond",
    });
  });
});

describe("dial deadlines", () => {
  it("fails the initialize handshake within the deadline against a silent child", async () => {
    const client = new PondMcpClient();
    let closed = false;
    const startedAt = Date.now();
    await expect(
      client.connect(hangingTransport({ completeHandshake: false, onClose: () => (closed = true) }), {
        timeoutMs: 100,
      }),
    ).rejects.toThrow(/timeout|timed out/i);
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    // The failed dial must tear the transport down, or a spawned-but-hung
    // child would survive every restart attempt.
    expect(closed).toBe(true);
  });

  it("fails the tool-list probe within the deadline when the child hangs post-handshake", async () => {
    const client = new PondMcpClient();
    await client.connect(hangingTransport({ completeHandshake: true }), { timeoutMs: 1_000 });
    const startedAt = Date.now();
    await expect(client.listToolNames({ timeoutMs: 100 })).rejects.toThrow(/timeout|timed out/i);
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    await client.close();
  });
});
