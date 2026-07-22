// Managed-mode supervisor: locate the pond binary, spawn
// `pond serve --transport stdio --with-sync`, speak MCP over its stdio, and
// restart with backoff on unexpected exit. In url mode it instead dials an
// external `pond serve` over streamable HTTP. Either way it exposes one
// callTool seam the tools use. The plugin NEVER writes pond config.
import { accessSync, constants as fsConstants } from "node:fs";
import { delimiter, join } from "node:path";
import type { PondPluginConfig } from "./config.js";
import {
  createHttpTransport,
  createStdioTransport,
  type PondCallResult,
  PondMcpClient,
} from "./mcp.js";

export type PondLogger = {
  info: (message: string) => void;
  warn: (message: string) => void;
  error: (message: string) => void;
};

const RESTART_BASE_DELAY_MS = 500;
const RESTART_MAX_DELAY_MS = 30_000;

// The MCP SDK's stdio transport passes the child a fixed safelist (HOME, PATH,
// USER, ...) when no env is given, silently dropping XDG_* and POND_* - a store
// relocated via XDG vars would open as a different, empty store. Build the
// child env explicitly: the SDK's safelist plus pond's own knobs.
const CHILD_ENV_VARS = [
  "HOME",
  "LOGNAME",
  "PATH",
  "SHELL",
  "TERM",
  "USER",
  "XDG_CONFIG_HOME",
  "XDG_DATA_HOME",
  "XDG_STATE_HOME",
  "RUST_LOG",
];

export function pondChildEnv(source: NodeJS.ProcessEnv = process.env): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(source)) {
    if (value !== undefined && (CHILD_ENV_VARS.includes(key) || key.startsWith("POND_"))) {
      env[key] = value;
    }
  }
  return env;
}

// The one production mapping from a thrown MCP call (JSON-RPC error envelopes:
// not_found, validation_failed, the get_message wrong-id hint) to the typed
// {ok:false} the tools relay. Exported so tests exercise this exact seam.
export async function relayPondCall(
  client: PondMcpClient,
  name: string,
  args: Record<string, unknown>,
): Promise<PondCallResult> {
  try {
    return await client.callTool(name, args);
  } catch (error) {
    return { ok: false, error: `pond call failed: ${String(error instanceof Error ? error.message : error)}` };
  }
}

const INSTALL_HINT =
  "install pond (https://github.com/tenequm/pond, `brew install tenequm/tap/pond` or `cargo install pond`), " +
  "then run `pond init` once to create the store and enable the openclaw adapter.";

function isExecutable(path: string): boolean {
  try {
    accessSync(path, fsConstants.X_OK);
    return true;
  } catch {
    return false;
  }
}

// Resolve the pond binary the way a shell would, so a missing install fails with
// a named fix rather than an opaque spawn ENOENT deep inside the transport.
export function resolvePondBinary(binaryPath: string | undefined): string {
  if (binaryPath) {
    if (!isExecutable(binaryPath)) {
      throw new Error(`configured pond.binaryPath is not an executable file: ${binaryPath}. ${INSTALL_HINT}`);
    }
    return binaryPath;
  }
  const pathEnv = process.env.PATH ?? "";
  for (const dir of pathEnv.split(delimiter)) {
    if (!dir) {
      continue;
    }
    const candidate = join(dir, "pond");
    if (isExecutable(candidate)) {
      return candidate;
    }
  }
  throw new Error(`pond binary not found on PATH. ${INSTALL_HINT}`);
}

export class PondController {
  private client = new PondMcpClient();
  private stopped = true;
  private attempt = 0;
  private restartTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly config: PondPluginConfig,
    private readonly logger: PondLogger,
  ) {}

  async start(): Promise<void> {
    this.stopped = false;
    await this.dial();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = undefined;
    }
    await this.client.close();
  }

  async callTool(name: string, args: Record<string, unknown>): Promise<PondCallResult> {
    if (!this.client.connected) {
      return {
        ok: false,
        error: "pond is not connected; the pond service is starting or unavailable. Check the gateway logs.",
      };
    }
    return relayPondCall(this.client, name, args);
  }

  private async dial(): Promise<void> {
    if (this.stopped) {
      return;
    }
    try {
      const transport =
        this.config.mode === "url" ? this.dialHttp() : this.dialStdio();
      await this.client.connect(transport, { onClose: () => this.handleDisconnect() });
      // Probe the handshake so a dead transport fails here, not on first tool
      // call. An uninitialized store is NOT caught by this: `pond serve` starts
      // and lists tools regardless; its own sync WARN names the `pond init` fix.
      await this.client.listToolNames();
      this.attempt = 0;
      this.logger.info(`pond connected (${this.config.mode} mode).`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (this.config.mode === "managed") {
        this.logger.error(
          `pond failed to start: ${message}. If the store is not initialized, run \`pond init\`. Otherwise ${INSTALL_HINT}`,
        );
      } else {
        this.logger.error(`pond connection to ${this.config.url ?? "(no url)"} failed: ${message}`);
      }
      this.scheduleRestart();
    }
  }

  private dialStdio() {
    const command = resolvePondBinary(this.config.binaryPath);
    return createStdioTransport({
      command,
      args: [
        "serve",
        "--transport",
        "stdio",
        "--with-sync",
        "--sync-every",
        String(this.config.syncIntervalMinutes),
      ],
      env: pondChildEnv(),
    });
  }

  private dialHttp() {
    if (!this.config.url) {
      throw new Error("pond.mode=url requires pond.url to be set");
    }
    return createHttpTransport({ url: this.config.url, headers: this.config.headers });
  }

  private handleDisconnect(): void {
    if (this.stopped) {
      return;
    }
    this.logger.warn("pond connection closed; scheduling reconnect.");
    void this.client.close();
    this.scheduleRestart();
  }

  private scheduleRestart(): void {
    if (this.stopped || this.restartTimer) {
      return;
    }
    this.attempt += 1;
    const delay = Math.min(RESTART_MAX_DELAY_MS, RESTART_BASE_DELAY_MS * 2 ** (this.attempt - 1));
    this.restartTimer = setTimeout(() => {
      this.restartTimer = undefined;
      void this.dial();
    }, delay);
    this.restartTimer.unref?.();
  }
}
