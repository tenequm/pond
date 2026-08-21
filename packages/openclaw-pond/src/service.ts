// Managed-mode supervisor: locate the pond binary, spawn
// `pond serve --transport stdio --with-sync`, speak MCP over its stdio, and
// restart with backoff on unexpected exit. In url mode it instead dials an
// external `pond serve` over streamable HTTP. Either way it exposes one
// callTool seam the tools use. The plugin never touches an existing pond
// config; on a completely unconfigured pond, `--bootstrap openclaw` enables
// that one adapter (equivalent to a minimal `pond init`).
import { execFile } from "node:child_process";
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
// Local-sidecar deadline for the initialize handshake and the tool-list probe
// (OpenClaw's house pattern probes local children with a 10s deadline); the
// SDK default of 60s would let a hung child stall gateway startup.
const DIAL_TIMEOUT_MS = 10_000;
const VERSION_PROBE_TIMEOUT_MS = 5_000;

// The MCP SDK's stdio transport passes the child a fixed safelist (HOME, PATH,
// USER, ...) when no env is given, silently dropping XDG_* and POND_* - a store
// relocated via XDG vars would open as a different, empty store. Build the
// child env explicitly: the SDK's safelist plus pond's own knobs.
//
// Twin of CHILD_ENV_VARS in packages/pi-pond/src/service.ts: a var added or
// dropped here must be mirrored there (and back) until the shared package is
// extracted - the failure mode of a drift is a silently relocated store.
const CHILD_ENV_VARS = [
  "HOME",
  "LOGNAME",
  "PATH",
  "SHELL",
  "TERM",
  "USER",
  "XDG_CACHE_HOME",
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
  "install pond (https://github.com/tenequm/pond, `brew install tenequm/tap/pond` or `cargo install pond`). " +
  "Nothing else is needed - the plugin bootstraps the openclaw adapter itself; `pond init` is only " +
  "for a cross-harness corpus.";

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

// Which pond this gateway is actually talking to. The tool descriptions are
// version-neutral by design, so this line is the only place a behaviour
// difference (default search arm, embeddings) traces back to a binary.
// Fire-and-forget: the dial never waits on it and never fails because of it.
function logPondVersion(pondBin: string, logger: PondLogger): void {
  execFile(
    pondBin,
    ["--version"],
    { timeout: VERSION_PROBE_TIMEOUT_MS },
    (error, stdout, stderr) => {
      if (error) {
        logger.warn(`could not read \`${pondBin} --version\`: ${error.message}`);
        return;
      }
      logger.info(`${pondBin}: ${(stdout || stderr).trim()}`);
    },
  );
}

export class PondController {
  private client = new PondMcpClient();
  private stopped = true;
  private attempt = 0;
  private restartTimer: ReturnType<typeof setTimeout> | undefined;
  private versionLogged = false;

  constructor(
    private readonly config: PondPluginConfig,
    private readonly logger: PondLogger,
  ) {}

  async start(): Promise<void> {
    this.stopped = false;
    await this.dial();
  }

  // Idempotent: both the service stop and the runtime-lifecycle cleanup route
  // here, and core may fire both on shutdown. Child teardown is the SDK stdio
  // transport's close() ladder - stdin end, 2s wait, SIGTERM, 2s wait, SIGKILL.
  // Signalling the direct child suffices: `pond serve` is a single Rust process
  // that spawns no descendants (and `nice` execs in place, same pid), so a
  // core-style process-tree kill would enumerate exactly this one pid.
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
      await this.client.connect(transport, {
        onClose: () => this.handleDisconnect(),
        timeoutMs: DIAL_TIMEOUT_MS,
      });
      // Probe the handshake so a dead transport fails here, not on first tool
      // call. A source-less pond is NOT a dial failure: `pond serve` starts and
      // lists tools regardless; bootstrap/sync WARN logs name the fix.
      await this.client.listToolNames({ timeoutMs: DIAL_TIMEOUT_MS });
      this.attempt = 0;
      this.logger.info(`pond connected (${this.config.mode} mode).`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (this.config.mode === "managed") {
        this.logger.error(`pond failed to start: ${message}. To fix: ${INSTALL_HINT}`);
      } else {
        this.logger.error(`pond connection to ${this.config.url ?? "(no url)"} failed: ${message}`);
      }
      // Covers the probe-timeout case: the client connected but the child is
      // unresponsive - tear it down before the backoff spawns a replacement.
      await this.client.close().catch(() => {});
      this.scheduleRestart();
    }
  }

  private dialStdio() {
    // Resolve first so a missing install still fails with the named fix, then
    // wrap in `nice -n 19` (posix): background sync must never compete with
    // interactive work. `nice` execs pond in place - the child pid IS pond, so
    // the stop() ladder signals the right process. Skipped on Windows.
    const pondBin = resolvePondBinary(this.config.binaryPath);
    if (!this.versionLogged) {
      this.versionLogged = true;
      logPondVersion(pondBin, this.logger);
    }
    const posix = process.platform !== "win32";
    const command = posix ? "nice" : pondBin;
    const prefix = posix ? ["-n", "19", pondBin] : [];
    return createStdioTransport({
      command,
      args: [
        ...prefix,
        "serve",
        "--transport",
        "stdio",
        "--with-sync",
        "--sync-every",
        String(this.config.syncIntervalMinutes),
        // Init-equivalent, first-run only: when pond has NO adapters
        // configured, enable the openclaw adapter so the first sync ingests
        // something. Never touches an existing pond config.
        "--bootstrap",
        "openclaw",
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
