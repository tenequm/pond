// Managed-mode supervisor: locate the pond binary, spawn `pond serve
// --transport stdio --with-sync` (plus `--bootstrap`, unless capture was
// declined), speak MCP over its stdio, and restart with backoff on unexpected
// exit. In url mode it instead dials an external `pond serve` over streamable
// HTTP. Either way it exposes one callTool seam the tools use.
//
// The child starts LAZILY, on the first tool call - not in the extension
// factory and not on `session_start`. Orchestrator extensions spawn headless
// child pi sessions that load every ambient extension; with lazy start a child
// that never touches pond costs one loaded module and zero processes, and a
// child that DOES call pond_search gets a pond legitimately serving recall.
// That is why there is no subagent detection and no env sniffing here: every
// session gets the same extension behaving the same way, and pond's own
// per-host sync flock plus `--no-wait` make overlapping loops safe.
import { accessSync, constants as fsConstants } from "node:fs";
import { delimiter, join } from "node:path";
import type { PondConfig } from "./config.ts";
import {
  createHttpTransport,
  createStdioTransport,
  type PondCallResult,
  PondMcpClient,
} from "./mcp.ts";

export type PondLogger = {
  warn: (message: string) => void;
  error: (message: string) => void;
};

/**
 * What the footer should say. Reported instead of inferred from log levels so
 * the status line cannot drift from what the controller is actually doing.
 * `idle` is the honest state before the lazy first dial: pond is installed and
 * nothing has needed it yet.
 */
export type PondState = "idle" | "connected" | "reconnecting" | "unavailable";

const RESTART_BASE_DELAY_MS = 500;
const RESTART_MAX_DELAY_MS = 30_000;
// Local-sidecar deadline for the initialize handshake and the tool-list probe;
// the SDK default of 60s would let a hung child stall the first tool call.
const DIAL_TIMEOUT_MS = 10_000;

// The MCP SDK's stdio transport passes the child a fixed safelist (HOME, PATH,
// USER, ...) when no env is given, silently dropping XDG_* and POND_* - a store
// relocated via XDG vars would open as a different, empty store. Build the
// child env explicitly: the SDK's safelist plus pond's own knobs.
//
// Twin of CHILD_ENV_VARS in packages/openclaw-pond/src/service.ts: a var added
// or dropped here must be mirrored there (and back) until the shared package is
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

/**
 * The `pond serve` argv of the managed child. Split out of the transport so the
 * consent gate on `--bootstrap` can be asserted without spawning anything.
 */
export function pondServeArgs(config: PondConfig): string[] {
  return [
    "serve",
    "--transport",
    "stdio",
    // One child serves the read tools AND runs the periodic sync, sharing a
    // single embedding model - instead of a `pond sync` child per trigger, each
    // cold-loading its own.
    "--with-sync",
    "--sync-every",
    String(config.syncIntervalMinutes),
    // Init-equivalent, first-run only: when pond has NO adapters configured at
    // all, enable the pi adapter so the first sync ingests something. A recorded
    // decline is binding and takes it away; every other state keeps it, so a
    // headless-only install (never asked, because a prompt needs a UI) still
    // captures out of the box.
    ...(config.captureConsent === "declined" ? [] : ["--bootstrap", "pi-coding-agent"]),
  ];
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
    return {
      ok: false,
      error: `pond call failed: ${String(error instanceof Error ? error.message : error)}`,
    };
  }
}

export const INSTALL_HINT =
  "install pond (https://github.com/tenequm/pond, `brew install tenequm/tap/pond` or " +
  "`cargo install pond`). Nothing else is needed - this extension bootstraps the " +
  "pi-coding-agent adapter itself; `pond init` is only for a cross-harness corpus.";

function isExecutable(path: string): boolean {
  try {
    accessSync(path, fsConstants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/**
 * Resolve the pond binary the way a shell would, so a missing install fails
 * with a named fix rather than an opaque spawn ENOENT deep inside the transport.
 */
export function resolvePondBinary(binaryPath: string | undefined): string {
  if (binaryPath) {
    if (!isExecutable(binaryPath)) {
      throw new Error(
        `configured pond binaryPath is not an executable file: ${binaryPath}. ${INSTALL_HINT}`,
      );
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

/**
 * Where pond is, or why it could not be found. Gates tool registration and the
 * consent prompt. The reason is carried rather than collapsed to "not found":
 * a configured-but-wrong `binaryPath` needs to say so, not send the operator
 * off to reinstall something they already have.
 */
export function pondBinaryPath(
  binaryPath: string | undefined,
): { path: string } | { error: string } {
  try {
    return { path: resolvePondBinary(binaryPath) };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

export class PondController {
  private client = new PondMcpClient();
  private stopped = false;
  private attempt = 0;
  private restartTimer: ReturnType<typeof setTimeout> | undefined;
  private starting: Promise<void> | undefined;

  constructor(
    private readonly config: PondConfig,
    private readonly logger: PondLogger,
    private readonly onState: (state: PondState) => void = () => {},
  ) {}

  /**
   * Idempotent and concurrency-safe: several tool calls landing at once share
   * one dial rather than racing three children into existence.
   */
  async ensureStarted(): Promise<void> {
    if (this.stopped || this.client.connected) {
      return;
    }
    this.starting ??= this.dial().finally(() => {
      this.starting = undefined;
    });
    await this.starting;
  }

  // Idempotent: `session_shutdown` may fire more than once across a session
  // replacement. Child teardown is the SDK stdio transport's close() ladder -
  // stdin end, 2s wait, SIGTERM, 2s wait, SIGKILL. Signalling the direct child
  // suffices: `pond serve` is a single Rust process that spawns no descendants
  // (and `nice` execs in place, same pid).
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = undefined;
    }
    // A dial in flight has already spawned the child but has not yet assigned
    // the client, so closing now would be a no-op and the child would outlive
    // the session. Let it finish (it re-checks `stopped` and closes itself),
    // then close whatever it left.
    await this.starting?.catch(() => {});
    await this.client.close();
  }

  async callTool(name: string, args: Record<string, unknown>): Promise<PondCallResult> {
    await this.ensureStarted();
    if (!this.client.connected) {
      return {
        ok: false,
        error:
          "pond is not reachable. If it is not installed yet: " +
          INSTALL_HINT +
          " Otherwise check `pond status`.",
      };
    }
    return relayPondCall(this.client, name, args);
  }

  private async dial(): Promise<void> {
    if (this.stopped) {
      return;
    }
    try {
      const transport = this.config.mode === "url" ? this.dialHttp() : this.dialStdio();
      await this.client.connect(transport, {
        onClose: () => this.handleDisconnect(),
        timeoutMs: DIAL_TIMEOUT_MS,
      });
      // Probe the handshake so a dead transport fails here, not on first tool
      // call. A source-less pond is NOT a dial failure: `pond serve` starts and
      // lists tools regardless; bootstrap/sync WARN logs name the fix.
      // The session can end during the dial; the child exists by now, so this
      // is the point that must notice and reap it.
      if (this.stopped) {
        await this.client.close().catch(() => {});
        return;
      }
      await this.client.listToolNames({ timeoutMs: DIAL_TIMEOUT_MS });
      this.attempt = 0;
      this.onState("connected");
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
      this.onState("unavailable");
      this.scheduleRestart();
    }
  }

  private dialStdio() {
    // Resolve first so a missing install still fails with the named fix, then
    // wrap in `nice -n 19` (posix): background sync must never compete with
    // interactive work. `nice` execs pond in place - the child pid IS pond, so
    // the stop() ladder signals the right process. Skipped on Windows.
    const pondBin = resolvePondBinary(this.config.binaryPath);
    const posix = process.platform !== "win32";
    const command = posix ? "nice" : pondBin;
    const prefix = posix ? ["-n", "19", pondBin] : [];
    return createStdioTransport({
      command,
      args: [...prefix, ...pondServeArgs(this.config)],
      env: pondChildEnv(),
    });
  }

  private dialHttp() {
    if (!this.config.url) {
      throw new Error('pond mode "url" requires a `url` in ~/.pi/agent/pond-pi.json');
    }
    return createHttpTransport({ url: this.config.url, headers: this.config.headers });
  }

  private handleDisconnect(): void {
    if (this.stopped) {
      return;
    }
    this.logger.warn("pond connection closed; scheduling reconnect.");
    void this.client.close().catch(() => {});
    this.onState("reconnecting");
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
      // Through the same single-flight gate as a tool call: dialling directly
      // would race a call landing in the backoff window, and the second
      // connect() overwrites `client` - orphaning the first `pond serve` child
      // that stop() then never closes.
      void this.ensureStarted().catch(() => {});
    }, delay);
    this.restartTimer.unref?.();
  }
}
