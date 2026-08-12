// pi-pond: capture pi sessions into a durable pond archive and recall them from
// inside pi.
//
// Two halves, one process. A single managed `pond serve --transport stdio
// --with-sync` child both serves the four read-only recall tools over MCP and
// runs pond's periodic sync loop, sharing one embedding model. Tools only - no
// memory slot, no auto-recall, no prompt hooks (see README for the
// positioning); the `/pond` command adds a search picker that can resume a past
// session or paste a reference to it.
import type {
  ExtensionAPI,
  ExtensionCommandContext,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import {
  type CaptureConsent,
  loadPondConfig,
  piAgentDir,
  type PondConfig,
  recordCaptureConsent,
} from "./src/config.ts";
import { hitReference, hitLabel, parsePondHits, type PondHit } from "./src/hits.ts";
import { resumeSession } from "./src/resume.ts";
import {
  INSTALL_HINT,
  PondController,
  pondBinaryPath,
  type PondState,
} from "./src/service.ts";
import { createPondTools, POND_TOOL_NAMES } from "./src/tools.ts";

const ADAPTER = "pi-coding-agent";
const STATUS_KEY = "pond";
const ADAPTERS_TIMEOUT_MS = 15_000;

const STATUS_TEXT: Record<PondState, string> = {
  idle: "pond",
  connected: "pond ready",
  reconnecting: "pond reconnecting",
  unavailable: "pond unavailable",
};

export default function piPond(pi: ExtensionAPI): void {
  const config = loadPondConfig();
  // In url mode the binary is someone else's problem; in managed mode a missing
  // pond means no tools at all - a tool that always errors is worse AX than no
  // tool - and exactly one notify carrying the REASON (a wrong configured
  // binaryPath needs a different fix than a missing install).
  const resolved = config.mode === "url" ? { path: "pond" } : pondBinaryPath(config.binaryPath);
  const binary = "path" in resolved ? resolved.path : undefined;
  const unavailable = "error" in resolved ? resolved.error : undefined;
  // The session's UI, captured for the footer. Held only between session_start
  // and session_shutdown, which is exactly the controller's own lifetime.
  let ui: ExtensionContext["ui"] | undefined;
  const controller = new PondController(
    config,
    {
      warn: (message) => console.error(`[pond] ${message}`),
      error: (message) => console.error(`[pond] ${message}`),
    },
    (state) => ui?.setStatus(STATUS_KEY, STATUS_TEXT[state]),
  );

  if (binary) {
    for (const tool of createPondTools((name, args) => controller.callTool(name, args))) {
      pi.registerTool(tool);
    }
  }

  pi.on("session_start", async (_event, ctx) => {
    if (!binary) {
      // Fire-and-forget, once per session, and nothing else breaks: pi works
      // normally without pond.
      ctx.ui.notify(unavailable ?? `pond not found - ${INSTALL_HINT}`, "warning");
      return;
    }
    ui = ctx.ui;
    // `idle`, not `ready`: the pond child starts on the first tool call, and a
    // footer that claims a running process before there is one is a lie.
    ctx.ui.setStatus(STATUS_KEY, STATUS_TEXT.idle);
    await maybeAskForCapture(pi, ctx, binary, config);
  });

  // Idempotent, and the only place the child is reaped: no orphan pond process
  // outlives a pi session, including across /new, /resume, and /fork.
  pi.on("session_shutdown", async (_event, ctx) => {
    ctx.ui.setStatus(STATUS_KEY, undefined);
    ui = undefined;
    await controller.stop();
  });

  pi.registerCommand("pond", {
    description: "Search past agent sessions; resume one here or insert a reference to it",
    handler: async (args, ctx) => {
      if (!binary) {
        ctx.ui.notify(unavailable ?? `pond not found - ${INSTALL_HINT}`, "warning");
        return;
      }
      await runPondCommand(pi, ctx, args, controller, binary, config);
    },
  });
}

/**
 * One-time, UI-only capture consent - the recorded answer that decides whether
 * pond's config may be written on the user's behalf, by `pond adapters enable`
 * here or by the child's `--bootstrap`.
 *
 * Asked at most once and remembered forever, in either direction: granted
 * enables the pi adapter now, declined takes `--bootstrap` off the child's argv
 * for good.
 *
 * Granted-and-enabled, declined, and already-enabled all settle, so an answered
 * question never re-spawns `pond adapters list` on the next session start. A
 * granted answer whose `pond adapters enable` FAILED is deliberately left
 * unrecorded - the next session asks again rather than remembering a consent
 * that never took effect.
 *
 * Skipped entirely in url mode (that pond's config belongs to whoever runs it)
 * and in headless sessions (json/print, orchestrator-spawned children) - no
 * exec, no prompt - so nothing can block on a dialog nobody can see.
 */
export async function maybeAskForCapture(
  pi: ExtensionAPI,
  ctx: ExtensionContext,
  binary: string,
  config: PondConfig,
): Promise<void> {
  if (config.mode === "url" || !ctx.hasUI || config.captureConsent !== undefined) {
    return;
  }
  const configured = await listConfiguredAdapters(pi, binary);
  const entry = configured.find((candidate) => candidate.name === ADAPTER);
  if (entry?.enabled === true) {
    // Already capturing pi - the question is answered. Record it, or every
    // interactive session start pays another `pond adapters list` subprocess
    // to re-learn the same thing, forever.
    settleCaptureConsent(config, "granted");
    return;
  }

  const yes = await ctx.ui.confirm(
    "Pond found",
    configured.length > 0
      ? "Capture pi sessions into your pond archive? (pond already captures other agents on this machine)"
      : "Capture pi sessions into your pond archive?",
  );
  if (!yes) {
    settleCaptureConsent(config, "declined");
    return;
  }
  try {
    const enabled = await pi.exec(binary, ["adapters", "enable", ADAPTER], {
      timeout: ADAPTERS_TIMEOUT_MS,
    });
    if (enabled.code === 0) {
      settleCaptureConsent(config, "granted");
      ctx.ui.notify("pond will now capture pi sessions.", "info");
      return;
    }
    ctx.ui.notify(
      `pond adapters enable ${ADAPTER} failed: ${enabled.stderr.trim() || `exit ${enabled.code}`}`,
      "error",
    );
  } catch (error) {
    ctx.ui.notify(`pond adapters enable ${ADAPTER} failed: ${String(error)}`, "error");
  }
}

/** pond's configured adapters, or an empty list when the answer is unreadable. */
async function listConfiguredAdapters(
  pi: ExtensionAPI,
  binary: string,
): Promise<{ name?: unknown; enabled?: unknown }[]> {
  try {
    const listed = await pi.exec(binary, ["adapters", "list", "--format", "json"], {
      timeout: ADAPTERS_TIMEOUT_MS,
    });
    const doc = JSON.parse(listed.stdout) as { configured?: unknown };
    return Array.isArray(doc.configured)
      ? (doc.configured as { name?: unknown; enabled?: unknown }[])
      : [];
  } catch {
    return [];
  }
}

function settleCaptureConsent(config: PondConfig, consent: CaptureConsent): void {
  // Mirrored onto the live config, not only the file: the controller reads it
  // when it spawns the child, so `--bootstrap` reflects this answer already in
  // the session that gave it.
  config.captureConsent = consent;
  recordCaptureConsent(consent);
}

async function runPondCommand(
  pi: ExtensionAPI,
  ctx: ExtensionCommandContext,
  args: string,
  controller: PondController,
  binary: string,
  config: PondConfig,
): Promise<void> {
  const query = args.trim() || (ctx.hasUI ? await ctx.ui.input("Search pond for:") : undefined);
  if (!query?.trim()) {
    return;
  }

  const response = await controller.callTool(POND_TOOL_NAMES.search, {
    query: query.trim(),
    limit: 10,
  });
  if (!response.ok) {
    ctx.ui.notify(response.error, "error");
    return;
  }
  const hits = parsePondHits(response.text);
  if (hits.length === 0) {
    // Relay pond's own absence-honesty text: it distinguishes "nothing stored
    // yet" from "the filters excluded everything" from "no match".
    ctx.ui.notify(response.text.trim().split("\n")[0] ?? "pond: no sessions matched.", "info");
    return;
  }

  const choice = await chooseHit(ctx, hits);
  if (!choice) {
    return;
  }
  if (choice.action === "insert") {
    ctx.ui.pasteToEditor(hitReference(choice.hit));
    return;
  }
  await resumeHere(pi, ctx, choice.hit, binary, config);
}

async function chooseHit(
  ctx: ExtensionCommandContext,
  hits: PondHit[],
): Promise<{ action: "resume" | "insert"; hit: PondHit } | undefined> {
  if (ctx.mode === "tui") {
    // Loaded lazily so headless runs never pull in the TUI component libraries.
    const { pickPondHit } = await import("./src/picker.ts");
    return pickPondHit(ctx, hits);
  }
  if (!ctx.hasUI) {
    ctx.ui.notify(hits.map((hit) => `${hitLabel(hit)}  ${hit.sessionId}`).join("\n"), "info");
    return undefined;
  }
  // rpc has dialogs but no custom components: two quick selects carry the same
  // two actions rather than degrading to a read-only list. `select` answers with
  // the chosen STRING, so the labels are numbered - two sessions from the same
  // agent, project, and day would otherwise be indistinguishable and the wrong
  // one would resume.
  const labels = hits.map((hit, index) => `${index + 1}. ${hitLabel(hit)}  ${hit.snippet}`);
  const picked = await ctx.ui.select("pond: pick a session", labels);
  const hit = picked === undefined ? undefined : hits[labels.indexOf(picked)];
  if (!hit) {
    return undefined;
  }
  const action = await ctx.ui.select(hitLabel(hit), [
    "Resume this session here",
    "Insert a reference into the editor",
  ]);
  if (action === undefined) {
    return undefined;
  }
  return { action: action.startsWith("Resume") ? "resume" : "insert", hit };
}

export const RESUME_NEEDS_LOCAL_POND =
  'pond: resume needs a local pond, and this session is in "url" mode - read-only recall over ' +
  "someone else's pond serve. Insert a reference instead, or switch to managed mode in " +
  "~/.pi/agent/pond-pi.json.";

/**
 * Resume: write the stored session back out as a pi session file, then switch
 * this pi to it. `--out-dir` is pi's agent dir because the pi adapter's own
 * relative paths carry the `sessions/--<cwd-slug>--/` layout, so the file lands
 * exactly where pi's session list looks for it.
 */
export async function resumeHere(
  pi: ExtensionAPI,
  ctx: ExtensionCommandContext,
  hit: PondHit,
  binary: string,
  config: PondConfig,
): Promise<void> {
  if (config.mode === "url") {
    // The only local-binary path left in url mode, and it cannot be assumed:
    // the pond holding this session runs elsewhere, so there is nothing here to
    // shell out to.
    ctx.ui.notify(RESUME_NEEDS_LOCAL_POND, "warning");
    return;
  }
  const outcome = await resumeSession({
    exec: (command, execArgs, options) => pi.exec(command, execArgs, options),
    binary,
    sessionId: hit.sessionId,
    outDir: piAgentDir(),
  });
  if (!outcome.ok) {
    ctx.ui.notify(outcome.error, "error");
    return;
  }
  // Only plain data crosses into `withSession`: the callback runs after this
  // extension instance has already been torn down, so any captured session-bound
  // object would be stale (extensions.md, "Session replacement lifecycle").
  const note = outcome.alreadyResumed
    ? `pond: already resumed - switching to ${outcome.sessionFile}`
    : `pond: resumed ${hit.sessionId} (${outcome.fidelity})`;
  await ctx.switchSession(outcome.sessionFile, {
    withSession: async (fresh) => {
      fresh.ui.notify(note, "info");
    },
  });
}
