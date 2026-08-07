// pi-pond: capture pi sessions into a durable pond archive and recall them from
// inside pi.
//
// Two halves, one process. A single managed `pond serve --transport stdio
// --with-sync --bootstrap pi-coding-agent` child both serves the four read-only
// recall tools over MCP and runs pond's periodic sync loop, sharing one
// embedding model. Tools only - no memory slot, no auto-recall, no prompt hooks
// (see README for the positioning); the `/pond` command adds a search picker
// that can resume a past session or paste a reference to it.
import type {
  ExtensionAPI,
  ExtensionCommandContext,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import {
  type CaptureConsent,
  loadPondConfig,
  piAgentDir,
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
    await maybeAskForCapture(pi, ctx, binary, config.captureConsent);
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
      await runPondCommand(pi, ctx, args, controller, binary);
    },
  });
}

/**
 * One-time, UI-only capture consent.
 *
 * `--bootstrap` already covers the cold start where pond has NO adapters. The
 * gap it deliberately leaves is a pond that already captures something else
 * (claude-code, say) but not pi: sync runs and quietly stores no pi sessions.
 * This asks once, remembers either answer forever, and never writes pond config
 * by any path other than the consented `pond adapters enable`.
 *
 * Headless sessions (json/print, orchestrator-spawned children) skip the whole
 * flow - no exec, no prompt - so nothing can block on a dialog nobody can see.
 */
async function maybeAskForCapture(
  pi: ExtensionAPI,
  ctx: ExtensionContext,
  binary: string,
  consent: CaptureConsent | undefined,
): Promise<void> {
  if (!ctx.hasUI || consent !== undefined) {
    return;
  }
  let configured: { name?: unknown; enabled?: unknown }[];
  try {
    const listed = await pi.exec(binary, ["adapters", "list", "--format", "json"], {
      timeout: ADAPTERS_TIMEOUT_MS,
    });
    const doc = JSON.parse(listed.stdout) as { configured?: unknown };
    configured = Array.isArray(doc.configured) ? (doc.configured as typeof configured) : [];
  } catch {
    // An unreadable adapter list is not a reason to nag; `--bootstrap` still
    // covers the case that matters most.
    return;
  }
  if (configured.length === 0) {
    // `--bootstrap pi-coding-agent` handles this on the next tool call, and it
    // will leave the adapter enabled - so this is not yet a settled answer and
    // deliberately stays unrecorded.
    return;
  }
  const entry = configured.find((candidate) => candidate.name === ADAPTER);
  if (entry?.enabled === true) {
    // Already capturing pi - the question is answered. Record it, or every
    // interactive session start pays another `pond adapters list` subprocess
    // to re-learn the same thing, forever.
    recordCaptureConsent("granted");
    return;
  }

  const yes = await ctx.ui.confirm(
    "Pond found",
    "Capture pi sessions into your pond archive? (pond already captures other agents on this machine)",
  );
  if (!yes) {
    recordCaptureConsent("declined");
    return;
  }
  try {
    const enabled = await pi.exec(binary, ["adapters", "enable", ADAPTER], {
      timeout: ADAPTERS_TIMEOUT_MS,
    });
    if (enabled.code === 0) {
      recordCaptureConsent("granted");
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

async function runPondCommand(
  pi: ExtensionAPI,
  ctx: ExtensionCommandContext,
  args: string,
  controller: PondController,
  binary: string,
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
  await resumeHere(pi, ctx, choice.hit, binary);
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

/**
 * Resume: write the stored session back out as a pi session file, then switch
 * this pi to it. `--out-dir` is pi's agent dir because the pi adapter's own
 * relative paths carry the `sessions/--<cwd-slug>--/` layout, so the file lands
 * exactly where pi's session list looks for it.
 */
async function resumeHere(
  pi: ExtensionAPI,
  ctx: ExtensionCommandContext,
  hit: PondHit,
  binary: string,
): Promise<void> {
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
