// The two places the extension is allowed to touch the machine: the one-time
// capture consent (the only writer of pond config) and `/pond` resume (the only
// caller of the local binary).
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  ExtensionAPI,
  ExtensionCommandContext,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { maybeAskForCapture, RESUME_NEEDS_LOCAL_POND, resumeHere } from "../index.ts";
import { configPath, loadPondConfig, parsePondConfig } from "../src/config.ts";
import type { PondHit } from "../src/hits.ts";
import { pondServeArgs } from "../src/service.ts";

// Consent is persisted through `configPath()` -> `homedir()`, so the sandbox is
// a temp HOME: no test may touch the developer's own ~/.pi.
let home = "";
const realHome = process.env.HOME;

beforeEach(() => {
  home = mkdtempSync(join(tmpdir(), "pi-pond-home-"));
  process.env.HOME = home;
});

afterEach(() => {
  if (realHome === undefined) {
    delete process.env.HOME;
  } else {
    process.env.HOME = realHome;
  }
});

type ExecResult = { stdout: string; stderr: string; code: number };

function fakePi(respond: (args: string[]) => ExecResult) {
  const exec = vi.fn(async (_command: string, args: string[]) => respond(args));
  return { pi: { exec } as unknown as ExtensionAPI, exec };
}

function fakeCtx(options: { hasUI?: boolean; confirm?: boolean } = {}) {
  const notifications: string[] = [];
  const questions: string[] = [];
  const ctx = {
    hasUI: options.hasUI ?? true,
    ui: {
      confirm: async (_title: string, message: string) => {
        questions.push(message);
        return options.confirm ?? false;
      },
      notify: (message: string) => notifications.push(message),
    },
  } as unknown as ExtensionContext;
  return { ctx, notifications, questions };
}

function adaptersList(configured: { name: string; enabled: boolean }[]): ExecResult {
  return { stdout: JSON.stringify({ configured }), stderr: "", code: 0 };
}

describe("maybeAskForCapture", () => {
  it("writes nothing and asks nothing in url mode - that pond is not ours to configure", async () => {
    const { pi, exec } = fakePi(() => adaptersList([]));
    const { ctx, questions } = fakeCtx();
    const config = parsePondConfig({ mode: "url", url: "http://127.0.0.1:9797/mcp" });
    await maybeAskForCapture(pi, ctx, "pond", config);
    expect(exec).not.toHaveBeenCalled();
    expect(questions).toEqual([]);
    expect(config.captureConsent).toBeUndefined();
    expect(existsSync(configPath(home))).toBe(false);
  });

  it("never prompts or spawns anything in a headless session", async () => {
    const { pi, exec } = fakePi(() => adaptersList([]));
    const { ctx, questions } = fakeCtx({ hasUI: false });
    await maybeAskForCapture(pi, ctx, "pond", parsePondConfig(undefined));
    expect(exec).not.toHaveBeenCalled();
    expect(questions).toEqual([]);
  });

  it("records the answer an already-enabled adapter already gives", async () => {
    const { pi } = fakePi(() => adaptersList([{ name: "pi-coding-agent", enabled: true }]));
    const { ctx, questions } = fakeCtx();
    const config = parsePondConfig(undefined);
    await maybeAskForCapture(pi, ctx, "pond", config);
    expect(questions).toEqual([]);
    expect(config.captureConsent).toBe("granted");
    expect(loadPondConfig(configPath(home)).captureConsent).toBe("granted");
  });

  it("asks on a pond with no adapters at all, then enables the adapter", async () => {
    const { pi, exec } = fakePi((args) =>
      args[1] === "list" ? adaptersList([]) : { stdout: "", stderr: "", code: 0 },
    );
    const { ctx, questions, notifications } = fakeCtx({ confirm: true });
    const config = parsePondConfig(undefined);
    await maybeAskForCapture(pi, ctx, "pond", config);
    // Nothing claims other agents are captured when nothing is.
    expect(questions[0]).not.toContain("other agents");
    expect(exec).toHaveBeenLastCalledWith(
      "pond",
      ["adapters", "enable", "pi-coding-agent"],
      expect.objectContaining({ timeout: expect.any(Number) }),
    );
    expect(config.captureConsent).toBe("granted");
    expect(notifications).toEqual(["pond will now capture pi sessions."]);
    expect(pondServeArgs(config)).toContain("--bootstrap");
  });

  it("settles a declined answer and never probes pond again", async () => {
    const { pi, exec } = fakePi(() => adaptersList([{ name: "claude-code", enabled: true }]));
    const { ctx, questions } = fakeCtx({ confirm: false });
    const config = parsePondConfig(undefined);
    await maybeAskForCapture(pi, ctx, "pond", config);
    expect(questions[0]).toContain("other agents");
    expect(config.captureConsent).toBe("declined");
    expect(loadPondConfig(configPath(home)).captureConsent).toBe("declined");
    expect(pondServeArgs(config)).not.toContain("--bootstrap");

    await maybeAskForCapture(pi, ctx, "pond", config);
    expect(exec).toHaveBeenCalledTimes(1);
  });

  it("settles an unreadable adapter list too, instead of re-spawning pond every session", async () => {
    const { pi, exec } = fakePi(() => {
      throw new Error("pond: exit 2");
    });
    const { ctx, questions } = fakeCtx({ confirm: false });
    const config = parsePondConfig(undefined);
    await maybeAskForCapture(pi, ctx, "pond", config);
    expect(questions).toHaveLength(1);
    expect(config.captureConsent).toBe("declined");

    // A later session reloads the file, and finds the question answered.
    const reloaded = loadPondConfig(configPath(home));
    await maybeAskForCapture(pi, ctx, "pond", reloaded);
    expect(exec).toHaveBeenCalledTimes(1);
  });
});

const HIT: PondHit = {
  sessionId: "019dd55d-99a4-7344-aa11-d1d71d2c80fb",
  project: "pond",
  sourceAgent: "pi-coding-agent",
  snippet: "the OCC retry loop",
};

describe("resumeHere", () => {
  it("refuses in url mode instead of shelling out to a local pond", async () => {
    const { pi, exec } = fakePi(() => ({ stdout: "", stderr: "", code: 0 }));
    const { ctx, notifications } = fakeCtx();
    const switchSession = vi.fn();
    const commandCtx = Object.assign({}, ctx, {
      switchSession,
    }) as unknown as ExtensionCommandContext;
    await resumeHere(pi, commandCtx, HIT, "pond", parsePondConfig({ mode: "url" }));
    expect(notifications).toEqual([RESUME_NEEDS_LOCAL_POND]);
    expect(exec).not.toHaveBeenCalled();
    expect(switchSession).not.toHaveBeenCalled();
  });

  it("switches to the file pond wrote in managed mode", async () => {
    const file = `/tmp/sessions/2026-08-06T00-00-01-000Z_${HIT.sessionId}.jsonl`;
    const { pi, exec } = fakePi(() => ({
      stdout: JSON.stringify({
        sessions: [{ session_id: HIT.sessionId, actual_fidelity: "native", files: [file] }],
      }),
      stderr: "",
      code: 0,
    }));
    const { ctx } = fakeCtx();
    const switchSession = vi.fn();
    const commandCtx = Object.assign({}, ctx, {
      switchSession,
    }) as unknown as ExtensionCommandContext;
    await resumeHere(pi, commandCtx, HIT, "pond", parsePondConfig(undefined));
    expect(exec).toHaveBeenCalledTimes(1);
    expect(switchSession).toHaveBeenCalledWith(file, expect.anything());
  });
});
