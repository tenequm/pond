// The supervisor's two correctness seams: one dial at a time (an orphaned
// `pond serve` child is the failure mode), and what the managed child's argv is
// allowed to contain.
import { afterEach, describe, expect, it, vi } from "vitest";
import { parsePondConfig } from "../src/config.ts";
import { PondController, type PondLogger, pondServeArgs } from "../src/service.ts";

function collectingLogger(): { logger: PondLogger; errors: string[] } {
  const errors: string[] = [];
  return {
    errors,
    logger: { info: () => {}, warn: () => {}, error: (message) => errors.push(message) },
  };
}

// `dial` and `scheduleRestart` are private to the class, not to the runtime -
// the race only exists between them, so the test drives them directly.
type ControllerInternals = { dial: () => Promise<void>; scheduleRestart: () => void };

afterEach(() => {
  vi.useRealTimers();
});

describe("PondController dial single-flight", () => {
  it("shares one dial between a backoff restart and a tool call in the same window", async () => {
    vi.useFakeTimers();
    const controller = new PondController(parsePondConfig(undefined), collectingLogger().logger);
    const internals = controller as unknown as ControllerInternals;
    const releases: (() => void)[] = [];
    let dials = 0;
    let inFlight = 0;
    let peakInFlight = 0;
    internals.dial = () => {
      dials += 1;
      inFlight += 1;
      peakInFlight = Math.max(peakInFlight, inFlight);
      return new Promise<void>((resolve) => {
        releases.push(() => {
          inFlight -= 1;
          resolve();
        });
      });
    };

    internals.scheduleRestart();
    vi.advanceTimersByTime(60_000);
    expect(dials).toBe(1);

    const call = controller.callTool("pond_search", { query: "x" });
    // The whole point: the tool call must join the restart's dial, not spawn a
    // second child whose client would overwrite the first.
    expect(dials).toBe(1);
    expect(peakInFlight).toBe(1);

    for (const release of releases) {
      release();
    }
    expect(await call).toMatchObject({ ok: false });

    // The gate reopens once the dial settles, or a failed restart could never
    // be retried.
    internals.scheduleRestart();
    vi.advanceTimersByTime(60_000);
    expect(dials).toBe(2);
    for (const release of releases) {
      release();
    }
    await controller.stop();
  });

  it("stops cleanly with a restart pending after a failed dial", async () => {
    const { logger, errors } = collectingLogger();
    // url mode without a url fails the dial and schedules a backoff restart.
    const controller = new PondController(parsePondConfig({ mode: "url" }), logger);
    await controller.ensureStarted();
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("no url");
    await controller.stop();
    await controller.stop();
  });
});

describe("pondServeArgs", () => {
  it("drops --bootstrap only for a recorded decline", () => {
    expect(pondServeArgs(parsePondConfig({ captureConsent: "declined" }))).toEqual([
      "serve",
      "--transport",
      "stdio",
      "--with-sync",
      "--sync-every",
      "5",
    ]);
  });

  it("keeps first-run bootstrap when the question was never answered", () => {
    // A headless-only install is never asked (a prompt needs a UI), so an unset
    // answer must not silently mean "never capture".
    expect(pondServeArgs(parsePondConfig(undefined))).toContain("--bootstrap");
    expect(
      pondServeArgs(parsePondConfig({ captureConsent: "granted", syncIntervalMinutes: 7 })),
    ).toEqual([
      "serve",
      "--transport",
      "stdio",
      "--with-sync",
      "--sync-every",
      "7",
      "--bootstrap",
      "pi-coding-agent",
    ]);
  });
});
