import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  DEFAULT_SYNC_INTERVAL_MINUTES,
  loadPondConfig,
  parsePondConfig,
  piAgentDir,
  recordCaptureConsent,
} from "../src/config.ts";
import { pondChildEnv } from "../src/service.ts";

function scratch(): string {
  return join(mkdtempSync(join(tmpdir(), "pi-pond-")), "pond-pi.json");
}

describe("config", () => {
  it("defaults to a managed pond on the standard sync interval", () => {
    const config = parsePondConfig(undefined);
    expect(config.mode).toBe("managed");
    expect(config.syncIntervalMinutes).toBe(DEFAULT_SYNC_INTERVAL_MINUTES);
    expect(config.captureConsent).toBeUndefined();
  });

  it("reads url mode with its headers", () => {
    const config = parsePondConfig({
      mode: "url",
      url: "http://127.0.0.1:9797/mcp",
      headers: { Authorization: "Bearer x", bad: 7 },
      syncIntervalMinutes: 0.4,
    });
    expect(config).toMatchObject({ mode: "url", url: "http://127.0.0.1:9797/mcp" });
    expect(config.headers).toEqual({ Authorization: "Bearer x" });
    expect(config.syncIntervalMinutes).toBe(1);
  });

  it("falls back to defaults when the file is missing or unparseable", () => {
    const path = scratch();
    expect(loadPondConfig(path).mode).toBe("managed");
    writeFileSync(path, "{ not json");
    expect(loadPondConfig(path).mode).toBe("managed");
  });

  it("remembers either consent answer without clobbering hand-written settings", () => {
    const path = scratch();
    writeFileSync(path, JSON.stringify({ mode: "url", url: "http://localhost:9797/mcp" }));
    expect(recordCaptureConsent("declined", path)).toBe(true);
    const written = JSON.parse(readFileSync(path, "utf8"));
    expect(written).toEqual({
      mode: "url",
      url: "http://localhost:9797/mcp",
      captureConsent: "declined",
    });
    expect(loadPondConfig(path).captureConsent).toBe("declined");
  });

  it("resolves pi's agent dir, the root a resumed session is written under", () => {
    expect(piAgentDir("/home/u")).toBe("/home/u/.pi/agent");
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
