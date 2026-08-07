// Extension-owned state: how to reach pond, and the one-time capture-consent
// answer. pi has no per-extension config schema, so this is a small JSON file
// next to pi's own state. Everything is optional and every read is
// fault-tolerant - a corrupt or absent file must never keep pi from starting.
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

export type PondMode = "managed" | "url";

/** Answer to the one-time "capture pi sessions?" prompt. Absent = never asked. */
export type CaptureConsent = "granted" | "declined";

export type PondConfig = {
  mode: PondMode;
  syncIntervalMinutes: number;
  binaryPath?: string;
  url?: string;
  headers: Record<string, string>;
  captureConsent?: CaptureConsent;
};

export const DEFAULT_SYNC_INTERVAL_MINUTES = 5;

/** pi's own agent directory: the sessions root and this file's home. */
export function piAgentDir(home: string = homedir()): string {
  return join(home, ".pi", "agent");
}

export function configPath(home: string = homedir()): string {
  return join(piAgentDir(home), "pond-pi.json");
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asStringMap(value: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, entry] of Object.entries(asRecord(value))) {
    if (typeof entry === "string") {
      out[key] = entry;
    }
  }
  return out;
}

export function parsePondConfig(raw: unknown): PondConfig {
  const root = asRecord(raw);
  const interval = root.syncIntervalMinutes;
  return {
    mode: root.mode === "url" ? "url" : "managed",
    syncIntervalMinutes:
      typeof interval === "number" && Number.isFinite(interval)
        ? Math.max(1, Math.floor(interval))
        : DEFAULT_SYNC_INTERVAL_MINUTES,
    ...(typeof root.binaryPath === "string" ? { binaryPath: root.binaryPath } : {}),
    ...(typeof root.url === "string" ? { url: root.url } : {}),
    headers: asStringMap(root.headers),
    ...(root.captureConsent === "granted" || root.captureConsent === "declined"
      ? { captureConsent: root.captureConsent as CaptureConsent }
      : {}),
  };
}

export function loadPondConfig(path: string = configPath()): PondConfig {
  try {
    return parsePondConfig(JSON.parse(readFileSync(path, "utf8")));
  } catch {
    // Absent or unreadable is the normal first-run state, and a hand-edited
    // file that no longer parses must not break pi - fall back to defaults.
    return parsePondConfig(undefined);
  }
}

/**
 * Persist the consent answer, preserving whatever else the user hand-wrote in
 * the file. Best-effort: a write failure means we may ask once more, which is
 * strictly better than failing the session.
 */
export function recordCaptureConsent(
  consent: CaptureConsent,
  path: string = configPath(),
): boolean {
  let existing: Record<string, unknown> = {};
  try {
    existing = asRecord(JSON.parse(readFileSync(path, "utf8")));
  } catch {
    existing = {};
  }
  try {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, `${JSON.stringify({ ...existing, captureConsent: consent }, null, 2)}\n`);
    return true;
  } catch {
    return false;
  }
}
