// Plugin-owned configuration: connection mode to pond plus the two access knobs
// the plugin adds on top of the operator's existing `tools.sessions.visibility`
// and `tools.agentToAgent` (which are read from OpenClaw config via the SDK,
// never redeclared here).

export type PondMode = "managed" | "url";

export type GroupSessionsPolicy = "clamp" | "inherit";

export type PondPluginConfig = {
  mode: PondMode;
  syncIntervalMinutes: number;
  binaryPath?: string;
  url?: string;
  headers: Record<string, string>;
  sources: string[];
  groupSessions: GroupSessionsPolicy;
};

export const DEFAULT_SYNC_INTERVAL_MINUTES = 5;
export const DEFAULT_SOURCES = ["openclaw"] as const;

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asStringArray(value: unknown, fallback: readonly string[]): string[] {
  if (!Array.isArray(value)) {
    return [...fallback];
  }
  const items = value.filter((entry): entry is string => typeof entry === "string" && entry.length > 0);
  return items.length > 0 ? items : [...fallback];
}

function asStringMap(value: unknown): Record<string, string> {
  const record = asRecord(value);
  const out: Record<string, string> = {};
  for (const [key, entry] of Object.entries(record)) {
    if (typeof entry === "string") {
      out[key] = entry;
    }
  }
  return out;
}

export function parsePluginConfig(raw: Record<string, unknown> | undefined): PondPluginConfig {
  const root = asRecord(raw);
  const pond = asRecord(root.pond);
  const mode: PondMode = pond.mode === "url" ? "url" : "managed";
  const syncIntervalMinutes =
    typeof pond.syncIntervalMinutes === "number" && Number.isFinite(pond.syncIntervalMinutes)
      ? Math.max(1, Math.floor(pond.syncIntervalMinutes))
      : DEFAULT_SYNC_INTERVAL_MINUTES;
  const groupSessions: GroupSessionsPolicy = root.groupSessions === "inherit" ? "inherit" : "clamp";
  return {
    mode,
    syncIntervalMinutes,
    ...(typeof pond.binaryPath === "string" ? { binaryPath: pond.binaryPath } : {}),
    ...(typeof pond.url === "string" ? { url: pond.url } : {}),
    headers: asStringMap(pond.headers),
    sources: asStringArray(root.sources, DEFAULT_SOURCES),
    groupSessions,
  };
}
