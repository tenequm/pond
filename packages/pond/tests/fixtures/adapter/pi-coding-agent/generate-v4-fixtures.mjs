// Regenerates the pi harness-v2 fixtures (v4 JSONL + the SQLite backend) by
// driving pi's OWN storage code, so the committed bytes are whatever pi writes
// rather than a hand-rolled guess. Run it whenever pi ships a storage change:
//
//   FIX=<repo>/packages/pond/tests/fixtures/adapter/pi-coding-agent
//   mkdir -p /tmp/pi-fixtures && cd /tmp/pi-fixtures && npm init -y
//   npm i @earendil-works/pi-agent-core@<ver> @earendil-works/pi-session-backend-sqlite-node@<ver>
//   cp "$FIX/generate-v4-fixtures.mjs" .   # node resolves imports next to the script
//   rm -rf "$FIX/sessions/--Users-user-Projects-harness-v2--" "$FIX/sqlite"
//   node generate-v4-fixtures.mjs "$FIX"
//
// Fixtures last generated 2026-08-06 against pi 0.84.1 (commit 6fb2d766a).
//
// A diff against the committed fixtures is the upgrade signal (spec.md#adapters
// conformance): the formats moved, and the pond adapter needs the same move.
//
// Determinism: every id is supplied by the caller and `Date.now` is replaced by
// a monotone fake clock, so a re-run on an unchanged pi produces byte-identical
// output and a real diff means a real format change.

import { mkdtempSync, mkdirSync, rmSync, cpSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";

const OUT = process.argv[2];
if (!OUT) {
  console.error("usage: generate-v4-fixtures.mjs <fixtures-dir>");
  process.exit(1);
}

// 2026-08-06T00:00:00.000Z, advanced 1s per call.
let clock = Date.UTC(2026, 7, 6, 0, 0, 0);
Date.now = () => (clock += 1000);

const { JsonlSessionRepo, NodeExecutionEnv } = await import("@earendil-works/pi-agent-core/node");
const { SqliteSessionRepository, createNodeSqliteFactory } = await import(
  "@earendil-works/pi-session-backend-sqlite-node"
);

const work = mkdtempSync(join(tmpdir(), "pi-fixtures-"));
const fs = new NodeExecutionEnv({ cwd: work });
// The session's recorded cwd is a stable placeholder, not this machine's paths:
// it becomes the fixture's `project` and the `--<slug>--` directory name.
const cwd = "/Users/user/Projects/harness-v2";

// -- v4 JSONL ---------------------------------------------------------------

const jsonlRoot = join(work, "sessions");
const repo = new JsonlSessionRepo({ fs, sessionsRoot: jsonlRoot });

// Exercises every entry type, every record type, a second lane, both fact
// kinds, and a tool call whose result and usage records tie back to it.
const main = await repo.create({
  id: "v4-main-session",
  cwd,
  metadata: { harness: "v2", fixture: "pond" },
});

await main.appendEntry(
  {
    id: "v4-entry-model",
    type: "model_change",
    provider: "anthropic",
    modelId: "claude-opus-5",
  },
  "main",
);
await main.appendEntry(
  { id: "v4-entry-thinking", type: "thinking_level_change", thinkingLevel: "high" },
  "main",
);
await main.appendEntry(
  { id: "v4-entry-tools", type: "active_tools_change", activeToolNames: ["bash", "read"] },
  "main",
);
await main.appendEntry(
  {
    id: "v4-entry-user",
    type: "message",
    message: {
      role: "user",
      content: [{ type: "text", text: "Summarize the harness-v2 storage rewrite." }],
      timestamp: Date.now(),
    },
  },
  "main",
);
await main.appendRecord({
  id: "v4-run-1",
  lane: "main",
  type: "operation_started",
  sourceLeafId: "v4-entry-user",
  intent: { kind: "run", originalPrompt: [], initialMessages: [] },
});
await main.appendRecord({
  id: "v4-step-1",
  lane: "main",
  type: "step_attempt",
  runId: "v4-run-1",
  step: "assistant",
  attempt: 1,
  resultEntryId: "v4-entry-assistant",
});
await main.appendEntry(
  {
    id: "v4-entry-assistant",
    type: "message",
    message: {
      role: "assistant",
      api: "anthropic-messages",
      provider: "anthropic",
      model: "claude-opus-5",
      content: [
        { type: "thinking", thinking: "Two storage generations coexist.", thinkingSignature: "sig-1" },
        { type: "text", text: "Reading the session module now." },
        { type: "toolCall", id: "call-1", name: "read", arguments: { path: "session/types.ts" } },
      ],
      usage: {
        input: 120,
        output: 44,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 164,
        cost: { input: 0.003, output: 0.001, cacheRead: 0, cacheWrite: 0, total: 0.004 },
      },
      stopReason: "toolUse",
      timestamp: Date.now(),
    },
  },
  "main",
);
await main.appendRecord({
  id: "v4-tool-1",
  lane: "main",
  type: "tool_started",
  runId: "v4-run-1",
  assistantEntryId: "v4-entry-assistant",
  toolIndex: 0,
  toolCallId: "call-1",
  toolName: "read",
  effectiveArgs: { path: "session/types.ts" },
  resultEntryId: "v4-entry-tool-result",
  replay: "safe",
});
await main.appendEntry(
  {
    id: "v4-entry-tool-result",
    type: "message",
    message: {
      role: "toolResult",
      toolCallId: "call-1",
      toolName: "read",
      content: [{ type: "text", text: "export interface EntryBase { ... }" }],
      isError: false,
      timestamp: Date.now(),
    },
  },
  "main",
);
await main.appendRecord({
  id: "v4-usage-1",
  lane: "main",
  type: "usage",
  cause: "assistant",
  runId: "v4-run-1",
  entryId: "v4-entry-assistant",
  attempt: 1,
  stopReason: "toolUse",
  usage: {
        input: 120,
        output: 44,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 164,
        cost: { input: 0.003, output: 0.001, cacheRead: 0, cacheWrite: 0, total: 0.004 },
      },
});
await main.appendRecord({
  id: "v4-queue-1",
  lane: "main",
  type: "queue_enqueued",
  queue: "steer",
  runId: "v4-run-1",
  target: {
    id: "v4-queued-entry",
    type: "message",
    message: { role: "user", content: [{ type: "text", text: "also check the codec" }], timestamp: Date.now() },
  },
});
await main.appendRecord({
  id: "v4-queue-cancel-1",
  lane: "main",
  type: "queue_cancelled",
  runId: "v4-run-1",
  entryId: "v4-queued-entry",
});
await main.appendRecord({
  id: "v4-defer-1",
  lane: "main",
  type: "write_deferred",
  runId: "v4-run-1",
  target: {
    id: "v4-deferred-entry",
    type: "custom",
    customType: "pond-fixture",
    data: { note: "deferred write" },
  },
});
await main.appendRecord({
  id: "v4-abort-1",
  lane: "main",
  type: "abort_requested",
  runId: "v4-run-1",
});
await main.appendRecord({
  id: "v4-finish-1",
  lane: "main",
  type: "operation_finished",
  runId: "v4-run-1",
  outcome: "completed",
});
await main.appendEntry(
  {
    id: "v4-entry-compaction",
    type: "compaction",
    summary: "Compacted the storage-rewrite discussion.",
    retainedTail: [],
    tokensBefore: 164,
  },
  "main",
);
await main.appendEntry(
  {
    id: "v4-entry-branch-summary",
    type: "branch_summary",
    fromId: "v4-entry-assistant",
    summary: "Explored the SQLite backend, came back.",
  },
  "main",
);
await main.appendEntry(
  { id: "v4-entry-custom", type: "custom", customType: "pond-fixture", data: { note: "custom entry" } },
  "main",
);
// A lane beyond `main`, plus a move, so both lane-mutation shapes are covered.
await main.createLane("side", "v4-entry-assistant");
await main.appendEntry(
  {
    id: "v4-entry-side",
    type: "message",
    message: { role: "user", content: [{ type: "text", text: "side lane probe" }], timestamp: Date.now() },
  },
  "side",
);
await main.moveLane("side", "v4-entry-assistant");
await main.setName("harness-v2 storage rewrite");
await main.setLabel("v4-entry-assistant", "key turn");

// A fork carries `parentSessionId` in its header - the v4 lineage signal.
const forked = await repo.fork(await main.getMetadata(), {
  id: "v4-fork-session",
  cwd,
  scope: "branch",
  entryId: "v4-entry-assistant",
  position: "at",
});
await forked.appendEntry(
  {
    id: "v4-fork-entry",
    type: "message",
    message: { role: "user", content: [{ type: "text", text: "continue on the fork" }], timestamp: Date.now() },
  },
  "main",
);

// -- SQLite backend ---------------------------------------------------------

const dbPath = join(work, "pi-sessions.sqlite");
const sqliteRepo = new SqliteSessionRepository({
  env: fs,
  sqlite: createNodeSqliteFactory(),
  databasePath: dbPath,
});
const dbMain = await sqliteRepo.create({
  id: "sqlite-main-session",
  cwd,
  metadata: { backend: "sqlite" },
});
await dbMain.appendEntry(
  { id: "sqlite-entry-model", type: "model_change", provider: "anthropic", modelId: "claude-opus-5" },
  "main",
);
await dbMain.appendEntry(
  {
    id: "sqlite-entry-user",
    type: "message",
    message: { role: "user", content: [{ type: "text", text: "Does the SQLite backend share the v4 shapes?" }], timestamp: Date.now() },
  },
  "main",
);
await dbMain.appendRecord({
  id: "sqlite-run-1",
  lane: "main",
  type: "operation_started",
  sourceLeafId: "sqlite-entry-user",
  intent: { kind: "run", originalPrompt: [], initialMessages: [] },
});
await dbMain.appendEntry(
  {
    id: "sqlite-entry-assistant",
    type: "message",
    message: {
      role: "assistant",
      api: "anthropic-messages",
      provider: "anthropic",
      model: "claude-opus-5",
      content: [
        { type: "text", text: "Yes - same entry and record payloads, different container." },
        { type: "toolCall", id: "sqlite-call-1", name: "bash", arguments: { command: "ls" } },
      ],
      usage: {
        input: 40,
        output: 20,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 60,
        cost: { input: 0.0008, output: 0.0002, cacheRead: 0, cacheWrite: 0, total: 0.001 },
      },
      stopReason: "toolUse",
      timestamp: Date.now(),
    },
  },
  "main",
);
await dbMain.appendEntry(
  {
    id: "sqlite-entry-tool-result",
    type: "message",
    message: {
      role: "toolResult",
      toolCallId: "sqlite-call-1",
      toolName: "bash",
      content: [{ type: "text", text: "migrations/ storage/ repo.ts" }],
      isError: false,
      timestamp: Date.now(),
    },
  },
  "main",
);
await dbMain.appendRecord({
  id: "sqlite-usage-1",
  lane: "main",
  type: "usage",
  cause: "assistant",
  runId: "sqlite-run-1",
  entryId: "sqlite-entry-assistant",
  attempt: 1,
  stopReason: "toolUse",
  usage: {
        input: 40,
        output: 20,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 60,
        cost: { input: 0.0008, output: 0.0002, cacheRead: 0, cacheWrite: 0, total: 0.001 },
      },
});
await dbMain.appendRecord({
  id: "sqlite-finish-1",
  lane: "main",
  type: "operation_finished",
  runId: "sqlite-run-1",
  outcome: "completed",
});
await dbMain.createLane("side", "sqlite-entry-assistant");
await dbMain.setName("sqlite backend probe");
await dbMain.setLabel("sqlite-entry-assistant", "answer");

const dbChild = await sqliteRepo.create({
  id: "sqlite-child-session",
  cwd,
  parentSessionId: "sqlite-main-session",
});
await dbChild.appendEntry(
  {
    id: "sqlite-child-entry",
    type: "message",
    message: { role: "user", content: [{ type: "text", text: "child of the sqlite session" }], timestamp: Date.now() },
  },
  "main",
);
await sqliteRepo.close();

// -- publish ----------------------------------------------------------------

cpSync(jsonlRoot, join(OUT, "sessions"), { recursive: true });
// Fold the WAL into the database before copying: `-wal` / `-shm` are transient
// process state, not fixture content, and committing them would ship a fixture
// whose data lives in a sidecar that the next writer invalidates.
const { DatabaseSync } = await import("node:sqlite");
const db = new DatabaseSync(dbPath);
db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
db.close();
rmSync(`${dbPath}-wal`, { force: true });
rmSync(`${dbPath}-shm`, { force: true });

mkdirSync(dirname(join(OUT, "sqlite", "pi-sessions.sqlite")), { recursive: true });
cpSync(dbPath, join(OUT, "sqlite", "pi-sessions.sqlite"));
rmSync(work, { recursive: true, force: true });
console.log(`wrote v4 + sqlite fixtures under ${OUT}`);
