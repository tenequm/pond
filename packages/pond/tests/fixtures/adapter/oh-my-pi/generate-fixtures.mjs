// Regenerates the oh-my-pi (omp) session fixtures by driving omp's OWN title-slot
// and session-entry code, so the committed bytes are whatever omp writes rather
// than a hand-rolled guess. Run it whenever omp ships a session-storage change:
//
//   FIX=<repo>/packages/pond/tests/fixtures/adapter/oh-my-pi
//   mkdir -p /tmp/omp-fixtures && cd /tmp/omp-fixtures && npm init -y
//   npm i @oh-my-pi/pi-coding-agent@<ver>
//   cp "$FIX/generate-fixtures.mjs" .   # imports resolve next to the script
//   rm -rf "$FIX/sessions"
//   bun run generate-fixtures.mjs "$FIX"
//
// BUN, not node: omp's package exports map every subpath to raw `./src/*.ts`
// whose own imports are extensionless, which plain node cannot resolve. bun is
// omp's own runtime, so it loads the package exactly as omp does.
//
// Fixtures last generated 2026-08-12 against @oh-my-pi/pi-coding-agent 17.2.15.
//
// Why the slot comes from omp and the entries do not: `serializeTitleSlot` is the
// one piece whose BYTES are load-bearing for pond (a fixed 256-byte first line
// that pond must fold, not parse as an entry), and omp exports it. The entry
// bodies are plain JSON shapes documented in omp's `docs/session.md`, so they are
// written literally here - readable in the diff, and no model call or API key
// needed to produce a session.
//
// A diff against the committed fixtures is the upgrade signal (spec.md#adapters
// conformance): the format moved, and the pond adapter needs the same move.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const out = process.argv[2];
if (!out) {
  console.error("usage: bun run generate-fixtures.mjs <fixture-dir>");
  process.exit(1);
}

// omp's own fixed-width slot serializer. Resolved from the installed package so
// a change to SESSION_TITLE_SLOT_BYTES or the slot shape shows up as a fixture
// diff rather than passing silently.
const { serializeTitleSlot } = await import("@oh-my-pi/pi-coding-agent/session/session-title-slot");

const CWD = "/Users/user/Projects/omp-demo";
// The bucket omp writes for a cwd under $HOME: `session-paths.ts` encodes the
// home-relative path with `/ \ :` mapped to `-`. (The hashed
// `<scope>-<basename>-<sha256>` form only ever shipped in 17.2.5-17.2.8, which
// reverted it - omp migrates those dirs back into this name.) Hardcoded rather
// than derived because deriving it needs a sandboxed $HOME, and the adapter
// treats the name as an inert placement hint.
const BUCKET = "-Projects-omp-demo";
const SESSION_ID = "0a1b2c3d4e5f6071";
const FORK_ID = "1b2c3d4e5f607182";

const line = (value) => `${JSON.stringify(value)}\n`;

function sessionFile({ id, title, parentSession, entries }) {
  const slot = serializeTitleSlot({
    title,
    source: "auto",
    updatedAt: "2026-08-12T10:00:00.000Z",
  });
  const header = {
    type: "session",
    version: 3,
    id,
    timestamp: "2026-08-12T10:00:00.000Z",
    cwd: CWD,
    title,
    titleSource: "auto",
    ...(parentSession ? { parentSession } : {}),
  };
  return slot + line(header) + entries.map(line).join("");
}

const mainEntries = [
  {
    type: "message",
    id: "aaaa0001",
    parentId: null,
    timestamp: "2026-08-12T10:00:05.000Z",
    message: {
      role: "user",
      content: [{ type: "text", text: "summarize the harness problem post" }],
    },
  },
  {
    type: "message",
    id: "aaaa0002",
    parentId: "aaaa0001",
    timestamp: "2026-08-12T10:00:09.000Z",
    message: {
      role: "assistant",
      provider: "anthropic",
      model: "claude-sonnet-4-5",
      content: [
        { type: "thinking", thinking: "the post argues the harness dominates model choice" },
        { type: "text", text: "It argues tool quality dominates raw model quality." },
        {
          type: "toolCall",
          id: "call_1",
          name: "read",
          arguments: { path: "blog/harness.md" },
        },
      ],
      usage: { input: 100, output: 20 },
      // omp records the model turn's own epoch-ms clock; keep it equal to
      // the entry timestamp so the fixture cannot read as two events.
      timestamp: Date.parse("2026-08-12T10:00:09.000Z"),
    },
  },
  {
    type: "message",
    id: "aaaa0003",
    parentId: "aaaa0002",
    timestamp: "2026-08-12T10:00:11.000Z",
    message: {
      role: "toolResult",
      toolCallId: "call_1",
      toolName: "read",
      content: [{ type: "text", text: "# The harness problem\n..." }],
      isError: false,
    },
  },
  // An entry type pi never had: it must survive as a System carrier, not a drop.
  {
    type: "ttsr_injection",
    id: "aaaa0004",
    parentId: "aaaa0003",
    timestamp: "2026-08-12T10:00:12.000Z",
    injectedRules: ["box-leak"],
  },
  // An image content item, externalized by omp into its blob store. pond keeps
  // the ref verbatim; the bytes stay in ~/.omp/agent/blobs.
  {
    type: "message",
    id: "aaaa0005",
    parentId: "aaaa0004",
    timestamp: "2026-08-12T10:00:20.000Z",
    message: {
      role: "user",
      content: [
        { type: "text", text: "and this screenshot?" },
        { type: "image", image_url: "blob:sha256:2f0c9a7b5d1e4f60" },
      ],
    },
  },
  {
    type: "model_change",
    id: "aaaa0006",
    parentId: "aaaa0005",
    timestamp: "2026-08-12T10:00:25.000Z",
    model: "openai/gpt-5.5",
    role: "default",
  },
];

const forkEntries = [
  {
    type: "branch_summary",
    id: "bbbb0001",
    parentId: null,
    timestamp: "2026-08-12T10:05:00.000Z",
    fromId: "aaaa0002",
    summary: "abandoned the LSP tangent",
  },
  {
    type: "message",
    id: "bbbb0002",
    parentId: "bbbb0001",
    timestamp: "2026-08-12T10:05:04.000Z",
    message: {
      role: "user",
      content: [{ type: "text", text: "try the debugger angle instead" }],
    },
  },
];

const bucket = join(out, "sessions", BUCKET);
mkdirSync(bucket, { recursive: true });

writeFileSync(
  join(bucket, `2026-08-12T10-00-00-000Z_${SESSION_ID}.jsonl`),
  sessionFile({ id: SESSION_ID, title: "harness problem", entries: mainEntries }),
);
writeFileSync(
  join(bucket, `2026-08-12T10-05-00-000Z_${FORK_ID}.jsonl`),
  sessionFile({
    id: FORK_ID,
    title: "debugger angle",
    parentSession: SESSION_ID,
    entries: forkEntries,
  }),
);
// A legacy slot-less file: omp still reads these, so pond must too.
writeFileSync(
  join(bucket, "2026-08-12T09-00-00-000Z_legacy00000001.jsonl"),
  line({
    type: "session",
    version: 3,
    id: "legacy00000001",
    timestamp: "2026-08-12T09:00:00.000Z",
    cwd: CWD,
  }) +
    line({
      type: "message",
      id: "cccc0001",
      parentId: null,
      timestamp: "2026-08-12T09:00:03.000Z",
      message: { role: "user", content: [{ type: "text", text: "legacy file, no slot" }] },
    }),
);

console.log(`wrote fixtures under ${bucket}`);
