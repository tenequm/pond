#!/usr/bin/env bash
# Build out/rotate-reply/fixture/ from the capture home produced by capture.sh,
# then sanitize it. Idempotent: rebuilds fixture/ from scratch each run.
set -euo pipefail

NODE=${NODE:-/tmp/openclaw-fixture/node/bin/node}
SRC=${SRC:-/tmp/openclaw-fixture/rotate-reply/capture-home/home/.openclaw}
# Defaults to this script's own directory, which IS the fixture root.
OUT=${OUT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

[ -d "$SRC/agents/main/sessions" ] || { echo "no capture at $SRC - run capture.sh first" >&2; exit 1; }

rm -rf "$OUT"
mkdir -p "$OUT/agents/main/sessions" "$OUT/state"

# 1. Sessions dir verbatim, EXCEPT skills-prompts/ (per COMMON.md).
find "$SRC/agents/main/sessions" -maxdepth 1 -type f -print0 \
  | xargs -0 -I{} cp -p {} "$OUT/agents/main/sessions/"

# 2. state/openclaw.sqlite: COPY, keep only audit_events, drop the rest, VACUUM.
#    audit_events is kept because it is a real session_id -> session_key map and is
#    the ONLY on-disk source that can resolve generation E (trajectories disabled).
cp -p "$SRC/state/openclaw.sqlite" "$OUT/state/openclaw.sqlite"
rm -f "$OUT/state/openclaw.sqlite-wal" "$OUT/state/openclaw.sqlite-shm"
"$NODE" - "$OUT/state/openclaw.sqlite" <<'MJS'
const { DatabaseSync } = require("node:sqlite");
const db = new DatabaseSync(process.argv[2]);
const keep = new Set(["audit_events"]);
const tables = db.prepare("select name from sqlite_master where type='table'").all().map(r => r.name);
const dropped = [];
db.exec("PRAGMA foreign_keys=OFF");
for (const t of tables) {
  if (keep.has(t) || t.startsWith("sqlite_")) continue;
  db.exec(`DROP TABLE IF EXISTS "${t}"`);
  dropped.push(t);
}
for (const v of db.prepare("select name from sqlite_master where type='view'").all()) {
  db.exec(`DROP VIEW IF EXISTS "${v.name}"`);
  dropped.push(`view:${v.name}`);
}
db.exec("VACUUM");
db.close();
require("fs").writeFileSync(
  "/tmp/openclaw-fixture/rotate-reply/dropped-tables.json",
  JSON.stringify({ kept: [...keep], dropped }, null, 2) + "\n",
);
console.log(`sqlite: kept ${[...keep].join(",")}; dropped ${dropped.length} tables/views`);
MJS

# 3. Hostname scrub: OpenClaw bakes os.hostname() into the system-prompt preamble
#    ("host=ws-pond-01") inside every .trajectory.jsonl. Replace, then re-parse.
"$NODE" - "$OUT" <<'MJS'
const fs = require("fs"), path = require("path");
let files = 0, replacements = 0;
const walk = (d) => fs.readdirSync(d, { withFileTypes: true }).flatMap((e) =>
  e.isDirectory() ? walk(path.join(d, e.name)) : [path.join(d, e.name)]);
for (const f of walk(process.argv[2])) {
  if (f.endsWith(".sqlite")) continue;
  const before = fs.readFileSync(f, "utf8");
  const n = (before.match(/ws-pond-01/g) ?? []).length;
  if (!n) continue;
  fs.writeFileSync(f, before.split("ws-pond-01").join("sandbox-host"));
  files++; replacements += n;
}
console.log(`hostname: replaced ${replacements} occurrence(s) of ws-pond-01 in ${files} file(s)`);
MJS

# 4. Re-parse every JSON / JSONL line after the rewrite.
"$NODE" - "$OUT" <<'MJS'
const fs = require("fs"), path = require("path");
const walk = (d) => fs.readdirSync(d, { withFileTypes: true }).flatMap((e) =>
  e.isDirectory() ? walk(path.join(d, e.name)) : [path.join(d, e.name)]);
let jsonFiles = 0, lines = 0, bad = 0;
for (const f of walk(process.argv[2])) {
  const base = path.basename(f);
  const isJsonl = base.endsWith(".jsonl") || base.includes(".jsonl.reset.") || base.includes(".jsonl.deleted.");
  const isJson = base.endsWith(".json");
  if (!isJsonl && !isJson) continue;
  jsonFiles++;
  const text = fs.readFileSync(f, "utf8");
  if (isJson) {
    try { JSON.parse(text); lines++; } catch (e) { bad++; console.error(`BAD JSON ${f}: ${e.message}`); }
  } else {
    for (const [i, l] of text.split("\n").entries()) {
      if (!l.trim()) continue;
      lines++;
      try { JSON.parse(l); } catch (e) { bad++; console.error(`BAD JSONL ${f}:${i + 1}: ${e.message}`); }
    }
  }
}
console.log(`parse check: ${jsonFiles} file(s), ${lines} document(s)/line(s), ${bad} failure(s)`);
if (bad) process.exit(1);
MJS

# 5. Lineage snapshots live NEXT TO fixture/, never inside it: they are derived
#    evidence, not files OpenClaw wrote, so they must not pollute the native tree.
#    They are the only record that usageFamilySessionIds was ever populated - the
#    final sessions.json has it wiped by the two gateway sessions.reset calls.
cp -p /tmp/openclaw-fixture/rotate-reply/capture-home/lineage.jsonl \
      "$(dirname "$OUT")/lineage.jsonl"

# 6. node:sqlite leaves -wal/-shm beside the db even for read-only opens; the
#    fixture must contain only openclaw.sqlite itself.
rm -f "$OUT/state/openclaw.sqlite-wal" "$OUT/state/openclaw.sqlite-shm"

echo
echo "fixture built at $OUT"
find "$OUT" -type f | sort
echo
echo "files: $(find "$OUT" -type f | wc -l)   bytes: $(du -sb "$OUT" | cut -f1)"
