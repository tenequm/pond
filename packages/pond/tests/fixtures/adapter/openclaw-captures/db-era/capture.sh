#!/usr/bin/env bash
# Pass `db-era` capture: drive every session-lifecycle scenario on OpenClaw 2026.9.3
# (the DB session store) and leave the resulting state root in place for fixture building.
#
# Idempotent: rm -rf's its own capture home and starts from empty.
# Runs a gateway on port 18840 and stops it via an EXIT trap.
#
#   RUN=capture-home bash capture.sh      # default
#   RUN=rehearsal2   bash capture.sh      # same script into a different work dir
set -euo pipefail

OC_RT=${OC_RT:-/tmp/openclaw-db}
export OC_RT
RUN=${RUN:-capture-home}
PASS=db-era
W="$OC_RT/$PASS/$RUN"
H="$W/home"
PORT=18840
OC="$OC_RT/oc"
NODE="$OC_RT/node/bin/node"
STUB=${STUB:-http://127.0.0.1:18555/v1}

run() { OC_RT="$OC_RT" OC_HOME="$H" "$OC" "$@"; }
say() { printf '\n=== %s\n' "$*"; }

# --- preflight ---------------------------------------------------------------
[ -x "$OC" ] || { echo "missing $OC — run: OPENCLAW_VERSION=2026.9.3 RT=$OC_RT setup.sh" >&2; exit 1; }
curl -s --max-time 5 "$STUB/models" >/dev/null || { echo "stub LLM down at $STUB" >&2; exit 1; }

# --- clean slate (only ever our own run dir) ---------------------------------
rm -rf "$W"
mkdir -p "$H/.openclaw"

cat > "$H/.openclaw/openclaw.json" <<JSON
{
  "models": { "mode": "merge", "providers": { "stub": {
    "baseUrl": "$STUB", "apiKey": "stub-not-a-secret",
    "api": "openai-completions",
    "models": [{ "id": "stub-model", "name": "Stub", "contextWindow": 200000, "maxTokens": 1024 }]
  } } },
  "logging": { "file": "$W/openclaw.log" },
  "agents": { "defaults": {
    "model": { "primary": "stub/stub-model" },
    "heartbeat": { "every": "15m" }
  } },
  "session": { "reset": { "mode": "idle", "idleMinutes": 1 } },
  "gateway": { "mode": "local", "port": $PORT, "bind": "loopback", "auth": { "mode": "none" } }
}
JSON
run config validate

GW_PID=""
stop_gateway() {
  [ -n "$GW_PID" ] || return 0
  # The gateway renames itself to `openclaw-gatewa`, so verify by sandbox HOME before killing.
  if grep -qa "HOME=$H" "/proc/$GW_PID/environ" 2>/dev/null; then
    kill "$GW_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$GW_PID" 2>/dev/null || break; sleep 1; done
  fi
  GW_PID=""
}
trap stop_gateway EXIT

start_gateway() {
  OC_RT="$OC_RT" OC_HOME="$H" nohup "$OC" gateway run --port "$PORT" > "$W/gateway.out" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 60); do
    curl -s --max-time 2 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "gateway did not come up on $PORT" >&2; exit 1
}

# =============================================================================
# S1  baseline turns on the main key, embedded/local path
#     (`--local` is REFUSED once a gateway owns the state dir, so do these first)
# =============================================================================
say "S1 baseline local turns on agent:main:main"
run agent --local --agent main --message "baseline one" --json > "$W/s1-turn1.json"
run agent --local --agent main --message "baseline two" --json > "$W/s1-turn2.json"

say "start gateway"
start_gateway

# =============================================================================
# S2  reset the main key twice (2026.9.3 writes an in-transcript `reset` EVENT,
#     it does NOT rotate the session id)
# =============================================================================
say "S2 reset agent:main:main"
run agent --agent main --message "baseline three" --json > "$W/s2-turn3.json"
run gateway call sessions.reset --params '{"key":"agent:main:main","reason":"reset"}' --json --port "$PORT" > "$W/s2-reset1.json"
run agent --agent main --message "after reset one" --json > "$W/s2-turn4.json"
run gateway call sessions.reset --params '{"key":"agent:main:main","reason":"new"}' --json --port "$PORT" > "$W/s2-reset2.json"
run agent --agent main --message "after reset two" --json > "$W/s2-turn5.json"

# =============================================================================
# S3  fork the main key at a real user entry id
# =============================================================================
say "S3 fork agent:main:main"
run gateway call chat.history --params '{"sessionKey":"agent:main:main","limit":50}' --json --port "$PORT" > "$W/s3-history.json"
ENTRY_ID=$("$NODE" -e '
const d=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));
const ids=d.messages.filter(m=>m.role==="user"&&m.__openclaw&&m.__openclaw.id).map(m=>m.__openclaw.id);
if(!ids.length){console.error("no user entry id in chat.history");process.exit(1);}
process.stdout.write(ids[ids.length-1]);' "$W/s3-history.json")
echo "entryId=$ENTRY_ID"
run gateway call sessions.fork --params "{\"sessionKey\":\"agent:main:main\",\"entryId\":\"$ENTRY_ID\"}" --json --port "$PORT" > "$W/s3-fork.json"
FORK_KEY=$("$NODE" -e '
process.stdout.write(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).sessionKey);' "$W/s3-fork.json")
echo "forkKey=$FORK_KEY"
run agent --agent main --session-key "$FORK_KEY" --message "fork turn one" --json > "$W/s3-forkturn.json"

# =============================================================================
# S4  compaction with truncation on its own key (it DELETES transcript rows,
#     so keep it off the main key whose reset-delimited history we want intact)
# =============================================================================
say "S4 compaction on agent:main:dashboard:compactme"
for i in 1 2 3; do
  run agent --agent main --session-key "agent:main:dashboard:compactme" --message "compact fodder $i" --json > "$W/s4-turn$i.json"
done
run sessions compact "agent:main:dashboard:compactme" --max-lines 5 --json > "$W/s4-compact.json"

# =============================================================================
# S5  one cron job, run twice -> two session_windows rows on one key
# =============================================================================
say "S5 cron job run twice"
run cron add --name alpha --every 1h --message "alpha job message" \
  --session isolated --no-deliver --agent main --json > "$W/s5-cronadd.json"
JOB=$("$NODE" -e 'process.stdout.write(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).id);' "$W/s5-cronadd.json")
echo "jobId=$JOB"
run cron run "$JOB" --wait --wait-timeout 180s --json > "$W/s5-cronrun1.json"
run cron run "$JOB" --wait --wait-timeout 180s --json > "$W/s5-cronrun2.json"

# =============================================================================
# S6  idle rollover -> the ONLY path that chains previous_session_id
# =============================================================================
say "S6 idle rollover on agent:main:dashboard:rollover (needs a >60s gap)"
run agent --agent main --session-key "agent:main:dashboard:rollover" --message "rollover A" --json > "$W/s6-a.json"
sleep 70
run agent --agent main --session-key "agent:main:dashboard:rollover" --message "rollover B" --json > "$W/s6-b.json"

# =============================================================================
# S7  hard delete -> session_transcript_archives row + the archive FILE
#     soft archive -> session_nodes.archived_at only
# =============================================================================
say "S7 delete + soft-archive"
run agent --agent main --session-key "agent:main:dashboard:doomed-delete" --message "doomed delete turn" --json > "$W/s7-del.json"
run agent --agent main --session-key "agent:main:dashboard:doomed-archive" --message "doomed archive turn" --json > "$W/s7-arc.json"
run sessions delete "agent:main:dashboard:doomed-delete" --yes --json > "$W/s7-delete.json"
run sessions archive "agent:main:dashboard:doomed-archive" --json > "$W/s7-archive.json"

# =============================================================================
# S8  one heartbeat run (cheap extra; non-fatal if the beat does not land)
# =============================================================================
say "S8 heartbeat"
mkdir -p "$H/.openclaw/workspace"
cat > "$H/.openclaw/workspace/HEARTBEAT.md" <<'MD'
Check whether anything needs attention. Keep the answer to one short sentence.
There is nothing scheduled right now, so reply that everything is quiet.
MD
run system event --text "capture heartbeat probe" --mode now --json > "$W/s8-event.json" 2>&1 || echo "system event failed (non-fatal)"
for _ in $(seq 1 12); do
  n=$("$NODE" -e '
const {DatabaseSync}=require("node:sqlite");
try{const db=new DatabaseSync(process.argv[1],{readOnly:true});
process.stdout.write(String(db.prepare("SELECT count(*) c FROM session_nodes WHERE session_key LIKE \"%heartbeat%\"").get().c));db.close();}
catch(e){process.stdout.write("0");}' "$H/.openclaw/agents/main/agent/openclaw-agent.sqlite" 2>/dev/null || echo 0)
  [ "${n:-0}" -gt 0 ] && { echo "heartbeat session present"; break; }
  sleep 5
done
# node:sqlite readOnly opens recreate -wal/-shm; the gateway is still live so leave them,
# the fixture builder checkpoints them away.

say "stop gateway"
stop_gateway

say "capture complete: $H"
KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$NODE" "$KIT_DIR/q.mjs" "$H/.openclaw/agents/main/agent/openclaw-agent.sqlite" \
  "SELECT session_key, count(*) windows FROM session_windows GROUP BY 1" 2>/dev/null || true
ls -la "$H/.openclaw/agents/main/sessions/" 2>&1 || true
