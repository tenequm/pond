#!/usr/bin/env bash
# =============================================================================
# OpenClaw fixture capture -- pass `compaction`
#
# Captures compaction successors, checkpoint branches and lineage paths from a
# real OpenClaw 2026.7.1-2 install driven by a deterministic loopback stub model.
#
# Idempotent: wipes and recreates ONLY /tmp/openclaw-fixture/compaction/capture-home.
# Writes nothing outside that dir and this script's own out/ dir.
#
# Scenarios (see out/compaction/REPORT.md):
#   Phase A -- truncateAfterCompaction = true
#     S1  agent:main:main                 3 turns -> compact -> 2 turns -> compact  (3 generations)
#     S3  agent:main:dashboard:<uuid>     sessions.compaction.branch off S1's FIRST checkpoint
#     S4  agent:main:explicit:auto        3 turns on stub-small -> AUTOMATIC overflow compaction
#   Phase B -- truncateAfterCompaction = false
#     S2  agent:main:explicit:notes       3 turns -> compact (in-place, no successor file)
# =============================================================================
set -euo pipefail

RT=/tmp/openclaw-fixture
WORK="$RT/compaction/capture-home"
H="$WORK/home"
SESSIONS="$H/.openclaw/agents/main/sessions"
OC="$RT/oc"
NODE="$RT/node/bin/node"
PORT=18850
STUB_PORT=18555          # shared stub
SPARE_STUB_PORT=18558    # this pass's own stub, used only if the shared one is down
STUB_PID=""

log() { printf '\n=== %s ===\n' "$*"; }
run() { timeout 300 env OC_HOME="$H" "$OC" "$@"; }

# --- teardown ----------------------------------------------------------------
# The gateway renames itself to `openclaw-gatewa`, so pkill -f on our path misses
# it. Find the listener on OUR port and kill it only if its HOME is our sandbox.
stop_gateway() {
  local pid
  pid=$(ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1 || true)
  if [ -n "$pid" ] && [ -r "/proc/$pid/environ" ] && grep -qz "HOME=$H" "/proc/$pid/environ"; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do ss -ltn 2>/dev/null | grep -q ":$PORT " || break; sleep 1; done
    echo "gateway $pid stopped"
  fi
}
cleanup() {
  stop_gateway
  [ -n "$STUB_PID" ] && kill "$STUB_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

start_gateway() {
  nohup env OC_HOME="$H" "$OC" gateway run --port "$PORT" >>"$WORK/gateway.out" 2>&1 &
  for _ in $(seq 1 60); do
    ss -ltn 2>/dev/null | grep -q ":$PORT " && { echo "gateway listening on $PORT"; return 0; }
    sleep 1
  done
  echo "gateway failed to listen on $PORT" >&2; tail -20 "$WORK/gateway.out" >&2; exit 1
}

# --- model provider ----------------------------------------------------------
STUB_URL="http://127.0.0.1:$STUB_PORT/v1"
if ! curl -fsS -m 5 "$STUB_URL/models" >/dev/null 2>&1; then
  echo "shared stub down; starting our own on $SPARE_STUB_PORT"
  mkdir -p "$WORK"
  nohup "$NODE" "$RT/stub-llm.mjs" "$SPARE_STUB_PORT" >>"$WORK/stub.out" 2>&1 &
  STUB_PID=$!
  STUB_URL="http://127.0.0.1:$SPARE_STUB_PORT/v1"
  for _ in $(seq 1 30); do curl -fsS -m 2 "$STUB_URL/models" >/dev/null 2>&1 && break; sleep 1; done
fi
echo "stub: $STUB_URL"

# --- fresh home --------------------------------------------------------------
rm -rf "$WORK"
mkdir -p "$H/.openclaw"

# `truncateAfterCompaction` is the only key that differs between phases, so the
# config is rewritten (and the gateway restarted) at the phase boundary.
write_config() { # $1 = true|false
  cat > "$H/.openclaw/openclaw.json" <<JSON
{
  "models": { "mode": "merge", "providers": { "stub": {
    "baseUrl": "$STUB_URL", "apiKey": "stub-not-a-secret",
    "api": "openai-completions",
    "models": [
      { "id": "stub-model", "name": "Stub", "contextWindow": 200000, "maxTokens": 1024 },
      { "id": "stub-small", "name": "StubSmall", "contextWindow": 24000, "maxTokens": 1024 }
    ] } } },
  "logging": { "file": "$WORK/openclaw.log" },
  "gateway": { "mode": "local", "port": $PORT, "bind": "loopback", "auth": { "mode": "none" } },
  "agents": { "defaults": { "model": { "primary": "stub/stub-model" },
    "compaction": { "truncateAfterCompaction": $1, "memoryFlush": { "enabled": false } } } }
}
JSON
}

turn() { # $1 = session key, $2 = message, $3 = optional model override
  local key="$1" msg="$2" model="${3:-}"
  if [ -n "$model" ]; then
    run agent --local --agent main --session-key "$key" --model "$model" --message "$msg" --json >/dev/null
  else
    run agent --local --agent main --session-key "$key" --message "$msg" --json >/dev/null
  fi
  echo "turn ok: $key <- $msg"
}

# `sessions compact` needs operator.admin. With auth.mode=none on loopback the CLI
# self-registers an already-approved admin device, so this normally just works; the
# fallback below covers an install where pairing IS required.
compact() { # $1 = session key
  local key="$1" out rc=0 reqid
  out=$(run sessions compact "$key" --json 2>&1) || rc=$?
  if [ $rc -ne 0 ] && grep -qi 'pairing required' <<<"$out"; then
    echo "pairing required -> approving device"
    reqid=$(run devices list --json 2>/dev/null | "$NODE" -e \
      'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{const j=JSON.parse(d);console.log((j.pending?.[0]?.requestId)??"")})')
    [ -n "$reqid" ] && run devices approve "$reqid" >/dev/null
    out=$(run sessions compact "$key" --json)
  fi
  printf '%s\n' "$out" | tail -14
}

# =============================================================================
log "PHASE A -- truncateAfterCompaction = true"
write_config true

log "S1 agent:main:main -- 3 turns"
for i in 1 2 3; do turn "agent:main:main" "capture turn $i"; done

start_gateway

log "S1 -- compaction #1 (gen-1 -> gen-2)"
compact "agent:main:main"

log "S1 -- 2 more turns"
for i in 4 5; do turn "agent:main:main" "capture turn $i"; done

log "S1 -- compaction #2 (gen-2 -> gen-3)"
compact "agent:main:main"

# Checkpoints come back NEWEST-FIRST (session-compaction-checkpoints.ts:822-826),
# so S1's FIRST checkpoint is the LAST array element.
log "S3 -- branch off S1's FIRST checkpoint"
CP=$(run gateway call sessions.compaction.list \
       --params '{"key":"agent:main:main","agentId":"main"}' --json 2>/dev/null | "$NODE" -e \
     'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{const j=JSON.parse(d);console.log(j.checkpoints[j.checkpoints.length-1].checkpointId)})')
echo "first checkpointId: $CP"
run gateway call sessions.compaction.branch \
    --params "{\"key\":\"agent:main:main\",\"agentId\":\"main\",\"checkpointId\":\"$CP\"}" --json \
  | "$NODE" -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{const j=JSON.parse(d);console.log(JSON.stringify({ok:j.ok,sourceKey:j.sourceKey,key:j.key,sessionId:j.sessionId},null,2))})'

# contextWindow 24000 vs an ~11k-token workspace system prompt: every turn overflows
# the prompt budget, so the runner compacts automatically (checkpoint reason
# "overflow-retry") and the turns themselves come back livenessState=blocked. That is
# the intended shape here -- we want the AUTOMATIC compaction artifacts, not a reply.
log "S4 -- 3 turns on stub-small (automatic overflow compaction)"
for i in 1 2 3; do turn "agent:main:explicit:auto" "auto overflow turn $i" "stub/stub-small"; done

# =============================================================================
log "PHASE B -- truncateAfterCompaction = false"
stop_gateway
write_config false

log "S2 agent:main:explicit:notes -- 3 turns"
for i in 1 2 3; do turn "agent:main:explicit:notes" "notes turn $i"; done

start_gateway

log "S2 -- compaction (in place, no successor file expected)"
compact "agent:main:explicit:notes"

stop_gateway

# =============================================================================
log "RESULT -- $SESSIONS"
ls -la "$SESSIONS"
"$NODE" -e '
const fs=require("fs"),p=require("path");
const j=JSON.parse(fs.readFileSync(process.argv[1]+"/sessions.json","utf8"));
for(const [k,v] of Object.entries(j)){
  if(!v||typeof v!=="object"||!v.sessionId) continue;
  console.log(k,"->",JSON.stringify({sessionId:v.sessionId,file:v.sessionFile&&p.basename(v.sessionFile),
    parentSessionKey:v.parentSessionKey,label:v.label,usageFamilyKey:v.usageFamilyKey,
    usageFamilySessionIds:v.usageFamilySessionIds,
    checkpoints:(v.compactionCheckpoints||[]).map(c=>c.reason)}));
}' "$SESSIONS"
echo
echo "capture complete"
