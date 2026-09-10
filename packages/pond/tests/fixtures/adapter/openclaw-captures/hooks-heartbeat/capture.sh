#!/usr/bin/env bash
# hooks-heartbeat capture: hook sessions (keyless + fixed key), isolated heartbeat.
# Idempotent from an empty home; wipes ONLY its own run dir under the pass work area.
#
#   bash capture.sh            # -> /tmp/openclaw-fixture/hooks-heartbeat/capture-home
#   RUN=rehearsal bash capture.sh
set -euo pipefail

PASS=hooks-heartbeat
WORK=/tmp/openclaw-fixture/$PASS
RUN=${RUN:-capture-home}
RUN_DIR=$WORK/$RUN
OC_HOME=$RUN_DIR/home
STATE=$OC_HOME/.openclaw
SESSIONS=$STATE/agents/main/sessions
OC=/tmp/openclaw-fixture/oc
NODE=/tmp/openclaw-fixture/node/bin/node
STUB_URL=http://127.0.0.1:18555/v1
GW_PORT=18870
HOOK_TOKEN=sandbox-hook-token-not-a-secret
export OC_HOME

case "$RUN_DIR" in /tmp/openclaw-fixture/hooks-heartbeat/*) ;; *) echo "refusing to wipe $RUN_DIR" >&2; exit 2 ;; esac

GW_PID=""
cleanup() {
  # The gateway renames itself to "openclaw-gateway"; find it by our port and
  # verify HOME= our sandbox home before killing.
  local pid
  pid=$(ss -ltnp 2>/dev/null | grep -F "127.0.0.1:$GW_PORT" | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2 || true)
  if [ -n "${pid:-}" ] && tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -qx "HOME=$OC_HOME"; then
    kill "$pid" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$pid" 2>/dev/null || true
  fi
  [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null || true
  return 0
}
trap cleanup EXIT INT TERM

say() { printf '\n=== %s (t+%ss) ===\n' "$1" "$(( $(date +%s) - T0 ))"; }
T0=$(date +%s)

curl -sf -m 5 "$STUB_URL/models" >/dev/null || { echo "stub LLM not reachable on $STUB_URL" >&2; exit 3; }

say "fresh home $RUN_DIR"
rm -rf "$RUN_DIR"
mkdir -p "$STATE/workspace"

cat > "$STATE/openclaw.json" <<JSON
{
  "models": { "mode": "merge", "providers": { "stub": {
    "baseUrl": "$STUB_URL", "apiKey": "stub-not-a-secret",
    "api": "openai-completions",
    "models": [{ "id": "stub-model", "name": "Stub", "contextWindow": 200000, "maxTokens": 1024 }] } } },
  "logging": { "file": "$RUN_DIR/openclaw.log" },
  "agents": { "defaults": {
    "model": { "primary": "stub/stub-model" },
    "heartbeat": { "every": "1m", "isolatedSession": true, "target": "none",
                   "prompt": "Report status briefly." } } },
  "hooks": { "enabled": true, "token": "$HOOK_TOKEN", "path": "/hooks",
             "allowRequestSessionKey": true, "allowedSessionKeyPrefixes": ["hook:"] },
  "gateway": { "mode": "local", "port": $GW_PORT, "bind": "loopback", "auth": { "mode": "none" } }
}
JSON

# Heartbeat is skipped unless HEARTBEAT.md holds prose and no `tasks:` block.
cat > "$STATE/workspace/HEARTBEAT.md" <<'MD'
# Heartbeat

Sandbox fixture heartbeat. On each beat, report status briefly and take no
other action. There is nothing to schedule and nothing to deliver.
MD

"$OC" config validate

say "scenario 0: ordinary local turn (contrast row)"
"$OC" agent --local --agent main --message "hello from the ordinary local turn" --json | tail -5

say "start gateway on $GW_PORT"
"$OC" gateway run --port "$GW_PORT" > "$RUN_DIR/gateway.out" 2>&1 &
GW_PID=$!
for _ in $(seq 1 60); do
  curl -sf -m 2 "http://127.0.0.1:$GW_PORT/hooks/agent" -o /dev/null -X OPTIONS 2>/dev/null && break
  ss -ltn 2>/dev/null | grep -qF "127.0.0.1:$GW_PORT" && break
  sleep 0.5
done
ss -ltn | grep -F "127.0.0.1:$GW_PORT" || { echo "gateway did not listen" >&2; exit 4; }

post_hook() { # $1 = json body
  curl -s -m 30 -X POST "http://127.0.0.1:$GW_PORT/hooks/agent" \
    -H "Authorization: Bearer $HOOK_TOKEN" -H 'Content-Type: application/json' \
    -d "$1"; echo
}

say "scenario 1: two keyless hook posts"
post_hook '{"message":"keyless hook one","name":"keyless-one","deliver":false,"wakeMode":"next-heartbeat"}'
sleep 8
post_hook '{"message":"keyless hook two","name":"keyless-two","deliver":false,"wakeMode":"next-heartbeat"}'
sleep 8

say "scenario 2: two hook posts under a fixed sessionKey hook:ingress"
post_hook '{"message":"ingress hook one","name":"ingress-one","sessionKey":"hook:ingress","deliver":false,"wakeMode":"next-heartbeat"}'
sleep 8
post_hook '{"message":"ingress hook two","name":"ingress-two","sessionKey":"hook:ingress","deliver":false,"wakeMode":"next-heartbeat"}'
sleep 8

say "scenario 3: wait for 2 natural heartbeats (every 1m)"
# Count distinct isolated-heartbeat transcripts by trajectory sidecar key.
# NOTE: `|| true` is load-bearing under `set -o pipefail` - grep exits 1 until
# the first beat lands, which would otherwise abort the whole capture.
beats() { grep -l '"sessionKey":"agent:main:main:heartbeat"' "$SESSIONS"/*.trajectory.jsonl 2>/dev/null | wc -l || true; }
seen=0
for _ in $(seq 1 120); do
  seen=$(beats)
  [ "$seen" -ge 2 ] && break
  sleep 2
done
echo "natural heartbeat transcripts: $seen"

say "scenario 3b: forced beat"
"$OC" system event --text "forced wake probe" --mode now --json
n=$seen
for _ in $(seq 1 60); do
  n=$(beats)
  [ "$n" -ge $((seen + 1)) ] && break
  sleep 2
done
echo "heartbeat transcripts after forced beat: $n"
sleep 5

say "stop gateway"
cleanup
trap - EXIT INT TERM

say "sessions dir"
ls -1 "$SESSIONS"
"$NODE" -e '
const fs=require("fs"),p=process.argv[1];
const s=JSON.parse(fs.readFileSync(p+"/sessions.json","utf8"));
for(const [k,v] of Object.entries(s.sessions??s)) console.log(k,"->",v.sessionId,"| base:",v.heartbeatIsolatedBaseSessionKey??"-");
' "$SESSIONS"
say "done"
