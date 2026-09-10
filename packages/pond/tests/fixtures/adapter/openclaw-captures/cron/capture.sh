#!/usr/bin/env bash
# Pass `cron` capture: isolated cron runs, the cron run log, and the session reaper.
#
# Produces a fresh OpenClaw state tree under /tmp/openclaw-fixture/cron/capture-home/home
# containing, for one agent (`main`):
#   - 3 isolated runs of job alpha  (only the last is referenced by sessions.json)
#   - 4 isolated runs of job beta   (run 3 written with OPENCLAW_TRAJECTORY=0, then orphaned
#                                    by run 4 -> resolvable only via cron_run_logs)
#   - 2 main-target runs of job gamma (each mints its own `:run:<ms>` sessions.json key,
#                                    later pruned by the reaper into `.deleted.` archives)
#   - 2 runs of job delta with an explicit `session:<key>` target
#   - 1 run of job epsilon with --delete-after-run (job removes itself)
#   - 1 SCHEDULED run of job zeta (--every 1m) -> the only row with run_id NULL
#
# Idempotent: wipes and rebuilds its own capture-home. Writes nothing outside
# /tmp/openclaw-fixture/cron/capture-home and this directory.
#
# Requires: /tmp/openclaw-fixture/oc + node runtime (see ../../setup.sh) and the shared
# stub model server on 127.0.0.1:18555 (../../stub-llm.mjs).
set -euo pipefail

RT=/tmp/openclaw-fixture
WORK=$RT/cron/capture-home
OC_HOME=$WORK/home
OC=$RT/oc
NODE=$RT/node/bin/node
PORT=18830
STUB=http://127.0.0.1:18555/v1
export OC_HOME

command -v ss >/dev/null || { echo "need iproute2 'ss'" >&2; exit 1; }
[ -x "$OC" ] || { echo "missing $OC - run setup.sh" >&2; exit 1; }
curl -fsS -m 5 "$STUB/models" >/dev/null || { echo "stub model server not up on $STUB" >&2; exit 1; }

# --- gateway lifecycle ------------------------------------------------------
# The gateway renames its process to `openclaw-gateway`, so pkill -f <path> misses it.
# Find the listener on our port and only kill it once /proc/<pid>/environ proves it is ours.
gateway_pid() {
  ss -ltnp 2>/dev/null | grep -m1 ":$PORT " | sed -n 's/.*pid=\([0-9]*\).*/\1/p'
}
gateway_stop() {
  local pid; pid=$(gateway_pid) || true
  [ -n "${pid:-}" ] || return 0
  case "$(tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -m1 '^HOME=')" in
    HOME=$OC_HOME) : ;;
    *) echo "refusing to kill pid $pid on :$PORT - not our sandbox home" >&2; return 1 ;;
  esac
  kill "$pid"
  for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ":$PORT " || return 0; sleep 1; done
  echo "gateway pid $pid did not exit" >&2; return 1
}
gateway_start() { # gateway_start [extra env assignments...]
  ( cd "$WORK" && env "$@" OC_HOME="$OC_HOME" nohup "$OC" gateway run --port "$PORT" \
      >> "$WORK/gateway.out" 2>&1 & )
  for _ in $(seq 1 60); do ss -ltn 2>/dev/null | grep -q ":$PORT " && { sleep 2; return 0; }; sleep 1; done
  echo "gateway failed to listen on :$PORT" >&2; return 1
}
trap gateway_stop EXIT

# --- helpers ----------------------------------------------------------------
# `cron add` prints a "No --agent specified" notice before the JSON; parse from the first brace.
job_add() { "$OC" cron add "$@" --json 2>/dev/null | "$NODE" -e \
  'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s.slice(s.indexOf("{"))).id))'; }
job_run() { "$OC" cron run "$1" --wait --wait-timeout 120s >/dev/null 2>&1 || true; }

# --- fresh home -------------------------------------------------------------
case "$WORK" in /tmp/openclaw-fixture/cron/capture-home) rm -rf "$WORK" ;; *) exit 1 ;; esac
mkdir -p "$OC_HOME/.openclaw"

cat > "$OC_HOME/.openclaw/openclaw.json" <<EOF
{
  "models": { "mode": "merge", "providers": { "stub": {
    "baseUrl": "$STUB", "apiKey": "stub-not-a-secret",
    "api": "openai-completions",
    "models": [{ "id": "stub-model", "name": "Stub", "contextWindow": 200000, "maxTokens": 1024 }]
  } } },
  "logging": { "file": "$WORK/openclaw.log" },
  "gateway": { "mode": "local", "port": $PORT, "bind": "loopback", "auth": { "mode": "none" } },
  "cron": { "sessionRetention": "1m" },
  "agents": { "defaults": { "model": { "primary": "stub/stub-model" } } }
}
EOF

echo "== starting gateway (trajectories on)"
gateway_start

# --- jobs -------------------------------------------------------------------
# gamma must use --system-event: `sessionTarget: main` rejects agentTurn payloads
# (cron/service/jobs.ts:308), and --no-deliver is rejected for main targets.
ALPHA=$(job_add --name alpha --every 1h --message "alpha job message" --session isolated --no-deliver)
BETA=$( job_add --name beta  --every 1h --message "beta job message"  --session isolated --no-deliver)
GAMMA=$(job_add --name gamma --every 1h --system-event "gamma system event" --session main)
DELTA=$(job_add --name delta --every 1h --message "delta job message" --session "session:agent:main:custom-delta" --no-deliver)
echo "== jobs: alpha=$ALPHA beta=$BETA gamma=$GAMMA delta=$DELTA"

echo "== alpha x3 (isolated; only run 3 stays in sessions.json)"
job_run "$ALPHA"; job_run "$ALPHA"; job_run "$ALPHA"

echo "== beta x2 (isolated)"
job_run "$BETA"; job_run "$BETA"

echo "== gamma x2 (main target; each run mints its own :run:<ms> key)"
job_run "$GAMMA"; job_run "$GAMMA"
GAMMA_DONE_AT=$(date +%s)   # reaper cutoff reference: entries must age past sessionRetention

echo "== delta x2 (explicit session:<key> target)"
job_run "$DELTA"; job_run "$DELTA"

echo "== beta run 3 with OPENCLAW_TRAJECTORY=0 (no sidecar written)"
gateway_stop
gateway_start OPENCLAW_TRAJECTORY=0
job_run "$BETA"

echo "== beta run 4 with trajectories restored (orphans run 3 from sessions.json)"
gateway_stop
gateway_start
job_run "$BETA"

echo "== epsilon x1 (--delete-after-run: the job deletes itself, transcript stays)"
# --at "+1h" (not a few seconds): a soon-due one-shot arms the cron timer, and an early tick
# would burn the reaper's 5-minute throttle before the gamma entries age past retention.
EPSILON=$(job_add --name epsilon --at "+1h" --message "epsilon one-shot message" --session isolated --no-deliver --delete-after-run)
job_run "$EPSILON"

# --- scheduled run + reaper sweep -------------------------------------------
# sweepCronRunSessions is only called from the cron timer tick (cron/service/timer.ts:1587),
# self-throttled to one sweep per 5 min per store. Every other job is an hour out, so add a
# 1-minute job: its tick both produces the one SCHEDULED run (run_id NULL) and runs the reaper.
# By then the gamma entries are older than the 1m retention, so they get pruned + archived.
echo "== zeta --every 1m: waiting for one scheduled run and the reaper sweep"
while [ $(( $(date +%s) - GAMMA_DONE_AT )) -lt 70 ]; do sleep 5; done  # outlive sessionRetention=1m
ZETA=$(job_add --name zeta --every 1m --message "zeta scheduled message" --session isolated --no-deliver)
for _ in $(seq 1 90); do
  grep -q 'cron-reaper: pruned' "$WORK/openclaw.log" 2>/dev/null && break
  sleep 2
done
grep -q 'cron-reaper: pruned' "$WORK/openclaw.log" \
  && echo "== reaper swept: $(grep -c 'cron-reaper: pruned' "$WORK/openclaw.log") sweep(s)" \
  || echo "!! reaper did not sweep within 180s" >&2
"$OC" cron rm "$ZETA" >/dev/null 2>&1 || true

echo "== stopping gateway"
gateway_stop
trap - EXIT

echo "== capture complete: $OC_HOME/.openclaw/agents/main/sessions"
ls -1 "$OC_HOME/.openclaw/agents/main/sessions" | grep -v '^skills-prompts$'
