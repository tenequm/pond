#!/usr/bin/env bash
# Pass `rotate-reply` - capture session rotation under one stable key.
#
# Produces, for the single key `agent:main:main`, six generations A..F rotated by
# every reply-path / gateway mechanism OpenClaw 2026.7.1-2 offers, plus a second
# key `agent:main:explicit:notes` rotated by the `oc agent` stale path.
#
# Idempotent: rm -rf's ONLY its own /tmp/openclaw-fixture/rotate-reply/capture-home.
# Writes nothing outside /tmp/openclaw-fixture/rotate-reply/ and this out dir.
#
# Runtime ~6 min (two mandatory 80s idle waits; session.reset.idleMinutes minimum is 1).

set -euo pipefail

OC=/tmp/openclaw-fixture/oc
NODE=/tmp/openclaw-fixture/node/bin/node
WORK=/tmp/openclaw-fixture/rotate-reply/capture-home
export OC_HOME="$WORK/home"
STATE="$OC_HOME/.openclaw"
SESSIONS="$STATE/agents/main/sessions"
CFG="$STATE/openclaw.json"
PORT=18810
STUB=http://127.0.0.1:18555/v1
KEY=agent:main:main
NOTES_KEY=agent:main:explicit:notes
TMUX_SESSION=oc-rotate-reply-capture

say() { printf '\n=== %s ===\n' "$*"; }

# --- gateway lifecycle -------------------------------------------------------
# The gateway renames its process to `openclaw-gateway`, so pkill -f <path> does
# NOT find it. Find the listener on our port and kill it only after confirming
# /proc/<pid>/environ has HOME= our sandbox home.
gw_pid() {
  (
    set +o pipefail
    ss -ltnp 2>/dev/null | awk -v p=":$PORT\$" '$4 ~ p {print}' \
      | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2
  )
}

gw_stop() {
  local pid
  pid=$(gw_pid || true)
  [ -n "${pid:-}" ] || return 0
  if tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -qx "HOME=$OC_HOME"; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do gw_pid | grep -q . || break; sleep 0.5; done
    echo "[gw] stopped pid=$pid"
  else
    echo "[gw] REFUSING to kill pid=$pid on port $PORT: HOME is not $OC_HOME" >&2
    return 1
  fi
}

# gw_start [extra env assignments...]
gw_start() {
  ( cd "$WORK" && env "$@" OC_HOME="$OC_HOME" nohup "$OC" gateway run --port "$PORT" \
      >> "$WORK/gateway.log" 2>&1 & )
  for _ in $(seq 1 40); do
    ss -ltn 2>/dev/null | grep -q ":$PORT" && { echo "[gw] up ${*:-(trajectory on)}"; sleep 2; return 0; }
    sleep 1
  done
  echo "[gw] FAILED to start" >&2; return 1
}

cleanup() {
  tmux kill-session -t "$TMUX_SESSION" 2>/dev/null || true
  gw_stop || true
}
trap cleanup EXIT INT TERM

# --- helpers -----------------------------------------------------------------
# Monotonic count of persisted assistant messages across the whole sessions dir.
# Archives keep their messages, so this only ever grows -> a safe turn barrier.
# `set -o pipefail` is on, and both the glob-less cat and a no-match grep exit
# non-zero, so run the whole thing with pipefail off inside a subshell.
assistant_count() {
  (
    set +o pipefail
    cat "$SESSIONS"/*.jsonl "$SESSIONS"/*.jsonl.reset.* 2>/dev/null \
      | grep -o '"role":"assistant"' | wc -l | tr -d ' '
  )
}

wait_for_turn() {
  local before="$1" label="$2"
  for _ in $(seq 1 90); do
    [ "$(assistant_count)" -gt "$before" ] && { sleep 2; return 0; }
    sleep 1
  done
  echo "[!] timed out waiting for turn: $label" >&2; return 1
}

# send <message> [extra env assignments before the CLI]
send() {
  local msg="$1"; shift || true
  local before; before=$(assistant_count)
  env "$@" OC_HOME="$OC_HOME" "$OC" gateway call chat.send \
    --params "$($NODE -e 'process.stdout.write(JSON.stringify({sessionKey:process.argv[1],message:process.argv[2],idempotencyKey:process.argv[3]}))' \
        "$KEY" "$msg" "cap-$(printf '%s' "$msg" | tr -c 'a-zA-Z0-9' '-')")" \
    --json --timeout 60000 > /dev/null
  wait_for_turn "$before" "$msg"
  echo "[send] $msg"
}

# tui_slash </new|/reset> - the ONLY way to drive a reply-path slash command:
#   * oc agent treats /new as literal text (no rotation);
#   * oc gateway call chat.send is silently unauthorized (needs operator.admin
#     in the connection scopes - commands-reset.ts:123 drops it with no reply);
#   * the TUI's interactive /new is intercepted client-side into sessions.create,
#     but --message bypasses that interception and goes through chat.send.
tui_slash() {
  local cmd="$1"
  local before; before=$(assistant_count)
  tmux kill-session -t "$TMUX_SESSION" 2>/dev/null || true
  tmux new-session -d -s "$TMUX_SESSION" -x 200 -y 50 \
    "OC_HOME='$OC_HOME' '$OC' tui --session '$KEY' --message '$cmd' >> '$WORK/tui.log' 2>&1"
  wait_for_turn "$before" "tui $cmd"
  tmux kill-session -t "$TMUX_SESSION" 2>/dev/null || true
  sleep 2
  echo "[tui] $cmd"
}

set_idle_policy() {  # set_idle_policy on|off
  "$NODE" -e '
    const fs = require("fs"), [p, mode] = process.argv.slice(1);
    const c = JSON.parse(fs.readFileSync(p, "utf8"));
    if (mode === "on") c.session = { reset: { mode: "idle", idleMinutes: 1 } };
    else delete c.session;
    fs.writeFileSync(p, JSON.stringify(c, null, 2) + "\n");
  ' "$CFG" "$1"
  echo "[cfg] idle reset policy $1"
}

# snap <label> - append the lineage-relevant slice of sessions.json to
# lineage.jsonl. The final sessions.json cannot show this: the two gateway
# sessions.reset calls at the end wipe usageFamilyKey/usageFamilySessionIds, so
# without these snapshots there is no on-disk evidence that they were ever set.
snap() {
  "$NODE" -e '
    const fs = require("fs"), path = require("path");
    const [storeFile, out, label] = process.argv.slice(1);
    const store = fs.existsSync(storeFile) ? JSON.parse(fs.readFileSync(storeFile, "utf8")) : {};
    const entries = Object.fromEntries(Object.entries(store).map(([k, v]) => [k, {
      sessionId: v.sessionId,
      usageFamilyKey: v.usageFamilyKey ?? null,
      usageFamilySessionIds: v.usageFamilySessionIds ?? null,
      sessionFile: v.sessionFile ? path.basename(v.sessionFile) : null,
    }]));
    fs.appendFileSync(out, JSON.stringify({ at: label, entries }) + "\n");
  ' "$SESSIONS/sessions.json" "$WORK/lineage.jsonl" "$1"
  echo "[snap] $1"
}

live_id() {
  "$NODE" -e '
    const s = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
    process.stdout.write(s[process.argv[2]]?.sessionId ?? "");
  ' "$SESSIONS/sessions.json" "$1"
}

# --- 0. fresh home -----------------------------------------------------------
say "0. fresh capture home"
curl -sf -m 5 "$STUB/models" > /dev/null || { echo "stub LLM at $STUB is down" >&2; exit 1; }
rm -rf "$WORK"
mkdir -p "$STATE"
cat > "$CFG" <<JSON
{
  "models": { "mode": "merge", "providers": { "stub": {
    "baseUrl": "$STUB", "apiKey": "stub-not-a-secret",
    "api": "openai-completions",
    "models": [{ "id": "stub-model", "name": "Stub", "contextWindow": 200000, "maxTokens": 1024 }]
  } } },
  "logging": { "file": "$WORK/openclaw.log" },
  "gateway": { "mode": "local", "port": $PORT, "bind": "loopback", "auth": { "mode": "none" } },
  "agents": { "defaults": { "model": { "primary": "stub/stub-model" } } }
}
JSON

# --- 1. gateway + admin pairing ---------------------------------------------
say "1. gateway up, approve the operator.admin scope upgrade"
gw_start
# Admin RPCs need the operator.admin device scope even with auth mode none. The
# first admin call fails with `pairing required` and registers a request; approve
# it inside the sandbox, then admin calls work for the rest of the run. Done here
# with a harmless read-only admin method so no session is touched.
# NB the "pairing required (requestId: ...)" text arrives on stdout, not stderr.
"$OC" gateway call config.schema --json > "$WORK/pair.out" 2>&1 || true
REQ=$( (set +o pipefail; grep -oE 'requestId: [0-9a-f-]{36}' "$WORK/pair.out" | head -1 | awk '{print $2}') || true)
if [ -n "${REQ:-}" ]; then
  "$OC" devices approve "$REQ" > /dev/null 2>&1 || true
  echo "[pair] approved $REQ"
else
  echo "[pair] no scope upgrade requested (already paired?)"
fi
"$OC" gateway call config.schema --json > /dev/null

# --- 2. generation A -> /new -------------------------------------------------
say "2. generation A (2 turns) -> rotated by reply-path /new"
send "generation A turn one"
send "generation A turn two"
GEN_A=$(live_id "$KEY"); echo "[gen] A = $GEN_A"
snap "gen-A-live-before-slash-new"
tui_slash "/new"
snap "after-reply-path-slash-new (A archived, B live)"

# --- 3. generation B -> /reset ----------------------------------------------
say "3. generation B (2 turns) -> rotated by reply-path /reset"
send "generation B turn one"
send "generation B turn two"
GEN_B=$(live_id "$KEY"); echo "[gen] B = $GEN_B"
tui_slash "/reset"
snap "after-reply-path-slash-reset (B archived, C live)"

# --- 4. generation C -> idle reset ------------------------------------------
say "4. idle policy on; generation C (2 turns) + second key; wait out the idle window"
gw_stop
set_idle_policy on
gw_start
send "generation C turn one"
send "generation C turn two"
GEN_C=$(live_id "$KEY"); echo "[gen] C = $GEN_C"

# Second key, on the `oc agent` path (no gateway involved). Two turns now, one
# after the idle window -> stale rotation via agents/command/session.ts.
"$OC" agent --local --agent main --session-key "$NOTES_KEY" --message "notes key turn one" --json > /dev/null
"$OC" agent --local --agent main --session-key "$NOTES_KEY" --message "notes key turn two" --json > /dev/null
NOTES_OLD=$(live_id "$NOTES_KEY"); echo "[gen] notes old = $NOTES_OLD"

echo "[wait] 80s for the idle window (session.reset.idleMinutes=1)"
sleep 80

# --- 5. generation D (born from the idle rollover) --------------------------
say "5. idle rollover C -> D, then D turn two; second key stale rotation"
send "generation D turn one after idle"      # this turn performs the C->D rollover
send "generation D turn two"                 # keep the gap < 60s or D rotates too
GEN_D=$(live_id "$KEY"); echo "[gen] D = $GEN_D"
snap "after-idle-reset (C archived, D live)"
"$OC" agent --local --agent main --session-key "$NOTES_KEY" --message "notes key turn three after idle" --json > /dev/null
NOTES_NEW=$(live_id "$NOTES_KEY"); echo "[gen] notes new = $NOTES_NEW"
snap "after-oc-agent-stale-rotation (notes key)"

# --- 6. generation E: trajectories disabled for its whole life --------------
say "6. idle policy off; gateway restarted with OPENCLAW_TRAJECTORY=0"
gw_stop
set_idle_policy off
gw_start OPENCLAW_TRAJECTORY=0
# Rotate D->E under the trajectory-off gateway so E is born without sidecars.
OC_HOME="$OC_HOME" OPENCLAW_TRAJECTORY=0 "$OC" gateway call sessions.reset \
  --params "{\"key\":\"$KEY\",\"reason\":\"reset\"}" --json --timeout 60000 > /dev/null
sleep 3
echo "[rot] D archived by gateway sessions.reset"
snap "after-gateway-sessions.reset (D archived, E live)"
send "generation E turn one no trajectory" OPENCLAW_TRAJECTORY=0
send "generation E turn two no trajectory" OPENCLAW_TRAJECTORY=0
GEN_E=$(live_id "$KEY"); echo "[gen] E = $GEN_E"
snap "gen-E-live-before-second-gateway-reset"

# --- 7. generation F: live, trajectories back on ----------------------------
say "7. rotate E -> F by gateway sessions.reset, then restart with trajectories on"
OC_HOME="$OC_HOME" OPENCLAW_TRAJECTORY=0 "$OC" gateway call sessions.reset \
  --params "{\"key\":\"$KEY\",\"reason\":\"reset\"}" --json --timeout 60000 > /dev/null
sleep 3
gw_stop
gw_start
send "generation F turn one live"
send "generation F turn two live"
GEN_F=$(live_id "$KEY"); echo "[gen] F = $GEN_F"
snap "final (E archived, F live)"

# --- 8. done -----------------------------------------------------------------
say "8. shutting down"
gw_stop

cat > "$WORK/generations.json" <<JSON
{
  "$KEY": {
    "A_rotated_by": "reply-path /new (tui --message)",       "A": "$GEN_A",
    "B_rotated_by": "reply-path /reset (tui --message)",     "B": "$GEN_B",
    "C_rotated_by": "idle reset (session.reset idleMinutes=1)", "C": "$GEN_C",
    "D_rotated_by": "gateway sessions.reset RPC",            "D": "$GEN_D",
    "E_rotated_by": "gateway sessions.reset RPC, OPENCLAW_TRAJECTORY=0 for its whole life", "E": "$GEN_E",
    "F_rotated_by": "none - left live",                      "F": "$GEN_F"
  },
  "$NOTES_KEY": {
    "old_rotated_by": "oc agent stale/idle rotation (not archived)", "old": "$NOTES_OLD",
    "new_rotated_by": "none - left live",                            "new": "$NOTES_NEW"
  }
}
JSON

say "capture complete"
ls -la "$SESSIONS" | grep -v skills-prompts
echo
echo "generations: $WORK/generations.json"
