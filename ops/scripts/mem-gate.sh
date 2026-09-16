#!/usr/bin/env bash
# Memory gate (#245): run every `mem_bench` scenario once. Two modes:
#
#   append (default): append one JSON line per scenario to
#     docs/benchmarks/mem-gate-baseline.jsonl and print the delta against the
#     previous row FOR THE SAME SCENARIO. No thresholds - a human reads the
#     delta while the fixes land (docs/plans/2609-16-memory-instrumentation.md).
#   --check: run the same scenarios but append nothing; compare each fresh row
#     against the LAST COMMITTED baseline row for (scenario, profile) and fail
#     if peak_rss_kb or peak_heap_bytes regressed more than
#     MEM_GATE_MAX_REGRESSION_PCT percent (default 20). CI shape: the baseline
#     is the contract, local `mem-gate` runs are how it gets new rows.
#
# One scenario per process invocation: peak RSS is a process-lifetime
# high-water mark, so two scenarios in one process cannot be told apart.
#
#   ops/scripts/mem-gate.sh                     # ci corpus, all scenarios
#   ops/scripts/mem-gate.sh --profile large     # 1M+ message corpus
#   ops/scripts/mem-gate.sh --check             # gate vs committed baseline
#   SCENARIOS="rowmap-build-cold" ops/scripts/mem-gate.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

PROFILE=ci
CHECK=0
while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --profile=*) PROFILE="${1#*=}"; shift ;;
    --check) CHECK=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

BASELINE="docs/benchmarks/mem-gate-baseline.jsonl"
SCENARIOS="${SCENARIOS:-sync-noop-local sync-incremental rowmap-build-cold mcp-query-growth ingest-large-session}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

MODE=append
if [ "$CHECK" = 1 ]; then MODE=check; fi
echo "=== mem gate: profile=$PROFILE mode=$MODE ==="

# Build once; every scenario then runs the same binary. `--message-format=json`
# is how the bench executable's path is learned - `cargo bench` would re-enter
# cargo per scenario and print its own noise into the row's stdout.
echo "--- build (release, --features mem-probe) ---"
cargo build --release --bench mem_bench --features mem-probe --message-format=json \
  > "$TMP/build.json" 2> "$TMP/build.err" || { cat "$TMP/build.err" >&2; exit 1; }
BENCH_BIN="$(python3 - "$TMP/build.json" <<'EOF'
import json, sys
path = None
for line in open(sys.argv[1]):
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        continue
    if msg.get("reason") == "compiler-artifact" and msg.get("executable") and msg.get("target", {}).get("name") == "mem_bench":
        path = msg["executable"]
if not path:
    sys.exit("no mem_bench executable in the cargo build output")
print(path)
EOF
)"
echo "binary: $BENCH_BIN"

# The corpus is generated once and cached under ~/.cache/pond-bench; building it
# inside a measured run would attribute the generator's allocations to the
# scenario.
echo "--- corpus ---"
"$BENCH_BIN" --prepare --profile "$PROFILE"

HOST_TAG="${MEM_GATE_HOST:-$(uname -s)-$(uname -m)}"

# Only append mode stamps a row, so only it needs the commit tag. A dirty tree
# means the measured binary may not match the named commit; the baseline this
# script appends to never affects the binary, so exclude it.
if [ "$CHECK" = 0 ]; then
  COMMIT="$(git rev-parse --short HEAD)"
  if [ -n "$(git status --porcelain -- ":!$BASELINE")" ]; then COMMIT="$COMMIT-dirty"; fi
  DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  mkdir -p "$(dirname "$BASELINE")"
  touch "$BASELINE"
fi
for scenario in $SCENARIOS; do
  echo "--- $scenario ---"
  "$BENCH_BIN" --scenario "$scenario" --profile "$PROFILE" > "$TMP/$scenario.json"
  if [ "$CHECK" = 1 ]; then
    python3 - "$TMP/$scenario.json" <<'EOF'
import json, sys
out = json.load(open(sys.argv[1]))
peak_rss = out.get("peak_rss_kb")
peak_heap = out.get("peak_heap_bytes")
print(
    "  wall {} ms  peak_rss {}  peak_heap {}".format(
        out.get("wall_ms"),
        f"{peak_rss / 1024:.1f} MiB" if peak_rss else "n/a",
        f"{peak_heap / 1048576:.1f} MiB" if peak_heap else "n/a",
    )
)
EOF
  else
    python3 - "$TMP/$scenario.json" "$BASELINE" "$DATE" "$COMMIT" "$HOST_TAG" <<'EOF'
import json, sys
row_file, baseline, date, commit, host = sys.argv[1:6]
row = json.load(open(row_file))
# Tags first so a row reads left-to-right as "when/what/where" then numbers.
out = {"date": date, "commit": commit, "host": host}
out.update(row)
with open(baseline, "a") as fh:
    fh.write(json.dumps(out) + "\n")
peak_rss = out.get("peak_rss_kb")
peak_heap = out.get("peak_heap_bytes")
print(
    "  wall {} ms  peak_rss {}  peak_heap {}".format(
        out.get("wall_ms"),
        f"{peak_rss / 1024:.1f} MiB" if peak_rss else "n/a",
        f"{peak_heap / 1048576:.1f} MiB" if peak_heap else "n/a",
    )
)
EOF
  fi
done

if [ "$CHECK" = 1 ]; then
  echo "--- check vs committed baseline (threshold ${MEM_GATE_MAX_REGRESSION_PCT:-20}%) ---"
  python3 - "$BASELINE" "$PROFILE" "$TMP" "${MEM_GATE_MAX_REGRESSION_PCT:-20}" "$SCENARIOS" "$HOST_TAG" <<'EOF'
import json, os, sys
baseline, profile, tmp, pct = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
scenarios, host = sys.argv[5].split(), sys.argv[6]
METRICS = ("peak_rss_kb", "peak_heap_bytes")
try:
    rows = [json.loads(line) for line in open(baseline) if line.strip()]
except FileNotFoundError:
    rows = []
failed = False
for scenario in scenarios:
    fresh = json.load(open(os.path.join(tmp, scenario + ".json")))
    # Host-scoped: peak RSS is a property of the machine as much as the code, so
    # a row from another box is not a threshold this one can be held to.
    same = [
        r
        for r in rows
        if r.get("scenario") == scenario
        and r.get("profile") == profile
        and r.get("host") == host
    ]
    print(f"\n[{scenario}] {profile} on {host}")
    if not same:
        print(f"  FAIL: no committed baseline row for host {host} - run ops/scripts/mem-gate.sh locally and commit the baseline")
        failed = True
        continue
    prev = same[-1]
    for key in METRICS:
        now, before = fresh.get(key), prev.get(key)
        if not isinstance(now, (int, float)) or not isinstance(before, (int, float)) or before <= 0:
            print(f"  skip {key}: not comparable (now={now!r}, baseline={before!r})")
            continue
        delta = (now - before) / before * 100
        verdict = "FAIL" if delta > pct else "ok"
        if delta > pct:
            failed = True
        print(f"  {verdict:<4} {key:<18} {before:>14} -> {now:<14} {delta:+.1f}%  (baseline {prev['date']} {prev['commit']})")
if failed:
    sys.exit(f"\nmem gate: regression beyond {pct:.0f}% (or missing baseline row)")
print(f"\nmem gate: all scenarios within {pct:.0f}% of the committed baseline")
EOF
else
  echo "--- delta vs previous run (same scenario, same profile) ---"
  python3 - "$BASELINE" "$PROFILE" "$SCENARIOS" <<'EOF'
import json, sys
baseline, profile, scenarios = sys.argv[1], sys.argv[2], sys.argv[3].split()
rows = [json.loads(line) for line in open(baseline) if line.strip()]
TAGS = ("date", "commit", "host", "scenario", "profile", "detail", "hwm_reset")
for scenario in scenarios:
    same = [r for r in rows if r.get("scenario") == scenario and r.get("profile") == profile]
    if not same:
        continue
    cur = same[-1]
    print(f"\n[{scenario}] {profile}")
    if len(same) < 2:
        print("  first row for this scenario; nothing to diff")
        continue
    prev = same[-2]
    print(f"  {'metric':<26}{'prev':>14}{'now':>14}{'delta':>9}   ({prev['date']} {prev['commit']} -> {cur['date']} {cur['commit']})")
    for key, value in cur.items():
        if key in TAGS or not isinstance(value, (int, float)):
            continue
        before = prev.get(key)
        delta = f"{(value - before) / before * 100:+.0f}%" if isinstance(before, (int, float)) and before else "n/a"
        print(f"  {key:<26}{before if before is not None else '-':>14}{value:>14}{delta:>9}")
EOF

  echo
  echo "rows appended to $BASELINE (no thresholds in append mode - read the deltas)"
fi
