#!/usr/bin/env bash
# Release bench gate: the pre-release validation of what pond actually delivers,
# runnable as one command (`moon run bench-gate`) against the configured remote
# store. Runs the regression-gate benches + CLI probes, checks map-vs-scan
# output equivalence (hard pass/fail), appends one JSON line per run to
# ops/bench-gate-baseline.jsonl, and prints the delta vs the previous run.
#
# Gate targets (run here, every release):
#   serve_mem_bench  - read-serving components + per-query S3 iops (io-trace)
#   ops_bench        - read-only phase timing of status/sync/optimize/copy
#   CLI probes       - get-session / get-message / search / sql wall-clock
# Research probes (NOT run here; run when touching their area):
#   read_bench (fold-batching threshold), sync_oracle_bench (oracle choice),
#   tokenizer_quality_bench, fmindex_probe (#47), multiwriter_bench (OCC),
#   backend_bench, plus write/ingest/embed benches for write-path work.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

OPERATOR_CONFIG="${POND_CONFIG_FILE:-$HOME/.config/pond/config.toml}"
STORE_URL="${STORE_URL:-$(awk -F'"' '/^\[storage\]/{s=1;next} /^\[/{s=0} s&&/^path/{print $2;exit}' "$OPERATOR_CONFIG")}"
PROBE_SID="${PROBE_SID:-8b7b9e47-66d2-464b-8ec6-0ad70855ff57}"
PROBE_MID="${PROBE_MID:-419caaa5-13d7-448a-807c-5fb5105112a7}"
BASELINE="ops/bench-gate-baseline.jsonl"
TMP="$(mktemp -d)"

# Force embeddings on through the env mirror: Config::load layers
# POND_EMBEDDINGS_ENABLED over the operator's file in every consumer (the CLI
# probes and the cargo benches alike), so the vector probe measures vector
# search whatever the machine has [embeddings].enabled set to - while the
# operator's model/dim (a different embedding space if overridden) stay
# exactly what the store was embedded with.
export POND_EMBEDDINGS_ENABLED=true

echo "=== bench gate: $STORE_URL ==="
cargo build --release
POND=target/release/pond

now() { python3 -c 'import time; print(time.time())'; }
probe() { # probe <name> <outfile> <cmd...> -> records best-of-runs seconds in $TMP/<name>.s
  local name=$1 outfile=$2 best="" t0 t1 dt
  shift 2
  for run in 1 2; do
    t0=$(now)
    "$@" > "$outfile" 2>/dev/null
    t1=$(now)
    dt=$(python3 -c "print(f'{$t1 - $t0:.1f}')")
    printf '%-28s run%d  %ss\n' "$name" "$run" "$dt"
    best=$(python3 -c "print(min($dt, ${best:-$dt}))")
  done
  echo "$best" > "$TMP/$name.s"
}

echo "--- CLI probes (2 runs each, best kept) ---"
probe get_session_sid "$TMP/map-sid.txt" $POND get-session "$PROBE_SID"
probe get_session_mid "$TMP/map-mid.txt" $POND get-session "$PROBE_MID"
probe get_message     "$TMP/map-msg.txt" $POND get-message "$PROBE_MID"
probe search          "$TMP/search.txt"  $POND search --mode vector "read performance optimization lance" --limit 10
t0=$(now); $POND sql "SELECT count(*) FROM messages" > "$TMP/sql.txt"; t1=$(now)
python3 -c "print(f'{$t1 - $t0:.1f}')" > "$TMP/sql_count.s"
printf '%-28s       %ss\n' sql_count "$(cat "$TMP/sql_count.s")"

echo "--- map-vs-scan equivalence (empty cache forces the scan path) ---"
EMPTY="$(mktemp -d)"
XDG_CACHE_HOME=$EMPTY $POND get-session "$PROBE_SID" > "$TMP/scan-sid.txt" 2>/dev/null
XDG_CACHE_HOME=$EMPTY $POND get-session "$PROBE_MID" > "$TMP/scan-mid.txt" 2>/dev/null
XDG_CACHE_HOME=$EMPTY $POND get-message "$PROBE_MID" > "$TMP/scan-msg.txt" 2>/dev/null
for f in sid mid msg; do
  diff "$TMP/map-$f.txt" "$TMP/scan-$f.txt" > /dev/null || { echo "EQUIVALENCE FAILED: $f"; diff "$TMP/map-$f.txt" "$TMP/scan-$f.txt" | head -20; exit 1; }
done
echo "EQUIVALENCE OK: map-served output identical to scan-served"

echo "--- serve_mem_bench (io-trace) ---"
cargo bench --bench serve_mem_bench --features io-trace -- --storage-path "$STORE_URL" --io-trace | tee "$TMP/serve.txt"

echo "--- ops_bench ---"
cargo bench --bench ops_bench | tee "$TMP/ops.txt"

# `null` when the bench skipped that phase (serve_mem_bench skips its vector
# phases on an embeddings-disabled instance), so a missing row can never emit
# invalid JSON like `"vector_iops":,`.
iops() { local v; v=$(awk -v c="$1" '$1 == c {print $2; exit}' "$TMP/serve.txt"); echo "${v:-null}"; }
ms() { local v; v=$(grep -F "$1" "$TMP/ops.txt" | awk '{print int($(NF-1))}'); echo "${v:-null}"; }
printf '{"date":"%s","commit":"%s","get_session_sid_s":%s,"get_session_mid_s":%s,"get_message_s":%s,"search_s":%s,"search_mode":"vector","sql_count_s":%s,"equivalence":"OK","fts_iops":%s,"vector_iops":%s,"get_message_iops":%s,"search_iops":%s,"open_store_ms":%s,"row_counts_ms":%s,"oracle_warm_ms":%s}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(git rev-parse --short HEAD)" \
  "$(cat "$TMP/get_session_sid.s")" "$(cat "$TMP/get_session_mid.s")" "$(cat "$TMP/get_message.s")" \
  "$(cat "$TMP/search.s")" "$(cat "$TMP/sql_count.s")" \
  "$(iops fts_search)" "$(iops vector_search)" "$(iops pond_get_message)" "$(iops pond_search)" \
  "$(ms 'open store (manifests)')" "$(ms row_counts)" "$(ms 'session_last_message_ids WARM')" \
  >> "$BASELINE"

echo "--- delta vs previous run ---"
python3 - "$BASELINE" <<'EOF'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
cur = rows[-1]
if len(rows) < 2:
    print("first baseline row recorded; nothing to diff")
    sys.exit()
prev = rows[-2]
print(f"{'metric':<20}{'prev':>12}{'now':>12}{'delta':>10}   ({prev['date']} {prev['commit']} -> {cur['date']} {cur['commit']})")
for k, v in cur.items():
    if k in ("date", "commit", "equivalence", "search_mode"):
        continue
    p = prev.get(k)
    d = f"{(v - p) / p * 100:+.0f}%" if p and isinstance(v, (int, float)) else "n/a"
    print(f"{k:<20}{p if p is not None else '-':>12}{v if v is not None else '-':>12}{d:>10}")
EOF
