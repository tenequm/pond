#!/usr/bin/env bash
# Release bench gate: the pre-release validation of what pond actually delivers,
# runnable as one command (`moon run bench-gate`) against the configured remote
# store. Runs the regression-gate benches + CLI probes, checks map-vs-scan
# output equivalence (hard pass/fail), appends one JSON line per run to
# docs/benchmarks/bench-gate-baseline.jsonl, and prints the delta vs the
# previous run.
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
# A trailing slash would nest the benchw scratch prefix inside the store itself.
STORE_URL="${STORE_URL%/}"
PROBE_SID="${PROBE_SID:-8b7b9e47-66d2-464b-8ec6-0ad70855ff57}"
PROBE_MID="${PROBE_MID:-419caaa5-13d7-448a-807c-5fb5105112a7}"
BASELINE="docs/benchmarks/bench-gate-baseline.jsonl"
TMP="$(mktemp -d)"
# Fixed synthetic write corpus so write rows stay comparable across time; the
# jsonl write_corpus tag derives from these.
WRITE_SESSIONS=500 WRITE_MESSAGES=5 WRITE_SWEEP_BATCH=512
SCRATCH_ENDPOINT="" SCRATCH_GLOB="" S5_KEY="" S5_SECRET="" S5_REGION=""
scratch_clean() {
  [ -n "$SCRATCH_GLOB" ] || return 0
  local out
  if out=$(AWS_ACCESS_KEY_ID="$S5_KEY" AWS_SECRET_ACCESS_KEY="$S5_SECRET" AWS_REGION="$S5_REGION" s5cmd --endpoint-url "$SCRATCH_ENDPOINT" rm "$SCRATCH_GLOB" 2>&1); then
    echo "scratch cleaned: $SCRATCH_GLOB"
  elif grep -q 'no object found' <<< "$out"; then
    echo "scratch already clean: $SCRATCH_GLOB"
  else
    echo "WARNING: scratch cleanup failed for $SCRATCH_GLOB - clean manually"
    printf '%s\n' "$out"
  fi
}
trap 'scratch_clean; rm -rf "$TMP"' EXIT

# Force embeddings on through the env mirror: Config::load layers
# POND_EMBEDDINGS_ENABLED over the operator's file in every consumer (the CLI
# probes and the cargo benches alike), so the vector probe measures vector
# search whatever the machine has [embeddings].enabled set to - while the
# operator's model/dim (a different embedding space if overridden) stay
# exactly what the store was embedded with.
export POND_EMBEDDINGS_ENABLED=true

echo "=== bench gate: $STORE_URL ==="
# POND_BIN: measure a prebuilt binary (e.g. the released pond) instead of HEAD.
# Only the CLI probes run through it - the cargo benches compile HEAD source,
# so they are skipped and their fields (iops/ops/write_*) land as null. A
# POND_BIN row is a probe-level snapshot of the named binary, not a full row.
if [ -n "${POND_BIN:-}" ]; then
  POND="$POND_BIN"
else
  cargo build --release
  POND=target/release/pond
fi
# Stripped of JSON-breaking chars - the row embeds it as a string.
BIN_VERSION="$($POND --version | head -1 | tr -d '"\\')"
echo "binary: $BIN_VERSION"

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
SEARCH_ARGS=(search --mode vector "read performance optimization lance" --limit 10)
probe get_session_sid "$TMP/map-sid.txt" $POND get-session "$PROBE_SID"
probe get_session_mid "$TMP/map-mid.txt" $POND get-session "$PROBE_MID"
probe get_message     "$TMP/map-msg.txt" $POND get-message "$PROBE_MID"
probe search          "$TMP/search.txt"  $POND "${SEARCH_ARGS[@]}"
# Date-scoped search is the worst-measured real query shape (28-31% success vs
# 47-48% unfiltered in the 63-day trace behind
# docs/researches/2608-21-semantic-vs-fts-usage-eval); the timestamp zonemap
# exists for it. Same query as `search` so the pair isolates the date filter.
DATED_DAYS="${DATED_DAYS:-7}"
DATED_FROM="$(python3 -c "import datetime; print((datetime.date.today() - datetime.timedelta(days=$DATED_DAYS)).isoformat())")"
probe search_dated    "$TMP/search-dated.txt" $POND "${SEARCH_ARGS[@]}" --from-date "$DATED_FROM"
t0=$(now); $POND sql "SELECT count(*) FROM messages" > "$TMP/sql.txt"; t1=$(now)
python3 -c "print(f'{$t1 - $t0:.1f}')" > "$TMP/sql_count.s"
printf '%-28s       %ss\n' sql_count "$(cat "$TMP/sql_count.s")"

echo "--- map-vs-scan equivalence (empty cache forces the scan path) ---"
EMPTY="$TMP/empty-cache"; mkdir "$EMPTY"
XDG_CACHE_HOME=$EMPTY $POND get-session "$PROBE_SID" > "$TMP/scan-sid.txt" 2>/dev/null
XDG_CACHE_HOME=$EMPTY $POND get-session "$PROBE_MID" > "$TMP/scan-mid.txt" 2>/dev/null
XDG_CACHE_HOME=$EMPTY $POND get-message "$PROBE_MID" > "$TMP/scan-msg.txt" 2>/dev/null
for f in sid mid msg; do
  diff "$TMP/map-$f.txt" "$TMP/scan-$f.txt" > /dev/null || { echo "EQUIVALENCE FAILED: $f"; diff "$TMP/map-$f.txt" "$TMP/scan-$f.txt" | head -20; exit 1; }
done
echo "EQUIVALENCE OK: map-served output identical to scan-served"

: > "$TMP/serve.txt"; : > "$TMP/ops.txt"
: > "$TMP/write-copy.txt"; : > "$TMP/write-sweep.txt"; : > "$TMP/write-prof.txt"
WRITE_BACKEND=null
WRITE_CORPUS=null
if [ -z "${POND_BIN:-}" ]; then
  echo "--- serve_mem_bench (io-trace) ---"
  cargo bench --bench serve_mem_bench --features io-trace -- --storage-path "$STORE_URL" --io-trace | tee "$TMP/serve.txt"

  echo "--- ops_bench ---"
  # --url is load-bearing: ops_bench resolves the operator XDG config directly
  # (it ignores POND_CONFIG_FILE), so without it the ops phases silently measure
  # whatever store the operator's config names instead of the gate store.
  cargo bench --bench ops_bench -- --url "$STORE_URL" | tee "$TMP/ops.txt"

  echo "--- write benches (scratch stores, the real store is never written) ---"
  WRITE_ARGS=(--sessions "$WRITE_SESSIONS" --messages "$WRITE_MESSAGES")
  WRITE_CORPUS="\"synthetic-${WRITE_SESSIONS}x${WRITE_MESSAGES}\""
  if [[ "$STORE_URL" == s3+http* ]]; then
    # Sibling prefix beside the gate store (same bucket/creds); the bench's
    # scratch stores are fixed-named benchw-* children, so stale ones from an
    # aborted run are swept up front - they would fail the copy verification.
    # Plain s3:// is excluded: its URL carries no endpoint host for s5cmd.
    WRITE_BASE="${STORE_URL%/*}/benchw"
    WRITE_ARGS+=(--dest-url "$WRITE_BASE")
    WRITE_BACKEND='"s3"'
    if [[ "${STORE_URL##*/}" == benchw* ]]; then
      echo "WARNING: store prefix '${STORE_URL##*/}' would match the scratch glob - clean s3 scratch under $WRITE_BASE-* manually"
    elif command -v s5cmd > /dev/null; then
      if SCRATCH_ENV="$(python3 - "$OPERATOR_CONFIG" "$WRITE_BASE" <<'PYEOF'
import pathlib, shlex, subprocess, sys, tomllib
c = tomllib.load(open(sys.argv[1], "rb"))
cr = c.get("creds", {}).get("default", {})
def val(base):
    if cr.get(base):
        return cr[base]
    f = cr.get(base + "_file")
    if f:
        return pathlib.Path(f).expanduser().read_text().strip()
    cmd = cr.get(base + "_command")
    if cmd:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True).stdout.strip()
    raise SystemExit(f"missing creds field: {base}")
rest = sys.argv[2].split("://", 1)[1]
host, key = rest.split("/", 1)
if "/" not in key:
    raise SystemExit(f"no prefix segment in {sys.argv[2]} - refusing a bucket-root scratch glob")
print(f"S5_KEY={shlex.quote(val('access_key_id'))}")
print(f"S5_SECRET={shlex.quote(val('secret_access_key'))}")
print(f"S5_REGION={shlex.quote(cr.get('region', 'us-east-1'))}")
print(f"SCRATCH_ENDPOINT={shlex.quote('https://' + host)}")
print(f"SCRATCH_GLOB={shlex.quote('s3://' + key + '-*')}")
PYEOF
)"; then
        eval "$SCRATCH_ENV"
        scratch_clean
      else
        echo "WARNING: creds for scratch cleanup unavailable - clean s3 scratch under $WRITE_BASE-* manually"
      fi
    else
      echo "WARNING: s5cmd not found - clean s3 scratch under $WRITE_BASE-* manually"
    fi
  else
    WRITE_BACKEND='"local"'
  fi
  cargo bench --bench write_bench -- "${WRITE_ARGS[@]}" | tee "$TMP/write-copy.txt"
  if grep -q ': false' "$TMP/write-copy.txt"; then echo "WRITE VERIFICATION FAILED"; exit 1; fi
  cargo bench --bench write_bench -- "${WRITE_ARGS[@]}" --append-sweep "$WRITE_SWEEP_BATCH" --sweep-commits-cap 10 | tee "$TMP/write-sweep.txt"
  # --grown 2: round 0 folds under the eager policy, round 1 under the deferred
  # policy production sync uses - round 1 is the scraped fold figure.
  cargo bench --bench write_bench -- "${WRITE_ARGS[@]}" --profile-optimize "$TMP/wprof" --grown 2 | tee "$TMP/write-prof.txt"
fi

# `null` when the bench skipped that phase (serve_mem_bench skips its vector
# phases on an embeddings-disabled instance; every bench is skipped under
# POND_BIN), so a missing row can never emit invalid JSON like `"vector_iops":,`.
iops() { local v; v=$(awk -v c="$1" '$1 == c {print $2; exit}' "$TMP/serve.txt"); echo "${v:-null}"; }
ms() { local v; v=$(grep -F "$1" "$TMP/ops.txt" | awk '{print int($(NF-1))}' || true); echo "${v:-null}"; }
# write_bench scrapers, keyed to its human-readable lines: "[1] full copy
# streaming :   N ms", the sweep table row, "build total: N ms", and the
# round-1 (production deferred policy) fold total.
wcopy() { local v; v=$(grep -F "[$1]" "$TMP/write-copy.txt" | awk -F: '{print $2}' | awk '{print $1; exit}' || true); echo "${v:-null}"; }
wsweep() { local v; v=$(awk -v f="$1" -v b="$WRITE_SWEEP_BATCH" '$1 == b {gsub(/\(/, "", $f); print $f; exit}' "$TMP/write-sweep.txt"); echo "${v:-null}"; }
wbuild() { local v; v=$(grep -F 'build total:' "$TMP/write-prof.txt" | awk '{print $3; exit}' || true); echo "${v:-null}"; }
wfold() { local v; v=$(grep -E 'round +1 \[after' "$TMP/write-prof.txt" | awk -F'total: ' '{print $2}' | awk '{print $1; exit}' || true); echo "${v:-null}"; }
# A dirty tree means the measured binary may not match the named commit; the
# baseline the gate itself appends to never affects the binary, so exclude it.
COMMIT="$(git rev-parse --short HEAD)"
if [ -n "$(git status --porcelain -- ':!docs/benchmarks/bench-gate-baseline.jsonl')" ]; then COMMIT="$COMMIT-dirty"; fi
# Rows from different stores must not be diffed as a regression, but the repo
# is public, so the row carries a digest (or operator-set STORE_LABEL), never
# the store URL itself.
STORE_LABEL="${STORE_LABEL:-$(printf %s "$STORE_URL" | shasum -a 256 | cut -c1-12)}"
printf '{"date":"%s","commit":"%s","bin":"%s","store":"%s","get_session_sid_s":%s,"get_session_mid_s":%s,"get_message_s":%s,"search_s":%s,"search_dated_s":%s,"search_mode":"vector","sql_count_s":%s,"equivalence":"OK","fts_iops":%s,"vector_iops":%s,"get_message_iops":%s,"search_iops":%s,"open_store_ms":%s,"row_counts_ms":%s,"oracle_warm_ms":%s,"write_backend":%s,"write_corpus":%s,"write_copy_ms":%s,"write_copy_merge_ms":%s,"write_copy_noop_ms":%s,"write_copy_delta_ms":%s,"write_ms_per_commit":%s,"write_rows_per_s":%s,"write_index_build_ms":%s,"write_fold_ms":%s}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$COMMIT" "$BIN_VERSION" "$STORE_LABEL" \
  "$(cat "$TMP/get_session_sid.s")" "$(cat "$TMP/get_session_mid.s")" "$(cat "$TMP/get_message.s")" \
  "$(cat "$TMP/search.s")" "$(cat "$TMP/search_dated.s")" "$(cat "$TMP/sql_count.s")" \
  "$(iops fts_search)" "$(iops vector_search)" "$(iops pond_get_message)" "$(iops pond_search)" \
  "$(ms 'open store (manifests)')" "$(ms row_counts)" "$(ms 'session_last_message_ids WARM')" \
  "$WRITE_BACKEND" "$WRITE_CORPUS" \
  "$(wcopy 1)" "$(wcopy 1b)" "$(wcopy 3)" "$(wcopy 4)" \
  "$(wsweep 5)" "$(wsweep 6)" "$(wbuild)" "$(wfold)" \
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
if prev.get("store") != cur.get("store"):
    print(f"WARNING: different stores ({prev.get('store')} -> {cur.get('store')}) - deltas below are cross-store, not a regression signal")
def label(r):
    return f"{r['date']} {r['commit']}" + (f" [{r['bin']}]" if r.get("bin") else "")
print(f"{'metric':<20}{'prev':>12}{'now':>12}{'delta':>10}   ({label(prev)} -> {label(cur)})")
TAGS = ("date", "commit", "bin", "store", "equivalence", "search_mode", "write_backend", "write_corpus")
for k, v in cur.items():
    if k in TAGS:
        continue
    p = prev.get(k)
    d = f"{(v - p) / p * 100:+.0f}%" if p and isinstance(v, (int, float)) else "n/a"
    print(f"{k:<20}{p if p is not None else '-':>12}{v if v is not None else '-':>12}{d:>10}")
if not any(k.startswith("write_") and k not in TAGS and cur[k] is not None for k in cur):
    print("NOTE: no write_* metrics in this row - storage-path changes need a write-side A/B (AGENTS.md#benchmarking-storage-path-changes)")
EOF
