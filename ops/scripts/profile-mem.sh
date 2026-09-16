#!/usr/bin/env bash
# Attribute one mem_bench scenario's allocations to source sites (#245):
#
#   ops/scripts/profile-mem.sh <scenario> [heaptrack|dhat] [--profile ci|large]
#
# heaptrack (default, Linux): full allocation trace of the built binary. dhat:
# the same scenario under the `dhat-heap` feature, writing dhat-heap.json for
# the online DHAT viewer (https://nnethercote.github.io/dh_view/dh_view.html),
# sorted by at-t-gmax (peak) or at-t-end (retention).
#
# Both build the `profiling` profile (release + debug = 1, workspace root) so
# the backtraces name pond functions. heaptrack is pointed at the BUILT binary,
# never at `cargo run` - wrapping cargo profiles cargo.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

SCENARIO="${1:-}"
[ $# -gt 0 ] && shift
# The tool is optional and positional, so only a bare second word is one - a
# flag there means the tool was omitted (`<scenario> --profile large`).
TOOL=heaptrack
case "${1:-}" in
  -*|'') ;;
  *) TOOL="$1"; shift ;;
esac
CORPUS_PROFILE=ci
while [ $# -gt 0 ]; do
  case "$1" in
    --profile) CORPUS_PROFILE="$2"; shift 2 ;;
    --profile=*) CORPUS_PROFILE="${1#*=}"; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$SCENARIO" ]; then
  sed -n '2,13p' "$0"
  exit 2
fi

case "$TOOL" in
  heaptrack) FEATURE=mem-probe ;;
  dhat) FEATURE=dhat-heap ;;
  *) echo "unknown tool: $TOOL (expected heaptrack|dhat)" >&2; exit 2 ;;
esac

if [ "$TOOL" = heaptrack ] && ! command -v heaptrack > /dev/null; then
  echo "heaptrack not found on PATH (nix shell nixpkgs#heaptrack)" >&2
  exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cargo build --profile profiling --bench mem_bench --features "$FEATURE" --message-format=json \
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

# Corpus generation is not what is being profiled; prepare it under the same
# (cheap) feature build first so the trace covers the scenario only.
"$BENCH_BIN" --prepare --profile "$CORPUS_PROFILE"

case "$TOOL" in
  heaptrack)
    heaptrack "$BENCH_BIN" --scenario "$SCENARIO" --profile "$CORPUS_PROFILE"
    echo "open the printed .zst with: heaptrack_gui <file>  (or heaptrack_print <file>)"
    ;;
  dhat)
    "$BENCH_BIN" --scenario "$SCENARIO" --profile "$CORPUS_PROFILE"
    echo "wrote dhat-heap.json - load it in https://nnethercote.github.io/dh_view/dh_view.html"
    ;;
esac
