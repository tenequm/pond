#!/usr/bin/env bash
# Driver for the FTS-vs-Vector-vs-Hybrid run.
#
# Usage: run.sh <mode> <queries.tsv> <out_dir> [limit]
#   mode: fts | vector | hybrid
#   queries.tsv: TSV with header `id lang stratum query ground_truth`
#   out_dir: directory to write one `<id>.json` per query
#   limit: optional `pond search --limit` value. Defaults to 20 (benchmark
#          scoring views top-20 only). Pass 100/200 to capture arm outputs at
#          production pool sizes when using these JSONs as simulator fixtures
#          (production hybrid runs `pool=100` for FTS and `vector_pool=200`
#          internally; capturing arms at `--limit 20` truncates below pool
#          and makes the simulator optimistic, so confidence-gating ideas
#          tested against limit=20 fixtures will look better than they are
#          in production).

set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "usage: $0 <mode> <queries.tsv> <out_dir> [limit]" >&2
    exit 64
fi

mode=$1
queries=$2
out_dir=$3
limit=${4:-20}

case "$mode" in
    fts|vector|hybrid) ;;
    *) echo "mode must be fts|vector|hybrid (got: $mode)" >&2; exit 64 ;;
esac

if [[ ! -f "$queries" ]]; then
    echo "queries file not found: $queries" >&2
    exit 66
fi

mkdir -p "$out_dir"

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
bin="$repo_root/target/release/pond"
cfg="$repo_root/bench/embeddings/config.toml"

if [[ ! -x "$bin" ]]; then
    echo "pond binary not found or not executable: $bin" >&2
    echo "build it with: cargo build --release --features bench-overrides" >&2
    exit 69
fi

count=0
errors=0
while IFS=$'\t' read -r id lang stratum query ground_truth; do
    [[ -z "${id:-}" || "$id" == "id" ]] && continue
    out_file="$out_dir/$id.json"
    if POND_SEARCH_MODE="$mode" POND_CONFIG="$cfg" "$bin" search \
        --limit "$limit" \
        --format json \
        "$query" > "$out_file" 2> "$out_dir/$id.stderr"; then
        count=$((count + 1))
    else
        errors=$((errors + 1))
        echo "FAIL $id (mode=$mode): see $out_dir/$id.stderr" >&2
    fi
done < "$queries"

echo "done: ran $count queries in $mode mode (limit=$limit); $errors errors"
