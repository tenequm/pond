#!/usr/bin/env bash
set -euo pipefail
mode=$1; queries=$2; out_dir=$3
mkdir -p "$out_dir"
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
bin="$repo_root/target/release/pond"
while IFS=$'\t' read -r id lang stratum query gt; do
    [[ -z "${id:-}" || "$id" == "id" ]] && continue
    "$bin" search \
        --mode "$mode" \
        --group-by-conversation \
        --limit 20 \
        --format json \
        "$query" > "$out_dir/$id.json" 2>/dev/null
done < "$queries"
echo "done ($mode, grouped)"
