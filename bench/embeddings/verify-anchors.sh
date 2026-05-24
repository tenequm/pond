#!/usr/bin/env bash
# Verify that every ground-truth anchor in a queries.tsv is reachable from
# the live pond corpus by at least one retriever (FTS or Vector). Catches
# the methodology defect that wasted Wave 3 of the hybrid redesign: 18 UK
# seed phrases scored 0/18 across all modes because the anchor text never
# appeared in any indexed message (the failure was invisible until we
# brute-forced FTS top-200 per anchor after the fact).
#
# Two ground-truth schemes are checked:
#   - prefix:<id1>,<id2>,...   - at least one session_id or message_id whose
#                                8-char prefix matches must appear in either
#                                arm's top-N for the literal query text.
#   - anchor:<substring>       - the literal substring must appear in the
#                                `text` field of some hit (NFC normalized,
#                                case-insensitive).
#
# Semantics: a query is "reachable" if either FTS top-N OR Vector top-N
# surfaces a matching hit. A query that fails BOTH arms is structurally
# unbenchmarkable - no fusion strategy can recover it - and the only honest
# response is to rewrite the query or drop it before locking ground truth.
#
# Exit code: 0 if every anchor is reachable; 1 if any are unreachable.
#
# Usage: verify-anchors.sh <queries.tsv> [--limit N]
#   N defaults to 200 to overshoot the production pool (pool=100 for FTS,
#   vector_pool=200 for Vector); see handlers.rs:plan_search for the
#   production values.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <queries.tsv> [--limit N]" >&2
    exit 64
fi

queries=$1
limit=200
if [[ "${2:-}" == "--limit" && -n "${3:-}" ]]; then
    limit=$3
fi

if [[ ! -f "$queries" ]]; then
    echo "queries file not found: $queries" >&2
    exit 66
fi

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
bin="$repo_root/target/release/pond"
cfg="$repo_root/bench/embeddings/config.toml"

if [[ ! -x "$bin" ]]; then
    echo "pond binary not found or not executable: $bin (run cargo build --release)" >&2
    exit 69
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 required (used for NFC anchor matching)" >&2
    exit 69
fi

check_hits() {
    local hits_json=$1
    local spec=$2
    printf "%s" "$hits_json" | python3 -c '
import json, sys, unicodedata
spec = sys.argv[1]
data = json.load(sys.stdin)
hits = data.get("hits", [])
if not hits:
    print("no"); sys.exit(0)
scheme, _, payload = spec.partition(":")
if scheme == "prefix":
    tokens = set(t.strip() for t in payload.split(",") if t.strip())
    for h in hits:
        sid = (h.get("session_id") or "")[:8]
        mid = (h.get("message_id") or "")[:8]
        if sid in tokens or mid in tokens:
            print("yes"); sys.exit(0)
    print("no")
elif scheme == "anchor":
    needle = unicodedata.normalize("NFC", payload).lower()
    for h in hits:
        text = unicodedata.normalize("NFC", (h.get("text") or "")).lower()
        if needle in text:
            print("yes"); sys.exit(0)
    print("no")
else:
    print(f"BAD_SCHEME:{scheme}")
' "$spec"
}

run_arm() {
    local mode=$1
    local query=$2
    POND_SEARCH_MODE="$mode" POND_CONFIG="$cfg" "$bin" search \
        --limit "$limit" \
        --format json \
        "$query" 2>/dev/null || echo '{"hits":[]}'
}

total=0
missing=0
report=""

while IFS=$'\t' read -r id lang stratum query ground_truth; do
    [[ -z "${id:-}" || "$id" == "id" ]] && continue
    total=$((total + 1))

    fts_hits=$(run_arm fts "$query")
    fts_found=$(check_hits "$fts_hits" "$ground_truth")

    vec_found="no"
    if [[ "$fts_found" != "yes" ]]; then
        vec_hits=$(run_arm vector "$query")
        vec_found=$(check_hits "$vec_hits" "$ground_truth")
    fi

    scheme="${ground_truth%%:*}"
    if [[ "$fts_found" == "BAD_SCHEME"* || "$vec_found" == "BAD_SCHEME"* ]]; then
        echo "$id: unknown ground-truth scheme: $ground_truth" >&2
        missing=$((missing + 1))
        report+="MISSING $id (bad scheme)\n"
    elif [[ "$fts_found" != "yes" && "$vec_found" != "yes" ]]; then
        missing=$((missing + 1))
        report+="MISSING $id ($scheme): \"$query\" -> $ground_truth\n"
    fi
done < "$queries"

if [[ $missing -gt 0 ]]; then
    printf "%b" "$report" >&2
    echo "" >&2
    echo "anchor verification FAILED: $missing/$total queries have unreachable ground truth" >&2
    echo "(target not in FTS top-$limit AND not in Vector top-$limit)" >&2
    exit 1
fi

echo "anchor verification OK: $total/$total queries reachable in FTS or Vector top-$limit"
