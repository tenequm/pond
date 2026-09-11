#!/bin/zsh
# Recall context cost: pond_search vs grep. Method: recall-context-cost.md.
# Edit QUERIES to questions from your own history: "natural query ::: rg pattern".
# Requires: pond (synced store), rg, python3. stat flags are macOS/BSD.
set -u
DIRS=(~/.claude/projects ~/.codex/sessions)

QUERIES=(
  "call recording split into two files after device change - did we solve this before ::: recording.*split|split.*recording"
  "how did we wire up the OCC retry loop ::: OCC.*retry|retry.*OCC"
  "mac cannot see the printer scanner over the network ::: scanner.*bonjour|bonjour.*scanner|_uscan"
  "tailscale peers relaying through DERP instead of direct connection ::: DERP"
  "rust target directory filling up the disk, how did we reclaim space ::: cargo clean"
)

now() { python3 -c 'import time;print(time.time())'; }
tmp=$(mktemp -d); trap "rm -rf $tmp" EXIT

printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "query" "pond_tokens" "pond_s" "grep_files" "median_file_tokens" "rg_s" "naive_matched_MB"
i=0
for q in "${QUERIES[@]}"; do
  i=$((i + 1))
  query="${q%% ::: *}"
  pattern="${q##* ::: }"

  t0=$(now)
  pond search "$query" > "$tmp/pond_$i" 2>/dev/null
  t1=$(now)
  pond_tok=$(( $(wc -c < "$tmp/pond_$i") / 4 ))

  t2=$(now)
  rg --ignore-case --files-with-matches "$pattern" $DIRS > "$tmp/files_$i" 2>/dev/null
  t3=$(now)
  nfiles=$(wc -l < "$tmp/files_$i" | tr -d ' ')
  median=0
  if [[ $nfiles -gt 0 ]]; then
    median=$(tr '\n' '\0' < "$tmp/files_$i" | xargs -0 stat -f '%z' \
      | sort -n | awk '{a[NR]=$1} END {print int(a[int(NR/2)+1]/4)}')
  fi

  rg --ignore-case --no-heading --no-line-number "$pattern" $DIRS > "$tmp/naive_$i" 2>/dev/null
  naive_mb=$(( $(wc -c < "$tmp/naive_$i") / 1048576 ))

  printf "%.44s\t%d\t%.1f\t%d\t%d\t%.1f\t%d\n" \
    "$query" "$pond_tok" "$(($t1-$t0))" "$nfiles" "$median" "$(($t3-$t2))" "$naive_mb"
done
