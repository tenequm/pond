#!/usr/bin/env bash
# Rule ids in docs/spec.md rot silently when code or prose cites them by a
# truncated, misspelled or removed name; this fails the build on any reference
# that no longer resolves.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

spec="${1:-docs/spec.md}"
code_roots=(packages/pond/src packages/pond/tests packages/pond/benches packages/pond/SKILL.md)

awk -v spec="$spec" '
function firstseg(s) { return substr(s, 1, index(s, "-") - 1) }
function fail(msg) { print msg > "/dev/stderr"; bad = 1 }

BEGIN { nd = nt = nh = nc = nspec = 0 }

FNR == 1 { fileno++ }

fileno == 1 {
  hl = match($0, /^#+/) ? RLENGTH : 0
  m = substr($0, hl + 1)
  if (hl >= 3 && hl <= 6 && match(m, /^ +`[a-z][a-z0-9]*(-[a-z0-9]+)+`/)) {
    match(m, /`[^`]+`/)
    id = substr(m, RSTART + 1, RLENGTH - 2)
    if (!(id in lines)) order[nd++] = id
    lines[id] = lines[id] (lines[id] == "" ? "" : ",") FNR
    next
  }
  if ($0 ~ /^#/) headings[nh++] = tolower($0)
  s = $0
  while (match(s, /`[a-z][a-z0-9]*(-[a-z0-9]+)+`/)) {
    tokline[nt] = FNR
    tok[nt++] = substr(s, RSTART + 1, RLENGTH - 2)
    s = substr(s, RSTART + RLENGTH)
  }
  next
}

{
  n = split($0, p, ":")
  cfile[nc] = p[1]; cline[nc] = p[2]; canchor[nc++] = p[3]
}

END {
  if (nd == 0) {
    print "check-spec-refs: no rule ids declared in " spec " (`####` headings)" > "/dev/stderr"
    exit 1
  }
  for (i = 0; i < nd; i++) {
    id = order[i]
    pre[firstseg(id)] = 1
    t = substr(id, index(id, "-") + 1)
    if (!(t in tails) || id < tails[t]) tails[t] = id
    if (index(lines[id], ",")) {
      first = lines[id]; sub(/,.*/, "", first)
      fail(spec ":" first ": duplicate rule-id declaration `" id "` (lines " lines[id] ")")
    }
  }

  for (i = 0; i < nt; i++) {
    t = tok[i]
    if (firstseg(t) in pre) {
      nspec++
      if (!(t in lines)) fail(spec ":" tokline[i] ": unresolved rule-id reference `" t "`")
    } else if (t in tails) {
      fail(spec ":" tokline[i] ": truncated rule-id reference `" t "` (did you mean `" tails[t] "`?)")
    }
  }

  for (i = 0; i < nc; i++) {
    a = canchor[i]; sub(/^spec\.md#/, "", a); sub(/[.-]+$/, "", a)
    where = cfile[i] ":" cline[i]
    if (a ~ /^[a-z][a-z0-9]*(-[a-z0-9]+)+$/ && (firstseg(a) in pre)) {
      if (!(a in lines)) fail(where ": spec.md#" a " is not a declared rule id")
    } else if (a in tails) {
      fail(where ": spec.md#" a " is a truncated rule id (did you mean `" tails[a] "`?)")
    } else {
      if (!(a in seen)) {
        pat = tolower(a); gsub(/\./, "[.]", pat); gsub(/-/, "[- ]", pat)
        re = "(^|[^a-z0-9])" pat "([^a-z0-9]|$)"
        seen[a] = 0
        for (j = 0; j < nh; j++) if (headings[j] ~ re) { seen[a] = 1; break }
      }
      if (!seen[a]) fail(where ": spec.md#" a " matches no rule id or heading word")
    }
  }

  if (bad) {
    print "" > "/dev/stderr"
    print "check-spec-refs: fix the reference, or declare the id as a `####` heading in " spec "." > "/dev/stderr"
    exit 1
  }
  print "check-spec-refs: ok (" nd " declared ids, " nspec " spec refs, " nc " code refs)"
}
' "$spec" <(grep -rnoE --exclude-dir=fixtures 'spec\.md#[A-Za-z0-9._-]+' "${code_roots[@]}" | sort -t: -k1,1 -k2,2n || true)
