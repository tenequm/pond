#!/usr/bin/env python3
"""Search-quality regression harness for pond's hybrid retrieval.

Wraps `target/release/pond search --format json` and scores the output against
ground truth. The research artifact (rejected fusion variants, methodology,
per-stratum numbers) lives in `docs/researches/embeddings/`; this file is the
operator entrypoint for re-running and adding new query sets.

# Subcommands

    run     - Run one retrieval mode against a query set; write one <id>.json
              envelope per query (the input shape for `score`).
    verify  - Anchor-reachability check: every query's ground truth must be
              findable in FTS or Vector top-N before locking the set.
    score   - Score a results directory against ground truth; emit a per-
              stratum S@3 / P@1 / MRR table plus a per-query ranks CSV.
    pair    - Paired sign test on two ranks CSVs (Success@3 indicator).

`bench.py <subcommand> --help` for full flags.

# Quick workflow

    # 1. (only when locking a new query set) anchor reachability
    python3 bench.py verify --queries queries-en.tsv

    # 2. capture results for one mode
    python3 bench.py run --mode hybrid --queries queries-en.tsv --out results/hybrid-en

    # 3. score against ground truth, emit per-query ranks CSV
    python3 bench.py score --queries queries-en.tsv --results results/hybrid-en \\
        --label hybrid-en --out /tmp/hybrid-en.csv

    # 4. (optional) paired sign test across two runs / modes
    python3 bench.py pair --csv-a /tmp/fts-en.csv --csv-b /tmp/hybrid-en.csv \\
        --label-a fts --label-b hybrid

# Anchor verification - run before locking a new query set

Wave 3 of the hybrid redesign burned a week because 18 of 18 UK queries had
anchors that literally did not appear in the corpus - a fault invisible until
brute-forced. `verify` runs both FTS and Vector at `--limit 200` (overshoots
the production `pool=100` / `vector_pool=200` internals) and reports any
query whose ground truth is in neither arm: a structurally unbenchmarkable
case no fusion strategy can recover. Cross-check unreachable queries against
the kb MCP (`mcp__kb__kb_search` at `min_score=0.3`); if kb also returns
nothing, the seed phrase is fictional.

# Pool-size invariant - relevant to ad-hoc per-arm analysis

Production hybrid runs `fts_search(pool=100)` and `vector_search(vector_pool=200)`
internally (handlers.rs:plan_search) and fuses those candidates. If you
capture arm JSONs at the default `--limit 20` and reason about cross-arm
agreement from them, you only see the top 20 from each arm: for queries
where a noise session sits at rank 30-50 in one arm, you never see it but
production does. Any cross-arm gating idea evaluated against truncated
fixtures will look better than it performs in production. For honest
per-arm analysis, capture at production pool sizes:

    python3 bench.py run --mode fts    --queries Q --out fixtures/fts    --limit 100
    python3 bench.py run --mode vector --queries Q --out fixtures/vector --limit 200

# Privacy note

`fixtures/` and `results/` directories are gitignored: every JSON envelope
captures full message text from the operator's local pond corpus, which
contains API keys, wallet addresses, and private project paths from indexed
conversations. Always regenerate locally rather than sharing these
directories.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
import unicodedata
from math import comb
from pathlib import Path
from typing import Iterator

K_FOR_SUCCESS = 3
BIN_PATH = Path(__file__).resolve().parents[2] / "target/release/pond"


def nfc(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def parse_ground_truth(spec: str) -> tuple[str, list[str]]:
    """Returns (`prefix` | `anchor`, [tokens])."""
    if spec.startswith("prefix:"):
        return "prefix", [t.strip() for t in spec[7:].split(",") if t.strip()]
    if spec.startswith("anchor:"):
        return "anchor", [nfc(spec[7:].strip().lower())]
    raise ValueError(f"unknown ground-truth scheme: {spec!r}")


def find_match_rank(hits: list[dict], kind: str, tokens: list[str]) -> int:
    """1-indexed rank of the first hit that matches ground truth; 0 if none."""
    for idx, hit in enumerate(hits, start=1):
        if kind == "prefix":
            sid = (hit.get("session_id") or "")[:8]
            mid = (hit.get("message_id") or "")[:8]
            if sid in tokens or mid in tokens:
                return idx
        else:
            text = nfc((hit.get("text") or "").lower())
            if any(tok in text for tok in tokens):
                return idx
    return 0


def wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson 95% CI for a binomial proportion."""
    if n == 0:
        return 0.0, 0.0
    p = successes / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    halfwidth = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return max(0.0, center - halfwidth), min(1.0, center + halfwidth)


def iter_queries(queries_path: Path) -> Iterator[dict]:
    with queries_path.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            yield row


def check_binary() -> None:
    if not BIN_PATH.is_file():
        print(f"pond binary not found: {BIN_PATH}", file=sys.stderr)
        print("build it with: cargo build --release", file=sys.stderr)
        sys.exit(69)


def run_search(query: str, mode: str, limit: int, grouped: bool = False) -> dict:
    """Run `pond search` once; return the parsed JSON envelope (`{hits: [...]}`
    on success, `{hits: [], error: <msg>}` on subprocess or parse failure)."""
    cmd = [str(BIN_PATH), "search", "--mode", mode, "--limit", str(limit), "--format", "json"]
    if grouped:
        cmd.append("--group-by-conversation")
    cmd.append(query)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        return {"hits": [], "error": result.stderr.strip() or f"exit {result.returncode}"}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        return {"hits": [], "error": f"json: {e}"}


def normalize_hits(payload: dict) -> list[dict]:
    """Coerce one of three envelope shapes into [{session_id, message_id, text}]:
    pond `hits` (default), pond `groups` (--group-by-conversation), or kb's
    nested `result.results` (cross-tool comparison runs)."""
    if (hits := payload.get("hits")) is not None:
        return hits
    if (groups := payload.get("groups")) is not None:
        return [
            {"session_id": g.get("session_id", ""), "message_id": "", "text": g.get("text", "")}
            for g in groups
        ]
    result = payload.get("result")
    if isinstance(result, dict) and (kb := result.get("results")):
        return [
            {
                "session_id": h.get("conversation_id", ""),
                "message_id": h.get("id", ""),
                "text": h.get("content", ""),
            }
            for h in kb
        ]
    return []


def cmd_run(args: argparse.Namespace) -> int:
    check_binary()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    count = errors = 0
    for row in iter_queries(Path(args.queries)):
        qid = row["id"]
        envelope = run_search(row["query"], args.mode, args.limit, args.grouped)
        if "error" in envelope:
            errors += 1
            (out_dir / f"{qid}.stderr").write_text(envelope["error"])
            print(f"FAIL {qid} (mode={args.mode}): {envelope['error']}", file=sys.stderr)
            continue
        (out_dir / f"{qid}.json").write_text(json.dumps(envelope))
        count += 1
    suffix = " grouped" if args.grouped else ""
    print(
        f"done: ran {count} queries in {args.mode} mode{suffix} "
        f"(limit={args.limit}); {errors} errors"
    )
    return 0 if errors == 0 else 1


def cmd_verify(args: argparse.Namespace) -> int:
    check_binary()
    total = 0
    missing: list[tuple[str, str, str, str]] = []
    for row in iter_queries(Path(args.queries)):
        total += 1
        kind, tokens = parse_ground_truth(row["ground_truth"])
        fts = run_search(row["query"], "fts", args.limit).get("hits") or []
        if find_match_rank(fts, kind, tokens) > 0:
            continue
        vec = run_search(row["query"], "vector", args.limit).get("hits") or []
        if find_match_rank(vec, kind, tokens) > 0:
            continue
        missing.append((row["id"], kind, row["query"], row["ground_truth"]))
    if missing:
        for qid, scheme, q, gt in missing:
            print(f'MISSING {qid} ({scheme}): "{q}" -> {gt}', file=sys.stderr)
        print(
            f"\nanchor verification FAILED: {len(missing)}/{total} queries "
            f"have unreachable ground truth",
            file=sys.stderr,
        )
        print(
            f"(target not in FTS top-{args.limit} AND not in Vector top-{args.limit})",
            file=sys.stderr,
        )
        return 1
    print(
        f"anchor verification OK: {total}/{total} queries reachable in "
        f"FTS or Vector top-{args.limit}"
    )
    return 0


def cmd_score(args: argparse.Namespace) -> int:
    queries_path = Path(args.queries)
    results_dir = Path(args.results)
    out_csv = Path(args.out)
    if not results_dir.is_dir():
        print(f"results dir not found: {results_dir}", file=sys.stderr)
        return 66
    rows: list[dict] = []
    for row in iter_queries(queries_path):
        qid = row["id"]
        base = {"id": qid, "stratum": row["stratum"], "lang": row["lang"]}
        result_file = results_dir / f"{qid}.json"
        if not result_file.exists():
            rows.append({**base, "rank": 0, "note": "missing"})
            continue
        try:
            payload = json.loads(result_file.read_text())
        except json.JSONDecodeError as e:
            rows.append({**base, "rank": 0, "note": f"json:{e}"})
            continue
        kind, tokens = parse_ground_truth(row["ground_truth"])
        rank = find_match_rank(normalize_hits(payload), kind, tokens)
        rows.append({**base, "rank": rank, "note": ""})

    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "stratum", "lang", "mode", "rank", "note"])
        w.writeheader()
        for r in rows:
            w.writerow({**r, "mode": args.label})

    strata: dict[str, list[dict]] = {}
    for r in rows:
        strata.setdefault(f"{r['lang']}/{r['stratum']}", []).append(r)
    print(f"# {args.label}\n")
    print(
        f"| stratum | n | S@{K_FOR_SUCCESS} | S@{K_FOR_SUCCESS} 95% CI | "
        f"P@1 | P@1 95% CI | MRR |"
    )
    print("|---------|---|----|----|-----|----|-----|")
    total_n = total_s3 = total_p1 = 0
    total_mrr = 0.0
    for stratum in sorted(strata):
        items = strata[stratum]
        n = len(items)
        s3 = sum(1 for r in items if 1 <= r["rank"] <= K_FOR_SUCCESS)
        p1 = sum(1 for r in items if r["rank"] == 1)
        mrr = sum((1.0 / r["rank"]) if r["rank"] >= 1 else 0.0 for r in items) / n
        s3_lo, s3_hi = wilson_ci(s3, n)
        p1_lo, p1_hi = wilson_ci(p1, n)
        print(
            f"| {stratum} | {n} | {s3}/{n} = {s3 / n:.2f} | "
            f"[{s3_lo:.2f},{s3_hi:.2f}] | "
            f"{p1}/{n} = {p1 / n:.2f} | "
            f"[{p1_lo:.2f},{p1_hi:.2f}] | {mrr:.3f} |"
        )
        total_n += n
        total_s3 += s3
        total_p1 += p1
        total_mrr += mrr * n
    if total_n:
        print(
            f"| ALL (unweighted sum) | {total_n} | "
            f"{total_s3}/{total_n} = {total_s3 / total_n:.2f} | -- | "
            f"{total_p1}/{total_n} = {total_p1 / total_n:.2f} | -- | "
            f"{total_mrr / total_n:.3f} |"
        )
    return 0


def cmd_pair(args: argparse.Namespace) -> int:
    def load(path: Path) -> dict[str, dict]:
        out: dict[str, dict] = {}
        with path.open() as f:
            for r in csv.DictReader(f):
                out[r["id"]] = {
                    "stratum": f"{r['lang']}/{r['stratum']}",
                    "rank": int(r["rank"]),
                }
        return out

    a = load(Path(args.csv_a))
    b = load(Path(args.csv_b))
    by_stratum: dict[str, tuple[list[int], list[int]]] = {}
    for qid in sorted(set(a) & set(b)):
        s = a[qid]["stratum"]
        by_stratum.setdefault(s, ([], []))
        by_stratum[s][0].append(a[qid]["rank"])
        by_stratum[s][1].append(b[qid]["rank"])

    print(
        f"# Paired sign test: {args.label_a} vs {args.label_b} "
        f"(Success@{K_FOR_SUCCESS})\n"
    )
    print(
        f"| stratum | n | {args.label_a}-only wins | "
        f"{args.label_b}-only wins | ties | n_nonzero | p (two-sided) |"
    )
    print("|---------|---|----|----|------|-----------|---------------|")

    def hit(rank: int) -> int:
        return 1 if 1 <= rank <= K_FOR_SUCCESS else 0

    for stratum in sorted(by_stratum):
        ar, br = by_stratum[stratum]
        wins_a = sum(1 for ra, rb in zip(ar, br) if hit(ra) and not hit(rb))
        wins_b = sum(1 for ra, rb in zip(ar, br) if hit(rb) and not hit(ra))
        ties = sum(1 for ra, rb in zip(ar, br) if hit(ra) == hit(rb))
        n_nz = wins_a + wins_b
        if n_nz == 0:
            p = 1.0
        else:
            smaller = min(wins_a, wins_b)
            tail = sum(comb(n_nz, k) for k in range(smaller + 1)) / (2**n_nz)
            p = min(1.0, 2 * tail)
        print(
            f"| {stratum} | {len(ar)} | {wins_a} | {wins_b} | "
            f"{ties} | {n_nz} | {p:.3f} |"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run one retrieval mode against a query set")
    p_run.add_argument("--queries", required=True, help="TSV: id\\tlang\\tstratum\\tquery\\tground_truth")
    p_run.add_argument("--mode", required=True, choices=["fts", "vector", "hybrid"])
    p_run.add_argument("--out", required=True, help="Output dir for per-query JSON envelopes")
    p_run.add_argument("--limit", type=int, default=20, help="pond search --limit (default 20)")
    p_run.add_argument("--grouped", action="store_true", help="Pass --group-by-conversation")
    p_run.set_defaults(func=cmd_run)

    p_verify = sub.add_parser("verify", help="Check every query's ground truth is reachable in FTS or Vector")
    p_verify.add_argument("--queries", required=True)
    p_verify.add_argument("--limit", type=int, default=200, help="Top-N to check per arm (default 200)")
    p_verify.set_defaults(func=cmd_verify)

    p_score = sub.add_parser("score", help="Score results against ground truth")
    p_score.add_argument("--queries", required=True)
    p_score.add_argument("--results", required=True, help="Dir of <id>.json files from `run`")
    p_score.add_argument("--label", required=True, help="Run label written into the CSV")
    p_score.add_argument("--out", required=True, help="CSV path for per-query ranks")
    p_score.set_defaults(func=cmd_score)

    p_pair = sub.add_parser("pair", help="Paired sign test on two ranks CSVs (Success@3)")
    p_pair.add_argument("--csv-a", required=True)
    p_pair.add_argument("--csv-b", required=True)
    p_pair.add_argument("--label-a", required=True)
    p_pair.add_argument("--label-b", required=True)
    p_pair.set_defaults(func=cmd_pair)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
