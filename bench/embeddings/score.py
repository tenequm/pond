#!/usr/bin/env python3
"""Score one retrieval mode's results against ground truth.

Reads a TSV of queries (id\tlang\tstratum\tquery\tground_truth) plus a directory
of pond search JSON envelopes (one file per query, named `<id>.json`). For each
query, computes the rank of the first hit that matches the ground truth:

- `prefix:<id1>,<id2>,...` matches if any hit has a `session_id` OR `message_id`
  whose 8-char prefix is in the list. The frozen tokenizer-experiment queries
  use 8-char message-uuid prefixes; matching on either id is the same recall
  semantics used in `tokenizer-experiment-report.md`.
- `anchor:<substring>` matches if the hit's `text` field contains the substring
  (case-insensitive, NFC-normalized). Used for Ukrainian queries where the
  message id was unstable across re-syncs in the original experiment.

Outputs:
- A Markdown table to stdout with one row per (stratum, mode), columns:
  n, Success@3, P@1, MRR, with 95% Wilson CIs for Success@3 and P@1.
- A CSV next to the results directory with the per-query first-hit rank
  (rank=0 if no match in top-20), for paired sign tests across modes.

Usage: score.py <queries.tsv> <results_dir> <mode_label> <out_csv>
"""

from __future__ import annotations

import csv
import json
import math
import sys
import unicodedata
from pathlib import Path

K_FOR_SUCCESS = 3


def nfc(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson 95% CI for a binomial proportion. Returns (lo, hi) in [0, 1]."""
    if n == 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    halfwidth = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return (max(0.0, center - halfwidth), min(1.0, center + halfwidth))


def parse_ground_truth(spec: str) -> tuple[str, list[str]]:
    """Return (`prefix` | `anchor`, [tokens])."""
    if spec.startswith("prefix:"):
        return ("prefix", [tok.strip() for tok in spec[len("prefix:") :].split(",") if tok.strip()])
    if spec.startswith("anchor:"):
        return ("anchor", [nfc(spec[len("anchor:") :].strip().lower())])
    raise ValueError(f"unknown ground-truth scheme: {spec!r}")


def find_first_match_rank(hits: list[dict], kind: str, tokens: list[str]) -> int:
    """Return the 1-indexed rank of the first hit that matches ground truth, or 0 if none in window."""
    for idx, hit in enumerate(hits, start=1):
        if kind == "prefix":
            sid = hit.get("session_id", "")[:8]
            mid = hit.get("message_id", "")[:8]
            if sid in tokens or mid in tokens:
                return idx
        elif kind == "anchor":
            text = nfc((hit.get("text") or "").lower())
            if any(tok in text for tok in tokens):
                return idx
    return 0


def score_mode(queries_path: Path, results_dir: Path, mode_label: str, out_csv: Path) -> None:
    rows: list[dict] = []
    with queries_path.open() as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            qid = row["id"]
            result_file = results_dir / f"{qid}.json"
            if not result_file.exists():
                rows.append({"id": qid, "stratum": row["stratum"], "lang": row["lang"], "rank": 0, "note": "missing"})
                continue
            try:
                payload = json.loads(result_file.read_text())
            except json.JSONDecodeError as e:
                rows.append({"id": qid, "stratum": row["stratum"], "lang": row["lang"], "rank": 0, "note": f"json:{e}"})
                continue
            # Pond hit shape: {"hits": [{session_id, message_id, text, ...}]}.
            # Pond grouped shape: {"groups": [{session_id, text, ...}]} (no message_id).
            # kb shape: {"result": {"results": [{id, conversation_id, content, ...}]}}.
            hits = payload.get("hits")
            if hits is None and payload.get("groups") is not None:
                hits = [{"session_id": g.get("session_id", ""), "message_id": "", "text": g.get("text", "")} for g in payload["groups"]]
            if hits is None and isinstance(payload.get("result"), dict):
                kb_results = payload["result"].get("results") or []
                hits = [
                    {
                        "session_id": h.get("conversation_id", ""),
                        "message_id": h.get("id", ""),
                        "text": h.get("content", ""),
                    }
                    for h in kb_results
                ]
            hits = hits or []
            kind, tokens = parse_ground_truth(row["ground_truth"])
            rank = find_first_match_rank(hits, kind, tokens)
            rows.append({"id": qid, "stratum": row["stratum"], "lang": row["lang"], "rank": rank, "note": ""})

    # Persist per-query ranks (CSV for paired tests later).
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "stratum", "lang", "mode", "rank", "note"])
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "mode": mode_label})

    # Per-stratum aggregates.
    strata: dict[str, list[dict]] = {}
    for row in rows:
        strata.setdefault(f"{row['lang']}/{row['stratum']}", []).append(row)

    print(f"# {mode_label}")
    print()
    print(f"| stratum | n | S@{K_FOR_SUCCESS} | S@{K_FOR_SUCCESS} 95% CI | P@1 | P@1 95% CI | MRR |")
    print(f"|---------|---|----|----|-----|----|-----|")
    total_n = 0
    total_s3 = 0
    total_p1 = 0
    total_mrr = 0.0
    for stratum in sorted(strata):
        items = strata[stratum]
        n = len(items)
        s3 = sum(1 for r in items if 1 <= r["rank"] <= K_FOR_SUCCESS)
        p1 = sum(1 for r in items if r["rank"] == 1)
        mrr = sum((1.0 / r["rank"]) if r["rank"] >= 1 else 0.0 for r in items) / n if n else 0.0
        s3_lo, s3_hi = wilson_ci(s3, n)
        p1_lo, p1_hi = wilson_ci(p1, n)
        print(
            f"| {stratum} | {n} | {s3}/{n} = {s3/n:.2f} | [{s3_lo:.2f},{s3_hi:.2f}] | "
            f"{p1}/{n} = {p1/n:.2f} | [{p1_lo:.2f},{p1_hi:.2f}] | {mrr:.3f} |"
        )
        total_n += n
        total_s3 += s3
        total_p1 += p1
        total_mrr += mrr * n
    if total_n:
        print(
            f"| ALL (unweighted sum) | {total_n} | {total_s3}/{total_n} = {total_s3/total_n:.2f} | -- | "
            f"{total_p1}/{total_n} = {total_p1/total_n:.2f} | -- | {total_mrr/total_n:.3f} |"
        )


def paired_sign_test(a_ranks: list[int], b_ranks: list[int], k: int = K_FOR_SUCCESS) -> dict:
    """Sign test of mode A vs mode B on per-query Success@k indicator.

    Returns dict with `wins_a`, `wins_b`, `ties`, `n_nonzero`, and a two-sided
    p-value computed exactly (binomial coefficient sum), since pilot n is tiny.
    """
    assert len(a_ranks) == len(b_ranks), "ranks lists must align"
    def s(rank: int) -> int:
        return 1 if 1 <= rank <= k else 0
    wins_a = sum(1 for ra, rb in zip(a_ranks, b_ranks) if s(ra) and not s(rb))
    wins_b = sum(1 for ra, rb in zip(a_ranks, b_ranks) if s(rb) and not s(ra))
    ties = sum(1 for ra, rb in zip(a_ranks, b_ranks) if s(ra) == s(rb))
    n = wins_a + wins_b
    if n == 0:
        return {"wins_a": wins_a, "wins_b": wins_b, "ties": ties, "n_nonzero": 0, "p_two_sided": 1.0}
    # Exact two-sided binomial p with H0 p=0.5.
    smaller = min(wins_a, wins_b)
    from math import comb
    tail = sum(comb(n, k) for k in range(smaller + 1)) / (2 ** n)
    p = min(1.0, 2 * tail)
    return {"wins_a": wins_a, "wins_b": wins_b, "ties": ties, "n_nonzero": n, "p_two_sided": p}


def pair_modes(csv_a: Path, csv_b: Path, label_a: str, label_b: str) -> None:
    """Read two ranks CSVs, run paired sign test on Success@3 per stratum, print table."""
    def load(path: Path) -> dict[str, dict[str, int]]:
        rows = {}
        with path.open() as f:
            for r in csv.DictReader(f):
                rows[r["id"]] = {"stratum": f"{r['lang']}/{r['stratum']}", "rank": int(r["rank"])}
        return rows
    a = load(csv_a)
    b = load(csv_b)
    by_stratum: dict[str, tuple[list[int], list[int]]] = {}
    for qid in sorted(set(a) & set(b)):
        s = a[qid]["stratum"]
        by_stratum.setdefault(s, ([], []))
        by_stratum[s][0].append(a[qid]["rank"])
        by_stratum[s][1].append(b[qid]["rank"])
    print(f"# Paired sign test: {label_a} vs {label_b} (Success@{K_FOR_SUCCESS})")
    print()
    print(f"| stratum | n | {label_a}-only wins | {label_b}-only wins | ties | n_nonzero | p (two-sided) |")
    print(f"|---------|---|----|----|------|-----------|---------------|")
    for s in sorted(by_stratum):
        ar, br = by_stratum[s]
        t = paired_sign_test(ar, br)
        print(f"| {s} | {len(ar)} | {t['wins_a']} | {t['wins_b']} | {t['ties']} | {t['n_nonzero']} | {t['p_two_sided']:.3f} |")


def main() -> int:
    if len(sys.argv) == 5:
        queries = Path(sys.argv[1])
        results = Path(sys.argv[2])
        mode_label = sys.argv[3]
        out_csv = Path(sys.argv[4])
        if not queries.exists():
            print(f"queries file not found: {queries}", file=sys.stderr)
            return 66
        if not results.is_dir():
            print(f"results dir not found: {results}", file=sys.stderr)
            return 66
        score_mode(queries, results, mode_label, out_csv)
        return 0
    if len(sys.argv) == 6 and sys.argv[1] == "pair":
        pair_modes(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], sys.argv[5])
        return 0
    print("usage:", file=sys.stderr)
    print("  score.py <queries.tsv> <results_dir> <mode_label> <out_csv>", file=sys.stderr)
    print("  score.py pair <csv_a> <csv_b> <label_a> <label_b>", file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main())
