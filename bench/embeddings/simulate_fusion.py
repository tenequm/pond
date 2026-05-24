#!/usr/bin/env python3
"""Replay FTS and Vector arm outputs through different fusion functions and
score against ground truth. Sweeps many fusion variants in seconds instead
of recompiling pond and re-running the benchmark for each.

Inputs:
- queries.tsv: id, lang, stratum, query, ground_truth
- fts_dir/<id>.json: pond FTS-only result envelope (hits, in BM25 order)
- vector_dir/<id>.json: pond Vector-only result envelope (hits, in cosine order)

CAVEAT - arm pool sizes MUST match production for predictions to be honest:
- Production hybrid (handlers.rs:plan_search) runs `pool=100` for FTS and
  `vector_pool=200` for Vector internally, then fuses those candidates.
- A simulator fed arm JSONs captured at `pond search --limit 20` sees only the
  top-20 from each arm. For queries where a noise session sits at rank 30-50
  in one arm, the simulator never sees it (so no cross-arm agreement is
  recorded) but production does (so it dominates fusion). The result: every
  variant looks better in the simulator than in production.
- Capture fixtures with `bench/embeddings/run.sh <mode> <queries.tsv> <dir> 100`
  for FTS and `... 200` for Vector before simulating. `fixtures/` ships
  archived pool-sized arms; `results/` is for ad-hoc captures.

The script extracts each hit's `session_id`, `base_score`, and `timestamp`.
For each fusion variant, it produces a fused ranked list of session_roots
and computes Success@3, P@1, and MRR against ground truth.

For pond's data:
- FTS `base_score`: `bm25_score / max(bm25_scores)` -> [0, 1] (max-normalized
  BM25; absolute confidence is NOT preserved).
- Vector `base_score`: rank-normalized `1 - rank/n` (cosine magnitude is NOT
  preserved either; vector is rank-only on this side).
"""

from __future__ import annotations

import csv
import json
import math
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

K_FOR_SUCCESS = 3
# Mirrors src/handlers.rs - RECENCY_MAX_BOOST and RECENCY_DECAY_SECONDS.
# Production applies this AFTER fusion (handlers.rs:1336-1340), so to predict
# production we model it the same way.
RECENCY_MAX_BOOST = 0.05
RECENCY_DECAY_SECONDS = 604_800.0  # one week


def nfc(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def session_root(session_id: str) -> str:
    idx = session_id.find("/")
    return session_id[:idx] if idx >= 0 else session_id


def parse_ground_truth(spec: str) -> tuple[str, list[str]]:
    if spec.startswith("prefix:"):
        return ("prefix", [tok.strip() for tok in spec[7:].split(",") if tok.strip()])
    if spec.startswith("anchor:"):
        return ("anchor", [nfc(spec[7:].strip().lower())])
    raise ValueError(f"unknown ground-truth scheme: {spec!r}")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        ts = datetime.fromisoformat(value)
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def load_arm(path: Path) -> list[dict]:
    """Return list of {session_id, message_id, base_score, text, timestamp}."""
    if not path.exists():
        return []
    payload = json.loads(path.read_text())
    hits = payload.get("hits") or []
    return [
        {
            "session_id": h["session_id"],
            "message_id": h["message_id"],
            "base_score": float(h.get("base_score", 0.0)),
            "text": h.get("text") or "",
            "timestamp": _parse_timestamp(h.get("timestamp")),
        }
        for h in hits
    ]


def dedup_by_session_root(hits: list[dict]) -> list[dict]:
    """Keep highest-ranked hit per session_root, preserving order."""
    seen: set[str] = set()
    out: list[dict] = []
    for h in hits:
        root = session_root(h["session_id"])
        if root in seen:
            continue
        seen.add(root)
        h2 = dict(h)
        h2["root"] = root
        out.append(h2)
    return out


# ---------- Fusion variants ----------

def fuse_rrf(fts: list[dict], vec: list[dict], k: float = 10.0) -> list[dict]:
    """Baseline RRF on session_root, equal weight, intra-arm dedup."""
    return fuse_weighted_asymmetric_rrf(fts, vec, k_fts=k, k_vec=k, w_fts=1.0, w_vec=1.0)


def fuse_weighted_asymmetric_rrf(
    fts: list[dict],
    vec: list[dict],
    k_fts: float,
    k_vec: float,
    w_fts: float,
    w_vec: float,
) -> list[dict]:
    """Per-arm k and weight: score(d) = w_fts/(k_fts+rank_fts) + w_vec/(k_vec+rank_vec)."""
    fts = dedup_by_session_root(fts)
    vec = dedup_by_session_root(vec)
    merged: dict[str, dict] = {}
    for rank, h in enumerate(fts, 1):
        merged[h["root"]] = {
            "root": h["root"],
            "score": w_fts / (k_fts + rank),
            "text": h["text"],
            "session_id": h["session_id"],
            "message_id": h["message_id"],
            "timestamp": h.get("timestamp"),
        }
    for rank, h in enumerate(vec, 1):
        entry = merged.get(h["root"])
        if entry is None:
            merged[h["root"]] = {
                "root": h["root"],
                "score": w_vec / (k_vec + rank),
                "text": h["text"],
                "session_id": h["session_id"],
                "message_id": h["message_id"],
                "timestamp": h.get("timestamp"),
            }
        else:
            entry["score"] += w_vec / (k_vec + rank)
            if entry.get("timestamp") is None and h.get("timestamp") is not None:
                entry["timestamp"] = h.get("timestamp")
    return sorted(merged.values(), key=lambda e: (-e["score"], e["root"]))


def fuse_convex_combination(
    fts: list[dict],
    vec: list[dict],
    alpha: float = 0.3,
) -> list[dict]:
    """Pure convex combination on per-arm normalized scores.
    score(d) = (1-alpha) * fts_norm(d) + alpha * vec_norm(d)
    Vector "norm" here is the rank-based 1-rank/n that pond uses.
    """
    fts = dedup_by_session_root(fts)
    vec = dedup_by_session_root(vec)
    n_v = len(vec)
    merged: dict[str, dict] = {}
    for h in fts:
        merged[h["root"]] = {
            "root": h["root"],
            "score": (1 - alpha) * h["base_score"],
            "text": h["text"],
            "session_id": h["session_id"],
            "message_id": h["message_id"],
            "timestamp": h.get("timestamp"),
        }
    for rank, h in enumerate(vec, 1):
        vec_norm = 1.0 - (rank - 1) / max(n_v, 1)
        entry = merged.get(h["root"])
        contribution = alpha * vec_norm
        if entry is None:
            merged[h["root"]] = {
                "root": h["root"],
                "score": contribution,
                "text": h["text"],
                "session_id": h["session_id"],
                "message_id": h["message_id"],
                "timestamp": h.get("timestamp"),
            }
        else:
            entry["score"] += contribution
            if entry.get("timestamp") is None and h.get("timestamp") is not None:
                entry["timestamp"] = h.get("timestamp")
    return sorted(merged.values(), key=lambda e: (-e["score"], e["root"]))


def fuse_combanz(fts: list[dict], vec: list[dict]) -> list[dict]:
    """CombANZ (Fox & Shaw 1994): sum of normalized scores divided by number of
    arms in which the doc appears. Cross-arm agreement does NOT amplify (unlike
    CombMNZ). Vector "score" is rank-based 1-rank/n.
    """
    fts = dedup_by_session_root(fts)
    vec = dedup_by_session_root(vec)
    n_v = len(vec)
    merged: dict[str, dict] = {}
    for h in fts:
        merged[h["root"]] = {
            "sum": h["base_score"],
            "arms": 1,
            "text": h["text"],
            "session_id": h["session_id"],
            "message_id": h["message_id"],
            "root": h["root"],
            "timestamp": h.get("timestamp"),
        }
    for rank, h in enumerate(vec, 1):
        vec_norm = 1.0 - (rank - 1) / max(n_v, 1)
        entry = merged.get(h["root"])
        if entry is None:
            merged[h["root"]] = {
                "sum": vec_norm,
                "arms": 1,
                "text": h["text"],
                "session_id": h["session_id"],
                "message_id": h["message_id"],
                "root": h["root"],
                "timestamp": h.get("timestamp"),
            }
        else:
            entry["sum"] += vec_norm
            entry["arms"] += 1
            if entry.get("timestamp") is None and h.get("timestamp") is not None:
                entry["timestamp"] = h.get("timestamp")
    out = []
    for v in merged.values():
        v["score"] = v["sum"] / v["arms"]
        out.append(v)
    return sorted(out, key=lambda e: (-e["score"], e["root"]))


def fuse_fts_confidence_gated_decay(
    fts: list[dict],
    vec: list[dict],
    base_k_fts: float = 5,
    base_k_vec: float = 20,
    low_k_fts: float = 30,
    decay_threshold: float = 0.05,
) -> list[dict]:
    """Confidence-gate: when FTS top-N base_scores hug ~1.0 (uniformly confident
    -> likely noise on this query), flatten the FTS arm's contribution by
    using a larger k_fts. Otherwise the standard EN-tuned asym RRF.

    Heuristic: decay = top1 - mean(top2..5) of base_scores. Decay < threshold
    means FTS lacks a clear winner.
    """
    fts_dd = dedup_by_session_root(fts)
    k_fts = base_k_fts
    if len(fts_dd) >= 5:
        scores = [h["base_score"] for h in fts_dd[:5]]
        decay = scores[0] - sum(scores[1:5]) / 4
        if decay < decay_threshold:
            k_fts = low_k_fts
    return fuse_weighted_asymmetric_rrf(fts, vec, k_fts, base_k_vec, 1.0, 1.0)


def fuse_router(fts: list[dict], vec: list[dict], query: str | None = None) -> list[dict]:
    """Mirrors src/handlers.rs::fusion_config_for. Latin-dominant queries use
    EN-tuned asym k (k_fts=5, k_vec=20, w=1/1); non-Latin queries use balanced
    k (k=10, w_fts=1, w_vec=2). Pass `query` via score_variant's wrapper.
    """
    # Default behavior without query context: EN config.
    if query and _is_non_latin_dominant(query):
        return fuse_weighted_asymmetric_rrf(fts, vec, 10, 10, 1.0, 2.0)
    return fuse_weighted_asymmetric_rrf(fts, vec, 5, 20, 1.0, 1.0)


def _is_non_latin_dominant(query: str) -> bool:
    """Mirrors src/handlers.rs::is_non_latin_dominant - 30% non-ASCII alpha."""
    latin = non_latin = 0
    for ch in query:
        if ch.isalpha():
            if ch.isascii():
                latin += 1
            else:
                non_latin += 1
    total = latin + non_latin
    return total > 0 and non_latin * 10 >= total * 3


# ---------- Recency modeling ----------

def apply_recency(
    fused: list[dict],
    now: datetime,
    max_boost: float = RECENCY_MAX_BOOST,
    decay_seconds: float = RECENCY_DECAY_SECONDS,
) -> list[dict]:
    """Layer the same additive exponential-decay recency boost production
    applies post-fusion (handlers.rs:recency_boost). A no-op if `now` is None
    or hit has no timestamp.
    """
    if now is None:
        return fused
    for h in fused:
        ts = h.get("timestamp")
        if ts is None:
            continue
        age = max((now - ts).total_seconds(), 0.0)
        h["score"] = h["score"] + max_boost * math.exp(-age / decay_seconds)
    return sorted(fused, key=lambda e: (-e["score"], e["root"]))


# ---------- Scoring ----------

def find_target_rank(ranked: list[dict], kind: str, tokens: list[str]) -> int:
    for idx, hit in enumerate(ranked, 1):
        if kind == "prefix":
            sid = hit.get("session_id", "")[:8]
            mid = hit.get("message_id", "")[:8]
            root_8 = hit.get("root", "")[:8]
            if sid in tokens or mid in tokens or root_8 in tokens:
                return idx
        else:  # anchor
            text = nfc((hit.get("text") or "").lower())
            if any(tok in text for tok in tokens):
                return idx
    return 0


def score_variant(
    name: str,
    fuse_fn,
    queries: list[dict],
    fts_dir: Path,
    vec_dir: Path,
    *,
    now: datetime | None = None,
    apply_recency_boost: bool = False,
    pass_query: bool = False,
) -> dict:
    per_query: list[dict] = []
    for q in queries:
        qid = q["id"]
        fts_hits = load_arm(fts_dir / f"{qid}.json")
        vec_hits = load_arm(vec_dir / f"{qid}.json")
        if pass_query:
            fused = fuse_fn(fts_hits, vec_hits, q["query"])
        else:
            fused = fuse_fn(fts_hits, vec_hits)
        if apply_recency_boost:
            fused = apply_recency(fused, now)
        kind, tokens = parse_ground_truth(q["ground_truth"])
        rank = find_target_rank(fused, kind, tokens)
        per_query.append({"id": qid, "stratum": q["stratum"], "lang": q["lang"], "rank": rank})

    n = len(per_query)
    s3 = sum(1 for r in per_query if 1 <= r["rank"] <= K_FOR_SUCCESS)
    p1 = sum(1 for r in per_query if r["rank"] == 1)
    mrr = sum((1.0 / r["rank"]) if r["rank"] >= 1 else 0.0 for r in per_query) / n if n else 0.0
    return {
        "name": name,
        "n": n,
        "s3": s3,
        "p1": p1,
        "mrr": mrr,
        "per_query": per_query,
    }


def main() -> int:
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print(
            "usage: simulate_fusion.py <queries.tsv> <fts_dir> <vector_dir> [now_iso]\n"
            "  now_iso: ISO-8601 timestamp for recency-boost reference (e.g.\n"
            "  2026-05-24T00:00:00Z). Required to enable recency modeling.",
            file=sys.stderr,
        )
        return 64
    queries_path = Path(sys.argv[1])
    fts_dir = Path(sys.argv[2])
    vec_dir = Path(sys.argv[3])
    now = _parse_timestamp(sys.argv[4]) if len(sys.argv) == 5 else None
    if now is None and len(sys.argv) == 5:
        print(f"could not parse now_iso: {sys.argv[4]!r}", file=sys.stderr)
        return 64

    queries: list[dict] = []
    with queries_path.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            queries.append(row)

    variants = [
        ("baseline-rrf-k10", lambda f, v: fuse_rrf(f, v, k=10), False),
        ("asym-kfts5-kvec20", lambda f, v: fuse_weighted_asymmetric_rrf(f, v, 5, 20, 1.0, 1.0), False),
        ("router (production)", fuse_router, True),
        ("uk-balanced-k10-wvec2", lambda f, v: fuse_weighted_asymmetric_rrf(f, v, 10, 10, 1.0, 2.0), False),
        ("uk-balanced-k10-wvec3", lambda f, v: fuse_weighted_asymmetric_rrf(f, v, 10, 10, 1.0, 3.0), False),
        ("convex-a0.5", lambda f, v: fuse_convex_combination(f, v, alpha=0.5), False),
        ("convex-a0.7", lambda f, v: fuse_convex_combination(f, v, alpha=0.7), False),
        ("combanz", fuse_combanz, False),
        ("fts-gated-decay<0.05", fuse_fts_confidence_gated_decay, False),
    ]

    use_recency = now is not None
    apply_label = " (recency on)" if use_recency else " (no recency)"
    print(f"variant{apply_label:<24}            {'S@3':>5} {'P@1':>5} {'MRR':>6}")
    print("-" * 70)
    for name, fn, pass_query in variants:
        r = score_variant(
            name, fn, queries, fts_dir, vec_dir,
            now=now, apply_recency_boost=use_recency, pass_query=pass_query,
        )
        print(f"{name:<40} {r['s3']:>2}/{r['n']:<2} {r['p1']:>2}/{r['n']:<2} {r['mrr']:.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
