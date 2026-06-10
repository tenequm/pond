#!/usr/bin/env python3
"""Capture production-faithful FTS + vector arm fixtures with RAW scores.

Why this exists: `pond search --format json` emits the grouped `sessions`
envelope, which caps matches per session - CLI captures are not full arm
pools, so `bench.py sweep` replays over them understate cross-arm signal.
This script reads the Lance tables directly (pylance) and writes one
`{"hits": [...]}` envelope per query into --fts-out / --vec-out, the shape
`bench.py sweep --fts-fixtures/--vector-fixtures` consumes.

Faithfulness to src/handlers.rs::run_search:
- FTS arm: Lance inverted-index match query over messages.search_text,
  `_score` = raw BM25.
- Vector arm: IVF_PQ cosine kNN over messages.vector, query embedded with
  intfloat/multilingual-e5-small, "query: " prefix, mean pooling + L2 norm
  (mirrors src/embed.rs); `_score` = cosine similarity (1 - distance), the
  same magnitude the production fusion consumes.
- Both arms prefiltered to exclude subagent sessions (the search default);
  pass --include-subagents to keep them.

Run with: uv run --with pylance --with sentence-transformers \
    python capture_arms.py --queries <set.tsv> --fts-out <dir> --vec-out <dir>
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

SUBAGENT_EXCLUSION = "NOT (source_agent LIKE '%/%')"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", required=True, help="TSV: id\\tlang\\tstratum\\tquery\\tground_truth")
    parser.add_argument("--fts-out", required=True, help="Output dir for FTS arm envelopes")
    parser.add_argument("--vec-out", required=True, help="Output dir for vector arm envelopes")
    parser.add_argument("--data-dir", default=str(Path.home() / ".local/share/pond"))
    parser.add_argument("--fts-limit", type=int, default=100, help="FTS pool (production: limit*5, min 50)")
    parser.add_argument("--vec-limit", type=int, default=200, help="Vector pool (production: 2x FTS pool)")
    parser.add_argument("--include-subagents", action="store_true")
    args = parser.parse_args()

    import lance
    from sentence_transformers import SentenceTransformer

    rows = list(csv.DictReader(open(args.queries), delimiter="\t"))
    print(f"{len(rows)} queries", flush=True)

    model = SentenceTransformer("intfloat/multilingual-e5-small")
    vectors = model.encode(
        ["query: " + r["query"] for r in rows], normalize_embeddings=True, batch_size=32
    )

    dataset = lance.dataset(str(Path(args.data_dir) / "messages.lance"))
    scope = None if args.include_subagents else SUBAGENT_EXCLUSION
    fts_dir, vec_dir = Path(args.fts_out), Path(args.vec_out)
    fts_dir.mkdir(parents=True, exist_ok=True)
    vec_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    for row, query_vector in zip(rows, vectors):
        try:
            table = dataset.to_table(
                columns=["session_id", "id"],
                full_text_query=row["query"],
                limit=args.fts_limit,
                filter=scope,
                prefilter=True,
            )
            fts_hits = [
                {
                    "session_id": table.column("session_id")[i].as_py(),
                    "message_id": table.column("id")[i].as_py(),
                    "_score": table.column("_score")[i].as_py(),
                }
                for i in range(table.num_rows)
            ]
        except Exception as error:  # noqa: BLE001 - per-query capture continues
            fts_hits = []
            print(f"  FTS error {row['id']}: {error}", file=sys.stderr, flush=True)
        try:
            table = dataset.to_table(
                columns=["session_id", "id"],
                nearest={"column": "vector", "q": query_vector, "k": args.vec_limit},
                filter=scope,
                prefilter=True,
            )
            vec_hits = [
                {
                    "session_id": table.column("session_id")[i].as_py(),
                    "message_id": table.column("id")[i].as_py(),
                    # Cosine similarity, the magnitude production fusion consumes.
                    "_score": 1.0 - table.column("_distance")[i].as_py(),
                }
                for i in range(table.num_rows)
            ]
        except Exception as error:  # noqa: BLE001 - per-query capture continues
            vec_hits = []
            print(f"  vector error {row['id']}: {error}", file=sys.stderr, flush=True)
        (fts_dir / f"{row['id']}.json").write_text(json.dumps({"hits": fts_hits}))
        (vec_dir / f"{row['id']}.json").write_text(json.dumps({"hits": vec_hits}))
        print(
            f"  {row['id']} fts={len(fts_hits)} vec={len(vec_hits)} ({time.time() - started:.0f}s)",
            flush=True,
        )
    print(f"done: arm fixtures in {fts_dir} and {vec_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
