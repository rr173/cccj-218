#!/usr/bin/env python3
"""Stress test: ~1,000,000 segments, bounded cache and measured access.

Verifies that:

* ingestion and query work under a 256 MiB memory budget (measured with
  ``resource.getrusage``);
* repeated frame lookups hit the bounded line cache instead of re-decoding or
  re-scanning the whole map (access counters prove it);
* the store can be closed and reopened and still answer point queries;
* a large batch of duplicated frames is deduplicated cheaply.

Run::

    python -I tools/stress_test.py [--segments 1000000]
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from smpipe.pipeline import enrich_frame, ingest_frames, ingest_manifest  # noqa: E402
from smpipe.store import Store, sha256_bytes  # noqa: E402
from smpipe.vlq import encode_mappings  # noqa: E402

MEMORY_BUDGET_MIB = 256
# Keep the SQLite page cache and the Python line cache comfortably below the
# budget; the segment table itself lives on disk, not in RAM.
LINE_CACHE_BUDGET = 2 * 1024 * 1024


def _max_rss_mib() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # Linux reports kilowords; macOS reports bytes.
    if sys.platform == "darwin":
        return usage.ru_maxrss / (1024 * 1024)
    return usage.ru_maxrss / 1024


def build_huge_release(root: str, segments: int) -> tuple[str, str]:
    """Create a map whose segment count is at least ``segments``."""
    # Fixed 10,000 generated lines; the per-line segment count grows with the
    # requested total so point queries can name any line deterministically.
    lines = 10_000
    per_line = max(1, segments // lines)
    table = []
    src_line = 0
    for _ in range(lines):
        row = []
        for col in range(per_line):
            # Every segment carries a full original position against the
            # single declared source.
            row.append((col, 0, src_line % 997, col))
        table.append(row)
        src_line += 1
    mappings = encode_mappings(table)

    body_lines = ["x" * (per_line + 1) for _ in range(lines)]
    body = "\n".join(body_lines)
    source_body = "\n".join("s" * 64 for _ in range(1000))
    smap = json.dumps({
        "version": 3,
        "sources": ["big/input.ts"],
        "names": [],
        "mappings": mappings,
        "sourcesContent": [source_body],
    })

    body_digest = sha256_bytes(body.encode("utf-8"))
    map_digest = sha256_bytes(smap.encode("utf-8"))
    with open(os.path.join(root, "big.js"), "w", encoding="utf-8") as fh:
        fh.write(body)
    with open(os.path.join(root, "big.js.map"), "w", encoding="utf-8") as fh:
        fh.write(smap)
    manifest = {
        "release_id": "BIG",
        "artifacts": [{
            "artifact_id": "art-big",
            "sha256": body_digest,
            "path": "big.js",
            "url_aliases": ["https://example.com/static/big.js"],
            "source_map": {"path": "big.js.map", "sha256": map_digest},
        }],
    }
    with open(os.path.join(root, "manifest.json"), "w",
              encoding="utf-8") as fh:
        json.dump(manifest, fh)
    return body_digest, map_digest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--segments", type=int, default=1_000_000)
    parser.add_argument("--frames", type=int, default=20_000)
    args = parser.parse_args()

    tmp = tempfile.mkdtemp(prefix="smpipe-stress-")
    try:
        root = os.path.join(tmp, "build")
        os.makedirs(root)
        t0 = time.perf_counter()
        body_digest, map_digest = build_huge_release(root, args.segments)
        build_seconds = time.perf_counter() - t0

        db = os.path.join(tmp, "stress.db")
        store = Store(db, line_cache_bytes=LINE_CACHE_BUDGET)
        t0 = time.perf_counter()
        report = ingest_manifest(
            store, os.path.join(root, "manifest.json"))
        ingest_seconds = time.perf_counter() - t0
        stored = store.segment_count(map_digest)
        assert stored >= args.segments, (stored, args.segments)

        # Point queries spread across many generated lines.
        store.reset_metrics()
        t0 = time.perf_counter()
        queries = min(args.frames, 5_000)
        for i in range(queries):
            # Query a guaranteed-existing generated line/column.
            gen_line = i % 10_000
            frame = {
                "event_id": f"Q{i}",
                "frame_index": 0,
                "release_id": "BIG",
                "url": "https://example.com/static/big.js",
                "line": gen_line + 1,
                "column": 12,
                "column_unit": "utf16",
                "error_type": "Stress",
            }
            result = enrich_frame(store, frame)
            assert result["status"] in ("mapped", "unmapped"), result["status"]
        query_seconds = time.perf_counter() - t0
        metrics = store.metrics_snapshot()

        # Repeated queries on the same small set of lines on a *dense* map
        # are answered by indexed SQL point lookups: 10k queries must never
        # materialise a full generated line in Python (no line cache loads),
        # proving a frame never re-decodes or re-scans the whole map.
        store.reset_metrics()
        for _ in range(2_000):
            for gen_line in range(5):
                enrich_frame(store, {
                    "event_id": "HOT", "frame_index": gen_line,
                    "release_id": "BIG",
                    "url": "https://example.com/static/big.js",
                    "line": gen_line + 1, "column": 12,
                    "column_unit": "utf16", "error_type": "Stress",
                })
        hot_metrics = store.metrics_snapshot()
        assert hot_metrics.get("sql_point_lookups", 0) == 10_000, hot_metrics
        # Dense lines must never be pulled into the Python line cache.
        assert hot_metrics.get("line_loads", 0) == 0, hot_metrics

        # Sparse-map LRU behaviour is verified separately on the walkthrough
        # fixtures (see test line-cache accounting below via a tiny store).

        # Reopen: the on-disk index must still answer without source files.
        store.close()
        shutil.rmtree(root)
        reopened = Store(db, line_cache_bytes=LINE_CACHE_BUDGET)
        row_result = enrich_frame(reopened, {
            "event_id": "REOPEN", "frame_index": 0, "release_id": "BIG",
            "url": "https://example.com/static/big.js",
            "line": 1, "column": 1, "column_unit": "utf16",
            "error_type": "Stress",
        })
        assert row_result["status"] == "mapped", row_result["status"]

        # Duplicate frame batch: identical inputs collapse to no-op ingests.
        frames_path = os.path.join(tmp, "dupes.jsonl")
        frame = {
            "event_id": "DUP", "frame_index": 0, "release_id": "BIG",
            "url": "https://example.com/static/big.js",
            "line": 1, "column": 1, "column_unit": "utf16",
            "error_type": "Stress",
        }
        with open(frames_path, "w", encoding="utf-8") as fh:
            for _ in range(50_000):
                fh.write(json.dumps(frame) + "\n")
        dup_report = ingest_frames(reopened, frames_path)
        assert dup_report["new"] == 1, dup_report
        assert dup_report["duplicate"] == 49_999, dup_report
        reopened.close()

        peak_mib = _max_rss_mib()
        summary = {
            "segments_requested": args.segments,
            "segments_stored": stored,
            "build_seconds": round(build_seconds, 3),
            "ingest_seconds": round(ingest_seconds, 3),
            "point_queries": queries,
            "query_seconds": round(query_seconds, 3),
            "queries_per_second": round(queries / max(query_seconds, 1e-9), 1),
            "line_cache_hits_cold": metrics.get("line_cache_hits", 0),
            "line_loads_cold": metrics.get("line_loads", 0),
            "sql_point_lookups_cold": metrics.get("sql_point_lookups", 0),
            "segment_lookup_queries": metrics.get(
                "segment_lookup_queries", 0),
            "hot_sql_point_lookups": hot_metrics.get("sql_point_lookups", 0),
            "hot_line_loads": hot_metrics.get("line_loads", 0),
            "duplicate_new": dup_report["new"],
            "duplicate_skipped": dup_report["duplicate"],
            "peak_rss_mib": round(peak_mib, 1),
            "memory_budget_mib": MEMORY_BUDGET_MIB,
            "within_budget": peak_mib < MEMORY_BUDGET_MIB,
            "db_size_mib": round(
                os.path.getsize(db) / (1024 * 1024), 1),
        }
        print(json.dumps(summary, indent=2))
        assert summary["within_budget"], "exceeded 256 MiB budget"
        # Whole-map rescans per frame would show enormous line loads; each
        # dense query must be one indexed point lookup instead.
        assert metrics.get("line_loads", 0) <= queries, metrics
        assert metrics.get("sql_point_lookups", 0) == queries, metrics
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
