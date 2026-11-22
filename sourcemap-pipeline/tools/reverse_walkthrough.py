#!/usr/bin/env python3
"""Reproducible walkthrough for the source-range reverse index.

The script is self-contained and never writes into the source tree:

* authors a small hand-written build (four releases) covering the emoji
  line proof, multi-entry convergence on one intermediate point, explicit
  bindings resolved per release, URL-alias dedup, an entry-dependent cycle,
  broken/unmapped/invalid branches next to valid ones, and a terminal source
  whose body was never pinned;
* ingests it with the real ``ingest-build`` CLI, seals the database
  (WAL checkpoint), builds the reverse index with ``build-reverse-index``
  and queries it with ``query-reverse-index``;
* enumerates expected hits independently -- per character, straight from
  the hand-written segment tables -- and requires exact equality with the
  index contents for the small releases;
* adds a runtime-generated long line (2,000,000 columns) plus 4,000 entry
  segments converging on one shared intermediate point, and shows from the
  recorded counters that indexing work grows with intervals and shared hop
  points, not with character counts;
* prints the query plan of a narrow query (bounded index SEARCH), then
  moves the sealed build database and the build directory away and re-runs
  the queries against the index file alone.

Run (isolated mode, as every other entry point)::

    python3 -I tools/reverse_walkthrough.py --out ./run/reverse-demo
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))

from smpipe.reverse import ReverseIndex  # noqa: E402
from smpipe.store import sha256_bytes  # noqa: E402
from smpipe.vlq import encode_mappings  # noqa: E402

SMPIPE = os.path.join(ROOT, "smpipe.py")

EMOJI_LINE = "ab\U0001F600cdef"  # UTF-16 cols: a0 b1 😀2-3 c4 d5 e6 f7
LONG_LINE_CHARS = 2_000_000
FANIN_SEGMENTS = 4_000


# --------------------------------------------------------------------------
# Hand-written build specification (the "expected" side of every check)
# --------------------------------------------------------------------------
def fixture_spec() -> dict:
    """The whole demo build as hand-authored segment tables.

    Segment tuples are ``(gen_col,)`` for generated-only segments and
    ``(gen_col, source_index, src_line, src_col)`` for mapped ones -- the
    same shape the fixture generator feeds to the VLQ encoder.
    """
    return {
        # RA: emoji line proof + branch isolation + convergence + aliases.
        "RA": {
            "artifacts": [
                {
                    "id": "app.min.js",
                    # Two URL aliases, still ONE entry.
                    "aliases": ["https://example.com/app.js",
                                "https://example.com/app.alias.js"],
                    "body": EMOJI_LINE + "\naabbccddee\nq",
                    "map": {
                        "sources": ["mid.js", "src/orig.ts", "gap.js",
                                    "badmap.js", "src/nowhere.ts"],
                        "lines": {
                            # The emoji line: mapped / no-original / mapped.
                            0: [(1, 0, 0, 4), (5,), (6, 0, 0, 9)],
                            # Branch line: terminal hit, hop hit, unmapped
                            # hop, invalid-map hop, generated-only.
                            1: [(0, 1, 1, 0), (2, 0, 0, 0), (4, 2, 0, 0),
                                (6, 3, 0, 0), (8,)],
                            # Terminal source whose body is never pinned.
                            2: [(0, 4, 3, 3)],
                        },
                    },
                    "bindings": {"mid.js": "art-mid", "gap.js": "art-gap",
                                 "badmap.js": "art-bad"},
                },
                {
                    "id": "app2.min.js",
                    "aliases": ["https://example.com/app2.js"],
                    "body": "zz",
                    "map": {"sources": ["mid.js"],
                            "lines": {0: [(0, 0, 0, 4)]}},
                    "bindings": {"mid.js": "art-mid"},
                },
                {
                    # Intermediate WITHOUT an alias: never an entry.
                    "id": "art-mid",
                    "aliases": [],
                    "body": "0123456789abcdef",
                    "map": {"sources": ["src/orig.ts"],
                            "lines": {0: [(0, 0, 1, 1), (4, 0, 0, 10),
                                          (9, 0, 0, 10)]}},
                    "bindings": {},
                },
                {
                    "id": "art-gap",
                    "aliases": [],
                    "body": "xyz",
                    # Segments exist only on line 3; a hop to (0,0) must not
                    # borrow them from another line.
                    "map": {"sources": ["src/orig.ts"],
                            "lines": {3: [(0, 0, 0, 0)]}},
                    "bindings": {},
                },
                {
                    "id": "art-bad",
                    "aliases": [],
                    "body": "qq",
                    "bad_map": True,  # corrupt VLQ on purpose
                    "bindings": {},
                },
            ],
            "pins": {"src/orig.ts": "// original source\nline one\n"},
        },
        # RB: same final source identity reached through RB's own bindings;
        # the source body is not pinned in this release at all.
        "RB": {
            "artifacts": [
                {
                    "id": "rb-bundle.js",
                    "aliases": ["https://example.com/rb.js"],
                    "body": "hello",
                    "map": {"sources": ["rbmid.js"],
                            "lines": {0: [(0, 0, 0, 2)]}},
                    "bindings": {"rbmid.js": "art-rbmid"},
                },
                {
                    "id": "art-rbmid",
                    "aliases": [],
                    "body": "abcdef",
                    "map": {"sources": ["src/orig.ts"],
                            "lines": {0: [(2, 0, 0, 10)]}},
                    "bindings": {},
                },
            ],
            "pins": {},
        },
        # RC: one shared hop point whose outcome differs by entry path.
        #   cyc-a -> cycmid -> cyc-b(0,10) -> terminal hit
        #   cyc-b -> cycmid -> cyc-b(0,10) -> cycle (back to itself)
        "RC": {
            "artifacts": [
                {
                    "id": "cyc-a",
                    "aliases": ["https://example.com/cyc-a.js"],
                    "body": "w",
                    "map": {"sources": ["midc.js"],
                            "lines": {0: [(0, 0, 0, 0)]}},
                    "bindings": {"midc.js": "art-cycmid"},
                },
                {
                    "id": "cyc-b",
                    "aliases": ["https://example.com/cyc-b.js"],
                    "body": "0123456789AB",
                    "map": {"sources": ["midc.js", "src/final.ts"],
                            "lines": {0: [(0, 0, 0, 0), (10, 1, 0, 7)]}},
                    "bindings": {"midc.js": "art-cycmid"},
                },
                {
                    "id": "art-cycmid",
                    "aliases": [],
                    "body": "yy",
                    "map": {"sources": ["back.js"],
                            "lines": {0: [(0, 0, 0, 10)]}},
                    "bindings": {"back.js": "cyc-b"},
                },
            ],
            "pins": {},
        },
        # RD: runtime-generated scale proof -- one 2M-column line covered by
        # three segments, and 4,000 entry segments converging on one shared
        # intermediate coordinate.
        "RD": {
            "artifacts": [
                {
                    "id": "fanin.js",
                    "aliases": ["https://example.com/fanin.js"],
                    "body": "y" * FANIN_SEGMENTS,
                    "map": {"sources": ["sharedmid.js"],
                            "lines": {0: [(i, 0, 0, 0)
                                          for i in range(FANIN_SEGMENTS)]}},
                    "bindings": {"sharedmid.js": "art-shared"},
                },
                {
                    "id": "long.js",
                    "aliases": ["https://example.com/long.js"],
                    "body": "x" * LONG_LINE_CHARS,
                    "map": {"sources": ["src/big.ts"],
                            "lines": {0: [(0, 0, 0, 0), (1000000,),
                                          (1500000, 0, 0, 1)]}},
                    "bindings": {},
                },
                {
                    "id": "art-shared",
                    "aliases": [],
                    "body": "s" * 10,
                    "map": {"sources": ["src/big.ts"],
                            "lines": {0: [(0, 0, 0, 5)]}},
                    "bindings": {},
                },
            ],
            "pins": {},
        },
    }


def _map_json(map_spec: dict) -> str:
    line_count = max(map_spec["lines"], default=-1) + 1
    lines = [[] for _ in range(line_count)]
    for gen_line, segments in map_spec["lines"].items():
        lines[gen_line] = [tuple(seg) for seg in segments]
    return json.dumps({
        "version": 3,
        "sources": map_spec["sources"],
        "names": [],
        "mappings": encode_mappings(lines),
    }, indent=2)


def build_fixtures(root: str, spec: dict) -> None:
    """Write every release directory (bodies, maps, manifest) to disk."""
    for release_id, rspec in spec.items():
        base = os.path.join(root, release_id.lower())
        os.makedirs(base, exist_ok=True)
        manifest_artifacts = []
        for art in rspec["artifacts"]:
            body = art["body"].encode("utf-8")
            with open(os.path.join(base, art["id"]), "wb") as handle:
                handle.write(body)
            entry = {
                "artifact_id": art["id"],
                "sha256": sha256_bytes(body),
                "path": art["id"],
            }
            if art["aliases"]:
                entry["url_aliases"] = art["aliases"]
            if art.get("bad_map"):
                map_text = json.dumps({
                    "version": 3, "sources": ["x.js"], "names": [],
                    "mappings": "~AAA",  # corrupt VLQ on purpose
                })
            elif "map" in art:
                map_text = _map_json(art["map"])
            else:
                map_text = None
            if map_text is not None:
                map_path = art["id"] + ".map"
                map_bytes = map_text.encode("utf-8")
                with open(os.path.join(base, map_path), "wb") as handle:
                    handle.write(map_bytes)
                entry["source_map"] = {
                    "path": map_path, "sha256": sha256_bytes(map_bytes),
                }
            if art["bindings"]:
                entry["source_bindings"] = [
                    {"source_identity": identity,
                     "next_artifact_id": target}
                    for identity, target in sorted(art["bindings"].items())
                ]
            manifest_artifacts.append(entry)
        manifest_sources = []
        for identity, text in sorted(rspec["pins"].items()):
            data = text.encode("utf-8")
            filename = "pin-" + identity.replace("/", "_")
            with open(os.path.join(base, filename), "wb") as handle:
                handle.write(data)
            manifest_sources.append({
                "source_identity": identity,
                "sha256": sha256_bytes(data),
                "path": filename,
            })
        manifest = {"release_id": release_id,
                    "artifacts": manifest_artifacts}
        if manifest_sources:
            manifest["sources"] = manifest_sources
        with open(os.path.join(base, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2)


# --------------------------------------------------------------------------
# Independent per-character reference (never touches reverse.py internals)
# --------------------------------------------------------------------------
def _utf16_columns(text: str) -> list[int]:
    """Valid character starts of one line as zero-based UTF-16 columns."""
    columns = []
    units = 0
    for ch in text:
        columns.append(units)
        units += 2 if ord(ch) > 0xFFFF else 1
    return columns


def _covering(segments: list, col: int):
    """Nearest segment on the line with seg col <= col (never another line)."""
    best = None
    for seg in segments:
        if seg[0] <= col and (best is None or seg[0] > best[0]):
            best = seg
    return best


def _hop_final(spec: dict, release: str, art_id: str, line: int, col: int,
               visited: frozenset, depth: int):
    """Resolve one hop exactly like enrich: fixed coordinate, cycle check."""
    if depth >= 32:
        return None  # layer limit
    if art_id in visited:
        return None  # binding cycle
    visited = visited | {art_id}
    art = _artifact(spec, release, art_id)
    if art.get("bad_map") or "map" not in art:
        return None  # invalid map
    segments = art["map"]["lines"].get(line, [])
    seg = _covering(segments, col)
    if seg is None or len(seg) == 1:
        return None  # unmapped on this layer
    identity = art["map"]["sources"][seg[1]]
    return _segment_final(spec, release, art_id, identity, seg[2], seg[3],
                          visited, depth)


def _segment_final(spec: dict, release: str, owner_id: str, identity: str,
                   line: int, col: int, visited: frozenset, depth: int):
    """Resolve a segment's original coordinate: terminal or follow binding."""
    target = _artifact(spec, release, owner_id)["bindings"].get(identity)
    if target is None:
        return (identity, line, col)  # terminal; body presence irrelevant
    return _hop_final(spec, release, target, line, col, visited, depth + 1)


def _artifact(spec: dict, release: str, art_id: str) -> dict:
    for art in spec[release]["artifacts"]:
        if art["id"] == art_id:
            return art
    raise KeyError(f"{release}/{art_id}")


def reference_runs(spec: dict, releases: tuple[str, ...]) -> set[tuple]:
    """Enumerate expected hits per character from the hand-written tables.

    Returns ``(release, artifact, gen_line, col_start, col_end, identity,
    src_line, src_col)`` tuples -- one per merged run of consecutive valid
    columns that share one final source point.
    """
    runs: set[tuple] = set()
    for release in releases:
        for art in spec[release]["artifacts"]:
            if not art["aliases"]:
                continue  # entries only
            body_lines = art["body"].split("\n")
            for gen_line, segments in art["map"]["lines"].items():
                if gen_line >= len(body_lines):
                    continue
                ordered = sorted(segments, key=lambda seg: seg[0])
                current = None  # [start, end, final]
                for col in _utf16_columns(body_lines[gen_line]):
                    seg = _covering(ordered, col)
                    final = None
                    if seg is not None and len(seg) == 4:
                        identity = art["map"]["sources"][seg[1]]
                        final = _segment_final(
                            spec, release, art["id"], identity,
                            seg[2], seg[3],
                            frozenset({art["id"]}), 0,
                        )
                    if final is None:
                        if current is not None:
                            runs.add((release, art["id"], gen_line,
                                      current[0], current[1],
                                      *current[2]))
                            current = None
                        continue
                    if (current is not None and current[1] == col
                            and current[2] == final):
                        current[1] = col + 1  # consecutive, same point
                    else:
                        if current is not None:
                            runs.add((release, art["id"], gen_line,
                                      current[0], current[1],
                                      *current[2]))
                        current = [col, col + 1, final]
                if current is not None:
                    runs.add((release, art["id"], gen_line,
                              current[0], current[1], *current[2]))
    return runs


# --------------------------------------------------------------------------
# CLI helpers
# --------------------------------------------------------------------------
def run_cli(*args: str, expect_ok: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        [sys.executable, "-I", SMPIPE, *args],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if expect_ok and proc.returncode != 0:
        raise SystemExit(f"command failed: {' '.join(args)}\n{proc.stderr}")
    return proc


def query(index: str, source: str, start: tuple[int, int],
          end: tuple[int, int], explain: bool = False) -> list[dict]:
    args = ["query-reverse-index", "--index", index, "--source", source,
            "--start-line", str(start[0]), "--start-col", str(start[1]),
            "--end-line", str(end[0]), "--end-col", str(end[1])]
    if explain:
        args.append("--explain")
    proc = run_cli(*args)
    rows = [json.loads(line) for line in proc.stdout.splitlines()
            if line.strip()]
    if explain:
        plan = json.loads(proc.stderr)
        print("  query plan:")
        for line in plan["query_plan"]:
            print(f"    {line}")
    return rows


def check(label: str, actual, expected) -> None:
    if actual != expected:
        raise SystemExit(f"FAILED {label}:\n  expected {expected!r}\n"
                         f"  got      {actual!r}")
    print(f"  ok  {label}")


def seal(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()


# --------------------------------------------------------------------------
# Walkthrough
# --------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = os.path.abspath(args.out)
    build_dir = os.path.join(out, "build")
    store_db = os.path.join(out, "store.db")
    index_db = os.path.join(out, "reverse.idx")
    os.makedirs(out, exist_ok=True)

    spec = fixture_spec()

    print("== [1/6] write hand-written build material ==")
    build_fixtures(build_dir, spec)
    print(f"  releases: {', '.join(spec)}")

    print("== [2/6] ingest-build every release ==")
    for release_id in spec:
        proc = run_cli("ingest-build", "--store", store_db,
                       "--manifest",
                       os.path.join(build_dir, release_id.lower(),
                                    "manifest.json"))
        report = json.loads(proc.stdout)
        print(f"  {release_id}: {json.dumps(report, sort_keys=True)}")
    seal(store_db)

    print("== [3/6] build-reverse-index from the sealed database ==")
    proc = run_cli("build-reverse-index", "--store", store_db,
                   "--index", index_db)
    report = json.loads(proc.stdout)
    print("  build report:")
    for key in sorted(report):
        print(f"    {key}: {report[key]}")

    print("== [4/6] semantic checks on small releases ==")
    # The emoji line: query a range containing the shared final point
    # src/orig.ts (0,10).  The entry line must show exactly [1,3), [4,5),
    # [6,8): the surrogate hole at col 3 splits the first segment's
    # coverage, and the generated-only segment at col 5 truncates it.
    rows = query(index_db, "src/orig.ts", (0, 0), (0, 11))
    emoji = [(r["gen_col_start"], r["gen_col_end"]) for r in rows
             if r["release_id"] == "RA" and r["artifact_id"] == "app.min.js"
             and r["gen_line"] == 0]
    check("emoji line runs for src/orig.ts (0,10)", emoji,
          [(1, 3), (4, 5), (6, 8)])
    # An end bound exactly equal to the point excludes it (half-open).
    check("end == point -> empty",
          query(index_db, "src/orig.ts", (0, 0), (0, 10)), [])
    # Multi-entry convergence: app2.min.js (RA) and rb-bundle.js (RB) reach
    # the same final point through their own releases' bindings.
    conv = sorted((r["release_id"], r["artifact_id"], r["gen_col_start"],
                   r["gen_col_end"]) for r in rows
                  if r["artifact_id"] != "app.min.js")
    check("converging entries for src/orig.ts (0,10)", conv,
          [("RA", "app2.min.js", 0, 2), ("RB", "rb-bundle.js", 0, 5)])
    # art-mid has no URL alias: it must never appear as an entry.
    check("intermediate without alias is not an entry",
          [r for r in rows if r["artifact_id"] == "art-mid"], [])
    # Branch isolation on line 1: terminal (1,0) and hop (1,1) hits survive
    # next to the unmapped / invalid-map / generated-only branches.
    rows = query(index_db, "src/orig.ts", (1, 0), (1, 5))
    branch = [(r["gen_col_start"], r["gen_col_end"], r["src_col"])
              for r in rows]
    check("valid branches survive broken ones", branch,
          [(0, 2, 0), (2, 4, 1)])
    # Terminal source whose body was never pinned still yields exact hits.
    rows = query(index_db, "src/nowhere.ts", (3, 0), (3, 5))
    check("missing source body still hits",
          [(r["gen_line"], r["gen_col_start"], r["gen_col_end"],
            r["src_line"], r["src_col"]) for r in rows],
          [(2, 0, 1, 3, 3)])
    # Entry-dependent cycle: cyc-a reaches src/final.ts (0,7) through the
    # shared hop point; cyc-b's own first segment cycles back to itself and
    # produces nothing, while its terminal segment still hits.
    rows = query(index_db, "src/final.ts", (0, 0), (0, 8))
    cyc = sorted((r["artifact_id"], r["gen_col_start"], r["gen_col_end"])
                 for r in rows)
    check("cycle outcome depends on the entry path", cyc,
          [("cyc-a", 0, 1), ("cyc-b", 10, 12)])
    # Unknown source identity: empty, not an error.
    check("unknown source -> empty",
          query(index_db, "src/ghost.ts", (0, 0), (99, 99)), [])
    # Cross-line range: (0,5)..(1,1) contains (0,10) and (1,0) but not (1,1).
    rows = query(index_db, "src/orig.ts", (0, 5), (1, 1))
    span = sorted((r["artifact_id"], r["gen_line"], r["gen_col_start"],
                   r["src_line"], r["src_col"]) for r in rows)
    check("range spanning lines compares coordinate pairs", span, [
        ("app.min.js", 0, 1, 0, 10),
        ("app.min.js", 0, 4, 0, 10),
        ("app.min.js", 0, 6, 0, 10),
        ("app.min.js", 1, 0, 1, 0),
        ("app2.min.js", 0, 0, 0, 10),
        ("rb-bundle.js", 0, 0, 0, 10),
    ])
    # Invalid ranges are rejected.
    for bad in (((0, 1), (0, 1)), ((1, 0), (0, 5)), ((0, 5), (0, 5))):
        proc = run_cli("query-reverse-index", "--index", index_db,
                       "--source", "src/orig.ts",
                       "--start-line", str(bad[0][0]),
                       "--start-col", str(bad[0][1]),
                       "--end-line", str(bad[1][0]),
                       "--end-col", str(bad[1][1]), expect_ok=False)
        check(f"range {bad} rejected",
              proc.returncode != 0
              and "invalid_source_range" in proc.stderr, True)

    print("== [5/6] independent per-character reference + scale proof ==")
    # Full equality between the index contents and the per-character
    # enumeration for the small releases (RD is scale material, checked
    # literally below).
    with ReverseIndex(index_db) as idx:
        stored = {
            (row["release_id"], row["artifact_id"], row["gen_line"],
             row["gen_col_start"], row["gen_col_end"],
             row["source_identity"], row["src_line"], row["src_col"])
            for row in idx.query("src/orig.ts", (0, 0), (1 << 30, 0))
        }
        stored |= {
            (row["release_id"], row["artifact_id"], row["gen_line"],
             row["gen_col_start"], row["gen_col_end"],
             row["source_identity"], row["src_line"], row["src_col"])
            for row in idx.query("src/nowhere.ts", (0, 0), (1 << 30, 0))
        }
        stored |= {
            (row["release_id"], row["artifact_id"], row["gen_line"],
             row["gen_col_start"], row["gen_col_end"],
             row["source_identity"], row["src_line"], row["src_col"])
            for row in idx.query("src/final.ts", (0, 0), (1 << 30, 0))
        }
    expected = reference_runs(spec, ("RA", "RB", "RC"))
    check("index == per-character reference (RA/RB/RC)", stored, expected)

    # Scale proof: the long line contributed ~1.5M indexed columns with two
    # resolutions, and 4,000 converging entry segments cost one hop.
    counters = {key: value for key, value in report.items()
                if key.startswith(("hop_", "entry_", "generated_",
                                   "terminal_"))}
    check("hop points computed stays tiny", report["hop_points_computed"], 9)
    check("shared intermediate point reused",
          report["hop_points_reused"], FANIN_SEGMENTS)
    check("path-dependent cache entries rejected, never reused",
          report["hop_cache_rejected_path"], 2)
    check("cycle outcomes never cached", report["hop_cycles"], 1)
    check("columns indexed via intervals, not per char",
          report["generated_columns_indexed"],
          1_500_000 + FANIN_SEGMENTS + 20)
    check("hit rows stay interval-sized", report["hit_rows"], 13)
    print(f"  counters: {json.dumps(counters, sort_keys=True)}")

    rows = query(index_db, "src/big.ts", (0, 0), (0, 10), explain=True)
    big = [(r["artifact_id"], r["gen_col_start"], r["gen_col_end"],
            r["src_col"]) for r in rows]
    check("long line + fan-in runs", big, [
        ("fanin.js", 0, FANIN_SEGMENTS, 5),
        ("long.js", 0, 1000000, 0),
        ("long.js", 1500000, 2000000, 1),
    ])

    print("== [6/6] move the build database away, query the index alone ==")
    before = query(index_db, "src/orig.ts", (0, 0), (0, 11))
    os.rename(store_db, store_db + ".gone")
    shutil.move(build_dir, build_dir + ".gone")
    after = query(index_db, "src/orig.ts", (0, 0), (0, 11))
    check("identical results with only the index file", after, before)
    with ReverseIndex(index_db) as idx:
        info = idx.info()
    check("index remembers the sealed build fingerprint",
          info["build_db_sha256"], report["build_db_sha256"])
    check("index state complete", info["state"], "complete")

    print("\nAll reverse-index walkthrough checks passed.")


if __name__ == "__main__":
    raise SystemExit(main())
