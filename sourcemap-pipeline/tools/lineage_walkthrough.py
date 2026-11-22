#!/usr/bin/env python3
"""Reproducible walkthrough for trigger-operation lineage export.

Self-contained and never writing into the source tree:

* builds a small *sealed* enrichment database under the output directory,
  shaped exactly like the ``enrich`` output (a ``frames`` table with stored
  ``result_json``), including one event carrying two frames and events whose
  frames failed attribution -- frame status never limits the universe;
* hand-authors async telemetry for every review scenario:
  - one reporting point reaching one action through two branches (the
    action stays a single candidate) with stable equal-length path choice;
  - a nearer action whose id ranks third cannot displace the two smallest
    ids;
  - a directed cycle with an exit to a known action, together with a
    missing parent; another run defines a same-named node differently,
    proving run boundaries are never crossed;
  - an unbound event (``no_anchor``), a multi-frame event (one row) and an
    error bound to an undefined reporting node;
* verifies atomicity (a conflicting tail line preserves the previous
  export) and determinism (reordering records and re-delivering equivalent
  rows yields byte-identical output);
* cross-checks every output row against the independent from-scratch
  :mod:`tests.reference_lineage` implementation, which shares no graph
  code with the production module;
* generates a shared-upstream fan-in at runtime and shows the lineage
  computation is reused: the graph counters stay flat while the number of
  errors at the same reporting node grows.

Run (isolated mode, as every other entry point)::

    python3 -I tools/lineage_walkthrough.py --out ./run/lineage-demo
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

from smpipe.lineage import export_lineage  # noqa: E402
from reference_lineage import ReferenceLineage  # noqa: E402

SMPIPE = os.path.join(ROOT, "smpipe.py")

# event_id -> list of (frame_index, release_id, status); ev-multi has two.
FRAMES = [
    ("evt-conv", 0, "R1", "mapped"),
    ("evt-rank", 0, "R1", "mapped"),
    ("evt-cyc", 0, "R1", "mapped"),
    ("evt-other", 0, "R2", "mapped"),
    ("evt-multi", 0, "R1", "unmapped"),
    ("evt-multi", 1, "R1", "mapped"),
    ("evt-ghost-anchor", 0, "R1", "mapped"),
    ("evt-unmapped-only", 0, "R1", "invalid_map"),
    # evt-quiet deliberately has frames but no error telemetry record.
    ("evt-quiet", 0, "R1", "mapped"),
]
UNIVERSE = sorted({event_id for event_id, _, _, _ in FRAMES})


def build_sealed_enrich(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE frames(event_id TEXT NOT NULL, frame_index INTEGER "
        "NOT NULL, raw_hash TEXT NOT NULL, release_id TEXT NOT NULL, "
        "input_json TEXT NOT NULL, result_json TEXT, "
        "PRIMARY KEY(event_id, frame_index))"
    )
    for event_id, frame_index, release_id, status in FRAMES:
        result = {"event_id": event_id, "frame_index": frame_index,
                  "release_id": release_id, "status": status}
        conn.execute(
            "INSERT INTO frames VALUES(?, ?, ?, ?, ?, ?)",
            (event_id, frame_index, f"raw-{frame_index}", release_id,
             json.dumps({"event_id": event_id}),
             json.dumps(result, sort_keys=True)),
        )
    conn.commit()
    conn.close()
    assert not os.path.exists(path + "-wal") or \
        os.path.getsize(path + "-wal") == 0


def write_jsonl(path: str, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_jsonl(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def check(label: str, actual, expected) -> None:
    if actual != expected:
        raise SystemExit(
            f"FAILED {label}:\n  expected {expected!r}\n  got      {actual!r}"
        )
    print(f"  ok  {label}: {actual!r}")


def run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-I", SMPIPE, *args],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )


def hand_records() -> list[dict]:
    records: list[dict] = []
    # Scenario A: anchor err-1 reaches action act-a through two branches.
    records += [
        {"type": "action", "run_id": "rA", "node_id": "act-a"},
        {"type": "work", "run_id": "rA", "node_id": "b1",
         "parents": ["act-a"]},
        {"type": "work", "run_id": "rA", "node_id": "b2",
         "parents": ["act-a"]},
        {"type": "work", "run_id": "rA", "node_id": "err-1",
         "parents": ["b1", "b2"]},
        {"type": "error", "event_id": "evt-conv", "run_id": "rA",
         "node_id": "err-1"},
    ]
    # Scenario B: act-zz is one edge away; act-aa/act-ab are two edges away
    # but own the two smallest ids.
    records += [
        {"type": "action", "run_id": "rB", "node_id": "act-aa"},
        {"type": "action", "run_id": "rB", "node_id": "act-ab"},
        {"type": "action", "run_id": "rB", "node_id": "act-zz"},
        {"type": "work", "run_id": "rB", "node_id": "hub",
         "parents": ["act-aa", "act-ab"]},
        {"type": "work", "run_id": "rB", "node_id": "err-2",
         "parents": ["hub", "act-zz"]},
        {"type": "error", "event_id": "evt-rank", "run_id": "rB",
         "node_id": "err-2"},
    ]
    # Scenario C: cycle cy1<->cy2 with exit cy2->act-exit, plus a missing
    # parent on the anchor.
    records += [
        {"type": "action", "run_id": "rC", "node_id": "act-exit"},
        {"type": "work", "run_id": "rC", "node_id": "cy1",
         "parents": ["cy2"]},
        {"type": "work", "run_id": "rC", "node_id": "cy2",
         "parents": ["cy1", "act-exit"]},
        {"type": "work", "run_id": "rC", "node_id": "err-3",
         "parents": ["cy1", "ghost-parent"]},
        {"type": "error", "event_id": "evt-cyc", "run_id": "rC",
         "node_id": "err-3"},
        # Error bound to a reporting node with no defining record.
        {"type": "error", "event_id": "evt-ghost-anchor", "run_id": "rC",
         "node_id": "no-such-node"},
    ]
    # Scenario C companion: run rD reuses the name cy1 but defines it as an
    # action; rC's cyclic work node must never be borrowed.
    records += [
        {"type": "action", "run_id": "rD", "node_id": "cy1"},
        {"type": "work", "run_id": "rD", "node_id": "rep",
         "parents": ["cy1"]},
        {"type": "error", "event_id": "evt-other", "run_id": "rD",
         "node_id": "rep"},
    ]
    # Multi-frame event bound through a trivial chain.
    records += [
        {"type": "action", "run_id": "rE", "node_id": "act-e"},
        {"type": "work", "run_id": "rE", "node_id": "err-m",
         "parents": ["act-e"]},
        {"type": "error", "event_id": "evt-multi", "run_id": "rE",
         "node_id": "err-m"},
        {"type": "error", "event_id": "evt-unmapped-only", "run_id": "rE",
         "node_id": "err-m"},
    ]
    return records


def build_reference(records: list[dict]) -> ReferenceLineage:
    reference = ReferenceLineage()
    for record in records:
        if record["type"] == "action":
            reference.add_node(record["run_id"], record["node_id"], "action")
        elif record["type"] == "work":
            reference.add_work(
                record["run_id"], record["node_id"], list(record["parents"])
            )
        else:
            reference.bind_error(
                record["event_id"], record["run_id"], record["node_id"]
            )
    return reference


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    if os.path.exists(args.out):
        shutil.rmtree(args.out)
    os.makedirs(args.out)
    enrich_db = os.path.join(args.out, "sealed-enrich.db")
    telemetry = os.path.join(args.out, "telemetry.jsonl")
    out_path = os.path.join(args.out, "lineage.jsonl")
    build_sealed_enrich(enrich_db)

    print("== hand-authored scenarios: row-by-row expectations ==")
    records = hand_records()
    write_jsonl(telemetry, records)
    report = export_lineage(enrich_db, telemetry, out_path)
    rows = read_jsonl(out_path)
    rows_by_id = {row["event_id"]: row for row in rows}

    # Output ordering and universe coverage.
    check("rows sorted by event_id", [r["event_id"] for r in rows],
          sorted(rows_by_id))
    check("one row per universe event", len(rows), len(UNIVERSE))
    check("multi-frame event appears once",
          sum(1 for r in rows if r["event_id"] == "evt-multi"), 1)

    conv = rows_by_id["evt-conv"]
    check("converged branches keep one candidate",
          [c["action_id"] for c in conv["candidates"]], ["act-a"])
    check("equal-length paths pick lexicographic branch",
          conv["candidates"][0]["path"],
          ["err-1", "b1", "act-a"])
    check("converged event is attributed", conv["status"], "attributed")

    rank = rows_by_id["evt-rank"]
    check("id order beats distance for top two",
          [c["action_id"] for c in rank["candidates"]],
          ["act-aa", "act-ab"])
    check("nearby third id is excluded",
          any(c["action_id"] == "act-zz" for c in rank["candidates"]), False)
    check("ranking event is ambiguous", rank["status"], "ambiguous")

    cyc = rows_by_id["evt-cyc"]
    check("cycle flag survives the exit", cyc["reaches_cycle"], True)
    check("missing-parent flag is not swallowed",
          cyc["reaches_missing"], True)
    check("cycle exit still yields the known action",
          cyc["candidates"],
          [{"action_id": "act-exit",
            "path": ["err-3", "cy1", "cy2", "act-exit"]}])
    check("cycle+gap stays unresolved", cyc["status"], "unresolved")

    other = rows_by_id["evt-other"]
    check("same-named node resolves inside its own run",
          other["candidates"],
          [{"action_id": "cy1", "path": ["rep", "cy1"]}])
    check("cross-run event is attributed", other["status"], "attributed")

    ghost = rows_by_id["evt-ghost-anchor"]
    check("undefined anchor is unresolved", ghost["status"], "unresolved")
    check("undefined anchor counts as a gap",
          ghost["reaches_missing"], True)
    check("undefined anchor has no candidates", ghost["candidates"], [])

    quiet = rows_by_id["evt-quiet"]
    check("unbound event is no_anchor", quiet["status"], "no_anchor")
    check("no_anchor identity fields are empty",
          (quiet["run_id"], quiet["node_id"]), (None, None))
    check("no_anchor flags are false",
          (quiet["reaches_missing"], quiet["reaches_cycle"]),
          (False, False))

    check("failed-attribution event is still in the universe",
          rows_by_id["evt-unmapped-only"]["status"], "attributed")

    print("\n== independent reference cross-check (every row) ==")
    reference = build_reference(records)
    expected = reference.all_rows(set(UNIVERSE))
    check("production rows equal from-scratch reference", rows, expected)

    print("\n== determinism: reorder + equivalent re-delivery ==")
    import random
    rng = random.Random(20261122)
    shuffled = records[:]
    rng.shuffle(shuffled)
    shuffled += [
        # Reordered parent list and duplicate rows fold equivalently.
        {"type": "work", "run_id": "rC", "node_id": "cy2",
         "parents": ["act-exit", "cy1"]},
        {"type": "action", "run_id": "rA", "node_id": "act-a"},
        {"type": "error", "event_id": "evt-conv", "run_id": "rA",
         "node_id": "err-1"},
    ]
    shuffled_path = os.path.join(args.out, "telemetry-shuffled.jsonl")
    shuffled_out = os.path.join(args.out, "lineage-shuffled.jsonl")
    write_jsonl(shuffled_path, shuffled)
    export_lineage(enrich_db, shuffled_path, shuffled_out)
    with open(out_path, encoding="utf-8") as handle:
        first_bytes = handle.read()
    with open(shuffled_out, encoding="utf-8") as handle:
        shuffled_bytes = handle.read()
    check("byte-identical output after reorder/resend",
          shuffled_bytes, first_bytes)

    print("\n== atomicity: conflicting tail line keeps the old export ==")
    bad_records = records + [
        {"type": "work", "run_id": "rA", "node_id": "act-a",
         "parents": ["b1"]},
    ]
    bad_path = os.path.join(args.out, "telemetry-bad.jsonl")
    write_jsonl(bad_path, bad_records)
    proc = run_cli("export-lineage", "--enrich-db", enrich_db,
                   "--telemetry", bad_path, "--out", out_path)
    check("conflicting export exits non-zero", proc.returncode, 2)
    check("reason is lineage_input_conflict",
          "lineage_input_conflict" in proc.stderr, True)
    with open(out_path, encoding="utf-8") as handle:
        preserved = handle.read()
    check("previous export preserved byte-for-byte", preserved, first_bytes)

    # Unknown event_id conflict, also at the tail.
    unknown_records = records + [
        {"type": "error", "event_id": "evt-not-in-universe",
         "run_id": "rA", "node_id": "err-1"},
    ]
    unknown_path = os.path.join(args.out, "telemetry-unknown.jsonl")
    write_jsonl(unknown_path, unknown_records)
    proc = run_cli("export-lineage", "--enrich-db", enrich_db,
                   "--telemetry", unknown_path, "--out", out_path)
    check("unknown event_id exits non-zero", proc.returncode, 2)
    with open(out_path, encoding="utf-8") as handle:
        check("export still preserved", handle.read(), first_bytes)

    print("\n== read-only guarantee: enrich database untouched ==")
    check("no WAL sidecar created",
          os.path.exists(enrich_db + "-wal"), False)

    print("\n== shared-upstream reuse: graph work vs error count ==")
    shared_small, shared_large = shared_fan_in(args.out, enrich_db)
    graph_keys = ("nodes_defined", "parent_edges_stored",
                  "cyclic_components", "path_bfs_runs")
    for key in graph_keys:
        check(f"graph counter {key!r} flat as errors grow",
              shared_large[key], shared_small[key])
    check("one BFS table for one shared anchor",
          shared_large["path_bfs_runs"], 1)
    check("repeated candidate paths served from the anchor table",
          shared_large["path_cache_hits"] > shared_small["path_cache_hits"],
          True)
    check("row count grows with error count only",
          shared_large["rows_written"] - shared_small["rows_written"],
          shared_large["fan_in_errors"] - shared_small["fan_in_errors"])

    print("\nAll walkthrough checks passed. Artifacts:")
    for name in sorted(os.listdir(args.out)):
        print(" ", os.path.join(args.out, name))


def shared_fan_in(out_dir: str, enrich_db: str) -> tuple[dict, dict]:
    """Two exports over one identical graph with different error counts.

    Many error events share the exact same reporting node (and therefore
    the same upstream subgraph).  Graph-processing counters must be
    identical between the small and large runs; only result writing grows.
    """
    base = [
        {"type": "action", "run_id": "rS", "node_id": f"act-{i:02d}"}
        for i in range(6)
    ]
    for hub in range(3):
        base.append({
            "type": "work", "run_id": "rS",
            "node_id": f"hub-{hub}",
            "parents": [f"act-{2 * hub:02d}", f"act-{2 * hub + 1:02d}"],
        })
    base.append({
        "type": "work", "run_id": "rS", "node_id": "shared-root",
        "parents": [f"hub-{hub}" for hub in range(3)],
    })

    def run(fan_in_errors: int) -> dict:
        records = list(base)
        for index in range(fan_in_errors):
            records.append({
                "type": "error", "event_id": f"evt-shared-{index:03d}",
                "run_id": "rS", "node_id": "shared-root",
            })
        # Universe events must exist in the sealed frames table.  Build a
        # matching sealed database per run inside a scratch directory.
        scratch = tempfile.mkdtemp(prefix="smpipe-lineage-fanin-")
        try:
            fan_enrich = os.path.join(scratch, "enrich.db")
            conn = sqlite3.connect(fan_enrich)
            conn.execute(
                "CREATE TABLE frames(event_id TEXT NOT NULL, frame_index "
                "INTEGER NOT NULL, raw_hash TEXT NOT NULL, release_id TEXT "
                "NOT NULL, input_json TEXT NOT NULL, result_json TEXT, "
                "PRIMARY KEY(event_id, frame_index))"
            )
            events = [f"evt-shared-{index:03d}"
                      for index in range(fan_in_errors)]
            for event_id in events:
                result = json.dumps(
                    {"event_id": event_id, "frame_index": 0,
                     "release_id": "R1", "status": "mapped"}, sort_keys=True)
                conn.execute(
                    "INSERT INTO frames VALUES(?, ?, ?, ?, ?, ?)",
                    (event_id, 0, "h", "R1", "{}", result),
                )
            conn.commit()
            conn.close()
            telemetry = os.path.join(scratch, "t.jsonl")
            target = os.path.join(scratch, "out.jsonl")
            write_jsonl(telemetry, records)
            metrics = export_lineage(fan_enrich, telemetry, target)
            metrics["fan_in_errors"] = fan_in_errors
            return metrics
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    return run(20), run(400)


if __name__ == "__main__":
    main()
