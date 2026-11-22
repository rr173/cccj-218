#!/usr/bin/env python3
"""Reproducible walkthrough for the exposure first-error fact table.

The script is self-contained and never writes into the source tree:

* it builds a small *sealed* enrichment database under the output directory,
  shaped exactly like the ``enrich`` output (a ``frames`` table with stored
  ``result_json``), including one event carrying frames for two releases;
* drives ``import-facts`` / ``export-facts`` through the public CLI across
  every required scenario (boundary exposure, insertion that removes a first
  error by release mismatch, retraction, backup takeover, same-ms ties,
  multi-frame smallest index, same-batch swap, rejected batch, reopen and
  identical replay);
* hand-checks every expectation and cross-checks the exported facts against
  the independent from-scratch reference implementation;
* prints the query plans proving indexed, bounded reads instead of scans.

Run (isolated mode, as every other entry point)::

    python3 -I tools/facts_walkthrough.py --out ./run/facts-demo
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

from smpipe.facts import FactStore  # noqa: E402
from reference_facts import ReferenceFacts  # noqa: E402

SMPIPE = os.path.join(ROOT, "smpipe.py")

# Hand-authored sealed frame library.  Every row mirrors what ``enrich``
# stores: a primary key (event_id, frame_index), the release the frame was
# attributed against and the final status inside result_json.
FRAMES = [
    # evt-alpha: frame 0 unmapped, frame 1 mapped for R1 (smallest mapped=1).
    ("evt-alpha", 0, "R1", "unmapped"),
    ("evt-alpha", 1, "R1", "mapped"),
    # evt-beta: mapped for R2 only.
    ("evt-beta", 0, "R2", "mapped"),
    # evt-gamma: mapped for R3 only, two mapped frames -> smallest is 0.
    ("evt-gamma", 0, "R3", "mapped"),
    ("evt-gamma", 1, "R3", "mapped"),
    # evt-duo: mapped frames in two different releases.
    ("evt-duo", 0, "R1", "mapped"),
    ("evt-duo", 1, "R2", "mapped"),
    # evt-dud: nothing mapped anywhere.
    ("evt-dud", 0, "R1", "unmapped"),
    ("evt-dud", 1, "R2", "invalid_frame"),
]


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
    # Seal: no WAL may remain, otherwise FactStore refuses the database.
    assert not os.path.exists(path + "-wal") or \
        os.path.getsize(path + "-wal") == 0


def write_batch(directory: str, name: str, payload: dict) -> str:
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
    return path


def run_cli(*args: str) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        [sys.executable, "-I", SMPIPE, *args],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"command failed: {' '.join(args)}\n{proc.stderr}"
        )
    return proc


def cli(*args: str) -> dict:
    return json.loads(run_cli(*args).stdout)


def check(label: str, actual, expected) -> None:
    if actual != expected:
        raise SystemExit(
            f"FAILED {label}: expected {expected!r}, got {actual!r}"
        )
    print(f"  ok  {label}: {actual!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = os.path.abspath(args.out)
    batches_dir = os.path.join(out, "batches")
    os.makedirs(batches_dir, exist_ok=True)

    enrich_db = os.path.join(out, "sealed-enrich.db")
    work_db = os.path.join(out, "facts-work.db")
    export_path = os.path.join(out, "facts.jsonl")
    for stale in (work_db, work_db + "-wal", work_db + "-shm", export_path):
        if os.path.exists(stale):
            os.remove(stale)

    print("== build sealed enrichment database ==")
    build_sealed_enrich(enrich_db)
    reference = ReferenceFacts.from_enrich_db(enrich_db)

    def imp(seq: int, exposures=None, events=None, *, expect_fail=False):
        payload = {"seq": seq,
                   "exposure_changes": exposures or [],
                   "event_changes": events or []}
        path = write_batch(
            batches_dir, f"batch-{seq:02d}.json", payload)
        report = cli("import-facts", "--work-db", work_db,
                     "--enrich-db", enrich_db, "--batch", path)
        if not expect_fail:
            reference.apply(payload)
        return report

    def facts() -> dict[str, dict]:
        run_cli("export-facts", "--work-db", work_db, "--out", export_path)
        rows = [json.loads(line) for line in
                open(export_path, encoding="utf-8")]
        expected = reference.compute()
        if rows != expected:
            raise SystemExit(
                "reference mismatch:\n"
                f"  production={json.dumps(rows, sort_keys=True)}\n"
                f"  reference ={json.dumps(expected, sort_keys=True)}"
            )
        return {row["exposure_id"]: row for row in rows}

    print("== batch 1: two adjacent exposures, boundary event ==")
    # X1 R1 [100,200)  X2 R2 [200, ...).  evt-beta occurs exactly at 200 and
    # must belong to X2; evt-alpha (R1 mapped) is X1's first error.
    imp(1,
        exposures=[
            {"exposure_id": "X1", "session_id": "sess-A",
             "release_id": "R1", "start_ms": 100},
            {"exposure_id": "X2", "session_id": "sess-A",
             "release_id": "R2", "start_ms": 200},
        ],
        events=[
            {"event_id": "evt-alpha", "session_id": "sess-A",
             "occurred_ms": 130},
            {"event_id": "evt-beta", "session_id": "sess-A",
             "occurred_ms": 200},
            # Event loc before any exposure of its (other) session exists.
            {"event_id": "evt-gamma", "session_id": "sess-B",
             "occurred_ms": 5},
        ])
    snapshot = facts()
    check("X1 first event", snapshot["X1"]["first_event_id"], "evt-alpha")
    check("X1 frame index (smallest mapped)",
          snapshot["X1"]["first_frame_index"], 1)
    check("X1 hand delay 130-100", snapshot["X1"]["first_delay_ms"], 30)
    check("X1 end is X2 start", snapshot["X1"]["end_ms"], 200)
    check("boundary event belongs to X2",
          snapshot["X2"]["first_event_id"], "evt-beta")
    check("X2 delay at exact start", snapshot["X2"]["first_delay_ms"], 0)
    check("event without exposure stays unmatched (no fact row)",
          "sess-B" in {row["session_id"] for row in snapshot.values()},
          False)

    print("== batch 2: insert R3 exposure; old first error must vanish ==")
    # Insert X3 (R3) at 150 and move X2 to 300 in one batch.  evt-beta@200 is
    # now inside X3 (R3) but only maps for R2, so it stops qualifying and is
    # NOT handed back to X1.  evt-alpha@130 stays with X1.
    imp(2, exposures=[
        {"exposure_id": "X3", "session_id": "sess-A",
         "release_id": "R3", "start_ms": 150},
        {"exposure_id": "X2", "session_id": "sess-A",
         "release_id": "R2", "start_ms": 300},
    ])
    snapshot = facts()
    check("X1 interval shrinks",
          (snapshot["X1"]["start_ms"], snapshot["X1"]["end_ms"]),
          (100, 150))
    check("X1 keeps its own first error",
          snapshot["X1"]["first_event_id"], "evt-alpha")
    check("X3 has no R3-mapped event yet",
          snapshot["X3"]["first_event_id"], None)
    check("evt-beta not leaked to previous exposure",
          snapshot["X2"]["first_event_id"], None)

    print("== batch 3: same-millisecond tie, multi-frame smallest index ==")
    # At 300 (X2 start) three events collide.  evt-duo (R2 frame 1) and
    # evt-beta (R2 frame 0) both qualify for R2; evt-gamma is R3 only.
    # String order "evt-beta" < "evt-duo" decides; import order is irrelevant.
    imp(3, events=[
        {"event_id": "evt-duo", "session_id": "sess-A", "occurred_ms": 300},
        {"event_id": "evt-beta", "session_id": "sess-A", "occurred_ms": 300},
        {"event_id": "evt-gamma", "session_id": "sess-A",
         "occurred_ms": 300},
    ])
    snapshot = facts()
    check("same-ms tie resolved by event_id string",
          snapshot["X2"]["first_event_id"], "evt-beta")
    check("its only frame", snapshot["X2"]["first_frame_index"], 0)
    check("X3 still empty (gamma is R3 but at 300, outside [150,300))",
          snapshot["X3"]["first_event_id"], None)

    print("== batch 4: retract the first error; backup takes over ==")
    imp(4, events=[{"event_id": "evt-beta", "op": "delete"}])
    snapshot = facts()
    check("retracted first error cleared",
          snapshot["X2"]["first_event_id"], "evt-duo")
    check("backup frame is duo's R2 frame index",
          snapshot["X2"]["first_frame_index"], 1)
    check("backup hand delay 300-300", snapshot["X2"]["first_delay_ms"], 0)

    print("== batch 5: move gamma into X3; backup takeover with delay ==")
    imp(5, events=[
        {"event_id": "evt-gamma", "session_id": "sess-A",
         "occurred_ms": 250},
    ])
    snapshot = facts()
    check("X3 first error is gamma",
          snapshot["X3"]["first_event_id"], "evt-gamma")
    check("smallest of two mapped frames",
          snapshot["X3"]["first_frame_index"], 0)
    check("X3 hand delay 250-150", snapshot["X3"]["first_delay_ms"], 100)

    print("== batch 6: delete X3; adjacency rebuilds, its fact row vanishes ==")
    imp(6, exposures=[{"exposure_id": "X3", "op": "delete"}])
    snapshot = facts()
    check("X3 fact row removed", "X3" in snapshot, False)
    check("X1 end jumps to X2 start again",
          snapshot["X1"]["end_ms"], 300)
    # gamma@250 now sits in X1 (R1) but only maps for R3: no leak.
    check("gamma does not leak into R1 exposure",
          snapshot["X1"]["first_event_id"], "evt-alpha")

    print("== batch 7: legal same-batch swap of two start times ==")
    imp(7, exposures=[
        {"exposure_id": "X1", "session_id": "sess-A",
         "release_id": "R1", "start_ms": 300},
        {"exposure_id": "X2", "session_id": "sess-A",
         "release_id": "R2", "start_ms": 100},
    ])
    snapshot = facts()
    check("swap applied",
          (snapshot["X1"]["start_ms"], snapshot["X2"]["start_ms"]),
          (300, 100))

    print("== batch 8: rejected batches leave no partial state ==")
    before = open(export_path, encoding="utf-8").read()
    # 8a: duplicate exposure id inside one batch.
    dup = write_batch(batches_dir, "batch-08-dup.json", {
        "seq": 8,
        "exposure_changes": [
            {"exposure_id": "Y", "session_id": "sess-A", "release_id": "R1",
             "start_ms": 1},
            {"exposure_id": "Y", "session_id": "sess-A", "release_id": "R1",
             "start_ms": 2},
        ],
    })
    proc = subprocess.run(
        [sys.executable, "-I", SMPIPE, "import-facts", "--work-db", work_db,
         "--enrich-db", enrich_db, "--batch", dup],
        cwd=ROOT, capture_output=True, text=True, check=False)
    check("duplicate-id batch rejected", proc.returncode, 2)
    # 8b: final timeline conflict (same session/start as X2).
    conflict = write_batch(batches_dir, "batch-08-conflict.json", {
        "seq": 8,
        "exposure_changes": [
            {"exposure_id": "Z", "session_id": "sess-A",
             "release_id": "R3", "start_ms": 100},
        ],
    })
    proc = subprocess.run(
        [sys.executable, "-I", SMPIPE, "import-facts", "--work-db", work_db,
         "--enrich-db", enrich_db, "--batch", conflict],
        cwd=ROOT, capture_output=True, text=True, check=False)
    check("timeline-conflict batch rejected", proc.returncode, 2)
    # 8c: deleting an unknown id.
    ghost = write_batch(batches_dir, "batch-08-ghost.json", {
        "seq": 8,
        "exposure_changes": [{"exposure_id": "ghost", "op": "delete"}]})
    proc = subprocess.run(
        [sys.executable, "-I", SMPIPE, "import-facts", "--work-db", work_db,
         "--enrich-db", enrich_db, "--batch", ghost],
        cwd=ROOT, capture_output=True, text=True, check=False)
    check("unknown-delete batch rejected", proc.returncode, 2)
    run_cli("export-facts", "--work-db", work_db, "--out", export_path)
    check("facts unchanged after rejected batches",
          open(export_path, encoding="utf-8").read(), before)

    print("== batch 9: skipped seq rejected; then valid batch 8 ==")
    skipped = write_batch(batches_dir, "batch-09-skip.json", {"seq": 9})
    proc = subprocess.run(
        [sys.executable, "-I", SMPIPE, "import-facts", "--work-db", work_db,
         "--enrich-db", enrich_db, "--batch", skipped],
        cwd=ROOT, capture_output=True, text=True, check=False)
    check("seq gap rejected", proc.returncode, 2)
    imp(8, events=[{"event_id": "evt-dud", "session_id": "sess-A",
                    "occurred_ms": 350}])
    snapshot = facts()
    # evt-dud has no mapped frame: never counted anywhere.
    check("unmapped-only event never becomes a first error",
          all(row["first_event_id"] != "evt-dud"
              for row in snapshot.values()), True)

    print("== reopen: identical replay is a no-op, ledger rules persist ==")
    b1_path = os.path.join(batches_dir, "batch-01.json")
    report = cli("import-facts", "--work-db", work_db,
                 "--enrich-db", enrich_db, "--batch", b1_path)
    check("identical replayed batch is noop", report["noop"], True)
    # Reopen without --enrich-db: recorded binding is reused.
    reopened = FactStore(work_db)
    try:
        check("last seq survives reopen", reopened.last_seq(), 8)
        tampered = {"seq": 1, "exposure_changes": []}
        tampered_path = write_batch(
            batches_dir, "tampered-01.json", tampered)
        proc = subprocess.run(
            [sys.executable, "-I", SMPIPE, "import-facts",
             "--work-db", work_db, "--batch", tampered_path],
            cwd=ROOT, capture_output=True, text=True, check=False)
        check("same seq different bytes rejected after reopen",
              proc.returncode, 2)
    finally:
        reopened.close()

    print("== locality evidence: query plans + maintenance counters ==")
    metrics = cli("facts-metrics", "--work-db", work_db,
                  "--enrich-db", enrich_db,
                  "--plan-event", "evt-alpha", "--plan-release", "R1")
    print(json.dumps(metrics, indent=2, sort_keys=True))
    plan = metrics["enrich_frame_query_plan"]
    if "SEARCH" not in plan or "SCAN frames" in plan:
        raise SystemExit(f"enrich lookup is not an indexed search: {plan}")
    check("enrich frame lookup uses primary-key search",
          "SEARCH" in plan, True)

    print("\nAll walkthrough checks passed. Artifacts:")
    for name in sorted(os.listdir(out)):
        print(" ", os.path.join(out, name))


if __name__ == "__main__":
    main()
