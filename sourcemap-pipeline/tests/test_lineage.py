"""Behavioral suite for trigger-operation lineage export.

Run with::

    python -I tests/run_tests.py

The suite covers the pure graph analysis directly (SCCs, capped top-2
propagation, lexicographic shortest paths), the full disk-spooled exporter
against a hand-built sealed enrichment database (conflicts, gaps, cycles
with exits, cross-run same-name isolation, tail-line atomicity), and an
exhaustive cross-check against the independent from-scratch
:mod:`tests.reference_lineage` implementation over generated graphs.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, HERE)

from smpipe.errors import LineageConflict, TelemetryError  # noqa: E402
from smpipe.lineage import (  # noqa: E402
    ACTION,
    WORK,
    LineageExporter,
    analyze_run_graph,
    classify,
    export_lineage,
    parse_telemetry_record,
    shortest_lineage_path,
    strongly_connected_components,
)
from reference_lineage import ReferenceLineage  # noqa: E402

SMPIPE = os.path.join(ROOT, "smpipe.py")


# --------------------------------------------------------------------- helpers
def build_enrich_db(path: str, frame_rows: list[tuple]) -> None:
    """Create a sealed-style frames table.

    Each tuple is ``(event_id, frame_index, release_id, status)``; one
    event may carry several frames, and ``status`` may be anything
    including failure statuses -- the lineage export only needs the event
    id of frames that have a stored result.
    """
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE frames(event_id TEXT NOT NULL, frame_index INTEGER "
        "NOT NULL, raw_hash TEXT NOT NULL, release_id TEXT NOT NULL, "
        "input_json TEXT NOT NULL, result_json TEXT, "
        "PRIMARY KEY(event_id, frame_index))"
    )
    for event_id, frame_index, release_id, status in frame_rows:
        result = json.dumps({
            "event_id": event_id,
            "frame_index": frame_index,
            "release_id": release_id,
            "status": status,
        }, sort_keys=True)
        conn.execute(
            "INSERT INTO frames VALUES(?, ?, ?, ?, ?, ?)",
            (event_id, frame_index, f"h:{frame_index}", release_id,
             "{}", result),
        )
    conn.commit()
    conn.close()


def write_jsonl(path: str, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_jsonl(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


# Hand graph:
#   e(work) -> w1(work) -> a1(action)
#   e       -> w2(work) -> a2(action) and w2 -> missing parent m
#   c1 <-> c2 (cycle) with exit c2 -> a3(action); e -> c1
KINDS = {
    "e": "work", "w1": "work", "w2": "work", "c1": "work", "c2": "work",
    "a1": "action", "a2": "action", "a3": "action",
}
PARENTS = {
    "e": ["w1", "w2", "c1"],
    "w1": ["a1"],
    "w2": ["a2", "m"],  # "m" is deliberately undefined: a sampling gap
    "c1": ["c2"],
    "c2": ["c1", "a3"],
    "a1": [], "a2": [], "a3": [],
}


# ------------------------------------------------------------- pure graph work
class TestPureGraph(unittest.TestCase):
    def test_scc_detection_and_sink_first_order(self) -> None:
        adj = {n: sorted(p for p in PARENTS[n] if p in KINDS)
               for n in sorted(KINDS)}
        components = strongly_connected_components(sorted(KINDS), adj)
        # The c1/c2 cycle is one SCC; every other node is its own SCC.
        cyclic = [sorted(c) for c in components if len(c) > 1]
        self.assertEqual(cyclic, [["c1", "c2"]])
        self.assertEqual(len(components), len(KINDS) - 1)

    def test_flags_and_capped_top_two(self) -> None:
        analysis = analyze_run_graph(dict(KINDS), {
            n: list(p) for n, p in PARENTS.items()
        })
        self.assertEqual(analysis.cyclic_components, 1)
        anchor = analysis.lineage["e"]
        self.assertTrue(anchor.reaches_missing)
        self.assertTrue(anchor.reaches_cycle)
        # Three reachable actions; only the two smallest ids survive.
        self.assertEqual(anchor.top_actions, ("a1", "a2"))
        w2 = analysis.lineage["w2"]
        self.assertTrue(w2.reaches_missing)
        self.assertFalse(w2.reaches_cycle)
        self.assertEqual(w2.top_actions, ("a2",))
        a1 = analysis.lineage["a1"]
        self.assertEqual(a1.top_actions, ("a1",))
        self.assertFalse(a1.reaches_missing)

    def test_cycle_exit_keeps_collecting_actions(self) -> None:
        analysis = analyze_run_graph(dict(KINDS), {
            n: list(p) for n, p in PARENTS.items()
        })
        c1 = analysis.lineage["c1"]
        self.assertTrue(c1.reaches_cycle)
        self.assertFalse(c1.reaches_missing)
        # Exit from the cycle to a3 is still collected.
        self.assertEqual(c1.top_actions, ("a3",))

    def test_self_loop_is_a_cycle(self) -> None:
        kinds = {"s": "work", "a": "action"}
        analysis = analyze_run_graph(
            kinds, {"s": ["s", "a"], "a": []}
        )
        self.assertEqual(analysis.cyclic_components, 1)
        self.assertTrue(analysis.lineage["s"].reaches_cycle)
        self.assertEqual(analysis.lineage["s"].top_actions, ("a",))

    def test_lexicographic_shortest_path_tie_break(self) -> None:
        kinds = {"e": "work", "b1": "work", "b2": "work", "x": "action"}
        parents = {"e": ["b1", "b2"], "b1": ["x"], "b2": ["x"], "x": []}
        analysis = analyze_run_graph(kinds, parents)
        path = shortest_lineage_path(analysis.adjacency, "e", "x")
        self.assertEqual(path, ["e", "b1", "x"])

    def test_two_branches_same_action_is_single_candidate(self) -> None:
        kinds = {"e": "work", "b1": "work", "b2": "work", "a": "action"}
        parents = {"e": ["b1", "b2"], "b1": ["a"], "b2": ["a"], "a": []}
        analysis = analyze_run_graph(kinds, parents)
        self.assertEqual(analysis.lineage["e"].top_actions, ("a",))

    def test_closer_action_ranking_uses_id_not_distance(self) -> None:
        # a-zz is one edge away; a-aa and a-ab sit two edges away.  The
        # two smallest *ids* must win regardless of distance.
        kinds = {"e": "work", "w": "work",
                 "a-aa": "action", "a-ab": "action", "a-zz": "action"}
        parents = {
            "e": ["w", "a-zz"], "w": ["a-aa", "a-ab"],
            "a-aa": [], "a-ab": [], "a-zz": [],
        }
        analysis = analyze_run_graph(kinds, parents)
        self.assertEqual(
            analysis.lineage["e"].top_actions, ("a-aa", "a-ab")
        )

    def test_classify_rules(self) -> None:
        from smpipe.lineage import NodeLineage
        self.assertEqual(
            classify(NodeLineage(False, False, ("a",))), "attributed"
        )
        self.assertEqual(
            classify(NodeLineage(True, False, ("a",))), "unresolved"
        )
        self.assertEqual(
            classify(NodeLineage(False, True, ("a",))), "unresolved"
        )
        self.assertEqual(
            classify(NodeLineage(False, False, ("a", "b"))), "ambiguous"
        )
        self.assertEqual(classify(None), "unresolved")

    def test_record_normalization_folds_parent_order_and_dupes(self) -> None:
        first = parse_telemetry_record({
            "type": "work", "run_id": "r", "node_id": "n",
            "parents": ["p2", "p1", "p1"],
        }, 1)
        second = parse_telemetry_record({
            "type": "work", "run_id": "r", "node_id": "n",
            "parents": ["p1", "p2"],
        }, 2)
        self.assertEqual(first, second)

    def test_record_validation_rejects_bad_shapes(self) -> None:
        bad_cases = [
            ({"type": "action", "run_id": "r"}, "node_id"),
            ({"type": "work", "run_id": "r", "node_id": "n",
              "parents": []}, "parents"),
            ({"type": "work", "run_id": "r", "node_id": "n",
              "parents": [""]}, "parents"),
            ({"type": "error", "event_id": "e", "run_id": "r"}, "node_id"),
            ({"type": "mystery"}, "type"),
            ({"type": "action", "run_id": "r", "node_id": "n",
              "extra": 1}, "unknown"),
        ]
        for obj, hint in bad_cases:
            with self.assertRaises(TelemetryError) as ctx:
                parse_telemetry_record(obj, 7)
            self.assertIn(hint, str(ctx.exception))


# ------------------------------------------------------------- full exporter
class LineageCaseBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="smpipe-lineage-test-")
        self.enrich = os.path.join(self.tmp, "enrich.db")
        self.telemetry = os.path.join(self.tmp, "telemetry.jsonl")
        self.out = os.path.join(self.tmp, "lineage.jsonl")
        # ev-multi has two frames (one failed attribution): one row only.
        build_enrich_db(self.enrich, [
            ("ev-multi", 0, "R1", "unmapped"),
            ("ev-multi", 1, "R1", "mapped"),
            ("ev-gap", 0, "R1", "mapped"),
            ("ev-cyc", 0, "R1", "mapped"),
            ("ev-anchor-missing", 0, "R1", "mapped"),
            ("ev-bound", 0, "R1", "mapped"),
            ("ev-silent", 0, "R1", "mapped"),
        ])

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_export(self, records: list[dict]) -> list[dict]:
        write_jsonl(self.telemetry, records)
        export_lineage(self.enrich, self.telemetry, self.out)
        return read_jsonl(self.out)

    def row_map(self, rows: list[dict]) -> dict[str, dict]:
        return {row["event_id"]: row for row in rows}


class TestExporterScenarios(LineageCaseBase):
    def base_records(self) -> list[dict]:
        return [
            {"type": "action", "run_id": "r1", "node_id": "a1"},
            {"type": "action", "run_id": "r1", "node_id": "a2"},
            {"type": "action", "run_id": "r1", "node_id": "a3"},
            {"type": "work", "run_id": "r1", "node_id": "w1",
             "parents": ["a1"]},
            {"type": "work", "run_id": "r1", "node_id": "w2",
             "parents": ["a2", "m"]},
            {"type": "work", "run_id": "r1", "node_id": "c1",
             "parents": ["c2"]},
            {"type": "work", "run_id": "r1", "node_id": "c2",
             "parents": ["c1", "a3"]},
            {"type": "work", "run_id": "r1", "node_id": "e",
             "parents": ["w1", "w2", "c1"]},
            {"type": "error", "event_id": "ev-multi",
             "run_id": "r1", "node_id": "e"},
            {"type": "error", "event_id": "ev-gap",
             "run_id": "r1", "node_id": "w2"},
            {"type": "error", "event_id": "ev-cyc",
             "run_id": "r1", "node_id": "c1"},
            # Reporting node itself has no defining record.
            {"type": "error", "event_id": "ev-anchor-missing",
             "run_id": "r1", "node_id": "ghost"},
            {"type": "error", "event_id": "ev-bound",
             "run_id": "r1", "node_id": "w1"},
        ]

    def test_full_scenario_row_by_row(self) -> None:
        rows = self.run_export(self.base_records())
        rows_by_id = self.row_map(rows)
        self.assertEqual(
            [row["event_id"] for row in rows],
            sorted(rows_by_id),
        )
        anchor = rows_by_id["ev-multi"]
        self.assertEqual(anchor["run_id"], "r1")
        self.assertEqual(anchor["node_id"], "e")
        self.assertTrue(anchor["reaches_missing"])
        self.assertTrue(anchor["reaches_cycle"])
        self.assertEqual(anchor["status"], "ambiguous")
        self.assertEqual(
            [c["action_id"] for c in anchor["candidates"]],
            ["a1", "a2"],
        )
        self.assertEqual(anchor["candidates"][0]["path"],
                         ["e", "w1", "a1"])
        self.assertEqual(anchor["candidates"][1]["path"],
                         ["e", "w2", "a2"])

        gap = rows_by_id["ev-gap"]
        self.assertTrue(gap["reaches_missing"])
        self.assertFalse(gap["reaches_cycle"])
        self.assertEqual(gap["status"], "unresolved")

        cyc = rows_by_id["ev-cyc"]
        self.assertTrue(cyc["reaches_cycle"])
        self.assertFalse(cyc["reaches_missing"])
        # The cycle has an exit to a known action; it must not be swallowed.
        self.assertEqual(
            cyc["candidates"],
            [{"action_id": "a3", "path": ["c1", "c2", "a3"]}],
        )
        self.assertEqual(cyc["status"], "unresolved")

        missing_anchor = rows_by_id["ev-anchor-missing"]
        self.assertEqual(missing_anchor["status"], "unresolved")
        self.assertTrue(missing_anchor["reaches_missing"])
        self.assertFalse(missing_anchor["reaches_cycle"])
        self.assertEqual(missing_anchor["candidates"], [])

        bound = rows_by_id["ev-bound"]
        self.assertEqual(bound["status"], "attributed")
        self.assertEqual(
            bound["candidates"],
            [{"action_id": "a1", "path": ["w1", "a1"]}],
        )

        silent = rows_by_id["ev-silent"]
        self.assertEqual(silent["status"], "no_anchor")
        self.assertIsNone(silent["run_id"])
        self.assertIsNone(silent["node_id"])
        self.assertFalse(silent["reaches_missing"])
        self.assertFalse(silent["reaches_cycle"])
        self.assertEqual(silent["candidates"], [])

    def test_multi_frame_event_is_one_row(self) -> None:
        rows = self.run_export(self.base_records())
        self.assertEqual(
            sum(1 for row in rows if row["event_id"] == "ev-multi"), 1
        )

    def test_same_named_node_in_other_run_is_never_borrowed(self) -> None:
        records = self.base_records() + [
            # Another run defines node "a1" as work and its own action;
            # r1's edges must never resolve against this run's names.
            {"type": "work", "run_id": "r2", "node_id": "a1",
             "parents": ["act-r2"]},
            {"type": "action", "run_id": "r2", "node_id": "act-r2"},
            {"type": "error", "event_id": "ev-silent",
             "run_id": "r2", "node_id": "a1"},
        ]
        rows_by_id = self.row_map(self.run_export(records))
        # r2's a1 is a work node, so its only reachable action is act-r2.
        r2_row = rows_by_id["ev-silent"]
        self.assertEqual(r2_row["status"], "attributed")
        self.assertEqual(
            r2_row["candidates"],
            [{"action_id": "act-r2", "path": ["a1", "act-r2"]}],
        )
        # r1 results are unchanged.
        r1_row = rows_by_id["ev-bound"]
        self.assertEqual(
            r1_row["candidates"][0]["action_id"], "a1"
        )

    def test_reorder_and_equivalent_resends_are_identical(self) -> None:
        rng = random.Random(20261122)
        first = self.base_records()
        shuffled = first[:]
        rng.shuffle(shuffled)
        # Re-deliver equivalent records (reordered parents, duplicate rows).
        shuffled += [
            {"type": "work", "run_id": "r1", "node_id": "w2",
             "parents": ["m", "a2"]},
            {"type": "action", "run_id": "r1", "node_id": "a1"},
            {"type": "error", "event_id": "ev-bound",
             "run_id": "r1", "node_id": "w1"},
        ]
        write_jsonl(self.telemetry, first)
        export_lineage(self.enrich, self.telemetry, self.out)
        with open(self.out, encoding="utf-8") as h:
            before = h.read()
        write_jsonl(self.telemetry, shuffled)
        export_lineage(self.enrich, self.telemetry, self.out)
        with open(self.out, encoding="utf-8") as h:
            after = h.read()
        self.assertEqual(before, after)

    def test_kind_conflict_rejects_whole_export(self) -> None:
        records = self.base_records() + [
            {"type": "work", "run_id": "r1", "node_id": "a1",
             "parents": ["a2"]},
        ]
        with self.assertRaises(LineageConflict):
            self.run_export(records)

    def test_parent_set_conflict_rejects_whole_export(self) -> None:
        records = self.base_records() + [
            {"type": "work", "run_id": "r1", "node_id": "w1",
             "parents": ["a1", "a2"]},
        ]
        with self.assertRaises(LineageConflict):
            self.run_export(records)

    def test_event_binding_conflict_rejects(self) -> None:
        records = self.base_records() + [
            {"type": "error", "event_id": "ev-bound",
             "run_id": "r1", "node_id": "w2"},
        ]
        with self.assertRaises(LineageConflict):
            self.run_export(records)

    def test_unknown_event_id_rejects(self) -> None:
        records = self.base_records() + [
            {"type": "error", "event_id": "ev-unknown",
             "run_id": "r1", "node_id": "e"},
        ]
        with self.assertRaises(LineageConflict):
            self.run_export(records)

    def test_tail_line_conflict_preserves_previous_export(self) -> None:
        write_jsonl(self.telemetry, self.base_records())
        export_lineage(self.enrich, self.telemetry, self.out)
        with open(self.out, encoding="utf-8") as h:
            before = h.read()
        # A conflicting last line: action a1 redefined as a work node.
        bad = self.base_records() + [
            {"type": "work", "run_id": "r1", "node_id": "a1",
             "parents": ["a2"]},
        ]
        write_jsonl(self.telemetry, bad)
        with self.assertRaises(LineageConflict):
            export_lineage(self.enrich, self.telemetry, self.out)
        with open(self.out, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), before)

    def test_enrich_database_is_opened_read_only(self) -> None:
        before = os.stat(self.enrich).st_mtime_ns
        self.run_export(self.base_records())
        self.assertEqual(os.stat(self.enrich).st_mtime_ns, before)
        # No WAL sidecar may be created against an immutable open.
        self.assertFalse(os.path.exists(self.enrich + "-wal"))

    def test_non_empty_wal_is_rejected(self) -> None:
        wal_path = self.enrich + "-wal"
        with open(wal_path, "wb") as handle:
            handle.write(b"x")
        with self.assertRaises(Exception):
            self.run_export(self.base_records())
        os.remove(wal_path)

    def test_malformed_jsonl_reports_line_number(self) -> None:
        with open(self.telemetry, "w", encoding="utf-8") as handle:
            handle.write("\n")
            handle.write(json.dumps(self.base_records()[0]) + "\n")
            handle.write("{not json\n")
        with self.assertRaises(TelemetryError) as ctx:
            export_lineage(self.enrich, self.telemetry, self.out)
        self.assertIn("line 3", str(ctx.exception))

    def test_scratch_database_is_removed_on_success_and_failure(self) -> None:
        import glob
        import tempfile as tf
        pattern = os.path.join(tf.gettempdir(), "smpipe-lineage-*")
        before = set(glob.glob(pattern))
        self.run_export(self.base_records())
        self.assertEqual(set(glob.glob(pattern)), before)
        bad = self.base_records() + [
            {"type": "work", "run_id": "r1", "node_id": "a1",
             "parents": ["a2"]},
        ]
        write_jsonl(self.telemetry, bad)
        with self.assertRaises(LineageConflict):
            export_lineage(self.enrich, self.telemetry, self.out)
        self.assertEqual(set(glob.glob(pattern)), before)


# ------------------------------------------------- shared upstream reuse work
class TestSharedUpstreamReuse(LineageCaseBase):
    def test_path_work_grows_with_graph_not_error_count(self) -> None:
        # One anchor subgraph shared by many errors.
        records = [
            {"type": "action", "run_id": "r", "node_id": "act-1"},
            {"type": "action", "run_id": "r", "node_id": "act-2"},
            {"type": "work", "run_id": "r", "node_id": "root",
             "parents": ["act-1", "act-2"]},
        ]
        error_events = ["ev-multi", "ev-gap", "ev-cyc", "ev-bound",
                        "ev-anchor-missing"]
        for event_id in error_events:
            records.append(
                {"type": "error", "event_id": event_id,
                 "run_id": "r", "node_id": "root"}
            )
        write_jsonl(self.telemetry, records)
        with LineageExporter(self.enrich) as exporter:
            exporter.ingest_telemetry(self.telemetry)
            exporter.build_results()
            # One distinct anchor => one BFS table, regardless of errors.
            self.assertEqual(exporter.metrics["path_bfs_runs"], 1)
            # Five errors share the same two candidate paths: lookups are
            # memoized after the first anchor's table is built.
            self.assertEqual(exporter.metrics["path_lookups"], 10)
            self.assertGreaterEqual(exporter.metrics["path_cache_hits"], 8)
            exporter.write_output(self.out)

        rows = read_jsonl(self.out)
        for event_id in error_events:
            row = self.row_map(rows)[event_id]
            self.assertEqual(row["status"], "ambiguous")
            self.assertEqual(
                [c["action_id"] for c in row["candidates"]],
                ["act-1", "act-2"],
            )


# --------------------------------------------- random cross-check vs reference
class TestReferenceCrossCheck(unittest.TestCase):
    def _graph_case(self, seed: int) -> tuple:
        rng = random.Random(seed)
        node_count = rng.randint(1, 14)
        nodes = [f"n{i:02d}" for i in range(node_count)]
        kinds = {node: (ACTION if rng.random() < 0.35 else WORK)
                 for node in nodes}
        parents: dict[str, list[str]] = {}
        for node in nodes:
            if kinds[node] == ACTION:
                parents[node] = []
                continue
            pool = [other for other in nodes if other != node]
            chosen = rng.sample(pool, rng.randint(1, min(3, len(pool)))) \
                if pool else []
            if not chosen:
                # A work node must keep a non-empty parent list per spec;
                # make it a dangling parent (sampling gap) instead.
                chosen = [f"ghost-{seed}-{node}"]
            elif rng.random() < 0.2:
                chosen.append(f"ghost-{seed}-{node}")
            parents[node] = chosen
        return kinds, parents

    def test_random_graphs_match_reference(self) -> None:
        for seed in range(120):
            kinds, parents = self._graph_case(seed)
            analysis = analyze_run_graph(
                dict(kinds), {n: list(p) for n, p in parents.items()}
            )
            reference = ReferenceLineage()
            for node, kind in kinds.items():
                reference.add_node("r", node, kind)
            for node, upstream in parents.items():
                if upstream:
                    reference.add_work("r", node, list(upstream))
            for node in kinds:
                ref_actions = reference.reachable_actions("r", node)[:2]
                self.assertEqual(
                    analysis.lineage[node].top_actions,
                    tuple(ref_actions),
                    msg=f"seed={seed} node={node}",
                )
                self.assertEqual(
                    analysis.lineage[node].reaches_missing,
                    reference._reaches_missing("r", node),
                    msg=f"seed={seed} node={node} gap",
                )
                self.assertEqual(
                    analysis.lineage[node].reaches_cycle,
                    reference._reaches_cycle("r", node),
                    msg=f"seed={seed} node={node} cycle",
                )
                for action in ref_actions:
                    self.assertEqual(
                        shortest_lineage_path(
                            analysis.adjacency, node, action
                        ),
                        reference.shortest_path("r", node, action),
                        msg=f"seed={seed} path {node}->{action}",
                    )

    def test_full_export_matches_reference_rows(self) -> None:
        rng = random.Random(777)
        tmp = tempfile.mkdtemp(prefix="smpipe-lineage-xref-")
        try:
            events = [f"evt-{i}" for i in range(12)]
            enrich = os.path.join(tmp, "enrich.db")
            build_enrich_db(
                enrich, [(event, 0, "R1", "mapped") for event in events]
            )
            reference = ReferenceLineage()
            records: list[dict] = []
            run_ids = ["run-A", "run-B"]
            for run_index, run_id in enumerate(run_ids):
                nodes = [f"{run_id}-n{i}" for i in range(8)]
                for node in nodes:
                    is_action = rng.random() < 0.4
                    if is_action:
                        reference.add_node(run_id, node, ACTION)
                        records.append(
                            {"type": "action", "run_id": run_id,
                             "node_id": node}
                        )
                        continue
                    upstream = rng.sample(
                        [n for n in nodes if n != node],
                        rng.randint(1, 3),
                    )
                    reference.add_work(run_id, node, list(upstream))
                    records.append({
                        "type": "work", "run_id": run_id,
                        "node_id": node, "parents": upstream,
                    })
            # Bind the first events to defined anchors; one to a ghost.
            for index, event_id in enumerate(events):
                if index < 5:
                    run_id = run_ids[index % 2]
                    anchor = f"{run_id}-n{index % 8}"
                    reference.bind_error(event_id, run_id, anchor)
                    records.append({
                        "type": "error", "event_id": event_id,
                        "run_id": run_id, "node_id": anchor,
                    })
                elif index == 6:
                    reference.bind_error(event_id, "run-A", "no-such-node")
                    records.append({
                        "type": "error", "event_id": event_id,
                        "run_id": "run-A", "node_id": "no-such-node",
                    })
                # remaining events stay unbound -> no_anchor
            telemetry = os.path.join(tmp, "t.jsonl")
            out = os.path.join(tmp, "out.jsonl")
            write_jsonl(telemetry, records)
            export_lineage(enrich, telemetry, out)
            actual = read_jsonl(out)
            expected = reference.all_rows(set(events))
            self.assertEqual(actual, expected)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ----------------------------------------------------------------- CLI surface
class TestCliEntry(unittest.TestCase):
    def test_cli_export_and_conflict_exit_code(self) -> None:
        tmp = tempfile.mkdtemp(prefix="smpipe-lineage-cli-")
        try:
            enrich = os.path.join(tmp, "enrich.db")
            build_enrich_db(enrich, [("e1", 0, "R1", "mapped")])
            good = os.path.join(tmp, "good.jsonl")
            write_jsonl(good, [
                {"type": "action", "run_id": "r", "node_id": "a"},
                {"type": "work", "run_id": "r", "node_id": "e",
                 "parents": ["a"]},
                {"type": "error", "event_id": "e1",
                 "run_id": "r", "node_id": "e"},
            ])
            out = os.path.join(tmp, "out.jsonl")
            proc = subprocess.run(
                [sys.executable, "-I", SMPIPE, "export-lineage",
                 "--enrich-db", enrich, "--telemetry", good, "--out", out],
                cwd=ROOT, capture_output=True, text=True, check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            report = json.loads(proc.stdout)
            self.assertEqual(report["rows_written"], 1)

            bad = os.path.join(tmp, "bad.jsonl")
            write_jsonl(bad, [
                {"type": "error", "event_id": "ghost-event",
                 "run_id": "r", "node_id": "e"},
            ])
            proc = subprocess.run(
                [sys.executable, "-I", SMPIPE, "export-lineage",
                 "--enrich-db", enrich, "--telemetry", bad, "--out", out],
                cwd=ROOT, capture_output=True, text=True, check=False,
            )
            self.assertEqual(proc.returncode, 2)
            self.assertIn("lineage_input_conflict", proc.stderr)
            # Previous export still on disk.
            self.assertTrue(os.path.exists(out))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
