"""Behavioral suite for the exposure first-error fact table.

Run with::

    python -I tests/run_tests.py

The suite builds its own tiny *sealed* enrichment database (a ``frames``
table shaped exactly like the one ``enrich`` produces), drives the
incremental :class:`smpipe.facts.FactStore` through hand-computed scenarios,
and cross-checks every fact snapshot against
:mod:`tests.reference_facts`, an independent from-scratch implementation.
"""

from __future__ import annotations

import json
import os
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

from smpipe.errors import (  # noqa: E402
    BatchError,
    BatchSequenceError,
    EnrichDatabaseError,
)
from smpipe.facts import (  # noqa: E402
    FactStore,
    apply_batch,
    exposure_at,
    session_timelines,
)
from reference_facts import ReferenceFacts  # noqa: E402


# --------------------------------------------------------------------- helpers
def build_enrich_db(path: str, frames: list[tuple[str, int, str, str]]
                    ) -> None:
    """Create a sealed-style frames table.

    Each tuple is ``(event_id, frame_index, release_id, status)``; the
    production feature only reads those three columns out of ``result_json``
    plus the primary key, so a minimal faithful row is enough.
    """
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE frames(event_id TEXT NOT NULL, frame_index INTEGER "
        "NOT NULL, raw_hash TEXT NOT NULL, release_id TEXT NOT NULL, "
        "input_json TEXT NOT NULL, result_json TEXT, "
        "PRIMARY KEY(event_id, frame_index))"
    )
    conn.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    for event_id, frame_index, release_id, status in frames:
        result = json.dumps({
            "event_id": event_id,
            "frame_index": frame_index,
            "release_id": release_id,
            "status": status,
        }, sort_keys=True)
        conn.execute(
            "INSERT INTO frames VALUES(?, ?, ?, ?, ?, ?)",
            (event_id, frame_index, "h:" + str(frame_index), release_id,
             "{}", result),
        )
    conn.commit()
    conn.close()


# A fixed frame library reused across tests.
# ev-multi: mapped frames for two releases; smallest mapped index must win.
# ev-ra: release R1 mapped at frame 2 (frame 0 unmapped).
# ev-rb: R2 mapped.  ev-rc: R3 mapped.  ev-bad: only unmapped/invalid.
LIBRARY = [
    ("ev-multi", 0, "R1", "unmapped"),
    ("ev-multi", 1, "R1", "mapped"),
    ("ev-multi", 2, "R2", "mapped"),
    ("ev-multi", 3, "R2", "unmapped"),
    ("ev-ra", 0, "R1", "unmapped"),
    ("ev-ra", 1, "R1", "unmapped"),
    ("ev-ra", 2, "R1", "mapped"),
    ("ev-rb", 0, "R2", "mapped"),
    ("ev-rc", 0, "R3", "mapped"),
    ("ev-rc", 1, "R3", "mapped"),
    ("ev-bad", 0, "R1", "unmapped"),
    ("ev-bad", 1, "R2", "invalid_frame"),
    ("ev-z", 0, "R1", "mapped"),
]


class FactCaseBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="smpipe-facts-")
        self.enrich = os.path.join(self.tmp, "enrich.db")
        self.work = os.path.join(self.tmp, "work.db")
        build_enrich_db(self.enrich, LIBRARY)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def batch(self, seq: int, exposures=None, events=None) -> bytes:
        return json.dumps({
            "seq": seq,
            "exposure_changes": exposures or [],
            "event_changes": events or [],
        }).encode("utf-8")

    def store(self) -> FactStore:
        return FactStore(self.work, enrich_path=self.enrich)

    def facts_map(self, store: FactStore) -> dict[str, dict]:
        return {row["exposure_id"]: row for row in store.export_facts()}


class TestPureTimeline(FactCaseBase):
    def test_half_open_interval_and_inclusive_start(self) -> None:
        exps = {
            "a": _exp("a", "s", "R1", 10),
            "b": _exp("b", "s", "R2", 20),
        }
        lines = session_timelines(exps)["s"]
        self.assertIsNone(exposure_at(lines, 9))
        self.assertEqual(exposure_at(lines, 10).exposure_id, "a")
        self.assertEqual(exposure_at(lines, 19).exposure_id, "a")
        # An event exactly on the later start belongs to the later exposure.
        self.assertEqual(exposure_at(lines, 20).exposure_id, "b")
        self.assertEqual(exposure_at(lines, 10_000).exposure_id, "b")

    def test_final_state_same_batch_swap_is_legal(self) -> None:
        old = {
            "a": _exp("a", "s", "R1", 10),
            "b": _exp("b", "s", "R2", 20),
        }
        payload = {
            "seq": 1,
            "exposure_changes": [
                {"exposure_id": "a", "session_id": "s", "release_id": "R1",
                 "start_ms": 20},
                {"exposure_id": "b", "session_id": "s", "release_id": "R2",
                 "start_ms": 10},
            ],
        }
        new_exp, _ = apply_batch(old, {}, payload)
        self.assertEqual(new_exp["a"].start_ms, 20)
        self.assertEqual(new_exp["b"].start_ms, 10)

    def test_final_conflict_is_rejected(self) -> None:
        old = {"a": _exp("a", "s", "R1", 10)}
        payload = {
            "seq": 1,
            "exposure_changes": [
                {"exposure_id": "b", "session_id": "s", "release_id": "R2",
                 "start_ms": 10},
            ],
        }
        with self.assertRaises(BatchError):
            apply_batch(old, {}, payload)

    def test_duplicate_id_in_batch_rejected(self) -> None:
        payload = {
            "seq": 1,
            "exposure_changes": [
                {"exposure_id": "a", "session_id": "s", "release_id": "R1",
                 "start_ms": 1},
                {"exposure_id": "a", "session_id": "s", "release_id": "R1",
                 "start_ms": 2},
            ],
        }
        with self.assertRaises(BatchError):
            apply_batch({}, {}, payload)

    def test_delete_unknown_and_bad_shapes(self) -> None:
        with self.assertRaises(BatchError):
            apply_batch({}, {}, {"seq": 1, "exposure_changes": [
                {"exposure_id": "ghost", "op": "delete"}]})
        with self.assertRaises(BatchError):
            apply_batch({}, {}, {"seq": 1, "exposure_changes": [
                {"exposure_id": "a", "session_id": "s",
                 "release_id": "R1", "start_ms": -1}]})
        with self.assertRaises(BatchError):
            apply_batch({}, {}, {"seq": 1, "event_changes": [
                {"event_id": "e", "session_id": "s", "occurred_ms": True}]})
        with self.assertRaises(BatchError):
            apply_batch({}, {}, {"seq": 1, "event_changes": [
                {"event_id": "e", "op": "delete", "occurred_ms": 5}]})


def _exp(exp_id: str, session: str, release: str, start: int):
    from smpipe.facts import Exposure
    return Exposure(exp_id, session, release, start)


class TestNarrativeScenario(FactCaseBase):
    """The hand-computed scenario requested in the task, batch by batch."""

    def test_full_narrative_and_reference_agreement(self) -> None:
        reference = ReferenceFacts.from_enrich_db(self.enrich)
        store = self.store()
        try:
            # Batch 1: two adjacent exposures X1(R1,[100,200)) X2(R2,[200,)).
            # ev-rb lands exactly on X2's start and is mapped for R2.
            b1 = self.batch(
                1,
                exposures=[
                    {"exposure_id": "X1", "session_id": "s",
                     "release_id": "R1", "start_ms": 100},
                    {"exposure_id": "X2", "session_id": "s",
                     "release_id": "R2", "start_ms": 200},
                ],
                events=[
                    {"event_id": "ev-rb", "session_id": "s",
                     "occurred_ms": 200},
                    # arrives before its exposure exists: not bad input
                    {"event_id": "ev-ra", "session_id": "s",
                     "occurred_ms": 150},
                ],
            )
            report = store.import_batch(b1)
            reference.apply(json.loads(b1))
            self._assert_matches(store, reference)
            facts = self.facts_map(store)
            self.assertEqual(facts["X1"]["first_event_id"], "ev-ra")
            self.assertEqual(facts["X1"]["first_frame_index"], 2)
            self.assertEqual(facts["X1"]["first_delay_ms"], 50)
            self.assertEqual(facts["X2"]["first_event_id"], "ev-rb")
            self.assertEqual(facts["X2"]["first_delay_ms"], 0)
            self.assertEqual(report["facts_inserted"], 2)

            # Batch 2: insert X3(R3) at 150 and move X2 to 300.  ev-rb at 200
            # now sits in X3 (R3) and only maps for R2: it must NOT be handed
            # back to X1, it simply stops qualifying.  X2 has no first error.
            b2 = self.batch(
                2,
                exposures=[
                    {"exposure_id": "X3", "session_id": "s",
                     "release_id": "R3", "start_ms": 150},
                    {"exposure_id": "X2", "session_id": "s",
                     "release_id": "R2", "start_ms": 300},
                ],
            )
            store.import_batch(b2)
            reference.apply(json.loads(b2))
            self._assert_matches(store, reference)
            facts = self.facts_map(store)
            self.assertEqual(
                (facts["X1"]["start_ms"], facts["X1"]["end_ms"]), (100, 150))
            self.assertIsNone(facts["X1"]["first_event_id"])
            self.assertEqual(
                (facts["X3"]["start_ms"], facts["X3"]["end_ms"]), (150, 300))
            self.assertIsNone(facts["X3"]["first_event_id"])
            self.assertIsNone(facts["X2"]["end_ms"])
            self.assertIsNone(facts["X2"]["first_event_id"])

            # Batch 3: move ev-rb onto X2's start; add a same-millisecond
            # tie candidate ev-z (R1-mapped, does not qualify for R2) and
            # ev-rc (R3-mapped, does not qualify for R2 either).  ev-rb is
            # the only R2 candidate and becomes X2's first error.
            b3 = self.batch(
                3,
                events=[
                    {"event_id": "ev-rb", "session_id": "s",
                     "occurred_ms": 300},
                    {"event_id": "ev-z", "session_id": "s",
                     "occurred_ms": 300},
                    {"event_id": "ev-rc", "session_id": "s",
                     "occurred_ms": 300},
                ],
            )
            store.import_batch(b3)
            reference.apply(json.loads(b3))
            self._assert_matches(store, reference)
            facts = self.facts_map(store)
            self.assertEqual(facts["X2"]["first_event_id"], "ev-rb")
            self.assertEqual(facts["X2"]["first_frame_index"], 0)
            self.assertEqual(facts["X2"]["first_delay_ms"], 0)

            # Batch 4: delete ev-rb.  X2 loses its first error; nothing else
            # in its interval qualifies.  Deleting an event loc never touches
            # the sealed frames table.
            b4 = self.batch(
                4,
                events=[{"event_id": "ev-rb", "op": "delete"}],
            )
            store.import_batch(b4)
            reference.apply(json.loads(b4))
            self._assert_matches(store, reference)
            self.assertIsNone(self.facts_map(store)["X2"]["first_event_id"])

            # Batch 5: relocate ev-rc (R3) into X3; it is the backup that now
            # takes over as X3's first error, hand-checked delay 300-150=150.
            b5 = self.batch(
                5,
                events=[
                    {"event_id": "ev-rc", "session_id": "s",
                     "occurred_ms": 300 - 1},
                ],
            )
            store.import_batch(b5)
            reference.apply(json.loads(b5))
            self._assert_matches(store, reference)
            facts = self.facts_map(store)
            self.assertEqual(facts["X3"]["first_event_id"], "ev-rc")
            # smallest mapped frame_index of ev-rc is 0
            self.assertEqual(facts["X3"]["first_frame_index"], 0)
            self.assertEqual(facts["X3"]["first_occurred_ms"], 299)
            self.assertEqual(facts["X3"]["first_delay_ms"], 149)

            # Batch 6: delete X3.  X1's end becomes 300 (X2), X1 still has no
            # qualifying event; the X3 fact row disappears.
            b6 = self.batch(
                6,
                exposures=[{"exposure_id": "X3", "op": "delete"}],
            )
            store.import_batch(b6)
            reference.apply(json.loads(b6))
            self._assert_matches(store, reference)
            facts = self.facts_map(store)
            self.assertNotIn("X3", facts)
            self.assertEqual(facts["X1"]["end_ms"], 300)
        finally:
            store.close()

    def _assert_matches(self, store: FactStore,
                        reference: ReferenceFacts) -> None:
        self.assertEqual(store.export_facts(), reference.compute())


class TestTieAndMultiFrame(FactCaseBase):
    def test_same_ms_string_id_tie_and_smallest_frame(self) -> None:
        store = self.store()
        try:
            store.import_batch(self.batch(
                1,
                exposures=[{"exposure_id": "X", "session_id": "s",
                            "release_id": "R1", "start_ms": 0}],
                events=[
                    # Both R1-mapped candidates at the same millisecond; the
                    # string-minimum event id must win, not import order.
                    {"event_id": "ev-z", "session_id": "s",
                     "occurred_ms": 50},
                    {"event_id": "ev-multi", "session_id": "s",
                     "occurred_ms": 50},
                ],
            ))
            facts = self.facts_map(store)
            # "ev-multi" < "ev-z" lexicographically.
            self.assertEqual(facts["X"]["first_event_id"], "ev-multi")
            # ev-multi's smallest R1 mapped frame is index 1 (0 is unmapped).
            self.assertEqual(facts["X"]["first_frame_index"], 1)
            self.assertEqual(facts["X"]["first_delay_ms"], 50)
        finally:
            store.close()

    def test_unqualified_event_does_not_count(self) -> None:
        store = self.store()
        try:
            store.import_batch(self.batch(
                1,
                exposures=[{"exposure_id": "X", "session_id": "s",
                            "release_id": "R1", "start_ms": 0}],
                events=[
                    {"event_id": "ev-bad", "session_id": "s",
                     "occurred_ms": 10},
                ],
            ))
            self.assertIsNone(
                self.facts_map(store)["X"]["first_event_id"])
            # Once a qualifying event appears later, it takes over.
            store.import_batch(self.batch(
                2,
                events=[{"event_id": "ev-ra", "session_id": "s",
                         "occurred_ms": 90}],
            ))
            facts = self.facts_map(store)
            self.assertEqual(facts["X"]["first_event_id"], "ev-ra")
            self.assertEqual(facts["X"]["first_frame_index"], 2)
        finally:
            store.close()


class TestBatchLedger(FactCaseBase):
    def test_replay_noop_skip_and_reject_rules(self) -> None:
        store = self.store()
        try:
            b1 = self.batch(1, exposures=[
                {"exposure_id": "X", "session_id": "s", "release_id": "R1",
                 "start_ms": 0}])
            first = store.import_batch(b1)
            self.assertFalse(first["noop"])
            replay = store.import_batch(b1)  # identical bytes
            self.assertTrue(replay["noop"])
            self.assertEqual(store.last_seq(), 1)

            # Same seq, different bytes -> rejected.
            tampered = self.batch(1, exposures=[
                {"exposure_id": "Y", "session_id": "s", "release_id": "R1",
                 "start_ms": 0}])
            with self.assertRaises(BatchSequenceError):
                store.import_batch(tampered)
            # Skip a number.
            with self.assertRaises(BatchSequenceError):
                store.import_batch(self.batch(3))
            # The rejected batches left no state behind.
            self.assertEqual(store.last_seq(), 1)
            self.assertEqual(set(self.facts_map(store)), {"X"})

            # Non-JSON is a bad batch, not a ledger mutation.
            with self.assertRaises(BatchError):
                store.import_batch(b"{not json")
            self.assertEqual(store.last_seq(), 1)
        finally:
            store.close()

    def test_rejected_batch_is_atomic(self) -> None:
        store = self.store()
        try:
            store.import_batch(self.batch(
                1,
                exposures=[{"exposure_id": "X", "session_id": "s",
                            "release_id": "R1", "start_ms": 0}],
                events=[{"event_id": "ev-ra", "session_id": "s",
                         "occurred_ms": 5}],
            ))
            before = store.export_facts()
            # Bad: duplicate exposure id inside the batch.
            bad = self.batch(2, exposures=[
                {"exposure_id": "Y", "session_id": "s", "release_id": "R2",
                 "start_ms": 10},
                {"exposure_id": "Y", "session_id": "s", "release_id": "R2",
                 "start_ms": 20},
            ])
            with self.assertRaises(BatchError):
                store.import_batch(bad)
            self.assertEqual(store.export_facts(), before)
            self.assertEqual(store.last_seq(), 1)
        finally:
            store.close()

    def test_rules_survive_reopen(self) -> None:
        store = self.store()
        b1 = self.batch(1, exposures=[
            {"exposure_id": "X", "session_id": "s", "release_id": "R1",
             "start_ms": 0}])
        store.import_batch(b1)
        store.close()

        reopened = FactStore(self.work, enrich_path=self.enrich)
        try:
            self.assertTrue(reopened.import_batch(b1)["noop"])
            with self.assertRaises(BatchSequenceError):
                reopened.import_batch(self.batch(1, exposures=[
                    {"exposure_id": "Z", "session_id": "s",
                     "release_id": "R1", "start_ms": 0}]))
            with self.assertRaises(BatchSequenceError):
                reopened.import_batch(self.batch(3))
            reopened.import_batch(self.batch(
                2,
                events=[{"event_id": "ev-ra", "session_id": "s",
                         "occurred_ms": 7}]))
            self.assertEqual(
                self.facts_map(reopened)["X"]["first_event_id"], "ev-ra")
        finally:
            reopened.close()

    def test_enrich_database_fingerprint_is_pinned(self) -> None:
        store = self.store()
        store.import_batch(self.batch(1, exposures=[
            {"exposure_id": "X", "session_id": "s", "release_id": "R1",
             "start_ms": 0}]))
        store.close()
        with open(self.enrich, "ab") as handle:
            handle.write(b"\x00")
        with self.assertRaises(EnrichDatabaseError):
            FactStore(self.work, enrich_path=self.enrich)

    def test_missing_enrich_database_is_domain_error(self) -> None:
        missing = os.path.join(self.tmp, "absent.db")
        with self.assertRaises(EnrichDatabaseError):
            FactStore(self.work, enrich_path=missing)
        # A fresh working store with no bound enrich cannot import either.
        bare = FactStore(os.path.join(self.tmp, "bare.db"))
        try:
            with self.assertRaises(EnrichDatabaseError):
                bare.import_batch(self.batch(1))
        finally:
            bare.close()

    def test_non_sqlite_enrich_database_rejected(self) -> None:
        bogus = os.path.join(self.tmp, "bogus.db")
        with open(bogus, "wb") as handle:
            handle.write(b"not a database")
        with self.assertRaises(EnrichDatabaseError):
            FactStore(self.work, enrich_path=bogus)


class TestLocality(FactCaseBase):
    """A local correction must not walk disjoint parts of a long timeline."""

    def _seed_long_timeline(self, store: FactStore, n: int,
                            other_events: int) -> None:
        exposures = [
            {"exposure_id": f"E{i:04d}", "session_id": "long",
             "release_id": "R1", "start_ms": i * 1000}
            for i in range(n)
        ]
        events = [
            {"event_id": "ev-z", "session_id": "long", "occurred_ms": 5},
        ]
        # Distractor events in an unrelated session.
        for i in range(other_events):
            events.append({
                "event_id": f"noise-{i}", "session_id": "other",
                "occurred_ms": i,
            })
        store.import_batch(self.batch(1, exposures=exposures, events=events))

    def test_local_event_correction_touches_only_local_facts(self) -> None:
        store = self.store()
        n = 200
        try:
            self._seed_long_timeline(store, n, other_events=500)
            baseline = {row["exposure_id"]: row for row in
                        store.export_facts()}
            store.reset_metrics()

            # Correct one event near the very end of the long timeline.
            target = f"E{n - 1:04d}"
            store.import_batch(self.batch(
                2,
                events=[{"event_id": "ev-z", "session_id": "long",
                         "occurred_ms": (n - 1) * 1000 + 25}],
            ))
            after = {row["exposure_id"]: row for row in store.export_facts()}
            metrics = store.metrics_snapshot()

            # Only the two endpoint exposures change: E0000 loses its first
            # error, E0199 gains it.  Every one of the 198 disjoint middle
            # rows is byte-identical and was never rewritten.
            changed = sorted(
                key for key in baseline if baseline[key] != after[key])
            self.assertEqual(changed, ["E0000", target])
            self.assertIsNone(after["E0000"]["first_event_id"])
            self.assertEqual(after[target]["first_event_id"], "ev-z")
            self.assertEqual(after[target]["first_delay_ms"], 25)
            # Two local recomputes, two endpoint rewrites; no insert/delete
            # and never a bulk rewrite of the 198 disjoint middle rows.
            self.assertEqual(metrics["fact_rows_updated"], 2)
            self.assertEqual(metrics["fact_rows_inserted"], 0)
            self.assertEqual(metrics.get("fact_rows_deleted", 0), 0)
            self.assertLessEqual(
                metrics["frame_qualification_queries"], 2)

            # The bounded window read stays bounded regardless of timeline size.
            plan = store._conn.execute(
                "EXPLAIN QUERY PLAN SELECT event_id, occurred_ms FROM "
                "event_locs WHERE session_id=? AND occurred_ms>=? "
                "AND occurred_ms<? ORDER BY occurred_ms, event_id",
                ("long", (n - 1) * 1000, n * 1000),
            ).fetchall()
            plan_text = " ".join(" ".join(str(v) for v in row)
                                 for row in plan)
            self.assertIn("idx_event_window", plan_text)
            self.assertNotIn("SCAN event_locs", plan_text)
        finally:
            store.close()

    def test_long_distance_event_jump_touches_only_two_facts(self) -> None:
        store = self.store()
        n = 200
        try:
            self._seed_long_timeline(store, n, other_events=0)
            baseline = {row["exposure_id"]: row for row in
                        store.export_facts()}
            store.reset_metrics()
            # Move ev-z from the very first exposure to the very last one: a
            # span covering all 200 intervals.  Only the two holder facts may
            # be recomputed; the 198 disjoint middle intervals stay untouched.
            store.import_batch(self.batch(
                2,
                events=[{"event_id": "ev-z", "session_id": "long",
                         "occurred_ms": (n - 1) * 1000 + 9}],
            ))
            after = {row["exposure_id"]: row for row in store.export_facts()}
            changed = sorted(
                key for key in baseline if baseline[key] != after[key])
            self.assertEqual(changed, ["E0000", f"E{n - 1:04d}"])
            metrics = store.metrics_snapshot()
            self.assertEqual(metrics["fact_rows_updated"], 2)
            # Each recompute reads one bounded index window; nothing scans.
            self.assertLessEqual(
                metrics["frame_qualification_queries"], 2)
        finally:
            store.close()

    def test_enrich_lookup_uses_primary_key_never_full_scan(self) -> None:
        store = self.store()
        try:
            plan = store.explain_enrich_plan("ev-ra", "R1")
            self.assertIn("frames", plan)
            # Primary key index, not a full table scan.
            self.assertIn("SEARCH", plan)
            self.assertNotIn("SCAN frames", plan)
        finally:
            store.close()

    def test_unrelated_session_is_untouched(self) -> None:
        store = self.store()
        try:
            self._seed_long_timeline(store, 50, other_events=300)
            # Build facts for the noise session too.
            store.import_batch(self.batch(
                2,
                exposures=[{"exposure_id": "N0", "session_id": "other",
                            "release_id": "R1", "start_ms": 0}],
            ))
            before = {row["exposure_id"]: row for row in
                      store.export_facts()}
            store.reset_metrics()
            # Insert an exposure in the long session only.
            store.import_batch(self.batch(
                3,
                exposures=[{"exposure_id": "INS", "session_id": "long",
                            "release_id": "R1", "start_ms": 1500}],
            ))
            after = {row["exposure_id"]: row for row in
                     store.export_facts()}
            noise_before = before["N0"]
            self.assertEqual(after["N0"], noise_before)
            # Only long-session rows around the insertion point changed.
            for key, value in before.items():
                if value["session_id"] != "long":
                    self.assertEqual(after[key], value)
        finally:
            store.close()


class TestDifferentialRandomized(FactCaseBase):
    """Random walk: production vs. from-scratch reference after every batch."""

    def test_randomized_walk_matches_reference(self) -> None:
        import random
        from smpipe.facts import Exposure, EventLoc
        rng = random.Random(20261122)
        reference = ReferenceFacts.from_enrich_db(self.enrich)
        store = self.store()
        live_exp: dict[str, Exposure] = {}
        live_evt: dict[str, EventLoc] = {}
        try:
            seq = 0
            for _step in range(60):
                seq += 1
                payload = {"seq": seq,
                           "exposure_changes": [], "event_changes": []}
                for _ in range(rng.randint(0, 4)):
                    if live_exp and rng.random() < 0.3:
                        exp_id = rng.choice(list(live_exp))
                        payload["exposure_changes"].append(
                            {"exposure_id": exp_id, "op": "delete"})
                    else:
                        exp_id = f"E{rng.randrange(40):02d}"
                        payload["exposure_changes"].append({
                            "exposure_id": exp_id,
                            "session_id": f"s{rng.randrange(3)}",
                            "release_id": rng.choice(["R1", "R2", "R3"]),
                            "start_ms": rng.randrange(0, 5000),
                        })
                for _ in range(rng.randint(0, 4)):
                    if live_evt and rng.random() < 0.3:
                        evt_id = rng.choice(list(live_evt))
                        payload["event_changes"].append(
                            {"event_id": evt_id, "op": "delete"})
                    else:
                        evt_id = rng.choice(
                            ["ev-ra", "ev-rb", "ev-rc", "ev-multi",
                             "ev-bad", "ev-z"])
                        payload["event_changes"].append({
                            "event_id": evt_id,
                            "session_id": f"s{rng.randrange(3)}",
                            "occurred_ms": rng.randrange(0, 5000),
                        })

                raw = json.dumps(payload).encode("utf-8")
                # Validity oracle: the production *pure* projection.  Only the
                # fact recomputation is cross-checked against the independent
                # from-scratch reference below.
                try:
                    next_exp, next_evt = apply_batch(
                        live_exp, live_evt, payload)
                except BatchError:
                    with self.assertRaises(BatchError):
                        store.import_batch(raw)
                    seq -= 1  # rejected batch consumes no sequence number
                    continue
                store.import_batch(raw)
                live_exp, live_evt = next_exp, next_evt
                reference.apply(payload)
                self.assertEqual(store.export_facts(), reference.compute())
        finally:
            store.close()


class TestCliEndToEnd(FactCaseBase):
    def test_cli_import_reopen_export(self) -> None:
        smpipe = os.path.join(ROOT, "smpipe.py")
        batch_path = os.path.join(self.tmp, "b1.json")
        out_path = os.path.join(self.tmp, "facts.jsonl")
        with open(batch_path, "wb") as handle:
            handle.write(self.batch(
                1,
                exposures=[
                    {"exposure_id": "X1", "session_id": "s",
                     "release_id": "R1", "start_ms": 100},
                    {"exposure_id": "X2", "session_id": "s",
                     "release_id": "R2", "start_ms": 200},
                ],
                events=[
                    {"event_id": "ev-ra", "session_id": "s",
                     "occurred_ms": 120},
                    {"event_id": "ev-rb", "session_id": "s",
                     "occurred_ms": 200},
                ],
            ))

        def run(*extra: str) -> subprocess.CompletedProcess:
            return subprocess.run(
                [sys.executable, "-I", smpipe, *extra],
                cwd=ROOT, capture_output=True, text=True, check=False,
            )

        proc = run("import-facts", "--work-db", self.work,
                   "--enrich-db", self.enrich, "--batch", batch_path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # Reopen without --enrich-db: the recorded binding is reused.
        proc = run("export-facts", "--work-db", self.work,
                   "--out", out_path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        lines = [json.loads(line) for line in
                 open(out_path, encoding="utf-8")]
        self.assertEqual([row["exposure_id"] for row in lines], ["X1", "X2"])
        self.assertEqual(lines[0]["first_event_id"], "ev-ra")
        self.assertEqual(lines[0]["first_delay_ms"], 20)
        self.assertEqual(lines[1]["first_event_id"], "ev-rb")
        self.assertEqual(lines[1]["first_delay_ms"], 0)

        # Replayed identical batch is a no-op through the CLI.
        proc = run("import-facts", "--work-db", self.work,
                   "--enrich-db", self.enrich, "--batch", batch_path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn('"noop": true', proc.stdout)

        proc = run("facts-metrics", "--work-db", self.work,
                   "--plan-event", "ev-ra", "--plan-release", "R1")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("enrich_frame_query_plan", proc.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
