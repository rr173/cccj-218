"""Assertion suite for the source-range reverse index.

Run with::

    python -I tests/run_tests.py

Fixtures come from ``tools/reverse_walkthrough.py`` (hand-written segment
tables); expectations are checked against explicit literals and against the
independent per-character enumerator that lives in the same tool.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tools"))

from smpipe.errors import InvalidSourceRange, ReverseIndexError  # noqa: E402
from smpipe.pipeline import ingest_manifest  # noqa: E402
from smpipe.reverse import ReverseIndex, ReverseIndexBuilder  # noqa: E402
from smpipe.store import Store  # noqa: E402

import reverse_walkthrough as walk  # noqa: E402


def _read_table(path: str, table: str) -> list[tuple]:
    conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    try:
        return conn.execute(f"SELECT * FROM {table}").fetchall()
    finally:
        conn.close()


class ReverseIndexCase(unittest.TestCase):
    """Builds the walkthrough fixtures once and queries the index."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.mkdtemp(prefix="smpipe-rev-test-")
        cls.spec = walk.fixture_spec()
        cls.build_dir = os.path.join(cls.tmp, "build")
        walk.build_fixtures(cls.build_dir, cls.spec)
        cls.store_path = os.path.join(cls.tmp, "store.db")
        store = Store(cls.store_path)
        for release_id in cls.spec:
            ingest_manifest(
                store,
                os.path.join(cls.build_dir, release_id.lower(),
                             "manifest.json"),
            )
        store.close()  # commit + WAL checkpoint: the database is now sealed
        cls.index_path = os.path.join(cls.tmp, "reverse.idx")
        with ReverseIndexBuilder(cls.store_path, cls.index_path) as builder:
            cls.report = builder.build()
        cls.index = ReverseIndex(cls.index_path)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.index.close()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def hits(self, source: str, start: tuple, end: tuple) -> list[tuple]:
        return [
            (row["release_id"], row["artifact_id"], row["gen_line"],
             row["gen_col_start"], row["gen_col_end"],
             row["src_line"], row["src_col"])
            for row in self.index.query(source, start, end)
        ]


class TestEmojiLine(ReverseIndexCase):
    def test_exact_runs_on_entry_line(self) -> None:
        rows = self.index.query("src/orig.ts", (0, 0), (0, 11))
        emoji = [
            (r["gen_col_start"], r["gen_col_end"]) for r in rows
            if r["release_id"] == "RA" and r["artifact_id"] == "app.min.js"
            and r["gen_line"] == 0
        ]
        # Segments at cols 1 (mapped), 5 (no original), 6 (mapped); the
        # surrogate low half at col 3 is a hole, so [1,5) splits into
        # [1,3) and [4,5); the generated-only segment truncates at 5.
        self.assertEqual(emoji, [(1, 3), (4, 5), (6, 8)])

    def test_end_bound_equal_to_point_misses(self) -> None:
        self.assertEqual(self.index.query("src/orig.ts", (0, 0), (0, 10)),
                         [])

    def test_output_row_shape_and_digest(self) -> None:
        rows = self.index.query("src/orig.ts", (0, 0), (0, 11))
        first = rows[0]
        self.assertEqual(
            set(first),
            {"release_id", "artifact_id", "artifact_digest", "gen_line",
             "gen_col_start", "gen_col_end", "source_identity", "src_line",
             "src_col"},
        )
        self.assertEqual(first["source_identity"], "src/orig.ts")
        self.assertEqual((first["src_line"], first["src_col"]), (0, 10))
        self.assertEqual(len(first["artifact_digest"]), 64)
        # Rows are ordered by release, artifact, generated line, start col.
        order = [(r["release_id"], r["artifact_id"], r["gen_line"],
                  r["gen_col_start"]) for r in rows]
        self.assertEqual(order, sorted(order))


class TestRangeValidation(ReverseIndexCase):
    def test_equal_start_end_rejected(self) -> None:
        with self.assertRaises(InvalidSourceRange):
            self.index.query("src/orig.ts", (0, 1), (0, 1))

    def test_inverted_same_line_rejected(self) -> None:
        with self.assertRaises(InvalidSourceRange):
            self.index.query("src/orig.ts", (0, 5), (0, 4))

    def test_inverted_across_lines_rejected(self) -> None:
        with self.assertRaises(InvalidSourceRange):
            self.index.query("src/orig.ts", (2, 0), (1, 99))

    def test_negative_and_bool_rejected(self) -> None:
        with self.assertRaises(InvalidSourceRange):
            self.index.query("src/orig.ts", (-1, 0), (1, 0))
        with self.assertRaises(InvalidSourceRange):
            self.index.query("src/orig.ts", (True, 0), (1, 0))

    def test_empty_source_identity_rejected(self) -> None:
        with self.assertRaises(InvalidSourceRange):
            self.index.query("", (0, 0), (1, 0))

    def test_unknown_source_is_empty_not_error(self) -> None:
        self.assertEqual(self.index.query("src/ghost.ts", (0, 0), (9, 9)),
                         [])


class TestEntries(ReverseIndexCase):
    def test_entry_set_and_alias_dedup(self) -> None:
        rows = _read_table(self.index_path, "entries")
        entries = {(r[0], r[1]) for r in rows}
        self.assertEqual(entries, {
            ("RA", "app.min.js"), ("RA", "app2.min.js"),
            ("RB", "rb-bundle.js"),
            ("RC", "cyc-a"), ("RC", "cyc-b"),
            ("RD", "fanin.js"), ("RD", "long.js"),
        })
        # app.min.js has two URL aliases but appears exactly once.
        self.assertEqual(len(rows), 7)
        self.assertEqual(self.report["entries_total"], 7)

    def test_intermediate_without_alias_never_an_entry(self) -> None:
        hits = _read_table(self.index_path, "hits")
        self.assertTrue(hits)
        for row in hits:
            self.assertNotIn(row[4],  # artifact_id column
                             {"art-mid", "art-gap", "art-bad", "art-rbmid",
                              "art-cycmid", "art-shared"})


class TestCrossReleaseAndBranches(ReverseIndexCase):
    def test_same_final_point_across_releases(self) -> None:
        rows = self.index.query("src/orig.ts", (0, 0), (0, 11))
        by_release = {(r["release_id"], r["artifact_id"])
                      for r in rows}
        self.assertIn(("RA", "app.min.js"), by_release)
        self.assertIn(("RA", "app2.min.js"), by_release)
        self.assertIn(("RB", "rb-bundle.js"), by_release)

    def test_broken_branches_do_not_swallow_valid_ones(self) -> None:
        # Line 1 mixes terminal hit, hop hit, unmapped hop, invalid-map hop
        # and a generated-only segment; only the two valid branches hit.
        self.assertEqual(
            self.hits("src/orig.ts", (1, 0), (1, 5)),
            [("RA", "app.min.js", 1, 0, 2, 1, 0),
             ("RA", "app.min.js", 1, 2, 4, 1, 1)],
        )

    def test_missing_source_body_still_hits(self) -> None:
        self.assertEqual(
            self.hits("src/nowhere.ts", (3, 0), (3, 5)),
            [("RA", "app.min.js", 2, 0, 1, 3, 3)],
        )

    def test_cycle_depends_on_entry_path(self) -> None:
        rows = self.hits("src/final.ts", (0, 0), (0, 8))
        self.assertEqual(rows, [("RC", "cyc-a", 0, 0, 1, 0, 7),
                                ("RC", "cyc-b", 0, 10, 12, 0, 7)])
        # cyc-b's own first segment cycles back to cyc-b: no [0,10) run.
        self.assertEqual(self.report["hop_cycles"], 1)
        self.assertEqual(self.report["hop_cache_rejected_path"], 2)


class TestReferenceEquality(ReverseIndexCase):
    def test_index_matches_per_character_enumeration(self) -> None:
        stored = {
            (r[3], r[4], r[6], r[7], r[8], r[0], r[1], r[2])
            for r in _read_table(self.index_path, "hits")
        }
        expected = walk.reference_runs(self.spec, ("RA", "RB", "RC"))
        # RD is scale material; it is asserted literally elsewhere.
        stored_small = {row for row in stored if row[0] != "RD"}
        self.assertEqual(stored_small, expected)


class TestScaleCounters(ReverseIndexCase):
    def test_interval_not_per_character_work(self) -> None:
        report = self.report
        self.assertEqual(report["hit_rows"], 13)
        self.assertEqual(report["hop_points_computed"], 9)
        self.assertEqual(report["hop_points_reused"], 4000)
        self.assertEqual(report["generated_columns_indexed"], 1_504_020)
        # 1.5M indexed columns materialised as 13 rows: the index grows
        # with intervals and shared hop points, not with characters.
        self.assertLess(report["hit_rows"], 20)

    def test_long_line_and_fanin_runs(self) -> None:
        self.assertEqual(
            self.hits("src/big.ts", (0, 0), (0, 10)),
            [("RD", "fanin.js", 0, 0, 4000, 0, 5),
             ("RD", "long.js", 0, 0, 1000000, 0, 0),
             ("RD", "long.js", 0, 1500000, 2000000, 0, 1)],
        )


class TestQueryPlan(ReverseIndexCase):
    def test_narrow_query_uses_source_index(self) -> None:
        plan = self.index.explain_plan("src/orig.ts", (0, 0), (0, 11))
        joined = "\n".join(plan)
        self.assertIn("SEARCH hits USING INDEX idx_hits_source", joined)
        self.assertNotIn("SCAN hits", joined)


class TestReopenAndSealing(ReverseIndexCase):
    def test_query_after_build_db_moved_away(self) -> None:
        before = self.index.query("src/orig.ts", (0, 0), (0, 11))
        self.index.close()
        os.rename(self.store_path, self.store_path + ".gone")
        shutil.move(self.build_dir, self.build_dir + ".gone")
        try:
            with ReverseIndex(self.index_path) as reopened:
                after = reopened.query("src/orig.ts", (0, 0), (0, 11))
                info = reopened.info()
            self.assertEqual(after, before)
            self.assertEqual(info["state"], "complete")
            self.assertEqual(info["build_db_sha256"],
                             self.report["build_db_sha256"])
        finally:
            # Restore nothing: later tests must not depend on the build db.
            self.__class__.index = ReverseIndex(self.index_path)

    def test_unsealed_build_db_rejected(self) -> None:
        tmp = tempfile.mkdtemp(prefix="smpipe-rev-wal-")
        try:
            path = os.path.join(tmp, "wal.db")
            conn = sqlite3.connect(path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE t(x)")
            conn.commit()
            self.assertGreater(os.path.getsize(path + "-wal"), 0)
            with self.assertRaises(ReverseIndexError):
                ReverseIndexBuilder(path, os.path.join(tmp, "x.idx"))
            conn.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_not_a_build_db_rejected(self) -> None:
        tmp = tempfile.mkdtemp(prefix="smpipe-rev-empty-")
        try:
            path = os.path.join(tmp, "empty.db")
            sqlite3.connect(path).close()
            with self.assertRaises(ReverseIndexError):
                ReverseIndexBuilder(path, os.path.join(tmp, "x.idx"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_incomplete_index_rejected(self) -> None:
        tmp = tempfile.mkdtemp(prefix="smpipe-rev-inc-")
        try:
            idx = os.path.join(tmp, "half.idx")
            builder = ReverseIndexBuilder(self._sealed_copy(), idx)
            builder.close()  # build() never ran: state stays "building"
            with self.assertRaises(ReverseIndexError):
                ReverseIndex(idx)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def _sealed_copy(self) -> str:
        # The build db was moved away by an earlier test; rebuild a tiny
        # sealed one on the fly for the incomplete-index check.
        tmp = tempfile.mkdtemp(prefix="smpipe-rev-src-")
        self.addCleanup(shutil.rmtree, tmp, True)
        path = os.path.join(tmp, "mini.db")
        store = Store(path)
        store.close()
        return path


class TestBuilderRebuild(ReverseIndexCase):
    def test_rebuild_over_existing_index(self) -> None:
        tmp = tempfile.mkdtemp(prefix="smpipe-rev-rebuild-")
        self.addCleanup(shutil.rmtree, tmp, True)
        idx = os.path.join(tmp, "r.idx")
        with ReverseIndexBuilder(self._fresh_sealed(), idx) as builder:
            first = builder.build()
        with ReverseIndexBuilder(self._fresh_sealed(), idx) as builder:
            second = builder.build()
        self.assertEqual(first["hit_rows"], second["hit_rows"])
        with ReverseIndex(idx) as index:
            self.assertEqual(index.info()["state"], "complete")

    def _fresh_sealed(self) -> str:
        # A minimal sealed build db with one entry and one terminal hit.
        tmp = tempfile.mkdtemp(prefix="smpipe-rev-mini-")
        self.addCleanup(shutil.rmtree, tmp, True)
        spec = {"RX": {
            "artifacts": [{
                "id": "only.js", "aliases": ["u://only.js"], "body": "ab",
                "map": {"sources": ["s.ts"], "lines": {0: [(1, 0, 0, 0)]}},
                "bindings": {},
            }],
            "pins": {},
        }}
        walk.build_fixtures(tmp, spec)
        store_path = os.path.join(tmp, "s.db")
        store = Store(store_path)
        ingest_manifest(store, os.path.join(tmp, "rx", "manifest.json"))
        store.close()
        return store_path


if __name__ == "__main__":
    unittest.main()
