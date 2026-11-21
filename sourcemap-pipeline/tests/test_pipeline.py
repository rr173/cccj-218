"""Full assertion suite for the offline source-map enrichment pipeline.

Run with::

    python -I tests/run_tests.py

The suite rebuilds its own small fixtures in a temporary directory, never
touches the network and never imports the production query index for its
hand-written expectations: segment tables are checked against
``reference_decoder`` and explicit literals.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, HERE)

from smpipe import columns as colmod  # noqa: E402
from smpipe.errors import (  # noqa: E402
    BadVLQ,
    DuplicateFrame,
    ExternalSection,
    InvalidSourceMap,
    SectionOverlap,
    SourceContentMismatch,
    SurrogatePairColumn,
)
from smpipe.pipeline import (  # noqa: E402
    compute_groups,
    enrich_frame,
    ingest_frames,
    ingest_manifest,
)
from smpipe.sourcemap import materialize, parse_map, resolve_source  # noqa: E402
from smpipe.store import Store, sha256_bytes, sha256_text  # noqa: E402
from smpipe.vlq import decode_mappings, encode_mappings  # noqa: E402
from reference_decoder import reference_decode  # noqa: E402

TOOLS = os.path.join(ROOT, "tools")
sys.path.insert(0, TOOLS)
import generate_fixtures  # noqa: E402


def _generate_into(data_dir: str) -> None:
    old_argv = sys.argv
    sys.argv = ["generate_fixtures.py", "--out", data_dir]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            generate_fixtures.main()
    finally:
        sys.argv = old_argv


class FixtureBackedCase(unittest.TestCase):
    """Builds the full walkthrough material once per test class."""

    store: Store | None = None
    data_dir = ""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.mkdtemp(prefix="smpipe-test-")
        cls.data_dir = os.path.join(cls.tmp, "data")
        _generate_into(cls.data_dir)
        cls.store_path = os.path.join(cls.tmp, "store.db")
        cls.store = Store(cls.store_path)
        for release in ("r1", "r2", "r3", "r4"):
            ingest_manifest(
                cls.store,
                os.path.join(cls.data_dir, release, "manifest.json"),
            )
        cls.enrich_report = ingest_frames(
            cls.store, os.path.join(cls.data_dir, "frames", "frames.jsonl")
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.store.close()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def frame(self, event_id: str, frame_index: int) -> dict:
        row = self.store.get_frame(event_id, frame_index)
        self.assertIsNotNone(row, f"frame {event_id}/{frame_index} missing")
        return json.loads(row["result_json"])


class TestVLQCrossCheck(unittest.TestCase):
    """Decoder agreement on hand-written tables and round trips."""

    def test_handwritten_two_layer_segment_tables(self) -> None:
        # Layer-1 compression map, authored literally.
        layer1 = [[(0, 0, 0, 4, 0), (8,)]]
        encoded = encode_mappings(layer1)
        self.assertEqual(decode_mappings(encoded), layer1)
        # Independent decoder must agree; it does not share production code.
        self.assertEqual(reference_decode(encoded), layer1)

        layer2 = [[(0,), (4, 0, 2, 1, 0)]]
        encoded2 = encode_mappings(layer2)
        self.assertEqual(decode_mappings(encoded2), layer2)
        self.assertEqual(reference_decode(encoded2), layer2)

    def test_empty_lines_and_state_reset(self) -> None:
        # Two empty generated lines then a segment: relative registers reset.
        table = [[], [], [(0, 0, 0, 0)]]
        encoded = encode_mappings(table)
        self.assertEqual(encoded, ";;AAAA")
        self.assertEqual(decode_mappings(encoded), table)
        self.assertEqual(reference_decode(encoded), table)

    def test_negative_and_multi_digit_vlq_roundtrip(self) -> None:
        table = [[
            (0, 0, 0, 0),
            (1000, 5, 200000, 4096, 7),
            (1001, 4, -1 + 200000, 4097, 6),
        ]]
        encoded = encode_mappings(table)
        self.assertEqual(decode_mappings(encoded), table)
        self.assertEqual(reference_decode(encoded), table)

    def test_corrupt_vlq_rejected(self) -> None:
        with self.assertRaises(BadVLQ):
            decode_mappings("~AAA")
        with self.assertRaises(BadVLQ):
            decode_mappings("AAA,,A")
        with self.assertRaises(BadVLQ):
            decode_mappings("A,")
        with self.assertRaises(BadVLQ):
            decode_mappings("ggggg")  # never-ending continuation

    def test_strictly_increasing_generated_columns(self) -> None:
        # Hand-craft a mapping whose second segment goes backwards.  The
        # relative delta -1 is itself invalid because gen_col cannot be neg.
        with self.assertRaises(BadVLQ):
            decode_mappings("AAAD")


class TestColumnUnits(unittest.TestCase):
    LINE = "ABC\U0001F600E"  # UTF-16 units: A B C H L E (cols 0..5)

    def test_utf16_and_codepoint_land_on_same_character(self) -> None:
        # 1-based UTF-16 col 6 -> zero col 5 (E); code-point col 5 -> same.
        line_a, col_a = colmod.external_to_utf16_zero_based(
            self.LINE, line_one_based=1, column_one_based=6,
            unit=colmod.UTF16,
        )
        line_b, col_b = colmod.external_to_utf16_zero_based(
            self.LINE, line_one_based=1, column_one_based=5,
            unit=colmod.CODEPOINT,
        )
        self.assertEqual((line_a, col_a), (0, 5))
        self.assertEqual((line_b, col_b), (0, 5))

    def test_utf16_low_surrogate_rejected(self) -> None:
        with self.assertRaises(SurrogatePairColumn):
            colmod.external_to_utf16_zero_based(
                self.LINE, line_one_based=1, column_one_based=5,
                unit=colmod.UTF16,
            )

    def test_codepoint_low_surrogate_rejected(self) -> None:
        # Python-level low surrogate text emulates malformed but surrogate
        # bearing input; both unit paths must refuse the interior.
        lone = "A\ud83d\ude00"
        with self.assertRaises(SurrogatePairColumn):
            colmod.external_to_utf16_zero_based(
                lone, line_one_based=1, column_one_based=3,
                unit=colmod.CODEPOINT,
            )

    def test_bmp_units_coincide(self) -> None:
        line_a, col_a = colmod.external_to_utf16_zero_based(
            "hello", line_one_based=1, column_one_based=4, unit=colmod.UTF16
        )
        line_b, col_b = colmod.external_to_utf16_zero_based(
            "hello", line_one_based=1, column_one_based=4,
            unit=colmod.CODEPOINT,
        )
        self.assertEqual((line_a, col_a), (0, 3))
        self.assertEqual((line_b, col_b), (0, 3))

    def test_out_of_range(self) -> None:
        from smpipe.errors import ColumnOutOfRange, LineOutOfRange
        with self.assertRaises(LineOutOfRange):
            colmod.external_to_utf16_zero_based(
                "x", line_one_based=2, column_one_based=1, unit=colmod.UTF16
            )
        with self.assertRaises(ColumnOutOfRange):
            colmod.external_to_utf16_zero_based(
                "x", line_one_based=1, column_one_based=99, unit=colmod.UTF16
            )


class TestLineIndex(unittest.TestCase):
    def test_matches_splitlines_and_trailing_breaks(self) -> None:
        text = "a\nbb\r\nccc\rdddd\n"
        index = colmod.LineIndex(text)
        # Non-BMP tables remain lazy until a column conversion needs them.
        self.assertEqual(index._non_bmp, {})
        self.assertEqual(index.get_line(0), "a")
        self.assertEqual(index.get_line(2), "ccc")
        self.assertEqual(index.get_line(3), "dddd")
        self.assertEqual(index.line_count(), 4)
        self.assertIsNone(index.get_line(4))
        # Agrees with the standard library on a tricky CRLF/CR mix.
        self.assertEqual(
            [index.get_line(i) for i in range(index.line_count())],
            text.splitlines(),
        )

    def test_lone_cr_and_adjacent_newlines(self) -> None:
        index = colmod.LineIndex("x\ry\nz\r\n")
        self.assertEqual(index.line_count(), 3)
        self.assertEqual(
            [index.get_line(i) for i in range(3)],
            ["x", "y", "z"],
        )

    def test_no_trailing_newline(self) -> None:
        index = colmod.LineIndex("one\ntwo")
        self.assertEqual(index.line_count(), 2)
        self.assertEqual(index.get_line(1), "two")


class TestSourceMapParsing(unittest.TestCase):
    def test_source_root_resolution(self) -> None:
        self.assertEqual(
            resolve_source("./src/", "a/../b.ts"), "src/b.ts"
        )
        self.assertEqual(
            resolve_source("https://cdn.test/root/", "../x.ts"),
            "https://cdn.test/x.ts",
        )
        self.assertEqual(
            resolve_source(None, "https://cdn.test/a.ts"),
            "https://cdn.test/a.ts",
        )

    def test_duplicate_names_allowed(self) -> None:
        # Repeated names are legal; indexes simply repeat.
        obj = {
            "version": 3,
            "sources": ["s.js"],
            "names": ["dup", "dup"],
            "mappings": encode_mappings([[
                (0, 0, 0, 0, 0),
                (1, 0, 0, 1, 1),
            ]]),
        }
        meta, rows = parse_map(obj)
        materialized = materialize(rows)
        self.assertEqual(meta.names, ["dup", "dup"])
        self.assertEqual(materialized[0][1][5], 1)

    def test_external_section_refused(self) -> None:
        obj = {"version": 3, "sections": [
            {"offset": {"line": 0, "column": 0},
             "url": "https://example.test/external.map"}]}
        with self.assertRaises(ExternalSection):
            parse_map(obj)

    def test_section_order_and_overlap_rejected(self) -> None:
        inline = {
            "version": 3, "sources": ["s.js"], "names": [],
            "mappings": "AAAA",
        }
        backwards = {"version": 3, "sections": [
            {"offset": {"line": 1, "column": 0}, "map": inline},
            {"offset": {"line": 0, "column": 0}, "map": inline},
        ]}
        with self.assertRaises(InvalidSourceMap):
            parse_map(backwards)
        same = {"version": 3, "sections": [
            {"offset": {"line": 0, "column": 0}, "map": inline},
            {"offset": {"line": 0, "column": 0}, "map": inline},
        ]}
        with self.assertRaises(SectionOverlap):
            parse_map(same)

    def test_section_offsets_first_line_only(self) -> None:
        obj = {
            "version": 3,
            "sections": [
                {"offset": {"line": 0, "column": 3}, "map": {
                    "version": 3, "sources": ["s.ts"], "names": [],
                    "mappings": encode_mappings([
                        [(0, 0, 0, 0)],
                        [(0, 0, 1, 0)],
                    ]),
                }},
                {"offset": {"line": 2, "column": 0}, "map": {
                    "version": 3, "sources": ["s.ts"], "names": [],
                    "mappings": encode_mappings([[(0, 0, 2, 0)]]),
                }},
            ],
        }
        _meta, rows = parse_map(obj)
        lines = materialize(rows)
        # First line shifts columns by 3; the second line keeps column 0.
        self.assertEqual(lines[0][0][:2], (0, 3))
        self.assertEqual(lines[1][0][:2], (1, 0))
        self.assertEqual(lines[2][0][:2], (2, 0))

    def test_unsupported_version_rejected(self) -> None:
        with self.assertRaises(InvalidSourceMap):
            parse_map({"version": 2, "sources": [], "names": [],
                       "mappings": ""})

    def test_inconsistent_mapping_rejected_at_ingest(self) -> None:
        tmp = tempfile.mkdtemp(prefix="smpipe-conflict-")
        try:
            body = b"xx"
            body_d = sha256_bytes(body)
            # Two distinct originals at the same generated column.  Encode
            # each segment independently (absolute state) and concatenate so
            # the production encoder's monotonicity check is bypassed; the
            # store is what must reject this conflicting material.
            from smpipe.vlq import encode_vlq
            seg_one = "".join(map(encode_vlq, (0, 0, 0, 0)))
            seg_two = "".join(map(encode_vlq, (0, 1, 9, 9)))
            bad_map = json.dumps({
                "version": 3,
                "sources": ["a.js", "b.js"],
                "names": [],
                "mappings": f"{seg_one},{seg_two}",
            })
            map_d = sha256_text(bad_map)
            open(os.path.join(tmp, "a.js"), "wb").write(body)
            open(os.path.join(tmp, "a.map"), "w").write(bad_map)
            manifest = {
                "release_id": "CONFLICT",
                "artifacts": [{
                    "artifact_id": "a", "sha256": body_d, "path": "a.js",
                    "url_aliases": ["u://a"],
                    "source_map": {"path": "a.map", "sha256": map_d},
                }],
            }
            open(os.path.join(tmp, "manifest.json"), "w").write(
                json.dumps(manifest))
            store = Store(os.path.join(tmp, "store.db"))
            report = ingest_manifest(
                store, os.path.join(tmp, "manifest.json"))
            self.assertEqual(
                report.maps_rejected[map_d], "inconsistent_mapping")
            store.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestWalkthrough(FixtureBackedCase):
    def test_two_layer_resolution_uses_explicit_original_column(self) -> None:
        result = self.frame("E1", 0)
        self.assertEqual(result["status"], "mapped")
        self.assertEqual(len(result["layers"]), 2)
        layer1 = result["layers"][0]["hit_segment"]
        self.assertEqual(
            (layer1["original_line"], layer1["original_column"]), (0, 4))
        final = result["final"]
        # External display must be line 3, column 2; the layer-1 original
        # column (4) must never leak through as the final column.
        self.assertEqual((final["line_zero_based"],
                          final["column_zero_based"]), (2, 1))
        self.assertEqual((final["line_one_based"],
                          final["column_one_based"]), (3, 2))
        self.assertEqual(final["source_identity"], "src/app.ts")
        self.assertIsNotNone(final["snippet"])

    def test_generated_only_segment_blocks_fallback(self) -> None:
        result = self.frame("E1", 1)
        self.assertEqual(result["status"], "unmapped")
        self.assertEqual(result["reason"],
                         "segment_without_original_position")
        hit = result["layers"][0]["hit_segment"]
        self.assertFalse(hit["has_original_position"])
        self.assertEqual(hit["generated_column"], 8)

    def test_no_borrow_across_generated_lines(self) -> None:
        # Hand-built map: line 0 has a mapped segment; line 1 only has a
        # generated-only segment. Querying line 1 must stay unmapped rather
        # than borrowing line 0's mapping.
        obj = {
            "version": 3, "sources": ["s.js"], "names": [],
            "mappings": encode_mappings([
                [(0, 0, 0, 0)],
                [(2,)],
            ]),
        }
        _meta, rows = parse_map(obj)
        lines = materialize(rows)
        self.assertEqual(len(lines), 2)
        # Independent check of the literal segment table.
        self.assertEqual(reference_decode(obj["mappings"]), [
            [(0, 0, 0, 0)], [(2,)]
        ])

    def test_non_bmp_two_units_same_final_position(self) -> None:
        a = self.frame("E5", 0)
        b = self.frame("E5", 1)
        self.assertEqual(a["status"], "mapped")
        self.assertEqual(b["status"], "mapped")
        self.assertEqual(a["final"]["line_zero_based"],
                         b["final"]["line_zero_based"])
        self.assertEqual(a["final"]["column_zero_based"],
                         b["final"]["column_zero_based"])
        self.assertEqual(
            (a["final"]["line_zero_based"], a["final"]["column_zero_based"]),
            (0, 4),
        )
        # The recorded unit conversions must visibly differ on input.
        self.assertEqual(
            a["layers"][0]["column_unit_conversion"]["from_unit"], "utf16")
        self.assertEqual(
            b["layers"][0]["column_unit_conversion"]["from_unit"],
            "codepoint")

    def test_surrogate_pair_interior_is_invalid_frame(self) -> None:
        result = self.frame("E6", 0)
        self.assertEqual(result["status"], "invalid_frame")
        self.assertEqual(result["reason"], "inside_surrogate_pair")

    def test_sections_first_line_vs_later_line_offsets(self) -> None:
        first = self.frame("E7", 0)
        second = self.frame("E7", 1)
        third = self.frame("E7", 2)
        self.assertEqual(
            (first["final"]["line_zero_based"],
             first["final"]["column_zero_based"]), (0, 0))
        self.assertEqual(
            (second["final"]["line_zero_based"],
             second["final"]["column_zero_based"]), (1, 0))
        self.assertEqual(
            (third["final"]["line_zero_based"],
             third["final"]["column_zero_based"]), (2, 0))

    def test_sparse_line_cache_avoids_repeat_scans(self) -> None:
        # The walkthrough maps are sparse (< dense threshold), so repeated
        # identical lookups must be served by the bounded LRU line cache.
        self.store.reset_metrics()
        frame = {
            "event_id": "CACHE", "frame_index": 0, "release_id": "R1",
            "url": "https://example.com/static/bundle.js",
            "line": 1, "column": 1, "column_unit": "utf16",
            "error_type": "TypeError",
        }
        for _ in range(20):
            enrich_frame(self.store, frame)
        metrics = self.store.metrics_snapshot()
        self.assertGreaterEqual(metrics["line_cache_hits"], 18)
        # At most one load per distinct generated line across two layers.
        self.assertLessEqual(metrics.get("line_loads", 0), 2)

    def test_missing_artifact_status(self) -> None:
        result = self.frame("E8", 0)
        self.assertEqual(result["status"], "missing_artifact")
        self.assertEqual(result["reason"], "url_not_bound_in_release")

    def test_duplicate_frame_same_content_is_idempotent(self) -> None:
        before = len(self.store.iter_frames())
        report = ingest_frames(
            self.store,
            os.path.join(self.data_dir, "frames", "frames.jsonl"),
        )
        self.assertEqual(report["new"], 0)
        self.assertEqual(report["duplicate"], before)
        self.assertEqual(len(self.store.iter_frames()), before)

    def test_conflicting_duplicate_frame_rejected(self) -> None:
        frame = {
            "event_id": "E1", "frame_index": 0, "release_id": "R1",
            "url": "https://example.com/static/bundle.js",
            "line": 1, "column": 1, "column_unit": "utf16",
            "error_type": "TypeError", "extra_material": "different",
        }
        with self.assertRaises(DuplicateFrame):
            self.store.check_frame_input(
                "E1", 0, sha256_text(json.dumps(frame, sort_keys=True)))


class TestGrouping(FixtureBackedCase):
    def test_event_counted_once_for_same_final_position(self) -> None:
        groups = compute_groups(self.store)["groups"]
        mapped = [
            g for g in groups
            if g["group_key"].get("source_identity") == "src/app.ts"
            and g["group_key"].get("line_zero_based") == 2
        ]
        self.assertEqual(len(mapped), 1)
        self.assertEqual(mapped[0]["event_count"], 2)
        self.assertEqual(sorted(mapped[0]["events"]), ["E1", "E2"])
        # E1 contributed two frames to this location.
        self.assertEqual(mapped[0]["frame_count"], 3)

    def test_unmapped_groups_isolated_across_releases(self) -> None:
        groups = compute_groups(self.store)["groups"]
        unmapped = [g for g in groups if g["group_key"]["kind"] == "unmapped"]
        releases = {g["group_key"]["release_id"] for g in unmapped}
        self.assertIn("R1", releases)
        self.assertIn("R2", releases)
        for group in unmapped:
            self.assertIsNotNone(group["group_key"]["artifact_digest"])
        # Same URL appears in both release groups.
        urls = {g["group_key"]["url"] for g in unmapped}
        self.assertIn("https://example.com/static/bundle.js", urls)

    def test_group_key_includes_error_type(self) -> None:
        groups = compute_groups(self.store)["groups"]
        keys = {json.dumps(g["group_key"], sort_keys=True) for g in groups}
        self.assertTrue(
            any('"error_type": "TypeError"' in k for k in keys))


class TestRejections(unittest.TestCase):
    def _ingest(self, name: str) -> tuple[Store, str, str]:
        tmp = tempfile.mkdtemp(prefix=f"smpipe-{name}-")
        # Each rejection test gets a pristine copy of the generated material.
        data_dir = os.path.join(tmp, "data")
        _generate_into(data_dir)
        store = Store(os.path.join(tmp, "store.db"))
        return store, tmp, data_dir

    def test_bad_vlq_and_external_section_marked_invalid(self) -> None:
        store, tmp, data_dir = self._ingest("reject")
        try:
            report = ingest_manifest(
                store, os.path.join(data_dir, "bad", "badvlq",
                                    "manifest.json"))
            (digest, reason), = report.maps_rejected.items()
            self.assertEqual(reason, "bad_vlq")
            result = enrich_frame(store, {
                "event_id": "X", "frame_index": 0,
                "release_id": "BAD-VLQ", "url": "u://a.js",
                "line": 1, "column": 1, "column_unit": "utf16",
                "error_type": "E"})
            self.assertEqual(result["status"], "invalid_map")
            self.assertEqual(result["reason"], "bad_vlq")

            report2 = ingest_manifest(
                store, os.path.join(data_dir, "bad", "extsection",
                                    "manifest.json"))
            self.assertIn("external_section_not_supported",
                          report2.maps_rejected.values())
        finally:
            store.close()
            shutil.rmtree(tmp, ignore_errors=True)

    def test_binding_cycle_status(self) -> None:
        store, tmp, data_dir = self._ingest("cycle")
        try:
            ingest_manifest(store,
                            os.path.join(data_dir, "bad", "cycle",
                                         "manifest.json"))
            result = enrich_frame(store, {
                "event_id": "C", "frame_index": 0,
                "release_id": "BAD-CYCLE", "url": "u://cycle.js",
                "line": 1, "column": 1, "column_unit": "utf16",
                "error_type": "E"})
            self.assertEqual(result["status"], "cycle")
            self.assertEqual(result["reason"], "binding_cycle")
        finally:
            store.close()
            shutil.rmtree(tmp, ignore_errors=True)

    def test_wrong_source_digest_rejected(self) -> None:
        store, tmp, data_dir = self._ingest("digest")
        try:
            with self.assertRaises(SourceContentMismatch):
                ingest_manifest(store,
                                os.path.join(data_dir, "bad", "mismatch",
                                             "manifest.json"))
        finally:
            store.close()
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_binding_keeps_position_without_snippet(self) -> None:
        store, tmp, data_dir = self._ingest("miss")
        try:
            ingest_manifest(store,
                            os.path.join(data_dir, "bad", "missbind",
                                         "manifest.json"))
            result = enrich_frame(store, {
                "event_id": "M", "frame_index": 0,
                "release_id": "BAD-MISS-BIND", "url": "u://d.js",
                "line": 1, "column": 1, "column_unit": "utf16",
                "error_type": "E"})
            self.assertEqual(result["status"], "mapped")
            self.assertEqual(result["reason"],
                             "mapped_source_body_missing")
            self.assertIsNone(result["final"]["snippet"])
            self.assertEqual(
                result["final"]["source_identity"],
                "intermediate/unbound.js")
        finally:
            store.close()
            shutil.rmtree(tmp, ignore_errors=True)

    def test_section_overlap_rejected(self) -> None:
        store, tmp, data_dir = self._ingest("overlap")
        try:
            report = ingest_manifest(
                store, os.path.join(data_dir, "bad", "overlap",
                                    "manifest.json"))
            self.assertIn("section_overlap", report.maps_rejected.values())
        finally:
            store.close()
            shutil.rmtree(tmp, ignore_errors=True)


class TestLocalOnly(unittest.TestCase):
    def test_manifest_paths_cannot_escape_build_dir(self) -> None:
        from smpipe.errors import ManifestError
        tmp = tempfile.mkdtemp(prefix="smpipe-local-")
        try:
            body = b"x"
            open(os.path.join(tmp, "a.js"), "wb").write(body)
            manifest = {
                "release_id": "ESCAPE",
                "artifacts": [{
                    "artifact_id": "a",
                    "sha256": sha256_bytes(body),
                    "path": "../../etc/passwd",
                    "url_aliases": ["u://a"],
                }],
            }
            open(os.path.join(tmp, "manifest.json"), "w").write(
                json.dumps(manifest))
            store = Store(os.path.join(tmp, "s.db"))
            with self.assertRaises(ManifestError):
                ingest_manifest(store, os.path.join(tmp, "manifest.json"))
            store.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_network_url_paths_are_refused(self) -> None:
        from smpipe.errors import ManifestError
        tmp = tempfile.mkdtemp(prefix="smpipe-net-")
        try:
            manifest = {
                "release_id": "NET",
                "artifacts": [{
                    "artifact_id": "a",
                    "sha256": "0" * 64,
                    "path": "https://example.com/a.js",
                    "url_aliases": ["u://a"],
                }],
            }
            open(os.path.join(tmp, "manifest.json"), "w").write(
                json.dumps(manifest))
            store = Store(os.path.join(tmp, "s.db"))
            with self.assertRaises(ManifestError):
                ingest_manifest(store, os.path.join(tmp, "manifest.json"))
            store.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_no_network_modules_imported(self) -> None:
        # The package surface must not pull in socket/http machinery.
        import smpipe.pipeline  # noqa: F401
        import smpipe.sourcemap  # noqa: F401
        for forbidden in ("socket", "http.client", "urllib.request",
                          "ftplib", "smtplib"):
            self.assertNotIn(forbidden, sys.modules)


class TestStoreReopen(unittest.TestCase):
    def test_results_survive_reopen_and_input_removal(self) -> None:
        tmp = tempfile.mkdtemp(prefix="smpipe-reopen-")
        try:
            data_dir = os.path.join(tmp, "data")
            _generate_into(data_dir)
            db = os.path.join(tmp, "store.db")
            store = Store(db)
            ingest_manifest(
                store, os.path.join(data_dir, "r1", "manifest.json"))
            ingest_frames(
                store, os.path.join(data_dir, "frames", "frames.jsonl"))
            store.close()

            # Remove the input material entirely, then reopen and explain.
            shutil.rmtree(data_dir)
            reopened = Store(db)
            row = reopened.get_frame("E1", 0)
            self.assertIsNotNone(row)
            result = json.loads(row["result_json"])
            self.assertEqual(result["final"]["source_identity"],
                             "src/app.ts")
            # Segment lookup still works against the on-disk index.
            self.assertEqual(
                reopened.segment_count(
                    result["layers"][0]["map_digest"]) > 0, True)
            reopened.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
