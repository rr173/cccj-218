#!/usr/bin/env python3
"""Generate the hand-written walkthrough fixtures.

The mappings in this file are authored as explicit segment tables (not
produced by the production decoder) and encoded with the project's VLQ
encoder only for storage; tests re-decode them with an independent decoder.

Layout produced under <out>/ :

  r1/  release A: two-layer map (compressed -> intermediate -> source),
       same-line generated-only segment, same URL as r2
  r2/  release B: same URL, different bytes and different map
  r3/  non-BMP generated line, UTF-16 vs code-point column units
  r4/  sectioned map: first-line column offset, later line without offset
  bad/ manifests exercising explicit rejection paths
  frames/ normalized input frames (JSONL)
"""

from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from smpipe.vlq import encode_mappings  # noqa: E402
from smpipe.store import sha256_bytes, sha256_text  # noqa: E402


def write(path: str, content: str | bytes) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = content.encode("utf-8") if isinstance(content, str) else content
    with open(path, "wb") as handle:
        handle.write(data)
    return sha256_bytes(data)


def source_map(*, sources, names, mappings, sources_content=None,
               source_root=None, sections=None, version=3):
    obj = {"version": version}
    if source_root is not None:
        obj["sourceRoot"] = source_root
    if sections is not None:
        obj["sections"] = sections
    else:
        obj["sources"] = sources
        obj["names"] = names
        obj["mappings"] = mappings
        if sources_content is not None:
            obj["sourcesContent"] = sources_content
    return json.dumps(obj, indent=2, ensure_ascii=False)


def build_release_one(root: str) -> tuple[dict, str, str, str]:
    """Two-layer pipeline: bundle1.js -> intermediate -> app.ts."""
    base = os.path.join(root, "r1")
    # Generated line is 12+ chars so columns 8 and 9 genuinely exist.
    bundle = "var x=fn(99);y=2;"
    # Hand-written compression map, single generated line:
    #   col 0 -> src/intermediate.js (0,4), name "fn"
    #   col 8 -> generated-only segment (no original position)
    compress_mappings = encode_mappings([[
        (0, 0, 0, 4, 0),
        (8,),
    ]])
    intermediate_text = "    function fn(n) { return n + 1; }"
    # Translator map on the intermediate file:
    #   col 4 -> src/app.ts (2,1), name "fn"
    # plus an earlier generated-only segment at col 0 (whitespace has no
    # original position), proving same-line nearest-segment semantics too.
    translate_mappings = encode_mappings([[
        (0,),
        (4, 0, 2, 1, 0),
    ]])
    app_ts = "\n".join([
        "// application",
        "const label = 'demo';",
        "function fn(n: number): number {",
        "  return n + 1;",
        "}",
    ])

    compress_map = source_map(
        sources=["src/intermediate.js"],
        names=["fn"],
        mappings=compress_mappings,
    )
    translate_map = source_map(
        sources=["src/app.ts"],
        names=["fn"],
        mappings=translate_mappings,
        sources_content=[app_ts],  # cross-checked against the pinned source
    )

    d_bundle = write(os.path.join(base, "dist", "bundle1.js"), bundle)
    d_cmap = write(os.path.join(base, "dist", "bundle1.js.map"), compress_map)
    d_inter = write(os.path.join(base, "build", "intermediate.js"),
                    intermediate_text)
    d_tmap = write(os.path.join(base, "build", "intermediate.js.map"),
                   translate_map)
    d_app = write(os.path.join(base, "src", "app.ts"), app_ts)

    manifest = {
        "release_id": "R1",
        "artifacts": [
            {
                "artifact_id": "art-bundle-r1",
                "sha256": d_bundle,
                "path": "dist/bundle1.js",
                "url_aliases": ["https://example.com/static/bundle.js"],
                "source_map": {
                    "path": "dist/bundle1.js.map",
                    "sha256": d_cmap,
                },
                "source_bindings": [
                    {
                        "source_identity": "src/intermediate.js",
                        "next_artifact_id": "art-intermediate-r1",
                    }
                ],
            },
            {
                "artifact_id": "art-intermediate-r1",
                "sha256": d_inter,
                "path": "build/intermediate.js",
                "source_map": {
                    "path": "build/intermediate.js.map",
                    "sha256": d_tmap,
                },
            },
        ],
        "sources": [
            {
                "source_identity": "src/app.ts",
                "sha256": d_app,
                "path": "src/app.ts",
            }
        ],
    }
    write(os.path.join(base, "manifest.json"), json.dumps(manifest, indent=2))
    return manifest, d_bundle, d_cmap, d_app


def build_release_two(root: str) -> tuple[dict, str, str]:
    """Same URL as R1 but different bytes and a different map."""
    base = os.path.join(root, "r2")
    bundle = "var z=fn(7);z++;"
    # Deliberately different from R1: only generated-only segments, so a query
    # on column 9 is unmapped.  Combined with R1's unmapped frame on the same
    # URL this proves unmapped groups never merge across releases.
    mappings = encode_mappings([[
        (0,),
        (8,),
    ]])
    other = "line0\nline1\nline2\nline3\nline4\nline5\nconst x = 1;\n"
    smap = source_map(sources=["src/other.js"], names=[], mappings=mappings)

    d_bundle = write(os.path.join(base, "dist", "bundle2.js"), bundle)
    d_map = write(os.path.join(base, "dist", "bundle2.js.map"), smap)
    d_other = write(os.path.join(base, "src", "other.js"), other)

    manifest = {
        "release_id": "R2",
        "artifacts": [
            {
                "artifact_id": "art-bundle-r2",
                "sha256": d_bundle,
                "path": "dist/bundle2.js",
                "url_aliases": ["https://example.com/static/bundle.js"],
                "source_map": {"path": "dist/bundle2.js.map", "sha256": d_map},
            }
        ],
        "sources": [
            {"source_identity": "src/other.js", "sha256": d_other,
             "path": "src/other.js"}
        ],
    }
    write(os.path.join(base, "manifest.json"), json.dumps(manifest, indent=2))
    return manifest, d_bundle, d_map


def build_release_three(root: str) -> tuple[dict, str]:
    """Non-BMP line; both column units must resolve to the same character."""
    base = os.path.join(root, "r3")
    # Code points: A B C 😀(U+1F600) E   -> UTF-16 cols 0 1 2 3-4 5
    bundle = "ABC\U0001F600E"
    # Map every character start to src/emoji.ts line 0, same code-point col.
    # Segment table (gen col, src idx, src line, src col):
    #   0 -> (0,0,0), 1 -> (0,0,1), 2 -> (0,0,2), 3 -> (0,0,3), 5 -> (0,0,4)
    mappings = encode_mappings([[
        (0, 0, 0, 0),
        (1, 0, 0, 1),
        (2, 0, 0, 2),
        (3, 0, 0, 3),
        (5, 0, 0, 4),
    ]])
    emoji_source = "// emoji source\nABC\U0001F600E\n"
    smap = source_map(
        sources=["src/emoji.ts"], names=[], mappings=mappings
    )
    d_bundle = write(os.path.join(base, "dist", "emoji.js"), bundle)
    d_map = write(os.path.join(base, "dist", "emoji.js.map"), smap)
    d_src = write(os.path.join(base, "src", "emoji.ts"), emoji_source)

    manifest = {
        "release_id": "R3",
        "artifacts": [
            {
                "artifact_id": "art-emoji",
                "sha256": d_bundle,
                "path": "dist/emoji.js",
                "url_aliases": ["https://example.com/static/emoji.js"],
                "source_map": {"path": "dist/emoji.js.map", "sha256": d_map},
            }
        ],
        "sources": [
            {"source_identity": "src/emoji.ts", "sha256": d_src,
             "path": "src/emoji.ts"}
        ],
    }
    write(os.path.join(base, "manifest.json"), json.dumps(manifest, indent=2))
    return manifest, d_bundle


def build_release_four(root: str) -> tuple[dict, str]:
    """Sectioned map: line-0 column offset, later lines without column shift."""
    base = os.path.join(root, "r4")
    # Three generated lines; the second section starts on line 3.
    bundle = "abcfunction alpha\ndef beta\ngamma tail\n"
    section_one = {
        "offset": {"line": 0, "column": 3},
        "map": {
            "version": 3,
            # Local col 0 (global col 3) -> src/sec.ts (0,0)
            # Local second line col 0 (global line 1, col 0) -> (1,0)
            "sources": ["src/sec.ts"],
            "names": ["alpha"],
            "mappings": encode_mappings([
                [(0, 0, 0, 0, 0)],
                [(0, 0, 1, 0)],
            ]),
        },
    }
    section_two = {
        "offset": {"line": 2, "column": 0},
        "map": {
            "version": 3,
            "sources": ["src/sec.ts"],
            "names": ["beta"],
            "mappings": encode_mappings([[(0, 0, 2, 0, 0)]]),
        },
    }
    smap = source_map(sources=[], names=[], mappings="",
                      sections=[section_one, section_two])
    sec_source = "alpha first\nbeta second line\nthird section line\n"
    d_bundle = write(os.path.join(base, "dist", "sec.js"), bundle)
    d_map = write(os.path.join(base, "dist", "sec.js.map"), smap)
    d_src = write(os.path.join(base, "src", "sec.ts"), sec_source)

    manifest = {
        "release_id": "R4",
        "artifacts": [
            {
                "artifact_id": "art-sec",
                "sha256": d_bundle,
                "path": "dist/sec.js",
                "url_aliases": ["https://example.com/static/sec.js"],
                "source_map": {"path": "dist/sec.js.map", "sha256": d_map},
            }
        ],
        "sources": [
            {"source_identity": "src/sec.ts", "sha256": d_src,
             "path": "src/sec.ts"}
        ],
    }
    write(os.path.join(base, "manifest.json"), json.dumps(manifest, indent=2))
    return manifest, d_bundle


def write_frames(root: str) -> str:
    frames = [
        # R1 frame 1: (1,1) -> two layers -> app.ts (3,2) externally.
        {"event_id": "E1", "frame_index": 0, "release_id": "R1",
         "url": "https://example.com/static/bundle.js",
         "line": 1, "column": 1, "column_unit": "utf16",
         "error_type": "TypeError"},
        # R1 frame 2: query column 9 must hit the generated-only segment at 8
        # and stay unmapped (no fallback to the segment at col 0).
        {"event_id": "E1", "frame_index": 1, "release_id": "R1",
         "url": "https://example.com/static/bundle.js",
         "line": 1, "column": 9, "column_unit": "utf16",
         "error_type": "TypeError"},
        # Same event, another frame landing on the SAME final position as
        # frame 0 -> counted once in the mapped group's event set.
        {"event_id": "E1", "frame_index": 2, "release_id": "R1",
         "url": "https://example.com/static/bundle.js",
         "line": 1, "column": 2, "column_unit": "utf16",
         "error_type": "TypeError"},
        # Distinct event, same final location.
        {"event_id": "E2", "frame_index": 0, "release_id": "R1",
         "url": "https://example.com/static/bundle.js",
         "line": 1, "column": 3, "column_unit": "utf16",
         "error_type": "TypeError"},
        # R2 same URL, unmapped query past the only segment's reach stays on
        # same line but differs by release/digest in grouping.
        {"event_id": "E3", "frame_index": 0, "release_id": "R2",
         "url": "https://example.com/static/bundle.js",
         "line": 1, "column": 9, "column_unit": "utf16",
         "error_type": "RangeError"},
        # R1 counterpart for cross-release unmapped isolation.
        {"event_id": "E4", "frame_index": 0, "release_id": "R1",
         "url": "https://example.com/static/bundle.js",
         "line": 1, "column": 9, "column_unit": "utf16",
         "error_type": "RangeError"},
        # R3 non-BMP: "ABC😀E" -> UTF-16 columns 0,1,2,(3 high)(4 low),5.
        # The 1-based UTF-16 column 6 and the 1-based code-point column 5
        # name the SAME character E and must map to the same final position.
        {"event_id": "E5", "frame_index": 0, "release_id": "R3",
         "url": "https://example.com/static/emoji.js",
         "line": 1, "column": 6, "column_unit": "utf16",
         "error_type": "Error"},
        {"event_id": "E5", "frame_index": 1, "release_id": "R3",
         "url": "https://example.com/static/emoji.js",
         "line": 1, "column": 5, "column_unit": "codepoint",
         "error_type": "Error"},
        # R3 surrogate-pair interior: code-point column 5 (1-based) is the
        # low surrogate half of the emoji and must be rejected.
        {"event_id": "E6", "frame_index": 0, "release_id": "R3",
         "url": "https://example.com/static/emoji.js",
         "line": 1, "column": 5, "column_unit": "utf16",
         "error_type": "Error"},
        # R4 sections: first-line column offset (global col 6 -> local col 3).
        {"event_id": "E7", "frame_index": 0, "release_id": "R4",
         "url": "https://example.com/static/sec.js",
         "line": 1, "column": 4, "column_unit": "utf16",
         "error_type": "SyntaxError"},
        # R4 later generated line: no column offset (global line 2 col 1).
        {"event_id": "E7", "frame_index": 1, "release_id": "R4",
         "url": "https://example.com/static/sec.js",
         "line": 2, "column": 1, "column_unit": "utf16",
         "error_type": "SyntaxError"},
        # R4 second section (line 3).
        {"event_id": "E7", "frame_index": 2, "release_id": "R4",
         "url": "https://example.com/static/sec.js",
         "line": 3, "column": 1, "column_unit": "utf16",
         "error_type": "SyntaxError"},
        # Unknown URL -> missing_artifact.
        {"event_id": "E8", "frame_index": 0, "release_id": "R1",
         "url": "https://example.com/static/never-heard-of.js",
         "line": 1, "column": 1, "column_unit": "utf16",
         "error_type": "Error"},
    ]
    path = os.path.join(root, "frames", "frames.jsonl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for frame in frames:
            handle.write(json.dumps(frame, ensure_ascii=False) + "\n")
    return path


def build_bad_manifests(root: str) -> dict[str, str]:
    """Small fixtures each exercising one explicit rejection."""
    out: dict[str, str] = {}
    bad = os.path.join(root, "bad")

    # 1. corrupt VLQ: '~' is not part of the base64 VLQ alphabet.
    d_body = write(os.path.join(bad, "badvlq", "a.js"), "x")
    d_bad_map = write(os.path.join(bad, "badvlq", "a.js.map"),
                      json.dumps({"version": 3, "sources": ["s.js"],
                                  "names": [], "mappings": "~AAA"}))
    write(os.path.join(bad, "badvlq", "manifest.json"), json.dumps({
        "release_id": "BAD-VLQ",
        "artifacts": [{
            "artifact_id": "a", "sha256": d_body, "path": "a.js",
            "url_aliases": ["u://a.js"],
            "source_map": {"path": "a.js.map", "sha256": d_bad_map},
        }],
    }))
    out["bad_vlq"] = os.path.join(bad, "badvlq", "manifest.json")

    # 2. external section URL must be refused, never fetched/ignored.
    d_body2 = write(os.path.join(bad, "extsection", "b.js"), "x")
    d_ext_map = write(os.path.join(bad, "extsection", "b.js.map"),
                      json.dumps({"version": 3, "sections": [
                          {"offset": {"line": 0, "column": 0},
                           "url": "https://example.com/external.map"}]}))
    write(os.path.join(bad, "extsection", "manifest.json"), json.dumps({
        "release_id": "BAD-SECTION",
        "artifacts": [{
            "artifact_id": "b", "sha256": d_body2, "path": "b.js",
            "url_aliases": ["u://b.js"],
            "source_map": {"path": "b.js.map", "sha256": d_ext_map},
        }],
    }))
    out["external_section"] = os.path.join(bad, "extsection",
                                           "manifest.json")

    # 3. binding cycle: A maps to source B (artifact B), B maps back to A.
    cycle = os.path.join(bad, "cycle")
    body_a = "aaaa"; body_b = "bbbb"
    map_a = source_map(sources=["B.js"], names=[],
                       mappings=encode_mappings([[(0, 0, 0, 0)]]))
    map_b = source_map(sources=["A.js"], names=[],
                       mappings=encode_mappings([[(0, 0, 0, 0)]]))
    da = write(os.path.join(cycle, "a.js"), body_a)
    db = write(os.path.join(cycle, "b.js"), body_b)
    dma = write(os.path.join(cycle, "a.map"), map_a)
    dmb = write(os.path.join(cycle, "b.map"), map_b)
    write(os.path.join(cycle, "manifest.json"), json.dumps({
        "release_id": "BAD-CYCLE",
        "artifacts": [
            {"artifact_id": "A", "sha256": da, "path": "a.js",
             "url_aliases": ["u://cycle.js"],
             "source_map": {"path": "a.map", "sha256": dma},
             "source_bindings": [
                 {"source_identity": "B.js", "next_artifact_id": "B"}]},
            {"artifact_id": "B", "sha256": db, "path": "b.js",
             "source_map": {"path": "b.map", "sha256": dmb},
             "source_bindings": [
                 {"source_identity": "A.js", "next_artifact_id": "A"}]},
        ],
    }))
    out["cycle"] = os.path.join(cycle, "manifest.json")

    # 4. wrong sourcesContent: the pinned source file digest is internally
    # consistent, but the map's inline sourcesContent carries different
    # bytes under the same source identity.
    mismatch = os.path.join(bad, "mismatch")
    body = "x"
    real_source = "original"
    sm = source_map(sources=["s.ts"], names=[],
                    mappings=encode_mappings([[(0, 0, 0, 0)]]),
                    sources_content=["tampered inline content"])
    d_body3 = write(os.path.join(mismatch, "c.js"), body)
    d_map3 = write(os.path.join(mismatch, "c.map"), sm)
    d_real = write(os.path.join(mismatch, "s.ts"), real_source)
    write(os.path.join(mismatch, "manifest.json"), json.dumps({
        "release_id": "BAD-DIGEST",
        "artifacts": [{
            "artifact_id": "c", "sha256": d_body3, "path": "c.js",
            "url_aliases": ["u://c.js"],
            "source_map": {"path": "c.map", "sha256": d_map3}}],
        "sources": [{"source_identity": "s.ts", "sha256": d_real,
                     "path": "s.ts"}],
    }))
    out["mismatch_manifest"] = os.path.join(mismatch, "manifest.json")
    out["mismatch_real_digest"] = d_real

    # 5. missing binding: map references an intermediate source that the
    # manifest never binds and whose body is not pinned either.
    miss = os.path.join(bad, "missbind")
    d_body4 = write(os.path.join(miss, "d.js"), "x")
    d_map4 = write(os.path.join(miss, "d.map"), source_map(
        sources=["intermediate/unbound.js"], names=[],
        mappings=encode_mappings([[(0, 0, 0, 0)]])))
    write(os.path.join(miss, "manifest.json"), json.dumps({
        "release_id": "BAD-MISS-BIND",
        "artifacts": [{
            "artifact_id": "d", "sha256": d_body4, "path": "d.js",
            "url_aliases": ["u://d.js"],
            "source_map": {"path": "d.map", "sha256": d_map4}}],
    }))
    out["missing_binding"] = os.path.join(miss, "manifest.json")

    # 6. section overlap (same offset twice).
    overlap = os.path.join(bad, "overlap")
    d_body5 = write(os.path.join(overlap, "e.js"), "x")
    d_map5 = write(os.path.join(overlap, "e.map"), json.dumps({
        "version": 3,
        "sections": [
            {"offset": {"line": 0, "column": 0}, "map": {
                "version": 3, "sources": ["s.js"], "names": [],
                "mappings": "AAAA"}},
            {"offset": {"line": 0, "column": 0}, "map": {
                "version": 3, "sources": ["s.js"], "names": [],
                "mappings": "AAAA"}},
        ],
    }))
    write(os.path.join(overlap, "manifest.json"), json.dumps({
        "release_id": "BAD-OVERLAP",
        "artifacts": [{
            "artifact_id": "e", "sha256": d_body5, "path": "e.js",
            "url_aliases": ["u://e.js"],
            "source_map": {"path": "e.map", "sha256": d_map5}}],
    }))
    out["section_overlap"] = os.path.join(overlap, "manifest.json")
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    root = os.path.abspath(args.out)
    os.makedirs(root, exist_ok=True)
    m1, d_bundle, d_cmap, d_app = build_release_one(root)
    m2, d_bundle2, d_map2 = build_release_two(root)
    m3, d_bundle3 = build_release_three(root)
    m4, d_bundle4 = build_release_four(root)
    frames_path = write_frames(root)
    bad = build_bad_manifests(root)
    summary = {
        "root": root,
        "manifests": {
            "R1": os.path.join(root, "r1", "manifest.json"),
            "R2": os.path.join(root, "r2", "manifest.json"),
            "R3": os.path.join(root, "r3", "manifest.json"),
            "R4": os.path.join(root, "r4", "manifest.json"),
        },
        "bad": bad,
        "frames": frames_path,
        "digests": {
            "r1_bundle": d_bundle, "r1_compress_map": d_cmap,
            "r1_app_ts": d_app, "r2_bundle": d_bundle2,
            "r2_map": d_map2, "r3_bundle": d_bundle3,
            "r4_bundle": d_bundle4,
        },
    }
    with open(os.path.join(root, "fixtures.json"), "w",
              encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
