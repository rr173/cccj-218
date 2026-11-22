"""Re-openable reverse index: final source range -> generated ranges.

``enrich`` answers the forward question (one known generated coordinate ->
final source position).  This module answers the reverse question for a
whole sealed build: given an exact ``source_identity`` and a half-open
source range ``[(start_line, start_col), (end_line, end_col))`` in zero-based
UTF-16 coordinates (coordinate pairs compared in order, ranges may span
lines), find every generated half-open column range, in every published
entry artifact, whose characters resolve to a final source point inside the
range.  Existing error groups are *not* consulted: coverage comes from the
whole build material, so positions that never errored are still found.

Two databases are involved:

* a **sealed build database** produced by ``ingest-build`` (the ``frames``
  table is not needed and the original build directory is never read).  It
  is opened read-only/immutable and only index-backed reads are issued;
* a **reverse index database** (owned by this module) that materialises the
  hits.  Once built it is self-contained: queries need only this one file.

Build-time rules (mirroring :func:`smpipe.pipeline.enrich_frame` semantics):

* entries are exactly the ``(release_id, artifact_id)`` pairs that own at
  least one registered URL alias; multiple aliases count once, and
  intermediate artifacts without an alias are never listed as entries (they
  are still traversed as hop targets);
* on an entry generated line, adjacent stored segments delimit coverage:
  columns before the first segment belong to nobody, and a generated-only
  segment truncates the previous segment's coverage.  Only real character
  starts are indexed -- never a column past the line end, never the low
  surrogate half of an astral character;
* every column covered by one segment resolves through the segment's
  *explicit* original coordinate; deeper layers are looked up at that fixed
  coordinate only (no entry column delta is ever added);
* coverage is computed as column intervals, never per character: consecutive
  valid columns sharing one final source point merge into one output range,
  and ranges never merge across surrogate holes or across different source
  points;
* hop resolutions are memoised per ``(release, artifact, map, line, col)``
  so many entry segments converging on one intermediate coordinate cost one
  resolution.  A memo entry carries the set of ``(artifact, map)``
  signatures its chain visited plus the layers it consumed; it is reused
  only when that set is disjoint from the current path's visited set and the
  remaining layer budget suffices.  Cycle and layer-limit outcomes are never
  cached, so an entry-path-dependent cycle can never be reused as a
  universal result;
* broken chains, unmapped branches, invalid maps and cycles produce no hits
  for their own columns only -- they never swallow other branches of the
  same artifact, and a missing final source body does not affect hits.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from .columns import LineIndex
from .errors import InvalidSourceRange, ReverseIndexError
from .pipeline import MAX_LAYERS
from .store import NO_ORIGINAL

SCHEMA_VERSION = 1
WAL_SUFFIX = "-wal"
REQUIRED_TABLES = (
    "artifacts", "url_aliases", "maps", "map_sources", "bindings",
    "segments", "blobs",
)
INDEX_TABLES = ("rev_meta", "entries", "hits")

_COUNTER_KEYS = (
    "entries_total",
    "entries_skipped_no_map",
    "entries_skipped_invalid_map",
    "entries_skipped_no_body",
    "entry_lines_scanned",
    "entry_segments_mapped",
    "entry_segments_generated_only",
    "terminal_at_entry",
    "hop_points_computed",
    "hop_points_reused",
    "hop_cache_rejected_path",
    "hop_cycles",
    "hop_layer_limits",
    "hop_unmapped",
    "hop_invalid_map",
    "hop_missing_artifact",
    "generated_columns_indexed",
    "hit_rows",
)


# --------------------------------------------------------------------------
# Sealed database handling
# --------------------------------------------------------------------------
def _hash_file(path: str) -> str:
    digest = hashlib.sha256()
    try:
        handle = open(path, "rb")
    except OSError as exc:
        raise ReverseIndexError(f"database not found: {path}") from exc
    with handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _open_sealed_readonly(path: str, what: str) -> sqlite3.Connection:
    """Open a sealed SQLite database strictly read-only (immutable).

    A non-empty ``-wal`` sibling means the producer never checkpointed, so
    the file is not safely sealed and is rejected instead of silently
    reading a partial view.
    """
    if not os.path.isfile(path):
        raise ReverseIndexError(f"{what} not found: {path}")
    wal = path + WAL_SUFFIX
    if os.path.exists(wal) and os.path.getsize(wal) > 0:
        raise ReverseIndexError(
            f"{what} {path!r} still has a non-empty WAL; checkpoint and "
            "seal it before use"
        )
    try:
        conn = sqlite3.connect(
            f"file:{os.path.abspath(path)}?immutable=1",
            uri=True, check_same_thread=False,
        )
    except sqlite3.DatabaseError as exc:
        raise ReverseIndexError(f"cannot open {path!r}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    return conn


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


# --------------------------------------------------------------------------
# Read-only view of the sealed build database
# --------------------------------------------------------------------------
class _BuildDB:
    """Index-backed point/range reads against a sealed build database."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.sha256 = _hash_file(path)
        self._conn = _open_sealed_readonly(path, "build database")
        missing = [t for t in REQUIRED_TABLES
                   if t not in _table_names(self._conn)]
        if missing:
            raise ReverseIndexError(
                f"{path!r} is not a smpipe build database; missing tables: "
                + ", ".join(missing)
            )
        self._sources_cache: dict[str, dict[int, str]] = {}
        self._bindings_cache: dict[tuple[str, str], dict[str, tuple]] = {}
        self._artifact_cache: dict[tuple[str, str], sqlite3.Row | None] = {}
        self._segment_lines_cache: dict[str, list[int]] = {}

    def close(self) -> None:
        self._conn.close()

    def entries(self) -> list[sqlite3.Row]:
        """One row per (release, artifact) owning at least one URL alias.

        Multiple aliases of the same artifact collapse to a single entry;
        artifacts without any alias (intermediates) are not entries.
        """
        return self._conn.execute(
            "SELECT a.release_id, a.artifact_id, a.sha256, a.content_digest, "
            "a.map_digest FROM artifacts a "
            "WHERE EXISTS (SELECT 1 FROM url_aliases u "
            "              WHERE u.release_id = a.release_id "
            "              AND u.artifact_id = a.artifact_id) "
            "ORDER BY a.release_id, a.artifact_id"
        ).fetchall()

    def artifact(self, release_id: str, artifact_id: str) -> sqlite3.Row | None:
        key = (release_id, artifact_id)
        if key not in self._artifact_cache:
            self._artifact_cache[key] = self._conn.execute(
                "SELECT * FROM artifacts WHERE release_id=? AND artifact_id=?",
                key,
            ).fetchone()
        return self._artifact_cache[key]

    def artifact_text(self, content_digest: str) -> str | None:
        row = self._conn.execute(
            "SELECT content FROM blobs WHERE digest=?", (content_digest,)
        ).fetchone()
        if row is None:
            return None
        return bytes(row["content"]).decode("utf-8")

    def map_meta(self, map_digest: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM maps WHERE map_digest=?", (map_digest,)
        ).fetchone()

    def map_sources(self, map_digest: str) -> dict[int, str]:
        cached = self._sources_cache.get(map_digest)
        if cached is None:
            cached = {
                int(r["source_index"]): r["source_identity"]
                for r in self._conn.execute(
                    "SELECT source_index, source_identity FROM map_sources "
                    "WHERE map_digest=?",
                    (map_digest,),
                )
            }
            self._sources_cache[map_digest] = cached
        return cached

    def binding(
        self, release_id: str, map_digest: str, identity: str
    ) -> tuple[str, str | None] | None:
        key = (release_id, map_digest)
        table = self._bindings_cache.get(key)
        if table is None:
            table = {
                r["source_identity"]: (r["next_artifact_id"],
                                       r["next_map_digest"])
                for r in self._conn.execute(
                    "SELECT source_identity, next_artifact_id, "
                    "next_map_digest FROM bindings "
                    "WHERE release_id=? AND map_digest=?",
                    key,
                )
            }
            self._bindings_cache[key] = table
        return table.get(identity)

    def segment_lines(self, map_digest: str) -> list[int]:
        cached = self._segment_lines_cache.get(map_digest)
        if cached is None:
            cached = [
                int(r["gen_line"])
                for r in self._conn.execute(
                    "SELECT DISTINCT gen_line FROM segments "
                    "WHERE map_digest=? ORDER BY gen_line",
                    (map_digest,),
                )
            ]
            self._segment_lines_cache[map_digest] = cached
        return cached

    def line_segments(
        self, map_digest: str, gen_line: int
    ) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT gen_col, src_index, src_line, src_col FROM segments "
            "WHERE map_digest=? AND gen_line=? ORDER BY gen_col",
            (map_digest, gen_line),
        ).fetchall()

    def segment_at_or_before(
        self, map_digest: str, gen_line: int, gen_col: int
    ) -> sqlite3.Row | None:
        """Nearest segment on the same generated line with col <= gen_col."""
        return self._conn.execute(
            "SELECT gen_col, src_index, src_line, src_col FROM segments "
            "WHERE map_digest=? AND gen_line=? AND gen_col<=? "
            "ORDER BY gen_col DESC LIMIT 1",
            (map_digest, gen_line, gen_col),
        ).fetchone()


# --------------------------------------------------------------------------
# Hop resolution outcomes
# --------------------------------------------------------------------------
@dataclass
class _Hop:
    """Outcome of resolving one hop point.

    ``cacheable`` is False exactly when the outcome depends on the entry
    path (a binding cycle) or on the remaining layer budget; such outcomes
    are never stored in the memo, so they can never be reused as if they
    were universal.
    """

    final: tuple[str, int, int] | None  # (identity, src_line, src_col)
    reason: str
    layers_used: int                    # lookups from this point inclusive
    suffix_sigs: frozenset              # (artifact_id, map_digest) visited
    cacheable: bool


def _subtract_holes(
    start: int, end: int, holes: list[int]
) -> list[tuple[int, int]]:
    """Split ``[start, end)`` around invalid columns (low-surrogate units).

    ``holes`` must be sorted ascending.  Runs in the result are maximal
    half-open intervals of valid character starts; the work is proportional
    to the number of holes inside the interval, never to its length.
    """
    runs: list[tuple[int, int]] = []
    cursor = start
    for hole in holes:
        if hole < start:
            continue
        if hole >= end:
            break
        if cursor < hole:
            runs.append((cursor, hole))
        cursor = hole + 1
    if cursor < end:
        runs.append((cursor, end))
    return runs


# --------------------------------------------------------------------------
# Index builder
# --------------------------------------------------------------------------
class ReverseIndexBuilder:
    """Build the reverse index from a sealed build database."""

    def __init__(self, build_path: str, index_path: str) -> None:
        self.build_db = _BuildDB(build_path)
        self.index_path = index_path
        for stale in (index_path, index_path + "-wal", index_path + "-shm"):
            if os.path.exists(stale):
                os.remove(stale)
        self._conn = sqlite3.connect(index_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()
        self._memo: dict[tuple, _Hop] = {}
        self._line_indexes: OrderedDict[str, LineIndex] = OrderedDict()
        self.counters: dict[str, int] = {key: 0 for key in _COUNTER_KEYS}

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE rev_meta(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE entries(
                release_id TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                artifact_digest TEXT NOT NULL,
                map_digest TEXT,
                PRIMARY KEY(release_id, artifact_id)
            );
            CREATE TABLE hits(
                source_identity TEXT NOT NULL,
                src_line INTEGER NOT NULL,
                src_col INTEGER NOT NULL,
                release_id TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                artifact_digest TEXT NOT NULL,
                gen_line INTEGER NOT NULL,
                gen_col_start INTEGER NOT NULL,
                gen_col_end INTEGER NOT NULL
            );
            CREATE INDEX idx_hits_source
                ON hits(source_identity, src_line, src_col);
            """
        )
        self._meta("schema_version", str(SCHEMA_VERSION))
        self._meta("state", "building")
        self._meta("build_db_sha256", self.build_db.sha256)
        self._meta("build_db_path", os.path.abspath(self.build_db.path))
        self._conn.commit()

    def _meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO rev_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def close(self) -> None:
        self.build_db.close()
        self._conn.commit()
        self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self._conn.close()

    def __enter__(self) -> "ReverseIndexBuilder":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------- build
    def build(self) -> dict[str, Any]:
        started = time.perf_counter()
        for entry in self.build_db.entries():
            self._process_entry(entry)
        for key, value in sorted(self.counters.items()):
            self._meta(f"counter_{key}", str(value))
        self._meta("state", "complete")
        self._meta("built_at_unix", str(int(time.time())))
        self._conn.commit()
        elapsed = time.perf_counter() - started
        report: dict[str, Any] = {
            "index": self.index_path,
            "build_db_sha256": self.build_db.sha256,
            "build_seconds": round(elapsed, 3),
        }
        report.update(self.counters)
        return report

    def _line_index(self, content_digest: str) -> LineIndex | None:
        cached = self._line_indexes.get(content_digest)
        if cached is not None:
            self._line_indexes.move_to_end(content_digest)
            return cached
        text = self.build_db.artifact_text(content_digest)
        if text is None:
            return None
        index = LineIndex(text)
        if len(self._line_indexes) >= 32:
            self._line_indexes.popitem(last=False)
        self._line_indexes[content_digest] = index
        return index

    def _process_entry(self, entry: sqlite3.Row) -> None:
        counters = self.counters
        counters["entries_total"] += 1
        release_id = entry["release_id"]
        artifact_id = entry["artifact_id"]
        map_digest = entry["map_digest"]
        self._conn.execute(
            "INSERT INTO entries(release_id, artifact_id, artifact_digest, "
            "map_digest) VALUES(?, ?, ?, ?)",
            (release_id, artifact_id, entry["sha256"], map_digest),
        )
        if map_digest is None:
            counters["entries_skipped_no_map"] += 1
            return
        meta = self.build_db.map_meta(map_digest)
        if meta is None or not meta["valid"]:
            counters["entries_skipped_invalid_map"] += 1
            return
        line_index = self._line_index(entry["content_digest"])
        if line_index is None:
            counters["entries_skipped_no_body"] += 1
            return
        sources = self.build_db.map_sources(map_digest)
        for gen_line in self.build_db.segment_lines(map_digest):
            if gen_line >= line_index.line_count():
                continue  # segments past the body end cover no characters
            segments = self.build_db.line_segments(map_digest, gen_line)
            if not segments:
                continue
            counters["entry_lines_scanned"] += 1
            utf16_len = line_index.utf16_length(gen_line)
            holes = [
                unit + 1
                for _cp, unit in line_index.non_bmp_spans(gen_line)
            ]
            self._cover_line(
                entry, sources, gen_line, segments, utf16_len, holes
            )

    def _cover_line(
        self,
        entry: sqlite3.Row,
        sources: dict[int, str],
        gen_line: int,
        segments: list[sqlite3.Row],
        utf16_len: int,
        holes: list[int],
    ) -> None:
        """Interval coverage of one generated line; never per character."""
        counters = self.counters
        last: list | None = None  # [start, end, identity, src_line, src_col]
        count = len(segments)
        for i, seg in enumerate(segments):
            gen_col = seg["gen_col"]
            # Adjacent stored segments delimit coverage; the last segment
            # reaches the line end.  Columns before the first segment belong
            # to nobody, and generated-only segments truncate coverage too.
            nxt = segments[i + 1]["gen_col"] if i + 1 < count else utf16_len
            cov_start = max(gen_col, 0)
            cov_end = min(nxt, utf16_len)
            if seg["src_index"] == NO_ORIGINAL:
                counters["entry_segments_generated_only"] += 1
                continue
            counters["entry_segments_mapped"] += 1
            if cov_start >= cov_end:
                continue
            identity = sources.get(seg["src_index"])
            if identity is None:  # defensive: ingestion validated indexes
                continue
            final = self._resolve_entry_segment(
                entry, identity, seg["src_line"], seg["src_col"]
            )
            if final is None:
                continue
            for run_start, run_end in _subtract_holes(
                cov_start, cov_end, holes
            ):
                if run_start >= run_end:
                    continue
                counters["generated_columns_indexed"] += run_end - run_start
                if (
                    last is not None
                    and last[1] == run_start
                    and last[2] == final[0]
                    and last[3] == final[1]
                    and last[4] == final[2]
                ):
                    # Consecutive valid columns, same final point: merge.
                    last[1] = run_end
                    continue
                if last is not None:
                    self._emit(entry, gen_line, last)
                last = [run_start, run_end, final[0], final[1], final[2]]
        if last is not None:
            self._emit(entry, gen_line, last)

    def _emit(self, entry: sqlite3.Row, gen_line: int, run: list) -> None:
        self._conn.execute(
            "INSERT INTO hits(source_identity, src_line, src_col, "
            "release_id, artifact_id, artifact_digest, gen_line, "
            "gen_col_start, gen_col_end) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run[2], run[3], run[4],
                entry["release_id"], entry["artifact_id"], entry["sha256"],
                gen_line, run[0], run[1],
            ),
        )
        self.counters["hit_rows"] += 1

    # ------------------------------------------------------- resolution
    def _resolve_entry_segment(
        self,
        entry: sqlite3.Row,
        identity: str,
        src_line: int,
        src_col: int,
    ) -> tuple[str, int, int] | None:
        """Resolve one entry-layer segment to a final point, or None."""
        release_id = entry["release_id"]
        map_digest = entry["map_digest"]
        binding = self.build_db.binding(release_id, map_digest, identity)
        if binding is None:
            # Terminal source: no deeper layer.  A missing source *body*
            # does not undo the exact original position.
            self.counters["terminal_at_entry"] += 1
            return (identity, src_line, src_col)
        next_artifact = self.build_db.artifact(release_id, binding[0])
        if next_artifact is None:
            self.counters["hop_missing_artifact"] += 1
            return None
        visited = frozenset({(entry["artifact_id"], map_digest or "")})
        hop = self._resolve_hop(
            release_id, next_artifact, src_line, src_col, visited, 1
        )
        return hop.final

    def _resolve_hop(
        self,
        release_id: str,
        artifact: sqlite3.Row,
        line: int,
        col: int,
        visited: frozenset,
        depth: int,
    ) -> _Hop:
        """Resolve one hop point (layer ``depth``) like ``enrich_frame``.

        The memo is keyed by the hop point alone, but an entry is reused
        only when its recorded suffix signatures are disjoint from the
        current path's visited set and the remaining layer budget suffices;
        cycle/limit outcomes are never stored at all.
        """
        artifact_id = artifact["artifact_id"]
        map_digest = artifact["map_digest"]
        key = (release_id, artifact_id, map_digest, line, col)
        cached = self._memo.get(key)
        if cached is not None:
            if (
                not (cached.suffix_sigs & visited)
                and depth + cached.layers_used <= MAX_LAYERS
            ):
                self.counters["hop_points_reused"] += 1
                return cached
            # Path-dependent or budget-tight entry: recompute fresh.
            self.counters["hop_cache_rejected_path"] += 1

        signature = (artifact_id, map_digest or "")
        if depth >= MAX_LAYERS:
            self.counters["hop_layer_limits"] += 1
            return _Hop(None, "layer_limit_exceeded", 0, frozenset(), False)
        if signature in visited:
            self.counters["hop_cycles"] += 1
            return _Hop(None, "binding_cycle", 0, frozenset(), False)

        suffix = {signature}
        meta = self.build_db.map_meta(map_digest) if map_digest else None
        if meta is None or not meta["valid"]:
            reason = (
                meta["error_reason"] if meta is not None else "map_not_stored"
            )
            self.counters["hop_invalid_map"] += 1
            return self._cache(key, _Hop(None, reason, 1,
                                         frozenset(suffix), True))

        seg = self.build_db.segment_at_or_before(map_digest, line, col)
        if seg is None:
            self.counters["hop_unmapped"] += 1
            return self._cache(key, _Hop(None, "no_segment_on_generated_line",
                                         1, frozenset(suffix), True))
        if seg["src_index"] == NO_ORIGINAL:
            self.counters["hop_unmapped"] += 1
            return self._cache(key, _Hop(None,
                                         "segment_without_original_position",
                                         1, frozenset(suffix), True))

        identity = self.build_db.map_sources(map_digest).get(seg["src_index"])
        if identity is None:  # defensive: ingestion validated indexes
            return self._cache(key, _Hop(None, "source_index_unknown",
                                         1, frozenset(suffix), True))
        binding = self.build_db.binding(release_id, map_digest, identity)
        if binding is None:
            final = (identity, seg["src_line"], seg["src_col"])
            return self._cache(key, _Hop(final, "mapped", 1,
                                         frozenset(suffix), True))
        next_artifact = self.build_db.artifact(release_id, binding[0])
        if next_artifact is None:
            self.counters["hop_missing_artifact"] += 1
            return self._cache(key, _Hop(None, "bound_artifact_absent",
                                         1, frozenset(suffix), True))
        # The next layer is queried at the segment's *explicit* original
        # coordinate -- never the entry column plus a delta.
        sub = self._resolve_hop(
            release_id, next_artifact, seg["src_line"], seg["src_col"],
            visited | {signature}, depth + 1,
        )
        if not sub.cacheable:
            return _Hop(None, sub.reason, 1 + sub.layers_used,
                        frozenset(), False)
        return self._cache(key, _Hop(sub.final, sub.reason,
                                     1 + sub.layers_used,
                                     frozenset(suffix | sub.suffix_sigs),
                                     True))

    def _cache(self, key: tuple, hop: _Hop) -> _Hop:
        self._memo[key] = hop
        self.counters["hop_points_computed"] += 1
        return hop


# --------------------------------------------------------------------------
# Query side: re-openable, self-contained
# --------------------------------------------------------------------------
_QUERY_SQL = (
    "SELECT release_id, artifact_id, artifact_digest, gen_line, "
    "gen_col_start, gen_col_end, source_identity, src_line, src_col "
    "FROM hits "
    "WHERE source_identity = ? "
    "AND src_line BETWEEN ? AND ? "
    "AND (src_line > ? OR src_col >= ?) "
    "AND (src_line < ? OR src_col < ?) "
    "ORDER BY release_id, artifact_id, gen_line, gen_col_start, "
    "gen_col_end, src_line, src_col"
)


class ReverseIndex:
    """Read-only access to a built reverse index file (no build DB needed)."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._conn = _open_sealed_readonly(path, "reverse index")
        missing = [t for t in INDEX_TABLES
                   if t not in _table_names(self._conn)]
        if missing:
            raise ReverseIndexError(
                f"{path!r} is not a smpipe reverse index; missing tables: "
                + ", ".join(missing)
            )
        meta = {
            row["key"]: row["value"]
            for row in self._conn.execute("SELECT key, value FROM rev_meta")
        }
        if meta.get("schema_version") != str(SCHEMA_VERSION):
            raise ReverseIndexError(
                f"{path!r} has unsupported schema version "
                f"{meta.get('schema_version')!r}"
            )
        if meta.get("state") != "complete":
            raise ReverseIndexError(
                f"{path!r} is an incomplete reverse index (build did not "
                "finish); rebuild it"
            )
        self._meta = meta

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "ReverseIndex":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def info(self) -> dict[str, Any]:
        """Build-time metadata and counters recorded in the index file."""
        return dict(self._meta)

    @staticmethod
    def _validate(
        source_identity: Any,
        start: tuple[Any, Any],
        end: tuple[Any, Any],
    ) -> None:
        if not isinstance(source_identity, str) or not source_identity:
            raise InvalidSourceRange(
                "source_identity must be a non-empty string"
            )
        for value, name in (
            (start[0], "start_line"), (start[1], "start_col"),
            (end[0], "end_line"), (end[1], "end_col"),
        ):
            if not isinstance(value, int) or isinstance(value, bool) \
                    or value < 0:
                raise InvalidSourceRange(
                    f"{name} must be a non-negative integer, got {value!r}"
                )
        if not (start < end):
            raise InvalidSourceRange(
                f"range start {start} must be strictly before end {end} "
                "(coordinate pairs compare in order; equal or inverted "
                "ranges are rejected)"
            )

    def query(
        self,
        source_identity: str,
        start: tuple[int, int],
        end: tuple[int, int],
    ) -> list[dict[str, Any]]:
        """All generated ranges whose final source point lies in the range.

        The read is bounded by the ``(source_identity, src_line, src_col)``
        index: only rows of this source inside the line band are touched --
        never a scan of other sources' generated ranges, and no mapping
        chain is re-walked at query time.
        """
        self._validate(source_identity, start, end)
        rows = self._conn.execute(
            _QUERY_SQL,
            (
                source_identity,
                start[0], end[0],
                start[0], start[1],
                end[0], end[1],
            ),
        ).fetchall()
        return [
            {
                "release_id": row["release_id"],
                "artifact_id": row["artifact_id"],
                "artifact_digest": row["artifact_digest"],
                "gen_line": row["gen_line"],
                "gen_col_start": row["gen_col_start"],
                "gen_col_end": row["gen_col_end"],
                "source_identity": row["source_identity"],
                "src_line": row["src_line"],
                "src_col": row["src_col"],
            }
            for row in rows
        ]

    def explain_plan(
        self,
        source_identity: str,
        start: tuple[int, int],
        end: tuple[int, int],
    ) -> list[str]:
        """EXPLAIN QUERY PLAN lines for the exact query above."""
        self._validate(source_identity, start, end)
        rows = self._conn.execute(
            "EXPLAIN QUERY PLAN " + _QUERY_SQL,
            (
                source_identity,
                start[0], end[0],
                start[0], start[1],
                end[0], end[1],
            ),
        ).fetchall()
        return [" | ".join(str(value) for value in row) for row in rows]
