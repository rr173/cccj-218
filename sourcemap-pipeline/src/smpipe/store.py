"""Re-openable on-disk store for build facts and enrichment results.

All data is held in a single SQLite database plus blob tables, so the store
keeps working after the original input directory has been replaced or
removed.  Segment lookups use a covering B-tree index and a small bounded
in-process LRU cache of generated lines; access counters prove that a frame
query never re-decodes a whole map.

The ``-1`` sentinel stands in for NULL in the segment table so that the
UNIQUE index can fold byte-identical mappings correctly (SQLite treats NULLs
as distinct).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections import OrderedDict
from collections.abc import Iterable
from typing import Any

from .errors import DigestMismatch, DuplicateFrame, InvalidSourceMap

NO_ORIGINAL = -1
SCHEMA_VERSION = 1
SEGMENT_BATCH = 5000


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


class _LineCache:
    """Bounded LRU cache of one generated line's segment rows."""

    def __init__(self, max_bytes: int = 4 * 1024 * 1024) -> None:
        self.max_bytes = max_bytes
        self._items: OrderedDict[tuple[str, int], list[tuple]] = OrderedDict()
        self._bytes = 0
        self.hits = 0
        self.misses = 0

    def get(self, key: tuple[str, int]) -> list[tuple] | None:
        value = self._items.get(key)
        if value is None:
            self.misses += 1
            return None
        self.hits += 1
        self._items.move_to_end(key)
        return value

    def put(self, key: tuple[str, int], value: list[tuple]) -> None:
        weight = max(64, len(value) * 32)
        self._items[key] = value
        self._bytes += weight
        self._items.move_to_end(key)
        while self._bytes > self.max_bytes and len(self._items) > 1:
            _old_key, old_value = self._items.popitem(last=False)
            self._bytes -= max(64, len(old_value) * 32)

    def clear(self) -> None:
        self._items.clear()
        self._bytes = 0


class Store:
    def __init__(self, path: str, *, line_cache_bytes: int = 2 * 1024 * 1024,
                 dense_line_threshold: int = 64):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._dense_line_threshold = dense_line_threshold
        # Small bounded caches: dense segment lines stay on disk, while map
        # name tables and generated bodies are small enough to memoize.
        from .columns import LineIndex  # local import avoids import cycles
        self._LineIndex = LineIndex
        self._line_index_cache: OrderedDict[str, "LineIndex"] = OrderedDict()
        self._names_cache: OrderedDict[str, list[str]] = OrderedDict()
        self._text_cache: OrderedDict[str, str] = OrderedDict()
        self._text_cache_bytes = 0
        self._TEXT_CACHE_LIMIT = 16 * 1024 * 1024
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA cache_size=-8192")  # ~8 MiB page cache
        self._line_cache = _LineCache(max_bytes=line_cache_bytes)
        self._init_schema()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS blobs(
                    digest TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    content BLOB NOT NULL,
                    size_bytes INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS releases(
                    release_id TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS artifacts(
                    release_id TEXT NOT NULL,
                    artifact_id TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    content_digest TEXT NOT NULL,
                    map_digest TEXT,
                    PRIMARY KEY(release_id, artifact_id)
                );
                CREATE TABLE IF NOT EXISTS url_aliases(
                    release_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    artifact_id TEXT NOT NULL,
                    PRIMARY KEY(release_id, url)
                );
                CREATE TABLE IF NOT EXISTS maps(
                    map_digest TEXT PRIMARY KEY,
                    valid INTEGER NOT NULL,
                    sectioned INTEGER NOT NULL,
                    section_count INTEGER NOT NULL,
                    source_root TEXT,
                    segment_count INTEGER NOT NULL DEFAULT 0,
                    error_reason TEXT
                );
                CREATE TABLE IF NOT EXISTS map_sources(
                    map_digest TEXT NOT NULL,
                    source_index INTEGER NOT NULL,
                    source_identity TEXT NOT NULL,
                    section_index INTEGER NOT NULL,
                    local_source_index INTEGER NOT NULL,
                    inline_content_digest TEXT,
                    PRIMARY KEY(map_digest, source_index)
                );
                CREATE TABLE IF NOT EXISTS bindings(
                    release_id TEXT NOT NULL,
                    map_digest TEXT NOT NULL,
                    source_identity TEXT NOT NULL,
                    next_artifact_id TEXT NOT NULL,
                    next_map_digest TEXT,
                    PRIMARY KEY(release_id, map_digest, source_identity)
                );
                CREATE TABLE IF NOT EXISTS source_pins(
                    source_identity TEXT PRIMARY KEY,
                    body_digest TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS segments(
                    map_digest TEXT NOT NULL,
                    gen_line INTEGER NOT NULL,
                    gen_col INTEGER NOT NULL,
                    src_index INTEGER NOT NULL,
                    src_line INTEGER NOT NULL,
                    src_col INTEGER NOT NULL,
                    name_index INTEGER NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_segments_fold
                    ON segments(map_digest, gen_line, gen_col, src_index,
                                src_line, src_col, name_index);
                CREATE INDEX IF NOT EXISTS idx_segments_lookup
                    ON segments(map_digest, gen_line, gen_col);
                CREATE TABLE IF NOT EXISTS frames(
                    event_id TEXT NOT NULL,
                    frame_index INTEGER NOT NULL,
                    raw_hash TEXT NOT NULL,
                    release_id TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    result_json TEXT,
                    PRIMARY KEY(event_id, frame_index)
                );
                CREATE INDEX IF NOT EXISTS idx_frames_release
                    ON frames(release_id);
                CREATE TABLE IF NOT EXISTS metrics(
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        with self._lock:
            # Flush any implicit transaction and checkpoint WAL so a reopened
            # store sees every write even if the WAL file is later removed.
            self._conn.commit()
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ----------------------------------------------------------------- metrics
    def incr(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO metrics(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = value + excluded.value",
                (key, amount),
            )

    def metrics_snapshot(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT key, value FROM metrics").fetchall()
        snap = {r["key"]: r["value"] for r in rows}
        snap["line_cache_hits"] = self._line_cache.hits
        snap["line_cache_misses"] = self._line_cache.misses
        return snap

    def reset_metrics(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM metrics")
        self._line_cache.hits = 0
        self._line_cache.misses = 0

    # ------------------------------------------------------------------- blobs
    def put_blob(self, digest: str, kind: str, content: bytes) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO blobs(digest, kind, content, size_bytes) "
                "VALUES(?, ?, ?, ?)",
                (digest, kind, content, len(content)),
            )

    def get_blob(self, digest: str) -> bytes | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT content FROM blobs WHERE digest = ?", (digest,)
            ).fetchone()
        if row is None:
            return None
        self.incr("blob_reads")
        return bytes(row["content"])

    def get_text(self, digest: str) -> str | None:
        """Memoized UTF-8 text fetch for small, repeatedly used bodies."""
        cached = self._text_cache.get(digest)
        if cached is not None:
            self._text_cache.move_to_end(digest)
            self.incr("text_cache_hits")
            return cached
        blob = self.get_blob(digest)
        if blob is None:
            return None
        text = blob.decode("utf-8")
        self._text_cache[digest] = text
        self._text_cache.move_to_end(digest)
        self._text_cache_bytes += len(blob)
        while (self._text_cache_bytes > self._TEXT_CACHE_LIMIT
               and len(self._text_cache) > 1):
            _old, old_text = self._text_cache.popitem(last=False)
            self._text_cache_bytes -= len(old_text.encode("utf-8"))
        return text

    def get_line_index(self, digest: str):
        """Memoized :class:`~smpipe.columns.LineIndex` for a text blob."""
        cached = self._line_index_cache.get(digest)
        if cached is not None:
            self._line_index_cache.move_to_end(digest)
            self.incr("line_index_cache_hits")
            return cached
        text = self.get_text(digest)
        if text is None:
            return None
        index = self._LineIndex(text)
        if len(self._line_index_cache) >= 64:
            self._line_index_cache.popitem(last=False)
        self._line_index_cache[digest] = index
        return index

    def get_map_names(self, map_digest: str) -> list[str]:
        """Cached flattening of a map's ``names`` (plain or sectioned)."""
        cached = self._names_cache.get(map_digest)
        if cached is not None:
            self._names_cache.move_to_end(map_digest)
            self.incr("names_cache_hits")
            return cached
        data = self.get_blob(map_digest)
        if data is None:
            return []
        obj = json.loads(data.decode("utf-8"))
        names: list[str] = []
        if "sections" in obj:
            for section in obj["sections"]:
                names.extend(section.get("map", {}).get("names", []))
        else:
            names.extend(obj.get("names", []))
        if len(self._names_cache) >= 256:
            self._names_cache.popitem(last=False)
        self._names_cache[map_digest] = names
        return names

    def has_blob(self, digest: str) -> bool:
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM blobs WHERE digest=?", (digest,)
            ).fetchone() is not None

    # ----------------------------------------------------------------- releases
    def register_release(self, release_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO releases(release_id) VALUES(?)",
                (release_id,),
            )

    def list_releases(self) -> list[str]:
        with self._lock:
            return [
                r["release_id"]
                for r in self._conn.execute(
                    "SELECT release_id FROM releases ORDER BY release_id"
                )
            ]

    # --------------------------------------------------------------- artifacts
    def put_artifact(
        self,
        release_id: str,
        artifact_id: str,
        sha256: str,
        content_digest: str,
        map_digest: str | None,
    ) -> None:
        with self._lock:
            existing = self._conn.execute(
                "SELECT sha256 FROM artifacts WHERE release_id=? AND artifact_id=?",
                (release_id, artifact_id),
            ).fetchone()
            if existing is not None and existing["sha256"] != sha256:
                raise DigestMismatch(
                    f"artifact {artifact_id!r} in release {release_id!r} is "
                    f"already pinned to {existing['sha256']}, refusing "
                    f"{sha256}"
                )
            self._conn.execute(
                "INSERT OR IGNORE INTO artifacts(release_id, artifact_id, "
                "sha256, content_digest, map_digest) VALUES(?, ?, ?, ?, ?)",
                (release_id, artifact_id, sha256, content_digest, map_digest),
            )

    def get_artifact(self, release_id: str, artifact_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM artifacts WHERE release_id=? AND artifact_id=?",
                (release_id, artifact_id),
            ).fetchone()

    def put_url_alias(self, release_id: str, url: str, artifact_id: str) -> None:
        with self._lock:
            existing = self._conn.execute(
                "SELECT artifact_id FROM url_aliases WHERE release_id=? AND url=?",
                (release_id, url),
            ).fetchone()
            if existing is not None and existing["artifact_id"] != artifact_id:
                raise DigestMismatch(
                    f"URL {url!r} in release {release_id!r} is already bound "
                    f"to artifact {existing['artifact_id']!r}"
                )
            self._conn.execute(
                "INSERT OR IGNORE INTO url_aliases(release_id, url, artifact_id) "
                "VALUES(?, ?, ?)",
                (release_id, url, artifact_id),
            )

    def resolve_alias(self, release_id: str, url: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT artifact_id FROM url_aliases WHERE release_id=? AND url=?",
                (release_id, url),
            ).fetchone()
        return None if row is None else row["artifact_id"]

    # -------------------------------------------------------------------- maps
    def put_map_meta(
        self,
        map_digest: str,
        *,
        valid: bool,
        sectioned: bool,
        section_count: int,
        source_root: str | None,
        segment_count: int,
        error_reason: str | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO maps(map_digest, valid, sectioned, "
                "section_count, source_root, segment_count, error_reason) "
                "VALUES(?, ?, ?, ?, ?, ?, ?)",
                (
                    map_digest,
                    1 if valid else 0,
                    1 if sectioned else 0,
                    section_count,
                    source_root,
                    segment_count,
                    error_reason,
                ),
            )

    def get_map_meta(self, map_digest: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM maps WHERE map_digest=?", (map_digest,)
            ).fetchone()

    def put_map_source(
        self,
        map_digest: str,
        source_index: int,
        source_identity: str,
        section_index: int,
        local_source_index: int,
        inline_content_digest: str | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO map_sources(map_digest, source_index, "
                "source_identity, section_index, local_source_index, "
                "inline_content_digest) VALUES(?, ?, ?, ?, ?, ?)",
                (
                    map_digest,
                    source_index,
                    source_identity,
                    section_index,
                    local_source_index,
                    inline_content_digest,
                ),
            )

    def get_map_source(self, map_digest: str, source_index: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM map_sources WHERE map_digest=? AND source_index=?",
                (map_digest, source_index),
            ).fetchone()

    def put_binding(
        self,
        release_id: str,
        map_digest: str,
        source_identity: str,
        next_artifact_id: str,
        next_map_digest: str | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO bindings(release_id, map_digest, "
                "source_identity, next_artifact_id, next_map_digest) "
                "VALUES(?, ?, ?, ?, ?)",
                (
                    release_id,
                    map_digest,
                    source_identity,
                    next_artifact_id,
                    next_map_digest,
                ),
            )

    def get_binding(
        self, release_id: str, map_digest: str, source_identity: str
    ) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM bindings WHERE release_id=? AND map_digest=? "
                "AND source_identity=?",
                (release_id, map_digest, source_identity),
            ).fetchone()

    def put_source_pin(self, source_identity: str, body_digest: str) -> None:
        with self._lock:
            existing = self._conn.execute(
                "SELECT body_digest FROM source_pins WHERE source_identity=?",
                (source_identity,),
            ).fetchone()
            if existing is not None and existing["body_digest"] != body_digest:
                raise DigestMismatch(
                    f"source {source_identity!r} already pinned to "
                    f"{existing['body_digest']}, refusing {body_digest}"
                )
            self._conn.execute(
                "INSERT OR IGNORE INTO source_pins(source_identity, "
                "body_digest) VALUES(?, ?)",
                (source_identity, body_digest),
            )

    def get_source_pin(self, source_identity: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT body_digest FROM source_pins WHERE source_identity=?",
                (source_identity,),
            ).fetchone()
        return None if row is None else row["body_digest"]

    # --------------------------------------------------------------- segments
    def insert_segments(
        self, map_digest: str, rows: Iterable[tuple[int, int, int, int, int, int]]
    ) -> int:
        """Stream segments into the index, folding exact duplicates.

        Returns the number of stored (post-fold) segments.
        """
        sql = (
            "INSERT OR IGNORE INTO segments(map_digest, gen_line, gen_col, "
            "src_index, src_line, src_col, name_index) VALUES(?, ?, ?, ?, ?, ?, ?)"
        )
        stored = 0
        batch: list[tuple] = []
        with self._lock:
            # One transaction for the whole stream; relaxed synchronous mode
            # is safe inside WAL and restored before returning.  Any implicit
            # transaction opened by earlier DML must be committed first.
            self._conn.commit()
            self._conn.execute("PRAGMA synchronous=OFF")
            try:
                for row in rows:
                    gen_line, gen_col, src_i, src_line, src_col, name_i = row
                    batch.append(
                        (
                            map_digest,
                            gen_line,
                            gen_col,
                            NO_ORIGINAL if src_i is None else src_i,
                            NO_ORIGINAL if src_line is None else src_line,
                            NO_ORIGINAL if src_col is None else src_col,
                            NO_ORIGINAL if name_i is None else name_i,
                        )
                    )
                    if len(batch) >= SEGMENT_BATCH:
                        stored += self._flush_segment_batch(sql, batch)
                        batch.clear()
                if batch:
                    stored += self._flush_segment_batch(sql, batch)
                self._conn.commit()
            finally:
                # No implicit transaction may be open when the safety level
                # changes; roll back a partial load on failure first.
                if self._conn.in_transaction:
                    self._conn.rollback()
                self._conn.execute("PRAGMA synchronous=NORMAL")
        return stored

    def _flush_segment_batch(self, sql: str, batch: list[tuple]) -> int:
        before = self._conn.total_changes
        self._conn.executemany(sql, batch)
        return self._conn.total_changes - before

    def conflicting_coordinates(self, map_digest: str) -> list[tuple[int, int]]:
        """Return generated coordinates with two or more distinct mappings."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT gen_line, gen_col FROM segments WHERE map_digest=? "
                "GROUP BY gen_line, gen_col "
                "HAVING (SELECT COUNT(*) FROM ("
                "  SELECT DISTINCT src_index, src_line, src_col, name_index "
                "  FROM segments s2 WHERE s2.map_digest=segments.map_digest "
                "  AND s2.gen_line=segments.gen_line "
                "  AND s2.gen_col=segments.gen_col)) > 1 LIMIT 20",
                (map_digest,),
            ).fetchall()
        return [(r["gen_line"], r["gen_col"]) for r in rows]

    def delete_segments(self, map_digest: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM segments WHERE map_digest=?", (map_digest,)
            )
            self._conn.execute(
                "DELETE FROM map_sources WHERE map_digest=?", (map_digest,)
            )

    def lookup_segment(
        self, map_digest: str, gen_line: int, gen_col: int
    ) -> tuple | None:
        """Nearest segment on the line whose column is <= ``gen_col``.

        Crucially only segments *on the same generated line* are considered;
        the previous line is never borrowed.  A generated-only segment on the
        line is returned like any other hit, and the caller decides that it
        means ``unmapped`` rather than falling through to an earlier segment.

        Two access paths are used depending on line density:

        * sparse lines are served from the bounded in-process LRU cache after
          one indexed B-tree range scan;
        * dense lines (large maps) are answered by a single indexed SQL
          point query so Python never materialises the whole line.
        """
        self.incr("segment_lookup_queries")
        cache_key = (map_digest, gen_line)
        line = self._line_cache.get(cache_key)
        if line is None:
            with self._lock:
                # Cheap density probe via the covering lookup index.
                density = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM segments WHERE map_digest=? "
                    "AND gen_line=?",
                    (map_digest, gen_line),
                ).fetchone()["c"]
                if density > self._dense_line_threshold:
                    self.incr("sql_point_lookups")
                    row = self._conn.execute(
                        "SELECT gen_line, gen_col, src_index, src_line, "
                        "src_col, name_index FROM segments "
                        "WHERE map_digest=? AND gen_line=? AND gen_col<=? "
                        "ORDER BY gen_col DESC LIMIT 1",
                        (map_digest, gen_line, gen_col),
                    ).fetchone()
                    return None if row is None else tuple(row)
                rows = self._conn.execute(
                    "SELECT gen_line, gen_col, src_index, src_line, src_col, "
                    "name_index FROM segments WHERE map_digest=? AND gen_line=? "
                    "ORDER BY gen_col",
                    (map_digest, gen_line),
                ).fetchall()
            line = [tuple(r) for r in rows]
            self.incr("line_loads")
            self._line_cache.put(cache_key, line)

        # Lines are stored sorted; walk backwards to the nearest <= column.
        best: tuple | None = None
        inspected = 0
        for row in reversed(line):
            inspected += 1
            if row[1] <= gen_col:
                best = row
                break
        self.incr("segment_rows_inspected", inspected)
        return best

    def segment_count(self, map_digest: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM segments WHERE map_digest=?",
                (map_digest,),
            ).fetchone()
        return int(row["c"])

    # ------------------------------------------------------------------ frames
    def check_frame_input(
        self, event_id: str, frame_index: int, raw_hash: str
    ) -> str:
        """Deduplicate input frames.

        Returns ``"new"``, ``"duplicate"`` (identical repeat) or raises
        :class:`DuplicateFrame` when the same ``(event_id, frame_index)``
        arrives with different content -- a data error that must not be
        silently merged.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT raw_hash FROM frames WHERE event_id=? AND frame_index=?",
                (event_id, frame_index),
            ).fetchone()
            if row is None:
                return "new"
            if row["raw_hash"] != raw_hash:
                raise DuplicateFrame(
                    f"frame ({event_id}, {frame_index}) re-ingested with "
                    "different content"
                )
            return "duplicate"

    def put_frame(
        self,
        event_id: str,
        frame_index: int,
        raw_hash: str,
        release_id: str,
        input_json: str,
        result_json: str | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO frames(event_id, frame_index, raw_hash, "
                "release_id, input_json, result_json) VALUES(?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(event_id, frame_index) DO UPDATE SET "
                "result_json=excluded.result_json",
                (
                    event_id,
                    frame_index,
                    raw_hash,
                    release_id,
                    input_json,
                    result_json,
                ),
            )

    def iter_frames(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM frames ORDER BY event_id, frame_index"
            ).fetchall()

    def get_frame(self, event_id: str, frame_index: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM frames WHERE event_id=? AND frame_index=?",
                (event_id, frame_index),
            ).fetchone()

    def explain_inconsistent_map(self, map_digest: str) -> InvalidSourceMap:
        meta = self.get_map_meta(map_digest)
        reason = meta["error_reason"] if meta is not None else "invalid_map"
        return InvalidSourceMap(f"map {map_digest[:12]} rejected: {reason}",
                                reason=reason)

    def dump_json(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"))
