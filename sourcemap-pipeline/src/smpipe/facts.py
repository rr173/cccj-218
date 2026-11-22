"""Exposure first-error fact table, maintained incrementally from telemetry.

The feature answers one operational question: for every client *exposure*
(a release made active in a browser session at a given instant), what is the
first error, after exact source attribution, whose frame attribution lands in
the release that the exposure was actually serving?

Two databases are involved:

* a **sealed enrichment database** produced by :mod:`smpipe.pipeline`
  (``enrich``).  It is frozen forever; this module opens it read-only/immutable,
  never writes it and never re-runs attribution.  It only reads the already
  stored ``frames`` rows;
* a **working fact database** (new, owned by this module) that holds the
  exposure/event-location timeline, the materialised fact rows and the batch
  ledger.

Incremental maintenance rules (see ``docs/design.md`` section 8):

* an exposure is the half-open interval ``[start_ms, next start_ms)`` inside
  one session; an event at an exposure's exact start belongs to that exposure;
* an event qualifies for exposure *X* only when one of the event's stored
  frames is ``mapped`` *and* carries *X*'s ``release_id``.  With multiple such
  frames the smallest ``frame_index`` is the evidence.  There is no fall-back
  to the previous exposure;
* per exposure the first error is the minimum ``(occurred_ms, event_id)``
  with ``event_id`` compared as a string;
* a batch is applied against its projected *final* state, so a legal
  same-batch swap of two start times cannot trip a uniqueness check on an
  intermediate state;
* only exposures whose interval (in the before or after timeline) overlaps the
  changed window are recomputed.  Disjoint, unaffected fact rows are never
  rewritten and the sealed frame table is never scanned.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from bisect import bisect_left
from dataclasses import dataclass
from typing import Any

from .errors import (
    BatchError,
    BatchSequenceError,
    EnrichDatabaseError,
)

MAPPED = "mapped"
SCHEMA_VERSION = 1
WAL_NAME_SUFFIX = "-wal"


# --------------------------------------------------------------------------
# Pure value types
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Exposure:
    exposure_id: str
    session_id: str
    release_id: str
    start_ms: int


@dataclass(frozen=True, slots=True)
class EventLoc:
    event_id: str
    session_id: str
    occurred_ms: int


@dataclass(frozen=True, slots=True)
class FirstError:
    event_id: str
    frame_index: int
    occurred_ms: int

    def delay_ms(self, start_ms: int) -> int:
        return self.occurred_ms - start_ms


# --------------------------------------------------------------------------
# Pure projection: batch -> final exposures / event locations
# --------------------------------------------------------------------------
def _require_nonempty_str(value: Any, field: str) -> str:
    if not isinstance(value, str) or value == "":
        raise BatchError(f"field {field!r} must be a non-empty string")
    return value


def _require_nonneg_int(value: Any, field: str) -> int:
    # bool is a subclass of int; a boolean timestamp is never meaningful.
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise BatchError(f"field {field!r} must be a non-negative integer")
    return value


def _validate_change(entry: Any, kind: str, id_field: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise BatchError(f"{kind} entries must be objects")
    extra = set(entry) - {id_field, "op", "session_id", "release_id",
                          "start_ms", "occurred_ms"}
    if extra:
        raise BatchError(f"{kind} entry has unknown fields: {sorted(extra)}")
    _require_nonempty_str(entry.get(id_field), id_field)
    op = entry.get("op", "upsert")
    if op not in ("upsert", "delete"):
        raise BatchError(f"{kind} entry op must be 'upsert' or 'delete'")
    entry["op"] = op
    if op == "upsert":
        _require_nonempty_str(entry.get("session_id"), "session_id")
        if kind == "exposure":
            _require_nonempty_str(entry.get("release_id"), "release_id")
            _require_nonneg_int(entry.get("start_ms"), "start_ms")
        else:
            _require_nonneg_int(entry.get("occurred_ms"), "occurred_ms")
    else:
        unexpected = {
            key for key in ("session_id", "release_id", "start_ms",
                            "occurred_ms") if key in entry
        }
        if unexpected:
            raise BatchError(
                f"{kind} delete only carries {id_field!r} and 'op'; got "
                f"{sorted(unexpected)}"
            )
    return entry


def apply_batch(
    exposures: dict[str, Exposure],
    events: dict[str, EventLoc],
    payload: dict[str, Any],
) -> tuple[dict[str, Exposure], dict[str, EventLoc]]:
    """Project one already-seq-checked batch onto the current live state.

    Raises :class:`BatchError` for malformed entries, duplicate ids inside the
    batch, deletes of unknown ids, or a final timeline with two exposures
    sharing ``(session_id, start_ms)``.  The inputs are never mutated.
    """
    if not isinstance(payload, dict):
        raise BatchError("batch must be a JSON object")
    unknown = set(payload) - {
        "seq", "exposure_changes", "event_changes",
    }
    if unknown:
        raise BatchError(f"batch has unknown top-level fields: {sorted(unknown)}")
    exp_changes = payload.get("exposure_changes", [])
    evt_changes = payload.get("event_changes", [])
    if not isinstance(exp_changes, list) or not isinstance(evt_changes, list):
        raise BatchError("exposure_changes/event_changes must be arrays")

    next_exp = dict(exposures)
    next_evt = dict(events)

    seen_exp: set[str] = set()
    for entry in exp_changes:
        entry = _validate_change(entry, "exposure", "exposure_id")
        exp_id = entry["exposure_id"]
        if exp_id in seen_exp:
            raise BatchError(
                f"exposure_id {exp_id!r} appears more than once in one batch"
            )
        seen_exp.add(exp_id)
        if entry["op"] == "delete":
            if exp_id not in next_exp:
                raise BatchError(
                    f"cannot delete unknown exposure_id {exp_id!r}"
                )
            del next_exp[exp_id]
        else:
            next_exp[exp_id] = Exposure(
                exp_id, entry["session_id"], entry["release_id"],
                entry["start_ms"],
            )

    seen_evt: set[str] = set()
    for entry in evt_changes:
        entry = _validate_change(entry, "event", "event_id")
        evt_id = entry["event_id"]
        if evt_id in seen_evt:
            raise BatchError(
                f"event_id {evt_id!r} appears more than once in one batch"
            )
        seen_evt.add(evt_id)
        if entry["op"] == "delete":
            if evt_id not in next_evt:
                raise BatchError(
                    f"cannot delete unknown event_id {evt_id!r}"
                )
            del next_evt[evt_id]
        else:
            next_evt[evt_id] = EventLoc(
                evt_id, entry["session_id"], entry["occurred_ms"],
            )

    _assert_timeline(next_exp)
    return next_exp, next_evt


def _assert_timeline(exposures: dict[str, Exposure]) -> None:
    seen: set[tuple[str, int]] = set()
    for exp in exposures.values():
        key = (exp.session_id, exp.start_ms)
        if key in seen:
            raise BatchError(
                "final timeline conflict: session "
                f"{exp.session_id!r} has two exposures starting at "
                f"{exp.start_ms}"
            )
        seen.add(key)


# --------------------------------------------------------------------------
# Pure timeline helpers
# --------------------------------------------------------------------------
def session_timelines(
    exposures: dict[str, Exposure],
) -> dict[str, list[Exposure]]:
    grouped: dict[str, list[Exposure]] = {}
    for exp in exposures.values():
        grouped.setdefault(exp.session_id, []).append(exp)
    for rows in grouped.values():
        rows.sort(key=lambda item: (item.start_ms, item.exposure_id))
    return grouped


def exposure_at(
    timeline: list[Exposure], occurred_ms: int
) -> Exposure | None:
    """The exposure effective at ``occurred_ms`` (start is inclusive).

    Returns None when the instant precedes the first exposure.
    """
    starts = [row.start_ms for row in timeline]
    index = bisect_left(starts, occurred_ms)
    if index < len(timeline) and timeline[index].start_ms == occurred_ms:
        return timeline[index]
    if index == 0:
        return None
    return timeline[index - 1]


def _mark_neighbours(
    timeline: list[Exposure], index: int, sink: set[str]
) -> None:
    for pos in (index - 1, index, index + 1):
        if 0 <= pos < len(timeline):
            sink.add(timeline[pos].exposure_id)


def changed_exposure_ids(
    old_exp: dict[str, Exposure],
    new_exp: dict[str, Exposure],
    old_evt: dict[str, EventLoc],
    new_evt: dict[str, EventLoc],
) -> set[str]:
    """Identify exactly the exposures whose fact row may change.

    The scope is strictly local and independent of the distance a record
    moves.  Disjoint intervals of the same session are never re-traversed:

    * a changed exposure marks itself plus its immediate predecessor and
      successor in *both* the before and after timelines (only a neighbour's
      ``end_ms`` can shift);
    * a changed event location marks only the single exposure effective at
      its old instant (before timeline) and at its new instant (after
      timeline).  A long-distance relocation therefore touches two facts, not
      every interval in between.
    """
    old_lines = session_timelines(old_exp)
    new_lines = session_timelines(new_exp)
    old_index = {
        session: {row.exposure_id: pos for pos, row in enumerate(lines)}
        for session, lines in old_lines.items()
    }
    new_index = {
        session: {row.exposure_id: pos for pos, row in enumerate(lines)}
        for session, lines in new_lines.items()
    }
    affected: set[str] = set()

    for exp_id in set(old_exp) | set(new_exp):
        before = old_exp.get(exp_id)
        after = new_exp.get(exp_id)
        if before == after:
            continue
        if before is not None:
            pos = old_index[before.session_id][exp_id]
            _mark_neighbours(old_lines[before.session_id], pos, affected)
        if after is not None:
            pos = new_index[after.session_id][exp_id]
            _mark_neighbours(new_lines[after.session_id], pos, affected)

    for evt_id in set(old_evt) | set(new_evt):
        before = old_evt.get(evt_id)
        after = new_evt.get(evt_id)
        if before == after:
            continue
        if before is not None:
            holder = exposure_at(
                old_lines.get(before.session_id, []), before.occurred_ms
            )
            if holder is not None:
                affected.add(holder.exposure_id)
        if after is not None:
            holder = exposure_at(
                new_lines.get(after.session_id, []), after.occurred_ms
            )
            if holder is not None:
                affected.add(holder.exposure_id)
    return affected


# --------------------------------------------------------------------------
# Sealed enrichment database (read-only)
# --------------------------------------------------------------------------
def hash_database(path: str) -> str:
    """SHA-256 over the exact database bytes; used as the seal fingerprint."""
    digest = hashlib.sha256()
    try:
        handle = open(path, "rb")
    except OSError as exc:
        raise EnrichDatabaseError(
            f"sealed enrichment database not found: {path}"
        ) from exc
    with handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _EnrichReader:
    """Point-read access to a frozen enrichment database.

    The database is opened immutable; this process can neither write it nor
    observe external changes through the connection.  A non-empty ``-wal``
    sibling means the producer never checkpointed, so the database is not
    safely sealed and is rejected instead of silently reading a partial view.
    """

    def __init__(self, path: str) -> None:
        if not os.path.isfile(path):
            raise EnrichDatabaseError(f"enrichment database not found: {path}")
        wal = path + WAL_NAME_SUFFIX
        if os.path.exists(wal) and os.path.getsize(wal) > 0:
            raise EnrichDatabaseError(
                f"{path!r} still has a non-empty WAL; checkpoint and seal it "
                "before use"
            )
        try:
            self._conn = sqlite3.connect(
                f"file:{os.path.abspath(path)}?immutable=1",
                uri=True, check_same_thread=False,
            )
        except sqlite3.DatabaseError as exc:
            raise EnrichDatabaseError(f"cannot open {path!r}: {exc}") from exc
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("SELECT 1 FROM frames LIMIT 1")
        except sqlite3.DatabaseError as exc:
            raise EnrichDatabaseError(
                f"{path!r} is not a sealed enrichment database: {exc}"
            ) from exc

    def close(self) -> None:
        self._conn.close()

    def qualifying_frame(self, event_id: str, release_id: str) -> int | None:
        """Smallest ``frame_index`` whose stored result is mapped for release.

        A single primary-key range scan answers the question; the frames table
        is never scanned as a whole.
        """
        rows = self._conn.execute(
            "SELECT frame_index, result_json FROM frames "
            "WHERE event_id = ? AND release_id = ? "
            "ORDER BY frame_index ASC",
            (event_id, release_id),
        ).fetchall()
        for row in rows:
            result = json.loads(row["result_json"])
            if result.get("status") == MAPPED and \
                    result.get("release_id") == release_id:
                return int(row["frame_index"])
        return None

    def explain_frame_scan_plan(self, event_id: str, release_id: str) -> str:
        row = self._conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT frame_index, result_json FROM frames "
            "WHERE event_id = ? AND release_id = ? ORDER BY frame_index ASC",
            (event_id, release_id),
        ).fetchone()
        return " | ".join(str(value) for value in row)


# --------------------------------------------------------------------------
# Working fact database
# --------------------------------------------------------------------------
class FactStore:
    def __init__(self, path: str, *, enrich_path: str | None = None) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._init_schema()
        self._enrich_path = enrich_path
        self._enrich: _EnrichReader | None = None
        if enrich_path is not None:
            self._bind_enrich(enrich_path)

    # -------------------------------------------------------------- schema
    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS fact_meta(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches(
                    seq INTEGER PRIMARY KEY,
                    sha256 TEXT NOT NULL,
                    byte_length INTEGER NOT NULL,
                    imported_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS exposures(
                    exposure_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    release_id TEXT NOT NULL,
                    start_ms INTEGER NOT NULL,
                    UNIQUE(session_id, start_ms)
                );
                CREATE TABLE IF NOT EXISTS event_locs(
                    event_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    occurred_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_event_window
                    ON event_locs(session_id, occurred_ms, event_id);
                CREATE TABLE IF NOT EXISTS exposure_facts(
                    exposure_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    release_id TEXT NOT NULL,
                    start_ms INTEGER NOT NULL,
                    end_ms INTEGER,
                    first_event_id TEXT,
                    first_frame_index INTEGER,
                    first_occurred_ms INTEGER,
                    first_delay_ms INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_facts_export
                    ON exposure_facts(session_id, start_ms);
                CREATE TABLE IF NOT EXISTS fact_metrics(
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO fact_meta(key, value) "
                "VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        if self._enrich is not None:
            self._enrich.close()
            self._enrich = None
        self._conn.commit()
        self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self._conn.close()

    def __enter__(self) -> "FactStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------- metrics
    def _incr(self, key: str, amount: int = 1) -> None:
        self._conn.execute(
            "INSERT INTO fact_metrics(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = value + excluded.value",
            (key, amount),
        )

    def metrics_snapshot(self) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT key, value FROM fact_metrics"
        ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    def reset_metrics(self) -> None:
        self._conn.execute("DELETE FROM fact_metrics")

    # ------------------------------------------------------ enrich binding
    def _bind_enrich(self, enrich_path: str) -> None:
        seal = hash_database(enrich_path)
        stored = self._meta("enrich_sha256")
        if stored is not None and stored != seal:
            raise EnrichDatabaseError(
                "sealed enrichment database fingerprint changed: working "
                f"store records {stored[:12]}..., file is {seal[:12]}..."
            )
        reader = _EnrichReader(enrich_path)
        self._enrich = reader
        self._enrich_path = enrich_path
        if stored is None:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO fact_meta(key, value) VALUES(?, ?)",
                    ("enrich_sha256", seal),
                )
                self._conn.execute(
                    "INSERT INTO fact_meta(key, value) VALUES(?, ?)",
                    ("enrich_path", os.path.abspath(enrich_path)),
                )

    def _require_enrich(self) -> _EnrichReader:
        if self._enrich is None:
            # Reopen case: rebind from the path recorded in fact_meta.
            if self._enrich_path is None:
                recorded = self._meta("enrich_path")
                if recorded is None:
                    raise EnrichDatabaseError(
                        "no sealed enrichment database is attached to this "
                        "working store"
                    )
                self._bind_enrich(recorded)
            else:  # pragma: no cover - constructor always binds eagerly
                self._bind_enrich(self._enrich_path)
        assert self._enrich is not None
        return self._enrich

    def _meta(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM fact_meta WHERE key=?", (key,)
        ).fetchone()
        return None if row is None else row["value"]

    def last_seq(self) -> int:
        row = self._conn.execute("SELECT MAX(seq) AS m FROM batches").fetchone()
        return 0 if row["m"] is None else int(row["m"])

    # ------------------------------------------------------------- loading
    def _load_exposures(self) -> dict[str, Exposure]:
        return {
            r["exposure_id"]: Exposure(
                r["exposure_id"], r["session_id"], r["release_id"],
                int(r["start_ms"]),
            )
            for r in self._conn.execute(
                "SELECT * FROM exposures"
            ).fetchall()
        }

    def _load_events(self) -> dict[str, EventLoc]:
        return {
            r["event_id"]: EventLoc(
                r["event_id"], r["session_id"], int(r["occurred_ms"]),
            )
            for r in self._conn.execute(
                "SELECT * FROM event_locs"
            ).fetchall()
        }

    # ------------------------------------------------------------ import
    def import_batch(self, raw: bytes) -> dict[str, Any]:
        """Validate and apply one batch atomically; return a short report."""
        if not isinstance(raw, (bytes, bytearray)):
            raise BatchError("batch payload must be raw bytes")
        raw = bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise BatchError(f"batch is not valid UTF-8 JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise BatchError("batch root must be a JSON object")
        seq = payload.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
            raise BatchError("batch seq must be a positive integer")

        previous = self.last_seq()
        row = self._conn.execute(
            "SELECT * FROM batches WHERE seq=?", (seq,)
        ).fetchone()
        if row is not None:
            if row["sha256"] == digest and row["byte_length"] == len(raw):
                return {
                    "seq": seq, "noop": True,
                    "reason": "identical_batch_replayed",
                    "facts_inserted": 0, "facts_updated": 0,
                    "facts_deleted": 0, "facts_unchanged": 0,
                    "frame_qualification_queries": 0,
                }
            raise BatchSequenceError(
                f"seq {seq} was already imported with different content"
            )
        if seq != previous + 1:
            raise BatchSequenceError(
                f"expected seq {previous + 1}, got {seq}"
            )

        old_exp = self._load_exposures()
        old_evt = self._load_events()
        # All structural/final-state validation happens before any write, so
        # a rejected batch cannot leave partial state behind.
        new_exp, new_evt = apply_batch(old_exp, old_evt, payload)
        affected = changed_exposure_ids(old_exp, new_exp, old_evt, new_evt)
        enrich = self._require_enrich()
        old_facts = {
            r["exposure_id"]: dict(r)
            for r in self._conn.execute(
                "SELECT * FROM exposure_facts"
            ).fetchall()
        }

        try:
            with self._conn:
                # Timeline + locations first; the fact recomputation below
                # then reads the same post-change state inside this one
                # transaction.  Any failure rolls the whole batch back.
                self._apply_changes(payload)
                new_lines = session_timelines(new_exp)
                surviving_affected = sorted(
                    exp_id for exp_id in affected if exp_id in new_exp
                )
                computed: dict[str, dict[str, Any]] = {}
                frame_queries = 0
                for exp_id in surviving_affected:
                    exp = new_exp[exp_id]
                    timeline = new_lines[exp.session_id]
                    position = next(
                        i for i, row in enumerate(timeline)
                        if row.exposure_id == exp_id
                    )
                    end_ms = (
                        timeline[position + 1].start_ms
                        if position + 1 < len(timeline) else None
                    )
                    first, queries = self._first_error(exp, end_ms, enrich)
                    frame_queries += queries
                    computed[exp_id] = self._fact_row(exp, end_ms, first)

                deleted_ids = [
                    exp_id for exp_id in affected if exp_id not in new_exp
                ]
                if deleted_ids:
                    self._conn.executemany(
                        "DELETE FROM exposure_facts WHERE exposure_id=?",
                        [(exp_id,) for exp_id in deleted_ids],
                    )
                    self._incr("fact_rows_deleted", len(deleted_ids))
                inserted = updated = unchanged = 0
                for exp_id, row_values in computed.items():
                    existed = exp_id in old_facts
                    if not existed:
                        self._insert_fact(row_values)
                        inserted += 1
                        continue
                    if self._fact_payload(old_facts[exp_id]) == row_values:
                        unchanged += 1
                        continue
                    self._update_fact(exp_id, row_values)
                    updated += 1
                self._conn.execute(
                    "INSERT INTO batches(seq, sha256, byte_length, "
                    "imported_at_ms) VALUES(?, ?, ?, ?)",
                    (seq, digest, len(raw), _now_ms()),
                )
                self._incr("batches_imported", 1)
                self._incr("fact_rows_inserted", inserted)
                self._incr("fact_rows_updated", updated)
                self._incr("fact_rows_unchanged", unchanged)
                self._incr("frame_qualification_queries", frame_queries)
        except sqlite3.DatabaseError as exc:
            raise BatchError(f"working store write failed: {exc}") from exc

        return {
            "seq": seq, "noop": False,
            "facts_inserted": inserted,
            "facts_updated": updated,
            "facts_deleted": len(deleted_ids),
            "facts_unchanged": unchanged,
            "frame_qualification_queries": frame_queries,
        }

    def _apply_changes(self, payload: dict[str, Any]) -> None:
        exp_changes = payload.get("exposure_changes", [])
        if exp_changes:
            # Apply the already-validated *final* state rather than a sequence
            # of row-wise updates: removing every changed exposure first means
            # a legal same-batch swap of two start times cannot trip the
            # (session, start) uniqueness index on an intermediate state.
            changed_ids = [entry["exposure_id"] for entry in exp_changes]
            self._conn.executemany(
                "DELETE FROM exposures WHERE exposure_id=?",
                [(exp_id,) for exp_id in changed_ids],
            )
            self._conn.executemany(
                "INSERT INTO exposures(exposure_id, session_id, "
                "release_id, start_ms) VALUES(?, ?, ?, ?)",
                [
                    (entry["exposure_id"], entry["session_id"],
                     entry["release_id"], entry["start_ms"])
                    for entry in exp_changes
                    if entry["op"] != "delete"
                ],
            )
        for entry in payload.get("event_changes", []):
            if entry.get("op", "upsert") == "delete":
                self._conn.execute(
                    "DELETE FROM event_locs WHERE event_id=?",
                    (entry["event_id"],),
                )
            else:
                self._conn.execute(
                    "INSERT INTO event_locs(event_id, session_id, "
                    "occurred_ms) VALUES(?, ?, ?) "
                    "ON CONFLICT(event_id) DO UPDATE SET "
                    "session_id=excluded.session_id, "
                    "occurred_ms=excluded.occurred_ms",
                    (entry["event_id"], entry["session_id"],
                     entry["occurred_ms"]),
                )

    def _first_error(
        self,
        exp: Exposure,
        end_ms: int | None,
        enrich: _EnrichReader,
    ) -> tuple[FirstError | None, int]:
        """Scan only event locations inside this exposure's own interval.

        The ``idx_event_window`` B-tree turns this into a bounded range read,
        never a full scan of ``event_locs``; events outside
        ``[start_ms, end_ms)`` are excluded by the index bounds.
        """
        if end_ms is None:
            rows = self._conn.execute(
                "SELECT event_id, occurred_ms FROM event_locs "
                "WHERE session_id=? AND occurred_ms>=? "
                "ORDER BY occurred_ms, event_id",
                (exp.session_id, exp.start_ms),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT event_id, occurred_ms FROM event_locs "
                "WHERE session_id=? AND occurred_ms>=? AND occurred_ms<? "
                "ORDER BY occurred_ms, event_id",
                (exp.session_id, exp.start_ms, end_ms),
            ).fetchall()
        queries = 0
        for row in rows:
            queries += 1
            frame_index = enrich.qualifying_frame(row["event_id"],
                                                  exp.release_id)
            if frame_index is not None:
                return (
                    FirstError(
                        row["event_id"], frame_index, int(row["occurred_ms"])
                    ),
                    queries,
                )
        return None, queries

    @staticmethod
    def _fact_row(
        exp: Exposure, end_ms: int | None, first: FirstError | None
    ) -> dict[str, Any]:
        return {
            "exposure_id": exp.exposure_id,
            "session_id": exp.session_id,
            "release_id": exp.release_id,
            "start_ms": exp.start_ms,
            "end_ms": end_ms,
            "first_event_id": None if first is None else first.event_id,
            "first_frame_index": None if first is None else first.frame_index,
            "first_occurred_ms": (
                None if first is None else first.occurred_ms
            ),
            "first_delay_ms": (
                None if first is None else first.delay_ms(exp.start_ms)
            ),
        }

    @staticmethod
    def _fact_payload(stored: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        keys = (
            "exposure_id", "session_id", "release_id", "start_ms", "end_ms",
            "first_event_id", "first_frame_index", "first_occurred_ms",
            "first_delay_ms",
        )
        return {key: (None if stored[key] is None else stored[key])
                for key in keys}

    def _insert_fact(self, values: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO exposure_facts(exposure_id, session_id, release_id, "
            "start_ms, end_ms, first_event_id, first_frame_index, "
            "first_occurred_ms, first_delay_ms) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(values[key] for key in (
                "exposure_id", "session_id", "release_id", "start_ms",
                "end_ms", "first_event_id", "first_frame_index",
                "first_occurred_ms", "first_delay_ms")),
        )

    def _update_fact(self, exp_id: str, values: dict[str, Any]) -> None:
        self._conn.execute(
            "UPDATE exposure_facts SET session_id=?, release_id=?, start_ms=?, "
            "end_ms=?, first_event_id=?, first_frame_index=?, "
            "first_occurred_ms=?, first_delay_ms=? WHERE exposure_id=?",
            (
                values["session_id"], values["release_id"],
                values["start_ms"], values["end_ms"],
                values["first_event_id"], values["first_frame_index"],
                values["first_occurred_ms"], values["first_delay_ms"],
                exp_id,
            ),
        )

    # ------------------------------------------------------------ export
    def export_facts(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT exposure_id, session_id, release_id, start_ms, end_ms, "
            "first_event_id, first_frame_index, first_occurred_ms, "
            "first_delay_ms FROM exposure_facts "
            "ORDER BY session_id, start_ms, exposure_id"
        ).fetchall()
        return [self._fact_payload(r) for r in rows]

    def explain_enrich_plan(self, event_id: str, release_id: str) -> str:
        return self._require_enrich().explain_frame_scan_plan(
            event_id, release_id
        )


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)
