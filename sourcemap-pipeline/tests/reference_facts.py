"""Independent reference model for the exposure first-error fact table.

This module deliberately shares no code with :mod:`smpipe.facts`: it keeps
only the *final* inputs (exposures, event locations and the sealed frame
results) in plain Python containers and recomputes every fact from scratch
after each batch.  The production store instead maintains facts
incrementally; the differential tests feed both sides the same batches and
demand byte-identical fact rows after every step.

Frames are supplied as a mapping ``event_id -> list[(frame_index, release_id,
status)]`` read once from the sealed enrichment database through a generic
``sqlite3`` connection, mirroring exactly what the production side is allowed
to rely on.
"""

from __future__ import annotations

import sqlite3
from bisect import bisect_left
from typing import Any


class ReferenceFacts:
    def __init__(self, frame_rows: dict[str, list[tuple[int, str, str]]]):
        self._frames = frame_rows
        self.exposures: dict[str, dict[str, Any]] = {}
        self.events: dict[str, dict[str, Any]] = {}

    @classmethod
    def from_enrich_db(cls, path: str) -> "ReferenceFacts":
        conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
        try:
            rows = conn.execute(
                "SELECT event_id, frame_index, release_id, result_json "
                "FROM frames"
            ).fetchall()
        finally:
            conn.close()
        import json
        frames: dict[str, list[tuple[int, str, str]]] = {}
        for event_id, frame_index, release_id, result_json in rows:
            status = json.loads(result_json)["status"]
            frames.setdefault(event_id, []).append(
                (frame_index, release_id, status)
            )
        return cls(frames)

    # ------------------------------------------------------------ projection
    def apply(self, payload: dict[str, Any]) -> None:
        for entry in payload.get("exposure_changes", []):
            key = entry["exposure_id"]
            if entry.get("op", "upsert") == "delete":
                del self.exposures[key]
            else:
                self.exposures[key] = {
                    "exposure_id": key,
                    "session_id": entry["session_id"],
                    "release_id": entry["release_id"],
                    "start_ms": entry["start_ms"],
                }
        for entry in payload.get("event_changes", []):
            key = entry["event_id"]
            if entry.get("op", "upsert") == "delete":
                del self.events[key]
            else:
                self.events[key] = {
                    "event_id": key,
                    "session_id": entry["session_id"],
                    "occurred_ms": entry["occurred_ms"],
                }

    # ------------------------------------------------------------- from-scratch
    def _timelines(self) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for exp in self.exposures.values():
            grouped.setdefault(exp["session_id"], []).append(exp)
        for rows in grouped.values():
            rows.sort(key=lambda item: (item["start_ms"],
                                        item["exposure_id"]))
        return grouped

    def _first_qualifying_frame(
        self, event_id: str, release_id: str
    ) -> int | None:
        best: int | None = None
        for frame_index, frame_release, status in self._frames.get(event_id, []):
            if frame_release == release_id and status == "mapped":
                if best is None or frame_index < best:
                    best = frame_index
        return best

    def compute(self) -> list[dict[str, Any]]:
        timelines = self._timelines()
        facts: list[dict[str, Any]] = []
        for session_id, timeline in timelines.items():
            starts = [row["start_ms"] for row in timeline]
            session_events = sorted(
                (row for row in self.events.values()
                 if row["session_id"] == session_id),
                key=lambda row: (row["occurred_ms"], row["event_id"]),
            )
            for pos, exp in enumerate(timeline):
                end_ms = (
                    timeline[pos + 1]["start_ms"]
                    if pos + 1 < len(timeline) else None
                )
                chosen_event = None
                chosen_frame = None
                chosen_time = None
                for evt in session_events:
                    occurred = evt["occurred_ms"]
                    if occurred < exp["start_ms"]:
                        continue
                    if end_ms is not None and occurred >= end_ms:
                        break
                    # Unique effective exposure via the same half-open rule.
                    idx = bisect_left(starts, occurred)
                    if idx < len(starts) and starts[idx] == occurred:
                        effective = timeline[idx]
                    else:
                        effective = timeline[idx - 1] if idx > 0 else None
                    if effective is None or \
                            effective["exposure_id"] != exp["exposure_id"]:
                        continue
                    frame_index = self._first_qualifying_frame(
                        evt["event_id"], exp["release_id"]
                    )
                    if frame_index is None:
                        continue  # no fall-back to the previous exposure
                    chosen_event = evt["event_id"]
                    chosen_frame = frame_index
                    chosen_time = occurred
                    break
                facts.append({
                    "exposure_id": exp["exposure_id"],
                    "session_id": session_id,
                    "release_id": exp["release_id"],
                    "start_ms": exp["start_ms"],
                    "end_ms": end_ms,
                    "first_event_id": chosen_event,
                    "first_frame_index": chosen_frame,
                    "first_occurred_ms": chosen_time,
                    "first_delay_ms": (
                        None if chosen_time is None
                        else chosen_time - exp["start_ms"]
                    ),
                })
        facts.sort(key=lambda row: (row["session_id"], row["start_ms"],
                                    row["exposure_id"]))
        return facts
