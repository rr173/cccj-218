"""Build ingestion, frame enrichment, explain and grouping orchestration.

Design summary
--------------
* ``ingest_manifest`` reads one local build manifest, pins every artefact by
  SHA-256, parses and indexes the bound source maps into the on-disk store,
  and refuses any binding that is not declared explicitly.
* ``enrich_frame`` walks zero or more map layers.  Each layer records the
  artefact/map digest, the exact segment hit, any column-unit conversion and
  the binding that justified the next hop.
* Grouping is computed from stored results, so replacing the input directory
  after ingestion changes nothing.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from . import columns as colmod
from .errors import (
    DigestMismatch,
    InvalidFrame,
    InvalidSourceMap,
    ManifestError,
    MissingArtifact,
    SmPipeError,
    SourceContentMismatch,
)
from .sourcemap import MapMeta, parse_map
from .store import NO_ORIGINAL, Store, sha256_bytes, sha256_text

# Final frame states.
MAPPED = "mapped"
UNMAPPED = "unmapped"
MISSING_ARTIFACT = "missing_artifact"
INVALID_MAP = "invalid_map"
CYCLE = "cycle"

MAX_LAYERS = 32


# --------------------------------------------------------------------- helpers
def _read_local(path: str, *, manifest_dir: str) -> bytes:
    """Read a file that must live inside the local build material directory."""
    if not isinstance(path, str) or path == "":
        raise ManifestError("file path must be a non-empty string")
    if "://" in path.split(os.sep)[0] or path.startswith(("http://", "https://",
                                                           "//")):
        raise ManifestError(
            f"refusing to fetch non-local reference {path!r}; all material "
            "must be provided inside the build directory"
        )
    full = os.path.normpath(os.path.join(manifest_dir, path))
    root = os.path.normpath(manifest_dir)
    if os.path.commonpath((full, root)) != root:
        raise ManifestError(
            f"path {path!r} escapes the build directory {root!r}"
        )
    if not os.path.isfile(full):
        raise MissingArtifact(f"local file not found: {path}")
    with open(full, "rb") as handle:
        return handle.read()


def _decode_utf8(data: bytes, what: str) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ManifestError(f"{what} is not valid UTF-8: {exc}") from exc


# ------------------------------------------------------------------ ingestion
@dataclass
class IngestReport:
    release_id: str
    artifacts: int
    maps_indexed: int
    maps_rejected: dict[str, str]
    bindings: int
    sources_pinned: int
    segments_total: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "artifacts": self.artifacts,
            "maps_indexed": self.maps_indexed,
            "maps_rejected": self.maps_rejected,
            "bindings": self.bindings,
            "sources_pinned": self.sources_pinned,
            "segments_total": self.segments_total,
        }


def ingest_manifest(store: Store, manifest_path: str) -> IngestReport:
    """Ingest one release manifest plus all locally bundled build material."""
    manifest_dir = os.path.dirname(os.path.abspath(manifest_path))
    with open(manifest_path, "rb") as handle:
        raw = handle.read()
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ManifestError("manifest root must be a JSON object")

    release_id = manifest.get("release_id")
    if not isinstance(release_id, str) or not release_id:
        raise ManifestError("manifest requires a non-empty release_id")
    store.register_release(release_id)

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ManifestError("manifest requires a non-empty artifacts array")

    report = IngestReport(
        release_id=release_id,
        artifacts=0,
        maps_indexed=0,
        maps_rejected={},
        bindings=0,
        sources_pinned=0,
        segments_total=0,
    )
    # First pass: pin artefacts, blobs, aliases and parse/index their maps.
    for entry in artifacts:
        report.artifacts += 1
        _ingest_artifact(store, release_id, entry, manifest_dir, report)

    # Second pass: explicit bindings for intermediate sources.  Binding
    # targets must already exist as artefacts in the same release.
    for entry in artifacts:
        for binding in entry.get("source_bindings", []) or []:
            _ingest_binding(store, release_id, entry, binding)
            report.bindings += 1

    # Third pass: pinned terminal source bodies + sourcesContent cross-checks.
    for source in manifest.get("sources", []) or []:
        _ingest_source(store, release_id, source, manifest_dir, report)

    return report


def _ingest_artifact(
    store: Store,
    release_id: str,
    entry: dict,
    manifest_dir: str,
    report: IngestReport,
) -> None:
    if not isinstance(entry, dict):
        raise ManifestError("artifact entries must be objects")
    artifact_id = entry.get("artifact_id")
    sha256 = entry.get("sha256")
    body_path = entry.get("path")
    if not (isinstance(artifact_id, str) and isinstance(sha256, str)
            and isinstance(body_path, str)):
        raise ManifestError(
            f"artifact {entry!r} requires artifact_id, sha256 and path"
        )

    body = _read_local(body_path, manifest_dir=manifest_dir)
    actual = sha256_bytes(body)
    if actual != sha256:
        raise DigestMismatch(
            f"artifact {artifact_id!r}: manifest pins {sha256} but file "
            f"digest is {actual}"
        )
    store.put_blob(actual, "artifact", body)

    map_digest: str | None = None
    map_ref = entry.get("source_map")
    if map_ref is not None:
        map_digest = _ingest_map(store, map_ref, manifest_dir, release_id,
                                 artifact_id, report)

    store.put_artifact(
        release_id, artifact_id, sha256, actual, map_digest
    )
    for url in entry.get("url_aliases", []) or []:
        if not isinstance(url, str) or not url:
            raise ManifestError(f"artifact {artifact_id!r}: invalid URL alias")
        store.put_url_alias(release_id, url, artifact_id)


def _ingest_map(
    store: Store,
    map_ref: Any,
    manifest_dir: str,
    release_id: str,
    artifact_id: str,
    report: IngestReport,
) -> str:
    if not isinstance(map_ref, dict):
        raise ManifestError(
            f"artifact {artifact_id!r}: source_map must be an object"
        )
    map_path = map_ref.get("path")
    pinned = map_ref.get("sha256")
    if not isinstance(map_path, str) or not isinstance(pinned, str):
        raise ManifestError(
            f"artifact {artifact_id!r}: source_map requires path and sha256"
        )
    data = _read_local(map_path, manifest_dir=manifest_dir)
    map_digest = sha256_bytes(data)
    if map_digest != pinned:
        raise DigestMismatch(
            f"source map for artifact {artifact_id!r}: manifest pins {pinned} "
            f"but file digest is {map_digest}"
        )

    if store.get_map_meta(map_digest) is not None:
        return map_digest  # shared map, already indexed

    try:
        meta, segment_stream = parse_map(data)
        store.put_blob(map_digest, "sourcemap", data)
        # Stream straight into the B-tree so a million-segment map never has
        # to exist as one in-memory list.
        stored = store.insert_segments(map_digest, segment_stream)
        conflicts = store.conflicting_coordinates(map_digest)
        if conflicts:
            line, col = conflicts[0]
            raise InvalidSourceMap(
                f"two inconsistent mappings share generated coordinate "
                f"({line},{col})",
                reason="inconsistent_mapping",
            )
        _index_map_sources(store, map_digest, meta)
        store.put_map_meta(
            map_digest,
            valid=True,
            sectioned=meta.sectioned,
            section_count=meta.section_count,
            source_root=meta.source_root,
            segment_count=stored,
            error_reason=None,
        )
        report.maps_indexed += 1
        report.segments_total += stored
    except InvalidSourceMap as exc:
        # Keep the invalid map's bytes and record why; frames hitting it get
        # a deterministic invalid_map result instead of a missing_map guess.
        store.put_blob(map_digest, "sourcemap", data)
        store.delete_segments(map_digest)
        store.put_map_meta(
            map_digest,
            valid=False,
            sectioned=False,
            section_count=0,
            source_root=None,
            segment_count=0,
            error_reason=exc.reason,
        )
        report.maps_rejected[map_digest] = exc.reason
    return map_digest


def _index_map_sources(store: Store, map_digest: str, meta: MapMeta) -> None:
    for index, identity in enumerate(meta.sources):
        inline_digest: str | None = None
        content = meta.sources_content[index]
        if content is not None:
            inline_digest = sha256_text(content)
            store.put_blob(inline_digest, "source", content.encode("utf-8"))
        store.put_map_source(
            map_digest,
            index,
            identity,
            meta.section_of_source[index],
            meta.local_source_index[index],
            inline_digest,
        )


def _ingest_binding(
    store: Store,
    release_id: str,
    owner_entry: dict,
    binding: Any,
) -> None:
    if not isinstance(binding, dict):
        raise ManifestError("source_bindings entries must be objects")
    owner_artifact = owner_entry["artifact_id"]
    owner = store.get_artifact(release_id, owner_artifact)
    if owner is None or owner["map_digest"] is None:
        raise ManifestError(
            f"binding on artifact {owner_artifact!r} requires a bound map"
        )
    map_digest = owner["map_digest"]
    identity = binding.get("source_identity")
    next_artifact_id = binding.get("next_artifact_id")
    if not isinstance(identity, str) or not isinstance(next_artifact_id, str):
        raise ManifestError(
            f"binding {binding!r} requires source_identity and next_artifact_id"
        )
    source_row = store.get_map_source(map_digest,
                                      binding.get("source_index", -1))
    # Bindings may name either the resolved source identity or an explicit
    # source index; either way the identity must actually occur in the map.
    identities = {
        r["source_identity"]
        for r in store._conn.execute(  # noqa: SLF001 - internal store query
            "SELECT source_identity FROM map_sources WHERE map_digest=?",
            (map_digest,),
        )
    }
    if "source_index" in binding:
        if source_row is None:
            raise ManifestError(
                f"binding source_index {binding['source_index']} does not "
                f"exist in map of {owner_artifact!r}"
            )
        identity = source_row["source_identity"]
    elif identity not in identities:
        raise ManifestError(
            f"binding source_identity {identity!r} does not appear in map of "
            f"{owner_artifact!r}; same-name guessing is forbidden"
        )
    target = store.get_artifact(release_id, next_artifact_id)
    if target is None:
        raise ManifestError(
            f"binding target {next_artifact_id!r} is not an artefact of "
            f"release {release_id!r}"
        )
    store.put_binding(
        release_id, map_digest, identity, next_artifact_id,
        target["map_digest"],
    )


def _ingest_source(
    store: Store,
    release_id: str,
    source: Any,
    manifest_dir: str,
    report: IngestReport,
) -> None:
    if not isinstance(source, dict):
        raise ManifestError("sources entries must be objects")
    identity = source.get("source_identity")
    sha = source.get("sha256")
    path = source.get("path")
    if not (isinstance(identity, str) and isinstance(sha, str)
            and isinstance(path, str)):
        raise ManifestError(
            f"source {source!r} requires source_identity, sha256 and path"
        )
    data = _read_local(path, manifest_dir=manifest_dir)
    actual = sha256_bytes(data)
    if actual != sha:
        raise DigestMismatch(
            f"source {identity!r}: manifest pins {sha} but file digest is "
            f"{actual}"
        )
    store.put_blob(actual, "source", data)
    store.put_source_pin(identity, actual)
    report.sources_pinned += 1

    # Cross-check any sourcesContent entries that map to the same source
    # identity: an inline body must be byte-identical to the pinned body.
    rows = store._conn.execute(  # noqa: SLF001
        "SELECT map_digest, source_index, inline_content_digest FROM "
        "map_sources WHERE source_identity=? AND inline_content_digest IS NOT "
        "NULL",
        (identity,),
    ).fetchall()
    for row in rows:
        if row["inline_content_digest"] != actual:
            raise SourceContentMismatch(
                f"sourcesContent of {identity!r} in map "
                f"{row['map_digest'][:12]}... has digest "
                f"{row['inline_content_digest']} but manifest pins {actual}"
            )


# ----------------------------------------------------------------- enrichment
def enrich_frame(store: Store, frame: dict) -> dict:
    """Enrich one normalized stack frame into a fully traceable result."""
    event_id = frame.get("event_id")
    frame_index = frame.get("frame_index")
    release_id = frame.get("release_id")
    url = frame.get("url")
    line = frame.get("line")
    column = frame.get("column")
    unit = frame.get("column_unit")
    error_type = frame.get("error_type")

    for field, value in (
        ("event_id", event_id),
        ("release_id", release_id),
        ("url", url),
    ):
        if not isinstance(value, str):
            raise InvalidFrame(f"frame field {field!r} must be a string")
    if not isinstance(frame_index, int) or isinstance(frame_index, bool):
        raise InvalidFrame("frame field 'frame_index' must be an integer")
    if not isinstance(line, int) or isinstance(line, bool):
        raise InvalidFrame("frame line must be an integer")
    if not isinstance(column, int) or isinstance(column, bool):
        raise InvalidFrame("frame column must be an integer")
    if unit not in colmod.VALID_UNITS:
        raise InvalidFrame(
            f"column_unit must be one of {sorted(colmod.VALID_UNITS)}"
        )

    result: dict[str, Any] = {
        "event_id": event_id,
        "frame_index": frame_index,
        "release_id": release_id,
        "error_type": error_type,
        "input": {
            "url": url,
            "line": line,
            "column": column,
            "column_unit": unit,
        },
        "status": None,
        "reason": None,
        "layers": [],
        "final": None,
        "group_key": None,
    }

    artifact_id = store.resolve_alias(release_id, url)
    if artifact_id is None:
        result["status"] = MISSING_ARTIFACT
        result["reason"] = "url_not_bound_in_release"
        result["group_key"] = _failure_key(result)
        return result
    artifact = store.get_artifact(release_id, artifact_id)
    assert artifact is not None
    # Unmapped/failure groups must retain the exact artefact identity so two
    # releases serving the same URL never collapse into one issue.
    result["artifact_id"] = artifact_id
    result["artifact_digest"] = artifact["sha256"]
    if artifact["map_digest"] is None:
        result["status"] = UNMAPPED
        result["reason"] = "artifact_has_no_source_map"
        result["group_key"] = _failure_key(result)
        return result

    line_index = store.get_line_index(artifact["content_digest"])
    if line_index is None:  # covered blobs are always stored; defensive only
        result["status"] = MISSING_ARTIFACT
        result["reason"] = "artifact_body_not_stored"
        result["group_key"] = _failure_key(result)
        return result

    try:
        zero_line, utf16_col = colmod.external_to_utf16_zero_based(
            line_index,
            line_one_based=line,
            column_one_based=column,
            unit=unit,
        )
    except SmPipeError as exc:
        result["status"] = "invalid_frame"
        result["reason"] = exc.reason
        result["group_key"] = _failure_key(result)
        return result

    conversion = {
        "from_unit": unit,
        "input_line_one_based": line,
        "input_column_one_based": column,
        "zero_based_utf16_line": zero_line,
        "zero_based_utf16_column": utf16_col,
    }

    current_artifact = dict(artifact)
    current_artifact_id = artifact_id
    current_line, current_col = zero_line, utf16_col
    current_unit = colmod.UTF16
    visited: set[tuple[str, str]] = set()
    terminal_source_identity: str | None = None
    terminal_name: str | None = None

    for layer_index in range(MAX_LAYERS):
        map_digest = current_artifact["map_digest"]
        signature = (current_artifact_id, map_digest or "")
        if signature in visited:
            result["status"] = CYCLE
            result["reason"] = "binding_cycle"
            result["group_key"] = _failure_key(result)
            return result
        visited.add(signature)

        meta = store.get_map_meta(map_digest)
        layer: dict[str, Any] = {
            "layer": layer_index,
            "artifact_id": current_artifact_id,
            "artifact_digest": current_artifact["sha256"],
            "map_digest": map_digest,
            "queried_zero_based": {
                "line": current_line,
                "column": current_col,
                "unit": current_unit,
            },
        }
        if layer_index == 0:
            layer["column_unit_conversion"] = conversion

        if meta is None or not meta["valid"]:
            reason = meta["error_reason"] if meta is not None else "map_not_stored"
            layer["outcome"] = "invalid_map"
            layer["reason"] = reason
            result["layers"].append(layer)
            result["status"] = INVALID_MAP
            result["reason"] = reason
            result["group_key"] = _failure_key(result)
            return result

        hit = store.lookup_segment(map_digest, current_line, current_col)
        if hit is None:
            # No segment at or before the query column on *this* line.  We do
            # not borrow the previous line.
            layer["outcome"] = "unmapped"
            layer["reason"] = "no_segment_on_generated_line"
            layer["hit_segment"] = None
            result["layers"].append(layer)
            result["status"] = UNMAPPED
            result["reason"] = "no_segment_on_generated_line"
            result["group_key"] = _failure_key(result)
            return result

        (_gl, gen_col, src_index, src_line, src_col, name_index) = hit
        hit_record = {
            "generated_line": _gl,
            "generated_column": gen_col,
            "has_original_position": src_index != NO_ORIGINAL,
        }
        source_row = None
        if src_index != NO_ORIGINAL:
            source_row = store.get_map_source(map_digest, src_index)
            hit_record["source_index"] = src_index
            hit_record["source_identity"] = source_row["source_identity"]
            hit_record["original_line"] = src_line
            hit_record["original_column"] = src_col
            hit_record["section_index"] = source_row["section_index"]
            hit_record["local_source_index"] = source_row["local_source_index"]
            if name_index != NO_ORIGINAL:
                map_names = store.get_map_names(map_digest)
                if 0 <= name_index < len(map_names):
                    hit_record["name"] = map_names[name_index]
                    terminal_name = map_names[name_index]
        layer["hit_segment"] = hit_record

        if src_index == NO_ORIGINAL:
            # Generated-only segment: unmapped, and we must not fall back to an
            # earlier segment on the same line.
            layer["outcome"] = "unmapped"
            layer["reason"] = "segment_without_original_position"
            result["layers"].append(layer)
            result["status"] = UNMAPPED
            result["reason"] = "segment_without_original_position"
            result["group_key"] = _failure_key(result)
            return result

        identity = source_row["source_identity"]
        binding = store.get_binding(release_id, map_digest, identity)
        if binding is None:
            # Terminal source.  Missing source *body* does not undo the
            # correctly resolved original position.
            layer["outcome"] = "mapped_terminal"
            layer["next_binding"] = None
            result["layers"].append(layer)
            terminal_source_identity = identity
            final_line, final_col = src_line, src_col
            final_blob = _resolve_source_body(store, map_digest, source_row)
            snippet = None
            body_digest = None
            if final_blob is not None:
                body_digest, body_text = final_blob
                snippet = _snippet(
                    colmod.LineIndex(body_text), final_line, final_col
                )
            result["status"] = MAPPED
            result["reason"] = (
                "mapped_with_snippet" if snippet is not None
                else "mapped_source_body_missing"
            )
            result["final"] = {
                "source_identity": identity,
                "release_id": release_id,
                "line_zero_based": final_line,
                "column_zero_based": final_col,
                "line_one_based": final_line + 1,
                "column_one_based": final_col + 1,
                "name": terminal_name,
                "source_body_digest": body_digest,
                "snippet": snippet,
            }
            result["group_key"] = _mapped_key(
                release_id, identity, body_digest, final_line, final_col,
                error_type,
            )
            return result

        # Continue to the next layer using the segment's *explicit* original
        # line/column -- never the query column plus a delta.
        next_artifact_id = binding["next_artifact_id"]
        next_artifact = store.get_artifact(release_id, next_artifact_id)
        if next_artifact is None:
            layer["outcome"] = "missing_artifact"
            layer["reason"] = "bound_artifact_absent"
            result["layers"].append(layer)
            result["status"] = MISSING_ARTIFACT
            result["reason"] = "bound_artifact_absent"
            result["group_key"] = _failure_key(result)
            return result
        layer["outcome"] = "continue"
        layer["next_binding"] = {
            "source_identity": identity,
            "next_artifact_id": next_artifact_id,
            "next_artifact_digest": next_artifact["sha256"],
            "next_map_digest": next_artifact["map_digest"],
        }
        result["layers"].append(layer)
        current_artifact = dict(next_artifact)
        current_artifact_id = next_artifact_id
        current_line, current_col = src_line, src_col

    result["status"] = CYCLE
    result["reason"] = "layer_limit_exceeded"
    result["group_key"] = _failure_key(result)
    return result


def _resolve_source_body(store: Store, map_digest: str, source_row: Any):
    """Return ``(digest, text)`` for a terminal source, or None if absent.

    Preference order:

    1. a manifest-pinned body for the resolved source identity (authoritative);
    2. an inline ``sourcesContent`` blob, only when no pin exists.

    A missing body never invalidates the resolved original position; the
    caller records ``mapped_source_body_missing`` instead of inventing text.
    """
    identity = source_row["source_identity"]
    pinned = store.get_source_pin(identity)
    if pinned is not None:
        text = store.get_text(pinned)
        if text is not None:
            inline = source_row["inline_content_digest"]
            if inline is not None and inline != pinned:
                # Ingestion normally rejects this; keep the guard at query
                # time so an older store cannot display an inconsistent body.
                raise SourceContentMismatch(
                    f"sourcesContent digest {inline} disagrees with pinned "
                    f"body digest {pinned} for {identity!r}"
                )
            return pinned, text
    inline = source_row["inline_content_digest"]
    if inline is not None:
        text = store.get_text(inline)
        if text is not None:
            return inline, text
    return None


def _snippet(line_index: "colmod.LineIndex", zero_line: int,
             zero_col: int) -> dict | None:
    text = line_index.get_line(zero_line)
    if text is None:
        return None
    if zero_col > len(text):
        return None
    return {
        "line_text": text,
        "line_number_one_based": zero_line + 1,
        "column_zero_based": zero_col,
    }


def _mapped_key(release_id, identity, body_digest, line, col, error_type):
    return {
        "kind": MAPPED,
        "release_id": release_id,
        "source_identity": identity,
        "source_body_digest": body_digest,
        "line_zero_based": line,
        "column_zero_based": col,
        "error_type": error_type,
    }


def _failure_key(result: dict) -> dict:
    key = {
        "kind": result["status"],
        "reason": result["reason"],
        "release_id": result["release_id"],
        "url": result["input"]["url"],
        "artifact_id": result.get("artifact_id"),
        "artifact_digest": result.get("artifact_digest"),
    }
    # Keep the original generated coordinate so different failures stay apart.
    key["generated_line_one_based"] = result["input"]["line"]
    key["generated_column_one_based"] = result["input"]["column"]
    key["error_type"] = result.get("error_type")
    return key


# -------------------------------------------------------------------- grouping
def compute_groups(store: Store) -> dict[str, Any]:
    """Group stored enrichment results with reviewable event counts."""
    groups: dict[str, dict[str, Any]] = {}
    for row in store.iter_frames():
        if row["result_json"] is None:
            continue
        result = json.loads(row["result_json"])
        key = result["group_key"]
        if key is None:
            continue
        key_text = store.dump_json(key)
        bucket = groups.setdefault(
            key_text,
            {"group_key": key, "events": set(), "frames": 0,
             "example_event": result["event_id"]},
        )
        # The same event may hit the same final location in multiple frames;
        # count each event only once per group.
        bucket["events"].add(result["event_id"])
        bucket["frames"] += 1

    output = []
    for bucket in groups.values():
        events = sorted(bucket["events"])
        output.append({
            "group_key": bucket["group_key"],
            "event_count": len(events),
            "frame_count": bucket["frames"],
            "events": events,
            "example_event": bucket["example_event"],
        })
    output.sort(key=lambda item: store.dump_json(item["group_key"]))
    return {
        "group_count": len(output),
        "total_events": len({
            e for item in output for e in item["events"]
        }),
        "groups": output,
    }


def ingest_frames(store: Store, frames_path: str) -> dict[str, int]:
    """Read a JSONL frame file and enrich every frame."""
    counts = {"new": 0, "duplicate": 0, "rejected": 0}
    errors: list[dict[str, str]] = []
    with open(frames_path, "rb") as handle:
        raw = handle.read()
    for lineno, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            frame = json.loads(line)
        except ValueError as exc:
            raise InvalidFrame(f"line {lineno}: invalid JSON: {exc}") from exc
        raw_hash = sha256_text(
            json.dumps(frame, sort_keys=True, ensure_ascii=False)
        )
        try:
            state = store.check_frame_input(
                frame["event_id"], frame["frame_index"], raw_hash
            )
        except KeyError as exc:
            raise InvalidFrame(f"line {lineno}: missing field {exc}") from exc
        if state == "duplicate":
            counts["duplicate"] += 1
            continue
        try:
            result = enrich_frame(store, frame)
        except SmPipeError as exc:
            counts["rejected"] += 1
            errors.append({
                "line": str(lineno),
                "event_id": str(frame.get("event_id")),
                "frame_index": str(frame.get("frame_index")),
                "reason": exc.reason,
                "message": str(exc),
            })
            continue
        store.put_frame(
            frame["event_id"], frame["frame_index"], raw_hash,
            frame["release_id"], json.dumps(frame, ensure_ascii=False),
            json.dumps(result, ensure_ascii=False, sort_keys=True),
        )
        counts["new"] += 1
    counts["input_errors"] = errors
    return counts
