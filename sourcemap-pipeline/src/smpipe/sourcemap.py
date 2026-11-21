"""ECMA-426 / Source Map v3 parsing with strict validation.

Supported:

* ``version == 3`` plain maps with a ``mappings`` string;
* sectioned maps whose sections carry *inline* ``map`` objects;
* ``sourceRoot`` / ``sources`` / ``names`` / ``sourcesContent``;
* generated-only segments (a single VLQ field, i.e. no original position);
* section line/column offsets, including zero-line offset with a column
  shift and later lines without a column shift.

Explicitly rejected instead of silently ignored:

* sections referencing an external ``url``;
* out-of-order or overlapping sections;
* corrupt VLQ, out-of-range source/name indexes;
* nested sectioned maps beyond a fixed depth limit ("parse loop" guard);
* any map whose ``version`` is not 3.

The parser is streaming: :func:`parse_map` returns metadata plus an iterator
of segment rows so that very large maps never have to be materialised.
"""

from __future__ import annotations

import json
import posixpath
import re
from dataclasses import dataclass, field
from typing import Iterator
from urllib.parse import urlsplit, urljoin

from .errors import (
    BadVLQ,
    ExternalSection,
    InvalidSourceMap,
    MapVersionUnsupported,
    SectionOrder,
    SectionOverlap,
)
from .vlq import decode_mappings

MAX_SECTION_DEPTH = 64
_URL_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")
SegmentRow = tuple[int, int, int | None, int | None, int | None, int | None]
# (gen_line, gen_col, src_index | None, src_line | None,
#  src_col | None, name_index | None)


@dataclass
class MapMeta:
    source_root: str | None
    sources: list[str] = field(default_factory=list)
    names: list[str] = field(default_factory=list)
    sources_content: list[str | None] = field(default_factory=list)
    sectioned: bool = False
    section_count: int = 1
    # For every flattened segment's global source index, the section it came
    # from and that section's local source index (for explain output).
    section_of_source: list[int] = field(default_factory=list)
    local_source_index: list[int] = field(default_factory=list)


def is_absolute_url(value: str) -> bool:
    return bool(_URL_RE.match(value)) or value.startswith("//")


def resolve_source(source_root: str | None, source: str) -> str:
    """Resolve one ``sources`` entry against ``sourceRoot`` (pure string work).

    No network access is involved; the function only normalises identities so
    that manifest bindings and group keys are stable.
    """
    if not isinstance(source, str) or source == "":
        raise InvalidSourceMap("sources entries must be non-empty strings")
    if source_root in (None, ""):
        return _normalize_path(source)
    if not isinstance(source_root, str):
        raise InvalidSourceMap("sourceRoot must be a string or null")

    if is_absolute_url(source):
        return source
    if source.startswith("/"):
        if is_absolute_url(source_root):
            parts = urlsplit(source_root)
            return f"{parts.scheme}://{parts.netloc}{_normalize_path(source)}"
        return _normalize_path(source)
    if is_absolute_url(source_root):
        return _normalize_url(urljoin(source_root, source))
    # Plain POSIX path base, e.g. "./src/" or "src/".
    base = source_root if source_root.endswith("/") else posixpath.dirname(
        source_root.rstrip("/")
    ) + "/"
    merged = posixpath.join(base, source)
    return _normalize_path(merged)


def _normalize_path(path: str) -> str:
    if is_absolute_url(path):
        return _normalize_url(path)
    trailing = path.endswith("/")
    normalized = posixpath.normpath(path)
    if normalized == "." and path.startswith("."):
        normalized = ""
    if trailing and not normalized.endswith("/"):
        normalized += "/"
    return normalized or path


def _normalize_url(url: str) -> str:
    parts = urlsplit(url)
    if not parts.scheme:
        return _normalize_path(url)
    path = posixpath.normpath(parts.path) if parts.path else parts.path
    return urljoin(url, path)


def parse_map(
    raw: str | bytes | dict,
    *,
    depth: int = 0,
) -> tuple[MapMeta, Iterator[SegmentRow]]:
    """Parse a source map. See module docstring for guarantees."""
    if depth > MAX_SECTION_DEPTH:
        raise InvalidSourceMap(
            f"section nesting exceeds {MAX_SECTION_DEPTH}; refusing possible "
            "parse loop",
            reason="map_recursion_limit",
        )
    if isinstance(raw, dict):
        obj = raw
    else:
        try:
            obj = json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise InvalidSourceMap(f"map is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise InvalidSourceMap("map root must be a JSON object")

    version = obj.get("version")
    if version != 3:
        raise MapVersionUnsupported(
            f"only source map version 3 is supported, got {version!r}"
        )

    if "sections" in obj:
        if "mappings" in obj:
            raise InvalidSourceMap(
                "a map cannot declare both 'sections' and 'mappings'"
            )
        return _parse_sections(obj, depth)
    return _parse_plain(obj, source_root_override=None, depth=depth)


def _parse_plain(
    obj: dict,
    *,
    source_root_override: str | None,
    depth: int,
) -> tuple[MapMeta, Iterator[SegmentRow]]:
    if "sections" in obj:
        return _parse_sections(obj, depth)

    source_root = obj.get("sourceRoot")
    if source_root_override is not None:
        # Inline section maps must not carry their own conflicting root; the
        # standard applies the section map's own root, so simply use it.
        source_root = obj.get("sourceRoot")
    if source_root is not None and not isinstance(source_root, str):
        raise InvalidSourceMap("sourceRoot must be a string or null")

    raw_sources = obj.get("sources", [])
    if not isinstance(raw_sources, list):
        raise InvalidSourceMap("'sources' must be an array")
    sources = [resolve_source(source_root, s) for s in raw_sources]

    names = obj.get("names", [])
    if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
        raise InvalidSourceMap("'names' must be an array of strings")

    content = obj.get("sourcesContent")
    if content is not None:
        if not isinstance(content, list) or len(content) not in (
            0,
            len(sources),
        ):
            raise InvalidSourceMap(
                "'sourcesContent' must align 1:1 with 'sources'"
            )
        if not all(c is None or isinstance(c, str) for c in content):
            raise InvalidSourceMap(
                "'sourcesContent' entries must be strings or null"
            )
    else:
        content = [None] * len(sources)

    mappings = obj.get("mappings", "")
    if not isinstance(mappings, str):
        raise BadVLQ("'mappings' must be a string")
    decoded = decode_mappings(mappings)

    meta = MapMeta(
        source_root=source_root,
        sources=sources,
        names=list(names),
        sources_content=content,
        sectioned=False,
        section_count=1,
        section_of_source=[0] * len(sources),
        local_source_index=list(range(len(sources))),
    )

    def stream() -> Iterator[SegmentRow]:
        for gen_line, line_segments in enumerate(decoded):
            for seg in line_segments:
                gen_col = seg[0]
                if len(seg) == 1:
                    yield gen_line, gen_col, None, None, None, None
                    continue
                src_index, src_line, src_col = seg[1], seg[2], seg[3]
                if not 0 <= src_index < len(sources):
                    raise InvalidSourceMap(
                        f"segment references source index {src_index} but map "
                        f"only declares {len(sources)} source(s)"
                    )
                if src_line < 0 or src_col < 0:
                    raise InvalidSourceMap("negative original coordinate")
                name_index = seg[4] if len(seg) == 5 else None
                if name_index is not None and not 0 <= name_index < len(names):
                    raise InvalidSourceMap(
                        f"segment references name index {name_index} but map "
                        f"only declares {len(names)} name(s)"
                    )
                yield gen_line, gen_col, src_index, src_line, src_col, name_index

    return meta, stream()


def _parse_sections(
    obj: dict,
    depth: int,
) -> tuple[MapMeta, Iterator[SegmentRow]]:
    sections = obj.get("sections")
    if not isinstance(sections, list) or not sections:
        raise InvalidSourceMap("'sections' must be a non-empty array")

    prepared: list[tuple[dict, MapMeta, Iterator[SegmentRow]]] = []
    previous_offset: tuple[int, int] | None = None
    global_meta = MapMeta(
        source_root=obj.get("sourceRoot"),
        sectioned=True,
        section_count=len(sections),
    )

    for section_index, section in enumerate(sections):
        if not isinstance(section, dict):
            raise InvalidSourceMap(f"section {section_index} must be an object")
        if "url" in section:
            # External section maps are explicitly unsupported; never silently
            # skipped, never fetched.
            raise ExternalSection(
                f"section {section_index} references external map URL "
                f"{section.get('url')!r}; inline 'map' is required"
            )
        offset = section.get("offset")
        if (
            not isinstance(offset, dict)
            or not isinstance(offset.get("line"), int)
            or not isinstance(offset.get("column"), int)
            or offset["line"] < 0
            or offset["column"] < 0
        ):
            raise InvalidSourceMap(
                f"section {section_index} has an invalid 'offset'"
            )
        current_offset = (offset["line"], offset["column"])
        if previous_offset is not None:
            if current_offset < previous_offset:
                raise SectionOrder(
                    f"section {section_index} offset {current_offset} is "
                    f"before previous offset {previous_offset}"
                )
            if current_offset == previous_offset:
                raise SectionOverlap(
                    f"section {section_index} starts exactly at the previous "
                    f"section offset {current_offset}"
                )
        previous_offset = current_offset

        inner = section.get("map")
        if not isinstance(inner, dict):
            raise InvalidSourceMap(
                f"section {section_index} must contain an inline 'map' object"
            )
        inner_meta, inner_stream = parse_map(inner, depth=depth + 1)

        source_offset = len(global_meta.sources)
        name_offset = len(global_meta.names)
        global_meta.sources.extend(inner_meta.sources)
        global_meta.names.extend(inner_meta.names)
        global_meta.sources_content.extend(inner_meta.sources_content)
        global_meta.section_of_source.extend(
            [section_index] * len(inner_meta.sources)
        )
        global_meta.local_source_index.extend(inner_meta.local_source_index)
        prepared.append(
            (
                {
                    "index": section_index,
                    "offset_line": current_offset[0],
                    "offset_col": current_offset[1],
                    "source_offset": source_offset,
                    "name_offset": name_offset,
                },
                inner_meta,
                inner_stream,
            )
        )

    def stream() -> Iterator[SegmentRow]:
        seen_coordinates: set[tuple[int, int]] = set()
        for info, _meta, rows in prepared:
            off_line = info["offset_line"]
            off_col = info["offset_col"]
            src_off = info["source_offset"]
            name_off = info["name_offset"]
            for gen_line, gen_col, src_i, src_line, src_col, name_i in rows:
                line = gen_line + off_line
                # Column offset only applies to segments on the section's
                # first generated line; later lines keep their own columns.
                col = gen_col + (off_col if gen_line == 0 else 0)
                coordinate = (line, col)
                if coordinate in seen_coordinates:
                    raise SectionOverlap(
                        f"sections produce duplicate mapping at generated "
                        f"({line},{col})"
                    )
                seen_coordinates.add(coordinate)
                if src_i is None:
                    yield line, col, None, None, None, None
                else:
                    yield (
                        line,
                        col,
                        src_i + src_off,
                        src_line,
                        src_col,
                        None if name_i is None else name_i + name_off,
                    )

    return global_meta, stream()


def materialize(
    rows: Iterator[SegmentRow],
) -> list[list[SegmentRow]]:
    """Collect segment rows into per-generated-line lists (small maps/tests)."""
    lines: list[list[SegmentRow]] = []
    for row in rows:
        gen_line = row[0]
        while len(lines) <= gen_line:
            lines.append([])
        lines[gen_line].append(row)
    return lines
