"""Independent reference VLQ decoder used *only* by the test suite.

This is deliberately written in a different style from
:mod:`smpipe.vlq`: a single flat character loop with no shared helper code,
so that agreement between the two decoders is meaningful evidence rather
than self-congratulation.  It returns absolute segment tuples in the same
shape (per generated line) as the production decoder.
"""

from __future__ import annotations

B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
VALUE = {c: i for i, c in enumerate(B64)}


def _vlq_stream(text: str):
    """Yield signed integers from a mappings fragment without separators."""
    i = 0
    while i < len(text):
        accumulator = 0
        bit_position = 0
        while True:
            if i >= len(text):
                raise ValueError("dangling continuation bit")
            digit = VALUE.get(text[i])
            if digit is None:
                raise ValueError(f"illegal character {text[i]!r}")
            i += 1
            accumulator |= (digit & 31) << bit_position
            bit_position += 5
            if not (digit & 32):
                break
        sign = accumulator & 1
        magnitude = accumulator >> 1
        yield -magnitude if sign else magnitude


def reference_decode(mappings: str) -> list[list[tuple[int, ...]]]:
    """Flat reference implementation of the v3 mappings grammar."""
    result: list[list[tuple[int, ...]]] = [[]]
    # Relative registers reset on every generated line boundary.
    gen_col = src_idx = src_line = src_col = name_idx = 0
    # Walk segments manually so that ';' and ',' are handled explicitly.
    segment_start = 0
    i = 0
    line_segments: list[tuple[int, ...]] = []

    def finish_segment(raw: str) -> tuple[int, ...]:
        nonlocal gen_col, src_idx, src_line, src_col, name_idx
        if raw == "":
            raise ValueError("empty segment")
        numbers = list(_vlq_stream(raw))
        gen_col += numbers[0]
        if gen_col < 0:
            raise ValueError("negative generated column")
        segment = [gen_col]
        if len(numbers) > 1:
            if len(numbers) not in (4, 5):
                raise ValueError("segment must have 1, 4 or 5 fields")
            src_idx += numbers[1]
            src_line += numbers[2]
            src_col += numbers[3]
            if min(src_idx, src_line, src_col) < 0:
                raise ValueError("negative absolute source coordinate")
            segment.extend((src_idx, src_line, src_col))
            if len(numbers) == 5:
                name_idx += numbers[4]
                if name_idx < 0:
                    raise ValueError("negative name index")
                segment.append(name_idx)
        return tuple(segment)

    while i < len(mappings):
        ch = mappings[i]
        if ch == ",":
            line_segments.append(finish_segment(mappings[segment_start:i]))
            i += 1
            segment_start = i
            continue
        if ch == ";":
            raw = mappings[segment_start:i]
            if raw:
                line_segments.append(finish_segment(raw))
            result[-1] = line_segments
            result.append([])
            line_segments = []
            gen_col = src_idx = src_line = src_col = name_idx = 0
            i += 1
            segment_start = i
            continue
        i += 1
    # Trailing segment (a mappings string never ends with a separator here
    # because production encoding omits them; guard anyway).
    tail = mappings[segment_start:]
    if tail:
        line_segments.append(finish_segment(tail))
    result[-1] = line_segments
    return result
