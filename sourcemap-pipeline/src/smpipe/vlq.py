"""Base64 Variable Length Quantity codec used by Source Map v3 (ECMA-426).

This module deliberately does *not* depend on any third-party source-map
library: the requirement is to be able to prove segment decoding coverage.
A second, independently written decoder lives in ``tests/`` and cross-checks
this implementation on the hand-written fixtures.

Encoded VLQ details (per the source-map v3 specification):

* the value is split into 5-bit groups, least significant group first;
* bit 5 of every output base64 digit is the continuation flag;
* bit 0 of the *first* digit is the sign bit (1 = negative);
* for subsequent digits bit 0 is ordinary value data.
"""

from __future__ import annotations

from .errors import BadVLQ

_B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_DECODE = {ch: i for i, ch in enumerate(_B64)}
# In the mappings grammar ',' separates segments and ';' separates generated
# lines; any other character outside the base64 alphabet is corrupt.
_SEGMENT_SEP = ","
_LINE_SEP = ";"


def _digit_value(ch: str) -> int:
    """Map one base64 character to its 6-bit value via ordinal arithmetic.

    Equivalent to the ``_DECODE`` table but avoids a dict lookup on every
    digit of hot VLQ decode paths.  Returns -1 for illegal characters.
    """
    code = ord(ch)
    if 65 <= code <= 90:       # A-Z -> 0..25
        return code - 65
    if 97 <= code <= 122:      # a-z -> 26..51
        return code - 71
    if 48 <= code <= 57:       # 0-9 -> 52..61
        return code + 4
    if code == 43:             # '+' -> 62
        return 62
    if code == 47:             # '/' -> 63
        return 63
    return -1


def encode_vlq(value: int) -> str:
    """Encode one signed integer as a base64 VLQ string."""
    if value < 0:
        # Shift into sign-magnitude form with sign in the least significant
        # bit, then add 1 so that -1 does not collide with +0.
        magnitude = ((-value) << 1) | 1
    else:
        magnitude = value << 1

    digits: list[str] = []
    while True:
        digit = magnitude & 0x1F
        magnitude >>= 5
        if magnitude > 0:
            digits.append(_B64[digit | 0x20])  # continuation bit set
        else:
            digits.append(_B64[digit])
            break
    return "".join(digits)


def _decode_one(text: str, pos: int) -> tuple[int, int]:
    """Decode a single VLQ starting at ``pos``; return ``(value, new_pos)``."""
    result = 0
    shift = 0
    start = pos
    n = len(text)
    while True:
        if pos >= n:
            raise BadVLQ(f"unterminated VLQ at offset {start}")
        ch = text[pos]
        if ch == _SEGMENT_SEP or ch == _LINE_SEP:
            raise BadVLQ(f"unexpected separator inside VLQ at offset {pos}")
        digit = _digit_value(ch)
        if digit < 0:
            raise BadVLQ(f"character {ch!r} is not a valid base64 VLQ digit "
                         f"at offset {pos}")
        pos += 1
        continuation = digit & 0x20
        result |= (digit & 0x1F) << shift
        if not continuation:
            break
        shift += 5
        if shift >= 35:
            # Guard against absurdly long encodings (integer overflow / DoS).
            raise BadVLQ(f"VLQ value too long at offset {start}")
    negative = result & 1
    value = result >> 1
    return (-value if negative else value), pos


def decode_mappings(mappings: str) -> list[list[tuple[int, ...]]]:
    """Decode a v3 ``mappings`` string into segments grouped by generated line.

    Each segment is a tuple of 1, 4 or 5 relative fields::

        (gen_col_delta,)
        (gen_col_delta, source_idx_delta, src_line_delta, src_col_delta)
        (gen_col_delta, source_idx_delta, src_line_delta, src_col_delta,
         name_idx_delta)

    The relative state (generated column, source index, source line, source
    column, name index) resets at every generated line boundary, exactly as
    the standard requires.
    """
    if not isinstance(mappings, str):
        raise BadVLQ("mappings field must be a string")

    # Number of generated lines is always number of ';' plus one, so start
    # with one empty line and append a fresh line on every ';'.
    lines: list[list[tuple[int, ...]]] = [[]]

    gen_col = src_idx = src_line = src_col = name_idx = 0
    prev_seg: tuple[int, ...] | None = None

    pos = 0
    n = len(mappings)

    while pos < n:
        ch = mappings[pos]
        if ch == _LINE_SEP:
            lines.append([])
            # Per-line state reset, as mandated by the v3 standard.
            gen_col = src_idx = src_line = src_col = name_idx = 0
            prev_seg = None
            pos += 1
            continue
        if ch == _SEGMENT_SEP:
            # Leading, doubled or otherwise dangling comma = corrupt data.
            raise BadVLQ(f"empty segment at offset {pos}")

        fields: list[int] = []
        # First field: generated column.
        delta, pos = _decode_one(mappings, pos)
        gen_col += delta
        if gen_col < 0:
            raise BadVLQ("negative generated column after applying delta")
        fields.append(gen_col)

        if pos < n and mappings[pos] not in (_SEGMENT_SEP, _LINE_SEP):
            # 4-field form (or 5-field with name).
            delta, pos = _decode_one(mappings, pos)
            src_idx += delta
            delta, pos = _decode_one(mappings, pos)
            src_line += delta
            delta, pos = _decode_one(mappings, pos)
            src_col += delta
            if src_idx < 0 or src_line < 0 or src_col < 0:
                raise BadVLQ("negative source index/line/column after delta")
            fields.extend((src_idx, src_line, src_col))
            if pos < n and mappings[pos] not in (_SEGMENT_SEP, _LINE_SEP):
                delta, pos = _decode_one(mappings, pos)
                name_idx += delta
                if name_idx < 0:
                    raise BadVLQ("negative name index after delta")
                fields.append(name_idx)

        seg = tuple(fields)
        # The v3 grammar requires increasing columns *within one mappings
        # string*; equal columns are left for the store-level conflict check
        # so that "identical, foldable" and "inconsistent" can be told apart
        # instead of both being reported as bad VLQ.
        if prev_seg is not None and seg[0] < prev_seg[0]:
            raise BadVLQ("generated columns within a line must not decrease")
        if len(seg) not in (1, 4, 5):  # pragma: no cover - enforced above
            raise BadVLQ(f"segment must have 1, 4 or 5 fields, got {len(seg)}")
        lines[-1].append(seg)
        prev_seg = seg

        if pos < n:
            nxt = mappings[pos]
            if nxt == _SEGMENT_SEP:
                pos += 1
                if pos == n or mappings[pos] in (_SEGMENT_SEP, _LINE_SEP):
                    raise BadVLQ("dangling segment separator")
                continue
            if nxt == _LINE_SEP:
                continue
            raise BadVLQ(f"expected separator at offset {pos}, got {nxt!r}")

    return lines


def encode_mappings(lines: list[list[tuple[int, ...]]]) -> str:
    """Inverse of :func:`decode_mappings` (used by the fixture generator)."""
    out: list[str] = []
    for li, line in enumerate(lines):
        gen_col = src_idx = src_line = src_col = name_idx = 0
        encoded_segments: list[str] = []
        for seg in line:
            parts: list[str] = [encode_vlq(seg[0] - gen_col)]
            gen_col = seg[0]
            if len(seg) >= 4:
                parts.append(encode_vlq(seg[1] - src_idx))
                parts.append(encode_vlq(seg[2] - src_line))
                parts.append(encode_vlq(seg[3] - src_col))
                src_idx, src_line, src_col = seg[1], seg[2], seg[3]
            if len(seg) == 5:
                parts.append(encode_vlq(seg[4] - name_idx))
                name_idx = seg[4]
            encoded_segments.append("".join(parts))
        out.append(",".join(encoded_segments))
    return ";".join(out)
