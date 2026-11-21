"""Column unit conversions between UTF-16 code units and Unicode code points.

The profile states that every *external* frame is 1-based and its column unit
is either ``utf16`` (JavaScript semantics) or ``codepoint``.  Source Map v3
coordinates themselves are always zero-based UTF-16 code units, so only the
entry frame needs a conversion; deeper layers already speak UTF-16.

The converter is deliberately strict:

* a UTF-16 column pointing at the low surrogate half of a non-BMP character is
  rejected -- it does not name a character boundary and cannot be mapped;
* columns past the end of the located line are rejected;
* BMP-only lines make the two unit systems coincide, which is how the
  non-BMP fixture proves that both input units land on the same character.
"""

from __future__ import annotations

from .errors import (
    ColumnOutOfRange,
    LineOutOfRange,
    SurrogatePairColumn,
)

UTF16 = "utf16"
CODEPOINT = "codepoint"
VALID_UNITS = frozenset((UTF16, CODEPOINT))


class LineIndex:
    """Random-access view over a generated body.

    The line-start table is built once with C-level string searches so point
    queries on arbitrary generated lines never trigger quadratic scanning;
    per-line non-BMP span tables are created lazily only for lines that
    actually contain astral characters.  CRLF and a lone CR are both treated
    as one line break.
    """

    def __init__(self, body: str) -> None:
        self._body = body
        self._length = len(body)
        # Building the full line-start table uses the C-level ``str.find``
        # loop, which processes even a multi-megabyte body in sub-millisecond
        # time -- far cheaper than a Python per-character loop and necessary
        # anyway once queries address arbitrary generated lines.
        self._line_starts = self._build_line_starts(body)
        # Per-line non-BMP marker tables, built lazily only for lines that
        # actually contain an astral character.  BMP-only lines (the common
        # case for generated JavaScript) need no table at all because UTF-16
        # units and code points coincide there.
        self._non_bmp: dict[int, list[tuple[int, int]]] = {}

    @staticmethod
    def _build_line_starts(body: str) -> list[int]:
        starts = [0]
        pos = 0
        length = len(body)
        while True:
            nl = body.find("\n", pos)
            cr = body.find("\r", pos)
            if nl == -1 and cr == -1:
                break
            if nl != -1 and (cr == -1 or nl < cr):
                pos = nl + 1
            else:
                pos = cr + 1
                # CRLF counts as a single boundary.
                if cr != -1 and nl == cr + 1:
                    pos = nl + 1
            starts.append(pos)
        # Drop the phantom empty slot after a trailing line break.
        if starts and starts[-1] >= length:
            starts.pop()
        return starts

    def line_count(self) -> int:
        # ``_line_starts`` already excludes the phantom slot after a
        # trailing break, so its length is the splitlines() line count.
        return len(self._line_starts)

    def get_line(self, zero_line: int) -> str | None:
        if zero_line < 0 or zero_line >= len(self._line_starts):
            return None
        start = self._line_starts[zero_line]
        end = self._find_line_end(start)
        return self._body[start:end]

    def _find_line_end(self, start: int) -> int:
        pos = start
        while pos < self._length and self._body[pos] not in ("\n", "\r"):
            pos += 1
        return pos

    def non_bmp_spans(self, zero_line: int) -> list[tuple[int, int]]:
        """Return ``(code_point_index, utf16_start_unit)`` for astral chars.

        Built lazily and cached.  An empty list means the line is pure BMP,
        where UTF-16 and code-point columns are identical.
        """
        spans = self._non_bmp.get(zero_line)
        if spans is not None:
            return spans
        text = self.get_line(zero_line) or ""
        spans = []
        extra = 0
        for cp_index, ch in enumerate(text):
            if ord(ch) > 0xFFFF:
                spans.append((cp_index, cp_index + extra))
                extra += 1
        self._non_bmp[zero_line] = spans
        return spans

    def utf16_length(self, zero_line: int) -> int:
        text = self.get_line(zero_line) or ""
        return len(text) + len(self.non_bmp_spans(zero_line))


def external_to_utf16_zero_based(
    body: str,
    *,
    line_one_based: int,
    column_one_based: int,
    unit: str,
) -> tuple[int, int]:
    """Convert a 1-based external frame into ``(zero_line, zero_utf16_col)``.

    ``body`` is the bound generated text for the current layer.
    """
    if unit not in VALID_UNITS:
        raise ColumnOutOfRange(f"unknown column unit {unit!r}")
    if line_one_based < 1 or column_one_based < 1:
        raise ColumnOutOfRange("line and column must be 1-based and positive")

    index = body if isinstance(body, LineIndex) else LineIndex(body)
    zero_line = line_one_based - 1
    if zero_line < 0:
        raise ColumnOutOfRange("line must be 1-based and positive")
    # Lazily scan only as far as the requested line; never count all lines.
    text = index.get_line(zero_line)
    if text is None:
        raise LineOutOfRange(
            f"generated line {line_one_based} does not exist "
            f"(artifact has fewer than {line_one_based} line(s))"
        )
    zero_index_1based_col = column_one_based - 1

    if unit == UTF16:
        return zero_line, _check_utf16_column_indexed(
            index, zero_line, text, zero_index_1based_col
        )
    return zero_line, _codepoint_to_utf16_indexed(
        index, zero_line, text, zero_index_1based_col
    )


def _check_utf16_column_indexed(
    index: "LineIndex", zero_line: int, text: str, utf16_col: int
) -> int:
    utf16_len = index.utf16_length(zero_line)
    if utf16_col >= utf16_len:
        raise ColumnOutOfRange(
            f"UTF-16 column {utf16_col} is past end of line "
            f"(length {utf16_len})"
        )
    spans = index.non_bmp_spans(zero_line)
    if not spans:
        # Pure BMP line: UTF-16 units and code points are the same axis.
        return utf16_col
    import bisect
    starts = [start for _cp, start in spans]
    pos = bisect.bisect_right(starts, utf16_col) - 1
    if pos >= 0:
        cp_index, high_unit = spans[pos]
        if utf16_col == high_unit + 1:
            # The unit immediately after a non-BMP char's high unit is its
            # low surrogate: an interior position, not a character boundary.
            raise SurrogatePairColumn(
                f"UTF-16 column {utf16_col} is inside a surrogate pair"
            )
    return utf16_col


def _codepoint_to_utf16_indexed(
    index: "LineIndex", zero_line: int, text: str, codepoint_col: int
) -> int:
    if codepoint_col >= len(text):
        raise ColumnOutOfRange(
            f"code-point column {codepoint_col} is past end of line "
            f"(length {len(text)} code points)"
        )
    ch = text[codepoint_col]
    if 0xDC00 <= ord(ch) <= 0xDFFF:
        raise SurrogatePairColumn(
            f"code-point column {codepoint_col} lands on a low surrogate "
            "(inside a surrogate pair)"
        )
    # Every astral character before this code point adds one extra unit.
    spans = index.non_bmp_spans(zero_line)
    extra = sum(1 for cp_index, _u in spans if cp_index < codepoint_col)
    return codepoint_col + extra


def utf16_to_codepoint_column(text: str, utf16_col: int) -> int:
    """Inverse conversion, used when explaining frames back to the caller."""
    units = text.encode("utf-16-le")
    if utf16_col * 2 >= len(units):
        raise ColumnOutOfRange("UTF-16 column past end of line")
    cp_col = 0
    consumed = 0
    for ch in text:
        if consumed == utf16_col:
            return cp_col
        consumed += 2 if ord(ch) > 0xFFFF else 1
        cp_col += 1
    raise ColumnOutOfRange("UTF-16 column not representable as code point")
