"""Stable, machine readable error categories.

Every rejection path uses one of these codes so that enrichment results and
build-ingestion reports can be reviewed mechanically.
"""

from __future__ import annotations


class SmPipeError(Exception):
    """Base class for pipeline errors carrying a stable ``reason`` code."""

    reason = "internal_error"

    def __init__(self, message: str = "", *, reason: str | None = None) -> None:
        super().__init__(message or self.reason)
        if reason is not None:
            self.reason = reason


class ManifestError(SmPipeError):
    reason = "invalid_manifest"


class DigestMismatch(SmPipeError):
    reason = "digest_mismatch"


class SourceContentMismatch(SmPipeError):
    reason = "source_content_digest_mismatch"


class InvalidSourceMap(SmPipeError):
    reason = "invalid_map"


class BadVLQ(InvalidSourceMap):
    reason = "bad_vlq"


class ExternalSection(InvalidSourceMap):
    reason = "external_section_not_supported"


class SectionOverlap(InvalidSourceMap):
    reason = "section_overlap"


class SectionOrder(InvalidSourceMap):
    reason = "section_order"


class MapVersionUnsupported(InvalidSourceMap):
    reason = "map_version_unsupported"


class DuplicateFrame(SmPipeError):
    reason = "duplicate_frame_conflict"


class InvalidFrame(SmPipeError):
    reason = "invalid_frame"


class ColumnUnitError(SmPipeError):
    reason = "column_unit_error"


class SurrogatePairColumn(ColumnUnitError):
    reason = "inside_surrogate_pair"


class ColumnOutOfRange(ColumnUnitError):
    reason = "column_out_of_range"


class LineOutOfRange(ColumnUnitError):
    reason = "line_out_of_range"


class MissingArtifact(SmPipeError):
    reason = "missing_artifact"


class BindingCycle(SmPipeError):
    reason = "cycle"


class BatchError(SmPipeError):
    """A telemetry change batch is malformed or conflicts with final state."""

    reason = "invalid_batch"


class BatchSequenceError(SmPipeError):
    """Batch seq is stale, skips a number, or reuses a seq with new bytes."""

    reason = "batch_sequence"


class EnrichDatabaseError(SmPipeError):
    """The sealed enrichment database is missing, changed or unreadable."""

    reason = "enrich_database_error"


class ReverseIndexError(SmPipeError):
    """The sealed build database or the reverse index file is unusable."""

    reason = "reverse_index_error"


class InvalidSourceRange(SmPipeError):
    """A source-range query whose start is not strictly before its end."""

    reason = "invalid_source_range"
