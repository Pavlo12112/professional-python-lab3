"""Pipeline: connection of lazy stages."""

from collections import Counter
from collections.abc import Iterator
from pathlib import Path

from stream_processor.filters import (
    filter_by_grade,
    filter_by_group,
    validate_students,
)
from stream_processor.models import StudentRecord
from stream_processor.parsers import parse_csv_rows
from stream_processor.readers import read_lines, read_many
from stream_processor.transformations import normalize_names


def build_pipeline(
    path: Path | list[Path],
    minimum_grade: float = 0.0,
    group: str | None = None,
    stats: Counter | None = None,
) -> Iterator[StudentRecord]:
    """Connect the stages. Nothing is read until the result is consumed.

    The returned pipeline is one-shot: after it is consumed, build a new one.
    """
    lines = read_many(path) if isinstance(path, list) else read_lines(path)
    rows = parse_csv_rows(lines)
    valid = validate_students(rows, stats)
    normalized = normalize_names(valid)
    stream = filter_by_grade(normalized, minimum_grade)

    if group is not None:
        stream = filter_by_group(stream, group)

    return stream
