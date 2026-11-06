"""Transformation stages."""

from collections.abc import Iterable, Iterator

from stream_processor.models import StudentRecord


def normalize_names(students: Iterable[StudentRecord]) -> Iterator[StudentRecord]:
    """Normalize letter case of names."""
    for student in students:
        yield student._replace(
            first_name=student.first_name.title(),
            last_name=student.last_name.title(),
        )


def to_short_view(students: Iterable[StudentRecord]) -> Iterator[str]:
    """Lazy transformation into a short text representation."""
    return (
        f"#{s.student_id} {s.full_name} ({s.group}): {s.average_grade:.1f}"
        for s in students
    )
