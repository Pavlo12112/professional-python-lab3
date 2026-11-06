"""Validation and filtering stages."""

from collections import Counter
from collections.abc import Callable, Iterable, Iterator

from stream_processor.models import StudentRecord


def parse_student_row(row: dict[str, str]) -> StudentRecord | None:
    """Convert one CSV row to a record or return None if it is invalid."""
    try:
        student_id = int(row["student_id"])
        first_name = row["first_name"].strip()
        last_name = row["last_name"].strip()
        group = row["group"].strip()
        grade = float(row["average_grade"])
    except (ValueError, TypeError, KeyError, AttributeError):
        return None

    if not (first_name and last_name and group):
        return None

    if not 0 <= grade <= 100:
        return None

    return StudentRecord(student_id, first_name, last_name, group, grade)


def validate_students(
    rows: Iterable[dict[str, str]], stats: Counter | None = None
) -> Iterator[StudentRecord]:
    """Validation stage; counts valid/invalid rows in ``stats``."""
    for row in rows:
        record = parse_student_row(row)

        if record is None:
            if stats is not None:
                stats["invalid"] += 1
            continue

        if stats is not None:
            stats["valid"] += 1

        yield record


def filter_by_group(
    students: Iterable[StudentRecord], group: str
) -> Iterator[StudentRecord]:
    """Keep only students of the given group."""
    for student in students:
        if student.group == group:
            yield student


def filter_by_grade(
    students: Iterable[StudentRecord], minimum: float
) -> Iterator[StudentRecord]:
    """Keep only students with grade >= minimum."""
    for student in students:
        if student.average_grade >= minimum:
            yield student


def find_by_last_name(
    students: Iterable[StudentRecord], last_name: str
) -> Iterator[StudentRecord]:
    """Lazy search by last name (case-insensitive)."""
    needle = last_name.lower()
    return (s for s in students if s.last_name.lower() == needle)


def find_first(
    records: Iterable[StudentRecord], predicate: Callable[[StudentRecord], bool]
) -> StudentRecord | None:
    """Lazy search with early termination."""
    for record in records:
        if predicate(record):
            return record

    return None
