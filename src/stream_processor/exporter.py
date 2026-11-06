"""Sink stage: streaming export to a new file."""

import csv
from collections.abc import Iterable
from pathlib import Path

from stream_processor.models import StudentRecord


def export_students(students: Iterable[StudentRecord], path: Path) -> int:
    """Write records to CSV while they pass through the pipeline."""
    written = 0
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["student_id", "first_name", "last_name", "group", "average_grade"]
        )

        for student in students:
            writer.writerow(student)
            written += 1

    return written
