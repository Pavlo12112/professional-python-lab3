"""Data model of the pipeline."""

from typing import NamedTuple


class StudentRecord(NamedTuple):
    """One validated student record (immutable, light-weight)."""

    student_id: int
    first_name: str
    last_name: str
    group: str
    average_grade: float

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"
