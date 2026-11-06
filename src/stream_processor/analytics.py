"""Aggregation stage: streaming statistics."""

import heapq
from collections import Counter
from collections.abc import Iterable, Iterator
from itertools import accumulate, groupby, islice, pairwise

from stream_processor.models import StudentRecord


def streaming_average(students: Iterable[StudentRecord]) -> float:
    """Average grade in one pass, memory does not depend on data size."""
    total = 0.0
    count = 0

    for student in students:
        total += student.average_grade
        count += 1

    return total / count if count else 0.0


def calculate_statistics(students: Iterable[StudentRecord]) -> dict:
    """Count, average, min, max and groups in a single pass."""
    count = 0
    total = 0.0
    minimum: float | None = None
    maximum: float | None = None
    groups: Counter = Counter()

    for student in students:
        grade = student.average_grade
        count += 1
        total += grade
        groups[student.group] += 1

        if minimum is None or grade < minimum:
            minimum = grade

        if maximum is None or grade > maximum:
            maximum = grade

    return {
        "count": count,
        "average": total / count if count else 0.0,
        "minimum": minimum,
        "maximum": maximum,
        "groups": dict(groups),
    }


def top_n(students: Iterable[StudentRecord], n: int) -> list[StudentRecord]:
    """N students with the highest grade; only N records are kept in memory."""
    return heapq.nlargest(n, students, key=lambda s: s.average_grade)


def first_n(students: Iterable[StudentRecord], n: int) -> list[StudentRecord]:
    """First N records of a stream; the rest is never processed (islice)."""
    return list(islice(students, n))


def running_totals(students: Iterable[StudentRecord]) -> Iterator[float]:
    """Cumulative sum of grades (itertools.accumulate)."""
    return accumulate(s.average_grade for s in students)


def grade_differences(students: Iterable[StudentRecord]) -> Iterator[float]:
    """Differences between neighbouring grades (itertools.pairwise)."""
    grades = (s.average_grade for s in students)
    return (round(b - a, 2) for a, b in pairwise(grades))


def count_by_group_sorted(students: list[StudentRecord]) -> dict[str, int]:
    """groupby works on consecutive keys, so the data is sorted first."""
    ordered = sorted(students, key=lambda s: s.group)
    return {
        group: sum(1 for _ in members)
        for group, members in groupby(ordered, key=lambda s: s.group)
    }
