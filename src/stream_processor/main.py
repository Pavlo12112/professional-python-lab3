"""Application entry point: demonstration of the streaming pipeline."""

from collections import Counter
from itertools import chain
from pathlib import Path

from stream_processor.analytics import (
    calculate_statistics,
    count_by_group_sorted,
    first_n,
    grade_differences,
    running_totals,
    streaming_average,
    top_n,
)
from stream_processor.batches import batched, dynamic_batches
from stream_processor.exporter import export_students
from stream_processor.filters import find_by_last_name
from stream_processor.iterators import (
    NumberRange,
    first_squares,
    flatten,
    squares_below,
)
from stream_processor.pipeline import build_pipeline
from stream_processor.transformations import to_short_view

INPUT = Path("data/input.csv")
EXPORT = Path("data/generated/export_85.csv")


def main() -> None:
    print("== 1. Iterator protocol ==")
    numbers = NumberRange(1, 5)
    print("list(NumberRange(1, 5)) twice:", list(numbers), list(numbers))
    print("first 5 squares (infinite generator + islice):", first_squares(5))
    print("squares below 50 (takewhile):", squares_below(50))
    print("flatten (yield from):", list(flatten([1, [2, 3], [4, [5, 6]]])))

    print("\n== 2. Pipeline: grade >= 85 ==")
    stats: Counter = Counter()
    pipeline = build_pipeline(INPUT, 85.0, stats=stats)
    print("pipeline is lazy, object:", type(pipeline).__name__)
    first_three = first_n(pipeline, 3)
    for student in first_three:
        print(" ", student.student_id, student.full_name, student.group, student.average_grade)

    print("\n== 3. Statistics (one pass, one-shot pipeline is rebuilt) ==")
    stats = Counter()
    result = calculate_statistics(build_pipeline(INPUT, 85.0, stats=stats))
    for key, value in result.items():
        print(f"  {key}: {value}")
    print("  valid/invalid rows:", dict(stats))

    print("\n== 4. Counting rows of the whole file ==")
    stats = Counter()
    total = streaming_average(build_pipeline(INPUT, 0.0, stats=stats))
    print(f"  average of all valid students: {total:.2f}, rows: {dict(stats)}")

    print("\n== 5. Search and top N ==")
    found = list(find_by_last_name(build_pipeline(INPUT), "moroz"))
    print("  by last name 'moroz':", [s.student_id for s in found])
    print("  top 3 by grade:", [(s.full_name, s.average_grade) for s in top_n(build_pipeline(INPUT), 3)])

    print("\n== 6. Batches of 4 students ==")
    for number, batch in enumerate(batched(build_pipeline(INPUT), 4), 1):
        print(f"  batch {number}: ids {[s.student_id for s in batch]}")

    print("\n== 7. itertools ==")
    print("  accumulate:", [round(x, 1) for x in running_totals(build_pipeline(INPUT, 85.0))])
    print("  pairwise differences:", list(grade_differences(build_pipeline(INPUT, 85.0))))
    print("  groupby (after sorting):", count_by_group_sorted(list(build_pipeline(INPUT))))
    print("  dynamic batches (sum >= 200):", list(dynamic_batches([90, 85, 70, 95, 60, 50, 99], 200)))
    both = chain(build_pipeline(INPUT, 90.0), build_pipeline(INPUT, 95.0))
    print("  chain of two pipelines:", [s.student_id for s in both])

    print("\n== 8. Lazy transformation ==")
    for line in first_n(to_short_view(build_pipeline(INPUT, 90.0)), 3):
        print(" ", line)

    print("\n== 9. Streaming export ==")
    written = export_students(build_pipeline(INPUT, 85.0), EXPORT)
    print(f"  {written} records written to {EXPORT}")


if __name__ == "__main__":
    main()
