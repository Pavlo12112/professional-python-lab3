"""Experiment: eager versus lazy processing."""

import csv
import tracemalloc
from collections.abc import Callable
from pathlib import Path
from time import perf_counter

from stream_processor.analytics import calculate_statistics
from stream_processor.filters import find_first, parse_student_row
from stream_processor.generate_data import generate_test_file
from stream_processor.pipeline import build_pipeline

DATA_DIR = Path("data/generated")
SIZES: tuple[int, ...] = (10_000, 100_000, 500_000)
MINIMUM_GRADE = 85.0


def eager_statistics(path: Path, minimum: float = MINIMUM_GRADE) -> dict:
    """Eager variant: every intermediate collection is a full list."""
    with path.open("r", encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    parsed = [parse_student_row(row) for row in rows]
    valid = [record for record in parsed if record is not None]
    filtered = [record for record in valid if record.average_grade >= minimum]

    return calculate_statistics(filtered)


def lazy_statistics(path: Path, minimum: float = MINIMUM_GRADE) -> dict:
    """Lazy variant: the same result through the generator pipeline."""
    return calculate_statistics(build_pipeline(path, minimum))


def measure_time(function: Callable, *args: object) -> float:
    start = perf_counter()
    function(*args)
    return perf_counter() - start


def measure_peak_memory(function: Callable, *args: object) -> float:
    """Peak memory in MiB. The pipeline is fully consumed inside function."""
    tracemalloc.start()
    function(*args)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak / 1024 / 1024


def first_result_eager(path: Path) -> float:
    start = perf_counter()
    with path.open("r", encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))
    parsed = [parse_student_row(row) for row in rows]
    filtered = [r for r in parsed if r is not None and r.average_grade >= MINIMUM_GRADE]
    _ = filtered[0]
    return perf_counter() - start


def first_result_lazy(path: Path) -> float:
    start = perf_counter()
    next(build_pipeline(path, MINIMUM_GRADE))
    return perf_counter() - start


def ensure_file(count: int) -> Path:
    path = DATA_DIR / f"students_{count}.csv"
    if not path.exists():
        generate_test_file(path, count)
    return path


def run_comparison() -> None:
    print("Eager versus lazy (filter: average_grade >= 85)")
    print(
        f"{'records':>9}{'eager s':>10}{'lazy s':>10}"
        f"{'eager MiB':>12}{'lazy MiB':>10}{'1st eager s':>13}{'1st lazy s':>12}"
    )

    for size in SIZES:
        path = ensure_file(size)
        eager_t = measure_time(eager_statistics, path)
        lazy_t = measure_time(lazy_statistics, path)
        eager_m = measure_peak_memory(eager_statistics, path)
        lazy_m = measure_peak_memory(lazy_statistics, path)
        first_e = first_result_eager(path)
        first_l = first_result_lazy(path)
        print(
            f"{size:>9}{eager_t:>10.3f}{lazy_t:>10.3f}"
            f"{eager_m:>12.1f}{lazy_m:>10.2f}{first_e:>13.4f}{first_l:>12.6f}"
        )


def run_early_termination(count: int = 1_000_000) -> None:
    """Find the first record with a rare condition in 1 000 000 records."""
    path = ensure_file(count)

    def predicate(record) -> bool:
        return record.average_grade >= 99.9

    def eager() -> object:
        with path.open("r", encoding="utf-8", newline="") as file:
            rows = list(csv.DictReader(file))
        parsed = [parse_student_row(row) for row in rows]
        matches = [r for r in parsed if r is not None and predicate(r)]
        return matches[0] if matches else None

    def lazy() -> object:
        return find_first(build_pipeline(path), predicate)

    print(f"\nEarly termination: first record with grade >= 99.9 in {count} records")
    for name, function in (("eager", eager), ("lazy", lazy)):
        start = perf_counter()
        result = function()
        elapsed = perf_counter() - start
        print(f"{name:>6}: {elapsed:8.4f} s  -> id {result.student_id}")


def main() -> None:
    run_comparison()
    run_early_termination()


if __name__ == "__main__":
    main()
