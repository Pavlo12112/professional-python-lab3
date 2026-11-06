"""Unit tests for stream_processor."""

import tempfile
import unittest
from collections import Counter
from itertools import islice
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
from stream_processor.filters import find_by_last_name, find_first
from stream_processor.iterators import NumberRange, first_squares, flatten, natural_numbers
from stream_processor.pipeline import build_pipeline

INPUT = Path(__file__).resolve().parent.parent / "data" / "input.csv"


class IteratorTest(unittest.TestCase):
    def test_range_is_reusable(self) -> None:
        values = NumberRange(1, 4)
        self.assertEqual(list(values), [1, 2, 3])
        self.assertEqual(list(values), [1, 2, 3])

    def test_exhausted_generator(self) -> None:
        generator = (x for x in range(3))
        self.assertEqual(list(generator), [0, 1, 2])
        self.assertEqual(list(generator), [])

    def test_infinite_generator_with_islice(self) -> None:
        self.assertEqual(list(islice(natural_numbers(), 3)), [1, 2, 3])
        self.assertEqual(first_squares(4), [1, 4, 9, 16])

    def test_flatten(self) -> None:
        self.assertEqual(list(flatten([1, [2, [3]], 4])), [1, 2, 3, 4])


class PipelineTest(unittest.TestCase):
    def test_validation_counts_invalid(self) -> None:
        stats: Counter = Counter()
        records = list(build_pipeline(INPUT, 0.0, stats=stats))
        self.assertEqual(stats["valid"], 12)
        self.assertEqual(stats["invalid"], 4)
        self.assertEqual(len(records), 12)

    def test_filter_and_statistics(self) -> None:
        result = calculate_statistics(build_pipeline(INPUT, 85.0))
        self.assertEqual(result["count"], 6)
        self.assertEqual(result["maximum"], 96.0)
        self.assertEqual(result["minimum"], 85.6)

    def test_group_filter(self) -> None:
        records = list(build_pipeline(INPUT, 0.0, group="FeP-33s"))
        self.assertEqual({r.group for r in records}, {"FeP-33s"})

    def test_pipeline_is_one_shot(self) -> None:
        pipeline = build_pipeline(INPUT, 0.0)
        self.assertGreater(len(list(pipeline)), 0)
        self.assertEqual(list(pipeline), [])

    def test_top_first_and_search(self) -> None:
        self.assertEqual(top_n(build_pipeline(INPUT), 1)[0].student_id, 4)
        self.assertEqual(len(first_n(build_pipeline(INPUT), 3)), 3)
        found = list(find_by_last_name(build_pipeline(INPUT), "MOROZ"))
        self.assertEqual([s.student_id for s in found], [6])
        self.assertIsNone(find_first(build_pipeline(INPUT), lambda s: s.average_grade > 100))

    def test_itertools_helpers(self) -> None:
        self.assertAlmostEqual(streaming_average(build_pipeline(INPUT, 85.0)), 90.18, places=2)
        self.assertEqual(len(list(running_totals(build_pipeline(INPUT, 85.0)))), 6)
        self.assertEqual(len(list(grade_differences(build_pipeline(INPUT, 85.0)))), 5)
        counts = count_by_group_sorted(list(build_pipeline(INPUT)))
        self.assertEqual(counts["FeP-33s"], 4)


class BatchTest(unittest.TestCase):
    def test_batched(self) -> None:
        self.assertEqual(list(batched(range(7), 3)), [[0, 1, 2], [3, 4, 5], [6]])
        with self.assertRaises(ValueError):
            list(batched(range(3), 0))

    def test_dynamic_batches(self) -> None:
        self.assertEqual(list(dynamic_batches([5, 5, 5, 5, 1], 10)), [[5, 5], [5, 5], [1]])

    def test_lazy_exception(self) -> None:
        def parse(values):
            for value in values:
                yield int(value)

        generator = parse(["1", "x"])  # no error yet
        self.assertEqual(next(generator), 1)
        with self.assertRaises(ValueError):
            next(generator)


class FileTest(unittest.TestCase):
    def test_multi_file_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "a.csv"
            second = Path(directory) / "b.csv"
            header = "student_id,first_name,last_name,group,average_grade\n"
            first.write_text(header + "1,A,B,G,90\n", encoding="utf-8")
            second.write_text(header + "2,C,D,G,80\n", encoding="utf-8")
            records = list(build_pipeline([first, second]))
            self.assertEqual([r.student_id for r in records], [1, 2])


if __name__ == "__main__":
    unittest.main()
