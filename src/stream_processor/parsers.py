"""Parsing stage: lines of CSV text to dictionaries."""

import csv
from collections.abc import Iterable, Iterator


def parse_csv_rows(lines: Iterable[str]) -> Iterator[dict[str, str]]:
    """Parse CSV lines lazily using csv.DictReader."""
    yield from csv.DictReader(lines)
