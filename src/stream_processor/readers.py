"""Source stage: streaming reading of files."""

from collections.abc import Iterator
from pathlib import Path


def read_lines(path: Path) -> Iterator[str]:
    """Yield lines of a file one by one (the file is never read whole)."""
    with path.open("r", encoding="utf-8", newline="") as file:
        yield from file


def read_many(paths: list[Path]) -> Iterator[str]:
    """Stream several files one after another, header only from the first."""
    for number, path in enumerate(paths):
        lines = read_lines(path)

        if number > 0:
            next(lines, None)  # skip the header of the next files

        yield from lines
