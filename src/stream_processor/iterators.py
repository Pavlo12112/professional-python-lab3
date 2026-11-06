"""Iterator protocol examples: classes, infinite and recursive generators."""

from collections.abc import Iterable, Iterator
from itertools import count, islice, takewhile


class NumberRange:
    """Iterable: every iter() call creates a new independent iterator."""

    def __init__(self, start: int, stop: int) -> None:
        self.start = start
        self.stop = stop

    def __iter__(self) -> "NumberRangeIterator":
        return NumberRangeIterator(self.start, self.stop)


class NumberRangeIterator:
    """Iterator: keeps the state of traversal and implements __next__."""

    def __init__(self, start: int, stop: int) -> None:
        self.current = start
        self.stop = stop

    def __iter__(self) -> "NumberRangeIterator":
        return self

    def __next__(self) -> int:
        if self.current >= self.stop:
            raise StopIteration

        value = self.current
        self.current += 1
        return value


def natural_numbers(start: int = 1) -> Iterator[int]:
    """Infinite generator."""
    value = start
    while True:
        yield value
        value += 1


def first_squares(n: int) -> list[int]:
    """Infinite generator limited with islice."""
    return list(islice((x * x for x in natural_numbers()), n))


def squares_below(limit: int) -> list[int]:
    """Infinite stream limited with takewhile."""
    return list(takewhile(lambda x: x < limit, (x * x for x in count(1))))


def flatten(values: Iterable) -> Iterator:
    """Recursive generator with yield from."""
    for value in values:
        if isinstance(value, list):
            yield from flatten(value)
        else:
            yield value
