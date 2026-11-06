"""Batching of a stream."""

from collections.abc import Iterable, Iterator
from itertools import islice
from typing import TypeVar

T = TypeVar("T")


def batched(items: Iterable[T], size: int) -> Iterator[list[T]]:
    """Split a stream into lists of ``size`` elements."""
    if size < 1:
        raise ValueError("Batch size must be positive.")

    iterator = iter(items)

    while batch := list(islice(iterator, size)):
        yield batch


def dynamic_batches(
    items: Iterable[float], limit: float
) -> Iterator[list[float]]:
    """Dynamic batching: a batch is closed when its sum reaches ``limit``."""
    batch: list[float] = []
    total = 0.0

    for item in items:
        batch.append(item)
        total += item

        if total >= limit:
            yield batch
            batch, total = [], 0.0

    if batch:
        yield batch
