"""Generation of large test files."""

import csv
import random
from pathlib import Path

GROUPS: tuple[str, ...] = ("FeP-31", "FeP-32", "FeP-33s", "FeP-34")


def generate_test_file(path: Path, count: int, seed: int = 1) -> None:
    """Write ``count`` student records to a CSV file."""
    rng = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["student_id", "first_name", "last_name", "group", "average_grade"]
        )

        for index in range(1, count + 1):
            writer.writerow(
                [
                    index,
                    f"name{index}",
                    f"surname{index}",
                    rng.choice(GROUPS),
                    round(rng.uniform(40, 100), 2),
                ]
            )
