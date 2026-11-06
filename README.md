# Stream Processor

Laboratory work No. 3 for the course "Professional Python":
iterators, generators and streaming data processing (variant 1,
streaming processing of student records).

## Description

Lazy pipeline over a CSV file with student records:

    read_lines -> parse_csv_rows -> validate_students -> normalize_names
               -> filter_by_grade -> (batches / aggregation / export)

Nothing is read until the result is consumed, so memory does not depend on
the size of the file.

## Requirements

Python 3.11+

## Installation

    python -m venv .venv

Activate the virtual environment
(Windows: `.venv\Scripts\activate`, Linux/macOS: `source .venv/bin/activate`).

    python -m pip install -e .

## Run

Demonstration of all stages (run from the project root):

    python -m stream_processor.main

Experiment eager versus lazy (generates files of 10 000, 100 000, 500 000
and 1 000 000 records in `data/generated/`, they are ignored by Git):

    python -m stream_processor.benchmark

Tests:

    python -m unittest discover -s tests -v

## Input format

    student_id,first_name,last_name,group,average_grade

Invalid rows (wrong id, empty name, grade outside 0..100) are skipped and
counted.

## Project structure

- `src/stream_processor/readers.py` - streaming source.
- `src/stream_processor/parsers.py` - CSV parsing.
- `src/stream_processor/filters.py` - validation, filtering, lazy search.
- `src/stream_processor/transformations.py` - transformations.
- `src/stream_processor/batches.py` - batching.
- `src/stream_processor/analytics.py` - streaming aggregation, itertools.
- `src/stream_processor/pipeline.py` - connection of the stages.
- `src/stream_processor/iterators.py` - iterator class, infinite and recursive generators.
- `src/stream_processor/exporter.py` - streaming export.
- `src/stream_processor/benchmark.py` - eager versus lazy experiment.
- `data/input.csv` - small input file.

## Author

Student: Tsiastun Pavlo
Group: FeP-33s
