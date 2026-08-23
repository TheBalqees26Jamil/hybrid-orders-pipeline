import csv
import sys
import time
import uuid
from pathlib import Path

from pymongo.errors import BulkWriteError, PyMongoError

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import BATCH_SIZE, CSV_ENCODING
from src.mongo_setup import get_raw_collection
from src.raw_builder import build_raw_record


def load_batch_python(input_path: str, id_run: str = None,
                       batch_size: int = BATCH_SIZE) -> dict:
    
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"File not found: {input_path}")

    if id_run is None:
        id_run = str(uuid.uuid4())

    raw_collection = get_raw_collection()

    engine_used = "python_batch"
    file_source = str(input_file)

    read_rows = 0
    loaded_raw = 0
    failed_rows = 0
    batch_number = 0
    buffer = []

    start_time = time.time()

    print("=" * 60)
    print(f"Python Batch Loader - id_run = {id_run}")
    print(f"File: {file_source} | Batch size: {batch_size} | Encoding: {CSV_ENCODING}")
    print("=" * 60)

    with open(input_file, "r", encoding=CSV_ENCODING, errors="replace", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            read_rows += 1
            record = build_raw_record(
                row=row,
                id_run=id_run,
                file_source=file_source,
                number_row_source=read_rows,
                engine_used=engine_used,
            )
            buffer.append(record)

            if len(buffer) >= batch_size:
                batch_number += 1
                inserted, failed = _flush_batch(raw_collection, buffer, batch_number, start_time)
                loaded_raw += inserted
                failed_rows += failed
                buffer = []

        # Write any remaining records smaller than a full batch
        if buffer:
            batch_number += 1
            inserted, failed = _flush_batch(raw_collection, buffer, batch_number, start_time)
            loaded_raw += inserted
            failed_rows += failed

    elapsed = time.time() - start_time
    throughput = round(loaded_raw / elapsed, 2) if elapsed > 0 else 0.0

    metrics = {
        "id_run": id_run,
        "used_engine": engine_used,
        "file_name": input_file.name,
        "read_rows": read_rows,
        "loaded_raw": loaded_raw,
        "failed_rows": failed_rows,
        "batch_count": batch_number,
        "size_batch": batch_size,
        "seconds_elapsed": round(elapsed, 3),
        "throughput": throughput,
    }

    print("-" * 60)
    print("Python Batch Loader finished:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    print("=" * 60)

    return metrics


def _flush_batch(collection, buffer: list, batch_number: int, start_time: float) -> tuple:
    
    batch_start = time.time()
    inserted_count = 0
    failed_count = 0

    try:
        result = collection.insert_many(buffer, ordered=False)
        inserted_count = len(result.inserted_ids)
    except BulkWriteError as bwe:
        # Some records succeeded and some failed (ordered=False continues despite errors)
        write_errors = bwe.details.get("writeErrors", [])
        failed_count = len(write_errors)
        inserted_count = len(buffer) - failed_count
        print(f"  [warning] Batch #{batch_number}: {failed_count} of {len(buffer)} records failed.")
        for err in write_errors[:5]:  # show only the first 5 errors to avoid flooding the output
            print(f"    - index={err.get('index')} | code={err.get('code')} | "
                  f"errmsg={err.get('errmsg')}")
    except PyMongoError as e:
        # The whole batch failed (e.g. connection lost) - logged, not hidden, then re-raised
        failed_count = len(buffer)
        inserted_count = 0
        print(f"  [error] Batch #{batch_number} failed completely: {type(e).__name__}: {e}")
        raise

    batch_elapsed = time.time() - batch_start
    batch_rate = round(len(buffer) / batch_elapsed, 2) if batch_elapsed > 0 else 0.0
    total_elapsed = time.time() - start_time

    print(f"  Batch #{batch_number}: records={len(buffer)} | ok={inserted_count} | "
          f"failed={failed_count} | batch_time={batch_elapsed:.3f}s | "
          f"rate={batch_rate} rec/s | total_time_so_far={total_elapsed:.2f}s")

    return inserted_count, failed_count


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Standalone test for the Python Batch engine.")
    parser.add_argument("--input", type=str, required=True, help="Path to the CSV file")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Batch size")
    args = parser.parse_args()

    load_batch_python(args.input, batch_size=args.batch_size)
