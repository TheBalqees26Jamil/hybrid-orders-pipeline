import sys
import time
from pathlib import Path

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import BATCH_SIZE
from src import mongo_setup
from src.classification import classify_record


def _flush_validated_batch(collection, ops: list, batch_number: int) -> tuple:
    
    if not ops:
        return 0, 0, 0, 0

    try:
        result = collection.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        updated = result.modified_count
        unchanged = max(result.matched_count - result.modified_count, 0)
        failed = 0
    except BulkWriteError as bwe:
        write_errors = bwe.details.get("writeErrors", [])
        failed = len(write_errors)
        details = bwe.details
        inserted = details.get("nUpserted", 0)
        updated = details.get("nModified", 0)
        matched = details.get("nMatched", 0)
        unchanged = max(matched - updated, 0)
        print(f"  [warning] Validated batch #{batch_number}: {failed} of {len(ops)} operations failed.")
        for err in write_errors[:5]:
            print(f"    - index={err.get('index')} | code={err.get('code')} | errmsg={err.get('errmsg')}")
    except PyMongoError as e:
        print(f"  [error] Validated batch #{batch_number} failed completely: {type(e).__name__}: {e}")
        raise

    return inserted, updated, unchanged, failed


def _flush_quarantine_batch(collection, docs: list, batch_number: int) -> tuple:
    
    if not docs:
        return 0, 0, 0, 0

    ops = [
        UpdateOne(
            filter={
                "file_source": doc.get("file_source"),
                "number_row_source": doc.get("number_row_source"),
            },
            update={"$set": doc},
            upsert=True,
        )
        for doc in docs
    ]

    try:
        result = collection.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        updated = result.modified_count
        unchanged = max(result.matched_count - result.modified_count, 0)
        failed = 0
    except BulkWriteError as bwe:
        write_errors = bwe.details.get("writeErrors", [])
        failed = len(write_errors)
        details = bwe.details
        inserted = details.get("nUpserted", 0)
        updated = details.get("nModified", 0)
        matched = details.get("nMatched", 0)
        unchanged = max(matched - updated, 0)
        print(f"  [warning] Quarantine batch #{batch_number}: {failed} of {len(ops)} operations failed.")
        for err in write_errors[:5]:
            print(f"    - index={err.get('index')} | code={err.get('code')} | errmsg={err.get('errmsg')}")
    except PyMongoError as e:
        print(f"  [error] Quarantine batch #{batch_number} failed completely: {type(e).__name__}: {e}")
        raise

    return inserted, updated, unchanged, failed


def run_elt(id_run: str, batch_size: int = BATCH_SIZE) -> dict:
    mongo_setup.setup_collections()

    raw_collection = mongo_setup.get_raw_collection()
    validated_collection = mongo_setup.get_validated_collection()
    quarantine_collection = mongo_setup.get_quarantine_collection()

    run_raw_count = 0
    count_valid = 0
    count_corrected = 0
    count_quarantine = 0
    count_inserted = 0
    count_updated = 0
    count_unchanged = 0
    count_write_failed = 0
    count_quarantine_inserted = 0
    count_quarantine_updated = 0
    count_quarantine_unchanged = 0
    count_quarantine_write_failed = 0
    counts_case_error = {}

    validated_ops = []
    quarantine_docs = []
    docs_in_buffer = 0
    batch_number = 0

    print("=" * 60)
    print(f"ELT Pipeline (Stage 10/11) - id_run = {id_run}")
    print(f"Batch size: {batch_size}")
    print("=" * 60)

    start_time = time.time()

    cursor = raw_collection.find({"id_run": id_run})

    def _flush(batch_number: int):
        nonlocal count_inserted, count_updated, count_unchanged, count_write_failed
        nonlocal count_quarantine_inserted, count_quarantine_updated
        nonlocal count_quarantine_unchanged, count_quarantine_write_failed
        batch_start = time.time()

        v_inserted, v_updated, v_unchanged, v_failed = _flush_validated_batch(
            validated_collection, validated_ops, batch_number
        )
        q_inserted, q_updated, q_unchanged, q_failed = _flush_quarantine_batch(
            quarantine_collection, quarantine_docs, batch_number
        )

        count_inserted += v_inserted
        count_updated += v_updated
        count_unchanged += v_unchanged
        count_write_failed += v_failed + q_failed

        count_quarantine_inserted += q_inserted
        count_quarantine_updated += q_updated
        count_quarantine_unchanged += q_unchanged
        count_quarantine_write_failed += q_failed

        batch_elapsed = time.time() - batch_start
        total_docs = len(validated_ops) + len(quarantine_docs)
        rate = round(total_docs / batch_elapsed, 2) if batch_elapsed > 0 else 0.0
        total_elapsed = time.time() - start_time

        print(
            f"  Batch #{batch_number}: validated_ops={len(validated_ops)} "
            f"(inserted={v_inserted}, updated={v_updated}, unchanged={v_unchanged}) | "
            f"quarantine_docs={len(quarantine_docs)} "
            f"(inserted={q_inserted}, updated={q_updated}, unchanged={q_unchanged}) | "
            f"batch_time={batch_elapsed:.3f}s | rate={rate} rec/s | "
            f"total_time_so_far={total_elapsed:.2f}s"
        )

    for raw_doc in cursor:
        run_raw_count += 1
        classification = classify_record(raw_doc)
        status = classification["quality_status"]

        if status == "valid":
            count_valid += 1
        elif status == "corrected":
            count_corrected += 1
        else:
            count_quarantine += 1

        for code in classification["quarantine_errors"]:
            counts_case_error[code] = counts_case_error.get(code, 0) + 1

        if classification["validated_doc"] is not None:
            doc = classification["validated_doc"]
            validated_ops.append(
                UpdateOne(
                    filter={"id_order": doc["id_order"]},
                    update={"$set": doc},
                    upsert=True,
                )
            )
        else:
            quarantine_doc = classification["quarantine_doc"]
            quarantine_doc["file_source"] = raw_doc.get("file_source")
            quarantine_docs.append(quarantine_doc)

        docs_in_buffer += 1
        if docs_in_buffer >= batch_size:
            batch_number += 1
            _flush(batch_number)
            validated_ops = []
            quarantine_docs = []
            docs_in_buffer = 0

    if docs_in_buffer > 0:
        batch_number += 1
        _flush(batch_number)

    elapsed = time.time() - start_time
    throughput = round(run_raw_count / elapsed, 2) if elapsed > 0 else 0.0

    expected_total = count_valid + count_corrected + count_quarantine
    consistency_check = (run_raw_count == expected_total)

    print("-" * 60)
    if consistency_check:
        print(
            f"[consistency check OK] run_raw_count ({run_raw_count}) == "
            f"count_valid + count_corrected + count_quarantine ({expected_total})."
        )
    else:
        print("!" * 60)
        print(
            f"[CONSISTENCY WARNING] run_raw_count ({run_raw_count}) != "
            f"count_valid + count_corrected + count_quarantine ({expected_total}). "
            f"Difference = {run_raw_count - expected_total}. "
            f"This does NOT crash the run, but it MUST be investigated - "
            f"it means some raw record for this id_run was not classified "
            f"into exactly one output layer."
        )
        print("!" * 60)

    metrics = {
        "id_run": id_run,
        "run_raw_count": run_raw_count,
        "count_valid": count_valid,
        "count_corrected": count_corrected,
        "count_quarantine": count_quarantine,
        "count_inserted": count_inserted,
        "count_updated": count_updated,
        "count_unchanged": count_unchanged,
        "count_write_failed": count_write_failed,
        # NEW - purely additive, existing keys above are untouched:
        "quarantine_inserted": count_quarantine_inserted,
        "quarantine_updated": count_quarantine_updated,
        "quarantine_unchanged": count_quarantine_unchanged,
        "quarantine_write_failed": count_quarantine_write_failed,
        "counts_case_error": counts_case_error,
        "consistency_check": consistency_check,
        "elt_seconds_elapsed": round(elapsed, 3),
        "elt_throughput": throughput,
    }

    print("-" * 60)
    print("ELT Pipeline finished:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    print("=" * 60)

    return metrics


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Standalone run of the ELT (classify + load) stage.")
    parser.add_argument("--id-run", type=str, required=True, help="id_run of an existing orders_raw batch")
    args = parser.parse_args()

    try:
        run_elt(args.id_run)
    finally:
        mongo_setup.close_client()