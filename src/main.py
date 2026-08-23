import argparse
import sys
import time
import uuid
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src import file_router
from src import batch_loader
from src import spark_loader
from src import elt_pipeline
from src import metrics as metrics_module
from src import mongo_setup


def parse_args():
    parser = argparse.ArgumentParser(description="Run the full hybrid orders data pipeline end-to-end.")
    parser.add_argument("--input", type=str, required=True, help="Path to the source CSV file")
    return parser.parse_args()


def _build_merged_metrics(id_run: str, decision: dict, loader_metrics: dict, elt_metrics: dict) -> dict:
    
    merged = {
        "id_run": id_run,
        "file_name": loader_metrics.get("file_name"),
        "file_size_mb": decision.get("file_size_mb"),
        "used_engine": loader_metrics.get("used_engine"),
        "read_rows": loader_metrics.get("read_rows"),
        "loaded_raw": loader_metrics.get("loaded_raw"),
        "count_valid": elt_metrics.get("count_valid"),
        "count_corrected": elt_metrics.get("count_corrected"),
        "count_quarantine": elt_metrics.get("count_quarantine"),
        "seconds_elapsed": loader_metrics.get("seconds_elapsed"),
        "throughput": loader_metrics.get("throughput"),
        "elt_seconds_elapsed": elt_metrics.get("elt_seconds_elapsed"),
        "elt_throughput": elt_metrics.get("elt_throughput"),
        "counts_case_error": elt_metrics.get("counts_case_error"),
        "count_inserted": elt_metrics.get("count_inserted"),
        "count_updated": elt_metrics.get("count_updated"),
        "count_unchanged": elt_metrics.get("count_unchanged"),
        "consistency_check": elt_metrics.get("consistency_check"),
        "quarantine_inserted": elt_metrics.get("quarantine_inserted"),
        "quarantine_updated": elt_metrics.get("quarantine_updated"),
        "quarantine_unchanged": elt_metrics.get("quarantine_unchanged"),
        "quarantine_write_failed": elt_metrics.get("quarantine_write_failed"),
    }

    
    if loader_metrics.get("used_engine") == "python_batch":
        merged["size_batch"] = loader_metrics.get("size_batch")
    else:
        merged["input_partitions"] = loader_metrics.get("input_partitions")

    return merged


def main():
    args = parse_args()
    id_run = str(uuid.uuid4())
    run_start = time.time()

    spark_session_used = False

    try:
        decision = file_router.choose_engine(args.input)
        engine = decision["engine"]

        if engine == "python_batch":
            loader_metrics = batch_loader.load_batch_python(args.input, id_run=id_run)
        else:
            loader_metrics = spark_loader.load_pyspark(args.input, id_run=id_run)
            spark_session_used = True  # spark_loader closes its own session internally

        elt_metrics = elt_pipeline.run_elt(id_run)

        merged_metrics = _build_merged_metrics(id_run, decision, loader_metrics, elt_metrics)
        metrics_module.save_metrics(merged_metrics)

        total_elapsed = time.time() - run_start

        print("=" * 60)
        print("PIPELINE RUN SUMMARY")
        print("=" * 60)
        print(f"id_run              : {id_run}")
        print(f"input file          : {args.input}")
        print(f"engine used         : {merged_metrics.get('used_engine')}")
        print(f"read_rows           : {merged_metrics.get('read_rows')}")
        print(f"loaded_raw          : {merged_metrics.get('loaded_raw')}")
        print(f"count_valid         : {merged_metrics.get('count_valid')}")
        print(f"count_corrected     : {merged_metrics.get('count_corrected')}")
        print(f"count_quarantine    : {merged_metrics.get('count_quarantine')}")
        print(f"quarantine ins/upd/unch/failed : "
              f"{merged_metrics.get('quarantine_inserted')}/"
              f"{merged_metrics.get('quarantine_updated')}/"
              f"{merged_metrics.get('quarantine_unchanged')}/"
              f"{merged_metrics.get('quarantine_write_failed')}")
        print(f"count_inserted      : {merged_metrics.get('count_inserted')}")
        print(f"count_updated       : {merged_metrics.get('count_updated')}")
        print(f"count_unchanged     : {merged_metrics.get('count_unchanged')}")
        print(f"load seconds/rate   : {merged_metrics.get('seconds_elapsed')}s / {merged_metrics.get('throughput')} rec/s")
        print(f"elt seconds/rate    : {merged_metrics.get('elt_seconds_elapsed')}s / {merged_metrics.get('elt_throughput')} rec/s")
        print(f"consistency_check   : {merged_metrics.get('consistency_check')}")
        print(f"total wall time     : {round(total_elapsed, 3)}s")
        print("=" * 60)

    finally:
        # Always released, even on error. The Spark session (if used) is
        # already closed inside spark_loader.load_pyspark()'s own
        # try/finally, so there is nothing extra to do for it here -
        # only the Mongo client, which may be reused across both the
        # loader stage and elt_pipeline, is closed at this single,
        # top-level call site.
        mongo_setup.close_client()


if __name__ == "__main__":
    main()