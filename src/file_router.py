import sys
from pathlib import Path
from enum import Enum

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import SMALL_FILE_THRESHOLD_MB


class Engine(str, Enum):
    PYTHON_BATCH = "python_batch"
    PYSPARK = "pyspark"


def get_file_size_mb(file_path: str) -> float:
    """Computes the file size in megabytes."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    size_bytes = path.stat().st_size
    return size_bytes / (1024 * 1024)


def choose_engine(file_path: str, threshold_mb: float = SMALL_FILE_THRESHOLD_MB) -> dict:
    
    file_size_mb = get_file_size_mb(file_path)

    if file_size_mb <= threshold_mb:
        engine = Engine.PYTHON_BATCH
        reason = (
            f"File size ({file_size_mb:.2f} MB) is <= the threshold "
            f"({threshold_mb} MB) => using Python Batch (streaming read + batches)."
        )
    else:
        engine = Engine.PYSPARK
        reason = (
            f"File size ({file_size_mb:.2f} MB) is greater than the threshold "
            f"({threshold_mb} MB) => using PySpark (distributed/parallel processing)."
        )

    decision = {
        "file_path": str(file_path),
        "file_size_mb": round(file_size_mb, 3),
        "threshold_mb": threshold_mb,
        "engine": engine.value,
        "reason": reason,
    }

    print("=" * 60)
    print("File Router - engine selection decision")
    print("=" * 60)
    print(f"File            : {decision['file_path']}")
    print(f"Size            : {decision['file_size_mb']} MB")
    print(f"Threshold       : {decision['threshold_mb']} MB")
    print(f"Chosen engine   : {decision['engine']}")
    print(f"Reason          : {decision['reason']}")
    print("=" * 60)

    return decision


if __name__ == "__main__":
    # Quick standalone test for the Router
    import argparse

    parser = argparse.ArgumentParser(description="Test the engine-selection router.")
    parser.add_argument("--input", type=str, required=True, help="Path to the file to inspect")
    args = parser.parse_args()

    choose_engine(args.input)
