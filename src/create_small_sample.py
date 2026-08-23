import argparse
import csv
import sys
import time
from pathlib import Path

# Allow importing config when running this script directly from src/
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import RAW_INPUT_FILE, SAMPLE_OUTPUT_FILE, SAMPLE_DEFAULT_ROWS, CSV_ENCODING


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a small, reproducible sample from a huge CSV file."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=RAW_INPUT_FILE,
        help="Path to the huge source CSV file (defaults to config/settings.py)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=SAMPLE_OUTPUT_FILE,
        help="Path to the output sample file",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=SAMPLE_DEFAULT_ROWS,
        help="Number of sample rows requested (excluding header)",
    )
    return parser.parse_args()


def create_small_sample(input_path: str, output_path: str, rows: int) -> dict:
   
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Source file not found: {input_path}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    rows_written = 0

    # encoding=CSV_ENCODING ("utf-8-sig") strips the real source file's BOM.
    with open(input_file, "r", encoding=CSV_ENCODING, errors="replace", newline="") as infile, \
         open(output_file, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Write the header row first (if present)
        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            raise ValueError("Source file is empty.")

        for i, row in enumerate(reader):
            if rows_written >= rows:
                break
            writer.writerow(row)
            rows_written += 1

            # Periodic progress print so the script never runs silently
            if rows_written % 20000 == 0:
                print(f"  ... {rows_written:,} rows written so far")

    elapsed = time.time() - start_time
    output_size_mb = output_file.stat().st_size / (1024 * 1024)

    summary = {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "rows_requested": rows,
        "rows_written": rows_written,
        "output_size_mb": round(output_size_mb, 3),
        "seconds_elapsed": round(elapsed, 3),
    }
    return summary


def main():
    args = parse_args()

    print("=" * 60)
    print("Generating a small, reproducible sample")
    print("=" * 60)
    print(f"Source file : {args.input}")
    print(f"Sample file : {args.output}")
    print(f"Rows        : {args.rows:,}")
    print(f"Encoding    : {CSV_ENCODING} (source)")
    print("-" * 60)

    summary = create_small_sample(args.input, args.output, args.rows)

    print("-" * 60)
    print("Success:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    print("=" * 60)


if __name__ == "__main__":
    main()
