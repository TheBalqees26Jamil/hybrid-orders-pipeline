
import argparse
import os
import random


def _estimate_avg_line_bytes(path: str, sample_lines: int = 2000) -> float:
   
    total_bytes = 0
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        f.readline()  
        for line in f:
            total_bytes += len(line.encode("utf-8"))
            count += 1
            if count >= sample_lines:
                break
    if count == 0:
        return 0.0
    return total_bytes / count


def make_sample(input_path: str, output_path: str, size_mb: float, seed: int = None):
    target_bytes = size_mb * 1024 * 1024
    input_size_bytes = os.path.getsize(input_path)

    avg_line_bytes = _estimate_avg_line_bytes(input_path)
    if avg_line_bytes == 0:
        raise ValueError("تعذّرت قراءة أي بيانات من الملف المدخل.")

   
    keep_probability = min(1.0, target_bytes / input_size_bytes)

    print("=" * 60)
    print("Representative Random Sampling")
    print("=" * 60)
    print(f"Input file size      : {input_size_bytes / (1024 * 1024):.2f} MB")
    print(f"Estimated line size  : {avg_line_bytes:.1f} bytes")
    print(f"Target sample size   : {size_mb} MB")
    print(f"Row keep probability : {keep_probability:.6f}")
    print("-" * 60)

    if seed is not None:
        random.seed(seed)
        print(f"Random seed          : {seed} (reproducible)")
    else:
        print("Random seed          : none (different sample each run)")
    print("=" * 60)

    written_bytes = 0
    rows_seen = 0
    rows_written = 0

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(input_path, "r", encoding="utf-8", errors="replace") as infile, \
         open(output_path, "w", encoding="utf-8", newline="") as outfile:

        header = infile.readline()
        outfile.write(header)
        written_bytes += len(header.encode("utf-8"))

        for line in infile:
            rows_seen += 1
            if random.random() < keep_probability:
                outfile.write(line)
                written_bytes += len(line.encode("utf-8"))
                rows_written += 1

            if rows_seen % 2_000_000 == 0:
                print(f"  ... scanned {rows_seen:,} rows so far, "
                      f"written {rows_written:,} rows "
                      f"({written_bytes / (1024 * 1024):.1f} MB)")

    final_mb = written_bytes / (1024 * 1024)
    print("-" * 60)
    print(f"[DONE] Sample written to : {output_path}")
    print(f"Rows scanned  : {rows_seen:,}")
    print(f"Rows written  : {rows_written:,}")
    print(f"Final size    : {final_mb:.2f} MB (target: {size_mb} MB)")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract a representative random sample spread across an entire large CSV file."
    )
    parser.add_argument("--input", required=True, help="مسار الملف الضخم الأصلي")
    parser.add_argument("--output", required=True, help="مسار ملف العينة الناتج")
    parser.add_argument("--size_mb", type=float, required=True, help="الحجم المستهدف للعينة بالـ MB")
    parser.add_argument("--seed", type=int, default=None, help="بذرة عشوائية اختيارية لعينة قابلة للتكرار")
    args = parser.parse_args()

    make_sample(args.input, args.output, args.size_mb, seed=args.seed)