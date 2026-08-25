# Results — Python Batch vs PySpark

One row per full pipeline run (`python src/main.py --input ...`).

## Run comparison

| Metric                        | Python Batch (sample file)     | PySpark (large file) |
|--------------------------------|----------------------------------|----------------------|
| File                            | orders_sample.csv               | orders_sample_1gb.csv |
| File size (MB)                  | 41.765                          | 1022.206             |
| read_rows                       | 100,000                         | 2,429,665             |
| loaded_raw                      | 100,000                         | 2,429,665             |
| Load seconds_elapsed            | 5.862                           | 68.335                |
| Load throughput (rec/s)         | 17,058.66                       | 35,555.38             |
| ELT seconds_elapsed             | 52.652                          | 1,339.864              |
| ELT throughput (rec/s)          | 1,899.27                        | 1,813.37               |
| count_valid                     | 26,935                          | 0                     |
| count_corrected                 | 65,916                          | 0                     |
| count_quarantine                | 7,149                           | 2,429,665             |
| count_inserted                  | 92,225                          | 0                     |
| count_updated                   | 626                              | 0                     |
| count_unchanged                 | 0                                | 0                     |
| quarantine_inserted             | 7,149                           | 2,429,665             |
| quarantine_updated              | 0                                | 0                     |
| quarantine_unchanged            | 0                                | 0                     |
| quarantine_write_failed         | 0                                | 0                     |
| consistency_check               | True                             | True                  |
| size_batch / input_partitions   | size_batch = 5000                | input_partitions = 8  |
| Total wall time                 | 58.601s                          | 1,408.521s (~23.48 min) |

*(Python Batch figures are from the first run after the database reset,
`id_run = 3f986f87-b253-4bb2-a8bc-b4e44bfe7e09`. PySpark figures are from
`id_run = 39176e8c-e436-4832-8616-6fbb3ba96f95`, run on `orders_sample_1gb.csv`
(1022.21 MB). See the idempotency section below for the Python Batch
second-run figures; PySpark has only been run once so far, so no
idempotency figures exist for it yet.)*

### counts_case_error (Python Batch run)

| Error code                   | Count |
|-------------------------------|-------|
| ID_CUSTOMER_MISSING            | 1,411 |
| PRICE_UNKNOWN                  | 674   |
| JSON_ITEMS_CORRUPTED           | 1,338 |
| ERRORS_CONFLICTING_MULTIPLE    | 674   |
| DATE_IMPOSSIBLE_INVALID        | 2,999 |
| VALUE_NEGATIVE_AMBIGUOUS       | 677   |
| ID_ORDER_MISSING               | 721   |
| ITEMS_EMPTY                    | 677   |

*(Identical across both Python Batch runs — same source file, no source
data changes, so the same 100,000 rows trigger the exact same
quality-rule errors every time, as expected.)*

### counts_case_error (PySpark run)

| Error code                   | Count     |
|-------------------------------|-----------|
| DATE_IMPOSSIBLE_INVALID        | 71,452    |
| JSON_ITEMS_CORRUPTED           | 2,412,782 |
| ERRORS_CONFLICTING_MULTIPLE    | 139,609   |
| ITEMS_EMPTY                    | 16,883    |
| VALUE_NEGATIVE_AMBIGUOUS       | 17,110    |
| ID_ORDER_MISSING               | 17,005    |
| ID_CUSTOMER_MISSING            | 34,042    |
| PRICE_UNKNOWN                  | 17,129    |

⚠️ **Note:** PySpark observation: All 2,429,665 records were quarantined, with approximately 99.3% flagged as JSON_ITEMS_CORRUPTED. This differs significantly from the Python Batch results and may indicate a CSV/JSON parsing or schema issue in the PySpark pipeline.

## Idempotency check (two consecutive runs, same file, unchanged source data)

| Run   | id_run                                | count_inserted | count_updated | count_unchanged | quarantine_inserted | quarantine_updated | quarantine_unchanged |
|-------|-----------------------------------------|-----------------|-----------------|-------------------|------------------------|------------------------|---------------------------|
| 1st   | 3f986f87-b253-4bb2-a8bc-b4e44bfe7e09    | 92,225          | 626             | 0                 | 7,149                  | 0                      | 0                         |
| 2nd   | fd533aa9-36ef-47d5-9c4e-0af45aa24172    | 0               | 92,851          | 0                 | 0                      | 7,149                  | 0                         |

**Result: idempotency confirmed for both layers (Python Batch).**
When the same 100,000-row input was processed twice, the second run created no duplicate records. In orders_validated, inserted records dropped from 92,225 to 0, while 92,851 existing records were updated. In quarantine_orders, new inserts dropped from 7,149 to 0, while all 7,149 existing quarantine records were updated. The raw row count remained 100,000 and the consistency check remained True on both runs.



## Notes
> PySpark achieved approximately 2× higher raw-load throughput than Python Batch, while Python Batch achieved slightly higher ELT throughput in this run.
> The PySpark run quarantined all records due to the unusually high JSON_ITEMS_CORRUPTED rate (~99.3%), so its ELT - --performance should be interpreted with caution until the CSV/JSON parsing behavior is verified.
> The Python Batch idempotency test produced no duplicate records when the same input was processed twice.
