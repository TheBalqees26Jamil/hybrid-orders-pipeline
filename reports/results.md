# Results — Python Batch vs PySpark

One row per full pipeline run (`python src/main.py --input ...`).


## Run comparison

| Metric                        | Python Batch (sample file)     | PySpark (large file) |
|--------------------------------|----------------------------------|----------------------|
| File                            | orders_sample.csv               | orders_sample_1gb.csv |
| File size (MB)                  | 41.765                          | 1022.206             |
| read_rows                       | 100,000                         | 2,429,665             |
| loaded_raw                      | 100,000                         | 2,429,665             |
| Load seconds_elapsed            | 5.862                           | 136.068                |
| Load throughput (rec/s)         | 17,058.66                       | 17,856.29              |
| ELT seconds_elapsed             | 52.652                          | 1,061.852              |
| ELT throughput (rec/s)          | 1,899.27                        | 2,288.14                |
| count_valid                     | 26,935 (26.94%)                 | 659,120 (27.13%)      |
| count_corrected                 | 65,916 (65.92%)                 | 1,597,170 (65.74%)    |
| count_quarantine                | 7,149 (7.15%)                   | 173,375 (7.14%)       |
| count_inserted                  | 92,225                          | 2,247,523             |
| count_updated                   | 626                              | 8,767                 |
| count_unchanged                 | 0                                | 0                     |
| quarantine_inserted             | 7,149                           | 151,370               |
| quarantine_updated              | 0                                | 22,005                |
| quarantine_unchanged            | 0                                | 0                     |
| quarantine_write_failed         | 0                                | 0                     |
| consistency_check               | True                             | True                  |
| size_batch / input_partitions   | size_batch = 5000                | input_partitions = 1  |
| Total wall time                 | 58.601s                          | 1,198.695s (~20.0 min) |



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

### counts_case_error (PySpark run, post-fix)

| Error code                   | Count  |
|-------------------------------|--------|
| ID_CUSTOMER_MISSING            | 34,042 |
| DATE_IMPOSSIBLE_INVALID        | 71,452 |
| JSON_ITEMS_CORRUPTED           | 34,012 |
| ID_ORDER_MISSING               | 17,005 |
| VALUE_NEGATIVE_AMBIGUOUS       | 17,110 |
| PRICE_UNKNOWN                  | 17,129 |
| ERRORS_CONFLICTING_MULTIPLE    | 17,129 |
| ITEMS_EMPTY                    | 16,883 |

 

## Idempotency check (two consecutive runs, same file, unchanged source data)

### Python Batch

| Run   | id_run                                | count_inserted | count_updated | count_unchanged | quarantine_inserted | quarantine_updated | quarantine_unchanged |
|-------|-----------------------------------------|-----------------|-----------------|-------------------|------------------------|------------------------|---------------------------|
| 1st   | 3f986f87-b253-4bb2-a8bc-b4e44bfe7e09    | 92,225          | 626             | 0                 | 7,149                  | 0                      | 0                         |
| 2nd   | fd533aa9-36ef-47d5-9c4e-0af45aa24172    | 0               | 92,851          | 0                 | 0                      | 7,149                  | 0                         |

Two further repeat runs (`8a1c9ff5-e603-4f30-bde5-24d09622b5bc` and
`c3c34522-9f39-40e9-8aeb-84565ebd3448`) reproduce the exact same
update-only pattern (`count_inserted = 0`, `count_updated = 92,851`,
`quarantine_inserted = 0`, `quarantine_updated = 7,149`), confirming the
behavior is stable across repeated re-runs, not a one-off.

### PySpark (post-fix)

| Run   | id_run                                | count_inserted | count_updated | count_unchanged | quarantine_inserted | quarantine_updated | quarantine_unchanged |
|-------|-----------------------------------------|-----------------|-----------------|-------------------|------------------------|------------------------|---------------------------|
| 1st   | 1af9962e-7334-4f91-9e62-cbf22ac88dab    | 2,247,523       | 8,767           | 0                 | 151,370                | 22,005                 | 0                         |
| 2nd   | 90e813e8-31ad-4158-afff-9cfeaaa33978    | 0               | 2,256,290       | 0                 | 0                      | 173,375                | 0                         |

**Result: idempotency confirmed for both layers, on both engines.**

- **orders_validated (PySpark):** `count_inserted` dropped from 2,247,523
  to **0** on the second run, while `count_updated` rose to **2,256,290**
  — exactly `2,247,523 + 8,767`, meaning every previously-written record
  (whether inserted or updated on the first run) was matched and updated
  in place on the second run, with zero new duplicates.
- **quarantine_orders (PySpark):** `quarantine_inserted` dropped from
  151,370 to **0**, while `quarantine_updated` rose to **173,375** —
  exactly `151,370 + 22,005`. Same guarantee as Python Batch: re-running
  the same file never re-inserts the same quarantine records.
- `run_raw_count` stayed at 2,429,665 and `consistency_check` stayed
  `True` on both PySpark runs, mirroring the Python Batch result.

Note: as with Python Batch, `count_unchanged` / `quarantine_unchanged`
stay at 0 on the second run because both `record_cleaned` and the
quarantine document carry an `id_run` field set to the *current* run's
`id_run` on every write, so MongoDB reports every matched document as
`modified` rather than `unchanged` — this is expected, not a bug.


