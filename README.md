# Hybrid Orders ETL Pipeline

<p align="center">
  <strong>A hybrid ETL system for processing order data with high quality and scalability</strong><br>
  <em>Hybrid Python Batch + PySpark ingestion with automated data-quality rules, quarantine layer, and full audit trail — extended in Phase 2 with queries & indexes, aggregation reports, incrementally-refreshed materialized views, scheduled jobs, and a unified FastAPI interface</em>
</p>

---

## Tech Stack

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/MongoDB-8.0-47A248?style=for-the-badge&logo=mongodb&logoColor=white" alt="MongoDB">
  <img src="https://img.shields.io/badge/Apache%20Spark-PySpark%203.5+-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark">
  <img src="https://img.shields.io/badge/FastAPI-API-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/Swagger-OpenAPI-85EA2D?style=for-the-badge&logo=swagger&logoColor=black" alt="Swagger">
  <img src="https://img.shields.io/badge/Pytest-8.0+-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" alt="Pytest">
</p>

| Layer | Technology | Used for |
|---|---|---|
| **Language** | ![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white) | The whole pipeline, API, jobs and tests |
| **Database** | ![MongoDB](https://img.shields.io/badge/MongoDB-8.0-47A248?logo=mongodb&logoColor=white) ![PyMongo](https://img.shields.io/badge/PyMongo-4.6+-47A248?logo=mongodb&logoColor=white) | Raw / validated / quarantine layers, indexes, aggregation pipelines, `$merge` materialized views, job log |
| **Big-data engine** | ![Apache Spark](https://img.shields.io/badge/PySpark-3.5+-E25A1C?logo=apachespark&logoColor=white) ![MongoDB Spark Connector](https://img.shields.io/badge/Spark--MongoDB%20Connector-11.1-47A248?logo=mongodb&logoColor=white) ![Java](https://img.shields.io/badge/Java-8+-ED8B00?logo=openjdk&logoColor=white) | Distributed ingestion of large files (> 200 MB); Java is required by Spark |
| **Ingestion (small files)** | ![Python csv](https://img.shields.io/badge/Python-csv%20%2B%20batch%20inserts-3776AB?logo=python&logoColor=white) | Streaming read with `csv.DictReader` and batched `insert_many` |
| **API** | ![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white) ![Uvicorn](https://img.shields.io/badge/Uvicorn-ASGI%20server-2F8FCE) ![Swagger](https://img.shields.io/badge/Swagger%20UI-OpenAPI-85EA2D?logo=swagger&logoColor=black) | Unified JSON interface to run and test every function; interactive docs at `/docs` |
| **Scheduling** | ![APScheduler](https://img.shields.io/badge/APScheduler-3.x-4B8BBE?logo=clockify&logoColor=white) | Interval + cron jobs, manual triggering, execution log in `job_runs` |
| **Configuration** | ![dotenv](https://img.shields.io/badge/python--dotenv-ECD53F?logo=dotenv&logoColor=black) | Settings via `.env` / environment variables with safe defaults |
| **Testing** | ![Pytest](https://img.shields.io/badge/Pytest-8.0+-0A9EDC?logo=pytest&logoColor=white) | Unit tests for quality rules, classification and refresh helpers; API smoke test |
| **Version control** | ![Git](https://img.shields.io/badge/Git-F05032?logo=git&logoColor=white) ![GitHub](https://img.shields.io/badge/GitHub-181717?logo=github&logoColor=white) | Source hosting and delivery |

---

## Table of Contents

- [Overview](#overview)
- [Architecture & Stages](#architecture--stages)
- [Features](#features)
- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Usage — Phase 1 (pipeline)](#usage--phase-1-pipeline)
- [Phase 2 — Final Project Additions](#phase-2--final-project-additions)
  - [Quick start for evaluators](#quick-start-for-evaluators-step-by-step)
  - [Run the API](#run-the-api)
  - [API endpoints](#api-endpoints)
  - [1. Queries, indexes and Explain](#1-queries-indexes-and-explain)
  - [2. Aggregation reports](#2-aggregation-reports)
  - [3. Materialized views (incremental refresh)](#3-materialized-views-incremental-refresh)
  - [4. Scheduled jobs](#4-scheduled-jobs)
  - [5. Tests](#5-tests)
  - [Configuration](#configuration)
  - [Verified results](#verified-results)
  - [Troubleshooting](#troubleshooting)
  - [Known limitations](#known-limitations)
- [How It Works](#how-it-works)

---

## Overview

This project is a Hybrid ETL Pipeline for processing large CSV files containing order data of varying quality. The system performs:

- **Raw Ingestion:** Reads the data exactly as-is, without any modification, preserving the original values.
- **Smart Routing (File Router):** Automatically selects the appropriate engine based on file size (Python Batch for small files, PySpark for large files).
- **Quality Rules:** Applies 11+ automatic cleaning and validation rules to every record.
- **Classification:** Classifies every record as `valid`, `corrected`, or `quarantined`.
- **Validated Layer:** Writes valid records to `orders_validated`, guaranteeing no duplicates (Upsert & Idempotency).
- **Quarantine Layer:** Isolates faulty records in `quarantine_orders` with human-readable error details.
- **Full Audit Trail:** Records every correction applied to every field of every record.
- **Metrics/Reports:** Saves performance metrics for every run in `reports/results.json`.

**Phase 2 (final project) adds, on top of the same pipeline:**

- **Queries & Indexes:** 6 practical queries, 5 supporting indexes (one compound, one multikey), and `explain("executionStats")` before/after reports.
- **Aggregation reports:** 7 named reports, each independently runnable.
- **Materialized views:** `daily_sales_summary` and `top_products_summary`, refreshed **incrementally** using the existing `id_run` mechanism.
- **Scheduled jobs:** 2 jobs with a fixed schedule, manual triggering, and a persistent execution log.
- **Unified FastAPI interface:** one API to run and test everything (Swagger UI at `/docs`).

---


# Hybrid Orders Data Pipeline

A hybrid ELT pipeline for processing order data at scale. It automatically routes small files to a Python batch loader and large files to a PySpark distributed engine, with integrated data quality rules and automatic record classification.

---

## Architecture & Stages

The project is organized into clear, interconnected stages.

### Phase 1 stages (pipeline)

| Stage | File | Description |
|-------|-------|-------|
| **Stage 3** | `file_router.py` | Inspects the file size and chooses the engine: `python_batch` or `pyspark`. |
| **Stage 4** | `spark_loader.py` | PySpark engine: distributed read + parallel write to MongoDB. |
| **Stage 5** | `raw_builder.py` | Builds the raw-layer record (`orders_raw`) in a unified structure. |
| **Stage 6** | `quality_rules.py` | 11+ cleaning/validation rules (Arabic numerals, currencies, dates, phones, emails, etc.). |
| **Stage 7** | `quality_rules.py` | Audit trail: every correction is logged as `{field, original_value, corrected_value, rule_code}`. |
| **Stage 8** | `classification.py` | Classifies the record as `valid` / `corrected` / `quarantined`. |
| **Stage 9** | `mongo_setup.py` | Sets up the collections: Schema Validation for `orders_validated`, plus unique indexes. |
| **Stage 10** | `elt_pipeline.py` | Smart upsert (Idempotency) for both `orders_validated` and `quarantine_orders`. |
| **Stage 11** | `elt_pipeline.py` | Consistency Check: `run_raw_count == valid + corrected + quarantine`. |
| **Stage 12** | `metrics.py` | Saves run metrics into `reports/results.json`. |
| **Stage 13** | `test_*.py` | Unit tests for the quality rules and the classification logic. |

### Phase 2 modules (final project)

| Requirement | File | Description |
|-------|-------|-------|
| Queries | `queries.py` | 6 practical queries; parameters default to values sampled from the data. |
| Indexes | `indexes.py` | 5 query indexes + 1 materialized-view index, each with its documented rationale. |
| Explain | `explain_report.py` | `explain("executionStats")` before/after creating the indexes → `reports/explain_report.md/.json`. |
| Aggregations | `aggregations.py` | 7 named aggregation reports. |
| Materialized views | `materialized_views.py` | `daily_sales_summary`, `top_products_summary`; incremental refresh by `id_run`. |
| Scheduled jobs | `jobs.py` | APScheduler jobs + manual run + execution log in `job_runs`. |
| API | `api.py` | FastAPI: unified run/test interface; Swagger at `/docs`. |
| Smoke test | `smoke_test.py` | Calls every endpoint and prints PASS/FAIL. |
| Settings | `config/final_settings.py` | Phase 2 settings (collection names, scheduler, upload folder). |

### Flow Diagram (Phase 1)

```
┌─────────────────┐
│  Input CSV File │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────────┐
│  file_router.py │────▶│ python_batch     │ (if size ≤ 200 MB)
│  (choose engine)│     │ batch_loader.py  │
└─────────────────┘     └──────────────────┘
         │
         └──────────────▶┌──────────────────┐
                         │ pyspark          │ (if size > 200 MB)
                         │ spark_loader.py  │
                         └──────────────────┘
                                    │
                                    ▼
                         ┌──────────────────┐
                         │  orders_raw      │  (MongoDB)
                         │  (Raw Layer)     │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │ quality_rules.py │
                         │ classification.py│
                         └────────┬─────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
            ┌──────────┐  ┌──────────┐  ┌──────────────┐
            │  valid   │  │ corrected│  │  quarantined │
            └────┬─────┘  └────┬─────┘  └──────┬───────┘
                 │             │               │
                 ▼             ▼               ▼
        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
        │orders_validated│ │orders_validated│ │quarantine_orders│
        │  (upsert)    │ │  (upsert)    │ │  (upsert)    │
        └──────────────┘ └──────────────┘ └──────────────┘
```

### Flow Diagram (Phase 2)

```
                         orders_validated (Phase 1 output, every doc carries id_run)
                                  │
        ┌─────────────────────────┼──────────────────────────────┐
        ▼                         ▼                              ▼
  queries.py + indexes.py   aggregations.py            materialized_views.py
  (+ explain_report.py)     (7 reports)                (incremental by id_run)
        │                         │                       │              │
        │                         │                daily_sales_summary  top_products_summary
        │                         │                       │              │
        └──────────┬──────────────┴───────────┬───────────┴──────────────┘
                   ▼                          ▼
                api.py (FastAPI)        jobs.py (APScheduler → job_runs log)
```

---

## Features

| Feature | Description |
|---------|-------------|
| **Smart File Routing** (`file_router.py`) | Auto-selects Python Batch (≤ 200 MB) or PySpark (> 200 MB) based on file size |
| **Dual Loading Engines** | `python_batch` for streaming reads, `pyspark` for distributed parallel processing |
| **Data Quality Rules** (`quality_rules.py`) | Cleans Arabic numerals, normalizes currencies, standardizes dates, fixes phones/emails, and more |
| **Automatic Classification** (`classification.py`) | Classifies every record as `valid`, `corrected`, or `quarantined` |
| **ELT Pipeline** (`elt_pipeline.py`) | Loads processed data into MongoDB across three collections: `orders_raw`, `orders_validated`, and `quarantine_orders` |
| **Metrics Tracking** (`metrics.py`) | Appends run metrics to `reports/results.json` for monitoring |
| **Indexed queries + Explain** (`queries.py`, `indexes.py`, `explain_report.py`) | Practical queries with measured before/after index impact |
| **Aggregation reports** (`aggregations.py`) | Sales by city / period, top customers, status distribution, payment methods, quarantine breakdown, top products |
| **Materialized views** (`materialized_views.py`) | Pre-computed daily sales and top products, refreshed incrementally |
| **Scheduled jobs** (`jobs.py`) | Periodic MV refresh and periodic sales report, with an execution log |
| **Unified API** (`api.py`) | One FastAPI interface to ingest, query, aggregate, refresh, and run jobs |

---

## Requirements

- Python 3.10+
- MongoDB 5.0+ (local or remote; tested with 8.0.11) — **must be running before starting anything**
- Java 8+ (only required when using PySpark, i.e. input files larger than 200 MB)

Python packages (installed from `requirements.txt`): `pymongo`, `pyspark`, `python-dotenv`, `pytest`, `fastapi`, `uvicorn`, `python-multipart`, `apscheduler`.

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Project Structure

```
hybrid-orders-etl-pipeline/
│
├── .gitignore
├── .env
├── requirements.txt
├── make_sample.py
├── README.md
│
├── config/
│   ├── settings.py              # Phase 1 settings
│   └── final_settings.py        # Phase 2 settings
│
├── src/
│   ├── main.py                  # pipeline entry point (CLI + run_pipeline() used by the API)
│   ├── file_router.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── raw_builder.py
│   ├── elt_pipeline.py
│   ├── classification.py
│   ├── quality_rules.py
│   ├── mongo_setup.py
│   ├── metrics.py
│   ├── create_small_sample.py
│   ├── test_classification.py
│   ├── test_cleaning_rules.py
│   │
│   ├── queries.py               # Phase 2
│   ├── indexes.py               # Phase 2
│   ├── explain_report.py        # Phase 2
│   ├── aggregations.py          # Phase 2
│   ├── materialized_views.py    # Phase 2
│   ├── jobs.py                  # Phase 2
│   ├── api.py                   # Phase 2
│   ├── smoke_test.py            # Phase 2
│   └── test_materialized_views.py   # Phase 2
│
├── reports/
│   ├── results.json             # Phase 1 run metrics (appended per run)
│   ├── results.md
│   ├── explain_report.md/.json  # generated by POST /indexes?explain=true
│   └── daily_report_*.json      # generated by the daily_sales_report job
│
└── screenshots/
    ├── batch_evidence/          # evi1.png, evi2.png
    └── spark_evidence/          # evi1.png, evi2.png
```

> **Note:** The `data/` directory (including `data/uploads/` for files uploaded through the API) is created locally at runtime and is not a part of the repository.

---

## Setup

```bash
git clone <repo-url>
cd <project-folder>
pip install -r requirements.txt
```

1. **Start MongoDB** and make sure it listens on `mongodb://localhost:27017` (the default).
   - Windows: `Get-Service MongoDB` (PowerShell) should show `Running`; otherwise `net start MongoDB` (as Administrator) or run `mongod` in a separate window.
2. **Configuration is optional.** Every setting has a working default in `config/settings.py` / `config/final_settings.py`. To override anything (e.g. a remote MongoDB), copy `.env.example` to `.env` and edit it:
   - Windows: `copy .env.example .env`   |   Linux/macOS: `cp .env.example .env`
   - `.env.example` contains placeholders only — no real credentials.
3. **Java** is needed only if you ingest a file larger than 200 MB (PySpark route).

---

## Usage — Phase 1 (pipeline)

### Run the Full Pipeline

Place your CSV file in the project directory (or reference it by path), then run:

```bash
py -m src.main --input data/orders_sample.csv
```

(Use `python` instead of `py` on Linux/macOS.) The router will automatically choose the best engine based on file size. A successful run ends with `consistency_check : True`, which means `run_raw_count == count_valid + count_corrected + count_quarantine`.

### Generate a Small Sample

To create a smaller CSV sample from a large source file for quick testing:

```bash
py -m src.create_small_sample --input <your-large-file>.csv --output sample.csv --rows 100000
```

> Replace `<your-large-file>.csv` with your actual source file name, and adjust `--rows` as needed.

Then run the pipeline on the sample:

```bash
py -m src.main --input data/sample.csv
```

---

## Phase 2 — Final Additions

Everything below works on top of the **same pipeline and the same MongoDB data** — nothing in Phase 1 was rebuilt. The only Phase 1 file touched is `src/main.py`, where the pipeline body was moved into a callable `run_pipeline()` (behavior and CLI unchanged) so that `POST /ingest` can call the same pipeline.

No file names, record counts, ids or results are hardcoded: query parameters default to values sampled from the data currently in MongoDB, and all reports are computed from whatever data was ingested.

### Quick start for evaluators (step by step)

1. Start MongoDB (see [Setup](#setup)) and install dependencies: `pip install -r requirements.txt`.
2. *(Optional, recommended for a clean test)* use a fresh database so your data is isolated:
   - PowerShell: `$env:MONGO_DB_NAME="eval_db"`
   - cmd: `set MONGO_DB_NAME=eval_db`
3. Start the API from the project root: `py -m uvicorn src.api:app --port 8000`
4. Open **http://127.0.0.1:8000/docs** (Swagger UI) and run, in this order:
   1. `GET /health` → `status: ok`, `mongo: up`.
   2. `POST /ingest` → *Choose File* (your CSV) → Execute. Expect `consistency_check: true` in the returned metrics.
   3. `POST /indexes` with `explain = true` → creates the indexes and writes `reports/explain_report.md` (before/after).
   4. `GET /queries`, then `GET /queries/{name}` for each query.
   5. `GET /aggregations`, then `GET /aggregations/{name}` for each report.
   6. `POST /refresh-mv` → first call `status: refreshed`; call it again → `status: up_to_date` (nothing new to process).
   7. *(Incremental proof)* `POST /ingest` another file, then `POST /refresh-mv` → `pending_runs: 1` and only the affected days are recomputed.
   8. `GET /mv/daily_sales_summary` and `GET /mv/top_products_summary` to view the views.
   9. `GET /jobs`, then `POST /jobs/refresh_materialized_views/run` and `POST /jobs/daily_sales_report/run`.
5. **Automated check of every endpoint** (in a second terminal, while the API is running):
   ```bash
   py -m src.smoke_test
   ```
   It must end with `N/N checks passed`. To include ingestion: `py -m src.smoke_test --input <csv path on this machine>` (use a path without special characters).
6. Unit tests: `py -m pytest -q`

### Run the API

```bash
py -m uvicorn src.api:app --host 127.0.0.1 --port 8000
```

- Swagger UI: `http://127.0.0.1:8000/docs`
- Run with **one worker** only (the scheduler lives inside the API process).
- The scheduler starts automatically with the API (disable with `ENABLE_SCHEDULER=false`).

### API endpoints

| Method | Endpoint | What it does |
|---|---|---|
| GET | `/health` | MongoDB ping + scheduler state. |
| POST | `/ingest` | Runs the **same Phase 1 pipeline** (router → loader → ELT → metrics). Upload a CSV (`file`) or send a server path (`input_path`). Uploaded files are stored in `data/uploads/`. Returns the run metrics. |
| POST | `/indexes` | Creates all indexes. With `explain=true`: drops them, runs `explain("executionStats")`, recreates them, runs explain again, and writes `reports/explain_report.md/.json`. |
| GET | `/queries` | Lists the available queries with their index and parameters. |
| GET | `/queries/{name}` | Runs one query. Optional parameters via query string, e.g. `?customer_id=...&limit=20`. |
| GET | `/aggregations` | Lists the aggregation reports. |
| GET | `/aggregations/{name}` | Runs one report (e.g. `?limit=5`, or `?granularity=day` for `sales_by_period`). |
| POST | `/refresh-mv` | Incremental refresh of the materialized views. `?mv=all\|daily_sales_summary\|top_products_summary`; `&full=true` forces a complete rebuild. |
| GET | `/mv/{name}` | Reads a materialized view (inspection helper). |
| GET | `/jobs` | Lists the jobs with schedule, next run, last run and recent runs. |
| POST | `/jobs/{name}/run` | Runs a job manually and logs the result. |

All responses are JSON. Errors return a JSON `detail` with the proper HTTP status (400 / 404 / 500 / 503).

### 1. Queries, indexes and Explain

| Query | Collection | Index used |
|---|---|---|
| `order_by_id` | `orders_validated` | `uniq_id_order` (unique, created by `mongo_setup.py`) |
| `orders_by_customer` | `orders_validated` | `idx_validated_customer` |
| `orders_by_city_status` | `orders_validated` | `idx_validated_city_status_date` — **compound**: `city`, `status`, `order_date` (desc) |
| `orders_by_date_range` | `orders_validated` | `idx_validated_order_date` |
| `quarantined_by_error_code` | `quarantine_orders` | `idx_quarantine_codes` (multikey on `codes_error`) |
| `orders_by_run` | `orders_validated` | `idx_validated_id_run` |

Why each index:

- `idx_validated_customer` — equality lookup on `customer_id`; without it MongoDB scans the whole collection.
- `idx_validated_city_status_date` — two equality fields first, then the sort field (Equality-Sort order), so filtering, sorting and `limit` are served from the index with **no in-memory SORT**.
- `idx_validated_order_date` — range query on the standardized ISO date string; also used by the materialized-view refresh.
- `idx_validated_id_run` — the incremental refresh finds new/changed records by `id_run`.
- `idx_quarantine_codes` — `codes_error` is an array, so a multikey index finds all records with a given error code.
- `idx_top_products_date` — lets the top-products refresh replace only the affected date ranges.

**Explain (before / after).** `POST /indexes?explain=true` (or `py -m src.explain_report`) runs `explain("executionStats")` for `orders_by_customer`, `orders_by_city_status` and `orders_by_date_range` with the indexes dropped, then recreates them and measures again. The generated `reports/explain_report.md` shows, per query: scan type (COLLSCAN → IXSCAN), plan stages, whether an in-memory sort is used, `nReturned`, `totalKeysExamined`, `totalDocsExamined`, `executionTimeMillis`, the reason for the index, and the measured impact.

### 2. Aggregation reports

| Report (`/aggregations/{name}`) | Description |
|---|---|
| `sales_by_city` | Orders count, total sales and average order value per city. |
| `top_customers` | Top customers by total spent. |
| `orders_by_status` | Distribution of orders by status. |
| `sales_by_period` | Sales per period; `granularity=day\|month\|year` (default `month`). |
| `payment_methods` | Orders and paid amounts by payment method and payment status. |
| `quarantine_by_error_code` | Number of quarantined records per error code. |
| `top_products` | Best-selling products (reads the `top_products_summary` materialized view). |


### 3. Materialized views (incremental refresh)

| View | Grain | Fields |
|---|---|---|
| `daily_sales_summary` | one row per day | `date`, `orders_count`, `total_sales`, `avg_order_value`, `total_delivery_cost`, `refreshed_at` |
| `top_products_summary` | one row per day × product | `date`, `product`, `product_name`, `quantity`, `revenue`, `order_lines`, `refreshed_at` |

Both are built from aggregation results (`daily_sales_summary` with `$merge`; `top_products_summary` by parsing `items_json`, tolerant to field names such as `sku`/`product_id`/`name`, `qty`/`quantity`, `total`/`unit_price`).

**Incremental refresh mechanism.** This reuses the Phase 1 mechanism: every document in `orders_validated` carries the `id_run` of the run that last wrote it (upsert). For each view, the collection `mv_state` stores which `id_run` values were already processed. A refresh:

1. finds the unprocessed runs (`distinct(id_run)` minus processed runs);
2. finds the **days** touched by those runs;
3. recomputes **only those days** with *replace* semantics (idempotent and safe for orders updated by a later run);
4. removes stale rows of those days and marks the runs as processed.

If nothing is new, the refresh returns `status: up_to_date` immediately. `full=true` (complete rebuild) exists only as a fallback.

Run: `POST /refresh-mv`, or `py -m src.materialized_views [--mv <name>] [--full]`.

### 4. Scheduled jobs

| Job | Schedule | Action |
|---|---|---|
| `refresh_materialized_views` | every `JOB_MV_REFRESH_INTERVAL_MINUTES` (default **15**) | Incremental refresh of both views. |
| `daily_sales_report` | cron `JOB_DAILY_REPORT_CRON` (default `0 2 * * *`, i.e. 02:00 daily, in `JOB_TIMEZONE`, default UTC) | Builds `reports/daily_report_<timestamp>.json` from the aggregation reports. |

- Jobs start automatically with the API (APScheduler) and can be **run manually at any time**: `POST /jobs/{name}/run`, or `py -m src.jobs run <name>` / `py -m src.jobs list`.
- Every execution (scheduled or manual) is logged in the MongoDB collection **`job_runs`**: `job_name`, `trigger` (`scheduled`/`manual`), `started_at`, `ended_at`, `duration_seconds`, `status` (`success`/`failed`), `result`, `error`.
- A job that is already running is skipped, never executed twice concurrently.
- Standalone scheduler without the API: `py -m src.jobs scheduler`.

### 5. Tests

```bash
py -m pytest -q
```

Covers the Phase 1 quality rules and classification, plus the Phase 2 helpers (date-range merging for incremental refresh, `items_json` parsing). `py -m src.smoke_test` exercises every API endpoint against a running server.

### Configuration

All settings have defaults; set them in `.env` or as environment variables only if you need to change them.

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB_NAME` | `orders_pipeline_db` | Database name |
| `ENABLE_SCHEDULER` | `true` | Start the scheduler with the API |
| `JOB_TIMEZONE` | `UTC` | Time zone for the schedules |
| `JOB_MV_REFRESH_INTERVAL_MINUTES` | `15` | Interval of the MV refresh job |
| `JOB_DAILY_REPORT_CRON` | `0 2 * * *` | Cron of the daily report job |
| `UPLOAD_DIR` | `data/uploads` | Where uploaded CSVs are stored |

### Verified results

Measured on a real run with **2,339,748** validated orders (121 days, 10 cities, 12 products) on MongoDB 8.0.11.

**Explain — before vs after indexes**

| Query | Before | After |
|---|---|---|
| `orders_by_customer` | COLLSCAN, 2,339,748 docs examined, 2482 ms | IXSCAN, 1 doc examined, 29 ms |
| `orders_by_city_status` | COLLSCAN + in-memory SORT, 2,339,748 docs, 2693 ms | IXSCAN (no SORT), 50 docs, 14 ms |
| `orders_by_date_range` | COLLSCAN + in-memory SORT, 2,339,748 docs, 2834 ms | IXSCAN (no SORT), 100 docs, 1 ms |

**Other checks**

- `sales_by_city`: the order counts of the 10 cities add up to exactly 2,339,748 (= number of validated orders).
- `daily_sales_summary`: 121 rows (one per day); `top_products_summary`: 1,452 rows (12 products × 121 days).
- Incremental refresh: `status: up_to_date` in ~4 ms when nothing is new, versus ~11.5 s (daily sales) and ~77.8 s (top products) for a full rebuild.
- Scheduler: the `refresh_materialized_views` job ran on its own schedule (`trigger: scheduled`), and manual runs of both jobs returned `status: success`; all are logged in `job_runs`.
- `py -m src.smoke_test` passes for every endpoint.

### Troubleshooting

| Symptom | Fix |
|---|---|
| `/health` returns 503 / `mongo: down` | MongoDB is not running or `MONGO_URI` is wrong. Start MongoDB and retry. |
| `ModuleNotFoundError` (fastapi, apscheduler, …) | Run `pip install -r requirements.txt`. |
| `Address already in use` | Another API instance is running on port 8000; stop it or use `--port 8001`. |
| `POST /ingest` takes very long | Ingestion is synchronous and the response is returned when the run finishes; large files (≈1 GB) take ~20 minutes. Use a smaller file for a quick test. |
| Input file larger than 200 MB fails | The PySpark route needs Java 8+ and internet access the first time (Spark–MongoDB connector download). |
| `top_products` is empty | The `items_json` field names in your data differ from `sku`/`product_id`/`name`, `qty`/`quantity`, `total`/`unit_price`. |
| Want an isolated, empty database | Set `MONGO_DB_NAME` to a new name before starting the API (see Quick start, step 2). |

### Known limitations

- Quantities in `items_json` are used as they appear; negative quantities (present in some source rows) are counted as-is in `top_products_summary`. Revenue uses the item `total` when present.
- "Sales" include all validated orders regardless of status (including cancelled).
- If an order's date changes between two runs, the new day is refreshed automatically; the old day is corrected on the next `full=true` refresh.
- The scheduler runs inside the API process; if the API is stopped, scheduled runs stop (jobs can still be run manually).

---

## How It Works

### `config/settings.py`
Central configuration for paths, MongoDB collection names, CSV columns, quarantine error codes, and quality status values.

### `src/file_router.py`
Measures the input file size and selects the appropriate engine:
- **≤ 200 MB** → `python_batch` (streaming read + batch inserts)
- **> 200 MB** → `pyspark` (distributed read + write via Spark MongoDB Connector)

### `src/batch_loader.py`
Reads the CSV line-by-line using `csv.DictReader`, builds `orders_raw` records via `raw_builder.py`, and writes them to MongoDB in configurable batches.

### `src/spark_loader.py`
Uses PySpark with a fixed schema to read the CSV, enriches records with metadata (`id_run`, `at_ingested`, `number_row_source`), and writes directly to MongoDB.

### `src/quality_rules.py`
Applies 11 cleaning and validation rules:

| Rule | Description |
|------|-------------|
| Arabic Numerals | Converts Arabic-Indic digits (٠١٢٣٤٥٦٧٨٩) and separators to Latin |
| Currency Code | Normalizes known aliases (e.g., `ريال`, `YER`) to `YER` |
| Thousands Separator | Removes comma separators from numeric strings |
| Price Words | Converts known Arabic price words (`ألف`, `ثلاثة آلاف`, etc.) to numbers |
| Phone | Strips spaces, dashes, and parentheses from phone numbers |
| Email | Fixes repeated symbols (`@@` → `@`, `..` → `.`) |
| Date | Standardizes dates to `YYYY-MM-DDTHH:MM:SS` |
| Whitespace & Synonyms | Normalizes status values (`مؤكد` → `confirmed`, `مدفوع` → `paid`, etc.) |
| Items JSON | Validates that `items_json` is well-formed and non-empty |
| Negative Value Flag | Flags negative amounts without guessing the correct value |
| Missing IDs | Flags missing or blank `order_id` and `customer_id` |

### `src/classification.py`
Runs `apply_quality_rules()` on each record and classifies it:
- **`valid`** — no errors, no corrections needed
- **`corrected`** — safe automatic fixes applied, no critical errors
- **`quarantined`** — contains unresolvable errors (missing ID, invalid date, corrupted JSON, etc.)

### `src/elt_pipeline.py`
Fetches all `orders_raw` records for a given `id_run`, classifies them, and writes:
- Valid/corrected records to `orders_validated` (upsert on `id_order`)
- Quarantined records to `quarantine_orders` (upsert on `file_source` + `number_row_source`)

### `src/mongo_setup.py`
Sets up MongoDB collections and indexes:
- `orders_raw` — plain collection, no constraints
- `orders_validated` — JSON Schema validator + unique index on `id_order`
- `quarantine_orders` — unique compound index on `(file_source, number_row_source)`

### `src/metrics.py`
Tracks and stores run metrics (row counts, engine used, timing, throughput, error breakdowns) in `reports/results.json`.

### `src/main.py`
Runs the full pipeline end-to-end (router → loader → ELT → metrics). Exposes `run_pipeline(input_path)`, used both by the command line and by `POST /ingest`.

### `src/queries.py`, `src/indexes.py`, `src/explain_report.py`
Practical queries with defaults sampled from the data; index definitions with their rationale; before/after `explain("executionStats")` reports.

### `src/aggregations.py`
Seven named aggregation reports over `orders_validated`, `quarantine_orders` and the top-products view.

### `src/materialized_views.py`
Builds and incrementally refreshes `daily_sales_summary` and `top_products_summary`, tracking processed runs in `mv_state`.

### `src/jobs.py`
Defines the scheduled jobs, runs them on schedule or manually, and logs each execution in `job_runs`.

### `src/api.py`
FastAPI application exposing the whole project through one JSON interface (Swagger at `/docs`).

### `src/smoke_test.py`
Calls every endpoint of a running API and prints PASS/FAIL per check.


---

> *Built to handle messy real-world data with clean, predictable logic — one record at a time, or a million.*
