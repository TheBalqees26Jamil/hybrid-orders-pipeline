# Hybrid Orders ETL Pipeline

<p align="center">
  <strong>A hybrid ETL system for processing order data with high quality and scalability</strong><br>
  <em>Hybrid Python Batch + PySpark ingestion with automated data-quality rules, quarantine layer, and full audit trail</em>
</p>

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Architecture & Stages](#architecture--stages)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Quality Rules](#data-quality-rules)
- [Engines](#engines)
- [Testing](#testing)
- [Notes & Troubleshooting](#notes--troubleshooting)

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

---


# Hybrid Orders Data Pipeline

A hybrid ELT pipeline for processing order data at scale. It automatically routes small files to a Python batch loader and large files to a PySpark distributed engine, with integrated data quality rules and automatic record classification.

---

## Architecture & Stages

The project is organized into clear, interconnected stages:

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

### Flow Diagram

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

---

## Requirements

- Python 3.10+
- MongoDB (local or remote)
- Java 8+ (only required when using PySpark)

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Project Structure

```
project-root/
│
├── .gitignore
├── requirements.txt
├── make_sample.py
├── README.md
│
├── config/
│   └── settings.py
│# Hybrid Orders Data Pipeline
└── src/
    ├── main.py
    ├── file_router.py
    ├── batch_loader.py
    ├── spark_loader.py
    ├── raw_builder.py
    ├── elt_pipeline.py
    ├── classification.py
    ├── quality_rules.py
    ├── mongo_setup.py
    ├── metrics.py
    ├── create_small_sample.py
    ├── test_classification.py
    └── test_cleaning_rules.py
```

> **Note:** The `data/` and `reports/` directories are created locally at runtime and are not part of the repository.

---

## Setup

```bash
git clone <repo-url>
cd project-root
pip install -r requirements.txt
```

---

## Usage

### Run the Full Pipeline

Place your CSV file in the project directory (or reference it by path), then run:

```bash
python -m src.main --input <your-file-name>.csv
```

> Replace `<your-file-name>.csv` with the actual name of your source file.

The router will automatically choose the best engine based on file size.

### Generate a Small Sample

To create a smaller CSV sample from a large source file for quick testing:

```bash
python -m src.create_small_sample --input <your-large-file>.csv --output sample.csv --rows 100000
```

> Replace `<your-large-file>.csv` with your actual source file name, and adjust `--rows` as needed.

Then run the pipeline on the sample:

```bash
python -m src.main --input sample.csv
```

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

---

## License

This is a student project — feel free to take it, modify it, and build on it.

---

> *Built to handle messy real-world data with clean, predictable logic — one record at a time, or a million.*
