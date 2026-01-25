# Amazon Reviews Big Data Pipeline — Architecture

## Overview

This project implements an end-to-end **Big Data analytics pipeline** for the Amazon Reviews (Appliances) dataset using **HDFS, Apache Spark, Hive, and Airflow**. The pipeline follows a **data lake architecture** with clear **Bronze (Raw) → Silver (Cleaned) → Gold (Curated)** layers and is fully orchestrated using Airflow.

---

## Objectives

* Ingest raw JSON data into HDFS
* Enforce schema control and data quality checks
* Produce analytics-ready curated data
* Expose curated data via Hive for SQL/BI access
* Orchestrate the pipeline reliably using Airflow

---

## Technology Stack

* **Storage:** HDFS
* **Processing:** Apache Spark (PySpark)
* **Query Layer:** Apache Hive (External Tables)
* **Orchestration:** Apache Airflow
* **Format:** Parquet (columnar)

---

## Data Lake Layers

### 1) Bronze / Raw Layer

* **Source:** Local JSON files (already Appliances-specific)
* **Location:** `hdfs:///amazon-review-analytics/big-data-pipeline/data/raw/`
* **Content:** Immutable raw JSON for traceability and reprocessing

### 2) Silver / Cleaned Layer

* **Location:** `hdfs:///amazon-review-analytics/big-data-pipeline/data/cleaned/`
* **Format:** Parquet
* **Actions:**

  * Explicit column selection
  * Schema flattening
  * Data quality detection (NULLs, empty strings, empty arrays)
  * Removal of unused columns (e.g., `price`)

### 3) Gold / Curated Layer

* **Location:** `hdfs:///amazon-review-analytics/big-data-pipeline/data/processed/appliance_reviews_curated/`
* **Format:** Parquet
* **Actions:**

  * INNER JOIN on `asin`
  * Business rule enforcement (mandatory fields, verified reviews)
  * Analytics-ready dataset

---

## Spark Job Responsibilities

| Job           | Script                  | Responsibility                                          |
| ------------- | ----------------------- | ------------------------------------------------------- |
| Ingest Raw    | `01_ingest_raw.py`      | Read raw JSON from HDFS, schema selection, DQ detection |
| Write Cleaned | `02_write_cleaned.py`   | Persist Silver data in Parquet                          |
| Join & Curate | `03_join_and_curate.py` | Join datasets and apply business rules                  |
| Write Gold    | `04_write_gold.py`      | Persist Gold data in Parquet                            |

---

## Hive Integration

* **Type:** External Table
* **Database:** `amazon_analytics`
* **Purpose:** Enable SQL access without duplicating data
* **Benefit:** Dropping the table does not delete HDFS data

---

## Airflow Orchestration

### DAG Flow

```
ingest_raw
  → write_cleaned
    → join_and_curate
      → write_gold
        → create_hive_table
```

### Key Points

* Airflow handles **orchestration only**
* Spark handles **all data processing**
* Hive DDL executed from version-controlled `.hive` file

---

## Design Principles

* Separation of concerns (storage vs compute vs orchestration)
* Idempotent and reproducible jobs
* Schema governance and data quality validation
* Interview-aligned, production-oriented structure

---

## Stopping Point

This architecture **intentionally stops before ML**. Sentiment analysis and feature engineering are handled in a separate ML module.

