# Cloud-Native Healthcare Data Platform

A production-style data lakehouse built with Python, PySpark, AWS S3, and Databricks Community Edition.

## Data Source
CMS Hospital Compare — 9 quarterly snapshots (2024–2026)

## Architecture
- **Bronze** — Raw ingestion from CMS CSV files → Parquet
- **Silver** — Cleaned, standardized, deduplicated Delta tables
- **Gold** — Star schema, business metrics, analytics-ready

## Tech Stack
Python · PySpark · Delta Lake · AWS S3 · Databricks · Apache Airflow · Great Expectations

## Structure
- /ingestion — Bronze ingestion pipeline
- /transformation — Silver + Gold PySpark transforms
- /dlt_pipeline — Databricks notebooks
- /sql_models — SQL/dbt models
- /tests — Data quality tests
- /config — Environment config

## Status
- [x] Bronze ingestion — 70 files, 3.8M rows, 9 snapshots
- [x] Silver transformation
- [x] Gold modeling
- [ ] Databricks integration
- [ ] AWS S3 integration
