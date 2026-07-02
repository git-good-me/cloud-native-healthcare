# Cloud-Native Healthcare Data Platform

A production-grade data engineering portfolio project built on real CMS Hospital Compare data. Demonstrates end-to-end lakehouse architecture across ingestion, transformation, quality validation, orchestration, and analytics — using Python, PySpark, AWS, and Apache Airflow.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   CMS Hospital Compare (70 CSV files × 9 quarterly snapshots)  │
│   OpenFDA Drug Adverse Events API                               │
└───────────────────┬─────────────────────────┬───────────────────┘
                    │                         │
                    ▼                         ▼
┌───────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER  (S3)                           │
│   Raw Parquet — schema-on-read, no transforms, full history       │
│   s3://cloud-native-healthcare-bronze/                            │
│   3.8M rows  ·  70 files  ·  9 snapshots                         │
└───────────────────────────────┬───────────────────────────────────┘
                                │  PySpark (boto3 download pattern)
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER  (S3)                           │
│   Cleaned · Deduplicated · Standardised · Type-enforced           │
│   s3://cloud-native-healthcare-silver/                            │
│   8.8M rows across 7 tables                                       │
│                                                                   │
│   hospital_general_information_latest   (5,624 rows)             │
│   hospital_general_information_history  (54,079 rows)            │
│   complications_and_deaths              (928,258 rows)           │
│   hcahps                                (4,210,432 rows)         │
│   healthcare_associated_infections      (1,722,564 rows)         │
│   unplanned_hospital_visits             (669,886 rows)           │
│   medicare_spending                     (46,194 rows)            │
└───────────────────────────────┬───────────────────────────────────┘
                                │  pandas + pyarrow
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER  (S3)                            │
│   Star schema  ·  BI-ready  ·  Athena-queryable                  │
│   s3://cloud-native-healthcare-gold/                              │
│                                                                   │
│   dim_hospital              (5,624 rows)   — facility dimension  │
│   fact_hospital_ratings     (54,079 rows)  — ratings over time   │
│   fact_quality_metrics      (47,849 rows)  — complications/HAI   │
│   fact_patient_satisfaction (47,849 rows)  — HCAHPS surveys      │
│   gold_hospital_scorecard   (5,624 rows)   — composite KPIs      │
└───────────────────────────────┬───────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
          ┌─────────────────┐    ┌──────────────────────┐
          │  AWS Athena     │    │  Great Expectations   │
          │  SQL analytics  │    │  Data quality checks  │
          │  7 queries      │    │  19/19 passing        │
          └─────────────────┘    └──────────────────────┘
                    │
          ┌─────────────────┐
          │  Apache Airflow │
          │  Weekly DAG     │
          │  6-task pipeline│
          └─────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Batch processing | PySpark 4.1.1 (local mode) |
| Cloud storage | AWS S3 (3 medallion buckets) |
| Analytics engine | AWS Athena |
| Orchestration | Apache Airflow |
| Data quality | Great Expectations 1.18 |
| Data format | Apache Parquet (pyarrow) |
| S3 connectivity | boto3 |
| External API | OpenFDA drug adverse events |

---

## Data Sources

**CMS Hospital Compare** — real quarterly snapshots from the Centers for Medicare & Medicaid Services. 9 snapshots spanning January 2024 to May 2026. 70+ CSV files per snapshot covering hospital ratings, complications, infections, patient surveys, readmissions, and Medicare spending.

**OpenFDA API** — drug adverse event reports. Pulls hospitalization-related serious events for specified drugs (default: acetaminophen). No authentication required.

Real data was chosen deliberately over synthetic datasets to demonstrate the ability to work with messy, inconsistent, real-world sources.

---

## Project Structure

```
Cloud-Native Healthcare/
├── ingestion/
│   ├── ingest_bronze.py          # CSV → Parquet → S3 Bronze
│   ├── upload_to_s3.py           # Bulk S3 upload utility
│   └── openfda_api.py            # OpenFDA REST API ingestion
│
├── transformation/
│   ├── transform_silver.py       # Bronze → Silver (pandas)
│   └── transform_gold.py        # Silver → Gold star schema (pandas)
│
├── dlt_pipeline/
│   ├── 01_bronze_to_silver_pyspark.py   # PySpark Bronze → Silver
│   └── 02_silver_to_gold_pyspark.py     # pandas Silver → Gold (DLT layer)
│
├── sql_models/
│   ├── analytics_queries.sql     # 7 Athena analytics queries
│   └── query_results/            # Saved Athena results
│
├── orchestration/
│   └── healthcare_dag.py         # Airflow DAG (weekly schedule)
│
├── quality/
│   └── ge_quality_checks.py      # Great Expectations validation
│
├── config/
│   └── .env                      # AWS credentials (gitignored)
│
├── data/
│   ├── raw/                      # Source CSVs (gitignored)
│   └── processed/                # Local Parquet staging (gitignored)
│
└── activate.ps1                  # Windows venv + JAVA_HOME setup
```

---

## Setup

### Prerequisites

- Python 3.12
- Java 17 (for PySpark) — [Adoptium JDK](https://adoptium.net/)
- AWS account with S3 and Athena access
- Git

### Installation

```powershell
git clone https://github.com/git-good-me/cloud-native-healthcare.git
cd "Cloud-Native Healthcare"
python -m venv venv
.\activate.ps1

pip install pyspark==4.1.1 pandas pyarrow boto3 python-dotenv requests \
            great-expectations loguru apache-airflow
```

### AWS Configuration

Create `config/.env`:

```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
```

Create three S3 buckets:
- `cloud-native-healthcare-bronze`
- `cloud-native-healthcare-silver`
- `cloud-native-healthcare-gold`

---

## Running the Pipeline

Each step is independent and idempotent.

```powershell
# 1. Activate environment (run every session)
.\activate.ps1

# 2. Ingest raw CMS data to Bronze
python ingestion/ingest_bronze.py

# 3. Upload Bronze to S3
python ingestion/upload_to_s3.py

# 4. Bronze → Silver (PySpark local)
python dlt_pipeline/01_bronze_to_silver_pyspark.py

# 5. Silver → Gold (pandas star schema)
python dlt_pipeline/02_silver_to_gold_pyspark.py

# 6. Ingest OpenFDA drug data
python ingestion/openfda_api.py --drug acetaminophen --limit 500

# 7. Run data quality checks
python quality/ge_quality_checks.py --layer gold

# 8. Query Gold data via Athena
# Run sql_models/analytics_queries.sql in the Athena console
```

---

## Key Findings (from Athena Analytics)

Queries run against live Gold data in S3 via AWS Athena:

- **Best performing state:** Utah — 4.24 average hospital star rating
- **Worst infection score:** BronxCare Health System — SIR of 6,966
- **Patient satisfaction correlation:** Perfect linear relationship between overall star rating and patient satisfaction scores (confirmed via Athena query)
- **Coverage:** 5,624 unique hospitals across all 50 US states + territories
- **Time range:** 9 quarterly snapshots, January 2024 – May 2026

---

## Data Quality

Great Expectations validates Gold tables after each pipeline run:

```
✓  dim_hospital              8/8 checks passed
✓  fact_hospital_ratings     5/5 checks passed
✓  gold_hospital_scorecard   6/6 checks passed
```

Checks include: column existence, null constraints, uniqueness, value range validation, row count thresholds, and state code format validation.

---

## Orchestration

The Airflow DAG (`orchestration/healthcare_dag.py`) runs weekly (Sundays 02:00 UTC):

```
start
  ├── openfda_ingest
  └── bronze_validation
            └── bronze_to_silver
                      └── silver_validation
                                └── silver_to_gold
                                          └── gold_validation
                                                    └── pipeline_complete
```

---

## Architecture Decisions

**Why real CMS data instead of synthetic?** Real data has genuine messiness — inconsistent schemas across quarters, mixed-case values, sentinel strings like "Not Available", columns that change meaning between snapshots. Handling this demonstrates skills that synthetic data hides.

**Why boto3 download → local PySpark → boto3 upload instead of s3a://?** PySpark 4.1.1 has JAR incompatibilities with hadoop-aws for direct S3 access. The boto3 boundary pattern is more explicit, easier to debug, and avoids the entire JAR dependency problem.

**Why pandas for Gold transforms instead of Spark?** PySpark's `toPandas()` and `collect()` require spawning Python worker subprocesses that fail on Windows + venv environments. Since Silver data already passes through pandas (to handle nanosecond timestamp incompatibilities), doing Gold transforms in pandas eliminates an entire failure surface with no loss of correctness at this data volume.

---

## AWS Resources

| Resource | Name |
|---|---|
| S3 Bronze | `cloud-native-healthcare-bronze` |
| S3 Silver | `cloud-native-healthcare-silver` |
| S3 Gold | `cloud-native-healthcare-gold` |
| Athena database | `healthcare_lakehouse` |
| Athena tables | `dim_hospital`, `fact_hospital_ratings`, `fact_quality_metrics`, `fact_patient_satisfaction`, `gold_hospital_scorecard` |
| IAM user | `cloud-native-healthcare-user` (S3FullAccess, AthenaFullAccess) |

---

## Commits

| Commit | Description |
|---|---|
| Initial | Project scaffold, Bronze ingestion, S3 upload |
| `0d9ac22` | Fix PySpark Bronze → Silver pipeline |
| `d30cba0` | Add Silver → Gold DLT pipeline |
| Latest | OpenFDA ingestion, Airflow DAG, Great Expectations |