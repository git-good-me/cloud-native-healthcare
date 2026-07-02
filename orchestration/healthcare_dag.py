"""
Airflow DAG: cloud_native_healthcare_pipeline
=============================================
Orchestrates the full data pipeline on a weekly schedule.

Pipeline order:
  1. openfda_ingest          — pull latest FDA adverse event data to Bronze
  2. bronze_validation       — verify Bronze S3 files are present
  3. bronze_to_silver        — PySpark local transform (01_bronze_to_silver_pyspark.py)
  4. silver_validation       — check Silver tables exist in S3
  5. silver_to_gold          — pandas transform (02_silver_to_gold_pyspark.py)
  6. gold_validation         — verify all 5 Gold tables in S3
  7. pipeline_complete       — terminal success marker

Setup (Windows via WSL, or Linux/Mac):
  pip install apache-airflow boto3 python-dotenv
  export AIRFLOW_HOME=~/airflow
  airflow db migrate
  airflow users create --username admin --password admin --role Admin \
          --email admin@example.com --firstname F --lastname L
  cp orchestration/healthcare_dag.py ~/airflow/dags/
  airflow webserver -p 8080 &
  airflow scheduler &
  # Open http://localhost:8080

Windows native alternative (no WSL):
  Use Task Scheduler to run each script in sequence.
  See docs/windows_scheduling.md for a PowerShell wrapper.

Note on PROJECT_ROOT:
  Update the path below to match your environment.
  WSL example:  /mnt/c/STUDY/Projects/Cloud-Native Healthcare
  Linux/Mac:    /home/<user>/projects/cloud-native-healthcare
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Project config ────────────────────────────────────────────────────────────
# Update PROJECT_ROOT to your environment
PROJECT_ROOT  = "/mnt/c/STUDY/Projects/Cloud-Native Healthcare"
PYTHON        = f"{PROJECT_ROOT}/venv/bin/python"    # WSL venv path
BRONZE_BUCKET = "cloud-native-healthcare-bronze"
SILVER_BUCKET = "cloud-native-healthcare-silver"
GOLD_BUCKET   = "cloud-native-healthcare-gold"

EXPECTED_SILVER_TABLES = [
    "hospital_general_information_latest",
    "hospital_general_information_history",
    "complications_and_deaths",
    "hcahps",
    "healthcare_associated_infections",
    "unplanned_hospital_visits",
    "medicare_spending",
]

EXPECTED_GOLD_TABLES = [
    "dim_hospital",
    "fact_hospital_ratings",
    "fact_quality_metrics",
    "fact_patient_satisfaction",
    "gold_hospital_scorecard",
]

# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner":            "faiz",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ── Validation helpers ────────────────────────────────────────────────────────

def _s3_client():
    import os, boto3
    from dotenv import load_dotenv
    load_dotenv(f"{PROJECT_ROOT}/config/.env")
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def _list_prefixes(s3, bucket: str, prefix: str) -> set[str]:
    """Return immediate child prefix names under a given prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    found = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"].rstrip("/").split("/")[-1]
            found.add(name)
    return found


def validate_bronze(**context):
    """Check Bronze bucket has parquet files — minimum 70 (one per core CSV)."""
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    count = sum(
        1
        for page in paginator.paginate(Bucket=BRONZE_BUCKET)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    )
    if count < 70:
        raise ValueError(
            f"Bronze validation failed: expected ≥70 parquet files, found {count}."
        )
    print(f"Bronze validation passed ✓ — {count} parquet files")
    context["ti"].xcom_push(key="bronze_file_count", value=count)


def validate_silver(**context):
    """Check all 7 Silver table prefixes exist in S3."""
    s3 = _s3_client()
    found = _list_prefixes(s3, SILVER_BUCKET, "silver/pyspark/")
    missing = [t for t in EXPECTED_SILVER_TABLES if t not in found]
    if missing:
        raise ValueError(f"Silver validation failed — missing tables: {missing}")
    print(f"Silver validation passed ✓ — tables: {sorted(found)}")


def validate_gold(**context):
    """Check all 5 Gold table prefixes exist in S3."""
    s3 = _s3_client()
    found = _list_prefixes(s3, GOLD_BUCKET, "gold/pyspark/")
    missing = [t for t in EXPECTED_GOLD_TABLES if t not in found]
    if missing:
        raise ValueError(f"Gold validation failed — missing tables: {missing}")
    print(f"Gold validation passed ✓ — tables: {sorted(found)}")

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="cloud_native_healthcare_pipeline",
    description="Weekly CMS hospital data pipeline: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * 0",   # every Sunday at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "medallion", "cms", "openfda"],
) as dag:

    # ── Start marker ──────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Task 1: OpenFDA ingestion (runs in parallel with bronze validation) ───
    openfda_ingest = BashOperator(
        task_id="openfda_ingest",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'{PYTHON} ingestion/openfda_api.py --drug acetaminophen --limit 500 && '
            f'{PYTHON} ingestion/openfda_api.py --drug metformin --limit 500'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── Task 2: Bronze validation ─────────────────────────────────────────────
    bronze_validation = PythonOperator(
        task_id="bronze_validation",
        python_callable=validate_bronze,
        provide_context=True,
    )

    # ── Task 3: Bronze → Silver ───────────────────────────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'{PYTHON} dlt_pipeline/01_bronze_to_silver_pyspark.py'
        ),
        execution_timeout=timedelta(hours=3),
    )

    # ── Task 4: Silver validation ─────────────────────────────────────────────
    silver_validation = PythonOperator(
        task_id="silver_validation",
        python_callable=validate_silver,
        provide_context=True,
    )

    # ── Task 5: Silver → Gold ─────────────────────────────────────────────────
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'{PYTHON} dlt_pipeline/02_silver_to_gold_pyspark.py'
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Task 6: Gold validation ───────────────────────────────────────────────
    gold_validation = PythonOperator(
        task_id="gold_validation",
        python_callable=validate_gold,
        provide_context=True,
    )

    # ── Terminal markers ──────────────────────────────────────────────────────
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    pipeline_failed = EmptyOperator(
        task_id="pipeline_failed",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    #
    #  start
    #    ├── openfda_ingest ─────────────────────────────────┐
    #    └── bronze_validation                               │
    #              └── bronze_to_silver                      │
    #                        └── silver_validation           │
    #                                  └── silver_to_gold    │
    #                                            └── gold_validation
    #                                                    ├── pipeline_complete  (all success)
    #                                                    └── pipeline_failed    (any failure)

    start >> [openfda_ingest, bronze_validation]
    bronze_validation >> bronze_to_silver >> silver_validation >> silver_to_gold >> gold_validation
    gold_validation >> [pipeline_complete, pipeline_failed]
    openfda_ingest >> pipeline_complete   # OpenFDA failure doesn't block core pipeline