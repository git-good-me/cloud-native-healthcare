from __future__ import annotations

"""
Notebook 01: Bronze to Silver PySpark transformation.

This runs locally with PySpark and uses boto3 at the S3 boundary:
1. Download Bronze Parquet files from S3 to a local staging folder.
2. Transform the local Parquet files with PySpark.
3. Write Silver Parquet files locally.
4. Upload the Silver Parquet files back to S3.

The previous direct s3a:// approach is intentionally avoided because the
PySpark/Hadoop AWS JAR combination is fragile on this local Windows setup.
"""

from pathlib import Path
import os
import re
import shutil
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from loguru import logger


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / "config" / ".env")

BRONZE_BUCKET = "cloud-native-healthcare-bronze"
SILVER_BUCKET = "cloud-native-healthcare-silver"

BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver/pyspark"

LOCAL_STAGE_PATH = BASE_DIR / "data" / "tmp" / "dlt_pipeline"
LOCAL_BRONZE_STAGE = LOCAL_STAGE_PATH / "bronze"
LOCAL_SILVER_STAGE = LOCAL_STAGE_PATH / "silver"
LOCAL_HADOOP_HOME = BASE_DIR / "data" / "tmp" / "hadoop"

LOG_PATH = BASE_DIR / "logs"
LOG_PATH.mkdir(exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add(LOG_PATH / "dlt_bronze_to_silver_pyspark.log", rotation="1 MB")


TABLES = [
    "hospital_general_information",
    "complications_and_deaths_hospital",
    "hcahps_hospital",
    "healthcare_associated_infections_hospital",
    "timely_and_effective_care_hospital",
    "unplanned_hospital_visits_hospital",
    "medicare_hospital_spending_per_patient_hospital",
]


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def create_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=require_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=require_env("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def create_spark_session() -> SparkSession:
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 8g pyspark-shell")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("healthcare-bronze-to-silver-local")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def configure_windows_hadoop() -> None:
    if os.name != "nt":
        return

    existing_home = os.getenv("HADOOP_HOME") or os.getenv("hadoop.home.dir")
    if existing_home and (Path(existing_home) / "bin" / "winutils.exe").exists():
        return

    candidates = [
        Path(r"C:\Program Files\RStudio\resources\app\bin\winutils\x64\winutils.exe"),
        Path(r"C:\Program Files\RStudio\resources\app\bin\winutils\winutils.exe"),
    ]
    source = next((candidate for candidate in candidates if candidate.exists()), None)
    if source is None:
        logger.warning("winutils.exe not found; Spark may fail on Windows local file operations")
        return

    target = LOCAL_HADOOP_HOME / "bin" / "winutils.exe"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        shutil.copy2(source, target)

    os.environ["HADOOP_HOME"] = str(LOCAL_HADOOP_HOME)
    os.environ["hadoop.home.dir"] = str(LOCAL_HADOOP_HOME)
    os.environ["PATH"] = f"{target.parent};{os.environ.get('PATH', '')}"
    logger.info(f"Using local Hadoop winutils from {target}")


def s3_list_parquet_keys(s3, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)

    return sorted(keys)


def download_bronze_table(s3, table_name: str) -> Path:
    prefix = f"{BRONZE_PREFIX}/{table_name}/"
    keys = s3_list_parquet_keys(s3, BRONZE_BUCKET, prefix)
    if not keys:
        raise FileNotFoundError(f"No Bronze Parquet files found at s3://{BRONZE_BUCKET}/{prefix}")

    local_table_path = LOCAL_BRONZE_STAGE / table_name
    reset_dir(local_table_path)

    for key in keys:
        destination = local_table_path / Path(key).name
        s3.download_file(BRONZE_BUCKET, key, str(destination))

    logger.info(f"  Downloaded {len(keys)} files from s3://{BRONZE_BUCKET}/{prefix}")
    return local_table_path


def delete_s3_prefix(s3, bucket: str, prefix: str) -> int:
    keys = s3_list_parquet_keys(s3, bucket, prefix)
    deleted = 0

    for start in range(0, len(keys), 1000):
        chunk = keys[start : start + 1000]
        if not chunk:
            continue
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk]},
        )
        deleted += len(chunk)

    return deleted


def upload_silver_table(s3, table_name: str) -> int:
    local_table_path = LOCAL_SILVER_STAGE / table_name
    files = sorted(local_table_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Silver Parquet files found in {local_table_path}")

    prefix = f"{SILVER_PREFIX}/{table_name}/"
    deleted = delete_s3_prefix(s3, SILVER_BUCKET, prefix)
    if deleted:
        logger.info(f"  Removed {deleted} old files from s3://{SILVER_BUCKET}/{prefix}")

    for file_path in files:
        s3_key = f"{prefix}{file_path.name}"
        s3.upload_file(str(file_path), SILVER_BUCKET, s3_key)

    logger.success(f"  Uploaded {len(files)} files to s3://{SILVER_BUCKET}/{prefix}")
    return len(files)


def normalize_name(column_name: str) -> str:
    normalized = column_name.strip().lower()
    normalized = re.sub(r"[ /]+", "_", normalized)
    normalized = re.sub(r"[^0-9a-zA-Z_]+", "", normalized)
    return normalized


def normalize_columns(df):
    for old_name in df.columns:
        new_name = normalize_name(old_name)
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def read_bronze(spark: SparkSession, table_name: str):
    table_path = LOCAL_BRONZE_STAGE / table_name
    if not table_path.exists():
        raise FileNotFoundError(f"Bronze table has not been downloaded: {table_path}")

    files = sorted(table_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No local Bronze Parquet files found in {table_path}")

    paths = [spark_file_uri(file_path) for file_path in files]
    return normalize_columns(spark.read.parquet(*paths))


def spark_file_uri(path: Path) -> str:
    resolved = str(path.resolve()).replace("\\", "/")
    if os.name == "nt":
        return f"file:///{resolved}"
    return f"file://{resolved}"


def write_silver(df, table_name: str) -> None:
    output_path = LOCAL_SILVER_STAGE / table_name
    reset_dir(output_path)

    # Keep transformations in Spark, but write through pandas/pyarrow to avoid
    # Hadoop's Windows local output committer NativeIO path.
    output_file = output_path / "data.parquet"
    df.toPandas().to_parquet(output_file, index=False)
    logger.success(f"  Written local Silver table: {output_file}")


def clean_hospital_id(df):
    from pyspark.sql import functions as F

    return df.withColumn("hospital_id", F.lpad(F.trim(F.col("hospital_id").cast("string")), 6, "0"))


def with_numeric_columns(df, columns: list[str]):
    from pyspark.sql import functions as F

    for column in columns:
        if column in df.columns:
            df = df.withColumn(
                column,
                F.expr(f"try_cast(regexp_replace(`{column}`, ',', '') as double)"),
            )
    return df


def add_transformed_at(df):
    from pyspark.sql import functions as F

    return df.withColumn("transformed_at", F.current_timestamp())


def transform_hospital_general_info(spark: SparkSession):
    from pyspark.sql import functions as F

    logger.info("Processing: hospital_general_information")
    df = read_bronze(spark, "hospital_general_information")

    df = (
        df.withColumnRenamed("facility_id", "hospital_id")
        .withColumnRenamed("facility_name", "hospital_name")
        .withColumnRenamed("city_town", "city")
        .withColumnRenamed("zip_code", "zip")
        .withColumnRenamed("county_parish", "county")
        .withColumnRenamed("telephone_number", "phone")
    )

    drop_columns = ["meets_criteria_for_promoting_interoperability_of_ehrs"]
    df = df.drop(*[column for column in drop_columns if column in df.columns])

    df = clean_hospital_id(df)
    df = with_numeric_columns(
        df,
        [
            "hospital_overall_rating",
            "count_of_facility_mort_measures",
            "count_of_mort_measures_better",
            "count_of_mort_measures_no_different",
            "count_of_mort_measures_worse",
            "count_of_facility_safety_measures",
            "count_of_safety_measures_better",
            "count_of_safety_measures_no_different",
            "count_of_safety_measures_worse",
            "count_of_facility_readm_measures",
            "count_of_readm_measures_better",
            "count_of_readm_measures_no_different",
            "count_of_readm_measures_worse",
            "count_of_facility_pt_exp_measures",
            "count_of_facility_te_measures",
        ],
    )

    for column in ["hospital_name", "address", "city", "state", "county", "emergency_services"]:
        if column in df.columns:
            df = df.withColumn(column, F.initcap(F.trim(F.col(column))))

    rated_types = [
        "Acute Care Hospitals",
        "Acute Care - Veterans Administration",
        "Acute Care - Department of Defense",
        "Childrens",
    ]

    df = (
        df.filter(F.col("hospital_type") != "Not Available")
        .withColumn("rating_eligible", F.col("hospital_type").isin(rated_types))
        .withColumn("has_overall_rating", F.col("hospital_overall_rating").isNotNull())
    )

    df_history = add_transformed_at(df)
    df_latest = (
        df_history.orderBy(F.col("snapshot_date").desc())
        .dropDuplicates(["hospital_id"])
    )

    logger.info(f"  Unique hospitals: {df_latest.count():,}")
    logger.info(f"  Historical rows: {df_history.count():,}")

    write_silver(df_latest, "hospital_general_information_latest")
    write_silver(df_history, "hospital_general_information_history")

    return ["hospital_general_information_latest", "hospital_general_information_history"]


def transform_measure_table(
    spark: SparkSession,
    source_table: str,
    output_table: str,
    numeric_columns: list[str],
    measure_column: str = "measure_id",
    duplicate_keys: list[str] | None = None,
):
    logger.info(f"Processing: {output_table}")
    df = read_bronze(spark, source_table)

    df = (
        df.withColumnRenamed("facility_id", "hospital_id")
        .withColumnRenamed("facility_name", "hospital_name")
        .withColumnRenamed("city_town", "city")
    )

    df = clean_hospital_id(df)
    df = with_numeric_columns(df, numeric_columns)

    if measure_column in df.columns:
        df = df.dropna(subset=[measure_column])

    keys = duplicate_keys or ["hospital_id", measure_column, "snapshot_date"]
    df = df.dropDuplicates([key for key in keys if key in df.columns])
    df = add_transformed_at(df)

    logger.info(f"  Rows: {df.count():,}")
    write_silver(df, output_table)
    return [output_table]


def run_transforms(spark: SparkSession) -> list[str]:
    uploaded_tables: list[str] = []

    uploaded_tables.extend(transform_hospital_general_info(spark))
    uploaded_tables.extend(
        transform_measure_table(
            spark,
            source_table="complications_and_deaths_hospital",
            output_table="complications_and_deaths",
            numeric_columns=["score", "lower_estimate", "higher_estimate"],
        )
    )
    uploaded_tables.extend(
        transform_measure_table(
            spark,
            source_table="hcahps_hospital",
            output_table="hcahps",
            numeric_columns=[
                "hcahps_answer_percent",
                "patient_survey_star_rating",
                "number_of_completed_surveys",
            ],
            measure_column="hcahps_measure_id",
            duplicate_keys=["hospital_id", "hcahps_measure_id", "snapshot_date"],
        )
    )
    uploaded_tables.extend(
        transform_measure_table(
            spark,
            source_table="healthcare_associated_infections_hospital",
            output_table="healthcare_associated_infections",
            numeric_columns=["score"],
        )
    )
    uploaded_tables.extend(
        transform_measure_table(
            spark,
            source_table="timely_and_effective_care_hospital",
            output_table="timely_and_effective_care",
            numeric_columns=["score", "sample"],
        )
    )
    uploaded_tables.extend(
        transform_measure_table(
            spark,
            source_table="unplanned_hospital_visits_hospital",
            output_table="unplanned_hospital_visits",
            numeric_columns=["score", "denominator"],
        )
    )
    uploaded_tables.extend(
        transform_measure_table(
            spark,
            source_table="medicare_hospital_spending_per_patient_hospital",
            output_table="medicare_spending",
            numeric_columns=["score"],
        )
    )

    return uploaded_tables


def main():
    logger.info("=" * 60)
    logger.info("PYSPARK BRONZE TO SILVER STARTED")
    logger.info("Input:  s3://cloud-native-healthcare-bronze/bronze/")
    logger.info("Output: s3://cloud-native-healthcare-silver/silver/pyspark/")
    logger.info("=" * 60)

    s3 = create_s3_client()
    configure_windows_hadoop()
    reset_dir(LOCAL_BRONZE_STAGE)
    reset_dir(LOCAL_SILVER_STAGE)

    try:
        for table_name in TABLES:
            download_bronze_table(s3, table_name)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to download Bronze data from S3: {exc}") from exc

    spark = create_spark_session()
    logger.info(f"Spark version: {spark.version}")

    try:
        silver_tables = run_transforms(spark)
    finally:
        spark.stop()

    uploaded_files = 0
    try:
        for table_name in silver_tables:
            uploaded_files += upload_silver_table(s3, table_name)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to upload Silver data to S3: {exc}") from exc

    logger.info("=" * 60)
    logger.info(f"PYSPARK BRONZE TO SILVER COMPLETE - {len(silver_tables)} tables, {uploaded_files} files uploaded")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
