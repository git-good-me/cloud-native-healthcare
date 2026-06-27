from __future__ import annotations

"""
Pipeline 02: Silver to Gold transformation.

This uses the same S3 boundary pattern that works in 01_bronze_to_silver:
1. Download Silver Parquet files from S3 to a local staging folder.
2. Build Gold star-schema tables locally.
3. Write Gold Parquet files locally.
4. Upload the Gold Parquet files back to S3.

Note on Spark:
The Silver files produced by 01_ contain pandas/pyarrow timestamp metadata that
PySpark 4.1.1 on Windows rejects or handles unreliably during collect/toPandas
workflows. For this Gold layer, the stable local path is pandas-only. The logic
matches transformation/transform_gold.py while reading from silver/pyspark and
writing to gold/pyspark.
"""

from datetime import UTC, datetime
from pathlib import Path
import os
import shutil
import sys

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from loguru import logger


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / "config" / ".env")

SILVER_BUCKET = "cloud-native-healthcare-silver"
GOLD_BUCKET = "cloud-native-healthcare-gold"

SILVER_PREFIX = "silver/pyspark"
GOLD_PREFIX = "gold/pyspark"

LOCAL_STAGE_PATH = BASE_DIR / "data" / "tmp" / "dlt_pipeline"
LOCAL_SILVER_STAGE = LOCAL_STAGE_PATH / "silver_download"
LOCAL_GOLD_STAGE = LOCAL_STAGE_PATH / "gold"

LOG_PATH = BASE_DIR / "logs"
LOG_PATH.mkdir(exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add(LOG_PATH / "dlt_silver_to_gold.log", rotation="1 MB")

SILVER_TABLES = [
    "hospital_general_information_latest",
    "hospital_general_information_history",
    "complications_and_deaths",
    "hcahps",
    "healthcare_associated_infections",
    "unplanned_hospital_visits",
    "medicare_spending",
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


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def s3_list_parquet_keys(s3, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)

    return sorted(keys)


def download_silver_table(s3, table_name: str) -> Path:
    prefix = f"{SILVER_PREFIX}/{table_name}/"
    keys = s3_list_parquet_keys(s3, SILVER_BUCKET, prefix)
    if not keys:
        raise FileNotFoundError(f"No Silver Parquet files found at s3://{SILVER_BUCKET}/{prefix}")

    local_table_path = LOCAL_SILVER_STAGE / table_name
    reset_dir(local_table_path)

    for key in keys:
        destination = local_table_path / Path(key).name
        s3.download_file(SILVER_BUCKET, key, str(destination))

    logger.info(f"  Downloaded {table_name}: {len(keys)} file(s)")
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


def upload_gold_table(s3, table_name: str) -> int:
    local_table_path = LOCAL_GOLD_STAGE / table_name
    files = sorted(local_table_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Gold Parquet files found in {local_table_path}")

    prefix = f"{GOLD_PREFIX}/{table_name}/"
    deleted = delete_s3_prefix(s3, GOLD_BUCKET, prefix)
    if deleted:
        logger.info(f"  Removed {deleted} old files from s3://{GOLD_BUCKET}/{prefix}")

    for file_path in files:
        s3_key = f"{prefix}{file_path.name}"
        s3.upload_file(str(file_path), GOLD_BUCKET, s3_key)

    logger.success(f"  Uploaded {table_name}: {len(files)} file(s)")
    return len(files)


def read_silver(table_name: str) -> pd.DataFrame:
    table_path = LOCAL_SILVER_STAGE / table_name
    files = sorted(table_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No local Silver Parquet files found in {table_path}")

    df = pd.concat((pd.read_parquet(file_path) for file_path in files), ignore_index=True)
    logger.info(f"  Loaded {table_name}: {len(df):,} rows")
    return df


def write_gold(df: pd.DataFrame, table_name: str) -> None:
    out_dir = LOCAL_GOLD_STAGE / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.parquet"
    df.to_parquet(out_file, index=False)
    logger.success(f"  Written {table_name}: {len(df):,} rows")


def now_utc() -> str:
    return datetime.now(UTC).isoformat()


def numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def build_dim_hospital(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger.info("Building: dim_hospital")
    df = tables["hospital_general_information_latest"]

    columns = [
        "hospital_id",
        "hospital_name",
        "address",
        "city",
        "state",
        "zip",
        "county",
        "phone",
        "hospital_type",
        "hospital_ownership",
        "emergency_services",
        "rating_eligible",
        "has_overall_rating",
    ]

    dim = df[[column for column in columns if column in df.columns]].copy()
    dim = dim.drop_duplicates(subset=["hospital_id"])
    dim["created_at"] = now_utc()

    logger.info(f"  Hospitals: {len(dim):,}")
    logger.info(f"  States covered: {dim['state'].nunique()}")
    logger.info(f"  Hospital types: {dim['hospital_type'].nunique()}")

    write_gold(dim, "dim_hospital")
    return dim


def build_fact_hospital_ratings(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger.info("Building: fact_hospital_ratings")
    df = tables["hospital_general_information_history"]

    columns = [
        "hospital_id",
        "hospital_name",
        "state",
        "hospital_type",
        "snapshot_date",
        "hospital_overall_rating",
        "rating_eligible",
        "has_overall_rating",
        "count_of_facility_mort_measures",
        "count_of_mort_measures_better",
        "count_of_mort_measures_worse",
        "count_of_facility_safety_measures",
        "count_of_safety_measures_better",
        "count_of_safety_measures_worse",
        "count_of_facility_readm_measures",
        "count_of_readm_measures_better",
        "count_of_readm_measures_worse",
    ]

    fact = df[[column for column in columns if column in df.columns]].copy()
    fact = numeric(fact, ["hospital_overall_rating"] + [c for c in fact.columns if c.startswith("count_")])

    fact["mort_better_pct"] = (
        fact["count_of_mort_measures_better"]
        / fact["count_of_facility_mort_measures"].replace(0, pd.NA)
    ).round(3)
    fact["safety_better_pct"] = (
        fact["count_of_safety_measures_better"]
        / fact["count_of_facility_safety_measures"].replace(0, pd.NA)
    ).round(3)
    fact["readm_better_pct"] = (
        fact["count_of_readm_measures_better"]
        / fact["count_of_facility_readm_measures"].replace(0, pd.NA)
    ).round(3)

    fact = fact.sort_values(["hospital_id", "snapshot_date"])
    fact["created_at"] = now_utc()

    logger.info(f"  Rating records: {len(fact):,}")
    logger.info(f"  Snapshots: {fact['snapshot_date'].nunique()}")
    logger.info(f"  Avg rating: {fact['hospital_overall_rating'].mean():.2f}")

    write_gold(fact, "fact_hospital_ratings")
    return fact


def build_fact_quality_metrics(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger.info("Building: fact_quality_metrics")

    complications = numeric(tables["complications_and_deaths"].copy(), ["score"])
    infections = numeric(tables["healthcare_associated_infections"].copy(), ["score"])
    readmissions = numeric(tables["unplanned_hospital_visits"].copy(), ["score"])
    spending = numeric(tables["medicare_spending"].copy(), ["score"])

    comp_summary = complications.groupby(["hospital_id", "snapshot_date"]).agg(
        complications_avg_score=("score", "mean"),
        complications_measures_count=("measure_id", "count"),
        complications_worse_count=(
            "compared_to_national",
            lambda x: (x == "Worse than the national rate").sum(),
        ),
        complications_better_count=(
            "compared_to_national",
            lambda x: (x == "Better than the national rate").sum(),
        ),
    ).round(3).reset_index()

    inf_summary = infections.groupby(["hospital_id", "snapshot_date"]).agg(
        infections_avg_score=("score", "mean"),
        infections_measure_count=("measure_id", "count"),
    ).round(3).reset_index()

    readm_summary = readmissions.groupby(["hospital_id", "snapshot_date"]).agg(
        readmission_avg_score=("score", "mean"),
        readmission_worse_count=(
            "compared_to_national",
            lambda x: (x == "Worse than the national rate").sum(),
        ),
        readmission_better_count=(
            "compared_to_national",
            lambda x: (x == "Better than the national rate").sum(),
        ),
    ).round(3).reset_index()

    spend_summary = spending.groupby(["hospital_id", "snapshot_date"]).agg(
        medicare_spending_score=("score", "mean"),
    ).round(3).reset_index()

    fact = comp_summary.merge(inf_summary, on=["hospital_id", "snapshot_date"], how="outer")
    fact = fact.merge(readm_summary, on=["hospital_id", "snapshot_date"], how="outer")
    fact = fact.merge(spend_summary, on=["hospital_id", "snapshot_date"], how="outer")

    fact = fact.sort_values(["hospital_id", "snapshot_date"])
    fact["created_at"] = now_utc()

    logger.info(f"  Quality metric records: {len(fact):,}")
    logger.info(f"  Hospitals covered: {fact['hospital_id'].nunique():,}")

    write_gold(fact, "fact_quality_metrics")
    return fact


def build_fact_patient_satisfaction(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger.info("Building: fact_patient_satisfaction")
    df = numeric(
        tables["hcahps"].copy(),
        [
            "patient_survey_star_rating",
            "number_of_completed_surveys",
            "hcahps_answer_percent",
        ],
    )

    star_measures = df[df["hcahps_measure_id"].str.contains("STAR", case=False, na=False)].copy()
    summary = star_measures.groupby(["hospital_id", "snapshot_date"]).agg(
        avg_star_rating=("patient_survey_star_rating", "mean"),
        total_surveys_completed=("number_of_completed_surveys", "max"),
        measures_with_rating=("patient_survey_star_rating", "count"),
    ).round(3).reset_index()

    overall = df[df["hcahps_measure_id"] == "H_STAR_RATING"][
        ["hospital_id", "snapshot_date", "patient_survey_star_rating"]
    ].rename(columns={"patient_survey_star_rating": "overall_hcahps_star_rating"})

    summary = summary.merge(overall, on=["hospital_id", "snapshot_date"], how="left")
    summary = summary.sort_values(["hospital_id", "snapshot_date"])
    summary["created_at"] = now_utc()

    logger.info(f"  Satisfaction records: {len(summary):,}")
    logger.info(f"  Hospitals with satisfaction data: {summary['hospital_id'].nunique():,}")

    write_gold(summary, "fact_patient_satisfaction")
    return summary


def build_hospital_scorecard(
    dim: pd.DataFrame,
    ratings: pd.DataFrame,
    quality: pd.DataFrame,
    satisfaction: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Building: gold_hospital_scorecard")

    latest_snapshot = ratings["snapshot_date"].max()
    logger.info(f"  Using latest snapshot: {latest_snapshot}")

    ratings_latest = ratings[ratings["snapshot_date"] == latest_snapshot]
    quality_latest = quality[quality["snapshot_date"] == latest_snapshot]
    satisfaction_latest = satisfaction[satisfaction["snapshot_date"] == latest_snapshot]

    scorecard = dim.merge(
        ratings_latest[
            [
                "hospital_id",
                "hospital_overall_rating",
                "mort_better_pct",
                "safety_better_pct",
                "readm_better_pct",
            ]
        ],
        on="hospital_id",
        how="left",
    )
    scorecard = scorecard.merge(
        quality_latest[
            [
                "hospital_id",
                "complications_avg_score",
                "infections_avg_score",
                "readmission_avg_score",
                "medicare_spending_score",
                "complications_worse_count",
                "readmission_worse_count",
            ]
        ],
        on="hospital_id",
        how="left",
    )
    scorecard = scorecard.merge(
        satisfaction_latest[
            [
                "hospital_id",
                "overall_hcahps_star_rating",
                "avg_star_rating",
                "total_surveys_completed",
            ]
        ],
        on="hospital_id",
        how="left",
    )

    def assign_tier(row):
        rating = row.get("hospital_overall_rating")
        if pd.isna(rating):
            return "Unrated"
        if rating >= 4:
            return "High Performing"
        if rating == 3:
            return "Average"
        return "Below Average"

    scorecard["performance_tier"] = scorecard.apply(assign_tier, axis=1)
    scorecard["scorecard_date"] = latest_snapshot
    scorecard["created_at"] = now_utc()

    logger.info(f"  Scorecard hospitals: {len(scorecard):,}")
    for tier, count in scorecard["performance_tier"].value_counts().items():
        logger.info(f"    {tier}: {count:,}")

    write_gold(scorecard, "gold_hospital_scorecard")
    return scorecard


def main():
    logger.info("=" * 60)
    logger.info("PANDAS SILVER TO GOLD STARTED")
    logger.info("Input:  s3://cloud-native-healthcare-silver/silver/pyspark/")
    logger.info("Output: s3://cloud-native-healthcare-gold/gold/pyspark/")
    logger.info("=" * 60)

    s3 = create_s3_client()
    reset_dir(LOCAL_SILVER_STAGE)
    reset_dir(LOCAL_GOLD_STAGE)

    try:
        for table_name in SILVER_TABLES:
            download_silver_table(s3, table_name)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to download Silver data from S3: {exc}") from exc

    tables = {table_name: read_silver(table_name) for table_name in SILVER_TABLES}

    dim = build_dim_hospital(tables)
    ratings = build_fact_hospital_ratings(tables)
    quality = build_fact_quality_metrics(tables)
    satisfaction = build_fact_patient_satisfaction(tables)
    build_hospital_scorecard(dim, ratings, quality, satisfaction)

    gold_tables = [
        "dim_hospital",
        "fact_hospital_ratings",
        "fact_quality_metrics",
        "fact_patient_satisfaction",
        "gold_hospital_scorecard",
    ]

    uploaded_files = 0
    try:
        for table_name in gold_tables:
            uploaded_files += upload_gold_table(s3, table_name)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to upload Gold data to S3: {exc}") from exc

    logger.info("=" * 60)
    logger.info(f"SILVER TO GOLD COMPLETE - {len(gold_tables)} tables, {uploaded_files} files uploaded")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
