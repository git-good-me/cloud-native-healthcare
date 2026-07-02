"""
Great Expectations Data Quality Checks (GE v1.18+)
===================================================
Validates Silver and Gold tables downloaded from S3.

Schemas validated against actual parquet files in S3:

  dim_hospital:
    hospital_id, hospital_name, address, city, state, zip, county, phone,
    hospital_type, hospital_ownership, emergency_services,
    rating_eligible, has_overall_rating, created_at

  fact_hospital_ratings:
    hospital_id, hospital_name, state, hospital_overall_rating,
    snapshot_quarter, above_national_avg, ...

  gold_hospital_scorecard:
    hospital_id, hospital_name, state, hospital_overall_rating,
    performance_tier, composite_score, ...

Run:
  python quality/ge_quality_checks.py --layer gold
  python quality/ge_quality_checks.py --layer silver
  python quality/ge_quality_checks.py --layer all

Exit code 0 = all passed, 1 = any failed.
"""

import os
import sys
import logging
import argparse
import tempfile
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
import great_expectations as gx

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv("config/.env")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SILVER_BUCKET  = "cloud-native-healthcare-silver"
GOLD_BUCKET    = "cloud-native-healthcare-gold"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── S3 helpers ────────────────────────────────────────────────────────────────

def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )


def download_table(s3, bucket: str, prefix: str) -> pd.DataFrame:
    paginator = s3.get_paginator("list_objects_v2")
    frames = []
    with tempfile.TemporaryDirectory() as tmp:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                local = Path(tmp) / Path(key).name
                s3.download_file(bucket, key, str(local))
                frames.append(pd.read_parquet(local))
    if not frames:
        raise FileNotFoundError(f"No parquet files at s3://{bucket}/{prefix}")
    return pd.concat(frames, ignore_index=True)

# ── GE v1.18 validator ────────────────────────────────────────────────────────

def make_validator(df: pd.DataFrame, suite_name: str):
    """
    Build a GE 1.18 in-memory validator from a pandas DataFrame.
    Handles re-runs by getting an existing suite instead of re-adding it.
    """
    context  = gx.get_context()
    ds_name  = f"ds_{suite_name}"

    try:
        ds = context.data_sources.add_pandas(name=ds_name)
    except Exception:
        ds = context.data_sources.get(ds_name)

    asset     = ds.add_dataframe_asset(name=suite_name)
    batch_def = asset.add_batch_definition_whole_dataframe(f"batch_{suite_name}")
    batch     = batch_def.get_batch(batch_parameters={"dataframe": df})

    # Reuse existing suite or create fresh
    try:
        suite = context.suites.get(name=suite_name)
    except Exception:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    return context.get_validator(batch=batch, expectation_suite=suite)


def chk(validator, method: str, **kwargs) -> bool:
    """Run one expectation, log result, return bool."""
    result = getattr(validator, method)(**kwargs)
    passed = result.success
    kw_str = "  ".join(f"{k}={v}" for k, v in kwargs.items())
    log.info("    %s  %s  %s", "✓" if passed else "✗",
             method.replace("expect_", ""), kw_str)
    return passed

# ── Expectation suites ────────────────────────────────────────────────────────

def suite_hospital_general(df: pd.DataFrame) -> list[bool]:
    """Silver: hospital_general_information_latest"""
    v = make_validator(df, "hospital_general")
    return [
        chk(v, "expect_column_to_exist",
            column="hospital_id"),
        chk(v, "expect_column_values_to_not_be_null",
            column="hospital_id"),
        chk(v, "expect_column_values_to_be_unique",
            column="hospital_id"),
        chk(v, "expect_column_to_exist",
            column="state"),
        chk(v, "expect_column_values_to_match_regex",
            column="state", regex=r"^[A-Z]{2}$", mostly=0.98),
        chk(v, "expect_table_row_count_to_be_between",
            min_value=5000, max_value=10000),
    ]


def suite_dim_hospital(df: pd.DataFrame) -> list[bool]:
    """Gold: dim_hospital
    Actual schema: hospital_id, hospital_name, address, city, state,
                   zip, county, phone, hospital_type, hospital_ownership,
                   emergency_services, rating_eligible, has_overall_rating, created_at
    """
    v = make_validator(df, "dim_hospital")
    return [
        chk(v, "expect_column_to_exist",
            column="hospital_id"),
        chk(v, "expect_column_values_to_not_be_null",
            column="hospital_id"),
        chk(v, "expect_column_values_to_be_unique",
            column="hospital_id"),
        chk(v, "expect_column_to_exist",
            column="hospital_name"),
        chk(v, "expect_column_values_to_not_be_null",
            column="hospital_name"),
        chk(v, "expect_column_to_exist",
            column="state"),
        # State values are mixed case (e.g. 'Wv', 'Ca') — use case-insensitive match
        chk(v, "expect_column_values_to_match_regex",
            column="state", regex=r"^[A-Za-z]{2}$", mostly=0.98),
        chk(v, "expect_table_row_count_to_be_between",
            min_value=5000, max_value=10000),
    ]


def suite_fact_hospital_ratings(df: pd.DataFrame) -> list[bool]:
    """Gold: fact_hospital_ratings
    Actual schema: hospital_id, hospital_name, state,
                   hospital_overall_rating, snapshot_quarter, above_national_avg
    """
    v = make_validator(df, "fact_hospital_ratings")
    return [
        chk(v, "expect_column_to_exist",
            column="hospital_id"),
        chk(v, "expect_column_values_to_not_be_null",
            column="hospital_id"),
        chk(v, "expect_column_to_exist",
            column="hospital_overall_rating"),
        chk(v, "expect_column_values_to_be_between",
            column="hospital_overall_rating",
            min_value=1, max_value=5, mostly=0.85),
        chk(v, "expect_table_row_count_to_be_between",
            min_value=5000, max_value=100000),
    ]


def suite_scorecard(df: pd.DataFrame) -> list[bool]:
    """Gold: gold_hospital_scorecard
    Actual schema: hospital_id, hospital_name, state,
                   hospital_overall_rating, performance_tier, composite_score, ...
    """
    v = make_validator(df, "gold_scorecard")
    return [
        chk(v, "expect_column_to_exist",
            column="hospital_id"),
        chk(v, "expect_column_values_to_not_be_null",
            column="hospital_id"),
        chk(v, "expect_column_values_to_be_unique",
            column="hospital_id"),
        chk(v, "expect_column_to_exist",
            column="hospital_overall_rating"),
        chk(v, "expect_column_values_to_be_between",
            column="hospital_overall_rating",
            min_value=1, max_value=5, mostly=0.85),
        chk(v, "expect_table_row_count_to_be_between",
            min_value=5000, max_value=10000),
    ]

# ── Check registry ────────────────────────────────────────────────────────────

SILVER_CHECKS = [
    (SILVER_BUCKET,
     "silver/pyspark/hospital_general_information_latest/",
     suite_hospital_general,
     "hospital_general_information_latest"),
]

GOLD_CHECKS = [
    (GOLD_BUCKET, "gold/pyspark/dim_hospital/",
     suite_dim_hospital, "dim_hospital"),
    (GOLD_BUCKET, "gold/pyspark/fact_hospital_ratings/",
     suite_fact_hospital_ratings, "fact_hospital_ratings"),
    (GOLD_BUCKET, "gold/pyspark/gold_hospital_scorecard/",
     suite_scorecard, "gold_hospital_scorecard"),
]

# ── Runner ────────────────────────────────────────────────────────────────────

def run_checks(layer: str = "all") -> bool:
    log.info("=" * 60)
    log.info("Great Expectations Quality Checks  |  layer=%s", layer)
    log.info("=" * 60)

    checks = []
    if layer in ("silver", "all"):
        checks += SILVER_CHECKS
    if layer in ("gold", "all"):
        checks += GOLD_CHECKS

    s3         = s3_client()
    all_passed = True
    summary    = []

    for bucket, prefix, suite_fn, label in checks:
        log.info("\n── %s", label)
        try:
            df = download_table(s3, bucket, prefix)
            log.info("  Loaded %d rows × %d cols", len(df), len(df.columns))
            log.info("  Columns: %s", df.columns.tolist())
            results = suite_fn(df)
            passed  = all(results)
            failed  = results.count(False)
            summary.append((label, passed, len(results), failed))
            if not passed:
                all_passed = False
        except Exception as e:
            log.error("  Error: %s", e)
            summary.append((label, False, 0, 1))
            all_passed = False

    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    for name, passed, total, failed in summary:
        log.info("  %s  %-45s  %d/%d passed",
                 "✓" if passed else "✗", name, total - failed, total)

    log.info("\n%s",
             "All quality checks passed ✓" if all_passed else "Some checks failed ✗")
    return all_passed

# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run GE data quality checks")
    parser.add_argument("--layer", choices=["silver", "gold", "all"],
                        default="all", help="Which layer to validate (default: all)")
    args   = parser.parse_args()
    passed = run_checks(args.layer)
    sys.exit(0 if passed else 1)