import pandas as pd
from pathlib import Path
from datetime import datetime, UTC
from loguru import logger
import sys
import os
from dotenv import load_dotenv

# ── Load config ────────────────────────────────────────────
load_dotenv("config/.env")

BASE_DIR      = Path(__file__).parent.parent
BRONZE_PATH   = BASE_DIR / os.getenv("LOCAL_BRONZE_PATH", "data/processed/bronze")
SILVER_PATH   = BASE_DIR / os.getenv("LOCAL_SILVER_PATH", "data/processed/silver")

# ── Logging ────────────────────────────────────────────────
LOG_PATH = BASE_DIR / "logs"
LOG_PATH.mkdir(exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add(LOG_PATH / "transform_silver.log", rotation="1 MB")

# ── Helper: read all snapshots for a table ─────────────────
def read_bronze_table(table_name: str) -> pd.DataFrame:
    table_path = BRONZE_PATH / table_name
    files = sorted(table_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {table_path}")
    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        dfs.append(df)
    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"  Read {len(files)} snapshots → {len(combined):,} total rows")
    return combined

# ── Helper: write silver table ─────────────────────────────
def write_silver(df: pd.DataFrame, table_name: str):
    out_dir = SILVER_PATH / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.parquet"
    df.to_parquet(out_file, index=False)
    logger.success(f"  Written → {out_file} ({len(df):,} rows)")

# ── Transform: hospital_general_information ────────────────
def transform_hospital_general_info():
    logger.info("Processing: hospital_general_information")
    df = read_bronze_table("hospital_general_information")

    # Standardize column names
    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    # Rename key columns
    df = df.rename(columns={
        "facility_id":      "hospital_id",
        "facility_name":    "hospital_name",
        "city_town":        "city",
        "zip_code":         "zip",
        "county_parish":    "county",
        "telephone_number": "phone",
    })

    # Drop near-empty columns
    df = df.drop(columns=[
        "meets_criteria_for_promoting_interoperability_of_ehrs",
    ], errors="ignore")

    # Clean strings
    for col in ["hospital_name", "address", "city", "state", "county"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    # Standardize hospital_id
    df["hospital_id"] = df["hospital_id"].astype(str).str.strip().str.zfill(6)

    # Clean rating
    df["hospital_overall_rating"] = pd.to_numeric(
        df["hospital_overall_rating"], errors="coerce"
    )

    # Remove "Not Available" hospital types
    df = df[df["hospital_type"] != "Not Available"]

    # Flag whether hospital is eligible for star rating
    RATED_TYPES = [
        "Acute Care Hospitals",
        "Acute Care - Veterans Administration",
        "Acute Care - Department of Defense",
        "Childrens",
    ]
    df["rating_eligible"] = df["hospital_type"].isin(RATED_TYPES)

    # Flag data quality
    df["has_overall_rating"] = df["hospital_overall_rating"].notna()

    # Emergency services clean
    if "emergency_services" in df.columns:
        df["emergency_services"] = df["emergency_services"].str.strip().str.title()

    # Sort and deduplicate
    df = df.sort_values("snapshot_date", ascending=False)
    df_latest = df.drop_duplicates(subset=["hospital_id"], keep="first")
    df_historical = df.copy()

    df_latest["transformed_at"] = datetime.now(UTC).isoformat()
    df_historical["transformed_at"] = datetime.now(UTC).isoformat()

    logger.info(f"  Unique hospitals: {len(df_latest):,}")
    logger.info(f"  Rating eligible: {df_latest['rating_eligible'].sum():,}")
    logger.info(f"  Has rating: {df_latest['has_overall_rating'].sum():,}")
    logger.info(f"  Historical rows: {len(df_historical):,}")

    write_silver(df_latest, "hospital_general_information_latest")
    write_silver(df_historical, "hospital_general_information_history")

    return df_latest

# ── Transform: complications_and_deaths ───────────────────
def transform_complications():
    logger.info("Processing: complications_and_deaths")
    df = read_bronze_table("complications_and_deaths_hospital")

    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    df = df.rename(columns={
        "facility_id":   "hospital_id",
        "facility_name": "hospital_name",
        "city_town":     "city",
    })

    df["hospital_id"] = df["hospital_id"].astype(str).str.strip().str.zfill(6)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["lower_estimate"] = pd.to_numeric(df["lower_estimate"], errors="coerce")
    df["higher_estimate"] = pd.to_numeric(df["higher_estimate"], errors="coerce")

    if "compared_to_national" in df.columns:
        df["compared_to_national"] = df["compared_to_national"].str.strip()

    df = df.dropna(subset=["measure_id"])
    df = df.drop_duplicates(subset=["hospital_id", "measure_id", "snapshot_date"])

    df["transformed_at"] = datetime.now(UTC).isoformat()

    write_silver(df, "complications_and_deaths")

# ── Transform: hcahps (patient satisfaction) ──────────────
def transform_hcahps():
    logger.info("Processing: hcahps_hospital")
    df = read_bronze_table("hcahps_hospital")

    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    df = df.rename(columns={
        "facility_id":   "hospital_id",
        "facility_name": "hospital_name",
        "city_town":     "city",
    })

    df["hospital_id"] = df["hospital_id"].astype(str).str.strip().str.zfill(6)
    df["hcahps_answer_percent"] = pd.to_numeric(df["hcahps_answer_percent"], errors="coerce")
    df["patient_survey_star_rating"] = pd.to_numeric(df["patient_survey_star_rating"], errors="coerce")
    df["number_of_completed_surveys"] = pd.to_numeric(df["number_of_completed_surveys"], errors="coerce")

    df = df.dropna(subset=["hcahps_measure_id"])
    df = df.drop_duplicates(subset=["hospital_id", "hcahps_measure_id", "snapshot_date"])

    df["transformed_at"] = datetime.now(UTC).isoformat()

    write_silver(df, "hcahps")

# ── Transform: healthcare associated infections ────────────
def transform_infections():
    logger.info("Processing: healthcare_associated_infections")
    df = read_bronze_table("healthcare_associated_infections_hospital")

    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    df = df.rename(columns={
        "facility_id":   "hospital_id",
        "facility_name": "hospital_name",
        "city_town":     "city",
    })

    df["hospital_id"] = df["hospital_id"].astype(str).str.strip().str.zfill(6)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    df = df.dropna(subset=["measure_id"])
    df = df.drop_duplicates(subset=["hospital_id", "measure_id", "snapshot_date"])

    df["transformed_at"] = datetime.now(UTC).isoformat()

    write_silver(df, "healthcare_associated_infections")

# ── Transform: timely and effective care ──────────────────
def transform_timely_care():
    logger.info("Processing: timely_and_effective_care")
    df = read_bronze_table("timely_and_effective_care_hospital")

    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    df = df.rename(columns={
        "facility_id":   "hospital_id",
        "facility_name": "hospital_name",
        "city_town":     "city",
    })

    df["hospital_id"] = df["hospital_id"].astype(str).str.strip().str.zfill(6)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["sample"] = pd.to_numeric(df["sample"], errors="coerce")

    df = df.dropna(subset=["measure_id"])
    df = df.drop_duplicates(subset=["hospital_id", "measure_id", "snapshot_date"])

    df["transformed_at"] = datetime.now(UTC).isoformat()

    write_silver(df, "timely_and_effective_care")

# ── Transform: unplanned hospital visits ──────────────────
def transform_unplanned_visits():
    logger.info("Processing: unplanned_hospital_visits")
    df = read_bronze_table("unplanned_hospital_visits_hospital")

    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    df = df.rename(columns={
        "facility_id":   "hospital_id",
        "facility_name": "hospital_name",
        "city_town":     "city",
    })

    df["hospital_id"] = df["hospital_id"].astype(str).str.strip().str.zfill(6)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["denominator"] = pd.to_numeric(df["denominator"], errors="coerce")

    df = df.dropna(subset=["measure_id"])
    df = df.drop_duplicates(subset=["hospital_id", "measure_id", "snapshot_date"])

    df["transformed_at"] = datetime.now(UTC).isoformat()

    write_silver(df, "unplanned_hospital_visits")

# ── Transform: medicare spending ──────────────────────────
def transform_spending():
    logger.info("Processing: medicare_spending")
    df = read_bronze_table("medicare_hospital_spending_per_patient_hospital")

    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    df = df.rename(columns={
        "facility_id":   "hospital_id",
        "facility_name": "hospital_name",
        "city_town":     "city",
    })

    df["hospital_id"] = df["hospital_id"].astype(str).str.strip().str.zfill(6)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    df = df.dropna(subset=["measure_id"])
    df = df.drop_duplicates(subset=["hospital_id", "measure_id", "snapshot_date"])

    df["transformed_at"] = datetime.now(UTC).isoformat()

    write_silver(df, "medicare_spending")

# ── Main ───────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("SILVER TRANSFORMATION STARTED")
    logger.info("=" * 60)

    transforms = [
        ("Hospital General Info",       transform_hospital_general_info),
        ("Complications & Deaths",      transform_complications),
        ("HCAHPS Patient Satisfaction", transform_hcahps),
        ("Healthcare Infections",       transform_infections),
        ("Timely & Effective Care",     transform_timely_care),
        ("Unplanned Hospital Visits",   transform_unplanned_visits),
        ("Medicare Spending",           transform_spending),
    ]

    for name, fn in transforms:
        logger.info(f"\n── {name} ──")
        try:
            fn()
        except Exception as e:
            logger.error(f"FAILED: {name} — {e}")

    logger.info("\n" + "=" * 60)
    logger.info("SILVER TRANSFORMATION COMPLETE")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()