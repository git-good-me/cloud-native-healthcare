import pandas as pd
from pathlib import Path
from datetime import datetime, UTC
from loguru import logger
import sys
import os
from dotenv import load_dotenv

# ── Load config ────────────────────────────────────────────
load_dotenv("config/.env")

BASE_DIR     = Path(__file__).parent.parent
SILVER_PATH  = BASE_DIR / os.getenv("LOCAL_SILVER_PATH", "data/processed/silver")
GOLD_PATH    = BASE_DIR / os.getenv("LOCAL_GOLD_PATH",   "data/processed/gold")

# ── Logging ────────────────────────────────────────────────
LOG_PATH = BASE_DIR / "logs"
LOG_PATH.mkdir(exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add(LOG_PATH / "transform_gold.log", rotation="1 MB")

# ── Helper: read silver table ──────────────────────────────
def read_silver(table_name: str) -> pd.DataFrame:
    path = SILVER_PATH / table_name / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Silver table not found: {path}")
    df = pd.read_parquet(path)
    logger.info(f"  Loaded {table_name}: {len(df):,} rows")
    return df

# ── Helper: write gold table ───────────────────────────────
def write_gold(df: pd.DataFrame, table_name: str):
    out_dir = GOLD_PATH / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.parquet"
    df.to_parquet(out_file, index=False)
    logger.success(f"  Written → {out_file} ({len(df):,} rows)")

# ── Gold 1: dim_hospital ───────────────────────────────────
def build_dim_hospital():
    logger.info("Building: dim_hospital")
    df = read_silver("hospital_general_information_latest")

    dim = df[[
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
    ]].copy()

    dim = dim.drop_duplicates(subset=["hospital_id"])
    dim["created_at"] = datetime.now(UTC).isoformat()

    logger.info(f"  Hospitals: {len(dim):,}")
    logger.info(f"  States covered: {dim['state'].nunique()}")
    logger.info(f"  Hospital types: {dim['hospital_type'].nunique()}")

    write_gold(dim, "dim_hospital")
    return dim

# ── Gold 2: fact_hospital_ratings ─────────────────────────
def build_fact_hospital_ratings():
    logger.info("Building: fact_hospital_ratings")
    df = read_silver("hospital_general_information_history")

    fact = df[[
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
    ]].copy()

    # Convert measure counts to numeric
    measure_cols = [c for c in fact.columns if c.startswith("count_")]
    for col in measure_cols:
        fact[col] = pd.to_numeric(fact[col], errors="coerce")

    # Calculate performance scores
    fact["mort_better_pct"] = (
        fact["count_of_mort_measures_better"] /
        fact["count_of_facility_mort_measures"].replace(0, pd.NA)
    ).round(3)

    fact["safety_better_pct"] = (
        fact["count_of_safety_measures_better"] /
        fact["count_of_facility_safety_measures"].replace(0, pd.NA)
    ).round(3)

    fact["readm_better_pct"] = (
        fact["count_of_readm_measures_better"] /
        fact["count_of_facility_readm_measures"].replace(0, pd.NA)
    ).round(3)

    fact = fact.sort_values(["hospital_id", "snapshot_date"])
    fact["created_at"] = datetime.now(UTC).isoformat()

    logger.info(f"  Rating records: {len(fact):,}")
    logger.info(f"  Snapshots: {fact['snapshot_date'].nunique()}")
    logger.info(f"  Avg rating (rated hospitals): {fact['hospital_overall_rating'].mean():.2f}")

    write_gold(fact, "fact_hospital_ratings")
    return fact

# ── Gold 3: fact_quality_metrics ──────────────────────────
def build_fact_quality_metrics():
    logger.info("Building: fact_quality_metrics")

    complications = read_silver("complications_and_deaths")
    infections    = read_silver("healthcare_associated_infections")
    readmissions  = read_silver("unplanned_hospital_visits")
    spending      = read_silver("medicare_spending")

    # ── Complications summary per hospital per snapshot ──
    comp_summary = complications.groupby(
        ["hospital_id", "snapshot_date"]
    ).agg(
        complications_avg_score        = ("score", "mean"),
        complications_measures_count   = ("measure_id", "count"),
        complications_worse_count      = ("compared_to_national",
                                          lambda x: (x == "Worse than the national rate").sum()),
        complications_better_count     = ("compared_to_national",
                                          lambda x: (x == "Better than the national rate").sum()),
    ).round(3).reset_index()

    # ── Infections summary per hospital per snapshot ──
    inf_summary = infections.groupby(
        ["hospital_id", "snapshot_date"]
    ).agg(
        infections_avg_score     = ("score", "mean"),
        infections_measure_count = ("measure_id", "count"),
    ).round(3).reset_index()

    # ── Readmissions summary per hospital per snapshot ──
    readm_summary = readmissions.groupby(
        ["hospital_id", "snapshot_date"]
    ).agg(
        readmission_avg_score  = ("score", "mean"),
        readmission_worse_count = ("compared_to_national",
                                   lambda x: (x == "Worse than the national rate").sum()),
        readmission_better_count = ("compared_to_national",
                                    lambda x: (x == "Better than the national rate").sum()),
    ).round(3).reset_index()

    # ── Spending per hospital per snapshot ──
    spend_summary = spending.groupby(
        ["hospital_id", "snapshot_date"]
    ).agg(
        medicare_spending_score = ("score", "mean"),
    ).round(3).reset_index()

    # ── Join all together ──
    fact = comp_summary.merge(inf_summary,   on=["hospital_id", "snapshot_date"], how="outer")
    fact = fact.merge(readm_summary,         on=["hospital_id", "snapshot_date"], how="outer")
    fact = fact.merge(spend_summary,         on=["hospital_id", "snapshot_date"], how="outer")

    fact = fact.sort_values(["hospital_id", "snapshot_date"])
    fact["created_at"] = datetime.now(UTC).isoformat()

    logger.info(f"  Quality metric records: {len(fact):,}")
    logger.info(f"  Hospitals covered: {fact['hospital_id'].nunique():,}")

    write_gold(fact, "fact_quality_metrics")
    return fact

# ── Gold 4: fact_patient_satisfaction ─────────────────────
def build_fact_patient_satisfaction():
    logger.info("Building: fact_patient_satisfaction")
    df = read_silver("hcahps")

    # Focus on the summary star rating measures only
    star_measures = df[df["hcahps_measure_id"].str.contains(
        "STAR", case=False, na=False
    )].copy()

    summary = star_measures.groupby(
        ["hospital_id", "snapshot_date"]
    ).agg(
        avg_star_rating          = ("patient_survey_star_rating", "mean"),
        total_surveys_completed  = ("number_of_completed_surveys", "max"),
        measures_with_rating     = ("patient_survey_star_rating", "count"),
    ).round(3).reset_index()

    # Overall HCAHPS summary rating
    overall = df[df["hcahps_measure_id"] == "H_STAR_RATING"][[
        "hospital_id", "snapshot_date", "patient_survey_star_rating"
    ]].rename(columns={"patient_survey_star_rating": "overall_hcahps_star_rating"})

    summary = summary.merge(overall, on=["hospital_id", "snapshot_date"], how="left")
    summary = summary.sort_values(["hospital_id", "snapshot_date"])
    summary["created_at"] = datetime.now(UTC).isoformat()

    logger.info(f"  Satisfaction records: {len(summary):,}")
    logger.info(f"  Hospitals with satisfaction data: {summary['hospital_id'].nunique():,}")

    write_gold(summary, "fact_patient_satisfaction")
    return summary

# ── Gold 5: gold_hospital_scorecard ───────────────────────
def build_hospital_scorecard():
    logger.info("Building: gold_hospital_scorecard")

    # Read all gold tables
    dim       = pd.read_parquet(GOLD_PATH / "dim_hospital"              / "data.parquet")
    ratings   = pd.read_parquet(GOLD_PATH / "fact_hospital_ratings"     / "data.parquet")
    quality   = pd.read_parquet(GOLD_PATH / "fact_quality_metrics"      / "data.parquet")
    satisf    = pd.read_parquet(GOLD_PATH / "fact_patient_satisfaction"  / "data.parquet")

    # Get latest snapshot only for scorecard
    latest_snapshot = ratings["snapshot_date"].max()
    logger.info(f"  Using latest snapshot: {latest_snapshot}")

    ratings_latest = ratings[ratings["snapshot_date"] == latest_snapshot]
    quality_latest = quality[quality["snapshot_date"] == latest_snapshot]
    satisf_latest  = satisf[satisf["snapshot_date"]  == latest_snapshot]

    # Build scorecard
    scorecard = dim.merge(
        ratings_latest[["hospital_id", "hospital_overall_rating",
                         "mort_better_pct", "safety_better_pct", "readm_better_pct"]],
        on="hospital_id", how="left"
    )
    scorecard = scorecard.merge(
        quality_latest[["hospital_id", "complications_avg_score",
                         "infections_avg_score", "readmission_avg_score",
                         "medicare_spending_score", "complications_worse_count",
                         "readmission_worse_count"]],
        on="hospital_id", how="left"
    )
    scorecard = scorecard.merge(
        satisf_latest[["hospital_id", "overall_hcahps_star_rating",
                        "avg_star_rating", "total_surveys_completed"]],
        on="hospital_id", how="left"
    )

    # Overall performance tier
    def assign_tier(row):
        rating = row.get("hospital_overall_rating")
        if pd.isna(rating):
            return "Unrated"
        elif rating >= 4:
            return "High Performing"
        elif rating == 3:
            return "Average"
        else:
            return "Below Average"

    scorecard["performance_tier"] = scorecard.apply(assign_tier, axis=1)
    scorecard["scorecard_date"]   = latest_snapshot
    scorecard["created_at"]       = datetime.now(UTC).isoformat()

    logger.info(f"  Scorecard hospitals: {len(scorecard):,}")
    logger.info(f"  Performance tiers:")
    tier_counts = scorecard["performance_tier"].value_counts()
    for tier, count in tier_counts.items():
        logger.info(f"    {tier}: {count:,}")

    write_gold(scorecard, "gold_hospital_scorecard")
    return scorecard

# ── Main ───────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("GOLD TRANSFORMATION STARTED")
    logger.info("=" * 60)

    steps = [
        ("dim_hospital",              build_dim_hospital),
        ("fact_hospital_ratings",     build_fact_hospital_ratings),
        ("fact_quality_metrics",      build_fact_quality_metrics),
        ("fact_patient_satisfaction", build_fact_patient_satisfaction),
        ("gold_hospital_scorecard",   build_hospital_scorecard),
    ]

    for name, fn in steps:
        logger.info(f"\n── {name} ──")
        try:
            fn()
        except Exception as e:
            logger.error(f"FAILED: {name} — {e}")
            import traceback
            traceback.print_exc()

    logger.info("\n" + "=" * 60)
    logger.info("GOLD TRANSFORMATION COMPLETE")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()