import pandas as pd
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys
import os
from dotenv import load_dotenv

# ── Load config ────────────────────────────────────────────
load_dotenv("config/.env")

BASE_DIR    = Path(__file__).parent.parent
RAW_PATH    = BASE_DIR / os.getenv("LOCAL_RAW_PATH", "data/raw")
BRONZE_PATH = BASE_DIR / os.getenv("LOCAL_BRONZE_PATH", "data/processed/bronze")

# ── Logging setup ──────────────────────────────────────────
LOG_PATH = BASE_DIR / "logs"
LOG_PATH.mkdir(exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add(LOG_PATH / "ingest_bronze.log", rotation="1 MB")

# ── Files we want from each snapshot ──────────────────────
TARGET_FILES = [
    "Hospital_General_Information.csv",
    "Complications_and_Deaths-Hospital.csv",
    "HCAHPS-Hospital.csv",
    "Healthcare_Associated_Infections-Hospital.csv",
    "Timely_and_Effective_Care-Hospital.csv",
    "Unplanned_Hospital_Visits-Hospital.csv",
    "Medicare_Hospital_Spending_Per_Patient-Hospital.csv",
]

# ── Helper: clean filename into a table name ───────────────
def to_table_name(filename: str) -> str:
    return filename.replace(".csv", "").replace("-", "_").replace(" ", "_").lower()

# ── Helper: parse snapshot date from folder name ───────────
def parse_snapshot_date(folder_name: str) -> str:
    # folder names like hospitals_01_2024 → 2024-01-01
    parts = folder_name.split("_")
    month = parts[1]
    year  = parts[2]
    return f"{year}-{month}-01"

# ── Core ingestion function ────────────────────────────────
def ingest_snapshot(snapshot_path: Path) -> dict:
    snapshot_name = snapshot_path.name
    snapshot_date = parse_snapshot_date(snapshot_name)
    results = {"snapshot": snapshot_name, "success": [], "skipped": [], "failed": []}

    logger.info(f"Processing snapshot: {snapshot_name} (date: {snapshot_date})")

    for filename in TARGET_FILES:
        filepath = snapshot_path / filename
        table_name = to_table_name(filename)

        if not filepath.exists():
            logger.warning(f"  SKIPPED — not found: {filename}")
            results["skipped"].append(filename)
            continue

        try:
            # Read CSV
            df = pd.read_csv(filepath, encoding="latin-1", low_memory=False, dtype=str)

            # Add metadata columns
            df["snapshot_date"]    = snapshot_date
            df["snapshot_folder"]  = snapshot_name
            df["ingested_at"]      = datetime.utcnow().isoformat()
            df["source_file"]      = filename

            # Write to bronze as parquet
            out_dir = BRONZE_PATH / table_name
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{snapshot_name}.parquet"
            df.to_parquet(out_file, index=False)

            logger.success(f"  OK — {filename} → {df.shape[0]:,} rows → {out_file.name}")
            results["success"].append(filename)

        except Exception as e:
            logger.error(f"  FAILED — {filename}: {e}")
            results["failed"].append(filename)

    return results

# ── Main: loop all snapshots ───────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("BRONZE INGESTION STARTED")
    logger.info(f"Source: {RAW_PATH}")
    logger.info(f"Target: {BRONZE_PATH}")
    logger.info("=" * 60)

    # Find all snapshot folders across all year folders
    snapshot_paths = sorted([
        p for p in RAW_PATH.rglob("hospitals_*_*")
        if p.is_dir() and len(p.name.split("_")) == 3
    ])

    if not snapshot_paths:
        logger.error("No snapshot folders found. Check your data/raw directory.")
        return

    logger.info(f"Found {len(snapshot_paths)} snapshots to process")

    total_success = 0
    total_skipped = 0
    total_failed  = 0

    for snapshot_path in snapshot_paths:
        result = ingest_snapshot(snapshot_path)
        total_success += len(result["success"])
        total_skipped += len(result["skipped"])
        total_failed  += len(result["failed"])

    logger.info("=" * 60)
    logger.info("BRONZE INGESTION COMPLETE")
    logger.info(f"  Files loaded:  {total_success}")
    logger.info(f"  Files skipped: {total_skipped}")
    logger.info(f"  Files failed:  {total_failed}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()