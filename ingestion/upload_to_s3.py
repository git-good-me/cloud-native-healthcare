import boto3
import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
import sys

# ── Load config ────────────────────────────────────────────
load_dotenv("config/.env")

BASE_DIR     = Path(__file__).parent.parent
BRONZE_PATH  = BASE_DIR / "data/processed/bronze"
SILVER_PATH  = BASE_DIR / "data/processed/silver"
GOLD_PATH    = BASE_DIR / "data/processed/gold"

BRONZE_BUCKET = "cloud-native-healthcare-bronze"
SILVER_BUCKET = "cloud-native-healthcare-silver"
GOLD_BUCKET   = "cloud-native-healthcare-gold"

# ── Logging ────────────────────────────────────────────────
LOG_PATH = BASE_DIR / "logs"
LOG_PATH.mkdir(exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add(LOG_PATH / "upload_s3.log", rotation="1 MB")

# ── S3 client ──────────────────────────────────────────────
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

# ── Upload a local folder to an S3 bucket ─────────────────
def upload_folder(local_path: Path, bucket: str, prefix: str = ""):
    files = list(local_path.rglob("*.parquet"))
    if not files:
        logger.warning(f"No parquet files found in {local_path}")
        return 0

    uploaded = 0
    for file_path in files:
        # Build S3 key from relative path
        relative = file_path.relative_to(local_path)
        s3_key = f"{prefix}/{relative}".replace("\\", "/").lstrip("/")

        try:
            s3.upload_file(str(file_path), bucket, s3_key)
            logger.success(f"  ✓ {s3_key}")
            uploaded += 1
        except Exception as e:
            logger.error(f"  ✗ {s3_key}: {e}")

    return uploaded

# ── Main ───────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("S3 UPLOAD STARTED")
    logger.info("=" * 60)

    layers = [
        (BRONZE_PATH, BRONZE_BUCKET, "bronze", "Bronze"),
        (SILVER_PATH, SILVER_BUCKET, "silver", "Silver"),
        (GOLD_PATH,   GOLD_BUCKET,   "gold",   "Gold"),
    ]

    total = 0
    for local_path, bucket, prefix, label in layers:
        logger.info(f"\n── Uploading {label} → s3://{bucket}/")
        count = upload_folder(local_path, bucket, prefix)
        logger.info(f"  {label}: {count} files uploaded")
        total += count

    logger.info("\n" + "=" * 60)
    logger.info(f"UPLOAD COMPLETE — {total} files uploaded to S3")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()