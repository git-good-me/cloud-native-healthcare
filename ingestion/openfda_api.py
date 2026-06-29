"""
OpenFDA API Ingestion
=====================
Pulls drug adverse event reports from the FDA public API.
Writes raw results as Parquet to S3 Bronze bucket.

Why OpenFDA?
  Adds a pharmaceutical dimension to the CMS hospital dataset.
  Connects via reporter_qualification (hospital-reported events).
  Public API, no auth required.
  Rate limit: 240 req/min — we stay well under.

API docs: https://open.fda.gov/apis/drug/event/

Data flow:
  OpenFDA REST API
  -> paginated JSON responses
  -> flattened pandas DataFrame
  -> Parquet (local temp)
  -> S3 Bronze:  openfda_drug_events/drug=<name>/snapshot=<YYYY-QN>/

Run:
  python ingestion/openfda_api.py
  python ingestion/openfda_api.py --drug metformin --limit 500
"""

import os
import sys
import time
import logging
import argparse
import tempfile
from datetime import datetime, timezone, date
from pathlib import Path

import requests
import pandas as pd
import boto3
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv("config/.env")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BRONZE_BUCKET  = "cloud-native-healthcare-bronze"

BASE_URL      = "https://api.fda.gov/drug/event.json"
PAGE_SIZE     = 100      # max per request
REQUEST_DELAY = 0.3      # seconds between calls
MAX_RETRIES   = 3

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── S3 ────────────────────────────────────────────────────────────────────────

def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

# ── API helpers ───────────────────────────────────────────────────────────────

def fetch_page(search: str, skip: int = 0, limit: int = PAGE_SIZE):
    """Call OpenFDA and return parsed JSON, or None on permanent failure."""
    params = {"search": search, "limit": limit, "skip": skip}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                return None          # no results
            elif resp.status_code == 429:
                wait = 2 ** attempt
                log.warning("Rate limited — waiting %ds", wait)
                time.sleep(wait)
            else:
                log.error("HTTP %d on attempt %d: %s",
                          resp.status_code, attempt, resp.text[:120])
                time.sleep(1)
        except requests.RequestException as e:
            log.error("Request error (attempt %d): %s", attempt, e)
            time.sleep(1)

    log.error("All retries exhausted for skip=%d", skip)
    return None


def flatten_event(event: dict) -> dict:
    """Flatten one FDA adverse event record to a row dict."""
    patient  = event.get("patient", {})
    reactions = patient.get("reaction", [])
    primary_reaction = reactions[0].get("reactionmeddrapt", "") if reactions else ""

    drugs = patient.get("drug", [])
    # Suspect drug = drugcharacterization "1"
    suspect = next(
        (d for d in drugs if str(d.get("drugcharacterization", "")) == "1"),
        drugs[0] if drugs else {}
    )

    return {
        "safety_report_id":       event.get("safetyreportid"),
        "receive_date":           event.get("receivedate"),         # YYYYMMDD string
        "serious":                event.get("serious"),
        "serious_death":          event.get("seriousnessdeath"),
        "serious_hospitalized":   event.get("seriousnesshospitalization"),
        "reporter_country":       event.get("primarysource", {}).get("reportercountry"),
        "reporter_qualification": event.get("primarysource", {}).get("qualification"),
        "patient_age":            patient.get("patientonsetage"),
        "patient_age_unit":       patient.get("patientonsetageunit"),
        "patient_sex":            patient.get("patientsex"),
        "primary_reaction":       primary_reaction,
        "num_reactions":          len(reactions),
        "drug_name":              suspect.get("medicinalproduct", ""),
        "drug_indication":        suspect.get("drugindication", ""),
        "drug_route":             suspect.get("drugadministrationroute", ""),
        "num_drugs_reported":     len(drugs),
        "_ingested_at":           datetime.now(timezone.utc).isoformat(),
        "_source":                "openfda_drug_event",
    }

# ── Snapshot label ────────────────────────────────────────────────────────────

def current_snapshot() -> str:
    today = date.today()
    q = (today.month - 1) // 3 + 1
    return f"{today.year}-Q{q}"

# ── Main ingestion ────────────────────────────────────────────────────────────

def ingest(drug_name: str = "acetaminophen", total_limit: int = 500) -> pd.DataFrame | None:
    log.info("=" * 60)
    log.info("OpenFDA Ingestion  |  drug=%-20s  limit=%d", drug_name, total_limit)
    log.info("=" * 60)

    # Simple drug name search — OpenFDA Lucene syntax
    # Keep it minimal: complex AND chains cause 500 errors on some drug names
    # We filter for serious/hospitalized events after fetching
    search = f'patient.drug.medicinalproduct:"{drug_name}"'

    # ── Get total available count ─────────────────────────────────────────────
    first = fetch_page(search, skip=0, limit=1)
    if not first:
        log.warning("No results for drug '%s'. Try a different name.", drug_name)
        return None

    total_available = first.get("meta", {}).get("results", {}).get("total", 0)
    to_fetch = min(total_limit, total_available, 1000)   # OpenFDA hard cap = 1000
    log.info("Total available: %d  |  Fetching: %d", total_available, to_fetch)

    # ── Paginate ──────────────────────────────────────────────────────────────
    records = []
    skip = 0

    while skip < to_fetch:
        page_limit = min(PAGE_SIZE, to_fetch - skip)
        data = fetch_page(search, skip=skip, limit=page_limit)
        if not data:
            log.warning("Empty response at skip=%d — stopping pagination", skip)
            break

        for event in data.get("results", []):
            records.append(flatten_event(event))

        skip += len(data.get("results", []))
        log.info("  Fetched %d / %d", skip, to_fetch)
        time.sleep(REQUEST_DELAY)

    if not records:
        log.error("No records collected.")
        return None

    log.info("Total records: %d", len(records))

    # ── Build DataFrame ───────────────────────────────────────────────────────
    df = pd.DataFrame(records)

    for col in ["patient_age", "num_reactions", "num_drugs_reported",
                "serious", "serious_death", "serious_hospitalized"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Keep only hospitalization-related serious events (post-fetch filter)
    if "serious_hospitalized" in df.columns:
        before = len(df)
        df = df[df["serious_hospitalized"] == 1].reset_index(drop=True)
        log.info("Filtered to hospitalized serious events: %d → %d rows", before, len(df))

    snapshot = current_snapshot()
    df["snapshot_quarter"] = snapshot
    df["drug_searched"] = drug_name.lower()

    log.info("Schema:\n%s", df.dtypes.to_string())

    # ── Save & upload ─────────────────────────────────────────────────────────
    with tempfile.TemporaryDirectory() as tmp:
        safe_drug = re.sub(r"[^a-z0-9]", "_", drug_name.lower()) if True else drug_name
        filename  = f"openfda_{safe_drug}_{snapshot}.parquet"
        local_path = Path(tmp) / filename
        df.to_parquet(local_path, index=False)

        s3  = s3_client()
        key = f"openfda_drug_events/drug={safe_drug}/snapshot={snapshot}/{filename}"
        s3.upload_file(str(local_path), BRONZE_BUCKET, key)
        log.info("↑ Uploaded → s3://%s/%s", BRONZE_BUCKET, key)

    log.info("Ingestion complete.")
    return df

# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import re
    parser = argparse.ArgumentParser(description="Ingest OpenFDA drug adverse events")
    parser.add_argument("--drug",  default="acetaminophen",
                        help="Drug name to search (default: acetaminophen)")
    parser.add_argument("--limit", type=int, default=500,
                        help="Max records to fetch, up to 1000 (default: 500)")
    args = parser.parse_args()
    ingest(drug_name=args.drug, total_limit=args.limit)