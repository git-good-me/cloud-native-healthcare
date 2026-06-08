import pandas as pd
from pathlib import Path

# Absolute path - works regardless of where you run the script from
BASE_DIR = Path(__file__).parent.parent
SNAPSHOT = BASE_DIR / "data" / "raw" / "hospitals_2024" / "hospitals_01_2024"

TARGET_FILES = [
    "Hospital_General_Information.csv",
    "Complications_and_Deaths-Hospital.csv",
    "HCAHPS-Hospital.csv",
    "Healthcare_Associated_Infections-Hospital.csv",
    "Timely_and_Effective_Care-Hospital.csv",
    "Unplanned_Hospital_Visits-Hospital.csv",
    "Medicare_Hospital_Spending_Per_Patient-Hospital.csv",
]

print(f"Looking in: {SNAPSHOT}")

for filename in TARGET_FILES:
    filepath = SNAPSHOT / filename
    if not filepath.exists():
        print(f"\n NOT FOUND: {filename}")
        continue

    df_full = pd.read_csv(filepath, encoding="latin-1")
    df_sample = pd.read_csv(filepath, nrows=1, encoding="latin-1")
    print(f"\n{'='*60}")
    print(f"FILE: {filename}")
    print(f"Shape: {df_full.shape}")
    print(f"Columns: {list(df_sample.columns)}")