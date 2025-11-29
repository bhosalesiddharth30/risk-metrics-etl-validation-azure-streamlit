"""
Simple ETL cleaner for risk data.
- Loads the 'raw' file created by azure_ingest_function.py
- Normalises column names
- Parses dates and trims whitespace
- Writes a cleaned CSV to data/processed/
"""

from pathlib import Path
import pandas as pd
from utils.logger import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(exist_ok=True)


def run_etl():
    raw_file = RAW_DIR / "risk_snapshot_latest.csv"
    logger.info(f"Running ETL on {raw_file}")

    df = pd.read_csv(raw_file)

    # normalise columns
    df.columns = [c.strip().lower() for c in df.columns]

    # basic clean-up
    df["date"] = pd.to_datetime(df["date"])
    df["portfolio"] = df["portfolio"].astype(str).str.strip()
    df["metric"] = df["metric"].astype(str).str.strip()

    cleaned_file = PROCESSED_DIR / "risk_snapshot_clean.csv"
    df.to_csv(cleaned_file, index=False)
    logger.info(f"Saved cleaned data to {cleaned_file}")

    return cleaned_file


if __name__ == "__main__":
    run_etl()
