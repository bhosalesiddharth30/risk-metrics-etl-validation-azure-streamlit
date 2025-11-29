"""
Simulated Azure ingestion function.

In production this would:
- be triggered by a timer or blob upload
- read a CSV/JSON file from Azure Blob Storage
- write it into the /data directory or a database.

For this demo we simply copy an existing local file
to a 'raw' location to mimic ingestion.
"""

from pathlib import Path
import shutil
from utils.logger import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True)


def main():
    logger.info("Starting simulated Azure ingest function")

    src = PROJECT_ROOT / "data" / "sample_risk_data.csv"
    dst = RAW_DIR / "risk_snapshot_latest.csv"

    shutil.copy(src, dst)
    logger.info(f"Copied {src} -> {dst}")


if __name__ == "__main__":
    main()
