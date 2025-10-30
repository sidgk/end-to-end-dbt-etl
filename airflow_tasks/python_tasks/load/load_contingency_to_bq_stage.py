#!/usr/bin/env python3
"""
load_contingency_to_bq_stage.py

Purpose:
  Load contingency CSVs from a timestamped folder under ./data_extract/
  into BigQuery stage tables. Each run overwrites existing stage tables.

Example usage:
  python load_contingency_to_bq_stage.py --bq-project inspiring-ring-382618 --bq-dataset omio_stage
  python load_contingency_to_bq_stage.py --folder "2025-10-30 19:15:59 UTC"
"""

import os
import argparse
import logging
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery

# ---------------------------------------
# LOGGING CONFIGURATION
# ---------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------
# CONFIGURATION
# ---------------------------------------
TABLE_MAP = {
    "contengency_bookings.csv": "stage_bookings",
    "contengency_segments.csv": "stage_segments",
    "contengency_tickets.csv": "stage_tickets",
    "contengency_passengers.csv": "stage_passengers",
}

BASE_PATH = "/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl/data_extract"

# ---------------------------------------
# HELPER FUNCTION
# ---------------------------------------
def load_csv_to_bq(client, project_id, dataset_id, table_name, csv_path):
    """Loads one CSV file into BigQuery (overwrite mode)."""
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    logger.info(f"🚀 Loading {csv_path.name} → {table_ref} (overwrite mode)")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
    )

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

    job.result()  # Wait for completion
    logger.info(f"✅ Loaded {job.output_rows} rows into {table_ref}")

# ---------------------------------------
# MAIN FUNCTION
# ---------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Load contingency extract CSVs into BigQuery stage tables.")
    parser.add_argument("--bq-project", required=True, help="BigQuery project ID")
    parser.add_argument("--bq-dataset", default="omio_stage", help="BigQuery dataset (default: omio_stage)")
    parser.add_argument("--folder", help="Timestamped folder name under data_extract, e.g. '2025-10-30 19:15:59 UTC'")
    args = parser.parse_args()

    # Determine which folder to load
    if args.folder:
        data_folder = Path(BASE_PATH) / args.folder
    else:
        # Auto-detect latest timestamped folder
        all_folders = [
            f for f in Path(BASE_PATH).iterdir()
            if f.is_dir() and "UTC" in f.name
        ]
        if not all_folders:
            logger.error("❌ No timestamped contingency folders found under data_extract/")
            return
        data_folder = max(all_folders, key=os.path.getmtime)

    if not data_folder.exists():
        logger.error(f"❌ Folder not found: {data_folder}")
        return

    logger.info(f"📂 Using contingency folder: {data_folder}")
    client = bigquery.Client(project=args.bq_project)

    # Load all available contingency CSVs
    for csv_name, table_name in TABLE_MAP.items():
        csv_path = data_folder / csv_name
        if not csv_path.exists():
            logger.warning(f"⚠️ Missing file: {csv_path}")
            continue
        load_csv_to_bq(client, args.bq_project, args.bq_dataset, table_name, csv_path)

    logger.info("🎉 All available contingency CSVs loaded successfully into BigQuery stage dataset.")

# ---------------------------------------
# ENTRY POINT
# ---------------------------------------
if __name__ == "__main__":
    main()
