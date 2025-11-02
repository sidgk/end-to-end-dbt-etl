#!/usr/bin/env python3
"""
load_to_bq_stage.py

Purpose:
  Load CSVs from ./data_extract/<YYYY-MM-DD>/ into BigQuery stage tables.
  Overwrites each table daily with the latest CSV data.

Usage:
  python load_to_bq_stage.py --bq-project project_name --bq-dataset omio_stage
  python load_to_bq_stage.py --process-date 2025-10-29
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------
# CONFIGURATION
# ---------------------------------------
TABLE_MAP = {
    "stage_bookings.csv": "stage_bookings",
    "stage_segments.csv": "stage_segments",
    "stage_passengers.csv": "stage_passengers",
    "stage_tickets.csv": "stage_tickets",
    "stage_ticket_segment.csv": "stage_ticket_segment",
    "stage_ticket_passenger.csv": "stage_ticket_passenger",
}


# ---------------------------------------
# HELPER
# ---------------------------------------
def load_csv_to_bq(client, project_id, dataset_id, table_name, csv_path):
    """Loads one CSV into BigQuery (overwrite mode)"""
    table_ref = f"{project_id}.{dataset_id}.{table_name}"

    logger.info(f"🚀 Loading {csv_path.name} -> {table_ref} (overwrite mode)")
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV
    )

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()  # Wait for completion
    logger.info(f"✅ Loaded {job.output_rows} rows into {table_ref}")


# ---------------------------------------
# MAIN
# ---------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Load CSVs from data_extract into BigQuery stage tables.")
    parser.add_argument("--process-date", help="Date folder to process (YYYY-MM-DD). Defaults to yesterday.")
    parser.add_argument("--bq-project", required=True, help="BigQuery project ID")
    parser.add_argument("--bq-dataset", default="omio_stage", help="BigQuery dataset name (default: omio_stage)")
    args = parser.parse_args()

    # Determine date folder
    if args.process_date:
        process_date = args.process_date
    else:
        process_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    data_folder = Path(f"/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl/data_extract/{process_date}")
    if not data_folder.exists():
        logger.error(f"❌ Folder not found: {data_folder}")
        return

    logger.info(f"📅 Processing CSVs for date: {process_date}")
    client = bigquery.Client(project=args.bq_project)

    for csv_name, table_name in TABLE_MAP.items():
        csv_path = data_folder / csv_name
        if not csv_path.exists():
            logger.warning(f"⚠️ Missing expected file: {csv_path}")
            continue
        load_csv_to_bq(client, args.bq_project, args.bq_dataset, table_name, csv_path)

    logger.info("🎉 All available CSVs loaded successfully into BigQuery stage dataset.")


if __name__ == "__main__":
    main()
