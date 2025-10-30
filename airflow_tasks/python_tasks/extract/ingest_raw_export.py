#!/usr/bin/env python3
"""
ingest_raw_export.py

Purpose:
  - Reads the daily raw JSON export (from raw_data/<yesterday>/)
  - Extracts 6 logical tables
  - Adds a dwh_load_time column (UTC timestamp)
  - Writes CSVs under data_extract/<yesterday>/
  - Optionally uploads them to BigQuery

Usage:
  python ingest_raw_export.py
  python ingest_raw_export.py --upload-bq --bq-project inspiring-ring-382618 --bq-dataset omio_raw
"""

import os
import json
import argparse
from pathlib import Path
import csv
import logging
import sys
from datetime import date, timedelta, datetime

# Optional external deps
try:
    import boto3
except Exception:
    boto3 = None

try:
    from google.cloud import bigquery
except Exception:
    bigquery = None


# -----------------------------------
# CONFIGURATION
# -----------------------------------

BASE_PATH = Path("/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl")
RAW_DATA_DIR = BASE_PATH / "raw_data"
DATA_EXTRACT_BASE_DIR = BASE_PATH / "data_extract"

TABLE_MAP = {
    "bookings": "stage_bookings.csv",
    "segments": "stage_segments.csv",
    "passengers": "stage_passengers.csv",
    "tickets": "stage_tickets.csv",
    "ticket_segment": "stage_ticket_segment.csv",
    "ticket_passenger": "stage_ticket_passenger.csv"
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------------------
# HELPER FUNCTIONS
# -----------------------------------

def read_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_csv_with_timestamp(rows, out_path, dwh_load_time):
    """Write list of dicts to CSV with dwh_load_time column"""
    if not rows:
        logger.warning(f"No rows for {out_path.name}, writing empty CSV")
        with open(out_path, "w", newline='', encoding='utf-8') as f:
            pass
        return

    # add dwh_load_time column
    for r in rows:
        r["dwh_load_time"] = dwh_load_time

    # collect all columns
    fieldnames = sorted(set().union(*(r.keys() for r in rows)))

    with open(out_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            flat_row = {}
            for k in fieldnames:
                v = row.get(k)
                if isinstance(v, (dict, list)):
                    flat_row[k] = json.dumps(v, ensure_ascii=False)
                else:
                    flat_row[k] = v
            writer.writerow(flat_row)

    logger.info(f"💾 Wrote {out_path} ({len(rows)} rows) with dwh_load_time={dwh_load_time}")


def upload_to_bigquery(project_id, dataset_id, table_id, csv_path):
    """Optional upload to BigQuery"""
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery not installed")
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(f"🚀 Uploading {csv_path} -> {table_ref}")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
    )

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()
    logger.info(f"✅ Loaded {job.output_rows} rows into {table_ref}")


# -----------------------------------
# MAIN SCRIPT
# -----------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload-bq", action="store_true", help="Upload CSVs to BigQuery")
    parser.add_argument("--bq-project", help="BigQuery project ID")
    parser.add_argument("--bq-dataset", default="omio_raw", help="BigQuery dataset (default: omio_raw)")
    args = parser.parse_args()

    # use yesterday’s data
    process_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    dwh_load_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    raw_folder = RAW_DATA_DIR / process_date
    extract_folder = DATA_EXTRACT_BASE_DIR / process_date
    extract_folder.mkdir(parents=True, exist_ok=True)

    logger.info(f"📅 Processing date: {process_date}")
    logger.info(f"🔍 Looking for JSON in: {raw_folder}")

    # find JSON file
    json_files = list(raw_folder.glob("*.json"))
    if not json_files:
        logger.error(f"No JSON files found in {raw_folder}")
        sys.exit(1)

    json_path = json_files[0]
    logger.info(f"📦 Reading file: {json_path}")
    data = read_json(json_path)

    for key, csv_name in TABLE_MAP.items():
        rows = data.get(key, [])
        csv_path = extract_folder / csv_name
        write_csv_with_timestamp(rows, csv_path, dwh_load_time)

        if args.upload_bq:
            if not args.bq_project:
                logger.error("--bq-project is required when using --upload-bq")
                continue
            table_name = csv_name.replace(".csv", "")
            upload_to_bigquery(args.bq_project, args.bq_dataset, table_name, csv_path)

    logger.info(f"🎉 Ingestion for {process_date} completed successfully.")


if __name__ == "__main__":
    main()
