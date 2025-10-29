#!/usr/bin/env python3
"""
ingest_raw_export.py

Purpose:
  - Reads the raw JSON export (sample_raw_export.json)
  - Extracts 6 logical tables
  - Writes them as CSVs under ./data_extract/
  - (Optional) Uploads the CSVs to BigQuery (stage_* tables)

Usage:
  python ingest_raw_export.py
  python ingest_raw_export.py --upload-bq --bq-project your_project --bq-dataset stage
"""

import os
import json
import argparse
from pathlib import Path
import csv
import logging
import sys

# Optional external deps
try:
    import boto3
except Exception:
    boto3 = None

try:
    from google.cloud import bigquery
except Exception:
    bigquery = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------
# CONFIGURATION
# -----------------------------------

# Absolute path to your sample JSON file
DEFAULT_JSON_PATH = Path("/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl/sample_raw_export.json")

# Folder to save CSVs
DATA_EXTRACT_DIR = Path("data_extract")
DATA_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

# Mapping: top-level JSON keys → CSV filenames
TABLE_MAP = {
    "bookings": "stage_bookings.csv",
    "segments": "stage_segments.csv",
    "passengers": "stage_passengers.csv",
    "tickets": "stage_tickets.csv",
    "ticket_segment": "stage_ticket_segment.csv",
    "ticket_passenger": "stage_ticket_passenger.csv"
}


# -----------------------------------
# HELPER FUNCTIONS
# -----------------------------------

def download_from_s3(bucket, key, local_path):
    """Optional helper if you later fetch JSON from S3"""
    if boto3 is None:
        raise RuntimeError("boto3 not installed; cannot download from S3")
    s3 = boto3.client("s3")
    logger.info(f"Downloading s3://{bucket}/{key} -> {local_path}")
    s3.download_file(bucket, key, str(local_path))


def read_json(path: Path):
    """Read the JSON file"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_csv_from_list_of_dicts(rows, out_path):
    """Write a list of dicts into CSV, flattening nested dicts as JSON strings"""
    if not rows:
        logger.warning(f"No rows found for {out_path.name}, writing empty CSV")
        with open(out_path, "w", newline='', encoding='utf-8') as f:
            pass
        return

    # Collect all possible keys (handle schema drift)
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    fieldnames = sorted(all_keys)

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

    logger.info(f"Wrote {out_path} ({len(rows)} rows)")


def upload_to_bigquery(project_id, dataset_id, table_id, csv_path):
    """Optional upload to BigQuery"""
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery not installed")
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(f"Uploading {csv_path} -> {table_ref}")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV
    )

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()
    logger.info(f"Loaded {job.output_rows} rows into {table_ref}")


# -----------------------------------
# MAIN SCRIPT
# -----------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        help="Path to JSON file (optional, defaults to Siddaling's local path)",
        default=str(DEFAULT_JSON_PATH)
    )
    parser.add_argument("--upload-bq", action="store_true", help="Upload CSVs to BigQuery")
    parser.add_argument("--bq-project", help="BigQuery project ID")
    parser.add_argument("--bq-dataset", default="stage", help="BigQuery dataset (default: stage)")
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    logger.info(f"Reading JSON file from: {input_path}")
    data = read_json(input_path)

    for key, csv_name in TABLE_MAP.items():
        rows = data.get(key, [])
        csv_path = DATA_EXTRACT_DIR / csv_name
        write_csv_from_list_of_dicts(rows, csv_path)

        if args.upload_bq:
            if not args.bq_project:
                logger.error("--bq-project is required when using --upload-bq")
                continue
            table_name = csv_name.replace(".csv", "")
            upload_to_bigquery(args.bq_project, args.bq_dataset, table_name, csv_path)

    logger.info("✅ Ingestion completed successfully.")


if __name__ == "__main__":
    main()
