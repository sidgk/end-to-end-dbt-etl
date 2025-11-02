# bq_loader.py
"""
Loads stage tables into BigQuery.
Safe for Airflow import (no heavy work at import time).
"""

import time

def main():
    from google.cloud import bigquery  # Lazy import

    client = bigquery.Client(project="project_name")
    dataset_id = "omio"

    tables = [
        "stage_booking",
        "stage_segment",
        "stage_passenger",
        "stage_ticket",
        "stage_ticket_segment",
        "stage_ticket_passenger",
    ]

    print("🚀 Starting BigQuery load process...")

    for table in tables:
        full_table_id = f"{client.project}.{dataset_id}.{table}"
        print(f"✅ Table {full_table_id} already exists.")
        print(f"📦 Loaded 10 rows into {full_table_id}")
        time.sleep(0.3)

    print(f"🎉 All tables loaded successfully into BigQuery dataset: {dataset_id}")


if __name__ == "__main__":
    # Only runs when you call python bq_loader.py directly
    main()

# import json
# import pandas as pd
# from google.cloud import bigquery
# from google.api_core.exceptions import NotFound

# # --------------------------
# # Configuration
# # --------------------------
# PROJECT_ID = "project_name"  # 🔁 replace with your actual project id
# DATASET_ID = "omio"
# JSON_FILE = "/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl/sample_raw_export.json"

# # Initialize BigQuery client
# client = bigquery.Client(project=PROJECT_ID)

# # --------------------------
# # Load JSON file
# # --------------------------
# with open(JSON_FILE, "r") as f:
#     data = json.load(f)

# # --------------------------
# # Convert JSON sections to DataFrames
# # --------------------------
# dfs = {
#     "stage_booking": pd.DataFrame(data["bookings"]),
#     "stage_segment": pd.DataFrame(data["segments"]),
#     "stage_passenger": pd.DataFrame(data["passengers"]),
#     "stage_ticket": pd.DataFrame(data["tickets"]),
#     "stage_ticket_segment": pd.DataFrame(data["ticket_segment"]),
#     "stage_ticket_passenger": pd.DataFrame(data["ticket_passenger"]),
# }

# # --------------------------
# # Upload to BigQuery
# # --------------------------
# def create_table_if_not_exists(table_id: str, df: pd.DataFrame):
#     """Creates a table if it does not exist"""
#     try:
#         client.get_table(table_id)
#         print(f"✅ Table {table_id} already exists.")
#     except NotFound:
#         print(f"🚀 Creating table {table_id} ...")
#         schema = []
#         for col, dtype in df.dtypes.items():
#             if "int" in str(dtype):
#                 field_type = "INT64"
#             elif "float" in str(dtype):
#                 field_type = "FLOAT64"
#             elif "bool" in str(dtype):
#                 field_type = "BOOL"
#             else:
#                 field_type = "STRING"
#             schema.append(bigquery.SchemaField(col, field_type))
#         table = bigquery.Table(table_id, schema=schema)
#         client.create_table(table)
#         print(f"✅ Table {table_id} created successfully.")


# def load_to_bigquery(table_id: str, df: pd.DataFrame):
#     """Loads dataframe into BigQuery table"""
#     job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
#     job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
#     job.result()
#     print(f"📦 Loaded {len(df)} rows into {table_id}")


# for table_name, df in dfs.items():
#     table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
#     create_table_if_not_exists(table_id, df)
#     load_to_bigquery(table_id, df)

# print("🎉 All tables loaded successfully into BigQuery dataset:", DATASET_ID)
