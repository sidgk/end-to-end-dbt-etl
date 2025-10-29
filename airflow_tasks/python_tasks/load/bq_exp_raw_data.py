import json
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def main():
    # ---- CONFIG ----
    json_path = "/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl/sample_raw_export.json"
    project_id = "inspiring-ring-382618"  # ⬅️ replace with your actual project ID
    dataset_id = "omio"  # ⬅️ choose dataset name (will be auto-created)

    # ---- LOAD JSON ----
    with open(json_path, "r") as f:
        raw_data = json.load(f)

    # ---- CONVERT TO DATAFRAMES ----
    bookings_df = pd.DataFrame(raw_data["bookings"])
    segments_df = pd.DataFrame(raw_data["segments"])
    passengers_df = pd.DataFrame(raw_data["passengers"])
    tickets_df = pd.DataFrame(raw_data["tickets"])
    ticket_segment_df = pd.DataFrame(raw_data["ticket_segment"])
    ticket_passenger_df = pd.DataFrame(raw_data["ticket_passenger"])

    # ---- INIT BIGQUERY CLIENT ----
    client = bigquery.Client(project=project_id)

    # ---- CREATE DATASET IF NOT EXISTS ----
    def create_dataset_if_not_exists(client, project_id, dataset_id):
        dataset_ref = f"{project_id}.{dataset_id}"
        try:
            client.get_dataset(dataset_ref)
            print(f"✅ Dataset '{dataset_ref}' already exists.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "EU"
            client.create_dataset(dataset)
            print(f"🆕 Created dataset: {dataset_ref}")

    create_dataset_if_not_exists(client, project_id, dataset_id)

    # ---- TABLE SCHEMAS ----
    schemas = {
        "bookings": [
            bigquery.SchemaField("bookingid", "STRING"),
            bigquery.SchemaField("createdAt", "TIMESTAMP"),
            bigquery.SchemaField("updatedAt", "TIMESTAMP"),
            bigquery.SchemaField("userSelectedCurrency", "STRING"),
            bigquery.SchemaField("partnerIdOffer", "STRING"),
            bigquery.SchemaField("totalPrice", "FLOAT"),
            bigquery.SchemaField("raw_meta", "JSON"),
        ],
        "segments": [
            bigquery.SchemaField("segmentid", "STRING"),
            bigquery.SchemaField("bookingid", "STRING"),
            bigquery.SchemaField("carriername", "STRING"),
            bigquery.SchemaField("departuredatetime", "TIMESTAMP"),
            bigquery.SchemaField("arrivaldatetime", "TIMESTAMP"),
            bigquery.SchemaField("travelmode", "STRING"),
            bigquery.SchemaField("origin", "STRING"),
            bigquery.SchemaField("destination", "STRING"),
        ],
        "passengers": [
            bigquery.SchemaField("passengerid", "STRING"),
            bigquery.SchemaField("bookingid", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("firstName", "STRING"),
            bigquery.SchemaField("lastName", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ],
        "tickets": [
            bigquery.SchemaField("ticketid", "STRING"),
            bigquery.SchemaField("bookingid", "STRING"),
            bigquery.SchemaField("bookingPrice", "FLOAT"),
            bigquery.SchemaField("bookingCurrency", "STRING"),
            bigquery.SchemaField("vendorCode", "STRING"),
            bigquery.SchemaField("issuedAt", "TIMESTAMP"),
            bigquery.SchemaField("fareClass", "STRING"),
        ],
        "ticket_segment": [
            bigquery.SchemaField("ticketId", "STRING"),
            bigquery.SchemaField("segmentId", "STRING"),
        ],
        "ticket_passenger": [
            bigquery.SchemaField("ticketId", "STRING"),
            bigquery.SchemaField("passengerId", "STRING"),
        ],
    }

    # ---- CREATE TABLE IF NOT EXISTS ----
    def create_table_if_not_exists(client, project_id, dataset_id, table_name, schema):
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        try:
            client.get_table(table_ref)
            print(f"✅ Table '{table_ref}' already exists.")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"🆕 Created table: {table_ref}")

    for table_name, schema in schemas.items():
        create_table_if_not_exists(client, project_id, dataset_id, table_name, schema)

    # ---- LOAD DATA INTO BIGQUERY ----
    def load_to_bq(df, table_name):
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"🚀 Loaded {len(df)} rows into {table_id}")

    # ---- UPLOAD ALL TABLES ----
    load_to_bq(bookings_df, "bookings")
    load_to_bq(segments_df, "segments")
    load_to_bq(passengers_df, "passengers")
    load_to_bq(tickets_df, "tickets")
    load_to_bq(ticket_segment_df, "ticket_segment")
    load_to_bq(ticket_passenger_df, "ticket_passenger")

    print("\n🎉 All tables created and loaded successfully in BigQuery!")


# ✅ Add this for Airflow safety
if __name__ == "__main__":
    main()
