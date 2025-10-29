"""
Airflow DAG: ingest_raw_export_dag

Purpose:
  - Orchestrates the extraction of raw JSON data into CSVs.
  - (Future) Can chain to BigQuery loaders (bq_loader.py, bq_exp_raw_data.py).

Project structure:
  airflow_tasks/
    └── python_tasks/
        ├── extract/
        │   └── ingest_raw_export.py
        ├── load/
        │   ├── bq_loader.py
        │   └── bq_exp_raw_data.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add airflow_tasks root to Python path
sys.path.append('/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl/airflow_tasks')

# Import your Python task
from python_tasks.extract.ingest_raw_export import main as run_ingest

# Default DAG settings
default_args = {
    'owner': 'siddu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='ingest_raw_export_dag',
    default_args=default_args,
    description='Extracts raw JSON data and generates stage CSVs',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['extract', 'raw_export', 'bq'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_raw_json',
        python_callable=run_ingest,
    )

    extract_task
