# load_to_bq_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Set up import path
PROJECT_ROOT = "/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl"
TASKS_PATH = os.path.join(PROJECT_ROOT, "airflow_tasks")
if TASKS_PATH not in sys.path:
    sys.path.append(TASKS_PATH)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
}

def run_bq_loader():
    """Wrapper for auto schema loader"""
    from python_tasks.load import bq_loader
    bq_loader.main()

def run_bq_explicit_loader():
    """Wrapper for explicit schema loader"""
    from python_tasks.load import bq_exp_raw_data
    bq_exp_raw_data.main()

with DAG(
    dag_id='load_to_bq_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['load'],
) as dag:

    load_auto_schema = PythonOperator(
        task_id='load_auto_schema',
        python_callable=run_bq_loader,
    )

    load_explicit_schema = PythonOperator(
        task_id='load_explicit_schema',
        python_callable=run_bq_explicit_loader,
    )

    load_auto_schema >> load_explicit_schema
