from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import sys

# Set up the absolute path to include your Python task folder
BASE_DIR = "/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl"
PYTHON_TASKS_PATH = os.path.join(BASE_DIR, "airflow_tasks", "python_tasks", "contingency")
sys.path.append(PYTHON_TASKS_PATH)

# Import your contingency script’s main function
from contingency_full_extract import main as contingency_full_extract_main

# Define default args
default_args = {
    'owner': 'siddu',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

# Define the DAG
with DAG(
    dag_id='dwh_contingency_full_refresh_extract',
    default_args=default_args,
    schedule_interval=None,  # Run manually only
    catchup=False,
    tags=['contingency', 'emergency', 'manual'],
    description='Emergency DAG to reprocess all raw_data files and export combined extracts'
) as dag:

    # Define the Python task
    contingency_extract_task = PythonOperator(
        task_id='run_contingency_full_extract',
        python_callable=contingency_full_extract_main,
        dag=dag
    )

    contingency_extract_task
