from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG default arguments
default_args = {
    "owner": "siddu",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define your base paths
BASE_PATH = "/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl"
PYTHON_TASKS = f"{BASE_PATH}/airflow_tasks/python_tasks"

# DAG definition
with DAG(
    dag_id='etl_ingest_load',
    default_args=default_args,
    description='End-to-end ETL pipeline: Ingest raw JSON and load to BigQuery stage',
    schedule_interval='0 2 * * *',  # Every day at 02:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'bq', 'raw_ingest'],
) as dag:

    # 1 Extract task — run ingest_raw_export.py
    extract_raw = BashOperator(
        task_id="extract_raw_data",
        bash_command=(
            f"python {PYTHON_TASKS}/extract/ingest_raw_export.py "
            f"--process-date {{ ds }} "
        ),
    )

    # 2 Load task — run load_to_bq_stage.py
    load_to_bq_stage = BashOperator(
        task_id="load_to_bq_stage",
        bash_command=(
            f"python {PYTHON_TASKS}/load/load_to_bq_stage.py "
            f"--bq-project project_name "
            f"--bq-dataset omio_stage "
            f"--process-date {{ ds }} "
        ),
    )

    # Define task order
    extract_raw >> load_to_bq_stage
