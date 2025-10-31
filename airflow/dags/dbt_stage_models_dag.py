from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'siddu',
    'retries': 1,
    'start_date': datetime(2025, 10, 29)
}

DBT_PROJECT_DIR = '/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl'

with DAG(
    dag_id='dbt_stage_models_dag',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # 5:00 AM
    catchup=False,
    description='Run DBT stage (raw) models tagged as "stage"',
) as dag:

    run_stage_models = BashOperator(
        task_id='run_stage_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select tag:stage'
    )
