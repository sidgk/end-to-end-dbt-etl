from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

default_args = {
    'owner': 'siddu',
    'retries': 1,
    'start_date': datetime(2025, 10, 29)
}

DBT_PROJECT_DIR = '/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl'

with DAG(
    dag_id='dbt_core_models_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # 7:00 AM
    catchup=False,
    description='Run DBT core models after snapshots DAG completes',
) as dag:

    wait_for_snapshots = ExternalTaskSensor(
        task_id='wait_for_snapshot_dag',
        external_dag_id='dbt_snapshot_dag',
        external_task_id='run_snapshots',
        poke_interval=60,
        timeout=60 * 60,
        mode='reschedule'
    )

    run_core_models = BashOperator(
        task_id='run_core_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select tag:core'
    )

    wait_for_snapshots >> run_core_models
