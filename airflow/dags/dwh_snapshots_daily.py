from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'siddu',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2025, 10, 29)
}

DBT_PROJECT_DIR = '/Users/siddaling.kattimani/Documents/CaseStudy/end-to-end-dbt-etl'

with DAG(
    dag_id='dwh_snapshots_daily',
    default_args=default_args,
    schedule_interval='0 5 * * *', 
    catchup=False,
    description='Run DBT snapshots after stage DAG completes',
) as dag:

    wait_for_stage = ExternalTaskSensor(
        task_id='wait_for_stage_dag',
        external_dag_id='dbt_stage_models',
        external_task_id='run_stage_models',
        poke_interval=60,
        timeout=60 * 60,  # 1 hour timeout
        mode='reschedule'
    )

    run_snapshots = BashOperator(
        task_id='run_snapshots',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt snapshot --select tag:snapshot'
    )

    wait_for_stage >> run_snapshots
