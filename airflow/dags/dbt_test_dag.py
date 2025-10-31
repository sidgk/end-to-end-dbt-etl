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
    dag_id='dbt_tests_dag',
    default_args=default_args,
    schedule_interval=None,  # Can be triggered automatically or manually
    catchup=False,
    description='Run DBT tests after core DAG completes',
) as dag:

    wait_for_core = ExternalTaskSensor(
        task_id='wait_for_core_dag',
        external_dag_id='dbt_core_models_dag',
        external_task_id='run_core_models',
        poke_interval=60,
        timeout=60 * 60,
        mode='reschedule'
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test'
    )

    wait_for_core >> run_dbt_tests
