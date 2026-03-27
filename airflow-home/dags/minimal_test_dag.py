"""
Minimal Airflow DAG for Render startup test
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'minimal_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

task = BashOperator(
    task_id='print_hello',
    bash_command='echo Hello, Airflow!',
    dag=dag,
)
