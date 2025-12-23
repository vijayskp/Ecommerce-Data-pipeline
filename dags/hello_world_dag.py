from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "vijay",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hello_world_dag",
    default_args=default_args,
    description="Simple test DAG",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_hello = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello from Airflow DAG!'"
    )
