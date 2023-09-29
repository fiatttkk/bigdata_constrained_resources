from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG

default_args = {
    'owner': 'Fiat',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dask_dag',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:
    
    start = DummyOperator(
        task_id='start',
    )

    t1 = BashOperator(
        task_id="run_dask_script",
        bash_command='python /opt/airflow/dags/scripts/local_to_postgres_dask.py',
    )
    
    end = DummyOperator(
        task_id='end',
    )
    
    start >> t1 >> end