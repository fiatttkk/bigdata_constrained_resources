from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Fiat',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dask_dag_4',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:
    
    start = DummyOperator(
        task_id='start',
    )
    
    t1 = BashOperator(
        task_id="fact_table_ingestion",
        bash_command="python3 /opt/airflow/dags/FactDataIngestion.py"
    )
    
    t2 = BashOperator(
        task_id="sensor_table_ingestion",
        bash_command="python3 /opt/airflow/dags/SensorDataIngestion.py"
    )
    
    t3 = BashOperator(
        task_id="ids_tables_ingestion",
        bash_command="python3 /opt/airflow/dags/IdsDataIngestion.py"
    )

    end = DummyOperator(
        task_id='end',
    )
    
    start >> t1 >> t2 >> t3 >> end