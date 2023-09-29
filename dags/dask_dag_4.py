from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago

# Paths and table names
data_directory = "/opt/airflow/data/data_sample"
fact_table_name = "fact_product_table"
sensor_table_name = "sensor_table"
product_table_name = "product_table"
department_table_name = "department_table"

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
        task_id="read_data_with_dask",
        bash_command="python3 /opt/airflow/dags/DataProcessing.py"
    )

    end = DummyOperator(
        task_id='end',
    )
    
    start >> t1 >> end