from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
import dask.dataframe as dd
from dask.distributed import Client
from sqlalchemy import create_engine

def local_to_postgres():

    with Client() as client: # Using a context manager to ensure the client is closed after use
        directory_path = "/opt/airflow/data_sample"
        fact_table_name = "fact_product_table"
        sensor_table_name = "sensor_table"
        products_table_name = "products_table"
        connection_string = "postgresql://postgres:5432/postgres"
        engine = create_engine(connection_string)

        all_df = dd.read_parquet(directory_path)
        sensor_df = all_df[["sensor_serial", "department_name"]].drop_duplicates(keep='first')
        product_df = all_df[["product_name"]].drop_duplicates(keep='first')

        all_df.compute().to_sql(fact_table_name, engine, if_exists="replace", index=False)
        sensor_df.compute().to_sql(sensor_table_name, engine, if_exists="replace", index=False)
        product_df.compute().to_sql(products_table_name, engine, if_exists="replace", index=False)

default_args = {
    'owner': 'Fiat',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dask_dag_2',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:
    
    start = DummyOperator(
        task_id='start',
    )

    t1 = PythonOperator(
        task_id="run_dask_script",
        python_callable=local_to_postgres
    )
    
    end = DummyOperator(
        task_id='end',
    )
    
    start >> t1 >> end