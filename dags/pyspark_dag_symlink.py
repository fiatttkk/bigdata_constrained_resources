from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from concurrent.futures import ThreadPoolExecutor
import shutil
import os
import logging

temp_directory = "/opt/airflow/data/data_sample_temp"
org_directory = "/opt/airflow/data/data_sample"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_symlink(original_file):
    file_name = os.path.basename(original_file)
    symlink_name = file_name.replace(':', '-').replace(' ', '_')
    symlink_path = os.path.join(temp_directory, symlink_name)
    os.symlink(original_file, symlink_path)

def main_symlink(temp_directory):
    shutil.rmtree(temp_directory, ignore_errors=True)
    os.makedirs(temp_directory, exist_ok=True)
    all_files = [os.path.join(org_directory, file) for file in os.listdir(org_directory) if file.endswith('.parquet')]
    with ThreadPoolExecutor() as executor:
        executor.map(create_symlink, all_files)

default_args = {
    'owner': 'Fiat',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pyspark_dag_symlink',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:
    
    start = DummyOperator(
        task_id='start',
    )
    
    t1 = PythonOperator(
        task_id="rename_script",
        python_callable=main_symlink,
        op_kwargs={
            "temp_directory":temp_directory
        }
    )
    
    t2 = SparkSubmitOperator(
        task_id="pyspark_read_script",
        application="/opt/airflow/scripts/local_to_postgres_pyspark.py",
        conn_id="spark_default",
        jars="/opt/airflow/scripts/jars/postgresql-42.6.0.jar",
        conf={
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.total.executor.cores': '8',
            'spark.driver.memory': '2g',
            'spark.driver.cores': '1'
        }
    )
    
    end = DummyOperator(
        task_id='end',
    )

    start >> t1 >> t2 >> end