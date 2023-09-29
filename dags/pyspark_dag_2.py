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

new_directory = "/opt/airflow/data/data_sample_renamed"
org_directory = "/opt/airflow/data/data_sample"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def copy_and_rename_file(file, org_directory, new_directory):
    try:
        new_name = file.replace(':', '-').replace(' ', '_')
        shutil.copy(os.path.join(org_directory, file), os.path.join(new_directory, new_name))
        logging.info(f'Successfully copied and renamed file: {file} to {new_name}')
    except Exception as e:
        logging.error(f'Failed to copy and rename file: {file} due to {str(e)}')

def copy_and_rename_files_concurrently(org_directory, new_directory, max_workers=10):
    if not os.path.exists(new_directory):
        os.makedirs(new_directory, exist_ok=True)
    
    files_to_copy = [file for file in os.listdir(org_directory) if file.endswith('.parquet')]
    logging.info(f'Found {len(files_to_copy)} files to copy and rename')
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(copy_and_rename_file, files_to_copy, [org_directory]*len(files_to_copy), [new_directory]*len(files_to_copy))

def main_rename_file(org_directory, new_directory):
    logging.info('Starting the file renaming process')
    shutil.rmtree(new_directory, ignore_errors=True)
    logging.info(f'Deleted the existing directory: {new_directory}')
    
    copy_and_rename_files_concurrently(org_directory, new_directory)
    logging.info('File renaming process completed')
    
default_args = {
    'owner': 'Fiat',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pyspark_dag_2',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:
    
    start = DummyOperator(
        task_id='start',
    )
    
    t1 = PythonOperator(
        task_id="rename_script",
        python_callable=main_rename_file,
        op_kwargs={
            "org_directory":org_directory,
            "new_directory":new_directory
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