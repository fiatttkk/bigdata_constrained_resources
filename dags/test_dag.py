from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    'test_spark_dag',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:
    
    start = DummyOperator(
        task_id='start',
    )
    
    t1 = SparkSubmitOperator(
        task_id="pyspark_dag",
        application="/opt/airflow/scripts/spark_test.py",
        conn_id="spark_default",
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

    start >> t1 >> end