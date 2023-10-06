from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from DataCustomFunction import fetcher
from DataCustomFunction import functions
from DataCustomFunction import processor
from DataCustomFunction import plubisher
from dask.distributed import Client
from sqlalchemy import create_engine
import dask.dataframe as dd
import pandas as pd
import psycopg2
import string
import logging

logging.basicConfig(filename='/opt/airflow/logs/manual_data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Path Variables
data_directory = "/opt/airflow/data/data_sample"

# Name Variables
fact_table_name = "fact_product_table"
sensor_table_name = "sensor_table"
product_table_name = "product_table"
department_table_name = "department_table"
fact_key_column = "create_at"
sensor_key_column = "sensor_serial"
product_key_column = "product_name"
department_key_column = "department_name"
product_id_column = "product_id"
department_id_column = "department_id"
product_id_name = "pid-"
department_id_name = "did-"

# Schema information for partitions
meta = pd.DataFrame(columns=['department_name',
                                'sensor_serial',
                                'create_at',
                                'product_name',
                                'product_expire'])
meta['create_at'] = pd.to_datetime(meta['create_at'])
meta['product_expire'] = pd.to_datetime(meta['product_expire'])

# Connection variables
conn_url = 'postgresql+psycopg2://airflow:airflow@postgres:5432/postgres'
conn_info = {
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "database": "postgres"
    }

def fact_product_table_ingestion(data_directory, conn_info, table_name, key_column, meta):
    client = Client(processes=False, n_workers=1, threads_per_worker=1)
    logging.info(f"Connected to Dask Client.")
    ddf = dd.read_parquet(data_directory)
    ddf = ddf.repartition(npartitions=30)
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        logging.info(f"Connected to PostgreSQL.")
        try :
            table_exists = fetcher.check_existing_table(conn_info=conn_info, table_name=table_name)
            
            if table_exists :
                logging.info(f"Table {table_name} exists. Checking for existing data...")
                data_exists = bool(fetcher.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
                if data_exists:
                    logging.info("Existing data found. Identifying new data...")
                    max_create_at = fetcher.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name)
                    ddf = ddf.map_partition(processor.return_latest_dataframe, max_create_at, meta=meta)
                else :
                    logging.info("No existing data found. Preparing entire dataset...")
            else:
                logging.info(f"Table {table_name} does not exist. Creating {table_name}...")
                sql_create_table = f'''
                CREATE TABLE {table_name} (
                    department_name VARCHAR(255),
                    sensor_serial VARCHAR(255),
                    create_at TIMESTAMP,
                    product_name VARCHAR(255),
                    product_expire TIMESTAMP
                );
                '''
                cur.execute(sql_create_table)
                conn.commit()
                logging.info(f"{table_name} created.")
            
            delayed_partitions = ddf.to_delayed()
            for i, partition in enumerate(delayed_partitions):
                plubisher.create_buffer_and_upload(partition.compute(), partition_num=i, conn_info=conn_info, index=False, header=False, table_name=table_name)
    
        except Exception as e:
            logging.error(f"Error in {table_name}_ingestion: {e}")
        finally :
            logging.info(f"{table_name}_ingestion completed.")
            client.close()
            logging.info(f"Dask client closed.")

def sensor_table_ingestion(data_directory, conn_info, conn_url, table_name, key_column):
    client = Client(processes=False, n_workers=1, threads_per_worker=1)
    logging.info(f"Connected to Dask Client.")
    ddf = dd.read_parquet(data_directory)
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        logging.info("Connected to PostgreSQL.")
        try :
            new_data_df = ddf[['sensor_serial', 'department_name', 'product_name']].drop_duplicates(subset=key_column)
            table_exists = fetcher.check_existing_table(conn_info=conn_info, table_name=table_name)
            if table_exists :
                logging.info(f"Table {table_name} exists. Checking for existing data...")
                data_exists = bool(fetcher.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
                if data_exists :
                    logging.info("Existing data found.")
                    exists_data_df = fetcher.get_all_data_from_table(conn_info=conn_info, table_name=table_name)
                    logging.info("Identifying new data...")
                    new_data_df = new_data_df[~new_data_df[key_column].isin(exists_data_df[key_column])]
                else :
                    logging.info("No existing data found. Preparing entire dataset...")
            else :
                sql_create_table = f'''
                CREATE TABLE {table_name} (
                    sensor_serial VARCHAR(255),
                    department_name VARCHAR(255),
                    product_name VARCHAR(255)
                );
                '''
                cur.execute(sql_create_table)
                conn.commit()
            
            result_df = new_data_df.compute()
            
            if result_df.empty :
                logging.info("There is no data to publish")
            else :
                logging.info("New data existed.")
                logging.info(f"Writing data to {table_name}...")
                engine = create_engine(conn_url)
                result_df.to_sql(table_name, engine, if_exists='append', index=False)
                engine.dispose()
                
        except Exception as e:
            logging.error(f"Error in {table_name}_ingestion: {e}")
        finally :
            logging.info(f"{table_name}_ingestion completed")
            client.close()
            logging.info(f"Dask client closed.")
        
def ids_table_ingestion(conn_info, conn_url, id_name,  table_name, id_column, key_column):
    long_of_text = 16
    text_list = list(string.ascii_lowercase + string.digits)
    id_name = id_name
    exist_ids_list = []
    client = Client(processes=False, n_workers=1, threads_per_worker=1)
    logging.info(f"Connected to Dask Client.")
    ddf = dd.read_parquet(data_directory)
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        logging.info("Connected to PostgreSQL.")
        try :
            new_data_df = ddf[[key_column]].drop_duplicates()
            table_exists = fetcher.check_existing_table(conn_info=conn_info, table_name=table_name)
            if table_exists :
                logging.info(f"Table {table_name} exists. Checking for existing data...")
                data_exists = bool(fetcher.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
                if data_exists :
                    logging.info("Existing data found.")
                    exists_data_df = fetcher.get_all_data_from_table(conn_info=conn_info, table_name=table_name)
                    logging.info("Identifying new data...")
                    exist_ids_list = exists_data_df[id_column].to_list()
                    logging.info("Comparing identified data to existed data...")
                    new_data_df = new_data_df[~new_data_df[key_column].isin(exists_data_df[key_column])]
                    
                else :
                    logging.info("No existing data found. Preparing entire dataset...")
                    
            else :
                sql_create_table = f'''
                CREATE TABLE {table_name} (
                    {id_column} VARCHAR(255),
                    {key_column} VARCHAR(255)
                )
                '''
                cur.execute(sql_create_table)
                conn.commit()
            
            result_df = new_data_df.compute()
            
            if result_df.empty :
                logging.info("There is no data to publish")
            else :
                logging.info("New data existed.")
                logging.info(f"Writing data to {table_name}...")
                unique_ids = functions.generate_random_char_ids(
                    unexist_row_count=len(new_data_df.index),
                    text_list=text_list,
                    long_of_text=long_of_text,
                    id_name=id_name,
                    exist_item_list=exist_ids_list
                )
                result_df[id_column] = unique_ids
                engine = create_engine(conn_url)
                result_df.to_sql(table_name, engine, if_exists='append', index=False)
                engine.dispose()
            
        except Exception as e :
            logging.error(f"Error in {table_name}_ingestion: {e}")
        finally :
            logging.info(f"{table_name}_ingestion completed")
            client.close()
            logging.info(f"Dask client closed.")

default_args = {
    'owner': 'Fiat',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dask_dag_3',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:
    
    start = DummyOperator(
        task_id='start',
    )
    
    t1 = PythonOperator(
        task_id="fact_product_table_ingestion",
        python_callable=fact_product_table_ingestion,
        op_kwargs={
            "data_directory": data_directory,
            "conn_info": conn_info,
            "table_name": fact_table_name,
            "key_column": fact_key_column,
            "meta": meta
        }
    )
    
    t2 = PythonOperator(
        task_id="sensor_table_ingestion",
        python_callable=sensor_table_ingestion,
        op_kwargs={
            "data_directory": data_directory,
            "conn_info": conn_info,
            "conn_url": conn_url,
            "table_name": sensor_table_name,
            "key_column": sensor_key_column
        }
    )
    
    t3 = PythonOperator(
        task_id="product_table_ingestion",
        python_callable=ids_table_ingestion,
        op_kwargs={
            "conn_info": conn_info,
            "conn_url": conn_url,
            "id_name": product_id_name, 
            "table_name": product_table_name,
            "id_column": product_id_column,
            "key_column": product_key_column
        }
    )
    
    t4 = PythonOperator(
        task_id="department_table_ingestion",
        python_callable=ids_table_ingestion,
        op_kwargs={
            "conn_info": conn_info,
            "conn_url": conn_url,
            "id_name": department_id_name, 
            "table_name": department_table_name,
            "id_column": department_id_column,
            "key_column": department_key_column
        }
    )

    end = DummyOperator(
        task_id='end',
    )
    
    start >> t1 >> t2 >> t3 >> t4 >> end