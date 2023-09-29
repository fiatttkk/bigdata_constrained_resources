from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from dask.distributed import Client
from io import StringIO
import psycopg2
import pandas as pd
import dask.dataframe as dd
import dask
import random
import string
import logging

logging.basicConfig(filename='/opt/airflow/logs/manual_data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Variables
data_directory = "/opt/airflow/data/data_sample"
fact_table_name = "fact_product_table"
sensor_table_name = "sensor_table"
product_table_name = "product_table"
department_table_name = "department_table"
conn_info = {
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",
        "database": "postgres"
        }

def generate_ids(unexist_row_count, text_list, long_of_text, id_name, exist_item_list):
    logging.info("Generating Unigue IDs...")
    item_list = set(exist_item_list)
    unique_ids = []
    try :
        while len(unique_ids) < unexist_row_count:
            item_name = id_name + ''.join(random.choices(text_list, k=long_of_text))
            
            if item_name not in item_list:
                unique_ids.append(item_name)
                item_list.add(item_name)
                
        logging.info("Unigue IDs generated.")
        
    except Exception as e :
        logging.error(f"Error in generate_ids: {e}")
        
    return unique_ids

def get_latest_time(partition, max_create_at):
    return partition[partition["create_at"] > max_create_at]

def create_buffer_and_upload(partition, partition_num, conn_info, index, header, table_name):
    try :
        logging.info(f"Writing partition number {partition_num} to Postgresql.")
        with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
            with StringIO() as buffer:
                partition.to_csv(buffer, index=index, header=header, sep='\t')
                buffer.seek(0)
                copy_query = f"COPY {table_name} FROM STDIN WITH CSV DELIMITER '\t' NULL ''"
                cur.copy_expert(copy_query, buffer)
            conn.commit()
            logging.info(f"Partition: {partition_num} uploaded successfully to PostgreSQL.")
    except Exception as e:
        logging.error(f"Error in create_buffer_and_upload: {e}")

def fact_product_table_ingestion(data_directory, conn_info, fact_table_name):
    client = Client(processes=False, n_workers=1, threads_per_worker=1)
    logging.info("Reading data from parquet files...")
    ddf = dd.read_parquet(data_directory)
    ddf = ddf.repartition(npartitions=25)
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        try :
            logging.info("Connected to PostgreSQL database.")
            logging.info(f"Checking for existing {fact_table_name}...")
            sql_check_table = f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name='{fact_table_name}')"
            cur.execute(sql_check_table)
            table_exists = bool(cur.fetchone()[0])

            if table_exists :
                logging.info(f"Table {fact_table_name} exists. Checking for existing data...")
                sql_max_create_at = f"SELECT MAX(create_at) FROM {fact_table_name}"
                cur.execute(sql_max_create_at)
                max_create_at = cur.fetchone()[0]
                
                if max_create_at is not None :
                    logging.info("Existing data found. Identifying new data...")
                    new_data = ddf.map_partitions(get_latest_time, max_create_at)
                    cur.execute(f"DROP INDEX IF EXISTS idx_create_at; DROP INDEX IF EXISTS idx_sensor_serial")
                    conn.commit()
                    delayed_partitions = new_data.to_delayed()
                    
                else :
                    logging.info("No existing data found. Uploading entire dataset...")
                    delayed_partitions = ddf.to_delayed()

            else:
                logging.info(f"Table {fact_table_name} does not exist. Creating table...")
                sql_create_table = f'''
                CREATE TABLE {fact_table_name} (
                    department_name VARCHAR(255),
                    sensor_serial VARCHAR(255),
                    create_at TIMESTAMP,
                    product_name VARCHAR(255),
                    product_expire TIMESTAMP
                );
                '''
                cur.execute(sql_create_table)
                conn.commit()
                logging.info(f"Table {fact_table_name} created. Uploading data...")
                delayed_partitions = ddf.to_delayed()
            
            for i, partition in enumerate(delayed_partitions) :
                create_buffer_and_upload(partition.compute(), partition_num=i, conn_info=conn_info, index=False, header=False, table_name=fact_table_name)
    
        except Exception as e:
            logging.error(f"Error in all_data_to_postgresql: {e}")
            
        finally :
            sql_create_index = f'''
            CREATE INDEX IF NOT EXISTS idx_create_at ON {fact_table_name}(create_at);
            CREATE INDEX IF NOT EXISTS idx_sensor_serial_fact ON {fact_table_name}(sensor_serial);
            '''
            cur.execute(sql_create_index)
            conn.commit()
            logging.info(f"Index created on {fact_table_name}.")
            logging.info("Fact product table ingestion is completed")
            client.close()

def sensor_table_ingestion(fact_table_name, conn_info, sensor_table_name):
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        try :
            logging.info("Connected to PostgreSQL Database.")
            logging.info(f"Fecthing data from {fact_table_name}...")
            all_data_query = f"SELECT DISTINCT sensor_serial, department_name, product_name FROM {fact_table_name}"
            all_data_df = pd.read_sql_query(all_data_query, conn)
            logging.info("Data fetched.")
            logging.info(f"Checking for existing {sensor_table_name}...")
            sql_check_table = f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{sensor_table_name}')"
            cur.execute(sql_check_table)
            table_exists = bool(cur.fetchone()[0])
            
            if table_exists :
                logging.info(f"Table {sensor_table_name} exists. Checking for existing data...")
                sql_check_data = f"SELECT MAX(sensor_serial) FROM {sensor_table_name}"
                cur.execute(sql_check_data)
                data_exists = bool(cur.fetchone()[0])
                
                if data_exists :
                    logging.info("Existing data found. Identifying new data...")
                    exist_data_query = f"SELECT * FROM {sensor_table_name}"
                    exist_data_df = pd.read_sql_query(exist_data_query, conn)
                    new_data_df = all_data_df[~all_data_df['sensor_serial'].isin(exist_data_df)]
                    new_data_df.to_sql(sensor_table_name, conn, if_exists='append', index=True, index_label="sensor_serial")
                
                else :
                    all_data_df.to_sql(sensor_table_name, conn, if_exists='append', index=True, index_label="sensor_serial")
                    
            else :
                sql_create_table = f'''
                CREATE TABLE {sensor_table_name} (
                    sensor_serial VARCHAR(255),
                    department_name VARCHAR(255),
                    product_name VARCHAR(255)
                );
                '''
                cur.execute(sql_create_table)
                conn.commit()
                all_data_df.to_sql(sensor_table_name, if_exists='append', index=True, index_label="sensor_serial")
                
        except Exception as e:
            logging.error(f"Error in sensor_table_ingestion: {e}")
                
        finally :
            logging.info(f"Index created on {sensor_table_name}.")
            logging.info("Sensor table ingestion is completed.")
        
def product_table_ingestion(conn_info, sensor_table_name, product_table_name):
    long_of_text = 7
    text_list = list(string.ascii_lowercase)
    id_name = "pid-"
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        try :
            logging.info("Connected to PostgreSQL Database.")
            logging.info(f"Fecthing data from {sensor_table_name}...")
            all_data_query = f"SELECT DISTINCT product_name FROM {sensor_table_name}"
            product_df = pd.read_sql_query(all_data_query, conn)
            logging.info("Data fetched.")
            logging.info(f"Checking for existing {product_table_name}...")
            sql_check_table = f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name='{product_table_name}'"
            cur.execute(sql_check_table)
            table_exists = bool(cur.fetchone()[0])
            
            if table_exists :
                logging.info(f"Table {sensor_table_name} exists. Checking for existing data...")
                sql_check_data = f"SELECT MAX(product_name) FROM {product_table_name}"
                cur.execute(sql_check_data)
                data_exists = bool(cur.fetchone()[0])
                
                if data_exists :
                    logging.info("Existing data found. Identifying new data...")
                    exist_data_query = f"SELECT * FROM {product_table_name}"
                    exist_data_df = pd.read_sql_query(exist_data_query, conn)
                    exist_ids_list = exist_data_df["product_id"].tolist()
                    new_data_df = product_df[~product_df['product_name'].isin(exist_data_df)]
                    
                    if new_data_df.empty :
                        logging.info("There is no new Product IDs to add.")
                        pass
                    
                    else :
                        logging.info("Adding new Product IDs...")
                        unique_ids = generate_ids(
                            unexist_row_count=len(new_data_df.index),
                            text_list=text_list,
                            long_of_text=long_of_text,
                            id_name=id_name,
                            exist_item_list=exist_ids_list
                        )
                        new_data_df["product_id"] = unique_ids
                        new_data_df.to_sql(product_table_name,  if_exists='append', index=True, index_label="product_id")
                
                else :
                    unique_ids = generate_ids(
                        unexist_row_count=len(product_df.index),
                        text_list=text_list,
                        long_of_text=long_of_text,
                        id_name=id_name,
                        exist_item_list=[]
                    )
                    product_df["product_id"] = unique_ids
                    product_df.to_sql(product_table_name,  if_exists='append', index=True, index_label="product_id")
                    
            else :
                sql_create_table = f'''
                CREATE TABLE {product_table_name} (
                    product_id VARCHAR(255),
                    product_name VARCHAR(255)
                )
                '''
                cur.execute(sql_create_table)
                conn.commit()
                unique_ids = generate_ids(
                    unexist_row_count=len(product_df.index),
                    text_list=text_list,
                    long_of_text=long_of_text,
                    id_name=id_name,
                    exist_item_list=[]
                )
                product_df["product_id"] = unique_ids
                product_df.to_sql(product_table_name,  if_exists='append', index=True, index_label="product_id")
            
        except Exception as e :
            logging.error(f"Error in product_table_ingestion: {e}")
            
        finally :
            logging.info("Product table ingestion is completed.")
                
def department_table_ingestion(conn_info, sensor_table_name, department_table_name):
    long_of_text = 7
    text_list = list(string.ascii_lowercase)
    id_name = "did-"
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        try :
            logging.info("Connected to PostgreSQL Database.")
            logging.info(f"Fecthing data from {sensor_table_name}...")
            all_data_query = f"SELECT DISTINCT department_name FROM {sensor_table_name}"
            department_df = pd.read_sql_query(all_data_query, conn)
            logging.info("Data fetched.")
            logging.info(f"Checking for existing {department_table_name}...")
            sql_check_table = f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name='{department_table_name}'"
            cur.execute(sql_check_table)
            table_exists = bool(cur.fetchone()[0])
            
            if table_exists :
                logging.info(f"Table {sensor_table_name} exists. Checking for existing data...")
                sql_check_data = f"SELECT MAX(department_name) FROM {department_table_name}"
                cur.execute(sql_check_data)
                data_exists = bool(cur.fetchone()[0])
                
                if data_exists :
                    logging.info("Existing data found. Identifying new data...")
                    exist_data_query = f"SELECT * FROM {department_table_name}"
                    exist_data_df = pd.read_sql_query(exist_data_query, conn)
                    exist_ids_list = exist_data_df["department_id"].tolist()
                    new_data_df = department_df[~department_df['department_name'].isin(exist_data_df)]
                    
                    if new_data_df.empty :
                        logging.info("There is no new Department IDs to add.")
                        pass
                        
                    else :
                        logging.info("Adding new Department IDs...")
                        unique_ids = generate_ids(
                            unexist_row_count=len(new_data_df.index),
                            text_list=text_list,
                            long_of_text=long_of_text,
                            id_name=id_name,
                            exist_item_list=exist_ids_list
                        )
                        new_data_df["department_id"] = unique_ids
                        new_data_df.to_sql(department_table_name,  if_exists='append', index=True, index_label="department_id")
                
                else :
                    unique_ids = generate_ids(
                        unexist_row_count=len(department_df.index),
                        text_list=text_list,
                        long_of_text=long_of_text,
                        id_name=id_name,
                        exist_item_list=[]
                    )
                    department_df["department_id"] = unique_ids
                    department_df.to_sql(department_table_name,  if_exists='append', index=True, index_label="department_id")
                    
            else :
                sql_create_table = f'''
                CREATE TABLE {department_table_name} (
                    department_id VARCHAR(255),
                    department_name VARCHAR(255)
                );
                '''
                cur.execute(sql_create_table)
                conn.commit()
                unique_ids = generate_ids(
                    unexist_row_count=len(department_df.index),
                    text_list=text_list,
                    long_of_text=long_of_text,
                    id_name=id_name,
                    exist_item_list=[]
                )
                department_df["department_id"] = unique_ids
                department_df.to_sql(department_table_name,  if_exists='append', index=True, index_label="product_id")
                
        except Exception as e :
            logging.error(f"Error in department_table_ingestion: {e}")
            
        finally :
            logging.info("Department table ingestion is completed.")

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
            "fact_table_name": fact_table_name
        }
    )
    
    t2 = PythonOperator(
        task_id="sensor_table_ingestion",
        python_callable=sensor_table_ingestion,
        op_kwargs={
            "fact_table_name": fact_table_name,
            "conn_info": conn_info,
            "sensor_table_name": sensor_table_name
        }
    )
    
    t3 = PythonOperator(
        task_id="product_table_ingestion",
        python_callable=product_table_ingestion,
        op_kwargs={
            "conn_info": conn_info,
            "sensor_table_name": sensor_table_name,
            "product_table_name": product_table_name
        }
    )
    
    t4 = PythonOperator(
        task_id="department_table_ingestion",
        python_callable=department_table_ingestion,
        op_kwargs={
            "conn_info": conn_info,
            "sensor_table_name": sensor_table_name,
            "department_table_name": department_table_name
        }
    )

    end = DummyOperator(
        task_id='end',
    )
    
    start >> t1 >> t2 >> [t3, t4] >> end