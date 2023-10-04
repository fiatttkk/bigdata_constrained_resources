from dask.distributed import Client, LocalCluster
from io import StringIO
import psycopg2
import pandas as pd
import dask.dataframe as dd
import dask
import random
import string
import logging

logging.basicConfig(filename='/opt/airflow/logs/manual_data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths and table names
data_directory = "/opt/airflow/data/data_sample"
fact_table_name = "fact_product_table"
sensor_table_name = "sensor_table"
product_table_name = "product_table"
department_table_name = "department_table"


# def generate_ids(unexist_item_list, text_list, long_of_text, id_name, exist_item_list):
#     item_list = []
#     while len(item_list) < len(unexist_item_list) :
#         item_name = id_name + ''.join(random.choices(text_list, k = long_of_text))
#         if item_name not in item_list and item_name not in exist_item_list :
#             item_list.append(item_name)
#     return item_list

# def drop_duplicates_partition(partition, subset):
#     return partition.drop_duplicates(subset=subset)

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
            logging.info(f"Partition {partition_num} uploaded successfully to PostgreSQL.")
    except Exception as e:
        logging.error(f"Error in create_buffer_and_upload Partition {partition_num}: {e}")

def all_data_to_postgresql(data_directory, fact_table_name):
    client = Client(n_workers=4, threads_per_worker=1)
    logging.info("Reading data from parquet files...")
    ddf = dd.read_parquet(data_directory)
    ddf = ddf.repartition(npartitions=100)
    conn_info = {
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",
        "database": "postgres"
        }
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        try :
            logging.info("Connected to PostgreSQL database.")
            query_check_table = f"SELECT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{fact_table_name}') AS tmp"
            cur.execute(query_check_table)
            table_exists = cur.fetchone()[0]

            if table_exists :
                logging.info(f"Table {fact_table_name} exists. Checking for existing data...")
                cur.execute(f"SELECT 1 FROM {fact_table_name} LIMIT 1")
                data_exists = bool(cur.fetchone())
                
                if data_exists :
                    logging.info("Existing data found. Identifying new data...")
                    cur.execute(f"SELECT MAX(create_at) AS max_create_at FROM {fact_table_name}")
                    max_create_at = cur.fetchone()[0]
                    new_data = ddf.map_partitions(get_latest_time, max_create_at)
                    cur.execute(f"DROP INDEX IF EXISTS idx_create_at;")
                    conn.commit()
                    delayed_partitions = new_data.to_delayed()
                    tasks = [dask.delayed(create_buffer_and_upload)(partition, partition_num=i, conn_info=conn_info, index=False, header=False, table_name=fact_table_name) for i, partition in enumerate(delayed_partitions)]
                    dask.compute(*tasks)
                    
                else :
                    logging.info("No existing data found. Uploading entire dataset...")
                    delayed_partitions = ddf.to_delayed()
                    tasks = [dask.delayed(create_buffer_and_upload)(partition, partition_num=i, conn_info=conn_info, index=False, header=False, table_name=fact_table_name) for i, partition in enumerate(delayed_partitions)]
                    dask.compute(*tasks)
    
            else:
                logging.info(f"Table {fact_table_name} does not exist. Creating table...")
                sql = f'''
                CREATE TABLE {fact_table_name} (
                    department_name VARCHAR(255),
                    sensor_serial VARCHAR(255),
                    create_at TIMESTAMP,
                    product_name VARCHAR(255),
                    product_expire TIMESTAMP
                );
                '''
                cur.execute(sql)
                conn.commit()
                logging.info(f"Table {fact_table_name} created. Uploading data...")
                delayed_partitions = ddf.to_delayed()
                tasks = [dask.delayed(create_buffer_and_upload)(partition, partition_num=i, conn_info=conn_info, index=False, header=False, table_name=fact_table_name) for i, partition in enumerate(delayed_partitions)]
                dask.compute(*tasks)
    
        except Exception as e:
            logging.error(f"Error in all_data_to_postgresql: {e}")
            
        finally :
            with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_create_at ON {fact_table_name}")
            client.close()
            
if __name__ == "__main__" :
    all_data_to_postgresql(data_directory, fact_table_name)