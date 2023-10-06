from dask.distributed import Client
from io import StringIO
import psycopg2
import pandas as pd
import dask.dataframe as dd
import logging
from DataCustomFunction import fetcher
from DataCustomFunction import processor
from DataCustomFunction import plubisher

logging.basicConfig(filename='/opt/airflow/logs/fact_data_ingestion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fact_product_table_ingestion(data_directory, conn_info, table_name, key_column, meta):
    client = Client(n_workers=4, threads_per_worker=1)
    logging.info(f"Connected to Dask Client.")
    ddf = dd.read_parquet(data_directory)
    ddf = ddf.repartition(npartitions=200)
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

if __name__ == "__main__" :

    # Path Variables
    data_directory = "/opt/airflow/data/data_sample"
    
    # Name Variables
    fact_table_name = "fact_product_table"
    fact_key_column = "create_at"
    
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
    
    fact_product_table_ingestion(data_directory=data_directory,
                                 conn_info=conn_info,
                                 conn_url=conn_url,
                                 table_name=fact_table_name,
                                 key_column=fact_key_column,
                                 meta=meta)