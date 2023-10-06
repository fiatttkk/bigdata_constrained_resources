from DataCustomFunction import fetcher
from dask.distributed import Client
from sqlalchemy import create_engine
import dask.dataframe as dd
import pandas as pd
import psycopg2
import logging

logging.basicConfig(filename='/opt/airflow/logs/sensor_data_ingestion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sensor_table_ingestion(data_directory, conn_info, conn_url, table_name, key_column):
    client = Client(n_workers=4, threads_per_worker=1)
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
            
if __name__ == "__main__" :
    
    # Path Variables
    data_directory = "/opt/airflow/data/data_sample"

    # Name Variables
    sensor_table_name = "sensor_table"
    sensor_key_column = "sensor_serial"

    # Connection variables
    conn_url = 'postgresql+psycopg2://airflow:airflow@postgres:5432/postgres'
    conn_info = {
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",
        "database": "postgres"
        }
    
    sensor_table_ingestion(data_directory=data_directory,
                           conn_info=conn_info,
                           conn_url=conn_url,
                           table_name=sensor_table_name,
                           key_column=sensor_key_column)