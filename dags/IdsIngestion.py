from dask.distributed import Client
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import dask.dataframe as dd
import logging
import string
from DataCustomFunction import fetcher
from DataCustomFunction import functions

logging.basicConfig(filename='/opt/airflow/logs/ids_data_ingestion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
                
def ids_table_ingestion(ddf, conn_info, conn_url, id_name,  table_name, id_column, key_column):
    long_of_text = 16
    text_list = list(string.ascii_lowercase + string.digits)
    id_name = id_name
    exist_ids_list = []
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        logging.info("Connected to PostgreSQL.")
        try :
            new_data_df = ddf[[key_column]].drop_duplicates().compute()
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
            
            if new_data_df.empty :
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
                new_data_df[id_column] = unique_ids
                engine = create_engine(conn_url)
                new_data_df.to_sql(table_name, engine, if_exists='append', index=False)
                engine.dispose()
            
        except Exception as e :
            logging.error(f"Error in {table_name}_ingestion: {e}")
        finally :
            logging.info(f"{table_name}_ingestion completed")
            
if __name__ == "__main__" :
    
    # Path Variables
    data_directory = "/opt/airflow/data/data_sample"

    # Name Variables
    product_table_name = "product_table"
    department_table_name = "department_table"
    product_key_column = "product_name"
    department_key_column = "department_name"
    product_id_column = "product_id"
    department_id_column = "department_id"
    product_id_name = "pid-"
    department_id_name = "did-"

    # Connection variables
    conn_url = 'postgresql+psycopg2://airflow:airflow@postgres:5432/postgres'
    conn_info = {
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",
        "database": "postgres"
        }
    
    client = Client(n_workers=4, threads_per_worker=1)
    logging.info(f"Connected to Dask Client.")
    ddf = dd.read_parquet(data_directory)
    
    ids_table_ingestion(ddf,
                        data_directory=data_directory,
                        conn_info=conn_info,
                        conn_url=conn_url,
                        id_name=product_id_name,
                        table_name=product_table_name,
                        id_column=product_id_name,
                        key_column=product_key_column)
    
    ids_table_ingestion(ddf,
                        data_directory=data_directory,
                        conn_info=conn_info,
                        conn_url=conn_url,
                        id_name=department_id_name,
                        table_name=department_table_name,
                        id_column=department_id_name,
                        key_column=department_key_column)
    
    client.close()
    logging.info(f"Dask client closed.")