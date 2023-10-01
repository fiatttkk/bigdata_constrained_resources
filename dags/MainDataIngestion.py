from dask.distributed import Client
from io import StringIO
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import dask.dataframe as dd
import dask
import logging
import random
import string

logging.basicConfig(filename='/opt/airflow/logs/manual_data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def get_max_value_from_table(conn_info, column_name, table_name):
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        logging.info(f"Checking for existing data on {table_name}...")
        cur.execute(f"SELECT MAX({column_name}) FROM {table_name}")
        max_value = cur.fetchone()[0]
    return max_value

def get_all_data_from_table(conn_url, table_name):
    with create_engine(conn_url) as engine :
        logging.info(f"Fetching all data from {table_name}...")
        df = pd.read_sql_table(table_name, engine)
    return df

def check_existing_table(conn_info, table_name):
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        logging.info(f"Checking for existing {table_name}...")
        cur.execute(f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name='{table_name}')")
        table_exists = bool(cur.fetchone()[0])
    return table_exists

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

def fact_product_data_preparation(ddf, conn_info, table_name, key_column):
    logging.info(f"Preparing {table_name} data...")
    with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
        try :
            logging.info(f"Checking for existing {table_name}...")
            table_exists = check_existing_table(conn_info=conn_info, table_name=table_name)
            
            if table_exists :
                logging.info(f"Table {table_name} exists. Checking for existing data...")
                data_exists = bool(get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
                
                if data_exists:
                    logging.info("Existing data found. Identifying new data...")
                    max_create_at = get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name)
                    ddf = ddf[ddf["create_at"] > max_create_at]
                    
                else :
                    logging.info("No existing data found. Uploading entire dataset...")

            else:
                logging.info(f"Table {table_name} does not exist. Creating table...")
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
                
            if ddf.empty :
                logging.info("No new data to insert.")
            
            else :
                logging.info("New data existed.")
    
        except Exception as e:
            logging.error(f"Error in fact_product_data_preparation: {e}")
            
        finally :
            logging.info("Fact product table ingestion is completed")
            
    return ddf

def sensor_data_preparation(ddf, conn_info, conn_url, table_name, key_column):
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['sensor_serial', 'department_name', 'product_name']].drop_duplicates(subset='sensor_serial')
        logging.info(f"{table_name} data prepared.")
        logging.info(f"Checking for existing {table_name}...")
        table_exists = check_existing_table(conn_info=conn_info, table_name=table_name)
        
        if table_exists :
            logging.info(f"Table {table_name} exists. Checking for existing data...")
            data_exists = bool(get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
            
            if data_exists :
                logging.info("Existing data found. Identifying new data...")
                existed_data_df = get_all_data_from_table(conn_url=conn_url, table_name=table_name)
                new_data_df = new_data_df[~new_data_df['sensor_serial'].isin(existed_data_df['sensor_serial'])]
                
            else :
                logging.info("No existing data found. Uploading entire dataset...")
                
        else :
            with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
                sql_create_table = f'''
                CREATE TABLE {sensor_table_name} (
                    sensor_serial VARCHAR(255),
                    department_name VARCHAR(255),
                    product_name VARCHAR(255)
                );
                '''
                cur.execute(sql_create_table)
                conn.commit()
            
        if new_data_df.empty :
            logging.info("No new data to insert.")
            
        else :
            logging.info("New data existed.")
            
    except Exception as e:
        logging.error(f"Error in sensor_table_ingestion: {e}")
            
    finally :
        logging.info(f"{table_name}_data preparation completed.")
        
    return new_data_df
        
def product_data_preparation(ddf, conn_info, conn_url, table_name, key_column):
    long_of_text = 7
    text_list = list(string.ascii_lowercase)
    id_name = "pid-"
    exist_ids_list = []
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['product_name']].drop_duplicates()
        logging.info(f"{table_name} data prepared.")
        logging.info(f"Checking for existing {table_name}...")
        table_exists = check_existing_table(conn_info=conn_info, table_name=table_name)
        
        if table_exists :
            logging.info(f"Table {table_name} exists. Checking for existing data...")
            data_exists = bool(get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
            
            if data_exists :
                logging.info("Existing data found. Identifying new data...")
                exist_data_df = get_all_data_from_table(conn_url=conn_url, table_name=table_name)
                exist_ids_list = exist_data_df['product_id'].to_list()
                new_data_df = new_data_df[~new_data_df['product_name'].isin(exist_data_df['product_name'])]
            
            else :
                logging.info("No existing data found. Uploading entire dataset...")
                
        else :
            sql_create_table = f'''
            CREATE TABLE {table_name} (
                product_id VARCHAR(255),
                product_name VARCHAR(255)
            )
            '''
            with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
                cur.execute(sql_create_table)
                conn.commit()
        
        if new_data_df.empty :
            logging.info("No new data to insert.")
            
        else :
            logging.info("New data existed.")
            unique_ids = generate_ids(
                        unexist_row_count=len(new_data_df.index),
                        text_list=text_list,
                        long_of_text=long_of_text,
                        id_name=id_name,
                        exist_item_list=exist_ids_list
                    )
            new_data_df['product_id'] = unique_ids
            new_data_df = new_data_df[['product_id', 'product_name']]
        
    except Exception as e :
        logging.error(f"Error in {table_name}_ingestion: {e}")
        
    finally :
        logging.info(f"{table_name}_data preparation completed.")
        
    return new_data_df
                
def deparment_data_preparation(ddf, conn_info, conn_url, table_name, key_column):
    long_of_text = 7
    text_list = list(string.ascii_lowercase)
    id_name = "did-"
    exist_ids_list = []
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['department_name']].drop_duplicates()
        logging.info(f"{table_name} data prepared.")
        logging.info(f"Checking for existing {table_name}...")
        table_exists = check_existing_table(conn_info=conn_info, table_name=table_name)
        
        if table_exists :
            logging.info(f"Table {table_name} exists. Checking for existing data...")
            data_exists = bool(get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
            
            if data_exists :
                logging.info("Existing data found. Identifying new data...")
                exist_data_df = get_all_data_from_table(conn_url=conn_url, table_name=table_name)
                exist_ids_list = exist_data_df['department_id'].to_list()
                new_data_df = new_data_df[~new_data_df['department_name'].isin(exist_data_df['department_name'])]
            
            else :
                logging.info("No existing data found. Uploading entire dataset...")
                
        else :
            sql_create_table = f'''
            CREATE TABLE {table_name} (
                department_id VARCHAR(255),
                department_name VARCHAR(255)
            )
            '''
            with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
                cur.execute(sql_create_table)
                conn.commit()
        
        if new_data_df.empty :
            logging.info("No new data to insert.")
            
        else :
            logging.info("New data existed.")
            unique_ids = generate_ids(
                        unexist_row_count=len(new_data_df.index),
                        text_list=text_list,
                        long_of_text=long_of_text,
                        id_name=id_name,
                        exist_item_list=exist_ids_list
                    )
            new_data_df['department_id'] = unique_ids
            new_data_df = new_data_df[['department_id', 'department_name']]
        
    except Exception as e :
        logging.error(f"Error in {table_name}_ingestion: {e}")
        
    finally :
        logging.info(f"{table_name}_data preparation completed.")
        
    return new_data_df

if __name__ == "__main__" :

    # Variables
    data_directory = "/opt/airflow/data/data_sample"
    fact_table_name = "fact_product_table"
    sensor_table_name = "sensor_table"
    product_table_name = "product_table"
    department_table_name = "department_table"
    fact_key_column = "create_at"
    sensor_key_column = "sensor_serial"
    product_key_column = "product_name"
    department_key_column = "department_name"
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
    
    try :
        client = Client(n_workers=4, threads_per_worker=1)
        ddf = dd.read_parquet(data_directory)
        ddf = ddf.repartition(npartitions=100)
        
        # Fact product table ingestion here
        fact_data_ddf = ddf.map_partitions(fact_product_data_preparation, conn_info=conn_info, table_name=fact_table_name, key_column=fact_key_column, meta=meta)
        fact_data_delayed_partitions = fact_data_ddf.to_delayed()
        for i in range(0, len(fact_data_delayed_partitions), 4) :
            partition = dask.compute(fact_data_delayed_partitions[i:i+4])
            create_buffer_and_upload(partition, partition_num=i, conn_info=conn_info, index=False, header=False, table_name=fact_table_name)
        
        # Sensor table ingestion here, return sensor dataframe
        new_sensor_df = dask.compute(dask.delayed(sensor_data_preparation)(ddf, conn_info=conn_info, conn_url=conn_url, table_name=sensor_table_name, key_column=sensor_key_column))
        new_sensor_df.to_sql(sensor_table_name,  if_exists='append', index=True, index_label="sensor_serial")
        
        # Concurrent functions with dask delayed
        new_product_df = dask.delayed(product_data_preparation)(ddf=new_sensor_df, conn_info=conn_info, conn_url=conn_url, table_name=product_table_name, key_column=product_key_column)
        new_department_df = dask.delayed(product_data_preparation)(ddf=new_sensor_df, conn_info=conn_info, conn_url=conn_url, table_name=product_table_name, key_column=product_key_column)
        
        # Compute delayed function
        new_product_df, new_department_df = dask.compute(new_product_df, new_department_df)
        new_product_df.to_sql(product_table_name,  if_exists='append', index=True, index_label="product_id")
        new_department_df.to_sql(department_table_name,  if_exists='append', index=True, index_label="department_id")
        
    except Exception as e :
        logging.error(f"Error in script: {e}")
        
    finally :
        client.close()
        logging.info("Ingestion completed")