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
import DaskCustomFunction as dc

logging.basicConfig(filename='/opt/airflow/logs/manual_data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            table_exists = dc.check_existing_table(conn_info=conn_info, table_name=table_name)
            
            if table_exists :
                logging.info(f"Table {table_name} exists. Checking for existing data...")
                data_exists = bool(dc.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
                
                if data_exists:
                    logging.info("Existing data found. Identifying new data...")
                    max_create_at = dc.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name)
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
                
            if dc.empty_ddf(ddf) :
                logging.info("No new data to insert.")
            
            else :
                logging.info("New data existed.")
    
        except Exception as e:
            logging.error(f"Error in fact_product_data_preparation: {e}")
            
        finally :
            logging.info("Fact product table ingestion is completed")
            
    return ddf

def sensor_data_preparation(ddf, conn_info, table_name, key_column):
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['sensor_serial', 'department_name', 'product_name']].drop_duplicates(subset='sensor_serial').compute
        logging.info(f"{table_name} data prepared.")
        logging.info(f"Checking for existing {table_name}...")
        table_exists = dc.check_existing_table(conn_info=conn_info, table_name=table_name)
        
        if table_exists :
            logging.info(f"Table {table_name} exists. Checking for existing data...")
            data_exists = bool(dc.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
            
            if data_exists :
                logging.info("Existing data found. Identifying new data...")
                existed_data_df = dc.get_all_data_from_table(conn_info=conn_info, table_name=table_name)
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
            
        if dc.empty_ddf(new_data_df) :
            logging.info("No new data to insert.")
            
        else :
            logging.info("New data existed.")
            
    except Exception as e:
        logging.error(f"Error in sensor_table_ingestion: {e}")
            
    finally :
        logging.info(f"{table_name}_data preparation completed.")
        
    return new_data_df
        
def product_data_preparation(ddf, conn_info, table_name, key_column, long_of_text, text_list, id_name):
    exist_ids_list = []
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['product_name']].drop_duplicates().compute()
        logging.info(f"{table_name} data prepared.")
        logging.info(f"Checking for existing {table_name}...")
        table_exists = dc.check_existing_table(conn_info=conn_info, table_name=table_name)
        
        if table_exists :
            logging.info(f"Table {table_name} exists. Checking for existing data...")
            data_exists = bool(dc.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
            
            if data_exists :
                logging.info("Existing data found. Identifying new data...")
                exist_data_df = dc.get_all_data_from_table(conn_info=conn_info, table_name=table_name)
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
        
        if dc.empty_ddf(new_data_df) :
            logging.info("No new data to insert.")
            
        else :
            logging.info("New data existed.")
            unique_ids = dc.generate_random_char_ids(
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
                
def department_data_preparation(ddf, conn_info, table_name, key_column, long_of_text, text_list, id_name):
    exist_ids_list = []
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['department_name']].drop_duplicates().compute()
        logging.info(f"{table_name} data prepared.")
        logging.info(f"Checking for existing {table_name}...")
        table_exists = dc.check_existing_table(conn_info=conn_info, table_name=table_name)
        
        if table_exists :
            logging.info(f"Table {table_name} exists. Checking for existing data...")
            data_exists = bool(dc.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
            
            if data_exists :
                logging.info("Existing data found. Identifying new data...")
                exist_data_df = dc.get_all_data_from_table(conn_info=conn_info, table_name=table_name)
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
        
        if dc.empty_ddf(new_data_df) :
            logging.info("No new data to insert.")
            
        else :
            logging.info("New data existed.")
            unique_ids = dc.generate_random_char_ids(
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
    
    # Generate IDs args
    long_of_text = 16
    text_list = list(string.ascii_lowercase + string.digits)
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
    
    try :
        client = Client(n_workers=4, threads_per_worker=1)
        ddf = dd.read_parquet(data_directory)
        ddf = ddf.repartition(npartitions=128)
        
        # Fact product table ingestion here
        fact_data_ddf = ddf.map_partitions(fact_product_data_preparation,
                                           conn_info=conn_info,
                                           table_name=fact_table_name,
                                           key_column=fact_key_column,
                                           meta=meta)
        fact_data_delayed_partitions = fact_data_ddf.to_delayed()
        
        for i in range(0, len(fact_data_delayed_partitions), 4) :
            partition = dask.compute(fact_data_delayed_partitions[i:i+4])
            create_buffer_and_upload(partition,
                                     partition_num=i,
                                     conn_info=conn_info,
                                     index=False,
                                     header=False,
                                     table_name=fact_table_name)
        
        # Sensor table ingestion here, return sensor dataframe
        new_sensor_df = sensor_data_preparation(ddf,
                                                conn_info=conn_info,
                                                table_name=sensor_table_name,
                                                key_column=sensor_key_column)
        new_sensor_df.to_sql(sensor_table_name, if_exists='append', index=False)
        
        # Get new data from functions
        new_product_df = product_data_preparation(ddf,
                                                  conn_info=conn_info,
                                                  table_name=product_table_name,
                                                  key_column=product_key_column,
                                                  long_of_text=long_of_text,
                                                  text_list=text_list,
                                                  id_name=product_id_name)
        new_department_df = department_data_preparation(ddf,
                                                     conn_info=conn_info,
                                                     table_name=department_table_name,
                                                     key_column=department_key_column,
                                                     long_of_text=long_of_text,
                                                     text_list=text_list,
                                                     id_name=department_id_name)
        
        # Upload to postgresql
        logging.info(f"Uploading data to {product_table_name}")
        new_product_df.to_sql(product_table_name,  if_exists='append', index=False)
        logging.info(f"Uploading data to {department_table_name}")
        new_department_df.to_sql(department_table_name,  if_exists='append', index=False)
        
    except Exception as e :
        logging.error(f"Error in script: {e}")
        
    finally :
        client.close()
        logging.info("Ingestion completed")