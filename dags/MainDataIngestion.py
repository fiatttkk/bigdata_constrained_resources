from dask.distributed import Client
from io import StringIO
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import dask.dataframe as dd
import dask
import logging
import string
import DataCustomFunction as dc

logging.basicConfig(filename='/opt/airflow/logs/manual_data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_buffer_and_upload(partition, conn_info, index, header, table_name):
    try :
        with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
            with StringIO() as buffer:
                partition.to_csv(buffer, index=index, header=header, sep=',')
                buffer.seek(0)
                if buffer.readline() != '' :
                    buffer.seek(0)
                    copy_query = f"COPY {table_name} FROM STDIN WITH CSV DELIMITER ',' NULL ''"
                    cur.copy_expert(copy_query, buffer)
                    conn.commit()
                else :
                    logging.info("There is no new data")
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
                    ddf = ddf.map_partition(dc.get_latest_dataframe, max_create_at)
                    
                else :
                    logging.info("No existing data found. Preparing entire dataset...")

            else:
                logging.info(f"Table {table_name} does not exist. Creating table...")
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
                logging.info(f"Table {table_name} created.")
    
        except Exception as e:
            logging.error(f"Error in fact_product_data_preparation: {e}")
            
    return ddf

def sensor_data_preparation(ddf, conn_info, table_name, key_column):
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['sensor_serial', 'department_name', 'product_name']].drop_duplicates(subset='sensor_serial').compute
        logging.info(f"{table_name} data fetched from Main Dataframe.")
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
                logging.info("No existing data found. Preparing entire dataset...")
                
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
            logging.info("No new data to insert. Go to next step")
            pass
            
        else :
            logging.info("New data existed.")
            logging.info("Sensor data preparation completed.")
            return new_data_df
            
    except Exception as e:
        logging.error(f"Error in sensor_table_ingestion: {e}")
        
def product_data_preparation(ddf, conn_info, table_name, key_column, long_of_text, text_list, id_name):
    exist_ids_list = []
    try :
        logging.info(f"Diagnosing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['product_name']].drop_duplicates().compute()
        logging.info(f"{table_name} data diagnosed from Main Dataframe.")
        logging.info(f"Waiting for IDs generator")
        logging.info(f"Checking for existing {table_name}...")
        table_exists = dc.check_existing_table(conn_info=conn_info, table_name=table_name)
        
        if table_exists :
            logging.info(f"Table {table_name} exists. Checking for existing data...")
            data_exists = bool(dc.get_max_value_from_table(conn_info=conn_info, column_name=key_column, table_name=table_name))
            
            if data_exists :
                logging.info("Existing data found. Identifying new data...")
                exist_data_df = dc.get_all_data_from_table(conn_info=conn_info, table_name=table_name)
                exist_ids_list = exist_data_df['product_id'].to_list()
                logging.info("Comparing identified data to existed data...")
                new_data_df = new_data_df[~new_data_df['product_name'].isin(exist_data_df['product_name'])]
            
            else :
                logging.info("No existing data found. Preparing entire dataset...")
                
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
            pass
            
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
            logging.info("Sensor data preparation completed.")
            return new_data_df
        
    except Exception as e :
        logging.error(f"Error in {table_name}_ingestion: {e}")
                
def department_data_preparation(ddf, conn_info, table_name, key_column, long_of_text, text_list, id_name):
    exist_ids_list = []
    try :
        logging.info(f"Preparing {table_name} data from Main Dataframe...")
        new_data_df = ddf[['department_name']].drop_duplicates().compute()
        logging.info(f"{table_name} data fetched from Main Dataframe.")
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
        
        if new_data_df.empty :
            logging.info("No new data to insert.")
            pass
            
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
            logging.info("Department data preparation completed.")
            return new_data_df
        
    except Exception as e :
        logging.error(f"Error in {table_name}_ingestion: {e}")

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
        client = Client(n_workers=1, threads_per_worker=1)
        ddf = dd.read_parquet(data_directory)
        ddf = ddf.repartition(npartitions=25)
        
        # Get
        fact_data_ddf = ddf.map_partitions(fact_product_data_preparation, conn_info=conn_info, table_name=fact_table_name, key_column=fact_key_column, meta=meta)
        fact_data_delayed_partitions = fact_data_ddf.partitions
        
        # Compute 4 tasks at a time
        for i in range(0, fact_data_ddf.npartitions) :
            try :
                logging.info(f"Writing Partition: {i} to Postgresql.")
                partition = fact_data_delayed_partitions[i].compute()
            
            except Exception as e :
                logging.error(f"Error while computing: {e}")
            try :
                create_buffer_and_upload(partition, conn_info=conn_info, index=False, header=False, table_name=fact_table_name)
                logging.info(f"Partition: {i} uploaded successfully to PostgreSQL.")
        
            except Exception as e :
                logging.error(f"Error in Partition: {i} {e}")
        
        # Get new sensor data form function and upload with create buffer and up load function
        new_sensor_df = sensor_data_preparation(ddf, conn_info=conn_info, table_name=sensor_table_name, key_column=sensor_key_column)
        create_buffer_and_upload(new_sensor_df, conn_info=conn_info, index=False, header=False, table_name=sensor_table_name)
        
        # Get new product data and department data from functions
        new_product_df = product_data_preparation(ddf, conn_info=conn_info, table_name=product_table_name, key_column=product_key_column, long_of_text=long_of_text, text_list=text_list, id_name=product_id_name)
        new_department_df = department_data_preparation(ddf, conn_info=conn_info, table_name=department_table_name, key_column=department_key_column, long_of_text=long_of_text, text_list=text_list, id_name=department_id_name)
        
        # Upload new product data and department data to postgresql
        with create_engine(conn_url) as engine :
            logging.info(f"Uploading data to {product_table_name}...")
            new_product_df.to_sql(product_table_name, engine,  if_exists='append', index=False)
            logging.info(f"Uploaded data to {product_table_name} successfully.")
            logging.info(f"Uploading data to {department_table_name}...")
            new_department_df.to_sql(department_table_name, engine,  if_exists='append', index=False)
            logging.info(f"Uploaded data to {department_table_name} successfully.")
        
    except Exception as e :
        logging.error(f"Error in script: {e}")
        
    finally :
        client.close()
        logging.info("Ingestion completed")