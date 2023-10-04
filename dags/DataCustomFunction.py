from io import StringIO
import logging
import psycopg2
import random
import pandas as pd

class functions:

    def generate_random_char_ids(unexist_row_count, text_list, long_of_text, id_name, exist_item_list):
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

class processor:
    
    def get_latest_dataframe(partition, max_create_at) :
        return partition[partition["create_at"] > max_create_at]

class fetcher:
    
    def get_max_value_from_table(conn_info, column_name, table_name):
        with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
            cur.execute(f"SELECT MAX({column_name}) FROM {table_name}")
            max_value = cur.fetchone()[0]
        return max_value

    def get_all_data_from_table(conn_info, table_name):
        with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
            logging.info(f"Fetching all data from {table_name}...")
            cur.execute(f"SELECT * FROM {table_name}")
            result = cur.fetchall()
            df = pd.DataFrame(result)
        return df

    def check_existing_table(conn_info, table_name):
        with psycopg2.connect(**conn_info) as conn, conn.cursor() as cur :
            logging.info(f"Checking for existing {table_name}...")
            cur.execute(f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name='{table_name}')")
            table_exists = bool(cur.fetchone()[0])
        return table_exists

class plubisher:

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