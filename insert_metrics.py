import os
from supabase import Client, create_client
from config.bootstrap import write_to_json

# Save records to JSON file
def save_json_output(records: list, output_path: str, logger, error_msgs):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        write_to_json(output_path, records)
        logger.info(error_msgs["success_message"].format(output_path))
    except Exception as e:
        logger.error(error_msgs["json_save_error"].format(e))
        raise

# Create Supabase client
def create_supabase_client(url, api_key, logger, error_msgs) -> Client:
    try:
        client = create_client(url, api_key)
        return client
    except Exception as e:
        logger.error(error_msgs["supabase_client_error"].format(e))
        raise

# Batch insert records into Supabase database
def insert_metrics_db(supabase: Client, table_name: str, records: list, logger, error_msgs):
    if len(records) == 0:
        logger.error(error_msgs["no_records"])
        return
    try:
        response = supabase.table(table_name).upsert(records).execute()
        
        if response.data:
            logger.info(error_msgs["db_success_message"].format(len(response.data), table_name))
        else:
            logger.error(error_msgs["db_insert_error"].format(table_name))

    except KeyError as e:
        logger.error(error_msgs["key_error"].format(e))

    except Exception as e:
        logger.error(error_msgs["db_insert_error"].format(e))