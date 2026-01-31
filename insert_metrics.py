import os
from supabase import create_client, Client
from config.bootstrap import get_env_variable, get_error_messages, get_output_path, get_path, setup_env, setup_logger, get_json_config

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Create Supabase client
def create_supabase_client(url, api_key, logger, error_msgs) -> Client:
    try:
        client = create_client(url, api_key)
        return client
    except Exception as e:
        logger.error(error_msgs["supabase_client_error"].format(e))
        raise

# Batch insert records into Supabase database
def insert_metrics(supabase: Client, table_name: str, records: list, logger, error_msgs):
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

def main():
    setup_env()
    logger = setup_logger()
    error_msgs = get_error_messages()

    config = {
        "url": get_env_variable("SUPABASE_URL", error_msgs["supabase_url_not_found"]),
        "api_key": get_env_variable("SUPABASE_API_KEY", error_msgs["supabase_api_key_not_found"]),
        "table_name": get_env_variable("TABLE_NAME", error_msgs["table_name_not_found"])
    }

    output_file = get_output_path(
        base_output_dir=get_path(get_env_variable("OUTPUT_PATH", error_msgs["output_path_not_found"])),
        metric_name=get_env_variable("METRIC_NAME", error_msgs["metric_name_not_found"]),
        dated=True
    )

    records = get_json_config(output_file)

    supabase = create_supabase_client(config["url"], config["api_key"], logger, error_msgs)
    insert_metrics(supabase, config["table_name"], records, logger, error_msgs)

if __name__ == "__main__":
    main()