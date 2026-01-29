import json
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from config import logging_config
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load error messages
def load_error_messages():
    try:
        path = os.path.join(BASE_DIR, "config", "error_msgs.json")
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load error messages: {e}")
        raise

# Load environment & setup logger
def load_config_and_logger(error_msgs):
    try:
        load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

        config = {
            "url": os.environ.get("SUPABASE_URL"),
            "api_key": os.environ.get("SUPABASE_API_KEY"),
            "output_format_path": os.path.join(BASE_DIR, os.getenv("OUTPUT_FORMAT_PATH")),
            "output_path": os.path.join(BASE_DIR, os.getenv("OUTPUT_PATH"), str(datetime.now().strftime("%Y-%m-%d")) + ".json"),
            "table_name": os.environ.get("TABLE_NAME")
        }

        logger = logging_config.setup_logger(os.path.join(BASE_DIR, "logs"))

        return config, logger
    except Exception as e:
        print(error_msgs["env_load_error"].format(e))
        raise

# Load JSON files
def load_json_files(config, logger, error_msgs):
    try:
        with open(config["output_path"]) as f:
            records = json.load(f)
        with open(config["output_format_path"]) as f:
            output_keys = json.load(f)

        return records, output_keys
    except Exception as e:
        logger.error(error_msgs["json_load_error"].format(e))
        raise

# Create Supabase client
def create_supabase_client(config, logger, error_msgs) -> Client:
    try:
        client = create_client(config["url"], config["api_key"])
        return client
    except Exception as e:
        logger.error(error_msgs["supabase_client_error"].format(e))
        raise

# Insert records into Supabase (batch)
def insert_metrics(supabase: Client, table_name: str, records: list, output_keys: dict, logger, error_msgs):
    try:
        rows_to_insert = [
            {
                output_keys["pipeline_name"]: r["pipeline_name"],
                output_keys["run_timestamp"]: r["run_timestamp"],
                output_keys["metric_date"]: r["metric_date"],
                output_keys["table_name"]: r["table_name"],
                output_keys["column_name"]: r["column_name"],
                output_keys["missing_rows"]: r["missing_rows"],
                output_keys["total_rows"]: r["total_rows"],
                output_keys["missing_percentage"]: r["missing_percentage"]
            }
            for r in records
        ]

        response = supabase.table(table_name).insert(rows_to_insert).execute()

        if response.data:
            logger.info(error_msgs["db_success_message"].format(len(response.data), table_name))
        else:
            logger.error(error_msgs["db_insert_error"].format(table_name))

    except KeyError as e:
        logger.error(error_msgs["key_error"].format(e))

    except Exception as e:
        logger.error(error_msgs["db_insert_error"].format(e))

def main():
    error_msgs = load_error_messages()
    config, logger = load_config_and_logger(error_msgs)
    records, output_keys = load_json_files(config, logger, error_msgs)
    supabase = create_supabase_client(config, logger, error_msgs)
    insert_metrics(supabase, config["table_name"], records, output_keys, logger, error_msgs)

if __name__ == "__main__":
    main()