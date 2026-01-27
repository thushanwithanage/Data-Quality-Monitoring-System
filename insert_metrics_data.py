import json
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from config import logging_config

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment & setup logger
def load_config_and_logger():
    try:
        load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

        config = {
            "url": os.environ.get("SUPABASE_URL"),
            "api_key": os.environ.get("SUPABASE_API_KEY"),
            "output_format_path": os.path.join(BASE_DIR, os.getenv("OUTPUT_FORMAT_PATH")),
            "output_path": os.path.join(BASE_DIR, os.getenv("OUTPUT_PATH"), "2026_01_27.json"),
            "table_name": os.environ.get("TABLE_NAME")
        }

        logger = logging_config.setup_logger(os.path.join(BASE_DIR, "logs"))

        return config, logger
    except Exception as e:
        print(f"Failed to load environment or logger: {e}")
        raise

# Load JSON files
def load_json_files(config, logger):
    try:
        with open(config["output_path"]) as f:
            records = json.load(f)
        with open(config["output_format_path"]) as f:
            output_keys = json.load(f)

        return records, output_keys
    except Exception as e:
        logger.error(f"Failed to load JSON files: {e}")
        raise

# Create Supabase client
def create_supabase_client(config, logger) -> Client:
    try:
        client = create_client(config["url"], config["api_key"])
        return client
    except Exception as e:
        logger.error(f"Failed to create Supabase client: {e}")
        raise

# Insert records into Supabase (batch)
def insert_metrics(supabase: Client, table_name: str, records: list, output_keys: dict, logger):
    try:
        rows_to_insert = [
            {
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
            logger.info(f"Inserted {len(response.data)} rows successfully")
        else:
            logger.error(f"Failed to insert data: {response.error}")

    except KeyError as e:
        logger.error(f"Missing expected key in record: {e}")
    except Exception as e:
        logger.error(f"Error inserting metrics: {e}")

def main():
    config, logger = load_config_and_logger()
    records, output_keys = load_json_files(config, logger)
    supabase = create_supabase_client(config, logger)
    insert_metrics(supabase, config["table_name"], records, output_keys, logger)

if __name__ == "__main__":
    main()
