import pandas as pd
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from config import logging_config

# Set base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load error messages
try:
    err_msgs_path = os.path.join(BASE_DIR, "config", "error_msgs.json")
    with open(err_msgs_path) as f:
        error_msgs = json.load(f)
except Exception as e:
    print(f"Could not load error messages: {e}")
    exit(1)

# Load environment variables
try:
    load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

    data_path = os.path.join(BASE_DIR, os.getenv("DATA_PATH"))
    tables_path = os.path.join(BASE_DIR, os.getenv("TABLES_PATH"))
    columns_path = os.path.join(BASE_DIR, os.getenv("REQ_COLUMNS_PATH"))
    output_path = os.path.join(BASE_DIR, os.getenv("OUTPUT_PATH"))
    output_format_path = os.path.join(BASE_DIR, os.getenv("OUTPUT_FORMAT_PATH"))
    log_dir = os.path.join(BASE_DIR, os.getenv("LOG_PATH"))

    log_dir = os.path.join(BASE_DIR, os.getenv("LOG_PATH", "logs"))
    logger = logging_config.setup_logger(log_dir)

except Exception as e:
    logger.error(error_msgs["env_load_error"].format(e))
    exit(1)

# Load JSON configs
try:
    with open(tables_path) as f:
        tables = json.load(f)["tables"]

    with open(columns_path) as f:
        req_cols_config = json.load(f)
    
    with open(output_format_path) as f:
        output_keys = json.load(f)
        
except Exception as e:
    logger.error(error_msgs["json_load_error"].format(e))
    exit(1)

dq_metrics = []

# Check data completeness
for table in tables:
    try:
        df = pd.read_csv(os.path.join(data_path, f"{table}.csv"))
    except FileNotFoundError:
        logger.error(error_msgs["csv_not_found"].format(table))
        continue
    except Exception as e:
        logger.error(error_msgs["csv_read_error"].format(table, e))
        continue

    req_cols_list = req_cols_config.get(table, [])
    if not req_cols_list:
        logger.warning(error_msgs["no_required_columns"].format(table))
        continue

    for col in req_cols_list:
        try:
            missing_count = df[col].isnull().sum()
            total_rows = len(df)
            dq_metrics.append({
                output_keys["table_name"]: table,
                output_keys["column_name"]: col,
                output_keys["missing_count"]: int(missing_count),
                output_keys["total_rows"]: int(total_rows),
                output_keys["missing_percentage"]: round((int(missing_count) / int(total_rows) * 100) if total_rows > 0 else 0, 2)
            })
        except KeyError:
            logger.error(error_msgs["column_not_found"].format(col, table))
        except Exception as e:
            logger.error(error_msgs["column_processing_error"].format(col, table, e))

# Save output
try:
    os.makedirs(output_path, exist_ok=True)
    current_date = datetime.today().strftime("%Y_%m_%d")
    output_file = os.path.join(output_path, f"{current_date}.json")
    with open(output_file, "w") as f:
        json.dump(dq_metrics, f, indent=4)
    logger.info(error_msgs["success_message"].format(output_file))
except Exception as e:
    logger.error(error_msgs["json_save_error"].format(e))