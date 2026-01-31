from datetime import datetime
import os
import json
import pandas as pd
from dotenv import load_dotenv
from config import logging_config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
# Setup environment variables
def setup_env():
    load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

def get_error_messages() -> dict:
    path = os.path.join(BASE_DIR, "config", "error_msgs.json")
    with open(path) as f:
        return json.load(f)

def setup_logger(log_subdir: str = "logs"):
    log_dir = os.path.join(BASE_DIR, log_subdir)
    logger = logging_config.setup_logger(log_dir)
    return logger

def get_current_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def get_current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Generate output file path
def get_output_path(base_output_dir: str, metric_name: str, dated: bool) -> str:
    if dated:
        date_str = get_current_date()
        base_output_dir = os.path.join(base_output_dir, date_str)

    os.makedirs(base_output_dir, exist_ok=True)

    if metric_name:
        return os.path.join(base_output_dir, metric_name)
    return base_output_dir

# Read JSON file
def get_json_config(config: str) -> dict:
    try:
        with open(config) as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError("json_load_error: {}".format(e)) 

# Get CSV file as DataFrame
def read_csv_file(data_path: str, table: str) -> pd.DataFrame:
    df = pd.read_csv(os.path.join(data_path, f"{table}.csv"), na_values=["", "NULL", "null"])
    return df, len(df)

# Get environment variable
def get_env_variable(var_name: str, error_msg: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(error_msg.format(var_name))
    return value

# Get file path
def get_path(env_var: str) -> str:
    return os.path.join(BASE_DIR, env_var)

# Wrtite data to JSON file
def write_to_json(output_file, dq_metrics, indent:int =4):
    with open(output_file, "w") as f:
        json.dump(dq_metrics, f, indent=indent)