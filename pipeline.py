from completeness_check import main as run_dq
from insert_metrics import create_supabase_client, insert_metrics
from config.bootstrap import get_path, get_env_variable, get_error_messages, get_output_path, setup_logger, setup_env
import os

def main(save_json: bool, output_file_path: str = None):
    setup_env()
    logger = setup_logger()
    error_msgs = get_error_messages()

    if output_file_path is None:
        output_file = get_output_path(
            base_output_dir=get_path(get_env_variable("OUTPUT_PATH", error_msgs["output_path_not_found"])),
            metric_name=get_env_variable("METRIC_NAME", error_msgs["metric_name_not_found"]),
            dated=True
        )
    
    # Run DQ metrics
    dq_metrics, _ = run_dq(persist_output=save_json)
    #dq_metrics, _ = run_dq(persist_output=save_json, output_file_path=output_file)

    # Supabase config
    for key in ["SUPABASE_URL", "SUPABASE_API_KEY", "TABLE_NAME"]:
        if not os.environ.get(key):
            raise EnvironmentError()

    config = {
        "url": os.environ.get("SUPABASE_URL"),
        "api_key": os.environ.get("SUPABASE_API_KEY"),
        "table_name": os.environ.get("TABLE_NAME")
    }

    supabase = create_supabase_client(config["url"], config["api_key"], logger, error_msgs)
    insert_metrics(supabase, config["table_name"], dq_metrics, logger, error_msgs)

if __name__ == "__main__":
    main(save_json=True)