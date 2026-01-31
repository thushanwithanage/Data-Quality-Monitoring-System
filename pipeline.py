from completeness_check import main as run_dq
from insert_metrics import create_supabase_client, insert_metrics_db, save_json_output
from config.bootstrap import get_path, get_env_variable, get_error_messages, get_output_path, setup_logger, setup_env
from dataclasses import asdict

def main(save_json: bool, save_db: bool = True):
    setup_env()
    logger = setup_logger()
    error_msgs = get_error_messages()

    # Run DQ metrics
    dq_metrics = run_dq(logger, error_msgs)
    records = [asdict(m) for m in dq_metrics]

    if save_json:
        output_file = get_output_path(
            base_output_dir=get_path(get_env_variable("OUTPUT_PATH", error_msgs["output_path_not_found"])),
            metric_name=get_env_variable("METRIC_NAME", error_msgs["metric_name_not_found"]),
            dated=True
        )
        save_json_output(records, output_file, logger, error_msgs)
    
    if save_db:
        config = {
            "url": get_env_variable("SUPABASE_URL", error_msgs["supabase_url_not_found"]),
            "api_key": get_env_variable("SUPABASE_API_KEY", error_msgs["supabase_api_key_not_found"]),
            "table_name": get_env_variable("TABLE_NAME", error_msgs["table_name_not_found"])
        }

        supabase = create_supabase_client(config["url"], config["api_key"], logger, error_msgs)
        insert_metrics_db(supabase, config["table_name"], records, logger, error_msgs)

if __name__ == "__main__":
    main(save_json=True, save_db=True)