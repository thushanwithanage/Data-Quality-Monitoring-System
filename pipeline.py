from completeness_check import main as run_dq
from insert_metrics import create_supabase_client, insert_metrics_db, save_json_output
from config.bootstrap import load_pipeline_config, get_error_messages, get_output_path, setup_logger, setup_env
from dataclasses import asdict

def main(save_json: bool, save_db: bool = True):
    setup_env()
    logger = setup_logger()
    error_msgs = get_error_messages()

    config = load_pipeline_config(error_msgs)

    # Run DQ metrics
    dq_metrics = run_dq(config, logger, error_msgs)
    records = [asdict(m) for m in dq_metrics]

    if save_json:
        output_file = get_output_path(
            base_output_dir=config["paths"]["output"],
            metric_name=config["pipeline"]["metric_name"],
            dated=True
        )
        save_json_output(records, output_file, logger, error_msgs)
    
    if save_db:
        supabase = create_supabase_client(config["supabase"]["url"], config["supabase"]["api_key"], logger, error_msgs)
        insert_metrics_db(supabase, config["supabase"]["table_name"], records, logger, error_msgs)

if __name__ == "__main__":
    main(save_json=True, save_db=True)