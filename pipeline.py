from completeness_check import main as run_dq
from insert_metrics import create_supabase_client, insert_metrics_db, save_json_output
from config.bootstrap import load_pipeline_config, get_error_messages, get_output_path, setup_logger, setup_env, get_json_config
from dataclasses import asdict
from models.PipelineSummary import PipelineSummary

def main(save_json: bool, save_db: bool = True):
    setup_env()
    logger = setup_logger()
    error_msgs = get_error_messages()

    config = load_pipeline_config(error_msgs)

    tables = get_json_config(config["paths"]["tables"])
    req_cols = get_json_config(config["paths"]["columns"])

    total_tables = len(tables.get("tables", []))

    # Run DQ metrics
    dq_metrics = run_dq(config, tables, req_cols, logger, error_msgs)
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
        total_metrics = insert_metrics_db(supabase, config["supabase"]["table_name"], records, logger, error_msgs)

    summary = PipelineSummary(
        pipeline=config["pipeline"]["name"],
        tables_processed=total_tables, 
        metrics_generated=total_metrics,
        json_saved=save_json,
        db_saved=True if total_metrics > 0 else False
    )

    logger.info(format_pipeline_summary(summary))

def format_pipeline_summary(summary: PipelineSummary) -> str:
    data = asdict(summary)
    parts = [f"{k.replace('_', ' ').title()}: {v}" for k, v in data.items()]
    return "Pipeline Summary | " + " | ".join(parts)

if __name__ == "__main__":
    main(save_json=False, save_db=True)