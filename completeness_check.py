import json
import os
from config.bootstrap import get_current_date, get_current_timestamp, get_error_messages, get_output_path, setup_logger, setup_env, get_path, get_json_config, read_csv_file, get_env_variable, write_to_json

# Create metric record
def build_metric_record(
    output_keys: dict,
    pipeline_name: str,
    run_timestamp: str,
    metric_date: str,
    table: str,
    column: str,
    total_rows: int,
    missing_rows: int,
) -> dict:

    return {
        output_keys["pipeline_name"]: pipeline_name,
        output_keys["run_timestamp"]: run_timestamp,
        output_keys["metric_date"]: metric_date,
        output_keys["table_name"]: table,
        output_keys["column_name"]: column,
        output_keys["total_rows"]: int(total_rows),
        output_keys["missing_rows"]: int(missing_rows),
        output_keys["missing_percentage"]: round(
            (missing_rows / total_rows * 100) if total_rows > 0 else 0, 2
        )
    }

# Run data quality checks
def run_data_quality_checks(tables, req_cols_config, output_keys, data_path, logger, error_msgs, pipeline_name):
    
    dq_metrics = []

    for table in tables:
        try:
            df, total_rows = read_csv_file(data_path, table)

            if df.empty:
                logger.error(error_msgs["table_empty"].format(table))
                continue

        except FileNotFoundError:
            logger.error(error_msgs["csv_not_found"].format(table))
            continue
        except Exception as e:
            logger.error(error_msgs["csv_read_error"].format(table, e))
            continue

        req_cols_list = req_cols_config.get(table, [])
        if not req_cols_list:
            logger.error(error_msgs["no_required_columns"].format(table))
            continue

        run_ts = get_current_timestamp()
        metric_date = get_current_date()

        null_counts = df.isnull().sum()

        for col in req_cols_list:
            try:
                if col not in df.columns:
                    logger.error(error_msgs["column_not_found"].format(col, table))
                    continue

                #missing_rows = int(df[col].isnull().sum())
                missing_rows = int(null_counts[col])

                metric = build_metric_record(
                    output_keys=output_keys,
                    pipeline_name=pipeline_name,
                    run_timestamp=run_ts,
                    metric_date=metric_date,
                    table=table,
                    column=col,
                    total_rows=total_rows,
                    missing_rows=missing_rows
                )

                dq_metrics.append(metric)

            except KeyError:
                logger.error(error_msgs["column_not_found"].format(col, table))
            except Exception as e:
                logger.error(error_msgs["column_processing_error"].format(col, table, e))

    return dq_metrics

# Save output
def save_output(dq_metrics, output_path, logger, error_msgs, enabled: bool):
    if not enabled:
        return
    try:
        os.makedirs(output_path, exist_ok=True)
        output_file = get_output_path(
            base_output_dir=get_path(get_env_variable("OUTPUT_PATH", error_msgs["output_path_not_found"])),
            metric_name=get_env_variable("METRIC_NAME", error_msgs["metric_name_not_found"]),
            dated=True
        )

        write_to_json(output_file, dq_metrics)
        logger.info(error_msgs["success_message"].format(output_file))

    except Exception as e:
        logger.error(error_msgs["json_save_error"].format(e))
        raise

def main(persist_output: bool):
    setup_env()
    logger = setup_logger()
    error_msgs = get_error_messages()

    config = {
        "data_path": get_path(get_env_variable("DATA_PATH", error_msgs["data_path_not_found"])),
        "tables_path": get_path(get_env_variable("TABLES_PATH", error_msgs["tables_path_not_found"])),
        "columns_path": get_path(get_env_variable("REQ_COLUMNS_PATH", error_msgs["columns_path_not_found"])),
        "output_path": get_path(get_env_variable("OUTPUT_PATH", error_msgs["output_path_not_found"])),
        "output_format_path": get_path(get_env_variable("OUTPUT_FORMAT_PATH", error_msgs["output_format_not_found"])),
        "pipeline_name": get_env_variable("PIPELINE_NAME", error_msgs["pipeline_name_not_found"])
    }

    tables = get_json_config(config["tables_path"])["tables"]
    req_cols_config = get_json_config(config["columns_path"])
    output_keys = get_json_config(config["output_format_path"])

    dq_metrics = run_data_quality_checks(
        tables,
        req_cols_config,
        output_keys,
        config["data_path"],
        logger,
        error_msgs,
        config["pipeline_name"]
    )

    save_output(dq_metrics, config["output_path"], logger, error_msgs, enabled=persist_output)

    return dq_metrics, output_keys

if __name__ == "__main__":
    main(persist_output=True)