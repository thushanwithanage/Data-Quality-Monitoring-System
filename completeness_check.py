from typing import List
from config.bootstrap import get_current_date, get_current_timestamp, read_csv_file
from models.DQMetrics import DQMetric

# Create metric record
def build_metric_record(
    pipeline_name: str,
    run_timestamp: str,
    metric_date: str,
    table: str,
    column: str,
    total_rows: int,
    missing_rows: int,
) -> DQMetric:

    return DQMetric(
        pipeline_name=pipeline_name,
        run_timestamp=run_timestamp,
        metric_date=metric_date,
        table_name=table,
        column_name=column,
        total_rows=total_rows,
        missing_rows=missing_rows,
        missing_percentage=round(
            (missing_rows / total_rows * 100) if total_rows > 0 else 0, 2
        ),
    )

# Run data quality checks
def run_data_quality_checks(
        tables: list[str], 
        req_cols_config: dict[str, list[str]], 
        data_path: str, 
        logger, 
        error_msgs: dict, 
        pipeline_name: str
    ) -> List[DQMetric]:
    
    dq_metrics: list[DQMetric] = []

    run_timestamp = get_current_timestamp()
    metric_date = get_current_date()
    
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

        null_counts = df.isnull().sum()

        for column in req_cols_list:
            try:
                if column not in df.columns:
                    logger.error(error_msgs["column_not_found"].format(column, table))
                    continue

                missing_rows = int(null_counts[column])

                metric = build_metric_record(
                    pipeline_name=pipeline_name,
                    run_timestamp=run_timestamp,
                    metric_date=metric_date,
                    table=table,
                    column=column,
                    total_rows=total_rows,
                    missing_rows=missing_rows
                )

                dq_metrics.append(metric)

            except KeyError:
                logger.error(error_msgs["column_not_found"].format(column, table))
            except Exception as e:
                logger.error(error_msgs["column_processing_error"].format(column, table, e))

    return dq_metrics

def main(config: dict, tables: dict, req_cols: dict, logger, error_msgs) -> List[DQMetric]:

    tables = tables.get("tables", [])

    if not tables:
        logger.error(error_msgs["no_tables_defined"])
        return []

    dq_metrics = run_data_quality_checks(
        tables,
        req_cols,
        config["paths"]["data"],
        logger,
        error_msgs,
        config["pipeline"]["name"]
    )

    return dq_metrics

if __name__ == "__main__":
    main()