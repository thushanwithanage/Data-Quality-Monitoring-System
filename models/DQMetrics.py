from dataclasses import dataclass

@dataclass
class DQMetric:
    pipeline_name: str
    run_timestamp: str
    metric_date: str
    table_name: str
    column_name: str
    total_rows: int
    missing_rows: int
    missing_percentage: float
