from dataclasses import dataclass

@dataclass
class PipelineSummary:
    pipeline: str
    tables_processed: int
    metrics_generated: int
    json_saved: bool
    db_saved: bool