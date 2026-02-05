import pytest
import pandas as pd
from completeness_check import build_metric_record

def test_build_metric_record():
    record = build_metric_record(
        pipeline_name="test",
        run_timestamp="2026-02-05 12:00:00",
        metric_date="2026-02-05",
        table="table1",
        column="col1",
        total_rows=10,
        missing_rows=2
    )
    assert record.missing_percentage == 20.0
