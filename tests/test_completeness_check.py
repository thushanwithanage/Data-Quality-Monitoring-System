import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, Mock
from completeness_check import build_metric_record, run_data_quality_checks

# Fixtures
@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def basic_config():
    return {
        "tables": ["users"],
        "req_cols": {"users": ["email"]},
        "data_path": "/fake/path",
        "error_msgs": {
            "table_empty": "Table {} is empty",
            "csv_not_found": "File {} not found",
            "column_not_found": "Column {} not found in {}"
        },
        "pipeline_name": "test_pipe"
    }

def test_build_metric_record_div_by_zero():
    record = build_metric_record(
        pipeline_name="test",
        run_timestamp="2026-02-05 12:00:00",
        metric_date="2026-02-05",
        table="table1",
        column="col1",
        total_rows=0,
        missing_rows=0
    )
    assert record.missing_percentage == 0.0

def test_build_metric_record_output_rounding():
    record = build_metric_record(
        pipeline_name="test",
        run_timestamp="2026-02-05 12:00:00",
        metric_date="2026-02-05",
        table="table1",
        column="col1",
        total_rows=13,
        missing_rows=3
    )
    assert record.missing_percentage == 23.08

def test_run_data_quality_checks_csv_not_found():
    with patch("completeness_check.read_csv_file", side_effect=FileNotFoundError):
        logger = Mock()
        error_msgs = {"csv_not_found": "{} not found"}
        
        result = run_data_quality_checks(
            tables=["users"],
            req_cols_config={"users": ["email"]},
            data_path="/data",
            logger=logger,
            error_msgs=error_msgs,
            pipeline_name="dq_pipeline"
        )

    assert result == []
    logger.error.assert_called_once()

@patch("completeness_check.read_csv_file")
@patch("completeness_check.get_current_timestamp", return_value="2024-01-01 12:00:00")
@patch("completeness_check.get_current_date", return_value="2024-01-01")
def test_run_checks_file_not_found(mock_date, mock_ts, mock_read_csv, mock_logger, basic_config):
    mock_read_csv.side_effect = FileNotFoundError("File missing")

    results = run_data_quality_checks(
        tables=basic_config["tables"],
        req_cols_config=basic_config["req_cols"],
        data_path=basic_config["data_path"],
        logger=mock_logger,
        error_msgs=basic_config["error_msgs"],
        pipeline_name=basic_config["pipeline_name"]
    )

    assert len(results) == 0
    mock_logger.error.assert_called_once()
    args, _ = mock_logger.error.call_args
    assert "File users not found" in args[0]

@patch("completeness_check.read_csv_file")
def test_run_checks_missing_column(mock_read_csv, mock_logger, basic_config):   
    df_mock = pd.DataFrame({"id": [1, 2, 3]})
    mock_read_csv.return_value = (df_mock, 3)

    results = run_data_quality_checks(
        tables=basic_config["tables"],
        req_cols_config=basic_config["req_cols"],
        data_path=basic_config["data_path"],
        logger=mock_logger,
        error_msgs=basic_config["error_msgs"],
        pipeline_name=basic_config["pipeline_name"]
    )

    assert len(results) == 0
    mock_logger.error.assert_called()
    assert "Column email not found" in mock_logger.error.call_args[0][0]