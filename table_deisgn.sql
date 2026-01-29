CREATE TABLE dq_metrics (
    id BIGSERIAL PRIMARY KEY,

    pipeline_name TEXT NOT NULL,
    run_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    metric_date DATE NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,

    total_rows INTEGER NOT NULL,
    missing_rows INTEGER NOT NULL,
    missing_percentage NUMERIC(5,2) NOT NULL
)

-- Stops duplicate records when the pipeline is re-run multiple times within the same day
CREATE UNIQUE INDEX uq_dq_metrics
ON dq_metrics (
    metric_date,
    table_name,
    column_name,
    pipeline_name
)