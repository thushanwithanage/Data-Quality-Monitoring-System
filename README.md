# 📊 Data Quality Monitoring System – Validation & Reporting Module

A comprehensive Python based data quality monitoring system that automatically validates CSV data files, generates completeness metrics, stores results in JSON and Supabase, and visualizes insights using Power BI dashboards (Daily and Monthly views).

---

## 📋 Overview

This repository contains the Validation and Reporting module of a Data Quality Monitoring System.
It performs automated completeness checks, tracks missing values at column and table levels, and enables data quality monitoring over time through interactive Power BI reports.

The system supports:
- Ongoing data quality validation
- SLA monitoring
- Daily and monthly trend analysis

---

## ✨ Key Features

### Data Quality Validation
- Automated completeness checks on configured tables
- Column-level missing value detection
- Severity classification (Low / Medium / High)

### Flexible Storage
- JSON output for file-based inspection
- Supabase database storage for analytics and reporting
- Upsert based metric persistence

### Configurable Pipeline
- JSON-based configuration for tables and required columns
- Environment driven setup using `.env`

### Reporting and Visualization
- Power BI dashboards for daily and monthly data quality monitoring
- SLA compliance tracking
- Trend and impact analysis

### Observability
- Structured logging
- Pipeline execution summaries
- Clear error diagnostics

---

## 🏗️ Architecture Overview

CSV Files  
↓  
Validation Pipeline (Python)  
↓  
Completeness Metrics  
↓  
JSON Output ↔ Supabase Database  
↓  
Power BI Dashboards

---

## 🔧 Core Components

### Pipeline (`pipeline.py`)
- Loads configuration and environment variables
- Executes completeness checks
- Controls JSON and database outputs
- Generates pipeline execution summaries

### Completeness Check (`completeness_check.py`)
- Reads CSV files from configured paths
- Validates required columns per table
- Counts total rows and missing values
- Calculates missing percentages
- Produces structured `DQMetric` records

### Metrics Storage (`insert_metrics.py`)
- Saves metrics to timestamped JSON files
- Upserts metrics into Supabase
- Manages Supabase client connection

---

## ⚙️ Configuration Files

### tables.json
Defines source data files:

{
  "tables": [
    { "name": "table_name", "path": "relative/path/to/file.csv" }
  ]
}

### required_columns.json
Defines required columns per table:

{
  "table_name": ["column1", "column2", "column3"]
}

---

## 🚀 Getting Started

### Prerequisites
- Python 3.8 or higher
- pandas
- python-dotenv
- supabase-py
- Power BI Desktop

### Installation

1. Clone the repository
2. Install dependencies:
   pip install pandas python-dotenv supabase

3. Configure environment variables in config/.env
4. Update tables.json and required_columns.json

---

## ▶️ Usage

Run the pipeline:
python pipeline.py

---

## 📊 Output

JSON files are stored in:
output/YYYY-MM-DD/dq_metrics.json

Supabase database stores the same schema for reporting and analytics.

---

## 📈 Power BI Dashboards

### Daily Data Quality Report
- Snapshot of data quality for a selected date
- Missing values, severity, and table impact
- Pipeline runtime visibility

### Monthly Data Quality Report
- Trend analysis across the month
- SLA compliance by table
- Top columns impacting SLA breaches
- Comparison with previous periods

---

## 🧪 Testing

Run unit tests:
python -m pytest tests/test_completeness_check.py -v

---

## 👤 Author

Thushan Withanage

Last Updated: February 2026