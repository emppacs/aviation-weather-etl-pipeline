# HIAA Geomet Hourly Climate Data Pipeline

## Overview
This production-grade pipeline automates the ingestion of hourly climate data for Halifax Stanfield International Airport (YHZ) from the Environment Canada Geomet API.

Designed for reliability and maintainability, the system extracts, transforms, and loads (ETL) meteorological data into a local SQLite database, following industry best practices for data engineering and Linux-based automation.

## How It Works
The pipeline follows a simple ETL approach:
1. **Extract:** 
    - Find the latest available date, then fetch all 24 hours of data for that date
    - Filters records for climate station 8202251

2. **Transform:**
    - Selects only the relevant columns that match the `hiaa_geomet_hourly` table schema
    - Converts API column names to lowercase (to match the database schema)
    - Converts numeric NULL/NA values to 0
    - Converts numeric fields to proper data types
    - Adds an insert_time column with the current UTC timestamp

3. **Load:** Appends the cleaned data into the `hiaa_geomet_hourly` table in the SQLite database
    - Uses INSERT or IGNORE logic combined with a UNIQUE constraint on the timestamp to prevent duplicate records.

4. **Validation:** Unit tests ensure the pipeline logic is correct and data transformations are accurate

## Requirements
- Linux OS
- Python 3.10+
- Internet Connectivity to access the Geomet API


## Setup & Installation
It is recommended to use a virtual environment to ensure dependency isolation:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## How to Run
1. Run Unit Tests: to ensure everything is working correctly before executing the pipeline
```bash
pytest test/test_pipeline.py
```

2. Run the Pipeline: to execute the ETL process and load data into the database
```bash
python src/pipeline.py
```
*Logs will display progress and status.*


## Scheduling in Production
To run this automatically on a schedule, set up a cron job on your Linux server. For example, to run the pipeline every day at 6am:
```bash
0 6 * * * /path/to/venv/bin/python /path/to/src/pipeline.py
```
*Make sure to replace `/path/to/pipeline.py` with the actual path to your pipeline script.*


## Project Structure
```text
aviation-weather-etl-pipeline/
├── src/
│   └── pipeline.py         # Main ETL logic
├── test/
│   └── test_pipeline.py    # Unit tests (Pytest)
├── data/
│   └── yhz_db.sqlite       # Local database storage
├── requirements.txt        # Project dependencies
├── .gitignore              # Project exclusions
└── README.md
```


## Database
The pipeline loads data into an existing SQLite database:

- Database file: `data/yhz_db.sqlite`
- Table: `hiaa_geomet_hourly`

The table schema is assumed to already exist and match the transformed dataset.


## Production Design Principles

This pipeline was built with production engineering principles in mind:
- **Automation:** Designed for scheduled, unattended execution
- **Reproducibility:** Dependencies managed via requirements.txt
- **Portability:** Runs on standard Linux environments
- **Maintainability:** Clear ETL structure (Extract → Transform → Load)
- **Observability:** Logging used to monitor pipeline execution
- **Reliability:** Error handling prevents silent failures