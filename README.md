# Automated ETL Pipeline with Apache Airflow and Great Expectations

This repository contains a containerized, production-grade batch ETL pipeline orchestrated by **Apache Airflow** and validated at each stage using **Great Expectations (GX)**. The pipeline ingests transactional e-commerce data, processes it into Bronze (raw parquet) and Silver (refined analytical) layers, runs rigorous schema and business validations (acting as circuit breakers), and loads the final aggregated data into a **SQLite** database serving layer.

---
## Directory Structure

```directory
.
├── dags/
│   └── ecommerce_analytics_pipeline.py    # Airflow DAG defining orchestration & task sequence
├── etl_scripts/
│   ├── __init__.py                        # Exposes the ETL tasks
│   ├── extractor.py                       # Downloads external CSV data
│   ├── bronze_processor.py                # Parses CSV, enforces dtypes, writes partitioned Parquet
│   ├── silver_transformer.py              # CustomerID imputation, TotalPrice, filtering, and aggregation
│   ├── validator.py                       # Great Expectations checkpoint executor & circuit breaker
│   └── loader.py                          # Idempotent load to SQLite analytics database
├── great_expectations/
│   ├── great_expectations.yml             # Main GX project config
│   ├── expectations/
│   │   ├── bronze_suite.json              # Data quality schema checks for Bronze layer
│   │   └── silver_suite.json              # Business rules checks for Silver layer
│   └── checkpoints/
│       ├── bronze_checkpoint.yml          # Checkpoint configuration for Bronze
│       └── silver_checkpoint.yml          # Checkpoint configuration for Silver
├── tests/
│   ├── __init__.py
│   └── test_silver_transformer.py         # Pytest suite validating silver transformations
├── Dockerfile.etl                         # Custom image with Python 3.9, Airflow, Pandas, PyArrow, GX, Pytest
├── docker-compose.yml                     # Orchestration for Airflow services, Postgres metadata, and etl-service
├── .env.example                           # Template for required environment variables
├── .gitignore                             # Prevents tracking of secrets and run-time binaries/databases
├── verify_project.py                      # Local testing/verification script (DAG syntax & Pytest checks)
└── README.md                              # Main documentation (this file)
```

---

## Requirements Met

1. **Containerization**: Entire Airflow system (Webserver, Scheduler, Postgres Metadata DB) and the custom CLI `etl-service` containerized.
2. **Strict DAG Orchestration**: DAG `ecommerce_analytics_pipeline` running on a daily schedule, executing tasks in the exact sequence:
   `download_raw_data -> bronze_layer_processing -> bronze_data_validation -> silver_layer_transformation -> silver_data_validation -> load_to_analytical_store`
3. **Robust Download**: `download_raw_data` throws explicit errors on HTTP failures (e.g. 404 Not Found) to fail the task immediately.
4. **Bronze Layer Partitioning**: CSV processed to Parquet and physically partitioned in `data/bronze/` by `Year` and `Month`.
5. **Bronze Data Quality**: Great Expectations checks for existence of all raw columns, non-null `InvoiceNo`/`CustomerID`, correct datatypes, and `Quantity > 0`.
6. **Silver Business Logic**: Imputes `CustomerID` with `UNKNOWN`, filters out negative quantities/prices, calculates `TotalPrice = Quantity * UnitPrice`, and groups by daily date and country to compute `DailyTotalSales`.
7. **Silver Layer Partitioning**: Silver Parquet partitioned in `data/silver/` by `Year` and `Month`.
8. **Silver Data Quality**: Checkpoint validates existence of `InvoiceDate`, `Country`, `DailyTotalSales`, non-null sales, uniqueness across the composite key `(InvoiceDate, Country)`, positive sales, and row counts.
9. **Analytical Serving Store**: Refined data loaded into SQLite database at `data/analytics.db` in the table `daily_country_sales` using an idempotent `replace` strategy.
10. **Circuit Breakers**: Failure of any Great Expectations checkpoint causes the validation task to raise an exception and exit with a non-zero code, preventing dirty data from propagating downstream.
11. **Modularity & Instrumentability**: ETL tasks are imported from the `etl_scripts/` module. All operations utilize Python's standard `logging` module to emit timestamps, logs, and tracebacks.
12. **Isolated Transformation Testing**: Pytest suite covering CustomerID imputation, filtering, and sales aggregation arithmetic.

---

## Setup & Running Instructions

### 1. Prerequisites
Ensure you have Docker and Docker Compose installed on your system.

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and adjust the variables if necessary:
```bash
cp .env.example .env
```

### 3. Spin Up the Container Cluster
Build and start all services in detached mode:
```bash
docker compose up -d --build
```
This command starts:
- **postgres**: Metastore database for Airflow.
- **airflow-init**: Initializes the Airflow database and creates a default Admin user (`username: admin, password: admin`).
- **airflow-webserver**: Serves the Web UI on `http://localhost:8080`.
- **airflow-scheduler**: Schedules the DAG runs.
- **etl-service**: Custom development and testing container containing all tools (Python, Pytest, Great Expectations, Pandas, PyArrow).

### 4. Running the Unit Tests
You can run and execute tests in two ways:

*   **Via Docker Container (Production Simulation)**:
    ```bash
    docker compose exec etl-service pytest tests/
    ```
*   **Offline / Locally on Host (Fast Check)**:
    Runs static file scans, DAG syntax verification, and execution of the unit test suite:
    ```bash
    python verify_project.py
    ```

### 5. Accessing Airflow & Running the DAG
1. Open your browser and navigate to `http://localhost:8080`.
2. Login with credentials `admin` / `admin`.
3. Locate the `ecommerce_analytics_pipeline` DAG and unpause it.
4. Trigger the DAG manually to see it execute the tasks sequentially.

### 6. Verifying Serving Layer Output
Once the DAG completes, you can inspect the SQLite database outputs inside the container:
```bash
docker compose exec etl-service sqlite3 data/analytics.db "SELECT * FROM daily_country_sales LIMIT 10;"
```

---

## Data Quality Validations & Circuit Breakers

The pipeline implements **Great Expectations** checkpoints at each stage to protect the integrity of downstream processes:

*   **Bronze Validation**: Enforces standard column availability and type constraints. If the raw dataset contains invalid entries (like a negative quantity in the raw files), the `bronze_data_validation` task fails, preventing the task from executing further downstream processing.
*   **Silver Validation**: Enforces business logic requirements. It verifies that the composite key `(InvoiceDate, Country)` is completely unique and that `DailyTotalSales` contains no nulls and is greater than or equal to zero.
*   **Data Docs Generation**: Each validation automatically compiles the results into a static HTML report. You can review the visual quality gates by opening `great_expectations/uncommitted/data_docs/local_site/index.html` in your browser.
