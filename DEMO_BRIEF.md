# Demo Project Brief — Booking-style Hadoop → Snowflake Migration Pipeline

## Context for Claude

This is a learning-focused demo of a data migration pipeline. The goal is to simulate
moving hotel booking data from a Hadoop-based system (represented as local Parquet files)
to Snowflake, using PySpark on AWS EMR Serverless for transformation, AWS S3 as an
intermediate data lake, and Apache Airflow (Docker) for orchestration.

The code must be simple, well-commented for learning purposes, and every component
must actually run and produce visible output. This is not production code — prioritise
clarity over optimisation.

---

## Tech Stack

- Python 3.11
- PySpark (submitted to AWS EMR Serverless)
- Apache Airflow 2.x (Docker Compose)
- AWS S3 (data lake — raw and staging layers)
- AWS EMR Serverless (managed Spark compute)
- Snowflake (data warehouse, via AWS Marketplace)
- No Terraform — AWS and Snowflake resources are created manually

---

## Project Structure to Generate

```
booking-migration-demo/
├── data_generator/
│   └── generate_data.py
├── local/
│   └── hdfs_simulation/
│       ├── hotels/
│       ├── customers/
│       └── bookings/
├── ingestion/
│   └── ingest_to_s3.py
├── spark_jobs/
│   ├── transform_hotels.py
│   ├── transform_customers.py
│   └── transform_bookings.py
├── snowflake/
│   └── ddl/
│       ├── 01_raw_tables.sql
│       ├── 02_staging_tables.sql
│       ├── 03_analytics_table.sql
│       └── 04_error_table.sql
├── validation/
│   ├── quality_gates.py
│   └── final_report.py
├── dags/
│   └── migration_dag.py
├── monitoring/
│   └── generate_dashboard.py
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Data Generation (`data_generator/generate_data.py`)

Generate small, realistic datasets using the `faker` library. Save as Parquet files
to `local/hdfs_simulation/`. These files simulate data exported from Hadoop HDFS.

### Table: hotels
- 50 rows
- Columns: hotel_id (INT), name (STRING), city (STRING), country_code (INT),
  star_rating (INT)
- Intentional dirty data: 3 rows with null city, 2 rows with star_rating = 0

### Table: customers
- 200 rows
- Columns: customer_id (INT), name (STRING), email (STRING), signup_date (STRING
  format "yyyyMMdd")
- Intentional dirty data: 10 rows with null email, 5 rows with malformed signup_date

### Table: bookings
- 500 rows
- Columns: booking_id (INT), customer_id (INT), hotel_id (INT), check_in (STRING
  format "yyyyMMdd"), check_out (STRING format "yyyyMMdd"), price (DOUBLE),
  currency (STRING), booking_status (INT)
- booking_status values: 1=CONFIRMED, 2=CANCELLED, 3=PENDING, 4=INVALID
- Intentional dirty data: 20 rows with booking_status=4 (INVALID — to be routed to
  error table), 10 rows with null price

---

## Ingestion (`ingestion/ingest_to_s3.py`)

Read Parquet files from `local/hdfs_simulation/`, run Quality Gate 1, then upload
to S3 `raw/` prefix. This script simulates `hadoop distcp`.

### Logic
1. For each table (hotels, customers, bookings):
   a. Read local Parquet file with pandas
   b. Count rows → store as `source_row_count`
   c. Quality Gate 1:
      - Check file exists → if missing: CRITICAL, raise exception, stop pipeline
      - Check row count > 0 → if zero: CRITICAL, raise exception
      - Check row count >= expected minimum (configurable per table) → if below: CRITICAL
   d. Upload file to `s3://{BUCKET}/raw/{table_name}/data.parquet` using boto3
   e. Log: table name, source rows, S3 path, upload duration

### Quality Gate 1 thresholds
- hotels: min_rows = 40
- customers: min_rows = 150
- bookings: min_rows = 400

### Error handling
- CRITICAL errors: raise exception with clear message → Airflow marks task as failed
- All steps wrapped in try/except with descriptive log messages

---

## Schema Mapping

Only one mapping type is implemented in this demo: **value_map**.

The mapping for `booking_status` (INT → VARCHAR) is defined as a Python dict inside
each Spark job (not a YAML file in the demo, for simplicity):

```python
STATUS_MAP = {
    1: "CONFIRMED",
    2: "CANCELLED",
    3: "PENDING",
    4: "INVALID"   # these rows go to error table
}
```

All other field transformations are data cleaning only (no value mapping):
- `signup_date` / `check_in` / `check_out`: reformat from "yyyyMMdd" STRING to
  proper DATE type
- `country_code` INT: keep as-is in staging (mapping to ISO string is noted as
  future extension in comments)

---

## PySpark Jobs (`spark_jobs/`)

Each job is submitted independently to EMR Serverless. Jobs read from S3 `raw/`,
apply transformations, and write to S3 `staging/`. Problem records go to
`staging/errors/`.

### General pattern for all three jobs

```
read from s3://BUCKET/raw/{table}/
    ↓
Quality Gate 2: schema check (expected columns present?)
    ↓
apply transformations
    ↓
split into clean_df and error_df
    ↓
write clean_df  → s3://BUCKET/staging/{table}/
write error_df  → s3://BUCKET/staging/errors/{table}/
    ↓
log: input rows, clean rows, error rows, duration
```

### `transform_hotels.py`
- Drop rows where city IS NULL → route to error_df with reason "null_city"
- Drop rows where star_rating = 0 → route to error_df with reason "invalid_star_rating"
- Standardise: uppercase country_code (keep as INT, cast to ensure type)
- Write clean rows to staging

### `transform_customers.py`
- Drop rows where email IS NULL → route to error_df with reason "null_email"
- Parse signup_date from STRING "yyyyMMdd" → DATE type
  - If parse fails → route to error_df with reason "invalid_date_format"
- Write clean rows to staging

### `transform_bookings.py` (most complex)
- Apply STATUS_MAP: booking_status INT → status_label VARCHAR
  - Rows where booking_status = 4 (INVALID) → route to error_df with reason
    "invalid_status"
- Drop rows where price IS NULL → route to error_df with reason "null_price"
- Parse check_in and check_out from STRING "yyyyMMdd" → DATE type
- Calculate derived column: stay_duration = check_out - check_in (in days)
- Write clean rows to staging

### Quality Gate 2 (inside each Spark job)
- Check all expected columns are present in the input DataFrame
- If a required column is missing: CRITICAL — raise exception, job fails
- Log null counts per column before transformation (INFO level)

### Error DataFrame schema (same for all tables)
- All original columns from the source table
- Additional column: `error_reason` (STRING) — describes why the row was rejected
- Additional column: `error_timestamp` (TIMESTAMP) — when the error was recorded

---

## Snowflake DDL (`snowflake/ddl/`)

Run these SQL files manually in the Snowflake UI (Worksheets) in order.

### `01_raw_tables.sql`
Create RAW_DB database and three tables mirroring the source schema exactly
(INT types, STRING dates, INT status codes). These are loaded directly from S3
raw/ with COPY INTO.

### `02_staging_tables.sql`
Create STAGING_DB database and three tables with cleaned schema
(DATE types, VARCHAR status labels, derived columns like stay_duration).

### `03_analytics_table.sql`
Create ANALYTICS_DB and one wide table `bookings_enriched` joining bookings +
hotels + customers from STAGING_DB. This is the final queryable output.

### `04_error_table.sql`
Create ERROR_DB and three error tables (one per source table) matching the
error DataFrame schema.

---

## Validation (`validation/`)

### `quality_gates.py`
A module of reusable functions imported by other scripts and the DAG.
Functions:
- `check_row_count_ratio(source_count, target_count, min_ratio=0.95)` →
  returns (passed: bool, message: str)
- `check_error_rate(total_rows, error_rows, max_rate=0.10)` →
  returns (passed: bool, message: str)
- `log_gate_result(gate_name, table, passed, message)` →
  prints structured log line

### `final_report.py`
Runs after all Snowflake loads complete. Queries Snowflake to collect metrics,
evaluates SLOs, and produces two outputs:

**1. Console report** (printed to stdout / visible in Airflow logs):
```
=============================================
  Migration Report — {timestamp}
=============================================
Table: hotels
  Raw rows:     50  |  Staging rows: 47  |  Error rows: 3
  Row ratio:    0.94  ✓  (min: 0.90)
  Error rate:   0.06  ✓  (max: 0.10)
  SLO: PASSED

Table: customers  ...
Table: bookings   ...

Business check:
  Total bookings in ANALYTICS_DB: {n}  ✓
=============================================
```

**2. HTML dashboard** saved to `monitoring/report_{timestamp}.html`
(see Monitoring section below)

---

## Monitoring (`monitoring/generate_dashboard.py`)

Called by `final_report.py` at the end of the pipeline. Generates a single
self-contained HTML file using `plotly` (no server needed — opens in browser).

### Charts to include
1. **Bar chart**: row counts per table — source vs staging vs error (grouped bars)
2. **Horizontal bar chart**: pipeline step durations in seconds
3. **Summary table**: SLO status per table (green = passed, red = failed)

### Output
`monitoring/report_{RUN_TIMESTAMP}.html` — open in any browser, no dependencies.
This file can be committed to the repo to show pipeline results.

---

## Airflow DAG (`dags/migration_dag.py`)

A single DAG named `booking_migration_demo`. Schedule: manual trigger only
(`schedule=None`).

### Task graph
```
generate_data
      │
      ▼
ingest_to_s3
      │
      ├─────────────────────┬──────────────────────┐
      ▼                     ▼                      ▼
transform_hotels   transform_customers   transform_bookings
      │                     │                      │
      └─────────────────────┴──────────────────────┘
                            │
                            ▼
                  load_snowflake_raw
                            │
                            ▼
                  load_snowflake_staging
                            │
                            ▼
                   build_analytics_table
                            │
                            ▼
                      run_final_report
```

### Task types
- `generate_data`: PythonOperator — runs `generate_data.py`
- `ingest_to_s3`: PythonOperator — runs `ingest_to_s3.py`
- `transform_*`: EmrServerlessStartJobOperator — submits each Spark job,
  waits for completion
- `load_snowflake_*`: PythonOperator — runs Snowflake COPY INTO via
  `snowflake-connector-python`
- `build_analytics_table`: PythonOperator — runs the JOIN SQL to populate
  ANALYTICS_DB
- `run_final_report`: PythonOperator — runs `final_report.py` and
  `generate_dashboard.py`

### DAG settings
- `retries=1`, `retry_delay=timedelta(minutes=2)` on all tasks
- `on_failure_callback`: log a clear message with the failed task name

---

## Docker Compose (`docker-compose.yml`)

Based on the official Airflow Docker Compose, with these modifications:

### 1 — AWS credentials injected via environment variables
```yaml
x-airflow-common:
  environment:
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
    SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT}
    SNOWFLAKE_USER: ${SNOWFLAKE_USER}
    SNOWFLAKE_PASSWORD: ${SNOWFLAKE_PASSWORD}
    S3_BUCKET: ${S3_BUCKET}
    EMR_APP_ID: ${EMR_APP_ID}
    EMR_EXECUTION_ROLE_ARN: ${EMR_EXECUTION_ROLE_ARN}
```

### 2 — Custom requirements installed at startup
```yaml
x-airflow-common:
  volumes:
    - ./requirements.txt:/requirements.txt
  environment:
    _PIP_ADDITIONAL_REQUIREMENTS: "apache-airflow-providers-amazon
      snowflake-connector-python boto3 pandas pyarrow plotly faker"
```

### 3 — Local project files mounted
```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./data_generator:/opt/airflow/data_generator
  - ./ingestion:/opt/airflow/ingestion
  - ./validation:/opt/airflow/validation
  - ./monitoring:/opt/airflow/monitoring
  - ./local:/opt/airflow/local
```

### 4 — Airflow connections set via environment variables
```yaml
environment:
  AIRFLOW_CONN_AWS_DEFAULT: "aws://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@?region_name=${AWS_DEFAULT_REGION}"
  AIRFLOW_CONN_SNOWFLAKE_DEFAULT: "snowflake://${SNOWFLAKE_USER}:${SNOWFLAKE_PASSWORD}@${SNOWFLAKE_ACCOUNT}"
```

---

## `.env.example`

```bash
# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=eu-west-1
S3_BUCKET=your-bucket-name

# EMR Serverless
EMR_APP_ID=your_emr_app_id
EMR_EXECUTION_ROLE_ARN=arn:aws:iam::ACCOUNT_ID:role/emr-serverless-role

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

---

## AWS Manual Setup (what to create before running the demo)

Claude should include a `AWS_SETUP.md` file with step-by-step instructions for:

### S3
1. Create bucket (e.g. `booking-migration-demo`) in `eu-west-1`
2. Create folders: `raw/hotels/`, `raw/customers/`, `raw/bookings/`,
   `staging/hotels/`, `staging/customers/`, `staging/bookings/`,
   `staging/errors/`
3. Block all public access (default)

### IAM — User for local scripts
1. Create IAM User `booking-demo-user`
2. Attach policy: `AmazonS3FullAccess` (demo only — production would scope to
   specific bucket)
3. Generate Access Key → put in `.env`

### IAM — Role for EMR Serverless
1. Create Role `emr-serverless-execution-role`
2. Trusted entity: `emr-serverless.amazonaws.com`
3. Attach policies: `AmazonS3FullAccess`, `AmazonEMRServerlessServicePolicy`
4. Note the Role ARN → put in `.env`

### IAM — Role for Snowflake Storage Integration
1. Create Role `snowflake-s3-role`
2. Trusted entity: placeholder initially (Snowflake provides the real trust
   policy after Storage Integration is created — update this role after)
3. Attach policy: S3 read access to your bucket

### EMR Serverless
1. Open EMR console → EMR Serverless → Create Application
2. Name: `booking-migration-demo`
3. Type: Spark
4. Release: emr-6.x (latest stable)
5. Note the Application ID → put in `.env`

### Snowflake Storage Integration
1. In Snowflake worksheet, run:
   ```sql
   CREATE STORAGE INTEGRATION s3_integration
     TYPE = EXTERNAL_STAGE
     STORAGE_PROVIDER = 'S3'
     ENABLED = TRUE
     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-s3-role'
     STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/');
   ```
2. Run `DESC INTEGRATION s3_integration` → copy `STORAGE_AWS_IAM_USER_ARN`
   and `STORAGE_AWS_EXTERNAL_ID`
3. Update the trust policy of `snowflake-s3-role` with these values

---

## Code Style Requirements

- Every function must have a docstring explaining what it does
- Every major step must have a print/log statement so progress is visible
- Use `print()` for simplicity in demo scripts (not logging module)
- PySpark jobs: add inline comments explaining each transformation
- Keep functions short — one function = one responsibility
- No abstract base classes or complex OOP — flat procedural style is fine for demo
- All hardcoded config (bucket name, table names, thresholds) must use environment
  variables or be clearly marked with a `# CONFIG` comment

---

## What "Done" Looks Like

When the demo is complete and running correctly, the user should be able to:

1. Run `docker-compose up` and see Airflow UI at `http://localhost:8080`
2. Trigger the `booking_migration_demo` DAG
3. Watch each task turn green in the Airflow graph view
4. Open Snowflake → query `ANALYTICS_DB.PUBLIC.bookings_enriched` and see rows
5. Open `monitoring/report_{timestamp}.html` in a browser and see charts
6. See a full reconciliation report printed in the Airflow logs of the
   `run_final_report` task

