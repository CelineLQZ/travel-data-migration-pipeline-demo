"""
migration_dag.py
----------------
Airflow DAG for the Booking → Snowflake migration demo.

DAG name: travel_migration_demo
Schedule: manual trigger only (schedule=None)

Task graph:
    generate_data
         |
    ingest_to_s3
         |
   ┌─────┼──────────────┐
   ▼     ▼              ▼
hotels customers    bookings
(EMR)   (EMR)        (EMR)
   └─────┼──────────────┘
         |
   load_snowflake_raw
         |
   load_snowflake_staging
         |
   build_analytics_table
         |
    run_final_report

Before running this DAG you must:
  1. Upload spark_jobs/*.py to s3://YOUR_BUCKET/scripts/
  2. Fill in .env and start docker-compose
  3. Set AIRFLOW_CONN_AWS_DEFAULT and AIRFLOW_CONN_SNOWFLAKE_DEFAULT
     (done automatically via docker-compose.yml environment variables)
"""

import os
import sys
from datetime import timedelta

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.utils.dates import days_ago

# --- Config (injected via docker-compose environment variables) ---
S3_BUCKET              = os.environ.get("S3_BUCKET", "your-bucket-name")
EMR_APP_ID             = os.environ.get("EMR_APP_ID", "")
EMR_EXECUTION_ROLE_ARN = os.environ.get("EMR_EXECUTION_ROLE_ARN", "")
SNOWFLAKE_ACCOUNT      = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER         = os.environ.get("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD     = os.environ.get("SNOWFLAKE_PASSWORD", "")

# S3 path where Spark scripts have been uploaded
SCRIPTS_S3 = f"s3://{S3_BUCKET}/scripts"

# Add project root to sys.path so PythonOperators can import local modules
sys.path.insert(0, "/opt/airflow")


# ---------------------------------------------------------------------------
# Shared failure callback
# ---------------------------------------------------------------------------

def on_failure_callback(context):
    """Log a clear message when any task fails."""
    task_id = context["task_instance"].task_id
    dag_id  = context["dag"].dag_id
    print(f"[FAILURE] DAG={dag_id}  Task={task_id} failed. Check logs above for details.")


# ---------------------------------------------------------------------------
# Python callables used by PythonOperators
# ---------------------------------------------------------------------------

def run_generate_data():
    """Generate synthetic Parquet files and save to local/hdfs_simulation/."""
    import importlib.util, os
    spec = importlib.util.spec_from_file_location(
        "generate_data", "/opt/airflow/data_generator/generate_data.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)


def run_ingest_to_s3():
    """Run Quality Gate 1 then upload Parquet files to S3 raw/ layer."""
    from ingestion.ingest_to_s3 import main
    main()


def _get_snowflake_conn():
    """Return a Snowflake connection using environment credentials."""
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
    )


def run_load_snowflake_raw():
    """COPY INTO RAW_DB tables from S3 raw/ layer."""
    conn = _get_snowflake_conn()
    cur  = conn.cursor()
    for table in ["hotels", "customers", "bookings"]:
        print(f"[load_raw] Loading {table}...")
        cur.execute(f"""
            COPY INTO RAW_DB.PUBLIC.{table}
            FROM @RAW_DB.PUBLIC.raw_s3_stage/{table}/
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FILE_FORMAT = (TYPE = PARQUET)
            PURGE = FALSE
        """)
        result = cur.fetchall()
        print(f"[load_raw] {table}: {result}")
    cur.close()
    conn.close()
    print("[load_raw] Done.")


def run_load_snowflake_staging():
    """COPY INTO STAGING_DB tables from S3 staging/ layer (post-Spark)."""
    conn = _get_snowflake_conn()
    cur  = conn.cursor()
    for table in ["hotels", "customers", "bookings"]:
        print(f"[load_staging] Loading {table}...")
        cur.execute(f"""
            COPY INTO STAGING_DB.PUBLIC.{table}
            FROM @STAGING_DB.PUBLIC.staging_s3_stage/{table}/
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = SKIP_FILE
            PURGE = FALSE
        """)
        result = cur.fetchall()
        print(f"[load_staging] {table}: {result}")

        # Load error rows into ERROR_DB
        print(f"[load_staging] Loading {table} errors...")
        cur.execute(f"""
            COPY INTO ERROR_DB.PUBLIC.{table}_errors
            FROM @ERROR_DB.PUBLIC.errors_s3_stage/{table}/
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = SKIP_FILE
            PURGE = FALSE
        """)

    cur.close()
    conn.close()
    print("[load_staging] Done.")


def run_build_analytics_table():
    """Populate ANALYTICS_DB.PUBLIC.bookings_enriched from staging tables."""
    conn = _get_snowflake_conn()
    cur  = conn.cursor()
    print("[analytics] Building bookings_enriched...")
    cur.execute("""
        CREATE OR REPLACE TABLE ANALYTICS_DB.PUBLIC.bookings_enriched AS
        SELECT
            b.booking_id, b.check_in, b.check_out, b.stay_duration,
            b.price, b.currency, b.status_label,
            c.customer_id, c.name AS customer_name, c.email AS customer_email,
            c.signup_date AS customer_signup_date,
            h.hotel_id, h.name AS hotel_name, h.city AS hotel_city,
            h.country_code AS hotel_country_code, h.star_rating AS hotel_star_rating
        FROM STAGING_DB.PUBLIC.bookings   b
        JOIN STAGING_DB.PUBLIC.customers  c ON b.customer_id = c.customer_id
        JOIN STAGING_DB.PUBLIC.hotels     h ON b.hotel_id    = h.hotel_id
    """)
    cur.execute("SELECT COUNT(*) FROM ANALYTICS_DB.PUBLIC.bookings_enriched")
    count = cur.fetchone()[0]
    print(f"[analytics] bookings_enriched has {count} rows")
    cur.close()
    conn.close()


def run_final_report():
    """Run the reconciliation report and generate the HTML dashboard."""
    from validation.final_report import main
    main()


# ---------------------------------------------------------------------------
# Helper: build EMR Serverless job driver dict
# ---------------------------------------------------------------------------

def emr_job_driver(script_name):
    """Return the job_driver dict for an EMR Serverless Spark submission."""
    return {
        "sparkSubmit": {
            "entryPoint": f"{SCRIPTS_S3}/{script_name}",
            "sparkSubmitParameters": (
                "--conf spark.executor.cores=1 "
                "--conf spark.executor.memory=2g "
                "--conf spark.driver.memory=1g "
                "--conf spark.executor.instances=1 "
                f"--conf spark.emr-serverless.driverEnv.S3_BUCKET={S3_BUCKET} "
                f"--conf spark.executorEnv.S3_BUCKET={S3_BUCKET}"
            ),
        }
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="travel_migration_demo",
    default_args=default_args,
    description="Demo: travel industry data migration from Hadoop to Snowflake",
    schedule=None,           # manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "migration"],
) as dag:

    # 1. Generate synthetic data
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=run_generate_data,
    )

    # 2. Upload raw Parquet files to S3 (Quality Gate 1 runs here)
    ingest_to_s3 = PythonOperator(
        task_id="ingest_to_s3",
        python_callable=run_ingest_to_s3,
    )

    # 3a-c. Spark transformation jobs on EMR Serverless (run in parallel)
    transform_hotels = EmrServerlessStartJobOperator(
        task_id="transform_hotels",
        application_id=EMR_APP_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=emr_job_driver("transform_hotels.py"),
        configuration_overrides={},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    transform_customers = EmrServerlessStartJobOperator(
        task_id="transform_customers",
        application_id=EMR_APP_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=emr_job_driver("transform_customers.py"),
        configuration_overrides={},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    transform_bookings = EmrServerlessStartJobOperator(
        task_id="transform_bookings",
        application_id=EMR_APP_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=emr_job_driver("transform_bookings.py"),
        configuration_overrides={},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # 4. Load raw Parquet into Snowflake RAW_DB
    load_snowflake_raw = PythonOperator(
        task_id="load_snowflake_raw",
        python_callable=run_load_snowflake_raw,
    )

    # 5. Load staged Parquet into Snowflake STAGING_DB + ERROR_DB
    load_snowflake_staging = PythonOperator(
        task_id="load_snowflake_staging",
        python_callable=run_load_snowflake_staging,
    )

    # 6. Build analytics wide table
    build_analytics_table = PythonOperator(
        task_id="build_analytics_table",
        python_callable=run_build_analytics_table,
    )

    # 7. Final reconciliation report + HTML dashboard
    run_final_report_task = PythonOperator(
        task_id="run_final_report",
        python_callable=run_final_report,
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    generate_data >> ingest_to_s3
    ingest_to_s3  >> [transform_hotels, transform_customers, transform_bookings]
    [transform_hotels, transform_customers, transform_bookings] >> load_snowflake_raw
    load_snowflake_raw >> load_snowflake_staging
    load_snowflake_staging >> build_analytics_table
    build_analytics_table  >> run_final_report_task
