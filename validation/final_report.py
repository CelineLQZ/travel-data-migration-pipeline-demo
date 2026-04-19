"""
final_report.py
---------------
Runs after all Snowflake loads complete.

1. Queries Snowflake to collect row counts from RAW_DB, STAGING_DB, ERROR_DB.
2. Evaluates SLOs using quality_gates.py.
3. Prints a console reconciliation report (visible in Airflow logs).
4. Calls generate_dashboard.py to produce the HTML report.

Usage:
    python validation/final_report.py

Called from Airflow via PythonOperator (run_final_report task).
"""

import os
import sys
from datetime import datetime

import snowflake.connector

# Add project root to path so we can import local modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from validation.quality_gates import check_row_count_ratio, check_error_rate, log_gate_result
from monitoring.generate_dashboard import generate_dashboard

# --- Config (from environment variables) ---
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]

# SLO thresholds
MIN_ROW_RATIO  = 0.90   # CONFIG — at least 90% of raw rows must reach staging
MAX_ERROR_RATE = 0.10   # CONFIG — at most 10% of rows can be errors


def get_snowflake_conn():
    """Open and return a Snowflake connection using environment credentials."""
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
    )


def query_row_count(cur, database, schema, table):
    """Return the row count for a given Snowflake table."""
    cur.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
    return cur.fetchone()[0]


def collect_metrics(cur):
    """
    Query Snowflake for row counts across all databases.

    Returns a dict keyed by table name with source/staging/error counts.
    """
    tables = ["hotels", "customers", "bookings"]
    metrics = {}

    for table in tables:
        raw_count     = query_row_count(cur, "RAW_DB",     "PUBLIC", table)
        staging_count = query_row_count(cur, "STAGING_DB", "PUBLIC", table)
        error_count   = query_row_count(cur, "ERROR_DB",   "PUBLIC", f"{table}_errors")

        metrics[table] = {
            "source":  raw_count,
            "staging": staging_count,
            "errors":  error_count,
        }
        print(f"  [{table}] raw={raw_count}  staging={staging_count}  errors={error_count}")

    # Total bookings in analytics layer
    cur.execute("SELECT COUNT(*) FROM ANALYTICS_DB.PUBLIC.bookings_enriched")
    metrics["analytics_total"] = cur.fetchone()[0]
    print(f"  [analytics] bookings_enriched = {metrics['analytics_total']}")

    return metrics


def evaluate_slos(metrics):
    """
    Run quality gate checks for each table and determine SLO status.

    Returns the metrics dict with 'slo' key added per table.
    """
    tables = ["hotels", "customers", "bookings"]

    for table in tables:
        m = metrics[table]
        total_rows = m["source"]

        ratio_passed, ratio_msg = check_row_count_ratio(
            m["source"], m["staging"], min_ratio=MIN_ROW_RATIO
        )
        error_passed, error_msg = check_error_rate(
            total_rows, m["errors"], max_rate=MAX_ERROR_RATE
        )

        log_gate_result("QG3-ratio", table, ratio_passed, ratio_msg)
        log_gate_result("QG3-error", table, error_passed, error_msg)

        metrics[table]["slo"] = "PASSED" if (ratio_passed and error_passed) else "FAILED"
        metrics[table]["ratio_msg"] = ratio_msg
        metrics[table]["error_msg"] = error_msg

    return metrics


def print_console_report(metrics, run_timestamp):
    """Print the reconciliation report to stdout (appears in Airflow logs)."""
    tables = ["hotels", "customers", "bookings"]

    print("\n" + "=" * 50)
    print(f"  Migration Report — {run_timestamp}")
    print("=" * 50)

    for table in tables:
        m = metrics[table]
        ratio = m["staging"] / m["source"] if m["source"] > 0 else 0
        error_rate = m["errors"] / m["source"] if m["source"] > 0 else 0

        print(f"\nTable: {table}")
        print(f"  Raw rows:     {m['source']:>5}  |  Staging rows: {m['staging']:>5}  |  Error rows: {m['errors']:>4}")
        print(f"  Row ratio:    {ratio:.2f}  (min: {MIN_ROW_RATIO})")
        print(f"  Error rate:   {error_rate:.2f}  (max: {MAX_ERROR_RATE})")
        print(f"  SLO: {m['slo']}")

    print(f"\nBusiness check:")
    print(f"  Total bookings in ANALYTICS_DB: {metrics['analytics_total']}")
    print("=" * 50)


def main():
    """Entry point — collect metrics, evaluate SLOs, print report, save dashboard."""
    run_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    print(f"[final_report] Starting — run_timestamp={run_timestamp}")

    # Connect to Snowflake
    print("[final_report] Connecting to Snowflake...")
    conn = get_snowflake_conn()
    cur  = conn.cursor()

    # Collect row counts
    print("[final_report] Collecting metrics...")
    metrics = collect_metrics(cur)

    cur.close()
    conn.close()

    # Evaluate SLOs
    print("[final_report] Evaluating SLOs...")
    metrics = evaluate_slos(metrics)

    # Print console report
    print_console_report(metrics, run_timestamp)

    # Generate HTML dashboard
    dashboard_metrics = {
        "tables": {
            t: {k: metrics[t][k] for k in ("source", "staging", "errors", "slo")}
            for t in ["hotels", "customers", "bookings"]
        },
        # Placeholder durations — in a real pipeline these would be passed via XCom
        "durations": {
            "generate_data":          0,
            "ingest_to_s3":           0,
            "transform_hotels":       0,
            "transform_customers":    0,
            "transform_bookings":     0,
            "load_snowflake_raw":     0,
            "load_snowflake_staging": 0,
            "build_analytics_table":  0,
        },
    }
    generate_dashboard(dashboard_metrics, run_timestamp)

    print("[final_report] Done.")


if __name__ == "__main__":
    main()
