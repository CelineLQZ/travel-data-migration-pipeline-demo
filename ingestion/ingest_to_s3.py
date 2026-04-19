"""
ingest_to_s3.py
---------------
Reads Parquet files from local/hdfs_simulation/ (simulates Hadoop HDFS),
runs Quality Gate 1, then uploads each file to S3 raw/ layer.

Simulates what 'hadoop distcp' would do in a real migration.

Usage (local):
    python ingestion/ingest_to_s3.py

Called from Airflow via PythonOperator.
"""

import os
import time

import boto3
import pandas as pd

# --- Config (all from environment variables) ---
BUCKET = os.environ["S3_BUCKET"]
LOCAL_BASE = os.path.join("local", "hdfs_simulation")  # CONFIG

# Minimum row thresholds for Quality Gate 1
MIN_ROWS = {
    "hotels":    40,   # CONFIG
    "customers": 150,  # CONFIG
    "bookings":  400,  # CONFIG
}


def quality_gate_1(table, df):
    """
    Quality Gate 1: validate the source file before uploading.

    Checks:
      1. Row count > 0
      2. Row count >= configured minimum for this table

    Raises an exception on failure — Airflow marks the task as failed.
    """
    print(f"[QG1] {table}: running checks (rows={len(df)}, min={MIN_ROWS[table]})...")

    if len(df) == 0:
        raise Exception(f"CRITICAL [QG1] {table}: file is empty (0 rows)")

    if len(df) < MIN_ROWS[table]:
        raise Exception(
            f"CRITICAL [QG1] {table}: only {len(df)} rows, "
            f"expected >= {MIN_ROWS[table]}"
        )

    print(f"[QG1] {table}: PASSED — {len(df)} rows")


def upload_to_s3(local_path, bucket, s3_key):
    """Upload a single local file to S3 and return upload duration in seconds."""
    s3 = boto3.client("s3")
    start = time.time()
    s3.upload_file(local_path, bucket, s3_key)
    duration = round(time.time() - start, 2)
    print(f"  Uploaded → s3://{bucket}/{s3_key}  ({duration}s)")
    return duration


def ingest_table(table):
    """
    Full ingestion flow for one table:
      1. Check file exists
      2. Read parquet with pandas
      3. Run Quality Gate 1
      4. Upload to S3 raw/ layer
    """
    local_path = os.path.join(LOCAL_BASE, table, f"{table}.parquet")
    s3_key = f"raw/{table}/data.parquet"

    print(f"\n[{table}] Starting ingestion...")

    # Step 1: check file exists
    if not os.path.exists(local_path):
        raise Exception(f"CRITICAL [{table}]: file not found at {local_path}")

    # Step 2: read parquet
    df = pd.read_parquet(local_path)
    print(f"[{table}] Read {len(df)} rows from {local_path}")

    # Step 3: Quality Gate 1
    quality_gate_1(table, df)

    # Step 4: upload to S3
    duration = upload_to_s3(local_path, BUCKET, s3_key)

    print(f"[{table}] Done — {len(df)} rows → s3://{BUCKET}/{s3_key} ({duration}s)")
    return len(df)


def main():
    """Ingest all three tables to S3."""
    print("=" * 55)
    print("  Ingestion: local Parquet → S3 raw layer")
    print("=" * 55)

    for table in ["hotels", "customers", "bookings"]:
        try:
            ingest_table(table)
        except Exception as e:
            print(f"[ERROR] {table}: {e}")
            raise  # re-raise so Airflow marks the task as failed

    print("\nAll tables uploaded to S3 raw layer successfully.")


if __name__ == "__main__":
    main()
