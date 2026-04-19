"""
transform_customers.py
----------------------
PySpark job: read customers from S3 raw layer, clean data, write to staging.

Submitted to AWS EMR Serverless via EmrServerlessStartJobOperator in the DAG.

Transformations:
  - Drop rows where email IS NULL                      → error_df (reason: null_email)
  - Parse signup_date from STRING "yyyyMMdd" → DATE
      If parsing fails (bad format)                    → error_df (reason: invalid_date_format)
  - Write clean rows to staging

Output:
  clean rows  → s3://BUCKET/staging/customers/
  error rows  → s3://BUCKET/staging/errors/customers/
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Config ---
BUCKET = os.environ["S3_BUCKET"]
INPUT_PATH = f"s3://{BUCKET}/raw/customers/"
CLEAN_PATH = f"s3://{BUCKET}/staging/customers/"
ERROR_PATH  = f"s3://{BUCKET}/staging/errors/customers/"

EXPECTED_COLS = ["customer_id", "name", "email", "signup_date"]


def quality_gate_2(df):
    """
    Quality Gate 2: check all expected columns are present in the input DataFrame.
    Raises an exception if any column is missing — job fails immediately.
    """
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise Exception(f"CRITICAL [QG2] Missing columns: {missing}")
    print(f"[QG2] Schema check passed. Columns present: {df.columns}")


def main():
    spark = SparkSession.builder.appName("transform_customers").getOrCreate()
    start = time.time()

    print(f"Reading from: {INPUT_PATH}")
    df = spark.read.parquet(INPUT_PATH)
    input_count = df.count()
    print(f"Input rows: {input_count}")

    # Quality Gate 2: schema check
    quality_gate_2(df)

    # Log null counts per column before transformation
    print("Null counts per column (before transformation):")
    df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

    error_ts = F.current_timestamp()

    # --- Error: null email ---
    null_email_df = (
        df.filter(F.col("email").isNull())
          .withColumn("error_reason", F.lit("null_email"))
          .withColumn("error_timestamp", error_ts)
    )

    # --- Parse signup_date: "yyyyMMdd" string → DATE ---
    # to_date returns null if the format doesn't match
    df = df.withColumn(
        "signup_date_parsed",
        F.to_date(F.col("signup_date"), "yyyyMMdd")
    )

    # Rows where original date was not null but parsing failed → bad format
    bad_date_df = (
        df.filter(
            F.col("signup_date").isNotNull() & F.col("signup_date_parsed").isNull()
        )
        .drop("signup_date_parsed")  # keep original columns for error table
        .withColumn("error_reason", F.lit("invalid_date_format"))
        .withColumn("error_timestamp", error_ts)
    )

    # Combine errors (drop parsed column from null_email_df too)
    null_email_df = null_email_df.drop("signup_date_parsed") \
        if "signup_date_parsed" in null_email_df.columns else null_email_df
    error_df = null_email_df.union(bad_date_df)

    # Clean rows: email not null AND date parsed successfully
    clean_df = (
        df.filter(
            F.col("email").isNotNull() & F.col("signup_date_parsed").isNotNull()
        )
        # Replace original string signup_date with parsed DATE column
        .drop("signup_date")
        .withColumnRenamed("signup_date_parsed", "signup_date")
    )

    clean_count = clean_df.count()
    error_count = error_df.count()
    print(f"Clean rows: {clean_count}  |  Error rows: {error_count}")

    # Write outputs
    clean_df.write.mode("overwrite").parquet(CLEAN_PATH)
    print(f"Written clean → {CLEAN_PATH}")

    error_df.write.mode("overwrite").parquet(ERROR_PATH)
    print(f"Written errors → {ERROR_PATH}")

    duration = round(time.time() - start, 2)
    print(f"transform_customers finished in {duration}s")
    spark.stop()


if __name__ == "__main__":
    main()
