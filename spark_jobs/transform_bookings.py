"""
transform_bookings.py
---------------------
PySpark job: read bookings from S3 raw layer, clean data, write to staging.
This is the most complex transformation job in the pipeline.

Submitted to AWS EMR Serverless via EmrServerlessStartJobOperator in the DAG.

Transformations:
  - Map booking_status INT → status_label VARCHAR using STATUS_MAP
      Rows with status=4 (INVALID)      → error_df (reason: invalid_status)
  - Drop rows where price IS NULL       → error_df (reason: null_price)
  - Parse check_in / check_out from STRING "yyyyMMdd" → DATE
  - Calculate derived column: stay_duration = check_out - check_in (days)

Output:
  clean rows  → s3://BUCKET/staging/bookings/
  error rows  → s3://BUCKET/staging/errors/bookings/
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# --- Config ---
BUCKET = os.environ["S3_BUCKET"]
INPUT_PATH = f"s3://{BUCKET}/raw/bookings/"
CLEAN_PATH = f"s3://{BUCKET}/staging/bookings/"
ERROR_PATH  = f"s3://{BUCKET}/staging/errors/bookings/"

EXPECTED_COLS = [
    "booking_id", "customer_id", "hotel_id",
    "check_in", "check_out", "price", "currency", "booking_status"
]

# Mapping from integer status code to human-readable label
# Status 4 = INVALID — these rows are routed to the error table
STATUS_MAP = {
    1: "CONFIRMED",
    2: "CANCELLED",
    3: "PENDING",
    4: "INVALID",
}
INVALID_STATUS_CODE = 4


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
    spark = SparkSession.builder.appName("transform_bookings").getOrCreate()
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

    # --- Apply STATUS_MAP: booking_status INT → status_label VARCHAR ---
    # Build a PySpark map from the Python dict
    status_map_expr = F.create_map(
        *[item for pair in STATUS_MAP.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    df = df.withColumn("status_label", status_map_expr[F.col("booking_status")])

    # --- Error: invalid status (status=4) ---
    invalid_status_df = (
        df.filter(F.col("booking_status") == INVALID_STATUS_CODE)
          .withColumn("error_reason", F.lit("invalid_status"))
          .withColumn("error_timestamp", error_ts)
    )

    # --- Error: null price ---
    null_price_df = (
        df.filter(
            F.col("price").isNull() & (F.col("booking_status") != INVALID_STATUS_CODE)
        )
        .withColumn("error_reason", F.lit("null_price"))
        .withColumn("error_timestamp", error_ts)
    )

    # Combine all error rows
    error_df = invalid_status_df.union(null_price_df)

    # --- Clean rows: valid status AND price not null ---
    clean_df = df.filter(
        (F.col("booking_status") != INVALID_STATUS_CODE) & F.col("price").isNotNull()
    )

    # --- Parse check_in and check_out: "yyyyMMdd" STRING → DATE ---
    clean_df = clean_df.withColumn("check_in",  F.to_date(F.col("check_in"),  "yyyyMMdd"))
    clean_df = clean_df.withColumn("check_out", F.to_date(F.col("check_out"), "yyyyMMdd"))

    # --- Derived column: stay_duration in days ---
    clean_df = clean_df.withColumn(
        "stay_duration",
        F.datediff(F.col("check_out"), F.col("check_in"))
    )

    # Drop the raw integer status column — replaced by status_label
    clean_df = clean_df.drop("booking_status")

    clean_count = clean_df.count()
    error_count = error_df.count()
    print(f"Clean rows: {clean_count}  |  Error rows: {error_count}")

    # Write outputs
    clean_df.write.mode("overwrite").parquet(CLEAN_PATH)
    print(f"Written clean → {CLEAN_PATH}")

    error_df.write.mode("overwrite").parquet(ERROR_PATH)
    print(f"Written errors → {ERROR_PATH}")

    duration = round(time.time() - start, 2)
    print(f"transform_bookings finished in {duration}s")
    spark.stop()


if __name__ == "__main__":
    main()
