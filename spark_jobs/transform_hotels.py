"""
transform_hotels.py
-------------------
PySpark job: read hotels from S3 raw layer, clean data, write to staging.

Submitted to AWS EMR Serverless via EmrServerlessStartJobOperator in the DAG.

Transformations:
  - Drop rows where city IS NULL       → error_df (reason: null_city)
  - Drop rows where star_rating = 0    → error_df (reason: invalid_star_rating)
  - Ensure country_code is INT type

Output:
  clean rows  → s3://BUCKET/staging/hotels/
  error rows  → s3://BUCKET/staging/errors/hotels/
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Config ---
BUCKET = os.environ["S3_BUCKET"]
INPUT_PATH = f"s3://{BUCKET}/raw/hotels/"
CLEAN_PATH = f"s3://{BUCKET}/staging/hotels/"
ERROR_PATH  = f"s3://{BUCKET}/staging/errors/hotels/"

EXPECTED_COLS = ["hotel_id", "name", "city", "country_code", "star_rating"]


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
    spark = SparkSession.builder.appName("transform_hotels").getOrCreate()
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

    # Add error metadata columns to flag rejected rows
    error_ts = F.current_timestamp()

    # Identify error rows
    null_city_df = (
        df.filter(F.col("city").isNull())
          .withColumn("error_reason", F.lit("null_city"))
          .withColumn("error_timestamp", error_ts)
    )
    zero_star_df = (
        df.filter(F.col("star_rating") == 0)
          .withColumn("error_reason", F.lit("invalid_star_rating"))
          .withColumn("error_timestamp", error_ts)
    )

    # Combine all error rows
    error_df = null_city_df.union(zero_star_df)

    # Clean rows: city not null AND star_rating not 0
    clean_df = df.filter(
        F.col("city").isNotNull() & (F.col("star_rating") != 0)
    )

    # Ensure country_code is INT (cast to be safe)
    clean_df = clean_df.withColumn("country_code", F.col("country_code").cast("int"))

    clean_count = clean_df.count()
    error_count = error_df.count()
    print(f"Clean rows: {clean_count}  |  Error rows: {error_count}")

    # Write outputs
    clean_df.write.mode("overwrite").parquet(CLEAN_PATH)
    print(f"Written clean → {CLEAN_PATH}")

    error_df.write.mode("overwrite").parquet(ERROR_PATH)
    print(f"Written errors → {ERROR_PATH}")

    duration = round(time.time() - start, 2)
    print(f"transform_hotels finished in {duration}s")
    spark.stop()


if __name__ == "__main__":
    main()
