-- =============================================================
-- 01_raw_tables.sql
-- =============================================================
-- Creates RAW_DB and three tables that mirror the source schema
-- exactly (INT types, VARCHAR dates, INT status codes).
-- Data is loaded here with COPY INTO directly from S3 raw/ layer.
--
-- Run this first in the Snowflake Worksheet.
-- =============================================================

-- Create the raw database
CREATE DATABASE IF NOT EXISTS RAW_DB;
USE DATABASE RAW_DB;
USE SCHEMA PUBLIC;

-- -------------------------------------------------------------
-- S3 stage — points to the raw/ prefix of your S3 bucket.
-- Requires the storage integration created in AWS_SETUP.md.
-- -------------------------------------------------------------
CREATE OR REPLACE STAGE raw_s3_stage
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://YOUR_BUCKET_NAME/raw/'      -- replace with your bucket
  FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- hotels — mirrors generate_data.py output exactly
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.hotels (
  hotel_id     INT,
  name         VARCHAR,
  city         VARCHAR,       -- nullable (dirty data: 3 null cities)
  country_code INT,
  star_rating  INT            -- dirty data: 2 rows have star_rating = 0
);

-- Load from S3 (run after ingest_to_s3.py uploads the file)
COPY INTO RAW_DB.PUBLIC.hotels
FROM @raw_s3_stage/hotels/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- customers — mirrors generate_data.py output exactly
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.customers (
  customer_id  INT,
  name         VARCHAR,
  email        VARCHAR,       -- nullable (dirty data: 10 null emails)
  signup_date  VARCHAR        -- stored as "yyyyMMdd" string from source
                              -- dirty data: 5 rows have malformed dates
);

COPY INTO RAW_DB.PUBLIC.customers
FROM @raw_s3_stage/customers/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- bookings — mirrors generate_data.py output exactly
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.bookings (
  booking_id     INT,
  customer_id    INT,
  hotel_id       INT,
  check_in       VARCHAR,     -- stored as "yyyyMMdd" string from source
  check_out      VARCHAR,     -- stored as "yyyyMMdd" string from source
  price          DOUBLE,      -- nullable (dirty data: 10 null prices)
  currency       VARCHAR,
  booking_status INT          -- 1=CONFIRMED 2=CANCELLED 3=PENDING 4=INVALID
);

COPY INTO RAW_DB.PUBLIC.bookings
FROM @raw_s3_stage/bookings/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- Quick row count check after loading
SELECT 'hotels'    AS tbl, COUNT(*) AS rows FROM RAW_DB.PUBLIC.hotels    UNION ALL
SELECT 'customers' AS tbl, COUNT(*) AS rows FROM RAW_DB.PUBLIC.customers UNION ALL
SELECT 'bookings'  AS tbl, COUNT(*) AS rows FROM RAW_DB.PUBLIC.bookings;
