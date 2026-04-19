-- =============================================================
-- 04_error_table.sql
-- =============================================================
-- Creates ERROR_DB and three error tables, one per source table.
-- These tables store rows rejected by the Spark transformation jobs,
-- along with the reason and timestamp of rejection.
--
-- Schema = all original source columns + error_reason + error_timestamp.
-- Run after 01_raw_tables.sql.
-- =============================================================

CREATE DATABASE IF NOT EXISTS ERROR_DB;
USE DATABASE ERROR_DB;
USE SCHEMA PUBLIC;

-- S3 stage pointing to the staging/errors/ prefix
CREATE OR REPLACE STAGE errors_s3_stage
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://YOUR_BUCKET_NAME/staging/errors/'  -- replace with your bucket
  FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- hotels_errors
-- Possible error_reason values: null_city, invalid_star_rating
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE ERROR_DB.PUBLIC.hotels_errors (
  hotel_id        INT,
  name            VARCHAR,
  city            VARCHAR,   -- will be NULL for null_city errors
  country_code    INT,
  star_rating     INT,       -- will be 0 for invalid_star_rating errors
  error_reason    VARCHAR,
  error_timestamp TIMESTAMP
);

COPY INTO ERROR_DB.PUBLIC.hotels_errors
FROM @errors_s3_stage/hotels/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- customers_errors
-- Possible error_reason values: null_email, invalid_date_format
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE ERROR_DB.PUBLIC.customers_errors (
  customer_id     INT,
  name            VARCHAR,
  email           VARCHAR,   -- will be NULL for null_email errors
  signup_date     VARCHAR,   -- kept as original STRING (may be malformed)
  error_reason    VARCHAR,
  error_timestamp TIMESTAMP
);

COPY INTO ERROR_DB.PUBLIC.customers_errors
FROM @errors_s3_stage/customers/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- bookings_errors
-- Possible error_reason values: invalid_status, null_price
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE ERROR_DB.PUBLIC.bookings_errors (
  booking_id      INT,
  customer_id     INT,
  hotel_id        INT,
  check_in        VARCHAR,   -- kept as original STRING
  check_out       VARCHAR,   -- kept as original STRING
  price           DOUBLE,    -- will be NULL for null_price errors
  currency        VARCHAR,
  booking_status  INT,       -- kept as original INT (4 for invalid_status)
  status_label    VARCHAR,   -- mapped value (INVALID for status=4)
  error_reason    VARCHAR,
  error_timestamp TIMESTAMP
);

COPY INTO ERROR_DB.PUBLIC.bookings_errors
FROM @errors_s3_stage/bookings/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- Quick check
SELECT 'hotels_errors'    AS tbl, COUNT(*) AS rows FROM ERROR_DB.PUBLIC.hotels_errors    UNION ALL
SELECT 'customers_errors' AS tbl, COUNT(*) AS rows FROM ERROR_DB.PUBLIC.customers_errors UNION ALL
SELECT 'bookings_errors'  AS tbl, COUNT(*) AS rows FROM ERROR_DB.PUBLIC.bookings_errors;
