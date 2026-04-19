-- =============================================================
-- 02_staging_tables.sql
-- =============================================================
-- Creates STAGING_DB and three tables with the cleaned schema.
-- Data is loaded here from S3 staging/ layer (written by Spark jobs).
--
-- Key differences from raw tables:
--   - signup_date / check_in / check_out are DATE (not VARCHAR)
--   - booking_status INT is replaced by status_label VARCHAR
--   - bookings has derived column: stay_duration INT
--
-- Run after 01_raw_tables.sql and after Spark jobs complete.
-- =============================================================

CREATE DATABASE IF NOT EXISTS STAGING_DB;
USE DATABASE STAGING_DB;
USE SCHEMA PUBLIC;

-- S3 stage pointing to the staging/ prefix
CREATE OR REPLACE STAGE staging_s3_stage
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://YOUR_BUCKET_NAME/staging/'  -- replace with your bucket
  FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- hotels (cleaned)
-- Removed: rows where city IS NULL or star_rating = 0
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE STAGING_DB.PUBLIC.hotels (
  hotel_id     INT,
  name         VARCHAR,
  city         VARCHAR     NOT NULL,
  country_code INT,
  star_rating  INT
);

COPY INTO STAGING_DB.PUBLIC.hotels
FROM @staging_s3_stage/hotels/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- customers (cleaned)
-- Removed: rows with null email or malformed signup_date
-- signup_date is now a proper DATE column
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE STAGING_DB.PUBLIC.customers (
  customer_id  INT,
  name         VARCHAR,
  email        VARCHAR     NOT NULL,
  signup_date  DATE                    -- was VARCHAR "yyyyMMdd" in raw
);

COPY INTO STAGING_DB.PUBLIC.customers
FROM @staging_s3_stage/customers/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- -------------------------------------------------------------
-- bookings (cleaned + enriched)
-- Removed: rows with invalid status or null price
-- check_in / check_out are DATE; status_label replaces booking_status INT
-- stay_duration is a derived column added by the Spark job
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE STAGING_DB.PUBLIC.bookings (
  booking_id    INT,
  customer_id   INT,
  hotel_id      INT,
  check_in      DATE,                  -- was VARCHAR "yyyyMMdd" in raw
  check_out     DATE,                  -- was VARCHAR "yyyyMMdd" in raw
  price         DOUBLE       NOT NULL,
  currency      VARCHAR,
  status_label  VARCHAR,               -- was booking_status INT in raw
  stay_duration INT                    -- derived: check_out - check_in in days
);

COPY INTO STAGING_DB.PUBLIC.bookings
FROM @staging_s3_stage/bookings/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);

-- Quick row count check
SELECT 'hotels'    AS tbl, COUNT(*) AS rows FROM STAGING_DB.PUBLIC.hotels    UNION ALL
SELECT 'customers' AS tbl, COUNT(*) AS rows FROM STAGING_DB.PUBLIC.customers UNION ALL
SELECT 'bookings'  AS tbl, COUNT(*) AS rows FROM STAGING_DB.PUBLIC.bookings;
