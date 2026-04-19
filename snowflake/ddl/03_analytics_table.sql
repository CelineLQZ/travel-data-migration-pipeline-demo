-- =============================================================
-- 03_analytics_table.sql
-- =============================================================
-- Creates ANALYTICS_DB and the final wide table bookings_enriched.
-- This is the table that business users and BI tools query.
--
-- It joins bookings + customers + hotels from STAGING_DB into one
-- denormalised table — no JOINs needed at query time.
--
-- Run after 02_staging_tables.sql and after staging data is loaded.
-- =============================================================

CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;
USE DATABASE ANALYTICS_DB;
USE SCHEMA PUBLIC;

-- Drop and recreate so the DAG can re-run idempotently
CREATE OR REPLACE TABLE ANALYTICS_DB.PUBLIC.bookings_enriched AS
SELECT
    -- Booking details
    b.booking_id,
    b.check_in,
    b.check_out,
    b.stay_duration,
    b.price,
    b.currency,
    b.status_label,

    -- Customer details (denormalised)
    c.customer_id,
    c.name         AS customer_name,
    c.email        AS customer_email,
    c.signup_date  AS customer_signup_date,

    -- Hotel details (denormalised)
    h.hotel_id,
    h.name         AS hotel_name,
    h.city         AS hotel_city,
    h.country_code AS hotel_country_code,
    h.star_rating  AS hotel_star_rating

FROM STAGING_DB.PUBLIC.bookings   b
JOIN STAGING_DB.PUBLIC.customers  c ON b.customer_id = c.customer_id
JOIN STAGING_DB.PUBLIC.hotels     h ON b.hotel_id    = h.hotel_id;

-- Quick sanity check — should return a single row count
SELECT COUNT(*) AS total_bookings_enriched FROM ANALYTICS_DB.PUBLIC.bookings_enriched;
