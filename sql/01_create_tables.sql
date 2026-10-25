-- Athena DDL for taxi_db   (query result location: s3://faiz-taxi-lake-2026/athena-results/)
-- Run each statement separately in the Athena query editor.

CREATE DATABASE IF NOT EXISTS taxi_db;

-- 1. Raw yellow trips. EXTERNAL = data stays in S3, Athena stores only the schema.
CREATE EXTERNAL TABLE taxi_db.raw_yellow_trips (
  VendorID bigint, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp,
  passenger_count bigint, trip_distance double, RatecodeID bigint,
  store_and_fwd_flag string, PULocationID bigint, DOLocationID bigint,
  payment_type bigint, fare_amount double, extra double, mta_tax double,
  tip_amount double, tolls_amount double, improvement_surcharge double,
  total_amount double, congestion_surcharge double, Airport_fee double
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://faiz-taxi-lake-2026/raw/yellow_trips/';

-- find the month=01..06 folders and register them as partitions
MSCK REPAIR TABLE taxi_db.raw_yellow_trips;

-- 2. Gold tables (written by Glue job taxi-raw-to-clean)
CREATE EXTERNAL TABLE taxi_db.gold_daily_zone_revenue (
  pickup_date date, pickup_borough string, pickup_zone string, trips bigint, revenue double)
STORED AS PARQUET LOCATION 's3://faiz-taxi-lake-2026/gold/daily_zone_revenue/';

CREATE EXTERNAL TABLE taxi_db.gold_hourly_demand (
  weekday string, pickup_hour int, is_airport boolean, trips bigint, avg_minutes double)
STORED AS PARQUET LOCATION 's3://faiz-taxi-lake-2026/gold/hourly_demand/';

CREATE EXTERNAL TABLE taxi_db.gold_payment_tips (
  month int, payment string, trips bigint, avg_tip_pct double)
STORED AS PARQUET LOCATION 's3://faiz-taxi-lake-2026/gold/payment_tips/';

-- 3. Framework audit (one row per framework run)
CREATE EXTERNAL TABLE taxi_db.framework_audit (
  feed string, raw_rows bigint, good_rows bigint, bad_rows bigint, started string, ended string)
STORED AS PARQUET LOCATION 's3://faiz-taxi-lake-2026/framework/audit/';
