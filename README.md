# RedShift_BigQuery_SQL_Queries

-- import table from gcs

CREATE OR REPLACE EXTERNAL TABLE `esoteric-cab-411900.trips_data_all.external_green_trip_data`
-- (
--   VendorID INT64,
--   lpep_pickup_datetime INT64,
--   lpep_dropoff_datetime INT64,
--   passenger_count FLOAT64,
--   trip_distance FLOAT64,
--   RatecodeID FLOAT64,
--   store_and_fwd_flag STRING,
--   PULocationID INT64,
--   DOLocationID INT64,
--   payment_type FLOAT64,
--   fare_amount FLOAT64,
--   extra FLOAT64,
--   mta_tax FLOAT64,
--   tip_amount FLOAT64,
--   tolls_amount FLOAT64,
--   ehail_fee INT64,
--   improvement_surcharge FLOAT64,
--   total_amount FLOAT64,
--   trip_type FLOAT64,
--   congestion_surcharge FLOAT64,
-- )
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_mage_bucket/green_taxi/1cccc7da96534ce1ba7ca4530472e355-0.parquet']
);

-- convert external table to internal
CREATE OR REPLACE TABLE esoteric-cab-411900.trips_data_all.internal_green_trip_data
AS
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime
FROM esoteric-cab-411900.trips_data_all.external_green_trip_data;

-- question 1, count records

SELECT COUNT(*)
FROM trips_data_all.external_green_trip_data;

-- question 2, distinct # of PULocationIDs for internal table

SELECT DISTINCT PULocationID
FROM esoteric-cab-411900.trips_data_all.internal_green_trip_data;

--distinct # of PULocationIDs for external table

SELECT DISTINCT PULocationID
FROM esoteric-cab-411900.trips_data_all.external_green_trip_data;

-- question 3, how many records have fare_amount of 0?

SELECT count(fare_amount)
FROM esoteric-cab-411900.trips_data_all.internal_green_trip_data
WHERE fare_amount = 0;

-- question 4, partitioned and clustered

CREATE OR REPLACE TABLE esoteric-cab-411900.trips_data_all.cleaned_green_trip_data
PARTITION BY DATE(cleaned_pickup_datetime)
CLUSTER BY PULocationID
AS
SELECT *
FROM esoteric-cab-411900.trips_data_all.internal_green_trip_data;

-- question 5, distinct PULocationID between 6/1/2022 and 6/30/2022 inclusive for regular table

SELECT COUNT(DISTINCT PULocationID)
FROM esoteric-cab-411900.trips_data_all.internal_green_trip_data
WHERE DATE(cleaned_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Distinct PULocationID between 6/1/2022 and 6/30/2022 inclusive for partitioned table

SELECT COUNT(DISTINCT PULocationID)
FROM esoteric-cab-411900.trips_data_all.cleaned_green_trip_data
WHERE DATE(cleaned_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

------ Building Machine Learning Model in Google Cloud ------

-- Optimizing data types for linear regression: enumerated int types get converted to strings for automatic one-hot encoding into sparse vector.
CREATE OR REPLACE TABLE esoteric-cab-411900.trips_data_all.green_trip_data_tip_linear_regression (
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  PULocationID STRING,
  DOLocationID STRING,
  payment_type STRING,
  fare_amount FLOAT64,
  tolls_amount FLOAT64,
  tip_amount FLOAT64,
) AS (
  SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING), CAST (payment_type AS STRING), fare_amount, tolls_amount, tip_amount
  FROM esoteric-cab-411900.trips_data_all.cleaned_green_trip_data WHERE fare_amount != 0
);

-- Build linear regression model to approximate tip
CREATE OR REPLACE MODEL `esoteric-cab-411900.trips_data_all.tip_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT *
FROM `esoteric-cab-411900.trips_data_all.green_trip_data_tip_linear_regression`
WHERE tip_amount IS NOT NULL;

-- Check features
SELECT *
FROM ML.FEATURE_INFO(MODEL `esoteric-cab-411900.trips_data_all.tip_model`);
