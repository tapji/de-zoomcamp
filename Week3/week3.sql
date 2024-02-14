-- Creating a table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-405316.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tapji_mage/2022_green_data.parquet']
);

--Q1: What is the count of the 2022 green taxi data?
SELECT count(*) FROM `data-engineering-405316.ny_taxi.external_green_tripdata`; --Ans: 840,402

--Q2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT(DISTINCT PULocationID) FROM `data-engineering-405316.ny_taxi.external_green_tripdata`; --Ans: 0MB from external table

SELECT COUNT(DISTINCT PULocationID) FROM `data-engineering-405316.ny_taxi.green_tripdata`; --Ans: 6.41MB from materialized table

--Q3: How many records have a fare_amount of 0?
SELECT count(*) FROM `data-engineering-405316.ny_taxi.external_green_tripdata`
WHERE fare_amount = 0; --Ans: 1622

--Q4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
  
  --Ans: Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE `data-engineering-405316.ny_taxi.green_data_partition_cluster`
PARTITION BY DATE(cleaned_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime FROM `data-engineering-405316.ny_taxi.external_green_tripdata`;

--Q5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?Choose the answer which most closely matches.

--Scenario 1: Materialized Table query (Formatting the datetime format first)
CREATE OR REPLACE TABLE `data-engineering-405316.ny_taxi.green_tripdata_formatted_datetime`
PARTITION BY DATE(cleaned_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime FROM `data-engineering-405316.ny_taxi.green_tripdata`;

SELECT DISTINCT (PULocationID) FROM `data-engineering-405316.ny_taxi.green_tripdata_formatted_datetime`
WHERE DATE(cleaned_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; --Ans: 1.12MB from materialized table
-- Ans: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

--Q6:Where is the data stored in the External Table you created?
--Ans: GCP Bucket

--Q7: It is best practice in Big Query to always cluster your data:
--Ans: True

--Q8: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT * FROM `data-engineering-405316.ny_taxi.green_tripdata`; --Ans: 114.11MB