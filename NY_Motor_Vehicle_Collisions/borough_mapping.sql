USE DATABASE NYC_NULL_BOROUGH_FIX;
USE SCHEMA BOROUGH_CURATED;

CREATE OR REPLACE TABLE NYC_MOTOR_VEHICLE_COLLISIONS AS
SELECT * FROM NYC_COLLISIONS.RAW.NYC_MOTOR_VEHICLE_COLLISIONS;

-- Compare row counts between the new schema and the raw database
SELECT 
    'BOROUGH_CURATED' AS SOURCE,
    COUNT(*) AS ROW_COUNT
FROM NYC_NULL_BOROUGH_FIX.BOROUGH_CURATED.NYC_MOTOR_VEHICLE_COLLISIONS

UNION ALL

SELECT 
    'RAW DATASET' AS SOURCE,
    COUNT(*) AS ROW_COUNT
FROM NYC_COLLISIONS.RAW.NYC_MOTOR_VEHICLE_COLLISIONS;

--How many NULL BOROUGH values do we have to begin with?
SELECT 
    COUNT(*) AS TOTAL_ROWS,
    SUM(CASE WHEN BOROUGH IS NULL THEN 1 ELSE 0 END) AS NULL_BOROUGH_ROWS,
    ROUND(
        100.0 * SUM(CASE WHEN BOROUGH IS NULL THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS PCT_NULL_BOROUGH
FROM NYC_NULL_BOROUGH_FIX.BOROUGH_CURATED.NYC_MOTOR_VEHICLE_COLLISIONS;

--Let's see how many rows there are where BOROUGH is null but zip code is not null
SELECT COUNT(*) AS null_borough_with_zip
FROM NYC_NULL_BOROUGH_FIX.BOROUGH_CURATED.NYC_MOTOR_VEHICLE_COLLISIONS
WHERE BOROUGH IS NULL
  AND NULLIF(TRIM("ZIP CODE"), '') IS NOT NULL;

--Turns out there are 0 rows where BOROUGH is NULL but zip code is NOT NULL, maybe we can create a latitude/longitude map instead

SELECT 
    COUNT(*) AS NULL_BOROUGH_WITH_COORDS
FROM NYC_NULL_BOROUGH_FIX.BOROUGH_CURATED.NYC_MOTOR_VEHICLE_COLLISIONS
WHERE BOROUGH IS NULL
  AND LATITUDE IS NOT NULL
  AND LONGITUDE IS NOT NULL;

--Yes, there are 495,245 rows where we have lat/long coordinates but BOROUGH is NULL. We can make a Python script using GeoJSON  with the NYC Department of City Planning's Borough Boundries data to fill in missing BOROUGH and ZIP CODE columns based on their geographical coordinates. We'll have to update our project's .env from the raw dataset to the NYC_NULL_BOROUGH_FIX database. Let's make sure we're looking at the right Database/Schema/Table

USE DATABASE NYC_NULL_BOROUGH_FIX;
USE SCHEMA BOROUGH_CURATED;

SHOW TABLES;

--That looks good, we'll also add a column to our table with a boolean value to show whether the BOROUGH column was updated in this process for testing. 

--We'll run borough_by_coordinates.py now and run some queries to make sure the script worked as intended 

SELECT BOROUGH, COUNT(*) AS rows_filled
FROM NYC_NULL_BOROUGH_FIX.BOROUGH_CURATED.NYC_MOTOR_VEHICLE_COLLISIONS
WHERE BOROUGH_UPDATED_MANUALLY = TRUE
GROUP BY BOROUGH
ORDER BY rows_filled DESC;

--How many remaining NULLs have coordinates? 

SELECT
  SUM(CASE WHEN LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL THEN 1 ELSE 0 END) AS null_borough_with_coords,
  SUM(CASE WHEN LATITUDE IS NULL OR LONGITUDE IS NULL THEN 1 ELSE 0 END) AS null_borough_without_coords
FROM NYC_NULL_BOROUGH_FIX.BOROUGH_CURATED.NYC_MOTOR_VEHICLE_COLLISIONS
WHERE BOROUGH IS NULL;

--We'll spot check a few updated rows

SELECT COLLISION_ID, BOROUGH, "ZIP CODE", LATITUDE, LONGITUDE, BOROUGH_UPDATED_MANUALLY
FROM NYC_NULL_BOROUGH_FIX.BOROUGH_CURATED.NYC_MOTOR_VEHICLE_COLLISIONS
WHERE BOROUGH_UPDATED_MANUALLY = TRUE
LIMIT 20;







