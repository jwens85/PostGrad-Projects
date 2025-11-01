--Create a xsmall compute warehouse
CREATE WAREHOUSE IF NOT EXISTS wh_etl
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

--Create NYC collisions database and schema
CREATE DATABASE IF NOT EXISTS nyc_collisions;
CREATE SCHEMA IF NOT EXISTS nyc_collisions.raw;

--Set as active for this session
USE WAREHOUSE wh_etl;
USE DATABASE nyc_collisions;
USE SCHEMA raw;
