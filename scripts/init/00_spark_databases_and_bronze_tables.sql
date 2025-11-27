-- Create core databases
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- Bronze tables (raw, NiFi targets)

CREATE TABLE IF NOT EXISTS bronze.district_bronze (
  A1   STRING,
  A2   STRING,
  A3   STRING,
  A4   STRING,
  A5   STRING,
  A6   STRING,
  A7   STRING,
  A8   STRING,
  A9   STRING,
  A10  STRING,
  A11  STRING,
  A12  STRING,
  A13  STRING,
  A14  STRING,
  A15  STRING,
  A16  STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bronze.client_bronze (
  client_id    STRING,
  birth_number STRING,
  district_id  STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bronze.account_bronze (
  account_id  STRING,
  district_id STRING,
  frequency   STRING,
  date        STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bronze.disp_bronze (
  disp_id    STRING,
  client_id  STRING,
  account_id STRING,
  type       STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bronze.card_bronze (
  card_id STRING,
  disp_id STRING,
  type    STRING,
  issued  STRING
)
STORED AS PARQUET;

