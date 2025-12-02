-- Load dimension data from CSV-backed external tables into bronze layer tables.
-- This script assumes that:
--   - Database `csv` exists and external tables {district,client,account,disp,card}_external
--     have been created by 05_external_berka_csv_tables.sql
--   - Database `bronze` and the corresponding *_bronze tables were created by
--     00_spark_databases_and_bronze_tables.sql

-- District
INSERT OVERWRITE TABLE bronze.district_bronze
SELECT
  REPLACE(A1,  '"', '') AS A1,
  REPLACE(A2,  '"', '') AS A2,
  REPLACE(A3,  '"', '') AS A3,
  REPLACE(A4,  '"', '') AS A4,
  REPLACE(A5,  '"', '') AS A5,
  REPLACE(A6,  '"', '') AS A6,
  REPLACE(A7,  '"', '') AS A7,
  REPLACE(A8,  '"', '') AS A8,
  REPLACE(A9,  '"', '') AS A9,
  REPLACE(A10, '"', '') AS A10,
  REPLACE(A11, '"', '') AS A11,
  REPLACE(A12, '"', '') AS A12,
  REPLACE(A13, '"', '') AS A13,
  REPLACE(A14, '"', '') AS A14,
  REPLACE(A15, '"', '') AS A15,
  REPLACE(A16, '"', '') AS A16
FROM csv.district_external;

-- Client
INSERT OVERWRITE TABLE bronze.client_bronze
SELECT
  REPLACE(client_id,    '"', '') AS client_id,
  REPLACE(birth_number, '"', '') AS birth_number,
  REPLACE(district_id,  '"', '') AS district_id
FROM csv.client_external;

-- Account
INSERT OVERWRITE TABLE bronze.account_bronze
SELECT
  REPLACE(account_id,   '"', '') AS account_id,
  REPLACE(district_id,  '"', '') AS district_id,
  REPLACE(frequency,    '"', '') AS frequency,
  REPLACE(created_date, '"', '') AS created_date
FROM csv.account_external;

-- Disp
INSERT OVERWRITE TABLE bronze.disp_bronze
SELECT
  REPLACE(disp_id,    '"', '') AS disp_id,
  REPLACE(client_id,  '"', '') AS client_id,
  REPLACE(account_id, '"', '') AS account_id,
  REPLACE(type,       '"', '') AS type
FROM csv.disp_external;

-- Card
INSERT OVERWRITE TABLE bronze.card_bronze
SELECT
  REPLACE(card_id, '"', '') AS card_id,
  REPLACE(disp_id, '"', '') AS disp_id,
  REPLACE(type,    '"', '') AS type,
  REPLACE(issued,  '"', '') AS issued
FROM csv.card_external;
