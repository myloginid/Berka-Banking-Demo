-- Import gold-layer dimensions and facts from AWS (S3) into:
--   1) Hive-style external Parquet tables (staging database)
--   2) Iceberg tables in the `gold` database.
--
-- Prerequisites:
--   - The Parquet directories exported by 30_export_gold_to_parquet.sql have
--     been copied from ADLS to S3, preserving the per-table subdirectories,
--     for example:
--       s3a://<bucket>/<prefix>/gold_export/dim_district
--       s3a://<bucket>/<prefix>/gold_export/dim_client
--       ...
--   - This script is intended to be run with Spark SQL (spark-sql or
--     spark.sql(...)) in an environment where Iceberg is configured for the
--     `spark_catalog` / `gold` database.
--
-- NOTE: Replace S3_BASE below with your actual S3 base path.
-- Example:
--   SET S3_BASE='s3a://my-bucket/berka/gold_export';

-- ---------------------------------------------------------------------------
-- Configuration: S3 base path (edit this line before running)
-- ---------------------------------------------------------------------------

SET S3_BASE='s3a://maybankpoc-buk-f1311532/copyfromazure/gold_export.db';

-- ---------------------------------------------------------------------------
-- Staging database: external Parquet tables on S3
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS gold_stage;

-- Dimensions (staging with explicit schemas)

DROP TABLE IF EXISTS gold_stage.dim_district_stage;
CREATE EXTERNAL TABLE gold_stage.dim_district_stage (
  district_id INT,
  district_name STRING,
  region STRING,
  A4 INT,
  A5 INT,
  A6 INT,
  A7 INT,
  A8 INT,
  A9 INT,
  A10 DOUBLE,
  A11 INT,
  A12 DOUBLE,
  A13 DOUBLE,
  A14 INT,
  A15 INT,
  A16 INT,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN,
  scd_version INT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_district';

DROP TABLE IF EXISTS gold_stage.dim_client_stage;
CREATE EXTERNAL TABLE gold_stage.dim_client_stage (
  client_id INT,
  birth_number STRING,
  district_id INT,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN,
  scd_version INT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_client';

DROP TABLE IF EXISTS gold_stage.dim_account_stage;
CREATE EXTERNAL TABLE gold_stage.dim_account_stage (
  account_id INT,
  district_id INT,
  frequency STRING,
  open_date DATE,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN,
  scd_version INT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_account';

DROP TABLE IF EXISTS gold_stage.dim_disp_stage;
CREATE EXTERNAL TABLE gold_stage.dim_disp_stage (
  disp_id INT,
  client_id INT,
  account_id INT,
  disp_type STRING,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN,
  scd_version INT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_disp';

DROP TABLE IF EXISTS gold_stage.dim_card_stage;
CREATE EXTERNAL TABLE gold_stage.dim_card_stage (
  card_id INT,
  disp_id INT,
  card_type STRING,
  issued_at TIMESTAMP,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN,
  scd_version INT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_card';

-- Facts (staging with explicit schemas)

DROP TABLE IF EXISTS gold_stage.fact_loan_stage;
CREATE EXTERNAL TABLE gold_stage.fact_loan_stage (
  loan_id BIGINT,
  account_id INT,
  client_id INT,
  disp_id INT,
  district_id INT,
  loan_date DATE,
  amount DOUBLE,
  duration INT,
  payments DOUBLE,
  status STRING,
  account_dim_district_id INT,
  ingest_ts TIMESTAMP,
  batch_id BIGINT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/fact_loan';

DROP TABLE IF EXISTS gold_stage.fact_order_stage;
CREATE EXTERNAL TABLE gold_stage.fact_order_stage (
  order_id BIGINT,
  account_id INT,
  client_id INT,
  disp_id INT,
  district_id INT,
  bank_to STRING,
  account_to STRING,
  amount DOUBLE,
  k_symbol STRING,
  account_dim_district_id INT,
  ingest_ts TIMESTAMP,
  batch_id BIGINT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/fact_order';

DROP TABLE IF EXISTS gold_stage.fact_trans_stage;
CREATE EXTERNAL TABLE gold_stage.fact_trans_stage (
  trans_id BIGINT,
  account_id INT,
  client_id INT,
  disp_id INT,
  district_id INT,
  trans_date DATE,
  trans_type STRING,
  operation STRING,
  amount DOUBLE,
  balance DOUBLE,
  k_symbol STRING,
  bank STRING,
  account STRING,
  account_dim_district_id INT,
  ingest_ts TIMESTAMP,
  batch_id BIGINT
)
STORED AS PARQUET
LOCATION '${S3_BASE}/fact_trans';

-- ---------------------------------------------------------------------------
-- Iceberg tables in `gold` database (created from staging tables)
-- ---------------------------------------------------------------------------

-- Dimensions (Iceberg)

DROP TABLE IF EXISTS gold.dim_district;
CREATE TABLE gold.dim_district
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_district_stage;

DROP TABLE IF EXISTS gold.dim_client;
CREATE TABLE gold.dim_client
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_client_stage;

DROP TABLE IF EXISTS gold.dim_account;
CREATE TABLE gold.dim_account
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_account_stage;

DROP TABLE IF EXISTS gold.dim_disp;
CREATE TABLE gold.dim_disp
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_disp_stage;

DROP TABLE IF EXISTS gold.dim_card;
CREATE TABLE gold.dim_card
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_card_stage;

-- Facts (Iceberg)

DROP TABLE IF EXISTS gold.fact_loan;
CREATE TABLE gold.fact_loan
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.fact_loan_stage;

DROP TABLE IF EXISTS gold.fact_order;
CREATE TABLE gold.fact_order
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.fact_order_stage;

DROP TABLE IF EXISTS gold.fact_trans;
CREATE TABLE gold.fact_trans
STORED BY ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.fact_trans_stage;

-- ---------------------------------------------------------------------------
-- Data validation: Check row counts in gold tables
-- ---------------------------------------------------------------------------

-- Dimensions
SELECT 'dim_district' AS table_name, COUNT(*) AS row_count FROM gold.dim_district
UNION ALL
SELECT 'dim_client' AS table_name, COUNT(*) AS row_count FROM gold.dim_client
UNION ALL
SELECT 'dim_account' AS table_name, COUNT(*) AS row_count FROM gold.dim_account
UNION ALL
SELECT 'dim_disp' AS table_name, COUNT(*) AS row_count FROM gold.dim_disp
UNION ALL
SELECT 'dim_card' AS table_name, COUNT(*) AS row_count FROM gold.dim_card
UNION ALL
-- Facts
SELECT 'fact_loan' AS table_name, COUNT(*) AS row_count FROM gold.fact_loan
UNION ALL
SELECT 'fact_order' AS table_name, COUNT(*) AS row_count FROM gold.fact_order
UNION ALL
SELECT 'fact_trans' AS table_name, COUNT(*) AS row_count FROM gold.fact_trans
ORDER BY table_name;
