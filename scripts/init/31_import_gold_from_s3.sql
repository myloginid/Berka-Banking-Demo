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

SET S3_BASE='s3a://my-bucket/berka/gold_export';

-- ---------------------------------------------------------------------------
-- Staging database: external Parquet tables on S3
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS gold_stage;

-- Dimensions (staging)

DROP TABLE IF EXISTS gold_stage.dim_district_stage;
CREATE EXTERNAL TABLE gold_stage.dim_district_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_district';

DROP TABLE IF EXISTS gold_stage.dim_client_stage;
CREATE EXTERNAL TABLE gold_stage.dim_client_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_client';

DROP TABLE IF EXISTS gold_stage.dim_account_stage;
CREATE EXTERNAL TABLE gold_stage.dim_account_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_account';

DROP TABLE IF EXISTS gold_stage.dim_disp_stage;
CREATE EXTERNAL TABLE gold_stage.dim_disp_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_disp';

DROP TABLE IF EXISTS gold_stage.dim_card_stage;
CREATE EXTERNAL TABLE gold_stage.dim_card_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/dim_card';

-- Facts (staging)

DROP TABLE IF EXISTS gold_stage.fact_loan_stage;
CREATE EXTERNAL TABLE gold_stage.fact_loan_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/fact_loan';

DROP TABLE IF EXISTS gold_stage.fact_order_stage;
CREATE EXTERNAL TABLE gold_stage.fact_order_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/fact_order';

DROP TABLE IF EXISTS gold_stage.fact_trans_stage;
CREATE EXTERNAL TABLE gold_stage.fact_trans_stage
STORED AS PARQUET
LOCATION '${S3_BASE}/fact_trans';

-- ---------------------------------------------------------------------------
-- Iceberg tables in `gold` database (created from staging tables)
-- ---------------------------------------------------------------------------

-- Dimensions (Iceberg)

DROP TABLE IF EXISTS gold.dim_district;
CREATE TABLE gold.dim_district
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_district_stage;

DROP TABLE IF EXISTS gold.dim_client;
CREATE TABLE gold.dim_client
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_client_stage;

DROP TABLE IF EXISTS gold.dim_account;
CREATE TABLE gold.dim_account
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_account_stage;

DROP TABLE IF EXISTS gold.dim_disp;
CREATE TABLE gold.dim_disp
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_disp_stage;

DROP TABLE IF EXISTS gold.dim_card;
CREATE TABLE gold.dim_card
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.dim_card_stage;

-- Facts (Iceberg)

DROP TABLE IF EXISTS gold.fact_loan;
CREATE TABLE gold.fact_loan
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.fact_loan_stage;

DROP TABLE IF EXISTS gold.fact_order;
CREATE TABLE gold.fact_order
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.fact_order_stage;

DROP TABLE IF EXISTS gold.fact_trans;
CREATE TABLE gold.fact_trans
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
AS
SELECT * FROM gold_stage.fact_trans_stage;

