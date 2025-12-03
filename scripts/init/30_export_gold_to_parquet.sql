-- Create an export-friendly database with Hive-style external Parquet tables
-- for all gold-layer dimensions and facts. The data is written to a dedicated
-- directory under the ABFS warehouse so it can be copied out (for example,
-- from ADLS to AWS).

CREATE DATABASE IF NOT EXISTS gold_export
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db';

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS gold_export.dim_district;
CREATE EXTERNAL TABLE gold_export.dim_district
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/dim_district'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.dim_district;

DROP TABLE IF EXISTS gold_export.dim_client;
CREATE EXTERNAL TABLE gold_export.dim_client
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/dim_client'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.dim_client;

DROP TABLE IF EXISTS gold_export.dim_account;
CREATE EXTERNAL TABLE gold_export.dim_account
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/dim_account'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.dim_account;

DROP TABLE IF EXISTS gold_export.dim_disp;
CREATE EXTERNAL TABLE gold_export.dim_disp
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/dim_disp'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.dim_disp;

DROP TABLE IF EXISTS gold_export.dim_card;
CREATE EXTERNAL TABLE gold_export.dim_card
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/dim_card'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.dim_card;

-- ---------------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS gold_export.fact_loan;
CREATE EXTERNAL TABLE gold_export.fact_loan
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/fact_loan'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.fact_loan;

DROP TABLE IF EXISTS gold_export.fact_order;
CREATE EXTERNAL TABLE gold_export.fact_order
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/fact_order'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.fact_order;

DROP TABLE IF EXISTS gold_export.fact_trans;
CREATE EXTERNAL TABLE gold_export.fact_trans
STORED AS PARQUET
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/warehouse/tablespace/external/hive/gold_export.db/fact_trans'
TBLPROPERTIES ('external.table.purge'='true')
AS
SELECT *
FROM gold.fact_trans;
