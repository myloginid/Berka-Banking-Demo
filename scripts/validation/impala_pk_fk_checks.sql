-- PK/FK validation queries for Berka demo (Impala).
-- Run this script in impala-shell against your Impala coordinator.

-- ---------------------------------------------------------------------------
-- 0. Basic row counts
-- ---------------------------------------------------------------------------

SELECT 'bronze.district_bronze' AS table_name, COUNT(*) AS row_count FROM bronze.district_bronze;
SELECT 'bronze.client_bronze'   AS table_name, COUNT(*) AS row_count FROM bronze.client_bronze;
SELECT 'bronze.account_bronze'  AS table_name, COUNT(*) AS row_count FROM bronze.account_bronze;
SELECT 'bronze.disp_bronze'     AS table_name, COUNT(*) AS row_count FROM bronze.disp_bronze;
SELECT 'bronze.card_bronze'     AS table_name, COUNT(*) AS row_count FROM bronze.card_bronze;

SELECT 'silver.district_silver' AS table_name, COUNT(*) AS row_count FROM silver.district_silver;
SELECT 'silver.client_silver'   AS table_name, COUNT(*) AS row_count FROM silver.client_silver;
SELECT 'silver.account_silver'  AS table_name, COUNT(*) AS row_count FROM silver.account_silver;
SELECT 'silver.disp_silver'     AS table_name, COUNT(*) AS row_count FROM silver.disp_silver;
SELECT 'silver.card_silver'     AS table_name, COUNT(*) AS row_count FROM silver.card_silver;

SELECT 'gold.dim_district'      AS table_name, COUNT(*) AS row_count FROM gold.dim_district;
SELECT 'gold.dim_client'        AS table_name, COUNT(*) AS row_count FROM gold.dim_client;
SELECT 'gold.dim_account'       AS table_name, COUNT(*) AS row_count FROM gold.dim_account;
SELECT 'gold.dim_disp'          AS table_name, COUNT(*) AS row_count FROM gold.dim_disp;
SELECT 'gold.dim_card'          AS table_name, COUNT(*) AS row_count FROM gold.dim_card;

SELECT 'gold.fact_loan'         AS table_name, COUNT(*) AS row_count FROM gold.fact_loan;
SELECT 'gold.fact_order'        AS table_name, COUNT(*) AS row_count FROM gold.fact_order;
SELECT 'gold.fact_trans'        AS table_name, COUNT(*) AS row_count FROM gold.fact_trans;

-- ---------------------------------------------------------------------------
-- 1. Bronze layer FK checks
-- ---------------------------------------------------------------------------

-- Clients with unknown district_id in bronze
SELECT COUNT(*) AS bad_client_district_fk
FROM bronze.client_bronze c
LEFT JOIN bronze.district_bronze d
  ON CAST(c.district_id AS INT) = CAST(d.A1 AS INT)
WHERE d.A1 IS NULL;

-- Accounts with unknown district_id in bronze
SELECT COUNT(*) AS bad_account_district_fk
FROM bronze.account_bronze a
LEFT JOIN bronze.district_bronze d
  ON CAST(a.district_id AS INT) = CAST(d.A1 AS INT)
WHERE d.A1 IS NULL;

-- Dispositions with unknown client/account in bronze
SELECT COUNT(*) AS bad_disp_client_fk
FROM bronze.disp_bronze disp
LEFT JOIN bronze.client_bronze c
  ON CAST(disp.client_id AS INT) = CAST(c.client_id AS INT)
WHERE c.client_id IS NULL;

SELECT COUNT(*) AS bad_disp_account_fk
FROM bronze.disp_bronze disp
LEFT JOIN bronze.account_bronze a
  ON CAST(disp.account_id AS INT) = CAST(a.account_id AS INT)
WHERE a.account_id IS NULL;

-- Cards with unknown disp in bronze
SELECT COUNT(*) AS bad_card_disp_fk
FROM bronze.card_bronze card
LEFT JOIN bronze.disp_bronze disp
  ON CAST(card.disp_id AS INT) = CAST(disp.disp_id AS INT)
WHERE disp.disp_id IS NULL;

-- ---------------------------------------------------------------------------
-- 2. Silver dimensions PK/FK checks
-- ---------------------------------------------------------------------------

-- District PK uniqueness
SELECT COUNT(*) AS total_districts,
       COUNT(DISTINCT district_id) AS distinct_district_ids
FROM silver.district_silver;

-- Client FK to silver.district_silver
SELECT COUNT(*) AS bad_client_district_fk_silver
FROM silver.client_silver c
LEFT JOIN silver.district_silver d
  ON c.district_id = d.district_id
WHERE d.district_id IS NULL;

-- Account FK to silver.district_silver
SELECT COUNT(*) AS bad_account_district_fk_silver
FROM silver.account_silver a
LEFT JOIN silver.district_silver d
  ON a.district_id = d.district_id
WHERE d.district_id IS NULL;

-- Disp FKs to client/account in silver
SELECT COUNT(*) AS bad_disp_client_fk_silver
FROM silver.disp_silver disp
LEFT JOIN silver.client_silver c
  ON disp.client_id = c.client_id
WHERE c.client_id IS NULL;

SELECT COUNT(*) AS bad_disp_account_fk_silver
FROM silver.disp_silver disp
LEFT JOIN silver.account_silver a
  ON disp.account_id = a.account_id
WHERE a.account_id IS NULL;

-- Card FK to disp in silver
SELECT COUNT(*) AS bad_card_disp_fk_silver
FROM silver.card_silver card
LEFT JOIN silver.disp_silver disp
  ON card.disp_id = disp.disp_id
WHERE disp.disp_id IS NULL;

-- ---------------------------------------------------------------------------
-- 3. Gold dimensions PK/FK checks (is_current rows)
-- ---------------------------------------------------------------------------

-- dim_client FK to dim_district
SELECT COUNT(*) AS bad_dim_client_district_fk
FROM gold.dim_client c
LEFT JOIN gold.dim_district d
  ON c.district_id = d.district_id
WHERE d.district_id IS NULL
  AND c.is_current = TRUE;

-- dim_account FK to dim_district
SELECT COUNT(*) AS bad_dim_account_district_fk
FROM gold.dim_account a
LEFT JOIN gold.dim_district d
  ON a.district_id = d.district_id
WHERE d.district_id IS NULL
  AND a.is_current = TRUE;

-- dim_disp FKs to dim_client and dim_account
SELECT COUNT(*) AS bad_dim_disp_client_fk
FROM gold.dim_disp disp
LEFT JOIN gold.dim_client c
  ON disp.client_id = c.client_id AND c.is_current = TRUE
WHERE c.client_id IS NULL
  AND disp.is_current = TRUE;

SELECT COUNT(*) AS bad_dim_disp_account_fk
FROM gold.dim_disp disp
LEFT JOIN gold.dim_account a
  ON disp.account_id = a.account_id AND a.is_current = TRUE
WHERE a.account_id IS NULL
  AND disp.is_current = TRUE;

-- dim_card FK to dim_disp
SELECT COUNT(*) AS bad_dim_card_disp_fk
FROM gold.dim_card card
LEFT JOIN gold.dim_disp disp
  ON card.disp_id = disp.disp_id AND disp.is_current = TRUE
WHERE disp.disp_id IS NULL
  AND card.is_current = TRUE;

-- ---------------------------------------------------------------------------
-- 4. Fact tables vs gold dimensions
-- ---------------------------------------------------------------------------

-- fact_loan vs dim_account / dim_client / dim_disp
SELECT COUNT(*) AS bad_fact_loan_account_fk
FROM gold.fact_loan f
LEFT JOIN gold.dim_account a
  ON f.account_id = a.account_id AND a.is_current = TRUE
WHERE a.account_id IS NULL;

SELECT COUNT(*) AS bad_fact_loan_client_fk
FROM gold.fact_loan f
LEFT JOIN gold.dim_client c
  ON f.client_id = c.client_id AND c.is_current = TRUE
WHERE c.client_id IS NULL;

SELECT COUNT(*) AS bad_fact_loan_disp_fk
FROM gold.fact_loan f
LEFT JOIN gold.dim_disp disp
  ON f.disp_id = disp.disp_id AND disp.is_current = TRUE
WHERE disp.disp_id IS NULL;

-- fact_order vs dim_account / dim_client / dim_disp
SELECT COUNT(*) AS bad_fact_order_account_fk
FROM gold.fact_order f
LEFT JOIN gold.dim_account a
  ON f.account_id = a.account_id AND a.is_current = TRUE
WHERE a.account_id IS NULL;

SELECT COUNT(*) AS bad_fact_order_client_fk
FROM gold.fact_order f
LEFT JOIN gold.dim_client c
  ON f.client_id = c.client_id AND c.is_current = TRUE
WHERE c.client_id IS NULL;

SELECT COUNT(*) AS bad_fact_order_disp_fk
FROM gold.fact_order f
LEFT JOIN gold.dim_disp disp
  ON f.disp_id = disp.disp_id AND disp.is_current = TRUE
WHERE disp.disp_id IS NULL;

-- fact_trans vs dim_account / dim_client / dim_disp
SELECT COUNT(*) AS bad_fact_trans_account_fk
FROM gold.fact_trans f
LEFT JOIN gold.dim_account a
  ON f.account_id = a.account_id AND a.is_current = TRUE
WHERE a.account_id IS NULL;

SELECT COUNT(*) AS bad_fact_trans_client_fk
FROM gold.fact_trans f
LEFT JOIN gold.dim_client c
  ON f.client_id = c.client_id AND c.is_current = TRUE
WHERE c.client_id IS NULL;

SELECT COUNT(*) AS bad_fact_trans_disp_fk
FROM gold.fact_trans f
LEFT JOIN gold.dim_disp disp
  ON f.disp_id = disp.disp_id AND disp.is_current = TRUE
WHERE disp.disp_id IS NULL;

