-- Simple BI queries for Berka gold layer (Trino)

-- Q1: Count of current clients and accounts
SELECT
  (SELECT COUNT(*) FROM gold.dim_client WHERE is_current = TRUE)  AS client_count,
  (SELECT COUNT(*) FROM gold.dim_account WHERE is_current = TRUE) AS account_count;

-- Q2: Average loan amount and duration by loan status
SELECT
  status,
  AVG(amount)   AS avg_loan_amount,
  AVG(duration) AS avg_duration_months,
  COUNT(*)      AS loan_count
FROM gold.fact_loan
GROUP BY status
ORDER BY status;

-- Q3: Daily transaction volume (last 30 days)
SELECT
  trans_date,
  COUNT(*)            AS transaction_count,
  SUM(amount)         AS total_amount,
  SUM(ABS(amount))    AS total_abs_amount
FROM gold.fact_trans
WHERE trans_date >= current_date - INTERVAL '30' DAY
GROUP BY trans_date
ORDER BY trans_date DESC;

