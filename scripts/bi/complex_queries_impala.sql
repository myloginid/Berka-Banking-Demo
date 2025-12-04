-- Complex BI queries for Berka gold layer (Impala)

-- Q1: Customer 360‑style summary per client
-- Combines dimensions and all three fact tables to give a consolidated view.
WITH base_clients AS (
  SELECT
    c.client_id,
    c.birth_number,
    c.district_id AS client_district_id
  FROM gold.dim_client c
  WHERE c.is_current = TRUE
),
accounts AS (
  SELECT
    a.account_id,
    a.district_id AS account_district_id,
    dd.client_id
  FROM gold.dim_account a
  JOIN gold.dim_disp dd
    ON a.account_id = dd.account_id
   AND dd.is_current = TRUE
  WHERE a.is_current = TRUE
),
loan_agg AS (
  SELECT
    client_id,
    COUNT(*)        AS loan_count,
    SUM(amount)     AS total_loan_amount,
    MAX(loan_date)  AS last_loan_date
  FROM gold.fact_loan
  GROUP BY client_id
),
order_agg AS (
  SELECT
    client_id,
    COUNT(*)        AS order_count,
    SUM(amount)     AS total_order_amount,
    MAX(ingest_ts)  AS last_order_ts
  FROM gold.fact_order
  GROUP BY client_id
),
trans_agg AS (
  SELECT
    client_id,
    COUNT(*)        AS trans_count,
    SUM(amount)     AS net_trans_amount,
    SUM(ABS(amount)) AS gross_trans_amount,
    MAX(trans_date) AS last_trans_date
  FROM gold.fact_trans
  GROUP BY client_id
)
SELECT
  bc.client_id,
  bc.birth_number,
  bc.client_district_id,
  COUNT(DISTINCT acc.account_id)             AS account_count,
  COALESCE(la.loan_count, 0)                 AS loan_count,
  COALESCE(la.total_loan_amount, 0.0)        AS total_loan_amount,
  COALESCE(oa.order_count, 0)                AS order_count,
  COALESCE(oa.total_order_amount, 0.0)       AS total_order_amount,
  COALESCE(ta.trans_count, 0)                AS trans_count,
  COALESCE(ta.net_trans_amount, 0.0)         AS net_trans_amount,
  GREATEST(
    COALESCE(la.last_loan_date, CAST('1900-01-01' AS DATE)),
    COALESCE(CAST(oa.last_order_ts AS DATE), CAST('1900-01-01' AS DATE)),
    COALESCE(ta.last_trans_date, CAST('1900-01-01' AS DATE))
  )                                          AS last_activity_date
FROM base_clients bc
LEFT JOIN accounts acc
  ON bc.client_id = acc.client_id
LEFT JOIN loan_agg la
  ON bc.client_id = la.client_id
LEFT JOIN order_agg oa
  ON bc.client_id = oa.client_id
LEFT JOIN trans_agg ta
  ON bc.client_id = ta.client_id
GROUP BY
  bc.client_id,
  bc.birth_number,
  bc.client_district_id,
  la.loan_count,
  la.total_loan_amount,
  oa.order_count,
  oa.total_order_amount,
  ta.trans_count,
  ta.net_trans_amount,
  la.last_loan_date,
  oa.last_order_ts,
  ta.last_trans_date;

-- Q2: 30‑day rolling transaction volume by region
WITH trans_with_region AS (
  SELECT
    t.trans_date,
    t.amount,
    d.region
  FROM gold.fact_trans t
  JOIN gold.dim_district d
    ON t.district_id = d.district_id
   AND d.is_current  = TRUE
  WHERE t.trans_date >= CURRENT_DATE() - INTERVAL 120 DAY
),
daily_region AS (
  SELECT
    region,
    trans_date,
    SUM(amount) AS daily_amount
  FROM trans_with_region
  GROUP BY region, trans_date
)
SELECT
  region,
  trans_date,
  daily_amount,
  SUM(daily_amount) OVER (
    PARTITION BY region
    ORDER BY trans_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS rolling_30d_amount
FROM daily_region
ORDER BY region, trans_date;

-- Q3: Top 20 districts by "active" customers with loans in the last 6 months
WITH recent_loans AS (
  SELECT DISTINCT
    client_id,
    district_id
  FROM gold.fact_loan
  WHERE loan_date >= CURRENT_DATE() - INTERVAL 6 MONTH
),
clients_per_district AS (
  SELECT
    district_id,
    COUNT(DISTINCT client_id) AS active_loan_clients
  FROM recent_loans
  GROUP BY district_id
)
SELECT
  d.region,
  d.district_name,
  cpd.active_loan_clients
FROM clients_per_district cpd
JOIN gold.dim_district d
  ON cpd.district_id = d.district_id
 AND d.is_current    = TRUE
ORDER BY cpd.active_loan_clients DESC
LIMIT 20;

-- Q4: Loan portfolio segmentation by duration and amount buckets
WITH segmented AS (
  SELECT
    CASE
      WHEN duration <= 12 THEN '0-12m'
      WHEN duration <= 36 THEN '13-36m'
      WHEN duration <= 60 THEN '37-60m'
      ELSE '60m+'
    END AS duration_bucket,
    CASE
      WHEN amount <  50000  THEN '<50k'
      WHEN amount < 150000  THEN '50k-150k'
      WHEN amount < 300000  THEN '150k-300k'
      ELSE '300k+'
    END AS amount_bucket,
    status,
    amount
  FROM gold.fact_loan
)
SELECT
  duration_bucket,
  amount_bucket,
  status,
  COUNT(*)      AS loan_count,
  SUM(amount)   AS total_amount,
  AVG(amount)   AS avg_amount
FROM segmented
GROUP BY duration_bucket, amount_bucket, status
ORDER BY duration_bucket, amount_bucket, status;

-- Q5: Customer profitability proxy (net cash flow) and top 50 customers
WITH loan_outflows AS (
  SELECT
    client_id,
    SUM(amount) AS total_loan_outflow
  FROM gold.fact_loan
  GROUP BY client_id
),
trans_net AS (
  SELECT
    client_id,
    SUM(amount) AS net_trans_amount
  FROM gold.fact_trans
  GROUP BY client_id
),
order_outflows AS (
  SELECT
    client_id,
    SUM(amount) AS total_order_outflow
  FROM gold.fact_order
  GROUP BY client_id
)
SELECT
  c.client_id,
  c.birth_number,
  COALESCE(lo.total_loan_outflow, 0.0)   AS total_loan_outflow,
  COALESCE(onf.total_order_outflow, 0.0) AS total_order_outflow,
  COALESCE(tn.net_trans_amount, 0.0)     AS net_trans_amount,
  COALESCE(tn.net_trans_amount, 0.0)
    - COALESCE(lo.total_loan_outflow, 0.0)
    - COALESCE(onf.total_order_outflow, 0.0) AS profitability_proxy
FROM gold.dim_client c
LEFT JOIN loan_outflows lo
  ON c.client_id = lo.client_id
LEFT JOIN trans_net tn
  ON c.client_id = tn.client_id
LEFT JOIN order_outflows onf
  ON c.client_id = onf.client_id
WHERE c.is_current = TRUE
ORDER BY profitability_proxy DESC
LIMIT 50;

