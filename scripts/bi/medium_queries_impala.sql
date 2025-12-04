-- Medium‑complexity BI queries for Berka gold layer (Impala)

-- Q1: Loan portfolio by district and region
SELECT
  d.region,
  d.district_name,
  COUNT(*)        AS loan_count,
  SUM(l.amount)   AS total_loan_amount,
  AVG(l.amount)   AS avg_loan_amount
FROM gold.fact_loan l
JOIN gold.dim_district d
  ON l.district_id = d.district_id
 AND d.is_current  = TRUE
GROUP BY d.region, d.district_name
ORDER BY total_loan_amount DESC;

-- Q2: Top 10 accounts by total outgoing transaction amount
SELECT
  account_id,
  SUM(CASE WHEN trans_type = 'VYDAJ' THEN amount ELSE 0 END) AS outgoing_amount,
  COUNT(*)                                                    AS txn_count
FROM gold.fact_trans
GROUP BY account_id
HAVING SUM(CASE WHEN trans_type = 'VYDAJ' THEN amount ELSE 0 END) > 0
ORDER BY outgoing_amount DESC
LIMIT 10;

-- Q3: Monthly loan originations (count and amount)
SELECT
  TRUNC(loan_date, 'MONTH') AS loan_month,
  COUNT(*)                  AS loan_count,
  SUM(amount)               AS total_loan_amount,
  AVG(amount)               AS avg_loan_amount
FROM gold.fact_loan
GROUP BY TRUNC(loan_date, 'MONTH')
ORDER BY loan_month;

-- Q4: Payment order volume by purpose (k_symbol)
SELECT
  COALESCE(k_symbol, '(none)') AS k_symbol,
  COUNT(*)                     AS order_count,
  SUM(amount)                  AS total_amount,
  AVG(amount)                  AS avg_amount
FROM gold.fact_order
GROUP BY COALESCE(k_symbol, '(none)')
ORDER BY total_amount DESC;

-- Q5: Card penetration by district (share of clients with at least one card)
WITH client_cards AS (
  SELECT DISTINCT
    dc.client_id,
    dc.district_id
  FROM gold.dim_disp   dd
  JOIN gold.dim_client dc
    ON dd.client_id = dc.client_id
   AND dc.is_current = TRUE
  JOIN gold.dim_card  c
    ON dd.disp_id = c.disp_id
   AND c.is_current = TRUE
  WHERE dd.is_current = TRUE
),
clients_per_district AS (
  SELECT
    district_id,
    COUNT(DISTINCT client_id) AS total_clients
  FROM gold.dim_client
  WHERE is_current = TRUE
  GROUP BY district_id
),
clients_with_card AS (
  SELECT
    district_id,
    COUNT(DISTINCT client_id) AS clients_with_card
  FROM client_cards
  GROUP BY district_id
)
SELECT
  d.region,
  d.district_name,
  cpd.total_clients,
  COALESCE(cwc.clients_with_card, 0)                  AS clients_with_card,
  COALESCE(cwc.clients_with_card, 0) * 1.0 /
    NULLIF(cpd.total_clients, 0)                      AS card_penetration_ratio
FROM clients_per_district cpd
JOIN gold.dim_district d
  ON cpd.district_id = d.district_id
 AND d.is_current    = TRUE
LEFT JOIN clients_with_card cwc
  ON cpd.district_id = cwc.district_id
ORDER BY card_penetration_ratio DESC;

