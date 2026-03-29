CREATE OR REPLACE TABLE `your-gcp-project-id.gold.merchant_risk_hourly_kpis`
PARTITION BY DATE(window_hour)
CLUSTER BY merchant_category, merchant_id AS
SELECT
  TIMESTAMP_TRUNC(transaction_ts, HOUR) AS window_hour,
  merchant_id,
  merchant_category,
  COUNT(*) AS total_transactions,
  ROUND(SUM(amount), 2) AS gross_amount,
  COUNTIF(risk_level = 'HIGH') AS high_risk_transactions,
  COUNTIF(txn_status = 'DECLINED') AS declined_transactions,
  COUNTIF(simulated_fraud_label) AS simulated_fraud_transactions,
  ROUND(AVG(risk_score), 2) AS avg_risk_score,
  ROUND(SAFE_DIVIDE(COUNTIF(risk_level = 'HIGH'), COUNT(*)) * 100, 2) AS high_risk_pct
FROM `your-gcp-project-id.silver.transactions_curated`
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE `your-gcp-project-id.gold.country_fraud_hotspots`
PARTITION BY transaction_date
CLUSTER BY txn_country AS
SELECT
  transaction_date,
  txn_country,
  COUNT(*) AS total_transactions,
  ROUND(SUM(amount), 2) AS total_amount,
  COUNTIF(risk_level = 'HIGH') AS high_risk_transactions,
  COUNTIF(txn_status = 'DECLINED') AS declined_transactions,
  COUNTIF(simulated_fraud_label) AS simulated_fraud_transactions,
  ROUND(AVG(risk_score), 2) AS avg_risk_score,
  ROUND(SAFE_DIVIDE(COUNTIF(risk_level = 'HIGH'), COUNT(*)) * 100, 2) AS high_risk_pct,
  ROUND(SAFE_DIVIDE(COUNTIF(txn_status = 'DECLINED'), COUNT(*)) * 100, 2) AS decline_pct
FROM `your-gcp-project-id.silver.transactions_curated`
GROUP BY 1, 2;

