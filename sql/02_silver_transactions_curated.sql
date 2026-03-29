CREATE OR REPLACE TABLE `your-gcp-project-id.silver.transactions_curated`
PARTITION BY transaction_date
CLUSTER BY risk_level, txn_country, merchant_category AS
SELECT
  transaction_id,
  customer_id,
  card_id,
  merchant_id,
  merchant_category,
  amount,
  currency,
  home_country,
  txn_country,
  city,
  device_id,
  payment_method,
  transaction_ts,
  transaction_date,
  txn_status,
  is_card_present,
  is_new_device,
  simulated_fraud_label,
  risk_score,
  risk_level,
  risk_reasons,
  IF(txn_country != home_country, TRUE, FALSE) AS is_cross_border,
  IF(txn_status = 'DECLINED', TRUE, FALSE) AS is_declined,
  IF(risk_score >= 80, TRUE, FALSE) AS requires_manual_review,
  processing_ts
FROM `your-gcp-project-id.bronze.payment_transactions`
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_ts DESC) = 1;

