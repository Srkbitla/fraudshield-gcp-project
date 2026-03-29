DECLARE run_date DATE DEFAULT CURRENT_DATE();

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'null_transaction_id_in_silver',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  COUNT(*),
  'transactions_curated should not contain null transaction_id values.',
  CURRENT_TIMESTAMP()
FROM `your-gcp-project-id.silver.transactions_curated`
WHERE transaction_id IS NULL;

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'duplicate_transaction_id_in_silver',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  COUNT(*),
  'Each transaction_id should be unique in the curated transaction table.',
  CURRENT_TIMESTAMP()
FROM (
  SELECT transaction_id
  FROM `your-gcp-project-id.silver.transactions_curated`
  GROUP BY transaction_id
  HAVING COUNT(*) > 1
);

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'negative_transaction_amount',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  COUNT(*),
  'Transaction amounts must remain positive.',
  CURRENT_TIMESTAMP()
FROM `your-gcp-project-id.silver.transactions_curated`
WHERE amount <= 0;

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'risk_score_above_100',
  IF(COUNT(*) = 0, 'PASS', 'WARN'),
  COUNT(*),
  'Risk score should stay within the expected rule-based range.',
  CURRENT_TIMESTAMP()
FROM `your-gcp-project-id.silver.transactions_curated`
WHERE risk_score > 100;
