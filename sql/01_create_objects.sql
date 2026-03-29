DECLARE project_id STRING DEFAULT 'your-gcp-project-id';

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.bronze`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.silver`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.gold`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.ops`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.bronze.payment_transactions`
(
  transaction_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  card_id STRING NOT NULL,
  merchant_id STRING NOT NULL,
  merchant_category STRING NOT NULL,
  amount FLOAT64 NOT NULL,
  currency STRING NOT NULL,
  home_country STRING NOT NULL,
  txn_country STRING NOT NULL,
  city STRING NOT NULL,
  device_id STRING NOT NULL,
  payment_method STRING NOT NULL,
  transaction_ts TIMESTAMP NOT NULL,
  transaction_date DATE NOT NULL,
  txn_status STRING NOT NULL,
  is_card_present BOOL NOT NULL,
  is_new_device BOOL NOT NULL,
  simulated_fraud_label BOOL NOT NULL,
  risk_score INT64 NOT NULL,
  risk_level STRING NOT NULL,
  risk_reasons ARRAY<STRING>,
  ingest_source STRING NOT NULL,
  processing_ts TIMESTAMP NOT NULL,
  is_late_arrival BOOL NOT NULL
)
PARTITION BY transaction_date
CLUSTER BY risk_level, txn_country, merchant_category
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.ops.flagged_transactions`
(
  transaction_id STRING NOT NULL,
  transaction_ts TIMESTAMP NOT NULL,
  customer_id STRING NOT NULL,
  merchant_id STRING NOT NULL,
  merchant_category STRING NOT NULL,
  amount FLOAT64 NOT NULL,
  txn_country STRING NOT NULL,
  txn_status STRING NOT NULL,
  risk_score INT64 NOT NULL,
  risk_level STRING NOT NULL,
  risk_reasons ARRAY<STRING>,
  alert_created_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(transaction_ts)
CLUSTER BY risk_level, txn_country, merchant_category
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.ops.realtime_fraud_metrics`
(
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  txn_country STRING NOT NULL,
  merchant_category STRING NOT NULL,
  total_transactions INT64 NOT NULL,
  approved_amount FLOAT64 NOT NULL,
  declined_transactions INT64 NOT NULL,
  high_risk_transactions INT64 NOT NULL,
  simulated_fraud_transactions INT64 NOT NULL,
  avg_risk_score FLOAT64 NOT NULL,
  pipeline_run_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY txn_country, merchant_category
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.ops.data_quality_audit`
(
  run_date DATE NOT NULL,
  check_name STRING NOT NULL,
  status STRING NOT NULL,
  failed_rows INT64 NOT NULL,
  details STRING,
  created_ts TIMESTAMP NOT NULL
)
PARTITION BY run_date
CLUSTER BY check_name, status
""", project_id);

