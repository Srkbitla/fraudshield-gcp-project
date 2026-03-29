# Architecture Deep Dive

## Project summary

FraudShield is a real-time payment fraud detection platform on GCP. It ingests transaction authorization events from Pub/Sub, validates and enriches them in Dataflow, writes raw and operational outputs into BigQuery, and uses Cloud Composer to orchestrate downstream fraud KPI and data quality SQL jobs.

## Functional requirements

- ingest card payment events continuously,
- identify suspicious transactions fast enough for operational response,
- separate invalid events from valid transactions,
- expose curated transaction-level analytics,
- generate merchant and country-level fraud KPI tables,
- keep the implementation easy to explain in interviews.

## Architecture decisions

### Pub/Sub for transaction ingestion

Pub/Sub provides the real-time event bus between payment producers and the streaming pipeline. It keeps the producer and consumer layers decoupled and integrates naturally with Dataflow.

### Dataflow for streaming validation and scoring

The streaming pipeline performs:

- JSON parsing and schema validation,
- normalization of timestamps and categorical values,
- deduplication by `transaction_id`,
- fraud rule scoring,
- routing of malformed events to Cloud Storage dead-letter files,
- writing validated transactions to BigQuery bronze,
- writing high-risk transactions to an alerts table,
- aggregating one-minute fraud metrics for operations dashboards.

### BigQuery for storage and analytics

The warehouse uses a bronze, silver, gold, and ops pattern:

- bronze: validated raw transaction events
- silver: curated transaction-level table with stable business fields and risk interpretation
- gold: merchant and country fraud KPI tables
- ops: live metrics, flagged transactions, and data quality audit results

This layout makes the project easy to present and reflects real-world analytical platform design.

## End-to-end data flow

1. A Python simulator generates payment authorization events with realistic amounts, countries, device behavior, and occasional fraud patterns.
2. Events are published to the `fraud-transactions` Pub/Sub topic.
3. Dataflow consumes the stream and validates required fields and business rules.
4. Invalid records are written to Cloud Storage for replay.
5. Valid records are normalized, deduplicated, and risk-scored.
6. All valid transactions are written to `bronze.payment_transactions`.
7. High-risk transactions are written to `ops.flagged_transactions`.
8. One-minute metrics are written to `ops.realtime_fraud_metrics`.
9. Composer runs scheduled SQL to build `silver.transactions_curated`, `gold.merchant_risk_hourly_kpis`, and `gold.country_fraud_hotspots`.
10. Data quality checks are written to `ops.data_quality_audit`.

## Fraud scoring approach

This project uses explainable rule-based scoring rather than a black-box model. The score is derived from:

- transaction amount,
- mismatch between cardholder home country and transaction country,
- card-not-present usage,
- new device indicator,
- risky merchant categories,
- declined status or suspicious approval behavior.

The rules produce:

- `risk_score`
- `risk_level`
- `risk_reasons`

This approach is strong for interviews because every flag is easy to justify.

## Table design

### bronze.payment_transactions

Purpose:
Store validated transactions at event grain.

Partitioning:
`transaction_date`

Clustering:
`risk_level`, `txn_country`, `merchant_category`

### ops.flagged_transactions

Purpose:
Expose transactions that require operational review.

Use case:
Fraud analysts or alert dashboards can query this table directly.

### ops.realtime_fraud_metrics

Purpose:
Support low-latency fraud monitoring with one-minute windows.

Metrics:

- transaction count
- approved amount
- declined transaction count
- high-risk transaction count
- suspicious amount share
- average risk score

### silver.transactions_curated

Purpose:
Create a clean, analyst-friendly transaction-level table with readable risk interpretation.

### gold.merchant_risk_hourly_kpis

Purpose:
Show hourly fraud trends by merchant and merchant category.

### gold.country_fraud_hotspots

Purpose:
Highlight countries with elevated suspicious transaction share and decline rate.

## Data quality strategy

### Stream-time validation

- malformed JSON is rejected,
- required fields are checked,
- amount must be positive,
- currency must be ISO-like,
- transaction timestamp must be parseable and not in the future,
- country and merchant identifiers must exist.

### Warehouse checks

- null primary keys,
- duplicate transaction IDs in silver,
- negative approved amounts,
- suspiciously high risk score counts,
- missing country or merchant attributes.

Results are inserted into `ops.data_quality_audit`.

## Reliability and performance

- Dead-letter path for malformed events
- Deduplication keyed by `transaction_id`
- Partitioned and clustered BigQuery tables
- Gold tables that reduce repeated scans of event-level data
- Direct streaming alerts table to separate operational use cases from analytics

## Production extensions you can mention

- feature store for customer/device velocity history,
- machine learning scoring using Vertex AI or BigQuery ML,
- alert routing to Security Command Center or Pub/Sub notifications,
- CI/CD for DAG and setup scripts,
- dbt tests for silver and gold models,
- Dataplex or Data Catalog for metadata and governance.

