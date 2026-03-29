# Interview Guide

## 60-second summary

I built a real-time fraud detection pipeline on GCP where payment transactions stream through Pub/Sub, are validated and risk-scored by Dataflow, and land in BigQuery using bronze, silver, gold, and ops layers. The pipeline writes high-risk transactions to an operational alerts table, maintains one-minute fraud metrics for dashboards, and uses Cloud Composer for downstream KPI and data quality SQL orchestration. I also documented both `gcloud` and manual GCP setup so the project is reproducible and easy to demo.

## Common interview questions

### Why is this a good real-time use case?

Fraud decisions lose value if they arrive late. A suspicious transaction should be identified quickly enough for review, blocking, or downstream alerting, which makes streaming a strong architectural fit.

### Why Dataflow instead of Dataproc?

Dataflow is a better fit for continuous event processing and managed streaming ETL. Dataproc is stronger when you need Spark or large batch workloads, but for this project the real-time ingestion and windowed metrics pattern fits Dataflow well.

### How are suspicious transactions identified?

Each transaction gets a rule-based `risk_score` from factors like amount, cross-border activity, new device usage, card-not-present behavior, and risky merchant categories. Based on that score, the transaction is assigned a `risk_level` and can be written to the alerts table.

### How do you handle bad data?

Malformed or invalid records are separated from the main stream and written to Cloud Storage dead-letter files with error details. That keeps the pipeline stable and gives an audit trail for replay and troubleshooting.

### How do you prevent duplicates?

The pipeline keys events by `transaction_id` and keeps a single record inside the processing window before writing to BigQuery. In a more advanced implementation, I would also align upstream retry behavior and optionally enforce idempotency downstream.

### What does Cloud Composer do here?

Composer orchestrates the downstream warehouse steps. It schedules SQL jobs to build the curated silver table, merchant and country KPI tables, and data quality checks in the correct dependency order.

### How do you optimize BigQuery in this project?

Large tables are partitioned by date and clustered by risk level, country, and merchant category. I also materialize gold KPI tables so dashboard workloads do not repeatedly scan event-level transaction data.

### What would you improve next?

- Add customer and device velocity features
- Introduce ML scoring with Vertex AI or BigQuery ML
- Push alerts to downstream operational systems
- Add dbt documentation and tests
- Add CI/CD for setup and orchestration deployment

