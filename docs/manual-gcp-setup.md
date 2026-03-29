# Manual GCP Setup Guide

This guide shows how to build FraudShield through the GCP Console and SQL editor without Terraform.

## Step 1: Enable APIs

Enable these services in `APIs & Services` -> `Library`:

- BigQuery API
- Cloud Composer API
- Cloud Dataflow API
- Cloud Pub/Sub API
- Cloud Storage API

## Step 2: Create storage buckets

Create these Cloud Storage buckets:

- `your-gcp-project-id-fraudshield-raw`
- `your-gcp-project-id-fraudshield-temp`

Recommended settings:

- region: `us-central1`
- access control: uniform
- public access prevention: enforced

## Step 3: Create Pub/Sub resources

Create topics:

- `fraud-transactions`
- `fraud-transactions-dlq`

Create subscription:

- name: `fraud-transactions-sub`
- topic: `fraud-transactions`
- acknowledgment deadline: `60`
- dead-letter topic: `fraud-transactions-dlq`
- maximum delivery attempts: `10`

## Step 4: Create a Dataflow service account

Create service account:

- name: `fraudshield-dataflow`
- email: `fraudshield-dataflow@your-gcp-project-id.iam.gserviceaccount.com`

Grant roles:

- `BigQuery Data Editor`
- `Dataflow Worker`
- `Pub/Sub Subscriber`
- `Storage Object Admin`
- `Viewer`

## Step 5: Create BigQuery datasets

Create these datasets in BigQuery:

- `bronze`
- `silver`
- `gold`
- `ops`

Use location `US`.

## Step 6: Create base tables

Run [sql/01_create_objects.sql](../sql/01_create_objects.sql) after replacing the project ID placeholder.

This creates:

- `bronze.payment_transactions`
- `ops.flagged_transactions`
- `ops.realtime_fraud_metrics`
- `ops.data_quality_audit`

## Step 7: Optional Cloud Composer setup

If you want orchestration:

1. Create a Composer environment in the same region.
2. Upload [orchestration/composer/fraudshield_realtime_dag.py](../orchestration/composer/fraudshield_realtime_dag.py).
3. Upload the SQL files from [sql](../sql).
4. Add Airflow variables:
   - `fraudshield_project_id`
   - `fraudshield_dataset_location`

If you skip Composer, you can still run the SQL manually and explain Composer as the production orchestrator.

## Step 8: Install dependencies

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r .\fraudshield\requirements.txt
```

## Step 9: Run the Dataflow pipeline

```powershell
python -m src.streaming.pipeline `
  --project_id=your-gcp-project-id `
  --region=us-central1 `
  --input_subscription=projects/your-gcp-project-id/subscriptions/fraud-transactions-sub `
  --raw_table=your-gcp-project-id:bronze.payment_transactions `
  --alerts_table=your-gcp-project-id:ops.flagged_transactions `
  --metrics_table=your-gcp-project-id:ops.realtime_fraud_metrics `
  --dead_letter_path=gs://your-gcp-project-id-fraudshield-raw/dead-letter/fraud-transactions `
  --temp_location=gs://your-gcp-project-id-fraudshield-temp/dataflow/temp `
  --staging_location=gs://your-gcp-project-id-fraudshield-temp/dataflow/staging `
  --runner=DataflowRunner
```

## Step 10: Publish sample transactions

```powershell
python -m src.producer.transaction_events_producer `
  --project_id=your-gcp-project-id `
  --topic_id=fraud-transactions `
  --event_count=500 `
  --sleep_seconds=0.3
```

## Step 11: Build curated and KPI tables

Run in order:

1. [sql/02_silver_transactions_curated.sql](../sql/02_silver_transactions_curated.sql)
2. [sql/03_gold_risk_kpis.sql](../sql/03_gold_risk_kpis.sql)
3. [sql/04_data_quality_checks.sql](../sql/04_data_quality_checks.sql)
