# Resume Kit

## Project title options

- Real-Time Fraud Detection Platform on GCP
- GCP Streaming Payment Risk Pipeline
- End-to-End Fraud Analytics and Alerting on GCP

## One-line summary

Built an end-to-end real-time fraud detection platform on GCP using Pub/Sub, Dataflow, BigQuery, Cloud Composer, Cloud Storage, and `gcloud` automation to process, score, and monitor streaming payment transactions.

## Resume bullets

- Designed and implemented a real-time fraud detection pipeline on GCP using Pub/Sub and Dataflow to validate, deduplicate, and risk-score streaming payment authorization events.
- Built BigQuery bronze, silver, gold, and ops layers to support flagged transaction review, merchant-level fraud KPIs, country-level hotspot analysis, and low-latency operations dashboards.
- Implemented data reliability controls including schema validation, dead-letter handling, partitioned tables, clustering, and automated data quality checks orchestrated with Cloud Composer.
- Automated GCP resource setup with `gcloud` CLI scripts and documented manual deployment steps, making the project reproducible for demos, interviews, and portfolio review.

## Stronger bullet versions with placeholders

- Built a streaming fraud analytics platform that processed [X]+ simulated payment events per hour with Pub/Sub and Dataflow, enabling near real-time detection of suspicious transactions and alert creation.
- Designed a rule-based fraud scoring layer using amount, cross-border behavior, device novelty, and card-not-present signals to classify transactions into risk levels for operational review.
- Modeled BigQuery silver and gold layers for merchant and country fraud reporting, supporting metrics such as suspicious transaction share, approval rate, decline rate, and flagged amount exposure.
- Automated setup and downstream orchestration with `gcloud` CLI and Cloud Composer, creating a portfolio-ready GCP project that demonstrates both engineering depth and production-style thinking.
