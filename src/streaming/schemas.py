RAW_TRANSACTIONS_SCHEMA = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "card_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "merchant_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "merchant_category", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
        {"name": "home_country", "type": "STRING", "mode": "REQUIRED"},
        {"name": "txn_country", "type": "STRING", "mode": "REQUIRED"},
        {"name": "city", "type": "STRING", "mode": "REQUIRED"},
        {"name": "device_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "payment_method", "type": "STRING", "mode": "REQUIRED"},
        {"name": "transaction_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "txn_status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "is_card_present", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "is_new_device", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "simulated_fraud_label", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "risk_score", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "risk_level", "type": "STRING", "mode": "REQUIRED"},
        {"name": "risk_reasons", "type": "STRING", "mode": "REPEATED"},
        {"name": "ingest_source", "type": "STRING", "mode": "REQUIRED"},
        {"name": "processing_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "is_late_arrival", "type": "BOOLEAN", "mode": "REQUIRED"},
    ]
}

FLAGGED_TRANSACTIONS_SCHEMA = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "transaction_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "merchant_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "merchant_category", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "txn_country", "type": "STRING", "mode": "REQUIRED"},
        {"name": "txn_status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "risk_score", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "risk_level", "type": "STRING", "mode": "REQUIRED"},
        {"name": "risk_reasons", "type": "STRING", "mode": "REPEATED"},
        {"name": "alert_created_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

REALTIME_METRICS_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "txn_country", "type": "STRING", "mode": "REQUIRED"},
        {"name": "merchant_category", "type": "STRING", "mode": "REQUIRED"},
        {"name": "total_transactions", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "approved_amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "declined_transactions", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "high_risk_transactions", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "simulated_fraud_transactions", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "avg_risk_score", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "pipeline_run_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

