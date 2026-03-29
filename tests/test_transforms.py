import json
import unittest
from datetime import datetime, timedelta, timezone

from fraudshield.src.streaming.transforms import (
    build_alert_row,
    build_metric_seed,
    normalize_event,
    parse_message,
    validate_event,
)


class FraudTransformTests(unittest.TestCase):
    def setUp(self) -> None:
        base_time = datetime.now(timezone.utc) - timedelta(minutes=2)
        self.transaction = {
            "transaction_id": "txn-1001",
            "customer_id": "cust-1001",
            "card_id": "card-1001",
            "merchant_id": "mer-1001",
            "merchant_category": "electronics",
            "amount": 1800.0,
            "currency": "usd",
            "home_country": "us",
            "txn_country": "gb",
            "city": "London",
            "device_id": "dev-1001",
            "payment_method": "visa",
            "transaction_ts": base_time.isoformat().replace("+00:00", "Z"),
            "txn_status": "approved",
            "is_card_present": False,
            "is_new_device": True,
            "simulated_fraud_label": True,
            "ingest_source": "unit-test",
        }

    def test_parse_message_from_bytes(self) -> None:
        payload = json.dumps(self.transaction).encode("utf-8")
        parsed = parse_message(payload)
        self.assertEqual(parsed["transaction_id"], "txn-1001")

    def test_normalize_event_calculates_high_risk(self) -> None:
        normalized = normalize_event(self.transaction)
        self.assertEqual(normalized["risk_level"], "HIGH")
        self.assertGreaterEqual(normalized["risk_score"], 60)
        self.assertIn("HIGH_AMOUNT", normalized["risk_reasons"])

    def test_validate_event_rejects_negative_amount(self) -> None:
        bad = dict(self.transaction)
        bad["amount"] = -1
        errors = validate_event(bad)
        self.assertTrue(any("amount must be greater than zero" in error for error in errors))

    def test_build_alert_row_keeps_core_fields(self) -> None:
        normalized = normalize_event(self.transaction)
        alert = build_alert_row(normalized)
        self.assertEqual(alert["transaction_id"], "txn-1001")
        self.assertEqual(alert["risk_level"], "HIGH")

    def test_build_metric_seed_tracks_risk_counts(self) -> None:
        normalized = normalize_event(self.transaction)
        seed = build_metric_seed(normalized)
        self.assertEqual(seed["total_transactions"], 1)
        self.assertEqual(seed["high_risk_transactions"], 1)
        self.assertEqual(seed["simulated_fraud_transactions"], 1)


if __name__ == "__main__":
    unittest.main()
