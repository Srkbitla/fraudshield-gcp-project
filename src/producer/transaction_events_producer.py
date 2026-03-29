import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from google.cloud import pubsub_v1


COUNTRIES = {
    "US": ["New York", "Austin", "Seattle"],
    "GB": ["London", "Manchester"],
    "IN": ["Bengaluru", "Mumbai", "Hyderabad"],
    "SG": ["Singapore"],
    "AE": ["Dubai"],
}

MERCHANT_CATEGORIES = [
    "grocery",
    "electronics",
    "travel",
    "gaming",
    "luxury",
    "fuel",
    "gift_cards",
]

RISKY_CATEGORIES = {"electronics", "luxury", "gift_cards", "travel"}
PAYMENT_METHODS = ["VISA", "MASTERCARD", "AMEX"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_transaction(rng: random.Random) -> dict[str, Any]:
    home_country = rng.choice(list(COUNTRIES.keys()))
    cross_border = rng.random() < 0.18
    txn_country = rng.choice(list(COUNTRIES.keys())) if cross_border else home_country
    city = rng.choice(COUNTRIES[txn_country])
    merchant_category = rng.choice(MERCHANT_CATEGORIES)
    is_card_present = rng.random() >= 0.35
    is_new_device = rng.random() < 0.15

    base_amount = rng.uniform(10.0, 900.0)
    if merchant_category in RISKY_CATEGORIES and rng.random() < 0.35:
        base_amount = rng.uniform(800.0, 2500.0)

    amount = round(base_amount, 2)
    transaction_ts = utc_now() - timedelta(seconds=rng.randint(0, 240))

    fraud_signal_score = 0
    if amount >= 1200:
        fraud_signal_score += 2
    if txn_country != home_country:
        fraud_signal_score += 2
    if not is_card_present:
        fraud_signal_score += 1
    if is_new_device:
        fraud_signal_score += 1
    if merchant_category in RISKY_CATEGORIES:
        fraud_signal_score += 1

    fraud_probability = 0.03 + fraud_signal_score * 0.12
    simulated_fraud_label = rng.random() < min(fraud_probability, 0.85)
    txn_status = "DECLINED" if simulated_fraud_label and rng.random() < 0.45 else "APPROVED"

    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "customer_id": f"CUST-{rng.randint(100000, 999999)}",
        "card_id": f"CARD-{rng.randint(10000, 99999)}",
        "merchant_id": f"MER-{rng.randint(1000, 9999)}",
        "merchant_category": merchant_category,
        "amount": amount,
        "currency": "USD",
        "home_country": home_country,
        "txn_country": txn_country,
        "city": city,
        "device_id": f"DEV-{rng.randint(100000, 999999)}",
        "payment_method": rng.choice(PAYMENT_METHODS),
        "transaction_ts": to_iso8601(transaction_ts),
        "txn_status": txn_status,
        "is_card_present": is_card_present,
        "is_new_device": is_new_device,
        "simulated_fraud_label": simulated_fraud_label,
        "ingest_source": "python-simulator",
    }


def publish_events(project_id: str, topic_id: str, event_count: int, sleep_seconds: float, seed: int) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    rng = random.Random(seed)

    for index in range(1, event_count + 1):
        event = build_transaction(rng)
        future = publisher.publish(
            topic_path,
            json.dumps(event).encode("utf-8"),
            txn_status=event["txn_status"],
            txn_country=event["txn_country"],
        )
        future.result(timeout=30)
        print(
            f"Published {index}/{event_count}: {event['transaction_id']} "
            f"{event['txn_status']} amount={event['amount']} risk_seed={event['simulated_fraud_label']}"
        )

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish simulated payment transactions to Pub/Sub.")
    parser.add_argument("--project_id", required=True, help="GCP project ID.")
    parser.add_argument("--topic_id", required=True, help="Pub/Sub topic name.")
    parser.add_argument("--event_count", type=int, default=100, help="Number of events to publish.")
    parser.add_argument("--sleep_seconds", type=float, default=0.2, help="Pause between publishes.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    publish_events(
        project_id=args.project_id,
        topic_id=args.topic_id,
        event_count=args.event_count,
        sleep_seconds=args.sleep_seconds,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()

