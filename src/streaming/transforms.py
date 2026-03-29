from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


REQUIRED_FIELDS = {
    "transaction_id",
    "customer_id",
    "card_id",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "home_country",
    "txn_country",
    "city",
    "device_id",
    "payment_method",
    "transaction_ts",
    "txn_status",
    "is_card_present",
    "is_new_device",
    "simulated_fraud_label",
    "ingest_source",
}

VALID_STATUSES = {"APPROVED", "DECLINED"}
RISKY_CATEGORIES = {"ELECTRONICS", "LUXURY", "GIFT_CARDS", "TRAVEL"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_message(message: bytes | str) -> dict[str, Any]:
    raw_payload = message.decode("utf-8") if isinstance(message, bytes) else message
    payload = json.loads(raw_payload)
    if not isinstance(payload, dict):
        raise ValueError("The Pub/Sub message must be a JSON object.")
    return payload


def calculate_risk(record: dict[str, Any]) -> tuple[int, str, list[str]]:
    score = 0
    reasons = []

    amount = float(record["amount"])
    if amount >= 1500:
        score += 40
        reasons.append("HIGH_AMOUNT")
    elif amount >= 800:
        score += 20
        reasons.append("ELEVATED_AMOUNT")

    if record["txn_country"] != record["home_country"]:
        score += 25
        reasons.append("CROSS_BORDER")

    if not bool(record["is_card_present"]):
        score += 10
        reasons.append("CARD_NOT_PRESENT")

    if bool(record["is_new_device"]):
        score += 15
        reasons.append("NEW_DEVICE")

    if str(record["merchant_category"]).upper() in RISKY_CATEGORIES:
        score += 10
        reasons.append("RISKY_MERCHANT_CATEGORY")

    if str(record["txn_status"]).upper() == "DECLINED":
        score += 5
        reasons.append("DECLINED_TRANSACTION")

    if score >= 60:
        level = "HIGH"
    elif score >= 30:
        level = "MEDIUM"
    else:
        level = "LOW"

    return score, level, reasons


def normalize_event(record: dict[str, Any], processed_at: datetime | None = None) -> dict[str, Any]:
    processed_at = processed_at or utc_now()
    transaction_ts = parse_timestamp(str(record["transaction_ts"]))
    risk_score, risk_level, risk_reasons = calculate_risk(record)

    return {
        "transaction_id": str(record["transaction_id"]),
        "customer_id": str(record["customer_id"]),
        "card_id": str(record["card_id"]),
        "merchant_id": str(record["merchant_id"]),
        "merchant_category": str(record["merchant_category"]).lower(),
        "amount": round(float(record["amount"]), 2),
        "currency": str(record["currency"]).upper(),
        "home_country": str(record["home_country"]).upper(),
        "txn_country": str(record["txn_country"]).upper(),
        "city": str(record["city"]),
        "device_id": str(record["device_id"]),
        "payment_method": str(record["payment_method"]).upper(),
        "transaction_ts": to_iso8601(transaction_ts),
        "transaction_date": transaction_ts.date().isoformat(),
        "txn_status": str(record["txn_status"]).upper(),
        "is_card_present": bool(record["is_card_present"]),
        "is_new_device": bool(record["is_new_device"]),
        "simulated_fraud_label": bool(record["simulated_fraud_label"]),
        "risk_score": risk_score,
        "risk_level": risk_level,
        "risk_reasons": risk_reasons,
        "ingest_source": str(record["ingest_source"]),
        "processing_ts": to_iso8601(processed_at),
        "is_late_arrival": (processed_at - transaction_ts).total_seconds() > 300,
    }


def validate_event(record: dict[str, Any]) -> list[str]:
    errors = []
    missing = sorted(REQUIRED_FIELDS - set(record))
    if missing:
        errors.append(f"Missing required fields: {', '.join(missing)}")
        return errors

    try:
        transaction_ts = parse_timestamp(str(record["transaction_ts"]))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"Invalid transaction_ts: {exc}")
        transaction_ts = None

    try:
        amount = float(record["amount"])
        if amount <= 0:
            errors.append("amount must be greater than zero")
    except (TypeError, ValueError):
        errors.append("amount must be numeric")

    if len(str(record["currency"])) != 3:
        errors.append("currency must be a 3-letter code")

    if str(record["txn_status"]).upper() not in VALID_STATUSES:
        errors.append(f"Unsupported txn_status: {record['txn_status']}")

    if transaction_ts and transaction_ts > utc_now().replace(microsecond=0):
        errors.append("transaction_ts cannot be in the future")

    if not str(record["merchant_id"]).strip():
        errors.append("merchant_id cannot be empty")

    if not str(record["txn_country"]).strip():
        errors.append("txn_country cannot be empty")

    return errors


def format_invalid_record(raw_message: bytes | str, errors: list[str]) -> dict[str, Any]:
    raw_payload = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else str(raw_message)
    return {
        "received_ts": to_iso8601(utc_now()),
        "error_count": len(errors),
        "errors": errors,
        "raw_message": raw_payload,
    }


def build_alert_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "transaction_id": record["transaction_id"],
        "transaction_ts": record["transaction_ts"],
        "customer_id": record["customer_id"],
        "merchant_id": record["merchant_id"],
        "merchant_category": record["merchant_category"],
        "amount": record["amount"],
        "txn_country": record["txn_country"],
        "txn_status": record["txn_status"],
        "risk_score": record["risk_score"],
        "risk_level": record["risk_level"],
        "risk_reasons": record["risk_reasons"],
        "alert_created_ts": to_iso8601(utc_now()),
    }


def build_metric_seed(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "txn_country": record["txn_country"],
        "merchant_category": record["merchant_category"],
        "total_transactions": 1,
        "approved_amount": float(record["amount"]) if record["txn_status"] == "APPROVED" else 0.0,
        "declined_transactions": 1 if record["txn_status"] == "DECLINED" else 0,
        "high_risk_transactions": 1 if record["risk_level"] == "HIGH" else 0,
        "simulated_fraud_transactions": 1 if record["simulated_fraud_label"] else 0,
        "risk_score_total": int(record["risk_score"]),
    }

