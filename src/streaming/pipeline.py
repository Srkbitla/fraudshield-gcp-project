import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp

from fraudshield.src.streaming.schemas import (
    FLAGGED_TRANSACTIONS_SCHEMA,
    RAW_TRANSACTIONS_SCHEMA,
    REALTIME_METRICS_SCHEMA,
)
from fraudshield.src.streaming.transforms import (
    build_alert_row,
    build_metric_seed,
    format_invalid_record,
    normalize_event,
    parse_message,
    validate_event,
)


class ParseValidateNormalizeFn(beam.DoFn):
    def process(self, raw_message: bytes):
        try:
            parsed = parse_message(raw_message)
            normalized = normalize_event(parsed)
            errors = validate_event(normalized)
            if errors:
                yield beam.pvalue.TaggedOutput("invalid", format_invalid_record(raw_message, errors))
                return
            yield normalized
        except Exception as exc:  # noqa: BLE001
            yield beam.pvalue.TaggedOutput("invalid", format_invalid_record(raw_message, [str(exc)]))


class FirstRecordCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return None

    def add_input(self, accumulator, element):
        if accumulator is None:
            return element
        return accumulator if accumulator["transaction_ts"] <= element["transaction_ts"] else element

    def merge_accumulators(self, accumulators):
        winner = None
        for accumulator in accumulators:
            if accumulator is None:
                continue
            if winner is None or accumulator["transaction_ts"] < winner["transaction_ts"]:
                winner = accumulator
        return winner

    def extract_output(self, accumulator):
        return accumulator


class FraudMetricsCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return {
            "total_transactions": 0,
            "approved_amount": 0.0,
            "declined_transactions": 0,
            "high_risk_transactions": 0,
            "simulated_fraud_transactions": 0,
            "risk_score_total": 0,
        }

    def add_input(self, accumulator, element):
        accumulator["total_transactions"] += int(element["total_transactions"])
        accumulator["approved_amount"] += float(element["approved_amount"])
        accumulator["declined_transactions"] += int(element["declined_transactions"])
        accumulator["high_risk_transactions"] += int(element["high_risk_transactions"])
        accumulator["simulated_fraud_transactions"] += int(element["simulated_fraud_transactions"])
        accumulator["risk_score_total"] += int(element["risk_score_total"])
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for accumulator in accumulators:
            for key in merged:
                merged[key] += accumulator[key]
        return merged

    def extract_output(self, accumulator):
        total_transactions = accumulator["total_transactions"]
        accumulator["approved_amount"] = round(accumulator["approved_amount"], 2)
        accumulator["avg_risk_score"] = round(
            accumulator["risk_score_total"] / total_transactions if total_transactions else 0.0,
            2,
        )
        return accumulator


class FormatMetricsRowFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        (txn_country, merchant_category), metrics = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat().replace("+00:00", "Z"),
            "window_end": window.end.to_utc_datetime().isoformat().replace("+00:00", "Z"),
            "txn_country": txn_country,
            "merchant_category": merchant_category,
            "total_transactions": metrics["total_transactions"],
            "approved_amount": metrics["approved_amount"],
            "declined_transactions": metrics["declined_transactions"],
            "high_risk_transactions": metrics["high_risk_transactions"],
            "simulated_fraud_transactions": metrics["simulated_fraud_transactions"],
            "avg_risk_score": metrics["avg_risk_score"],
            "pipeline_run_ts": Timestamp.now().to_utc_datetime().isoformat().replace("+00:00", "Z"),
        }


def build_pipeline_options(known_args: argparse.Namespace, pipeline_args: list[str]) -> PipelineOptions:
    options = PipelineOptions(
        pipeline_args,
        save_main_session=True,
        project=known_args.project_id,
        region=known_args.region,
        runner=known_args.runner,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
    )
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True
    return options


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="FraudShield real-time Dataflow pipeline.")
    parser.add_argument("--project_id", required=True, help="GCP project ID.")
    parser.add_argument("--region", default="us-central1", help="Dataflow region.")
    parser.add_argument("--input_subscription", required=True, help="Pub/Sub subscription path.")
    parser.add_argument("--raw_table", required=True, help="BigQuery target table for raw transactions.")
    parser.add_argument("--alerts_table", required=True, help="BigQuery target table for flagged transactions.")
    parser.add_argument("--metrics_table", required=True, help="BigQuery target table for one-minute fraud metrics.")
    parser.add_argument("--dead_letter_path", required=True, help="GCS path prefix for invalid events.")
    parser.add_argument("--runner", default="DirectRunner", help="Beam runner to use.")
    parser.add_argument("--temp_location", default="", help="GCS temp location for Dataflow.")
    parser.add_argument("--staging_location", default="", help="GCS staging location for Dataflow.")

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = build_pipeline_options(known_args, pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        parsed = (
            pipeline
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=known_args.input_subscription)
            | "ParseValidateNormalize" >> beam.ParDo(ParseValidateNormalizeFn()).with_outputs(
                "invalid",
                main="valid",
            )
        )

        valid_events = (
            parsed.valid
            | "WindowValidEvents" >> beam.WindowInto(FixedWindows(60), allowed_lateness=300)
            | "KeyByTransactionId" >> beam.Map(lambda row: (row["transaction_id"], row))
            | "DeduplicateByTransactionId" >> beam.CombinePerKey(FirstRecordCombineFn())
            | "DropTransactionIdKey" >> beam.Values()
        )

        _ = (
            valid_events
            | "WriteRawTransactionsToBigQuery"
            >> WriteToBigQuery(
                table=known_args.raw_table,
                schema=RAW_TRANSACTIONS_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        _ = (
            valid_events
            | "FilterHighRiskTransactions" >> beam.Filter(lambda row: row["risk_level"] == "HIGH")
            | "BuildAlertRows" >> beam.Map(build_alert_row)
            | "WriteFlaggedTransactions"
            >> WriteToBigQuery(
                table=known_args.alerts_table,
                schema=FLAGGED_TRANSACTIONS_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        _ = (
            valid_events
            | "BuildFraudMetricSeed" >> beam.Map(build_metric_seed)
            | "WindowMetricSeed" >> beam.WindowInto(FixedWindows(60), allowed_lateness=300)
            | "KeyMetricDimensions" >> beam.Map(lambda row: ((row["txn_country"], row["merchant_category"]), row))
            | "AggregateFraudMetrics" >> beam.CombinePerKey(FraudMetricsCombineFn())
            | "FormatFraudMetricsRow" >> beam.ParDo(FormatMetricsRowFn())
            | "WriteFraudMetricsToBigQuery"
            >> WriteToBigQuery(
                table=known_args.metrics_table,
                schema=REALTIME_METRICS_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        _ = (
            parsed.invalid
            | "WindowInvalidEvents" >> beam.WindowInto(FixedWindows(300))
            | "InvalidToJson" >> beam.Map(json.dumps)
            | "WriteInvalidEvents"
            >> WriteToText(
                file_path_prefix=known_args.dead_letter_path,
                file_name_suffix=".jsonl",
                shard_name_template="-SS-of-NN",
            )
        )


def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    run()


if __name__ == "__main__":
    main()

