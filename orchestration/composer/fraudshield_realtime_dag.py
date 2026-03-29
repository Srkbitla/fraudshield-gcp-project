import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


DATASET_LOCATION = Variable.get("fraudshield_dataset_location", default_var="US")
PROJECT_ID = Variable.get(
    "fraudshield_project_id",
    default_var=os.environ.get("GCP_PROJECT", "your-gcp-project-id"),
)

SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"


def load_sql(file_name: str) -> str:
    query = (SQL_ROOT / file_name).read_text(encoding="utf-8")
    return query.replace("your-gcp-project-id", PROJECT_ID)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="fraudshield_realtime_warehouse",
    description="Build curated fraud analytics tables from streaming payment events.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "fraud", "streaming", "data-engineering"],
) as dag:
    build_silver_transactions = BigQueryInsertJobOperator(
        task_id="build_silver_transactions_curated",
        location=DATASET_LOCATION,
        configuration={
            "query": {
                "query": load_sql("02_silver_transactions_curated.sql"),
                "useLegacySql": False,
            }
        },
    )

    build_gold_kpis = BigQueryInsertJobOperator(
        task_id="build_gold_risk_kpis",
        location=DATASET_LOCATION,
        configuration={
            "query": {
                "query": load_sql("03_gold_risk_kpis.sql"),
                "useLegacySql": False,
            }
        },
    )

    run_data_quality_checks = BigQueryInsertJobOperator(
        task_id="run_data_quality_checks",
        location=DATASET_LOCATION,
        configuration={
            "query": {
                "query": load_sql("04_data_quality_checks.sql"),
                "useLegacySql": False,
            }
        },
    )

    build_silver_transactions >> build_gold_kpis >> run_data_quality_checks

