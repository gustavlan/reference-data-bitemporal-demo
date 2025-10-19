"""Airflow DAG orchestrating bi-temporal pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError as exc:  # Airflow is optional for tests
    raise RuntimeError(
        "apache airflow must be installed to use the DAG. Install the optional "
        "dependency with `pip install -e .[airflow]`."
    ) from exc

from scripts import run_pipeline

DATA_PATH = run_pipeline.DATA_PATH

_LOG = logging.getLogger(__name__)


def extract_fn(**_: Any) -> List[Dict[str, str]]:
    """Load the synthetic CSV into memory."""

    records = run_pipeline.extract(DATA_PATH)
    _LOG.info("Extracted %s records from %s", len(records), DATA_PATH)
    return records


def transform_fn(**context: Any) -> List[Dict[str, Any]]:
    """Pass through records while capturing the batch knowledge timestamp."""

    records = context["ti"].xcom_pull(task_ids="extract") or []
    now = datetime.now(tz=timezone.utc)
    iso_now = now.isoformat(timespec="seconds")

    context["ti"].xcom_push(key="knowledge_time", value=iso_now)
    _LOG.info("Prepared %s records for load", len(records))
    return records


def load_fn(**context: Any) -> Dict[str, Any]:
    """Persist records into SQLite using the shared merge helper."""

    records: List[Dict[str, Any]] = context["ti"].xcom_pull(task_ids="transform") or []
    knowledge_iso: str | None = context["ti"].xcom_pull(
        task_ids="transform", key="knowledge_time"
    )
    knowledge_dt = (
        datetime.fromisoformat(knowledge_iso.replace("Z", "+00:00"))
        if knowledge_iso
        else datetime.now(tz=timezone.utc)
    )
    dag_run = context.get("dag_run")
    allow_late = False
    if dag_run and dag_run.conf:
        allow_late = bool(dag_run.conf.get("allow_late", False))

    summary = run_pipeline.load(records, knowledge_dt, allow_late)
    payload = summary.to_dict()
    _LOG.info("Merge summary: %s", payload)
    return payload


with DAG(
    dag_id="reference_data_ingestion",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["demo", "bi-temporal"],
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract_fn)
    transform_task = PythonOperator(task_id="transform", python_callable=transform_fn)
    load_task = PythonOperator(task_id="load", python_callable=load_fn)

    extract_task >> transform_task >> load_task
