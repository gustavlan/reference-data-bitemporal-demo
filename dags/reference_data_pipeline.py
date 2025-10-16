"""Airflow DAG orchestrating bi-temporal pipeline."""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError as exc:  # Airflow is optional for tests
    raise RuntimeError(
        "apache airflow must be installed to use the DAG. Install the optional "
        "dependency with `pip install -e .[airflow]`."
    ) from exc

from bitemporal import merge_logic

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT_DIR / "data" / "synthetic_refdata.csv"
DB_PATH = ROOT_DIR / "reference_data.db"

_LOG = logging.getLogger(__name__)


def extract_fn(**_: Any) -> List[Dict[str, str]]:
    """Load the synthetic CSV into memory."""

    records: List[Dict[str, str]] = []
    with DATA_PATH.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            records.append(dict(row))
    _LOG.info("Extracted %s records from %s", len(records), DATA_PATH)
    return records


def transform_fn(**context: Any) -> List[Dict[str, Any]]:
    """Annotate each record with a consistent knowledge timestamp."""

    records = context["ti"].xcom_pull(task_ids="extract") or []
    now = datetime.now(tz=timezone.utc)
    iso_now = now.isoformat(timespec="seconds")

    transformed: List[Dict[str, Any]] = []
    for record in records:
        payload = dict(record)
        payload.setdefault("event_time", payload.get("event_time"))
        payload["knowledge_time"] = iso_now
        transformed.append(payload)

    context["ti"].xcom_push(key="knowledge_time", value=iso_now)
    _LOG.info("Transformed %s records", len(transformed))
    return transformed


def load_fn(**context: Any) -> Dict[str, Any]:
    """Persist records into SQLite using the merge logic."""

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

    conn = merge_logic.connect(DB_PATH)
    summary = merge_logic.merge_records(
        conn,
        records,
        knowledge_time=knowledge_dt,
        allow_late_arrivals=allow_late,
    )
    conn.commit()
    conn.close()

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
