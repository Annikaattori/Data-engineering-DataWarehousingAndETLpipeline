"""Airflow DAG orchestrating ingestion and BigQuery processing."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data_processing.kafka_stream import ObservationConsumer, ObservationProducer


def produce_latest_hourly(**_):  # pragma: no cover - orchestration glue
    producer = ObservationProducer()
    producer.publish_latest_hourly()


def produce_backfill_last_year_hourly(**_):  # pragma: no cover - orchestration glue
    producer = ObservationProducer()
    producer.publish_backfill_last_year_hourly()


def consume_batch(max_messages: int | None = None, **_):  # pragma: no cover - orchestration glue
    consumer = ObservationConsumer()
    consumer.consume_once(max_messages=max_messages)


def build_dag():  # pragma: no cover - orchestration glue
    with DAG(
        dag_id="fmi_weather_pipeline",
        description="Ingest and process FMI observations",
        start_date=datetime(2024, 6, 1),
        schedule_interval="@hourly",
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
        tags=["fmi", "weather"],
    ) as dag:
        ingest = PythonOperator(
            task_id="produce_latest_hourly",
            python_callable=produce_latest_hourly,
        )
        load = PythonOperator(task_id="consume_batch", python_callable=consume_batch)

        ingest >> load
    return dag


def build_backfill_dag():  # pragma: no cover - orchestration glue
    with DAG(
        dag_id="fmi_weather_backfill_hourly",
        description="Backfill hourly FMI observations from last year",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
        tags=["fmi", "weather", "backfill"],
    ) as dag:
        backfill = PythonOperator(
            task_id="produce_backfill_last_year_hourly",
            python_callable=produce_backfill_last_year_hourly,
        )
        load = PythonOperator(
            task_id="consume_backfill_batch",
            python_callable=lambda **kwargs: consume_batch(max_messages=2000, **kwargs),
        )

        backfill >> load
    return dag


globals()["dag"] = build_dag()
globals()["backfill_dag"] = build_backfill_dag()
