"""Airflow DAG orchestrating ingestion and BigQuery processing."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data_processing.kafka_stream import ObservationConsumer, ObservationProducer


def produce_latest_hourly(**_):  # pragma: no cover - orchestration glue
    producer = ObservationProducer()
    producer.publish_latest_hourly()


def consume_continuously(**_):  # pragma: no cover - orchestration glue
    consumer = ObservationConsumer()
    consumer.consume_forever()


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
        load = PythonOperator(task_id="consume_stream", python_callable=consume_continuously)

        ingest >> load
    return dag
globals()["dag"] = build_dag()
