"""Airflow DAG orchestrating ingestion and BigQuery processing."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from src.data_processing.config import CONFIG


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
        ingest = DockerOperator(
            task_id="produce_latest_hourly",
            image="fmi-weather-pipeline:latest",  # Oletetaan, että image on rakennettu
            command=["python", "-m", "src.data_processing.kafka_stream", "produce", "--mode", "latest-hourly"],
            auto_remove=True,
            environment={
                "USE_SAMPLE_DATA": "false",
                "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
                "FMI_API_KEY": "{{ var.value.fmi_api_key }}",  # Tai ympäristömuuttuja
            },
            networks=["data-engineering-datawarehousingandetlpipeline_default"],  # Oletetaan docker-compose network
        )
        load = DockerOperator(
            task_id="consume_stream",
            image="fmi-weather-pipeline:latest",
            command=["python", "-m", "src.data_processing.kafka_stream", "consume", "--batch-size", "500"],
            auto_remove=True,
            execution_timeout=timedelta(hours=1),  # Pysäytä 1 tunnin jälkeen
            environment={
                "USE_SAMPLE_DATA": "false",
                "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
                "BIGQUERY_PROJECT": "fmiweatherdatapipeline",
                "BIGQUERY_DATASET": "fmi_weather",
                "BIGQUERY_HOURLY_TABLE": "weather",
                "BIGQUERY_API_KEY_PATH": "/app/keys/bigquery/api_key.json",
                "GOOGLE_APPLICATION_CREDENTIALS": "/app/keys/bigquery/api_key.json",
            },
            networks=["data-engineering-datawarehousingandetlpipeline_default"], 
            volumes=CONFIG.bigquery_api_key_path,  # Aseta oikea polku
        )

        ingest >> load
    return dag
globals()["dag"] = build_dag()
