"""Configuration utilities for the FMI weather pipeline."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BIGQUERY_KEY_PATH = "/app/keys/bigquery/api_key.json"

bigquery_api_key_path: str | None = (
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    or os.getenv("BIGQUERY_API_KEY_PATH")
    or DEFAULT_BIGQUERY_KEY_PATH
)

def _list_from_env(env_value: str | None) -> List[str]:
    if not env_value:
        return []
    return [item.strip() for item in env_value.split(",") if item.strip()]


@dataclass
class PipelineConfig:
    """Centralised runtime configuration.

    Values are sourced from environment variables so local development, CI, and
    deployment environments can all inject secrets and connection details
    without changing the codebase.
    """

    fmi_api_key: str | None = os.getenv("FMI_API_KEY")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "fmi_observations")
    bigquery_project: str = os.getenv("BIGQUERY_PROJECT", "fmiweatherdatapipeline")
    bigquery_dataset: str = os.getenv("BIGQUERY_DATASET", "fmi_weather")
    bigquery_api_key_path: str | None = os.getenv("BIGQUERY_API_KEY_PATH") or str(
        DEFAULT_BIGQUERY_KEY_PATH
    )
    daily_table: str = os.getenv("BIGQUERY_DAILY_TABLE", "weather")
    hourly_table: str = os.getenv("BIGQUERY_HOURLY_TABLE", "weather_hourly_samples")
    long_term_table: str = os.getenv("BIGQUERY_LONG_TERM_TABLE", "weather_history")
    station_whitelist: list[str] = field(
        default_factory=lambda: _list_from_env(os.getenv("STATION_WHITELIST"))
        or [
            # Sensible defaults so demos render long-term charts without extra env vars
            "101104",  # Helsinki Kaisaniemi
            "100968",  # Turku Artukainen
            "100946",  # Tampere Pirkkala
            "102172",  # Oulu Oulunsalo
            "101932",  # Kuopio Savilahti
        ]
    )
    forecast_places: list[str] = field(
        default_factory=lambda: _list_from_env(os.getenv("FORECAST_PLACES"))
    )
    use_sample_data: bool = os.getenv("USE_SAMPLE_DATA", "false").lower() == "true"


CONFIG = PipelineConfig()
