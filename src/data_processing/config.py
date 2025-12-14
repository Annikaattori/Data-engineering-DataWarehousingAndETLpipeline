"""Configuration utilities for the FMI weather pipeline."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


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
    bigquery_dataset: str = os.getenv("BIGQUERY_DATASET", "fmi_weather")
    daily_table: str = os.getenv("BIGQUERY_DAILY_TABLE", "daily_observations")
    long_term_table_prefix: str = os.getenv("BIGQUERY_LONG_TERM_PREFIX", "station_")
    station_whitelist: List[str] = _list_from_env(
        os.getenv(
            "STATION_WHITELIST",
            "101104,100968,100946,102172,101932",
        )
    )
    use_sample_data: bool = os.getenv("USE_SAMPLE_DATA", "false").lower() == "true"


CONFIG = PipelineConfig()
