"""Configuration utilities for the FMI weather pipeline."""
from __future__ import annotations
from dataclasses import dataclass, field

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BIGQUERY_KEY_PATH = BASE_DIR / "keys" / "bigquery" / "api_key.json"


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
    long_term_table_prefix: str = os.getenv("BIGQUERY_LONG_TERM_PREFIX", "station_")
    station_whitelist: list[str] = field(default_factory=list)
    use_sample_data: bool = os.getenv("USE_SAMPLE_DATA", "false").lower() == "true"


CONFIG = PipelineConfig()
