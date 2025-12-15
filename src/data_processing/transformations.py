"""Transformations used in BigQuery or locally for demo purposes."""
from __future__ import annotations

import logging
from typing import Iterable, List

import pandas as pd

from .fmi_client import Observation

LOGGER = logging.getLogger(__name__)


# BigQuery table schema used by the pipeline uploads. Keeping the schema in
# code makes it easy to validate and reshape the dataframe before loading so
# that upstream API changes do not break ingestion.
BIGQUERY_SCHEMA = [
    {
        "name": "station_id",
        "mode": "REQUIRED",
        "type": "STRING",
        "description": "",
        "fields": [],
    },
    {
        "name": "station_name",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": [],
    },
    {
        "name": "timestamp",
        "mode": "REQUIRED",
        "type": "TIMESTAMP",
        "description": "",
        "fields": [],
    },
    {
        "name": "temperature",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "",
        "fields": [],
    },
    {
        "name": "humidity",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "",
        "fields": [],
    },
    {
        "name": "wind_speed",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "",
        "fields": [],
    },
]


def deduplicate(observations: Iterable[Observation]) -> List[Observation]:
    frame = pd.DataFrame(observations)
    if frame.empty:
        return []
    before = len(frame)
    frame.drop_duplicates(subset=["station_id", "timestamp"], inplace=True)
    after = len(frame)
    LOGGER.info("Removed %s duplicate rows", before - after)
    return frame.to_dict(orient="records")


def detect_missing_values(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a summary of missingness per column for quality monitoring."""
    if frame.empty:
        return pd.DataFrame()
    summary = frame.isna().sum().reset_index()
    summary.columns = ["column", "missing_count"]
    return summary


def detect_outliers(frame: pd.DataFrame, z_threshold: float = 3.0) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    numeric_cols = ["temperature", "humidity", "wind_speed"]
    numeric_frame = frame[numeric_cols].astype(float)
    z_scores = (numeric_frame - numeric_frame.mean()) / numeric_frame.std(ddof=0)
    mask = (z_scores.abs() > z_threshold).any(axis=1)
    return frame.loc[mask]


def daily_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    aggregated = frame.copy()
    aggregated["date"] = pd.to_datetime(aggregated["timestamp"], utc=True).dt.date
    return aggregated


def build_long_term_tables(frame: pd.DataFrame, station_ids: Iterable[str]) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    if frame.empty:
        return tables

    filtered = frame[frame["station_id"].isin(station_ids)]
    for station_id, station_frame in filtered.groupby("station_id"):
        tables[station_id] = station_frame.sort_values("timestamp").reset_index(drop=True)
    return tables


def apply_bigquery_schema(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe that matches the expected BigQuery schema.

    Columns are ordered to mirror ``BIGQUERY_SCHEMA`` and values are coerced to
    the appropriate types. Missing optional columns are filled with ``pd.NA`` so
    uploads remain resilient when the upstream payload omits a field.
    """

    if frame.empty:
        return frame

    typed = frame.copy()
    typed["station_id"] = typed["station_id"].astype(str)
    typed["station_name"] = typed.get("station_name", pd.NA)
    typed["timestamp"] = pd.to_datetime(typed["timestamp"], utc=True)

    for column in ["temperature", "humidity", "wind_speed"]:
        typed[column] = pd.to_numeric(typed.get(column, pd.NA), errors="coerce")

    ordered_columns = [field["name"] for field in BIGQUERY_SCHEMA]
    return typed[ordered_columns]


def prepare_for_bigquery(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce, clean, and deduplicate observations prior to BigQuery load."""

    if frame.empty:
        return frame

    formatted = apply_bigquery_schema(frame)

    required_columns = ["station_id", "timestamp"]
    before_missing = len(formatted)
    formatted = formatted.dropna(subset=required_columns)
    dropped_missing = before_missing - len(formatted)
    if dropped_missing:
        LOGGER.info(
            "Dropped %s rows with missing required fields (%s)",
            dropped_missing,
            ", ".join(required_columns),
        )

    before_dupes = len(formatted)
    formatted = formatted.drop_duplicates(subset=["station_id", "timestamp"])
    dropped_dupes = before_dupes - len(formatted)
    if dropped_dupes:
        LOGGER.info(
            "Removed %s duplicate rows based on station_id and timestamp", dropped_dupes
        )

    return formatted.reset_index(drop=True)
