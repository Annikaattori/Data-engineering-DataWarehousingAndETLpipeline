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
BIGQUERY_HOURLY_SCHEMA = [
    {
        "name": "station_id",
        "mode": "REQUIRED",
        "type": "STRING",
        "description": "Finnish Meteorological Institute station ID",
        "fields": [],
    },
    {
        "name": "station_name",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "Finnish Meteorological Institute station name",
        "fields": [],
    },
    {
        "name": "latitude",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "Station latitude (degrees)",
        "fields": [],
    },
    {
        "name": "longitude",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "Station longitude (degrees)",
        "fields": [],
    },
    {
        "name": "timestamp",
        "mode": "REQUIRED",
        "type": "TIMESTAMP",
        "description": "Sample timestamp (UTC)",
        "fields": [],
    },
    {
        "name": "temperature",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "Celsius",
        "fields": [],
    },
    {
        "name": "humidity",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "%",
        "fields": [],
    },
    {
        "name": "wind_speed",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "m/s",
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


def build_long_term_tables(frame: pd.DataFrame, station_ids: Iterable[str]) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    if frame.empty:
        return tables

    filtered = frame[frame["station_id"].isin(station_ids)]
    for station_id, station_frame in filtered.groupby("station_id"):
        tables[station_id] = station_frame.sort_values("timestamp").reset_index(drop=True)
    return tables


def apply_bigquery_schema(frame: pd.DataFrame, schema: list[dict] | None = None) -> pd.DataFrame:
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
    typed["latitude"] = pd.to_numeric(typed.get("latitude", pd.NA), errors="coerce")
    typed["longitude"] = pd.to_numeric(typed.get("longitude", pd.NA), errors="coerce")
    typed["timestamp"] = pd.to_datetime(typed["timestamp"], utc=True)

    for column in ["temperature", "humidity", "wind_speed"]:
        typed[column] = pd.to_numeric(typed.get(column, pd.NA), errors="coerce")

    selected_schema = schema or BIGQUERY_HOURLY_SCHEMA
    ordered_columns = [field["name"] for field in selected_schema]
    return typed[ordered_columns]


def validate_against_schema(
    frame: pd.DataFrame, schema: list[dict] | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter out rows that do not match the expected schema.

    Rows where required fields are missing are handled earlier in the pipeline.
    This validation step ensures that the remaining values conform to the
    expected pandas dtypes implied by the BigQuery schema. Rows that fail
    validation are returned separately so callers can log and skip them.
    """

    if frame.empty:
        return frame, pd.DataFrame()

    selected_schema = schema or BIGQUERY_HOURLY_SCHEMA
    expected_types = {field["name"]: field["type"] for field in selected_schema}

    def _row_is_valid(row: pd.Series) -> bool:
        for column, expected_type in expected_types.items():
            value = row.get(column)
            if pd.isna(value):
                continue

            if expected_type == "STRING" and not isinstance(value, str):
                return False
            if expected_type == "FLOAT" and not isinstance(value, (int, float)):
                return False
            if expected_type == "TIMESTAMP" and not isinstance(value, pd.Timestamp):
                return False
        return True

    valid_mask = frame.apply(_row_is_valid, axis=1)
    invalid_rows = frame[~valid_mask].reset_index(drop=True)
    valid_rows = frame[valid_mask].reset_index(drop=True)
    return valid_rows, invalid_rows


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

    formatted, invalid_rows = validate_against_schema(formatted)
    if not invalid_rows.empty:
        LOGGER.warning(
            "Skipped %s rows that failed schema validation for daily table", len(invalid_rows)
        )

    return formatted.reset_index(drop=True)


def prepare_hourly_for_bigquery(frame: pd.DataFrame) -> pd.DataFrame:
    """Prepare hourly samples for loading into BigQuery."""

    if frame.empty:
        return frame

    formatted = apply_bigquery_schema(frame, schema=BIGQUERY_HOURLY_SCHEMA)

    formatted["timestamp"] = pd.to_datetime(formatted["timestamp"], utc=True)

    before_missing = len(formatted)
    formatted = formatted.dropna(subset=["station_id", "timestamp"])
    dropped_missing = before_missing - len(formatted)
    if dropped_missing:
        LOGGER.info("Dropped %s rows with missing required fields (station_id, timestamp)", dropped_missing)

    before_dupes = len(formatted)
    formatted = formatted.drop_duplicates(subset=["station_id", "timestamp"], keep="last")
    dropped_dupes = before_dupes - len(formatted)
    if dropped_dupes:
        LOGGER.info("Removed %s duplicate hourly rows based on station_id and timestamp", dropped_dupes)

    formatted, invalid_rows = validate_against_schema(formatted, schema=BIGQUERY_HOURLY_SCHEMA)
    if not invalid_rows.empty:
        LOGGER.warning(
            "Skipped %s rows that failed schema validation for hourly table", len(invalid_rows)
        )

    return formatted.reset_index(drop=True)
