"""Transformations used in BigQuery or locally for demo purposes."""
from __future__ import annotations

import logging
from typing import Iterable, List

import pandas as pd

from .fmi_client import Observation

LOGGER = logging.getLogger(__name__)


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
