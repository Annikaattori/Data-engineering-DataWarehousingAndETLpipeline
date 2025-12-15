from __future__ import annotations

import json
from pathlib import Path

import pytest

pandas = pytest.importorskip("pandas")
pd = pandas

from src.data_processing import transformations
from src.data_processing.fmi_client import observations_as_dataframe

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def load_sample_frame():
    with (DATA_DIR / "sample_observations.json").open("r", encoding="utf-8") as file:
        observations = json.load(file)
    return observations_as_dataframe(observations)


def test_deduplication_removes_duplicates():
    observations = json.loads((DATA_DIR / "sample_observations.json").read_text())
    # Add a duplicate record
    observations.append(observations[0])
    deduped = transformations.deduplicate(observations)
    assert len(deduped) == len(observations) - 1


def test_missing_detection():
    frame = load_sample_frame()
    frame.loc[0, "temperature"] = None
    summary = transformations.detect_missing_values(frame)
    assert summary.loc[summary["column"] == "temperature", "missing_count"].iloc[0] == 1


def test_outlier_detection_flags_extreme_values():
    frame = load_sample_frame()
    frame.loc[0, "temperature"] = 1000  # unrealistic spike
    outliers = transformations.detect_outliers(frame, z_threshold=2.5)
    assert not outliers.empty


def test_long_term_tables():
    frame = load_sample_frame()
    tables = transformations.build_long_term_tables(frame, ["101104", "100968"])
    assert set(tables.keys()) == {"101104", "100968"}
    for table in tables.values():
        assert list(table.columns) == list(frame.columns)


def test_apply_bigquery_schema_orders_and_casts_columns():
    frame = load_sample_frame()
    frame["station_id"] = frame["station_id"].astype(int)

    formatted = transformations.apply_bigquery_schema(frame)

    assert list(formatted.columns) == [
        "station_id",
        "station_name",
        "timestamp",
        "temperature",
        "humidity",
        "wind_speed",
    ]
    assert formatted["station_id"].iloc[0] == "101104"
    assert str(formatted["timestamp"].dtype).startswith("datetime64[ns, UTC]")


def test_prepare_for_bigquery_handles_missing_and_duplicates():
    frame = load_sample_frame()
    # duplicate first row and introduce a missing required field
    duplicated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    duplicated.loc[1, "station_id"] = pd.NA

    cleaned = transformations.prepare_for_bigquery(duplicated)

    assert len(cleaned) == len(frame)  # one missing + one duplicate removed
    assert cleaned["station_id"].isna().sum() == 0
    assert cleaned.drop_duplicates(subset=["station_id", "timestamp"]).shape[0] == len(
        cleaned
    )
