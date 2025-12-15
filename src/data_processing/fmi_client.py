"""Client helpers for retrieving observations from the FMI open data API.

The implementation supports both live HTTP queries and offline development
against bundled sample data so the pipeline can be demonstrated without
network or API access.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, TypedDict

import requests

from .config import CONFIG

LOGGER = logging.getLogger(__name__)
DATA_DIR = Path(__file__).resolve().parents[2] / "data"


class Observation(TypedDict):
    station_id: str
    station_name: str
    timestamp: str
    temperature: float
    humidity: float
    wind_speed: float


class FMIClient:
    """Simple FMI client capable of returning the most recent observations."""

    BASE_URL = "https://opendata.fmi.fi/wfs"

    def __init__(self, api_key: str | None = None, use_sample_data: bool | None = None):
        self.api_key = api_key or CONFIG.fmi_api_key
        self.use_sample_data = CONFIG.use_sample_data if use_sample_data is None else use_sample_data

    def _build_query(self) -> dict:
        return {
            "service": "WFS",
            "request": "getFeature",
            "storedquery_id": "fmi::observations::weather::multipointcoverage",
            "place": "finland",
            "timestep": 10,
        }

    def _parse_response(self, content: bytes) -> List[Observation]:
        # Parsing XML coverage is intentionally simplified for the demo and
        # expects that the response is already flattened in GeoJSON by a
        # pre-processing step.
        try:
            payload = json.loads(content.decode("utf-8"))
            return [Observation(**item) for item in payload["observations"]]
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to parse FMI payload: %s", exc)
            raise

    def fetch_latest(self) -> List[Observation]:
        """Return the latest observations.

        When ``use_sample_data`` is true, bundled JSON fixtures are used so the
        pipeline can run offline.
        """

        if self.use_sample_data:
            sample_path = DATA_DIR / "sample_observations.json"
            with sample_path.open("r", encoding="utf-8") as file:
                return json.load(file)

        params = self._build_query()
        if self.api_key:
            params["apikey"] = self.api_key

        response = requests.get(self.BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return self._parse_response(response.content)


def observations_as_dataframe(observations: Iterable[Observation]):
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:  # pragma: no cover - guard for optional dependency
        raise RuntimeError("pandas is required for dataframe conversion") from exc

    frame = pd.DataFrame(observations)
    if frame.empty:
        return frame

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    # Älä lisää "date"-saraketta, koska se tuottaa datetime.date-olioita ja rikkoo to_gbq:n
    frame.sort_values(["station_id", "timestamp"], inplace=True)
    return frame


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
