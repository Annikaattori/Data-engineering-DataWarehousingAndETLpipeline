"""Client helpers for retrieving observations and forecasts from FMI.

The implementation now uses ``fmi-weather-client`` to follow the recommended
API integration pattern for both observations and forecasts while still
supporting offline development via bundled fixtures.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, TypedDict

import fmi_weather_client as fmi
from fmi_weather_client.errors import ClientError, ServerError

from .config import CONFIG

LOGGER = logging.getLogger(__name__)
DATA_DIR = Path(__file__).resolve().parents[2] / "data"


class Observation(TypedDict):
    station_id: str
    station_name: str
    latitude: Optional[float]
    longitude: Optional[float]
    elevation: Optional[float]
    timestamp: str
    temperature: Optional[float]
    humidity: Optional[float]
    wind_speed: Optional[float]


class ForecastPoint(TypedDict, total=False):
    place: str
    timestamp: str
    temperature: Optional[float]
    wind_speed: Optional[float]


class FMIClient:
    """Simple FMI client capable of returning the most recent observations."""

    def __init__(
        self,
        api_key: str | None = None,
        use_sample_data: bool | None = None,
        station_ids: Iterable[str] | None = None,
        forecast_places: Iterable[str] | None = None,
    ):
        self.api_key = api_key or CONFIG.fmi_api_key
        self.use_sample_data = CONFIG.use_sample_data if use_sample_data is None else use_sample_data
        self.station_ids = list(station_ids) if station_ids is not None else list(CONFIG.station_whitelist)
        self.forecast_places = list(forecast_places) if forecast_places is not None else (
            list(CONFIG.forecast_places) if CONFIG.forecast_places else None
        )

    def _extract_time(self, payload) -> str:
        candidate = getattr(payload, "time", None)

        if candidate is None and isinstance(payload, dict):
            candidate = payload.get("time") or payload.get("timestamp")

        if candidate is None and hasattr(payload, "data"):
            candidate = getattr(payload.data, "time", None)

        if candidate is None and isinstance(payload, dict):
            nested = payload.get("data")
            if isinstance(nested, dict):
                candidate = nested.get("time") or nested.get("timestamp")

        if candidate is None:
            return utc_now_iso()

        if hasattr(candidate, "isoformat"):
            return candidate.isoformat()

        return str(candidate)

    def _extract_value(self, payload, keys: Iterable[str]) -> Optional[float]:
        for key in keys:
            value = getattr(payload, key, None)

            if value is None and isinstance(payload, dict):
                value = payload.get(key)

            if value is None and hasattr(payload, "data"):
                value = getattr(payload.data, key, None)

            if value is None and isinstance(payload, dict):
                nested = payload.get("data")
                if isinstance(nested, dict):
                    value = nested.get(key)

            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    LOGGER.debug("Unable to coerce %s to float from payload %s", key, payload)

        return None

    def _build_observation(self, station_id: str, payload) -> Observation | None:
        if payload is None:
            return None

        station_name = getattr(payload, "place", str(station_id))
        return Observation(
            station_id=str(station_id),
            station_name=station_name,
            latitude=self._extract_value(payload, ["latitude", "lat"]),
            longitude=self._extract_value(payload, ["longitude", "lon", "lng"]),
            elevation=self._extract_value(payload, ["elevation", "altitude", "height"]),
            timestamp=self._extract_time(payload),
            temperature=self._extract_value(payload, ["temperature"]),
            humidity=self._extract_value(payload, ["humidity", "relative_humidity"]),
            wind_speed=self._extract_value(payload, ["wind_speed", "windspeed"]),
        )

    def _fetch_station_observation(self, station_id: str) -> Observation | None:
        try:
            weather = fmi.observation_by_station_id(int(station_id))
        except ClientError as err:
            LOGGER.warning(
                "Client error with station %s (status %s): %s", station_id, err.status_code, err.message
            )
            return None
        except ServerError as err:  # pragma: no cover - network interactions
            LOGGER.error(
                "Server error with station %s (status %s): %s", station_id, err.status_code, err.body
            )
            return None

        if weather is None:
            LOGGER.warning("No observation returned for station %s", station_id)
            return None

        return self._build_observation(station_id, weather)

    def _downsample_hourly(self, observations: List[Observation]) -> List[Observation]:
        """Return one observation per station per hour using deterministic flooring.

        Timestamps are floored to the start of the hour (UTC). If multiple
        observations exist within the same hour for a station, the latest
        timestamp in that hour is retained to maximise freshness.
        """

        if not observations:
            return []

        latest_by_key: dict[tuple[str, datetime], tuple[Observation, datetime]] = {}

        for obs in observations:
            timestamp = obs.get("timestamp")
            if not timestamp:
                continue

            cleaned_timestamp = str(timestamp).replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(cleaned_timestamp)
            except ValueError:
                LOGGER.debug("Unable to parse observation timestamp %s", timestamp)
                continue

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            hour_bucket = parsed.replace(minute=0, second=0, microsecond=0)
            key = (str(obs.get("station_id")), hour_bucket)

            existing = latest_by_key.get(key)
            if existing is not None:
                _, existing_ts = existing
                if parsed <= existing_ts:
                    continue

            latest_by_key[key] = (obs, parsed)

        hourly: List[Observation] = []
        for (station_id, hour_bucket), (obs, _) in latest_by_key.items():
            observation_copy = dict(obs)
            observation_copy["station_id"] = str(station_id)
            observation_copy["timestamp"] = hour_bucket.isoformat()
            hourly.append(observation_copy)  # type: ignore[arg-type]

        hourly.sort(key=lambda item: (item.get("station_id"), item.get("timestamp")))
        return hourly

    def fetch_latest(self) -> List[Observation]:
        """Return the latest observations using ``fmi-weather-client``.

        When ``use_sample_data`` is true, bundled JSON fixtures are used so the
        pipeline can run offline.
        """

        if self.use_sample_data:
            sample_path = DATA_DIR / "sample_observations.json"
            with sample_path.open("r", encoding="utf-8") as file:
                return json.load(file)

        observations: List[Observation] = []
        for station_id in self.station_ids:
            observation = self._fetch_station_observation(station_id)
            if observation:
                observations.append(observation)
        return observations

    def fetch_latest_hourly(self) -> List[Observation]:
        """Return the latest observations downsampled to hourly resolution."""

        latest = self.fetch_latest()
        return self._downsample_hourly(latest)

    def fetch_forecast(
        self,
        place_name: str,
        *,
        timestep_hours: int = 24,
        forecast_points: int = 4,
    ) -> List[ForecastPoint]:
        """Return forecast points for a place using ``forecast_by_place_name``.

        The method returns an empty list when sample data is enabled or when the
        FMI API responds with an error so that downstream code can continue
        gracefully.
        """

        if self.use_sample_data:
            LOGGER.info("Sample mode enabled; skipping live forecast fetch for %s", place_name)
            return []

        try:
            forecast = fmi.forecast_by_place_name(
                place_name,
                timestep_hours=timestep_hours,
                forecast_points=forecast_points,
            )
        except ClientError as err:
            LOGGER.warning(
                "Client error while fetching forecast for %s (status %s): %s",
                place_name,
                err.status_code,
                err.message,
            )
            return []
        except ServerError as err:  # pragma: no cover - network interactions
            LOGGER.error(
                "Server error while fetching forecast for %s (status %s): %s",
                place_name,
                err.status_code,
                err.body,
            )
            return []

        if forecast is None:
            return []

        points: List[ForecastPoint] = []
        for entry in getattr(forecast, "forecasts", []):
            points.append(
                ForecastPoint(
                    place=getattr(forecast, "place", place_name),
                    timestamp=self._extract_time(entry),
                    temperature=self._extract_value(entry, ["temperature"]),
                    wind_speed=self._extract_value(entry, ["wind_speed", "windspeed"]),
                )
            )
        return points


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
