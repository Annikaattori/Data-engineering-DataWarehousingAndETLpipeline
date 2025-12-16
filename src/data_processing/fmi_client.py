"""Client helpers for retrieving observations and forecasts from FMI.

The implementation now uses ``fmi-weather-client`` to follow the recommended
API integration pattern for both observations and forecasts while still
supporting offline development via bundled fixtures.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
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

    _history_cache: List[Observation] = []
    _history_fetched_at: datetime | None = None
    _history_ttl = timedelta(minutes=30)

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
        if candidate is None and hasattr(payload, "data"):
            candidate = getattr(payload.data, "time", None)
        if candidate is None:
            return utc_now_iso()
        if hasattr(candidate, "isoformat"):
            return candidate.isoformat()
        return str(candidate)

    def _extract_value(self, payload, keys: Iterable[str]) -> Optional[float]:
        for key in keys:
            value = getattr(payload, key, None)
            if value is None and hasattr(payload, "data"):
                value = getattr(payload.data, key, None)
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

    def _fetch_station_observation_history(
        self, station_id: str, start_time: datetime, end_time: datetime
    ) -> List[Observation]:
        if not hasattr(fmi, "observations_by_station_id"):
            LOGGER.warning(
                "Historical fetch unavailable; falling back to latest observation for station %s", station_id
            )
            latest = self._fetch_station_observation(station_id)
            return [latest] if latest else []

        try:
            history = fmi.observations_by_station_id(
                int(station_id), start_time=start_time, end_time=end_time
            )
        except ClientError as err:
            LOGGER.warning(
                "Client error while fetching history for %s (status %s): %s",
                station_id,
                err.status_code,
                err.message,
            )
            return []
        except ServerError as err:  # pragma: no cover - network interactions
            LOGGER.error(
                "Server error while fetching history for %s (status %s): %s",
                station_id,
                err.status_code,
                err.body,
            )
            return []

        if not history:
            LOGGER.warning("No historical observations returned for station %s", station_id)
            return []

        entries = getattr(history, "observations", history)
        observations: List[Observation] = []
        for payload in entries:
            observation = self._build_observation(station_id, payload)
            if observation:
                observations.append(observation)
        return observations

    def _filter_window(
        self, observations: List[Observation], start: datetime, end: datetime
    ) -> List[Observation]:
        if not observations:
            return []

        filtered: List[Observation] = []
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

            if start <= parsed <= end:
                filtered.append(obs)
        return filtered

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

    def fetch_last_three_years(self) -> List[Observation]:
        """Return historical observations starting from 2022 in half-hour resolution."""

        now = datetime.now(timezone.utc)
        if self.__class__._history_fetched_at and now - self.__class__._history_fetched_at < self._history_ttl:
            LOGGER.info(
                "Returning cached historical observations fetched at %s",
                self.__class__._history_fetched_at,
            )
            return list(self.__class__._history_cache)

        start = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end = now

        if self.use_sample_data:
            sample_path = DATA_DIR / "sample_observations.json"
            with sample_path.open("r", encoding="utf-8") as file:
                sample = json.load(file)
            filtered_sample = self._filter_window(sample, start, end)
            self.__class__._history_cache = filtered_sample
            self.__class__._history_fetched_at = now
            return filtered_sample

        observations: List[Observation] = []
        for station_id in self.station_ids:
            history = self._fetch_station_observation_history(station_id, start, end)
            observations.extend(self._filter_window(history, start, end))
        self.__class__._history_cache = observations
        self.__class__._history_fetched_at = now
        return observations

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

    def fetch_forecasts_last_three_years(
        self,
        *,
        places: Iterable[str] | None = None,
        timestep_hours: int = 1,
        forecast_points: int = 168,
    ) -> List[ForecastPoint]:
        """Fetch forecasts and keep only entries within the last three years."""

        if self.use_sample_data:
            LOGGER.info("Sample mode enabled; skipping historical forecast fetch")
            return []

        place_list = list(places) if places is not None else (self.forecast_places or [])
        if not place_list:
            LOGGER.info("No forecast places supplied; skipping forecast retrieval")
            return []

        collected: List[ForecastPoint] = []
        for place in place_list:
            collected.extend(
                self.fetch_forecast(
                    place,
                    timestep_hours=timestep_hours,
                    forecast_points=forecast_points,
                )
            )

        if not collected:
            return []

        three_years_ago = datetime.now(timezone.utc) - timedelta(days=365 * 3)
        now = datetime.now(timezone.utc)
        filtered: List[ForecastPoint] = []
        for entry in collected:
            timestamp = entry.get("timestamp")
            if not timestamp:
                continue

            cleaned_timestamp = str(timestamp).replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(cleaned_timestamp)
            except ValueError:
                LOGGER.debug("Unable to parse forecast timestamp %s", timestamp)
                continue

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            if three_years_ago <= parsed <= now:
                filtered.append(entry)

        return filtered


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
