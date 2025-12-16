"""Kafka producer and consumer for streaming FMI observations."""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Callable, Iterable, List

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from .config import CONFIG
from .fmi_client import FMIClient, Observation, observations_as_dataframe
from . import transformations

LOGGER = logging.getLogger(__name__)


def _connect_with_retries(
    factory: Callable[[], object],
    component: str,
    attempts: int = 5,
    delay_seconds: int = 2,
):
    """Create Kafka clients with simple retry logic while the broker starts up."""

    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return factory()
        except NoBrokersAvailable as exc:  # pragma: no cover - depends on live Kafka
            last_error = exc
            if attempt == attempts:
                break
            LOGGER.warning(
                "Kafka %s unavailable (attempt %s/%s): %s. Retrying in %ss.",
                component,
                attempt,
                attempts,
                exc,
                delay_seconds,
            )
            time.sleep(delay_seconds)

    LOGGER.error(
        "Kafka %s unavailable after %s attempts: %s.", component, attempts, last_error
    )
    raise last_error


class ObservationProducer:
    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None):
        self.topic = topic or CONFIG.kafka_topic
        # KafkaProducer serialises Observation dicts as UTF-8 JSON strings for portability
        self.producer = _connect_with_retries(
            lambda: KafkaProducer(
                bootstrap_servers=bootstrap_servers or CONFIG.kafka_bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            ),
            component="producer",
        )

    def publish_batch(self, observations: Iterable[Observation]) -> int:
        count = 0
        for obs in observations:
            self.producer.send(self.topic, obs)
            count += 1
        self.producer.flush()
        LOGGER.info("Published %s messages to %s", count, self.topic)
        return count

    def publish_latest(self) -> int:
        client = FMIClient()
        observations = client.fetch_latest()
        return self.publish_batch(observations)

    def publish_last_three_years(self) -> int:
        client = FMIClient()
        observations = client.fetch_last_three_years()
        return self.publish_batch(observations)


class BigQuerySink:
    def __init__(
        self,
        dataset: str | None = None,
        project_id: str | None = None,
        credentials_path: str | None = None,
    ):
        self.dataset = dataset or CONFIG.bigquery_dataset
        self.project_id = project_id or CONFIG.bigquery_project
        self.daily_table = CONFIG.daily_table
        self.long_term_table = CONFIG.long_term_table
        self.station_whitelist = set(CONFIG.station_whitelist)
        self.credentials_path = credentials_path or CONFIG.bigquery_api_key_path

        # BigQuery client is lazy-loaded to avoid dependency issues during unit tests
        self._bq_client = None
        self._credentials = None

    @property
    def bq_client(self):
        if self._bq_client is None:  # pragma: no cover - requires google cloud
            from google.cloud import bigquery

            try:
                self._bq_client = bigquery.Client(
                    project=self.project_id, credentials=self.credentials
                )
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error(
                    "Failed to initialise BigQuery client for project %s using credentials %s: %s. "
                    "Ensure the service account has BigQuery access and that the network can reach the BigQuery API.",
                    self.project_id,
                    self.credentials_path or "application default credentials",
                    exc,
                )
                raise
        return self._bq_client

    @property
    def credentials(self):  # pragma: no cover - requires google cloud
        if self._credentials is None and self.credentials_path:
            path = Path(self.credentials_path).expanduser()
            if path.exists():
                from google.oauth2 import service_account

                try:
                    self._credentials = (
                        service_account.Credentials.from_service_account_file(path)
                    )
                    LOGGER.info("Loaded BigQuery credentials from %s", path)
                except Exception as exc:  # pylint: disable=broad-except
                    LOGGER.error(
                        "Unable to parse BigQuery credentials at %s: %s. Confirm the file is a valid service account JSON.",
                        path,
                        exc,
                    )
                    raise
            else:
                LOGGER.error(
                    "BigQuery API key file not found at %s; default credentials will be used if available. "
                    "Set BIGQUERY_API_KEY_PATH or mount the service account file for reliable uploads.",
                    path,
                )
        return self._credentials

    def _verify_row_persistence(self, destination_table: str, expected_rows: int) -> None:
        """Confirm rows landed in BigQuery and emit actionable error logs if not.

        The verification step is best-effort; the upload should not be considered
        successful unless the table metadata shows at least the number of rows we
        attempted to append. We avoid failing fast on auth/network errors to keep
        local development flows usable.
        """

        try:  # pragma: no cover - requires live BigQuery
            table = self.bq_client.get_table(destination_table)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning(
                "Unable to verify BigQuery load for %s: %s. Double-check credentials and table access.",
                destination_table,
                exc,
            )
            return

        if table.num_rows < expected_rows:
            LOGGER.error(
                "BigQuery load verification failed for %s: expected at least %s rows but found %s.",
                destination_table,
                expected_rows,
                table.num_rows,
            )
        else:
            LOGGER.info(
                "Verified %s rows now present in %s (table total: %s rows).",
                expected_rows,
                destination_table,
                table.num_rows,
            )

    def _upload_and_verify(self, frame, destination_table: str) -> None:
        """Upload a dataframe to BigQuery with detailed error logging."""

        try:  # pragma: no cover - requires google cloud
            frame.to_gbq(
                destination_table=destination_table,
                project_id=self.project_id,
                credentials=self.credentials,
                if_exists="append",
            )
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error(
                "Failed to upload %s rows to %s in project %s using credentials %s: %s. "
                "Verify that the dataset exists and that the account has bigquery.dataEditor access.",
                len(frame),
                destination_table,
                self.project_id,
                self.credentials_path or "application default credentials",
                exc,
            )
            raise

        self._verify_row_persistence(destination_table, expected_rows=len(frame))

    def write_daily_batch(self, observations: List[Observation]):
        frame = observations_as_dataframe(observations)
        if frame.empty:
            LOGGER.warning("Received empty batch; nothing to load")
            return 0

        frame = transformations.prepare_for_bigquery(frame)

        destination_table = f"{self.dataset}.{self.daily_table}"
        LOGGER.info(
            "Loading %s rows to %s in project %s",
            len(frame),
            destination_table,
            self.project_id,
        )
        self._upload_and_verify(frame, destination_table=destination_table)
        self._update_long_term_table(frame)
        return len(frame)

    def _update_long_term_table(self, frame):
        if frame.empty:
            return

        filtered = frame[frame["station_id"].isin(self.station_whitelist)]
        if filtered.empty:
            LOGGER.info("No whitelisted stations present; skipping long-term update")
            return

        destination_table = f"{self.dataset}.{self.long_term_table}"
        LOGGER.info(
            "Updating long-term table %s with %s rows across %s stations",
            destination_table,
            len(filtered),
            filtered["station_id"].nunique(),
        )
        self._upload_and_verify(filtered, destination_table=destination_table)


class ObservationConsumer:
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str = "fmi-ingestion",
    ):
        self.topic = topic or CONFIG.kafka_topic
        self.consumer = _connect_with_retries(
            lambda: KafkaConsumer(
                self.topic,
                bootstrap_servers=bootstrap_servers or CONFIG.kafka_bootstrap_servers,
                value_deserializer=lambda message: json.loads(message.decode("utf-8")),
                enable_auto_commit=True,
                group_id=group_id,
                auto_offset_reset="earliest",
                consumer_timeout_ms=5000,  # lopettaa jos 5s ei tule uusia viestejä
            ),
            component="consumer",
        )

        self.sink = BigQuerySink()

    def consume_once(self, max_messages: int | None = None) -> int:
        # Collect Kafka messages in-memory so we can upload in a single BigQuery batch
        buffer: List[Observation] = []
        for index, message in enumerate(self.consumer):
            buffer.append(message.value)
            if max_messages is not None and index + 1 >= max_messages:
                break

        if not buffer:
            LOGGER.warning("No messages read from Kafka topic %s", self.topic)
            return 0

        LOGGER.info("Uploading %s messages to BigQuery", len(buffer))
        return self.sink.write_daily_batch(buffer)


def _cli():  # pragma: no cover - convenience entrypoint
    import argparse

    parser = argparse.ArgumentParser(description="Kafka streaming utilities for FMI observations")
    parser.add_argument("action", choices=["produce", "consume"], help="Whether to fetch and publish or consume a batch")
    parser.add_argument("--max-messages", type=int, default=200, help="Number of messages to read when consuming")
    parser.add_argument("--group-id", default="fmi-ingestion", help="Kafka consumer group id")
    args = parser.parse_args()

    if args.action == "produce":
        ObservationProducer().publish_latest()
    else:
        ObservationConsumer(group_id=args.group_id).consume_once(max_messages=args.max_messages)


if __name__ == "__main__":
    _cli()
