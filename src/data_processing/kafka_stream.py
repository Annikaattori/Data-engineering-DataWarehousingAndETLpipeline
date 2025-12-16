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

    def publish_latest_hourly(self) -> int:
        client = FMIClient()
        observations = client.fetch_latest_hourly()
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
        self.observations_table = CONFIG.hourly_table
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

    def _table_exists(self, table_id: str) -> bool:
        try:  # pragma: no cover - requires live BigQuery
            from google.cloud.exceptions import NotFound

            self.bq_client.get_table(table_id)
            return True
        except NotFound:
            LOGGER.info("BigQuery table %s not found", table_id)
            return False
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("Unable to verify existence of %s: %s", table_id, exc)
            return False

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

        destination_table = f"{self.dataset}.{self.observations_table}"
        LOGGER.info(
            "Loading %s rows to %s in project %s",
            len(frame),
            destination_table,
            self.project_id,
        )
        self._upload_and_verify(frame, destination_table=destination_table)
        return len(frame)

    def write_hourly_batch(self, observations: List[Observation]):
        frame = observations_as_dataframe(observations)
        if frame.empty:
            LOGGER.warning("Received empty batch; nothing to load")
            return 0

        frame = transformations.prepare_hourly_for_bigquery(frame)

        destination_table = f"{self.dataset}.{self.observations_table}"
        LOGGER.info(
            "Loading %s hourly rows to %s in project %s",
            len(frame),
            destination_table,
            self.project_id,
        )
        self._upload_and_verify(frame, destination_table=destination_table)
        return len(frame)


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
            ),
            component="consumer",
        )

        self.sink = BigQuerySink()

    def _flush_buffer(self, buffer: List[Observation]) -> int:
        if not buffer:
            return 0

        LOGGER.info("Uploading %s messages to BigQuery", len(buffer))
        ingested = self.sink.write_hourly_batch(buffer)
        buffer.clear()
        return ingested

    def consume_forever(
        self, batch_size: int = 500, flush_interval_seconds: int = 10
    ) -> None:  # pragma: no cover - long-running loop
        LOGGER.info(
            "Starting continuous consumer with batch size %s and flush interval %ss",
            batch_size,
            flush_interval_seconds,
        )
        buffer: List[Observation] = []
        last_flush = time.monotonic()

        while True:
            message_pack = self.consumer.poll(timeout_ms=1000)
            for messages in message_pack.values():
                for message in messages:
                    buffer.append(message.value)
                    if len(buffer) >= batch_size:
                        self._flush_buffer(buffer)
                        last_flush = time.monotonic()

            if buffer and time.monotonic() - last_flush >= flush_interval_seconds:
                self._flush_buffer(buffer)
                last_flush = time.monotonic()


class HourlyIngestionService:
    """Run periodic hourly ingestion directly to BigQuery."""

    def __init__(
        self,
        *,
        sink: BigQuerySink | None = None,
        client: FMIClient | None = None,
        interval_seconds: int = 3600,
    ):
        self.sink = sink or BigQuerySink()
        self.client = client or FMIClient()
        self.interval_seconds = interval_seconds

    def run_once(self) -> int:
        observations = self.client.fetch_latest_hourly()
        return self.sink.write_hourly_batch(observations)

    def run_forever(self) -> None:  # pragma: no cover - long-running loop
        LOGGER.info(
            "Starting hourly ingestion service with %s second interval", self.interval_seconds
        )
        while True:
            ingested = self.run_once()
            LOGGER.info("Hourly ingestion cycle completed with %s rows", ingested)
            time.sleep(self.interval_seconds)


def _cli():  # pragma: no cover - convenience entrypoint
    import argparse

    parser = argparse.ArgumentParser(description="Kafka streaming utilities for FMI observations")
    parser.add_argument(
        "action",
        choices=["produce", "consume", "bootstrap-hourly"],
        help="Whether to fetch and publish, consume continuously, or run the hourly bootstrap",
    )
    parser.add_argument(
        "--mode",
        choices=["latest-hourly", "latest"],
        default="latest-hourly",
        help="Producer mode: hourly sampling or raw latest observations",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of messages to accumulate before uploading to BigQuery",
    )
    parser.add_argument("--group-id", default="fmi-ingestion", help="Kafka consumer group id")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=3600,
        help="Polling interval for the hourly ingestion service",
    )
    args = parser.parse_args()

    if args.action == "produce":
        producer = ObservationProducer()
        if args.mode == "latest-hourly":
            producer.publish_latest_hourly()
        else:
            producer.publish_latest()
    elif args.action == "consume":
        ObservationConsumer(group_id=args.group_id).consume_forever(
            batch_size=args.batch_size
        )
    else:
        HourlyIngestionService(interval_seconds=args.interval_seconds).run_forever()


if __name__ == "__main__":
    _cli()
