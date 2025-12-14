"""Kafka producer and consumer for streaming FMI observations."""
from __future__ import annotations

import json
import logging
from typing import Iterable, List

from kafka import KafkaConsumer, KafkaProducer

from .config import CONFIG
from .fmi_client import FMIClient, Observation, observations_as_dataframe

LOGGER = logging.getLogger(__name__)


class ObservationProducer:
    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None):
        self.topic = topic or CONFIG.kafka_topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers or CONFIG.kafka_bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
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


class BigQuerySink:
    def __init__(self, dataset: str | None = None):
        self.dataset = dataset or CONFIG.bigquery_dataset
        self.daily_table = CONFIG.daily_table
        self.station_whitelist = set(CONFIG.station_whitelist)

        # BigQuery client is lazy-loaded to avoid dependency issues during unit tests
        self._bq_client = None

    @property
    def bq_client(self):
        if self._bq_client is None:  # pragma: no cover - requires google cloud
            from google.cloud import bigquery

            self._bq_client = bigquery.Client()
        return self._bq_client

    def _long_term_table(self, station_id: str) -> str:
        return f"{self.dataset}.{CONFIG.long_term_table_prefix}{station_id}"

    def write_daily_batch(self, observations: List[Observation]):
        frame = observations_as_dataframe(observations)
        if frame.empty:
            LOGGER.warning("Received empty batch; nothing to load")
            return 0

        LOGGER.info("Loading %s rows to %s.%s", len(frame), self.dataset, self.daily_table)
        frame.to_gbq(destination_table=f"{self.dataset}.{self.daily_table}", if_exists="append")
        self._update_long_term_tables(frame)
        return len(frame)

    def _update_long_term_tables(self, frame):
        if frame.empty:
            return
        for station_id, station_frame in frame.groupby("station_id"):
            if station_id not in self.station_whitelist:
                continue
            table_name = self._long_term_table(station_id)
            station_frame.to_gbq(destination_table=table_name, if_exists="append")
            LOGGER.info("Updated long-term table %s with %s rows", table_name, len(station_frame))


class ObservationConsumer:
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str = "fmi-ingestion",
    ):
        self.topic = topic or CONFIG.kafka_topic
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap_servers or CONFIG.kafka_bootstrap_servers,
            value_deserializer=lambda message: json.loads(message.decode("utf-8")),
            enable_auto_commit=True,
            group_id=group_id,
            auto_offset_reset="earliest",
        )
        self.sink = BigQuerySink()

    def consume_once(self, max_messages: int = 200) -> int:
        buffer: List[Observation] = []
        for index, message in enumerate(self.consumer):
            buffer.append(message.value)
            if index + 1 >= max_messages:
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
    args = parser.parse_args()

    if args.action == "produce":
        ObservationProducer().publish_latest()
    else:
        ObservationConsumer().consume_once(max_messages=args.max_messages)


if __name__ == "__main__":
    _cli()
