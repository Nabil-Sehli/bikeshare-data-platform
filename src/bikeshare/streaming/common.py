"""Shared Kafka settings, message schema and graceful-shutdown handling."""

from __future__ import annotations

import logging
import signal
import time

import pyarrow as pa
from confluent_kafka.admin import AdminClient, NewTopic

from bikeshare.config import env

log = logging.getLogger("streaming")

TOPIC = env("KAFKA_TOPIC_STATION_STATUS", "bikeshare.station_status")

# One Kafka message = one station status change. Also the Parquet schema in the lake.
STATUS_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("num_bikes_available", pa.int32()),
        ("num_ebikes_available", pa.int32()),
        ("num_bikes_disabled", pa.int32()),
        ("num_docks_available", pa.int32()),
        ("num_docks_disabled", pa.int32()),
        ("is_installed", pa.bool_()),
        ("is_renting", pa.bool_()),
        ("is_returning", pa.bool_()),
        ("last_reported", pa.timestamp("s", tz="UTC")),
        ("feed_updated_at", pa.timestamp("s", tz="UTC")),
        ("ingested_at", pa.timestamp("ms", tz="UTC")),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
    ]
)


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def bootstrap_servers() -> str:
    return env("KAFKA_BOOTSTRAP", "kafka:9092")


def ensure_topic(partitions: int = 3, retries: int = 30) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers()})
    for attempt in range(1, retries + 1):
        try:
            existing = admin.list_topics(timeout=10).topics
            if TOPIC in existing:
                return
            futures = admin.create_topics(
                [NewTopic(TOPIC, num_partitions=partitions, replication_factor=1,
                          config={"retention.ms": str(3 * 24 * 3600 * 1000)})]
            )
            futures[TOPIC].result()
            log.info("created topic %s (%d partitions)", TOPIC, partitions)
            return
        except Exception as exc:  # broker not ready yet, or topic created concurrently
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                return
            log.warning("waiting for Kafka (%d/%d): %s", attempt, retries, exc)
            time.sleep(5)
    raise RuntimeError("Kafka not reachable")


class Shutdown:
    """Flip `requested` on SIGTERM/SIGINT so loops can flush and exit cleanly."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, _frame) -> None:
        log.info("received signal %s, shutting down", signum)
        self.requested = True
