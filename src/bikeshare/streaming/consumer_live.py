"""Module 7 - Streaming: Kafka -> live serving table in Postgres.

Consumer group `live-view` (independent from `lake-writer`, so both receive
every message). Upserts the latest status per station into
live.station_status_latest, which powers the real-time dashboard tile.
"""

from __future__ import annotations

import json
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import execute_values

from bikeshare.config import pg_dsn
from bikeshare.streaming.common import TOPIC, Shutdown, bootstrap_servers, ensure_topic, log, setup_logging
from bikeshare.warehouse.bootstrap import SQL as BOOTSTRAP_SQL

BATCH_SECONDS = 5

COLUMNS = [
    "station_id", "num_bikes_available", "num_ebikes_available", "num_bikes_disabled",
    "num_docks_available", "num_docks_disabled", "is_installed", "is_renting", "is_returning",
    "last_reported", "feed_updated_at", "ingested_at",
]

UPSERT = f"""
INSERT INTO live.station_status_latest ({", ".join(COLUMNS)}) VALUES %s
ON CONFLICT (station_id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS[1:])},
    updated_at = now()
WHERE live.station_status_latest.last_reported IS NULL
   OR EXCLUDED.last_reported >= live.station_status_latest.last_reported
"""


def connect_with_retry(retries: int = 30):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(pg_dsn())
            with conn, conn.cursor() as cur:
                cur.execute(BOOTSTRAP_SQL)
            return conn
        except psycopg2.Error as exc:
            log.warning("waiting for Postgres / live schema (%d/%d): %s", attempt, retries, exc)
            time.sleep(5)
    raise RuntimeError("Postgres not reachable")


def main() -> None:
    setup_logging()
    ensure_topic()
    conn = connect_with_retry()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": "live-view",
            # replaying retained history on first start is safe: the upsert keeps the newest report
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    shutdown = Shutdown()
    latest: dict[str, tuple] = {}
    last_flush = time.monotonic()

    try:
        while not shutdown.requested:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                record = json.loads(msg.value())
                latest[record["station_id"]] = tuple(record.get(c) for c in COLUMNS)
            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("consumer error: %s", msg.error())

            if latest and time.monotonic() - last_flush >= BATCH_SECONDS:
                with conn, conn.cursor() as cur:
                    execute_values(cur, UPSERT, list(latest.values()))
                consumer.commit(asynchronous=False)
                log.info("upserted %d stations into live.station_status_latest", len(latest))
                latest.clear()
                last_flush = time.monotonic()
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
