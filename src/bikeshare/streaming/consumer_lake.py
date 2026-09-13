"""Streaming: Kafka -> data lake (Parquet on MinIO).

Consumer group `lake-writer`. Buffers messages and flushes a Parquet file every
FLUSH_SECONDS or FLUSH_MAX_ROWS, partitioned by ingestion date/hour:

    s3://bikeshare-lake/streaming/station_status/dt=YYYY-MM-DD/hour=HH/part-<ts>-<p>.parquet

Offsets are committed only after the file is written (at-least-once delivery;
duplicates are removed downstream in dbt).
"""

from __future__ import annotations

import io
import json
import time
import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError

from bikeshare.config import env, lake_bucket
from bikeshare.lake import s3_client
from bikeshare.streaming.common import (
    STATUS_SCHEMA,
    TOPIC,
    Shutdown,
    bootstrap_servers,
    ensure_topic,
    log,
    setup_logging,
)

FLUSH_SECONDS = int(env("LAKE_FLUSH_SECONDS", "300"))
FLUSH_MAX_ROWS = int(env("LAKE_FLUSH_MAX_ROWS", "50000"))


def parse_ts(value: str | None):
    return datetime.fromisoformat(value) if value else None


def to_row(msg) -> dict:
    record = json.loads(msg.value())
    for field in ("last_reported", "feed_updated_at", "ingested_at"):
        record[field] = parse_ts(record.get(field))
    record["kafka_partition"] = msg.partition()
    record["kafka_offset"] = msg.offset()
    return record


def write_parquet(rows: list[dict]) -> str:
    now = datetime.now(UTC)
    key = (
        f"streaming/station_status/dt={now:%Y-%m-%d}/hour={now:%H}/"
        f"part-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.parquet"
    )
    table = pa.Table.from_pylist(rows, schema=STATUS_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    s3_client().put_object(Bucket=lake_bucket(), Key=key, Body=buf.getvalue())
    return key


def main() -> None:
    setup_logging()
    ensure_topic()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": "lake-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    shutdown = Shutdown()
    buffer: list[dict] = []
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal buffer, last_flush
        if buffer:
            key = write_parquet(buffer)
            consumer.commit(asynchronous=False)
            log.info("wrote %d rows to s3://%s/%s", len(buffer), lake_bucket(), key)
            buffer = []
        last_flush = time.monotonic()

    try:
        while not shutdown.requested:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("consumer error: %s", msg.error())
                else:
                    buffer.append(to_row(msg))
            if len(buffer) >= FLUSH_MAX_ROWS or time.monotonic() - last_flush >= FLUSH_SECONDS:
                flush()
        flush()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
