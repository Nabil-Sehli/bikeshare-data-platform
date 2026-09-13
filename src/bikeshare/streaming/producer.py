"""Module 7 - Streaming: GBFS station_status -> Kafka.

Polls the Citi Bike GBFS feed every GBFS_POLL_SECONDS and publishes one message
per station whose `last_reported` changed since the previous poll. Messages are
keyed by station_id, so all updates for a station land in the same partition
and stay ordered.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import requests
from confluent_kafka import Producer

from bikeshare.config import GBFS_BASE_URL, env
from bikeshare.streaming.common import TOPIC, Shutdown, bootstrap_servers, ensure_topic, log, setup_logging

STATUS_URL = f"{GBFS_BASE_URL}/station_status.json"


def iso(epoch: int | None) -> str | None:
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC).isoformat()


def to_message(station: dict, feed_updated: int, ingested_at: str) -> dict:
    return {
        "station_id": station["station_id"],
        "num_bikes_available": station.get("num_bikes_available"),
        "num_ebikes_available": station.get("num_ebikes_available"),
        "num_bikes_disabled": station.get("num_bikes_disabled"),
        "num_docks_available": station.get("num_docks_available"),
        "num_docks_disabled": station.get("num_docks_disabled"),
        "is_installed": bool(station.get("is_installed")),
        "is_renting": bool(station.get("is_renting")),
        "is_returning": bool(station.get("is_returning")),
        "last_reported": iso(station.get("last_reported")),
        "feed_updated_at": iso(feed_updated),
        "ingested_at": ingested_at,
    }


def main() -> None:
    setup_logging()
    ensure_topic()
    poll_seconds = int(env("GBFS_POLL_SECONDS", "60"))
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "client.id": "gbfs-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 100,
        }
    )
    failures = {"count": 0}

    def on_delivery(err, _msg):
        if err is not None:
            failures["count"] += 1
            log.error("delivery failed: %s", err)

    shutdown = Shutdown()
    last_seen: dict[str, int] = {}
    session = requests.Session()

    while not shutdown.requested:
        started = time.monotonic()
        try:
            resp = session.get(STATUS_URL, timeout=20)
            resp.raise_for_status()
            feed = resp.json()
            ingested_at = datetime.now(UTC).isoformat()
            changed = 0
            for station in feed["data"]["stations"]:
                sid, reported = station["station_id"], station.get("last_reported") or 0
                if last_seen.get(sid) == reported:
                    continue
                last_seen[sid] = reported
                producer.produce(
                    TOPIC,
                    key=sid,
                    value=json.dumps(to_message(station, feed.get("last_updated"), ingested_at)),
                    on_delivery=on_delivery,
                )
                changed += 1
                producer.poll(0)
            producer.flush(30)
            log.info("published %d/%d station updates (delivery failures so far: %d)",
                     changed, len(feed["data"]["stations"]), failures["count"])
        except requests.RequestException as exc:
            log.warning("GBFS request failed, will retry: %s", exc)

        while not shutdown.requested and time.monotonic() - started < poll_seconds:
            time.sleep(1)

    producer.flush(30)


if __name__ == "__main__":
    main()
