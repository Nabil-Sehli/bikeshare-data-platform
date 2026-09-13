"""Data ingestion with dlt: GBFS reference data -> raw_gbfs.

Loads station_information (name, coordinates, capacity) and system_regions,
merged on their natural keys so the tables always reflect the current network.

Usage:
    python -m bikeshare.ingestion.dlt_stations
"""

from __future__ import annotations

from datetime import UTC, datetime

import dlt
import requests

from bikeshare.config import GBFS_BASE_URL
from bikeshare.ingestion.dlt_common import run_pipeline


def get_feed(name: str) -> dict:
    resp = requests.get(f"{GBFS_BASE_URL}/{name}.json", timeout=30)
    resp.raise_for_status()
    return resp.json()


@dlt.source(name="gbfs")
def gbfs():
    @dlt.resource(name="stations", write_disposition="merge", primary_key="station_id")
    def stations():
        feed = get_feed("station_information")
        loaded_at = datetime.now(UTC)
        for s in feed["data"]["stations"]:
            yield {
                "station_id": s["station_id"],
                "short_name": s.get("short_name"),
                "name": s.get("name"),
                "region_id": s.get("region_id"),
                "lat": s.get("lat"),
                "lon": s.get("lon"),
                "capacity": s.get("capacity"),
                "feed_updated_at": datetime.fromtimestamp(feed["last_updated"], tz=UTC),
                "loaded_at": loaded_at,
            }

    @dlt.resource(name="regions", write_disposition="merge", primary_key="region_id")
    def regions():
        for r in get_feed("system_regions")["data"]["regions"]:
            yield {"region_id": r["region_id"], "name": r["name"]}

    return stations, regions


def main() -> None:
    run_pipeline("gbfs_reference", "raw_gbfs", gbfs())


if __name__ == "__main__":
    main()
