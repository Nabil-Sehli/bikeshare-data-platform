"""Central configuration, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class City:
    code: str
    name: str
    trip_file_prefix: str
    latitude: float
    longitude: float
    timezone: str = "America/New_York"


CITIES: dict[str, City] = {
    "NYC": City("NYC", "New York City", "", 40.7128, -74.0060),
    "JC": City("JC", "Jersey City & Hoboken", "JC-", 40.7178, -74.0431),
}

GBFS_BASE_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en"
TRIPDATA_URL = "https://s3.amazonaws.com/tripdata"


def city(code: str) -> City:
    try:
        return CITIES[code.upper()]
    except KeyError as exc:
        raise ValueError(f"Unknown city {code!r}; choose one of {sorted(CITIES)}") from exc


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of YYYY-MM strings, e.g. ('2025-11', '2026-01')."""
    sy, sm = (int(p) for p in start.split("-"))
    ey, em = (int(p) for p in end.split("-"))
    if (sy, sm) > (ey, em):
        raise ValueError(f"start month {start} is after end month {end}")
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def month_start(month: str) -> date:
    y, m = (int(p) for p in month.split("-"))
    return date(y, m, 1)


def pg_dsn() -> str:
    return (
        f"postgresql://{env('PG_USER')}:{env('PG_PASSWORD')}"
        f"@{env('PG_HOST', 'postgres')}:{env('PG_PORT', '5432')}/{env('PG_DATABASE', 'bikeshare')}"
    )


def pg_jdbc_url() -> str:
    return (
        f"jdbc:postgresql://{env('PG_HOST', 'postgres')}:{env('PG_PORT', '5432')}"
        f"/{env('PG_DATABASE', 'bikeshare')}?reWriteBatchedInserts=true"
    )


def lake_bucket() -> str:
    return env("LAKE_BUCKET", "bikeshare-lake")
