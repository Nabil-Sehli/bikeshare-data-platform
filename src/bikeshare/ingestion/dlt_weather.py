"""Workshop - data ingestion with dlt: Open-Meteo hourly weather -> raw_weather.

Incremental: dlt keeps the last loaded date per city in its state. Each run
re-fetches the last few days (recent data is provisional and gets corrected),
and the `merge` write disposition on (city, observed_at) upserts them.

Usage:
    python -m bikeshare.ingestion.dlt_weather [--start-date 2025-01-01] [--cities JC NYC]
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

import dlt
import requests

from bikeshare.config import CITIES, City, env
from bikeshare.ingestion.dlt_common import run_pipeline

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "wind_speed_10m",
    "weather_code",
]
ARCHIVE_DELAY_DAYS = 6    # the reanalysis archive lags a few days behind
REFETCH_DAYS = 3          # re-load recent days to pick up corrected values


def fetch(url: str, c: City, start: date, end: date) -> list[dict]:
    params = {
        "latitude": c.latitude,
        "longitude": c.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": c.timezone,
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    hourly = resp.json()["hourly"]
    rows = []
    for i, ts in enumerate(hourly["time"]):
        row = {"city": c.code, "observed_at": datetime.fromisoformat(ts), "source": url.split("//")[1].split(".")[0]}
        row.update({v: hourly[v][i] for v in HOURLY_VARS})
        if row["temperature_2m"] is not None:
            rows.append(row)
    return rows


def date_chunks(start: date, end: date, days: int = 180):
    while start <= end:
        chunk_end = min(end, start + timedelta(days=days - 1))
        yield start, chunk_end
        start = chunk_end + timedelta(days=1)


@dlt.source(name="open_meteo")
def open_meteo(city_codes: list[str], start_date: date):
    @dlt.resource(
        name="weather_hourly",
        write_disposition="merge",
        primary_key=["city", "observed_at"],
        columns={"observed_at": {"data_type": "timestamp", "timezone": False}},
    )
    def weather_hourly():
        state = dlt.current.resource_state()
        today = date.today()
        archive_end = today - timedelta(days=ARCHIVE_DELAY_DAYS)
        for code in city_codes:
            c = CITIES[code]
            last = state.get(code)
            start = max(start_date, date.fromisoformat(last) - timedelta(days=REFETCH_DAYS)) if last else start_date

            if start <= archive_end:
                for s, e in date_chunks(start, archive_end):
                    yield fetch(ARCHIVE_URL, c, s, e)
            recent_start = max(start, archive_end + timedelta(days=1))
            if recent_start <= today:
                yield fetch(FORECAST_URL, c, recent_start, today)
            state[code] = today.isoformat()

    return weather_hourly


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", default=env("WEATHER_START_DATE", "2025-01-01"))
    parser.add_argument("--cities", nargs="+", default=sorted(CITIES))
    args = parser.parse_args()
    run_pipeline(
        "open_meteo_weather",
        "raw_weather",
        open_meteo([c.upper() for c in args.cities], date.fromisoformat(args.start_date)),
    )


if __name__ == "__main__":
    main()
