"""Spark transformation tests - run inside the pipelines image (needs Java)."""

import pytest
from pyspark.sql import SparkSession

from bikeshare.spark.process_trips import RAW_SCHEMA, clean_trips, station_daily_flows


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def row(ride_id, start, end, start_station="A", end_station="B", member="member"):
    return (ride_id, "classic_bike", start, end, "Station A", start_station, "Station B", end_station,
            "40.7194", "-74.0510", "40.7272", "-74.0338", member)


def test_clean_trips_applies_quality_rules(spark):
    raw = spark.createDataFrame(
        [
            row("ok", "2025-06-03 19:48:09.552", "2025-06-03 20:07:44.535"),
            row("ok", "2025-06-03 19:48:09.552", "2025-06-03 20:07:44.535"),       # duplicate
            row("too_short", "2025-06-03 10:00:00", "2025-06-03 10:00:30"),         # < 1 minute
            row("negative", "2025-06-03 10:00:00", "2025-06-03 09:00:00"),          # ends before start
            row("other_month", "2025-05-31 23:50:00", "2025-06-01 00:10:00"),       # belongs to May file
            row("bad_ts", "not-a-date", "2025-06-03 10:00:00"),                      # unparseable
            row("bad_member", "2025-06-03 10:00:00", "2025-06-03 10:20:00", member="robot"),
            row(None, "2025-06-03 10:00:00", "2025-06-03 10:20:00"),                 # no id
        ],
        RAW_SCHEMA,
    )
    result = clean_trips(raw, "JC", "2025-06").collect()

    assert [r.ride_id for r in result] == ["ok"]
    trip = result[0]
    assert trip.city == "JC"
    # second precision: 19:48:09 -> 20:07:44 is 19 min 35 s
    assert trip.duration_min == pytest.approx(19.58, abs=0.001)
    assert trip.distance_km == pytest.approx(1.69, abs=0.05)
    assert trip.is_round_trip is False


def test_station_daily_flows_nets_arrivals_and_departures(spark):
    raw = spark.createDataFrame(
        [
            row("t1", "2025-06-03 08:00:00", "2025-06-03 08:20:00", "A", "B"),
            row("t2", "2025-06-03 09:00:00", "2025-06-03 09:20:00", "A", "B"),
            row("t3", "2025-06-03 18:00:00", "2025-06-03 18:20:00", "B", "A"),
        ],
        RAW_SCHEMA,
    )
    flows = {r.station_id: r for r in station_daily_flows(clean_trips(raw, "JC", "2025-06")).collect()}

    assert (flows["A"].departures, flows["A"].arrivals, flows["A"].net_flow) == (2, 1, -1)
    assert (flows["B"].departures, flows["B"].arrivals, flows["B"].net_flow) == (1, 2, 1)
