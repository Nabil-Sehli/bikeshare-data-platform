"""Batch processing with Spark.

For each month of raw trip CSVs in the lake:
  1. enforce a schema, parse timestamps, deduplicate, drop invalid trips
  2. derive duration, haversine distance, round-trip flag
  3. write curated Parquet back to the lake   (curated/trips/city=/month=)
  4. aggregate daily departures/arrivals/net-flow per station
  5. load both into Postgres (raw.trips is range-partitioned by month)
  6. record row counts in raw.trip_load_audit

Usage:
    python -m bikeshare.spark.process_trips --city JC --start-month 2025-01 --end-month 2025-03
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from bikeshare.config import env, lake_bucket, month_range, month_start, pg_dsn, pg_jdbc_url
from bikeshare.lake import delete_prefix, object_exists
from bikeshare.spark.session import build_spark
from bikeshare.warehouse.bootstrap import SQL as BOOTSTRAP_SQL

log = logging.getLogger("process_trips")

RAW_SCHEMA = T.StructType(
    [
        T.StructField("ride_id", T.StringType()),
        T.StructField("rideable_type", T.StringType()),
        T.StructField("started_at", T.StringType()),
        T.StructField("ended_at", T.StringType()),
        T.StructField("start_station_name", T.StringType()),
        T.StructField("start_station_id", T.StringType()),
        T.StructField("end_station_name", T.StringType()),
        T.StructField("end_station_id", T.StringType()),
        T.StructField("start_lat", T.StringType()),
        T.StructField("start_lng", T.StringType()),
        T.StructField("end_lat", T.StringType()),
        T.StructField("end_lng", T.StringType()),
        T.StructField("member_casual", T.StringType()),
    ]
)

MIN_DURATION_MIN = 1.0
MAX_DURATION_MIN = 24 * 60.0

def clean_trips(raw: DataFrame, city_code: str, month: str) -> DataFrame:
    first_day = month_start(month)
    lat_lng = ["start_lat", "start_lng", "end_lat", "end_lng"]

    df = raw.select(
        F.trim("ride_id").alias("ride_id"),
        F.lit(city_code).alias("city"),
        F.lit(first_day).cast("date").alias("source_month"),
        F.lower(F.trim("rideable_type")).alias("rideable_type"),
        F.lower(F.trim("member_casual")).alias("member_type"),
        F.try_to_timestamp(F.col("started_at")).alias("started_at"),
        F.try_to_timestamp(F.col("ended_at")).alias("ended_at"),
        F.nullif(F.trim("start_station_id"), F.lit("")).alias("start_station_id"),
        F.nullif(F.trim("start_station_name"), F.lit("")).alias("start_station_name"),
        F.nullif(F.trim("end_station_id"), F.lit("")).alias("end_station_id"),
        F.nullif(F.trim("end_station_name"), F.lit("")).alias("end_station_name"),
        *[F.expr(f"try_cast({c} AS double)").alias(c) for c in lat_lng],
    )

    duration_min = (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")) / 60.0
    df = (
        df.withColumn("duration_min", F.round(duration_min, 2))
        .withColumn("distance_km", F.round(haversine_km("start_lat", "start_lng", "end_lat", "end_lng"), 3))
        .withColumn(
            "is_round_trip",
            F.coalesce(F.col("start_station_id") == F.col("end_station_id"), F.lit(False)),
        )
    )

    valid = (
        F.col("ride_id").isNotNull()
        & F.col("started_at").isNotNull()
        & F.col("ended_at").isNotNull()
        & F.col("duration_min").between(MIN_DURATION_MIN, MAX_DURATION_MIN)
        & (F.date_trunc("month", "started_at") == F.lit(first_day).cast("timestamp"))
        & F.col("member_type").isin("member", "casual")
    )
    return df.where(valid).dropDuplicates(["ride_id"])


def haversine_km(lat1: str, lng1: str, lat2: str, lng2: str):
    rlat1, rlat2 = F.radians(lat1), F.radians(lat2)
    dlat = rlat2 - rlat1
    dlng = F.radians(lng2) - F.radians(lng1)
    a = F.sin(dlat / 2) ** 2 + F.cos(rlat1) * F.cos(rlat2) * F.sin(dlng / 2) ** 2
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(a))


def station_daily_flows(trips: DataFrame) -> DataFrame:
    departures = (
        trips.where(F.col("start_station_id").isNotNull())
        .groupBy(
            "city",
            "source_month",
            F.col("start_station_id").alias("station_id"),
            F.to_date("started_at").alias("flow_date"),
        )
        .agg(F.count("*").alias("departures"), F.max("start_station_name").alias("dep_name"))
    )
    arrivals = (
        trips.where(F.col("end_station_id").isNotNull())
        .groupBy(
            "city",
            "source_month",
            F.col("end_station_id").alias("station_id"),
            F.to_date("ended_at").alias("flow_date"),
        )
        .agg(F.count("*").alias("arrivals"), F.max("end_station_name").alias("arr_name"))
    )
    keys = ["city", "source_month", "station_id", "flow_date"]
    return (
        departures.join(arrivals, keys, "full_outer")
        .select(
            *keys,
            F.coalesce("dep_name", "arr_name").alias("station_name"),
            F.coalesce("departures", F.lit(0)).alias("departures"),
            F.coalesce("arrivals", F.lit(0)).alias("arrivals"),
        )
        .withColumn("net_flow", F.col("arrivals") - F.col("departures"))
        .select("city", "source_month", "station_id", "station_name", "flow_date", "departures", "arrivals", "net_flow")
    )


def prepare_warehouse(city_code: str, month: str) -> None:
    """Create tables + the month partition, and delete rows from a previous run
    so every month load is idempotent."""
    first_day = month_start(month)
    next_month = date(first_day.year + (first_day.month == 12), first_day.month % 12 + 1, 1)
    partition = f"raw.trips_{first_day:%Y_%m}"
    with psycopg2.connect(pg_dsn()) as conn, conn.cursor() as cur:
        cur.execute(BOOTSTRAP_SQL)
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {partition} PARTITION OF raw.trips "
            "FOR VALUES FROM (%s) TO (%s)",
            (first_day, next_month),
        )
        for table in ("raw.trips", "raw.station_daily_flows"):
            cur.execute(f"DELETE FROM {table} WHERE city = %s AND source_month = %s", (city_code, first_day))


def write_jdbc(df: DataFrame, table: str, partitions: int = 8) -> None:
    (
        df.repartition(partitions)
        .write.format("jdbc")
        .option("url", pg_jdbc_url())
        .option("dbtable", table)
        .option("user", env("PG_USER"))
        .option("password", env("PG_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 20000)
        .mode("append")
        .save()
    )


def record_audit(city_code: str, month: str, raw_rows: int, clean_rows: int) -> None:
    with psycopg2.connect(pg_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.trip_load_audit (city, source_month, raw_rows, clean_rows, rejected_rows)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (city, source_month) DO UPDATE SET
                raw_rows = EXCLUDED.raw_rows, clean_rows = EXCLUDED.clean_rows,
                rejected_rows = EXCLUDED.rejected_rows, loaded_at = now()
            """,
            (city_code, month_start(month), raw_rows, clean_rows, raw_rows - clean_rows),
        )


def process_month(spark: SparkSession, city_code: str, month: str) -> None:
    if not object_exists(f"raw/tripdata/city={city_code}/month={month}/_SUCCESS"):
        log.warning("no complete raw data in the lake for %s %s, skipping", city_code, month)
        return
    bucket = lake_bucket()
    source = f"s3a://{bucket}/raw/tripdata/city={city_code}/month={month}/"
    target = f"s3a://{bucket}/curated/trips/city={city_code}/month={month}"
    log.info("processing %s*.csv", source)

    raw = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("pathGlobFilter", "*.csv")
        .schema(RAW_SCHEMA)
        .csv(source)
    )
    raw_rows = raw.count()

    # Write curated Parquet first, then read it back: downstream steps reuse the
    # cleaned data without re-parsing the CSVs. Clearing the prefix first also
    # removes leftovers of any earlier failed attempt.
    delete_prefix(f"curated/trips/city={city_code}/month={month}/")
    clean_trips(raw, city_code, month).write.mode("overwrite").parquet(target)
    trips = spark.read.parquet(target)
    clean_rows = trips.count()
    log.info("  %s %s: %d raw rows -> %d clean rows (%.2f%% rejected)",
             city_code, month, raw_rows, clean_rows, 100 * (raw_rows - clean_rows) / max(raw_rows, 1))

    prepare_warehouse(city_code, month)
    ordered = trips.select(
        "ride_id", "city", "source_month", "rideable_type", "member_type", "started_at", "ended_at",
        "start_station_id", "start_station_name", "end_station_id", "end_station_name",
        "start_lat", "start_lng", "end_lat", "end_lng", "duration_min", "distance_km", "is_round_trip",
    )
    write_jdbc(ordered, "raw.trips", partitions=max(4, min(16, clean_rows // 250_000)))
    write_jdbc(station_daily_flows(trips), "raw.station_daily_flows", partitions=4)
    record_audit(city_code, month, raw_rows, clean_rows)
    log.info("  loaded raw.trips and raw.station_daily_flows for %s %s", city_code, month)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--city", default=os.getenv("DEFAULT_CITY", "JC"))
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    args = parser.parse_args()

    spark = build_spark("bikeshare-process-trips")
    spark.sparkContext.setLogLevel("WARN")
    try:
        for month in month_range(args.start_month, args.end_month):
            process_month(spark, args.city.upper(), month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
