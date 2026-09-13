-- Warehouse source tables that must exist before the first dbt build.
-- Schemas + grants are managed by Terraform (infra/terraform); this file only
-- creates tables, idempotently. Run with: python -m bikeshare.warehouse.bootstrap

-- ---------------------------------------------------------------- Spark batch job
CREATE TABLE IF NOT EXISTS raw.trips (
    ride_id             text        NOT NULL,
    city                text        NOT NULL,
    source_month        date        NOT NULL,
    rideable_type       text,
    member_type         text,
    started_at          timestamp   NOT NULL,
    ended_at            timestamp   NOT NULL,
    start_station_id    text,
    start_station_name  text,
    end_station_id      text,
    end_station_name    text,
    start_lat           double precision,
    start_lng           double precision,
    end_lat             double precision,
    end_lng             double precision,
    duration_min        double precision,
    distance_km         double precision,
    is_round_trip       boolean
) PARTITION BY RANGE (started_at);

CREATE INDEX IF NOT EXISTS trips_city_month_idx    ON raw.trips (city, source_month);
CREATE INDEX IF NOT EXISTS trips_start_station_idx ON raw.trips (start_station_id);
CREATE INDEX IF NOT EXISTS trips_started_at_brin   ON raw.trips USING brin (started_at);

CREATE TABLE IF NOT EXISTS raw.station_daily_flows (
    city          text    NOT NULL,
    source_month  date    NOT NULL,
    station_id    text    NOT NULL,
    station_name  text,
    flow_date     date    NOT NULL,
    departures    bigint  NOT NULL,
    arrivals      bigint  NOT NULL,
    net_flow      bigint  NOT NULL
);
CREATE INDEX IF NOT EXISTS station_daily_flows_city_month_idx ON raw.station_daily_flows (city, source_month);

CREATE TABLE IF NOT EXISTS raw.trip_load_audit (
    city           text        NOT NULL,
    source_month   date        NOT NULL,
    raw_rows       bigint      NOT NULL,
    clean_rows     bigint      NOT NULL,
    rejected_rows  bigint      NOT NULL,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (city, source_month)
);

-- ------------------------------------------------------ Kafka live consumer
CREATE TABLE IF NOT EXISTS live.station_status_latest (
    station_id            text PRIMARY KEY,
    num_bikes_available   integer,
    num_ebikes_available  integer,
    num_bikes_disabled    integer,
    num_docks_available   integer,
    num_docks_disabled    integer,
    is_installed          boolean,
    is_renting            boolean,
    is_returning          boolean,
    last_reported         timestamptz,
    feed_updated_at       timestamptz,
    ingested_at           timestamptz,
    updated_at            timestamptz NOT NULL DEFAULT now()
);

-- -------------------------------------------- dlt lake -> warehouse (created empty so
-- dbt can build before the first streaming files arrive; dlt reuses the table)
CREATE TABLE IF NOT EXISTS raw_stream.station_status (
    station_id            varchar,
    num_bikes_available   integer,
    num_ebikes_available  integer,
    num_bikes_disabled    integer,
    num_docks_available   integer,
    num_docks_disabled    integer,
    is_installed          boolean,
    is_renting            boolean,
    is_returning          boolean,
    last_reported         timestamptz,
    feed_updated_at       timestamptz,
    ingested_at           timestamptz,
    kafka_partition       integer,
    kafka_offset          bigint,
    _dlt_load_id          varchar NOT NULL,
    _dlt_id               varchar NOT NULL UNIQUE
);
