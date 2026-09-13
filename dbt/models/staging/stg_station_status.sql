-- Streaming events can be delivered more than once (at-least-once Kafka consumer),
-- so keep a single row per station per report time.
with events as (
    select
        station_id                     as gbfs_station_id,
        num_bikes_available,
        coalesce(num_ebikes_available, 0) as num_ebikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,
        is_installed,
        is_renting,
        is_returning,
        last_reported,
        ingested_at,
        row_number() over (
            partition by station_id, last_reported
            order by ingested_at desc
        ) as rn
    from {{ source('raw_stream', 'station_status') }}
    where last_reported > timestamptz '2020-01-01'
)

select
    gbfs_station_id,
    num_bikes_available,
    num_ebikes_available,
    num_bikes_disabled,
    num_docks_available,
    num_docks_disabled,
    is_installed,
    is_renting,
    is_returning,
    last_reported,
    (last_reported at time zone 'America/New_York') as last_reported_local,
    ingested_at
from events
where rn = 1
