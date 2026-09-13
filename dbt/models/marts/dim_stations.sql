-- Every station seen either in the live GBFS network or in historical trips.
{{ config(indexes=[{'columns': ['station_key'], 'unique': true}, {'columns': ['gbfs_station_id']}]) }}

with trip_stations as (
    select
        station_id                  as station_key,
        max(city)                   as city,
        max(station_name)           as station_name,
        min(flow_date)              as first_trip_date,
        max(flow_date)              as last_trip_date,
        sum(departures + arrivals)  as lifetime_trip_events
    from {{ ref('stg_station_daily_flows') }}
    group by station_id
),

gbfs as (
    select *
    from (
        select
            s.*,
            -- a short name can briefly exist twice while a station is being replaced
            row_number() over (partition by station_key order by capacity desc nulls last, loaded_at desc) as rn
        from {{ ref('stg_gbfs_stations') }} s
        where station_key is not null
    ) ranked
    where rn = 1
)

select
    coalesce(g.station_key, t.station_key)          as station_key,
    g.gbfs_station_id,
    coalesce(g.station_name, t.station_name)        as station_name,
    coalesce(g.city, t.city)                        as city,
    g.region_name,
    g.latitude,
    g.longitude,
    g.capacity,
    g.gbfs_station_id is not null                   as is_active,
    t.first_trip_date,
    t.last_trip_date,
    coalesce(t.lifetime_trip_events, 0)             as lifetime_trip_events
from gbfs g
full outer join trip_stations t
    on t.station_key = g.station_key
