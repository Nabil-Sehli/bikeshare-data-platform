-- From the streaming feed: per station and hour, how often was it empty / full?
-- Status events are change-based, so each event is weighted by how long it stayed
-- current (until the next event, capped at 1 hour).
{{ config(indexes=[{'columns': ['station_key', 'status_hour']}]) }}

with events as (
    select
        s.gbfs_station_id,
        s.last_reported_local,
        s.num_bikes_available,
        s.num_docks_available,
        s.is_renting,
        lead(s.last_reported_local) over (
            partition by s.gbfs_station_id order by s.last_reported_local
        ) as next_reported_local
    from {{ ref('stg_station_status') }} s
    where s.last_reported >= now() - interval '{{ var("station_status_lookback_days") }} days'
      -- the first poll after a producer restart re-sends every station; ignore reports
      -- that were already stale then (decommissioned / non-reporting stations)
      and s.last_reported >= s.ingested_at - interval '2 hours'
),

weighted as (
    select
        gbfs_station_id,
        date_trunc('hour', last_reported_local) as status_hour,
        num_bikes_available,
        num_docks_available,
        is_renting,
        extract(epoch from least(
            coalesce(next_reported_local, last_reported_local + interval '5 minutes'),
            last_reported_local + interval '1 hour'
        ) - last_reported_local) as seconds_current
    from events
)

select
    d.station_key,
    d.station_name,
    d.city,
    w.status_hour,
    count(*)                                                                 as status_events,
    round((sum(num_bikes_available * seconds_current) / nullif(sum(seconds_current), 0))::numeric, 1) as avg_bikes_available,
    round((100 * sum(case when num_bikes_available = 0 then seconds_current else 0 end)
          / nullif(sum(seconds_current), 0))::numeric, 1)                   as pct_time_empty,
    round((100 * sum(case when num_docks_available = 0 then seconds_current else 0 end)
          / nullif(sum(seconds_current), 0))::numeric, 1)                   as pct_time_full
from weighted w
join {{ ref('dim_stations') }} d
    on d.gbfs_station_id = w.gbfs_station_id
group by 1, 2, 3, 4
