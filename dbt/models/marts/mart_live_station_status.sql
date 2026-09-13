-- Real-time view: always reads the latest row the Kafka consumer upserted.
{{ config(materialized='view') }}

select
    d.station_key,
    d.station_name,
    d.city,
    d.region_name,
    d.latitude,
    d.longitude,
    d.capacity,
    l.num_bikes_available,
    l.num_ebikes_available,
    l.num_docks_available,
    l.num_bikes_disabled,
    l.is_renting,
    l.last_reported,
    round(extract(epoch from now() - l.last_reported) / 60.0) as minutes_since_report,
    round(l.num_bikes_available::numeric / nullif(l.num_bikes_available + l.num_docks_available, 0), 2) as fill_ratio,
    case
        when not l.is_installed or not l.is_renting                      then 'Offline'
        when l.num_bikes_available = 0                                   then 'Empty'
        when l.num_docks_available = 0                                   then 'Full'
        when l.num_bikes_available::numeric
             / nullif(l.num_bikes_available + l.num_docks_available, 0)
             < {{ var('low_availability_ratio') }}                       then 'Low bikes'
        when l.num_docks_available::numeric
             / nullif(l.num_bikes_available + l.num_docks_available, 0)
             < {{ var('low_availability_ratio') }}                       then 'Low docks'
        else 'OK'
    end                                                                  as availability_status,
    l.updated_at
from {{ source('live', 'station_status_latest') }} l
join {{ ref('dim_stations') }} d
    on d.gbfs_station_id = l.station_id
