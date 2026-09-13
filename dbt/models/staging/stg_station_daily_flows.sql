select
    city,
    source_month,
    station_id,
    station_name,
    flow_date,
    departures,
    arrivals,
    net_flow
from {{ source('raw', 'station_daily_flows') }}
