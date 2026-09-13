-- Which stations drain (more departures than arrivals) or fill up, i.e. where
-- the operator has to move bikes by truck.
with flows as (
    select
        city,
        station_id                         as station_key,
        count(*)                           as active_days,
        sum(departures)                    as departures,
        sum(arrivals)                      as arrivals,
        sum(net_flow)                      as net_flow,
        avg(abs(net_flow))                 as avg_abs_daily_imbalance,
        avg(net_flow)                      as avg_daily_net_flow
    from {{ ref('stg_station_daily_flows') }}
    group by 1, 2
)

select
    f.city,
    f.station_key,
    s.station_name,
    s.latitude,
    s.longitude,
    s.capacity,
    f.active_days,
    f.departures,
    f.arrivals,
    f.net_flow,
    round(f.avg_daily_net_flow::numeric, 2)      as avg_daily_net_flow,
    round(f.avg_abs_daily_imbalance::numeric, 2) as avg_abs_daily_imbalance,
    case
        when f.avg_daily_net_flow <= -5 then 'Drains (needs bikes)'
        when f.avg_daily_net_flow >= 5  then 'Fills up (needs docks)'
        else 'Balanced'
    end                                          as rebalancing_profile,
    rank() over (partition by f.city order by f.avg_abs_daily_imbalance desc) as imbalance_rank
from flows f
left join {{ ref('dim_stations') }} s
    on s.station_key = f.station_key
