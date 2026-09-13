-- Grain: city x day. Demand KPIs next to the day's weather.
with demand as (
    select
        city,
        start_date,
        sum(trips)                                                        as trips,
        sum(case when member_type = 'member' then trips else 0 end)       as member_trips,
        sum(case when member_type = 'casual' then trips else 0 end)       as casual_trips,
        sum(case when rideable_type = 'electric_bike' then trips else 0 end) as ebike_trips,
        round(sum(avg_duration_min * trips) / nullif(sum(trips), 0), 2)   as avg_duration_min,
        sum(total_distance_km)                                            as total_distance_km
    from {{ ref('agg_daily_demand') }}
    group by 1, 2
),

daily_weather as (
    select
        city,
        observed_hour::date                          as weather_date,
        round(avg(temperature_c)::numeric, 1)        as avg_temperature_c,
        round(max(temperature_c)::numeric, 1)        as max_temperature_c,
        round(sum(precipitation_mm)::numeric, 1)     as precipitation_mm,
        round(sum(snowfall_cm)::numeric, 1)          as snowfall_cm,
        sum(case when is_wet_hour then 1 else 0 end) as wet_hours
    from {{ ref('stg_weather_hourly') }}
    group by 1, 2
)

select
    d.city,
    d.start_date,
    dd.day_name,
    dd.is_weekend,
    d.trips,
    d.member_trips,
    d.casual_trips,
    d.ebike_trips,
    round(100.0 * d.ebike_trips / nullif(d.trips, 0), 1) as ebike_share_pct,
    d.avg_duration_min,
    d.total_distance_km,
    w.avg_temperature_c,
    w.max_temperature_c,
    w.precipitation_mm,
    w.snowfall_cm,
    w.wet_hours,
    case
        when w.snowfall_cm > 1        then 'Snow day'
        when w.wet_hours >= 4         then 'Rainy day'
        when w.wet_hours between 1 and 3 then 'Showers'
        when w.city is null           then 'No weather data'
        else 'Dry day'
    end                                                  as day_weather_type
from demand d
left join daily_weather w
    on w.city = d.city and w.weather_date = d.start_date
left join {{ ref('dim_date') }} dd
    on dd.date_day = d.start_date
