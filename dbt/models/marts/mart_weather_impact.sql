-- How weather changes demand: average trips per hour split by temperature band,
-- wet/dry and weekday/weekend. Hours with zero trips count too (left join from weather).
with trips_per_hour as (
    select city, start_date, start_hour, count(*) as trips
    from {{ ref('fct_trips') }}
    group by 1, 2, 3
),

loaded_months as (
    select distinct city, source_month
    from {{ ref('stg_trip_load_audit') }}
),

hourly as (
    select
        w.city,
        w.observed_hour,
        extract(hour from w.observed_hour)::int         as hour_of_day,
        extract(isodow from w.observed_hour) in (6, 7)  as is_weekend,
        w.temperature_band,
        w.is_wet_hour,
        coalesce(t.trips, 0)                            as trips
    from {{ ref('stg_weather_hourly') }} w
    -- only hours inside months we actually have trips for
    join loaded_months m
        on m.city = w.city
        and m.source_month = date_trunc('month', w.observed_hour)::date
    left join trips_per_hour t
        on  t.city = w.city
        and t.start_date = w.observed_hour::date
        and t.start_hour = extract(hour from w.observed_hour)::int
)

select
    city,
    temperature_band,
    is_wet_hour,
    is_weekend,
    count(*)                                   as observed_hours,
    round(avg(trips), 1)                       as avg_trips_per_hour,
    -- daytime (7:00-21:59) only, to avoid night hours diluting the comparison
    round(avg(case when hour_of_day between 7 and 21 then trips end), 1) as avg_daytime_trips_per_hour
from hourly
group by 1, 2, 3, 4
