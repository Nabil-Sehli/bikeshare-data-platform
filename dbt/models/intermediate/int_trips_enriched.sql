-- Trips joined to the weather observed in the hour the trip started.
select
    t.*,
    w.temperature_c,
    w.feels_like_c,
    w.precipitation_mm,
    w.snowfall_cm,
    w.wind_speed_kmh,
    w.weather_category,
    w.temperature_band,
    w.is_wet_hour
from {{ ref('stg_trips') }} t
left join {{ ref('stg_weather_hourly') }} w
    on  w.city = t.city
    and w.observed_hour = date_trunc('hour', t.started_at)
