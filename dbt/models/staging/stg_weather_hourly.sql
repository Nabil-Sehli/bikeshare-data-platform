select
    w.city,
    w.observed_at                               as observed_hour,
    w.temperature_2m                            as temperature_c,
    w.apparent_temperature                      as feels_like_c,
    w.relative_humidity_2m                      as humidity_pct,
    w.precipitation                             as precipitation_mm,
    w.rain                                      as rain_mm,
    w.snowfall                                  as snowfall_cm,
    w.cloud_cover                               as cloud_cover_pct,
    w.wind_speed_10m                            as wind_speed_kmh,
    w.weather_code::int                         as weather_code,
    coalesce(c.weather_category, 'Unknown')     as weather_category,
    c.description                               as weather_description,
    case
        when w.temperature_2m < 0  then '1: below 0°C'
        when w.temperature_2m < 10 then '2: 0-10°C'
        when w.temperature_2m < 20 then '3: 10-20°C'
        when w.temperature_2m < 28 then '4: 20-28°C'
        else '5: 28°C+'
    end                                         as temperature_band,
    w.precipitation > 0.1                       as is_wet_hour
from {{ source('raw_weather', 'weather_hourly') }} w
left join {{ ref('wmo_weather_codes') }} c
    on c.weather_code = w.weather_code::int
