{#
    Incremental by (city, month):
      * a month is (re)loaded when it is missing from this table
      * the pre-hook first deletes months that Spark reloaded after we last built them
    This avoids an expensive row-by-row merge on ride_id for millions of trips.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['city', 'source_month']},
            {'columns': ['start_date']},
            {'columns': ['start_station_id']},
            {'columns': ['ride_id'], 'unique': false},
        ],
        pre_hook="""
            {% if is_incremental() %}
            delete from {{ this }} f
            using {{ ref('stg_trip_load_audit') }} a
            where f.city = a.city
              and f.source_month = a.source_month
              and f.batch_loaded_at < a.loaded_at
            {% endif %}
        """
    )
}}

with months_to_load as (
    select a.city, a.source_month, a.loaded_at
    from {{ ref('stg_trip_load_audit') }} a
    {% if is_incremental() %}
    where not exists (
        select 1 from {{ this }} f
        where f.city = a.city and f.source_month = a.source_month
    )
    {% endif %}
)

select
    t.ride_id,
    t.city,
    t.source_month,
    t.start_date,
    t.start_hour,
    t.start_day_of_week,
    t.started_at,
    t.ended_at,
    t.member_type,
    t.rideable_type,
    t.start_station_id,
    t.start_station_name,
    t.end_station_id,
    t.end_station_name,
    t.duration_min,
    t.distance_km,
    t.is_round_trip,
    t.end_station_id is null                          as ended_outside_station,
    t.temperature_c,
    t.feels_like_c,
    t.precipitation_mm,
    t.snowfall_cm,
    t.wind_speed_kmh,
    coalesce(t.weather_category, 'Unknown')           as weather_category,
    t.temperature_band,
    t.is_wet_hour,
    m.loaded_at                                       as batch_loaded_at
from {{ ref('int_trips_enriched') }} t
join months_to_load m
    on m.city = t.city and m.source_month = t.source_month
