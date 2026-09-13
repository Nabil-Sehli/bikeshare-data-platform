-- Grain: city x day x member type x bike type
{{ config(indexes=[{'columns': ['city', 'start_date']}]) }}

select
    city,
    start_date,
    member_type,
    rideable_type,
    count(*)                                            as trips,
    round(avg(duration_min)::numeric, 2)                as avg_duration_min,
    round(percentile_cont(0.5) within group (order by duration_min)::numeric, 2) as median_duration_min,
    round(sum(distance_km)::numeric, 1)                 as total_distance_km,
    sum(case when is_round_trip then 1 else 0 end)      as round_trips
from {{ ref('fct_trips') }}
group by 1, 2, 3, 4
