with bounds as (
    select
        least(coalesce(min(flow_date), current_date), current_date - 30) as start_date,
        greatest(coalesce(max(flow_date), current_date), current_date)   as end_date
    from {{ ref('stg_station_daily_flows') }}
)

select
    d::date                                   as date_day,
    extract(year from d)::int                 as year,
    extract(quarter from d)::int              as quarter,
    extract(month from d)::int                as month,
    to_char(d, 'Mon')                         as month_name,
    date_trunc('month', d)::date              as month_start,
    extract(isodow from d)::int               as day_of_week,
    to_char(d, 'Dy')                          as day_name,
    extract(isodow from d) in (6, 7)          as is_weekend
from bounds, generate_series(bounds.start_date, bounds.end_date, interval '1 day') as d
