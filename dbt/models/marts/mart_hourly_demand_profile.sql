-- Weekday x hour heatmap of average trips (per calendar day observed)
with days as (
    select city, count(distinct start_date) filter (where start_day_of_week < 6) as weekdays,
                 count(distinct start_date) filter (where start_day_of_week >= 6) as weekend_days
    from {{ ref('fct_trips') }}
    group by 1
),

counts as (
    select city, start_day_of_week, start_hour, member_type, count(*) as trips,
           count(distinct start_date) as days_observed
    from {{ ref('fct_trips') }}
    group by 1, 2, 3, 4
)

select
    c.city,
    c.start_day_of_week                                  as day_of_week,
    (array['Mon','Tue','Wed','Thu','Fri','Sat','Sun'])[c.start_day_of_week] as day_name,
    c.start_hour                                         as hour_of_day,
    c.member_type,
    c.trips,
    round(c.trips::numeric / nullif(c.days_observed, 0), 1) as avg_trips
from counts c
join days d using (city)
