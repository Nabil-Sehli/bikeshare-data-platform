/* @bruin
name: bruin_mart.ebike_single_ride_revenue_daily
type: pg.sql
description: >
  Estimated daily revenue from casual riders' e-bike single rides, applying the
  current GBFS e-bike single-ride price (unlock fee + per-minute rate) to trip history.
  An estimate only: members, passes and historical price changes are not modelled.

depends:
  - bruin_raw.gbfs_pricing_plans
  - marts.fct_trips

materialization:
  type: table

columns:
  - name: city
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: ["JC", "NYC"]
  - name: start_date
    type: date
    checks:
      - name: not_null
  - name: ebike_casual_trips
    type: integer
    checks:
      - name: positive
  - name: est_revenue_usd
    type: float
    checks:
      - name: not_null
      - name: min
        value: 0

custom_checks:
  - name: one row per city and day
    query: |
      select count(*) from (
        select city, start_date from bruin_mart.ebike_single_ride_revenue_daily
        group by 1, 2 having count(*) > 1
      ) dup
    value: 0
@bruin */

with plan as (
    select unlock_price, per_minute_rate
    from bruin_raw.gbfs_pricing_plans
    where plan_id = 'EBIKE_SINGLE_RIDE'
)

select
    t.city,
    t.start_date,
    count(*)                                                        as ebike_casual_trips,
    round(avg(t.duration_min)::numeric, 1)                          as avg_duration_min,
    round(sum(p.unlock_price + p.per_minute_rate * ceil(t.duration_min))::numeric, 2) as est_revenue_usd
from marts.fct_trips t
cross join plan p
where t.member_type = 'casual'
  and t.rideable_type = 'electric_bike'
group by 1, 2
