/* @bruin
name: bruin_mart.network_health_snapshots
type: pg.sql
description: >
  Point-in-time health of the station network per city, appended on every run
  to build a history (empty / full / offline stations, bikes in circulation).

depends:
  - marts.mart_live_station_status

materialization:
  type: table
  strategy: append

columns:
  - name: snapshot_at
    type: timestamp
    checks:
      - name: not_null
  - name: city
    type: varchar
    checks:
      - name: not_null
  - name: stations
    type: integer
    checks:
      - name: positive
  - name: pct_stations_empty
    type: float
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: bikes_available
    type: integer
    checks:
      - name: min
        value: 0

custom_checks:
  - name: live feed is fresh (latest snapshot saw a report in the last 2 hours)
    query: |
      select count(*) from bruin_mart.network_health_snapshots
      where snapshot_at = (select max(snapshot_at) from bruin_mart.network_health_snapshots)
        and newest_report_minutes_ago > 120
    value: 0
@bruin */

select
    date_trunc('minute', now())::timestamp                                         as snapshot_at,
    city,
    count(*)                                                                       as stations,
    sum(case when availability_status = 'Empty' then 1 else 0 end)                 as empty_stations,
    sum(case when availability_status = 'Full' then 1 else 0 end)                  as full_stations,
    sum(case when availability_status = 'Offline' then 1 else 0 end)               as offline_stations,
    round(100.0 * avg(case when availability_status = 'Empty' then 1 else 0 end), 1) as pct_stations_empty,
    sum(num_bikes_available)                                                       as bikes_available,
    sum(num_ebikes_available)                                                      as ebikes_available,
    sum(num_docks_available)                                                       as docks_available,
    min(minutes_since_report)                                                      as newest_report_minutes_ago
from marts.mart_live_station_status
group by city
