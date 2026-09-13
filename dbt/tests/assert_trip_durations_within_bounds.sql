-- Spark filters trips to 1 minute .. 24 hours; nothing outside may reach the fact table.
select ride_id, duration_min
from {{ ref('fct_trips') }}
where duration_min < 1 or duration_min > 1440
