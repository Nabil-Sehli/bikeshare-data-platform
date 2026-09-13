-- Exposes batch load statistics to the (read-only) dashboard.
select
    city,
    source_month,
    raw_rows,
    clean_rows,
    rejected_rows,
    rejected_pct,
    loaded_at
from {{ ref('stg_trip_load_audit') }}
