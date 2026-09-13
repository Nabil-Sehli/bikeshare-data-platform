select
    city,
    source_month,
    raw_rows,
    clean_rows,
    rejected_rows,
    round(100.0 * rejected_rows / nullif(raw_rows, 0), 3) as rejected_pct,
    loaded_at
from {{ source('raw', 'trip_load_audit') }}
