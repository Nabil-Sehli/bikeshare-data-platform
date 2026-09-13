-- Every loaded month in the fact table must have exactly the row count Spark reported.
with fct as (
    select city, source_month, count(*) as fct_rows
    from {{ ref('fct_trips') }}
    group by 1, 2
)

select a.city, a.source_month, a.clean_rows, f.fct_rows
from {{ ref('stg_trip_load_audit') }} a
left join fct f using (city, source_month)
where coalesce(f.fct_rows, 0) <> a.clean_rows
