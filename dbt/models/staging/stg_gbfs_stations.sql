select
    s.station_id                                        as gbfs_station_id,
    s.short_name                                        as station_key,
    s.name                                              as station_name,
    s.region_id,
    r.name                                              as region_name,
    {{ city_from_station('s.short_name', 's.region_id') }} as city,
    s.lat                                               as latitude,
    s.lon                                               as longitude,
    s.capacity::int                                     as capacity,
    s.feed_updated_at,
    s.loaded_at
from {{ source('raw_gbfs', 'stations') }} s
left join {{ source('raw_gbfs', 'regions') }} r
    on r.region_id = s.region_id
where coalesce(r.name, '') not in ('testzone', 'IC HQ')
