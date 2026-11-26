select
    geolocation_zip_code_prefix                         as zip_code_prefix,
    avg(try_to_double(geolocation_lat))                 as latitude,
    avg(try_to_double(geolocation_lng))                 as longitude,
    mode(lower(trim(geolocation_city)))                 as city,
    mode(upper(trim(geolocation_state)))                as state,
    count(*)                                            as source_row_count
from {{ source('bronze', 'geolocation') }}
where geolocation_zip_code_prefix is not null
group by geolocation_zip_code_prefix