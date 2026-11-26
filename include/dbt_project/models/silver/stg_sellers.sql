select
    seller_id,
    seller_zip_code_prefix          as zip_code_prefix,
    lower(trim(seller_city))        as city,
    upper(trim(seller_state))       as state,
    _batch_id,
    _ingested_at
from {{ source('bronze', 'sellers') }}
qualify row_number() over (partition by seller_id order by _ingested_at desc) = 1