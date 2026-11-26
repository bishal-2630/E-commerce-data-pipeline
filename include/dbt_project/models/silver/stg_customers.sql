select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix          as zip_code_prefix,
    lower(trim(customer_city))        as city,
    lower(trim(customer_state))       as state,
    _batch_id,
    _ingested_at
from {{ source('bronze', 'customers') }}
qualify row_number() over (partition by customer_id order by _ingested_at desc) = 1
