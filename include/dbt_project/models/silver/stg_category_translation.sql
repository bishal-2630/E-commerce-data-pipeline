select
    lower(trim(product_category_name))          as category_name_pt,
    lower(trim(product_category_name_english))  as category_name_en,
    _batch_id,
    _ingested_at
from {{ source('bronze', 'product_category_translation') }}
qualify row_number() over (partition by product_category_name order by _ingested_at desc) = 1