select
    order_id || '-' || order_item_id        as order_item_key,
    order_id,
    try_to_number(order_item_id)            as order_item_seq,
    product_id,
    seller_id,
    try_to_timestamp(shipping_limit_date)   as shipping_limit_at,
    try_to_decimal(price, 12, 2)            as price,
    try_to_decimal(freight_value, 12, 2)    as freight_value,
    _batch_id,
    _ingested_at
from {{ source('bronze', 'order_items') }}
qualify row_number() over (partition by order_id, order_item_id order by _ingested_at desc) = 1

