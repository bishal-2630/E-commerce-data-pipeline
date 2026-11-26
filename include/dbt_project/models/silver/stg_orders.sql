select
    order_id,
    customer_id,
    lower(order_status)                              as order_status,
    try_to_timestamp(order_purchase_timestamp)       as purchased_at,
    try_to_timestamp(order_approved_at)              as approved_at,
    try_to_timestamp(order_delivered_customer_date)  as delivered_at,
    try_to_timestamp(order_estimated_delivery_date)  as estimated_delivery_at,
    _batch_id,
    _ingested_at
from {{ source('bronze', 'orders') }}
qualify row_number() over (partition by order_id order by _ingested_at desc) = 1