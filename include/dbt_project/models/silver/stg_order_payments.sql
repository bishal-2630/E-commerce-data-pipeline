select
    order_id || '-' || payment_sequential   as payment_key,
    order_id,
    try_to_number(payment_sequential)       as payment_seq,
    lower(payment_type)                     as payment_type,
    try_to_number(payment_installments)     as installments,
    try_to_decimal(payment_value, 12, 2)    as payment_value,
    _batch_id,
    _ingested_at
from {{ source('bronze', 'order_payments') }}
qualify row_number() over (partition by order_id, payment_sequential order by _ingested_at desc) = 1