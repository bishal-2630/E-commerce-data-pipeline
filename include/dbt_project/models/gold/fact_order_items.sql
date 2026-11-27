select
    i.order_item_key,
    i.order_id,
    i.order_item_seq,
    i.product_id,
    i.seller_id,
    c.customer_unique_id,
    o.order_status,
    o.purchased_at::date             as purchase_date,
    i.price,
    i.freight_value,
    i.price + i.freight_value        as item_total
from {{ ref('stg_order_items') }} i
left join {{ ref('stg_orders') }} o
    on i.order_id = o.order_id
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id