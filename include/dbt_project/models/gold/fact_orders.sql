with items as (
    select
        order_id,
        count(*)            as item_count,
        sum(price)          as items_revenue,
        sum(freight_value)  as freight_total
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value)  as payment_total
    from {{ ref('stg_order_payments') }}
    group by order_id
)

select
    o.order_id,
    c.customer_unique_id,
    o.order_status,
    o.purchased_at,
    o.purchased_at::date                                    as purchase_date,
    o.delivered_at,
    o.estimated_delivery_at,

    case when o.delivered_at >= o.purchased_at
         then datediff(day, o.purchased_at, o.delivered_at) end   as delivery_days,
    case when o.delivered_at is not null
         then datediff(day, o.estimated_delivery_at, o.delivered_at) end as delay_days,
    case when o.delivered_at is not null
         then o.delivered_at > o.estimated_delivery_at end        as is_late,

    coalesce(i.item_count, 0)       as item_count,
    coalesce(i.items_revenue, 0)    as items_revenue,
    coalesce(i.freight_total, 0)    as freight_total,
    coalesce(p.payment_total, 0)    as payment_total,
    r.review_score
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
left join items i
    on o.order_id = i.order_id
left join payments p
    on o.order_id = p.order_id
left join {{ ref('stg_order_reviews') }} r
    on o.order_id = r.order_id