select
    p.category_name,
    count(*)                       as items_sold,
    count(distinct i.order_id)     as orders,
    sum(i.price)                   as revenue,
    sum(i.freight_value)           as freight,
    round(avg(i.price), 2)         as avg_price
from {{ ref('fact_order_items') }} i
join {{ ref('dim_product') }} p
    on i.product_id = p.product_id
where i.order_status not in ('canceled', 'unavailable')
group by p.category_name