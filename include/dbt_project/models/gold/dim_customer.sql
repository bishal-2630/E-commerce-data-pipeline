with customer_orders as (
    select
        c.customer_unique_id,
        c.customer_id,
        c.zip_code_prefix,
        c.city,
        c.state,
        o.purchased_at
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_orders') }} o
        on c.customer_id = o.customer_id
),

latest_profile as (
    select *
    from customer_orders
    qualify row_number() over (
        partition by customer_unique_id
        order by purchased_at desc nulls last
    ) = 1
),

order_stats as (
    select
        customer_unique_id,
        count(distinct customer_id)  as total_orders,
        min(purchased_at)            as first_order_at,
        max(purchased_at)            as last_order_at
    from customer_orders
    group by customer_unique_id
)

select
    l.customer_unique_id,
    l.city,
    l.state,
    l.zip_code_prefix,
    s.total_orders,
    s.first_order_at,
    s.last_order_at,
    s.total_orders > 1 as is_repeat_customer
from latest_profile l
join order_stats s
    on l.customer_unique_id = s.customer_unique_id