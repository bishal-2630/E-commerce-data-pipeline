with monthly as (
    select
        d.year_month,
        d.year,
        d.month,
        count(*)                          as orders,
        count(distinct f.customer_unique_id) as customers,
        sum(f.items_revenue)              as revenue
    from {{ ref('fact_orders') }} f
    join {{ ref('dim_date') }} d
        on f.purchase_date = d.date_key
    where f.order_status not in ('canceled', 'unavailable')
    group by d.year_month, d.year, d.month
)

select
    year_month,
    year,
    month,
    orders,
    customers,
    revenue,
    round(revenue / orders, 2)            as avg_order_value,
    lag(revenue) over (order by year_month) as prev_month_revenue,
    round(
        (revenue - lag(revenue) over (order by year_month))
        / nullif(lag(revenue) over (order by year_month), 0) * 100, 1
    )                                     as mom_growth_pct
from monthly