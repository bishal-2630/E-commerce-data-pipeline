select
    c.state,
    count(*)                                          as delivered_orders,
    round(avg(f.delivery_days), 1)                    as avg_delivery_days,
    count_if(f.is_late)                               as late_orders,
    round(100 * count_if(not f.is_late) / count(*), 1) as on_time_pct,
    round(avg(f.review_score), 2)                     as avg_review_score
from {{ ref('fact_orders') }} f
join {{ ref('dim_customer') }} c
    on f.customer_unique_id = c.customer_unique_id
where f.order_status = 'delivered'
  and f.is_late is not null
group by c.state