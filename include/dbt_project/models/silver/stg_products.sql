with products as (
    select * from {{ source('bronze', 'products') }}
    qualify row_number() over (partition by product_id order by _ingested_at desc) = 1
)
select
    p.product_id,
    coalesce(t.category_name_en, lower(trim(p.product_category_name)), 'unknown') as category_name,
    try_to_number(p.product_name_lenght)         as name_length,
    try_to_number(p.product_description_lenght)  as description_length,
    try_to_number(p.product_photos_qty)          as photos_qty,
    try_to_number(p.product_weight_g)            as weight_g,
    try_to_number(p.product_length_cm)           as length_cm,
    try_to_number(p.product_height_cm)           as height_cm,
    try_to_number(p.product_width_cm)            as width_cm,
    p._batch_id,
    p._ingested_at
from products p
left join {{ ref('stg_category_translation') }} t
    on lower(trim(p.product_category_name)) = t.category_name_pt