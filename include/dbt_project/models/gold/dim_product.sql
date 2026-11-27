select
    product_id,
    category_name,
    photos_qty,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    length_cm * height_cm * width_cm as volume_cm3,
    case
        when weight_g is null then 'unknown'
        when weight_g < 500       then 'light'
        when weight_g < 5000      then 'medium'
        else 'heavy'
    end as weight_class
from {{ ref('stg_products') }}