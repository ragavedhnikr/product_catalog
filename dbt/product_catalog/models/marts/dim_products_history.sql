{{ config(materialized='table') }}

select
    dbt_scd_id as product_version_key,

    product_id,
    name,
    brand,
    category,
    sub_category,
    price,
    list_price,
    discount_pct,
    average_rating,
    out_of_stock,
    description,
    seller,
    product_url,

    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,

    case
        when dbt_valid_to is null then true
        else false
    end as is_current,

    source_file,
    batch_id,
    loaded_at

from {{ ref('products_snapshot') }}