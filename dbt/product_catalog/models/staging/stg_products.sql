{{ config(materialized='view') }}

with source_data as (

    select *
    from {{ source('raw', 'raw_products') }}

),

cleaned as (

    select
        nullif(trim(product_id), '') as product_id,

        nullif(trim(name), '') as name,

        nullif(trim(brand), '') as brand,

        nullif(trim(category), '') as category,

        nullif(trim(sub_category), '') as sub_category,

        try_to_decimal(
            nullif(regexp_replace(price, '[^0-9.-]', ''), ''),
            12,
            2
        ) as price,

        try_to_decimal(
            nullif(regexp_replace(list_price, '[^0-9.-]', ''), ''),
            12,
            2
        ) as list_price,

        try_to_decimal(
            nullif(regexp_replace(discount_pct, '[^0-9.-]', ''), ''),
            5,
            2
        ) as discount_pct,

        try_to_decimal(
            nullif(regexp_replace(average_rating, '[^0-9.-]', ''), ''),
            4,
            2
        ) as average_rating,

        case
            when upper(trim(out_of_stock)) = 'TRUE' then true
            when upper(trim(out_of_stock)) = 'FALSE' then false
            else null
        end as out_of_stock,

        nullif(trim(description), '') as description,

        nullif(trim(seller), '') as seller,

        nullif(trim(product_url), '') as product_url,

        try_to_timestamp_ntz(
            source_crawled_at,
            'DD/MM/YYYY, HH24:MI:SS'
        ) as source_crawled_at,

        nullif(trim(source_file), '') as source_file,

        nullif(trim(batch_id), '') as batch_id,

        loaded_at

    from source_data

)

select *
from cleaned
where product_id is not null