{{ config(
    materialized='table',
    schema='CORE'
) }}

select
    product_id,
    name,
    average_rating,
    case
        when average_rating >= 4.5 then 'excellent'
        when average_rating >= 3.5 then 'good'
        when average_rating >= 2.5 then 'average'
        else 'low'
    end as rating_band
from {{ ref('stg_products') }}