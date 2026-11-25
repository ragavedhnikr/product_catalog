{% snapshot products_snapshot %}

{{
    config(
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'name',
            'brand',
            'category',
            'sub_category',
            'price',
            'list_price',
            'discount_pct',
            'average_rating',
            'out_of_stock',
            'description',
            'seller',
            'product_url'
        ]
    )
}}

select
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
    source_crawled_at,
    source_file,
    batch_id,
    loaded_at
from {{ ref('stg_products') }}

{% endsnapshot %}