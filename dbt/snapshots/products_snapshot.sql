{% snapshot products_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_name', 'category', 'sub_category', 'brand',
                    'price', 'is_discontinued'],
        invalidate_hard_deletes=True,
    )
}}

-- SCD Type 2 snapshot of the products dimension, using dbt's `check`
-- strategy (hash-based comparison across check_cols). Deliberately
-- excludes `stock_qty` from check_cols -- that field changes on nearly
-- every order/restock and is a fast-moving metric, not a dimensional
-- attribute worth versioning; including it would create a new SCD
-- version on almost every run, defeating the purpose.
--
-- Query this table to answer "what was this product's price on date X"
-- or to compute revenue at the price actually charged at time of sale
-- rather than the product's current price.

select
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price,
    stock_qty,
    is_discontinued,
    updated_at
from {{ source('bronze', 'products') }}

{% endsnapshot %}