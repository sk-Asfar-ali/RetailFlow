-- Silver: order line items. Insert-only in Bronze, so no dedup needed
-- beyond basic sanity filtering.

with source as (

    select * from {{ source('bronze', 'order_items') }}

),

cleaned as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price as line_total,
        created_at
    from source
    where quantity > 0
      and unit_price >= 0

)

select * from cleaned
