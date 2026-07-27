-- Silver: cleaned orders, deduplicated on order_id, keeping each order's
-- latest known status (Bronze already upserts on order_id, but this
-- protects against any duplicate rows landing from re-runs or retries).

with source as (

    select * from {{ source('bronze', 'orders') }}

),

deduplicated as (

    select *,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        order_id,
        customer_id,
        order_date,
        upper(trim(status)) as status,
        initcap(trim(shipping_city)) as shipping_city,
        shipping_country,
        total_amount,
        created_at,
        updated_at
    from deduplicated
    where row_num = 1
      and order_id is not null
      and customer_id is not null
      and total_amount >= 0

)

select * from cleaned
