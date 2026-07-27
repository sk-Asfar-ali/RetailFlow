-- Silver: cleaned product catalog, deduplicated on product_id.

with source as (

    select * from {{ source('bronze', 'products') }}

),

deduplicated as (

    select *,
        row_number() over (
            partition by product_id
            order by updated_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        product_id,
        trim(product_name) as product_name,
        trim(category) as category,
        trim(sub_category) as sub_category,
        trim(brand) as brand,
        price,
        stock_qty,
        is_discontinued,
        created_at,
        updated_at
    from deduplicated
    where row_num = 1
      and price >= 0

)

select * from cleaned
