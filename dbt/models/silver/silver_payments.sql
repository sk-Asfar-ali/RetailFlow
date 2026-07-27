-- Silver: payments, deduplicated on payment_id, keeping the latest status.

with source as (

    select * from {{ source('bronze', 'payments') }}

),

deduplicated as (

    select *,
        row_number() over (
            partition by payment_id
            order by updated_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        payment_id,
        order_id,
        amount,
        upper(trim(method)) as method,
        upper(trim(status)) as status,
        paid_at,
        created_at,
        updated_at
    from deduplicated
    where row_num = 1
      and amount >= 0

)

select * from cleaned
