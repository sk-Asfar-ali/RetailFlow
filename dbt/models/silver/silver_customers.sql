-- Silver: cleaned, deduplicated customer records.
-- Deduplicates on customer_id (keeping the most recently updated row),
-- standardizes text casing, and filters out clearly invalid rows.

with source as (

    select * from {{ source('bronze', 'customers') }}

),

deduplicated as (

    select *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        customer_id,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        lower(trim(email)) as email,
        phone,
        address_line,
        initcap(trim(city)) as city,
        state,
        country,
        postal_code,
        signup_date,
        is_active,
        created_at,
        updated_at
    from deduplicated
    where row_num = 1
      and email is not null
      and customer_id is not null

)

select * from cleaned
